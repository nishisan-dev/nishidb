package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// ErrPausado indica ingestão parada por política de erro.
//
// NÃO é erro de processo. O §51 põe "start queries" antes de "resume
// ingestion", e o §82 permite vários datasets por instância: encerrar o
// processo derrubaria consultas que não têm nada a ver com o evento
// problemático. Além disso, como o §49 obriga seek no offset do checkpoint e
// o §48 só avança o offset após checkpoint durável, o evento que causou a
// parada é exatamente o primeiro após o offset persistido — encerrar faria o
// reinício reproduzir o mesmo evento e parar de novo, o que sob supervisor é
// crash loop. A saída é o comando `nishidb offsets`.
var ErrPausado = errors.New("kafka: ingestão pausada por política de erro")

// Store é o que o writer precisa do storage. Declarada aqui, no consumidor,
// pelo mesmo motivo que o materializer declara a sua: manter o pacote
// testável com um dublê sem arrastar a implementação real.
type Store interface {
	Checkpoint() error
	SetSourceOffsets(checkpoint.SourceOffsets) error
	SourceOffsets() (checkpoint.SourceOffsets, bool, error)
	// RowCount é o sinal que separa "dataset novo" de "dataset com dados e
	// sem posição" — a diferença entre aplicar o autoOffsetReset e recusar a
	// subida (D11).
	RowCount() uint64
}

// Cadencia é quando o checkpoint dispara: por contagem OU por tempo, o que
// vier primeiro. É a mesma regra do `nishidb ingest` (§78,
// storage.checkpoint.interval), e não uma chave nova — o §48 fixa a ORDEM do
// fluxo, não a periodicidade.
type Cadencia struct {
	Eventos   int64
	Intervalo time.Duration
}

// Writer é a goroutine única de mutação do §27.
//
// Ninguém além dela chama Apply ou Checkpoint enquanto o consumer roda. O
// Materializer reusa patch/prevBuf/outBuf entre eventos e não é thread-safe:
// duas goroutines aqui corromperiam o payload em construção.
type Writer struct {
	cfg    *Config
	mat    *materializer.Materializer
	store  Store
	fila   *Fila
	log    *logging.Logger
	met    *metricas
	origem checkpoint.Origem
	cad    Cadencia

	// commit publica o offset no broker DEPOIS do checkpoint durável (§48).
	// É injetado pelo consumer para manter o writer testável sem broker.
	commit func(context.Context, map[int32]int64) error

	// offsets é a posição por partição: PRÓXIMO offset a consumir. Só a
	// goroutine do writer o toca.
	offsets map[int32]int64

	desdeCP  int64
	ultimoCP time.Time

	mu       sync.Mutex
	pausa    error
	desfecho Desfecho
}

// Desfecho resume o que a ingestão fez, para o resumo e para os testes.
type Desfecho struct {
	Eventos     int64
	Aplicados   int64
	Stale       int64
	Descartados int64
	Falhas      int64
}

// NovoWriter monta o writer.
func NovoWriter(cfg *Config, mat *materializer.Materializer, st Store, fila *Fila,
	log *logging.Logger, met *metricas, offsets map[int32]int64, cad Cadencia,
	commit func(context.Context, map[int32]int64) error,
) *Writer {
	if log == nil {
		log = logging.Descartado()
	}
	o := make(map[int32]int64, len(offsets))
	for p, v := range offsets {
		o[p] = v
	}
	return &Writer{
		cfg: cfg, mat: mat, store: st, fila: fila,
		log:      log.Component("kafka").Dataset(cfg.Dataset),
		met:      met,
		origem:   cfg.Origem(),
		cad:      cad,
		commit:   commit,
		offsets:  o,
		ultimoCP: time.Now(),
	}
}

// Pausa devolve o motivo da parada, ou nil se a ingestão segue.
func (w *Writer) Pausa() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pausa
}

// Desfecho devolve os contadores acumulados.
func (w *Writer) Desfecho() Desfecho {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.desfecho
}

// Roda drena a fila até o contexto acabar, a fila fechar ou a ingestão
// pausar. Devolve ErrPausado quando parou por política.
func (w *Writer) Roda(ctx context.Context) error {
	for {
		it, ok := w.fila.Recebe(ctx)
		if !ok {
			// Fila fechada ou contexto encerrado: torna durável o que já foi
			// aplicado antes de sair. Sem isto, um encerramento gracioso
			// jogaria fora até um intervalo inteiro de checkpoint.
			return w.finaliza(ctx)
		}
		if it.marca != nil {
			close(it.marca)
			continue
		}
		if err := w.aplica(ctx, it); err != nil {
			return err
		}
	}
}

func (w *Writer) aplica(ctx context.Context, it item) error {
	r := w.mat.Apply(it.rec.Value)
	w.conta(r)

	switch {
	case r.Fatal():
		// Writer em modo falho (sticky): reprocessar é inútil até o dataset
		// ser reaberto. O offset NÃO avança, então o reinício retoma
		// exatamente daqui.
		return w.para(ctx, fmt.Errorf("%w: writer em modo falho: %w", ErrPausado, r.Err),
			"writer.falho", it.rec)

	case r.Category.BadEvent():
		if w.cfg.InvalidEvent == schema.InvalidEventStop {
			return w.para(ctx, fmt.Errorf("%w: errors.invalidEvent=stop e o evento é %s: %w",
				ErrPausado, r.Category, r.Err), "evento.parou", it.rec)
		}
		// log-and-skip: o evento é ruim, a decisão de descartá-lo é do
		// operador, e o offset AVANÇA — não avançar aqui seria livelock.
		w.log.Warn("evento.descartado", "evento descartado pela política do §101",
			"categoria", r.Category.String(),
			"partition", it.rec.Partition, "offset", it.rec.Offset,
			"erro", r.Err.Error())
		w.avanca(it.rec)

	case r.Err != nil:
		// STORAGE_ERROR e INTERNAL_STATE: não é culpa do evento e não há
		// política que cubra. Descartar aqui avançaria o offset e a perda
		// seria definitiva e invisível; seguir sem avançar seria livelock.
		// A saída é PARAR de forma observável.
		return w.para(ctx, fmt.Errorf("%w: %s não é culpa do evento e não tem política: %w",
			ErrPausado, r.Category, r.Err), "storage.falhou", it.rec)

	default:
		w.avanca(it.rec)
	}
	return w.talvezCheckpoint(ctx)
}

func (w *Writer) conta(r materializer.ApplyResult) {
	w.mu.Lock()
	w.desfecho.Eventos++
	switch {
	case r.Action == materializer.ActionStale:
		w.desfecho.Stale++
	case r.Category.BadEvent():
		w.desfecho.Descartados++
	case r.Err != nil:
		w.desfecho.Falhas++
	default:
		w.desfecho.Aplicados++
	}
	w.mu.Unlock()

	w.met.eventos.Inc()
	switch {
	case r.Action == materializer.ActionStale:
		w.met.stale.Inc()
	case r.Category.BadEvent():
		w.met.invalidos.Inc()
	case r.Err != nil:
		w.met.falhas.Inc()
	}
}

// avanca move a posição da partição para o PRÓXIMO offset a consumir. Só é
// chamado para eventos DECIDIDOS — aplicados, stale ou descartados por
// política. Evento que parou a ingestão nunca avança.
func (w *Writer) avanca(rec *kgo.Record) {
	w.offsets[rec.Partition] = rec.Offset + 1
	w.desdeCP++
	w.met.offset(rec.Partition).Set(float64(rec.Offset + 1))
}

// para registra a pausa de forma observável e torna durável o que já entrou.
func (w *Writer) para(ctx context.Context, motivo error, evento string, rec *kgo.Record) error {
	// Checkpoint ANTES de parar: os eventos já aplicados têm de ficar
	// duráveis, e o offset persistido tem de apontar exatamente para o
	// evento problemático — é isso que permite ao operador vê-lo e decidir.
	if err := w.checkpoint(ctx); err != nil {
		w.log.Error("checkpoint.falhou", "checkpoint na parada falhou; o reinício reprocessa desde o último ponto durável",
			"erro", err.Error())
	}
	w.mu.Lock()
	if w.pausa == nil {
		w.pausa = motivo
	}
	w.mu.Unlock()
	w.met.bloqueado.Set(1)
	w.log.Error(evento, "ingestão PARADA; as consultas seguem de pé (§51). Use `nishidb offsets` para reposicionar",
		"partition", rec.Partition, "offset", rec.Offset, "motivo", motivo.Error())
	return w.Pausa()
}

func (w *Writer) talvezCheckpoint(ctx context.Context) error {
	if !precisaCheckpoint(w.cad, w.desdeCP, w.ultimoCP) {
		return nil
	}
	return w.checkpoint(ctx)
}

// checkpoint executa a ordem do §48 na parte que cabe ao writer:
//
//	persist → durable checkpoint → commit Kafka offset
//
// SetSourceOffsets antes do Checkpoint porque é o Checkpoint que grava o
// arquivo; o commit no broker por ÚLTIMO porque o §48 é explícito sobre isso
// e o bloco `Nunca:` proíbe a inversão. Falha de commit NÃO é fatal: o
// checkpoint local é autoritativo (§49) e o commit existe para lag e para
// terceiros.
func (w *Writer) checkpoint(ctx context.Context) error {
	if len(w.offsets) == 0 {
		return nil
	}
	o := checkpoint.SourceOffsets{Origem: w.origem, Offsets: w.offsets}
	if err := w.store.SetSourceOffsets(o); err != nil {
		return err
	}
	if err := w.store.Checkpoint(); err != nil {
		return err
	}
	w.desdeCP = 0
	w.ultimoCP = time.Now()

	if w.commit == nil {
		return nil
	}
	if err := w.commit(ctx, w.offsets); err != nil {
		w.met.errosKafka.Inc()
		w.log.Warn("offset.commit_falhou",
			"commit no broker falhou; o checkpoint local é autoritativo (§49) e o consumo não é afetado",
			"erro", err.Error())
	}
	return nil
}

// finaliza torna durável o que entrou antes de o writer sair.
func (w *Writer) finaliza(ctx context.Context) error {
	// Contexto próprio: o ctx do consumer já pode estar cancelado no
	// encerramento, e um checkpoint que não roda por causa disso jogaria
	// fora trabalho já aplicado.
	cctx, cancela := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancela()
	return w.checkpoint(cctx)
}

// precisaCheckpoint dispara por contagem OU por tempo, o que vier primeiro.
// Eventos == 0 desliga os dois.
func precisaCheckpoint(c Cadencia, desde int64, ultimo time.Time) bool {
	if c.Eventos <= 0 {
		return false
	}
	if desde >= c.Eventos {
		return true
	}
	return c.Intervalo > 0 && time.Since(ultimo) >= c.Intervalo
}
