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
// vier primeiro. É o mesmo gatilho do `nishidb ingest` e a MESMA chave (§78,
// storage.checkpoint.interval), e não uma chave nova — o §48 fixa a ORDEM do
// fluxo, não a periodicidade.
//
// Onde as duas regras divergem, e por quê: o needsCheckpoint do
// `internal/cli/ingest.go` cai para um padrão quando o intervalo é <= 0 e não
// tem guarda de "nenhum evento desde o último checkpoint". Ele pode se dar a
// esse luxo porque lê uma entrada FINITA do começo ao fim — só é consultado
// com um evento recém-lido na mão, e o processo termina. Aqui o laço é
// perpétuo e desperta sozinho (débito 56): o intervalo <= 0 tem de DESLIGAR o
// relógio em vez de adotar padrão nenhum, e o tique sem evento novo tem de ser
// barrado, sob pena de um dataset parado reescrever o manifest para sempre.
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
	cfg   *Config
	mat   *materializer.Materializer
	store Store
	// fila é a entrada do pipeline. O writer NÃO a consome: quem tira dela são
	// os decodificadores. Ele a mantém porque o Quiesce enfileira a barreira
	// nela, que é como a revogação entra na ordem total dos eventos.
	fila *Fila
	// est é a esteira de reordenação, a saída dos decodificadores e a única
	// entrada do writer. Com decodeWorkers=1 ela tem um produtor só, e ainda
	// assim está no caminho: um caminho de código, dimensionado diferente.
	est *esteira
	// limiarOfEncolhimento é o teto de memória retida por slot acima do qual os
	// buffers são soltos ao liberar o slot. Ver o godoc de slotThreshold.
	limiarOfEncolhimento int
	log                  *logging.Logger
	met                  *metricas
	source               checkpoint.Source
	cad                  Cadencia

	// commit publica o offset no broker DEPOIS do checkpoint durável (§48).
	// É injetado pelo consumer para manter o writer testável sem broker.
	commit func(context.Context, map[int32]int64) error

	// posMu protege offsets, desdeCP e ultimoCP.
	//
	// HISTÓRIA, porque ela explica por que o lock continua aqui. aoRevogar
	// chamava w.checkpoint DIRETO, da goroutine do group do franz-go, enquanto
	// o writer podia estar em talvezCheckpoint pelo despertador — a barreira
	// de fim de lote esvaziava a FILA, não parava a GOROUTINE. As duas liam e
	// escreviam o mapa de offsets em paralelo, e a detecção de escrita
	// concorrente em mapa é best-effort no runtime: quando pega, é
	// `fatal error: concurrent map read and map write`, throw não recuperável
	// que derruba o processo; quando não pega, corrompe o mapa em silêncio.
	//
	// Com o Quiesce, o checkpoint voltou a rodar SÓ na goroutine do writer:
	// quem pede o quiesce apenas enfileira uma barreira e espera. A corrida
	// deixou de existir por construção, e este lock passou a ser redundante.
	//
	// Ele fica assim mesmo, e não é dívida: um mutex não contendido custa
	// dezenas de nanossegundos por checkpoint — que acontece uma vez a cada 30
	// segundos —, e é o que impede que a próxima chamada de checkpoint vinda de
	// fora da goroutine do writer reintroduza o throw em silêncio. O godoc
	// acima é o aviso; este lock é a rede.
	//
	// É lock SEPARADO do mu de propósito. O checkpoint segura este durante o
	// fsync inteiro (centenas de ms), e o poller chama Pausa() a cada volta do
	// laço: se as duas coisas dividissem um mutex, o poller pararia junto com
	// cada checkpoint.
	posMu    sync.Mutex
	offsets  map[int32]int64
	desdeCP  int64
	ultimoCP time.Time

	mu       sync.Mutex
	pausa    error
	desfecho Desfecho

	// parado fecha quando Run retorna, e existe para o Quiesce.
	//
	// É o braço que impede o modo de falha mais perigoso desta mudança: um
	// writer que já saiu (por ErrPausado, por exemplo) nunca alcançaria a
	// barreira, e o Quiesce — que roda DENTRO de um callback do franz-go —
	// esperaria para sempre. O group cairia por callback travado, as partições
	// seriam revogadas e a ingestão morreria em silêncio.
	parado chan struct{}

	// iniciado fecha quando Run começa, e é o QUARTO caso do Quiesce.
	//
	// O plano da Fase 3 nomeou três: barreira pronta, writer que já saiu, e
	// contexto. Falta o writer que NUNCA subiu — e ele existe de verdade: o
	// group do franz-go pode revogar partições durante o próprio Abre, antes de
	// o Consumidor.Run ter lançado goroutine nenhuma. Nesse instante a fila
	// aceita a barreira (está aberta), ninguém a consome, `parado` não está
	// fechado porque Run não retornou, e o contexto que o franz-go passa ao
	// callback de revogação não está cancelado. O Quiesce espera para sempre,
	// dentro de um callback: o group cai, e o Close do cliente trava atrás dele.
	//
	// Medido: 2 travamentos em 12 execuções de
	// TestDatasetVazioSemOffsetsSobeNormalmente sob -race, contra 0 em 12 antes
	// da esteira. O buraco é ANTERIOR a ela — a Fase 3 já o tinha —, e a Fase 4
	// só o tornou provável ao mudar o tempo da subida.
	//
	// Sem writer, a resposta é nil e não é atalho: a garantia do quiesce é que
	// tudo o que entrou antes dele foi APLICADO e tornado durável, e nada foi
	// aplicado. Ela vale por vacuidade.
	iniciado chan struct{}
}

// Desfecho resume o que a ingestão fez, para o resumo e para os testes.
type Desfecho struct {
	Eventos     int64
	Aplicados   int64
	Stale       int64
	Descartados int64
	Falhas      int64
}

// NewWriter monta o writer.
func NewWriter(cfg *Config, mat *materializer.Materializer, st Store, fila *Fila,
	log *logging.Logger, met *metricas, offsets map[int32]int64, cad Cadencia,
	commit func(context.Context, map[int32]int64) error,
) *Writer {
	if log == nil {
		log = logging.Discard()
	}
	o := make(map[int32]int64, len(offsets))
	for p, v := range offsets {
		o[p] = v
	}
	return &Writer{
		cfg: cfg, mat: mat, store: st, fila: fila,
		est:                  newPipeline(cfg.MaxPollRecords),
		limiarOfEncolhimento: slotThreshold(cfg),
		log:                  log.Component("kafka").Dataset(cfg.Dataset),
		met:                  met,
		source:               cfg.Source(),
		cad:                  cad,
		commit:               commit,
		offsets:              o,
		ultimoCP:             time.Now(),
		parado:               make(chan struct{}),
		iniciado:             make(chan struct{}),
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

// Run drena a fila até o contexto acabar, a fila fechar ou a ingestão
// pausar. Devolve ErrPausado quando parou por política.
func (w *Writer) Run(ctx context.Context) error {
	// Antes de qualquer outra coisa: é este fechamento que autoriza o Quiesce a
	// esperar. Fechá-lo depois deixaria uma janela — estreita, e já basta — em
	// que a revogação vê o writer como "não iniciado" enquanto ele está subindo.
	close(w.iniciado)
	wake, paraDespertador := w.despertador()
	defer paraDespertador()
	// Fechar isto é o que destrava um Quiesce que chegou tarde demais.
	defer close(w.parado)
	// E isto destrava um decodificador esperando vaga na esteira. Sem ele, o
	// writer que sai por ErrPausado deixa K goroutinas presas e o encerramento
	// nunca termina — o supervisor mata o processo no meio de um checkpoint.
	defer w.est.abandona()

	for {
		s, ok, acordou := w.est.consome(ctx, wake)
		switch {
		case acordou:
			// Débito 56: sem evento novo o disparo por CONTAGEM nunca vem, e
			// o writer ficaria dormindo no channel com a posição consumida
			// nunca virando posição durável. O commit no broker vem depois do
			// checkpoint (§48), então quem olha o `nishidb_kafka_lag` lê meio
			// milhão de atraso num consumidor que não tem atraso nenhum.
			//
			// O despertar só AVALIA a regra — quem decide continua sendo o
			// needsCheckpoint.
			if err := w.talvezCheckpoint(ctx); err != nil {
				return err
			}
		case !ok:
			// Fila fechada ou contexto encerrado: torna durável o que já foi
			// aplicado antes de sair. Sem isto, um encerramento gracioso
			// jogaria fora até um intervalo inteiro de checkpoint.
			return w.finaliza(ctx)
		case s.barreira != nil:
			// Quiesce: tudo o que entrou na fila antes desta barreira já foi
			// aplicado, então é AQUI que a posição vira durável. Checkpoint
			// incondicional, e não talvezCheckpoint: quem pede o quiesce é a
			// revogação, e "ainda não deu a hora" não é resposta para ela.
			//
			// A barreira atravessa a esteira como um evento qualquer, com seq
			// próprio: é isso que garante que ela chegue ao writer DEPOIS de
			// tudo o que foi enfileirado antes dela, mesmo com K
			// decodificadores terminando fora de ordem.
			s.barreira.conclui(w.checkpoint(ctx))
			w.releaseSlot()
		default:
			err := w.aplica(ctx, s)
			w.releaseSlot()
			if err != nil {
				return err
			}
		}
	}
}

// despertador é o relógio que tira o writer do channel para reavaliar o
// checkpoint por tempo, com a função que o encerra.
//
// Sem cadência por tempo não há o que despertar, e o canal nil devolvido
// nesse caso deixa o select idêntico ao laço de antes — nenhum timer criado,
// nenhum wakeup a pagar.
func (w *Writer) despertador() (<-chan time.Time, func()) {
	if w.cad.Eventos <= 0 || w.cad.Intervalo <= 0 {
		return nil, func() {}
	}
	// Ticker, e não timer rearmado a cada checkpoint: o despertar é barato
	// (needsCheckpoint é aritmética) e um tique que chega cedo demais
	// simplesmente não faz nada. O custo de errar para o lado do timer seria
	// pior — esquecer um Reset num caminho de erro deixa o laço mudo de novo,
	// que é exatamente o débito que se está fechando.
	t := time.NewTicker(w.cad.Intervalo)
	return t.C, t.Stop
}

// releaseSlot devolve o slot ao anel e contabiliza o encolhimento.
//
// Sempre depois de aplicar, inclusive no caminho de erro: o slot preso seria
// uma vaga a menos para sempre, e com a janela cheia os decodificadores param.
func (w *Writer) releaseSlot() {
	if w.est.advance(w.limiarOfEncolhimento) {
		w.met.slotsEncolhidos.Inc()
	}
}

func (w *Writer) aplica(ctx context.Context, s *slot) error {
	it := Item{Rec: s.rec}
	// A medida cobre SÓ o Aplica. O talvezCheckpoint lá embaixo fica fora de
	// propósito: ele tem histograma próprio, e somá-lo aqui produziria um
	// bimodal cujo p99 é sempre o stall de checkpoint, escondendo a cauda que
	// o Aplica de fato tem.
	//
	// E cobre só a APLICAÇÃO, não o decode: com K decodificadores o tempo de
	// decode passa a ser concorrente, e somá-lo aqui faria o histograma medir
	// a soma de trabalhos paralelos como se fosse latência serial. O que este
	// número responde é "o writer é o gargalo?", e o writer é o Aplica.
	start := time.Now()
	r := w.mat.Aplica(&s.dec)
	w.met.aplicaSec.Observe(time.Since(start).Seconds())
	w.count(r)

	switch {
	case r.Fatal():
		// Writer em modo falho (sticky): reprocessar é inútil até o dataset
		// ser reaberto. O offset NÃO avança, então o reinício retoma
		// exatamente daqui.
		return w.para(ctx, fmt.Errorf("%w: writer em modo falho: %w", ErrPausado, r.Err),
			"writer.falho", it.Rec)

	case r.Category.BadEvent():
		if w.cfg.InvalidEventPolicy == schema.InvalidEventStop {
			return w.para(ctx, fmt.Errorf("%w: errors.invalidEvent=stop e o evento é %s: %w",
				ErrPausado, r.Category, r.Err), "evento.parou", it.Rec)
		}
		// log-and-skip: o evento é ruim, a decisão de descartá-lo é do
		// operador, e o offset AVANÇA — não avançar aqui seria livelock.
		w.log.Warn("evento.descartado", "evento descartado pela política do §101",
			"categoria", r.Category.String(),
			"partition", it.Rec.Partition, "offset", it.Rec.Offset,
			"erro", r.Err.Error())
		w.advance(it.Rec)

	case r.Err != nil:
		// STORAGE_ERROR e INTERNAL_STATE: não é culpa do evento e não há
		// política que cubra. Descartar aqui avançaria o offset e a perda
		// seria definitiva e invisível; seguir sem avançar seria livelock.
		// A saída é PARAR de forma observável.
		return w.para(ctx, fmt.Errorf("%w: %s não é culpa do evento e não tem política: %w",
			ErrPausado, r.Category, r.Err), "storage.falhou", it.Rec)

	default:
		w.advance(it.Rec)
	}
	return w.talvezCheckpoint(ctx)
}

func (w *Writer) count(r materializer.ApplyResult) {
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
		// O motivo é a categoria §101 do evento (ex.: EVENT_TOO_LARGE),
		// nomeada em nishidb_ingest_invalid_total desde a Task A5 — sem isso
		// o painel e um alerta só enxergavam um total misturado.
		w.met.invalido(r.Category.String()).Inc()
	case r.Err != nil:
		w.met.falhas.Inc()
	}
}

// advance move a posição da partição para o PRÓXIMO offset a consumir. Só é
// chamado para eventos DECIDIDOS — aplicados, stale ou descartados por
// política. Evento que parou a ingestão nunca avança.
func (w *Writer) advance(rec *kgo.Record) {
	w.posMu.Lock()
	w.offsets[rec.Partition] = rec.Offset + 1
	w.desdeCP++
	w.posMu.Unlock()
	w.met.positionOf(rec.Partition).Store(rec.Offset + 1)
}

// para registra a pausa de forma observável e torna durável o que já entrou.
func (w *Writer) para(ctx context.Context, motivo error, event string, rec *kgo.Record) error {
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
	w.log.Error(event, "ingestão PARADA; as consultas seguem de pé (§51). Use `nishidb offsets` para reposicionar",
		"partition", rec.Partition, "offset", rec.Offset, "motivo", motivo.Error())
	return w.Pausa()
}

// Quiesce drena o que já está na fila, torna durável e só então volta.
//
// É a barreira que o poller emitia a cada lote, movida para onde ela de fato é
// necessária: a revogação. A propriedade a preservar é que nenhum commit rode
// para uma partição depois de a revogação dela ter sido reconhecida — e isso
// exige drenar quando a revogação chega, não a cada volta do poll.
//
// A ordem por partição (§26) NÃO depende disto e nunca dependeu: ela vem de
// "uma FIFO, um consumidor". Está escrito aqui porque a barreira parecia
// sustentá-la, e sem o registro a próxima revisão a reintroduz "por segurança".
//
// Run dentro de um callback do franz-go, então NUNCA pode esperar para
// sempre: os três braços do select são a barreira, o writer que já saiu, e o
// contexto.
func (w *Writer) Quiesce(ctx context.Context) error {
	select {
	case <-w.iniciado:
	default:
		// O writer nunca subiu — a revogação chegou durante o Abre. Não há o
		// que drenar e não há quem drene: enfileirar a barreira aqui a deixaria
		// órfã na fila e o callback esperando por ela para sempre. Ver o godoc
		// de iniciado.
		return nil
	}

	b := newBarrier()
	if err := w.fila.EnviaBarreira(ctx, b); err != nil {
		if errors.Is(err, ErrFilaFechada) {
			// O consumidor está encerrando: o writer drena o que sobrou e faz
			// o checkpoint final por conta própria (ver finaliza). Não há o
			// que pedir, e insistir seria erro sobre um caminho saudável.
			return nil
		}
		return err
	}
	select {
	case <-b.pronto:
		return b.err
	case <-w.parado:
		// O writer encerrou antes de alcançar a barreira. Não há o que
		// drenar, e insistir travaria o callback.
		return w.Pausa()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Writer) talvezCheckpoint(ctx context.Context) error {
	w.posMu.Lock()
	precisa := needsCheckpoint(w.cad, w.desdeCP, w.ultimoCP)
	w.posMu.Unlock()
	if !precisa {
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
	// O lock cobre o corpo INTEIRO, e não só o acesso ao mapa. Duas razões:
	// aoRevogar e o despertador do writer podem chegar aqui ao mesmo tempo, e
	// deixá-los intercalar faria o SetSourceOffsets de um durabilizar sob o
	// Checkpoint do outro — a ordem do §48 é sobre a sequência inteira, não
	// sobre cada passo isolado.
	w.posMu.Lock()
	defer w.posMu.Unlock()

	if len(w.offsets) == 0 {
		return nil
	}
	// Cópia, e não a referência: o mapa atravessa duas fronteiras (o store, que
	// o guarda até gravar, e o commit no broker) e continuaria sendo mutado por
	// avanca assim que o lock saísse. Passar w.offsets direto manteria a corrida
	// viva do outro lado, onde este lock não alcança.
	snap := make(map[int32]int64, len(w.offsets))
	for p, v := range w.offsets {
		snap[p] = v
	}
	o := checkpoint.SourceOffsets{Source: w.source, Offsets: snap}
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
	if err := w.commit(ctx, snap); err != nil {
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
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	return w.checkpoint(cctx)
}

// needsCheckpoint dispara por contagem OU por tempo, o que vier primeiro.
// Eventos == 0 desliga os dois.
//
// `desde == 0` também desliga, e a guarda passou a importar quando o laço
// ganhou despertador (débito 56): sem evento decidido desde o último
// checkpoint não há posição nova para tornar durável nem offset novo para
// commitar, e um dataset parado reescreveria o manifest e daria fsync a cada
// intervalo, para sempre. Pelo caminho do evento a guarda é redundante —
// avanca() sempre incrementa antes —, e é por isso que ela não existia.
func needsCheckpoint(c Cadencia, desde int64, ultimo time.Time) bool {
	if c.Eventos <= 0 || desde <= 0 {
		return false
	}
	if desde >= c.Eventos {
		return true
	}
	return c.Intervalo > 0 && time.Since(ultimo) >= c.Intervalo
}
