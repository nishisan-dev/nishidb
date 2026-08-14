// Package server hospeda, num processo só, a ingestão Kafka e o motor de
// consulta (§78, §93, §94, §96).
//
// Os dois moram juntos por uma razão de mecanismo, não de gosto: o flock do
// dataset é EXCLUSIVO por processo (internal/storage/store_lock.go), então um
// consumer de longa duração num processo separado tornaria o dataset
// inconsultável — toda `nishidb query` sairia com exit 3 enquanto a ingestão
// estivesse de pé. Dentro do mesmo processo, o Store já é um writer com muitos
// leitores concorrentes, que é exatamente o modelo do §27.
//
// A ordem de subida é a do §51, e não é negociável: as consultas sobem ANTES
// de a ingestão retomar. Um dataset cuja ingestão não pode subir (posição
// desconhecida, origem divergente, assignment parcial) continua CONSULTÁVEL, e
// o problema aparece no /v1/health em vez de derrubar o processo.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// Estado de um subsistema no /v1/health (§94).
const (
	EstadoUP        = "UP"
	EstadoDown      = "DOWN"
	EstadoDegraded  = "DEGRADED"
	EstadoDesligado = "DISABLED"
)

// dataset é um dataset aberto no processo.
type dataset struct {
	nome string
	spec *schema.DatasetSpec
	st   *storage.Store
	mat  *materializer.Materializer

	// cons é nil quando o dataset não declara Kafka ou está com
	// `enabled: false` — o botão de pausa declarativo.
	cons *kafka.Consumidor
	// erroIngestao é a falha que impediu a ingestão de subir ou a parou. O
	// dataset continua consultável; é o health que reporta.
	erroIngestao error
	mu           sync.Mutex
}

func (d *dataset) falha(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.erroIngestao == nil {
		d.erroIngestao = err
	}
}

func (d *dataset) erro() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.erroIngestao
}

// Servidor é o processo de longa duração.
type Servidor struct {
	cfg      *config.Config
	log      *logging.Logger
	reg      *metrics.Registry
	eng      *query.Engine
	datasets []*dataset
	http     *http.Server
	ln       net.Listener
	pprofSrv *http.Server
}

// Opcoes configura a subida.
type Opcoes struct {
	Log *logging.Logger
	// PprofAddr, quando não vazio, sobe os endpoints de debug do §112 num
	// listener SEPARADO. Separado de propósito: o §112 diz "somente em modo
	// administrativo", e expor profiles na mesma porta das métricas os
	// tornaria alcançáveis por quem só deveria raspar /metrics.
	PprofAddr string
}

// Novo abre todos os datasets do datasets.directory e registra o que for
// consultável. NÃO sobe consumer nenhum: isso é do Roda, depois de as
// consultas estarem de pé (§51).
func Novo(cfg *config.Config, o Opcoes) (*Servidor, error) {
	log := o.Log
	if log == nil {
		log = logging.Descartado()
	}
	log = log.Component("server")

	reg := metrics.NewRegistry()
	metrics.RegistraProcesso(reg)

	nomes, err := cfg.ListaDatasets()
	if err != nil {
		return nil, err
	}
	if len(nomes) == 0 {
		return nil, fmt.Errorf("nenhum dataset em %s", cfg.Datasets.Directory)
	}

	// Carrega e valida TODAS as specs antes de abrir qualquer dataset: uma
	// colisão de groupId entre o primeiro e o último arquivo não pode ser
	// descoberta depois de meio processo já estar de pé.
	specs := make(map[string]*schema.DatasetSpec, len(nomes))
	for _, nome := range nomes {
		spec, err := schema.Load(cfg.SpecDoDataset(nome), cfg.DefaultsDeQuery())
		if err != nil {
			return nil, err
		}
		if spec.Name != nome {
			return nil, fmt.Errorf("dataset %q: o YAML declara name %q", nome, spec.Name)
		}
		specs[nome] = spec
	}
	if err := config.ValidaUnicidadeDeGroup(specs); err != nil {
		return nil, err
	}

	s := &Servidor{cfg: cfg, log: log, reg: reg, eng: query.NewEngine()}

	// O bind vem ANTES de abrir os datasets: descobrir porta ocupada depois
	// de já ter tomado o flock de N datasets é desperdício, e deixa o
	// operador com um processo morto que segurou os locks por um instante.
	if s.ln, err = net.Listen("tcp", cfg.Endereco()); err != nil {
		return nil, fmt.Errorf("server: escutar em %s: %w", cfg.Endereco(), err)
	}
	ok := false
	defer func() {
		if !ok {
			s.ln.Close()
			s.fechaDatasets()
		}
	}()

	for _, nome := range nomes {
		d, err := s.abreDataset(specs[nome], log)
		if err != nil {
			return nil, err
		}
		s.datasets = append(s.datasets, d)
	}
	ok = true
	return s, nil
}

func (s *Servidor) abreDataset(spec *schema.DatasetSpec, log *logging.Logger) (*dataset, error) {
	var bitmaps []string
	for nome, f := range spec.Schema.Fields {
		if f.Index == "bitmap" {
			bitmaps = append(bitmaps, nome)
		}
	}
	st, err := storage.Open(storage.Options{
		Dir:              s.cfg.CaminhoDoDataset(spec.Name),
		Dataset:          spec.Name,
		SegmentSize:      s.cfg.SegmentSize(),
		BitmapFieldNames: bitmaps,
		Log:              log,
		Metrics:          s.reg,
	})
	if err != nil {
		return nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	if err := s.eng.Register(spec.Name, compilado, st); err != nil {
		st.Close()
		return nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	return &dataset{
		nome: spec.Name,
		spec: spec,
		st:   st,
		mat:  materializer.New(compilado, st),
	}, nil
}

// Roda sobe o HTTP e os consumers, nessa ordem (§51), e bloqueia até o
// contexto acabar.
func (s *Servidor) Roda(ctx context.Context, pprofAddr string) error {
	if err := s.sobeHTTP(); err != nil {
		return err
	}
	if pprofAddr != "" {
		s.sobePprof(pprofAddr)
	}
	s.log.Info("query.pronta", "consultas disponíveis", "datasets", len(s.datasets), "endereco", s.cfg.Endereco())

	var wg sync.WaitGroup
	for _, d := range s.datasets {
		d := d
		if !s.ingestaoHabilitada(d) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.rodaIngestao(ctx, d)
		}()
	}

	<-ctx.Done()
	s.log.Info("encerrando", "sinal recebido; drenando a ingestão antes de fechar")
	wg.Wait()
	return s.encerra()
}

// ingestaoHabilitada aplica a decisão do D15: descrever o consumer não é
// ligá-lo.
func (s *Servidor) ingestaoHabilitada(d *dataset) bool {
	k := d.spec.Source.Kafka
	switch {
	case k == nil:
		return false
	case !k.Enabled:
		s.log.Info("ingestao.desligada",
			"o dataset declara source.kafka com enabled: false; nada é consumido",
			"dataset", d.nome)
		return false
	}
	return true
}

func (s *Servidor) rodaIngestao(ctx context.Context, d *dataset) {
	log := s.log.Dataset(d.nome)

	cfg, err := kafka.Resolve(d.nome, d.spec.Source.Kafka, d.spec.Errors)
	if err != nil {
		d.falha(err)
		log.Error("ingestao.nao_subiu", "a ingestão não pôde subir; as consultas seguem de pé (§51)",
			"erro", err.Error())
		return
	}
	cons, err := kafka.Abre(ctx, cfg, kafka.Opcoes{
		Store:   d.st,
		Mat:     d.mat,
		Log:     s.log,
		Metrics: s.reg,
		Cadencia: kafka.Cadencia{
			Eventos:   checkpointEventosPadrao,
			Intervalo: s.cfg.IntervaloDeCheckpoint(),
		},
	})
	if err != nil {
		d.falha(err)
		log.Error("ingestao.nao_subiu", "a ingestão não pôde subir; as consultas seguem de pé (§51)",
			"erro", err.Error())
		return
	}
	d.mu.Lock()
	d.cons = cons
	d.mu.Unlock()
	defer cons.Fecha()

	log.Info("ingestao.subiu", "consumindo", "topico", cfg.Topic, "group", cfg.GroupID)
	if err := cons.Roda(ctx); err != nil && !errors.Is(err, context.Canceled) {
		d.falha(err)
		log.Error("ingestao.parou", "a ingestão parou; as consultas seguem de pé (§51)",
			"erro", err.Error())
	}
}

// checkpointEventosPadrao é a mesma constante do `nishidb ingest`: o custo de
// não checkpointar é o heap acompanhar o tamanho do dataset.
const checkpointEventosPadrao = 500_000

func (s *Servidor) sobeHTTP() error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /v1/health", s.handleHealth)

	s.http = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.http.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http.falhou", "o servidor HTTP caiu", "erro", err.Error())
		}
	}()
	return nil
}

func (s *Servidor) encerra() error {
	ctx, cancela := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancela()
	var errs []error
	if s.http != nil {
		errs = append(errs, s.http.Shutdown(ctx))
	}
	if s.pprofSrv != nil {
		errs = append(errs, s.pprofSrv.Shutdown(ctx))
	}
	errs = append(errs, s.fechaDatasets())
	return errors.Join(errs...)
}

// fechaDatasets faz o quiesce de cada Store, que é o que devolve o flock.
func (s *Servidor) fechaDatasets() error {
	var errs []error
	for _, d := range s.datasets {
		if d.st != nil {
			errs = append(errs, d.st.Close())
		}
	}
	return errors.Join(errs...)
}

// Endereco é o endereço REAL do listener, que só é conhecido depois do bind.
func (s *Servidor) Endereco() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}
