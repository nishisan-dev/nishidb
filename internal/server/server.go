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

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
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
	// sch é a spec COMPILADA, a mesma que foi registrada no motor. Fica aqui
	// porque /v1/metadata a descreve campo a campo, e recompilá-la a cada
	// requisição seria trabalho repetido sobre um dado imutável.
	sch *schema.Compiled
	st  *storage.Store
	mat *materializer.Materializer

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

// Servidor é o processo de longa duração.
type Servidor struct {
	cfg      *config.Config
	log      *logging.Logger
	reg      *metrics.Registry
	eng      *query.Engine
	datasets []*dataset
	// porNome resolve o dataset de uma requisição. É construído na subida e
	// só lido depois: nenhum dataset entra ou sai com o processo de pé, então
	// não precisa de lock.
	porNome map[string]*dataset
	met     *metricasDeQuery
	// vagas é o teto de consultas simultâneas do §80. Um canal com capacidade
	// fixa, e não um contador: a aquisição não-bloqueante é o que permite
	// REJEITAR na hora (§81) em vez de enfileirar — uma fila de espera
	// seguraria conexões e esconderia a saturação de quem lê o /metrics.
	vagas    chan struct{}
	http     *http.Server
	ln       net.Listener
	pprofSrv *http.Server

	// credenciais é o cache imutável de server.authentication.users já
	// resolvido contra o ambiente (§74-77, resolveCredenciais em
	// seguranca.go): username -> digest SHA-256 da senha + papel. Nil quando
	// AutenticacaoHabilitada() é false — é o que comAutenticacao
	// (autenticacao.go) lê para decidir entre o middleware de verdade e o
	// passthrough puro.
	credenciais map[string]credencial
	// authFalhas conta nishidb_auth_failures_total por razão (Task B3). Só é
	// incrementado quando o middleware de fato roda — com credenciais nil o
	// wrapper nunca chama recusaAutenticacao, então a série simplesmente não
	// aparece no scrape (comportamento igual ao de qualquer família lazy
	// deste registry).
	authFalhas *metrics.FamiliaContador
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

	// server.enabled tem leitor. Aceitar a chave e ignorá-la seria repetir,
	// dentro do próprio conserto, o defeito que a issue #1 denuncia: o
	// operador escreve `enabled: false`, o servidor sobe assim mesmo, e a
	// configuração mente.
	if !cfg.ServidorHabilitado() {
		return nil, fmt.Errorf("server.enabled é false em %s: o servidor não sobe com a chave desligada", cfg.Datasets.Directory)
	}

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

	// A segurança do §74-78 é resolvida ANTES do bind: falha de credencial,
	// de certificado, ou a matriz de recusa do §77 não podem deixar um
	// listener aberto para trás. A ordem é a mais barata primeiro — a matriz
	// é só leitura de config e não faz I/O — para não gastar em ler env var
	// ou arquivo de senha uma subida que já vai ser recusada pela topologia.
	if err := avaliaExposicaoEAutenticacao(cfg, log); err != nil {
		return nil, err
	}
	if err := validaCertificadoNaSubida(cfg); err != nil {
		return nil, err
	}
	credenciais, err := resolveCredenciais(cfg)
	if err != nil {
		return nil, err
	}
	for _, chave := range cfg.ChavesDeRuntimeIgnoradas() {
		log.Warn("config.sem_efeito",
			"a chave é aceita e NÃO tem implementação nesta versão; não dimensione a máquina por ela",
			"chave", chave)
	}
	for _, chave := range cfg.ChavesDeStorageIgnoradas() {
		log.Warn("config.sem_efeito",
			"a chave é aceita e NÃO tem implementação nesta versão; a janela real de durabilidade é storage.checkpoint.interval",
			"chave", chave)
	}

	s := &Servidor{
		cfg:         cfg,
		log:         log,
		reg:         reg,
		eng:         query.NewEngine(),
		porNome:     make(map[string]*dataset, len(nomes)),
		vagas:       make(chan struct{}, cfg.MaxConcurrent()),
		credenciais: credenciais,
	}
	s.met = registraMetricasDeQuery(reg, s.eng)
	s.authFalhas = registraMetricasDeAuth(reg)

	// O bind vem ANTES de abrir os datasets: descobrir porta ocupada depois
	// de já ter tomado o flock de N datasets é desperdício, e deixa o
	// operador com um processo morto que segurou os locks por um instante.
	if s.ln, err = net.Listen("tcp", cfg.Endereco()); err != nil {
		return nil, fmt.Errorf("server: escutar em %s: %w", cfg.Endereco(), err)
	}
	ok := false
	defer func() {
		if ok {
			return
		}
		// Caminho de erro: o que importa é não vazar o listener nem os
		// flocks. Uma falha ao fechar aqui não tem a quem ser reportada — o
		// erro que interessa é o que abortou a subida.
		if err := s.ln.Close(); err != nil {
			log.Warn("listener.close", "falha ao fechar o listener na subida abortada", "erro", err.Error())
		}
		if err := s.fechaDatasets(); err != nil {
			log.Warn("datasets.close", "falha ao fechar datasets na subida abortada", "erro", err.Error())
		}
	}()

	for _, nome := range nomes {
		d, err := s.abreDataset(specs[nome], log)
		if err != nil {
			return nil, err
		}
		s.datasets = append(s.datasets, d)
		s.porNome[d.nome] = d
	}
	// Depois do loop: o coletor percorre a lista pronta, e registrá-lo antes
	// exporia um scrape com metade dos datasets durante a subida.
	registraMetricasDeDataset(reg, s)
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
		return nil, fechandoStore(st, fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	if err := s.eng.Register(spec.Name, compilado, st); err != nil {
		return nil, fechandoStore(st, fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	return &dataset{
		nome: spec.Name,
		spec: spec,
		sch:  compilado,
		st:   st,
		mat:  materializer.New(compilado, st),
	}, nil
}

// fechandoStore devolve o erro original, fechando o Store antes. O erro do
// Close é agregado e nunca substitui a causa: quem lê precisa do motivo de a
// abertura ter falhado, não do que aconteceu na limpeza.
func fechandoStore(st *storage.Store, causa error) error {
	if err := st.Close(); err != nil {
		return errors.Join(causa, err)
	}
	return causa
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
	s.log.Info("query.pronta", "consultas disponíveis",
		"datasets", len(s.datasets), "endereco", s.cfg.Endereco(), "versao", buildinfo.String())
	s.ligaCompaction(ctx)

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

// ligaCompaction sobe a goroutine do §53-55 em cada dataset, a partir da
// política do §78 lida pela Task 3.
//
// Depois do HTTP e ANTES da ingestão, que é a ordem do §51 aplicada a mais um
// consumidor de disco: quem sobe primeiro é quem responde consulta. E a
// compaction precisa estar de pé antes do consumer justamente porque é o
// consumer que produz o garbage — ligá-la depois abriria uma janela em que o
// dataset cresce sem ninguém devolvendo nada.
//
// O contexto é o do Roda, então o SIGTERM cancela as goroutinas junto com a
// ingestão. O Close de cada Store as aguarda de novo (decisão D7): as duas
// coisas não se repetem à toa — o cancelamento é o pedido, e o join é o que
// garante que ninguém está renomeando arquivo quando o dataset fecha.
//
// A ausência tem log próprio de propósito. `enabled: false` é uma decisão do
// operador, e um processo que a honra em silêncio é indistinguível de um que a
// ignora — que é o defeito que a issue #1 denuncia, em outra chave.
func (s *Servidor) ligaCompaction(ctx context.Context) {
	habilitada, ratio := s.cfg.Compaction()
	if !habilitada {
		s.log.Warn("compaction.desligada",
			"storage.compaction.enabled é false: nada devolve o disco dos updates, e o §86 projeta 30-43 GB/dia escritos contra 5,6-8 GB vivos a 500 ev/s")
		return
	}
	for _, d := range s.datasets {
		d.st.IniciaCompaction(ctx, ratio)
	}
	s.log.Info("compaction.ligada",
		"a compaction do §53-55 está de pé", "datasets", len(s.datasets), "minimumLiveRatio", ratio)
}

// checkpointEventosPadrao é a mesma constante do `nishidb ingest`: o custo de
// não checkpointar é o heap acompanhar o tamanho do dataset.
const checkpointEventosPadrao = 500_000

// sobeHTTP é o ÚNICO ponto de entrada do listener principal — a costura do
// §77 que a B4 herda: montaMux constrói o mux CRU (sem autenticação nenhuma),
// e comAutenticacao é o wrapper único em torno dele (autenticacao.go). A B4
// serve o mux cru, sem este wrapper, no listener loopback — o mesmo *rt.mux,
// dois caminhos de acesso.
func (s *Servidor) sobeHTTP() error {
	rt := s.montaMux()

	s.http = &http.Server{
		Handler:           s.comAutenticacao(rt),
		ReadHeaderTimeout: 10 * time.Second,
		// SEM WriteTimeout de propósito: ele é teto de CONEXÃO, e mataria um
		// stream longo legítimo no meio. O teto de uma consulta é o
		// query.timeout do dataset, aplicado pelo executor via contexto — o
		// único que sabe distinguir "demorou porque tem muito resultado" de
		// "travou".
	}
	go func() {
		if err := s.http.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http.falhou", "o servidor HTTP caiu", "erro", err.Error())
		}
	}()
	return nil
}

// Fecha libera o listener e os datasets sem ter rodado.
//
// Existe porque o Novo já TOMA recursos — o listener e o flock de cada
// dataset — e até aqui só o Roda os devolvia: um chamador que montasse o
// servidor e decidisse não subi-lo deixava os datasets travados até o processo
// morrer. É idempotente.
func (s *Servidor) Fecha() error { return s.encerra() }

func (s *Servidor) encerra() error {
	ctx, cancela := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancela()
	var errs []error
	if s.http != nil {
		errs = append(errs, s.http.Shutdown(ctx))
	} else if s.ln != nil {
		// Sem Roda, o listener nunca foi entregue ao http.Server: fechá-lo é
		// responsabilidade daqui.
		errs = append(errs, s.ln.Close())
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
