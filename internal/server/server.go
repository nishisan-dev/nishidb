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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/alarm"
	"github.com/nishisan-dev/nishidb/internal/alarmtable"
	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/countermetrics"
	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metakafka"
	"github.com/nishisan-dev/nishidb/internal/metausers"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/budget"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/series"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/users"
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

	// irma é a tabela histórica deste dataset, ou nil. Ela é um dataset de
	// pleno direito para /v1/metadata, /v1/health e as métricas — o que ela
	// não tem é consumer próprio, compaction e coletor de expiração.
	irma *dataset
	// ehHistorico marca a própria tabela irmã.
	ehHistorico bool

	// alarms é o observador dos alarmes declarados nos contadores, ou nil
	// quando nenhum contador declara `alarms:`. Mora AQUI, e não no Store,
	// porque o enriquecimento de um evento precisa do motor de consulta — que
	// o Store não conhece, e nem deve.
	alarms *alarm.Runner

	// emitters são as emissões periódicas declaradas nos contadores, uma por
	// contador com bloco `emit`. Moram aqui pelo mesmo motivo dos alarmes: o
	// laço vive fora do Store, e é o servidor que o para antes do quiesce.
	emitters []*series.Emitter

	// metrics é o amostrador das métricas de janela dos contadores, ou nil
	// quando nenhum contador declara `metrics:`. Mora aqui pelo mesmo motivo
	// dos alarmes e das emissões: o laço tem relógio, vive fora do Store, e é o
	// servidor que o para antes do quiesce.
	metrics *countermetrics.Sampler

	// cons é nil quando o dataset não declara Kafka ou está com
	// `enabled: false` — o botão de pausa declarativo.
	cons *kafka.Consumidor
	// erroIngestao é a falha que impediu a ingestão de subir ou a parou. O
	// dataset continua consultável; é o health que reporta.
	erroIngestao error
	// trips e desistiu são as DUAS séries do watchdog, criadas em runIngestion
	// e guardadas aqui para que a `_meta.kafka` as leia. São os MESMOS handles
	// que alimentam nishidb_kafka_watchdog_{trips,desistiu} — a série É o
	// armazenamento, e um contador ao lado seria a segunda fonte que este
	// projeto trata como defeito.
	//
	// Nil enquanto runIngestion não começou, e nil para sempre num dataset sem
	// ingestão. É a resposta certa: série de watchdog num dataset que não
	// consome afirmaria "existe um watchdog e ele nunca disparou".
	trips    *metrics.CounterSeries
	desistiu *metrics.GaugeSeries
	mu       sync.Mutex
}

// consumidorCorrente devolve o consumidor de AGORA, sob o MESMO lock que o
// /v1/stats e o /metrics usam.
//
// O nil é EXPLÍCITO: um `return d.cons` direto entregaria um
// (*kafka.Consumidor)(nil) embrulhado numa interface NÃO-nil, e quem a
// recebesse chamaria Instantaneo sobre ele — panic num caminho de consulta,
// escondendo "a ingestão ainda não subiu".
func (d *dataset) consumidorCorrente() metakafka.Consumido {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cons == nil {
		return nil
	}
	return d.cons
}

// vigiaCorrente lê as duas séries do watchdog. ok=false enquanto não há
// goroutine de ingestão — e aí as colunas saem NULL, porque "não há watchdog"
// e "nunca recriou" são afirmações diferentes.
func (d *dataset) vigiaCorrente() (trips uint64, desistiu, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.trips == nil {
		return 0, false, false
	}
	return d.trips.Value(), d.desistiu.Value() == 1, true
}

func (d *dataset) falha(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.erroIngestao == nil {
		d.erroIngestao = err
	}
}

// Server é o processo de longa duração.
type Server struct {
	cfg      *config.Config
	log      *logging.Logger
	reg      *metrics.Registry
	eng      *query.Engine
	datasets []*dataset
	// byNome resolve o dataset de uma requisição. É construído na subida e
	// só lido depois: nenhum dataset entra ou sai com o processo de pé, então
	// não precisa de lock.
	byNome map[string]*dataset
	// bySchema resolve o handle da CONEXÃO POR SCHEMA: o dataset base de cada
	// schema declarado. A irmã do histórico herda o schema do base e NÃO entra
	// aqui — é o base quem responde pelo atalho `_ns.x` da conexão. Mesmo
	// ciclo de vida do byNome, sem lock.
	bySchema map[string]*dataset
	met      *metricsOfQuery
	// vagas é o teto de consultas simultâneas do §80. Um canal com capacidade
	// fixa, e não um contador: a aquisição não-bloqueante é o que permite
	// REJEITAR na hora (§81) em vez de enfileirar — uma fila de espera
	// seguraria conexões e esconderia a saturação de quem lê o /metrics.
	vagas chan struct{}
	// vagasLongas é o SUB-POOL do §80 para a consulta que pediu prazo acima do
	// `query.timeout` do dataset. Vive DENTRO de vagas: quem entra aqui já
	// tomou uma vaga geral, e por isso o config recusa maxLongRunning acima do
	// maxConcurrent. Sem ele, o prazo longo por padrão faria maxConcurrent
	// relatórios ocuparem o semáforo inteiro e toda consulta interativa
	// receber 503 pelo tempo que eles durassem.
	vagasLongas chan struct{}
	// inVoo é o registro de QUAIS consultas estão correndo, ao lado do medidor
	// que diz quantas. Ver inflight.go.
	inVoo *inVoo
	// budget é o teto de memória de consulta do §80
	// (runtime.memoryTarget), do qual cada requisição toma uma fatia limitada
	// por runtime.query.maxBufferPerQuery. É do PROCESSO pela mesma razão do
	// semáforo: o §82 diz que todos os datasets dividem o mesmo orçamento, e um
	// por dataset multiplicaria o pior caso pelo número deles.
	budget   *budget.Global
	http     *http.Server
	ln       net.Listener
	pprofSrv *http.Server

	// users é a tabela de usuários do servidor (issue #40, internal/users),
	// aberta antes do bind com o flock de <storage.path>/_system. Nil quando
	// AuthenticationEnabled() é false — é o que withAuth (auth.go) lê para
	// decidir entre o middleware de verdade e o passthrough puro.
	users *users.Store
	// authFalhas conta nishidb_auth_failures_total por razão (Task B3). Só é
	// incrementado quando o middleware de fato roda — com users nil o
	// wrapper nunca chama rejectAuth, então a série simplesmente não
	// aparece no scrape (comportamento igual ao de qualquer família lazy
	// deste registry).
	authFalhas *metrics.CounterFamily

	// tlsCert é o par certificado/chave já CARREGADO por
	// validateCertificateAtStartup (security.go), nil quando server.tls está
	// desligado. risesHTTP o REUSA no ServeTLS — nunca relido do disco depois
	// da subida; rotação de certificado é reiniciar o processo.
	tlsCert *tls.Certificate

	// loopbackLn é o listener do plano loopback (decisão 4 do plano do
	// ciclo), bindado em Novo() junto do listener principal — mesma razão do
	// bind cedo de s.ln: descobrir porta ocupada depois de já ter tomado o
	// flock de N datasets é desperdício. Nil quando server.loopback.enabled é
	// false (default): nenhum listener a mais sobe.
	loopbackLn net.Listener
	// loopbackSrv é o SEGUNDO http.Server que serve o mux CRU (rt.Raw(), sem
	// autenticação nem TLS) sobre loopbackLn — o que preserva painel, script
	// de update e cron locais funcionando sem credencial mesmo com
	// server.authentication ligado no listener exposto.
	loopbackSrv *http.Server

	// vidaCtx é o MESMO ctx que Run recebe — o que signal.NotifyContext
	// cancela no SIGTERM (cmd/nishidb/main.go). handleCompact (mutations.go)
	// o compõe com r.Context() via ctxOfMutacao, porque http.Server.Shutdown
	// NÃO cancela o contexto de uma requisição em voo — só espera o handler
	// devolver, e por um tempo limitado (o ctx de 15s de encerra()). Sem
	// esta composição, uma passada manual de compaction (POST /v1/compact)
	// em voo no encerramento ignora o SIGTERM e só para quando processa
	// todos os candidatos ou o cliente desiste — e o compInVoo.Wait() do
	// Close (storage/compaction.go, ForCompaction) fica sem teto esperando
	// por ela, minutos em produção, até o systemd desistir e mandar SIGKILL.
	// Nil só em testes que constroem o handler sem passar por Run —
	// ctxOfMutacao cai para o contrato antigo (só o cliente decide) nesse
	// caso, o que preserva o comportamento anterior a esta composição.
	vidaCtx context.Context

	// now é o relógio dos laços que medem tempo por conta própria — alarmes e
	// amostrador das métricas de janela. Nil em produção, que eles tratam como
	// time.Now (ver Options.Now).
	now func() time.Time
}

// Options configura a subida.
type Options struct {
	Log *logging.Logger
	// PprofAddr, quando não vazio, sobe os endpoints de debug do §112 num
	// listener SEPARADO. Separado de propósito: o §112 diz "somente em modo
	// administrativo", e expor profiles na mesma porta das métricas os
	// tornaria alcançáveis por quem só deveria raspar /metrics.
	PprofAddr string
	// Now é o relógio dos alarmes e do amostrador das métricas de janela.
	// Exclusivo de teste, no molde do alarm.Options.Now e do
	// countermetrics.Options.Now: a prova de ponta a ponta de uma janela de 24 h
	// e de um prazo de sustentação precisa avançar o tempo sem dormir. Não tem
	// chave de configuração de propósito, e nil é time.Now.
	Now func() time.Time
}

// Novo abre todos os datasets do datasets.directory e registra o que for
// consultável. NÃO sobe consumer nenhum: isso é do Run, depois de as
// consultas estarem de pé (§51).
func Novo(cfg *config.Config, o Options) (*Server, error) {
	log := o.Log
	if log == nil {
		log = logging.Discard()
	}
	log = log.Component("server")

	reg := metrics.NewRegistry()
	metrics.RegisterProcess(reg)

	// server.enabled tem leitor. Aceitar a chave e ignorá-la seria repetir,
	// dentro do próprio conserto, o defeito que a issue #1 denuncia: o
	// operador escreve `enabled: false`, o servidor sobe assim mesmo, e a
	// configuração mente.
	if !cfg.ServerEnabled() {
		return nil, fmt.Errorf("server.enabled é false em %s: o servidor não sobe com a chave desligada", cfg.Datasets.Directory)
	}

	// Carrega e valida TODAS as specs antes de abrir qualquer dataset: uma
	// colisão de groupId ou de schemaName entre o primeiro e o último arquivo
	// não pode ser descoberta depois de meio processo já estar de pé.
	//
	// A carga é do config, e não daqui, porque as duas regras de conjunto
	// valem para o diretório e não para o processo: o `nishidb ingest` e o
	// `nishidb query` leem o MESMO diretório e entram pela MESMA porta.
	specs, err := cfg.LoadDatasetSpecs()
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("nenhum dataset em %s", cfg.Datasets.Directory)
	}
	names := make([]string, 0, len(specs))
	for name := range specs {
		names = append(names, name)
	}
	// Alfabética, como a do ListDatasets: a ordem de abertura dos datasets é
	// a ordem em que os erros de subida aparecem, e ela tem de ser a mesma em
	// dois processos com a mesma configuração.
	sort.Strings(names)

	if err := validateHistoryNames(specs); err != nil {
		return nil, err
	}
	avisaChavesWithoutEngine(specs, log)

	// A segurança do §74-78 é resolvida ANTES do bind: falha de credencial,
	// de certificado, de porta loopback, ou a matriz de recusa do §77 não
	// podem deixar um listener aberto para trás. A ordem é a mais barata
	// primeiro — matriz e endereço loopback são só leitura de config, sem
	// I/O — para não gastar em ler env var, arquivo de senha ou certificado
	// uma subida que já vai ser recusada pela topologia.
	if err := evaluateExposicaoEAuth(cfg, log); err != nil {
		return nil, err
	}
	loopbackAddr, err := loopbackAddress(cfg)
	if err != nil {
		return nil, err
	}
	cert, err := validateCertificateAtStartup(cfg)
	if err != nil {
		return nil, err
	}
	if cert != nil {
		// Aviso de validade do leaf (Task B4): nunca recusa a subida, só
		// registra WARN/ERRO estruturado e publica a gauge de expiração —
		// ver o comentário de avisaValidadeOfCert (security.go).
		if err := avisaValidadeOfCert(*cert, log, reg); err != nil {
			return nil, err
		}
	}
	// A tabela de usuários abre antes do bind, pela mesma razão do resto da
	// segurança: tabela ilegível ou já aberta por outro processo recusa a
	// subida sem listener nenhum aberto. Na primeira subida é aqui que nasce a
	// credencial de bootstrap.
	var userTable *users.Store
	if cfg.AuthenticationEnabled() {
		userTable, err = users.Open(users.Dir(cfg.Storage.Path), users.Options{Logger: log.Component("users")})
		if err != nil {
			return nil, err
		}
	}
	for _, key := range cfg.IgnoredRuntimeKeys() {
		log.Warn("config.sem_efeito",
			"a chave é aceita e NÃO tem implementação nesta versão; não dimensione a máquina por ela",
			"chave", key)
	}
	for _, key := range cfg.IgnoredStorageKeys() {
		log.Warn("config.sem_efeito",
			"a chave é aceita e NÃO tem implementação nesta versão; a janela real de durabilidade é storage.checkpoint.interval",
			"chave", key)
	}

	s := &Server{
		cfg:         cfg,
		log:         log,
		reg:         reg,
		eng:         query.NewEngine(),
		byNome:      make(map[string]*dataset, len(names)),
		bySchema:    make(map[string]*dataset, len(names)),
		vagas:       make(chan struct{}, cfg.MaxConcurrent()),
		vagasLongas: make(chan struct{}, cfg.MaxLongRunning()),
		inVoo:       newInVoo(),
		budget:      budget.NewGlobal(cfg.MemoryTarget()),
		users:       userTable,
		tlsCert:     cert,
		now:         o.Now,
	}
	s.met = registerMetricsOfQuery(reg, s.eng, s.inVoo, s.budget)
	s.authFalhas = registerMetricsOfAuth(reg)

	// O bind vem ANTES de abrir os datasets: descobrir porta ocupada depois
	// de já ter tomado o flock de N datasets é desperdício, e deixa o
	// operador com um processo morto que segurou os locks por um instante.
	// Os DOIS listeners (principal e loopback, quando ligado) bindam aqui,
	// pela mesma razão — a falha do segundo não pode deixar o primeiro
	// vazado, por isso o defer de limpeza abaixo cobre os dois.
	if s.ln, err = net.Listen("tcp", cfg.Addr()); err != nil {
		s.closeUsers(log)
		return nil, fmt.Errorf("server: escutar em %s: %w", cfg.Addr(), err)
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
		if s.loopbackLn != nil {
			if err := s.loopbackLn.Close(); err != nil {
				log.Warn("loopback.close", "falha ao fechar o listener loopback na subida abortada", "erro", err.Error())
			}
		}
		// Os amostradores dos datasets já abertos fecham ANTES dos Stores, pela
		// mesma ordem do encerra(): seguram mmap próprio e leem os contadores
		// pelo campo do Store.
		s.forCounterMetrics()
		if err := s.closeDatasets(); err != nil {
			log.Warn("datasets.close", "falha ao fechar datasets na subida abortada", "erro", err.Error())
		}
		s.closeUsers(log)
	}()

	if loopbackAddr != "" {
		if s.loopbackLn, err = net.Listen("tcp", loopbackAddr); err != nil {
			return nil, fmt.Errorf("server: escutar loopback em %s: %w", loopbackAddr, err)
		}
	}

	for _, name := range names {
		d, err := s.openDataset(specs[name], log)
		if err != nil {
			return nil, err
		}
		s.datasets = append(s.datasets, d)
		s.byNome[d.nome] = d
		s.bySchema[d.sch.SchemaName] = d
		// A irmã entra na lista logo depois do base, e não numa lista à parte:
		// /v1/metadata, /v1/health e as métricas por dataset a tratam como
		// qualquer outra, e é isso que a faz aparecer na árvore do DBeaver sem
		// nenhuma mudança no driver.
		if d.irma != nil {
			s.datasets = append(s.datasets, d.irma)
			s.byNome[d.irma.nome] = d.irma
		}
	}
	// Depois do loop: o coletor percorre a lista pronta, e registrá-lo antes
	// exporia um scrape com metade dos datasets durante a subida.
	registerMetricsOfDataset(reg, s)
	registerMetricsOfAlarms(reg, s)
	registerMetricsOfCounterMetrics(reg, s)
	ok = true
	return s, nil
}

func (s *Server) openDataset(spec *schema.DatasetSpec, log *logging.Logger) (*dataset, error) {
	dir := s.cfg.DatasetRootDir(spec.Name)
	indexados := schema.IndexedFieldNames(spec)
	// O schema é compilado DENTRO do Open, pelo callback dos contadores: as
	// definições precisam do schema, e o replay da recovery precisa das
	// definições. É o mesmo Compile de sempre, no mesmo dicionário — só que
	// antes do replay.
	comp := counters.NewCompiler(spec)
	st, err := storage.Open(storage.Options{
		Dir:                             dir,
		Dataset:                         spec.Name,
		SegmentSize:                     s.cfg.SegmentSize(),
		IndexedFieldsByName:             indexados,
		DerivedColumnNames:              spec.DerivedColumnNames(),
		Log:                             log,
		Metrics:                         s.reg,
		ActiveTTL:                       len(spec.Materialization.TTLRules) > 0,
		FallbackDuration:                spec.Materialization.FallbackDuration,
		WriteInstantFieldName:           schema.WriteInstantFieldName,
		TTLFieldName:                    schema.TTLFieldName,
		SnapshotMaxBytes:                s.cfg.SnapshotMaxBytes(),
		CompileCountersCallbackFunction: comp.Callback(),
	})
	if err != nil {
		return nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	compilado := comp.Compiled()
	if compilado == nil {
		return nil, closingStore(st, fmt.Errorf("dataset %q: o Open não compilou o schema", spec.Name))
	}
	// O contador de descarte diz QUANTO; o log diz por ONDE começar. Uma vez
	// por dataset: sob a política `ignore` o descarte é o estado normal, e uma
	// linha por evento afogaria o log.
	compilado.OnFieldDiscarded(func(field string) {
		log.Warn("schema.teto_de_campos",
			"o dataset alcançou schema.maxCols; campos novos deixaram de virar coluna",
			"campo", field, "maxCols", compilado.MaxCols, "politica", compilado.OnMaxCols)
	})
	// Divergência de tipo em coluna aninhada não descarta o evento, então ela
	// não aparece em lugar nenhum da contabilidade de ingestão. Uma vez por
	// dataset, com os DOIS tipos: só com eles o operador sabe o que mudou.
	compilado.OnIncompatibleValue(func(column, fixado, veio string) {
		log.Warn("schema.tipo_aninhado_divergente",
			"valor não coube no tipo fixado da coluna aninhada; a coluna é removida nessa linha e o documento guarda o valor",
			"coluna", column, "tipoFixado", fixado, "tipoQueVeio", veio)
	})
	if regras := spec.Materialization.TTLRules; len(regras) > 0 {
		// O operador precisa poder conferir que o YAML que escreveu é o que o
		// processo entendeu — inclusive a ORDEM, que aqui é semântica.
		log.Info("ttl.regras",
			"regras de expiração carregadas",
			"dataset", spec.Name, "total", len(regras),
			"temFundo", spec.Materialization.FallbackDuration > 0,
			"regras", schema.DescribeRules(regras))
	}
	mat, err := materializer.New(compilado, st)
	if err != nil {
		// Regra de TTL que não compila derruba a SUBIDA do dataset: um
		// servidor que aceitasse a regra e a ignorasse expiraria pelo prazo
		// errado, em silêncio.
		return nil, closingStore(st, fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// O decode paralelo é somente-leitura, e três coisas o mandam de volta ao
	// writer: coluna a criar, tipo a fixar, e documento com forma divergente
	// (que precisa enumerar as colunas do pai com o schema já aplicado). As duas
	// primeiras acabam num dataset saudável — descoberta termina. Reprocesso
	// seguido que NÃO faz o dicionário crescer não acaba, e a mensagem nomeia as
	// três causas em vez de afirmar uma: o caso conhecido é o teto de campos sob
	// `ignore`, mas uma origem que mande `null` no documento com frequência
	// produz o mesmo sintoma sem defeito nenhum. Uma vez por dataset, como os
	// avisos irmãos acima.
	mat.AoReprocessar(func(seguidos int64, columns int) {
		log.Warn("ingest.reprocesso_improdutivo",
			"o writer está redecodificando eventos sem que colunas novas apareçam; a ingestão perdeu o ganho do decode paralelo. Causas: teto de campos sob `ignore`, ou documento chegando com forma divergente (array/null) com frequência",
			"dataset", spec.Name, "seguidos", seguidos, "colunas", columns,
			"maxCols", compilado.MaxCols, "politica", compilado.OnMaxCols)
	})
	if err := s.eng.Register(spec.Name, compilado, st); err != nil {
		return nil, closingStore(st, fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// O amostrador das métricas de janela abre ANTES do counters.Register e da
	// compilação dos alarmes: a tabela `_counters.<nome>` e a tabela própria do
	// laço de alarmes resolvem o leitor de cada métrica UMA vez, na montagem.
	// Aberto depois, a fonte seria nil naquele instante e as colunas das
	// métricas sairiam NULL para sempre — e NULL compara falso, então um
	// `qualifyWhen` sobre métrica nunca qualificaria, sem erro nenhum.
	sampler, err := openCounterMetrics(spec, st, s.now, log)
	if err != nil {
		return nil, closingStore(st, fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// Daqui em diante, toda falha fecha o amostrador ANTES do Store: ele segura
	// mmap próprio e lê os contadores pelo campo do Store, sem a guarda de vida.
	failing := func(err error) error {
		return closingStore(st, closingSampler(sampler, err))
	}
	// As tabelas `_counters.<nome>` entram no motor sob o dataset — e o
	// namespace é declarado mesmo sem contador, para que a recusa nomeie o
	// que existe.
	if err := counters.Register(s.eng, spec.Name, st, spec.Counters, sampler.MetricSource()); err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// Os alarmes são compilados DEPOIS do counters.Register, que é quando as
	// tabelas dos contadores existem no motor, e ANTES de qualquer laço subir:
	// condição com nome de coluna errado ou consulta de enriquecimento
	// malformada derrubam a subida aqui, em vez de esperar a madrugada em que
	// o alarme disparia.
	alarmes, err := alarm.Compile(spec.Name, spec.Counters, defsOfCounters(st))
	if err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	d := &dataset{
		nome:    spec.Name,
		spec:    spec,
		sch:     compilado,
		st:      st,
		mat:     mat,
		metrics: sampler,
	}
	if err := openEmitters(d, spec, st, dir, sampler.MetricSource(), log); err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	if len(alarmes) > 0 {
		runner, err := alarm.New(alarmes, readersOfCounters(st), alarm.Options{
			Sink: alarm.NewLogSink(log),
			Log:  log,
			// O motor é o MESMO que serve as consultas remotas. As de
			// enriquecimento passam pelo mesmo binder, pelo mesmo planner e
			// pelo mesmo orçamento — e é isso que garante que elas não podem
			// fazer nada que um SELECT do cliente não pudesse.
			Engine:    s.eng,
			StatePath: st.AlarmStatePath(),
			Now:       s.now,
			// A MESMA fonte que a tabela `_counters.<nome>` recebeu: o valor que
			// o `qualifyWhen` enxerga é o que o SELECT devolve.
			Metrics: sampler.MetricSource(),
		})
		if err != nil {
			return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
		}
		d.alarms = runner
	}
	// `_meta.kafka` entra no motor DEPOIS de d existir: a tabela resolve o
	// consumidor por closure sobre ele, e o consumidor só nasce em
	// runIngestion — capturá-lo aqui guardaria nil para sempre. A tabela é
	// registrada mesmo num dataset sem Kafka: "não consome" é uma resposta, e
	// `SELECT * FROM _meta.kafka` nunca pode ser erro.
	//
	// A tabela irmã não a recebe, como não recebe a `_meta.counters`: a
	// ingestão dela é a do dataset base.
	if err := metakafka.Register(s.eng, spec.Name, metakafka.Source{
		Declarada:  metakafka.OfYAML(spec.Source.Kafka),
		Consumidor: d.consumidorCorrente,
		Duraveis:   st.SourceOffsets,
		State:      func() (string, string) { return stateOfIngestion(d) },
		Vigia:      d.vigiaCorrente,
	}); err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// A view `<schema>.alarms` (atalho `_counters.alarms`) entra pelo mesmo
	// caminho, e SEMPRE — inclusive num dataset sem alarme declarado, onde ela
	// responde a lista vazia: "este dataset não observa nada" é uma resposta.
	//
	// Groups é closure sobre d, e não a fatia: o retrato TROCA a cada passada,
	// e uma fatia capturada aqui descreveria para sempre o estado da subida.
	if err := alarmtable.Register(s.eng, spec.Name, alarmtable.Source{
		Groups: d.activeAlarmGroups,
	}); err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	if err := s.registerMetaUsers(spec.Name); err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// `_meta.tables` entra por último só por leitura: ela lista as tabelas
	// acima, e as linhas são montadas a cada consulta, a partir do catálogo do
	// momento — registrá-la antes ou depois das outras dá a mesma resposta.
	//
	// A irmã recebe a DELA, em openHistory, e é por isso que esta não é como a
	// `_meta.counters` e a `_meta.kafka`: aquelas descrevem a ingestão, que a
	// irmã não tem própria; esta descreve o que se pode consultar, e a irmã é
	// um dataset consultável por direito.
	if err := s.eng.RegisterMetaTables(spec.Name); err != nil {
		return nil, failing(fmt.Errorf("dataset %q: %w", spec.Name, err))
	}

	// A irmã abre por ÚLTIMO, depois de o base estar inteiro: ela recebe o
	// dicionário dele e espelha o Compiled dele, e as duas coisas exigem que
	// ele já exista.
	irma, err := s.openHistory(d, log)
	if err != nil {
		return nil, failing(err)
	}
	d.irma = irma
	return d, nil
}

// closingStore devolve o erro original, fechando o Store antes. O erro do
// Close é agregado e nunca substitui a causa: quem lê precisa do motivo de a
// abertura ter falhado, não do que aconteceu na limpeza.
func closingStore(st *storage.Store, causa error) error {
	if err := st.Close(); err != nil {
		return errors.Join(causa, err)
	}
	return causa
}

// closingSampler devolve a causa, fechando antes o amostrador (nil é aceito).
// Mesma disciplina do closingStore: o erro do Close é agregado e nunca
// substitui o motivo de a abertura ter falhado.
func closingSampler(sampler *countermetrics.Sampler, cause error) error {
	if sampler == nil {
		return cause
	}
	if err := sampler.Close(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// openCounterMetrics abre o amostrador das métricas de janela do dataset, ou
// devolve nil quando nenhum contador declara `metrics:` — não se paga arquivo
// nem goroutine para não amostrar nada, e o nil já é a fonte válida que deixa
// as colunas das métricas NULL (ver counters.MetricSource). Não sobe o laço:
// isso é do Run, pelo turnonCounterMetrics.
func openCounterMetrics(spec *schema.DatasetSpec, st *storage.Store, now func() time.Time, log *logging.Logger) (*countermetrics.Sampler, error) {
	inputs, err := countermetrics.CountersWithMetrics(spec.Counters, st.Counters())
	if err != nil {
		return nil, err
	}
	if len(inputs) == 0 {
		// Sem métrica declarada o amostrador não abre — e com ele não roda a
		// limpeza de órfãos do Open. É aqui que somem os arquivos que a última
		// métrica deixou ao sair do YAML; sem isto eles ficariam no disco para
		// sempre. Dataset que nunca declarou métrica não tem o diretório, e ele
		// não é criado.
		return nil, countermetrics.RemoveOrphans(spec.Name, st.CounterMetricsDir(), nil, log)
	}
	return countermetrics.Open(countermetrics.Options{
		Dataset:  spec.Name,
		Dir:      st.CounterMetricsDir(),
		Counters: inputs,
		Log:      log,
		Now:      now,
	})
}

// Run sobe o HTTP e os consumers, nessa ordem (§51), e bloqueia até o
// contexto acabar.
func (s *Server) Run(ctx context.Context, pprofAddr string) error {
	// Escrito ANTES de risesHTTP() subir a goroutine que serve requisições:
	// a criação da goroutine (o `go` dentro de risesHTTP) é um ponto de
	// sincronização do memory model, então todo handler que rodar depois
	// enxerga este campo sem precisar de lock — o mesmo raciocínio que já
	// vale para s.credenciais e s.tlsCert, escritos antes do bind.
	s.vidaCtx = ctx
	if err := s.risesHTTP(); err != nil {
		return err
	}
	if s.loopbackLn != nil {
		s.risesLoopback()
	}
	if pprofAddr != "" {
		s.risesPprof(pprofAddr)
	}
	s.log.Info("query.pronta", "consultas disponíveis",
		"datasets", len(s.datasets), "endereco", s.cfg.Addr(), "versao", buildinfo.String())
	s.turnonCompaction(ctx)
	s.turnonExpiration(ctx)
	s.turnonRetirementOfGroups(ctx)
	s.turnonCounterMetrics(ctx)
	s.turnonAlarms(ctx)
	s.turnonEmit(ctx)
	s.turnonRetention(ctx)
	s.turnonRebuild(ctx)

	var wg sync.WaitGroup
	for _, d := range s.datasets {
		d := d
		if !s.ingestionHabilitada(d) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runIngestion(ctx, d)
		}()
	}

	<-ctx.Done()
	s.log.Info("encerrando", "sinal recebido; drenando a ingestão antes de fechar")
	wg.Wait()
	return s.encerra()
}

// ingestionHabilitada aplica a decisão do D15: descrever o consumer não é
// ligá-lo.
func (s *Server) ingestionHabilitada(d *dataset) bool {
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

func (s *Server) runIngestion(ctx context.Context, d *dataset) {
	log := s.log.Dataset(d.nome)

	cfg, err := kafka.ResolveKafkaConfig(d.nome, d.spec.Source.Kafka, d.spec.Errors)
	if err != nil {
		d.falha(err)
		log.Error("ingestao.nao_subiu", "a ingestão não pôde subir; as consultas seguem de pé (§51)",
			"erro", err.Error())
		return
	}
	// O Store do writer é o PAR quando há tabela irmã: o §48 manda checkpoint
	// durável antes do commit do offset, e sem incluir o histórico nesse
	// checkpoint os eventos que ele ainda não gravou sairiam da janela de
	// replay quando o offset avançasse.
	var source kafka.Store = d.st
	if d.irma != nil {
		source = storage.PairWithHistory{
			Store: d.st,
			Hist:  d.irma.st,
			AoFalharOHistory: func(err error) {
				log.Error("historico.checkpoint_falhou",
					"a tabela histórica não durabilizou; a ingestão do dataset base segue",
					"historico", d.irma.nome, "erro", err.Error())
			},
		}
	}
	options := kafka.Options{
		Store:   source,
		Mat:     d.mat,
		Log:     s.log,
		Metrics: s.reg,
		Cadencia: kafka.Cadencia{
			Eventos:   checkpointEventosPadrao,
			Intervalo: s.cfg.CheckpointInterval(),
		},
	}

	// A recriação passa pelo MESMO kafka.Abre da subida, e é o ponto inteiro
	// do desenho. O §49 põe o checkpoint local como autoritativo e lista
	// quatro situações que RECUSAM subir a ingestão; um caminho de reconexão
	// que redescobrisse a posição por conta própria seria um segundo boot,
	// menos testado, que divergiria do primeiro no dia em que alguém mexesse
	// num dos dois. Aqui há uma implementação de posicionamento, não duas.
	// As duas séries do watchdog nascem AQUI, com zero, e não no ramo que as
	// usa. Série que só aparece quando há o que mostrar vira lacuna no
	// gráfico, e lacuna não se distingue de alvo fora do ar — é a mesma regra
	// que as séries de compaction já seguem. Alarmar em "desistiu" exige que
	// ela exista ANTES de desistir.
	trips := s.reg.Counter(metrics.MetKafkaWatchdogTrips,
		"Recriações do consumidor por falta de progresso com lag pendente.",
		metrics.LabelDataset).With(d.nome)
	desistiu := s.reg.Gauge(metrics.MetKafkaWatchdogGaveUp,
		"1 quando o watchdog esgotou as tentativas e a ingestão do dataset ficou parada.",
		metrics.LabelDataset).With(d.nome)
	desistiu.Set(0)
	// Publicadas no dataset ANTES do laço, e é o que permite a `_meta.kafka`
	// ler um zero LEGÍTIMO desde a primeira tentativa em vez de NULL.
	d.mu.Lock()
	d.trips, d.desistiu = trips, desistiu
	d.mu.Unlock()

	tentativas := cfg.WatchdogAttempts
	for tentativa := 0; ; tentativa++ {
		cons, err := kafka.Abre(ctx, cfg, options)
		if err != nil {
			d.falha(err)
			log.Error("ingestao.nao_subiu", "a ingestão não pôde subir; as consultas seguem de pé (§51)",
				"erro", err.Error())
			return
		}
		d.mu.Lock()
		d.cons = cons
		d.mu.Unlock()

		log.Info("ingestao.subiu", "consumindo", "topico", cfg.Topic, "group", cfg.GroupID)
		err = cons.Run(ctx)
		cons.Close()

		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		if !errors.Is(err, kafka.ErrWithoutProgresso) {
			d.falha(err)
			log.Error("ingestao.parou", "a ingestão parou; as consultas seguem de pé (§51)",
				"erro", err.Error())
			return
		}
		// Só o watchdog chega aqui. A contagem é de tentativas CONSECUTIVAS:
		// ela nasce a cada runIngestion e o laço só a incrementa enquanto
		// recriar não resolver — um consumidor que volte a andar sai do laço
		// pelo caminho de cima, e o próximo travamento recomeça do zero.
		// Sem isso, meses de uptime acumulariam três blips não relacionados e
		// a ingestão desistiria para sempre.
		if tentativa+1 >= tentativas {
			d.falha(err)
			log.Error("ingestao.desistiu",
				"o watchdog esgotou as tentativas de recriar o consumidor; a ingestão fica PARADA e as consultas seguem de pé (§51)",
				"tentativas", tentativas, "erro", err.Error())
			desistiu.Set(1)
			return
		}
		trips.Inc()
		log.Warn("ingestao.recriando",
			"o consumidor parou de receber com mensagens pendentes no broker; recriando pelo caminho da subida",
			"tentativa", tentativa+1, "de", tentativas, "erro", err.Error())
	}
}

// turnonCompaction sobe a goroutine do §53-55 em cada dataset, a partir da
// política do §78 lida pela Task 3.
//
// Depois do HTTP e ANTES da ingestão, que é a ordem do §51 aplicada a mais um
// consumidor de disco: quem sobe primeiro é quem responde consulta. E a
// compaction precisa estar de pé antes do consumer justamente porque é o
// consumer que produz o garbage — ligá-la depois abriria uma janela em que o
// dataset cresce sem ninguém devolvendo nada.
//
// O contexto é o do Run, então o SIGTERM cancela as goroutinas junto com a
// ingestão. O Close de cada Store as aguarda de novo (decisão D7): as duas
// coisas não se repetem à toa — o cancelamento é o pedido, e o join é o que
// garante que ninguém está renomeando arquivo quando o dataset fecha.
//
// A ausência tem log próprio de propósito. `enabled: false` é uma decisão do
// operador, e um processo que a honra em silêncio é indistinguível de um que a
// ignora — que é o defeito que a issue #1 denuncia, em outra chave.
// avisaChavesWithoutEngine anuncia, na subida, as chaves de spec que são aceitas e
// validadas mas ainda não têm implementação.
//
// A alternativa é o que havia antes: aceitar em silêncio. Um operador que
// declara uma janela de retenção e não recebe nada de volta não tem como
// distinguir "está funcionando" de "não existe" — é o defeito que a issue #1
// denuncia, e ele custa mais caro numa chave de retenção do que em qualquer
// outra, porque o sintoma esperado (disco parando de crescer) só apareceria
// dias depois.
func avisaChavesWithoutEngine(specs map[string]*schema.DatasetSpec, log *logging.Logger) {
	for _, name := range sortedNames(specs) {
		spec := specs[name]
		if d := spec.Materialization.TombstoneRetention; d > 0 {
			log.Warn("ttl.tombstoneRetention.sem_motor",
				"a chave é válida e NÃO tem efeito: esquecer a key exige purgar a primary e ensinar a compaction a descartar lápide vencida",
				"dataset", name, "tombstoneRetention", d)
		}
	}
}

func sortedNames(specs map[string]*schema.DatasetSpec) []string {
	out := make([]string, 0, len(specs))
	for name := range specs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func (s *Server) turnonCompaction(ctx context.Context) {
	habilitada, ratio := s.cfg.Compaction()
	if !habilitada {
		s.log.Warn("compaction.desligada",
			"storage.compaction.enabled é false: nada devolve o disco dos updates, e o §86 projeta 30-43 GB/dia escritos contra 5,6-8 GB vivos a 500 ev/s")
		return
	}
	for _, d := range s.datasets {
		d.st.StartCompaction(ctx, ratio)
	}
	s.log.Info("compaction.ligada",
		"a compaction do §53-55 está de pé", "datasets", len(s.datasets), "minimumLiveRatio", ratio)
}

// turnonExpiration sobe o coletor do idle timeout nos datasets que o declaram.
//
// Só neles: quem não declara expireAfterWrite não tem índice de escritas e não
// paga goroutine. O log nomeia quais subiram, porque "o disco não está sendo
// devolvido" é indistinguível de "não há nada para devolver" quando ninguém
// conta qual dos dois é.
// turnonRetirementOfGroups sobe, nos datasets com contador que declara
// retainEmptyGroups, o laço que aposenta grupos vazios — só neles, pela mesma
// economia do coletor de expiração.
func (s *Server) turnonRetirementOfGroups(ctx context.Context) {
	var withRetention []string
	for _, d := range s.datasets {
		d.st.StartRetirementOfGroups(ctx)
		if d.st.RetirementOfGroupsActive() {
			withRetention = append(withRetention, d.nome)
		}
	}
	if len(withRetention) == 0 {
		return
	}
	sort.Strings(withRetention)
	s.log.Info("contadores.aposentadoria_ligada",
		"o laço de aposentadoria de grupos vazios está de pé",
		"datasets", withRetention, "cadencia", storage.CadenciaOfRetirement.String())
}

// turnonCounterMetrics sobe o laço do amostrador nos datasets com contador que
// declara `metrics:` — só neles, pela mesma economia dos laços irmãos.
//
// ANTES dos alarmes: a primeira varredura de alarmes é imediata e lê as
// colunas das métricas, e com o amostrador já de pé ela disputa com a primeira
// amostra, em vez de começar sempre sobre o que a restauração publicou. O log
// nomeia quais subiram, porque "a janela não mudou" é indistinguível de
// "ninguém está amostrando" quando ninguém conta qual dos dois é.
func (s *Server) turnonCounterMetrics(ctx context.Context) {
	var sampling []string
	for _, d := range s.datasets {
		if d.metrics == nil {
			continue
		}
		d.metrics.Start(ctx)
		if d.metrics.Active() {
			sampling = append(sampling, d.nome)
		}
	}
	if len(sampling) == 0 {
		return
	}
	sort.Strings(sampling)
	s.log.Info("counter_metric.sampling",
		"window metric sampling is running",
		"datasets", sampling)
}

// turnonAlarms sobe o laço de varredura nos datasets que declaram alarme — só
// neles, pela mesma economia dos laços irmãos. O log nomeia quais subiram,
// porque "nenhum alarme disparou" é indistinguível de "ninguém está
// observando" quando ninguém conta qual dos dois é.
func (s *Server) turnonAlarms(ctx context.Context) {
	var withAlarms []string
	for _, d := range s.datasets {
		if d.alarms == nil {
			continue
		}
		d.alarms.Start(ctx)
		if d.alarms.Active() {
			withAlarms = append(withAlarms, d.nome)
		}
	}
	if len(withAlarms) == 0 {
		return
	}
	sort.Strings(withAlarms)
	s.log.Info("alarm.watching",
		"o laço de varredura de alarmes está de pé",
		"datasets", withAlarms)
}

// turnonEmit sobe o laço de emissão periódica nos contadores que a declaram.
func (s *Server) turnonEmit(ctx context.Context) {
	var withEmit []string
	for _, d := range s.datasets {
		for _, e := range d.emitters {
			e.Start(ctx)
		}
		if len(d.emitters) > 0 {
			withEmit = append(withEmit, d.nome)
		}
	}
	if len(withEmit) == 0 {
		return
	}
	sort.Strings(withEmit)
	s.log.Info("series.emitting",
		"a emissão periódica dos contadores está de pé",
		"datasets", withEmit)
}

func (s *Server) turnonExpiration(ctx context.Context) {
	maxByPassada, timeByPassada := s.cfg.Expiry()
	var withExpiration []string
	for _, d := range s.datasets {
		if !d.st.HasExpiration() {
			continue
		}
		d.st.StartExpiration(ctx, storage.LimitesOfExpiration{
			MaxByPassada:  maxByPassada,
			TimeByPassada: timeByPassada,
		})
		withExpiration = append(withExpiration, d.nome)
	}
	if len(withExpiration) == 0 {
		return
	}
	sort.Strings(withExpiration)
	s.log.Info("expiracao.ligada",
		"o coletor do idle timeout está de pé",
		"datasets", withExpiration,
		"maxPorPassada", maxByPassada, "tempoPorPassada", timeByPassada)
}

// checkpointEventosPadrao é a mesma constante do `nishidb ingest`: o custo de
// não checkpointar é o heap acompanhar o tamanho do dataset.
const checkpointEventosPadrao = 500_000

// risesHTTP é o ÚNICO ponto de entrada do listener principal — a costura do
// §77: buildMux constrói o mux CRU (sem autenticação nenhuma), e
// withAuth é o wrapper único em torno dele (auth.go). O
// listener loopback (risesLoopback, em http.go — decisão 4 do plano do ciclo)
// serve o MESMO mux via muxLoopback(rt), não rt.Raw() puro: o wrapper é
// DIFERENTE (rota READ passa direto, rota ADMIN responde 403 —
// rejectLoopbackAdmin), mas nasce da MESMA função de definição de rota,
// nunca o *http.ServeMux cru exposto direto nem uma segunda lista de rotas.
func (s *Server) risesHTTP() error {
	rt := s.buildMux()

	s.http = &http.Server{
		Handler:           s.withAuth(rt),
		ReadHeaderTimeout: 10 * time.Second,
		// ErrorLog roteia o que net/http grava sozinho (erro de handshake,
		// conexão cortada no meio) para o logger estruturado — scanners
		// corporativos batendo na porta exposta geram esse ruído o tempo
		// todo, e sem isto cada linha caía em log.Printf cru no journal,
		// fora do formato do §95.
		ErrorLog: s.errorLog("principal"),
		// SEM WriteTimeout de propósito: ele é teto de CONEXÃO, e mataria um
		// stream longo legítimo no meio. O teto de uma consulta é o
		// query.timeout do dataset, aplicado pelo executor via contexto — o
		// único que sabe distinguir "demorou porque tem muito resultado" de
		// "travou".
	}

	// ServeTLS no ponto único (§77, Task B4): MinVersion TLS12 é o mesmo
	// precedente de internal/kafka/config.go (Resolve, security.TLS()). O
	// certificado é o que validateCertificateAtStartup já carregou e validou —
	// nunca relido do disco aqui. Rotação de certificado é reiniciar o
	// processo (limite nomeado: sem SIGHUP-reload nesta versão).
	if s.tlsCert != nil {
		s.http.TLSConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{*s.tlsCert},
		}
		go func() {
			// certFile/keyFile vazios: o par já está em TLSConfig.Certificates
			// (doc de http.Server.ServeTLS — só exige os caminhos quando
			// NENHUM dos dois está populado).
			if err := s.http.ServeTLS(s.ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.log.Error("http.falhou", "o servidor HTTPS caiu", "erro", err.Error())
			}
		}()
		return nil
	}

	go func() {
		if err := s.http.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http.falhou", "o servidor HTTP caiu", "erro", err.Error())
		}
	}()
	return nil
}

// ctxOfMutacao devolve um ctx cancelado por QUALQUER um dos dois: o cliente
// HTTP desistindo (r.Context()) OU o servidor encerrando (vidaCtx, o
// cancelado no SIGTERM — godoc do campo). É o que handleCompact (mutations.go)
// usa para chamar Store.CompactaUmaVez, cuja passada pode levar minutos.
//
// A composição existe porque http.Server.Shutdown não cancela r.Context() —
// só espera o handler devolver, com um teto (o ctx de 15s de encerra()) que
// estoura e devolve o controle ANTES de o handler ter de fato terminado. Sem
// vidaCtx no meio, uma passada manual em voo no encerramento nunca saberia
// que o processo está de saída, e o compInVoo.Wait() do Close (ForCompaction,
// storage/compaction.go) ficaria esperando por ela sem teto nenhum.
//
// context.AfterFunc, não um select bloqueante numa goroutine à parte: o stop
// que ele devolve é o que permite liberar o registro em vidaCtx assim que a
// chamada acaba (via o cancel devolvido aqui), em vez de vazar uma goroutine
// por requisição até o processo inteiro sair.
func (s *Server) ctxOfMutacao(r *http.Request) (context.Context, context.CancelFunc) {
	base := r.Context()
	if s.vidaCtx == nil {
		// Só em teste que chama o handler sem passar por Run: sem vidaCtx
		// para compor, o contrato cai para o de sempre — só o cliente decide.
		return base, func() {}
	}
	ctx, cancel := context.WithCancel(base)
	para := context.AfterFunc(s.vidaCtx, cancel)
	return ctx, func() {
		para()
		cancel()
	}
}

// Close libera o listener e os datasets sem ter rodado.
//
// Existe porque o Novo já TOMA recursos — o listener e o flock de cada
// dataset — e até aqui só o Run os devolvia: um chamador que montasse o
// servidor e decidisse não subi-lo deixava os datasets travados até o processo
// morrer. É idempotente.
func (s *Server) Close() error { return s.encerra() }

func (s *Server) encerra() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var errs []error
	if s.http != nil {
		errs = append(errs, s.http.Shutdown(ctx))
	} else if s.ln != nil {
		// Sem Run, o listener nunca foi entregue ao http.Server: fechá-lo é
		// responsabilidade daqui.
		errs = append(errs, s.ln.Close())
	}
	// Mesma disciplina do listener principal, para o do plano loopback: se
	// risesLoopback rodou, o Shutdown do http.Server drena e fecha; se Novo
	// bindou mas Run nunca chegou a subir o http.Server, fechar o listener
	// cru é responsabilidade daqui.
	if s.loopbackSrv != nil {
		errs = append(errs, s.loopbackSrv.Shutdown(ctx))
	} else if s.loopbackLn != nil {
		errs = append(errs, s.loopbackLn.Close())
	}
	if s.pprofSrv != nil {
		errs = append(errs, s.pprofSrv.Shutdown(ctx))
	}
	// ORDEM: os alarmes param ANTES do quiesce dos Stores.
	//
	// O laço lê os contadores pelo campo do Store, que é leitura direta sem a
	// guarda de vida — diferente da auditoria, ele não passa por
	// entra()/sai(). E como ele vive AQUI, e não no Store, o Close de lá não o
	// aguarda. Invertida a ordem, uma varredura em voo leria um Store fechado.
	//
	// O amostrador das métricas de janela obedece à mesma regra e pelo mesmo
	// motivo, e para DEPOIS dos alarmes: a varredura em voo que o forAlarms
	// aguarda lê as colunas das métricas, e com o amostrador ainda de pé ela
	// termina sobre valores que continuam sendo amostrados, e não sobre uma
	// janela congelada no meio da passada. (O publicado continua legível depois
	// do Close — a ordem entre os dois laços é de frescor, e não de memória; a
	// que não pode inverter é a de ambos antes do closeDatasets.)
	s.forAlarms()
	s.forEmitters()
	s.forCounterMetrics()
	errs = append(errs, s.closeDatasets())
	// A tabela de usuários fecha por último: até aqui o http.Server ainda
	// podia estar drenando requests autenticados contra ela.
	if s.users != nil {
		errs = append(errs, s.users.Close())
	}
	return errors.Join(errs...)
}

// registerMetaUsers declara `_meta.users` sob o dataset, como tabela
// só-ADMIN. Só existe com a autenticação ligada: sem tabela de usuários não há
// o que listar, nem ADMIN para ler. A tabela é a mesma em qualquer dataset —
// o handle da consulta só decide sob que nome ela é alcançada.
func (s *Server) registerMetaUsers(dataset string) error {
	if s.users == nil {
		return nil
	}
	return metausers.Register(s.eng, dataset, metausers.Source{Users: s.users.Users})
}

// closeUsers solta o flock da tabela de usuários numa subida abortada. Não
// devolve erro pela mesma razão do resto da limpeza de subida: o erro que
// importa é o que abortou.
func (s *Server) closeUsers(log *logging.Logger) {
	if s.users == nil {
		return
	}
	if err := s.users.Close(); err != nil {
		log.Warn("users.close", "failed to close the user table on aborted startup", "error", err.Error())
	}
}

// forAlarms cancela e AGUARDA o laço de alarmes de cada dataset. Idempotente e
// seguro sem nenhum laço de pé, como os For* do storage.
func (s *Server) forAlarms() {
	for _, d := range s.datasets {
		if d.alarms != nil {
			d.alarms.Stop()
		}
	}
}

// forEmitters cancela e AGUARDA o laço de emissão de cada contador, e fecha os
// arquivos. Mesma ordem e mesma razão dos alarmes: o laço lê os contadores sem
// a guarda de vida do Store, e vive fora dele.
func (s *Server) forEmitters() {
	for _, d := range s.datasets {
		for _, e := range d.emitters {
			if err := e.Close(); err != nil {
				s.log.Warn("series.close_failed",
					"a emissão periódica não fechou o arquivo limpo; a cauda da série pode ter sido perdida",
					"dataset", d.nome, "error", err.Error())
			}
		}
	}
}

// forCounterMetrics cancela e AGUARDA o laço do amostrador das métricas de
// janela de cada dataset, grava os baldes abertos e fecha os arquivos.
// Idempotente — o Close do amostrador é — e seguro sem nenhum laço de pé, o que
// deixa a subida abortada chamá-lo sem perguntar o que chegou a abrir.
//
// Mesma razão dos alarmes e das emissões: o amostrador lê os contadores pelo
// campo do Store, sem a guarda de vida, e vive fora dele.
func (s *Server) forCounterMetrics() {
	for _, d := range s.datasets {
		if d.metrics == nil {
			continue
		}
		if err := d.metrics.Close(); err != nil {
			s.log.Warn("counter_metric.close_failed",
				"the window metric files did not close cleanly; the window history may be incomplete on the next start",
				"dataset", d.nome, "error", err.Error())
		}
	}
}

// CounterMetricsActive diz se o laço do amostrador das métricas de janela do
// dataset está de pé. Existe para a observabilidade e para os testes: "a
// janela não mudou" é indistinguível de "ninguém está amostrando" sem esta
// resposta.
func (s *Server) CounterMetricsActive(dataset string) bool {
	for _, d := range s.datasets {
		if d.nome == dataset {
			return d.metrics != nil && d.metrics.Active()
		}
	}
	return false
}

// EmitActive diz se ALGUM laço de emissão do dataset está de pé. Existe para a
// observabilidade e para o teste da parada.
func (s *Server) EmitActive(dataset string) bool {
	for _, d := range s.datasets {
		if d.nome != dataset {
			continue
		}
		for _, e := range d.emitters {
			if e.Active() {
				return true
			}
		}
	}
	return false
}

// openEmitters monta um emissor por contador com bloco `emit`.
//
// A limpeza do diretório roda ANTES de qualquer arquivo ser aberto, e recebe o
// que está declarado: baixar o keep no YAML devolve o disco na subida seguinte,
// e o que não está declarado é deixado em paz (series.Cleanup explica por quê).
//
// src é a fonte das métricas de janela, e nil é válido: é o dataset em que
// nenhum contador declara `metrics:`, e também o que o amostrador ausente
// entrega. As colunas declaradas continuam saindo na linha, todas null — o
// formato da linha é função do YAML (ver series.Metric).
func openEmitters(d *dataset, spec *schema.DatasetSpec, st *storage.Store, dir string, src counters.MetricSource, log *logging.Logger) error {
	declared := make(map[string]int)
	for _, cs := range spec.Counters {
		if cs.Emit != nil {
			declared[cs.Emit.File] = cs.Emit.Keep
		}
	}
	if len(declared) == 0 {
		return nil
	}
	seriesDir := filepath.Join(dir, schema.EmitDirName)
	if err := series.Cleanup(seriesDir, declared); err != nil {
		return err
	}
	readers := readersOfCounters(st)
	byName := make(map[string][]string, len(spec.Counters))
	for _, c := range st.Counters() {
		byName[c.Def().Name] = c.Def().Names
	}
	for _, cs := range spec.Counters {
		if cs.Emit == nil {
			continue
		}
		reader, ok := readers[cs.Name]
		if !ok {
			return fmt.Errorf("emit: o contador %q não está aberto neste dataset", cs.Name)
		}
		metrics, err := emittedMetrics(cs, src)
		if err != nil {
			return err
		}
		e, err := series.NewEmitter(series.Options{
			Dataset:  spec.Name,
			Counter:  cs.Name,
			Columns:  byName[cs.Name],
			Metrics:  metrics,
			Reader:   reader,
			Dir:      seriesDir,
			File:     cs.Emit.File,
			Every:    cs.Emit.Every,
			MaxBytes: cs.Emit.MaxBytes,
			Keep:     cs.Emit.Keep,
			Log:      log,
		})
		if err != nil {
			return err
		}
		d.emitters = append(d.emitters, e)
	}
	return nil
}

// emittedMetrics resolve, na ordem declarada, o leitor de cada métrica de
// janela do contador, para que o emissor as escreva na linha.
//
// Com src nil não há leitor nenhum, e as colunas saem null: é o dataset sem
// amostrador, e o formato da linha não pode depender de ele estar de pé — a
// mesma regra que faz o schema de `_counters.<nome>` não depender da fonte.
//
// Com a fonte presente, uma métrica declarada que ela não resolve é erro na
// subida, e não coluna calada para sempre: é a mesma recusa de counters.
func emittedMetrics(cs schema.CounterSpec, src counters.MetricSource) ([]series.Metric, error) {
	if len(cs.Metrics) == 0 {
		return nil, nil
	}
	out := make([]series.Metric, 0, len(cs.Metrics))
	for _, m := range cs.Metrics {
		e := series.Metric{Name: m.Name, Aggregates: m.Aggregates}
		if src != nil {
			reader, ok := src.Metric(cs.Name, m.Name)
			if !ok || reader == nil {
				return nil, fmt.Errorf("emit: the metric source has no reader for metric %q of counter %q", m.Name, cs.Name)
			}
			e.Reader = reader
		}
		out = append(out, e)
	}
	return out, nil
}

// activeAlarmGroups é o retrato dos alarmes em observação deste dataset, para
// a view `<schema>.alarms` (atalho `_counters.alarms`). nil quando o dataset
// não declara alarme — e a lista vazia é a resposta certa, não um erro.
func (d *dataset) activeAlarmGroups() []alarm.ActiveGroup {
	if d.alarms == nil {
		return nil
	}
	return d.alarms.ActiveGroups()
}

// AlarmsActive diz se o laço de alarmes do dataset está de pé. Existe para a
// observabilidade e para o teste da ordem de parada: "nenhum alarme disparou" é
// indistinguível de "ninguém está observando" sem esta resposta.
func (s *Server) AlarmsActive(dataset string) bool {
	for _, d := range s.datasets {
		if d.nome == dataset {
			return d.alarms != nil && d.alarms.Active()
		}
	}
	return false
}

// defsOfCounters são as definições dos contadores abertos, na ordem declarada.
func defsOfCounters(st *storage.Store) []counter.Definition {
	cs := st.Counters()
	out := make([]counter.Definition, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.Def())
	}
	return out
}

// readersOfCounters casa nome de contador → leitor, para o laço de alarmes.
func readersOfCounters(st *storage.Store) map[string]counters.Reader {
	cs := st.Counters()
	out := make(map[string]counters.Reader, len(cs))
	for _, c := range cs {
		out[c.Def().Name] = c
	}
	return out
}

// closeDatasets faz o quiesce de cada Store, que é o que devolve o flock.
func (s *Server) closeDatasets() error {
	var errs []error
	// Ordem INVERSA à da abertura, e a razão é o dicionário compartilhado: a
	// irmã entra na lista logo depois do seu base, então fechar de trás para
	// frente garante que ela sempre feche ANTES dele. Na ordem direta, o base
	// fecharia o fields.dict enquanto a passada de retenção da irmã ainda
	// pudesse estar num Checkpoint que o lê — e o Close dela é justamente
	// quem cancela e aguarda essa passada.
	for i := len(s.datasets) - 1; i >= 0; i-- {
		if d := s.datasets[i]; d.st != nil {
			errs = append(errs, d.st.Close())
		}
	}
	return errors.Join(errs...)
}

// Endereco é o endereço REAL do listener, que só é conhecido depois do bind.
func (s *Server) Endereco() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// EnderecoLoopback é o endereço REAL do listener do plano loopback (decisão
// 4 do plano do ciclo), vazio quando server.loopback.enabled é false — o
// mesmo formato de Endereco, para quem precisa dialar o segundo listener sem
// reconstruir "127.0.0.1:" + a porta declarada em config à mão.
func (s *Server) EnderecoLoopback() string {
	if s.loopbackLn == nil {
		return ""
	}
	return s.loopbackLn.Addr().String()
}
