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
	"sort"
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

	// tlsCert é o par certificado/chave já CARREGADO por
	// validaCertificadoNaSubida (seguranca.go), nil quando server.tls está
	// desligado. sobeHTTP o REUSA no ServeTLS — nunca relido do disco depois
	// da subida; rotação de certificado é reiniciar o processo.
	tlsCert *tls.Certificate

	// loopbackLn é o listener do plano loopback (decisão 4 do plano do
	// ciclo), bindado em Novo() junto do listener principal — mesma razão do
	// bind cedo de s.ln: descobrir porta ocupada depois de já ter tomado o
	// flock de N datasets é desperdício. Nil quando server.loopback.enabled é
	// false (default): nenhum listener a mais sobe.
	loopbackLn net.Listener
	// loopbackSrv é o SEGUNDO http.Server que serve o mux CRU (rt.Cru(), sem
	// autenticação nem TLS) sobre loopbackLn — o que preserva painel, script
	// de update e cron locais funcionando sem credencial mesmo com
	// server.authentication ligado no listener exposto.
	loopbackSrv *http.Server

	// vidaCtx é o MESMO ctx que Roda recebe — o que signal.NotifyContext
	// cancela no SIGTERM (cmd/nishidb/main.go). handleCompact (mutacoes.go)
	// o compõe com r.Context() via ctxDeMutacao, porque http.Server.Shutdown
	// NÃO cancela o contexto de uma requisição em voo — só espera o handler
	// devolver, e por um tempo limitado (o ctx de 15s de encerra()). Sem
	// esta composição, uma passada manual de compaction (POST /v1/compact)
	// em voo no encerramento ignora o SIGTERM e só para quando processa
	// todos os candidatos ou o cliente desiste — e o compEmVoo.Wait() do
	// Close (storage/compaction.go, ParaCompaction) fica sem teto esperando
	// por ela, minutos em produção, até o systemd desistir e mandar SIGKILL.
	// Nil só em testes que constroem o handler sem passar por Roda —
	// ctxDeMutacao cai para o contrato antigo (só o cliente decide) nesse
	// caso, o que preserva o comportamento anterior a esta composição.
	vidaCtx context.Context
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
	avisaChavesSemMotor(specs, log)

	// A segurança do §74-78 é resolvida ANTES do bind: falha de credencial,
	// de certificado, de porta loopback, ou a matriz de recusa do §77 não
	// podem deixar um listener aberto para trás. A ordem é a mais barata
	// primeiro — matriz e endereço loopback são só leitura de config, sem
	// I/O — para não gastar em ler env var, arquivo de senha ou certificado
	// uma subida que já vai ser recusada pela topologia.
	if err := avaliaExposicaoEAutenticacao(cfg, log); err != nil {
		return nil, err
	}
	loopbackAddr, err := enderecoLoopback(cfg)
	if err != nil {
		return nil, err
	}
	certificado, err := validaCertificadoNaSubida(cfg)
	if err != nil {
		return nil, err
	}
	if certificado != nil {
		// Aviso de validade do leaf (Task B4): nunca recusa a subida, só
		// registra WARN/ERRO estruturado e publica a gauge de expiração —
		// ver o comentário de avisaValidadeDoCertificado (seguranca.go).
		if err := avisaValidadeDoCertificado(*certificado, log, reg); err != nil {
			return nil, err
		}
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
		tlsCert:     certificado,
	}
	s.met = registraMetricasDeQuery(reg, s.eng)
	s.authFalhas = registraMetricasDeAuth(reg)

	// O bind vem ANTES de abrir os datasets: descobrir porta ocupada depois
	// de já ter tomado o flock de N datasets é desperdício, e deixa o
	// operador com um processo morto que segurou os locks por um instante.
	// Os DOIS listeners (principal e loopback, quando ligado) bindam aqui,
	// pela mesma razão — a falha do segundo não pode deixar o primeiro
	// vazado, por isso o defer de limpeza abaixo cobre os dois.
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
		if s.loopbackLn != nil {
			if err := s.loopbackLn.Close(); err != nil {
				log.Warn("loopback.close", "falha ao fechar o listener loopback na subida abortada", "erro", err.Error())
			}
		}
		if err := s.fechaDatasets(); err != nil {
			log.Warn("datasets.close", "falha ao fechar datasets na subida abortada", "erro", err.Error())
		}
	}()

	if loopbackAddr != "" {
		if s.loopbackLn, err = net.Listen("tcp", loopbackAddr); err != nil {
			return nil, fmt.Errorf("server: escutar loopback em %s: %w", loopbackAddr, err)
		}
	}

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
	dir := s.cfg.CaminhoDoDataset(spec.Name)
	bitmaps := schema.CamposComBitmap(spec)
	hashes := schema.CamposComHash(spec)
	st, err := storage.Open(storage.Options{
		Dir:              dir,
		Dataset:          spec.Name,
		SegmentSize:      s.cfg.SegmentSize(),
		BitmapFieldNames: bitmaps,
		HashFieldNames:   hashes,
		ColunasDerivadas: spec.NomesDasExtracoes(),
		Log:              log,
		Metrics:          s.reg,
		ExpireAfterWrite: spec.ExpiraApos(),
		CampoDeEscrita:   schema.CampoInstanteDeEscrita,
	})
	if err != nil {
		return nil, fmt.Errorf("dataset %q: %w", spec.Name, err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		return nil, fechandoStore(st, fmt.Errorf("dataset %q: %w", spec.Name, err))
	}
	// O contador de descarte diz QUANTO; o log diz por ONDE começar. Uma vez
	// por dataset: sob a política `ignore` o descarte é o estado normal, e uma
	// linha por evento afogaria o log.
	compilado.AoDescartarCampo(func(campo string) {
		log.Warn("schema.teto_de_campos",
			"o dataset alcançou schema.maxCols; campos novos deixaram de virar coluna",
			"campo", campo, "maxCols", compilado.MaxCols, "politica", compilado.OnMaxCols)
	})
	// Divergência de tipo em coluna aninhada não descarta o evento, então ela
	// não aparece em lugar nenhum da contabilidade de ingestão. Uma vez por
	// dataset, com os DOIS tipos: só com eles o operador sabe o que mudou.
	compilado.AoValorIncompativel(func(coluna, fixado, veio string) {
		log.Warn("schema.tipo_aninhado_divergente",
			"valor não coube no tipo fixado da coluna aninhada; a coluna é removida nessa linha e o documento guarda o valor",
			"coluna", coluna, "tipoFixado", fixado, "tipoQueVeio", veio)
	})
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
	// Escrito ANTES de sobeHTTP() subir a goroutine que serve requisições:
	// a criação da goroutine (o `go` dentro de sobeHTTP) é um ponto de
	// sincronização do memory model, então todo handler que rodar depois
	// enxerga este campo sem precisar de lock — o mesmo raciocínio que já
	// vale para s.credenciais e s.tlsCert, escritos antes do bind.
	s.vidaCtx = ctx
	if err := s.sobeHTTP(); err != nil {
		return err
	}
	if s.loopbackLn != nil {
		s.sobeLoopback()
	}
	if pprofAddr != "" {
		s.sobePprof(pprofAddr)
	}
	s.log.Info("query.pronta", "consultas disponíveis",
		"datasets", len(s.datasets), "endereco", s.cfg.Endereco(), "versao", buildinfo.String())
	s.ligaCompaction(ctx)
	s.ligaExpiracao(ctx)

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
// avisaChavesSemMotor anuncia, na subida, as chaves de spec que são aceitas e
// validadas mas ainda não têm implementação.
//
// A alternativa é o que havia antes: aceitar em silêncio. Um operador que
// declara uma janela de retenção e não recebe nada de volta não tem como
// distinguir "está funcionando" de "não existe" — é o defeito que a issue #1
// denuncia, e ele custa mais caro numa chave de retenção do que em qualquer
// outra, porque o sintoma esperado (disco parando de crescer) só apareceria
// dias depois.
func avisaChavesSemMotor(specs map[string]*schema.DatasetSpec, log *logging.Logger) {
	for _, nome := range nomesOrdenados(specs) {
		spec := specs[nome]
		if d := spec.Materialization.TombstoneRetention; d > 0 {
			log.Warn("ttl.tombstoneRetention.sem_motor",
				"a chave é válida e NÃO tem efeito: esquecer a key exige purgar a primary e ensinar a compaction a descartar lápide vencida",
				"dataset", nome, "tombstoneRetention", d)
		}
		if ttl := spec.Materialization.TTL; ttl != nil && ttl.Field != "" {
			log.Warn("ttl.field.sem_motor",
				"a chave é válida e NÃO tem efeito: a expiração implementada é a ttl.expireAfterWrite",
				"dataset", nome, "field", ttl.Field)
		}
	}
}

func nomesOrdenados(specs map[string]*schema.DatasetSpec) []string {
	out := make([]string, 0, len(specs))
	for nome := range specs {
		out = append(out, nome)
	}
	sort.Strings(out)
	return out
}

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

// ligaExpiracao sobe o coletor do idle timeout nos datasets que o declaram.
//
// Só neles: quem não declara expireAfterWrite não tem índice de escritas e não
// paga goroutine. O log nomeia quais subiram, porque "o disco não está sendo
// devolvido" é indistinguível de "não há nada para devolver" quando ninguém
// conta qual dos dois é.
func (s *Servidor) ligaExpiracao(ctx context.Context) {
	maxPorPassada, tempoPorPassada := s.cfg.Expiracao()
	var comExpiracao []string
	for _, d := range s.datasets {
		if !d.st.TemExpiracao() {
			continue
		}
		d.st.IniciaExpiracao(ctx, storage.LimitesDeExpiracao{
			MaxPorPassada:   maxPorPassada,
			TempoPorPassada: tempoPorPassada,
		})
		comExpiracao = append(comExpiracao, d.nome)
	}
	if len(comExpiracao) == 0 {
		return
	}
	sort.Strings(comExpiracao)
	s.log.Info("expiracao.ligada",
		"o coletor do idle timeout está de pé",
		"datasets", comExpiracao,
		"maxPorPassada", maxPorPassada, "tempoPorPassada", tempoPorPassada)
}

// checkpointEventosPadrao é a mesma constante do `nishidb ingest`: o custo de
// não checkpointar é o heap acompanhar o tamanho do dataset.
const checkpointEventosPadrao = 500_000

// sobeHTTP é o ÚNICO ponto de entrada do listener principal — a costura do
// §77: montaMux constrói o mux CRU (sem autenticação nenhuma), e
// comAutenticacao é o wrapper único em torno dele (autenticacao.go). O
// listener loopback (sobeLoopback, em http.go — decisão 4 do plano do ciclo)
// serve o MESMO mux via muxLoopback(rt), não rt.Cru() puro: o wrapper é
// DIFERENTE (rota READ passa direto, rota ADMIN responde 403 —
// recusaLoopbackAdmin), mas nasce da MESMA função de definição de rota,
// nunca o *http.ServeMux cru exposto direto nem uma segunda lista de rotas.
func (s *Servidor) sobeHTTP() error {
	rt := s.montaMux()

	s.http = &http.Server{
		Handler:           s.comAutenticacao(rt),
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
	// certificado é o que validaCertificadoNaSubida já carregou e validou —
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

// ctxDeMutacao devolve um ctx cancelado por QUALQUER um dos dois: o cliente
// HTTP desistindo (r.Context()) OU o servidor encerrando (vidaCtx, o
// cancelado no SIGTERM — godoc do campo). É o que handleCompact (mutacoes.go)
// usa para chamar Store.CompactaUmaVez, cuja passada pode levar minutos.
//
// A composição existe porque http.Server.Shutdown não cancela r.Context() —
// só espera o handler devolver, com um teto (o ctx de 15s de encerra()) que
// estoura e devolve o controle ANTES de o handler ter de fato terminado. Sem
// vidaCtx no meio, uma passada manual em voo no encerramento nunca saberia
// que o processo está de saída, e o compEmVoo.Wait() do Close (ParaCompaction,
// storage/compaction.go) ficaria esperando por ela sem teto nenhum.
//
// context.AfterFunc, não um select bloqueante numa goroutine à parte: o stop
// que ele devolve é o que permite liberar o registro em vidaCtx assim que a
// chamada acaba (via o cancel devolvido aqui), em vez de vazar uma goroutine
// por requisição até o processo inteiro sair.
func (s *Servidor) ctxDeMutacao(r *http.Request) (context.Context, context.CancelFunc) {
	base := r.Context()
	if s.vidaCtx == nil {
		// Só em teste que chama o handler sem passar por Roda: sem vidaCtx
		// para compor, o contrato cai para o de sempre — só o cliente decide.
		return base, func() {}
	}
	ctx, cancela := context.WithCancel(base)
	para := context.AfterFunc(s.vidaCtx, cancela)
	return ctx, func() {
		para()
		cancela()
	}
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
	// Mesma disciplina do listener principal, para o do plano loopback: se
	// sobeLoopback rodou, o Shutdown do http.Server drena e fecha; se Novo
	// bindou mas Roda nunca chegou a subir o http.Server, fechar o listener
	// cru é responsabilidade daqui.
	if s.loopbackSrv != nil {
		errs = append(errs, s.loopbackSrv.Shutdown(ctx))
	} else if s.loopbackLn != nil {
		errs = append(errs, s.loopbackLn.Close())
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

// EnderecoLoopback é o endereço REAL do listener do plano loopback (decisão
// 4 do plano do ciclo), vazio quando server.loopback.enabled é false — o
// mesmo formato de Endereco, para quem precisa dialar o segundo listener sem
// reconstruir "127.0.0.1:" + a porta declarada em config à mão.
func (s *Servidor) EnderecoLoopback() string {
	if s.loopbackLn == nil {
		return ""
	}
	return s.loopbackLn.Addr().String()
}
