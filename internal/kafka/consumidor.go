package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

var (
	// ErrPosicaoDesconhecida indica dataset COM linhas e SEM offsets locais.
	//
	// É o buraco que o `autoOffsetReset` sozinho não cobre: "não há offset
	// local" também é verdade quando o kafka.offsets sumiu, ficou ilegível ou
	// veio de um restore parcial. Nesse estado, `earliest` reprocessaria o
	// tópico retido inteiro e `latest` pularia tudo — os dois em silêncio. O
	// sinal para detectar já é persistido (RowCount no manifest), então a
	// recusa é barata e a alternativa é corrupção lógica silenciosa.
	ErrPosicaoDesconhecida = errors.New("kafka: dataset tem linhas e não tem offsets locais")

	// ErrOrigemDivergente indica que o YAML aponta para um fluxo diferente do
	// que gerou os offsets persistidos.
	ErrOrigemDivergente = errors.New("kafka: a origem mudou desde o último checkpoint")

	// ErrTopicoAusente indica tópico inexistente ou sem partições no cluster.
	ErrTopicoAusente = errors.New("kafka: tópico ausente")

	// ErrAssignmentParcial indica que o group não entregou todas as partições
	// do tópico a esta instância.
	//
	// Single-writer (§27) somado a checkpoint local autoritativo (§49)
	// implica UMA instância ativa por group, mas a spec nunca diz isso. Sem
	// esta guarda o modo de falha é silencioso: duas instâncias dividem as
	// partições, cada dataset materializa estado parcial, e nenhuma métrica
	// do §92 acusa — o group consome tudo e o lag fica normal.
	ErrAssignmentParcial = errors.New("kafka: assignment parcial")
)

// Consumidor é o processo de ingestão de UM dataset.
type Consumidor struct {
	cfg    *Config
	cl     *kgo.Client
	adm    *kadm.Client
	fila   *Fila
	w      *Writer
	log    *logging.Logger
	met    *metricas
	origem checkpoint.Origem

	// erroAssign carrega a falha detectada dentro do callback de assignment,
	// que não tem por onde devolver erro.
	erroAssign atomic.Value
	// erroVigia guarda a causa do watchdog. Escrito ANTES do cancelamento
	// que ele dispara, para que o Roda a encontre depois de o poll voltar.
	erroVigia atomic.Value
	// cancelaPoll interrompe o PollRecords em curso. Sem ele, uma recusa de
	// assignment só seria vista quando o poll voltasse — e num assignment
	// parcial o poll pode não voltar nunca, porque as partições que sobraram
	// para esta instância podem estar sem tráfego nenhum.
	cancelaPoll atomic.Value

	// offsetsIniciais é a posição de onde retomar, por partição.
	offsetsIniciais map[int32]int64

	// admPronto barra aoAtribuir até c.adm existir (débito 60).
	//
	// O franz-go entrega partições assim que o metadata loop descobre o
	// tópico em background, e isso não espera o primeiro Poll nem o retorno
	// de kgo.NewClient: um assignment pode chegar ENQUANTO NewClient ainda
	// está em curso, antes de `Abre` ter a chance de montar c.adm. Sem esta
	// barreira, aoAtribuir tocava c.adm nil nessa janela e o processo morria
	// de SIGSEGV inteiro — não uma falha de ingestão.
	//
	// Fecha assim que c.adm é atribuído (logo após kgo.NewClient devolver com
	// sucesso), e não no fim do construtor: bloquear pelo tempo de
	// resolveOrigemEPosicao (que faz I/O de rede) atrasaria o rebalance sem
	// necessidade, e um erro ali chamaria cl.Close() antes de um defer no fim
	// da função ter a chance de abrir o gate — cl.Close() esperaria a
	// goroutine da sessão do group sair do callback, e o callback esperaria
	// o gate para sempre: deadlock. Fechando o gate no instante em que c.adm
	// fica válido, esse caminho de erro nunca vê o gate ainda fechado.
	admPronto chan struct{}

	// offsetsPronto barra ajustaOffsets até c.offsetsIniciais ter seu valor
	// final (débito 69 — a janela-irmã do 60, achada pela revisão do
	// fechamento dele).
	//
	// O franz-go chama onAssigned (aoAtribuir) e SÓ DEPOIS — numa goroutine
	// própria, disparada assim que aoAtribuir retorna — busca os offsets do
	// group e invoca AdjustFetchOffsetsFn (ajustaOffsets). admPronto fecha
	// logo depois de kgo.NewClient devolver, bem ANTES de
	// resolveOrigemEPosicao terminar de escrever c.offsetsIniciais (que faz
	// I/O de rede e de disco): aoAtribuir já pode ter retornado — e a
	// goroutine de fetchOffsets já pode estar chamando ajustaOffsets —
	// enquanto resolveOrigemEPosicao ainda está no meio da escrita do mapa.
	// Sem uma barreira própria para este campo, isso é leitura concorrente
	// com mapassign — não "mapa nil degrada com segurança" como a triagem
	// original do débito 69 presumiu: o -race pegou isso ao vivo no CI.
	//
	// Fecha assim que resolveOrigemEPosicao retorna — sucesso OU erro —, e
	// SEMPRE antes do cl.Close() do caminho de erro logo em seguida, pela
	// mesma razão do admPronto: se o gate só fechasse depois (ou nunca, no
	// caminho de erro), o Close esperaria a goroutine de fetchOffsets —
	// presa neste gate — para a sessão do group terminar de sair: deadlock.
	//
	// Deliberadamente um gate PRÓPRIO, e não o mesmo admPronto: aoAtribuir
	// não toca offsetsIniciais, e fundir os dois atrasaria o
	// totalDeParticoes de aoAtribuir (uma chamada de rede) até
	// resolveOrigemEPosicao terminar, quando as duas hoje correm em
	// paralelo — uma no callback do group, outra na goroutine do construtor.
	offsetsPronto chan struct{}

	// wPronto barra aoRevogar até c.w existir (débito 70 — o terceiro da
	// família 60/69, achado pela varredura que a revisão do 69 pediu sobre
	// os demais callbacks).
	//
	// c.w só é atribuído ao FIM de Abre, depois de resolveOrigemEPosicao E
	// de NovaFila. A checagem `if c.w == nil` em aoRevogar impede o
	// nil-deref, mas não sincroniza a LEITURA do ponteiro com a escrita: o
	// loop do group nasce assíncrono no metadata loop (mesma raiz do 60) e
	// pode completar join→assign→heartbeat→revoke — `manageFailWait` chama
	// onRevoked incondicionalmente ao perceber o contexto do group cancelado
	// — antes de Abre chegar a atribuir c.w. É a mesma leitura-sem-barreira
	// que o -race provou real no 69, agora sobre um ponteiro em vez de um
	// mapa: sem esta barreira, o "degrada com segurança" do nil-check não é
	// garantia de sincronização, só de não panicar.
	//
	// Reordenar a atribuição de c.w para mais cedo NÃO é opção: NovoWriter
	// depende de c.fila (criada depois de resolveOrigemEPosicao) e de
	// c.offsetsIniciais (escrita POR resolveOrigemEPosicao), então c.w não
	// pode existir antes desse ponto — a barreira é a única saída, como já
	// era para admPronto e offsetsPronto.
	//
	// Fecha em TODO caminho de saída de Abre, sempre antes de qualquer
	// cl.Close() dentro da própria função: Close() chama LeaveGroupContext e
	// espera `<-c.g.left`, e o caminho de erro do group (`manageFailWait`,
	// acionado pelo cl.ctx cancelado que o Close provoca) chama onRevoked
	// SINCRONAMENTE antes de o group terminar de sair — se aoRevogar ficasse
	// preso neste gate, `c.g.left` nunca fecharia e Close() travaria para
	// sempre. Um gate próprio, não fundido aos outros dois, pela mesma razão
	// de latência do offsetsPronto: aoRevogar não depende de c.adm nem de
	// offsetsIniciais, e um gate único atrasaria os três até o mais lento.
	wPronto chan struct{}

	fecha sync.Once
}

// Opcoes reúne o que o consumidor precisa de fora.
type Opcoes struct {
	Store    Store
	Mat      *materializer.Materializer
	Log      *logging.Logger
	Metrics  *metrics.Registry
	Cadencia Cadencia
	// ClienteKgo permite ao teste injetar opções extras (kfake usa isto para
	// apontar para o broker em memória).
	ClienteKgo []kgo.Opt
}

// Abre monta o consumidor e valida tudo o que pode ser validado ANTES de
// consumir o primeiro evento.
//
// A ordem das checagens importa: identidade e posição são conferidas antes de
// qualquer fetch, porque um seek num offset de outro fluxo não tem sintoma —
// o consumo simplesmente começa no meio.
func Abre(ctx context.Context, cfg *Config, o Opcoes) (*Consumidor, error) {
	log := o.Log
	if log == nil {
		log = logging.Descartado()
	}
	log = log.Component("kafka").Dataset(cfg.Dataset)
	met := registraMetricas(o.Metrics, cfg.Dataset)

	c := &Consumidor{
		cfg: cfg, log: log, met: met, origem: cfg.Origem(),
		admPronto:     make(chan struct{}),
		offsetsPronto: make(chan struct{}),
		wPronto:       make(chan struct{}),
	}

	opts := append(cfg.opcoesDoCliente(),
		kgo.OnPartitionsAssigned(c.aoAtribuir),
		kgo.OnPartitionsRevoked(c.aoRevogar),
		kgo.AdjustFetchOffsetsFn(c.ajustaOffsets),
	)
	opts = append(opts, o.ClienteKgo...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		// kgo.NewClient só falha antes de qualquer goroutine do cliente
		// nascer (validação de opções), então aoAtribuir, ajustaOffsets e
		// aoRevogar nunca tiveram chance de rodar aqui — mas fechar os três
		// gates de todo caminho de saída, sem exceção, é o invariante que
		// evita reintroduzir o débito 60/69/70 por omissão num caminho
		// futuro.
		close(c.admPronto)
		close(c.offsetsPronto)
		close(c.wPronto)
		return nil, fmt.Errorf("kafka: criar cliente do dataset %q: %w", cfg.Dataset, err)
	}
	c.cl = cl
	c.adm = kadm.NewClient(cl)
	close(c.admPronto)

	// c.offsetsIniciais ganha seu valor final dentro de resolveOrigemEPosicao
	// (ou fica nil, se ela falhar). offsetsPronto fecha logo em seguida, e
	// SEMPRE antes do cl.Close() abaixo — nunca depois, pela razão do
	// deadlock documentada no campo.
	err = c.resolveOrigemEPosicao(ctx, o.Store)
	close(c.offsetsPronto)
	if err != nil {
		// c.w nunca vai existir para este Consumidor (a construção parou
		// aqui): libera aoRevogar ANTES do cl.Close() que já vem a seguir —
		// Close espera o group sair, e sair pode passar por onRevoked.
		close(c.wPronto)
		cl.Close()
		return nil, err
	}

	c.fila = NovaFila(cfg.QueueSize, cfg.QueueBytes)
	met.OlhaAFila(c.fila.Profundidade)
	c.w = NovoWriter(cfg, o.Mat, o.Store, c.fila, log, met, c.offsetsIniciais, o.Cadencia, c.commitNoBroker)
	close(c.wPronto)
	return c, nil
}

// resolveOrigemEPosicao completa a identidade com o cluster id, confere a
// divergência (D14) e resolve de onde retomar (D11).
func (c *Consumidor) resolveOrigemEPosicao(ctx context.Context, st Store) error {
	md, err := c.adm.Metadata(ctx, c.cfg.Topic)
	if err != nil {
		c.met.errosKafka.Inc()
		return fmt.Errorf("kafka: metadados do tópico %q: %w", c.cfg.Topic, err)
	}
	c.origem.ClusterID = md.Cluster

	persistidos, existe, err := st.SourceOffsets()
	switch {
	case err != nil:
		// Arquivo existia e estava ilegível. Tratar como "sem posição" faria
		// exatamente o que ErrPosicaoDesconhecida existe para impedir.
		return fmt.Errorf("%w: %w", ErrPosicaoDesconhecida, err)

	case existe:
		if campo := persistidos.Origem.Divergencia(c.origem); campo != "" {
			return fmt.Errorf("%w: %s mudou (persistido %q, configurado %q); "+
				"use `nishidb offsets` para reposicionar deliberadamente",
				ErrOrigemDivergente, campo,
				valorDoCampo(persistidos.Origem, campo), valorDoCampo(c.origem, campo))
		}
		c.offsetsIniciais = persistidos.Offsets

	default:
		// Sem posição local. Só é seguro aplicar o autoOffsetReset num
		// dataset VAZIO: com linhas, "sem offsets" é estado anômalo.
		if n := st.RowCount(); n > 0 {
			return fmt.Errorf("%w: %d linhas materializadas sem posição conhecida; "+
				"autoOffsetReset=%s reprocessaria ou pularia uma janela arbitrária em silêncio. "+
				"Use `nishidb offsets` para decidir explicitamente",
				ErrPosicaoDesconhecida, n, c.cfg.AutoOffsetReset)
		}
		c.offsetsIniciais = map[int32]int64{}
		c.log.Info("posicao.inicial", "dataset vazio e sem offsets locais; vale o autoOffsetReset",
			"autoOffsetReset", c.cfg.AutoOffsetReset)
	}

	// A existência do tópico é conferida DEPOIS da divergência de origem, e a
	// ordem é deliberada: quando o operador aponta o dataset para um tópico
	// que não existe, as duas coisas são verdade, mas "a origem mudou" é a
	// CAUSA e "o tópico não existe" é a consequência. Reportar a consequência
	// mandaria criar o tópico errado.
	//
	// A checagem em si existe porque, sem ela, o consumer sobe, não recebe
	// assignment nenhum e fica sentado: o health reporta UP, as métricas
	// mostram 0 partições, e nada é consumido — o operador vê tudo verde com o
	// dataset parado. A guarda de assignment não pega o caso, porque compara
	// atribuídas com total e 0 == 0.
	//
	// Recusar torna a ordem de subida relevante (o tópico precisa existir
	// antes do NishiDB), e é a troca certa: um consumer silenciosamente ocioso
	// é pior que uma recusa com o nome do tópico na mensagem.
	d, ok := md.Topics[c.cfg.Topic]
	switch {
	case !ok:
		return fmt.Errorf("%w: o tópico %q não existe no cluster", ErrTopicoAusente, c.cfg.Topic)
	case d.Err != nil:
		return fmt.Errorf("%w: tópico %q: %w", ErrTopicoAusente, c.cfg.Topic, d.Err)
	case len(d.Partitions) == 0:
		return fmt.Errorf("%w: o tópico %q não tem partição nenhuma", ErrTopicoAusente, c.cfg.Topic)
	}
	c.met.partTotal.Set(float64(len(d.Partitions)))
	return nil
}

func valorDoCampo(o checkpoint.Origem, campo string) string {
	switch campo {
	case "topic":
		return o.Topic
	case "consumer.groupId":
		return o.GroupID
	case "bootstrapServers":
		return o.Bootstrap
	case "cluster":
		return o.ClusterID
	}
	return ""
}

// aoAtribuir faz o seek pela posição LOCAL (§49) e aplica a guarda de
// instância única (D10).
//
// O seek é explícito e incondicional para toda partição com posição
// conhecida: o offset commitado no group é secundário, e deixá-lo posicionar
// o consumer faria o dataset divergir do que o Kafka acha que já foi
// entregue.
func (c *Consumidor) aoAtribuir(ctx context.Context, _ *kgo.Client, atribuidas map[string][]int32) {
	// Débito 60: o group pode entregar partições antes de c.adm existir.
	// admPronto fecha assim que Abre atribui c.adm, e nunca antes — ver o
	// comentário do campo.
	<-c.admPronto

	partes := atribuidas[c.cfg.Topic]
	c.met.partAtribuidas.Set(float64(len(partes)))

	total, err := c.totalDeParticoes(ctx)
	if err != nil {
		c.met.errosKafka.Inc()
		c.log.Warn("particoes.desconhecidas",
			"não foi possível confirmar o total de partições do tópico; a guarda de instância única não pôde ser aplicada",
			"erro", err.Error())
	} else if len(partes) != total {
		c.erroAssign.Store(fmt.Errorf("%w: o group %q entregou %d de %d partições de %q — "+
			"o v1 é single-writer (§27) com checkpoint local autoritativo (§49), o que exige "+
			"UMA instância ativa por group; com duas, cada dataset materializa estado parcial e "+
			"nenhuma métrica acusa",
			ErrAssignmentParcial, c.cfg.GroupID, len(partes), total, c.cfg.Topic))
		c.interrompePoll()
		return
	}

	c.log.Info("particoes.atribuidas", "partições atribuídas pelo group", "particoes", len(partes))
}

// ajustaOffsets é o SEEK do §49: a posição persistida localmente sobrepõe o
// offset commitado no group, em toda partição que já tenha posição conhecida.
//
// Tem de ser aqui, e não no callback de assignment, por uma limitação
// documentada do cliente: SetOffsets "só ajusta partições previamente
// consumidas", então chamá-lo num join novo é silenciosamente inócuo — o
// consumo seguiria pelo offset do broker, e o sintoma seria um dataset que
// nunca reprocessa o que deveria. Este hook roda depois de o cliente buscar
// os offsets do group e ANTES de a primeira fetch começar, que é exatamente a
// janela que o §49 descreve.
//
// WithEpoch(-1) desliga a detecção de data loss do cliente: reposicionar para
// trás é a operação PRETENDIDA aqui, e com epoch não negativo o cliente a
// trataria como truncamento do log.
func (c *Consumidor) ajustaOffsets(_ context.Context, doGroup map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	// Débito 69: fetchOffsets (que chama este callback) pode disparar assim
	// que aoAtribuir retorna, antes de resolveOrigemEPosicao terminar de
	// escrever c.offsetsIniciais. offsetsPronto fecha só depois — ver o
	// comentário do campo.
	<-c.offsetsPronto

	if len(c.offsetsIniciais) == 0 {
		return doGroup, nil
	}
	porParticao, ok := doGroup[c.cfg.Topic]
	if !ok {
		return doGroup, nil
	}
	ajustado := make(map[int32]kgo.Offset, len(porParticao))
	usados := make([]int32, 0, len(porParticao))
	for p, o := range porParticao {
		local, temLocal := c.offsetsIniciais[p]
		if !temLocal {
			// Partição sem posição local (tópico cresceu depois do último
			// checkpoint): vale o que o group diz, ou o autoOffsetReset.
			ajustado[p] = o
			continue
		}
		ajustado[p] = kgo.NewOffset().At(local).WithEpoch(-1)
		usados = append(usados, p)
	}
	saida := make(map[string]map[int32]kgo.Offset, len(doGroup))
	for t, v := range doGroup {
		saida[t] = v
	}
	saida[c.cfg.Topic] = ajustado

	sort.Slice(usados, func(i, j int) bool { return usados[i] < usados[j] })
	for _, p := range usados {
		c.met.posicaoDe(p).Store(c.offsetsIniciais[p])
	}
	c.log.Info("offsets.seek", "retomando pela posição do checkpoint local (§49)",
		"particoes", len(usados))
	return saida, nil
}

// aoRevogar torna durável o que já entrou antes de perder as partições.
//
// Com BlockRebalanceOnPoll, este callback só roda quando o lote corrente já
// foi processado, então não há evento em voo na FILA. O checkpoint aqui é o
// que evita reprocessar desde o último ponto durável a cada rebalance.
//
// CUIDADO, e a correção que este comentário já induziu ao erro uma vez: fila
// vazia NÃO quer dizer goroutine parada. O writer segue vivo no select de
// recebeOuAcorda e o despertador pode acordá-lo para talvezCheckpoint a
// qualquer instante — inclusive agora. Este checkpoint roda na goroutine do
// GROUP do franz-go, não na do writer, e as duas se encontram em offsets,
// desdeCP e ultimoCP. É por isso que o Writer tem o posMu; ver o godoc dele.
func (c *Consumidor) aoRevogar(ctx context.Context, _ *kgo.Client, revogadas map[string][]int32) {
	// Débito 70: o group pode revogar antes de c.w existir (a mesma raiz
	// assíncrona do 60/69). wPronto fecha só depois de Abre atribuir c.w —
	// ver o comentário do campo.
	<-c.wPronto

	if c.w == nil {
		return
	}
	if err := c.w.checkpoint(ctx); err != nil {
		c.log.Error("checkpoint.falhou", "checkpoint na revogação falhou; o próximo dono reprocessa desde o último ponto durável",
			"erro", err.Error())
	}
	c.met.partAtribuidas.Set(0)
	c.log.Info("particoes.revogadas", "partições revogadas pelo group",
		"particoes", len(revogadas[c.cfg.Topic]))
}

func (c *Consumidor) totalDeParticoes(ctx context.Context) (int, error) {
	d, err := c.adm.ListTopics(ctx, c.cfg.Topic)
	if err != nil {
		return 0, err
	}
	t, ok := d[c.cfg.Topic]
	if !ok {
		return 0, fmt.Errorf("tópico %q não existe no cluster", c.cfg.Topic)
	}
	if t.Err != nil {
		return 0, t.Err
	}
	return len(t.Partitions), nil
}

// commitNoBroker publica a posição no group DEPOIS do checkpoint durável.
//
// O commit existe para lag e para ferramentas de terceiros; ele NUNCA
// posiciona o consumer no boot (§49). Por isso a falha aqui é registrada e
// não interrompe nada.
func (c *Consumidor) commitNoBroker(ctx context.Context, offsets map[int32]int64) error {
	if len(offsets) == 0 {
		return nil
	}
	m := make(map[int32]kgo.EpochOffset, len(offsets))
	for p, off := range offsets {
		m[p] = kgo.EpochOffset{Epoch: -1, Offset: off}
	}
	// CommitOffsetsSync não devolve erro: o resultado chega no callback. É
	// síncrono, então ler a variável depois do retorno é seguro.
	var erroCommit error
	c.cl.CommitOffsetsSync(ctx,
		map[string]map[int32]kgo.EpochOffset{c.cfg.Topic: m},
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				erroCommit = err
				return
			}
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if p.ErrorCode != 0 {
						erroCommit = fmt.Errorf("partição %d: código de erro %d", p.Partition, p.ErrorCode)
						return
					}
				}
			}
		})
	return erroCommit
}

// Roda executa o pipeline até o contexto acabar ou a ingestão parar.
func (c *Consumidor) Roda(ctx context.Context) error {
	ctx, cancela := context.WithCancel(ctx)
	defer cancela()
	c.cancelaPoll.Store(cancela)

	erroWriter := make(chan error, 1)
	go func() {
		err := c.w.Roda(ctx)
		erroWriter <- err
		if err != nil {
			// Acorda o poll: a pausa do writer só seria vista quando o
			// PollRecords voltasse, e ele pode não voltar tão cedo — um
			// tópico sem tráfego novo deixaria o consumer "parado" sem
			// ninguém saber por um prazo inteiro de fetch.
			cancela()
		}
	}()

	// O watchdog cancela o ctx quando conclui que não há progresso, e é isso
	// que acorda um PollRecords travado. Ele guarda a causa ANTES de
	// cancelar, porque o poll devolve nil sob cancelamento ("encerramento
	// pedido; não é falha") — sem a causa guardada, a recriação viraria um
	// encerramento limpo e ninguém tentaria de novo.
	go func() {
		if err := c.vigia(ctx, c.cfg.WatchdogTimeout); err != nil {
			c.erroVigia.Store(err)
			cancela()
		}
	}()

	erro := c.poll(ctx)

	// Fecha a fila para o writer drenar o que sobrou e checkpointar.
	c.fila.Fecha()
	if e := <-erroWriter; e != nil && erro == nil {
		erro = e
	}
	// A causa do watchdog vence o nil do poll, e perde para um erro real: uma
	// falha de assignment ou do writer descreve melhor o que houve.
	if v := c.erroVigia.Load(); v != nil && erro == nil {
		erro = v.(error)
	}
	return erro
}

// poll é o laço do §27: um fetch por vez, tudo para a fila limitada.
func (c *Consumidor) poll(ctx context.Context) error {
	for {
		// A ordem importa: o cancelamento pode ter vindo justamente de uma
		// recusa de assignment, e devolver nil por causa do contexto
		// esconderia a causa real.
		if err := c.assignmentInvalido(); err != nil {
			return err
		}
		if p := c.w.Pausa(); p != nil {
			return p
		}
		if ctx.Err() != nil {
			return nil // encerramento pedido; não é falha
		}

		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := c.registraErrosDeFetch(fetches); err != nil {
			c.cl.AllowRebalance()
			return err
		}
		if err := c.assignmentInvalido(); err != nil {
			c.cl.AllowRebalance()
			return err
		}

		enfileirados := 0
		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() != nil {
				return
			}
			if err := c.fila.Envia(ctx, r); err == nil {
				enfileirados++
			}
		})
		if enfileirados > 0 {
			// Espera o writer decidir o lote inteiro antes de liberar o
			// rebalance: é isso que garante que o commit seguinte só toque
			// partições que ainda são nossas.
			marca := make(chan struct{})
			if err := c.fila.EnviaMarca(ctx, marca); err == nil {
				select {
				case <-marca:
				case <-ctx.Done():
				}
			}
		}
		c.cl.AllowRebalance()
	}
}

// interrompePoll acorda o PollRecords para que a falha seja vista agora.
func (c *Consumidor) interrompePoll() {
	if v := c.cancelaPoll.Load(); v != nil {
		v.(context.CancelFunc)()
	}
}

func (c *Consumidor) assignmentInvalido() error {
	if v := c.erroAssign.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// registraErrosDeFetch trata erro de BROKER no balde próprio (D8).
//
// Fica antes e fora do Apply de propósito: o classify do materializer tem
// `default: CategoryStorage`, então um erro de rede que passasse por lá seria
// contado como perda de dado — diagnóstico errado que levaria o operador a
// procurar corrupção onde há indisponibilidade.
func (c *Consumidor) registraErrosDeFetch(f kgo.Fetches) error {
	var fatal error
	f.EachError(func(topico string, particao int32, err error) {
		c.met.errosKafka.Inc()
		if errors.Is(err, context.Canceled) {
			return
		}
		c.log.Warn("fetch.erro", "erro de fetch; o cliente tenta de novo com backoff próprio",
			"topico", topico, "partition", particao, "erro", err.Error())
		if errors.Is(err, kgo.ErrClientClosed) {
			fatal = err
		}
	})
	return fatal
}

// AtualizaMarcaDagua lê o fim do log no broker (§92). É chamado pelo scrape, e
// não por um timer: consultar num timer publicaria um número velho cuja idade
// o operador não conhece.
//
// ListEndOffsets, e não kadm.Lag. São duas mudanças numa:
//
//  1. CORRETUDE. kadm.Lag devolve o lag do offset COMMITADO no group, e o
//     commit só sai depois do checkpoint durável (§48). Entre dois checkpoints
//     a série crescia à taxa de produção com o consumidor em dia — ver o godoc
//     de amostrasDaPosicao para a medição no piloto.
//  2. CUSTO. kadm.Lag é um processo de TRÊS requisições (DescribeGroups,
//     FetchOffsets, ListEndOffsets). Esta é UMA. O scrape passa a custar um
//     terço do que já custava; não há trade-off a negociar aqui.
//
// Erro por PARTIÇÃO omite só aquela partição, não a leitura inteira: uma
// partição sem líder não é razão para o painel perder as outras.
func (c *Consumidor) AtualizaMarcaDagua(ctx context.Context) {
	fins, err := c.adm.ListEndOffsets(ctx, c.cfg.Topic)
	if err != nil {
		c.met.errosKafka.Inc()
		return
	}
	m := make(map[int32]int64)
	fins.Each(func(o kadm.ListedOffset) {
		if o.Err != nil || o.Topic != c.cfg.Topic || o.Offset < 0 {
			return
		}
		m[o.Partition] = o.Offset
	})
	if len(m) == 0 {
		// Nada utilizável: preserva a marca anterior e deixa a validade
		// decidir. Publicar um mapa vazio faria as séries sumirem por uma
		// resposta ruim, e não por falta de resposta.
		c.met.errosKafka.Inc()
		return
	}
	c.met.publicaMarca(m, time.Now())
}

// Pausa devolve o motivo da parada da ingestão, ou nil.
func (c *Consumidor) Pausa() error { return c.w.Pausa() }

// Desfecho devolve os contadores da ingestão.
func (c *Consumidor) Desfecho() Desfecho { return c.w.Desfecho() }

// Fecha encerra o cliente. Idempotente.
func (c *Consumidor) Fecha() {
	c.fecha.Do(func() {
		if c.cl != nil {
			c.cl.Close()
		}
	})
}
