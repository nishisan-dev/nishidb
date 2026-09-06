// Package config carrega e valida o nishidb.yaml (spec §78).
//
// Vive em pacote próprio, e não dentro do cli, porque tanto os comandos
// quanto o servidor precisam dele: com a configuração morando no cli, o
// servidor importaria o cli e o cli importaria o servidor para o dispatch —
// um ciclo. O §98 já previa internal/config/ por este motivo.
package config

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/units"
)

// Config é o subconjunto do nishidb.yaml (spec §78) que os comandos do M1
// precisam.
//
// As seções que o M1 não implementa (runtime, storage.durability) são
// ACEITAS e ignoradas: o arquivo que o M3 vai usar não pode ser inválido
// para o binário do M1, senão o operador mantém dois arquivos e eles
// divergem. server.tls e server.authentication SAIRAM dessa lista na Fase
// 11: continuam opcionais, mas quando presentes têm forma validada — ver
// TLSSpec/AuthenticationSpec/LoopbackSpec, em config_security.go.
type Config struct {
	// Version é a versão do PRÓPRIO arquivo (§78). Era aceita e nunca lida:
	// no dia em que existisse um `version: 2` global, este binário leria o
	// arquivo novo como se fosse v1, silenciosamente.
	Version *int `yaml:"version"`

	Storage struct {
		Path        string `yaml:"path"`
		SegmentSize string `yaml:"segmentSize"`
		Checkpoint  struct {
			Interval string `yaml:"interval"`
		} `yaml:"checkpoint"`

		// Compaction é o bloco §78 que decide se o disco para de crescer. Aqui
		// se valida a FORMA; o comportamento entra na ativação, no motor
		// (doutrina do débito 49).
		//
		// Enabled é PONTEIRO pelo mesmo motivo de server.enabled: ausente e
		// `false` significam coisas opostas. Ausente vale TRUE — o §78 traz a
		// chave ligada no exemplo, e exigir declaração explícita para ter
		// compaction inverteria o default seguro, deixando todo YAML herdado do
		// M1 acumular garbage até o ENOSPC que o §86 projeta em 2 a 3 dias.
		// `enabled: false` é o operador desligando deliberadamente, e aí a
		// decisão dele vale.
		//
		// MinimumLiveRatio é ponteiro para separar "ausente" de zero, que aqui
		// não é omissão: é valor fora da faixa, e precisa ser recusado com a
		// mesma clareza de um negativo.
		Compaction struct {
			Enabled          *bool    `yaml:"enabled"`
			MinimumLiveRatio *float64 `yaml:"minimumLiveRatio"`
		} `yaml:"compaction"`

		// Expiracao é o freio da coleta do idle timeout. Os dois limites
		// valem JUNTOS e a passada termina no que bater primeiro: o de
		// quantidade protege o disco e o lock de escrita (marcar uma
		// entidade como expirada é um APPEND, então uma leva grande escreve
		// muito antes de a compaction recuperar qualquer coisa), e o de tempo
		// protege do caso oposto — varrer milhões de entradas sem achar nada
		// que vença, que não escreve nada mas segura a goroutine.
		//
		// Ponteiros pelo mesmo motivo do MinimumLiveRatio: zero aqui não é
		// omissão, é "não faça nada", e precisa ser recusado com clareza em
		// vez de virar um coletor que nunca coleta.
		Expiracao struct {
			MaxPorPassada   *int   `yaml:"maxPorPassada"`
			TempoPorPassada string `yaml:"tempoPorPassada"`
		} `yaml:"expiracao"`

		// Durability é o bloco §47 (débito 50): `mode: sync|batch|async` com
		// `interval`, e nenhum dos dois tem implementação — só existe o
		// SyncEveryPut interno, que o CLI nunca liga. Os campos ficam CRUS
		// (string, sem parse de duração nem validação de enum) porque aqui só
		// se detecta PRESENÇA para poder avisar; interpretar `mode`/`interval`
		// de verdade é o resto do débito 50, e não é escopo desta chave.
		Durability struct {
			Mode     string `yaml:"mode"`
			Interval string `yaml:"interval"`
		} `yaml:"durability"`
	} `yaml:"storage"`

	Datasets struct {
		Directory string `yaml:"directory"`
	} `yaml:"datasets"`

	// Server é o bloco §78 do processo de longa duração. Continua opcional:
	// `nishidb ingest` e `nishidb query` não o usam.
	//
	// Enabled é PONTEIRO para separar "ausente" de "false", e os dois têm
	// significados opostos: rodar `nishidb server` já É o ato de ligar, então
	// exigir a chave também seria cerimônia dupla sem segurança nenhuma em
	// troca — ausente vale true. Já `enabled: false` é o operador desligando
	// deliberadamente, e aí o comando recusa em vez de ignorar a decisão.
	Server struct {
		Enabled *bool  `yaml:"enabled"`
		Bind    string `yaml:"bind"`
		Port    *int   `yaml:"port"`

		// TLS, Authentication e Loopback são os blocos de segurança da Fase 11
		// (§74-78). A FORMA de cada um é validada em validaServerSeguranca
		// (config_security.go): whitelist manual de chave dentro do bloco (o
		// KnownFields GLOBAL continua desligado, ver comentário em
		// CarregaConfig) e as recusas cruzadas do §75/§77 que dependem só do
		// arquivo. A resolução de segredos e a matriz de recusa "auth sem TLS
		// com bind exposto" (§77) são ativação — B2, em server.Novo.
		TLS            TLSSpec            `yaml:"tls"`
		Authentication AuthenticationSpec `yaml:"authentication"`
		Loopback       LoopbackSpec       `yaml:"loopback"`
	} `yaml:"server"`

	// Runtime é o bloco §79/§80, e desde o fechamento de metade do débito 15
	// quase todo ele tem leitor: query.maxConcurrent é o teto de consultas
	// simultâneas do §80, query.maxLongRunning é o sub-pool da consulta longa,
	// e memoryTarget/query.maxBufferPerQuery são o orçamento de memória de
	// consulta (internal/query/budget), cujo primeiro consumidor real é o
	// StreamWriter.
	//
	// managedMemoryTarget continua aceita e sem efeito, e não em silêncio:
	// quem a escreve conclui que há um teto que não há, e a subida avisa — no
	// mesmo molde de storage.durability (débito 50, ChavesDeStorageIgnoradas).
	Runtime struct {
		MemoryTarget        string `yaml:"memoryTarget"`
		ManagedMemoryTarget string `yaml:"managedMemoryTarget"`
		Query               struct {
			MaxConcurrent *int `yaml:"maxConcurrent"`
			// MaxLongRunning é o sub-pool da query LONGA, DENTRO do
			// maxConcurrent. Sem ele, dar prazo longo por padrão faria 16
			// consultas longas ocuparem o semáforo inteiro e TODA consulta
			// curta receber 503 — o teto do §80 protegeria a ingestão e
			// entregaria a consulta interativa no lugar dela.
			MaxLongRunning    *int   `yaml:"maxLongRunning"`
			MaxBufferPerQuery string `yaml:"maxBufferPerQuery"`
			// SnapshotMaxBytes é o teto de heap do journal de VERSÕES: a
			// memória que o isolamento de leitura pode reter enquanto
			// consultas estão abertas. Ele completa o orçamento do §80 —
			// maxBufferPerQuery limita o que UMA consulta bufferiza, e este
			// limita o que TODAS elas juntas fazem o writer segurar.
			//
			// Estourado, é a consulta mais antiga que é invalidada
			// (SNAPSHOT_TOO_OLD), nunca a ingestão que é barrada.
			SnapshotMaxBytes string `yaml:"snapshotMaxBytes"`
		} `yaml:"query"`
	} `yaml:"runtime"`

	// query.defaultMaxRows e query.defaultTimeout são os DEFAULTS GLOBAIS do
	// teto de linhas e do timeout de consulta: valem para o dataset que omite
	// `query.maxRows`/`query.timeout` no YAML dele, e perdem para o dataset
	// que os declara. Chegam ao motor por CarregaConfig →
	// Config.DefaultsDeQuery → schema.Load.
	//
	// DefaultMaxRows é ponteiro para separar "ausente" de "zero": ausente
	// herda o padrão da spec, zero é valor inválido e precisa ser recusado
	// com a mesma clareza de um negativo.
	Query struct {
		DefaultMaxRows *int   `yaml:"defaultMaxRows"`
		DefaultTimeout string `yaml:"defaultTimeout"`
		// DefaultMaxTimeout é o default global do TETO de prazo — a chave que
		// torna a query longa cidadã de primeira classe sem dar prazo longo a
		// quem nunca o pediu. Ausente (string vazia) NÃO vira 30s: vira zero,
		// e zero significa "cada dataset herda o próprio query.timeout
		// resolvido" (ver schema.Defaults.MaxTimeout). É isso que faz um
		// nishidb.yaml de hoje produzir comportamento idêntico ao de antes de
		// a chave existir.
		DefaultMaxTimeout string `yaml:"defaultMaxTimeout"`
		ResultBatchSize   *int   `yaml:"resultBatchSize"`
	} `yaml:"query"`

	// Resolvidos a partir dos campos textuais acima.
	segmentSize        int64
	checkpointInterval time.Duration
	defaultMaxRows     int
	defaultTimeout     time.Duration
	defaultMaxTimeout  time.Duration
	resultBatchSize    int
	serverPort         int
	maxConcurrent      int
	maxLongRunning     int
	memoryTarget       int64
	maxBufferPerQuery  int64
	snapshotMaxBytes   int64
	minimumLiveRatio   float64
	expiracaoMax       int
	expiracaoTempo     time.Duration

	// authenticationDeclarada é fato do DOCUMENTO (a chave server.authentication
	// apareceu, com qualquer valor de enabled), e não do valor decodificado —
	// por isso não cabe em AuthenticationSpec, que só enxerga o que foi
	// decodificado. Ver AuthenticationDeclarada.
	authenticationDeclarada bool
}

const (
	segmentSizePadrao        = 512 << 20
	checkpointIntervalPadrao = 30 * time.Second
	maxRowsPadrao            = 100000
	timeoutPadrao            = 30 * time.Second
	resultBatchPadrao        = 256
	serverPortPadrao         = 9142
	maxConcurrentPadrao      = 16

	// maxLongRunningPadrao é DOIS, e não uma fração do maxConcurrent.
	//
	// Duas porque o sub-pool existe para garantir que a consulta interativa
	// sempre tenha vaga, e uma fração faria o pior caso crescer junto com o
	// teto geral — com `maxConcurrent: 64`, um quarto seriam 16 consultas
	// longas segurando 16 vagas do pool geral, que é exatamente o cenário que
	// o sub-pool veio impedir. Duas cobrem o caso real (um relatório e um
	// export em paralelo) e deixam 14 das 16 vagas do default sempre
	// disponíveis para quem responde em milissegundos.
	maxLongRunningPadrao = 2

	// Os dois defaults do §80, literais da spec: 512 MiB de orçamento de
	// memória de consulta para o processo e 4 MiB de buffer por consulta.
	//
	// A relação entre eles não é arbitrária: 4 MiB × os 16 do maxConcurrent
	// padrão são 64 MiB, um oitavo do alvo — folga suficiente para que o
	// orçamento por consulta seja o que de fato recusa a linha patológica, e
	// não o global, que recusaria por culpa de uma consulta a consulta de
	// outro cliente.
	memoryTargetPadrao      = 512 << 20
	maxBufferPerQueryPadrao = 4 << 20
	// snapshotMaxBytesPadrao são ~2,7 milhões de versões retidas: folga larga
	// para o relatório de minutos que a consulta longa serve, e pequeno o
	// bastante para que uma consulta esquecida seja RECUSADA em vez de
	// competir com o 1 GiB de RSS do §97.
	snapshotMaxBytesPadrao = 64 << 20

	// Defaults da coleta de expiração. Mil marcações por passada a 452 ev/s
	// do piloto são ~136 KB escritos por passada — ruído perto da ingestão —,
	// e cinco segundos é a mesma ordem do intervalo entre passadas da
	// compaction, que já se provou uma cadência que não atrapalha o writer.
	expiracaoMaxPorPassadaPadrao   = 1000
	expiracaoTempoPorPassadaPadrao = 5 * time.Second

	// compactionRatioPadrao é o default do §53: um segmento vira candidato
	// quando menos da metade dele ainda é dado vivo.
	compactionRatioPadrao = 0.50

	// serverBindPadrao é LOOPBACK, e não o 0.0.0.0 que o exemplo do §78
	// escreve.
	//
	// A razão é o que passou a existir na Fase 6: até o M2 o servidor só
	// expunha /metrics e /v1/health, que são operacionais e defensáveis numa
	// rede interna. Com POST /v1/query ele expõe OS DADOS, e a autenticação do
	// §74-77 só chega na Fase 11 — um default aberto entregaria leitura
	// irrestrita de todo o dataset a quem alcançasse a porta.
	//
	// Não é contradição com o §78: o exemplo de lá traz `bind: 0.0.0.0` no
	// MESMO arquivo em que liga `tls.enabled` e `authentication.enabled`. O
	// bind aberto da spec pressupõe autenticação; enquanto ela não existe, o
	// default seguro é o loopback e expor é ato explícito de quem opera — que
	// a subida registra com um aviso.
	serverBindPadrao = "127.0.0.1"
)

// CarregaConfig lê e valida o arquivo de configuração.
func CarregaConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("configuração: %w", err)
	}
	// O documento é decodificado primeiro em ÁRVORE CRUA (yaml.Node), e só
	// depois no Config tipado, via raiz.Decode — e não yaml.Unmarshal(b, &c)
	// direto. A árvore é o que validaServerSeguranca usa para a whitelist
	// manual de chave dentro de server.tls/authentication/loopback (recusa 7
	// do §75/§77); yaml.Node.Decode não herda estritume nenhum do documento
	// (mesma observação de internal/schema/source.go), então o resto do
	// arquivo continua exatamente tão tolerante quanto antes.
	var raiz yaml.Node
	if err := yaml.Unmarshal(b, &raiz); err != nil {
		return nil, fmt.Errorf("configuração %s: %w", path, err)
	}
	var c Config
	// KnownFields fica DESLIGADO de propósito: o arquivo do M3 traz seções
	// que este binário não conhece, e recusá-las obrigaria o operador a mantê
	// -las em dois arquivos. Isso vale para o documento INTEIRO — inclusive
	// para dentro de server.tls/authentication/loopback, cuja whitelist é
	// manual (validaServerSeguranca), e não pelo KnownFields do decoder.
	if err := raiz.Decode(&c); err != nil {
		return nil, fmt.Errorf("configuração %s: %w", path, err)
	}

	// A versão é conferida ANTES de qualquer outra chave: ler um arquivo de
	// versão desconhecida com a gramática de hoje é pior que recusá-lo, porque
	// o desvio aparece no comportamento e não na mensagem.
	if c.Version != nil && *c.Version != 1 {
		return nil, fmt.Errorf("configuração %s: version %d não suportada (suportada: 1)", path, *c.Version)
	}

	if c.Storage.Path == "" {
		return nil, fmt.Errorf("configuração %s: storage.path é obrigatório", path)
	}
	if c.Datasets.Directory == "" {
		return nil, fmt.Errorf("configuração %s: datasets.directory é obrigatório", path)
	}

	if c.segmentSize, err = units.Size(c.Storage.SegmentSize, segmentSizePadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.segmentSize: %w", path, err)
	}
	if c.checkpointInterval, err = units.Duration(c.Storage.Checkpoint.Interval, checkpointIntervalPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.checkpoint.interval: %w", path, err)
	}
	if c.minimumLiveRatio, err = razaoDeLiveness(c.Storage.Compaction.MinimumLiveRatio); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.compaction.minimumLiveRatio: %w", path, err)
	}
	if c.expiracaoMax, err = units.PositiveInt(c.Storage.Expiracao.MaxPorPassada, expiracaoMaxPorPassadaPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.expiracao.maxPorPassada: %w", path, err)
	}
	if c.expiracaoTempo, err = units.Duration(c.Storage.Expiracao.TempoPorPassada, expiracaoTempoPorPassadaPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.expiracao.tempoPorPassada: %w", path, err)
	}
	if c.defaultMaxRows, err = units.PositiveInt(c.Query.DefaultMaxRows, maxRowsPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultMaxRows: %w", path, err)
	}
	if c.defaultTimeout, err = units.Duration(c.Query.DefaultTimeout, timeoutPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultTimeout: %w", path, err)
	}
	// O padrão é ZERO, e zero é o sinal de "ausente" que schema.validate lê
	// como "cada dataset herda o próprio query.timeout". Passar timeoutPadrao
	// aqui daria um teto global de 30s a datasets que hoje declaram
	// `query.timeout: 5m` — e a spec deles passaria a ser recusada na abertura.
	if c.defaultMaxTimeout, err = units.Duration(c.Query.DefaultMaxTimeout, 0); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultMaxTimeout: %w", path, err)
	}
	if c.defaultMaxTimeout != 0 && c.defaultMaxTimeout < c.defaultTimeout {
		// A recusa é AQUI, com os nomes GLOBAIS, e não lá na spec do dataset:
		// o par inválido foi escrito neste arquivo, e deixá-lo chegar em
		// schema.validate faria a mensagem nomear `query.maxTimeout`/
		// `query.timeout` — chaves que o operador não encontraria no YAML que
		// ele acabou de editar.
		return nil, fmt.Errorf(
			"configuração %s: query.defaultMaxTimeout (%s) é menor que query.defaultTimeout (%s): "+
				"o teto global recusaria o prazo que ele mesmo dá por padrão",
			path, c.defaultMaxTimeout, c.defaultTimeout)
	}
	if c.resultBatchSize, err = units.PositiveInt(c.Query.ResultBatchSize, resultBatchPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.resultBatchSize: %w", path, err)
	}
	if c.serverPort, err = units.PositiveInt(c.Server.Port, serverPortPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: server.port: %w", path, err)
	}
	if c.maxConcurrent, err = units.PositiveInt(c.Runtime.Query.MaxConcurrent, maxConcurrentPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: runtime.query.maxConcurrent: %w", path, err)
	}
	// O DEFAULT do sub-pool é limitado pelo pool, e a chave EXPLÍCITA não é.
	//
	// A assimetria não é indulgência: `runtime.query.maxConcurrent: 1` é
	// configuração legítima e existente, e um default fixo de 2 mataria a
	// subida dela na primeira troca de binário — a chave nova recusaria um
	// arquivo que ninguém editou. Com uma vaga só no pool não há folga a
	// reservar, e o sub-pool honesto é o pool inteiro. Já quem ESCREVE
	// maxLongRunning acima do maxConcurrent está pedindo algo que o semáforo
	// geral recusaria primeiro, e aí a recusa é o serviço.
	padraoLongo := maxLongRunningPadrao
	if padraoLongo > c.maxConcurrent {
		padraoLongo = c.maxConcurrent
	}
	if c.maxLongRunning, err = units.PositiveInt(c.Runtime.Query.MaxLongRunning, padraoLongo); err != nil {
		return nil, fmt.Errorf("configuração %s: runtime.query.maxLongRunning: %w", path, err)
	}
	if c.maxLongRunning > c.maxConcurrent {
		// O sub-pool vive DENTRO do geral: uma consulta longa toma as duas
		// vagas. Um sub-pool maior que o pool prometeria vagas longas que o
		// semáforo geral recusaria primeiro, e o operador leria o 503 de
		// TOO_MANY_QUERIES onde configurou espaço para consulta longa.
		return nil, fmt.Errorf(
			"configuração %s: runtime.query.maxLongRunning (%d) acima de runtime.query.maxConcurrent (%d): "+
				"a consulta longa toma uma vaga de CADA pool, então o sub-pool não pode ser maior que o pool",
			path, c.maxLongRunning, c.maxConcurrent)
	}
	if c.memoryTarget, err = units.Size(c.Runtime.MemoryTarget, memoryTargetPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: runtime.memoryTarget: %w", path, err)
	}
	if c.maxBufferPerQuery, err = units.Size(c.Runtime.Query.MaxBufferPerQuery, maxBufferPerQueryPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: runtime.query.maxBufferPerQuery: %w", path, err)
	}
	// O DEFAULT escala com o orçamento do processo, e é isso que o separa de
	// uma constante: num processo de 8 MiB, um journal de 64 MiB não é um teto
	// — é o orçamento inteiro entregue a uma consulta. Um oitavo do
	// memoryTarget, limitado pelo teto absoluto, dá uma folga proporcional em
	// qualquer tamanho de máquina.
	//
	// Ele também não pode RECUSAR a subida: uma configuração que hoje sobe tem
	// de continuar subindo depois de a chave existir, e derrubar o processo por
	// causa de um valor que o operador nunca escreveu é o oposto de "config
	// recusa na subida, nunca ignora em silêncio" — não há nada que ele tenha
	// declarado para recusar.
	padrao := min(int64(snapshotMaxBytesPadrao), c.memoryTarget/8)
	if c.snapshotMaxBytes, err = units.Size(c.Runtime.Query.SnapshotMaxBytes, padrao); err != nil {
		return nil, fmt.Errorf("configuração %s: runtime.query.snapshotMaxBytes: %w", path, err)
	}
	// A recusa vale só para o valor DECLARADO, pela razão acima.
	if c.Runtime.Query.SnapshotMaxBytes != "" && c.snapshotMaxBytes > c.memoryTarget {
		return nil, fmt.Errorf(
			"configuração %s: runtime.query.snapshotMaxBytes (%d bytes) acima de runtime.memoryTarget "+
				"(%d bytes): o journal de versões sozinho estouraria o orçamento do processo",
			path, c.snapshotMaxBytes, c.memoryTarget)
	}
	if c.maxBufferPerQuery > c.memoryTarget {
		// Um teto por consulta acima do orçamento do processo é uma promessa
		// que o processo nunca poderia cumprir: a PRIMEIRA consulta a chegar
		// perto do próprio limite já seria recusada pelo global, e o operador
		// leria a recusa como defeito do motor em vez de como a contradição que
		// ele escreveu. Recusar na abertura, nomeando as duas chaves, é o que
		// transforma isso num erro de configuração legível.
		return nil, fmt.Errorf(
			"configuração %s: runtime.query.maxBufferPerQuery (%d bytes) acima de runtime.memoryTarget "+
				"(%d bytes): o orçamento de UMA consulta não pode passar do orçamento do processo inteiro",
			path, c.maxBufferPerQuery, c.memoryTarget)
	}
	if c.serverPort > 65535 {
		return nil, fmt.Errorf("configuração %s: server.port %d fora de 1..65535", path, c.serverPort)
	}
	if c.Server.Bind == "" {
		c.Server.Bind = serverBindPadrao
	}
	if err := validaServerSeguranca(&c, &raiz, path); err != nil {
		return nil, err
	}
	return &c, nil
}

// razaoDeLiveness aplica o default do §53 quando a chave está AUSENTE e recusa
// qualquer valor fora da faixa ABERTA (0, 1).
//
// As duas pontas são exclusivas porque nenhuma delas descreve uma política. Com
// ratio >= 1 todo segmento vira candidato, inclusive o que é só dado vivo: a
// compaction reescreveria o dataset em laço, queimando I/O sem devolver disco.
// Com ratio <= 0 nenhum segmento jamais seria eleito, e a chave mentiria —
// haveria compaction configurada e nada compactado.
//
// O NaN é recusado por NOME, e não pela faixa, porque faixa não o alcança:
// `.nan` é literal de float do YAML e toda comparação com NaN é falsa, então
// `*v <= 0 || *v >= 1` — que diz "está fora" — o deixa passar, enquanto o que
// se queria dizer era "está dentro". E ele cai no pior dos dois lados descritos
// acima: com ratio=NaN o predicado do §53 `liveBytes/totalBytes < ratio` é
// SEMPRE falso, nenhum segmento é eleito, e a chave mente da forma mais cara
// que existe — o operador lê `minimumLiveRatio` no arquivo, conclui que a
// política está valendo, e o disco cresce até o ENOSPC que o §86 projeta em 2 a
// 3 dias. Os infinitos, esses a faixa já pega: +Inf >= 1 e -Inf <= 0.
//
// Não mora em units porque o pacote de lá existe para os escalares que os DOIS
// YAMLs do produto compartilham, e esta faixa é regra de uma chave só.
func razaoDeLiveness(v *float64) (float64, error) {
	if v == nil {
		return compactionRatioPadrao, nil
	}
	if math.IsNaN(*v) || *v <= 0 || *v >= 1 {
		return 0, fmt.Errorf("deve ficar entre 0 e 1, exclusive, é %v", *v)
	}
	return *v, nil
}

// ServidorHabilitado diz se o bloco `server` autoriza a subida. Ausente é
// true; só `enabled: false` explícito recusa.
func (c *Config) ServidorHabilitado() bool {
	return c.Server.Enabled == nil || *c.Server.Enabled
}

// TLSHabilitado diz se server.tls está ligado (§77).
//
// Ao contrário de ServidorHabilitado, aqui ausente é FALSE: TLS é recurso
// novo e opt-in, sem YAML herdado cujo comportamento dependa do oposto.
func (c *Config) TLSHabilitado() bool { return c.Server.TLS.Enabled }

// AutenticacaoHabilitada diz se server.authentication está LIGADO (§74-76).
// Deriva do ponteiro Server.Authentication.Enabled: nil (seção ausente OU
// `enabled` omitido dentro dela) vale FALSE, mesma regra prática de antes —
// só que agora expressa via ponteiro, para que AuthenticationDeclarada possa
// perguntar "a seção apareceu?" com independência do valor de enabled.
func (c *Config) AutenticacaoHabilitada() bool {
	return c.Server.Authentication.Enabled != nil && *c.Server.Authentication.Enabled
}

// AuthenticationDeclarada diz se o bloco server.authentication apareceu no
// YAML, com QUALQUER valor de enabled — inclusive ausente ou `false`.
//
// Existe para a matriz de recusa do §77 (B2, ruling do controlador): bind
// exposto + authentication AUSENTE pede decisão explícita do operador (typo
// engolido não pode liberar um bind aberto sem auth por omissão); bind
// exposto + `enabled: false` EXPLÍCITO é a decisão já tomada, e ela vale —
// sobe com aviso. AutenticacaoHabilitada() sozinho não distingue os dois
// casos (os dois dão FALSE); este método é o que permite a B2 perguntar
// "apareceu?" separado de "está ligada?".
func (c *Config) AuthenticationDeclarada() bool { return c.authenticationDeclarada }

// LoopbackHabilitado diz se o listener secundário SEM TLS/auth
// (server.loopback, plano duplo de observabilidade da Fase 11) está ligado.
// Ausente é FALSE — nenhum YAML herdado ganha um listener novo sem o
// operador pedir.
func (c *Config) LoopbackHabilitado() bool { return c.Server.Loopback.Enabled }

// Expiracao é o freio da coleta: quantas entidades uma passada pode marcar e
// por quanto tempo ela pode rodar. Os dois valem juntos, e vence o primeiro a
// ser atingido.
func (c *Config) Expiracao() (maxPorPassada int, tempoPorPassada time.Duration) {
	return c.expiracaoMax, c.expiracaoTempo
}

// Compaction é a política de compaction do §53/§78: se ela roda, e a partir de
// que fração de dado vivo um segmento deixa de valer a pena manter como está.
//
// Devolve os dois juntos porque quem consome decide UMA coisa — eleger ou não
// um segmento —, e dois métodos separados convidariam a ler o ratio de um
// processo com a compaction desligada.
func (c *Config) Compaction() (habilitada bool, minimumLiveRatio float64) {
	e := c.Storage.Compaction.Enabled
	return e == nil || *e, c.minimumLiveRatio
}

// Endereco é o host:porta em que o servidor escuta (§78).
func (c *Config) Endereco() string {
	return net.JoinHostPort(c.Server.Bind, strconv.Itoa(c.serverPort))
}

// ResultBatchSize é o tamanho do lote de linhas na resposta (§78).
//
// O lote é o de FLUSH: a cada N linhas o servidor descarrega o buffer do
// stream e entrega o chunk ao cliente. Não é buffer de resultado — o §128
// proíbe qualquer memória proporcional ao número de linhas, e o writer
// continua com teto fixo em bytes.
func (c *Config) ResultBatchSize() int { return c.resultBatchSize }

// MaxConcurrent é o teto de consultas simultâneas do §80.
//
// É por PROCESSO, e não por dataset, porque o recurso que ele protege é do
// processo: o §82 diz que todos os datasets de uma instância dividem o mesmo
// orçamento de memória, e um teto por dataset multiplicaria o pior caso pelo
// número deles.
func (c *Config) MaxConcurrent() int { return c.maxConcurrent }

// MaxLongRunning é quantas consultas LONGAS podem correr ao mesmo tempo — as
// que pediram, via `timeoutMs`, um prazo acima do `query.timeout` do dataset.
//
// É um sub-pool DENTRO do MaxConcurrent, e não um segundo teto ao lado dele:
// a consulta longa toma uma vaga de cada. Sem essa contenção, o prazo longo
// por padrão transformaria o teto do §80 numa arma contra a própria consulta
// interativa — bastariam MaxConcurrent relatórios para toda query de
// milissegundos passar a receber 503.
func (c *Config) MaxLongRunning() int { return c.maxLongRunning }

// MemoryTarget é o orçamento de memória de CONSULTA do processo (§80), e
// MaxBufferPerQuery é a fatia dele que uma consulta pode tomar.
//
// Os dois eram parseados e nunca lidos (metade do débito 15). A chave não era
// só inócua: quem escreve `memoryTarget: 1GiB` dimensiona a máquina por ela, e
// não havia teto nenhum. Hoje alimentam internal/query/budget, cujo primeiro
// consumidor real é o StreamWriter — que também nunca encolhia (débito 28), e
// cujo texto já dizia que o remédio era esta chave.
//
// MemoryTarget NÃO é um teto de RSS do processo: é o orçamento dos buffers de
// consulta, que é o que este pacote sabe contar. Chamar de teto de memória o
// que não inclui índices, mmap e heap do consumidor seria a mesma mentira que a
// chave sem leitor era, com um número a mais.
func (c *Config) MemoryTarget() int64 { return c.memoryTarget }

// MaxBufferPerQuery é o teto de buffer de UMA consulta (§80). Ver MemoryTarget.
func (c *Config) MaxBufferPerQuery() int64 { return c.maxBufferPerQuery }

// SnapshotMaxBytes é o teto de heap do journal de versões do isolamento de
// leitura. Chega ao Store por storage.Options.SnapshotMaxBytes.
func (c *Config) SnapshotMaxBytes() int64 { return c.snapshotMaxBytes }

// BindExposto diz se o servidor vai escutar fora do loopback.
//
// Existe para a subida poder AVISAR ou RECUSAR (§77): a autenticação do
// §74-77 já existe desde a Fase 11, mas o operador pode desligá-la
// explicitamente (server.authentication.enabled: false) — e nesse caso um
// bind exposto entrega POST /v1/query (leitura) E POST /v1/compact, POST
// /v1/checkpoint (mutação, ADMIN) a quem chegar na porta, sem credencial
// nenhuma. Não é lacuna do produto: é a decisão do operador tendo o alcance
// completo que ela de fato tem.
//
// Nome que não resolve para IP conta como exposto: o servidor vai bindar
// naquilo, e presumir inocência de um endereço que não se sabe qual é seria
// errar para o lado errado.
func (c *Config) BindExposto() bool {
	if c.Server.Bind == "localhost" {
		return false
	}
	ip := net.ParseIP(c.Server.Bind)
	if ip == nil {
		return true
	}
	// O unspecified (0.0.0.0, ::) é o caso mais comum e o mais perigoso: ele
	// escuta em TODAS as interfaces, inclusive as que o operador esqueceu.
	return !ip.IsLoopback()
}

// ChavesDeRuntimeIgnoradas nomeia as chaves de `runtime` que este binário
// aceita e não implementa (resto do débito 15).
//
// A lista existe porque configuração que mente é pior que configuração
// ausente: quem escreve uma chave de memória e não recebe aviso nenhum conclui
// que há um teto, e vai dimensionar a máquina por ele. É a mesma doutrina que
// ChavesDeStorageIgnoradas, logo abaixo, aplica a storage.durability (débito
// 50).
//
// Sobrou UMA. `memoryTarget` e `query.maxBufferPerQuery` SAÍRAM daqui ao
// ganharem leitor — e sair é obrigatório, não cosmético: um aviso que continua
// dizendo "esta chave não faz nada" sobre uma chave que passou a fazer mente na
// direção OPOSTA, e é a pior das duas, porque agora o operador desconfia de um
// limite que está de fato valendo.
//
// `managedMemoryTarget` fica: ela descreve o teto do runtime Go
// (GOMEMLIMIT/soft limit), que é outra coisa do orçamento de buffers de
// consulta, e não tem leitor.
func (c *Config) ChavesDeRuntimeIgnoradas() []string {
	var out []string
	if c.Runtime.ManagedMemoryTarget != "" {
		out = append(out, "runtime.managedMemoryTarget")
	}
	return out
}

// ChavesDeStorageIgnoradas nomeia storage.durability quando presente no
// arquivo: a seção é aceita e não tem implementação nesta versão (débito 50,
// §47) — só existe o SyncEveryPut interno, que o CLI nunca liga.
//
// O bloco entra como UMA chave, e não uma por sub-campo (`mode`, `interval`):
// ao contrário de runtime, onde query.maxConcurrent TEM leitor e as demais
// chaves são as exceções que precisam ser nomeadas uma a uma, aqui NADA do
// bloco tem efeito — nomear a seção inteira já diz tudo que há para dizer, e
// nomear `mode`/`interval` em separado sugeriria que um dos dois poderia, um
// dia, valer sozinho.
func (c *Config) ChavesDeStorageIgnoradas() []string {
	if c.Storage.Durability.Mode == "" && c.Storage.Durability.Interval == "" {
		return nil
	}
	return []string{"storage.durability"}
}

// IntervaloDeCheckpoint é a janela de reprocessamento em caso de crash.
func (c *Config) IntervaloDeCheckpoint() time.Duration { return c.checkpointInterval }

// SegmentSize é o tamanho de rotação de segmento.
func (c *Config) SegmentSize() int64 { return c.segmentSize }

// DefaultsDeQuery são os defaults globais que o dataset herda quando omite a
// chave equivalente.
func (c *Config) DefaultsDeQuery() schema.Defaults {
	return schema.Defaults{
		MaxRows: c.defaultMaxRows,
		Timeout: c.defaultTimeout,
		// Zero quando a chave está ausente, e zero é o sinal de "o dataset
		// herda o próprio Timeout" — nunca um teto de 30s aplicado por cima
		// de quem já declara `query.timeout` maior.
		MaxTimeout: c.defaultMaxTimeout,
	}
}

// DirDoDataset é onde os arquivos do dataset vivem.
func (c *Config) DirDoDataset(nome string) string {
	return c.Storage.Path + string(os.PathSeparator) + nome
}

// SpecDoDataset é o caminho do YAML de materialização do dataset.
func (c *Config) SpecDoDataset(nome string) string {
	return c.Datasets.Directory + string(os.PathSeparator) + nome + ".yaml"
}

// ListaDatasets enumera os datasets declarados em datasets.directory.
//
// Não existia no M1: o CLI resolvia <directory>/<nome>.yaml sob demanda, um
// arquivo por vez, porque cada comando recebia --dataset. O servidor precisa
// do CONJUNTO — e é só com o conjunto na mão que a validação de unicidade do
// groupId tem onde nascer (schema.Parse enxerga um arquivo por vez).
//
// A ordem é alfabética para que a subida seja determinística: dois processos
// com a mesma configuração têm de reportar o mesmo primeiro erro.
func (c *Config) ListaDatasets() ([]string, error) {
	entradas, err := os.ReadDir(c.Datasets.Directory)
	if err != nil {
		return nil, fmt.Errorf("datasets.directory %s: %w", c.Datasets.Directory, err)
	}
	var nomes []string
	for _, e := range entradas {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		nomes = append(nomes, strings.TrimSuffix(e.Name(), ".yaml"))
	}
	sort.Strings(nomes)
	return nomes, nil
}

// ValidaUnicidadeDeGroup recusa dois datasets que consumam o MESMO tópico com
// o MESMO groupId no mesmo cluster.
//
// É validação de CONJUNTO e por isso não cabe em schema.validate, que vê um
// arquivo por vez. O cenário que ela impede é silencioso por construção:
// copiar um YAML e trocar só o nome faz o Kafka dividir as partições entre os
// dois consumers, cada dataset materializa estado parcial, e nenhuma métrica
// do §92 acusa — o group consome tudo e o lag fica normal.
func ValidaUnicidadeDeGroup(specs map[string]*schema.DatasetSpec) error {
	type origem struct{ bootstrap, topico, group string }
	visto := make(map[origem]string, len(specs))

	nomes := make([]string, 0, len(specs))
	for n := range specs {
		nomes = append(nomes, n)
	}
	sort.Strings(nomes)

	for _, nome := range nomes {
		k := specs[nome].Source.Kafka
		if k == nil {
			continue
		}
		o := origem{
			bootstrap: checkpoint.CanonizaBootstrap(k.BootstrapServers),
			topico:    k.Topic,
			group:     k.Consumer.GroupID,
		}
		if outro, dup := visto[o]; dup {
			return fmt.Errorf(
				"datasets %q e %q consomem o tópico %q com o mesmo consumer.groupId %q no mesmo cluster: "+
					"o Kafka dividiria as partições entre os dois e cada dataset materializaria estado parcial, "+
					"sem nenhuma métrica acusar",
				outro, nome, o.topico, o.group)
		}
		visto[o] = nome
	}
	return nil
}

// CaminhoDoDataset é o diretório de dados de um dataset.
func (c *Config) CaminhoDoDataset(nome string) string {
	return filepath.Join(c.Storage.Path, nome)
}
