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
// As seções que o M1 não implementa (server, TLS, auth, runtime, durability)
// são ACEITAS e ignoradas: o arquivo que o M3 vai usar não pode
// ser inválido para o binário do M1, senão o operador mantém dois arquivos e
// eles divergem.
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
	} `yaml:"server"`

	// Runtime é o bloco §79/§80. Só query.maxConcurrent tem leitor: é o teto
	// de consultas simultâneas que o §80 nomeia e que o §81 autoriza a fazer
	// valer REJEITANDO consulta antes de comprometer a ingestão.
	//
	// As demais chaves continuam aceitas e sem efeito (débito 15), mas não em
	// silêncio: quem escreve `memoryTarget: 1GiB` conclui que há um teto de
	// memória, e não há. A subida avisa, como já faz com storage.durability.
	Runtime struct {
		MemoryTarget        string `yaml:"memoryTarget"`
		ManagedMemoryTarget string `yaml:"managedMemoryTarget"`
		Query               struct {
			MaxConcurrent     *int   `yaml:"maxConcurrent"`
			MaxBufferPerQuery string `yaml:"maxBufferPerQuery"`
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
		DefaultMaxRows  *int   `yaml:"defaultMaxRows"`
		DefaultTimeout  string `yaml:"defaultTimeout"`
		ResultBatchSize *int   `yaml:"resultBatchSize"`
	} `yaml:"query"`

	// Resolvidos a partir dos campos textuais acima.
	segmentSize        int64
	checkpointInterval time.Duration
	defaultMaxRows     int
	defaultTimeout     time.Duration
	resultBatchSize    int
	serverPort         int
	maxConcurrent      int
	minimumLiveRatio   float64
}

const (
	segmentSizePadrao        = 512 << 20
	checkpointIntervalPadrao = 30 * time.Second
	maxRowsPadrao            = 100000
	timeoutPadrao            = 30 * time.Second
	resultBatchPadrao        = 256
	serverPortPadrao         = 9142
	maxConcurrentPadrao      = 16

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
	var c Config
	// KnownFields fica DESLIGADO de propósito: o arquivo do M3 traz seções
	// que este binário não conhece, e recusá-las obrigaria o operador a mantê
	// -las em dois arquivos.
	if err := yaml.Unmarshal(b, &c); err != nil {
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

	if c.segmentSize, err = units.Tamanho(c.Storage.SegmentSize, segmentSizePadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.segmentSize: %w", path, err)
	}
	if c.checkpointInterval, err = units.Duracao(c.Storage.Checkpoint.Interval, checkpointIntervalPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.checkpoint.interval: %w", path, err)
	}
	if c.minimumLiveRatio, err = razaoDeLiveness(c.Storage.Compaction.MinimumLiveRatio); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.compaction.minimumLiveRatio: %w", path, err)
	}
	if c.defaultMaxRows, err = units.InteiroPositivo(c.Query.DefaultMaxRows, maxRowsPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultMaxRows: %w", path, err)
	}
	if c.defaultTimeout, err = units.Duracao(c.Query.DefaultTimeout, timeoutPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultTimeout: %w", path, err)
	}
	if c.resultBatchSize, err = units.InteiroPositivo(c.Query.ResultBatchSize, resultBatchPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.resultBatchSize: %w", path, err)
	}
	if c.serverPort, err = units.InteiroPositivo(c.Server.Port, serverPortPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: server.port: %w", path, err)
	}
	if c.maxConcurrent, err = units.InteiroPositivo(c.Runtime.Query.MaxConcurrent, maxConcurrentPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: runtime.query.maxConcurrent: %w", path, err)
	}
	if c.serverPort > 65535 {
		return nil, fmt.Errorf("configuração %s: server.port %d fora de 1..65535", path, c.serverPort)
	}
	if c.Server.Bind == "" {
		c.Server.Bind = serverBindPadrao
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

// BindExposto diz se o servidor vai escutar fora do loopback.
//
// Existe para a subida poder AVISAR: enquanto a autenticação do §74-77 não
// existir (Fase 11), escutar em endereço alcançável significa entregar
// /v1/query — e com ele os dados — a quem chegar na porta.
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
// aceita e não implementa (débito 15).
//
// A lista existe porque configuração que mente é pior que configuração
// ausente: quem escreve `memoryTarget: 1GiB` e não recebe aviso nenhum conclui
// que há um teto de memória, e vai dimensionar a máquina por ele. É a mesma
// doutrina aplicada a storage.durability no M2.
func (c *Config) ChavesDeRuntimeIgnoradas() []string {
	var out []string
	if c.Runtime.MemoryTarget != "" {
		out = append(out, "runtime.memoryTarget")
	}
	if c.Runtime.ManagedMemoryTarget != "" {
		out = append(out, "runtime.managedMemoryTarget")
	}
	if c.Runtime.Query.MaxBufferPerQuery != "" {
		out = append(out, "runtime.query.maxBufferPerQuery")
	}
	return out
}

// IntervaloDeCheckpoint é a janela de reprocessamento em caso de crash.
func (c *Config) IntervaloDeCheckpoint() time.Duration { return c.checkpointInterval }

// SegmentSize é o tamanho de rotação de segmento.
func (c *Config) SegmentSize() int64 { return c.segmentSize }

// DefaultsDeQuery são os defaults globais que o dataset herda quando omite a
// chave equivalente.
func (c *Config) DefaultsDeQuery() schema.Defaults {
	return schema.Defaults{MaxRows: c.defaultMaxRows, Timeout: c.defaultTimeout}
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
