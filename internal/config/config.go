// Package config carrega e valida o nishidb.yaml (spec §78).
//
// Vive em pacote próprio, e não dentro do cli, porque tanto os comandos
// quanto o servidor precisam dele: com a configuração morando no cli, o
// servidor importaria o cli e o cli importaria o servidor para o dispatch —
// um ciclo. O §98 já previa internal/config/ por este motivo.
package config

import (
	"fmt"
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
// As seções que o M1 não implementa (server, TLS, auth, runtime, compaction,
// durability) são ACEITAS e ignoradas: o arquivo que o M3 vai usar não pode
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
	} `yaml:"storage"`

	Datasets struct {
		Directory string `yaml:"directory"`
	} `yaml:"datasets"`

	// Server é o bloco §78 do processo de longa duração. Continua opcional:
	// `nishidb ingest` e `nishidb query` não o usam.
	Server struct {
		Enabled bool   `yaml:"enabled"`
		Bind    string `yaml:"bind"`
		Port    *int   `yaml:"port"`
	} `yaml:"server"`

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
}

const (
	segmentSizePadrao        = 512 << 20
	checkpointIntervalPadrao = 30 * time.Second
	maxRowsPadrao            = 100000
	timeoutPadrao            = 30 * time.Second
	resultBatchPadrao        = 256
	serverPortPadrao         = 9142
	serverBindPadrao         = "0.0.0.0"
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
	if c.serverPort > 65535 {
		return nil, fmt.Errorf("configuração %s: server.port %d fora de 1..65535", path, c.serverPort)
	}
	if c.Server.Bind == "" {
		c.Server.Bind = serverBindPadrao
	}
	return &c, nil
}

// Endereco é o host:porta em que o servidor escuta (§78).
func (c *Config) Endereco() string {
	return net.JoinHostPort(c.Server.Bind, strconv.Itoa(c.serverPort))
}

// ResultBatchSize é o tamanho do lote de linhas na resposta (§78).
func (c *Config) ResultBatchSize() int { return c.resultBatchSize }

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
