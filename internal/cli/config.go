// Package cli implementa os comandos do binário nishidb. A lógica mora aqui,
// e não no main, para que cada comando seja testável como função — o teste
// fim-a-fim do CLI não deveria precisar de um binário compilado para rodar.
package cli

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

// Config é o subconjunto do nishidb.yaml (spec §78) que os comandos do M1
// precisam.
//
// As seções que o M1 não implementa (server, TLS, auth, runtime, compaction,
// durability) são ACEITAS e ignoradas: o arquivo que o M3 vai usar não pode
// ser inválido para o binário do M1, senão o operador mantém dois arquivos e
// eles divergem.
type Config struct {
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
	} `yaml:"query"`

	// Resolvidos a partir dos campos textuais acima.
	segmentSize        int64
	checkpointInterval time.Duration
	defaultMaxRows     int
	defaultTimeout     time.Duration
}

const (
	segmentSizePadrao        = 512 << 20
	checkpointIntervalPadrao = 30 * time.Second
	maxRowsPadrao            = 100000
	timeoutPadrao            = 30 * time.Second
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

	if c.Storage.Path == "" {
		return nil, fmt.Errorf("configuração %s: storage.path é obrigatório", path)
	}
	if c.Datasets.Directory == "" {
		return nil, fmt.Errorf("configuração %s: datasets.directory é obrigatório", path)
	}

	if c.segmentSize, err = tamanho(c.Storage.SegmentSize, segmentSizePadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.segmentSize: %w", path, err)
	}
	if c.checkpointInterval, err = duracao(c.Storage.Checkpoint.Interval, checkpointIntervalPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: storage.checkpoint.interval: %w", path, err)
	}
	if c.defaultMaxRows, err = inteiroPositivo(c.Query.DefaultMaxRows, maxRowsPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultMaxRows: %w", path, err)
	}
	if c.defaultTimeout, err = duracao(c.Query.DefaultTimeout, timeoutPadrao); err != nil {
		return nil, fmt.Errorf("configuração %s: query.defaultTimeout: %w", path, err)
	}
	return &c, nil
}

// DefaultsDeQuery são os defaults globais que o dataset herda quando omite a
// chave equivalente.
func (c *Config) DefaultsDeQuery() schema.Defaults {
	return schema.Defaults{MaxRows: c.defaultMaxRows, Timeout: c.defaultTimeout}
}

// inteiroPositivo aplica o padrão quando a chave está AUSENTE e recusa
// qualquer valor não positivo. Zero e negativo não são "sem valor": são teto
// que nenhuma consulta poderia satisfazer, e aceitá-los em silêncio faria a
// configuração mentir.
func inteiroPositivo(v *int, padrao int) (int, error) {
	if v == nil {
		return padrao, nil
	}
	if *v <= 0 {
		return 0, fmt.Errorf("deve ser positivo, é %d", *v)
	}
	return *v, nil
}

// tamanho aceita as unidades binárias da spec (512MiB, 1GiB) e o número puro
// em bytes. As unidades DECIMAIS (MB, GB) são recusadas em vez de
// interpretadas: aceitar as duas grafias faria "512MB" e "512MiB" parecerem a
// mesma coisa e diferirem em 7% de disco.
func tamanho(s string, padrao int64) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return padrao, nil
	}
	unidades := []struct {
		sufixo string
		mult   int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40}, {"B", 1},
	}
	for _, u := range unidades {
		if !strings.HasSuffix(s, u.sufixo) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimSuffix(s, u.sufixo)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("valor inválido %q", s)
		}
		if n <= 0 {
			return 0, fmt.Errorf("valor deve ser positivo, é %q", s)
		}
		return n * u.mult, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("valor inválido %q (use bytes ou sufixo KiB/MiB/GiB)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("valor deve ser positivo, é %q", s)
	}
	return n, nil
}

func duracao(s string, padrao time.Duration) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return padrao, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("duração inválida %q", s)
	}
	if d <= 0 {
		return 0, fmt.Errorf("duração deve ser positiva, é %q", s)
	}
	return d, nil
}

// DirDoDataset é onde os arquivos do dataset vivem.
func (c *Config) DirDoDataset(nome string) string {
	return c.Storage.Path + string(os.PathSeparator) + nome
}

// SpecDoDataset é o caminho do YAML de materialização do dataset.
func (c *Config) SpecDoDataset(nome string) string {
	return c.Datasets.Directory + string(os.PathSeparator) + nome + ".yaml"
}
