package schema

import (
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
)

// A emissão periódica de um contador é declarada no bloco `emit:` do próprio
// contador:
//
//	counters:
//	  - name: autenticados_por_olt
//	    groupBy: [olt]
//	    emit:
//	      every: 30s                     # intervalo entre passadas
//	      file: autenticados.ndjson      # opcional; default <name>.ndjson
//	      maxBytes: 64MiB                # opcional; teto por arquivo
//	      keep: 5                        # opcional; quantos arquivos guardar
//
// A cada `every`, UMA LINHA por grupo vivo do contador é escrita em
// <dataset>/series/<file>, inclusive a do grupo com total zero — que é
// justamente a que interessa num gráfico por equipamento.
//
// O `file` é só o NOME do arquivo, nunca um caminho: o diretório é fixo
// (EmitDirName, ao lado de data/ e spill/), pela mesma razão que o spill tem o
// seu — o arquivo fica sob o flock exclusivo do dataset, herda a política de
// filesystem dele, e o operador não dimensiona um segundo lugar. Aceitar
// caminho livre permitiria escrever dentro de data/ ou checkpoint/, e a lista
// de diretórios reservados vive no storage, que este pacote não importa.
//
// Não existe grafia para "nunca rotacionar": `maxBytes` e `keep` têm default
// justamente para que o teto de disco seja sempre conhecido (maxBytes × keep).

const (
	// EmitDirName é o diretório da emissão dentro do dataset.
	EmitDirName = "series"
	// DefaultEmitFileExt é a extensão do arquivo default, derivado do nome do
	// contador quando `file` não vem.
	DefaultEmitFileExt = ".ndjson"
	// DefaultEmitMaxBytes é o teto por arquivo quando o YAML não declara.
	DefaultEmitMaxBytes = 64 << 20
	// DefaultEmitKeep é quantos arquivos rotacionados guardar por default. Com
	// os dois defaults, o teto de disco de um contador é 320 MiB.
	DefaultEmitKeep = 5
	// MinEmitEvery é o menor intervalo declarável. Milhares de grupos a cada
	// poucos segundos é escrita contínua para uma série que nenhuma ferramenta
	// de gráfico amostra mais fino que isso.
	MinEmitEvery = 10 * time.Second
	// MinEmitMaxBytes é o menor teto por arquivo: abaixo de um mebibyte a
	// rotação acontece mais vezes que a passada e o histórico vira picado.
	MinEmitMaxBytes = 1 << 20
	// MaxEmitKeep é o maior número de arquivos guardados. O teto existe para
	// que `keep` não seja a via de encher o disco por engano.
	MaxEmitKeep = 100
)

// EmitSpec é a emissão periódica validada de um contador.
type EmitSpec struct {
	// Targets selects file and/or globally named Kafka destinations.
	// Empty is the legacy file-only default for Go callers.
	Targets []string
	// Every é o intervalo entre passadas; sempre >= MinEmitEvery.
	Every time.Duration
	// File é o nome do arquivo dentro de <dataset>/series, nunca um caminho.
	File string
	// MaxBytes é o teto por arquivo antes de rotacionar.
	MaxBytes int64
	// Keep é quantos arquivos rotacionados guardar, além do corrente.
	Keep int
}

// rawEmit é o bloco como lido do YAML, antes da validação.
type rawEmit struct {
	Targets     []string
	FileOptions bool
	Every       string
	File        string
	MaxBytes    string
	Keep        *int
}

// emitFromNode decodifica o bloco `emit` de um contador.
func emitFromNode(n *yaml.Node, keyPath string) (*rawEmit, error) {
	m, err := block(n, keyPath, "every", "file", "maxBytes", "keep", "target")
	if err != nil {
		return nil, err
	}
	var r rawEmit
	r.FileOptions = m["file"] != nil || m["maxBytes"] != nil || m["keep"] != nil
	r.Targets = []string{"file"}
	for key, value := range m {
		var err error
		switch key {
		case "target":
			for value.Kind == yaml.AliasNode {
				value = value.Alias
			}
			if value.Kind == yaml.ScalarNode && value.Tag == "!!str" {
				r.Targets = []string{value.Value}
			} else if value.Kind == yaml.SequenceNode {
				r.Targets = nil
				for _, item := range value.Content {
					for item.Kind == yaml.AliasNode {
						item = item.Alias
					}
					if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
						return nil, specErrf("%s.target must contain destination names", keyPath)
					}
					r.Targets = append(r.Targets, item.Value)
				}
			} else {
				return nil, specErrf("%s.target must be a name or a non-empty list", keyPath)
			}
			if len(r.Targets) == 0 {
				return nil, specErrf("%s.target must not be empty", keyPath)
			}
		case "every":
			err = decodeInto(value, keyPath+".every", &r.Every)
		case "file":
			err = decodeInto(value, keyPath+".file", &r.File)
		case "maxBytes":
			err = decodeInto(value, keyPath+".maxBytes", &r.MaxBytes)
		case "keep":
			var k int
			err = decodeInto(value, keyPath+".keep", &k)
			r.Keep = &k
		}
		if err != nil {
			return nil, err
		}
	}
	return &r, nil
}

// validateEmit confere o bloco e aplica os defaults. counterName é o nome do
// contador, usado como nome de arquivo quando `file` não vem. seenFiles casa
// nome de arquivo → contador que o declarou: dois contadores escrevendo no
// mesmo arquivo intercalariam séries de contadores diferentes no mesmo fluxo, e
// cada um rotacionaria por baixo do outro.
func validateEmit(r *rawEmit, keyPath, counterName string, seenFiles map[string]string) (*EmitSpec, error) {
	e := &EmitSpec{Targets: r.Targets}
	seen := make(map[string]bool)
	for _, target := range e.Targets {
		if !ValidDeliveryName(target) || seen[target] {
			return nil, specErrf("%s.target: invalid or duplicate destination %q", keyPath, target)
		}
		seen[target] = true
	}
	if e.HasFile() {
		e.File, e.MaxBytes, e.Keep = counterName+DefaultEmitFileExt, DefaultEmitMaxBytes, DefaultEmitKeep
	} else if r.FileOptions {
		return nil, specErrf("%s: file, maxBytes and keep require target file", keyPath)
	}
	if r.Every == "" {
		return nil, specErrf("%s.every obrigatório — é o intervalo entre passadas", keyPath)
	}
	if isZeroDuration(r.Every) {
		return nil, specErrf("%s.every %q não declara intervalo — o mínimo é %s", keyPath, r.Every, MinEmitEvery)
	}
	d, err := units.Duration(r.Every, 0)
	if err != nil {
		return nil, specErrf("%s.every: %s", keyPath, withoutSpecPrefix(err))
	}
	if d < MinEmitEvery {
		return nil, specErrf("%s.every %s é menor que o mínimo de %s", keyPath, r.Every, MinEmitEvery)
	}
	e.Every = d
	if !e.HasFile() {
		return e, nil
	}
	if r.File != "" {
		if err := validateEmitFile(keyPath, r.File); err != nil {
			return nil, err
		}
		e.File = r.File
	}
	if other, dup := seenFiles[e.File]; dup {
		return nil, specErrf("%s.file %q já é escrito pelo contador %q — "+
			"dois contadores no mesmo arquivo intercalariam as séries e rotacionariam um por baixo do outro",
			keyPath, e.File, other)
	}
	seenFiles[e.File] = counterName
	if r.MaxBytes != "" {
		b, err := units.Size(r.MaxBytes, 0)
		if err != nil {
			return nil, specErrf("%s.maxBytes: %s", keyPath, withoutSpecPrefix(err))
		}
		if b < MinEmitMaxBytes {
			return nil, specErrf("%s.maxBytes %s é menor que o mínimo de %d bytes", keyPath, r.MaxBytes, MinEmitMaxBytes)
		}
		e.MaxBytes = b
	}
	if r.Keep != nil {
		if *r.Keep < 1 || *r.Keep > MaxEmitKeep {
			return nil, specErrf("%s.keep %d fora de [1, %d]", keyPath, *r.Keep, MaxEmitKeep)
		}
		e.Keep = *r.Keep
	}
	return e, nil
}

// ValidDeliveryName is shared by named series and alarm destinations.
func ValidDeliveryName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, c := range name {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

// HasFile reports whether local NDJSON output is selected.
func (e *EmitSpec) HasFile() bool {
	if len(e.Targets) == 0 {
		return true
	}
	for _, t := range e.Targets {
		if t == "file" {
			return true
		}
	}
	return false
}

// HasKafka reports whether any named output is selected.
func (e *EmitSpec) HasKafka() bool {
	for _, t := range e.Targets {
		if t != "file" {
			return true
		}
	}
	return false
}

// validateEmitFile recusa qualquer coisa que não seja um nome de arquivo
// simples. A recusa é por alfabeto, e não por limpeza de caminho: normalizar
// `../x` em silêncio deixaria o operador achando que declarou um lugar e
// escrevendo em outro.
func validateEmitFile(keyPath, file string) error {
	if strings.ContainsRune(file, '/') {
		return specErrf("%s.file %q é um caminho; declare só o nome do arquivo — o diretório é %s/ dentro do dataset",
			keyPath, file, EmitDirName)
	}
	if file == "." || file == ".." || strings.HasPrefix(file, ".") {
		return specErrf("%s.file %q inválido: o nome não pode começar com ponto", keyPath, file)
	}
	for _, c := range file {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '_' || c == '-' || c == '.'
		if !ok {
			return specErrf("%s.file %q inválido (esperado [A-Za-z0-9._-]+)", keyPath, file)
		}
	}
	return nil
}
