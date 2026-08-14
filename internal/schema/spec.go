// Package schema carrega, valida e compila as materialization definitions
// YAML dos datasets (spec §12-17). A forma compilada (Compiled) é a usada
// no hot path de ingestão e pelo query engine.
package schema

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrInvalidSpec categoriza qualquer erro de validação de spec.
var ErrInvalidSpec = errors.New("materialization spec inválida")

// DatasetSpec é a spec validada, com defaults aplicados.
type DatasetSpec struct {
	Version         int
	Name            string
	Source          SourceSpec
	Errors          ErrorsSpec
	Materialization MaterializationSpec
	Schema          SchemaSpec
	Query           QuerySpec
}

type MaterializationSpec struct {
	Key                KeySpec
	Mode               string // merge | replace
	Sequence           *SequenceSpec
	TTL                *TTLSpec
	TombstoneRetention string
}

type KeySpec struct {
	Source string // payload (único no v1)
	Field  string
	Type   string // string (único no v1)
}

type SequenceSpec struct {
	Field    string
	Type     string // int64 (único no v1)
	Strategy string // greater (único no v1)
}

type TTLSpec struct {
	Field string
	Type  string // timestamp (único no v1)
}

type SchemaSpec struct {
	Include       []string // ["*"] ou lista explícita; default = campos declarados
	Exclude       []string
	UnknownFields string // include | ignore | reject (default ignore)
	Fields        map[string]FieldSpec
}

type FieldSpec struct {
	Type  string
	Alias string
	Index string // "" | bitmap | hash
}

type QuerySpec struct {
	MaxRows int
	Timeout time.Duration
}

// Estruturas cruas do YAML (tudo opcional; defaults e validação em validate).
type rawSpec struct {
	Version *int   `yaml:"version"`
	Name    string `yaml:"name"`
	// Source e Errors ficam como nó cru porque a recusa de chave desconhecida
	// nesses dois blocos é feita à mão, sobre as chaves do documento — ver
	// bloco(), em source.go, para o motivo de yaml.KnownFields não servir.
	//
	// São VALOR e não ponteiro porque o yaml.v3 só trata yaml.Node de forma
	// especial no tipo valor: um campo *yaml.Node fica com o nó zerado, sem
	// erro nenhum. Bloco ausente é Kind == 0.
	Source          yaml.Node `yaml:"source"`
	Errors          yaml.Node `yaml:"errors"`
	Materialization rawMat    `yaml:"materialization"`
	Schema          rawSchema `yaml:"schema"`
	Query           rawQuery  `yaml:"query"`
}

type rawMat struct {
	Key                rawKey        `yaml:"key"`
	Mode               string        `yaml:"mode"`
	Sequence           *SequenceSpec `yaml:"sequence"`
	TTL                *TTLSpec      `yaml:"ttl"`
	TombstoneRetention string        `yaml:"tombstoneRetention"`
}

type rawKey struct {
	Source string `yaml:"source"`
	Field  string `yaml:"field"`
	Type   string `yaml:"type"`
}

type rawSchema struct {
	Include       []string             `yaml:"include"`
	Exclude       []string             `yaml:"exclude"`
	UnknownFields string               `yaml:"unknownFields"`
	Fields        map[string]FieldSpec `yaml:"fields"`
}

type rawQuery struct {
	MaxRows *int   `yaml:"maxRows"`
	Timeout string `yaml:"timeout"`
}

// Defaults são os valores GLOBAIS de `query` do nishidb.yaml (§78:
// defaultMaxRows e defaultTimeout), aplicados ao dataset que OMITE a chave
// equivalente. O dataset que a declara vence — o global é default, não teto.
//
// Vem por parâmetro, e não de uma constante interna, porque só assim a chave
// global tem leitor: enquanto o default morava aqui, editá-la não mudava nada
// e não avisava nada (débito 37).
type Defaults struct {
	MaxRows int
	Timeout time.Duration
}

// DefaultsPadrao são os valores da spec §78 para quem carrega uma spec sem
// nishidb.yaml nenhum por perto — testes, benchmarks e ferramentas.
var DefaultsPadrao = Defaults{MaxRows: 100000, Timeout: 30 * time.Second}

// Load lê e valida uma spec de um arquivo YAML.
func Load(path string, d Defaults) (*DatasetSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lendo spec %s: %w", path, err)
	}
	s, err := Parse(data, d)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// Parse valida o YAML e devolve a spec com os defaults aplicados.
func Parse(data []byte, d Defaults) (*DatasetSpec, error) {
	var raw rawSpec
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%w: yaml malformado: %v", ErrInvalidSpec, err)
	}
	return validate(&raw, d)
}

func specErrf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(format, args...))
}

var fieldTypeNames = map[string]types.FieldType{
	"string":      types.TypeString,
	"int64":       types.TypeInt64,
	"double":      types.TypeDouble,
	"boolean":     types.TypeBool,
	"timestamp":   types.TypeTimestamp,
	"ipv4":        types.TypeIPv4,
	"ipv6":        types.TypeIPv6,
	"ipv4_prefix": types.TypeIPv4Prefix,
	"ipv6_prefix": types.TypeIPv6Prefix,
	"bytes":       types.TypeBytes,
	"json":        types.TypeJSON,
}

// FieldTypeByName resolve o nome YAML de um tipo; "auto" não entra aqui
// (é flag, não tipo concreto).
func FieldTypeByName(name string) (types.FieldType, bool) {
	t, ok := fieldTypeNames[name]
	return t, ok
}

func isValidTypeName(name string) bool {
	if name == "auto" {
		return true
	}
	_, ok := fieldTypeNames[name]
	return ok
}

func isIdentName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'
		if !ok {
			return false
		}
	}
	return true
}

// bitmapIndexable/hashIndexable definem os tipos indexáveis por índice
// (design §2.1); auto não é indexável (tipo desconhecido até runtime).
func bitmapIndexable(t string) bool {
	return t == "string" || t == "int64" || t == "boolean"
}

func hashIndexable(t string) bool {
	return t == "string" || t == "int64" || t == "ipv4" || t == "ipv6"
}

func validate(raw *rawSpec, padroes Defaults) (*DatasetSpec, error) {
	// Default inválido é erro do CHAMADOR, e sai aqui em vez de virar um
	// padrão implícito: cair no 100000 quando o global veio zerado esconderia
	// exatamente o defeito que se está corrigindo.
	if padroes.MaxRows <= 0 {
		return nil, specErrf("query.defaultMaxRows global deve ser > 0, é %d", padroes.MaxRows)
	}
	if padroes.Timeout <= 0 {
		return nil, specErrf("query.defaultTimeout global deve ser > 0, é %s", padroes.Timeout)
	}
	s := &DatasetSpec{}

	if raw.Version == nil {
		return nil, specErrf("version obrigatória (suportada: 1)")
	}
	if *raw.Version != 1 {
		return nil, specErrf("version %d não suportada (suportada: 1)", *raw.Version)
	}
	s.Version = 1

	if !isIdentName(raw.Name) {
		return nil, specErrf("name %q inválido (esperado [a-z0-9_]+)", raw.Name)
	}
	s.Name = raw.Name

	var err error
	if s.Source, err = validaSource(&raw.Source); err != nil {
		return nil, err
	}
	if s.Errors, err = validaErrors(&raw.Errors); err != nil {
		return nil, err
	}

	// materialization.key
	key := raw.Materialization.Key
	if key.Field == "" {
		return nil, specErrf("materialization.key.field obrigatório")
	}
	if key.Source == "" {
		key.Source = "payload"
	}
	if key.Source != "payload" {
		return nil, specErrf("materialization.key.source %q não suportado (v1: payload)", key.Source)
	}
	if key.Type == "" {
		key.Type = "string"
	}
	if key.Type != "string" {
		return nil, specErrf("materialization.key.type %q não suportado (v1: string)", key.Type)
	}
	s.Materialization.Key = KeySpec(key)

	mode := raw.Materialization.Mode
	if mode == "" {
		mode = "merge"
	}
	if mode != "merge" && mode != "replace" {
		return nil, specErrf("materialization.mode %q inválido (merge|replace)", mode)
	}
	s.Materialization.Mode = mode
	s.Materialization.TombstoneRetention = raw.Materialization.TombstoneRetention

	if seq := raw.Materialization.Sequence; seq != nil {
		if seq.Field == "" {
			return nil, specErrf("materialization.sequence.field obrigatório")
		}
		if seq.Type == "" {
			seq.Type = "int64"
		}
		if seq.Type != "int64" {
			return nil, specErrf("materialization.sequence.type %q não suportado (v1: int64)", seq.Type)
		}
		if seq.Strategy == "" {
			seq.Strategy = "greater"
		}
		if seq.Strategy != "greater" {
			return nil, specErrf("materialization.sequence.strategy %q não suportada (v1: greater)", seq.Strategy)
		}
		s.Materialization.Sequence = seq
	}

	if ttl := raw.Materialization.TTL; ttl != nil {
		if ttl.Field == "" {
			return nil, specErrf("materialization.ttl.field obrigatório")
		}
		if ttl.Type == "" {
			ttl.Type = "timestamp"
		}
		if ttl.Type != "timestamp" {
			return nil, specErrf("materialization.ttl.type %q não suportado (v1: timestamp)", ttl.Type)
		}
		s.Materialization.TTL = ttl
	}

	// schema.fields
	fields := raw.Schema.Fields
	if len(fields) == 0 {
		return nil, specErrf("schema.fields vazio")
	}
	aliases := make(map[string]string, len(fields)) // alias → campo dono
	for name, f := range fields {
		if name == "" {
			return nil, specErrf("schema.fields contém nome vazio")
		}
		if !isValidTypeName(f.Type) {
			return nil, specErrf("campo %q: tipo %q inválido", name, f.Type)
		}
		if f.Alias != "" {
			if !isIdentName(f.Alias) {
				return nil, specErrf("campo %q: alias %q inválido (esperado [a-z0-9_]+)", name, f.Alias)
			}
			if owner, dup := aliases[f.Alias]; dup {
				return nil, specErrf("alias %q duplicado (campos %q e %q)", f.Alias, owner, name)
			}
			if _, clash := fields[f.Alias]; clash && f.Alias != name {
				return nil, specErrf("alias %q colide com o nome do campo %q", f.Alias, f.Alias)
			}
			aliases[f.Alias] = name
		}
		switch f.Index {
		case "":
		case "bitmap":
			if !bitmapIndexable(f.Type) {
				return nil, specErrf("campo %q: tipo %q não suporta índice bitmap", name, f.Type)
			}
		case "hash":
			if !hashIndexable(f.Type) {
				return nil, specErrf("campo %q: tipo %q não suporta índice hash", name, f.Type)
			}
		default:
			return nil, specErrf("campo %q: index %q inválido (bitmap|hash)", name, f.Index)
		}
		if f.Index != "" && name == key.Field {
			return nil, specErrf("campo %q é a key do dataset e já é indexado pela primary; remova index", name)
		}
	}
	s.Schema.Fields = fields

	// Consistência de key/sequence/ttl com as declarações.
	if kf, ok := fields[key.Field]; !ok {
		return nil, specErrf("materialization.key.field %q não declarado em schema.fields", key.Field)
	} else if kf.Type != "string" {
		return nil, specErrf("campo key %q deve ser string, é %q", key.Field, kf.Type)
	}
	// Sequence e ttl têm a MESMA exigência incondicional da key: o campo
	// referenciado precisa estar declarado em schema.fields com o tipo
	// certo. Sem isso, um include explícito contendo o nome compilaria o
	// campo como auto (TypeNull), quebrando o `greater` do materializer e
	// a expiração por timestamp.
	if seq := s.Materialization.Sequence; seq != nil {
		sf, ok := fields[seq.Field]
		if !ok {
			return nil, specErrf("materialization.sequence.field %q não declarado em schema.fields", seq.Field)
		}
		if sf.Type != "int64" {
			return nil, specErrf("campo sequence %q deve ser int64, é %q", seq.Field, sf.Type)
		}
	}
	if ttl := s.Materialization.TTL; ttl != nil {
		tf, ok := fields[ttl.Field]
		if !ok {
			return nil, specErrf("materialization.ttl.field %q não declarado em schema.fields", ttl.Field)
		}
		if tf.Type != "timestamp" {
			return nil, specErrf("campo ttl %q deve ser timestamp, é %q", ttl.Field, tf.Type)
		}
	}

	// include / exclude / unknownFields
	include := raw.Schema.Include
	includeAll := false
	for _, item := range include {
		if item == "*" {
			includeAll = true
		}
	}
	if includeAll && len(include) > 1 {
		return nil, specErrf(`"*" no include deve aparecer sozinho`)
	}
	if len(include) == 0 {
		// Default: só os campos declarados.
		include = make([]string, 0, len(fields))
		for name := range fields {
			include = append(include, name)
		}
	}
	s.Schema.Include = include
	if len(raw.Schema.Exclude) > 0 && !includeAll {
		return nil, specErrf(`exclude só é válido com include: ["*"]`)
	}
	// Excluir key/sequence/ttl compila sem reclamar e condena TODO evento a
	// falhar em runtime (INVALID_KEY, INVALID_SEQUENCE, expiração muda): é
	// erro de configuração, não de dado. E exclude lista NOMES de campo —
	// uma entrada que casa com um alias declarado seria um no-op silencioso.
	for _, name := range raw.Schema.Exclude {
		switch {
		case name == key.Field:
			return nil, specErrf("campo key %q não pode estar no exclude", name)
		case s.Materialization.Sequence != nil && name == s.Materialization.Sequence.Field:
			return nil, specErrf("campo sequence %q não pode estar no exclude", name)
		case s.Materialization.TTL != nil && name == s.Materialization.TTL.Field:
			return nil, specErrf("campo ttl %q não pode estar no exclude", name)
		}
		if owner, isAlias := aliases[name]; isAlias {
			return nil, specErrf("exclude %q é alias do campo %q; exclua pelo nome", name, owner)
		}
	}
	s.Schema.Exclude = raw.Schema.Exclude

	unknown := raw.Schema.UnknownFields
	if unknown == "" {
		unknown = "ignore"
	}
	switch unknown {
	case "include", "ignore", "reject":
	default:
		return nil, specErrf("unknownFields %q inválido (include|ignore|reject)", unknown)
	}
	if unknown == "include" && !includeAll {
		return nil, specErrf(`unknownFields: include exige include: ["*"] (com lista explícita o universo de campos é fechado)`)
	}
	s.Schema.UnknownFields = unknown

	// query: o dataset que declara a chave vence; quem omite herda o global.
	maxRows := padroes.MaxRows
	if raw.Query.MaxRows != nil {
		maxRows = *raw.Query.MaxRows
	}
	if maxRows <= 0 {
		return nil, specErrf("query.maxRows deve ser > 0, é %d", maxRows)
	}
	s.Query.MaxRows = maxRows
	timeout := padroes.Timeout
	if raw.Query.Timeout != "" {
		d, err := time.ParseDuration(raw.Query.Timeout)
		if err != nil || d <= 0 {
			return nil, specErrf("query.timeout %q inválido", raw.Query.Timeout)
		}
		timeout = d
	}
	s.Query.Timeout = timeout

	return s, nil
}
