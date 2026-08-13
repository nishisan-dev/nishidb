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
	Materialization MaterializationSpec
	Schema          SchemaSpec
	Query           QuerySpec
}

// SourceSpec é aceita e validada apenas sintaticamente no M1 (Kafka é M2).
type SourceSpec struct {
	Type  string
	Kafka map[string]any
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
	Version         *int       `yaml:"version"`
	Name            string     `yaml:"name"`
	Source          *rawSource `yaml:"source"`
	Materialization rawMat     `yaml:"materialization"`
	Schema          rawSchema  `yaml:"schema"`
	Query           rawQuery   `yaml:"query"`
}

type rawSource struct {
	Type  string         `yaml:"type"`
	Kafka map[string]any `yaml:"kafka"`
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

// Load lê e valida uma spec de um arquivo YAML.
func Load(path string) (*DatasetSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lendo spec %s: %w", path, err)
	}
	s, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// Parse valida o YAML e devolve a spec com defaults aplicados.
func Parse(data []byte) (*DatasetSpec, error) {
	var raw rawSpec
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%w: yaml malformado: %v", ErrInvalidSpec, err)
	}
	return validate(&raw)
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

func validate(raw *rawSpec) (*DatasetSpec, error) {
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

	if raw.Source != nil {
		s.Source = SourceSpec{Type: raw.Source.Type, Kafka: raw.Source.Kafka}
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
	if seq := s.Materialization.Sequence; seq != nil {
		if sf, ok := fields[seq.Field]; ok && sf.Type != "int64" {
			return nil, specErrf("campo sequence %q deve ser int64, é %q", seq.Field, sf.Type)
		}
	}
	if ttl := s.Materialization.TTL; ttl != nil {
		if tf, ok := fields[ttl.Field]; ok && tf.Type != "timestamp" {
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

	// query
	maxRows := 100000
	if raw.Query.MaxRows != nil {
		maxRows = *raw.Query.MaxRows
	}
	if maxRows <= 0 {
		return nil, specErrf("query.maxRows deve ser > 0, é %d", maxRows)
	}
	s.Query.MaxRows = maxRows
	timeout := 30 * time.Second
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
