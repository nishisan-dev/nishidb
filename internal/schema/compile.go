package schema

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrUnknownField é retornado por ResolveUnknown sob policy reject
// (spec §101: SCHEMA_VALIDATION).
var ErrUnknownField = errors.New("campo desconhecido rejeitado pela policy")

type Mode uint8

const (
	ModeMerge Mode = iota
	ModeReplace
)

type UnknownPolicy uint8

const (
	UnknownIgnore UnknownPolicy = iota
	UnknownInclude
	UnknownReject
)

// IndexKind é o índice secundário declarado para o campo no YAML.
//
// IndexHash é ACEITO na configuração desde o M1, mas o índice físico só
// existe a partir da Fase 8 (M4): até lá o metadado é carregado sem efeito
// colateral e as queries sobre o campo caem no caminho de scan. Declarar
// hash hoje é, portanto, um forward-compat sem penalidade.
type IndexKind uint8

const (
	IndexNone IndexKind = iota
	IndexBitmap
	IndexHash
)

// FieldDictionary é o que o schema consome do storage (fielddict): IDs
// estáveis e persistentes por nome de campo.
type FieldDictionary interface {
	LookupOrCreate(name []byte) (types.FieldID, error)
}

// CompiledField é a forma resolvida de um campo, imutável após criação.
type CompiledField struct {
	ID                       types.FieldID
	Name                     string
	Alias                    string // "" se ausente
	Type                     types.FieldType
	IsAuto                   bool // tipo resolvido em runtime na primeira observação
	Index                    IndexKind
	IsKey, IsSequence, IsTTL bool
}

// Compiled é a spec compilada usada no hot path. FieldByName/FieldByID são
// seguros para uso concorrente (writer + queries); ResolveUnknown muta o
// cache sob lock — chamado apenas pelo writer.
type Compiled struct {
	Name       string
	Mode       Mode
	IncludeAll bool
	Unknown    UnknownPolicy
	MaxRows    int
	Timeout    time.Duration

	Key, Seq, TTL *CompiledField

	mu      sync.RWMutex
	byName  map[string]*CompiledField
	byID    map[types.FieldID]*CompiledField
	exclude map[string]struct{}
	dict    FieldDictionary
}

// Compile resolve a spec contra o field dictionary. IDs são atribuídos em
// ordem determinística (nomes ordenados) — o mesmo spec + dict vazio produz
// sempre o mesmo mapeamento.
func (s *DatasetSpec) Compile(dict FieldDictionary) (*Compiled, error) {
	c := &Compiled{
		Name:    s.Name,
		MaxRows: s.Query.MaxRows,
		Timeout: s.Query.Timeout,
		byName:  make(map[string]*CompiledField),
		byID:    make(map[types.FieldID]*CompiledField),
		exclude: make(map[string]struct{}, len(s.Schema.Exclude)),
		dict:    dict,
	}
	if s.Materialization.Mode == "replace" {
		c.Mode = ModeReplace
	}
	switch s.Schema.UnknownFields {
	case "include":
		c.Unknown = UnknownInclude
	case "reject":
		c.Unknown = UnknownReject
	}
	// Campo excluído é INEXISTENTE para o runtime: não é compilado, não
	// consome FieldID e não volta pelo ResolveUnknown. O alias entra no
	// conjunto junto com o nome, senão o evento que trouxesse o alias
	// materializaria exatamente o campo que o operador mandou descartar.
	for _, name := range s.Schema.Exclude {
		c.exclude[name] = struct{}{}
		if spec, declared := s.Schema.Fields[name]; declared && spec.Alias != "" {
			c.exclude[spec.Alias] = struct{}{}
		}
	}

	// Conjunto de campos a compilar: com "*", todos os declarados; com
	// include explícito, exatamente os listados (declarados ou auto).
	var names []string
	for _, item := range s.Schema.Include {
		if item == "*" {
			c.IncludeAll = true
		}
	}
	if c.IncludeAll {
		names = make([]string, 0, len(s.Schema.Fields))
		for name := range s.Schema.Fields {
			names = append(names, name)
		}
	} else {
		names = append(names, s.Schema.Include...)
	}
	sort.Strings(names)

	seqField := ""
	if s.Materialization.Sequence != nil {
		seqField = s.Materialization.Sequence.Field
	}
	ttlField := ""
	if s.Materialization.TTL != nil {
		ttlField = s.Materialization.TTL.Field
	}

	for _, name := range names {
		if _, excluded := c.exclude[name]; excluded {
			continue
		}
		id, err := dict.LookupOrCreate([]byte(name))
		if err != nil {
			return nil, fmt.Errorf("registrando campo %q no dicionário: %w", name, err)
		}
		cf := &CompiledField{
			ID:         id,
			Name:       name,
			IsKey:      name == s.Materialization.Key.Field,
			IsSequence: name == seqField,
			IsTTL:      name == ttlField,
		}
		if spec, declared := s.Schema.Fields[name]; declared {
			cf.Alias = spec.Alias
			if spec.Type == "auto" {
				cf.IsAuto = true
			} else {
				cf.Type, _ = FieldTypeByName(spec.Type)
			}
			switch spec.Index {
			case "bitmap":
				cf.Index = IndexBitmap
			case "hash":
				cf.Index = IndexHash
			}
		} else {
			cf.IsAuto = true
		}
		c.byName[cf.Name] = cf
		if cf.Alias != "" {
			c.byName[cf.Alias] = cf
		}
		c.byID[cf.ID] = cf
		if cf.IsKey {
			c.Key = cf
		}
		if cf.IsSequence {
			c.Seq = cf
		}
		if cf.IsTTL {
			c.TTL = cf
		}
	}

	if c.Key == nil {
		return nil, specErrf("materialization.key.field %q fora do conjunto incluído", s.Materialization.Key.Field)
	}
	if seqField != "" && c.Seq == nil {
		return nil, specErrf("materialization.sequence.field %q fora do conjunto incluído", seqField)
	}
	return c, nil
}

// FieldByName resolve nome original OU alias. Lookup com []byte não aloca
// (elisão de map[string(b)]).
func (c *Compiled) FieldByName(name []byte) (*CompiledField, bool) {
	c.mu.RLock()
	f, ok := c.byName[string(name)]
	c.mu.RUnlock()
	return f, ok
}

// FieldByID resolve pelo ID do dicionário.
func (c *Compiled) FieldByID(id types.FieldID) (*CompiledField, bool) {
	c.mu.RLock()
	f, ok := c.byID[id]
	c.mu.RUnlock()
	return f, ok
}

// Excluded reporta se o nome (ou o alias) está na lista de exclusão. Não é
// consultado no hot path do parse: campo excluído simplesmente não existe em
// byName, e o ResolveUnknown se encarrega de não o recriar.
func (c *Compiled) Excluded(name []byte) bool {
	_, ok := c.exclude[string(name)]
	return ok
}

// ResolveUnknown trata um campo do evento que FieldByName não conhece:
//
//   - excluído (por nome ou alias) → (nil, nil): descartado, inclusive sob
//     policy reject — o operador já decidiu o destino do campo;
//   - include explícito (universo fechado) → (nil, nil): ignorado;
//   - policy ignore → (nil, nil);
//   - policy reject → ErrUnknownField;
//   - policy include → registra no dicionário, cria CompiledField auto e
//     cacheia (idempotente).
//
// Chamado apenas pela goroutine do writer; leitores concorrentes de
// FieldByName/FieldByID são protegidos pelo RWMutex.
func (c *Compiled) ResolveUnknown(name []byte) (*CompiledField, error) {
	if _, excluded := c.exclude[string(name)]; excluded {
		return nil, nil
	}
	if !c.IncludeAll || c.Unknown == UnknownIgnore {
		return nil, nil
	}
	if c.Unknown == UnknownReject {
		return nil, fmt.Errorf("%w: %s", ErrUnknownField, name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.byName[string(name)]; ok {
		return f, nil
	}
	id, err := c.dict.LookupOrCreate(name)
	if err != nil {
		return nil, fmt.Errorf("registrando campo desconhecido %q: %w", name, err)
	}
	cf := &CompiledField{ID: id, Name: string(name), IsAuto: true}
	c.byName[cf.Name] = cf
	c.byID[cf.ID] = cf
	return cf, nil
}
