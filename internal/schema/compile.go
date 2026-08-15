package schema

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
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

// String devolve o índice como o operador o escreveu no YAML.
//
// O vocabulário do YAML é também o do wire (o `index` de /v1/metadata): são a
// mesma informação — o que a configuração declara —, e traduzi-la para um
// segundo conjunto de palavras só criaria a chance de os dois divergirem.
func (k IndexKind) String() string {
	switch k {
	case IndexNone:
		return "none"
	case IndexBitmap:
		return "bitmap"
	case IndexHash:
		return "hash"
	default:
		return "desconhecido"
	}
}

// FieldDictionary é o que o schema consome do storage (fielddict): IDs
// estáveis e persistentes por nome de campo, e o tipo FIXADO dos campos sem
// declaração (R11).
//
// Os dois métodos de tipo entram na interface, e não numa capability
// opcional: com type assertion, um dicionário que não a implementasse fixaria
// o tipo só em memória e o perderia no restart EM SILÊNCIO — o campo passaria
// a não ser comparável em query sem nada indicar o porquê.
// Len e Name existem para o Compile ENUMERAR o dicionário e carregar os campos
// que uma execução anterior materializou (ver o carregamento no fim do
// Compile). São de leitura pura: nenhuma delas cria entry.
type FieldDictionary interface {
	LookupOrCreate(name []byte) (types.FieldID, error)
	TypeHint(id types.FieldID) (types.FieldType, bool)
	SetTypeHint(id types.FieldID, t types.FieldType) error
	Len() int
	Name(id types.FieldID) (string, bool)
}

// CompiledField é a forma resolvida de um campo. Imutável após a criação,
// exceto pelo tipo fixado de campo auto (autoType), que é publicado uma única
// vez na primeira observação.
//
// SEMPRE use por ponteiro: o atômico o torna não-copiável (go vet -copylocks).
type CompiledField struct {
	ID                       types.FieldID
	Name                     string
	Alias                    string          // "" se ausente
	Type                     types.FieldType // tipo DECLARADO; TypeNull quando IsAuto
	IsAuto                   bool            // tipo resolvido na primeira observação
	Index                    IndexKind
	IsKey, IsSequence, IsTTL bool

	// autoType é o tipo FIXADO de um campo auto (R11); TypeNull = ainda não
	// observado. Atômico porque o writer o fixa enquanto goroutines de query
	// leem EffectiveType sem lock — um campo simples aqui seria data race que
	// o -race pega no CI e nem sempre em dev.
	autoType atomic.Uint32
}

// EffectiveType devolve o tipo contra o qual os valores deste campo devem ser
// interpretados: o declarado no YAML ou, para campo auto, o FIXADO na
// primeira observação.
//
// types.TypeNull significa "campo auto que este dataset nunca viu" — o único
// estado em que não há tipo para comparar, e o binder SQL recusa a comparação
// em vez de inventar um.
func (f *CompiledField) EffectiveType() types.FieldType {
	if !f.IsAuto {
		return f.Type
	}
	return types.FieldType(f.autoType.Load())
}

// FixAutoType fixa o tipo de um campo auto na primeira observação: persiste
// no dicionário (durável antes de retornar) e só então publica em memória.
// Idempotente para o mesmo tipo; tipo divergente devolve o erro do dicionário
// (fielddict.ErrTypeHintConflict), que o materializer classifica como
// TYPE_CONVERSION — evento ruim, não falha de ingestão.
//
// Exclusivo da goroutine do writer.
func (c *Compiled) FixAutoType(f *CompiledField, t types.FieldType) error {
	if f == nil {
		return specErrf("FixAutoType com campo nulo")
	}
	if !f.IsAuto {
		return specErrf("campo %q tem tipo declarado (%s): só campo auto tem tipo a fixar", f.Name, f.Type)
	}
	if t == types.TypeNull {
		return specErrf("campo %q: tipo nulo não pode ser fixado", f.Name)
	}
	if err := c.dict.SetTypeHint(f.ID, t); err != nil {
		return err
	}
	f.autoType.Store(uint32(t))
	return nil
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
	// Errors é a política de erro de evento do §101, resolvida. Vive na forma
	// compilada porque o caminho de ingestão — que é onde ela vale — só
	// carrega o Compiled, e ir buscar a DatasetSpec de novo ali obrigaria a
	// carregar o YAML duas vezes.
	Errors ErrorsSpec

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
		Errors:  s.Errors,
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
		// Carrega o tipo já fixado em execuções anteriores: é isto que faz o
		// tipo de um campo auto sobreviver a restart. Sem esta linha o
		// processo reiniciado age como se o campo nunca tivesse sido visto.
		if cf.IsAuto {
			if t, ok := dict.TypeHint(cf.ID); ok {
				cf.autoType.Store(uint32(t))
			}
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

	// Campos que o DICIONÁRIO conhece e o YAML não: campos auto que uma
	// execução anterior materializou via ResolveUnknown. Sem carregá-los aqui,
	// depois de um restart o campo tem dados e typeHint no fields.dict mas não
	// existe no Compiled — FieldByName devolve false, a query sobre ele erra
	// "coluna não existe" (mentira) e o SELECT * omite a coluna.
	//
	// A rota é READ-ONLY por construção: a única outra forma de materializar o
	// campo seria ResolveUnknown, que é do writer e CRIA entry durável no
	// dicionário. O binder jamais pode criar campo — um erro de digitação numa
	// query gravaria um fieldID para sempre.
	//
	// O carregamento vale só para o dataset de universo ABERTO, que é
	// exatamente a condição sob a qual ResolveUnknown poderia ter criado o
	// campo. Num universo fechado o que sobra no dicionário é resíduo de uma
	// configuração anterior, e o operador que tirou o campo da lista o quer
	// fora.
	if c.IncludeAll && c.Unknown == UnknownInclude {
		for i := 1; i <= dict.Len(); i++ {
			id := types.FieldID(i)
			name, ok := dict.Name(id)
			if !ok || name == "" {
				continue
			}
			// Nome já tomado (inclusive por ALIAS de campo declarado): uma
			// segunda entrada sob o mesmo nome faria o resultado da query
			// depender da ordem de inserção no mapa.
			if _, tomado := c.byName[name]; tomado {
				continue
			}
			if _, tomado := c.byID[id]; tomado {
				continue
			}
			if _, excluído := c.exclude[name]; excluído {
				continue
			}
			cf := &CompiledField{ID: id, Name: name, IsAuto: true}
			// Sem typeHint o campo entra SEM tipo: existe, é projetável, e o
			// binder recusa comparação sobre ele com "sem tipo conhecido" — não
			// com "coluna não existe".
			if t, ok := dict.TypeHint(id); ok {
				cf.autoType.Store(uint32(t))
			}
			c.byName[cf.Name] = cf
			c.byID[cf.ID] = cf
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

// FieldsByID devolve os campos conhecidos em ordem CRESCENTE de FieldID.
//
// É um SNAPSHOT: campo auto registrado por ResolveUnknown depois desta
// chamada não aparece. Isso é comportamento, não bug — num dataset com
// wildcard e unknownFields include, duas execuções de `SELECT *` em momentos
// diferentes podem devolver conjuntos de colunas diferentes, porque o
// universo de campos cresce com os eventos.
//
// Caminho frio (bind de query): aloca. Os ponteiros são seguros de reter —
// CompiledField é imutável após a criação, exceto pelo tipo fixado de campo
// auto, que é publicado atomicamente.
func (c *Compiled) FieldsByID() []*CompiledField {
	c.mu.RLock()
	// byID tem UMA entrada por campo (byName tem duas para campo com alias):
	// não há duplicata a remover aqui.
	out := make([]*CompiledField, 0, len(c.byID))
	for _, f := range c.byID {
		out = append(out, f)
	}
	c.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
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
	// O campo pode já existir no dicionário, com tipo fixado, de uma execução
	// anterior — mesma razão do carregamento no Compile.
	if t, ok := c.dict.TypeHint(id); ok {
		cf.autoType.Store(uint32(t))
	}
	c.byName[cf.Name] = cf
	c.byID[cf.ID] = cf
	return cf, nil
}
