package schema

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// ErrUnknownField é retornado por ResolveUnknown sob policy reject
// (spec §101: SCHEMA_VALIDATION).
var ErrUnknownField = errors.New("campo desconhecido rejeitado pela policy")

// ErrTetoDeCampos é devolvido quando o dataset alcançou schema.maxCols.
//
// Tem sentinela PRÓPRIA de propósito. Sem ela o erro chegava à classificação
// sem dono, caía no balde de "bug nosso ou dataset avariado" e PARAVA o writer
// para sempre — o offset não avança, então o restart reproduz o mesmo evento e
// o mesmo estouro. Sobre uma grandeza que cresce sem teto, alcançar o limite
// não é anomalia: é questão de tempo, e precisa de desfecho declarado.
var ErrTetoDeCampos = errors.New("teto de campos do dataset alcançado")

// ErrColunaDerivada é devolvido quando uma coluna extraída não pode ser criada
// com segurança contra o dicionário existente.
var ErrColunaDerivada = errors.New("coluna derivada inválida")

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

	// IsSystem marca campo que o WRITER injeta e o operador não declara.
	// Ele existe no schema como qualquer outro — tem FieldID, é gravado no
	// payload e SELECT pelo nome funciona —, mas fica fora do `SELECT *` e
	// das colunas do /v1/metadata: quem pede "tudo" quer os campos do
	// próprio dado, não a contabilidade interna do banco.
	IsSystem bool

	// Parent é o campo json de onde esta coluna foi extraída; nil para coluna
	// de topo. Path é o caminho dentro daquele documento.
	//
	// A derivada é uma coluna NORMAL em tudo o que importa ao binder e ao
	// planner — tem FieldID, tipo e índice, e por isso é comparável e
	// indexável, que é o ponto inteiro da feature. O par Parent/Path governa
	// só duas coisas: quem a materializa, e o fato de ela ficar FORA do
	// SELECT *, pela mesma razão do IsSystem acima — quem pede "tudo" não
	// espera o dataset multiplicar de largura.
	Parent *CompiledField
	Path   string
	// DescobreAninhados liga a promoção automática das chaves de dentro deste
	// documento a colunas. Só faz sentido em campo json.
	DescobreAninhados bool

	// Derivadas são as colunas extraídas DESTE campo, em ordem de FieldID.
	// Pré-computada no Compile porque o materializer a consulta por campo por
	// evento: procurar as filhas varrendo o mapa no hot path custaria o preço
	// de um SELECT * por evento.
	Derivadas []*CompiledField

	// Format é o padrão declarado no YAML e Layout é ele compilado. Não-nil
	// exatamente quando Type é datetime ou time; o Format é guardado ao lado
	// do Layout porque a mensagem de literal inválido precisa dizer o que se
	// esperava — "literal inválido para a coluna x" sozinho é inacionável.
	Format string
	Layout *typeparse.Layout

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
	// MaxCols/OnMaxCols governam o crescimento do dicionário. Vivem aqui, e
	// não só na spec, porque quem os consulta é o ResolveUnknown — hot path do
	// writer, que só carrega o Compiled.
	MaxCols       int
	OnMaxCols     string
	MaxNestedCols int
	// Errors é a política de erro de evento do §101, resolvida. Vive na forma
	// compilada porque o caminho de ingestão — que é onde ela vale — só
	// carrega o Compiled, e ir buscar a DatasetSpec de novo ali obrigaria a
	// carregar o YAML duas vezes.
	Errors ErrorsSpec

	Key, Seq, TTL *CompiledField

	// ExpireAfterWrite é o idle timeout do dataset; zero = sem expiração por
	// escrita. Mutuamente exclusivo com TTL, pela validação da spec.
	ExpireAfterWrite time.Duration

	// Escrita é o campo de sistema que registra o instante da escrita.
	// Não-nil exatamente quando ExpireAfterWrite > 0 — sem expiração não há
	// para que pagar 11 bytes por record.
	Escrita *CompiledField

	mu      sync.RWMutex
	byName  map[string]*CompiledField
	byID    map[types.FieldID]*CompiledField
	exclude map[string]struct{}
	dict    FieldDictionary

	// camposDescartados conta os campos recusados pelo teto. Atômico porque o
	// writer o incrementa enquanto o coletor de métricas o lê. É o que torna a
	// perda CONTÁVEL em vez de descoberta por acaso — o modo de falha
	// inaceitável aqui é o silêncio, não a perda.
	camposDescartados atomic.Uint64
}

// CamposDescartados é quantos campos o teto recusou desde a abertura.
func (c *Compiled) CamposDescartados() uint64 { return c.camposDescartados.Load() }

// CamposConhecidos é o tamanho corrente do dicionário — o numerador de que
// MaxCols é o denominador.
func (c *Compiled) CamposConhecidos() int { return c.dict.Len() }

// Compile resolve a spec contra o field dictionary. IDs são atribuídos em
// ordem determinística (nomes ordenados) — o mesmo spec + dict vazio produz
// sempre o mesmo mapeamento.
func (s *DatasetSpec) Compile(dict FieldDictionary) (*Compiled, error) {
	c := &Compiled{
		Name:          s.Name,
		MaxRows:       s.Query.MaxRows,
		Timeout:       s.Query.Timeout,
		MaxCols:       s.Schema.MaxCols,
		OnMaxCols:     s.Schema.OnMaxCols,
		MaxNestedCols: s.Schema.MaxNestedCols,
		Errors:        s.Errors,
		byName:        make(map[string]*CompiledField),
		byID:          make(map[types.FieldID]*CompiledField),
		exclude:       make(map[string]struct{}, len(s.Schema.Exclude)),
		dict:          dict,
	}
	if ttl := s.Materialization.TTL; ttl != nil {
		c.ExpireAfterWrite = ttl.ExpireAfterWrite
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
	// O campo de sistema entra no conjunto a compilar como qualquer outro:
	// assim ele ganha FieldID pelo mesmo caminho determinístico e o dicionário
	// o registra de forma durável, que é o que faz o replay do tail conseguir
	// reconstruir o índice de expiração a partir dos próprios records.
	if c.ExpireAfterWrite > 0 {
		names = append(names, CampoInstanteDeEscrita)
	}
	sort.Strings(names)

	// Declarar mais campos do que o próprio teto é erro de configuração, e sai
	// aqui: deixar passar faria o dataset nascer acima do limite que ele mesmo
	// declarou, e o primeiro autodescoberto seria recusado por uma decisão que
	// ninguém tomou conscientemente.
	//
	// A conta é sobre os DECLARADOS, e não sobre dict.Len(): um dataset que já
	// cresceu além do teto numa execução anterior continua abrindo — os campos
	// existem e são legíveis, e o que o teto governa é o crescimento daqui
	// para a frente.
	if c.MaxCols > 0 {
		declarados := 0
		for _, name := range names {
			if _, excluded := c.exclude[name]; !excluded {
				declarados++
			}
		}
		if declarados > c.MaxCols {
			return nil, fmt.Errorf("%w: o schema declara %d campos, acima de schema.maxCols (%d)",
				ErrTetoDeCampos, declarados, c.MaxCols)
		}
	}

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
		if name == CampoInstanteDeEscrita {
			// Tipo fixo, não declarável: é o writer que o preenche, com o
			// relógio do processo, e não há format nem inferência envolvidos.
			cf.Type, cf.IsSystem = types.TypeTimestamp, true
			c.Escrita = cf
			c.byName[cf.Name] = cf
			c.byID[cf.ID] = cf
			continue
		}
		if spec, declared := s.Schema.Fields[name]; declared {
			cf.Alias = spec.Alias
			cf.DescobreAninhados = spec.UnknownFields == "include"
			if spec.Type == "auto" {
				cf.IsAuto = true
			} else {
				cf.Type, _ = FieldTypeByName(spec.Type)
				l, err := CompilaFormato(spec.Type, spec.Format)
				if err != nil {
					return nil, fmt.Errorf("campo %q: %w", name, err)
				}
				cf.Format, cf.Layout = spec.Format, l
			}
			cf.Index = indexKindDe(spec.Index)
		} else {
			cf.IsAuto = true
		}
		// Carrega o tipo já fixado em execuções anteriores: é isto que faz o
		// tipo de um campo auto sobreviver a restart. Sem esta linha o
		// processo reiniciado age como se o campo nunca tivesse sido visto.
		if cf.IsAuto {
			if t, ok := dict.TypeHint(cf.ID); ok {
				// datetime e time só existem declarados, porque só a
				// declaração traz o format: sem ele não há Layout, e um campo
				// auto com tipo temporal derefereciaria nil no materializer e
				// no binder. InferType nunca os produz, então um hint assim é
				// dicionário escrito por outra configuração — recusa alta na
				// subida, com a causa nomeada, em vez de panic na primeira
				// query.
				if t == types.TypeDateTime || t == types.TypeTime {
					return nil, fmt.Errorf("campo %q: dicionário traz tipo %v para campo auto, "+
						"que não tem format declarado; declare o campo com type e format em schema.fields", name, t)
				}
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
	// As colunas DERIVADAS são registradas aqui: depois das de topo, para que
	// os FieldIDs de topo não mudem quando um extract é acrescentado, e ANTES
	// do carregamento do dicionário abaixo — que é o que impede a derivada de
	// ressuscitar como campo de topo autodescoberto no reopen, levando junto o
	// SELECT * de que ela precisa ficar fora.
	if err := c.registraDerivadas(s, dict); err != nil {
		return nil, err
	}

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
			// Nome QUALIFICADO cujo prefixo é um campo json com descoberta
			// ligada: é uma coluna aninhada que uma execução anterior
			// descobriu. Reatá-la ao pai é o que a mantém fora do SELECT * e
			// alcançável por `pai.*`.
			//
			// O nome é o registro: ele é qualificado por construção, então não
			// há estado durável novo a inventar. E a condição exige o pai
			// DESCOBRIR — num dataset sem descoberta, um campo de topo cujo
			// nome tenha ponto continua sendo de topo.
			if i := strings.IndexByte(name, '.'); i > 0 {
				if pai, ok := c.byName[name[:i]]; ok && pai.DescobreAninhados {
					cf.Parent, cf.Path = pai, name[i+1:]
				}
			}
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

// indexKindDe traduz o vocabulário do YAML. Vive numa função só porque o
// campo de topo e a coluna derivada leem a MESMA chave, e duas traduções
// divergiriam no dia em que um índice novo entrasse.
func indexKindDe(yaml string) IndexKind {
	switch yaml {
	case "bitmap":
		return IndexBitmap
	case "hash":
		return IndexHash
	default:
		return IndexNone
	}
}

// registraDerivadas compila as colunas declaradas em `extract`.
//
// A ordem é determinística — pais em ordem alfabética, extrações na ordem
// escrita — pela mesma razão que o laço de topo ordena os nomes: o mesmo spec
// contra o mesmo dicionário tem de produzir sempre o mesmo mapeamento.
func (c *Compiled) registraDerivadas(s *DatasetSpec, dict FieldDictionary) error {
	pais := make([]string, 0, len(s.Schema.Fields))
	for nome := range s.Schema.Fields {
		if len(s.Schema.Fields[nome].Extract) > 0 {
			pais = append(pais, nome)
		}
	}
	sort.Strings(pais)

	for _, nomeDoPai := range pais {
		pai, ok := c.byName[nomeDoPai]
		if !ok {
			// Pai excluído ou fora do include: a spec já recusou isso na
			// validação, então chegar aqui seria bug de compilação.
			return fmt.Errorf("%w: campo %q declara extract e não foi compilado", ErrColunaDerivada, nomeDoPai)
		}
		for i, e := range s.Schema.Fields[nomeDoPai].Extract {
			nome, err := NomeDaExtracao(nomeDoPai, i, e)
			if err != nil {
				return err
			}
			tipo, _ := FieldTypeByName(e.Type)
			layout, err := CompilaFormato(e.Type, e.Format)
			if err != nil {
				return fmt.Errorf("%w: extract %q: %w", ErrColunaDerivada, nome, err)
			}
			id, err := dict.LookupOrCreate([]byte(nome))
			if err != nil {
				return fmt.Errorf("registrando coluna derivada %q no dicionário: %w", nome, err)
			}
			// Tipo já fixado no dicionário e DIVERGENTE do declarado: o ID
			// devolvido é de outra coluna — um campo de topo autodescoberto
			// com o mesmo nome, observado numa execução anterior. Aceitar
			// faria a derivada ler valores que nunca foram dela.
			//
			// Hint ausente não é conflito: sem tipo fixado também não há valor
			// materializado, e adotar o ID é inofensivo. É o que permite o
			// reopen normal de um dataset criado com extract.
			if t, temHint := dict.TypeHint(id); temHint && t != tipo {
				return fmt.Errorf("%w: %q declara tipo %s, mas o dataset já fixou %s para esse nome; "+
					"escolha outro `as` ou recrie o dataset",
					ErrColunaDerivada, nome, tipo, t)
			}
			cf := &CompiledField{
				ID:     id,
				Name:   nome,
				Type:   tipo,
				Index:  indexKindDe(e.Index),
				Format: e.Format,
				Layout: layout,
				Parent: pai,
				Path:   e.Path,
			}
			c.byName[cf.Name] = cf
			c.byID[cf.ID] = cf
			pai.Derivadas = append(pai.Derivadas, cf)
		}
	}
	return nil
}

// ResolveAninhado resolve — criando na primeira vez — a coluna de uma chave
// DESCOBERTA dentro de um documento json.
//
// É o irmão de ResolveUnknown um nível abaixo, e obedece às mesmas regras: o
// tipo é fixado na primeira observação, o teto de campos governa o
// crescimento, e o dicionário é a fonte durável do FieldID.
//
// O nome vem QUALIFICADO ("pai.chave") e o caller o monta num buffer reusado,
// porque isto roda no writer a cada evento: um nome novo por chave por evento
// seria alocação no hot path. O nome qualificado também é o que impede uma
// chave interna chamada `id` de colidir com a key do dataset.
//
// Devolve (nil, nil) quando o teto recusa sob política ignore — o mesmo
// contrato de ResolveUnknown, e o caller trata como "não virou coluna".
//
// Chamado apenas pela goroutine do writer.
func (c *Compiled) ResolveAninhado(pai *CompiledField, nomeQualificado, caminho []byte) (*CompiledField, error) {
	c.mu.RLock()
	f, ok := c.byName[string(nomeQualificado)]
	c.mu.RUnlock()
	if ok {
		if f.Parent != pai {
			// Nome já tomado por outra coluna: não adota. Só acontece se
			// alguém declarar um campo de topo com ponto no nome.
			return nil, nil
		}
		return f, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.byName[string(nomeQualificado)]; ok {
		if f.Parent != pai {
			return nil, nil
		}
		return f, nil
	}
	if recusa := c.tetoAlcancado(); recusa != nil {
		return nil, recusa
	}
	id, err := c.dict.LookupOrCreate(nomeQualificado)
	if err != nil {
		return nil, fmt.Errorf("registrando coluna aninhada %q: %w", nomeQualificado, err)
	}
	cf := &CompiledField{
		ID:     id,
		Name:   string(nomeQualificado),
		IsAuto: true,
		Parent: pai,
		Path:   string(caminho),
	}
	if t, ok := c.dict.TypeHint(id); ok {
		cf.autoType.Store(uint32(t))
	}
	c.byName[cf.Name] = cf
	c.byID[cf.ID] = cf
	return cf, nil
}

// tetoAlcancado aplica a política de estouro do dicionário. Chamado com c.mu
// já travado. nil = há espaço; erro = a política é `stop`; e (nil, nil) do
// caller quando ela é `ignore` — por isso ele devolve um erro sentinela só no
// caso que precisa parar.
func (c *Compiled) tetoAlcancado() error {
	if c.MaxCols <= 0 || c.dict.Len() < c.MaxCols {
		return nil
	}
	c.camposDescartados.Add(1)
	if c.OnMaxCols == OnMaxColsStop {
		return fmt.Errorf("%w (maxCols=%d)", ErrTetoDeCampos, c.MaxCols)
	}
	return errTetoIgnorado
}

// errTetoIgnorado é interno: sinaliza ao caller que a coluna não foi criada
// por política `ignore`, sem que isso vire erro de evento.
var errTetoIgnorado = errors.New("teto de campos alcançado; coluna ignorada")

// TetoIgnorou informa se o erro é a recusa silenciosa do teto.
func TetoIgnorou(err error) bool { return errors.Is(err, errTetoIgnorado) }

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
	return c.camposOrdenados(false, false)
}

// FieldsByIDIncluindoSistema devolve TODOS os campos, inclusive os que o
// writer injeta. É para quem precisa do conjunto físico do payload — não para
// quem monta a lista de colunas que o usuário vê.
func (c *Compiled) FieldsByIDIncluindoSistema() []*CompiledField {
	return c.camposOrdenados(true, true)
}

func (c *Compiled) camposOrdenados(comSistema, comDerivadas bool) []*CompiledField {
	c.mu.RLock()
	// byID tem UMA entrada por campo (byName tem duas para campo com alias):
	// não há duplicata a remover aqui.
	out := make([]*CompiledField, 0, len(c.byID))
	for _, f := range c.byID {
		if f.IsSystem && !comSistema {
			continue
		}
		if f.Parent != nil && !comDerivadas {
			continue
		}
		out = append(out, f)
	}
	c.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// FieldsParaMetadata é o conjunto que o /v1/metadata publica: as colunas de
// topo MAIS as derivadas, sem os campos de sistema.
//
// É deliberadamente um SUPERSET do SELECT *. Esconder a derivada aqui faria o
// DBeaver não a listar, e ninguém descobriria que ela existe — o operador a
// declarou justamente para consultá-la pelo nome.
func (c *Compiled) FieldsParaMetadata() []*CompiledField {
	return c.camposOrdenados(false, true)
}

// FieldByPath resolve a coluna derivada pelo CAMINHO dentro do documento do
// pai — que é como ela aparece no `pai.caminho` da projeção, e não
// necessariamente como ela se chama (o `as` pode renomeá-la).
func (c *Compiled) FieldByPath(pai, path string) (*CompiledField, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, f := range c.byID {
		if f.Parent != nil && f.Parent.Name == pai && f.Path == path {
			return f, true
		}
	}
	return nil, false
}

// FieldsByParent devolve as colunas extraídas de um campo json, em ordem
// crescente de FieldID — a mesma ordem do SELECT *, pela mesma razão: a
// projeção precisa ser estável entre execuções.
func (c *Compiled) FieldsByParent(pai string) []*CompiledField {
	c.mu.RLock()
	var out []*CompiledField
	for _, f := range c.byID {
		if f.Parent != nil && f.Parent.Name == pai {
			out = append(out, f)
		}
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
//     cacheia (idempotente);
//   - teto de campos alcançado, onMaxCols ignore → (nil, nil): o campo é
//     descartado e o EVENTO é gravado com todos os outros;
//   - teto de campos alcançado, onMaxCols stop → ErrTetoDeCampos.
//
// Nos dois últimos o contador de descartados sobe: o teto pode custar dado, e
// custá-lo em silêncio é o que não pode acontecer.
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
	// O teto governa o CRESCIMENTO, e a checagem vem depois do cache acima:
	// campo que já existe continua resolvendo mesmo com o dicionário cheio.
	// Sem essa ordem, o primeiro campo excedente derrubaria todo evento
	// seguinte — exatamente o que o teto deveria evitar.
	//
	// Ela também vem ANTES do LookupOrCreate, e é isso que torna o erro de
	// teto do próprio fielddict inalcançável por esta rota: os dois usam o
	// mesmo limiar, e este dispara primeiro, com política.
	if recusa := c.tetoAlcancado(); recusa != nil {
		if TetoIgnorou(recusa) {
			// ignore: mesmo desfecho do campo excluído — o evento é gravado
			// com todos os outros campos, e a perda fica no contador.
			return nil, nil
		}
		return nil, fmt.Errorf("%w: %s", recusa, name)
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
