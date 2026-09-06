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
	ID                types.FieldID
	Name              string
	Alias             string          // "" se ausente
	Type              types.FieldType // tipo DECLARADO; TypeNull quando IsAuto
	IsAuto            bool            // tipo resolvido na primeira observação
	Indexado          bool            // `index: true` no YAML — a única grafia desde a D5
	IsKey, IsSequence bool

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

	// FrozenFrom é a coluna de onde o valor congelado vem; nil quando esta
	// não é congelada. DoRelogio marca a congelada cuja origem é o relógio do
	// writer, que não tem CompiledField para apontar.
	//
	// A congelada é uma coluna NORMAL em tudo o que importa ao binder, ao
	// planner e ao encoder — tem FieldID, tipo e índice, aparece no SELECT *
	// e no /v1/metadata. O par governa uma coisa só: quem escreve o valor.
	FrozenFrom *CompiledField
	DoRelogio  bool

	// Acumula faz o merge SOMAR o valor do evento ao que já está no estado,
	// em vez de sobrescrevê-lo. Ver schema.MergeSoma para o que a soma custa
	// em idempotência.
	Acumula bool

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

	// ocorrencias e divergencias são a densidade e a confiabilidade DESTA
	// coluna, e existem porque o contador por DATASET é alarme sem endereço:
	// ele diz "2.085.138 valores perdidos" e não diz qual das ~1.700 colunas
	// respondeu por isso.
	//
	// São DUAS colunas cruas e não uma razão, porque a razão não compõe: saber
	// que uma chave divergiu 2.315 vezes só significa alguma coisa contra as
	// 2.383 em que ela apareceu, e `93 de 3.334` é problema de outra ordem. A
	// divisão fica com quem consulta.
	//
	// ocorrencias conta a chave APARECER no documento — antes da conversão —,
	// e não a materialização bem-sucedida. É o que faz divergencias ser um
	// SUBCONJUNTO de ocorrencias e a leitura "2.315 de 2.383" sair direta do
	// SQL; contando o sucesso, o denominador viraria uma soma na cabeça de
	// quem consulta.
	//
	// Atômicos pela mesma razão do autoType logo acima, e é a família dos
	// débitos 60, 69 e 70: as queries de _meta.fields leem estes campos
	// enquanto o writer os incrementa. A lição registrada naqueles três é que
	// "degrada com segurança" não sobreviveu ao -race.
	//
	// São contadores de PROCESSO e zeram no restart, como todos os outros. É
	// aceitável para a pergunta que respondem — "esta coluna é confiável?" —,
	// desde que esteja dito na documentação da tabela, senão alguém lê 0 e
	// conclui "estável".
	ocorrencias  atomic.Uint64
	divergencias atomic.Uint64

	// coercoes conta as ocorrências que chegaram noutra forma JSON e ainda
	// assim entraram na coluna — número numa coluna de texto, string numérica
	// numa coluna int64.
	//
	// Ele existe porque a coerção CEGA o contador vizinho. Enquanto a coluna
	// `string` recusava número, um valor divergente aparecia em divergencias e
	// o operador via que a origem estava inconsistente; passando a converter,
	// divergencias vai a ZERO no mesmo cenário e o diagnóstico some junto com o
	// problema aparente. Sem este terceiro contador, o efeito líquido da
	// mudança seria trocar uma perda visível por uma inconsistência invisível —
	// e a origem continuaria escrevendo `07` e `7` para a mesma coisa.
	//
	// É um contador SEPARADO e não uma soma com divergencias porque as duas
	// afirmações são diferentes e pedem remédios diferentes: divergiu é "o
	// valor se perdeu, a coluna está incompleta"; coergiu é "o valor está lá,
	// mas a origem escreve a mesma coisa de duas formas e o filtro por um único
	// literal não a encontra inteira". Somá-las apagaria justamente a distinção
	// que decide o que fazer.
	//
	// É subconjunto de ocorrencias, como divergencias, e disjunto dela: uma
	// ocorrência ou converteu ou não converteu.
	coercoes atomic.Uint64
}

// ContaOcorrencia registra que a chave desta coluna apareceu no documento.
//
// Roda no caminho mais quente do produto: uma vez por coluna derivada por
// evento. Um atomic.Add não aloca — o que o gate do materializer cobra — e o
// custo de CPU está medido em benchmark/baselines/segunda-passada-20260825.txt.
func (f *CompiledField) ContaOcorrencia() { f.ocorrencias.Add(1) }

// Ocorrencias é quantas vezes a chave desta coluna apareceu no documento desde
// a abertura do dataset. Zero para coluna que não é derivada.
func (f *CompiledField) Ocorrencias() uint64 { return f.ocorrencias.Load() }

// Divergencias é quantas dessas ocorrências não couberam no tipo da coluna.
// É sempre <= Ocorrencias.
func (f *CompiledField) Divergencias() uint64 { return f.divergencias.Load() }

// ContaCoercao registra que o valor chegou noutra forma JSON e foi convertido
// para o tipo da coluna sem perda.
//
// Roda no mesmo caminho quente do ContaOcorrencia acima — uma vez por coluna
// derivada por evento, e só nas que de fato coergiram —, e pela mesma razão é um
// atomic.Add: não aloca, que é o que o gate do materializer cobra.
func (f *CompiledField) ContaCoercao() { f.coercoes.Add(1) }

// Coercoes é quantas dessas ocorrências vieram noutra forma JSON e ainda assim
// entraram na coluna. É sempre <= Ocorrencias, e disjunto de Divergencias.
func (f *CompiledField) Coercoes() uint64 { return f.coercoes.Load() }

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

// NomeDoIndice devolve o índice em texto, para a coluna `index` do
// /v1/metadata e da homônima de `_meta.fields`. Substitui o IndexKind.String()
// que morreu com o enum dos dois motores, e devolve as MESMAS duas palavras que
// ele devolvia.
//
// "bitmap" e "none" são vocabulário de WIRE, e é por isso que eles não
// acompanharam a unificação dos motores: o driver JDBC decide por igualdade com
// "none" se a coluna entra no getIndexInfo (NishiDatabaseMetaData), e o teste de
// integração do client Python afirma "bitmap" contra o servidor real. Trocar as
// palavras aqui seria mudança de comportamento — em duas linguagens de cliente —
// vestida de rename, e ela não pertence a esta tarefa.
//
// O que a unificação de fato tirou do wire foi o terceiro valor: "hash" não é
// mais alcançável por config nenhuma desde a D5, e desde esta tarefa não existe
// sequer o tipo que o representava.
func (f *CompiledField) NomeDoIndice() string {
	if f.Indexado {
		return "bitmap"
	}
	return "none"
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
	// O typeHint é DURÁVEL e compartilhado, então o espelho já o veria no
	// próximo restart. Propagar agora é o que impede a janela em que a mesma
	// coluna tem tipo no dataset base e "sem tipo conhecido" na tabela irmã —
	// janela em que o binder recusaria, na irmã, a comparação que aceita no
	// base.
	for _, m := range c.listaDeEspelhos() {
		m.fixaTipoEspelhado(f.ID, t)
	}
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
	// Timeout é o prazo DEFAULT e MaxTimeout é o TETO que o request pode
	// alcançar. Os dois viajam para o compilado porque quem os consulta são o
	// binder (o default, que vira o deadline do executor quando o chamador não
	// trouxe prazo) e o servidor (o teto, que ele faz valer contra o
	// `timeoutMs` do request e publica em /v1/metadata). Ver QuerySpec, em
	// spec.go, para o porquê de serem dois campos e não um.
	Timeout    time.Duration
	MaxTimeout time.Duration
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

	Key, Seq *CompiledField

	// espelhos são os Compiled que recebem, em RUNTIME, toda coluna que este
	// aqui descobrir. O único caso hoje é a tabela histórica: ela tem Compiled
	// próprio (o nome, os índices e a ausência das congeladas diferem), mas
	// compartilha o dicionário — e sem o espelho uma chave promovida a coluna
	// depois da subida seria consultável no dataset base e invisível na tabela
	// irmã até o restart, quando a reidratação pelo dicionário a traria de
	// volta. Divergência entre as duas tabelas é exatamente o que a feature
	// existe para não ter.
	//
	// atomic.Pointer, e não um campo sob c.mu, porque os dois pontos de
	// propagação estão em regimes de lock diferentes: ResolveUnknown e
	// ResolveAninhado já seguram c.mu na escrita, e FixAutoType não segura
	// nada. Ler um slice imutável por ponteiro atômico serve aos dois sem
	// inventar uma segunda ordem de travamento.
	//
	// Espelho NÃO se encadeia: quem recebe a propagação não a repassa. Um
	// espelho de espelho tomaria dois locks em cadeia a partir do writer, e o
	// produto não tem caso que peça isso.
	espelhos atomic.Pointer[[]*Compiled]

	// Congeladas são as colunas com frozenFrom, em ordem de FieldID.
	// Pré-computada e ordenada pela mesma razão das Derivadas: o materializer
	// a percorre por evento, e varrer o mapa no caminho quente custaria caro.
	Congeladas []*CompiledField

	// RegrasTTL são as regras de expiração validadas, na ordem do arquivo —
	// que é semântica: a primeira que casa vence. O `when` continua em TEXTO
	// aqui de propósito: compilá-lo exige o binder, e o binder importa este
	// pacote. Quem compila é internal/predicate, o único ponto da árvore que
	// pode importar os dois.
	RegrasTTL []RegraTTL
	// DuracaoDeFundo é a duração da regra sem `when`, ou zero quando não há.
	DuracaoDeFundo time.Duration

	// Escrita é o campo de sistema que registra o instante da escrita.
	// Não-nil exatamente quando há regras de TTL — sem expiração não há para
	// que pagar 11 bytes por record.
	Escrita *CompiledField
	// Prazo é o campo de sistema que carrega a DURAÇÃO de ociosidade que vale
	// para a entidade, em millis. Não-nil junto com o Escrita: os dois só
	// fazem sentido em par — um diz quando foi a última escrita, o outro por
	// quanto tempo ela vale.
	Prazo *CompiledField

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
	// aoDescartar é avisado UMA vez, com o nome do primeiro campo que não
	// coube. Uma vez, e não por evento: sob a política `ignore` o descarte
	// passa a ser o estado normal, e uma linha por evento afogaria o log num
	// dataset que ingere a centenas de eventos por segundo. O contador conta
	// tudo; o log diz por onde começar a investigar.
	//
	// Escrito na fiação, antes de a ingestão começar, e lido pelo writer.
	aoDescartar  func(campo string)
	avisouDeTeto atomic.Bool

	// valoresIncompativeis conta os valores de coluna ANINHADA que não
	// couberam no tipo fixado. É um contador SEPARADO do de descarte por teto
	// de propósito: as causas são diferentes e os remédios também — o teto se
	// resolve subindo o número ou aceitando a perda, e a incompatibilidade de
	// tipo é a origem tendo mudado de forma.
	//
	// O evento entra de qualquer jeito; o que fica incompleto é a COLUNA
	// naquela linha, e o documento preserva o valor. Sem este contador isso
	// era invisível: o `descartados` do painel conta EVENTOS, e o de campos
	// conta o que o teto recusou.
	valoresIncompativeis atomic.Uint64
	aoIncompativel       func(coluna, fixado, veio string)
	avisouIncompativel   atomic.Bool
}

// ValoresIncompativeis é quantos valores de coluna aninhada não couberam no
// tipo fixado desde a abertura.
func (c *Compiled) ValoresIncompativeis() uint64 { return c.valoresIncompativeis.Load() }

// AoValorIncompativel registra o aviso da primeira divergência de tipo numa
// coluna aninhada. Chamado na fiação, antes de a ingestão começar.
func (c *Compiled) AoValorIncompativel(fn func(coluna, fixado, veio string)) { c.aoIncompativel = fn }

// ContaValorIncompativel registra que um valor não coube no tipo da coluna.
//
// O aviso sai UMA vez, com o tipo fixado E o que chegou: só com os dois o
// operador sabe o que mudou na origem. O contador conta todas.
func (c *Compiled) ContaValorIncompativel(coluna *CompiledField, veio types.FieldType) {
	c.valoresIncompativeis.Add(1)
	coluna.divergencias.Add(1)
	if c.aoIncompativel == nil || !c.avisouIncompativel.CompareAndSwap(false, true) {
		return
	}
	c.aoIncompativel(coluna.Name, coluna.EffectiveType().String(), veio.String())
}

// AoDescartarCampo registra o aviso de que o teto recusou um campo. Chamado na
// fiação do dataset, antes de a ingestão começar.
func (c *Compiled) AoDescartarCampo(fn func(campo string)) { c.aoDescartar = fn }

// avisaDescarte dispara o aviso na primeira vez, e só nela.
func (c *Compiled) avisaDescarte(campo string) {
	if c.aoDescartar == nil || !c.avisouDeTeto.CompareAndSwap(false, true) {
		return
	}
	c.aoDescartar(campo)
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
		MaxTimeout:    s.Query.MaxTimeout,
		MaxCols:       s.Schema.MaxCols,
		OnMaxCols:     s.Schema.OnMaxCols,
		MaxNestedCols: s.Schema.MaxNestedCols,
		Errors:        s.Errors,
		byName:        make(map[string]*CompiledField),
		byID:          make(map[types.FieldID]*CompiledField),
		exclude:       make(map[string]struct{}, len(s.Schema.Exclude)),
		dict:          dict,
	}
	c.RegrasTTL = s.Materialization.RegrasTTL
	c.DuracaoDeFundo = s.Materialization.DuracaoDeFundo
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
	// A ordem é contratual, e a razão é a do comentário do Open do storage: o
	// LookupOrCreate queima ids na ordem em que é chamado, e essa ordem decide
	// o mapeamento nome→id de um dataset NOVO, gravado para sempre. _writtenAt
	// primeiro, _expiresIn depois, sempre — nunca reordene.
	//
	// Os dois campos têm gatilhos DIFERENTES, e separá-los foi o que a tabela
	// histórica exigiu: ela precisa do instante de escrita para a retenção por
	// tempo, e não tem prazo por linha para guardar. Enquanto o gatilho era um
	// só, ligar `history.expireAfterWrite` num dataset sem regras de TTL
	// produzia um histórico sem `_writtenAt` — e um motor de retenção sem nada
	// contra o que comparar, que não expiraria coisa alguma sem erro e sem log.
	if s.ExigeInstanteDeEscrita() {
		names = append(names, CampoInstanteDeEscrita)
	}
	if len(c.RegrasTTL) > 0 {
		names = append(names, CampoDePrazo)
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
		if name == CampoDePrazo {
			// int64, e NÃO timestamp: ela carrega uma DURAÇÃO em millis, e
			// timestamp faria `SELECT _expiresIn` renderizar 3 minutos como
			// 1970-01-01T00:03:00Z — uma data que não é uma data.
			cf.Type, cf.IsSystem = types.TypeInt64, true
			c.Prazo = cf
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
			cf.Indexado = indexadoNoYAML(spec.Index)
			cf.Acumula = spec.Merge == MergeSoma
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
	}

	// A religação das congeladas roda DEPOIS do laço porque a origem pode ter
	// sido compilada depois da congelada — os nomes são percorridos em ordem
	// alfabética, e `originalSeverity` vem antes de `severity`.
	for name, spec := range s.Schema.Fields {
		if spec.FrozenFrom == "" {
			continue
		}
		cf, ok := c.byName[name]
		if !ok {
			continue // excluída do dataset
		}
		if spec.FrozenFrom == OrigemRelogio {
			cf.Type, cf.DoRelogio = types.TypeTimestamp, true
		} else {
			origem, ok := c.byName[spec.FrozenFrom]
			if !ok {
				return nil, specErrf("campo %q: origem %q não compilou (excluída?)", name, spec.FrozenFrom)
			}
			cf.FrozenFrom = origem
			cf.Type, cf.IsAuto = origem.Type, origem.IsAuto
			// O Format/Layout vem junto com o tipo, e não é acessório: em
			// datetime e time é ele que o binder usa para converter o literal
			// de texto do WHERE. Sem herdá-lo, a coluna guardaria o valor e
			// recusaria como "literal inválido" exatamente a grafia que a
			// origem aceita — a coluna existiria e não seria consultável.
			cf.Format, cf.Layout = origem.Format, origem.Layout
			if cf.IsAuto {
				// O hint é carregado aqui pela mesma razão do laço acima, e
				// não de graça: a congelada tem FieldID PRÓPRIO, então o tipo
				// que ela fixou numa execução anterior está sob o id DELA. Sem
				// esta leitura, o processo reiniciado trataria uma coluna que
				// já tem dados gravados como nunca observada.
				if t, ok := dict.TypeHint(cf.ID); ok {
					cf.autoType.Store(uint32(t))
				}
			}
		}
		c.Congeladas = append(c.Congeladas, cf)
	}
	sort.Slice(c.Congeladas, func(i, j int) bool { return c.Congeladas[i].ID < c.Congeladas[j].ID })

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
	// O dicionário é COMPARTILHADO entre o dataset base e a tabela irmã, e a
	// reidratação abaixo adota tudo o que estiver nele. Sem estes nomes fora
	// do universo, cada lado adotaria as colunas do outro como campo auto sem
	// tipo: o `_raw` apareceria no SELECT * do base sempre nulo, e a congelada
	// e o `_expiresIn` voltariam ao da irmã depois de a derivação os ter
	// tirado — os três com `IS NULL` eterno e recusa de comparação por "sem
	// tipo conhecido", que é pior do que a coluna não existir.
	for _, nome := range nomesForaDoUniverso(s) {
		c.exclude[nome] = struct{}{}
	}
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

	// Key nil é erro quando a spec DECLARA uma key — é o include que a deixou
	// de fora. A spec que não declara nenhuma é a do histórico, e ali a
	// ausência é o desenho: sem key não há primary, sem primary não há dedup,
	// e é o Key nil que impede o planner de eleger o point lookup (planner.go:94)
	// sobre um Store que não tem contra o que resolvê-lo.
	if s.Materialization.Key.Field != "" && c.Key == nil {
		return nil, specErrf("materialization.key.field %q fora do conjunto incluído", s.Materialization.Key.Field)
	}
	if seqField != "" && c.Seq == nil {
		return nil, specErrf("materialization.sequence.field %q fora do conjunto incluído", seqField)
	}
	return c, nil
}

// indexadoNoYAML traduz o vocabulário do YAML já validado (validaIndice, em
// congelada.go, só deixa passar "" ou "true" — chamar isto sobre um valor cru
// não validado seria bug de quem compila sem ter passado pelo validate).
// Vive numa função só porque o campo de topo e a coluna derivada leem a MESMA
// chave, e duas traduções divergiriam.
//
// O resultado é um bool, e não mais um enum de três valores, porque existe um
// motor de índice só (D5): "qual dos dois" deixou de ser uma pergunta, e o que
// resta é "tem ou não tem".
func indexadoNoYAML(yaml string) bool { return yaml == "true" }

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
				ID:       id,
				Name:     nome,
				Type:     tipo,
				Indexado: indexadoNoYAML(e.Index),
				Format:   e.Format,
				Layout:   layout,
				Parent:   pai,
				Path:     e.Path,
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
	if f, precisa := c.ResolveAninhadoSoLeitura(pai, nomeQualificado); !precisa {
		return f, nil
	}
	return c.criaAninhado(pai, nomeQualificado, caminho)
}

// ResolveAninhadoSoLeitura é o ResolveAninhado SEM NENHUM EFEITO.
//
// `precisa=true` marca o exato ponto em que a coluna seria criada. O caso
// `f.Parent != pai` — nome tomado por uma coluna de topo com ponto no nome —
// continua sendo um NÃO legítimo, e não um "precisaria criar": ele nunca vira
// coluna, em decodificador nenhum, e devolvê-lo como pendência faria o writer
// reprocessar o mesmo evento para sempre.
func (c *Compiled) ResolveAninhadoSoLeitura(pai *CompiledField, nomeQualificado []byte) (*CompiledField, bool) {
	c.mu.RLock()
	f, ok := c.byName[string(nomeQualificado)]
	c.mu.RUnlock()
	if !ok {
		return nil, true
	}
	if f.Parent != pai {
		// Nome já tomado por outra coluna: não adota. Só acontece se alguém
		// declarar um campo de topo com ponto no nome.
		return nil, false
	}
	return f, false
}

// criaAninhado é a metade que ESCREVE. Exclusivo da goroutine do writer.
func (c *Compiled) criaAninhado(pai *CompiledField, nomeQualificado, caminho []byte) (*CompiledField, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f, ok := c.byName[string(nomeQualificado)]; ok {
		if f.Parent != pai {
			return nil, nil
		}
		return f, nil
	}
	if recusa := c.tetoAlcancado(string(nomeQualificado)); recusa != nil {
		if TetoIgnorou(recusa) {
			// ignore: a coluna não existe, o evento é gravado com todo o resto,
			// e a perda fica no contador. Mesmo desfecho do ResolveUnknown.
			return nil, nil
		}
		// stop: o erro PRECISA subir. Devolver os dois casos como erro e deixar
		// o chamador tratá-los igual — que era o que esta função fazia — fazia a
		// política `stop` ser silenciosamente rebaixada a `ignore` no caminho
		// aninhado, enquanto o caminho de topo a honrava.
		return nil, fmt.Errorf("%w: %s", recusa, nomeQualificado)
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
	c.propagaAosEspelhos(cf)
	return cf, nil
}

// tetoAlcancado aplica a política de estouro do dicionário. Chamado com c.mu
// já travado. nil = há espaço; erro = a política é `stop`; e (nil, nil) do
// caller quando ela é `ignore` — por isso ele devolve um erro sentinela só no
// caso que precisa parar.
func (c *Compiled) tetoAlcancado(campo string) error {
	if c.MaxCols <= 0 || c.dict.Len() < c.MaxCols {
		return nil
	}
	c.camposDescartados.Add(1)
	c.avisaDescarte(campo)
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

// AnexaFilhos acrescenta a dst todas as colunas derivadas de pai — as
// DECLARADAS por `extract` e as DESCOBERTAS por `unknownFields: include` —, e
// devolve o slice possivelmente crescido.
//
// Existe separada de FieldsByParent por dois motivos, e os dois importam. O
// primeiro é o buffer: FieldsByParent aloca um slice novo e o ordena a cada
// chamada, o que serve à projeção (uma vez por query) e não ao caminho de
// ingestão. Esta anexa num buffer do chamador e não ordena — quem consome as
// entries já as ordena por FieldID depois.
//
// O segundo é o conjunto. `pai.Derivadas` tem só as DECLARADAS: ResolveAninhado
// registra a coluna descoberta em byName/byID com o Parent apontado, e nunca a
// anexa à lista do pai. Quem precisar tratar "todas as colunas deste
// documento" e olhar só as Derivadas trata um dataset de descoberta pura — o
// caso do piloto — como se ele não tivesse coluna nenhuma.
//
// A ordem NÃO é estável entre chamadas (varre um map). Quem precisar de ordem
// estável usa FieldsByParent.
func (c *Compiled) AnexaFilhos(pai *CompiledField, dst []*CompiledField) []*CompiledField {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, f := range c.byID {
		if f.Parent == pai {
			dst = append(dst, f)
		}
	}
	return dst
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
//
// Ele NÃO é acessório de teste: é o único ponto que torna observável o alias
// dentro do conjunto de exclusão. Sem ele, apagar a linha que interna o alias
// (compile.go, `c.exclude[spec.Alias]`) passa VERDE na suíte inteira — medido.
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
	f, precisa, err := c.resolveDesconhecido(name)
	if err != nil || !precisa {
		return f, err
	}
	return c.criaDesconhecido(name)
}

// ResolveUnknownSoLeitura é o ResolveUnknown SEM NENHUM EFEITO.
//
// Devolve `precisa=true` no exato ponto em que o ResolveUnknown criaria a
// coluna — e nem um passo além. É o que permite decodificar um evento fora da
// goroutine do writer: o decodificador descobre que não pode decidir e para,
// sem ter tocado o dicionário, os contadores nem o aviso do teto.
//
// A DISTINÇÃO QUE NÃO PODE COLAPSAR. Há quatro maneiras de um campo
// desconhecido NÃO virar coluna — excluído, universo fechado, política
// `ignore` e política `reject` —, e nenhuma delas é "precisaria criar". Um
// leitor que devolvesse `precisa=true` para todas faria o writer reprocessar
// todo evento que traga um campo excluído, para sempre, num laço que nenhuma
// métrica explicaria.
//
// O TETO FICA DE FORA de propósito, e é a única assimetria. Ele podia ser
// consultado puramente (`dict.Len() >= MaxCols`), mas o ResolveUnknown
// CONTABILIZA a recusa — camposDescartados e o aviso de uma vez só — antes de
// decidir entre parar e ignorar. Antecipar essa contabilidade aqui a duplicaria
// no evento reprocessado; adiá-la exigiria uma terceira via. A escolha é
// devolver `precisa=true` e deixar o writer decidir, ao custo de um parse
// dobrado por evento enquanto o dicionário estiver cheio sob `ignore` — a
// degradação nomeada no plano da Fase 4: nunca pior que a vazão de antes, e
// visível em nishidb_ingest_decode_serial_total.
func (c *Compiled) ResolveUnknownSoLeitura(name []byte) (*CompiledField, bool, error) {
	return c.resolveDesconhecido(name)
}

// resolveDesconhecido é a parte PURA do ResolveUnknown: a política e o cache.
// precisa=true significa "daqui em diante só o writer pode seguir".
func (c *Compiled) resolveDesconhecido(name []byte) (*CompiledField, bool, error) {
	if _, excluded := c.exclude[string(name)]; excluded {
		return nil, false, nil
	}
	if !c.IncludeAll || c.Unknown == UnknownIgnore {
		return nil, false, nil
	}
	if c.Unknown == UnknownReject {
		return nil, false, fmt.Errorf("%w: %s", ErrUnknownField, name)
	}
	// RLock, e não Lock: esta metade é consulta, e ela roda em N
	// decodificadores ao mesmo tempo. A escrita mora no criaDesconhecido, que
	// retoma o lock exclusivo e reconfere o cache.
	c.mu.RLock()
	f, ok := c.byName[string(name)]
	c.mu.RUnlock()
	if ok {
		return f, false, nil
	}
	return nil, true, nil
}

// criaDesconhecido é a metade que ESCREVE: teto, dicionário, cache e espelhos.
// Exclusivo da goroutine do writer.
func (c *Compiled) criaDesconhecido(name []byte) (*CompiledField, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Reconfere sob o lock exclusivo: entre o RLock do resolveDesconhecido e
	// este Lock, o próprio writer pode ter criado a coluna ao reprocessar um
	// evento anterior do mesmo lote.
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
	if recusa := c.tetoAlcancado(string(name)); recusa != nil {
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
	c.propagaAosEspelhos(cf)
	return cf, nil
}
