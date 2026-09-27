// Package schema carrega, valida e compila as materialization definitions
// YAML dos datasets (spec §12-17). A forma compilada (Compiled) é a usada
// no hot path de ingestão e pelo query engine.
package schema

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/units"
)

// ErrInvalidSpec categoriza qualquer erro de validação de spec.
var ErrInvalidSpec = errors.New("materialization spec inválida")

// SyntheticSchemaName é o schemaName das specs SINTÉTICAS — as das tabelas
// virtuais (`_meta.*`, `_counters.*`), montadas em código para atravessar o
// mesmo Parse/Compile de um dataset de verdade em vez de terem um caminho
// próprio de validação.
//
// Ela existe porque a obrigatoriedade do schemaName vale para todas as specs,
// sem exceção que abriria buraco; e é UMA constante porque três literais
// soltos em três pacotes viram três valores diferentes na primeira vez que
// alguém mexer num deles.
//
// # POR QUE O PREFIXO "_"
//
// Ninguém LÊ este valor hoje, e é justamente por isso que ele não pode ser um
// nome que um dataset de usuário também poderia declarar: sentinela que se
// confunde com dado legítimo não se denuncia quando vaza — ela aparece como um
// schema plausível e ninguém desconfia. O prefixo "_" é o que validate recusa
// para todo schemaName vindo de YAML, e esta constante é a ÚNICA exceção a
// essa recusa; Load, que é a porta por onde entra todo dataset de arquivo,
// recusa o valor explicitamente. As duas guardas juntas dizem que nenhuma spec
// de usuário pode carregar este schema, e que encontrá-lo fora das quatro
// specs sintéticas é bug, não configuração.
//
// # O CATÁLOGO SOBRESCREVE ESTE VALOR
//
// O catálogo existe, e nenhuma linha dele carrega este schema: a tabela virtual
// aparece no schema do DATASET a que pertence — que é quem declarou um — ou em
// `_meta`, quando ela descreve o motor e não o dado
// (query.Engine.catalogEntries). Este valor continua sem consumidor: ele existe
// para a spec sintética atravessar o Parse/Compile, e a montagem da árvore o
// substitui antes de publicar qualquer coisa.
const SyntheticSchemaName = "_virtual"

// DatasetSpec é a spec validada, com defaults aplicados.
type DatasetSpec struct {
	Version int
	Name    string
	// SchemaName é o schema em que TODAS as tabelas deste dataset vivem — a
	// base, os contadores e as de meta. É obrigatório e não tem default: um
	// default faria `servicos.servicos`, e a ausência faria schema nula na
	// árvore do cliente SQL. As duas foram olhadas e recusadas no desenho.
	//
	// A validação desta spec estabelece a gramática (`[a-z0-9_]+`) e a reserva
	// do prefixo "_" — que é o que separa o que é do motor do que é do dado.
	// Ela NÃO estabelece a unicidade entre datasets: isso é validação de
	// CONJUNTO e não cabe aqui, que vê um arquivo por vez; quem recusa dois
	// datasets no mesmo schema é config.ValidateSchemaUniqueness, na carga do
	// diretório.
	//
	// O campo é LIDO em três lugares, e por isso a recusa acima deixou de ser
	// preventiva: o motor resolve o qualificador de um `FROM telecom.tabela`
	// contra os schemas declarados (query.Engine.resolveSchemaQualified, que
	// varre a ENUMERAÇÃO do catálogo — catalogEntries — e não o
	// query.Engine.Catalog, que é o invólucro que acrescenta as colunas e não
	// participa da resolução), a `_meta.tables` o publica por SQL e o
	// `/v1/metadata` o publica no wire, por dataset e por tabela. A tabela IRMÃ do histórico herda este schema (DeriveHistory), e é
	// por isso que ela não entra na conta da unicidade: é outra tabela do mesmo
	// dataset, não outro dataset.
	SchemaName      string
	Source          SourceSpec
	Errors          ErrorsSpec
	Materialization MaterializationSpec
	Schema          SchemaSpec
	Query           QuerySpec
	// History é o bloco `history` validado, ou nil no dataset sem tabela
	// irmã. Ver DeriveHistory para a spec que ele gera.
	History *HistorySpec
	// IsHistory marca a spec DERIVADA — a da própria tabela irmã. Ela não
	// vem de arquivo nenhum: nasce de DeriveHistory e existe só em memória,
	// e é por isso que ListDatasets nunca a enxerga.
	IsHistory bool
	// Counters é o bloco `counters` validado; nil no dataset sem contador.
	Counters []CounterSpec
	// Replication configures the optional master-side change feed. It is
	// intentionally per dataset because a feed is shared by that dataset's
	// followers and has its own bounded memory budget.
	Replication ReplicationSpec
}

// ReplicationSpec configures the optional master/follower change feed.
type ReplicationSpec struct {
	Enabled          bool
	Role             string
	MasterURL        string
	UsernameEnv      string
	PasswordEnv      string
	FlushInterval    time.Duration
	MaxMessages      int
	MaxBatchBytes    int64
	MaxBufferedBytes int64
	// Targets são os nomes dos destinos Kafka globais (`kafkaTargets:` do
	// nishidb.yaml) que também recebem o feed deste dataset, na ordem do
	// arquivo. Vazio (nil) significa feed servido só aos seguidores. Aqui só
	// se valida a forma de cada nome: a spec do dataset não enxerga o
	// nishidb.yaml, e por isso a existência do destino NÃO é conferida aqui.
	Targets []string
}

// MaterializationSpec ...
type MaterializationSpec struct {
	Key      KeySpec
	Mode     string // merge | replace
	Sequence *SequenceSpec
	// TTLRules são as regras de expiração VALIDADAS, na ordem do arquivo —
	// que é semântica: a primeira que casa vence. Vazia significa dataset sem
	// expiração.
	TTLRules []TTLRule
	// FallbackDuration é a duração da regra sem `when`, ou zero quando não há.
	// Sai pré-computada porque quem a consome — os montadores de Options do
	// storage — não deve reimplementar a busca pela última regra.
	FallbackDuration time.Duration
	// TombstoneRetention é por quanto tempo a lápide de uma entidade expirada
	// precisa sobreviver antes de a key poder ser esquecida. VALIDADO aqui e
	// ainda SEM MOTOR: implementá-lo exige purgar a primary (remoção em
	// open-addressing) e ensinar a compaction a descartar lápide vencida.
	//
	// Era guardado como string crua, sem parse e sem leitor — configuração que
	// não faz nada e não avisa. Agora a gramática é conferida no load e a
	// subida avisa que a chave não tem efeito, que é o mínimo honesto
	// enquanto o motor não existe.
	TombstoneRetention time.Duration
}

// KeySpec ...

type KeySpec struct {
	Source string // payload (único no v1)
	Field  string
	Type   string // string (único no v1)
	// SequenceSpec ...
}

type SequenceSpec struct {
	Field    string
	Type     string // int64 (único no v1)
	Strategy string // greater (único no v1)
}

// ExpiryResolution é o passo do agendador (§57). Duração menor que isto é
// recusada em vez de arredondada: o operador leria "30s" e teria até 90s, e
// SchemaSpec ...
// uma chave que mente é pior que uma chave recusada.
const ExpiryResolution = time.Minute

type SchemaSpec struct {
	Include       []string // ["*"] ou lista explícita; default = campos declarados
	Exclude       []string
	UnknownFields string // include | ignore | reject (default ignore)
	Fields        map[string]FieldSpec
	// MaxCols é quantos campos o dataset pode registrar no dicionário, e
	// OnMaxCols é o que acontece ao bater nele. São dois porque o número não é
	// a decisão importante: o COMPORTAMENTO ao estourar é.
	MaxCols   int
	OnMaxCols string // ignore | stop (default ignore)
	// MaxNestedCols é quantas colunas DERIVADAS uma consulta pode projetar.
	// Governa a expansão `pai.*`, que é a única forma de a projeção crescer
	// sem o usuário escrever cada nome.
	MaxNestedCols int
}

// Política de estouro do teto de campos.
//
// `partial` — gravar o evento e MARCÁ-LO como incompleto — foi considerada e
// descartada: no modo merge, campo ausente do patch é preservado
// (materializer.mergeRecords), então uma marca de "veio incompleto" nunca
// poderia ser limpa por um evento posterior completo, nem atribuída ao evento
// que de fato perdeu o campo. Ela mentiria para sempre a partir da primeira
// vez. O que a proposta queria — que a perda fosse contável e não descoberta
// por acaso — é entregue pelo contador por dataset, que não tem esse defeito.
const (
	OnMaxColsIgnore = "ignore"
	OnMaxColsStop   = "stop"

	// DefaultMaxNestedCols é o teto default de colunas derivadas por consulta.
	// O número é folgado de propósito: quem controla o caso real é a lista de
	// extrações que o operador escreveu, e este é backstop contra uma
	// expansão `pai.*` sobre um pai com centenas de chaves declaradas.
	DefaultMaxNestedCols = 256

	// MaxColsPhysicalCeiling é o teto que o dicionário impõe por construção: o
	// types.FieldID é uint16. Declarar acima disto seria uma promessa que o
	// fielddict recusa primeiro, então a spec recusa antes.
	MaxColsPhysicalCeiling = 65535
)

// FieldSpec tags de YAML são EXPLÍCITAS mesmo quando redundantes. O default do
// yaml.v3 é minúsculo o nome inteiro do campo Go, então um nome de duas
// palavras vira "unknownfields" e a chave camelCase do arquivo não casa com
// nada — em silêncio, porque este struct é decodificado sem KnownFields. É a
// mesma família do defeito de configuração sem leitor que o projeto já
// registra, e ela mordeu aqui na primeira chave de duas palavras.
type FieldSpec struct {
	Type   string `yaml:"type"`
	Alias  string `yaml:"alias"`
	Index  string `yaml:"index"`  // "" | "true" — hash e bitmap são RECUSADOS na subida (D5)
	Format string `yaml:"format"` // padrão de parsing; obrigatório em datetime/time, proibido no resto
	// UnknownFields liga a DESCOBERTA das chaves de dentro de um campo json:
	// "include" as promove a coluna conforme aparecem, "ignore" (default)
	// deixa o documento opaco.
	//
	// É a mesma palavra do bloco `schema`, um nível abaixo, e de propósito:
	// quem já sabe o que ela faz com os campos de topo sabe o que ela faz aqui.
	// "reject" NÃO existe neste nível — recusar o evento por uma chave interna
	// desconhecida derrubaria o evento inteiro por causa do documento, que é o
	// oposto do que este campo serve para fazer.
	UnknownFields string `yaml:"unknownFields"`

	// Extract declara chaves de DENTRO de um campo json como colunas de
	// verdade. Só vale sobre type: json — nos outros tipos não há o que
	// percorrer, e aceitar em silêncio seria configuração sem leitor.
	Extract []ExtractSpec `yaml:"extract"`

	// Merge é a política de escrita do campo quando o merge roda: vazio
	// sobrescreve (o default), MergeSum acumula. Ver o godoc da constante
	// para o que a soma custa em idempotência.
	Merge string `yaml:"merge"`

	// FrozenFrom nomeia a ORIGEM de uma coluna congelada: outra coluna
	// declarada, ou FrozenFromClock. A coluna recebe o primeiro valor que a
	// origem tiver e nunca mais muda.
	//
	// A tag é explícita como todas as outras: sem ela o yaml.v3 procuraria
	// "frozenfrom" e a chave camelCase do arquivo não casaria com nada, em
	// silêncio — este struct é decodificado sem KnownFields.
	FrozenFrom string `yaml:"frozenFrom"`
}

// ExtractSpec é uma chave de dentro de um documento json promovida a coluna.
//
// O documento original NÃO é removido: extrair não é mover. A coluna json
// continua inteira, e quem já lia o documento continua lendo — o que a
// extração acrescenta é um lugar tipado, comparável e indexável para a chave
// que importa.
type ExtractSpec struct {
	// Path é o caminho dentro do documento, com '.' separando níveis.
	Path string `yaml:"path"`
	// As é o nome da coluna resultante. Omitido, vale o próprio Path — o que
	// só é possível quando ele não tem ponto, porque nome de coluna com ponto
	// colidiria com a sintaxe de caminho da projeção.
	As   string `yaml:"as"`
	Type string `yaml:"type"`
	// QuerySpec ...
	Index  string `yaml:"index"`
	Format string `yaml:"format"`
}

type QuerySpec struct {
	MaxRows int
	// Timeout é o prazo DEFAULT: o que vale quando o request NÃO traz
	// `timeoutMs`. MaxTimeout é o TETO que o `timeoutMs` pode alcançar.
	//
	// São dois campos porque são duas perguntas, e enquanto havia um só as duas
	// tinham a mesma resposta: `query.timeout` era ao mesmo tempo o prazo de
	// quem não pede nada E o limite de quem pede. Com uma fonte só, dar prazo
	// longo a UMA consulta exigia subir o teto de TODAS — inclusive o das que
	// esqueceram de chamar setQueryTimeout, que passariam a poder segurar uma
	// vaga do §80 por minutos sem nunca terem pedido isso.
	//
	// MaxTimeout ausente vale exatamente o Timeout RESOLVIDO deste dataset —
	// não o default global, não 30s. É essa regra que faz todo nishidb.yaml
	// escrito antes da separação produzir comportamento idêntico ao de antes:
	// um dataset com `query.timeout: 5m` continua com teto de 5m em vez de
	// passar a ser recusado por um teto global menor.
	Timeout    time.Duration
	MaxTimeout time.Duration
}

// Estruturas cruas do YAML (tudo opcional; defaults e validação em validate).
type rawSpec struct {
	Version    *int   `yaml:"version"`
	Name       string `yaml:"name"`
	SchemaName string `yaml:"schemaName"`
	// Source e Errors ficam como nó cru porque a recusa de chave desconhecida
	// nesses dois blocos é feita à mão, sobre as chaves do documento — ver
	// block(), em source.go, para o motivo de yaml.KnownFields não servir.
	//
	// São VALOR e não ponteiro porque o yaml.v3 só trata yaml.Node de forma
	// especial no tipo valor: um campo *yaml.Node fica com o nó zerado, sem
	// erro nenhum. Bloco ausente é Kind == 0.
	Source yaml.Node `yaml:"source"`
	Errors yaml.Node `yaml:"errors"`
	// History é nó cru pela MESMA razão de source e errors: bloco novo, sem
	// legado a preservar, e a recusa de chave desconhecida ali é o que separa
	// `maxByte: 1GiB` (typo) de uma retenção que simplesmente não existe.
	History yaml.Node `yaml:"history"`
	// Index existe aqui para ser RECUSADO, e não para ser lido: a janela de
	// indexação é `history.index`, e uma chave `index:` no topo do documento
	// seria simplesmente IGNORADA — o rawSpec não usa yaml.KnownFields, então
	// chave desconhecida de topo passa em silêncio. O operador escreveria a
	// janela no lugar errado e receberia o comportamento default sem uma linha
	// de log, que é exatamente a configuração aceita e sem efeito que este
	// projeto recusa na porta de entrada.
	Index           yaml.Node `yaml:"index"`
	Materialization rawMat    `yaml:"materialization"`
	Schema          rawSchema `yaml:"schema"`
	Query           rawQuery  `yaml:"query"`
	// Counters é nó cru pela razão de history: bloco novo, sem legado, e a
	// recusa de chave desconhecida por item é o que separa `wher:` (typo) de
	// um contador que conta tudo sem que ninguém tenha pedido.
	Counters    yaml.Node      `yaml:"counters"`
	Replication rawReplication `yaml:"replication"`
}

type rawReplication struct {
	Enabled          *bool  `yaml:"enabled"`
	Role             string `yaml:"role"`
	MasterURL        string `yaml:"masterURL"`
	UsernameEnv      string `yaml:"usernameEnv"`
	PasswordEnv      string `yaml:"passwordEnv"`
	FlushInterval    string `yaml:"flushInterval"`
	MaxMessages      *int   `yaml:"maxMessages"`
	MaxBatchBytes    string `yaml:"maxBatchBytes"`
	MaxBufferedBytes string `yaml:"maxBufferedBytes"`
	// Targets é nó cru, e não []string, pela razão de `emit.target`: o
	// yaml.v3 descarta em silêncio o item nulo de um []string (`[a, ~]` vira
	// `[a]`, `[~]` vira lista vazia) e coage `1`/`true` para string. Com o nó,
	// ausente (Kind zero) se distingue de `targets: []` e de `targets:` nulo,
	// e todo item que não seja string explícita é recusado pelo nome.
	Targets yaml.Node `yaml:"targets"`
	// Target existe aqui para ser RECUSADO, pela razão do Index de topo: a
	// chave irmã em `emit` é `target`, no singular, e o rawReplication não usa
	// yaml.KnownFields — `replication.target: [x]` passaria em silêncio e o
	// feed não seria publicado em lugar nenhum, sem uma linha de log.
	Target yaml.Node `yaml:"target"`
}

type rawMat struct {
	Key      rawKey        `yaml:"key"`
	Mode     string        `yaml:"mode"`
	Sequence *SequenceSpec `yaml:"sequence"`
	// TTL é yaml.Node, e não []TTLRuleSpec direto, para que a forma ANTIGA —
	// o mapa único — seja recusada com uma mensagem que nomeia a lista, em vez
	// da mensagem do yaml.v3 sobre tipos que o operador nunca escreveu.
	//
	// Valor e não ponteiro pela razão do rawSpec: o yaml.v3 só trata
	// yaml.Node de forma especial no tipo valor.
	TTL                yaml.Node `yaml:"ttl"`
	TombstoneRetention string    `yaml:"tombstoneRetention"`
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
	// Ponteiro pela mesma razão do rawQuery.MaxRows: separa "ausente" de
	// "zero", e zero é um pedido explícito de nada, que precisa ser recusado
	// com a mesma clareza de um negativo.
	MaxCols       *int   `yaml:"maxCols"`
	OnMaxCols     string `yaml:"onMaxCols"`
	MaxNestedCols *int   `yaml:"maxNestedCols"`
}

type rawQuery struct {
	MaxRows *int   `yaml:"maxRows"`
	Timeout string `yaml:"timeout"`
	// MaxTimeout é string vazia quando ausente, e ausente NÃO é zero: vale o
	// Timeout resolvido deste dataset. Ver QuerySpec.
	MaxTimeout string `yaml:"maxTimeout"`
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
	// MaxTimeout é ZERO quando o nishidb.yaml não declara
	// `query.defaultMaxTimeout`, e zero aqui significa "cada dataset herda o
	// PRÓPRIO Timeout resolvido" — não "sem teto" nem "trinta segundos".
	//
	// É esta a peça que preserva o comportamento de todo arquivo escrito antes
	// de o teto existir: um dataset com `query.timeout: 5m` e nenhum
	// maxTimeout continua com teto de 5m, e não com o default global de 30s,
	// que recusaria a própria consulta que ele já atende hoje.
	MaxTimeout time.Duration
}

// BuiltinDefaults são os valores da spec §78 para quem carrega uma spec sem
// nishidb.yaml nenhum por perto — testes, benchmarks e ferramentas.
//
// MaxTimeout fica ZERO de propósito: escrever 30s aqui seria um segundo lugar
// de onde o teto poderia sair, e ele divergiria do Timeout no dia em que um
// dataset declarasse `query.timeout` maior.
var BuiltinDefaults = Defaults{MaxRows: 100000, Timeout: 30 * time.Second}

// Load lê e valida uma spec de um arquivo YAML.
//
// É a porta por onde entra todo dataset que vem de arquivo — o servidor, a CLI
// e o benchmark passam por aqui —, e por isso é aqui que mora a recusa do
// SyntheticSchemaName: validate precisa aceitá-lo, porque as specs sintéticas
// das tabelas virtuais atravessam o mesmo caminho, mas nada que venha de um
// YAML de usuário pode carregar o sentinela do produto.
func Load(path string, d Defaults) (*DatasetSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("lendo spec %s: %w", path, err)
	}
	s, err := Parse(data, d)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if s.SchemaName == SyntheticSchemaName {
		return nil, fmt.Errorf("%s: %w", path, specErrf(
			"schemaName %q é reservado às tabelas virtuais do produto e não pode ser declarado por um dataset; escolha outro nome",
			SyntheticSchemaName))
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

// withoutSpecPrefix desembrulha a mensagem de um erro já marcado com
// ErrInvalidSpec, para que aninhá-lo em outro specErrf não produza
// "spec inválida: spec inválida: ..." na cara do operador.
func withoutSpecPrefix(err error) string {
	s := err.Error()
	if p := ErrInvalidSpec.Error() + ": "; strings.HasPrefix(s, p) {
		return s[len(p):]
	}
	return s
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
	"datetime":    types.TypeDateTime,
	"time":        types.TypeTime,
}

// formatDomain mapeia o tipo YAML para o domínio do compilador de padrão.
// Só estes dois tipos aceitam (e exigem) `format:`.
func formatDomain(typ string) (typeparse.Domain, bool) {
	switch typ {
	case "datetime":
		return typeparse.DomainDateTime, true
	case "time":
		return typeparse.DomainTime, true
	}
	return 0, false
}

// CompileFormat resolve o `format:` de um campo para o layout de parsing.
// Dono único da regra: quem valida a spec e quem compila o schema chamam
// AQUI, nunca uma segunda lista de formatos — duas listas divergiriam e o
// dataset aceitaria no load o que recusa por evento na ingestão.
func CompileFormat(typ, format string) (*typeparse.Layout, error) {
	d, usesFormat := formatDomain(typ)
	if !usesFormat {
		if format != "" {
			return nil, specErrf("format só se aplica a datetime e time; o tipo %q não o usa", typ)
		}
		return nil, nil
	}
	if format == "" {
		// Sem default de propósito: um format esquecido escolheria um padrão
		// que recusa 100% dos eventos, e sob log-and-skip isso é um dataset
		// que não ingere nada — só WARNs, sem nada apontando para a causa.
		return nil, specErrf("tipo %q exige format (tokens: %s)", typ, typeparse.SupportedTokens())
	}
	l, err := typeparse.CompileLayout(format, d)
	if err != nil {
		return nil, specErrf("%s", err)
	}
	return l, nil
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

// systemNames são os campos que o WRITER injeta e o operador não declara.
//
// Lista fechada, e não o prefixo "_" inteiro: campos com underscore são
// legítimos e já estão em uso — `_SEQ` é o nome de sequence da spec §12 e da
// fixture de tipos. Reservar o prefixo recusaria datasets que hoje sobem.
var systemNames = map[string]string{
	WriteInstantFieldName: "é injetado pelo writer a cada escrita",
	TTLFieldName:          "é calculado pelo writer a partir das regras de ttl",
}

// WriteInstantFieldName registra QUANDO o writer gravou a entidade. É um
// fato, não uma política: guardar o instante da escrita (e não o prazo já
// somado) é o que faz uma mudança de expireAfterWrite valer imediatamente
// para as linhas existentes, em vez de exigir reescrever o dataset.
const WriteInstantFieldName = "_writtenAt"

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

// bitmapIndexable/hashIndexable definem os tipos que cada um dos DOIS motores
// GLOBAIS aceitava (design §2.1) — histórico, porque os dois motores saíram
// (Tarefa 9), mas os dois conjuntos continuam vivos como a base do que
// isIndexableType une logo abaixo: `auto` não entra em nenhum dos dois porque
// o tipo só é conhecido em runtime.
func bitmapIndexable(t string) bool {
	return t == "string" || t == "int64" || t == "boolean"
}

func hashIndexable(t string) bool {
	return t == "string" || t == "int64" || t == "ipv4" || t == "ipv6"
}

// isIndexableType é o conjunto que `index: true` aceita: a UNIÃO dos dois
// motores antigos, e não a interseção. Com um motor só, a distinção de
// cardinalidade que os separava morreu — mas nenhuma coluna pode PERDER a
// indexabilidade que já tinha só porque as duas grafias viraram uma: um campo
// `ipv4` que só bitmapIndexable recusava (e só hashIndexable aceitava)
// continua indexável com `index: true`, exatamente como continuava indexável
// com `index: hash`.
func isIndexableType(t string) bool {
	return bitmapIndexable(t) || hashIndexable(t)
}

// validateExtracts confere o bloco `extract` de todos os campos.
//
// Roda numa passada PRÓPRIA, depois de include/exclude estarem resolvidos, e
// percorre os campos em ordem alfabética. As duas coisas importam: a primeira
// porque a validade de uma extração depende de o PAI estar visível, e a
// segunda porque o laço de schema.fields itera um mapa — sem ordenar, a
// mensagem de erro de um YAML com dois defeitos mudaria entre execuções.
func validateExtracts(s *DatasetSpec, fields map[string]FieldSpec, aliases map[string]string, include []string, includeAll bool) error {
	parentNames := make([]string, 0, len(fields))
	for name := range fields {
		if len(fields[name].Extract) > 0 {
			parentNames = append(parentNames, name)
		}
	}
	sort.Strings(parentNames)

	included := make(map[string]struct{}, len(include))
	for _, n := range include {
		included[n] = struct{}{}
	}
	excluded := make(map[string]struct{}, len(s.Schema.Exclude))
	for _, n := range s.Schema.Exclude {
		excluded[n] = struct{}{}
	}

	seen := map[string]string{} // as → pai que o declarou
	for _, parent := range parentNames {
		f := fields[parent]
		if f.Type != "json" {
			return specErrf("campo %q: extract só vale sobre type: json, e ele é %q — nos outros tipos não há documento a percorrer", parent, f.Type)
		}
		if _, isExcluded := excluded[parent]; isExcluded {
			return specErrf("campo %q está no exclude e declara extract; o campo excluído nem é materializado, então a extração nunca rodaria", parent)
		}
		if !includeAll {
			if _, isIncluded := included[parent]; !isIncluded {
				return specErrf("campo %q declara extract e não está no include; a extração nunca rodaria", parent)
			}
		}
		for i, e := range f.Extract {
			name, err := derivedColumnName(parent, i, e)
			if err != nil {
				return err
			}
			if !isValidTypeName(e.Type) {
				return specErrf("campo %q: extract %q: tipo %q inválido", parent, name, e.Type)
			}
			if e.Type == "auto" {
				return specErrf("campo %q: extract %q não pode ser auto; a coluna derivada existe para ter tipo", parent, name)
			}
			if _, err := CompileFormat(e.Type, e.Format); err != nil {
				return specErrf("campo %q: extract %q: %s", parent, name, withoutSpecPrefix(err))
			}
			if _, clash := fields[name]; clash {
				return specErrf("extract %q colide com o campo de topo de mesmo nome; escolha outro `as`", name)
			}
			if owner, clash := aliases[name]; clash {
				return specErrf("extract %q colide com o alias do campo %q; escolha outro `as`", name, owner)
			}
			if prev, dup := seen[name]; dup {
				return specErrf("extract %q declarado duas vezes (campos %q e %q)", name, prev, parent)
			}
			seen[name] = parent
			// A mesma validateIndex do campo de topo, e não uma segunda cópia: as
			// duas fazem a MESMA pergunta (grafia aceita? tipo indexável?), e
			// divergiriam no dia em que uma das duas mudasse sozinha.
			if err := validateIndex(name, e.Index, e.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

// derivedColumnName resolve o `as`, com o default e as recusas de forma.
func derivedColumnName(parent string, i int, e ExtractSpec) (string, error) {
	if e.Path == "" {
		return "", specErrf("campo %q: extract[%d] sem path", parent, i)
	}
	name := e.As
	if name == "" {
		// O default só serve ao caminho de um nível: derivar de "a.b" daria
		// um nome com ponto, e nome de coluna com ponto colidiria com a
		// sintaxe de caminho da projeção. Nesse caso o `as` é obrigatório.
		if strings.Contains(e.Path, ".") {
			return "", specErrf("campo %q: extract com path %q precisa de `as` — o nome não pode ser derivado de um caminho com ponto", parent, e.Path)
		}
		name = e.Path
	}
	if strings.Contains(name, ".") {
		return "", specErrf("campo %q: extract `as` %q não pode conter ponto; ele separaria o pai da coluna na projeção", parent, name)
	}
	// A regra é a de NOME DE CAMPO, e não a de alias: nome de campo no NishiDB
	// nunca foi restrito a minúsculas — a fixture da spec tem
	// `Acct-Status-Type` —, e chave de documento json é tipicamente camelCase.
	// Aplicar aqui o isIdentName do alias tornaria o `as` obrigatório em
	// praticamente toda extração real, sem nada ganhar.
	if reason, reserved := systemNames[name]; reserved {
		return "", specErrf("campo %q: extract `as` %q é reservado: %s", parent, name, reason)
	}
	return name, nil
}

// IndexedFieldNames devolve, em ordem determinística, os campos que o Store
// precisa abrir com índice — os de topo E as colunas derivadas.
//
// É o ÚNICO lugar que decide isso. O laço vivia copiado em quatro sítios —
// servidor, duas rotas do CLI e o benchmark — e nenhuma cópia conhecia a
// coluna derivada. O efeito não seria degradação: o servidor criaria o dataset
// COM o índice, e o `nishidb query` seguinte recusaria abrir, porque o Store
// não deixa passar um manifest que declara índice que a abertura não conhece.
//
// A ORDEM é contrato, não estética: ela decide os FieldID de um dataset novo,
// e o produto promete mapeamento determinístico para o mesmo spec.
//
// É UMA função, e não mais o par CamposComBitmap/CamposComHash: as duas
// devolviam o MESMO conjunto desde a D5, e sobreviviam separadas apenas porque
// storage.Options ainda tinha um par de campos por motor. Com o par unificado
// não resta chamador para a segunda — e duas funções idênticas são a primeira
// cópia a divergir.
func IndexedFieldNames(s *DatasetSpec) []string {
	var out []string
	for name, f := range s.Schema.Fields {
		if f.Index == "true" {
			out = append(out, name)
		}
		for i, e := range f.Extract {
			if e.Index != "true" {
				continue
			}
			derived, err := derivedColumnName(name, i, e)
			if err != nil {
				continue // a validação da spec já recusou isto
			}
			out = append(out, derived)
		}
	}
	sort.Strings(out)
	return out
}

// DerivedColumnNames devolve, em ordem determinística, o nome de todas as
// colunas derivadas declaradas.
func (s *DatasetSpec) DerivedColumnNames() []string {
	parents := make([]string, 0, len(s.Schema.Fields))
	for name := range s.Schema.Fields {
		if len(s.Schema.Fields[name].Extract) > 0 {
			parents = append(parents, name)
		}
	}
	sort.Strings(parents)
	var out []string
	for _, parent := range parents {
		for i, e := range s.Schema.Fields[parent].Extract {
			name, err := derivedColumnName(parent, i, e)
			if err != nil {
				continue // a validação da spec já recusou isto
			}
			out = append(out, name)
		}
	}
	return out
}

// DerivedColumnName expõe a resolução do `as` para o compilador.
func DerivedColumnName(parent string, i int, e ExtractSpec) (string, error) {
	return derivedColumnName(parent, i, e)
}

func validate(raw *rawSpec, defaults Defaults) (*DatasetSpec, error) {
	// Default inválido é erro do CHAMADOR, e sai aqui em vez de virar um
	// padrão implícito: cair no 100000 quando o global veio zerado esconderia
	// exatamente o defeito que se está corrigindo.
	if defaults.MaxRows <= 0 {
		return nil, specErrf("query.defaultMaxRows global deve ser > 0, é %d", defaults.MaxRows)
	}
	if defaults.Timeout <= 0 {
		return nil, specErrf("query.defaultTimeout global deve ser > 0, é %s", defaults.Timeout)
	}
	// Zero é AUSENTE aqui (o dataset herda o próprio Timeout), e por isso a
	// faixa recusada é só a negativa — que nenhum parser produz, mas que um
	// chamador que monte Defaults à mão produziria, e daria um teto no passado.
	if defaults.MaxTimeout < 0 {
		return nil, specErrf("query.defaultMaxTimeout global não pode ser negativo, é %s", defaults.MaxTimeout)
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
	// O prefixo "_" em nome de DATASET fica reservado aos espaços de nome do
	// produto — `_meta` foi o primeiro, `_counters` o segundo. A recusa vale a pena AGORA porque o custo
	// é assimétrico: uma linha hoje, e "recrie o dataset" depois, para quem já
	// tiver subido um nome assim.
	//
	// Ela mora aqui, e não dentro de isIdentName, porque aquela função também
	// valida ALIAS logo abaixo — e para CAMPO o prefixo continua legítimo, pela
	// razão que o comentário de systemNames registra: `_SEQ` é nome de
	// sequence em uso, e reservá-lo recusaria datasets que hoje sobem.
	if strings.HasPrefix(raw.Name, "_") {
		return nil, specErrf(
			"name %q: o prefixo \"_\" é reservado aos espaços de nome do produto (_meta, %s); escolha outro nome",
			raw.Name, CountersNamespace)
	}
	// ReservedAlarmsTable colide pelo mesmo motivo do prefixo "_": a view de
	// alarmes publicada sob CountersNamespace usa este nome fixo em todo
	// dataset, e um dataset chamado `alarms` disputaria a mesma entrada no
	// catálogo. A spec SINTÉTICA da própria view escapa por SchemaName ==
	// SyntheticSchemaName — mesmo padrão da isenção do prefixo "_" em
	// schemaName, abaixo —, porque ela também se chama `alarms`, de propósito.
	// A isenção só é segura porque o Load recusa `_virtual` vindo de arquivo:
	// nenhum dataset de usuário chega aqui com esse schemaName.
	if raw.Name == ReservedAlarmsTable && raw.SchemaName != SyntheticSchemaName {
		return nil, specErrf(
			"name %q is reserved for the alarms view published under %s.%s; choose another name",
			raw.Name, CountersNamespace, ReservedAlarmsTable)
	}
	s.Name = raw.Name

	// O schema é o container de tudo que o dataset publica, e ele é
	// OBRIGATÓRIO. Um default derivado do nome entregaria `servicos.servicos`,
	// e a ausência entregaria a schema nula que o DBeaver mostra hoje — as
	// duas foram consideradas e recusadas, porque a chave existe exatamente
	// para desfazer a ambiguidade entre datasets de um mesmo servidor.
	//
	// Que isto derrube todo YAML existente é sabido e aceito: não há release
	// publicada, e config recusada na subida custa uma linha e um restart,
	// enquanto config adivinhada custa uma árvore errada que ninguém consegue
	// explicar. A mensagem nomeia a chave E mostra um exemplo — ensinar a
	// correção é o que separa uma recusa útil de um "faltando".
	if raw.SchemaName == "" {
		return nil, specErrf(
			"schemaName obrigatório: é o schema em que as tabelas deste dataset aparecem para o cliente SQL (exemplo: `schemaName: telecom`)")
	}
	if !isIdentName(raw.SchemaName) {
		return nil, specErrf("schemaName %q inválido (esperado [a-z0-9_]+)", raw.SchemaName)
	}
	// Mesma reserva do nome de DATASET, e pela mesma razão: as tabelas de meta
	// moram num schema PRÓPRIO com o "_" na frente — o `_meta`, ao lado do
	// schema do dataset e não dentro dele —, e um schema de usuário que
	// pudesse se chamar `_meta` disputaria o nome com o espaço de nome do
	// produto.
	//
	// A ÚNICA exceção é SyntheticSchemaName, e ela é o motivo de o sentinela
	// das tabelas virtuais morar dentro da faixa reservada: as quatro specs
	// sintéticas atravessam este mesmo validate, e um sentinela fora da faixa
	// seria um nome que qualquer dataset de usuário também poderia declarar.
	// Quem fecha o outro lado é Load: arquivo de dataset não passa por aqui
	// com este valor.
	if strings.HasPrefix(raw.SchemaName, "_") && raw.SchemaName != SyntheticSchemaName {
		return nil, specErrf(
			"schemaName %q: o prefixo \"_\" é reservado aos espaços de nome do produto (_meta, %s); escolha outro nome",
			raw.SchemaName, CountersNamespace)
	}
	s.SchemaName = raw.SchemaName

	var err error
	if s.Source, err = validateSource(&raw.Source); err != nil {
		return nil, err
	}
	if s.Errors, err = validateErrors(&raw.Errors); err != nil {
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
	if rawText := raw.Materialization.TombstoneRetention; rawText != "" {
		d, err := units.DurationWithDays(rawText, 0)
		if err != nil {
			return nil, specErrf("materialization.tombstoneRetention: %s", err)
		}
		s.Materialization.TombstoneRetention = d
	}

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

	if rules, err := rulesFromNode(&raw.Materialization.TTL); err != nil {
		return nil, err
	} else if rules != nil {
		validated, err := validateTTLRules(rules)
		if err != nil {
			return nil, err
		}
		s.Materialization.TTLRules = validated
		s.Materialization.FallbackDuration = fallbackDuration(validated)
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
		if reason, reserved := systemNames[name]; reserved {
			return nil, specErrf("campo %q é reservado: %s", name, reason)
		}
		// A coluna congelada NÃO declara tipo: ele é herdado da origem na
		// compilação, e o isValidTypeName recusa vazio. A validação própria
		// dela — inclusive a recusa de `type` junto — mora em
		// validateFrozen, que precisa do mapa inteiro na mão.
		if f.FrozenFrom == "" && !isValidTypeName(f.Type) {
			return nil, specErrf("campo %q: tipo %q inválido", name, f.Type)
		}
		// O format é compilado AQUI, no load, e o layout resultante é
		// descartado: o que importa neste ponto é recusar o padrão inválido
		// antes de o dataset subir. O Compile o compila de novo para guardar.
		//
		// A checagem cobre os dois sentidos, e o segundo é o que não erra
		// alto sozinho: FieldSpec é decodificado sem KnownFields, então um
		// `format:` sob um campo que não o usa decodifica com sucesso e não
		// faz nada — configuração sem leitor, que é o defeito que o débito 37
		// registra.
		//
		// A congelada fica de fora: ela não declara tipo, então não há o que
		// compilar, e a recusa de `format` sobre ela mora em validateFrozen
		// — com a mensagem que nomeia frozenFrom, que é a que o operador
		// precisa ler.
		if f.FrozenFrom == "" {
			if _, err := CompileFormat(f.Type, f.Format); err != nil {
				return nil, specErrf("campo %q: %s", name, withoutSpecPrefix(err))
			}
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
		// A congelada é indexável como qualquer coluna, mas o tipo contra o
		// qual a indexabilidade se decide é o da ORIGEM — que este laço não
		// tem resolvido. A checagem dela mora em validateFrozen.
		if f.FrozenFrom == "" {
			if err := validateIndex(name, f.Index, f.Type); err != nil {
				return nil, err
			}
		}
		if f.UnknownFields != "" {
			if f.Type != "json" {
				return nil, specErrf("campo %q: unknownFields só vale sobre type: json, e ele é %q — nos outros tipos não há documento a percorrer", name, f.Type)
			}
			switch f.UnknownFields {
			case "include", "ignore":
			case "reject":
				return nil, specErrf("campo %q: unknownFields reject não existe dentro de um documento; "+
					"recusar o evento por uma chave interna desconhecida o derrubaria inteiro, que é o oposto do que o campo json serve para fazer", name)
			default:
				return nil, specErrf("campo %q: unknownFields %q inválido (include|ignore)", name, f.UnknownFields)
			}
		}
		if f.Index != "" && name == key.Field {
			return nil, specErrf("campo %q é a key do dataset e já é indexado pela primary; remova index", name)
		}
	}
	s.Schema.Fields = fields

	// As congeladas são validadas com o mapa inteiro na mão, e depois de key e
	// mode estarem resolvidos: as três regras que dependem de outro campo —
	// cadeia, autorreferência e origem inexistente — não são decidíveis dentro
	// do laço por campo.
	if err := validateFrozen(fields, key.Field, mode); err != nil {
		return nil, err
	}
	declaredSeq := ""
	if seq := s.Materialization.Sequence; seq != nil {
		declaredSeq = seq.Field
	}
	if err := validateMergePolicies(fields, key.Field, declaredSeq, mode); err != nil {
		return nil, err
	}

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

	// maxCols/onMaxCols: quem declara vence, quem omite herda o teto físico.
	//
	// O default é o teto do dicionário, e não um número redondo menor, porque
	// um teto menor aplicaria política a quem nunca a escolheu. Ele funciona
	// como backstop contra forma patológica de dado (chave derivada de
	// identificador); quem controla o caso real é a declaração.
	maxCols := MaxColsPhysicalCeiling
	if raw.Schema.MaxCols != nil {
		maxCols = *raw.Schema.MaxCols
	}
	if maxCols <= 0 {
		return nil, specErrf("schema.maxCols deve ser > 0, é %d", maxCols)
	}
	if maxCols > MaxColsPhysicalCeiling {
		return nil, specErrf(
			"schema.maxCols %d acima do teto físico do dicionário (%d, porque o FieldID é uint16); "+
				"declarar acima disso prometeria o que o fielddict recusa primeiro",
			maxCols, MaxColsPhysicalCeiling)
	}
	s.Schema.MaxCols = maxCols

	onMaxCols := raw.Schema.OnMaxCols
	if onMaxCols == "" {
		onMaxCols = OnMaxColsIgnore
	}
	if onMaxCols != OnMaxColsIgnore && onMaxCols != OnMaxColsStop {
		return nil, specErrf("schema.onMaxCols %q inválido (%s|%s)", onMaxCols, OnMaxColsIgnore, OnMaxColsStop)
	}
	s.Schema.OnMaxCols = onMaxCols

	// maxNestedCols tem a forma do maxRows: teto no dataset, e ao estourar
	// RECUSA — nunca trunca. Truncar entregaria um resultado com colunas
	// faltando e cara de completo, que é o defeito que o produto fecha em
	// toda parte. O erro é acionável: pedem-se as colunas pelo nome.
	maxNested := DefaultMaxNestedCols
	if raw.Schema.MaxNestedCols != nil {
		maxNested = *raw.Schema.MaxNestedCols
	}
	if maxNested <= 0 {
		return nil, specErrf("schema.maxNestedCols deve ser > 0, é %d", maxNested)
	}
	s.Schema.MaxNestedCols = maxNested

	if err := validateExtracts(s, fields, aliases, include, includeAll); err != nil {
		return nil, err
	}

	if err := refuseWindowOutsideHistory(&raw.Index); err != nil {
		return nil, err
	}
	// O histórico vem por último entre as validações de schema porque toda a
	// checagem de `history.fields` é cruzada: a coluna precisa existir, o tipo
	// dela decide a indexabilidade, e a congelada precisa ser reconhecida.
	if s.History, err = validateHistory(&raw.History, s.Name, fields); err != nil {
		return nil, err
	}

	// Os contadores vêm DEPOIS de key e sequence estarem resolvidos — metade
	// das recusas do groupBy é sobre o PAPEL da coluna, não sobre ela — e
	// DEPOIS do histórico, porque o nome da tabela irmã é uma das colisões que
	// eles têm de recusar: ela herda o schema do base e o nome dela sai de um
	// sufixo configurável, então `name: events_history` num dataset `events`
	// com bloco `history` põe duas tabelas diferentes no mesmo (schema, nome).
	historyTable := ""
	if s.History != nil {
		historyTable = s.History.TableName(s.Name)
	}
	if raws, err := countersFromNode(&raw.Counters); err != nil {
		return nil, err
	} else if raws != nil {
		validated, err := validateCounters(raws, fields, key.Field, declaredSeq, s.Name, historyTable)
		if err != nil {
			return nil, err
		}
		s.Counters = validated
	}

	// query: o dataset que declara a chave vence; quem omite herda o global.
	maxRows := defaults.MaxRows
	if raw.Query.MaxRows != nil {
		maxRows = *raw.Query.MaxRows
	}
	if maxRows <= 0 {
		return nil, specErrf("query.maxRows deve ser > 0, é %d", maxRows)
	}
	s.Query.MaxRows = maxRows
	timeout := defaults.Timeout
	if raw.Query.Timeout != "" {
		d, err := time.ParseDuration(raw.Query.Timeout)
		if err != nil || d <= 0 {
			return nil, specErrf("query.timeout %q inválido", raw.Query.Timeout)
		}
		timeout = d
	}
	s.Query.Timeout = timeout

	// maxTimeout é o TETO; timeout é o DEFAULT. O default do teto é o próprio
	// timeout resolvido — nesta ordem, e não a partir de padroes.Timeout —
	// porque é isso que faz o dataset que declara `query.timeout: 5m` continuar
	// com teto de 5m em vez de ganhar o global de 30s e passar a recusar a
	// consulta que ele atende hoje.
	maxTimeout := defaults.MaxTimeout
	if raw.Query.MaxTimeout != "" {
		d, err := time.ParseDuration(raw.Query.MaxTimeout)
		if err != nil || d <= 0 {
			return nil, specErrf("query.maxTimeout %q inválido", raw.Query.MaxTimeout)
		}
		maxTimeout = d
	}
	if maxTimeout == 0 {
		maxTimeout = timeout
	}
	if maxTimeout < timeout {
		// As DUAS chaves na mensagem, com os dois valores: sem elas o operador
		// lê "teto menor que o default" e não sabe qual dos dois números veio
		// do YAML do dataset e qual veio do default global — e o par pode ter
		// se formado de quatro maneiras diferentes.
		return nil, specErrf(
			"query.maxTimeout (%s) é menor que query.timeout (%s): o teto teria de recusar "+
				"o prazo que o próprio dataset usa por padrão; suba query.maxTimeout ou baixe query.timeout",
			maxTimeout, timeout)
	}
	s.Query.MaxTimeout = maxTimeout

	// replication is optional and disabled by default. The feed's limits are
	// validated here so a malformed follower setup cannot silently become an
	// unbounded master buffer.
	r := ReplicationSpec{FlushInterval: 5 * time.Second, MaxMessages: 10000, MaxBatchBytes: 8 << 20, MaxBufferedBytes: 64 << 20}
	if raw.Replication.Enabled != nil {
		r.Enabled = *raw.Replication.Enabled
	}
	if raw.Replication.FlushInterval != "" {
		d, e := time.ParseDuration(raw.Replication.FlushInterval)
		if e != nil || d < 0 || d > time.Minute || d%time.Second != 0 {
			return nil, specErrf("replication.flushInterval %q inválido (use 0s a 60s)", raw.Replication.FlushInterval)
		}
		r.FlushInterval = d
	}
	var e error
	if r.MaxMessages, e = units.PositiveInt(raw.Replication.MaxMessages, r.MaxMessages); e != nil {
		return nil, specErrf("replication.maxMessages: %v", e)
	}
	if raw.Replication.MaxBatchBytes != "" {
		if r.MaxBatchBytes, e = units.Size(raw.Replication.MaxBatchBytes, r.MaxBatchBytes); e != nil {
			return nil, specErrf("replication.maxBatchBytes: %v", e)
		}
	}
	if raw.Replication.MaxBufferedBytes != "" {
		if r.MaxBufferedBytes, e = units.Size(raw.Replication.MaxBufferedBytes, r.MaxBufferedBytes); e != nil {
			return nil, specErrf("replication.maxBufferedBytes: %v", e)
		}
	}
	if r.MaxBufferedBytes < r.MaxBatchBytes {
		return nil, specErrf("replication.maxBufferedBytes (%d) é menor que maxBatchBytes (%d)", r.MaxBufferedBytes, r.MaxBatchBytes)
	}
	if raw.Replication.Role != "" {
		r.Role = raw.Replication.Role
	}
	if r.Role == "" {
		r.Role = "master"
	}
	if r.Role != "master" && r.Role != "follower" {
		return nil, specErrf("replication.role %q inválido (use master ou follower)", r.Role)
	}
	if r.Role == "follower" && raw.Replication.MasterURL == "" {
		return nil, specErrf("replication.masterURL é obrigatório para role follower")
	}
	r.MasterURL = raw.Replication.MasterURL
	if (raw.Replication.UsernameEnv == "") != (raw.Replication.PasswordEnv == "") {
		return nil, specErrf("replication.usernameEnv e replication.passwordEnv devem ser informados juntos")
	}
	if raw.Replication.UsernameEnv != "" {
		if err := validateEnvName("replication.usernameEnv", raw.Replication.UsernameEnv); err != nil {
			return nil, err
		}
		if err := validateEnvName("replication.passwordEnv", raw.Replication.PasswordEnv); err != nil {
			return nil, err
		}
	}
	r.UsernameEnv, r.PasswordEnv = raw.Replication.UsernameEnv, raw.Replication.PasswordEnv
	if !absent(&raw.Replication.Target) {
		return nil, specErrf("replication.target is not a key; use replication.targets (a list of Kafka target names)")
	}
	if r.Targets, e = validateReplicationTargets(&raw.Replication.Targets, r); e != nil {
		return nil, e
	}
	s.Replication = r

	return s, nil
}

// validateReplicationTargets decodifica `replication.targets` e o confere
// contra o restante do bloco já validado em r (enabled, role e os dois tetos
// do buffer), devolvendo a lista a publicar — nil quando a chave não veio.
// Só lista de strings explícitas é aceita: nulo, escalar e lista vazia são
// recusados, como em `emit.target`, em vez de lidos como "sem destino".
//
// O nome segue a MESMA regra dos destinos nomeados de `emit.target` e de
// `kafkaTargets:` (ValidDeliveryName), e `file` é recusado porque a
// replicação não tem saída em arquivo: aceitá-lo seria publicar em lugar
// nenhum sem avisar. O piso `maxBufferedBytes >= 2*maxBatchBytes` existe
// porque um lote arrendado — pelo publicador Kafka enquanto o copia para o
// próprio buffer, ou por um seguidor enquanto o envia — continua contando no
// buffer do feed até ser solto, mesmo depois de despejado
// (internal/replication, makeRoomLocked). Sem a folga de um lote inteiro além
// do arrendado, a próxima escrita não acharia espaço e invalidaria o
// intervalo retido.
func validateReplicationTargets(n *yaml.Node, r ReplicationSpec) ([]string, error) {
	if n.Kind == 0 {
		return nil, nil
	}
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n.Kind != yaml.SequenceNode {
		return nil, specErrf("replication.targets must be a list of Kafka target names")
	}
	targets := make([]string, 0, len(n.Content))
	for i, item := range n.Content {
		for item.Kind == yaml.AliasNode {
			item = item.Alias
		}
		if item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
			return nil, specErrf("replication.targets[%d] must be a Kafka target name string, got %s", i, item.ShortTag())
		}
		targets = append(targets, item.Value)
	}
	if len(targets) == 0 {
		return nil, specErrf("replication.targets must not be empty; omit the key to publish to no Kafka target")
	}
	if !r.Enabled {
		return nil, specErrf("replication.targets requires replication.enabled: true")
	}
	if r.Role != "master" {
		return nil, specErrf("replication.targets is only valid for role master (role is %q)", r.Role)
	}
	seen := make(map[string]bool, len(targets))
	for _, name := range targets {
		if name == "file" {
			return nil, specErrf("replication.targets: %q is not a Kafka target (replication has no file output)", name)
		}
		if !ValidDeliveryName(name) {
			return nil, specErrf("replication.targets: invalid Kafka target name %q (use a-z, 0-9 and _, 1 to 64 characters)", name)
		}
		if seen[name] {
			return nil, specErrf("replication.targets: duplicate Kafka target %q", name)
		}
		seen[name] = true
	}
	// Dividir em vez de dobrar: 2*MaxBatchBytes transbordaria int64 num
	// maxBatchBytes gigante e aprovaria o par por engano.
	if r.MaxBufferedBytes/2 < r.MaxBatchBytes {
		return nil, specErrf("replication.maxBufferedBytes (%d) must be at least twice replication.maxBatchBytes (%d) "+
			"when replication.targets is set: a leased batch keeps counting against the buffer until it is released",
			r.MaxBufferedBytes, r.MaxBatchBytes)
	}
	return targets, nil
}
