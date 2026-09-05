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

// DatasetSpec é a spec validada, com defaults aplicados.
type DatasetSpec struct {
	Version         int
	Name            string
	Source          SourceSpec
	Errors          ErrorsSpec
	Materialization MaterializationSpec
	Schema          SchemaSpec
	Query           QuerySpec
	// Historico é o bloco `history` validado, ou nil no dataset sem tabela
	// irmã. Ver DerivaHistorico para a spec que ele gera.
	Historico *HistoricoSpec
	// EhHistorico marca a spec DERIVADA — a da própria tabela irmã. Ela não
	// vem de arquivo nenhum: nasce de DerivaHistorico e existe só em memória,
	// e é por isso que ListaDatasets nunca a enxerga.
	EhHistorico bool
	// Contadores é o bloco `counters` validado; nil no dataset sem contador.
	Contadores []ContadorSpec
}

type MaterializationSpec struct {
	Key      KeySpec
	Mode     string // merge | replace
	Sequence *SequenceSpec
	// RegrasTTL são as regras de expiração VALIDADAS, na ordem do arquivo —
	// que é semântica: a primeira que casa vence. Vazia significa dataset sem
	// expiração.
	RegrasTTL []RegraTTL
	// DuracaoDeFundo é a duração da regra sem `when`, ou zero quando não há.
	// Sai pré-computada porque quem a consome — os montadores de Options do
	// storage — não deve reimplementar a busca pela última regra.
	DuracaoDeFundo time.Duration
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

// ResolucaoDaExpiracao é o passo do agendador (§57). Duração menor que isto é
// recusada em vez de arredondada: o operador leria "30s" e teria até 90s, e
// uma chave que mente é pior que uma chave recusada.
const ResolucaoDaExpiracao = time.Minute

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

	// MaxNestedColsPadrao é o teto default de colunas derivadas por consulta.
	// O número é folgado de propósito: quem controla o caso real é a lista de
	// extrações que o operador escreveu, e este é backstop contra uma
	// expansão `pai.*` sobre um pai com centenas de chaves declaradas.
	MaxNestedColsPadrao = 256

	// MaxColsTetoFisico é o teto que o dicionário impõe por construção: o
	// types.FieldID é uint16. Declarar acima disto seria uma promessa que o
	// fielddict recusa primeiro, então a spec recusa antes.
	MaxColsTetoFisico = 65535
)

// As tags de YAML são EXPLÍCITAS mesmo quando redundantes. O default do
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
	// sobrescreve (o default), MergeSoma acumula. Ver o godoc da constante
	// para o que a soma custa em idempotência.
	Merge string `yaml:"merge"`

	// FrozenFrom nomeia a ORIGEM de uma coluna congelada: outra coluna
	// declarada, ou OrigemRelogio. A coluna recebe o primeiro valor que a
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
	As     string `yaml:"as"`
	Type   string `yaml:"type"`
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
	Version *int   `yaml:"version"`
	Name    string `yaml:"name"`
	// Source e Errors ficam como nó cru porque a recusa de chave desconhecida
	// nesses dois blocos é feita à mão, sobre as chaves do documento — ver
	// bloco(), em source.go, para o motivo de yaml.KnownFields não servir.
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
	Counters yaml.Node `yaml:"counters"`
}

type rawMat struct {
	Key      rawKey        `yaml:"key"`
	Mode     string        `yaml:"mode"`
	Sequence *SequenceSpec `yaml:"sequence"`
	// TTL é yaml.Node, e não []RegraTTLSpec direto, para que a forma ANTIGA —
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

// DefaultsPadrao são os valores da spec §78 para quem carrega uma spec sem
// nishidb.yaml nenhum por perto — testes, benchmarks e ferramentas.
//
// MaxTimeout fica ZERO de propósito: escrever 30s aqui seria um segundo lugar
// de onde o teto poderia sair, e ele divergiria do Timeout no dia em que um
// dataset declarasse `query.timeout` maior.
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

// semPrefixoDeSpec desembrulha a mensagem de um erro já marcado com
// ErrInvalidSpec, para que aninhá-lo em outro specErrf não produza
// "spec inválida: spec inválida: ..." na cara do operador.
func semPrefixoDeSpec(err error) string {
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

// dominioDeFormato mapeia o tipo YAML para o domínio do compilador de padrão.
// Só estes dois tipos aceitam (e exigem) `format:`.
func dominioDeFormato(tipo string) (typeparse.Dominio, bool) {
	switch tipo {
	case "datetime":
		return typeparse.DominioDateTime, true
	case "time":
		return typeparse.DominioTime, true
	}
	return 0, false
}

// CompilaFormato resolve o `format:` de um campo para o layout de parsing.
// Dono único da regra: quem valida a spec e quem compila o schema chamam
// AQUI, nunca uma segunda lista de formatos — duas listas divergiriam e o
// dataset aceitaria no load o que recusa por evento na ingestão.
func CompilaFormato(tipo, format string) (*typeparse.Layout, error) {
	d, usa := dominioDeFormato(tipo)
	if !usa {
		if format != "" {
			return nil, specErrf("format só se aplica a datetime e time; o tipo %q não o usa", tipo)
		}
		return nil, nil
	}
	if format == "" {
		// Sem default de propósito: um format esquecido escolheria um padrão
		// que recusa 100% dos eventos, e sob log-and-skip isso é um dataset
		// que não ingere nada — só WARNs, sem nada apontando para a causa.
		return nil, specErrf("tipo %q exige format (tokens: %s)", tipo, typeparse.TokensSuportados())
	}
	l, err := typeparse.CompilaLayout(format, d)
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

// nomesDeSistema são os campos que o WRITER injeta e o operador não declara.
//
// Lista fechada, e não o prefixo "_" inteiro: campos com underscore são
// legítimos e já estão em uso — `_SEQ` é o nome de sequence da spec §12 e da
// fixture de tipos. Reservar o prefixo recusaria datasets que hoje sobem.
var nomesDeSistema = map[string]string{
	CampoInstanteDeEscrita: "é injetado pelo writer a cada escrita",
	CampoDePrazo:           "é calculado pelo writer a partir das regras de ttl",
}

// CampoInstanteDeEscrita registra QUANDO o writer gravou a entidade. É um
// fato, não uma política: guardar o instante da escrita (e não o prazo já
// somado) é o que faz uma mudança de expireAfterWrite valer imediatamente
// para as linhas existentes, em vez de exigir reescrever o dataset.
const CampoInstanteDeEscrita = "_writtenAt"

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
// indexavel une logo abaixo: `auto` não entra em nenhum dos dois porque o tipo
// só é conhecido em runtime.
func bitmapIndexable(t string) bool {
	return t == "string" || t == "int64" || t == "boolean"
}

func hashIndexable(t string) bool {
	return t == "string" || t == "int64" || t == "ipv4" || t == "ipv6"
}

// indexavel é o conjunto que `index: true` aceita: a UNIÃO dos dois motores
// antigos, e não a interseção. Com um motor só, a distinção de cardinalidade
// que os separava morreu — mas nenhuma coluna pode PERDER a indexabilidade que
// já tinha só porque as duas grafias viraram uma: um campo `ipv4` que só
// bitmapIndexable recusava (e só hashIndexable aceitava) continua indexável
// com `index: true`, exatamente como continuava indexável com `index: hash`.
func indexavel(t string) bool {
	return bitmapIndexable(t) || hashIndexable(t)
}

// validaExtracoes confere o bloco `extract` de todos os campos.
//
// Roda numa passada PRÓPRIA, depois de include/exclude estarem resolvidos, e
// percorre os campos em ordem alfabética. As duas coisas importam: a primeira
// porque a validade de uma extração depende de o PAI estar visível, e a
// segunda porque o laço de schema.fields itera um mapa — sem ordenar, a
// mensagem de erro de um YAML com dois defeitos mudaria entre execuções.
func validaExtracoes(s *DatasetSpec, fields map[string]FieldSpec, aliases map[string]string, include []string, includeAll bool) error {
	nomesDePais := make([]string, 0, len(fields))
	for nome := range fields {
		if len(fields[nome].Extract) > 0 {
			nomesDePais = append(nomesDePais, nome)
		}
	}
	sort.Strings(nomesDePais)

	noInclude := make(map[string]struct{}, len(include))
	for _, n := range include {
		noInclude[n] = struct{}{}
	}
	excluido := make(map[string]struct{}, len(s.Schema.Exclude))
	for _, n := range s.Schema.Exclude {
		excluido[n] = struct{}{}
	}

	vistos := map[string]string{} // as → pai que o declarou
	for _, pai := range nomesDePais {
		f := fields[pai]
		if f.Type != "json" {
			return specErrf("campo %q: extract só vale sobre type: json, e ele é %q — nos outros tipos não há documento a percorrer", pai, f.Type)
		}
		if _, fora := excluido[pai]; fora {
			return specErrf("campo %q está no exclude e declara extract; o campo excluído nem é materializado, então a extração nunca rodaria", pai)
		}
		if !includeAll {
			if _, dentro := noInclude[pai]; !dentro {
				return specErrf("campo %q declara extract e não está no include; a extração nunca rodaria", pai)
			}
		}
		for i, e := range f.Extract {
			nome, err := nomeDaExtracao(pai, i, e)
			if err != nil {
				return err
			}
			if !isValidTypeName(e.Type) {
				return specErrf("campo %q: extract %q: tipo %q inválido", pai, nome, e.Type)
			}
			if e.Type == "auto" {
				return specErrf("campo %q: extract %q não pode ser auto; a coluna derivada existe para ter tipo", pai, nome)
			}
			if _, err := CompilaFormato(e.Type, e.Format); err != nil {
				return specErrf("campo %q: extract %q: %s", pai, nome, semPrefixoDeSpec(err))
			}
			if _, clash := fields[nome]; clash {
				return specErrf("extract %q colide com o campo de topo de mesmo nome; escolha outro `as`", nome)
			}
			if dono, clash := aliases[nome]; clash {
				return specErrf("extract %q colide com o alias do campo %q; escolha outro `as`", nome, dono)
			}
			if antes, dup := vistos[nome]; dup {
				return specErrf("extract %q declarado duas vezes (campos %q e %q)", nome, antes, pai)
			}
			vistos[nome] = pai
			// A mesma validaIndice do campo de topo, e não uma segunda cópia: as
			// duas fazem a MESMA pergunta (grafia aceita? tipo indexável?), e
			// divergiriam no dia em que uma das duas mudasse sozinha.
			if err := validaIndice(nome, e.Index, e.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

// nomeDaExtracao resolve o `as`, com o default e as recusas de forma.
func nomeDaExtracao(pai string, i int, e ExtractSpec) (string, error) {
	if e.Path == "" {
		return "", specErrf("campo %q: extract[%d] sem path", pai, i)
	}
	nome := e.As
	if nome == "" {
		// O default só serve ao caminho de um nível: derivar de "a.b" daria
		// um nome com ponto, e nome de coluna com ponto colidiria com a
		// sintaxe de caminho da projeção. Nesse caso o `as` é obrigatório.
		if strings.Contains(e.Path, ".") {
			return "", specErrf("campo %q: extract com path %q precisa de `as` — o nome não pode ser derivado de um caminho com ponto", pai, e.Path)
		}
		nome = e.Path
	}
	if strings.Contains(nome, ".") {
		return "", specErrf("campo %q: extract `as` %q não pode conter ponto; ele separaria o pai da coluna na projeção", pai, nome)
	}
	// A regra é a de NOME DE CAMPO, e não a de alias: nome de campo no NishiDB
	// nunca foi restrito a minúsculas — a fixture da spec tem
	// `Acct-Status-Type` —, e chave de documento json é tipicamente camelCase.
	// Aplicar aqui o isIdentName do alias tornaria o `as` obrigatório em
	// praticamente toda extração real, sem nada ganhar.
	if motivo, reservado := nomesDeSistema[nome]; reservado {
		return "", specErrf("campo %q: extract `as` %q é reservado: %s", pai, nome, motivo)
	}
	return nome, nil
}

// CamposComIndice devolve, em ordem determinística, os campos que o Store
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
func CamposComIndice(s *DatasetSpec) []string {
	var out []string
	for nome, f := range s.Schema.Fields {
		if f.Index == "true" {
			out = append(out, nome)
		}
		for i, e := range f.Extract {
			if e.Index != "true" {
				continue
			}
			derivada, err := nomeDaExtracao(nome, i, e)
			if err != nil {
				continue // a validação da spec já recusou isto
			}
			out = append(out, derivada)
		}
	}
	sort.Strings(out)
	return out
}

// NomesDasExtracoes devolve, em ordem determinística, o nome de todas as
// colunas derivadas declaradas.
func (s *DatasetSpec) NomesDasExtracoes() []string {
	pais := make([]string, 0, len(s.Schema.Fields))
	for nome := range s.Schema.Fields {
		if len(s.Schema.Fields[nome].Extract) > 0 {
			pais = append(pais, nome)
		}
	}
	sort.Strings(pais)
	var out []string
	for _, pai := range pais {
		for i, e := range s.Schema.Fields[pai].Extract {
			nome, err := nomeDaExtracao(pai, i, e)
			if err != nil {
				continue // a validação da spec já recusou isto
			}
			out = append(out, nome)
		}
	}
	return out
}

// NomeDaExtracao expõe a resolução do `as` para o compilador.
func NomeDaExtracao(pai string, i int, e ExtractSpec) (string, error) {
	return nomeDaExtracao(pai, i, e)
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
	// Zero é AUSENTE aqui (o dataset herda o próprio Timeout), e por isso a
	// faixa recusada é só a negativa — que nenhum parser produz, mas que um
	// chamador que monte Defaults à mão produziria, e daria um teto no passado.
	if padroes.MaxTimeout < 0 {
		return nil, specErrf("query.defaultMaxTimeout global não pode ser negativo, é %s", padroes.MaxTimeout)
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
	// razão que o comentário de nomesDeSistema registra: `_SEQ` é nome de
	// sequence em uso, e reservá-lo recusaria datasets que hoje sobem.
	if strings.HasPrefix(raw.Name, "_") {
		return nil, specErrf(
			"name %q: o prefixo \"_\" é reservado aos espaços de nome do produto (_meta, %s); escolha outro nome",
			raw.Name, NamespaceContadores)
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
	if bruto := raw.Materialization.TombstoneRetention; bruto != "" {
		d, err := units.DuracaoComDias(bruto, 0)
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

	if regras, err := regrasDoNo(&raw.Materialization.TTL); err != nil {
		return nil, err
	} else if regras != nil {
		validadas, err := validaRegrasTTL(regras)
		if err != nil {
			return nil, err
		}
		s.Materialization.RegrasTTL = validadas
		s.Materialization.DuracaoDeFundo = duracaoDeFundo(validadas)
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
		if motivo, reservado := nomesDeSistema[name]; reservado {
			return nil, specErrf("campo %q é reservado: %s", name, motivo)
		}
		// A coluna congelada NÃO declara tipo: ele é herdado da origem na
		// compilação, e o isValidTypeName recusa vazio. A validação própria
		// dela — inclusive a recusa de `type` junto — mora em
		// validaCongeladas, que precisa do mapa inteiro na mão.
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
		// compilar, e a recusa de `format` sobre ela mora em validaCongeladas
		// — com a mensagem que nomeia frozenFrom, que é a que o operador
		// precisa ler.
		if f.FrozenFrom == "" {
			if _, err := CompilaFormato(f.Type, f.Format); err != nil {
				return nil, specErrf("campo %q: %s", name, semPrefixoDeSpec(err))
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
		// tem resolvido. A checagem dela mora em validaCongeladas.
		if f.FrozenFrom == "" {
			if err := validaIndice(name, f.Index, f.Type); err != nil {
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
	if err := validaCongeladas(fields, key.Field, mode); err != nil {
		return nil, err
	}
	seqDeclarada := ""
	if seq := s.Materialization.Sequence; seq != nil {
		seqDeclarada = seq.Field
	}
	if err := validaPoliticasDeMerge(fields, key.Field, seqDeclarada, mode); err != nil {
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

	// Os contadores vêm DEPOIS de key e sequence estarem resolvidos: metade
	// das recusas do groupBy é sobre o PAPEL da coluna, não sobre ela.
	if raws, err := contadoresDoNo(&raw.Counters); err != nil {
		return nil, err
	} else if raws != nil {
		validados, err := validaContadores(raws, fields, key.Field, seqDeclarada)
		if err != nil {
			return nil, err
		}
		s.Contadores = validados
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
	maxCols := MaxColsTetoFisico
	if raw.Schema.MaxCols != nil {
		maxCols = *raw.Schema.MaxCols
	}
	if maxCols <= 0 {
		return nil, specErrf("schema.maxCols deve ser > 0, é %d", maxCols)
	}
	if maxCols > MaxColsTetoFisico {
		return nil, specErrf(
			"schema.maxCols %d acima do teto físico do dicionário (%d, porque o FieldID é uint16); "+
				"declarar acima disso prometeria o que o fielddict recusa primeiro",
			maxCols, MaxColsTetoFisico)
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
	maxNested := MaxNestedColsPadrao
	if raw.Schema.MaxNestedCols != nil {
		maxNested = *raw.Schema.MaxNestedCols
	}
	if maxNested <= 0 {
		return nil, specErrf("schema.maxNestedCols deve ser > 0, é %d", maxNested)
	}
	s.Schema.MaxNestedCols = maxNested

	if err := validaExtracoes(s, fields, aliases, include, includeAll); err != nil {
		return nil, err
	}

	if err := recusaJanelaForaDoHistorico(&raw.Index); err != nil {
		return nil, err
	}
	// O histórico vem por último entre as validações de schema porque toda a
	// checagem de `history.fields` é cruzada: a coluna precisa existir, o tipo
	// dela decide a indexabilidade, e a congelada precisa ser reconhecida.
	if s.Historico, err = validaHistorico(&raw.History, s.Name, fields); err != nil {
		return nil, err
	}

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

	// maxTimeout é o TETO; timeout é o DEFAULT. O default do teto é o próprio
	// timeout resolvido — nesta ordem, e não a partir de padroes.Timeout —
	// porque é isso que faz o dataset que declara `query.timeout: 5m` continuar
	// com teto de 5m em vez de ganhar o global de 30s e passar a recusar a
	// consulta que ele atende hoje.
	maxTimeout := padroes.MaxTimeout
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

	return s, nil
}
