// Package binder faz a análise semântica do SQL v1: resolve nomes contra o
// schema do dataset, coere literais e parâmetros para a representação binária
// da coluna e numera os slots que o executor usa para varrer o payload.
//
// Duas regras semânticas que o binder DOCUMENTA e o executor implementa:
//
//  1. campo ausente na row compara FALSE para qualquer operador, inclusive
//     `!=`. Não há lógica trivalente na v1; como também não existe NOT
//     genérico, colapsar "desconhecido" em falso é seguro sob AND e OR.
//  2. `IS NULL` é campo AUSENTE da row e `IS NOT NULL` é campo presente —
//     coerente com a decisão de que NULL é ausência nas três camadas (merge,
//     índice e query).
//
// O binder é puro: não toca dados, não toca o Store e não muta a AST nem o
// schema. Também não reescreve a árvore WHERE (nada de constant folding,
// reordenação ou dedup): o planner depende da FORMA da árvore para achatar
// cadeias de AND/OR, e mexer aqui o quebraria em silêncio.
package binder

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// BoundQuery é a query resolvida, pronta para o planner e o executor.
type BoundQuery struct {
	Dataset string           // == sc.Name
	Schema  *schema.Compiled // repassado ao executor (FieldByID no encoder)

	Star    bool          // a projeção veio de '*'
	Columns []BoundColumn // projeção resolvida, na ORDEM DE SAÍDA

	Where BoundExpr // nil quando não há WHERE

	HasLimit bool  // LIMIT explícito na query
	Limit    int64 // válido só com HasLimit; sempre >= 0
	MaxRows  int64 // teto implícito do dataset
	// EffectiveLimit é min(Limit, MaxRows) com HasLimit, e MaxRows sem.
	//
	// O trio (HasLimit, Limit, MaxRows) viaja separado de propósito: quando a
	// parada vier do teto IMPLÍCITO e ainda houver candidatos, o executor
	// precisa dizer que truncou — um corte silencioso por MaxRows faria o
	// cliente ler um resultado parcial como completo.
	EffectiveLimit int64
	// OrderBy é a ordenação pedida, resolvida contra o schema. nil quando
	// ausente — e ausente significa "sem operador de ordenação", não "ordenar
	// por rowId".
	OrderBy []BoundOrderKey
	// Offset é quantas linhas QUE PASSARAM no predicado são puladas antes de a
	// primeira sair. Não conta candidatos rejeitados, e não entra no
	// EffectiveLimit: o teto é de linhas ENTREGUES, e somar as puladas a ele
	// faria o `LIMIT 10 OFFSET 100` devolver menos de 10 linhas num dataset
	// que tem de sobra.
	Offset  int64
	Timeout time.Duration

	Slots Slots // campos a coletar por row (projeção ∪ WHERE)

	// arena guarda os bytes dos literais parseados (ips, prefixos). Vive
	// enquanto o BoundQuery viver; as janelas em Val.Bytes apontam para cá.
	arena []byte
}

// BoundColumn é uma coluna de saída.
type BoundColumn struct {
	Field *schema.CompiledField
	Name  string // nome de SAÍDA
	Slot  int    // índice em Slots.IDs
}

// BoundExpr é selada: só os três tipos abaixo a implementam, então o planner
// e o executor fazem type switch exaustivo.
type BoundExpr interface{ isBoundExpr() }

// BoundLogical preserva a forma da árvore do parser (left-leaning).
type BoundLogical struct {
	Op          parser.LogicalOp
	Left, Right BoundExpr
}

// BoundComparison já carrega o valor coerido e o modo de comparação: o
// executor não re-deriva tipo por row, e o planner enxerga `key = ?` como
// point lookup no plan-time.
type BoundComparison struct {
	Field *schema.CompiledField
	Slot  int
	Op    parser.CmpOp
	// Val é o literal ou parâmetro JÁ coerido; nunca zero.
	//
	// ATENÇÃO: Val.Type pode DIVERGIR de Field.EffectiveType(). Coluna int64
	// comparada a literal float promove os dois lados para double, e aí
	// `octets > 1.5` (coluna int64) e `ratio > 1.5` (coluna double) produzem um
	// Val idêntico byte a byte — mesmo Type e mesmo I64.
	Val types.Value
	// Kind é o ÚNICO discriminador de COMO comparar. Nunca faça switch em
	// Val.Type: nos dois casos acima ele vale double, e ler o payload de uma
	// coluna int64 como Float64bits transforma 1000 em 4,94e-321 — a query
	// devolve VAZIO sobre milhões de linhas, sem erro, sem log, sem métrica.
	// Nunca é CompareUnset.
	Kind     CompareKind
	IsParam  bool // diagnóstico apenas; o valor já está em Val
	ParamIdx int  // diagnóstico apenas; 0-based, válido só com IsParam
}

// BoundNullCheck vale para QUALQUER tipo, inclusive bytes, json e auto sem
// tipo fixado: presença não depende de tipo.
type BoundNullCheck struct {
	Field *schema.CompiledField
	Slot  int
	Not   bool
}

// BoundOrderKey é uma chave de ordenação resolvida.
//
// Want é o tipo FÍSICO esperado no payload, e não o do valor: é ele que separa
// "esta linha tem a coluna no tipo da coluna" de "tem outra coisa ali". Vale a
// mesma regra do predicado — valor de tipo divergente não é ordenável no
// domínio declarado e vai para o grupo dos ausentes.
type BoundOrderKey struct {
	Field      *schema.CompiledField
	Slot       int
	Desc       bool
	NullsFirst bool
	Kind       CompareKind
	Want       types.FieldType
}

// BoundLike é `coluna [NOT] LIKE <padrão>` com o padrão já compilado.
//
// Só coluna `string`. Os demais tipos são recusados no bind e não no executor,
// pela mesma razão que a tabela Comparable existe: erro de tipo tem de sair
// como mensagem acionável na posição da coluna, não como resultado vazio.
type BoundLike struct {
	Field *schema.CompiledField
	Slot  int
	Not   bool
	Pat   LikePattern
}

func (*BoundLogical) isBoundExpr()    {}
func (*BoundComparison) isBoundExpr() {}
func (*BoundNullCheck) isBoundExpr()  {}
func (*BoundLike) isBoundExpr()       {}

// Slots é o plano de coleta do executor: os FieldIDs de que a query precisa,
// em ordem CRESCENTE e sem duplicatas — a mesma ordem do payload, o que
// permite casar payload×slots por merge linear, sem map e sem busca, numa
// passada só por row.
//
// Os slots são numerados AQUI e são contrato: o executor os consome como
// estão e não os recalcula. Com dois lugares calculando a mesma ordenação, a
// primeira divergência apareceria como uma coluna lendo o valor do campo
// errado — falha silenciosa.
type Slots struct {
	IDs []types.FieldID
}

// Len é a largura do array de valores que o executor mantém por row.
func (s Slots) Len() int { return len(s.IDs) }

// Index devolve o slot de um FieldID (busca binária). O executor não precisa
// dele no laço por row — só o planner e os testes.
func (s Slots) Index(id types.FieldID) (int, bool) {
	i := sort.Search(len(s.IDs), func(i int) bool { return s.IDs[i] >= id })
	if i < len(s.IDs) && s.IDs[i] == id {
		return i, true
	}
	return 0, false
}

// Bind faz a análise semântica de um SELECT já parseado contra o schema
// compilado do dataset e RESOLVE os parâmetros posicionais.
//
// params é indexado por Operand.ParamIdx (0-based); len(params) tem de ser
// igual a stmt.ParamCount. Nas MENSAGENS os parâmetros são 1-based, porque é
// como o usuário os conta.
//
// Lifetime: para coluna string, Val.Bytes copia o literal, mas o valor vindo
// de PARÂMETRO também é copiado — o chamador pode reutilizar o slice params
// depois do Bind.
//
// `SELECT *` é um SNAPSHOT do conjunto de campos no instante do bind: num
// dataset com wildcard e unknownFields include, duas execuções da mesma query
// podem devolver conjuntos de colunas diferentes conforme novos campos são
// observados. É comportamento, não bug.
func Bind(stmt *parser.SelectStmt, sc *schema.Compiled, params []types.Value) (*BoundQuery, error) {
	if stmt == nil || sc == nil {
		return nil, errInPos("bind sem statement ou schema")
	}
	// Case-sensitive: identificadores são case-sensitive em toda a v1.
	if stmt.Dataset != sc.Name {
		return nil, errAt(stmt.DatasetPos, "dataset %q desconhecido (este handle é de %q)%s",
			stmt.Dataset, sc.Name, wholeQuotedNameHint(stmt))
	}
	if len(params) != stmt.ParamCount {
		return nil, errInPos("a query tem %d parâmetros, foram fornecidos %d",
			stmt.ParamCount, len(params))
	}
	if stmt.HasLimit && stmt.Limit < 0 {
		return nil, errAt(stmt.LimitPos, "LIMIT deve ser >= 0, é %d", stmt.Limit)
	}

	b := &binder{
		stmt:       stmt,
		sc:         sc,
		params:     params,
		referenced: map[types.FieldID]struct{}{},
	}
	bq, err := b.run()
	if err != nil {
		return nil, err
	}
	return bq, nil
}

// wholeQuotedNameHint devolve o complemento da mensagem de "dataset
// desconhecido" para quem citou um nome QUALIFICADO inteiro entre aspas —
// `FROM "_meta.counters"` —, e string vazia em todo o resto.
//
// A recusa em si está certa: pelo SQL padrão as aspas envolvem UM
// identificador, e `"_meta.counters"` nomeia uma tabela cujo nome tem um ponto,
// não a tabela `counters` do schema `_meta`. O que faltava era dizer isso: a
// mensagem sozinha manda o operador procurar um dataset que ele nunca quis
// abrir. É o erro de quem copia o nome já qualificado de uma ferramenta
// gráfica e o cola entre aspas inteiro.
//
// A detecção é pela presença do ponto no nome com Namespace vazio, e não por
// um sinalizador de "veio entre aspas" propagado do lexer, porque na AST que o
// parser produz as duas condições coincidem: identificador nu é ASCII de
// [A-Za-z0-9_] (isIdentStart/isIdentPart no lexer) e o '.' sai do lexer como
// TokenDot, que o parser consome como separador e grava em Namespace. Um ponto
// DENTRO de stmt.Dataset, portanto, só chega aqui vindo de TokenQuotedIdent.
// Isso vale para o caminho Parse → Bind; uma SelectStmt montada à mão pelo
// chamador não passa por essa garantia, e aí o complemento fala de aspas que
// ninguém escreveu.
//
// Só é consultada no ramo que JÁ ia recusar a query, e por isso não estraga
// nome legítimo nenhum: um dataset chamado `a.b` é inconstruível. O `name` do
// YAML tem de casar com [a-z0-9_]+ (isIdentName, spec.go:504-515, checado em
// spec.go:736-737), e Config.LoadDatasetSpecs recusa a carga se esse `name`
// não for idêntico ao nome do arquivo (config.go:733-736) — o sc.Name que
// chega até aqui é sempre esse valor já validado, nunca um nome de arquivo não
// conferido. Não existe, portanto, um handle com ponto para `FROM "a.b"`
// colidir; o complemento só é alcançado onde nenhum dataset legítimo poderia
// estar.
func wholeQuotedNameHint(stmt *parser.SelectStmt) string {
	if stmt.Namespace != "" {
		return ""
	}
	// Corte no PRIMEIRO ponto, com as duas partes não vazias. Em `"a.b.c"` a
	// sugestão sai `"a"."b.c"`, e não três níveis: o FROM aceita no máximo
	// namespace.tabela (parser.parseSelect), e sugerir uma forma que a
	// gramática recusa trocaria uma recusa por outra.
	i := strings.IndexByte(stmt.Dataset, '.')
	if i <= 0 || i == len(stmt.Dataset)-1 {
		return ""
	}
	return "; o nome foi citado INTEIRO, e as aspas fazem dele um identificador único" +
		" — se o ponto separa schema e tabela, cite cada parte: " +
		quoteIdent(stmt.Dataset[:i]) + "." + quoteIdent(stmt.Dataset[i+1:])
}

// quoteIdent devolve name entre aspas duplas, dobrando as aspas internas — é o
// escape que o lexer desfaz (scanQuoted), então o que sai daqui é colável de
// volta na query.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// binder acumula o estado de uma passada. Os campos Slot só podem ser
// preenchidos na SEGUNDA passada, quando o conjunto de slots está fechado.
type binder struct {
	stmt   *parser.SelectStmt
	sc     *schema.Compiled
	params []types.Value

	// refusesParams marca o bind de uma CONDIÇÃO solta, que não tem quem forneça
	// parâmetro posicional. A recusa precisa acontecer no ponto em que o `?`
	// aparece: mais tarde, a mensagem sairia como "parser inconsistente" e
	// mandaria o operador procurar um bug que não existe.
	refusesParams bool

	referenced map[types.FieldID]struct{}
	arena      []byte
	nParams    int      // comparações com parâmetro (invariante contra ParamCount)
	pending    []window // offsets de Val.Bytes a materializar depois dos appends
}

// window é um (start,end) no arena, guardado até o fim dos appends. Emitir
// Val.Bytes durante a construção deixaria janelas apontando para um array que
// o append seguinte pode realocar.
type window struct {
	cmp        *BoundComparison
	start, end int
}

func (b *binder) run() (*BoundQuery, error) {
	bq := &BoundQuery{
		Dataset:  b.sc.Name,
		Schema:   b.sc,
		Star:     b.stmt.Star,
		HasLimit: b.stmt.HasLimit,
		Limit:    b.stmt.Limit,
		Offset:   b.stmt.Offset,
		MaxRows:  int64(b.sc.MaxRows),
		Timeout:  b.sc.Timeout,
	}

	cols, err := b.projection()
	if err != nil {
		return nil, err
	}
	bq.Columns = cols

	if b.stmt.Where != nil {
		where, err := b.expr(b.stmt.Where)
		if err != nil {
			return nil, err
		}
		bq.Where = where
	}
	// ORDER BY DEPOIS do WHERE e ANTES de fechar os slots: as colunas de
	// ordenação entram no conjunto a coletar como qualquer outra. Sem o
	// b.markReferenced(f.ID) que a resolução faz, a coluna não teria slot e o
	// executor ordenaria por um valor que nunca leu.
	if len(b.stmt.OrderBy) > 0 {
		keys, err := b.ordering()
		if err != nil {
			return nil, err
		}
		bq.OrderBy = keys
	}
	// Defesa contra bug do PARSER, não contra o usuário: cada '?' aparece em
	// exatamente uma comparação, então as contagens têm de bater.
	if b.nParams != b.stmt.ParamCount {
		return nil, errInPos("parser inconsistente: ParamCount=%d, comparações com parâmetro=%d",
			b.stmt.ParamCount, b.nParams)
	}

	// Fecha o conjunto de slots e só então materializa os índices.
	bq.Slots = b.slots()
	b.fillSlots(bq)

	// Agora que o arena parou de crescer, as janelas são estáveis.
	for _, j := range b.pending {
		j.cmp.Val.Bytes = b.arena[j.start:j.end:j.end]
	}
	bq.arena = b.arena

	bq.EffectiveLimit = bq.MaxRows
	if bq.HasLimit && bq.Limit < bq.MaxRows {
		bq.EffectiveLimit = bq.Limit
	}
	return bq, nil
}

func (b *binder) projection() ([]BoundColumn, error) {
	if b.stmt.Star {
		fields := b.sc.FieldsByID()
		cols := make([]BoundColumn, 0, len(fields))
		for _, f := range fields {
			// No '*' o usuário não escreveu nome nenhum, então o rótulo é o
			// alias quando existe.
			name := f.Name
			if f.Alias != "" {
				name = f.Alias
			}
			b.markReferenced(f.ID)
			cols = append(cols, BoundColumn{Field: f, Name: name})
		}
		return cols, nil
	}

	cols := make([]BoundColumn, 0, len(b.stmt.Projection))
	// seen só materializa em projeção grande; até o limiar a varredura linear
	// sobre cols resolve sem alocar nada.
	var seen map[string]struct{}
	derivedCount := 0
	for _, ref := range b.stmt.Projection {
		if ref.Qualifier != "" {
			var err error
			cols, derivedCount, err = b.qualifiedProjection(cols, ref, derivedCount)
			if err != nil {
				return nil, err
			}
			seen = nil // o conjunto cresceu por fora; recalcula no limiar
			continue
		}
		f, err := b.column(ref.Name, ref.Pos)
		if err != nil {
			return nil, err
		}
		if isDuplicate(cols, seen, ref.Name) {
			// `SELECT id, id` vira objeto JSON com chave repetida: válido para
			// o formato, e o consumidor COLAPSA em silêncio — o usuário pede
			// duas colunas e recebe uma. A posição é a da segunda ocorrência,
			// que é a que ele apaga para corrigir.
			return nil, errAt(ref.Pos, "coluna %q aparece mais de uma vez na projeção", ref.Name)
		}
		b.markReferenced(f.ID)
		// Nome de saída é o que o usuário ESCREVEU: `SELECT status` rotula
		// "status" mesmo que o campo se chame Acct-Status-Type.
		cols = append(cols, BoundColumn{Field: f, Name: ref.Name})
		switch {
		case seen != nil:
			seen[ref.Name] = struct{}{}
		case len(cols) == projectionMapThreshold:
			seen = make(map[string]struct{}, len(b.stmt.Projection))
			for i := range cols {
				seen[cols[i].Name] = struct{}{}
			}
		}
	}
	return cols, nil
}

// maybeQualifiedColumn resolve o nome do filtro, com ou sem qualificador.
//
// Com qualificador, o nome é o CAMINHO dentro do documento — a mesma regra da
// projeção, e é o que faz `WHERE doc.x` valer para a coluna que
// `SELECT doc.x` projeta, tenha ela sido declarada com outro `as` ou
// descoberta com o nome qualificado.
func (b *binder) maybeQualifiedColumn(qualifier, name string, pos int) (*schema.CompiledField, error) {
	if qualifier == "" {
		return b.column(name, pos)
	}
	parent, ok := b.sc.FieldByName([]byte(qualifier))
	if !ok {
		return nil, errAt(pos, "campo %q não existe no dataset %q", qualifier, b.sc.Name)
	}
	f, ok := b.sc.FieldByPath(parent.Name, name)
	if !ok {
		return nil, errAt(pos, "o campo %q não tem a coluna aninhada %q", qualifier, name)
	}
	return f, nil
}

// qualifiedProjection resolve `pai.*` e `pai.caminho`.
//
// A expansão existe porque a coluna derivada fica FORA do SELECT * — sem ela,
// pedir as colunas de um documento exigiria saber de cor os nomes que o YAML
// declarou.
func (b *binder) qualifiedProjection(cols []BoundColumn, ref parser.ColumnRef, derivedCount int) ([]BoundColumn, int, error) {
	parent, ok := b.sc.FieldByName([]byte(ref.Qualifier))
	if !ok {
		return nil, 0, errAt(ref.Pos, "campo %q não existe no dataset %q", ref.Qualifier, b.sc.Name)
	}
	children := b.sc.FieldsByParent(parent.Name)
	if len(children) == 0 {
		// A recusa precisa dizer o que FALTA, e isso depende do que o campo
		// declarou. Um pai que descobre não precisa de extract nenhum: o que
		// falta a ele é evento que traga a chave.
		if parent.DiscoverNested {
			return nil, 0, errAt(ref.Pos,
				"campo %q ainda não descobriu nenhuma coluna; elas aparecem conforme os eventos trazem as chaves", ref.Qualifier)
		}
		return nil, 0, errAt(ref.Pos,
			"campo %q não declara extract nem descoberta de campos aninhados; não há coluna a projetar", ref.Qualifier)
	}

	// addedCols é quantas colunas aninhadas este item acrescenta. O teto é
	// conferido para os DOIS caminhos: contá-lo só na expansão deixava
	// `pai.*, pai.caminho` passar acima do limite, porque cada metade cabia
	// sozinha.
	addedCols := 1
	if ref.Star {
		addedCols = len(children)
	}
	// Ao estourar, RECUSA nomeando o limite — nunca trunca. Truncar entregaria
	// colunas faltando com cara de resultado completo, e o erro aqui é
	// acionável: pedem-se as colunas pelo nome.
	if n := derivedCount + addedCols; n > b.sc.MaxNestedCols {
		return nil, 0, errAt(ref.Pos,
			"a projeção pede %d colunas aninhadas, acima de schema.maxNestedCols (%d); "+
				"peça as colunas pelo nome ou ajuste o teto do dataset", n, b.sc.MaxNestedCols)
	}

	if !ref.Star {
		f, ok := b.sc.FieldByPath(parent.Name, ref.Name)
		if !ok {
			return nil, 0, errAt(ref.Pos, "o campo %q não extrai o caminho %q", ref.Qualifier, ref.Name)
		}
		name := ref.Qualifier + "." + ref.Name
		if isDuplicate(cols, nil, name) {
			return nil, 0, errAt(ref.Pos, "coluna %q aparece mais de uma vez na projeção", name)
		}
		b.markReferenced(f.ID)
		return append(cols, BoundColumn{Field: f, Name: name}), derivedCount + 1, nil
	}

	for _, f := range children {
		if isDuplicate(cols, nil, f.Name) {
			return nil, 0, errAt(ref.Pos, "coluna %q aparece mais de uma vez na projeção", f.Name)
		}
		b.markReferenced(f.ID)
		cols = append(cols, BoundColumn{Field: f, Name: f.Name})
	}
	return cols, derivedCount + len(children), nil
}

// projectionMapThreshold é onde a checagem de nome repetido troca varredura
// linear por mapa. A projeção real tem unidades de colunas, e aí a varredura é
// mais barata e não aloca; o mapa existe porque o número de nomes DISTINTOS
// que bindam com sucesso não é pequeno (FieldID é uint16, e cada campo
// responde por nome e alias), e o custo quadrático transformaria uma query de
// algumas centenas de KB em segundos de CPU.
const projectionMapThreshold = 32

// isDuplicate usa o mapa quando ele existe e a varredura enquanto não existe.
// As duas responderiam igual em qualquer ponto: o mapa é semeado com o que a
// varredura já tinha visto.
func isDuplicate(cols []BoundColumn, seen map[string]struct{}, name string) bool {
	if seen != nil {
		_, ok := seen[name]
		return ok
	}
	for i := range cols {
		if cols[i].Name == name {
			return true
		}
	}
	return false
}

// column resolve nome original OU alias. Nunca chama ResolveUnknown: aquilo é
// do writer e registraria um campo novo no dicionário a cada erro de
// digitação numa query.
func (b *binder) column(name string, pos int) (*schema.CompiledField, error) {
	f, ok := b.sc.FieldByName([]byte(name))
	if !ok {
		// Campo excluído do schema também cai aqui, e é o certo: para o
		// runtime ele não existe.
		return nil, errAt(pos, "coluna %q não existe no dataset %q", name, b.sc.Name)
	}
	return f, nil
}

func (b *binder) markReferenced(id types.FieldID) { b.referenced[id] = struct{}{} }

// expr percorre a ESPINHA ESQUERDA iterativamente, pelo mesmo motivo que o
// parser é iterativo em parseAnd/parseOr: `a=1 AND a=1 AND …` é left-leaning e
// cresce um nível por termo, sem parênteses. Uma query gerada por máquina (um
// IN expandido, um filtro montado por UI) chega a dezenas de milhares de
// termos sem intenção nenhuma, e recursão aqui seria um frame por termo.
//
// O lado DIREITO continua recursivo, e isso é seguro para AST vinda do parser:
// só se aprofunda com parênteses, que o parser já limita a MaxExprDepth. O
// budget abaixo cobre o caso de uma AST construída à mão (o Bind é público e
// recebe uma AST que qualquer um pode montar).
//
// A ORDEM de visita é preservada — folha mais à esquerda primeiro, depois os
// filhos direitos do mais profundo ao topo, que é a mesma ordem léxica da
// recursão anterior. Ela decide qual erro é reportado primeiro e o layout do
// arena.
func (b *binder) expr(e parser.Expr) (BoundExpr, error) {
	return b.exprAtDepth(e, 0)
}

func (b *binder) exprAtDepth(e parser.Expr, depth int) (BoundExpr, error) {
	if depth > MaxBindDepth {
		return nil, errInPos("expressão aninhada demais no WHERE (máximo %d níveis à direita)", MaxBindDepth)
	}

	var spine []*parser.LogicalExpr
	for {
		l, ok := e.(*parser.LogicalExpr)
		if !ok {
			break
		}
		spine = append(spine, l)
		e = l.Left
	}

	current, err := b.leaf(e)
	if err != nil {
		return nil, err
	}
	for i := len(spine) - 1; i >= 0; i-- {
		right, err := b.exprAtDepth(spine[i].Right, depth+1)
		if err != nil {
			return nil, err
		}
		current = &BoundLogical{Op: spine[i].Op, Left: current, Right: right}
	}
	return current, nil
}

// leaf liga os nós terminais. Um *parser.LogicalExpr nunca chega aqui: a
// espinha já o consumiu.
func (b *binder) leaf(e parser.Expr) (BoundExpr, error) {
	switch n := e.(type) {
	case *parser.Comparison:
		return b.comparison(n)

	case *parser.NullCheck:
		f, err := b.maybeQualifiedColumn(n.Qualifier, n.Column, n.ColumnPos)
		if err != nil {
			return nil, err
		}
		b.markReferenced(f.ID)
		// Sem coerção e sem restrição de tipo: presença vale para bytes, json
		// e campo auto nunca observado.
		return &BoundNullCheck{Field: f, Not: n.Not}, nil

	case *parser.LikeExpr:
		return b.like(n)

	default:
		// Nó novo na AST tem de falhar ALTO, não ser ignorado.
		return nil, errInPos("expressão não suportada: %T", e)
	}
}

// ordering resolve as chaves do ORDER BY contra o schema.
//
// Chave REPETIDA é recusada, e não colapsada: `ORDER BY a, a DESC` é
// contraditório e aceitar em silêncio uma das duas leituras seria escolher pelo
// usuário. É a mesma doutrina que já recusa coluna repetida na projeção.
func (b *binder) ordering() ([]BoundOrderKey, error) {
	out := make([]BoundOrderKey, 0, len(b.stmt.OrderBy))
	seen := make(map[types.FieldID]struct{}, len(b.stmt.OrderBy))
	for _, k := range b.stmt.OrderBy {
		f, err := b.maybeQualifiedColumn(k.Qualifier, k.Column, k.ColumnPos)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[f.ID]; duplicate {
			return nil, errAt(k.ColumnPos, "coluna %q aparece duas vezes no ORDER BY", f.Name)
		}
		seen[f.ID] = struct{}{}

		t := f.EffectiveType()
		if t == types.TypeNull {
			return nil, errAt(k.ColumnPos, "coluna %q é auto e nunca foi observada neste dataset, "+
				"então ainda não tem tipo e não pode ordenar", f.Name)
		}
		kind, ok := admitsOrdering(t)
		if !ok {
			return nil, errAt(k.ColumnPos, "coluna %q é %s e não tem ordem definida neste produto", f.Name, t)
		}
		b.markReferenced(f.ID)
		out = append(out, BoundOrderKey{
			Field: f, Desc: k.Desc, NullsFirst: k.NullsFirst, Kind: kind, Want: t})
	}
	return out, nil
}

// like liga `coluna [NOT] LIKE <padrão>`.
//
// O padrão é compilado AQUI, uma vez por query. Fazer isso por linha seria o
// erro clássico deste operador — e é também o que permite `LIKE ?` funcionar
// sem caminho separado, porque o bind já recebe os parâmetros.
func (b *binder) like(n *parser.LikeExpr) (BoundExpr, error) {
	f, err := b.maybeQualifiedColumn(n.Qualifier, n.Column, n.ColumnPos)
	if err != nil {
		return nil, err
	}
	b.markReferenced(f.ID)

	t := f.EffectiveType()
	if t == types.TypeNull {
		return nil, errAt(n.ColumnPos, "coluna %q é auto e nunca foi observada neste dataset, "+
			"então ainda não tem tipo; só admite IS NULL / IS NOT NULL", f.Name)
	}
	if t != types.TypeString {
		return nil, errAt(n.ColumnPos, "LIKE só vale para coluna de texto; %q é %s", f.Name, t)
	}

	var pat []byte
	switch n.Pattern.Kind {
	case parser.OperandString:
		pat = []byte(n.Pattern.S)
	case parser.OperandParam:
		if b.refusesParams {
			return nil, errAt(n.Pattern.Pos, "condição não aceita parâmetro posicional")
		}
		idx := n.Pattern.ParamIdx
		if idx < 0 || idx >= len(b.params) {
			return nil, errInPos("parser inconsistente: ParamIdx=%d fora de [0,%d)", idx, len(b.params))
		}
		b.nParams++
		if b.params[idx].Type != types.TypeString {
			return nil, errAt(n.Pattern.Pos, "o parâmetro %d do LIKE é %s; o padrão é texto",
				idx+1, b.params[idx].Type)
		}
		pat = b.params[idx].Bytes
	default:
		return nil, errAt(n.Pattern.Pos, "o padrão de LIKE é texto ou parâmetro")
	}
	if len(pat) > parser.MaxLikePatternBytes {
		return nil, errAt(n.Pattern.Pos, "padrão de LIKE acima de %d bytes", parser.MaxLikePatternBytes)
	}
	// CompileLike copia os bytes, então o padrão sobrevive ao arena e ao
	// buffer do chamador dos parâmetros.
	return &BoundLike{Field: f, Not: n.Not, Pat: CompileLike(pat)}, nil
}

func (b *binder) comparison(n *parser.Comparison) (BoundExpr, error) {
	f, err := b.maybeQualifiedColumn(n.Qualifier, n.Column, n.ColumnPos)
	if err != nil {
		return nil, err
	}
	b.markReferenced(f.ID)

	cmp := &BoundComparison{Field: f, Op: n.Op}
	var (
		val         types.Value
		kind        CompareKind
		cerr        error
		arenaBefore = len(b.arena)
	)
	if n.Operand.Kind == parser.OperandParam {
		if b.refusesParams {
			return nil, errAt(n.Operand.Pos, "condição não aceita parâmetro posicional")
		}
		idx := n.Operand.ParamIdx
		if idx < 0 || idx >= len(b.params) {
			return nil, errInPos("parser inconsistente: ParamIdx=%d fora de [0,%d)", idx, len(b.params))
		}
		b.nParams++
		cmp.IsParam = true
		cmp.ParamIdx = idx
		val, kind, b.arena, cerr = coerceParam(f, n.Op, b.params[idx], b.arena)
		if cerr != nil {
			return nil, b.paramError(idx, f, n, cerr)
		}
	} else {
		val, kind, b.arena, cerr = coerceLiteral(f, n.Op, n.Operand, b.arena)
		if cerr != nil {
			return nil, b.literalError(f, n, cerr)
		}
	}

	cmp.Val = val
	cmp.Kind = kind
	if len(b.arena) > arenaBefore {
		// O valor foi para o arena: guarda os offsets e materializa a janela
		// só quando o arena parar de crescer.
		cmp.Val.Bytes = nil
		b.pending = append(b.pending, window{cmp: cmp, start: arenaBefore, end: len(b.arena)})
	}
	return cmp, nil
}

// literalError traduz o sentinela da tabela para a mensagem final, que precisa
// do nome da coluna e do tipo.
func (b *binder) literalError(f *schema.CompiledField, n *parser.Comparison, cerr error) error {
	t := f.EffectiveType()
	pos := n.Operand.Pos
	switch cerr {
	case errAutoWithoutType:
		return errAt(n.ColumnPos, "coluna %q é auto e nunca foi observada neste dataset, "+
			"então ainda não tem tipo; só admite IS NULL / IS NOT NULL", f.Name)
	case errColumnWithoutType:
		return errAt(n.ColumnPos, "coluna %q não tem tipo utilizável", f.Name)
	case errNotComparable:
		return errAt(n.ColumnPos, "coluna %q (%s) só admite IS NULL / IS NOT NULL", f.Name, t)
	case errIncompatibleLiteral:
		return errAt(pos, "coluna %q (%s) não é comparável com literal do tipo %s",
			f.Name, t, literalName(n.Operand.Kind))
	case errOperatorNotAdmitted:
		return errAt(pos, "operador %q não é permitido para a coluna %q (%s); use = ou !=",
			n.Op.String(), f.Name, t)
	case errInvalidLiteral:
		return errAt(pos, "literal %q inválido para a coluna %q (%s)", literalValue(n.Operand), f.Name, describeType(f, t))
	default:
		return errAt(pos, "coluna %q (%s): %v", f.Name, t, cerr)
	}
}

func (b *binder) paramError(idx int, f *schema.CompiledField, n *parser.Comparison, cerr error) error {
	// Nas mensagens o parâmetro é 1-based, porque é como o usuário o conta.
	num := idx + 1
	t := f.EffectiveType()
	switch cerr {
	case errParamNull:
		return errInPos("parâmetro %d: NULL não é suportado; use IS NULL", num)
	case errParamType:
		return errInPos("parâmetro %d: tipo %s não suportado (use string, int64, double ou boolean)",
			num, b.params[idx].Type)
	case errAutoWithoutType:
		return errInPos("parâmetro %d: coluna %q é auto e nunca foi observada neste dataset, "+
			"então ainda não tem tipo; só admite IS NULL / IS NOT NULL", num, f.Name)
	case errNotComparable:
		return errInPos("parâmetro %d: coluna %q (%s) só admite IS NULL / IS NOT NULL", num, f.Name, t)
	case errIncompatibleLiteral:
		return errInPos("parâmetro %d: coluna %q (%s) não é comparável com literal do tipo %s",
			num, f.Name, t, b.params[idx].Type)
	case errOperatorNotAdmitted:
		return errInPos("parâmetro %d: operador %q não é permitido para a coluna %q (%s); use = ou !=",
			num, n.Op.String(), f.Name, t)
	case errInvalidLiteral:
		return errInPos("parâmetro %d: valor inválido para a coluna %q (%s)", num, f.Name, describeType(f, t))
	default:
		return errInPos("parâmetro %d: %v", num, cerr)
	}
}

// describeType acrescenta o format declarado quando o tipo o tem. Sem ele,
// "literal 'x' inválido para a coluna momento (datetime)" não diz ao usuário
// em que grafia escrever o literal, e a mensagem fica inacionável.
func describeType(f *schema.CompiledField, t types.FieldType) string {
	if f.Format == "" {
		return t.String()
	}
	return t.String() + ", format: " + f.Format
}

func literalName(k parser.OperandKind) string {
	switch k {
	case parser.OperandBool:
		return "boolean"
	default:
		return k.String()
	}
}

func literalValue(op parser.Operand) string {
	if op.Kind == parser.OperandString {
		return op.S
	}
	return op.Kind.String()
}

func (b *binder) slots() Slots {
	ids := make([]types.FieldID, 0, len(b.referenced))
	for id := range b.referenced {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return Slots{IDs: ids}
}

// fillSlots é a segunda passada: os índices só existem depois que o
// conjunto de campos usados está fechado.
func (b *binder) fillSlots(bq *BoundQuery) {
	for i := range bq.Columns {
		slot, _ := bq.Slots.Index(bq.Columns[i].Field.ID)
		bq.Columns[i].Slot = slot
	}
	for i := range bq.OrderBy {
		slot, _ := bq.Slots.Index(bq.OrderBy[i].Field.ID)
		bq.OrderBy[i].Slot = slot
	}
	b.fillExprSlots(bq.Where, bq.Slots)
}

// fillExprSlots numera os slots de uma expressão contra um conjunto já
// fechado. Vive separada porque tem DOIS chamadores — o WHERE de uma query e
// a condição solta de uma regra de TTL —, e duas cópias divergiriam no dia em
// que um nó novo entrasse no bound-AST.
func (b *binder) fillExprSlots(e BoundExpr, s Slots) {
	// Percurso com PILHA EXPLÍCITA, pela mesma razão que o expr percorre a
	// espinha iterativamente: uma cadeia de 50 mil termos é uma árvore de 50
	// mil níveis, e um walk recursivo aqui gastaria um frame por termo —
	// desfazendo, na segunda passada, o cuidado tomado na primeira.
	// Query SEM WHERE tem Where nil, e empilhá-lo faria o `default` abaixo
	// entrar em pânico com um <nil>. Antes de o default existir, o nil caía no
	// switch em silêncio — o que funcionava por acidente, e escondia que a
	// pilha podia carregar algo que nenhum case trata.
	if e == nil {
		return
	}
	stack := []BoundExpr{e}
	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		switch n := e.(type) {
		case *BoundLogical:
			stack = append(stack, n.Left, n.Right)
		case *BoundComparison:
			n.Slot, _ = s.Index(n.Field.ID)
		case *BoundNullCheck:
			n.Slot, _ = s.Index(n.Field.ID)
		case *BoundLike:
			n.Slot, _ = s.Index(n.Field.ID)
		default:
			// Nó novo sem case aqui é FALHA SILENCIOSA, e a pior espécie: o
			// Slot fica em 0, o predicado testa o campo de MENOR FieldID da
			// query, e a resposta sai errada sem erro, sem log e sem métrica.
			// A `leaf` acima já falha alto por este motivo; este switch não
			// falhava, e não há linter `exhaustive` no .golangci.yml para
			// cobrar. Entra ANTES do primeiro nó novo precisar dele.
			panic(fmt.Sprintf("binder: fillExprSlots sem case para %T", e))
		}
	}
}
