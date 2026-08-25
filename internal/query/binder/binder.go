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
	"sort"
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
	Timeout        time.Duration

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

func (*BoundLogical) isBoundExpr()    {}
func (*BoundComparison) isBoundExpr() {}
func (*BoundNullCheck) isBoundExpr()  {}

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
		return nil, errNoPos("bind sem statement ou schema")
	}
	// Case-sensitive: identificadores são case-sensitive em toda a v1.
	if stmt.Dataset != sc.Name {
		return nil, errAt(stmt.DatasetPos, "dataset %q desconhecido (este handle é de %q)",
			stmt.Dataset, sc.Name)
	}
	if len(params) != stmt.ParamCount {
		return nil, errNoPos("a query tem %d parâmetros, foram fornecidos %d",
			stmt.ParamCount, len(params))
	}
	if stmt.HasLimit && stmt.Limit < 0 {
		return nil, errAt(stmt.LimitPos, "LIMIT deve ser >= 0, é %d", stmt.Limit)
	}

	b := &binder{
		stmt:   stmt,
		sc:     sc,
		params: params,
		usados: map[types.FieldID]struct{}{},
	}
	bq, err := b.run()
	if err != nil {
		return nil, err
	}
	return bq, nil
}

// binder acumula o estado de uma passada. Os campos Slot só podem ser
// preenchidos na SEGUNDA passada, quando o conjunto de slots está fechado.
type binder struct {
	stmt   *parser.SelectStmt
	sc     *schema.Compiled
	params []types.Value

	usados   map[types.FieldID]struct{}
	arena    []byte
	nParams  int      // comparações com parâmetro (invariante contra ParamCount)
	pendente []janela // offsets de Val.Bytes a materializar depois dos appends
}

// janela é um (start,end) no arena, guardado até o fim dos appends. Emitir
// Val.Bytes durante a construção deixaria janelas apontando para um array que
// o append seguinte pode realocar.
type janela struct {
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
		MaxRows:  int64(b.sc.MaxRows),
		Timeout:  b.sc.Timeout,
	}

	cols, err := b.projecao()
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
	// Defesa contra bug do PARSER, não contra o usuário: cada '?' aparece em
	// exatamente uma comparação, então as contagens têm de bater.
	if b.nParams != b.stmt.ParamCount {
		return nil, errNoPos("parser inconsistente: ParamCount=%d, comparações com parâmetro=%d",
			b.stmt.ParamCount, b.nParams)
	}

	// Fecha o conjunto de slots e só então materializa os índices.
	bq.Slots = b.slots()
	b.preencheSlots(bq)

	// Agora que o arena parou de crescer, as janelas são estáveis.
	for _, j := range b.pendente {
		j.cmp.Val.Bytes = b.arena[j.start:j.end:j.end]
	}
	bq.arena = b.arena

	bq.EffectiveLimit = bq.MaxRows
	if bq.HasLimit && bq.Limit < bq.MaxRows {
		bq.EffectiveLimit = bq.Limit
	}
	return bq, nil
}

func (b *binder) projecao() ([]BoundColumn, error) {
	if b.stmt.Star {
		campos := b.sc.FieldsByID()
		cols := make([]BoundColumn, 0, len(campos))
		for _, f := range campos {
			// No '*' o usuário não escreveu nome nenhum, então o rótulo é o
			// alias quando existe.
			nome := f.Name
			if f.Alias != "" {
				nome = f.Alias
			}
			b.usa(f.ID)
			cols = append(cols, BoundColumn{Field: f, Name: nome})
		}
		return cols, nil
	}

	cols := make([]BoundColumn, 0, len(b.stmt.Projection))
	// vistos só materializa em projeção grande; até o limiar a varredura linear
	// sobre cols resolve sem alocar nada.
	var vistos map[string]struct{}
	derivadas := 0
	for _, ref := range b.stmt.Projection {
		if ref.Qualifier != "" {
			var err error
			cols, derivadas, err = b.qualificada(cols, ref, derivadas)
			if err != nil {
				return nil, err
			}
			vistos = nil // o conjunto cresceu por fora; recalcula no limiar
			continue
		}
		f, err := b.coluna(ref.Name, ref.Pos)
		if err != nil {
			return nil, err
		}
		if repetido(cols, vistos, ref.Name) {
			// `SELECT id, id` vira objeto JSON com chave repetida: válido para
			// o formato, e o consumidor COLAPSA em silêncio — o usuário pede
			// duas colunas e recebe uma. A posição é a da segunda ocorrência,
			// que é a que ele apaga para corrigir.
			return nil, errAt(ref.Pos, "coluna %q aparece mais de uma vez na projeção", ref.Name)
		}
		b.usa(f.ID)
		// Nome de saída é o que o usuário ESCREVEU: `SELECT status` rotula
		// "status" mesmo que o campo se chame Acct-Status-Type.
		cols = append(cols, BoundColumn{Field: f, Name: ref.Name})
		switch {
		case vistos != nil:
			vistos[ref.Name] = struct{}{}
		case len(cols) == limiarMapaProjecao:
			vistos = make(map[string]struct{}, len(b.stmt.Projection))
			for i := range cols {
				vistos[cols[i].Name] = struct{}{}
			}
		}
	}
	return cols, nil
}

// qualificada resolve `pai.*` e `pai.caminho`.
//
// A expansão existe porque a coluna derivada fica FORA do SELECT * — sem ela,
// pedir as colunas de um documento exigiria saber de cor os nomes que o YAML
// declarou.
func (b *binder) qualificada(cols []BoundColumn, ref parser.ColumnRef, derivadas int) ([]BoundColumn, int, error) {
	pai, ok := b.sc.FieldByName([]byte(ref.Qualifier))
	if !ok {
		return nil, 0, errAt(ref.Pos, "campo %q não existe no dataset %q", ref.Qualifier, b.sc.Name)
	}
	filhas := b.sc.FieldsByParent(pai.Name)
	if len(filhas) == 0 {
		return nil, 0, errAt(ref.Pos, "campo %q não declara extract; não há coluna aninhada a projetar", ref.Qualifier)
	}

	// Quantas colunas aninhadas este item acrescenta. O teto é conferido para
	// os DOIS caminhos: contá-lo só na expansão deixava `pai.*, pai.caminho`
	// passar acima do limite, porque cada metade cabia sozinha.
	acrescenta := 1
	if ref.Star {
		acrescenta = len(filhas)
	}
	// Ao estourar, RECUSA nomeando o limite — nunca trunca. Truncar entregaria
	// colunas faltando com cara de resultado completo, e o erro aqui é
	// acionável: pedem-se as colunas pelo nome.
	if n := derivadas + acrescenta; n > b.sc.MaxNestedCols {
		return nil, 0, errAt(ref.Pos,
			"a projeção pede %d colunas aninhadas, acima de schema.maxNestedCols (%d); "+
				"peça as colunas pelo nome ou ajuste o teto do dataset", n, b.sc.MaxNestedCols)
	}

	if !ref.Star {
		f, ok := b.sc.FieldByPath(pai.Name, ref.Name)
		if !ok {
			return nil, 0, errAt(ref.Pos, "o campo %q não extrai o caminho %q", ref.Qualifier, ref.Name)
		}
		nome := ref.Qualifier + "." + ref.Name
		if repetido(cols, nil, nome) {
			return nil, 0, errAt(ref.Pos, "coluna %q aparece mais de uma vez na projeção", nome)
		}
		b.usa(f.ID)
		return append(cols, BoundColumn{Field: f, Name: nome}), derivadas + 1, nil
	}

	for _, f := range filhas {
		if repetido(cols, nil, f.Name) {
			return nil, 0, errAt(ref.Pos, "coluna %q aparece mais de uma vez na projeção", f.Name)
		}
		b.usa(f.ID)
		cols = append(cols, BoundColumn{Field: f, Name: f.Name})
	}
	return cols, derivadas + len(filhas), nil
}

// limiarMapaProjecao é onde a checagem de nome repetido troca varredura linear
// por mapa. A projeção real tem unidades de colunas, e aí a varredura é mais
// barata e não aloca; o mapa existe porque o número de nomes DISTINTOS que
// bindam com sucesso não é pequeno (FieldID é uint16, e cada campo responde por
// nome e alias), e o custo quadrático transformaria uma query de algumas
// centenas de KB em segundos de CPU.
const limiarMapaProjecao = 32

// repetido usa o mapa quando ele existe e a varredura enquanto não existe. As
// duas responderiam igual em qualquer ponto: o mapa é semeado com o que a
// varredura já tinha visto.
func repetido(cols []BoundColumn, vistos map[string]struct{}, nome string) bool {
	if vistos != nil {
		_, ok := vistos[nome]
		return ok
	}
	for i := range cols {
		if cols[i].Name == nome {
			return true
		}
	}
	return false
}

// coluna resolve nome original OU alias. Nunca chama ResolveUnknown: aquilo é
// do writer e registraria um campo novo no dicionário a cada erro de
// digitação numa query.
func (b *binder) coluna(nome string, pos int) (*schema.CompiledField, error) {
	f, ok := b.sc.FieldByName([]byte(nome))
	if !ok {
		// Campo excluído do schema também cai aqui, e é o certo: para o
		// runtime ele não existe.
		return nil, errAt(pos, "coluna %q não existe no dataset %q", nome, b.sc.Name)
	}
	return f, nil
}

func (b *binder) usa(id types.FieldID) { b.usados[id] = struct{}{} }

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
	return b.exprProf(e, 0)
}

func (b *binder) exprProf(e parser.Expr, prof int) (BoundExpr, error) {
	if prof > MaxBindDepth {
		return nil, errNoPos("expressão aninhada demais no WHERE (máximo %d níveis à direita)", MaxBindDepth)
	}

	var espinha []*parser.LogicalExpr
	for {
		l, ok := e.(*parser.LogicalExpr)
		if !ok {
			break
		}
		espinha = append(espinha, l)
		e = l.Left
	}

	atual, err := b.folha(e)
	if err != nil {
		return nil, err
	}
	for i := len(espinha) - 1; i >= 0; i-- {
		direita, err := b.exprProf(espinha[i].Right, prof+1)
		if err != nil {
			return nil, err
		}
		atual = &BoundLogical{Op: espinha[i].Op, Left: atual, Right: direita}
	}
	return atual, nil
}

// folha liga os nós terminais. Um *parser.LogicalExpr nunca chega aqui: a
// espinha já o consumiu.
func (b *binder) folha(e parser.Expr) (BoundExpr, error) {
	switch n := e.(type) {
	case *parser.Comparison:
		return b.comparacao(n)

	case *parser.NullCheck:
		f, err := b.coluna(n.Column, n.ColumnPos)
		if err != nil {
			return nil, err
		}
		b.usa(f.ID)
		// Sem coerção e sem restrição de tipo: presença vale para bytes, json
		// e campo auto nunca observado.
		return &BoundNullCheck{Field: f, Not: n.Not}, nil

	default:
		// Nó novo na AST tem de falhar ALTO, não ser ignorado.
		return nil, errNoPos("expressão não suportada: %T", e)
	}
}

func (b *binder) comparacao(n *parser.Comparison) (BoundExpr, error) {
	f, err := b.coluna(n.Column, n.ColumnPos)
	if err != nil {
		return nil, err
	}
	b.usa(f.ID)

	cmp := &BoundComparison{Field: f, Op: n.Op}
	var (
		val   types.Value
		kind  CompareKind
		cerr  error
		antes = len(b.arena)
	)
	if n.Operand.Kind == parser.OperandParam {
		idx := n.Operand.ParamIdx
		if idx < 0 || idx >= len(b.params) {
			return nil, errNoPos("parser inconsistente: ParamIdx=%d fora de [0,%d)", idx, len(b.params))
		}
		b.nParams++
		cmp.IsParam = true
		cmp.ParamIdx = idx
		val, kind, b.arena, cerr = coerceParam(f, n.Op, b.params[idx], b.arena)
		if cerr != nil {
			return nil, b.erroParam(idx, f, n, cerr)
		}
	} else {
		val, kind, b.arena, cerr = coerceLiteral(f, n.Op, n.Operand, b.arena)
		if cerr != nil {
			return nil, b.erroLiteral(f, n, cerr)
		}
	}

	cmp.Val = val
	cmp.Kind = kind
	if len(b.arena) > antes {
		// O valor foi para o arena: guarda os offsets e materializa a janela
		// só quando o arena parar de crescer.
		cmp.Val.Bytes = nil
		b.pendente = append(b.pendente, janela{cmp: cmp, start: antes, end: len(b.arena)})
	}
	return cmp, nil
}

// erroLiteral traduz o sentinela da tabela para a mensagem final, que precisa
// do nome da coluna e do tipo.
func (b *binder) erroLiteral(f *schema.CompiledField, n *parser.Comparison, cerr error) error {
	t := f.EffectiveType()
	pos := n.Operand.Pos
	switch cerr {
	case errAutoSemTipo:
		return errAt(n.ColumnPos, "coluna %q é auto e nunca foi observada neste dataset, "+
			"então ainda não tem tipo; só admite IS NULL / IS NOT NULL", f.Name)
	case errColunaSemTipo:
		return errAt(n.ColumnPos, "coluna %q não tem tipo utilizável", f.Name)
	case errNaoComparavel:
		return errAt(n.ColumnPos, "coluna %q (%s) só admite IS NULL / IS NOT NULL", f.Name, t)
	case errLiteralIncompativel:
		return errAt(pos, "coluna %q (%s) não é comparável com literal do tipo %s",
			f.Name, t, nomeLiteral(n.Operand.Kind))
	case errOperadorNaoAdmitido:
		return errAt(pos, "operador %q não é permitido para a coluna %q (%s); use = ou !=",
			n.Op.String(), f.Name, t)
	case errLiteralInvalido:
		return errAt(pos, "literal %q inválido para a coluna %q (%s)", valorLiteral(n.Operand), f.Name, descreveTipo(f, t))
	default:
		return errAt(pos, "coluna %q (%s): %v", f.Name, t, cerr)
	}
}

func (b *binder) erroParam(idx int, f *schema.CompiledField, n *parser.Comparison, cerr error) error {
	// Nas mensagens o parâmetro é 1-based, porque é como o usuário o conta.
	num := idx + 1
	t := f.EffectiveType()
	switch cerr {
	case errParamNulo:
		return errNoPos("parâmetro %d: NULL não é suportado; use IS NULL", num)
	case errParamTipo:
		return errNoPos("parâmetro %d: tipo %s não suportado (use string, int64, double ou boolean)",
			num, b.params[idx].Type)
	case errAutoSemTipo:
		return errNoPos("parâmetro %d: coluna %q é auto e nunca foi observada neste dataset, "+
			"então ainda não tem tipo; só admite IS NULL / IS NOT NULL", num, f.Name)
	case errNaoComparavel:
		return errNoPos("parâmetro %d: coluna %q (%s) só admite IS NULL / IS NOT NULL", num, f.Name, t)
	case errLiteralIncompativel:
		return errNoPos("parâmetro %d: coluna %q (%s) não é comparável com literal do tipo %s",
			num, f.Name, t, b.params[idx].Type)
	case errOperadorNaoAdmitido:
		return errNoPos("parâmetro %d: operador %q não é permitido para a coluna %q (%s); use = ou !=",
			num, n.Op.String(), f.Name, t)
	case errLiteralInvalido:
		return errNoPos("parâmetro %d: valor inválido para a coluna %q (%s)", num, f.Name, descreveTipo(f, t))
	default:
		return errNoPos("parâmetro %d: %v", num, cerr)
	}
}

// descreveTipo acrescenta o format declarado quando o tipo o tem. Sem ele,
// "literal 'x' inválido para a coluna momento (datetime)" não diz ao usuário
// em que grafia escrever o literal, e a mensagem fica inacionável.
func descreveTipo(f *schema.CompiledField, t types.FieldType) string {
	if f.Format == "" {
		return t.String()
	}
	return t.String() + ", format: " + f.Format
}

func nomeLiteral(k parser.OperandKind) string {
	switch k {
	case parser.OperandBool:
		return "boolean"
	default:
		return k.String()
	}
}

func valorLiteral(op parser.Operand) string {
	if op.Kind == parser.OperandString {
		return op.S
	}
	return op.Kind.String()
}

func (b *binder) slots() Slots {
	ids := make([]types.FieldID, 0, len(b.usados))
	for id := range b.usados {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return Slots{IDs: ids}
}

// preencheSlots é a segunda passada: os índices só existem depois que o
// conjunto de campos usados está fechado.
func (b *binder) preencheSlots(bq *BoundQuery) {
	for i := range bq.Columns {
		slot, _ := bq.Slots.Index(bq.Columns[i].Field.ID)
		bq.Columns[i].Slot = slot
	}
	// Percurso com PILHA EXPLÍCITA, pela mesma razão que o expr percorre a
	// espinha iterativamente: uma cadeia de 50 mil termos é uma árvore de 50
	// mil níveis, e um walk recursivo aqui gastaria um frame por termo —
	// desfazendo, na segunda passada, o cuidado tomado na primeira.
	pilha := []BoundExpr{bq.Where}
	for len(pilha) > 0 {
		e := pilha[len(pilha)-1]
		pilha = pilha[:len(pilha)-1]
		switch n := e.(type) {
		case *BoundLogical:
			pilha = append(pilha, n.Left, n.Right)
		case *BoundComparison:
			n.Slot, _ = bq.Slots.Index(n.Field.ID)
		case *BoundNullCheck:
			n.Slot, _ = bq.Slots.Index(n.Field.ID)
		}
	}
}
