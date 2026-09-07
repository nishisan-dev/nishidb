package parser_test

import (
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

func parseOK(t *testing.T, sql string) *parser.SelectStmt {
	t.Helper()
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	if st == nil {
		t.Fatalf("Parse(%q) devolveu stmt nil sem erro", sql)
	}
	return st
}

// cmp compara a árvore inteira: DeepEqual só funciona porque a AST não tem
// campo não-exportado e Star ⇒ Projection nil (slice vazia seria diferente).
func cmp(t *testing.T, sql string, want *parser.SelectStmt) {
	t.Helper()
	got := parseOK(t, sql)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Parse(%q) =\n %#v\nwant\n %#v", sql, got, want)
	}
}

func TestProjecaoEstrela(t *testing.T) {
	cmp(t, "SELECT * FROM radius_sessions", &parser.SelectStmt{
		Star:       true,
		Dataset:    "radius_sessions",
		DatasetPos: 14,
	})
	// Star com Projection não-nil seria outro estado: o teste acima já trava
	// isso pelo DeepEqual, mas a asserção explícita nomeia a intenção.
	if st := parseOK(t, "SELECT * FROM x"); st.Projection != nil {
		t.Errorf("Projection = %#v, want nil quando Star", st.Projection)
	}
}

func TestProjecaoComColunas(t *testing.T) {
	cmp(t, "SELECT id, status, ip FROM radius_sessions", &parser.SelectStmt{
		Projection: []parser.ColumnRef{
			{Name: "id", Pos: 7},
			{Name: "status", Pos: 11},
			{Name: "ip", Pos: 19},
		},
		Dataset:    "radius_sessions",
		DatasetPos: 27,
	})
}

// A query da spec §68, conferida por inteiro: é o caso que o driver JDBC vai
// emitir.
func TestQuerySpec68(t *testing.T) {
	cmp(t, "SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100", &parser.SelectStmt{
		Projection: []parser.ColumnRef{{Name: "id", Pos: 7}, {Name: "ip", Pos: 11}},
		Dataset:    "radius_sessions",
		DatasetPos: 19,
		Where: &parser.Comparison{
			Column:    "status",
			ColumnPos: 41,
			Op:        parser.CmpEq,
			Operand:   parser.Operand{Kind: parser.OperandParam, ParamIdx: 0, Pos: 50},
		},
		HasLimit:   true,
		Limit:      100,
		LimitPos:   58,
		ParamCount: 1,
	})
}

// Query multi-linha (spec §72): quebras e indentação não podem vazar para
// nomes nem deslocar posições.
func TestQueryMultilinha(t *testing.T) {
	const sql = "SELECT service_id, ip, nas\nFROM radius_sessions\nWHERE status = ?\n  AND nas = ?\nLIMIT 100"
	st := parseOK(t, sql)

	if st.ParamCount != 2 {
		t.Fatalf("ParamCount = %d, want 2", st.ParamCount)
	}
	and, ok := st.Where.(*parser.LogicalExpr)
	if !ok || and.Op != parser.LogicalAnd {
		t.Fatalf("Where = %#v, want AND", st.Where)
	}
	left, ok := and.Left.(*parser.Comparison)
	if !ok || left.Column != "status" || left.Operand.ParamIdx != 0 {
		t.Errorf("esquerda = %#v", and.Left)
	}
	right, ok := and.Right.(*parser.Comparison)
	if !ok || right.Column != "nas" || right.Operand.ParamIdx != 1 {
		t.Errorf("direita = %#v", and.Right)
	}
	if got := sql[st.DatasetPos : st.DatasetPos+len(st.Dataset)]; got != st.Dataset {
		t.Errorf("DatasetPos aponta para %q, want %q", got, st.Dataset)
	}
}

// Precedência e associatividade são codificadas na FORMA da árvore — o
// planner conta com isso ao achatar a espinha à esquerda.
func TestPrecedenciaEAssociatividade(t *testing.T) {
	t.Run("and liga mais que or", func(t *testing.T) {
		st := parseOK(t, "SELECT a FROM x WHERE a = 1 OR b = 2 AND c = 3")
		root, ok := st.Where.(*parser.LogicalExpr)
		if !ok || root.Op != parser.LogicalOr {
			t.Fatalf("raiz = %#v, want OR", st.Where)
		}
		if _, ok := root.Left.(*parser.Comparison); !ok {
			t.Errorf("esquerda do OR = %#v, want Comparison", root.Left)
		}
		right, ok := root.Right.(*parser.LogicalExpr)
		if !ok || right.Op != parser.LogicalAnd {
			t.Errorf("direita do OR = %#v, want AND", root.Right)
		}
	})

	t.Run("parenteses invertem", func(t *testing.T) {
		st := parseOK(t, "SELECT a FROM x WHERE (a = 1 OR b = 2) AND c = 3")
		root, ok := st.Where.(*parser.LogicalExpr)
		if !ok || root.Op != parser.LogicalAnd {
			t.Fatalf("raiz = %#v, want AND", st.Where)
		}
		if left, ok := root.Left.(*parser.LogicalExpr); !ok || left.Op != parser.LogicalOr {
			t.Errorf("esquerda do AND = %#v, want OR", root.Left)
		}
	})

	// Left-leaning: recursão à direita produziria AND(a, AND(b,c)) e quebraria
	// o flatten do planner.
	for _, tc := range []struct {
		nome string
		sql  string
		op   parser.LogicalOp
	}{
		{"and", "SELECT a FROM x WHERE a=1 AND b=2 AND c=3", parser.LogicalAnd},
		{"or", "SELECT a FROM x WHERE a=1 OR b=2 OR c=3", parser.LogicalOr},
	} {
		t.Run("left-leaning/"+tc.nome, func(t *testing.T) {
			st := parseOK(t, tc.sql)
			root, ok := st.Where.(*parser.LogicalExpr)
			if !ok || root.Op != tc.op {
				t.Fatalf("raiz = %#v", st.Where)
			}
			if _, ok := root.Left.(*parser.LogicalExpr); !ok {
				t.Errorf("esquerda = %#v, want conectivo (árvore à esquerda)", root.Left)
			}
			if _, ok := root.Right.(*parser.Comparison); !ok {
				t.Errorf("direita = %#v, want Comparison (árvore à esquerda)", root.Right)
			}
		})
	}
}

// Parênteses não existem na AST: são só precedência.
func TestParentesesRedundantesNaoAparecemNaArvore(t *testing.T) {
	plain := parseOK(t, "SELECT a FROM x WHERE a = 1")
	withParens := parseOK(t, "SELECT a FROM x WHERE ((a = 1))")
	// As posições diferem (o literal está deslocado), então comparamos a
	// forma do nó, não a árvore inteira.
	plainCmp := plain.Where.(*parser.Comparison)
	parensCmp := withParens.Where.(*parser.Comparison)
	if plainCmp.Column != parensCmp.Column || plainCmp.Op != parensCmp.Op || plainCmp.Operand.Kind != parensCmp.Operand.Kind || plainCmp.Operand.I != parensCmp.Operand.I {
		t.Errorf("árvores diferentes:\n %#v\n %#v", plainCmp, parensCmp)
	}
}

func TestComparadores(t *testing.T) {
	cases := map[string]parser.CmpOp{
		"=":  parser.CmpEq,
		"!=": parser.CmpNe,
		">":  parser.CmpGt,
		">=": parser.CmpGte,
		"<":  parser.CmpLt,
		"<=": parser.CmpLte,
	}
	for op, want := range cases {
		t.Run(op, func(t *testing.T) {
			st := parseOK(t, "SELECT a FROM x WHERE a "+op+" 1")
			c, ok := st.Where.(*parser.Comparison)
			if !ok {
				t.Fatalf("Where = %#v", st.Where)
			}
			if c.Op != want {
				t.Errorf("Op = %v, want %v", c.Op, want)
			}
			if got := c.Op.String(); got != op {
				t.Errorf("CmpOp.String() = %q, want %q", got, op)
			}
		})
	}
}

func TestIsNull(t *testing.T) {
	st := parseOK(t, "SELECT a FROM x WHERE a IS NULL")
	if nc, ok := st.Where.(*parser.NullCheck); !ok || nc.Not || nc.Column != "a" {
		t.Errorf("Where = %#v, want NullCheck{a, Not:false}", st.Where)
	}
	st = parseOK(t, "SELECT a FROM x WHERE a IS NOT NULL")
	if nc, ok := st.Where.(*parser.NullCheck); !ok || !nc.Not {
		t.Errorf("Where = %#v, want NullCheck{Not:true}", st.Where)
	}
}

func TestOperandos(t *testing.T) {
	cases := []struct {
		lit  string
		want parser.Operand
	}{
		{"'x'", parser.Operand{Kind: parser.OperandString, S: "x"}},
		{"42", parser.Operand{Kind: parser.OperandInt, I: 42}},
		{"-42", parser.Operand{Kind: parser.OperandInt, I: -42}},
		{"0", parser.Operand{Kind: parser.OperandInt, I: 0}},
		{"1.5", parser.Operand{Kind: parser.OperandFloat, F: 1.5}},
		{"-1.5", parser.Operand{Kind: parser.OperandFloat, F: -1.5}},
		// Expoente sem ponto ainda é float: é o que a gramática do lexer diz.
		{"1e3", parser.Operand{Kind: parser.OperandFloat, F: 1000}},
		{"2.5E-3", parser.Operand{Kind: parser.OperandFloat, F: 0.0025}},
		{"TRUE", parser.Operand{Kind: parser.OperandBool, B: true}},
		{"false", parser.Operand{Kind: parser.OperandBool, B: false}},
		{"?", parser.Operand{Kind: parser.OperandParam, ParamIdx: 0}},
		{"9223372036854775807", parser.Operand{Kind: parser.OperandInt, I: 9223372036854775807}},
	}
	for _, tc := range cases {
		t.Run(tc.lit, func(t *testing.T) {
			const prefix = "SELECT a FROM x WHERE a = "
			st := parseOK(t, prefix+tc.lit)
			c := st.Where.(*parser.Comparison)
			tc.want.Pos = len(prefix)
			if c.Operand != tc.want {
				t.Errorf("Operand = %#v, want %#v", c.Operand, tc.want)
			}
		})
	}
}

// A numeração é léxica e atribuída num único ponto: contar em dois lugares
// abriria buracos.
func TestPlaceholdersNumeradosEmOrdemLexica(t *testing.T) {
	st := parseOK(t, "SELECT a FROM x WHERE (a = ? OR b = ?) AND c = ?")
	if st.ParamCount != 3 {
		t.Fatalf("ParamCount = %d, want 3", st.ParamCount)
	}
	var idx []int
	var walk func(parser.Expr)
	walk = func(e parser.Expr) {
		switch n := e.(type) {
		case *parser.LogicalExpr:
			walk(n.Left)
			walk(n.Right)
		case *parser.Comparison:
			if n.Operand.Kind == parser.OperandParam {
				idx = append(idx, n.Operand.ParamIdx)
			}
		}
	}
	walk(st.Where)
	if !reflect.DeepEqual(idx, []int{0, 1, 2}) {
		t.Errorf("ParamIdx em ordem in-order = %v, want [0 1 2]", idx)
	}
}

func TestIdentificadoresQuoted(t *testing.T) {
	st := parseOK(t, `SELECT "Acct-Status-Type" FROM "radius-sessions" WHERE "Acct-Status-Type" = 'Stop'`)
	if st.Projection[0].Name != "Acct-Status-Type" {
		t.Errorf("coluna = %q", st.Projection[0].Name)
	}
	if st.Dataset != "radius-sessions" {
		t.Errorf("dataset = %q", st.Dataset)
	}
	c := st.Where.(*parser.Comparison)
	if c.Column != "Acct-Status-Type" || c.Operand.S != "Stop" {
		t.Errorf("predicado = %#v", c)
	}
}

func TestEscapesChegamResolvidos(t *testing.T) {
	st := parseOK(t, `SELECT "a""b" FROM x`)
	if st.Projection[0].Name != `a"b` {
		t.Errorf("coluna = %q, want %q", st.Projection[0].Name, `a"b`)
	}
	st = parseOK(t, `SELECT a FROM x WHERE u = 'O''Brien'`)
	if got := st.Where.(*parser.Comparison).Operand.S; got != "O'Brien" {
		t.Errorf("literal = %q, want %q", got, "O'Brien")
	}
}

func TestKeywordsCaseInsensitiveIdentCaseSensitive(t *testing.T) {
	st := parseOK(t, "select * from x where a is not null limit 1")
	if !st.Star || !st.HasLimit || st.Limit != 1 {
		t.Errorf("stmt = %#v", st)
	}
	// Identificador preserva a caixa: o binder resolve nome exato.
	st = parseOK(t, "SELECT Status FROM X WHERE Status = 'a'")
	if st.Projection[0].Name != "Status" || st.Dataset != "X" {
		t.Errorf("nomes normalizados: %#v / %q", st.Projection, st.Dataset)
	}
}

func TestPontoEVirgulaFinal(t *testing.T) {
	withoutSemicolon := parseOK(t, "SELECT * FROM x")
	for _, sql := range []string{"SELECT * FROM x;", "SELECT * FROM x ;"} {
		withSemicolon := parseOK(t, sql)
		if !reflect.DeepEqual(withSemicolon, withoutSemicolon) {
			t.Errorf("%q produziu AST diferente da versão sem ';'", sql)
		}
	}
}

// LIMIT 0 é distinto de LIMIT ausente: um pede zero linhas, o outro não pede
// nada.
func TestLimitZeroNaoEhAusente(t *testing.T) {
	st := parseOK(t, "SELECT * FROM x LIMIT 0")
	if !st.HasLimit || st.Limit != 0 {
		t.Errorf("HasLimit=%v Limit=%d, want true/0", st.HasLimit, st.Limit)
	}
	st = parseOK(t, "SELECT * FROM x")
	if st.HasLimit {
		t.Errorf("HasLimit = true sem LIMIT na query")
	}
}

func TestPalavraReservadaComoColunaEntreAspas(t *testing.T) {
	st := parseOK(t, `SELECT "limit" FROM x`)
	if st.Projection[0].Name != "limit" {
		t.Errorf("coluna = %q", st.Projection[0].Name)
	}
}

func TestColunasRepetidasSaoAceitas(t *testing.T) {
	st := parseOK(t, "SELECT a, a FROM x")
	if len(st.Projection) != 2 {
		t.Errorf("%d colunas, want 2 (dedup seria surpresa silenciosa)", len(st.Projection))
	}
}

// Parse é puro: mesma entrada, mesma árvore, e seguro entre goroutines.
func TestParseConcorrente(t *testing.T) {
	const sql = "SELECT id, ip FROM s WHERE (a = ? OR b = 'x') AND c >= 1.5 LIMIT 10"
	ref := parseOK(t, sql)

	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 50; j++ {
				got, err := parser.Parse(sql)
				if err != nil {
					t.Errorf("Parse: %v", err)
					return
				}
				if !reflect.DeepEqual(got, ref) {
					t.Errorf("AST divergente entre execuções")
					return
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}

func TestAninhamentoNoLimite(t *testing.T) {
	openParens := ""
	closeParens := ""
	for i := 0; i < parser.MaxExprDepth; i++ {
		openParens += "("
		closeParens += ")"
	}
	if _, err := parser.Parse("SELECT a FROM x WHERE " + openParens + "a = 1" + closeParens); err != nil {
		t.Errorf("%d níveis deveriam ser aceitos: %v", parser.MaxExprDepth, err)
	}
}

// A profundidade precisa ser DECREMENTADA ao fechar o parêntese: sem isso,
// muitos grupos irmãos rasos estouram o limite como se fossem aninhados.
func TestGruposIrmaosNaoAcumulamProfundidade(t *testing.T) {
	sql := "SELECT a FROM x WHERE (a = 1)"
	for i := 0; i < parser.MaxExprDepth+10; i++ {
		sql += " AND (b = 2)"
	}
	if _, err := parser.Parse(sql); err != nil {
		t.Errorf("grupos irmãos não deveriam acumular profundidade: %v", err)
	}
}
