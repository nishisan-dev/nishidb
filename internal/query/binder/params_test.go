package binder_test

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func wireString(s string) types.Value {
	return types.Value{Type: types.TypeString, Bytes: []byte(s)}
}
func wireInt(i int64) types.Value { return types.Value{Type: types.TypeInt64, I64: i} }
func wireBool(b bool) types.Value {
	v := types.Value{Type: types.TypeBool}
	if b {
		v.I64 = 1
	}
	return v
}
func wireFloat(f float64) types.Value {
	return types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(f))}
}

// Parâmetro é coerido DENTRO do bind, com a mesma tabela dos literais. É isso
// que permite ao planner ver `key = ?` como point lookup no plan-time, em vez
// de descobrir o valor só na execução.
func TestParametroCoeridoNoBind(t *testing.T) {
	casos := []struct {
		nome  string
		sql   string
		param types.Value
		kind  binder.CompareKind
		check func(t *testing.T, v types.Value)
	}{
		{
			nome: "string", sql: "SELECT id FROM radius_sessions WHERE status = ?",
			param: wireString("Stop"), kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeString || string(v.Bytes) != "Stop" {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			nome: "int64", sql: "SELECT id FROM radius_sessions WHERE octets >= ?",
			param: wireInt(1000), kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeInt64 || v.I64 != 1000 {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			nome: "double", sql: "SELECT id FROM radius_sessions WHERE ratio < ?",
			param: wireFloat(0.25), kind: binder.CompareFloatBits,
			check: func(t *testing.T, v types.Value) {
				if got := math.Float64frombits(uint64(v.I64)); got != 0.25 {
					t.Errorf("valor = %v", got)
				}
			},
		},
		{
			nome: "boolean", sql: "SELECT id FROM radius_sessions WHERE ativo = ?",
			param: wireBool(true), kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.Type != types.TypeBool || v.I64 != 1 {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			// Parâmetro string num campo ipv4 passa pelos MESMOS parsers da
			// ingestão: a query encontra o que foi gravado.
			nome: "ipv4 por string", sql: "SELECT id FROM radius_sessions WHERE nas_ip = ?",
			param: wireString("192.168.0.7"), kind: binder.CompareBytes,
			check: func(t *testing.T, v types.Value) {
				if !bytes.Equal(v.Bytes, []byte{192, 168, 0, 7}) {
					t.Errorf("bytes = %v", v.Bytes)
				}
			},
		},
		{
			nome: "timestamp por string", sql: "SELECT id FROM radius_sessions WHERE quando > ?",
			param: wireString("2026-08-12T21:00:00.000Z"), kind: binder.CompareInt,
			check: func(t *testing.T, v types.Value) {
				if v.I64 != 1786568400000 {
					t.Errorf("valor = %+v", v)
				}
			},
		},
		{
			nome: "int64 com param float", sql: "SELECT id FROM radius_sessions WHERE octets > ?",
			param: wireFloat(1.5), kind: binder.CompareIntAsFloat,
			check: func(t *testing.T, v types.Value) {
				if got := math.Float64frombits(uint64(v.I64)); got != 1.5 {
					t.Errorf("valor = %v", got)
				}
			},
		},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			bq := bindOK(t, tc.sql, tc.param)
			cmp, ok := bq.Where.(*binder.BoundComparison)
			if !ok {
				t.Fatalf("Where = %#v", bq.Where)
			}
			if !cmp.IsParam || cmp.ParamIdx != 0 {
				t.Errorf("IsParam=%v ParamIdx=%d", cmp.IsParam, cmp.ParamIdx)
			}
			if cmp.Kind != tc.kind {
				t.Errorf("Kind = %v, want %v", cmp.Kind, tc.kind)
			}
			// O executor não vê parâmetro pendente: o valor já está resolvido.
			if cmp.Val.Type == types.TypeNull {
				t.Error("Val vazio: o parâmetro não foi coerido no bind")
			}
			tc.check(t, cmp.Val)
		})
	}
}

func TestParametrosMultiplosEmOrdem(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE status = ? AND octets > ? AND ativo = ?",
		wireString("Stop"), wireInt(10), wireBool(false))

	var idx []int
	var vals []types.Value
	var walk func(binder.BoundExpr)
	walk = func(e binder.BoundExpr) {
		switch n := e.(type) {
		case *binder.BoundLogical:
			walk(n.Left)
			walk(n.Right)
		case *binder.BoundComparison:
			if n.IsParam {
				idx = append(idx, n.ParamIdx)
				vals = append(vals, n.Val)
			}
		}
	}
	walk(bq.Where)

	if len(idx) != 3 {
		t.Fatalf("%d parâmetros ligados", len(idx))
	}
	for i, got := range idx {
		if got != i {
			t.Errorf("ParamIdx[%d] = %d", i, got)
		}
	}
	if string(vals[0].Bytes) != "Stop" || vals[1].I64 != 10 || vals[2].I64 != 0 {
		t.Errorf("valores = %+v", vals)
	}
}

// O chamador pode reutilizar o slice de params depois do bind: o valor é
// copiado, não aliasado.
func TestParametroStringEhCopiado(t *testing.T) {
	p := wireString("Stop")
	bq := bindOK(t, "SELECT id FROM radius_sessions WHERE status = ?", p)
	cmp := bq.Where.(*binder.BoundComparison)

	for i := range p.Bytes {
		p.Bytes[i] = 'X'
	}
	if string(cmp.Val.Bytes) != "Stop" {
		t.Errorf("valor = %q após mutar o param: o bind aliasou o slice do chamador", cmp.Val.Bytes)
	}
}

func TestErrosDeParametro(t *testing.T) {
	casos := []struct {
		nome   string
		sql    string
		params []types.Value
		msg    string
	}{
		{
			nome: "faltando", sql: "SELECT id FROM radius_sessions WHERE status = ? AND octets = ?",
			params: []types.Value{wireString("a")}, msg: "foram fornecidos 1",
		},
		{
			nome: "sobrando", sql: "SELECT id FROM radius_sessions WHERE status = ?",
			params: []types.Value{wireString("a"), wireInt(1)}, msg: "foram fornecidos 2",
		},
		{
			nome: "nenhum fornecido", sql: "SELECT id FROM radius_sessions WHERE status = ?",
			params: nil, msg: "a query tem 1 parâmetros",
		},
		{
			// NULL não tem semântica definida sem lógica trivalente: a
			// mensagem manda usar IS NULL, que é o que o usuário quer.
			nome: "null", sql: "SELECT id FROM radius_sessions WHERE status = ?",
			params: []types.Value{{Type: types.TypeNull}}, msg: "use IS NULL",
		},
		{
			// Um caminho de coerção só: representação já binária é recusada.
			nome: "tipo binario", sql: "SELECT id FROM radius_sessions WHERE nas_ip = ?",
			params: []types.Value{{Type: types.TypeIPv4, Bytes: []byte{10, 0, 0, 1}}},
			msg:    "não suportado (use string",
		},
		{
			nome: "tipo incompativel com a coluna", sql: "SELECT id FROM radius_sessions WHERE octets = ?",
			params: []types.Value{wireString("1000")}, msg: "não é comparável com literal do tipo",
		},
		{
			nome: "valor invalido para a coluna", sql: "SELECT id FROM radius_sessions WHERE nas_ip = ?",
			params: []types.Value{wireString("10.0.0.256")}, msg: "valor inválido para a coluna",
		},
		{
			nome: "operador restrito", sql: "SELECT id FROM radius_sessions WHERE ativo > ?",
			params: []types.Value{wireBool(true)}, msg: "não é permitido para a coluna",
		},
		{
			nome: "coluna nao comparavel", sql: "SELECT id FROM radius_sessions WHERE raw = ?",
			params: []types.Value{wireString("x")}, msg: "só admite IS NULL / IS NOT NULL",
		},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			err := bindErr(t, tc.sql, tc.params...)
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
		})
	}
}

// A numeração nas MENSAGENS é 1-based, porque é como o usuário conta os
// parâmetros; o ParamIdx interno é 0-based.
func TestMensagemDeParametroEh1Based(t *testing.T) {
	err := bindErr(t, "SELECT id FROM radius_sessions WHERE status = ? AND octets = ?",
		wireString("ok"), wireString("nao numerico"))
	if !strings.Contains(err.Error(), "parâmetro 2") {
		t.Errorf("erro = %q, want 'parâmetro 2' (1-based)", err)
	}
}

// Query sem parâmetro com params fornecidos também é erro de aridade.
func TestParametrosEmQuerySemPlaceholder(t *testing.T) {
	err := bindErr(t, "SELECT id FROM radius_sessions", wireInt(1))
	if !strings.Contains(err.Error(), "a query tem 0 parâmetros") {
		t.Errorf("erro = %q", err)
	}
}

// Bind com AST construída à mão: o LIMIT negativo é inalcançável pelo parser
// (que já recusa), mas o binder tem de continuar defendendo — é o caminho que
// um construtor de AST programático exercitaria.
func TestLimitNegativoEmASTConstruidaAMao(t *testing.T) {
	st := &parser.SelectStmt{
		Projection: []parser.ColumnRef{{Name: "id", Pos: 7}},
		Dataset:    "radius_sessions",
		HasLimit:   true,
		Limit:      -1,
		LimitPos:   30,
	}
	_, err := binder.Bind(st, compilar(t), nil)
	if err == nil {
		t.Fatal("LIMIT negativo deveria falhar")
	}
	if !strings.Contains(err.Error(), "LIMIT deve ser >= 0") {
		t.Errorf("erro = %q", err)
	}
}

// Invariante contra bug do PARSER: se ParamCount não bater com o número de
// comparações com '?', o bind acusa em vez de deixar um parâmetro fantasma.
func TestParamCountInconsistenteEhDetectado(t *testing.T) {
	st := &parser.SelectStmt{
		Projection: []parser.ColumnRef{{Name: "id", Pos: 7}},
		Dataset:    "radius_sessions",
		Where: &parser.Comparison{
			Column: "status", Op: parser.CmpEq,
			Operand: parser.Operand{Kind: parser.OperandString, S: "Stop"},
		},
		ParamCount: 1, // mente: não há '?' na árvore
	}
	_, err := binder.Bind(st, compilar(t), []types.Value{wireString("x")})
	if err == nil {
		t.Fatal("inconsistência de ParamCount deveria falhar")
	}
	if !strings.Contains(err.Error(), "parser inconsistente") {
		t.Errorf("erro = %q", err)
	}
}

// Where nil é o caso alcançável de árvore "vazia": query sem WHERE.
//
// Nota sobre o default do type switch (nó de AST desconhecido): ele é
// INALCANÇÁVEL de fora do pacote, porque parser.Expr é selada — o método
// isExpr() não é exportado, então nenhum pacote externo consegue construir um
// nó novo, nem em teste. A defesa fica no código como rede para o dia em que
// um quarto nó nascer DENTRO do parser: sem ela, o binder o ignoraria e
// produziria um WHERE que não filtra o que o usuário pediu.
func TestQuerySemWhereNaoTemArvore(t *testing.T) {
	bq := bindOK(t, "SELECT id FROM radius_sessions")
	if bq.Where != nil {
		t.Errorf("Where = %#v, want nil", bq.Where)
	}
	if bq.Slots.Len() != 1 {
		t.Errorf("Slots = %v, want só a coluna projetada", bq.Slots.IDs)
	}
}
