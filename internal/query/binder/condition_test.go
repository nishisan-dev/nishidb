package binder_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func conditionOK(t *testing.T, sql string) *binder.BoundCondition {
	t.Helper()
	e, err := parser.ParseCondition(sql)
	if err != nil {
		t.Fatalf("ParseCondition(%q): %v", sql, err)
	}
	bc, err := binder.BindCondition(e, compile(t))
	if err != nil {
		t.Fatalf("BindCondition(%q): %v", sql, err)
	}
	return bc
}

func TestBindCondicaoResolveSlots(t *testing.T) {
	bc := conditionOK(t, "status = 'Stop' AND octets > 3")
	if bc.Slots.Len() != 2 {
		t.Fatalf("slots = %d, quero 2", bc.Slots.Len())
	}
	if bc.Slots.IDs[0] >= bc.Slots.IDs[1] {
		t.Error("os ids precisam sair ordenados e sem duplicata")
	}
	l, ok := bc.Expr.(*binder.BoundLogical)
	if !ok {
		t.Fatalf("raiz = %T, quero BoundLogical", bc.Expr)
	}
	left, ok := l.Left.(*binder.BoundComparison)
	if !ok {
		t.Fatalf("esquerda = %T", l.Left)
	}
	right := l.Right.(*binder.BoundComparison)
	if left.Slot == right.Slot {
		t.Error("as duas comparações ficaram no mesmo slot")
	}
	if s, _ := bc.Slots.Index(left.Field.ID); s != left.Slot {
		t.Errorf("slot da comparação = %d, o conjunto diz %d", left.Slot, s)
	}
}

// A condição é uma expressão SOLTA: qualquer sobra é erro, e não texto
// ignorado em silêncio — um `LIMIT 5` esquecido numa regra de TTL não pode
// virar uma regra que faz outra coisa.
func TestParseCondicaoRecusaTextoDepoisDaExpressao(t *testing.T) {
	for _, sql := range []string{"octets = 2 LIMIT 5", "octets = 2 xyz", "octets = 2; octets = 3"} {
		if _, err := parser.ParseCondition(sql); err == nil {
			t.Errorf("ParseCondition(%q) aceitou a sobra", sql)
		}
	}
}

func TestBindCondicaoRecusaParametro(t *testing.T) {
	e, err := parser.ParseCondition("octets = ?")
	if err != nil {
		t.Fatal(err)
	}
	_, err = binder.BindCondition(e, compile(t))
	if err == nil {
		t.Fatal("parâmetro posicional foi aceito numa condição, e não há quem o forneça")
	}
	if !strings.Contains(err.Error(), "parâmetro") {
		t.Errorf("mensagem = %q, precisa nomear o parâmetro", err)
	}
}

func TestBindCondicaoRecusaColunaInexistente(t *testing.T) {
	e, err := parser.ParseCondition("naoExiste = 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := binder.BindCondition(e, compile(t)); err == nil {
		t.Fatal("coluna inexistente foi aceita")
	}
}

// A condição passa pela MESMA coerção do WHERE: é essa identidade que garante
// que uma regra e uma consulta signifiquem a mesma coisa.
func TestBindCondicaoUsaAMesmaCoercaoDoWhere(t *testing.T) {
	bc := conditionOK(t, "octets > 1.5")
	cmp, ok := bc.Expr.(*binder.BoundComparison)
	if !ok {
		t.Fatalf("raiz = %T", bc.Expr)
	}
	if cmp.Kind != binder.CompareIntAsFloat {
		t.Errorf("kind = %v, quero CompareIntAsFloat (coluna int64 contra literal double)", cmp.Kind)
	}
}

// Literal string vive no arena do BoundCondition, e ele tem de sobreviver ao
// retorno: uma janela sobre buffer realocado devolveria bytes de outro literal.
func TestBindCondicaoMantemOsBytesDoLiteral(t *testing.T) {
	bc := conditionOK(t, "status = 'Stop' AND id = 'CMP-001'")
	l := bc.Expr.(*binder.BoundLogical)
	left := l.Left.(*binder.BoundComparison)
	right := l.Right.(*binder.BoundComparison)
	if string(left.Val.Bytes) != "Stop" || left.Val.Type != types.TypeString {
		t.Errorf("literal da esquerda = %q (%v)", left.Val.Bytes, left.Val.Type)
	}
	if string(right.Val.Bytes) != "CMP-001" {
		t.Errorf("literal da direita = %q", right.Val.Bytes)
	}
}
