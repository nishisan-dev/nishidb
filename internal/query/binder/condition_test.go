package binder_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func condicaoOK(t *testing.T, sql string) *binder.BoundCondicao {
	t.Helper()
	e, err := parser.ParseCondition(sql)
	if err != nil {
		t.Fatalf("ParseCondition(%q): %v", sql, err)
	}
	bc, err := binder.BindCondicao(e, compilar(t))
	if err != nil {
		t.Fatalf("BindCondicao(%q): %v", sql, err)
	}
	return bc
}

func TestBindCondicaoResolveSlots(t *testing.T) {
	bc := condicaoOK(t, "status = 'Stop' AND octets > 3")
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
	esq, ok := l.Left.(*binder.BoundComparison)
	if !ok {
		t.Fatalf("esquerda = %T", l.Left)
	}
	dir := l.Right.(*binder.BoundComparison)
	if esq.Slot == dir.Slot {
		t.Error("as duas comparações ficaram no mesmo slot")
	}
	if s, _ := bc.Slots.Index(esq.Field.ID); s != esq.Slot {
		t.Errorf("slot da comparação = %d, o conjunto diz %d", esq.Slot, s)
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
	_, err = binder.BindCondicao(e, compilar(t))
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
	if _, err := binder.BindCondicao(e, compilar(t)); err == nil {
		t.Fatal("coluna inexistente foi aceita")
	}
}

// A condição passa pela MESMA coerção do WHERE: é essa identidade que garante
// que uma regra e uma consulta signifiquem a mesma coisa.
func TestBindCondicaoUsaAMesmaCoercaoDoWhere(t *testing.T) {
	bc := condicaoOK(t, "octets > 1.5")
	cmp, ok := bc.Expr.(*binder.BoundComparison)
	if !ok {
		t.Fatalf("raiz = %T", bc.Expr)
	}
	if cmp.Kind != binder.CompareIntAsFloat {
		t.Errorf("kind = %v, quero CompareIntAsFloat (coluna int64 contra literal double)", cmp.Kind)
	}
}

// Literal string vive no arena do BoundCondicao, e ele tem de sobreviver ao
// retorno: uma janela sobre buffer realocado devolveria bytes de outro literal.
func TestBindCondicaoMantemOsBytesDoLiteral(t *testing.T) {
	bc := condicaoOK(t, "status = 'Stop' AND id = 'CMP-001'")
	l := bc.Expr.(*binder.BoundLogical)
	esq := l.Left.(*binder.BoundComparison)
	dir := l.Right.(*binder.BoundComparison)
	if string(esq.Val.Bytes) != "Stop" || esq.Val.Type != types.TypeString {
		t.Errorf("literal da esquerda = %q (%v)", esq.Val.Bytes, esq.Val.Type)
	}
	if string(dir.Val.Bytes) != "CMP-001" {
		t.Errorf("literal da direita = %q", dir.Val.Bytes)
	}
}
