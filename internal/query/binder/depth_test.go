package binder_test

import (
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// longChain monta a AST de `status='Stop' AND status='Stop' AND …` com n
// termos, DIRETAMENTE — sem passar pelo parser.
//
// Construir à mão é deliberado: o parser tem um teto de termos (recurso), e
// amarrar estes testes a ele faria a propriedade do binder — percorrer a
// espinha com pilha constante — deixar de ser exercitada acima do teto. O Bind
// é público e aceita uma árvore que qualquer chamador monta, então esta é a
// entrada realista para o pior caso.
func longChain(n int) *parser.SelectStmt {
	leaf := func() parser.Expr {
		return &parser.Comparison{
			Column: "status", Op: parser.CmpEq,
			Operand: parser.Operand{Kind: parser.OperandString, S: "Stop"},
		}
	}
	var e parser.Expr = leaf()
	for i := 1; i < n; i++ {
		e = &parser.LogicalExpr{Op: parser.LogicalAnd, Left: e, Right: leaf()}
	}
	return &parser.SelectStmt{
		Projection: []parser.ColumnRef{{Name: "id"}},
		Dataset:    "radius_sessions",
		Where:      e,
	}
}

// Uma query gerada por máquina (IN expandido, filtro montado por UI) chega a
// dezenas de milhares de termos sem intenção nenhuma. O parser aceita — a
// espinha de AND é iterativa lá —, então o binder também precisa aceitar, com
// pilha constante. Recursão aqui seria um frame por termo.
func TestBindCadeiaLongaComPilhaConstante(t *testing.T) {
	const terms = 50_000
	bq, err := binder.Bind(longChain(terms), compile(t), nil)
	if err != nil {
		t.Fatalf("bind de %d termos: %v", terms, err)
	}

	// A árvore ligada tem a mesma height da do parser, e é left-leaning: é o
	// formato que o flatten do planner assume.
	height, e := 0, bq.Where
	for {
		l, ok := e.(*binder.BoundLogical)
		if !ok {
			break
		}
		if _, leaf := l.Right.(*binder.BoundComparison); !leaf {
			t.Fatalf("filho direito = %T, want folha", l.Right)
		}
		height++
		e = l.Left
	}
	if height != terms-1 {
		t.Errorf("altura = %d, want %d", height, terms-1)
	}
	// Um único slot: os 50 mil termos são sobre a mesma coluna.
	if len(bq.Slots.IDs) != 2 { // id (projeção) + status (WHERE)
		t.Errorf("Slots = %v, want 2 campos", bq.Slots.IDs)
	}
}

// O teste acima prova que a cadeia longa é ACEITA, mas não que ela custa
// pilha constante — Go cresce a pilha até 1 GB e a versão recursiva passaria
// também, só que gastando um frame por termo.
//
// Este prova o custo: roda o mesmo bind num processo filho com a pilha máxima
// limitada a 4 MiB. Com a espinha iterativa sobra folga; com um frame por
// termo, 50 mil termos estouram e o runtime mata o processo.
func TestBindCadeiaLongaNaoUsaPilhaProporcional(t *testing.T) {
	const terms = 50_000
	if os.Getenv("NISHIDB_BIND_STACK_CHILD") == "1" {
		debug.SetMaxStack(4 << 20)
		if _, err := binder.Bind(longChain(terms), compile(t), nil); err != nil {
			t.Fatal(err)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestBindCadeiaLongaNaoUsaPilhaProporcional$", "-test.v")
	cmd.Env = append(os.Environ(), "NISHIDB_BIND_STACK_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bind de %d termos não coube em 4 MiB de pilha: %v\n%s", terms, err, output)
	}
}

// A ORDEM de visita não pode mudar com o percurso iterativo: é ela que decide
// qual erro o usuário vê primeiro numa query com vários problemas.
func TestBindReportaOPrimeiroErroDaOrdemLexica(t *testing.T) {
	sql := "SELECT id FROM radius_sessions WHERE naoexiste = 'a' AND tambemnao = 'b'"
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	_, err = binder.Bind(st, compile(t), nil)
	if err == nil {
		t.Fatal("bind deveria falhar")
	}
	if !strings.Contains(err.Error(), "naoexiste") {
		t.Errorf("erro = %q, want reportar a PRIMEIRA coluna inválida", err)
	}
}

// O teto do lado direito é de RECURSO e só é alcançável com AST montada à
// mão: pelo parser, aprofundar à direita exige parênteses, limitados a 64.
// Sem ele, uma árvore adversarial derrubaria o processo em vez de devolver
// erro.
func TestBindRecusaProfundidadeAbsurdaADireita(t *testing.T) {
	leaf := func() parser.Expr {
		return &parser.Comparison{
			Column: "status", Op: parser.CmpEq,
			Operand: parser.Operand{Kind: parser.OperandString, S: "Stop"},
		}
	}
	var e parser.Expr = leaf()
	for i := 0; i < binder.MaxBindDepth+2; i++ {
		e = &parser.LogicalExpr{Op: parser.LogicalAnd, Left: leaf(), Right: e}
	}
	st := &parser.SelectStmt{
		Projection: []parser.ColumnRef{{Name: "id"}},
		Dataset:    "radius_sessions",
		Where:      e,
	}

	_, err := binder.Bind(st, compile(t), []types.Value(nil))
	if err == nil {
		t.Fatal("profundidade acima do teto deveria falhar")
	}
	if !strings.Contains(err.Error(), "aninhada demais") {
		t.Errorf("erro = %q, want mensagem de aninhamento", err)
	}

	// Logo ABAIXO do teto ainda passa: o teto é de recurso, não uma recusa
	// preguiçosa de árvore grande.
	e = leaf()
	for i := 0; i < binder.MaxBindDepth-2; i++ {
		e = &parser.LogicalExpr{Op: parser.LogicalAnd, Left: leaf(), Right: e}
	}
	st.Where = e
	if _, err := binder.Bind(st, compile(t), nil); err != nil {
		t.Errorf("profundidade dentro do teto falhou: %v", err)
	}
}
