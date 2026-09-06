package binder_test

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// sqlComTermos monta `SELECT id FROM radius_sessions WHERE status='Stop' AND …`
// com n termos, como texto — para exercitar o TETO do parser.
func sqlComTermos(n int) string {
	partes := make([]string, n)
	for i := range partes {
		partes[i] = "status = 'Stop'"
	}
	return "SELECT id FROM radius_sessions WHERE " + strings.Join(partes, " AND ")
}

// O teto de termos é de RECURSO, e a fronteira precisa ser exata: um teto que
// recusasse no limite declarado quebraria a query que o cliente montou para
// caber nele.
func TestTetoDeTermosDoWhere(t *testing.T) {
	if _, err := parser.Parse(sqlComTermos(parser.MaxWhereTerms)); err != nil {
		t.Errorf("%d termos (o teto exato) foram recusados: %v", parser.MaxWhereTerms, err)
	}

	_, err := parser.Parse(sqlComTermos(parser.MaxWhereTerms + 1))
	if err == nil {
		t.Fatalf("%d termos deveriam ser recusados", parser.MaxWhereTerms+1)
	}
	if !errors.Is(err, parser.ErrSyntax) {
		t.Errorf("erro = %v, want ErrSyntax", err)
	}
	if !strings.Contains(err.Error(), "10000 termos") {
		t.Errorf("mensagem = %q, want citar o teto", err)
	}
}

// O custo por termo é o que justifica o teto — medido, não estimado. O número
// entra no comentário de MaxWhereTerms; se ele mudar de ordem de grandeza, o
// teto precisa ser reavaliado.
func TestCustoDeMemoriaPorTermo(t *testing.T) {
	const termos = parser.MaxWhereTerms
	st, err := parser.Parse(sqlComTermos(termos))
	if err != nil {
		t.Fatal(err)
	}

	var antes, depois runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&antes)
	bq, err := binder.Bind(st, compilar(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&depois)
	runtime.KeepAlive(bq)

	total := depois.TotalAlloc - antes.TotalAlloc
	porTermo := float64(total) / termos
	t.Logf("no teto (%d termos): %.0f B/termo, %.1f MB por query", termos, porTermo,
		float64(total)/(1<<20))
	if porTermo > 512 {
		t.Errorf("%.0f B/termo: o custo por termo subiu, o teto precisa ser reavaliado", porTermo)
	}
}

// O binder é PÚBLICO e aceita uma árvore montada à mão, que não passa pelo
// teto do parser. Com a espinha iterativa, 1 milhão de termos custa memória e
// nada mais — antes da correção, derrubava o processo com um fatal error do
// runtime, que nem recover() captura.
func TestUmMilhaoDeTermosNaoDerrubaOProcesso(t *testing.T) {
	if testing.Short() {
		t.Skip("monta uma AST de 1 milhão de nós")
	}
	bq, err := binder.Bind(cadeiaLonga(1_000_000), compilar(t), nil)
	if err != nil {
		t.Fatalf("bind de 1 milhão de termos: %v", err)
	}
	if len(bq.Slots.IDs) != 2 { // id (projeção) + status (WHERE)
		t.Errorf("Slots = %v", bq.Slots.IDs)
	}
}
