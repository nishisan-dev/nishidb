package query_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestRevalidacaoDeCandidatoDoIndiceHash é o gêmeo do
// TestRevalidacaoDeIndiceStale para o segundo motor, e o motivo de ele existir
// separado é o CONTRATO do índice hash, não a simetria.
//
// O bitmap devolve candidato a mais só por posting órfã (falha de Clear
// tolerada, §14.16). O hash devolve candidato a mais também por COLISÃO: ele
// endereça o slot por um hash de 64 bits e, por decisão de projeto, NÃO valida
// a colisão lendo o record — é o que elimina o ValidateFunc reentrante e o
// risco de deadlock com a reconstrução que o Store documenta para o primary. O
// preço dessa decisão é exatamente este candidato a mais, e quem o paga é a
// revalidação do §44.
//
// Um teste que só afirmasse "resultado vazio" passaria também com o índice
// devolvendo nada — isto é, passaria com o índice quebrado. Candidates=1 com
// Rejected=1 prova que o candidato FOI consultado e FOI recusado.
func TestRevalidacaoDeCandidatoDoIndiceHash(t *testing.T) {
	h := novoHarness(t)
	h.ingere(`{"id":"CMP-0001","_SEQ":1,"NAS-Identifier":"core-01","User-Name":"joao"}`)

	nas, ok := h.sch.FieldByName([]byte("NAS-Identifier"))
	if !ok {
		t.Fatal("NAS-Identifier não existe no fixture")
	}
	// O candidato a mais: o índice passa a responder por um valor que a row
	// não tem. É o que uma colisão de hash produz, e é o que a fase de Clear
	// deixa para trás quando falha.
	if err := h.store.InjectPosting(nas.ID,
		types.Value{Type: types.TypeString, Bytes: []byte("core-99")}, h.rowID("CMP-0001")); err != nil {
		t.Fatal(err)
	}

	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE nas = 'core-99'`)
	if len(rows) != 0 {
		t.Fatalf("resultado = %v, want vazio: a row corrente tem nas='core-01'", rows)
	}

	m := h.engine.Metrics()
	if got := m.Candidates.Load(); got != 1 {
		t.Errorf("Candidates = %d, want 1: o índice hash devolveu o candidato", got)
	}
	if got := m.Rejected.Load(); got != 1 {
		t.Errorf("Rejected = %d, want 1: o predicado foi reavaliado e recusou", got)
	}
	if got := m.Emitted.Load(); got != 0 {
		t.Errorf("Emitted = %d, want 0", got)
	}
	// E o plano foi mesmo o indexado: com FullScan a asserção acima passaria
	// pelo motivo errado — a varredura também não emitiria a row.
	if got := h.engine.PlanMetrics().HashIndex.Load(); got != 1 {
		t.Errorf("query.hash_index = %d, want 1: a query não usou o índice", got)
	}
}
