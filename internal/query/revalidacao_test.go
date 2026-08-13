package query_test

import (
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// O teste mais importante do executor (spec §44).
//
// O índice devolve um SUPERSET por construção: o storage tolera posting órfão
// (§14.16 — falha de Clear é contabilizada e o posting fica). Se o executor
// confiasse no índice, a query devolveria uma linha que NÃO satisfaz o filtro,
// e o cliente não teria como saber.
//
// As métricas são parte da asserção, e não enfeite: um teste que só afira
// "resultado vazio" passa também quando o índice simplesmente não devolveu o
// candidato — isto é, passa com o índice quebrado, provando nada. Candidates=1
// com Rejected=1 prova que o candidato FOI consultado e FOI rejeitado.
func TestRevalidacaoDeIndiceStale(t *testing.T) {
	h := novoHarness(t)
	h.ingere(`{"id":"CMP-0001","_SEQ":1,"Acct-Status-Type":"Start","User-Name":"joao"}`)

	status, _ := h.sch.FieldByName([]byte("Acct-Status-Type"))
	// Posting mentiroso: o índice passa a dizer que a row tem status 'Stop'.
	if err := h.store.InjectPosting(status.ID,
		types.Value{Type: types.TypeString, Bytes: []byte("Stop")}, h.rowID("CMP-0001")); err != nil {
		t.Fatal(err)
	}

	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE status = 'Stop'`)
	if len(rows) != 0 {
		t.Fatalf("resultado = %v, want vazio: a row corrente é 'Start'", rows)
	}

	m := h.engine.Metrics()
	if got := m.Candidates.Load(); got != 1 {
		t.Errorf("Candidates = %d, want 1: o índice devolveu o candidato", got)
	}
	if got := m.Rejected.Load(); got != 1 {
		t.Errorf("Rejected = %d, want 1: o predicado foi reavaliado e recusou", got)
	}
	if got := m.Emitted.Load(); got != 0 {
		t.Errorf("Emitted = %d, want 0", got)
	}
}

// O point lookup revalida como qualquer outro plano. É o caso onde a tentação
// de "achei pela key, devolvo" é maior — e onde ela produziria uma linha que
// não satisfaz o resto do WHERE.
func TestPointLookupTambemRevalida(t *testing.T) {
	h := novoHarness(t)
	h.ingere(`{"id":"CMP-0001","_SEQ":1,"Acct-Status-Type":"Start","User-Name":"joao"}`)

	rows := h.consultaOK(
		`SELECT id FROM radius_sessions WHERE id = 'CMP-0001' AND status = 'Stop'`)
	if len(rows) != 0 {
		t.Fatalf("resultado = %v, want vazio", rows)
	}
	if got := h.engine.Metrics().Rejected.Load(); got != 1 {
		t.Errorf("Rejected = %d, want 1", got)
	}

	// E o predicado da PRÓPRIA key é reavaliado: com a key certa, a row sai.
	rows = h.consultaOK(`SELECT id FROM radius_sessions WHERE id = 'CMP-0001'`)
	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0001"}) {
		t.Fatalf("resultado = %v", got)
	}
}

// A liveness do POINT LOOKUP tem de ser honrada pelo fetch, e não por
// acidente do predicado.
//
// Sem a asserção de métricas este teste passa mesmo com a liveness desligada,
// e pelo motivo errado: o GetByKey de uma row morta devolve Record{} com
// payload vazio, todos os slots ficam ausentes, e o predicado `id = 'X'` dá
// falso. É a revalidação da §44 cobrindo o buraco da liveness — coincidência
// estrutural (todo PointLookupPlan tem `key = literal` como conjunct), não
// invariante. DeadSkipped=1 com Rejected=0 é o que separa os dois.
func TestPointLookupHonraLiveness(t *testing.T) {
	h := novoHarness(t)
	h.ingere(`{"id":"CMP-0001","_SEQ":1,"Acct-Status-Type":"Stop"}`)
	h.store.SetTombstone(h.rowID("CMP-0001"))

	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE id = 'CMP-0001'`)
	if len(rows) != 0 {
		t.Fatalf("resultado = %v, want vazio: a row está morta", rows)
	}
	m := h.engine.Metrics()
	if got := m.DeadSkipped.Load(); got != 1 {
		t.Errorf("DeadSkipped = %d, want 1: a row foi pulada pela LIVENESS", got)
	}
	if got := m.Rejected.Load(); got != 0 {
		t.Errorf("Rejected = %d, want 0: se a row chegou ao predicado, a liveness não a filtrou", got)
	}
}

// Candidato tombstoned: o índice o devolve (as postings não são limpas), o
// fetch reporta morto, e ele não conta como row. A liveness é do FETCH.
func TestCandidatoTombstonedNaoSai(t *testing.T) {
	h := novoHarness(t)
	h.ingere(
		`{"id":"CMP-0001","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		`{"id":"CMP-0002","_SEQ":1,"Acct-Status-Type":"Stop"}`,
	)
	h.store.SetTombstone(h.rowID("CMP-0001"))

	rows := h.consultaOK(`SELECT id FROM radius_sessions WHERE status = 'Stop'`)
	if got := chaves(rows, "id"); !reflect.DeepEqual(got, []string{"CMP-0002"}) {
		t.Fatalf("resultado = %v, want só a row viva", got)
	}
	m := h.engine.Metrics()
	if got := m.DeadSkipped.Load(); got != 1 {
		t.Errorf("DeadSkipped = %d, want 1", got)
	}
	if got := m.Candidates.Load(); got != 2 {
		t.Errorf("Candidates = %d, want 2: a row morta FOI consumida da fonte", got)
	}
}
