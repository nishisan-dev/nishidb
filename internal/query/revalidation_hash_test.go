package query_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestRevalidacaoDeCandidatoDoIndiceHash nasceu como o gêmeo do
// TestRevalidacaoDeIndiceStale para o segundo motor GLOBAL: o hash endereçava
// o slot por um hash de 64 bits e, por decisão de projeto, não validava a
// colisão lendo o record, e esse candidato a mais era pago pela revalidação
// do §44 — distinto da posting órfã que já bastava para o bitmap.
//
// O motor que sustentava essa distinção saiu (Tarefa 9 — índice por segmento
// único), e a D5 aposentou a grafia que a declarava: `nas` (NAS-Identifier)
// passou a compilar para o mesmo estado que `status` — era o mesmo IndexKind
// (IndexBitmap), hoje o mesmo bool — e o candidato a mais que este teste injeta
// com InjectPosting representa a MESMA
// causa do bitmap (posting que sobrevive à mudança de valor, não colisão de
// hash). O nome ficou histórico; o que o teste continua provando — e que o
// gêmeo não prova sozinho — é que Candidates=1/Rejected=1 vêm acompanhados do
// PLANO INDEXADO de fato escolhido (linha final), e não de um FullScan que
// coincidentemente devolveria o mesmo vazio.
func TestRevalidacaoDeCandidatoDoIndiceHash(t *testing.T) {
	h := newHarness(t)
	h.ingere(`{"id":"CMP-0001","_SEQ":1,"NAS-Identifier":"core-01","User-Name":"joao"}`)

	nas, ok := h.sch.FieldByName([]byte("NAS-Identifier"))
	if !ok {
		t.Fatal("NAS-Identifier não existe no fixture")
	}
	// O candidato a mais: o índice passa a responder por um valor que a row
	// não tem. É o estado NORMAL depois de um update — o índice por segmento
	// não tem fase de Clear (spec §5.2), então a posting do valor anterior
	// fica onde está até o segmento ser aposentado.
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
		t.Errorf("Candidates = %d, want 1: o índice devolveu o candidato", got)
	}
	if got := m.Rejected.Load(); got != 1 {
		t.Errorf("Rejected = %d, want 1: o predicado foi reavaliado e recusou", got)
	}
	if got := m.Emitted.Load(); got != 0 {
		t.Errorf("Emitted = %d, want 0", got)
	}
	// E o plano foi mesmo o indexado: com FullScan a asserção acima passaria
	// pelo motivo errado — a varredura também não emitiria a row.
	if got := h.engine.PlanMetrics().IndexScan.Load(); got != 1 {
		t.Errorf("contador de index scan = %d, want 1: a query não usou o índice", got)
	}
}
