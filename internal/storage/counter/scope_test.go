package counter

import (
	"sync/atomic"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// cEligible é o campo do `scope` dos testes, depois dos quatro do dataset.
const cEligible types.FieldID = 5

// eligibleEvaluator é o `scope: eligible = true` sem passar pelo binder;
// campo ausente é fora do escopo, como no predicado.
type eligibleEvaluator struct{ fr encoding.FieldReader }

func (a *eligibleEvaluator) Evaluate(payload []byte) (bool, error) {
	a.fr.Reset(payload)
	for {
		id, v, ok := a.fr.Next()
		if !ok {
			return false, a.fr.Err()
		}
		if id == cEligible {
			return v.Type == types.TypeBool && v.I64 == 1, nil
		}
	}
}

// defByOLTWithScope é o contador por OLT com `where authenticated = true` e
// `scope eligible = true`. created conta as instâncias do avaliador do scope.
func defByOLTWithScope(name string, created *atomic.Int64) Definition {
	d := defByOLT(name, true, 100)
	d.Scope = "eligible = true"
	d.NewScopeEvaluator = func() Evaluator {
		if created != nil {
			created.Add(1)
		}
		return &eligibleEvaluator{}
	}
	return d
}

// scopedPayload é payload() com o campo eligible acrescentado no fim (o maior
// FieldID).
func scopedPayload(t testing.TB, olt string, port int64, auth, eligible bool) []byte {
	t.Helper()
	p := payloadBench([]byte(olt), 1, port, auth)
	v := types.Value{Type: types.TypeBool}
	if eligible {
		v.I64 = 1
	}
	p, err := encoding.AppendField(p, cEligible, v)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestOutOfScopeRowInternsNoGroupAndCountsNowhere: a linha fora do escopo é
// WithoutGroup — não interna grupo, não conta nos refs, no total nem nos
// agregados, e o dicionário não cresce.
func TestOutOfScopeRowInternsNoGroupAndCountsNowhere(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLTWithScope("spl", nil), false)
	if err := c.Transition(0, scopedPayload(t, "A", 1, true, false), false); err != nil {
		t.Fatal(err)
	}
	if c.Limit() != 1 {
		t.Fatalf("Limit = %d depois de uma linha fora do escopo, quero 1 — o grupo foi internado", c.Limit())
	}
	if e := c.entry(0); e != WithoutGroup {
		t.Fatalf("a entrada da linha fora do escopo é %#x, quero WithoutGroup", e)
	}
	if st := c.Stats(); st.Rows != 0 || st.Total != 0 || st.Groups != 0 {
		t.Fatalf("agregados depois da linha fora do escopo: rows %d total %d grupos %d, quero zero", st.Rows, st.Total, st.Groups)
	}
	// Dentro do escopo, a mesma OLT entra; uma segunda linha fora dela não
	// mexe no grupo que já existe.
	if err := c.Transition(1, scopedPayload(t, "A", 2, false, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(2, scopedPayload(t, "A", 3, true, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 1 {
		t.Fatalf("A: total %d refs %d, quero 0 e 1 — só a linha elegível é membro", total, refs)
	}
	requireCoherentAggregates(t, c)
}

// TestLeavingTheScopeDecrementsRowsAndTotalInTheSameTransition: o patch que
// torna `eligible = false` tira a linha do grupo na MESMA aplicação — rows e,
// como o bit estava ligado, total. Voltar ao escopo devolve as duas.
func TestLeavingTheScopeDecrementsRowsAndTotalInTheSameTransition(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLTWithScope("spl", nil), false)
	for r, auth := range []bool{true, false} {
		if err := c.Transition(types.RowID(r), scopedPayload(t, "A", int64(r), auth, true), false); err != nil {
			t.Fatal(err)
		}
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 2 {
		t.Fatalf("A antes: total %d refs %d, quero 1 e 2", total, refs)
	}
	if err := c.Transition(0, scopedPayload(t, "A", 0, true, false), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 1 {
		t.Fatalf("A depois de sair do escopo: total %d refs %d, quero 0 e 1", total, refs)
	}
	if e := c.entry(0); e != WithoutGroup {
		t.Fatalf("a linha que saiu do escopo ficou com a entrada %#x", e)
	}
	requireCoherentAggregates(t, c)
	if err := c.Transition(0, scopedPayload(t, "A", 0, true, true), false); err != nil {
		t.Fatal(err)
	}
	if total, refs := totalOf(t, c, "A"); total != 1 || refs != 2 {
		t.Fatalf("A depois de voltar: total %d refs %d, quero 1 e 2", total, refs)
	}
	// O último membro sai pelo escopo: o grupo esvazia e marca emptySince,
	// como esvaziaria pela lápide.
	for r := range 2 {
		if err := c.Transition(types.RowID(r), scopedPayload(t, "A", int64(r), true, false), false); err != nil {
			t.Fatal(err)
		}
	}
	id := groupIDOf(t, c, "A")
	if total, refs := totalOf(t, c, "A"); total != 0 || refs != 0 || c.emptySince[id] == 0 {
		t.Fatalf("A vazio pelo escopo: total %d refs %d emptySince %d, quero 0, 0 e marcado", total, refs, c.emptySince[id])
	}
	requireCoherentAggregates(t, c)
}

// TestEachDeriverGetsItsOwnScopeEvaluator: o avaliador tem scratch de um dono
// só, e o writer e a auditoria derivam em goroutines diferentes — cada Deriver
// instancia o seu pela fábrica, nunca compartilha.
func TestEachDeriverGetsItsOwnScopeEvaluator(t *testing.T) {
	var created atomic.Int64
	c := openCounter(t, t.TempDir(), defByOLTWithScope("spl", &created), false)
	if created.Load() != 1 {
		t.Fatalf("o Open instanciou %d avaliadores do scope, quero 1 (o do writer)", created.Load())
	}
	audit := c.NewDeriver()
	if created.Load() != 2 {
		t.Fatalf("um Deriver novo instanciou %d avaliadores no total, quero 2", created.Load())
	}
	if audit.scope == c.deriv.scope {
		t.Fatal("o Deriver da auditoria compartilha o avaliador do scope com o do writer")
	}
	if _, _, has, err := audit.Derive(scopedPayload(t, "A", 1, true, false)); err != nil || has {
		t.Fatalf("o Deriver da auditoria ignorou o scope: has=%v err=%v", has, err)
	}
}

// TestValidateRefusesScopeWithoutAnEvaluator: a guarda espelha a do where — um
// scope declarado sem fábrica contaria toda linha como membro, em silêncio.
func TestValidateRefusesScopeWithoutAnEvaluator(t *testing.T) {
	def := defByOLT("spl", true, 100)
	def.Scope = "eligible = true"
	_, err := Open(t.TempDir(), def, 1024, false)
	if err == nil || err.Error() != "counter spl: scope declared without an evaluator" {
		t.Fatalf("Open com scope sem avaliador: %v, quero a recusa própria", err)
	}
}
