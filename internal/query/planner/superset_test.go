package planner_test

import (
	"sort"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A propriedade que dá sentido ao pacote inteiro:
//
//	candidatos(plano) ⊇ { rows que realmente satisfazem o WHERE }
//
// É o único teste capaz de pegar a classe de bug "resultado silenciosamente
// incompleto" — regra de índice usada onde não devia. Um falso positivo o
// executor filtra; um falso NEGATIVO ninguém recupera, e a query mente sem
// nenhum sinal.
//
// O conjunto esperado é escrito à MÃO: o avaliador de predicado é do executor,
// e antecipá-lo aqui faria o teste comparar o planner com uma cópia da mesma
// lógica que ele deveria vigiar.
func TestPlanCandidatosSaoSuperset(t *testing.T) {
	c := compilar(t)
	store := todosOsIndices(t, c)
	p := planner.New(store, nil)

	var (
		id       = campo(t, c, "id")
		status   = campo(t, c, "Acct-Status-Type")
		protocol = campo(t, c, "Framed-Protocol")
		nasIP    = campo(t, c, "NAS-IP-Address")
		username = campo(t, c, "User-Name")
	)

	linhas := []struct {
		key    string
		campos []campoValor
		morta  bool
	}{
		{key: "A", campos: []campoValor{
			{status, str("Stop")}, {protocol, str("PPPoE")}, {nasIP, ipv4(t, "10.0.0.1")}, {username, str("ze")}}},
		{key: "B", campos: []campoValor{
			{status, str("Start")}, {protocol, str("PPPoE")}, {nasIP, ipv4(t, "10.0.0.1")}, {username, str("ana")}}},
		{key: "C", campos: []campoValor{
			{status, str("Stop")}, {protocol, str("PPP")}, {nasIP, ipv4(t, "10.0.0.2")}, {username, str("ze")}}},
		{key: "D", campos: []campoValor{
			{status, str("Stop")}, {protocol, str("PPPoE")}, {nasIP, ipv4(t, "10.0.0.2")}, {username, str("bia")}}},
		// E satisfaz o WHERE de várias queries, mas está MORTA: aparece nos
		// candidatos (as postings não são limpas) e não no resultado. É o
		// superset acontecendo na prática.
		{key: "E", campos: []campoValor{
			{status, str("Stop")}, {protocol, str("PPPoE")}, {nasIP, ipv4(t, "10.0.0.1")}, {username, str("ze")}},
			morta: true},
		// F não tem o campo status: campo AUSENTE compara falso para qualquer
		// operador, e não há posting para ausência.
		{key: "F", campos: []campoValor{
			{protocol, str("PPPoE")}, {nasIP, ipv4(t, "10.0.0.3")}, {username, str("ze")}}},
	}

	rowIDs := map[string]types.RowID{}
	for _, l := range linhas {
		campos := append([]campoValor{{id, str(l.key)}}, l.campos...)
		rid, _, err := store.Put([]byte(l.key), 1, 0, montaPayload(t, campos), nil)
		if err != nil {
			t.Fatalf("Put(%s): %v", l.key, err)
		}
		rowIDs[l.key] = rid
		if l.morta {
			store.SetTombstone(rid)
		}
	}

	casos := []struct {
		nome     string
		where    binder.BoundExpr
		plano    string
		esperado []string // rows VIVAS que satisfazem o WHERE
	}{
		{"point lookup", eq(id, str("A")), `PointLookup(id="A")`, []string{"A"}},
		{"index scan", eq(status, str("Stop")), `IndexScan(Acct-Status-Type="Stop")`,
			[]string{"A", "C", "D"}},
		{"interseção", and(eq(status, str("Stop")), eq(protocol, str("PPPoE"))),
			`IndexIntersect(Acct-Status-Type="Stop", Framed-Protocol="PPPoE")`, []string{"A", "D"}},
		// nasIP era `index: hash` antes da D5; com `index: true` como grafia
		// única ele sai indexado como status, e a string da união já não prefixa
		// termo nenhum com o motor.
		{"união de dois campos indexados", or(eq(status, str("Stop")), eq(nasIP, ipv4(t, "10.0.0.3"))),
			`IndexUnion(Acct-Status-Type="Stop", NAS-IP-Address=0x0a000003)`,
			[]string{"A", "C", "D", "F"}},
		// OR com um disjunct não indexado degrada por inteiro: uma união
		// parcial deixaria B de fora.
		{"OR degradado", or(eq(status, str("Stop")), eq(username, str("ana"))), "FullScan",
			[]string{"A", "B", "C", "D"}},
		{"sem where", nil, "FullScan", []string{"A", "B", "C", "D", "F"}},
	}

	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			plan := p.Plan(consulta(tc.where))
			if got := plan.String(); got != tc.plano {
				t.Fatalf("plano = %s, want %s", got, tc.plano)
			}
			cands := candidatos(t, store, plan)
			for _, key := range tc.esperado {
				if !cands[rowIDs[key]] {
					t.Errorf("row %s satisfaz o WHERE e NÃO está nos candidatos de %s: "+
						"resultado silenciosamente incompleto", key, plan)
				}
			}
		})
	}

	// A contraprova de que o teste tem dentes: a row MORTA aparece mesmo entre
	// os candidatos do bitmap. Se um dia ela sumir, o superset virou conjunto
	// exato em algum lugar — e aí o teste acima deixa de provar o que promete.
	t.Run("candidato morto aparece", func(t *testing.T) {
		plan := p.Plan(consulta(eq(status, str("Stop"))))
		if !candidatos(t, store, plan)[rowIDs["E"]] {
			t.Error("a row tombstoned não apareceu nos candidatos do índice")
		}
	})
}

type campoValor struct {
	f *schema.CompiledField
	v types.Value
}

// montaPayload ordena por FieldID antes de codificar: payload fora de ordem é
// recusado pelo Put (do fake e do real).
func montaPayload(t *testing.T, campos []campoValor) []byte {
	t.Helper()
	ordenado := append([]campoValor(nil), campos...)
	sort.Slice(ordenado, func(i, j int) bool { return ordenado[i].f.ID < ordenado[j].f.ID })

	var p []byte
	for _, cv := range ordenado {
		var err error
		if p, err = encoding.AppendField(p, cv.f.ID, cv.v); err != nil {
			t.Fatalf("AppendField(%s): %v", cv.f.Name, err)
		}
	}
	return p
}

// candidatos materializa a fonte de candidatos de um plano — é o que o
// executor fará, de forma ingênua (maps) porque aqui é teste.
func candidatos(t *testing.T, s *fakestorage.Store, plan planner.Plan) map[types.RowID]bool {
	t.Helper()
	out := map[types.RowID]bool{}
	switch pl := plan.(type) {
	case *planner.FullScanPlan:
		coleta(t, s.ScanLive(), out)

	case *planner.PointLookupPlan:
		rec, _, ok, err := s.GetByKey(pl.Key.Bytes, nil)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			out[rec.RowID] = true
		}

	case *planner.IndexScanPlan:
		it, err := s.Lookup(pl.Field.ID, pl.Val)
		if err != nil {
			t.Fatal(err)
		}
		coleta(t, it, out)

	case *planner.IndexIntersectPlan:
		for i, term := range pl.Terms {
			parcial := map[types.RowID]bool{}
			coleta(t, lookup(t, s, term), parcial)
			if i == 0 {
				out = parcial
				continue
			}
			for id := range out {
				if !parcial[id] {
					delete(out, id)
				}
			}
		}

	case *planner.IndexUnionPlan:
		for _, term := range pl.Terms {
			coleta(t, lookup(t, s, term), out)
		}

	default:
		t.Fatalf("tipo de plano não tratado: %T", plan)
	}
	return out
}

// O despacho por term.Kind saiu daqui junto com o campo: um ramo chamava
// LookupHash e o outro LookupBitmap, e existe um motor só.
func lookup(t *testing.T, s *fakestorage.Store, term planner.IndexTerm) types.RowIDIterator {
	t.Helper()
	it, err := s.Lookup(term.Field.ID, term.Val)
	if err != nil {
		t.Fatalf("lookup de %s: %v", term.Field.Name, err)
	}
	return it
}

func coleta(t *testing.T, it types.RowIDIterator, dst map[types.RowID]bool) {
	t.Helper()
	defer it.Close()
	for it.Next() {
		dst[it.RowID()] = true
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
}
