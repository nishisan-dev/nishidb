package planner_test

import (
	"math"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/planner"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A forma canônica do plano é o que os testes comparam e o que vai para o log.
// Ela precisa ser determinística e independente de COMO o usuário escreveu a
// query: o nome impresso é sempre o original, nunca o alias.
func TestPlanString(t *testing.T) {
	c := compilar(t)
	status := campo(t, c, "Acct-Status-Type")
	protocol := campo(t, c, "Framed-Protocol")
	nasIP := campo(t, c, "NAS-IP-Address")
	id := campo(t, c, "id")
	octets := campo(t, c, "Acct-Input-Octets")
	quando := campo(t, c, "Event-Timestamp")
	prefixo := campo(t, c, "Framed-IPv6-Prefix")

	casos := []struct {
		nome string
		plan planner.Plan
		want string
	}{
		{"full scan", planner.FullScan(), "FullScan"},
		{"point lookup", &planner.PointLookupPlan{Field: id, Key: str("CMP-V0001PPCLP-001")},
			`PointLookup(id="CMP-V0001PPCLP-001")`},
		{"aspas internas", &planner.PointLookupPlan{Field: id, Key: str(`a"b`)},
			`PointLookup(id="a\"b")`},
		{"ipv4 em hex", &planner.IndexScanPlan{Field: nasIP, Val: ipv4(t, "10.0.0.1")},
			"IndexScan(NAS-IP-Address=0x0a000001)"},
		{"index scan", &planner.IndexScanPlan{Field: status, Val: str("Stop")},
			`IndexScan(Acct-Status-Type="Stop")`},
		{"int64", &planner.IndexScanPlan{Field: octets, Val: i64(4096)},
			"IndexScan(Acct-Input-Octets=4096)"},
		{"timestamp", &planner.IndexScanPlan{Field: quando, Val: types.Value{Type: types.TypeTimestamp, I64: 1723500000000}},
			"IndexScan(Event-Timestamp=1723500000000)"},
		{"double", &planner.IndexScanPlan{Field: octets,
			Val: types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(10.5))}},
			"IndexScan(Acct-Input-Octets=10.5)"},
		{"bool", &planner.IndexScanPlan{Field: octets, Val: types.Value{Type: types.TypeBool, I64: 1}},
			"IndexScan(Acct-Input-Octets=true)"},
		{"bool falso", &planner.IndexScanPlan{Field: octets, Val: types.Value{Type: types.TypeBool}},
			"IndexScan(Acct-Input-Octets=false)"},
		{"prefixo em hex", &planner.IndexScanPlan{Field: prefixo,
			Val: types.Value{Type: types.TypeIPv6Prefix, Bytes: []byte{0x40, 0x20, 0x01, 0x0d, 0xb8}}},
			"IndexScan(Framed-IPv6-Prefix=0x4020010db8)"},
		{"interseção preserva a ordem dos termos", &planner.IndexIntersectPlan{Terms: []planner.IndexTerm{
			{Field: status, Val: str("Stop")},
			{Field: protocol, Val: str("PPPoE")},
		}}, `IndexIntersect(Acct-Status-Type="Stop", Framed-Protocol="PPPoE")`},
		// A união também preserva a ordem dos termos, e já não prefixa cada um
		// com o motor: o prefixo distinguia bitmap de hash, e não há mais dois.
		{"união preserva a ordem dos termos", &planner.IndexUnionPlan{Terms: []planner.IndexTerm{
			{Field: status, Val: str("Stop")},
			{Field: nasIP, Val: ipv4(t, "10.0.0.1")},
		}}, `IndexUnion(Acct-Status-Type="Stop", NAS-IP-Address=0x0a000001)`},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			if got := tc.plan.String(); got != tc.want {
				t.Errorf("String() = %s, want %s", got, tc.want)
			}
		})
	}
}

// A query escrita com ALIAS produz a MESMA string da escrita com o nome
// original: o plano fala em campos, não em como o usuário os chamou.
func TestPlanStringUsaNomeOriginalENaoAlias(t *testing.T) {
	p, c, _ := novo(t)
	porAlias := planeja(t, p, eq(campo(t, c, "status"), str("Stop")))
	porNome := planeja(t, p, eq(campo(t, c, "Acct-Status-Type"), str("Stop")))

	if porAlias.String() != porNome.String() {
		t.Errorf("alias = %s, nome = %s: deveriam ser iguais", porAlias, porNome)
	}
	if porAlias.String() != `IndexScan(Acct-Status-Type="Stop")` {
		t.Errorf("String() = %s, want o nome original", porAlias)
	}
}

// String() é caminho de LOG: um plano malformado (campo nil, que o planner
// nunca produz mas um teste ou um dump podem construir) tem de render texto,
// não derrubar o processo que estava só tentando registrar o que aconteceu.
func TestPlanStringNaoQuebraComCampoNil(t *testing.T) {
	// Eram quatro entradas: o BitmapScanPlan e o HashIndexPlan viraram a MESMA
	// forma de plano, e listar IndexScanPlan duas vezes não acrescenta caso.
	planos := []planner.Plan{
		&planner.IndexScanPlan{Val: str("Stop")},
		&planner.PointLookupPlan{Key: str("A")},
		&planner.IndexUnionPlan{Terms: []planner.IndexTerm{{Val: str("a")}, {Val: str("b")}}},
	}
	for _, pl := range planos {
		if s := pl.String(); s == "" {
			t.Errorf("%T devolveu string vazia", pl)
		}
	}
}

// O FullScan é SINGLETON compartilhado entre queries concorrentes: um plano
// com estado aqui seria corrupção cruzada.
func TestFullScanEhSingleton(t *testing.T) {
	if planner.FullScan() != planner.FullScan() {
		t.Error("FullScan() devolveu instâncias diferentes")
	}
	p, c, _ := novo(t)
	semWhere := p.Plan(consulta(nil))
	degradado := planeja(t, p, eq(campo(t, c, "username"), str("ze")))
	if semWhere != planner.FullScan() || degradado != planner.FullScan() {
		t.Error("Plan não devolveu o singleton nos caminhos de FullScan")
	}
}
