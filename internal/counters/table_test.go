package counters_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// FieldIDs do dataset de teste, em ordem crescente como o payload exige.
const (
	fOlt  types.FieldID = 1
	fSlot types.FieldID = 2
	fPort types.FieldID = 3
	fAuth types.FieldID = 4
)

type avaliadorDeTeste struct{ fr encoding.FieldReader }

func (a *avaliadorDeTeste) Evaluate(payload []byte) (bool, error) {
	a.fr.Reset(payload)
	for {
		id, v, ok := a.fr.Next()
		if !ok {
			return false, a.fr.Err()
		}
		if id == fAuth {
			return v.Type == types.TypeBool && v.I64 == 1, nil
		}
	}
}

func defPorPON(nome string) counter.Definition {
	return counter.Definition{
		Name:         nome,
		Columns:      []types.FieldID{fOlt, fSlot, fPort},
		Names:        []string{"olt", "slot", "port"},
		Types:        []types.FieldType{types.TypeString, types.TypeInt64, types.TypeInt64},
		Where:        "authenticated = true",
		NewEvaluator: func() counter.Evaluator { return &avaliadorDeTeste{} },
		MaxGroups:    1000,
	}
}

func defPorOLT(nome string) counter.Definition {
	return counter.Definition{
		Name:         nome,
		Columns:      []types.FieldID{fOlt},
		Names:        []string{"olt"},
		Types:        []types.FieldType{types.TypeString},
		Where:        "authenticated = true",
		NewEvaluator: func() counter.Evaluator { return &avaliadorDeTeste{} },
		MaxGroups:    1000,
	}
}

func payloadDeServico(t *testing.T, olt string, slot, port int64, auth bool) []byte {
	t.Helper()
	var buf []byte
	var err error
	if buf, err = encoding.AppendField(buf, fOlt, types.Value{Type: types.TypeString, Bytes: []byte(olt)}); err != nil {
		t.Fatal(err)
	}
	if buf, err = encoding.AppendField(buf, fSlot, types.Value{Type: types.TypeInt64, I64: slot}); err != nil {
		t.Fatal(err)
	}
	if buf, err = encoding.AppendField(buf, fPort, types.Value{Type: types.TypeInt64, I64: port}); err != nil {
		t.Fatal(err)
	}
	a := types.Value{Type: types.TypeBool}
	if auth {
		a.I64 = 1
	}
	if buf, err = encoding.AppendField(buf, fAuth, a); err != nil {
		t.Fatal(err)
	}
	return buf
}

func abreContador(t *testing.T, def counter.Definition) *counter.Counter {
	t.Helper()
	c, err := counter.Open(t.TempDir(), def, 1024, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// motorComContador registra um dataset de mentira (o alvo) e a tabela virtual
// do contador sob ele.
func motorComContador(t *testing.T, c *counter.Counter) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte("version: 1\nname: servicos\nmaterialization:\n  key:\n    field: id\nschema:\n  fields:\n    id:\n      type: string\n"), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register("servicos", sc, fakestorage.New(fakestorage.Indexes{})); err != nil {
		t.Fatal(err)
	}
	tsc, fonte, err := counters.Tabela(c.Def(), c)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterVirtual("servicos", schema.CountersNamespace, c.Def().Name, tsc, fonte); err != nil {
		t.Fatal(err)
	}
	return eng
}

type linha map[string]string

func consulta(t *testing.T, eng *query.Engine, sql string) []linha {
	t.Helper()
	it, err := eng.Execute(context.Background(), "servicos", sql, nil)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	var out []linha
	for it.Next() {
		l := make(linha, it.Row().Len())
		for _, c := range it.Row().Cols() {
			l[c.Name] = render(c)
		}
		out = append(out, l)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return out
}

func render(c executor.ColView) string {
	if c.Null {
		return "<null>"
	}
	switch c.Val.Type {
	case types.TypeString:
		return string(c.Val.Bytes)
	case types.TypeInt64:
		return strconv.FormatInt(c.Val.I64, 10)
	case types.TypeBool:
		if c.Val.I64 != 0 {
			return "true"
		}
		return "false"
	default:
		return "?"
	}
}

func coluna(rows []linha, col string) string {
	out := make([]string, 0, len(rows))
	for _, l := range rows {
		out = append(out, l[col])
	}
	return strings.Join(out, ",")
}

// populaPON monta o cenário da proposta §5.3: PRCTA_G1M10 com três PONs (uma
// zerada) e outra OLT.
func populaPON(t *testing.T, c *counter.Counter) {
	t.Helper()
	row := types.RowID(0)
	put := func(olt string, slot, port int64, auth bool, n int) {
		for i := 0; i < n; i++ {
			if err := c.Transition(row, payloadDeServico(t, olt, slot, port, auth), false); err != nil {
				t.Fatal(err)
			}
			row++
		}
	}
	put("PRCTA_G1M10", 1, 9, false, 3)  // grupo com total 0
	put("PRCTA_G1M10", 1, 12, true, 47) // 47
	put("PRCTA_G1M10", 3, 2, true, 58)  // 58
	put("BR_SPOMO_OLT10", 2, 1, true, 5)
}

func TestTabelaDoContadorRespondeAsConsultasDaProposta(t *testing.T) {
	c := abreContador(t, defPorPON("autenticados_por_pon"))
	populaPON(t, c)
	eng := motorComContador(t, c)

	rows := consulta(t, eng, "SELECT olt, slot, port, total FROM _counters.autenticados_por_pon WHERE olt = 'PRCTA_G1M10' ORDER BY total ASC LIMIT 3")
	if got := coluna(rows, "total"); got != "0,47,58" {
		t.Fatalf("totais = %s, quer 0,47,58 (o grupo zerado aparece)", got)
	}
	if got := coluna(rows, "slot"); got != "1,1,3" {
		t.Fatalf("slot tem de sair tipado como int64: %s", got)
	}
	if got := coluna(rows, "port"); got != "9,12,2" {
		t.Fatalf("port = %s", got)
	}

	// groupKey é a key: point lookup pelo texto canônico.
	rows = consulta(t, eng, "SELECT groupKey, total FROM _counters.autenticados_por_pon WHERE groupKey = 'PRCTA_G1M10|1|12'")
	if len(rows) != 1 || rows[0]["total"] != "47" || rows[0]["groupKey"] != "PRCTA_G1M10|1|12" {
		t.Fatalf("point lookup = %v", rows)
	}
	rows = consulta(t, eng, "SELECT groupKey FROM _counters.autenticados_por_pon WHERE groupKey = 'nao existe'")
	if len(rows) != 0 {
		t.Fatalf("groupKey inexistente tem de devolver nada: %v", rows)
	}

	// Todas as linhas, sem filtro: quatro grupos.
	rows = consulta(t, eng, "SELECT groupKey, total FROM _counters.autenticados_por_pon ORDER BY groupKey ASC")
	if got := coluna(rows, "groupKey"); got != "BR_SPOMO_OLT10|2|1,PRCTA_G1M10|1|12,PRCTA_G1M10|1|9,PRCTA_G1M10|3|2" {
		t.Fatalf("groupKeys = %s", got)
	}
}

func TestTabelaPorOLTTemUmComponenteSemSeparador(t *testing.T) {
	c := abreContador(t, defPorOLT("autenticados_por_olt"))
	for i := 0; i < 10; i++ {
		if err := c.Transition(types.RowID(i), payloadDeServico(t, "PRCTA_G1M10", 1, 1, i%2 == 0), false); err != nil {
			t.Fatal(err)
		}
	}
	eng := motorComContador(t, c)
	rows := consulta(t, eng, "SELECT olt, groupKey, total FROM _counters.autenticados_por_olt WHERE olt = 'PRCTA_G1M10'")
	if len(rows) != 1 || rows[0]["groupKey"] != "PRCTA_G1M10" || rows[0]["total"] != "5" {
		t.Fatalf("%v", rows)
	}
}

func TestTabelaLeOPresenteEOConjuntoDeGruposCongelaPorConsulta(t *testing.T) {
	c := abreContador(t, defPorOLT("aut"))
	if err := c.Transition(0, payloadDeServico(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	eng := motorComContador(t, c)
	it, err := eng.Execute(context.Background(), "servicos", "SELECT olt, total FROM _counters.aut", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	// Um grupo novo depois de a consulta abrir não entra no universo dela.
	if err := c.Transition(1, payloadDeServico(t, "B", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	// Mas o total do grupo existente é o PRESENTE — os totais não são
	// versionados pelo snapshot.
	if err := c.Transition(2, payloadDeServico(t, "A", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	var vistos []string
	for it.Next() {
		l := make(linha, 2)
		for _, col := range it.Row().Cols() {
			l[col.Name] = render(col)
		}
		vistos = append(vistos, l["olt"]+"="+l["total"])
	}
	if strings.Join(vistos, ",") != "A=2" {
		t.Fatalf("vistos = %v, quer só A com o total presente (2)", vistos)
	}
}

func TestTabelaRecusaColunaInexistenteNomeandoATabela(t *testing.T) {
	c := abreContador(t, defPorOLT("aut"))
	eng := motorComContador(t, c)
	_, err := eng.Execute(context.Background(), "servicos", "SELECT pon FROM _counters.aut", nil)
	if err == nil || !strings.Contains(err.Error(), "_counters.aut") {
		t.Fatalf("erro tem de nomear _counters.aut: %v", err)
	}
}

func TestTabelaRecusaDefinicaoQueColideComAsColunasFixas(t *testing.T) {
	def := defPorOLT("x")
	def.Names = []string{"total"}
	if _, _, err := counters.Tabela(def, nil); err == nil {
		t.Fatal("coluna do groupBy chamada total tem de ser recusada")
	}
}
