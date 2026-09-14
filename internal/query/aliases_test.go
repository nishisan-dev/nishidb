package query_test

import (
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestAliasOrderingAndWhereRemainIndependent(t *testing.T) {
	h := newHarness(t)
	h.ingere(
		`{"id":"A","_SEQ":1,"User-Name":"z","Acct-Session-Time":10}`,
		`{"id":"B","_SEQ":1,"User-Name":"a","Acct-Session-Time":30}`,
		`{"id":"C","_SEQ":1,"Acct-Session-Time":20}`,
	)
	for _, tc := range []struct {
		suffix string
		want   []string
	}{
		{`ORDER BY id`, []string{"B", "A", "C"}},
		{`ORDER BY id_1`, []string{"A", "B", "C"}},
		{`ORDER BY id DESC NULLS FIRST`, []string{"C", "A", "B"}},
		{`ORDER BY id LIMIT 1 OFFSET 1`, []string{"A"}},
		{`WHERE id = 'A' ORDER BY id`, []string{"A"}},
		{`ORDER BY "Acct-Session-Time" DESC`, []string{"B", "C", "A"}},
	} {
		rows := h.consultaOK(`SELECT id, "User-Name" AS id FROM radius_sessions ` + tc.suffix)
		if got := chaves(rows, "id_1"); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%s: ordem = %q, quer %q", tc.suffix, got, tc.want)
		}
	}
}

func TestAliasesOnVirtualTables(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, dataset, "_counters", "autenticados_por_olt")
	rows := h.consultaOK(`SELECT olt equipamento, total AS quantidade FROM _counters.autenticados_por_olt ORDER BY quantidade DESC LIMIT 1`)
	if len(rows) != 1 || rows[0]["equipamento"] != "PRCTA_G1M10" || rows[0]["quantidade"] != "2802" {
		t.Fatalf("contador = %v", rows)
	}
	rows = h.consultaOK(`SELECT name AS campo, name campo FROM _meta.fields ORDER BY campo`)
	if len(rows) == 0 {
		t.Fatal("_meta.fields sem linhas")
	}
	for _, row := range rows {
		if row["campo"] != row["campo_1"] || row["campo"] == "" {
			t.Fatalf("metadado = %v", row)
		}
	}
}

func TestEmptyResultRetainsAliasMetadata(t *testing.T) {
	cols := columns(t, newHarness(t), `SELECT id AS campo, filter_id campo FROM radius_sessions LIMIT 0`)
	if len(cols) != 2 || cols[0].Name != "campo" || cols[1].Name != "campo_1" || cols[0].Nullable || !cols[1].Nullable || cols[0].Type != types.TypeString || cols[1].Type != types.TypeString {
		t.Fatalf("metadados = %+v", cols)
	}
}
