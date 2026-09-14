package binder_test

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

func TestProjectionAliasesAndSuffixes(t *testing.T) {
	for _, tc := range []struct {
		projection string
		names      []string
		fields     []string
	}{
		{"id AS equipamento, status", []string{"equipamento", "status"}, []string{"id", "status"}},
		{"id equipamento, status", []string{"equipamento", "status"}, []string{"id", "status"}},
		{"id, id", []string{"id", "id_1"}, []string{"id", "id"}},
		{"status, id AS status", []string{"status_1", "status"}, []string{"status", "id"}},
		{"id AS x, status AS x", []string{"x", "x_1"}, []string{"id", "status"}},
		{"id AS x, status AS x, octets AS x_1", []string{"x", "x_2", "x_1"}, []string{"id", "status", "octets"}},
		{"id AS x, status AS x, octets AS x, ratio AS x_1", []string{"x", "x_2", "x_3", "x_1"}, []string{"id", "status", "octets", "ratio"}},
		{"id AS X, status AS x", []string{"X", "x"}, []string{"id", "status"}},
		{`service_id AS "FROM", status "com""aspas"`, []string{"FROM", `com"aspas`}, []string{"id", "status"}},
	} {
		t.Run(tc.projection, func(t *testing.T) {
			bq := bindOK(t, "SELECT "+tc.projection+" FROM radius_sessions")
			if got := columnNames(bq.Columns); !reflect.DeepEqual(got, tc.names) {
				t.Fatalf("nomes = %q, quer %q", got, tc.names)
			}
			for i, c := range bq.Columns {
				if c.Field.Name != tc.fields[i] {
					t.Fatalf("campo %d = %s, quer %s", i, c.Field.Name, tc.fields[i])
				}
				for j := 0; j < i; j++ {
					if c.Field == bq.Columns[j].Field && c.Slot != bq.Columns[j].Slot {
						t.Fatal("campo repetido não compartilha slot")
					}
				}
			}
		})
	}
}

// Cruza o limiar do mapa com colisões de aliases e um sufixo reservado
// depois das duplicatas. Repetir o bind também prova que a AST é reutilizável.
func TestLargeAliasProjectionReservesNamesWithoutMutatingAST(t *testing.T) {
	const count = 128
	st, err := parser.Parse("SELECT " + strings.Repeat("id AS x, ", count) + "status AS x_1 FROM radius_sessions ORDER BY x_1")
	if err != nil {
		t.Fatal(err)
	}
	before := append([]parser.ColumnRef(nil), st.Projection...)
	sc := compile(t)
	for range 2 {
		bq, err := binder.Bind(st, sc, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(bq.Columns) != count+1 || bq.Slots.Len() != 2 {
			t.Fatalf("colunas = %d, slots = %d", len(bq.Columns), bq.Slots.Len())
		}
		for i, c := range bq.Columns {
			want := "x"
			if i == count {
				want = "x_1"
			} else if i > 0 {
				want = "x_" + strconv.Itoa(i+1)
			}
			if c.Name != want {
				t.Fatalf("nome %d = %q, quer %q", i, c.Name, want)
			}
		}
		if bq.OrderBy[0].Field.Name != "status" || !reflect.DeepEqual(st.Projection, before) {
			t.Fatal("ORDER BY incorreto ou AST modificada pelo bind")
		}
	}
	plain := &parser.SelectStmt{Dataset: "radius_sessions", Projection: []parser.ColumnRef{{Name: "service_id"}}}
	bq, err := binder.Bind(plain, sc, nil)
	if err != nil || bq.Columns[0].Name != "service_id" || bq.Columns[0].Field.Name != "id" {
		t.Fatalf("schema alterado pelo alias: %+v, %v", bq, err)
	}
}

func TestOrderByUsesFinalProjectionNames(t *testing.T) {
	for _, tc := range []struct{ projection, order, field string }{
		{"status, id AS status", "status", "id"},
		{"status, id AS status", "status_1", "status"},
		{"id AS x, status AS x", "x", "id"},
		{"id AS x, status AS x", "x_1", "status"},
		{"id AS x", "octets", "octets"},
		{`id AS "doc.x"`, `"doc.x"`, "id"},
	} {
		bq := bindOK(t, "SELECT "+tc.projection+" FROM radius_sessions ORDER BY "+tc.order+" DESC NULLS FIRST")
		k := bq.OrderBy[0]
		if k.Field.Name != tc.field || !k.Desc || !k.NullsFirst {
			t.Fatalf("ORDER BY %s = %+v, quer %s DESC NULLS FIRST", tc.order, k, tc.field)
		}
	}
	for _, sql := range []string{
		"SELECT id AS x FROM radius_sessions WHERE x = 'a'",
		"SELECT id AS x, x FROM radius_sessions",
		"SELECT id AS x FROM radius_sessions ORDER BY x, id",
		"SELECT raw AS x FROM radius_sessions ORDER BY x",
		"SELECT livre AS x FROM radius_sessions ORDER BY x",
	} {
		_ = bindErr(t, sql)
	}
	bq := bindOK(t, "SELECT id AS status FROM radius_sessions WHERE status = 'ok'")
	if bq.Where.(*binder.BoundComparison).Field.Name != "status" {
		t.Fatal("alias interferiu no WHERE")
	}
}

func TestNestedProjectionAliasesAndSuffixes(t *testing.T) {
	bq, err := bindWithYAML(t, nestedYAML, `SELECT doc.probeIpNode AS ip, probe_ip ip, id AS "doc.probeIpNode" FROM eventos ORDER BY doc.probeIpNode`)
	if err != nil {
		t.Fatal(err)
	}
	if got := columnNames(bq.Columns); !reflect.DeepEqual(got, []string{"ip", "ip_1", "doc.probeIpNode"}) {
		t.Fatalf("nomes = %q", got)
	}
	if bq.OrderBy[0].Field != bq.Columns[0].Field {
		t.Fatal("alias tomou o lugar do caminho qualificado")
	}
	bq, err = bindWithYAML(t, nestedYAML, `SELECT doc.*, id AS probe_ip FROM eventos`)
	if err != nil {
		t.Fatal(err)
	}
	if got := columnNames(bq.Columns); !reflect.DeepEqual(got, []string{"probe_ip_1", "alarme_id", "probe_ip"}) {
		t.Fatalf("nomes = %q", got)
	}
	_, err = bindWithYAML(t, nestedYAML, `SELECT doc.*, doc.probeIpNode AS ip FROM eventos`)
	if err == nil || !strings.Contains(err.Error(), "maxNestedCols") {
		t.Fatalf("teto aninhado não foi preservado: %v", err)
	}
}
