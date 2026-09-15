package server_test

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func TestProjectionAliasesReachStreamMetadataAndRows(t *testing.T) {
	srv := serverWithData(t, 2, blockPadrao)
	for _, limit := range []string{" LIMIT 0", ""} {
		request, err := json.Marshal(map[string]string{"dataset": "d", "sql": `SELECT estado AS "FROM", porta "com""aspas", porta AS "com""aspas" FROM d ORDER BY "com""aspas_1" DESC` + limit})
		if err != nil {
			t.Fatal(err)
		}
		resp := consulta(t, srv, string(request))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		rows, reader := result(t, resp)
		cols := reader.Schema().Columns
		want := []protocol.Column{protocol.NewColumn("FROM", types.TypeString, true), protocol.NewColumn(`com"aspas`, types.TypeInt64, true), protocol.NewColumn(`com"aspas_1`, types.TypeInt64, true)}
		if !reflect.DeepEqual(cols, want) {
			t.Fatalf("metadados = %+v, quer %+v", cols, want)
		}
		if limit != "" {
			if len(rows) != 0 {
				t.Fatal("LIMIT 0 retornou linhas")
			}
			continue
		}
		if len(rows) != 2 {
			t.Fatalf("linhas = %d", len(rows))
		}
		for i, raw := range rows {
			var row map[string]any
			if err := json.Unmarshal([]byte(raw), &row); err != nil {
				t.Fatal(err)
			}
			if len(row) != 3 || row[`com"aspas`] != float64(1001-i) || row[`com"aspas_1`] != float64(1001-i) || row["FROM"] == nil {
				t.Fatalf("linha = %s", raw)
			}
		}
	}
}

func TestProjectionAliasCannotHideReservedFrameKey(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id AS nishidb FROM d"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := failOf(t, resp); got.Code != protocol.CodeReservedColumn {
		t.Fatalf("erro = %+v", got)
	}
}
