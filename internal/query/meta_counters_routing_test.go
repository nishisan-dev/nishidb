package query_test

import (
	"strings"
	"testing"
)

// TestMetaRoteiaFieldsParaOMetaEOsDemaisParaORegistro: `_meta.fields` é a
// tabela fixa do pacote meta; qualquer outra tabela do `_meta` é uma tabela
// REGISTRADA sob o dataset — é por aí que a `_meta.counters` entra sem o
// pacote meta conhecer o storage.
func TestMetaRoteiaFieldsParaOMetaEOsDemaisParaORegistro(t *testing.T) {
	h := newHarness(t)
	// Antes de qualquer registro o `_meta` já existe, com a `fields`.
	rows := h.consultaOK("SELECT name FROM _meta.fields")
	if len(rows) == 0 {
		t.Fatal("_meta.fields tem de responder sem registro nenhum")
	}
	_, err := h.consulta("SELECT a FROM _meta.counters")
	if err == nil || !strings.Contains(err.Error(), `_meta não tem tabela "counters"`) || !strings.Contains(err.Error(), "existem: fields") {
		t.Fatalf("sem registro, a recusa tem de nomear a fields: %v", err)
	}

	// Registrada, a tabela corre pelo pipeline do registro.
	tableVirtualOfTest(t, h.engine, dataset, "_meta", "counters")
	rows = h.consultaOK("SELECT groupKey, total FROM _meta.counters ORDER BY total DESC LIMIT 1")
	if len(rows) != 1 || rows[0]["total"] != "2802" {
		t.Fatalf("_meta.counters registrada = %v", rows)
	}
	// E a `fields` continua no pacote meta, não no registro.
	if rows := h.consultaOK("SELECT name FROM _meta.fields"); len(rows) == 0 {
		t.Fatal("_meta.fields sumiu depois do registro")
	}
	_, err = h.consulta("SELECT a FROM _meta.nada")
	if err == nil || !strings.Contains(err.Error(), "existem: counters, fields") {
		t.Fatalf("a recusa tem de listar as duas: %v", err)
	}
}
