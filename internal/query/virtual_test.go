package query_test

import (
	"context"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// tabelaVirtualDeTeste monta um Store virtual com o formato de um contador
// (groupKey, olt, total) sobre o dublê, e o registra no motor sob o namespace
// dado. O dublê serve porque o motor só precisa de um query.Store.
func tabelaVirtualDeTeste(t *testing.T, eng *query.Engine, namespace, tabela string) {
	t.Helper()
	spec, err := schema.Parse([]byte(`version: 1
name: `+tabela+`
materialization:
  key:
    field: groupKey
schema:
  include: [groupKey, olt, total]
  fields:
    groupKey:
      type: string
    olt:
      type: string
    total:
      type: int64
`), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	st := fakestorage.New(fakestorage.Indexes{})
	for i, olt := range []string{"BR_SPOMO_OLT14", "PRCTA_G1M10"} {
		var p []byte
		for _, col := range []struct {
			nome string
			v    types.Value
		}{
			{"groupKey", types.Value{Type: types.TypeString, Bytes: []byte(olt)}},
			{"olt", types.Value{Type: types.TypeString, Bytes: []byte(olt)}},
			{"total", types.Value{Type: types.TypeInt64, I64: int64(i * 2802)}},
		} {
			f, ok := sc.FieldByName([]byte(col.nome))
			if !ok {
				t.Fatal(col.nome)
			}
			if p, err = encoding.AppendField(p, f.ID, col.v); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := st.Put([]byte(olt), int64(i+1), 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.RegistraVirtual(dataset, namespace, tabela, sc, st); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteRoteiaTabelaVirtualRegistrada(t *testing.T) {
	h := novoHarness(t)
	tabelaVirtualDeTeste(t, h.engine, "_counters", "autenticados_por_olt")
	rows := h.consultaOK("SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC LIMIT 5")
	if got := chaves(rows, "olt"); strings.Join(got, ",") != "BR_SPOMO_OLT14,PRCTA_G1M10" {
		t.Fatalf("olts = %v", got)
	}
	if rows[1]["total"] != "2802" {
		t.Fatalf("total = %q", rows[1]["total"])
	}
	// Point lookup pela key da tabela virtual.
	rows = h.consultaOK("SELECT groupKey, total FROM _counters.autenticados_por_olt WHERE groupKey = 'PRCTA_G1M10'")
	if len(rows) != 1 || rows[0]["total"] != "2802" {
		t.Fatalf("point lookup = %v", rows)
	}
	// O binder da tabela virtual continua recusando coluna inexistente,
	// nomeando a tabela pelo nome que o usuário escreveu.
	_, err := h.consulta("SELECT nada FROM _counters.autenticados_por_olt")
	if err == nil || !strings.Contains(err.Error(), "_counters.autenticados_por_olt") {
		t.Fatalf("coluna inexistente tem de nomear _counters.<tabela>: %v", err)
	}
}

func TestNamespaceDesconhecidoListaOsExistentes(t *testing.T) {
	h := novoHarness(t)
	// A costura declara o namespace mesmo num dataset sem contador: ele existe
	// como conceito do produto, e a recusa tem de nomeá-lo.
	if err := h.engine.DeclaraNamespace(dataset, "_counters"); err != nil {
		t.Fatal(err)
	}
	_, err := h.consulta("SELECT a FROM _outro.coisa")
	if err == nil {
		t.Fatal("namespace desconhecido tem de ser recusado")
	}
	if !strings.Contains(err.Error(), "_meta") || !strings.Contains(err.Error(), "_counters") {
		t.Fatalf("a recusa tem de listar os namespaces que existem: %v", err)
	}
}

func TestTabelaVirtualDesconhecidaListaAsDoDataset(t *testing.T) {
	h := novoHarness(t)
	tabelaVirtualDeTeste(t, h.engine, "_counters", "servicos_por_olt")
	_, err := h.consulta("SELECT a FROM _counters.nao_existe")
	if err == nil || !strings.Contains(err.Error(), "servicos_por_olt") {
		t.Fatalf("tabela desconhecida tem de listar as que existem no dataset: %v", err)
	}
	// Dataset sem tabela nenhuma no namespace: diz que não há.
	eng := query.NewEngine()
	if err := eng.Register("vazio", h.sch, h.store); err != nil {
		t.Fatal(err)
	}
	if err := eng.DeclaraNamespace("vazio", "_counters"); err != nil {
		t.Fatal(err)
	}
	_, err = eng.Execute(context.Background(), "vazio", "SELECT a FROM _counters.x", nil)
	if err == nil || !strings.Contains(err.Error(), "não declara") {
		t.Fatalf("dataset sem contadores: %v", err)
	}
}

func TestRegistraVirtualRecusaDuplicataEDatasetDesconhecido(t *testing.T) {
	h := novoHarness(t)
	tabelaVirtualDeTeste(t, h.engine, "_counters", "x")
	spec, _ := schema.Parse([]byte("version: 1\nname: x\nmaterialization:\n  key:\n    field: k\nschema:\n  fields:\n    k:\n      type: string\n"), schema.DefaultsPadrao)
	sc, _ := spec.Compile(fakestorage.NewDict())
	if err := h.engine.RegistraVirtual(dataset, "_counters", "x", sc, fakestorage.New(fakestorage.Indexes{})); err == nil {
		t.Fatal("tabela virtual repetida tem de ser recusada")
	}
	if err := h.engine.RegistraVirtual("nao_registrado", "_counters", "y", sc, fakestorage.New(fakestorage.Indexes{})); err == nil {
		t.Fatal("dataset não registrado tem de ser recusado")
	}
	if err := h.engine.RegistraVirtual(dataset, "semprefixo", "y", sc, fakestorage.New(fakestorage.Indexes{})); err == nil {
		t.Fatal("namespace sem o prefixo reservado tem de ser recusado")
	}
}

func TestMetaContinuaRespondendoComORegistroVirtual(t *testing.T) {
	h := novoHarness(t)
	tabelaVirtualDeTeste(t, h.engine, "_counters", "x")
	rows := h.consultaOK("SELECT name FROM _meta.fields WHERE name = 'Acct-Status-Type'")
	if len(rows) != 1 {
		t.Fatalf("_meta.fields = %v", rows)
	}
}
