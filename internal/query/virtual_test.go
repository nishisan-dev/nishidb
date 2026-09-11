package query_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// tableVirtualOfTest monta um Store virtual com o formato de um contador
// (groupKey, olt, total) sobre o dublê, e o registra no motor sob o namespace
// dado. O dublê serve porque o motor só precisa de um query.Store.
func tableVirtualOfTest(t *testing.T, eng *query.Engine, namespace, table string) {
	t.Helper()
	spec, err := schema.Parse([]byte(`version: 1
name: `+table+`
schemaName: telecom
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
`), schema.BuiltinDefaults)
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
			name string
			v    types.Value
		}{
			{"groupKey", types.Value{Type: types.TypeString, Bytes: []byte(olt)}},
			{"olt", types.Value{Type: types.TypeString, Bytes: []byte(olt)}},
			{"total", types.Value{Type: types.TypeInt64, I64: int64(i * 2802)}},
		} {
			f, ok := sc.FieldByName([]byte(col.name))
			if !ok {
				t.Fatal(col.name)
			}
			if p, err = encoding.AppendField(p, f.ID, col.v); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := st.Put([]byte(olt), int64(i+1), 0, p, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.RegisterVirtual(dataset, namespace, table, sc, st); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteRoteiaTabelaVirtualRegistrada(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, "_counters", "autenticados_por_olt")
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
	h := newHarness(t)
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
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, "_counters", "servicos_por_olt")
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
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, "_counters", "x")
	spec, _ := schema.Parse([]byte("version: 1\nname: x\nschemaName: telecom\nmaterialization:\n  key:\n    field: k\nschema:\n  fields:\n    k:\n      type: string\n"), schema.BuiltinDefaults)
	sc, _ := spec.Compile(fakestorage.NewDict())
	if err := h.engine.RegisterVirtual(dataset, "_counters", "x", sc, fakestorage.New(fakestorage.Indexes{})); err == nil {
		t.Fatal("tabela virtual repetida tem de ser recusada")
	}
	if err := h.engine.RegisterVirtual("nao_registrado", "_counters", "y", sc, fakestorage.New(fakestorage.Indexes{})); err == nil {
		t.Fatal("dataset não registrado tem de ser recusado")
	}
	if err := h.engine.RegisterVirtual(dataset, "semprefixo", "y", sc, fakestorage.New(fakestorage.Indexes{})); err == nil {
		t.Fatal("namespace sem o prefixo reservado tem de ser recusado")
	}
}

func TestMetaContinuaRespondendoComORegistroVirtual(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, "_counters", "x")
	rows := h.consultaOK("SELECT name FROM _meta.fields WHERE name = 'Acct-Status-Type'")
	if len(rows) != 1 {
		t.Fatalf("_meta.fields = %v", rows)
	}
}

// TestRecusaDeRoteamentoVirtualEhErroDeBind: as três recusas de roteamento
// — namespace inexistente, namespace sem tabela nenhuma e tabela inexistente —
// são erro de QUEM ESCREVEU A QUERY, e têm de sair tipadas como tal.
//
// Sem o tipo, o servidor não as distingue de um bug e responde INTERNAL/500 a
// quem só digitou o nome de uma tabela que não existe: foi o que o piloto viu
// em 05/09/2026 com `SELECT a FROM _meta.nada` (issue #35). A posição aponta o
// nome DEPOIS do ponto, que é o token ofensor.
func TestRecusaDeRoteamentoVirtualEhErroDeBind(t *testing.T) {
	h := newHarness(t)
	if err := h.engine.DeclaraNamespace(dataset, "_counters"); err != nil {
		t.Fatal(err)
	}

	casos := []struct {
		name    string
		sql     string
		ofensor string // o texto que a posição tem de apontar
		naMsg   string
	}{
		{"namespace inexistente", "SELECT a FROM _outro.coisa", "coisa", "_outro"},
		{"namespace sem tabela nenhuma", "SELECT a FROM _counters.nao_existe", "nao_existe", "não declara tabela nenhuma"},
		{"tabela inexistente no _meta", "SELECT a FROM _meta.nada", "nada", `_meta não tem tabela "nada"`},
	}
	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			_, err := h.consulta(c.sql)
			if err == nil {
				t.Fatal("a recusa tem de acontecer")
			}
			if !errors.Is(err, binder.ErrBind) {
				t.Fatalf("a recusa tem de ser da família do bind (senão o servidor responde INTERNAL): %v", err)
			}
			var be *binder.Error
			if !errors.As(err, &be) {
				t.Fatalf("a recusa tem de carregar a posição para o cliente destacar o trecho: %v", err)
			}
			if be.Pos < 0 || be.Pos > len(c.sql) || !strings.HasPrefix(c.sql[be.Pos:], c.ofensor) {
				t.Fatalf("a posição %d tem de apontar %q em %q", be.Pos, c.ofensor, c.sql)
			}
			if !strings.Contains(err.Error(), c.naMsg) {
				t.Fatalf("a recusa tem de continuar nomeando o que existe (%q): %v", c.naMsg, err)
			}
		})
	}
}
