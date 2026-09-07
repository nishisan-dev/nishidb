package binder_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/binder"
	"github.com/nishisan-dev/nishidb/internal/query/parser"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// restart simula um RESTART: o dicionário do dataset já tem o campo auto que
// uma execução anterior materializou (com ou sem tipo fixado) e o Compiled é
// construído do zero a partir do YAML.
func restart(t *testing.T, field string, typ types.FieldType) *schema.Compiled {
	t.Helper()
	dict := fakestorage.NewDict()
	id, err := dict.LookupOrCreate([]byte(field))
	if err != nil {
		t.Fatal(err)
	}
	if typ != types.TypeNull {
		if err := dict.SetTypeHint(id, typ); err != nil {
			t.Fatal(err)
		}
	}
	spec, err := schema.Parse([]byte(dsYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func bindWith(t *testing.T, sc *schema.Compiled, sql string) (*binder.BoundQuery, error) {
	t.Helper()
	st, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%q): %v", sql, err)
	}
	return binder.Bind(st, sc, nil)
}

// O caso que motiva a rota read-only: o campo NÃO está no YAML, só no
// dicionário. Antes disso o binder respondia "coluna não existe" para uma
// coluna que tem dados — e o operador concluiria que perdeu os eventos.
func TestBindCampoSoNoDicionarioAposRestart(t *testing.T) {
	sc := restart(t, "X-Vendor-Attr", types.TypeString)

	bq, err := bindWith(t, sc, `SELECT id FROM radius_sessions WHERE "X-Vendor-Attr" = 'abc'`)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	cmp, ok := bq.Where.(*binder.BoundComparison)
	if !ok {
		t.Fatalf("Where = %T, want *BoundComparison", bq.Where)
	}
	if cmp.Field.Name != "X-Vendor-Attr" {
		t.Errorf("campo = %q", cmp.Field.Name)
	}
	// O tipo fixado sobreviveu ao restart: o literal foi coerido contra ele.
	if cmp.Val.Type != types.TypeString || string(cmp.Val.Bytes) != "abc" {
		t.Errorf("Val = %+v, want string \"abc\"", cmp.Val)
	}
}

// SELECT * enxerga a coluna materializada antes do restart.
func TestBindStarIncluiCampoDoDicionario(t *testing.T) {
	sc := restart(t, "X-Vendor-Attr", types.TypeString)

	bq, err := bindWith(t, sc, "SELECT * FROM radius_sessions")
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	for _, col := range bq.Columns {
		if col.Field.Name == "X-Vendor-Attr" {
			return
		}
	}
	t.Errorf("SELECT * = %v, want incluir X-Vendor-Attr", columnNames(bq.Columns))
}

// Campo no dicionário SEM tipo fixado (criado por um evento que trouxe null):
// a comparação é recusada, mas com a mensagem certa. "Coluna não existe"
// seria mentira e mandaria o operador procurar erro de digitação; "sem tipo"
// diz a verdade — o dataset ainda não viu valor nenhum neste campo.
func TestBindCampoDoDicionarioSemTipoRecusaComMensagemCerta(t *testing.T) {
	sc := restart(t, "X-Sem-Tipo", types.TypeNull)

	_, err := bindWith(t, sc, `SELECT id FROM radius_sessions WHERE "X-Sem-Tipo" = 'abc'`)
	if err == nil {
		t.Fatal("comparação sobre campo sem tipo deveria falhar")
	}
	if strings.Contains(err.Error(), "não existe") {
		t.Errorf("erro = %q, want NÃO dizer que a coluna não existe", err)
	}
	if !strings.Contains(err.Error(), "nunca foi observada") {
		t.Errorf("erro = %q, want explicar que o campo ainda não tem tipo", err)
	}

	// E o IS NULL sobre ele continua válido: presença não depende de tipo.
	if _, err := bindWith(t, sc, `SELECT id FROM radius_sessions WHERE "X-Sem-Tipo" IS NULL`); err != nil {
		t.Errorf("IS NULL sobre campo sem tipo: %v", err)
	}
}

func columnNames(cols []binder.BoundColumn) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}
