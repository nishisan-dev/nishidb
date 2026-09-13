package metausers_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metausers"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/users"
)

var t0 = time.Date(2026, 9, 13, 10, 0, 0, 0, time.UTC)

// engineWith registra um dataset de mentira e a tabela sob ele.
func engineWith(t *testing.T, rows func() []users.UserView) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte("version: 1\nname: servicos\nschemaName: telecom\n"+
		"materialization:\n  key:\n    field: id\nschema:\n  fields:\n    id:\n      type: string\n"),
		schema.BuiltinDefaults)
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
	if err := eng.DeclaraNamespace("servicos", meta.Namespace); err != nil {
		t.Fatal(err)
	}
	if err := metausers.Register(eng, "servicos", metausers.Source{Users: rows}); err != nil {
		t.Fatal(err)
	}
	return eng
}

func sampleRows() []users.UserView {
	return []users.UserView{
		{Username: "admin", Role: "ADMIN", Bootstrap: true, CreatedAt: t0, CreatedBy: "bootstrap",
			UpdatedAt: t0, UpdatedBy: "bootstrap", PasswordChangedAt: t0},
		{Username: "leitor", Role: "READ", CreatedAt: t0.Add(time.Hour), CreatedBy: "admin",
			UpdatedAt: t0.Add(2 * time.Hour), UpdatedBy: "leitor", PasswordChangedAt: t0.Add(2 * time.Hour)},
	}
}

type row map[string]string

func queryRows(t *testing.T, ctx context.Context, eng *query.Engine, sql string) ([]row, []string, error) {
	t.Helper()
	it, err := eng.Execute(ctx, "servicos", sql, nil)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = it.Close() }()
	var names []string
	for _, c := range it.Columns() {
		names = append(names, c.Name)
	}
	var out []row
	for it.Next() {
		r := row{}
		for _, c := range it.Row().Cols() {
			switch {
			case c.Null:
				r[c.Name] = "<null>"
			case len(c.Val.Bytes) > 0:
				r[c.Name] = string(c.Val.Bytes)
			default:
				r[c.Name] = strconv.FormatInt(c.Val.I64, 10)
			}
		}
		out = append(out, r)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out, names, nil
}

// Com a marca de ADMIN, a tabela responde nome, papel e quem mexeu quando.
func TestAdminLeATabelaDeUsuariosSemOHash(t *testing.T) {
	eng := engineWith(t, sampleRows)
	got, names, err := queryRows(t, query.WithAdmin(context.Background()), eng, "SELECT * FROM _meta.users")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("%d linhas, quer 2", len(got))
	}
	if got[0]["username"] != "admin" || got[0]["role"] != "ADMIN" || got[0]["bootstrap"] != "1" ||
		got[0]["createdBy"] != "bootstrap" {
		t.Errorf("linha do admin = %+v", got[0])
	}
	if got[1]["username"] != "leitor" || got[1]["bootstrap"] != "0" || got[1]["updatedBy"] != "leitor" ||
		got[1]["passwordChangedAt"] != strconv.FormatInt(t0.Add(2*time.Hour).UnixMilli(), 10) {
		t.Errorf("linha do leitor = %+v", got[1])
	}
	// SELECT * é o teste do que o schema expõe: nenhuma coluna de segredo.
	for _, n := range names {
		for _, secret := range []string{"hash", "salt", "key", "password"} {
			if strings.Contains(strings.ToLower(n), secret) && n != "passwordChangedAt" {
				t.Errorf("a tabela expõe a coluna %q", n)
			}
		}
	}
}

// Sem a marca, a recusa vem antes do bind — inclusive para uma coluna que não
// existe, que de outro modo revelaria o schema pela mensagem.
func TestSemAMarcaDeAdminATabelaDeUsuariosEhRecusada(t *testing.T) {
	eng := engineWith(t, sampleRows)
	for _, sql := range []string{
		"SELECT username FROM _meta.users",
		"SELECT coluna_que_nao_existe FROM _meta.users",
	} {
		_, _, err := queryRows(t, context.Background(), eng, sql)
		if !errors.Is(err, query.ErrForbidden) {
			t.Errorf("%s: erro = %v, quer ErrForbidden", sql, err)
		}
	}
	// As outras tabelas do `_meta` continuam abertas para qualquer consulta.
	if _, _, err := queryRows(t, context.Background(), eng, "SELECT name FROM _meta.fields"); err != nil {
		t.Errorf("_meta.fields passou a exigir ADMIN: %v", err)
	}
}

func TestTabelaDeUsuariosVaziaNaoEhErro(t *testing.T) {
	ctx := query.WithAdmin(context.Background())
	if got, _, err := queryRows(t, ctx, engineWith(t, nil), "SELECT username FROM _meta.users"); err != nil || len(got) != 0 {
		t.Fatalf("linhas = %+v, erro = %v", got, err)
	}
}
