package metareplication_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metareplication"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const dataset = "events"

// engineWithReplication registra o dataset de mentira e a
// `_meta.replication` sobre a fonte dada — a mesma costura do Register, sem
// servidor.
func engineWithReplication(t *testing.T, src metareplication.Source) *query.Engine {
	t.Helper()
	spec, err := schema.Parse([]byte("version: 1\nname: "+dataset+"\nschemaName: telecom\nmaterialization:\n  key:\n    field: id\nschema:\n  fields:\n    id:\n      type: string\n"), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register(dataset, sc, fakestorage.New(fakestorage.Indexes{})); err != nil {
		t.Fatal(err)
	}
	if err := metareplication.Register(eng, dataset, src); err != nil {
		t.Fatal(err)
	}
	return eng
}

// resultRow é uma linha do resultado, coluna → valor renderizado.
type resultRow map[string]string

// querySQL executa e devolve as linhas, com as colunas na ordem do
// resultado em names.
func querySQL(t *testing.T, eng *query.Engine, sql string) (rows []resultRow, names []string) {
	t.Helper()
	it, err := eng.Execute(context.Background(), dataset, sql, nil)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	for it.Next() {
		r := make(resultRow, it.Row().Len())
		var cols []string
		for _, c := range it.Row().Cols() {
			r[c.Name] = render(c)
			cols = append(cols, c.Name)
		}
		if names == nil {
			names = cols
		}
		rows = append(rows, r)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return rows, names
}

func render(c executor.ColView) string {
	if c.Null {
		return "<null>"
	}
	switch c.Val.Type {
	case types.TypeString:
		return string(c.Val.Bytes)
	case types.TypeInt64, types.TypeTimestamp:
		return strconv.FormatInt(c.Val.I64, 10)
	default:
		return "?"
	}
}

// column junta os valores da coluna, na ordem das linhas.
func column(rows []resultRow, col string) string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r[col])
	}
	return strings.Join(out, ",")
}
