package parser_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

func TestProjectionAliasSyntax(t *testing.T) {
	for _, syntax := range []string{`a AS x`, `a x`, `a aS x`, `doc.a AS x`, `"distinct" x`, `distinct AS x`, `as AS x`, `distinct.path x`} {
		st := parseOK(t, "SELECT "+syntax+" FROM t ORDER BY x")
		ref := st.Projection[0]
		if ref.Alias != "x" || ref.AliasPos != 7+strings.LastIndex(syntax, "x") {
			t.Fatalf("alias = %+v", ref)
		}
	}
	for _, word := range []string{"select", "from", "where", "limit", "and", "or", "is", "not", "null", "true", "false", "as", "order", "by", "asc", "desc", "nulls", "first", "last", "offset", "like", "group", "join", "having", "distinct", "union"} {
		for _, form := range []string{" AS ", " "} {
			prefix := "SELECT a" + form
			sql := prefix + strings.ToUpper(word) + " FROM t"
			_, err := parser.Parse(sql)
			if err == nil {
				t.Fatalf("alias reservado aceito: %s", sql)
			}
			st := parseOK(t, prefix+`"`+word+`" FROM t`)
			if st.Projection[0].Alias != word || st.Projection[0].AliasPos != len(prefix) {
				t.Fatalf("alias entre aspas = %+v", st.Projection)
			}
		}
	}
	st := parseOK(t, `SELECT a AS "ação ""especial""", b "doc.x" FROM t`)
	if st.Projection[0].Alias != `ação "especial"` || st.Projection[1].Alias != "doc.x" {
		t.Fatalf("escapes = %+v", st.Projection)
	}
}

func TestProjectionAliasErrorsHavePositions(t *testing.T) {
	for _, tc := range []struct{ sql, token, message string }{
		{`SELECT a AS FROM t`, "FROM", "expected alias"},
		{`SELECT a AS "" FROM t`, `""`, "identificador entre aspas vazio"},
		{`SELECT a AS 123 FROM t`, "123", "expected alias"},
		{`SELECT a AS ? FROM t`, "?", "expected alias"},
		{`SELECT a AS 'x' FROM t`, "'x'", "expected alias"},
		{`SELECT a AS ORDER FROM t`, "ORDER", "reserved word"},
		{`SELECT a ORDER FROM t`, "ORDER", "reserved word"},
		{`SELECT * AS x FROM t`, "AS", "wildcard"},
		{`SELECT * x FROM t`, "x", "wildcard"},
		{`SELECT doc.* AS x FROM t`, "AS", "wildcard"},
		{`SELECT doc.* x FROM t`, "x", "wildcard"},
	} {
		_, err := parser.Parse(tc.sql)
		var se *parser.SyntaxError
		if !errors.As(err, &se) || se.Pos != strings.Index(tc.sql, tc.token) || !strings.Contains(err.Error(), tc.message) {
			t.Fatalf("%s: erro = %v", tc.sql, err)
		}
	}
	if _, err := parser.Parse("SELECT a AS"); err == nil {
		t.Fatal("AS sem alias foi aceito")
	}
}
