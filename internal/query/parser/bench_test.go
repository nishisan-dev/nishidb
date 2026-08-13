package parser_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/parser"
)

// A query multi-linha da spec §72, como o driver JDBC a envia.
const querySpec72 = `SELECT service_id, ip, nas
FROM radius_sessions
WHERE status = ?
  AND nas = ?
LIMIT 100`

var sinkStmt *parser.SelectStmt

// Parse é caminho FRIO (uma vez por query): alocar a AST é esperado e o
// benchmark serve de referência, não de meta de otimização.
func BenchmarkParse(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(querySpec72)))
	for b.Loop() {
		st, err := parser.Parse(querySpec72)
		if err != nil {
			b.Fatal(err)
		}
		sinkStmt = st
	}
}

func BenchmarkParseComplexa(b *testing.B) {
	const sql = `SELECT id, status, ip, nas, "Acct-Status-Type" FROM radius_sessions ` +
		`WHERE (status = ? OR status = 'Stop') AND nas != 'x' AND octets >= 1000 ` +
		`AND "Framed-IP-Address" IS NOT NULL LIMIT 500`
	b.ReportAllocs()
	b.SetBytes(int64(len(sql)))
	for b.Loop() {
		st, err := parser.Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		sinkStmt = st
	}
}
