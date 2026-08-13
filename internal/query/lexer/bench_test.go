package lexer_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
)

const (
	querySpec68   = "SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100"
	queryComplexa = `SELECT "Acct-Status-Type", id FROM radius_sessions ` +
		`WHERE ("Framed-IP-Address" = '10.0.0.1' OR nas >= 10) AND status IS NOT NULL LIMIT 500;`
	queryComEscapes = `SELECT id FROM s WHERE a = 'it''s' AND b = 'd''oh' AND c = 'y''all'`
)

// sink impede que o compilador elimine o laço do benchmark.
var sink lexer.Token

func lexBench(b *testing.B, sql string) {
	b.Helper()
	var l lexer.Lexer
	b.ReportAllocs()
	b.SetBytes(int64(len(sql)))
	for b.Loop() {
		l.Init(sql)
		for {
			tk, err := l.Next()
			if err != nil {
				b.Fatal(err)
			}
			sink = tk
			if tk.Type == lexer.TokenEOF {
				break
			}
		}
	}
}

// Gate da Q11: query sem escapes = 0 allocs/op. Todo Text é subslice do
// fonte, Token é valor e o Lexer é reutilizado.
func BenchmarkLexSpec68(b *testing.B) { lexBench(b, querySpec68) }

func BenchmarkLexQueryComplexa(b *testing.B) { lexBench(b, queryComplexa) }

// Baseline documentado: cada literal COM escape custa uma cópia (é o que dá
// ao Token o contrato "Text válido para sempre"). Cresce com o número de
// literais escapados, nunca com o número de tokens.
func BenchmarkLexComEscapes(b *testing.B) { lexBench(b, queryComEscapes) }
