package lexer_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/lexer"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
)

var numerosValidos = []string{
	"0", "-0", "7", "123", "-1", "1.5", "-1.5e10", "1E-3", "2e+9",
	"0.0001", "9223372036854775807",
	// Overflow é problema do conversor (Q12/typeparse.Int64), não do lexer:
	// a gramática está correta.
	"99999999999999999999",
}

var numerosInvalidos = []string{
	"007", "-007", "1.", "1e", "1E+", "1.2.3", "123abc", "1_000", "0x1f",
}

func TestNumerosValidos(t *testing.T) {
	for _, sql := range numerosValidos {
		t.Run(sql, func(t *testing.T) {
			// Text é o lexema VERBATIM: nada de normalizar sinal ou expoente.
			wantStream(t, sql, []tok{{lexer.TokenNumber, sql, 0}})
		})
	}
}

func TestNumerosInvalidos(t *testing.T) {
	for _, sql := range numerosInvalidos {
		t.Run(sql, func(t *testing.T) {
			err := lexUntilErr(t, sql)
			if !strings.Contains(err.Error(), "literal numérico inválido") {
				t.Errorf("erro = %q, want literal numérico inválido", err)
			}
			// A posição é a do INÍCIO do lexema — é o que o usuário corrige,
			// não o byte ofensor no meio.
			if !strings.Contains(err.Error(), "posição 0:") {
				t.Errorf("erro = %q, want posição 0 (início do lexema)", err)
			}
		})
	}
}

// Estes não são erro de NÚMERO e sim de dispatch: a distinção importa porque
// a mensagem muda o que o usuário procura.
func TestNaoSaoErroDeNumero(t *testing.T) {
	casos := []struct {
		sql string
		msg string
	}{
		{".5", "caractere inesperado '.'"},
		{"-", "caractere inesperado '-'"},
		{"- 1", "caractere inesperado '-'"},
		{"--1", "caractere inesperado '-'"},
	}
	for _, tc := range casos {
		t.Run(tc.sql, func(t *testing.T) {
			err := lexUntilErr(t, tc.sql)
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want %q", err, tc.msg)
			}
			if strings.Contains(err.Error(), "numérico") {
				t.Errorf("erro = %q: é erro de dispatch, não de número", err)
			}
		})
	}
}

// Anti-drift: a gramática numérica do lexer e a da INGESTÃO precisam ser a
// mesma, senão um literal aceito na query não converte (ou converte
// diferente) do valor gravado. O lexer não importa typeparse no código de
// produção — a ausência de drift é pinada AQUI, no teste.
func TestNumeroCompativelComTypeparse(t *testing.T) {
	for _, sql := range numerosValidos {
		t.Run("aceito/"+sql, func(t *testing.T) {
			got := lexAll(t, sql)
			if got[0].Type != lexer.TokenNumber {
				t.Fatalf("%q não virou número: %v", sql, got[0])
			}
			if !typeparse.JSONNumber([]byte(got[0].Text)) {
				t.Errorf("lexer aceitou %q, mas typeparse.JSONNumber recusa: gramáticas divergiram", got[0].Text)
			}
		})
	}
	// Os recusados pelo lexer que são lexemas numéricos completos também
	// precisam ser recusados pelo typeparse (os com lixo aderente, como
	// "123abc", o lexer nunca entregaria inteiros).
	for _, sql := range []string{"007", "-007", "1.", "1e", "1E+", "1.2.3"} {
		t.Run("recusado/"+sql, func(t *testing.T) {
			if typeparse.JSONNumber([]byte(sql)) {
				t.Errorf("lexer recusou %q, mas typeparse.JSONNumber aceita: gramáticas divergiram", sql)
			}
		})
	}
}
