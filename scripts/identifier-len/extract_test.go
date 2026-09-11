package main

import "testing"

// TestExtraiCampo cobre os quatro desfechos que a issue #15 precisa
// distinguir: valor extraído, campo ausente, JSON que não parseia e campo
// presente mas que não é string (não dá para medir "tamanho em bytes da
// string" do que não é string).
func TestExtraiCampo(t *testing.T) {
	casos := []struct {
		name       string
		payload    string
		field      string
		querValue  string
		querResult ResultadoExtracao
	}{
		{
			name:       "campo presente e string",
			payload:    `{"identifier":"abc123","outro":1}`,
			field:      "identifier",
			querValue:  "abc123",
			querResult: ExtraidoOK,
		},
		{
			name:       "campo ausente em JSON válido",
			payload:    `{"outro":1}`,
			field:      "identifier",
			querValue:  "",
			querResult: ExtraidoSemCampo,
		},
		{
			name:       "payload não é JSON",
			payload:    `{isso nao e json}`,
			field:      "identifier",
			querValue:  "",
			querResult: ExtraidoInvalido,
		},
		{
			name:       "payload vazio",
			payload:    ``,
			field:      "identifier",
			querValue:  "",
			querResult: ExtraidoInvalido,
		},
		{
			name:       "campo presente mas não é string",
			payload:    `{"identifier":12345}`,
			field:      "identifier",
			querValue:  "",
			querResult: ExtraidoInvalido,
		},
		{
			name:       "campo presente e string vazia conta como OK com comprimento zero",
			payload:    `{"identifier":""}`,
			field:      "identifier",
			querValue:  "",
			querResult: ExtraidoOK,
		},
		{
			name:       "string com bytes multibyte mede em BYTES, não em runas",
			payload:    `{"identifier":"áéí"}`, // 3 runas, 6 bytes em UTF-8
			field:      "identifier",
			querValue:  "áéí",
			querResult: ExtraidoOK,
		},
		{
			name:       "campo de nome configurável, não só identifier",
			payload:    `{"outroCampo":"xyz"}`,
			field:      "outroCampo",
			querValue:  "xyz",
			querResult: ExtraidoOK,
		},
	}

	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			valor, resultado := ExtraiCampo([]byte(c.payload), c.field)
			if resultado != c.querResult {
				t.Errorf("resultado = %v, quer %v", resultado, c.querResult)
			}
			if valor != c.querValue {
				t.Errorf("valor = %q, quer %q", valor, c.querValue)
			}
		})
	}
}

// TestExtraiCampoMedeBytesNaoRunas prova a exigência explícita do brief: o
// comprimento relevante é EM BYTES, não em runas — um identifier com
// caracteres multibyte tem de contar mais bytes que runas.
func TestExtraiCampoMedeBytesNaoRunas(t *testing.T) {
	valor, resultado := ExtraiCampo([]byte(`{"identifier":"áéí"}`), "identifier")
	if resultado != ExtraidoOK {
		t.Fatalf("resultado = %v, quer ExtraidoOK", resultado)
	}
	if got := len(valor); got != 6 {
		t.Errorf("len(valor) = %d bytes, quer 6 (3 runas acentuadas, 2 bytes cada em UTF-8)", got)
	}
}
