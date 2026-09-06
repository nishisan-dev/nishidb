package main

import "testing"

// TestExtraiCampo cobre os quatro desfechos que a issue #15 precisa
// distinguir: valor extraído, campo ausente, JSON que não parseia e campo
// presente mas que não é string (não dá para medir "tamanho em bytes da
// string" do que não é string).
func TestExtraiCampo(t *testing.T) {
	casos := []struct {
		nome          string
		payload       string
		campo         string
		querValor     string
		querResultado ResultadoExtracao
	}{
		{
			nome:          "campo presente e string",
			payload:       `{"identifier":"abc123","outro":1}`,
			campo:         "identifier",
			querValor:     "abc123",
			querResultado: ExtraidoOK,
		},
		{
			nome:          "campo ausente em JSON válido",
			payload:       `{"outro":1}`,
			campo:         "identifier",
			querValor:     "",
			querResultado: ExtraidoSemCampo,
		},
		{
			nome:          "payload não é JSON",
			payload:       `{isso nao e json}`,
			campo:         "identifier",
			querValor:     "",
			querResultado: ExtraidoInvalido,
		},
		{
			nome:          "payload vazio",
			payload:       ``,
			campo:         "identifier",
			querValor:     "",
			querResultado: ExtraidoInvalido,
		},
		{
			nome:          "campo presente mas não é string",
			payload:       `{"identifier":12345}`,
			campo:         "identifier",
			querValor:     "",
			querResultado: ExtraidoInvalido,
		},
		{
			nome:          "campo presente e string vazia conta como OK com comprimento zero",
			payload:       `{"identifier":""}`,
			campo:         "identifier",
			querValor:     "",
			querResultado: ExtraidoOK,
		},
		{
			nome:          "string com bytes multibyte mede em BYTES, não em runas",
			payload:       `{"identifier":"áéí"}`, // 3 runas, 6 bytes em UTF-8
			campo:         "identifier",
			querValor:     "áéí",
			querResultado: ExtraidoOK,
		},
		{
			nome:          "campo de nome configurável, não só identifier",
			payload:       `{"outroCampo":"xyz"}`,
			campo:         "outroCampo",
			querValor:     "xyz",
			querResultado: ExtraidoOK,
		},
	}

	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			valor, resultado := ExtraiCampo([]byte(c.payload), c.campo)
			if resultado != c.querResultado {
				t.Errorf("resultado = %v, quer %v", resultado, c.querResultado)
			}
			if valor != c.querValor {
				t.Errorf("valor = %q, quer %q", valor, c.querValor)
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
