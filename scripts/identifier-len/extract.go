package main

import "encoding/json"

// ResultExtracao classifica a tentativa de extrair um campo do payload.
type ResultExtracao int

const (
	// ExtraidoOK: o payload é um objeto JSON e o campo existe como string.
	ExtraidoOK ResultExtracao = iota
	// ExtraidoWithoutField: o payload é um objeto JSON válido, mas o campo não
	// existe. Balde próprio — um tópico pode legitimamente ter mensagens sem
	// o campo-chave, e isso não é a mesma falha que payload corrompido.
	ExtraidoWithoutField
	// ExtraidoInvalido: o payload não parseia como objeto JSON, ou o campo
	// existe e não é uma string. Nos dois casos não há "tamanho em bytes da
	// string" para medir.
	ExtraidoInvalido
)

// ExtractField lê `campo` do payload JSON de uma mensagem e devolve seu valor
// como string, junto do resultado da extração.
//
// O tamanho que a issue #15 quer é o da STRING em bytes — é o que
// internal/storage/encoding/record.go:36 compara contra MaxKeySize —, então
// um campo presente mas não-string (número, objeto, array, bool, null) cai em
// ExtraidoInvalido: não é "sem campo" (o campo existe) e não é o que o teto de
// formato mede.
func ExtractField(payload []byte, field string) (string, ResultExtracao) {
	var doc map[string]any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return "", ExtraidoInvalido
	}
	v, presente := doc[field]
	if !presente {
		return "", ExtraidoWithoutField
	}
	s, ehString := v.(string)
	if !ehString {
		return "", ExtraidoInvalido
	}
	return s, ExtraidoOK
}
