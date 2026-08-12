// Package unsafex concentra as conversões unsafe permitidas no projeto.
// Este é o ÚNICO lugar do M1 autorizado a importar unsafe; qualquer novo uso
// deve ser adicionado aqui, com contrato documentado e revisado.
package unsafex

import "unsafe"

// String converte b em string sem copiar os bytes.
//
// Contrato (responsabilidade do CALLER garantir):
//   - a string retornada aliasa o array subjacente de b: mutações em b
//     aparecem na string, violando a imutabilidade esperada de strings Go;
//   - o callee que recebe a string NÃO pode retê-la além do escopo da
//     chamada (uso típico: argumentos de parse como netip.ParseAddr e
//     strconv.ParseFloat, que não retêm);
//   - b não pode ser reutilizado/mutado enquanto a string estiver viva.
//
// Uso correto é pontual e auditável; na dúvida, use string(b) e pague a cópia.
func String(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
