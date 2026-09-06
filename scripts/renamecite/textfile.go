package main

import "strings"

// RewriteText aplica as regras comuns a um arquivo que não é Go: .md, .puml,
// .sh, .py, .yml, Makefile. Nada além de crase, aspas (chave de log), nome
// de teste e caminho é tocado — prosa solta é do glossário.
//
// A extensão entra na assinatura porque é por ela que quem chama despacha
// entre esta passada e a de Go, e porque é o eixo por onde uma regra de um
// formato só entraria; hoje as regras são as mesmas para todo arquivo de
// texto, então ela não é lida aqui.
func RewriteText(src []byte, _ string, m *Mapping) ([]byte, []Skipped) {
	var skips []Skipped
	s := string(src)
	line := func(off int) int { return 1 + strings.Count(s[:max(0, min(off, len(s)))], "\n") }
	out := m.applyCommon(s, &skips, line)
	return []byte(out), skips
}
