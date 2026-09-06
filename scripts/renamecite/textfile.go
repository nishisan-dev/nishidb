package main

// RewriteText aplica as regras a um arquivo que não é Go: .md, .puml, .sh,
// .py, .yml, Makefile.
//
// A EXTENSÃO decide quanto a passada tem licença para fazer, e é por isso que
// ela está na assinatura. Em `.md` e `.puml` valem também as regras de prosa —
// a forma qualificada (`snap.Epoca`) e o token nu —, porque ali o texto é
// documentação do repositório. Em `.sh`, `.py`, `.yml` e `.java` valem só
// crase, aspas (chave de log), nome de teste e caminho: naqueles arquivos a
// forma `x.Ident` é CÓDIGO, e este comando não tem licença sobre código.
func RewriteText(src []byte, ext string, m *Mapping) ([]byte, *Report) {
	m.buildIndex()
	rep := &Report{}
	out := m.applyCommon(string(src), rep, 1)
	out = m.applyProse(out, rep, 1, proseKindOf(ext), "")
	return []byte(out), rep
}

// proseKindOf traduz a extensão do arquivo na licença da passada de prosa.
func proseKindOf(ext string) proseKind {
	switch ext {
	case ".md":
		return proseMarkdown
	case ".puml":
		return prosePUML
	default:
		return proseOther
	}
}
