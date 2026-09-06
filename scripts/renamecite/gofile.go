package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

// RewriteGoComments é a passada Go, e o que ela toca num arquivo Go é
// exatamente isto, sem exceção:
//
//   - os COMENTÁRIOS, pelas mesmas regras dos arquivos de texto mais o doc
//     link e o qualificado `pkg.Ident`;
//   - os literais de string que contêm um caminho MAPEADO — e nunca o caminho
//     de um `import`, que é código: trocar o caminho do import sem trocar o
//     qualificador nas expressões deixa um arquivo que não compila;
//   - o primeiro argumento de `t.Run`, quando o arquivo tem subtestes no
//     mapeamento.
//
// Identificador em código é do rf; um que ainda esteja em PT-BR depois do rf
// é erro de mapeamento, não trabalho desta passada.
func RewriteGoComments(src []byte, m *Mapping) ([]byte, *Report) {
	return RewriteGoFile("", src, m)
}

// RewriteGoFile é RewriteGoComments mais os subtestes do arquivo dado.
func RewriteGoFile(name string, src []byte, m *Mapping) ([]byte, *Report) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return src, &Report{Skipped: []Skipped{{Reason: "não parseia: " + err.Error()}}}
	}
	type span struct {
		start, end int
		text       string
	}
	var spans []span
	rep := &Report{}
	for _, g := range f.Comments {
		for _, c := range g.List {
			start := fset.Position(c.Pos()).Offset
			end := fset.Position(c.End()).Offset
			body := c.Text
			base := fset.Position(c.Pos()).Line
			out := m.applyCommon(body, rep, base)
			out = m.applyGoCommentExtras(out, rep, base)
			if out != body {
				spans = append(spans, span{start, end, out})
			}
		}
	}
	subs := m.Subtests[name]
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ImportSpec:
			// O caminho do import é CÓDIGO: reescrevê-lo sem reescrever o
			// qualificador das expressões (que é do rf) deixa um arquivo que
			// não compila. Não descer no nó é o que impede o BasicLit abaixo
			// de alcançá-lo.
			return false
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			val, raw, ok := literalValue(x.Value)
			if !ok {
				return true
			}
			if newVal := m.applyPaths(val, rep, fset.Position(x.Pos()).Line, "literal"); newVal != val {
				spans = append(spans, span{fset.Position(x.Pos()).Offset, fset.Position(x.End()).Offset, quoteLike(newVal, raw)})
			}
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Run" || len(x.Args) == 0 || subs == nil {
				return true
			}
			lit, ok := x.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			val, raw, ok := literalValue(lit.Value)
			if !ok {
				return true
			}
			if newVal, ok := subs[val]; ok {
				rep.aplica(fset.Position(lit.Pos()).Line, val, newVal, "subteste")
				spans = append(spans, span{fset.Position(lit.Pos()).Offset, fset.Position(lit.End()).Offset, quoteLike(newVal, raw)})
			}
		}
		return true
	})
	if len(spans) == 0 {
		return src, rep
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start > spans[j].start })
	out := append([]byte(nil), src...)
	for _, s := range spans {
		out = append(out[:s.start], append([]byte(s.text), out[s.end:]...)...)
	}
	return out, rep
}

// literalValue devolve o CONTEÚDO de um literal de string Go, se ele é bruto
// (entre crases) e se deu para ler.
//
// O literal bruto é lido fatiando as crases, e não por strconv.Unquote, de
// propósito: Unquote descarta os `\r` de um literal bruto, e esta passada não
// tem licença para mexer em nada além do caminho.
func literalValue(lit string) (value string, raw, ok bool) {
	if strings.HasPrefix(lit, "`") && strings.HasSuffix(lit, "`") && len(lit) >= 2 {
		return lit[1 : len(lit)-1], true, true
	}
	v, err := strconv.Unquote(lit)
	if err != nil {
		return "", false, false
	}
	return v, false, true
}

// quoteLike reemite o valor no MESMO formato do literal original. Um literal
// bruto de várias linhas reemitido com strconv.Quote vira uma linha só com
// `\n` escapado — e o repositório é cheio de fixtures assim. O único caso em
// que o bruto não volta bruto é o valor que contém uma crase, que o literal
// bruto não sabe representar.
func quoteLike(value string, raw bool) string {
	if raw && !strings.Contains(value, "`") {
		return "`" + value + "`"
	}
	return strconv.Quote(value)
}
