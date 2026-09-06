package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
)

// RewriteGoComments reescreve só os COMENTÁRIOS e os literais de caminho de
// um arquivo Go. Código é do rf; um identificador em código que ainda esteja
// em PT-BR depois do rf é erro de mapeamento, não trabalho desta passada.
func RewriteGoComments(src []byte, m *Mapping) ([]byte, []Skipped) {
	return RewriteGoFile("", src, m)
}

// RewriteGoFile é RewriteGoComments mais os subtestes do arquivo dado.
func RewriteGoFile(name string, src []byte, m *Mapping) ([]byte, []Skipped) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return src, []Skipped{{Reason: "não parseia: " + err.Error()}}
	}
	type span struct {
		start, end int
		text       string
	}
	var spans []span
	var skips []Skipped
	for _, g := range f.Comments {
		for _, c := range g.List {
			start := fset.Position(c.Pos()).Offset
			end := fset.Position(c.End()).Offset
			body := c.Text
			line := func(int) int { return fset.Position(c.Pos()).Line }
			out := m.applyCommon(body, &skips, line)
			out = m.applyGoCommentExtras(out, &skips)
			if out != body {
				spans = append(spans, span{start, end, out})
			}
		}
	}
	subs := m.Subtests[name]
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			val, err := strconv.Unquote(x.Value)
			if err != nil {
				return true
			}
			if newVal := m.applyPaths(val); newVal != val {
				spans = append(spans, span{fset.Position(x.Pos()).Offset, fset.Position(x.End()).Offset, strconv.Quote(newVal)})
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
			val, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			if newVal, ok := subs[val]; ok {
				spans = append(spans, span{fset.Position(lit.Pos()).Offset, fset.Position(lit.End()).Offset, strconv.Quote(newVal)})
			}
		}
		return true
	})
	if len(spans) == 0 {
		return src, skips
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start > spans[j].start })
	out := append([]byte(nil), src...)
	for _, s := range spans {
		out = append(out[:s.start], append([]byte(s.text), out[s.end:]...)...)
	}
	return out, skips
}
