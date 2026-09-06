package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// RewriteGoComments é a passada Go, e o que ela toca num arquivo Go é
// exatamente isto, sem exceção:
//
//   - os COMENTÁRIOS, pelas mesmas regras dos arquivos de texto mais o doc
//     link, o qualificado `pkg.Ident`/`snap.Epoca` e o token nu;
//   - os literais de string que contêm um caminho MAPEADO ou uma CITAÇÃO de
//     identificador (`"LiberaBlocos em %s"`, `"PorQuery.Admite/Libera"`,
//     `"Presenca("`) — e nunca o caminho de um `import`, que é código: trocar
//     o caminho do import sem trocar o qualificador nas expressões deixa um
//     arquivo que não compila; nem a TAG de um campo de struct, que é formato
//     de disco e de wire;
//   - o primeiro argumento de `t.Run`, quando o arquivo tem subtestes no
//     mapeamento.
//
// Identificador em código é do rf; um que ainda esteja em PT-BR depois do rf
// é erro de mapeamento, não trabalho desta passada.
func RewriteGoComments(src []byte, m *Mapping) ([]byte, *Report) {
	return RewriteGoFile("", src, m)
}

// RewriteGoFile é RewriteGoComments mais os subtestes do arquivo dado. O nome
// também é o que diz de que PACOTE o arquivo é, e a regra do token com só a
// primeira maiúscula depende disso: `Epoca` no comentário de internal/types é
// citação do campo, e no de outro pacote é palavra.
func RewriteGoFile(name string, src []byte, m *Mapping) ([]byte, *Report) {
	m.buildIndex()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return src, &Report{Skipped: []Skipped{{Reason: "não parseia: " + err.Error()}}}
	}
	pkg := ""
	if name != "" {
		pkg = path.Dir(path.Clean(filepath.ToSlash(name)))
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
			out = m.applyProse(out, rep, base, proseGoComment, pkg)
			if out != body {
				spans = append(spans, span{start, end, out})
			}
		}
	}
	subs := m.Subtests[name]
	// A TAG de um campo é literal de string e é FORMATO — nome de chave JSON no
	// disco e no wire. Trocar um identificador dentro dela mudaria dado, não
	// prosa, e o AST é o único lugar onde essa diferença aparece.
	tags := map[token.Pos]bool{}
	// done marca o literal que o caso do t.Run já reescreveu: sem isso o
	// BasicLit abaixo alcança o MESMO literal e dois spans sobre o mesmo
	// intervalo se sobrescrevem.
	done := map[token.Pos]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if fld, ok := n.(*ast.Field); ok && fld.Tag != nil {
			tags[fld.Tag.Pos()] = true
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ImportSpec:
			// O caminho do import é CÓDIGO: reescrevê-lo sem reescrever o
			// qualificador das expressões (que é do rf) deixa um arquivo que
			// não compila. Não descer no nó é o que impede o BasicLit abaixo
			// de alcançá-lo.
			return false
		case *ast.BasicLit:
			if x.Kind != token.STRING || tags[x.Pos()] || done[x.Pos()] {
				return true
			}
			val, raw, ok := literalValue(x.Value)
			if !ok {
				return true
			}
			line := fset.Position(x.Pos()).Line
			newVal := m.applyPaths(val, rep, line, "literal")
			newVal = m.rewriteLiteralIdents(newVal, rep, line)
			if newVal != val {
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
				done[lit.Pos()] = true
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

// rewriteLiteralIdents reescreve as CITAÇÕES de identificador dentro de um
// literal de string. Mensagem de erro de produção, rótulo de tabela de casos e
// texto de falha de teste nomeiam método e tipo, e depois de um rename passam a
// mandar o leitor procurar o que não existe — `fmt.Errorf("%w: LiberaBlocos em
// %s")` estava, e continuou depois de duas ferramentas e uma revisão.
//
// # O LITERAL É ELEGÍVEL, DEPOIS OS TOKENS SÃO REESCRITOS
//
// A decisão é em dois passos de propósito. Primeiro o literal precisa CONTER
// uma forma que só citação tem: token com maiúscula interna que é nome velho
// (`"LiberaBlocos"`), endereço qualificado (`"PorQuery.Admite/Libera"`) ou nome
// de tipo mapeado seguido de `(` (`"Presenca("`). Só então os tokens são
// reescritos — e aí `Libera`, que sozinho seria palavra, é trocado junto,
// porque o literal já se identificou como citação.
//
// Token todo MINÚSCULO nunca é tocado, nem em literal elegível: os fixtures do
// repositório são NDJSON, YAML e SQL cheios de chaves minúsculas em PT-BR
// (`campos`, `sequencia`, `chave`), e um literal elegível por um token vizinho
// arrastaria o fixture inteiro junto. Aí não seria prosa reescrita, seria dado.
func (m *Mapping) rewriteLiteralIdents(val string, rep *Report, line int) string {
	if !m.literalCitesIdent(val) {
		return val
	}
	return replaceAll(reDottedToken, val, func(tok string, _ int) string {
		if m.Protected[tok] {
			rep.pula(line, tok, "protegido")
			return tok
		}
		if !strings.Contains(tok, ".") && !hasInnerUpper(tok) && !startsUpper(tok) {
			return tok
		}
		out, skipped := m.rewriteToken(tok)
		if skipped != nil {
			rep.pula(line, skipped.Token, skipped.Reason)
		}
		rep.aplica(line, tok, out, "literal")
		return out
	})
}

// literalCitesIdent é a elegibilidade do literal: ele contém alguma forma que
// só citação de identificador tem.
func (m *Mapping) literalCitesIdent(val string) bool {
	for _, loc := range reDottedToken.FindAllStringIndex(val, -1) {
		tok := val[loc[0]:loc[1]]
		if m.Protected[tok] {
			continue
		}
		switch {
		case strings.Contains(tok, "."):
			if out, _ := m.rewriteToken(tok); out != tok {
				return true
			}
		case hasInnerUpper(tok):
			if _, _, ok := m.resolveLeaf(tok); ok {
				return true
			}
		case loc[1] < len(val) && val[loc[1]] == '(':
			if _, _, ok := m.resolveType("", tok); ok {
				return true
			}
		}
	}
	return false
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
