package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// IdentEntry é um identificador do mapeamento: pacote (diretório), nome
// velho (`Fila`, `Fila.Empurra`) e nome novo.
type IdentEntry struct {
	Pkg, Old, New string
}

// Mapping é tudo que a passada de prosa sabe substituir.
type Mapping struct {
	Idents   []IdentEntry
	Tests    map[string]string
	LogKeys  map[string]string
	Paths    map[string]string
	Packages map[string]string            // nome curto velho → novo (contadores → counters)
	Subtests map[string]map[string]string // arquivo → velho → novo
}

// Skipped é uma ocorrência que casou com um nome do mapeamento mas ficou
// de fora por regra (ambiguidade, palavra solta). Vai para o relatório.
type Skipped struct {
	Line   int
	Token  string
	Reason string
}

// NewMapping devolve um mapeamento vazio com todos os mapas prontos: quem
// carrega os TSV escreve direto neles, sem checar nil a cada linha.
func NewMapping() *Mapping {
	return &Mapping{Tests: map[string]string{}, LogKeys: map[string]string{}, Paths: map[string]string{}, Packages: map[string]string{}, Subtests: map[string]map[string]string{}}
}

// AddIdent registra um identificador do pacote dado: `old` é o último
// elemento do endereço, com o prefixo `T.` para método e campo.
func (m *Mapping) AddIdent(pkg, old, newName string) {
	m.Idents = append(m.Idents, IdentEntry{pkg, old, newName})
}

// resolve decide o nome novo de um token entre crases. `qual` é o
// qualificador (pacote curto ou tipo), vazio quando o token vem nu.
//
// Nu: substitui só se todo pacote que declara `old` concorda no `new`.
// Qualificado por pacote (velho ou novo nome curto): só as entradas daquele
// pacote. Qualificado por tipo (`Fila.Empurra`): o próprio Old da entrada.
func (m *Mapping) resolve(qual, old string) (string, string, bool) {
	var candidates []IdentEntry
	for _, e := range m.Idents {
		if e.Old != old {
			continue
		}
		if qual == "" || shortPkg(e.Pkg) == qual || m.Packages[shortPkg(e.Pkg)] == qual {
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 {
		return "", "", false
	}
	newName := candidates[0].New
	for _, c := range candidates[1:] {
		if c.New != newName {
			return "", fmt.Sprintf("ambíguo: %s vira %q em %s e %q em %s", old, newName, candidates[0].Pkg, c.New, c.Pkg), false
		}
	}
	return newName, "", true
}

// shortPkg devolve o último elemento do diretório, que é o nome curto pelo
// qual a prosa qualifica (`internal/contadores` → `contadores`).
func shortPkg(dir string) string {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[i+1:]
	}
	return dir
}

// rewriteToken reescreve o CONTEÚDO de uma crase: `X`, `pkg.X`, `T.M`,
// `pkg.T.M`, `(*T).M`. Devolve o texto novo e o que ficou de fora, se ficou.
func (m *Mapping) rewriteToken(tok string) (string, *Skipped) {
	t := strings.TrimPrefix(tok, "(*")
	t = strings.Replace(t, ").", ".", 1)
	parts := strings.Split(t, ".")
	switch len(parts) {
	case 1:
		if newName, reason, ok := m.resolve("", parts[0]); ok {
			return newName, nil
		} else if reason != "" {
			return tok, &Skipped{Token: tok, Reason: reason}
		}
	case 2:
		// pkg.X ou T.M
		if newName, reason, ok := m.resolve(parts[0], parts[1]); ok {
			return m.pkgOrType(parts[0]) + "." + newName, nil
		} else if reason != "" {
			return tok, &Skipped{Token: tok, Reason: reason}
		}
		if newName, reason, ok := m.resolve("", parts[0]+"."+parts[1]); ok {
			return newName, nil
		} else if reason != "" {
			return tok, &Skipped{Token: tok, Reason: reason}
		}
		// só o tipo muda (`Fila.Len` com Len já em inglês)
		if newName, _, ok := m.resolve("", parts[0]); ok {
			return newName + "." + parts[1], nil
		}
	case 3:
		// pkg.T.M
		if newName, reason, ok := m.resolve(parts[0], parts[1]+"."+parts[2]); ok {
			return m.pkgOrType(parts[0]) + "." + newName, nil
		} else if reason != "" {
			return tok, &Skipped{Token: tok, Reason: reason}
		}
		if newName, _, ok := m.resolve(parts[0], parts[1]); ok {
			return m.pkgOrType(parts[0]) + "." + newName + "." + parts[2], nil
		}
	}
	return tok, nil
}

// pkgOrType traduz o qualificador de um token: nome curto de pacote quando
// o pacote foi renomeado, nome de tipo quando o tipo foi renomeado, e o
// próprio texto quando nenhum dos dois.
func (m *Mapping) pkgOrType(q string) string {
	if newName, ok := m.Packages[q]; ok {
		return newName
	}
	if newName, _, ok := m.resolve("", q); ok {
		return newName
	}
	return q
}

var (
	reBacktick  = regexp.MustCompile("`([^`\n]+)`")
	reQuoted    = regexp.MustCompile(`"([a-z][a-z0-9_.]*\.[a-z][a-z0-9_]*)"`)
	reTestGo    = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)
	reTestPy    = regexp.MustCompile(`\btest_[a-z0-9_]+`)
	reDocLink   = regexp.MustCompile(`\[([A-Za-z_][A-Za-z0-9_.*()]*)\]`)
	reQualified = regexp.MustCompile(`\b([a-z][a-z0-9]*)\.([A-Z][A-Za-z0-9_]*(?:\.[A-Z][A-Za-z0-9_]*)?)\b`)
)

// applyCommon são as regras válidas em qualquer texto: crases, nomes de
// teste, chaves de log entre crases ou aspas, caminhos.
func (m *Mapping) applyCommon(s string, skips *[]Skipped, line func(int) int) string {
	s = reBacktick.ReplaceAllStringFunc(s, func(x string) string {
		inner := x[1 : len(x)-1]
		if newName, ok := m.LogKeys[inner]; ok {
			return "`" + newName + "`"
		}
		if newName, ok := m.Tests[inner]; ok {
			return "`" + newName + "`"
		}
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			skipped.Line = line(strings.Index(s, x))
			*skips = append(*skips, *skipped)
		}
		return "`" + out + "`"
	})
	s = reQuoted.ReplaceAllStringFunc(s, func(x string) string {
		inner := x[1 : len(x)-1]
		if newName, ok := m.LogKeys[inner]; ok {
			return `"` + newName + `"`
		}
		return x
	})
	s = reTestGo.ReplaceAllStringFunc(s, func(x string) string {
		if newName, ok := m.Tests[x]; ok {
			return newName
		}
		return x
	})
	s = reTestPy.ReplaceAllStringFunc(s, func(x string) string {
		if newName, ok := m.Tests[x]; ok {
			return newName
		}
		return x
	})
	return m.applyPaths(s)
}

// applyPaths troca caminhos, o mais longo primeiro, para
// internal/kafka/fila.go não virar internal/kafka/fila.go pela metade.
func (m *Mapping) applyPaths(s string) string {
	keys := make([]string, 0, len(m.Paths))
	for k := range m.Paths {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, k := range keys {
		s = replacePath(s, k, m.Paths[k])
	}
	return s
}

// replacePath substitui o caminho só quando ele termina em fronteira
// (fim, `/`, `)`, crase, aspas, espaço, `:` de linha), para
// internal/contadores não casar dentro de internal/contadores_x.
func replacePath(s, old, newPath string) string {
	var b strings.Builder
	for {
		i := strings.Index(s, old)
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		end := i + len(old)
		ok := end == len(s) || strings.ContainsRune("/)`\" :,;\n\t]>", rune(s[end]))
		b.WriteString(s[:i])
		if ok {
			b.WriteString(newPath)
		} else {
			b.WriteString(old)
		}
		s = s[end:]
	}
}

// applyGoCommentExtras são as regras que só valem em comentário Go: doc
// link `[Ident]` e o qualificado `pkg.Ident` fora de crase.
func (m *Mapping) applyGoCommentExtras(s string, skips *[]Skipped) string {
	s = reDocLink.ReplaceAllStringFunc(s, func(x string) string {
		inner := x[1 : len(x)-1]
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			*skips = append(*skips, *skipped)
		}
		return "[" + out + "]"
	})
	return reQualified.ReplaceAllStringFunc(s, func(x string) string {
		out, skipped := m.rewriteToken(x)
		if skipped != nil {
			*skips = append(*skips, *skipped)
		}
		return out
	})
}
