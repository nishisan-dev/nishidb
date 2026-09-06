package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// IdentEntry é um identificador do mapeamento: pacote (diretório), espécie
// (`type`, `func`, `method`, `field`, `const`, `var`), endereço velho
// (`Fila`, `Fila.Empurra`) e endereço novo COMPLETO (`Queue.Push`) — nunca só
// a folha, que é o que o TSV traz na coluna `novo`.
type IdentEntry struct {
	Pkg, Kind, Old, New string
}

// Mapping é tudo que a passada de prosa sabe substituir.
type Mapping struct {
	Idents    []IdentEntry
	Tests     map[string]string
	LogKeys   map[string]string
	Paths     map[string]string
	Packages  map[string]string            // nome curto velho → novo (contadores → counters)
	Subtests  map[string]map[string]string // arquivo → velho → novo
	Protected map[string]bool              // token que a regra de identificador nunca toca

	// types é pacote → tipo velho → tipo novo, e existe para o endereço novo
	// de método e campo poder ser sintetizado com o receptor certo. Por isso
	// o carregamento é em DUAS passadas: os tipos primeiro.
	types map[string]map[string]string
}

// Skipped é uma ocorrência que casou com um nome do mapeamento mas ficou
// de fora por regra (ambiguidade, token protegido). Vai para o relatório.
type Skipped struct {
	Line   int
	Token  string
	Reason string
}

// Applied é uma substituição EFETIVAMENTE aplicada. O relatório lista todas,
// não só as pulas: a passada mexe em prosa versionada, e a revisão do lote
// depende de ler o que mudou sem depender do diff — foi assim que uma tabela
// De → Para colapsando em `X → X` passou por revisão com relatório vazio.
type Applied struct {
	Line     int
	Old, New string
	Rule     string
}

// Report é a saída de auditoria de uma passada sobre um arquivo.
type Report struct {
	Applied []Applied
	Skipped []Skipped
}

// aplica registra uma substituição no relatório; velho == novo não é
// substituição e não entra.
func (r *Report) aplica(line int, old, newText, rule string) {
	if old == newText {
		return
	}
	r.Applied = append(r.Applied, Applied{Line: line, Old: old, New: newText, Rule: rule})
}

// pula registra no relatório uma ocorrência que casou e ficou de fora.
func (r *Report) pula(line int, token, reason string) {
	r.Skipped = append(r.Skipped, Skipped{Line: line, Token: token, Reason: reason})
}

// NewMapping devolve um mapeamento vazio com todos os mapas prontos: quem
// carrega os TSV escreve direto neles, sem checar nil a cada linha.
func NewMapping() *Mapping {
	return &Mapping{
		Tests:     map[string]string{},
		LogKeys:   map[string]string{},
		Paths:     map[string]string{},
		Packages:  map[string]string{},
		Subtests:  map[string]map[string]string{},
		Protected: map[string]bool{},
		types:     map[string]map[string]string{},
	}
}

// addIdentRow acrescenta UMA linha do TSV de identificadores, e é o único
// caminho de construção — o carregador do comando e o fixture dos testes
// chamam este mesmo código, porque um fixture que monta as entradas na mão
// esconde exatamente o defeito que esta função existe para não ter.
//
// `address` é o endereço do TSV (`Fila`, `Fila.Empurra`) e `newLeaf` é a
// coluna `novo`, que é sempre só a FOLHA. Para método e campo o endereço novo
// é SINTETIZADO com o receptor — o tipo novo se o lote renomeia o tipo, o
// velho se não —, porque `Store.Apenda` citado em prosa vira `Store.Append`,
// e não `Append` solto.
//
// Só funciona com os tipos já carregados: daí as duas passadas do
// carregamento.
func (m *Mapping) addIdentRow(pkg, kind, address, newLeaf string) {
	if kind == "type" {
		if m.types[pkg] == nil {
			m.types[pkg] = map[string]string{}
		}
		m.types[pkg][address] = newLeaf
	}
	newAddress := newLeaf
	if i := strings.LastIndex(address, "."); i >= 0 {
		owner := address[:i]
		if renamed, ok := m.types[pkg][owner]; ok {
			owner = renamed
		}
		newAddress = owner + "." + newLeaf
	}
	m.Idents = append(m.Idents, IdentEntry{Pkg: pkg, Kind: kind, Old: address, New: newAddress})
}

// resolve decide o endereço novo de um token entre crases. `qual` é o
// qualificador (pacote curto ou tipo), vazio quando o token vem nu.
//
// Nu: substitui só se todo pacote que declara `old` concorda no `new`.
// Qualificado por pacote (velho ou novo nome curto): só as entradas daquele
// pacote. Qualificado por tipo (`Fila.Empurra`): o próprio Old da entrada.
func (m *Mapping) resolve(qual, old string) (string, string, bool) {
	return m.resolveKind(qual, old, "")
}

// resolveType é resolve restrito a entradas de TIPO. É o que separa
// `T.membro` — citação de campo ou método, que a prosa escreve mesmo — de
// `grupos.dict`, que é nome de arquivo em disco e só casava porque existe
// uma variável chamada `grupos`.
func (m *Mapping) resolveType(qual, old string) (string, string, bool) {
	return m.resolveKind(qual, old, "type")
}

// resolveKind é resolve com filtro opcional de espécie; kind vazio aceita
// qualquer uma.
func (m *Mapping) resolveKind(qual, old, kind string) (string, string, bool) {
	var candidates []IdentEntry
	for _, e := range m.Idents {
		if e.Old != old || (kind != "" && e.Kind != kind) {
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
	if m.Protected[tok] {
		return tok, &Skipped{Token: tok, Reason: "protegido"}
	}
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
		// Só o tipo muda (`Fila.Len`, com Len já em inglês). Este atalho
		// dispara SÓ para tipo: com qualquer espécie ele transformava
		// `grupos.dict` (nome de arquivo em disco) em `groups.dict` porque
		// existe uma variável chamada `grupos`.
		if m.Protected[parts[0]] {
			return tok, &Skipped{Token: tok, Reason: "protegido"}
		}
		if newName, _, ok := m.resolveType("", parts[0]); ok {
			return newName + "." + parts[1], nil
		}
	case 3:
		// pkg.T.M
		if newName, reason, ok := m.resolve(parts[0], parts[1]+"."+parts[2]); ok {
			return m.pkgOrType(parts[0]) + "." + newName, nil
		} else if reason != "" {
			return tok, &Skipped{Token: tok, Reason: reason}
		}
		if newName, _, ok := m.resolveType(parts[0], parts[1]); ok {
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
	if newName, _, ok := m.resolveType("", q); ok {
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

// replaceAll é ReplaceAllStringFunc com o OFFSET do casamento. A versão da
// biblioteca entrega só o texto casado, e quem quisesse a posição tinha de
// procurá-la de novo com strings.Index — que devolve a PRIMEIRA ocorrência,
// não a corrente, e reportava o mesmo número de linha para todas as
// repetições de um token. O relatório é guarda: linha errada nele é defeito.
func replaceAll(re *regexp.Regexp, s string, fn func(match string, off int) string) string {
	locs := re.FindAllStringIndex(s, -1)
	if locs == nil {
		return s
	}
	var b strings.Builder
	last := 0
	for _, l := range locs {
		b.WriteString(s[last:l[0]])
		b.WriteString(fn(s[l[0]:l[1]], l[0]))
		last = l[1]
	}
	b.WriteString(s[last:])
	return b.String()
}

// lineAt devolve a linha de off dentro de s, contando a partir de base
// (1 num arquivo de texto; a linha do comentário, numa passada Go). Nenhuma
// regra insere ou remove `\n`, então a contagem vale igual em qualquer
// passada intermediária.
func lineAt(base int, s string, off int) int {
	if off < 0 {
		off = 0
	}
	if off > len(s) {
		off = len(s)
	}
	return base + strings.Count(s[:off], "\n")
}

// applyCommon são as regras válidas em qualquer texto: crases, nomes de
// teste, chaves de log entre crases ou aspas, caminhos.
func (m *Mapping) applyCommon(s string, rep *Report, base int) string {
	s = replaceAll(reBacktick, s, func(x string, off int) string {
		inner := x[1 : len(x)-1]
		if newName, ok := m.LogKeys[inner]; ok {
			rep.aplica(lineAt(base, s, off), inner, newName, "crase")
			return "`" + newName + "`"
		}
		if newName, ok := m.Tests[inner]; ok {
			rep.aplica(lineAt(base, s, off), inner, newName, "crase")
			return "`" + newName + "`"
		}
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			rep.pula(lineAt(base, s, off), skipped.Token, skipped.Reason)
		}
		rep.aplica(lineAt(base, s, off), inner, out, "crase")
		return "`" + out + "`"
	})
	s = replaceAll(reQuoted, s, func(x string, off int) string {
		inner := x[1 : len(x)-1]
		if newName, ok := m.LogKeys[inner]; ok {
			rep.aplica(lineAt(base, s, off), inner, newName, "aspas")
			return `"` + newName + `"`
		}
		return x
	})
	s = replaceAll(reTestGo, s, func(x string, off int) string {
		if newName, ok := m.Tests[x]; ok {
			rep.aplica(lineAt(base, s, off), x, newName, "teste")
			return newName
		}
		return x
	})
	s = replaceAll(reTestPy, s, func(x string, off int) string {
		if newName, ok := m.Tests[x]; ok {
			rep.aplica(lineAt(base, s, off), x, newName, "teste")
			return newName
		}
		return x
	})
	return m.applyPaths(s, rep, base, "caminho")
}

// applyPaths troca caminhos, o mais longo primeiro, para
// internal/kafka/fila.go não virar internal/kafka/fila.go pela metade.
func (m *Mapping) applyPaths(s string, rep *Report, base int, rule string) string {
	keys := make([]string, 0, len(m.Paths))
	for k := range m.Paths {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, k := range keys {
		texto, novo := s, m.Paths[k]
		s = replacePath(s, k, novo, func(off int) {
			rep.aplica(lineAt(base, texto, off), k, novo, rule)
		})
	}
	return s
}

// wordByte diz se o byte continua uma palavra de caminho: letra, dígito, `_`
// ou `-`. É o complemento da fronteira, e é o que separa
// internal/predicado-x (que NÃO é o pacote) de internal/predicado.
func wordByte(c byte) bool {
	return c == '_' || c == '-' ||
		(c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// pathBoundary diz se a posição i fecha um caminho: fim da string, ou um
// caractere que não continua a palavra. O ponto é o caso especial — ele fecha
// a frase ("agora em internal/predicado.") mas NÃO fecha o caminho quando vem
// seguido de palavra (internal/predicado.go, que é outro alvo).
//
// A primeira redação usava uma LISTA de caracteres de fronteira, e a lista
// esquecia `.`, `*`, `-`, `'`, `(` e `\r`: caminho no fim de frase e caminho
// em negrito ficavam para trás, em silêncio, e o repositório tem os dois.
func pathBoundary(s string, i int) bool {
	if i >= len(s) {
		return true
	}
	if s[i] == '.' {
		return i+1 >= len(s) || !wordByte(s[i+1])
	}
	return !wordByte(s[i])
}

// replacePath substitui o caminho só quando ele termina em fronteira, para
// internal/contadores não casar dentro de internal/contadores_x. `on` recebe
// o offset de cada troca aplicada, medido no texto de ENTRADA.
func replacePath(s, old, newPath string, on func(off int)) string {
	var b strings.Builder
	consumed, rest := 0, s
	for {
		i := strings.Index(rest, old)
		if i < 0 {
			b.WriteString(rest)
			return b.String()
		}
		end := i + len(old)
		b.WriteString(rest[:i])
		if pathBoundary(rest, end) {
			b.WriteString(newPath)
			if on != nil {
				on(consumed + i)
			}
		} else {
			b.WriteString(old)
		}
		consumed += end
		rest = rest[end:]
	}
}

// applyGoCommentExtras são as regras que só valem em comentário Go: doc
// link `[Ident]` e o qualificado `pkg.Ident` fora de crase.
func (m *Mapping) applyGoCommentExtras(s string, rep *Report, base int) string {
	s = replaceAll(reDocLink, s, func(x string, off int) string {
		inner := x[1 : len(x)-1]
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			rep.pula(lineAt(base, s, off), skipped.Token, skipped.Reason)
		}
		rep.aplica(lineAt(base, s, off), inner, out, "doclink")
		return "[" + out + "]"
	})
	return replaceAll(reQualified, s, func(x string, off int) string {
		out, skipped := m.rewriteToken(x)
		if skipped != nil {
			rep.pula(lineAt(base, s, off), skipped.Token, skipped.Reason)
		}
		rep.aplica(lineAt(base, s, off), x, out, "qualificado")
		return out
	})
}
