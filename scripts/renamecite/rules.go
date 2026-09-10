package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// IdentEntry é um identificador do mapeamento: pacote (diretório), espécie
// (`type`, `func`, `method`, `field`, `const`, `var`, mais `ifacemethod` para
// o que vem do TSV de grupos de interface), endereço velho (`Fila`,
// `Fila.Empurra`) e endereço novo COMPLETO (`Queue.Push`) — nunca só a folha,
// que é o que o TSV traz na coluna `novo`.
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

	// PackageDirs são os nomes curtos de TODO pacote do módulo, lidos da
	// árvore. Existem para a regra do receptor qualificado: `storage.Modo` é
	// `pkg.Ident` de um pacote que este lote nem toca, e sem saber que
	// `storage` é pacote a regra o lia como receptor e trocava a folha por um
	// `Modo` homônimo de OUTRO pacote. O mapeamento sozinho não sabe disso —
	// ele só conhece os pacotes que estão sendo renomeados.
	PackageDirs map[string]bool

	// Declared é o ORÁCULO DA DECLARAÇÃO VIVA: nome → `<pacote>.<nome>` de
	// onde ele ainda é declarado no módulo. Ver oracleBlocks.
	Declared map[string]string

	// types é pacote → tipo velho → tipo novo, e existe para o endereço novo
	// de método e campo poder ser sintetizado com o receptor certo. Por isso
	// o carregamento é em DUAS passadas: os tipos primeiro.
	types map[string]map[string]string

	// idx é o índice por endereço e por FOLHA, reconstruído no começo de cada
	// arquivo. Ver buildIndex: sem ele a regra do token nu, que consulta o
	// mapeamento uma vez por PALAVRA de comentário, viraria uma varredura
	// linear sobre milhares de entradas por palavra.
	idx *mappingIndex
}

// mappingIndex é a Mapping vista pelas consultas das regras: entradas por
// endereço velho, entradas por folha do endereço, e o conjunto dos nomes
// curtos de pacote conhecidos (que é o que distingue `kafka.Fila` de
// `snap.Epoca`).
type mappingIndex struct {
	byOld    map[string][]IdentEntry
	byLeaf   map[string][]IdentEntry
	pkgNames map[string]bool
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
		Tests:       map[string]string{},
		LogKeys:     map[string]string{},
		Paths:       map[string]string{},
		Packages:    map[string]string{},
		Subtests:    map[string]map[string]string{},
		Protected:   map[string]bool{},
		PackageDirs: map[string]bool{},
		Declared:    map[string]string{},
		types:       map[string]map[string]string{},
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

// buildIndex (re)monta o índice do mapeamento. É chamado no começo de cada
// arquivo varrido, e não uma vez só, porque quem carrega o mapeamento escreve
// direto nos mapas públicos (o carregador do comando e os fixtures dos testes):
// um índice montado na primeira consulta e nunca mais tocado ficaria velho sem
// avisar, que é a classe de defeito que este pacote existe para não ter.
func (m *Mapping) buildIndex() {
	idx := &mappingIndex{
		byOld:    map[string][]IdentEntry{},
		byLeaf:   map[string][]IdentEntry{},
		pkgNames: map[string]bool{},
	}
	for _, e := range m.Idents {
		idx.byOld[e.Old] = append(idx.byOld[e.Old], e)
		idx.byLeaf[leafOf(e.Old)] = append(idx.byLeaf[leafOf(e.Old)], e)
		idx.pkgNames[shortPkg(e.Pkg)] = true
	}
	for old, novo := range m.Packages {
		idx.pkgNames[old] = true
		idx.pkgNames[novo] = true
	}
	for old, new := range m.Paths {
		if strings.Contains(old, ".go") {
			continue
		}
		idx.pkgNames[shortPkg(old)] = true
		idx.pkgNames[shortPkg(new)] = true
	}
	for name := range m.PackageDirs {
		idx.pkgNames[name] = true
	}
	m.idx = idx
}

// index devolve o índice, montando-o se quem chamou entrou por uma função de
// regra em vez de por RewriteText/RewriteGoFile.
func (m *Mapping) index() *mappingIndex {
	if m.idx == nil {
		m.buildIndex()
	}
	return m.idx
}

// leafOf devolve a folha de um endereço (`Snapshot.Epoca` → `Epoca`). Para
// declaração de topo a folha é o próprio nome.
func leafOf(address string) string {
	if i := strings.LastIndex(address, "."); i >= 0 {
		return address[i+1:]
	}
	return address
}

// hasInnerUpper diz se o token tem maiúscula DEPOIS do primeiro caractere.
// É o teste que separa `LimiteRowID`, `specsDoMarcador` e `FormatoJSON` — que
// não são palavra de prosa em nenhuma das duas línguas do repositório, e por
// isso podem ser substituídos em qualquer posição — de `Epoca`, `Nome` e
// `Teto`, que em PT-BR são palavra começando frase.
func hasInnerUpper(tok string) bool {
	for i := 1; i < len(tok); i++ {
		if tok[i] >= 'A' && tok[i] <= 'Z' {
			return true
		}
	}
	return false
}

func startsUpper(tok string) bool { return tok != "" && tok[0] >= 'A' && tok[0] <= 'Z' }
func startsLower(tok string) bool { return tok != "" && tok[0] >= 'a' && tok[0] <= 'z' }

// resolveLeaf é o ÍNDICE POR FOLHA: `Epoca` resolve a partir de
// `Snapshot.Epoca`, e `Fila` a partir dela mesma. É o que faz a prosa que cita
// campo e método pelo nome nu — que é como a prosa os cita — ser alcançada.
//
// Só resolve quando a folha é UNÍVOCA no mapeamento carregado: toda entrada
// com aquela folha, de topo ou de membro, tem de concordar no nome novo. Duas
// discordando é `pulado ... ambíguo` no relatório, nunca um chute.
func (m *Mapping) resolveLeaf(leaf string) (string, string, bool) {
	return m.resolveLeafIn(leaf, false)
}

// resolveLeafIn é resolveLeaf com o filtro de MEMBRO: com membersOnly, só as
// entradas de endereço `T.M`/`T.F` contam. É o que a forma qualificada por
// receptor usa — `snap.Epoca` fala de um campo, e uma função de topo homônima
// não tem nada a dizer sobre ela.
//
// # O ORÁCULO DA DECLARAÇÃO VIVA VEM PRIMEIRO
//
// Antes de qualquer resolução por folha, o nome é confrontado com o que o
// MÓDULO ainda declara (Mapping.Declared, lido da árvore por
// collectDeclarations). Folha que ainda é declaração de alguém não é citação
// pendurada: `Nome` segue sendo campo de `counter.Definicao`, `BytesRetidos`
// de `Store` e de `materializer.Decodificado`, `Fecha` de
// `kafka.Fila`. Reescrevê-las pelo nome novo de um homônimo de outro
// pacote é criar a citação pendurada que esta passada existe para não deixar
// para trás, e foi o que uma rodada de verdade fez seis vezes.
//
// A recusa sai no relatório como `pulado ... ainda declarado <pacote>.<nome>`,
// nunca em silêncio: o oráculo é uma decisão, e decisão que não aparece no
// relatório não pode ser revista.
//
// A forma QUALIFICADA não passa por aqui: `Manifest.Contadores` diz de quem
// está falando, e o tipo do endereço é o desempate que a folha nua não tem.
func (m *Mapping) resolveLeafIn(leaf string, membersOnly bool) (string, string, bool) {
	var candidates []IdentEntry
	for _, e := range m.index().byLeaf[leaf] {
		if membersOnly && !strings.Contains(e.Old, ".") {
			continue
		}
		candidates = append(candidates, e)
	}
	if len(candidates) == 0 {
		return "", "", false
	}
	// O oráculo vem DEPOIS de haver candidato, e não antes: a pergunta é "esta
	// folha, que o lote renomeou, ainda é declaração de alguém?". Perguntando
	// antes, toda palavra entre crases que por acaso é declaração do módulo
	// (`build`, `jdbc`, `Store`, nome de teste) virava linha de relatório —
	// 6570 delas numa rodada, afogando as decisões que importam.
	if onde, vivo := m.Declared[leaf]; vivo {
		return "", "ainda declarado " + onde, false
	}
	newLeaf := leafOf(candidates[0].New)
	for _, c := range candidates[1:] {
		if leafOf(c.New) != newLeaf {
			return "", fmt.Sprintf("ambíguo: %s vira %q em %s e %q em %s",
				leaf, newLeaf, candidates[0].Pkg, leafOf(c.New), c.Pkg), false
		}
	}
	return newLeaf, "", true
}

// leafInPackage diz se alguma entrada com esta folha é do pacote dado. É a
// condição da regra do token com só a primeira maiúscula em comentário Go:
// `Epoca` no comentário de internal/types é citação do campo; a mesma palavra
// no comentário de outro pacote é palavra.
func (m *Mapping) leafInPackage(leaf, pkg string) bool {
	if pkg == "" {
		return false
	}
	for _, e := range m.index().byLeaf[leaf] {
		if e.Pkg == pkg {
			return true
		}
	}
	return false
}

// isPackageName diz se o token é nome curto de pacote conhecido — velho ou
// novo. É o que separa `kafka.Fila` (qualificado por PACOTE) de `snap.Epoca`
// (qualificado por RECEPTOR).
func (m *Mapping) isPackageName(tok string) bool { return m.index().pkgNames[tok] }

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
	for _, e := range m.index().byOld[old] {
		if kind != "" && e.Kind != kind {
			continue
		}
		// O qualificador casa com o nome curto do pacote da entrada, com o
		// nome NOVO desse pacote e com o nome VELHO dele. A última forma é a
		// que a Fase 2 usa: a coluna `pacote` do TSV já traz o caminho NOVO
		// (internal/query/budget), e a prosa ainda cita o velho
		// (`orcamento.NovoGlobal`) — sem ela o `-paths` não fecharia o
		// qualificador e a citação sairia pela metade.
		if qual == "" || shortPkg(e.Pkg) == qual ||
			m.Packages[shortPkg(e.Pkg)] == qual || m.Packages[qual] == shortPkg(e.Pkg) {
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
// `pkg.T.M`, `(*T).M`, com ou sem os parênteses da CHAMADA colados ao fim.
// Devolve o texto novo e o que ficou de fora, se ficou.
func (m *Mapping) rewriteToken(tok string) (string, *Skipped) {
	if m.Protected[tok] {
		return tok, &Skipped{Token: tok, Reason: "protegido"}
	}
	// `T.M()`, `pkg.T.M()`, `x.M()`: os parênteses colados ao fim são a marca da
	// CHAMADA, não parte do nome. Sem separá-los, `Leitor.Linha()` entra no
	// switch com a folha `Linha()`, que não é endereço de nada — nenhuma regra
	// de membro casa e sobra o atalho do TIPO, que devolve `Reader.Linha()`.
	// Meia citação é pior que a inteira velha: ela afirma que `Reader` tem um
	// método `Linha`. Foi assim que as duas docstrings do client Python saíram
	// da passada do G2.
	//
	// Vale para o que vier entre os parênteses: `Reader.Linha(x)` é exemplo de
	// chamada, e o método ali é citação igual.
	if i := callSuffix(tok); i > 0 {
		out, skipped := m.rewriteToken(tok[:i])
		return out + tok[i:], skipped
	}
	t := strings.TrimPrefix(tok, "(*")
	t = strings.Replace(t, ").", ".", 1)
	parts := strings.Split(t, ".")
	switch len(parts) {
	case 1:
		// O índice por FOLHA, e não só o endereço de topo: a prosa cita campo
		// e método pelo nome nu (`Epoca`, `LiberaBlocos`), e antes disso só
		// `Snapshot.Epoca` era alcançado.
		if newName, reason, ok := m.resolveLeaf(parts[0]); ok {
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
		// Qualificado por RECEPTOR: `snap.Epoca`, `se.Clausula`, `orc.Teto`.
		// O receptor é minúsculo e não é nome de pacote conhecido, e a folha
		// resolve como MEMBRO — só ela é trocada, porque o receptor é nome de
		// variável de quem escreveu a frase, não identificador do mapeamento.
		//
		// A folha tem de ser EXPORTADA. Sem essa condição a rodada de verdade
		// trocou `pai.chave`, `c.entrada`, `b.campo` e `d.erro` — receptor com
		// folha minúscula é, no repositório, campo de struct de tabela de teste
		// e chave de exemplo, e a folha minúscula casa com qualquer campo
		// homônimo de qualquer pacote do lote.
		if startsLower(parts[0]) && startsUpper(parts[1]) && !m.isPackageName(parts[0]) {
			if newName, reason, ok := m.resolveLeafIn(parts[1], true); ok {
				return parts[0] + "." + newName, nil
			} else if reason != "" {
				return tok, &Skipped{Token: tok, Reason: reason}
			}
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

// callSuffix devolve o índice do parêntese que ABRE a chamada colada ao fim de
// um token QUALIFICADO (`Reader.Linha()` → 12), ou -1 quando não há.
//
// Duas condições, e as duas são a diferença entre uma regra e um estrago:
//
//   - o parêntese do receptor ponteiro não conta. Em `(*T).M()` o primeiro `(`
//     abre o QUALIFICADOR, e cortar ali deixaria o token vazio;
//   - o que vem ANTES do parêntese tem de ser um endereço qualificado, com
//     ponto. Sem o ponto, `dict(linha)` viraria `dict(...)` resolvido por
//     folha nua minúscula — exatamente o que rewriteCompound recusa fazer, e
//     pela mesma razão: dentro de uma crase composta a palavra minúscula é do
//     EXEMPLO, não do mapeamento.
func callSuffix(tok string) int {
	start := 0
	if strings.HasPrefix(tok, "(*") {
		i := strings.Index(tok, ").")
		if i < 0 {
			return -1
		}
		start = i + 2
	}
	i := strings.IndexByte(tok[start:], '(')
	if i < 0 {
		return -1
	}
	i += start
	head := strings.Replace(strings.TrimPrefix(tok[:i], "(*"), ").", ".", 1)
	if !strings.Contains(head, ".") || !isSimpleToken(head) {
		return -1
	}
	return i
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
	reBacktick = regexp.MustCompile("`([^`\n]+)`")
	reQuoted   = regexp.MustCompile(`"([a-z][a-z0-9_.]*\.[a-z][a-z0-9_]*)"`)
	reTestGo   = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)
	reTestPy   = regexp.MustCompile(`\btest_[a-z0-9_]+`)
	reDocLink  = regexp.MustCompile(`\[([A-Za-z_][A-Za-z0-9_.*()]*)\]`)
	// reDottedToken casa um identificador, com os qualificadores que vierem
	// colados: é o tokenizador de dentro da crase composta e do literal.
	reDottedToken = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*`)
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
		if out == inner && !isSimpleToken(inner) {
			out = m.rewriteCompound(inner, rep, lineAt(base, s, off))
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

// applyGoCommentExtras é a regra que só vale em comentário Go: o doc link
// `[Ident]`. O qualificado `pkg.Ident` vale nos três formatos de prosa e mora
// em applyProse.
func (m *Mapping) applyGoCommentExtras(s string, rep *Report, base int) string {
	return replaceAll(reDocLink, s, func(x string, off int) string {
		inner := x[1 : len(x)-1]
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			rep.pula(lineAt(base, s, off), skipped.Token, skipped.Reason)
		}
		rep.aplica(lineAt(base, s, off), inner, out, "doclink")
		return "[" + out + "]"
	})
}

// isSimpleToken diz se o texto inteiro é UM identificador, com ou sem
// qualificadores. É o que separa a crase simples da COMPOSTA: a simples já foi
// tentada como endereço, e tentá-la de novo token a token repetiria a mesma
// pula no relatório — que é guarda, e guarda que conta duas vezes mente.
func isSimpleToken(s string) bool {
	loc := reDottedToken.FindStringIndex(s)
	return loc != nil && loc[0] == 0 && loc[1] == len(s)
}

// rewriteCompound reescreve os tokens DENTRO de uma crase composta —
// `presenca != PresencaAusente`, `Snapshot{Epoca, Instante}`,
// `[snap.BaseRowID, snap.LimiteRowID)` — quando o conteúdo inteiro não é um
// token só. Antes disto a regra da crase tentava o conteúdo INTEIRO como
// endereço, não achava nada e devolvia a crase intacta: o repositório tem
// dezenas dessas, e todas passaram pelas rodadas anteriores sem uma linha de
// relatório.
//
// Cada token interno passa pelas mesmas regras do token entre crases simples,
// inclusive a folha e o receptor qualificado — MENOS a do token nu todo
// minúsculo.
//
// Essa exceção é a diferença entre a crase simples e a composta, e ela custou
// uma rodada inteira para ficar clara. A crase simples é uma citação: quem
// escreve `usado` está apontando para o identificador, e por isso o minúsculo
// vale ali. A crase COMPOSTA quase nunca é: ela cerca uma linha de comando, um
// trecho de código, uma sintaxe de CLI ou um exemplo de JSON, e cada palavra
// minúscula lá dentro é do EXEMPLO, não do mapeamento. A primeira rodada com
// esta regra transformou `--param <tipo>:<valor>` em `--param <typ>:<value>`,
// `"campo": null` em `"field": null`, `echo senha > arquivo` em
// `echo senha > file`, `dict(linha)` em `dict(line)` e
// `Arrays.fill(tipo, null)` em `Arrays.fill(typ, null)` — documentação de
// sintaxe e código de cliente, todos destruídos em silêncio a não ser pelo
// relatório.
func (m *Mapping) rewriteCompound(inner string, rep *Report, line int) string {
	return replaceAll(reDottedToken, inner, func(tok string, _ int) string {
		if !strings.Contains(tok, ".") && !hasInnerUpper(tok) && !startsUpper(tok) {
			return tok
		}
		out, skipped := m.rewriteToken(tok)
		if skipped != nil {
			rep.pula(line, skipped.Token, skipped.Reason)
		}
		return out
	})
}

// proseKind é o formato do texto em que a regra do TOKEN NU vale, e a razão de
// a extensão do arquivo chegar até aqui: a mesma palavra é citação num lugar e
// prosa em outro.
type proseKind int

const (
	proseOther proseKind = iota // .sh, .py, .yml, .java, Makefile: nada de token nu
	proseMarkdown
	prosePUML
	proseGoComment
)

// applyProseTokens é a regra de citação FORA de crase, e é a que alcança o que
// a prosa realmente escreve: `LimiteRowID` no meio da frase, `FormatoJSON` no
// godoc, `specsDoMarcador` numa mensagem de falha, `snap.Epoca` num diagrama.
//
// O tokenizador leva o token com os qualificadores COLADOS, e essa é a parte
// que não pode ser separada em duas passadas: com o nu resolvido primeiro,
// `orcamento.NovoGlobal` virava `orcamento.NewGlobal` — meia citação, pior que
// a inteira velha, porque agora ela afirma que o pacote `orcamento` tem um
// `NewGlobal`.
//
// # QUALIFICADO
//
// `pkg.Ident`, `T.Membro` e o receptor `snap.Epoca` passam pelo mesmo
// rewriteToken da crase. Vale em comentário Go, `.md` e `.puml` — e em mais
// nada, porque em `.py`, `.java`, `.yml` e `.sh` a mesma forma é CÓDIGO do
// cliente, e reescrevê-la seria mudar programa alheio numa passada que só tem
// licença sobre prosa.
//
// # NU: DUAS FORMAS, DUAS LICENÇAS
//
// Token com maiúscula DEPOIS do primeiro caractere é substituído em qualquer
// posição: `escreveAtomico` e `AbreSomenteLeitura` não são palavra de nenhuma
// língua. Token com só a PRIMEIRA maiúscula (`Epoca`, `Nome`, `Teto`) é
// palavra de prosa em PT-BR começando frase, e só é substituído onde o
// contexto o denuncia como identificador: no comentário do PRÓPRIO pacote que
// declarava o nome, ou, num diagrama, dentro das chaves ou parênteses de uma
// caixa (`Snapshot{Epoca, Instante}`). Em `.md` fora de crase, nunca.
//
// Token todo minúsculo continua fora desta regra em qualquer formato — a crase
// é a única licença que ele tem, e é a que a regra `crase` já dá.
func (m *Mapping) applyProseTokens(s string, rep *Report, base int, kind proseKind, pkg string) string {
	if kind == proseOther {
		return s
	}
	var fences [][2]int
	if kind == proseMarkdown {
		fences = goFenceRanges(s)
	}
	return replaceAll(reDottedToken, s, func(tok string, off int) string {
		if strings.Contains(tok, ".") {
			out, skipped := m.rewriteToken(tok)
			if skipped != nil {
				rep.pula(lineAt(base, s, off), skipped.Token, skipped.Reason)
			}
			rep.aplica(lineAt(base, s, off), tok, out, "qualificado")
			return out
		}
		if !hasInnerUpper(tok) && !startsUpper(tok) {
			return tok
		}
		if len(m.index().byLeaf[tok]) == 0 {
			return tok
		}
		if !hasInnerUpper(tok) && !m.bareSingleCapAllowed(tok, s, off, kind, pkg, fences) {
			return tok
		}
		if m.Protected[tok] {
			rep.pula(lineAt(base, s, off), tok, "protegido")
			return tok
		}
		out, reason, ok := m.resolveLeaf(tok)
		if !ok {
			if reason != "" {
				rep.pula(lineAt(base, s, off), tok, reason)
			}
			return tok
		}
		rep.aplica(lineAt(base, s, off), tok, out, "nu")
		return out
	})
}

// bareSingleCapAllowed é a licença do token com só a primeira maiúscula.
func (m *Mapping) bareSingleCapAllowed(tok, s string, off int, kind proseKind, pkg string, fences [][2]int) bool {
	switch kind {
	case proseGoComment:
		return m.leafInPackage(tok, pkg)
	case prosePUML:
		return insideBox(s, off)
	case proseMarkdown:
		return insideRanges(fences, off)
	default:
		return false
	}
}

// goFenceRanges devolve as faixas de bloco cercado ```go de um `.md`. Ali
// dentro o texto é CÓDIGO Go — declaração de struct, assinatura,
// trecho de plano —, e `Epoca` numa linha de campo é identificador pelo mesmo
// motivo por que `Snapshot{Epoca}` é numa caixa de diagrama.
//
// O achado que fecha a regra: a limpeza do G1 deixou dois blocos ```go se
// contradizendo — `type Presenca uint8` ao lado de `PresenceAbsent Presenca`
// em planning/tipos-temporais-e-expire-after-write.md, e `Epoca`/`Instante` ao
// lado de `LimitRowID` dentro de `type Snapshot struct` em
// planning/snapshot-de-leitura-plano.md. Metade de cada bloco tinha sido
// alcançada pela crase e pela maiúscula interna, e a outra metade não.
//
// Só ```go: um bloco ```yaml ou ```sql é dado e sintaxe de outra língua, e a
// mesma palavra ali é chave de configuração.
//
// A varredura é UMA por arquivo, e não uma por token: quem pergunta compara
// offsets contra as faixas. Contar crases desde o começo do arquivo a cada
// palavra transformaria a passada num algoritmo quadrático sobre planos de
// dois mil linhas.
func goFenceRanges(s string) [][2]int {
	var faixas [][2]int
	abertura := -1
	for i := 0; i < len(s); {
		fim := strings.IndexByte(s[i:], '\n')
		if fim < 0 {
			fim = len(s) - i
		}
		linha := strings.TrimSpace(s[i : i+fim])
		if strings.HasPrefix(linha, "```") {
			if abertura >= 0 {
				faixas = append(faixas, [2]int{abertura, i})
				abertura = -1
			} else if strings.TrimSpace(strings.TrimPrefix(linha, "```")) == "go" {
				abertura = i + fim + 1
			}
		}
		i += fim + 1
	}
	if abertura >= 0 {
		faixas = append(faixas, [2]int{abertura, len(s)})
	}
	return faixas
}

// insideRanges diz se off cai em alguma das faixas.
func insideRanges(faixas [][2]int, off int) bool {
	for _, f := range faixas {
		if off >= f[0] && off < f[1] {
			return true
		}
	}
	return false
}

// insideBox diz se a posição está dentro de `{...}` ou `(...)` na mesma linha,
// que é como uma caixa de diagrama lista os campos de um tipo. A conta é por
// LINHA: uma chave aberta e nunca fechada não pode fazer o resto do arquivo
// virar citação.
func insideBox(s string, off int) bool {
	start := 0
	if i := strings.LastIndexByte(s[:off], '\n'); i >= 0 {
		start = i + 1
	}
	depth := 0
	for i := start; i < off; i++ {
		switch s[i] {
		case '{', '(':
			depth++
		case '}', ')':
			if depth > 0 {
				depth--
			}
		}
	}
	return depth > 0
}

// applyProse são as regras que valem em prosa de verdade — comentário Go,
// `.md` e `.puml`.
func (m *Mapping) applyProse(s string, rep *Report, base int, kind proseKind, pkg string) string {
	return m.applyProseTokens(s, rep, base, kind, pkg)
}
