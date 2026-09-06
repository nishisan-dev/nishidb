// Este arquivo confere que identificador RENOMEADO pelo ciclo de
// identificadores em inglês não continua CITADO pelo nome velho.
package gates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// identMappingDir é onde moram os TSV do ciclo, e identMappingHeader é o
// cabeçalho EXATO do TSV de IDENTIFICADORES. O diretório guarda mais de um
// formato (caminhos e pacotes têm duas colunas, o inventário tem sete), então
// o cabeçalho é o que discrimina: ler `mapeamento-*.tsv` sem conferi-lo faria
// o gate interpretar `velho`/`novo` como `pacote`/`especie` e sair com zero
// candidato — que é justamente a vacuidade que ele proíbe.
const (
	identMappingDir    = "planning/identificadores-em-ingles"
	identMappingHeader = "pacote\tespecie\tendereco\tnovo\tmotivo"
)

// renamedIdent é uma linha de mapeamento com rename EFETIVO: o nome velho
// (a folha do endereço) e o pacote/endereço que a mensagem de falha cita para
// quem for consertar.
type renamedIdent struct {
	pkg, address, old, new string
}

// TestRenamedIdentifiersAreNotCitedByTheirOldNames é o gate de CITAÇÃO
// PENDURADA: para todo identificador que este ciclo renomeou, o nome velho não
// pode sobreviver em comentário, godoc, literal de string, documento, diagrama,
// Makefile, workflow ou script.
//
// # POR QUE ELE EXISTE
//
// O rf renomeia o CÓDIGO e o `scripts/renamecite` renomeia as CITAÇÕES, e o
// segundo só alcança o que as regras dele descrevem. A revisão do grupo G1
// encontrou 77 citações penduradas que passaram pelas duas ferramentas e por
// uma revisão humana: godoc exportado descrevendo campo que mudou de nome,
// mensagem de erro de produção nomeando método que não existe mais, diagrama
// desenhando struct com os campos velhos. Nenhuma delas quebra build, teste ou
// lint — é a primeira classe da doutrina de verificação, texto normativo que
// mente, e a única forma de fechá-la é um gate que leia o mapeamento e o
// repositório como DADO e compare as duas extrações.
//
// # O QUE É PROCURADO
//
// Só o que é reconhecível como identificador: nome velho que COMEÇA por
// maiúscula (declaração exportada, folha de método ou de campo) ou que tem
// maiúscula DEPOIS do primeiro caractere (`specsDoMarcador`, `LimiteRowID`).
// Nome inteiramente minúsculo (`campo`, `linha`, `usado`, `teto`) fica de
// fora, e não por economia: em PT-BR eles são palavra de prosa, e procurá-los
// como token acusaria centenas de frases legítimas — um gate que grita em
// tudo é um gate que ninguém lê.
//
// # ONDE O TOKEN CONTA, E POR QUE NÃO É "EM QUALQUER LUGAR"
//
// A distinção é a MESMA que o `scripts/renamecite` usa para decidir o que
// substituir, e por uma razão de contrato: o gate não pode cobrar o que a
// ferramenta do ciclo não sabe consertar.
//
//   - Token com maiúscula INTERNA (`LimiteRowID`, `escreveAtomico`) conta em
//     qualquer lugar: essa forma não é palavra de prosa em nenhuma das duas
//     línguas do repositório.
//   - Token com só a PRIMEIRA maiúscula (`Epoca`, `Nome`, `Teto`, `Devolve`)
//     conta em CONTEXTO de identificador — entre crases, qualificado por ponto,
//     em doc link `[X]`, seguido de `(`, dentro das chaves de uma caixa de
//     diagrama — ou dentro do PRÓPRIO pacote que declarava o nome. Fora disso
//     ele é `Nome` no começo de uma frase, e uma primeira redação deste gate
//     acusou 579 ocorrências, das quais mais de quatrocentas eram prosa PT-BR
//     legítima. Cobrar tudo teria uma única saída prática: despejar `Nome`,
//     `Teto`, `Existe` e `Devolve` na lista de protegidos — que é a MESMA que o
//     renamecite lê, e que assim passaria a impedir a substituição das citações
//     de verdade. O gate teria comprado silêncio quebrando a ferramenta.
//
// # NOME QUE AINDA EXISTE
//
// Nome velho que sobreviveu como OUTRA declaração é cobrado só quando a
// citação diz de quem está falando: qualificada pelo dono do endereço
// (`Manifest.Contadores`, `checkpoint.Contadores`) ou dentro do próprio pacote
// que perdeu o nome. `Store.Contadores()` continua existindo em
// internal/storage, e `Nome` continua sendo campo de `metrics.Amostra` — um
// gate que trocasse esses dois criaria a citação pendurada que diz combater.
//
// Sobreviver é decidido por DUAS extrações, nenhuma delas um inventário
// congelado: a coluna `novo` dos próprios TSV (o alvo de um rename e o nome de
// toda linha "mantido") e as DECLARAÇÕES do módulo lidas do AST na mesma
// varredura que colhe os comentários. A segunda é o que cobre os pacotes que
// este ciclo ainda não mapeou, e é por isso que ela não pode ser o
// `inventario-pos-fase1.tsv`: aquele arquivo é anterior à Fase 2 e listaria
// como vivo exatamente o nome que a fase acabou de matar.
//
// # VACUIDADE É FALHA
//
// Zero ocorrência com os TSV presentes só é passe se houve o que procurar.
// Zero candidato significa TSV movido, renomeado ou com o cabeçalho mudado, e
// nesse estado o gate carimba verde sem olhar nada.
func TestRenamedIdentifiersAreNotCitedByTheirOldNames(t *testing.T) {
	renamed, surviving := loadRenamedIdents(t)
	protected := loadProtectedTokens(t)

	// byOldName é o índice que torna a varredura viável: um passe por token do
	// repositório, não um por (token × candidato).
	byOldName := map[string][]renamedIdent{}
	for _, r := range renamed {
		if protected[r.old] {
			continue
		}
		if !startsUpper(r.old) && !hasInnerUpper(r.old) {
			continue
		}
		byOldName[r.old] = append(byOldName[r.old], r)
	}
	if len(byOldName) == 0 {
		t.Fatalf("nenhum nome velho a procurar em %s: com %d linhas de rename lidas, isso significa TSV movido, "+
			"cabeçalho mudado ou lista de protegidos engolindo tudo — e o gate passaria por vacuidade",
			identMappingDir, len(renamed))
	}

	type hit struct {
		file  string
		line  int
		token string
		entry renamedIdent
	}
	var hits []hit
	chunks, declared := danglingCitationChunks(t)
	for _, c := range chunks {
		eachWordToken(c.text, func(tok string, off int) {
			entries, ok := byOldName[tok]
			if !ok {
				return
			}
			alive := surviving[tok] || declared.module[tok]
			entry, ok := citedEntry(c, off, tok, entries, alive, declared)
			if !ok {
				return
			}
			hits = append(hits, hit{c.file, c.line + strings.Count(c.text[:off], "\n"), tok, entry})
		})
	}
	if len(chunks) == 0 {
		t.Fatalf("nenhum texto varrido: o escopo do gate ficou vazio e ele passaria por vacuidade")
	}
	if len(hits) == 0 {
		return
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].file != hits[j].file {
			return hits[i].file < hits[j].file
		}
		return hits[i].line < hits[j].line
	})
	for _, h := range hits {
		t.Errorf("%s:%d: %s foi renomeado para %s (%s %s) e a citação ficou pendurada — texto que nomeia "+
			"identificador inexistente é pior que texto nenhum: ele afirma o que ninguém pode conferir",
			h.file, h.line, h.token, h.entry.new, h.entry.pkg, h.entry.address)
	}
	t.Errorf("%d citações penduradas em %d nomes velhos; o que não for citação de identificador entra em %s/protegidos.txt",
		len(hits), len(byOldName), identMappingDir)
}

// loadRenamedIdents lê os TSV de identificadores do ciclo e devolve as linhas
// com rename EFETIVO mais o conjunto dos nomes que SOBREVIVEM.
//
// Sobrevive todo nome que aparece na coluna `novo` de qualquer linha — o alvo
// de um rename, e também o nome de uma linha "mantido", cujo `novo` é o nome
// atual. É por esse conjunto que o gate decide se `Commit` ainda é uma
// declaração viva (e portanto só pode ser cobrado na forma qualificada) sem
// precisar de um inventário regenerado a cada rodada.
func loadRenamedIdents(t *testing.T) ([]renamedIdent, map[string]bool) {
	t.Helper()
	dir := filepath.Join(repoRoot, filepath.FromSlash(identMappingDir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("os mapeamentos do ciclo não puderam ser lidos em %s: %v", identMappingDir, err)
	}
	var renamed []renamedIdent
	surviving := map[string]bool{}
	files := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "mapeamento-") || !strings.HasSuffix(e.Name(), ".tsv") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("%s: %v", e.Name(), err)
		}
		lines := strings.Split(string(content), "\n")
		if len(lines) == 0 || strings.TrimRight(lines[0], "\r") != identMappingHeader {
			continue // TSV de outro formato (caminhos, pacotes, inventário)
		}
		files++
		for _, line := range lines[1:] {
			cols := strings.Split(strings.TrimRight(line, "\r"), "\t")
			if len(cols) < 5 {
				continue
			}
			pkg, especie, address, novo := cols[0], strings.SplitN(cols[1], "#", 2)[0], cols[2], cols[3]
			if novo == "" {
				continue
			}
			surviving[novo] = true
			switch especie {
			case "param", "local", "label", "receiver":
				// Prosa não cita local por endereço, e o nome de um parâmetro
				// renomeado não é citação pendurada em lugar nenhum.
				continue
			}
			if old := addressLeaf(address); old != novo {
				renamed = append(renamed, renamedIdent{pkg: pkg, address: address, old: old, new: novo})
			}
		}
	}
	if files == 0 {
		t.Fatalf("nenhum TSV de identificadores em %s com o cabeçalho %q: o gate ficaria sem oráculo",
			identMappingDir, identMappingHeader)
	}
	return renamed, surviving
}

// loadProtectedTokens lê a lista de tokens que o ciclo declarou NÃO serem
// citação de identificador (nome de coluna, chave de YAML, arquivo em disco).
// É a mesma lista que o `scripts/renamecite` consome, e por isso o formato é
// um token por linha, com `#` comentando.
//
// Arquivo ausente é FALHA: a lista é parte do contrato do ciclo, e um gate que
// segue em frente sem ela mudaria de escopo em silêncio.
func loadProtectedTokens(t *testing.T) map[string]bool {
	t.Helper()
	path := filepath.Join(repoRoot, filepath.FromSlash(identMappingDir), "protegidos.txt")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("a lista de protegidos do ciclo não pôde ser lida (%v): sem ela o gate cobraria tokens que o "+
			"ciclo já decidiu que não são citação", err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(content), "\n") {
		if l := strings.TrimSpace(line); l != "" && !strings.HasPrefix(l, "#") {
			out[l] = true
		}
	}
	return out
}

// citationChunk é um pedaço de texto varrido: o arquivo, o pacote (só em
// `.go`), a linha em que o pedaço começa, o texto e a espécie do pedaço. O
// pedaço existe porque em `.go` o gate NÃO varre o arquivo inteiro — só
// comentários e literais de string, lidos do AST.
type citationChunk struct {
	file string
	pkg  string // diretório do pacote, vazio fora de .go
	line int
	kind chunkKind
	text string
}

// chunkKind separa os pedaços cujo delimitador MUDA de sentido. A crase marca
// citação em comentário e em documento; num literal Go ela é o próprio
// delimitador do literal bruto, e tratá-la como marca de citação poria todo
// fixture multilinha do repositório "entre crases".
type chunkKind int

const (
	chunkGoComment chunkKind = iota
	chunkGoLiteral
	chunkMarkup
)

// declarations é o que o módulo AINDA declara, colhido do AST na mesma
// varredura dos comentários: o conjunto do módulo inteiro e o de cada pacote.
type declarations struct {
	module map[string]bool
	byPkg  map[string]map[string]bool
}

// citedEntry decide se a ocorrência é CITAÇÃO PENDURADA e devolve a entrada do
// mapeamento que a explica. Ver o godoc do teste: com o nome morto no módulo,
// vale a forma (maiúscula interna, contexto de identificador, pacote de
// origem); com o nome vivo em outra declaração, só a citação que diz de quem
// fala — qualificada pelo dono do endereço, ou dentro do pacote que perdeu o
// nome.
func citedEntry(c citationChunk, off int, tok string, entries []renamedIdent, alive bool, declared declarations) (renamedIdent, bool) {
	for _, e := range entries {
		if c.pkg != "" && c.pkg == e.pkg && !declared.byPkg[c.pkg][tok] {
			return e, true
		}
		if qual := qualifierBefore(c.text, off); qual != "" && (qual == addressOwner(e.address) || qual == shortPkg(e.pkg)) {
			return e, true
		}
	}
	if alive {
		return renamedIdent{}, false
	}
	if hasInnerUpper(tok) || isIdentifierContext(c, off, tok) {
		return entries[0], true
	}
	return renamedIdent{}, false
}

// qualifierBefore devolve o identificador que qualifica a ocorrência em off
// (`Manifest` em `Manifest.Contadores`), ou vazio se ela vem nua.
func qualifierBefore(s string, off int) string {
	if !isDotQualified(s, off) {
		return ""
	}
	i := off - 1
	for i > 0 && isWordByte(s[i-1]) {
		i--
	}
	return s[i : off-1]
}

// addressOwner devolve o dono de um endereço do TSV (`Manifest.Contadores` →
// `Manifest`), ou vazio quando a declaração é de topo.
func addressOwner(address string) string {
	if i := strings.LastIndex(address, "."); i >= 0 {
		return address[:i]
	}
	return ""
}

// shortPkg devolve o nome curto pelo qual a prosa qualifica um pacote
// (`internal/storage/checkpoint` → `checkpoint`).
func shortPkg(dir string) string {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[i+1:]
	}
	return dir
}

// isIdentifierContext diz se a ocorrência tem, ao redor, alguma marca que
// prosa não põe: crase, ponto de qualificação, doc link, parêntese de chamada
// ou chave de caixa de diagrama.
func isIdentifierContext(c citationChunk, off int, tok string) bool {
	if isDotQualified(c.text, off) {
		return true
	}
	end := off + len(tok)
	if off > 0 && c.text[off-1] == '[' && end < len(c.text) && c.text[end] == ']' {
		return true
	}
	if end < len(c.text) && c.text[end] == '(' {
		return true
	}
	line := lineAround(c.text, off)
	if c.kind != chunkGoLiteral && strings.Count(c.text[line:off], "`")%2 == 1 {
		return true
	}
	// `Snapshot{Epoca, Instante}`: a caixa de um diagrama lista os campos entre
	// chaves, e é assim que os dois .puml do repositório citam o tipo.
	return strings.HasSuffix(c.file, ".puml") &&
		strings.Count(c.text[line:off], "{") > strings.Count(c.text[line:off], "}")
}

// lineAround devolve o offset do começo da linha que contém off. As marcas de
// contexto valem por LINHA: uma crase aberta e nunca fechada não pode fazer o
// resto do arquivo virar citação.
func lineAround(s string, off int) int {
	if i := strings.LastIndexByte(s[:off], '\n'); i >= 0 {
		return i + 1
	}
	return 0
}

// danglingCitationSkipDirs são as subárvores fora do escopo. As três primeiras
// não são código do repositório (`.claude` guarda worktrees, que são cópias da
// árvore inteira); `planning/identificadores-em-ingles` e `scripts/renamecite`
// são o registro DE → PARA do próprio ciclo e os fixtures da ferramenta que o
// aplica, onde o nome velho é o DADO; `benchmark/baselines` são registros de
// rodada, que o projeto não reescreve.
var danglingCitationSkipDirs = map[string]bool{
	".git": true, ".claude": true, ".superpowers": true,
	"node_modules": true, "target": true, ".venv": true, "__pycache__": true,
}

// danglingCitationSkipPaths são os caminhos relativos fora do escopo, pela
// mesma razão do diretório de mapeamentos: neles a coluna da esquerda é o nome
// velho por construção.
// danglingCitationSkipPaths são os arquivos e diretórios fora do escopo, pela
// mesma razão do diretório de mapeamentos: neles o nome velho é o DADO.
// O próprio arquivo deste gate entra na lista pelo motivo que já tirou
// `scripts/renamecite` dela: o godoc explica a regra com os nomes velhos de
// verdade (`Epoca`, `LimiteRowID`, `specsDoMarcador`), e sem a exceção ele
// acusaria as próprias explicações — 22 ocorrências na primeira rodada.
var danglingCitationSkipPaths = map[string]bool{
	"planning/identificadores-em-ingles":          true,
	"planning/identificadores-em-ingles.md":       true,
	"planning/identificadores-em-ingles-plano.md": true,
	"scripts/renamecite":                          true,
	"benchmark/baselines":                         true,
	"docs/glossario.md":                           true,
	"internal/gates/dangling_citations_test.go":   true,
}

// danglingCitationChunks devolve tudo que o gate varre — os comentários e
// literais de string de todo `.go` (lidos com go/parser, nunca por grep cru —
// um nome velho dentro de uma expressão é código, e código é problema do
// compilador), e o texto inteiro de `.md`, `.puml`, `Makefile`, dos workflows
// e dos scripts de `scripts/` — mais as DECLARAÇÕES que o módulo ainda tem.
//
// As duas saídas vêm da mesma varredura de propósito: o oráculo de "este nome
// ainda existe" tem de descrever a árvore que está sendo cobrada, não uma
// extração de outro momento.
func danglingCitationChunks(t *testing.T) ([]citationChunk, declarations) {
	t.Helper()
	var chunks []citationChunk
	declared := declarations{module: map[string]bool{}, byPkg: map[string]map[string]bool{}}
	err := filepath.WalkDir(repoRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(repoRoot, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if danglingCitationSkipDirs[d.Name()] || danglingCitationSkipPaths[rel] {
				return fs.SkipDir
			}
			return nil
		}
		if danglingCitationSkipPaths[rel] {
			return nil
		}
		switch {
		case strings.HasSuffix(rel, ".go"):
			chunks = append(chunks, goCitationChunks(rel, p, declared)...)
		case strings.HasSuffix(rel, ".md"), strings.HasSuffix(rel, ".puml"),
			filepath.Base(rel) == "Makefile",
			strings.HasPrefix(rel, ".github/workflows/") && strings.HasSuffix(rel, ".yml"),
			strings.HasPrefix(rel, "scripts/") && (strings.HasSuffix(rel, ".sh") || strings.HasSuffix(rel, ".py")):
			content, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			chunks = append(chunks, citationChunk{file: rel, line: 1, kind: chunkMarkup, text: string(content)})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("varredura: %v", err)
	}
	return chunks, declared
}

// goCitationChunks extrai de um arquivo Go os comentários e os literais de
// string, e acrescenta a `declared` os nomes que o arquivo DECLARA. Arquivo que
// não parseia devolve nada: sintaxe é problema do compilador, e cair para uma
// leitura por bytes seria trocar o dado estruturado por grep — o defeito que
// este pacote já pagou uma vez.
func goCitationChunks(rel, path string, declared declarations) []citationChunk {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	pkg := filepath.ToSlash(filepath.Dir(rel))
	if declared.byPkg[pkg] == nil {
		declared.byPkg[pkg] = map[string]bool{}
	}
	declare := func(name string) {
		declared.module[name] = true
		declared.byPkg[pkg][name] = true
	}
	var out []citationChunk
	for _, g := range f.Comments {
		for _, c := range g.List {
			out = append(out, citationChunk{file: rel, pkg: pkg, line: fset.Position(c.Pos()).Line, kind: chunkGoComment, text: c.Text})
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ImportSpec:
			return false // caminho de import é código
		case *ast.FuncDecl:
			declare(x.Name.Name)
		case *ast.TypeSpec:
			declare(x.Name.Name)
		case *ast.ValueSpec:
			for _, id := range x.Names {
				declare(id.Name)
			}
		case *ast.Field:
			// Campo de struct, método de interface e parâmetro nomeado. Os três
			// entram: o que interessa é se o NOME ainda é declarado em algum
			// lugar, e um campo ainda vivo é exatamente o que faz `Nome` de
			// `metrics.Amostra` não ser citação pendurada de `ContadorInfo.Nome`.
			for _, id := range x.Names {
				declare(id.Name)
			}
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				out = append(out, citationChunk{file: rel, pkg: pkg, line: fset.Position(x.Pos()).Line, kind: chunkGoLiteral, text: x.Value})
			}
		}
		return true
	})
	return out
}

// eachWordToken chama fn para cada sequência maximal de [A-Za-z0-9_] de s, com
// o offset do começo. É a fronteira `\b...\b` sem regexp: o token só casa
// INTEIRO, então `LimiteRowIDs` não conta como citação de `LimiteRowID`.
func eachWordToken(s string, fn func(tok string, off int)) {
	for i := 0; i < len(s); {
		if !isWordByte(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && isWordByte(s[j]) {
			j++
		}
		fn(s[i:j], i)
		i = j
	}
}

// isDotQualified diz se o token que começa em off vem depois de `x.` — a
// forma `pkg.Nome`/`T.Nome`, única em que um nome velho que ainda existe como
// outra declaração é cobrado.
func isDotQualified(s string, off int) bool {
	return off >= 2 && s[off-1] == '.' && isWordByte(s[off-2])
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func startsUpper(s string) bool { return s != "" && s[0] >= 'A' && s[0] <= 'Z' }

// hasInnerUpper diz se o token tem maiúscula DEPOIS do primeiro caractere.
// Essa forma não é palavra de prosa em PT-BR nem em inglês: é identificador.
func hasInnerUpper(s string) bool {
	for i := 1; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return true
		}
	}
	return false
}

// addressLeaf devolve a folha de um endereço do TSV (`Snapshot.Epoca` →
// `Epoca`), que é o nome pelo qual a prosa cita um método ou campo.
func addressLeaf(address string) string {
	if i := strings.LastIndex(address, "."); i >= 0 {
		return address[i+1:]
	}
	return address
}
