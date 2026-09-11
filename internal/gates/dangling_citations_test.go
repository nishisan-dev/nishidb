// Este arquivo confere que identificador RENOMEADO pelo ciclo de
// identificadores em inglês não continua CITADO pelo nome velho.
package gates_test

import (
	"fmt"
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

// identMappingDir é onde moram os TSV do ciclo, e os dois cabeçalhos são os
// EXATOS dos dois formatos que este gate lê. O diretório guarda mais de um
// formato (caminhos e pacotes têm duas colunas, o inventário tem sete), então
// o cabeçalho é o que discrimina: ler `mapeamento-*.tsv` sem conferi-lo faria
// o gate interpretar `velho`/`novo` como `pacote`/`especie` e sair com zero
// candidato — que é justamente a vacuidade que ele proíbe.
//
// O segundo formato é o dos GRUPOS DE INTERFACE, que o `scripts/ifacerename`
// aplica (spec §4.2). Ele ficou de fora da primeira redação, e o furo é da
// mesma classe que o gate existe para fechar: método de interface é renomeado
// por uma ferramenta DIFERENTE, com um TSV de outro formato, e por isso as
// citações de `AbreSnapshot`, `Indexado` e `Estado` sobreviveram a duas
// ferramentas e a duas revisões — dezessete delas em godoc, diagrama e
// documento. O endereço de uma linha desse TSV é `Interface.metodo` e a folha
// é o `metodo`, que é como a prosa cita um método.
const (
	identMappingDir      = "planning/identificadores-em-ingles"
	identMappingHeader   = "pacote\tespecie\tendereco\tnovo\tmotivo"
	identInterfaceHeader = "pacote\tinterface\tmetodo\tnovo"
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
// internal/storage, e `Nome` continua sendo campo de `counter.Definicao` — um
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

	byOldName := indexByOldName(renamed, protected)
	if len(byOldName) == 0 {
		t.Fatalf("nenhum nome velho a procurar em %s: com %d linhas de rename lidas, isso significa TSV movido, "+
			"cabeçalho mudado ou lista de protegidos engolindo tudo — e o gate passaria por vacuidade",
			identMappingDir, len(renamed))
	}

	chunks, declared := danglingCitationChunks(t)
	if len(chunks) == 0 {
		t.Fatalf("nenhum texto varrido: o escopo do gate ficou vazio e ele passaria por vacuidade")
	}
	hits := danglingHits(chunks, declared, byOldName, surviving)
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

// danglingHit é uma citação pendurada encontrada: onde ela está, o token e a
// entrada do mapeamento que a explica.
type danglingHit struct {
	file  string
	line  int
	token string
	entry renamedIdent
}

// indexByOldName é o índice que torna a varredura viável: um passe por token do
// repositório, não um por (token × candidato). Token protegido e nome que não é
// reconhecível como identificador (ver o godoc do teste) ficam de fora aqui, e
// não no laço de varredura.
func indexByOldName(renamed []renamedIdent, protected map[string]bool) map[string][]renamedIdent {
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
	return byOldName
}

// danglingHits varre os pedaços e devolve as citações penduradas. É uma função
// à parte do teste porque é o que o caso do TSV de interface exercita: um teste
// que reimplementasse esta varredura provaria a si mesmo, não ao gate.
func danglingHits(chunks []citationChunk, declared declarations, byOldName map[string][]renamedIdent, surviving map[string]bool) []danglingHit {
	var hits []danglingHit
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
			hits = append(hits, danglingHit{c.file, c.line + strings.Count(c.text[:off], "\n"), tok, entry})
		})
	}
	return hits
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
	renamed, surviving, identFiles, ifaceFiles, err := readIdentMappings(dir)
	if err != nil {
		t.Fatalf("os mapeamentos do ciclo não puderam ser lidos em %s: %v", identMappingDir, err)
	}
	if identFiles == 0 {
		t.Fatalf("nenhum TSV de identificadores em %s com o cabeçalho %q: o gate ficaria sem oráculo",
			identMappingDir, identMappingHeader)
	}
	if ifaceFiles == 0 {
		t.Fatalf("nenhum TSV de grupos de interface em %s com o cabeçalho %q: os métodos renomeados pelo "+
			"scripts/ifacerename sairiam do escopo do gate sem ninguém notar",
			identMappingDir, identInterfaceHeader)
	}
	return renamed, surviving
}

// readIdentMappings lê os DOIS formatos de TSV do ciclo num diretório e devolve
// as linhas com rename efetivo, o conjunto dos nomes que sobrevivem e quantos
// arquivos de cada formato foram lidos. As contagens são o que a vacuidade
// cobra: um formato que sumisse do diretório calaria metade do gate.
//
// O formato de IDENTIFICADORES traz a espécie e o endereço; o de GRUPOS DE
// INTERFACE traz interface e método, e o endereço `Interface.metodo` é
// sintetizado — é por ele que `Store.Estado` num documento é cobrado como
// citação qualificada, e a folha `Estado` sozinha só onde a regra normal
// permite.
func readIdentMappings(dir string) (renamed []renamedIdent, surviving map[string]bool, identFiles, ifaceFiles int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	surviving = map[string]bool{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tsv") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, nil, 0, 0, fmt.Errorf("%s: %w", e.Name(), err)
		}
		lines := strings.Split(string(content), "\n")
		if len(lines) == 0 {
			continue
		}
		switch strings.TrimRight(lines[0], "\r") {
		case identMappingHeader:
			identFiles++
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
		case identInterfaceHeader:
			ifaceFiles++
			for _, line := range lines[1:] {
				cols := strings.Split(strings.TrimRight(line, "\r"), "\t")
				if len(cols) < 4 {
					continue
				}
				pkg, iface, metodo, newName := cols[0], cols[1], cols[2], cols[3]
				if newName == "" || metodo == "" {
					continue
				}
				surviving[newName] = true
				if metodo != newName {
					renamed = append(renamed, renamedIdent{pkg: pkg, address: iface + "." + metodo, old: metodo, new: newName})
				}
			}
		default:
			continue // TSV de outro formato (caminhos, pacotes, inventário)
		}
	}
	return renamed, surviving, identFiles, ifaceFiles, nil
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
	// goFences são as faixas de bloco cercado ```go, calculadas UMA vez por
	// arquivo `.md`. Ver isIdentifierContext.
	goFences [][2]int
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
// prosa não põe: crase, ponto de qualificação, doc link, parêntese de chamada,
// chave de caixa de diagrama, RÓTULO de printf num literal ou bloco cercado
// ```go num `.md`.
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
	// `t.Errorf("Clausula = %q, quer %q", …)`: o rótulo antes do `=` é o IDIOMA
	// de mensagem de falha deste repositório, e é ali que sobrevive a citação
	// que nenhuma outra marca alcança — token de uma só maiúscula, dentro de
	// literal, em pacote diferente do que declarava o nome. Três delas
	// (parser/clauses_test.go:56,107,149) passaram pela primeira redação deste
	// gate com ele verde.
	if c.kind == chunkGoLiteral && strings.HasPrefix(c.text[end:], " = ") {
		return true
	}
	line := lineAround(c.text, off)
	if c.kind != chunkGoLiteral && strings.Count(c.text[line:off], "`")%2 == 1 {
		return true
	}
	// Bloco cercado ```go num `.md` é CÓDIGO, e a linha de campo de um
	// `type ... struct` cita identificador como qualquer arquivo Go. É a mesma
	// licença que a caixa do diagrama tem, e fecha os blocos de plano que a
	// limpeza do G1 deixou metade em inglês e metade em PT-BR.
	if insideRanges(c.goFences, off) {
		return true
	}
	// `Snapshot{Epoca, Instante}`: a caixa de um diagrama lista os campos entre
	// chaves, e é assim que os dois .puml do repositório citam o tipo.
	return strings.HasSuffix(c.file, ".puml") &&
		strings.Count(c.text[line:off], "{") > strings.Count(c.text[line:off], "}")
}

// goFenceRanges devolve as faixas de bloco cercado ```go de um `.md`. A
// varredura é UMA por arquivo, e não uma por token: contar crases desde o
// começo a cada palavra faria do gate um algoritmo quadrático sobre planos de
// dois mil linhas.
//
// Só ```go: um bloco ```yaml ou ```sql é dado e sintaxe de outra língua, e a
// mesma palavra ali é chave de configuração.
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

// danglingCitationSkipPaths são os arquivos e diretórios fora do escopo, pela
// mesma razão do diretório de mapeamentos: neles o nome velho é o DADO.
// O próprio arquivo deste gate entra na lista pelo motivo que já tirou
// `scripts/renamecite` dela: o godoc explica a regra com os nomes velhos de
// verdade (`Epoca`, `LimiteRowID`, `specsDoMarcador`), e sem a exceção ele
// acusaria as próprias explicações — 22 ocorrências na primeira rodada.
//
// `scripts/ifacerename` e `scripts/renametools` entram pelo MESMO motivo do
// `scripts/renamecite`, e a razão só ficou visível quando o oráculo parou de
// contar `testdata` (ver underTestdata): as duas ferramentas são escritas em
// cima do módulo de fixture `scripts/renametools/testdata/modulo`, que copia de
// propósito os nomes do repositório real (`a.Escritor`, `b.Arquivo`,
// `a.Medidor`) para reproduzir os achados da varredura. A prosa que explica os
// testes fala DESSES tipos, não dos homônimos de internal/index/segindex ou
// internal/metrics — 22 ocorrências, todas citação correta do fixture.
var danglingCitationSkipPaths = map[string]bool{
	"planning/identificadores-em-ingles":          true,
	"planning/identificadores-em-ingles.md":       true,
	"planning/identificadores-em-ingles-plano.md": true,
	"scripts/renamecite":                          true,
	"scripts/ifacerename":                         true,
	"scripts/renametools":                         true,
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
			chunks = append(chunks, goCitationChunks(rel, p, declared, !underTestdata(rel))...)
		case strings.HasSuffix(rel, ".md"), strings.HasSuffix(rel, ".puml"),
			filepath.Base(rel) == "Makefile",
			strings.HasPrefix(rel, ".github/workflows/") && strings.HasSuffix(rel, ".yml"),
			strings.HasPrefix(rel, "scripts/") && (strings.HasSuffix(rel, ".sh") || strings.HasSuffix(rel, ".py")):
			content, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			chunk := citationChunk{file: rel, line: 1, kind: chunkMarkup, text: string(content)}
			if strings.HasSuffix(rel, ".md") {
				chunk.goFences = goFenceRanges(chunk.text)
			}
			chunks = append(chunks, chunk)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("varredura: %v", err)
	}
	return chunks, declared
}

// underTestdata diz se o caminho está sob um diretório `testdata`.
//
// O go tool ignora essas árvores, então o que está declarado lá não é
// declaração do MÓDULO — é dado de fixture. É a mesma exclusão que o
// `scripts/renamecite` faz em collectDeclarations, e ela não é simetria por
// simetria: `scripts/renametools/testdata/modulo` declara `Escritor`, `Fecha`
// e `Apenda`, e enquanto essas três contaram como declaração viva o gate
// tratou como sobrevivente todo nome que só o fixture ainda tinha — cegando-se
// justamente para as citações penduradas que o ciclo deixou.
func underTestdata(rel string) bool {
	for _, seg := range strings.Split(rel, "/") {
		if seg == "testdata" {
			return true
		}
	}
	return false
}

// goCitationChunks extrai de um arquivo Go os comentários e os literais de
// string, e acrescenta a `declared` os nomes que o arquivo DECLARA. Arquivo que
// não parseia devolve nada: sintaxe é problema do compilador, e cair para uma
// leitura por bytes seria trocar o dado estruturado por grep — o defeito que
// este pacote já pagou uma vez.
//
// `oracle` diz se o arquivo alimenta o oráculo de declaração viva. Ele é falso
// sob `testdata` (ver underTestdata): o texto de lá continua sendo VARRIDO —
// comentário de fixture cita identificador como qualquer outro —, mas o que
// ele declara não conta como declaração do módulo.
func goCitationChunks(rel, path string, declared declarations, oracle bool) []citationChunk {
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
		if !oracle {
			return
		}
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
			// `counter.Definicao` não ser citação pendurada de
			// `ContadorInfo.Nome`, que o ciclo renomeou para `CounterInfo.Name`.
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

// TestInterfaceMethodRenamesAreChargedLikeAnyOtherLeaf exercita o segundo
// formato de TSV — o dos grupos que o `scripts/ifacerename` aplica — com um
// diretório de fixture, e não com o do repositório: o do repositório muda a
// cada grupo do ciclo, e um teste amarrado a ele mediria o estado da rodada em
// vez da regra.
//
// O caso é o que a revisão do G3 encontrou: `Store.AbreSnapshot` virou
// `OpenSnapshot` por uma ferramenta que o gate não lia, e dezessete citações
// ficaram penduradas. As três asserções cobrem as três metades da regra:
//
//   - folha com maiúscula INTERNA (`AbreSnapshot`) é cobrada em qualquer
//     lugar, como qualquer identificador que não é palavra de nenhuma língua;
//   - folha de uma maiúscula só (`Estado`) NÃO é cobrada em prosa nua — em
//     PT-BR ela abre frase —, e é o controle que prova que a leitura do TSV
//     novo não virou uma máquina de acusar prosa;
//   - a mesma folha É cobrada na forma qualificada pelo dono do endereço
//     (`Store.Estado`), que é a citação que diz de quem está falando.
func TestInterfaceMethodRenamesAreChargedLikeAnyOtherLeaf(t *testing.T) {
	dir := t.TempDir()
	// O TSV de identificadores entra porque o carregador lê os dois formatos, e
	// um diretório com um só não exercitaria a discriminação por cabeçalho.
	if err := os.WriteFile(filepath.Join(dir, "mapeamento-2-exemplo.tsv"),
		[]byte(identMappingHeader+"\ninternal/exemplo\ttype\tFila\tQueue\tfila é queue\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "grupos-de-interface-2-G9.tsv"),
		[]byte(identInterfaceHeader+"\ninternal/query/executor\tStore\tAbreSnapshot\tOpenSnapshot\n"+
			"internal/materializer\tStore\tEstado\tState\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	renamed, surviving, identFiles, ifaceFiles, err := readIdentMappings(dir)
	if err != nil {
		t.Fatalf("leitura do fixture: %v", err)
	}
	if identFiles != 1 || ifaceFiles != 1 {
		t.Fatalf("arquivos lidos: %d de identificadores e %d de interface, esperado 1 e 1", identFiles, ifaceFiles)
	}
	if !surviving["OpenSnapshot"] {
		t.Errorf("OpenSnapshot não entrou no conjunto dos nomes vivos: o alvo de um rename de método de "+
			"interface seria cobrado como se fosse nome velho, %v", surviving)
	}
	byOldName := indexByOldName(renamed, map[string]bool{})
	if _, ok := byOldName["AbreSnapshot"]; !ok {
		t.Fatalf("AbreSnapshot não entrou no índice de nomes velhos: o TSV de grupos de interface não foi lido, "+
			"e as citações dos métodos renomeados pelo ifacerename ficam fora do gate (%d linhas lidas)", len(renamed))
	}
	casos := []struct {
		nome   string
		chunk  citationChunk
		cobra  bool
		porque string
	}{
		{
			nome:   "folha com maiúscula interna em prosa nua",
			chunk:  citationChunk{file: "internal/outro/o.go", pkg: "internal/outro", line: 7, kind: chunkGoComment, text: "// A leitura chama AbreSnapshot antes de varrer."},
			cobra:  true,
			porque: "AbreSnapshot não é palavra de prosa em nenhuma das duas línguas do repositório",
		},
		{
			nome:   "folha de uma maiúscula em prosa nua",
			chunk:  citationChunk{file: "internal/outro/o.go", pkg: "internal/outro", line: 3, kind: chunkGoComment, text: "// Estado corrente da entidade, mantido na escrita."},
			cobra:  false,
			porque: "Estado abre frase em PT-BR, e cobrá-la nua encheria o gate de prosa legítima",
		},
		{
			nome:   "folha de uma maiúscula qualificada pelo dono",
			chunk:  citationChunk{file: "docs/debitos.md", line: 230, kind: chunkMarkup, text: "O materializador expõe Store.Estado para o executor."},
			cobra:  true,
			porque: "a forma qualificada diz de quem está falando",
		},
	}
	vazio := declarations{module: map[string]bool{}, byPkg: map[string]map[string]bool{}}
	for _, c := range casos {
		hits := danglingHits([]citationChunk{c.chunk}, vazio, byOldName, surviving)
		if got := len(hits) > 0; got != c.cobra {
			t.Errorf("%s: cobrada = %v, esperado %v (%s): %q", c.nome, got, c.cobra, c.porque, c.chunk.text)
		}
	}
}

// TestTheLiveDeclarationOracleIgnoresTestdata exercita a regra de underTestdata
// no arquivo REAL que a motivou, e não num fixture escrito para ela: o módulo
// de `scripts/renametools/testdata` declara `Escritor`, `Fecha` e `Apenda`, e
// enquanto essas declarações contaram como declaração do módulo o gate tratou
// como VIVO todo nome que só o fixture ainda tinha — calando-se justamente
// sobre as citações penduradas que o ciclo deixou.
//
// O controle é a metade que prova que o teste não passa por vacuidade: o MESMO
// arquivo, apresentado por um caminho fora de `testdata`, tem de alimentar o
// oráculo. Sem ele, uma regressão que parasse de colher declaração em qualquer
// lugar passaria verde aqui.
func TestTheLiveDeclarationOracleIgnoresTestdata(t *testing.T) {
	const rel = "scripts/renametools/testdata/modulo/a/a.go"
	path := filepath.Join(repoRoot, filepath.FromSlash(rel))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a fixture que este teste usa como caso real não está em %s (%v): o teste passaria sem "+
			"exercitar nada", rel, err)
	}
	dentro := declarations{module: map[string]bool{}, byPkg: map[string]map[string]bool{}}
	if chunks := goCitationChunks(rel, path, dentro, !underTestdata(rel)); len(chunks) == 0 {
		t.Fatalf("%s não rendeu pedaço nenhum: o arquivo não foi lido, e o resto do teste seria vácuo", rel)
	}
	for _, nome := range []string{"Escritor", "Fecha", "Apenda"} {
		if dentro.module[nome] {
			t.Errorf("%s: a declaração de %s sob testdata entrou no oráculo de declaração viva — o go tool "+
				"ignora essa árvore, e com ela o gate dá por vivo um nome que o ciclo matou", rel, nome)
		}
	}
	fora := declarations{module: map[string]bool{}, byPkg: map[string]map[string]bool{}}
	const relFora = "internal/exemplo/a.go"
	goCitationChunks(relFora, path, fora, !underTestdata(relFora))
	if !fora.module["Escritor"] {
		t.Errorf("%s: o mesmo arquivo, fora de testdata, não alimentou o oráculo — a colheita de declaração "+
			"parou de funcionar, e o teste de cima passaria por vacuidade", relFora)
	}
}
