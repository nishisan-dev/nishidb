package gates_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// A Task A2 (#14) cria o CI que o resto do ciclo vai usar, e este arquivo é o
// oráculo dele: sem verificação, o workflow diverge do Makefile em silêncio —
// o mesmo defeito do débito 53 (gate que passa sem a ferramenta, ou que mede
// com outra versão dela) por uma porta nova, entre .github/workflows/ci.yml e
// o Makefile em vez de entre `make lint` e o binário ausente.
//
// O teste lê .github/workflows/ci.yml como DADO — não executa `act` nem
// qualquer runner de Actions, que não roda de graça fora do GitHub — e fixa
// só o que É verificável sem executar: o arquivo existe; a versão do
// golangci-lint no workflow bate, byte a byte, com a instrução de instalação
// do Makefile (comparando as duas extrações, sem duplicar o número aqui);
// os dois alvos de teste complementares (`./...` e `-race`) estão presentes;
// e a versão de Go do workflow é a exata que mede a base (1.26.5), não uma
// faixa.

// ciWorkflowPath usa raizDoRepo, já definido em lint_test.go: os dois
// arquivos deste pacote enxergam a raiz do repositório do mesmo lugar.
//
// releaseWorkflowPath e benchWorkflowPath são os outros dois
// workflows que fixam go-version (Important 1 da revisão final): antes deles,
// só ci.yml tinha oráculo — nada fixava que release.yml builda com `make
// build` (a única fonte do comando, segundo o próprio comentário do
// workflow) nem que a go-version de release.yml/bench.yml acompanha o pin de
// ci.yml. Um `go build` próprio no lugar de `make build` sobe o binário de
// release SEM os ldflags de buildinfo (Task A3), e
// scripts/nishidb-update.sh usa esse carimbo para confirmar a versão nova em
// /v1/stats — sem ele a verificação pós-start nunca fecha e o script dispara
// rollback espúrio no piloto mesmo com o binário certo no ar.
var (
	ciWorkflowPath      = filepath.Join(repoRoot, ".github", "workflows", "ci.yml")
	releaseWorkflowPath = filepath.Join(repoRoot, ".github", "workflows", "release.yml")
	benchWorkflowPath   = filepath.Join(repoRoot, ".github", "workflows", "bench.yml")
)

// readWorkflow devolve o conteúdo bruto de um arquivo de workflow qualquer.
// Ausência é FALHA, não SKIP: pular aqui repetiria, no teste, o defeito que a
// Task A2 existe para fechar — a automação sumir e ninguém perceber.
func readWorkflow(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s não existe ou não pôde ser lido: %v — sem ele o projeto não tem essa automação, só o gate local", path, err)
	}
	return string(raw)
}

// readCIWorkflow é o atalho para ci.yml que os testes existentes já usavam.
func readCIWorkflow(t *testing.T) string { return readWorkflow(t, ciWorkflowPath) }

func TestWorkflowDeCIExiste(t *testing.T) {
	readCIWorkflow(t)
}

// A versão do golangci-lint no workflow tem de ser a MESMA da instrução de
// instalação do Makefile, e a comparação reaproveita reInstalacao — o mesmo
// regexp que TestAInstrucaoDeInstalacaoFixaAVersaoDoLinter usa para extrair a
// versão do Makefile, declarado em lint_test.go. Hardcodar "v2.12.2" aqui de
// novo reabriria, no próprio teste, o defeito que ele existe para fechar:
// alguém sobe a versão em um lugar, esquece o outro, e os dois arquivos
// divergem em silêncio — o mesmo binário mediria a linha de base com um
// instrumento diferente do que o Makefile documenta.
func TestVersaoDoGolangciLintNoWorkflowBateComAInstrucaoDoMakefile(t *testing.T) {
	mk, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	fromMakefile := reInstallation.FindStringSubmatch(string(mk))
	if fromMakefile == nil {
		t.Fatal("o Makefile não instrui mais como instalar o golangci-lint; a comparação perde a referência")
	}

	fromWorkflow := reInstallation.FindStringSubmatch(readCIWorkflow(t))
	if fromWorkflow == nil {
		t.Fatal("o workflow de CI não instala o golangci-lint com uma versão fixa (golangci-lint@vX.Y.Z)")
	}

	if fromMakefile[1] != fromWorkflow[1] {
		t.Errorf("Makefile instala golangci-lint@%s mas o workflow instala golangci-lint@%s — os dois precisam medir com a MESMA versão, senão a comparação com a linha de base passa a comparar instrumentos diferentes", fromMakefile[1], fromWorkflow[1])
	}
}

var (
	reTestWithoutRace = regexp.MustCompile(`(?m)go test \./\.\.\.[ \t]*$`)
	reTestWithRace    = regexp.MustCompile(`(?m)go test (?:\./\.\.\. -race|-race \./\.\.\.)[ \t]*$`)
)

// Os alvos `test` e `test-race` do Makefile são complementares POR
// CONSTRUÇÃO — os gates de alocação e o de vazão da ingestão pelo CLI são
// `//go:build !race` e se anunciam SKIP sob -race —, então o CI precisa dos
// DOIS, e nenhum substitui o outro. Faltar qualquer um dos dois devolve, em
// CI, exatamente o buraco que o Makefile já documenta entre os dois alvos.
func TestWorkflowDeCIRodaOsDoisAlvosDeTeste(t *testing.T) {
	raw := readCIWorkflow(t)
	if !reTestWithoutRace.MatchString(raw) {
		t.Error("o workflow não roda `go test ./...` sem -race — os gates de alocação e o de vazão do CLI ficam sem cobertura em CI")
	}
	if !reTestWithRace.MatchString(raw) {
		t.Error("o workflow não roda `go test ./... -race` — o detector de corrida fica sem cobertura em CI")
	}
}

// TestWorkflowDeCIRodaOTestePython fixa o mesmo ratchet da Task A3: antes
// dela, scripts/nishidb-stats_test.py — o teste do painel que lê /v1/stats e
// mostra a versão do binário — só rodava pela mão de quem lembrasse de
// chamar `python3` direto. Nenhum Makefile nem workflow o executava, então a
// próxima alteração em nishidb-stats.py podia quebrar a exibição da versão
// sem que NADA acusasse — o mesmo defeito do débito 53 (gate ausente que
// ninguém percebe), agora numa porta em Python em vez de Go. A verificação
// roda sobre o SCRIPT do job `test` (scriptDoJob), não sobre o arquivo
// bruto, pelo mesmo motivo do job `lint`: um comentário mencionando
// "test-python" não pode satisfazer o teste com o passo de fato removido.
func TestWorkflowDeCIRodaOTestePython(t *testing.T) {
	if !strings.Contains(jobScript(t, "test"), "test-python") {
		t.Error("o job `test` não roda `make test-python` — scripts/nishidb-stats.py (o painel do operador) fica sem cobertura em CI")
	}
}

var reGoVersion = regexp.MustCompile(`go-version:\s*['"]?([^\s'"\n]+)['"]?`)

// A versão de Go do workflow tem de ser a EXATA que mede a base localmente
// (1.26.5), não uma faixa (`1.26`, `>=1.26.5`, `^1.26`, `stable`): faixa
// solta deixa o runner escolher outro patch, e a mesma lição que justifica a
// versão fixa do golangci-lint — versão do instrumento é a mesma dimensão
// que os tetos, configuração que muda o número — vale para o compilador.
func TestWorkflowDeCIPinaGoNaVersaoExata(t *testing.T) {
	raw := readCIWorkflow(t)
	matches := reGoVersion.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		t.Fatal("o workflow não fixa go-version em nenhum step de actions/setup-go")
	}
	for _, match := range matches {
		if match[1] != "1.26.5" {
			t.Errorf("go-version = %q no workflow: a base mede com go1.26.5 exato, faixa solta compara instrumentos diferentes", match[1])
		}
	}
}

// goVersionsInWorkflow extrai todas as ocorrências de go-version de um
// workflow qualquer, reaproveitando reVersaoDeGo — a mesma regexp que
// TestWorkflowDeCIPinaGoNaVersaoExata já usa para ci.yml. Fatal se nenhuma
// aparecer: um workflow sem go-version fixado não tem o que comparar.
func goVersionsInWorkflow(t *testing.T, path string) []string {
	t.Helper()
	matches := reGoVersion.FindAllStringSubmatch(readWorkflow(t, path), -1)
	if len(matches) == 0 {
		t.Fatalf("%s não fixa go-version em nenhum step de actions/setup-go", path)
	}
	out := make([]string, len(matches))
	for i, a := range matches {
		out[i] = a[1]
	}
	return out
}

// TestWorkflowDeReleasePinaGoNaMesmaVersaoDoCI e
// TestWorkflowDeBenchPinaGoNaMesmaVersaoDoCI fecham o Important 1 da revisão
// final: TestWorkflowDeCIPinaGoNaVersaoExata só varria ci.yml, e nada fixava
// que release.yml e bench.yml acompanham o mesmo pin. Um Go diferente ali
// builda o binário de release (ou o benchmark) com outro toolchain do que a
// suíte testou — o mesmo risco de "instrumento diferente medindo o mesmo
// número" que já justifica o pin exato em ci.yml.
//
// A comparação é entre as EXTRAÇÕES dos dois arquivos, e não contra uma
// constante nova aqui — hardcodar "1.26.5" outra vez reabriria, neste
// próprio teste, o defeito que ele existe para fechar: alguém sobe a versão
// em um lugar, esquece os outros, e a comparação segue "verde" porque cada
// teste checa contra o número escrito à mão, não contra o vizinho.
func TestWorkflowDeReleasePinaGoNaMesmaVersaoDoCI(t *testing.T) {
	fromCI := goVersionsInWorkflow(t, ciWorkflowPath)
	fromRelease := goVersionsInWorkflow(t, releaseWorkflowPath)
	for _, match := range fromRelease {
		if match != fromCI[0] {
			t.Errorf("release.yml fixa go-version=%q, ci.yml mede a base com %q — os dois precisam medir com a MESMA versão, senão o binário de release sai de um Go diferente do que a suíte testou", match, fromCI[0])
		}
	}
}

func TestWorkflowDeBenchPinaGoNaMesmaVersaoDoCI(t *testing.T) {
	fromCI := goVersionsInWorkflow(t, ciWorkflowPath)
	fromBench := goVersionsInWorkflow(t, benchWorkflowPath)
	for _, match := range fromBench {
		if match != fromCI[0] {
			t.Errorf("bench.yml fixa go-version=%q, ci.yml mede a base com %q — os dois precisam medir com a MESMA versão, senão o benchmark roda sobre um binário compilado por outro Go", match, fromCI[0])
		}
	}
}

// TestWorkflowDeReleaseBuildaComOAlvoDoMakefile fixa a outra metade do
// Important 1: nada garantia que .github/workflows/release.yml builda com
// `make build` — a ÚNICA fonte do comando de build, segundo o próprio
// comentário do workflow — em vez de um `go build` próprio. Um `go build`
// direto sobe o binário de release SEM os ldflags de buildinfo (Task A3), e
// scripts/nishidb-update.sh usa exatamente esse carimbo para confirmar a
// versão nova em /v1/stats — sem ele a verificação pós-start nunca fecha, e
// o script dispara ROLLBACK ESPÚRIO no piloto mesmo com o binário certo no
// ar.
//
// A verificação roda sobre o SCRIPT do job (scriptDoJobDoArquivo), não sobre
// o arquivo bruto: um comentário em prosa mencionando "make build" não pode
// satisfazer o teste com o passo de fato trocado por outro comando.
func TestWorkflowDeReleaseBuildaComOAlvoDoMakefile(t *testing.T) {
	script := jobScriptInFile(t, releaseWorkflowPath, "anexa-binario")
	if !regexp.MustCompile(`(?m)^\s*make build\s*$`).MatchString(script) {
		t.Errorf("o job `anexa-binario` de release.yml não roda `make build` — pode ter sido trocado por um `go build` próprio, que sobe o binário SEM o carimbo de buildinfo (Task A3)\nscript:\n%s", script)
	}
}

// TestArquivoDeBaselineExisteEContemUmInteiro fixa a outra metade do ratchet
// (Ruling 3 do controlador): o arquivo que guarda o número contra o qual o
// job `lint` compara. Sem ele — ou com o conteúdo corrompido —, o job fica
// sem o número contra o qual julgar a contagem, e `cat .golangci-baseline`
// falharia no runner com um erro que não nomeia a causa.
func TestArquivoDeBaselineExisteEContemUmInteiro(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot, ".golangci-baseline"))
	if err != nil {
		t.Fatalf(".golangci-baseline não existe ou não pôde ser lido: %v — o job lint fica sem o número contra o qual comparar a contagem de apontamentos", err)
	}
	content := strings.TrimSpace(string(raw))
	if _, err := strconv.Atoi(content); err != nil {
		t.Fatalf(".golangci-baseline não contém um inteiro: %q (%v)", content, err)
	}
}

// workflowStep e jobDeWorkflow decodificam só o campo que os testes
// abaixo precisam do YAML de Actions — não o schema inteiro do formato.
// Decodificar como DADO, via yaml.v3, e não varrer o arquivo bruto como
// texto, é o que impede uma MENÇÃO em comentário de satisfazer o teste: o
// parser descarta comentários, então só o que o runner de fato executa
// pode bater com os regexps abaixo.
type workflowStep struct {
	Run string `yaml:"run"`
}

type workflowJob struct {
	Steps []workflowStep `yaml:"steps"`
}

type ciWorkflowFile struct {
	Jobs map[string]workflowJob `yaml:"jobs"`
}

// jobScriptInFile devolve a concatenação dos `run:` de todos os steps
// do job pedido, em QUALQUER arquivo de workflow — a generalização de
// scriptDoJob (que só enxergava ci.yml) para cobrir release.yml e bench.yml
// sem duplicar o parser.
func jobScriptInFile(t *testing.T, filePath, jobName string) string {
	t.Helper()
	var wf ciWorkflowFile
	if err := yaml.Unmarshal([]byte(readWorkflow(t, filePath)), &wf); err != nil {
		t.Fatalf("%s não é YAML válido: %v", filePath, err)
	}
	job, ok := wf.Jobs[jobName]
	if !ok {
		t.Fatalf("%s não tem um job `%s`", filePath, jobName)
	}
	var script strings.Builder
	for _, step := range job.Steps {
		script.WriteString(step.Run)
		script.WriteString("\n")
	}
	return script.String()
}

// jobScript devolve a concatenação dos `run:` de todos os steps do job
// pedido em ci.yml — só o script que o runner de fato executa.
func jobScript(t *testing.T, name string) string {
	return jobScriptInFile(t, ciWorkflowPath, name)
}

// lintJobScript é o atalho que os testes do job `lint` já usavam antes
// de scriptDoJob virar genérico — mantido para não duplicar "lint" em cada
// chamador.
func lintJobScript(t *testing.T) string { return jobScript(t, "lint") }

var (
	reBaselineReference = regexp.MustCompile(`\.golangci-baseline`)
	reCountExtraction   = regexp.MustCompile(`Issues\s*\|\s*length`)
)

// TestWorkflowDeCIComparaAContagemComABaseline fixa a PRESENÇA do passo de
// comparação em si (Ruling 3). O exit code do golangci-lint NÃO é o
// veredito — com a linha de base acima de zero ele sempre sai != 0 —, então
// trocar o corpo do step por só `golangci-lint run || true`, sem extrair a
// contagem nem compará-la com .golangci-baseline, devolveria um job lint que
// SEMPRE passa, apontamento novo incluído. Sem este teste, essa reversão do
// ratchet passaria despercebida — o mesmo defeito do débito 53, agora no
// passo que sucedeu o exit code como veredito.
//
// A verificação roda sobre o SCRIPT do job (scriptDoJobDeLint), não sobre o
// arquivo bruto: um comentário em prosa mencionando ".golangci-baseline"
// não pode fazer este teste passar com a comparação de fato removida.
func TestWorkflowDeCIComparaAContagemComABaseline(t *testing.T) {
	script := lintJobScript(t)
	if !reBaselineReference.MatchString(script) {
		t.Error("o script do job lint não referencia .golangci-baseline — a comparação com a linha de base pode ter sido removida")
	}
	if !reCountExtraction.MatchString(script) {
		t.Error("o script do job lint não extrai a contagem de apontamentos (`Issues | length`) — sem isso não há o que comparar com a linha de base")
	}
}

var (
	reJavaVersion  = regexp.MustCompile(`java-version:\s*['"]?([^\s'"\n]+)['"]?`)
	reDistribution = regexp.MustCompile(`distribution:\s*['"]?([^\s'"\n]+)['"]?`)
)

// O job `jdbc` roda o teste de INTEGRAÇÃO do driver, e não só os unitários.
//
// Sem ele, `make test-jdbc` só roda para quem lembrar de chamá-lo — e o que
// fica sem cobertura é justamente a metade que os unitários não podem provar:
// o driver contra o binário real, com TLS, credencial e um dataset com um
// campo de cada tipo. É o mesmo defeito do débito 53 (gate que existe mas
// ninguém executa) por uma porta nova.
//
// A verificação roda sobre o SCRIPT do job, e não sobre o arquivo bruto: um
// comentário mencionando "test-jdbc" não pode satisfazer o teste com o passo
// de fato removido.
func TestWorkflowDeCIRodaOTesteDeIntegracaoDoDriver(t *testing.T) {
	script := jobScript(t, "jdbc")
	if !strings.Contains(script, "test-jdbc") {
		t.Error("o job `jdbc` não roda `make test-jdbc` — o driver fica sem prova contra o servidor real em CI")
	}
	// test-jdbc-unit NÃO basta: ele fala com o servidor HTTP do JDK, com
	// respostas escritas à mão. Aceitar o alvo dos unitários aqui deixaria o
	// gate verde exatamente no caso que ele existe para pegar.
	if strings.Contains(script, "test-jdbc-unit") && !strings.Contains(script, "make test-jdbc\n") {
		t.Error("o job `jdbc` roda só os unitários do driver; o alvo de integração é `make test-jdbc`")
	}
}

// A versão de Java do workflow é a EXATA do piso declarado no pom, e a
// distribuição é fixada.
//
// A comparação é entre as duas EXTRAÇÕES — do ci.yml e do pom.xml —, nunca
// contra um "21" escrito à mão neste teste. Hardcodar o número reabriria aqui
// o defeito que o teste fecha: alguém sobe o piso do driver no pom, o
// workflow continua compilando com o Java antigo, e o gate segue verde porque
// cada lado é conferido contra a constante e não contra o vizinho.
//
// Faixa solta (`21.x`, `21+`) é recusada pelo mesmo motivo que a versão de Go
// é fixa: versão de instrumento é configuração que muda o resultado, e deixar
// o runner escolher o patch faz o CI compilar com um compilador que ninguém
// mediu.
func TestWorkflowDeCIPinaJavaNoPisoDeclaradoNoPom(t *testing.T) {
	raw := readCIWorkflow(t)
	matches := reJavaVersion.FindAllStringSubmatch(raw, -1)
	if len(matches) == 0 {
		t.Fatal("o workflow não fixa java-version em nenhum step de actions/setup-java")
	}
	floor := readPom(t).Properties.Release
	if floor == "" {
		t.Fatal("o pom do driver não declara maven.compiler.release")
	}
	for _, match := range matches {
		if strings.ContainsAny(match[1], ".+*") {
			t.Errorf("java-version = %q é faixa, não versão: o runner escolheria o patch", match[1])
		}
		if match[1] != floor {
			t.Errorf("java-version = %q no workflow e maven.compiler.release = %q no pom: "+
				"o CI compilaria o driver com um Java que não é o piso declarado", match[1], floor)
		}
	}
}

// A distribuição do JDK é FIXADA, e não deixada ao default da action.
//
// Distribuições diferentes trazem bibliotecas e defaults de TLS diferentes, e
// o teste de integração do driver depende de handshake com certificado
// gerado na hora. Deixar isso ao acaso do runner é ter um teste que passa por
// motivo que ninguém controla.
func TestWorkflowDeCIFixaADistribuicaoDoJdk(t *testing.T) {
	matches := reDistribution.FindAllStringSubmatch(readCIWorkflow(t), -1)
	if len(matches) == 0 {
		t.Error("o workflow não fixa `distribution` no setup-java: o JDK do CI fica a cargo do default da action")
	}
}
