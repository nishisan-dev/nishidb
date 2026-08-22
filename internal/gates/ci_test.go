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

// caminhoDoWorkflowDeCI usa raizDoRepo, já definido em lint_test.go: os dois
// arquivos deste pacote enxergam a raiz do repositório do mesmo lugar.
var caminhoDoWorkflowDeCI = filepath.Join(raizDoRepo, ".github", "workflows", "ci.yml")

// leWorkflowDeCI devolve o conteúdo bruto do workflow. Ausência é FALHA, não
// SKIP: pular aqui repetiria, no teste, o defeito que a Task A2 existe para
// fechar — a automação sumir e ninguém perceber.
func leWorkflowDeCI(t *testing.T) string {
	t.Helper()
	bruto, err := os.ReadFile(caminhoDoWorkflowDeCI)
	if err != nil {
		t.Fatalf(".github/workflows/ci.yml não existe ou não pôde ser lido: %v — sem ele o projeto não tem CI, só o gate local", err)
	}
	return string(bruto)
}

func TestWorkflowDeCIExiste(t *testing.T) {
	leWorkflowDeCI(t)
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
	mk, err := os.ReadFile(filepath.Join(raizDoRepo, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	doMakefile := reInstalacao.FindStringSubmatch(string(mk))
	if doMakefile == nil {
		t.Fatal("o Makefile não instrui mais como instalar o golangci-lint; a comparação perde a referência")
	}

	doWorkflow := reInstalacao.FindStringSubmatch(leWorkflowDeCI(t))
	if doWorkflow == nil {
		t.Fatal("o workflow de CI não instala o golangci-lint com uma versão fixa (golangci-lint@vX.Y.Z)")
	}

	if doMakefile[1] != doWorkflow[1] {
		t.Errorf("Makefile instala golangci-lint@%s mas o workflow instala golangci-lint@%s — os dois precisam medir com a MESMA versão, senão a comparação com a linha de base passa a comparar instrumentos diferentes", doMakefile[1], doWorkflow[1])
	}
}

var (
	reTesteSemRace = regexp.MustCompile(`(?m)go test \./\.\.\.[ \t]*$`)
	reTesteComRace = regexp.MustCompile(`(?m)go test (?:\./\.\.\. -race|-race \./\.\.\.)[ \t]*$`)
)

// Os alvos `test` e `test-race` do Makefile são complementares POR
// CONSTRUÇÃO — os gates de alocação e o de vazão da ingestão pelo CLI são
// `//go:build !race` e se anunciam SKIP sob -race —, então o CI precisa dos
// DOIS, e nenhum substitui o outro. Faltar qualquer um dos dois devolve, em
// CI, exatamente o buraco que o Makefile já documenta entre os dois alvos.
func TestWorkflowDeCIRodaOsDoisAlvosDeTeste(t *testing.T) {
	bruto := leWorkflowDeCI(t)
	if !reTesteSemRace.MatchString(bruto) {
		t.Error("o workflow não roda `go test ./...` sem -race — os gates de alocação e o de vazão do CLI ficam sem cobertura em CI")
	}
	if !reTesteComRace.MatchString(bruto) {
		t.Error("o workflow não roda `go test ./... -race` — o detector de corrida fica sem cobertura em CI")
	}
}

var reVersaoDeGo = regexp.MustCompile(`go-version:\s*['"]?([^\s'"\n]+)['"]?`)

// A versão de Go do workflow tem de ser a EXATA que mede a base localmente
// (1.26.5), não uma faixa (`1.26`, `>=1.26.5`, `^1.26`, `stable`): faixa
// solta deixa o runner escolher outro patch, e a mesma lição que justifica a
// versão fixa do golangci-lint — versão do instrumento é a mesma dimensão
// que os tetos, configuração que muda o número — vale para o compilador.
func TestWorkflowDeCIPinaGoNaVersaoExata(t *testing.T) {
	bruto := leWorkflowDeCI(t)
	achados := reVersaoDeGo.FindAllStringSubmatch(bruto, -1)
	if len(achados) == 0 {
		t.Fatal("o workflow não fixa go-version em nenhum step de actions/setup-go")
	}
	for _, achado := range achados {
		if achado[1] != "1.26.5" {
			t.Errorf("go-version = %q no workflow: a base mede com go1.26.5 exato, faixa solta compara instrumentos diferentes", achado[1])
		}
	}
}

// TestArquivoDeBaselineExisteEContemUmInteiro fixa a outra metade do ratchet
// (Ruling 3 do controlador): o arquivo que guarda o número contra o qual o
// job `lint` compara. Sem ele — ou com o conteúdo corrompido —, o job fica
// sem o número contra o qual julgar a contagem, e `cat .golangci-baseline`
// falharia no runner com um erro que não nomeia a causa.
func TestArquivoDeBaselineExisteEContemUmInteiro(t *testing.T) {
	bruto, err := os.ReadFile(filepath.Join(raizDoRepo, ".golangci-baseline"))
	if err != nil {
		t.Fatalf(".golangci-baseline não existe ou não pôde ser lido: %v — o job lint fica sem o número contra o qual comparar a contagem de apontamentos", err)
	}
	conteudo := strings.TrimSpace(string(bruto))
	if _, err := strconv.Atoi(conteudo); err != nil {
		t.Fatalf(".golangci-baseline não contém um inteiro: %q (%v)", conteudo, err)
	}
}

// passoDeWorkflow e jobDeWorkflow decodificam só o campo que os testes
// abaixo precisam do YAML de Actions — não o schema inteiro do formato.
// Decodificar como DADO, via yaml.v3, e não varrer o arquivo bruto como
// texto, é o que impede uma MENÇÃO em comentário de satisfazer o teste: o
// parser descarta comentários, então só o que o runner de fato executa
// pode bater com os regexps abaixo.
type passoDeWorkflow struct {
	Run string `yaml:"run"`
}

type jobDeWorkflow struct {
	Steps []passoDeWorkflow `yaml:"steps"`
}

type arquivoDeWorkflowDeCI struct {
	Jobs map[string]jobDeWorkflow `yaml:"jobs"`
}

// scriptDoJobDeLint devolve a concatenação dos `run:` de todos os steps do
// job `lint` — só o script que o runner de fato executa.
func scriptDoJobDeLint(t *testing.T) string {
	t.Helper()
	var wf arquivoDeWorkflowDeCI
	if err := yaml.Unmarshal([]byte(leWorkflowDeCI(t)), &wf); err != nil {
		t.Fatalf(".github/workflows/ci.yml não é YAML válido: %v", err)
	}
	job, ok := wf.Jobs["lint"]
	if !ok {
		t.Fatal("o workflow não tem um job `lint`")
	}
	var script strings.Builder
	for _, passo := range job.Steps {
		script.WriteString(passo.Run)
		script.WriteString("\n")
	}
	return script.String()
}

var (
	reReferenciaABaseline = regexp.MustCompile(`\.golangci-baseline`)
	reExtracaoDaContagem  = regexp.MustCompile(`Issues\s*\|\s*length`)
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
	script := scriptDoJobDeLint(t)
	if !reReferenciaABaseline.MatchString(script) {
		t.Error("o script do job lint não referencia .golangci-baseline — a comparação com a linha de base pode ter sido removida")
	}
	if !reExtracaoDaContagem.MatchString(script) {
		t.Error("o script do job lint não extrai a contagem de apontamentos (`Issues | length`) — sem isso não há o que comparar com a linha de base")
	}
}
