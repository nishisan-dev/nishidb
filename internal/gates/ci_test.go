package gates_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
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
