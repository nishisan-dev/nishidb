package gates_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// O alvo `lint` do Makefile é um GATE, e a tese dele é "gate que passa com a
// ferramenta ausente não é gate, é carimbo" — foi assim que o projeto
// atravessou o M1 achando que tinha lint (débito 53). O alvo foi corrigido para
// falhar alto, e a correção veio SEM verificação de si mesma: nada no
// repositório executava o alvo, de modo que trocar `exit 1` por `exit 0`
// devolvia o defeito original com UM caractere e a suíte seguia verde. Um gate
// que não é verificado é exatamente o defeito que ele existe para fechar, um
// nível acima.
//
// É este teste que fecha o laço: ele EXECUTA `make lint` — o comando que a
// doutrina do projeto cita — com PATH controlado, e afirma as três coisas que
// a correção prometeu: exit não-zero, mensagem NOMEANDO o que instalar, e a
// mensagem no STDERR (quem redireciona `2>/dev/null` não pode confundir
// "ferramenta ausente" com "lint limpo").
//
// O Makefile não é intestável: o projeto já dispara binário externo em teste
// (o SIGKILL da ingestão em internal/cli) e já executa documentação (o
// quickstart do README). Este é o mesmo padrão aplicado ao alvo de lint.

const raizDoRepo = "../.."

// ondeEstaOMake resolve o binário antes de mexer no PATH do filho — o PATH que
// os testes montam existe para controlar o que a RECEITA enxerga, não para
// esconder o `make` do próprio teste.
//
// Ausência de `make` é FALHA, não SKIP: pular aqui repetiria, no teste, o
// defeito que ele fecha no alvo — sumir junto com a ferramenta e ainda assim
// devolver verde. `make` é o ponto de entrada documentado do projeto.
func ondeEstaOMake(t *testing.T) string {
	t.Helper()
	mk, err := exec.LookPath("make")
	if err != nil {
		t.Fatalf("make não está no PATH e é o ponto de entrada de todos os gates do projeto: %v", err)
	}
	return mk
}

// ambienteComPATH devolve o ambiente atual com o PATH SUBSTITUÍDO (não
// acrescentado): o teste precisa do controle total sobre o que `command -v`
// acha, senão o golangci-lint da máquina de quem roda decide o resultado.
func ambienteComPATH(caminho string, extras ...string) []string {
	atual := os.Environ()
	env := make([]string, 0, len(atual)+1+len(extras))
	for _, kv := range atual {
		if strings.HasPrefix(kv, "PATH=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "PATH="+caminho)
	return append(env, extras...)
}

// rodaMakeLint executa o alvo na raiz do repositório e devolve stdout, stderr e
// o código de saída. O código do `make` é 2 quando a receita falha (ele reporta
// "Error 1" do shell e sai 2, que é a convenção do GNU make): o que o gate
// promete é NÃO-ZERO, e é isso que os testes afirmam.
func rodaMakeLint(t *testing.T, path string, extras ...string) (stdout, stderr string, codigo int) {
	t.Helper()
	cmd := exec.Command(ondeEstaOMake(t), "lint")
	cmd.Dir = raizDoRepo
	cmd.Env = ambienteComPATH(path, extras...)
	var out, errBuf strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		var saiu *exec.ExitError
		if !errors.As(err, &saiu) {
			t.Fatalf("make lint não chegou a executar: %v", err)
		}
		codigo = saiu.ExitCode()
	}
	return out.String(), errBuf.String(), codigo
}

// Este é o teste do defeito: com o golangci-lint fora do PATH, o alvo tem de
// RECUSAR, não passar. O PATH do filho é um diretório temporário VAZIO — mais
// forte que "tirar um diretório da lista", porque não depende de onde a
// ferramenta está instalada nesta máquina.
func TestAlvoLintRecusaQuandoOGolangciLintNaoEstaNoPATH(t *testing.T) {
	stdout, stderr, codigo := rodaMakeLint(t, t.TempDir())

	if codigo == 0 {
		t.Fatalf("o alvo saiu 0 com o golangci-lint AUSENTE — é o carimbo do débito 53 de volta\nstdout: %q\nstderr: %q", stdout, stderr)
	}

	// A mensagem tem de bastar para o operador se desbloquear sozinho: o que
	// falta, como instalar e onde o binário cai. Sem isso o exit não-zero vira
	// um enigma, que é a outra forma de o gate não ser gate.
	for _, trecho := range []string{"golangci-lint", "NÃO rodou", "go install", "PATH"} {
		if !strings.Contains(stderr, trecho) {
			t.Errorf("o stderr do alvo não menciona %q — a recusa precisa nomear o que falta\nstderr: %q", trecho, stderr)
		}
	}

	// O canal importa. Com a mensagem no stdout, `make lint 2>/dev/null` não
	// distingue "ferramenta ausente" de "lint rodou limpo" — e era essa
	// indistinção que fazia o aviso do fallback rolar despercebido no M1.
	if strings.Contains(stdout, "golangci-lint") {
		t.Errorf("a recusa saiu no stdout; ela pertence ao stderr\nstdout: %q", stdout)
	}

	// O fallback não pode voltar por nenhum caminho. `go vet` cobre um
	// subconjunto de `govet`, e `govet` não responde por NENHUM dos 390
	// apontamentos da linha de base: trocá-lo pelo lint é trocar o gate por
	// uma checagem que hoje não acusa nada.
	if strings.Contains(stdout+stderr, "go vet") {
		t.Errorf("o alvo voltou a cair para `go vet` — é a metade da cegueira do débito 53 que a Task 0b fechou\nstdout: %q\nstderr: %q", stdout, stderr)
	}
}

// O alvo tem de RODAR o linter quando ele existe, e rodar com os DOIS tetos
// desligados. A duplicação dos flags com o `issues:` do .golangci.yml é
// deliberada e comprovadamente load-bearing: repondo os tetos no arquivo de
// configuração, `golangci-lint run` cru cai de 390 para 114 e `make lint`
// continua devolvendo 390 — quem defende são os flags da linha de comando.
// Sem este teste, apagar os dois flags também passaria verde.
func TestAlvoLintChamaOLinterComOsDoisTetosDesligados(t *testing.T) {
	dir := t.TempDir()
	argv := filepath.Join(dir, "argv.txt")

	// Dublê no lugar do binário real: o que se afirma aqui é COMO o alvo chama
	// o linter, não o que o linter acha. Rodar o golangci-lint de verdade
	// tornaria o teste dependente da máquina e caro por rodada.
	dublê := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$NISHIDB_ARGV\"\n"
	if err := os.WriteFile(filepath.Join(dir, "golangci-lint"), []byte(dublê), 0o755); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, codigo := rodaMakeLint(t, dir, "NISHIDB_ARGV="+argv)
	if codigo != 0 {
		t.Fatalf("o alvo recusou com o linter PRESENTE (exit %d)\nstdout: %q\nstderr: %q", codigo, stdout, stderr)
	}

	bruto, err := os.ReadFile(argv)
	if err != nil {
		t.Fatalf("o alvo não chegou a chamar o golangci-lint: %v", err)
	}
	chamada := strings.Fields(string(bruto))
	for _, esperado := range []string{"run", "--max-issues-per-linter=0", "--max-same-issues=0"} {
		if !contém(chamada, esperado) {
			t.Errorf("a chamada ao linter não passou %q — com o teto ligado, apontamento NOVO entra no lugar de um truncado e o total não muda\nchamada: %v", esperado, chamada)
		}
	}
}

var reInstalacao = regexp.MustCompile(`golangci-lint@([^\s"']+)`)

// A linha do `go install` do Makefile é a ÚNICA instrução de instalação do
// linter em todo o repositório, e por isso é ela que decide qual binário mede a
// linha de base. Com `@latest` a versão fica solta: o próximo desenvolvedor
// instala outra, mede outro número, e a comparação "390 antes, 390 depois"
// volta a mentir — que é literalmente o defeito que os débitos 53 e 59
// descrevem, reaberto por outra dimensão da mesma configuração.
//
// O teste amarra as duas pontas: a versão instruída é FIXA, e é a mesma que a
// documentação registra ao lado dos 390. Separadas, as duas envelhecem em
// silêncio.
func TestAVersaoDoLinterEstaFixadaEBateComALinhaDeBaseDocumentada(t *testing.T) {
	mk, err := os.ReadFile(filepath.Join(raizDoRepo, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	achado := reInstalacao.FindStringSubmatch(string(mk))
	if achado == nil {
		t.Fatal("o Makefile não instrui mais como instalar o golangci-lint; a recusa do alvo perde a saída")
	}
	versão := achado[1]
	if !strings.HasPrefix(versão, "v") {
		t.Fatalf("a instrução de instalação manda buscar %q: versão solta mede outra linha de base (débitos 53 e 59)", versão)
	}

	débitos, err := os.ReadFile(filepath.Join(raizDoRepo, "docs", "debitos.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(débitos), versão) {
		t.Fatalf("docs/debitos.md não registra a versão %s ao lado dos 390: o número da linha de base fica sem o instrumento que o produziu", versão)
	}
}

var reLinhaDeBase = regexp.MustCompile(`Medido nesta branch com os tetos desligados: \*\*(\d+)\*\*`)

// A linha de base do lint é um número que só significa alguma coisa se for O
// MESMO em todo lugar que o afirma. Ele já divergiu uma vez — os 114 truncados
// ficaram vivos na documentação depois de o instrumento ter sido corrigido, e
// foi preciso uma revisão para descobrir —, e documento de planejamento não tem
// consumidor nenhum: nada lê, nada quebra, o número envelhece em silêncio até
// a próxima revisão se apoiar nele.
//
// Este teste é o consumidor que faltava. A fonte é UMA — o débito 53, que é o
// registro canônico da medida —, e os outros três lugares que citam o número
// têm de concordar com ela.
func TestALinhaDeBaseDoLintEhAMesmaEmTodoLugarQueAAfirma(t *testing.T) {
	canônico := lê(t, "docs", "debitos.md")
	achado := reLinhaDeBase.FindStringSubmatch(canônico)
	if achado == nil {
		t.Fatal("o débito 53 não registra mais a medida da linha de base; ela é a fonte de todos os outros números")
	}
	base := achado[1]

	for _, arquivo := range [][]string{
		{"Makefile"},
		{".golangci.yml"},
		{"planning", "roadmap-m4-m5-m6.md"},
	} {
		if !strings.Contains(lê(t, arquivo...), base) {
			t.Errorf("%s não cita a linha de base de %s apontamentos que o débito 53 registra — número que diverge em silêncio é o defeito dos 114 de volta",
				filepath.Join(arquivo...), base)
		}
	}
}

func lê(t *testing.T, partes ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(append([]string{raizDoRepo}, partes...)...))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func contém(lista []string, alvo string) bool {
	for _, s := range lista {
		if s == alvo {
			return true
		}
	}
	return false
}
