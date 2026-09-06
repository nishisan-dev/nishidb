package gates_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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

const repoRoot = "../.."

// locateMake resolve o binário antes de mexer no PATH do filho — o PATH que
// os testes montam existe para controlar o que a RECEITA enxerga, não para
// esconder o `make` do próprio teste.
//
// Ausência de `make` é FALHA, não SKIP: pular aqui repetiria, no teste, o
// defeito que ele fecha no alvo — sumir junto com a ferramenta e ainda assim
// devolver verde. `make` é o ponto de entrada documentado do projeto.
func locateMake(t *testing.T) string {
	t.Helper()
	mk, err := exec.LookPath("make")
	if err != nil {
		t.Fatalf("make não está no PATH e é o ponto de entrada de todos os gates do projeto: %v", err)
	}
	return mk
}

// envWithPATH devolve o ambiente atual com o PATH SUBSTITUÍDO (não
// acrescentado): o teste precisa do controle total sobre o que `command -v`
// acha, senão o golangci-lint da máquina de quem roda decide o resultado.
func envWithPATH(path string, extras ...string) []string {
	current := os.Environ()
	env := make([]string, 0, len(current)+1+len(extras))
	for _, kv := range current {
		if strings.HasPrefix(kv, "PATH=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "PATH="+path)
	return append(env, extras...)
}

// runMakeLint executa o alvo na raiz do repositório e devolve stdout, stderr e
// o código de saída. O código do `make` é 2 quando a receita falha (ele reporta
// "Error 1" do shell e sai 2, que é a convenção do GNU make): o que o gate
// promete é NÃO-ZERO, e é isso que os testes afirmam.
func runMakeLint(t *testing.T, path string, extras ...string) (stdout, stderr string, code int) {
	t.Helper()
	cmd := exec.Command(locateMake(t), "lint")
	cmd.Dir = repoRoot
	cmd.Env = envWithPATH(path, extras...)
	var out, errBuf strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("make lint não chegou a executar: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return out.String(), errBuf.String(), code
}

// Este é o teste do defeito: com o golangci-lint fora do PATH, o alvo tem de
// RECUSAR, não passar. O PATH do filho é um diretório temporário VAZIO — mais
// forte que "tirar um diretório da lista", porque não depende de onde a
// ferramenta está instalada nesta máquina.
func TestAlvoLintRecusaQuandoOGolangciLintNaoEstaNoPATH(t *testing.T) {
	stdout, stderr, code := runMakeLint(t, t.TempDir())

	if code == 0 {
		t.Fatalf("o alvo saiu 0 com o golangci-lint AUSENTE — é o carimbo do débito 53 de volta\nstdout: %q\nstderr: %q", stdout, stderr)
	}

	// A mensagem tem de bastar para o operador se desbloquear sozinho: o que
	// falta, como instalar e onde o binário cai. Sem isso o exit não-zero vira
	// um enigma, que é a outra forma de o gate não ser gate.
	for _, snippet := range []string{"golangci-lint", "NÃO rodou", "go install", "PATH"} {
		if !strings.Contains(stderr, snippet) {
			t.Errorf("o stderr do alvo não menciona %q — a recusa precisa nomear o que falta\nstderr: %q", snippet, stderr)
		}
	}

	// O canal importa. Com a mensagem no stdout, `make lint 2>/dev/null` não
	// distingue "ferramenta ausente" de "lint rodou limpo" — e era essa
	// indistinção que fazia o aviso do fallback rolar despercebido no M1.
	if strings.Contains(stdout, "golangci-lint") {
		t.Errorf("a recusa saiu no stdout; ela pertence ao stderr\nstdout: %q", stdout)
	}

	// O fallback não pode voltar por nenhum caminho. `go vet` cobre um
	// subconjunto de `govet`, e `govet` não responde por NENHUM dos
	// apontamentos da linha de base: trocá-lo pelo lint é trocar o gate por
	// uma checagem que hoje não acusa nada.
	if strings.Contains(stdout+stderr, "go vet") {
		t.Errorf("o alvo voltou a cair para `go vet` — é a metade da cegueira do débito 53 que a Task 0b fechou\nstdout: %q\nstderr: %q", stdout, stderr)
	}
}

// O alvo tem de RODAR o linter quando ele existe, e rodar com os DOIS tetos
// desligados. A duplicação dos flags com o `issues:` do .golangci.yml é
// deliberada e comprovadamente load-bearing: repondo os tetos no arquivo de
// configuração, `golangci-lint run` cru cai para os 114 truncados e `make lint`
// continua devolvendo a linha de base inteira — quem defende são os flags da
// linha de comando.
// Sem este teste, apagar os dois flags também passaria verde.
func TestAlvoLintChamaOLinterComOsDoisTetosDesligados(t *testing.T) {
	dir := t.TempDir()
	argv := filepath.Join(dir, "argv.txt")

	// Dublê no lugar do binário real: o que se afirma aqui é COMO o alvo chama
	// o linter, não o que o linter acha. Rodar o golangci-lint de verdade
	// tornaria o teste dependente da máquina e caro por rodada.
	fake := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$NISHIDB_ARGV\"\n"
	if err := os.WriteFile(filepath.Join(dir, "golangci-lint"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runMakeLint(t, dir, "NISHIDB_ARGV="+argv)
	if code != 0 {
		t.Fatalf("o alvo recusou com o linter PRESENTE (exit %d)\nstdout: %q\nstderr: %q", code, stdout, stderr)
	}

	raw, err := os.ReadFile(argv)
	if err != nil {
		t.Fatalf("o alvo não chegou a chamar o golangci-lint: %v", err)
	}
	call := strings.Fields(string(raw))
	for _, want := range []string{"run", "--max-issues-per-linter=0", "--max-same-issues=0"} {
		if !contains(call, want) {
			t.Errorf("a chamada ao linter não passou %q — com o teto ligado, apontamento NOVO entra no lugar de um truncado e o total não muda\nchamada: %v", want, call)
		}
	}
}

var reInstallation = regexp.MustCompile(`golangci-lint@([^\s"']+)`)

// A linha do `go install` do Makefile é a ÚNICA instrução de instalação do
// linter no repositório, e por isso é ela que decide qual binário mede a linha
// de base. Com `@latest` a versão fica solta: o próximo desenvolvedor instala
// outra, mede outro número, e a comparação "mesmo número antes e depois" volta
// a mentir — o defeito dos débitos 53 e 59 reaberto por outra dimensão.
//
// O teste afirma só o que é CONFIGURAÇÃO: a versão instruída é fixa. Não
// afirma nada sobre o texto da documentação — número citado em prosa não é
// gate, e amarrar teste a substring de markdown quebra em toda edição legítima
// sem pegar defeito nenhum.
func TestAInstrucaoDeInstalacaoFixaAVersaoDoLinter(t *testing.T) {
	mk, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	match := reInstallation.FindStringSubmatch(string(mk))
	if match == nil {
		t.Fatal("o Makefile não instrui mais como instalar o golangci-lint; a recusa do alvo perde a saída")
	}
	if version := match[1]; !strings.HasPrefix(version, "v") {
		t.Fatalf("a instrução manda buscar %q: versão solta mede outra linha de base (débitos 53 e 59)", version)
	}
}

// O alvo tem de PROPAGAR o veredito do linter, e não só chamá-lo. É o mesmo
// defeito da recusa ausente, um ramo do `if` ao lado: um `|| true` na linha do
// golangci-lint faz `make lint` sair 0 com a linha de base inteira na tela, e a
// tese da doutrina ("gate que passa não é gate, é carimbo") volta a valer
// contra o próprio alvo que a corrigiu.
func TestAlvoLintPropagaOVeredictoDoLinter(t *testing.T) {
	dir := t.TempDir()
	// Dublê que REPROVA: é o veredito do linter que precisa atravessar o
	// alvo, e o conteúdo do apontamento não interessa aqui.
	fake := "#!/bin/sh\necho 'algum.go:1:1: apontamento' >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(dir, "golangci-lint"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := runMakeLint(t, dir)
	if code == 0 {
		t.Fatalf("o linter reprovou e `make lint` saiu 0: o alvo engole o veredito\nstdout: %q\nstderr: %q", stdout, stderr)
	}
}

// linterConfig lê o .golangci.yml como DADO, não como texto. A distinção é o
// que separa este teste dos que foram removidos daqui: configuração é lida por
// máquina e tem consumidor real (o próprio golangci-lint), enquanto número
// citado em prosa não tem.
type linterConfig struct {
	Linters struct {
		Enable []string `yaml:"enable"`
	} `yaml:"linters"`
	Issues struct {
		MaxIssuesPerLinter *int `yaml:"max-issues-per-linter"`
		MaxSameIssues      *int `yaml:"max-same-issues"`
	} `yaml:"issues"`
}

// Metade do gate mora no `.golangci.yml`, e era a metade que nada fixava:
// devolver os tetos a 50/3 — ou apagar o bloco `issues:` — faz `golangci-lint
// run` cru voltar a reportar 114 truncados, que é o defeito dos débitos 53 e
// 59 restaurado em silêncio, com a suíte inteira verde.
//
// O conjunto de linters entra pela mesma razão: sem `errcheck` e `revive` a
// linha de base desaba para uma fração do que é — os dois respondem pela quase
// totalidade dela —, e a comparação "mesmo número antes e depois" passaria a
// comparar outro instrumento.
func TestConfiguracaoDoLinterMantemOsTetosEmZeroEOsLintersQueMedemALinhaDeBase(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot, ".golangci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var cfg linterConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf(".golangci.yml não é YAML válido: %v", err)
	}
	for _, ceiling := range []struct {
		key   string
		value *int
	}{
		{"issues.max-issues-per-linter", cfg.Issues.MaxIssuesPerLinter},
		{"issues.max-same-issues", cfg.Issues.MaxSameIssues},
	} {
		if ceiling.value == nil {
			t.Errorf("%s ausente: sem ela o golangci-lint trunca no default e o apontamento novo entra no lugar de um descartado", ceiling.key)
			continue
		}
		if *ceiling.value != 0 {
			t.Errorf("%s = %d, queria 0: teto ligado é o gate cego dos débitos 53 e 59", ceiling.key, *ceiling.value)
		}
	}
	for _, linter := range []string{"errcheck", "revive", "staticcheck", "govet"} {
		if !contains(cfg.Linters.Enable, linter) {
			t.Errorf("%s não está em linters.enable: a linha de base foi medida com ele, e sem ele o número passa a descrever outro instrumento", linter)
		}
	}
}

func contains(list []string, item string) bool {
	for _, s := range list {
		if s == item {
			return true
		}
	}
	return false
}
