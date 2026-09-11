package cli_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// O documento de aceite cita testes como evidência de cada critério. Duas
// dessas citações apontavam para testes que não provavam o item — uma delas
// para um teste de outro assunto, outra para um encerramento LIMPO num critério
// sobre `kill -9`. Nenhuma ferramenta acusou, porque evidência em markdown não
// tem compilador.
//
// Este teste é o compilador que faltava: todo identificador TestXxx citado nos
// documentos precisa existir na suíte. Ele não julga se o teste prova o
// critério — isso é trabalho de revisão —, mas fecha a classe mais barata de
// defeito, que é citar o que não existe.
//
// Mora aqui, junto do teste que executa o README, porque este pacote é onde a
// documentação é tratada como código.

var reNameOfTest = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)

// reNameOfTestPython casa o nome de um teste Python (`test_algo_assim`).
//
// A segunda grafia existe porque o client Python é código de produção com
// suíte própria, e um documento de aceite que só pudesse citar teste Go teria
// de descrever a evidência em prosa — que é exatamente a liberdade de citar o
// que não existe que este gate fecha.
//
// O `\b` antes de `test_` é o que impede um NOME DE ARQUIVO (`sql_test.py`,
// `integracao_test.py`) de ser lido como citação: ali o "test" vem precedido
// de `_`, que é caractere de palavra, e a fronteira não casa.
var reNameOfTestPython = regexp.MustCompile(`\btest_[a-z0-9_]+`)

func TestEvidenciasCitadasNosDocumentosExistem(t *testing.T) {
	// mutacoes-indice-por-segmento.md entra aqui porque ele é FEITO de citações:
	// o documento inteiro é uma tabela de "mutação → teste que reprovou →
	// mensagem", e um nome de teste errado ali não é imprecisão de prosa, é a
	// afirmação de cobertura que ninguém pode conferir. Cinco dos nomes que o
	// diário do ciclo registrou já não existiam quando ele foi escrito, todos por
	// rename de tarefas posteriores — a classe exata que este gate fecha.
	//
	// O CLAUDE.md entra porque ele passou a ser VERSIONADO e é prosa normativa:
	// ele cita cinco testes como prova das convenções que impõe, e um nome
	// obsoleto ali manda o leitor seguinte procurar cobertura que não existe.
	// Enquanto ficou de fora, era a única prosa normativa do repositório sem
	// compilador.
	docs := []string{"../../docs/aceite-m1.md", "../../docs/aceite-m2.md", "../../docs/aceite-fase6.md",
		"../../docs/aceite-m4.md", "../../docs/aceite-m5.md", "../../docs/aceite-client-python.md",
		"../../docs/aceite-indice-por-segmento.md", "../../docs/aceite-contadores.md",
		"../../docs/architecture.md", "../../docs/debitos.md", "../../docs/operacao-kafka.md",
		"../../docs/metricas.md",
		"../../docs/mutacoes-indice-por-segmento.md",
		"../../docs/doutrina-de-verificacao.md", "../../README.md", "../../CLAUDE.md"}

	existentes := testesOfRepositorio(t)
	for _, doc := range docs {
		conteudo, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		vistos := map[string]bool{}
		nomes := reNameOfTest.FindAllString(string(conteudo), -1)
		nomes = append(nomes, reNameOfTestPython.FindAllString(string(conteudo), -1)...)
		for _, name := range nomes {
			if vistos[name] {
				continue
			}
			vistos[name] = true
			if !existentes[name] {
				t.Errorf("%s cita %s, que não existe na suíte", filepath.Base(doc), name)
			}
		}
	}
}

// testesOfRepositorio indexa toda função de teste declarada no repositório.
func testesOfRepositorio(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	raiz := "../.."
	err := filepath.WalkDir(raiz, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// .git e as worktrees não têm código nosso e são grandes;
			// target é a saída do build do driver JDBC, que o Maven enche de
			// classes e de relatórios. Nenhum dos três tem *_test.go, então
			// pular é economia de varredura — mas é também o que impede um
			// arquivo COPIADO para dentro de target de virar oráculo: o
			// documento poderia citar um teste que não existe mais na fonte.
			if name := d.Name(); name == ".git" || name == ".claude" || name == "target" {
				return fs.SkipDir
			}
			return nil
		}
		ehGo := strings.HasSuffix(d.Name(), "_test.go")
		ehPython := strings.HasSuffix(d.Name(), "_test.py")
		if !ehGo && !ehPython {
			return nil
		}
		conteudo, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, row := range strings.Split(string(conteudo), "\n") {
			if ehGo {
				if !strings.HasPrefix(row, "func Test") {
					continue
				}
				name := strings.TrimPrefix(row, "func ")
				if i := strings.IndexByte(name, '('); i > 0 {
					out[name[:i]] = true
				}
				continue
			}
			// Python: o método é indentado dentro da classe de teste, então a
			// comparação é sobre a linha aparada.
			corpo := strings.TrimSpace(row)
			if !strings.HasPrefix(corpo, "def test_") {
				continue
			}
			name := strings.TrimPrefix(corpo, "def ")
			if i := strings.IndexByte(name, '('); i > 0 {
				out[name[:i]] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 100 {
		t.Fatalf("só %d testes indexados: a varredura não achou a suíte", len(out))
	}
	return out
}
