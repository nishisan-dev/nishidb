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

var reNomeDeTest = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)

// reNomeDeTestPython casa o nome de um teste Python (`test_algo_assim`).
//
// A segunda grafia existe porque o client Python é código de produção com
// suíte própria, e um documento de aceite que só pudesse citar teste Go teria
// de descrever a evidência em prosa — que é exatamente a liberdade de citar o
// que não existe que este gate fecha.
//
// O `\b` antes de `test_` é o que impede um NOME DE ARQUIVO (`sql_test.py`,
// `integracao_test.py`) de ser lido como citação: ali o "test" vem precedido
// de `_`, que é caractere de palavra, e a fronteira não casa.
var reNomeDeTestPython = regexp.MustCompile(`\btest_[a-z0-9_]+`)

// reNomeDeTestJava casa a citação de um teste JUnit dos clients, e ela é
// QUALIFICADA pela classe: `NishiDatabaseMetaDataTest#metadataSemTablesDegradaSemErro`.
//
// A terceira grafia existe porque o driver JDBC é código de produção com suíte
// própria, e a arquitetura passou a citar um teste dele. As duas grafias
// anteriores se reconhecem sozinhas — `Test` na frente, `test_` na frente —, e
// um nome de método JUnit não tem prefixo nenhum: ele é camelCase, que é
// exatamente a forma de toda identificador citado em prosa técnica
// (`schemaName`, `applyRecord`, `frozenFrom`). Um regex de camelCase cru
// cobraria existência de teste para cada um deles, e um gate que não
// discrimina é pior que gate nenhum.
//
// O qualificador é o que dá ao gate uma forma para reconhecer, e ao leitor o
// arquivo em que a evidência mora. O preço, declarado: uma citação escrita SEM
// a classe continua invisível — o gate cobra a forma que ele consegue
// conferir, e não cobra que se use a forma.
var reNomeDeTestJava = regexp.MustCompile(`\b[A-Z][A-Za-z0-9]*(?:Test|IT)#[a-zA-Z][A-Za-z0-9_]*\b`)

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
		"../../docs/aceite-formato-do-groupkey.md",
		"../../docs/aceite-identificadores-em-ingles.md",
		"../../docs/aceite-schema-declarado.md",
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
		nomes := reNomeDeTest.FindAllString(string(conteudo), -1)
		nomes = append(nomes, reNomeDeTestPython.FindAllString(string(conteudo), -1)...)
		nomes = append(nomes, reNomeDeTestJava.FindAllString(string(conteudo), -1)...)
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

// reMetodoJava casa a ASSINATURA de um método JUnit, e ela é conferida só
// depois de uma anotação de teste: sem isso um helper `void arvore(...)` do
// arquivo de teste viraria oráculo de citação, e o documento poderia "citar"
// um método que não é teste nenhum.
var reMetodoJava = regexp.MustCompile(`^(?:public |private |protected )?(?:static )?void ([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

// testesOfRepositorio indexa toda função de teste declarada no repositório.
//
// O teste Go entra pelo nome (`TestXxx`), o Python pelo nome (`test_xxx`) e o
// JUnit dos clients entra QUALIFICADO pela classe (`Classe#metodo`), que é a
// forma que o documento tem de usar para ser conferido — o porquê está no
// reNomeDeTestJava.
func testesOfRepositorio(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	junit := 0
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
		// O Maven acha o teste pelo nome do ARQUIVO (`*Test.java` e `*IT.java`
		// no surefire/failsafe deste pom), então é o mesmo critério que indexa
		// aqui: um arquivo que o Maven não roda não é evidência de nada.
		ehJava := strings.HasSuffix(d.Name(), "Test.java") || strings.HasSuffix(d.Name(), "IT.java")
		if !ehGo && !ehPython && !ehJava {
			return nil
		}
		conteudo, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		classeJava := strings.TrimSuffix(d.Name(), ".java")
		anotado := false
		for _, row := range strings.Split(string(conteudo), "\n") {
			if ehJava {
				corpo := strings.TrimSpace(row)
				if strings.HasPrefix(corpo, "@Test") || strings.HasPrefix(corpo, "@ParameterizedTest") {
					anotado = true
					continue
				}
				if !anotado {
					continue
				}
				if m := reMetodoJava.FindStringSubmatch(corpo); m != nil {
					out[classeJava+"#"+m[1]] = true
					junit++
					anotado = false
				}
				continue
			}
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
	// O piso do JUnit é separado porque a suíte Go sozinha passa folgado do
	// primeiro: sem ele, uma quebra na leitura do Java (a anotação que muda de
	// nome, o método que passa a caber numa linha só) faria o índice esvaziar
	// em silêncio, e toda citação Java viraria "existe" por vacuidade. O
	// número é uma ordem de grandeza abaixo da suíte do driver, que hoje passa
	// de duzentos casos.
	if junit < 100 {
		t.Fatalf("só %d testes JUnit indexados: a leitura dos clients quebrou", junit)
	}
	return out
}
