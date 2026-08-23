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

var reNomeDeTeste = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)

func TestEvidenciasCitadasNosDocumentosExistem(t *testing.T) {
	docs := []string{"../../docs/aceite-m1.md", "../../docs/aceite-m2.md", "../../docs/aceite-fase6.md",
		"../../docs/aceite-m4.md", "../../docs/aceite-m5.md",
		"../../docs/architecture.md", "../../docs/debitos.md", "../../docs/operacao-kafka.md", "../../README.md"}

	existentes := testesDoRepositorio(t)
	for _, doc := range docs {
		conteudo, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		vistos := map[string]bool{}
		for _, nome := range reNomeDeTeste.FindAllString(string(conteudo), -1) {
			if vistos[nome] {
				continue
			}
			vistos[nome] = true
			if !existentes[nome] {
				t.Errorf("%s cita %s, que não existe na suíte", filepath.Base(doc), nome)
			}
		}
	}
}

// testesDoRepositorio indexa toda função de teste declarada no repositório.
func testesDoRepositorio(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	raiz := "../.."
	err := filepath.WalkDir(raiz, func(caminho string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// .git e as worktrees não têm código nosso e são grandes.
			if nome := d.Name(); nome == ".git" || nome == ".claude" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		conteudo, err := os.ReadFile(caminho)
		if err != nil {
			return err
		}
		for _, linha := range strings.Split(string(conteudo), "\n") {
			if !strings.HasPrefix(linha, "func Test") {
				continue
			}
			nome := strings.TrimPrefix(linha, "func ")
			if i := strings.IndexByte(nome, '('); i > 0 {
				out[nome[:i]] = true
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
