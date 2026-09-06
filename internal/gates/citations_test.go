// Package gates — este arquivo confere as CITAÇÕES DE TESTE dentro dos
// comentários do próprio código.
package gates

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// reTestName casa uma citação de teste em prosa — o identificador começando por
// Test com maiúscula em seguida.
var reTestName = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)

// segmentIndexFiles é o ESCOPO deste gate, e a lista é explícita de
// propósito. Ver o godoc abaixo.
var segmentIndexFiles = []string{
	"internal/metrics/names.go",
	"internal/protocol/documents.go",
	"internal/server/history.go",
	"internal/server/metrics_index.go",
	"internal/server/metrics_index_test.go",
	"internal/storage/indices_enum.go",
	"internal/storage/indices_window.go",
	"internal/storage/indices_metrics.go",
	"internal/storage/indices_metrics_test.go",
	"internal/storage/indices_rebuild.go",
	"internal/storage/store.go",
}

// TestAsCitacoesDeTesteNosComentariosExistem é o irmão de
// TestEvidenciasCitadasNosDocumentosExistem para o lado do CÓDIGO.
//
// Aquele confere os testes citados nos .md; este confere os citados em godoc e
// comentários. A lacuna é real e mordeu duas vezes no mesmo ciclo: um teste
// renomeado deixa para trás um ponteiro que aponta para nada, e um comentário que
// diz "o FATO está provado em X" com X inexistente é pior que comentário nenhum —
// ele afirma cobertura que ninguém pode conferir.
//
// # O ESCOPO É UMA LISTA, E ISSO É UM LIMITE, NÃO UM DESENHO
//
// Este gate cobre os arquivos do índice por segmento, e só eles. Rodado sobre o
// repositório inteiro ele acusa mais de vinte citações mortas em outros pacotes —
// dívida pré-existente, herdada de renomeações antigas, e limpá-la é trabalho de
// outra tarefa, com outros donos. Escrever o limite aqui é preferível a um gate
// que não roda ou a uma limpeza feita de passagem em código alheio.
//
// A segunda limitação: uma citação a teste de OUTRO pacote é aceita se o nome
// existir em qualquer arquivo varrido pela árvore, porque resolver o pacote
// exigiria carregar o repositório inteiro. É suficiente para o modo de falha que
// importa, que é o rename.
func TestAsCitacoesDeTesteNosComentariosExistem(t *testing.T) {
	root := filepath.Join("..", "..")
	inScope := map[string]bool{}
	for _, f := range segmentIndexFiles {
		inScope[filepath.Join(root, filepath.FromSlash(f))] = true
	}
	existing := map[string]bool{}
	type citation struct{ file, name string }
	var cited []citation

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// worktrees são cópias da árvore; varrê-las duplicaria tudo.
			if name := d.Name(); name == ".git" || name == ".claude" || name == "clients" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil // arquivo que não parseia é problema de outro gate
		}
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && strings.HasPrefix(fn.Name.Name, "Test") {
				existing[fn.Name.Name] = true
			}
		}
		if !inScope[path] {
			return nil
		}
		for _, group := range f.Comments {
			for _, name := range reTestName.FindAllString(group.Text(), -1) {
				cited = append(cited, citation{path, name})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("varredura: %v", err)
	}
	if len(cited) == 0 {
		t.Fatalf("nenhuma citação de teste nos %d arquivos do escopo: ou a lista ficou obsoleta, ou o gate passaria "+
			"por vacuidade", len(segmentIndexFiles))
	}
	for f := range inScope {
		if _, err := os.Stat(f); err != nil {
			t.Errorf("o escopo lista %s, que não existe: um arquivo renomeado tira o gate de cima dele em silêncio", f)
		}
	}

	dead := map[string][]string{}
	for _, c := range cited {
		if !existing[c.name] {
			dead[c.file] = append(dead[c.file], c.name)
		}
	}
	if len(dead) == 0 {
		return
	}
	files := make([]string, 0, len(dead))
	for a := range dead {
		files = append(files, a)
	}
	sort.Strings(files)
	for _, a := range files {
		names := dead[a]
		sort.Strings(names)
		t.Errorf("%s cita teste(s) que não existem: %s — um rename deixou o ponteiro para trás, e um comentário que "+
			"afirma cobertura inexistente é pior que comentário nenhum", a, strings.Join(names, ", "))
	}
}
