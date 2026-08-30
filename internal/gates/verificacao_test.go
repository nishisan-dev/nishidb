package gates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Este arquivo fecha duas classes de defeito que o ciclo do índice por
// segmento pagou caro, e que nenhuma ferramenta acusava porque as duas vivem
// em texto sem compilador: citação de spec que não resolve, e amostra de
// métrica sem família.
//
// A doutrina que os sustenta é a mesma dos outros gates deste pacote: ler o
// documento/código como DADO, comparar duas extrações, e falhar alto nomeando
// o que está errado — nunca SKIP silencioso.

// reCitacao casa uma citação de spec: §NN ou §NN.M.
var reCitacao = regexp.MustCompile(`§(\d+(?:\.\d+)?)`)

// reCabecalho casa um cabeçalho numerado de spec: "# 14.", "## 1.7", "### 3.2".
var reCabecalho = regexp.MustCompile(`^#{1,4}\s+(\d+(?:\.\d+)?)\.?\s`)

// reCabecalhoQualquer casa QUALQUER cabeçalho (numerado ou não), para encerrar
// a seção corrente: um divisor como "## Core" não pode deixar os itens
// seguintes atribuídos à seção anterior.
var reCabecalhoQualquer = regexp.MustCompile(`^#{1,4}\s`)

// reItemNumerado casa um item de lista numerado ("1. iniciar...").
var reItemNumerado = regexp.MustCompile(`^(\d+)\.\s`)

// reItemMarcador casa um item de lista com marcador ("* Consumir...").
var reItemMarcador = regexp.MustCompile(`^\s*[*\-]\s`)

// TestCitacoesDeSecaoResolvem confere que toda citação §NN/§NN.M em docs,
// README, planning e comentários Go resolve em pelo menos uma seção ou item
// numerado de uma spec.
//
// O formato §NN.M referencia DOIS níveis: um cabeçalho "N.M" (ex.: "### 1.5
// Corrupção") OU o item M dentro da seção N (ex.: o item 15 da lista do §126,
// citado como §126.15). O gate coleta os dois níveis de nishidb-spec-proposal.md
// e de planning/*.md, e reprova a citação que não resolve em nenhum deles.
//
// Ele NÃO julga se a citação aponta para o lugar certo — isso é trabalho de
// revisão. Ele fecha a classe barata: citar uma seção que não existe, que é o
// defeito que um revisor lendo prosa deixa passar e que um compilador pegaria.
func TestCitacoesDeSecaoResolvem(t *testing.T) {
	alvos := secoesDasSpecs(t)

	// Fontes de citação: docs (recursivo), README, planning e comentários Go.
	var fontes []string
	adiciona := func(caminho string) { fontes = append(fontes, caminho) }

	adiciona(filepath.Join(raizDoRepo, "README.md"))
	err := filepath.WalkDir(filepath.Join(raizDoRepo, "docs"), func(c string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(c, ".md") {
			adiciona(c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	entradas, err := os.ReadDir(filepath.Join(raizDoRepo, "planning"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entradas {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			adiciona(filepath.Join(raizDoRepo, "planning", e.Name()))
		}
	}
	err = filepath.WalkDir(raizDoRepo, func(c string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if nome := d.Name(); nome == ".git" || nome == ".claude" {
				return fs.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(c, ".go") {
			adiciona(c)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	orfas := map[string]bool{}
	for _, f := range fontes {
		conteudo, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range reCitacao.FindAllStringSubmatch(string(conteudo), -1) {
			if !alvos[m[1]] && !orfas[m[1]] {
				orfas[m[1]] = true
				t.Errorf("%s cita §%s, que não resolve em nenhuma spec",
					filepath.Base(f), m[1])
			}
		}
	}
}

// secoesDasSpecs indexa toda seção e item numerado das specs: a spec raiz e
// os documentos de planning.
func secoesDasSpecs(t *testing.T) map[string]bool {
	t.Helper()
	alvos := map[string]bool{}

	registra := func(caminho string) {
		conteudo, err := os.ReadFile(caminho)
		if err != nil {
			t.Fatal(err)
		}
		secao := ""
		marcador := 0
		for _, linha := range strings.Split(string(conteudo), "\n") {
			if reCabecalhoQualquer.MatchString(linha) {
				if m := reCabecalho.FindStringSubmatch(linha); m != nil {
					secao = m[1]
					alvos[secao] = true
				} else {
					// Divisor sem número ("## Core"): encerra a seção.
					secao = ""
				}
				marcador = 0
				continue
			}
			if secao == "" {
				continue
			}
			if m := reItemNumerado.FindStringSubmatch(linha); m != nil {
				// Item numerado usa o número explícito, independente dos
				// marcadores: §126.15 é o item 15 da lista do §126.
				alvos[secao+"."+m[1]] = true
				continue
			}
			if reItemMarcador.MatchString(linha) {
				// Item com marcador usa a posição entre os marcadores da
				// seção: §105.1..§105.8 são os oito itens do §105.
				marcador++
				alvos[secao+"."+strconv.Itoa(marcador)] = true
			}
		}
	}

	registra(filepath.Join(raizDoRepo, "nishidb-spec-proposal.md"))
	entradas, err := os.ReadDir(filepath.Join(raizDoRepo, "planning"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entradas {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			registra(filepath.Join(raizDoRepo, "planning", e.Name()))
		}
	}
	return alvos
}

// TestAmostrasDeMetricaTemFamiliaENome confere que todo literal
// metrics.Amostra{...} em código de produção declara Familia e Nome por
// CONSTANTE (nunca string literal, nunca vazio).
//
// O defeito que ele fecha é concreto: as amostras do coletor Kafka nasciam sem
// Familia, e o `# HELP`/`# TYPE` saía com nome vazio — o Prometheus recusa o
// scrape inteiro, não só a linha. Nenhum teste acusava, porque o formato de
// exposição só é exercitado com amostras bem-formadas.
//
// A regra "não pode ser string literal" é a mesma doutrina de
// internal/metrics/nomes.go: nome de métrica digitado à mão em dois lugares
// diverge no dia em que um deles é corrigido, e o sintoma é um painel vazio.
func TestAmostrasDeMetricaTemFamiliaENome(t *testing.T) {
	raiz := filepath.Join(raizDoRepo, "internal")
	err := filepath.WalkDir(raiz, func(caminho string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(caminho, ".go") || strings.HasSuffix(caminho, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, caminho, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		pkg := f.Name.Name
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || !ehAmostra(cl, pkg) {
				return true
			}
			pos := fset.Position(cl.Pos())
			for _, nome := range []string{"Familia", "Nome"} {
				valor, presente := campoDoLiteral(cl, nome)
				if !presente {
					t.Errorf("%s:%d: Amostra sem o campo %s — o # HELP/# TYPE sai vazio",
						caminho, pos.Line, nome)
					continue
				}
				if ehStringLiteral(valor) {
					t.Errorf("%s:%d: Amostra com %s como string literal — use a constante de internal/metrics/nomes.go",
						caminho, pos.Line, nome)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// ehAmostra reconhece um literal do tipo metrics.Amostra, qualificado
// (metrics.Amostra{...}) ou não (Amostra{...} dentro do pacote metrics).
func ehAmostra(cl *ast.CompositeLit, pkg string) bool {
	switch t := cl.Type.(type) {
	case *ast.SelectorExpr:
		if t.Sel.Name != "Amostra" {
			return false
		}
		id, ok := t.X.(*ast.Ident)
		return ok && id.Name == "metrics"
	case *ast.Ident:
		return t.Name == "Amostra" && pkg == "metrics"
	}
	return false
}

// campoDoLiteral devolve o valor do campo nomeado num literal chave:valor.
func campoDoLiteral(cl *ast.CompositeLit, nome string) (ast.Expr, bool) {
	for _, el := range cl.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		id, ok := kv.Key.(*ast.Ident)
		if !ok || id.Name != nome {
			continue
		}
		return kv.Value, true
	}
	return nil, false
}

// ehStringLiteral reconhece uma string literal (o que a regra proíbe).
func ehStringLiteral(e ast.Expr) bool {
	bl, ok := e.(*ast.BasicLit)
	return ok && bl.Kind == token.STRING
}
