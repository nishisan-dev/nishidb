package gates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// caminhoDoDocDeMetricas é a referência que esta suíte cobra.
const caminhoDoDocDeMetricas = "docs/metricas.md"

// seriesDeclaradas extrai as constantes Met* de internal/metrics/nomes.go pela
// AST, e não por grep: o nome da série é DADO no código, e ler prosa para
// descobri-lo é a classe de defeito que a doutrina de verificação nomeia.
func seriesDeclaradas(t *testing.T) map[string]string {
	t.Helper()
	caminho := filepath.Join(raizDoRepo, "internal/metrics/nomes.go")
	fset := token.NewFileSet()
	arquivo, err := parser.ParseFile(fset, caminho, nil, 0)
	if err != nil {
		t.Fatalf("parseando %s: %v", caminho, err)
	}
	out := map[string]string{}
	for _, decl := range arquivo.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, nome := range vs.Names {
				if !strings.HasPrefix(nome.Name, "Met") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				out[nome.Name] = strings.Trim(lit.Value, `"`)
			}
		}
	}
	return out
}

// TestTodaMetricaDeclaradaEstaDocumentada: o inventário de docs/metricas.md
// cobre TODA série que o código declara.
//
// Sem ele o documento envelhece calado: a série nova entra em nomes.go, sai no
// /metrics, e a página que promete ser o inventário deixa de sê-lo sem que
// nada acuse. É a mesma cobrança que os gates dos clients fazem sobre os
// códigos do protocolo.
func TestTodaMetricaDeclaradaEstaDocumentada(t *testing.T) {
	series := seriesDeclaradas(t)
	if len(series) < 50 {
		t.Fatalf("só %d séries extraídas de nomes.go: a extração quebrou, e um gate "+
			"que não acha nada passa sempre", len(series))
	}
	doc := leArquivo(t, caminhoDoDocDeMetricas)
	for nomeGo, serie := range series {
		// Casa a série DELIMITADA: sem isso, `nishidb_query_total` seria dada
		// por documentada pela linha de `nishidb_query_total_bucket`, e a
		// família que faltasse passaria escondida atrás da vizinha.
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(serie) + `\b`).MatchString(doc) {
			t.Errorf("%s (%s) não está em %s: acrescente a linha na tabela do subsistema",
				nomeGo, serie, caminhoDoDocDeMetricas)
		}
	}
}

// TestODocDeMetricasNaoInventaSerie é o lado inverso, e o que impede a página
// de mentir: toda `nishidb_*` citada nela tem de existir em nomes.go.
//
// Uma série renomeada no código deixaria o nome velho no documento, e o
// operador escreveria o alerta contra algo que não existe mais — a falha
// silenciosa que só aparece no dia do incidente.
func TestODocDeMetricasNaoInventaSerie(t *testing.T) {
	series := seriesDeclaradas(t)
	existe := map[string]bool{}
	for _, s := range series {
		existe[s] = true
		// Os três sufixos que o histograma expande no texto Prometheus.
		for _, sufixo := range []string{"_bucket", "_sum", "_count"} {
			existe[s+sufixo] = true
		}
	}
	doc := leArquivo(t, caminhoDoDocDeMetricas)
	citadas := regexp.MustCompile(`nishidb_[a-z0-9_]+`).FindAllString(doc, -1)
	if len(citadas) < 50 {
		t.Fatalf("só %d citações de série em %s: a extração quebrou",
			len(citadas), caminhoDoDocDeMetricas)
	}
	// `nishidb_history_*` é a forma legítima de citar uma FAMÍLIA inteira na
	// prosa. O prefixo não é ignorado por isso: ele tem de casar com pelo
	// menos uma série declarada, senão um `nishidb_inexistente_*` passaria
	// escondido atrás da convenção.
	comAsterisco := map[string]bool{}
	for _, m := range regexp.MustCompile(`nishidb_[a-z0-9_]+\*`).FindAllString(doc, -1) {
		comAsterisco[strings.TrimSuffix(m, "*")] = true
	}
	for prefixo := range comAsterisco {
		algum := false
		for s := range existe {
			if strings.HasPrefix(s, prefixo) {
				algum = true
				break
			}
		}
		if !algum {
			t.Errorf("%s* é citada em %s e nenhuma série declarada começa assim",
				prefixo, caminhoDoDocDeMetricas)
		}
	}

	vistas := map[string]bool{}
	for _, c := range citadas {
		if vistas[c] || existe[c] || comAsterisco[c] {
			continue
		}
		vistas[c] = true
		t.Errorf("%s é citada em %s e NÃO existe em internal/metrics/nomes.go: "+
			"a série foi renomeada ou o nome está errado", c, caminhoDoDocDeMetricas)
	}
}
