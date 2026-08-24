package gates_test

import (
	"encoding/xml"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// O pom do driver, lido como DADO — a mesma doutrina de ci_test.go com os
// workflows.
//
// Estes gates vivem em Go, e não em Maven, por uma razão prática: o enforcer do
// pom só reprova quem lembra de rodar `mvnw`, enquanto `go test ./...` roda em
// todo commit e no CI. Um pom alterado sem build Java chegaria ao main sem
// nada acusar.
type pomDoDriver struct {
	ArtifactID string `xml:"artifactId"`
	Properties struct {
		Release string `xml:"maven.compiler.release"`
	} `xml:"properties"`
	Dependencies struct {
		Dependency []struct {
			GroupID    string `xml:"groupId"`
			ArtifactID string `xml:"artifactId"`
			Scope      string `xml:"scope"`
		} `xml:"dependency"`
	} `xml:"dependencies"`
	Build struct {
		FinalName string `xml:"finalName"`
	} `xml:"build"`
}

const caminhoDoPom = "clients/jdbc/pom.xml"

func lePom(t *testing.T) pomDoDriver {
	t.Helper()
	bruto, err := os.ReadFile(filepath.Join(raizDoRepo, caminhoDoPom))
	if err != nil {
		t.Fatalf("lendo %s: %v", caminhoDoPom, err)
	}
	var p pomDoDriver
	if err := xml.Unmarshal(bruto, &p); err != nil {
		t.Fatalf("%s não é XML válido: %v", caminhoDoPom, err)
	}
	return p
}

// "Zero dependência de runtime" é a doutrina do binário único traduzida para um
// jar, e este é o gate que a sustenta.
//
// O que ela promete é sobre o classpath de QUEM CONSOME o driver: um jar que
// arraste dependências obriga quem o usa a resolvê-las, e é assim que um driver
// vira transitive hell numa aplicação que só queria consultar um banco.
// scope=test nunca entra nesse classpath; qualquer outro escopo entra.
func TestOPomDoDriverNaoTemDependenciaDeRuntime(t *testing.T) {
	p := lePom(t)
	for _, d := range p.Dependencies.Dependency {
		if d.Scope != "test" {
			t.Errorf("%s:%s tem escopo %q: o driver não pode ter dependência de runtime, "+
				"e só <scope>test</scope> fica fora do classpath de quem consome o jar",
				d.GroupID, d.ArtifactID, d.Scope)
		}
	}
}

// O artefato é literalmente o nome do §70: nishidb-jdbc.jar.
//
// Sem o finalName, o Maven emitiria nishidb-jdbc-0.1.0-SNAPSHOT.jar e toda
// instrução de classpath da documentação passaria a mentir a cada release.
func TestOArtefatoDoDriverTemONomeDoSpec70(t *testing.T) {
	p := lePom(t)
	if p.Build.FinalName != "nishidb-jdbc" {
		t.Errorf("finalName = %q, quer %q (o §70 nomeia nishidb-jdbc.jar)", p.Build.FinalName, "nishidb-jdbc")
	}
	if p.ArtifactID != "nishidb-jdbc" {
		t.Errorf("artifactId = %q, quer nishidb-jdbc", p.ArtifactID)
	}
}

// O piso de Java é EXATO e usa `release`, não `source`/`target`.
//
// source/target compila contra a biblioteca do JDK instalado: um build feito
// num JDK 25 aceitaria API que não existe no 21, e o jar quebraria só em
// runtime, na máquina de quem usa. É a mesma lição do pin do Go e do
// golangci-lint — a versão da ferramenta é parte da configuração.
func TestOPomDoDriverFixaOPisoDeJavaComRelease(t *testing.T) {
	p := lePom(t)
	if p.Properties.Release != "21" {
		t.Errorf("maven.compiler.release = %q, quer \"21\" (o piso do §70)", p.Properties.Release)
	}
	bruto, err := os.ReadFile(filepath.Join(raizDoRepo, caminhoDoPom))
	if err != nil {
		t.Fatal(err)
	}
	for _, proibido := range []string{"<maven.compiler.source>", "<maven.compiler.target>"} {
		if strings.Contains(string(bruto), proibido) {
			t.Errorf("o pom usa %s: com ele o build aceita API que o piso não tem, e a falha só aparece em runtime", proibido)
		}
	}
}

// O wrapper é only-script e NENHUM .jar é versionado.
//
// Commitar um binário opaco para buscar o Maven é o oposto exato da doutrina
// do binário único, que existe para que o que roda seja o que se pode ler. E a
// distribuição é pinada com checksum pela mesma razão que o golangci-lint tem
// versão exata no Makefile: sem isso, o build de amanhã é outro build e nada
// acusa.
func TestOWrapperDoMavenNaoVersionaJarEFixaADistribuicao(t *testing.T) {
	dir := filepath.Join(raizDoRepo, "clients/jdbc/.mvn")
	err := filepath.Walk(dir, func(caminho string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".jar") {
			t.Errorf("%s é um .jar versionado; o wrapper tem de ser distributionType=only-script", caminho)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("varrendo o .mvn: %v", err)
	}

	props, err := os.ReadFile(filepath.Join(dir, "wrapper/maven-wrapper.properties"))
	if err != nil {
		t.Fatalf("lendo o maven-wrapper.properties: %v", err)
	}
	texto := string(props)
	for _, exigido := range []string{
		"distributionType=only-script",
		"distributionSha256Sum=",
	} {
		if !strings.Contains(texto, exigido) {
			t.Errorf("o maven-wrapper.properties não declara %s", exigido)
		}
	}
	// Versão EXATA na URL: um `3.9` solto (ou um "latest") faria a
	// distribuição mudar sem que o checksum pudesse acompanhar.
	if !strings.Contains(texto, "apache-maven-3.9.16-bin.zip") {
		t.Error("o distributionUrl não fixa uma versão exata do Maven")
	}
}

// A saída do build Java não pode entrar no repositório.
func TestOTargetDoDriverEstaNoGitignore(t *testing.T) {
	bruto, err := os.ReadFile(filepath.Join(raizDoRepo, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(bruto), "clients/jdbc/target/") {
		t.Error(".gitignore não ignora clients/jdbc/target/")
	}
}

// TODO código de erro do protocolo tem tradução para SQLState no driver.
//
// Este é o gate que fecha o laço entre os dois lados, e ele existe porque o
// modo de falha é SILENCIOSO: um código novo acrescentado ao servidor sairia
// no fio, o driver não o reconheceria e publicaria HY000 — a aplicação veria
// "erro genérico" para uma condição que o servidor sabe nomear, e nada
// acusaria. O CI Go reprova antes de qualquer JVM subir.
//
// A lista não é escrita à mão em lugar nenhum: sai do go/ast sobre
// internal/protocolo/protocolo.go de um lado, e do texto de Codigos.java do
// outro. É a mesma técnica de
// TestVersaoDoGolangciLintNoWorkflowBateComAInstrucaoDoMakefile — comparar
// duas EXTRAÇÕES, nunca uma extração contra um valor decorado, que envelhece
// junto com quem o escreveu.
func TestTodoCodigoDoProtocoloTemSqlStateNoDriver(t *testing.T) {
	codigos := codigosDoProtocolo(t)
	if len(codigos) < 15 {
		t.Fatalf("só %d códigos extraídos de protocolo.go: a extração quebrou, e um gate que "+
			"não acha nada passa sempre", len(codigos))
	}

	javaCodigos := leArquivo(t, "clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/wire/Codigos.java")
	javaEstados := leArquivo(t, "clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/wire/SqlStates.java")

	for nomeGo, valor := range codigos {
		literal := `"` + valor + `"`
		if !strings.Contains(javaCodigos, literal) {
			t.Errorf("%s (%s) não está em Codigos.java: acrescente a constante", nomeGo, valor)
			continue
		}
		// A constante existir não basta: ela precisa estar NA TABELA, senão o
		// código continua caindo em HY000.
		if !strings.Contains(javaEstados, "Codigos."+nomeJavaDoCodigo(valor)) {
			t.Errorf("%s (%s) tem constante mas não está na tabela de SqlStates.java", nomeGo, valor)
		}
	}
}

// nomeJavaDoCodigo devolve o nome da constante Java, que é o próprio valor do
// código — a convenção que faz o gate ser uma comparação e não um mapeamento a
// manter.
func nomeJavaDoCodigo(valor string) string { return valor }

func leArquivo(t *testing.T, relativo string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(raizDoRepo, relativo))
	if err != nil {
		t.Fatalf("lendo %s: %v", relativo, err)
	}
	return string(b)
}

// codigosDoProtocolo extrai as constantes Cod* de valor string, por AST.
//
// Por AST e não por regexp: o valor de uma constante é o que o compilador vê,
// e um grep casaria também comentários e strings de mensagem que mencionem o
// código.
func codigosDoProtocolo(t *testing.T) map[string]string {
	t.Helper()
	caminho := filepath.Join(raizDoRepo, "internal/protocolo/protocolo.go")
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
				if !strings.HasPrefix(nome.Name, "Cod") || i >= len(vs.Values) {
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
