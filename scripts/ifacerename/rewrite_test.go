package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// copyModule leva o módulo de teste para um diretório descartável: o
// rewrite escreve em disco, e o testdata não pode mudar entre testes.
func copyModule(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	src := filepath.Join("..", "renametools", "testdata", "modulo")
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func readFile(t *testing.T, dir, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func buildAndVet(t *testing.T, dir string) error {
	t.Helper()
	for _, args := range [][]string{{"build", "./..."}, {"vet", "./..."}} {
		cmd := exec.Command("go", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOFLAGS=-buildvcs=false")
		if out, err := cmd.CombinedOutput(); err != nil {
			return &buildError{args[0], string(out)}
		}
	}
	return nil
}

type buildError struct{ stage, output string }

func (e *buildError) Error() string { return e.stage + ": " + e.output }

func TestRenamesTheWholeGroupAndTheBuildStaysGreen(t *testing.T) {
	dir := copyModule(t)
	rep, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := buildAndVet(t, dir); err != nil {
		t.Fatalf("o módulo não compila depois do rename: %v", err)
	}
	for _, rel := range []string{"a/a.go", "a/a_test.go", "b/b.go", "c/c.go", "c/c_test.go"} {
		s := readFile(t, dir, rel)
		if strings.Contains(s, "Apenda(") {
			t.Errorf("%s ainda tem Apenda(", rel)
		}
	}
	// Uso por seletor, em outro pacote, e o dublê do pacote externo de teste.
	if !strings.Contains(readFile(t, dir, "c/c.go"), "arq.Append(") {
		t.Error("c.go: o seletor arq.Apenda não virou arq.Append")
	}
	if !strings.Contains(readFile(t, dir, "c/c_test.go"), "func (dublê) Append(") {
		t.Error("c_test.go: o dublê do pacote c_test não foi renomeado")
	}
	if len(rep.Renamed) != 5 {
		t.Errorf("renomeados: %d, esperado 5 (2 interfaces + 3 implementadores)", len(rep.Renamed))
	}
}

func TestRewritesTheFirstWordOfTheGodoc(t *testing.T) {
	dir := copyModule(t)
	if _, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"}}, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(readFile(t, dir, "a/a.go"), "// Append apenda um registro.") {
		t.Error("a.go: o godoc do método da interface não começa por Append")
	}
	if !strings.Contains(readFile(t, dir, "b/b.go"), "// Append guarda a chave. Ver a.Escritor.") {
		t.Error("b.go: o godoc do implementador não começa por Append (e a menção a.Escritor fica: é prosa)")
	}
	// A menção em godoc de OUTRA declaração fica para o renamecite.
	if !strings.Contains(readFile(t, dir, "a/a.go"), "Fecha libera o que Apenda reteve") {
		t.Error("a.go: o ifacerename mexeu em prosa alheia; isso é trabalho do renamecite")
	}
}

// O godoc COLADO: o grupo de comentários da declaração começa por um parágrafo
// de outro assunto, e a linha que nomeia o método vem depois, sem a linha em
// branco que separaria os dois grupos. Olhando só `doc.List[0]`, o aplicador
// não reconhece o nome velho, não troca nada e reporta sucesso — e o godoc
// fica descrevendo um método que não existe mais.
//
// O caso é real: `internal/materializer` declara a interface Store com o
// parágrafo do contrato do buffer colado à frase que abre o godoc de Estado
// (materializer.go:25-41), e foi assim que `Estado` sobreviveu ao G3.
func TestRewritesTheGodocLineThatNamesTheMethodEvenWhenItIsNotTheFirst(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod": "module exemplo.local/godoccolado\n\ngo 1.25\n",
		"p/p.go": `package p

// Store é a superfície do storage consumida pelo materializer.
type Store interface {
	// Contrato do buffer: o caller passa buf[:0] e REATRIBUI o slice
	// devolvido; o provedor pode substituí-lo por um maior, mas NUNCA pode
	// preservar o conteúdo anterior. O Estado anterior continua no buf.
	// Estado devolve os TRÊS estados da key.
	Estado(key []byte) int
}
`,
	})
	if _, err := Apply(dir, []Entry{{Pkg: "p", Iface: "Store", Method: "Estado", New: "State"}}, false); err != nil {
		t.Fatal(err)
	}
	s := readFile(t, dir, "p/p.go")
	if !strings.Contains(s, "// State devolve os TRÊS estados da key.") {
		t.Errorf("p.go: a linha do godoc que nomeia o método não foi reescrita:\n%s", s)
	}
	if strings.Contains(s, "// Estado devolve") {
		t.Errorf("p.go: o nome velho sobrou no godoc:\n%s", s)
	}
	if !strings.Contains(s, "// Contrato do buffer:") {
		t.Errorf("p.go: o parágrafo de outro assunto foi mexido:\n%s", s)
	}
	// A menção NO MEIO da prosa fica: só o token que abre a linha é o nome que
	// o revive cobra, e o resto é trabalho do renamecite, que sabe distinguir
	// citação de palavra.
	if !strings.Contains(s, "O Estado anterior continua no buf.") {
		t.Errorf("p.go: a menção no meio da prosa foi trocada; isso é trabalho do renamecite:\n%s", s)
	}
}

func TestRefusesCollisionInTheMethodSet(t *testing.T) {
	dir := copyModule(t)
	_, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Fecha"}}, false)
	if err == nil || !strings.Contains(err.Error(), "já tem Fecha") {
		t.Fatalf("esperava recusa por colisão, veio %v", err)
	}
	if strings.Contains(readFile(t, dir, "b/b.go"), "Fecha(chave") {
		t.Error("escreveu apesar da recusa")
	}
}

// moduleSnapshot lê o módulo inteiro. A barreira que os testes de recusa
// cobram é "nada é escrito", e conferir um arquivo só deixaria passar uma
// escrita parcial em qualquer outro — que é exatamente o estado em que uma
// recusa tardia deixa a árvore.
func moduleSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(dir, p)
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func assertUnchanged(t *testing.T, dir string, before map[string]string) {
	t.Helper()
	for rel, body := range moduleSnapshot(t, dir) {
		if before[rel] != body {
			t.Errorf("%s mudou apesar da recusa", rel)
		}
	}
}

// Duas entradas, dois grupos DIFERENTES, o mesmo nome novo, e um receptor que
// está nos dois: b.Arquivo declara Apenda e Fecha. A checagem de colisão contra
// o estado ANTES do rename aprova as duas (ninguém tem Close ainda) e o método
// sairia declarado duas vezes em b.Arquivo — só o build de quem chama pegaria,
// depois de a árvore inteira já ter sido reescrita.
func TestRefusesTwoGroupsConvergingOnTheSameNameOnOneReceiver(t *testing.T) {
	dir := copyModule(t)
	before := moduleSnapshot(t, dir)
	_, err := Apply(dir, []Entry{
		{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Close"},
		{Pkg: "a", Iface: "Escritor", Method: "Fecha", New: "Close"},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "duas vezes no mesmo lote") {
		t.Fatalf("esperava recusa por convergência dentro do lote, veio %v", err)
	}
	if !strings.Contains(err.Error(), "Close") {
		t.Errorf("a recusa não nomeia o nome novo: %v", err)
	}
	if !strings.Contains(err.Error(), "a.Escritor") {
		t.Errorf("a recusa não nomeia o receptor: %v", err)
	}
	assertUnchanged(t, dir, before)
}

// O caso espelho — Apenda→Fecha enquanto Fecha→Close no mesmo lote — é LEGAL
// (o Fecha velho sai de cena antes de o novo entrar), e mesmo assim é recusado
// pela checagem contra o estado anterior. É conservadorismo aceito: recusar o
// que daria certo custa uma segunda rodada; escrever o que não compila custa a
// árvore. O teste existe para que a troca seja uma decisão, não um acidente.
func TestRefusesTheLegalButChainedRename(t *testing.T) {
	dir := copyModule(t)
	before := moduleSnapshot(t, dir)
	_, err := Apply(dir, []Entry{
		{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Fecha"},
		{Pkg: "a", Iface: "Escritor", Method: "Fecha", New: "Close"},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "já tem Fecha") {
		t.Fatalf("esperava a recusa conservadora da checagem contra o estado anterior, veio %v", err)
	}
	assertUnchanged(t, dir, before)
}

// Nome novo que não é identificador Go: sem esta barreira o lote começa a
// escrever, o gofmt do primeiro arquivo falha e o Apply volta com a árvore já
// meio reescrita e sem formatar.
func TestRefusesAnInvalidNewName(t *testing.T) {
	dir := copyModule(t)
	before := moduleSnapshot(t, dir)
	_, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Put Record"}}, false)
	if err == nil || !strings.Contains(err.Error(), "não é um identificador Go válido") {
		t.Fatalf("esperava recusa por nome inválido, veio %v", err)
	}
	assertUnchanged(t, dir, before)
}

// writeModule escreve um módulo mínimo num diretório descartável. O módulo de
// `renametools/testdata` não serve para o caso abaixo e é fixture de outro
// pacote: mexer nele quebraria os testes da Tarefa 0.3.
func writeModule(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, body := range files {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// O grupo de Avanca não tem implementador NENHUM dentro do módulo, e é o único
// arranjo em que a colisão só existe dentro da própria interface. Perguntar
// pelo method set de `*Cursor` não a veria — o de um ponteiro para interface é
// vazio —, o lote passaria e o módulo sairia com Fecha declarado duas vezes.
// Os outros testes de colisão não discriminam isso: lá todo implementador
// concreto do grupo também carrega o nome novo e trip a checagem por tabela.
func TestRefusesCollisionInsideAnInterfaceWithNoLocalImplementer(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod": "module exemplo.local/soiface\n\ngo 1.25\n",
		"p/p.go": `package p

// Cursor não é satisfeita por nenhum tipo deste módulo.
type Cursor interface {
	// Avanca anda uma posição.
	Avanca() bool
	Fecha() error
}
`,
	})
	antes := readFile(t, dir, "p/p.go")
	_, err := Apply(dir, []Entry{{Pkg: "p", Iface: "Cursor", Method: "Avanca", New: "Fecha"}}, false)
	if err == nil || !strings.Contains(err.Error(), "já tem Fecha") {
		t.Fatalf("esperava recusa por colisão dentro da interface, veio %v", err)
	}
	if readFile(t, dir, "p/p.go") != antes {
		t.Error("escreveu apesar da recusa")
	}
}

func TestRefusesDivergentNewNamesInTheSameGroup(t *testing.T) {
	dir := copyModule(t)
	_, err := Apply(dir, []Entry{
		{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"},
		{Pkg: "c", Iface: "SoApenda", Method: "Apenda", New: "Push"},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "mesmo grupo") {
		t.Fatalf("esperava recusa por nomes divergentes, veio %v", err)
	}
}

func TestDryRunDoesNotWrite(t *testing.T) {
	dir := copyModule(t)
	before := readFile(t, dir, "b/b.go")
	rep, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if readFile(t, dir, "b/b.go") != before {
		t.Error("dry-run escreveu")
	}
	if len(rep.Files) != 5 {
		t.Errorf("dry-run listou %d arquivos, esperado 5", len(rep.Files))
	}
}
