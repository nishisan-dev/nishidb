package renametools

import (
	"strings"
	"testing"
)

// findRow acha a linha do inventário para a espécie e o endereço dados.
func findRow(t *testing.T, rows [][]string, kind, address string) []string {
	t.Helper()
	for _, r := range rows[1:] {
		if r[1] == kind && r[2] == address {
			return r
		}
	}
	t.Fatalf("inventário sem a linha %s %s", kind, address)
	return nil
}

func TestInventoryAddressesEachKindTheWayRfUnderstands(t *testing.T) {
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(inv.Rows[0], " "); got != "pacote especie endereco nome posicao exportado grupo" {
		t.Fatalf("cabeçalho: %q", got)
	}
	cases := []struct{ kind, address, pkg, exported string }{
		{"type", "Escritor", "a", "sim"},
		{"ifacemethod", "Escritor.Apenda", "a", "sim"},
		{"type", "Contagem", "a", "sim"},
		{"field", "Contagem.Total", "a", "sim"},
		{"field", "Contagem.erros", "a", "nao"},
		{"func", "Roda", "a", "sim"},
		{"param", "Roda.e", "a", "nao"},
		{"param", "Roda.chaves", "a", "nao"},
		{"local", "Roda.c", "a", "nao"},
		{"local", "Roda.chave", "a", "nao"},
		{"method", "Arquivo.Apenda", "b", "sim"},
		{"param", "Arquivo.Apenda.chave", "b", "nao"},
		{"local", "Abre.tamanho", "b", "nao"},
		{"type", "escritorFalso", "a", "nao"},
		{"method", "escritorFalso.Apenda", "a", "sim"},
		{"func", "TestRodaConta", "a", "sim"},
		{"type", "dublê", "c", "nao"},
		{"method", "dublê.Fecha", "c", "sim"},
	}
	for _, c := range cases {
		r := findRow(t, inv.Rows, c.kind, c.address)
		if r[0] != c.pkg || r[5] != c.exported {
			t.Errorf("%s %s: pacote=%q exportado=%q, esperado %q %q", c.kind, c.address, r[0], r[5], c.pkg, c.exported)
		}
	}
}

func TestInventorySkipsShortReceiverAndEmbeddedField(t *testing.T) {
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range inv.Rows[1:] {
		if r[2] == "Arquivo.Apenda.arq" {
			t.Errorf("receptor de três letras entrou: %v — receptor só entra quando tem quatro letras ou mais", r)
		}
		if r[1] == "field" && r[3] == "" {
			t.Errorf("campo embutido entrou sem nome: %v", r)
		}
	}
}

func TestInventoryMarksAmbiguousLocal(t *testing.T) {
	// TestRodaConta declara `t` duas vezes no MESMO endereço rf
	// TestRodaConta.t: o parâmetro da função de teste e o parâmetro homônimo
	// do closure passado a t.Run — enclosingFunc atribui identificadores de
	// função literal à FuncDecl externa (spec §4.1), então as duas contam
	// para o mesmo endereço. Endereço declarado mais de uma vez na mesma
	// função é o que o rf recusa: cada ocorrência ganha o sufixo #ambiguo na
	// espécie (a primeira como param, a segunda como local — nenhuma das
	// duas achatada na outra), e o aplicador cai para gopls por posição.
	// Nenhuma outra declaração do fixture repete endereço.
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	var ambiguous [][]string
	for _, r := range inv.Rows[1:] {
		if strings.HasSuffix(r[1], "#ambiguo") {
			ambiguous = append(ambiguous, r)
		}
	}
	if len(ambiguous) != 2 {
		t.Fatalf("linhas ambíguas: %v, esperadas 2 (TestRodaConta.t duas vezes)", ambiguous)
	}
	kinds := map[string]bool{}
	for _, r := range ambiguous {
		if r[2] != "TestRodaConta.t" {
			t.Errorf("linha ambígua fora de TestRodaConta.t: %v", r)
		}
		kinds[r[1]] = true
	}
	if !kinds["param#ambiguo"] || !kinds["local#ambiguo"] {
		t.Errorf("espécies ambíguas de TestRodaConta.t: %v, esperado param#ambiguo e local#ambiguo", kinds)
	}
}

func TestInventoryMarksFieldsOfFunctionLocalStructForGopls(t *testing.T) {
	// Rotula (b/b.go) declara `type rotulo struct{...}` DENTRO da função:
	// rotulo não é endereço de pacote, então rotulo.nome não é sintaxe que o
	// rf resolve (Ruling R6) — a espécie leva #gopls e o endereço é
	// F.T.campo (Rotula.rotulo.nome), para o aplicador renomear por posição.
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	r := findRow(t, inv.Rows, "field#gopls", "Rotula.rotulo.nome")
	if r[0] != "b" || r[5] != "nao" {
		t.Errorf("pacote=%q exportado=%q, esperado b nao", r[0], r[5])
	}
}

func TestInventoryMarksFieldsOfAnonymousStructInsideFunctionForGopls(t *testing.T) {
	// Tabela, Mapa, Pesa e Marca (b/b.go) declaram struct ANÔNIMA dentro da
	// função — a forma dominante da tabela de casos de teste do repositório.
	// Não há tipo nomeado nem `var` de pacote para o rf endereçar (Ruling
	// R29): a espécie leva #gopls e o endereço leva o nome de quem batiza a
	// struct (a variável do `:=`, a do `var`, o parâmetro), ou nada quando
	// nenhum nome a batiza. Enquanto esses campos ficaram fora do inventário
	// eles não chegaram a nenhum tradutor: o crítico de completude do G2 achou
	// 17 deles ainda em PT-BR em quatro pacotes.
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct{ address, name string }{
		{"Tabela.casos.nome", "nome"},   // `casos := []struct{...}{...}`
		{"Tabela.casos.conta", "conta"}, // segundo campo da mesma struct
		{"Mapa.pesos.peso", "peso"},     // `map[string]struct{...}`
		{"Pesa.p.peso", "peso"},         // parâmetro de tipo struct anônimo
		{"Marca.peso", "peso"},          // literal sem nome que o batize
	}
	for _, c := range cases {
		r := findRow(t, inv.Rows, "field#gopls", c.address)
		if r[0] != "b" || r[3] != c.name || r[5] != "nao" {
			t.Errorf("%s: pacote=%q nome=%q exportado=%q, esperado b %q nao", c.address, r[0], r[3], r[5], c.name)
		}
		if !strings.HasPrefix(r[4], "b/b.go:") {
			t.Errorf("%s: posição %q, esperada em b/b.go (o gopls renomeia por posição)", c.address, r[4])
		}
	}
	// Negativo: campo de struct NOMEADA de topo continua `T.F` com a espécie
	// `field`, que vai ao rf por endereço — a mudança não pode arrastar o caso
	// comum para o fallback por posição.
	findRow(t, inv.Rows, "field", "Contagem.Total")
	for _, r := range inv.Rows[1:] {
		if r[1] == "field#gopls" && (r[2] == "Contagem.Total" || r[2] == "Arquivo.Linhas") {
			t.Errorf("campo de struct nomeada de topo caiu para gopls: %v", r)
		}
	}
}

func TestInventoryDoesNotCountTheFunctionNameAsAmbiguity(t *testing.T) {
	// Conta(Conta int) int: o parâmetro tem o MESMO nome da função. O Ident
	// de fn.Name não é uma segunda declaração no endereço Conta.Conta — é o
	// PRÓPRIO endereço `Conta` da função, que não colide por posição (nome
	// duplicado no mesmo escopo de pacote é erro de compilação) — e não pode
	// contar para a detecção de ambiguidade; senão qualquer parâmetro,
	// receptor, local, type/const ou label homônimo da função que o declara
	// sairia #ambiguo por engano.
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	r := findRow(t, inv.Rows, "param", "Conta.Conta")
	// exportado é "sim" aqui: Exported() é puramente sobre a primeira letra
	// do nome (types.Object.Exported()), não sobre o escopo — um parâmetro
	// com inicial maiúscula é "sim" mesmo sendo local, igual a qualquer
	// outro `local`/`param` maiúsculo do inventário.
	if r[0] != "b" || r[5] != "sim" {
		t.Errorf("pacote=%q exportado=%q, esperado b sim", r[0], r[5])
	}
}

func TestInventoryDeduplicatesTestVariants(t *testing.T) {
	// Com Tests: true, go/packages carrega `a`, `a [a.test]` e `a.test`; a
	// mesma declaração aparece nas três com a mesma posição. Uma linha só.
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, r := range inv.Rows[1:] {
		seen[r[4]]++
	}
	for pos, n := range seen {
		if n > 1 {
			t.Errorf("posição %s inventariada %d vezes", pos, n)
		}
	}
}
