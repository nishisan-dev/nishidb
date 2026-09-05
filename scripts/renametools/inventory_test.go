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
	// Roda declara `chave` uma vez (no for). Abre declara `tamanho` uma vez.
	// Nenhum é ambíguo; o teste é o inverso: um endereço declarado duas vezes
	// na mesma função ganha o sufixo #ambiguo na especie, e o rf não o aceita
	// — cai para gopls por posição (spec §4.1).
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range inv.Rows[1:] {
		if strings.HasSuffix(r[1], "#ambiguo") {
			t.Errorf("o módulo de teste não tem local repetido, mas %v saiu ambíguo", r)
		}
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
