package segindex_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestOEscritorAgrupaOrdenaEDeduplicaAsPostings fixa as três propriedades que
// tornam o Lookup selado uma fatia contígua: as postings de um valor ficam
// juntas, ascendentes e sem repetição. A entrada é dada FORA DE ORDEM e com
// duplicata de propósito — as duas coisas acontecem no dataset materializado,
// onde um update traz um rowId arbitrário e a mesma linha pode receber dois
// records com o mesmo valor dentro de um segmento.
func TestOEscritorAgrupaOrdenaEDeduplicaAsPostings(t *testing.T) {
	t.Skip("AbreSelado chega na Tarefa 2")

	dir := t.TempDir()
	e := segindex.NovoEscritor()
	// campo 7, valor A (hash 100): rows 30, 10, 30 (duplicata), 20
	e.Adiciona(7, 100, 30)
	e.Adiciona(7, 100, 10)
	e.Adiciona(7, 100, 30)
	e.Adiciona(7, 100, 20)
	// campo 7, valor B (hash 50): row 15  -- hash MENOR, adicionado depois
	e.Adiciona(7, 50, 15)
	// campo 3 (fieldID MENOR, adicionado por último)
	e.Adiciona(3, 999, 12)

	caminho := segindex.CaminhoSelado(dir, 42)
	if err := e.Escreve(caminho, 42); err != nil {
		t.Fatalf("Escreve: %v", err)
	}

	s, err := segindex.AbreSelado(caminho)
	if err != nil {
		t.Fatalf("AbreSelado: %v", err)
	}
	defer s.Fecha()

	if got, want := s.Campos(), []types.FieldID{3, 7}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Campos() = %v, quero %v (ordenados por fieldID)", got, want)
	}
	if got, want := s.MinRowID(), types.RowID(10); got != want {
		t.Errorf("MinRowID() = %d, quero %d", got, want)
	}
	if got, want := s.MaxRowID(), types.RowID(30); got != want {
		t.Errorf("MaxRowID() = %d, quero %d", got, want)
	}

	it, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup(7, 100) não encontrou o valor")
	}
	var rows []types.RowID
	for it.Next() {
		rows = append(rows, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterador: %v", err)
	}
	want := []types.RowID{10, 20, 30}
	if len(rows) != len(want) {
		t.Fatalf("Lookup(7,100) = %v, quero %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("Lookup(7,100) = %v, quero %v", rows, want)
		}
	}
}

// TestOEscritorRebaseiaParaAMenorLarguraQueCabe prova a promessa de disco da
// spec: o WIP guarda rowId absoluto em 8 bytes por precaução, e a selagem
// re-baseia para 4 porque a incerteza já acabou. Sem isto a conta de
// "16 x distintos + 4 x linhas" seria mentira.
func TestOEscritorRebaseiaParaAMenorLarguraQueCabe(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NovoEscritor()
	// rowIds ALTOS, mas com intervalo pequeno: o delta cabe em u32 mesmo com
	// o rowId absoluto passando de 2^32. É exatamente o caso do histórico com
	// meses no ar, e é o caso em que uma implementação que ignorasse o base
	// escolheria 8 bytes sem precisar.
	base := types.RowID(1) << 35
	e.Adiciona(7, 100, base)
	e.Adiciona(7, 100, base+1000)

	caminho := segindex.CaminhoSelado(dir, 1)
	if err := e.Escreve(caminho, 1); err != nil {
		t.Fatalf("Escreve: %v", err)
	}
	b, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if got := b[32]; got != 4 {
		t.Fatalf("postingWidth = %d, quero 4 (o intervalo é 1000, cabe em u32)", got)
	}
}

// TestOEscritorVazioNaoCriaArquivo: um segmento sem nenhuma linha indexada não
// tem índice, e "arquivo ausente" é o estado que o Lookup já sabe tratar por
// enumeração. Criar um .sidx vazio faria a fila de reconstrução considerá-lo
// pronto e o segmento nunca ganharia índice de verdade.
func TestOEscritorVazioNaoCriaArquivo(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NovoEscritor()
	if !e.Vazio() {
		t.Fatal("escritor recém-criado deveria ser Vazio()")
	}
	caminho := segindex.CaminhoSelado(dir, 1)
	if err := e.Escreve(caminho, 1); err != nil {
		t.Fatalf("Escreve: %v", err)
	}
	if _, err := os.Stat(caminho); !os.IsNotExist(err) {
		t.Fatalf("Stat(%s) = %v, quero IsNotExist", filepath.Base(caminho), err)
	}
}
