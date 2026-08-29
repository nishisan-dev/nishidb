package segindex_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestOEscritorAgrupaOrdenaEDeduplicaAsPostings fixa as três propriedades que
// tornam o Lookup selado (Tarefa 2) uma fatia contígua: as postings de um
// valor ficam juntas, ascendentes e sem repetição. A entrada é dada FORA DE
// ORDEM e com duplicata de propósito — as duas coisas acontecem no dataset
// materializado, onde um update traz um rowId arbitrário e a mesma linha
// pode receber dois records com o mesmo valor dentro de um segmento.
//
// O oráculo é um decodificador LOCAL deste teste, escrito a partir do layout
// documentado em formato.go — não a partir de montaCorpo, e sobretudo não do
// leitor da Tarefa 2 (AbreSelado, que ainda não existe). Um teste do
// escritor que abrisse o próprio arquivo com o leitor não provaria nada: os
// dois lados são alimentados pela mesma leitura do mesmo formato.go, e um
// agrupamento errado em montaCorpo que o leitor decodificasse do mesmo jeito
// errado passaria batido — "os dois estão certos" e "os dois erram do mesmo
// jeito" ficam indistinguíveis. Ler os bytes aqui, direto do []byte que
// Escreve produziu, é o que trava o ESCRITOR sozinho.
func TestOEscritorAgrupaOrdenaEDeduplicaAsPostings(t *testing.T) {
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
	b, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatalf("ler arquivo gerado: %v", err)
	}

	// --- header ---
	secCount := binary.LittleEndian.Uint16(b[6:8])
	baseRowID := types.RowID(binary.LittleEndian.Uint64(b[16:24]))
	maxRowID := types.RowID(binary.LittleEndian.Uint64(b[24:32]))
	largura := b[32]

	if got, want := baseRowID, types.RowID(10); got != want {
		t.Errorf("baseRowID = %d, quero %d", got, want)
	}
	if got, want := maxRowID, types.RowID(30); got != want {
		t.Errorf("maxRowID = %d, quero %d", got, want)
	}
	if got, want := secCount, uint16(2); got != want {
		t.Fatalf("secCount = %d, quero %d (campos 3 e 7)", got, want)
	}

	// --- tabela de seções: ORDENADA por fieldID (campo 3 antes do 7) ---
	type secao struct {
		fieldID   types.FieldID
		distintos uint32
		dirOff    uint64
		postOff   uint64
	}
	secoes := make([]secao, secCount)
	for i := range secoes {
		off := segindex.HeaderSelado + i*segindex.TamSecao
		secoes[i] = secao{
			fieldID:   types.FieldID(binary.LittleEndian.Uint16(b[off : off+2])),
			distintos: binary.LittleEndian.Uint32(b[off+4 : off+8]),
			dirOff:    binary.LittleEndian.Uint64(b[off+8 : off+16]),
			postOff:   binary.LittleEndian.Uint64(b[off+16 : off+24]),
		}
	}
	if got, want := secoes[0].fieldID, types.FieldID(3); got != want {
		t.Fatalf("secoes[0].fieldID = %d, quero %d (campo 3 antes do 7)", got, want)
	}
	if got, want := secoes[1].fieldID, types.FieldID(7); got != want {
		t.Fatalf("secoes[1].fieldID = %d, quero %d", got, want)
	}

	sec7 := secoes[1]
	if got, want := sec7.distintos, uint32(2); got != want {
		t.Fatalf("campo 7: distintos = %d, quero %d (hash 50 e hash 100)", got, want)
	}

	// --- diretório do campo 7: ORDENADO por hash (50 antes de 100) ---
	type dirEntry struct {
		hash    uint64
		postIdx uint32
		count   uint32
	}
	dirs := make([]dirEntry, sec7.distintos)
	for i := range dirs {
		off := int(sec7.dirOff) + i*segindex.TamSlotDir
		dirs[i] = dirEntry{
			hash:    binary.LittleEndian.Uint64(b[off : off+8]),
			postIdx: binary.LittleEndian.Uint32(b[off+8 : off+12]),
			count:   binary.LittleEndian.Uint32(b[off+12 : off+16]),
		}
	}
	if got, want := dirs[0].hash, uint64(50); got != want {
		t.Fatalf("dirs[0].hash = %d, quero %d (50 antes de 100)", got, want)
	}
	if got, want := dirs[1].hash, uint64(100); got != want {
		t.Fatalf("dirs[1].hash = %d, quero %d", got, want)
	}

	// --- postings do valor 100: {10, 20, 30}, ascendente, SEM a duplicata ---
	valor100 := dirs[1]
	if got, want := valor100.count, uint32(3); got != want {
		t.Fatalf("valor 100: count = %d, quero %d (a duplicata de 30 tem de sumir)", got, want)
	}
	var rows []types.RowID
	for i := uint32(0); i < valor100.count; i++ {
		off := int(sec7.postOff) + int(valor100.postIdx+i)*int(largura)
		var delta uint64
		if largura == 4 {
			delta = uint64(binary.LittleEndian.Uint32(b[off : off+4]))
		} else {
			delta = binary.LittleEndian.Uint64(b[off : off+8])
		}
		rows = append(rows, baseRowID+types.RowID(delta))
	}
	want := []types.RowID{10, 20, 30}
	if len(rows) != len(want) {
		t.Fatalf("postings do valor 100 = %v, quero %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("postings do valor 100 = %v, quero %v", rows, want)
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
