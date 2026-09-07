package segindex_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
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
// documentado em format.go — não a partir de buildBody, e sobretudo não do
// leitor da Tarefa 2 (OpenSealed, que ainda não existe). Um teste do
// escritor que abrisse o próprio arquivo com o leitor não provaria nada: os
// dois lados são alimentados pela mesma leitura do mesmo format.go, e um
// agrupamento errado em buildBody que o leitor decodificasse do mesmo jeito
// errado passaria batido — "os dois estão certos" e "os dois erram do mesmo
// jeito" ficam indistinguíveis. Ler os bytes aqui, direto do []byte que
// WriteSealed produziu, é o que trava o ESCRITOR sozinho.
func TestOEscritorAgrupaOrdenaEDeduplicaAsPostings(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NewWriter()
	// campo 7, valor A (hash 100): rows 30, 10, 30 (duplicata), 20
	e.Add(7, 100, 30)
	e.Add(7, 100, 10)
	e.Add(7, 100, 30)
	e.Add(7, 100, 20)
	// campo 7, valor B (hash 50): row 15  -- hash MENOR, adicionado depois
	e.Add(7, 50, 15)
	// campo 3 (fieldID MENOR, adicionado por último)
	e.Add(3, 999, 12)

	path := segindex.SealedPath(dir, 42)
	if err := e.WriteSealed(path, 42); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ler arquivo gerado: %v", err)
	}

	// --- header ---
	secCount := binary.LittleEndian.Uint16(b[6:8])
	baseRowID := types.RowID(binary.LittleEndian.Uint64(b[16:24]))
	maxRowID := types.RowID(binary.LittleEndian.Uint64(b[24:32]))
	width := b[32]

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
	type section struct {
		fieldID        types.FieldID
		distinctValues uint32
		dirOff         uint64
		postOff        uint64
	}
	sections := make([]section, secCount)
	for i := range sections {
		off := segindex.SealedHeaderSize + i*segindex.SectionEntrySize
		sections[i] = section{
			fieldID:        types.FieldID(binary.LittleEndian.Uint16(b[off : off+2])),
			distinctValues: binary.LittleEndian.Uint32(b[off+4 : off+8]),
			dirOff:         binary.LittleEndian.Uint64(b[off+8 : off+16]),
			postOff:        binary.LittleEndian.Uint64(b[off+16 : off+24]),
		}
	}
	if got, want := sections[0].fieldID, types.FieldID(3); got != want {
		t.Fatalf("secoes[0].fieldID = %d, quero %d (campo 3 antes do 7)", got, want)
	}
	if got, want := sections[1].fieldID, types.FieldID(7); got != want {
		t.Fatalf("secoes[1].fieldID = %d, quero %d", got, want)
	}

	sec7 := sections[1]
	if got, want := sec7.distinctValues, uint32(2); got != want {
		t.Fatalf("campo 7: distintos = %d, quero %d (hash 50 e hash 100)", got, want)
	}

	// --- diretório do campo 7: ORDENADO por hash (50 antes de 100) ---
	type dirEntry struct {
		hash    uint64
		postIdx uint32
		count   uint32
	}
	dirs := make([]dirEntry, sec7.distinctValues)
	for i := range dirs {
		off := int(sec7.dirOff) + i*segindex.DirSlotSize
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
	value100 := dirs[1]
	if got, want := value100.count, uint32(3); got != want {
		t.Fatalf("valor 100: count = %d, quero %d (a duplicata de 30 tem de sumir)", got, want)
	}
	var rows []types.RowID
	for i := uint32(0); i < value100.count; i++ {
		off := int(sec7.postOff) + int(value100.postIdx+i)*int(width)
		var delta uint64
		if width == 4 {
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
	e := segindex.NewWriter()
	// rowIds ALTOS, mas com intervalo pequeno: o delta cabe em u32 mesmo com
	// o rowId absoluto passando de 2^32. É exatamente o caso do histórico com
	// meses no ar, e é o caso em que uma implementação que ignorasse o base
	// escolheria 8 bytes sem precisar.
	base := types.RowID(1) << 35
	e.Add(7, 100, base)
	e.Add(7, 100, base+1000)

	path := segindex.SealedPath(dir, 1)
	if err := e.WriteSealed(path, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	b, err := os.ReadFile(path)
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
	e := segindex.NewWriter()
	if !e.IsEmpty() {
		t.Fatal("escritor recém-criado deveria ser IsEmpty()")
	}
	path := segindex.SealedPath(dir, 1)
	if err := e.WriteSealed(path, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Stat(%s) = %v, quero IsNotExist", filepath.Base(path), err)
	}
}

// TestCadaEscritaUsaUmTemporarioProprio é a restrição que a §2.4 emendada põe no
// lugar do wmu na publicação do índice, e ela é o segundo dos três fatos que
// tornam a corrida no nome FINAL inofensiva.
//
// O nome do temporário era FIXO (`<sidx>.tmp`), e o aposAppend troca o segmento
// corrente ANTES de selar o anterior: existe uma janela em que o segmento
// recém-rotacionado aparece na fila de reconstrução, e então a selagem e o laço
// da §2.4 escreviam no MESMO arquivo intermediário. O que sai disso é um .sidx
// rasgado — inclusive o de ZERO BYTES que o mmap.ErrEmptyFile hoje tolera,
// cujo único caminho conhecido de produção é exatamente esta corrida.
//
// As duas asserções são independentes e nenhuma cobre a outra: a primeira é a
// unicidade (sem ela a corrida volta), a segunda é o SUFIXO (sem ele o
// temporário sai da conta do IndexBytes e da limpeza da abertura, e vira disco
// que nada explica).
func TestCadaEscritaUsaUmTemporarioProprio(t *testing.T) {
	path := segindex.SealedPath(t.TempDir(), 7)
	a := segindex.TempPathForTest(path)
	b := segindex.TempPathForTest(path)
	if a == b {
		t.Fatalf("dois escritores do segmento 7 receberam o MESMO temporário (%s): a selagem da rotação e a "+
			"reconstrução da fila escreveriam no mesmo arquivo, e o .sidx publicado sairia rasgado", a)
	}
	for _, name := range []string{a, b} {
		if !segindex.IsIndexTempFile(filepath.Base(name)) {
			t.Errorf("o temporário %s não casa com IsIndexTempFile: ele sairia da conta do IndexBytes e "+
				"a limpeza da abertura deixaria de encontrá-lo", filepath.Base(name))
		}
	}
}

// TestDoisEscritoresConcorrentesNaoPublicamArquivoRasgado é a mesma restrição
// pelo COMPORTAMENTO, e não pelo nome: duas escritas simultâneas no mesmo .sidx
// têm de deixar em disco um arquivo íntegro — o de um dos dois escritores,
// inteiro, nunca a mistura dos dois.
//
// Os dois conteúdos são DIFERENTES de propósito. Em produção eles são idênticos
// (a selagem e a reconstrução produzem os mesmos bytes, e é o golden da §6.3 que
// prova), e escrever o mesmo conteúdo duas vezes esconderia o rasgo: o arquivo
// sairia certo mesmo com as duas escritas embaralhadas. Aqui a diferença é o que
// torna a mistura VISÍVEL — um crc que não fecha, ou um arquivo de zero bytes.
func TestDoisEscritoresConcorrentesNaoPublicamArquivoRasgado(t *testing.T) {
	for round := range 50 {
		dir := t.TempDir()
		path := segindex.SealedPath(dir, 1)
		var wg sync.WaitGroup
		for i, postings := range [][]types.RowID{{1, 2, 3}, {10, 20, 30, 40, 50, 60, 70, 80}} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				e := segindex.NewWriter()
				for _, row := range postings {
					e.Add(types.FieldID(1+i), uint64(100+i), row)
				}
				if err := e.WriteSealed(path, 1); err != nil {
					t.Errorf("WriteSealed concorrente: %v", err)
				}
			}()
		}
		wg.Wait()

		sel, err := segindex.OpenSealed(path)
		if err != nil {
			t.Fatalf("rodada %d: o .sidx publicado por duas escritas concorrentes não abre: %v", round, err)
		}
		err = sel.IntegrityErr()
		if cerr := sel.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatalf("rodada %d: o .sidx publicado por duas escritas concorrentes saiu rasgado: %v", round, err)
		}
	}
}

// TestApagaTemporariosRemoveOResiduoDeUmProcessoMorto é a contrapartida do nome
// único. Com o nome fixo, o temporário de um processo morto era truncado pela
// escrita seguinte e o disco voltava sozinho; com um nome por escritor, ninguém
// mais o alcança — e o IndexBytes o conta, porque ele casa com IsIndexFile.
//
// Os arquivos que NÃO são temporário ficam, e a asserção é tão importante quanto
// a outra: uma limpeza que levasse junto o .sidx ou o .widx apagaria índice bom
// em toda abertura.
func TestApagaTemporariosRemoveOResiduoDeUmProcessoMorto(t *testing.T) {
	dir := t.TempDir()
	residue := segindex.TempPathForTest(segindex.SealedPath(dir, 3))
	sealed := segindex.SealedPath(dir, 3)
	data := filepath.Join(dir, "segment-00000003.dat")
	for _, name := range []string{residue, sealed, data} {
		if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := segindex.RemoveTempFiles(dir); err != nil {
		t.Fatalf("RemoveTempFiles: %v", err)
	}
	if _, err := os.Stat(residue); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist: o temporário de um processo morto ficaria em disco para sempre, "+
			"contado pelo IndexBytes e sem ninguém para reaproveitá-lo", filepath.Base(residue), err)
	}
	for _, name := range []string{sealed, data} {
		if _, err := os.Stat(name); err != nil {
			t.Errorf("a limpeza levou %s junto: %v", filepath.Base(name), err)
		}
	}
}
