package segindex_test

import (
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// campoDeTeste e segmentoDeTeste são o (segmento, campo) de todos os WIP deste
// arquivo. Ficam nomeados porque o par é o que batiza o arquivo em disco, e um
// teste que trocasse um dos dois estaria falando de outro arquivo.
const (
	segmentoDeTeste = uint32(1)
	campoDeTeste    = types.FieldID(7)
)

// abreWIPDeTesteComSlots abre um WIP novo com o número de slots pedido.
func abreWIPDeTesteComSlots(t *testing.T, slots uint32) *segindex.WIP {
	t.Helper()
	w, err := segindex.AbreWIP(t.TempDir(), segmentoDeTeste, campoDeTeste, slots)
	if err != nil {
		t.Fatalf("AbreWIP: %v", err)
	}
	return w
}

// abreWIPDeTeste abre um WIP folgado o bastante para que NADA cresça durante o
// teste — nem o diretório, nem o array de postings.
//
// A folga é requisito dos gates de alocação, e não conforto: Region.Grow chama
// unix.Mmap, que mantém os mapeamentos vivos num map e portanto ALOCA. Um
// crescimento dentro da janela medida contaria a alocação do mmap como se fosse
// do Set. Os gates medem o regime, que é onde a ingestão vive; o crescimento é
// amortizado e tem gate próprio (TestOWIPCresceSemPerderPosting).
func abreWIPDeTeste(t *testing.T) *segindex.WIP {
	t.Helper()
	return abreWIPDeTesteComSlots(t, 4096)
}

// fechaWIP fecha o WIP reportando a falha.
//
// Existe porque `defer w.Fecha()` descartaria o erro em silêncio, e o gate de
// lint deste projeto conta esse descarte. Fechar é também onde o header ganha o
// postCount definitivo, então um erro aqui significa um arquivo que a Tarefa 4
// não conseguiria reabrir.
func fechaWIP(t *testing.T, w *segindex.WIP) {
	t.Helper()
	if err := w.Fecha(); err != nil {
		t.Errorf("Fecha: %v", err)
	}
}

// TestOWIPDevolveOrdemEstritamenteCrescenteComRowIdsForaDeOrdem cobre o
// dataset materializado, onde um update traz para o segmento corrente um rowId
// ARBITRÁRIO — a cadeia percorrida ao contrário NÃO sai ascendente ali. E a
// mesma linha pode receber dois records com o mesmo valor dentro de um
// segmento, o que exige compactar: o contrato do iterador é ESTRITAMENTE
// crescente, e um id repetido faria o unionIter do executor emitir a mesma
// linha duas vezes num OR.
func TestOWIPDevolveOrdemEstritamenteCrescenteComRowIdsForaDeOrdem(t *testing.T) {
	w := abreWIPDeTeste(t)
	defer fechaWIP(t, w)
	for _, r := range []types.RowID{50, 10, 30, 10, 40} {
		if err := w.Set(100, r); err != nil {
			t.Fatal(err)
		}
	}
	got := w.Lookup(100)
	want := []types.RowID{10, 30, 40, 50}
	if !slices.Equal(got, want) {
		t.Fatalf("Lookup = %v, quero %v", got, want)
	}
}

// TestOWIPCresceSemPerderPosting: o array de postings dobra e o diretório
// rehasheia, e nenhum dos dois pode perder uma posting. O rehash reescreve SÓ o
// diretório — as postings não se movem, porque o índice 1-based delas é o que
// a cadeia guarda.
func TestOWIPCresceSemPerderPosting(t *testing.T) {
	w := abreWIPDeTesteComSlots(t, 16) // força vários rehashes
	defer fechaWIP(t, w)
	const n = 5000
	for i := range n {
		if err := w.Set(uint64(i)+2, types.RowID(i)); err != nil { // n valores DISTINTOS
			t.Fatal(err)
		}
	}
	for i := range n {
		got := w.Lookup(uint64(i) + 2)
		if len(got) != 1 || got[0] != types.RowID(i) {
			t.Fatalf("Lookup(%d) = %v, quero [%d] — o crescimento perdeu a posting", i+2, got, i)
		}
	}
	if got := w.Postings(); got != n {
		t.Errorf("Postings() = %d, quero %d", got, n)
	}
	if got := w.Distintos(); got != n {
		t.Errorf("Distintos() = %d, quero %d", got, n)
	}
}

// TestValorAusenteNoWIPNaoDevolveNada separa "o valor não está aqui" de "está,
// e a resposta é vazia". No WIP as duas viram nil, e é isso que o chamador
// precisa poder confiar: um slot vazio na cadeia de sondagem NÃO pode fazer a
// busca continuar até dar a volta na tabela.
func TestValorAusenteNoWIPNaoDevolveNada(t *testing.T) {
	w := abreWIPDeTeste(t)
	defer fechaWIP(t, w)
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	if got := w.Lookup(200); got != nil {
		t.Errorf("Lookup de valor ausente = %v, quero nil", got)
	}
}

// TestOWIPGuardaOCampoQueFoiPedido trava o par (arquivo, campo). O nome do
// arquivo carrega o fieldID e o header o repete: quem abre um .widx precisa
// saber que está falando do campo que pediu, porque um Lookup sobre o campo
// errado devolveria linhas erradas sem nenhum sintoma.
func TestOWIPGuardaOCampoQueFoiPedido(t *testing.T) {
	w := abreWIPDeTeste(t)
	defer fechaWIP(t, w)
	if got := w.Campo(); got != campoDeTeste {
		t.Errorf("Campo() = %d, quero %d", got, campoDeTeste)
	}
}

// TestParaCadaVarreTodasAsPostingsUmaVez é o que a selagem (Tarefa 4) consome
// para alimentar o Escritor.
//
// A varredura NÃO promete ordem, e a asserção é feita sobre um multiconjunto de
// propósito: o Escritor já ordena e deduplica, e ordenar aqui seria a mesma
// ordenação em dois lugares — que divergem na primeira mudança de critério. O
// que ela promete é COBERTURA: cada posting exatamente uma vez, duplicatas
// incluídas, senão a selagem perderia postings que o WIP tem.
func TestParaCadaVarreTodasAsPostingsUmaVez(t *testing.T) {
	w := abreWIPDeTeste(t)
	defer fechaWIP(t, w)
	postadas := []struct {
		h   uint64
		row types.RowID
	}{
		{100, 50}, {100, 10}, {100, 10}, // duplicata deliberada
		{200, 7},
		{300, 42}, {300, 41},
	}
	for _, p := range postadas {
		if err := w.Set(p.h, p.row); err != nil {
			t.Fatal(err)
		}
	}

	vistas := map[[2]uint64]int{}
	total := 0
	if err := w.ParaCada(func(h uint64, row types.RowID) {
		vistas[[2]uint64{h, uint64(row)}]++
		total++
	}); err != nil {
		t.Fatalf("ParaCada: %v", err)
	}
	if total != len(postadas) {
		t.Errorf("ParaCada visitou %d postings, quero %d", total, len(postadas))
	}
	querido := map[[2]uint64]int{}
	for _, p := range postadas {
		querido[[2]uint64{p.h, uint64(p.row)}]++
	}
	for chave, n := range querido {
		if vistas[chave] != n {
			t.Errorf("ParaCada viu (h=%d,row=%d) %d vezes, quero %d", chave[0], chave[1], vistas[chave], n)
		}
	}
	for chave := range vistas {
		if querido[chave] == 0 {
			t.Errorf("ParaCada inventou (h=%d,row=%d)", chave[0], chave[1])
		}
	}
}

// TestAbreWIPComSlotsZeroExigeArquivoExistente trava a forma que a Tarefa 4 usa
// para LER um WIP já gravado: slotsMin 0 significa "abre o que está lá, sem
// redimensionar". Sobre arquivo ausente ela precisa FALHAR, e não criar um WIP
// vazio — um WIP vazio faria a selagem emitir um índice sem postings e o
// segmento seria dado por indexado.
func TestAbreWIPComSlotsZeroExigeArquivoExistente(t *testing.T) {
	dir := t.TempDir()
	if _, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 0); err == nil {
		t.Fatal("AbreWIP com slotsMin 0 sobre arquivo ausente devolveu nil")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("erro = %v, quero algo que case com os.ErrNotExist", err)
	}
}

// TestOWIPReabertoRecuperaOEstadoGravado é o round-trip que a selagem depende:
// ela fecha o WIP e o reabre com slotsMin 0 para varrer as postings. Se o
// header não guardasse dirSlots e postCount, o reaberto leria o diretório com
// a máscara errada e o array de postings com o comprimento errado — e as duas
// falhas seriam silenciosas, não um erro.
//
// O corpus tem mais de um valor e mais de uma posting por valor de propósito:
// com um valor só, uma máscara errada ainda acharia o slot por acaso.
func TestOWIPReabertoRecuperaOEstadoGravado(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 64)
	if err != nil {
		t.Fatalf("AbreWIP: %v", err)
	}
	for _, p := range []struct {
		h   uint64
		row types.RowID
	}{{100, 30}, {100, 10}, {200, 20}} {
		if err := w.Set(p.h, p.row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Fecha(); err != nil {
		t.Fatalf("Fecha: %v", err)
	}

	reaberto, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 0)
	if err != nil {
		t.Fatalf("AbreWIP(slotsMin=0): %v", err)
	}
	defer fechaWIP(t, reaberto)
	if got := reaberto.Campo(); got != campoDeTeste {
		t.Errorf("Campo() = %d, quero %d", got, campoDeTeste)
	}
	if got := reaberto.Postings(); got != 3 {
		t.Errorf("Postings() = %d, quero 3", got)
	}
	if got := reaberto.Distintos(); got != 2 {
		t.Errorf("Distintos() = %d, quero 2", got)
	}
	if got := reaberto.Lookup(100); !slices.Equal(got, []types.RowID{10, 30}) {
		t.Errorf("Lookup(100) = %v, quero [10 30]", got)
	}
	if got := reaberto.Lookup(200); !slices.Equal(got, []types.RowID{20}) {
		t.Errorf("Lookup(200) = %v, quero [20]", got)
	}
}

// TestCaminhoEListaDoWIP trava o nome do arquivo e a enumeração por segmento.
// A selagem descobre os campos do segmento corrente por ListaWIP — não há
// metadado listando isso, seguindo a doutrina de "um metadado a menos é uma
// janela de crash a menos" (spec §5.3) —, então um nome fora do padrão faria a
// selagem perder um campo inteiro em silêncio.
func TestCaminhoEListaDoWIP(t *testing.T) {
	dir := t.TempDir()
	if got, want := segindex.CaminhoWIP(dir, 42, 7), dir+"/segment-00000042-00007.widx"; got != want {
		t.Errorf("CaminhoWIP = %q, quero %q", got, want)
	}
	for _, campo := range []types.FieldID{9, 3} {
		w, err := segindex.AbreWIP(dir, 42, campo, 16)
		if err != nil {
			t.Fatalf("AbreWIP(campo %d): %v", campo, err)
		}
		if err := w.Fecha(); err != nil {
			t.Fatalf("Fecha(campo %d): %v", campo, err)
		}
	}
	// Um WIP de OUTRO segmento, para provar que a lista filtra pelo segmento e
	// não só pela extensão.
	outro, err := segindex.AbreWIP(dir, 43, 5, 16)
	if err != nil {
		t.Fatalf("AbreWIP(outro segmento): %v", err)
	}
	if err := outro.Fecha(); err != nil {
		t.Fatalf("Fecha(outro segmento): %v", err)
	}

	campos, err := segindex.ListaWIP(dir, 42)
	if err != nil {
		t.Fatalf("ListaWIP: %v", err)
	}
	if !slices.Equal(campos, []types.FieldID{3, 9}) {
		t.Errorf("ListaWIP = %v, quero [3 9] (ordenado, só do segmento 42)", campos)
	}
}

// TestApagaRemoveOArquivoDoWIP: a selagem desliga os .widx depois de emitir o
// .sidx, e o Open descarta os órfãos (spec §2.5). Os dois passam por aqui, e um
// Apaga que fechasse sem remover deixaria o segmento com índice em duas formas.
func TestApagaRemoveOArquivoDoWIP(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 16)
	if err != nil {
		t.Fatalf("AbreWIP: %v", err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Apaga(); err != nil {
		t.Fatalf("Apaga: %v", err)
	}
	if _, err := os.Stat(segindex.CaminhoWIP(dir, segmentoDeTeste, campoDeTeste)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat após Apaga = %v, quero não-existe", err)
	}
	campos, err := segindex.ListaWIP(dir, segmentoDeTeste)
	if err != nil {
		t.Fatalf("ListaWIP: %v", err)
	}
	if len(campos) != 0 {
		t.Errorf("ListaWIP após Apaga = %v, quero vazio", campos)
	}
}
