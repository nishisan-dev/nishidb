package segindex_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// testField e testSegment são o (segmento, campo) de todos os WIP deste
// arquivo. Ficam nomeados porque o par é o que batiza o arquivo em disco, e um
// teste que trocasse um dos dois estaria falando de outro arquivo.
const (
	testSegment = uint32(1)
	testField   = types.FieldID(7)
)

// openTestWIPWithSlots abre um WIP novo com o número de slots pedido.
func openTestWIPWithSlots(t *testing.T, slots uint32) *segindex.WIP {
	t.Helper()
	w, err := segindex.OpenWIP(t.TempDir(), testSegment, testField, slots)
	if err != nil {
		t.Fatalf("OpenWIP: %v", err)
	}
	return w
}

// openTestWIP abre um WIP folgado o bastante para que NADA cresça durante o
// teste — nem o diretório, nem o array de postings.
//
// A folga é requisito dos gates de alocação, e não conforto: Region.Grow chama
// unix.Mmap, que mantém os mapeamentos vivos num map e portanto ALOCA. Um
// crescimento dentro da janela medida contaria a alocação do mmap como se fosse
// do Set. Os gates medem o regime, que é onde a ingestão vive; o crescimento é
// amortizado e tem gate próprio (TestOWIPCresceSemPerderPosting).
func openTestWIP(t *testing.T) *segindex.WIP {
	t.Helper()
	return openTestWIPWithSlots(t, 4096)
}

// closeWIP fecha o WIP reportando a falha.
//
// Existe porque `defer w.Close()` descartaria o erro em silêncio, e o gate de
// lint deste projeto conta esse descarte. Fechar é também onde o header ganha o
// postCount definitivo, então um erro aqui significa um arquivo que a Tarefa 4
// não conseguiria reabrir.
func closeWIP(t *testing.T, w *segindex.WIP) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Errorf("Close: %v", err)
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
	w := openTestWIP(t)
	defer closeWIP(t, w)
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
// rehasheia, e nenhum dos dois pode perder uma posting.
//
// O que o rehash NÃO faz é renumerar as postings — o que a cadeia guarda é o
// índice 1-based delas, e renumerar invalidaria todo elo `anterior` já gravado.
// Os BYTES delas se movem, com um memmove, porque o layout põe o diretório
// antes do array e dobrar o diretório empurra o array para frente. As duas
// afirmações são diferentes e só a segunda é sobre endereços; confundi-las é o
// caminho mais curto para um leitor concluir que o memmove de rehash() é
// código morto.
func TestOWIPCresceSemPerderPosting(t *testing.T) {
	w := openTestWIPWithSlots(t, 16) // força vários rehashes
	defer closeWIP(t, w)
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
	if got := w.DistinctValues(); got != n {
		t.Errorf("DistinctValues() = %d, quero %d", got, n)
	}
}

// TestOWIPAtendeLeitorConcorrenteDuranteOCrescimento é o gate do contrato de
// concorrência, e ele existe por causa do CRESCIMENTO, não do Set.
//
// A linearização (contagem e cabeça na mesma palavra) já protege o leitor
// contra o Set. Contra o rehash ela não protege nada: dirSlots e postBase
// mudam de valor e as postings mudam de ENDEREÇO, com um memmove que não é
// atômico em lugar nenhum. Um leitor que atravessasse essa janela leria a
// cadeia no lugar errado — e o resultado não seria um erro, seriam rowIds de
// lixo, que o executor revalidaria a um custo qualquer, ou rowIds PERDIDOS,
// que é o desfecho que este produto não tolera.
//
// O WIP é aberto APERTADO de propósito: com 16 slots iniciais, os 5.000 valores
// distintos do enchimento forçam nove rehashes e as 10.000 postings forçam dez
// crescimentos do array. Sem isso o teste não exercitaria nada.
//
// A cadeia observada recebe exatamente os rowIds 0, 1, 2, ... em ordem, então o
// resultado CORRETO em qualquer instante é o prefixo [0, k). É por isso que a
// asserção do leitor é tão dura: qualquer rowId fora de posição é um id que
// nunca foi inserido, e qualquer encurtamento é um id publicado que se perdeu.
//
// Sob -race o detector acrescenta a prova de que não há corrida de dados; as
// asserções valem nas duas rodadas.
func TestOWIPAtendeLeitorConcorrenteDuranteOCrescimento(t *testing.T) {
	const (
		observed = uint64(7)
		n        = 5000
	)
	w := openTestWIPWithSlots(t, 16)
	defer closeWIP(t, w)

	var (
		mu              sync.Mutex
		complaints      []string
		reads           atomic.Uint64
		lastSeen        atomic.Int64 // último comprimento observado pelo leitor
		wg              sync.WaitGroup
		done            = make(chan struct{})
		recordComplaint = func(format string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			if len(complaints) < 8 {
				complaints = append(complaints, fmt.Sprintf(format, args...))
			}
		}
	)
	// LIFO: fecha o canal, espera o leitor sair e SÓ ENTÃO o closeWIP desmonta
	// o mapeamento. Invertido, um Lookup em voo leria memória desmontada.
	defer wg.Wait()
	defer close(done)

	wg.Add(1)
	go func() {
		defer wg.Done()
		largest := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			rows := w.Lookup(observed)
			for i, r := range rows {
				if r != types.RowID(i) {
					recordComplaint("Lookup de %d elementos tem rowId %d na posição %d", len(rows), r, i)
					break
				}
			}
			if len(rows) < largest {
				recordComplaint("Lookup encolheu de %d para %d — uma posting publicada se perdeu", largest, len(rows))
			}
			largest = max(largest, len(rows))
			lastSeen.Store(int64(len(rows)))
			reads.Add(1)
		}
	}()

	var partial int64
	for i := range n {
		if err := w.Set(observed, types.RowID(i)); err != nil {
			t.Errorf("Set(observado, %d): %v", i, err)
			break
		}
		// Enchimento com valores DISTINTOS: é ele que faz o diretório dobrar.
		if err := w.Set(uint64(i)+1000, types.RowID(i)); err != nil {
			t.Errorf("Set(enchimento, %d): %v", i, err)
			break
		}
		if i == n/2 {
			// Barreira de NÃO-VACUIDADE: o escritor só passa daqui depois que o
			// leitor tiver enxergado um estado intermediário. Sem ela, a
			// sobreposição entre escrita e leitura seria decisão do
			// escalonador, e uma rodada em que o leitor só rodasse no fim
			// passaria sem nunca ter havido concorrência. Não trava: o leitor
			// só sai quando `pronto` fecha, e isso é depois daqui.
			// O prazo é o que separa "falha" de "trava": sem ele, um Lookup
			// que regredisse para devolver sempre nil faria este laço girar
			// até o timeout de dez minutos do go test — sinal caro e sem nome.
			// Com ele, a mesma regressão falha em segundos, nomeada.
			deadline := time.Now().Add(30 * time.Second)
			for {
				if partial = lastSeen.Load(); partial > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("o leitor nunca observou estado intermediário em 30s — o Lookup deixou de devolver as postings publicadas")
				}
				runtime.Gosched()
			}
		}
		if i%64 == 0 {
			runtime.Gosched()
		}
	}

	mu.Lock()
	recorded := slices.Clone(complaints)
	mu.Unlock()
	for _, q := range recorded {
		t.Error(q)
	}
	if reads.Load() == 0 {
		t.Error("o leitor não completou nenhum Lookup — o teste mediu um laço vazio")
	}
	if partial <= 0 || partial >= n {
		t.Errorf("o leitor observou o comprimento parcial %d, quero algo em (0, %d) — ele não atravessou o crescimento", partial, n)
	}
	if got := w.Lookup(observed); len(got) != n {
		t.Fatalf("Lookup final devolveu %d rowIds, quero %d", len(got), n)
	}
}

// TestValorAusenteNoWIPNaoDevolveNada separa "o valor não está aqui" de "está,
// e a resposta é vazia". No WIP as duas viram nil, e é isso que o chamador
// precisa poder confiar: um slot vazio na cadeia de sondagem NÃO pode fazer a
// busca continuar até dar a volta na tabela.
func TestValorAusenteNoWIPNaoDevolveNada(t *testing.T) {
	w := openTestWIP(t)
	defer closeWIP(t, w)
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
	w := openTestWIP(t)
	defer closeWIP(t, w)
	if got := w.Field(); got != testField {
		t.Errorf("Field() = %d, quero %d", got, testField)
	}
}

// TestParaCadaVarreTodasAsPostingsUmaVez é o que a selagem (Tarefa 4) consome
// para alimentar o Writer.
//
// A varredura NÃO promete ordem, e não se assere ordem nenhuma de propósito: o
// Writer já ordena e deduplica, e ordenar aqui seria a mesma ordenação em dois
// lugares — que divergem na primeira mudança de critério. O que ela promete é
// COBERTURA: cada posting do array visitada exatamente uma vez, senão a selagem
// perderia postings que o WIP tem.
//
// # O oráculo do total é w.Postings(), e NÃO len(postadas)
//
// A diferença separa duas coisas que a primeira versão deste teste confundia, e
// foi a disciplina de mutação que a expôs. "Quantas postings o array guarda" é
// política do Set — ele é append-only, então dois Set com o MESMO par (h, row)
// deixam duas entradas —, enquanto "ForEach visita todas elas, uma vez cada" é o
// contrato desta função. Asserir o total contra len(postadas) amarra o contrato
// da varredura à política de escrita, e o preço é que a suíte deixa de saber
// separar bug de regressão de desempenho: um Set que voltasse a varrer a cadeia
// para deduplicar — a forma exata do O(n²) que este pacote existe para matar —
// produz respostas IDÊNTICAS em Lookup e no .sidx selado, e mesmo assim deixava
// este teste vermelho ("ForEach visitou 5 postings, quero 6"). Ele tem de
// reprovar em TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento e em mais lugar
// nenhum.
//
// Contra w.Postings(), o teste continua pegando o defeito que lhe cabe, e mais
// apertado: o total é o que o WIP diz que tem no array, então uma cadeia
// interrompida cedo, um slot pulado ou uma visita repetida reprovam qualquer que
// seja a política de escrita. A duplicata deliberada do corpus fica, porque é ela
// que põe duas entradas na MESMA cadeia sob a política de hoje.
func TestParaCadaVarreTodasAsPostingsUmaVez(t *testing.T) {
	w := openTestWIP(t)
	defer closeWIP(t, w)
	posted := []struct {
		h   uint64
		row types.RowID
	}{
		{100, 50}, {100, 10}, {100, 10}, // duplicata deliberada
		{200, 7},
		{300, 42}, {300, 41},
	}
	for _, p := range posted {
		if err := w.Set(p.h, p.row); err != nil {
			t.Fatal(err)
		}
	}

	// Sem esta guarda o teste passaria com um WIP vazio: 0 == 0 é verdade, e a
	// asserção de cobertura mediria uma varredura que não roda.
	if w.Postings() == 0 {
		t.Fatal("o corpus não gravou posting nenhuma — a asserção de cobertura mede um laço vazio")
	}
	visits := map[[2]uint64]int{}
	total := 0
	if err := w.ForEach(func(h uint64, row types.RowID) {
		visits[[2]uint64{h, uint64(row)}]++
		total++
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if total != int(w.Postings()) {
		t.Errorf("ForEach visitou %d postings e o array tem %d", total, w.Postings())
	}
	want := map[[2]uint64]int{}
	for _, p := range posted {
		want[[2]uint64{p.h, uint64(p.row)}]++
	}
	// Cobertura é sobre o CONJUNTO de pares: cada par postado tem de aparecer, e
	// a multiplicidade de cada um é consequência da política do Set, medida pelo
	// total acima.
	for key := range want {
		if visits[key] == 0 {
			t.Errorf("ForEach não visitou (h=%d,row=%d)", key[0], key[1])
		}
	}
	for key := range visits {
		if want[key] == 0 {
			t.Errorf("ForEach inventou (h=%d,row=%d)", key[0], key[1])
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
	if _, err := segindex.OpenWIP(dir, testSegment, testField, 0); err == nil {
		t.Fatal("OpenWIP com slotsMin 0 sobre arquivo ausente devolveu nil")
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
	w, err := segindex.OpenWIP(dir, testSegment, testField, 64)
	if err != nil {
		t.Fatalf("OpenWIP: %v", err)
	}
	for _, p := range []struct {
		h   uint64
		row types.RowID
	}{{100, 30}, {100, 10}, {200, 20}} {
		if err := w.Set(p.h, p.row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := segindex.OpenWIP(dir, testSegment, testField, 0)
	if err != nil {
		t.Fatalf("OpenWIP(slotsMin=0): %v", err)
	}
	defer closeWIP(t, reopened)
	if got := reopened.Field(); got != testField {
		t.Errorf("Field() = %d, quero %d", got, testField)
	}
	if got := reopened.Postings(); got != 3 {
		t.Errorf("Postings() = %d, quero 3", got)
	}
	if got := reopened.DistinctValues(); got != 2 {
		t.Errorf("DistinctValues() = %d, quero 2", got)
	}
	if got := reopened.Lookup(100); !slices.Equal(got, []types.RowID{10, 30}) {
		t.Errorf("Lookup(100) = %v, quero [10 30]", got)
	}
	if got := reopened.Lookup(200); !slices.Equal(got, []types.RowID{20}) {
		t.Errorf("Lookup(200) = %v, quero [20]", got)
	}
}

// TestCaminhoEListaDoWIP trava o nome do arquivo e a enumeração por segmento.
// A selagem descobre os campos do segmento corrente por ListWIP — não há
// metadado listando isso, seguindo a doutrina de "um metadado a menos é uma
// janela de crash a menos" (spec §5.3) —, então um nome fora do padrão faria a
// selagem perder um campo inteiro em silêncio.
func TestCaminhoEListaDoWIP(t *testing.T) {
	dir := t.TempDir()
	if got, want := segindex.WIPPath(dir, 42, 7), dir+"/segment-00000042-00007.widx"; got != want {
		t.Errorf("WIPPath = %q, quero %q", got, want)
	}
	for _, field := range []types.FieldID{9, 3} {
		w, err := segindex.OpenWIP(dir, 42, field, 16)
		if err != nil {
			t.Fatalf("OpenWIP(campo %d): %v", field, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(campo %d): %v", field, err)
		}
	}
	// Um WIP de OUTRO segmento, para provar que a lista filtra pelo segmento e
	// não só pela extensão.
	other, err := segindex.OpenWIP(dir, 43, 5, 16)
	if err != nil {
		t.Fatalf("OpenWIP(outro segmento): %v", err)
	}
	if err := other.Close(); err != nil {
		t.Fatalf("Close(outro segmento): %v", err)
	}

	fields, err := segindex.ListWIP(dir, 42)
	if err != nil {
		t.Fatalf("ListWIP: %v", err)
	}
	if !slices.Equal(fields, []types.FieldID{3, 9}) {
		t.Errorf("ListWIP = %v, quero [3 9] (ordenado, só do segmento 42)", fields)
	}
}

// TestSegmentosComWIPEnumeraODiretorioInteiro: a abertura descarta o .widx de
// TODO segmento (spec §2.5), e o caso que obriga a pergunta a ser feita ao
// diretório — e não à lista de segmentos — é o do segmento já aposentado: ele
// não tem mais .dat, então nenhuma enumeração que parta dos dados o alcança, e
// sem esta função ele ficaria em disco para sempre.
func TestSegmentosComWIPEnumeraODiretorioInteiro(t *testing.T) {
	dir := t.TempDir()
	for _, pair := range []struct {
		seg   uint32
		campo types.FieldID
	}{{7, 3}, {7, 9}, {2, 1}} {
		w, err := segindex.OpenWIP(dir, pair.seg, pair.campo, 16)
		if err != nil {
			t.Fatalf("OpenWIP(%d, %d): %v", pair.seg, pair.campo, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%d, %d): %v", pair.seg, pair.campo, err)
		}
	}
	// Vizinhos que NÃO são .widx: o diretório de dados é compartilhado com os
	// segmentos e com a forma selada, e confundi-los faria a limpeza da
	// abertura remover dado.
	for _, name := range []string{"segment-00000007.dat", "segment-00000007.sidx", "segment-00000007.sidx.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	segs, err := segindex.SegmentsWithWIP(dir)
	if err != nil {
		t.Fatalf("SegmentsWithWIP: %v", err)
	}
	if !slices.Equal(segs, []uint32{2, 7}) {
		t.Errorf("SegmentsWithWIP = %v, quero [2 7] (ordenado e sem repetir o segmento de dois campos)", segs)
	}
}

// TestSegmentosComWIPRecusaNomeForaDoCanonico: quem enumera .widx REMOVE o que
// encontra, e a remoção reconstrói o caminho por WIPPath. Aceitar um nome com
// padding divergente faria a limpeza mirar um arquivo que não existe e reportar
// sucesso sobre o resíduo intacto.
func TestSegmentosComWIPRecusaNomeForaDoCanonico(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "segment-7-00003.widx"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := segindex.SegmentsWithWIP(dir); !errors.Is(err, segindex.ErrFormat) {
		t.Errorf("SegmentsWithWIP = %v, quero ErrFormat", err)
	}
}

// TestApagaRemoveOArquivoDoWIP: a selagem desliga os .widx depois de emitir o
// .sidx, e o Open descarta os órfãos (spec §2.5). Os dois passam por aqui, e um
// Remove que fechasse sem remover deixaria o segmento com índice em duas
// formas.
func TestApagaRemoveOArquivoDoWIP(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, testSegment, testField, 16)
	if err != nil {
		t.Fatalf("OpenWIP: %v", err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(segindex.WIPPath(dir, testSegment, testField)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat após Remove = %v, quero não-existe", err)
	}
	fields, err := segindex.ListWIP(dir, testSegment)
	if err != nil {
		t.Fatalf("ListWIP: %v", err)
	}
	if len(fields) != 0 {
		t.Errorf("ListWIP após Remove = %v, quero vazio", fields)
	}
}

// Offsets do header do WIP — escritos aqui, e não emprestados do pacote, pela
// mesma razão do bloco equivalente de sealed_test.go: quem mutila um formato de
// disco tem de saber onde está pondo a mão. Um teste que derivasse os offsets
// das constantes do pacote passaria a mutilar o lugar errado no dia em que o
// layout mudasse, e continuaria verde.
const (
	offMagicWIP       = 0  // 4 bytes
	offVersionInWIP   = 4  // u8
	offFieldInWIP     = 6  // u16
	offDirSlotsInWIP  = 8  // u32
	offPostCountInWIP = 12 // u32
)

// writeTestWIP emite um .widx bem-formado — um campo, dois valores, três
// postings — e devolve o diretório e os bytes do arquivo, para os testes que
// precisam estragá-lo.
func writeTestWIP(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, testSegment, testField, 16)
	if err != nil {
		t.Fatalf("OpenWIP: %v", err)
	}
	for _, p := range []struct {
		h   uint64
		row types.RowID
	}{{100, 30}, {100, 10}, {200, 20}} {
		if err := w.Set(p.h, p.row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	b, err := os.ReadFile(segindex.WIPPath(dir, testSegment, testField))
	if err != nil {
		t.Fatal(err)
	}
	return dir, b
}

// TestWIPRecusaHeaderInvalido cobre as recusas de FORMATO da abertura.
//
// O irmão selado tem o gate equivalente desde a Tarefa 2, e a ausência dele
// aqui deixava sem prova a afirmação mais load-bearing do godoc de newWIP: a
// conferência do fieldID, que impede um arquivo renomeado de responder pelo
// campo errado. Um Lookup sobre o campo errado devolve linhas erradas sem
// sintoma nenhum — não é um erro que apareça, é um resultado que mente.
//
// As mutilações são CIRÚRGICAS, um guarda por caso: escrever 0xFF em tudo
// provaria só que o guarda mais externo funciona.
func TestWIPRecusaHeaderInvalido(t *testing.T) {
	cases := map[string]func(b []byte){
		"magic trocado": func(b []byte) { copy(b[offMagicWIP:offMagicWIP+4], "XXXX") },
		"versão desconhecida": func(b []byte) {
			b[offVersionInWIP] = segindex.WIPVersion + 1
		},
		"campo divergente do nome do arquivo": func(b []byte) {
			// O nome diz testField e o header passa a dizer outro. É o caso
			// do arquivo renomeado, e o único desfecho tolerável é a recusa.
			binary.LittleEndian.PutUint16(b[offFieldInWIP:offFieldInWIP+2], uint16(testField)+1)
		},
		"dirSlots zero": func(b []byte) {
			// Máscara dirSlots-1 viraria 0xFFFFFFFF e a sondagem andaria fora
			// do diretório.
			binary.LittleEndian.PutUint32(b[offDirSlotsInWIP:offDirSlotsInWIP+4], 0)
		},
		"dirSlots não é potência de dois": func(b []byte) {
			binary.LittleEndian.PutUint32(b[offDirSlotsInWIP:offDirSlotsInWIP+4], 24)
		},
		"postCount maior que o arquivo": func(b []byte) {
			binary.LittleEndian.PutUint32(b[offPostCountInWIP:offPostCountInWIP+4], 1<<20)
		},
		"cabeça de cadeia além do postCount": func(b []byte) {
			// O caso do .widx sobrevivente de crash: o header só ganha o
			// postCount definitivo no Close, então a contagem fica atrasada e
			// as cadeias apontam além dela. Adotado assim, o próximo Set
			// sobrescreveria postings vivas em silêncio.
			binary.LittleEndian.PutUint32(b[offPostCountInWIP:offPostCountInWIP+4], 1)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			dir, original := writeTestWIP(t)
			b := append([]byte(nil), original...)
			corrupt(b)
			if err := os.WriteFile(segindex.WIPPath(dir, testSegment, testField), b, 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := segindex.OpenWIP(dir, testSegment, testField, 0)
			if err == nil {
				_ = w.Close()
				t.Fatal("OpenWIP aceitou o header mutilado")
			}
			if !errors.Is(err, segindex.ErrFormat) {
				t.Errorf("erro = %v, quero ErrFormat", err)
			}
		})
	}
}

// TestListaWIPRecusaFieldIDIlegivel: o espaço de nomes `segment-NNNNNNNN-*.widx`
// é deste pacote, e um arquivo ali que não saiu daqui significa que outra coisa
// escreveu no diretório do dataset. Pular em silêncio faria a selagem perder um
// campo inteiro — o segmento seria selado sem a seção dele e responderia zero
// linhas onde tem linhas.
func TestListaWIPRecusaFieldIDIlegivel(t *testing.T) {
	dir := t.TempDir()
	intruder := filepath.Join(dir, fmt.Sprintf("segment-%08d-abcde.widx", testSegment))
	if err := os.WriteFile(intruder, []byte("nada"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := segindex.ListWIP(dir, testSegment); !errors.Is(err, segindex.ErrFormat) {
		t.Errorf("ListWIP = %v, quero ErrFormat", err)
	}
}

// TestUsarOWIPAposFecharPanicaEmVezDeFalharMudo trava a rede do nível de baixo.
//
// Region.Close zera data e size, então o writeHeader de um segundo Close não
// daria erro: daria um pânico de bounds vindo de DENTRO do mmap, sem nome de
// método e sem nome de arquivo — e o Lookup leria um mapeamento desmontado. O
// pânico nomeado troca isso por uma falha que aponta para a linha certa, como
// TestLookupAposFechaPanicaEmVezDeLerMemoriaDesmontada já faz no selado.
func TestUsarOWIPAposFecharPanicaEmVezDeFalharMudo(t *testing.T) {
	operations := map[string]func(w *segindex.WIP){
		"Set":     func(w *segindex.WIP) { _ = w.Set(100, 1) },
		"Lookup":  func(w *segindex.WIP) { _ = w.Lookup(100) },
		"ForEach": func(w *segindex.WIP) { _ = w.ForEach(func(uint64, types.RowID) {}) },
		"Close":   func(w *segindex.WIP) { _ = w.Close() },
	}
	for name, op := range operations {
		t.Run(name, func(t *testing.T) {
			w := openTestWIPWithSlots(t, 16)
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			defer func() {
				if recover() == nil {
					t.Errorf("%s após Close deveria entrar em pânico", name)
				}
			}()
			op(w)
		})
	}
}

// TestApagaDepoisDeFecharNaoDevolveErro é o encadeamento que a selagem faz:
// emite o .sidx, fecha o .widx e o apaga.
//
// Remove é a operação TERMINAL, e é a única que aceita ser chamada depois do
// Close. Um Remove que recusasse região já fechada devolveria à selagem o erro
// de "descritor já fechado" — que ela leria como falha de uma selagem que deu
// certo. Chamar Remove duas vezes também não é erro: o estado desejado é o
// mesmo.
func TestApagaDepoisDeFecharNaoDevolveErro(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, testSegment, testField, 16)
	if err != nil {
		t.Fatalf("OpenWIP: %v", err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := w.Remove(); err != nil {
		t.Fatalf("Remove após Close: %v — a selagem leria isso como falha", err)
	}
	if err := w.Remove(); err != nil {
		t.Errorf("Remove repetido: %v, quero nil", err)
	}
	if _, err := os.Stat(segindex.WIPPath(dir, testSegment, testField)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat após Remove = %v, quero não-existe", err)
	}
}

// TestSetComHashZeroPanica: 0 é o sentinela de slot vazio. Um Set com hash 0
// ocuparia um slot que todo leitor continua lendo como vazio, incrementaria os
// distintos e faria a posting sumir em SILÊNCIO — falso negativo permanente.
//
// A segurança vem hoje de primary.HashKey nunca devolver 0 nem 1, mas isso é
// convenção de OUTRO pacote; o pânico é o que a transforma em invariante deste.
func TestSetComHashZeroPanica(t *testing.T) {
	w := openTestWIPWithSlots(t, 16)
	defer closeWIP(t, w)
	defer func() {
		if recover() == nil {
			t.Error("Set com hash 0 deveria entrar em pânico")
		}
	}()
	_ = w.Set(0, 1)
}
