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
		observado = uint64(7)
		n         = 5000
	)
	w := abreWIPDeTesteComSlots(t, 16)
	defer fechaWIP(t, w)

	var (
		mu       sync.Mutex
		queixas  []string
		lidas    atomic.Uint64
		visto    atomic.Int64 // último comprimento observado pelo leitor
		wg       sync.WaitGroup
		pronto   = make(chan struct{})
		anotaUma = func(formato string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			if len(queixas) < 8 {
				queixas = append(queixas, fmt.Sprintf(formato, args...))
			}
		}
	)
	// LIFO: fecha o canal, espera o leitor sair e SÓ ENTÃO o fechaWIP desmonta
	// o mapeamento. Invertido, um Lookup em voo leria memória desmontada.
	defer wg.Wait()
	defer close(pronto)

	wg.Add(1)
	go func() {
		defer wg.Done()
		maior := 0
		for {
			select {
			case <-pronto:
				return
			default:
			}
			rows := w.Lookup(observado)
			for i, r := range rows {
				if r != types.RowID(i) {
					anotaUma("Lookup de %d elementos tem rowId %d na posição %d", len(rows), r, i)
					break
				}
			}
			if len(rows) < maior {
				anotaUma("Lookup encolheu de %d para %d — uma posting publicada se perdeu", maior, len(rows))
			}
			maior = max(maior, len(rows))
			visto.Store(int64(len(rows)))
			lidas.Add(1)
		}
	}()

	var parcial int64
	for i := range n {
		if err := w.Set(observado, types.RowID(i)); err != nil {
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
			limite := time.Now().Add(30 * time.Second)
			for {
				if parcial = visto.Load(); parcial > 0 {
					break
				}
				if time.Now().After(limite) {
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
	registradas := slices.Clone(queixas)
	mu.Unlock()
	for _, q := range registradas {
		t.Error(q)
	}
	if lidas.Load() == 0 {
		t.Error("o leitor não completou nenhum Lookup — o teste mediu um laço vazio")
	}
	if parcial <= 0 || parcial >= n {
		t.Errorf("o leitor observou o comprimento parcial %d, quero algo em (0, %d) — ele não atravessou o crescimento", parcial, n)
	}
	if got := w.Lookup(observado); len(got) != n {
		t.Fatalf("Lookup final devolveu %d rowIds, quero %d", len(got), n)
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

// TestSegmentosComWIPEnumeraODiretorioInteiro: a abertura descarta o .widx de
// TODO segmento (spec §2.5), e o caso que obriga a pergunta a ser feita ao
// diretório — e não à lista de segmentos — é o do segmento já aposentado: ele
// não tem mais .dat, então nenhuma enumeração que parta dos dados o alcança, e
// sem esta função ele ficaria em disco para sempre.
func TestSegmentosComWIPEnumeraODiretorioInteiro(t *testing.T) {
	dir := t.TempDir()
	for _, par := range []struct {
		seg   uint32
		campo types.FieldID
	}{{7, 3}, {7, 9}, {2, 1}} {
		w, err := segindex.AbreWIP(dir, par.seg, par.campo, 16)
		if err != nil {
			t.Fatalf("AbreWIP(%d, %d): %v", par.seg, par.campo, err)
		}
		if err := w.Fecha(); err != nil {
			t.Fatalf("Fecha(%d, %d): %v", par.seg, par.campo, err)
		}
	}
	// Vizinhos que NÃO são .widx: o diretório de dados é compartilhado com os
	// segmentos e com a forma selada, e confundi-los faria a limpeza da
	// abertura remover dado.
	for _, nome := range []string{"segment-00000007.dat", "segment-00000007.sidx", "segment-00000007.sidx.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, nome), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	segs, err := segindex.SegmentosComWIP(dir)
	if err != nil {
		t.Fatalf("SegmentosComWIP: %v", err)
	}
	if !slices.Equal(segs, []uint32{2, 7}) {
		t.Errorf("SegmentosComWIP = %v, quero [2 7] (ordenado e sem repetir o segmento de dois campos)", segs)
	}
}

// TestSegmentosComWIPRecusaNomeForaDoCanonico: quem enumera .widx REMOVE o que
// encontra, e a remoção reconstrói o caminho por CaminhoWIP. Aceitar um nome com
// padding divergente faria a limpeza mirar um arquivo que não existe e reportar
// sucesso sobre o resíduo intacto.
func TestSegmentosComWIPRecusaNomeForaDoCanonico(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "segment-7-00003.widx"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := segindex.SegmentosComWIP(dir); !errors.Is(err, segindex.ErrFormato) {
		t.Errorf("SegmentosComWIP = %v, quero ErrFormato", err)
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

// Offsets do header do WIP — escritos aqui, e não emprestados do pacote, pela
// mesma razão do bloco equivalente de selado_test.go: quem mutila um formato de
// disco tem de saber onde está pondo a mão. Um teste que derivasse os offsets
// das constantes do pacote passaria a mutilar o lugar errado no dia em que o
// layout mudasse, e continuaria verde.
const (
	offMagicWIP     = 0  // 4 bytes
	offVersaoNoWIP  = 4  // u8
	offCampoNoWIP   = 6  // u16
	offDirSlotsNoW  = 8  // u32
	offPostCountNoW = 12 // u32
)

// escreveWIPDeTeste emite um .widx bem-formado — um campo, dois valores, três
// postings — e devolve o diretório e os bytes do arquivo, para os testes que
// precisam estragá-lo.
func escreveWIPDeTeste(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	w, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 16)
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
	b, err := os.ReadFile(segindex.CaminhoWIP(dir, segmentoDeTeste, campoDeTeste))
	if err != nil {
		t.Fatal(err)
	}
	return dir, b
}

// TestWIPRecusaHeaderInvalido cobre as recusas de FORMATO da abertura.
//
// O irmão selado tem o gate equivalente desde a Tarefa 2, e a ausência dele
// aqui deixava sem prova a afirmação mais load-bearing do godoc de montaWIP: a
// conferência do fieldID, que impede um arquivo renomeado de responder pelo
// campo errado. Um Lookup sobre o campo errado devolve linhas erradas sem
// sintoma nenhum — não é um erro que apareça, é um resultado que mente.
//
// As mutilações são CIRÚRGICAS, um guarda por caso: escrever 0xFF em tudo
// provaria só que o guarda mais externo funciona.
func TestWIPRecusaHeaderInvalido(t *testing.T) {
	casos := map[string]func(b []byte){
		"magic trocado": func(b []byte) { copy(b[offMagicWIP:offMagicWIP+4], "XXXX") },
		"versão desconhecida": func(b []byte) {
			b[offVersaoNoWIP] = segindex.VersaoWIP + 1
		},
		"campo divergente do nome do arquivo": func(b []byte) {
			// O nome diz campoDeTeste e o header passa a dizer outro. É o caso
			// do arquivo renomeado, e o único desfecho tolerável é a recusa.
			binary.LittleEndian.PutUint16(b[offCampoNoWIP:offCampoNoWIP+2], uint16(campoDeTeste)+1)
		},
		"dirSlots zero": func(b []byte) {
			// Máscara dirSlots-1 viraria 0xFFFFFFFF e a sondagem andaria fora
			// do diretório.
			binary.LittleEndian.PutUint32(b[offDirSlotsNoW:offDirSlotsNoW+4], 0)
		},
		"dirSlots não é potência de dois": func(b []byte) {
			binary.LittleEndian.PutUint32(b[offDirSlotsNoW:offDirSlotsNoW+4], 24)
		},
		"postCount maior que o arquivo": func(b []byte) {
			binary.LittleEndian.PutUint32(b[offPostCountNoW:offPostCountNoW+4], 1<<20)
		},
		"cabeça de cadeia além do postCount": func(b []byte) {
			// O caso do .widx sobrevivente de crash: o header só ganha o
			// postCount definitivo no Fecha, então a contagem fica atrasada e
			// as cadeias apontam além dela. Adotado assim, o próximo Set
			// sobrescreveria postings vivas em silêncio.
			binary.LittleEndian.PutUint32(b[offPostCountNoW:offPostCountNoW+4], 1)
		},
	}
	for nome, estraga := range casos {
		t.Run(nome, func(t *testing.T) {
			dir, original := escreveWIPDeTeste(t)
			b := append([]byte(nil), original...)
			estraga(b)
			if err := os.WriteFile(segindex.CaminhoWIP(dir, segmentoDeTeste, campoDeTeste), b, 0o644); err != nil {
				t.Fatal(err)
			}
			w, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 0)
			if err == nil {
				_ = w.Fecha()
				t.Fatal("AbreWIP aceitou o header mutilado")
			}
			if !errors.Is(err, segindex.ErrFormato) {
				t.Errorf("erro = %v, quero ErrFormato", err)
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
	intruso := filepath.Join(dir, fmt.Sprintf("segment-%08d-abcde.widx", segmentoDeTeste))
	if err := os.WriteFile(intruso, []byte("nada"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := segindex.ListaWIP(dir, segmentoDeTeste); !errors.Is(err, segindex.ErrFormato) {
		t.Errorf("ListaWIP = %v, quero ErrFormato", err)
	}
}

// TestUsarOWIPAposFecharPanicaEmVezDeFalharMudo trava a rede do nível de baixo.
//
// Region.Close zera data e size, então o gravaHeader de um segundo Fecha não
// daria erro: daria um pânico de bounds vindo de DENTRO do mmap, sem nome de
// método e sem nome de arquivo — e o Lookup leria um mapeamento desmontado. O
// pânico nomeado troca isso por uma falha que aponta para a linha certa, como
// TestLookupAposFechaPanicaEmVezDeLerMemoriaDesmontada já faz no selado.
func TestUsarOWIPAposFecharPanicaEmVezDeFalharMudo(t *testing.T) {
	operacoes := map[string]func(w *segindex.WIP){
		"Set":      func(w *segindex.WIP) { _ = w.Set(100, 1) },
		"Lookup":   func(w *segindex.WIP) { _ = w.Lookup(100) },
		"ParaCada": func(w *segindex.WIP) { _ = w.ParaCada(func(uint64, types.RowID) {}) },
		"Fecha":    func(w *segindex.WIP) { _ = w.Fecha() },
	}
	for nome, op := range operacoes {
		t.Run(nome, func(t *testing.T) {
			w := abreWIPDeTesteComSlots(t, 16)
			if err := w.Fecha(); err != nil {
				t.Fatalf("Fecha: %v", err)
			}
			defer func() {
				if recover() == nil {
					t.Errorf("%s após Fecha deveria entrar em pânico", nome)
				}
			}()
			op(w)
		})
	}
}

// TestApagaDepoisDeFecharNaoDevolveErro é o encadeamento que a selagem faz:
// emite o .sidx, fecha o .widx e o apaga.
//
// Apaga é a operação TERMINAL, e é a única que aceita ser chamada depois do
// Fecha. Um Apaga que recusasse região já fechada devolveria à selagem o erro
// de "descritor já fechado" — que ela leria como falha de uma selagem que deu
// certo. Chamar Apaga duas vezes também não é erro: o estado desejado é o
// mesmo.
func TestApagaDepoisDeFecharNaoDevolveErro(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.AbreWIP(dir, segmentoDeTeste, campoDeTeste, 16)
	if err != nil {
		t.Fatalf("AbreWIP: %v", err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Fecha(); err != nil {
		t.Fatalf("Fecha: %v", err)
	}
	if err := w.Apaga(); err != nil {
		t.Fatalf("Apaga após Fecha: %v — a selagem leria isso como falha", err)
	}
	if err := w.Apaga(); err != nil {
		t.Errorf("Apaga repetido: %v, quero nil", err)
	}
	if _, err := os.Stat(segindex.CaminhoWIP(dir, segmentoDeTeste, campoDeTeste)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stat após Apaga = %v, quero não-existe", err)
	}
}

// TestSetComHashZeroPanica: 0 é o sentinela de slot vazio. Um Set com hash 0
// ocuparia um slot que todo leitor continua lendo como vazio, incrementaria os
// distintos e faria a posting sumir em SILÊNCIO — falso negativo permanente.
//
// A segurança vem hoje de primary.HashKey nunca devolver 0 nem 1, mas isso é
// convenção de OUTRO pacote; o pânico é o que a transforma em invariante deste.
func TestSetComHashZeroPanica(t *testing.T) {
	w := abreWIPDeTesteComSlots(t, 16)
	defer fechaWIP(t, w)
	defer func() {
		if recover() == nil {
			t.Error("Set com hash 0 deveria entrar em pânico")
		}
	}()
	_ = w.Set(0, 1)
}
