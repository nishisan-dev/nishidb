package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// localizacaoDaRow devolve o (seg, off) que a location da row nomeia AGORA.
// É a mesma leitura que decide liveness no §55 — "a location ainda aponta
// para este record" —, e o teste a faz pelo Get do rowindex, que já resolve
// overlay, lote em aplicação e mmap. Reimplementá-la sobre o mmap deixaria o
// teste cego justamente para o commit point que ainda está no overlay.
func localizacaoDaRow(t *testing.T, s *Store, rowID types.RowID) (uint32, uint32) {
	t.Helper()
	packed, ok := s.locs.Get(rowID)
	if !ok {
		t.Fatalf("row %d sem location", rowID)
	}
	seg, off, _ := rowindex.Unpack(packed)
	return seg, off
}

// recordEm lê o record físico gravado em (seg, off) pelo MESMO Reader que a
// varredura do §54 usará: o copy-forward não inventa um segundo caminho de
// leitura, e o teste também não.
func recordEm(t *testing.T, s *Store, seg, off uint32) types.Record {
	t.Helper()
	rec, _, err := s.segR.ReadRecord(seg, off, nil)
	if err != nil {
		t.Fatalf("ReadRecord(%d, %d): %v", seg, off, err)
	}
	return rec
}

// tallyDoSegmento lê (liveRecords, liveBytes) de um segmento sob o mutex do
// writer, pelo mesmo motivo do somaDosTallies: segTally é writer-only e ler
// mapa Go sem sincronizar é fatal error do runtime, não corrida benigna.
func tallyDoSegmento(s *Store, seg uint32) (liveRecords, liveBytes uint64) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	t := s.segTally[seg]
	if t == nil {
		return 0, 0
	}
	return t.liveRecords, t.liveBytes
}

// rotacionaAlemDe apenda até o segmento corrente deixar de ser `seg`, e
// devolve quantos records isso custou. Existe porque o §53 D6 nunca elege o
// segmento corrente como candidato — compactar o segmento em que se está
// apendando é copiar dentro de si mesmo —, então o record de origem de um
// teste honesto tem de estar num segmento já SELADO.
func rotacionaAlemDe(t *testing.T, s *Store, seg uint32) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if s.Stats().CurrentSegmentID != seg {
			return
		}
		k := fmt.Sprintf("cf-enche-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	t.Fatalf("o segmento %d continua corrente após 200 records: a montagem não sela o segmento de origem", seg)
}

// TestCopyForwardNaoRessuscitaPayloadAntigoSobUpdateConcorrente é o teste que
// justifica a existência do reapendaSeVivo em vez de um Put comum. Se a
// verificação de liveness e o append não estiverem na MESMA seção crítica do
// writer (§53-55, decisão D3 do plano), um update chegado no meio perde para
// a cópia: o payload ANTIGO é publicado DEPOIS do novo e o dataset volta no
// tempo — sem erro, sem log e sem nada que a recovery consiga distinguir de
// uma escrita legítima, porque a cópia é durável e o CRC bate.
//
// A montagem antecipa o update de propósito: ele já entrou quando a cópia é
// tentada, então a location deixou de nomear (seg, off) e a recusa é a única
// resposta correta.
func TestCopyForwardNaoRessuscitaPayloadAntigoSobUpdateConcorrente(t *testing.T) {
	s := openStore(t, Options{})
	const key = "cf-corrida"
	antigo := statusPayload(t, s, key, "antigo", 1)
	rowID, _ := mustPut(t, s, key, 1, antigo, nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	// O record é lido ANTES do update, como a varredura do §54 o leria: ela
	// varre o arquivo, não o índice, e não tem como saber que a row migrou.
	rec := recordEm(t, s, origSeg, origOff)

	prev := mustGetAud(t, s, key)
	novo := statusPayload(t, s, key, "novo", 2)
	mustPut(t, s, key, 2, novo, &prev)

	copiado, n, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if copiado {
		t.Fatal("a cópia foi aceita depois do update: o payload antigo venceria o novo")
	}
	if n != 0 {
		t.Errorf("bytes copiados = %d na recusa, queria 0 — a Task 5 soma este número para decidir o progresso da varredura", n)
	}
	if seg, off := localizacaoDaRow(t, s, rowID); seg == origSeg && off == origOff {
		t.Fatalf("a location voltou a nomear a origem (%d, %d): a cópia republicou por cima do update", origSeg, origOff)
	}
	if got := mustGetAud(t, s, key); !bytes.Equal(got.Payload, novo) || got.Sequence != 2 {
		t.Errorf("o dataset voltou no tempo: seq=%d, payload igual ao antigo=%v",
			got.Sequence, bytes.Equal(got.Payload, antigo))
	}
}

// TestCopyForwardRepublicaALocationSemInflarALiveness fixa o lado positivo,
// sem o qual um reapendaSeVivo que recusasse SEMPRE passaria no teste da
// corrida e a compaction nunca esvaziaria segmento nenhum.
//
// As duas asserções de contabilidade são o ponto: o copy-forward MOVE bytes
// vivos de um segmento para outro, então o agregado do Stats não pode mudar,
// e o segmento de origem tem de perder exatamente o record copiado — é essa
// baixa que faz o candidato caminhar para o tally zerado do §53. Um
// applyRecord chamado com hadPrevious=false somaria o record novo sem
// descontar o antigo, e a nishidb_garbage_bytes passaria a encolher a cada
// compaction enquanto o disco cresce.
func TestCopyForwardRepublicaALocationSemInflarALiveness(t *testing.T) {
	// Segmento minúsculo força a rotação que sela o segmento de origem.
	s := openStore(t, Options{SegmentSize: 400})
	const key = "cf-alvo"
	payload := statusPayload(t, s, key, "Start", 1)
	rowID, _ := mustPut(t, s, key, 1, payload, nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	rotacionaAlemDe(t, s, origSeg)

	recsAntes, bytesAntes := tallyDoSegmento(s, origSeg)
	vivoAntes := s.Stats().LiveBytesUpperBound
	rec := recordEm(t, s, origSeg, origOff)

	copiado, n, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if !copiado {
		t.Fatal("a location ainda nomeava o record e a cópia foi recusada: nenhum segmento chegaria ao tally zerado")
	}
	if want := bytesDoRecord(key, payload); uint64(n) != want {
		t.Errorf("bytes copiados = %d, queria %d (header+key+payload)", n, want)
	}

	seg, off := localizacaoDaRow(t, s, rowID)
	if seg == origSeg && off == origOff {
		t.Fatalf("a location continua nomeando (%d, %d): o segmento de origem nunca ficaria elegível à aposentadoria", seg, off)
	}
	got := mustGetAud(t, s, key)
	if got.RowID != rowID || got.Sequence != 1 || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("a cópia mudou o estado da row: rowID=%d seq=%d payload igual=%v",
			got.RowID, got.Sequence, bytes.Equal(got.Payload, payload))
	}

	if vivoDepois := s.Stats().LiveBytesUpperBound; vivoDepois != vivoAntes {
		t.Errorf("LiveBytesUpperBound = %d após o copy-forward, era %d — mover um record vivo de segmento não cria nem destrói liveness",
			vivoDepois, vivoAntes)
	}
	recsDepois, bytesDepois := tallyDoSegmento(s, origSeg)
	if recsDepois != recsAntes-1 || bytesDepois != bytesAntes-uint64(n) {
		t.Errorf("tally da origem = (%d records, %d bytes), queria (%d, %d) — a baixa do record copiado não foi descontada",
			recsDepois, bytesDepois, recsAntes-1, bytesAntes-uint64(n))
	}
	exigeEspelhoIgualAosTallies(t, s, "após o copy-forward")

	// O detector da Fase A′ roda no início do checkpoint e é a aferição de
	// produção do invariante dos espelhos: se o copy-forward mexesse nos mapas
	// por fora deles, o WARN sairia a cada compaction.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if div := s.EspelhosDivergentesForTest(); div != 0 {
		t.Errorf("o checkpoint achou os espelhos divergentes %d vez(es) após o copy-forward", div)
	}
}

// TestCopyForwardConcorrenteComOWriterNuncaRegrideOValor põe a ingestão e a
// compaction em goroutines separadas, que é como a Fase B vai rodar em
// produção (a goroutine da Task 7 ao lado do consumidor Kafka). O que se
// afirma não depende de quem chegou primeiro: se a cópia venceu, o update
// entrou depois e governa; se o update venceu, a cópia foi recusada. Nas duas
// ordens o valor final é o NOVO — e é essa a propriedade que a seção crítica
// única compra.
//
// Roda sob -race porque é ali que a alternativa rejeitada pela D3 aparece:
// verificar a location fora do wmu e apendar depois não é só corrida lógica,
// é acesso concorrente aos buffers e ao offset do writer.
func TestCopyForwardConcorrenteComOWriterNuncaRegrideOValor(t *testing.T) {
	s := openStore(t, Options{})
	const n = 32
	keys := make([]string, n)
	origSeg := make([]uint32, n)
	origOff := make([]uint32, n)
	recs := make([]types.Record, n)
	novos := make([][]byte, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("cf-conc-%03d", i)
		antigo := statusPayload(t, s, keys[i], "antigo", int64(i))
		rowID, _ := mustPut(t, s, keys[i], 1, antigo, nil)
		origSeg[i], origOff[i] = localizacaoDaRow(t, s, rowID)
		recs[i] = recordEm(t, s, origSeg[i], origOff[i])
		// Os payloads do update nascem ANTES das goroutines: statusPayload
		// cria entradas no dicionário, e criar entrada é escrita — deixá-la
		// para dentro da corrida testaria o dicionário, não o copy-forward.
		novos[i] = statusPayload(t, s, keys[i], "novo", int64(i))
	}

	var wg sync.WaitGroup
	var errPut, errCopia error
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range keys {
			if _, _, err := s.Put([]byte(keys[i]), 2, 0, novos[i], nil); err != nil {
				errPut = fmt.Errorf("Put(%q): %w", keys[i], err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range keys {
			if _, _, err := s.reapendaSeVivo(origSeg[i], origOff[i], recs[i]); err != nil {
				errCopia = fmt.Errorf("reapendaSeVivo(%q): %w", keys[i], err)
				return
			}
		}
	}()
	wg.Wait()
	if errPut != nil || errCopia != nil {
		t.Fatalf("ingestão: %v; compaction: %v", errPut, errCopia)
	}

	for i := range keys {
		got := mustGetAud(t, s, keys[i])
		if got.Sequence != 2 || !bytes.Equal(got.Payload, novos[i]) {
			t.Fatalf("%s: seq=%d com payload do update=%v — a cópia venceu o update e o dataset voltou no tempo",
				keys[i], got.Sequence, bytes.Equal(got.Payload, novos[i]))
		}
	}
	exigeEspelhoIgualAosTallies(t, s, "após a corrida entre ingestão e compaction")
}
