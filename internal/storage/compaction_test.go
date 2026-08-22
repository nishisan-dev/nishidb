package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
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
// O -race NÃO é o oráculo deste teste, e a afirmação contrária (a de que o
// -race é o que prova a seção crítica) foi medida e é falsa: verificar a
// location fora do wmu e apendar depois não produz acesso de memória não
// sincronizado nenhum — locs.Get resolve overlay e mmap com sincronização
// própria —, e o detector não reportou DATA RACE em nenhuma das 8 execuções
// do defeito. O que ele faz é perturbar o escalonamento, o que muda a taxa de
// detecção sem mudar o que é detectável. A prova reprodutível está no
// TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica; este teste continua
// valendo pelo que ele cobre e nenhum outro cobre: a ingestão e a compaction
// no MESMO Store, cada uma pelo seu caminho de produção inteiro.
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

// flagForaDoTombstone é um bit de flag que o RECORD carrega e a LOCATION não.
// Ele existe porque as duas fontes de flags do copy-forward só divergem aí: o
// Put não valida o byte de flags e o grava inteiro no segmento, enquanto o
// applyRecord empacota apenas `a.flags&types.FlagTombstone` na location
// (store.go:909). Sem esse bit, escolher a location ou o record dá exatamente
// o mesmo resultado e nenhuma asserção consegue separar as duas fontes.
const flagForaDoTombstone uint8 = 0x02

// flagsDaLocation devolve o byte de flags que a location publica para a row —
// a mesma leitura que o §44 faz para decidir visibilidade.
func flagsDaLocation(t *testing.T, s *Store, rowID types.RowID) uint8 {
	t.Helper()
	packed, ok := s.locs.Get(rowID)
	if !ok {
		t.Fatalf("row %d sem location", rowID)
	}
	_, _, flags := rowindex.Unpack(packed)
	return flags
}

// TestCopyForwardGravaAsFlagsDaLocationENaoAsDoRecord fixa a escolha que o
// doc-comment do reapendaSeVivo declara e que, até aqui, nenhuma asserção
// sustentava: trocar `flags` por `rec.Flags` no BuildRecord deixava a suíte
// INTEIRA verde. Uma decisão de projeto que nenhum teste defende não é
// decisão, é acidente esperando a próxima leitura distraída.
//
// O oráculo é o byte de flags do record no destino. A origem carrega
// flagForaDoTombstone, a location carrega 0, e a cópia tem de sair com 0: a
// compaction é o único caminho de escrita que não recebe flags de um
// chamador, e ela não reescreve no segmento um bit que nenhuma location
// sustenta. O preço da escolha está aqui à vista — bits fora do
// FlagTombstone não sobrevivem à cópia —, e é exatamente por estar à vista
// que ele é uma escolha.
func TestCopyForwardGravaAsFlagsDaLocationENaoAsDoRecord(t *testing.T) {
	s := openStore(t, Options{SegmentSize: 400})
	const key = "cf-flags"
	payload := statusPayload(t, s, key, "Start", 1)
	rowID, _, err := s.Put([]byte(key), 1, flagForaDoTombstone, payload, nil)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	rec := recordEm(t, s, origSeg, origOff)
	// As duas guardas de montagem: sem a divergência entre record e location
	// o teste passaria a verde por acidente, e um teste que não distingue as
	// duas fontes é pior que teste nenhum — ele DECLARA a cobertura.
	if rec.Flags != flagForaDoTombstone {
		t.Fatalf("montagem: o record de origem gravou flags %#x, queria %#x", rec.Flags, flagForaDoTombstone)
	}
	if f := flagsDaLocation(t, s, rowID); f != 0 {
		t.Fatalf("montagem: a location guardou flags %#x, queria 0", f)
	}
	rotacionaAlemDe(t, s, origSeg)

	copiado, _, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if !copiado {
		t.Fatal("a location ainda nomeava o record e a cópia foi recusada")
	}
	novoSeg, novoOff := localizacaoDaRow(t, s, rowID)
	if copia := recordEm(t, s, novoSeg, novoOff); copia.Flags != 0 {
		t.Errorf("flags do record copiado = %#x, queria %#x — a cópia veio do record, não da location, e a compaction passou a publicar no segmento um bit que o índice nunca validou",
			copia.Flags, 0)
	}
}

// TestCopyForwardCopiaORecordTombstonadoSemRessuscitarARow fixa a decisão D2,
// que era normativa no plano, estava repetida no doc-comment da função e não
// tinha um único teste: um `if flags&types.FlagTombstone != 0 { return }`
// logo depois da verificação de liveness — a "otimização" mais plausível que
// existe, porque copiar o que já está morto parece desperdício — deixava a
// suíte INTEIRA verde.
//
// O estrago dessa otimização não aparece aqui e sim na Task 6: a location da
// row tombstonada continuaria nomeando o segmento aposentado, e o Open
// seguinte acharia location apontando para arquivo que não existe mais —
// ErrDatasetCorrupt, dataset que não abre. Liveness para o copy-forward é "a
// location nomeia este record" (§55), nunca "a row é visível".
//
// As duas asserções finais são o outro lado: copiar o tombstone não pode
// RESSUSCITAR a row. Ela sai da cópia tão morta quanto entrou.
func TestCopyForwardCopiaORecordTombstonadoSemRessuscitarARow(t *testing.T) {
	s := openStore(t, Options{SegmentSize: 400})
	const key = "cf-tumulo"
	rowID, _ := mustPut(t, s, key, 1, statusPayload(t, s, key, "Start", 1), nil)
	prev := mustGetAud(t, s, key)
	morto := statusPayload(t, s, key, "Stop", 2)
	if _, _, err := s.Put([]byte(key), 2, types.FlagTombstone, morto, &prev); err != nil {
		t.Fatalf("Put do tombstone: %v", err)
	}
	tumSeg, tumOff := localizacaoDaRow(t, s, rowID)
	rec := recordEm(t, s, tumSeg, tumOff)
	rotacionaAlemDe(t, s, tumSeg)

	copiado, n, err := s.reapendaSeVivo(tumSeg, tumOff, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if !copiado {
		t.Fatal("a row tombstonada NÃO foi copiada: a location seguiria nomeando o segmento que a aposentadoria remove, e o Open seguinte declararia o dataset corrompido")
	}
	if want := bytesDoRecord(key, morto); uint64(n) != want {
		t.Errorf("bytes copiados = %d, queria %d — a Task 5 soma este número para medir o progresso da varredura", n, want)
	}
	novoSeg, novoOff := localizacaoDaRow(t, s, rowID)
	if novoSeg == tumSeg && novoOff == tumOff {
		t.Fatalf("a location continua em (%d, %d): o segmento de origem nunca ficaria elegível à aposentadoria", tumSeg, tumOff)
	}
	if _, _, ok, err := s.GetByKey([]byte(key), nil); err != nil || ok {
		t.Fatalf("a cópia ressuscitou a row: GetByKey ok=%v, err=%v", ok, err)
	}
	if f := flagsDaLocation(t, s, rowID); f&types.FlagTombstone == 0 {
		t.Errorf("a location republicada perdeu o FlagTombstone (flags=%#x): a row voltaria a ser visível", f)
	}
	if copia := recordEm(t, s, novoSeg, novoOff); copia.Flags&types.FlagTombstone == 0 {
		t.Errorf("o record copiado perdeu o FlagTombstone (flags=%#x): o replay da recovery republicaria a row VIVA", copia.Flags)
	}
	exigeEspelhoIgualAosTallies(t, s, "após copiar um record tombstonado")
}

// apendaResiduoSemLocation grava no segmento corrente um record cuja location
// NUNCA foi publicada — o resíduo da janela de crash do §10.2, e a única
// precondição do ramo `!had` do reapendaSeVivo.
//
// Não dá para produzi-lo pelo Put: o Put apenda e publica a location na MESMA
// seção crítica, e é justamente por isso que o resíduo só nasce de um
// processo que morre no meio. O append direto é a forma honesta de plantar
// um estado que o produto não alcança sem morrer — mesma doutrina do
// CorrompeEspelhoParaTeste, que também escreve sob o wmu.
func apendaResiduoSemLocation(t *testing.T, s *Store, rowID types.RowID, key string, payload []byte) (uint32, uint32) {
	t.Helper()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	buf, err := encoding.BuildRecord(nil, rowID, 1, 0, []byte(key), payload)
	if err != nil {
		t.Fatalf("BuildRecord: %v", err)
	}
	seg, off, err := s.segW.Append(buf)
	if err != nil {
		t.Fatalf("Append do resíduo: %v", err)
	}
	s.curSegID.Store(seg)
	return seg, off
}

// TestCopyForwardRecusaResiduoSemLocationEmVezDeRessuscitarOEstado cobre o
// ramo `!had`, que carregava um comentário normativo forte e nenhuma
// asserção. A varredura do §54 lê o ARQUIVO, não o índice: ela vai topar com
// records que o commit point nunca publicou, e copiá-los seria a compaction
// promovendo a durável uma escrita que o dataset nunca aceitou.
//
// Registro honesto do que este teste NÃO prova: remover o ramo hoje não muda
// comportamento nenhum, porque packed==0 desempacota para segmento 0 e
// nenhum segmentID válido é 0 (rowindex.go:33 chega a entrar em panic com
// ele), de modo que a comparação `curSeg != seg` logo abaixo recusaria do
// mesmo jeito. O ramo é explícito por legibilidade e para não pendurar a
// recusa num detalhe do empacotamento; o que o teste fixa é o
// COMPORTAMENTO — resíduo não vira dado —, que precisa continuar valendo
// qualquer que seja o caminho pelo qual a função chega à recusa.
func TestCopyForwardRecusaResiduoSemLocationEmVezDeRessuscitarOEstado(t *testing.T) {
	s := openStore(t, Options{})
	const key = "cf-residuo"
	payload := statusPayload(t, s, key, "Start", 1)
	// rowID bem à frente do contador: nenhuma location foi escrita para ele,
	// e nenhum Put deste teste chega perto de alcançá-lo.
	rowID := types.RowID(s.nextRowID.Load() + 1000)
	seg, off := apendaResiduoSemLocation(t, s, rowID, key, payload)
	rec := recordEm(t, s, seg, off)

	copiado, n, err := s.reapendaSeVivo(seg, off, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if copiado || n != 0 {
		t.Fatalf("o resíduo foi copiado (copiado=%v, n=%d): a compaction publicaria o que o commit point nunca publicou", copiado, n)
	}
	if _, ok := s.locs.Get(rowID); ok {
		t.Error("a cópia criou location para a row do resíduo: o estado ressuscitou pela compaction")
	}
	if _, _, ok, err := s.GetByKey([]byte(key), nil); err != nil || ok {
		t.Errorf("a key do resíduo ficou visível: ok=%v, err=%v", ok, err)
	}
}

// TestCopyForwardRecusaComOWriterEmModoFalho cobre o primeiro ramo da função,
// que também estava sem teste. O modo falho é sticky porque depois do Append
// não existe rollback parcial (store.go:1012): apendar a cópia por cima de um
// writer que já falhou é escrever num segmento cujo estado ninguém sabe
// descrever. A varredura da Task 5 recebe este erro e para — é o sentinela
// que separa "pule este record" de "pare a compaction".
func TestCopyForwardRecusaComOWriterEmModoFalho(t *testing.T) {
	s := openStore(t, Options{BitmapFields: []types.FieldID{fldStatus}})
	const key = "cf-sticky"
	rowID, _ := mustPut(t, s, key, 1, statusPayload(t, s, key, "Start", 1), nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	rec := recordEm(t, s, origSeg, origOff)

	// Falha no Set dos bitmaps: erro DEPOIS do Append, que é exatamente o
	// que deixa o writer morto (§14.12).
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })
	if _, _, err := s.Put([]byte("cf-sticky-2"), 1, 0, statusPayload(t, s, "cf-sticky-2", "Start", 1), nil); err == nil {
		t.Fatal("montagem: o Put com Set falho deveria falhar e deixar o writer em modo falho")
	}
	s.SetSetErrHookForTest(nil)

	copiado, n, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if !errors.Is(err, ErrWriterFailed) {
		t.Fatalf("reapendaSeVivo com writer morto = %v, queria ErrWriterFailed", err)
	}
	if copiado || n != 0 {
		t.Errorf("a cópia foi aceita com o writer morto (copiado=%v, n=%d)", copiado, n)
	}
	if !strings.Contains(err.Error(), "set falho simulado") {
		t.Errorf("a causa original precisa sobreviver na cadeia para o operador saber o que matou o writer: %v", err)
	}
}

// TestCopyForwardRecusaDepoisDoClose cobre o entra(), o terceiro ramo sem
// teste. A goroutine de compaction da Task 7 vive AO LADO do Close: quem
// desliga o servidor não tem como esperar uma varredura de segmento inteiro
// terminar, e o entra() é o que faz a cópia em voo desistir em vez de apendar
// num segmento já fechado. Erro, não panic — a ingestão tem política para
// erro (§101) e não tem para panic.
func TestCopyForwardRecusaDepoisDoClose(t *testing.T) {
	s := openStore(t, Options{})
	const key = "cf-fechado"
	rowID, _ := mustPut(t, s, key, 1, statusPayload(t, s, key, "Start", 1), nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	rec := recordEm(t, s, origSeg, origOff)

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	copiado, n, err := s.reapendaSeVivo(origSeg, origOff, rec)
	if !errors.Is(err, ErrDatasetClosed) {
		t.Fatalf("reapendaSeVivo após o Close = %v, queria ErrDatasetClosed", err)
	}
	if copiado || n != 0 {
		t.Errorf("a cópia foi aceita depois do Close (copiado=%v, n=%d)", copiado, n)
	}
}

// copiaDaLocationCorrente copia a row a partir da location que ela nomeia
// AGORA, devolvendo o buffer de leitura para a próxima chamada. Repetir a
// cópia sobre a mesma row é o que aproxima o teste da varredura real: cada
// record copiado passa pelo mesmo BuildRecord, e é aí que o buffer do writer
// ou é reutilizado ou não é.
func copiaDaLocationCorrente(t *testing.T, s *Store, rowID types.RowID, buf []byte) []byte {
	t.Helper()
	packed, ok := s.locs.Get(rowID)
	if !ok {
		t.Fatalf("row %d sem location", rowID)
	}
	seg, off, _ := rowindex.Unpack(packed)
	rec, out, err := s.segR.ReadRecord(seg, off, buf)
	if err != nil {
		t.Fatalf("ReadRecord(%d, %d): %v", seg, off, err)
	}
	copiado, _, err := s.reapendaSeVivo(seg, off, rec)
	if err != nil {
		t.Fatalf("reapendaSeVivo: %v", err)
	}
	if !copiado {
		t.Fatal("a cópia da location corrente foi recusada: sem cópia não há alocação para medir")
	}
	return out
}

// TestCopyForwardReusaOBufferDoWriterEmVezDeAlocarPorRecord ancora o
// compactBuf, que era mecanismo de produção acrescentado ao Store com onze
// linhas de comentário invocando o §54 e ZERO asserção: trocá-lo por um
// buffer local — alocação a cada record copiado, exatamente o que o §54
// proíbe — deixava o pacote inteiro verde, e o campo poderia ser removido sem
// que nada acusasse.
//
// O oráculo é em BYTES ALOCADOS, não em contagem: TotalAlloc é cumulativo e o
// GC não o desconta, então ele mede o que a varredura de fato pediu ao
// alocador. O payload é grande de propósito — com 64 KiB por record, um
// buffer local aloca da ordem de um record por cópia e o teto de UM record
// para DEZESSEIS cópias separa as duas implementações por mais de uma ordem
// de grandeza, sem depender de contagem exata de alocações miúdas que a
// evolução do pacote pode legitimamente mudar.
//
// A cópia de aquecimento fica FORA da medição: o §54 proíbe alocar por
// record, não a primeira alocação do buffer — é ela que dá capacidade ao
// compactBuf, e cobrá-la seria exigir um buffer que nunca nasce.
func TestCopyForwardReusaOBufferDoWriterEmVezDeAlocarPorRecord(t *testing.T) {
	const (
		copias  = 16
		tamanho = 64 << 10
	)
	// Segmento folgado: rotacionar no meio da medição abriria arquivo novo e
	// mediria o alocador do segment, não o buffer do copy-forward.
	s := openStore(t, Options{SegmentSize: 64 << 20})
	const key = "cf-buffer"
	payload := statusPayload(t, s, key, strings.Repeat("x", tamanho), 1)
	rowID, _ := mustPut(t, s, key, 1, payload, nil)
	leitura := copiaDaLocationCorrente(t, s, rowID, nil)

	var antes, depois runtime.MemStats
	runtime.ReadMemStats(&antes)
	for i := 0; i < copias; i++ {
		leitura = copiaDaLocationCorrente(t, s, rowID, leitura)
	}
	runtime.ReadMemStats(&depois)

	gasto := depois.TotalAlloc - antes.TotalAlloc
	teto := bytesDoRecord(key, payload)
	if gasto > teto {
		t.Errorf("copiar %d records alocou %d bytes; o teto é %d (um record) — o buffer do writer deixou de ser reutilizado e a compaction voltou a alocar por record copiado (§54)",
			copias, gasto, teto)
	}
}

// payloadDaJanela é o payload das rodadas da janela: grande de propósito.
// O tamanho não é capricho — ele é o que ALARGA a seção crítica do Put
// (validação, BuildRecord, CRC e o write do Append acontecem todos com o wmu
// na mão), e é dentro dessa janela alargada que a leitura de location feita
// fora do lock precisa cair para o defeito aparecer. Com payload miúdo a
// seção crítica do Put dura menos que o caminho da goroutine de compaction
// até o Get, e o defeito passa despercebido em 199 de 200 rodadas — medido.
func payloadDaJanela(t *testing.T, s *Store, key, marca string, r int) []byte {
	t.Helper()
	return statusPayload(t, s, key, strings.Repeat(marca, 16<<10), int64(r))
}

// TestCopyForwardPerdeParaOUpdateQueEntraNaJanelaDaVerificacao põe a
// ingestão e a compaction na MESMA row, com largada alinhada, e repete. Ele
// existe porque o teste concorrente acima quase não abre a janela do §53-55:
// medido sobre o defeito (ler a location ANTES de pegar o wmu), ele sobrevive
// a ~84% das rodadas de `go test ./...` e a ~28% das rodadas com -race.
//
// Duas coisas mudam aqui, e as duas saíram de medição, não de intuição:
//
//  1. A largada é alinhada no sentido CERTO: a compaction espera a ingestão
//     anunciar que vai entrar e só então tenta copiar. Largar as duas juntas
//     de uma barreira comum não serve — a compaction ganha o lock primeiro em
//     200 de 200 rodadas e a janela nunca chega a abrir.
//  2. O payload é grande, para que a seção crítica do Put dure mais que o
//     caminho da outra goroutine até a leitura da location. Com payload
//     miúdo a detecção cai para 1 em 200 rodadas; com este, sobe para ~55%
//     por rodada, e 64 rodadas fecham a conta dentro de UMA execução do gate.
//
// O que este teste NÃO prova, e é honesto dizer: com GOMAXPROCS=1 ele não
// detecta nada (medido: 0 de 30), porque a goroutine da compaction nem chega
// a ser escalonada no meio do Put. Prova que depende de núcleo sobrando não é
// prova, e por isso o kill reprodutível do invariante mora no
// TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica. O valor deste aqui
// é outro e nenhum outro cobre: a corrida acontece entre os caminhos de
// PRODUÇÃO inteiros dos dois lados, sem nenhuma peça simulada pelo teste.
//
// A afirmação não depende da ordem: se a cópia venceu, o update entrou depois
// e governa; se o update venceu, a cópia foi recusada. Nas duas ordens o
// valor final é o NOVO, e é só isso que se pede.
func TestCopyForwardPerdeParaOUpdateQueEntraNaJanelaDaVerificacao(t *testing.T) {
	// Segmento folgado: a rotação embute fdatasync e alongaria a seção
	// crítica DEPOIS do commit point, que é o lado que não interessa.
	s := openStore(t, Options{SegmentSize: 256 << 20})
	const rodadas = 64
	for r := 0; r < rodadas; r++ {
		key := fmt.Sprintf("cf-janela-%03d", r)
		antigo := payloadDaJanela(t, s, key, "a", r)
		rowID, _ := mustPut(t, s, key, 1, antigo, nil)
		origSeg, origOff := localizacaoDaRow(t, s, rowID)
		// O record sai do arquivo ANTES da largada, como a varredura o leria:
		// ela varre o arquivo, não o índice, e não sabe que a row vai migrar.
		rec := recordEm(t, s, origSeg, origOff)
		novo := payloadDaJanela(t, s, key, "n", r)

		emVoo := make(chan struct{})
		var wg sync.WaitGroup
		var errPut, errCopia error
		wg.Add(2)
		go func() {
			defer wg.Done()
			close(emVoo)
			_, _, errPut = s.Put([]byte(key), 2, 0, novo, nil)
		}()
		go func() {
			defer wg.Done()
			<-emVoo
			// Um Gosched: com as duas goroutines no mesmo P, é ele que dá a
			// vez à ingestão para que ela já esteja DENTRO da seção crítica
			// quando a cópia for ler a location.
			runtime.Gosched()
			_, _, errCopia = s.reapendaSeVivo(origSeg, origOff, rec)
		}()
		wg.Wait()
		if errPut != nil || errCopia != nil {
			t.Fatalf("rodada %d: ingestão: %v; compaction: %v", r, errPut, errCopia)
		}

		got := mustGetAud(t, s, key)
		if got.Sequence != 2 || !bytes.Equal(got.Payload, novo) {
			t.Fatalf("rodada %d (%s): o valor corrente é seq=%d com o payload do update=%v — a cópia do record ANTIGO foi publicada DEPOIS do update e o dataset voltou no tempo, sem erro e com o CRC batendo",
				r, key, got.Sequence, bytes.Equal(got.Payload, novo))
		}
	}
	exigeEspelhoIgualAosTallies(t, s, "após as rodadas da janela")
}

// compactionParadaNoWmu bloqueia até a goroutine do copy-forward estar parada
// no s.wmu.Lock() de dentro do reapendaSeVivo, e devolve false se ela não
// chegar lá dentro do prazo.
//
// É leitura de stack, e não sleep, de propósito: o que o teste da barreira
// precisa saber é um FATO ("a compaction já está esperando o lock"), não uma
// aposta em quanto tempo o escalonador leva. Um sleep transformaria o oráculo
// determinístico de volta em probabilístico, que é exatamente o defeito que a
// revisão apontou no teste concorrente.
func compactionParadaNoWmu(prazo time.Duration) bool {
	return paradaNoWmuEm("reapendaSeVivo", prazo)
}

// paradaNoWmuEm é o instrumento por trás do helper acima, com a marca da
// função escolhida por quem chama: a barreira da Task 7 precisa do mesmo fato
// ("já está esperando o lock") sobre a SELEÇÃO de candidatos, e duplicar a
// leitura de stack faria as duas divergirem no primeiro ajuste.
func paradaNoWmuEm(marca string, prazo time.Duration) bool {
	buf := make([]byte, 1<<20)
	limite := time.Now().Add(prazo)
	for {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\ngoroutine ") {
			// As duas marcas na MESMA goroutine: parada no Lock, e parada
			// nele por dentro da função que interessa.
			if strings.Contains(g, marca) && strings.Contains(g, "sync.(*Mutex).Lock") {
				return true
			}
		}
		if time.Now().After(limite) {
			return false
		}
		runtime.Gosched()
	}
}

// publicaUpdateSobOWmu grava um update da row com o mutex do writer JÁ na mão
// de quem chama. É o núcleo do Put (store.go:828-857) — BuildRecord, Append e
// applyRecord, nesta ordem —, sem a validação de argumento e sem o lookup do
// estado anterior, que são justamente as partes que precisariam do lock que o
// teste está segurando.
//
// Chamar s.Put aqui seria deadlock, e é essa impossibilidade que dá ao teste
// da barreira o seu poder: para publicar um update ENQUANTO a compaction
// espera o lock, é preciso ser o writer.
func publicaUpdateSobOWmu(t *testing.T, s *Store, rowID types.RowID, key string, seq int64, prevPayload, payload []byte) {
	t.Helper()
	buf, err := encoding.BuildRecord(nil, rowID, seq, 0, []byte(key), payload)
	if err != nil {
		t.Fatalf("BuildRecord do update: %v", err)
	}
	seg, off, err := s.segW.Append(buf)
	if err != nil {
		t.Fatalf("Append do update: %v", err)
	}
	s.curSegID.Store(seg)
	if err := s.applyRecord(recordApply{
		rowID:       rowID,
		key:         []byte(key),
		seq:         seq,
		payload:     payload,
		prevPayload: prevPayload,
		hadPrevious: true,
		seg:         seg,
		off:         off,
		recLen:      len(buf),
	}); err != nil {
		t.Fatalf("applyRecord do update: %v", err)
	}
}

// TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica é o kill
// REPRODUTÍVEL do invariante normativo 1, e a razão de ele existir ao lado
// dos testes concorrentes é que aqueles medem o escalonador junto com o
// código: o defeito de ler a location fora do wmu sobrevive a ~84% das
// rodadas de `go test ./...`, a ~28% das rodadas com -race, e a 100% delas
// com GOMAXPROCS=1 — onde a goroutine da compaction nem chega a ser
// escalonada no meio do Put. Prova que só vale em máquina com núcleo
// sobrando não é prova.
//
// Aqui não há aposta nenhuma. O TESTE é o writer: ele segura o wmu, espera a
// compaction PARAR no Lock (fato lido da stack, não presumido de um sleep),
// publica o update com a compaction já parada e só então solta. A partir daí
// as duas implementações são distinguíveis por construção, sempre:
//
//   - lendo a location DENTRO da seção crítica, a compaction acorda depois do
//     update, vê a location nomeando outro lugar e recusa;
//   - lendo a location ANTES, ela acorda com um valor de antes do update na
//     mão, e publica a cópia do payload ANTIGO por cima do novo.
//
// O segundo caso é a perda de dado silenciosa do §53-55: durável, com CRC
// batendo, indistinguível de uma escrita legítima para a recovery.
func TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica(t *testing.T) {
	s := openStore(t, Options{})
	const key = "cf-barreira"
	antigo := statusPayload(t, s, key, "antigo", 1)
	rowID, _ := mustPut(t, s, key, 1, antigo, nil)
	origSeg, origOff := localizacaoDaRow(t, s, rowID)
	rec := recordEm(t, s, origSeg, origOff)
	novo := statusPayload(t, s, key, "novo", 2)

	s.wmu.Lock()
	var wg sync.WaitGroup
	var copiado bool
	var errCopia error
	wg.Add(1)
	go func() {
		defer wg.Done()
		copiado, _, errCopia = s.reapendaSeVivo(origSeg, origOff, rec)
	}()
	if !compactionParadaNoWmu(5 * time.Second) {
		s.wmu.Unlock()
		wg.Wait()
		t.Fatal("a compaction não chegou a esperar o wmu em 5s: a montagem não abre a janela que o teste mede")
	}
	publicaUpdateSobOWmu(t, s, rowID, key, 2, antigo, novo)
	s.wmu.Unlock()
	wg.Wait()

	if errCopia != nil {
		t.Fatalf("reapendaSeVivo: %v", errCopia)
	}
	if copiado {
		t.Error("a cópia foi ACEITA depois de um update que entrou enquanto ela esperava o lock: a liveness foi decidida com uma location lida fora da seção crítica")
	}
	if seg, off := localizacaoDaRow(t, s, rowID); seg == origSeg && off == origOff {
		t.Errorf("a location voltou a nomear a origem (%d, %d): a cópia republicou por cima do update", origSeg, origOff)
	}
	if got := mustGetAud(t, s, key); got.Sequence != 2 || !bytes.Equal(got.Payload, novo) {
		t.Errorf("o dataset voltou no tempo: seq=%d, payload igual ao do update=%v",
			got.Sequence, bytes.Equal(got.Payload, novo))
	}
	exigeEspelhoIgualAosTallies(t, s, "após a janela fechada pela seção crítica")
}

// segmentoCorrenteDoWriter lê onde o writer está de fato apendando. É a
// VERDADE contra a qual o espelho curSegID se confere — perguntar ao Stats
// seria perguntar ao próprio espelho.
func segmentoCorrenteDoWriter(s *Store) uint32 {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	seg, _ := s.segW.Current()
	return seg
}

// TestCopyForwardQueRotacionaAtualizaOSegmentoCorrentePublicado fecha a única
// linha do reapendaSeVivo que nenhum teste alcançava: o
// `s.curSegID.Store(novoSeg)` que segue o Append.
//
// Apagá-la deixava a suíte inteira verde, e o estrago não é cosmético. O
// curSegID é o espelho que `Stats().CurrentSegmentID` publica, e dele saem o
// `/v1/stats`, o `/metrics` e os comandos operacionais do §96. Diferente dos
// espelhos de liveness, ele NÃO é recarregado pela reconciliação do checkpoint
// nem conferido pelo conferaEspelhos: uma vez atrasado, fica atrasado. Num
// período de compaction com a ingestão parada — que é exatamente o cenário do
// §53-55 — o operador leria um segmento já SELADO como corrente por tempo
// indeterminado.
//
// O custo real chega na seleção de candidatos: a decisão D6 é "nunca eleger o
// segmento corrente", e é este campo que responde qual é ele. Com o espelho
// obsoleto, a compaction elegeria justamente o segmento em que o writer está
// apendando — copiar dentro de si mesmo, que é o que a D6 existe para impedir.
func TestCopyForwardQueRotacionaAtualizaOSegmentoCorrentePublicado(t *testing.T) {
	s := openStore(t, Options{SegmentSize: 400})
	const key = "cf-rotaciona"
	payload := statusPayload(t, s, key, "Start", 1)
	rowID, _ := mustPut(t, s, key, 1, payload, nil)

	inicial := segmentoCorrenteDoWriter(s)
	// Copiar a row para frente repetidamente enche o segmento corrente até o
	// Append rotacionar. Cada volta relê a location, porque a cópia anterior
	// fez a antiga deixar de nomear o record.
	var rotacionou bool
	for i := 0; i < 200 && !rotacionou; i++ {
		seg, off := localizacaoDaRow(t, s, rowID)
		rec := recordEm(t, s, seg, off)
		copiado, _, err := s.reapendaSeVivo(seg, off, rec)
		if err != nil {
			t.Fatalf("reapendaSeVivo na volta %d: %v", i, err)
		}
		if !copiado {
			t.Fatalf("a cópia foi recusada na volta %d com a location recém-lida", i)
		}
		rotacionou = segmentoCorrenteDoWriter(s) != inicial
	}
	if !rotacionou {
		t.Fatal("o copy-forward não rotacionou em 200 cópias: o teste não chegou a exercitar o espelho")
	}

	correnteDeFato := segmentoCorrenteDoWriter(s)
	if got := s.Stats().CurrentSegmentID; got != correnteDeFato {
		t.Errorf("CurrentSegmentID = %d e o writer está no %d: o copy-forward rotacionou e o espelho ficou para trás", got, correnteDeFato)
	}
	// A mentira não cicatriza. O checkpoint recarrega os espelhos de liveness
	// e NÃO o curSegID; sem esta segunda asserção alguém poderia supor que o
	// próximo checkpoint conserta, e operar em cima disso.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := s.Stats().CurrentSegmentID; got != correnteDeFato {
		t.Errorf("CurrentSegmentID = %d depois do checkpoint e o writer está no %d: este espelho não é reconciliado", got, correnteDeFato)
	}
}

// abreComResiduoOrfaoNoSegmento1 monta o dataset que a Task 5 precisa e que
// nenhum atalho produz: um segmento SELADO cujo tally está ACIMA do real por
// exatamente um record órfão, e um Store aberto por cima dele. Devolve também
// o diretório do dataset e o tamanho em bytes do órfão, que é o oráculo EXATO
// das duas asserções que interessam — quanto os decrementos deixariam para
// trás e quanto o espelho tem de descer quando a varredura zerar o tally.
//
// O resíduo é produzido pelo MECANISMO, não injetado no mapa: Put, checkpoint
// (que fixa o ponto S), update, crash entre os passos 3 e 4 do §10.2
// (locations duráveis, manifest atrás) e reabertura, onde o replay do tail
// reaplica o update sem conhecer o estado anterior e deixa o record de S
// contado como vivo para sempre. É o cenário que o
// TestResiduoDeLivenessDoReplayNaoConverge mede e declara PERMANENTE, e é a
// razão de o débito 38 existir.
//
// Inflar o tally à mão seria mais curto e provaria menos: a asserção passaria
// a valer sobre um estado que o produto não alcança, quando o que está em jogo
// é justamente o estado que ele alcança e do qual não sai por subtração.
func abreComResiduoOrfaoNoSegmento1(t *testing.T) (*Store, string, uint64) {
	t.Helper()
	dir := t.TempDir()
	opts := Options{Dir: dir, SegmentSize: 400, BitmapFields: []types.FieldID{fldStatus}}

	s := audOpen(t, opts)
	mustPut(t, s, "lv", 1, statusPayload(t, s, "lv", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint do ponto S: %v", err)
	}
	// Em S o segmento 1 tem UM record vivo, e é ele que vira o órfão: o
	// update abaixo o torna garbage sem que ninguém desconte.
	orfao := statsDoSegmento(t, dir).LiveBytes
	if orfao == 0 {
		t.Fatal("o manifest de S não contabilizou o record: sem órfão não há o que provar")
	}

	prev := mustGetAud(t, s, "lv")
	mustPut(t, s, "lv", 2, statusPayload(t, s, "lv", "Stop", 2), &prev)
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatalf("checkpoint parcial: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close antes da reabertura: %v", err)
	}

	s2 := audOpen(t, opts)
	t.Cleanup(func() { _ = s2.Close() })
	if got := s2.Stats().CurrentSegmentID; got != 1 {
		t.Fatalf("o cenário exige o resíduo no segmento 1, mas o writer reabriu no %d", got)
	}
	// A decisão D6 nunca elege o segmento corrente, então o candidato de um
	// teste honesto tem de estar SELADO antes de a varredura tocá-lo.
	rotacionaAlemDe(t, s2, 1)
	return s2, dir, orfao
}

// TestVarreduraCompletaZeraOTallyMesmoComResiduoOrfao ataca o débito 38 de
// frente. A pergunta que ele responde não é "a compaction esvazia o
// segmento?", é "por que o tally é ZERADO em vez de subtraído?".
//
// A resposta está na asserção do meio, e ela é aritmética: o que o segmento
// reivindicava como vivo MENOS o que a varredura de fato copiou é exatamente
// o órfão. Ou seja, os decrementos do applyRecord param nele e nunca alcançam
// zero, por mais completa que a varredura seja — o tally é limite superior
// por construção (débitos 38 e 45) e o record órfão não é nomeado por
// location nenhuma, logo nenhum decremento o encontra. Quem zera é a
// varredura, que viu TODOS os records e é a única leitura exata do segmento
// que existe no produto (§53-55, decisão D4).
//
// Sem a asserção do meio o teste seria vacuoso num dataset sem resíduo: com o
// tally exato, "zerar" e "subtrair" produzem o mesmo 0 e nenhum oráculo os
// separa.
func TestVarreduraCompletaZeraOTallyMesmoComResiduoOrfao(t *testing.T) {
	s, _, orfao := abreComResiduoOrfaoNoSegmento1(t)
	recsAntes, vivoAntes := tallyDoSegmento(s, 1)
	if recsAntes == 0 || vivoAntes <= orfao {
		t.Fatalf("montagem sem o que compactar: o segmento 1 reivindica %d records / %d bytes com um órfão de %d",
			recsAntes, vivoAntes, orfao)
	}

	copiados, lidos, escritos, err := s.compactaSegmento(context.Background(), 1)
	if err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}
	if copiados == 0 {
		t.Fatal("nenhum record copiado: a varredura não exercitou o copy-forward")
	}
	if escritos >= lidos {
		t.Errorf("a varredura leu %d bytes e escreveu %d: sem garbage deixado para trás a compaction não devolve disco nenhum",
			lidos, escritos)
	}
	if resto := vivoAntes - uint64(escritos); resto != orfao {
		t.Errorf("os decrementos parariam em %d bytes (reivindicados %d, copiados %d) e o órfão mede %d — "+
			"se estes números passaram a bater, o resíduo do replay foi corrigido e este teste perdeu o alvo",
			resto, vivoAntes, escritos, orfao)
	}

	recs, vivo := tallyDoSegmento(s, 1)
	if recs != 0 || vivo != 0 {
		t.Errorf("tally do segmento 1 = %d records / %d bytes após a varredura completa, queria 0/0: "+
			"a varredura tem autoridade sobre o segmento que ela leu inteiro (§53-55, D4)", recs, vivo)
	}
	// O dado não pode ter ido junto: o copy-forward move records vivos, não
	// os descarta.
	if got := mustGetAud(t, s, "lv"); got.Sequence != 2 {
		t.Errorf("a row copiada voltou para a seq %d; o update seguia sendo o estado corrente", got.Sequence)
	}
}

// TestZeramentoDoTallyDescontaOEspelhoNaMesmaSecaoCritica fixa o invariante 4
// da Fase B — toda escrita que um espelho atômico descreve ajusta o espelho,
// na MESMA seção crítica.
//
// O zeramento do tally é o único ponto da compaction que mexe no mapa por
// fora do applyRecord, e é por isso que ele precisa de teste próprio: o
// copy-forward passa pelo applyRecord, que já mantém os dois lados, mas esta
// linha não passa por lugar nenhum. Zerar só o mapa deixaria o liveBytesAll
// à frente dele — o /v1/stats e a nishidb_garbage_bytes publicando um vivo
// que o dataset não tem, a cada compaction.
//
// São duas asserções porque uma só não fecha. O oráculo EXATO (o espelho
// desce exatamente o órfão, nem mais nem menos) pega o desconto errado; a
// conferência do checkpoint seguinte pega o desconto ESQUECIDO, que é o
// mutante que passaria despercebido até alguém ler o WARN no log.
func TestZeramentoDoTallyDescontaOEspelhoNaMesmaSecaoCritica(t *testing.T) {
	s, _, orfao := abreComResiduoOrfaoNoSegmento1(t)
	divergenciasAntes := s.EspelhosDivergentesForTest()
	antes := s.Stats().LiveBytesUpperBound

	if _, _, _, err := s.compactaSegmento(context.Background(), 1); err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}

	depois := s.Stats().LiveBytesUpperBound
	if antes-depois != orfao {
		t.Errorf("LiveBytesUpperBound foi de %d para %d (queda de %d), e a queda tem de ser exatamente o órfão, %d: "+
			"o copy-forward move bytes vivos de um segmento para o outro sem mudar o agregado, então o que sobra "+
			"para o zeramento descontar é só o resíduo", antes, depois, antes-depois, orfao)
	}
	exigeEspelhoIgualAosTallies(t, s, "após o zeramento do tally pela varredura")

	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := s.EspelhosDivergentesForTest(); got != divergenciasAntes {
		t.Errorf("o checkpoint seguinte acusou %d divergências de espelho contra %d antes da compaction: "+
			"o mapa e o espelho saíram do zeramento discordando, e o detector está certo",
			got, divergenciasAntes)
	}
}

// TestVarreduraCanceladaNaoZeraOTally fixa a outra metade do invariante 2:
// completa E sem cancelamento. Uma varredura interrompida não viu todos os
// records, então não tem autoridade sobre nenhum deles — zerar ali diria que
// o segmento está vazio quando metade dele nunca foi lida, e a aposentadoria
// da Task 6 removeria o arquivo com as locations ainda apontando para dentro.
//
// O cancelamento chega pelo contexto e sai pelo erro do callback, que o
// ScanFrom propaga. É o caminho que a goroutine da Task 7 vai usar no Close:
// quem desliga o servidor não espera uma varredura de segmento inteiro
// terminar.
//
// Registro honesto do alcance: com o contexto já cancelado na entrada, a
// varredura para no PRIMEIRO record, então o que este teste prova é "não zera
// quando cancelada", não "preserva o trabalho parcial". Cancelar no meio
// exigiria um seam de produção que a tarefa não pede; o trabalho parcial já
// está coberto pelo teste da varredura que para em record corrompido, que
// interrompe a varredura DEPOIS de copiar.
func TestVarreduraCanceladaNaoZeraOTally(t *testing.T) {
	s, _, _ := abreComResiduoOrfaoNoSegmento1(t)
	recsAntes, vivoAntes := tallyDoSegmento(s, 1)

	ctx, cancela := context.WithCancel(context.Background())
	cancela()
	copiados, _, _, err := s.compactaSegmento(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("compactaSegmento com contexto cancelado devolveu %v, queria context.Canceled", err)
	}
	if copiados != 0 {
		t.Errorf("%d records copiados com o contexto cancelado na entrada: a verificação do ctx não é a primeira coisa do callback", copiados)
	}
	if recs, vivo := tallyDoSegmento(s, 1); recs != recsAntes || vivo != vivoAntes {
		t.Errorf("tally do segmento 1 = %d/%d após a varredura cancelada, queria %d/%d intactos",
			recs, vivo, recsAntes, vivoAntes)
	}
	exigeEspelhoIgualAosTallies(t, s, "após a varredura cancelada")
}

// TestVarreduraQueParaAntesDoFimDoArquivoNaoZeraOTally cobre o terceiro modo
// de varredura incompleta, e é o único que não vem de fora: o ScanFrom para
// no primeiro record inválido SEM erro, porque para o tail scan da recovery
// (§52) isso é o caminho normal — é assim que o EOF chega até lá. Num
// segmento SELADO não é: ele não cresce desde a rotação, então prefixo válido
// menor que o arquivo é corrupção real, e os records depois dela continuam
// sendo nomeados por locations que a varredura nunca visitou.
//
// Zerar o tally aqui daria à varredura uma autoridade que ela não tem, e o
// estrago só aterrissaria na Task 6: o arquivo seria aposentado e removido
// com locations vivas apontando para dentro dele, e o Open seguinte recusaria
// o dataset inteiro (ErrDatasetCorrupt em store_recovery.go). Uma corrupção
// de UM record viraria perda do segmento inteiro, causada pela compaction.
//
// A corrupção é plantada com o Store ABERTO de propósito: o recoverFrom já
// recusa abrir um dataset com corrupção no meio da cadeia
// (TestCorrupcaoNoMeioDaCadeiaEhFatalENaoTrunca), então o único jeito de a
// varredura topar com ela é o dano ter aparecido depois da abertura — que é
// exatamente o que bit rot é.
func TestVarreduraQueParaAntesDoFimDoArquivoNaoZeraOTally(t *testing.T) {
	s, dir, _ := abreComResiduoOrfaoNoSegmento1(t)
	recsAntes, vivoAntes := tallyDoSegmento(s, 1)

	// O último record do segmento selado: corrompê-lo deixa a varredura
	// copiar os anteriores e parar antes do fim do arquivo, que é a
	// precondição exata deste teste.
	caminho := audSegPath(dir, 1)
	alvo := fileSizeAud(t, caminho) - 8
	f, err := os.OpenFile(caminho, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("abrir o segmento para plantar a corrupção: %v", err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], alvo); err != nil {
		t.Fatalf("ler o byte a corromper: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], alvo); err != nil {
		t.Fatalf("gravar a corrupção: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("fechar o segmento corrompido: %v", err)
	}

	copiados, _, escritos, err := s.compactaSegmento(context.Background(), 1)
	if !errors.Is(err, ErrDatasetCorrupt) {
		t.Fatalf("compactaSegmento sobre segmento com record corrompido devolveu %v, queria ErrDatasetCorrupt", err)
	}
	if copiados == 0 {
		t.Fatal("a varredura parou antes de copiar qualquer coisa: o teste não chegou a exercitar a varredura parcial")
	}
	recs, vivo := tallyDoSegmento(s, 1)
	if recs == 0 && vivo == 0 {
		t.Fatal("o tally foi zerado por uma varredura que parou no meio do arquivo: " +
			"a autoridade do D4 vem de ter lido o segmento INTEIRO, e ela não leu")
	}
	// E o trabalho parcial não se perde nem se conta duas vezes: o que a
	// varredura alcançou a copiar saiu do tally da origem pelo applyRecord, e
	// só isso.
	if recs != recsAntes-copiados || vivo != vivoAntes-uint64(escritos) {
		t.Errorf("tally do segmento 1 = %d records / %d bytes após copiar %d records (%d bytes) de %d/%d; "+
			"queria %d/%d — só a baixa do que foi de fato copiado",
			recs, vivo, copiados, escritos, recsAntes, vivoAntes, recsAntes-copiados, vivoAntes-uint64(escritos))
	}
	exigeEspelhoIgualAosTallies(t, s, "após a varredura que parou em record corrompido")
}

// TestZeramentoDoTallyRecusaDepoisDoCloseSemRecordParaCopiar cobre o entra()
// do ZERAMENTO — o único trecho da compactaSegmento que roda fora da seção de
// vida do reapendaSeVivo e que nenhuma asserção defendia. Apagar as quatro
// linhas do guard deixava a suíte inteira verde, e guard de ciclo de vida que
// a suíte não defende é mecanismo que o próximo refator remove sem notar.
//
// A porta é um segmento SEM record, e é deliberada. Com record, o dataset
// fechado já é recusado antes, pelo entra() do reapendaSeVivo (fixado pelo
// TestCopyForwardRecusaDepoisDoClose): o erro sairia idêntico com e sem o
// guard do zeramento, e o teste seria vacuoso — mediria o guard errado. Num
// segmento que só tem o header a varredura termina COMPLETA, sem erro e com o
// prefixo válido igual ao arquivo, então o gate de corrupção deixa passar e o
// zeramento é de fato alcançado. O único jeito de a chamada não devolver
// sucesso, aí, é o guard.
//
// E "sucesso" é o ponto, não o valor do tally: sem o guard, compactaSegmento
// devolve nil para um dataset FECHADO — e é sobre esse nil que a
// aposentadoria da Task 6 vai decidir renomear e depois remover o arquivo. A
// recusa é da mesma família da do copy-forward: erro, não panic, porque a
// ingestão tem política para erro (§101) e não tem para panic.
//
// ALCANCE, registrado honestamente: o que fica fixado é "dataset fechado
// ANTES da chamada". A outra janela do guard — o Close aterrissando ENTRE o
// fim da varredura e o zeramento — não é alcançável pela suíte sem um seam de
// produção que a Task 5 não pede, e é redundante por projeto: pela decisão D7
// o Close cancela e AGUARDA a goroutine de compaction antes de tomar o
// vida.Lock().
func TestZeramentoDoTallyRecusaDepoisDoCloseSemRecordParaCopiar(t *testing.T) {
	s := openStore(t, Options{})
	const seg = 1
	if got := s.Stats().CurrentSegmentID; got != seg {
		t.Fatalf("montagem: o writer abriu no segmento %d e o vazio deste teste é o %d", got, seg)
	}

	// A precondição é o teste inteiro: se houvesse UM record, o erro viria do
	// entra() do reapendaSeVivo e esta função não estaria medindo nada.
	var visitados int
	fimValido, err := s.segR.ScanFrom(seg, segment.HeaderSize, func(uint32, types.Record) error {
		visitados++
		return nil
	})
	if err != nil {
		t.Fatalf("montagem: varredura do segmento recém-criado: %v", err)
	}
	if visitados != 0 {
		t.Fatalf("montagem: o segmento %d já tem %d records, e a porta deste teste é justamente não ter nenhum", seg, visitados)
	}
	tamanho, err := s.tamanhoDoSegmento(seg)
	if err != nil {
		t.Fatalf("montagem: tamanho do segmento %d: %v", seg, err)
	}
	if fimValido != tamanho {
		t.Fatalf("montagem: prefixo válido de %d bytes contra arquivo de %d — assim o gate de corrupção responderia antes do guard",
			fimValido, tamanho)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	copiados, lidos, escritos, err := s.compactaSegmento(context.Background(), seg)
	if !errors.Is(err, ErrDatasetClosed) {
		t.Fatalf("compactaSegmento sobre dataset fechado devolveu %v, queria ErrDatasetClosed: "+
			"varredura sem record termina completa e chega ao zeramento, e zerar tally de dataset fechado "+
			"é reportar sucesso sobre um dataset que já foi embora", err)
	}
	if copiados != 0 || lidos != 0 || escritos != 0 {
		t.Errorf("a varredura de um segmento vazio contabilizou %d records / %d lidos / %d escritos, queria 0/0/0",
			copiados, lidos, escritos)
	}
}

// TestVarreduraContaComoLidoOArquivoInteiroMenosOHeader fixa a aritmética dos
// números que a compactaSegmento devolve, porque a Task 8 vai publicá-los
// como série de compaction e comentário não é oráculo.
//
// `lidos` soma RecordHeaderSize + key + payload a partir de
// segment.HeaderSize: vale o tamanho do arquivo MENOS o header do segmento,
// não o arquivo inteiro. São 64 bytes de viés por segmento compactado, e
// métrica com viés constante é pior que métrica ausente — ela parece medir. É
// a mesma família do defeito dos tetos do lint: instrumento que devolve
// resultado sem ter medido o que diz medir.
//
// O oráculo é o ARQUIVO em disco, e não uma segunda soma sobre os mesmos
// records: repetir a soma provaria apenas que ela é determinística. E o
// arquivo é medido de novo no fim porque a Task 5 ainda não devolve disco
// nenhum — quem aposenta o segmento é a Task 6, e até lá o que a varredura
// leu continua todo lá.
func TestVarreduraContaComoLidoOArquivoInteiroMenosOHeader(t *testing.T) {
	s, dir, _ := abreComResiduoOrfaoNoSegmento1(t)
	tamanho := fileSizeAud(t, audSegPath(dir, 1))

	copiados, lidos, escritos, err := s.compactaSegmento(context.Background(), 1)
	if err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}
	if copiados == 0 {
		t.Fatal("nenhum record copiado: a varredura não exercitou o copy-forward")
	}
	if lidos != tamanho-segment.HeaderSize {
		t.Errorf("lidos = %d bytes num arquivo de %d bytes cujo header ocupa %d: a varredura começa DEPOIS do "+
			"header, e é esta conta — arquivo menos header — que a série de compaction da Task 8 vai publicar",
			lidos, tamanho, int64(segment.HeaderSize))
	}
	if escritos <= 0 || escritos >= lidos {
		t.Errorf("a varredura leu %d bytes e escreveu %d: o destino recebe só a parte viva, e a distância entre "+
			"os dois é o disco que a aposentadoria da Task 6 devolve", lidos, escritos)
	}
	if depois := fileSizeAud(t, audSegPath(dir, 1)); depois != tamanho {
		t.Errorf("o segmento de origem foi de %d para %d bytes: a Task 5 copia para frente e não encolhe arquivo "+
			"nenhum, então a conta acima seguiria valendo", tamanho, depois)
	}
}

// ---------------------------------------------------------------------------
// Task 6 — a aposentadoria do segmento
// ---------------------------------------------------------------------------

// discoDosSegmentosPorOsStat soma o tamanho dos arquivos de segmento
// perguntando ao FILESYSTEM, um os.Stat por arquivo. Não usa Stats(), e a
// distinção é a tarefa inteira: Stats é a coisa sob teste, e um oráculo que a
// consultasse aprovaria uma compaction que só mexeu na contabilidade — o
// disco que o §86 dá 2 a 3 dias para encher não é o que o painel diz, é o que
// o filesystem cobra.
//
// DIVERGÊNCIA declarada em relação ao item 1 do critério de aceite, que pede
// "o tamanho do diretório": esta medida vê só `segment-*.dat`. Ela é mais
// AFIADA, não mais frouxa — os índices são mmap de tamanho fixo que a
// compaction não move, e somá-los junto dilui o sinal num denominador que não
// participa do que se mede (no corpus deste teste são ~16 MB de índice contra
// ~90 KB de segmento). É por isso que o piso da metade é aferido aqui, e só
// aqui. O diretório INTEIRO, que é o texto literal do item, é medido pelo
// discoDoDatasetInteiroPorOsStat logo abaixo e também tem de cair — sem isso,
// a escolha de escopo ficaria por conta do implementador, e escopo escolhido
// por quem é medido é exatamente o defeito que o §92 persegue.
func discoDosSegmentosPorOsStat(t *testing.T, dir string) int64 {
	t.Helper()
	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob dos segmentos: %v", err)
	}
	var total int64
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		total += fi.Size()
	}
	return total
}

// discoDoDatasetInteiroPorOsStat é o item 1 do critério de aceite ao pé da
// letra: TODO arquivo sob o diretório do dataset, um os.Stat por arquivo —
// segmentos, índices, dicionários, manifest. É o número que o `du` do
// operador devolveria, e o único capaz de responder à pergunta que o §86 faz
// de verdade ("o volume para de encher?"), porque a compaction que encolhesse
// os segmentos inflando um índice não teria devolvido nada a ninguém.
//
// Ele não substitui a medida por segmento, e o motivo está no godoc dela: os
// índices são mmap de tamanho fixo e respondem pela quase totalidade destes
// bytes, então a queda aqui é percentualmente pequena mesmo quando a
// compaction fez todo o trabalho que podia. Por isso a asserção sobre este
// número é de DIREÇÃO — tem de cair —, e a de magnitude fica com os
// segmentos.
func discoDoDatasetInteiroPorOsStat(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(caminho string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := os.Stat(caminho)
		if err != nil {
			return err
		}
		total += fi.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("percorrer o diretório do dataset %s: %v", dir, err)
	}
	return total
}

// seladosEmDisco fotografa os segmentos que a compaction pode eleger AGORA
// (decisão D6: o corrente nunca entra). A fotografia é tirada antes do
// primeiro ciclo de propósito — o copy-forward empurra o segmento corrente
// para frente e SELA novos arquivos, e um laço que relesse a lista a cada
// volta perseguiria as próprias cópias sem nunca terminar.
func seladosEmDisco(t *testing.T, dir string, corrente uint32) []uint32 {
	t.Helper()
	refs, err := segment.ListSegments(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("listar segmentos: %v", err)
	}
	var ids []uint32
	for _, ref := range refs {
		if ref.ID != corrente {
			ids = append(ids, ref.ID)
		}
	}
	return ids
}

func chaveDoCorpus(i int) string           { return fmt.Sprintf("e2e-%04d", i) }
func sequenceDoCorpus(rodada, i int) int64 { return int64(rodada*10000 + i) }

func payloadDoCorpus(t *testing.T, s *Store, key string, rodada int) []byte {
	t.Helper()
	return statusPayload(t, s, key, fmt.Sprintf("R%02d", rodada), int64(rodada))
}

// exigeCorpusNaUltimaRodada confere TODA key do corpus, uma a uma, com
// sequence e payload. Amostragem não serve: o modo de falha desta tarefa é a
// row que ficou apontando para um arquivo que a aposentadoria removeu, e ela
// é uma entre dezenas — um sorteio a perde e devolve verde.
func exigeCorpusNaUltimaRodada(t *testing.T, s *Store, keys, rodada int, quando string) {
	t.Helper()
	for i := 0; i < keys; i++ {
		k := chaveDoCorpus(i)
		rec, _, ok, err := s.GetByKey([]byte(k), nil)
		if err != nil || !ok {
			t.Fatalf("%s: a row %q sumiu (ok=%v, err=%v)", quando, k, ok, err)
		}
		if want := sequenceDoCorpus(rodada, i); rec.Sequence != want {
			t.Fatalf("%s: a row %q voltou com sequence %d, queria %d", quando, k, rec.Sequence, want)
		}
		if want := payloadDoCorpus(t, s, k, rodada); !bytes.Equal(rec.Payload, want) {
			t.Fatalf("%s: o payload da row %q não é o da última rodada", quando, k)
		}
	}
}

// TestAposentadoriaSaiDosMapasNoMesmoInstanteEmQueSaiDoDiretorio é o teste da
// ORDEM, e ele afirma sobre o DISCO em cada passo — não sobre a sequência de
// nomes que o próprio trace produz. A distinção importa: um trace que só
// confere a si mesmo aprova qualquer implementação que continue chamando os
// mesmos rótulos na mesma ordem, inclusive uma que remova o arquivo antes de
// o manifest deixar de nomeá-lo.
//
// São três invariantes num teste só, porque as três descrevem o mesmo
// instante:
//
//  1. Invariante 3 da Fase B: o Checkpoint das locations acontece ANTES do
//     rename, e o segundo Checkpoint ANTES do unlink. Entre o rename e esse
//     segundo checkpoint o manifest AINDA nomeia o segmento — é a janela que
//     o Open repara, e ela existe de propósito.
//  2. Invariante 4: a poda dos mapas e o ajuste dos espelhos acontecem na
//     mesma seção crítica, e no MESMO passo do rename. Sem observar aqui
//     dentro, a poda é invisível: o segundo Checkpoint reconstrói o segSize
//     do ListSegments e poda o tally por conta própria, de modo que o estado
//     FINAL é idêntico com e sem ela. O que a poda decide é o que o Stats
//     publica DURANTE a janela — e é justamente aí que ele estaria mentindo.
//  3. O arquivo só some do disco no fim, depois de o manifest concordar.
func TestAposentadoriaSaiDosMapasNoMesmoInstanteEmQueSaiDoDiretorio(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const n = 400
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("apo-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	const alvo = 1
	if _, _, _, err := s.compactaSegmento(context.Background(), alvo); err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}
	// O checkpoint ancora o "antes": o segSize só se move no checkpoint, e sem
	// este a comparação de StorageBytes mediria o atraso da série em vez da
	// aposentadoria.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint de base: %v", err)
	}
	canonico := audSegPath(dir, alvo)
	info, err := os.Stat(canonico)
	if err != nil {
		t.Fatalf("stat do segmento a aposentar: %v", err)
	}
	antes := s.Stats()
	divergenciasAntes := s.EspelhosDivergentesForTest()

	type foto struct {
		passo      string
		canonico   bool
		aposentado bool
		listado    bool
		segmentos  uint32
		disco      int64
	}
	var fotos []foto
	s.SetAposentadoriaTraceForTest(func(passo string) {
		st := s.Stats()
		m, err := checkpoint.Load(audManifestPath(dir))
		if err != nil {
			t.Fatalf("carregar o manifest no passo %q: %v", passo, err)
		}
		listado := false
		for _, si := range m.Segments {
			if si.ID == alvo {
				listado = true
			}
		}
		_, errCanonico := os.Stat(canonico)
		_, errAposentado := os.Stat(canonico + sufixoAposentadoNoTeste)
		fotos = append(fotos, foto{
			passo:      passo,
			canonico:   errCanonico == nil,
			aposentado: errAposentado == nil,
			listado:    listado,
			segmentos:  st.SegmentsTotal,
			disco:      st.StorageBytes,
		})
	})
	defer s.SetAposentadoriaTraceForTest(nil)

	if err := s.aposentaSegmento(alvo); err != nil {
		t.Fatalf("aposentaSegmento: %v", err)
	}

	querido := []foto{
		{"checkpointDasLocations", true, false, true, antes.SegmentsTotal, antes.StorageBytes},
		{"rename", false, true, true, antes.SegmentsTotal - 1, antes.StorageBytes - info.Size()},
		{"checkpointDoManifest", false, true, false, antes.SegmentsTotal - 1, antes.StorageBytes - info.Size()},
		{"unlink", false, false, false, antes.SegmentsTotal - 1, antes.StorageBytes - info.Size()},
	}
	if len(fotos) != len(querido) {
		t.Fatalf("a aposentadoria passou por %d passos (%+v), queria %d", len(fotos), fotos, len(querido))
	}
	for i, q := range querido {
		if fotos[i] != q {
			t.Errorf("passo %d:\n  obtido %+v\n  queria %+v", i, fotos[i], q)
		}
	}

	// E o estado final tem de sobreviver ao checkpoint seguinte sem que a
	// conferência de espelhos acuse nada: a reconciliação se cura sozinha, e é
	// por isso que ela não serve de oráculo da poda — mas se a poda tivesse
	// descontado o valor ERRADO, é aqui que apareceria.
	exigeEspelhoIgualAosTallies(t, s, "após a aposentadoria do segmento 1")
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint pós-aposentadoria: %v", err)
	}
	if got := s.EspelhosDivergentesForTest(); got != divergenciasAntes {
		t.Errorf("o checkpoint seguinte acusou %d divergências contra %d antes: a poda e os espelhos saíram discordando",
			got, divergenciasAntes)
	}
}

// TestCompactionDevolveDiscoSemPerderNenhumaRow é o CRITÉRIO DE ACEITE da
// Task 6, e ele existe porque suíte verde não prova nada numa tarefa que
// APAGA arquivo. As duas coisas que precisam ser demonstradas são que o disco
// VOLTA e que nenhuma row se perde, e nenhuma das duas é observável num teste
// que afirme sobre contadores.
//
// O corpus é um key set FIXO reescrito em muitas rodadas, que é a forma do
// dataset real do §86: 500 ev/s de updates sobre as mesmas sessões, 30-43 GB
// escritos por dia contra 5,6-8 GB de estado vivo. Depois da última rodada
// todo record vivo está nos ÚLTIMOS segmentos, e os selados anteriores são
// garbage quase puro — é exatamente o disco que nada no projeto devolvia.
//
// A queda é medida por os.Stat e afirmada em bytes; a conferência das rows é
// uma a uma, antes e DEPOIS de um Close+Open, porque a location pendurada num
// arquivo removido não machuca ninguém enquanto o índice ainda está em
// memória — ela reaparece na reabertura.
func TestCompactionDevolveDiscoSemPerderNenhumaRow(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	for r := 0; r < rodadas; r++ {
		for i := 0; i < keys; i++ {
			k := chaveDoCorpus(i)
			mustPut(t, s, k, sequenceDoCorpus(r, i), payloadDoCorpus(t, s, k, r), nil)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	alvos := seladosEmDisco(t, dir, s.Stats().CurrentSegmentID)
	if len(alvos) < 3 {
		t.Fatalf("o corpus selou %d segmento(s): sem uma pilha de selados o teste não mede compaction nenhuma", len(alvos))
	}
	antes := discoDosSegmentosPorOsStat(t, dir)
	antesDoDataset := discoDoDatasetInteiroPorOsStat(t, dir)
	for _, seg := range alvos {
		if _, _, _, err := s.compactaSegmento(context.Background(), seg); err != nil {
			t.Fatalf("compactaSegmento(%d): %v", seg, err)
		}
		if err := s.aposentaSegmento(seg); err != nil {
			t.Fatalf("aposentaSegmento(%d): %v", seg, err)
		}
	}
	depois := discoDosSegmentosPorOsStat(t, dir)
	depoisDoDataset := discoDoDatasetInteiroPorOsStat(t, dir)
	t.Logf("disco em segment-*.dat: %d bytes antes, %d depois — %d devolvidos ao filesystem em %d segmentos aposentados",
		antes, depois, antes-depois, len(alvos))
	t.Logf("diretório do dataset INTEIRO (item 1 do critério de aceite, ao pé da letra): %d bytes antes, %d depois — %d devolvidos",
		antesDoDataset, depoisDoDataset, antesDoDataset-depoisDoDataset)

	if depois >= antes {
		t.Fatalf("o diretório saiu de %d para %d bytes: não houve compaction nenhuma", antes, depois)
	}
	// O item 1 pede o DIRETÓRIO, e é isto: se os segmentos encolhem enquanto o
	// dataset inteiro não, a compaction moveu o custo de lugar em vez de
	// devolvê-lo — e é o dataset inteiro que ocupa o volume que o §86 dá 2 a 3
	// dias para encher.
	if depoisDoDataset >= antesDoDataset {
		t.Fatalf("os segmentos caíram de %d para %d bytes mas o dataset inteiro foi de %d para %d: "+
			"o disco mudou de arquivo, não voltou para o filesystem", antes, depois, antesDoDataset, depoisDoDataset)
	}
	// O piso não é cosmética. Com %d rodadas sobre o mesmo key set, o vivo é
	// uma fração pequena do escrito, então uma queda menor que a metade
	// denuncia aposentadoria PARCIAL — o mutante que retira um segmento e para
	// deixaria "depois < antes" verde.
	if depois > antes/2 {
		t.Errorf("o disco caiu de %d para %d bytes, menos da metade, com %d rodadas de update sobre %d keys: "+
			"a maior parte do arquivo era garbage e deveria ter voltado", antes, depois, rodadas, keys)
	}
	sobreviventes, err := filepath.Glob(filepath.Join(dir, "data", "*"+sufixoAposentadoNoTeste))
	if err != nil {
		t.Fatalf("glob dos aposentados: %v", err)
	}
	if len(sobreviventes) != 0 {
		t.Errorf("sobraram %d arquivo(s) aposentado(s) em disco: %v", len(sobreviventes), sobreviventes)
	}

	exigeCorpusNaUltimaRodada(t, s, keys, rodadas-1, "após a compaction")
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reaberto := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	exigeCorpusNaUltimaRodada(t, reaberto, keys, rodadas-1, "após Close+Open")
	if got := reaberto.Stats().RowCount; got != keys {
		t.Errorf("RowCount = %d após a reabertura, queria %d", got, keys)
	}
}

// TestPodaDoSegmentoAposentadoDescontaOsTresEspelhos afere a ARITMÉTICA da
// poda, e existe porque o teste da ordem não a alcança inteira: lá o segmento
// chega à aposentadoria com o tally já zerado pela varredura (decisão D4), de
// modo que o desconto de liveBytesAll é uma subtração de zero e some do
// oráculo. Apagar aquela linha deixava a suíte inteira verde.
//
// A invariante 4 não tem cláusula de exceção — toda escrita que um espelho
// descreve ajusta o espelho, na mesma seção crítica —, então o que se afere
// aqui é a poda contra um tally QUALQUER, e não contra o tally que a
// sequência feliz produz. É por isso que o teste chama a poda direto em vez de
// passar pelo aposentaSegmento: aposentar um segmento com record vivo dentro
// dele é o que a função proíbe, e montar essa proibição só para observar uma
// subtração deixaria em disco um dataset com location pendurada.
func TestPodaDoSegmentoAposentadoDescontaOsTresEspelhos(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const n = 400
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("poda-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	// O checkpoint ancora o segSize, que só se move nele: sem isto o
	// StorageBytes de "antes" descreveria um diretório mais antigo que o
	// arquivo que a poda desconta.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint de base: %v", err)
	}
	const alvo = 1
	info, err := os.Stat(audSegPath(dir, alvo))
	if err != nil {
		t.Fatalf("stat do segmento alvo: %v", err)
	}
	_, vivo := tallyDoSegmento(s, alvo)
	if vivo == 0 {
		t.Fatal("a montagem deixou o segmento 1 sem tally vivo: o desconto seria de zero e o teste não provaria nada")
	}
	antes := s.Stats()
	divergenciasAntes := s.EspelhosDivergentesForTest()

	if err := s.podaSegmentoAposentado(alvo); err != nil {
		t.Fatalf("podaSegmentoAposentado: %v", err)
	}

	depois := s.Stats()
	if want := antes.LiveBytesUpperBound - vivo; depois.LiveBytesUpperBound != want {
		t.Errorf("LiveBytesUpperBound = %d, queria %d (%d menos os %d que o segmento reivindicava)",
			depois.LiveBytesUpperBound, want, antes.LiveBytesUpperBound, vivo)
	}
	if want := antes.SegmentsTotal - 1; depois.SegmentsTotal != want {
		t.Errorf("SegmentsTotal = %d, queria %d", depois.SegmentsTotal, want)
	}
	if want := antes.StorageBytes - info.Size(); depois.StorageBytes != want {
		t.Errorf("StorageBytes = %d, queria %d (%d menos os %d bytes do arquivo)",
			depois.StorageBytes, want, antes.StorageBytes, info.Size())
	}
	exigeEspelhoIgualAosTallies(t, s, "após a poda do segmento aposentado")
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint pós-poda: %v", err)
	}
	if got := s.EspelhosDivergentesForTest(); got != divergenciasAntes {
		t.Errorf("o checkpoint seguinte acusou %d divergências contra %d antes da poda", got, divergenciasAntes)
	}
}

// ---------------------------------------------------------------------------
// Task 6 — a PRIMEIRA metade do invariante 3, aferida no arquivo de locations
// ---------------------------------------------------------------------------

// segmentosDuraveisDasRows lê a location de cada row no ARQUIVO, e não no
// Store. A diferença é o teste inteiro: o Get do Store resolve overlay antes
// do mmap (rowindex.go:152), então ele responde o destino da varredura desde o
// instante da cópia — inclusive quando nada daquilo chegou ao disco. Um
// segundo handle sobre o mesmo arquivo nasce com overlay vazio, de modo que o
// que ele devolve é exatamente o que um processo que abrisse este dataset
// agora encontraria.
//
// É a mesma técnica do TestReplayNaoDuplicaSlotComPrimaryAFrenteDasLocations,
// que também vai ao locations.idx por fora do Store, e pela mesma razão:
// afirmar sobre durabilidade consultando quem tem o estado em memória é
// perguntar ao réu.
func segmentosDuraveisDasRows(t *testing.T, dir string, rows []types.RowID) map[types.RowID]uint32 {
	t.Helper()
	locs, err := rowindex.OpenLocations(filepath.Join(dir, "indexes", "locations.idx"), 0)
	if err != nil {
		t.Fatalf("abrir o locations.idx para leitura independente: %v", err)
	}
	// O descarte é explícito porque este handle é só de LEITURA: ele não
	// publicou nada, não tem overlay para aplicar e o unmap não pode falhar de
	// um jeito que mude o que já foi lido. Fechar em silêncio um handle de
	// escrita seria outra conversa.
	defer func() { _ = locs.Close() }()
	fora := make(map[types.RowID]uint32, len(rows))
	for _, rid := range rows {
		packed, ok := locs.Get(rid)
		if !ok {
			t.Fatalf("a row %d não tem location DURÁVEL: o dataset em disco não a nomeia em lugar nenhum", rid)
		}
		seg, _, _ := rowindex.Unpack(packed)
		fora[rid] = seg
	}
	return fora
}

// contaPresasAoSegmento devolve quantas das rows ainda têm, EM DISCO, location
// apontando para o segmento dado.
func contaPresasAoSegmento(duraveis map[types.RowID]uint32, seg uint32) int {
	n := 0
	for _, s := range duraveis {
		if s == seg {
			n++
		}
	}
	return n
}

// TestAposentadoriaTornaAsLocationsDuraveisAntesDoRename é a PRIMEIRA metade
// do invariante 3 da Fase B — "o Checkpoint que torna as locations duráveis
// acontece ANTES do rename" —, e existe porque a revisão desta tarefa mostrou
// que ela não tinha oráculo nenhum: remover aquele Checkpoint e deixar a linha
// do trace no lugar deixava `go test ./...` inteiro verde. O que segurava o
// passo era a CONTAGEM de rótulos do teste da ordem, e contagem de rótulo
// aprova qualquer no-op que continue chamando os mesmos nomes.
//
// O que este teste mede é outra coisa: o ARQUIVO de locations, lido por um
// handle independente, em cada instante em que a aposentadoria dá sinal. A
// asserção não é sobre o nome do passo, é sobre o disco — enquanto o segmento
// AINDA TEM o nome canônico, nenhuma das rows que a varredura moveu pode
// continuar presa a ele no arquivo. Com o Checkpoint no lugar, a varredura
// republicou e o ApplyOverlay já levou tudo ao mmap; sem ele, as
// republicações continuam no overlay em memória e o disco ainda nomeia o
// segmento que está prestes a perder o nome.
//
// A âncora é FATO DE DISCO e não rótulo de propósito: quem apagar também a
// linha do trace não escapa, porque aí não sobra observação nenhuma com o
// canônico presente e o teste falha por vacuidade declarada — um oráculo que
// não chegou a observar a janela não aprova ninguém.
//
// O que este teste NÃO afirma, e o registro é honesto: que um crash na janela
// perderia dado sem o Checkpoint. Não perderia — o passo 3 aplica o mesmo
// overlay e os bytes de origem ficam sob o `.retired` até o fim, de modo que o
// reparo do Open devolve o dataset dos dois jeitos. O invariante 3 é sobre a
// ORDEM valer em disco desde o primeiro passo, e é isso, exatamente isso, que
// está aferido aqui.
func TestAposentadoriaTornaAsLocationsDuraveisAntesDoRename(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const n = 400
	rows := make([]types.RowID, 0, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("dur-%03d", i)
		rid, _ := mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
		rows = append(rows, rid)
	}
	const alvo = 1
	var migradas []types.RowID
	for _, rid := range rows {
		if seg, _ := localizacaoDaRow(t, s, rid); seg == alvo {
			migradas = append(migradas, rid)
		}
	}
	if len(migradas) == 0 {
		t.Fatal("nenhuma row viva no segmento 1: sem republicação para durabilizar, o teste aprovaria qualquer coisa")
	}
	// O checkpoint de base é o que põe a PRECONDIÇÃO em disco: sem ele as
	// locations do corpus estariam no overlay desde sempre, e "ainda presa ao
	// segmento 1" seria indistinguível de "nunca escrita".
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint de base: %v", err)
	}
	if presas := contaPresasAoSegmento(segmentosDuraveisDasRows(t, dir, migradas), alvo); presas != len(migradas) {
		t.Fatalf("o checkpoint de base deixou %d das %d rows fora do segmento %d em disco: a montagem não é a que o teste descreve",
			len(migradas)-presas, len(migradas), alvo)
	}

	if _, _, _, err := s.compactaSegmento(context.Background(), alvo); err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}
	// A premissa que dá sentido à asserção: a varredura republica no OVERLAY
	// (rowindex.go:171), então neste instante o Store já responde o destino e
	// o DISCO ainda nomeia a origem. É a diferença que o Checkpoint do passo 1
	// existe para fechar — se ela não existisse aqui, o teste seria vacuidade
	// com outro nome.
	for _, rid := range migradas {
		if seg, _ := localizacaoDaRow(t, s, rid); seg == alvo {
			t.Fatalf("a row %d continua no segmento %d depois da varredura: o copy-forward não republicou nada", rid, alvo)
		}
	}
	if presas := contaPresasAoSegmento(segmentosDuraveisDasRows(t, dir, migradas), alvo); presas != len(migradas) {
		t.Fatalf("a varredura já deixou %d das %d republicações DURÁVEIS sem checkpoint nenhum: "+
			"o overlay deixou de ser overlay e este teste não mede mais o passo 1", len(migradas)-presas, len(migradas))
	}

	canonico := audSegPath(dir, alvo)
	type observacao struct {
		passo    string
		canonico bool
		presas   int
	}
	var obs []observacao
	s.SetAposentadoriaTraceForTest(func(passo string) {
		_, errCanonico := os.Stat(canonico)
		obs = append(obs, observacao{
			passo:    passo,
			canonico: errCanonico == nil,
			presas:   contaPresasAoSegmento(segmentosDuraveisDasRows(t, dir, migradas), alvo),
		})
	})
	defer s.SetAposentadoriaTraceForTest(nil)

	if err := s.aposentaSegmento(alvo); err != nil {
		t.Fatalf("aposentaSegmento: %v", err)
	}

	antesDoRename := 0
	for _, o := range obs {
		if !o.canonico {
			continue
		}
		antesDoRename++
		if o.presas != 0 {
			t.Errorf("no passo %q o segmento %d ainda tinha o nome canônico e %d das %d locations republicadas continuavam presas a ele EM DISCO: "+
				"o Checkpoint que as torna duráveis não rodou antes do rename (invariante 3 da Fase B, §53-55 D5)",
				o.passo, alvo, o.presas, len(migradas))
		}
	}
	if antesDoRename == 0 {
		t.Fatalf("a aposentadoria não deu sinal nenhum enquanto o segmento %d ainda tinha o nome canônico (%+v): "+
			"sem uma observação ANTES do rename este teste não afirma nada sobre a ordem", alvo, obs)
	}
}

// TestAposentadoriaDurabilizaODiretorioNoRenameENoUnlink fixa as duas
// chamadas de fsync de diretório da aposentadoria, e existe porque a revisão
// as encontrou em produção com justificativa escrita e nenhum teste que
// distinguisse a presença da ausência: apagar qualquer uma das duas deixava
// o pacote verde.
//
// O oráculo não é a ordem dos avisos, é o estado do DIRETÓRIO no instante de
// cada um — o aviso chega de dentro do helper, depois de o fsync devolver
// nil, e o teste pergunta ao disco o que já aconteceu. São duas afirmações:
//
//  1. houve fsync do diretório de dados num instante em que o rename JÁ estava
//     feito e o manifest AINDA nomeava o segmento. É a ponta que importa: o
//     manifest do passo 3 se durabiliza sozinho, e sem este fsync ele poderia
//     sobreviver a um rename que não sobreviveu.
//  2. houve fsync depois do unlink, com o `.retired` já fora do disco e fora
//     do manifest.
//
// O limite é declarado: isto prova a CHAMADA e a POSIÇÃO, não a reordenação
// que ela previne — essa exige perda de energia com writeback pela metade, e
// o kill -9 não a produz porque o SIGKILL não descarta page cache.
func TestAposentadoriaDurabilizaODiretorioNoRenameENoUnlink(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const n = 400
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("fsy-%03d", i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	const alvo = 1
	if _, _, _, err := s.compactaSegmento(context.Background(), alvo); err != nil {
		t.Fatalf("compactaSegmento: %v", err)
	}
	canonico := audSegPath(dir, alvo)
	aposentado := canonico + sufixoAposentadoNoTeste

	type sinal struct {
		dir        string
		canonico   bool
		aposentado bool
		listado    bool
	}
	var sinais []sinal
	s.SetSyncDeDiretorioTraceForTest(func(d string) {
		m, err := checkpoint.Load(audManifestPath(dir))
		if err != nil {
			t.Fatalf("carregar o manifest no fsync de %q: %v", d, err)
		}
		listado := false
		for _, si := range m.Segments {
			if si.ID == alvo {
				listado = true
			}
		}
		_, errCanonico := os.Stat(canonico)
		_, errAposentado := os.Stat(aposentado)
		sinais = append(sinais, sinal{
			dir:        d,
			canonico:   errCanonico == nil,
			aposentado: errAposentado == nil,
			listado:    listado,
		})
	})
	defer s.SetSyncDeDiretorioTraceForTest(nil)

	if err := s.aposentaSegmento(alvo); err != nil {
		t.Fatalf("aposentaSegmento: %v", err)
	}

	dataDir := filepath.Join(dir, "data")
	depoisDoRename, depoisDoUnlink := false, false
	for _, sn := range sinais {
		if sn.dir != dataDir {
			t.Errorf("a aposentadoria durabilizou o diretório %q, e o que ela renomeia e remove está em %q", sn.dir, dataDir)
			continue
		}
		if !sn.canonico && sn.aposentado && sn.listado {
			depoisDoRename = true
		}
		if !sn.canonico && !sn.aposentado && !sn.listado {
			depoisDoUnlink = true
		}
	}
	if !depoisDoRename {
		t.Errorf("nenhum fsync de %q aconteceu com o rename feito e o manifest ainda nomeando o segmento %d (sinais: %+v): "+
			"o manifest do passo 3 pode ficar durável sobre uma entrada de diretório que não ficou", dataDir, alvo, sinais)
	}
	if !depoisDoUnlink {
		t.Errorf("nenhum fsync de %q aconteceu depois do unlink (sinais: %+v): a remoção fica só em cache e o ciclo seguinte "+
			"volta a medir disco que esta função já devolveu", dataDir, sinais)
	}
}

// ============================================================================
// Task 7 — seleção de candidatos e ciclo de vida da goroutine (§53, D6, D7)
// ============================================================================

// corpusDeSegmentosSelados enche o dataset com updates sobre um key set FIXO
// até haver uma pilha de segmentos SELADOS quase todos de garbage, e deixa o
// checkpoint feito.
//
// O checkpoint não é cerimônia: o segSize só se move nele (store.go, godoc do
// campo), e a seleção do §53 divide o tally pelo TAMANHO DO ARQUIVO. Sem ele
// nenhum segmento teria tamanho conhecido e a eleição devolveria lista vazia
// por falta de medida — um teste verde que não mediu nada.
func corpusDeSegmentosSelados(t *testing.T, s *Store, keys, rodadas int) {
	t.Helper()
	for r := 0; r < rodadas; r++ {
		for i := 0; i < keys; i++ {
			k := chaveDoCorpus(i)
			mustPut(t, s, k, sequenceDoCorpus(r, i), payloadDoCorpus(t, s, k, r), nil)
		}
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint do corpus: %v", err)
	}
}

// TestSegmentoCorrenteNuncaEhCandidato é a decisão D6 em forma de asserção:
// compactar o segmento em que se está apendando é copiar dentro de si mesmo —
// o destino é a própria origem, a varredura persegue os records que ela mesma
// acabou de escrever, e a aposentadoria removeria o arquivo em que o writer
// está no meio de um Append.
func TestSegmentoCorrenteNuncaEhCandidato(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 20)

	corrente := s.Stats().CurrentSegmentID
	eleitos := s.candidatos(0.99)
	if len(eleitos) == 0 {
		t.Fatal("nenhum candidato com 20 rodadas de update sobre 40 keys: a montagem não produziu garbage, e a asserção abaixo passaria por vacuidade")
	}
	for _, c := range eleitos {
		if c == corrente {
			t.Fatalf("o segmento corrente %d foi eleito candidato (eleitos: %v)", corrente, eleitos)
		}
	}
}

// TestCandidatosLeOSegmentoCorrenteDEPOISDeEntrarNaSecaoCritica fixa a ORDEM
// que o esboço do plano tem invertida, e ela decide a D6 inteira.
//
// Lido ANTES do wmu, o curSegID é um valor de um instante em que ninguém
// segurava o lock do writer — e quem espera esse lock quase sempre está
// esperando um CHECKPOINT, que é a operação que o segura do início ao fim.
// É justamente o checkpoint que instala no segSize o tamanho do segmento
// recém-rotacionado: ou seja, a janela entre a leitura e o Lock é exatamente
// aquela em que o segmento que virou corrente ganha tamanho conhecido e passa
// a ser elegível. A seleção acorda com o corrente ANTIGO na mão, não reconhece
// o novo, e elege o segmento em que o writer está apendando.
//
// O teste é o writer: segura o wmu, espera a seleção PARAR no Lock (fato lido
// da stack, e não um sleep — o mesmo instrumento do
// TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica), rotaciona e
// contabiliza o segmento novo como o checkpoint contabilizaria, e só então
// solta. A partir daí as duas implementações são distinguíveis sempre.
func TestCandidatosLeOSegmentoCorrenteDEPOISDeEntrarNaSecaoCritica(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 20, 20)
	const key = "cand-barreira"
	payload := statusPayload(t, s, key, "Start", 1)
	rowID, _ := mustPut(t, s, key, 1, payload, nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint da barreira: %v", err)
	}

	s.wmu.Lock()
	eleitos := make(chan []uint32, 1)
	go func() { eleitos <- s.candidatos(0.99) }()
	if !paradaNoWmuEm("candidatos", 10*time.Second) {
		s.wmu.Unlock()
		t.Fatal("a seleção não chegou a parar no lock do writer: sem a barreira o teste não distingue implementação nenhuma")
	}

	// Rotação de verdade, com a seleção já parada no Lock: Append mais
	// applyRecord, que é o par que o Put executa.
	antes, _ := s.segW.Current()
	corrente := antes
	for i := 0; i < 500 && corrente == antes; i++ {
		publicaUpdateSobOWmu(t, s, rowID, key, int64(1000+i), payload, payload)
		corrente, _ = s.segW.Current()
	}
	if corrente == antes {
		s.wmu.Unlock()
		t.Fatalf("o writer não rotacionou além do segmento %d: sem rotação nesta janela não há o que a leitura antecipada erre", antes)
	}
	// O que o checkpoint que segurava este lock deixaria feito. Não é estado
	// inventado: é a MESMA escrita da reconciliação (store_checkpoint.go), na
	// mesma seção crítica, e com o espelho ajustado junto — invariante 4.
	fi, err := os.Stat(audSegPath(dir, corrente))
	if err != nil {
		s.wmu.Unlock()
		t.Fatalf("stat do segmento recém-rotacionado: %v", err)
	}
	s.segSize[corrente] = fi.Size()
	s.diskBytesAll.Add(fi.Size())
	s.wmu.Unlock()

	lista := <-eleitos
	if len(lista) == 0 {
		t.Fatal("a seleção devolveu lista vazia: o corpus deixou de produzir candidato e a asserção abaixo não discriminaria nada")
	}
	for _, c := range lista {
		if c == corrente {
			t.Fatalf("o segmento %d virou corrente COM a seleção já parada no lock e ainda assim foi eleito (eleitos: %v): "+
				"o corrente foi lido antes da seção crítica, e a D6 vale sobre um instante que já passou", corrente, lista)
		}
	}
}

// TestCandidatoSoSaiAbaixoDoMinimumLiveRatio prende o predicado do §53 —
// `liveBytes/sizeBytes < ratio` — em direção e em fronteira.
//
// O oráculo é recalculado dos MESMOS mapas que a seleção lê, e isso é
// deliberado: o que se afere aqui não é a exatidão do tally (ela é limite
// superior por construção, débitos 38 e 45), é o predicado. Uma comparação
// invertida elegeria justamente os segmentos densos, e um `<=` no lugar do `<`
// tornaria candidato o segmento cuja liveness bate exatamente o teto — que
// com ratio 1.0 seria o dataset inteiro, em laço.
func TestCandidatoSoSaiAbaixoDoMinimumLiveRatio(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 20)
	corrente := s.Stats().CurrentSegmentID

	razoes := map[uint32]float64{}
	s.wmu.Lock()
	for id, t := range s.segTally {
		if tam, ok := s.segSize[id]; ok && tam > 0 && id != corrente {
			razoes[id] = float64(t.liveBytes) / float64(tam)
		}
	}
	s.wmu.Unlock()
	if len(razoes) < 3 {
		t.Fatalf("só %d segmento(s) selado(s) com tamanho conhecido: sem uma pilha deles a tabela abaixo não separa eleito de recusado", len(razoes))
	}

	for _, ratio := range []float64{0.01, 0.25, 0.5, 0.99} {
		querido := map[uint32]bool{}
		for id, r := range razoes {
			if r < ratio {
				querido[id] = true
			}
		}
		obtido := map[uint32]bool{}
		for _, id := range s.candidatos(ratio) {
			obtido[id] = true
		}
		if !maps.Equal(querido, obtido) {
			t.Errorf("ratio %v: eleitos %v, queria %v (razões medidas: %v)", ratio, obtido, querido, razoes)
		}
	}
}

// TestCandidatosSaemEmOrdemDeterministica: a mesma eleição tem de produzir a
// mesma ordem. Iteração de mapa Go é aleatória de propósito, então sem a
// ordenação duas passadas idênticas compactariam em ordens diferentes — e o
// operador que lê a sequência de aposentadorias no log não teria como
// distinguir uma mudança de estado de uma troca de ordem.
func TestCandidatosSaemEmOrdemDeterministica(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 20)

	primeira := s.candidatos(0.99)
	if len(primeira) < 3 {
		t.Fatalf("só %d candidato(s): com menos de três a ordenação não é observável", len(primeira))
	}
	if !slices.IsSorted(primeira) {
		t.Errorf("a eleição saiu fora de ordem: %v", primeira)
	}
	for i := 0; i < 20; i++ {
		if got := s.candidatos(0.99); !slices.Equal(got, primeira) {
			t.Fatalf("a volta %d devolveu %v contra %v da primeira: a eleição não é determinística", i, got, primeira)
		}
	}
}

// TestCloseCancelaEAguardaACompaction é a decisão D7 nas duas direções, e as
// duas são necessárias.
//
// O Close toma vida.Lock(), que só é concedido quando nenhuma operação está em
// voo, e a varredura do §54 é uma operação longa. Se a goroutine não for
// cancelada, o Close espera a passada inteira. Se não for AGUARDADA, é pior: o
// Close vence a corrida, desmapeia o índice e fecha os segmentos sob os pés de
// uma compaction que está no meio de renomear e apagar arquivo — o único
// caminho do projeto que destrói dado.
//
// A barreira é o seam da aposentadoria porque ali a goroutine está entre dois
// passos do D5 SEM segurar o entra(): é exatamente o instante em que um Close
// sem join receberia o vida.Lock() na hora e passaria à frente. Sem essa
// asserção do meio, um Close que apenas não trava passaria verde.
func TestCloseCancelaEAguardaACompaction(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, SegmentSize: 8 << 10})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	corpusDeSegmentosSelados(t, s, 40, 30)

	emVoo := make(chan struct{})
	libera := make(chan struct{})
	liberar := sync.OnceFunc(func() { close(libera) })
	defer liberar()
	var umaVez sync.Once
	s.SetAposentadoriaTraceForTest(func(string) {
		umaVez.Do(func() {
			close(emVoo)
			<-libera
		})
	})

	s.IniciaCompaction(context.Background(), 0.5)
	select {
	case <-emVoo:
	case <-time.After(60 * time.Second):
		t.Fatal("a compaction não chegou a aposentar nada em 60s: sem uma operação em voo o Close não tem o que aguardar")
	}

	feito := make(chan error, 1)
	go func() { feito <- s.Close() }()
	select {
	case err := <-feito:
		t.Fatalf("Close retornou (%v) com a compaction parada NO MEIO de uma aposentadoria: "+
			"a goroutine não foi aguardada, e o que vem depois dela é rename e unlink de arquivo sobre um Store já fechado", err)
	case <-time.After(500 * time.Millisecond):
	}

	liberar()
	select {
	case err := <-feito:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Close não retornou: a compaction não foi cancelada nem aguardada")
	}
}

// TestCompactionEmBackgroundDevolveDiscoSemNinguemChamar é a Task 7 inteira do
// lado do storage: ninguém chama compactaSegmento nem aposentaSegmento aqui.
// A goroutine elege, varre, aposenta e volta a dormir por conta própria, que é
// como ela roda em produção.
//
// O que separa este teste do critério de aceite da Task 6 é justamente o
// AGENDADOR: lá as duas funções eram chamadas à mão sobre uma lista de alvos
// escolhida pelo teste, e uma seleção que nunca elegesse nada deixaria a suíte
// verde. Aqui a lista sai do §53.
func TestCompactionEmBackgroundDevolveDiscoSemNinguemChamar(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	antes := discoDosSegmentosPorOsStat(t, dir)
	s.IniciaCompaction(context.Background(), 0.5)

	prazo := time.Now().Add(60 * time.Second)
	depois := antes
	for time.Now().Before(prazo) {
		if depois = discoDosSegmentosPorOsStat(t, dir); depois <= antes/2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.ParaCompaction()

	t.Logf("disco em segment-*.dat: %d bytes antes, %d depois — %d devolvidos sem ninguém chamar a compaction",
		antes, depois, antes-depois)
	if depois > antes/2 {
		t.Fatalf("o disco saiu de %d para %d bytes com %d rodadas de update sobre %d keys: "+
			"a maior parte do arquivo era garbage e a goroutine não a devolveu", antes, depois, rodadas, keys)
	}
	sobreviventes, err := filepath.Glob(filepath.Join(dir, "data", "*"+sufixoAposentadoNoTeste))
	if err != nil {
		t.Fatalf("glob dos aposentados: %v", err)
	}
	if len(sobreviventes) != 0 {
		t.Errorf("sobraram %d arquivo(s) aposentado(s) em disco: %v", len(sobreviventes), sobreviventes)
	}
	exigeCorpusNaUltimaRodada(t, s, keys, rodadas-1, "após a compaction em background")
	exigeEspelhoIgualAosTallies(t, s, "após a compaction em background")

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reaberto := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	exigeCorpusNaUltimaRodada(t, reaberto, keys, rodadas-1, "após Close+Open")
}

// registroDeLog é o io.Writer do logger quando quem escreve é uma goroutine e
// quem lê é o teste. Um bytes.Buffer nu aí é corrida de dado, e o -race a
// acusaria antes de o oráculo chegar a dizer qualquer coisa — o que se perde
// nesse caso não é só a asserção, é a razão da falha.
type registroDeLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *registroDeLog) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *registroDeLog) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

// mataOWriter põe o writer em modo falho STICKY pelo caminho que o produto tem:
// erro na fase de Set dos bitmaps, ou seja DEPOIS do Append (§14.12), que é
// justamente onde não existe rollback parcial (store.go:1012).
//
// É a mesma montagem que o TestCopyForwardRecusaComOWriterEmModoFalho faz em
// linha, e a repetição é deliberada: aquele teste afirma sobre a RECUSA DA
// CÓPIA e este sobre a PARADA DO LAÇO, e reescrevê-lo para depender deste
// helper acoplaria dois oráculos que precisam falhar por motivos diferentes.
func mataOWriter(t *testing.T, s *Store) {
	t.Helper()
	s.SetSetErrHookForTest(func(types.FieldID) error { return errors.New("set falho simulado") })
	defer s.SetSetErrHookForTest(nil)
	const key = "mata-o-writer"
	if _, _, err := s.Put([]byte(key), 1, 0, statusPayload(t, s, key, "Start", 1), nil); err == nil {
		t.Fatal("montagem: o Put com Set falho deveria falhar e deixar o writer em modo falho")
	}
}

// TestCompactionEmBackgroundParaNaPrimeiraFalhaDoWriter é o oráculo do modo
// falho sticky do LAÇO, que o godoc do rodaCompaction gasta um parágrafo
// argumentando e que a suíte não media: trocar o bloco de erro inteiro por
// `_ = s.passadaDeCompaction(ctx, ratio)` deixava `go test ./...` verde de ponta
// a ponta, com o laço insistindo para sempre sobre um writer que só sai do modo
// falho com Close e reabertura (§14.14).
//
// A asserção do e2e (`compaction.parou` NÃO aparece na corrida boa,
// compaction_e2e_test.go) não substitui esta, e é importante dizer por quê: ela
// só falha se a linha existir, então passa por VACUIDADE exatamente quando o
// ramo é apagado. Asserção negativa não é oráculo da presença.
//
// São duas metades, e nenhuma delas basta sozinha:
//
//   - a goroutine TERMINA por conta própria, sem cancelamento e sem Close. Quem
//     responde isso sem inventar seam novo é o próprio compEmVoo: um laço que
//     insistisse continuaria de pé, e o Wait nunca voltaria;
//   - e ela se EXPLICA. `compaction.parou` é o único aviso de que o disco
//     voltou a crescer sem nada devolvê-lo (§53-55, §86); uma parada silenciosa
//     é indistinguível, no painel do operador, de uma compaction que nunca teve
//     o que fazer.
func TestCompactionEmBackgroundParaNaPrimeiraFalhaDoWriter(t *testing.T) {
	var registro registroDeLog
	s := openStore(t, Options{
		Dir:          t.TempDir(),
		SegmentSize:  8 << 10,
		BitmapFields: []types.FieldID{fldStatus},
		Log:          logging.New(&registro, logging.FormatoTexto, slog.LevelInfo),
	})
	corpusDeSegmentosSelados(t, s, 40, 30)
	if eleitos := s.candidatos(0.5); len(eleitos) == 0 {
		t.Fatal("nenhum candidato depois do corpus: sem passada para falhar, a asserção abaixo passaria por vacuidade")
	}
	mataOWriter(t, s)

	s.IniciaCompaction(context.Background(), 0.5)

	terminou := make(chan struct{})
	go func() { s.compEmVoo.Wait(); close(terminou) }()
	select {
	case <-terminou:
	case <-time.After(60 * time.Second):
		t.Fatal("a goroutine de compaction continua de pé com o writer em modo falho: ela está insistindo sobre um estado " +
			"que só Close e reabertura desfazem (§14.14), varrendo segmento e disputando o wmu para reencontrar o mesmo erro")
	}
	if saida := registro.String(); !strings.Contains(saida, "compaction.parou") {
		t.Errorf("a compaction parou sem dizer nada ao operador: o disco volta a crescer e o único aviso do §53-55 não saiu.\nlog:\n%s", saida)
	}
}

// TestSegundaPartidaDaCompactionNaoSobeSegundaGoroutine é a guarda de dupla
// partida do IniciaCompaction, que o godoc afirma e que nada sustentava: apagar
// o `if s.compCancela != nil { return }` deixava storage, server e cli verdes.
//
// O estrago que a guarda impede está escrito no próprio godoc e é de ARQUIVO,
// não de contagem: duas varreduras disputam o wmu record a record, elegem a
// mesma lista, e a segunda renomeia e apaga um segmento que a primeira ainda
// está lendo. A isso soma-se um vazamento de ciclo de vida — a segunda partida
// SOBRESCREVE o compCancela, de modo que o ParaCompaction do Close cancelaria só
// a última goroutine e ficaria esperando para sempre pela primeira.
//
// O oráculo é positivo, e não uma espera por travamento: a primeira goroutine
// fica PARADA dentro de uma aposentadoria, no seam do D5, e a partir daí
// qualquer passo de aposentadoria que apareça é necessariamente de OUTRA
// goroutine. Contar visitas ao seam sem a barreira não distinguiria a intrusa —
// são quatro passos por segmento, todos da mesma goroutine.
func TestSegundaPartidaDaCompactionNaoSobeSegundaGoroutine(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	emVoo := make(chan struct{})
	libera := make(chan struct{})
	liberar := sync.OnceFunc(func() { close(libera) })
	defer liberar()
	var barreira atomic.Bool
	var intrusos atomic.Int64
	s.SetAposentadoriaTraceForTest(func(string) {
		if barreira.CompareAndSwap(false, true) {
			close(emVoo)
			<-libera
			return
		}
		intrusos.Add(1)
	})

	comecou := time.Now()
	s.IniciaCompaction(context.Background(), 0.5)
	select {
	case <-emVoo:
	case <-time.After(60 * time.Second):
		t.Fatal("a compaction não chegou a aposentar nada em 60s: sem a primeira goroutine parada dentro de uma " +
			"aposentadoria não há com quem a segunda colidir, e a asserção passaria por vacuidade")
	}
	// O prazo é CALIBRADO pela bancada em vez de chutado. `decorrido` é o que a
	// primeira goroutine levou para eleger, varrer um segmento inteiro e chegar
	// ao primeiro passo da aposentadoria — exatamente o trabalho que uma
	// intrusa teria de repetir, e ela não precisa de sorte para começar: a
	// primeira passada do laço é IMEDIATA, sem cochilo. Vinte vezes isso, com
	// piso de dois segundos, é folga para o -race e para máquina carregada sem
	// transformar o teste numa espera fixa.
	decorrido := time.Since(comecou)
	janela := 20 * decorrido
	if janela < 2*time.Second {
		janela = 2 * time.Second
	}

	s.IniciaCompaction(context.Background(), 0.5)
	prazo := time.Now().Add(janela)
	for time.Now().Before(prazo) && intrusos.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if n := intrusos.Load(); n != 0 {
		t.Fatalf("a segunda chamada do IniciaCompaction subiu outra goroutine: %d passo(s) de aposentadoria em %v com a "+
			"primeira parada no seam. Duas varreduras concorrentes aposentam o arquivo que a outra está lendo, e o "+
			"cancelamento do Close só alcança a última que partiu", n, janela)
	}
	t.Logf("segunda partida ignorada: nenhum passo de aposentadoria alheio em %v (primeira passada em %v)", janela, decorrido)
	liberar()
}
