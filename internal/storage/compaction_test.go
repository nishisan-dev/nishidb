package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
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
	buf := make([]byte, 1<<20)
	limite := time.Now().Add(prazo)
	for {
		n := runtime.Stack(buf, true)
		for _, g := range strings.Split(string(buf[:n]), "\n\ngoroutine ") {
			// As duas marcas na MESMA goroutine: parada no Lock, e parada
			// nele por dentro do copy-forward.
			if strings.Contains(g, "reapendaSeVivo") && strings.Contains(g, "sync.(*Mutex).Lock") {
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

	real := segmentoCorrenteDoWriter(s)
	if got := s.Stats().CurrentSegmentID; got != real {
		t.Errorf("CurrentSegmentID = %d e o writer está no %d: o copy-forward rotacionou e o espelho ficou para trás", got, real)
	}
	// A mentira não cicatriza. O checkpoint recarrega os espelhos de liveness
	// e NÃO o curSegID; sem esta segunda asserção alguém poderia supor que o
	// próximo checkpoint conserta, e operar em cima disso.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := s.Stats().CurrentSegmentID; got != real {
		t.Errorf("CurrentSegmentID = %d depois do checkpoint e o writer está no %d: este espelho não é reconciliado", got, real)
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
