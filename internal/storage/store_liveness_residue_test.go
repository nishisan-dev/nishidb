package storage

// O resíduo de liveness do replay: quando uma row já existia no ponto S e o
// tail traz um update cuja location está À FRENTE, o replay não sabe qual era
// o estado anterior — e, por isso, não decrementa a liveness do record que o
// update tornou garbage. LiveRecords e LiveBytes ficam ACIMA do real.
//
// Este arquivo existe para que o fato esteja em TESTE, e não só em prosa: o
// número é o insumo do minimumLiveRatio da compaction (M4), e quem for
// implementá-la precisa encontrar a medida, não uma promessa. NÃO conserta o
// algoritmo — isso é escopo do M4 —, apenas fixa o que ele hoje produz e a
// direção segura da assimetria.

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// statsDoSegmento devolve as stats do único segmento do manifest.
func statsDoSegmento(t *testing.T, dir string) checkpoint.SegmentInfo {
	t.Helper()
	m, err := checkpoint.Load(audManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Segments) != 1 {
		t.Fatalf("%d segmentos no manifest, esperado 1: %+v", len(m.Segments), m.Segments)
	}
	return m.Segments[0]
}

// atualizaLv sobrescreve a row "lv" lendo antes o estado corrente, como faz
// quem escreve de verdade (o materializer passa o prev que acabou de ler).
func atualizaLv(t *testing.T, s *Store, seq int64, status string) {
	t.Helper()
	prev := mustGetAud(t, s, "lv")
	mustPut(t, s, "lv", seq, statusPayload(t, s, "lv", status, seq), &prev)
}

// TestResiduoDeLivenessDoReplayNaoConverge mede o resíduo e prova que ele NÃO
// se corrige sozinho.
//
// O comentário do replayRecord afirmava que "o contador converge no próximo
// checkpoint que enxergar a row inteira". Não converge: o decremento do
// applyRecord (§10.1 passo 3) lê o estado anterior por s.locs.Get(rowID), que
// no instante do update já aponta para o record do TAIL. O record órfão — o
// que estava vivo no ponto S — nunca volta a ser nomeado por location nenhuma,
// então nenhum checkpoint futuro tem como descontá-lo. O erro é permanente
// até uma compaction reconstruir o tally varrendo o segmento.
func TestResiduoDeLivenessDoReplayNaoConverge(t *testing.T) {
	// A referência é a MESMA sequência de escritas sem crash nenhum, medida
	// aqui e não cravada: o que o replay deveria ter produzido em cada fase.
	// Assim o teste sobrevive a qualquer mudança no tamanho do record — o
	// resíduo é expresso como "o real MAIS o record órfão", que é o que ele é.
	certo := livenessSemCrash(t)

	dir := t.TempDir()
	opts := Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}}

	// 1. Row criada e CHECKPOINTADA: a partir daqui ela existe no ponto S, e
	// o record dela é o que vai virar órfão.
	s := audOpen(t, opts)
	mustPut(t, s, "lv", 1, statusPayload(t, s, "lv", "Start", 1), nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	emS := statsDoSegmento(t, dir)
	if emS.TotalRecords != 1 || emS.LiveRecords != 1 {
		t.Fatalf("estado inicial inesperado: %+v", emS)
	}
	bytesDoOrfao := emS.LiveBytes

	// 2. Update no tail e crash entre os passos 3 e 4 do §10.2: as locations
	// ficam duráveis apontando para o record NOVO, o manifest não.
	atualizaLv(t, s, 2, "Stop")
	if err := s.PartialCheckpointForTest(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 3. Recovery: o replay do tail reaplica o update sem conhecer o estado
	// anterior, e o Open termina com um checkpoint imediato.
	s2 := audOpen(t, opts)
	defer s2.Close()
	if rec := mustGetAud(t, s2, "lv"); rec.Sequence != 2 {
		t.Fatalf("o estado do tail deveria estar visível: %+v", rec)
	}
	depoisDoReplay := statsDoSegmento(t, dir)
	t.Logf("após o replay:                          %+v", depoisDoReplay)
	t.Logf("  o certo seria:                        %+v (órfão de %d bytes)",
		certo.doisRecords, bytesDoOrfao)
	exigeAssimetriaSegura(t, "após o replay", depoisDoReplay, certo.doisRecords, bytesDoOrfao)

	// 4. Um TERCEIRO update com checkpoint completo — o "próximo checkpoint
	// que enxerga a row inteira". O contador do órfão continua lá: o que este
	// update desconta é o record do tail, não o de antes dele.
	atualizaLv(t, s2, 3, "Start")
	if err := s2.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	depoisDoTerceiro := statsDoSegmento(t, dir)
	t.Logf("após o terceiro update + checkpoint:    %+v", depoisDoTerceiro)
	t.Logf("  o certo seria:                        %+v", certo.tresRecords)
	exigeAssimetriaSegura(t, "após o terceiro update + checkpoint",
		depoisDoTerceiro, certo.tresRecords, bytesDoOrfao)
}

// livenessSemCrash roda a mesma sequência de escritas sem crash nenhum e
// devolve as stats corretas de cada fase. É a contraprova de que o resíduo é
// do REPLAY: o write path fecha a conta exata.
func livenessSemCrash(t *testing.T) struct{ doisRecords, tresRecords checkpoint.SegmentInfo } {
	t.Helper()
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir, IndexedFields: []types.FieldID{fldStatus}})
	defer s.Close()

	mustPut(t, s, "lv", 1, statusPayload(t, s, "lv", "Start", 1), nil)
	atualizaLv(t, s, 2, "Stop")
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var certo struct{ doisRecords, tresRecords checkpoint.SegmentInfo }
	certo.doisRecords = statsDoSegmento(t, dir)

	atualizaLv(t, s, 3, "Start")
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	certo.tresRecords = statsDoSegmento(t, dir)

	for _, c := range []checkpoint.SegmentInfo{certo.doisRecords, certo.tresRecords} {
		if c.LiveRecords != 1 {
			t.Fatalf("write path sem crash deveria deixar 1 record vivo, obtido %+v", c)
		}
	}
	if certo.doisRecords.TotalRecords != 2 || certo.tresRecords.TotalRecords != 3 {
		t.Fatalf("contagem total inesperada na referência: %+v / %+v",
			certo.doisRecords, certo.tresRecords)
	}
	return certo
}

// exigeAssimetriaSegura separa as duas afirmações que o M4 precisa encontrar:
//
//   - a INVARIANTE, que vale para sempre e é de segurança: a liveness pode
//     ficar acima do real (superestimar só adia a compaction), NUNCA abaixo —
//     subestimar faria a compaction descartar dado vivo;
//   - a MEDIDA de hoje: o excesso é EXATAMENTE um record órfão, em contagem e
//     em bytes. Se ela mudar, o teste falha de propósito, com a instrução do
//     que fazer.
func exigeAssimetriaSegura(t *testing.T, momento string, obtido, certo checkpoint.SegmentInfo, bytesDoOrfao uint64) {
	t.Helper()
	if obtido.TotalRecords != certo.TotalRecords {
		t.Fatalf("%s: TotalRecords = %d, esperado %d — o total não tem resíduo nenhum",
			momento, obtido.TotalRecords, certo.TotalRecords)
	}
	if obtido.LiveRecords < certo.LiveRecords || obtido.LiveBytes < certo.LiveBytes {
		t.Fatalf("%s: liveness ABAIXO do real (obtido %+v, real %+v) — "+
			"subestimar faz a compaction descartar dado vivo",
			momento, obtido, certo)
	}
	esperado := certo
	esperado.LiveRecords += 1
	esperado.LiveBytes += bytesDoOrfao
	if obtido.LiveRecords != esperado.LiveRecords || obtido.LiveBytes != esperado.LiveBytes {
		t.Fatalf("%s: liveness = %d records / %d bytes; o resíduo medido do replay é de "+
			"exatamente um record órfão sobre o real (%d records / %d bytes), o que dá "+
			"%d / %d. Se a conta passou a fechar exato, a correção do M4 aconteceu: "+
			"atualize esta expectativa, o comentário do replayRecord em "+
			"store_recovery.go e o godoc de checkpoint.SegmentInfo, que é onde o "+
			"contrato do limite superior está publicado para quem consome as stats",
			momento, obtido.LiveRecords, obtido.LiveBytes,
			certo.LiveRecords, certo.LiveBytes, esperado.LiveRecords, esperado.LiveBytes)
	}
}
