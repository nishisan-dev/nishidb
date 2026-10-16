package storage

import (
	"bytes"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// feedChange é a cópia de uma mudança entregue ao OnReplicationChange. O
// record chega emprestado (o feed real copia antes de voltar), então o
// gravador também copia.
type feedChange struct {
	op  replication.Operation
	rec types.Record
}

type feedRecorder struct{ changes []feedChange }

func (r *feedRecorder) record(_ string, op replication.Operation, rec types.Record, _ time.Time) {
	rec.Key = bytes.Clone(rec.Key)
	rec.Payload = bytes.Clone(rec.Payload)
	r.changes = append(r.changes, feedChange{op: op, rec: rec})
}

func (r *feedRecorder) reset() { r.changes = nil }

// storeWithExpirationAndFeed é o storeWithExpiration com o callback do feed
// ligado e o tamanho de segmento escolhido pelo teste.
func storeWithExpirationAndFeed(t *testing.T, dir string, segmentSize int64) (*Store, *relogioOfTest, *feedRecorder) {
	t.Helper()
	feed := &feedRecorder{}
	s := openStore(t, Options{
		Dir:                   dir,
		SegmentSize:           segmentSize,
		ActiveTTL:             true,
		FallbackDuration:      time.Hour,
		WriteInstantFieldName: "_writtenAt",
		TTLFieldName:          "_expiresIn",
		OnReplicationChange:   feed.record,
	})
	rel := &relogioOfTest{t: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)}
	s.UsaRelogio(rel.agora)
	return s, rel, feed
}

// sealedRowForCompaction grava a linha-alvo e enche o segmento dela até a
// rotação, para que a compaction possa copiá-la para frente.
func sealedRowForCompaction(t *testing.T, s *Store, rel *relogioOfTest, key string) (types.RowID, []byte, uint32, uint32) {
	t.Helper()
	live := payloadWithWrite(t, s, key, "Start", rel.t)
	rowID, _ := mustPut(t, s, key, 1, live, nil)
	sec, off := localizacaoOfRow(t, s, rowID)
	for i := 0; s.Stats().CurrentSegmentID == sec && i < 200; i++ {
		k := "fill-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		mustPut(t, s, k, int64(i), payloadWithWrite(t, s, k, "B", rel.t), nil)
	}
	if s.Stats().CurrentSegmentID == sec {
		t.Fatal("o segmento da linha-alvo não rotacionou: o cenário não exercita a compaction")
	}
	return rowID, live, sec, off
}

// requireSingleDelete exige que o feed tenha recebido exatamente um delete, da
// linha e key esperadas, com o último estado vivo no payload e a lápide no
// Flags.
func requireSingleDelete(t *testing.T, feed *feedRecorder, rowID types.RowID, key string, live []byte) {
	t.Helper()
	if len(feed.changes) != 1 {
		t.Fatalf("mudanças publicadas=%d, quer 1: %+v", len(feed.changes), feed.changes)
	}
	got := feed.changes[0]
	if got.op != replication.Delete {
		t.Fatalf("operação=%v, quer Delete", got.op)
	}
	if got.rec.RowID != rowID || string(got.rec.Key) != key {
		t.Fatalf("delete da linha errada: rowID=%d key=%q, quer %d %q", got.rec.RowID, got.rec.Key, rowID, key)
	}
	if got.rec.Flags&types.FlagTombstone == 0 {
		t.Fatalf("o delete perdeu o bit de lápide: flags=%#x", got.rec.Flags)
	}
	if !bytes.Equal(got.rec.Payload, live) {
		t.Fatalf("o delete não carrega o último estado vivo: payload=%x, quer %x", got.rec.Payload, live)
	}
}

// O coletor troca o payload da linha por uma lápide que só guarda o instante
// de escrita; o feed tem de levar o estado que SAIU, não a lápide.
func TestExpiredDeleteCarriesLastLiveState(t *testing.T) {
	s, rel, feed := storeWithExpirationAndFeed(t, t.TempDir(), 0)
	live := payloadWithWrite(t, s, "A", "Start", rel.t)
	id, _ := mustPut(t, s, "A", 7, live, nil)
	feed.reset()

	rel.advance(2 * time.Hour)
	expired, err := s.Expira(id)
	if err != nil || !expired {
		t.Fatalf("Expira: expirou=%v err=%v", expired, err)
	}
	requireSingleDelete(t, feed, id, "A", live)
}

// Voltar a escrever uma key apagada reusa o rowID e publica update, não
// insert: a linha nunca deixou de existir para a primary.
func TestResurrectedKeyPublishesUpdate(t *testing.T) {
	s, rel, feed := storeWithExpirationAndFeed(t, t.TempDir(), 0)
	id, _ := mustPut(t, s, "A", 1, payloadWithWrite(t, s, "A", "Start", rel.t), nil)
	rel.advance(2 * time.Hour)
	if expired, err := s.Expira(id); err != nil || !expired {
		t.Fatalf("Expira: expirou=%v err=%v", expired, err)
	}
	feed.reset()

	back := payloadWithWrite(t, s, "A", "Back", rel.t)
	again, created := mustPut(t, s, "A", 2, back, nil)
	if created || again != id {
		t.Fatalf("ressurreição: rowID=%d created=%v, quer %d false", again, created, id)
	}
	if len(feed.changes) != 1 || feed.changes[0].op != replication.Update {
		t.Fatalf("ressurreição publicou %+v, quer um Update", feed.changes)
	}
	// Aqui o estado anterior é a LÁPIDE, não nil: o update tem de levar o
	// estado novo, e não o que a linha tinha antes.
	if got := feed.changes[0].rec.Payload; !bytes.Equal(got, back) {
		t.Fatalf("a ressurreição publicou payload=%x, quer o estado novo %x", got, back)
	}
}

// Só o delete troca o payload pelo estado anterior; o update de uma linha
// viva publica o estado NOVO.
func TestUpdatePublishesNewStateNotPrevious(t *testing.T) {
	s, rel, feed := storeWithExpirationAndFeed(t, t.TempDir(), 0)
	mustPut(t, s, "A", 1, payloadWithWrite(t, s, "A", "Start", rel.t), nil)
	prev, _, pres, err := s.State([]byte("A"), nil)
	if err != nil || pres != types.PresenceLive {
		t.Fatalf("State: pres=%v err=%v", pres, err)
	}
	feed.reset()

	next := payloadWithWrite(t, s, "A", "Stop", rel.t)
	mustPut(t, s, "A", 2, next, &prev)
	if len(feed.changes) != 1 || feed.changes[0].op != replication.Update {
		t.Fatalf("update publicou %+v, quer um Update", feed.changes)
	}
	if got := feed.changes[0].rec.Payload; !bytes.Equal(got, next) {
		t.Fatalf("o update publicou payload=%x, quer o estado novo %x", got, next)
	}
}

// Um Put de lápide para uma key que nunca existiu não tem estado anterior: o
// delete sai com a própria lápide, e o writer segue são.
func TestTombstoneForUnknownKeyPublishesTombstone(t *testing.T) {
	s, rel, feed := storeWithExpirationAndFeed(t, t.TempDir(), 0)
	tomb, err := s.buildLapide(payloadWithWrite(t, s, "ghost", "Start", rel.t))
	if err != nil {
		t.Fatal(err)
	}
	tomb = bytes.Clone(tomb)
	if len(tomb) == 0 {
		t.Fatal("lápide vazia: o cenário não distingue a lápide de um payload nil")
	}
	feed.reset()

	if _, _, err := s.Put([]byte("ghost"), 1, types.FlagTombstone, tomb, nil); err != nil {
		t.Fatalf("Put de lápide para key nova: %v", err)
	}
	if len(feed.changes) != 1 || feed.changes[0].op != replication.Delete {
		t.Fatalf("lápide de key nova publicou %+v, quer um Delete", feed.changes)
	}
	if got := feed.changes[0].rec.Payload; !bytes.Equal(got, tomb) {
		t.Fatalf("o delete sem estado anterior publicou payload=%x, quer a lápide %x", got, tomb)
	}
	// O writer não falhou: uma escrita seguinte continua passando.
	mustPut(t, s, "other", 1, payloadWithWrite(t, s, "other", "Start", rel.t), nil)
}

// A expiração oportunista da compaction é a mesma mudança lógica do coletor:
// tem de chegar ao feed, e com o último estado vivo.
func TestCompactionExpiryPublishesDeleteWithLastState(t *testing.T) {
	s, rel, feed := storeWithExpirationAndFeed(t, t.TempDir(), 400)
	rowID, live, sec, off := sealedRowForCompaction(t, s, rel, "expiring")
	feed.reset()

	rel.advance(2 * time.Hour)
	copied, _, err := s.reapendaSeVivo(sec, off, recordIn(t, s, sec, off))
	if err != nil || !copied {
		t.Fatalf("reapendaSeVivo: copiado=%v err=%v", copied, err)
	}
	if flagsOfLocation(t, s, rowID)&types.FlagTombstone == 0 {
		t.Fatal("a compaction tinha de expirar a linha oportunisticamente")
	}
	requireSingleDelete(t, feed, rowID, "expiring", live)
}

// A cópia pura de uma linha viva só move bytes: o estado lógico é o mesmo e o
// feed não recebe nada.
func TestCopyForwardDoesNotPublish(t *testing.T) {
	s, rel, feed := storeWithExpirationAndFeed(t, t.TempDir(), 400)
	rowID, _, sec, off := sealedRowForCompaction(t, s, rel, "alive")
	feed.reset()

	copied, _, err := s.reapendaSeVivo(sec, off, recordIn(t, s, sec, off))
	if err != nil || !copied {
		t.Fatalf("reapendaSeVivo: copiado=%v err=%v", copied, err)
	}
	if newSec, _ := localizacaoOfRow(t, s, rowID); newSec == sec {
		t.Fatal("a location não andou: a cópia não aconteceu")
	}
	if len(feed.changes) != 0 {
		t.Fatalf("o copy-forward publicou %+v", feed.changes)
	}
}

// O replay da recovery reconstrói estado que já foi publicado antes da
// queda; republicá-lo inventaria mudanças num feed de época nova.
func TestReplayDoesNotPublish(t *testing.T) {
	dir := t.TempDir()
	s, rel, _ := storeWithExpirationAndFeed(t, dir, 0)
	id, _ := mustPut(t, s, "A", 1, payloadWithWrite(t, s, "A", "Start", rel.t), nil)
	prev, _, pres, err := s.State([]byte("A"), nil)
	if err != nil || pres != types.PresenceLive {
		t.Fatalf("State: pres=%v err=%v", pres, err)
	}
	mustPut(t, s, "A", 2, payloadWithWrite(t, s, "A", "Stop", rel.t), &prev)
	// SEM checkpoint: os dois records ficam no tail e a reabertura os
	// reaplica pelo replay.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, rel2, feed := storeWithExpirationAndFeed(t, dir, 0)
	rel2.t = rel.t
	rec, _, ok, err := s2.GetByRowID(s2.presente(), id, nil)
	if err != nil || !ok || rec.Sequence != 2 {
		t.Fatalf("o replay não reaplicou o tail: ok=%v seq=%d err=%v", ok, rec.Sequence, err)
	}
	if len(feed.changes) != 0 {
		t.Fatalf("o replay publicou %+v", feed.changes)
	}
}
