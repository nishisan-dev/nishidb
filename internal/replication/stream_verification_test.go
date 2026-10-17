package replication

import (
	"context"
	"testing"
	"time"
)

// A pendência de conferência é contável: com dois destinos Kafka no mesmo
// dataset, o feed continua pendente até o ÚLTIMO soltar a dele. E a função
// devolvida é idempotente: o Close que solta de novo o que a subida já
// soltou não pode apagar a pendência de outro destino.
func TestVerificationHoldIsCountedAndEachReleaseCountsOnce(t *testing.T) {
	s := newFeedForTest(t)
	if s.VerificationPending() {
		t.Fatal("feed recém-aberto já anuncia conferência pendente")
	}
	first := s.HoldForVerification()
	second := s.HoldForVerification()
	if !s.VerificationPending() {
		t.Fatal("duas pendências registradas e o feed não anuncia nenhuma")
	}
	first()
	first()
	if !s.VerificationPending() {
		t.Fatal("a segunda soltura da MESMA pendência apagou a do outro destino")
	}
	second()
	if s.VerificationPending() {
		t.Fatal("as duas pendências foram soltas e o feed continua pendente")
	}
}

// A pendência só é anunciada: o feed continua aceitando Publish e servindo o
// Acquire (o publicador Kafka lê o feed sem passar por ela), e o Rotate do
// rewind acontece com ela registrada.
func TestVerificationHoldNeverBlocksTheFeedItself(t *testing.T) {
	s := newFeedForTest(t)
	release := s.HoldForVerification()
	defer release()
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Flush(now, "test")
	epoch, _ := s.Cursor()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, done, err := s.Acquire(ctx, epoch, 0)
	if err != nil || b.To != 1 {
		t.Fatalf("Acquire com a pendência registrada: lote %+v, err %v", b, err)
	}
	done()
	if !s.RotateFrom(epoch, "rewind") {
		t.Fatal("o Rotate do rewind não aconteceu com a pendência registrada")
	}
	if !s.VerificationPending() {
		t.Fatal("o Rotate soltou a pendência; quem solta é o publicador")
	}
}

// OriginAndPosition devolve o mesmo que Origin e Position, na geração
// retomada e na rotacionada.
func TestOriginAndPositionMatchesOriginAndPositionAfterRotate(t *testing.T) {
	s := newFeedForTest(t)
	now := time.Unix(10, 0)
	s.Publish("events", Insert, rec(1, "a"), now)
	s.Flush(now, "test")
	check := func(stage string) {
		t.Helper()
		origin, epoch, sequence, endPos := s.OriginAndPosition()
		e, q, p := s.Position()
		if origin != s.Origin() || epoch != e || sequence != q || endPos != p {
			t.Fatalf("%s: OriginAndPosition = (%+v, %s, %d, %d), Origin/Position = (%+v, %s, %d, %d)",
				stage, origin, epoch, sequence, endPos, s.Origin(), e, q, p)
		}
	}
	check("geração retomada")
	old, _ := s.Cursor()
	s.Rotate("test")
	check("geração rotacionada")
	if origin, epoch, _, _ := s.OriginAndPosition(); origin.Kind != OriginRotated || origin.PreviousEpoch != old || epoch == old {
		t.Fatalf("depois do Rotate: origem %+v, epoch %s (anterior %s)", origin, epoch, old)
	}
}
