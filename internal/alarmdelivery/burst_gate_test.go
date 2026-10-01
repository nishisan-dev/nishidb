//go:build !race

package alarmdelivery

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Tamanhos da rajada medida: o menor roda em todo `make test`; o maior, que é
// o número registrado no aceite, só com disco escolhido ou pedido explícito.
const (
	burstGateEvents = 2_000
	burstFullEvents = 10_000
)

// TestBurstPersistenceThroughputIsMeasured mede quanto a varredura leva para
// admitir e persistir uma rajada com os limites reais: com a contrapressão, é
// a duração da passada numa rajada. Mede burstGateEvents eventos em
// t.TempDir(); com NISHIDB_BENCH_DIR (disco real, como os benchmarks de
// escrita) mede burstFullEvents nesse diretório, e com
// NISHIDB_ALARM_BURST_FULL=1 mede burstFullEvents em t.TempDir(). O log diz o
// tamanho e o lugar. O número medido vai para o aceite, não para cá; o teste
// não fixa teto, só recusa uma medida sobre rajada que perdeu evento.
func TestBurstPersistenceThroughputIsMeasured(t *testing.T) {
	events, root, where := burstGateEvents, t.TempDir(), "t.TempDir()"
	if os.Getenv("NISHIDB_ALARM_BURST_FULL") == "1" {
		events, where = burstFullEvents, "t.TempDir(), NISHIDB_ALARM_BURST_FULL=1"
	}
	if dir := os.Getenv("NISHIDB_BENCH_DIR"); dir != "" {
		var err error
		if root, err = os.MkdirTemp(dir, "alarm-burst-"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(root) })
		events, where = burstFullEvents, "NISHIDB_BENCH_DIR"
	}
	bodies := make([][]byte, events)
	for i := range bodies {
		bodies[i] = eventBody(t, fmt.Sprintf("burst-%05d", i))
	}
	q := testQueue(t, filepath.Join(root, "noc"), nil)
	start := time.Now()
	offerAllWithin(t, q, bodies, time.Duration(events)*50*time.Millisecond)
	admitted := time.Since(start)
	if err := q.close(); err != nil {
		t.Fatal(err)
	}
	persisted := time.Since(start)
	s := q.snapshot()
	onlyDrop(t, s, "", 0)
	if s.DiskEvents != events {
		t.Fatalf("a rajada medida não chegou inteira ao disco: %d de %d", s.DiskEvents, events)
	}
	t.Logf("rajada de %d eventos em %s (%s): admissão %s, admissão+persistência %s (%.0f eventos/s), espera acumulada %.2fs",
		events, root, where, admitted, persisted, float64(events)/persisted.Seconds(), s.AdmissionWaitSeconds)
}
