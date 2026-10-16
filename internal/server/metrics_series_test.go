package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/series"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// oneGroupReader é o contador de um grupo só, com a chave composta de um
// valor.
type oneGroupReader struct{ key []byte }

func (oneGroupReader) Limit() uint32 { return 2 }
func (r oneGroupReader) Group(id uint32) ([]byte, []byte, int64, int64, bool) {
	return r.key, []byte("g"), 1, 1, id == 1
}

type discardSeriesOutput struct{}

func (discardSeriesOutput) Publish(context.Context, []byte, []byte, time.Time) error { return nil }

// As duas séries do laço saem para o emissor só-Kafka, e com os valores do
// MESMO retrato que o emissor publica — não de um contador paralelo.
func TestSeriesSamplesReadTheLoopStatsOfAKafkaOnlyEmitter(t *testing.T) {
	var ns atomic.Int64
	ns.Store(time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC).UnixNano())
	// Cada leitura do relógio anda 1,5 s: a passada lê o relógio no início e
	// no fim, e dura 1,5 s.
	now := func() time.Time { return time.Unix(0, ns.Add(int64(1500*time.Millisecond))).UTC() }
	key, err := valuekey.AppendComposite(nil, []types.Value{{Type: types.TypeString, Bytes: []byte("g")}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := series.NewEmitter(series.Options{
		Dataset: "servicos", Counter: "c", Columns: []string{"olt"}, Reader: oneGroupReader{key: key},
		DisableFile: true, Outputs: []series.Output{discardSeriesOutput{}}, Every: 10 * time.Second, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.EmitOnce(context.Background(), time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	st := e.Stats()
	if st.LastPassDuration != 1500*time.Millisecond {
		t.Fatalf("montagem errada: duração da passada %v", st.LastPassDuration)
	}
	got := map[string]float64{}
	for _, s := range samplesOfSeries("servicos", []*series.Emitter{e}) {
		if s.Family == "" || s.Name == "" {
			t.Fatalf("amostra sem família ou nome: %+v", s)
		}
		got[s.Name] = s.Value
	}
	if v, ok := got[metrics.MetSeriesPassSeconds]; !ok || v != st.LastPassDuration.Seconds() {
		t.Errorf("%s = %v (presente %v), quero %v", metrics.MetSeriesPassSeconds, v, ok, st.LastPassDuration.Seconds())
	}
	if v, ok := got[metrics.MetSeriesSkippedBoundariesTotal]; !ok || v != float64(st.SkippedBoundaries) {
		t.Errorf("%s = %v (presente %v), quero %v", metrics.MetSeriesSkippedBoundariesTotal, v, ok, st.SkippedBoundaries)
	}
	if _, ok := got[metrics.MetSeriesRowsTotal]; ok {
		t.Error("emissor só-Kafka publicou série do arquivo")
	}
}
