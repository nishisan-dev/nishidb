//go:build !race

package metricwindow_test

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

// TestFileAccessorsDoNotAllocate: o amostrador grava um balde por grupo a
// cada fechamento e lê identidade e baldes de todos os grupos na subida — com
// milhares de grupos, uma alocação por chamada viraria lixo proporcional ao
// número de grupos a cada passada. O gate mede as quatro operações sobre ids
// já cobertos pela capacidade, com o seam de ordem de escrita no lugar (nil,
// como em produção); o crescimento é reescrita de arquivo, I/O, e fica fora da
// medição.
func TestFileAccessorsDoNotAllocate(t *testing.T) {
	h := metricwindow.Header{BucketCount: 4, BucketWidthMs: 5_000, Source: 1}
	f, _, err := metricwindow.Open(filepath.Join(t.TempDir(), "w.mwin"), h, 1, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	capacity := f.Capacity()

	var id uint32
	var slot uint32
	next := func() {
		id = (id + 1) % capacity
		slot = (slot + 1) % h.BucketCount
	}

	measures := []struct {
		name string
		op   func()
	}{
		{"SetIdentity", func() {
			next()
			f.SetIdentity(id, metricwindow.Identity{KeyHash: uint64(id), ValidFromEpoch: uint64(slot)})
		}},
		{"Identity", func() {
			next()
			_ = f.Identity(id)
		}},
		{"WriteBucket", func() {
			next()
			f.WriteBucket(slot, id, metricwindow.Bucket{Epoch: id, Count: uint16(slot), Sum: int64(id), Min: -1, Max: 1})
		}},
		{"Bucket", func() {
			next()
			_ = f.Bucket(slot, id)
		}},
	}
	for _, m := range measures {
		if n := testing.AllocsPerRun(1000, m.op); n != 0 {
			t.Errorf("%s: allocs/chamada = %.1f, quer 0", m.name, n)
		}
	}
}
