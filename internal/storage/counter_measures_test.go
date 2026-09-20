package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
)

func measureOptions(dir string, disabled bool, window time.Duration) Options {
	opts := optionsWithCounters(dir, byOLT("rates", true))
	base := opts.CompileCountersCallbackFunction
	opts.CompileCountersCallbackFunction = func(d *fielddict.Dict) ([]counter.Definition, error) {
		defs, err := base(d)
		if err != nil {
			return nil, err
		}
		id, err := d.LookupOrCreate([]byte("_writtenAt"))
		if err != nil {
			return nil, err
		}
		defs[0].DisableTotal = disabled
		defs[0].WriteInstantField = id
		defs[0].Measures = []counter.MeasureDefinition{{Name: "rate", Window: window, Unit: time.Second}}
		return defs, nil
	}
	return opts
}

func requireOccurrences(t *testing.T, s *Store, at time.Time, want int64) {
	t.Helper()
	c := s.Counters()[0]
	id, ok := c.ByText([]byte("A"))
	if !ok {
		t.Fatal("grupo A ausente")
	}
	if got := c.MeasureCount(id, 0, at.UnixMilli()); got != want {
		t.Fatalf("ocorrências = %d, quero %d", got, want)
	}
}

func TestMeasuresCountEventsAndRecoverWithoutCountingCompaction(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "total", true: "no_total"}[disabled], func(t *testing.T) {
			dir := t.TempDir()
			opts := measureOptions(dir, disabled, time.Minute)
			s := openStore(t, opts)
			at := time.Now().Truncate(time.Second)
			payload := payloadServico(t, s, "x", "A", true, at)
			for i := 0; i < 1800; i++ {
				mustPut(t, s, "x", int64(i), payload, nil)
			}
			requireOccurrences(t, s, at, 1800)
			total := int64(1)
			if disabled {
				total = 0
			}
			exigeTotal(t, s, "rates", "A", total, 1)
			var steps []string
			s.SetCheckpointTraceForTest(func(step string) { steps = append(steps, step) })
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if a, b, c := slices.Index(steps, "segSync"), slices.Index(steps, "counterMeasures"), slices.Index(steps, "manifest"); a < 0 || b <= a || c <= b {
				t.Fatalf("ordem incorreta: %v", steps)
			}
			rowID, _ := mustPut(t, s, "x", 1801, payload, nil)
			sec, off := localizacaoOfRow(t, s, rowID)
			rec, _, err := s.secR.ReadRecord(sec, off, nil)
			if err != nil {
				t.Fatal(err)
			}
			if copied, _, err := s.reapendaSeVivo(sec, off, rec); err != nil || !copied {
				t.Fatalf("copy-forward = %v, %v", copied, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, opts)
			requireOccurrences(t, s, at, 1801)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, opts)
			requireOccurrences(t, s, at, 1801)
			requireOccurrences(t, s, at.Add(time.Minute), 0)
			reports, err := s.AuditaCounters(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if reports[0].DivergentGroups != 0 || reports[0].DivergentAggregates != 0 {
				t.Fatalf("auditoria divergiu: %+v", reports[0])
			}
		})
	}
}

func TestMeasureCheckpointUncommittedGenerationIsIgnored(t *testing.T) {
	dir := t.TempDir()
	opts := measureOptions(dir, true, time.Minute)
	s := openStore(t, opts)
	at := time.Now()
	p := payloadServico(t, s, "x", "A", true, at)
	mustPut(t, s, "x", 1, p, nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "x", 2, p, nil)
	if err := s.secW.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Counters()[0].SaveMeasures(s.epoch + 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, opts)
	requireOccurrences(t, s, at, 2)
}

func TestMeasureSnapshotMissingOrCorruptRefusesOpen(t *testing.T) {
	for _, corruption := range []string{"missing", "truncated", "checksum", "generation"} {
		t.Run(corruption, func(t *testing.T) {
			dir := t.TempDir()
			opts := measureOptions(dir, true, time.Minute)
			s := openStore(t, opts)
			m, err := checkpoint.Load(s.manifestPath())
			if err != nil {
				t.Fatal(err)
			}
			if len(m.MeasureCounters) != 1 {
				t.Fatal("manifest não declara measures")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "indexes", "contadores", "rates", "*.snapshot"))
			if err != nil || len(files) != 1 {
				t.Fatalf("snapshots: %v %v", files, err)
			}
			switch corruption {
			case "missing":
				err = os.Remove(files[0])
			case "truncated":
				err = os.WriteFile(files[0], []byte("broken"), 0o644)
			default:
				data, readErr := os.ReadFile(files[0])
				if readErr != nil {
					t.Fatal(readErr)
				}
				if corruption == "checksum" {
					data[len(data)-1] ^= 1
				} else {
					binary.LittleEndian.PutUint64(data[4:12], m.Epoch+1)
					binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.ChecksumIEEE(data[:len(data)-4]))
				}
				err = os.WriteFile(files[0], data, 0o644)
			}
			if err != nil {
				t.Fatal(err)
			}
			opened, err := Open(opts)
			if opened != nil {
				_ = opened.Close()
			}
			if !errors.Is(err, ErrDatasetCorrupt) {
				t.Fatalf("abertura = %v, quero corrupção", err)
			}
		})
	}
}

func TestChangingMeasureWindowResetsWithoutBackfill(t *testing.T) {
	dir := t.TempDir()
	opts := measureOptions(dir, true, time.Minute)
	s := openStore(t, opts)
	at := time.Now()
	p := payloadServico(t, s, "x", "A", true, at)
	mustPut(t, s, "x", 1, p, nil)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	mustPut(t, s, "x", 2, p, nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, measureOptions(dir, true, 5*time.Minute))
	requireOccurrences(t, s, at, 0)
	mustPut(t, s, "x", 3, p, nil)
	requireOccurrences(t, s, at, 1)
}

func TestMeasuresRecoverAtEveryCheckpointBoundary(t *testing.T) {
	for _, stage := range []string{"segSync", "applyOverlayContadores", "msyncContadores", "counterMeasures", "manifest"} {
		t.Run(stage, func(t *testing.T) {
			opts := measureOptions(t.TempDir(), true, time.Minute)
			s := openStore(t, opts)
			at := time.Now()
			p := payloadServico(t, s, "x", "A", true, at)
			mustPut(t, s, "x", 1, p, nil)
			if err := s.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			mustPut(t, s, "x", 2, p, nil)
			stopped := false
			s.SetCheckpointTraceForTest(func(step string) {
				if step == stage {
					stopped = true
					panic(stage)
				}
			})
			func() {
				defer func() {
					if v := recover(); v != stage {
						t.Fatalf("interrupção = %v", v)
					}
				}()
				if err := s.Checkpoint(); err != nil {
					t.Fatal(err)
				}
			}()
			if !stopped {
				t.Fatal("não alcançou a etapa de interrupção")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openStore(t, opts)
			requireOccurrences(t, s, at, 2)
		})
	}
}

func TestTogglingTotalPreservesMeasuresAndRebuildDoesNotCountEvents(t *testing.T) {
	dir := t.TempDir()
	at := time.Now()
	s := openStore(t, measureOptions(dir, false, time.Minute))
	p := payloadServico(t, s, "x", "A", true, at)
	for i := range 3 {
		mustPut(t, s, "x", int64(i), p, nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, measureOptions(dir, true, time.Minute))
	requireOccurrences(t, s, at, 3)
	if _, err := s.ReconstroiCounters(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireOccurrences(t, s, at, 3)
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, measureOptions(dir, false, time.Minute))
	requireOccurrences(t, s, at, 3)
	exigeTotal(t, s, "rates", "A", 1, 1)
}
