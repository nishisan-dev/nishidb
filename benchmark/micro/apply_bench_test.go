package micro

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// voidStore é o piso da medição: aceita tudo, não guarda nada e devolve um
// estado corrente fixo quando configurado. Isola o custo do MATERIALIZER —
// com o dublê completo ou com o Store real, o que se mede é a soma dos dois.
type voidStore struct {
	rowID   types.RowID
	seq     int64
	key     []byte
	payload []byte
	found   bool
}

func (s *voidStore) State(key, buf []byte) (types.Record, []byte, types.Presence, error) {
	rec, out, ok, err := s.GetByKey(key, buf)
	if err != nil || !ok {
		return types.Record{}, out, types.PresenceAbsent, err
	}
	return rec, out, types.PresenceLive, nil
}

func (s *voidStore) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
	if !s.found {
		return types.Record{}, buf, false, nil
	}
	buf = buf[:0]
	buf = append(buf, s.key...)
	keyEnd := len(buf)
	buf = append(buf, s.payload...)
	return types.Record{
		RowID:    s.rowID,
		Sequence: s.seq,
		Key:      buf[:keyEnd:keyEnd],
		Payload:  buf[keyEnd:len(buf):len(buf)],
	}, buf, true, nil
}

func (s *voidStore) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error) {
	return s.rowID, !s.found, nil
}

// stateOfReference devolve o estado materializado de uma entidade real,
// tirado do PRÓPRIO pipeline. Montá-lo à mão arriscaria medir o merge contra
// um payload que a ingestão nunca produziria.
func stateOfReference(b *testing.B, events [][]byte) (key, payload []byte) {
	b.Helper()
	m, st, _ := fakeSetup(b)
	r := m.Apply(events[0])
	if r.Err != nil {
		b.Fatal(r.Err)
	}
	snap := st.OpenSnapshot()
	defer st.CloseSnapshot(snap)
	rec, _, ok, err := st.GetByRowID(snap, r.RowID, nil)
	if err != nil || !ok {
		b.Fatalf("estado de referência ausente: ok=%v err=%v", ok, err)
	}
	return append([]byte(nil), rec.Key...), append([]byte(nil), rec.Payload...)
}

// aplicaCiclando roda o corpus em ciclo pelo b.Loop.
func aplicaCiclando(b *testing.B, m *materializer.Materializer, events [][]byte, want materializer.Action) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(events[0])))
	i := 0
	for b.Loop() {
		r := m.Apply(events[i%len(events)])
		if r.Err != nil {
			b.Fatal(r.Err)
		}
		if want != materializer.ActionFailed && r.Action != want {
			b.Fatalf("Action = %v, want %v", r.Action, want)
		}
		i++
	}
}

// BenchmarkApplyCreate: entidade nova a cada evento — a fase LOAD.
func BenchmarkApplyCreate(b *testing.B) {
	events := corpusCheio(b, 7, 4096)
	m, err := materializer.New(compiledSetup(b), &voidStore{rowID: 1})
	if err != nil {
		b.Fatal(err)
	}
	// Warm-up: registra no dicionário os campos que só aparecem em parte do
	// corpus, para medir o steady state.
	for _, e := range events[:64] {
		if r := m.Apply(e); r.Err != nil {
			b.Fatal(r.Err)
		}
	}
	aplicaCiclando(b, m, events, materializer.ActionCreated)
}

// BenchmarkApplyMergePatch: interim-update parcial sobre estado existente —
// o caminho de 85% do tráfego.
func BenchmarkApplyMergePatch(b *testing.B) {
	events := corpusPatch(b, 8, 4096)
	key, payload := stateOfReference(b, corpusCheio(b, 8, 1))
	// Sequence corrente baixa: todo patch do corpus é mais novo e aplica.
	m, err := materializer.New(compiledSetup(b), &voidStore{
		rowID: 42, seq: 1, key: key, payload: payload, found: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, e := range events[:64] {
		if r := m.Apply(e); r.Err != nil {
			b.Fatal(r.Err)
		}
	}
	aplicaCiclando(b, m, events, materializer.ActionUpdated)
}

// BenchmarkApplyStale: evento antigo descartado pelo sequence check. Precisa
// ser barato porque numa reentrega de Kafka ele é a maioria.
func BenchmarkApplyStale(b *testing.B) {
	events := corpusPatch(b, 9, 4096)
	key, payload := stateOfReference(b, corpusCheio(b, 9, 1))
	m, err := materializer.New(compiledSetup(b), &voidStore{
		rowID: 42, seq: 1 << 40, key: key, payload: payload, found: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	if r := m.Apply(events[0]); r.Action != materializer.ActionStale {
		b.Fatalf("Action = %v, want Stale", r.Action)
	}
	aplicaCiclando(b, m, events, materializer.ActionStale)
}

// BenchmarkApplyIngestFake: pipeline completo contra o dublê, com índices e
// estado retido. Mede Apply + dublê — referência entre commits, não número
// absoluto.
func BenchmarkApplyIngestFake(b *testing.B) {
	m, _, _ := fakeSetup(b)
	events := corpusMisto(b, 10, 8192)
	aplicaTudo(b, m, events[:512]) // popula as entidades
	aplicaCiclando(b, m, events, materializer.ActionFailed)
}

// BenchmarkApplyIngestReal: o número que interessa ao harness — pipeline
// inteiro sobre o Store REAL, com append durável, índices e publicação
// atômica.
//
// Exige NISHIDB_BENCH_DIR em disco real: cada iteração appenda um record.
func BenchmarkApplyIngestReal(b *testing.B) {
	m, _, _ := realSetup(b, writableBenchDir(b), corpusEntities)
	events := corpusMisto(b, 11, 8192)
	aplicaTudo(b, m, events[:512])
	aplicaCiclando(b, m, events, materializer.ActionFailed)
}
