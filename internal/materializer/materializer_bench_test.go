package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// nullStore é o store MÍNIMO possível: devolve o estado corrente já montado e
// aceita a escrita sem guardar nada. Existe para o gate de alocação medir o
// Apply, e só ele — o fakestorage copia key e payload a cada Put e mantém
// mapas de postings, então mediria a alocação DELE.
//
// O contrato de buffer é respeitado (Key/Payload são janelas sobre o buf do
// caller), senão o gate deixaria passar um aliasing que o Store real quebra.
type nullStore struct {
	rowID   types.RowID
	seq     int64
	payload []byte
	key     []byte
	found   bool
}

func (s *nullStore) State(key, buf []byte) (types.Record, []byte, types.Presence, error) {
	rec, out, ok, err := s.GetByKey(key, buf)
	if err != nil || !ok {
		return types.Record{}, out, types.PresenceAbsent, err
	}
	return rec, out, types.PresenceLive, nil
}

func (s *nullStore) GetByKey(key, buf []byte) (types.Record, []byte, bool, error) {
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

func (s *nullStore) Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error) {
	return s.rowID, !s.found, nil
}

// newNullMaterializer monta o materializer sobre o nullStore, com o estado
// corrente semeado a partir do evento RADIUS completo quando found.
func newNullMaterializer(tb testing.TB, c *schema.Compiled, found bool, seq int64) *Materializer {
	tb.Helper()
	state, _ := benchState(tb, c)
	st := &nullStore{
		rowID:   42,
		seq:     seq,
		payload: state,
		key:     []byte("CMP-V0001PPCLP-013"),
		found:   found,
	}
	m, err := New(c, st)
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	return m
}

// Gate da Q10: o Apply não aloca em steady state — nem no create, nem no
// merge, nem no descarte por sequence.
func BenchmarkApplyCreateNullStore(b *testing.B) {
	c := benchCompiled(b)
	m := newNullMaterializer(b, c, false, 0)
	if r := m.Apply(benchRadiusEvent); r.Err != nil { // warm-up do dicionário
		b.Fatal(r.Err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRadiusEvent)))
	for b.Loop() {
		if r := m.Apply(benchRadiusEvent); r.Err != nil {
			b.Fatal(r.Err)
		}
	}
}

func BenchmarkApplyMergePatchNullStore(b *testing.B) {
	c := benchCompiled(b)
	m := newNullMaterializer(b, c, true, 1)
	if r := m.Apply(benchRadiusPatch); r.Err != nil {
		b.Fatal(r.Err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRadiusPatch)))
	for b.Loop() {
		if r := m.Apply(benchRadiusPatch); r.Err != nil {
			b.Fatal(r.Err)
		}
	}
}

func BenchmarkApplyStaleNullStore(b *testing.B) {
	c := benchCompiled(b)
	// Sequence corrente ACIMA da do patch: todo evento é descartado.
	m := newNullMaterializer(b, c, true, 1<<40)
	if r := m.Apply(benchRadiusPatch); r.Action != ActionStale {
		b.Fatalf("Action = %v, want Stale", r.Action)
	}
	b.ReportAllocs()
	for b.Loop() {
		if r := m.Apply(benchRadiusPatch); r.Action != ActionStale {
			b.Fatalf("Action = %v", r.Action)
		}
	}
}

// O mesmo dataset do patchYAML, sem sequence: repetir o MESMO evento num
// dataset com sequence vira stale da segunda iteração em diante, e o
// benchmark mediria o caminho de descarte em vez do merge.
const benchNoSeqYAML = `
version: 1
name: radius_bench
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  exclude:
    - debug
  unknownFields: include
  fields:
    id:
      type: string
    _SEQ:
      type: int64
    Acct-Status-Type:
      type: string
      index: true
    Framed-IP-Address:
      type: ipv4
    Event-Timestamp:
      type: timestamp
    Acct-Input-Octets:
      type: int64
`

// Referência realista com o dublê completo (índices, cópias, mapas): mede o
// Apply MAIS o fake, então não é gate — o fake copia key e payload a cada Put
// e mantém mapas de postings, e é daí que vêm as alocações.
func newFakeMaterializer(tb testing.TB) (*Materializer, *schema.Compiled) {
	tb.Helper()
	s, err := schema.Parse([]byte(benchNoSeqYAML), schema.BuiltinDefaults)
	if err != nil {
		tb.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	f, ok := c.FieldByName([]byte("Acct-Status-Type"))
	if !ok {
		tb.Fatal("Acct-Status-Type não compilado")
	}
	m, err := New(c, fakestorage.New(fakestorage.Indexes{Campos: []types.FieldID{f.ID}}))
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	return m, c
}

// Evento cheio sobre estado cheio: o pior caso do merge no dublê.
func BenchmarkApplyFullEventFake(b *testing.B) {
	m, _ := newFakeMaterializer(b)
	if r := m.Apply(benchRadiusEvent); r.Err != nil {
		b.Fatal(r.Err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRadiusEvent)))
	for b.Loop() {
		if r := m.Apply(benchRadiusEvent); r.Err != nil {
			b.Fatal(r.Err)
		}
	}
}

// Patch de 8 campos sobre estado de ~30: o formato real do interim-update.
func BenchmarkApplyMergePatchFake(b *testing.B) {
	m, _ := newFakeMaterializer(b)
	if r := m.Apply(benchRadiusEvent); r.Err != nil {
		b.Fatal(r.Err)
	}
	if r := m.Apply(benchRadiusPatch); r.Action != ActionUpdated {
		b.Fatalf("Action = %v, want Updated", r.Action)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRadiusPatch)))
	for b.Loop() {
		if r := m.Apply(benchRadiusPatch); r.Err != nil {
			b.Fatal(r.Err)
		}
	}
}
