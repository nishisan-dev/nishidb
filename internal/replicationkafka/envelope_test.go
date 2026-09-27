package replicationkafka_test

import (
	"bytes"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// goldenYAML é o dataset dos goldens: uma coluna com alias, um documento json
// com uma coluna derivada, um datetime, e as colunas de sistema que as regras
// de TTL e a histórica injetam. A histórica grava o `_raw`.
const goldenYAML = `
version: 1
name: events
schemaName: telecom
materialization:
  key:
    field: identifier
  mode: merge
  ttl:
    - expireAfterWrite: 2h
history:
  expireAfterWrite: 7d
  rawPayload: true
schema:
  include: ["*"]
  unknownFields: include
  fields:
    identifier:
      type: string
    severity:
      type: int64
      alias: sev
    raisedAt:
      type: datetime
      format: "yyyy-MM-dd'T'HH:mm:ss"
    persistent:
      type: json
      extract:
        - path: probe
          as: probe_ip
          type: string
`

const (
	goldenEpoch   = "0123456789abcdef0123456789abcdef"
	goldenHistory = "events_history"
)

// compiledPair compila o base e a irmã contra o MESMO dicionário, como o
// servidor faz (internal/server/history.go), e liga o espelho.
func compiledPair(t testing.TB, yaml string) (base, hist *schema.Compiled) {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.BuiltinDefaults)
	if err != nil {
		t.Fatalf("spec recusada: %v", err)
	}
	dict := fakestorage.NewDict()
	base, err = spec.Compile(dict)
	if err != nil {
		t.Fatalf("compile do base: %v", err)
	}
	hist, err = spec.DeriveHistory().Compile(dict)
	if err != nil {
		t.Fatalf("compile da histórica: %v", err)
	}
	base.AddMirror(hist)
	return base, hist
}

type namedValue struct {
	name string
	v    types.Value
}

// payloadOf monta o payload pela codificação REAL, na ordem crescente de
// FieldID que o storage exige — nada de bytes escritos à mão.
func payloadOf(t testing.TB, c *schema.Compiled, fields ...namedValue) []byte {
	t.Helper()
	type idValue struct {
		id types.FieldID
		v  types.Value
	}
	resolved := make([]idValue, 0, len(fields))
	for _, f := range fields {
		cf, ok := c.FieldByName([]byte(f.name))
		if !ok {
			t.Fatalf("campo %q não existe no schema %q", f.name, c.Name)
		}
		resolved = append(resolved, idValue{cf.ID, f.v})
	}
	sort.Slice(resolved, func(i, j int) bool { return resolved[i].id < resolved[j].id })
	var out []byte
	for _, f := range resolved {
		var err error
		if out, err = encoding.AppendField(out, f.id, f.v); err != nil {
			t.Fatalf("AppendField(%d): %v", f.id, err)
		}
	}
	return out
}

func str(s string) types.Value   { return types.Value{Type: types.TypeString, Bytes: []byte(s)} }
func i64(n int64) types.Value    { return types.Value{Type: types.TypeInt64, I64: n} }
func doc(s string) types.Value   { return types.Value{Type: types.TypeJSON, Bytes: []byte(s)} }
func ts(ms int64) types.Value    { return types.Value{Type: types.TypeTimestamp, I64: ms} }
func dtime(ms int64) types.Value { return types.Value{Type: types.TypeDateTime, I64: ms} }

func newGoldenRenderer(t testing.TB) (*replicationkafka.Renderer, *schema.Compiled, *schema.Compiled) {
	t.Helper()
	base, hist := compiledPair(t, goldenYAML)
	r, err := replicationkafka.NewRenderer("events", goldenHistory, base, hist)
	if err != nil {
		t.Fatalf("NewRenderer: %v", err)
	}
	return r, base, hist
}

// alarmPayload é o estado de um alarme com TODAS as famílias de coluna: as
// visíveis, a derivada (probe_ip) e as duas de sistema.
func alarmPayload(t testing.TB, base *schema.Compiled, severity int64) []byte {
	return payloadOf(t, base,
		namedValue{"identifier", str("ALM-1")},
		namedValue{"severity", i64(severity)},
		namedValue{"raisedAt", dtime(1790000000123)},
		namedValue{"persistent", doc(`{"probe":"10.0.0.1","state":"open","tags":["a","b"]}`)},
		namedValue{"probe_ip", str("10.0.0.1")},
		namedValue{schema.WriteInstantFieldName, ts(1790000001000)},
		namedValue{schema.TTLFieldName, i64(7200000)},
	)
}

var goldenAt = time.UnixMilli(1790000002456).UTC()

// TestEnvelopeGolden fixa os bytes de cada forma do envelope. A expectativa é
// LITERAL, escrita à mão a partir do contrato — nunca gerada pelo próprio
// renderizador.
func TestEnvelopeGolden(t *testing.T) {
	r, base, hist := newGoldenRenderer(t)
	state := alarmPayload(t, base, 3)
	const record = `{"identifier":"ALM-1","persistent":{"probe":"10.0.0.1","state":"open","tags":["a","b"]},"raisedAt":"2026-09-21T14:13:20.123Z","sev":3}`
	histPayload := payloadOf(t, hist,
		namedValue{"identifier", str("ALM-1")},
		namedValue{"severity", i64(3)},
		namedValue{schema.WriteInstantFieldName, ts(1790000001000)},
		namedValue{schema.RawFieldName, doc(`{"identifier":"ALM-1","severity":3}`)},
	)
	snapAt := time.UnixMilli(1790000009000).UTC()

	cases := []struct {
		name   string
		render func(dst []byte) ([]byte, error)
		want   string
	}{
		{"insert", func(dst []byte) ([]byte, error) {
			return r.AppendEntry(dst, goldenEpoch, replication.Entry{Sequence: 41, Table: "events", Operation: replication.Insert,
				Record: types.Record{RowID: 7, Sequence: 900, Key: []byte("ALM-1"), Payload: state}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":41,"dataset":"events","table":"events","op":"insert","key":"ALM-1","at":"2026-09-21T14:13:22.456Z","record":` + record + `}`},
		{"update", func(dst []byte) ([]byte, error) {
			return r.AppendEntry(dst, goldenEpoch, replication.Entry{Sequence: 42, Table: "events", Operation: replication.Update,
				Record: types.Record{RowID: 7, Key: []byte("ALM-1"), Payload: alarmPayload(t, base, 5)}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":42,"dataset":"events","table":"events","op":"update","key":"ALM-1","at":"2026-09-21T14:13:22.456Z","record":{"identifier":"ALM-1","persistent":{"probe":"10.0.0.1","state":"open","tags":["a","b"]},"raisedAt":"2026-09-21T14:13:20.123Z","sev":5}}`},
		{"delete carries last live state", func(dst []byte) ([]byte, error) {
			return r.AppendEntry(dst, goldenEpoch, replication.Entry{Sequence: 43, Table: "events", Operation: replication.Delete,
				Record: types.Record{RowID: 7, Flags: types.FlagTombstone, Key: []byte("ALM-1"), Payload: state}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":43,"dataset":"events","table":"events","op":"delete","key":"ALM-1","at":"2026-09-21T14:13:22.456Z","record":` + record + `}`},
		{"delete without prior state renders empty record", func(dst []byte) ([]byte, error) {
			tomb := payloadOf(t, base, namedValue{schema.WriteInstantFieldName, ts(1790000001000)})
			return r.AppendEntry(dst, goldenEpoch, replication.Entry{Sequence: 44, Table: "events", Operation: replication.Delete,
				Record: types.Record{RowID: 8, Flags: types.FlagTombstone, Key: []byte("ALM-2"), Payload: tomb}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":44,"dataset":"events","table":"events","op":"delete","key":"ALM-2","at":"2026-09-21T14:13:22.456Z","record":{}}`},
		{"snapshot uses reset cursor and snapshot instant", func(dst []byte) ([]byte, error) {
			return r.AppendSnapshot(dst, goldenEpoch, 100, snapAt, replication.Entry{Sequence: 7, Table: "events", Operation: replication.Insert,
				Record: types.Record{RowID: 7, Sequence: 900, Key: []byte("ALM-1"), Payload: state}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":100,"dataset":"events","table":"events","op":"snapshot","key":"ALM-1","at":"2026-09-21T14:13:29.000Z","record":` + record + `}`},
		{"history insert", func(dst []byte) ([]byte, error) {
			return r.AppendEntry(dst, goldenEpoch, replication.Entry{Sequence: 45, Table: goldenHistory, Operation: replication.Insert,
				Record: types.Record{RowID: 9001, Payload: histPayload}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":45,"dataset":"events","table":"events_history","op":"insert","key":"9001","at":"2026-09-21T14:13:22.456Z","record":{"identifier":"ALM-1","sev":3,"_raw":{"identifier":"ALM-1","severity":3}}}`},
		{"tail retain", func(dst []byte) ([]byte, error) {
			return r.AppendEntry(dst, goldenEpoch, replication.Entry{Sequence: 46, Table: goldenHistory, Operation: replication.Retain,
				Record: types.Record{RowID: 8000}, At: goldenAt})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":46,"dataset":"events","table":"events_history","op":"retain","floor":8000,"at":"2026-09-21T14:13:22.456Z"}`},
		{"snapshot retain", func(dst []byte) ([]byte, error) {
			return r.AppendSnapshot(dst, goldenEpoch, 100, snapAt, replication.Entry{Table: goldenHistory, Operation: replication.Retain,
				Record: types.Record{RowID: 8000}})
		}, `{"epoch":"0123456789abcdef0123456789abcdef","sequence":100,"dataset":"events","table":"events_history","op":"retain","floor":8000,"at":"2026-09-21T14:13:29.000Z"}`},
		{"reset", func(dst []byte) ([]byte, error) {
			return r.AppendReset(dst, goldenEpoch, replicationkafka.ResetGap, 100, snapAt)
		}, `{"epoch":"0123456789abcdef0123456789abcdef","dataset":"events","op":"reset","reason":"gap","cursor":100,"at":"2026-09-21T14:13:29.000Z"}`},
		{"snapshot_end", func(dst []byte) ([]byte, error) {
			return r.AppendSnapshotEnd(dst, goldenEpoch, 100, 17, snapAt), nil
		}, `{"epoch":"0123456789abcdef0123456789abcdef","dataset":"events","op":"snapshot_end","cursor":100,"count":17,"at":"2026-09-21T14:13:29.000Z"}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// O prefixo prova que o renderizador APPENDA e não sobrescreve.
			got, err := c.render([]byte("PFX"))
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if string(got) != "PFX"+c.want {
				t.Errorf("envelope divergente:\n got %s\nwant PFX%s", got, c.want)
			}
		})
	}
}

// Os quatro motivos de reset são a lista fechada do contrato, e o motivo fora
// dela é recusado em vez de sair como string vazia.
func TestResetReasonsSpellTheContract(t *testing.T) {
	r, _, _ := newGoldenRenderer(t)
	want := map[replicationkafka.ResetReason]string{
		replicationkafka.ResetStartup:           "startup",
		replicationkafka.ResetGap:               "gap",
		replicationkafka.ResetSnapshotFailed:    "snapshot_failed",
		replicationkafka.ResetPartitionsChanged: "partitions_changed",
	}
	for reason, name := range want {
		got, err := r.AppendReset(nil, goldenEpoch, reason, 1, goldenAt)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Contains(got, []byte(`"reason":"`+name+`"`)) || reason.String() != name {
			t.Errorf("motivo %d: envelope %s, String %q; quer %q", reason, got, reason.String(), name)
		}
	}
	for _, bad := range []replicationkafka.ResetReason{0, 5, 255} {
		got, err := r.AppendReset([]byte("PFX"), goldenEpoch, bad, 1, goldenAt)
		if !errors.Is(err, replicationkafka.ErrUnknownResetReason) {
			t.Errorf("motivo %d: err = %v, quer ErrUnknownResetReason", bad, err)
		}
		if string(got) != "PFX" {
			t.Errorf("motivo %d: dst = %q, quer intacto", bad, got)
		}
	}
}

// O header nishidb-op é a mesma grafia do `op` do JSON, e não aloca.
func TestOpHeaderSpellsEveryOp(t *testing.T) {
	want := map[replicationkafka.Op]string{
		replicationkafka.OpInsert: "insert", replicationkafka.OpUpdate: "update",
		replicationkafka.OpDelete: "delete", replicationkafka.OpSnapshot: "snapshot",
		replicationkafka.OpRetain: "retain", replicationkafka.OpReset: "reset",
		replicationkafka.OpSnapshotEnd: "snapshot_end",
	}
	for op, name := range want {
		if got := string(replicationkafka.OpHeader(op)); got != name || op.String() != name {
			t.Errorf("op %d: header %q, String %q; quer %q", op, got, op.String(), name)
		}
	}
	if h := replicationkafka.OpHeader(0); h != nil {
		t.Errorf("op inválido devolveu header %q", h)
	}
	if h := replicationkafka.OpHeader(99); h != nil {
		t.Errorf("op 99 devolveu header %q", h)
	}
	if replicationkafka.HeaderOp != "nishidb-op" {
		t.Errorf("HeaderOp = %q", replicationkafka.HeaderOp)
	}
}

// A key do registro Kafka é a autoritativa: bytes crus na base (mesmo UTF-8
// inválido, que o `key` do JSON troca por U+FFFD), rowID decimal na
// histórica, e a chave de controle no retain.
func TestKafkaKeyIsAuthoritative(t *testing.T) {
	r, base, _ := newGoldenRenderer(t)
	rawKey := []byte("k\xff1")
	e := replication.Entry{Sequence: 1, Table: "events", Operation: replication.Update,
		Record: types.Record{RowID: 3, Key: rawKey, Payload: alarmPayload(t, base, 1)}, At: goldenAt}

	k, err := r.KafkaKey([]byte("PFX"), e)
	if err != nil || !bytes.Equal(k, append([]byte("PFX"), rawKey...)) {
		t.Errorf("key da base = %q (%v), quer os bytes crus %q", k, err, rawKey)
	}
	env, err := r.AppendEntry(nil, goldenEpoch, e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(env, []byte(`"key":"k`+`\`+`ufffd1"`)) {
		t.Errorf("o `key` do JSON devia trocar o byte inválido por \\ufffd: %s", env)
	}

	h := replication.Entry{Table: goldenHistory, Operation: replication.Insert, Record: types.Record{RowID: 9001, Key: []byte("ignored")}}
	if k, err := r.KafkaKey(nil, h); err != nil || string(k) != "9001" {
		t.Errorf("key da histórica = %q (%v), quer 9001", k, err)
	}
	ret := replication.Entry{Table: goldenHistory, Operation: replication.Retain, Record: types.Record{RowID: 8000}}
	if k, err := r.KafkaKey(nil, ret); err != nil || !bytes.Equal(k, replicationkafka.ControlKey) {
		t.Errorf("key do retain = %q (%v), quer %q", k, err, replicationkafka.ControlKey)
	}
	if string(replicationkafka.ControlKey) != "__nishidb_control__" {
		t.Errorf("ControlKey = %q", replicationkafka.ControlKey)
	}
}

// A linha da histórica é resolvida pelo Compiled DA HISTÓRICA: é ele que
// conhece o `_raw`. Resolvida pelo do base, o `_raw` seria campo desconhecido.
func TestHistoryRowResolvesThroughHistorySchema(t *testing.T) {
	r, base, hist := newGoldenRenderer(t)
	rawField, ok := hist.FieldByName([]byte(schema.RawFieldName))
	if !ok {
		t.Fatal("a histórica não compilou o _raw")
	}
	if _, inBase := base.FieldByID(rawField.ID); inBase {
		t.Fatal("o base conhece o _raw: o teste não discriminaria qual Compiled resolve a histórica")
	}
	p := payloadOf(t, hist, namedValue{"identifier", str("X")}, namedValue{schema.RawFieldName, doc(`{"a":1}`)})
	got, err := r.AppendEntry(nil, goldenEpoch, replication.Entry{Table: goldenHistory, Operation: replication.Insert,
		Record: types.Record{RowID: 1, Payload: p}, At: goldenAt})
	if err != nil {
		t.Fatalf("a linha da histórica não renderizou: %v", err)
	}
	if !bytes.HasSuffix(got, []byte(`"record":{"identifier":"X","_raw":{"a":1}}}`)) {
		t.Errorf("record da histórica: %s", got)
	}
}

// O campo que o Compiled da tabela não conhece FALHA a renderização: pulá-lo
// publicaria um `record` que o `SELECT *` não devolve. O dst volta intacto.
func TestUnknownFieldIDFailsRender(t *testing.T) {
	r, base, _ := newGoldenRenderer(t)
	const unknown types.FieldID = 4000
	if _, ok := base.FieldByID(unknown); ok {
		t.Fatalf("o id %d existe no schema; escolha outro", unknown)
	}
	p := payloadOf(t, base, namedValue{"identifier", str("ALM-1")})
	p, err := encoding.AppendField(p, unknown, i64(1))
	if err != nil {
		t.Fatal(err)
	}
	e := replication.Entry{Sequence: 1, Table: "events", Operation: replication.Update,
		Record: types.Record{RowID: 3, Key: []byte("ALM-1"), Payload: p}, At: goldenAt}
	// Duas vezes: o miss não pode ser cacheado como "coluna oculta".
	for i := 0; i < 2; i++ {
		got, err := r.AppendEntry([]byte("PFX"), goldenEpoch, e)
		if !errors.Is(err, replicationkafka.ErrUnknownField) {
			t.Fatalf("passada %d: err = %v, quer ErrUnknownField", i, err)
		}
		if !strings.Contains(err.Error(), "4000") {
			t.Errorf("a mensagem precisa nomear o id do campo: %v", err)
		}
		if string(got) != "PFX" {
			t.Errorf("passada %d: dst = %q, quer intacto", i, got)
		}
	}
}

// Payload corrompido é bug, e sobe com o ErrCorrupt da codificação.
func TestCorruptPayloadFailsRender(t *testing.T) {
	r, base, _ := newGoldenRenderer(t)
	p := alarmPayload(t, base, 3)
	e := replication.Entry{Sequence: 1, Table: "events", Operation: replication.Update,
		Record: types.Record{RowID: 3, Key: []byte("ALM-1"), Payload: p[:len(p)-3]}, At: goldenAt}
	got, err := r.AppendEntry([]byte("PFX"), goldenEpoch, e)
	if !errors.Is(err, encoding.ErrCorrupt) {
		t.Fatalf("err = %v, quer encoding.ErrCorrupt", err)
	}
	if string(got) != "PFX" {
		t.Errorf("dst = %q, quer intacto", got)
	}
}

// Schema e Reset do feed não têm envelope, e o snapshot só exporta Insert e
// Retain. Todos são bug — nunca registro a pular —, e o dst volta intacto.
func TestSchemaAndResetOperationsAreRefused(t *testing.T) {
	r, base, _ := newGoldenRenderer(t)
	p := alarmPayload(t, base, 3)
	entry := func(op replication.Operation) replication.Entry {
		return replication.Entry{Sequence: 1, Table: "events", Operation: op,
			Record: types.Record{RowID: 3, Key: []byte("ALM-1"), Payload: p}, At: goldenAt}
	}
	for _, op := range []replication.Operation{replication.Schema, replication.Reset, 0, 99} {
		got, err := r.AppendEntry([]byte("PFX"), goldenEpoch, entry(op))
		if !errors.Is(err, replicationkafka.ErrUnsupportedOperation) || string(got) != "PFX" {
			t.Errorf("AppendEntry(op %d) = %q, %v; quer ErrUnsupportedOperation e dst intacto", op, got, err)
		}
		if _, err := replicationkafka.EntryOp(op); !errors.Is(err, replicationkafka.ErrUnsupportedOperation) {
			t.Errorf("EntryOp(%d) = %v", op, err)
		}
		if _, err := r.KafkaKey(nil, entry(op)); !errors.Is(err, replicationkafka.ErrUnsupportedOperation) {
			t.Errorf("KafkaKey(op %d) = %v", op, err)
		}
	}
	for _, op := range []replication.Operation{replication.Schema, replication.Reset, replication.Update, replication.Delete} {
		got, err := r.AppendSnapshot([]byte("PFX"), goldenEpoch, 10, goldenAt, entry(op))
		if !errors.Is(err, replicationkafka.ErrUnsupportedOperation) || string(got) != "PFX" {
			t.Errorf("AppendSnapshot(op %d) = %q, %v; quer ErrUnsupportedOperation e dst intacto", op, got, err)
		}
	}
}

// Entry de tabela que não é nem o base nem a irmã é recusada — inclusive a
// da histórica num renderizador de dataset sem histórica.
func TestEntryOfForeignTableIsRefused(t *testing.T) {
	r, base, _ := newGoldenRenderer(t)
	p := alarmPayload(t, base, 3)
	e := replication.Entry{Table: "other", Operation: replication.Insert,
		Record: types.Record{RowID: 1, Key: []byte("k"), Payload: p}}
	if _, err := r.AppendEntry(nil, goldenEpoch, e); !errors.Is(err, replicationkafka.ErrUnknownTable) {
		t.Errorf("AppendEntry: err = %v, quer ErrUnknownTable", err)
	}
	if _, err := r.KafkaKey(nil, e); !errors.Is(err, replicationkafka.ErrUnknownTable) {
		t.Errorf("KafkaKey: err = %v, quer ErrUnknownTable", err)
	}
	noHistory, err := replicationkafka.NewRenderer("events", "", base, nil)
	if err != nil {
		t.Fatal(err)
	}
	e.Table = goldenHistory
	if _, err := noHistory.AppendEntry(nil, goldenEpoch, e); !errors.Is(err, replicationkafka.ErrUnknownTable) {
		t.Errorf("histórica sem irmã: err = %v, quer ErrUnknownTable", err)
	}
}

func TestNewRendererRefusesInconsistentConfiguration(t *testing.T) {
	base, hist := compiledPair(t, goldenYAML)
	cases := map[string]func() (*replicationkafka.Renderer, error){
		"dataset vazio": func() (*replicationkafka.Renderer, error) { return replicationkafka.NewRenderer("", "", base, nil) },
		"base nil": func() (*replicationkafka.Renderer, error) {
			return replicationkafka.NewRenderer("events", "", nil, nil)
		},
		"nome sem schema": func() (*replicationkafka.Renderer, error) {
			return replicationkafka.NewRenderer("events", goldenHistory, base, nil)
		},
		"schema sem nome": func() (*replicationkafka.Renderer, error) {
			return replicationkafka.NewRenderer("events", "", base, hist)
		},
		"irmã com nome do base": func() (*replicationkafka.Renderer, error) {
			return replicationkafka.NewRenderer("events", "events", base, hist)
		},
	}
	for name, c := range cases {
		if _, err := c(); !errors.Is(err, replicationkafka.ErrInvalidRenderer) {
			t.Errorf("%s: err = %v, quer ErrInvalidRenderer", name, err)
		}
	}
}

// A troca de epoch (reinício do master) aparece no envelope seguinte: o
// cache da abertura não pode segurar a epoch velha.
func TestEpochChangeIsRendered(t *testing.T) {
	r, _, _ := newGoldenRenderer(t)
	a := r.AppendSnapshotEnd(nil, "aaaa", 1, 1, goldenAt)
	b := r.AppendSnapshotEnd(nil, "bbbb", 1, 1, goldenAt)
	if !bytes.HasPrefix(a, []byte(`{"epoch":"aaaa",`)) || !bytes.HasPrefix(b, []byte(`{"epoch":"bbbb",`)) {
		t.Errorf("epoch não acompanhou a troca: %s / %s", a, b)
	}
}

// O encoder não pode importar fmt, encoding/json nem reflect: o gate de zero
// alocação depende disso. Os imports são lidos pelo parser de Go (código como
// DADO), em todos os arquivos não-teste do pacote, e não por busca em texto.
func TestEnvelopeImportsNoFmtNorEncodingJSON(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, file, src, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		checked++
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatalf("%s: import %s: %v", file, imp.Path.Value, err)
			}
			switch path {
			case "fmt", "encoding/json", "reflect":
				t.Errorf("%s importa %q no caminho do envelope", file, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("nenhum arquivo de produção examinado: o gate não estaria olhando nada")
	}
}
