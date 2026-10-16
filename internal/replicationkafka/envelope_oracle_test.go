package replicationkafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// oracleYAML tem UMA coluna de cada tipo do wire format, uma com alias, uma
// congelada (que o `SELECT *` do base projeta e a histórica não tem), um json
// com coluna derivada, as colunas de sistema do TTL, e a histórica com `_raw`.
// `persistent` e `extra` NÃO são declaradas: chegam pela descoberta, como no
// piloto.
const oracleYAML = `
version: 1
name: events
schemaName: telecom
materialization:
  key:
    field: id
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
    id:
      type: string
    texto:
      type: string
      alias: t
    textoOriginal:
      frozenFrom: texto
    inteiro:
      type: int64
    real:
      type: double
    booleano:
      type: boolean
    quando:
      type: timestamp
    v4:
      type: ipv4
    v6:
      type: ipv6
    rede4:
      type: ipv4_prefix
    rede6:
      type: ipv6_prefix
    cru:
      type: bytes
    doc:
      type: json
      extract:
        - path: probe
          as: probe_ip
          type: string
    momento:
      type: datetime
      format: "yyyy-MM-dd'T'HH:mm:ss"
    hora:
      type: time
      format: "HH:mm:ss.SSS"
`

const (
	// eventFull tem TODAS as colunas do `SELECT *` presentes: a comparação
	// com a linha do oráculo é byte a byte, sem filtro de null.
	eventFull = `{"id":"A","texto":"ação ✓ \"q\" \\ tab\t fim","inteiro":-42,"real":0.1,` +
		`"booleano":true,"quando":"2026-09-27T12:34:56.789Z","v4":"10.0.0.1","v6":"2001:db8::1",` +
		`"rede4":"10.0.0.1/24","rede6":"2001:db8::/32","cru":"AAEC/w==",` +
		`"doc":{"probe":"10.1.1.1","x":[1,2]},"momento":"2026-08-22T20:54:02","hora":"20:54:02.123",` +
		`"persistent":{"state":"open","tags":["a","b"],"n":1.5},"extra":7}`
	// eventSparse deixa a maioria das colunas ausente.
	eventSparse = `{"id":"B","inteiro":1}`
	// eventUpdate REMOVE `real` (null é remoção) e muda `inteiro` por merge.
	eventUpdate = `{"id":"A","real":null,"inteiro":5}`
)

// historyWriter é o dublê da tabela irmã: grava cada linha num fakestorage
// com key sintética, porque a histórica não tem key.
type historyWriter struct {
	st *fakestorage.Store
	n  int64
}

func (w *historyWriter) Apenda(payload []byte) (types.RowID, error) {
	w.n++
	id, _, err := w.st.Put([]byte(strconv.FormatInt(w.n, 10)), w.n, 0, payload, nil)
	return id, err
}

// oracleFixture costura a ingestão REAL (materializer) e o motor de consulta
// REAL sobre os mesmos stores, com o base e a histórica registrados como o
// servidor registra.
type oracleFixture struct {
	base, hist           *schema.Compiled
	baseStore, histStore *fakestorage.Store
	eng                  *query.Engine
	mat                  *materializer.Materializer
	r                    *replicationkafka.Renderer
}

func newOracleFixture(t *testing.T, events ...string) *oracleFixture {
	t.Helper()
	base, hist := compiledPair(t, oracleYAML)
	f := &oracleFixture{
		base: base, hist: hist,
		baseStore: fakestorage.New(fakestorage.Indexes{}),
		histStore: fakestorage.New(fakestorage.Indexes{}),
		eng:       query.NewEngine(),
	}
	if err := f.eng.Register("events", base, f.baseStore); err != nil {
		t.Fatal(err)
	}
	if err := f.eng.Register("events_history", hist, f.histStore); err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.New(base, f.baseStore)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := hist.FieldByName([]byte(schema.RawFieldName))
	if !ok {
		t.Fatal("a histórica não compilou o _raw")
	}
	mat.TurnonHistory(&historyWriter{st: f.histStore}, raw)
	f.mat = mat
	for _, ev := range events {
		if res := mat.Apply([]byte(ev)); res.Err != nil {
			t.Fatalf("ingestão de %s: %v (%v)", ev, res.Err, res.Category)
		}
	}
	if f.r, err = replicationkafka.NewRenderer("events", "events_history", base, hist); err != nil {
		t.Fatal(err)
	}
	return f
}

// selectStar devolve as linhas NDJSON do `SELECT *` do motor real, cada uma
// sem o LF final.
func (f *oracleFixture) selectStar(t *testing.T, table, sql string) [][]byte {
	t.Helper()
	it, err := f.eng.Execute(context.Background(), table, sql, nil)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	var out [][]byte
	for it.Next() {
		line := executor.AppendRow(nil, it.Row())
		out = append(out, bytes.TrimSuffix(line, []byte("\n")))
	}
	if err := it.Err(); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return out
}

// recordOf renderiza o envelope da Entry e devolve só o objeto `record`.
func (f *oracleFixture) recordOf(t *testing.T, e replication.Entry) []byte {
	t.Helper()
	env, err := f.r.AppendEntry(nil, goldenEpoch, e)
	if err != nil {
		t.Fatalf("render da linha %d de %s: %v", e.Record.RowID, e.Table, err)
	}
	if !json.Valid(env) {
		t.Fatalf("envelope não é JSON válido: %s", env)
	}
	i := bytes.Index(env, []byte(`,"record":`))
	if i < 0 || env[len(env)-1] != '}' {
		t.Fatalf("envelope sem record: %s", env)
	}
	return env[i+len(`,"record":`) : len(env)-1]
}

// dropNulls tira do objeto do oráculo as colunas `null`, preservando os
// bytes de cada valor e a ordem das chaves. Devolve também quantas saíram.
func dropNulls(t *testing.T, line []byte) ([]byte, int) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(line))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("linha do oráculo não abre objeto: %s", line)
	}
	out := []byte{'{'}
	dropped := 0
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		name, ok := tok.(string)
		if !ok {
			t.Fatalf("chave não-string no oráculo: %v", tok)
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			t.Fatal(err)
		}
		if string(raw) == "null" {
			dropped++
			continue
		}
		if len(out) > 1 {
			out = append(out, ',')
		}
		out = executor.AppendJSONString(out, []byte(name))
		out = append(out, ':')
		out = append(out, raw...)
	}
	return append(out, '}'), dropped
}

func (f *oracleFixture) baseEntry(t *testing.T, key string, op replication.Operation) replication.Entry {
	t.Helper()
	rec, _, ok, err := f.baseStore.GetByKey(f.baseStore.OpenSnapshot(), []byte(key), nil)
	if err != nil || !ok {
		t.Fatalf("key %q não materializada (ok=%v err=%v)", key, ok, err)
	}
	return replication.Entry{Sequence: 1, Table: "events", Operation: op, Record: rec}
}

// TestRecordMatchesSelectStarNDJSON: o `record` é a linha do `SELECT *` do
// motor REAL — mesmos rótulos (alias), mesma ordem, mesmos bytes de valor —,
// menos as colunas null. O oráculo é o executor de consulta, não uma tabela
// escrita à mão.
func TestRecordMatchesSelectStarNDJSON(t *testing.T) {
	t.Run("full row matches byte for byte", func(t *testing.T) {
		f := newOracleFixture(t, eventFull)
		lines := f.selectStar(t, "events", `SELECT * FROM events WHERE id = 'A'`)
		if len(lines) != 1 {
			t.Fatalf("%d linhas do oráculo, quer 1", len(lines))
		}
		if _, dropped := dropNulls(t, lines[0]); dropped != 0 {
			t.Fatalf("a linha 'cheia' tem %d colunas null — o caso não seria byte a byte: %s", dropped, lines[0])
		}
		for _, want := range []string{`"t":`, `"textoOriginal":`, `"persistent":{`, `"cru":"AAEC/w=="`} {
			if !bytes.Contains(lines[0], []byte(want)) {
				t.Fatalf("o oráculo não exercita %s: %s", want, lines[0])
			}
		}
		for _, op := range []replication.Operation{replication.Insert, replication.Update, replication.Delete} {
			if got := f.recordOf(t, f.baseEntry(t, "A", op)); !bytes.Equal(got, lines[0]) {
				t.Errorf("op %d: record diverge do SELECT *:\n got %s\nwant %s", op, got, lines[0])
			}
		}
	})

	t.Run("absent columns are omitted", func(t *testing.T) {
		f := newOracleFixture(t, eventFull, eventSparse, eventUpdate)
		for _, key := range []string{"A", "B"} {
			lines := f.selectStar(t, "events", `SELECT * FROM events WHERE id = '`+key+`'`)
			if len(lines) != 1 {
				t.Fatalf("key %s: %d linhas do oráculo, quer 1", key, len(lines))
			}
			want, dropped := dropNulls(t, lines[0])
			if dropped == 0 {
				t.Fatalf("key %s: nenhuma coluna ausente — o caso não exercitaria a omissão: %s", key, lines[0])
			}
			got := f.recordOf(t, f.baseEntry(t, key, replication.Update))
			if !bytes.Equal(got, want) {
				t.Errorf("key %s: record diverge do SELECT * sem nulls:\n got %s\nwant %s", key, got, want)
			}
			if bytes.Contains(got, []byte("null")) {
				t.Errorf("key %s: record com null explícito: %s", key, got)
			}
		}
	})

	t.Run("history table", func(t *testing.T) {
		f := newOracleFixture(t, eventFull, eventSparse, eventUpdate)
		lines := f.selectStar(t, "events_history", `SELECT * FROM events_history`)
		snap := f.histStore.OpenSnapshot()
		it := f.histStore.ScanLive(snap)
		defer func() { _ = it.Close() }()
		i := 0
		for it.Next() {
			rec, _, live, err := f.histStore.GetByRowID(snap, it.RowID(), nil)
			if err != nil || !live {
				t.Fatalf("rowID %d: live=%v err=%v", it.RowID(), live, err)
			}
			if i >= len(lines) {
				t.Fatalf("o store tem mais linhas que o SELECT * (%d)", len(lines))
			}
			want, _ := dropNulls(t, lines[i])
			if !bytes.Contains(want, []byte(`"_raw":`)) {
				t.Fatalf("linha %d da histórica sem _raw: o caso não discriminaria o Compiled da irmã: %s", i, want)
			}
			got := f.recordOf(t, replication.Entry{Table: "events_history", Operation: replication.Insert, Record: rec})
			if !bytes.Equal(got, want) {
				t.Errorf("linha %d da histórica diverge do SELECT *:\n got %s\nwant %s", i, got, want)
			}
			i++
		}
		if i != 3 || len(lines) != 3 {
			t.Fatalf("store com %d linhas e SELECT * com %d; quer 3 e 3", i, len(lines))
		}
	})
}

// TestRecordOmitsSystemAndDerivedColumns: as colunas de sistema e a derivada
// ESTÃO no payload (conferido antes, senão o teste passaria por ausência) e
// ficam fora do record, como ficam fora do `SELECT *`.
func TestRecordOmitsSystemAndDerivedColumns(t *testing.T) {
	f := newOracleFixture(t, eventFull)
	e := f.baseEntry(t, "A", replication.Update)

	inPayload := map[string]bool{}
	var fr encoding.FieldReader
	fr.Reset(e.Record.Payload)
	for {
		id, _, ok := fr.Next()
		if !ok {
			break
		}
		if cf, known := f.base.FieldByID(id); known {
			inPayload[cf.Name] = true
		}
	}
	if err := fr.Err(); err != nil {
		t.Fatal(err)
	}
	hidden := []string{schema.WriteInstantFieldName, schema.TTLFieldName, "probe_ip"}
	for _, name := range hidden {
		if !inPayload[name] {
			t.Fatalf("%s não está no payload: o teste não provaria a omissão (payload tem %v)", name, inPayload)
		}
	}
	got := f.recordOf(t, e)
	for _, name := range hidden {
		if bytes.Contains(got, []byte(`"`+name+`":`)) {
			t.Errorf("record projetou a coluna oculta %s: %s", name, got)
		}
	}
	// A positiva no mesmo teste: o record não está vazio, e o documento pai
	// da derivada continua inteiro.
	if !bytes.Contains(got, []byte(`"doc":{"probe":"10.1.1.1","x":[1,2]}`)) {
		t.Errorf("record sem o documento pai da derivada: %s", got)
	}
}
