package replicationkafka_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestBadEventRawRendersAsJSONString: o evento que não parseia vai para a
// histórica com `_raw` cru, sem validação (materializer/history.go). Pelo
// MATERIALIZER REAL, a linha tem de sair num envelope JSON válido e UTF-8
// válido, com o `_raw` como STRING — nunca inline, que envenenaria o tópico.
func TestBadEventRawRendersAsJSONString(t *testing.T) {
	cases := []struct {
		name  string
		event string
	}{
		{"broken JSON", `{"id": "A", broken`},
		{"invalid UTF-8", "{\"id\":\"U\xff1\",\"inteiro\":1}"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newOracleFixture(t)
			if res := f.mat.Apply([]byte(c.event)); res.Err == nil || !res.Category.BadEvent() {
				t.Fatalf("o evento devia ser dado ruim: err=%v categoria=%v", res.Err, res.Category)
			}
			snap := f.histStore.OpenSnapshot()
			it := f.histStore.ScanLive(snap)
			defer func() { _ = it.Close() }()
			rows := 0
			for it.Next() {
				rec, _, live, err := f.histStore.GetByRowID(snap, it.RowID(), nil)
				if err != nil || !live {
					t.Fatalf("rowID %d: live=%v err=%v", it.RowID(), live, err)
				}
				rows++
				env, err := f.r.AppendEntry(nil, goldenEpoch, replication.Entry{
					Table: "events_history", Operation: replication.Insert, Record: rec})
				if err != nil {
					t.Fatalf("o `_raw` de evento ruim não pode ser erro de render: %v", err)
				}
				if !json.Valid(env) || !utf8.Valid(env) {
					t.Fatalf("envelope inválido (json=%v utf8=%v): %q", json.Valid(env), utf8.Valid(env), env)
				}
				var decoded struct {
					Record map[string]json.RawMessage `json:"record"`
				}
				if err := json.Unmarshal(env, &decoded); err != nil {
					t.Fatal(err)
				}
				var raw string
				if err := json.Unmarshal(decoded.Record[schema.RawFieldName], &raw); err != nil {
					t.Fatalf("`_raw` não saiu como string: %s (%v)", decoded.Record[schema.RawFieldName], err)
				}
				if want := strings.ToValidUTF8(c.event, "�"); raw != want {
					t.Errorf("`_raw` = %q, quer %q", raw, want)
				}
			}
			if rows != 1 {
				t.Fatalf("%d linhas na histórica, quer 1 (a do evento ruim)", rows)
			}
		})
	}
}

// TestLabelCacheIsPerTable: o cache de rótulos é POR TABELA. O `_raw`
// resolvido pelo Compiled da histórica não pode vazar para a linha do base,
// que não o conhece.
func TestLabelCacheIsPerTable(t *testing.T) {
	r, _, hist := newGoldenRenderer(t)
	p := payloadOf(t, hist, namedValue{"identifier", str("X")}, namedValue{schema.RawFieldName, doc(`{"a":1}`)})
	if _, err := r.AppendEntry(nil, goldenEpoch, replication.Entry{Table: goldenHistory, Operation: replication.Insert,
		Record: types.Record{RowID: 1, Payload: p}}); err != nil {
		t.Fatalf("linha da histórica: %v", err)
	}
	got, err := r.AppendEntry(nil, goldenEpoch, replication.Entry{Table: "events", Operation: replication.Update,
		Record: types.Record{RowID: 1, Key: []byte("X"), Payload: p}})
	if !errors.Is(err, replicationkafka.ErrUnknownField) {
		t.Errorf("linha do base com o `_raw`: err=%v env=%s; quer ErrUnknownField", err, got)
	}
}
