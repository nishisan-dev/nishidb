package server_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// Os doze casos do §106 pelo caminho HTTP.
//
// Eles já existem contra o Engine (internal/query/spec106_test.go), e repeti-los
// aqui não é redundância: entre o Engine e o cliente há projeção, alias,
// serialização e moldura, e um erro de fiação em qualquer um desses degraus
// aparece SÓ neste caminho — que é justamente o que o driver da Fase 7 vai
// usar. Um campo quoted que chegasse ao cliente com o nome errado passaria
// verde na suíte do motor.

// spec106YAML tem o que os doze casos exigem: chave, campo com alias E índice,
// SEGUNDO campo indexado (para o AND/OR de dois termos), numérico para
// comparação, campo com hífen (que obriga aspas no SQL) e um campo que NENHUM
// evento traz, para o caso NULL.
const spec106YAML = `version: 1
name: %s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    Acct-Status-Type:
      type: string
      alias: status
      index: true
    nas:
      type: string
      index: true
    porta:
      type: int64
    ip:
      type: ipv4
    observacao:
      type: string
`

func servidorSpec106(t *testing.T) *server.Servidor {
	t.Helper()
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("d", strings.ReplaceAll(spec106YAML, "%s", "d"))

	cfg := a.carrega()
	spec, err := schema.Load(cfg.DatasetSpecPath("d"), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{
		Dir:                    cfg.DatasetDir("d"),
		Dataset:                "d",
		SegmentSize:            cfg.SegmentSize(),
		CamposIndexadosPorNome: schema.IndexedFieldNames(spec),
	})
	if err != nil {
		t.Fatal(err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	mat, err := materializer.New(compilado, st)
	if err != nil {
		t.Fatal(err)
	}
	// Seis linhas, escolhidas para que cada caso do §106 tenha um resultado
	// diferente do dos outros — dois casos com a mesma resposta não
	// discriminariam nada. `observacao` não aparece em nenhuma: é o caso NULL.
	eventos := []string{
		`{"id":"L-1","Acct-Status-Type":"Start","nas":"nas-a","porta":10,"ip":"10.0.0.1"}`,
		`{"id":"L-2","Acct-Status-Type":"Stop","nas":"nas-a","porta":20,"ip":"10.0.0.2"}`,
		`{"id":"L-3","Acct-Status-Type":"Stop","nas":"nas-b","porta":30,"ip":"10.0.0.3"}`,
		`{"id":"L-4","Acct-Status-Type":"Interim","nas":"nas-b","porta":40,"ip":"10.0.0.4"}`,
		`{"id":"L-5","Acct-Status-Type":"Stop","nas":"nas-c","porta":50,"ip":"10.0.0.5"}`,
		`{"id":"L-6","Acct-Status-Type":"Start","nas":"nas-c","porta":60,"ip":"10.0.0.6"}`,
	}
	for _, ev := range eventos {
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			_ = st.Close()
			t.Fatalf("evento %s: %v", ev, r.Err)
		}
	}
	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	srv, err := server.Novo(cfg, server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}

// idsDe extrai a coluna `id` das rows cruas, na ordem em que chegaram.
func idsDe(t *testing.T, rows []string) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		var m map[string]any
		if err := json.Unmarshal([]byte(r), &m); err != nil {
			t.Fatalf("row ilegível %q: %v", r, err)
		}
		id, _ := m["id"].(string)
		out = append(out, id)
	}
	return out
}

func TestSpec106PeloProtocoloHTTP(t *testing.T) {
	srv := servidorSpec106(t)

	casos := []struct {
		caso   string // o item do §106
		corpo  string
		querID []string
	}{
		{"primary lookup", `{"dataset":"d","sql":"SELECT id FROM d WHERE id = 'L-3'"}`,
			[]string{"L-3"}},

		{"bitmap lookup", `{"dataset":"d","sql":"SELECT id FROM d WHERE status = 'Stop'"}`,
			[]string{"L-2", "L-3", "L-5"}},

		// "hash lookup" é o nome histórico do caso — `nas` usa o MESMO índice
		// único que `status` desde a D5 (index: true é a única grafia); o que
		// o caso prova é que um SEGUNDO campo indexado responde certo.
		{"hash lookup", `{"dataset":"d","sql":"SELECT id FROM d WHERE nas = 'nas-b'"}`,
			[]string{"L-3", "L-4"}},

		{"AND", `{"dataset":"d","sql":"SELECT id FROM d WHERE status = 'Stop' AND nas = 'nas-a'"}`,
			[]string{"L-2"}},

		{"OR", `{"dataset":"d","sql":"SELECT id FROM d WHERE id = 'L-1' OR id = 'L-6'"}`,
			[]string{"L-1", "L-6"}},

		{"comparison", `{"dataset":"d","sql":"SELECT id FROM d WHERE porta >= 40"}`,
			[]string{"L-4", "L-5", "L-6"}},

		// NULL é AUSÊNCIA do campo, e nenhum evento traz `observacao`.
		{"NULL", `{"dataset":"d","sql":"SELECT id FROM d WHERE observacao IS NULL AND porta = 10"}`,
			[]string{"L-1"}},

		{"LIMIT", `{"dataset":"d","sql":"SELECT id FROM d LIMIT 2"}`,
			[]string{"L-1", "L-2"}},

		{"full scan", `{"dataset":"d","sql":"SELECT id FROM d"}`,
			[]string{"L-1", "L-2", "L-3", "L-4", "L-5", "L-6"}},

		// O alias vale na projeção E no WHERE, e é o nome que sai na chave.
		{"aliases", `{"dataset":"d","sql":"SELECT id, status FROM d WHERE status = 'Interim'"}`,
			[]string{"L-4"}},

		// Nome com hífen exige aspas duplas no SQL; o rótulo de saída é o que
		// o usuário escreveu.
		{"quoted fields", `{"dataset":"d","sql":"SELECT id, \"Acct-Status-Type\" FROM d WHERE \"Acct-Status-Type\" = 'Interim'"}`,
			[]string{"L-4"}},

		{"parameters", `{"dataset":"d","sql":"SELECT id FROM d WHERE status = ? AND porta > ?",
			"parameters":[{"type":"string","value":"Stop"},{"type":"int64","value":25}]}`,
			[]string{"L-3", "L-5"}},
	}

	for _, c := range casos {
		t.Run(c.caso, func(t *testing.T) {
			resp := consulta(t, srv, c.corpo)
			rows, l := resultado(t, resp)
			if l.Failure() != nil {
				t.Fatalf("fecho de erro: %+v", l.Failure())
			}
			if l.End() == nil {
				t.Fatal("a resposta não trouxe fecho de sucesso")
			}
			if got := idsDe(t, rows); !mesmosIDs(got, c.querID) {
				t.Fatalf("ids = %v, quer %v", got, c.querID)
			}
			if l.End().Rows != int64(len(c.querID)) {
				t.Fatalf("fecho diz %d rows, saíram %d", l.End().Rows, len(c.querID))
			}
		})
	}
}

// O rótulo das colunas na moldura é o de SAÍDA, e é ele que o driver vai casar
// com as chaves das rows. Divergir aqui faria o ResultSetMetaData descrever
// colunas que o dado não tem.
func TestSpec106RotuloDaMolduraCasaComAsRows(t *testing.T) {
	srv := servidorSpec106(t)

	casos := map[string][]string{
		`{"dataset":"d","sql":"SELECT id, status FROM d LIMIT 1"}`:               {"id", "status"},
		`{"dataset":"d","sql":"SELECT id, \"Acct-Status-Type\" FROM d LIMIT 1"}`: {"id", "Acct-Status-Type"},
		`{"dataset":"d","sql":"SELECT * FROM d LIMIT 1"}`:                        nil, // o '*' rotula pelo alias
	}
	for corpo, quer := range casos {
		resp := consulta(t, srv, corpo)
		rows, l := resultado(t, resp)
		if len(rows) != 1 {
			t.Fatalf("%s: rows = %d", corpo, len(rows))
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(rows[0]), &row); err != nil {
			t.Fatal(err)
		}
		for _, c := range l.Schema().Columns {
			if _, ok := row[c.Name]; !ok {
				t.Fatalf("%s: a moldura anuncia a coluna %q e a row não a tem: %s", corpo, c.Name, rows[0])
			}
		}
		if quer == nil {
			// No '*', o rótulo é o alias quando existe.
			if _, ok := row["status"]; !ok {
				t.Fatalf("o '*' não rotulou pelo alias: %s", rows[0])
			}
			continue
		}
		if len(l.Schema().Columns) != len(quer) {
			t.Fatalf("%s: colunas = %d, quer %d", corpo, len(l.Schema().Columns), len(quer))
		}
		for i, nome := range quer {
			if l.Schema().Columns[i].Name != nome {
				t.Fatalf("%s: coluna %d = %q, quer %q", corpo, i, l.Schema().Columns[i].Name, nome)
			}
		}
	}
}

func mesmosIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
