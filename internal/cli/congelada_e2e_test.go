package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A spec do ciclo das congeladas: uma coluna que congela o valor de outra, e
// uma que congela o relógio do writer. `origSev` tem bitmap de propósito — a
// congelada é uma coluna normal para o planner, e isso precisa ser provado.
const alarmesYAML = `
version: 1
name: alarmes
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    sev:
      type: int64
    origSev:
      frozenFrom: sev
      index: bitmap
    criadoEm:
      frozenFrom: "@writeTime"
`

// ambienteAlarmes acrescenta o dataset das congeladas ao ambiente padrão.
func ambienteAlarmes(t *testing.T) *ambiente {
	t.Helper()
	a := novoAmbiente(t)
	caminho := filepath.Join(a.dir, "datasets", "alarmes.yaml")
	if err := os.WriteFile(caminho, []byte(alarmesYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	return a
}

// consultaAlarmes ingere os eventos e devolve as linhas da consulta já
// decodificadas, pelo caminho REAL do binário.
func consultaAlarmes(t *testing.T, a *ambiente, eventos []string, sql string) []map[string]any {
	t.Helper()
	if code := a.rodaComEntrada(strings.Join(eventos, "\n")+"\n",
		"ingest", "--config", a.config, "--dataset", "alarmes", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	if code := a.roda("query", "--config", a.config, "--dataset", "alarmes", sql); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	saida := strings.TrimSuffix(a.stdout.String(), "\n")
	if saida == "" {
		return nil
	}
	var out []map[string]any
	for _, l := range strings.Split(saida, "\n") {
		m := map[string]any{}
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("linha não é JSON válido: %v\n%s", err, l)
		}
		out = append(out, m)
	}
	return out
}

func TestCongeladaSobreviveATresUpdatesDaOrigem(t *testing.T) {
	a := ambienteAlarmes(t)
	linhas := consultaAlarmes(t, a, []string{
		`{"id":"a","sev":5}`,
		`{"id":"a","sev":3}`,
		`{"id":"a","sev":1}`,
		`{"id":"a","sev":7}`,
	}, `SELECT id, sev, origSev FROM alarmes`)
	if len(linhas) != 1 {
		t.Fatalf("%d linhas, quero 1: %v", len(linhas), linhas)
	}
	if got := linhas[0]["sev"]; got != float64(7) {
		t.Errorf("sev = %v, quero 7 (o corrente)", got)
	}
	if got := linhas[0]["origSev"]; got != float64(5) {
		t.Errorf("origSev = %v, quero 5 (o de nascimento)", got)
	}
}

// A captura é do PRIMEIRO valor visto, não do primeiro evento: fonte que manda
// evento parcial não perde a coluna, ela só é preenchida mais tarde.
func TestCongeladaCapturaNoSegundoEventoQuandoOPrimeiroNaoTrouxeAOrigem(t *testing.T) {
	a := ambienteAlarmes(t)
	linhas := consultaAlarmes(t, a, []string{
		`{"id":"a"}`,
		`{"id":"a","sev":3}`,
		`{"id":"a","sev":9}`,
	}, `SELECT id, sev, origSev FROM alarmes`)
	if len(linhas) != 1 {
		t.Fatalf("%d linhas, quero 1: %v", len(linhas), linhas)
	}
	if got := linhas[0]["origSev"]; got != float64(3) {
		t.Errorf("origSev = %v, quero 3 — o primeiro valor VISTO, não o do primeiro evento", got)
	}
}

// A congelada é uma coluna normal para o planner: com bitmap declarado, o
// WHERE sobre ela devolve a entidade mesmo depois de a origem ter mudado.
func TestCongeladaComIndiceBitmapEhFiltravel(t *testing.T) {
	a := ambienteAlarmes(t)
	linhas := consultaAlarmes(t, a, []string{
		`{"id":"a","sev":5}`,
		`{"id":"b","sev":8}`,
		`{"id":"a","sev":1}`,
		`{"id":"b","sev":2}`,
	}, `SELECT id, origSev FROM alarmes WHERE origSev = 5`)
	if len(linhas) != 1 {
		t.Fatalf("%d linhas, quero 1: %v", len(linhas), linhas)
	}
	if got := linhas[0]["id"]; got != "a" {
		t.Errorf("id = %v, quero a", got)
	}
}

// O valor congelado é um campo do record como qualquer outro, então o replay
// do tail o repõe sem código novo. Isso precisa ser PROVADO, não assumido: a
// alternativa — um valor que só vive num índice derivado — voltaria como o
// valor CORRENTE depois de um crash, e ninguém veria.
func TestCongeladaSobreviveAoReplayDoTail(t *testing.T) {
	a := ambienteAlarmes(t)
	spec, err := schema.Load(filepath.Join(a.dir, "datasets", "alarmes.yaml"), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	opts := storage.Options{
		Dir:              filepath.Join(a.dir, "dados", "alarmes"),
		BitmapFieldNames: schema.CamposComBitmap(spec),
		HashFieldNames:   schema.CamposComHash(spec),
	}
	st, err := storage.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(st.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	mat := materializer.New(c, st)
	for _, ev := range []string{`{"id":"a","sev":5}`, `{"id":"a","sev":9}`} {
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			t.Fatalf("ingestão de %s: %v", ev, r.Err)
		}
	}
	// Close SEM checkpoint é o equivalente a crash para o estado em heap: o
	// overlay morre e o estado só sobrevive pelos records do tail.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := storage.Open(opts)
	if err != nil {
		t.Fatalf("reabrir com recovery: %v", err)
	}
	defer func() { _ = st2.Close() }()
	c2, err := spec.Compile(st2.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	rec, _, ok, err := st2.GetByKey([]byte("a"), nil)
	if err != nil || !ok {
		t.Fatalf("entidade perdida no replay (ok=%v err=%v)", ok, err)
	}
	campos := decodeCampos(t, c2, rec.Payload)
	if campos["origSev"] != int64(5) {
		t.Errorf("origSev depois do replay = %v, quero 5 (o de nascimento, não o corrente)", campos["origSev"])
	}
	if campos["sev"] != int64(9) {
		t.Errorf("sev depois do replay = %v, quero 9", campos["sev"])
	}
}

// decodeCampos lê o payload materializado e devolve nome→valor dos campos
// int64, que é o que estes testes conferem. Decodifica pelo FieldReader do
// storage — o mesmo caminho da query, e não uma releitura paralela.
func decodeCampos(t *testing.T, c *schema.Compiled, payload []byte) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	var r encoding.FieldReader
	r.Reset(payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			if err := r.Err(); err != nil {
				t.Fatalf("payload ilegível: %v", err)
			}
			return out
		}
		f, existe := c.FieldByID(id)
		if !existe || v.Type != types.TypeInt64 {
			continue
		}
		out[f.Name] = v.I64
	}
}
