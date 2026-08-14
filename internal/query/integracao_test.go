package query_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A integração que fecha o M1: corpus determinístico → ingestão pelo
// Materializer → checkpoint → FECHAR e REABRIR o dataset → consultas pelas
// quatro rotas do planner → NDJSON validado linha a linha.
//
// Tudo no bloco de query foi construído contra o dublê. Cada divergência entre
// dublê e real só aparece AQUI, e o valor do teste não é "mais um caso": é
// conferir o engine contra um ORÁCULO que o próprio teste mantém — o estado
// que as regras de merge dizem que deveria existir — em vez de contra o que o
// engine devolveu na primeira execução.

// oraculo é o estado materializado esperado, mantido em memória pelas MESMAS
// regras da spec: merge por campo, null remove, sequence maior vence.
type oraculo map[string]map[string]any

// aplica reproduz o merge do materializer sobre o evento JSON.
func (o oraculo) aplica(t *testing.T, evento []byte) {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(evento, &m); err != nil {
		t.Fatalf("evento inválido: %v", err)
	}
	key, _ := m["id"].(string)
	if key == "" {
		return // sem key o materializer descarta; o oráculo também
	}
	estado, existe := o[key]
	if !existe {
		estado = map[string]any{}
		o[key] = estado
	}
	// Sequence: evento mais velho é descartado inteiro (strategy greater).
	if seqNovo, ok := numero(m["_SEQ"]); ok {
		if seqAtual, ok := numero(estado["_SEQ"]); ok && seqNovo <= seqAtual {
			return
		}
	}
	for campo, valor := range m {
		if valor == nil {
			// D2: null REMOVE o campo — não grava "nulo".
			delete(estado, campo)
			continue
		}
		estado[campo] = desembrulha(valor)
	}
}

// desembrulha resolve os wrappers Mongo Extended JSON que o gerador emite.
func desembrulha(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return v
	}
	for _, w := range []string{"$numberLong", "$numberInt", "$numberDouble", "$date"} {
		if inner, ok := m[w]; ok {
			return inner
		}
	}
	return v
}

func numero(v any) (float64, bool) {
	switch n := desembrulha(v).(type) {
	case float64:
		return n, true
	case string:
		var f float64
		if err := json.Unmarshal([]byte(n), &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

// texto devolve o valor de um campo como string, para comparar com o que a
// query devolve.
func (o oraculo) texto(key, campo string) (string, bool) {
	estado, ok := o[key]
	if !ok {
		return "", false
	}
	v, ok := estado[campo]
	if !ok {
		return "", false
	}
	s, ok := desembrulha(v).(string)
	return s, ok
}

// keysOnde devolve, em ordem, as keys cujo estado corrente satisfaz o
// predicado — o conjunto que a query TEM de devolver.
func (o oraculo) keysOnde(pred func(estado map[string]any) bool) []string {
	var out []string
	for k, estado := range o {
		if pred(estado) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

type integracao struct {
	t      *testing.T
	dir    string
	store  *storage.Store
	sch    *schema.Compiled
	engine *query.Engine
	oracle oraculo
}

// bitmaps e hashes do YAML de produção.
var (
	camposBitmap = []string{"Acct-Status-Type", "Framed-Protocol", "Acct-Terminate-Cause",
		"Filter-Id", "NAS-Port-Type", "Service-Type", "Acct-Authentic"}
	camposHash = []string{"NAS-IP-Address", "NAS-Identifier", "Framed-IP-Address"}
)

// abre (ou reabre) o dataset e recompila o schema contra o dicionário DO
// STORE — é ele que atribui os FieldIDs duráveis, e usar outro dicionário faria
// os IDs do schema não baterem com os do payload gravado.
func (ig *integracao) abre() {
	ig.t.Helper()
	st, err := storage.Open(storage.Options{
		Dir:              ig.dir,
		BitmapFieldNames: camposBitmap,
		EstimatedRows:    10_000,
	})
	if err != nil {
		ig.t.Fatal(err)
	}
	spec, err := schema.Load(fixtureYAML, schema.DefaultsPadrao)
	if err != nil {
		ig.t.Fatal(err)
	}
	sch, err := spec.Compile(st.FieldDict())
	if err != nil {
		ig.t.Fatal(err)
	}
	eng := query.NewEngine()
	if err := eng.Register(dataset, sch, st); err != nil {
		ig.t.Fatal(err)
	}
	ig.store, ig.sch, ig.engine = st, sch, eng
}

func (ig *integracao) fecha() {
	ig.t.Helper()
	if err := ig.store.Close(); err != nil {
		ig.t.Fatalf("Close: %v", err)
	}
}

func (ig *integracao) consulta(sql string, params ...types.Value) []linha {
	ig.t.Helper()
	it, err := ig.engine.Execute(context.Background(), dataset, sql, params)
	if err != nil {
		ig.t.Fatalf("query %q: %v", sql, err)
	}
	defer it.Close()

	var out []linha
	for it.Next() {
		l := make(linha, it.Row().Len())
		for _, c := range it.Row().Cols() {
			l[c.Name] = render(c)
		}
		out = append(out, l)
	}
	if err := it.Err(); err != nil {
		ig.t.Fatalf("query %q: %v", sql, err)
	}
	return out
}

// ids devolve a coluna id do resultado, ORDENADA — o oráculo é um conjunto, e
// a ordem de saída (rowId crescente) não é contrato da v1.
func (ig *integracao) ids(sql string, params ...types.Value) []string {
	ig.t.Helper()
	out := chaves(ig.consulta(sql, params...), "id")
	sort.Strings(out)
	return out
}

func TestIntegracaoFimAFim(t *testing.T) {
	ig := &integracao{t: t, dir: filepath.Join(t.TempDir(), "radius"), oracle: oraculo{}}
	ig.abre()

	// 1. Corpus determinístico com patches, Extended JSON, campos
	// desconhecidos e nulls — o workload de accounting real.
	const eventos = 4000
	// DefaultConfig, e não um Config zerado: os ratios do gerador são
	// LITERAIS por decisão da Q17 (só os campos de cardinalidade tratam 0 como
	// default), então `Config{Seed, Entities}` produziria um corpus SEM
	// patches, sem Extended JSON, sem campos desconhecidos e sem nulls — o
	// oposto do workload que este teste afirma exercitar.
	cfg := radiusgen.DefaultConfig()
	cfg.Seed, cfg.Entities = 42, 800
	corpus, err := radiusgen.GenerateCorpus(cfg, eventos)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Ingestão pelo pipeline real, com checkpoint periódico: sem ele o
	// overlay de locations vive no heap e o consumo acompanha o dataset.
	mat := materializer.New(ig.sch, ig.store)
	for i, ev := range corpus {
		r := mat.Apply(ev)
		if r.Err != nil {
			t.Fatalf("evento %d: %v (%v)\n%s", i, r.Err, r.Category, ev)
		}
		ig.oracle.aplica(t, ev)
		if (i+1)%1000 == 0 {
			if err := ig.store.Checkpoint(); err != nil {
				t.Fatalf("checkpoint no evento %d: %v", i, err)
			}
		}
	}
	if err := ig.store.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	// 3. FECHAR e REABRIR: o recovery entra no caminho, e os FieldIDs têm de
	// sobreviver — é o que prova que o dicionário durável e o schema
	// recompilado continuam falando a mesma língua.
	ig.fecha()
	ig.abre()

	if got, want := ig.store.Stats().RowCount, uint64(len(ig.oracle)); got != want {
		t.Fatalf("após reabrir: RowCount = %d, want %d entidades do oráculo", got, want)
	}
	t.Logf("%d eventos → %d entidades vivas", eventos, len(ig.oracle))

	// 4. As rotas do planner, cada uma conferida contra o oráculo.
	t.Run("point lookup", func(t *testing.T) {
		key := algumaKey(t, ig.oracle)
		got := ig.ids(`SELECT id FROM radius_sessions WHERE id = ?`,
			types.Value{Type: types.TypeString, Bytes: []byte(key)})
		if len(got) != 1 || got[0] != key {
			t.Errorf("point lookup de %q = %v", key, got)
		}
	})

	t.Run("bitmap scan", func(t *testing.T) {
		want := ig.oracle.keysOnde(func(e map[string]any) bool { return campoIgual(e, "Acct-Status-Type", "Stop") })
		got := ig.ids(`SELECT id FROM radius_sessions WHERE status = 'Stop'`)
		comparaConjuntos(t, "status='Stop'", got, want)
	})

	t.Run("intersecao de bitmaps", func(t *testing.T) {
		want := ig.oracle.keysOnde(func(e map[string]any) bool {
			return campoIgual(e, "Acct-Status-Type", "Stop") && campoIgual(e, "Framed-Protocol", "PPPoE")
		})
		got := ig.ids(`SELECT id FROM radius_sessions WHERE status = 'Stop' AND protocol = 'PPPoE'`)
		comparaConjuntos(t, "interseção", got, want)
	})

	t.Run("uniao de bitmaps", func(t *testing.T) {
		want := ig.oracle.keysOnde(func(e map[string]any) bool {
			return campoIgual(e, "Acct-Status-Type", "Stop") || campoIgual(e, "Acct-Status-Type", "Start")
		})
		got := ig.ids(`SELECT id FROM radius_sessions WHERE status = 'Stop' OR status = 'Start'`)
		comparaConjuntos(t, "união", got, want)
	})

	t.Run("full scan com campo nao indexado", func(t *testing.T) {
		// Um username que exista de verdade no corpus.
		alvo := algumValor(t, ig.oracle, "User-Name")
		want := ig.oracle.keysOnde(func(e map[string]any) bool { return campoIgual(e, "User-Name", alvo) })
		got := ig.ids(`SELECT id FROM radius_sessions WHERE username = ?`,
			types.Value{Type: types.TypeString, Bytes: []byte(alvo)})
		comparaConjuntos(t, "full scan", got, want)
		if n := ig.engine.PlanMetrics().FullScan.Load(); n == 0 {
			t.Error("query.full_scan não contou o full scan")
		}
	})

	t.Run("is null e is not null particionam", func(t *testing.T) {
		nulos := ig.ids(`SELECT id FROM radius_sessions WHERE terminate_cause IS NULL`)
		naoNulos := ig.ids(`SELECT id FROM radius_sessions WHERE terminate_cause IS NOT NULL`)
		if len(nulos)+len(naoNulos) != len(ig.oracle) {
			t.Errorf("IS NULL (%d) + IS NOT NULL (%d) = %d, want %d entidades",
				len(nulos), len(naoNulos), len(nulos)+len(naoNulos), len(ig.oracle))
		}
		want := ig.oracle.keysOnde(func(e map[string]any) bool {
			_, tem := e["Acct-Terminate-Cause"]
			return !tem
		})
		comparaConjuntos(t, "IS NULL", nulos, want)
	})

	t.Run("limit trunca sem errar", func(t *testing.T) {
		got := ig.consulta(`SELECT id FROM radius_sessions WHERE status = 'Stop' LIMIT 5`)
		if len(got) > 5 {
			t.Errorf("LIMIT 5 devolveu %d linhas", len(got))
		}
	})

	t.Run("projecao explicita e star", func(t *testing.T) {
		key := algumaKey(t, ig.oracle)
		p := types.Value{Type: types.TypeString, Bytes: []byte(key)}
		explicita := ig.consulta(`SELECT id, status FROM radius_sessions WHERE id = ?`, p)
		if len(explicita) != 1 || len(explicita[0]) != 2 {
			t.Fatalf("projeção explícita = %v", explicita)
		}
		if want, ok := ig.oracle.texto(key, "Acct-Status-Type"); ok && explicita[0]["status"] != want {
			t.Errorf("status = %q, want %q", explicita[0]["status"], want)
		}
		estrela := ig.consulta(`SELECT * FROM radius_sessions WHERE id = ?`, p)
		if len(estrela) != 1 || len(estrela[0]) <= len(explicita[0]) {
			t.Fatalf("SELECT * devolveu %d colunas", len(estrela[0]))
		}
	})

	// 5. O produto final: NDJSON válido, uma linha por row.
	t.Run("ndjson do resultado", func(t *testing.T) {
		it, err := ig.engine.Execute(context.Background(), dataset,
			`SELECT * FROM radius_sessions WHERE status = 'Stop' LIMIT 50`, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		var buf bytes.Buffer
		w := executor.NewStreamWriter(&buf, 512) // pequeno de propósito: força flushes
		linhas := 0
		for it.Next() {
			if err := w.WriteRow(it.Row()); err != nil {
				t.Fatal(err)
			}
			linhas++
		}
		if err := it.Err(); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
		if linhas == 0 {
			t.Fatal("nenhuma linha para serializar")
		}

		saida := buf.Bytes()
		if n := bytes.Count(saida, []byte{'\n'}); n != linhas {
			t.Fatalf("%d quebras de linha para %d rows", n, linhas)
		}
		for i, l := range bytes.Split(bytes.TrimSuffix(saida, []byte{'\n'}), []byte{'\n'}) {
			var m map[string]json.RawMessage
			if err := json.Unmarshal(l, &m); err != nil {
				t.Fatalf("linha %d não é JSON válido: %v\n%s", i, err, l)
			}
			if len(m) == 0 {
				t.Errorf("linha %d sem colunas", i)
			}
		}
		t.Logf("%d linhas NDJSON, %d bytes, todas válidas", linhas, len(saida))
	})
}

func algumaKey(t *testing.T, o oraculo) string {
	t.Helper()
	keys := o.keysOnde(func(map[string]any) bool { return true })
	if len(keys) == 0 {
		t.Fatal("oráculo vazio")
	}
	return keys[len(keys)/2]
}

func algumValor(t *testing.T, o oraculo, campo string) string {
	t.Helper()
	keys := o.keysOnde(func(e map[string]any) bool { _, tem := e[campo]; return tem })
	if len(keys) == 0 {
		t.Fatalf("nenhuma entidade tem %q", campo)
	}
	v, _ := o.texto(keys[len(keys)/2], campo)
	return v
}

func campoIgual(estado map[string]any, campo, valor string) bool {
	s, ok := desembrulha(estado[campo]).(string)
	return ok && s == valor
}

func comparaConjuntos(t *testing.T, nome string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: %d linhas, oráculo diz %d", nome, len(got), len(want))
	}
	falta := diferenca(want, got)
	sobra := diferenca(got, want)
	if len(falta) > 0 {
		t.Errorf("%s: FALTAM no resultado (falso negativo — o pior modo de falha): %v",
			nome, primeiros(falta))
	}
	if len(sobra) > 0 {
		t.Errorf("%s: SOBRAM no resultado: %v", nome, primeiros(sobra))
	}
}

func diferenca(a, b []string) []string {
	m := make(map[string]bool, len(b))
	for _, s := range b {
		m[s] = true
	}
	var out []string
	for _, s := range a {
		if !m[s] {
			out = append(out, s)
		}
	}
	return out
}

func primeiros(s []string) []string {
	if len(s) > 5 {
		return append(s[:5:5], "…")
	}
	return s
}
