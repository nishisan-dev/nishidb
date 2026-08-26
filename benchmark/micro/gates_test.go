//go:build !race

package micro

import (
	"os"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// Gates de alocação do M1, executáveis na suíte (`make test`) e não só no
// `make bench`: uma regressão de alocação por evento só apareceria na rodada
// de 8M, quando o custo de descobrir já é alto.
//
// Fora do build com -race, onde a instrumentação altera a contagem.
func TestGateAllocsApply(t *testing.T) {
	casos := []struct {
		nome   string
		build  func(t *testing.T) (*materializer.Materializer, [][]byte)
		want   materializer.Action
		teto   float64
		porque string
	}{
		{
			nome: "create",
			build: func(t *testing.T) (*materializer.Materializer, [][]byte) {
				ev := corpusCheio(t, 7, 512)
				return mustNew(t, compiledSetup(t), &voidStore{rowID: 1}), ev
			},
			want: materializer.ActionCreated,
			teto: 0,
		},
		{
			nome: "merge",
			build: func(t *testing.T) (*materializer.Materializer, [][]byte) {
				ev := corpusPatch(t, 8, 512)
				key, payload := estadoDeReferenciaT(t, corpusCheio(t, 8, 1))
				return mustNew(t, compiledSetup(t), &voidStore{
					rowID: 42, seq: 1, key: key, payload: payload, found: true,
				}), ev
			},
			want: materializer.ActionUpdated,
			teto: 0,
		},
		{
			nome: "stale",
			build: func(t *testing.T) (*materializer.Materializer, [][]byte) {
				ev := corpusPatch(t, 9, 512)
				key, payload := estadoDeReferenciaT(t, corpusCheio(t, 9, 1))
				return mustNew(t, compiledSetup(t), &voidStore{
					rowID: 42, seq: 1 << 40, key: key, payload: payload, found: true,
				}), ev
			},
			want: materializer.ActionStale,
			teto: 0,
		},
	}

	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			m, events := tc.build(t)
			// Warm-up: registra os campos do corpus no dicionário e faz os
			// buffers chegarem ao tamanho de regime.
			for _, e := range events[:64] {
				if r := m.Apply(e); r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			i := 0
			got := testing.AllocsPerRun(200, func() {
				r := m.Apply(events[i%len(events)])
				if r.Err != nil {
					t.Fatal(r.Err)
				}
				if r.Action != tc.want {
					t.Fatalf("Action = %v, want %v", r.Action, tc.want)
				}
				i++
			})
			if got > tc.teto {
				t.Errorf("Apply alocou %.1f objetos/evento, teto %.0f", got, tc.teto)
			}
		})
	}
}

// Pipeline completo contra o dublê (índices e estado retido).
//
// O design fixou ≤ 10 allocs/evento como alvo do M1; o valor MEDIDO depois do
// fix do dublê é 1. Um gate em 10 não pegaria uma regressão de ordem de
// grandeza — deixaria passar um evento alocando 9 objetos a mais em silêncio.
// Fica em 4: folga suficiente para variação de crescimento de buffer, apertado
// o bastante para acusar mudança real. Afrouxar até o alvo do design é decisão
// consciente, não conserto de teste vermelho.
func TestGateAllocsIngestFake(t *testing.T) {
	const teto = 4.0
	m, _, _ := fakeSetup(t)
	events := corpusMisto(t, 10, 1024)
	aplicaTudo(t, m, events[:256])

	i := 0
	got := testing.AllocsPerRun(300, func() {
		if r := m.Apply(events[i%len(events)]); r.Action == materializer.ActionFailed {
			t.Fatal(r.Err)
		}
		i++
	})
	if got > teto {
		t.Errorf("ingestão com dublê alocou %.1f objetos/evento, teto %.0f", got, teto)
	}
	t.Logf("allocs/evento com dublê: %.1f (teto %.0f)", got, teto)
}

// Leitura por primary key no Store REAL: o caminho que o executor SQL vai
// usar em toda query com igualdade na key. Exige NISHIDB_BENCH_DIR porque
// carrega um dataset com fallocate real.
func TestGateAllocsPointLookupReal(t *testing.T) {
	const teto = 2.0
	dir := benchDirOuSkip(t)
	m, st, _ := realSetup(t, dir, 2000)
	events := corpusCheio(t, 21, 2000)
	aplicaTudo(t, m, events)

	keys := make([][]byte, 0, len(events))
	for _, e := range events {
		keys = append(keys, keyDoEvento(t, e))
	}

	var buf []byte
	i := 0
	got := testing.AllocsPerRun(200, func() {
		rec, out, ok, err := st.GetByKey(keys[i%len(keys)], buf[:0])
		if err != nil || !ok {
			t.Fatalf("lookup falhou: ok=%v err=%v", ok, err)
		}
		buf = out
		if len(rec.Payload) == 0 {
			t.Fatal("payload vazio")
		}
		i++
	})
	if got > teto {
		t.Errorf("GetByKey alocou %.1f objetos/lookup, teto %.0f", got, teto)
	}
	t.Logf("allocs/lookup no Store real: %.1f (teto %.0f)", got, teto)
}

// benchDirOuSkip é o writableBenchDir para *testing.T.
func benchDirOuSkip(t *testing.T) string {
	t.Helper()
	root := os.Getenv(benchDirEnv)
	if root == "" {
		t.Skipf("defina %s apontando para DISCO REAL: este gate carrega um dataset "+
			"com fallocate de verdade", benchDirEnv)
	}
	dir, err := os.MkdirTemp(root, "gate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// estadoDeReferenciaT é o estadoDeReferencia para *testing.T (os gates rodam
// como teste, os benchmarks como benchmark).
func estadoDeReferenciaT(t *testing.T, events [][]byte) (key, payload []byte) {
	t.Helper()
	m, st, _ := fakeSetup(t)
	r := m.Apply(events[0])
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	rec, _, ok, err := st.GetByRowID(r.RowID, nil)
	if err != nil || !ok {
		t.Fatalf("estado de referência ausente: ok=%v err=%v", ok, err)
	}
	return append([]byte(nil), rec.Key...), append([]byte(nil), rec.Payload...)
}

// mustNew monta o materializer exigindo sucesso: o New recusa regra de TTL que
// não compila, e a spec destes gates não declara nenhuma.
func mustNew(tb testing.TB, c *schema.Compiled, st materializer.Store) *materializer.Materializer {
	tb.Helper()
	m, err := materializer.New(c, st)
	if err != nil {
		tb.Fatal(err)
	}
	return m
}
