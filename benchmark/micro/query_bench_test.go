package micro

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// datasetDeLeitura carrega um dataset pequeno no Store REAL e devolve o store,
// o schema e as keys ingeridas.
//
// Os três benchmarks abaixo medem os caminhos que o executor SQL vai usar por
// baixo — point lookup, bitmap com limite e scan. Medi-los ANTES do executor
// existir separa o custo do storage do custo do plano: quando a query ficar
// lenta, o baseline diz de qual lado olhar.
func datasetDeLeitura(b *testing.B, entidades int) (*storage.Store, *schema.Compiled, [][]byte) {
	b.Helper()
	m, st, c := realSetup(b, readOnlyDir(b), uint64(entidades))
	events := corpusCheio(b, 21, entidades)
	aplicaTudo(b, m, events)

	keys := make([][]byte, 0, len(events))
	vistas := map[string]bool{}
	for _, e := range events {
		k := keyDoEvento(b, e)
		if vistas[string(k)] {
			continue
		}
		vistas[string(k)] = true
		keys = append(keys, k)
	}
	if len(keys) < entidades/10 {
		b.Fatalf("só %d keys distintas em %d eventos", len(keys), len(events))
	}
	return st, c, keys
}

// keyDoEvento extrai o campo "id" do evento. Usa encoding/json de propósito:
// é setup de benchmark, e um oráculo independente do nosso tokenizer.
func keyDoEvento(tb testing.TB, event []byte) []byte {
	tb.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(event, &m); err != nil {
		tb.Fatal(err)
	}
	raw, ok := m["id"]
	if !ok {
		tb.Fatalf("evento sem campo id: %s", event)
	}
	return bytes.Trim(raw, `"`)
}

// BenchmarkQueryPointLookup: resolução por primary key (§89, p95 < 10ms).
// Inclui a validação de colisão, que obriga a ler o record — é o custo real.
func BenchmarkQueryPointLookup(b *testing.B) {
	st, _, keys := datasetDeLeitura(b, 5000)

	var buf []byte
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		rec, out, ok, err := st.GetByKey(keys[i%len(keys)], buf[:0])
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatalf("key %q ausente", keys[i%len(keys)])
		}
		buf = out
		if len(rec.Payload) == 0 {
			b.Fatal("payload vazio")
		}
		i++
	}
}

// BenchmarkQueryBitmapLimit100: o caminho indexado com LIMIT (§90, p95 <
// 50ms) — lookup no bitmap e materialização dos 100 primeiros candidatos
// vivos, que é o que o executor fará.
func BenchmarkQueryBitmapLimit100(b *testing.B) {
	st, c, _ := datasetDeLeitura(b, 5000)
	field := fieldID(b, c, "Acct-Status-Type")
	// O valor mais frequente do corpus: o caso em que o LIMIT realmente corta.
	valor := types.Value{Type: types.TypeString, Bytes: []byte("Interim-Update")}

	if n := contaPostings(b, st, field, valor); n < 100 {
		b.Fatalf("só %d rows com o valor: o LIMIT 100 não seria exercitado", n)
	}

	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		it, err := st.LookupBitmap(field, valor)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for n < 100 && it.Next() {
			rec, out, ok, err := st.GetByRowID(it.RowID(), buf[:0])
			if err != nil {
				b.Fatal(err)
			}
			buf = out
			if ok && len(rec.Payload) > 0 {
				n++
			}
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
		if n != 100 {
			b.Fatalf("materializou %d rows, want 100", n)
		}
	}
}

func contaPostings(b *testing.B, st *storage.Store, field types.FieldID, v types.Value) int {
	b.Helper()
	it, err := st.LookupBitmap(field, v)
	if err != nil {
		b.Fatal(err)
	}
	defer it.Close()
	n := 0
	for it.Next() {
		n++
	}
	if err := it.Err(); err != nil {
		b.Fatal(err)
	}
	return n
}

// BenchmarkFullScan1k: varredura de 1000 rows vivas com materialização — o
// plano de fallback quando não há índice utilizável.
func BenchmarkFullScan1k(b *testing.B) {
	st, _, _ := datasetDeLeitura(b, 5000)

	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		it := st.ScanLive()
		n := 0
		for n < 1000 && it.Next() {
			rec, out, ok, err := st.GetByRowID(it.RowID(), buf[:0])
			if err != nil {
				b.Fatal(err)
			}
			buf = out
			if ok && len(rec.Payload) > 0 {
				n++
			}
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}
		if n != 1000 {
			b.Fatalf("varreu %d rows, want 1000", n)
		}
	}
}
