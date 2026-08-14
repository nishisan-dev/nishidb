package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// Patch de accounting típico: a key, a sequence e os seis contadores que o
// Interim-Update mexe. Todos os outros ~27 campos do estado seguem intocados —
// é a proporção que faz a cópia crua valer a pena.
var benchRadiusPatch = []byte(`{"id":"CMP-V0001PPCLP-013","_SEQ":{"$numberLong":"7455409734"},` +
	`"Acct-Input-Octets":66953,"Acct-Output-Octets":998232,"Acct-Session-Time":73220,` +
	`"Acct-Input-Packets":48212,"Acct-Output-Packets":881238,"Acct-Status-Type":"Interim-Update"}`)

// benchState materializa o evento RADIUS completo num payload, para servir de
// estado anterior do merge.
func benchState(tb testing.TB, c *schema.Compiled) ([]byte, []patchEntry) {
	tb.Helper()
	var full PatchBuffer
	if err := full.Parse(benchRadiusEvent, c); err != nil {
		tb.Fatal(err)
	}
	var state []byte
	var err error
	for _, e := range full.entries {
		if e.isNull {
			continue
		}
		if state, err = encoding.AppendField(state, e.field.ID, e.val); err != nil {
			tb.Fatal(err)
		}
	}
	return state, full.entries
}

func benchPatchEntries(tb testing.TB, c *schema.Compiled, event []byte) []patchEntry {
	tb.Helper()
	pb := &PatchBuffer{}
	if err := pb.Parse(event, c); err != nil {
		tb.Fatal(err)
	}
	return pb.entries
}

func benchCompiled(tb testing.TB) *schema.Compiled {
	tb.Helper()
	s, err := schema.Parse([]byte(patchYAML), schema.DefaultsPadrao)
	if err != nil {
		tb.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// Gate da Q9: 0 allocs/op no merge de um estado de ~30 campos com um patch de
// 8, com o buffer de saída reutilizado (steady state da ingestão).
func BenchmarkMergeRadiusPatch(b *testing.B) {
	c := benchCompiled(b)
	state, _ := benchState(b, c)
	patch := benchPatchEntries(b, c, benchRadiusPatch)

	out := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.SetBytes(int64(len(state)))
	for b.Loop() {
		var err error
		out, err = mergeRecords(state, patch, out[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Contraponto: o replace/create codifica o patch inteiro, sem estado anterior.
func BenchmarkEncodePatchRadiusEvent(b *testing.B) {
	c := benchCompiled(b)
	_, entries := benchState(b, c)

	out := make([]byte, 0, 4096)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		out, err = encodePatch(entries, out[:0])
		if err != nil {
			b.Fatal(err)
		}
	}
}
