//go:build !race

package counter

import (
	"runtime"
	"testing"
)

// heapInuseAfterGC é o HeapInuse depois de duas coletas: a segunda pega o que
// a primeira só marcou, e o número fica estável o bastante para comparar
// patamares de dezenas de MB.
func heapInuseAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

// TestOverlayDrainAfterABuildReturnsTheHeap é o defeito da issue #88: depois
// de construir N linhas (N muito acima do limiar) e do checkpoint, o heap
// volta para perto do patamar anterior à construção. Com o `delete` entrada
// por entrada — o comportamento anterior —, o mapa do overlay continuava com a
// capacidade do pico, preso até o restart.
//
// Dois grupos só, para que o que cresce seja o overlay e não os vetores por
// grupo, e os payloads montados antes da primeira medida.
func TestOverlayDrainAfterABuildReturnsTheHeap(t *testing.T) {
	const rows = 600_000
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	ps := twoGroupPayloads(t)
	base := heapInuseAfterGC()
	transitionRange(t, c, 0, rows, ps)
	built := heapInuseAfterGC()
	if built < base+8<<20 {
		t.Fatalf("a construção de %d linhas cresceu o heap só %d bytes; o teste não mediu um overlay grande", rows, built-base)
	}
	if err := c.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	after := heapInuseAfterGC()
	grown, kept := built-base, int64(after)-int64(base)
	if kept > int64(grown)/4 {
		t.Fatalf("depois do checkpoint o heap ficou %d bytes acima do patamar anterior, de %d que a construção cresceu: o mapa do overlay não devolveu a memória", kept, grown)
	}
	t.Logf("heap: antes %d, construído %d (+%d), depois do checkpoint %d (%+d)", base, built, grown, after, kept)
}
