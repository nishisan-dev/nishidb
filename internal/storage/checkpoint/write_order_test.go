package checkpoint

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestWriteOrdemDeDurabilidade(t *testing.T) {
	// S11-C2 (eixo C): dentro do Write do manifest, fsync do tmp ANTES do
	// rename e fsync do diretório DEPOIS — remover qualquer um falha aqui
	// (mutação C da revisão).
	var trace []string
	restore := SetWriteTraceForTest(func(step string) { trace = append(trace, step) })
	defer restore()

	m := Manifest{Epoch: 1, CurrentSegment: 1, DurableOffset: 64, HashKind: 1}
	if err := Write(filepath.Join(t.TempDir(), "manifest.bin"), &m); err != nil {
		t.Fatal(err)
	}
	want := []string{"write", "fsync", "rename", "dirsync"}
	if !slices.Equal(trace, want) {
		t.Fatalf("ordem do Write violada:\nesperado %v\nobtido   %v", want, trace)
	}
}
