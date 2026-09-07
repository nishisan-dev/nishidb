package segindex_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// goldenPath é o arquivo selado congelado que TestGoldenDoSelado compara
// byte a byte.
const goldenPath = "testdata/golden.sidx"

// TestGoldenDoSelado fixa os bytes do arquivo selado para um corpus com dois
// campos, um valor de posting única e outro com muitas, e um span de rowId
// que força postingWidth = 8 — o lado que
// TestOEscritorRebaseiaParaAMenorLarguraQueCabe não cobre (aquele fixa o lado
// que cabe em 4).
//
// O ponto do golden não é provar que o escritor está certo — isso é papel
// dos testes de comportamento. É travar os BYTES: o formato selado é lido
// direto do mmap (Tarefa 2), sem parser tolerante a desvio nenhum, então um
// campo do header movido, um offset calculado diferente ou a ordem de uma
// seção trocada têm de aparecer como FAIL nomeado aqui, e não como um Lookup
// que erra em produção sem explicar por quê.
func TestGoldenDoSelado(t *testing.T) {
	e := segindex.NewWriter()

	const base = types.RowID(100)
	const far = base + (1 << 32) // span >= 2^32 a partir de base: postingWidth = 8

	// campo 1, valor de hash 20: "muitas" postings — fora de ordem e com uma
	// duplicata, a mesma mistura de TestOEscritorAgrupaOrdenaEDeduplicaAsPostings.
	e.Add(1, 20, 300)
	e.Add(1, 20, base)
	e.Add(1, 20, 300)
	e.Add(1, 20, far)
	e.Add(1, 20, 250)
	// campo 1, valor de hash 5: posting única.
	e.Add(1, 5, 600)
	// campo 4, valor de hash 999: posting única.
	e.Add(4, 999, 120)
	// campo 4, valor de hash 50: duas postings.
	e.Add(4, 50, 700)
	e.Add(4, 50, 150)

	dir := t.TempDir()
	path := segindex.SealedPath(dir, 7)
	if err := e.WriteSealed(path, 7); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ler arquivo gerado: %v", err)
	}

	if os.Getenv("NISHIDB_REGRAVA_GOLDEN") == "1" {
		if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
			t.Fatalf("regravar %s: %v", goldenPath, err)
		}
		// FALHA de propósito mesmo tendo gravado: regravar o golden é um ato
		// deliberado, nunca um efeito colateral de rodar a suíte com a
		// variável esquecida ligada num CI.
		t.Fatalf("golden regravado em %s — rode de novo SEM NISHIDB_REGRAVA_GOLDEN=1 para confirmar", goldenPath)
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("ler %s: %v (rode com NISHIDB_REGRAVA_GOLDEN=1 para gerar)", goldenPath, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("o arquivo selado mudou de formato — obtido %d bytes, golden %d bytes:\n"+
			"obtido  %x\ngolden  %x\n"+
			"Se a mudança é intencional, regrave com NISHIDB_REGRAVA_GOLDEN=1 — e saiba "+
			"que isso invalida todo índice selado já gravado em disco.",
			len(got), len(want), got, want)
	}
}
