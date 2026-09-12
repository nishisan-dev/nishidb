package series

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// abre monta um escritor no diretório temporário do teste.
func abre(t *testing.T, maxBytes int64, keep int) (*Writer, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := OpenWriter(dir, "autenticados.ndjson", maxBytes, keep)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w, dir
}

// escreve grava n linhas de tamanho previsível.
func escreve(t *testing.T, w *Writer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		// 20 bytes por linha, contando o \n que o Write acrescenta.
		if err := w.Write([]byte(fmt.Sprintf("{\"i\":%013d}", i))); err != nil {
			t.Fatal(err)
		}
	}
}

// arquivos lista os arquivos do diretório, ordenados.
func arquivos(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func conteudo(t *testing.T, dir, nome string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, nome))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestCadaLinhaSaiComUmFimDeLinha: é NDJSON — uma linha por registro, e o
// consumidor lê por linha.
func TestCadaLinhaSaiComUmFimDeLinha(t *testing.T) {
	w, dir := abre(t, 1<<20, 3)
	escreve(t, w, 3)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	got := conteudo(t, dir, "autenticados.ndjson")
	if n := strings.Count(got, "\n"); n != 3 {
		t.Fatalf("%d fins de linha para 3 registros:\n%s", n, got)
	}
	if strings.Contains(got, "\r") {
		t.Error("o arquivo trouxe CR: o fim de linha do NDJSON é LF")
	}
}

// TestRotacionaAoCruzarOTeto: o arquivo corrente nunca passa do teto
// declarado, que é o que torna o consumo de disco calculável.
func TestRotacionaAoCruzarOTeto(t *testing.T) {
	w, dir := abre(t, 100, 3)
	escreve(t, w, 10) // 200 bytes: uma rotação
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := arquivos(t, dir); len(got) != 2 {
		t.Fatalf("arquivos = %v, quero o corrente e um rotacionado", got)
	}
	for _, nome := range arquivos(t, dir) {
		if n := int64(len(conteudo(t, dir, nome))); n > 100 {
			t.Errorf("%s tem %d bytes, acima do teto de 100", nome, n)
		}
	}
}

// TestOSufixoMaiorEhOMaisVELHO: é a convenção que todo coletor de log espera —
// o arquivo sem sufixo é o corrente, e o número cresce para o passado.
func TestOSufixoMaiorEhOMaisVelho(t *testing.T) {
	w, dir := abre(t, 100, 3)
	// Cada bloco de 5 linhas (100 bytes) enche um arquivo.
	for bloco := 0; bloco < 3; bloco++ {
		for i := 0; i < 5; i++ {
			if err := w.Write([]byte(fmt.Sprintf("{\"b\":%d,\"i\":%08d}", bloco, i))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	// O corrente tem o bloco mais recente; o .1 o anterior; o .2 o primeiro.
	if got := conteudo(t, dir, "autenticados.ndjson"); !strings.Contains(got, `"b":2`) {
		t.Errorf("o arquivo corrente não tem o bloco mais recente:\n%s", got)
	}
	if got := conteudo(t, dir, "autenticados.ndjson.1"); !strings.Contains(got, `"b":1`) {
		t.Errorf(".1 devia ter o bloco anterior:\n%s", got)
	}
	if got := conteudo(t, dir, "autenticados.ndjson.2"); !strings.Contains(got, `"b":0`) {
		t.Errorf(".2 devia ter o bloco mais antigo:\n%s", got)
	}
}

// TestGuardaSoAQuantidadeDeclarada: o teto de disco é maxBytes × (keep+1), e é
// isso que o operador dimensiona. Sem o descarte, o "teto" não existe.
func TestGuardaSoAQuantidadeDeclarada(t *testing.T) {
	w, dir := abre(t, 100, 2)
	escreve(t, w, 100) // 2000 bytes: muitas rotações
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	got := arquivos(t, dir)
	if len(got) != 3 {
		t.Fatalf("arquivos = %v, quero o corrente mais keep=2", got)
	}
	var total int64
	for _, nome := range got {
		total += int64(len(conteudo(t, dir, nome)))
	}
	if teto := int64(100) * 3; total > teto {
		t.Errorf("%d bytes em disco, acima do teto de %d (maxBytes × (keep+1))", total, teto)
	}
}

// TestLinhaMaiorQueOTetoNaoTravaOEscritor: um registro que não cabe no teto
// não pode travar a emissão. Rotacionar não ajudaria — o arquivo novo também
// não o comportaria —, então ele é escrito e a rotação vem depois.
func TestLinhaMaiorQueOTetoNaoTravaOEscritor(t *testing.T) {
	w, dir := abre(t, 50, 2)
	grande := []byte("{\"x\":\"" + strings.Repeat("a", 200) + "\"}")
	if err := w.Write(grande); err != nil {
		t.Fatalf("a linha grande foi recusada: %v", err)
	}
	if err := w.Write([]byte(`{"depois":1}`)); err != nil {
		t.Fatalf("o escritor travou depois da linha grande: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	var achou bool
	for _, nome := range arquivos(t, dir) {
		if strings.Contains(conteudo(t, dir, nome), `"depois":1`) {
			achou = true
		}
	}
	if !achou {
		t.Error("a linha seguinte à grande não foi escrita em lugar nenhum")
	}
}

// TestReabrirContinuaNoArquivoCorrente: reiniciar o processo não pode apagar a
// série do período anterior.
func TestReabrirContinuaNoArquivoCorrente(t *testing.T) {
	dir := t.TempDir()
	w1, err := OpenWriter(dir, "s.ndjson", 1<<20, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w1.Write([]byte(`{"antes":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w1.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWriter(dir, "s.ndjson", 1<<20, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Write([]byte(`{"depois":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	got := conteudo(t, dir, "s.ndjson")
	if !strings.Contains(got, `"antes":1`) {
		t.Errorf("a reabertura truncou o arquivo:\n%s", got)
	}
	if !strings.Contains(got, `"depois":1`) {
		t.Errorf("a segunda escrita não entrou:\n%s", got)
	}
}

// TestReabrirRotacionaSeOCorrenteJaEstavaCheio: um processo que caiu com o
// arquivo no teto não pode continuar escrevendo nele para sempre. A rotação
// acontece na primeira escrita, pela conferência de sempre — não há um segundo
// caminho na abertura.
func TestReabrirRotacionaSeOCorrenteJaEstavaCheio(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "s.ndjson"), []byte(strings.Repeat("x", 200)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWriter(dir, "s.ndjson", 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write([]byte(`{"novo":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := conteudo(t, dir, "s.ndjson"); !strings.Contains(got, `"novo":1`) || len(got) > 100 {
		t.Errorf("o arquivo corrente = %d bytes:\n%s", len(got), got)
	}
	if got := conteudo(t, dir, "s.ndjson.1"); !strings.Contains(got, "xxx") {
		t.Error("o conteúdo cheio devia ter ido para o .1")
	}
}

// TestLimpezaApagaResiduoAcimaDoKeep: baixar o keep no YAML tem de devolver o
// disco, e um arquivo órfão de uma configuração antiga não pode ficar lá para
// sempre sem ninguém o contar no teto.
func TestLimpezaApagaResiduoAcimaDoKeep(t *testing.T) {
	dir := t.TempDir()
	for _, nome := range []string{"s.ndjson", "s.ndjson.1", "s.ndjson.2", "s.ndjson.7", "outro.ndjson.9"} {
		if err := os.WriteFile(filepath.Join(dir, nome), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := Cleanup(dir, map[string]int{"s.ndjson": 1}); err != nil {
		t.Fatal(err)
	}
	got := arquivos(t, dir)
	quero := []string{"outro.ndjson.9", "s.ndjson", "s.ndjson.1"}
	if strings.Join(got, ",") != strings.Join(quero, ",") {
		t.Fatalf("arquivos = %v, quero %v", got, quero)
	}
}

// TestLimpezaNaoTocaArquivoDeOutroContador: a limpeza recebe o que está
// declarado, e apagar o que ela não reconhece transformaria um erro de
// digitação no YAML em perda da série de outro contador.
func TestLimpezaNaoTocaArquivoDeOutroContador(t *testing.T) {
	dir := t.TempDir()
	for _, nome := range []string{"a.ndjson", "a.ndjson.1", "b.ndjson", "b.ndjson.1"} {
		if err := os.WriteFile(filepath.Join(dir, nome), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := Cleanup(dir, map[string]int{"a.ndjson": 0}); err != nil {
		t.Fatal(err)
	}
	got := arquivos(t, dir)
	quero := []string{"a.ndjson", "b.ndjson", "b.ndjson.1"}
	if strings.Join(got, ",") != strings.Join(quero, ",") {
		t.Fatalf("arquivos = %v, quero %v", got, quero)
	}
}

// TestLimpezaNuncaApagaOArquivoCorrente: ele é a série VIVA. Com keep=0 o
// operador pediu para não guardar histórico, não para descartar o presente — e
// um Cleanup que confundisse os dois apagaria a série a cada subida.
func TestLimpezaNuncaApagaOArquivoCorrente(t *testing.T) {
	dir := t.TempDir()
	for _, nome := range []string{"s.ndjson", "s.ndjson.1"} {
		if err := os.WriteFile(filepath.Join(dir, nome), []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := Cleanup(dir, map[string]int{"s.ndjson": 0}); err != nil {
		t.Fatal(err)
	}
	got := arquivos(t, dir)
	if strings.Join(got, ",") != "s.ndjson" {
		t.Fatalf("arquivos = %v, quero só o corrente", got)
	}
}

// TestLimpezaEmDiretorioAusenteNaoEhErro: o primeiro dataset a declarar emissão
// ainda não tem o diretório.
func TestLimpezaEmDiretorioAusenteNaoEhErro(t *testing.T) {
	if err := Cleanup(filepath.Join(t.TempDir(), "nao-existe"), map[string]int{"s.ndjson": 1}); err != nil {
		t.Fatal(err)
	}
}
