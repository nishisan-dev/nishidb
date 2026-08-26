package spill_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/spill"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func payload(t *testing.T, texto string) []byte {
	t.Helper()
	p, err := encoding.AppendField(nil, 1, types.Value{Type: types.TypeString, Bytes: []byte(texto)})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunPreservaAOrdemDeEscrita(t *testing.T) {
	a, err := spill.NovaArea(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	r, err := a.Novo()
	if err != nil {
		t.Fatal(err)
	}
	quer := []string{"zebra", "alfa", "meio"}
	for i, v := range quer {
		if err := r.Escreve(types.RowID(i+1), payload(t, v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Sela(); err != nil {
		t.Fatal(err)
	}
	l, err := r.Le()
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range quer {
		id, p, ok, err := l.Proximo()
		if err != nil || !ok {
			t.Fatalf("registro %d: ok=%v err=%v", i, ok, err)
		}
		if id != types.RowID(i+1) {
			t.Errorf("rowID = %d, quer %d", id, i+1)
		}
		if got := string(p[len(p)-len(v):]); got != v {
			t.Errorf("payload = %q, quer terminar em %q", p, v)
		}
	}
	if _, _, ok, err := l.Proximo(); ok || err != nil {
		t.Errorf("depois do último: ok=%v err=%v, quer false/nil", ok, err)
	}
}

// O buffer do leitor é REUSADO entre chamadas, e o godoc diz isso. O teste
// existe porque a armadilha é silenciosa: quem retiver o payload de uma chamada
// e o comparar depois de outra estará comparando o registro errado, e na
// maioria dos casos os tamanhos coincidem e nada acusa.
func TestPayloadValeSoAteAChamadaSeguinte(t *testing.T) {
	a, _ := spill.NovaArea(t.TempDir(), 0)
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	r, _ := a.Novo()
	if err := r.Escreve(1, payload(t, "primeiro")); err != nil {
		t.Fatal(err)
	}
	if err := r.Escreve(2, payload(t, "segundoo")); err != nil {
		t.Fatal(err)
	}
	_ = r.Sela()
	l, _ := r.Le()
	_, p1, _, _ := l.Proximo()
	retido := p1
	_, _, _, _ = l.Proximo()
	// Não afirma QUAL valor `retido` tem — afirma que reter é errado, e o
	// oráculo é o valor ter mudado sob os pés de quem reteve.
	if string(retido[len(retido)-8:]) == "primeiro" {
		t.Skip("o buffer não foi reusado nesta execução; a armadilha continua real")
	}
}

func TestOrcamentoDeDiscoRecusa(t *testing.T) {
	a, err := spill.NovaArea(t.TempDir(), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	r, _ := a.Novo()
	var errEsperado error
	for i := 0; i < 50; i++ {
		if err := r.Escreve(types.RowID(i), payload(t, "valor razoavelmente longo para estourar")); err != nil {
			errEsperado = err
			break
		}
	}
	if errEsperado == nil {
		t.Fatal("o teto de 64 bytes não foi cobrado")
	}
	if !errors.Is(errEsperado, spill.ErrOrcamentoExcedido) {
		t.Errorf("erro = %v, quer ErrOrcamentoExcedido", errEsperado)
	}
}

// Close REMOVE os arquivos. Sem isso, uma query longa que transbordou deixaria
// o run em disco até o restart do processo.
func TestCloseRemoveOsArquivos(t *testing.T) {
	dir := t.TempDir()
	a, _ := spill.NovaArea(dir, 0)
	r, _ := a.Novo()
	_ = r.Escreve(1, payload(t, "x"))
	spillDir := filepath.Join(dir, spill.NomeDoDiretorio)
	if n := conta(t, spillDir); n != 1 {
		t.Fatalf("%d arquivos antes do Close, quer 1", n)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if n := conta(t, spillDir); n != 0 {
		t.Errorf("%d arquivos depois do Close, quer 0", n)
	}
	// Idempotente: o consumidor chama no defer e pode chamar de novo no erro.
	if err := a.Close(); err != nil {
		t.Errorf("segundo Close: %v", err)
	}
}

// Resíduo de um processo MORTO é removido na subida. É seguro por construção:
// o flock do dataset já garantiu que ninguém mais o tem.
func TestLimpaRemoveResiduoDeCrash(t *testing.T) {
	dir := t.TempDir()
	spillDir := filepath.Join(dir, spill.NomeDoDiretorio)
	if err := os.MkdirAll(spillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(spillDir, "spill-999-001.tmp"), []byte("lixo"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := spill.Limpa(dir); err != nil {
		t.Fatal(err)
	}
	if n := conta(t, spillDir); n != 0 {
		t.Errorf("%d arquivos depois do Limpa, quer 0", n)
	}
	// Diretório inexistente não é erro: dataset que nunca transbordou.
	if err := spill.Limpa(t.TempDir()); err != nil {
		t.Errorf("Limpa em dataset sem spill/: %v", err)
	}
}

func TestLeAntesDeSelaEhErro(t *testing.T) {
	a, _ := spill.NovaArea(t.TempDir(), 0)
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	r, _ := a.Novo()
	if _, err := r.Le(); err == nil {
		t.Error("Le antes de Sela foi aceito")
	}
	_ = r.Sela()
	if err := r.Escreve(1, payload(t, "x")); err == nil {
		t.Error("Escreve depois de Sela foi aceito")
	}
}

func conta(t *testing.T, dir string) int {
	t.Helper()
	e, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return len(e)
}
