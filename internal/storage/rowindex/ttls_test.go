package rowindex

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func openTTLs(t *testing.T, dir string, minRows uint64) *TTLs {
	t.Helper()
	p, err := OpenTTLs(filepath.Join(dir, "prazos.idx"), minRows)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// Sem a distinção, a entidade que casa com uma regra `never` num dataset que
// TAMBÉM declara regra de fundo herdaria o prazo de fundo — e o alarme que o
// operador mandou nunca expirar sairia sozinho.
func TestPrazosDistingueImortalDeDesconhecido(t *testing.T) {
	p := openTTLs(t, t.TempDir(), 16)
	if _, known := p.Get(1); known {
		t.Error("entrada nunca gravada deveria ser desconhecida")
	}
	if err := p.Set(1, 0); err != nil {
		t.Fatal(err)
	}
	ms, known := p.Get(1)
	if !known || ms != 0 {
		t.Errorf("Get depois de Set(0) = (%d,%v), quero (0,true)", ms, known)
	}
}

func TestPrazosGuardaDuracao(t *testing.T) {
	p := openTTLs(t, t.TempDir(), 4)
	const ms = 180000
	if err := p.Set(1, ms); err != nil {
		t.Fatal(err)
	}
	got, known := p.Get(1)
	if !known || got != ms {
		t.Errorf("Get = (%d,%v), quero (%d,true)", got, known, ms)
	}
}

func TestPrazosSobreviveAoReabrir(t *testing.T) {
	dir := t.TempDir()
	p := openTTLs(t, dir, 4)
	if err := p.Set(3, 604800000); err != nil {
		t.Fatal(err)
	}
	if err := p.Set(4, 0); err != nil { // imortal
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p2 := openTTLs(t, dir, 4)
	if ms, known := p2.Get(3); !known || ms != 604800000 {
		t.Errorf("depois do reopen: (%d,%v)", ms, known)
	}
	if ms, known := p2.Get(4); !known || ms != 0 {
		t.Errorf("o imortal virou %v (%d) depois do reopen", known, ms)
	}
}

// Magia própria: abrir o arquivo errado é erro alto, não leitura de lixo com
// significado de prazo.
func TestPrazosMagicaPropriaRecusaArquivoDeEscritas(t *testing.T) {
	dir := t.TempDir()
	e := openLastWrites(t, dir, 4)
	if err := e.Set(1, 123); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenTTLs(filepath.Join(dir, "expiry.idx"), 4); err == nil {
		t.Fatal("abrir o índice de escritas como prazos foi aceito")
	}
}

// A menor duração declarável é um minuto (60000 ms), então nenhum prazo real
// colide com o sentinela.
func TestSentinelaNaoColideComDuracaoReal(t *testing.T) {
	p := openTTLs(t, t.TempDir(), 4)
	if err := p.Set(1, int64(ImmortalTTL)); err != nil {
		t.Fatal(err)
	}
	if ms, known := p.Get(1); !known || ms != 0 {
		t.Errorf("Get = (%d,%v): o valor do sentinela é lido como imortal", ms, known)
	}
	if err := p.Set(2, 60000); err != nil {
		t.Fatal(err)
	}
	if ms, _ := p.Get(2); ms != 60000 {
		t.Errorf("a menor duração declarável virou %d", ms)
	}
}

func TestPrazoNegativoEhGravadoComoImortal(t *testing.T) {
	p := openTTLs(t, t.TempDir(), 4)
	if err := p.Set(1, -5); err != nil {
		t.Fatal(err)
	}
	if ms, known := p.Get(1); !known || ms != 0 {
		t.Errorf("Get = (%d,%v), quero imortal", ms, known)
	}
}

func TestPrazosCresceParaRowIDAlem(t *testing.T) {
	p := openTTLs(t, t.TempDir(), 4)
	const high = types.RowID(5000)
	if err := p.Set(high, 180000); err != nil {
		t.Fatal(err)
	}
	if ms, known := p.Get(high); !known || ms != 180000 {
		t.Errorf("Get(%d) = (%d,%v)", high, ms, known)
	}
}
