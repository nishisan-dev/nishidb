package series

import (
	"fmt"
	"testing"
	"time"
)

// TestEstatisticasNascemZeradas: mesma doutrina das séries de alarme — zero na
// subida, para que "nada emitido" não se confunda com "ninguém emitindo".
func TestEstatisticasNascemZeradas(t *testing.T) {
	e, _, _ := emitterDeTeste(t, newFakeCounter(t, grupo{olt: "OLT_A", slot: 1, total: 10, live: true}))
	s := e.Stats()
	if s.Dataset != "servicos" || s.Counter != "autenticados_por_pon" {
		t.Errorf("identificação = %+v", s)
	}
	if s.Rows != 0 || s.Rotations != 0 || s.Errors != 0 || s.LastSuccessUnixMs != 0 {
		t.Errorf("estatísticas não nasceram zeradas: %+v", s)
	}
}

// TestEstatisticasContamLinhasERotacoes: os dois números dizem coisas
// diferentes — o primeiro é o volume da série, o segundo é quanto histórico
// está sendo descartado por intervalo.
func TestEstatisticasContamLinhasERotacoes(t *testing.T) {
	f := newFakeCounter(t,
		grupo{olt: "OLT_A", slot: 1, total: 10, live: true},
		grupo{olt: "OLT_A", slot: 2, total: 20, live: true},
	)
	dir := t.TempDir()
	clock := &clockOfTest{now: t0}
	// Teto pequeno: cada passada de duas linhas força rotação.
	e, err := NewEmitter(Options{
		Dataset: "servicos", Counter: "c", Columns: []string{"olt", "slot"},
		Reader: f, Dir: dir, File: "s.ndjson",
		Every: 30 * time.Second, MaxBytes: 200, Keep: 2, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	for i := 0; i < 4; i++ {
		if err := e.EmitOnce(); err != nil {
			t.Fatal(err)
		}
		clock.advance(30 * time.Second)
	}
	s := e.Stats()
	if s.Rows != 8 {
		t.Errorf("linhas = %d, quero 8 (duas por passada, quatro passadas)", s.Rows)
	}
	if s.Rotations == 0 {
		t.Errorf("rotações = 0, e o teto de %d bytes foi cruzado", 200)
	}
	if s.Bytes <= 0 {
		t.Errorf("bytes do arquivo corrente = %d", s.Bytes)
	}
	if s.LastSuccessUnixMs != clock.now.Add(-30*time.Second).UnixMilli() {
		t.Errorf("último sucesso = %d, quero o instante da última passada", s.LastSuccessUnixMs)
	}
}

// TestErroNaPassadaEhContado: o contador é o que distingue "a série parou
// porque nada mudou" de "a série parou porque a escrita falha".
func TestErroNaPassadaEhContado(t *testing.T) {
	e, _, _ := emitterDeTeste(t, newFakeCounter(t, grupo{olt: "OLT_A", slot: 1, total: 10, live: true}))
	// Fecha o arquivo por baixo do emissor: a escrita seguinte falha.
	if err := e.w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.EmitOnce(); err == nil {
		t.Fatal("a passada sobre um escritor fechado tinha de falhar")
	}
	if got := e.Stats().Errors; got != 1 {
		t.Errorf("erros = %d, quero 1", got)
	}
	_ = fmt.Sprint()
}
