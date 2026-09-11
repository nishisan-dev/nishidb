package main

import (
	"reflect"
	"testing"
)

// TestHistogramaContagens confere os quatro totais que o relatório final
// abre: lido, com campo, sem_campo e inválido — cada Register incrementa
// exatamente um balde de desfecho, mais o total lido.
func TestHistogramaContagens(t *testing.T) {
	h := NewHistograma()
	h.Register(ExtraidoOK, 100)
	h.Register(ExtraidoOK, 200)
	h.Register(ExtraidoWithoutField, 0)
	h.Register(ExtraidoInvalido, 0)
	h.Register(ExtraidoInvalido, 0)

	if h.TotalLido() != 5 {
		t.Errorf("TotalLido = %d, quer 5", h.TotalLido())
	}
	if h.WithField() != 2 {
		t.Errorf("WithField = %d, quer 2", h.WithField())
	}
	if h.WithoutField() != 1 {
		t.Errorf("WithoutField = %d, quer 1", h.WithoutField())
	}
	if h.Invalidos() != 2 {
		t.Errorf("Invalidos = %d, quer 2", h.Invalidos())
	}
}

// TestHistogramaBuckets confere as cinco faixas do brief, incluindo as
// fronteiras exatas (256, 512, 768, 1024 caem no balde "≤", não no seguinte).
func TestHistogramaBuckets(t *testing.T) {
	h := NewHistograma()
	comprimentos := []int{1, 256, 257, 512, 513, 768, 769, 1024, 1025, 1099}
	for _, c := range comprimentos {
		h.Register(ExtraidoOK, c)
	}

	buckets := h.Buckets()
	if len(buckets) != 5 {
		t.Fatalf("len(Buckets()) = %d, quer 5", len(buckets))
	}

	querCount := map[string]int64{
		"≤256":  2, // 1, 256
		"≤512":  2, // 257, 512
		"≤768":  2, // 513, 768
		"≤1024": 2, // 769, 1024
		">1024": 2, // 1025, 1099
	}
	for _, b := range buckets {
		q, ok := querCount[b.Rotulo]
		if !ok {
			t.Fatalf("rótulo inesperado %q", b.Rotulo)
		}
		if b.Count != q {
			t.Errorf("bucket %q: contagem = %d, quer %d", b.Rotulo, b.Count, q)
		}
		delete(querCount, b.Rotulo)
	}
	if len(querCount) != 0 {
		t.Errorf("rótulos não vistos: %v", querCount)
	}
}

// TestHistogramaBucketsPercentualSobreComCampo confere que a % de cada
// bucket é sobre o total COM CAMPO, não sobre o total lido — mensagens
// sem_campo/inválidas não têm comprimento a distribuir.
func TestHistogramaBucketsPercentualSobreComCampo(t *testing.T) {
	h := NewHistograma()
	h.Register(ExtraidoOK, 100)
	h.Register(ExtraidoOK, 100)
	h.Register(ExtraidoWithoutField, 0)
	h.Register(ExtraidoInvalido, 0)

	for _, b := range h.Buckets() {
		if b.Rotulo == "≤256" {
			if b.Count != 2 {
				t.Fatalf("contagem do balde ≤256 = %d, quer 2", b.Count)
			}
			if b.Percentual != 100.0 {
				t.Errorf("percentual do balde ≤256 = %.2f, quer 100.00 (2 de 2 COM CAMPO, não 2 de 4 lidos)", b.Percentual)
			}
		}
	}
}

func TestHistogramaBucketsVazioNaoQuebra(t *testing.T) {
	h := NewHistograma()
	for _, b := range h.Buckets() {
		if b.Count != 0 || b.Percentual != 0 {
			t.Errorf("bucket %q de histograma vazio = %+v, quer zerado", b.Rotulo, b)
		}
	}
}

// TestHistogramaPercentisExatos usa um conjunto pequeno e conhecido para que
// o percentil de posto mais próximo (nearest-rank) seja verificável à mão:
// 10 valores ordenados 1..10, p50 é o 5º valor (posto = ceil(0.5*10) = 5).
func TestHistogramaPercentisExatos(t *testing.T) {
	h := NewHistograma()
	for i := 1; i <= 10; i++ {
		h.Register(ExtraidoOK, i*10) // 10, 20, ..., 100
	}

	casos := []struct {
		p    float64
		quer int
	}{
		{50, 50},  // posto 5 -> 5º valor = 50
		{95, 100}, // posto ceil(9.5) = 10 -> 10º valor = 100
		{99, 100}, // posto ceil(9.9) = 10 -> 10º valor = 100
		{100, 100},
		{1, 10}, // posto ceil(0.1) = 1 -> 1º valor = 10
	}
	for _, c := range casos {
		if got := h.Percentil(c.p); got != c.quer {
			t.Errorf("Percentil(%v) = %d, quer %d", c.p, got, c.quer)
		}
	}
	if got := h.Maximo(); got != 100 {
		t.Errorf("Maximo() = %d, quer 100", got)
	}
}

// TestHistogramaPercentisComRepeticao confere que o posto conta OCORRÊNCIAS,
// não valores distintos: cardinalidade baixa com muita repetição é
// precisamente o cenário do §92 (map[int]int reaproveitado por ser pequeno).
func TestHistogramaPercentisComRepeticao(t *testing.T) {
	h := NewHistograma()
	for i := 0; i < 90; i++ {
		h.Register(ExtraidoOK, 100)
	}
	for i := 0; i < 10; i++ {
		h.Register(ExtraidoOK, 2000)
	}
	// posto do p90 = ceil(0.9*100) = 90 -> exatamente o limite entre as duas
	// massas: a 90ª ocorrência ainda é 100.
	if got := h.Percentil(90); got != 100 {
		t.Errorf("Percentil(90) = %d, quer 100 (90ª ocorrência das 100 é a última do valor 100)", got)
	}
	if got := h.Percentil(91); got != 2000 {
		t.Errorf("Percentil(91) = %d, quer 2000 (91ª ocorrência já é do segundo valor)", got)
	}
}

// TestHistogramaPercentisComUmUnicoElemento fixa a borda mínima não-vazia:
// com uma única mensagem registrada, todo percentil — e o máximo — só pode
// ser esse único comprimento, qualquer que seja p.
func TestHistogramaPercentisComUmUnicoElemento(t *testing.T) {
	h := NewHistograma()
	h.Register(ExtraidoOK, 777)

	for _, p := range []float64{1, 50, 95, 99, 100} {
		if got := h.Percentil(p); got != 777 {
			t.Errorf("Percentil(%v) = %d, quer 777 (único elemento registrado)", p, got)
		}
	}
	if got := h.Maximo(); got != 777 {
		t.Errorf("Maximo() = %d, quer 777", got)
	}
}

// TestHistogramaPercentisComTodosOsComprimentosIdenticos fixa a outra borda
// que TestHistogramaPercentisComRepeticao não cobre: repetição não de UMA
// entre duas massas, mas de TODAS as mensagens no mesmo comprimento — o caso
// degenerado em que não há "distribuição" nenhuma para distinguir percentis.
func TestHistogramaPercentisComTodosOsComprimentosIdenticos(t *testing.T) {
	h := NewHistograma()
	for i := 0; i < 500; i++ {
		h.Register(ExtraidoOK, 300)
	}

	for _, p := range []float64{1, 50, 95, 99, 100} {
		if got := h.Percentil(p); got != 300 {
			t.Errorf("Percentil(%v) = %d, quer 300 (todas as 500 mensagens têm o mesmo comprimento)", p, got)
		}
	}
	if got := h.Maximo(); got != 300 {
		t.Errorf("Maximo() = %d, quer 300", got)
	}
}

func TestHistogramaPercentilVazioDevolveZero(t *testing.T) {
	h := NewHistograma()
	if got := h.Percentil(50); got != 0 {
		t.Errorf("Percentil(50) de histograma vazio = %d, quer 0", got)
	}
	if got := h.Maximo(); got != 0 {
		t.Errorf("Maximo() de histograma vazio = %d, quer 0", got)
	}
}

// TestHistogramaTopN confere a lista da cauda: maior primeiro, com
// REPETIÇÃO honrada quando o mesmo comprimento aparece em mais de uma
// mensagem — "os N maiores" é sobre mensagens, não sobre valores distintos.
func TestHistogramaTopN(t *testing.T) {
	h := NewHistograma()
	for _, c := range []int{10, 1099, 20, 1099, 1074, 30} {
		h.Register(ExtraidoOK, c)
	}

	got := h.TopN(4)
	quer := []int{1099, 1099, 1074, 30}
	if !reflect.DeepEqual(got, quer) {
		t.Errorf("TopN(4) = %v, quer %v", got, quer)
	}
}

func TestHistogramaTopNMaiorQueOTotalDevolveTudo(t *testing.T) {
	h := NewHistograma()
	h.Register(ExtraidoOK, 5)
	h.Register(ExtraidoOK, 10)

	got := h.TopN(10)
	quer := []int{10, 5}
	if !reflect.DeepEqual(got, quer) {
		t.Errorf("TopN(10) sobre 2 mensagens = %v, quer %v", got, quer)
	}
}

func TestHistogramaTopNVazioDevolveVazio(t *testing.T) {
	h := NewHistograma()
	if got := h.TopN(10); len(got) != 0 {
		t.Errorf("TopN(10) de histograma vazio = %v, quer vazio", got)
	}
}
