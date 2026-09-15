//go:build !race

package counters_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestTabelaDoContadorNaoAlocaPorLinha: a fonte virtual é lida linha a linha
// pelo executor, e a consulta do alarme varre milhares de grupos por ciclo.
// Chave e texto vêm do dicionário (memória estável); o payload é montado no
// buffer do chamador.
func TestTabelaDoContadorNaoAlocaPorLinha(t *testing.T) {
	c := openCounter(t, defByPON("pon"))
	populaPON(t, c)
	_, source, err := counters.Table(c.Def(), c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	snap := source.OpenSnapshot()
	defer source.CloseSnapshot(snap)
	var buf []byte
	// Aquecimento: o buffer cresce na primeira linha.
	if _, b, ok, err := source.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = b
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%4 + 1
		rec, b, ok, err := source.GetByRowID(snap, id, buf)
		buf = b
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha = %.1f, quer 0", n)
	}
	key := []byte("PRCTA_G1M10|1|12")
	if n := testing.AllocsPerRun(1000, func() {
		_, b, ok, err := source.GetByKey(snap, key, buf)
		buf = b
		if err != nil || !ok {
			t.Fatalf("point lookup: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/point lookup = %.1f, quer 0", n)
	}
}

// TestCounterTableDoesNotAllocatePerRowWithMetrics é o gate acima com as
// métricas de janela presentes: o leitor é resolvido na montagem, o agregado
// vem por valor e as colunas entram no MESMO array de pilha — ler três colunas
// a mais por grupo não pode custar alocação. A varredura alterna entre o grupo
// sem amostra (colunas ausentes) e os com amostra, que têm larguras
// diferentes.
func TestCounterTableDoesNotAllocatePerRowWithMetrics(t *testing.T) {
	c := openCounter(t, defByPON("pon"))
	populaPON(t, c)
	_, source, err := counters.Table(c.Def(), c, exampleMetrics(), ponMetricSource(t, c))
	if err != nil {
		t.Fatal(err)
	}
	snap := source.OpenSnapshot()
	defer source.CloseSnapshot(snap)
	var buf []byte
	if _, b, ok, err := source.GetByRowID(snap, 1, buf); err != nil || !ok {
		t.Fatalf("linha 1: %v %v", ok, err)
	} else {
		buf = b
	}
	id := types.RowID(0)
	if n := testing.AllocsPerRun(1000, func() {
		id = id%4 + 1
		rec, b, ok, err := source.GetByRowID(snap, id, buf)
		buf = b
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha %d: %v %v", id, ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha com métricas = %.1f, quer 0", n)
	}
	key := []byte("PRCTA_G1M10|1|12")
	if n := testing.AllocsPerRun(1000, func() {
		_, b, ok, err := source.GetByKey(snap, key, buf)
		buf = b
		if err != nil || !ok {
			t.Fatalf("point lookup: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/point lookup com métricas = %.1f, quer 0", n)
	}
}

// TestCounterTableAtRowCeilingDoesNotAllocatePerRow: a linha no teto — as
// colunas todas que dimensionam o array de pilha do record — continua sem
// alocar. Uma coluna além do array não cabe num append silencioso: é aqui que
// um array subdimensionado que tivesse virado fatia do heap apareceria.
func TestCounterTableAtRowCeilingDoesNotAllocatePerRow(t *testing.T) {
	_, source, _ := ceilingTable(t)
	snap := source.OpenSnapshot()
	defer source.CloseSnapshot(snap)
	_, buf, ok, err := source.GetByRowID(snap, 1, nil)
	if err != nil || !ok {
		t.Fatalf("linha no teto: %v %v", ok, err)
	}
	if n := testing.AllocsPerRun(1000, func() {
		rec, b, ok, err := source.GetByRowID(snap, 1, buf)
		buf = b
		if err != nil || !ok || len(rec.Payload) == 0 {
			t.Fatalf("linha no teto: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/linha no teto = %.1f, quer 0", n)
	}
	key := []byte(ceilingGroupText)
	if n := testing.AllocsPerRun(1000, func() {
		_, b, ok, err := source.GetByKey(snap, key, buf)
		buf = b
		if err != nil || !ok {
			t.Fatalf("point lookup no teto: %v %v", ok, err)
		}
	}); n != 0 {
		t.Errorf("allocs/point lookup no teto = %.1f, quer 0", n)
	}
}
