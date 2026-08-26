package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const acumulaYAML = `
version: 1
name: alarmes
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    matchedCounter:
      type: int64
      merge: sum
    severity:
      type: int64
`

func TestAcumulaSomaAoLongoDeVariosEventos(t *testing.T) {
	h := newHarness(t, acumulaYAML)
	h.apply(`{"id":"a","matchedCounter":3}`)
	h.apply(`{"id":"a","matchedCounter":5}`)
	h.apply(`{"id":"a","matchedCounter":2}`)
	if got := h.state("a")["matchedCounter"]; got.I64 != 10 {
		t.Errorf("matchedCounter = %d, quero 10 (3+5+2)", got.I64)
	}
}

// O evento que não menciona o campo não muda o acumulado — é a mesma regra de
// preservação de qualquer outro campo.
func TestAcumulaNaoMudaQuandoOEventoNaoTrazOCampo(t *testing.T) {
	h := newHarness(t, acumulaYAML)
	h.apply(`{"id":"a","matchedCounter":4}`)
	h.apply(`{"id":"a","severity":2}`)
	if got := h.state("a")["matchedCounter"]; got.I64 != 4 {
		t.Errorf("matchedCounter = %d, quero 4", got.I64)
	}
}

// Chave repetida DENTRO do mesmo evento continua no last-wins: a soma é sobre
// o ESTADO anterior, não sobre as ocorrências de um evento malformado.
func TestChaveDuplicadaNoMesmoEventoContinuaLastWins(t *testing.T) {
	h := newHarness(t, acumulaYAML)
	h.apply(`{"id":"a","matchedCounter":3,"matchedCounter":5}`)
	if got := h.state("a")["matchedCounter"]; got.I64 != 5 {
		t.Errorf("matchedCounter = %d, quero 5 (last-wins, não 8)", got.I64)
	}
}

// null explícito apaga a coluna, e o acumulado recomeça do zero no evento
// seguinte. É a mesma semântica de D2 para qualquer campo.
func TestAcumulaRecomecaDepoisDeNullExplicito(t *testing.T) {
	h := newHarness(t, acumulaYAML)
	h.apply(`{"id":"a","matchedCounter":7}`)
	h.apply(`{"id":"a","matchedCounter":null}`)
	if _, tem := h.state("a")["matchedCounter"]; tem {
		t.Fatal("null explícito não removeu a coluna")
	}
	h.apply(`{"id":"a","matchedCounter":2}`)
	if got := h.state("a")["matchedCounter"]; got.I64 != 2 {
		t.Errorf("matchedCounter = %d, quero 2", got.I64)
	}
}

// A entidade que expirou e volta é uma encarnação NOVA: o merge não roda sobre
// a lápide, então o contador recomeça em vez de herdar o total do que morreu.
func TestAcumulaRecomecaNaEntidadeQueRessuscita(t *testing.T) {
	h := newHarness(t, acumulaYAML)
	h.apply(`{"id":"a","matchedCounter":9}`)
	row, ok := h.st.RowIDDaKey("a")
	if !ok {
		t.Fatal("a entidade não foi materializada")
	}
	h.st.SetTombstone(row)
	h.apply(`{"id":"a","matchedCounter":4}`)
	if got := h.state("a")["matchedCounter"]; got.I64 != 4 {
		t.Errorf("matchedCounter = %d, quero 4: a encarnação nova não herda o total da anterior", got.I64)
	}
}

// O valor gravado é sempre int64: a soma não muda o tipo físico da coluna.
func TestAcumulaMantemOTipoDaColuna(t *testing.T) {
	h := newHarness(t, acumulaYAML)
	h.apply(`{"id":"a","matchedCounter":1}`)
	h.apply(`{"id":"a","matchedCounter":1}`)
	if got := h.state("a")["matchedCounter"]; got.Type != types.TypeInt64 {
		t.Errorf("tipo = %v, quero int64", got.Type)
	}
}
