package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
)

// O laço do resumo ia até CategoryStorage (7) e parava — CategoryInternal (8)
// ficava de fora. Um evento que falhasse por ErrInternalState (alcançável por
// "estado anterior ilegível", que é sintoma de dataset avariado) produzia uma
// linha de contagem com a lista de categorias VAZIA. O comentário do laço
// justifica a ordem fixa porque "a saída é lida por humano e por script"; o
// script lia uma linha sem categoria nenhuma.
//
// É teste interno de propósito: a categoria não é alcançável pela entrada do
// CLI, e o defeito está no limite do laço — que é exatamente o que ele fixa.
func TestResumoNomeiaTodasAsCategorias(t *testing.T) {
	for c := materializer.CategoryInvalidJSON; c < materializer.NumCategories; c++ {
		r := resumoIngest{lidos: 1}
		if c.BadEvent() {
			r.descartados[c], r.descartadosTotal = 1, 1
		} else {
			r.falhas[c], r.falhasTotal = 1, 1
		}

		var buf bytes.Buffer
		imprimeResumo(&buf, r, time.Second)

		if !strings.Contains(buf.String(), c.String()+"=1") {
			t.Errorf("categoria %v (=%d) não aparece no resumo:\n%s", c, c, buf.String())
		}
	}
}

// A ordem é fixa porque a saída é lida por script: mapa iterado daria ordem
// diferente a cada execução.
func TestResumoMantemAOrdemDasCategorias(t *testing.T) {
	r := resumoIngest{lidos: 3, descartadosTotal: 3}
	r.descartados[materializer.CategoryTypeConversion] = 1
	r.descartados[materializer.CategoryInvalidJSON] = 1
	r.descartados[materializer.CategoryInvalidKey] = 1

	var buf bytes.Buffer
	imprimeResumo(&buf, r, time.Second)

	saida := buf.String()
	pos := func(s string) int { return strings.Index(saida, s) }
	if !(pos("INVALID_JSON") < pos("INVALID_KEY") && pos("INVALID_KEY") < pos("TYPE_CONVERSION")) {
		t.Errorf("categorias fora da ordem de declaração:\n%s", saida)
	}
}
