package kafka

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// TestRegistraMetricasPreRegistraAsSeisCategoriasDeEventoRuimEmZero prova o
// Minor 3 da revisão final da branch feature/m5-fundacao: antes da Task A5,
// nishidb_ingest_invalid_total nascia SEM label, e por isso já existia desde
// a subida, em zero. Depois de separar a série por motivo (famInvalidos), a
// família só materializa cada série no primeiro Apply que descartar um
// evento daquela categoria — mudança observável na superfície /metrics que o
// piloto em produção já expõe. Como o enum §101 de evento ruim é fechado
// (ErrorCategory.BadEvent()), pré-registrar as seis categorias em
// registraMetricas restaura a presença-em-zero, que é a prática canônica do
// Prometheus para séries cuja cardinalidade é conhecida de antemão.
func TestRegistraMetricasPreRegistraAsSeisCategoriasDeEventoRuimEmZero(t *testing.T) {
	m := registraMetricas(nil, "events")

	amostras := m.famInvalidos.Coleta()
	vistas := make(map[string]float64, len(amostras))
	for _, a := range amostras {
		var motivo string
		for _, l := range a.Labels {
			if l.Nome == metrics.LabelMotivo {
				motivo = l.Valor
			}
		}
		vistas[motivo] = a.Valor
	}

	// As seis categorias são lidas do próprio enum (BadEvent), não
	// hardcodadas aqui: o mesmo defeito que o comentário de NumCategories em
	// internal/materializer/materializer.go já nomeia (laço com limite
	// escrito à mão parando de reportar a última categoria) valeria para
	// este teste se a lista de motivos fosse copiada à mão.
	var esperadas []string
	for cat := materializer.ErrorCategory(0); cat < materializer.NumCategories; cat++ {
		if cat.BadEvent() {
			esperadas = append(esperadas, cat.String())
		}
	}
	if len(esperadas) != 6 {
		t.Fatalf("materializer.ErrorCategory.BadEvent() devolveu %d categoria(s), esperava 6 — o enum §101 mudou de tamanho, e este teste (e o comentário do Minor 3) precisam ser revistos junto", len(esperadas))
	}

	for _, motivo := range esperadas {
		valor, ok := vistas[motivo]
		if !ok {
			t.Errorf("motivo=%q não aparece em %s logo após a subida — a presença-em-zero foi perdida", motivo, metrics.MetIngestInvalidTotal)
			continue
		}
		if valor != 0 {
			t.Errorf("motivo=%q = %v, queria 0 (nenhum evento descartado ainda)", motivo, valor)
		}
	}
	if len(vistas) != len(esperadas) {
		t.Errorf("%s nasceu com %d série(s), esperava exatamente %d (nenhuma categoria a mais nem a menos)", metrics.MetIngestInvalidTotal, len(vistas), len(esperadas))
	}
}
