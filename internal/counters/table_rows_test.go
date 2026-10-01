package counters_test

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// TestCounterTableRowsIsTheGroupMembersEvenWithTotalDisabled é a coluna `rows`
// lida por SQL: é o refs do grupo (todas as linhas dele, satisfaçam ou não o
// where), e sai também com `total: false` — rows presente, total NULL. O grupo
// PRCTA_G1M10|1|9 tem três linhas e nenhuma autenticada: total 0, rows 3.
func TestCounterTableRowsIsTheGroupMembersEvenWithTotalDisabled(t *testing.T) {
	c := openCounter(t, defByPON("pon"))
	populaPON(t, c)
	rows := consulta(t, engineWithCounter(t, c), "SELECT groupKey, total, rows FROM _counters.pon ORDER BY groupKey ASC")
	if got := column(rows, "total"); got != "5,47,0,58" {
		t.Fatalf("total = %s, quero 5,47,0,58", got)
	}
	if got := column(rows, "rows"); got != "5,47,3,58" {
		t.Fatalf("rows = %s, quero 5,47,3,58 — o grupo sem autenticado tem 3 membros", got)
	}

	// total: false exige ao menos uma medida, e a medida precisa ser aberta
	// antes da primeira transição.
	def := defByPON("pon_sem_total")
	def.DisableTotal = true
	def.Measures = []counter.MeasureDefinition{{Name: "rate", Window: time.Minute, Unit: time.Second}}
	d := openCounter(t, def)
	if err := d.OpenMeasures(0, false, false); err != nil {
		t.Fatal(err)
	}
	d.ActivateMeasures()
	populaPON(t, d)
	rows = consulta(t, engineWithCounter(t, d), "SELECT groupKey, total, rows FROM _counters.pon_sem_total ORDER BY groupKey ASC")
	if got := column(rows, "total"); got != "<null>,<null>,<null>,<null>" {
		t.Fatalf("total com total: false = %s, quero NULL em todo grupo", got)
	}
	if got := column(rows, "rows"); got != "5,47,3,58" {
		t.Fatalf("rows com total: false = %s, quero 5,47,3,58 — rows nunca é NULL", got)
	}
}
