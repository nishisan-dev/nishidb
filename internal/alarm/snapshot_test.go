package alarm

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// TestRetratoListaOsGruposEmObservacao: é o que a tabela `_meta.alarms` serve.
// Um alarme pendente e um disparado são estados distintos, e quem inspeciona
// precisa dos dois — "vai alarmar em 40 segundos" é informação.
func TestRetratoListaOsGruposEmObservacao(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, total: 10, live: true},
		grupo{olt: "OLT_A", slot: 2, total: 900, live: true},
	)
	r, _, clock := runnerDeTeste(t, f, "")
	if got := r.ActiveGroups(); len(got) != 0 {
		t.Fatalf("retrato antes da primeira passada = %+v", got)
	}
	passada(t, r)
	got := r.ActiveGroups()
	if len(got) != 1 {
		t.Fatalf("%d grupos no retrato, quero 1 (só o que satisfaz a condição)", len(got))
	}
	g := got[0]
	if g.Counter != "autenticados_por_pon" || g.Alarm != "olt_down" || g.GroupKey != "OLT_A|1" {
		t.Errorf("identificação = %+v", g)
	}
	if g.Firing {
		t.Error("o grupo ainda não cumpriu o prazo e já aparece como disparado")
	}
	if !g.Since.Equal(t0) {
		t.Errorf("since = %v, quero %v", g.Since, t0)
	}
	if g.Condition != "total < 100" || g.SustainedFor != 2*time.Minute {
		t.Errorf("regra = %q / %v", g.Condition, g.SustainedFor)
	}
	clock.advance(2 * time.Minute)
	passada(t, r)
	if got := r.ActiveGroups(); len(got) != 1 || !got[0].Firing {
		t.Fatalf("depois do prazo, o retrato = %+v", got)
	}
}

// TestRetratoEsvaziaQuandoOAlarmeResolve: a tabela mostra o AGORA, e um grupo
// que voltou ao normal não pode continuar listado.
func TestRetratoEsvaziaQuandoOAlarmeResolve(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 10, live: true})
	r, _, clock := runnerDeTeste(t, f, "")
	passada(t, r)
	clock.advance(2 * time.Minute)
	passada(t, r)
	f.setTotal("OLT_A", 1, 900)
	clock.advance(time.Minute)
	passada(t, r)
	if got := r.ActiveGroups(); len(got) != 0 {
		t.Fatalf("retrato depois da resolução = %+v", got)
	}
}

// TestRetratoTemOrdemDeterministica: a tabela é consultada por SQL, e uma
// ordem que variasse entre passadas faria o mesmo SELECT sem ORDER BY devolver
// as linhas em ordem diferente a cada consulta.
func TestRetratoTemOrdemDeterministica(t *testing.T) {
	gs := make([]grupo, 0, 8)
	for i := int64(8); i > 0; i-- {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: 10, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	r, _, _ := runnerDeTeste(t, f, "")
	passada(t, r)
	primeiro := r.ActiveGroups()
	if len(primeiro) != 8 {
		t.Fatalf("%d grupos, quero 8", len(primeiro))
	}
	for i := 1; i < len(primeiro); i++ {
		if primeiro[i-1].GroupKey >= primeiro[i].GroupKey {
			t.Fatalf("o retrato não está ordenado por grupo: %q antes de %q",
				primeiro[i-1].GroupKey, primeiro[i].GroupKey)
		}
	}
}

// TestRetratoEhLidoDeOutraGoroutine: a consulta chega pelo servidor HTTP,
// concorrente com a passada.
func TestRetratoEhLidoDeOutraGoroutine(t *testing.T) {
	gs := make([]grupo, 0, 16)
	for i := int64(0); i < 16; i++ {
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: 10, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	r, _, clock := runnerDeTeste(t, f, "")

	parar, pronto := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(pronto)
		for {
			select {
			case <-parar:
				return
			default:
				for _, g := range r.ActiveGroups() {
					_ = g.GroupKey
				}
			}
		}
	}()
	for i := 0; i < 40; i++ {
		if err := r.EvaluateOnce(); err != nil {
			t.Fatal(err)
		}
		for j := range f.groups {
			if f.groups[j].total == 10 {
				f.groups[j].total = 900
			} else {
				f.groups[j].total = 10
			}
		}
		clock.advance(time.Minute)
	}
	close(parar)
	<-pronto
}

// TestRetratoNasceComOQueVeioDoDisco: um processo que volta com alarmes em
// curso tem de responder à tabela ANTES da primeira passada. Sem isto, a
// inspeção durante a janela de subida diria que não há nada acontecendo —
// justamente quando alguém está olhando.
func TestRetratoNasceComOQueVeioDoDisco(t *testing.T) {
	path := filepath.Join(t.TempDir(), checkpoint.AlarmStateFileName)
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 10, live: true})
	r1, _, _ := runnerDeTeste(t, f, path)
	passada(t, r1)

	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", specWith(alarmeDe("total < 100")), []counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	clock := &clockOfTest{now: t0.Add(time.Hour)}
	r2, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f},
		Options{Sink: &sinkOfTest{}, StatePath: path, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	got := r2.ActiveGroups()
	if len(got) != 1 {
		t.Fatalf("%d grupos no retrato da subida, quero 1 restaurado do disco", len(got))
	}
	if !got[0].Since.Equal(t0) {
		t.Errorf("since = %v, quero o instante salvo %v", got[0].Since, t0)
	}
}
