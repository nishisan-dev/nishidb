package alarm

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// TestCadenciaEhDerivadaDoMenorPrazo: amostrar mais fino que o prazo que se
// quer medir é o cálculo do watchdogInterval do Kafka, e por aqui vale pelo
// mesmo motivo — com passada igual ao prazo, um alarme de dois minutos
// demoraria até quatro para sair.
func TestCadenciaEhDerivadaDoMenorPrazo(t *testing.T) {
	cases := []struct {
		nome   string
		prazos []time.Duration
		quero  time.Duration
	}{
		{"um alarme de dois minutos", []time.Duration{2 * time.Minute}, 30 * time.Second},
		{"o menor manda", []time.Duration{time.Hour, 2 * time.Minute, 10 * time.Minute}, 30 * time.Second},
		{"o piso protege o prazo mínimo", []time.Duration{10 * time.Second}, 2500 * time.Millisecond},
		{"sem alarme não há cadência", nil, 0},
	}
	for _, c := range cases {
		t.Run(c.nome, func(t *testing.T) {
			var defs []Definition
			if len(c.prazos) > 0 {
				d := Definition{Counter: "c"}
				for _, p := range c.prazos {
					d.alarms = append(d.alarms, compiledAlarm{sustainedFor: p})
				}
				defs = append(defs, d)
			}
			if got := sweepInterval(defs); got != c.quero {
				t.Errorf("cadência = %v, quero %v", got, c.quero)
			}
		})
	}
}

// TestCadenciaNuncaFicaAbaixoDoPiso: um prazo curto não pode virar varredura
// contínua. O piso é o mesmo argumento do watchdog — window/4 com piso de um
// segundo.
func TestCadenciaNuncaFicaAbaixoDoPiso(t *testing.T) {
	d := Definition{Counter: "c", alarms: []compiledAlarm{{sustainedFor: time.Second}}}
	if got := sweepInterval([]Definition{d}); got < minSweepInterval {
		t.Fatalf("cadência = %v, abaixo do piso de %v", got, minSweepInterval)
	}
}

// TestLacoSobeEParaComOMedidorCoerente: o molde do StartRetention — medidor
// atômico observável, cancelamento por contexto, e o Stop cancela e AGUARDA.
func TestLacoSobeEParaComOMedidorCoerente(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 500, live: true})
	r, _, _ := runnerDeTeste(t, f, "")
	if r.Active() {
		t.Fatal("o medidor sobe antes do laço")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)
	if !r.Active() {
		t.Fatal("quem chamou Start tem direito de ver o estado que acabou de instalar")
	}
	// Segunda chamada não sobe uma segunda goroutine.
	r.Start(ctx)
	r.Stop()
	if r.Active() {
		t.Fatal("o Stop devolveu o controle com o medidor ainda em 1")
	}
	// Idempotente: o Close de quem nos usa chama o Stop sem perguntar se o
	// laço subiu.
	r.Stop()
}

// TestLacoNaoSobeSemAlarmeDeclarado: não se paga thread para não fazer nada.
func TestLacoNaoSobeSemAlarmeDeclarado(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	defs, err := Compile("telecom", []schema.CounterSpec{{Name: "autenticados_por_pon", GroupBy: []string{"olt", "slot"}}},
		[]counter.Definition{def})
	if err != nil {
		t.Fatal(err)
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 3, total: 50, live: true})
	r, err := New(defs, map[string]counters.Reader{"autenticados_por_pon": f}, Options{Sink: &sinkOfTest{}})
	if err != nil {
		t.Fatal(err)
	}
	r.Start(context.Background())
	defer r.Stop()
	if r.Active() {
		t.Fatal("subiu goroutine para um dataset sem alarme nenhum")
	}
}
