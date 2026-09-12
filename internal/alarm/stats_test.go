package alarm

import (
	"testing"
	"time"
)

// TestEstatisticasNascemZeradas: série que pode alarmar não pode nascer no
// primeiro alarme — "nenhum disparo" tem de ser distinguível de "ninguém está
// observando", e sem o zero na subida um painel vazio é ambíguo.
func TestEstatisticasNascemZeradas(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 500, live: true})
	r, _, _ := runnerDeTeste(t, f, "")
	got := r.Stats()
	if len(got) != 1 {
		t.Fatalf("%d alarmes nas estatísticas, quero 1 (o declarado)", len(got))
	}
	s := got[0]
	if s.Dataset != "telecom" || s.Counter != "autenticados_por_pon" || s.Alarm != "olt_down" {
		t.Errorf("identificação = %+v", s)
	}
	if s.Tracked != 0 || s.Firing != 0 || s.Fired != 0 || s.Resolved != 0 ||
		s.EnrichFailed != 0 || s.EnrichSkipped != 0 {
		t.Errorf("estatísticas não nasceram zeradas: %+v", s)
	}
}

// TestEstatisticasAcompanhamOCicloDoAlarme: os números são a contagem do que
// de fato aconteceu, e não uma segunda derivação dos eventos.
func TestEstatisticasAcompanhamOCicloDoAlarme(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, total: 50, live: true},
		grupo{olt: "OLT_A", slot: 2, total: 500, live: true},
	)
	r, _, clock := runnerDeTeste(t, f, "")

	passada(t, r)
	if s := r.Stats()[0]; s.Tracked != 1 || s.Firing != 0 {
		t.Errorf("em observação = %+v; um grupo pendente, nenhum disparado", s)
	}
	clock.advance(2 * time.Minute)
	passada(t, r)
	if s := r.Stats()[0]; s.Tracked != 1 || s.Firing != 1 || s.Fired != 1 {
		t.Errorf("depois do disparo = %+v", s)
	}
	f.setTotal("OLT_A", 1, 900)
	clock.advance(time.Minute)
	passada(t, r)
	s := r.Stats()[0]
	if s.Tracked != 0 || s.Firing != 0 {
		t.Errorf("depois da resolução ainda há estado: %+v", s)
	}
	if s.Fired != 1 || s.Resolved != 1 {
		t.Errorf("acumulados = %+v; um disparo e uma resolução", s)
	}
}

// TestDuracaoDaPassadaEhMedida: é o número que diz se a varredura está
// acompanhando a cadência. Sem ele, uma passada que passou a durar mais que o
// intervalo só apareceria como série atrasada, sem causa.
func TestDuracaoDaPassadaEhMedida(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), grupo{olt: "OLT_A", slot: 1, total: 50, live: true})
	r, _, _ := runnerDeTeste(t, f, "")
	if d := r.LastSweep(); d != 0 {
		t.Errorf("duração antes da primeira passada = %v, quero zero", d)
	}
	passada(t, r)
	if d := r.LastSweep(); d <= 0 {
		t.Errorf("duração da passada = %v, quero um valor positivo", d)
	}
}

// TestEstatisticasSaoLidasDeOutraGoroutine: o scrape do /metrics lê as
// estatísticas enquanto a passada as escreve. Ler o mapa de trackers para saber
// quantos são é corrida de dados — e o detector só a acusa se as duas coisas
// acontecerem ao mesmo tempo, que é o que este teste força.
func TestEstatisticasSaoLidasDeOutraGoroutine(t *testing.T) {
	gs := make([]grupo, 0, 32)
	for i := int64(0); i < 32; i++ {
		// Metade satisfaz a condição, para que haja transição em cada passada.
		total := int64(500)
		if i%2 == 0 {
			total = 10
		}
		gs = append(gs, grupo{olt: "OLT_A", slot: i, total: total, live: true})
	}
	f := newFakeCounter(t, defByPON("autenticados_por_pon"), gs...)
	r, _, clock := runnerDeTeste(t, f, "")

	parar := make(chan struct{})
	pronto := make(chan struct{})
	go func() {
		defer close(pronto)
		for {
			select {
			case <-parar:
				return
			default:
				_ = r.Stats()
				_ = r.LastSweep()
			}
		}
	}()
	for i := 0; i < 50; i++ {
		if err := r.EvaluateOnce(); err != nil {
			t.Fatal(err)
		}
		// Alterna os totais, para que grupos entrem e saiam de observação e o
		// mapa de trackers mude de tamanho a cada passada.
		for j := range f.groups {
			if f.groups[j].total == 10 {
				f.groups[j].total = 500
			} else {
				f.groups[j].total = 10
			}
		}
		clock.advance(time.Minute)
	}
	close(parar)
	<-pronto
}

// TestContadorDeObservadosAcompanhaOMapa: o número publicado é mantido à parte
// do mapa, e duas fontes para o mesmo fato divergem no dia em que um caminho de
// transição esquece de somar. Este teste é o que as reconcilia.
func TestContadorDeObservadosAcompanhaOMapa(t *testing.T) {
	f := newFakeCounter(t, defByPON("autenticados_por_pon"),
		grupo{olt: "OLT_A", slot: 1, total: 10, live: true},
		grupo{olt: "OLT_A", slot: 2, total: 10, live: true},
		grupo{olt: "OLT_A", slot: 3, total: 900, live: true},
	)
	r, _, clock := runnerDeTeste(t, f, "")
	confere := func(etapa string) {
		t.Helper()
		mapa := 0
		for _, rc := range r.counters {
			for _, ra := range rc.alarms {
				mapa += len(ra.trackers)
			}
		}
		if got := r.Stats()[0].Tracked; got != int64(mapa) {
			t.Fatalf("%s: contador publicado = %d, mapa = %d", etapa, got, mapa)
		}
	}
	passada(t, r)
	confere("dois em observação")
	f.setTotal("OLT_A", 1, 900)
	clock.advance(time.Minute)
	passada(t, r)
	confere("um saiu")
	f.retire("OLT_A", 2)
	clock.advance(time.Minute)
	passada(t, r)
	confere("o outro foi aposentado")
	f.setTotal("OLT_A", 3, 10)
	clock.advance(time.Minute)
	passada(t, r)
	confere("um novo entrou")
}
