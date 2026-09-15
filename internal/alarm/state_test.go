package alarm

import (
	"testing"
	"time"
)

const prazo = 2 * time.Minute

var t0 = time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

// liveGroup monta a observação de um grupo presente no contador e elegível ao
// alarme — o caso de todo alarme sem `qualifyWhen`.
func liveGroup(holds bool) observation {
	return observation{present: true, conditionHolds: holds, qualified: true}
}

// disqualifiedGroup monta a observação de um grupo presente e inelegível: o
// `qualifyWhen` é falso.
func disqualifiedGroup(holds bool) observation {
	return observation{present: true, conditionHolds: holds, qualified: false}
}

// retiredGroup monta a observação de um grupo que saiu do contador.
func retiredGroup() observation { return observation{} }

// fire leva o tracker até o disparo, a partir de t0.
func fire(t *testing.T, tr *tracker) {
	t.Helper()
	tr.evaluate(liveGroup(true), prazo, t0)
	if d := tr.evaluate(liveGroup(true), prazo, t0.Add(prazo)); d.verdict != verdictFiring {
		t.Fatalf("veredito = %v, quero disparo para montar o cenário", d.verdict)
	}
}

// TestConditionOverIneligibleGroupNeverStartsCounting: Absent só vai a Pending
// com a condição valendo E o grupo elegível. A OLT que nunca teve ninguém
// satisfaz `auth_5m_avg < 1` desde sempre, e contar prazo para ela seria
// transformar o inventário vazio em alarme.
func TestConditionOverIneligibleGroupNeverStartsCounting(t *testing.T) {
	var tr tracker
	for i := 0; i < 5; i++ {
		at := t0.Add(time.Duration(i) * prazo)
		if d := tr.evaluate(disqualifiedGroup(true), prazo, at); d.verdict != verdictNone {
			t.Fatalf("passada %d emitiu %v sobre grupo inelegível", i, d.verdict)
		}
		if tr.active() {
			t.Fatalf("passada %d deixou estado para grupo inelegível", i)
		}
	}
}

// TestPendingGroupThatLosesQualificationReturnsToAbsentSilently: nunca houve
// disparo, então não há o que resolver.
func TestPendingGroupThatLosesQualificationReturnsToAbsentSilently(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	d := tr.evaluate(disqualifiedGroup(true), prazo, t0.Add(prazo))
	if d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: a sustentação não tinha se cumprido", d.verdict)
	}
	if tr.active() {
		t.Error("o grupo desqualificado antes do prazo tem de voltar a Absent")
	}
	// Requalificado no instante seguinte, o prazo recomeça: a contagem antiga
	// não é herdada.
	at := t0.Add(prazo + time.Second)
	if d := tr.evaluate(liveGroup(true), prazo, at); d.verdict != verdictNone {
		t.Fatalf("requalificado disparou na primeira observação: %v", d.verdict)
	}
	if !tr.observedSince().Equal(at) {
		t.Errorf("since = %v, quero %v: o cronômetro tinha de recomeçar na requalificação", tr.observedSince(), at)
	}
}

// TestFiringTrackerThatLosesQualificationResolvesAsDisqualified é a OLT morta há
// mais de 24h: a condição continua valendo, mas o máximo do dia caiu abaixo do
// limiar e ela deixou de ser o assunto do alarme.
func TestFiringTrackerThatLosesQualificationResolvesAsDisqualified(t *testing.T) {
	var tr tracker
	fire(t, &tr)
	d := tr.evaluate(disqualifiedGroup(true), prazo, t0.Add(prazo+time.Minute))
	if d.verdict != verdictDisqualified {
		t.Fatalf("veredito = %v, quero desqualificação", d.verdict)
	}
	if !d.since.Equal(t0) {
		t.Errorf("since = %v, quero %v", d.since, t0)
	}
	if tr.active() {
		t.Error("o estado do grupo desqualificado tem de ser descartado")
	}
}

// TestDisqualifiedAlarmFiresAgainOnlyAfterRequalifyingAndSustaining: enquanto
// inelegível não volta a disparar; requalificado, sustenta o prazo inteiro de
// novo.
func TestDisqualifiedAlarmFiresAgainOnlyAfterRequalifyingAndSustaining(t *testing.T) {
	var tr tracker
	fire(t, &tr)
	at := t0.Add(prazo + time.Minute)
	tr.evaluate(disqualifiedGroup(true), prazo, at)
	for i := 1; i <= 5; i++ {
		if d := tr.evaluate(disqualifiedGroup(true), prazo, at.Add(time.Duration(i)*prazo)); d.verdict != verdictNone {
			t.Fatalf("passada %d inelegível emitiu %v", i, d.verdict)
		}
	}
	back := at.Add(6 * prazo)
	if d := tr.evaluate(liveGroup(true), prazo, back); d.verdict != verdictNone {
		t.Fatalf("requalificado disparou sem sustentar: %v", d.verdict)
	}
	if d := tr.evaluate(liveGroup(true), prazo, back.Add(prazo)); d.verdict != verdictFiring {
		t.Fatalf("requalificado não disparou depois do prazo: %v", d.verdict)
	}
}

// TestSameSweepPrecedenceIsRetiredThenClearedThenDisqualified fixa a ordem dos
// motivos quando mais de um vale na mesma passada.
func TestSameSweepPrecedenceIsRetiredThenClearedThenDisqualified(t *testing.T) {
	cases := []struct {
		name string
		obs  observation
		want verdict
	}{
		// O grupo sumiu: nada do que a observação diga sobre a linha vale.
		{"retired over cleared and disqualified", observation{present: false, conditionHolds: false, qualified: false}, verdictRetired},
		{"retired over disqualified", observation{present: false, conditionHolds: true, qualified: false}, verdictRetired},
		// A OLT voltou E ficou inelegível: o fato relevante é que ela voltou.
		{"cleared over disqualified", observation{present: true, conditionHolds: false, qualified: false}, verdictCleared},
		{"disqualified alone", observation{present: true, conditionHolds: true, qualified: false}, verdictDisqualified},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var tr tracker
			fire(t, &tr)
			if d := tr.evaluate(c.obs, prazo, t0.Add(prazo+time.Minute)); d.verdict != c.want {
				t.Fatalf("veredito = %v, quero %v", d.verdict, c.want)
			}
		})
	}
}

// TestRestoredTimerOfIneligibleGroupIsDroppedSilently: o estado restaurado de um
// grupo que deixou de ser elegível durante a parada some sem evento — nunca
// houve disparo observado a resolver.
func TestRestoredTimerOfIneligibleGroupIsDroppedSilently(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-10 * time.Minute))
	if d := tr.evaluate(disqualifiedGroup(true), prazo, t0); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum", d.verdict)
	}
	if tr.active() {
		t.Error("o estado restaurado de grupo inelegível tem de ser descartado")
	}
}

// TestAlarmeNaoDisparaNaPrimeiraVezQueACondicaoVale: é a sustentação. Sem ela o
// alarme seria a consulta que a aplicação já sabe fazer.
func TestAlarmeNaoDisparaNaPrimeiraVezQueACondicaoVale(t *testing.T) {
	var tr tracker
	if d := tr.evaluate(liveGroup(true), prazo, t0); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: a condição acabou de passar a valer", d.verdict)
	}
}

// TestAlarmeNaoDisparaAntesDoPrazo: um segundo a menos ainda não é o prazo.
func TestAlarmeNaoDisparaAntesDoPrazo(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	if d := tr.evaluate(liveGroup(true), prazo, t0.Add(prazo-time.Second)); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: faltava um segundo", d.verdict)
	}
}

// TestAlarmeDisparaQuandoOPrazoSeCumpre: exatamente no prazo já conta.
func TestAlarmeDisparaQuandoOPrazoSeCumpre(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	d := tr.evaluate(liveGroup(true), prazo, t0.Add(prazo))
	if d.verdict != verdictFiring {
		t.Fatalf("veredito = %v, quero disparo", d.verdict)
	}
	if d.restored {
		t.Error("sustentação observada por este processo não pode sair marcada como restaurada")
	}
}

// TestAlarmeNaoRepeteODisparoEnquantoACondicaoPersiste: o pedido da issue é
// avisar uma vez, não a cada passada.
func TestAlarmeNaoRepeteODisparoEnquantoACondicaoPersiste(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	if d := tr.evaluate(liveGroup(true), prazo, t0.Add(prazo)); d.verdict != verdictFiring {
		t.Fatalf("não disparou: %v", d.verdict)
	}
	for i := 1; i <= 5; i++ {
		at := t0.Add(prazo + time.Duration(i)*time.Minute)
		if d := tr.evaluate(liveGroup(true), prazo, at); d.verdict != verdictNone {
			t.Fatalf("passada %d repetiu o disparo: %v", i, d.verdict)
		}
	}
}

// TestAlarmeResolveQuandoACondicaoDeixaDeValer: é o "voltou".
func TestAlarmeResolveQuandoACondicaoDeixaDeValer(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	tr.evaluate(liveGroup(true), prazo, t0.Add(prazo))
	if d := tr.evaluate(liveGroup(false), prazo, t0.Add(prazo+time.Minute)); d.verdict != verdictCleared {
		t.Fatalf("veredito = %v, quero resolução", d.verdict)
	}
}

// TestAlarmeVoltaAContarOPrazoDepoisDeResolver: depois do "voltou", a próxima
// queda precisa se sustentar outra vez — o prazo não fica cumprido para sempre.
func TestAlarmeVoltaAContarOPrazoDepoisDeResolver(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	tr.evaluate(liveGroup(true), prazo, t0.Add(prazo))
	tr.evaluate(liveGroup(false), prazo, t0.Add(prazo+time.Minute))
	at := t0.Add(prazo + 2*time.Minute)
	if d := tr.evaluate(liveGroup(true), prazo, at); d.verdict != verdictNone {
		t.Fatalf("a queda nova disparou na primeira observação: %v", d.verdict)
	}
	if d := tr.evaluate(liveGroup(true), prazo, at.Add(prazo)); d.verdict != verdictFiring {
		t.Fatalf("a queda nova não disparou depois do prazo: %v", d.verdict)
	}
}

// TestCondicaoQueOscilaAntesDoPrazoNaoEmiteNada: é a rajada que a sustentação
// existe para filtrar. Nenhum evento, nem disparo nem resolução — resolução sem
// disparo seria um "voltou" de algo que nunca foi anunciado.
func TestCondicaoQueOscilaAntesDoPrazoNaoEmiteNada(t *testing.T) {
	var tr tracker
	at := t0
	for i := 0; i < 10; i++ {
		if d := tr.evaluate(liveGroup(true), prazo, at); d.verdict != verdictNone {
			t.Fatalf("oscilação %d emitiu %v ao subir", i, d.verdict)
		}
		at = at.Add(time.Minute)
		if d := tr.evaluate(liveGroup(false), prazo, at); d.verdict != verdictNone {
			t.Fatalf("oscilação %d emitiu %v ao descer", i, d.verdict)
		}
		at = at.Add(time.Minute)
	}
}

// TestGrupoAposentadoResolveOAlarmeDisparado: sem esta cláusula o alarme ficaria
// ativo para sempre, em memória e em disco, para um grupo que não existe mais. O
// motivo é distinto do "a condição passou": o assunto desapareceu.
func TestGrupoAposentadoResolveOAlarmeDisparado(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	tr.evaluate(liveGroup(true), prazo, t0.Add(prazo))
	if d := tr.evaluate(retiredGroup(), prazo, t0.Add(prazo+time.Minute)); d.verdict != verdictRetired {
		t.Fatalf("veredito = %v, quero resolução por aposentadoria", d.verdict)
	}
	if tr.active() {
		t.Error("o estado do grupo aposentado tem de ser descartado")
	}
}

// TestGrupoAposentadoAntesDoDisparoNaoEmiteNada: nunca houve disparo a resolver.
func TestGrupoAposentadoAntesDoDisparoNaoEmiteNada(t *testing.T) {
	var tr tracker
	tr.evaluate(liveGroup(true), prazo, t0)
	if d := tr.evaluate(retiredGroup(), prazo, t0.Add(time.Minute)); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum", d.verdict)
	}
}

// TestEstadoRestauradoComPrazoVencidoDisparaNaPrimeiraPassada é a decisão de
// persistir o "desde quando": o processo caiu com a condição valendo há tempo
// suficiente, e ao voltar o alarme sai de imediato em vez de recomeçar a contar.
func TestEstadoRestauradoComPrazoVencidoDisparaNaPrimeiraPassada(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-10 * time.Minute))
	d := tr.evaluate(liveGroup(true), prazo, t0)
	if d.verdict != verdictFiring {
		t.Fatalf("veredito = %v, quero disparo imediato", d.verdict)
	}
	if !d.restored {
		t.Error("o disparo tem de sair marcado: a sustentação não foi observada por este processo")
	}
}

// TestAMarcaDeRestauradoDescreveUmEventoSoh: ela acompanha o disparo que a
// consumiu e não reaparece nos eventos seguintes do mesmo alarme. Guardada no
// estado, uma resolução observada de ponta a ponta por este processo sairia
// dizendo que veio de antes do reinício.
func TestAMarcaDeRestauradoDescreveUmEventoSoh(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-10 * time.Minute))
	if d := tr.evaluate(liveGroup(true), prazo, t0); !d.restored {
		t.Fatal("o disparo tinha de vir marcado")
	}
	d := tr.evaluate(liveGroup(false), prazo, t0.Add(time.Minute))
	if d.verdict != verdictCleared {
		t.Fatalf("veredito = %v, quero resolução", d.verdict)
	}
	if d.restored {
		t.Error("a resolução saiu marcada: a marca sobreviveu ao evento que a consumiu")
	}
}

// TestEstadoRestauradoAindaDentroDoPrazoEsperaOResto: o prazo é contado do
// instante salvo, não do instante da subida.
func TestEstadoRestauradoAindaDentroDoPrazoEsperaOResto(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-time.Minute))
	if d := tr.evaluate(liveGroup(true), prazo, t0); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: falta um minuto do prazo", d.verdict)
	}
	if d := tr.evaluate(liveGroup(true), prazo, t0.Add(time.Minute)); d.verdict != verdictFiring {
		t.Fatalf("veredito = %v, quero disparo ao completar o prazo contado do instante salvo", d.verdict)
	}
}

// TestEstadoRestauradoComCondicaoQueNaoValeMaisEhDescartadoEmSilencio: nunca
// houve disparo, então não há o que resolver — e anunciar uma resolução aqui
// inventaria um par disparo/resolução que o consumidor nunca viu abrir.
func TestEstadoRestauradoComCondicaoQueNaoValeMaisEhDescartadoEmSilencio(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-10 * time.Minute))
	if d := tr.evaluate(liveGroup(false), prazo, t0); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum", d.verdict)
	}
	if tr.active() {
		t.Error("o estado restaurado tem de ser descartado quando a condição não vale mais")
	}
}

// TestEstadoRestauradoGuardaOInstanteLido: é o que vai de volta ao arquivo
// durável quando o processo cair outra vez, e o que o evento reporta como
// "desde quando".
func TestEstadoRestauradoGuardaOInstanteLido(t *testing.T) {
	var tr tracker
	salvo := t0.Add(-10 * time.Minute)
	tr.restore(salvo)
	if got := tr.observedSince(); !got.Equal(salvo) {
		t.Fatalf("observedSince = %v, quero %v", got, salvo)
	}
}
