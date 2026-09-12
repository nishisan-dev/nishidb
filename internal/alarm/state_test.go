package alarm

import (
	"testing"
	"time"
)

const prazo = 2 * time.Minute

var t0 = time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)

// vivo monta a observação de um grupo presente no contador.
func vivo(holds bool) observation { return observation{present: true, conditionHolds: holds} }

// aposentado monta a observação de um grupo que saiu do contador.
func aposentado() observation { return observation{} }

// TestAlarmeNaoDisparaNaPrimeiraVezQueACondicaoVale: é a sustentação. Sem ela o
// alarme seria a consulta que a aplicação já sabe fazer.
func TestAlarmeNaoDisparaNaPrimeiraVezQueACondicaoVale(t *testing.T) {
	var tr tracker
	if d := tr.evaluate(vivo(true), prazo, t0); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: a condição acabou de passar a valer", d.verdict)
	}
}

// TestAlarmeNaoDisparaAntesDoPrazo: um segundo a menos ainda não é o prazo.
func TestAlarmeNaoDisparaAntesDoPrazo(t *testing.T) {
	var tr tracker
	tr.evaluate(vivo(true), prazo, t0)
	if d := tr.evaluate(vivo(true), prazo, t0.Add(prazo-time.Second)); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: faltava um segundo", d.verdict)
	}
}

// TestAlarmeDisparaQuandoOPrazoSeCumpre: exatamente no prazo já conta.
func TestAlarmeDisparaQuandoOPrazoSeCumpre(t *testing.T) {
	var tr tracker
	tr.evaluate(vivo(true), prazo, t0)
	d := tr.evaluate(vivo(true), prazo, t0.Add(prazo))
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
	tr.evaluate(vivo(true), prazo, t0)
	if d := tr.evaluate(vivo(true), prazo, t0.Add(prazo)); d.verdict != verdictFiring {
		t.Fatalf("não disparou: %v", d.verdict)
	}
	for i := 1; i <= 5; i++ {
		at := t0.Add(prazo + time.Duration(i)*time.Minute)
		if d := tr.evaluate(vivo(true), prazo, at); d.verdict != verdictNone {
			t.Fatalf("passada %d repetiu o disparo: %v", i, d.verdict)
		}
	}
}

// TestAlarmeResolveQuandoACondicaoDeixaDeValer: é o "voltou".
func TestAlarmeResolveQuandoACondicaoDeixaDeValer(t *testing.T) {
	var tr tracker
	tr.evaluate(vivo(true), prazo, t0)
	tr.evaluate(vivo(true), prazo, t0.Add(prazo))
	if d := tr.evaluate(vivo(false), prazo, t0.Add(prazo+time.Minute)); d.verdict != verdictCleared {
		t.Fatalf("veredito = %v, quero resolução", d.verdict)
	}
}

// TestAlarmeVoltaAContarOPrazoDepoisDeResolver: depois do "voltou", a próxima
// queda precisa se sustentar outra vez — o prazo não fica cumprido para sempre.
func TestAlarmeVoltaAContarOPrazoDepoisDeResolver(t *testing.T) {
	var tr tracker
	tr.evaluate(vivo(true), prazo, t0)
	tr.evaluate(vivo(true), prazo, t0.Add(prazo))
	tr.evaluate(vivo(false), prazo, t0.Add(prazo+time.Minute))
	at := t0.Add(prazo + 2*time.Minute)
	if d := tr.evaluate(vivo(true), prazo, at); d.verdict != verdictNone {
		t.Fatalf("a queda nova disparou na primeira observação: %v", d.verdict)
	}
	if d := tr.evaluate(vivo(true), prazo, at.Add(prazo)); d.verdict != verdictFiring {
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
		if d := tr.evaluate(vivo(true), prazo, at); d.verdict != verdictNone {
			t.Fatalf("oscilação %d emitiu %v ao subir", i, d.verdict)
		}
		at = at.Add(time.Minute)
		if d := tr.evaluate(vivo(false), prazo, at); d.verdict != verdictNone {
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
	tr.evaluate(vivo(true), prazo, t0)
	tr.evaluate(vivo(true), prazo, t0.Add(prazo))
	if d := tr.evaluate(aposentado(), prazo, t0.Add(prazo+time.Minute)); d.verdict != verdictRetired {
		t.Fatalf("veredito = %v, quero resolução por aposentadoria", d.verdict)
	}
	if tr.active() {
		t.Error("o estado do grupo aposentado tem de ser descartado")
	}
}

// TestGrupoAposentadoAntesDoDisparoNaoEmiteNada: nunca houve disparo a resolver.
func TestGrupoAposentadoAntesDoDisparoNaoEmiteNada(t *testing.T) {
	var tr tracker
	tr.evaluate(vivo(true), prazo, t0)
	if d := tr.evaluate(aposentado(), prazo, t0.Add(time.Minute)); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum", d.verdict)
	}
}

// TestEstadoRestauradoComPrazoVencidoDisparaNaPrimeiraPassada é a decisão de
// persistir o "desde quando": o processo caiu com a condição valendo há tempo
// suficiente, e ao voltar o alarme sai de imediato em vez de recomeçar a contar.
func TestEstadoRestauradoComPrazoVencidoDisparaNaPrimeiraPassada(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-10 * time.Minute))
	d := tr.evaluate(vivo(true), prazo, t0)
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
	if d := tr.evaluate(vivo(true), prazo, t0); !d.restored {
		t.Fatal("o disparo tinha de vir marcado")
	}
	d := tr.evaluate(vivo(false), prazo, t0.Add(time.Minute))
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
	if d := tr.evaluate(vivo(true), prazo, t0); d.verdict != verdictNone {
		t.Fatalf("veredito = %v, quero nenhum: falta um minuto do prazo", d.verdict)
	}
	if d := tr.evaluate(vivo(true), prazo, t0.Add(time.Minute)); d.verdict != verdictFiring {
		t.Fatalf("veredito = %v, quero disparo ao completar o prazo contado do instante salvo", d.verdict)
	}
}

// TestEstadoRestauradoComCondicaoQueNaoValeMaisEhDescartadoEmSilencio: nunca
// houve disparo, então não há o que resolver — e anunciar uma resolução aqui
// inventaria um par disparo/resolução que o consumidor nunca viu abrir.
func TestEstadoRestauradoComCondicaoQueNaoValeMaisEhDescartadoEmSilencio(t *testing.T) {
	var tr tracker
	tr.restore(t0.Add(-10 * time.Minute))
	if d := tr.evaluate(vivo(false), prazo, t0); d.verdict != verdictNone {
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
