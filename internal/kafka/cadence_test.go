package kafka

import (
	"testing"
	"time"
)

// TestPrecisaCheckpointDecideOsDoisGatilhos exercita a regra do §78 SEM
// broker no caminho.
//
// A separação é de propósito: os testes de ponta a ponta do débito 56 provam
// que o despertar CHEGA até aqui, e este prova o que a regra decide quando
// chega. Antes do despertador, o ramo do tempo só era consultado com um
// evento recém-decidido na mão, e as três formas de DESLIGAR o relógio
// (cadência zerada, intervalo não configurado, nenhum evento desde o último
// checkpoint) nunca eram atingidas por um caminho real. Agora um tique as
// atinge a cada intervalo, com a fila seca — e o custo de errar qualquer uma
// delas é um dataset parado reescrevendo o manifest para sempre.
func TestPrecisaCheckpointDecideOsDoisGatilhos(t *testing.T) {
	agora := time.Now()

	casos := []struct {
		name   string
		cad    Cadencia
		desde  int64
		ultimo time.Time
		quer   bool
	}{
		{
			// O caso do débito 56: a contagem está longe do limiar e quem
			// dispara é o relógio.
			name:   "tempo vencido com evento decidido dispara",
			cad:    Cadencia{Eventos: 1000, Intervalo: 50 * time.Millisecond},
			desde:  1,
			ultimo: agora.Add(-time.Second),
			quer:   true,
		},
		{
			// A guarda que o despertador tornou necessária: o tique acorda o
			// laço a cada intervalo, e sem evento novo não há posição para
			// tornar durável nem offset para commitar.
			name:   "tempo vencido sem evento decidido não dispara",
			cad:    Cadencia{Eventos: 1000, Intervalo: 50 * time.Millisecond},
			desde:  0,
			ultimo: agora.Add(-time.Second),
			quer:   false,
		},
		{
			// Intervalo não configurado DESLIGA o relógio, não adota padrão:
			// quem não pediu checkpoint por tempo fica só com a contagem.
			name:   "intervalo zerado desliga o ramo do tempo",
			cad:    Cadencia{Eventos: 1000},
			desde:  1,
			ultimo: agora.Add(-time.Hour),
			quer:   false,
		},
		{
			name:   "tempo ainda não vencido não dispara",
			cad:    Cadencia{Eventos: 1000, Intervalo: time.Hour},
			desde:  1,
			ultimo: agora,
			quer:   false,
		},
		{
			// "O que vier primeiro": a contagem não espera o relógio.
			name:   "contagem atingida dispara com o relógio novo em folha",
			cad:    Cadencia{Eventos: 2, Intervalo: time.Hour},
			desde:  2,
			ultimo: agora,
			quer:   true,
		},
		{
			// Eventos <= 0 desliga os DOIS gatilhos, e não só o da contagem.
			name:   "cadência desligada ignora o relógio vencido",
			cad:    Cadencia{Intervalo: time.Nanosecond},
			desde:  1000,
			ultimo: agora.Add(-time.Hour),
			quer:   false,
		},
	}

	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			if got := needsCheckpoint(c.cad, c.desde, c.ultimo); got != c.quer {
				t.Errorf("needsCheckpoint(%+v, desde=%d, ocioso há %s) = %v, quer %v",
					c.cad, c.desde, time.Since(c.ultimo).Round(time.Millisecond), got, c.quer)
			}
		})
	}
}

// TestDespertadorDesligaORelogioSemCadenciaPorTempo prova o off-switch que o
// comentário do tipo Cadencia declara obrigatório — e que era código de
// produção novo com zero teste.
//
// Não é zelo: `time.NewTicker` PANICA com intervalo não positivo
// ("non-positive interval for NewTicker"), então a guarda é a diferença entre
// devolver canal nil e derrubar o processo. A forma `Cadencia{Eventos: 1}`,
// com intervalo zero, já é construída nesta suíte.
func TestDespertadorDesligaORelogioSemCadenciaPorTempo(t *testing.T) {
	casos := []struct {
		name string
		cad  Cadencia
	}{
		{"intervalo zero", Cadencia{Eventos: 1}},
		{"intervalo negativo", Cadencia{Eventos: 1, Intervalo: -time.Second}},
		{"cadência por contagem desligada", Cadencia{Intervalo: time.Second}},
	}
	for _, caso := range casos {
		t.Run(caso.name, func(t *testing.T) {
			w := &Writer{cad: caso.cad}
			wake, para := w.despertador()
			defer para()
			if wake != nil {
				t.Errorf("despertador criou relógio para %+v: sem cadência por tempo o select passa a acordar sozinho, e o NewTicker de intervalo não positivo panica", caso.cad)
			}
		})
	}
}
