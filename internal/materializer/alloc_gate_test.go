//go:build !race

package materializer

import (
	"testing"
	"time"
)

// A segunda passada não aloca. Nada, nunca.
//
// Todos os buffers dela são reusados — os parsers por nível, a lista de pais e
// o scratch da folha —, e o gate mede o Parse, que é onde ela vive: medir o
// Apply inteiro contaria também o Put, cujo custo cresce com o tamanho do
// payload e afogaria o sinal. A ingestão do piloto roda a ~500 eventos/s,
// 24/7, então alocação por evento aqui é pressão de GC permanente.
//
// Os quatro casos cobrem as rotas distintas: sem o pai no evento, pai sem
// nenhuma chave extraída, uma extração, e duas mais uma aninhada.
func TestExtracaoNaoAloca(t *testing.T) {
	c := compileYAML(t, extractYAML)
	casos := []struct {
		nome  string
		event string
	}{
		{"sem o pai", `{"id":"A","outro":"x"}`},
		{"pai sem chave extraida", `{"id":"A","doc":{"nada":"x"}}`},
		{"uma extracao", `{"id":"A","doc":{"probeIpNode":"10.0.0.1"}}`},
		{"duas e uma aninhada", `{"id":"A","doc":{"probeIpNode":"1","tally":2,"alvo":{"nome":"z"}}}`},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			var b PatchBuffer
			ev := []byte(tc.event)
			// Aquece: a primeira passagem aloca os buffers que as seguintes
			// reusam, e é ela que o gate NÃO mede.
			if err := b.Parse(ev, c, time.Now()); err != nil {
				t.Fatalf("aquecimento: %v", err)
			}
			if n := testing.AllocsPerRun(300, func() { _ = b.Parse(ev, c, time.Time{}) }); n != 0 {
				t.Errorf("allocs/evento = %.1f, quer 0", n)
			}
		})
	}
}

// A descoberta não pode alocar por evento: ela roda no writer, a ~500 ev/s,
// 24/7.
func TestDescobertaNaoAlocaEmRegime(t *testing.T) {
	c := compileYAML(t, descobreYAML)
	var b PatchBuffer
	ev := []byte(`{"id":"A","doc":{"a":1,"b":"x","c":true,"d":{"e":1}}}`)
	for i := 0; i < 3; i++ {
		if err := b.Parse(ev, c, time.Time{}); err != nil {
			t.Fatalf("aquecimento: %v", err)
		}
	}
	if n := testing.AllocsPerRun(300, func() { _ = b.Parse(ev, c, time.Time{}) }); n != 0 {
		t.Errorf("allocs/evento = %.1f, quer 0", n)
	}
}
