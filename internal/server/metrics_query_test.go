package server_test

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// TestSeriesDeRecusaDeQueryNascemComZero: as duas séries que contam recusa
// existem no /metrics de um servidor que nunca recusou nada.
//
// Elas são o alarme — "consultas longas sem vaga" e "linhas fora do orçamento"
// —, e uma série que só passa a existir no primeiro alarme deixa
// `absent(...)` significando duas coisas ao mesmo tempo: "nunca aconteceu" e
// "o alvo caiu". O operador escreve o alerta contra a primeira e recebe a
// segunda calada.
//
// A armadilha está em reg.Contador criar a FAMÍLIA e não a série: enquanto
// ninguém chama Sozinha(), o /metrics não publica linha nenhuma — nem sequer
// o # HELP. Por isso o teste raspa a saída de verdade em vez de perguntar ao
// registry, que responderia que a família existe.
func TestSeriesDeRecusaDeQueryNascemComZero(t *testing.T) {
	srv := servidorComDados(t, 4, blocoPadrao)
	amostras := coletaAmostras(t, srv)

	for _, serie := range []string{
		metrics.MetQueryLongRejectedTotal,
		metrics.MetQueryBudgetRejectedTotal,
	} {
		var achada *amostraExposta
		for i := range amostras {
			if amostras[i].Nome == serie {
				achada = &amostras[i]
				break
			}
		}
		if achada == nil {
			t.Errorf("%s ausente do scrape de um servidor recém-subido: "+
				"série de alarme que nasce no primeiro alarme faz absent() "+
				"confundir 'nunca recusou' com 'o alvo caiu'", serie)
			continue
		}
		if achada.Valor != 0 {
			t.Errorf("%s = %v num servidor que não recusou nada, quer 0", serie, achada.Valor)
		}
		if achada.Tipo != metrics.TipoContador {
			t.Errorf("%s tem de ser contador (só sobe), é %v", serie, achada.Tipo)
		}
		// Sem label: o sub-pool e o orçamento são do PROCESSO, não do dataset.
		if len(achada.Labels) != 0 {
			t.Errorf("%s saiu com labels %v; as duas são do processo", serie, achada.Labels)
		}
	}
}
