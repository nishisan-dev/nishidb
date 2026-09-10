package cli

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// O ramo LIGADO do resumo da ingestão não é alcançável pela suíte de fora: o
// ambiente remoto sobe sem Kafka, todo dataset dele responde DISABLED, e o
// oráculo que existia (`começa com o State da rota`) é satisfeito pelos DOIS
// ramos por construção — trocar o Sprintf inteiro por `i.State` não mudava uma
// linha da saída conferida.
//
// É teste interno pelo mesmo motivo do TestResumoNomeiaTodasAsCategorias: a
// entrada do CLI não chega ao ramo, e o ramo é o que interessa. Um dataset
// DISABLED é o caso em que não há nada a diagnosticar; quem consome é
// justamente quem faz o operador querer os cinco contadores do §94.
//
// Os cinco valores são diferentes de propósito: com dois iguais, um contador
// impresso no lugar do outro passaria despercebido.
func TestResumoDaIngestaoTrazOsContadoresDoDatasetQueConsome(t *testing.T) {
	ligada := protocol.IngestionStats{
		Enabled: true, State: "UP",
		Events: 1, Applied: 2, Stale: 3, Discarded: 4, Failed: 5,
	}

	got := ingestionStatsSummary(ligada)
	for _, quer := range []string{"UP", "1 eventos", "2 aplicados", "3 stale", "4 descartados", "5 falhas"} {
		if !strings.Contains(got, quer) {
			t.Errorf("o resumo não traz %q: %q", quer, got)
		}
	}
	// Sem pausa declarada não pode haver motivo de pausa: o sufixo é o que o
	// operador lê como "parou por política de erro" (§101), e inventá-lo o
	// mandaria procurar uma parada que não houve.
	if strings.Contains(got, "parada:") {
		t.Errorf("o resumo anuncia parada sem motivo declarado: %q", got)
	}

	// Com motivo, ele viaja junto do estado — é o que distingue "está parada"
	// de "por que está parada".
	parada := ligada
	parada.State, parada.Paused = "DEGRADED", "erro de desserialização em 3 partições"
	if got := ingestionStatsSummary(parada); !strings.Contains(got, "; parada: "+parada.Paused) {
		t.Errorf("o resumo não traz o motivo da parada: %q", got)
	}

	// O dataset DESLIGADO não tem contador para mostrar. Publicar cinco zeros
	// para ele faria o operador procurar por que nada é aplicado num dataset
	// que ninguém pediu para consumir.
	desligada := ligada
	desligada.Enabled, desligada.State = false, "DISABLED"
	if got := ingestionStatsSummary(desligada); got != "DISABLED" {
		t.Errorf("resumo do dataset desligado = %q, queria só o estado", got)
	}
}
