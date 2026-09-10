package cli

import "testing"

// A ordem de gravidade é o contrato do ingest com quem lê $?: quando dois
// desfechos acontecem na mesma execução, o código de saída tem de ser o do
// desfecho que exige a ação mais forte, não o do último que ocorreu.
//
// É teste interno porque dois dos desfechos (writer em modo falho, falha de
// checkpoint) não são alcançáveis pela entrada do CLI, e é justamente o
// cruzamento deles com a perda que o defeito original deixava ao acaso.
func TestDesfechoMaisGraveRespeitaAOrdemDeGravidade(t *testing.T) {
	casos := []struct {
		nome   string
		a, b   int
		quer   int
		porque string
	}{
		{"writer falho vence perda", ExitWriter, ExitDataLoss, ExitWriter,
			"o 4 exige reabrir o dataset antes de qualquer reprocessamento"},
		{"dataset vence perda", ExitDataset, ExitDataLoss, ExitDataset,
			"sem checkpoint o delta não está durável: o problema é o dataset"},
		{"writer falho vence dataset", ExitWriter, ExitDataset, ExitWriter,
			"modo falho é sticky; o dataset avariado ainda aceita reabrir"},
		{"perda vence interrupção", ExitInterrupted, ExitDataLoss, ExitDataLoss,
			"o sinal foi pedido por quem chamou; a perda é notícia"},
		{"perda vence erro de uso", ExitUsageError, ExitDataLoss, ExitDataLoss,
			"o 1 manda olhar a linha de comando, que não é o problema"},
		{"interrupção vence erro de uso", ExitUsageError, ExitInterrupted, ExitInterrupted,
			"o 1 é o vala-comum: qualquer desfecho nomeado é mais acionável"},
		{"qualquer coisa vence sucesso", ExitOK, ExitUsageError, ExitUsageError,
			"sair 0 com desfecho anômalo é a perda silenciosa de novo"},
		{"sucesso com sucesso é sucesso", ExitOK, ExitOK, ExitOK, ""},
		{"código desconhecido não vence os nomeados", 99, ExitDataLoss, ExitDataLoss,
			"código sem gravidade declarada vale o mesmo que o vala-comum"},
		{"código desconhecido vence sucesso", ExitOK, 99, 99, ""},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			// Comutativo: a ordem dos argumentos não pode mudar o resultado.
			if got := desfechoMaisGrave(c.a, c.b); got != c.quer {
				t.Errorf("desfechoMaisGrave(%d, %d) = %d, want %d — %s", c.a, c.b, got, c.quer, c.porque)
			}
			if got := desfechoMaisGrave(c.b, c.a); got != c.quer {
				t.Errorf("desfechoMaisGrave(%d, %d) = %d, want %d (não é comutativo)", c.b, c.a, got, c.quer)
			}
		})
	}
}
