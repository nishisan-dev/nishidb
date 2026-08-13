//go:build race

package materializer

import "testing"

// Contraparte visível do gate de alocação sob -race: o detector instrumenta o
// runtime e a contagem deixa de refletir o binário real, então o gate roda no
// build normal (`go test ./...`) e aqui só registra o pulo — sem isto, o
// desaparecimento do gate na rodada com -race seria silencioso.
func TestApplyNaoAlocaEmSteadyState(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}

func TestMergeNaoAloca(t *testing.T) {
	t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
}
