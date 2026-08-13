//go:build !race

package materializer

import "testing"

// Gate executável na suíte (não só no `make bench`): o Apply não pode alocar
// em steady state. Sem ele, uma alocação por evento entraria calada e só
// apareceria na rodada de 8M — quando o custo de descobrir já é alto.
//
// Roda contra o nullStore porque o alvo da medição é o Apply: o dublê
// completo aloca por construção e o gate viraria uma medida do dublê.
//
// Fica fora do build com -race: o detector instrumenta o runtime e a contagem
// de alocações deixa de ser a do binário real.
func TestApplyNaoAlocaEmSteadyState(t *testing.T) {
	c := benchCompiled(t)

	casos := []struct {
		nome  string
		m     *Materializer
		event []byte
		want  Action
	}{
		{"create", newNullMaterializer(t, c, false, 0), benchRadiusEvent, ActionCreated},
		{"merge", newNullMaterializer(t, c, true, 1), benchRadiusPatch, ActionUpdated},
		{"stale", newNullMaterializer(t, c, true, 1<<40), benchRadiusPatch, ActionStale},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			// Warm-up: registra os campos desconhecidos no dicionário e faz os
			// buffers crescerem até o tamanho de regime.
			if r := tc.m.Apply(tc.event); r.Action != tc.want {
				t.Fatalf("Action = %v (%v), want %v", r.Action, r.Err, tc.want)
			}
			got := testing.AllocsPerRun(100, func() {
				if r := tc.m.Apply(tc.event); r.Action != tc.want {
					t.Fatalf("Action = %v (%v), want %v", r.Action, r.Err, tc.want)
				}
			})
			if got != 0 {
				t.Errorf("Apply alocou %.1f objetos/evento, want 0", got)
			}
		})
	}
}

// O merge também é medido isolado: o gate acima cobre o Apply inteiro, este
// falha apontando direto para o merge quando a regressão vier de lá.
func TestMergeNaoAloca(t *testing.T) {
	c := benchCompiled(t)
	state, _ := benchState(t, c)
	patch := benchPatchEntries(t, c, benchRadiusPatch)
	out := make([]byte, 0, 4096)

	got := testing.AllocsPerRun(100, func() {
		var err error
		out, err = mergeRecords(state, patch, out[:0])
		if err != nil {
			t.Fatal(err)
		}
	})
	if got != 0 {
		t.Errorf("mergeRecords alocou %.1f objetos/op, want 0", got)
	}
}
