package counter

import "time"

// UseClock troca o relógio do contador (o instante em que um grupo
// esvaziou). O Store propaga o dele aqui; os testes o congelam.
func (c *Counter) UseClock(now func() time.Time) { c.now = now }

// RetireEmptyGroups aposenta os grupos que estão vazios há mais que o prazo de
// visibilidade. Writer-only (o Store a chama sob o wmu). Devolve quantos
// saíram.
//
// # A regra, e por que ela tem três cláusulas
//
// `refs == 0`, nunca `total == 0`: total zero diz "ninguém aqui satisfaz a
// condição", e um grupo com 500 linhas não autenticadas tem total zero e 500
// entradas do vetor apontando para ele. Apagá-lo e reemitir o id faria o
// próximo incremento daquelas linhas cair no equipamento errado, sem sintoma
// (planning §6.5).
//
// Prazo vencido desde que o grupo esvaziou (`emptySince`): é a visibilidade
// do colapso — o equipamento que apagou por inteiro continua com total zero
// na tabela até alguém ter olhado. Zero em RetainEmptyGroups é "nunca".
//
// E a última mexida do grupo COBERTA por um checkpoint (`lastTouch <=
// coveredTouches`): a transição que esvaziou o grupo pode estar numa cauda
// rasgada, e nesse caso o estado durável ainda diz "a linha está aqui". Um
// grupo aposentado com uma entrada durável apontando para ele reapareceria
// na dobra da reabertura como órfão — e, pior, se o id fosse reemitido, como
// outro grupo. A cobertura vem do checkpoint (segSync → ApplyOverlay → msync),
// que é o que torna record E página duráveis juntos.
//
// A aposentadoria é registro durável no dicionário (fsync), e o id nunca é
// reemitido. Grupos são inventário físico, com rotatividade lenta; reciclar
// ids entra depois, com medida na mão, se entrar.
func (c *Counter) RetireEmptyGroups(now time.Time) (int, error) {
	if c.def.RetainEmptyGroups <= 0 {
		return 0, nil
	}
	limit := c.dict.Limit()
	cutoff := now.Add(-c.def.RetainEmptyGroups).UnixMilli()
	retired := 0
	for g := uint32(1); g < limit; g++ {
		if _, live := c.dict.Key(g); !live {
			continue
		}
		if int(g) >= len(c.refs) || c.refs[g] != 0 {
			continue
		}
		if c.emptySince[g] == 0 || c.emptySince[g] > cutoff {
			continue
		}
		if c.lastTouch[g] > c.coveredTouches {
			continue
		}
		if err := c.dict.Retire(g); err != nil {
			return retired, err
		}
		c.dict.mu.Lock()
		delete(c.byText, string(c.texts[g]))
		c.emptySince[g] = 0
		c.dict.mu.Unlock()
		retired++
	}
	return retired, nil
}
