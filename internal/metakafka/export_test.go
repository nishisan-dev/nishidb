package metakafka

import "github.com/nishisan-dev/nishidb/internal/query"

// RetratosAbertosForTest expõe quantos retratos por consulta a fonte ainda
// segura — o observável de que o CloseSnapshot devolve o que o OpenSnapshot
// tomou. Um retrato que vazasse ficaria preso pela vida do processo, e nada
// no resultado da consulta o denunciaria.
func RetratosAbertosForTest(st query.Store) int {
	f, ok := st.(*source)
	if !ok {
		return -1
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.abertos)
}
