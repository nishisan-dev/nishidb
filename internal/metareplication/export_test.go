package metareplication

import "github.com/nishisan-dev/nishidb/internal/query"

// OpenPicturesForTest expõe quantos retratos por consulta o Store ainda
// segura: o observável de que o CloseSnapshot devolve o que o OpenSnapshot
// tomou. Um retrato que vazasse ficaria preso pela vida do processo, e nada
// no resultado da consulta o denunciaria.
func OpenPicturesForTest(st query.Store) int {
	s, ok := st.(*store)
	if !ok {
		return -1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.open)
}
