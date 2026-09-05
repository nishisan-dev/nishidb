package storage

import "github.com/nishisan-dev/nishidb/internal/types"

// ContadoresConstruidosForTest expõe quantos contadores esta abertura teve de
// construir em vez de adotar — o observável que separa "adotou" de
// "reconstruiu e chegou ao mesmo resultado", como o da agenda.
func (s *Store) ContadoresConstruidosForTest() int { return s.contadoresConstruidos }

// PartialCheckpointComContadoresForTest é o PartialCheckpointForTest com os
// passos dos contadores: segSync, overlays aplicados (locations e contadores)
// e msyncs, SEM o manifest — o crash entre os passos 3 e 4 do §10.2, com o
// vetor de grupos em disco à frente do ponto S.
func (s *Store) PartialCheckpointComContadoresForTest() error {
	if err := s.PartialCheckpointForTest(); err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	for _, c := range s.contadores {
		if err := c.ApplyOverlay(); err != nil {
			return err
		}
		if err := c.Msync(); err != nil {
			return err
		}
	}
	return nil
}

// SetAuditoriaTraceForTest instala o observador chamado depois de cada linha
// que a auditoria dos contadores varre (seam de teste; nil em produção). É por
// ele que o teste escreve NO MEIO da varredura — o único jeito determinístico
// de estourar o snapshot que a auditoria segura.
func (s *Store) SetAuditoriaTraceForTest(fn func(linha types.RowID)) { s.audTrace = fn }

// SetAuditoriaAntesDaCopiaForTest instala o observador chamado entre abrir o
// snapshot e copiar os totais (seam de teste; nil em produção). No código
// certo ele roda SOB o wmu — e é isso que o teste prova: um Put disparado
// dali só entra depois da cópia.
func (s *Store) SetAuditoriaAntesDaCopiaForTest(fn func()) { s.audAntesDaCopia = fn }
