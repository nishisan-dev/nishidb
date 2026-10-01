package storage

import "github.com/nishisan-dev/nishidb/internal/types"

// CountersBuiltForTest expõe quantos contadores esta abertura teve de
// construir em vez de adotar — o observável que separa "adotou" de
// "reconstruiu e chegou ao mesmo resultado", como o da agenda.
func (s *Store) CountersBuiltForTest() int { return s.countersBuilt }

// PartialCheckpointWithCountersForTest é o PartialCheckpointForTest com os
// passos dos contadores: segSync, overlays aplicados (locations e contadores)
// e msyncs, SEM o manifest — o crash entre os passos 3 e 4 do §10.2, com o
// vetor de grupos em disco à frente do ponto S.
//
// O fsync do dicionário de grupos vem junto, e não por escolha do seam: ele é
// a primeira coisa do Counter.ApplyOverlay, pelo MESMO passo do checkpoint
// (counterDictSyncStep). Um seam que levasse ids ao vetor sem ele produziria
// um estado que o produto não alcança.
func (s *Store) PartialCheckpointWithCountersForTest() error {
	if err := s.PartialCheckpointForTest(); err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	for _, c := range s.counters {
		if err := c.ApplyOverlay(s.counterDictSyncStep(c, "")); err != nil {
			return err
		}
		if err := c.Msync(); err != nil {
			return err
		}
	}
	return nil
}

// SetCounterDictSyncHookForTest instala o seam do fsync do dicionário de
// grupos (nil em produção): o hook recebe o nome do contador e o fsync de
// fato, e decide se o chama. Devolver erro sem chamar é a falha de fsync
// injetada; chamar e olhar o arquivo depois é observar o prefixo durável.
func (s *Store) SetCounterDictSyncHookForTest(fn func(counter string, sync func() error) error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.dictSyncHook = fn
}

// SetAuditTraceForTest instala o observador chamado depois de cada linha
// que a auditoria dos contadores varre (seam de teste; nil em produção). É por
// ele que o teste escreve NO MEIO da varredura — o único jeito determinístico
// de estourar o snapshot que a auditoria segura.
func (s *Store) SetAuditTraceForTest(fn func(row types.RowID)) { s.audTrace = fn }

// SetAuditBeforeOfCopyForTest instala o observador chamado entre abrir o
// snapshot e copiar os totais (seam de teste; nil em produção). No código
// certo ele roda SOB o wmu — e é isso que o teste prova: um Put disparado
// dali só entra depois da cópia.
func (s *Store) SetAuditBeforeOfCopyForTest(fn func()) { s.audBeforeOfCopy = fn }

// RetirementActiveForTest expõe se o laço de aposentadoria de grupos está
// de pé — o mesmo observável que o servidor consulta na subida.
func (s *Store) RetirementActiveForTest() bool { return s.RetirementOfGroupsActive() }
