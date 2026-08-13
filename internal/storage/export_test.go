package storage

// SetCheckpointTraceForTest instala o gravador da ordem de durabilidade do
// Checkpoint (seam de teste; nil em produção).
func (s *Store) SetCheckpointTraceForTest(fn func(step string)) { s.cpTrace = fn }

// ClearFailuresForTest expõe o contador de erros tolerados na fase de Clear.
func (s *Store) ClearFailuresForTest() uint64 { return s.clearFailures.Load() }
