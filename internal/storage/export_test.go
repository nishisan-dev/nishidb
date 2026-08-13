package storage

import "github.com/nishisan-dev/nishidb/internal/types"

// SetCheckpointTraceForTest instala o gravador da ordem de durabilidade do
// Checkpoint (seam de teste; nil em produção).
func (s *Store) SetCheckpointTraceForTest(fn func(step string)) { s.cpTrace = fn }

// ClearFailuresForTest expõe o contador de erros tolerados na fase de Clear.
func (s *Store) ClearFailuresForTest() uint64 { return s.clearFailures.Load() }

// SetClearErrHookForTest injeta falhas na fase de Clear dos bitmaps para
// exercitar o caminho tolerado pós-commit.
func (s *Store) SetClearErrHookForTest(fn func(types.FieldID) error) { s.clearErrHook = fn }
