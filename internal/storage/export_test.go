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

// SetSetErrHookForTest injeta falhas na fase de Set dos bitmaps — produz o
// phantom commit (§14.12) com payload replayável.
func (s *Store) SetSetErrHookForTest(fn func(types.FieldID) error) { s.setErrHook = fn }

// PartialCheckpointForTest executa os passos 1-3 do checkpoint (segSync,
// ApplyOverlay, msyncs) SEM gravar o manifest — simula o crash entre os
// passos 3 e 4 do §10.2 (índices em disco à frente do manifest).
func (s *Store) PartialCheckpointForTest() error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if err := s.segW.Sync(); err != nil {
		return err
	}
	if err := s.locs.ApplyOverlay(); err != nil {
		return err
	}
	if err := s.prim.Msync(); err != nil {
		return err
	}
	if err := s.locs.Msync(); err != nil {
		return err
	}
	if err := s.seqs.Msync(); err != nil {
		return err
	}
	for _, ix := range s.bitmaps {
		if err := ix.Msync(); err != nil {
			return err
		}
	}
	return nil
}
