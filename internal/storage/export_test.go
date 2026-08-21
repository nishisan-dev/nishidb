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

// PrimaryUsedForTest expõe a ocupação da primary: é o observável que denuncia
// slot duplicado quando o replay insere a mesma key duas vezes.
func (s *Store) PrimaryUsedForTest() uint64 { return s.prim.Used() }

// EspelhosDivergentesForTest expõe quantas vezes o checkpoint encontrou os
// espelhos do Stats fora dos mapas de contabilidade. É o único observável da
// janela entre o replay do tail e o Checkpoint do Open: depois que o Open
// retorna, a recarga já apagou o rastro e nenhuma leitura de Stats distingue
// uma recovery que semeou os espelhos de uma que os deixou furados.
func (s *Store) EspelhosDivergentesForTest() uint64 { return s.espelhosDivergentes.Load() }

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

// CorrompeEspelhoParaTeste move os espelhos atômicos por fora dos mapas de
// contabilidade — a precondição que a conferência do checkpoint existe para
// achar e que nenhum caminho legítimo do writer produz.
//
// Roda sob o mutex do writer porque escrever nos espelhos é writer-only
// (§128, e o godoc dos três campos no Store): cutucar os atômicos de fora do
// contrato faria o seam plantar um estado que o produto não consegue
// alcançar, e um detector aferido contra estado impossível não diz nada sobre
// o estado real.
func (s *Store) CorrompeEspelhoParaTeste(fn func(*Store)) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	fn(s)
}
