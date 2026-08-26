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
	// O hash entra aqui pela mesma razão dos bitmaps: este seam reproduz o
	// crash entre os passos 3 e 4 do §10.2, e o estado que ele tem de deixar é
	// "todo índice durável, manifest ainda não". Esquecer um motor faria o seam
	// simular um crash que o protocolo não produz.
	for _, ix := range s.hashes {
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

// SetAposentadoriaTraceForTest instala o gravador dos passos da aposentadoria
// de segmento (seam de teste; nil em produção). É por ele que o crash test
// para o processo DENTRO de cada janela do D5.
func (s *Store) SetAposentadoriaTraceForTest(fn func(passo string)) { s.apTrace = fn }

// SetSyncDeDiretorioTraceForTest instala o observador dos fsyncs de diretório
// da aposentadoria (seam de teste; nil em produção). Ele é chamado de dentro
// do helper que executa o fsync, DEPOIS de o fsync devolver nil — quem apaga
// a chamada apaga o aviso junto, que é a única forma de o teste distinguir a
// presença da ausência de uma durabilidade que não deixa rastro em disco.
func (s *Store) SetSyncDeDiretorioTraceForTest(fn func(dir string)) { s.apDirSync = fn }
