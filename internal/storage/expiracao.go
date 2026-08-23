package storage

import (
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// UsaRelogio troca o relógio da expiração. Exclusivo de teste: um teste de
// prazo precisa avançar o tempo sem dormir, e a alternativa seria uma suíte
// que leva minutos ou que só prova a expiração com prazos irrealistas.
func (s *Store) UsaRelogio(f func() time.Time) {
	s.agora = f
}

// TemExpiracao reporta se este dataset expira por escrita.
func (s *Store) TemExpiracao() bool { return s.escritas != nil }

// Expira publica a lápide de uma row cujo prazo venceu, tornando o record
// anterior recuperável pela compaction.
//
// É método próprio, e não uma chamada a Put, pela mesma razão do copy-forward:
// o Put resolve a key pela primary e apenda o que lhe deram, sem perguntar se
// o estado de origem ainda é o corrente. Aqui a pergunta é obrigatória.
//
// A lápide é um RECORD DURÁVEL, não um bit na location. applyRecord deriva a
// flag publicada do próprio record, e o replay do tail reconstrói as locations
// a partir dos records: uma lápide que só existisse no índice seria apagada
// pelo primeiro replay e a row RESSUSCITARIA — exatamente o que o §58 proíbe.
//
// Devolve expirou=false, sem erro, quando a row já não precisava expirar: sem
// location, já tombstonada, ou RENOVADA entre a decisão do agendador e esta
// chamada. O último caso é o que torna a corrida com o Put segura.
func (s *Store) Expira(rowID types.RowID) (expirou bool, err error) {
	if s.escritas == nil {
		return false, fmt.Errorf("%w: dataset sem expiração declarada", ErrInvalidArgument)
	}
	if err := s.entra(); err != nil {
		return false, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return false, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}

	// A location E o prazo são relidos DENTRO da seção crítica. É o ponto
	// inteiro do método: o Put toma o mesmo wmu, então ou ele já terminou (e
	// aqui se vê o prazo renovado, e aborta) ou ele espera (e vê a lápide, e
	// trata como ressurreição). Não há janela entre as duas leituras.
	//
	// Mover qualquer uma delas para fora do lock NÃO produz acesso de memória
	// não sincronizado — locs.Get é seguro para leitor concorrente —, então o
	// -race não veria a violação de atomicidade. Só um teste com seam vê.
	packed, had := s.locs.Get(rowID)
	if !had {
		return false, nil
	}
	seg, off, flags := rowindex.Unpack(packed)
	if flags&types.FlagTombstone != 0 {
		return false, nil // idempotente
	}
	if !s.prazoVencido(rowID) {
		return false, nil // renovada entre a decisão e agora
	}

	rec, buf, err := s.segR.ReadRecord(seg, off, s.expiraBuf[:0])
	s.expiraBuf = buf
	if err != nil {
		return false, err
	}
	if rec.RowID != rowID {
		return false, fmt.Errorf("%w: location da row %d aponta para record da row %d",
			encoding.ErrCorrupt, rowID, rec.RowID)
	}

	// O payload da lápide preserva o instante de escrita e nada mais.
	//
	// Preservá-lo é o que mantém o índice de expiração convergente no replay:
	// applyRecord relê o campo do payload, e uma lápide sem ele deixaria a
	// entrada como DESCONHECIDA depois de um crash — a row voltaria a ganhar
	// carência e a lápide seria reescrita a cada reinício.
	//
	// Não preservar o RESTO é o que faz a fase de CLEAR do applyRecord limpar
	// todas as postings da row: com o payload novo sem campo indexado nenhum,
	// todo campo do anterior cai no ramo "campo que sai". É o "secondary
	// indexes são limpos" do §56, sem uma linha nova.
	ms, temInstante := instanteDeEscrita(rec.Payload, s.campoDeEscrita)
	tomb := s.tombBuf[:0]
	if temInstante {
		tomb, err = encoding.AppendField(tomb, s.campoDeEscrita,
			types.Value{Type: types.TypeTimestamp, I64: ms})
		if err != nil {
			return false, err
		}
	}
	s.tombBuf = tomb

	// A sequence da lápide é a ÚLTIMA aplicada, não zero: é o lastSequence do
	// §58, e é ele que faz o stale check recusar o evento antigo que tentaria
	// reabrir a entidade.
	buf, err = encoding.BuildRecord(s.compactBuf[:0], rowID, rec.Sequence,
		types.FlagTombstone, rec.Key, tomb)
	s.compactBuf = buf
	if err != nil {
		return false, err
	}
	novoSeg, novoOff, err := s.segW.Append(buf)
	if err != nil {
		return false, s.failWriter(err)
	}
	s.curSegID.Store(novoSeg)

	// applyRecord entrega o resto de graça: baixa da liveness do segmento de
	// origem (é o que o torna candidato à compaction), limpeza das postings,
	// e o commit da location já com o bit. countsAsNew é false — a row não
	// nasceu, morreu.
	if err := s.applyRecord(recordApply{
		rowID:       rowID,
		key:         rec.Key,
		seq:         rec.Sequence,
		flags:       types.FlagTombstone,
		payload:     tomb,
		prevPayload: rec.Payload,
		hadPrevious: true,
		countsAsNew: false,
		seg:         novoSeg,
		off:         novoOff,
		recLen:      len(buf),
		mode:        applyWrite,
	}); err != nil {
		return false, s.failWriter(err)
	}
	s.expiradas.Add(1)
	return true, nil
}
