package storage

import (
	"context"
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
	tomb, err := s.montaLapide(rec.Payload)
	if err != nil {
		return false, err
	}

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

// montaLapide devolve o payload de uma lápide: o instante de escrita, e nada
// mais.
//
// Preservar o instante é o que mantém o índice de expiração convergente no
// replay — applyRecord relê o campo do payload, e uma lápide sem ele deixaria
// a entrada como DESCONHECIDA depois de um crash, fazendo a row ganhar
// carência e a lápide ser reescrita a cada reinício.
//
// Não preservar o RESTO é o que devolve disco (o payload grande morre) e o
// que limpa os índices secundários: com o payload novo sem campo indexado
// nenhum, todo campo do anterior cai no ramo "campo que sai" do
// applyBitmapClears.
//
// Usa o tombBuf, que é do writer e está sob o wmu — os dois chamadores (o
// coletor e a compaction) já estão dentro da seção crítica.
func (s *Store) montaLapide(payloadAnterior []byte) ([]byte, error) {
	tomb := s.tombBuf[:0]
	if ms, tem := instanteDeEscrita(payloadAnterior, s.campoDeEscrita); tem {
		var err error
		if tomb, err = encoding.AppendField(tomb, s.campoDeEscrita,
			types.Value{Type: types.TypeTimestamp, I64: ms}); err != nil {
			return nil, err
		}
	}
	s.tombBuf = tomb
	return tomb, nil
}

// LimitesDeExpiracao é o freio de uma passada do coletor. Os dois valem
// JUNTOS e vence o primeiro a ser atingido.
//
// São dois porque os riscos são opostos. O de QUANTIDADE protege o disco e o
// lock de escrita: marcar uma entidade é um append, então uma leva grande
// escreve muito antes de a compaction recuperar qualquer coisa, e cada marca
// disputa o wmu com a ingestão. O de TEMPO protege do caso inverso — varrer
// milhões de entradas sem achar nada que vença não escreve nada, mas seguraria
// a goroutine numa passada longa.
type LimitesDeExpiracao struct {
	MaxPorPassada   int
	TempoPorPassada time.Duration
}

// ResultadoDaExpiracao descreve o que uma passada fez.
type ResultadoDaExpiracao struct {
	Examinadas int  // entradas do índice lidas
	Expiradas  int  // entidades que ganharam lápide
	Renovadas  int  // venceram na leitura do índice mas foram renovadas antes da marca
	Carencias  int  // entidades sem instante que ganharam prazo cheio a partir de agora
	DeuVolta   bool // o cursor completou o círculo e recomeçou
	BateuTeto  bool // parou pelo limite, não por ter acabado a lista
}

// ExpiraUmaPassada percorre o índice de escritas a partir de onde a passada
// anterior parou e marca as entidades vencidas, respeitando os limites.
//
// O cursor é de MEMÓRIA, não persistido: reiniciar o processo faz a próxima
// passada recomeçar do rowId 0. Com prazos da ordem de dias isso não perde
// nada — o que ficou para trás é reencontrado na volta seguinte —, e persistir
// um cursor custaria um arquivo a mais no caminho de crash para economizar uma
// varredura de mmap que leva milissegundos.
//
// A entrada DESCONHECIDA (zero, de entidade materializada antes de a expiração
// ser ligada) recebe carência: grava-se agora como instante de escrita, e ela
// passa a vencer um prazo cheio à frente. É o que permite ligar a expiração
// num dataset já materializado sem reprocessar nada, e é também o que impede
// essas entidades de ficarem imortais por acidente de calendário.
func (s *Store) ExpiraUmaPassada(ctx context.Context, lim LimitesDeExpiracao) (ResultadoDaExpiracao, error) {
	var r ResultadoDaExpiracao
	if s.escritas == nil {
		return r, nil
	}
	if lim.MaxPorPassada <= 0 || lim.TempoPorPassada <= 0 {
		return r, fmt.Errorf("%w: limites de expiração devem ser positivos", ErrInvalidArgument)
	}
	if err := s.entra(); err != nil {
		return r, err
	}
	defer s.sai()

	limite := types.RowID(s.nextRowID.Load())
	if limite == 0 {
		return r, nil
	}
	fim := s.agora().Add(lim.TempoPorPassada)
	id := s.cursorExpiracao
	if id >= limite {
		id, r.DeuVolta = 0, true
	}

	for percorridas := types.RowID(0); percorridas < limite; percorridas++ {
		// O ctx é checado a cada entrada, e não só entre marcas: uma passada
		// que não encontra nada para expirar não passaria por nenhum ponto de
		// cancelamento, e o Close ficaria esperando a varredura inteira.
		if ctx.Err() != nil {
			break
		}
		if r.Expiradas >= lim.MaxPorPassada || !s.agora().Before(fim) {
			r.BateuTeto = true
			break
		}

		r.Examinadas++
		ms, conhecido := s.escritas.Get(id)
		switch {
		case !conhecido:
			dada, err := s.daCarencia(id)
			if err != nil {
				return r, err
			}
			if dada {
				r.Carencias++
			}
		case s.agora().UnixMilli()-ms >= s.expireAfterWrite.Milliseconds():
			// O Expira releva a decisão dentro do wmu, então uma renovação
			// que chegue entre esta leitura e a marca faz ele abortar sozinho.
			expirou, err := s.Expira(id)
			if err != nil {
				return r, err
			}
			if expirou {
				r.Expiradas++
			} else {
				r.Renovadas++
			}
		}

		id++
		if id >= limite {
			id, r.DeuVolta = 0, true
		}
	}
	s.cursorExpiracao = id
	return r, nil
}

// intervaloEntreExpiracoes é o cochilo entre passadas do coletor.
//
// Um minuto, e não os cinco segundos da compaction, porque as duas correm
// atrás de coisas com urgências diferentes: a compaction persegue disco que
// já é lixo AGORA, o coletor persegue prazos que vencem em dias. Com o freio
// de mil marcas por passada, um minuto ainda dá 60 mil expirações por hora —
// mais do que qualquer dataset produz de abandono nessa janela.
const intervaloEntreExpiracoes = time.Minute

// IniciaExpiracao sobe o coletor do idle timeout. Sem expiração declarada não
// há goroutine: o dataset não paga uma thread para não fazer nada.
func (s *Store) IniciaExpiracao(ctx context.Context, lim LimitesDeExpiracao) {
	if s.escritas == nil {
		return
	}
	s.expMu.Lock()
	defer s.expMu.Unlock()
	if s.expCancela != nil {
		return
	}
	ctx, cancela := context.WithCancel(ctx)
	s.expCancela = cancela
	// O medidor sobe aqui, e não dentro da goroutine, pela mesma razão da
	// compaction: quem chamou tem direito de ver o estado que instalou, e um
	// Stats lido antes do primeiro escalonamento não pode dizer que o coletor
	// não subiu.
	s.expAtiva.Store(true)
	s.expEmVoo.Add(1)
	go func() {
		defer s.expEmVoo.Done()
		defer s.expAtiva.Store(false)
		s.rodaExpiracao(ctx, lim)
	}()
}

// ParaExpiracao cancela e AGUARDA a passada em voo.
//
// Aguardar não é zelo: a passada toma o wmu por trecho e lê o mmap do índice,
// então um Close que vencesse a corrida desmapearia a região sob os pés dela.
// É a mesma razão do ParaCompaction, e é por isso que o Close chama os dois
// antes do quiesce.
func (s *Store) ParaExpiracao() {
	s.expMu.Lock()
	defer s.expMu.Unlock()
	if s.expCancela != nil {
		s.expCancela()
		s.expCancela = nil
	}
	s.expEmVoo.Wait()
}

func (s *Store) rodaExpiracao(ctx context.Context, lim LimitesDeExpiracao) {
	t := time.NewTimer(0) // primeira passada imediata
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		r, err := s.ExpiraUmaPassada(ctx, lim)
		if err != nil {
			// Erro aqui é do writer (sticky) ou do store fechando. Nos dois
			// casos insistir só produz log em laço: o laço termina e o
			// medidor cai, que é o sinal que o operador tem.
			s.opts.Log.Warn("expiracao.parou",
				"o coletor do idle timeout encerrou e o disco das entidades vencidas deixa de ser devolvido",
				"erro", err)
			return
		}
		// A carência entra na condição de propósito: sem ela, a passada que
		// só dá prazo às entidades legadas — que é EXATAMENTE o que acontece
		// ao ligar a expiração num dataset já materializado — não emitiria
		// linha nenhuma, e o operador não teria como distinguir "está
		// trabalhando" de "não subiu".
		if r.Expiradas > 0 || r.Renovadas > 0 || r.Carencias > 0 {
			s.opts.Log.Info("expiracao.passada",
				"passada do coletor do idle timeout",
				"examinadas", r.Examinadas, "expiradas", r.Expiradas,
				"renovadas", r.Renovadas, "carencias", r.Carencias,
				"bateuTeto", r.BateuTeto, "deuVolta", r.DeuVolta)
		}
		// Passada que bateu o teto volta JÁ: há trabalho represado, e esperar
		// o cochilo inteiro faria a fila drenar na velocidade do relógio em
		// vez da do disco.
		espera := intervaloEntreExpiracoes
		if r.BateuTeto {
			espera = 0
		}
		t.Reset(espera)
	}
}

// daCarencia grava o instante de agora para uma entidade que não tem nenhum,
// dando-lhe um prazo cheio a partir daqui.
//
// Toma o wmu, e o motivo NÃO é corrida de dados: o valor do índice é gravado
// com AtomicStore64 e o remapeamento do Grow é protegido pelo mutex da própria
// região, então gravar fora da seção crítica seria seguro para a memória.
//
// O motivo é o stickyErr. Fora do wmu não há como consultá-lo, e a carência
// continuaria gravando no índice de um writer que já morreu — deixando prazo
// novo para entidades num dataset que só pode ser reaberto por replay. Sob o
// lock, ela para junto com todo o resto.
//
// O efeito colateral é bem-vindo: o par Get+Set vira atômico, então a carência
// não sobrescreve um instante que a ingestão acabou de gravar.
//
// O custo é uma aquisição de lock por entidade, e é aceitável porque a passada
// é limitada: no pior caso são MaxPorPassada aquisições espalhadas ao longo de
// TempoPorPassada, intercaladas com uma ingestão que já toma esse mesmo lock a
// cada evento.
//
// Devolve false, sem erro, quando não havia o que fazer: rowId sem location
// (nunca publicado, ou resíduo de janela de crash) não ganha prazo, porque
// encher o índice de entradas de rows que não existem só faria a varredura
// seguinte reexaminá-las para sempre. E entidade que ganhou instante entre a
// leitura da passada e esta chamada já está resolvida.
func (s *Store) daCarencia(id types.RowID) (bool, error) {
	if err := s.entra(); err != nil {
		return false, err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return false, fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	if _, conhecido := s.escritas.Get(id); conhecido {
		return false, nil // a ingestão chegou primeiro
	}
	if _, publicada := s.locs.Get(id); !publicada {
		return false, nil
	}
	if err := s.escritas.Set(id, s.agora().UnixMilli()); err != nil {
		return false, err
	}
	return true, nil
}
