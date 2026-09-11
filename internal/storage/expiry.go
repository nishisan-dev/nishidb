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
	s.now = f
	// Os contadores marcam o instante em que um grupo esvazia com o MESMO
	// relógio: um teste de aposentadoria avança o tempo por aqui.
	for _, c := range s.contadores {
		c.UseClock(f)
	}
}

// TemExpiracao reporta se este dataset expira por escrita.
func (s *Store) TemExpiracao() bool { return s.writes != nil }

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
	if s.writes == nil {
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
	// Par (Get, Read) sob a barreira de aposentadoria: ver o godoc de aposMu.
	// A barreira segue segurada pelo resto do método, e é aceitável: este
	// caminho já tem o wmu na mão, a ordem wmu→aposMu é a do godoc, e o que
	// se paga é atrasar uma aposentadoria por um trabalho limitado.
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	packed, had := s.locs.Get(rowID)
	if !had {
		return false, nil
	}
	seg, off, flags := rowindex.Unpack(packed)
	if flags&types.FlagTombstone != 0 {
		return false, nil // idempotente
	}
	if !s.prazoVencido(rowID, s.now()) {
		return false, nil // renovada entre a decisão e agora
	}

	rec, buf, err := s.segR.ReadRecord(seg, off, s.expireBuffer[:0])
	s.expireBuffer = buf
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
	// Não preservar o RESTO é o que tira a row do índice dali em diante: a
	// lápide não tem campo indexado nenhum, então nenhuma posting nova nasce
	// para ela (não há fase de Clear desde a §5.2 do subsistema 1 — as
	// postings antigas viram candidatas que o §44 descarta e morrem com os
	// segmentos). É o "secondary indexes são limpos" do §56 no OBSERVÁVEL,
	// sem uma linha nova.
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
	if err := s.aposAppend(novoSeg); err != nil {
		return false, s.failWriter(err)
	}

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
		// A lápide MUDA o estado visível: quem abriu a consulta antes dela
		// tem de continuar recebendo a linha. Sem versionar aqui, o coletor
		// de expiração apagaria do meio do resultado a linha que o relógio
		// congelado acabou de preservar — os dois mecanismos se anulariam.
		versiona: true,
		seg:      novoSeg,
		off:      novoOff,
		recLen:   len(buf),
		mode:     applyWrite,
	}); err != nil {
		return false, s.failWriter(err)
	}
	s.expiredCount.Add(1)
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
// que tira a row do índice DALI EM DIANTE: a lápide não tem campo indexado
// nenhum, então aplicaIndiceSets não grava posting nenhuma para ela. As
// postings ANTIGAS ninguém limpa — não há fase de Clear desde a §5.2 do
// subsistema 1 —, elas ficam nos segmentos em que nasceram como candidatas que
// a revalidação do §44 descarta, e morrem com os segmentos. O "secondary
// indexes são limpos" do §56 continua verdadeiro no OBSERVÁVEL (a row expirada
// não sai em consulta indexada), por outro mecanismo.
//
// Usa o tempBuffer, que é do writer e está sob o wmu — os dois chamadores (o
// coletor e a compaction) já estão dentro da seção crítica.
func (s *Store) montaLapide(payloadAnterior []byte) ([]byte, error) {
	tomb := s.tempBuffer[:0]
	if ms, tem := writeInstant(payloadAnterior, s.writeFieldID); tem {
		var err error
		if tomb, err = encoding.AppendField(tomb, s.writeFieldID,
			types.Value{Type: types.TypeTimestamp, I64: ms}); err != nil {
			return nil, err
		}
	}
	s.tempBuffer = tomb
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
//
// Examinadas conta as entradas da AGENDA em que a descida entrou — os nós de
// sumário que prometiam vencimento, mais as folhas entregues. Continua sendo
// "entradas de índice examinadas", que é o que a métrica sempre disse, mas o
// índice passou a ser uma árvore em vez de um array percorrido do começo ao
// fim. A diferença é a fase inteira: antes o número crescia com o TAMANHO do
// dataset a cada passada — 24,1 milhões no piloto — e agora cresce com o que
// venceu.
//
// Não há DeuVolta porque não há mais círculo a completar: a passada não
// percorre o dataset, ela pergunta quem venceu.
type ResultadoDaExpiracao struct {
	Examinadas int  // entradas da agenda em que a descida entrou
	Expiradas  int  // entidades que ganharam lápide
	Renovadas  int  // a agenda entregou, mas o writer discordou sob o lock
	Carencias  int  // entidades sem instante que ganharam prazo cheio a partir de agora
	BateuTeto  bool // parou pelo limite, não por ter acabado a lista
}

// ExpiraUmaPassada trata as entidades cujo vencimento agendado já passou,
// respeitando os limites.
//
// Ela PERGUNTA em vez de procurar. A versão anterior varria o índice de
// escritas a partir de um cursor circular, lendo toda linha do dataset para
// achar as poucas vencidas: no piloto, uma volta completa em 4,57 milhões de
// linhas a cada ~2,5 minutos, 19% do CPU da ingestão, para encontrar as ~0,4%
// que a regra condicional de três minutos tinha vencido. A agenda troca essa
// varredura por uma descida que só entra nos ramos onde há vencimento.
//
// Duas coisas somem junto com a varredura. O CURSOR, porque não há mais
// círculo a percorrer — e com ele some a pergunta "de onde recomeçar depois de
// um restart". E o PRÉ-FILTRO DE TOMBSTONE, porque a linha tombstonada recebe
// o sentinela "nunca" e sai da agenda de vez, em vez de continuar vencida para
// sempre e ser reexaminada em toda volta.
//
// A entrada DESCONHECIDA (zero, de entidade materializada antes de a expiração
// ser ligada) recebe carência: grava-se agora como instante de escrita, e ela
// passa a vencer um prazo cheio à frente. É o que permite ligar a expiração
// num dataset já materializado sem reprocessar nada, e é também o que impede
// essas entidades de ficarem imortais por acidente de calendário. Na agenda
// elas aparecem pelo sentinela de carência, e não por um vencimento.
func (s *Store) ExpiraUmaPassada(ctx context.Context, lim LimitesDeExpiracao) (ResultadoDaExpiracao, error) {
	var r ResultadoDaExpiracao
	if s.writes == nil {
		return r, nil
	}
	if lim.MaxPorPassada <= 0 || lim.TempoPorPassada <= 0 {
		return r, fmt.Errorf("%w: limites de expiração devem ser positivos", ErrInvalidArgument)
	}
	if err := s.entra(); err != nil {
		return r, err
	}
	defer s.sai()

	fim := s.now().Add(lim.TempoPorPassada)
	var falha error
	r.Examinadas = s.schedule.VisitExpired(uint64(s.now().UnixMilli()), func(id types.RowID) bool {
		// O ctx é checado a cada linha, e não só entre marcas: uma descida que
		// entra em ramos frouxos e não acha nada para expirar não passaria por
		// nenhum ponto de cancelamento, e o Close ficaria esperando por ela.
		if ctx.Err() != nil {
			return false
		}
		if r.Expiradas >= lim.MaxPorPassada || !s.now().Before(fim) {
			r.BateuTeto = true
			return false
		}
		agiu, err := s.trataVencida(id, &r)
		if err != nil {
			falha = err
			return false
		}
		if !agiu {
			// A agenda entregou uma linha que o writer, sob o lock, disse não
			// precisar de nada: entrada frouxa deixada por uma interrupção, ou
			// por uma escrita que chegou entre a descida e a marca.
			//
			// Corrigir aqui não é zelo — é o que garante PROGRESSO. Uma folha
			// que promete um vencimento que já não existe seria reentregue em
			// toda passada, para sempre, e o coletor gastaria o teto de tempo
			// inteiro tomando o lock do writer para não fazer nada.
			if err := s.reagenda(id); err != nil {
				falha = err
				return false
			}
		}
		return true
	})
	if falha != nil {
		return r, falha
	}
	s.examinedCount.Add(uint64(r.Examinadas))
	s.lostCount.Add(uint64(r.Carencias))
	return r, nil
}

// trataVencida aplica a uma linha entregue pela schedule a ação que ela pede, e
// devolve agiu=false quando não havia nada a fazer.
func (s *Store) trataVencida(id types.RowID, r *ResultadoDaExpiracao) (bool, error) {
	if _, conhecido := s.writes.Get(id); !conhecido {
		dada, err := s.daCarencia(id)
		if err != nil {
			return false, err
		}
		if dada {
			r.Carencias++
		}
		return dada, nil
	}
	// O Expira revalida a decisão dentro do wmu, então uma renovação que
	// chegue entre a descida e a marca faz ele abortar sozinho.
	expirou, err := s.Expira(id)
	if err != nil {
		return false, err
	}
	if expirou {
		r.Expiradas++
	} else {
		r.Renovadas++
	}
	return expirou, nil
}

// reagenda recoloca a linha no vencimento que os índices dizem, sob o lock do
// writer.
//
// Toma o wmu pelo mesmo motivo do daCarencia, e não por corrida de dados: fora
// da seção crítica não há como consultar o stickyErr, e a correção seguiria
// escrevendo no índice de um writer que já morreu.
func (s *Store) reagenda(id types.RowID) error {
	if err := s.entra(); err != nil {
		return err
	}
	defer s.sai()
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.stickyErr != nil {
		return fmt.Errorf("%w: %w", ErrWriterFailed, s.stickyErr)
	}
	return s.schedule.Update(id, s.vencimentoDe(id))
}

// tetoDeEspera é o quanto o coletor aceita dormir mesmo quando a agenda diz
// que não há nada por perto.
//
// A agenda dá o instante do próximo vencimento, e dormir exatamente até lá é o
// que faz o coletor parar de acordar de minuto em minuto para não achar nada.
// O teto existe porque uma linha pode entrar na agenda com vencimento ANTES
// daquele em que o coletor foi dormir — o writer atualiza a árvore, não a
// goroutine adormecida —, e sem ele essa linha esperaria até o vencimento
// antigo, que pode estar a dias de distância. A alternativa seria acordar o
// coletor a partir do write path, e ela custa um acoplamento que este teto de
// um minuto compra por nada: o ganho da fase é não VARRER, e ler o topo da
// árvore uma vez por minuto é da ordem de microssegundos.
//
// Um minuto, e não os cinco segundos da compaction, porque as duas correm
// atrás de coisas com urgências diferentes: a compaction persegue disco que
// já é lixo AGORA, o coletor persegue prazos que vencem em dias.
const tetoDeEspera = time.Minute

// IniciaExpiracao sobe o coletor do idle timeout. Sem expiração declarada não
// há goroutine: o dataset não paga uma thread para não fazer nada.
func (s *Store) IniciaExpiracao(ctx context.Context, lim LimitesDeExpiracao) {
	if s.writes == nil {
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
				"bateuTeto", r.BateuTeto)
		}
		// Passada que bateu o teto volta JÁ: há trabalho represado, e esperar
		// o cochilo inteiro faria a fila drenar na velocidade do relógio em
		// vez da do disco.
		t.Reset(s.esperaAteOProximoVencimento(r))
	}
}

// esperaAteOProximoVencimento decide quanto o coletor dorme antes da passada
// seguinte.
//
// Passada que bateu o teto volta JÁ: há trabalho represado, e esperar faria a
// fila drenar na velocidade do relógio em vez da do disco.
//
// Fora isso, a espera é até o vencimento mais próximo que a schedule conhece,
// limitada pelo tetoDeEspera. O valor que a schedule devolve é um limite
// INFERIOR do vencimento real — a invariante da árvore garante isso —, então
// errar aqui só pode acordar o coletor cedo, nunca tarde.
func (s *Store) esperaAteOProximoVencimento(r ResultadoDaExpiracao) time.Duration {
	if r.BateuTeto {
		return 0
	}
	venc, ok := s.schedule.NextExpiry()
	if !ok {
		return tetoDeEspera
	}
	falta := time.UnixMilli(int64(venc)).Sub(s.now())
	return max(0, min(falta, tetoDeEspera))
}

// daCarencia grava o instante de now para uma entidade que não tem nenhum,
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
	if _, conhecido := s.writes.Get(id); conhecido {
		return false, nil // a ingestão chegou primeiro
	}
	if _, publicada := s.locs.Get(id); !publicada {
		return false, nil
	}
	if err := s.writes.Set(id, s.now().UnixMilli()); err != nil {
		return false, err
	}
	// A agenda sai do sentinela de carência e passa a apontar o vencimento de
	// verdade. Sem esta linha a entrada ficaria em Carencia para sempre, e a
	// linha seria reentregue em toda descida — o coletor daria a mesma
	// carência indefinidamente, tomando o lock do writer a cada volta.
	if s.schedule != nil {
		if err := s.schedule.Update(id, s.vencimentoDe(id)); err != nil {
			return false, err
		}
	}
	return true, nil
}
