package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/mmap"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// intervaloEntreRetencoes é o tempo entre duas passadas.
//
// Um minuto, e não os cinco segundos da compaction, porque as duas correm
// atrás de coisas de ritmos diferentes: a compaction persegue garbage que
// nasce a cada update, e aqui o que muda é o relógio e o tamanho de um
// segmento de centenas de MiB. É o mesmo número do coletor de expiração, e
// pela mesma razão — a resolução declarável mínima é de um minuto.
const intervaloEntreRetencoes = time.Minute

// LimitesDeRetencao são as duas políticas do bloco `history`. Zero desliga a
// política correspondente; as duas zeradas fazem a passada não ter o que
// fazer, e o schema já recusa essa combinação no load.
type LimitesDeRetencao struct {
	// ExpireAfterWrite é o prazo de uma linha contado do _writtenAt dela.
	ExpireAfterWrite time.Duration
	// MaxBytes é o teto dos SEGMENTOS DE DADOS — o que a purga sabe devolver.
	MaxBytes int64
}

// Ativa reporta se há alguma política declarada.
func (l LimitesDeRetencao) Ativa() bool { return l.ExpireAfterWrite > 0 || l.MaxBytes > 0 }

// ResultadoDeRetencao é o que uma passada fez.
type ResultadoDeRetencao struct {
	// MarcaAvancou é quantos rowIds saíram da visibilidade nesta passada.
	MarcaAvancou uint64
	// PorTempo e PorTamanho contam os segmentos aposentados por cada política.
	PorTempo, PorTamanho int
	// BytesDevolvidos é a soma dos tamanhos dos segmentos removidos.
	BytesDevolvidos int64
}

// IniciaRetencao sobe a goroutine de retenção do dataset histórico.
//
// Mesmo molde de IniciaCompaction e IniciaExpiracao: primeira passada
// imediata, medidor atômico observável, cancelamento por contexto, e o Close
// cancela e AGUARDA antes do quiesce. Chamada sobre um dataset materializado
// não faz nada — é o que permite ao servidor chamá-la sem perguntar.
func (s *Store) IniciaRetencao(ctx context.Context, lim LimitesDeRetencao) {
	if !s.EhHistorico() || !lim.Ativa() {
		return
	}
	s.retMu.Lock()
	defer s.retMu.Unlock()
	if s.retCancela != nil {
		return
	}
	ctx, cancela := context.WithCancel(ctx)
	s.retCancela = cancela
	// O medidor sobe AQUI e não dentro da goroutine, pela razão do §92: quem
	// chamou tem direito de ver o estado que acabou de instalar.
	s.retAtiva.Store(true)
	s.retEmVoo.Add(1)
	go func() {
		defer s.retEmVoo.Done()
		defer s.retAtiva.Store(false)
		s.rodaRetencao(ctx, lim)
	}()
}

// ParaRetencao cancela a goroutine e ESPERA por ela. Idempotente, e pode ser
// chamada sem nunca ter havido retenção.
func (s *Store) ParaRetencao() {
	s.retMu.Lock()
	cancela := s.retCancela
	s.retCancela = nil
	s.retMu.Unlock()
	if cancela != nil {
		cancela()
	}
	s.retEmVoo.Wait()
}

// RetencaoAtiva reporta se a goroutine está de pé.
func (s *Store) RetencaoAtiva() bool { return s.retAtiva.Load() }

func (s *Store) rodaRetencao(ctx context.Context, lim LimitesDeRetencao) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		res, err := s.RetencaoUmaPassada(ctx, lim)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Para de vez, como a compaction: o erro que chega aqui é o modo
			// falho sticky do writer ou corrupção recusada, e nos dois casos a
			// passada seguinte reencontraria o mesmo estado. Quem observa isso
			// é a série de retenção ativa — o log fala com quem já foi lê-lo.
			s.opts.Log.Error("retencao.parou",
				"a retenção do histórico parou e o disco não será devolvido até a reabertura",
				"dataset", s.opts.Dataset, "erro", err)
			return
		}
		if res.PorTempo+res.PorTamanho > 0 {
			s.opts.Log.Info("retencao.passada",
				"segmentos aposentados pela retenção do histórico",
				"dataset", s.opts.Dataset,
				"porTempo", res.PorTempo, "porTamanho", res.PorTamanho,
				"bytes", res.BytesDevolvidos, "marcaDagua", s.rowIDBase.Load())
		}
		t.Reset(intervaloEntreRetencoes)
	}
}

// RetencaoUmaPassada roda uma passada completa: avança a marca-d'água pelo
// prazo, aposenta o que venceu e aposenta o que não cabe.
//
// A ORDEM é a garantia. A marca-d'água anda ANTES de qualquer arquivo sair do
// disco, e ela é durabilizada pelo primeiro dos quatro passos da
// aposentadoria. Na ordem inversa, um crash entre o unlink e o checkpoint
// deixaria locations vivas apontando para um segmento que já não existe — e a
// primeira consulta a tocá-las erraria com "segmento ausente" em vez de
// simplesmente não ver a linha.
func (s *Store) RetencaoUmaPassada(ctx context.Context, lim LimitesDeRetencao) (ResultadoDeRetencao, error) {
	var res ResultadoDeRetencao
	if !s.EhHistorico() {
		return res, fmt.Errorf("%w: %s não é uma tabela histórica", ErrInvalidArgument, s.opts.Dataset)
	}
	if !lim.Ativa() {
		return res, nil
	}
	// Uma passada por vez. Duas concorrentes disputariam a mesma
	// aposentadoria e a segunda encontraria o segmento já renomeado.
	s.retPassadaMu.Lock()
	defer s.retPassadaMu.Unlock()
	if err := s.entra(); err != nil {
		return res, err
	}
	defer s.sai()

	if lim.ExpireAfterWrite > 0 {
		avancou, err := s.avancaMarcaDagua(lim.ExpireAfterWrite)
		if err != nil {
			return res, err
		}
		res.MarcaAvancou = avancou
	}
	if err := s.aposentaOQueVenceu(ctx, &res); err != nil {
		return res, err
	}
	if lim.MaxBytes > 0 {
		if err := s.aposentaAteCaber(ctx, lim.MaxBytes, &res); err != nil {
			return res, err
		}
	}
	return res, nil
}

// avancaMarcaDagua acha, por busca binária, o primeiro rowId cujo _writtenAt
// ainda não venceu — e é isso que faz `expireAfterWrite: 7d` significar sete
// dias, e não "sete dias mais o resto do segmento".
//
// A busca binária é possível por uma propriedade que só a tabela histórica
// tem: os appends são serializados pelo mutex do writer e o _writtenAt é o
// relógio da ingestão, então ele é NÃO-DECRESCENTE por rowId. No dataset
// materializado a mesma pergunta exige varrer o índice de escritas, porque
// cada update reescreve o instante de uma linha qualquer.
//
// São O(log n) preads por passada — com 100 milhões de linhas, 27 leituras.
// A alternativa de varrer para achar a fronteira custaria a tabela inteira.
func (s *Store) avancaMarcaDagua(prazo time.Duration) (uint64, error) {
	corte := s.agora().Add(-prazo).UnixMilli()
	lo := s.rowIDBase.Load()
	hi := s.nextRowID.Load()
	if lo >= hi {
		return 0, nil
	}
	base := lo
	// Invariante: tudo abaixo de lo venceu, e hi ainda não foi decidido.
	for lo < hi {
		meio := lo + (hi-lo)/2
		venceu, conhecido, err := s.vencidaEm(types.RowID(meio), corte)
		if err != nil {
			return 0, err
		}
		// Instante DESCONHECIDO nunca vence, e é a mesma regra do coletor do
		// dataset materializado: uma linha sem _writtenAt não tem contra o que
		// comparar, e presumir que venceu apagaria dado por omissão.
		if !conhecido || !venceu {
			hi = meio
			continue
		}
		lo = meio + 1
	}
	if lo == base {
		return 0, nil
	}
	s.rowIDBase.Store(lo)
	return lo - base, nil
}

// vencidaEm lê o _writtenAt da linha e o compara com o corte. conhecido=false
// quando a linha não tem location (purgada ou nunca publicada) ou não carrega
// o campo.
func (s *Store) vencidaEm(id types.RowID, corte int64) (venceu, conhecido bool, err error) {
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	packed, ok := s.locs.Get(id)
	if !ok {
		return false, false, nil
	}
	seg, off, _ := rowindex.Unpack(packed)
	rec, buf, err := s.segR.ReadRecord(seg, off, s.retBuf)
	s.retBuf = buf
	if err != nil {
		return false, false, err
	}
	ms, ok := instanteDeEscrita(rec.Payload, s.campoDeEscrita)
	if !ok {
		return false, false, nil
	}
	return ms < corte, true, nil
}

// aposentaOQueVenceu remove os segmentos inteiramente cobertos pela
// marca-d'água.
func (s *Store) aposentaOQueVenceu(ctx context.Context, res *ResultadoDeRetencao) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		alvo, bytes, cobre, err := s.candidatoDeRetencao()
		if err != nil {
			return err
		}
		if !cobre {
			return nil
		}
		if err := s.aposentaDoHistorico(alvo, nil); err != nil {
			return err
		}
		res.PorTempo++
		s.retPorTempo.Add(1)
		res.BytesDevolvidos += bytes
	}
}

// aposentaAteCaber remove os segmentos mais antigos enquanto a soma dos
// dados estiver acima do teto.
//
// Aqui a marca-d'água é EMPURRADA pela purga, e não o contrário: o teto de
// tamanho é uma promessa sobre disco, e honrá-la significa deixar de enxergar
// linhas que o prazo ainda não tinha vencido. É a política mais forte, e é
// por isso que a série de instante mais antigo existe — é ela que mostra o
// maxBytes engolindo o expireAfterWrite declarado.
func (s *Store) aposentaAteCaber(ctx context.Context, maxBytes int64, res *ResultadoDeRetencao) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		total, err := s.bytesDosSegmentos()
		if err != nil {
			return err
		}
		if total <= maxBytes {
			return nil
		}
		alvo, bytes, temSeguinte, err := s.maisAntigoAposentavel()
		if err != nil {
			return err
		}
		if !temSeguinte {
			// Só sobrou o segmento corrente, que a D6 proíbe aposentar. Ficar
			// acima do teto aqui é o desenho: o schema já exige maxBytes para
			// quatro segmentos, então chegar neste ponto significa que a
			// configuração mudou por baixo — e recusar a escrita seria pior.
			return nil
		}
		primeiro, ok, err := s.primeiroRowIDDoSeguinte(alvo)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := s.aposentaDoHistorico(alvo, &primeiro); err != nil {
			return err
		}
		res.PorTamanho++
		s.retPorTamanho.Add(1)
		res.BytesDevolvidos += bytes
	}
}

// aposentaDoHistorico avança a marca-d'água (quando novaMarca não é nil) e
// aposenta o segmento pelos QUATRO passos que a compaction já usa.
//
// Reusa aposentaSegmento inteiro, sem o compactaSegmento antes: aqui não há
// record vivo a copiar para frente, porque "vivo" na tabela histórica é "o
// rowId está acima da marca-d'água" e não "a location nomeia este record". O
// primeiro dos quatro passos é um Checkpoint, e é ele que torna a marca
// durável ANTES de qualquer byte sair do disco.
func (s *Store) aposentaDoHistorico(seg uint32, novaMarca *uint64) error {
	if novaMarca != nil && *novaMarca > s.rowIDBase.Load() {
		s.rowIDBase.Store(*novaMarca)
	}
	if err := s.aposentaSegmento(seg); err != nil {
		return err
	}
	// O prefixo morto do locations.idx sai DEPOIS, e a falha dele não é a
	// falha da passada: é degradação de espaço, e o dataset acabou de devolver
	// um segmento inteiro. Filesystem sem punch-hole entra aqui uma vez por
	// aposentadoria e o aviso fica no log.
	if err := s.locs.LiberaAte(s.RowIDBase()); err != nil {
		if errors.Is(err, mmap.ErrFallocateUnsupported) {
			s.opts.Log.Warn("retencao.sem_punch_hole",
				"o filesystem não libera blocos: o prefixo morto do locations.idx continua alocado",
				"dataset", s.opts.Dataset)
			return nil
		}
		return err
	}
	return nil
}

// candidatoDeRetencao devolve o segmento mais antigo INTEIRAMENTE coberto
// pela marca-d'água. cobre=false quando não há nenhum.
func (s *Store) candidatoDeRetencao() (seg uint32, bytes int64, cobre bool, err error) {
	alvo, bytes, temSeguinte, err := s.maisAntigoAposentavel()
	if err != nil || !temSeguinte {
		return 0, 0, false, err
	}
	primeiro, ok, err := s.primeiroRowIDDoSeguinte(alvo)
	if err != nil || !ok {
		return 0, 0, false, err
	}
	// O segmento está coberto quando o PRIMEIRO rowId do seguinte já está
	// abaixo (ou é) a marca: todo record deste alvo nasceu antes daquele.
	return alvo, bytes, primeiro <= s.rowIDBase.Load(), nil
}

// maisAntigoAposentavel devolve o segmento de menor ID que NÃO é o corrente.
// O corrente nunca entra: é o arquivo em que o writer está apendando (D6).
func (s *Store) maisAntigoAposentavel() (seg uint32, bytes int64, ok bool, err error) {
	refs, err := s.listaSegmentos()
	if err != nil {
		return 0, 0, false, err
	}
	corrente := s.curSegID.Load()
	for _, ref := range refs {
		if ref.ID == corrente {
			continue
		}
		return ref.ID, ref.SizeBytes, true, nil
	}
	return 0, 0, false, nil
}

// primeiroRowIDDoSeguinte lê o rowId do primeiro record do segmento logo
// depois de alvo.
//
// É lido do ARQUIVO, e não de um mapa no manifest, porque não precisa ser
// gravado: o primeiro record de um segmento está sempre no offset fixo logo
// após o header, e um pread por aposentadoria é ruído ao lado de remover um
// arquivo de centenas de MiB. Um metadado a menos é uma janela de crash a
// menos.
func (s *Store) primeiroRowIDDoSeguinte(alvo uint32) (uint64, bool, error) {
	refs, err := s.listaSegmentos()
	if err != nil {
		return 0, false, err
	}
	for _, ref := range refs {
		if ref.ID <= alvo {
			continue
		}
		if ref.SizeBytes <= segment.HeaderSize {
			// Segmento sem record nenhum: não há fronteira a ler. Adiar é
			// correto — ele receberá o próximo append e a passada seguinte o
			// encontra povoado.
			return 0, false, nil
		}
		rec, buf, err := s.segR.ReadRecord(ref.ID, uint32(segment.HeaderSize), s.retBuf)
		s.retBuf = buf
		if err != nil {
			return 0, false, err
		}
		return uint64(rec.RowID), true, nil
	}
	return 0, false, nil
}

// bytesDosSegmentos soma o tamanho dos arquivos de dados — que é o que o
// maxBytes governa, e só ele. Índices e locations.idx ficam de fora porque a
// purga não sabe devolvê-los: um teto que a purga não consegue perseguir
// seria um teto que mente.
func (s *Store) bytesDosSegmentos() (int64, error) {
	refs, err := s.listaSegmentos()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, ref := range refs {
		total += ref.SizeBytes
	}
	return total, nil
}

func (s *Store) listaSegmentos() ([]segment.SegmentRef, error) {
	return segment.ListSegments(filepath.Join(s.opts.Dir, "data"))
}

// EstatisticasDeRetencao é o retrato da tabela histórica em disco.
type EstatisticasDeRetencao struct {
	// DataBytes é a soma dos segmentos; IndexBytes é o resto do diretório.
	DataBytes, IndexBytes int64
	// InstanteMaisAntigo é o _writtenAt da linha mais antiga ainda visível,
	// em millis, ou zero quando a tabela está vazia.
	InstanteMaisAntigo int64
	// PurgadosPorTTL e PurgadosPorTamanho contam desde a subida.
	PurgadosPorTTL, PurgadosPorTamanho int64
	// MarcaDagua é o rowId a partir do qual a tabela é visível.
	MarcaDagua types.RowID
}

// EstatisticasDeRetencao lê o estado sem tomar o lock do writer.
func (s *Store) EstatisticasDeRetencao() EstatisticasDeRetencao {
	e := EstatisticasDeRetencao{
		PurgadosPorTTL:     s.retPorTempo.Load(),
		PurgadosPorTamanho: s.retPorTamanho.Load(),
		MarcaDagua:         s.RowIDBase(),
	}
	if dados, err := s.bytesDosSegmentos(); err == nil {
		e.DataBytes = dados
	}
	e.IndexBytes = blocosDoDiretorio(filepath.Join(s.opts.Dir, "indexes")) +
		blocosDoDiretorio(filepath.Join(s.opts.Dir, "checkpoint"))
	if ms, ok := s.instanteMaisAntigo(); ok {
		e.InstanteMaisAntigo = ms
	}
	return e
}

// instanteMaisAntigo lê o _writtenAt da primeira linha visível.
//
// Uma leitura só, na marca-d'água: a linha mais antiga viva é exatamente a
// que está nela, porque o rowId cresce com o tempo de escrita. Varrer para
// achá-la seria pagar a tabela inteira por um número de painel.
func (s *Store) instanteMaisAntigo() (int64, bool) {
	if !s.EhHistorico() {
		return 0, false
	}
	base := s.RowIDBase()
	if uint64(base) >= s.nextRowID.Load() {
		return 0, false
	}
	s.aposMu.RLock()
	defer s.aposMu.RUnlock()
	packed, ok := s.locs.Get(base)
	if !ok {
		return 0, false
	}
	seg, off, _ := rowindex.Unpack(packed)
	rec, _, err := s.segR.ReadRecord(seg, off, nil)
	if err != nil {
		return 0, false
	}
	return instanteDeEscrita(rec.Payload, s.campoDeEscrita)
}

// blocosDoDiretorio soma os BLOCOS alocados dos arquivos, e não o tamanho
// aparente deles.
//
// A diferença não é cosmética aqui: a purga libera o prefixo morto do
// locations.idx com punch-hole, o que devolve blocos sem mudar o tamanho do
// arquivo. Somar Size() faria o painel mostrar disco que já voltou ao
// dispositivo — que é a mesma classe de erro que o débito 78 registrou na
// compaction, com o sinal trocado.
//
// Erro de leitura devolve o que deu para somar: é número de painel, e um
// diretório que sumiu no meio da varredura não justifica derrubar o /v1/stats.
func blocosDoDiretorio(dir string) int64 {
	entradas, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entradas {
		if e.IsDir() {
			total += blocosDoDiretorio(filepath.Join(dir, e.Name()))
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512
			continue
		}
		total += info.Size()
	}
	return total
}
