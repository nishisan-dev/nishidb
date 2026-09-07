package storage

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/versions"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// registroDeSnapshots guarda QUANTAS consultas vivem em cada época.
//
// É contagem por época, e não um handle por consulta, porque a única pergunta
// que o writer faz é "qual a época mais antiga que ainda tem leitor?" — e ela
// governa as três decisões que dependem de leitor vivo: se vale guardar a
// versão anterior de uma linha, o que pode ser podado do journal, e até onde a
// compaction pode aplicar a expiração oportunista.
//
// snapMu é FOLHA na ordem de locks, como o selMu: nada tomado sob ele toma wmu
// ou aposMu. O writer entra aqui com o wmu na mão (é ele quem publica a época),
// e o leitor entra sem lock nenhum — inverter isso seria wmu→snapMu de um lado
// e snapMu→wmu do outro.
type registroDeSnapshots struct {
	mu sync.Mutex
	// porEpoca conta os snapshots ABERTOS em cada época e guarda o menor
	// instante entre eles. Uma época só sai do mapa quando o último leitor
	// dela fecha.
	//
	// O instante vem junto porque época e relógio respondem a perguntas
	// diferentes: a época diz QUAL VERSÃO um leitor vê, e o instante diz
	// QUANDO ele acha que é — e é o segundo que a compaction consulta antes de
	// aplicar a expiração oportunista. Dois snapshots da mesma época podem ter
	// instantes distintos, porque o relógio anda sem que ninguém publique.
	porEpoca map[uint64]epocaViva
	// menor espelha a menor chave viva de porEpoca, para que o caminho quente
	// do writer a leia sem tomar o mutex. NoReader quando o mapa está vazio.
	menor atomic.Uint64
	// menorInstante espelha o instante do snapshot mais antigo, em epoch
	// millis, e vale ZERO quando não há leitor. Zero é um sentinela seguro
	// aqui — nenhum dataset ingere em 1970 —, e é o que faz a compaction
	// voltar ao relógio corrente sozinha quando a última consulta fecha.
	menorInstante atomic.Int64
	// menorBase espelha o piso do snapshot mais antigo — o rowId a partir do
	// qual ele ainda enxerga linhas. A purga do histórico não pode passar
	// dele: ela não só move a marca-d'água, ela APAGA os segmentos cobertos, e
	// um leitor com o piso congelado sobre um segmento removido passaria de
	// "não vê a linha" para "erra ao lê-la". SemPiso quando não há leitor.
	menorBase atomic.Uint64
	// epocaMinimaValida é a fronteira da invalidação: snapshot com época ABAIXO
	// dela teve as suas versões descartadas pelo teto de memória e não pode
	// mais ser servido. Só avança, e só sob a pressão do teto.
	epocaMinimaValida atomic.Uint64
}

// SemPiso é o valor de menorBase sem consulta aberta: nada a segurar, e a
// retenção avança até onde o prazo mandar.
const SemPiso = uint64(math.MaxUint64)

// epocaViva é a contagem de leitores de uma época, o instante do mais antigo e
// o menor piso entre eles.
type epocaViva struct {
	n        int
	instante time.Time
	base     types.RowID
}

func novoRegistroDeSnapshots() *registroDeSnapshots {
	r := &registroDeSnapshots{porEpoca: make(map[uint64]epocaViva)}
	r.menor.Store(versions.NoReader)
	r.menorBase.Store(SemPiso)
	return r
}

// abre registra mais um leitor na época e devolve a menor época viva.
func (r *registroDeSnapshots) abre(epoca uint64, instante time.Time, base types.RowID) {
	r.mu.Lock()
	v := r.porEpoca[epoca]
	if v.n == 0 || instante.Before(v.instante) {
		v.instante = instante
	}
	if v.n == 0 || base < v.base {
		v.base = base
	}
	v.n++
	r.porEpoca[epoca] = v
	r.recalculaLocked()
	r.mu.Unlock()
}

// fecha retira um leitor da época. Fechar uma época sem leitor é no-op: o
// snapshot é um VALOR, e nada impede o chamador de copiá-lo — o que não pode é
// um Fecha a mais derrubar a contagem de quem ainda está lendo.
func (r *registroDeSnapshots) fecha(epoca uint64) {
	r.mu.Lock()
	if v := r.porEpoca[epoca]; v.n > 1 {
		v.n--
		r.porEpoca[epoca] = v
	} else if v.n == 1 {
		delete(r.porEpoca, epoca)
	}
	r.recalculaLocked()
	r.mu.Unlock()
}

func (r *registroDeSnapshots) recalculaLocked() {
	menor := versions.NoReader
	menorBase := SemPiso
	var menorMs int64
	for e, v := range r.porEpoca {
		if e < menor {
			menor = e
		}
		if ms := v.instante.UnixMilli(); menorMs == 0 || ms < menorMs {
			menorMs = ms
		}
		if uint64(v.base) < menorBase {
			menorBase = uint64(v.base)
		}
	}
	r.menor.Store(menor)
	r.menorInstante.Store(menorMs)
	r.menorBase.Store(menorBase)
}

// invalidaAte marca como não-servíveis todos os snapshots de época abaixo de
// `epoca`. Monotônico: a fronteira nunca desce.
func (r *registroDeSnapshots) invalidaAte(epoca uint64) {
	for {
		atual := r.epocaMinimaValida.Load()
		if epoca <= atual || r.epocaMinimaValida.CompareAndSwap(atual, epoca) {
			return
		}
	}
}

// esquece retira a época inteira da contagem de vivos — todos os leitores dela
// de uma vez. É o que a invalidação por teto faz: os snapshots continuam
// existindo do lado do dono, mas deixam de segurar versão nenhuma.
func (r *registroDeSnapshots) esquece(epoca uint64) {
	r.mu.Lock()
	delete(r.porEpoca, epoca)
	r.recalculaLocked()
	r.mu.Unlock()
}

// valido responde se um snapshot ainda pode ser servido.
func (r *registroDeSnapshots) valido(epoca uint64) bool {
	return epoca >= r.epocaMinimaValida.Load()
}

// instanteMaisAntigoVivo devolve o relógio do snapshot mais antigo aberto, e
// ok=false quando não há consulta nenhuma.
func (r *registroDeSnapshots) instanteMaisAntigoVivo() (time.Time, bool) {
	ms := r.menorInstante.Load()
	if ms == 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// menorEpocaViva devolve versions.NoReader quando nenhuma consulta está aberta.
func (r *registroDeSnapshots) menorEpocaViva() uint64 { return r.menor.Load() }

// OpenSnapshot fixa o universo da consulta: a versão, o relógio, o teto e o
// piso.
//
// O par com FechaSnapshot é OBRIGATÓRIO, e é isso que muda em relação a um
// snapshot puramente de valor: enquanto ele está aberto, o writer guarda as
// versões anteriores das linhas que atualizar, e a compaction não aposenta os
// segmentos onde essas versões moram. Um snapshot que nunca fecha faz o
// journal crescer pela vida do processo.
func (s *Store) OpenSnapshot() types.Snapshot {
	snap := types.Snapshot{
		Epoch:      s.epocaPub.Load(),
		Instant:    s.agora(),
		LimitRowID: types.RowID(s.nextRowID.Load()),
		BaseRowID:  types.RowID(s.rowIDBase.Load()),
	}
	s.snaps.abre(snap.Epoch, snap.Instant, snap.BaseRowID)
	return snap
}

// CloseSnapshot devolve o que o snapshot segurava e poda o que nenhum leitor
// mais alcança.
//
// A poda acontece AQUI, e não num laço de fundo, porque o instante em que ela
// pode acontecer é exatamente este: nenhuma outra coisa muda a menor época
// viva. Um coletor periódico só acrescentaria latência entre a última consulta
// fechar e a memória voltar.
func (s *Store) CloseSnapshot(snap types.Snapshot) {
	s.snaps.fecha(snap.Epoch)
	s.podaVersoes()
}

// podaVersoes descarta do journal o que nenhum leitor mais alcança.
func (s *Store) podaVersoes() {
	s.vers.Prune(s.snaps.menorEpocaViva())
}

// presente é o snapshot do WRITER: o instante e a época de agora, sem
// registrar leitor nenhum.
//
// Existe porque o writer atravessa o mesmo lookup que a leitura, e precisa
// enxergar o estado CORRENTE — o stale check do §58 comparando sequences
// contra uma versão congelada recusaria eventos legítimos. Não registra
// porque não há o que reter: ele não vai voltar para ler o passado.
func (s *Store) presente() types.Snapshot {
	return types.Snapshot{
		Epoch:      s.epocaPub.Load(),
		Instant:    s.agora(),
		LimitRowID: types.RowID(s.nextRowID.Load()),
		BaseRowID:  types.RowID(s.rowIDBase.Load()),
	}
}

// VersoesRetidas é quantas versões anteriores o journal guarda por causa de
// consultas abertas, e BytesRetidos o heap que elas ocupam.
//
// São o observável do isolamento — o que o /v1/stats publica e o que um teste
// de ponta a ponta usa para provar que uma consulta devolveu o que segurava.
// Sem eles, "a consulta fechou" e "a memória voltou" seriam a mesma afirmação
// sem oráculo.
func (s *Store) VersoesRetidas() int { return s.vers.Entries() }

// BytesRetidos é o heap estimado do journal de versões. Ver VersoesRetidas.
func (s *Store) BytesRetidos() int64 { return s.vers.Bytes() }

// relogioDaExpiracaoOportunista é o instante contra o qual a compaction decide
// se troca o payload de uma linha vencida por uma lápide.
//
// É o do snapshot mais ANTIGO vivo, e não o corrente, por uma razão que o
// copy-forward não pode resolver de outro jeito: ele NÃO versiona (ver o campo
// `versiona` do recordApply), então a versão que ele apaga não vai para o
// journal e nenhuma consulta a alcança depois. Lapidar com o relógio de agora
// apagaria o payload que uma consulta aberta ainda tem o direito de ver, e o
// relógio congelado que a preservava não teria como devolvê-lo.
//
// Sem consulta nenhuma, volta a ser o relógio corrente — e a compaction
// recupera sozinha o alcance total assim que o último leitor fecha. O que se
// paga por uma consulta longa é a expiração oportunista ficar mais conservadora
// enquanto ela dura; o coletor continua pegando o resto.
func (s *Store) relogioDaExpiracaoOportunista() time.Time {
	if t, ok := s.snaps.instanteMaisAntigoVivo(); ok {
		return t
	}
	return s.agora()
}

// pisoDaRetencao é o teto até onde a purga do histórico pode avançar a
// marca-d'água: o piso do snapshot mais antigo vivo, ou SemPiso quando não há
// consulta nenhuma.
func (s *Store) pisoDaRetencao() uint64 { return s.snaps.menorBase.Load() }

// snapshotMaxBytesPadrao é o teto de heap do journal de versões quando o
// dataset não declara um.
//
// 64 MiB são cerca de 2,7 milhões de versões retidas — folga larga para o
// relatório de minutos que a consulta longa existe para servir, e pequeno o
// bastante para que uma consulta esquecida seja RECUSADA em vez de competir
// com o 1 GiB de RSS que o produto inteiro se propõe a caber (§97). É o mesmo
// critério do sortMemoryPadrao do executor.
const snapshotMaxBytesPadrao = 64 << 20

// invalidaOMaisAntigoSeEstourou devolve memória quando o journal passa do teto,
// invalidando a consulta mais antiga.
//
// Roda no caminho do writer, logo depois de registrar uma versão, e é o único
// lugar que avança a fronteira de invalidação. A escolha da VÍTIMA é a consulta
// mais antiga por dois motivos que apontam para o mesmo lado: ela é a que mais
// versões segura (o journal retém tudo publicado desde que ela abriu) e a que
// há mais tempo não entrega resultado.
//
// Invalidar não é o mesmo que fechar: o snapshot continua registrado, e é o
// Close do dono que o retira. Fechá-lo aqui faria o Fecha do dono derrubar a
// contagem de um leitor que já não existe.
func (s *Store) invalidaOMaisAntigoSeEstourou() {
	teto := s.opts.SnapshotMaxBytes
	if teto <= 0 {
		teto = snapshotMaxBytesPadrao
	}
	for s.vers.Bytes() > teto {
		menor := s.snaps.menorEpocaViva()
		if menor == versions.NoReader {
			// Journal acima do teto sem leitor nenhum: não há consulta a
			// invalidar, e a poda comum resolve. Acontece na janela entre o
			// último Fecha e a poda dele.
			s.vers.Prune(versions.NoReader)
			return
		}
		// A fronteira sobe para DEPOIS da vítima: tudo que ela via, e só ela,
		// deixa de ser servível.
		s.snaps.invalidaAte(menor + 1)
		s.snaps.esquece(menor)
		s.snapsExpirados.Add(1)
		if antes := s.vers.Entries(); s.vers.Prune(s.snaps.menorEpocaViva()) == 0 && antes == s.vers.Entries() {
			// Nada saiu: insistir seria laço infinito. Acontece quando o
			// journal está cheio de versões que o PRÓPRIO leitor seguinte
			// ainda alcança, e aí o teto já não tem o que devolver.
			return
		}
	}
}

// IdadeDoSnapshotMaisAntigo é há quanto tempo a consulta mais antiga está
// segurando o passado deste dataset, e zero quando não há nenhuma.
//
// É medida contra o relógio DO STORE, e não contra time.Now, porque é o mesmo
// relógio que decidiu o instante do snapshot — misturar os dois faria a série
// mentir em qualquer teste com relógio controlado, e mentir por um valor que
// parece plausível.
func (s *Store) IdadeDoSnapshotMaisAntigo() time.Duration {
	t, ok := s.snaps.instanteMaisAntigoVivo()
	if !ok {
		return 0
	}
	if d := s.agora().Sub(t); d > 0 {
		return d
	}
	return 0
}

// SegmentosPinados é quantos segmentos a compaction não pode aposentar porque
// guardam versões que uma consulta ainda alcança.
func (s *Store) SegmentosPinados() int { return s.vers.PinnedSegments() }

// SnapshotsExpirados conta as consultas invalidadas pelo teto de memória do
// journal desde a subida. Enquanto for zero, o teto está sobrando.
func (s *Store) SnapshotsExpirados() uint64 { return s.snapsExpirados.Load() }
