package storage

import (
	"sync"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/storage/versoes"
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
	// porEpoca conta os snapshots ABERTOS em cada época. Uma época só sai do
	// mapa quando o último leitor dela fecha.
	porEpoca map[uint64]int
	// menor espelha a menor chave viva de porEpoca, para que o caminho quente
	// do writer a leia sem tomar o mutex. SemLeitor quando o mapa está vazio.
	menor atomic.Uint64
}

func novoRegistroDeSnapshots() *registroDeSnapshots {
	r := &registroDeSnapshots{porEpoca: make(map[uint64]int)}
	r.menor.Store(versoes.SemLeitor)
	return r
}

// abre registra mais um leitor na época e devolve a menor época viva.
func (r *registroDeSnapshots) abre(epoca uint64) {
	r.mu.Lock()
	r.porEpoca[epoca]++
	r.recalculaLocked()
	r.mu.Unlock()
}

// fecha retira um leitor da época. Fechar uma época sem leitor é no-op: o
// snapshot é um VALOR, e nada impede o chamador de copiá-lo — o que não pode é
// um Fecha a mais derrubar a contagem de quem ainda está lendo.
func (r *registroDeSnapshots) fecha(epoca uint64) {
	r.mu.Lock()
	if n := r.porEpoca[epoca]; n > 1 {
		r.porEpoca[epoca] = n - 1
	} else if n == 1 {
		delete(r.porEpoca, epoca)
	}
	r.recalculaLocked()
	r.mu.Unlock()
}

func (r *registroDeSnapshots) recalculaLocked() {
	menor := versoes.SemLeitor
	for e := range r.porEpoca {
		if e < menor {
			menor = e
		}
	}
	r.menor.Store(menor)
}

// menorEpocaViva devolve versoes.SemLeitor quando nenhuma consulta está aberta.
func (r *registroDeSnapshots) menorEpocaViva() uint64 { return r.menor.Load() }

// AbreSnapshot fixa o universo da consulta: a versão, o relógio, o teto e o
// piso.
//
// O par com FechaSnapshot é OBRIGATÓRIO, e é isso que muda em relação a um
// snapshot puramente de valor: enquanto ele está aberto, o writer guarda as
// versões anteriores das linhas que atualizar, e a compaction não aposenta os
// segmentos onde essas versões moram. Um snapshot que nunca fecha faz o
// journal crescer pela vida do processo.
func (s *Store) AbreSnapshot() types.Snapshot {
	snap := types.Snapshot{
		Epoca:       s.epocaPub.Load(),
		Instante:    s.agora(),
		LimiteRowID: types.RowID(s.nextRowID.Load()),
		BaseRowID:   types.RowID(s.rowIDBase.Load()),
	}
	s.snaps.abre(snap.Epoca)
	return snap
}

// FechaSnapshot devolve o que o snapshot segurava e poda o que nenhum leitor
// mais alcança.
//
// A poda acontece AQUI, e não num laço de fundo, porque o instante em que ela
// pode acontecer é exatamente este: nenhuma outra coisa muda a menor época
// viva. Um coletor periódico só acrescentaria latência entre a última consulta
// fechar e a memória voltar.
func (s *Store) FechaSnapshot(snap types.Snapshot) {
	s.snaps.fecha(snap.Epoca)
	s.podaVersoes()
}

// podaVersoes descarta do journal o que nenhum leitor mais alcança.
func (s *Store) podaVersoes() {
	s.vers.Poda(s.snaps.menorEpocaViva())
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
		Epoca:       s.epocaPub.Load(),
		Instante:    s.agora(),
		LimiteRowID: types.RowID(s.nextRowID.Load()),
		BaseRowID:   types.RowID(s.rowIDBase.Load()),
	}
}

// VersoesRetidas é quantas versões anteriores o journal guarda por causa de
// consultas abertas, e BytesRetidos o heap que elas ocupam.
//
// São o observável do isolamento — o que o /v1/stats publica e o que um teste
// de ponta a ponta usa para provar que uma consulta devolveu o que segurava.
// Sem eles, "a consulta fechou" e "a memória voltou" seriam a mesma afirmação
// sem oráculo.
func (s *Store) VersoesRetidas() int { return s.vers.Entradas() }

// BytesRetidos é o heap estimado do journal de versões. Ver VersoesRetidas.
func (s *Store) BytesRetidos() int64 { return s.vers.Bytes() }
