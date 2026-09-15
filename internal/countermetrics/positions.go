package countermetrics

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/metricwindow"
)

const (
	// positionMapShift dá o bloco do mapa id → posição: 4.096 ids, 16 KiB.
	positionMapShift     = 12
	positionMapChunkSize = 1 << positionMapShift
	positionMapMask      = positionMapChunkSize - 1
	// positionBytes é o custo de um id no mapa: uma posição u32 atômica.
	positionBytes = 4

	// duplicatedPosition marca, no mapa temporário hash → posição da
	// restauração, um hash achado em mais de uma posição.
	duplicatedPosition = math.MaxUint32
)

type positionChunk [positionMapChunkSize]atomic.Uint32

// positionMap é o mapa id de grupo → posição de UM contador, compartilhado por
// todas as métricas dele. Posição 0 é "sem posição": a posição 0 nunca é
// atribuída. É a única estrutura do amostrador que cresce com o id, a 4 B por
// id já emitido.
//
// Blocos nunca se movem e o diretório é trocado inteiro por atomic.Pointer,
// como os blocos de entradas: o leitor (Metric.Aggregate) carrega o diretório
// e a posição sem lock; só o amostrador escreve.
type positionMap struct {
	chunks atomic.Pointer[[]*positionChunk]
	bytes  atomic.Int64
}

// load devolve a posição do id, ou 0. Seguro de qualquer goroutine.
func (p *positionMap) load(id uint32) uint32 {
	dir := p.chunks.Load()
	if dir == nil {
		return 0
	}
	ci := id >> positionMapShift
	if uint64(ci) >= uint64(len(*dir)) {
		return 0
	}
	return (*dir)[ci][id&positionMapMask].Load()
}

// store grava a posição do id. Só o amostrador chama, depois de ensure.
func (p *positionMap) store(id, position uint32) {
	(*p.chunks.Load())[id>>positionMapShift][id&positionMapMask].Store(position)
}

// ensure garante blocos para os ids [0, limit). Aloca só quando cresce.
func (p *positionMap) ensure(limit uint32) {
	cur := p.chunks.Load()
	need := int((uint64(limit) + positionMapChunkSize - 1) >> positionMapShift)
	if cur != nil && len(*cur) >= need {
		return
	}
	grown := make([]*positionChunk, need)
	if cur != nil {
		copy(grown, *cur)
	}
	for i := range grown {
		if grown[i] == nil {
			grown[i] = new(positionChunk)
		}
	}
	p.chunks.Store(&grown)
	p.bytes.Store(int64(need) * positionMapChunkSize * positionBytes)
}

// counterState é o estado do amostrador para UM contador: o mapa id → posição,
// a lista de posições livres e as métricas, que usam a MESMA posição de um
// grupo em todos os arquivos e em todas as entradas em heap.
//
// # Posição
//
// A posição é o índice denso da entrada de um grupo VIVO: o arquivo
// metricwindow e os blocos em heap são endereçados por ela, e não pelo id do
// grupo. O dicionário do contador nunca reaproveita o id de um grupo
// aposentado — a mesma chave que reaparece ganha id novo —, então estado
// indexado por id cresceria com todo id que o contador já emitiu, aposentados
// incluídos. A posição de um grupo que se aposenta volta para a lista livre, e
// o próximo grupo novo a reaproveita pelo caminho de troca de dono que o
// formato já tem (adopt: identidade nova antes de qualquer balde, balde aberto
// anterior descartado, marca de dono e validFromEpoch separando os baldes).
//
// # Custo
//
//   - Mapa id → posição, um por contador: 4 B por id já emitido (blocos de
//     4.096 ids, 16 KiB). É a única parte que cresce com o id, e fica abaixo do
//     que o próprio contador já guarda por id: o dicionário e o contador
//     mantêm por id a chave e o texto (dois slices, 48 B), o total e os refs
//     (16 B) e o bit de vivo (1 B) — 65 B mais os bytes da chave e do texto
//     (internal/storage/counter, Dict e Counter).
//   - Entradas em heap, por métrica: blocos de 1.024 × entryBytes até a maior
//     posição usada. Uma posição nova só é criada com a lista livre vazia, ou
//     seja, com todas as anteriores ocupadas: a maior posição é o MÁXIMO DE
//     GRUPOS VIVOS SIMULTÂNEOS que o amostrador viu.
//   - Arquivo, por métrica: capacidade potência de 2 com piso 1.024, acima da
//     maior posição; 16 B de identidade mais 60 × 32 B de baldes por posição de
//     capacidade (~1,9 KiB). O teto fileCapacityLimit é o do formato, e o
//     tamanho efetivo vem do número de vivos.
//   - Passada: uma chamada a Reader.Group por id já emitido — o Reader só
//     endereça por id. É CPU proporcional aos ids, não memória.
//   - Restauração: O(capacidade × métricas) leituras de identidade, mais um
//     mapa temporário hash → posição do tamanho da capacidade. Só na abertura.
type counterState struct {
	name    string
	reader  Reader
	metrics []*Metric

	positions positionMap
	// free é a pilha de posições livres; next é a próxima posição nunca usada.
	// Os dois são só do amostrador.
	free []uint32
	next uint32
	// occupied é quantas posições estão atribuídas a grupos vivos.
	occupied atomic.Int64
	// epochs é o scratch da época corrente de cada métrica na passada.
	epochs []uint32
}

// checkLimit recusa um limite de ids acima do espaço de ids do dicionário. O
// contador real não o produz; a recusa impede que um Reader com defeito leve
// o mapa id → posição a alocar gigabytes.
func checkLimit(limit uint32) error {
	if limit > fileCapacityLimit {
		return fmt.Errorf("group id limit %d exceeds the dictionary ceiling of %d ids", limit, maxGroupID)
	}
	return nil
}

// sample é a passada de um contador: lê cada id uma vez e alimenta todas as
// métricas na posição do grupo. Grupo vivo sem posição ganha uma; grupo que
// deixou de estar vivo devolve a sua.
func (c *counterState) sample(now time.Time) error {
	limit := c.reader.Limit()
	if err := checkLimit(limit); err != nil {
		return err
	}
	c.positions.ensure(limit)
	for i, m := range c.metrics {
		c.epochs[i] = m.epochOf(now)
	}
	for id := uint32(1); id < limit; id++ {
		key, _, total, _, live := c.reader.Group(id)
		position := c.positions.load(id)
		if !live {
			if position != 0 {
				c.release(id, position)
			}
			continue
		}
		if position == 0 {
			var err error
			if position, err = c.acquire(id); err != nil {
				return err
			}
		}
		h := keyHash(key)
		for i, m := range c.metrics {
			m.observe(position, id, h, total, c.epochs[i])
		}
	}
	ms := now.UnixMilli()
	for _, m := range c.metrics {
		m.sampledAt.Store(ms)
	}
	return nil
}

// acquire atribui uma posição ao id: a do topo da lista livre, ou a próxima
// nunca usada. A capacidade de todos os arquivos e os blocos de entradas são
// garantidos ANTES de a posição ser tirada da lista: numa falha de infra, a
// lista e o mapa ficam como estavam. A troca de dono da entrada (adopt) vem
// depois, na primeira observação de cada métrica.
func (c *counterState) acquire(id uint32) (uint32, error) {
	fromFree := len(c.free) > 0
	position := c.next
	if fromFree {
		position = c.free[len(c.free)-1]
	} else if position >= fileCapacityLimit {
		return 0, fmt.Errorf("%w: position %d", metricwindow.ErrCapacityExceeded, position)
	}
	for _, m := range c.metrics {
		err := m.file.EnsureCapacity(position)
		m.diskBytes.Store(m.file.SizeBytes())
		if err != nil {
			return 0, fmt.Errorf("metric %q: %w", m.name, err)
		}
		m.ensureEntries(position + 1)
	}
	if fromFree {
		c.free = c.free[:len(c.free)-1]
	} else {
		c.next++
	}
	c.positions.store(id, position)
	c.occupied.Add(1)
	return position, nil
}

// release devolve a posição de um id que deixou de estar vivo: cada métrica
// zera o publicado e a identidade da entrada, o mapa do id vira 0 e a posição
// vai para a lista livre. O publicado zera ANTES do mapa: um leitor que já
// carregou a posição vê dono 0 e responde sem valor.
func (c *counterState) release(id, position uint32) {
	for _, m := range c.metrics {
		m.retire(position)
	}
	c.positions.store(id, 0)
	c.free = append(c.free, position)
	c.occupied.Add(-1)
}

// restore reconstrói, na abertura, o mapa id → posição e a lista livre a partir
// da coluna de identidade dos arquivos — o mapa não vai a disco — e publica o
// agregado dos grupos vivos restaurados. Devolve quantas entradas saíram com
// amostra, somado sobre as métricas.
//
// A identidade de uma posição é o hash que as métricas com identidade não
// vazia ali concordam; métricas divergentes invalidam a posição (lixo de crash
// ou arquivo de outro histórico). Um hash em mais de uma posição também é
// inválido, e as duas posições são liberadas. Posição com hash que nenhum
// grupo vivo reclama é de grupo aposentado enquanto o processo estava parado:
// vai para a lista livre, e em modo escrita a identidade é zerada no disco —
// sem isso, a mesma chave adotada noutra posição mais tarde deixaria o hash
// duplicado para a restauração seguinte, que descartaria os dois.
//
// Entre aberturas, a identidade de uma posição é o hash da chave, e não o id
// do grupo — o mesmo princípio do alarme, que casa pela chave canônica; o id
// nem vai a disco. A consequência, aceita: a mesma chave recriada com id novo
// reclama a posição antiga e herda a janela quando o amostrador NÃO observou a
// aposentadoria antes da última gravação E a chave recriada já está viva na
// abertura seguinte — processo parado; laço parado por erro de infra enquanto
// a ingestão seguia; aposentadoria e recriação dentro do último intervalo
// antes do Stop. Não há herança quando a aposentadoria é observada (por uma
// passada, que libera a posição) nem quando a chave só volta DEPOIS da
// abertura: nesse caso é esta restauração que não acha dono vivo, libera a
// posição e zera a identidade, e a janela da chave recriada recomeça do zero.
func (c *counterState) restore(now time.Time, readOnly bool) (int64, error) {
	capacity := uint32(0)
	for _, m := range c.metrics {
		if m.file != nil {
			capacity = max(capacity, m.file.Capacity())
		}
	}
	if capacity == 0 {
		return 0, nil
	}
	hashes := make([]uint64, capacity)
	highest := uint32(0)
	for position := uint32(1); position < capacity; position++ {
		var h uint64
		conflict := false
		for _, m := range c.metrics {
			if m.file == nil || position >= m.file.Capacity() {
				continue
			}
			ih := m.file.Identity(position).KeyHash
			if ih == 0 {
				continue
			}
			highest = position
			switch {
			case h == 0:
				h = ih
			case h != ih:
				conflict = true
			}
		}
		if !conflict {
			hashes[position] = h
		}
	}
	byHash := make(map[uint64]uint32, highest)
	for position := uint32(1); position <= highest; position++ {
		h := hashes[position]
		if h == 0 {
			continue
		}
		if _, seen := byHash[h]; seen {
			byHash[h] = duplicatedPosition
			continue
		}
		byHash[h] = position
	}

	if !readOnly {
		for _, m := range c.metrics {
			err := m.file.EnsureCapacity(highest)
			m.diskBytes.Store(m.file.SizeBytes())
			if err != nil {
				return 0, fmt.Errorf("metric %q: %w", m.name, err)
			}
		}
	}
	for _, m := range c.metrics {
		if m.file != nil {
			m.ensureEntries(highest + 1)
		}
	}

	limit := c.reader.Limit()
	if err := checkLimit(limit); err != nil {
		return 0, err
	}
	c.positions.ensure(limit)
	claimed := make([]bool, highest+1)
	restored := int64(0)
	for id := uint32(1); id < limit; id++ {
		key, _, _, _, live := c.reader.Group(id)
		if !live {
			continue
		}
		h := keyHash(key)
		position, ok := byHash[h]
		if !ok || position == duplicatedPosition || claimed[position] {
			continue
		}
		claimed[position] = true
		c.positions.store(id, position)
		c.occupied.Add(1)
		for _, m := range c.metrics {
			restored += m.restoreEntry(position, id, h, now)
		}
	}

	// Empilhadas da maior para a menor: a menor posição livre sai primeiro.
	for position := highest; position >= 1; position-- {
		if claimed[position] {
			continue
		}
		c.free = append(c.free, position)
		if readOnly {
			continue
		}
		for _, m := range c.metrics {
			if m.file.Identity(position).KeyHash != 0 {
				m.file.SetIdentity(position, metricwindow.Identity{})
			}
		}
	}
	c.next = highest + 1
	return restored, nil
}
