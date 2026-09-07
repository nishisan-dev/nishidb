// Package versions guarda, enquanto uma consulta está aberta, as locations que
// as linhas TINHAM quando ela começou.
//
// É o que dá isolamento de leitura ao NishiDB, e ele custa menos do que o
// nome sugere porque não copia dado nenhum: os segmentos são append-only, então
// a versão anterior de uma linha continua fisicamente no segmento até a
// compaction aposentá-lo. O que falta para alcançá-la é só o ENDEREÇO, que o
// writer tem na mão no instante do publish — 24 bytes por atualização, contra
// o record inteiro que um journal de before-images copiaria.
//
// O preço que sobra é de tempo, não de espaço: enquanto a versão está retida,
// o segmento onde ela mora não pode ser aposentado. É o que o pin registra.
package versions

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// NoReader é a menor época viva de um dataset sem consulta aberta.
//
// É o MAIOR uint64, e não zero, porque o valor entra em comparações do tipo
// "esta versão ainda interessa a alguém?": com zero, toda versão pareceria
// interessar a um leitor inexistente e nada seria podado nunca.
const NoReader = uint64(math.MaxUint64)

// bytesPerVersion é o custo de heap de uma entrada, usado pelo teto de memória.
// Duas palavras da própria entrada mais a amortização do slice e da chave do
// mapa — é estimativa declarada, e não medida: o que o teto precisa é de um
// número estável e ligeiramente pessimista.
const bytesPerVersion = 24

// version é uma location anterior e a época em que ela deixou de ser a
// corrente.
type version struct {
	// epoch é a época da PUBLICAÇÃO que a aposentou. Um leitor de época E
	// precisa desta entrada quando E < epoch.
	epoch  uint64
	packed uint64
}

// Journal mapeia cada linha às versões dela que ainda interessam a algum
// leitor, em ordem CRESCENTE de época.
//
// mu é FOLHA na ordem de locks e nunca fica segurado enquanto outro lock é
// tomado: o leitor consulta, solta, e só então toma o aposMu para ler os
// bytes. Isso é seguro porque o pin pertence ao SNAPSHOT vivo, não à entrada
// consultada — enquanto a consulta existe, o segmento não pode ser aposentado.
type Journal struct {
	mu    sync.RWMutex
	byRow map[types.RowID][]version
	// pin conta quantas versões retidas moram em cada segmento. É por isso
	// que a compaction sabe qual segmento não pode aposentar sem apagar o
	// passado de alguém.
	pin map[uint32]int
	n   int
	// liveEntries espelha n para leitura SEM LOCK, e existe por causa do
	// caminho quente: At é chamado a CADA linha varrida, e o caso dominante
	// é o journal vazio — nenhuma consulta concorrente com escrita. Sem o
	// atômico, toda linha de todo full scan pagaria um RLock e um lookup de
	// mapa para descobrir que não há nada a descobrir.
	//
	// Ele pode estar momentaneamente atrasado em relação a n, e isso é seguro
	// na única direção que importa: ele é escrito DENTRO do mesmo mu que
	// protege n, então um leitor que veja zero está vendo um estado em que o
	// journal esteve vazio — e nesse estado a location corrente é a resposta
	// certa para qualquer época.
	liveEntries atomic.Int64
}

// New devolve um journal vazio.
func New() *Journal {
	return &Journal{byRow: make(map[types.RowID][]version), pin: make(map[uint32]int)}
}

// Retain guarda a location que a linha tinha ANTES da publicação da época
// dada. Exclusivo do writer (roda sob o wmu).
//
// As entradas de uma linha nascem em ordem crescente de época por construção —
// a época é monotônica e o writer é único —, e é essa ordem que a busca
// binária de At consome.
func (c *Journal) Retain(row types.RowID, prevPacked, epoch uint64) {
	if prevPacked == 0 {
		return // linha sem location anterior: não há passado a guardar
	}
	seg, _, _ := rowindex.Unpack(prevPacked)
	c.mu.Lock()
	c.byRow[row] = append(c.byRow[row], version{epoch: epoch, packed: prevPacked})
	c.pin[seg]++
	c.n++
	c.liveEntries.Store(int64(c.n))
	c.mu.Unlock()
}

// IsEmpty responde se não há versão retida nenhuma — o estado de todo dataset
// sem consulta concorrente com escrita.
//
// É método próprio, e minúsculo de propósito: ele fica no caminho por LINHA de
// todo full scan, e um corpo que o compilador não inline põe uma chamada de
// função onde bastava um load.
func (c *Journal) IsEmpty() bool { return c.liveEntries.Load() == 0 }

// At devolve a location que a linha tinha na época dada, e achou=false quando
// a location CORRENTE já é a certa para esse leitor.
//
// A resposta é a entrada de MENOR época maior que a do leitor: ela é a que
// guarda o estado vigente para ele. Devolver a maior — ou a primeira que
// aparecer — erra em toda linha atualizada duas vezes durante a mesma consulta,
// que é o caso que nenhum teste de uma atualização só distingue.
func (c *Journal) At(row types.RowID, epoch uint64) (uint64, bool) {
	// Saída sem lock para o caso dominante: journal vazio, que é o estado de
	// todo dataset sem consulta concorrente com escrita. Ver o campo
	// `liveEntries`.
	if c.liveEntries.Load() == 0 {
		return 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	vs := c.byRow[row]
	for _, v := range vs {
		if v.epoch > epoch {
			return v.packed, true
		}
	}
	return 0, false
}

// Prune descarta o que nenhum leitor alcança e devolve quantas entradas saíram.
//
// Uma entrada de época P interessa a todo leitor com época < P; com a menor
// época viva em E, tudo com P <= E é lixo. NoReader esvazia o journal.
func (c *Journal) Prune(minLiveEpoch uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	pruned := 0
	for row, vs := range c.byRow {
		cut := 0
		for cut < len(vs) && vs[cut].epoch <= minLiveEpoch {
			seg, _, _ := rowindex.Unpack(vs[cut].packed)
			if c.pin[seg] > 1 {
				c.pin[seg]--
			} else {
				delete(c.pin, seg)
			}
			cut++
		}
		if cut == 0 {
			continue
		}
		pruned += cut
		if cut == len(vs) {
			delete(c.byRow, row)
			continue
		}
		// A cópia é para o slice não continuar segurando o array antigo
		// inteiro: um append que só avança a janela deixaria o prefixo podado
		// vivo em heap, que é justamente a memória que a poda existe para
		// devolver.
		rest := make([]version, len(vs)-cut)
		copy(rest, vs[cut:])
		c.byRow[row] = rest
	}
	c.n -= pruned
	c.liveEntries.Store(int64(c.n))
	return pruned
}

// Pinned responde se o segmento guarda alguma versão que um leitor ainda
// alcança. A compaction não pode aposentá-lo enquanto isso for verdade.
func (c *Journal) Pinned(seg uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pin[seg] > 0
}

// Bytes é o consumo de heap estimado do journal, para o teto de memória.
func (c *Journal) Bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(c.n) * bytesPerVersion
}

// Entries é quantas versões estão retidas — o observável dos testes e da
// métrica.
func (c *Journal) Entries() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.n
}

// PinnedSegments é quantos segmentos guardam versão retida — o que a
// compaction não pode aposentar agora.
func (c *Journal) PinnedSegments() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pin)
}
