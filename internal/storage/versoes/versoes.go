// Package versoes guarda, enquanto uma consulta está aberta, as locations que
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
package versoes

import (
	"math"
	"sync"

	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// SemLeitor é a menor época viva de um dataset sem consulta aberta.
//
// É o MAIOR uint64, e não zero, porque o valor entra em comparações do tipo
// "esta versão ainda interessa a alguém?": com zero, toda versão pareceria
// interessar a um leitor inexistente e nada seria podado nunca.
const SemLeitor = uint64(math.MaxUint64)

// bytesPorVersao é o custo de heap de uma entrada, usado pelo teto de memória.
// Duas palavras da própria entrada mais a amortização do slice e da chave do
// mapa — é estimativa declarada, e não medida: o que o teto precisa é de um
// número estável e ligeiramente pessimista.
const bytesPorVersao = 24

// versao é uma location anterior e a época em que ela deixou de ser a corrente.
type versao struct {
	// epoca é a da PUBLICAÇÃO que a aposentou. Um leitor de época E precisa
	// desta entrada quando E < epoca.
	epoca  uint64
	packed uint64
}

// Cadeias mapeia cada linha às versões dela que ainda interessam a algum
// leitor, em ordem CRESCENTE de época.
//
// mu é FOLHA na ordem de locks e nunca fica segurado enquanto outro lock é
// tomado: o leitor consulta, solta, e só então toma o aposMu para ler os
// bytes. Isso é seguro porque o pin pertence ao SNAPSHOT vivo, não à entrada
// consultada — enquanto a consulta existe, o segmento não pode ser aposentado.
type Cadeias struct {
	mu     sync.RWMutex
	porRow map[types.RowID][]versao
	// pin conta quantas versões retidas moram em cada segmento. É por isso
	// que a compaction sabe qual segmento não pode aposentar sem apagar o
	// passado de alguém.
	pin map[uint32]int
	n   int
}

// Novas devolve um journal vazio.
func Novas() *Cadeias {
	return &Cadeias{porRow: make(map[types.RowID][]versao), pin: make(map[uint32]int)}
}

// Registra guarda a location que a linha tinha ANTES da publicação da época
// dada. Exclusivo do writer (roda sob o wmu).
//
// As entradas de uma linha nascem em ordem crescente de época por construção —
// a época é monotônica e o writer é único —, e é essa ordem que a busca
// binária de Em consome.
func (c *Cadeias) Registra(row types.RowID, packedAnterior, epoca uint64) {
	if packedAnterior == 0 {
		return // linha sem location anterior: não há passado a guardar
	}
	seg, _, _ := rowindex.Unpack(packedAnterior)
	c.mu.Lock()
	c.porRow[row] = append(c.porRow[row], versao{epoca: epoca, packed: packedAnterior})
	c.pin[seg]++
	c.n++
	c.mu.Unlock()
}

// Em devolve a location que a linha tinha na época dada, e achou=false quando
// a location CORRENTE já é a certa para esse leitor.
//
// A resposta é a entrada de MENOR época maior que a do leitor: ela é a que
// guarda o estado vigente para ele. Devolver a maior — ou a primeira que
// aparecer — erra em toda linha atualizada duas vezes durante a mesma consulta,
// que é o caso que nenhum teste de uma atualização só distingue.
func (c *Cadeias) Em(row types.RowID, epoca uint64) (uint64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	vs := c.porRow[row]
	for _, v := range vs {
		if v.epoca > epoca {
			return v.packed, true
		}
	}
	return 0, false
}

// Poda descarta o que nenhum leitor alcança e devolve quantas entradas saíram.
//
// Uma entrada de época P interessa a todo leitor com época < P; com a menor
// época viva em E, tudo com P <= E é lixo. SemLeitor esvazia o journal.
func (c *Cadeias) Poda(menorEpocaViva uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	saíram := 0
	for row, vs := range c.porRow {
		corte := 0
		for corte < len(vs) && vs[corte].epoca <= menorEpocaViva {
			seg, _, _ := rowindex.Unpack(vs[corte].packed)
			if c.pin[seg] > 1 {
				c.pin[seg]--
			} else {
				delete(c.pin, seg)
			}
			corte++
		}
		if corte == 0 {
			continue
		}
		saíram += corte
		if corte == len(vs) {
			delete(c.porRow, row)
			continue
		}
		// A cópia é para o slice não continuar segurando o array antigo
		// inteiro: um append que só avança a janela deixaria o prefixo podado
		// vivo em heap, que é justamente a memória que a poda existe para
		// devolver.
		resto := make([]versao, len(vs)-corte)
		copy(resto, vs[corte:])
		c.porRow[row] = resto
	}
	c.n -= saíram
	return saíram
}

// Pinado responde se o segmento guarda alguma versão que um leitor ainda
// alcança. A compaction não pode aposentá-lo enquanto isso for verdade.
func (c *Cadeias) Pinado(seg uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pin[seg] > 0
}

// Bytes é o consumo de heap estimado do journal, para o teto de memória.
func (c *Cadeias) Bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(c.n) * bytesPorVersao
}

// Entradas é quantas versões estão retidas — o observável dos testes e da
// métrica.
func (c *Cadeias) Entradas() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.n
}

// SegmentosPinados é quantos segmentos guardam versão retida — o que a
// compaction não pode aposentar agora.
func (c *Cadeias) SegmentosPinados() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pin)
}
