// Package budget é o teto de memória de consulta do §80: o `memoryTarget`
// do processo e o `query.maxBufferPerQuery` de cada consulta.
//
// As duas chaves eram parseadas e nunca lidas — metade do débito 15 —, e a
// consequência não é só uma chave sem efeito: quem escreve `memoryTarget: 1GiB`
// dimensiona a MÁQUINA por ela. O que faltava para implementá-las não era
// aritmética, era um lugar onde a reserva pudesse acontecer sem custo, no
// caminho quente de quem aloca.
//
// É por isso que aqui não há mutex nem alocação: dois `atomic.Int64`, um por
// nível. O consumidor real é o StreamWriter (débito 28), que é chamado UMA VEZ
// POR ROW e cujo gate de alocação exige zero allocs em regime — qualquer coisa
// que alocasse neste caminho reprovaria o gate antes de proteger memória
// nenhuma.
//
// A hierarquia é de dois níveis e cada um recusa uma coisa diferente. O Global
// impede que N consultas SOMADAS estourem a máquina, e recusa na ADMISSÃO — ou a
// consulta reserva o orçamento dela inteiro antes do primeiro byte, ou recebe um
// 503 com Retry-After. O PorQuery impede que UMA consulta patológica consuma
// sozinha o que era de todas, e recusa POR LINHA, dentro do StreamWriter, contra
// um teto que já está reservado.
package budget

import "sync/atomic"

// Global é o orçamento do PROCESSO (runtime.memoryTarget).
//
// É por processo, e não por dataset, pela mesma razão que o §82 dá ao
// maxConcurrent: todos os datasets de uma instância dividem o mesmo orçamento de
// memória, e um teto por dataset multiplicaria o pior caso pelo número deles.
type Global struct {
	teto  int64
	usado atomic.Int64
}

// NovoGlobal cria o orçamento do processo. teto <= 0 é tratado como ILIMITADO,
// e o caso existe para o chamador que não tem configuração — ferramenta, teste,
// benchmark. Não é fallback do servidor: lá o teto vem do units.Size, que já
// recusa valor não positivo.
func NovoGlobal(teto int64) *Global { return &Global{teto: teto} }

// Usado é quanto do orçamento está reservado NESTE instante. É a fonte de
// nishidb_query_buffer_bytes.
func (g *Global) Usado() int64 { return g.usado.Load() }

// Reserva tenta tomar n bytes e devolve false quando eles não cabem.
//
// O laço de CAS é o que torna a decisão atômica: um `Load` seguido de `Add`
// deixaria duas consultas lerem o mesmo "cabe" e passarem juntas do teto, e o
// orçamento seria excedido justamente sob a concorrência que ele existe para
// conter. O `Add` sozinho tampouco serve — ele não sabe recusar, e desfazê-lo
// depois já teria publicado um valor acima do teto para quem lesse a métrica.
func (g *Global) Reserva(n int64) bool {
	if g.teto <= 0 {
		g.usado.Add(n)
		return true
	}
	for {
		usado := g.usado.Load()
		novo := usado + n
		if novo > g.teto {
			return false
		}
		if g.usado.CompareAndSwap(usado, novo) {
			return true
		}
	}
}

// Devolve libera n bytes. Só pode ser chamada com o que foi reservado: devolver
// a mais faria o `usado` andar para baixo do real e o teto deixaria de valer.
func (g *Global) Devolve(n int64) { g.usado.Add(-n) }

// PorQuery é o orçamento de UMA consulta (runtime.query.maxBufferPerQuery).
//
// Existe além do Global porque os dois recusam coisas diferentes. O Global
// impede que N consultas somadas estourem a máquina; este impede que UMA
// consulta patológica — uma linha de 1 MiB num resultado de milhões — consuma
// sozinha o orçamento que era de todas. Sem o teto por consulta, a primeira a
// chegar levaria o processo inteiro e as demais receberiam recusa por culpa
// dela.
type PorQuery struct {
	global *Global
	teto   int64
	usado  atomic.Int64
}

// Query abre um orçamento para uma consulta, com o teto por consulta dado.
// teto <= 0 é ilimitado, pela mesma razão do NovoGlobal.
func (g *Global) Query(teto int64) *PorQuery {
	return &PorQuery{global: g, teto: teto}
}

// Teto é o limite desta consulta; 0 ou negativo significa ilimitado.
func (p *PorQuery) Teto() int64 { return p.teto }

// Usado é quanto esta consulta tem reservado.
func (p *PorQuery) Usado() int64 { return p.usado.Load() }

// Admite reserva o orçamento INTEIRO desta consulta, de UMA VEZ, e diz se ele
// coube no processo.
//
// De uma vez, e não conforme o buffer cresce, porque a decisão de memória tem de
// ser de ADMISSÃO. Com reserva incremental o orçamento do processo poderia
// acabar no MEIO de um stream — e a consulta morreria depois de o cliente já ter
// recebido quarenta mil linhas, por causa da memória de OUTRA consulta que nada
// tem a ver com ela. Reservado na entrada, ou a consulta tem o orçamento dela
// inteiro desde o começo, ou não começa; e quem chama tem, aí sim, um 503
// honesto para devolver, com Retry-After, antes do primeiro byte.
//
// O efeito colateral é que o caminho quente do StreamWriter não chama este
// pacote NENHUMA vez: ele só compara a capacidade do buffer contra o Teto. Nada
// de CAS por linha, nada de contenção entre as N consultas simultâneas do §80.
//
// Teto <= 0 é ilimitado e sempre admite, sem contabilizar: não há número a
// reservar, e inventar um faria o nishidb_query_buffer_bytes de um processo sem
// configuração publicar uma grandeza que não descreve nada.
func (p *PorQuery) Admite() bool {
	if p.teto <= 0 {
		return true
	}
	if !p.global.Reserva(p.teto) {
		return false
	}
	p.usado.Store(p.teto)
	return true
}

// Libera devolve TUDO o que esta consulta tinha reservado, e é idempotente: uma
// segunda chamada devolve zero.
//
// Idempotente de propósito — o chamador a põe num `defer` e pode tê-la chamado
// no caminho de erro também. Uma devolução dupla faria o `usado` do processo
// andar para baixo do real, e o teto deixaria de valer para todo mundo a partir
// dali, em silêncio.
func (p *PorQuery) Libera() {
	if n := p.usado.Swap(0); n != 0 {
		p.global.Devolve(n)
	}
}
