package kafka

import (
	"context"
	"time"
)

// PrazoDaMarcaDagua é o teto de UMA consulta de fim de log ao broker, por
// dataset.
//
// Folgado dentro dos 5 s do scrape: mesmo com vários datasets em paralelo, o
// pior caso de cada um é este, e não a soma. Estourá-lo omite as colunas e as
// séries que dependem do broker naquela leitura, que é a resposta honesta.
//
// Mora aqui, e não no servidor, porque é propriedade da conversa com o broker
// e tem DOIS chamadores — o scrape do /metrics e a tabela `_meta.kafka`. Com
// uma constante em cada lado, um gate teria de vigiar que os dois números
// batem; com uma constante só, não há o que divergir.
const PrazoDaMarcaDagua = 2 * time.Second

// Instantaneo é o retrato da ingestão de UM dataset num instante, lido das
// MESMAS estruturas que alimentam o /metrics e o /v1/stats: as posições
// publicadas pelo writer, a marca d'água do broker, a profundidade da fila e
// os contadores do writer. Nenhum contador paralelo — duas fontes para o mesmo
// número divergem, e a divergência aparece como um painel que discorda do SQL
// sem nada estar quebrado.
//
// O que é ausência viaja como ausência: fatia vazia, tempo zero, HasEnd falso.
// Quem monta a linha da tabela transforma isso em NULL, e "medi e deu zero"
// nunca vira "não há o que medir".
//
// A identidade do fluxo — tópico, grupo, bootstrap — NÃO está aqui: ela sai do
// YAML do dataset, e um campo neste retrato seria um segundo caminho para o
// mesmo fato.
type Instantaneo struct {
	// Partitions vem em ordem crescente de partição. Vazia quando o consumidor
	// ainda não teve partição atribuída, o que NÃO é o mesmo que "todas as
	// partições em zero".
	Partitions []PositionOfPartition
	// MarkIn carimba a idade de EndOfLog e Atraso. Zero quando não há marca
	// válida — e então nenhuma partição tem HasEnd.
	MarkIn time.Time

	FilaRecords int
	FilaBytes   int64

	// UltimoLote é o instante do último lote NÃO VAZIO entregue pelo poller.
	// Zero enquanto nenhum chegou nesta subida.
	UltimoLote time.Time

	Desfecho Desfecho
	// Pausa é o motivo da parada por política (§101), ou nil.
	Pausa error

	PartitionsAtribuidas int64
	PartitionsOfTopic    int64
}

// Instantaneo colhe o retrato AGORA, e atualiza a marca d'água no broker
// SOMENTE quando ela está vencida.
//
// A decisão de ir ao broker é do consumidor, e não de quem chama: a regra de
// validade mora ao lado do dado que ela governa, e um chamador que a
// reimplementasse seria a segunda fonte da mesma decisão. Dentro da validade
// este método não faz I/O nenhum e o ctx não é tocado.
//
// O ctx limita EXCLUSIVAMENTE a ida ao broker.
func (c *Consumidor) Instantaneo(ctx context.Context) Instantaneo {
	if c.met.markVencida() {
		c.AtualizaMarkDagua(ctx)
	}
	ps, in := c.met.posicoesOfInstant()
	records, bytes := c.fila.Profundidade()
	return Instantaneo{
		Partitions:           ps,
		MarkIn:               in,
		FilaRecords:          records,
		FilaBytes:            bytes,
		UltimoLote:           c.met.ultimoLoteIn(),
		Desfecho:             c.w.Desfecho(),
		Pausa:                c.w.Pausa(),
		PartitionsAtribuidas: int64(c.met.partAtribuidas.Value()),
		PartitionsOfTopic:    int64(c.met.partTotal.Value()),
	}
}
