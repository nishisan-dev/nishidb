package counter

import "time"

// Divergence é um grupo cujo estado mantido não bate com o recalculado por
// varredura. Text é o groupKey — o nome que o operador consegue procurar.
type Divergence struct {
	ID                             uint32
	Text                           string
	Maintained, Recomputed         int64
	RefsMaintained, RefsRecomputed int64
}

// Report é o resultado da auditoria de UM contador: o retrato T0 dos
// totais contra o que a varredura das linhas vivas (com location e sem lápide
// em T0) recalculou com a mesma definição.
//
// DivergentGroups conta grupos com total OU refs diferentes — refs entra
// porque é ele que governa a aposentadoria, e um refs errado é o que faria
// um grupo referenciado sair. NotInterned conta linhas vivas cujo grupo o
// dicionário não tem: é a divergência que não cabe num grupo, porque o grupo
// não existe.
type Report struct {
	TotalDisabled bool
	Name          string
	Groups        uint32
	// Maintained e Recomputed são as SOMAS dos totais, para a linha da tabela.
	Maintained, Recomputed int64
	// RowsMaintained e RowsRecomputed são o mesmo par para os REFS:
	// quantas linhas apontam para algum grupo.
	RowsMaintained, RowsRecomputed int64
	DivergentGroups                int
	NotInterned                    uint64
	// AggregateRows e AggregateTotal são os agregados do PRÓPRIO contador
	// (Stats.Rows e .Total) em T0 — a segunda fonte para o mesmo
	// fato que os vetores por grupo.
	AggregateRows, AggregateTotal int64
	// DivergentAggregates é 0, 1 ou 2: quantos dos dois discordam do
	// RECALCULADO. Separado de DivergentGroups porque a pergunta é outra —
	// DivergentGroups diz "algum grupo derivou", este diz "o agregado derivou".
	// Uma linha que trocou de grupo por deriva dá DivergentGroups > 0 e este
	// ZERO: os grupos estão errados e o agregado, certo.
	DivergentAggregates int
	// Sample são as primeiras divergências, por id de grupo; o teto evita um
	// relatório do tamanho do inventário num contador inteiramente errado.
	Sample      []Divergence
	RowsScanned uint64
	Duration    time.Duration
}

// SampleCeiling é quantas divergências o relatório nomeia.
const SampleCeiling = 20

// Rebuild é o resultado de uma reconstrução em operação de UM contador.
type Rebuild struct {
	Name     string
	Rows     uint64
	Groups   uint32
	Duration time.Duration
}

// ByKey resolve a chave composta de um grupo VIVO para o id. É o que a
// auditoria usa: ela rederiva a chave de cada linha e a procura, sem internar
// — auditoria é leitura.
func (c *Counter) ByKey(key []byte) (uint32, bool) {
	return c.dict.ByKey(key)
}
