package counter

import "time"

// Divergencia é um grupo cujo estado mantido não bate com o recalculado por
// varredura. Texto é o groupKey — o nome que o operador consegue procurar.
type Divergencia struct {
	ID                           uint32
	Texto                        string
	Mantido, Recalculado         int64
	RefsMantido, RefsRecalculado int64
}

// Relatorio é o resultado da auditoria de UM contador: o retrato T0 dos
// totais contra o que a varredura das linhas vivas (com location e sem lápide
// em T0) recalculou com a mesma definição.
//
// Divergentes conta grupos com total OU refs diferentes — refs entra porque é
// ele que governa a aposentadoria, e um refs errado é o que faria um grupo
// referenciado sair. NaoInternadas conta linhas vivas cujo grupo o dicionário
// não tem: é a divergência que não cabe num grupo, porque o grupo não existe.
type Relatorio struct {
	Nome   string
	Grupos uint32
	// Mantido e Recalculado são as SOMAS dos totais, para a linha da tabela.
	Mantido, Recalculado int64
	// LinhasMantidas e LinhasRecalculadas são o mesmo par para os REFS:
	// quantas linhas apontam para algum grupo.
	LinhasMantidas, LinhasRecalculadas int64
	Divergentes                        int
	NaoInternadas                      uint64
	// AgregadoLinhas e AgregadoTotal são os agregados do PRÓPRIO contador
	// (Estatisticas.Linhas e .Total) em T0 — a segunda fonte para o mesmo
	// fato que os vetores por grupo.
	AgregadoLinhas, AgregadoTotal int64
	// AgregadosDivergentes é 0, 1 ou 2: quantos dos dois discordam do
	// RECALCULADO. Separado de Divergentes porque a pergunta é outra —
	// Divergentes diz "algum grupo derivou", este diz "o agregado derivou".
	// Uma linha que trocou de grupo por deriva dá Divergentes > 0 e este
	// ZERO: os grupos estão errados e o agregado, certo.
	AgregadosDivergentes int
	// Amostra são as primeiras divergências, por id de grupo; o teto evita um
	// relatório do tamanho do inventário num contador inteiramente errado.
	Amostra        []Divergencia
	LinhasVarridas uint64
	Duracao        time.Duration
}

// TetoDaAmostra é quantas divergências o relatório nomeia.
const TetoDaAmostra = 20

// Reconstrucao é o resultado de uma reconstrução em operação de UM contador.
type Reconstrucao struct {
	Nome    string
	Linhas  uint64
	Grupos  uint32
	Duracao time.Duration
}

// PorChave resolve a chave composta de um grupo VIVO para o id. É o que a
// auditoria usa: ela rederiva a chave de cada linha e a procura, sem internar
// — auditoria é leitura.
func (c *Contador) PorChave(chave []byte) (uint32, bool) {
	return c.dict.PorChave(chave)
}
