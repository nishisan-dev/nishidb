package segindex

import "github.com/nishisan-dev/nishidb/internal/types"

// LastSetAccesses devolve quantos acessos ao mapeamento o último Set fez.
//
// Só existe no binário de teste, e é o instrumento do gate de custo: ele mede
// a FORMA do algoritmo — sondas por inserção — em vez de nanossegundos, então
// não tem relógio, não tem ruído de runner e roda também sob -race. Exportar o
// contador é o preço de o gate poder afirmar "O(1)" em vez de "rápido nesta
// máquina".
func (w *WIP) LastSetAccesses() int { return w.accesses }

// TempPathForTest expõe o nome do arquivo intermediário de UMA
// escrita.
//
// Só existe no binário de teste, e é o único observável direto do que a §2.4
// corrigiu: com o nome fixo anterior, dois escritores simultâneos do mesmo
// segmento — a selagem da rotação e a reconstrução da fila — escreviam no MESMO
// arquivo e o resultado saía rasgado. Provar isso só pelo comportamento
// dependeria de vencer uma corrida; aqui a propriedade que o impede é afirmada
// diretamente.
func TempPathForTest(path string) string { return tempPath(path) }

// StrategyOf expõe qual caminho um iterador devolvido por Union tomou.
//
// Só existe no binário de teste: é o que permite ao teste de fronteira (faixas
// que se TOCAM) e ao de faixas disjuntas afirmarem a ESTRATÉGIA em si, e não
// só o resultado — um bug que trocasse concatenação por merge (ou vice-versa)
// mas ainda produzisse a resposta certa por acidente de dados pequenos
// passaria batido sem isto.
//
// "direta" é o ramo de fonte única: Union devolve a própria Source sem
// embrulhar quando só uma sobrevive ao descarte de vazias. Não é caso de
// borda — é o caminho quente em produção para colunas de alta cardinalidade
// (ex.: identifier), onde um valor mora em praticamente um segmento só. Como
// nenhum tipo de Union é exposto para esse ramo (a Source devolvida é do
// chamador, não um combinador deste pacote), ele é o único jeito de o switch
// cair no default sem ambiguidade — "vazia" e os dois combinadores já têm
// case próprio.
func StrategyOf(it types.RowIDIterator) string {
	switch it.(type) {
	case *concatIter:
		return "concatenacao"
	case *mergeIter:
		return "merge"
	case emptyIter:
		return "vazia"
	default:
		return "direta"
	}
}
