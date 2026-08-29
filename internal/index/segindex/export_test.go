package segindex

import "github.com/nishisan-dev/nishidb/internal/types"

// SondasDoUltimoSet devolve quantos slots de diretório o último Set examinou.
//
// Só existe no binário de teste, e é o instrumento do gate de custo: ele mede
// a FORMA do algoritmo — sondas por inserção — em vez de nanossegundos, então
// não tem relógio, não tem ruído de runner e roda também sob -race. Exportar o
// contador é o preço de o gate poder afirmar "O(1)" em vez de "rápido nesta
// máquina".
func (w *WIP) SondasDoUltimoSet() int { return w.sondas }

// EstrategiaDe expõe qual caminho um iterador devolvido por Une tomou.
//
// Só existe no binário de teste: é o que permite ao teste de fronteira (faixas
// que se TOCAM) e ao de faixas disjuntas afirmarem a ESTRATÉGIA em si, e não
// só o resultado — um bug que trocasse concatenação por merge (ou vice-versa)
// mas ainda produzisse a resposta certa por acidente de dados pequenos
// passaria batido sem isto.
func EstrategiaDe(it types.RowIDIterator) string {
	switch it.(type) {
	case *concatIter:
		return "concatenacao"
	case *mergeIter:
		return "merge"
	default:
		return "outra"
	}
}
