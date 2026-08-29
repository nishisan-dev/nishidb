package segindex

// SondasDoUltimoSet devolve quantos slots de diretório o último Set examinou.
//
// Só existe no binário de teste, e é o instrumento do gate de custo: ele mede
// a FORMA do algoritmo — sondas por inserção — em vez de nanossegundos, então
// não tem relógio, não tem ruído de runner e roda também sob -race. Exportar o
// contador é o preço de o gate poder afirmar "O(1)" em vez de "rápido nesta
// máquina".
func (w *WIP) SondasDoUltimoSet() int { return w.sondas }
