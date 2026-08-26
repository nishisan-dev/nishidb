package hash

// injetaHashParaTeste substitui a função de hash por uma determinística, para
// forçar colisões e wrap-around sem depender de encontrar uma colisão real de
// 64 bits (que não é construível em tempo de teste).
//
// A colisão é o caminho onde mora o único falso NEGATIVO possível deste
// índice — ver o godoc de Clear —, e um caminho sem oráculo é um caminho que
// não existe para a suíte.
func (ix *Index) injetaHashParaTeste(fn func([]byte) uint64) { ix.hashFn = fn }

// injetaSyncDirParaTeste injeta falha no fsync de diretório da reconstrução;
// devolve a função de restauração.
func injetaSyncDirParaTeste(fn func(string) error) func() {
	old := syncDirFn
	syncDirFn = fn
	return func() { syncDirFn = old }
}
