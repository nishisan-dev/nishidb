package primary

// setHashForTest injeta uma função de hash determinística para forçar
// colisões e wrap-around nos testes.
func (ix *Index) setHashForTest(fn func([]byte) uint64) { ix.hashFn = fn }
