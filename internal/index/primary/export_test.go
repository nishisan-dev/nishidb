package primary

// setHashForTest injeta uma função de hash determinística para forçar
// colisões e wrap-around nos testes.
func (ix *Index) setHashForTest(fn func([]byte) uint64) { ix.hashFn = fn }

// setSyncDirForTest injeta falha no fsync de diretório do rehash; devolve a
// função de restauração.
func setSyncDirForTest(fn func(string) error) func() {
	old := syncDirFn
	syncDirFn = fn
	return func() { syncDirFn = old }
}
