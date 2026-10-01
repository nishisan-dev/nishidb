package counter

// wrapFileForTest substitui o handle do grupos.dict por um wrapper de
// teste (o gravador de ordem das chamadas de durabilidade).
func (d *Dict) wrapFileForTest(wrap func(dictFile) dictFile) {
	d.f = wrap(d.f)
}

// wrapDictOpenForTest faz toda abertura de grupos.dict passar pelo wrapper
// ANTES do primeiro uso do handle — é o único jeito de observar o fsync da
// abertura sincronizada. Devolve a restauração.
func wrapDictOpenForTest(wrap func(dictFile) dictFile) (restore func()) {
	old := openDictFile
	openDictFile = func(path string) (dictFile, error) {
		f, err := old(path)
		if err != nil {
			return nil, err
		}
		return wrap(f), nil
	}
	return func() { openDictFile = old }
}
