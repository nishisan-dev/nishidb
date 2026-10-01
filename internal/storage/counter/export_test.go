package counter

import "reflect"

// overlayIdentityForTest identifica o mapa do overlay: muda quando o
// ApplyOverlay o troca por um novo, e só então.
func (c *Counter) overlayIdentityForTest() uintptr {
	return reflect.ValueOf(c.over).Pointer()
}

// wrapFileForTest substitui o handle do grupos.dict por um wrapper de
// teste (o gravador de ordem das chamadas de durabilidade).
func (d *Dict) wrapFileForTest(wrap func(dictFile) dictFile) {
	d.f = wrap(d.f)
}

// abandonForTest solta o contador SEM nenhum fsync nem msync — a morte do
// processo. O que foi escrito fica no page cache (o append do dicionário, as
// páginas do vetor), exatamente como depois de um kill -9; perder o que não foi
// sincronizado é trabalho do teste, que trunca o arquivo explicitamente.
func (c *Counter) abandonForTest() {
	_ = c.closeMeasures()
	_ = c.vector.Close()
	_ = c.dict.f.Close()
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
