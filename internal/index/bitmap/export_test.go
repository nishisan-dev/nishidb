package bitmap

// WrapDictFileForTest substitui o handle do values.dict por um wrapper de
// teste (ex.: gravador de ordem de chamadas de durabilidade).
func (ix *Index) WrapDictFileForTest(wrap func(dictFile) dictFile) {
	ix.dictF = wrap(ix.dictF)
}
