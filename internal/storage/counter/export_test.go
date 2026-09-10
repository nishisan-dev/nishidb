package counter

// wrapFileForTest substitui o handle do grupos.dict por um wrapper de
// teste (o gravador de ordem das chamadas de durabilidade).
func (d *Dict) wrapFileForTest(wrap func(dictFile) dictFile) {
	d.f = wrap(d.f)
}
