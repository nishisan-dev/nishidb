package contador

// envolveArquivoParaTeste substitui o handle do grupos.dict por um wrapper de
// teste (o gravador de ordem das chamadas de durabilidade).
func (d *Dicionario) envolveArquivoParaTeste(envolve func(arquivoDoDicionario) arquivoDoDicionario) {
	d.f = envolve(d.f)
}
