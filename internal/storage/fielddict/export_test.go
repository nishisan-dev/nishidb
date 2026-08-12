package fielddict

import "github.com/nishisan-dev/nishidb/internal/types"

// setNextIDForTest posiciona o próximo id para testar o limite de u16 sem
// criar 65 mil entries reais.
func (d *Dict) setNextIDForTest(next types.FieldID) {
	for types.FieldID(len(d.byID)) < next-1 {
		d.byID = append(d.byID, "")
	}
}

// WrapFileForTest substitui o handle do fields.dict por um wrapper de teste
// (ex.: gravador de ordem de chamadas de durabilidade).
func (d *Dict) WrapFileForTest(wrap func(dictFile) dictFile) {
	d.f = wrap(d.f)
}
