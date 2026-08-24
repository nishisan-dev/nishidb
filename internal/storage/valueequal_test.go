package storage

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// valueEqual decide se o diff do write path toca o índice. Um tipo de largura
// fixa que caísse no ramo dos bytes compararia nil com nil, devolveria true
// para valores DIFERENTES, e a posting simplesmente não seria atualizada —
// sem erro, sem log, sem métrica. Este teste é o que torna esse esquecimento
// visível.
func TestValueEqualDistingueValoresDeTodoTipoDeLarguraFixa(t *testing.T) {
	fixos := []types.FieldType{
		types.TypeInt64, types.TypeDouble, types.TypeBool,
		types.TypeTimestamp, types.TypeDateTime, types.TypeTime,
	}
	for _, ft := range fixos {
		a := types.Value{Type: ft, I64: 1}
		b := types.Value{Type: ft, I64: 2}
		if valueEqual(a, b) {
			t.Errorf("%v: valores diferentes (1 vs 2) foram considerados iguais", ft)
		}
		if !valueEqual(a, types.Value{Type: ft, I64: 1}) {
			t.Errorf("%v: valores iguais foram considerados diferentes", ft)
		}
	}
}

func TestValueEqualNaoIgualaTiposDiferentesComMesmoI64(t *testing.T) {
	a := types.Value{Type: types.TypeTimestamp, I64: 68042000}
	b := types.Value{Type: types.TypeTime, I64: 68042000}
	if valueEqual(a, b) {
		t.Fatal("timestamp e time com o mesmo I64 não podem comparar iguais")
	}
}
