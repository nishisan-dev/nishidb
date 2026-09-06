package storage

import (
	"bytes"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// valueEqual compara duas Values de mesmo campo.
//
// Ela ERA código de produção: decidia se o diff do write path tocava o índice.
// O índice por segmento não diffa — entra cada campo indexado do payload, sempre
// (spec §5.2) —, então o último chamador de produção saiu com os motores
// globais, e ela veio junto para o arquivo de teste. Manter no pacote de
// produção uma função que só teste chama é o começo do próximo motor morto.
//
// Ela continua valendo o teste abaixo porque os testes de índice comparam
// valores de payload pelo mesmo critério que o produto usa em outros pontos, e o
// modo de falha que o teste guarda é o mesmo de sempre.
func valueEqual(a, b types.Value) bool {
	if a.Type != b.Type {
		return false
	}
	// CarriesI64 e não um switch local: tipo de largura fixa esquecido no
	// ramo de bytes compara nil com nil e devolve true para valores
	// diferentes, e a comparação conclui em silêncio que o campo não mudou.
	if a.Type.CarriesI64() {
		return a.I64 == b.I64
	}
	return bytes.Equal(a.Bytes, b.Bytes)
}

// Um tipo de largura fixa que caísse no ramo dos bytes compararia nil com nil e
// devolveria true para valores DIFERENTES — sem erro, sem log, sem métrica.
// Este teste é o que torna esse esquecimento visível.
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
