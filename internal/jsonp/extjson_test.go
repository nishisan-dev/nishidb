package jsonp

import "testing"

func TestExtendedNumberLong(t *testing.T) {
	cases := []struct {
		name string
		in   string
		raw  string
	}{
		{"basico", `{"n":{"$numberLong":"66952"}}`, "66952"},
		{"negativo", `{"n":{"$numberLong":"-5"}}`, "-5"},
		{"whitespace interno", `{"n":{ "$numberLong" : "1" }}`, "1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "n", Kind: KindNumber, Ext: ExtNumberLong, Raw: tc.raw},
			})
		})
	}
}

func TestExtendedDateRelaxed(t *testing.T) {
	assertFields(t, collect(t, `{"d":{"$date":"2026-08-11T22:28:10.000Z"}}`), []ownedField{
		{Key: "d", Kind: KindString, Ext: ExtDate, Raw: "2026-08-11T22:28:10.000Z"},
	})
}

func TestExtendedDateCanonical(t *testing.T) {
	assertFields(t, collect(t, `{"d":{"$date":{"$numberLong":"1755037690000"}}}`), []ownedField{
		{Key: "d", Kind: KindNumber, Ext: ExtDate, Raw: "1755037690000"},
	})
}

func TestExtendedFollowedByOtherFields(t *testing.T) {
	in := `{"_SEQ":{"$numberLong":"101"},"status":"Start"}`
	assertFields(t, collect(t, in), []ownedField{
		{Key: "_SEQ", Kind: KindNumber, Ext: ExtNumberLong, Raw: "101"},
		{Key: "status", Kind: KindString, Raw: "Start"},
	})
}

// Shapes que NÃO são wrapper caem no fallback: objeto opaco, Ext=ExtNone.
func TestNotAWrapperFallsBackToOpaque(t *testing.T) {
	cases := []struct {
		name string
		in   string
		raw  string
	}{
		{"duas chaves", `{"n":{"$numberLong":"1","x":2}}`, `{"$numberLong":"1","x":2}`},
		{"outro dollar", `{"n":{"$oid":"abc"}}`, `{"$oid":"abc"}`},
		{"numberLong valor nao-string", `{"n":{"$numberLong":123}}`, `{"$numberLong":123}`},
		{"date valor numero", `{"n":{"$date":123}}`, `{"$date":123}`},
		{"date canonical com outra chave", `{"n":{"$date":{"$numberInt":"1"}}}`, `{"$date":{"$numberInt":"1"}}`},
		{"chave comum", `{"n":{"numberLong":"1"}}`, `{"numberLong":"1"}`},
		// F1: $numberLong exige conteúdo -?[0-9]+ não vazio; qualquer outra
		// string degrada para objeto opaco (senão o typeconv receberia
		// ExtNumberLong→int64 com lixo e erraria a categoria do erro).
		{"numberLong nao-digitos", `{"n":{"$numberLong":"abc"}}`, `{"$numberLong":"abc"}`},
		{"numberLong vazio", `{"n":{"$numberLong":""}}`, `{"$numberLong":""}`},
		{"numberLong fracao", `{"n":{"$numberLong":"1.5"}}`, `{"$numberLong":"1.5"}`},
		{"numberLong espacos", `{"n":{"$numberLong":" 12 "}}`, `{"$numberLong":" 12 "}`},
		{"numberLong so sinal", `{"n":{"$numberLong":"-"}}`, `{"$numberLong":"-"}`},
		{"date canonical nao-digitos", `{"n":{"$date":{"$numberLong":"xyz"}}}`, `{"$date":{"$numberLong":"xyz"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertFields(t, collect(t, tc.in), []ownedField{
				{Key: "n", Kind: KindObject, Ext: ExtNone, Raw: tc.raw},
			})
		})
	}
}

// Wrapper aninhado dentro de array/objeto opaco NÃO é desembrulhado:
// só valores diretos de campos top-level são inspecionados.
func TestWrapperInsideNestedStaysRaw(t *testing.T) {
	assertFields(t, collect(t, `{"a":[{"$numberLong":"1"}]}`), []ownedField{
		{Key: "a", Kind: KindArray, Raw: `[{"$numberLong":"1"}]`},
	})
}

// Interação de maior risco do pacote: tryExtended salva/restaura len(scratch)
// enquanto a chave do campo (e/ou o valor do wrapper) já vivem no scratch.
func TestExtendedWithEscapes(t *testing.T) {
	t.Run("chave escapada com numberLong", func(t *testing.T) {
		assertFields(t, collect(t, `{"k\nx":{"$numberLong":"12"}}`), []ownedField{
			{Key: "k\nx", Kind: KindNumber, Ext: ExtNumberLong, Raw: "12"},
		})
	})
	t.Run("chave escapada com date canonical", func(t *testing.T) {
		assertFields(t, collect(t, `{"k\nx":{"$date":{"$numberLong":"12"}}}`), []ownedField{
			{Key: "k\nx", Kind: KindNumber, Ext: ExtDate, Raw: "12"},
		})
	})
	t.Run("valor relaxed com escape unicode", func(t *testing.T) {
		// \u005A = 'Z'; o valor cai no parseStringSlow com a chave já no scratch.
		assertFields(t, collect(t, `{"k\tx":{"$date":"2026-08-11T22:28:10.000\u005A"}}`), []ownedField{
			{Key: "k\tx", Kind: KindString, Ext: ExtDate, Raw: "2026-08-11T22:28:10.000Z"},
		})
	})
	t.Run("chave escapada com fallback e campo seguinte", func(t *testing.T) {
		assertFields(t, collect(t, `{"k\nx":{"$oid":"abc"},"z":"w"}`), []ownedField{
			{Key: "k\nx", Kind: KindObject, Raw: `{"$oid":"abc"}`},
			{Key: "z", Kind: KindString, Raw: "w"},
		})
	})
}
