package materializer

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
)

func layoutDT(t *testing.T, padrao string) *typeparse.Layout {
	t.Helper()
	l, err := typeparse.CompileLayout(padrao, typeparse.DomainDateTime)
	if err != nil {
		t.Fatalf("CompilaLayout(%q): %v", padrao, err)
	}
	return l
}

func layoutTOD(t *testing.T, padrao string) *typeparse.Layout {
	t.Helper()
	l, err := typeparse.CompileLayout(padrao, typeparse.DomainTime)
	if err != nil {
		t.Fatalf("CompilaLayout(%q): %v", padrao, err)
	}
	return l
}

func TestConvertDatetimeUsaOLayoutDoCampo(t *testing.T) {
	l := layoutDT(t, "MMM d, yyyy hh:mm:ss a")
	v, _, err := Convert(str("Aug 22, 2026 08:54:02 PM"), types.TypeDateTime, l, nil)
	if err != nil {
		t.Fatalf("Convert: %v", err)
	}
	if v.Type != types.TypeDateTime {
		t.Fatalf("tipo = %v", v.Type)
	}
	want := time.Date(2026, 8, 22, 20, 54, 2, 0, time.Local).UnixMilli()
	if v.I64 != want {
		t.Fatalf("I64 = %d, esperado %d (resolvido em time.Local)", v.I64, want)
	}
}

// O format é um contrato: um texto em outra grafia é dado que este campo não
// aceita, e tem de sair como TYPE_CONVERSION — não como sucesso silencioso.
func TestConvertDatetimeRecusaTextoEmOutroFormato(t *testing.T) {
	l := layoutDT(t, "MMM d, yyyy hh:mm:ss a")
	for _, in := range []string{"2026-08-22T20:54:02Z", "22/08/2026 20:54:02", "Aug 22, 2026"} {
		_, _, err := Convert(str(in), types.TypeDateTime, l, nil)
		if err == nil {
			t.Errorf("%q: esperava recusa", in)
			continue
		}
		if !errors.Is(err, ErrTypeConversion) {
			t.Errorf("%q: erro fora de ErrTypeConversion: %v", in, err)
		}
	}
}

// Simetria com o timestamp: número é epoch millis nos dois. Sem ela, trocar o
// tipo de um campo mudaria o significado de um número já ingerido.
func TestConvertDatetimeAceitaNumeroComoEpochMillis(t *testing.T) {
	l := layoutDT(t, "MMM d, yyyy hh:mm:ss a")
	v, _, err := Convert(num("1787878442000"), types.TypeDateTime, l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.I64 != 1787878442000 {
		t.Fatalf("I64 = %d", v.I64)
	}
}

func TestConvertTimeUsaOLayoutEIgnoraFuso(t *testing.T) {
	l := layoutTOD(t, "hh:mm:ss a")
	v, _, err := Convert(str("08:54:02 PM"), types.TypeTime, l, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != types.TypeTime {
		t.Fatalf("tipo = %v", v.Type)
	}
	const want = int64((20*3600 + 54*60 + 2) * 1000)
	if v.I64 != want {
		t.Fatalf("I64 = %d, esperado %d", v.I64, want)
	}
}

// Um epoch millis mandado por engano para um campo time seria gravado como
// hora do dia absurda, e a saída NDJSON o renderiza como null — dado perdido
// em silêncio. A faixa é validada na porta.
func TestConvertTimeRecusaNumeroForaDoDia(t *testing.T) {
	l := layoutTOD(t, "hh:mm:ss a")
	for _, in := range []string{"1787878442000", "86400000", "-1"} {
		if _, _, err := Convert(num(in), types.TypeTime, l, nil); err == nil {
			t.Errorf("%q: esperava recusa", in)
		}
	}
	for _, in := range []string{"0", "86399999"} {
		if _, _, err := Convert(num(in), types.TypeTime, l, nil); err != nil {
			t.Errorf("%q: extremo do dia foi recusado: %v", in, err)
		}
	}
}

// NULL é campo ausente para qualquer alvo, inclusive os temporais.
func TestConvertTemporalNuloContinuaCampoAusente(t *testing.T) {
	nulo := jsonp.Field{Kind: jsonp.KindNull, Raw: []byte("null")}
	for _, alvo := range []types.FieldType{types.TypeDateTime, types.TypeTime} {
		v, _, err := Convert(nulo, alvo, nil, nil)
		if err != nil {
			t.Fatalf("%v: %v", alvo, err)
		}
		if v.Type != types.TypeNull {
			t.Errorf("%v: esperava TypeNull, obtido %v", alvo, v.Type)
		}
	}
}

// O schema garante que todo campo temporal tem layout, mas a porta não pode
// depender disso: sem o fecho, um caminho novo que esquecesse o layout viraria
// nil deref no meio da ingestão em vez de um evento recusado.
func TestConvertTemporalSemLayoutFalhaEmVezDeEntrarEmPanic(t *testing.T) {
	for _, alvo := range []types.FieldType{types.TypeDateTime, types.TypeTime} {
		_, _, err := Convert(str("qualquer coisa"), alvo, nil, nil)
		if err == nil {
			t.Errorf("%v: esperava erro", alvo)
			continue
		}
		if !strings.Contains(err.Error(), "format") {
			t.Errorf("%v: mensagem não nomeia a causa: %v", alvo, err)
		}
	}
}

// Uma string com cara de data continua string num campo auto: o tipo temporal
// só existe declarado, porque só a declaração traz o format.
func TestInferTypeNuncaProduzTipoTemporal(t *testing.T) {
	for _, in := range []string{"Aug 22, 2026 08:54:02 PM", "2026-08-22 20:54:02", "08:54:02 PM"} {
		if got := InferType(str(in)); got != types.TypeString {
			t.Errorf("InferType(%q) = %v, esperado string", in, got)
		}
	}
}
