// Package materializer implementa a Fase 2 da spec: conversão tipada de
// eventos JSON, montagem de patches e materialização merge/replace.
package materializer

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/unsafex"
)

// ErrTypeConversion é a categoria de erros de conversão (spec §101:
// TYPE_CONVERSION). Use errors.Is(err, ErrTypeConversion).
var ErrTypeConversion = errors.New("type conversion")

// ConversionError descreve uma conversão inválida. O nome do campo é
// adicionado pelo caller (PatchBuffer), que o conhece.
type ConversionError struct {
	Target types.FieldType
	Msg    string
}

func (e *ConversionError) Error() string {
	return fmt.Sprintf("conversão para %v falhou: %s", e.Target, e.Msg)
}

func (e *ConversionError) Unwrap() error { return ErrTypeConversion }

// Convert converte um campo do tokenizer para a representação binária do
// tipo alvo (spec §17-18). Tipos representados em Value.Bytes são SEMPRE
// copiados/derivados para arena (nunca aliasam f.Raw, que morre no próximo
// Next do parser); o arena possivelmente crescido é retornado. null JSON
// vira Value{Type: TypeNull} para qualquer alvo (NULL = campo ausente, D2).
func Convert(f jsonp.Field, target types.FieldType, arena []byte) (types.Value, []byte, error) {
	if f.Kind == jsonp.KindNull {
		return types.Value{Type: types.TypeNull}, arena, nil
	}
	switch target {
	case types.TypeString:
		if f.Kind != jsonp.KindString {
			return types.Value{}, arena, convErrf(target, "esperava string JSON, veio %v", f.Kind)
		}
		return appendBytesValue(target, f.Raw, arena)

	case types.TypeInt64:
		if f.Kind != jsonp.KindNumber {
			return types.Value{}, arena, convErrf(target, "esperava número JSON, veio %v", f.Kind)
		}
		n, err := parseInt64(f.Raw)
		if err != nil {
			return types.Value{}, arena, err
		}
		return types.Value{Type: target, I64: n}, arena, nil

	case types.TypeDouble:
		if f.Kind != jsonp.KindNumber {
			return types.Value{}, arena, convErrf(target, "esperava número JSON, veio %v", f.Kind)
		}
		d, err := parseFloat64(f.Raw)
		if err != nil {
			return types.Value{}, arena, err
		}
		return types.Value{Type: target, I64: int64(math.Float64bits(d))}, arena, nil

	case types.TypeBool:
		if f.Kind != jsonp.KindBool {
			return types.Value{}, arena, convErrf(target, "esperava bool JSON, veio %v", f.Kind)
		}
		var i int64
		if len(f.Raw) == 4 { // "true"
			i = 1
		}
		return types.Value{Type: target, I64: i}, arena, nil

	case types.TypeTimestamp:
		switch f.Kind {
		case jsonp.KindString:
			ms, err := parseRFC3339Millis(f.Raw)
			if err != nil {
				return types.Value{}, arena, err
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		case jsonp.KindNumber:
			// Número = epoch millis (inclui $date canonical já desembrulhado).
			ms, err := parseInt64(f.Raw)
			if err != nil {
				return types.Value{}, arena, convErrf(target, "epoch millis inválido: %s", f.Raw)
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		}
		return types.Value{}, arena, convErrf(target, "esperava string RFC3339 ou epoch millis, veio %v", f.Kind)

	case types.TypeJSON:
		if f.Kind != jsonp.KindObject && f.Kind != jsonp.KindArray {
			return types.Value{}, arena, convErrf(target, "esperava object/array JSON, veio %v", f.Kind)
		}
		return appendBytesValue(target, f.Raw, arena)

	case types.TypeIPv4, types.TypeIPv6, types.TypeIPv4Prefix,
		types.TypeIPv6Prefix, types.TypeBytes:
		return convertNetwork(f, target, arena)
	}
	return types.Value{}, arena, convErrf(target, "tipo alvo não suportado")
}

// InferType resolve o tipo de um campo `auto` (spec §17) a partir do shape
// JSON, honrando os wrappers Mongo (spec §19). Retorna TypeNull para null.
func InferType(f jsonp.Field) types.FieldType {
	switch f.Ext {
	case jsonp.ExtNumberLong:
		return types.TypeInt64
	case jsonp.ExtDate:
		return types.TypeTimestamp
	}
	switch f.Kind {
	case jsonp.KindString:
		return types.TypeString
	case jsonp.KindNumber:
		if numberIsIntegral(f.Raw) {
			return types.TypeInt64
		}
		return types.TypeDouble
	case jsonp.KindBool:
		return types.TypeBool
	case jsonp.KindObject, jsonp.KindArray:
		return types.TypeJSON
	}
	return types.TypeNull
}

// appendBytesValue copia src para o arena e devolve o Value apontando para
// a janela copiada.
func appendBytesValue(t types.FieldType, src, arena []byte) (types.Value, []byte, error) {
	start := len(arena)
	arena = append(arena, src...)
	return types.Value{Type: t, Bytes: arena[start:len(arena):len(arena)]}, arena, nil
}

// numberIsIntegral reporta se um número JSON já validado é inteiro
// (sem fração nem expoente).
func numberIsIntegral(raw []byte) bool {
	for _, c := range raw {
		if c == '.' || c == 'e' || c == 'E' {
			return false
		}
	}
	return true
}

// parseInt64 converte dígitos JSON (sem fração/expoente) com check de
// overflow. Não usa strconv para evitar a conversão []byte→string.
func parseInt64(raw []byte) (int64, error) {
	if !numberIsIntegral(raw) {
		return 0, convErrf(types.TypeInt64, "número com fração/expoente: %s", raw)
	}
	i := 0
	neg := false
	if len(raw) > 0 && raw[0] == '-' {
		neg = true
		i++
	}
	if i >= len(raw) {
		return 0, convErrf(types.TypeInt64, "número vazio")
	}
	// Acumula como negativo (o range de int64 é assimétrico: MinInt64 não
	// tem par positivo).
	var n int64
	for ; i < len(raw); i++ {
		c := raw[i]
		if c < '0' || c > '9' {
			return 0, convErrf(types.TypeInt64, "dígito inválido em %s", raw)
		}
		d := int64(c - '0')
		if n < (math.MinInt64+d)/10 {
			return 0, convErrf(types.TypeInt64, "overflow de int64: %s", raw)
		}
		n = n*10 - d
	}
	if !neg {
		if n == math.MinInt64 {
			return 0, convErrf(types.TypeInt64, "overflow de int64: %s", raw)
		}
		n = -n
	}
	return n, nil
}

// parseFloat64 converte um número JSON para float64. strconv.ParseFloat não
// retém o argumento, então a conversão via unsafex.String é segura.
func parseFloat64(raw []byte) (float64, error) {
	d, err := strconv.ParseFloat(unsafex.String(raw), 64)
	if err != nil {
		return 0, convErrf(types.TypeDouble, "número inválido: %s", raw)
	}
	return d, nil
}

func convErrf(target types.FieldType, format string, args ...any) error {
	return &ConversionError{Target: target, Msg: fmt.Sprintf(format, args...)}
}
