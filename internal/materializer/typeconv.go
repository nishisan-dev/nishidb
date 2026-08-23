// Package materializer implementa a Fase 2 da spec: conversão tipada de
// eventos JSON, montagem de patches e materialização merge/replace.
package materializer

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
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
// O layout é o padrão compilado do campo, e é não-nil exatamente para os
// alvos datetime e time. Vem por parâmetro, e não por um lookup de schema
// aqui dentro, para manter este arquivo sem importar schema — a coerção é
// função do VALOR e do alvo, e é isso que permite ao binder SQL usar as
// mesmas regras (risco R3).
func Convert(f jsonp.Field, target types.FieldType, layout *typeparse.Layout, arena []byte) (types.Value, []byte, error) {
	if f.Kind == jsonp.KindNull {
		return types.Value{Type: types.TypeNull}, arena, nil
	}
	switch target {
	case types.TypeString:
		if f.Kind != jsonp.KindString {
			return types.Value{}, arena, convErrf(target, "esperava string JSON, veio %v", f.Kind)
		}
		if !utf8.Valid(f.Raw) {
			return types.Value{}, arena, convErrf(target, "utf-8 inválido: %s", truncVal(f.Raw))
		}
		return appendBytesValue(target, f.Raw, arena)

	case types.TypeInt64:
		// Ajuste pós-amostra (vinculante): tipo declarado coerce string de
		// dígitos — a amostra real traz Connect-Info como "1000000000".
		if f.Kind != jsonp.KindNumber && f.Kind != jsonp.KindString {
			return types.Value{}, arena, convErrf(target, "esperava número JSON ou string numérica, veio %v", f.Kind)
		}
		n, err := typeparse.Int64(f.Raw)
		if err != nil {
			return types.Value{}, arena, convErrf(target, "%s: %s", err, truncVal(f.Raw))
		}
		return types.Value{Type: target, I64: n}, arena, nil

	case types.TypeDouble:
		// Coerção string→double com a MESMA gramática de número JSON
		// (typeparse.Float64 a impõe; nada de NaN/Inf/hex).
		if f.Kind != jsonp.KindNumber && f.Kind != jsonp.KindString {
			return types.Value{}, arena, convErrf(target, "esperava número JSON ou string numérica, veio %v", f.Kind)
		}
		d, err := typeparse.Float64(f.Raw)
		if err != nil {
			return types.Value{}, arena, convErrf(target, "%s: %s", err, truncVal(f.Raw))
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
			ms, err := typeparse.RFC3339Millis(f.Raw)
			if err != nil {
				return types.Value{}, arena, convErrf(target, "%s: %s", err, truncVal(f.Raw))
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		case jsonp.KindNumber:
			// Número = epoch millis (inclui $date canonical já desembrulhado).
			ms, err := typeparse.Int64(f.Raw)
			if err != nil {
				return types.Value{}, arena, convErrf(target, "epoch millis inválido: %s", truncVal(f.Raw))
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		}
		return types.Value{}, arena, convErrf(target, "esperava string RFC3339 ou epoch millis, veio %v", f.Kind)

	case types.TypeJSON:
		if f.Kind != jsonp.KindObject && f.Kind != jsonp.KindArray {
			return types.Value{}, arena, convErrf(target, "esperava object/array JSON, veio %v", f.Kind)
		}
		// O jsonp faz skip estrutural sem validar o conteúdo da janela
		// (leniência documentada lá); a validade é garantida AQUI, na porta
		// de armazenamento — o valor seria emitido verbatim pelo encoder
		// NDJSON. json.Valid usa scanner pooled: 0 allocs em steady state.
		if !json.Valid(f.Raw) {
			return types.Value{}, arena, convErrf(target, "json aninhado inválido: %s", truncVal(f.Raw))
		}
		// json.Valid NÃO valida UTF-8: sem esta checagem, um valor json com
		// bytes malformados atravessa a ingestão e reaparece como linha NDJSON
		// inválida, quebrando o consumidor em runtime por um dado que deveria
		// ter sido recusado na porta. Mesma filosofia do teto do valor
		// indexado: recusar na entrada em vez de gravar algo que envenena a
		// leitura.
		if !utf8.Valid(f.Raw) {
			return types.Value{}, arena, convErrf(target, "utf-8 inválido: %s", truncVal(f.Raw))
		}
		return appendBytesValue(target, f.Raw, arena)

	case types.TypeDateTime:
		switch f.Kind {
		case jsonp.KindString:
			if layout == nil {
				return types.Value{}, arena, convErrf(target, "campo sem format compilado")
			}
			// time.Local, e não um fuso declarado: a decisão é que o instante
			// de um wall-clock sem offset é resolvido pelo fuso do HOST. Vale
			// para a ingestão e para o literal SQL, e é o que os mantém
			// concordando sobre o mesmo texto.
			ms, err := layout.DateTime(f.Raw, time.Local)
			if err != nil {
				return types.Value{}, arena, convErrf(target, "%s: %s", err, truncVal(f.Raw))
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		case jsonp.KindNumber:
			// Número é epoch millis, como no timestamp: é a grafia
			// independente de formato, e a simetria entre os dois evita que
			// trocar o tipo de um campo mude o significado de um número.
			ms, err := typeparse.Int64(f.Raw)
			if err != nil {
				return types.Value{}, arena, convErrf(target, "epoch millis inválido: %s", truncVal(f.Raw))
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		}
		return types.Value{}, arena, convErrf(target, "esperava string no format declarado ou epoch millis, veio %v", f.Kind)

	case types.TypeTime:
		switch f.Kind {
		case jsonp.KindString:
			if layout == nil {
				return types.Value{}, arena, convErrf(target, "campo sem format compilado")
			}
			ms, err := layout.TimeOfDay(f.Raw)
			if err != nil {
				return types.Value{}, arena, convErrf(target, "%s: %s", err, truncVal(f.Raw))
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		case jsonp.KindNumber:
			// Aqui o número NÃO é epoch: é millis desde a meia-noite. A faixa
			// é validada porque um epoch millis mandado por engano para um
			// campo time seria gravado como uma hora do dia absurda, e a
			// saída NDJSON o renderiza como null — dado perdido em silêncio.
			ms, err := typeparse.Int64(f.Raw)
			if err != nil || ms < 0 || ms >= types.MillisPorDia {
				return types.Value{}, arena, convErrf(target, "millis do dia fora de [0,%d): %s", types.MillisPorDia, truncVal(f.Raw))
			}
			return types.Value{Type: target, I64: ms}, arena, nil
		}
		return types.Value{}, arena, convErrf(target, "esperava string no format declarado ou millis do dia, veio %v", f.Kind)

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
		if typeparse.IntegralNumber(f.Raw) {
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

// truncVal limita o valor interpolado em mensagens de erro (o caminho de
// erro pode receber payloads grandes). Corta em 64 bytes, recuando até um
// início de rune para não partir UTF-8, e marca o corte com reticências.
func truncVal(b []byte) string {
	const max = 64
	if len(b) <= max {
		return string(b)
	}
	end := max
	for end > 0 && !utf8.RuneStart(b[end]) {
		end--
	}
	return string(b[:end]) + "…"
}

func convErrf(target types.FieldType, format string, args ...any) error {
	return &ConversionError{Target: target, Msg: fmt.Sprintf(format, args...)}
}
