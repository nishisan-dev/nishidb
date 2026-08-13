package materializer

import (
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// parseRFC3339Millis converte um timestamp RFC 3339 (spec §2.3 do design:
// único formato string aceito na v1, sem heurísticas de epoch-seconds) para
// epoch millis UTC. Parser próprio sobre []byte para manter o hot path sem
// alocações — time.Parse exigiria conversão para string e é mais caro.
//
// Formato: YYYY-MM-DDTHH:MM:SS[.f{1,9}](Z|±hh:mm), com T/Z aceitos também
// em minúsculas. Frações além de millis são truncadas.
func parseRFC3339Millis(raw []byte) (int64, error) {
	// Comprimento mínimo: "YYYY-MM-DDTHH:MM:SSZ" = 20 bytes.
	if len(raw) < 20 {
		return 0, convErrf(types.TypeTimestamp, "timestamp curto demais: %s", raw)
	}
	year, ok1 := digits4(raw[0:4])
	month, ok2 := digits2(raw[5:7])
	day, ok3 := digits2(raw[8:10])
	hour, ok4 := digits2(raw[11:13])
	minute, ok5 := digits2(raw[14:16])
	sec, ok6 := digits2(raw[17:19])
	sepsOK := raw[4] == '-' && raw[7] == '-' &&
		(raw[10] == 'T' || raw[10] == 't') &&
		raw[13] == ':' && raw[16] == ':'
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && sepsOK) {
		return 0, convErrf(types.TypeTimestamp, "formato RFC3339 inválido: %s", raw)
	}
	if month < 1 || month > 12 || hour > 23 || minute > 59 || sec > 59 {
		return 0, convErrf(types.TypeTimestamp, "componente fora de range: %s", raw)
	}

	pos := 19
	millis := 0
	if pos < len(raw) && raw[pos] == '.' {
		pos++
		fracStart := pos
		for pos < len(raw) && raw[pos] >= '0' && raw[pos] <= '9' {
			pos++
		}
		nfrac := pos - fracStart
		if nfrac < 1 || nfrac > 9 {
			return 0, convErrf(types.TypeTimestamp, "fração inválida: %s", raw)
		}
		// Só os 3 primeiros dígitos contam (millis); o resto é truncado.
		for i := 0; i < 3; i++ {
			millis *= 10
			if i < nfrac {
				millis += int(raw[fracStart+i] - '0')
			}
		}
	}

	if pos >= len(raw) {
		return 0, convErrf(types.TypeTimestamp, "timezone ausente: %s", raw)
	}
	offsetSec := 0
	switch c := raw[pos]; {
	case c == 'Z' || c == 'z':
		pos++
	case c == '+' || c == '-':
		if len(raw)-pos != 6 || raw[pos+3] != ':' {
			return 0, convErrf(types.TypeTimestamp, "offset inválido: %s", raw)
		}
		oh, okh := digits2(raw[pos+1 : pos+3])
		om, okm := digits2(raw[pos+4 : pos+6])
		if !okh || !okm || oh > 23 || om > 59 {
			return 0, convErrf(types.TypeTimestamp, "offset fora de range: %s", raw)
		}
		offsetSec = oh*3600 + om*60
		if c == '-' {
			offsetSec = -offsetSec
		}
		pos += 6
	default:
		return 0, convErrf(types.TypeTimestamp, "timezone inválido: %s", raw)
	}
	if pos != len(raw) {
		return 0, convErrf(types.TypeTimestamp, "conteúdo após timezone: %s", raw)
	}

	// time.Date normaliza componentes inválidos (ex.: 30/02 → 02/03), então
	// a data é validada comparando o resultado com os componentes de entrada.
	t := time.Date(year, time.Month(month), day, hour, minute, sec, 0, time.UTC)
	y2, m2, d2 := t.Date()
	if y2 != year || int(m2) != month || d2 != day {
		return 0, convErrf(types.TypeTimestamp, "data inexistente: %s", raw)
	}
	return (t.Unix()-int64(offsetSec))*1000 + int64(millis), nil
}

func digits2(b []byte) (int, bool) {
	if b[0] < '0' || b[0] > '9' || b[1] < '0' || b[1] > '9' {
		return 0, false
	}
	return int(b[0]-'0')*10 + int(b[1]-'0'), true
}

func digits4(b []byte) (int, bool) {
	hi, ok1 := digits2(b[0:2])
	lo, ok2 := digits2(b[2:4])
	if !ok1 || !ok2 {
		return 0, false
	}
	return hi*100 + lo, true
}
