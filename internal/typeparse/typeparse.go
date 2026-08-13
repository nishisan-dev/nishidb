// Package typeparse reúne os parsers de valores compartilhados entre o
// materializer (coerção JSON→binário) e o binder SQL (coerção de literais e
// parâmetros). É um pacote folha: além da stdlib, só internal/unsafex.
//
// Manter os dois consumidores sobre ESTES parsers é mitigação direta do
// risco R3 do plano M1 (coerção SQL divergindo da coerção de ingestão):
// um valor que entra por aqui e um literal de query comparado a ele devem
// ser interpretados byte a byte da mesma forma.
//
// Nenhuma função retém o slice recebido. Erros são estáticos (zero alloc)
// e respondem a errors.Is(err, ErrParse); o texto não inclui o valor de
// entrada — quem formata mensagens (e trunca o valor) é o consumidor.
package typeparse

import (
	"errors"
	"math"
	"net/netip"
	"strconv"
	"time"

	"github.com/nishisan-dev/nishidb/internal/unsafex"
)

// ErrParse é a categoria comum de todos os erros deste pacote.
var ErrParse = errors.New("valor não parseável")

type parseErr string

func (e parseErr) Error() string { return string(e) }
func (e parseErr) Unwrap() error { return ErrParse }

var (
	errIntFrac     = parseErr("número com fração/expoente")
	errIntEmpty    = parseErr("número vazio")
	errIntDigit    = parseErr("dígito inválido")
	errIntOverflow = parseErr("overflow de int64")

	errFloatSyntax = parseErr("número inválido")

	errTsShort       = parseErr("timestamp curto demais")
	errTsFormat      = parseErr("formato RFC3339 inválido")
	errTsRange       = parseErr("componente fora de range")
	errTsFrac        = parseErr("fração inválida")
	errTsNoTZ        = parseErr("timezone ausente")
	errTsOffset      = parseErr("offset inválido")
	errTsOffsetRange = parseErr("offset fora de range")
	errTsTZ          = parseErr("timezone inválido")
	errTsTrailing    = parseErr("conteúdo após timezone")
	errTsDate        = parseErr("data inexistente")
)

// IntegralNumber reporta se um número JSON já validado é inteiro (sem
// fração nem expoente).
func IntegralNumber(b []byte) bool {
	for _, c := range b {
		if c == '.' || c == 'e' || c == 'E' {
			return false
		}
	}
	return true
}

// Int64 converte dígitos decimais com sinal opcional (-?[0-9]+) com check
// de overflow. Não usa strconv para evitar a conversão []byte→string.
func Int64(b []byte) (int64, error) {
	if !IntegralNumber(b) {
		return 0, errIntFrac
	}
	i := 0
	neg := false
	if len(b) > 0 && b[0] == '-' {
		neg = true
		i++
	}
	if i >= len(b) {
		return 0, errIntEmpty
	}
	// Acumula como negativo (o range de int64 é assimétrico: MinInt64 não
	// tem par positivo).
	var n int64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, errIntDigit
		}
		d := int64(c - '0')
		if n < (math.MinInt64+d)/10 {
			return 0, errIntOverflow
		}
		n = n*10 - d
	}
	if !neg {
		if n == math.MinInt64 {
			return 0, errIntOverflow
		}
		n = -n
	}
	return n, nil
}

// Float64 converte um número na gramática de número JSON (RFC 8259):
// -?(0|[1-9][0-9]*)(.[0-9]+)?([eE][+-]?[0-9]+)?. A gramática é imposta
// ANTES do strconv.ParseFloat porque este aceita formas que a ingestão
// nunca produziria (NaN, Inf, hex float, underscores) — e a coerção de
// strings declaradas como double deve se comportar byte a byte como um
// número que entrasse cru no JSON. ParseFloat não retém o argumento, então
// a conversão via unsafex.String é segura.
func Float64(b []byte) (float64, error) {
	if !JSONNumber(b) {
		return 0, errFloatSyntax
	}
	d, err := strconv.ParseFloat(unsafex.String(b), 64)
	if err != nil {
		return 0, errFloatSyntax
	}
	return d, nil
}

// JSONNumber reporta se b casa exatamente a gramática de número do RFC
// 8259 (sem espaços, sem sinal +, sem leading zeros, sem NaN/Inf/hex).
func JSONNumber(b []byte) bool {
	i := 0
	if i < len(b) && b[i] == '-' {
		i++
	}
	switch {
	case i < len(b) && b[i] == '0':
		i++
	case i < len(b) && b[i] >= '1' && b[i] <= '9':
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	default:
		return false
	}
	if i < len(b) && b[i] == '.' {
		i++
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(b)
}

// RFC3339Millis converte um timestamp RFC 3339 (único formato string aceito
// na v1, sem heurísticas de epoch-seconds) para epoch millis UTC. Parser
// próprio sobre []byte para manter o hot path sem alocações — time.Parse
// exigiria conversão para string e é mais caro.
//
// Formato: YYYY-MM-DDTHH:MM:SS[.f{1,9}](Z|±hh:mm), com T/Z aceitos também
// em minúsculas. Frações além de millis são truncadas.
func RFC3339Millis(raw []byte) (int64, error) {
	// Comprimento mínimo: "YYYY-MM-DDTHH:MM:SSZ" = 20 bytes.
	if len(raw) < 20 {
		return 0, errTsShort
	}
	year, ok1 := Digits4(raw[0:4])
	month, ok2 := Digits2(raw[5:7])
	day, ok3 := Digits2(raw[8:10])
	hour, ok4 := Digits2(raw[11:13])
	minute, ok5 := Digits2(raw[14:16])
	sec, ok6 := Digits2(raw[17:19])
	sepsOK := raw[4] == '-' && raw[7] == '-' &&
		(raw[10] == 'T' || raw[10] == 't') &&
		raw[13] == ':' && raw[16] == ':'
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && sepsOK) {
		return 0, errTsFormat
	}
	if month < 1 || month > 12 || hour > 23 || minute > 59 || sec > 59 {
		return 0, errTsRange
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
			return 0, errTsFrac
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
		return 0, errTsNoTZ
	}
	offsetSec := 0
	switch c := raw[pos]; {
	case c == 'Z' || c == 'z':
		pos++
	case c == '+' || c == '-':
		if len(raw)-pos != 6 || raw[pos+3] != ':' {
			return 0, errTsOffset
		}
		oh, okh := Digits2(raw[pos+1 : pos+3])
		om, okm := Digits2(raw[pos+4 : pos+6])
		if !okh || !okm || oh > 23 || om > 59 {
			return 0, errTsOffsetRange
		}
		offsetSec = oh*3600 + om*60
		if c == '-' {
			offsetSec = -offsetSec
		}
		pos += 6
	default:
		return 0, errTsTZ
	}
	if pos != len(raw) {
		return 0, errTsTrailing
	}

	// time.Date normaliza componentes inválidos (ex.: 30/02 → 02/03), então
	// a data é validada comparando o resultado com os componentes de entrada.
	t := time.Date(year, time.Month(month), day, hour, minute, sec, 0, time.UTC)
	y2, m2, d2 := t.Date()
	if y2 != year || int(m2) != month || d2 != day {
		return 0, errTsDate
	}
	return (t.Unix()-int64(offsetSec))*1000 + int64(millis), nil
}

// IPv4 valida dotted-quad estrito: 4 octetos decimais 0-255, sem leading
// zeros (evita ambiguidade octal), sem espaços.
func IPv4(raw []byte) (a, b, c, d byte, ok bool) {
	var oct [4]int
	pos := 0
	for i := 0; i < 4; i++ {
		if i > 0 {
			if pos >= len(raw) || raw[pos] != '.' {
				return 0, 0, 0, 0, false
			}
			pos++
		}
		start := pos
		v := 0
		for pos < len(raw) && raw[pos] >= '0' && raw[pos] <= '9' {
			v = v*10 + int(raw[pos]-'0')
			pos++
			if v > 255 {
				return 0, 0, 0, 0, false
			}
		}
		n := pos - start
		if n == 0 || (n > 1 && raw[start] == '0') {
			return 0, 0, 0, 0, false
		}
		oct[i] = v
	}
	if pos != len(raw) {
		return 0, 0, 0, 0, false
	}
	return byte(oct[0]), byte(oct[1]), byte(oct[2]), byte(oct[3]), true
}

// IPv6 valida um endereço IPv6 textual e devolve os 16 bytes em network
// order.
//
// Os critérios são os MESMOS da ingestão, e cada um tem uma razão:
//
//   - endereço v4 puro é RECUSADO. netip.ParseAddr("10.0.0.1") tem sucesso
//     com Is4() == true, e aceitá-lo produziria 16 bytes de v4-mapped que a
//     ingestão nunca gravou — toda comparação daria false em silêncio;
//   - zona (%eth0) é RECUSADA: não faz parte do valor armazenado;
//   - ::ffff:1.2.3.4 (Is4In6) é ACEITO — é um endereço v6 legítimo e a
//     ingestão sempre o aceitou.
func IPv6(raw []byte) (addr [16]byte, ok bool) {
	// netip.ParseAddr não retém o argumento: unsafex.String é seguro.
	a, err := netip.ParseAddr(unsafex.String(raw))
	if err != nil || a.Is4() || a.Zone() != "" {
		return [16]byte{}, false
	}
	return a.As16(), true
}

// IPv4Prefix valida um prefixo IPv4 textual (a.b.c.d/bits) e devolve os 4
// bytes do endereço e o comprimento do prefixo.
//
// Os host bits NÃO são mascarados: 10.0.0.1/24 continua 10.0.0.1/24. É o que
// netip.ParsePrefix faz e o que a ingestão gravou — mascarar aqui faria a
// query procurar bytes diferentes dos armazenados.
//
// A zona (%eth0) é recusada, como no IPv6 solto, mas quem a recusa é o próprio
// netip.ParsePrefix ("IPv6 zones cannot be present in a prefix") — não há
// checagem nossa a acrescentar, e uma seria código morto. A recusa está
// coberta por teste justamente porque depende de comportamento de terceiro.
func IPv4Prefix(raw []byte) (addr [4]byte, bits byte, ok bool) {
	pfx, err := netip.ParsePrefix(unsafex.String(raw))
	if err != nil || !pfx.Addr().Is4() {
		return [4]byte{}, 0, false
	}
	return pfx.Addr().As4(), byte(pfx.Bits()), true
}

// IPv6Prefix é o análogo de IPv4Prefix para IPv6: 16 bytes mais o
// comprimento, host bits preservados, prefixo v4 recusado.
func IPv6Prefix(raw []byte) (addr [16]byte, bits byte, ok bool) {
	pfx, err := netip.ParsePrefix(unsafex.String(raw))
	if err != nil || pfx.Addr().Is4() {
		return [16]byte{}, 0, false
	}
	return pfx.Addr().As16(), byte(pfx.Bits()), true
}

// Digits2 lê exatamente 2 dígitos decimais; b deve ter len ≥ 2.
func Digits2(b []byte) (int, bool) {
	if b[0] < '0' || b[0] > '9' || b[1] < '0' || b[1] > '9' {
		return 0, false
	}
	return int(b[0]-'0')*10 + int(b[1]-'0'), true
}

// Digits4 lê exatamente 4 dígitos decimais; b deve ter len ≥ 4.
func Digits4(b []byte) (int, bool) {
	hi, ok1 := Digits2(b[0:2])
	lo, ok2 := Digits2(b[2:4])
	if !ok1 || !ok2 {
		return 0, false
	}
	return hi*100 + lo, true
}
