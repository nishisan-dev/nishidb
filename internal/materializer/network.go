package materializer

import (
	"encoding/base64"
	"net/netip"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/nishisan-dev/nishidb/internal/unsafex"
)

// convertNetwork trata os alvos ipv4/ipv6/prefixes/bytes (todos exigem
// string JSON e materializam em Value.Bytes no arena).
func convertNetwork(f jsonp.Field, target types.FieldType, arena []byte) (types.Value, []byte, error) {
	if f.Kind != jsonp.KindString {
		return types.Value{}, arena, convErrf(target, "esperava string JSON, veio %v", f.Kind)
	}
	start := len(arena)
	switch target {
	case types.TypeIPv4:
		a, b, c, d, ok := parseIPv4(f.Raw)
		if !ok {
			return types.Value{}, arena, convErrf(target, "ipv4 inválido: %s", f.Raw)
		}
		arena = append(arena, a, b, c, d)

	case types.TypeIPv6:
		// netip.ParseAddr não retém o argumento: unsafex.String é seguro.
		addr, err := netip.ParseAddr(unsafex.String(f.Raw))
		if err != nil || addr.Is4() || addr.Zone() != "" {
			return types.Value{}, arena, convErrf(target, "ipv6 inválido: %s", f.Raw)
		}
		a16 := addr.As16()
		arena = append(arena, a16[:]...)

	case types.TypeIPv4Prefix:
		pfx, err := netip.ParsePrefix(unsafex.String(f.Raw))
		if err != nil || !pfx.Addr().Is4() {
			return types.Value{}, arena, convErrf(target, "prefixo ipv4 inválido: %s", f.Raw)
		}
		a4 := pfx.Addr().As4()
		arena = append(arena, a4[:]...)
		arena = append(arena, byte(pfx.Bits()))

	case types.TypeIPv6Prefix:
		pfx, err := netip.ParsePrefix(unsafex.String(f.Raw))
		if err != nil || pfx.Addr().Is4() {
			return types.Value{}, arena, convErrf(target, "prefixo ipv6 inválido: %s", f.Raw)
		}
		a16 := pfx.Addr().As16()
		arena = append(arena, a16[:]...)
		arena = append(arena, byte(pfx.Bits()))

	case types.TypeBytes:
		var err error
		arena, err = base64.StdEncoding.AppendDecode(arena, f.Raw)
		if err != nil {
			return types.Value{}, arena[:start], convErrf(target, "base64 inválido")
		}
	}
	return types.Value{Type: target, Bytes: arena[start:len(arena):len(arena)]}, arena, nil
}

// parseIPv4 valida dotted-quad estrito: 4 octetos decimais 0-255, sem
// leading zeros (evita ambiguidade octal), sem espaços.
func parseIPv4(raw []byte) (a, b, c, d byte, ok bool) {
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
