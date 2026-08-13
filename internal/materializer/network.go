package materializer

import (
	"encoding/base64"
	"net/netip"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
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
		a, b, c, d, ok := typeparse.IPv4(f.Raw)
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
