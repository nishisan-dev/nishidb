package materializer

import (
	"encoding/base64"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/typeparse"
	"github.com/nishisan-dev/nishidb/internal/types"
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
			return types.Value{}, arena, convErrf(target, "ipv4 inválido: %s", truncVal(f.Raw))
		}
		arena = append(arena, a, b, c, d)

	case types.TypeIPv6:
		a16, ok := typeparse.IPv6(f.Raw)
		if !ok {
			return types.Value{}, arena, convErrf(target, "ipv6 inválido: %s", truncVal(f.Raw))
		}
		arena = append(arena, a16[:]...)

	case types.TypeIPv4Prefix:
		a4, bits, ok := typeparse.IPv4Prefix(f.Raw)
		if !ok {
			return types.Value{}, arena, convErrf(target, "prefixo ipv4 inválido: %s", truncVal(f.Raw))
		}
		arena = append(arena, a4[:]...)
		arena = append(arena, bits)

	case types.TypeIPv6Prefix:
		a16, bits, ok := typeparse.IPv6Prefix(f.Raw)
		if !ok {
			return types.Value{}, arena, convErrf(target, "prefixo ipv6 inválido: %s", truncVal(f.Raw))
		}
		arena = append(arena, a16[:]...)
		arena = append(arena, bits)

	case types.TypeBytes:
		var err error
		arena, err = base64.StdEncoding.AppendDecode(arena, f.Raw)
		if err != nil {
			return types.Value{}, arena[:start], convErrf(target, "base64 inválido")
		}
	}
	return types.Value{Type: target, Bytes: arena[start:len(arena):len(arena)]}, arena, nil
}
