package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Gate de alocação da tarefa Q4: 0 allocs/op no mix representativo de um
// evento RADIUS (strings, ints, timestamps, ips) com arena reutilizado.
func BenchmarkConvertRadiusMix(b *testing.B) {
	type pair struct {
		f      jsonp.Field
		target types.FieldType
	}
	mix := []pair{
		{jsonp.Field{Kind: jsonp.KindString, Raw: []byte("CMP-V0001PPCLP-013")}, types.TypeString},
		{jsonp.Field{Kind: jsonp.KindNumber, Ext: jsonp.ExtNumberLong, Raw: []byte("7455409733")}, types.TypeInt64},
		{jsonp.Field{Kind: jsonp.KindString, Raw: []byte("Interim-Update")}, types.TypeString},
		{jsonp.Field{Kind: jsonp.KindNumber, Raw: []byte("66952")}, types.TypeInt64},
		{jsonp.Field{Kind: jsonp.KindString, Raw: []byte("2026-08-11T22:28:10.000Z")}, types.TypeTimestamp},
		{jsonp.Field{Kind: jsonp.KindNumber, Ext: jsonp.ExtDate, Raw: []byte("1786399690000")}, types.TypeTimestamp},
		{jsonp.Field{Kind: jsonp.KindString, Raw: []byte("100.65.223.104")}, types.TypeIPv4},
		{jsonp.Field{Kind: jsonp.KindString, Raw: []byte("2804:14c::42")}, types.TypeIPv6},
		{jsonp.Field{Kind: jsonp.KindBool, Raw: []byte("true")}, types.TypeBool},
		{jsonp.Field{Kind: jsonp.KindNumber, Raw: []byte("0.75")}, types.TypeDouble},
		{jsonp.Field{Kind: jsonp.KindObject, Raw: []byte(`{"vsa":[1,2]}`)}, types.TypeJSON},
	}
	arena := make([]byte, 0, 1024)
	b.ReportAllocs()
	for b.Loop() {
		arena = arena[:0]
		for _, p := range mix {
			v, a, err := Convert(p.f, p.target, arena)
			if err != nil {
				b.Fatal(err)
			}
			arena = a
			_ = v
		}
	}
}
