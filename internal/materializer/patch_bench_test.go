package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// Evento RADIUS representativo (~30 campos; desconhecidos viram auto no
// warm-up via wildcard+unknownFields:include).
var benchRadiusEvent = []byte(`{"id":"CMP-V0001PPCLP-013","_id":"68992d1a77b6f2618d867d43","_SEQ":{"$numberLong":"7455409733"},"Acct-Authentic":"RADIUS","Acct-Session-Id":"D0D94381DB27B076","Acct-Status-Type":"Interim-Update","Calling-Station-Id":"B4:0F:3B:4E:8F:AA","Class":"srv=hsi&prof=200M","Connect-Info":1000000,"Event-Timestamp":{"$date":"2026-08-11T22:28:10.000Z"},"EventExpireDate":{"$date":{"$numberLong":"1786399690000"}},"Filter-Id":"filter-hsi-200","Framed-IP-Address":"10.65.223.104","Framed-Protocol":"PPP","Idle-Timeout":300,"NAS-IP-Address":"189.40.10.7","NAS-Identifier":"i-br-sp-cmp-cmp-hl4-01","NAS-Port":1063256064,"NAS-Port-Id":"ethernet1/1/1:100.200","NAS-Port-Type":"Ethernet","Service-Type":"Framed-User","Session-Timeout":86400,"User-Name":"user@isp.net.br","Acct-Input-Gigawords":2,"Acct-Input-Octets":66952,"Acct-Input-Packets":48211,"Acct-Output-Gigawords":11,"Acct-Output-Octets":998231,"Acct-Output-Packets":881237,"Acct-Session-Time":73210,"roaming":false,"debug":null}`)

// Gate da Q8: 0 allocs/op no Parse de evento RADIUS em steady state
// (todos os campos já resolvidos no schema/cache).
func BenchmarkPatchParseRadiusEvent(b *testing.B) {
	s, err := schema.Parse([]byte(patchYAML))
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.Compile(fakestorage.NewDict())
	if err != nil {
		b.Fatal(err)
	}
	var pb PatchBuffer
	// Warm-up: resolve o campo desconhecido X-Vendor uma vez.
	if err := pb.Parse(benchRadiusEvent, c); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(benchRadiusEvent)))
	for b.Loop() {
		if err := pb.Parse(benchRadiusEvent, c); err != nil {
			b.Fatal(err)
		}
	}
}
