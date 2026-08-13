package jsonp

import "testing"

// Evento RADIUS representativo (~30 campos, com Extended JSON e escapes).
var benchEvent = []byte(`{"id":"CMP-V0001PPCLP-013","_id":"68992d1a77b6f2618d867d43","_SEQ":{"$numberLong":"7455409733"},"Acct-Authentic":"RADIUS","Acct-Session-Id":"D0D94381DB27B076","Acct-Status-Type":"Interim-Update","Calling-Station-Id":"B4:0F:3B:4E:8F:AA","Class":"srv=hsi&prof=200M","Connect-Info":1000000,"Event-Timestamp":{"$date":"2026-08-11T22:28:10.000Z"},"EventExpireDate":{"$date":{"$numberLong":"1786399690000"}},"Filter-Id":"filter-hsi-200","Framed-IP-Address":"100.65.223.104","Framed-Protocol":"PPP","Idle-Timeout":300,"NAS-IP-Address":"189.40.10.7","NAS-Identifier":"i-br-sp-cmp-cmp-hl4-01","NAS-Port":1063256064,"NAS-Port-Id":"ethernet1/1/1:100.200","NAS-Port-Type":"Ethernet","Service-Type":"Framed-User","Session-Timeout":86400,"User-Name":"user@isp.net.br","Acct-Input-Gigawords":2,"Acct-Input-Octets":66952,"Acct-Input-Packets":48211,"Acct-Output-Gigawords":11,"Acct-Output-Octets":998231,"Acct-Output-Packets":881237,"Acct-Session-Time":73210,"roaming":false,"debug":null}`)

// Gate de alocação da tarefa Q3: 0 allocs/op em steady state.
func BenchmarkParseRadiusEvent(b *testing.B) {
	var p Parser
	b.ReportAllocs()
	b.SetBytes(int64(len(benchEvent)))
	for b.Loop() {
		p.Reset(benchEvent)
		for {
			f, ok, err := p.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			_ = f
		}
	}
}
