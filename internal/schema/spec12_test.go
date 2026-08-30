package schema

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Carrega e compila o radius.materialization.yaml COMPLETO da spec §12
// (testdata/ na raiz, verbatim do documento, ao lado da amostra de evento
// que ele descreve): ~40 campos declarados, source.kafka multi-nível
// (aceito e ignorado no M1), wildcard com unknownFields include, ttl e
// tombstoneRetention.
func TestLoadSpec12YAML(t *testing.T) {
	s, err := Load("../../testdata/radius.materialization.yaml", DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Compile(newFakeDict())
	if err != nil {
		t.Fatal(err)
	}

	if s.Name != "radius_sessions" || s.Materialization.Mode != "merge" {
		t.Errorf("name/mode = %q/%q", s.Name, s.Materialization.Mode)
	}
	if s.Query.MaxRows != 100000 || s.Query.Timeout != 30*time.Second {
		t.Errorf("query = %+v", s.Query)
	}

	// Key e sequence resolvidos.
	if c.Key == nil || c.Key.Name != "id" || c.Key.Type != types.TypeString || !c.Key.IsKey {
		t.Errorf("key = %+v", c.Key)
	}
	if c.Seq == nil || c.Seq.Name != "_SEQ" || c.Seq.Type != types.TypeInt64 {
		t.Errorf("seq = %+v", c.Seq)
	}
	exp, ok := c.FieldByName([]byte("EventExpireDate"))
	if !ok || exp.Type != types.TypeTimestamp {
		t.Errorf("EventExpireDate = %+v (ok=%v)", exp, ok)
	}

	// Aliases e índices representativos do arquivo real.
	checks := []struct {
		lookup string
		name   string
		typ    types.FieldType
		index  bool
	}{
		// nas_ip e ip eram index: hash antes da D5; hoje `index: true` é a única
		// grafia, e os dois saem indexados como status e terminate_cause (era o
		// mesmo IndexKind; hoje o mesmo bool).
		{"nas_ip", "NAS-IP-Address", types.TypeIPv4, true},
		{"status", "Acct-Status-Type", types.TypeString, true},
		{"ip", "Framed-IP-Address", types.TypeIPv4, true},
		{"username", "User-Name", types.TypeString, false},
		{"service_id", "id", types.TypeString, false},
		{"connect_info", "Connect-Info", types.TypeInt64, false},
		{"ipv6_prefix", "Framed-IPv6-Prefix", types.TypeIPv6Prefix, false},
		{"terminate_cause", "Acct-Terminate-Cause", types.TypeString, true},
		{"NAS-Port-Type", "NAS-Port-Type", types.TypeString, true},
		{"PSA-SERVER", "PSA-SERVER", types.TypeIPv4, false},
	}
	for _, tc := range checks {
		cf, ok := c.FieldByName([]byte(tc.lookup))
		if !ok {
			t.Errorf("lookup %q não resolve", tc.lookup)
			continue
		}
		if cf.Name != tc.name || cf.Type != tc.typ || cf.Indexado != tc.index {
			t.Errorf("%q → {Name:%s Type:%v Indexado:%v}, want {%s %v %v}",
				tc.lookup, cf.Name, cf.Type, cf.Indexado, tc.name, tc.typ, tc.index)
		}
	}

	// Wildcard + unknownFields include: campo fora do YAML (RemoteServerAddr
	// da amostra real) resolve via ResolveUnknown como auto.
	cf, err := c.ResolveUnknown([]byte("RemoteServerAddr"))
	if err != nil || cf == nil || !cf.IsAuto {
		t.Errorf("ResolveUnknown(RemoteServerAddr) = %+v, %v", cf, err)
	}
}
