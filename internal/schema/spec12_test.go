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
		index  IndexKind
	}{
		// nas_ip e ip eram index: hash antes da D5; hoje `index: true` mapeia
		// SEMPRE para IndexBitmap (R3/R4, a regra que intersecta), então os
		// dois saem com o mesmo IndexKind que status e terminate_cause.
		{"nas_ip", "NAS-IP-Address", types.TypeIPv4, IndexBitmap},
		{"status", "Acct-Status-Type", types.TypeString, IndexBitmap},
		{"ip", "Framed-IP-Address", types.TypeIPv4, IndexBitmap},
		{"username", "User-Name", types.TypeString, IndexNone},
		{"service_id", "id", types.TypeString, IndexNone},
		{"connect_info", "Connect-Info", types.TypeInt64, IndexNone},
		{"ipv6_prefix", "Framed-IPv6-Prefix", types.TypeIPv6Prefix, IndexNone},
		{"terminate_cause", "Acct-Terminate-Cause", types.TypeString, IndexBitmap},
		{"NAS-Port-Type", "NAS-Port-Type", types.TypeString, IndexBitmap},
		{"PSA-SERVER", "PSA-SERVER", types.TypeIPv4, IndexNone},
	}
	for _, tc := range checks {
		cf, ok := c.FieldByName([]byte(tc.lookup))
		if !ok {
			t.Errorf("lookup %q não resolve", tc.lookup)
			continue
		}
		if cf.Name != tc.name || cf.Type != tc.typ || cf.Index != tc.index {
			t.Errorf("%q → {Name:%s Type:%v Index:%v}, want {%s %v %v}",
				tc.lookup, cf.Name, cf.Type, cf.Index, tc.name, tc.typ, tc.index)
		}
	}

	// Wildcard + unknownFields include: campo fora do YAML (RemoteServerAddr
	// da amostra real) resolve via ResolveUnknown como auto.
	cf, err := c.ResolveUnknown([]byte("RemoteServerAddr"))
	if err != nil || cf == nil || !cf.IsAuto {
		t.Errorf("ResolveUnknown(RemoteServerAddr) = %+v, %v", cf, err)
	}
}
