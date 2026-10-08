package server_test

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// specAnchorYAML termina numa regra de fundo `never`: duração zero, e mesmo
// assim fundo — o caso que o log de subida errava ao inferir o fundo da
// duração.
const specAnchorYAML = `
version: 1
name: %s
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - anchor: lastAuthAt
      expireAfter: 48h
    - expireAfterWrite: never
schema:
  fields:
    id:
      type: string
    lastAuthAt:
      type: timestamp
`

func serverWithAnchor(t *testing.T, record *recordOfServer) *server.Server {
	t.Helper()
	a := newEnvWith(t, blockPadrao)
	a.writeDataset("d", strings.ReplaceAll(specAnchorYAML, "%s", "d"))
	srv, err := server.Novo(a.load(), server.Options{Log: logging.New(record, logging.FormatJSON, slog.LevelInfo)})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}

// A costura do /v1/stats e do /metrics com a regra ancorada: `expireAfterWrite`
// presente e VAZIO, a âncora e a duração sob as chaves próprias, e as séries
// novas no scrape.
func TestStatsAndMetricsPublishTheAnchoredRule(t *testing.T) {
	srv := serverWithAnchor(t, &recordOfServer{})

	var doc struct {
		Datasets []struct {
			Materialization struct {
				TTLRules          []map[string]any `json:"ttlRules"`
				TTLAnchorInFuture *int64           `json:"ttlAnchorInFuture"`
				TTLExpiredOnWrite *int64           `json:"ttlExpiredOnWrite"`
			} `json:"materialization"`
		} `json:"datasets"`
	}
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/stats")), &doc); err != nil {
		t.Fatalf("/v1/stats ilegível: %v", err)
	}
	m := doc.Datasets[0].Materialization
	if len(m.TTLRules) != 2 {
		t.Fatalf("%d regras, quero 2", len(m.TTLRules))
	}
	r := m.TTLRules[0]
	if r["anchor"] != "lastAuthAt" || r["expireAfter"] != "48h0m0s" || r["anchorAbsent"] != float64(0) {
		t.Errorf("regra ancorada = %v", r)
	}
	if v, ok := r["expireAfterWrite"]; !ok || v != "" {
		t.Errorf("expireAfterWrite da regra ancorada = %v (presente=%v), quero presente e vazio", v, ok)
	}
	if fb := m.TTLRules[1]; fb["expireAfterWrite"] != "never" || fb["anchor"] != nil || fb["anchorAbsent"] != nil {
		t.Errorf("regra de fundo = %v", fb)
	}
	if m.TTLAnchorInFuture == nil || m.TTLExpiredOnWrite == nil {
		t.Errorf("contadores da âncora ausentes do documento: %+v", m)
	}

	corpo := fetch(t, srv, "/metrics")
	for _, serie := range []string{
		`nishidb_ttl_anchor_absent_total{rule="0"`,
		"nishidb_ttl_anchor_in_future_total{",
		"nishidb_ttl_expired_on_write_total{",
	} {
		if !strings.Contains(corpo, serie) {
			t.Errorf("/metrics não publica %s", serie)
		}
	}
	if strings.Contains(corpo, `nishidb_ttl_anchor_absent_total{rule="1"`) {
		t.Error("/metrics publica âncora ausente para a regra de fundo")
	}
}

// O log de subida diz que há fundo quando a última regra é um fundo `never`:
// inferir o fundo de FallbackDuration > 0 dizia que não havia.
func TestStartupLogSeesNeverFallback(t *testing.T) {
	record := &recordOfServer{}
	serverWithAnchor(t, record)
	var line string
	waitUntil(t, func() bool {
		for _, l := range strings.Split(record.String(), "\n") {
			if strings.Contains(l, `"ttl.regras"`) {
				line = l
				return true
			}
		}
		return false
	}, "o log ttl.regras não saiu em 30s")
	if !strings.Contains(line, `"temFundo":true`) {
		t.Errorf("log de subida sem temFundo=true com fundo never: %s", line)
	}
	if !strings.Contains(line, `[0] anchor=lastAuthAt expireAfter=48h0m0s; [1] fallback never`) {
		t.Errorf("descrição das regras = %s", line)
	}
}
