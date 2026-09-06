package server_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/server"
)

// O dataset usa as três políticas que o bloco publica, mais as regras de TTL.
const specPoliticasYAML = `
version: 1
name: %s
materialization:
  key:
    field: id
  ttl:
    - when: "tipo = 2"
      expireAfterWrite: 3m
    - expireAfterWrite: 7d
schema:
  fields:
    id:
      type: string
    tipo:
      type: int64
    contador:
      type: int64
      merge: sum
    origTipo:
      frozenFrom: tipo
`

// A costura importa tanto quanto a tradução: uma função que monta contadores e
// que ninguém chama vale o mesmo que contador nenhum — que é o defeito que
// estas séries existem para fechar.
func TestStatsPublicaOBlocoDeMaterializacao(t *testing.T) {
	srv := servidorComPoliticas(t)

	var doc struct {
		Datasets []struct {
			Name            string `json:"name"`
			Materialization struct {
				FrozenCaptured  int64 `json:"frozenCaptured"`
				Summed          int64 `json:"summed"`
				SummedSaturated int64 `json:"summedSaturated"`
				TTLRules        []struct {
					Index            int    `json:"index"`
					When             string `json:"when"`
					ExpireAfterWrite string `json:"expireAfterWrite"`
					Matched          int64  `json:"matched"`
				} `json:"ttlRules"`
			} `json:"materialization"`
		} `json:"datasets"`
	}
	if err := json.Unmarshal([]byte(pega(t, srv, "/v1/stats")), &doc); err != nil {
		t.Fatalf("/v1/stats ilegível: %v", err)
	}
	if len(doc.Datasets) != 1 {
		t.Fatalf("%d datasets", len(doc.Datasets))
	}
	m := doc.Datasets[0].Materialization
	if len(m.TTLRules) != 2 {
		t.Fatalf("%d regras publicadas, quero 2: %+v", len(m.TTLRules), m)
	}
	if m.TTLRules[0].When != "tipo = 2" || m.TTLRules[0].ExpireAfterWrite != "3m0s" {
		t.Errorf("regra 0 = %+v", m.TTLRules[0])
	}
	if m.TTLRules[1].When != "" || m.TTLRules[1].ExpireAfterWrite != "168h0m0s" {
		t.Errorf("regra de fundo = %+v", m.TTLRules[1])
	}
}

func TestMetricsPublicaAsSeriesDeMaterializacao(t *testing.T) {
	srv := servidorComPoliticas(t)
	corpo := pega(t, srv, "/metrics")

	for _, serie := range []string{
		"nishidb_frozen_captured_total",
		"nishidb_frozen_preserved_total",
		"nishidb_merge_sum_total",
		"nishidb_merge_sum_saturated_total",
		"nishidb_ttl_unmatched_total",
		`nishidb_ttl_rule_matched_total{rule="0"`,
		`nishidb_ttl_rule_matched_total{rule="1"`,
	} {
		if !strings.Contains(corpo, serie) {
			t.Errorf("/metrics não publica %s", serie)
		}
	}
}

func servidorComPoliticas(t *testing.T) *server.Servidor {
	t.Helper()
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("d", strings.ReplaceAll(specPoliticasYAML, "%s", "d"))
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}
