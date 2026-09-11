package server_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// oneRowOfKafka roda a consulta e devolve a única linha, já decodificada.
func oneRowOfKafka(t *testing.T, srv *server.Server, dataset, sql string) map[string]any {
	t.Helper()
	resp := consulta(t, srv, `{"dataset":"`+dataset+`","sql":"`+sql+`"}`)
	rows, end := result(t, resp)
	if end.End() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	if len(rows) != 1 {
		t.Fatalf("%d linhas para %q, quero 1: %v", len(rows), sql, rows)
	}
	var l map[string]any
	if err := json.Unmarshal([]byte(rows[0]), &l); err != nil {
		t.Fatalf("linha ilegível: %v", err)
	}
	return l
}

// TestMetaKafkaRespondeNoDatasetSemIngestao: o dataset do fixture não declara
// Kafka, e a tabela responde a LINHA DE RESUMO — nunca erro de tabela
// desconhecida. "Não consome" é uma resposta.
func TestMetaKafkaRespondeNoDatasetSemIngestao(t *testing.T) {
	srv := serverWithData(t, 4, blockPadrao)
	l := oneRowOfKafka(t, srv, "d",
		"SELECT id, enabled, state, partition, queueRecords, watchdogTrips FROM _meta.kafka")
	if l["id"] != "source" {
		t.Fatalf("id = %v, quero \"source\"", l["id"])
	}
	if l["enabled"] != false {
		t.Fatalf("enabled = %v; sem bloco Kafka o dataset não consome", l["enabled"])
	}
	if l["state"] != protocol.StateDisabled {
		t.Fatalf("state = %v, quero %q", l["state"], protocol.StateDisabled)
	}
	// Ausência, e não zero: não há partição, não há fila para medir e não há
	// watchdog. As três respostas são diferentes de "medi e deu zero".
	for _, col := range []string{"partition", "queueRecords", "watchdogTrips"} {
		if v, presente := l[col]; !presente || v != nil {
			t.Fatalf("%s = %v (presente=%v), quero null", col, v, presente)
		}
	}
}

// TestEstadoDaTabelaEhOMesmoDoHealth: `state` sai da MESMA classificação que o
// /v1/health e o /v1/stats usam. Uma segunda leitura do YAML ao lado dela
// descreveria a mesma realidade com outra palavra no dia em que uma das duas
// mudasse.
func TestEstadoDaTabelaEhOMesmoDoHealth(t *testing.T) {
	srv := serverWithData(t, 4, blockPadrao)

	var stats protocol.Stats
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/stats")), &stats); err != nil {
		t.Fatal(err)
	}
	var ofStats string
	for _, d := range stats.Datasets {
		if d.Name == "d" {
			ofStats = d.Ingestion.State
		}
	}
	if ofStats == "" {
		t.Fatal("dataset ausente do /v1/stats")
	}
	l := oneRowOfKafka(t, srv, "d", "SELECT state FROM _meta.kafka")
	if l["state"] != ofStats {
		t.Fatalf("a tabela diz %v e o /v1/stats diz %q", l["state"], ofStats)
	}
}

// TestMetaKafkaNaoExisteNaTabelaIrma: a ingestão da irmã é a do dataset base.
// A recusa nomeia o que existe lá, e é erro do cliente — não falha interna.
func TestMetaKafkaNaoExisteNaTabelaIrma(t *testing.T) {
	srv := serverWithHistory(t, specWithHistory)
	// No dataset base a tabela responde.
	if l := oneRowOfKafka(t, srv, "eventos", "SELECT id FROM _meta.kafka"); l["id"] != "source" {
		t.Fatalf("o dataset base tinha de responder: %v", l)
	}
	resp := consulta(t, srv, `{"dataset":"eventos_history","sql":"SELECT id FROM _meta.kafka"}`)
	defer fecha(t, resp)
	if resp.StatusCode == 200 {
		t.Fatal("a irmã não tem fonte própria; a consulta tinha de ser recusada")
	}
	f := failOf(t, resp)
	if !strings.Contains(f.Message, "fields") {
		t.Fatalf("a recusa tem de nomear o que existe na irmã: %s", f.Message)
	}
}

// TestMetaKafkaTrazAIdentidadeDoYAML: com bloco Kafka declarado — ainda que
// desligado —, tópico, grupo e bootstrap saem. É o que permite conferir para
// onde o dataset apontaria se fosse religado, sem abrir o YAML no servidor.
func TestMetaKafkaTrazAIdentidadeDoYAML(t *testing.T) {
	a := newEnvWith(t, blockPadrao)
	a.writeDataset("eventos", specWithKafkaDesligado)
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	l := oneRowOfKafka(t, srv, "eventos",
		"SELECT topic, groupId, bootstrap, enabled, state FROM _meta.kafka")
	if l["topic"] != "service-view" || l["groupId"] != "piloto-7" {
		t.Fatalf("identidade = %v", l)
	}
	// Canonizado: ordenado, na mesma forma que a origem grava no kafka.offsets.
	if l["bootstrap"] != "kafka01:9092,kafka02:9092" {
		t.Fatalf("bootstrap = %v, quero a forma canônica", l["bootstrap"])
	}
	if l["enabled"] != false || l["state"] != protocol.StateDisabled {
		t.Fatalf("enabled=%v state=%v", l["enabled"], l["state"])
	}
}

const specWithKafkaDesligado = `version: 1
name: eventos
source:
  type: kafka
  kafka:
    enabled: false
    bootstrapServers:
      - kafka02:9092
      - kafka01:9092
    topic: service-view
    consumer:
      groupId: piloto-7
materialization:
  key:
    field: id
schema:
  include: ["*"]
  fields:
    id:
      type: string
`
