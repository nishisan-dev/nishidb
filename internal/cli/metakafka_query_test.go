package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// specWithKafkaCLI declara a fonte Kafka, LIGADA. O `nishidb query --config`
// roda com o servidor parado e sem consumidor nenhum — é justamente o caso em
// que a tabela tem de responder com o que existe, sem inventar o que não
// observa.
const specWithKafkaCLI = `version: 1
name: eventos
source:
  type: kafka
  kafka:
    enabled: true
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
  fields:
    id:
      type: string
`

func writeDatasetWithKafka(t *testing.T, a *env, nome, spec string) {
	t.Helper()
	specs := filepath.Join(a.dir, "datasets")
	if err := os.WriteFile(filepath.Join(specs, nome+".yaml"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestQueryLocalRespondeMetaKafkaComOQueExiste: sem servidor e sem consumidor,
// a identidade do fluxo sai do YAML e as colunas vivas são NULL.
//
// O `state` também é NULL, e é a asserção mais importante daqui: este processo
// não observa ingestão nenhuma, e responder UP, DOWN ou DEGRADED seria inventar
// um estado a partir de um arquivo de configuração.
func TestQueryLocalRespondeMetaKafkaComOQueExiste(t *testing.T) {
	a := newEnv(t)
	writeDatasetWithKafka(t, a, "eventos", specWithKafkaCLI)

	code := a.run("query", "--config", a.config, "--dataset", "eventos",
		"SELECT id, topic, groupId, bootstrap, enabled, state, queueRecords, appliedOffset FROM _meta.kafka")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSpace(a.stdout.String()), "\n")
	if len(rows) != 1 {
		t.Fatalf("%d linhas, quero 1 (a de resumo):\n%s", len(rows), a.stdout.String())
	}
	l := rows[0]
	for _, quer := range []string{
		`"id":"source"`,
		`"topic":"service-view"`,
		`"groupId":"piloto-7"`,
		`"bootstrap":"kafka01:9092,kafka02:9092"`, // canonizado: ordenado
		`"enabled":true`,
		`"state":null`,
		`"queueRecords":null`,
		`"appliedOffset":null`,
	} {
		if !strings.Contains(l, quer) {
			t.Fatalf("faltou %s em %s", quer, l)
		}
	}
}

// TestQueryLocalMetaKafkaSemKafkaDizDesligado: sem bloco Kafka, o veredito não
// depende de processo nenhum — o YAML o prova sozinho.
func TestQueryLocalMetaKafkaSemKafkaDizDesligado(t *testing.T) {
	a := newEnv(t)
	writeDatasetServices(t, a)

	code := a.run("query", "--config", a.config, "--dataset", "servicos",
		"SELECT id, enabled, state, topic FROM _meta.kafka")
	if code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	l := strings.TrimSpace(a.stdout.String())
	for _, quer := range []string{`"enabled":false`, `"state":"DISABLED"`, `"topic":null`} {
		if !strings.Contains(l, quer) {
			t.Fatalf("faltou %s em %s", quer, l)
		}
	}
}
