package server_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// specServicesWithWindowMetricsAndValue é specServicesWithWindowMetrics com
// `value: auth_24h_max` no alarme: o valor de disparo (12, de uma coluna
// `_max` — portanto int64 na ORIGEM) sai DIFERENTE de zero, que é o que
// distingue os bits de double dos de int64 na coluna `firingValue`. O default
// (`total`) dispararia com 0, e 0 tem os MESMOS bits nos dois tipos — não
// provaria a conversão (math.Float64bits) que `internal/alarmtable` faz.
const specServicesWithWindowMetricsAndValue = `version: 1
name: servicos
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    metrics:
      - name: auth_5m
        type: window
        source: total
        window: 5m
        aggregates: [avg]
      - name: auth_24h
        type: window
        source: total
        window: 24h
        aggregates: [max]
    alarms:
      - name: primary_quase_vazia
        qualifyWhen: "auth_24h_max > 10"
        when: "auth_5m_avg < 1"
        sustainedFor: 5m
        value: auth_24h_max
`

// secondSchemaDataset e specSecondSchemaAlarmObservesOltZ são o SEGUNDO
// dataset do teste de isolamento por schema: schema PRÓPRIO ("outro"), com um
// alarme SEM qualifyWhen que entra em observação (pending) já na PRIMEIRA
// passada — não precisa do ciclo de reinício que o disparo do outro schema
// exige, e mesmo assim já basta para provar o que se quer: o `FROM alarms` NU
// numa conexão nunca abre o `alarms` de outro schema.
const secondSchemaDataset = "servicos2"

const specSecondSchemaAlarmObservesOltZ = `version: 1
name: ` + secondSchemaDataset + `
schemaName: outro
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
counters:
  - name: autenticados_por_olt2
    groupBy: [olt]
    where: authenticated = true
    alarms:
      - name: sempre_baixo
        when: "total < 1000"
        sustainedFor: 1m
`

// queryRowsInSchema é o queryRows (counter_metrics_test.go) da CONEXÃO POR
// SCHEMA: o corpo leva "schema", não "dataset" (queryBody, de
// connection_schema_test.go) — é essa forma que resolve `FROM alarms` sem
// qualificar, e o que prova a promessa central do plano.
func queryRowsInSchema(t *testing.T, srv *server.Server, schemaName, sql string) []map[string]any {
	t.Helper()
	resp := consulta(t, srv, queryBody(schemaName, sql))
	if resp.StatusCode != http.StatusOK {
		defer fecha(t, resp)
		t.Fatalf("%s (schema %s): status %d: %+v", sql, schemaName, resp.StatusCode, failOf(t, resp))
	}
	lines, _ := result(t, resp)
	out := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("linha não é JSON: %v: %s", err, line)
		}
		out = append(out, row)
	}
	return out
}

// driveSecondSchema aguarda o laço de alarmes do segundo schema subir, o para
// e conduz uma passada em `at` — a MESMA disciplina de drivePass, por conta
// própria porque startDrivenServer/runDriven só dirigem o windowDataset.
func driveSecondSchema(t *testing.T, srv *server.Server, clock *testClock, at time.Time) {
	t.Helper()
	for i := 0; i < 400 && !srv.AlarmsActive(secondSchemaDataset); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if !srv.AlarmsActive(secondSchemaDataset) {
		t.Fatalf("o laço de alarmes do segundo schema não subiu")
	}
	srv.StopSamplingAndAlarmsForTest(secondSchemaDataset)
	clock.set(at)
	if err := srv.EvaluateAlarmsOnceForTest(secondSchemaDataset); err != nil {
		t.Fatalf("varredura do segundo schema em %s: %v", at, err)
	}
}

// TestAlarmsViewIsPublishedInTheSchemaAndCarriesTheFiringValue é a costura de
// ponta a ponta da view: servidor real, HTTP real, disparo real.
//
//   - /v1/metadata?schema=<s> publica `alarms` como VIEW sob o namespace
//     `_counters`, com as três colunas novas;
//   - pela conexão por schema, `FROM alarms` e `FROM telecom.alarms` abrem;
//   - `_meta.alarms` é recusada com BIND — a tabela foi removida de lá;
//   - num disparo de verdade, a linha traz state=firing, firedAt não nulo,
//     valueColumn=auth_24h_max e firingValue=12 — o double correto, não os
//     bits de um int64 reinterpretados;
//   - bônus: com DOIS schemas no mesmo servidor, o `FROM alarms` nu na conexão
//     de um nunca abre o `alarms` do outro.
func TestAlarmsViewIsPublishedInTheSchemaAndCarriesTheFiringValue(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, specServicesWithWindowMetricsAndValue)
	applyWithCounters(t, a, windowDataset,
		append(serviceEvents("OLT_A", 12, true), serviceEvents("OLT_B", 3, true)...))

	a.writeDataset(secondSchemaDataset, specSecondSchemaAlarmObservesOltZ)
	applyWithCounters(t, a, secondSchemaDataset, serviceEvents("OLT_Z", 2, true))

	t0 := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	clock := newTestClock(t0)

	// Primeira subida: com auth_5m_avg=12 (não < 1) o alarme de telecom ainda
	// não observa NADA — a tabela existe (é metadado de SCHEMA), mas está
	// vazia. É o ponto certo para provar publicação e recusa, que não dependem
	// de dado nenhum.
	srv, stop := startDrivenServer(t, a, clock)
	drivePass(t, srv, clock, t0)
	driveSecondSchema(t, srv, clock, t0)

	// (a) /v1/metadata?schema=telecom publica `alarms` como VIEW sob
	// `_counters`, com as três colunas novas.
	metadata := readJSON[protocol.Metadata](t, srv, "/v1/metadata?schema=telecom")
	if len(metadata.Datasets) != 1 {
		t.Fatalf("datasets do schema telecom = %d, quero 1: %+v", len(metadata.Datasets), metadata.Datasets)
	}
	alarmsTable := tableOf(t, metadata.Datasets[0], "_counters.alarms")
	if alarmsTable.Kind != "VIEW" || alarmsTable.Namespace != "_counters" {
		t.Errorf("alarms = %+v, quero Kind VIEW e Namespace _counters", alarmsTable)
	}
	wantColumns := map[string]bool{"valueColumn": false, "firedAt": false, "firingValue": false}
	for _, c := range alarmsTable.Columns {
		if _, ok := wantColumns[c.Name]; ok {
			wantColumns[c.Name] = true
		}
	}
	for col, found := range wantColumns {
		if !found {
			t.Errorf("a coluna %q não saiu no /v1/metadata da view alarms: %+v", col, alarmsTable.Columns)
		}
	}

	// (b) pela conexão por schema, o nome NU e o QUALIFICADO abrem — e o
	// atalho `_counters.alarms` também. A tabela está vazia neste instante
	// (nenhum grupo em observação ainda), então o que se cobra é a ABERTURA
	// (sem erro), não a contagem.
	for _, sql := range []string{"SELECT id FROM alarms", "SELECT id FROM telecom.alarms", "SELECT id FROM _counters.alarms"} {
		queryRowsInSchema(t, srv, "telecom", sql)
	}

	// (c) `_meta.alarms` é recusada com BIND: a quebra é deliberada, sem
	// legado — a tabela não existe mais sob `_meta`.
	resp := consulta(t, srv, queryBody("telecom", "SELECT id FROM _meta.alarms"))
	f := failOf(t, resp)
	if f.Code != protocol.CodeBind {
		t.Fatalf("_meta.alarms respondeu %q, quero %s: %+v", f.Code, protocol.CodeBind, f)
	}

	// bônus, primeira metade do isolamento: o schema "outro" já observa OLT_Z
	// (pending, sem qualifyWhen), e o "telecom" não vê essa linha.
	telecomEmpty := queryRowsInSchema(t, srv, "telecom", "SELECT groupKey FROM alarms")
	outroRows := queryRowsInSchema(t, srv, "outro", "SELECT groupKey FROM alarms")
	if len(telecomEmpty) != 0 {
		t.Fatalf("FROM alarms em telecom = %v, quero vazio (nenhum grupo em observação ainda)", telecomEmpty)
	}
	if len(outroRows) != 1 || outroRows[0]["groupKey"] != "OLT_Z" {
		t.Fatalf("FROM alarms em outro = %v, quero só OLT_Z", outroRows)
	}
	stop()

	// (d) o disparo real: como em TestWindowMetricsFeedTheTableTheAlarmAndSurviveARestart,
	// a restauração é o que deixa auth_24h_max=12 sobreviver à desautenticação
	// total — sem ela o `qualifyWhen` nunca veria a OLT que já teve gente.
	applyWithCounters(t, a, windowDataset,
		append(serviceEvents("OLT_A", 12, false), serviceEvents("OLT_B", 3, false)...))

	restart := t0.Add(10 * time.Minute)
	clock.set(restart)
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("segunda subida: %v", err)
	}
	stop = runDriven(t, srv)
	driveSecondSchema(t, srv, clock, restart)
	drivePass(t, srv, clock, restart)
	drivePass(t, srv, clock, restart.Add(6*time.Minute))
	defer stop()

	rows := queryRowsInSchema(t, srv, "telecom",
		"SELECT groupKey, state, valueColumn, firedAt, firingValue FROM alarms WHERE groupKey = 'OLT_A'")
	if len(rows) != 1 {
		t.Fatalf("linhas do alarme de OLT_A = %d, quero 1: %v", len(rows), rows)
	}
	row := rows[0]
	if row["state"] != "firing" {
		t.Fatalf("state = %v, quero firing", row["state"])
	}
	if row["firedAt"] == nil {
		t.Fatalf("firedAt = nil, o disparo aconteceu e tem de aparecer: %v", row)
	}
	if row["valueColumn"] != "auth_24h_max" {
		t.Fatalf("valueColumn = %v, quero auth_24h_max", row["valueColumn"])
	}
	if row["firingValue"] != 12.0 {
		t.Fatalf("firingValue = %v (%T), quero 12 — um int64(12) gravado nos bits de double sairia como outro "+
			"número (denormalizado, perto de zero), não 12", row["firingValue"], row["firingValue"])
	}

	// bônus, segunda metade do isolamento: com o disparo de pé, o `FROM
	// alarms` de telecom só traz OLT_A (não OLT_Z, do outro schema), e o de
	// "outro" continua só com OLT_Z (não OLT_A, disparado no OUTRO schema).
	telecomRows := queryRowsInSchema(t, srv, "telecom", "SELECT groupKey FROM alarms")
	if len(telecomRows) != 1 || telecomRows[0]["groupKey"] != "OLT_A" {
		t.Fatalf("FROM alarms em telecom = %v, quero só OLT_A", telecomRows)
	}
	outroRows = queryRowsInSchema(t, srv, "outro", "SELECT groupKey FROM alarms")
	if len(outroRows) != 1 || outroRows[0]["groupKey"] != "OLT_Z" {
		t.Fatalf("FROM alarms em outro = %v, quero só OLT_Z", outroRows)
	}
}
