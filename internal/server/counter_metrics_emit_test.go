package server_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// specServicesWithWindowMetricsAndEmit declara, no mesmo contador, as métricas
// de janela e a emissão periódica: é a costura que o débito 124 deixou aberta.
const specServicesWithWindowMetricsAndEmit = `version: 1
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
        when: "auth_5m_avg < 1"
        sustainedFor: 5m
    emit:
      every: 10s
      maxBytes: 1MiB
      keep: 2
`

// TestTheEmittedLineCarriesTheWindowColumnsTheTableServes é a costura ponta a
// ponta: o YAML declara `metrics:` e `emit:` no mesmo contador, o amostrador
// publica, e a linha da série traz as MESMAS colunas, com os MESMOS valores,
// que um SELECT sobre `_counters.<nome>` devolve para o mesmo grupo.
//
// O servidor é montado SEM Run: os laços sobem lá, e com eles de pé a passada
// da emissão que o teste conduz disputaria o arquivo com a do laço. Sem Run o
// amostrador e o emissor existem desde o Novo e estão parados, então cada
// passada é a do teste. É a razão pela qual QueryRowsForTest existe.
func TestTheEmittedLineCarriesTheWindowColumnsTheTableServes(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset(windowDataset, specServicesWithWindowMetricsAndEmit)
	applyWithCounters(t, a, windowDataset,
		append(serviceEvents("OLT_A", 12, true), serviceEvents("OLT_B", 3, true)...))

	t0 := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	srv := stoppedServer(t, a, newTestClock(t0))
	if err := srv.SampleCounterMetricsOnceForTest(windowDataset); err != nil {
		t.Fatalf("amostragem: %v", err)
	}
	if err := srv.EmitOnceForTest(windowDataset); err != nil {
		t.Fatalf("emissão: %v", err)
	}

	lines := seriesLines(t, a, windowDataset, "autenticados_por_olt")
	if len(lines) != 2 {
		t.Fatalf("%d linhas na passada, quero uma por grupo:\n%s", len(lines), strings.Join(lines, "\n"))
	}
	byGroup := make(map[string]map[string]any, len(lines))
	for _, line := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("a linha não é JSON: %v\n%s", err, line)
		}
		byGroup[fmt.Sprint(row["groupKey"])] = row
	}

	// Os mesmos números que a tabela serve, lidos pelo mesmo motor.
	table := rowsByGroupKey(t, engineRows(t, srv,
		"SELECT groupKey, total, auth_5m_avg, auth_24h_max FROM _counters.autenticados_por_olt"))
	for _, group := range []string{"OLT_A", "OLT_B"} {
		emitted, ok := byGroup[group]
		if !ok {
			t.Fatalf("o grupo %q não saiu na série: %v", group, byGroup)
		}
		for _, col := range []string{"total", "auth_5m_avg", "auth_24h_max"} {
			if emitted[col] != table[group][col] {
				t.Errorf("%s.%s: a série diz %v e a tabela diz %v",
					group, col, emitted[col], table[group][col])
			}
		}
	}
	// E os valores são os esperados, não só iguais entre si.
	if byGroup["OLT_A"]["auth_5m_avg"] != 12.0 || byGroup["OLT_B"]["auth_24h_max"] != 3.0 {
		t.Errorf("os agregados da série não batem com os eventos: %v", byGroup)
	}
}

// TestACounterWithoutWindowMetricsEmitsTheLineUnchanged fixa o outro lado do
// contrato: quem não declara `metrics:` tem a linha de sempre, e nenhuma chave
// nova aparece nela.
func TestACounterWithoutWindowMetricsEmitsTheLineUnchanged(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("servicos", fmt.Sprintf(specServicesComEmissao, "servicos"))
	seedServices(t, a, "servicos", 24)
	srv := stoppedServer(t, a, newTestClock(time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)))
	if err := srv.EmitOnceForTest("servicos"); err != nil {
		t.Fatalf("emissão: %v", err)
	}
	for _, line := range seriesLines(t, a, "servicos", "autenticados_por_olt") {
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("a linha não é JSON: %v\n%s", err, line)
		}
		if len(row) != 5 {
			t.Errorf("a linha de quem não declara métricas tem %d campos, quero os 5 de sempre: %v", len(row), row)
		}
		for _, col := range []string{"counter", "groupKey", "olt", "total", "at"} {
			if _, ok := row[col]; !ok {
				t.Errorf("a linha perdeu o campo %q: %v", col, row)
			}
		}
	}
}

// stoppedServer monta o servidor sem subir o Run: os laços ficam parados, e
// cada passada é a que o teste conduz.
func stoppedServer(t *testing.T, a *env, clock *testClock) *server.Server {
	t.Helper()
	srv, err := server.Novo(a.load(), server.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("encerramento: %v", err)
		}
	})
	return srv
}

// seriesLines lê as linhas do arquivo da série de um contador.
func seriesLines(t *testing.T, a *env, dataset, counter string) []string {
	t.Helper()
	path := filepath.Join(a.load().DatasetRootDir(dataset), schema.EmitDirName, counter+".ndjson")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	text := strings.TrimRight(string(b), "\n")
	if text == "" {
		t.Fatalf("nada escrito em %s", path)
	}
	return strings.Split(text, "\n")
}
