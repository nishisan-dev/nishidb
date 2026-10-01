package alarm

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

func eventoDeExemplo() Event {
	return Event{
		Dataset:      "telecom",
		Counter:      "autenticados_por_pon",
		Alarm:        "olt_down",
		State:        StateFiring,
		GroupKey:     "OLT_A|3",
		Columns:      []Column{{Name: "olt", Value: "OLT_A"}, {Name: "slot", Value: "3"}},
		Total:        42,
		Condition:    "total < 100",
		SustainedFor: 2 * time.Minute,
		Since:        t0,
		At:           t0.Add(2 * time.Minute),
	}
}

// emitir devolve a linha de log como mapa.
func emitir(t *testing.T, e Event) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	NewLogSink(logging.New(&buf, logging.FormatJSON, slog.LevelDebug)).Emit(e)
	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("a linha não é JSON: %v\n%s", err, buf.String())
	}
	return got
}

// TestDisparoSaiComOEventoNomeado: `event` é o identificador estável que um
// alarme externo casa, e `message` é a prosa. A separação é do §95.
func TestDisparoSaiComOEventoNomeado(t *testing.T) {
	got := emitir(t, eventoDeExemplo())
	if got["event"] != "alarm.firing" {
		t.Errorf("event = %v, quero alarm.firing", got["event"])
	}
	if got["message"] == nil || got["message"] == "" {
		t.Error("a linha tem de trazer prosa para quem lê o log")
	}
}

func TestResolucaoSaiComOMotivo(t *testing.T) {
	e := eventoDeExemplo()
	e.State, e.Reason = StateResolved, ReasonGroupRetired
	got := emitir(t, e)
	if got["event"] != "alarm.resolved" {
		t.Errorf("event = %v, quero alarm.resolved", got["event"])
	}
	if got["reason"] != ReasonGroupRetired {
		t.Errorf("reason = %v, quero %v", got["reason"], ReasonGroupRetired)
	}
}

// TestNenhumCampoDoEventoEhRedigido é o teste que existe por causa de uma
// armadilha real: o handler de log suprime chaves como `value`, `record` e
// `payload`, e um campo do evento batizado com um desses nomes sairia como
// [REDACTADO] sem nenhum aviso. Quem mexer nos nomes dos campos precisa
// descobrir isso aqui, e não num incidente.
func TestNenhumCampoDoEventoEhRedigido(t *testing.T) {
	got := emitir(t, eventoDeExemplo())
	for k, v := range got {
		if s, ok := v.(string); ok && s == logging.Redacted {
			t.Errorf("o campo %q saiu redigido: escolha outro nome", k)
		}
	}
	// Os campos que tornam o evento acionável têm de estar todos lá: sem o
	// limiar e sem o grupo, "total baixo" não diz o que fazer.
	for _, k := range []string{"counter", "alarm", "groupKey", "total", "condition", "sustainedFor", "since", "olt", "slot"} {
		if _, ok := got[k]; !ok {
			t.Errorf("o campo %q não chegou ao writer", k)
		}
	}
}

// TestColunasDoGrupoSaemComoCamposProprios: quem consome o log filtra por OLT,
// e repartir o texto do grupo exigiria conhecer o keyFormat.
func TestColunasDoGrupoSaemComoCamposProprios(t *testing.T) {
	got := emitir(t, eventoDeExemplo())
	if got["olt"] != "OLT_A" || got["slot"] != "3" {
		t.Errorf("colunas do groupBy = olt:%v slot:%v", got["olt"], got["slot"])
	}
}

// TestColunaComNomeSensivelNaoDerrubaOsDemaisCampos: o nome da coluna vem do
// YAML do cliente, e uma coluna chamada `token` cai na lista de supressão do
// handler. O evento continua utilizável — o que se perde é aquele valor.
func TestColunaComNomeSensivelNaoDerrubaOsDemaisCampos(t *testing.T) {
	e := eventoDeExemplo()
	e.Columns = []Column{{Name: "token", Value: "abc"}}
	got := emitir(t, e)
	if got["groupKey"] != "OLT_A|3" || got["event"] != "alarm.firing" {
		t.Errorf("uma coluna de nome sensível levou o evento com ela: %+v", got)
	}
}

// TestLacunaSaiSoQuandoOEstadoVeioDoDisco: um campo de lacuna sempre presente,
// com zero, diria "o processo não ficou fora" para todo alarme observado de
// ponta a ponta — que é verdade, mas indistinguível de "não sei".
func TestLacunaSaiSoQuandoOEstadoVeioDoDisco(t *testing.T) {
	if got := emitir(t, eventoDeExemplo()); got["restored"] != nil || got["downtimeMs"] != nil {
		t.Errorf("alarme observado por este processo trouxe marca de restauro: %+v", got)
	}
	e := eventoDeExemplo()
	e.Restored, e.DowntimeMs = true, 3_600_000
	got := emitir(t, e)
	if got["restored"] != true || got["downtimeMs"] != float64(3_600_000) {
		t.Errorf("restored=%v downtimeMs=%v", got["restored"], got["downtimeMs"])
	}
}

// TestEnriquecimentoSaiAninhadoPorNome: o consumidor acha o bloco pelo nome
// declarado, nunca pela posição na lista.
func TestEnriquecimentoSaiAninhadoPorNome(t *testing.T) {
	e := eventoDeExemplo()
	e.Enrichment = []Enrichment{
		{Name: "servicos_da_olt", Rows: []byte(`[{"customerId":"c1"},{"customerId":"c2"}]`)},
		{Name: "vizinhas", Note: "consulta falhou: dataset desconhecido"},
	}
	got := emitir(t, e)
	bloco, ok := got["enrichment"].(map[string]any)
	if !ok {
		t.Fatalf("enrichment = %#v, quero um objeto", got["enrichment"])
	}
	primeiro, ok := bloco["servicos_da_olt"].(map[string]any)
	if !ok {
		t.Fatalf("o bloco nomeado não saiu: %#v", bloco)
	}
	rows, ok := primeiro["rows"].([]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("rows = %#v, quero as duas linhas como array JSON", primeiro["rows"])
	}
	// O array chega como JSON de verdade, e não como texto escapado: quem
	// consome o log com jq precisa poder indexar dentro dele.
	linha, ok := rows[0].(map[string]any)
	if !ok || linha["customerId"] != "c1" {
		t.Errorf("a primeira linha não é um objeto navegável: %#v", rows[0])
	}
	segundo, ok := bloco["vizinhas"].(map[string]any)
	if !ok {
		t.Fatalf("o bloco que falhou não saiu: %#v", bloco)
	}
	if segundo["note"] == nil {
		t.Error("o bloco vazio tem de dizer por quê")
	}
	if segundo["rows"] != nil {
		t.Error("bloco que falhou não pode trazer rows")
	}
}

// TestDisqualifiedResolutionIsLoggedWithItsReason: o motivo novo sai pelo mesmo
// campo dos outros dois.
func TestDisqualifiedResolutionIsLoggedWithItsReason(t *testing.T) {
	e := eventoDeExemplo()
	e.State, e.Reason = StateResolved, ReasonDisqualified
	got := emitir(t, e)
	if got["event"] != "alarm.resolved" || got["reason"] != "disqualified" {
		t.Errorf("event=%v reason=%v, quero alarm.resolved/disqualified", got["event"], got["reason"])
	}
}

// TestQualifyConditionIsLoggedOnlyWhenDeclared: presente e vazio, o campo
// obrigaria quem consome a distinguir "não declarei" de "declarei vazio".
func TestQualifyConditionIsLoggedOnlyWhenDeclared(t *testing.T) {
	if got := emitir(t, eventoDeExemplo()); got["qualifyCondition"] != nil {
		t.Errorf("qualifyCondition = %v num alarme sem qualifyWhen", got["qualifyCondition"])
	}
	e := eventoDeExemplo()
	e.QualifyCondition = "auth_24h_max > 10"
	if got := emitir(t, e); got["qualifyCondition"] != "auth_24h_max > 10" {
		t.Errorf("qualifyCondition = %v", got["qualifyCondition"])
	}
}

// TestMetricsAreLoggedAsAGroup: as colunas das métricas saem aninhadas em
// `metrics`, fora do espaço de nomes das colunas do groupBy e dos campos do
// evento — onde um nome vindo do YAML poderia bater num campo como `total`.
func TestMetricsAreLoggedAsAGroup(t *testing.T) {
	if got := emitir(t, eventoDeExemplo()); got["metrics"] != nil {
		t.Errorf("metrics = %#v num evento sem métrica", got["metrics"])
	}
	e := eventoDeExemplo()
	e.Metrics = []Column{{Name: "auth_5m_avg", Value: "0.5"}, {Name: "auth_24h_max", Value: "50"}}
	got := emitir(t, e)
	group, ok := got["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("metrics = %#v, quero um objeto", got["metrics"])
	}
	if group["auth_5m_avg"] != "0.5" || group["auth_24h_max"] != "50" {
		t.Errorf("metrics = %#v", group)
	}
	if got["auth_5m_avg"] != nil {
		t.Error("a coluna da métrica vazou para o nível dos campos soltos")
	}
	if got["olt"] != "OLT_A" || got["total"] != float64(42) {
		t.Errorf("as métricas deslocaram os campos do evento: olt=%v total=%v", got["olt"], got["total"])
	}
}

// TestEventoSemEnriquecimentoNaoTrazOCampo: um campo vazio sempre presente
// obrigaria quem consome a distinguir "não declarei enriquecimento" de "declarei
// e não veio nada" pelo conteúdo.
func TestEventoSemEnriquecimentoNaoTrazOCampo(t *testing.T) {
	if got := emitir(t, eventoDeExemplo()); got["enrichment"] != nil {
		t.Errorf("enrichment = %#v num evento sem enriquecimento declarado", got["enrichment"])
	}
}

// TestFiringAndResolvedCarryRowsAndTotalInLogAndBody: `rows` e `total` saem
// nos DOIS estados e nas DUAS projeções — a linha de log e o corpo JSON do
// webhook, que são a mesma projeção (eventArguments). Na resolução o par é o
// que fez a condição passar, e sem `rows` o consumidor não saberia de quantos
// membros o total voltou a ser fração. Com `total: false`, total sai null e
// rows continua lá.
func TestFiringAndResolvedCarryRowsAndTotalInLogAndBody(t *testing.T) {
	for _, state := range []string{StateFiring, StateResolved} {
		e := eventoDeExemplo()
		e.State, e.Total, e.Rows = state, 1, 10
		if state == StateResolved {
			e.Reason, e.Total = ReasonConditionCleared, 2
		}
		var body map[string]any
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		for name, got := range map[string]map[string]any{"log": emitir(t, e), "corpo": body} {
			if got["rows"] != float64(10) || got["total"] != float64(e.Total) {
				t.Errorf("%s de %s: rows=%v total=%v, quero 10 e %d", name, state, got["rows"], got["total"], e.Total)
			}
		}
		e.TotalDisabled = true
		got := emitir(t, e)
		if v, present := got["total"]; !present || v != nil || got["rows"] != float64(10) {
			t.Errorf("%s com total desligado: total=%v (presente=%v) rows=%v, quero total null e rows 10",
				state, v, present, got["rows"])
		}
	}
	if !ReservedEventKey("rows") {
		t.Error("rows é campo do evento e tem de ser chave reservada: uma coluna com esse nome geraria JSON ambíguo")
	}
}
