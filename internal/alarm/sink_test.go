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
