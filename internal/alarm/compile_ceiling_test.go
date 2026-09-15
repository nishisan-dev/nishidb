package alarm

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// conditionOfLength monta uma condição VÁLIDA com exatamente n bytes. Tem de
// compilar por si só: sem isso a recusa de tamanho não seria a única barreira, e
// o teste passaria com ela removida, recusado pelo parser.
func conditionOfLength(n int) string {
	const prefix = "groupKey = '"
	return prefix + strings.Repeat("x", n-len(prefix)-1) + "'"
}

// TestAlarmTextAboveTheStateCeilingIsRefusedAtStartup: um `when` ou um
// `qualifyWhen` que o alarms.state não comporta é recusado na SUBIDA, com o
// caminho do YAML e o tamanho. Aceito, ele compilaria e só falharia na primeira
// gravação do estado — de madrugada, no primeiro alarme, parando o laço com
// `alarm.stopped`. O limite exato continua aceito, para que a recusa não pegue o
// que cabe.
func TestAlarmTextAboveTheStateCeilingIsRefusedAtStartup(t *testing.T) {
	def := defByPON("autenticados_por_pon")
	ceiling := checkpoint.MaxAlarmStateText
	fits := schema.AlarmSpec{
		Name: "olt_down", When: conditionOfLength(ceiling), QualifyWhen: conditionOfLength(ceiling),
		SustainedFor: 2 * time.Minute,
	}
	if _, err := Compile("telecom", specWith(fits), []counter.Definition{def}); err != nil {
		t.Fatalf("condições de exatamente %d bytes foram recusadas: %v", ceiling, err)
	}
	cases := map[string]schema.AlarmSpec{
		"when": {Name: "olt_down", When: conditionOfLength(ceiling + 1), SustainedFor: 2 * time.Minute},
		"qualifyWhen": {
			Name: "olt_down", When: "total < 100", QualifyWhen: conditionOfLength(ceiling + 1),
			SustainedFor: 2 * time.Minute,
		},
	}
	for field, as := range cases {
		t.Run(field, func(t *testing.T) {
			_, err := Compile("telecom", specWith(as), []counter.Definition{def})
			if err == nil {
				t.Fatal("compilou um texto que o alarms.state não grava: a falha ficaria para a primeira gravação")
			}
			if path := "counters[0].alarms[0]." + field; !strings.Contains(err.Error(), path) {
				t.Errorf("a recusa não nomeia o caminho %q: %v", path, err)
			}
			if size := strconv.Itoa(ceiling + 1); !strings.Contains(err.Error(), size) {
				t.Errorf("a recusa não diz o tamanho %s: %v", size, err)
			}
		})
	}
}

// TestMetricsFingerprintAboveTheStateCeilingIsRefused cobre a terceira
// assinatura pela mesma checagem. Um fingerprint desse tamanho exigiria nomes de
// métrica que o schema sintético nem montaria, então a checagem é exercitada
// direto.
func TestMetricsFingerprintAboveTheStateCeilingIsRefused(t *testing.T) {
	const path = "counters[0].alarms[0] metrics fingerprint"
	if err := checkAlarmStateText(path, strings.Repeat("x", checkpoint.MaxAlarmStateText)); err != nil {
		t.Fatalf("fingerprint de exatamente %d bytes recusado: %v", checkpoint.MaxAlarmStateText, err)
	}
	err := checkAlarmStateText(path, strings.Repeat("x", checkpoint.MaxAlarmStateText+1))
	if err == nil {
		t.Fatal("fingerprint acima do teto aceito")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("a recusa não nomeia %q: %v", path, err)
	}
}
