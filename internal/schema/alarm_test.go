package schema

import (
	"strings"
	"testing"
	"time"
)

// TestContadorAceitaOBlocoDeAlarmes é o primeiro passo do bloco: hoje a
// whitelist de chaves do contador recusa `alarms:` como chave desconhecida, que
// é exatamente o que ela existe para fazer. Este teste fixa que a chave passa a
// ser conhecida.
func TestContadorAceitaOBlocoDeAlarmes(t *testing.T) {
	_, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    alarms:
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
`)), BuiltinDefaults)
	if err != nil {
		t.Fatalf("o bloco alarms tem de ser aceito: %v", err)
	}
}

// TestAlarmeDeclaradoChegaNaSpecComOWhenNormalizado confere o que a validação
// guarda. O `when` é normalizado como o `where` do contador — espaços
// colapsados — porque ele é a MESMA condição para o mesmo avaliador, e duas
// grafias do mesmo predicado não podem produzir specs diferentes.
func TestAlarmeDeclaradoChegaNaSpecComOWhenNormalizado(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    alarms:
      - name: olt_down
        when: "total    <   100"
        sustainedFor: 2m
      - name: olt_vazia
        when: "total = 0"
        sustainedFor: 5m
        enrich:
          - name: servicos_da_olt
            sql: "SELECT customerId, olt FROM servicos WHERE olt = ? LIMIT 5"
            bind: [olt]
          - name: amostra
            sql: "SELECT customerId FROM servicos LIMIT 1"
`)), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	alarms := spec.Counters[0].Alarms
	if len(alarms) != 2 {
		t.Fatalf("%d alarmes, quero 2", len(alarms))
	}
	if got, quero := alarms[0].When, "total < 100"; got != quero {
		t.Errorf("when = %q, quero %q normalizado", got, quero)
	}
	if got, quero := alarms[0].SustainedFor, 2*time.Minute; got != quero {
		t.Errorf("sustainedFor = %v, quero %v", got, quero)
	}
	if alarms[0].Enrich != nil {
		t.Errorf("alarme sem enrich tem de ficar sem enrich, veio %+v", alarms[0].Enrich)
	}
	if alarms[1].Name != "olt_vazia" || alarms[1].SustainedFor != 5*time.Minute {
		t.Errorf("segundo alarme = %+v", alarms[1])
	}
	if len(alarms[1].Enrich) != 2 {
		t.Fatalf("%d consultas de enriquecimento, quero 2", len(alarms[1].Enrich))
	}
	if got, quero := alarms[1].Enrich[0].Bind, []string{"olt"}; strings.Join(got, ",") != strings.Join(quero, ",") {
		t.Errorf("bind = %v, quero %v", got, quero)
	}
	// O nome identifica o bloco no evento. Sem ele o consumidor teria de casar
	// a consulta pela POSIÇÃO na lista, que muda toda vez que alguém insere
	// uma consulta no meio.
	if got := alarms[1].Enrich[0].Name; got != "servicos_da_olt" {
		t.Errorf("name = %q, quero servicos_da_olt", got)
	}
	if got := alarms[1].Enrich[1].Name; got != "amostra" {
		t.Errorf("name da segunda = %q", got)
	}
	// Consulta sem `?` não precisa de bind: a contagem de placeholders é
	// conferida contra o parser do SQL na compilação, não aqui.
	if alarms[1].Enrich[1].Bind != nil {
		t.Errorf("consulta sem placeholder tem de ficar sem bind, veio %v", alarms[1].Enrich[1].Bind)
	}
}

// TestAlarmeAusenteDeixaOContadorSemAlarme: o contador de hoje continua válido
// e não ganha alarme implícito.
func TestAlarmeAusenteDeixaOContadorSemAlarme(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
`)), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Counters[0].Alarms != nil {
		t.Fatalf("sem bloco, Alarms tem de ser nil; veio %+v", spec.Counters[0].Alarms)
	}
}

// TestAlarmeRecusas cobre a tabela de admissão do bloco. Cada linha é uma
// recusa que, sem ela, deixaria o operador com um alarme que não observa o que
// ele escreveu — ou que não observa nada.
func TestAlarmeRecusas(t *testing.T) {
	cases := []struct {
		name, block, want string
	}{
		{"não é lista", "alarms:\n      name: x\n", "deve ser uma lista de alarmes"},
		{"lista vazia", "alarms: []\n", "lista vazia"},
		{"chave desconhecida", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedForr: 2m\n", "chave desconhecida"},
		{"name ausente", "alarms:\n      - when: \"total = 0\"\n        sustainedFor: 2m\n", "alarms[0].name obrigatório"},
		{"name inválido", "alarms:\n      - name: OLT-Down\n        when: \"total = 0\"\n        sustainedFor: 2m\n", "inválido"},
		{"name repetido", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n      - name: a\n        when: \"total = 1\"\n        sustainedFor: 2m\n", "alarms[1].name \"a\" repetido"},
		{"when ausente", "alarms:\n      - name: a\n        sustainedFor: 2m\n", "when obrigatório"},
		{"when vazio", "alarms:\n      - name: a\n        when: \"   \"\n        sustainedFor: 2m\n", "when vazio"},
		{"sustainedFor ausente", "alarms:\n      - name: a\n        when: \"total = 0\"\n", "sustainedFor obrigatório"},
		{"sustainedFor never", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: never\n", "nunca dispara"},
		{"sustainedFor zero", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 0\n", "não declara prazo"},
		{"sustainedFor abaixo do mínimo", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 5s\n", "menor que o mínimo"},
		{"sustainedFor inválido", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: dois minutos\n", "sustainedFor"},
		{"sustainedFor negativo", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: -2m\n", "sustainedFor"},
		{"enrich não é lista", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          sql: SELECT 1\n", "lista de consultas"},
		{"enrich lista vazia", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich: []\n", "lista vazia"},
		{"enrich chave desconhecida", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - query: SELECT 1\n", "chave desconhecida"},
		{"enrich sql ausente", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - name: e\n            bind: [olt]\n", "sql obrigatório"},
		{"enrich sql vazio", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - name: e\n            sql: \"  \"\n", "sql vazio"},
		{"enrich bind vazio", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - name: e\n            sql: SELECT 1\n            bind: [\"\"]\n", "coluna vazia"},
		{"enrich bind repetido", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - name: e\n            sql: SELECT 1\n            bind: [olt, olt]\n", "repetida"},
		{"enrich name ausente", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - sql: SELECT 1\n", "name obrigatório"},
		{"enrich name inválido", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - name: Da-OLT\n            sql: SELECT 1\n", "inválido"},
		{"enrich name repetido", "alarms:\n      - name: a\n        when: \"total = 0\"\n        sustainedFor: 2m\n        enrich:\n          - name: e\n            sql: SELECT 1\n          - name: e\n            sql: SELECT 2\n", "repetido"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			block := "counters:\n  - name: x\n    groupBy: [olt]\n    " + c.block
			_, err := Parse([]byte(yamlWithCounters(block)), BuiltinDefaults)
			if err == nil {
				t.Fatalf("aceitou:\n%s", block)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("erro %q não menciona %q", err, c.want)
			}
		})
	}
}

// TestAlarmValueKeyIsParsed confere que a chave `value` chega na spec como
// declarada: é o nome da coluna de `_counters.<contador>` que vira o
// firingValue do alarme, e aqui só a forma é conferida — o default (`total`)
// e a validação contra a tabela são papel da compilação em internal/alarm.
func TestAlarmValueKeyIsParsed(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    alarms:
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
        value: avgLatency
`)), BuiltinDefaults)
	if err != nil {
		t.Fatalf("a chave value tem de ser aceita: %v", err)
	}
	if got, quero := spec.Counters[0].Alarms[0].Value, "avgLatency"; got != quero {
		t.Fatalf("Value = %q, quero %q", got, quero)
	}
}

// TestAlarmBlankValueIsRefused: presente e em branco é recusado, e não lido
// como ausente — a mesma regra do qualifyWhen (alarm.go): quem escreveu a
// chave quis uma coluna, e a que ele escreveu não existe.
func TestAlarmBlankValueIsRefused(t *testing.T) {
	_, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    alarms:
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
        value: "   "
`)), BuiltinDefaults)
	if err == nil || !strings.Contains(err.Error(), "value") {
		t.Fatalf("value em branco tem de ser recusado pelo motivo certo: %v", err)
	}
}

// TestAlarmWithoutValueLeavesItEmpty: ausente é "sem coluna declarada", e o
// zero value da string continua vazio — é o sinal que a compilação lê como
// "use a coluna total".
func TestAlarmWithoutValueLeavesItEmpty(t *testing.T) {
	spec, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    alarms:
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
`)), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if got := spec.Counters[0].Alarms[0].Value; got != "" {
		t.Fatalf("Value = %q, quero vazio (default total)", got)
	}
}
