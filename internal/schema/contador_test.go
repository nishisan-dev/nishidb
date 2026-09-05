package schema

import (
	"strings"
	"testing"
	"time"
)

// yamlComContadores monta uma spec com o bloco counters dado (já como texto
// YAML de topo), sobre um schema que tem um campo de cada caso que a validação
// precisa distinguir.
func yamlComContadores(bloco string) string {
	return `
version: 1
name: servicos
materialization:
  key:
    field: customerId
  sequence:
    field: seq
    type: int64
schema:
  fields:
    customerId:
      type: string
    seq:
      type: int64
    olt:
      type: string
    slot:
      type: int64
    port:
      type: int64
    authenticated:
      type: boolean
    lastSeen:
      type: timestamp
    extra:
      type: auto
` + bloco
}

func TestCountersAceitaAFormaDaProposta(t *testing.T) {
	spec, err := Parse([]byte(yamlComContadores(`
counters:
  - name: servicos_por_olt
    groupBy: [olt]
  - name: autenticados_por_pon
    groupBy: [olt, slot, port]
    where: authenticated = true
    maxGroups: 262144
    retainEmptyGroups: 30d
`)), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Contadores) != 2 {
		t.Fatalf("%d contadores, quero 2", len(spec.Contadores))
	}
	a, b := spec.Contadores[0], spec.Contadores[1]
	if a.Name != "servicos_por_olt" || len(a.GroupBy) != 1 || a.GroupBy[0] != "olt" || a.Where != "" {
		t.Fatalf("primeiro = %+v", a)
	}
	if a.MaxGroups != MaxGroupsPadrao || a.RetainEmptyGroups != RetainEmptyGroupsPadrao {
		t.Fatalf("defaults não aplicados: %+v", a)
	}
	if b.Name != "autenticados_por_pon" || strings.Join(b.GroupBy, ",") != "olt,slot,port" ||
		b.Where != "authenticated = true" || b.MaxGroups != 262144 || b.RetainEmptyGroups != 30*24*time.Hour {
		t.Fatalf("segundo = %+v", b)
	}
}

func TestCountersAusenteEhDatasetSemContador(t *testing.T) {
	spec, err := Parse([]byte(yamlComContadores("")), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Contadores != nil {
		t.Fatalf("sem bloco, Contadores tem de ser nil; veio %+v", spec.Contadores)
	}
}

func TestCountersRetainNeverEhZero(t *testing.T) {
	spec, err := Parse([]byte(yamlComContadores(`
counters:
  - name: x
    groupBy: [olt]
    retainEmptyGroups: never
`)), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Contadores[0].RetainEmptyGroups != 0 {
		t.Fatalf("never tem de virar zero, veio %s", spec.Contadores[0].RetainEmptyGroups)
	}
}

// TestCountersNormalizaOWhere: o texto vai ao manifest e decide reconstrução;
// espaço a mais não pode ser "outro contador".
func TestCountersNormalizaOWhere(t *testing.T) {
	spec, err := Parse([]byte(yamlComContadores(`
counters:
  - name: x
    groupBy: [olt]
    where: "  authenticated   =  true "
`)), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Contadores[0].Where != "authenticated = true" {
		t.Fatalf("where não normalizado: %q", spec.Contadores[0].Where)
	}
}

func TestCountersRecusas(t *testing.T) {
	casos := []struct {
		nome, bloco, quero string
	}{
		{"lista vazia", "counters: []\n", "counters: lista vazia"},
		{"não é lista", "counters:\n  name: x\n", "deve ser uma lista"},
		{"chave desconhecida", "counters:\n  - name: x\n    groupBy: [olt]\n    wher: a = 1\n", "chave desconhecida"},
		{"name ausente", "counters:\n  - groupBy: [olt]\n", "counters[0].name"},
		{"name inválido", "counters:\n  - name: Por-OLT\n    groupBy: [olt]\n", "name \"Por-OLT\" inválido"},
		{"name repetido", "counters:\n  - name: x\n    groupBy: [olt]\n  - name: x\n    groupBy: [slot]\n", "counters[1].name \"x\" repetido"},
		{"groupBy ausente", "counters:\n  - name: x\n", "counters[0].groupBy"},
		{"groupBy vazio", "counters:\n  - name: x\n    groupBy: []\n", "counters[0].groupBy"},
		{"groupBy repetido", "counters:\n  - name: x\n    groupBy: [olt, olt]\n", "repetida"},
		{"coluna não declarada", "counters:\n  - name: x\n    groupBy: [pon]\n", "\"pon\" não declarada"},
		{"coluna auto", "counters:\n  - name: x\n    groupBy: [extra]\n", "tipo declarado"},
		{"tipo fora dos três", "counters:\n  - name: x\n    groupBy: [lastSeen]\n", "string, int64 e boolean"},
		{"coluna key", "counters:\n  - name: x\n    groupBy: [customerId]\n", "key"},
		{"coluna sequence", "counters:\n  - name: x\n    groupBy: [seq]\n", "sequence"},
		{"coluna groupKey", "counters:\n  - name: x\n    groupBy: [groupKey]\n", "groupKey"},
		{"coluna total", "counters:\n  - name: x\n    groupBy: [total]\n", "total"},
		{"where vazio", "counters:\n  - name: x\n    groupBy: [olt]\n    where: \"  \"\n", "counters[0].where"},
		{"maxGroups zero", "counters:\n  - name: x\n    groupBy: [olt]\n    maxGroups: 0\n", "maxGroups"},
		{"maxGroups negativo", "counters:\n  - name: x\n    groupBy: [olt]\n    maxGroups: -1\n", "maxGroups"},
		{"maxGroups acima do teto", "counters:\n  - name: x\n    groupBy: [olt]\n    maxGroups: 2147483648\n", "maxGroups"},
		{"retain zero explícito", "counters:\n  - name: x\n    groupBy: [olt]\n    retainEmptyGroups: 0\n", "never"},
		{"retain abaixo da resolução", "counters:\n  - name: x\n    groupBy: [olt]\n    retainEmptyGroups: 10s\n", "resolução"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			_, err := Parse([]byte(yamlComContadores(c.bloco)), DefaultsPadrao)
			if err == nil {
				t.Fatalf("aceitou:\n%s", c.bloco)
			}
			if !strings.Contains(err.Error(), c.quero) {
				t.Fatalf("erro %q não menciona %q", err, c.quero)
			}
		})
	}
}

// TestCountersRecusaColunaDeExtractComoNaoDeclarada: uma coluna extraída
// declarada em `extract` É declarada — o groupBy pode agrupar por ela.
func TestCountersAceitaColunaExtraida(t *testing.T) {
	spec, err := Parse([]byte(`
version: 1
name: servicos
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    doc:
      type: json
      extract:
        - path: olt
          as: olt
          type: string
counters:
  - name: x
    groupBy: [olt]
`), DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Contadores[0].GroupBy[0] != "olt" {
		t.Fatalf("%+v", spec.Contadores)
	}
}

func TestOPrefixoReservadoNomeiaOsDoisNamespaces(t *testing.T) {
	_, err := Parse([]byte(strings.Replace(yamlComContadores(""), "name: servicos", "name: _counters", 1)), DefaultsPadrao)
	if err == nil || !strings.Contains(err.Error(), "_counters") || !strings.Contains(err.Error(), "_meta") {
		t.Fatalf("a recusa tem de nomear os namespaces reservados: %v", err)
	}
}
