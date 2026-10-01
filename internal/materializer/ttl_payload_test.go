package materializer

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// storedTTL devolve o `_expiresIn` gravado para a key, como duração. É a
// leitura do RECORD, e não de um retorno interno do materializer: o que o
// storage indexa e o replay reconstrói é o campo no payload.
func (h *harness) storedTTL(key string) time.Duration {
	h.t.Helper()
	v, ok := h.state(key)[schema.TTLFieldName]
	if !ok || v.Type != types.TypeInt64 {
		h.t.Fatalf("key %q sem %s no record: %+v", key, schema.TTLFieldName, v)
	}
	return time.Duration(v.I64) * time.Millisecond
}

// replaceTTLYAML é o modo replace com uma regra condicional: o estado anterior
// é DESCARTADO a cada evento, então a condição só pode ver o que o evento trouxe.
const replaceTTLYAML = `
version: 1
name: alarmes
schemaName: telecom
materialization:
  mode: replace
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
    sev:
      type: int64
`

// No replace o campo que só o estado anterior tinha NÃO existe no record novo,
// e a regra não pode casar com ele. Avaliada sobre o anterior + o patch, a
// condição via o `tipo = 2` de uma encarnação que o replace acabou de jogar
// fora, e a entidade ganhava o prazo curto de um estado que não é o dela.
func TestReplaceRuleDoesNotSeeFieldOnlyInPreviousState(t *testing.T) {
	h := newHarness(t, replaceTTLYAML)
	h.apply(`{"id":"a","tipo":2}`)
	if got := h.storedTTL("a"); got != 3*time.Minute {
		t.Fatalf("primeira escrita: prazo %v, quero 3m", got)
	}
	h.apply(`{"id":"a","sev":5}`)
	if _, ok := h.state("a")["tipo"]; ok {
		t.Fatal("o replace preservou `tipo`: o cenário do teste não vale")
	}
	if got := h.storedTTL("a"); got != 7*24*time.Hour {
		t.Errorf("prazo %v depois do replace sem `tipo`, quero 7d (fundo): a regra casou com um campo-fantasma", got)
	}
}

// frozenTTLYAML condiciona o prazo a duas congeladas: uma de campo e uma de
// relógio. A congelada só muda na primeira escrita; o evento seguinte traz a
// origem de novo e o merge DESCARTA o valor novo.
const frozenTTLYAML = `
version: 1
name: alarmes
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - when: "origSev = 5"
      expireAfterWrite: 3m
    - when: "criadoEm < '2026-08-26T12:30:00Z'"
      expireAfterWrite: 10m
    - expireAfterWrite: 7d
schema:
  fields:
    id:
      type: string
    sev:
      type: int64
    origSev:
      frozenFrom: sev
    criadoEm:
      frozenFrom: "@writeTime"
`

// A regra sobre congelada de CAMPO vê o valor congelado. O segundo evento traz
// sev=9, a captura emite origSev=9 e o merge segura o 5: o record diz 5, e o
// prazo tem de ser o do 5.
func TestRuleOnFrozenFieldReadsFrozenValue(t *testing.T) {
	h := newHarness(t, frozenTTLYAML)
	t0 := time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC) // depois do corte da regra 1
	h.m.UsaRelogio(func() time.Time { return t0 })
	h.apply(`{"id":"a","sev":5}`)
	h.apply(`{"id":"a","sev":9}`)
	if got := h.state("a")["origSev"]; got.I64 != 5 {
		t.Fatalf("origSev = %+v, quero 5 congelado: o cenário do teste não vale", got)
	}
	if got := h.storedTTL("a"); got != 3*time.Minute {
		t.Errorf("prazo %v, quero 3m: a regra leu a captura do evento (9) em vez do valor congelado (5)", got)
	}
}

// A regra sobre congelada de RELÓGIO vê o instante da PRIMEIRA escrita. A
// captura de relógio emite entry com o instante corrente em TODO evento; sem
// olhar o record final, a condição via sempre o agora.
func TestRuleOnFrozenClockReadsFirstWriteInstant(t *testing.T) {
	h := newHarness(t, frozenTTLYAML)
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC) // antes do corte
	h.m.UsaRelogio(func() time.Time { return now })
	h.apply(`{"id":"a","sev":1}`)
	if got := h.storedTTL("a"); got != 10*time.Minute {
		t.Fatalf("primeira escrita: prazo %v, quero 10m", got)
	}
	now = now.Add(time.Hour) // depois do corte: só a captura nova passaria dele
	h.apply(`{"id":"a","sev":2}`)
	if got := h.state("a")["criadoEm"]; got.I64 != time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC).UnixMilli() {
		t.Fatalf("criadoEm = %+v: a congelada de relógio não congelou", got)
	}
	if got := h.storedTTL("a"); got != 10*time.Minute {
		t.Errorf("prazo %v, quero 10m: a regra leu o instante desta escrita em vez do congelado", got)
	}
}

// sumTTLYAML condiciona o prazo a um campo `merge: sum`.
const sumTTLYAML = `
version: 1
name: alarmes
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - when: "hits >= 10"
      expireAfterWrite: 3m
    - expireAfterWrite: 7d
schema:
  fields:
    id:
      type: string
    hits:
      type: int64
      merge: sum
`

// A regra sobre `merge: sum` vê a SOMA, que é o que o record guarda — e não a
// parcela que o evento trouxe.
func TestRuleOnSummedFieldReadsTheSum(t *testing.T) {
	h := newHarness(t, sumTTLYAML)
	h.apply(`{"id":"a","hits":6}`)
	if got := h.storedTTL("a"); got != 7*24*time.Hour {
		t.Fatalf("primeira escrita: prazo %v, quero 7d (6 < 10)", got)
	}
	h.apply(`{"id":"a","hits":6}`)
	if got := h.state("a")["hits"]; got.I64 != 12 {
		t.Fatalf("hits = %+v, quero 12: o cenário do teste não vale", got)
	}
	if got := h.storedTTL("a"); got != 3*time.Minute {
		t.Errorf("prazo %v, quero 3m: a regra leu a parcela (6) em vez da soma (12)", got)
	}
}
