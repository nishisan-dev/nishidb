package materializer

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/rowindex"
)

// anchorYAML é o contrato do service_state: sem autenticação, 1h renovável;
// autenticado, 48h contadas da última autenticação.
const anchorYAML = `
version: 1
name: service_state
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - when: "lastAuthAt IS NULL"
      expireAfterWrite: 1h
    - anchor: lastAuthAt
      expireAfter: 48h
schema:
  fields:
    id:
      type: string
    lastAuthAt:
      type: timestamp
    sev:
      type: int64
`

// anchorFirstYAML põe a regra ancorada PRIMEIRO, para que a ausência da
// âncora tenha uma próxima regra a tentar.
const anchorFirstYAML = `
version: 1
name: service_state
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - anchor: lastAuthAt
      expireAfter: 48h
    - expireAfterWrite: 1h
schema:
  fields:
    id:
      type: string
    lastAuthAt:
      type: timestamp
    sev:
      type: int64
`

var anchorT0 = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

// clockAt fixa o relógio do materializer num instante que o teste move.
func (h *harness) clockAt(t *time.Time) {
	h.m.UsaRelogio(func() time.Time { return *t })
}

// deadline devolve o vencimento gravado: `_writtenAt + _expiresIn`, que é a
// conta que o storage faz. É ele, e não o `_expiresIn` sozinho, que as
// garantias da âncora fixam.
func (h *harness) deadline(key string) time.Time {
	h.t.Helper()
	st := h.state(key)
	w, ok := st[schema.WriteInstantFieldName]
	if !ok {
		h.t.Fatalf("key %q sem %s", key, schema.WriteInstantFieldName)
	}
	return time.UnixMilli(w.I64 + h.storedTTL(key).Milliseconds()).UTC()
}

func authEvent(at time.Time) string {
	return fmt.Sprintf(`{"id":"a","lastAuthAt":"%s"}`, at.UTC().Format(time.RFC3339Nano))
}

// Garantia 1: a escrita que NÃO muda a âncora não renova o prazo.
func TestAnchorWriteThatKeepsTheAnchorDoesNotRenew(t *testing.T) {
	h := newHarness(t, anchorYAML)
	now := anchorT0
	h.clockAt(&now)
	h.apply(authEvent(anchorT0))
	want := anchorT0.Add(48 * time.Hour)
	if got := h.deadline("a"); !got.Equal(want) {
		t.Fatalf("vencimento %v, quero %v", got, want)
	}
	now = anchorT0.Add(10 * time.Hour)
	h.apply(`{"id":"a","sev":3}`) // dying gasp: não traz lastAuthAt
	if got := h.deadline("a"); !got.Equal(want) {
		t.Errorf("vencimento %v depois da escrita sem âncora, quero %v: a escrita renovou o prazo", got, want)
	}
}

// Garantia 2: a escrita que muda a âncora move o prazo para `nova + d`.
func TestAnchorWriteThatMovesTheAnchorMovesTheDeadline(t *testing.T) {
	h := newHarness(t, anchorYAML)
	now := anchorT0
	h.clockAt(&now)
	h.apply(authEvent(anchorT0))
	now = anchorT0.Add(10 * time.Hour)
	h.apply(authEvent(now))
	if got, want := h.deadline("a"), now.Add(48*time.Hour); !got.Equal(want) {
		t.Errorf("vencimento %v, quero %v (nova âncora + 48h)", got, want)
	}
}

// Garantia 3: âncora ausente cai na próxima regra — e a série conta a regra
// que faltou.
func TestAnchorAbsentFallsToTheNextRule(t *testing.T) {
	h := newHarness(t, anchorFirstYAML)
	now := anchorT0
	h.clockAt(&now)
	h.apply(`{"id":"a","sev":1}`)
	if got := h.storedTTL("a"); got != time.Hour {
		t.Errorf("prazo %v, quero 1h (a regra seguinte)", got)
	}
	m := h.m.Metrics()
	if m.TTLAnchorAbsent[0] != 1 || m.TTLRuleMatches[0] != 0 || m.TTLRuleMatches[1] != 1 {
		t.Errorf("anchorAbsent=%v matches=%v, quero [1 0] e [0 1]", m.TTLAnchorAbsent, m.TTLRuleMatches)
	}
}

// Garantia 3, sem próxima regra: a escrita fica sem prazo — como hoje,
// quando nenhuma regra casa e não há fundo.
func TestAnchorAbsentWithoutNextRuleDoesNotExpire(t *testing.T) {
	h := newHarness(t, `
version: 1
name: s
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - anchor: lastAuthAt
      expireAfter: 48h
schema:
  fields:
    id:
      type: string
    lastAuthAt:
      type: timestamp
`)
	h.apply(`{"id":"a"}`)
	if got := h.storedTTL("a"); got != 0 {
		t.Errorf("prazo %v, quero 0 (não expira)", got)
	}
	if m := h.m.Metrics(); m.TTLUnmatched != 1 || m.TTLAnchorAbsent[0] != 1 {
		t.Errorf("unmatched=%d anchorAbsent=%v, quero 1 e [1]", m.TTLUnmatched, m.TTLAnchorAbsent)
	}
}

// A série de âncora ausente só conta quando o `when` casou: a regra que não
// casaria de qualquer jeito não "perdeu" a âncora.
func TestAnchorAbsentCountsOnlyWhenTheConditionMatched(t *testing.T) {
	h := newHarness(t, `
version: 1
name: s
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - when: "sev > 3"
      anchor: lastAuthAt
      expireAfter: 48h
    - expireAfterWrite: 1h
schema:
  fields:
    id:
      type: string
    lastAuthAt:
      type: timestamp
    sev:
      type: int64
`)
	h.apply(`{"id":"a","sev":1}`)
	h.apply(`{"id":"b","sev":9}`)
	if m := h.m.Metrics(); m.TTLAnchorAbsent[0] != 1 {
		t.Errorf("anchorAbsent = %v, quero [1 0]: só a escrita com sev > 3 conta", m.TTLAnchorAbsent)
	}
}

// Garantia 9: `when` e `anchor` na mesma regra valem as duas.
func TestAnchorWithConditionRequiresBoth(t *testing.T) {
	h := newHarness(t, `
version: 1
name: s
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - when: "sev > 3"
      anchor: lastAuthAt
      expireAfter: 48h
    - expireAfterWrite: 1h
schema:
  fields:
    id:
      type: string
    lastAuthAt:
      type: timestamp
    sev:
      type: int64
`)
	now := anchorT0
	h.clockAt(&now)
	h.apply(fmt.Sprintf(`{"id":"a","sev":9,"lastAuthAt":"%s"}`, anchorT0.Add(-time.Hour).Format(time.RFC3339)))
	if got := h.storedTTL("a"); got != 47*time.Hour {
		t.Errorf("as duas condições: prazo %v, quero 47h", got)
	}
	h.apply(`{"id":"a","sev":1}`) // a âncora continua, o when deixa de casar
	if got := h.storedTTL("a"); got != time.Hour {
		t.Errorf("só a âncora: prazo %v, quero 1h (a regra seguinte)", got)
	}
}

// Garantia 5: âncora no futuro é limitada ao instante da escrita.
func TestAnchorInTheFutureIsClampedToTheWrite(t *testing.T) {
	h := newHarness(t, anchorYAML)
	now := anchorT0
	h.clockAt(&now)
	h.apply(authEvent(anchorT0.Add(3 * time.Hour)))
	if got := h.storedTTL("a"); got != 48*time.Hour {
		t.Errorf("prazo %v, quero 48h: a âncora futura comprou vida além da regra", got)
	}
	if m := h.m.Metrics(); m.TTLAnchorInFuture != 1 || m.TTLExpiredOnWrite != 0 {
		t.Errorf("inFuture=%d expiredOnWrite=%d, quero 1 e 0", m.TTLAnchorInFuture, m.TTLExpiredOnWrite)
	}
}

// Garantia 4, no materializer: âncora vencida grava o MENOR prazo mortal —
// nunca zero, que o storage lê como imortal.
func TestAnchorExpiredOnWriteStoresTheMinimumMortalTTL(t *testing.T) {
	h := newHarness(t, anchorYAML)
	now := anchorT0
	h.clockAt(&now)
	h.apply(authEvent(anchorT0.Add(-72 * time.Hour)))
	if got := h.storedTTL("a"); got != 2*time.Millisecond {
		t.Errorf("prazo %v, quero 2ms (o menor prazo mortal)", got)
	}
	if m := h.m.Metrics(); m.TTLExpiredOnWrite != 1 {
		t.Errorf("expiredOnWrite = %d, quero 1", m.TTLExpiredOnWrite)
	}
}

// T9: o prazo é função do RECORD. A conta parte do `_writtenAt` carimbado
// nele, e não de um relógio relido: com um relógio que avança entre leituras,
// `_writtenAt + _expiresIn` continua sendo `âncora + d`.
func TestAnchorDeadlineIsAFunctionOfTheRecord(t *testing.T) {
	h := newHarness(t, anchorYAML)
	reads := 0
	h.m.UsaRelogio(func() time.Time {
		reads++
		return anchorT0.Add(time.Duration(reads-1) * time.Hour)
	})
	anchor := anchorT0.Add(-time.Hour)
	h.apply(authEvent(anchor))
	if got, want := h.deadline("a"), anchor.Add(48*time.Hour); !got.Equal(want) {
		t.Errorf("vencimento %v, quero %v: a conta leu um relógio diferente do carimbado no record", got, want)
	}
}

// Garantia 11: a âncora sobre a congelada de relógio dá TETO de idade
// absoluta — escrita nenhuma o move.
func TestAnchorOnFrozenWriteTimeCapsAbsoluteAge(t *testing.T) {
	h := newHarness(t, `
version: 1
name: s
schemaName: telecom
materialization:
  key:
    field: id
  ttl:
    - anchor: firstSeenAt
      expireAfter: 1h
schema:
  fields:
    id:
      type: string
    sev:
      type: int64
    firstSeenAt:
      frozenFrom: "@writeTime"
`)
	now := anchorT0
	h.clockAt(&now)
	want := anchorT0.Add(time.Hour)
	for i := 0; i < 3; i++ {
		h.apply(fmt.Sprintf(`{"id":"a","sev":%d}`, i))
		if got := h.deadline("a"); !got.Equal(want) {
			t.Fatalf("escrita %d em %v: vencimento %v, quero %v (primeira escrita + 1h)", i, now, got, want)
		}
		now = now.Add(20 * time.Minute)
	}
}

// T3: a conta é por comparação e não estoura em nenhum extremo. d = 48h.
func TestAnchoredExpiresInBoundaries(t *testing.T) {
	const d = int64(48 * time.Hour / time.Millisecond)
	w := anchorT0.UnixMilli()
	minMortal := int64(rowindex.MinMortalTTL)
	cases := []struct {
		name          string
		anchor, wrote int64
		want          int64
		outcome       anchorOutcome
	}{
		{"no instante da escrita", w, w, d, anchorInRange},
		{"no meio", w - d/2, w, d - d/2, anchorInRange},
		{"fronteira exata: vence na escrita", w - d, w, minMortal, anchorExpiredOnWrite},
		{"1 ms antes da fronteira: o resto de 1 ms sobe ao mínimo", w - d + 1, w, minMortal, anchorInRange},
		{"2 ms antes da fronteira", w - d + 2, w, 2, anchorInRange},
		{"MinInt64", math.MinInt64, w, minMortal, anchorExpiredOnWrite},
		{"1 ms no futuro", w + 1, w, d, anchorInFuture},
		{"MaxInt64", math.MaxInt64, w, d, anchorInFuture},
		{"escrita perto de MinInt64", math.MinInt64, math.MinInt64 + 5, d - 5, anchorInRange},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, outcome := anchoredExpiresIn(tc.anchor, tc.wrote, d)
			if got != tc.want || outcome != tc.outcome {
				t.Errorf("anchoredExpiresIn = (%d, %d), quero (%d, %d)", got, outcome, tc.want, tc.outcome)
			}
			if got < minMortal {
				t.Errorf("prazo %d abaixo do mínimo mortal %d: o storage o leria como imortal", got, minMortal)
			}
		})
	}
}
