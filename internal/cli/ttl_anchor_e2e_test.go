package cli_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// serviceStateTTLYAML é o contrato aprovado com o service-view: sem
// autenticação, 1h renovável; autenticado, 48h contadas da última
// autenticação — o dying gasp do TEMS não renova. O contador conta, entre os
// serviços que já tiveram autenticação, os autenticados.
const serviceStateTTLYAML = `
version: 1
name: service_state
schemaName: telecom
materialization:
  key:
    field: customerId
  ttl:
    - when: "lastAuthAt IS NULL"
      expireAfterWrite: 1h
    - anchor: lastAuthAt
      expireAfter: 48h
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    authenticated:
      type: boolean
    lastAuthAt:
      type: timestamp
    dyingGasp:
      type: boolean
counters:
  - name: auth
    groupBy: [olt]
    keyFormat: "AUTH.{olt}"
    scope: "authenticated IS NOT NULL"
    where: "authenticated = true"
`

// anchorBench é o benchTTL com os contadores ligados: storage real,
// materializer, contador mantido na escrita e o MESMO relógio controlado.
type anchorBench struct {
	*benchTTL
	auth *counter.Counter
}

func newAnchorBench(t *testing.T, yaml string) *anchorBench {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	comp := counters.NewCompiler(spec)
	st, err := storage.Open(storage.Options{
		Dir:                             t.TempDir(),
		Dataset:                         spec.Name,
		IndexedFieldsByName:             schema.IndexedFieldNames(spec),
		ActiveTTL:                       len(spec.Materialization.TTLRules) > 0,
		FallbackDuration:                spec.Materialization.FallbackDuration,
		WriteInstantFieldName:           schema.WriteInstantFieldName,
		TTLFieldName:                    schema.TTLFieldName,
		CompileCountersCallbackFunction: comp.Callback(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	mat, err := materializer.New(comp.Compiled(), st)
	if err != nil {
		t.Fatal(err)
	}
	b := &anchorBench{benchTTL: &benchTTL{t: t, st: st, mat: mat, agora: time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)}}
	relogio := func() time.Time { return b.agora }
	mat.UsaRelogio(relogio)
	st.UsaRelogio(relogio)
	for _, c := range st.Counters() {
		b.auth = c
	}
	return b
}

// group devolve (total, rows) do grupo, e zeros quando ele não existe.
func (b *anchorBench) group(text string) (int64, int64) {
	b.t.Helper()
	if b.auth == nil {
		b.t.Fatal("o dataset não abriu o contador")
	}
	id, ok := b.auth.ByText([]byte(text))
	if !ok {
		return 0, 0
	}
	_, _, total, rows, _ := b.auth.Group(id)
	return total, rows
}

func (b *anchorBench) expire() storage.ResultOfExpiration {
	b.t.Helper()
	r, err := b.st.ExpiraOnePass(context.Background(), storage.LimitesOfExpiration{
		MaxByPassada: 100, TimeByPassada: time.Second,
	})
	if err != nil {
		b.t.Fatal(err)
	}
	return r
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// Garantia 4: a linha cuja âncora já venceu na escrita fica invisível a
// partir de `_writtenAt + 2 ms` e o coletor lhe dá a lápide — NUNCA imortal.
// Sem a trava, o prazo ≤ 0 seria gravado como imortal e a linha viveria para
// sempre.
func TestAnchorExpiredOnWriteIsInvisibleAndCollected(t *testing.T) {
	b := newAnchorBench(t, serviceStateTTLYAML)
	t0 := b.agora
	b.ingere(fmt.Sprintf(`{"customerId":"C","olt":"O1","authenticated":true,"lastAuthAt":"%s"}`, ts(t0.Add(-72*time.Hour))))

	b.advance(2 * time.Millisecond)
	if b.visivel("C") {
		t.Fatal("a linha nascida vencida está visível 2 ms depois da escrita")
	}
	b.advance(time.Hour)
	if b.visivel("C") {
		t.Fatal("a linha nascida vencida está visível 1h depois da escrita")
	}
	if r := b.expire(); r.Expiradas != 1 {
		t.Fatalf("o coletor não deu a lápide à linha nascida vencida: %+v", r)
	}
	if _, _, pres, err := b.st.State([]byte("C"), nil); err != nil || pres != types.PresenceExpired {
		t.Errorf("presença = %v (err %v), quero expirada", pres, err)
	}
}

// Garantia 7, no storage: a regra ancorada na última posição não é fundo, e a
// linha legada (sem `_expiresIn`) continua sem prazo — não ganha o da âncora.
func TestAnchorLastRuleGivesNoDeadlineToLegacyRows(t *testing.T) {
	b := newAnchorBench(t, serviceStateTTLYAML)
	payload, err := payloadLegado(b.benchTTL, "legada")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.st.Put([]byte("legada"), 1, 0, payload, nil); err != nil {
		t.Fatal(err)
	}
	b.advance(100 * 24 * time.Hour)
	if !b.visivel("legada") {
		t.Error("a linha legada expirou pelo prazo de uma regra ancorada que nunca a avaliou")
	}
}

// Garantia 11, no storage: âncora sobre a congelada `@writeTime` é teto de
// idade absoluta — escrevendo a cada 20 minutos, a linha vence 1h depois da
// primeira escrita.
func TestAnchorOnFrozenWriteTimeExpiresDespiteWrites(t *testing.T) {
	b := newAnchorBench(t, `
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
	for i := 0; i < 3; i++ {
		b.ingere(fmt.Sprintf(`{"id":"a","sev":%d}`, i))
		b.advance(20 * time.Minute)
	}
	b.advance(-time.Minute) // primeira escrita + 59min
	if !b.visivel("a") {
		t.Fatal("a linha venceu antes do teto")
	}
	b.advance(time.Minute)
	if b.visivel("a") {
		t.Error("a linha passou do teto de 1h desde a primeira escrita: as escritas renovaram o prazo")
	}
}

// Garantia 13 e T5, de ponta a ponta com o contrato: o serviço autenticado
// que só recebe dying gasp vence em lastAuthAt + 48h; o que só recebe dying
// gasp vive 1h renovável; o que nasce vencido (backlog relido) é contado até
// a lápide e renasce do vazio. O contador com `scope` acompanha cada passo.
func TestServiceStateContractEndToEnd(t *testing.T) {
	b := newAnchorBench(t, serviceStateTTLYAML)
	t0 := b.agora
	const grp = "AUTH.O1"
	b.ingere(fmt.Sprintf(`{"customerId":"A","olt":"O1","authenticated":true,"lastAuthAt":"%s"}`, ts(t0)))
	b.ingere(`{"customerId":"B","olt":"O1","dyingGasp":true}`)
	b.ingere(fmt.Sprintf(`{"customerId":"C","olt":"O1","authenticated":true,"lastAuthAt":"%s"}`, ts(t0.Add(-72*time.Hour))))

	// T5: C nasceu vencida e some da consulta, mas o contador a conta até a
	// lápide — "viva" para ele é linha sem lápide.
	b.advance(2 * time.Millisecond)
	if !b.visivel("A") || !b.visivel("B") || b.visivel("C") {
		t.Fatalf("visíveis A=%v B=%v C=%v, quero true true false", b.visivel("A"), b.visivel("B"), b.visivel("C"))
	}
	if total, rows := b.group(grp); total != 2 || rows != 2 {
		t.Fatalf("antes do coletor: total %d rows %d, quero 2 e 2 (A e C; B fora do scope)", total, rows)
	}
	b.advance(time.Minute)
	if r := b.expire(); r.Expiradas != 1 {
		t.Fatalf("coletor: %+v, quero só C expirada", r)
	}
	if total, rows := b.group(grp); total != 1 || rows != 1 {
		t.Fatalf("depois do coletor: total %d rows %d, quero 1 e 1 (só A)", total, rows)
	}

	// B: só dying gasp, 1h renovável.
	b.agora = t0.Add(50 * time.Minute)
	b.ingere(`{"customerId":"B","olt":"O1","dyingGasp":true}`)
	b.agora = t0.Add(time.Hour + 40*time.Minute)
	if !b.visivel("B") {
		t.Fatal("B morreu antes de 1h depois da escrita que a renovou")
	}
	b.agora = t0.Add(time.Hour + 51*time.Minute)
	if b.visivel("B") {
		t.Fatal("B passou de 1h sem escrita e continua visível")
	}

	// T5: C renasce do VAZIO com um dying gasp — sem lastAuthAt e sem
	// authenticated da encarnação anterior —, então cai na regra de 1h e fica
	// fora do scope do contador.
	b.ingere(`{"customerId":"C","olt":"O1","dyingGasp":true}`)
	if got := b.storedTTL("C"); got != time.Hour {
		t.Errorf("C renascida: prazo %v, quero 1h (sem lastAuthAt)", got)
	}
	if total, rows := b.group(grp); total != 1 || rows != 1 {
		t.Errorf("C renascida entrou no contador: total %d rows %d, quero 1 e 1", total, rows)
	}

	// A: dying gasps não renovam; vence em lastAuthAt + 48h.
	b.agora = t0.Add(10 * time.Hour)
	b.ingere(`{"customerId":"A","olt":"O1","dyingGasp":true}`)
	b.agora = t0.Add(30 * time.Hour)
	b.ingere(`{"customerId":"A","olt":"O1","dyingGasp":true}`)
	b.agora = t0.Add(48*time.Hour - time.Minute)
	if !b.visivel("A") {
		t.Fatal("A venceu antes de lastAuthAt + 48h")
	}
	b.agora = t0.Add(48 * time.Hour)
	if b.visivel("A") {
		t.Fatal("A continua visível em lastAuthAt + 48h: os dying gasps renovaram o prazo")
	}
	if total, rows := b.group(grp); total != 1 || rows != 1 {
		t.Fatalf("A vencida antes do coletor: total %d rows %d, quero 1 e 1", total, rows)
	}
	b.advance(time.Minute)
	if r := b.expire(); r.Expiradas < 1 {
		t.Fatalf("coletor não deu a lápide a A: %+v", r)
	}
	if total, rows := b.group(grp); total != 0 || rows != 0 {
		t.Errorf("depois da lápide de A: total %d rows %d, quero 0 e 0", total, rows)
	}
}
