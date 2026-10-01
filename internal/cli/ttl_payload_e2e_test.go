package cli_test

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// storedTTL lê o `_expiresIn` do record corrente da key, viva ou não — é o
// valor que o índice de prazos recebe e que o replay reconstrói.
func (b *benchTTL) storedTTL(key string) time.Duration {
	b.t.Helper()
	rec, _, _, err := b.st.State([]byte(key), nil)
	if err != nil {
		b.t.Fatal(err)
	}
	id, err := b.st.FieldDict().LookupOrCreate([]byte(schema.TTLFieldName))
	if err != nil {
		b.t.Fatal(err)
	}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		fid, v, ok := r.Next()
		if !ok {
			b.t.Fatalf("record de %q sem %s (err=%v)", key, schema.TTLFieldName, r.Err())
		}
		if fid == id && v.Type == types.TypeInt64 {
			return time.Duration(v.I64) * time.Millisecond
		}
	}
}

// A linha VENCIDA e ainda sem lápide (o coletor não passou) volta ao writer
// como expirada, e a escrita seguinte parte do VAZIO: o merge não roda sobre
// ela. O prazo tem de ser decidido sobre esse mesmo record novo. Avaliado
// sobre o estado anterior, o `tipo = 2` da encarnação morta casava, e a
// entidade que renasce sem `tipo` herdava o prazo curto de quem morreu.
func TestExpiredRowWithoutTombstoneRuleSeesOnlyTheNewRecord(t *testing.T) {
	b := newBenchTTL(t, ttlCondicionalYAML)
	b.ingere(`{"id":"a","tipo":2}`) // 3m

	b.advance(4 * time.Minute) // vencida, e o coletor não rodou
	if b.visivel("a") {
		t.Fatal("a linha devia estar vencida: o cenário do teste não vale")
	}
	b.ingere(`{"id":"a"}`) // renasce sem `tipo`
	if got := b.storedTTL("a"); got != 7*24*time.Hour {
		t.Fatalf("prazo %v, quero 7d (fundo): a regra casou com o `tipo` da encarnação vencida", got)
	}
	b.advance(4 * time.Minute)
	if !b.visivel("a") {
		t.Error("a entidade renascida morreu pelo prazo curto da encarnação anterior")
	}
}
