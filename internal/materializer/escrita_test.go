package materializer

import (
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const yamlComExpire = `
version: 1
name: alarmes
materialization:
  key:
    field: id
  mode: merge
  ttl:
    - expireAfterWrite: 2h
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    severidade:
      type: int64
`

func compiladoComExpire(t *testing.T, yaml string) (*schema.Compiled, *fakestorage.Store) {
	t.Helper()
	spec, err := schema.Parse([]byte(yaml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	return c, fakestorage.New(fakestorage.Indexes{})
}

// leInstanteDeEscrita extrai o campo de sistema do payload materializado.
func leInstanteDeEscrita(t *testing.T, c *schema.Compiled, st *fakestorage.Store, key string) (int64, bool) {
	t.Helper()
	rec, _, presente, err := st.GetByKey([]byte(key), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !presente {
		t.Fatalf("key %q não materializada", key)
	}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		id, v, ok := r.Next()
		if !ok {
			break
		}
		if id == c.Escrita.ID {
			if v.Type != types.TypeTimestamp {
				t.Fatalf("campo de sistema com tipo %v", v.Type)
			}
			return v.I64, true
		}
	}
	return 0, false
}

// O idle timeout depende disto: TODA escrita renova o instante, ou uma
// entidade que recebe update sem parar expiraria do mesmo jeito.
func TestCadaEscritaRenovaOInstante(t *testing.T) {
	for _, modo := range []string{"merge", "replace"} {
		t.Run(modo, func(t *testing.T) {
			yaml := strings.Replace(yamlComExpire, "mode: merge", "mode: "+modo, 1)
			c, st := compiladoComExpire(t, yaml)
			m := mustNew(t, c, st)

			relogio := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
			m.UsaRelogio(func() time.Time { return relogio })
			if r := m.Apply([]byte(`{"id":"A","severidade":3}`)); r.Err != nil {
				t.Fatal(r.Err)
			}
			primeiro, ok := leInstanteDeEscrita(t, c, st, "A")
			if !ok {
				t.Fatal("o campo de sistema não foi gravado na criação")
			}
			if primeiro != relogio.UnixMilli() {
				t.Fatalf("instante = %d, quer %d", primeiro, relogio.UnixMilli())
			}

			relogio = relogio.Add(90 * time.Minute)
			if r := m.Apply([]byte(`{"id":"A","severidade":4}`)); r.Err != nil {
				t.Fatal(r.Err)
			}
			segundo, ok := leInstanteDeEscrita(t, c, st, "A")
			if !ok {
				t.Fatal("o campo de sistema sumiu no update")
			}
			if segundo != relogio.UnixMilli() {
				t.Fatalf("o update NÃO renovou o instante: %d, quer %d", segundo, relogio.UnixMilli())
			}
		})
	}
}

// Sem expiração declarada não há por que pagar 11 bytes por record.
func TestSemExpireAfterWriteNaoHaCampoDeSistema(t *testing.T) {
	yaml := strings.Replace(yamlComExpire, "  ttl:\n    - expireAfterWrite: 2h\n", "", 1)
	c, st := compiladoComExpire(t, yaml)
	if c.Escrita != nil {
		t.Fatal("campo de sistema compilado sem expiração declarada")
	}
	m := mustNew(t, c, st)
	if r := m.Apply([]byte(`{"id":"A","severidade":3}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	rec, _, presente, err := st.GetByKey([]byte("A"), nil)
	if err != nil || !presente {
		t.Fatalf("presente=%v err=%v", presente, err)
	}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		id, _, ok := r.Next()
		if !ok {
			break
		}
		if f, achou := c.FieldByID(id); achou && f.IsSystem {
			t.Fatal("campo de sistema gravado num dataset sem expiração")
		}
	}
}

// Aceitar o campo vindo do evento deixaria o produtor controlar quando a
// própria entidade expira — um valor no futuro a tornaria imortal, sem erro.
func TestEventoQueTrazOCampoDeSistemaEhRecusado(t *testing.T) {
	c, st := compiladoComExpire(t, yamlComExpire)
	m := mustNew(t, c, st)
	r := m.Apply([]byte(`{"id":"A","` + schema.CampoInstanteDeEscrita + `":253402300799000}`))
	if r.Err == nil {
		t.Fatal("evento com o campo de sistema foi aceito")
	}
	if r.Action != ActionSkipped && r.Action != ActionFailed {
		t.Fatalf("Action = %v, quer Skipped ou Failed", r.Action)
	}
	if !strings.Contains(r.Err.Error(), schema.CampoInstanteDeEscrita) {
		t.Errorf("a mensagem não nomeia o campo: %v", r.Err)
	}
}

// Todo campo do mesmo evento carimba o MESMO instante: um time.Now por campo
// produziria carimbos distintos dentro de uma escrita só.
func TestOInstanteEhOMesmoParaOEventoInteiro(t *testing.T) {
	c, st := compiladoComExpire(t, yamlComExpire)
	m := mustNew(t, c, st)
	chamadas := 0
	base := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	m.UsaRelogio(func() time.Time {
		chamadas++
		return base.Add(time.Duration(chamadas) * time.Second)
	})
	if r := m.Apply([]byte(`{"id":"A","severidade":3}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	if chamadas != 1 {
		t.Fatalf("o relógio foi lido %d vezes num evento; tem de ser 1", chamadas)
	}
}

// O débito 24, provado. Enquanto GetByKey colapsava "ausente" e "expirada"
// num ok=false, o stale check do §25 não rodava sobre a entidade expirada: o
// materializer a tratava como key NOVA e a recriava a partir de um evento
// antigo — a ressurreição que o §58 diz que o tombstone deve impedir.
func TestEventoAntigoNaoRessuscitaEntidadeExpirada(t *testing.T) {
	yaml := `
version: 1
name: alarmes
materialization:
  key:
    field: id
  mode: merge
  sequence:
    field: seq
    type: int64
    strategy: greater
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    seq:
      type: int64
    severidade:
      type: int64
`
	c, st := compiladoComExpire(t, yaml)
	m := mustNew(t, c, st)

	if r := m.Apply([]byte(`{"id":"A","seq":10,"severidade":3}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	rowID, ok := st.RowIDDaKey("A")
	if !ok {
		t.Fatal("key não materializada")
	}
	st.SetTombstone(rowID)

	// Evento ATRASADO (sequence menor que a corrente) sobre a entidade
	// expirada: tem de ser descartado como stale.
	r := m.Apply([]byte(`{"id":"A","seq":5,"severidade":9}`))
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if r.Action != ActionStale {
		t.Fatalf("Action = %v, quer Stale: o evento antigo ressuscitou a entidade", r.Action)
	}

	// Evento NOVO reabre legitimamente, no MESMO rowId (a primary ainda
	// nomeia a key; alocar um rowId novo deixaria o antigo órfão).
	r = m.Apply([]byte(`{"id":"A","seq":20,"severidade":9}`))
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if r.Action == ActionStale {
		t.Fatal("o evento novo foi descartado como stale")
	}
	if r.RowID != rowID {
		t.Fatalf("ressurreição alocou rowId novo: %d, quer %d", r.RowID, rowID)
	}
}

// Merge sobre a lápide arrastaria os campos da encarnação anterior para a
// entidade que reabre: um alarme novo herdaria a severidade do que foi
// fechado.
func TestRessurreicaoNaoFazMergeSobreOPayloadDaLapide(t *testing.T) {
	c, st := compiladoComExpire(t, yamlComExpire)
	m := mustNew(t, c, st)

	if r := m.Apply([]byte(`{"id":"A","severidade":3,"extra":"velho"}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	rowID, ok := st.RowIDDaKey("A")
	if !ok {
		t.Fatal("key não materializada")
	}
	st.SetTombstone(rowID)

	if r := m.Apply([]byte(`{"id":"A","severidade":9}`)); r.Err != nil {
		t.Fatal(r.Err)
	}
	st.LimpaTombstone(rowID)

	rec, _, presente, err := st.GetByKey([]byte("A"), nil)
	if err != nil || !presente {
		t.Fatalf("presente=%v err=%v", presente, err)
	}
	var r encoding.FieldReader
	r.Reset(rec.Payload)
	for {
		id, _, ok := r.Next()
		if !ok {
			break
		}
		if f, achou := c.FieldByID(id); achou && f.Name == "extra" {
			t.Fatal("a entidade reaberta herdou um campo da encarnação anterior")
		}
	}
}
