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
    expireAfterWrite: 2h
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
			m := New(c, st)

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
	yaml := strings.Replace(yamlComExpire, "  ttl:\n    expireAfterWrite: 2h\n", "", 1)
	c, st := compiladoComExpire(t, yaml)
	if c.Escrita != nil {
		t.Fatal("campo de sistema compilado sem expiração declarada")
	}
	m := New(c, st)
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
	m := New(c, st)
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
	m := New(c, st)
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
