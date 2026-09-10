package cli_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A spec do ciclo: alarme de tipo 2 (fechado) morre em 3 minutos, o resto vive
// uma semana. É o caso que a feature existe para atender.
const ttlCondicionalYAML = `
version: 1
name: alarmes
materialization:
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
`

// bancadaTTL costura storage real + materializer com o MESMO relógio
// controlado: a expiração é sobre tempo, e um teste que dormisse minutos não
// caberia na suíte.
type bancadaTTL struct {
	t     *testing.T
	st    *storage.Store
	mat   *materializer.Materializer
	agora time.Time
}

func novaBancadaTTL(t *testing.T, yaml string) *bancadaTTL {
	t.Helper()
	dir := t.TempDir()
	caminho := filepath.Join(dir, "alarmes.yaml")
	if err := os.WriteFile(caminho, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	spec, err := schema.Load(caminho, schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{
		Dir:                    filepath.Join(dir, "dados"),
		CamposIndexadosPorNome: schema.IndexedFieldNames(spec),
		TTLAtivo:               len(spec.Materialization.TTLRules) > 0,
		DuracaoDeFundo:         spec.Materialization.FallbackDuration,
		CampoDeEscrita:         schema.WriteInstantFieldName,
		CampoDePrazo:           schema.TTLFieldName,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	c, err := spec.Compile(st.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.New(c, st)
	if err != nil {
		t.Fatal(err)
	}
	b := &bancadaTTL{t: t, st: st, mat: mat, agora: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)}
	relogio := func() time.Time { return b.agora }
	mat.UsaRelogio(relogio)
	st.UsaRelogio(relogio)
	return b
}

func (b *bancadaTTL) ingere(evento string) {
	b.t.Helper()
	if r := b.mat.Apply([]byte(evento)); r.Err != nil {
		b.t.Fatalf("Apply(%s): %v (%v)", evento, r.Err, r.Category)
	}
}

func (b *bancadaTTL) avanca(d time.Duration) { b.agora = b.agora.Add(d) }

// visivel responde a pergunta da CONSULTA: a entidade aparece?
func (b *bancadaTTL) visivel(key string) bool {
	b.t.Helper()
	_, _, ok, err := b.st.GetByKey(b.st.OpenSnapshot(), []byte(key), nil)
	if err != nil {
		b.t.Fatal(err)
	}
	return ok
}

func (b *bancadaTTL) coleta() storage.ResultadoDaExpiracao {
	b.t.Helper()
	r, err := b.st.ExpiraUmaPassada(context.Background(), storage.LimitesDeExpiracao{
		MaxPorPassada: 100, TempoPorPassada: time.Second,
	})
	if err != nil {
		b.t.Fatal(err)
	}
	return r
}

// O teste central da feature: o alarme fecha (prazo curto), REABRE antes de
// vencer, e o prazo curto é CANCELADO. Sem a emissão do prazo em toda escrita,
// a entidade morreria logo depois de reabrir.
func TestCicloCancelamentoDoTTLCondicional(t *testing.T) {
	b := novaBancadaTTL(t, ttlCondicionalYAML)
	b.ingere(`{"id":"a","tipo":2}`) // fechado: 3m

	b.avanca(90 * time.Second)
	b.ingere(`{"id":"a","tipo":1}`) // reabriu: 7d

	b.avanca(4 * time.Minute) // muito além dos 3m do primeiro evento
	if !b.visivel("a") {
		t.Fatal("a entidade morreu pelo prazo antigo depois de o alarme reabrir")
	}
	if r := b.coleta(); r.Expiradas != 0 {
		t.Errorf("o coletor expirou %d entidades vivas", r.Expiradas)
	}
}

func TestCicloExpiracaoQuandoACondicaoPersiste(t *testing.T) {
	b := novaBancadaTTL(t, ttlCondicionalYAML)
	b.ingere(`{"id":"a","tipo":2}`)

	b.avanca(4 * time.Minute)
	if b.visivel("a") {
		t.Fatal("a entidade fechada continua visível 4 minutos depois, com prazo de 3m")
	}
	r := b.coleta()
	if r.Expiradas != 1 {
		t.Fatalf("o coletor não deu a lápide: %+v", r)
	}
	// A lápide é durável, e o estado da ingestão vê a entidade como EXPIRADA —
	// é o que impede um evento antigo de ressuscitá-la.
	_, _, pres, err := b.st.State([]byte("a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pres != types.PresenceExpired {
		t.Errorf("presença = %v, quero expirada", pres)
	}
}

// `never` vence a regra de fundo que vem depois: é o ponto de ela existir.
func TestCicloNeverNaoExpiraComRegraDeFundoPresente(t *testing.T) {
	b := novaBancadaTTL(t, `
version: 1
name: alarmes
materialization:
  key:
    field: id
  ttl:
    - when: "tipo = 9"
      expireAfterWrite: never
    - expireAfterWrite: 3m
schema:
  fields:
    id:
      type: string
    tipo:
      type: int64
`)
	b.ingere(`{"id":"imortal","tipo":9}`)
	b.ingere(`{"id":"comum","tipo":1}`)

	b.avanca(4 * time.Minute)
	if !b.visivel("imortal") {
		t.Error("a entidade que casou com `never` expirou pelo prazo de fundo")
	}
	if b.visivel("comum") {
		t.Error("a entidade de prazo curto continua visível")
	}
}

// A renovação empurra o prazo: alarme que recebe update a cada minuto nunca
// expira, mesmo com a condição valendo o tempo todo.
//
// Isto NÃO é bug — é a decisão de desenho registrada como risco R1 da spec, e
// este teste existe com este nome para impedir que alguém a "conserte" por
// engano. Um teto de idade absoluta por regra é a feature que faltaria, e ela
// ficou de fora de propósito.
func TestCicloRenovacaoEmpurraOPrazo(t *testing.T) {
	b := novaBancadaTTL(t, ttlCondicionalYAML)
	b.ingere(`{"id":"a","tipo":2}`)
	for i := 0; i < 5; i++ {
		b.avanca(60 * time.Second)
		b.ingere(`{"id":"a","tipo":2}`)
		if !b.visivel("a") {
			t.Fatalf("a entidade expirou na renovação %d, com prazo de 3m e escrita a cada 60s", i)
		}
	}
	// Parou de receber evento: agora expira.
	b.avanca(4 * time.Minute)
	if b.visivel("a") {
		t.Error("parou de renovar e continuou visível")
	}
}

// A migração: entidade materializada ANTES de as regras existirem não tem o
// campo de prazo no documento, e continua expirando pelo prazo de FUNDO.
func TestCicloEntidadeLegadaUsaAduracaoDeFundo(t *testing.T) {
	b := novaBancadaTTL(t, ttlCondicionalYAML)
	// Escreve direto pelo store, sem o campo de prazo — é o que o writer
	// anterior à feature produzia.
	payload, err := payloadLegado(b, "legada")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.st.Put([]byte("legada"), 1, 0, payload, nil); err != nil {
		t.Fatal(err)
	}

	b.avanca(4 * time.Minute) // além do prazo curto, muito aquém do fundo
	if !b.visivel("legada") {
		t.Error("a entidade legada expirou pelo prazo de uma regra que ela nunca casou")
	}
	b.avanca(8 * 24 * time.Hour)
	if b.visivel("legada") {
		t.Error("a entidade legada não caiu no prazo de fundo")
	}
}

// payloadLegado monta o payload de uma entidade materializada ANTES de as
// regras existirem: instante de escrita sim, campo de prazo não.
func payloadLegado(b *bancadaTTL, key string) ([]byte, error) {
	b.t.Helper()
	dict := b.st.FieldDict()
	idID, err := dict.LookupOrCreate([]byte("id"))
	if err != nil {
		return nil, err
	}
	escritaID, err := dict.LookupOrCreate([]byte(schema.WriteInstantFieldName))
	if err != nil {
		return nil, err
	}
	campos := []struct {
		id types.FieldID
		v  types.Value
	}{
		{idID, types.Value{Type: types.TypeString, Bytes: []byte(key)}},
		{escritaID, types.Value{Type: types.TypeTimestamp, I64: b.agora.UnixMilli()}},
	}
	sort.Slice(campos, func(i, j int) bool { return campos[i].id < campos[j].id })
	var out []byte
	for _, c := range campos {
		if out, err = encoding.AppendField(out, c.id, c.v); err != nil {
			return nil, err
		}
	}
	return out, nil
}
