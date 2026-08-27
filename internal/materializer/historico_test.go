package materializer

import (
	"errors"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const specComHistorico = `version: 1
name: events
materialization:
  key:
    field: id
history:
  expireAfterWrite: 7d
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    severity:
      type: int64
    originalSeverity:
      frozenFrom: severity
`

// histFake é o dublê da tabela irmã: guarda os payloads e conta as chamadas,
// que é como se prova que a degradação é sticky.
type histFake struct {
	linhas  [][]byte
	falha   error
	chamado int
}

func (h *histFake) Apenda(payload []byte) (types.RowID, error) {
	h.chamado++
	if h.falha != nil {
		return 0, h.falha
	}
	// Cópia: o materializer reutiliza o buffer entre eventos.
	linha := make([]byte, len(payload))
	copy(linha, payload)
	h.linhas = append(h.linhas, linha)
	return types.RowID(len(h.linhas)), nil
}

// specComRaw é a mesma spec com a coluna _raw ligada. Duas constantes em vez
// de cirurgia de string: o YAML é o insumo do teste, e um insumo montado por
// substituição textual esconde o que está sendo testado.
const specComRaw = `version: 1
name: events
materialization:
  key:
    field: id
history:
  expireAfterWrite: 7d
  rawPayload: true
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    severity:
      type: int64
    originalSeverity:
      frozenFrom: severity
`

// parComHistorico monta o materializer do dataset base com a tabela irmã
// ligada, compilando as duas specs contra o MESMO dicionário — que é a
// condição sem a qual os FieldIDs divergem e o payload decodifica errado.
func parComHistorico(t *testing.T, comRaw bool) (*Materializer, *schema.Compiled, *histFake) {
	t.Helper()
	yaml := specComHistorico
	if comRaw {
		yaml = specComRaw
	}
	spec, err := schema.Parse([]byte(yaml), schema.DefaultsPadrao)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dict := fakestorage.NewDict()
	c, err := spec.Compile(dict)
	if err != nil {
		t.Fatalf("Compile do base: %v", err)
	}
	ch, err := spec.DerivaHistorico().Compile(dict)
	if err != nil {
		t.Fatalf("Compile do histórico: %v", err)
	}
	c.Espelha(ch)

	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))
	h := &histFake{}
	var raw *schema.CompiledField
	if comRaw {
		f, ok := ch.FieldByName([]byte(schema.CampoRaw))
		if !ok {
			t.Fatal("a spec derivada não tem _raw")
		}
		raw = f
	}
	m.LigaHistorico(h, raw)
	// O Compiled do HISTÓRICO volta junto porque é ele que conhece o _raw: a
	// coluna não existe no schema do base, e resolver a linha contra o base
	// diria que o campo não existe.
	return m, ch, h
}

// A propriedade central: dois eventos da MESMA entidade são uma linha no base
// e duas no histórico.
func TestOHistoricoRecebeUmaLinhaPorEvento(t *testing.T) {
	m, _, h := parComHistorico(t, false)
	for _, ev := range []string{`{"id":"a","severity":1}`, `{"id":"a","severity":5}`} {
		if res := m.Apply([]byte(ev)); res.Category != CategoryNone {
			t.Fatalf("Apply(%s) = %v", ev, res.Category)
		}
	}
	if len(h.linhas) != 2 {
		t.Fatalf("%d linhas no histórico, quer 2", len(h.linhas))
	}
	if got := m.Metrics().HistoricoLinhas; got != 2 {
		t.Fatalf("HistoricoLinhas = %d, quer 2", got)
	}
}

// A captura emite a entry com o valor CORRENTE da origem; é o merge que
// preserva o primeiro. No histórico, originalSeverity seria igual a severity
// em toda linha.
func TestACongeladaNaoVaiParaOHistorico(t *testing.T) {
	m, ch, h := parComHistorico(t, false)
	if res := m.Apply([]byte(`{"id":"a","severity":3}`)); res.Category != CategoryNone {
		t.Fatalf("Apply = %v", res.Category)
	}
	if len(h.linhas) != 1 {
		t.Fatalf("%d linhas, quer 1", len(h.linhas))
	}
	nomes := colunasDaLinha(t, ch, h.linhas[0])
	if temColuna(nomes, "originalSeverity") {
		t.Fatalf("a congelada foi para o histórico: %v", nomes)
	}
	if !temColuna(nomes, "severity") {
		t.Fatalf("a origem da congelada sumiu junto: %v", nomes)
	}
}

func colunasDaLinha(t *testing.T, c *schema.Compiled, payload []byte) []string {
	t.Helper()
	var r encoding.FieldReader
	r.Reset(payload)
	var out []string
	for {
		id, _, ok := r.Next()
		if !ok {
			break
		}
		f, achou := c.FieldByID(id)
		if !achou {
			t.Fatalf("campo %d não está no schema", id)
		}
		out = append(out, f.Name)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("payload do histórico ilegível: %v", err)
	}
	return out
}

func temColuna(nomes []string, alvo string) bool {
	for _, n := range nomes {
		if n == alvo {
			return true
		}
	}
	return false
}

// O histórico é ferramenta; o produto é o dataset base. Uma falha ali não
// pode mudar o desfecho da ingestão.
func TestFalhaNoHistoricoNaoMudaOApplyResult(t *testing.T) {
	m, _, h := parComHistorico(t, false)
	h.falha = errors.New("disco cheio")
	res := m.Apply([]byte(`{"id":"a","severity":1}`))
	if res.Category != CategoryNone {
		t.Fatalf("Category = %v, quer nenhuma", res.Category)
	}
	if res.Action != ActionCreated {
		t.Fatalf("Action = %v, quer created", res.Action)
	}
}

// Sem a marca sticky, cada evento tentaria de novo e o log viraria uma linha
// por evento sobre uma condição que não muda sozinha.
func TestFalhaNoHistoricoEhSticky(t *testing.T) {
	m, _, h := parComHistorico(t, false)
	h.falha = errors.New("disco cheio")
	for range 5 {
		m.Apply([]byte(`{"id":"a","severity":1}`))
	}
	if h.chamado != 1 {
		t.Fatalf("o histórico foi chamado %d vezes depois de falhar, quer 1", h.chamado)
	}
	if m.HistoricoDegradado() == nil {
		t.Fatal("HistoricoDegradado = nil depois da falha")
	}
	snap := m.Metrics()
	if !snap.HistoricoDegradado {
		t.Error("o snapshot não marca a degradação")
	}
	if snap.HistoricoDescartadas != 5 {
		t.Errorf("HistoricoDescartadas = %d, quer 5", snap.HistoricoDescartadas)
	}
}

// Sem a coluna _raw não há o que guardar de um JSON inválido, e o evento que
// nunca virou linha continua sendo só um número num contador.
func TestEventoRuimVaiParaOHistoricoComRaw(t *testing.T) {
	m, ch, h := parComHistorico(t, true)
	res := m.Apply([]byte(`{"id":"a", isso não é json`))
	if !res.Category.BadEvent() {
		t.Fatalf("Category = %v, quer evento ruim", res.Category)
	}
	if len(h.linhas) != 1 {
		t.Fatalf("%d linhas, quer 1", len(h.linhas))
	}
	nomes := colunasDaLinha(t, ch, h.linhas[0])
	if temColuna(nomes, "id") {
		t.Fatalf("a linha do evento ruim tem colunas do schema: %v", nomes)
	}
}

func TestEventoRuimNaoVaiSemRaw(t *testing.T) {
	m, _, h := parComHistorico(t, false)
	if res := m.Apply([]byte(`{"id":"a", isso não é json`)); !res.Category.BadEvent() {
		t.Fatalf("Category = %v, quer evento ruim", res.Category)
	}
	if len(h.linhas) != 0 {
		t.Fatalf("%d linhas sem rawPayload, quer 0", len(h.linhas))
	}
}

// O payload exige ordem estritamente crescente de FieldID, e o id de _raw não
// é necessariamente o maior: toda coluna descoberta depois do Compile do
// histórico ganha um id maior que o dele.
func TestRawEntraNaPosicaoDoFieldID(t *testing.T) {
	m, ch, h := parComHistorico(t, true)
	// A chave `zzz` é descoberta em runtime e ganha um FieldID MAIOR que o
	// de _raw, que foi atribuído quando o histórico compilou.
	if res := m.Apply([]byte(`{"id":"a","severity":1,"zzz":"depois"}`)); res.Category != CategoryNone {
		t.Fatalf("Apply = %v", res.Category)
	}
	if len(h.linhas) != 1 {
		t.Fatalf("%d linhas, quer 1", len(h.linhas))
	}
	if _, err := encoding.ValidatePayload(h.linhas[0]); err != nil {
		t.Fatalf("payload do histórico fora de ordem: %v", err)
	}
	nomes := colunasDaLinha(t, ch, h.linhas[0])
	if !temColuna(nomes, schema.CampoRaw) || !temColuna(nomes, "zzz") {
		t.Fatalf("colunas = %v", nomes)
	}
}

func TestSemHistoricoNadaEhGravado(t *testing.T) {
	spec, err := schema.Parse([]byte(specComHistorico), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))
	if res := m.Apply([]byte(`{"id":"a","severity":1}`)); res.Category != CategoryNone {
		t.Fatalf("Apply = %v", res.Category)
	}
	if snap := m.Metrics(); snap.HistoricoLinhas != 0 || snap.HistoricoDescartadas != 0 {
		t.Fatalf("contadores do histórico se moveram sem histórico: %+v", snap)
	}
}

const specComSequence = `version: 1
name: events
materialization:
  key:
    field: id
  sequence:
    field: seq
history:
  expireAfterWrite: 7d
schema:
  include: ["*"]
  fields:
    id:
      type: string
    seq:
      type: int64
    severity:
      type: int64
`

// O evento descartado por stale é justamente o que se quer ver ao depurar um
// descarte por stale: ele vai para o histórico, e é por isso que o enxerto
// fica ANTES da checagem, e não depois.
func TestOEventoStaleVaiParaOHistorico(t *testing.T) {
	spec, err := schema.Parse([]byte(specComSequence), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	dict := fakestorage.NewDict()
	c, err := spec.Compile(dict)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spec.DerivaHistorico().Compile(dict); err != nil {
		t.Fatal(err)
	}
	m := mustNew(t, c, fakestorage.New(fakestorage.Indexes{}))
	h := &histFake{}
	m.LigaHistorico(h, nil)

	if res := m.Apply([]byte(`{"id":"a","seq":10,"severity":1}`)); res.Action != ActionCreated {
		t.Fatalf("primeiro Apply = %v", res.Action)
	}
	if res := m.Apply([]byte(`{"id":"a","seq":5,"severity":9}`)); res.Action != ActionStale {
		t.Fatalf("segundo Apply = %v, quer stale", res.Action)
	}
	if len(h.linhas) != 2 {
		t.Fatalf("%d linhas no histórico, quer 2 (o stale também é evidência)", len(h.linhas))
	}
}
