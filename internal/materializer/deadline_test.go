package materializer

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const prazoYAML = `
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
    sev:
      type: int64
`

// duracaoDoEvento roda o caminho REAL — Parse e depois a avaliação — sobre um
// estado anterior dado, e devolve a duração escolhida.
func duracaoDoEvento(t *testing.T, h *harness, prevPayload []byte, evento string) time.Duration {
	t.Helper()
	if err := h.m.patch.Parse([]byte(evento), h.c, time.Now()); err != nil {
		t.Fatalf("Parse(%s): %v", evento, err)
	}
	d, err := h.m.duracaoDe(prevPayload, h.m.patch.entries)
	if err != nil {
		t.Fatalf("duracaoDe: %v", err)
	}
	return d
}

func TestPrazoUsaAPrimeiraRegraQueCasa(t *testing.T) {
	h := newHarness(t, prazoYAML)
	if d := duracaoDoEvento(t, h, nil, `{"id":"a","tipo":2}`); d != 3*time.Minute {
		t.Errorf("duração = %v, quero 3m", d)
	}
}

func TestPrazoCaiNaRegraDeFundoQuandoNenhumaCasa(t *testing.T) {
	h := newHarness(t, prazoYAML)
	if d := duracaoDoEvento(t, h, nil, `{"id":"a","tipo":1}`); d != 7*24*time.Hour {
		t.Errorf("duração = %v, quero 7d", d)
	}
}

func TestPrazoZeroQuandoNenhumaCasaESemRegraDeFundo(t *testing.T) {
	yaml := `
version: 1
name: alarmes
materialization:
  key:
    field: id
  ttl:
    - when: "tipo = 2"
      expireAfterWrite: 3m
schema:
  fields:
    id:
      type: string
    tipo:
      type: int64
`
	h := newHarness(t, yaml)
	if d := duracaoDoEvento(t, h, nil, `{"id":"a","tipo":1}`); d != 0 {
		t.Errorf("duração = %v: sem regra de fundo, quem não casa não tem prazo", d)
	}
}

// `never` decide que a entidade NÃO expira, e essa decisão vence a regra de
// fundo que vem depois — é o ponto inteiro de ela existir.
func TestPrazoNeverDaZeroMesmoComRegraDeFundo(t *testing.T) {
	yaml := `
version: 1
name: alarmes
materialization:
  key:
    field: id
  ttl:
    - when: "tipo = 9"
      expireAfterWrite: never
    - expireAfterWrite: 7d
schema:
  fields:
    id:
      type: string
    tipo:
      type: int64
`
	h := newHarness(t, yaml)
	if d := duracaoDoEvento(t, h, nil, `{"id":"a","tipo":9}`); d != 0 {
		t.Errorf("duração = %v, quero 0 (imortal)", d)
	}
	if d := duracaoDoEvento(t, h, nil, `{"id":"a","tipo":1}`); d != 7*24*time.Hour {
		t.Errorf("duração = %v, quero o fundo", d)
	}
}

// A avaliação é sobre o valor PÓS-MERGE: o que o evento não menciona continua
// valendo do estado anterior, porque é isso que o merge vai produzir.
func TestPrazoVeOValorDoEstadoQuandoOEventoNaoTrazOCampo(t *testing.T) {
	h := newHarness(t, prazoYAML)
	prev := encodeFields(t, fieldKV{h.fieldID("tipo"), vi64(2)})
	if d := duracaoDoEvento(t, h, prev, `{"id":"a","sev":5}`); d != 3*time.Minute {
		t.Errorf("duração = %v, quero 3m: o tipo=2 do estado continua valendo", d)
	}
}

func TestPrazoVeOValorDoEventoQuandoEleSobrescreveOEstado(t *testing.T) {
	h := newHarness(t, prazoYAML)
	prev := encodeFields(t, fieldKV{h.fieldID("tipo"), vi64(2)})
	if d := duracaoDoEvento(t, h, prev, `{"id":"a","tipo":1}`); d != 7*24*time.Hour {
		t.Errorf("duração = %v, quero 7d: o evento tirou a condição de valer", d)
	}
}

// null explícito APAGA o campo no merge, então a condição deixa de casar.
func TestPrazoTrataNullExplicitoComoAusente(t *testing.T) {
	h := newHarness(t, prazoYAML)
	prev := encodeFields(t, fieldKV{h.fieldID("tipo"), vi64(2)})
	if d := duracaoDoEvento(t, h, prev, `{"id":"a","tipo":null}`); d != 7*24*time.Hour {
		t.Errorf("duração = %v, quero 7d: o campo foi removido", d)
	}
}

// O teste que protege o CANCELAMENTO. O merge preserva campo que o evento não
// menciona, então uma emissão condicional deixaria o prazo curto sobreviver ao
// evento que quebra a condição — e a entidade morreria logo depois de o alarme
// reabrir. Se alguém "otimizar" a emissão, este teste quebra.
func TestExpiresInEhEmitidoEmTodaEscrita(t *testing.T) {
	h := newHarness(t, prazoYAML)
	h.apply(`{"id":"a","tipo":2}`)
	st := h.state("a")
	if got := st[schema.TTLFieldName]; got.Type != types.TypeInt64 || got.I64 != (3*time.Minute).Milliseconds() {
		t.Fatalf("primeira escrita: _expiresIn = %+v, quero 180000", got)
	}
	// Segunda escrita com a MESMA duração: a entry sai de novo.
	h.apply(`{"id":"a","sev":7}`)
	st = h.state("a")
	if got := st[schema.TTLFieldName]; got.I64 != (3 * time.Minute).Milliseconds() {
		t.Fatalf("segunda escrita: _expiresIn = %+v", got)
	}
	// E o evento que quebra a condição REESCREVE o prazo.
	h.apply(`{"id":"a","tipo":1}`)
	st = h.state("a")
	if got := st[schema.TTLFieldName]; got.I64 != (7 * 24 * time.Hour).Milliseconds() {
		t.Errorf("depois do cancelamento: _expiresIn = %+v, quero 604800000", got)
	}
}

// O payload sai com FieldIDs estritamente crescentes — pré-condição do Put —
// mesmo com a entry do prazo entrando depois do sort do Parse.
func TestPrazoEntraOrdenadoNoPatch(t *testing.T) {
	h := newHarness(t, prazoYAML)
	h.apply(`{"id":"a","tipo":2,"sev":5}`)
	for i := 1; i < len(h.m.patch.entries); i++ {
		if h.m.patch.entries[i].field.ID <= h.m.patch.entries[i-1].field.ID {
			t.Fatalf("entries fora de ordem em %d: %+v", i, h.m.patch.entries)
		}
	}
}

// Qual regra está pegando o quê é a primeira pergunta do operador quando o
// dataset expira mais (ou menos) do que ele esperava.
func TestContadoresDeRegraContamPorRegra(t *testing.T) {
	h := newHarness(t, prazoYAML)
	h.apply(`{"id":"a","tipo":2}`)
	h.apply(`{"id":"b","tipo":2}`)
	h.apply(`{"id":"c","tipo":1}`)

	m := h.m.Metrics()
	if len(m.RegrasCasadas) != 2 {
		t.Fatalf("%d contadores, quero um por regra", len(m.RegrasCasadas))
	}
	if m.RegrasCasadas[0] != 2 {
		t.Errorf("regra condicional casou %d vezes, quero 2", m.RegrasCasadas[0])
	}
	if m.RegrasCasadas[1] != 1 {
		t.Errorf("regra de fundo casou %d vezes, quero 1", m.RegrasCasadas[1])
	}
	if m.SemRegra != 0 {
		t.Errorf("semRegra = %d: com regra de fundo, ninguém fica sem", m.SemRegra)
	}
}

func TestContadorSemRegraContaOQueNaoCasa(t *testing.T) {
	h := newHarness(t, `
version: 1
name: alarmes
materialization:
  key:
    field: id
  ttl:
    - when: "tipo = 2"
      expireAfterWrite: 3m
schema:
  fields:
    id:
      type: string
    tipo:
      type: int64
`)
	h.apply(`{"id":"a","tipo":1}`)
	if m := h.m.Metrics(); m.SemRegra != 1 || m.RegrasCasadas[0] != 0 {
		t.Errorf("semRegra=%d regra[0]=%d, quero 1 e 0", m.SemRegra, m.RegrasCasadas[0])
	}
}

// Captura sem preservação é coluna que nunca congelou; os dois contadores
// separados são o que torna isso visível.
func TestContadorDeCongeladaSeparaCapturaDePreservacao(t *testing.T) {
	h := newHarness(t, congeladaYAML)
	h.apply(`{"id":"a","sev":5}`) // create: captura, nada a preservar
	m := h.m.Metrics()
	if m.CongeladasCapturadas != 2 { // origSev e criadoEm
		t.Errorf("capturadas = %d, quero 2", m.CongeladasCapturadas)
	}
	if m.CongeladasPreservadas != 0 {
		t.Errorf("preservadas = %d no create, quero 0", m.CongeladasPreservadas)
	}

	h.apply(`{"id":"a","sev":9}`) // update: captura de novo e o merge segura
	m = h.m.Metrics()
	if m.CongeladasCapturadas != 4 {
		t.Errorf("capturadas = %d, quero 4", m.CongeladasCapturadas)
	}
	if m.CongeladasPreservadas != 2 {
		t.Errorf("preservadas = %d, quero 2 (origSev e criadoEm)", m.CongeladasPreservadas)
	}
	if got := h.state("a")["origSev"]; got.I64 != 5 {
		t.Errorf("origSev = %+v, quero 5", got)
	}
}
