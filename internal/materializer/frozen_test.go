package materializer

import (
	"errors"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

const congeladaYAML = `
version: 1
name: alarmes
materialization:
  key:
    field: id
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

// congeladaYAMLWithTTL acrescenta a expiração, que é o que faz o writer
// injetar o _writtenAt — sem ela não há com que comparar o relógio da
// congelada.
const congeladaYAMLWithTTL = `
version: 1
name: alarmes
materialization:
  key:
    field: id
  ttl:
    - expireAfterWrite: 1h
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

// entryOf devolve a entry do campo, e ok=false quando o patch não a tem — a
// AUSÊNCIA é o que metade destes testes afirma, e entryByName aborta nela.
func entryOf(b *PatchBuffer, name string) (patchEntry, bool) {
	for _, e := range b.entries {
		if e.field.Name == name {
			return e, true
		}
	}
	return patchEntry{}, false
}

func TestCongeladaCapturaDoEvento(t *testing.T) {
	c := compileYAML(t, congeladaYAML)
	b := parsePatch(t, c, `{"id":"a","sev":5}`)
	e, ok := entryOf(b, "origSev")
	if !ok {
		t.Fatal("a congelada não foi capturada")
	}
	if !e.congelada {
		t.Error("a entry da congelada precisa carregar a política de escrita")
	}
	if e.val.Type != types.TypeInt64 || e.val.I64 != 5 {
		t.Errorf("valor capturado = %+v, quero int64 5", e.val)
	}
}

// Origem ausente não emite nada: a coluna fica vazia e é preenchida no
// primeiro evento que trouxer a origem. É a semântica de primeiro valor VISTO.
func TestCongeladaNaoEmiteQuandoAOrigemNaoVeioNoEvento(t *testing.T) {
	c := compileYAML(t, congeladaYAML)
	b := parsePatch(t, c, `{"id":"a"}`)
	if _, ok := entryOf(b, "origSev"); ok {
		t.Error("a congelada foi emitida sem a origem ter vindo no evento")
	}
}

func TestCongeladaNaoEmiteQuandoAOrigemVeioNull(t *testing.T) {
	c := compileYAML(t, congeladaYAML)
	b := parsePatch(t, c, `{"id":"a","sev":null}`)
	if _, ok := entryOf(b, "origSev"); ok {
		t.Error("null explícito na origem não pode congelar nada")
	}
}

func TestEventoQueTrazONomeDaCongeladaEhRecusado(t *testing.T) {
	c := compileYAML(t, congeladaYAML)
	var b PatchBuffer
	err := b.Parse([]byte(`{"id":"a","sev":5,"origSev":9}`), c, time.Now())
	if !errors.Is(err, schema.ErrUnknownField) {
		t.Fatalf("erro = %v, quero ErrUnknownField", err)
	}
}

// O relógio é o MESMO do _writtenAt porque os dois saem do `agora` que o
// Parse recebe: um `createdAt` que divergisse do instante de escrita do
// próprio evento que o criou seria um fato inventado.
func TestCongeladaDoRelogioUsaOMesmoInstanteQueOWrittenAt(t *testing.T) {
	c := compileYAML(t, congeladaYAMLWithTTL)
	agora := time.UnixMilli(1_700_000_000_000)
	var b PatchBuffer
	if err := b.Parse([]byte(`{"id":"a","sev":5}`), c, agora); err != nil {
		t.Fatal(err)
	}
	criado, ok := entryOf(&b, "criadoEm")
	if !ok {
		t.Fatal("a congelada do relógio não foi emitida")
	}
	write, ok := entryOf(&b, schema.WriteInstantFieldName)
	if !ok {
		t.Fatal("o campo de sistema não foi emitido")
	}
	if criado.val.I64 != write.val.I64 {
		t.Errorf("criadoEm = %d, _writtenAt = %d: os dois têm de sair do mesmo instante",
			criado.val.I64, write.val.I64)
	}
	if criado.val.Type != types.TypeTimestamp {
		t.Errorf("tipo da congelada do relógio = %v, quero timestamp", criado.val.Type)
	}
}

// A congelada do relógio é emitida em TODO evento; é o merge que preserva a
// primeira. Sem estado anterior — que é o caso do Parse isolado — ela sai
// sempre, e é o que o create precisa.
func TestCongeladaDoRelogioEhEmitidaMesmoSemOrigemNoEvento(t *testing.T) {
	c := compileYAML(t, congeladaYAML)
	b := parsePatch(t, c, `{"id":"a"}`)
	if _, ok := entryOf(b, "criadoEm"); !ok {
		t.Error("a congelada do relógio não depende de campo nenhum do evento")
	}
}

// A congelada tem FieldID próprio, então o tipo dela é fixado sob o id DELA na
// primeira captura. Sem isso a coluna ficaria com dado gravado e sem tipo, e
// nenhum WHERE sobre ela devolveria linha.
func TestCongeladaDeOrigemAutoFixaOProprioTipo(t *testing.T) {
	c := compileYAML(t, `
version: 1
name: alarmes
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    sev:
      type: auto
    origSev:
      frozenFrom: sev
`)
	b := parsePatch(t, c, `{"id":"a","sev":5}`)
	f, _ := c.FieldByName([]byte("origSev"))
	if f.EffectiveType() != types.TypeInt64 {
		t.Errorf("tipo efetivo da congelada = %v, quero int64", f.EffectiveType())
	}
	e, ok := entryOf(b, "origSev")
	if !ok || e.val.I64 != 5 {
		t.Errorf("entry = %+v, ok = %v", e, ok)
	}
}
