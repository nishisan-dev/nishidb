package schema

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

const autoYAML = `
version: 1
name: auto_ds
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    declarado:
      type: int64
    livre:
      type: auto
`

func compileAuto(t *testing.T, dict FieldDictionary) *Compiled {
	t.Helper()
	c, err := parseOK(t, autoYAML).Compile(dict)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return c
}

func fieldNamed(t *testing.T, c *Compiled, name string) *CompiledField {
	t.Helper()
	f, ok := c.FieldByName([]byte(name))
	if !ok {
		t.Fatalf("campo %q não compilado", name)
	}
	return f
}

// EffectiveType é o que o binder consulta para escolher a coerção do
// literal. Os três estados precisam ser distinguíveis: declarado, auto
// fixado e auto nunca observado.
func TestEffectiveTypeDeclaradoEAuto(t *testing.T) {
	dict := newFakeDict()
	c := compileAuto(t, dict)

	if got := fieldNamed(t, c, "declarado").EffectiveType(); got != types.TypeInt64 {
		t.Errorf("campo declarado: EffectiveType = %v, want int64", got)
	}
	// Campo auto sem observação: TypeNull é "não há tipo contra o que
	// comparar", e o binder recusa em vez de inventar um.
	free := fieldNamed(t, c, "livre")
	if got := free.EffectiveType(); got != types.TypeNull {
		t.Errorf("campo auto virgem: EffectiveType = %v, want null", got)
	}

	if err := c.FixAutoType(free, types.TypeTimestamp); err != nil {
		t.Fatal(err)
	}
	if got := free.EffectiveType(); got != types.TypeTimestamp {
		t.Errorf("após fixar: EffectiveType = %v, want timestamp", got)
	}
	// A fixação foi ao DICIONÁRIO, não só à memória — é o que a faz durar.
	if got, ok := dict.TypeHint(free.ID); !ok || got != types.TypeTimestamp {
		t.Errorf("dicionário = %v/%v, want timestamp/true", got, ok)
	}
	if dict.setCalls != 1 {
		t.Errorf("setCalls = %d, want 1", dict.setCalls)
	}

	// Idempotente: fixar o mesmo tipo de novo não escreve.
	if err := c.FixAutoType(free, types.TypeTimestamp); err != nil {
		t.Fatal(err)
	}
	if dict.setCalls != 1 {
		t.Errorf("setCalls = %d após repetição, want 1", dict.setCalls)
	}
}

// O caminho de RESTART: o dicionário já tem o tipo, e o Compile precisa
// carregá-lo. Sem isto o tipo continua no disco mas o processo age como se o
// campo nunca tivesse sido observado, e o binder recusa comparação num campo
// que tem dados.
func TestCompileCarregaTypeHint(t *testing.T) {
	dict := newFakeDict()
	c1 := compileAuto(t, dict)
	free := fieldNamed(t, c1, "livre")
	if err := c1.FixAutoType(free, types.TypeInt64); err != nil {
		t.Fatal(err)
	}

	c2 := compileAuto(t, dict) // "reabertura"
	if got := fieldNamed(t, c2, "livre").EffectiveType(); got != types.TypeInt64 {
		t.Errorf("após recompilar: EffectiveType = %v, want int64 (o tipo precisa sobreviver)", got)
	}
	// Campo declarado não é afetado pelo hint.
	if got := fieldNamed(t, c2, "declarado").EffectiveType(); got != types.TypeInt64 {
		t.Errorf("campo declarado = %v", got)
	}
}

func TestResolveUnknownCarregaTypeHint(t *testing.T) {
	dict := newFakeDict()
	c1 := compileAuto(t, dict)
	created, err := c1.ResolveUnknown([]byte("X-Vendor"))
	if err != nil || created == nil {
		t.Fatalf("ResolveUnknown: %v (campo %v)", err, created)
	}
	if err := c1.FixAutoType(created, types.TypeString); err != nil {
		t.Fatal(err)
	}

	// Novo processo: o campo volta pelo ResolveUnknown, e o tipo tem de vir
	// junto — senão o primeiro evento após restart o fixaria de novo, com o
	// tipo do shape daquele evento.
	c2 := compileAuto(t, dict)
	again, err := c2.ResolveUnknown([]byte("X-Vendor"))
	if err != nil || again == nil {
		t.Fatalf("ResolveUnknown após recompilar: %v", err)
	}
	if got := again.EffectiveType(); got != types.TypeString {
		t.Errorf("EffectiveType = %v, want string", got)
	}
}

func TestFixAutoTypeRejeitaCampoDeclarado(t *testing.T) {
	c := compileAuto(t, newFakeDict())

	err := c.FixAutoType(fieldNamed(t, c, "declarado"), types.TypeString)
	if err == nil {
		t.Fatal("FixAutoType sobre campo declarado deveria falhar")
	}
	if !strings.Contains(err.Error(), "declarado") {
		t.Errorf("erro = %q, want nomear o campo", err)
	}

	if err := c.FixAutoType(fieldNamed(t, c, "livre"), types.TypeNull); err == nil {
		t.Error("fixar TypeNull deveria falhar")
	}
	if err := c.FixAutoType(nil, types.TypeInt64); err == nil {
		t.Error("FixAutoType(nil) deveria falhar")
	}
}

// O conflito vem do DICIONÁRIO e precisa atravessar o FixAutoType intacto: é
// por esse erro que o materializer classifica o evento como TYPE_CONVERSION.
func TestFixAutoTypeConflitoPropagaErroDoDicionario(t *testing.T) {
	dict := newFakeDict()
	c := compileAuto(t, dict)
	free := fieldNamed(t, c, "livre")
	if err := c.FixAutoType(free, types.TypeInt64); err != nil {
		t.Fatal(err)
	}

	err := c.FixAutoType(free, types.TypeString)
	if err == nil {
		t.Fatal("conflito deveria falhar")
	}
	// O tipo em memória NÃO muda: a tentativa falha, não corrompe.
	if got := free.EffectiveType(); got != types.TypeInt64 {
		t.Errorf("EffectiveType após conflito = %v, want int64", got)
	}
}

// O writer fixa enquanto as queries leem: sem o atômico isto é data race.
func TestFixAutoTypeConcorrenteComLeitura(t *testing.T) {
	c := compileAuto(t, newFakeDict())
	free := fieldNamed(t, c, "livre")

	var wg sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				// Toda leitura devolve TypeNull OU o tipo final, nunca lixo.
				if got := free.EffectiveType(); got != types.TypeNull && got != types.TypeDouble {
					t.Errorf("EffectiveType = %v, want null ou double", got)
					return
				}
			}
		}()
	}
	if err := c.FixAutoType(free, types.TypeDouble); err != nil {
		t.Fatal(err)
	}
	close(done)
	wg.Wait()
	if got := free.EffectiveType(); got != types.TypeDouble {
		t.Errorf("EffectiveType final = %v, want double", got)
	}
}

// A interface exige os três métodos: um dicionário sem eles fixaria o tipo só
// em memória e o perderia no restart, em silêncio.
func TestFakeDictSatisfazAInterface(t *testing.T) {
	var _ FieldDictionary = newFakeDict()
	// E o erro de tipo nulo continua sendo erro no dublê.
	if err := newFakeDict().SetTypeHint(1, types.TypeNull); err == nil {
		t.Error("SetTypeHint(TypeNull) deveria falhar")
	} else if !errors.Is(err, err) { // sanity: erro não-nil
		t.Fatal("impossível")
	}
}
