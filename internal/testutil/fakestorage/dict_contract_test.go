package fakestorage_test

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/fielddict"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// As duas implementações de FieldDictionary do repositório precisam satisfazer
// a MESMA interface. Sem estas asserções, a divergência entre o dublê e o real
// só apareceria no binder (Q13), longe da causa.
var (
	_ schema.FieldDictionary = (*fielddict.Dict)(nil)
	_ schema.FieldDictionary = (*fakestorage.Dict)(nil)
)

// dictUnderTest é a superfície comum exercitada nos dois.
type dictUnderTest interface {
	LookupOrCreate(name []byte) (types.FieldID, error)
	TypeHint(id types.FieldID) (types.FieldType, bool)
	SetTypeHint(id types.FieldID, t types.FieldType) error
	Len() int
	Name(id types.FieldID) (string, bool)
}

// O schema.Compile ENUMERA o dicionário (1..Len) para carregar os campos que
// uma execução anterior materializou. Se o dublê e o real divergirem nessa
// numeração, o SELECT * passa com o fake e perde colunas com o real.
func TestEnumeracaoDoDicionarioConcorda(t *testing.T) {
	realDict, err := fielddict.Open(filepath.Join(t.TempDir(), "fields.dict"))
	if err != nil {
		t.Fatal(err)
	}
	defer realDict.Close()

	cases := map[string]dictUnderTest{"real": realDict, "fake": fakestorage.NewDict()}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			if d.Len() != 0 {
				t.Errorf("Len inicial = %d, want 0", d.Len())
			}
			names := []string{"id", "Acct-Status-Type", "X-Vendor-Attr"}
			for i, n := range names {
				id, err := d.LookupOrCreate([]byte(n))
				if err != nil {
					t.Fatal(err)
				}
				if want := types.FieldID(i + 1); id != want {
					t.Errorf("LookupOrCreate(%q) = %d, want %d (ordem de criação)", n, id, want)
				}
			}
			if d.Len() != len(names) {
				t.Errorf("Len = %d, want %d", d.Len(), len(names))
			}
			for i, want := range names {
				got, ok := d.Name(types.FieldID(i + 1))
				if !ok || got != want {
					t.Errorf("Name(%d) = %q/%v, want %q/true", i+1, got, ok, want)
				}
			}
			// Fora da faixa: os dois recusam, e nenhum entra em panic — o
			// Compile varre a faixa inteira sem checar antes.
			for _, id := range []types.FieldID{0, types.FieldID(len(names) + 1), 9999} {
				if got, ok := d.Name(id); ok {
					t.Errorf("Name(%d) = %q/true, want false", id, got)
				}
			}
		})
	}
}

// O dublê e o real precisam concordar em TUDO o que o consumidor observa —
// inclusive no sentinela do conflito. Um fake mais permissivo faria o teste do
// binder passar com ele e falhar com o real, que é a divergência que um dublê
// existe para expor, não para criar.
func TestDicionarioRealEFakeConcordam(t *testing.T) {
	realDict, err := fielddict.Open(filepath.Join(t.TempDir(), "fields.dict"))
	if err != nil {
		t.Fatal(err)
	}
	defer realDict.Close()

	cases := map[string]dictUnderTest{
		"real": realDict,
		"fake": fakestorage.NewDict(),
	}
	for name, d := range cases {
		t.Run(name, func(t *testing.T) {
			id, err := d.LookupOrCreate([]byte("Connect-Info"))
			if err != nil {
				t.Fatal(err)
			}

			// Campo novo não tem tipo fixado.
			if got, ok := d.TypeHint(id); ok {
				t.Errorf("campo novo já tem hint: %v", got)
			}
			// Campo inexistente também não — e não pode explodir.
			if _, ok := d.TypeHint(types.FieldID(9999)); ok {
				t.Error("campo inexistente devolveu hint")
			}

			if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
				t.Fatal(err)
			}
			if got, ok := d.TypeHint(id); !ok || got != types.TypeInt64 {
				t.Errorf("TypeHint = %v/%v, want int64/true", got, ok)
			}

			// Idempotente.
			if err := d.SetTypeHint(id, types.TypeInt64); err != nil {
				t.Errorf("repetir o mesmo tipo devolveu erro: %v", err)
			}

			// Conflito: MESMO sentinela nos dois.
			err = d.SetTypeHint(id, types.TypeString)
			if !errors.Is(err, fielddict.ErrTypeHintConflict) {
				t.Errorf("conflito = %v, want fielddict.ErrTypeHintConflict", err)
			}
			if !strings.Contains(err.Error(), "Connect-Info") {
				t.Errorf("mensagem de conflito não nomeia o campo: %q", err)
			}
			// E o hint anterior sobrevive.
			if got, _ := d.TypeHint(id); got != types.TypeInt64 {
				t.Errorf("hint após conflito = %v, want int64", got)
			}

			// Tipo nulo é recusado nos dois.
			if err := d.SetTypeHint(id, types.TypeNull); err == nil {
				t.Error("SetTypeHint(TypeNull) deveria falhar")
			}
			// Campo inexistente é recusado nos dois.
			if err := d.SetTypeHint(types.FieldID(9999), types.TypeInt64); err == nil {
				t.Error("SetTypeHint em campo inexistente deveria falhar")
			}
		})
	}
}
