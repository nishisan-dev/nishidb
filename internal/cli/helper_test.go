package cli_test

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// openDirect abre o dataset por baixo do CLI, para simular o "outro processo"
// que segura o lock.
func openDirect(t *testing.T, a *env) (*storage.Store, *schema.Compiled) {
	t.Helper()
	spec, err := schema.Load(filepath.Join(a.dir, "datasets", "radius_sessions.yaml"), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	// O conjunto sai do schema, e não de um laço local: abrir o dataset com um
	// conjunto de índices diferente do que o CLI usa faria este helper
	// reproduzir, dentro da suíte, o modo de falha cruzado que
	// schema.IndexedFieldNames existe para impedir — e que o par
	// CamposComBitmap/CamposComHash existia para impedir antes dela.
	st, err := storage.Open(storage.Options{
		Dir:                 filepath.Join(a.dir, "dados", "radius_sessions"),
		IndexedFieldsByName: schema.IndexedFieldNames(spec),
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(st.FieldDict())
	if err != nil {
		st.Close()
		t.Fatal(err)
	}
	return st, c
}
