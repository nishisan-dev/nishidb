package cli_test

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// abreDireto abre o dataset por baixo do CLI, para simular o "outro processo"
// que segura o lock.
func abreDireto(t *testing.T, a *ambiente) (*storage.Store, *schema.Compiled) {
	t.Helper()
	spec, err := schema.Load(filepath.Join(a.dir, "datasets", "radius_sessions.yaml"), schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	var bitmaps []string
	for campo, f := range spec.Schema.Fields {
		if f.Index == "bitmap" {
			bitmaps = append(bitmaps, campo)
		}
	}
	st, err := storage.Open(storage.Options{
		Dir:              filepath.Join(a.dir, "dados", "radius_sessions"),
		BitmapFieldNames: bitmaps,
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
