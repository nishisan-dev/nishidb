package server

import (
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/users"
)

// testAccount é uma conta a criar na tabela de teste.
type testAccount struct {
	name     string
	password string
	role     string
}

// userTableOfTest abre uma tabela de usuários num diretório temporário e cria
// as contas pedidas. A tabela sempre traz também o ADMIN de bootstrap, como
// qualquer tabela de verdade.
func userTableOfTest(t testing.TB, accounts ...testAccount) *users.Store {
	t.Helper()
	st, err := users.Open(filepath.Join(t.TempDir(), users.DirName), users.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	actor := users.Actor{Name: "test"}
	for _, a := range accounts {
		if err := st.Create(actor, a.name, a.role, []byte(a.password)); err != nil {
			t.Fatalf("criar %q: %v", a.name, err)
		}
	}
	return st
}
