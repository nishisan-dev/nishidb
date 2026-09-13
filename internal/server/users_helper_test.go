package server_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/users"
)

// account é uma conta a criar na tabela de usuários antes de o servidor subir.
type account struct {
	name     string
	password string
	role     string
}

// authAccounts são as duas contas das suítes autenticadas: "leitor" (READ) e
// "chefe" (ADMIN), com as senhas de auth_test.go.
func authAccounts() []account {
	return []account{
		{name: "leitor", password: passwordReaderAuth, role: "READ"},
		{name: "chefe", password: passwordChefeAuth, role: "ADMIN"},
	}
}

// storagePathOf é o storage.path que newEnvWithStorage escreve no YAML.
func storagePathOf(a *env) string {
	return filepath.Join(a.dir, "dados")
}

// provisionUsers cria as contas na tabela de usuários do ambiente, com o
// servidor ainda parado — o mesmo caminho da gestão offline do CLI. A tabela
// nasce com o ADMIN de bootstrap, como numa primeira subida de verdade.
func provisionUsers(t *testing.T, a *env, accounts ...account) {
	t.Helper()
	st, err := users.Open(users.Dir(storagePathOf(a)), users.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, acc := range accounts {
		if err := st.Create(users.Actor{Name: "test"}, acc.name, acc.role, []byte(acc.password)); err != nil {
			t.Fatalf("criar %q: %v", acc.name, err)
		}
	}
}

// serverWithDataAndUsers é serverWithData com contas criadas antes da subida.
func serverWithDataAndUsers(t *testing.T, n int, blockServer string, accounts ...account) *server.Server {
	t.Helper()
	return serverWithStorageAndUsers(t, n, blockServer, "", accounts...)
}

// serverWithStorageAndUsers aceita também linhas extras do bloco `storage:`,
// como serverWithDataDiretorioEStorage.
func serverWithStorageAndUsers(t *testing.T, n int, blockServer, storageExtra string, accounts ...account) *server.Server {
	t.Helper()
	a := newEnvWithStorage(t, blockServer, storageExtra)
	a.writeDataset("d", strings.ReplaceAll(specConsultavel, "%s", "d"))
	seed(t, a, "d", n)
	provisionUsers(t, a, accounts...)

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}
