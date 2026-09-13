package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/users"
)

func TestOUsoAnunciaUsers(t *testing.T) {
	a := newEnv(t)
	if code := a.run("--help"); code != 0 {
		t.Fatalf("--help saiu com %d", code)
	}
	for _, trecho := range []string{"nishidb users create", "--new-password-file", "_meta.users", "bootstrap-admin.password"} {
		if !strings.Contains(a.stdout.String(), trecho) {
			t.Errorf("o texto de uso não anuncia %q", trecho)
		}
	}
}

func TestUsersValidaAsFlags(t *testing.T) {
	a := newEnv(t)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"sem acao", []string{"users"}, "missing action"},
		{"acao desconhecida", []string{"users", "list", "--config", a.config}, "unknown action"},
		{"sem config nem server", []string{"users", "delete", "--name", "ana"}, "exactly one"},
		{"config e server", []string{"users", "delete", "--config", a.config, "--server", "127.0.0.1:1", "--name", "ana"}, "exactly one"},
		{"sem nome", []string{"users", "delete", "--config", a.config}, "--name"},
		{"create sem papel", []string{"users", "create", "--config", a.config, "--name", "ana", "--new-password-file", "-"}, "--role"},
		{"delete com papel", []string{"users", "delete", "--config", a.config, "--name", "ana", "--role", "READ"}, "--role does not apply"},
		{"create sem senha", []string{"users", "create", "--config", a.config, "--name", "ana", "--role", "READ"}, "never a flag"},
		{"role com senha", []string{"users", "role", "--config", a.config, "--name", "ana", "--role", "READ", "--new-password-file", "-"}, "does not apply"},
		{"senha vazia", []string{"users", "password", "--config", a.config, "--name", "ana", "--new-password-file", "-"}, "empty"},
		{"posicional", []string{"users", "delete", "--config", a.config, "--name", "ana", "extra"}, "positional"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if code := a.run(c.args...); code != cli.ExitUsageError {
				t.Fatalf("exit = %d, quer %d\n%s", code, cli.ExitUsageError, a.stderr.String())
			}
			if !strings.Contains(a.stderr.String(), c.want) {
				t.Fatalf("stderr não explica (%q):\n%s", c.want, a.stderr.String())
			}
		})
	}
}

// authenticates abre a tabela do ambiente e confere a credencial. Só com o
// comando já encerrado: a tabela tem flock.
func authenticates(t *testing.T, a *env, name, password string) (string, bool) {
	t.Helper()
	table, err := users.Open(users.Dir(filepath.Join(a.dir, "dados")), users.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = table.Close() }()
	role, ok, _ := table.Authenticate(name, []byte(password))
	return role, ok
}

// O caminho --config inteiro, com o servidor parado: a primeira chamada cria a
// tabela com a credencial de bootstrap, e as seguintes a gerem.
func TestUsersLocalGereATabelaComOServidorParado(t *testing.T) {
	a := newEnv(t)
	const anaPassword = "senha-da-ana-longa"

	if code := a.runWithEntry(anaPassword+"\n",
		"users", "create", "--config", a.config, "--name", "ana", "--role", "READ", "--new-password-file", "-"); code != cli.ExitOK {
		t.Fatalf("create = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "user ana created with role READ") {
		t.Errorf("saída do create:\n%s", a.stdout.String())
	}
	bootstrapFile := filepath.Join(a.dir, "dados", users.DirName, users.BootstrapPasswordFileName)
	if !strings.Contains(a.stderr.String(), "users.bootstrap") || !strings.Contains(a.stderr.String(), bootstrapFile) {
		t.Errorf("a primeira gestão local tem de avisar o bootstrap e o caminho da senha:\n%s", a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "users.created") || !strings.Contains(a.stderr.String(), "local:") {
		t.Errorf("a gestão local tem de auditar o autor:\n%s", a.stderr.String())
	}
	if strings.Contains(a.stdout.String()+a.stderr.String(), anaPassword) {
		t.Fatalf("a senha vazou na saída")
	}
	if role, ok := authenticates(t, a, "ana", anaPassword); !ok || role != "READ" {
		t.Fatalf("ana não autentica: %q %v", role, ok)
	}

	// Senha por arquivo, papel, remoção.
	passwordFile := filepath.Join(a.dir, "senha-nova.txt")
	if err := os.WriteFile(passwordFile, []byte("senha-nova-da-ana\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := a.run("users", "password", "--config", a.config, "--name", "ana", "--new-password-file", passwordFile); code != cli.ExitOK {
		t.Fatalf("password = %d\n%s", code, a.stderr.String())
	}
	if _, ok := authenticates(t, a, "ana", "senha-nova-da-ana"); !ok {
		t.Fatalf("a senha nova (com CRLF aparado) não autentica")
	}
	if code := a.run("users", "role", "--config", a.config, "--name", "ana", "--role", "ADMIN"); code != cli.ExitOK {
		t.Fatalf("role = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "user ana now has role ADMIN") {
		t.Errorf("saída do role:\n%s", a.stdout.String())
	}
	if code := a.run("users", "delete", "--config", a.config, "--name", "admin"); code != cli.ExitOK {
		t.Fatalf("delete do admin de bootstrap = %d\n%s", code, a.stderr.String())
	}
	if _, err := os.Stat(bootstrapFile); !os.IsNotExist(err) {
		t.Errorf("remover o admin de bootstrap tem de apagar o arquivo de senha")
	}

	// Última ADMIN: a recusa nomeia a regra e sai com erro de uso.
	if code := a.run("users", "delete", "--config", a.config, "--name", "ana"); code != cli.ExitUsageError {
		t.Fatalf("remover a última ADMIN = %d, quer %d\n%s", code, cli.ExitUsageError, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "last ADMIN") {
		t.Errorf("a recusa não nomeia o último ADMIN:\n%s", a.stderr.String())
	}
}

// Com o servidor autenticado no ar, a tabela está travada: exit 3, como o
// dataset travado.
func TestUsersLocalComOServidorNoArSaiComExit3(t *testing.T) {
	a, _ := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))

	code := a.runWithEntry("senha-nova-bem-longa\n",
		"users", "password", "--config", a.config, "--name", "leitor", "--new-password-file", "-")
	if code != cli.ExitDataset {
		t.Fatalf("exit = %d, quer %d\n%s", code, cli.ExitDataset, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "already open") {
		t.Errorf("a mensagem não explica o flock:\n%s", a.stderr.String())
	}
}

// O caminho --server, pelas rotas com TLS e credencial: ADMIN cria, a conta
// nova autentica, e as recusas do servidor chegam ao operador.
func TestUsersRemotoGereATabelaComOServidorNoAr(t *testing.T) {
	a, certPath := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))
	server := "https://" + a.endereco()
	const anaPassword = "senha-da-ana-remota"

	t.Setenv("NISHIDB_PASSWORD", passwordChefeCLI)
	if code := a.runWithEntry(anaPassword+"\n", "users", "create", "--server", server, "--tls-ca", certPath,
		"--user", "chefe", "--name", "ana", "--role", "READ", "--new-password-file", "-"); code != cli.ExitOK {
		t.Fatalf("create remoto = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "user ana created with role READ") {
		t.Errorf("saída do create remoto:\n%s", a.stdout.String())
	}
	if strings.Contains(a.stdout.String()+a.stderr.String(), anaPassword) {
		t.Fatalf("a senha vazou na saída")
	}

	// A conta nova fala com o servidor.
	t.Setenv("NISHIDB_PASSWORD", anaPassword)
	if code := a.run("status", "--server", server, "--tls-ca", certPath, "--user", "ana"); code != cli.ExitOK {
		t.Fatalf("status com a conta nova = %d\n%s", code, a.stderr.String())
	}

	// READ não remove ninguém: a recusa de papel do servidor sai no stderr.
	t.Setenv("NISHIDB_PASSWORD", passwordReaderCLI)
	if code := a.run("users", "delete", "--server", server, "--tls-ca", certPath, "--user", "leitor", "--name", "ana"); code != cli.ExitUsageError {
		t.Fatalf("delete por READ = %d, quer %d\n%s", code, cli.ExitUsageError, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "requer ADMIN") {
		t.Errorf("a recusa não nomeia o papel exigido:\n%s", a.stderr.String())
	}

	t.Setenv("NISHIDB_PASSWORD", passwordChefeCLI)
	if code := a.runWithEntry("outra-senha-longa\n", "users", "create", "--server", server, "--tls-ca", certPath,
		"--user", "chefe", "--name", "ana", "--role", "READ", "--new-password-file", "-"); code != cli.ExitUsageError {
		t.Fatalf("create repetido = %d, quer %d\n%s", code, cli.ExitUsageError, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "already exists") {
		t.Errorf("a recusa não explica o nome repetido:\n%s", a.stderr.String())
	}
}
