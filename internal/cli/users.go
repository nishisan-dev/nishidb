package cli

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/user"
	"strconv"
	"strings"

	nishiconfig "github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/users"
)

// `nishidb users` é a gestão da tabela de usuários do servidor (issue #40):
// criar, trocar senha, mudar papel e remover.
//
// Dois caminhos, a MESMA operação: `--server` fala com as rotas /v1/users/*
// do servidor no ar, com a credencial de sempre (--user + NISHIDB_PASSWORD ou
// --password-file); `--config` abre a tabela local e exige o servidor PARADO
// (toma o flock de <storage.path>/_system, exit 3 com o servidor de pé). O
// caminho local é a recuperação de uma senha de ADMIN perdida — quem tem o
// disco já é root — e o provisionamento determinístico antes da primeira
// subida. Sem tabela, ele a cria com a credencial de bootstrap, como a subida
// do servidor faria.
//
// A senha NOVA nunca é flag, pela mesma razão da senha da credencial: flag
// aparece em `ps`. Ela vem de --new-password-file, com `-` lendo do stdin.

// userAction é o que cada subcomando precisa, e o que ele exige das flags.
type userAction struct {
	name          string
	needsRole     bool
	needsPassword bool
}

var userActions = map[string]userAction{
	"create":   {name: "create", needsRole: true, needsPassword: true},
	"password": {name: "password", needsPassword: true},
	"role":     {name: "role", needsRole: true},
	"delete":   {name: "delete"},
}

func runUsers(args []string, amb Context) int {
	if len(args) == 0 {
		notify(amb, "users: missing action (create | password | role | delete)\n")
		return ExitUsageError
	}
	action, ok := userActions[args[0]]
	if !ok {
		notify(amb, "users: unknown action %q (create | password | role | delete)\n", args[0])
		return ExitUsageError
	}
	cmd := "users " + action.name

	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(amb.Stderr)
	var (
		argConfig    = fs.String("config", "", "path to nishidb.yaml (local path: requires the server to be stopped)")
		server       = fs.String("server", "", "nishidb server address (host:port)")
		name         = fs.String("name", "", "the user to act on")
		role         = fs.String("role", "", "READ or ADMIN")
		passwordFile = fs.String("new-password-file", "", "file with the new password (\"-\" reads stdin; trailing newline trimmed)")
	)
	credFlags := setupRemoteFlags(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return ExitUsageError
	}
	switch {
	case fs.NArg() != 0:
		notify(amb, "%s: unexpected positional argument %q\n", cmd, fs.Arg(0))
		return ExitUsageError
	case (*argConfig == "") == (*server == ""):
		notify(amb, "%s: give exactly one of --config (server stopped) or --server (server running)\n", cmd)
		return ExitUsageError
	case *name == "":
		notify(amb, "%s: --name is required\n", cmd)
		return ExitUsageError
	case action.needsRole && *role == "":
		notify(amb, "%s: --role is required (READ | ADMIN)\n", cmd)
		return ExitUsageError
	case !action.needsRole && *role != "":
		notify(amb, "%s: --role does not apply to this action\n", cmd)
		return ExitUsageError
	case action.needsPassword && *passwordFile == "":
		notify(amb, "%s: --new-password-file is required (the password is never a flag)\n", cmd)
		return ExitUsageError
	case !action.needsPassword && *passwordFile != "":
		notify(amb, "%s: --new-password-file does not apply to this action\n", cmd)
		return ExitUsageError
	}

	var password []byte
	if action.needsPassword {
		var err error
		if password, err = readNewPassword(*passwordFile, amb); err != nil {
			notify(amb, "%s: %v\n", cmd, err)
			return ExitUsageError
		}
	}

	if *server != "" {
		cred, err := credFlags.resolve()
		if err != nil {
			notify(amb, "%s: %v\n", cmd, err)
			return ExitUsageError
		}
		return usersRemote(cmd, action, *server, *name, *role, password, cred, amb)
	}
	return usersLocal(cmd, action, *argConfig, *name, *role, password, amb)
}

// readNewPassword lê a senha nova do arquivo ou do stdin, aparando a quebra
// de linha final. Vazio é recusado aqui; a política de tamanho é da tabela.
func readNewPassword(path string, amb Context) ([]byte, error) {
	var raw []byte
	var err error
	if path == "-" {
		raw, err = io.ReadAll(amb.Stdin)
	} else {
		raw, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("--new-password-file: %w", err)
	}
	password := bytes.TrimRight(raw, "\r\n")
	if len(password) == 0 {
		return nil, fmt.Errorf("--new-password-file %s is empty (or only a line break) after trimming", path)
	}
	return password, nil
}

func usersLocal(cmd string, action userAction, argConfig, name, role string, password []byte, amb Context) int {
	cfg, err := nishiconfig.Load(argConfig)
	if err != nil {
		notify(amb, "%s: %v\n", cmd, err)
		return ExitUsageError
	}
	// A auditoria da gestão offline vai para o stderr, no mesmo formato do
	// log do servidor: quem mexe na tabela com o servidor parado também deixa
	// rastro, e o aviso de bootstrap (com o caminho do arquivo de senha)
	// chega a quem rodou o comando.
	log := logging.New(amb.Stderr, logging.FormatText, slog.LevelInfo).Component("users")
	table, err := users.Open(users.Dir(cfg.Storage.Path), users.Options{Logger: log})
	if err != nil {
		notify(amb, "%s: %v\n", cmd, err)
		return errorCode(err)
	}
	defer func() { _ = table.Close() }()

	actor := users.Actor{Name: localActor()}
	switch action.name {
	case "create":
		err = table.Create(actor, name, role, password)
	case "password":
		err = table.SetPassword(actor, name, password)
	case "role":
		err = table.SetRole(actor, name, role)
	case "delete":
		err = table.Delete(actor, name)
	}
	if err != nil {
		notify(amb, "%s: %v\n", cmd, err)
		return errorCode(err)
	}
	return writeUserOutcome(cmd, action, name, roleAfter(table, name, action), amb)
}

// roleAfter devolve o papel do usuário depois da operação local ("" se ele
// foi removido).
func roleAfter(table *users.Store, name string, action userAction) string {
	if action.name == "delete" {
		return ""
	}
	for _, u := range table.Users() {
		if u.Username == name {
			return u.Role
		}
	}
	return ""
}

// localActor é o autor registrado na gestão offline: o usuário do sistema
// operacional que rodou o comando, prefixado para nunca se confundir com uma
// conta da tabela.
func localActor() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return "local:" + u.Username
	}
	return "local:uid-" + strconv.Itoa(os.Getuid())
}

func usersRemote(cmd string, action userAction, server, name, role string, password []byte,
	cred remoteCredential, amb Context) int {
	var body any
	switch action.name {
	case "create":
		body = protocol.UserCreateRequest{Username: name, Role: role, Password: string(password)}
	case "password":
		body = protocol.UserPasswordRequest{Username: name, Password: string(password)}
	case "role":
		body = protocol.UserRoleRequest{Username: name, Role: role}
	case "delete":
		body = protocol.UserDeleteRequest{Username: name}
	}
	var resp protocol.UserResponse
	if code := postJSON(cmd, server, "/v1/users/"+action.name, body, cred, &resp, amb); code != ExitOK {
		return code
	}
	return writeUserOutcome(cmd, action, resp.Username, resp.Role, amb)
}

func writeUserOutcome(cmd string, action userAction, name, role string, amb Context) int {
	return writeOutputWithContext(cmd, amb, func(w io.Writer) {
		switch action.name {
		case "create":
			title(w, "user %s created with role %s", name, role)
		case "password":
			title(w, "password of user %s changed", name)
		case "role":
			title(w, "user %s now has role %s", name, role)
		case "delete":
			title(w, "user %s deleted", strings.TrimSpace(name))
		}
	})
}
