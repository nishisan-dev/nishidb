package server_test

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

const metaUsersQuery = `{"dataset":"d","sql":"SELECT username, role, bootstrap, createdBy FROM _meta.users"}`

// ADMIN lê a tabela de usuários por SQL, com as contas criadas e a de
// bootstrap, e nenhuma resposta traz o hash.
func TestAdminConsultaAMetaUsersPelaRotaDeConsulta(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	resp := consultaWithAuth(t, srv, metaUsersQuery, "chefe", passwordChefeAuth)
	defer fecha(t, resp)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200: %s", resp.StatusCode, body)
	}
	for _, want := range []string{`"username":"admin"`, `"username":"chefe"`, `"username":"leitor"`, `"role":"READ"`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("a resposta não traz %s:\n%s", want, body)
		}
	}

	star := consultaWithAuth(t, srv, `{"dataset":"d","sql":"SELECT * FROM _meta.users"}`, "chefe", passwordChefeAuth)
	defer fecha(t, star)
	all, err := io.ReadAll(star.Body)
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(all))
	for _, secret := range []string{"hash", "salt", passwordReaderAuth, passwordChefeAuth} {
		if strings.Contains(lower, strings.ToLower(secret)) {
			t.Errorf("SELECT * FROM _meta.users expôs %q:\n%s", secret, all)
		}
	}
}

// READ não lista contas nem descobre quem é ADMIN.
func TestLeitorNaoConsultaAMetaUsers(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	resp := consultaWithAuth(t, srv, metaUsersQuery, "leitor", passwordReaderAuth)
	status := resp.StatusCode
	if f := failOf(t, resp); f.Code != protocol.CodeForbidden || status != http.StatusForbidden {
		t.Fatalf("leitor consultando _meta.users: status %d código %q, quer 403 FORBIDDEN", status, f.Code)
	}
}

// O plano loopback é anônimo: nenhum principal, nenhuma marca de ADMIN.
func TestMetaUsersPeloLoopbackEhRecusada(t *testing.T) {
	loopbackPort := portaLivre(t)
	block := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n  authentication:\n    enabled: true\n" +
		"  loopback:\n    enabled: true\n    port: " + strconv.Itoa(loopbackPort) + "\n"
	srv := serverWithDataAndUsers(t, 1, block, authAccounts()...)

	resp, err := http.Post("http://"+srv.EnderecoLoopback()+"/v1/query", "application/json", strings.NewReader(metaUsersQuery))
	if err != nil {
		t.Fatal(err)
	}
	status := resp.StatusCode
	if f := failOf(t, resp); f.Code != protocol.CodeForbidden || status != http.StatusForbidden {
		t.Fatalf("_meta.users pelo loopback: status %d código %q, quer 403 FORBIDDEN", status, f.Code)
	}
}
