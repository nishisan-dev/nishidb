package cli_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// resyncRequest é o que o servidor de mentira viu do pedido.
type resyncRequest struct {
	method, path, dataset, destination, user string
	hasAuth                                  bool
}

// fakeResyncServer responde a rota do snapshot sob demanda com status e corpo
// dados, e guarda o último pedido recebido.
func fakeResyncServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Pointer[resyncRequest]) {
	t.Helper()
	var seen atomic.Pointer[resyncRequest]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, ok := r.BasicAuth()
		seen.Store(&resyncRequest{
			method: r.Method, path: r.URL.Path,
			dataset: r.URL.Query().Get("dataset"), destination: r.URL.Query().Get("destination"),
			user: user, hasAuth: ok,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &seen
}

// O comando fala POST na rota ADMIN, com o dataset e o destino na query e a
// credencial do --user, e confirma o PEDIDO na saída. A recusa do servidor
// sai pela tabela de exit codes, com a mensagem dele no stderr.
func TestCLIReplicationResyncCallsRoute(t *testing.T) {
	srv, seen := fakeResyncServer(t, http.StatusAccepted, `{"dataset":"events","destination":"cdc","requested":true,"state":"failed"}`)
	a := newEnv(t)
	t.Setenv("NISHIDB_PASSWORD", "senha-qualquer-do-servidor-de-mentira")
	if code := a.run("replication", "resync", "--server", srv.URL, "--dataset", "events", "--destination", "cdc", "--user", "chefe"); code != 0 {
		t.Fatalf("exit = %d, quero 0: %s", code, a.stderr.String())
	}
	got := seen.Load()
	if got == nil {
		t.Fatal("o comando não chamou o servidor")
	}
	want := resyncRequest{method: http.MethodPost, path: "/v1/replication/resync", dataset: "events", destination: "cdc", user: "chefe", hasAuth: true}
	if *got != want {
		t.Fatalf("pedido = %+v, quero %+v", *got, want)
	}
	out := a.stdout.String()
	if !strings.Contains(out, "DESTINATION") || !strings.Contains(out, "events") || !strings.Contains(out, "cdc") || !strings.Contains(out, "yes") {
		t.Fatalf("a saída não confirma o pedido:\n%s", out)
	}
	// O estado do publicador sai na saída: com ele parado, o pedido só é
	// atendido na próxima subida.
	if !strings.Contains(out, "PUBLISHER STATE") || !strings.Contains(out, "failed") {
		t.Fatalf("a saída não mostra o estado do publicador:\n%s", out)
	}

	refusing, _ := fakeResyncServer(t, http.StatusNotFound,
		`{"error":{"code":"UNKNOWN_DATASET","message":"destination \"nope\" is not a Kafka replication target of dataset \"events\""}}`)
	if code := a.run("replication", "resync", "--server", refusing.URL, "--dataset", "events", "--destination", "nope", "--user", "chefe"); code != 1 {
		t.Fatalf("recusa: exit = %d, quero 1: %s", code, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), `destination "nope" is not a Kafka replication target`) {
		t.Fatalf("a recusa não repassa a mensagem do servidor:\n%q", a.stderr.String())
	}

	// O 200 não é o contrato da rota: o comando não o confunde com o pedido
	// registrado.
	wrong, _ := fakeResyncServer(t, http.StatusOK, `{"dataset":"events","destination":"cdc","requested":true}`)
	if code := a.run("replication", "resync", "--server", wrong.URL, "--dataset", "events", "--destination", "cdc", "--user", "chefe"); code != 1 {
		t.Fatalf("o 200: exit = %d, quero 1:\n%s", code, a.stdout.String())
	}
	if !strings.Contains(a.stderr.String(), "200 OK") || !strings.Contains(a.stderr.String(), "202 Accepted") {
		t.Fatalf("a recusa do 200 não nomeia o status recebido e o esperado:\n%s", a.stderr.String())
	}
}

// Sem --destination o comando é recusado como erro de uso, sem chamar o
// servidor.
func TestCLIReplicationResyncRequiresDatasetAndDestination(t *testing.T) {
	srv, seen := fakeResyncServer(t, http.StatusAccepted, `{}`)
	a := newEnv(t)
	if code := a.run("replication", "resync", "--server", srv.URL, "--dataset", "events"); code != 1 {
		t.Fatalf("exit = %d, quero 1", code)
	}
	if !strings.Contains(a.stderr.String(), "--dataset and --destination are required") {
		t.Fatalf("stderr sem a recusa própria:\n%s", a.stderr.String())
	}
	if seen.Load() != nil {
		t.Fatal("o comando chamou o servidor sem o destino")
	}
	if code := a.run("replication", "nope"); code != 1 || !strings.Contains(a.stderr.String(), "nishidb replication resync") {
		t.Fatalf("subcomando desconhecido: exit = %d, stderr:\n%s", code, a.stderr.String())
	}
}
