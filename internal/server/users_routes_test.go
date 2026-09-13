package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// userResponseOf decodifica a resposta de 200 de uma rota de usuário.
func userResponseOf(t *testing.T, resp *http.Response) protocol.UserResponse {
	t.Helper()
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, quer 200: %s", resp.StatusCode, b)
	}
	var out protocol.UserResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// healthStatus faz GET /v1/health com a credencial e devolve só o status.
func healthStatus(t *testing.T, srv interface{ Endereco() string }, user, password string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+srv.Endereco()+"/v1/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(user, password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, resp)
	return resp.StatusCode
}

// O ciclo inteiro com o servidor no ar: criar, autenticar, promover, remover —
// sem editar YAML e sem reiniciar.
func TestGestaoDeUsuarioComOServidorNoAr(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	const anaPassword = "senha-da-ana-longa"

	out := userResponseOf(t, postWithAuth(t, srv, "/v1/users/create",
		`{"username":"ana","role":"READ","password":"`+anaPassword+`"}`, "chefe", passwordChefeAuth))
	if out.Username != "ana" || out.Role != "READ" {
		t.Errorf("resposta da criação = %+v", out)
	}
	if got := healthStatus(t, srv, "ana", anaPassword); got != http.StatusOK {
		t.Fatalf("usuária recém-criada não autentica: status %d", got)
	}

	// READ ainda não cria ninguém...
	resp := postWithAuth(t, srv, "/v1/users/create", `{"username":"bia","role":"READ","password":"senha-da-bia-longa"}`,
		"ana", anaPassword)
	fecha(t, resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("READ criando usuário: status %d, quer 403", resp.StatusCode)
	}
	// ...até ser promovida, e a promoção vale no request seguinte.
	userResponseOf(t, postWithAuth(t, srv, "/v1/users/role", `{"username":"ana","role":"ADMIN"}`, "chefe", passwordChefeAuth))
	userResponseOf(t, postWithAuth(t, srv, "/v1/users/create",
		`{"username":"bia","role":"READ","password":"senha-da-bia-longa"}`, "ana", anaPassword))

	userResponseOf(t, postWithAuth(t, srv, "/v1/users/delete", `{"username":"ana"}`, "chefe", passwordChefeAuth))
	if got := healthStatus(t, srv, "ana", anaPassword); got != http.StatusUnauthorized {
		t.Fatalf("usuária removida continua autenticando: status %d", got)
	}
}

// A troca da própria senha é o direito de todo usuário; a de outro, só do
// ADMIN. A recusa para READ vem antes de consultar a tabela: pedir a senha de
// um nome que não existe dá o MESMO 403 de um que existe.
func TestLeitorTrocaAPropriaSenhaENuncaADeOutro(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	const fresh = "senha-nova-do-leitor"

	for _, target := range []string{"chefe", "ninguem"} {
		resp := postWithAuth(t, srv, "/v1/users/password",
			`{"username":"`+target+`","password":"`+fresh+`"}`, "leitor", passwordReaderAuth)
		status := resp.StatusCode
		if f := failOf(t, resp); f.Code != protocol.CodeForbidden || status != http.StatusForbidden {
			t.Errorf("leitor trocando a senha de %q: status %d código %q, quer 403 FORBIDDEN", target, status, f.Code)
		}
	}

	out := userResponseOf(t, postWithAuth(t, srv, "/v1/users/password",
		`{"username":"leitor","password":"`+fresh+`"}`, "leitor", passwordReaderAuth))
	if out.Role != "READ" {
		t.Errorf("a troca de senha mudou o papel: %+v", out)
	}
	if got := healthStatus(t, srv, "leitor", passwordReaderAuth); got != http.StatusUnauthorized {
		t.Errorf("a senha antiga continua valendo: status %d", got)
	}
	if got := healthStatus(t, srv, "leitor", fresh); got != http.StatusOK {
		t.Errorf("a senha nova não autentica: status %d", got)
	}

	// ADMIN troca a de qualquer um.
	userResponseOf(t, postWithAuth(t, srv, "/v1/users/password",
		`{"username":"leitor","password":"senha-escolhida-pelo-chefe"}`, "chefe", passwordChefeAuth))
	if got := healthStatus(t, srv, "leitor", "senha-escolhida-pelo-chefe"); got != http.StatusOK {
		t.Errorf("a senha trocada pelo ADMIN não autentica: status %d", got)
	}
}

func TestRecusasDaGestaoDeUsuarioSaemComOCodigoEstavel(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	cases := []struct {
		name   string
		path   string
		body   string
		status int
		code   string
	}{
		{"nome existente", "/v1/users/create", `{"username":"leitor","role":"READ","password":"senha-bem-longa"}`,
			http.StatusConflict, protocol.CodeUserExists},
		{"senha curta", "/v1/users/create", `{"username":"ana","role":"READ","password":"curta"}`,
			http.StatusBadRequest, protocol.CodeWeakPassword},
		{"papel invalido", "/v1/users/create", `{"username":"ana","role":"WRITE","password":"senha-bem-longa"}`,
			http.StatusBadRequest, protocol.CodeBadParameter},
		{"nome invalido", "/v1/users/create", `{"username":"Ana","role":"READ","password":"senha-bem-longa"}`,
			http.StatusBadRequest, protocol.CodeBadParameter},
		{"usuario inexistente", "/v1/users/role", `{"username":"ninguem","role":"READ"}`,
			http.StatusNotFound, protocol.CodeUnknownUser},
		{"campo desconhecido", "/v1/users/delete", `{"username":"leitor","force":true}`,
			http.StatusBadRequest, protocol.CodeBadRequest},
		{"corpo ausente", "/v1/users/delete", "",
			http.StatusBadRequest, protocol.CodeBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := postWithAuth(t, srv, c.path, c.body, "chefe", passwordChefeAuth)
			status := resp.StatusCode
			if f := failOf(t, resp); f.Code != c.code || status != c.status {
				t.Errorf("status %d código %q, quer %d %q (%s)", status, f.Code, c.status, c.code, f.Message)
			}
		})
	}

	// O último ADMIN: com o de bootstrap removido, o chefe é o único.
	userResponseOf(t, postWithAuth(t, srv, "/v1/users/delete", `{"username":"admin"}`, "chefe", passwordChefeAuth))
	for _, body := range []string{`{"username":"chefe"}`} {
		resp := postWithAuth(t, srv, "/v1/users/delete", body, "chefe", passwordChefeAuth)
		status := resp.StatusCode
		if f := failOf(t, resp); f.Code != protocol.CodeLastAdmin || status != http.StatusConflict {
			t.Errorf("remover o último ADMIN: status %d código %q, quer 409 LAST_ADMIN", status, f.Code)
		}
	}
	resp := postWithAuth(t, srv, "/v1/users/role", `{"username":"chefe","role":"READ"}`, "chefe", passwordChefeAuth)
	status := resp.StatusCode
	if f := failOf(t, resp); f.Code != protocol.CodeLastAdmin || status != http.StatusConflict {
		t.Errorf("rebaixar o último ADMIN: status %d código %q, quer 409 LAST_ADMIN", status, f.Code)
	}

	get, err := http.NewRequest(http.MethodGet, "http://"+srv.Endereco()+"/v1/users/create", nil)
	if err != nil {
		t.Fatal(err)
	}
	get.SetBasicAuth("chefe", passwordChefeAuth)
	getResp, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	fecha(t, getResp)
	if getResp.StatusCode != http.StatusMethodNotAllowed || getResp.Header.Get("Allow") != http.MethodPost {
		t.Errorf("GET numa rota de usuário: status %d Allow %q, quer 405 POST", getResp.StatusCode, getResp.Header.Get("Allow"))
	}
}

// Nenhuma recusa devolve a senha que veio no corpo — nem a de política, nem a
// de JSON malformado, cujo erro do decoder poderia citar o trecho.
func TestRecusaDaGestaoNuncaEcoaASenha(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	const planted = "SENHA-PLANTADA-3K9"
	for _, body := range []string{
		`{"username":"ana","role":"READ","password":"` + planted + `"`,
		`{"username":"ana","role":"READ","password":"` + planted + `","extra":1}`,
		`{"username":"leitor","role":"READ","password":"` + planted + `"}`,
	} {
		resp := postWithAuth(t, srv, "/v1/users/create", body, "chefe", passwordChefeAuth)
		b, err := io.ReadAll(resp.Body)
		fecha(t, resp)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("corpo %q foi aceito", body)
		}
		if strings.Contains(string(b), planted) {
			t.Errorf("a resposta ecoou a senha: %s", b)
		}
	}
}

// O plano loopback é anônimo por construção: a rota de senha (READ) chega ao
// handler sem principal e é recusada lá; as rotas ADMIN nem chegam, porque o
// loopback recusa todo papel ADMIN.
func TestGestaoDeUsuarioPeloLoopbackEhRecusada(t *testing.T) {
	loopbackPort := portaLivre(t)
	block := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n  authentication:\n    enabled: true\n" +
		"  loopback:\n    enabled: true\n    port: " + strconv.Itoa(loopbackPort) + "\n"
	srv := serverWithDataAndUsers(t, 1, block, authAccounts()...)

	for _, c := range []struct {
		path string
		body string
	}{
		{"/v1/users/password", `{"username":"leitor","password":"senha-nova-bem-longa"}`},
		{"/v1/users/create", `{"username":"ana","role":"ADMIN","password":"senha-nova-bem-longa"}`},
	} {
		resp, err := http.Post("http://"+srv.EnderecoLoopback()+c.path, "application/json", strings.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		status := resp.StatusCode
		if f := failOf(t, resp); f.Code != protocol.CodeForbidden || status != http.StatusForbidden {
			t.Errorf("%s pelo loopback: status %d código %q, quer 403 FORBIDDEN", c.path, status, f.Code)
		}
	}
	if got := healthStatus(t, srv, "leitor", passwordReaderAuth); got != http.StatusOK {
		t.Errorf("a senha do leitor mudou por um request anônimo: status %d", got)
	}
}

// Com a autenticação desligada não há principal, e não existe gestão anônima.
func TestGestaoDeUsuarioComAutenticacaoDesligadaEhRecusada(t *testing.T) {
	srv := serverWithData(t, 1, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n")
	resp := postWithAuth(t, srv, "/v1/users/create", `{"username":"ana","role":"ADMIN","password":"senha-bem-longa"}`, "", "")
	status := resp.StatusCode
	if f := failOf(t, resp); f.Code != protocol.CodeForbidden || status != http.StatusForbidden {
		t.Errorf("gestão sem autenticação: status %d código %q, quer 403 FORBIDDEN", status, f.Code)
	}
}
