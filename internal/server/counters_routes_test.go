package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// serverWithServicesAutenticado sobe o dataset com contadores atrás de
// autenticação — as rotas de auditoria e reconstrução são ADMIN.
func serverWithServicesAutenticado(t *testing.T, n int) *server.Server {
	t.Helper()
	a := newEnvWithStorage(t, blockAuth(""), "")
	a.writeDataset("servicos", fmt.Sprintf(specServices, "servicos"))
	seedServices(t, a, "servicos", n)
	provisionUsers(t, a, authAccounts()...)
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}

func TestAuditoriaDeContadoresPelaRota(t *testing.T) {
	srv := serverWithServicesAutenticado(t, 24)
	resp := postWithAuth(t, srv, "/v1/counters/audit", `{"dataset":"servicos"}`, "chefe", passwordChefeAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
	var out protocol.CountersAuditResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Datasets) != 1 || out.Datasets[0].Name != "servicos" || len(out.Datasets[0].Counters) != 3 {
		t.Fatalf("resposta = %+v", out)
	}
	for _, c := range out.Datasets[0].Counters {
		if c.DivergentGroups != 0 || c.NotInterned != 0 {
			t.Fatalf("%s divergiu num dataset recém-construído: %+v", c.Name, c)
		}
		if c.RowsScanned != 24 || c.Groups == 0 || c.Maintained != c.Recomputed {
			t.Fatalf("%s = %+v", c.Name, c)
		}
	}
	// A auditoria conclusiva aparece no /v1/stats e no scrape pela mesma
	// leitura: timestamp diferente de zero, divergentes em zero.
	var e protocol.Stats
	if err := json.Unmarshal([]byte(fetchWithAuthOK(t, srv, "/v1/stats")), &e); err != nil {
		t.Fatal(err)
	}
	for _, ds := range e.Datasets {
		if ds.Name != "servicos" {
			continue
		}
		for _, c := range ds.Counters {
			if c.AuditLastSuccessUnix == 0 || c.AuditDivergentGroups != 0 {
				t.Fatalf("stats de %s depois da auditoria: %+v", c.Name, c)
			}
		}
	}
}

func TestReconstrucaoDeContadoresPelaRota(t *testing.T) {
	srv := serverWithServicesAutenticado(t, 24)
	resp := postWithAuth(t, srv, "/v1/counters/rebuild", "", "chefe", passwordChefeAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
	var out protocol.CountersRebuildResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	// Corpo ausente: todos os datasets — o "d" de teste não existe aqui, só
	// "servicos".
	if len(out.Datasets) != 1 || len(out.Datasets[0].Counters) != 3 {
		t.Fatalf("resposta = %+v", out)
	}
	for _, c := range out.Datasets[0].Counters {
		if c.Rows != 24 || c.Groups == 0 {
			t.Fatalf("%s = %+v", c.Name, c)
		}
	}
	// Depois de reconstruir, a auditoria continua limpa.
	resp2 := postWithAuth(t, srv, "/v1/counters/audit", "", "chefe", passwordChefeAuth)
	defer fecha(t, resp2)
	var aud protocol.CountersAuditResponse
	if err := json.NewDecoder(resp2.Body).Decode(&aud); err != nil {
		t.Fatal(err)
	}
	for _, c := range aud.Datasets[0].Counters {
		if c.DivergentGroups != 0 {
			t.Fatalf("%s divergiu depois de reconstruir: %+v", c.Name, c)
		}
	}
}

func TestRotasDeContadoresExigemAutenticacaoEPapelADMIN(t *testing.T) {
	srv := serverWithServicesAutenticado(t, 1)
	for _, path := range []string{"/v1/counters/audit", "/v1/counters/rebuild"} {
		t.Run(path, func(t *testing.T) {
			anon := postWithAuth(t, srv, path, "", "", "")
			status := anon.StatusCode
			fecha(t, anon)
			if status != http.StatusUnauthorized {
				t.Errorf("anônimo: status = %d, quer 401", status)
			}
			reader := postWithAuth(t, srv, path, "", "leitor", passwordReaderAuth)
			status = reader.StatusCode
			if f := failOf(t, reader); f.Code != protocol.CodeForbidden {
				t.Errorf("leitor: código = %q, quer %q", f.Code, protocol.CodeForbidden)
			}
			if status != http.StatusForbidden {
				t.Errorf("leitor: status = %d, quer 403", status)
			}
		})
	}
}

func TestRotasDeContadoresRecusamMetodoErrado(t *testing.T) {
	srv := serverWithServicesAutenticado(t, 1)
	for _, path := range []string{"/v1/counters/audit", "/v1/counters/rebuild"} {
		t.Run(path, func(t *testing.T) {
			resp := fetchWithAuth(t, srv, path, "chefe", passwordChefeAuth)
			status := resp.StatusCode
			if f := failOf(t, resp); f.Code != protocol.CodeMethod {
				t.Errorf("código = %q, quer %q", f.Code, protocol.CodeMethod)
			}
			if status != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, quer 405", status)
			}
		})
	}
}

func TestAuditoriaDeDatasetSemContadoresEhListaVazia(t *testing.T) {
	srv := serverWithAuth(t, 1, "")
	resp := postWithAuth(t, srv, "/v1/counters/audit", `{"dataset":"d"}`, "chefe", passwordChefeAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
	var out protocol.CountersAuditResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if len(out.Datasets) != 1 || out.Datasets[0].Counters == nil || len(out.Datasets[0].Counters) != 0 {
		t.Fatalf("dataset sem contadores tem de responder lista vazia: %+v", out)
	}
	resp2 := postWithAuth(t, srv, "/v1/counters/audit", `{"dataset":"nao_existe"}`, "chefe", passwordChefeAuth)
	status := resp2.StatusCode
	if f := failOf(t, resp2); f.Code != protocol.CodeUnknownDataset {
		t.Errorf("código = %q, quer %q", f.Code, protocol.CodeUnknownDataset)
	}
	if status != http.StatusNotFound {
		t.Errorf("status = %d, quer 404", status)
	}
}

// fetchWithAuthOK faz o GET autenticado como chefe e devolve o corpo.
func fetchWithAuthOK(t *testing.T, srv *server.Server, path string) string {
	t.Helper()
	resp := fetchWithAuth(t, srv, path, "chefe", passwordChefeAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", path, resp.StatusCode)
	}
	var sb []byte
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb = append(sb, buf[:n]...)
		if err != nil {
			break
		}
	}
	return string(sb)
}
