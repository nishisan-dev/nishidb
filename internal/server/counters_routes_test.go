package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// servidorComServicosAutenticado sobe o dataset com contadores atrás de
// autenticação — as rotas de auditoria e reconstrução são ADMIN.
func servidorComServicosAutenticado(t *testing.T, n int) *server.Servidor {
	t.Helper()
	t.Setenv(varLeitorAuth, senhaLeitorAuth)
	t.Setenv(varChefeAuth, senhaChefeAuth)
	a := novoAmbienteComStorage(t, blocoAutenticacao(""), "")
	a.escreveDataset("servicos", fmt.Sprintf(specServicos, "servicos"))
	semeiaServicos(t, a, "servicos", n)
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}

func TestAuditoriaDeContadoresPelaRota(t *testing.T) {
	srv := servidorComServicosAutenticado(t, 24)
	resp := postComAuth(t, srv, "/v1/counters/audit", `{"dataset":"servicos"}`, "chefe", senhaChefeAuth)
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
	if err := json.Unmarshal([]byte(pegaComAuthOK(t, srv, "/v1/stats")), &e); err != nil {
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
	srv := servidorComServicosAutenticado(t, 24)
	resp := postComAuth(t, srv, "/v1/counters/rebuild", "", "chefe", senhaChefeAuth)
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
	resp2 := postComAuth(t, srv, "/v1/counters/audit", "", "chefe", senhaChefeAuth)
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
	srv := servidorComServicosAutenticado(t, 1)
	for _, caminho := range []string{"/v1/counters/audit", "/v1/counters/rebuild"} {
		t.Run(caminho, func(t *testing.T) {
			anon := postComAuth(t, srv, caminho, "", "", "")
			status := anon.StatusCode
			fecha(t, anon)
			if status != http.StatusUnauthorized {
				t.Errorf("anônimo: status = %d, quer 401", status)
			}
			leitor := postComAuth(t, srv, caminho, "", "leitor", senhaLeitorAuth)
			status = leitor.StatusCode
			if f := falhaDe(t, leitor); f.Code != protocol.CodeForbidden {
				t.Errorf("leitor: código = %q, quer %q", f.Code, protocol.CodeForbidden)
			}
			if status != http.StatusForbidden {
				t.Errorf("leitor: status = %d, quer 403", status)
			}
		})
	}
}

func TestRotasDeContadoresRecusamMetodoErrado(t *testing.T) {
	srv := servidorComServicosAutenticado(t, 1)
	for _, caminho := range []string{"/v1/counters/audit", "/v1/counters/rebuild"} {
		t.Run(caminho, func(t *testing.T) {
			resp := pegaComAuth(t, srv, caminho, "chefe", senhaChefeAuth)
			status := resp.StatusCode
			if f := falhaDe(t, resp); f.Code != protocol.CodeMethod {
				t.Errorf("código = %q, quer %q", f.Code, protocol.CodeMethod)
			}
			if status != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, quer 405", status)
			}
		})
	}
}

func TestAuditoriaDeDatasetSemContadoresEhListaVazia(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")
	resp := postComAuth(t, srv, "/v1/counters/audit", `{"dataset":"d"}`, "chefe", senhaChefeAuth)
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
	resp2 := postComAuth(t, srv, "/v1/counters/audit", `{"dataset":"nao_existe"}`, "chefe", senhaChefeAuth)
	status := resp2.StatusCode
	if f := falhaDe(t, resp2); f.Code != protocol.CodeUnknownDataset {
		t.Errorf("código = %q, quer %q", f.Code, protocol.CodeUnknownDataset)
	}
	if status != http.StatusNotFound {
		t.Errorf("status = %d, quer 404", status)
	}
}

// pegaComAuthOK faz o GET autenticado como chefe e devolve o corpo.
func pegaComAuthOK(t *testing.T, srv *server.Servidor, caminho string) string {
	t.Helper()
	resp := pegaComAuth(t, srv, caminho, "chefe", senhaChefeAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", caminho, resp.StatusCode)
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
