package server_test

import (
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// Testes de caixa preta do middleware: aqui é o servidor de VERDADE, subido
// via server.Novo + Run e alcançado pela porta — o que auth_internal_test.go
// (package server) não cobre porque exige o pipeline real (handleQuery lendo
// o corpo, a vaga do §80, o registry de métricas de produção, o log durante o
// Run). O que já é caixa branca — byte-identidade do 401, enum de razão,
// mecanismo de papel, atraso fixo — mora lá, mais rápido e sem listener.

const (
	varReaderAuth = "NISHIDB_TESTE_AUTENTICACAO_LEITOR"
	varChefeAuth  = "NISHIDB_TESTE_AUTENTICACAO_CHEFE"

	passwordReaderAuth = "senha-leitor-bem-forte"
	passwordChefeAuth  = "senha-chefe-bem-forte"
)

// blockAuth é o bloco `server:` com dois usuários — "leitor" (READ) e
// "chefe" (ADMIN) — no loopback (auth ligada sem TLS é aviso, não recusa,
// célula 1 da matriz do §77 — security_test.go).
func blockAuth(maisConfig string) string {
	return "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: leitor\n        passwordEnv: " + varReaderAuth + "\n        role: READ\n" +
		"      - username: chefe\n        passwordEnv: " + varChefeAuth + "\n        role: ADMIN\n" +
		maisConfig
}

// serverWithAuth sobe um servidor de verdade com autenticação
// ligada e n linhas materializadas no dataset "d", pelo mesmo caminho de
// serverWithData (query_test.go).
func serverWithAuth(t *testing.T, n int, maisConfig string) *server.Server {
	t.Helper()
	t.Setenv(varReaderAuth, passwordReaderAuth)
	t.Setenv(varChefeAuth, passwordChefeAuth)
	return serverWithData(t, n, blockAuth(maisConfig))
}

// consultaWithAuth faz o POST /v1/query com Basic opcional (usuario == ""
// significa SEM header Authorization nenhum).
func consultaWithAuth(t *testing.T, srv *server.Server, corpo, usuario, password string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query", strings.NewReader(corpo))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if usuario != "" {
		req.SetBasicAuth(usuario, password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// fetchWithAuth faz o GET com Basic opcional.
func fetchWithAuth(t *testing.T, srv *server.Server, path, usuario, password string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+srv.Endereco()+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if usuario != "" {
		req.SetBasicAuth(usuario, password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func fetchCorpoWithAuth(t *testing.T, srv *server.Server, path, usuario, password string) string {
	t.Helper()
	resp := fetchWithAuth(t, srv, path, usuario, password)
	defer fecha(t, resp)
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func assertStatusFechando(t *testing.T, resp *http.Response, quer int, rota string) {
	t.Helper()
	defer fecha(t, resp)
	if resp.StatusCode != quer {
		t.Errorf("%s: status = %d, quer %d", rota, resp.StatusCode, quer)
	}
}

// Regra 1 do §77: 401 antes de LER o corpo — nem o teto do
// MaxRequestBody de anônimo é gasto. Se a ordem estivesse invertida
// (readRequisicao antes da autenticação), este corpo sairia 413, não 401: o
// mesmo tamanho que TestCorpoAcimaDoTetoRecebe413 (query_test.go) já prova
// que o servidor de fato rejeitaria se chegasse a ler.
func TestOrdem401AntesDeLerOCorpo(t *testing.T) {
	srv := serverWithAuth(t, 3, "")

	corpoEnorme := strings.Repeat("x", protocol.MaxRequestBody+1)
	resp := consultaWithAuth(t, srv, corpoEnorme, "", "")
	defer fecha(t, resp)

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: a leitura do corpo não pode acontecer antes da autenticação", resp.StatusCode)
	}
}

// Regra 2 do §77: 401 antes da vaga do §80. Com a única vaga ocupada por uma
// consulta AUTENTICADA em voo, uma requisição SEM credencial tem de sair 401
// — se a ordem estivesse invertida, o select não-bloqueante do §80
// encontraria a vaga ocupada e devolveria 503 em vez de 401. A prova vem
// dobrada: o status da resposta E a métrica nishidb_query_total, que só
// nishidb_query_total incrementa em s.met.admite (metrics_query.go),
// continuando em 1 depois da tentativa anônima.
func TestOrdem401AntesDaVagaDoSetenta(t *testing.T) {
	srv := serverWithAuth(t, 30000, "runtime:\n  query:\n    maxConcurrent: 1\n")

	// A primeira consulta, AUTENTICADA, fica em voo segurando a única vaga: o
	// cliente lê uma linha e para de ler.
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("leitor", passwordReaderAuth)
	primeira, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, primeira)

	l := protocol.NewReader(primeira.Body)
	if !l.Next() {
		t.Fatalf("a primeira consulta não produziu row: %v", l.Err())
	}

	respAnon := consultaWithAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "", "")
	defer fecha(t, respAnon)
	if respAnon.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: a vaga ocupada não pode se manifestar antes da autenticação", respAnon.StatusCode)
	}

	corpoMetrics := fetchCorpoWithAuth(t, srv, "/metrics", "chefe", passwordChefeAuth)
	if !strings.Contains(corpoMetrics, `nishidb_query_total{dataset="d"} 1`) {
		t.Fatalf("nishidb_query_total não ficou em 1 após a tentativa anônima (ela não pode ter sido admitida):\n%s",
			corpoMetrics)
	}
}

// Regra 3 do §77: path desconhecido SEM credencial sai 401, nunca 404 — não
// revelar que rota existe ou não a quem não provou identidade nenhuma.
func TestOrdemRotaDesconhecidaSemCredencialEh401NaoEh404(t *testing.T) {
	srv := serverWithAuth(t, 1, "")

	resp := fetchWithAuth(t, srv, "/rota-que-nao-existe-em-lugar-nenhum", "", "")
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: path desconhecido sem credencial não pode devolver 404", resp.StatusCode)
	}
}

// O outro lado, de sanidade: com credencial VÁLIDA, o roteamento normal
// continua valendo — path desconhecido é 404, não é engolido pelo
// middleware.
func TestRotaDesconhecidaComCredencialValidaAinda404(t *testing.T) {
	srv := serverWithAuth(t, 1, "")

	resp := fetchWithAuth(t, srv, "/rota-que-nao-existe-em-lugar-nenhum", "leitor", passwordReaderAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, quer 404: com credencial válida o mux decide como sempre decidiu", resp.StatusCode)
	}
}

// Nenhuma das cinco rotas de hoje responde sem credencial — todas exigem
// READ no mínimo (§74-77: "no plano exposto TUDO exige auth").
func TestSemCredencialEmQualquerRotaProtegidaEh401(t *testing.T) {
	srv := serverWithAuth(t, 1, "")

	for _, rota := range []string{"/v1/health", "/metrics", "/v1/metadata", "/v1/stats"} {
		t.Run(rota, func(t *testing.T) {
			resp := fetchWithAuth(t, srv, rota, "", "")
			defer fecha(t, resp)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d, quer 401", resp.StatusCode)
			}
			if wa := resp.Header.Get("WWW-Authenticate"); wa != `Basic realm="nishidb"` {
				t.Errorf("WWW-Authenticate = %q", wa)
			}
		})
	}

	respQ := consultaWithAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "", "")
	defer fecha(t, respQ)
	if respQ.StatusCode != http.StatusUnauthorized {
		t.Errorf("/v1/query sem credencial: status = %d, quer 401", respQ.StatusCode)
	}
}

// ADMIN ⊇ READ ponta a ponta, nas cinco rotas de verdade — o mecanismo em si
// já está travado em tabela e com handler de teste em
// auth_internal_test.go; isto confirma que a fiação em buildMux
// registrou as cinco com RoleRead, como o brief pede.
func TestPapelReadEAdminAlcancamTodasAsRotasDeLeitura(t *testing.T) {
	srv := serverWithAuth(t, 3, "")

	for _, cred := range []struct{ name, usuario, password string }{
		{"leitor(READ)", "leitor", passwordReaderAuth},
		{"chefe(ADMIN)", "chefe", passwordChefeAuth},
	} {
		t.Run(cred.name, func(t *testing.T) {
			for _, rota := range []string{"/v1/health", "/metrics", "/v1/metadata", "/v1/stats"} {
				assertStatusFechando(t, fetchWithAuth(t, srv, rota, cred.usuario, cred.password), http.StatusOK, rota)
			}

			respQ := consultaWithAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1"}`, cred.usuario, cred.password)
			if respQ.StatusCode != http.StatusOK {
				t.Errorf("/v1/query: status = %d", respQ.StatusCode)
				fecha(t, respQ)
				return
			}
			rows, _ := result(t, respQ)
			if len(rows) != 1 {
				t.Errorf("/v1/query: rows = %d, quer 1", len(rows))
			}
		})
	}
}

// O enum fechado do label `reason` chegando de verdade ao /metrics do
// processo (auth_internal_test.go já prova o mesmo isolado, sem
// listener; isto prova que registerMetricsOfAuth está de fato ligado ao
// registry que o servidor expõe).
func TestAuthFailuresTotalApareceNoScrapePorRazao(t *testing.T) {
	srv := serverWithAuth(t, 1, "")

	fecha(t, consultaWithAuth(t, srv, `{}`, "", ""))                                // razaoHeader
	fecha(t, consultaWithAuth(t, srv, `{}`, "usuario-nao-configurado", "qualquer")) // razaoUsuario
	fecha(t, consultaWithAuth(t, srv, `{}`, "leitor", "senha-errada-de-verdade"))   // razaoPassword

	corpo := fetchCorpoWithAuth(t, srv, "/metrics", "chefe", passwordChefeAuth)
	for _, row := range []string{
		`nishidb_auth_failures_total{reason="header"} 1`,
		`nishidb_auth_failures_total{reason="usuario"} 1`,
		`nishidb_auth_failures_total{reason="senha"} 1`,
	} {
		if !strings.Contains(corpo, row) {
			t.Errorf("scrape não tem %q:\n%s", row, corpo)
		}
	}
}

// O header Authorization NUNCA é logado, em nenhum caminho de recusa — nem a
// senha em texto claro, nem o header codificado em base64 inteiro. Usa o
// servidor de VERDADE porque é durante o Run (a goroutine que atende o
// request) que o log é escrito, e é exatamente essa concorrência entre
// escrita e leitura que recordOfServer (compaction_test.go) já existe
// para proteger sob -race.
func TestAuthRecusadaLogaSemVazarAuthorizationHeader(t *testing.T) {
	t.Setenv(varReaderAuth, passwordReaderAuth)
	block := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: leitor\n        passwordEnv: " + varReaderAuth + "\n        role: READ\n"

	a := newEnvWith(t, block)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	record := &recordOfServer{}
	srv, err := server.Novo(a.load(), server.Options{Log: logging.New(record, logging.FormatJSON, slog.LevelInfo)})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	const passwordPlantada = "SENHA-PLANTADA-PARA-VAZAMENTO-7Q2X"

	// Uma tentativa que falha por SENHA ERRADA de usuário configurado...
	fecha(t, consultaWithAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "leitor", passwordPlantada))
	// ...e outra que falha por USUÁRIO DESCONHECIDO, senha plantada no mesmo
	// lugar (o dedo-gordo mais comum: senha digitada no campo de usuário).
	fecha(t, consultaWithAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "usuario-fantasma", passwordPlantada))

	saida := record.String()
	if !strings.Contains(saida, "auth.recusada") {
		t.Fatalf("o evento auth.recusada não apareceu no log: %s", saida)
	}
	if strings.Contains(saida, passwordPlantada) {
		t.Fatalf("a senha plantada vazou para o log: %s", saida)
	}
	// O header Authorization CODIFICADO (base64 de "usuario:senha") também
	// não pode aparecer — não é só a senha em texto claro que é sensível.
	auth := base64.StdEncoding.EncodeToString([]byte("leitor:" + passwordPlantada))
	if strings.Contains(saida, auth) {
		t.Fatalf("o header Authorization codificado vazou para o log: %s", saida)
	}
	if !strings.Contains(saida, `"user":"leitor"`) {
		t.Errorf("log não nomeia o usuário CONFIGURADO na falha de senha: %s", saida)
	}
	if !strings.Contains(saida, `"user":"desconhecido"`) {
		t.Errorf("log não usa 'desconhecido' para o usuário não configurado: %s", saida)
	}
}
