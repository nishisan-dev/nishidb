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
// via server.Novo + Roda e alcançado pela porta — o que auth_internal_test.go
// (package server) não cobre porque exige o pipeline real (handleQuery lendo
// o corpo, a vaga do §80, o registry de métricas de produção, o log durante o
// Roda). O que já é caixa branca — byte-identidade do 401, enum de razão,
// mecanismo de papel, atraso fixo — mora lá, mais rápido e sem listener.

const (
	varLeitorAuth = "NISHIDB_TESTE_AUTENTICACAO_LEITOR"
	varChefeAuth  = "NISHIDB_TESTE_AUTENTICACAO_CHEFE"

	senhaLeitorAuth = "senha-leitor-bem-forte"
	senhaChefeAuth  = "senha-chefe-bem-forte"
)

// blocoAutenticacao é o bloco `server:` com dois usuários — "leitor" (READ) e
// "chefe" (ADMIN) — no loopback (auth ligada sem TLS é aviso, não recusa,
// célula 1 da matriz do §77 — security_test.go).
func blocoAutenticacao(maisConfig string) string {
	return "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: leitor\n        passwordEnv: " + varLeitorAuth + "\n        role: READ\n" +
		"      - username: chefe\n        passwordEnv: " + varChefeAuth + "\n        role: ADMIN\n" +
		maisConfig
}

// servidorComAutenticacao sobe um servidor de verdade com autenticação
// ligada e n linhas materializadas no dataset "d", pelo mesmo caminho de
// servidorComDados (query_test.go).
func servidorComAutenticacao(t *testing.T, n int, maisConfig string) *server.Servidor {
	t.Helper()
	t.Setenv(varLeitorAuth, senhaLeitorAuth)
	t.Setenv(varChefeAuth, senhaChefeAuth)
	return servidorComDados(t, n, blocoAutenticacao(maisConfig))
}

// consultaComAuth faz o POST /v1/query com Basic opcional (usuario == ""
// significa SEM header Authorization nenhum).
func consultaComAuth(t *testing.T, srv *server.Servidor, corpo, usuario, senha string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query", strings.NewReader(corpo))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if usuario != "" {
		req.SetBasicAuth(usuario, senha)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// pegaComAuth faz o GET com Basic opcional.
func pegaComAuth(t *testing.T, srv *server.Servidor, caminho, usuario, senha string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+srv.Endereco()+caminho, nil)
	if err != nil {
		t.Fatal(err)
	}
	if usuario != "" {
		req.SetBasicAuth(usuario, senha)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func pegaCorpoComAuth(t *testing.T, srv *server.Servidor, caminho, usuario, senha string) string {
	t.Helper()
	resp := pegaComAuth(t, srv, caminho, usuario, senha)
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
// MaxCorpoRequisicao de anônimo é gasto. Se a ordem estivesse invertida
// (leRequisicao antes da autenticação), este corpo sairia 413, não 401: o
// mesmo tamanho que TestCorpoAcimaDoTetoRecebe413 (query_test.go) já prova
// que o servidor de fato rejeitaria se chegasse a ler.
func TestOrdem401AntesDeLerOCorpo(t *testing.T) {
	srv := servidorComAutenticacao(t, 3, "")

	corpoEnorme := strings.Repeat("x", protocol.MaxCorpoRequisicao+1)
	resp := consultaComAuth(t, srv, corpoEnorme, "", "")
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
	srv := servidorComAutenticacao(t, 30000, "runtime:\n  query:\n    maxConcurrent: 1\n")

	// A primeira consulta, AUTENTICADA, fica em voo segurando a única vaga: o
	// cliente lê uma linha e para de ler.
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("leitor", senhaLeitorAuth)
	primeira, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, primeira)

	l := protocol.NovoLeitor(primeira.Body)
	if !l.Next() {
		t.Fatalf("a primeira consulta não produziu row: %v", l.Err())
	}

	respAnon := consultaComAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "", "")
	defer fecha(t, respAnon)
	if respAnon.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: a vaga ocupada não pode se manifestar antes da autenticação", respAnon.StatusCode)
	}

	corpoMetrics := pegaCorpoComAuth(t, srv, "/metrics", "chefe", senhaChefeAuth)
	if !strings.Contains(corpoMetrics, `nishidb_query_total{dataset="d"} 1`) {
		t.Fatalf("nishidb_query_total não ficou em 1 após a tentativa anônima (ela não pode ter sido admitida):\n%s",
			corpoMetrics)
	}
}

// Regra 3 do §77: path desconhecido SEM credencial sai 401, nunca 404 — não
// revelar que rota existe ou não a quem não provou identidade nenhuma.
func TestOrdemRotaDesconhecidaSemCredencialEh401NaoEh404(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	resp := pegaComAuth(t, srv, "/rota-que-nao-existe-em-lugar-nenhum", "", "")
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: path desconhecido sem credencial não pode devolver 404", resp.StatusCode)
	}
}

// O outro lado, de sanidade: com credencial VÁLIDA, o roteamento normal
// continua valendo — path desconhecido é 404, não é engolido pelo
// middleware.
func TestRotaDesconhecidaComCredencialValidaAinda404(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	resp := pegaComAuth(t, srv, "/rota-que-nao-existe-em-lugar-nenhum", "leitor", senhaLeitorAuth)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, quer 404: com credencial válida o mux decide como sempre decidiu", resp.StatusCode)
	}
}

// Nenhuma das cinco rotas de hoje responde sem credencial — todas exigem
// READ no mínimo (§74-77: "no plano exposto TUDO exige auth").
func TestSemCredencialEmQualquerRotaProtegidaEh401(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	for _, rota := range []string{"/v1/health", "/metrics", "/v1/metadata", "/v1/stats"} {
		t.Run(rota, func(t *testing.T) {
			resp := pegaComAuth(t, srv, rota, "", "")
			defer fecha(t, resp)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d, quer 401", resp.StatusCode)
			}
			if wa := resp.Header.Get("WWW-Authenticate"); wa != `Basic realm="nishidb"` {
				t.Errorf("WWW-Authenticate = %q", wa)
			}
		})
	}

	respQ := consultaComAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "", "")
	defer fecha(t, respQ)
	if respQ.StatusCode != http.StatusUnauthorized {
		t.Errorf("/v1/query sem credencial: status = %d, quer 401", respQ.StatusCode)
	}
}

// ADMIN ⊇ READ ponta a ponta, nas cinco rotas de verdade — o mecanismo em si
// já está travado em tabela e com handler de teste em
// auth_internal_test.go; isto confirma que a fiação em montaMux
// registrou as cinco com RoleRead, como o brief pede.
func TestPapelReadEAdminAlcancamTodasAsRotasDeLeitura(t *testing.T) {
	srv := servidorComAutenticacao(t, 3, "")

	for _, cred := range []struct{ nome, usuario, senha string }{
		{"leitor(READ)", "leitor", senhaLeitorAuth},
		{"chefe(ADMIN)", "chefe", senhaChefeAuth},
	} {
		t.Run(cred.nome, func(t *testing.T) {
			for _, rota := range []string{"/v1/health", "/metrics", "/v1/metadata", "/v1/stats"} {
				assertStatusFechando(t, pegaComAuth(t, srv, rota, cred.usuario, cred.senha), http.StatusOK, rota)
			}

			respQ := consultaComAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1"}`, cred.usuario, cred.senha)
			if respQ.StatusCode != http.StatusOK {
				t.Errorf("/v1/query: status = %d", respQ.StatusCode)
				fecha(t, respQ)
				return
			}
			rows, _ := resultado(t, respQ)
			if len(rows) != 1 {
				t.Errorf("/v1/query: rows = %d, quer 1", len(rows))
			}
		})
	}
}

// O enum fechado do label `reason` chegando de verdade ao /metrics do
// processo (auth_internal_test.go já prova o mesmo isolado, sem
// listener; isto prova que registraMetricasDeAuth está de fato ligado ao
// registry que o servidor expõe).
func TestAuthFailuresTotalApareceNoScrapePorRazao(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	fecha(t, consultaComAuth(t, srv, `{}`, "", ""))                                // razaoHeader
	fecha(t, consultaComAuth(t, srv, `{}`, "usuario-nao-configurado", "qualquer")) // razaoUsuario
	fecha(t, consultaComAuth(t, srv, `{}`, "leitor", "senha-errada-de-verdade"))   // razaoSenha

	corpo := pegaCorpoComAuth(t, srv, "/metrics", "chefe", senhaChefeAuth)
	for _, linha := range []string{
		`nishidb_auth_failures_total{reason="header"} 1`,
		`nishidb_auth_failures_total{reason="usuario"} 1`,
		`nishidb_auth_failures_total{reason="senha"} 1`,
	} {
		if !strings.Contains(corpo, linha) {
			t.Errorf("scrape não tem %q:\n%s", linha, corpo)
		}
	}
}

// O header Authorization NUNCA é logado, em nenhum caminho de recusa — nem a
// senha em texto claro, nem o header codificado em base64 inteiro. Usa o
// servidor de VERDADE porque é durante o Roda (a goroutine que atende o
// request) que o log é escrito, e é exatamente essa concorrência entre
// escrita e leitura que registroDoServidor (compaction_test.go) já existe
// para proteger sob -race.
func TestAuthRecusadaLogaSemVazarAuthorizationHeader(t *testing.T) {
	t.Setenv(varLeitorAuth, senhaLeitorAuth)
	bloco := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: leitor\n        passwordEnv: " + varLeitorAuth + "\n        role: READ\n"

	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	registro := &registroDoServidor{}
	srv, err := server.Novo(a.carrega(), server.Opcoes{Log: logging.New(registro, logging.FormatoJSON, slog.LevelInfo)})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)

	const senhaPlantada = "SENHA-PLANTADA-PARA-VAZAMENTO-7Q2X"

	// Uma tentativa que falha por SENHA ERRADA de usuário configurado...
	fecha(t, consultaComAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "leitor", senhaPlantada))
	// ...e outra que falha por USUÁRIO DESCONHECIDO, senha plantada no mesmo
	// lugar (o dedo-gordo mais comum: senha digitada no campo de usuário).
	fecha(t, consultaComAuth(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`, "usuario-fantasma", senhaPlantada))

	saida := registro.String()
	if !strings.Contains(saida, "auth.recusada") {
		t.Fatalf("o evento auth.recusada não apareceu no log: %s", saida)
	}
	if strings.Contains(saida, senhaPlantada) {
		t.Fatalf("a senha plantada vazou para o log: %s", saida)
	}
	// O header Authorization CODIFICADO (base64 de "usuario:senha") também
	// não pode aparecer — não é só a senha em texto claro que é sensível.
	auth := base64.StdEncoding.EncodeToString([]byte("leitor:" + senhaPlantada))
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
