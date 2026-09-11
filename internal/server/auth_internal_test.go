package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// Testes de caixa branca do middleware (auth.go): tudo aqui exercita
// comAutenticacao/autentica diretamente, contra um *Servidor mínimo e um
// *roteador com handlers de teste — sem passar por server.New, config.Config
// nem por um listener de verdade. É deliberado: a lógica de autenticação e
// papel não depende de dataset, Store nem Engine nenhum, e montar tudo isso só
// para testar um compare de credencial seria o disfarce de integração que
// esconde qual PEDAÇO de código o teste realmente prende.
//
// Os testes que dependem do servidor de VERDADE (ordem contra a vaga do §80,
// contra a leitura do corpo, scrape de métrica pela porta, log durante o Roda)
// estão em auth_test.go (package server_test).

// senhaLeitorDeTeste e senhaChefeDeTeste são as senhas em texto claro por trás
// dos dois digests de servidorDeTeste — nomeadas para os testes que precisam
// autenticar de verdade, e não só inspecionar a credencial.
const (
	senhaLeitorDeTeste = "senha-leitor"
	senhaChefeDeTeste  = "senha-chefe"
)

// servidorDeTeste é o *Servidor mínimo que comAutenticacao/autentica
// precisam: log, credenciais e a métrica de falha. Nenhum outro campo de
// Servidor é tocado pelo middleware.
func servidorDeTeste(log *logging.Logger) *Servidor {
	if log == nil {
		log = logging.Discard()
	}
	return &Servidor{
		log: log,
		credenciais: map[string]credencial{
			"leitor": {digestSenha: sha256.Sum256([]byte(senhaLeitorDeTeste)), role: config.RoleRead},
			"chefe":  {digestSenha: sha256.Sum256([]byte(senhaChefeDeTeste)), role: config.RoleAdmin},
		},
		authFalhas: registraMetricasDeAuth(metrics.NewRegistry()),
	}
}

// roteadorDeTeste é O HANDLER DE TESTE que o brief da B3 pede para travar o
// mecanismo "rota exige papel X" antes de a B5 ter uma rota ADMIN-only de
// verdade: /rota-read exige READ (o piso de toda rota hoje) e /rota-admin
// exige ADMIN — a mesma chamada rt.registra que a B5 vai usar.
func roteadorDeTeste() *roteador {
	rt := novoRoteador()
	rt.registra("/rota-read", config.RoleRead, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("read-ok"))
	})
	rt.registra("/rota-admin", config.RoleAdmin, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("admin-ok"))
	})
	return rt
}

// A paridade byte a byte do §77 provada estruturalmente, não por
// comportamento: com autenticação desligada, comAutenticacao devolve o
// PRÓPRIO mux, não uma função que "deixa passar" — não existe função nenhuma
// a mais no caminho do request além do que sobeHTTP já registrava antes desta
// task.
func TestComAutenticacaoDesligadaDevolveOMuxCruSemEmbrulho(t *testing.T) {
	s := &Servidor{log: logging.Discard()} // credenciais == nil: autenticação desligada
	rt := roteadorDeTeste()

	got := s.comAutenticacao(rt)

	if got != rt.Cru() {
		t.Fatal("com autenticação desligada, comAutenticacao precisa devolver o PRÓPRIO mux (via Cru()): " +
			"qualquer wrapper, mesmo um que só delegasse, já não seria byte a byte igual ao caminho de hoje")
	}
}

// O outro lado: ligada, o mux cru NUNCA é devolvido diretamente — senão não
// haveria onde autenticar.
func TestComAutenticacaoLigadaEmbrulhaOMux(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()

	got := s.comAutenticacao(rt)

	if got == rt.Cru() {
		t.Fatal("com autenticação ligada, comAutenticacao não pode devolver o mux cru diretamente")
	}
}

// O fail-closed do mecanismo de papel (Important da revisão pós-implementação):
// uma rota registrada por FORA de roteador.registra — direto em rt.mux, sem
// papel declarado — tem de ser RECUSADA, nunca servida sem checagem, mesmo
// para o usuário ADMIN. Prova que a segurança do mecanismo vem da
// CONSTRUÇÃO (comAutenticacao recusa padrão sem entrada em papeis), não da
// disciplina de quem registra rota nova — a B5 é quem primeiro acrescenta
// rotas ADMIN-only de verdade sobre esta costura.
func TestRotaRegistradaPorForaDeRegistraEhRecusadaFailClosed(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()

	// Bypass DELIBERADO: grava direto no mux, sem passar por rt.registra —
	// exatamente o cenário que o mecanismo tem de fechar sozinho.
	rt.mux.HandleFunc("/rota-esquecida", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("nao-deveria-chegar-aqui"))
	})

	h := s.comAutenticacao(rt)

	req := httptest.NewRequest(http.MethodGet, "/rota-esquecida", nil)
	req.SetBasicAuth("chefe", senhaChefeDeTeste) // ADMIN — nem o papel mais alto deveria bastar
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Fatalf("a rota registrada por fora foi SERVIDA sem checagem de papel: %s", rec.Body.String())
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, quer 500 (bug de registro nomeado, não passagem livre)", rec.Code)
	}
	var corpo protocol.ErrorBody
	if err := json.NewDecoder(rec.Body).Decode(&corpo); err != nil {
		t.Fatal(err)
	}
	// Retoque da revisão da B3 (dono natural: B4): o corpo HTTP é genérico —
	// nomear a rota ou o símbolo interno (roteador.registra) para um cliente
	// já AUTENTICADO devolveria detalhe de implementação sem relação com quem
	// só tem uma credencial válida. TestRecusaRegistroSemPapelLogaODetalheQueNaoVaiAoCliente
	// prova o outro lado: o detalhe existe, só que SÓ no log.
	if corpo.Error.Message != "erro interno do servidor" {
		t.Errorf("mensagem = %q, quer o texto genérico (o detalhe vai só para o log)", corpo.Error.Message)
	}
	if strings.Contains(corpo.Error.Message, "/rota-esquecida") || strings.Contains(corpo.Error.Message, "registra") {
		t.Errorf("o corpo HTTP vazou detalhe interno: %q", corpo.Error.Message)
	}
}

// O outro lado do retoque acima: o corpo ao cliente é genérico, mas o
// detalhe — a rota e a dica de correção — não pode desaparecer. Ele existe,
// só que SÓ no log estruturado, que é quem o operador lê.
func TestRecusaRegistroSemPapelLogaODetalheQueNaoVaiAoCliente(t *testing.T) {
	var buf bytes.Buffer
	s := servidorDeTeste(logging.New(&buf, logging.FormatJSON, slog.LevelInfo))
	rt := roteadorDeTeste()
	rt.mux.HandleFunc("/rota-esquecida", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := s.comAutenticacao(rt)

	req := httptest.NewRequest(http.MethodGet, "/rota-esquecida", nil)
	req.SetBasicAuth("chefe", senhaChefeDeTeste)
	h.ServeHTTP(httptest.NewRecorder(), req)

	saida := buf.String()
	if !strings.Contains(saida, "/rota-esquecida") {
		t.Errorf("o log não nomeia a rota sem papel: %s", saida)
	}
	if !strings.Contains(saida, "roteador.registra") {
		t.Errorf("o log não traz a dica de correção (roteador.registra): %s", saida)
	}
	if !strings.Contains(saida, `"level":"ERROR"`) {
		t.Errorf("o evento não saiu em nível Error: %s", saida)
	}
}

// Minor da revisão: username é CASE-SENSITIVE — "Leitor" não pode casar com o
// usuário configurado "leitor". Trava a escolha atual (comparação exata de
// mapa Go) contra regressão futura.
func TestUsernameEhCaseSensitive(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
	req.SetBasicAuth("Leitor", senhaLeitorDeTeste) // usuário configurado é "leitor", minúsculo
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: username é case-sensitive, \"Leitor\" != \"leitor\"", rec.Code)
	}
}

// ADMIN ⊇ READ é uma regra pura, travada em tabela — a fonte de verdade que os
// testes de HTTP abaixo só confirmam ponta a ponta.
func TestPapelAutorizadoAdminAlcancaRead(t *testing.T) {
	casos := []struct {
		usuario, exigido string
		quer             bool
	}{
		{config.RoleRead, config.RoleRead, true},
		{config.RoleRead, config.RoleAdmin, false},
		{config.RoleAdmin, config.RoleRead, true},
		{config.RoleAdmin, config.RoleAdmin, true},
	}
	for _, c := range casos {
		if got := papelAutorizado(c.usuario, c.exigido); got != c.quer {
			t.Errorf("papelAutorizado(%s, %s) = %v, quer %v", c.usuario, c.exigido, got, c.quer)
		}
	}
}

// O mecanismo "rota exige papel X" pronto e testado com um handler de teste,
// como o brief da B3 pede para a B5 herdar — inclusive a ordem: autenticação
// SEMPRE roda antes da decisão de papel, mesmo para a rota ADMIN-only.
func TestMecanismoDeRotaExigePapelComHandlerDeTeste(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	chama := func(caminho, usuario, senha string, comCredencial bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, caminho, nil)
		if comCredencial {
			req.SetBasicAuth(usuario, senha)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("READ chega na rota READ", func(t *testing.T) {
		rec := chama("/rota-read", "leitor", senhaLeitorDeTeste, true)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, corpo = %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("READ nao chega na rota ADMIN", func(t *testing.T) {
		rec := chama("/rota-admin", "leitor", senhaLeitorDeTeste, true)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, quer 403", rec.Code)
		}
		var corpo protocol.ErrorBody
		if err := json.NewDecoder(rec.Body).Decode(&corpo); err != nil {
			t.Fatal(err)
		}
		if corpo.Error.Code != protocol.CodeForbidden {
			t.Fatalf("code = %q, quer %q", corpo.Error.Code, protocol.CodeForbidden)
		}
	})

	t.Run("ADMIN chega na rota ADMIN", func(t *testing.T) {
		rec := chama("/rota-admin", "chefe", senhaChefeDeTeste, true)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, corpo = %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("ADMIN tambem chega na rota READ (ADMIN contem READ)", func(t *testing.T) {
		rec := chama("/rota-read", "chefe", senhaChefeDeTeste, true)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
	})

	t.Run("sem credencial nao chega em rota nenhuma, nem a ADMIN-only", func(t *testing.T) {
		rec := chama("/rota-admin", "", "", false)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, quer 401: autenticação vem antes de qualquer decisão de papel", rec.Code)
		}
	})
}

// O núcleo do §77: "usuário não existe" e "senha errada" têm de ser
// INDISTINGUÍVEIS pela resposta HTTP — mesmo status, mesmo corpo, byte a
// byte, mesmos headers que o servidor controla.
func TestAutenticacao401EhByteIdenticoEntreUsuarioInexistenteESenhaErrada(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	gravaResposta := func(usuario, senha string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
		req.SetBasicAuth(usuario, senha)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	senhaErrada := gravaResposta("leitor", "senha-completamente-errada")
	usuarioInexistente := gravaResposta("usuario-que-nao-existe-em-lugar-nenhum", "qualquer-coisa")

	if senhaErrada.Code != http.StatusUnauthorized {
		t.Fatalf("senha errada: status = %d, quer 401", senhaErrada.Code)
	}
	if usuarioInexistente.Code != http.StatusUnauthorized {
		t.Fatalf("usuário inexistente: status = %d, quer 401", usuarioInexistente.Code)
	}
	if !bytes.Equal(senhaErrada.Body.Bytes(), usuarioInexistente.Body.Bytes()) {
		t.Fatalf("corpos divergem — um atacante distinguiria usuário existente de inexistente pelo corpo:\n"+
			"senha errada:        %q\nusuário inexistente: %q", senhaErrada.Body.String(), usuarioInexistente.Body.String())
	}
	// O CONJUNTO completo de headers, não só os dois óbvios (WWW-Authenticate,
	// Content-Type): qualquer header a mais em um dos dois caminhos seria
	// outro canal de distinção que o §77 pede fechado. httptest.ResponseRecorder
	// não acrescenta Date sozinho (diferente de um http.Server de verdade),
	// então a comparação pode ser exata sem falso negativo por relógio.
	if !reflect.DeepEqual(senhaErrada.Header(), usuarioInexistente.Header()) {
		t.Fatalf("headers divergem entre os dois casos:\nsenha errada:        %v\nusuário inexistente: %v",
			senhaErrada.Header(), usuarioInexistente.Header())
	}
}

// Os cinco formatos adversariais do brief: nenhum deles pode expor um
// caminho de código diferente do de credencial ausente — r.BasicAuth já
// devolve ok=false para todos, e o teste trava que o middleware trata os
// cinco da mesma forma (401, sem tentar resolver um username sequer).
func TestCredencialMalformadaEmTodasAsFormasRecusaComRazaoHeader(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	casos := map[string]func(*http.Request){
		"sem header Authorization": func(_ *http.Request) {},
		"Basic sem payload":        func(r *http.Request) { r.Header.Set("Authorization", "Basic") },
		"Basic com payload vazio":  func(r *http.Request) { r.Header.Set("Authorization", "Basic ") },
		"base64 invalido":          func(r *http.Request) { r.Header.Set("Authorization", "Basic %%%nao-e-base64%%%") },
		"sem dois pontos apos decodificar": func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("semdoispontosnenhum")))
		},
	}
	for nome, ajusta := range casos {
		t.Run(nome, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
			ajusta(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, quer 401", rec.Code)
			}
		})
	}
}

// "usuario:" — payload decodificado tem o ':' mas a senha é vazia. Regressão
// visada: um bug de comparação com string vazia não pode autenticar contra a
// senha de verdade de um usuário configurado.
func TestSenhaVaziaComUsuarioConfiguradoEhRecusada(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
	req.SetBasicAuth("leitor", "") // decodifica para "leitor:"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, quer 401: senha vazia não pode casar com a senha de verdade", rec.Code)
	}
}

// Round-trip do outro teste adversarial: r.BasicAuth corta no PRIMEIRO ':' do
// payload decodificado, então uma senha que CONTÉM ':' tem de sobreviver
// inteira.
func TestSenhaContendoDoisPontosAutenticaCorretamente(t *testing.T) {
	const senhaComDoisPontos = "segredo:com:dois:pontos"
	s := &Servidor{
		log: logging.Discard(),
		credenciais: map[string]credencial{
			"leitor": {digestSenha: sha256.Sum256([]byte(senhaComDoisPontos)), role: config.RoleRead},
		},
		authFalhas: registraMetricasDeAuth(metrics.NewRegistry()),
	}
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
	req.SetBasicAuth("leitor", senhaComDoisPontos)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, corpo = %s: a senha com ':' deveria autenticar", rec.Code, rec.Body.String())
	}
}

// O enum fechado do label `reason`: as três razões, e SÓ elas, aparecem no
// scrape depois de uma falha de cada tipo.
func TestRazaoDeFalhaFechaOEnumHeaderUsuarioSenha(t *testing.T) {
	reg := metrics.NewRegistry()
	s := &Servidor{
		log: logging.Discard(),
		credenciais: map[string]credencial{
			"leitor": {digestSenha: sha256.Sum256([]byte(senhaLeitorDeTeste)), role: config.RoleRead},
		},
		authFalhas: registraMetricasDeAuth(reg),
	}
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	dispara := func(ajusta func(*http.Request)) {
		req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
		ajusta(req)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	dispara(func(_ *http.Request) {})                                                         // razaoHeader
	dispara(func(r *http.Request) { r.SetBasicAuth("nao-existe", "qualquer") })               // razaoUsuario
	dispara(func(r *http.Request) { r.SetBasicAuth("leitor", "senha-completamente-errada") }) // razaoSenha

	var sb strings.Builder
	if _, err := reg.WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	saida := sb.String()
	for _, razao := range []string{razaoHeader, razaoUsuario, razaoSenha} {
		linha := fmt.Sprintf(`nishidb_auth_failures_total{reason=%q} 1`, razao)
		if !strings.Contains(saida, linha) {
			t.Errorf("scrape não tem %q:\n%s", linha, saida)
		}
	}
}

// O `user` do log só casa com um usuário CONFIGURADO; senão é "desconhecido"
// — e o valor plantado no campo de SENHA nunca aparece em log nenhum, em
// nenhum dos três caminhos de recusa.
func TestLogAuthRecusadaSoNomeiaUsuarioConfiguradoENuncaASenha(t *testing.T) {
	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	s := &Servidor{
		log: log,
		credenciais: map[string]credencial{
			"leitor": {digestSenha: sha256.Sum256([]byte(senhaLeitorDeTeste)), role: config.RoleRead},
		},
		authFalhas: registraMetricasDeAuth(metrics.NewRegistry()),
	}
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	const senhaPlantada = "SENHA-SUPER-SECRETA-PLANTADA-9F3A"

	t.Run("usuario inexistente loga desconhecido", func(t *testing.T) {
		buf.Reset()
		req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
		req.SetBasicAuth("usuario-que-nao-existe", senhaPlantada)
		h.ServeHTTP(httptest.NewRecorder(), req)

		saida := buf.String()
		if !strings.Contains(saida, `"user":"desconhecido"`) {
			t.Errorf("log não tem user=desconhecido: %s", saida)
		}
		if strings.Contains(saida, senhaPlantada) {
			t.Fatalf("a senha plantada vazou para o log: %s", saida)
		}
		if strings.Contains(saida, "usuario-que-nao-existe") {
			t.Errorf("o username não-configurado vazou para o log em vez de 'desconhecido': %s", saida)
		}
	})

	t.Run("senha errada de usuario configurado loga o username, nunca a senha", func(t *testing.T) {
		buf.Reset()
		req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
		req.SetBasicAuth("leitor", senhaPlantada)
		h.ServeHTTP(httptest.NewRecorder(), req)

		saida := buf.String()
		if !strings.Contains(saida, `"user":"leitor"`) {
			t.Errorf("log não tem user=leitor: %s", saida)
		}
		if strings.Contains(saida, senhaPlantada) {
			t.Fatalf("a senha plantada vazou para o log: %s", saida)
		}
	})

	t.Run("header ausente loga desconhecido", func(t *testing.T) {
		buf.Reset()
		req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
		h.ServeHTTP(httptest.NewRecorder(), req)

		saida := buf.String()
		if !strings.Contains(saida, `"user":"desconhecido"`) {
			t.Errorf("log não tem user=desconhecido: %s", saida)
		}
	})
}

// O atraso fixo do §77: mensurável sem flakiness porque não afirmamos um
// valor exato (o agendador do SO não garante isso), só um PISO seguro bem
// abaixo de 100ms — inalcançável se o sleep não tivesse rodado — e uma
// comparação relativa para o caminho de sucesso, que não paga o atraso.
func TestAtrasoFixoDeAproximadamente100msEmTodaFalhaDeAutenticacao(t *testing.T) {
	s := servidorDeTeste(nil)
	rt := roteadorDeTeste()
	h := s.comAutenticacao(rt)

	const pisoSeguro = 80 * time.Millisecond

	medeFalha := func(name string, ajusta func(*http.Request)) time.Duration {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
		ajusta(req)
		rec := httptest.NewRecorder()
		inicio := time.Now()
		h.ServeHTTP(rec, req)
		decorrido := time.Since(inicio)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: status = %d, quer 401 para medir o atraso de falha", name, rec.Code)
		}
		return decorrido
	}

	casos := []struct {
		name   string
		ajusta func(*http.Request)
	}{
		{"header ausente", func(_ *http.Request) {}},
		{"senha errada", func(r *http.Request) { r.SetBasicAuth("leitor", "senha-completamente-errada") }},
		{"usuario inexistente", func(r *http.Request) { r.SetBasicAuth("nao-existe", "qualquer-coisa") }},
	}
	for _, c := range casos {
		if d := medeFalha(c.name, c.ajusta); d < pisoSeguro {
			t.Errorf("%s respondeu em %s, quer >= %s: o atraso tem de valer para TODA falha, não só senha errada",
				c.name, d, pisoSeguro)
		}
	}

	// O caminho de SUCESSO não paga o atraso — comparação contra o mesmo piso
	// usado acima, não um teto próprio, para não introduzir um segundo número
	// mágico.
	req := httptest.NewRequest(http.MethodGet, "/rota-read", nil)
	req.SetBasicAuth("leitor", senhaLeitorDeTeste)
	rec := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(rec, req)
	sucesso := time.Since(start)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, credencial válida deveria autenticar", rec.Code)
	}
	if sucesso >= pisoSeguro {
		t.Errorf("autenticação bem-sucedida levou %s — o atraso de falha não pode se aplicar a quem acertou a credencial", sucesso)
	}
}
