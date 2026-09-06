package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// Paridade (decisão 4 do plano do ciclo): com server.loopback.enabled
// ausente (default false), NADA sobe além do listener principal. Provar
// "porta fechada" e não só "EnderecoLoopback() vazio" — a garantia real que
// o operador sente é a conexão recusada, não um método Go.
func TestLoopbackDesligadoNaoAbrePorta(t *testing.T) {
	loopbackPorta := portaLivre(t)
	srv := servidorComDados(t, 1, blocoPadrao)

	if srv.EnderecoLoopback() != "" {
		t.Fatalf("EnderecoLoopback() = %q, quer vazio com server.loopback desligado", srv.EnderecoLoopback())
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", loopbackPorta))
	if err == nil {
		_ = conn.Close()
		t.Fatalf("127.0.0.1:%d deveria recusar conexão com o plano loopback desligado", loopbackPorta)
	}
}

// A lacuna que a B1 deixou nomeada para a ativação decidir: habilitar o
// loopback sem declarar a porta não tem endereço determinístico para
// bindar. Aqui pelo caminho de subida REAL (server.Novo com YAML de
// verdade), não só a função pura (enderecoLoopback, security_internal_test.go).
func TestLoopbackHabilitadoSemPortaRecusaASubidaDoServidor(t *testing.T) {
	a := novoAmbienteCom(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n  loopback:\n    enabled: true\n")
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
	if err == nil {
		t.Fatal("quer recusa: server.loopback.enabled: true sem server.loopback.port declarado")
	}
	if !strings.Contains(err.Error(), "server.loopback.port") {
		t.Errorf("a mensagem não nomeia server.loopback.port: %v", err)
	}
}

// O flagship do plano loopback (decisão 4): com autenticação (e TLS) ligados
// no principal, o loopback responde SEM credencial — paridade de
// comportamento com o servidor de hoje — e o principal continua exigindo
// credencial. Os DOIS simultâneos, no MESMO processo, mesma rota
// (/v1/health), provando que é a MESMA definição servida por dois caminhos.
func TestLoopbackRespondeSemCredencialComPrincipalAutenticadoSimultaneamente(t *testing.T) {
	certPath, keyPath := geraCertificadoTLSComValidade(t, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	const variavel = "NISHIDB_TESTE_LOOPBACK_SIMULTANEO_LEITOR"
	const senha = "senha-bem-forte-de-teste"
	t.Setenv(variavel, senha)
	loopbackPorta := portaLivre(t)

	bloco := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
		"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n"+
		"  authentication:\n    enabled: true\n    users:\n"+
		"      - username: leitor\n        passwordEnv: %s\n        role: READ\n"+
		"  loopback:\n    enabled: true\n    port: %d\n",
		certPath, keyPath, variavel, loopbackPorta)

	srv := servidorComDados(t, 1, bloco)
	client := clienteTLSDeTeste(t, certPath)

	t.Run("principal sem credencial exige autenticacao", func(t *testing.T) {
		resp, err := client.Get("https://" + srv.Endereco() + "/v1/health")
		if err != nil {
			t.Fatalf("GET https: %v", err)
		}
		defer fecha(t, resp)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, quer 401 sem credencial", resp.StatusCode)
		}
	})

	t.Run("principal com credencial responde", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "https://"+srv.Endereco()+"/v1/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth("leitor", senha)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET https com credencial: %v", err)
		}
		defer fecha(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, quer 200 com credencial válida", resp.StatusCode)
		}
	})

	t.Run("loopback sem credencial e sem TLS responde", func(t *testing.T) {
		if srv.EnderecoLoopback() == "" {
			t.Fatal("EnderecoLoopback() vazio com server.loopback.enabled: true")
		}
		// Client com Timeout, e não http.Get cru: se uma regressão futura
		// deixar o listener ACEITO mas sem ninguém servindo (Novo bindou,
		// Roda não subiu o http.Server), a conexão fica aberta esperando
		// resposta que nunca chega — sem teto aqui, o teste travaria a
		// suíte inteira em vez de falhar rápido e nomeado.
		clienteComTeto := &http.Client{Timeout: 5 * time.Second}
		resp, err := clienteComTeto.Get("http://" + srv.EnderecoLoopback() + "/v1/health")
		if err != nil {
			t.Fatalf("GET http (sem TLS, sem credencial) no loopback: %v", err)
		}
		defer fecha(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, quer 200: o plano loopback preserva o acesso anônimo local", resp.StatusCode)
		}
	})
}

// TestLoopbackRecusaRotaAdminMasRotaReadContinuaAnonima é o RULING do §77b
// (plano do ciclo) posto à prova, ponta a ponta pelo listener de verdade: o
// loopback é READ-only por construção. Nem o servidor tem autenticação
// configurada aqui — a recusa é sobre o LISTENER que respondeu, não sobre uma
// credencial que faltou, e é por isso que POST /v1/compact sai 403 (papel
// insuficiente) em vez de 401 (sem credencial não existe conceito nenhum
// neste listener).
func TestLoopbackRecusaRotaAdminMasRotaReadContinuaAnonima(t *testing.T) {
	loopbackPorta := portaLivre(t)
	bloco := blocoPadrao + fmt.Sprintf("  loopback:\n    enabled: true\n    port: %d\n", loopbackPorta)
	srv := servidorComDados(t, 1, bloco)

	if srv.EnderecoLoopback() == "" {
		t.Fatal("EnderecoLoopback() vazio com server.loopback.enabled: true")
	}

	t.Run("compact via loopback eh 403", func(t *testing.T) {
		resp, err := http.Post("http://"+srv.EnderecoLoopback()+"/v1/compact", "", nil)
		if err != nil {
			t.Fatalf("POST /v1/compact no loopback: %v", err)
		}
		defer fecha(t, resp)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, quer 403: rota ADMIN não pode responder anônima no loopback", resp.StatusCode)
		}
		var corpo struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&corpo); err != nil {
			t.Fatalf("corpo de erro ilegível: %v", err)
		}
		if corpo.Error.Code != "FORBIDDEN" {
			t.Errorf("código = %q, quer FORBIDDEN", corpo.Error.Code)
		}
	})

	t.Run("stats via loopback continua 200 sem credencial", func(t *testing.T) {
		resp, err := http.Get("http://" + srv.EnderecoLoopback() + "/v1/stats")
		if err != nil {
			t.Fatalf("GET /v1/stats no loopback: %v", err)
		}
		defer fecha(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, quer 200: rota READ continua anônima no loopback (comportamento intocado)", resp.StatusCode)
		}
	})
}

// Ciclo de vida conjunto: os DOIS listeners sobem com o principal e caem
// juntos no shutdown gracioso — nenhum accept loop sobrevive ao Roda
// retornar. Roda sob -race de propósito (dois Serve concorrentes com o
// Shutdown que os encerra).
func TestShutdownFechaListenerPrincipalELoopbackSemVazarGoroutine(t *testing.T) {
	loopbackPorta := portaLivre(t)
	bloco := blocoPadrao + fmt.Sprintf("  loopback:\n    enabled: true\n    port: %d\n", loopbackPorta)

	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}

	ctx, cancela := context.WithCancel(context.Background())
	fim := make(chan error, 1)
	go func() { fim <- srv.Roda(ctx, "") }()

	enderecoPrincipal := srv.Endereco()
	enderecoLoopback := srv.EnderecoLoopback()
	if enderecoLoopback == "" {
		t.Fatal("EnderecoLoopback() vazio com server.loopback.enabled: true")
	}
	esperaListenerAceitar(t, enderecoPrincipal)
	esperaListenerAceitar(t, enderecoLoopback)

	cancela()
	if err := <-fim; err != nil {
		t.Fatalf("Roda: %v", err)
	}

	if conn, err := net.Dial("tcp", enderecoPrincipal); err == nil {
		_ = conn.Close()
		t.Error("o listener principal ainda aceita conexão depois do shutdown gracioso")
	}
	if conn, err := net.Dial("tcp", enderecoLoopback); err == nil {
		_ = conn.Close()
		t.Error("o listener loopback ainda aceita conexão depois do shutdown gracioso")
	}
}

// esperaListenerAceitar espera por FATO ("já aceita conexão"), não por
// sleep: os dois Serve sobem em goroutines separadas depois de Roda
// retornar o controle ao chamador, e um sleep fixo trocaria um teste
// determinístico por uma aposta de agendador.
func esperaListenerAceitar(t *testing.T, endereco string) {
	t.Helper()
	prazo := time.Now().Add(5 * time.Second)
	for time.Now().Before(prazo) {
		conn, err := net.Dial("tcp", endereco)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s nunca aceitou conexão", endereco)
}

// O loopback serve o MESMO mux, então a uniformização do 405 tem de valer
// nele também.
//
// Não é redundante com o teste do plano exposto: enquanto o método vivia no
// padrão de registro, POST /metrics dava padrão VAZIO no mux, o que faz
// papelDaRota devolver existe=false — e o wrapper do loopback devolvia a
// requisição ao mux, que respondia o 405 de texto puro. Depois da mudança o
// padrão casa, papelDaRota devolve READ, e a recusa passa a vir do handler. Os
// dois caminhos são diferentes por dentro e precisam sair iguais por fora.
func TestMetricsComMetodoErradoNoPlanoLoopbackTambemSaiComCorpoErro(t *testing.T) {
	loopbackPorta := portaLivre(t)
	bloco := blocoPadrao + fmt.Sprintf("  loopback:\n    enabled: true\n    port: %d\n", loopbackPorta)
	srv := servidorComDados(t, 1, bloco)

	clienteComTeto := &http.Client{Timeout: 5 * time.Second}
	for _, caminho := range []string{"/metrics", "/v1/health"} {
		resp, err := clienteComTeto.Post("http://"+srv.EnderecoLoopback()+caminho,
			"application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("POST %s no loopback: %v", caminho, err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			fecha(t, resp)
			t.Fatalf("POST %s no loopback = %d, quer 405", caminho, resp.StatusCode)
		}
		var corpo protocol.CorpoErro
		if err := json.NewDecoder(resp.Body).Decode(&corpo); err != nil {
			fecha(t, resp)
			t.Fatalf("POST %s no loopback: corpo não é CorpoErro (o 405 de texto puro do mux?): %v", caminho, err)
		}
		fecha(t, resp)
		if corpo.Error.Code != protocol.CodMetodo {
			t.Errorf("POST %s no loopback: código = %q, quer %q", caminho, corpo.Error.Code, protocol.CodMetodo)
		}
	}
}
