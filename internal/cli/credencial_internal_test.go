package cli

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Testes de caixa branca das regras de precedência de fonte de senha
// (flagsRemotas.resolve) e da montagem do client HTTP compartilhado
// (clienteRemoto/aplicaCredencial) — os dois mecanismos que credencial_test.go
// (package cli_test) exercita ponta a ponta contra um servidor de verdade.
// Aqui o oráculo é a REGRA em si, sem precisar subir nada.

func flagsParaTeste(t *testing.T, args ...string) *flagsRemotas {
	t.Helper()
	fs := flag.NewFlagSet("teste", flag.ContinueOnError)
	f := registraFlagsRemotas(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFlagsRemotasResolveRegrasDePrecedencia(t *testing.T) {
	arq := filepath.Join(t.TempDir(), "senha.txt")
	if err := os.WriteFile(arq, []byte("abc\r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	arqVazio := filepath.Join(t.TempDir(), "vazia.txt")
	if err := os.WriteFile(arqVazio, []byte("\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	casos := []struct {
		nome        string
		args        []string
		env         string // "" = não seta a variável
		querErro    bool
		querUsuario string
		querSenha   string
		querTLSCA   string
	}{
		{
			nome: "nenhuma flag nem env: sem credencial, sem erro",
		},
		{
			nome:     "user sem nenhuma fonte de senha: erro",
			args:     []string{"--user", "chefe"},
			querErro: true,
		},
		{
			nome:     "password-file sem user: erro",
			args:     []string{"--password-file", arq},
			querErro: true,
		},
		{
			nome:     "env sem user: erro",
			env:      "abc",
			querErro: true,
		},
		{
			nome:        "user + env: ok",
			args:        []string{"--user", "chefe"},
			env:         "abc",
			querUsuario: "chefe",
			querSenha:   "abc",
		},
		{
			nome:        "user + password-file: ok, apara \\r\\n",
			args:        []string{"--user", "chefe", "--password-file", arq},
			querUsuario: "chefe",
			querSenha:   "abc",
		},
		{
			nome:     "user + as duas fontes ao mesmo tempo: erro",
			args:     []string{"--user", "chefe", "--password-file", arq},
			env:      "abc",
			querErro: true,
		},
		{
			nome:     "user + password-file vazio (só quebra de linha): erro",
			args:     []string{"--user", "chefe", "--password-file", arqVazio},
			querErro: true,
		},
		{
			nome:      "--tls-ca sozinho, sem user: ok, tlsCA preenchido",
			args:      []string{"--tls-ca", "/algum/caminho.pem"},
			querTLSCA: "/algum/caminho.pem",
		},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			if c.env != "" {
				t.Setenv(envSenha, c.env)
			}
			f := flagsParaTeste(t, c.args...)
			cred, err := f.resolve()
			if c.querErro {
				if err == nil {
					t.Fatalf("queria erro, saiu nil (cred=%+v)", cred)
				}
				return
			}
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if cred.usuario != c.querUsuario || cred.senha != c.querSenha || cred.tlsCA != c.querTLSCA {
				t.Fatalf("cred = %+v, queria usuario=%q senha=%q tlsCA=%q",
					cred, c.querUsuario, c.querSenha, c.querTLSCA)
			}
		})
	}
}

// A mensagem de cada recusa nunca ecoa o VALOR de senha nenhuma — só nomeia
// a flag/variável. Complementa TestSenhaNuncaApareceNaMensagemDeDuasFontesAoMesmoTempo
// (package cli_test), que confere o mesmo através do binário inteiro.
func TestFlagsRemotasResolveMensagensNaoEcoamSenha(t *testing.T) {
	arq := filepath.Join(t.TempDir(), "senha.txt")
	segredo := "senha-secreta-de-teste-nao-pode-vazar"
	if err := os.WriteFile(arq, []byte(segredo), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envSenha, segredo)

	f := flagsParaTeste(t, "--user", "chefe", "--password-file", arq)
	_, err := f.resolve()
	if err == nil {
		t.Fatal("queria erro (as duas fontes presentes)")
	}
	if got := err.Error(); strings.Contains(got, segredo) {
		t.Fatalf("a mensagem de erro ecoou a senha: %q", got)
	}
}

// certPEMDeTeste gera um certificado autoassinado mínimo, só para exercitar
// clienteRemoto lendo e parseando um PEM de verdade — o handshake TLS de
// ponta a ponta (SAN, validade) é responsabilidade do e2e em credencial_test.go
// (package cli_test, que não pode enxergar este arquivo _internal_test.go).
func certPEMDeTeste(t *testing.T) string {
	t.Helper()
	chave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	modelo := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nishidb-teste-cli-interno"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &chave.PublicKey, chave)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// A prova de que a classe de bug do proxy corporativo (docs/piloto-events.md:142)
// morreu é ESTRUTURAL, e não um e2e com proxy fake: um e2e contra os
// servidores de teste deste pacote (todos em 127.0.0.1) não consegue
// discriminar "o código está certo" de "o Go nunca proxiaria isso de
// qualquer jeito" — httpproxy.(*config).useProxy (vendorizado em
// net/http/transport.go) exclui QUALQUER endereço loopback do proxy do
// ambiente, incondicionalmente, antes mesmo de olhar Transport.Proxy.
// Confirmado na prática em credencial_test.go (cli_test): com esta mesma
// linha (transport.Proxy = nil) comentada, um e2e com proxy fake continuou
// verde. Este teste É a prova que resta — leitura direta do campo.
func TestClienteRemotoTransportTemProxyNil(t *testing.T) {
	client, err := clienteRemoto(credencialRemota{})
	if err != nil {
		t.Fatal(err)
	}
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport não é *http.Transport: %T", client.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("Proxy deveria ser nil — a classe de bug do proxy corporativo (docs/piloto-events.md:142)")
	}
}

// Revisão da B6, Important 1: clienteRemoto tem de instalar CheckRedirect —
// sem ele, o *http.Client usa a política default do stdlib, que segue
// redirect same-host reenviando Authorization. A prova ponta a ponta (302
// de verdade, credencial nunca chega ao alvo) mora em credencial_test.go;
// aqui é só a fiação.
func TestClienteRemotoInstalaCheckRedirect(t *testing.T) {
	client, err := clienteRemoto(credencialRemota{})
	if err != nil {
		t.Fatal(err)
	}
	if client.CheckRedirect == nil {
		t.Fatal("CheckRedirect não foi instalado — o cliente seguiria redirect pela política default do stdlib")
	}
}

func TestRecusaRedirectNomeiaOStatusEODestino(t *testing.T) {
	alvo, err := url.Parse("http://10.0.0.9:9142/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	req := &http.Request{
		URL:      alvo,
		Response: &http.Response{StatusCode: http.StatusFound},
	}
	err = recusaRedirect(req, nil)
	if err == nil {
		t.Fatal("queria erro: redirect tem de ser recusado sempre")
	}
	msg := err.Error()
	if !strings.Contains(msg, "302") {
		t.Errorf("a mensagem não nomeia o status: %q", msg)
	}
	if !strings.Contains(msg, "10.0.0.9:9142") {
		t.Errorf("a mensagem não nomeia o destino: %q", msg)
	}
	if !strings.Contains(msg, "não redireciona") {
		t.Errorf("a mensagem não afirma a postura: %q", msg)
	}
}

// req.Response pode ser nil em tese (a função não deve confiar cegamente);
// a recusa continua valendo, só sem o número do status.
func TestRecusaRedirectSemResponseAindaAssimRecusa(t *testing.T) {
	alvo, err := url.Parse("http://10.0.0.9:9142/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	req := &http.Request{URL: alvo}
	if err := recusaRedirect(req, nil); err == nil {
		t.Fatal("queria erro mesmo sem req.Response")
	}
}

func TestClienteRemotoComTLSCAMontaCertPoolEMinVersion(t *testing.T) {
	caPath := certPEMDeTeste(t)
	client, err := clienteRemoto(credencialRemota{tlsCA: caPath})
	if err != nil {
		t.Fatal(err)
	}
	tr := client.Transport.(*http.Transport)
	if tr.Proxy != nil {
		t.Fatal("Proxy deveria continuar nil mesmo com --tls-ca")
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.RootCAs == nil {
		t.Fatal("RootCAs não foi montado a partir de --tls-ca")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %d, quer TLS12 (%d)", tr.TLSClientConfig.MinVersion, tls.VersionTLS12)
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify nunca pode ser true — doutrina do plano (PROIBIDO --insecure)")
	}
}

func TestClienteRemotoComTLSCAInexistenteDevolveErro(t *testing.T) {
	_, err := clienteRemoto(credencialRemota{tlsCA: filepath.Join(t.TempDir(), "nao-existe.pem")})
	if err == nil {
		t.Fatal("queria erro para --tls-ca apontando para um arquivo inexistente")
	}
}

func TestClienteRemotoComTLSCAInvalidaDevolveErro(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalido.pem")
	if err := os.WriteFile(path, []byte("isto não é um certificado PEM"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := clienteRemoto(credencialRemota{tlsCA: path})
	if err == nil {
		t.Fatal("queria erro para --tls-ca apontando para um PEM inválido")
	}
}

func TestAplicaCredencialSoDefineBasicComUsuario(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://exemplo.invalido/", nil)
	if err != nil {
		t.Fatal(err)
	}
	aplicaCredencial(req, credencialRemota{})
	if _, _, ok := req.BasicAuth(); ok {
		t.Fatal("sem usuário, não deveria haver Authorization nenhum")
	}

	req2, err := http.NewRequest(http.MethodGet, "http://exemplo.invalido/", nil)
	if err != nil {
		t.Fatal(err)
	}
	aplicaCredencial(req2, credencialRemota{usuario: "chefe", senha: "s"})
	u, p, ok := req2.BasicAuth()
	if !ok || u != "chefe" || p != "s" {
		t.Fatalf("BasicAuth = %q/%q/%v, queria chefe/s/true", u, p, ok)
	}
}

func TestDicaDeEsquemaSoDisparaNoCorpoEStatusExatos(t *testing.T) {
	casos := []struct {
		status   int
		corpo    string
		querDica bool
	}{
		{http.StatusBadRequest, "Client sent an HTTP request to an HTTPS server.\n", true},
		{http.StatusBadRequest, `{"error":{"code":"BAD_REQUEST","message":"x"}}`, false},
		{http.StatusInternalServerError, "Client sent an HTTP request to an HTTPS server.\n", false},
		{http.StatusBadRequest, "", false},
	}
	for _, c := range casos {
		got := dicaDeEsquema(c.status, []byte(c.corpo))
		if (got != "") != c.querDica {
			t.Errorf("status=%d corpo=%q: dica=%q, queria presente=%v", c.status, c.corpo, got, c.querDica)
		}
	}
}
