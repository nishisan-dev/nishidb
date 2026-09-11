package cli_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Task B6 — credencial e TLS nos comandos remotos, ponta a ponta pelo
// binário do CLI. O oráculo aqui é "o operador consegue falar com um
// servidor de verdade com TLS+auth ligados" — o comportamento de FUNDO das
// duas coisas (HTTP Basic, papéis, o handshake) já está travado nas suítes
// de internal/server (Tasks B3/B4); esta suíte confere que o CLI monta a
// credencial e o transporte certos, na hora certa.
//
// generateCertCLIOfTest duplica generateCertTLSWithValidade
// (internal/server/tls_test.go) — não dá para importar um helper de outro
// pacote de teste (_test.go não gera símbolo exportável entre pacotes), e o
// próprio internal/server já duplica esse gerador entre tls_test.go e
// security_test.go pela mesma razão.

const (
	varReaderCLI = "NISHIDB_TESTE_CLI_LEITOR"
	varChefeCLI  = "NISHIDB_TESTE_CLI_CHEFE"

	passwordReaderCLI = "senha-leitor-cli-bem-forte"
	passwordChefeCLI  = "senha-chefe-cli-bem-forte"
)

// configWithTLSEAuth combina server.tls e server.authentication no MESMO
// bloco — bind em 127.0.0.1 (loopback, então evaluateExposicaoEAuth
// sobe de qualquer jeito), com um usuário READ e um ADMIN, no molde de
// blockAuth (internal/server/auth_test.go).
const configWithTLSEAuth = `version: 1
storage:
  path: %s
datasets:
  directory: %s
server:
  bind: 127.0.0.1
  port: %d
  tls:
    enabled: true
    certificateFile: %s
    privateKeyFile: %s
  authentication:
    enabled: true
    users:
      - username: leitor
        passwordEnv: ` + varReaderCLI + `
        role: READ
      - username: chefe
        passwordEnv: ` + varChefeCLI + `
        role: ADMIN
`

func generateCertCLIOfTest(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	modelo := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "nishidb-teste-cli-tls"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")

	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	if err := certOut.Close(); err != nil {
		t.Fatal(err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatal(err)
	}
	if err := keyOut.Close(); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// newEnvRemoteWithTLSEAuth monta um ambiente remoto com TLS e
// autenticação de verdade — devolve também o caminho do certificado, que os
// testes usam como --tls-ca.
func newEnvRemoteWithTLSEAuth(t *testing.T) (a *envRemote, certPath string) {
	t.Helper()
	amb := newEnv(t)
	porta := portaLivre(t)
	certPath, keyPath := generateCertCLIOfTest(t)
	t.Setenv(varReaderCLI, passwordReaderCLI)
	t.Setenv(varChefeCLI, passwordChefeCLI)
	corpo := fmt.Sprintf(configWithTLSEAuth,
		filepath.Join(amb.dir, "dados"), filepath.Join(amb.dir, "datasets"), porta, certPath, keyPath)
	if err := os.WriteFile(amb.config, []byte(corpo), 0o644); err != nil {
		t.Fatal(err)
	}
	return &envRemote{env: amb, porta: porta}, certPath
}

// ---------------------------------------------------------------- caminho feliz

// O caminho feliz completo: query streaming via HTTPS, credencial por
// NISHIDB_PASSWORD, validando o certificado do servidor por --tls-ca — as
// TRÊS pontas do B6 (credencial, TLS, sem --insecure) numa consulta real.
func TestQueryRemotaComTLSEAuthFuncionaDePontaAPonta(t *testing.T) {
	a, certPath := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 20))

	t.Setenv("NISHIDB_PASSWORD", passwordReaderCLI)
	code := a.run("query", "--server", "https://"+a.endereco(), "--dataset", "radius_sessions",
		"--user", "leitor", "--tls-ca", certPath,
		"SELECT id FROM radius_sessions LIMIT 3")
	if code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSpace(a.stdout.String()), "\n")
	if len(rows) != 3 {
		t.Fatalf("linhas = %d, quer 3:\n%s", len(rows), a.stdout.String())
	}
	for _, l := range rows {
		if !strings.HasPrefix(l, `{"id":"`) {
			t.Fatalf("linha inesperada no stdout: %s", l)
		}
	}
}

// status/datasets/inspect também falam TLS+auth pelo MESMO ponto de costura
// (searchDocument) — um representante basta para confirmar que a costura
// vale para os três, já que a suíte de operation_test.go trava o resto do
// contrato deles sem credencial.
func TestStatusRemotoComTLSEAuthFuncionaComPasswordFile(t *testing.T) {
	a, certPath := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))

	arq := filepath.Join(a.dir, "senha-leitor.txt")
	if err := os.WriteFile(arq, []byte(passwordReaderCLI+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code := a.run("status", "--server", "https://"+a.endereco(), "--tls-ca", certPath,
		"--user", "leitor", "--password-file", arq)
	if code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "radius_sessions") {
		t.Errorf("a saída não nomeia o dataset:\n%s", a.stdout.String())
	}
	saida := a.stdout.String() + a.stderr.String()
	if strings.Contains(saida, passwordReaderCLI) {
		t.Fatalf("a senha (lida de --password-file) vazou na saída:\n%s", saida)
	}
}

// ADMIN via compact — o laço automático de compaction (default: ligado)
// pode estar em voo, exatamente como TestCompactRemotoNaConfigDefaultEventualmenteSai0
// (mutation_test.go, Task B5) já documenta: tentativas imediatas até 200,
// sem sleep de corrida.
func TestCompactRemotoComUsuarioAdminFuncionaDePontaAPonta(t *testing.T) {
	a, certPath := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))

	t.Setenv("NISHIDB_PASSWORD", passwordChefeCLI)
	const tentativas = 200
	var code int
	for i := 0; i < tentativas; i++ {
		code = a.run("compact", "--server", "https://"+a.endereco(), "--tls-ca", certPath, "--user", "chefe")
		if code == 0 {
			break
		}
	}
	if code != 0 {
		t.Fatalf("exit = %d após %d tentativas imediatas: %s", code, tentativas, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "radius_sessions") {
		t.Errorf("a saída não nomeia o dataset:\n%s", a.stdout.String())
	}
}

// ---------------------------------------------------------------- recusas

func TestQueryRemotaComTLSSemCredencialSaiComExit1EMensagemDoServidor(t *testing.T) {
	a, certPath := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))

	code := a.run("query", "--server", "https://"+a.endereco(), "--dataset", "radius_sessions",
		"--tls-ca", certPath, "SELECT id FROM radius_sessions")
	if code != 1 {
		t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
	}
	if a.stdout.Len() != 0 {
		t.Fatalf("nenhuma linha deveria sair sem credencial: %q", a.stdout.String())
	}
}

// compact exige ADMIN; um usuário READ válido é AUTENTICADO mas não
// AUTORIZADO — 403, exit 1, mensagem nomeando o papel (internal/server/
// auth.go:rejectAuthorization).
func TestCompactRemotoComUsuarioReadSaiComExit1EMencionaOPapel(t *testing.T) {
	a, certPath := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))

	t.Setenv("NISHIDB_PASSWORD", passwordReaderCLI)
	code := a.run("compact", "--server", "https://"+a.endereco(), "--tls-ca", certPath, "--user", "leitor")
	if code != 1 {
		t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "papel") {
		t.Fatalf("a mensagem não nomeia o papel insuficiente:\n%s", a.stderr.String())
	}
}

// ---------------------------------------------------------------- dica de esquema

// A porta fala TLS; o operador esqueceu o https:// e normalizeAddress
// assume http:// por default — o Go entrega "Client sent an HTTP request to
// an HTTPS server." cru, e a mensagem do comando precisa virar uma dica
// acionável em vez do genérico "sem corpo legível".
func TestDicaDeEsquemaQuandoAPortaFalaTLSMasOClienteUsaHTTP(t *testing.T) {
	a, _ := newEnvRemoteWithTLSEAuth(t)
	risesWithEntry(t, a, corpus(t, 5))

	code := a.run("status", "--server", a.endereco()) // sem https://
	if code != 1 {
		t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "https://") {
		t.Fatalf("a mensagem não traz a dica do esquema:\n%s", a.stderr.String())
	}
}

// ---------------------------------------------------------------- redirect

// Revisão da B6, Important 1: o cliente compartilhado fala com UM servidor
// que o operador nomeou na linha de comando — esse servidor não redireciona,
// e a política DEFAULT do stdlib seguiria um 3xx same-host reenviando
// Authorization nele (só remove em cross-host). remoteClient instala
// refuseRedirect para eliminar a classe inteira; este teste prova a versão
// observável pela rede, com DOIS servidores de mentira: "origem" (onde o
// --server aponta, sempre responde 302) e "alvo" (o destino do Location),
// que precisa NUNCA ser alcançado.
func TestComandoRemotoNaoSegueRedirectENuncaReenviaCredencial(t *testing.T) {
	var chamadasTarget int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&chamadasTarget, 1)
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("o alvo do redirect recebeu Authorization (%q) — não deveria nem ter sido alcançado", auth)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/v1/stats", http.StatusFound)
	}))
	defer source.Close()

	a := newEnv(t)
	t.Setenv("NISHIDB_PASSWORD", "irrelevante-o-servidor-de-mentira-nao-valida-nada")
	code := a.run("status", "--server", source.URL, "--user", "alguem")
	if code != 1 {
		t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "302") {
		t.Fatalf("a mensagem não nomeia o status do redirect:\n%s", a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "não redireciona") {
		t.Fatalf("a mensagem não afirma a postura de recusa:\n%s", a.stderr.String())
	}
	if n := atomic.LoadInt32(&chamadasTarget); n != 0 {
		t.Fatalf("o alvo do redirect foi alcançado %d vez(es): o cliente seguiu o redirect em vez de recusar", n)
	}
}

// ---------------------------------------------------------------- proxy
//
// A prova estrutural — TestClienteRemotoTransportTemProxyNil, em
// credential_internal_test.go — confirma que o Transport do client
// compartilhado nasce com Proxy: nil, e uma mutação manual (comentar a
// linha) confirmou que o teste CAPTURA a regressão.
//
// Um e2e "sobe um proxy fake e prova que não foi consultado" (a versão que o
// brief da task oferece como "melhor se couber") foi tentado e descartado
// deliberadamente: o próprio net/http do Go já EXCLUI qualquer endereço
// loopback do proxy do ambiente, incondicionalmente — ver
// httpproxy.(*config).useProxy (golang.org/x/net/http/httpproxy/proxy.go),
// que devolve "sem proxy" para 127.0.0.1/::1/localhost ANTES mesmo de olhar
// Transport.Proxy. Como todo servidor de teste deste pacote sobe em
// 127.0.0.1, um e2e contra ele não consegue DISTINGUIR "o código está
// certo" de "o Go nunca proxiaria isso de qualquer jeito" — confirmado na
// prática: com transport.Proxy = nil comentado (a mutação que deveria
// quebrar o teste), a chamada ao proxy fake continuou em zero, e o teste
// permaneceu verde. Reproduzir o bug de verdade exigiria um bind NÃO
// loopback de um endereço roteável de verdade — o mesmo desenho do piloto
// (10.200.20.218) — o que não é portável entre máquina de desenvolvedor e
// CI (interfaces de rede variam, e alguns sandboxes não têm nenhuma além do
// loopback). A prova estrutural fecha a lacuna sem essa fragilidade.

// ---------------------------------------------------------------- senha não vaza

// Asserção ESTRUTURAL do §95: não existe flag de senha em NENHUM comando
// remoto. --password não está registrada em nenhum FlagSet, então o parser
// da stdlib recusa com "flag provided but not defined" antes de qualquer
// requisição de rede.
func TestNenhumComandoRemotoAceitaFlagDePassword(t *testing.T) {
	a := newEnvRemote(t, 10)
	casos := [][]string{
		{"query", "--server", a.endereco(), "--dataset", "radius_sessions", "--password", "x", "SELECT 1"},
		{"status", "--server", a.endereco(), "--password", "x"},
		{"datasets", "--server", a.endereco(), "--password", "x"},
		{"inspect", "--server", a.endereco(), "--password", "x", "radius_sessions"},
		{"compact", "--server", a.endereco(), "--password", "x"},
		{"checkpoint", "--server", a.endereco(), "--password", "x"},
	}
	for _, args := range casos {
		if code := a.run(args...); code != 1 {
			t.Errorf("%v: exit = %d, quer 1 (--password não pode ser uma flag aceita)", args, code)
		}
		if !strings.Contains(a.stderr.String(), "password") {
			t.Errorf("%v: a mensagem não nomeia a flag desconhecida:\n%s", args, a.stderr.String())
		}
	}
}

// A senha nunca aparece na SAÍDA do comando, nem quando o erro é justamente
// "as duas fontes presentes ao mesmo tempo" — o caso mais fácil de vazar por
// engano, porque as duas já foram lidas antes de a validação recusar.
func TestSenhaNuncaApareceNaMensagemDeDuasFontesAoMesmoTempo(t *testing.T) {
	a := newEnvRemote(t, 10)
	segredo := "segredo-nao-pode-vazar-8k2j91"
	arq := filepath.Join(a.dir, "senha.txt")
	if err := os.WriteFile(arq, []byte(segredo+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NISHIDB_PASSWORD", segredo)

	code := a.run("status", "--server", a.endereco(), "--user", "chefe", "--password-file", arq)
	if code != 1 {
		t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
	}
	saida := a.stdout.String() + a.stderr.String()
	if strings.Contains(saida, segredo) {
		t.Fatalf("a senha vazou na mensagem de erro:\n%s", saida)
	}
}

// ---------------------------------------------------------------- precedência de fontes

func TestRegrasDePrecedenciaDeFontesDeSenha(t *testing.T) {
	a := newEnvRemote(t, 10)
	arq := filepath.Join(a.dir, "senha.txt")
	if err := os.WriteFile(arq, []byte("abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	casos := []struct {
		name    string
		args    []string
		withEnv bool
		querIn  string
	}{
		{"user sem fonte de senha", []string{"status", "--server", a.endereco(), "--user", "chefe"}, false, "--user"},
		{"password-file sem user", []string{"status", "--server", a.endereco(), "--password-file", arq}, false, "--user"},
		{"env sem user", []string{"status", "--server", a.endereco()}, true, "--user"},
		{"duas fontes com user", []string{"status", "--server", a.endereco(), "--user", "chefe", "--password-file", arq}, true, "exclus"},
	}
	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			if c.withEnv {
				t.Setenv("NISHIDB_PASSWORD", "abc")
			}
			code := a.run(c.args...)
			if code != 1 {
				t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
			}
			if !strings.Contains(a.stderr.String(), c.querIn) {
				t.Fatalf("mensagem não contém %q:\n%s", c.querIn, a.stderr.String())
			}
		})
	}
}

// --tls-ca apontando para um arquivo que não existe é erro de uso claro,
// nomeando a flag — não um "servidor inalcançável" genérico, que mandaria o
// operador investigar a rede em vez do próprio caminho que digitou.
func TestTLSCAInexistenteFalhaComMensagemClara(t *testing.T) {
	a := newEnvRemote(t, 10)
	code := a.run("status", "--server", a.endereco(), "--tls-ca", filepath.Join(a.dir, "nao-existe.pem"))
	if code != 1 {
		t.Fatalf("exit = %d, quer 1: %s", code, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "tls-ca") {
		t.Fatalf("a mensagem não nomeia --tls-ca:\n%s", a.stderr.String())
	}
}

// ---------------------------------------------------------------- --help

func TestOUsoAnunciaCredencialETLS(t *testing.T) {
	a := newEnv(t)
	if code := a.run("--help"); code != 0 {
		t.Fatalf("--help saiu com %d", code)
	}
	for _, trecho := range []string{"--user", "--password-file", "NISHIDB_PASSWORD", "--tls-ca"} {
		if !strings.Contains(a.stdout.String(), trecho) {
			t.Errorf("o texto de uso não anuncia %q", trecho)
		}
	}
}
