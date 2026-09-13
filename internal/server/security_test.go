package server_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/users"
)

// blocoExposto* montam server.tls/server.authentication no molde de
// newEnvWith (server_test.go): um único %d para a porta, injetado pelo
// harness.

// A célula central da matriz do §77: bind exposto, autenticação LIGADA,
// TLS DESLIGADO. HTTP Basic sem TLS transmitiria a senha em texto claro pela
// rede aberta — o servidor não pode subir assim, e a mensagem tem de nomear
// as três chaves que decidiram a recusa.
func TestAuthSemTLSComBindExpostoRecusaASubida(t *testing.T) {
	block := "\nserver:\n  bind: 0.0.0.0\n  port: %d\n" +
		"  authentication:\n    enabled: true\n"
	a := newEnvWith(t, block)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	_, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		t.Fatal("quer recusa: bind exposto, authentication ligada, tls desligado")
	}
	for _, key := range []string{"server.bind", "server.authentication.enabled", "server.tls.enabled"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("a mensagem não nomeia %s: %v", key, err)
		}
	}
}

// Defesa contra typo: a seção authentication nunca apareceu no arquivo. O
// KnownFields GLOBAL continua desligado (doutrina da B1) — sem esta recusa,
// um `authentification:` (typo) desapareceria em silêncio e o servidor
// subiria exposto e sem credencial nenhuma, sem que ninguém tivesse decidido
// isso de propósito.
func TestBindExpostoSemSecaoDeAuthenticationRecusaASubida(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 0.0.0.0\n  port: %d\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	_, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		t.Fatal("quer recusa: bind exposto sem a seção server.authentication no arquivo")
	}
	if !strings.Contains(err.Error(), "server.authentication") {
		t.Errorf("a mensagem não nomeia server.authentication: %v", err)
	}
	if !strings.Contains(err.Error(), "server.bind") {
		t.Errorf("a mensagem não nomeia server.bind: %v", err)
	}
}

// O outro lado da mesma defesa: quando o operador DECLAROU a seção com
// `enabled: false`, a decisão já foi tomada e vale — sobe com o aviso de
// sempre. TestBindForaDoLoopbackAvisa (server_test.go) cobre exatamente esta
// célula desde que a B2 passou a exigir a declaração explícita.

// Fora do loopback com autenticação ligada e SEM TLS o risco é rede aberta;
// dentro do loopback o mesmo descuido ainda merece aviso — HTTP Basic em
// texto claro é texto claro mesmo que só o próprio host possa ler — mas não
// merece recusa: é o caminho do painel local (piloto em produção).
func TestLoopbackComAutenticacaoSemTLSSobeComAviso(t *testing.T) {
	block := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n"
	a := newEnvWith(t, block)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	if !strings.Contains(saida, "auth.sem_tls") {
		t.Fatalf("loopback com autenticação ligada e sem TLS deveria avisar: %s", saida)
	}
}

// A combinação que o §77 pede de fato: bind exposto, autenticação ligada,
// TLS ligado com um par de certificado/chave que CARREGA. Sobe sem recusa e
// sem o aviso de bind exposto — a topologia está correta.
func TestBindExpostoComAutenticacaoETLSSobeSemRecusa(t *testing.T) {
	certPath, keyPath := generateCertOfTest(t)

	block := fmt.Sprintf("\nserver:\n  bind: 0.0.0.0\n  port: %%d\n"+
		"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n"+
		"  authentication:\n    enabled: true\n",
		certPath, keyPath)
	a := newEnvWith(t, block)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("bind exposto com authentication e tls ligados deveria subir: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}

// A célula 1 da matriz por completo: LOOPBACK com autenticação ligada e TLS
// TAMBÉM ligado não tem nada para avisar — "auth.sem_tls" só faz sentido
// quando TLS está desligado, e "bind.exposto" não se aplica a loopback. A
// combinação correta sobe em silêncio, sem nenhum dos dois avisos.
func TestLoopbackComAutenticacaoETLSSobeSemAviso(t *testing.T) {
	certPath, keyPath := generateCertOfTest(t)

	block := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
		"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n"+
		"  authentication:\n    enabled: true\n",
		certPath, keyPath)
	a := newEnvWith(t, block)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	if strings.Contains(saida, "auth.sem_tls") {
		t.Errorf("loopback com TLS ligado não deveria avisar auth.sem_tls: %s", saida)
	}
	if strings.Contains(saida, "bind.exposto") {
		t.Errorf("loopback nunca deveria avisar bind.exposto: %s", saida)
	}
}

// A escolha atual para `authentication: {}` (seção PRESENTE, mas `enabled`
// omitido dentro dela): trata como "declarada" (a seção apareceu) e
// "desligada" (Enabled fica nil, que AuthenticationEnabled() lê como
// false) — a mesma célula de `enabled: false` EXPLÍCITO, não a de seção
// ausente. Este teste existe para TRAVAR essa decisão: se algum dia alguém
// decidir que `authentication: {}` deveria pedir decisão explícita como a
// seção ausente pede, este teste tem de ser atualizado de propósito, e não
// quebrar como efeito colateral de outra mudança.
func TestBindExpostoComAuthenticationSecaoVaziaSobeComAviso(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 0.0.0.0\n  port: %d\n  authentication: {}\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	if !strings.Contains(saida, "bind.exposto") {
		t.Fatalf("authentication: {} deveria ser tratada como declarada+desligada, subindo com aviso: %s", saida)
	}
}

// Violações simultâneas: bind exposto, autenticação ligada, TLS desligado, E
// a tabela de usuários ilegível. A ORDEM documentada em
// evaluateExposicaoEAuth/Novo (matriz antes de segredos, porque a matriz é só
// leitura de config) diz que a recusa da matriz (célula "auth sem TLS") tem
// de sair PRIMEIRO — nomeando as três chaves — sem que a tabela de usuários
// chegue a ser aberta. Este teste pega uma reordenação acidental futura: se
// alguém inverter a ordem, o erro passaria a ser o da tabela corrompida em
// vez das três chaves da matriz.
func TestViolacoesSimultaneasSaemNaOrdemDaMatrizNaoNosSegredos(t *testing.T) {
	block := "\nserver:\n  bind: 0.0.0.0\n  port: %d\n" +
		"  authentication:\n    enabled: true\n"
	a := newEnvWith(t, block)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))
	dir := users.Dir(storagePathOf(a))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, users.StateFileName), []byte("tabela ilegível"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		t.Fatal("quer recusa: bind exposto, auth ligada, tls desligado E tabela de usuários ilegível")
	}
	for _, key := range []string{"server.bind", "server.authentication.enabled", "server.tls.enabled"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("a recusa que sai não é a da matriz, não nomeia %s: %v", key, err)
		}
	}
	if errors.Is(err, users.ErrCorruptUsers) {
		t.Errorf("a recusa veio da tabela de usuários: ela foi aberta antes da matriz, na ordem errada: %v", err)
	}
}

// Tabela de usuários ilegível com a topologia correta recusa a subida — nunca
// sobe sem ninguém capaz de autenticar, e nunca regera a credencial de
// bootstrap por cima de uma tabela estragada.
func TestTabelaDeUsuariosIlegivelRecusaASubidaDoServidor(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n  authentication:\n    enabled: true\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))
	dir := users.Dir(storagePathOf(a))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, users.StateFileName), []byte("tabela ilegível"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := server.Novo(a.load(), server.Options{})
	if !errors.Is(err, users.ErrCorruptUsers) {
		t.Fatalf("erro = %v, quer ErrCorruptUsers", err)
	}
	if _, err := os.Stat(filepath.Join(dir, users.BootstrapPasswordFileName)); !os.IsNotExist(err) {
		t.Errorf("a subida recusada gerou credencial de bootstrap por cima da tabela estragada")
	}
}

// Primeira subida com autenticação ligada: nasce o ADMIN de bootstrap, a senha
// vai para o arquivo 0600 e o log só aponta o caminho.
func TestPrimeiraSubidaComAutenticacaoGeraACredencialDeBootstrap(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n  authentication:\n    enabled: true\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	path := filepath.Join(users.Dir(storagePathOf(a)), users.BootstrapPasswordFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("arquivo de senha de bootstrap: %v", err)
	}
	if !strings.Contains(saida, "users.bootstrap") || !strings.Contains(saida, path) {
		t.Errorf("o log de subida tem de nomear o evento e o caminho: %s", saida)
	}
	if password := strings.TrimSpace(string(raw)); strings.Contains(saida, password) {
		t.Fatalf("a senha de bootstrap vazou no log de subida")
	}
}

// TLS ligado exige que o par cert/key CARREGUE na subida — LoadX509KeyPair,
// não só "os arquivos existem" — para que o erro apareça aqui, nomeando o
// caminho, e não no primeiro handshake com o listener já aberto.
func TestCertificadoOuChaveInvalidosRecusamASubidaNomeandoOCaminho(t *testing.T) {
	t.Run("certificado e chave trocados", func(t *testing.T) {
		certPath, keyPath := generateCertOfTest(t)
		// certificateFile recebe a CHAVE e privateKeyFile recebe o
		// CERTIFICADO: LoadX509KeyPair falha ao tentar interpretar cada
		// arquivo como o tipo de PEM errado.
		block := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
			"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n",
			keyPath, certPath)
		a := newEnvWith(t, block)
		a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

		_, err := server.Novo(a.load(), server.Options{})
		if err == nil {
			t.Fatal("quer recusa: certificado e chave trocados")
		}
		if !strings.Contains(err.Error(), certPath) || !strings.Contains(err.Error(), keyPath) {
			t.Errorf("a mensagem não nomeia os dois caminhos: %v", err)
		}
	})

	t.Run("chave inexistente", func(t *testing.T) {
		certPath, _ := generateCertOfTest(t)
		keyPath := filepath.Join(t.TempDir(), "nao-existe.pem")
		block := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
			"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n",
			certPath, keyPath)
		a := newEnvWith(t, block)
		a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

		_, err := server.Novo(a.load(), server.Options{})
		if err == nil {
			t.Fatal("quer recusa: privateKeyFile inexistente")
		}
		if !strings.Contains(err.Error(), keyPath) {
			t.Errorf("a mensagem não nomeia o caminho da chave: %v", err)
		}
	})
}

// generateCertOfTest gera um par certificado/chave autoassinado (ECDSA
// P-256) só para exercitar tls.LoadX509KeyPair — não precisa ser válido para
// handshake real, só decodificar como PEM do tipo certo (isso é B4).
func generateCertOfTest(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	modelo := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nishidb-teste"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
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
