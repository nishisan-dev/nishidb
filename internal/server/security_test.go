package server_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/server"
)

// blocoExposto* montam server.tls/server.authentication no molde de
// novoAmbienteCom (server_test.go): um único %d para a porta, injetado pelo
// harness.

// A célula central da matriz do §77: bind exposto, autenticação LIGADA,
// TLS DESLIGADO. HTTP Basic sem TLS transmitiria a senha em texto claro pela
// rede aberta — o servidor não pode subir assim, e a mensagem tem de nomear
// as três chaves que decidiram a recusa.
func TestAuthSemTLSComBindExpostoRecusaASubida(t *testing.T) {
	const variavel = "NISHIDB_TESTE_AUTHSEMTLS_ADMIN"
	t.Setenv(variavel, "senha-bem-forte")

	bloco := "\nserver:\n  bind: 0.0.0.0\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: admin\n        passwordEnv: " + variavel + "\n        role: ADMIN\n"
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
	if err == nil {
		t.Fatal("quer recusa: bind exposto, authentication ligada, tls desligado")
	}
	for _, chave := range []string{"server.bind", "server.authentication.enabled", "server.tls.enabled"} {
		if !strings.Contains(err.Error(), chave) {
			t.Errorf("a mensagem não nomeia %s: %v", chave, err)
		}
	}
}

// Defesa contra typo: a seção authentication nunca apareceu no arquivo. O
// KnownFields GLOBAL continua desligado (doutrina da B1) — sem esta recusa,
// um `authentification:` (typo) desapareceria em silêncio e o servidor
// subiria exposto e sem credencial nenhuma, sem que ninguém tivesse decidido
// isso de propósito.
func TestBindExpostoSemSecaoDeAuthenticationRecusaASubida(t *testing.T) {
	a := novoAmbienteCom(t, "\nserver:\n  bind: 0.0.0.0\n  port: %d\n")
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
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
	const variavel = "NISHIDB_TESTE_LOOPBACK_AUTH_SEM_TLS"
	t.Setenv(variavel, "senha-bem-forte")

	bloco := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: op\n        passwordEnv: " + variavel + "\n        role: READ\n"
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	saida := sobeCapturandoLog(t, a)
	if !strings.Contains(saida, "auth.sem_tls") {
		t.Fatalf("loopback com autenticação ligada e sem TLS deveria avisar: %s", saida)
	}
}

// A combinação que o §77 pede de fato: bind exposto, autenticação ligada,
// TLS ligado com um par de certificado/chave que CARREGA. Sobe sem recusa e
// sem o aviso de bind exposto — a topologia está correta.
func TestBindExpostoComAutenticacaoETLSSobeSemRecusa(t *testing.T) {
	certPath, keyPath := geraCertificadoDeTeste(t)
	const variavel = "NISHIDB_TESTE_EXPOSTO_COM_TLS_ADMIN"
	t.Setenv(variavel, "senha-bem-forte")

	bloco := fmt.Sprintf("\nserver:\n  bind: 0.0.0.0\n  port: %%d\n"+
		"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n"+
		"  authentication:\n    enabled: true\n    users:\n"+
		"      - username: admin\n        passwordEnv: %s\n        role: ADMIN\n",
		certPath, keyPath, variavel)
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("bind exposto com authentication e tls ligados deveria subir: %v", err)
	}
	if err := srv.Fecha(); err != nil {
		t.Fatal(err)
	}
}

// A célula 1 da matriz por completo: LOOPBACK com autenticação ligada e TLS
// TAMBÉM ligado não tem nada para avisar — "auth.sem_tls" só faz sentido
// quando TLS está desligado, e "bind.exposto" não se aplica a loopback. A
// combinação correta sobe em silêncio, sem nenhum dos dois avisos.
func TestLoopbackComAutenticacaoETLSSobeSemAviso(t *testing.T) {
	certPath, keyPath := geraCertificadoDeTeste(t)
	const variavel = "NISHIDB_TESTE_LOOPBACK_COM_TLS_ADMIN"
	t.Setenv(variavel, "senha-bem-forte")

	bloco := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
		"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n"+
		"  authentication:\n    enabled: true\n    users:\n"+
		"      - username: admin\n        passwordEnv: %s\n        role: ADMIN\n",
		certPath, keyPath, variavel)
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	saida := sobeCapturandoLog(t, a)
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
	a := novoAmbienteCom(t, "\nserver:\n  bind: 0.0.0.0\n  port: %d\n  authentication: {}\n")
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	saida := sobeCapturandoLog(t, a)
	if !strings.Contains(saida, "bind.exposto") {
		t.Fatalf("authentication: {} deveria ser tratada como declarada+desligada, subindo com aviso: %s", saida)
	}
}

// Violações simultâneas: bind exposto, autenticação ligada, TLS desligado, E
// a variável de senha ausente do ambiente. A ORDEM documentada em
// avaliaExposicaoEAutenticacao/Novo (matriz antes de segredos, porque a
// matriz é só leitura de config) diz que a recusa da matriz (célula "auth
// sem TLS") tem de sair PRIMEIRO — nomeando as três chaves — sem que
// resolveCredenciais chegue a tocar o ambiente. Este teste pega uma
// reordenação acidental futura: se alguém inverter a ordem, o erro passaria
// a nomear a variável de ambiente em vez das três chaves da matriz.
func TestViolacoesSimultaneasSaemNaOrdemDaMatrizNaoNosSegredos(t *testing.T) {
	const variavel = "NISHIDB_TESTE_ORDEM_VIOLACOES_SIMULTANEAS"
	os.Unsetenv(variavel) //nolint:errcheck // ausência é o próprio setup do teste

	bloco := "\nserver:\n  bind: 0.0.0.0\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: admin\n        passwordEnv: " + variavel + "\n        role: ADMIN\n"
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
	if err == nil {
		t.Fatal("quer recusa: bind exposto, auth ligada, tls desligado E env de senha ausente")
	}
	for _, chave := range []string{"server.bind", "server.authentication.enabled", "server.tls.enabled"} {
		if !strings.Contains(err.Error(), chave) {
			t.Errorf("a recusa que sai não é a da matriz, não nomeia %s: %v", chave, err)
		}
	}
	if strings.Contains(err.Error(), variavel) {
		t.Errorf("a recusa nomeou a variável de ambiente: a resolução de segredos rodou antes da "+
			"matriz, na ordem errada: %v", err)
	}
}

// TLS ligado exige que o par cert/key CARREGUE na subida — LoadX509KeyPair,
// não só "os arquivos existem" — para que o erro apareça aqui, nomeando o
// caminho, e não no primeiro handshake com o listener já aberto.
func TestCertificadoOuChaveInvalidosRecusamASubidaNomeandoOCaminho(t *testing.T) {
	t.Run("certificado e chave trocados", func(t *testing.T) {
		certPath, keyPath := geraCertificadoDeTeste(t)
		// certificateFile recebe a CHAVE e privateKeyFile recebe o
		// CERTIFICADO: LoadX509KeyPair falha ao tentar interpretar cada
		// arquivo como o tipo de PEM errado.
		bloco := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
			"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n",
			keyPath, certPath)
		a := novoAmbienteCom(t, bloco)
		a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

		_, err := server.Novo(a.carrega(), server.Opcoes{})
		if err == nil {
			t.Fatal("quer recusa: certificado e chave trocados")
		}
		if !strings.Contains(err.Error(), certPath) || !strings.Contains(err.Error(), keyPath) {
			t.Errorf("a mensagem não nomeia os dois caminhos: %v", err)
		}
	})

	t.Run("chave inexistente", func(t *testing.T) {
		certPath, _ := geraCertificadoDeTeste(t)
		keyPath := filepath.Join(t.TempDir(), "nao-existe.pem")
		bloco := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
			"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n",
			certPath, keyPath)
		a := novoAmbienteCom(t, bloco)
		a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

		_, err := server.Novo(a.carrega(), server.Opcoes{})
		if err == nil {
			t.Fatal("quer recusa: privateKeyFile inexistente")
		}
		if !strings.Contains(err.Error(), keyPath) {
			t.Errorf("a mensagem não nomeia o caminho da chave: %v", err)
		}
	})
}

// Prova de que a resolução de segredos (security.go) está de fato ligada ao
// caminho de subida de server.Novo, e não só correta em isolamento
// (security_internal_test.go já cobre resolveSenha em detalhe).
func TestPasswordEnvAusenteRecusaASubidaDoServidor(t *testing.T) {
	const variavel = "NISHIDB_TESTE_SUBIDA_SENHA_AUSENTE"
	os.Unsetenv(variavel) //nolint:errcheck // ausência é o próprio setup do teste

	bloco := "\nserver:\n  bind: 127.0.0.1\n  port: %d\n" +
		"  authentication:\n    enabled: true\n    users:\n" +
		"      - username: op\n        passwordEnv: " + variavel + "\n        role: READ\n"
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
	if err == nil {
		t.Fatal("quer recusa: passwordEnv não definida no ambiente")
	}
	if !strings.Contains(err.Error(), variavel) {
		t.Errorf("a mensagem não nomeia a variável %s: %v", variavel, err)
	}
}

func TestPasswordFileWorldReadableRecusaASubidaDoServidor(t *testing.T) {
	dir := t.TempDir()
	senhaPath := filepath.Join(dir, "senha.txt")
	if err := os.WriteFile(senhaPath, []byte("segredo\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(senhaPath, 0o644); err != nil {
		t.Fatal(err)
	}

	bloco := fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
		"  authentication:\n    enabled: true\n    users:\n"+
		"      - username: op\n        passwordFile: %s\n        role: READ\n", senhaPath)
	a := novoAmbienteCom(t, bloco)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
	if err == nil {
		t.Fatal("quer recusa: passwordFile legível por group/world")
	}
	if !strings.Contains(err.Error(), senhaPath) {
		t.Errorf("a mensagem não nomeia o caminho: %v", err)
	}
}

// geraCertificadoDeTeste gera um par certificado/chave autoassinado (ECDSA
// P-256) só para exercitar tls.LoadX509KeyPair — não precisa ser válido para
// handshake real, só decodificar como PEM do tipo certo (isso é B4).
func geraCertificadoDeTeste(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	chave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
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
	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &chave.PublicKey, chave)
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

	chaveBytes, err := x509.MarshalECPrivateKey(chave)
	if err != nil {
		t.Fatal(err)
	}
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: chaveBytes}); err != nil {
		t.Fatal(err)
	}
	if err := keyOut.Close(); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}
