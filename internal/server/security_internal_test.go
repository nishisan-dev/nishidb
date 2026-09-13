package server

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// certOfTestWithValidade gera um certificado autoassinado (ECDSA
// P-256) EM MEMÓRIA, com o NotBefore/NotAfter que o teste pedir — é
// exatamente o que avisaValidadeOfCert parseia para decidir entre
// silêncio, WARN e ERRO. Ao contrário de generateCertOfTest
// (security_test.go, que escreve em disco para exercitar
// tls.LoadX509KeyPair via arquivo), este helper devolve o tls.Certificate
// pronto: os testes abaixo chamam avisaValidadeOfCert diretamente,
// sem passar pela subida inteira do servidor.
func certOfTestWithValidade(t *testing.T, notBefore, notAfter time.Time) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	modelo := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "nishidb-teste-validade"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// O ramo central do brief: certificado JÁ expirado loga ERRO (nunca recusa
// a subida — isso é provado à parte, em tls_test.go, com server.New de
// verdade) e publica a gauge de expiração com o NotAfter exato, epoch
// segundos.
func TestAvisaValidadeDoCertificadoJaExpiradoLogaErroEPublicaGauge(t *testing.T) {
	notAfter := time.Now().Add(-time.Hour)
	cert := certOfTestWithValidade(t, time.Now().Add(-48*time.Hour), notAfter)

	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	reg := metrics.NewRegistry()

	if err := avisaValidadeOfCert(cert, log, reg); err != nil {
		t.Fatalf("avisaValidadeOfCert: %v", err)
	}

	saida := buf.String()
	if !strings.Contains(saida, "tls.certificado_expirado") {
		t.Errorf("não logou o evento de certificado expirado: %s", saida)
	}
	if !strings.Contains(saida, `"level":"ERROR"`) {
		t.Errorf("o evento não saiu em nível Error: %s", saida)
	}
	if strings.Contains(saida, "tls.certificado_expirando") {
		t.Errorf("logou também o WARN de 'expirando em breve' para um certificado JÁ expirado: %s", saida)
	}

	var sb strings.Builder
	if _, err := reg.WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	quer := metrics.MetTLSCertExpiryTimestamp + " " + strconv.FormatInt(notAfter.Unix(), 10)
	if !strings.Contains(sb.String(), quer) {
		t.Errorf("scrape não tem %q:\n%s", quer, sb.String())
	}
}

// Certificado vencendo em menos de 30 dias, mas AINDA válido: WARN, nunca
// ERRO, e o NotAfter aparece no evento (o brief pede explicitamente).
func TestAvisaValidadeDoCertificadoExpirandoEmBreveLogaWarnComNotAfter(t *testing.T) {
	notAfter := time.Now().Add(10 * 24 * time.Hour)
	cert := certOfTestWithValidade(t, time.Now().Add(-time.Hour), notAfter)

	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	reg := metrics.NewRegistry()

	if err := avisaValidadeOfCert(cert, log, reg); err != nil {
		t.Fatal(err)
	}

	saida := buf.String()
	if !strings.Contains(saida, "tls.certificado_expirando") {
		t.Errorf("não logou o WARN de expiração próxima: %s", saida)
	}
	if strings.Contains(saida, `"level":"ERROR"`) {
		t.Errorf("certificado ainda válido não pode logar em nível Error: %s", saida)
	}
	if !strings.Contains(saida, notAfter.UTC().Format(time.RFC3339)) {
		t.Errorf("o evento não carrega o NotAfter: %s", saida)
	}
}

// Aviso que aparece sempre é ruído (mesma doutrina de TestBindEmLoopbackNaoAvisa,
// server_test.go): um certificado saudável não pode logar nada, mas a gauge
// segue publicada — o operador deriva "quanto falta" dela em qualquer caso.
func TestAvisaValidadeDoCertificadoSaudavelNaoLogaMasPublicaGauge(t *testing.T) {
	cert := certOfTestWithValidade(t, time.Now().Add(-time.Hour), time.Now().Add(365*24*time.Hour))

	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	reg := metrics.NewRegistry()

	if err := avisaValidadeOfCert(cert, log, reg); err != nil {
		t.Fatal(err)
	}
	if saida := buf.String(); strings.Contains(saida, "tls.certificado_") {
		t.Errorf("certificado saudável não deveria logar aviso nenhum: %s", saida)
	}

	var sb strings.Builder
	if _, err := reg.WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sb.String(), metrics.MetTLSCertExpiryTimestamp) {
		t.Error("a gauge de expiração deveria ser publicada mesmo com certificado saudável")
	}
}

// loopbackAddress desligado (default) não monta endereço nenhum — nenhum
// listener a mais sobe.
func TestEnderecoLoopbackDesligadoDevolveVazio(t *testing.T) {
	var cfg config.Config
	addr, err := loopbackAddress(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if addr != "" {
		t.Errorf("endereco = %q, quer vazio com loopback desligado", addr)
	}
}

// A lacuna que a B1 deixou nomeada para a ativação decidir (task-b1-report.md,
// "Preocupações"): server.loopback.enabled: true SEM port declarado não tem
// endereço determinístico para bindar — recusa, nomeando a chave, na mesma
// doutrina do resto do §77 (nunca um default silencioso escolhendo pelo
// operador).
func TestEnderecoLoopbackLigadoSemPortaRecusa(t *testing.T) {
	var cfg config.Config
	cfg.Server.Loopback.Enabled = true
	_, err := loopbackAddress(&cfg)
	if err == nil {
		t.Fatal("quer recusa: loopback ligado sem porta declarada")
	}
	if !strings.Contains(err.Error(), "server.loopback.port") {
		t.Errorf("a mensagem não nomeia server.loopback.port: %v", err)
	}
}

// Ligado e com porta: o endereço é SEMPRE 127.0.0.1, nunca o bind principal
// — o plano loopback é o ferramental LOCAL, mesmo que server.bind aponte
// para uma interface exposta.
func TestEnderecoLoopbackLigadoComPortaMontaEnderecoNoLoopback(t *testing.T) {
	var cfg config.Config
	cfg.Server.Loopback.Enabled = true
	porta := 9143
	cfg.Server.Loopback.Port = &porta
	addr, err := loopbackAddress(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if addr != "127.0.0.1:9143" {
		t.Errorf("endereco = %q, quer 127.0.0.1:9143", addr)
	}
}
