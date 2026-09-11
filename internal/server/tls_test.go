package server_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// blockWithTLS monta server.tls com o par de arquivos dado — %d fica para a
// porta, no mesmo molde de blockPadrao (query_test.go).
func blockWithTLS(certPath, keyPath string) string {
	return fmt.Sprintf("\nserver:\n  bind: 127.0.0.1\n  port: %%d\n"+
		"  tls:\n    enabled: true\n    certificateFile: %s\n    privateKeyFile: %s\n", certPath, keyPath)
}

// generateCertTLSWithValidade gera um par certificado/chave autoassinado
// (ECDSA P-256), em ARQUIVO, com SAN IPAddress 127.0.0.1 — necessário para um
// handshake TLS DE VERDADE contra "https://127.0.0.1:porta" (o cliente Go
// verifica o SAN contra o host da URL). Diferente de generateCertOfTest
// (security_test.go): aquele só precisa decodificar como PEM para exercitar
// tls.LoadX509KeyPair na subida (B2/B3 nunca abrem handshake real); os
// testes aqui abrem.
//
// validade permite simular certificado saudável, expirando em breve ou já
// expirado — os três ramos que avisaValidadeOfCert (security.go)
// decide.
func generateCertTLSWithValidade(t *testing.T, notBefore, notAfter time.Time) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	modelo := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "nishidb-teste-tls"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
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

// clienteTLSOfTest monta um *http.Client que confia SOMENTE na CA gerada
// pelo teste — NUNCA InsecureSkipVerify: true. É a mesma doutrina que o
// plano da Fase 11 exige de qualquer cliente do PRODUTO (B6, "proibido
// --insecure/InsecureSkipVerify em qualquer cliente"): um x509.CertPool com
// o PEM do certificado (aqui autoassinado, então o próprio cert é a CA), e a
// verificação de cadeia acontece de verdade. A B6 (helper de CLI/painel real
// com --tls-ca) segue o MESMO padrão — RootCAs a partir do PEM apontado
// pela flag, nunca pular a verificação.
func clienteTLSOfTest(t *testing.T, certPath string) *http.Client {
	t.Helper()
	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("PEM do certificado de teste inválido")
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}
}

// O caminho feliz do ServeTLS: streaming NDJSON COMPLETO atravessando um
// handshake TLS de verdade, com o cliente validando a cadeia contra a CA de
// teste (nunca InsecureSkipVerify) — a mesma moldura de
// TestQueryRespondeAMolduraCompleta (query_test.go), só que por HTTPS.
func TestServeTLSStreamingNDJSONCompleto(t *testing.T) {
	certPath, keyPath := generateCertTLSWithValidade(t, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	srv := serverWithData(t, 4, blockWithTLS(certPath, keyPath))

	client := clienteTLSOfTest(t, certPath)
	req, err := http.NewRequest(http.MethodPost, "https://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, ip FROM d"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST https: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	rows, l := result(t, resp)
	if len(rows) != 4 {
		t.Fatalf("rows = %d, quer 4", len(rows))
	}
	if l.End() == nil || l.End().Rows != 4 || l.End().Truncated {
		t.Fatalf("fecho = %+v", l.End())
	}
}

// MinVersion TLS12 (kafka/config.go:108 é o precedente citado pelo brief):
// um cliente que só oferece TLS 1.0 ou 1.1 tem o handshake recusado antes de
// qualquer byte de aplicação trafegar.
func TestServeTLSRecusaVersaoAbaixoDeTLS12(t *testing.T) {
	certPath, keyPath := generateCertTLSWithValidade(t, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	srv := serverWithData(t, 1, blockWithTLS(certPath, keyPath))

	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("PEM inválido")
	}

	for _, versao := range []uint16{tls.VersionTLS10, tls.VersionTLS11} {
		name := map[uint16]string{tls.VersionTLS10: "TLS 1.0", tls.VersionTLS11: "TLS 1.1"}[versao]
		t.Run(name, func(t *testing.T) {
			conn, err := tls.Dial("tcp", srv.Endereco(), &tls.Config{
				RootCAs:    pool,
				MinVersion: versao,
				MaxVersion: versao,
			})
			if err == nil {
				_ = conn.Close()
				t.Fatalf("handshake em %s deveria ser recusado (MinVersion do servidor é TLS 1.2)", name)
			}
		})
	}
}

// ErrorLog do http.Server principal roteado para o logger estruturado (o
// brief): um handshake malformado/recusado (aqui, TLS 1.0 contra um servidor
// MinVersion 1.2 — o mesmo cenário de TestServeTLSRecusaVersaoAbaixoDeTLS12)
// é exatamente o ruído de scanner corporativo que o retoque descreve. Sem o
// ErrorLog customizado, isso iria para log.Printf cru no journal (stderr),
// nunca para o buffer estruturado que este teste inspeciona.
func TestErrorLogDoServidorPrincipalVaiParaOLoggerEstruturado(t *testing.T) {
	certPath, keyPath := generateCertTLSWithValidade(t, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))

	a := newEnvWith(t, blockWithTLS(certPath, keyPath))
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	record := &recordOfServer{}
	srv, err := server.Novo(a.load(), server.Options{
		Log: logging.New(record, logging.FormatJSON, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	pemBytes, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("PEM inválido")
	}

	// O handshake recusado é assíncrono do lado do servidor (a goroutine de
	// conexão escreve o ErrorLog depois de o Dial já ter retornado o erro do
	// LADO DO CLIENTE) — por isso a espera por CONTEÚDO, não por sleep fixo.
	prazo := time.Now().Add(5 * time.Second)
	for time.Now().Before(prazo) {
		conn, dialErr := tls.Dial("tcp", srv.Endereco(), &tls.Config{
			RootCAs: pool, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS10,
		})
		if dialErr == nil {
			_ = conn.Close()
			t.Fatal("handshake TLS 1.0 deveria ser recusado")
		}
		if strings.Contains(record.String(), "http.erro_transporte") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	saida := record.String()
	if !strings.Contains(saida, "http.erro_transporte") {
		t.Fatalf("o erro de handshake não foi roteado para o logger estruturado: %s", saida)
	}
	if !strings.Contains(saida, `"listener":"principal"`) {
		t.Errorf("o evento não nomeia o listener de origem: %s", saida)
	}
}

// HTTP em claro na porta TLS: o net/http do Go reconhece o verbo HTTP no
// início do que deveria ser um ClientHello e devolve, ele MESMO, um
// "400 Bad Request" de texto cru ANTES de qualquer handshake completar —
// dica de "Client sent an HTTP request to an HTTPS server." (é o mesmo
// mecanismo que o plano do ciclo cita para o CLI sugerir https://). A prova
// de que "nenhuma credencial em claro é processada" (o brief) não é a
// AUSÊNCIA de resposta — é que esta resposta nunca passa por
// withAuth/roteamento: se a requisição chegasse a ser roteada de
// verdade, /v1/health responderia 200 com Content-Type application/json (o
// handler de sempre) — o que este teste prova que NÃO acontece: a resposta
// real não tem Content-Type nenhum e carrega o texto cru do net/http.
func TestHTTPClaroNaPortaTLSNaoAtravessaAutenticacaoNemRoteamento(t *testing.T) {
	certPath, keyPath := generateCertTLSWithValidade(t, time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour))
	srv := serverWithData(t, 1, blockWithTLS(certPath, keyPath))

	resp, err := http.Get("http://" + srv.Endereco() + "/v1/health")
	if err != nil {
		// Também é uma forma válida do handshake falhar (payload que o Go
		// não reconhece como HTTP): a conexão só cai, sem resposta nenhuma —
		// ainda mais forte que o 400 cru abaixo.
		return
	}
	defer fecha(t, resp)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, quer 400 cru do net/http (nunca a rota real, que exigiria autenticação)", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		t.Errorf("Content-Type = %q: a resposta deveria ser TEXTO CRU do net/http, nunca o JSON do protocolo "+
			"(prova de que não passou por withAuth/roteamento)", ct)
	}
	corpo, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(corpo), "HTTP request to an HTTPS server") {
		t.Errorf("corpo = %q, quer a mensagem padrão do net/http sobre HTTP em claro na porta TLS", corpo)
	}
}

// Integração de ponta a ponta do aviso de validade (unidade em
// security_internal_test.go): um certificado JÁ EXPIRADO sobe pelo caminho
// de subida REAL (server.Novo, com LoadX509KeyPair de verdade) sem recusar,
// e o log de subida carrega o evento de ERRO — nunca a subida falha por
// causa da validade.
func TestSubidaComCertificadoExpiradoNaoRecusaELogaErro(t *testing.T) {
	certPath, keyPath := generateCertTLSWithValidade(t, time.Now().Add(-48*time.Hour), time.Now().Add(-time.Hour))

	a := newEnvWith(t, blockWithTLS(certPath, keyPath))
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	record := &recordOfServer{}
	srv, err := server.Novo(a.load(), server.Options{
		Log: logging.New(record, logging.FormatJSON, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("certificado expirado NÃO pode recusar a subida: %v", err)
	}
	run(t, srv)

	saida := record.String()
	if !strings.Contains(saida, "tls.certificado_expirado") {
		t.Fatalf("a subida com certificado expirado não logou o evento de erro: %s", saida)
	}
	if !strings.Contains(saida, `"level":"ERROR"`) {
		t.Fatalf("o evento de certificado expirado não saiu em nível Error: %s", saida)
	}
}
