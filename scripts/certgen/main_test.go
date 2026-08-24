package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// O certificado precisa servir para os DOIS nomes que o teste de integração
// usa, e não para um só.
//
// O harness sobe o servidor em 127.0.0.1 e o driver conecta por uma URL que
// pode nomear qualquer um dos dois. Um certificado só com `localhost` faria a
// conexão por IP falhar na verificação de nome — e o sintoma seria uma
// mensagem de handshake, que manda procurar no lugar errado.
func TestOCertificadoServeParaLocalhostEParaOIP(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	chave := filepath.Join(dir, "key.pem")

	if err := gera(cert, chave, 2*time.Hour); err != nil {
		t.Fatalf("gera: %v", err)
	}

	bruto, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	bloco, _ := pem.Decode(bruto)
	if bloco == nil || bloco.Type != "CERTIFICATE" {
		t.Fatal("o arquivo de certificado não é um PEM de CERTIFICATE")
	}
	c, err := x509.ParseCertificate(bloco.Bytes)
	if err != nil {
		t.Fatalf("parseando o certificado: %v", err)
	}

	temLocalhost := false
	for _, n := range c.DNSNames {
		if n == "localhost" {
			temLocalhost = true
		}
	}
	if !temLocalhost {
		t.Errorf("DNSNames = %v, falta localhost", c.DNSNames)
	}
	temIP := false
	for _, ip := range c.IPAddresses {
		if ip.Equal(net.ParseIP("127.0.0.1")) {
			temIP = true
		}
	}
	if !temIP {
		t.Errorf("IPAddresses = %v, falta 127.0.0.1", c.IPAddresses)
	}
}

// O par cert/chave tem de casar: um par que não casa só falha no handshake,
// longe daqui.
func TestOParDeCertificadoEChaveCasa(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	chave := filepath.Join(dir, "key.pem")
	if err := gera(cert, chave, time.Hour); err != nil {
		t.Fatalf("gera: %v", err)
	}
	if _, err := tls.LoadX509KeyPair(cert, chave); err != nil {
		t.Fatalf("o par não carrega: %v", err)
	}
}

// A chave privada sai com permissão 0600.
//
// O arquivo vive num diretório temporário que o script apaga no fim, mas a
// permissão frouxa é o tipo de detalhe que sobrevive a um copiar-e-colar para
// um script de produção.
func TestAChavePrivadaNaoEhLegivelPorOutros(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	chave := filepath.Join(dir, "key.pem")
	if err := gera(cert, chave, time.Hour); err != nil {
		t.Fatalf("gera: %v", err)
	}
	info, err := os.Stat(chave)
	if err != nil {
		t.Fatal(err)
	}
	if modo := info.Mode().Perm(); modo != 0o600 {
		t.Errorf("permissão da chave = %o, quer 600", modo)
	}
}

// Validade curta é deliberada: o certificado é de UMA execução do teste, e um
// certificado de teste com validade longa é o que acaba esquecido num
// diretório e reutilizado em outro lugar.
func TestAValidadeEhCurtaEComeçaNoPassado(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	if err := gera(cert, filepath.Join(dir, "key.pem"), time.Hour); err != nil {
		t.Fatalf("gera: %v", err)
	}
	bruto, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	bloco, _ := pem.Decode(bruto)
	c, err := x509.ParseCertificate(bloco.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if c.NotAfter.Sub(c.NotBefore) > 25*time.Hour {
		t.Errorf("validade de %v é longa demais para um certificado de teste",
			c.NotAfter.Sub(c.NotBefore))
	}
	// NotBefore no passado absorve relógio dessincronizado entre o processo
	// que gera e o que valida — sem isso o handshake falha com "certificate
	// is not yet valid" de forma intermitente.
	if !c.NotBefore.Before(time.Now()) {
		t.Error("NotBefore não está no passado; o handshake pode falhar por relógio")
	}
}
