// Command certgen escreve um par certificado/chave autoassinado para o teste
// de integração do driver JDBC.
//
// Existe para que o `scripts/jdbc-it.sh` não dependa de openssl nem de
// certificado versionado. Certificado commitado é um segredo que envelhece no
// repositório — e mesmo sem chave privada junto, ele acaba sendo reaproveitado
// fora do teste. Aqui o par nasce e morre com a execução: o script o cria num
// diretório temporário e o apaga no trap.
//
// O molde é o mesmo de internal/server/tls_test.go: EC P-256, uso de servidor,
// SAN para `localhost` E para `127.0.0.1`. Os dois nomes, e não um: o harness
// sobe em 127.0.0.1 e a URL de conexão pode nomear qualquer um dos dois.
//
// Uso:
//
//	go run ./scripts/certgen -cert <arquivo> -key <arquivo> [-validade 2h]
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

func main() {
	cert := flag.String("cert", "", "caminho do certificado a escrever (PEM)")
	chave := flag.String("key", "", "caminho da chave privada a escrever (PEM, 0600)")
	validade := flag.Duration("validade", 2*time.Hour, "por quanto tempo o certificado vale")
	flag.Parse()

	if *cert == "" || *chave == "" {
		fmt.Fprintln(os.Stderr, "certgen: -cert e -key são obrigatórios")
		os.Exit(2)
	}
	if err := gera(*cert, *chave, *validade); err != nil {
		fmt.Fprintln(os.Stderr, "certgen:", err)
		os.Exit(1)
	}
}

// gera escreve o par e devolve erro em vez de sair, para que o teste o exercite
// sem subprocesso.
func gera(caminhoCert, caminhoChave string, validade time.Duration) error {
	chave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("gerando a chave: %w", err)
	}

	agora := time.Now()
	modelo := &x509.Certificate{
		SerialNumber: big.NewInt(agora.UnixNano()),
		Subject:      pkix.Name{CommonName: "nishidb-jdbc-it"},
		// NotBefore no passado absorve relógio dessincronizado entre quem
		// gera e quem valida: sem a folga, o handshake falha com
		// "certificate is not yet valid" de forma intermitente, que é o pior
		// tipo de falha de teste.
		NotBefore:             agora.Add(-time.Hour),
		NotAfter:              agora.Add(validade),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		// Autoassinado E usado como CA pelo driver (o `tlsCaFile` aponta para
		// este mesmo arquivo), então ele precisa ser CA — sem isto o
		// TrustManager do JDK o recusa como âncora de confiança.
		IsCA:        true,
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &chave.PublicKey, chave)
	if err != nil {
		return fmt.Errorf("criando o certificado: %w", err)
	}

	if err := escrevePEM(caminhoCert, 0o644, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		return err
	}

	bytesChave, err := x509.MarshalECPrivateKey(chave)
	if err != nil {
		return fmt.Errorf("serializando a chave: %w", err)
	}
	return escrevePEM(caminhoChave, 0o600, &pem.Block{Type: "EC PRIVATE KEY", Bytes: bytesChave})
}

func escrevePEM(caminho string, modo os.FileMode, bloco *pem.Block) error {
	arquivo, err := os.OpenFile(caminho, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, modo)
	if err != nil {
		return fmt.Errorf("abrindo %s: %w", caminho, err)
	}
	if err := pem.Encode(arquivo, bloco); err != nil {
		_ = arquivo.Close()
		return fmt.Errorf("escrevendo %s: %w", caminho, err)
	}
	return arquivo.Close()
}
