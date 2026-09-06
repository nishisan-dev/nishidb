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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// Molde de kafka.env (internal/kafka/config.go:154-169): variável ausente
// recusa nomeando a VARIÁVEL, nunca o valor (que nem existe aqui).
func TestResolveSenhaComPasswordEnvAusenteNomeiaAVariavel(t *testing.T) {
	const variavel = "NISHIDB_TESTE_SENHA_AUSENTE_B2"
	os.Unsetenv(variavel) //nolint:errcheck // ausência é o próprio setup do teste

	u := config.UserSpec{Username: "op", PasswordEnv: variavel, Role: config.RoleRead}
	_, err := resolveSenha(u)
	if err == nil {
		t.Fatal("quer recusa: passwordEnv não definida no ambiente")
	}
	if !strings.Contains(err.Error(), variavel) {
		t.Errorf("a mensagem não nomeia a variável %s: %v", variavel, err)
	}
}

// Variável DEFINIDA e vazia é o modo de falha mais traiçoeiro do deploy por
// container (§95): o orquestrador injeta a chave sem valor e o processo
// segue adiante se ninguém distinguir os dois casos.
func TestResolveSenhaComPasswordEnvVaziaNomeiaAVariavel(t *testing.T) {
	const variavel = "NISHIDB_TESTE_SENHA_VAZIA_B2"
	t.Setenv(variavel, "")

	u := config.UserSpec{Username: "op", PasswordEnv: variavel, Role: config.RoleRead}
	_, err := resolveSenha(u)
	if err == nil {
		t.Fatal("quer recusa: passwordEnv definida e vazia")
	}
	if !strings.Contains(err.Error(), variavel) {
		t.Errorf("a mensagem não nomeia a variável %s: %v", variavel, err)
	}
}

func TestResolveSenhaComPasswordFileInexistenteNomeiaOCaminho(t *testing.T) {
	caminho := filepath.Join(t.TempDir(), "nao-existe.txt")

	u := config.UserSpec{Username: "op", PasswordFile: caminho, Role: config.RoleRead}
	_, err := resolveSenha(u)
	if err == nil {
		t.Fatal("quer recusa: passwordFile inexistente")
	}
	if !strings.Contains(err.Error(), caminho) {
		t.Errorf("a mensagem não nomeia o caminho %s: %v", caminho, err)
	}
}

// Regra NOVA em relação ao molde do kafka.Resolve: lá security.caFile é
// pública; senha não. group/world-readable recusa nomeando caminho E modo.
func TestResolveSenhaComPasswordFileGroupOuWorldReadableRecusaNomeandoCaminhoEModo(t *testing.T) {
	casos := []struct {
		nome string
		modo os.FileMode
	}{
		{"legível pelo group", 0o640},
		{"legível por todos", 0o644},
		{"escrevível pelo group", 0o620},
		{"legível por outros sem group", 0o604},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			dir := t.TempDir()
			caminho := filepath.Join(dir, "senha.txt")
			if err := os.WriteFile(caminho, []byte("segredo\n"), c.modo); err != nil {
				t.Fatal(err)
			}
			// WriteFile aplica umask; força o modo exato que o caso quer
			// exercitar.
			if err := os.Chmod(caminho, c.modo); err != nil {
				t.Fatal(err)
			}

			u := config.UserSpec{Username: "op", PasswordFile: caminho, Role: config.RoleRead}
			_, err := resolveSenha(u)
			if err == nil {
				t.Fatalf("quer recusa: passwordFile com modo %04o", c.modo)
			}
			if !strings.Contains(err.Error(), caminho) {
				t.Errorf("a mensagem não nomeia o caminho: %v", err)
			}
			if !strings.Contains(err.Error(), "0640") && !strings.Contains(err.Error(), "0644") &&
				!strings.Contains(err.Error(), "0620") && !strings.Contains(err.Error(), "0604") {
				t.Errorf("a mensagem não nomeia o modo: %v", err)
			}
		})
	}
}

// chmod 600 continua aceito: é o modo que a mensagem de recusa recomenda.
func TestResolveSenhaComPasswordFileModo600EhAceito(t *testing.T) {
	dir := t.TempDir()
	caminho := filepath.Join(dir, "senha.txt")
	if err := os.WriteFile(caminho, []byte("segredo\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	u := config.UserSpec{Username: "op", PasswordFile: caminho, Role: config.RoleRead}
	if _, err := resolveSenha(u); err != nil {
		t.Fatalf("passwordFile 0600 deveria ser aceito: %v", err)
	}
}

// Todo `echo senha > arquivo` termina em newline (LF ou CRLF). Sem aparar, a
// mesma senha digitada sem passar por echo deixaria de autenticar — a chave
// mentiria sobre qual é a senha. A prova é por DIGEST: mesma senha, com e sem
// quebra de linha, tem de resolver para o mesmo hash.
func TestResolveSenhaAparaTrailingNewlineDoPasswordFile(t *testing.T) {
	dir := t.TempDir()
	semQuebra := filepath.Join(dir, "sem-quebra.txt")
	comLF := filepath.Join(dir, "com-lf.txt")
	comCRLF := filepath.Join(dir, "com-crlf.txt")

	if err := os.WriteFile(semQuebra, []byte("segredo123"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(comLF, []byte("segredo123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(comCRLF, []byte("segredo123\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	base, err := resolveSenha(config.UserSpec{Username: "op", PasswordFile: semQuebra, Role: config.RoleRead})
	if err != nil {
		t.Fatal(err)
	}

	casos := []struct {
		nome    string
		caminho string
	}{
		{"LF", comLF},
		{"CRLF", comCRLF},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			digest, err := resolveSenha(config.UserSpec{Username: "op", PasswordFile: c.caminho, Role: config.RoleRead})
			if err != nil {
				t.Fatal(err)
			}
			if digest != base {
				t.Errorf("digest com quebra de linha final diverge do digest sem quebra: "+
					"a mesma senha 'autenticaria' diferente dependendo de como o operador a escreveu no arquivo (base=%x, %s=%x)",
					base, c.nome, digest)
			}
		})
	}
}

// Um passwordFile vazio ou que só contém quebra de linha apara para "" — e
// sem esta recusa resolveria em silêncio para o digest de string vazia
// (sha256("") = e3b0c442...). Um `touch senha.txt` de script de
// provisionamento criaria, sem ninguém perceber, uma conta que autentica com
// QUALQUER Basic de senha em branco — e o cache já não guarda a informação
// de que o arquivo estava vazio para a B3 desconfiar depois. Mesma doutrina
// da recusa de passwordEnv vazia, aplicada ao vizinho de baixo.
func TestResolveSenhaComPasswordFileVazioOuSoNewlineRecusaNomeandoOCaminho(t *testing.T) {
	casos := []struct {
		nome      string
		conteudo  string
		descricao string
	}{
		{"vazio", "", "arquivo com zero bytes"},
		{"só LF", "\n", "arquivo que só contém uma quebra de linha"},
		{"só CRLF", "\r\n", "arquivo que só contém um CRLF"},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			dir := t.TempDir()
			caminho := filepath.Join(dir, "senha.txt")
			if err := os.WriteFile(caminho, []byte(c.conteudo), 0o600); err != nil {
				t.Fatal(err)
			}

			u := config.UserSpec{Username: "op", PasswordFile: caminho, Role: config.RoleRead}
			_, err := resolveSenha(u)
			if err == nil {
				t.Fatalf("quer recusa: %s não pode resolver para senha vazia em silêncio", c.descricao)
			}
			if !strings.Contains(err.Error(), caminho) {
				t.Errorf("a mensagem não nomeia o caminho: %v", err)
			}
		})
	}
}

// passwordEnv não passa por TrimRight: variável de ambiente não carrega
// newline do jeito que um arquivo escrito por `echo` carrega, e aparar um
// valor que o operador definiu explicitamente (inclusive com espaço/quebra
// de propósito) mudaria a senha sem ele pedir.
func TestResolveSenhaPasswordEnvNaoEhAparada(t *testing.T) {
	const variavel = "NISHIDB_TESTE_SENHA_COM_NEWLINE_B2"
	t.Setenv(variavel, "segredo123\n")

	digestEnv, err := resolveSenha(config.UserSpec{Username: "op", PasswordEnv: variavel, Role: config.RoleRead})
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	caminho := filepath.Join(dir, "com-newline.txt")
	if err := os.WriteFile(caminho, []byte("segredo123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	digestArquivo, err := resolveSenha(config.UserSpec{Username: "op", PasswordFile: caminho, Role: config.RoleRead})
	if err != nil {
		t.Fatal(err)
	}

	// O arquivo com o MESMO newline literal apara para "segredo123"; a env
	// var não apara, então os dois digests têm de DIVERGIR — a prova de que
	// só passwordFile é aparado.
	if digestEnv == digestArquivo {
		t.Error("passwordEnv com newline literal não deveria coincidir com passwordFile aparado: a env var não deveria ser aparada")
	}
}

// resolveCredenciais só faz I/O quando a autenticação está LIGADA: um
// usuário com FORMA válida mas passwordEnv apontando para uma variável
// inexistente não pode travar a subida enquanto authentication.enabled é
// false ou ausente (débito 49, mesma doutrina da B1 aplicada à ativação).
func TestResolveCredenciaisIgnoraUsuariosQuandoAutenticacaoDesligada(t *testing.T) {
	desligado := false
	cfg := &config.Config{}
	cfg.Server.Authentication.Enabled = &desligado
	cfg.Server.Authentication.Users = []config.UserSpec{
		{Username: "op", PasswordEnv: "NISHIDB_TESTE_VARIAVEL_QUE_NAO_EXISTE_B2", Role: config.RoleRead},
	}

	out, err := resolveCredenciais(cfg)
	if err != nil {
		t.Fatalf("com autenticação desligada, resolveCredenciais não deveria tocar o ambiente: %v", err)
	}
	if out != nil {
		t.Errorf("quer nil com autenticação desligada, veio %v", out)
	}
}

func TestResolveCredenciaisMapeiaUsernameParaDigestEPapel(t *testing.T) {
	const variavelAdmin = "NISHIDB_TESTE_SENHA_ADMIN_B2"
	t.Setenv(variavelAdmin, "senha-do-admin")

	dir := t.TempDir()
	arquivoLeitor := filepath.Join(dir, "leitor.txt")
	if err := os.WriteFile(arquivoLeitor, []byte("senha-do-leitor\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ligado := true
	cfg := &config.Config{}
	cfg.Server.Authentication.Enabled = &ligado
	cfg.Server.Authentication.Users = []config.UserSpec{
		{Username: "admin", PasswordEnv: variavelAdmin, Role: config.RoleAdmin},
		{Username: "leitor", PasswordFile: arquivoLeitor, Role: config.RoleRead},
	}

	out, err := resolveCredenciais(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("quer 2 credenciais resolvidas, veio %d", len(out))
	}
	admin, ok := out["admin"]
	if !ok {
		t.Fatal("username \"admin\" ausente do mapa resolvido")
	}
	if admin.role != config.RoleAdmin {
		t.Errorf("papel de admin = %q, quer %q", admin.role, config.RoleAdmin)
	}
	leitor, ok := out["leitor"]
	if !ok {
		t.Fatal("username \"leitor\" ausente do mapa resolvido")
	}
	if leitor.role != config.RoleRead {
		t.Errorf("papel de leitor = %q, quer %q", leitor.role, config.RoleRead)
	}
	if admin.digestSenha == leitor.digestSenha {
		t.Error("dois usuários com senhas diferentes resolveram para o mesmo digest")
	}
	var zero [32]byte
	if admin.digestSenha == zero || leitor.digestSenha == zero {
		t.Error("digest resolvido não pode ser o valor zero")
	}
}

// certificadoDeTesteComValidade gera um certificado autoassinado (ECDSA
// P-256) EM MEMÓRIA, com o NotBefore/NotAfter que o teste pedir — é
// exatamente o que avisaValidadeDoCertificado parseia para decidir entre
// silêncio, WARN e ERRO. Ao contrário de geraCertificadoDeTeste
// (security_test.go, que escreve em disco para exercitar
// tls.LoadX509KeyPair via arquivo), este helper devolve o tls.Certificate
// pronto: os testes abaixo chamam avisaValidadeDoCertificado diretamente,
// sem passar pela subida inteira do servidor.
func certificadoDeTesteComValidade(t *testing.T, notBefore, notAfter time.Time) tls.Certificate {
	t.Helper()
	chave, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
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
	der, err := x509.CreateCertificate(rand.Reader, modelo, modelo, &chave.PublicKey, chave)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	chaveBytes, err := x509.MarshalECPrivateKey(chave)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: chaveBytes})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// O ramo central do brief: certificado JÁ expirado loga ERRO (nunca recusa
// a subida — isso é provado à parte, em tls_test.go, com server.Novo de
// verdade) e publica a gauge de expiração com o NotAfter exato, epoch
// segundos.
func TestAvisaValidadeDoCertificadoJaExpiradoLogaErroEPublicaGauge(t *testing.T) {
	notAfter := time.Now().Add(-time.Hour)
	cert := certificadoDeTesteComValidade(t, time.Now().Add(-48*time.Hour), notAfter)

	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	reg := metrics.NewRegistry()

	if err := avisaValidadeDoCertificado(cert, log, reg); err != nil {
		t.Fatalf("avisaValidadeDoCertificado: %v", err)
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
	cert := certificadoDeTesteComValidade(t, time.Now().Add(-time.Hour), notAfter)

	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	reg := metrics.NewRegistry()

	if err := avisaValidadeDoCertificado(cert, log, reg); err != nil {
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
	cert := certificadoDeTesteComValidade(t, time.Now().Add(-time.Hour), time.Now().Add(365*24*time.Hour))

	var buf bytes.Buffer
	log := logging.New(&buf, logging.FormatJSON, slog.LevelInfo)
	reg := metrics.NewRegistry()

	if err := avisaValidadeDoCertificado(cert, log, reg); err != nil {
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

// enderecoLoopback desligado (default) não monta endereço nenhum — nenhum
// listener a mais sobe.
func TestEnderecoLoopbackDesligadoDevolveVazio(t *testing.T) {
	var cfg config.Config
	addr, err := enderecoLoopback(&cfg)
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
	_, err := enderecoLoopback(&cfg)
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
	addr, err := enderecoLoopback(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if addr != "127.0.0.1:9143" {
		t.Errorf("endereco = %q, quer 127.0.0.1:9143", addr)
	}
}
