package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/config"
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
				!strings.Contains(err.Error(), "0620") {
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
