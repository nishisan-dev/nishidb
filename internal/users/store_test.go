package users

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

var admin = Actor{Name: "admin", Remote: "127.0.0.1:5555"}

func openTest(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	s, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func readBootstrapPassword(t *testing.T, dir string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, BootstrapPasswordFileName))
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimRight(raw, "\n")
}

func mustAuth(t *testing.T, s *Store, user string, password []byte, wantRole string) {
	t.Helper()
	role, ok, _ := s.Authenticate(user, password)
	if !ok || role != wantRole {
		t.Fatalf("Authenticate(%q) = %q,%v; quer %q,true", user, role, ok, wantRole)
	}
}

func mustNotAuth(t *testing.T, s *Store, user string, password []byte) {
	t.Helper()
	if _, ok, _ := s.Authenticate(user, password); ok {
		t.Fatalf("Authenticate(%q) autenticou com a senha que não devia", user)
	}
}

func fileMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

func TestPrimeiraAberturaGeraAdminDeBootstrapComArquivo0600(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	var logBuf bytes.Buffer
	s := openTest(t, dir, Options{Logger: logging.New(&logBuf, logging.FormatJSON, slog.LevelDebug)})

	path := s.BootstrapPasswordPath()
	if path != filepath.Join(dir, BootstrapPasswordFileName) {
		t.Fatalf("BootstrapPasswordPath = %q", path)
	}
	for _, p := range []string{path, filepath.Join(dir, StateFileName)} {
		if m := fileMode(t, p); m != 0o600 {
			t.Errorf("%s com modo %04o, quer 0600", filepath.Base(p), m)
		}
	}
	password := readBootstrapPassword(t, dir)
	if len(password) < MinPasswordBytes {
		t.Fatalf("senha gerada curta demais: %d bytes", len(password))
	}
	mustAuth(t, s, BootstrapUsername, password, "ADMIN")

	users := s.Users()
	if len(users) != 1 || !users[0].Bootstrap || users[0].CreatedBy != "bootstrap" {
		t.Errorf("tabela depois do bootstrap = %+v", users)
	}
	log := logBuf.String()
	if !strings.Contains(log, "users.bootstrap") || !strings.Contains(log, path) {
		t.Errorf("o log tem de nomear o evento e o caminho do arquivo: %s", log)
	}
	if strings.Contains(log, string(password)) {
		t.Errorf("a senha gerada vazou no log")
	}
}

func TestReaberturaNaoRegeraOBootstrap(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	s := openTest(t, dir, Options{})
	password := readBootstrapPassword(t, dir)
	_ = s.Close()

	s2 := openTest(t, dir, Options{})
	if s2.BootstrapPasswordPath() != "" {
		t.Errorf("a reabertura rodou o bootstrap de novo")
	}
	mustAuth(t, s2, BootstrapUsername, password, "ADMIN")
}

func TestTabelaCorrompidaRecusaAAberturaSemGerarCredencial(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, StateFileName), []byte("lixo que não é tabela"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, Options{})
	if !errors.Is(err, ErrCorruptUsers) {
		t.Fatalf("erro = %v, quer ErrCorruptUsers", err)
	}
	if _, err := os.Stat(filepath.Join(dir, BootstrapPasswordFileName)); !os.IsNotExist(err) {
		t.Errorf("tabela corrompida não pode virar credencial de bootstrap nova")
	}
}

func TestSegundaAberturaDaMesmaTabelaEhRecusada(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	openTest(t, dir, Options{})
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrUsersLocked) {
		t.Fatalf("erro = %v, quer ErrUsersLocked", err)
	}
}

func TestUltimoAdminNaoPodeSerRemovidoNemRebaixado(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), DirName), Options{})
	if err := s.Delete(admin, BootstrapUsername); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("Delete do último ADMIN: erro = %v, quer ErrLastAdmin", err)
	}
	if err := s.SetRole(admin, BootstrapUsername, "READ"); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("rebaixar o último ADMIN: erro = %v, quer ErrLastAdmin", err)
	}

	// Com um segundo ADMIN, os dois passam a ser permitidos.
	if err := s.Create(admin, "ops", "ADMIN", []byte("senha-do-ops-longa")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRole(admin, BootstrapUsername, "READ"); err != nil {
		t.Errorf("rebaixar com outro ADMIN presente: %v", err)
	}
	if err := s.Delete(admin, "ops"); !errors.Is(err, ErrLastAdmin) {
		t.Errorf("remover o ADMIN que sobrou: erro = %v, quer ErrLastAdmin", err)
	}
}

func TestMutacaoInvalidaEhRecusadaComOErroNomeado(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), DirName), Options{})
	good := []byte("senha-suficiente")
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"nome existente", s.Create(admin, BootstrapUsername, "READ", good), ErrUserExists},
		{"nome invalido", s.Create(admin, "Maria", "READ", good), ErrInvalidUsername},
		{"papel invalido", s.Create(admin, "maria", "WRITE", good), ErrInvalidRole},
		{"senha curta", s.Create(admin, "maria", "READ", []byte("curta")), ErrWeakPassword},
		{"senha de usuario inexistente", s.SetPassword(admin, "ninguem", good), ErrUnknownUser},
		{"papel de usuario inexistente", s.SetRole(admin, "ninguem", "READ"), ErrUnknownUser},
		{"remover inexistente", s.Delete(admin, "ninguem"), ErrUnknownUser},
	}
	for _, c := range cases {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: erro = %v, quer %v", c.name, c.err, c.want)
		}
	}
	if n := len(s.Users()); n != 1 {
		t.Errorf("mutação recusada mexeu na tabela: %d linhas", n)
	}
}

func TestTrocarASenhaDoBootstrapApagaOArquivoGerado(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	s := openTest(t, dir, Options{})
	old := readBootstrapPassword(t, dir)
	fresh := []byte("senha-escolhida-pelo-operador")
	if err := s.SetPassword(admin, BootstrapUsername, fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, BootstrapPasswordFileName)); !os.IsNotExist(err) {
		t.Errorf("o arquivo de senha gerada sobreviveu à troca")
	}
	if s.Users()[0].Bootstrap {
		t.Errorf("a linha continua marcada como bootstrap depois da troca")
	}
	mustNotAuth(t, s, BootstrapUsername, old)
	mustAuth(t, s, BootstrapUsername, fresh, "ADMIN")
}

func TestRemoverOAdminDeBootstrapApagaOArquivoGerado(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	s := openTest(t, dir, Options{})
	if err := s.Create(admin, "ops", "ADMIN", []byte("senha-do-ops-longa")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(Actor{Name: "ops"}, BootstrapUsername); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, BootstrapPasswordFileName)); !os.IsNotExist(err) {
		t.Errorf("o arquivo de senha gerada sobreviveu à remoção do usuário")
	}
}

// Uma queda antes do rename não pode perder a senha antiga; uma queda depois do
// rename não pode devolver a antiga. Em ambos os casos a gestão fica travada
// até reabrir, porque a memória não sabe mais o que está no disco.
func TestQuedaNaGravacaoDaTabelaPreservaOQueChegouAoDisco(t *testing.T) {
	injected := errors.New("queda simulada")
	cases := []struct {
		failAfter string
		newOnDisk bool
	}{
		{atomicfile.StepFsync, false},
		{atomicfile.StepRename, true},
	}
	for _, c := range cases {
		t.Run(c.failAfter, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), DirName)
			var armed atomic.Bool
			s, err := Open(dir, Options{WriteHook: func(step string) error {
				if armed.Load() && step == c.failAfter {
					return injected
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			old := readBootstrapPassword(t, dir)
			fresh := []byte("senha-nova-bem-longa")

			armed.Store(true)
			if err := s.SetPassword(admin, BootstrapUsername, fresh); !errors.Is(err, ErrWriterFailed) {
				t.Fatalf("erro = %v, quer ErrWriterFailed", err)
			}
			// A memória não publicou a troca que falhou.
			mustAuth(t, s, BootstrapUsername, old, "ADMIN")
			if err := s.Create(admin, "outro", "READ", fresh); !errors.Is(err, ErrWriterFailed) {
				t.Errorf("mutação depois da falha: erro = %v, quer ErrWriterFailed", err)
			}
			_ = s.Close()

			reopened := openTest(t, dir, Options{})
			if c.newOnDisk {
				mustAuth(t, reopened, BootstrapUsername, fresh, "ADMIN")
				mustNotAuth(t, reopened, BootstrapUsername, old)
			} else {
				mustAuth(t, reopened, BootstrapUsername, old, "ADMIN")
				mustNotAuth(t, reopened, BootstrapUsername, fresh)
			}
		})
	}
}

func TestSenhaTrocadaDeixaDeAutenticarMesmoComOCacheQuente(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	s := openTest(t, dir, Options{})
	pw := []byte("senha-do-leitor-1")
	if err := s.Create(admin, "leitor", "READ", pw); err != nil {
		t.Fatal(err)
	}
	mustAuth(t, s, "leitor", pw, "READ") // aquece o cache
	mustAuth(t, s, "leitor", pw, "READ")

	if err := s.SetPassword(admin, "leitor", []byte("senha-do-leitor-2")); err != nil {
		t.Fatal(err)
	}
	mustNotAuth(t, s, "leitor", pw)

	if err := s.SetRole(admin, "leitor", "ADMIN"); err != nil {
		t.Fatal(err)
	}
	mustAuth(t, s, "leitor", []byte("senha-do-leitor-2"), "ADMIN")
}

// countKDF conta as derivações do teste. Não roda em paralelo com outros
// testes do pacote que troquem o seam.
func countKDF(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	orig := deriveKey
	deriveKey = func(password, salt []byte, time, memory uint32, threads uint8, keyLen uint32) []byte {
		n.Add(1)
		return orig(password, salt, time, memory, threads, keyLen)
	}
	t.Cleanup(func() { deriveKey = orig })
	return &n
}

// O cache só pode encurtar o SUCESSO. Usuário inexistente, senha errada com
// cache frio e senha errada com cache quente pagam exatamente um KDF cada — é
// o que impede o tempo de resposta de enumerar contas.
func TestSoOSucessoDispensaOKDFEOsFracassosPagamOMesmoPreco(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), DirName), Options{})
	pw := []byte("senha-do-leitor-1")
	if err := s.Create(admin, "leitor", "READ", pw); err != nil {
		t.Fatal(err)
	}
	n := countKDF(t)

	measure := func(_, user string, password []byte) int64 {
		before := n.Load()
		s.Authenticate(user, password)
		return n.Load() - before
	}
	if got := measure("inexistente", "ninguem", pw); got != 1 {
		t.Errorf("usuário inexistente pagou %d KDF, quer 1", got)
	}
	if got := measure("errada fria", "leitor", []byte("senha-errada-xyz")); got != 1 {
		t.Errorf("senha errada com cache frio pagou %d KDF, quer 1", got)
	}
	if got := measure("certa fria", "leitor", pw); got != 1 {
		t.Errorf("primeiro sucesso pagou %d KDF, quer 1", got)
	}
	if got := measure("errada quente", "leitor", []byte("senha-errada-xyz")); got != 1 {
		t.Errorf("senha errada com cache quente pagou %d KDF, quer 1", got)
	}
	if got := measure("certa quente", "leitor", pw); got != 0 {
		t.Errorf("sucesso com cache quente pagou %d KDF, quer 0", got)
	}
	if got := measure("longa demais", "leitor", bytes.Repeat([]byte("x"), MaxPasswordBytes+1)); got != 1 {
		t.Errorf("senha acima do teto pagou %d KDF, quer 1", got)
	}
}

func TestAuditoriaNomeiaQuemFezENuncaRegistraASenha(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DirName)
	var logBuf bytes.Buffer
	s := openTest(t, dir, Options{Logger: logging.New(&logBuf, logging.FormatJSON, slog.LevelDebug)})
	secrets := []string{"senha-criada-no-teste", "senha-trocada-no-teste"}
	if err := s.Create(admin, "leitor", "READ", []byte(secrets[0])); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPassword(admin, "leitor", []byte(secrets[1])); err != nil {
		t.Fatal(err)
	}
	if err := s.SetRole(admin, "leitor", "ADMIN"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(admin, "leitor"); err != nil {
		t.Fatal(err)
	}
	log := logBuf.String()
	for _, event := range []string{"users.created", "users.password_changed", "users.role_changed", "users.deleted"} {
		if !strings.Contains(log, event) {
			t.Errorf("evento %s ausente do log", event)
		}
	}
	if !strings.Contains(log, `"actor":"admin"`) || !strings.Contains(log, `"remote":"127.0.0.1:5555"`) {
		t.Errorf("auditoria sem actor/remote: %s", log)
	}
	for _, secret := range secrets {
		if strings.Contains(log, secret) {
			t.Errorf("a senha %q vazou no log", secret)
		}
	}
}

// Uma autenticação que já leu a tabela antiga e está no meio do KDF quando a
// senha é trocada termina com sucesso (é request em curso), mas NÃO pode
// deixar no cache uma entrada que faça a senha antiga continuar valendo. É a
// geração que fecha isso: o forget da troca roda ANTES de a autenticação em
// curso gravar a entrada velha, então só a comparação de geração a descarta.
func TestAutenticacaoEmCursoDuranteATrocaNaoEnvenenaOCache(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), DirName), Options{})
	old := []byte("senha-do-leitor-1")
	fresh := []byte("senha-do-leitor-2")
	if err := s.Create(admin, "leitor", "READ", old); err != nil {
		t.Fatal(err)
	}

	orig := deriveKey
	t.Cleanup(func() { deriveKey = orig })
	var fired atomic.Bool
	deriveKey = func(password, salt []byte, time, memory uint32, threads uint8, keyLen uint32) []byte {
		out := orig(password, salt, time, memory, threads, keyLen)
		if fired.CompareAndSwap(false, true) {
			// Dentro do KDF da autenticação: a troca acontece agora.
			if err := s.SetPassword(admin, "leitor", fresh); err != nil {
				t.Errorf("SetPassword no meio da autenticação: %v", err)
			}
		}
		return out
	}
	if _, ok, _ := s.Authenticate("leitor", old); !ok {
		t.Fatalf("a autenticação em curso deveria terminar com a senha que leu")
	}
	deriveKey = orig

	mustNotAuth(t, s, "leitor", old)
	mustAuth(t, s, "leitor", fresh, "READ")
}
