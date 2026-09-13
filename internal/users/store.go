package users

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/storage/atomicfile"
)

// Nomes em disco. São contrato: o operador procura por eles.
const (
	// DirName é o diretório de sistema dentro de storage.path. Começa com "_"
	// porque nome de dataset com "_" é recusado no parse do schema — nunca
	// colide com um dataset de dados.
	DirName = "_system"
	// StateFileName é a tabela de usuários.
	StateFileName = "users.state"
	// BootstrapPasswordFileName guarda a senha gerada na primeira subida, com
	// modo 0600, até que ela seja trocada ou o usuário removido.
	BootstrapPasswordFileName = "bootstrap-admin.password"
	// BootstrapUsername é o ADMIN criado na primeira subida.
	BootstrapUsername = "admin"

	lockFileName = "LOCK"
)

// bootstrapActor é o autor registrado na linha criada pelo bootstrap.
const bootstrapActor = "bootstrap"

// bootstrapPasswordBytes é a entropia da senha gerada: 24 bytes de
// crypto/rand, 32 caracteres em base64url.
const bootstrapPasswordBytes = 24

// Options configura a abertura da tabela.
type Options struct {
	// Logger recebe a auditoria. nil descarta.
	Logger *logging.Logger
	// Now é o relógio das datas de auditoria. nil usa time.Now.
	Now func() time.Time
	// WriteHook é o seam de injeção de falha na gravação do users.state
	// (nil em produção).
	WriteHook atomicfile.Hook
}

// Store é a tabela de usuários aberta, com o flock do diretório de sistema.
//
// Leitura (Authenticate, Users) é livre de lock sobre um snapshot imutável.
// Mutação é serializada por mu e segue SEMPRE a mesma ordem: valida → monta a
// tabela nova → grava durável → publica o ponteiro → audita. Publicar o
// ponteiro é o commit de visibilidade e vem por último, como a location no
// storage: ninguém autentica com uma senha que ainda não está no disco.
type Store struct {
	dir  string
	lock *os.File
	log  *logging.Logger
	now  func() time.Time
	hook atomicfile.Hook

	cur atomic.Pointer[table]

	mu      sync.Mutex
	failed  error  // sob mu; ver ErrWriterFailed
	nextGen uint64 // sob mu

	verifier verifier

	bootstrapPath string
}

type table struct {
	users []User // ordenada por nome; nunca mutada depois de publicada
}

func (t *table) find(name string) (int, bool) {
	return slices.BinarySearchFunc(t.users, name, func(u User, n string) int {
		return strings.Compare(u.Username, n)
	})
}

// Dir devolve o diretório de sistema de um storage.path.
func Dir(storagePath string) string {
	return filepath.Join(storagePath, DirName)
}

// Open abre a tabela em dir, tomando o flock exclusivo. Sem tabela, roda o
// bootstrap: gera o ADMIN, grava a senha em BootstrapPasswordFileName (0600)
// e só então grava a tabela. A ordem importa: uma queda entre as duas
// gravações deixa só o arquivo de senha, e a próxima abertura sobrescreve os
// dois com um par novo e consistente — nunca uma tabela com uma senha que
// ninguém recebeu.
func Open(dir string, opts Options) (*Store, error) {
	s := &Store{dir: dir, log: opts.Logger, now: opts.Now, hook: opts.WriteHook}
	if s.log == nil {
		s.log = logging.Discard()
	}
	if s.now == nil {
		s.now = time.Now
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("users: create %s: %w", dir, err)
	}
	if err := atomicfile.SyncDir(filepath.Dir(dir)); err != nil {
		return nil, fmt.Errorf("users: %w", err)
	}
	lock, err := acquireLock(dir)
	if err != nil {
		return nil, err
	}
	s.lock = lock
	if err := s.verifier.init(); err != nil {
		_ = s.Close()
		return nil, err
	}

	users, err := loadState(s.statePath())
	switch {
	case errors.Is(err, errStateAbsent):
		users, err = s.bootstrap()
		if err != nil {
			_ = s.Close()
			return nil, err
		}
	case err != nil:
		_ = s.Close()
		return nil, err
	}
	for i := range users {
		users[i].generation = s.generation()
	}
	s.cur.Store(&table{users: users})
	return s, nil
}

// Close solta o flock. A tabela em memória continua legível, mas nenhuma
// mutação deve vir depois.
func (s *Store) Close() error {
	if s.lock == nil {
		return nil
	}
	f := s.lock
	s.lock = nil
	return f.Close() // o close libera o flock
}

// BootstrapPasswordPath devolve o caminho do arquivo de senha quando ESTA
// abertura rodou o bootstrap, e "" caso contrário.
func (s *Store) BootstrapPasswordPath() string {
	return s.bootstrapPath
}

func (s *Store) statePath() string {
	return filepath.Join(s.dir, StateFileName)
}

func (s *Store) bootstrapFilePath() string {
	return filepath.Join(s.dir, BootstrapPasswordFileName)
}

// generation devolve uma geração nova. Chamado sob mu, ou antes de o Store
// ser publicado.
func (s *Store) generation() uint64 {
	s.nextGen++
	return s.nextGen
}

func (s *Store) bootstrap() ([]User, error) {
	raw := make([]byte, bootstrapPasswordBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("users: generate bootstrap password: %w", err)
	}
	password := base64.RawURLEncoding.EncodeToString(raw)
	hash, err := NewHash([]byte(password))
	if err != nil {
		return nil, err
	}
	now := s.now()
	admin := User{
		Username:          BootstrapUsername,
		Role:              config.RoleAdmin,
		Hash:              hash,
		Bootstrap:         true,
		CreatedAt:         now,
		CreatedBy:         bootstrapActor,
		UpdatedAt:         now,
		UpdatedBy:         bootstrapActor,
		PasswordChangedAt: now,
	}
	path := s.bootstrapFilePath()
	if err := atomicfile.Write(path, []byte(password+"\n"), 0o600); err != nil {
		return nil, fmt.Errorf("users: write bootstrap password: %w", err)
	}
	users := []User{admin}
	if err := atomicfile.WriteWithHook(s.statePath(), encodeState(users), 0o600, s.hook); err != nil {
		return nil, fmt.Errorf("users: write user table: %w", err)
	}
	s.bootstrapPath = path
	s.log.Warn("users.bootstrap",
		"user table created with a generated ADMIN credential; read the password from the file and replace it",
		"user", BootstrapUsername, "path", path)
	return users, nil
}

// Users devolve a tabela como um ADMIN pode vê-la: sem hash, em ordem de nome.
func (s *Store) Users() []UserView {
	t := s.cur.Load()
	out := make([]UserView, len(t.users))
	for i, u := range t.users {
		out[i] = u.view()
	}
	return out
}

// Create cria um usuário.
func (s *Store) Create(actor Actor, username, role string, password []byte) error {
	if err := ValidateUsername(username); err != nil {
		return err
	}
	if err := ValidateRole(role); err != nil {
		return err
	}
	// O KDF corre FORA do lock de mutação: são dezenas de milissegundos, e
	// nada nele depende da tabela.
	hash, err := NewHash(password)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	t := s.cur.Load()
	i, exists := t.find(username)
	if exists {
		return fmt.Errorf("%w: %q", ErrUserExists, username)
	}
	now := s.now()
	u := User{
		Username:          username,
		Role:              role,
		Hash:              hash,
		CreatedAt:         now,
		CreatedBy:         actor.Name,
		UpdatedAt:         now,
		UpdatedBy:         actor.Name,
		PasswordChangedAt: now,
		generation:        s.generation(),
	}
	next := slices.Insert(slices.Clone(t.users), i, u)
	if err := s.commit(next); err != nil {
		return err
	}
	s.audit("users.created", "user created", actor, username, role)
	return nil
}

// SetPassword troca a senha de um usuário. Trocar a senha do usuário de
// bootstrap aposenta o arquivo de senha gerado.
func (s *Store) SetPassword(actor Actor, username string, password []byte) error {
	hash, err := NewHash(password)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	t := s.cur.Load()
	i, exists := t.find(username)
	if !exists {
		return fmt.Errorf("%w: %q", ErrUnknownUser, username)
	}
	now := s.now()
	next := slices.Clone(t.users)
	wasBootstrap := next[i].Bootstrap
	next[i].Hash = hash
	next[i].Bootstrap = false
	next[i].UpdatedAt = now
	next[i].UpdatedBy = actor.Name
	next[i].PasswordChangedAt = now
	next[i].generation = s.generation()
	if err := s.commit(next); err != nil {
		return err
	}
	s.verifier.forget(username)
	s.audit("users.password_changed", "user password changed", actor, username, next[i].Role)
	if wasBootstrap {
		s.retireBootstrapFile()
	}
	return nil
}

// SetRole muda o papel de um usuário. Rebaixar o último ADMIN é recusado.
// Pedir o papel que o usuário já tem é sucesso sem gravação.
func (s *Store) SetRole(actor Actor, username, role string) error {
	if err := ValidateRole(role); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	t := s.cur.Load()
	i, exists := t.find(username)
	if !exists {
		return fmt.Errorf("%w: %q", ErrUnknownUser, username)
	}
	if t.users[i].Role == role {
		return nil
	}
	if t.users[i].Role == config.RoleAdmin && adminCount(t.users) == 1 {
		return fmt.Errorf("%w: %q", ErrLastAdmin, username)
	}
	next := slices.Clone(t.users)
	next[i].Role = role
	next[i].UpdatedAt = s.now()
	next[i].UpdatedBy = actor.Name
	next[i].generation = s.generation()
	if err := s.commit(next); err != nil {
		return err
	}
	s.verifier.forget(username)
	s.audit("users.role_changed", "user role changed", actor, username, role)
	return nil
}

// Delete remove um usuário. Remover o último ADMIN é recusado; remover a si
// mesmo é permitido desde que não seja o último.
func (s *Store) Delete(actor Actor, username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return s.failed
	}
	t := s.cur.Load()
	i, exists := t.find(username)
	if !exists {
		return fmt.Errorf("%w: %q", ErrUnknownUser, username)
	}
	removed := t.users[i]
	if removed.Role == config.RoleAdmin && adminCount(t.users) == 1 {
		return fmt.Errorf("%w: %q", ErrLastAdmin, username)
	}
	next := slices.Delete(slices.Clone(t.users), i, i+1)
	if err := s.commit(next); err != nil {
		return err
	}
	s.verifier.forget(username)
	s.audit("users.deleted", "user deleted", actor, username, removed.Role)
	if removed.Bootstrap {
		s.retireBootstrapFile()
	}
	return nil
}

// commit grava a tabela nova e, só depois, a publica. Chamado sob mu.
//
// Falha na gravação deixa o Store FALHO para mutação: a queda pode ter
// acontecido depois do rename, com o disco já à frente da memória, e a próxima
// mutação montaria a tabela a partir da memória e apagaria essa mudança sem
// ninguém saber. O servidor segue autenticando pelo que publicou.
func (s *Store) commit(next []User) error {
	if err := atomicfile.WriteWithHook(s.statePath(), encodeState(next), 0o600, s.hook); err != nil {
		s.failed = fmt.Errorf("%w: %w", ErrWriterFailed, err)
		s.log.Error("users.write_failed", "user table write failed; user management disabled until restart",
			"error", err.Error())
		return s.failed
	}
	s.cur.Store(&table{users: next})
	return nil
}

// retireBootstrapFile apaga o arquivo de senha gerada depois que a tabela já
// não aceita mais aquela senha. Falha aqui não desfaz a mutação — a senha do
// arquivo já não autentica —, mas é registrada para o operador apagar à mão.
func (s *Store) retireBootstrapFile() {
	path := s.bootstrapFilePath()
	err := os.Remove(path)
	if err == nil {
		err = atomicfile.SyncDir(s.dir)
	}
	if err != nil && !os.IsNotExist(err) {
		s.log.Warn("users.bootstrap_file_not_removed",
			"the bootstrap password no longer authenticates, but its file could not be removed",
			"path", path, "error", err.Error())
	}
}

func (s *Store) audit(event, msg string, actor Actor, username, role string) {
	args := []any{"actor", actor.Name, "user", username, "role", role}
	if actor.Remote != "" {
		args = append(args, "remote", actor.Remote)
	}
	s.log.Info(event, msg, args...)
}

func adminCount(users []User) int {
	n := 0
	for _, u := range users {
		if u.Role == config.RoleAdmin {
			n++
		}
	}
	return n
}

// acquireLock trava o diretório de sistema de forma não bloqueante, no mesmo
// molde do lock de dataset (storage/store_lock.go): flock sobre o descritor,
// liberado pelo kernel se o processo morrer, pid gravado só para diagnóstico.
func acquireLock(dir string) (*os.File, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("users: open lock %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		owner := ""
		if raw, rerr := os.ReadFile(path); rerr == nil {
			if pid := strings.TrimSpace(string(raw)); pid != "" {
				owner = " (pid " + pid + ")"
			}
		}
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s%s", ErrUsersLocked, dir, owner)
	}
	if err := f.Truncate(0); err == nil {
		if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0); err == nil {
			_ = f.Sync()
		}
	}
	return f, nil
}
