// Package users é o dono da tabela de usuários do servidor (issue #40): a
// ÚNICA fonte da verdade de quem autentica e com que papel.
//
// A tabela mora em <storage.path>/_system/users.state, fora de qualquer
// dataset. Não é um Store: são poucas linhas, e um Store traria dezenas de
// MiB de mmap, recovery e presença no catálogo, no health e na compactação,
// cada um pedindo uma exceção. O arquivo é regravado inteiro a cada mutação
// pelo atomicfile — o mesmo caminho durável do manifest —, o que dá a garantia
// que a issue exige: uma troca de senha que sobreviveu a um crash não volta à
// anterior.
//
// A senha nunca é guardada: só o hash argon2id com sal e custo (hash.go). Nada
// deste pacote registra senha ou hash em log, e nenhum tipo exportado para
// consulta carrega o hash (UserView).
package users

import (
	"errors"
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
)

var (
	// ErrUsersLocked indica que outro processo já tem a tabela aberta — em
	// geral o servidor no ar quando se tenta a gestão offline.
	ErrUsersLocked = errors.New("users: user table already open by another process")

	// ErrCorruptUsers indica users.state ilegível: magic, versão, CRC,
	// tamanho ou conteúdo que viola as invariantes da tabela.
	//
	// Ao contrário do estado de alarme, NÃO é degradação tolerada: subir sem a
	// tabela seria subir sem ninguém autenticar, e regenerar a credencial de
	// bootstrap seria entregar um admin novo a quem apagou ou estragou o
	// arquivo. O remédio é do operador, nunca automático.
	ErrCorruptUsers = errors.New("users: user table is corrupt")

	// ErrUserExists recusa criar um nome que já está na tabela.
	ErrUserExists = errors.New("users: user already exists")

	// ErrUnknownUser recusa mexer num nome que não está na tabela.
	ErrUnknownUser = errors.New("users: unknown user")

	// ErrLastAdmin recusa remover ou rebaixar o último ADMIN: sem ele ninguém
	// mais faria gestão com o servidor no ar.
	ErrLastAdmin = errors.New("users: cannot remove or demote the last ADMIN")

	// ErrWeakPassword recusa senha curta demais (ou longa demais).
	ErrWeakPassword = errors.New("users: password does not meet the length policy")

	// ErrInvalidUsername recusa nome fora de [a-z0-9_.-]{1,64}.
	ErrInvalidUsername = errors.New("users: invalid username")

	// ErrInvalidRole recusa papel fora do enum fechado READ|ADMIN.
	ErrInvalidRole = errors.New("users: invalid role")

	// ErrWriterFailed indica que uma gravação anterior da tabela falhou depois
	// de começar a mexer no disco. Como o stickyErr do storage: o arquivo pode
	// já estar à frente da memória, e seguir mutando a partir da memória
	// sobrescreveria em silêncio uma mudança que talvez tenha chegado ao
	// disco. A autenticação continua servindo; a gestão volta ao reabrir.
	ErrWriterFailed = errors.New("users: a previous write to the user table failed; restart to resume user management")
)

// MaxUsernameBytes é o teto do nome de usuário.
const MaxUsernameBytes = 64

// User é uma linha da tabela, COM o hash. Nunca sai deste pacote: o que sai
// para consulta é UserView.
type User struct {
	Username          string
	Role              string
	Hash              PasswordHash
	Bootstrap         bool
	CreatedAt         time.Time
	CreatedBy         string
	UpdatedAt         time.Time
	UpdatedBy         string
	PasswordChangedAt time.Time

	// generation muda a cada mutação desta linha e não é persistida. É o que
	// invalida o cache de verificação: uma entrada do cache só vale para a
	// geração em que foi criada.
	generation uint64
}

// UserView é a linha como ela pode ser vista por um ADMIN: tudo, menos o
// segredo. É o tipo que alimenta _meta.users.
type UserView struct {
	Username          string
	Role              string
	Bootstrap         bool
	CreatedAt         time.Time
	CreatedBy         string
	UpdatedAt         time.Time
	UpdatedBy         string
	PasswordChangedAt time.Time
}

func (u User) view() UserView {
	return UserView{
		Username:          u.Username,
		Role:              u.Role,
		Bootstrap:         u.Bootstrap,
		CreatedAt:         u.CreatedAt,
		CreatedBy:         u.CreatedBy,
		UpdatedAt:         u.UpdatedAt,
		UpdatedBy:         u.UpdatedBy,
		PasswordChangedAt: u.PasswordChangedAt,
	}
}

// Actor é quem pede a mutação, para a auditoria. Remote é vazio na gestão
// offline.
type Actor struct {
	Name   string
	Remote string
}

// ValidateUsername aplica [a-z0-9_.-]{1,64}. Minúsculas só, para que "Admin"
// e "admin" não sejam duas contas que o operador lê como a mesma.
func ValidateUsername(name string) error {
	if name == "" || len(name) > MaxUsernameBytes {
		return fmt.Errorf("%w: length must be 1..%d bytes", ErrInvalidUsername, MaxUsernameBytes)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '.', c == '-':
		default:
			return fmt.Errorf("%w: only [a-z0-9_.-] is allowed", ErrInvalidUsername)
		}
	}
	return nil
}

// ValidateRole aplica o enum fechado de papel, case-sensitive como no resto
// do servidor.
func ValidateRole(role string) error {
	switch role {
	case config.RoleRead, config.RoleAdmin:
		return nil
	}
	return fmt.Errorf("%w: %q (want %s|%s)", ErrInvalidRole, role, config.RoleRead, config.RoleAdmin)
}
