package users

import (
	"crypto/rand"
	"crypto/subtle"
	"fmt"

	"golang.org/x/crypto/argon2"
)

// Política de tamanho de senha. O mínimo recusa, nunca completa. O máximo
// existe porque o argon2id consome a senha inteira: sem teto, um corpo de
// requisição de megabytes vira CPU gasta antes de qualquer recusa.
const (
	MinPasswordBytes = 12
	MaxPasswordBytes = 1024
)

// algorithmArgon2id é o único algoritmo conhecido. O identificador viaja em
// cada hash para que um algoritmo ou custo novo entre sem migração: a linha
// antiga continua verificável pelos parâmetros com que foi gravada.
const algorithmArgon2id = 1

// Parâmetros default do argon2id: t=2, m=19 MiB, p=1 — o mínimo recomendado
// pela OWASP. O custo em memória é o que pesa no orçamento de RSS do
// processo, e é por isso que o verificador limita quantas derivações correm
// ao mesmo tempo (verifier.go).
const (
	defaultTimeCost  = 2
	defaultMemoryKiB = 19 * 1024
	defaultThreads   = 1
	saltBytes        = 16
	keyBytes         = 32
)

// PasswordHash é a senha derivada, com tudo o que é preciso para verificá-la
// de novo.
type PasswordHash struct {
	Algorithm uint8
	TimeCost  uint32
	MemoryKiB uint32
	Threads   uint8
	Salt      [saltBytes]byte
	Key       [keyBytes]byte
}

// deriveKey é o seam de teste da derivação: os testes de timing contam as
// chamadas para provar que "usuário inexistente" e "senha errada" pagam o
// mesmo KDF.
var deriveKey = argon2.IDKey

// ValidatePassword aplica a política de tamanho.
func ValidatePassword(password []byte) error {
	if len(password) < MinPasswordBytes || len(password) > MaxPasswordBytes {
		return fmt.Errorf("%w: length must be %d..%d bytes", ErrWeakPassword, MinPasswordBytes, MaxPasswordBytes)
	}
	return nil
}

// NewHash valida a senha e deriva o hash com sal novo e os parâmetros default.
func NewHash(password []byte) (PasswordHash, error) {
	if err := ValidatePassword(password); err != nil {
		return PasswordHash{}, err
	}
	h := PasswordHash{
		Algorithm: algorithmArgon2id,
		TimeCost:  defaultTimeCost,
		MemoryKiB: defaultMemoryKiB,
		Threads:   defaultThreads,
	}
	if _, err := rand.Read(h.Salt[:]); err != nil {
		return PasswordHash{}, fmt.Errorf("users: generate salt: %w", err)
	}
	copy(h.Key[:], h.derive(password))
	return h, nil
}

func (h PasswordHash) derive(password []byte) []byte {
	return deriveKey(password, h.Salt[:], h.TimeCost, h.MemoryKiB, h.Threads, keyBytes)
}

// Matches deriva a senha com os parâmetros deste hash e compara em tempo
// constante. Custa um KDF inteiro, sempre — inclusive quando a senha tem
// tamanho fora da política, para que o tempo não diga nada sobre ela.
func (h PasswordHash) Matches(password []byte) bool {
	return subtle.ConstantTimeCompare(h.derive(password), h.Key[:]) == 1
}

// validate recusa parâmetros que o decodificador não sabe verificar ou que
// fariam a verificação pedir memória absurda (um arquivo adulterado com
// MemoryKiB de 4 bilhões derrubaria o processo no primeiro login).
func (h PasswordHash) validate() error {
	if h.Algorithm != algorithmArgon2id {
		return fmt.Errorf("unknown hash algorithm %d", h.Algorithm)
	}
	if h.TimeCost == 0 || h.TimeCost > 64 {
		return fmt.Errorf("hash time cost %d out of range", h.TimeCost)
	}
	if h.MemoryKiB < 1024 || h.MemoryKiB > 256*1024 {
		return fmt.Errorf("hash memory %d KiB out of range", h.MemoryKiB)
	}
	if h.Threads == 0 || h.Threads > 16 {
		return fmt.Errorf("hash parallelism %d out of range", h.Threads)
	}
	return nil
}

// ghostHash é o hash contra o qual um usuário INEXISTENTE é verificado, com os
// mesmos parâmetros default dos hashes reais: o caminho de "usuário não
// existe" paga o mesmo KDF de "senha errada", e o tempo de resposta não
// enumera contas. O sal é aleatório por processo; nenhuma senha real
// autentica contra ele, porque ele nunca está ligado a um nome.
var ghostHash = newGhostHash()

func newGhostHash() PasswordHash {
	h := PasswordHash{
		Algorithm: algorithmArgon2id,
		TimeCost:  defaultTimeCost,
		MemoryKiB: defaultMemoryKiB,
		Threads:   defaultThreads,
	}
	if _, err := rand.Read(h.Salt[:]); err != nil {
		panic(fmt.Sprintf("users: generate ghost salt: %v", err))
	}
	if _, err := rand.Read(h.Key[:]); err != nil {
		panic(fmt.Sprintf("users: generate ghost key: %v", err))
	}
	return h
}
