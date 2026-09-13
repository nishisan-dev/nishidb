package users

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"
)

// kdfConcurrency é quantas derivações argon2id correm ao mesmo tempo. Cada uma
// segura ~19 MiB enquanto dura; com duas, o pico transitório fica perto de
// 38 MiB, dentro do orçamento de RSS do processo. As demais esperam na fila.
const kdfConcurrency = 2

// verifier é o que torna viável verificar HTTP Basic com argon2id a CADA
// requisição. Sem ele, todo request pagaria dezenas de milissegundos e
// dezenas de MiB.
//
// O cache guarda, por usuário, um HMAC-SHA256 da senha que JÁ autenticou, com
// chave aleatória do processo, junto com a geração da linha naquele instante.
// Um acerto compara em tempo constante e dispensa o KDF. A chave nunca sai da
// memória e morre com o processo; o HMAC não é reversível para a senha, e sem
// a chave não serve nem para testar candidatas.
//
// Só o SUCESSO toma o atalho. Senha errada paga o KDF mesmo com o cache
// quente: se não pagasse, "senha errada para um usuário que autenticou há
// pouco" responderia mais rápido que "usuário inexistente", e o tempo
// enumeraria contas.
type verifier struct {
	key   [sha256.Size]byte
	slots chan struct{}

	mu    sync.RWMutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	generation uint64
	mac        [sha256.Size]byte
}

func (v *verifier) init() error {
	if _, err := rand.Read(v.key[:]); err != nil {
		return fmt.Errorf("users: generate verification key: %w", err)
	}
	v.slots = make(chan struct{}, kdfConcurrency)
	v.cache = make(map[string]cacheEntry)
	return nil
}

func (v *verifier) mac(username string, password []byte) [sha256.Size]byte {
	h := hmac.New(sha256.New, v.key[:])
	h.Write([]byte(username))
	h.Write([]byte{0})
	h.Write(password)
	var out [sha256.Size]byte
	h.Sum(out[:0])
	return out
}

// forget descarta a entrada de um usuário. A geração já basta para invalidar;
// descartar mantém o mapa do tamanho da tabela.
func (v *verifier) forget(username string) {
	v.mu.Lock()
	delete(v.cache, username)
	v.mu.Unlock()
}

// Authenticate verifica uma credencial contra a tabela publicada.
//
// Devolve o papel e ok=true quando confere. known diz se o nome existe na
// tabela, e serve só para a razão da métrica e do log de recusa — nunca para a
// resposta HTTP, que é a mesma nos dois casos.
func (s *Store) Authenticate(username string, password []byte) (role string, ok, known bool) {
	t := s.cur.Load()
	i, exists := t.find(username)
	var u User
	target := ghostHash
	if exists {
		u = t.users[i]
		target = u.Hash
	}
	v := &s.verifier
	tooLong := len(password) > MaxPasswordBytes
	if tooLong {
		// O KDF roda sobre o teto, e o resultado é descartado: o tempo não
		// pode dizer que a senha foi recusada pelo tamanho.
		password = password[:MaxPasswordBytes]
	}

	var mac [sha256.Size]byte
	if exists && !tooLong {
		mac = v.mac(username, password)
		v.mu.RLock()
		e, hit := v.cache[username]
		v.mu.RUnlock()
		if hit && e.generation == u.generation && hmac.Equal(e.mac[:], mac[:]) {
			return u.Role, true, true
		}
	}

	v.slots <- struct{}{}
	match := target.Matches(password)
	<-v.slots

	if !exists || tooLong || !match {
		return "", false, exists
	}
	v.mu.Lock()
	v.cache[username] = cacheEntry{generation: u.generation, mac: mac}
	v.mu.Unlock()
	return u.Role, true, true
}
