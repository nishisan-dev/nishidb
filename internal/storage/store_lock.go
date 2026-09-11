package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// ErrDatasetLocked indica que o diretório do dataset já está aberto por
// outro processo. No M1 o acesso é exclusivo: o Store assume writer único e
// dois processos no mesmo diretório teriam dois overlays de location, dois
// contadores de rowId e dois checkpoints concorrentes — corrupção garantida,
// e alcançável por operação humana normal assim que existir um CLI (rodar
// ingest e query ao mesmo tempo).
var ErrDatasetLocked = errors.New("storage: dataset já aberto por outro processo")

// lockFileName é o arquivo de lock no diretório do dataset. O conteúdo (pid)
// é apenas diagnóstico — o mecanismo é o flock sobre o descritor.
const lockFileName = "LOCK"

// datasetLock mantém o flock exclusivo enquanto o Store estiver aberto.
type datasetLock struct {
	f *os.File
}

// acquireDatasetLock trava o dataset de forma não bloqueante.
//
// flock é associado ao descritor aberto, o que dá duas propriedades que
// importam: o kernel libera sozinho se o processo morrer (um crash não
// deixa o dataset inacessível, ao contrário de um lock por arquivo-marcador)
// e duas aberturas do MESMO processo também colidem, porque cada open cria
// uma descrição própria.
func acquireDatasetLock(dir string) (*datasetLock, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: abrir o lock %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		dono := donoOfLock(path)
		f.Close()
		return nil, fmt.Errorf("%w: %s%s", ErrDatasetLocked, dir, dono)
	}
	// Regrava o pid para diagnóstico. Falha aqui não invalida o lock.
	if err := f.Truncate(0); err == nil {
		if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0); err == nil {
			_ = f.Sync()
		}
	}
	return &datasetLock{f: f}, nil
}

// donoOfLock lê o pid registrado, para a mensagem de erro apontar o culpado.
func donoOfLock(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	pid := strings.TrimSpace(string(raw))
	if pid == "" {
		return ""
	}
	return " (pid " + pid + ")"
}

// release solta o lock. O arquivo NÃO é removido: apagá-lo abriria uma
// janela em que outro processo já criou e travou o seu próprio LOCK e teria
// a entrada de diretório removida por baixo.
func (l *datasetLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	f := l.f
	l.f = nil
	return f.Close() // o close libera o flock
}
