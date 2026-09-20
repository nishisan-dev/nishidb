// Package atomicfile é o ÚNICO caminho de gravação durável de arquivo
// inteiro do projeto: tmp → fsync → rename → fsync do diretório (a
// política de rename durável do design §9/M18).
//
// Nasceu dentro de internal/storage/checkpoint, onde servia ao manifest.bin,
// ao kafka.offsets e ao alarms.state. Saiu de lá quando a tabela de usuários
// do servidor (issue #40) precisou da mesma garantia fora de qualquer dataset:
// a alternativa era uma quarta cópia da sequência, e uma cópia diverge na
// primeira correção.
package atomicfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Os nomes dos quatro passos, na ordem em que acontecem. São o vocabulário do
// Hook e é por eles que os testes travam a ordem de durabilidade.
const (
	StepWrite   = "write"
	StepFsync   = "fsync"
	StepRename  = "rename"
	StepDirsync = "dirsync"
)

// Hook é chamado DEPOIS de cada passo bem-sucedido, com o nome do passo.
// Devolver erro interrompe a gravação ali mesmo, como se o processo tivesse
// caído logo depois daquele passo — é o seam de injeção de falha dos testes de
// crash. Amarrar o evento à execução garante que remover a operação também
// remove o evento (poder de detecção contra a mutação "remove o Sync, mantém o
// emit").
type Hook func(step string) error

// Write grava data em path atomicamente, com a permissão perm no arquivo
// final. Um leitor concorrente ou uma subida depois de crash vê o conteúdo
// antigo inteiro ou o novo inteiro, nunca um meio-termo.
func Write(path string, data []byte, perm os.FileMode) error {
	return WriteWithHook(path, data, perm, nil)
}

// WriteWithHook é Write com o seam de teste (hook nil em produção).
//
// A permissão é aplicada no tmp, ANTES de o conteúdo existir no disco: criar
// com 0644 e fazer chmod depois deixaria uma janela em que um segredo fica
// legível por outro usuário da máquina. O OpenFile só respeita perm na
// CRIAÇÃO, então um tmp que sobrou de uma gravação interrompida é removido
// antes — senão herdaria o modo antigo.
func WriteWithHook(path string, data []byte, perm os.FileMode, hook Hook) error {
	return writeStream(path, perm, func(w io.Writer) error { _, err := w.Write(data); return err }, hook)
}

// WriteStream grava um snapshot por blocos, sem carregar o arquivo inteiro no heap.
func WriteStream(path string, perm os.FileMode, write func(io.Writer) error) error {
	return writeStream(path, perm, write, nil)
}

func writeStream(path string, perm os.FileMode, write func(io.Writer) error, hook Hook) error {
	tmp := path + ".tmp"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("atomicfile: remover sobra %s: %w", tmp, err)
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("atomicfile: criar %s: %w", tmp, err)
	}
	if err := step(hook, StepWrite, func() error {
		return write(f)
	}); err != nil {
		_ = f.Close()
		return fmt.Errorf("atomicfile: escrever %s: %w", tmp, err)
	}
	if err := step(hook, StepFsync, f.Sync); err != nil {
		_ = f.Close()
		return fmt.Errorf("atomicfile: fsync de %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("atomicfile: fechar %s: %w", tmp, err)
	}
	if err := step(hook, StepRename, func() error {
		return os.Rename(tmp, path)
	}); err != nil {
		return fmt.Errorf("atomicfile: rename de %s: %w", filepath.Base(path), err)
	}
	return step(hook, StepDirsync, func() error {
		return SyncDir(filepath.Dir(path))
	})
}

func step(hook Hook, name string, op func() error) error {
	if err := op(); err != nil {
		return err
	}
	if hook != nil {
		return hook(name)
	}
	return nil
}

// SyncDir força a durabilidade das entradas de um diretório — o que torna
// durável um rename, uma criação ou uma remoção feita nele.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("atomicfile: fsync do diretório %s: %w", dir, err)
	}
	return nil
}
