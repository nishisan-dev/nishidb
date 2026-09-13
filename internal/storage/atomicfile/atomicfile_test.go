package atomicfile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestWriteOrdemDeDurabilidade(t *testing.T) {
	// fsync do tmp ANTES do rename e fsync do diretório DEPOIS — remover
	// qualquer um dos passos falha aqui.
	var trace []string
	path := filepath.Join(t.TempDir(), "state.bin")
	err := WriteWithHook(path, []byte("x"), 0o644, func(s string) error {
		trace = append(trace, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{StepWrite, StepFsync, StepRename, StepDirsync}
	if !slices.Equal(trace, want) {
		t.Fatalf("ordem violada:\nesperado %v\nobtido   %v", want, trace)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("sobrou o arquivo temporário")
	}
}

func TestWriteAplicaAPermissaoNoArquivoFinal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := Write(path, []byte("s"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("modo = %04o, quer 0600", got)
	}
}

// Um tmp que sobrou de uma gravação interrompida, criado com modo aberto, não
// pode emprestar esse modo ao arquivo novo: OpenFile só aplica perm na criação.
func TestWriteNaoHerdaOModoDeUmTmpQueSobrou(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path+".tmp", []byte("lixo"), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path+".tmp", 0o666); err != nil {
		t.Fatal(err)
	}
	if err := Write(path, []byte("s"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("modo = %04o, quer 0600", got)
	}
}

// A queda antes do rename preserva o conteúdo antigo inteiro; a queda depois
// do rename já entrega o novo. É o contrato que a tabela de usuários promete:
// uma troca de senha que sobreviveu ao crash não volta atrás.
func TestQuedaAntesDoRenamePreservaOAntigoEDepoisEntregaONovo(t *testing.T) {
	injected := errors.New("queda simulada")
	cases := []struct {
		failAfter string
		want      string
	}{
		{StepWrite, "antigo"},
		{StepFsync, "antigo"},
		{StepRename, "novo"},
		{StepDirsync, "novo"},
	}
	for _, c := range cases {
		t.Run(c.failAfter, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.bin")
			if err := Write(path, []byte("antigo"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := WriteWithHook(path, []byte("novo"), 0o600, func(s string) error {
				if s == c.failAfter {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) {
				t.Fatalf("erro = %v, quer a queda injetada", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, []byte(c.want)) {
				t.Errorf("conteúdo = %q, quer %q", got, c.want)
			}
		})
	}
}
