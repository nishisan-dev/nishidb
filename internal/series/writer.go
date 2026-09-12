// Package series escreve a emissão periódica dos contadores: uma linha NDJSON
// por grupo, num arquivo com rotação por tamanho dentro do dataset.
//
// O diretório é fixo (schema.EmitDirName) e fica ao lado de data/ e spill/,
// pelas mesmas três razões do spill: cai sob o flock exclusivo do dataset, então
// nenhum outro processo confunde os arquivos; herda a política de filesystem do
// dataset; e o operador que dimensionou o disco não precisa dimensionar um
// segundo lugar.
//
// # Durabilidade
//
// NÃO há fsync, e é deliberado. Isto é telemetria: um crash perde a cauda da
// série, que é o desfecho CERTO — pagar fsync por linha para garantir amostras
// de gráfico custaria mais que o valor delas, e competiria com o writer pelo
// mesmo disco. É o mesmo argumento do spill do ORDER BY.
package series

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// bufSize é o buffer de escrita. Uma passada emite uma linha por grupo, e
// milhares de write(2) de cem bytes seriam syscall por linha.
const bufSize = 64 << 10

// Writer escreve linhas NDJSON com rotação por tamanho.
//
// Não é thread-safe: o dono é o laço de emissão, um por contador.
type Writer struct {
	dir      string
	name     string
	maxBytes int64
	keep     int

	f    *os.File
	size int64
	buf  []byte
}

// OpenWriter abre (ou cria) o arquivo corrente da série.
//
// O arquivo é aberto em APPEND: reiniciar o processo não pode apagar a série do
// período anterior. Um corrente que já está no teto — o processo caiu com ele
// cheio — NÃO é rotacionado aqui: a primeira escrita o rotaciona pela mesma
// conferência de sempre, e uma rotação na abertura seria um segundo caminho
// para o mesmo efeito. (Mutação: desligá-la não mudava teste nenhum.)
//
// A permissão é 0644, e não a 0600 do spill: aqui o conteúdo é telemetria que um
// coletor precisa ler, e não resultado de consulta de um usuário autenticado.
func OpenWriter(dir, name string, maxBytes int64, keep int) (*Writer, error) {
	if name == "" || strings.ContainsRune(name, os.PathSeparator) {
		return nil, fmt.Errorf("series: nome de arquivo inválido %q", name)
	}
	if maxBytes <= 0 || keep < 0 {
		return nil, fmt.Errorf("series: maxBytes %d e keep %d têm de ser positivos", maxBytes, keep)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("series: criar %s: %w", dir, err)
	}
	w := &Writer{dir: dir, name: name, maxBytes: maxBytes, keep: keep, buf: make([]byte, 0, bufSize)}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) open() error {
	f, err := os.OpenFile(w.path(0), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("series: abrir %s: %w", w.path(0), err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("series: stat de %s: %w", w.path(0), err)
	}
	w.f, w.size = f, info.Size()
	return nil
}

// path é o caminho do arquivo com o sufixo n; zero é o corrente.
func (w *Writer) path(n int) string {
	if n == 0 {
		return filepath.Join(w.dir, w.name)
	}
	return filepath.Join(w.dir, w.name+"."+strconv.Itoa(n))
}

// Write acrescenta uma linha, com o fim de linha do NDJSON.
//
// A rotação é conferida ANTES de escrever, e o teto vale sobre o que já está no
// arquivo: uma linha MAIOR que o teto é escrita de qualquer forma, porque
// rotacionar não a faria caber — o arquivo novo também não a comportaria — e
// recusá-la travaria a emissão do contador inteiro por causa de um grupo.
func (w *Writer) Write(line []byte) error {
	if w.f == nil {
		return errors.New("series: escritor fechado")
	}
	if w.size > 0 && w.size+int64(len(line))+1 > w.maxBytes {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	w.buf = append(w.buf, line...)
	w.buf = append(w.buf, '\n')
	w.size += int64(len(line)) + 1
	if len(w.buf) >= bufSize {
		return w.flushBuf()
	}
	return nil
}

// Flush esvazia o buffer no arquivo. O laço o chama ao fim de cada passada,
// para que a série não fique presa em memória entre intervalos.
func (w *Writer) Flush() error {
	if w.f == nil {
		return nil
	}
	return w.flushBuf()
}

func (w *Writer) flushBuf() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.f.Write(w.buf); err != nil {
		return fmt.Errorf("series: escrever em %s: %w", w.path(0), err)
	}
	// Trunca, não realoca: o buffer é do escritor e vive enquanto ele viver.
	w.buf = w.buf[:0]
	return nil
}

// rotate sela o corrente e desloca a série.
//
// O deslocamento é do fim para o começo — apaga o mais velho, move cada um para
// o sufixo seguinte, e só então renomeia o corrente. Na ordem inversa, cada
// rename sobrescreveria o arquivo que ainda não foi movido.
//
// O sufixo maior é o mais VELHO, que é a convenção que todo coletor de log
// espera: o arquivo sem sufixo é o corrente, e o número cresce para o passado.
func (w *Writer) rotate() error {
	if err := w.flushBuf(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("series: fechar %s: %w", w.path(0), err)
	}
	w.f = nil
	if w.keep == 0 {
		// Sem histórico a guardar, o corrente é descartado em vez de virar
		// .1 — que seria um arquivo a mais do que o operador declarou.
		if err := os.Remove(w.path(0)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("series: remover %s: %w", w.path(0), err)
		}
		return w.open()
	}
	// O mais velho não é removido antes: o rename para o último sufixo
	// SOBRESCREVE o arquivo que estava lá, e um Remove a mais só pareceria
	// fazer o descarte. Foi uma mutação que mostrou isso — desligar aquele
	// Remove não mudava teste nenhum.
	for n := w.keep - 1; n >= 0; n-- {
		err := os.Rename(w.path(n), w.path(n+1))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("series: rotacionar %s: %w", w.path(n), err)
		}
	}
	return w.open()
}

// Close esvazia o buffer e fecha o arquivo. Idempotente.
func (w *Writer) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.flushBuf()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	w.f = nil
	return err
}

// Cleanup apaga, na subida, os arquivos rotacionados acima do keep declarado.
//
// declared casa nome do arquivo corrente → keep. Baixar o keep no YAML tem de
// devolver o disco, e um arquivo órfão de uma configuração antiga não pode ficar
// lá para sempre sem ninguém o contar no teto.
//
// O que NÃO está declarado é DEIXADO EM PAZ, e isso é deliberado: apagar o que
// esta função não reconhece transformaria um erro de digitação no YAML — ou uma
// subida com um dataset temporariamente sem o bloco `emit` — em perda da série
// de outro contador. O preço é resíduo visível, que é o modo de falha barato.
func Cleanup(dir string, declared map[string]int) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		// Primeiro dataset a declarar emissão: o diretório nasce no OpenWriter.
		return nil
	}
	if err != nil {
		return fmt.Errorf("series: listar %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		base, n, ok := splitSuffix(e.Name())
		if !ok {
			continue
		}
		keep, declarado := declared[base]
		if !declarado || n <= keep {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("series: remover %s: %w", e.Name(), err)
		}
	}
	return nil
}

// splitSuffix reparte "s.ndjson.3" em ("s.ndjson", 3).
//
// O arquivo CORRENTE devolve ok=false, e é o que garante que a limpeza nunca o
// apague — quem garante isso é o Atoi falhando sobre a extensão ("ndjson"), não
// a faixa abaixo. A faixa recusa sufixo não positivo, que a rotação nunca
// produz: é validação de formato para um nome que só pode ter vindo de fora.
func splitSuffix(name string) (string, int, bool) {
	i := strings.LastIndexByte(name, '.')
	if i < 0 {
		return "", 0, false
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil || n < 1 {
		return "", 0, false
	}
	return name[:i], n, true
}
