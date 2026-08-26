package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// fdsApagadosEm devolve os caminhos que ESTE processo mantém abertos e que já
// foram removidos do diretório — os inodes presos.
//
// O oráculo é /proc/self/fd, e não statfs, por duas razões. A primeira é
// precisão: statfs mede o dispositivo inteiro, então qualquer outro processo
// escrevendo durante a rodada entra na conta, e o teste vira ruído. A segunda é
// que /proc funciona sobre tmpfs, onde o t.TempDir() do CI vive — um teste que
// exigisse disco de verdade só rodaria sob a tag `integration` e não protegeria
// o caminho no dia a dia.
//
// O que ele mede é a CAUSA (fd aberto sobre inode sem nome), e a causa é o que
// prende os blocos: o unlink do POSIX tira o nome, nunca o inode.
func fdsApagadosEm(t *testing.T, dir string) []string {
	t.Helper()
	entradas, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("/proc/self/fd indisponível (%v): este oráculo é de Linux, que é o alvo do produto", err)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatalf("Abs(%s): %v", dir, err)
	}
	var presos []string
	for _, e := range entradas {
		alvo, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			// O fd do próprio ReadDir some no meio da varredura: não é falha.
			continue
		}
		caminho, apagado := strings.CutSuffix(alvo, " (deleted)")
		if apagado && strings.HasPrefix(caminho, dir) {
			presos = append(presos, caminho)
		}
	}
	return presos
}

// TestSegmentoAposentadoFicaComInodePresoPorFdAberto é um teste de
// CARACTERIZAÇÃO: ele afirma o defeito, não o conserto.
//
// A cadeia é determinística, e não uma corrida. compactaSegmento lê o segmento
// que vai aposentar pelo s.segR.ScanFrom, e o ScanFrom chama r.file(seg), que
// ABRE e CACHEIA o descritor. O cache do Reader não tem despejo — o único
// delete(r.fds, id) vive dentro do Reader.Close, que só roda quando o Store
// inteiro fecha. Depois, aposentaSegmento faz os.Remove, e o unlink do POSIX
// remove o NOME e não o INODE.
//
// Resultado: os blocos de todo segmento aposentado seguem alocados enquanto o
// processo vive, e a compaction devolve disco ao LISTING do diretório sem
// devolver um byte ao filesystem.
//
// Nenhum instrumento do projeto via isso: dirBytes do harness, discoDosSegmentos
// do e2e e o Stats.StorageBytes somam o listing, e para os três o arquivo sumiu.
//
// A fase seguinte INVERTE esta asserção. Que ela exista primeiro é o que torna
// o conserto bisectável.
func TestSegmentoAposentadoFicaComInodePresoPorFdAberto(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	if presos := fdsApagadosEm(t, dir); len(presos) != 0 {
		t.Fatalf("antes de compactar já havia %d inode(s) preso(s): %v — a montagem não isola o que o teste mede", len(presos), presos)
	}

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado: sem aposentadoria não há o que este teste caracterize")
	}

	presos := fdsApagadosEm(t, dir)
	if len(presos) != res.SegmentosAposentados {
		t.Errorf("inodes presos = %d, quer %d (um por segmento aposentado): %v",
			len(presos), res.SegmentosAposentados, presos)
	}
}

// BenchmarkGetByRowID é a LINHA DE BASE do caminho quente de leitura, medida
// antes de a barreira de aposentadoria existir.
//
// A fase seguinte põe um RWMutex entre resolver a location e ler os bytes que
// ela nomeia, no par (Get, Read) que hoje é desprotegido. Um RLock não
// contendido custa poucos nanossegundos, mas "poucos" é raciocínio, e este
// projeto cobra medida: o número desta bancada é o que decide se a barreira
// fica como está ou se troca por um contador de geração com retry.
//
// Mede o GetByRowID SOZINHO — sem plano, sem predicado, sem projeção —, porque
// é exatamente o trecho que a barreira envolve. Um benchmark de query inteira
// afogaria o sinal no custo do resto.
func BenchmarkGetByRowID(b *testing.B) {
	s := openStore(b, Options{Dir: b.TempDir(), SegmentSize: 1 << 20})
	const keys = 512
	ids := make([]types.RowID, keys)
	for i := 0; i < keys; i++ {
		k := chaveDoCorpus(i)
		id, _ := mustPut(b, s, k, sequenceDoCorpus(0, i), payloadDoCorpus(b, s, k, 0), nil)
		ids[i] = id
	}
	if err := s.Checkpoint(); err != nil {
		b.Fatalf("Checkpoint: %v", err)
	}

	var buf []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, out, ok, err := s.GetByRowID(ids[i%keys], buf)
		buf = out
		if err != nil || !ok {
			b.Fatalf("GetByRowID(%d): ok=%v err=%v", ids[i%keys], ok, err)
		}
		if len(rec.Payload) == 0 {
			b.Fatal("payload vazio: a bancada estaria medindo uma leitura que não leu")
		}
	}
}
