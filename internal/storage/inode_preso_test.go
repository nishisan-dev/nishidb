package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

// TestSegmentoAposentadoDevolveOsBlocosAoFilesystem é a inversão do teste de
// caracterização que este arquivo trazia antes: onde ele exigia um inode preso
// por segmento aposentado, agora se exige nenhum.
//
// O que mudou: aposentaSegmento passou a FECHAR o descritor cacheado antes do
// unlink. O unlink do POSIX remove o nome e nunca o inode, e o cache de fds do
// segment.Reader não tem despejo — sem o fechamento, os blocos de todo segmento
// aposentado seguiam alocados enquanto o processo vivesse, e a compaction
// devolvia disco ao LISTING do diretório sem devolver um byte ao dispositivo.
//
// A asserção é de IGUALDADE A ZERO e não de "diminuiu": o vazamento era
// determinístico, um inode por aposentadoria, então qualquer número acima de
// zero é o defeito de volta em escala menor.
func TestSegmentoAposentadoDevolveOsBlocosAoFilesystem(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, Options{Dir: dir, SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado: sem aposentadoria a asserção abaixo passaria por vacuidade")
	}

	if presos := fdsApagadosEm(t, dir); len(presos) != 0 {
		t.Errorf("%d inode(s) preso(s) depois de %d aposentadoria(s): %v",
			len(presos), res.SegmentosAposentados, presos)
	}
}

// TestCorpusContinuaLegivelDepoisDaAposentadoria existe porque o fechamento do
// descritor removeu o ACIDENTE que fazia a leitura funcionar.
//
// Antes, o pread sobre um inode sem nome devolvia os bytes certos porque o
// descritor vazado o mantinha vivo. Fechá-lo torna a leitura de uma location
// que ainda aponte para o segmento aposentado um erro de verdade — e é por isso
// que a barreira existe. Este teste cobra a outra metade: depois da
// aposentadoria, TODA key do corpus continua legível, o que só vale se a
// varredura republicou tudo o que estava vivo antes de o arquivo sumir.
//
// Confere key a key, sem amostragem, pelo mesmo motivo do
// exigeCorpusNaUltimaRodada: o modo de falha é UMA row apontando para o arquivo
// removido, e um sorteio a perde e devolve verde.
func TestCorpusContinuaLegivelDepoisDaAposentadoria(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado: o teste não exercitaria nada")
	}
	exigeCorpusNaUltimaRodada(t, s, keys, rodadas-1, "após a aposentadoria fechar os descritores")
}

// TestAposentadoriaEsperaALeituraEmVoo é o teste da BARREIRA, e o oráculo é
// uma relação de ACONTECE-DEPOIS, não um tempo de espera.
//
// Por que não dá para provar isto com uma corrida natural. Quando o segmento é
// aposentado, a varredura do compactaSegmento já republicou tudo o que estava
// vivo — é essa varredura que dá autoridade para afirmar que nenhuma location
// aponta para dentro do arquivo. Então um leitor que CHEGA depois nunca lê o
// segmento que vai sumir. A janela real é outra e é estreita: o leitor resolve
// a location ANTES de o copy-forward movê-la, e ainda não leu os bytes quando o
// descritor fecha. São nanossegundos entre duas linhas adjacentes, e um teste
// de carga a perde — verificado: com a barreira REMOVIDA, oito leitores
// varrendo o dataset inteiro durante uma compaction completa passaram três
// vezes seguidas. Teste que passa com e sem o conserto não é oráculo.
//
// O que este teste faz, então, é ocupar a barreira do lado do leitor e exigir
// que a aposentadoria só termine DEPOIS de ela ser liberada. Remover o
// aposMu.Lock do fechaODescritorEApaga faz a compaction terminar enquanto o
// leitor ainda está dentro, e a comparação de instantes reprova.
//
// A asserção é de ordem entre dois instantes registrados, e não "esperou N
// milissegundos": uma máquina lenta só aumenta a folga, nunca inverte a ordem.
func TestAposentadoriaEsperaALeituraEmVoo(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	corpusDeSegmentosSelados(t, s, 40, 30)

	// Ocupa a barreira como um leitor que resolveu a location e ainda não leu
	// os bytes. É exatamente o estado que o par (Get, Read) produz.
	s.aposMu.RLock()

	var (
		liberouEm  time.Time
		terminouEm time.Time
		res        ResultadoCompactacao
		errComp    error
		fim        = make(chan struct{})
	)
	go func() {
		defer close(fim)
		res, errComp = s.CompactaUmaVez(context.Background(), 0.5)
		terminouEm = time.Now()
	}()

	// Dá à compaction tempo de chegar até a seção crítica. Se a barreira não
	// existir, ela atravessa e termina aqui dentro.
	select {
	case <-fim:
		s.aposMu.RUnlock()
		t.Fatal("a compaction terminou com um leitor DENTRO da barreira: o fechaODescritorEApaga fechou o descritor debaixo de uma leitura em voo")
	case <-time.After(300 * time.Millisecond):
	}

	liberouEm = time.Now()
	s.aposMu.RUnlock()
	<-fim

	if errComp != nil {
		t.Fatalf("CompactaUmaVez: %v", errComp)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado: a barreira não foi exercitada")
	}
	if !terminouEm.After(liberouEm) {
		t.Errorf("a aposentadoria terminou em %v, ANTES de o leitor liberar a barreira em %v", terminouEm, liberouEm)
	}
}

// TestLeituraConcorrenteSobreviveAUmaCompactionInteira não é o teste da
// barreira — ver o godoc acima para por que uma corrida natural não a prova. É
// o teste de FUMAÇA do caminho inteiro: oito leitores varrendo por GetByRowID
// enquanto a compaction aposenta segmentos debaixo deles, exigindo que nenhuma
// leitura falhe e nenhuma volte vazia.
//
// Ele pega uma classe diferente de defeito, e por isso fica: um fechamento de
// descritor no lugar errado (fechar o segmento de DESTINO, ou fechar antes da
// varredura terminar) quebra este teste de imediato, sem depender de janela
// nenhuma.
func TestLeituraConcorrenteSobreviveAUmaCompactionInteira(t *testing.T) {
	s := openStore(t, Options{Dir: t.TempDir(), SegmentSize: 8 << 10})
	const keys, rodadas = 40, 30
	corpusDeSegmentosSelados(t, s, keys, rodadas)

	ids := make([]types.RowID, 0, keys)
	for i := 0; i < keys; i++ {
		rec, _, ok, err := s.GetByKey([]byte(chaveDoCorpus(i)), nil)
		if err != nil || !ok {
			t.Fatalf("GetByKey(%s): ok=%v err=%v", chaveDoCorpus(i), ok, err)
		}
		ids = append(ids, rec.RowID)
	}

	const leitores = 8
	var wg sync.WaitGroup
	pare := make(chan struct{})
	falhas := make(chan error, leitores)
	for l := 0; l < leitores; l++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf []byte
			for {
				select {
				case <-pare:
					return
				default:
				}
				for _, id := range ids {
					rec, out, ok, err := s.GetByRowID(id, buf)
					buf = out
					if err != nil {
						falhas <- fmt.Errorf("GetByRowID(%d): %w", id, err)
						return
					}
					if !ok || len(rec.Payload) == 0 {
						falhas <- fmt.Errorf("GetByRowID(%d): ok=%v payload=%d bytes", id, ok, len(rec.Payload))
						return
					}
				}
			}
		}()
	}

	res, err := s.CompactaUmaVez(context.Background(), 0.5)
	close(pare)
	wg.Wait()
	close(falhas)

	if err != nil {
		t.Fatalf("CompactaUmaVez: %v", err)
	}
	if res.SegmentosAposentados == 0 {
		t.Fatal("nenhum segmento aposentado durante a varredura dos leitores: nada foi exercitado")
	}
	for e := range falhas {
		t.Error(e)
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
