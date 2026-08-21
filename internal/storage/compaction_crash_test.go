package storage

// A partir da Task 5 o copy-forward escreve no segmento corrente ao lado da
// ingestão, e o modo de falha da compaction deixou de ser "teste vermelho"
// para ser "estado errado depois de um kill -9": a varredura apenda cópias
// duráveis, com CRC batendo, SEM checkpoint entre elas — exatamente o formato
// de um record legítimo. Se essa janela estivesse errada, nenhuma asserção de
// unidade acusaria, porque o estrago só aparece na REABERTURA.
//
// Este arquivo é o crash test que o §53-55 cobra, no molde do
// TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado (internal/cli): processo
// filho de verdade, SIGKILL de verdade, sem defer, sem Close e sem chance de
// o programa arrumar a casa antes de morrer. A Task 6 acrescenta aqui as duas
// janelas do rename/unlink da decisão D5.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/segment"
)

const (
	// envKill9Compaction carrega o diretório do dataset para o processo
	// filho. É por variável de ambiente, e não por flag, porque o filho é o
	// PRÓPRIO binário de teste reexecutado (mesmo padrão do
	// TestBindCadeiaLongaNaoUsaPilhaProporcional): flag nova exigiria
	// registrá-la no pacote e ela vazaria para toda a suíte.
	envKill9Compaction = "NISHIDB_COMPACTION_KILL9_DIR"
	// rowsDoKill9 e segmentoDoKill9 são escolhidos juntos: com segmentos de
	// 4 KiB, este corpus enche algumas dezenas de arquivos SELADOS, que é o
	// que dá à compaction trabalho contínuo para o sinal interromper. Um
	// dataset de um segmento só não teria candidato nenhum (decisão D6) e o
	// teste passaria por vacuidade.
	rowsDoKill9     = 3000
	segmentoDoKill9 = 4096
)

func chaveDoKill9(i int) string { return fmt.Sprintf("k9-%05d", i) }

// esperaPor devolve true quando a condição passa a valer dentro do prazo.
// Espera por FATO, não por tempo: o sleep é só o intervalo entre duas
// leituras do mesmo observável, e o oráculo continua sendo a condição.
func esperaPor(prazo time.Duration, cond func() bool) bool {
	limite := time.Now().Add(prazo)
	for time.Now().Before(limite) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// discoDosSegmentos soma o tamanho dos arquivos de segmento pelo mesmo
// ListSegments do checkpoint. É o observável que denuncia a compaction em
// curso do lado de fora do processo: depois do checkpoint o filho não ingere
// mais nada, então todo byte novo no diretório é cópia em voo.
func discoDosSegmentos(t *testing.T, dataDir string) int64 {
	t.Helper()
	refs, err := segment.ListSegments(dataDir)
	if err != nil {
		t.Fatalf("listar %s: %v", dataDir, err)
	}
	var total int64
	for _, ref := range refs {
		total += ref.SizeBytes
	}
	return total
}

// compactaAteReceberOSinal é o corpo do processo FILHO: ingere, confirma o
// estado com um checkpoint, avisa o pai e passa a compactar em laço até
// morrer. Nunca retorna por bem — se o prazo estourar sem o sinal, ele falha,
// e o pai vê o código de saída errado em vez de um verde inventado.
func compactaAteReceberOSinal(t *testing.T, dir, marcador string) {
	t.Helper()
	s, err := Open(Options{Dir: dir, SegmentSize: segmentoDoKill9})
	if err != nil {
		t.Fatalf("Open no filho: %v", err)
	}
	for i := 0; i < rowsDoKill9; i++ {
		k := chaveDoKill9(i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	// É este checkpoint que define "estado confirmado" (§10): o manifest
	// gravado aqui é o contrato que o SIGKILL não pode quebrar. Sem ele o
	// teste mataria o processo antes de haver o que preservar.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint no filho: %v", err)
	}
	if err := os.WriteFile(marcador, []byte("pronto"), 0o644); err != nil {
		t.Fatalf("gravar o marcador de prontidão: %v", err)
	}

	// Daqui até o sinal só a compaction escreve, e ela NÃO checkpointa: cada
	// cópia é durável no segmento e invisível para o manifest, que é a janela
	// exata em que o pai quer matar o processo. O laço reabre a lista de
	// selados a cada volta porque as cópias empurram o segmento corrente para
	// frente, e o que era destino vira candidato na volta seguinte.
	prazo := time.Now().Add(30 * time.Second)
	var escritos int64
	for time.Now().Before(prazo) && escritos < 64<<20 {
		corrente := s.Stats().CurrentSegmentID
		for seg := uint32(1); seg < corrente; seg++ {
			_, _, n, err := s.compactaSegmento(context.Background(), seg)
			if err != nil {
				t.Fatalf("compactaSegmento(%d) no filho: %v", seg, err)
			}
			escritos += n
		}
	}
	t.Fatalf("o filho copiou %d bytes e o SIGKILL não chegou: sem o sinal no MEIO da compaction o teste não afirma nada", escritos)
}

// TestKill9NoMeioDaCompactionPreservaOEstadoConfirmado mata o processo com o
// copy-forward em voo e exige que o estado que o checkpoint confirmou volte
// inteiro — row a row, com sequence e payload conferidos.
//
// O que ele cobre e nenhum teste de unidade cobre: a compaction apenda no
// segmento corrente sem fsync por record (a divergência declarada no
// reapendaSeVivo) e sem checkpoint por cópia, então o que o kill deixa em
// disco é um segmento com rasgo no fim e um manifest que ainda nomeia os
// records de ORIGEM. A recovery tem de truncar o rasgo, replayar o que
// sobrou e chegar num dataset em que nenhuma row regrediu. Se a ordem de
// durabilidade estivesse errada — e ela vai ficar mais delicada na Task 6,
// que remove o arquivo de origem —, é aqui que apareceria.
//
// O sinal só é enviado depois que o diretório de dados CRESCE em relação ao
// tamanho medido logo após o checkpoint. Esperar por bytes novos é esperar
// por um fato: o filho parou de ingerir antes do marcador, então o
// crescimento é a cópia em voo e nada mais. Um sleep no lugar disso mataria o
// processo em algum ponto que a máquina escolhe, e o teste passaria a medir o
// escalonador junto com o código.
func TestKill9NoMeioDaCompactionPreservaOEstadoConfirmado(t *testing.T) {
	if dir := os.Getenv(envKill9Compaction); dir != "" {
		compactaAteReceberOSinal(t, dir, filepath.Join(filepath.Dir(dir), "pronto"))
		return
	}
	if testing.Short() {
		t.Skip("reexecuta o binário de teste e mata o processo filho")
	}

	raiz := t.TempDir()
	dir := filepath.Join(raiz, "ds")
	marcador := filepath.Join(raiz, "pronto")

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), envKill9Compaction+"="+dir)
	var saida strings.Builder
	cmd.Stdout = &saida
	cmd.Stderr = &saida
	if err := cmd.Start(); err != nil {
		t.Fatalf("iniciar o filho: %v", err)
	}
	// Toda falha antes do SIGKILL precisa enterrar o filho ANTES de ler a
	// saída: o exec.Cmd copia stdout numa goroutine própria, e ler o buffer
	// com o processo vivo é corrida de verdade — o -race a reportaria, e com
	// razão.
	falha := func(formato string, args ...any) {
		t.Helper()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf(formato+"\nsaída do filho:\n%s", append(args, saida.String())...)
	}

	if !esperaPor(60*time.Second, func() bool { _, err := os.Stat(marcador); return err == nil }) {
		falha("o filho não chegou a confirmar estado nenhum")
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		falha("carregar o manifest confirmado: %v", err)
	}
	confirmadas := m.RowCount
	if confirmadas < rowsDoKill9 {
		falha("o checkpoint confirmou %d rows de %d: o corpus não chegou inteiro ao disco", confirmadas, rowsDoKill9)
	}

	// O piso de crescimento não é cosmética. Medido com "qualquer byte novo",
	// o sinal chegava com 64 bytes de diferença — o header de um segmento
	// recém-rotacionado, ou seja a compaction ainda no primeiro append —, e um
	// kill nesse instante não exercita janela nenhuma. Com 64 KiB são
	// centenas de cópias já duráveis e algumas rotações atrás, que é o que
	// "no meio da compaction" quer dizer.
	const crescimentoMinimoDoKill9 = 64 << 10
	dataDir := filepath.Join(dir, "data")
	discoAntes := discoDosSegmentos(t, dataDir)
	if !esperaPor(60*time.Second, func() bool {
		return discoDosSegmentos(t, dataDir)-discoAntes >= crescimentoMinimoDoKill9
	}) {
		falha("o diretório %s não cresceu %d bytes desde o checkpoint: a compaction não estava em voo, e matar o processo agora não provaria nada",
			dataDir, crescimentoMinimoDoKill9)
	}

	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		falha("enviar o SIGKILL: %v", err)
	}
	estado, err := cmd.Process.Wait()
	if err != nil {
		t.Fatalf("aguardar o filho: %v", err)
	}
	if estado.ExitCode() != -1 {
		t.Fatalf("o filho saiu com %d em vez de morrer pelo sinal\nsaída do filho:\n%s", estado.ExitCode(), saida.String())
	}

	// A reabertura roda o recovery inteiro: manifest, verificação da cadeia,
	// tail scan truncando o rasgo que o SIGKILL deixou no segmento corrente.
	s := openStore(t, Options{Dir: dir, SegmentSize: segmentoDoKill9})
	if got := s.Stats().RowCount; got < confirmadas {
		t.Errorf("RowCount = %d após o kill -9, e o checkpoint já confirmava %d", got, confirmadas)
	}
	for i := 0; i < rowsDoKill9; i++ {
		k := chaveDoKill9(i)
		rec, _, ok, err := s.GetByKey([]byte(k), nil)
		if err != nil || !ok {
			t.Fatalf("a row %q, confirmada pelo checkpoint, não sobreviveu ao kill -9 no meio da compaction: (ok=%v, err=%v)", k, ok, err)
		}
		if rec.Sequence != int64(i) {
			t.Fatalf("a row %q voltou com sequence %d, confirmada com %d: o copy-forward interrompido regrediu o estado", k, rec.Sequence, i)
		}
		if want := statusPayload(t, s, k, "Start", int64(i)); !bytes.Equal(rec.Payload, want) {
			t.Fatalf("o payload da row %q mudou ao atravessar o kill -9: a cópia em voo publicou um estado que o dataset nunca confirmou", k)
		}
	}
	t.Logf("kill -9 com %d rows confirmadas e %d bytes de cópia já em disco; todas voltaram após o recovery",
		confirmadas, discoDosSegmentos(t, dataDir)-discoAntes)
}
