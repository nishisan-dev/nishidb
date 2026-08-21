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

// ---------------------------------------------------------------------------
// Task 6 — as duas janelas do D5, montadas em disco
// ---------------------------------------------------------------------------

const (
	// rowsDaAposentadoria é bem menor que o corpus da varredura porque o que
	// estes testes precisam ter em disco é outra coisa: não uma compaction
	// longa o bastante para o sinal cair no meio dela, e sim um segmento
	// SELADO com manifest que o liste. Com segmentos de 4 KiB isto dá algumas
	// dezenas de arquivos, e a conferência row a row do critério de aceite
	// custa milissegundos.
	rowsDaAposentadoria = 600
	// segmentoAposentavel é sempre o 1: ele é o primeiro a ser selado, logo é
	// o único que se pode afirmar não-corrente sem consultar o Stats — e a
	// decisão D6 proíbe eleger o corrente.
	segmentoAposentavel = 1
	// sufixoAposentadoNoTeste repete o sufixo em vez de importar a constante
	// de produção DE PROPÓSITO: o que estes testes afirmam é que o Open repara
	// um nome específico deixado em disco por um processo que já morreu, e um
	// oráculo que segue a constante concordaria com ela mesmo depois de alguém
	// trocá-la — o dataset da versão anterior é que ficaria sem reparo.
	sufixoAposentadoNoTeste = ".retired"
)

func chaveDaAposentadoria(i int) string { return fmt.Sprintf("ap-%05d", i) }

// datasetComSegmentoSelado monta um dataset FECHADO, com manifest gravado e
// mais de um segmento — a precondição comum a todas as janelas do D5, porque
// o arquivo que a aposentadoria renomeia é sempre um segmento selado.
func datasetComSegmentoSelado(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	s := audOpen(t, Options{Dir: dir, SegmentSize: segmentoDoKill9})
	for i := 0; i < rowsDaAposentadoria; i++ {
		k := chaveDaAposentadoria(i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint da montagem: %v", err)
	}
	if got := s.Stats().SegmentsTotal; got < 2 {
		t.Fatalf("a montagem ficou com %d segmento(s): sem um SELADO não há o que aposentar", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close da montagem: %v", err)
	}
	return dir
}

// exigeCorpusDaAposentadoriaIntacto confere TODA key do corpus, uma a uma,
// com payload e sequence. Amostragem não serve aqui: o modo de falha desta
// tarefa é a row que ficou apontando para um arquivo REMOVIDO, e ela é uma só
// entre centenas — um sorteio a perde e devolve verde.
func exigeCorpusDaAposentadoriaIntacto(t *testing.T, s *Store, quando string) {
	t.Helper()
	for i := 0; i < rowsDaAposentadoria; i++ {
		k := chaveDaAposentadoria(i)
		rec, _, ok, err := s.GetByKey([]byte(k), nil)
		if err != nil || !ok {
			t.Fatalf("%s: a row %q sumiu (ok=%v, err=%v)", quando, k, ok, err)
		}
		if rec.Sequence != int64(i) {
			t.Fatalf("%s: a row %q voltou com sequence %d, gravada com %d", quando, k, rec.Sequence, i)
		}
		if want := statusPayload(t, s, k, "Start", int64(i)); !bytes.Equal(rec.Payload, want) {
			t.Fatalf("%s: o payload da row %q mudou", quando, k)
		}
	}
}

// TestCrashEntreORenameEOCheckpointRestauraOSegmento cobre a única janela em
// que a aposentadoria produz estado INCONSISTENTE: o manifest ainda lista o
// segmento e o arquivo já não tem nome canônico. Sem o reparo, o Open sai com
// ErrDatasetCorrupt (store_recovery.go: "segmento do manifest ausente") num
// dataset que está inteiro em disco — a decisão D5 renomeia justamente para
// que os bytes continuem lá até o manifest concordar, e de nada adianta
// mantê-los se ninguém sabe devolvê-los ao nome que o manifest nomeia.
func TestCrashEntreORenameEOCheckpointRestauraOSegmento(t *testing.T) {
	dir := datasetComSegmentoSelado(t)
	canonico := audSegPath(dir, segmentoAposentavel)
	antes, err := os.Stat(canonico)
	if err != nil {
		t.Fatalf("stat do segmento a aposentar: %v", err)
	}
	if err := os.Rename(canonico, canonico+sufixoAposentadoNoTeste); err != nil {
		t.Fatalf("simular o rename da aposentadoria: %v", err)
	}

	s := openStore(t, Options{Dir: dir, SegmentSize: segmentoDoKill9})
	depois, err := os.Stat(canonico)
	if err != nil {
		t.Fatalf("o segmento %d não foi restaurado: %v", segmentoAposentavel, err)
	}
	if depois.Size() != antes.Size() {
		t.Errorf("o segmento restaurado tem %d bytes e tinha %d: o reparo não devolveu o mesmo arquivo",
			depois.Size(), antes.Size())
	}
	if _, err := os.Stat(canonico + sufixoAposentadoNoTeste); !os.IsNotExist(err) {
		t.Errorf("o nome aposentado sobreviveu ao reparo (err=%v): o próximo Open o veria como órfão e o removeria", err)
	}
	exigeCorpusDaAposentadoriaIntacto(t, s, "após restaurar o segmento renomeado")
}

// TestRetiredOrfaoNaoListadoEhRemovidoNoOpen cobre a outra janela: o crash
// entre o checkpoint e o unlink deixa em disco bytes que NENHUM manifest
// referencia mais. Sem a varredura, a compaction devolveria menos disco a
// cada ciclo do que promete, e o resíduo é permanente — o ListSegments ignora
// o nome não canônico de propósito, então nem o checkpoint nem o Stats
// voltariam a mencionar o arquivo para alguém notar.
func TestRetiredOrfaoNaoListadoEhRemovidoNoOpen(t *testing.T) {
	dir := datasetComSegmentoSelado(t)
	// O órfão é uma cópia fiel de um segmento real: é isto que o rename da
	// aposentadoria deixa para trás, e um arquivo de brinquedo não provaria
	// que o reparo remove o que ele de fato encontra.
	bytesDoSegmento, err := os.ReadFile(audSegPath(dir, segmentoAposentavel))
	if err != nil {
		t.Fatalf("ler o segmento de origem: %v", err)
	}
	orfao := audSegPath(dir, 9999) + sufixoAposentadoNoTeste
	if err := os.WriteFile(orfao, bytesDoSegmento, 0o644); err != nil {
		t.Fatalf("plantar o órfão: %v", err)
	}

	s := openStore(t, Options{Dir: dir, SegmentSize: segmentoDoKill9})
	if _, err := os.Stat(orfao); !os.IsNotExist(err) {
		t.Errorf("o .retired órfão sobreviveu ao Open (err=%v): são %d bytes que ninguém referencia e nada mais remove",
			err, len(bytesDoSegmento))
	}
	exigeCorpusDaAposentadoriaIntacto(t, s, "após remover o órfão não listado")
}

// TestRetiredComOCanonicoPresenteEhDescartadoEmVezDeSobrescrever fixa o ramo
// DEFENSIVO do reparo, e o registro é honesto: a sequência do D5 não produz
// este estado, porque o rename cria o nome aposentado e apaga o canônico no
// mesmo passo atômico. O ramo existe porque a alternativa é pior — restaurar
// por rename incondicional SOBRESCREVE o arquivo que o manifest nomeia, e o
// candidato a sobrescrevê-lo é justamente um resíduo antigo. Trocar o
// segmento vivo por um resíduo é a única forma de este reparo PERDER dado.
func TestRetiredComOCanonicoPresenteEhDescartadoEmVezDeSobrescrever(t *testing.T) {
	dir := datasetComSegmentoSelado(t)
	canonico := audSegPath(dir, segmentoAposentavel)
	vivo, err := os.ReadFile(canonico)
	if err != nil {
		t.Fatalf("ler o segmento vivo: %v", err)
	}
	if err := os.WriteFile(canonico+sufixoAposentadoNoTeste, []byte("resíduo de um ciclo anterior"), 0o644); err != nil {
		t.Fatalf("plantar o resíduo ao lado do canônico: %v", err)
	}

	s := openStore(t, Options{Dir: dir, SegmentSize: segmentoDoKill9})
	depois, err := os.ReadFile(canonico)
	if err != nil {
		t.Fatalf("o segmento vivo sumiu: %v", err)
	}
	if !bytes.Equal(vivo, depois) {
		t.Fatalf("o reparo sobrescreveu o segmento %d com o resíduo: %d bytes viraram %d",
			segmentoAposentavel, len(vivo), len(depois))
	}
	if _, err := os.Stat(canonico + sufixoAposentadoNoTeste); !os.IsNotExist(err) {
		t.Errorf("o resíduo sobreviveu ao Open (err=%v)", err)
	}
	exigeCorpusDaAposentadoriaIntacto(t, s, "após descartar o resíduo ao lado do canônico")
}

// ---------------------------------------------------------------------------
// Task 6 — kill -9 DENTRO de cada janela do D5
// ---------------------------------------------------------------------------

const (
	// envAposentadoriaKill9 e envJanelaDoKill9 levam ao processo filho o
	// dataset e o passo em que ele deve parar de existir. São variáveis de
	// ambiente pela mesma razão do envKill9Compaction: o filho é o PRÓPRIO
	// binário de teste reexecutado, e uma flag nova vazaria para a suíte
	// inteira.
	envAposentadoriaKill9 = "NISHIDB_APOSENTADORIA_KILL9_DIR"
	envJanelaDoKill9      = "NISHIDB_APOSENTADORIA_KILL9_JANELA"
	// Os dois passos que abrem janela. Matar o processo fora deles não
	// exercita nada: antes do rename o dataset está íntegro e listado, e
	// depois do unlink já não há aposentadoria em curso.
	passoDoRename               = "rename"
	passoDoCheckpointDoManifest = "checkpointDoManifest"
)

// aposentaAteAJanela é o corpo do processo FILHO: ingere, confirma o estado
// com um checkpoint, compacta o segmento selado e entra na aposentadoria —
// onde para de progredir no passo pedido, avisa o pai e espera o sinal. Nunca
// retorna por bem: se o SIGKILL não chegar, ele falha, e o pai vê o código de
// saída errado em vez de um verde inventado.
func aposentaAteAJanela(t *testing.T, dir, janela, marcador string) {
	t.Helper()
	s, err := Open(Options{Dir: dir, SegmentSize: segmentoDoKill9})
	if err != nil {
		t.Fatalf("Open no filho: %v", err)
	}
	for i := 0; i < rowsDaAposentadoria; i++ {
		k := chaveDaAposentadoria(i)
		mustPut(t, s, k, int64(i), statusPayload(t, s, k, "Start", int64(i)), nil)
	}
	// É este checkpoint que define "estado confirmado" (§10): o manifest
	// gravado aqui é o contrato que o SIGKILL não pode quebrar.
	if err := s.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint no filho: %v", err)
	}
	if _, _, _, err := s.compactaSegmento(context.Background(), segmentoAposentavel); err != nil {
		t.Fatalf("compactaSegmento no filho: %v", err)
	}
	s.SetAposentadoriaTraceForTest(func(passo string) {
		if passo != janela {
			return
		}
		if err := os.WriteFile(marcador, []byte(passo), 0o644); err != nil {
			t.Fatalf("gravar o marcador da janela %q: %v", passo, err)
		}
		// O processo fica aqui até morrer. O sleep em laço, e não um select{}
		// vazio, porque o runtime abate um programa cujas goroutines estão
		// TODAS bloqueadas — o filho morreria de deadlock detectado em vez de
		// pelo sinal, e o pai leria um código de saída que não é o do kill.
		prazo := time.Now().Add(60 * time.Second)
		for time.Now().Before(prazo) {
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatalf("o filho parou na janela %q e o SIGKILL não chegou", passo)
	})
	if err := s.aposentaSegmento(segmentoAposentavel); err != nil {
		t.Fatalf("aposentaSegmento no filho: %v", err)
	}
	t.Fatalf("a aposentadoria terminou sem passar pela janela %q: o teste não exercitou janela nenhuma", janela)
}

// kill9NaJanelaDaAposentadoria mata o processo DENTRO de uma das duas janelas
// do D5 e exige que o dataset volte inteiro pela reabertura. É o item 3 do
// critério de aceite da Task 6, no molde do
// TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado (internal/cli): processo
// filho de verdade, SIGKILL de verdade, sem defer, sem Close e sem chance de
// o programa arrumar a casa antes de morrer.
//
// O que ele cobre e o teste de unidade do reparo não cobre: lá o estado em
// disco é MONTADO pelo teste, aqui ele é PRODUZIDO pelo código sob teste. Um
// reparo aferido só contra a montagem concorda com a ideia que o teste tem da
// aposentadoria, não com o que ela deixa em disco.
func kill9NaJanelaDaAposentadoria(t *testing.T, janela string) {
	t.Helper()
	if dir := os.Getenv(envAposentadoriaKill9); dir != "" {
		aposentaAteAJanela(t, dir, os.Getenv(envJanelaDoKill9), filepath.Join(filepath.Dir(dir), "pronto"))
		return
	}
	if testing.Short() {
		t.Skip("reexecuta o binário de teste e mata o processo filho")
	}

	raiz := t.TempDir()
	dir := filepath.Join(raiz, "ds")
	marcador := filepath.Join(raiz, "pronto")

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), envAposentadoriaKill9+"="+dir, envJanelaDoKill9+"="+janela)
	var saida strings.Builder
	cmd.Stdout = &saida
	cmd.Stderr = &saida
	if err := cmd.Start(); err != nil {
		t.Fatalf("iniciar o filho: %v", err)
	}
	falha := func(formato string, args ...any) {
		t.Helper()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf(formato+"\nsaída do filho:\n%s", append(args, saida.String())...)
	}
	if !esperaPor(120*time.Second, func() bool { _, err := os.Stat(marcador); return err == nil }) {
		falha("o filho não chegou à janela %q", janela)
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

	// O estado em disco tem de ser o da janela, e não o de outra: sem esta
	// conferência o teste passaria com o processo morto em qualquer lugar, que
	// é precisamente o que ele existe para não fazer.
	canonico := audSegPath(dir, segmentoAposentavel)
	aposentado := canonico + sufixoAposentadoNoTeste
	if _, err := os.Stat(canonico); !os.IsNotExist(err) {
		t.Fatalf("o segmento %d ainda tem o nome canônico (err=%v): o kill não caiu depois do rename", segmentoAposentavel, err)
	}
	if _, err := os.Stat(aposentado); err != nil {
		t.Fatalf("o arquivo aposentado não está em disco (%v): o kill caiu depois do unlink e a janela não foi exercitada", err)
	}
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatalf("carregar o manifest deixado pelo kill: %v", err)
	}
	listado := false
	for _, si := range m.Segments {
		if si.ID == segmentoAposentavel {
			listado = true
		}
	}
	precisaRestaurar := janela == passoDoRename
	if listado != precisaRestaurar {
		t.Fatalf("o manifest %s o segmento %d na janela %q: não é o estado que a janela produz",
			map[bool]string{true: "ainda lista", false: "já não lista"}[listado], segmentoAposentavel, janela)
	}
	if m.RowCount < rowsDaAposentadoria {
		t.Fatalf("o checkpoint confirmou %d rows de %d: o corpus não chegou inteiro ao disco", m.RowCount, rowsDaAposentadoria)
	}

	// A reabertura roda o reparo e o recovery inteiro. Um Open que recuse o
	// dataset é reprovação, não observação.
	s := openStore(t, Options{Dir: dir, SegmentSize: segmentoDoKill9})
	if _, err := os.Stat(aposentado); !os.IsNotExist(err) {
		t.Errorf("o arquivo aposentado sobreviveu ao Open (err=%v)", err)
	}
	_, errCanonico := os.Stat(canonico)
	if precisaRestaurar && errCanonico != nil {
		t.Errorf("o segmento %d, que o manifest ainda lista, não foi restaurado: %v", segmentoAposentavel, errCanonico)
	}
	if !precisaRestaurar && errCanonico == nil {
		t.Errorf("o segmento %d voltou ao nome canônico sem que o manifest o listasse: o órfão foi restaurado em vez de removido",
			segmentoAposentavel)
	}
	exigeCorpusDaAposentadoriaIntacto(t, s, "após o kill -9 na janela "+janela)
	t.Logf("kill -9 na janela %q: %d rows confirmadas, dataset reaberto sem erro e conferido uma a uma", janela, m.RowCount)
}

// TestKill9EntreORenameEOCheckpointDevolveODatasetInteiro exercita a janela em
// que o manifest ainda lista um segmento que já não tem nome canônico — a
// única da aposentadoria em que o dataset em disco é INCONSISTENTE, e a que
// custaria ErrDatasetCorrupt num dataset íntegro se o reparo não existisse.
func TestKill9EntreORenameEOCheckpointDevolveODatasetInteiro(t *testing.T) {
	kill9NaJanelaDaAposentadoria(t, passoDoRename)
}

// TestKill9EntreOCheckpointEOUnlinkDevolveODatasetInteiro exercita a outra:
// o manifest já não nomeia o segmento e os bytes continuam em disco sob um
// nome que o ListSegments ignora. Nada volta a mencioná-los — sem a varredura
// do Open, cada ciclo de compaction devolveria menos disco do que promete, em
// silêncio.
func TestKill9EntreOCheckpointEOUnlinkDevolveODatasetInteiro(t *testing.T) {
	kill9NaJanelaDaAposentadoria(t, passoDoCheckpointDoManifest)
}
