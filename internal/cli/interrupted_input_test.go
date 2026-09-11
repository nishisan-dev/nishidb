package cli_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// readerOfRows entrega UMA line por Read e, esgotadas as linhas, devolve
// errFinal. Um Read por line é o que torna determinístico o ponto em que o
// erro (ou o cancelamento de aoEntregarAUltima) alcança o laço de ingestão:
// com um strings.Reader o Scanner leria o arquivo inteiro de uma vez e não
// haveria "meio da entrada".
type readerOfRows struct {
	rows              []string
	i                 int
	errFinal          error
	aoEntregarAUltima func()
}

func (l *readerOfRows) Read(p []byte) (int, error) {
	if l.i >= len(l.rows) {
		return 0, l.errFinal
	}
	row := l.rows[l.i] + "\n"
	if len(p) < len(row) {
		return 0, fmt.Errorf("buffer de %d bytes menor que a linha de %d", len(p), len(row))
	}
	n := copy(p, row)
	l.i++
	if l.i == len(l.rows) && l.aoEntregarAUltima != nil {
		l.aoEntregarAUltima()
	}
	return n, nil
}

// runIngestWith executa o ingest com um reader e um contexto arbitrários — o
// helper runWithEntry só aceita string, e a entrada interrompida exige um
// reader que falhe no meio.
func runIngestWith(a *env, ctx context.Context, entry io.Reader) int {
	a.stdout.Reset()
	a.stderr.Reset()
	return cli.Run([]string{"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"},
		cli.Context{Stdin: entry, Stdout: &a.stdout, Stderr: &a.stderr, Ctx: ctx})
}

func manifestOf(t *testing.T, a *env) *checkpoint.Manifest {
	t.Helper()
	m, err := checkpoint.Load(filepath.Join(a.dir, "dados", "radius_sessions", "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatalf("lendo o manifest: %v", err)
	}
	return m
}

// O erro de leitura saía com exit 1 — "corrija a linha de comando" — e saía
// ANTES do teste de perda, então um evento que NÃO entrou por falha nossa
// desaparecia do código de saída. É o mesmo defeito que criou o 7: perda com
// código enganoso é o pior desfecho para quem opera por $?.
func TestErroDeLeituraNaoMascaraPerdaDeDado(t *testing.T) {
	a := newEnv(t)
	reader := &readerOfRows{
		rows: []string{
			`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
			eventWithNameOfFieldGigante(),
		},
		errFinal: errors.New("disco caiu no meio da leitura"),
	}

	code := runIngestWith(a, context.Background(), reader)

	if code != cli.ExitDataLoss {
		t.Errorf("code = %d, want %d (perda de dado, não erro de uso)\n%s",
			code, cli.ExitDataLoss, a.stderr.String())
	}
	msg := a.stderr.String()
	// O erro de leitura continua VISÍVEL: trocar o código enganoso por
	// silêncio seria a outra metade do mesmo defeito.
	if !strings.Contains(msg, "ENTRADA INTERROMPIDA") || !strings.Contains(msg, "disco caiu no meio da leitura") {
		t.Errorf("o erro de leitura sumiu do stderr:\n%s", msg)
	}
	if !strings.Contains(msg, "não aplicados por falha (perda de dado): STORAGE_ERROR=1") {
		t.Errorf("a perda apurada não aparece no resumo:\n%s", msg)
	}
	if !strings.Contains(msg, "NÃO entraram no dataset por falha") {
		t.Errorf("a linha que explica o exit 7 não foi impressa:\n%s", msg)
	}
}

// Sem nenhuma falha de evento, a entrada interrompida JÁ é perda: o que vinha
// depois do erro nunca foi ingerido, e a ação de quem recebe o código é a
// mesma do 7 — investigar e reprocessar —, não conferir a linha de comando.
func TestEntradaInterrompidaSemFalhaDeEventoAindaEPerda(t *testing.T) {
	a := newEnv(t)
	reader := &readerOfRows{
		rows:     []string{`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`},
		errFinal: errors.New("stdin fechou"),
	}

	code := runIngestWith(a, context.Background(), reader)

	if code != cli.ExitDataLoss {
		t.Errorf("code = %d, want %d\n%s", code, cli.ExitDataLoss, a.stderr.String())
	}
	if msg := a.stderr.String(); !strings.Contains(msg, "ENTRADA INTERROMPIDA") ||
		!strings.Contains(msg, "stdin fechou") {
		t.Errorf("stderr = %q", msg)
	}

	// O que ENTROU antes do erro é durabilizado do mesmo jeito que no caminho
	// de interrupção por sinal: o manifest conta a row aplicada, em vez de
	// deixá-la para o tail scan da recovery.
	if m := manifestOf(t, a); m.RowCount != 1 {
		t.Errorf("manifest.RowCount = %d, want 1 (o checkpoint final não rodou)", m.RowCount)
	}

	// E o dataset reabre com a row: o encerramento é limpo.
	if code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions"); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}
	if n := strings.Count(a.stdout.String(), "\n"); n != 1 {
		t.Errorf("%d rows, want 1\n%s", n, a.stdout.String())
	}
}

// A rota real mais provável do erro de leitura: uma linha maior que o teto do
// Scanner. O Scanner não sabe pular a linha — ele para —, então o resto do
// arquivo não entra: perda, não erro de uso.
func TestLinhaLongaDemaisInterrompeAEntradaComPerda(t *testing.T) {
	a := newEnv(t)
	entry := io.MultiReader(
		strings.NewReader(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n"),
		// Acima do teto de 8 MiB configurado no sc.Buffer, e sem \n.
		strings.NewReader(strings.Repeat("x", 9<<20)),
	)

	code := runIngestWith(a, context.Background(), entry)

	if code != cli.ExitDataLoss {
		t.Errorf("code = %d, want %d\n%s", code, cli.ExitDataLoss, a.stderr.String())
	}
	if msg := a.stderr.String(); !strings.Contains(msg, "ENTRADA INTERROMPIDA") ||
		!strings.Contains(msg, "token too long") {
		t.Errorf("stderr = %q", msg)
	}
}

// Interrupção por sinal também saía antes do teste de perda. O sinal é um
// desfecho que o operador PEDIU e já conhece; a perda é notícia que exige
// investigação — quando os dois acontecem, quem manda no $? é a notícia.
func TestInterrupcaoNaoApagaAPerdaJaApurada(t *testing.T) {
	a := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	reader := &readerOfRows{
		rows: []string{
			`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
			eventWithNameOfFieldGigante(),
			`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
		},
		errFinal: io.EOF,
		// O cancelamento chega junto com a ÚLTIMA linha: as duas primeiras já
		// passaram pelo materializador, e a falha do evento gigante já está
		// apurada quando o laço vê o contexto cancelado.
		aoEntregarAUltima: cancel,
	}
	defer cancel()

	code := runIngestWith(a, ctx, reader)

	if code != cli.ExitDataLoss {
		t.Errorf("code = %d, want %d (a perda vence o sinal)\n%s",
			code, cli.ExitDataLoss, a.stderr.String())
	}
	msg := a.stderr.String()
	// A interrupção continua visível no stderr — o código de saída escolhe o
	// desfecho mais grave, não apaga o outro.
	if !strings.Contains(msg, "interrompido") {
		t.Errorf("a interrupção sumiu do stderr:\n%s", msg)
	}
	if !strings.Contains(msg, "NÃO entraram no dataset por falha") {
		t.Errorf("a perda não foi reportada no caminho de interrupção:\n%s", msg)
	}
}
