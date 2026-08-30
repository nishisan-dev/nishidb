package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// Um dataset com maxRows baixo, para provocar o truncamento.
const yamlTetoBaixo = `
version: 1
name: pequeno
materialization:
  key:
    source: payload
    field: id
    type: string
  mode: merge
  sequence:
    field: _SEQ
    type: int64
    strategy: greater
schema:
  include:
    - "*"
  unknownFields: include
  fields:
    id:
      type: string
    _SEQ:
      type: int64
    status:
      type: string
      index: true
query:
  maxRows: 3
  timeout: 30s
`

func comDatasetPequeno(t *testing.T) *ambiente {
	t.Helper()
	a := novoAmbiente(t)
	caminho := filepath.Join(a.dir, "datasets", "pequeno.yaml")
	if err := os.WriteFile(caminho, []byte(yamlTetoBaixo), 0o644); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&sb, `{"id":"K%02d","_SEQ":1,"status":"Stop"}`+"\n", i)
	}
	if code := a.rodaComEntrada(sb.String(),
		"ingest", "--config", a.config, "--dataset", "pequeno", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	return a
}

// Truncamento por maxRows tem código PRÓPRIO: as linhas emitidas são válidas,
// e o cliente precisa saber que o resultado não está completo. Truncar em
// silêncio entrega resposta parcial com cara de resposta completa.
func TestTruncamentoPorMaxRowsTemExitCodeProprio(t *testing.T) {
	a := comDatasetPequeno(t)

	code := a.roda("query", "--config", a.config, "--dataset", "pequeno",
		"SELECT id FROM pequeno")
	if code != cli.ExitTruncado {
		t.Fatalf("code = %d, want %d\n%s", code, cli.ExitTruncado, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "truncado") {
		t.Errorf("stderr = %q", a.stderr.String())
	}
	// As linhas já emitidas continuam válidas e são exatamente o teto.
	linhas := strings.Split(strings.TrimSuffix(a.stdout.String(), "\n"), "\n")
	if len(linhas) != 3 {
		t.Fatalf("%d linhas, want 3 (o maxRows do dataset)", len(linhas))
	}
	for i, l := range linhas {
		if !json.Valid([]byte(l)) {
			t.Errorf("linha %d inválida: %s", i, l)
		}
	}
}

// LIMIT explícito abaixo do teto NÃO é truncamento.
func TestLimitExplicitoNaoTrunca(t *testing.T) {
	a := comDatasetPequeno(t)
	if code := a.roda("query", "--config", a.config, "--dataset", "pequeno",
		"SELECT id FROM pequeno LIMIT 2"); code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if n := strings.Count(a.stdout.String(), "\n"); n != 2 {
		t.Errorf("%d linhas, want 2", n)
	}
}

// A flag --limit sobrepõe o LIMIT escrito na query: é o que o operador espera
// de uma flag explícita.
func TestFlagLimitSobrepoeOLimitDaQuery(t *testing.T) {
	a := comDatasetPequeno(t)

	if code := a.roda("query", "--config", a.config, "--dataset", "pequeno",
		"--limit", "1", "SELECT id FROM pequeno LIMIT 3"); code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if n := strings.Count(a.stdout.String(), "\n"); n != 1 {
		t.Errorf("%d linhas, want 1 (a flag vence)", n)
	}

	// Também funciona quando a query não tem LIMIT, e com ponto e vírgula.
	if code := a.roda("query", "--config", a.config, "--dataset", "pequeno",
		"--limit", "2", "SELECT id FROM pequeno;"); code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if n := strings.Count(a.stdout.String(), "\n"); n != 2 {
		t.Errorf("%d linhas, want 2", n)
	}
}

// Um literal que CONTÉM a palavra limit não pode ser confundido com a
// cláusula: cortar ali produziria uma query diferente da que o usuário
// escreveu.
func TestFlagLimitNaoCortaDentroDeLiteral(t *testing.T) {
	a := comDatasetPequeno(t)
	if code := a.rodaComEntrada(`{"id":"L1","_SEQ":1,"status":"sem limit aqui"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "pequeno", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}

	code := a.roda("query", "--config", a.config, "--dataset", "pequeno",
		"--limit", "5", `SELECT id FROM pequeno WHERE status = 'sem limit aqui'`)
	if code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if n := strings.Count(a.stdout.String(), "\n"); n != 1 {
		t.Errorf("%d linhas, want 1", n)
	}
}

// Nome de campo com espaço EXIGE aspas duplas, e aspas duplas eram o
// delimitador que o corte do --limit não conhecia: a query virava
// `SELECT id, "no LIMIT 2` — texto que o usuário nunca escreveu, com erro de
// sintaxe numa posição inventada.
func TestFlagLimitNaoCortaDentroDeIdentificadorCitado(t *testing.T) {
	a := comDatasetPequeno(t)
	if code := a.rodaComEntrada(`{"id":"Q1","_SEQ":1,"no limit aqui":"ok"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "pequeno", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}

	code := a.roda("query", "--config", a.config, "--dataset", "pequeno",
		"--limit", "2", `SELECT id, "no limit aqui" FROM pequeno`)
	if code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if n := strings.Count(a.stdout.String(), "\n"); n != 2 {
		t.Errorf("%d linhas, want 2\n%s", n, a.stdout.String())
	}
}

// Interrupção durante a ingestão: o processo sai com o código de sinal, e o
// que já foi aplicado ATÉ ALI sobrevive — o dataset reabre íntegro.
func TestIngestInterrompidoPreservaOQueFoiAplicado(t *testing.T) {
	a := novoAmbiente(t)

	var sb strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&sb, `{"id":"K%05d","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n", i)
	}

	// Contexto já cancelado: o comando encerra na primeira verificação, grava
	// o checkpoint do que aplicou e sai com o código de interrupção.
	ctx, cancela := context.WithCancel(context.Background())
	cancela()
	code := cli.Run([]string{"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"},
		cli.Ambiente{
			Stdin: strings.NewReader(sb.String()), Stdout: &a.stdout, Stderr: &a.stderr, Ctx: ctx,
		})
	if code != cli.ExitInterromp {
		t.Fatalf("code = %d, want %d\n%s", code, cli.ExitInterromp, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "interrompido") {
		t.Errorf("stderr = %q", a.stderr.String())
	}

	// O dataset reabre — o Close com quiesce devolveu o lock corretamente
	// mesmo no caminho de interrupção.
	if code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions"); code != cli.ExitOK {
		t.Fatalf("o dataset não reabriu após a interrupção: %d\n%s", code, a.stderr.String())
	}
}

// O ingest fecha o dataset ao terminar, e o comando seguinte assume o lock.
// Sem o quiesce do Close (F-1), esta sequência era uma corrida.
func TestComandosSequenciaisReabremODataset(t *testing.T) {
	a := novoAmbiente(t)
	for i := 0; i < 5; i++ {
		entrada := fmt.Sprintf(`{"id":"K%d","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n", i)
		if code := a.rodaComEntrada(entrada,
			"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
			t.Fatalf("ingest %d = %d\n%s", i, code, a.stderr.String())
		}
		if code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
			"SELECT id FROM radius_sessions"); code != cli.ExitOK {
			t.Fatalf("query %d = %d\n%s", i, code, a.stderr.String())
		}
		if n := strings.Count(a.stdout.String(), "\n"); n != i+1 {
			t.Errorf("após %d ingestões: %d linhas", i+1, n)
		}
	}
}

// A ingestão lê de ARQUIVO, não só de stdin.
func TestIngestDeArquivo(t *testing.T) {
	a := novoAmbiente(t)
	caminho := filepath.Join(a.dir, "eventos.ndjson")
	if err := os.WriteFile(caminho, []byte(corpus(t, 100)), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := a.roda("ingest", "--config", a.config, "--dataset", "radius_sessions",
		"--input", caminho); code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "100 lidos") {
		t.Errorf("resumo = %q", a.stderr.String())
	}
}

// Um dataset com uma grafia de índice que a D5 aposentou.
const yamlGrafiaDeIndiceObsoleta = `
version: 1
name: grafiaantiga
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    status:
      type: string
      index: hash
`

// TestDatasetComGrafiaDeIndiceObsoletaSaiComExitDataset é o gate de exit code
// da D5: sem ele, a recusa do schema poderia sair pelo default genérico do
// codigoDoErro (ExitUso, 1) sem que ninguém percebesse — e exit 1 manda o
// operador olhar a LINHA DE COMANDO, não o dataset. `index: hash`/`index:
// bitmap` são config RECUSADA na subida (o `Config recusa na subida` do
// README), a mesma categoria do manifest ilegível e do dataset travado, e por
// isso o código certo é o 3 (ExitDataset) — não o 4 (ExitWriter, que é writer
// em modo falho, categoria diferente), nem o 1 do default.
func TestDatasetComGrafiaDeIndiceObsoletaSaiComExitDataset(t *testing.T) {
	a := novoAmbiente(t)
	caminho := filepath.Join(a.dir, "datasets", "grafiaantiga.yaml")
	if err := os.WriteFile(caminho, []byte(yamlGrafiaDeIndiceObsoleta), 0o644); err != nil {
		t.Fatal(err)
	}
	code := a.roda("query", "--config", a.config, "--dataset", "grafiaantiga",
		"SELECT id FROM grafiaantiga")
	if code != cli.ExitDataset {
		t.Fatalf("code = %d, want %d (ExitDataset)\n%s", code, cli.ExitDataset, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "index: true") {
		t.Errorf("stderr não ensina a grafia nova: %q", a.stderr.String())
	}
}

// TestServidorComGrafiaDeIndiceObsoletaSaiComExitDataset é o MESMO gate,
// pela SUBIDA de verdade — "nishidb server", que é a subida de que o README
// fala ("a subida recusa com exit 3"). O gate acima só cobre "nishidb
// query"; sem este, a frase do README ficava sem trava sobre o comando que
// ela de fato descreve. server.Novo carrega e valida TODAS as specs do
// diretório antes de abrir qualquer dataset (internal/server/server.go), e
// roteia pelo MESMO codigoDoErro que "query" — o teste conta com isso, não
// com um caminho de erro próprio do server.
func TestServidorComGrafiaDeIndiceObsoletaSaiComExitDataset(t *testing.T) {
	a := novoAmbiente(t)
	caminho := filepath.Join(a.dir, "datasets", "grafiaantiga.yaml")
	if err := os.WriteFile(caminho, []byte(yamlGrafiaDeIndiceObsoleta), 0o644); err != nil {
		t.Fatal(err)
	}
	code := a.roda("server", "--config", a.config)
	if code != cli.ExitDataset {
		t.Fatalf("code = %d, want %d (ExitDataset)\n%s", code, cli.ExitDataset, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "index: true") {
		t.Errorf("stderr não ensina a grafia nova: %q", a.stderr.String())
	}
}
