package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
)

// O CLI é o entregável demonstrável do M1: é por ele que tudo o que foi
// construído existe para o usuário. Os testes rodam a função Run direto — o
// binário é só um main de três linhas em volta dela.

type env struct {
	dir    string
	config string
	stdout bytes.Buffer
	stderr bytes.Buffer
}

const configYAML = `
version: 1

# runtime é seção que o M1 NÃO implementa, presente de propósito: o arquivo
# do M3 não pode ser inválido para o binário do M1.
runtime:
  memoryTarget: 1GiB
server:
  enabled: true
  port: 9142
  # server.tls ganhou FORMA validada na Fase 11 (débito zero de
  # "aceita e ignorada em silêncio"): enabled: true sem os dois arquivos é
  # recusado no parse. O M1 continua sem ATIVAR TLS nenhum — só a validação
  # de forma chegou — mas o valor aqui precisa ser um bloco válido para este
  # fixture continuar representando um arquivo real.
  tls:
    enabled: true
    certificateFile: /etc/nishidb/tls/server.crt
    privateKeyFile: /etc/nishidb/tls/server.key

storage:
  path: %s
  segmentSize: 8MiB
  checkpoint:
    interval: 30s

datasets:
  directory: %s

query:
  defaultMaxRows: 100000
  defaultTimeout: 30s
`

func newEnv(t *testing.T) *env {
	t.Helper()
	raiz := t.TempDir()
	data := filepath.Join(raiz, "dados")
	specs := filepath.Join(raiz, "datasets")
	for _, d := range []string{data, specs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// O YAML de produção, com o nome do arquivo casando o dataset. Ele declara
	// `schemaName: telecom`.
	//
	// Todo fixture que ACRESCENTA um dataset a este mesmo diretório — e são
	// vários, espalhados pelos testes do pacote — declara o PRÓPRIO nome como
	// schemaName, e não `telecom`: dois datasets no mesmo schema são recusados
	// na carga (config.ValidateSchemaUniqueness), e um fixture que repetisse o
	// schema daqui falharia pela colisão em vez de pelo que o teste afirma.
	yaml, err := os.ReadFile("../../testdata/radius.materialization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(specs, "radius_sessions.yaml"), yaml, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := filepath.Join(raiz, "nishidb.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(configYAML, data, specs)), 0o644); err != nil {
		t.Fatal(err)
	}
	return &env{dir: raiz, config: cfg}
}

func (a *env) run(args ...string) int {
	a.stdout.Reset()
	a.stderr.Reset()
	return cli.Run(args, cli.Context{
		Stdin:  strings.NewReader(""),
		Stdout: &a.stdout,
		Stderr: &a.stderr,
		Ctx:    context.Background(),
	})
}

func (a *env) runWithEntry(entry string, args ...string) int {
	a.stdout.Reset()
	a.stderr.Reset()
	return cli.Run(args, cli.Context{
		Stdin:  strings.NewReader(entry),
		Stdout: &a.stdout,
		Stderr: &a.stderr,
		Ctx:    context.Background(),
	})
}

// corpus determinístico em NDJSON, uma line por evento.
func corpus(t *testing.T, n int) string {
	t.Helper()
	cfg := radiusgen.DefaultConfig()
	cfg.Seed, cfg.Entities = 7, 200
	eventos, err := radiusgen.GenerateCorpus(cfg, n)
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for _, ev := range eventos {
		sb.Write(ev)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// O caminho completo pelo binário: ingerir e consultar, com cada linha da
// saída validada como JSON e o conteúdo conferido contra o que foi ingerido.
func TestFluxoCompletoIngestEQuery(t *testing.T) {
	a := newEnv(t)

	if code := a.runWithEntry(corpus(t, 2000),
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	summary := a.stderr.String()
	if !strings.Contains(summary, "2000 lidos") {
		t.Errorf("resumo não reporta os eventos lidos:\n%s", summary)
	}
	if !strings.Contains(summary, "ev/s") {
		t.Errorf("resumo não reporta throughput:\n%s", summary)
	}

	// O dataset foi FECHADO pelo ingest, então o query assume o lock.
	if code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		`SELECT id, status FROM radius_sessions WHERE status = 'Stop'`); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}

	saida := a.stdout.String()
	if saida == "" {
		t.Fatal("query não devolveu linha nenhuma")
	}
	rows := strings.Split(strings.TrimSuffix(saida, "\n"), "\n")
	for i, l := range rows {
		var m map[string]json.RawMessage
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("linha %d não é JSON válido: %v\n%s", i, err, l)
		}
		if len(m) != 2 {
			t.Errorf("linha %d tem %d colunas, want 2", i, len(m))
		}
		var status string
		if err := json.Unmarshal(m["status"], &status); err != nil || status != "Stop" {
			t.Errorf("linha %d: status = %v (%v), want Stop", i, status, err)
		}
	}
	t.Logf("%d linhas NDJSON válidas", len(rows))
}

// Dois processos no mesmo dataset: o segundo falha com mensagem acionável. É o
// cenário que o lock existe para cobrir, e o CLI é onde ele acontece.
func TestDoisProcessosNoMesmoDataset(t *testing.T) {
	a := newEnv(t)
	if code := a.runWithEntry(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest inicial = %d\n%s", code, a.stderr.String())
	}

	// Segura o dataset como um "outro processo" faria.
	st, compilado := openDirect(t, a)
	defer st.Close()
	_ = compilado

	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		`SELECT id FROM radius_sessions`)
	if code != cli.ExitDataset {
		t.Fatalf("query com dataset travado = %d, want %d\n%s", code, cli.ExitDataset, a.stderr.String())
	}
	// Acionável significa dizer QUEM está segurando e ONDE: o diretório do
	// dataset e o pid do outro processo. Uma mensagem genérica de "travado"
	// deixaria o operador sem o próximo passo.
	msg := a.stderr.String()
	for _, quer := range []string{"outro processo", "radius_sessions", "pid"} {
		if !strings.Contains(msg, quer) {
			t.Errorf("mensagem não é acionável, falta %q: %q", quer, msg)
		}
	}
	if a.stdout.Len() != 0 {
		t.Errorf("stdout deveria ficar vazio, veio %q", a.stdout.String())
	}
}

// Erro de sintaxe: stderr traz posição e trecho, stdout fica VAZIO. Uma linha
// parcial seguida de erro quebraria quem consome a saída em streaming.
func TestErroDeSintaxeNaoEscreveNoStdout(t *testing.T) {
	a := newEnv(t)
	a.runWithEntry(`{"id":"A","_SEQ":1}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions WHERE status ==== 'Stop'")
	if code != cli.ExitQuery {
		t.Fatalf("code = %d, want %d\n%s", code, cli.ExitQuery, a.stderr.String())
	}
	if a.stdout.Len() != 0 {
		t.Errorf("stdout = %q, want vazio", a.stdout.String())
	}
	msg := a.stderr.String()
	for _, quer := range []string{"posição", "linha 1", "coluna", "^"} {
		if !strings.Contains(msg, quer) {
			t.Errorf("stderr não traz %q:\n%s", quer, msg)
		}
	}
}

// Coluna inexistente é erro de BIND, e também sai com o código da query.
func TestErroDeBindTemCodigoProprio(t *testing.T) {
	a := newEnv(t)
	a.runWithEntry(`{"id":"A","_SEQ":1}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT naoexiste FROM radius_sessions")
	if code != cli.ExitQuery {
		t.Fatalf("code = %d, want %d\n%s", code, cli.ExitQuery, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "não existe") {
		t.Errorf("stderr = %q", a.stderr.String())
	}
}

// --checkpoint-every 0 é permitido e NUNCA silencioso.
func TestCheckpointDesligadoAvisa(t *testing.T) {
	a := newEnv(t)
	code := a.runWithEntry(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions",
		"--input", "-", "--checkpoint-every", "0")
	if code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "AVISO") || !strings.Contains(msg, "DESLIGADO") {
		t.Errorf("desligar o checkpoint não avisou:\n%s", msg)
	}
	if !strings.Contains(msg, "memória") {
		t.Errorf("o aviso não diz a consequência:\n%s", msg)
	}
}

// O padrão é LIGADO: sem a flag, nenhum aviso, e o checkpoint acontece.
func TestCheckpointLigadoPorPadraoNaoAvisa(t *testing.T) {
	a := newEnv(t)
	code := a.runWithEntry(corpus(t, 50),
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	if code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	if strings.Contains(a.stderr.String(), "AVISO") {
		t.Errorf("avisou sem a flag:\n%s", a.stderr.String())
	}
}

// Eventos inválidos são descartados por categoria (política §101), sem parar a
// ingestão — e o resumo diz quantos e de qual tipo.
func TestEventosInvalidosSaoContabilizadosSemPararAIngestao(t *testing.T) {
	a := newEnv(t)
	entry := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		`{isso nao e json}`,
		`{"_SEQ":1,"Acct-Status-Type":"Stop"}`, // sem key
		`{"id":"B","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	if code := a.runWithEntry(entry,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "4 lidos, 2 aplicados") {
		t.Errorf("resumo = %q", msg)
	}
	if !strings.Contains(msg, "descartados por categoria") {
		t.Errorf("resumo não detalha os descartes:\n%s", msg)
	}
	if !strings.Contains(msg, "INVALID_JSON") || !strings.Contains(msg, "INVALID_KEY") {
		t.Errorf("resumo não nomeia as categorias:\n%s", msg)
	}
}

func TestErrosDeUso(t *testing.T) {
	a := newEnv(t)
	casos := []struct {
		name string
		args []string
		msg  string
	}{
		{"sem comando", nil, "USO"},
		{"comando desconhecido", []string{"explodir"}, "desconhecido"},
		{"ingest sem config", []string{"ingest", "--dataset", "x", "--input", "-"}, "obrigatórios"},
		{"query sem sql", []string{"query", "--config", a.config, "--dataset", "radius_sessions"},
			"exatamente uma query"},
		{"config inexistente", []string{"query", "--config", "/nao/existe.yaml",
			"--dataset", "radius_sessions", "SELECT id FROM radius_sessions"}, "configuração"},
		{"dataset inexistente", []string{"query", "--config", a.config,
			"--dataset", "fantasma", "SELECT id FROM fantasma"}, "fantasma"},
	}
	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
			if code := a.run(tc.args...); code != cli.ExitUsageError {
				t.Errorf("code = %d, want %d\n%s", code, cli.ExitUsageError, a.stderr.String())
			}
			if !strings.Contains(a.stderr.String(), tc.msg) {
				t.Errorf("stderr = %q, want conter %q", a.stderr.String(), tc.msg)
			}
		})
	}
}

func TestHelpSaiComSucesso(t *testing.T) {
	a := newEnv(t)
	if code := a.run("--help"); code != cli.ExitOK {
		t.Errorf("--help = %d, want 0", code)
	}
	// O contrato de exit codes é documentado, porque é o que um script usa.
	for _, quer := range []string{"EXIT CODES", "0  sucesso", "5  resultado truncado"} {
		if !strings.Contains(a.stdout.String(), quer) {
			t.Errorf("--help não documenta %q", quer)
		}
	}
}

// TestQueryLocalRecusaDiretorioComSchemaRepetido é a contraparte, no CLI, do
// TestDoisDatasetsNoMesmoSchemaRecusamASubida do servidor.
//
// Ela existe porque a regra é do DIRETÓRIO, não do processo: o `nishidb query`
// e o `nishidb ingest` leem os mesmos YAMLs que o servidor, e uma unicidade que
// valesse só na subida deixaria o comando local gravar num dataset que o
// servidor depois recusa abrir. O comando pede um dataset que NÃO é o
// duplicado, e mesmo assim é recusado — o defeito está na configuração, não na
// tabela que se quis ler.
func TestQueryLocalRecusaDiretorioComSchemaRepetido(t *testing.T) {
	a := newEnv(t)
	// O `radius_sessions` do harness declara `schemaName: telecom`; este
	// segundo dataset repete o schema de propósito.
	yml := `version: 1
name: colidente
schemaName: telecom
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`
	if err := os.WriteFile(filepath.Join(a.dir, "datasets", "colidente.yaml"), []byte(yml), 0o644); err != nil {
		t.Fatal(err)
	}

	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT * FROM radius_sessions")
	if code == cli.ExitOK {
		t.Fatalf("quer recusa: dois datasets no mesmo schema tornariam `FROM telecom.tabela` ambíguo\n%s",
			a.stdout.String())
	}
	for _, want := range []string{"schemaName", "telecom", "radius_sessions", "colidente"} {
		if !strings.Contains(a.stderr.String(), want) {
			t.Errorf("a recusa não nomeia %q — ela precisa do schema E dos DOIS datasets: %s", want, a.stderr.String())
		}
	}
}
