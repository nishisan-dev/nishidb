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

type ambiente struct {
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

func novoAmbiente(t *testing.T) *ambiente {
	t.Helper()
	raiz := t.TempDir()
	dados := filepath.Join(raiz, "dados")
	specs := filepath.Join(raiz, "datasets")
	for _, d := range []string{dados, specs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// O YAML de produção, com o nome do arquivo casando o dataset.
	yaml, err := os.ReadFile("../../testdata/radius.materialization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(specs, "radius_sessions.yaml"), yaml, 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := filepath.Join(raiz, "nishidb.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(configYAML, dados, specs)), 0o644); err != nil {
		t.Fatal(err)
	}
	return &ambiente{dir: raiz, config: cfg}
}

func (a *ambiente) roda(args ...string) int {
	a.stdout.Reset()
	a.stderr.Reset()
	return cli.Run(args, cli.CLIContext{
		Stdin:  strings.NewReader(""),
		Stdout: &a.stdout,
		Stderr: &a.stderr,
		Ctx:    context.Background(),
	})
}

func (a *ambiente) rodaComEntrada(entrada string, args ...string) int {
	a.stdout.Reset()
	a.stderr.Reset()
	return cli.Run(args, cli.CLIContext{
		Stdin:  strings.NewReader(entrada),
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
	a := novoAmbiente(t)

	if code := a.rodaComEntrada(corpus(t, 2000),
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	resumo := a.stderr.String()
	if !strings.Contains(resumo, "2000 lidos") {
		t.Errorf("resumo não reporta os eventos lidos:\n%s", resumo)
	}
	if !strings.Contains(resumo, "ev/s") {
		t.Errorf("resumo não reporta throughput:\n%s", resumo)
	}

	// O dataset foi FECHADO pelo ingest, então o query assume o lock.
	if code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
		`SELECT id, status FROM radius_sessions WHERE status = 'Stop'`); code != cli.ExitOK {
		t.Fatalf("query = %d\n%s", code, a.stderr.String())
	}

	saida := a.stdout.String()
	if saida == "" {
		t.Fatal("query não devolveu linha nenhuma")
	}
	linhas := strings.Split(strings.TrimSuffix(saida, "\n"), "\n")
	for i, l := range linhas {
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
	t.Logf("%d linhas NDJSON válidas", len(linhas))
}

// Dois processos no mesmo dataset: o segundo falha com mensagem acionável. É o
// cenário que o lock existe para cobrir, e o CLI é onde ele acontece.
func TestDoisProcessosNoMesmoDataset(t *testing.T) {
	a := novoAmbiente(t)
	if code := a.rodaComEntrada(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest inicial = %d\n%s", code, a.stderr.String())
	}

	// Segura o dataset como um "outro processo" faria.
	st, compilado := abreDireto(t, a)
	defer st.Close()
	_ = compilado

	code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
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
	a := novoAmbiente(t)
	a.rodaComEntrada(`{"id":"A","_SEQ":1}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
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
	a := novoAmbiente(t)
	a.rodaComEntrada(`{"id":"A","_SEQ":1}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
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
	a := novoAmbiente(t)
	code := a.rodaComEntrada(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n",
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
	a := novoAmbiente(t)
	code := a.rodaComEntrada(corpus(t, 50),
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
	a := novoAmbiente(t)
	entrada := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		`{isso nao e json}`,
		`{"_SEQ":1,"Acct-Status-Type":"Stop"}`, // sem key
		`{"id":"B","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	if code := a.rodaComEntrada(entrada,
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
	a := novoAmbiente(t)
	casos := []struct {
		nome string
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
		t.Run(tc.nome, func(t *testing.T) {
			if code := a.roda(tc.args...); code != cli.ExitUsageError {
				t.Errorf("code = %d, want %d\n%s", code, cli.ExitUsageError, a.stderr.String())
			}
			if !strings.Contains(a.stderr.String(), tc.msg) {
				t.Errorf("stderr = %q, want conter %q", a.stderr.String(), tc.msg)
			}
		})
	}
}

func TestHelpSaiComSucesso(t *testing.T) {
	a := novoAmbiente(t)
	if code := a.roda("--help"); code != cli.ExitOK {
		t.Errorf("--help = %d, want 0", code)
	}
	// O contrato de exit codes é documentado, porque é o que um script usa.
	for _, quer := range []string{"EXIT CODES", "0  sucesso", "5  resultado truncado"} {
		if !strings.Contains(a.stdout.String(), quer) {
			t.Errorf("--help não documenta %q", quer)
		}
	}
}
