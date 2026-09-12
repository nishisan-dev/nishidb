package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// O `offsets` é a saída de três becos do M2: o `stop` do §101, o dataset com
// linhas e sem posição, e a troca legítima de origem. Sem ele, `stop` seria
// pílula de veneno e um restore parcial obrigaria a apagar o dataset.

func TestOffsetsShowEmDatasetSemPosicao(t *testing.T) {
	a := newEnv(t)
	code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions", "--show")
	if code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	if !strings.Contains(saida, "posição: ausente") {
		t.Errorf("saída = %q", saida)
	}
	if !strings.Contains(saida, "rows: 0") {
		t.Errorf("a saída não informa o RowCount, que é o sinal que decide se a ingestão sobe: %q", saida)
	}
}

// Um dataset com linhas e sem posição não sobe ingestão. O --show tem de
// dizer isso, e não só mostrar "ausente".
func TestOffsetsShowAvisaQuandoHaLinhasSemPosicao(t *testing.T) {
	a := newEnv(t)
	a.runWithEntry(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Start"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions", "--show"); code != cli.ExitOK {
		t.Fatalf("code = %d\nstdout:%s\nstderr:%s", code, a.stdout.String(), a.stderr.String())
	}
	saida := a.stdout.String()
	if !strings.Contains(saida, "rows: 1") {
		t.Errorf("saída = %q", saida)
	}
	if !strings.Contains(saida, "a ingestão NÃO sobe") {
		t.Errorf("a saída não avisa que a ingestão está bloqueada: %q", saida)
	}
}

// --set e --reset são destrutivos: pedem confirmação explícita. Uma reposição
// acidental reprocessa ou pula uma janela inteira do tópico.
func TestOffsetsSetExigeConfirmacao(t *testing.T) {
	a := newEnv(t)
	code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions", "--set", "0:100")
	if code != cli.ExitUsageError {
		t.Errorf("code = %d, quer %d (sem --yes)", code, cli.ExitUsageError)
	}
	if !strings.Contains(a.stderr.String(), "--yes") {
		t.Errorf("stderr = %q", a.stderr.String())
	}
	if _, err := os.Stat(a.pathOffsets()); !os.IsNotExist(err) {
		t.Error("gravou sem confirmação")
	}
}

func TestOffsetsSetGravaEDepoisEhLido(t *testing.T) {
	a := newEnv(t)
	code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions",
		"--set", "0:100,2:250", "--yes")
	if code != cli.ExitOK {
		t.Fatalf("code = %d\n%s", code, a.stderr.String())
	}

	if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions", "--show"); code != cli.ExitOK {
		t.Fatalf("show: code = %d\n%s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	for _, quer := range []string{"offset: 0=100", "offset: 2=250", "topic: radius-accounting"} {
		if !strings.Contains(saida, quer) {
			t.Errorf("saída não traz %q:\n%s", quer, saida)
		}
	}
}

func TestOffsetsSetRecusaEntradaInvalida(t *testing.T) {
	casos := []struct{ value, trecho string }{
		{"lixo", "particao:offset"},
		{"0:-1", "offset inválido"},
		{"-1:0", "partição inválida"},
		{"0:1,0:2", "repetida"},
	}
	for _, c := range casos {
		t.Run(c.value, func(t *testing.T) {
			a := newEnv(t)
			if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions",
				"--set", c.value, "--yes"); code != cli.ExitUsageError {
				t.Errorf("code = %d, quer %d", code, cli.ExitUsageError)
			}
			if !strings.Contains(a.stderr.String(), c.trecho) {
				t.Errorf("stderr = %q, quer conter %q", a.stderr.String(), c.trecho)
			}
		})
	}
}

// --reset descarta a posição: a próxima subida volta a valer o
// autoOffsetReset, o que é exatamente a saída para um restore parcial.
func TestOffsetsResetDescartaAPosicao(t *testing.T) {
	a := newEnv(t)
	if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions",
		"--set", "0:100", "--yes"); code != cli.ExitOK {
		t.Fatalf("set: %d\n%s", code, a.stderr.String())
	}
	if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions",
		"--reset", "earliest", "--yes"); code != cli.ExitOK {
		t.Fatalf("reset: %d\n%s", code, a.stderr.String())
	}
	if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions", "--show"); code != cli.ExitOK {
		t.Fatalf("show: %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "offsets: nenhum") {
		t.Errorf("a posição não foi descartada:\n%s", a.stdout.String())
	}
}

func TestOffsetsExigeUmaAcao(t *testing.T) {
	a := newEnv(t)
	if code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions"); code != cli.ExitUsageError {
		t.Errorf("code = %d, quer %d (nenhuma ação)", code, cli.ExitUsageError)
	}
	a2 := newEnv(t)
	if code := a2.run("offsets", "--config", a2.config, "--dataset", "radius_sessions",
		"--show", "--reset", "earliest"); code != cli.ExitUsageError {
		t.Errorf("code = %d, quer %d (duas ações)", code, cli.ExitUsageError)
	}
}

// §101 — a política `stop` deixou de ser chave sem leitor: o `ingest` a lê e
// para, com código próprio.
func TestIngestParaComInvalidEventStop(t *testing.T) {
	a := newEnv(t)
	a.declaraPolitica(t, "stop")

	entry := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Start"}`,
		`{isso nao e json}`,
		`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	code := a.runWithEntry(entry,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	if code != cli.ExitPolicyStopped {
		t.Errorf("code = %d, quer %d\n%s", code, cli.ExitPolicyStopped, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "errors.invalidEvent=stop") {
		t.Errorf("stderr não nomeia a política: %q", msg)
	}
	// O terceiro evento NÃO pode ter entrado: a parada é imediata.
	if !strings.Contains(msg, "2 lidos, 1 aplicados") {
		t.Errorf("a parada não foi imediata: %q", msg)
	}
}

// O default continua sendo log-and-skip: mudar o default silenciosamente
// mudaria o comportamento de todo YAML já escrito.
func TestPoliticaPadraoContinuaLogAndSkip(t *testing.T) {
	a := newEnv(t)
	entry := `{"id":"A","_SEQ":1,"Acct-Status-Type":"Start"}` + "\n" + `{isso nao e json}` + "\n"
	if code := a.runWithEntry(entry,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Errorf("code = %d, quer 0\n%s", code, a.stderr.String())
	}
}

// Débito 48: `mode: replace` era contado no materializer e sumia do resumo, e
// o total não fechava com as parcelas.
func TestResumoQuebraSubstituidos(t *testing.T) {
	a := newEnv(t)
	a.runWithEntry(`{"id":"A","_SEQ":1,"Acct-Status-Type":"Start"}`+"\n",
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	if !strings.Contains(a.stderr.String(), "substituídos") {
		t.Errorf("o resumo não quebra substituídos: %q", a.stderr.String())
	}
}

// TestOffsetsRecusaDiretorioComSchemaRepetido amarra que o `offsets` entra
// pela MESMA porta do servidor e do `query` — o config.LoadDatasetSpecs — e
// não por um schema.Load de um arquivo só.
//
// Sem este caso o comando reverte em silêncio: trocar o corpo por um
// `schema.Load(cfg.DatasetSpecPath(dataset))` deixa a suíte inteira verde,
// enquanto o README promete que a recusa vale aqui também. E o `offsets` é
// justamente o comando em que a omissão dói: ele grava no checkpoint de um
// dataset que o servidor depois recusa subir, o que é reposicionar às cegas.
//
// O dataset pedido NÃO é o duplicado, de propósito: o defeito está na
// configuração do diretório, não na tabela que se quis mexer.
func TestOffsetsRecusaDiretorioComSchemaRepetido(t *testing.T) {
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

	code := a.run("offsets", "--config", a.config, "--dataset", "radius_sessions", "--show")
	if code != cli.ExitUsageError {
		t.Fatalf("code = %d, quer %d: schema repetido no diretório tem de barrar o offsets\nstdout:%s\nstderr:%s",
			code, cli.ExitUsageError, a.stdout.String(), a.stderr.String())
	}
	for _, want := range []string{"schemaName", "telecom", "radius_sessions", "colidente"} {
		if !strings.Contains(a.stderr.String(), want) {
			t.Errorf("a recusa não nomeia %q — ela precisa do schema E dos DOIS datasets: %s", want, a.stderr.String())
		}
	}
	if _, err := os.Stat(a.pathOffsets()); !os.IsNotExist(err) {
		t.Error("o dataset foi aberto apesar da recusa")
	}
}

// TestOffsetsRecusaDatasetNaoDeclaradoNoDiretorio cobre a mensagem PRÓPRIA do
// caminho: o LoadDatasetSpecs devolve o diretório inteiro e quem diz que o
// dataset pedido não está lá é o `offsets`.
//
// Sem ela a mensagem não tem dono: uma implementação que carregasse o mapa e
// nunca consultasse a chave seguiria para o storage.Open e criaria o diretório
// de dados de um dataset que ninguém declarou.
func TestOffsetsRecusaDatasetNaoDeclaradoNoDiretorio(t *testing.T) {
	a := newEnv(t)
	code := a.run("offsets", "--config", a.config, "--dataset", "inexistente", "--show")
	if code != cli.ExitUsageError {
		t.Fatalf("code = %d, quer %d\nstdout:%s\nstderr:%s",
			code, cli.ExitUsageError, a.stdout.String(), a.stderr.String())
	}
	for _, want := range []string{"não há", "inexistente.yaml", "em datasets.directory"} {
		if !strings.Contains(a.stderr.String(), want) {
			t.Errorf("a recusa não traz %q — ela precisa dizer QUAL arquivo falta e ONDE: %s", want, a.stderr.String())
		}
	}
}

func (a *env) pathOffsets() string {
	return filepath.Join(a.dir, "dados", "radius_sessions", "checkpoint", "kafka.offsets")
}

// declaraPolitica acrescenta o bloco `errors` ao YAML do dataset. É append
// porque o fixture é o do §12 e tem de continuar intocado no repositório.
func (a *env) declaraPolitica(t *testing.T, value string) {
	t.Helper()
	path := filepath.Join(a.dir, "datasets", "radius_sessions.yaml")
	yml, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	yml = append(yml, []byte("\nerrors:\n  invalidEvent: "+value+"\n")...)
	if err := os.WriteFile(path, yml, 0o644); err != nil {
		t.Fatal(err)
	}
}
