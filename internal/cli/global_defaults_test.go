package cli_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// As chaves globais `query.defaultMaxRows` e `query.defaultTimeout` do
// nishidb.yaml eram parseadas e NUNCA lidas: o teto e o timeout efetivos vinham
// só do YAML do dataset, e como os padrões coincidiam (100000 e 30s) quem
// editasse a chave global não via efeito nem aviso. Além disso, o
// defaultMaxRows não era validado — negativo passava em silêncio.
//
// Agora as duas são o DEFAULT do dataset que omite a chave equivalente, e valor
// não positivo é recusado no carregamento. Estes testes exercem o caminho pelo
// binário, que é onde o operador vive.

// specWithoutSecaoQuery é um dataset que NÃO declara `query`: é o caso em que o
// default global vale.
const specWithoutSecaoQuery = `
version: 1
name: sem_query
schemaName: sem_query
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
`

// swapQueryGlobal regrava o bloco `query:` do nishidb.yaml do ambiente. O
// bloco é o ÚLTIMO do arquivo, então trocá-lo é truncar e reescrever.
func swapQueryGlobal(t *testing.T, a *env, block string) {
	t.Helper()
	b, err := os.ReadFile(a.config)
	if err != nil {
		t.Fatal(err)
	}
	corte := strings.Index(string(b), "\nquery:\n")
	if corte < 0 {
		t.Fatalf("nishidb.yaml sem bloco query:\n%s", b)
	}
	nw := string(b[:corte+1]) + block
	if err := os.WriteFile(a.config, []byte(nw), 0o644); err != nil {
		t.Fatal(err)
	}
}

// withDatasetWithoutQuery escreve o dataset sem seção `query` e ingere 20 rows.
func withDatasetWithoutQuery(t *testing.T, a *env, specExtra string) {
	t.Helper()
	path := filepath.Join(a.dir, "datasets", "sem_query.yaml")
	if err := os.WriteFile(path, []byte(specWithoutSecaoQuery+specExtra), 0o644); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for i := 0; i < 20; i++ {
		fmt.Fprintf(&sb, `{"id":"K%02d","_SEQ":1,"status":"Stop"}`+"\n", i)
	}
	if code := a.runWithEntry(sb.String(),
		"ingest", "--config", a.config, "--dataset", "sem_query", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
}

// O teto global vale para o dataset que omite query.maxRows: com
// defaultMaxRows: 3 a consulta trunca em 3 linhas e sai pelo código de
// truncamento. Antes, o global era ignorado e o dataset caía nos 100000 da
// constante interna — a consulta devolvia as 20 linhas com exit 0.
func TestDefaultMaxRowsGlobalValeParaDatasetQueOmite(t *testing.T) {
	a := newEnv(t)
	swapQueryGlobal(t, a, "query:\n  defaultMaxRows: 3\n  defaultTimeout: 30s\n")
	withDatasetWithoutQuery(t, a, "")

	code := a.run("query", "--config", a.config, "--dataset", "sem_query",
		"SELECT id FROM sem_query")
	if code != cli.ExitResultTruncated {
		t.Fatalf("code = %d, want %d (truncado pelo teto global)\n%s",
			code, cli.ExitResultTruncated, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSuffix(a.stdout.String(), "\n"), "\n")
	if len(rows) != 3 {
		t.Fatalf("%d linhas, want 3 (o defaultMaxRows global)", len(rows))
	}
	for i, l := range rows {
		if !json.Valid([]byte(l)) {
			t.Errorf("linha %d inválida: %s", i, l)
		}
	}
}

// O dataset que DECLARA a chave vence o global: o global é default, não teto.
// Sem esta separação, a correção anterior viraria outro tipo de mentira — a de
// um limite global que ninguém pediu e que o dataset não consegue afrouxar.
func TestDatasetQueDeclaraMaxRowsVenceOGlobal(t *testing.T) {
	a := newEnv(t)
	swapQueryGlobal(t, a, "query:\n  defaultMaxRows: 3\n  defaultTimeout: 30s\n")
	withDatasetWithoutQuery(t, a, "query:\n  maxRows: 50\n  timeout: 30s\n")

	code := a.run("query", "--config", a.config, "--dataset", "sem_query",
		"SELECT id FROM sem_query")
	if code != cli.ExitOK {
		t.Fatalf("code = %d, want 0 (o maxRows do dataset é 50)\n%s", code, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSuffix(a.stdout.String(), "\n"), "\n")
	if len(rows) != 20 {
		t.Fatalf("%d linhas, want 20", len(rows))
	}
}

// Valor não positivo é recusado no carregamento da configuração, com a chave
// nomeada. Zero e negativo não são "sem valor": são teto que nenhuma consulta
// satisfaz, e antes passavam em silêncio.
func TestDefaultMaxRowsNaoPositivoEhRecusado(t *testing.T) {
	for _, value := range []string{"-1", "0"} {
		t.Run("valor "+value, func(t *testing.T) {
			a := newEnv(t)
			swapQueryGlobal(t, a, "query:\n  defaultMaxRows: "+value+"\n  defaultTimeout: 30s\n")

			// Os DOIS comandos carregam a configuração, e os dois precisam
			// recusar: um ingest que aceitasse a config e um query que a
			// recusasse seria o mesmo silêncio, só que pela metade.
			for _, args := range [][]string{
				{"ingest", "--config", a.config, "--dataset", "sem_query", "--input", "-"},
				{"query", "--config", a.config, "--dataset", "sem_query", "SELECT id FROM sem_query"},
			} {
				code := a.runWithEntry("", args...)
				if code != cli.ExitUsageError {
					t.Fatalf("%s: code = %d, want %d\n%s", args[0], code, cli.ExitUsageError, a.stderr.String())
				}
				if !strings.Contains(a.stderr.String(), "query.defaultMaxRows") {
					t.Errorf("%s: stderr não nomeia a chave: %q", args[0], a.stderr.String())
				}
				if !strings.Contains(a.stderr.String(), value) {
					t.Errorf("%s: stderr não mostra o valor recusado: %q", args[0], a.stderr.String())
				}
			}
		})
	}
}

// Chave ausente continua valendo o padrão da spec §78 — o nishidb.yaml do M1
// não é obrigado a declarar o bloco query.
func TestSemBlocoQueryValemOsPadroesDaSpec(t *testing.T) {
	a := newEnv(t)
	swapQueryGlobal(t, a, "")
	withDatasetWithoutQuery(t, a, "")

	code := a.run("query", "--config", a.config, "--dataset", "sem_query",
		"SELECT id FROM sem_query")
	if code != cli.ExitOK {
		t.Fatalf("code = %d, want 0\n%s", code, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSuffix(a.stdout.String(), "\n"), "\n")
	if len(rows) != 20 {
		t.Fatalf("%d linhas, want 20 (padrão 100000 não trunca)", len(rows))
	}
}
