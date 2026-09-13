package cli_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// configWithServer é o nishidb.yaml do ciclo de operação desta fase: um
// servidor de verdade, em porta livre, com o teto de linhas que o teste quiser.
const configWithServer = `version: 1
storage:
  path: %s
datasets:
  directory: %s
server:
  bind: 127.0.0.1
  port: %d
`

type envRemote struct {
	*env
	porta int
}

// newEnvRemote monta config + dataset com um teto de linhas escolhido
// pelo teste.
//
// O teto vai no YAML do DATASET, e não em query.defaultMaxRows: o fixture do
// §12 declara `maxRows: 100000`, e o dataset que declara VENCE o default
// global — é o contrato da errata do débito 37. Mexer no global aqui não
// mudaria nada, e o teste passaria a afirmar o oposto do que verifica.
func newEnvRemote(t *testing.T, maxRows int) *envRemote {
	t.Helper()
	a := newEnv(t)
	porta := portaLivre(t)
	corpo := fmt.Sprintf(configWithServer,
		filepath.Join(a.dir, "dados"), filepath.Join(a.dir, "datasets"), porta)
	if err := os.WriteFile(a.config, []byte(corpo), 0o644); err != nil {
		t.Fatal(err)
	}

	spec := filepath.Join(a.dir, "datasets", "radius_sessions.yaml")
	yml, err := os.ReadFile(spec)
	if err != nil {
		t.Fatal(err)
	}
	nw := strings.Replace(string(yml), "maxRows: 100000", fmt.Sprintf("maxRows: %d", maxRows), 1)
	if nw == string(yml) && maxRows != 100000 {
		t.Fatal("o fixture mudou: `maxRows: 100000` não está mais no YAML do dataset")
	}
	if err := os.WriteFile(spec, []byte(nw), 0o644); err != nil {
		t.Fatal(err)
	}
	return &envRemote{env: a, porta: porta}
}

func (a *envRemote) endereco() string {
	return fmt.Sprintf("127.0.0.1:%d", a.porta)
}

// risesServer roda `nishidb server` como o operador o roda, e espera o
// /v1/health responder antes de devolver.
func (a *envRemote) risesServer(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan int, 1)
	go func() {
		end <- cli.Run([]string{"server", "--config", a.config},
			cli.Context{Stdin: strings.NewReader(""), Stdout: os.Stdout, Stderr: os.Stderr, Ctx: ctx})
	}()
	t.Cleanup(func() {
		cancel()
		if code := <-end; code != 0 {
			t.Errorf("nishidb server saiu com %d", code)
		}
	})

	prazo := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get("http://" + a.endereco() + "/v1/health")
		if err == nil {
			resp.Body.Close()
			return
		}
		if time.Now().After(prazo) {
			t.Fatalf("o servidor não subiu: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func portaLivre(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// A flag diz o que cada modo endereça: o remoto é a conexão por schema, o
// local abre um dataset. A flag trocada é recusada nomeando a certa, antes de
// qualquer conexão — por isso nenhum servidor sobe aqui.
func TestRemoteQueryAddressesTheSchemaNotTheDataset(t *testing.T) {
	a := newEnvRemote(t, 100000)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"dataset com --server", []string{"--server", a.endereco(), "--dataset", "radius_sessions"}, "use --schema"},
		{"--server sem --schema", []string{"--server", a.endereco()}, "--schema is required"},
		{"--schema com --config", []string{"--config", a.config, "--dataset", "radius_sessions", "--schema", "telecom"}, "only applies to --server"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"query"}, c.args...)
			args = append(args, "SELECT id FROM radius_sessions")
			if code := a.run(args...); code != cli.ExitUsageError {
				t.Fatalf("exit = %d, quer %d", code, cli.ExitUsageError)
			}
			if !strings.Contains(a.stderr.String(), c.want) {
				t.Fatalf("a recusa não contém %q:\n%s", c.want, a.stderr.String())
			}
		})
	}
}

// O débito 54, fim a fim: com o servidor no ar, a consulta LOCAL é barrada
// pelo flock e a REMOTA responde. É a diferença entre alternar servidor e
// consulta e operar as duas ao mesmo tempo.
func TestConsultaRemotaFuncionaComOServidorNoAr(t *testing.T) {
	a := newEnvRemote(t, 100000)
	if code := a.runWithEntry(corpus(t, 50), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.risesServer(t)

	// Local: barrado pelo flock, com a dica de como prosseguir.
	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions LIMIT 3")
	if code != 3 {
		t.Fatalf("consulta local com o servidor no ar saiu %d, quer 3", code)
	}
	if !strings.Contains(a.stderr.String(), "--server") {
		t.Fatalf("a mensagem do exit 3 não ensina o caminho de volta:\n%s", a.stderr.String())
	}

	// Remota: responde, com o servidor de pé e sem tocar no flock.
	code = a.run("query", "--server", a.endereco(), "--schema", "telecom",
		"SELECT id FROM radius_sessions LIMIT 3")
	if code != 0 {
		t.Fatalf("consulta remota saiu %d: %s", code, a.stderr.String())
	}
	rows := strings.Split(strings.TrimSpace(a.stdout.String()), "\n")
	if len(rows) != 3 {
		t.Fatalf("linhas = %d, quer 3:\n%s", len(rows), a.stdout.String())
	}
	for _, l := range rows {
		// O stdout é o NDJSON cru do §69: nada de moldura vazando para quem
		// consome a saída do comando.
		if !strings.HasPrefix(l, `{"id":"`) {
			t.Fatalf("linha inesperada no stdout: %s", l)
		}
	}
}

// A tabela de exit codes é a MESMA nos dois caminhos: é o que permite trocar
// --config por --server num script sem reescrever o tratamento de $?.
func TestOsExitCodesDaConsultaRemota(t *testing.T) {
	a := newEnvRemote(t, 3)
	if code := a.runWithEntry(corpus(t, 50), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.risesServer(t)

	t.Run("truncado no teto do dataset", func(t *testing.T) {
		// maxRows: 3 no YAML do dataset contra 50 linhas materializadas.
		code := a.run("query", "--server", a.endereco(), "--schema", "telecom",
			"SELECT id FROM radius_sessions")
		if code != 5 {
			t.Fatalf("exit = %d, quer 5 (truncado)", code)
		}
		if n := len(strings.Split(strings.TrimSpace(a.stdout.String()), "\n")); n != 3 {
			t.Fatalf("linhas = %d, quer 3: as rows válidas continuam saindo", n)
		}
	})

	t.Run("erro de query", func(t *testing.T) {
		code := a.run("query", "--server", a.endereco(), "--schema", "telecom",
			"SELECT coluna_que_nao_existe FROM radius_sessions")
		if code != 2 {
			t.Fatalf("exit = %d, quer 2", code)
		}
		if a.stdout.Len() != 0 {
			t.Fatalf("erro de bind não pode emitir linha no stdout: %q", a.stdout.String())
		}
	})

	t.Run("sintaxe com posição", func(t *testing.T) {
		code := a.run("query", "--server", a.endereco(), "--schema", "telecom",
			"SELECT FROM radius_sessions")
		if code != 2 {
			t.Fatalf("exit = %d, quer 2", code)
		}
		if !strings.Contains(a.stderr.String(), "linha 1") {
			t.Fatalf("a posição do erro não chegou ao operador:\n%s", a.stderr.String())
		}
	})

	t.Run("schema desconhecido", func(t *testing.T) {
		code := a.run("query", "--server", a.endereco(), "--schema", "fantasma",
			"SELECT id FROM fantasma")
		if code != 1 {
			t.Fatalf("exit = %d, quer 1", code)
		}
	})

	// O nome do DATASET no lugar do schema é o engano de quem vinha da forma
	// anterior, e a recusa do servidor chega ao operador ensinando o schema.
	t.Run("dataset no lugar do schema", func(t *testing.T) {
		code := a.run("query", "--server", a.endereco(), "--schema", "radius_sessions",
			"SELECT id FROM radius_sessions")
		if code != 1 {
			t.Fatalf("exit = %d, quer 1", code)
		}
		if !strings.Contains(a.stderr.String(), `use "telecom"`) {
			t.Fatalf("a recusa não ensina o schema:\n%s", a.stderr.String())
		}
	})

	// Na conexão por schema o nome simples é a tabela do schema, e o schema
	// qualificado resolve igual.
	t.Run("nome simples e qualificado pelo schema", func(t *testing.T) {
		for _, sql := range []string{
			"SELECT id FROM radius_sessions LIMIT 1",
			"SELECT id FROM telecom.radius_sessions LIMIT 1",
		} {
			if code := a.run("query", "--server", a.endereco(), "--schema", "telecom", sql); code != 0 {
				t.Fatalf("%q saiu %d: %s", sql, code, a.stderr.String())
			}
		}
	})

	t.Run("servidor inalcançável", func(t *testing.T) {
		code := a.run("query", "--server", "127.0.0.1:1", "--schema", "telecom",
			"SELECT id FROM radius_sessions")
		if code != 1 {
			t.Fatalf("exit = %d, quer 1", code)
		}
		if !strings.Contains(a.stderr.String(), "127.0.0.1:1") {
			t.Fatalf("a mensagem não nomeia o endereço tentado:\n%s", a.stderr.String())
		}
	})
}

// O --limit continua valendo no caminho remoto: ele reescreve o SQL antes do
// envio, então o servidor recebe a query já com o LIMIT.
func TestLimitValeNaConsultaRemota(t *testing.T) {
	a := newEnvRemote(t, 100000)
	if code := a.runWithEntry(corpus(t, 30), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.risesServer(t)

	if code := a.run("query", "--server", a.endereco(), "--schema", "telecom",
		"--limit", "2", "SELECT id FROM radius_sessions"); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	if n := len(strings.Split(strings.TrimSpace(a.stdout.String()), "\n")); n != 2 {
		t.Fatalf("linhas = %d, quer 2", n)
	}
}

// --config e --server descrevem caminhos diferentes para a mesma pergunta;
// aceitá-los juntos deixaria o operador sem saber qual venceu.
func TestConfigEServerSaoExclusivos(t *testing.T) {
	a := newEnvRemote(t, 10)

	code := a.run("query", "--config", a.config, "--server", "127.0.0.1:9142",
		"--dataset", "radius_sessions", "SELECT id FROM radius_sessions")
	if code != 1 {
		t.Fatalf("exit = %d, quer 1", code)
	}
	if !strings.Contains(a.stderr.String(), "exclusivos") {
		t.Fatalf("a mensagem não explica o conflito:\n%s", a.stderr.String())
	}

	code = a.run("query", "--dataset", "radius_sessions", "SELECT id FROM radius_sessions")
	if code != 1 {
		t.Fatalf("sem --config e sem --server: exit = %d, quer 1", code)
	}
}
