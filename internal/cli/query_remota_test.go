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

// configComServidor é o nishidb.yaml do ciclo de operação desta fase: um
// servidor de verdade, em porta livre, com o teto de linhas que o teste quiser.
const configComServidor = `version: 1
storage:
  path: %s
datasets:
  directory: %s
server:
  bind: 127.0.0.1
  port: %d
`

type ambienteRemoto struct {
	*ambiente
	porta int
}

// novoAmbienteRemoto monta config + dataset com um teto de linhas escolhido
// pelo teste.
//
// O teto vai no YAML do DATASET, e não em query.defaultMaxRows: o fixture do
// §12 declara `maxRows: 100000`, e o dataset que declara VENCE o default
// global — é o contrato da errata do débito 37. Mexer no global aqui não
// mudaria nada, e o teste passaria a afirmar o oposto do que verifica.
func novoAmbienteRemoto(t *testing.T, maxRows int) *ambienteRemoto {
	t.Helper()
	a := novoAmbiente(t)
	porta := portaLivre(t)
	corpo := fmt.Sprintf(configComServidor,
		filepath.Join(a.dir, "dados"), filepath.Join(a.dir, "datasets"), porta)
	if err := os.WriteFile(a.config, []byte(corpo), 0o644); err != nil {
		t.Fatal(err)
	}

	spec := filepath.Join(a.dir, "datasets", "radius_sessions.yaml")
	yml, err := os.ReadFile(spec)
	if err != nil {
		t.Fatal(err)
	}
	novo := strings.Replace(string(yml), "maxRows: 100000", fmt.Sprintf("maxRows: %d", maxRows), 1)
	if novo == string(yml) && maxRows != 100000 {
		t.Fatal("o fixture mudou: `maxRows: 100000` não está mais no YAML do dataset")
	}
	if err := os.WriteFile(spec, []byte(novo), 0o644); err != nil {
		t.Fatal(err)
	}
	return &ambienteRemoto{ambiente: a, porta: porta}
}

func (a *ambienteRemoto) endereco() string {
	return fmt.Sprintf("127.0.0.1:%d", a.porta)
}

// sobeServidor roda `nishidb server` como o operador o roda, e espera o
// /v1/health responder antes de devolver.
func (a *ambienteRemoto) sobeServidor(t *testing.T) {
	t.Helper()
	ctx, cancela := context.WithCancel(context.Background())
	fim := make(chan int, 1)
	go func() {
		fim <- cli.Run([]string{"server", "--config", a.config},
			cli.Ambiente{Stdin: strings.NewReader(""), Stdout: os.Stdout, Stderr: os.Stderr, Ctx: ctx})
	}()
	t.Cleanup(func() {
		cancela()
		if code := <-fim; code != 0 {
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

// O débito 54, fim a fim: com o servidor no ar, a consulta LOCAL é barrada
// pelo flock e a REMOTA responde. É a diferença entre alternar servidor e
// consulta e operar as duas ao mesmo tempo.
func TestConsultaRemotaFuncionaComOServidorNoAr(t *testing.T) {
	a := novoAmbienteRemoto(t, 100000)
	if code := a.rodaComEntrada(corpus(t, 50), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.sobeServidor(t)

	// Local: barrado pelo flock, com a dica de como prosseguir.
	code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions LIMIT 3")
	if code != 3 {
		t.Fatalf("consulta local com o servidor no ar saiu %d, quer 3", code)
	}
	if !strings.Contains(a.stderr.String(), "--server") {
		t.Fatalf("a mensagem do exit 3 não ensina o caminho de volta:\n%s", a.stderr.String())
	}

	// Remota: responde, com o servidor de pé e sem tocar no flock.
	code = a.roda("query", "--server", a.endereco(), "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions LIMIT 3")
	if code != 0 {
		t.Fatalf("consulta remota saiu %d: %s", code, a.stderr.String())
	}
	linhas := strings.Split(strings.TrimSpace(a.stdout.String()), "\n")
	if len(linhas) != 3 {
		t.Fatalf("linhas = %d, quer 3:\n%s", len(linhas), a.stdout.String())
	}
	for _, l := range linhas {
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
	a := novoAmbienteRemoto(t, 3)
	if code := a.rodaComEntrada(corpus(t, 50), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.sobeServidor(t)

	t.Run("truncado no teto do dataset", func(t *testing.T) {
		// maxRows: 3 no YAML do dataset contra 50 linhas materializadas.
		code := a.roda("query", "--server", a.endereco(), "--dataset", "radius_sessions",
			"SELECT id FROM radius_sessions")
		if code != 5 {
			t.Fatalf("exit = %d, quer 5 (truncado)", code)
		}
		if n := len(strings.Split(strings.TrimSpace(a.stdout.String()), "\n")); n != 3 {
			t.Fatalf("linhas = %d, quer 3: as rows válidas continuam saindo", n)
		}
	})

	t.Run("erro de query", func(t *testing.T) {
		code := a.roda("query", "--server", a.endereco(), "--dataset", "radius_sessions",
			"SELECT coluna_que_nao_existe FROM radius_sessions")
		if code != 2 {
			t.Fatalf("exit = %d, quer 2", code)
		}
		if a.stdout.Len() != 0 {
			t.Fatalf("erro de bind não pode emitir linha no stdout: %q", a.stdout.String())
		}
	})

	t.Run("sintaxe com posição", func(t *testing.T) {
		code := a.roda("query", "--server", a.endereco(), "--dataset", "radius_sessions",
			"SELECT FROM radius_sessions")
		if code != 2 {
			t.Fatalf("exit = %d, quer 2", code)
		}
		if !strings.Contains(a.stderr.String(), "linha 1") {
			t.Fatalf("a posição do erro não chegou ao operador:\n%s", a.stderr.String())
		}
	})

	t.Run("dataset desconhecido", func(t *testing.T) {
		code := a.roda("query", "--server", a.endereco(), "--dataset", "fantasma",
			"SELECT id FROM fantasma")
		if code != 1 {
			t.Fatalf("exit = %d, quer 1", code)
		}
	})

	t.Run("servidor inalcançável", func(t *testing.T) {
		code := a.roda("query", "--server", "127.0.0.1:1", "--dataset", "radius_sessions",
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
	a := novoAmbienteRemoto(t, 100000)
	if code := a.rodaComEntrada(corpus(t, 30), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.sobeServidor(t)

	if code := a.roda("query", "--server", a.endereco(), "--dataset", "radius_sessions",
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
	a := novoAmbienteRemoto(t, 10)

	code := a.roda("query", "--config", a.config, "--server", "127.0.0.1:9142",
		"--dataset", "radius_sessions", "SELECT id FROM radius_sessions")
	if code != 1 {
		t.Fatalf("exit = %d, quer 1", code)
	}
	if !strings.Contains(a.stderr.String(), "exclusivos") {
		t.Fatalf("a mensagem não explica o conflito:\n%s", a.stderr.String())
	}

	code = a.roda("query", "--dataset", "radius_sessions", "SELECT id FROM radius_sessions")
	if code != 1 {
		t.Fatalf("sem --config e sem --server: exit = %d, quer 1", code)
	}
}
