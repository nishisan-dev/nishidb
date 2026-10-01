package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

func TestOUsoAnunciaCounters(t *testing.T) {
	a := newEnv(t)
	if code := a.run("--help"); code != 0 {
		t.Fatalf("--help saiu com %d", code)
	}
	for _, trecho := range []string{"nishidb counters", "--audit", "--rebuild", "_counters."} {
		if !strings.Contains(a.stdout.String(), trecho) {
			t.Errorf("o texto de uso não anuncia %q", trecho)
		}
	}
}

func TestCountersValidaAsFlags(t *testing.T) {
	a := newEnv(t)
	casos := []struct {
		name string
		args []string
		quer string
	}{
		{"sem config nem server", []string{"counters", "--dataset", "servicos"}, "exatamente um"},
		{"config e server", []string{"counters", "--config", a.config, "--server", "127.0.0.1:1", "--dataset", "servicos"}, "exatamente um"},
		{"sem dataset", []string{"counters", "--config", a.config}, "--dataset"},
		{"audit e rebuild", []string{"counters", "--config", a.config, "--dataset", "servicos", "--audit", "--rebuild"}, "exclusivos"},
		{"posicional", []string{"counters", "--config", a.config, "--dataset", "servicos", "extra"}, "posicional"},
	}
	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			if code := a.run(c.args...); code != cli.ExitUsageError {
				t.Fatalf("exit = %d, quer %d\n%s", code, cli.ExitUsageError, a.stderr.String())
			}
			if !strings.Contains(a.stderr.String(), c.quer) {
				t.Fatalf("stderr não explica (%q):\n%s", c.quer, a.stderr.String())
			}
		})
	}
}

// TestCountersLocalListaAuditaEReconstroi: o caminho --config, com o servidor
// parado — a tabela da proposta §5.6, sem divergência num dataset recém-ingerido.
func TestCountersLocalListaAuditaEReconstroi(t *testing.T) {
	a := newEnv(t)
	writeDatasetServices(t, a)
	if code := a.runWithEntry(eventosOfServico(24),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}

	if code := a.run("counters", "--config", a.config, "--dataset", "servicos"); code != cli.ExitOK {
		t.Fatalf("counters = %d\n%s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	for _, trecho := range []string{"CONTADOR", "LINHAS", "TOTAL", "GRUPOS", "TETO", "autenticados_por_olt", "nunca"} {
		if !strings.Contains(saida, trecho) {
			t.Errorf("listagem sem %q:\n%s", trecho, saida)
		}
	}
	// 24 serviços, todos com `olt`; autenticados são os i%3 != 0, que são 16.
	// Os dois números serem DIFERENTES é o que faz a asserção discriminar a
	// troca das colunas.
	if got := columnsOfCounter(t, saida, "autenticados_por_olt"); got[0] != "24" || got[1] != "16" {
		t.Errorf("autenticados_por_olt: linhas %q total %q, quero 24 e 16:\n%s", got[0], got[1], saida)
	}

	if code := a.run("counters", "--config", a.config, "--dataset", "servicos", "--audit"); code != cli.ExitOK {
		t.Fatalf("counters --audit = %d\n%s", code, a.stderr.String())
	}
	saida = a.stdout.String()
	for _, trecho := range []string{"MANTIDO", "RECALCULADO", "DIVERGÊNCIA", "autenticados_por_olt"} {
		if !strings.Contains(saida, trecho) {
			t.Errorf("auditoria sem %q:\n%s", trecho, saida)
		}
	}
	// 24 serviços, 16 autenticados (i%3 != 0): mantido e recalculado batem.
	if !strings.Contains(saida, "16") {
		t.Errorf("a auditoria tinha de mostrar os 16 autenticados:\n%s", saida)
	}

	if code := a.run("counters", "--config", a.config, "--dataset", "servicos", "--rebuild"); code != cli.ExitOK {
		t.Fatalf("counters --rebuild = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "LINHAS") || !strings.Contains(a.stdout.String(), "24") {
		t.Errorf("reconstrução sem as linhas varridas:\n%s", a.stdout.String())
	}
	// A reconstrução local termina num checkpoint, que a adota: sem ele o
	// marcador de construção ficaria no disco e a próxima abertura
	// reconstruiria tudo de novo.
	marker := filepath.Join(a.dir, "dados", "servicos", "indexes", "contadores", "autenticados_por_olt", "building")
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("depois de counters --rebuild o marcador de construção ficou no disco (%v): a próxima abertura reconstruiria de novo", err)
	}
	// O resultado da auditoria vive na ABERTURA (não é persistido), e cada
	// `--config` é uma abertura nova: a listagem local volta a dizer "nunca".
	// Quem quer ver a última auditoria na listagem é o servidor no ar, e o
	// teste remoto cobre isso.
	if code := a.run("counters", "--config", a.config, "--dataset", "servicos"); code != cli.ExitOK {
		t.Fatalf("counters = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "nunca") {
		t.Errorf("numa abertura nova a listagem tem de dizer 'nunca':\n%s", a.stdout.String())
	}
}

func TestCountersLocalEmDatasetSemContadores(t *testing.T) {
	a := newEnv(t)
	if code := a.runWithEntry(corpus(t, 10),
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	if code := a.run("counters", "--config", a.config, "--dataset", "radius_sessions", "--audit"); code != cli.ExitOK {
		t.Fatalf("counters --audit = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "não declara counters") {
		t.Errorf("dataset sem contadores tem de dizer isso:\n%s", a.stdout.String())
	}
}

// TestCountersRemotoChamaAsRotas: o caminho --server, com o servidor no ar —
// a listagem sai do /v1/stats, a auditoria e a reconstrução das rotas ADMIN.
func TestCountersRemotoChamaAsRotas(t *testing.T) {
	a := newEnvRemote(t, 100000)
	writeDatasetServices(t, a.env)
	if code := a.runWithEntry(eventosOfServico(24),
		"ingest", "--config", a.config, "--dataset", "servicos", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest = %d\n%s", code, a.stderr.String())
	}
	a.risesServer(t)

	if code := a.run("counters", "--server", a.endereco(), "--dataset", "servicos"); code != cli.ExitOK {
		t.Fatalf("counters --server = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "autenticados_por_olt") {
		t.Errorf("listagem remota sem o contador:\n%s", a.stdout.String())
	}
	// O caminho remoto lê o /v1/stats, e é aqui que a troca de Rows por Total
	// no mapeamento do wire aparece.
	if got := columnsOfCounter(t, a.stdout.String(), "autenticados_por_olt"); got[0] != "24" || got[1] != "16" {
		t.Errorf("listagem remota: linhas %q total %q, quero 24 e 16:\n%s", got[0], got[1], a.stdout.String())
	}
	if code := a.run("counters", "--server", a.endereco(), "--dataset", "servicos", "--audit"); code != cli.ExitOK {
		t.Fatalf("counters --server --audit = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "DIVERGÊNCIA") {
		t.Errorf("auditoria remota sem a tabela:\n%s", a.stdout.String())
	}
	if code := a.run("counters", "--server", a.endereco(), "--dataset", "servicos", "--rebuild"); code != cli.ExitOK {
		t.Fatalf("counters --server --rebuild = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "24") {
		t.Errorf("reconstrução remota sem as linhas:\n%s", a.stdout.String())
	}
	// No servidor no ar a listagem mostra a última auditoria desta abertura.
	if code := a.run("counters", "--server", a.endereco(), "--dataset", "servicos"); code != cli.ExitOK {
		t.Fatalf("counters --server = %d\n%s", code, a.stderr.String())
	}
	if !strings.Contains(a.stdout.String(), "0 divergente(s)") {
		t.Errorf("depois da auditoria a listagem remota tinha de mostrá-la:\n%s", a.stdout.String())
	}
	// Dataset que o servidor não serve.
	if code := a.run("counters", "--server", a.endereco(), "--dataset", "nao_existe"); code != cli.ExitUsageError {
		t.Fatalf("dataset desconhecido = %d, quer %d", code, cli.ExitUsageError)
	}
	// E o caminho local, com o servidor no ar, sai com 3 e aponta o --server.
	if code := a.run("counters", "--config", a.config, "--dataset", "servicos"); code != cli.ExitDataset {
		t.Fatalf("counters --config com o servidor no ar = %d, quer %d\n%s", code, cli.ExitDataset, a.stderr.String())
	}
	if !strings.Contains(a.stderr.String(), "--server") {
		t.Errorf("a recusa tinha de apontar o --server:\n%s", a.stderr.String())
	}
}

// columnsOfCounter devolve LINHAS e TOTAL da line do contador nomeado na
// tabela do `counters`. A tabela sai alinhada por um tabwriter, então as
// colunas se separam por espaços e nenhum dos três primeiros campos os contém.
func columnsOfCounter(t *testing.T, saida, name string) [2]string {
	t.Helper()
	for _, l := range strings.Split(saida, "\n") {
		fields := strings.Fields(l)
		if len(fields) < 3 || fields[0] != name {
			continue
		}
		return [2]string{fields[1], fields[2]}
	}
	t.Fatalf("o contador %q não está na tabela:\n%s", name, saida)
	return [2]string{}
}
