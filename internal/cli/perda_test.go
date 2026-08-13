package cli_test

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// A política de três baldes (§101) distingue o que é culpa do EVENTO (descarta
// e segue) do que NÃO é (a row não entrou, e alguém precisa saber). O CLI
// colapsava os dois num único mapa de "descartados" e terminava com exit 0: a
// row sumia e `$?` dizia sucesso, que é a definição de perda silenciosa.
//
// O evento gigante é a rota reprodutível: um valor acima de
// types.MaxIndexedValueBytes num campo com índice bitmap é recusado pelo
// storage, vira STORAGE_ERROR (balde 2) e o evento NÃO é aplicado.
func eventoGigante() string {
	return `{"id":"B","_SEQ":1,"Filter-Id":"` + strings.Repeat("x", types.MaxIndexedValueBytes+1) + `"}`
}

func TestEventoNaoAplicadoPorFalhaTemExitCodeProprio(t *testing.T) {
	a := novoAmbiente(t)
	entrada := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		eventoGigante(),
		`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	if code != cli.ExitPerdaDeDados {
		t.Errorf("code = %d, want %d (perda de dado): a ingestão perdeu uma row e reportou sucesso\n%s",
			code, cli.ExitPerdaDeDados, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "3 lidos, 2 aplicados") {
		t.Errorf("resumo = %q", msg)
	}
	// O balde 2 é reportado SEPARADO do balde 1: "descartado" é uma decisão
	// tomada sobre um evento ruim, "não aplicado" é uma falha nossa.
	if !strings.Contains(msg, "não aplicados por falha") {
		t.Errorf("o resumo não separa a falha do descarte:\n%s", msg)
	}
	if !strings.Contains(msg, "STORAGE_ERROR=1") {
		t.Errorf("o resumo não nomeia a categoria da falha:\n%s", msg)
	}

	// A prova de que houve perda: a row do meio não existe.
	if code := a.roda("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions"); code != cli.ExitOK {
		t.Fatalf("query: code = %d\n%s", code, a.stderr.String())
	}
	if linhas := strings.Count(strings.TrimSpace(a.stdout.String()), "\n") + 1; linhas != 2 {
		t.Errorf("%d rows no dataset, want 2\n%s", linhas, a.stdout.String())
	}
}

// Evento ruim continua sendo exit 0: a política é log-and-skip, e uma fonte
// ruidosa não pode fazer o pipeline inteiro falhar. O exit novo é só para o
// balde que representa perda.
func TestEventoRuimContinuaSaindoComSucesso(t *testing.T) {
	a := novoAmbiente(t)
	entrada := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		`{isso nao e json}`,
		`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")
	if code != cli.ExitOK {
		t.Errorf("code = %d, want 0 (evento ruim é descarte, não falha)\n%s", code, a.stderr.String())
	}
	if msg := a.stderr.String(); strings.Contains(msg, "não aplicados por falha") {
		t.Errorf("descarte reportado como falha:\n%s", msg)
	}
}

// As duas categorias na mesma rodada: cada uma na sua linha, e o exit é o da
// falha — o balde mais grave manda.
func TestDescarteEFalhaSaoReportadosSeparadamente(t *testing.T) {
	a := novoAmbiente(t)
	entrada := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		`{isso nao e json}`,
		eventoGigante(),
	}, "\n") + "\n"

	if code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitPerdaDeDados {
		t.Errorf("code = %d, want %d\n%s", code, cli.ExitPerdaDeDados, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "1 descartados por categoria: INVALID_JSON=1") {
		t.Errorf("linha de descarte ausente ou errada:\n%s", msg)
	}
	if !strings.Contains(msg, "1 não aplicados por falha (perda de dado): STORAGE_ERROR=1") {
		t.Errorf("linha de falha ausente ou errada:\n%s", msg)
	}
}
