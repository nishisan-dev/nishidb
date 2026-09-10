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
// eventoGigante tem um VALOR acima de types.MaxIndexedValueBytes num field
// com índice bitmap. Até o M1 essa era a rota de FALHA (balde 2), porque o
// erro caía em STORAGE_ERROR por falta de categoria melhor. No M2 ele é a
// rota de DESCARTE: um atributo RADIUS de 70 KiB é culpa do evento, e o §101
// governa (categoria EVENT_TOO_LARGE).
func eventoGigante() string {
	return `{"id":"B","_SEQ":1,"Filter-Id":"` + strings.Repeat("x", types.MaxIndexedValueBytes+1) + `"}`
}

// eventoComNomeDeCampoGigante é a rota de FALHA que sobrou, e é
// qualitativamente diferente: o teto violado não é de um VALOR, é do NOME do
// field no dicionário. Um valor grande é dado legítimo mal dimensionado; um
// nome de field de 64 KiB é produtor emitindo lixo estrutural, e cada um
// desses consumiria permanentemente um slot do dicionário do dataset. Pedir
// atenção humana em vez de descartar e seguir é conservador na direção certa.
func eventoComNomeDeCampoGigante() string {
	return `{"id":"B","_SEQ":1,"` + strings.Repeat("n", 1<<16) + `":"x"}`
}

// A rota de descarte: o evento não entra, mas isso NÃO é perda de dado — a
// política do §101 decidiu descartá-lo, e o exit code diz sucesso.
func TestEventoAcimaDoTetoDeFormatoEDescarteENaoPerda(t *testing.T) {
	a := novoAmbiente(t)
	entrada := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		eventoGigante(),
		`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	if code != cli.ExitOK {
		t.Errorf("code = %d, want %d: teto de formato é culpa do evento, não perda\n%s",
			code, cli.ExitOK, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "3 lidos, 2 aplicados") {
		t.Errorf("resumo = %q", msg)
	}
	if !strings.Contains(msg, "descartados por categoria: EVENT_TOO_LARGE=1") {
		t.Errorf("o descarte não foi nomeado pela categoria nova:\n%s", msg)
	}
	if strings.Contains(msg, "não aplicados por falha") {
		t.Errorf("evento grande foi contado como perda de dado:\n%s", msg)
	}
}

func TestEventoNaoAplicadoPorFalhaTemExitCodeProprio(t *testing.T) {
	a := novoAmbiente(t)
	entrada := strings.Join([]string{
		`{"id":"A","_SEQ":1,"Acct-Status-Type":"Stop"}`,
		eventoComNomeDeCampoGigante(),
		`{"id":"C","_SEQ":1,"Acct-Status-Type":"Start"}`,
	}, "\n") + "\n"

	code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-")

	if code != cli.ExitDataLoss {
		t.Errorf("code = %d, want %d (perda de dado): a ingestão perdeu uma row e reportou sucesso\n%s",
			code, cli.ExitDataLoss, a.stderr.String())
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
		eventoComNomeDeCampoGigante(),
	}, "\n") + "\n"

	if code := a.rodaComEntrada(entrada,
		"ingest", "--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitDataLoss {
		t.Errorf("code = %d, want %d\n%s", code, cli.ExitDataLoss, a.stderr.String())
	}
	msg := a.stderr.String()
	if !strings.Contains(msg, "1 descartados por categoria: INVALID_JSON=1") {
		t.Errorf("linha de descarte ausente ou errada:\n%s", msg)
	}
	if !strings.Contains(msg, "1 não aplicados por falha (perda de dado): STORAGE_ERROR=1") {
		t.Errorf("linha de falha ausente ou errada:\n%s", msg)
	}
}
