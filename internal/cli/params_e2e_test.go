package cli_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// countRows devolve as linhas NDJSON do stdout, sem a vazia do fim.
func countRows(saida string) []string {
	saida = strings.TrimSpace(saida)
	if saida == "" {
		return nil
	}
	return strings.Split(saida, "\n")
}

// O caminho de parâmetros ponta a ponta, PELO PRÓPRIO PRODUTO — que é o que o
// débito 58 pedia.
//
// Os `?` funcionavam pela API Go desde o M1 e pelo POST /v1/query desde a Fase
// 6, mas nenhum cliente do produto os exercitava: `nishidb query --server` não
// tinha como fornecê-los. O driver JDBC seria o primeiro, e testar o Java
// contra um caminho Go que nenhum cliente Go percorreu seria testar duas
// coisas de uma vez.
// umStatusOfCorpus descobre um valor que EXISTE no dado, em vez de fixar um.
//
// O gerador do corpus é determinístico mas o conjunto de status que ele produz
// depende do número de eventos: com 40, não há um único "Stop". Um teste que
// fixasse o valor passaria a afirmar "o parâmetro não casou nada" quando o que
// mudou foi o corpus — e o diagnóstico apontaria para o lugar errado.
// conexao são as flags que escolhem o modo: {"--config", path} ou
// {"--server", endereço}. A sondagem tem de usar o MESMO modo do teste que a
// chama — com o servidor no ar, o flock barra a consulta local, e o erro
// apontaria para o lugar errado.
func umStatusOfCorpus(t *testing.T, a *env, conexao ...string) string {
	t.Helper()
	args := append([]string{"query"}, conexao...)
	args = append(args, "--dataset", "radius_sessions", "SELECT status FROM radius_sessions LIMIT 1")
	if code := a.run(args...); code != cli.ExitOK {
		t.Fatalf("sondando o corpus: exit %d: %s", code, a.stderr.String())
	}
	rows := countRows(a.stdout.String())
	if len(rows) == 0 {
		t.Fatal("o corpus não produziu nenhuma row")
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(rows[0]), &m); err != nil {
		t.Fatal(err)
	}
	if m["status"] == "" {
		t.Fatalf("a primeira row não tem status: %s", rows[0])
	}
	return m["status"]
}

// O caminho de parâmetros ponta a ponta, PELO PRÓPRIO PRODUTO — que é o que o
// débito 58 pedia.
//
// Os `?` funcionavam pela API Go desde o M1 e pelo POST /v1/query desde a Fase
// 6, mas nenhum cliente do produto os exercitava: `nishidb query --server` não
// tinha como fornecê-los. O driver JDBC seria o primeiro, e testar o Java
// contra um caminho Go que nenhum cliente Go percorreu seria testar duas
// coisas de uma vez.
//
// O oráculo é a EQUIVALÊNCIA com o literal, e não uma contagem: prova que o
// `?` produz exatamente o mesmo resultado que o valor escrito na query, que é
// a promessa inteira do parâmetro. Uma asserção sobre o número de linhas
// passaria com um WHERE que casasse a coisa errada.
func TestParamProduzOMesmoResultadoQueOLiteralPeloProtocoloRemoto(t *testing.T) {
	a := newEnvRemote(t, 100000)
	risesWithEntry(t, a, corpus(t, 200))

	status := umStatusOfCorpus(t, a.env, "--server", a.endereco())

	if code := a.run("query", "--server", a.endereco(), "--dataset", "radius_sessions",
		"--param", "string:"+status,
		"SELECT id, status FROM radius_sessions WHERE status = ?"); code != cli.ExitOK {
		t.Fatalf("com ?: exit = %d: %s", code, a.stderr.String())
	}
	withParametro := a.stdout.String()

	if code := a.run("query", "--server", a.endereco(), "--dataset", "radius_sessions",
		"SELECT id, status FROM radius_sessions WHERE status = '"+status+"'"); code != cli.ExitOK {
		t.Fatalf("com literal: exit = %d: %s", code, a.stderr.String())
	}
	withLiteral := a.stdout.String()

	if len(countRows(withParametro)) == 0 {
		t.Fatalf("nenhuma linha para status=%q, que veio do próprio corpus", status)
	}
	if withParametro != withLiteral {
		t.Errorf("o ? não produziu o mesmo que o literal:\n--- com ? ---\n%s\n--- com literal ---\n%s",
			withParametro, withLiteral)
	}
}

// A flag vale nos DOIS caminhos.
//
// consultaRemota promete que "um script que troca --config por --server não
// muda mais nada". Uma flag que só funcionasse no remoto quebraria a promessa
// justamente onde ela é mais fácil de verificar — e a query com ? pelo caminho
// local saía com exit 2 desde o M1, o que o README registrava como limite.
func TestParamFuncionaTambemNoCaminhoLocalComConfig(t *testing.T) {
	a := newEnv(t)
	if code := a.runWithEntry(corpus(t, 30), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("ingest: %s", a.stderr.String())
	}

	status := umStatusOfCorpus(t, a, "--config", a.config)

	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"--param", "string:"+status,
		"SELECT id, status FROM radius_sessions WHERE status = ?")
	if code != cli.ExitOK {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	if len(countRows(a.stdout.String())) == 0 {
		t.Fatalf("nenhuma linha pelo caminho local para status=%q", status)
	}
}

// A validação é a do protocolo, e roda na linha de comando — não depois de um
// round-trip.
//
// É o que o int64 verbatim compra: com o valor convertido por strconv no CLI,
// ou pior, por float64, este número chegaria ao servidor arredondado e ACEITO.
func TestParamInt64AcimaDoTetoRecusaComAMensagemDoProtocolo(t *testing.T) {
	a := newEnv(t)
	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"--param", "int64:9223372036854775808",
		"SELECT id FROM radius_sessions WHERE _SEQ = ?")
	if code != cli.ExitUsageError {
		t.Fatalf("exit = %d, quer %d (erro de uso)", code, cli.ExitUsageError)
	}
	if !strings.Contains(a.stderr.String(), "parameters[1]") {
		t.Errorf("a mensagem não é a do protocolo (índice 1-based): %s", a.stderr.String())
	}
}

// Não existe --param que produza null, e é assim que tem de ser.
//
// O protocolo recusa `null` em parameters[] por decisão: `col = ?` com ? nulo
// não tem semântica sem lógica trivalente, e o que o usuário quer dizer é IS
// NULL. Este é o oráculo do lado Go para o limite que o driver JDBC vai
// publicar como SQLFeatureNotSupportedException em setNull.
func TestParamNuloEnsinaIsNull(t *testing.T) {
	a := newEnv(t)
	// `string:null` é a string "null", não o null de JSON — e é a única coisa
	// que a sintaxe da flag consegue produzir.
	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"--param", "string:null",
		"SELECT id FROM radius_sessions WHERE status = ?")
	if code == cli.ExitUsageError && strings.Contains(a.stderr.String(), "IS NULL") {
		t.Fatal("a flag conseguiu produzir um parâmetro NULO; ela não deveria ter sintaxe para isso")
	}
}

// As flags vêm ANTES da query, e a mensagem diz isso.
//
// flag.Parse para no primeiro argumento posicional, então --param depois do
// SQL vira Arg(1) e é silenciosamente ignorado. --param é a primeira flag do
// comando que um humano vai instintivamente pôr depois da query, e "informe
// exatamente uma query SQL" sozinho não ajudaria nesse caso.
func TestParamDepoisDaQueryEhRecusadoComMensagemQueEnsinaAOrdem(t *testing.T) {
	a := newEnv(t)
	code := a.run("query", "--config", a.config, "--dataset", "radius_sessions",
		"SELECT id FROM radius_sessions WHERE status = ?", "--param", "string:Stop")
	if code != cli.ExitUsageError {
		t.Fatalf("exit = %d, quer %d", code, cli.ExitUsageError)
	}
	if !strings.Contains(a.stderr.String(), "ANTES") {
		t.Errorf("a mensagem não ensina a ordem: %s", a.stderr.String())
	}
}
