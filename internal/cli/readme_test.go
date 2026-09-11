package cli_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/cli"
)

// O quickstart do README é EXECUTADO por este teste, com os blocos extraídos do
// próprio arquivo. A revisão final rodou o README à mão e achou quatro defeitos
// em sequência — YAML que não abre o dataset, query citando coluna inexistente,
// saída que o binário não produz e posições de erro erradas. Nenhum apareceu na
// suíte porque a documentação nunca tinha sido executada.
//
// O teste lê os blocos do arquivo em vez de duplicá-los: uma cópia no teste
// envelheceria em silêncio, que é exatamente a classe de defeito que ele existe
// para fechar.

const readmePath = "../../README.md"

// blocosOfCodigo devolve o conteúdo de cada bloco cercado por ``` com a
// linguagem pedida, na ordem em que aparecem.
//
// Só blocos SEM indentação contam: a cerca indentada vive dentro de um item de
// lista e é trecho ilustrativo (o snippet de `history.index` da nota de
// upgrade), não um arquivo executável do quickstart. A versão com TrimSpace
// promovia o snippet a yamls[0] e o quickstart passava a "instalar" um YAML
// que nem tem storage.path.
func blocosOfCodigo(t *testing.T, md, lang string) []string {
	t.Helper()
	var out []string
	rows := strings.Split(md, "\n")
	for i := 0; i < len(rows); i++ {
		if rows[i] != "```"+lang {
			continue
		}
		var corpo []string
		for i++; i < len(rows) && strings.TrimSpace(rows[i]) != "```"; i++ {
			corpo = append(corpo, rows[i])
		}
		out = append(out, strings.Join(corpo, "\n"))
	}
	return out
}

// A duração e a taxa mudam a cada execução: são o único trecho da saída
// documentada que não pode ser comparado literalmente.
var reTime = regexp.MustCompile(`em \d+(\.\d+)?[a-zµ]+ \(\d+ ev/s\)`)

func normalizaTempos(s string) string { return reTime.ReplaceAllString(s, "em T (R ev/s)") }

// comandoESaida separa um bloco ```console em (comando, saída esperada). O
// comando continua na line seguinte por dois motivos: '\' no fim (quebra de
// shell) ou aspas ainda ABERTAS — que é o caso do exemplo de erro, cujo SQL
// ocupa duas linhas de propósito, para mostrar line e coluna.
func comandoESaida(block string) (cmd string, saida string) {
	var expected []string
	inComando := false
	for _, l := range strings.Split(block, "\n") {
		switch {
		case strings.HasPrefix(l, "$ "):
			inComando = true
			cmd = strings.TrimSuffix(strings.TrimPrefix(l, "$ "), "\\")
		case inComando && strings.Count(cmd, `"`)%2 == 1:
			cmd += "\n" + l // dentro de aspas: a quebra faz parte do argumento
		case inComando && strings.HasSuffix(cmd, " "):
			cmd += strings.TrimSuffix(strings.TrimSpace(l), "\\")
		default:
			inComando = false
			expected = append(expected, l)
		}
	}
	return cmd, strings.Join(expected, "\n")
}

func TestQuickstartDoREADMEExecuta(t *testing.T) {
	bruto, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatal(err)
	}
	md := string(bruto)

	yamls := blocosOfCodigo(t, md, "yaml")
	if len(yamls) < 2 {
		t.Fatalf("%d blocos yaml no README, want ao menos 2 (config e dataset)", len(yamls))
	}

	raiz := t.TempDir()
	data := filepath.Join(raiz, "dados")
	specs := filepath.Join(raiz, "datasets")
	for _, d := range []string{data, specs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Os caminhos do README são os de uma instalação real; só eles mudam.
	cfgYAML := strings.NewReplacer(
		"/var/lib/nishidb", data,
		"/etc/nishidb/datasets", specs,
	).Replace(yamls[0])
	cfg := filepath.Join(raiz, "nishidb.yaml")
	if err := os.WriteFile(cfg, []byte(cfgYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(specs, "radius_sessions.yaml"), []byte(yamls[1]), 0o644); err != nil {
		t.Fatal(err)
	}

	consoles := blocosOfCodigo(t, md, "console")
	if len(consoles) < 3 {
		t.Fatalf("%d blocos console no README, want 3 (eventos+ingest, query, erro)", len(consoles))
	}

	// Bloco 1: `cat eventos.ndjson` seguido do ingest. O conteúdo exibido pelo
	// cat É o corpus — é ele que vai para o stdin do ingest.
	eventos, resumoExpected := eventosESummary(t, consoles[0])
	a := &env{dir: raiz, config: cfg}
	if code := a.runWithEntry(eventos, "ingest", "--config", cfg,
		"--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("o ingest do README saiu com %d:\n%s", code, a.stderr.String())
	}
	if got, want := normalizaTempos(strings.TrimSpace(a.stderr.String())), normalizaTempos(resumoExpected); got != want {
		t.Errorf("resumo do ingest:\n  obtido:   %s\n  no README: %s", got, want)
	}

	// Blocos 2 e 3: as duas consultas, com a saída conferida linha a linha.
	casos := []struct {
		block    string
		wantCode int
	}{
		{consoles[1], cli.ExitOK},
		{consoles[2], cli.ExitQuery},
	}
	for i, caso := range casos {
		cmd, expected := comandoESaida(caso.block)
		sql, ok := sqlOfComando(cmd)
		if !ok {
			t.Fatalf("bloco console %d não tem uma query: %q", i+1, cmd)
		}
		code := a.run("query", "--config", cfg, "--dataset", "radius_sessions", sql)
		if code != caso.wantCode {
			t.Errorf("query %d: code = %d, want %d\n%s", i+1, code, caso.wantCode, a.stderr.String())
		}
		got := strings.TrimSpace(a.stdout.String() + a.stderr.String())
		if got != strings.TrimSpace(expected) {
			t.Errorf("saída da query %d:\n  obtida:\n%s\n  no README:\n%s", i+1, got, strings.TrimSpace(expected))
		}
	}
}

// eventosESummary separa o bloco `cat eventos.ndjson` + `nishidb ingest` nas
// duas partes: o corpus exibido e a line de resumo documentada.
func eventosESummary(t *testing.T, block string) (eventos, summary string) {
	t.Helper()
	var ev []string
	inCat := false
	for _, l := range strings.Split(block, "\n") {
		switch {
		case strings.HasPrefix(l, "$ cat "):
			inCat = true
		case strings.HasPrefix(l, "$ "):
			inCat = false
		case inCat && strings.TrimSpace(l) != "":
			ev = append(ev, l)
		case strings.HasPrefix(l, "ingest:"):
			summary = l
		}
	}
	if len(ev) == 0 || summary == "" {
		t.Fatalf("bloco de ingest sem eventos ou sem resumo:\n%s", block)
	}
	return strings.Join(ev, "\n") + "\n", summary
}

// sqlOfComando extrai o SQL entre aspas duplas da line de comando documentada.
func sqlOfComando(cmd string) (string, bool) {
	i := strings.Index(cmd, `"`)
	j := strings.LastIndex(cmd, `"`)
	if i < 0 || j <= i {
		return "", false
	}
	return cmd[i+1 : j], true
}
