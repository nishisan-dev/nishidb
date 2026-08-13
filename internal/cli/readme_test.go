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

// blocosDeCodigo devolve o conteúdo de cada bloco cercado por ``` com a
// linguagem pedida, na ordem em que aparecem.
func blocosDeCodigo(t *testing.T, md, lang string) []string {
	t.Helper()
	var out []string
	linhas := strings.Split(md, "\n")
	for i := 0; i < len(linhas); i++ {
		if strings.TrimSpace(linhas[i]) != "```"+lang {
			continue
		}
		var corpo []string
		for i++; i < len(linhas) && strings.TrimSpace(linhas[i]) != "```"; i++ {
			corpo = append(corpo, linhas[i])
		}
		out = append(out, strings.Join(corpo, "\n"))
	}
	return out
}

// A duração e a taxa mudam a cada execução: são o único trecho da saída
// documentada que não pode ser comparado literalmente.
var reTempo = regexp.MustCompile(`em \d+(\.\d+)?[a-zµ]+ \(\d+ ev/s\)`)

func normalizaTempos(s string) string { return reTempo.ReplaceAllString(s, "em T (R ev/s)") }

// comandoESaida separa um bloco ```console em (comando, saída esperada). O
// comando continua na linha seguinte por dois motivos: '\' no fim (quebra de
// shell) ou aspas ainda ABERTAS — que é o caso do exemplo de erro, cujo SQL
// ocupa duas linhas de propósito, para mostrar linha e coluna.
func comandoESaida(bloco string) (cmd string, saida string) {
	var esperado []string
	emComando := false
	for _, l := range strings.Split(bloco, "\n") {
		switch {
		case strings.HasPrefix(l, "$ "):
			emComando = true
			cmd = strings.TrimSuffix(strings.TrimPrefix(l, "$ "), "\\")
		case emComando && strings.Count(cmd, `"`)%2 == 1:
			cmd += "\n" + l // dentro de aspas: a quebra faz parte do argumento
		case emComando && strings.HasSuffix(cmd, " "):
			cmd += strings.TrimSuffix(strings.TrimSpace(l), "\\")
		default:
			emComando = false
			esperado = append(esperado, l)
		}
	}
	return cmd, strings.Join(esperado, "\n")
}

func TestQuickstartDoREADMEExecuta(t *testing.T) {
	bruto, err := os.ReadFile(readmePath)
	if err != nil {
		t.Fatal(err)
	}
	md := string(bruto)

	yamls := blocosDeCodigo(t, md, "yaml")
	if len(yamls) < 2 {
		t.Fatalf("%d blocos yaml no README, want ao menos 2 (config e dataset)", len(yamls))
	}

	raiz := t.TempDir()
	dados := filepath.Join(raiz, "dados")
	specs := filepath.Join(raiz, "datasets")
	for _, d := range []string{dados, specs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Os caminhos do README são os de uma instalação real; só eles mudam.
	cfgYAML := strings.NewReplacer(
		"/var/lib/nishidb", dados,
		"/etc/nishidb/datasets", specs,
	).Replace(yamls[0])
	cfg := filepath.Join(raiz, "nishidb.yaml")
	if err := os.WriteFile(cfg, []byte(cfgYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(specs, "radius_sessions.yaml"), []byte(yamls[1]), 0o644); err != nil {
		t.Fatal(err)
	}

	consoles := blocosDeCodigo(t, md, "console")
	if len(consoles) < 3 {
		t.Fatalf("%d blocos console no README, want 3 (eventos+ingest, query, erro)", len(consoles))
	}

	// Bloco 1: `cat eventos.ndjson` seguido do ingest. O conteúdo exibido pelo
	// cat É o corpus — é ele que vai para o stdin do ingest.
	eventos, resumoEsperado := eventosEResumo(t, consoles[0])
	a := &ambiente{dir: raiz, config: cfg}
	if code := a.rodaComEntrada(eventos, "ingest", "--config", cfg,
		"--dataset", "radius_sessions", "--input", "-"); code != cli.ExitOK {
		t.Fatalf("o ingest do README saiu com %d:\n%s", code, a.stderr.String())
	}
	if got, want := normalizaTempos(strings.TrimSpace(a.stderr.String())), normalizaTempos(resumoEsperado); got != want {
		t.Errorf("resumo do ingest:\n  obtido:   %s\n  no README: %s", got, want)
	}

	// Blocos 2 e 3: as duas consultas, com a saída conferida linha a linha.
	casos := []struct {
		bloco    string
		wantCode int
	}{
		{consoles[1], cli.ExitOK},
		{consoles[2], cli.ExitQuery},
	}
	for i, caso := range casos {
		cmd, esperado := comandoESaida(caso.bloco)
		sql, ok := sqlDoComando(cmd)
		if !ok {
			t.Fatalf("bloco console %d não tem uma query: %q", i+1, cmd)
		}
		code := a.roda("query", "--config", cfg, "--dataset", "radius_sessions", sql)
		if code != caso.wantCode {
			t.Errorf("query %d: code = %d, want %d\n%s", i+1, code, caso.wantCode, a.stderr.String())
		}
		got := strings.TrimSpace(a.stdout.String() + a.stderr.String())
		if got != strings.TrimSpace(esperado) {
			t.Errorf("saída da query %d:\n  obtida:\n%s\n  no README:\n%s", i+1, got, strings.TrimSpace(esperado))
		}
	}
}

// eventosEResumo separa o bloco `cat eventos.ndjson` + `nishidb ingest` nas
// duas partes: o corpus exibido e a linha de resumo documentada.
func eventosEResumo(t *testing.T, bloco string) (eventos, resumo string) {
	t.Helper()
	var ev []string
	emCat := false
	for _, l := range strings.Split(bloco, "\n") {
		switch {
		case strings.HasPrefix(l, "$ cat "):
			emCat = true
		case strings.HasPrefix(l, "$ "):
			emCat = false
		case emCat && strings.TrimSpace(l) != "":
			ev = append(ev, l)
		case strings.HasPrefix(l, "ingest:"):
			resumo = l
		}
	}
	if len(ev) == 0 || resumo == "" {
		t.Fatalf("bloco de ingest sem eventos ou sem resumo:\n%s", bloco)
	}
	return strings.Join(ev, "\n") + "\n", resumo
}

// sqlDoComando extrai o SQL entre aspas duplas da linha de comando documentada.
func sqlDoComando(cmd string) (string, bool) {
	i := strings.Index(cmd, `"`)
	j := strings.LastIndex(cmd, `"`)
	if i < 0 || j <= i {
		return "", false
	}
	return cmd[i+1 : j], true
}
