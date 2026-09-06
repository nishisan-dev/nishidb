// Command renamecite é a passada de prosa do ciclo de identificadores em
// inglês (spec §4.4): reescreve citações — token entre crases, doc link,
// `pkg.Ident` em comentário Go, `TestXxx`/`test_xxx`, caminho de arquivo ou
// pacote, chave de log entre crases ou aspas, nome de subteste em t.Run —
// a partir dos TSV de mapeamento. Palavra solta na prosa nunca é tocada.
//
//	go run ./scripts/renamecite -dir . \
//	    -idents planning/identificadores-em-ingles/mapeamento-2-G1-types.tsv \
//	    -paths planning/identificadores-em-ingles/mapeamento-pacotes.tsv \
//	    -protect planning/identificadores-em-ingles/protegidos.txt \
//	    -report /tmp/renamecite.txt
//
// # O RELATÓRIO É A GUARDA, E LISTA TUDO
//
// Duas formas de linha, uma por ocorrência:
//
//	aplicado	<arquivo>:<linha>	<velho>	<novo>	<regra>
//	pulado	<arquivo>:<linha>	<token>	<motivo>
//
// As regras são `crase`, `aspas`, `teste`, `caminho`, `doclink`,
// `qualificado`, `literal` e `subteste`. Listar SÓ as pulas não serve: o modo
// de falha real desta ferramenta é substituir o que não devia, em silêncio, e
// quem revisa o lote precisa ler o que ela fez sem depender do diff.
//
// # O QUE ELA NUNCA VARRE
//
// Os documentos DE → PARA do próprio ciclo (ver defaultExcludes) e o que
// -exclude acrescentar. Ali a coluna da esquerda é o nome velho por
// construção, e varrer colapsa cada linha em `X → X`.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// multi é um flag repetível: cada -idents/-paths/... acrescenta um arquivo.
type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

// textExtensions são os arquivos de texto que a passada varre; `Makefile` e
// `CLAUDE.md` entram por nome, e `.go` tem passada própria.
var textExtensions = map[string]bool{".md": true, ".puml": true, ".sh": true, ".py": true, ".yml": true, ".yaml": true, ".java": true}

// defaultExcludes são os caminhos que a passada NUNCA pode varrer. Os
// quatro primeiros são o registro DE → PARA do próprio ciclo, e a razão é a
// mesma para os quatro: a coluna da esquerda é, por construção, o nome
// velho entre crases. Varridos, cada linha colapsa em `X → X` e o
// mapeamento some — sem erro, sem relatório, e com o dado que permitiria
// desfazer já perdido. O padrão de diretório vale para a subárvore inteira:
// os mapeamentos TSV versionados moram em planning/identificadores-em-ingles/.
//
// scripts/renamecite entra por um motivo diferente: os testes da PRÓPRIA
// ferramenta (rules_test.go, main_test.go) usam nomes reais de pacote do
// repositório como fixture de tabela De → Para da lógica de reescrita — por
// exemplo `m.Paths["internal/contadores"] = "internal/counters"` — para
// ilustrar o caso com um exemplo verdadeiro em vez de um `Foo`/`Bar`
// qualquer. Quando um ciclo de rename de verdade usa esses mesmos nomes, a
// varredura encontra o mesmo `X → X` do parágrafo acima, só que aqui o
// "documento" é código Go: o fixture não fica em silêncio, vira um caso de
// teste que passa testando uma identidade sem sentido. Achado da Tarefa 1.1
// do ciclo de identificadores em inglês: uma rodada real contra
// internal/predicado colapsou TestImportPathIsNotRewritten (src e want
// ficaram iguais, o teste parou de provar que import Go não é tocado) e
// mais dois casos de mesa.
var defaultExcludes = []string{
	"docs/glossario.md",
	"planning/identificadores-em-ingles.md",
	"planning/identificadores-em-ingles-plano.md",
	"planning/identificadores-em-ingles",
	"scripts/renamecite",
}

// excluded diz se o caminho relativo cai em algum padrão — o próprio caminho
// ou QUALQUER diretório ancestral, que é o que faz um padrão de diretório
// excluir a subárvore inteira.
func excluded(rel string, patterns []string) bool {
	rel = filepath.ToSlash(rel)
	for _, p := range patterns {
		if ok, _ := path.Match(p, rel); ok {
			return true
		}
		for d := path.Dir(rel); d != "." && d != "/"; d = path.Dir(d) {
			if ok, _ := path.Match(p, d); ok {
				return true
			}
		}
	}
	return false
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("renamecite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", ".", "raiz do repositório")
	var idents, paths, tests, logkeys, subtests, protect, exclude multi
	flags.Var(&idents, "idents", "TSV pacote/especie/endereco/novo/motivo (repetível)")
	flags.Var(&paths, "paths", "TSV velho/novo de caminhos (repetível)")
	flags.Var(&tests, "tests", "TSV pacote/velho/novo de testes (repetível)")
	flags.Var(&logkeys, "logkeys", "TSV velho/novo de chaves de log (repetível)")
	flags.Var(&subtests, "subtests", "TSV arquivo/velho/novo de subtestes (repetível)")
	flags.Var(&protect, "protect", "lista de tokens que a regra de identificador não toca, um por linha (repetível)")
	flags.Var(&exclude, "exclude", "glob relativo à raiz de arquivo ou diretório a não varrer (repetível)")
	report := flags.String("report", "", "arquivo do relatório do que foi aplicado e do que ficou de fora (obrigatório)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *report == "" {
		_, _ = fmt.Fprintln(stderr, "renamecite: -report é obrigatório")
		return 2
	}
	excludes := append(append([]string(nil), defaultExcludes...), exclude...)
	for _, p := range excludes {
		if _, err := path.Match(p, "x"); err != nil {
			_, _ = fmt.Fprintf(stderr, "renamecite: -exclude %q: %v\n", p, err)
			return 2
		}
	}
	m, err := loadMapping(idents, paths, tests, logkeys, subtests, protect)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "renamecite: %v\n", err)
		return 2
	}
	reportFile, err := os.Create(*report)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "renamecite: %v\n", err)
		return 1
	}
	defer func() { _ = reportFile.Close() }()
	touched := 0
	err = filepath.WalkDir(*dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(*dir, p)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", ".superpowers", "target", ".venv", "__pycache__", "node_modules":
				return filepath.SkipDir
			}
			if rel != "." && excluded(rel, excludes) {
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(p)
		base := d.Name()
		if ext != ".go" && !textExtensions[ext] && base != "Makefile" && base != "CLAUDE.md" {
			return nil
		}
		if excluded(rel, excludes) {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var out []byte
		var rep *Report
		if ext == ".go" {
			out, rep = RewriteGoFile(rel, src, m)
		} else {
			out, rep = RewriteText(src, ext, m)
		}
		for _, a := range rep.Applied {
			_, _ = fmt.Fprintf(reportFile, "aplicado\t%s:%d\t%s\t%s\t%s\n", rel, a.Line, a.Old, a.New, a.Rule)
		}
		for _, s := range rep.Skipped {
			_, _ = fmt.Fprintf(reportFile, "pulado\t%s:%d\t%s\t%s\n", rel, s.Line, s.Token, s.Reason)
		}
		if string(out) != string(src) {
			touched++
			return os.WriteFile(p, out, 0o644)
		}
		return nil
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "renamecite: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "%d arquivos reescritos; relatório em %s\n", touched, *report)
	return 0
}

// loadMapping lê os TSV de mapeamento. No -idents, o endereço `Fila.Empurra`
// entra como Old inteiro (`T.M`) e `Compila` como nome nu; param, local,
// label e receiver são descartados porque prosa não os cita por endereço.
//
// Os identificadores entram em DUAS passadas — os tipos primeiro, o resto
// depois —, porque o endereço novo de um método ou campo é sintetizado com o
// receptor, e o receptor pode estar sendo renomeado no mesmo lote. Ver
// Mapping.addIdentRow.
func loadMapping(idents, paths, tests, logkeys, subtests, protect []string) (*Mapping, error) {
	m := NewMapping()
	type identRow struct{ pkg, kind, address, newLeaf string }
	var rows []identRow
	for _, f := range idents {
		if err := eachRow(f, 5, func(c []string) {
			kind := strings.SplitN(c[1], "#", 2)[0]
			switch kind {
			case "param", "local", "label", "receiver":
				return // prosa não cita local por endereço; e "mantido" tem novo == velho
			}
			if c[3] == "" || strings.HasSuffix(c[2], "."+c[3]) || c[2] == c[3] {
				return
			}
			rows = append(rows, identRow{c[0], kind, c[2], c[3]})
		}); err != nil {
			return nil, err
		}
	}
	for _, r := range rows {
		if r.kind == "type" {
			m.addIdentRow(r.pkg, r.kind, r.address, r.newLeaf)
		}
	}
	for _, r := range rows {
		if r.kind != "type" {
			m.addIdentRow(r.pkg, r.kind, r.address, r.newLeaf)
		}
	}
	for _, f := range protect {
		if err := eachToken(f, func(tok string) { m.Protected[tok] = true }); err != nil {
			return nil, err
		}
	}
	for _, f := range paths {
		if err := eachRow(f, 2, func(c []string) {
			m.Paths[c[0]] = c[1]
			if !strings.Contains(c[0], ".go") {
				m.Packages[shortPkg(c[0])] = shortPkg(c[1])
			}
		}); err != nil {
			return nil, err
		}
	}
	for _, f := range tests {
		if err := eachRow(f, 3, func(c []string) { m.Tests[c[1]] = c[2] }); err != nil {
			return nil, err
		}
	}
	for _, f := range logkeys {
		if err := eachRow(f, 2, func(c []string) { m.LogKeys[c[0]] = c[1] }); err != nil {
			return nil, err
		}
	}
	for _, f := range subtests {
		if err := eachRow(f, 3, func(c []string) {
			if m.Subtests[c[0]] == nil {
				m.Subtests[c[0]] = map[string]string{}
			}
			m.Subtests[c[0]][c[1]] = c[2]
		}); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// eachToken lê a lista do -protect: um token por linha, SEM cabeçalho (não é
// TSV), linha vazia ignorada e `#` na primeira coluna comentando a linha.
func eachToken(path string, fn func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		fn(l)
	}
	return sc.Err()
}

// eachRow lê um TSV com cabeçalho e chama fn por linha, recusando o arquivo
// inteiro se alguma linha tiver menos colunas que o esperado.
func eachRow(path string, cols int, fn func([]string)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	first := true
	for sc.Scan() {
		l := sc.Text()
		if first {
			first = false
			continue // cabeçalho
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		c := strings.Split(l, "\t")
		if len(c) < cols {
			return fmt.Errorf("%s: linha com %d colunas, esperado %d: %q", path, len(c), cols, l)
		}
		fn(c)
	}
	return sc.Err()
}
