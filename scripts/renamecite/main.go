// Command renamecite é a passada de prosa do ciclo de identificadores em
// inglês (spec §4.4): reescreve citações — token entre crases, doc link,
// `pkg.Ident` em comentário Go, `TestXxx`/`test_xxx`, caminho de arquivo ou
// pacote, chave de log entre crases ou aspas, nome de subteste em t.Run —
// a partir dos TSV de mapeamento, e relata o que casou mas ficou de fora
// (ambiguidade). Palavra solta na prosa nunca é tocada.
//
//	go run ./scripts/renamecite -dir . \
//	    -idents planning/identificadores-em-ingles/mapeamento-2-G1-types.tsv \
//	    -paths planning/identificadores-em-ingles/mapeamento-pacotes.tsv \
//	    -report /tmp/renamecite.txt
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
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

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("renamecite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dir := flags.String("dir", ".", "raiz do repositório")
	var idents, paths, tests, logkeys, subtests multi
	flags.Var(&idents, "idents", "TSV pacote/especie/endereco/novo/motivo (repetível)")
	flags.Var(&paths, "paths", "TSV velho/novo de caminhos (repetível)")
	flags.Var(&tests, "tests", "TSV pacote/velho/novo de testes (repetível)")
	flags.Var(&logkeys, "logkeys", "TSV velho/novo de chaves de log (repetível)")
	flags.Var(&subtests, "subtests", "TSV arquivo/velho/novo de subtestes (repetível)")
	report := flags.String("report", "", "arquivo do relatório do que ficou de fora (obrigatório)")
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
	m, err := loadMapping(idents, paths, tests, logkeys, subtests)
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
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", ".superpowers", "target", ".venv", "__pycache__", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(p)
		base := d.Name()
		if ext != ".go" && !textExtensions[ext] && base != "Makefile" && base != "CLAUDE.md" {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(*dir, p)
		var out []byte
		var skips []Skipped
		if ext == ".go" {
			out, skips = RewriteGoFile(filepath.ToSlash(rel), src, m)
		} else {
			out, skips = RewriteText(src, ext, m)
		}
		for _, s := range skips {
			_, _ = fmt.Fprintf(reportFile, "%s:%d\t%s\t%s\n", rel, s.Line, s.Token, s.Reason)
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
func loadMapping(idents, paths, tests, logkeys, subtests []string) (*Mapping, error) {
	m := NewMapping()
	for _, f := range idents {
		if err := eachRow(f, 5, func(c []string) {
			switch strings.SplitN(c[1], "#", 2)[0] {
			case "param", "local", "label", "receiver":
				return // prosa não cita local por endereço; e "mantido" tem novo == velho
			}
			if c[3] == "" || strings.HasSuffix(c[2], "."+c[3]) || c[2] == c[3] {
				return
			}
			m.AddIdent(c[0], c[2], c[3])
		}); err != nil {
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
