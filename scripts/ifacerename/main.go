// Command ifacerename renomeia métodos de interface com o fecho transitivo
// que nem o rf nem o gopls fazem entre pacotes (spec de identificadores em
// inglês, §4.2): interface, todas as interfaces que a compartilham por
// implementador comum, e todos os tipos concretos que as satisfazem —
// dublês de teste inclusos — numa passada só, com o godoc de cada declaração
// começando pelo nome novo. Recusa o lote inteiro antes de escrever se um
// membro já tiver o nome novo no method set.
//
//	go run ./scripts/ifacerename -dir . -groups planning/identificadores-em-ingles/grupos-de-interface-2.tsv
//
// Quem chama roda `go build ./... && go vet ./...` em seguida; se ficar
// vermelho, `git checkout -- .` e o mapeamento volta ao juiz.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ifacerename", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "raiz do módulo")
	groupsPath := fs.String("groups", "", "TSV: pacote interface metodo novo (obrigatório)")
	dryRun := fs.Bool("dry-run", false, "só lista o que mudaria")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *groupsPath == "" {
		_, _ = fmt.Fprintln(stderr, "ifacerename: -groups é obrigatório")
		return 2
	}
	entries, err := readEntries(*groupsPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "ifacerename: %v\n", err)
		return 2
	}
	rep, err := Apply(*dir, entries, *dryRun)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "ifacerename: recusado: %v\n", err)
		return 1
	}
	for _, m := range rep.Renamed {
		_, _ = fmt.Fprintf(stdout, "%s\t%s.%s\t%s\n", m.Role, m.Pkg, m.Type, m.Method)
	}
	for _, f := range rep.Files {
		_, _ = fmt.Fprintf(stdout, "arquivo\t%s\n", f)
	}
	_, _ = fmt.Fprintf(stdout, "%d métodos em %d arquivos\n", len(rep.Renamed), len(rep.Files))
	return 0
}

func readEntries(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []Entry
	sc := bufio.NewScanner(f)
	first := true
	for sc.Scan() {
		l := sc.Text()
		if first {
			first = false
			if strings.HasPrefix(l, "pacote\t") {
				continue
			}
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		c := strings.Split(l, "\t")
		if len(c) < 4 {
			return nil, fmt.Errorf("%s: linha com %d colunas, esperado 4: %q", path, len(c), l)
		}
		out = append(out, Entry{Pkg: c[0], Iface: c[1], Method: c[2], New: c[3]})
	}
	return out, sc.Err()
}
