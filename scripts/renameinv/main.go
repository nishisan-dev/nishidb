// Command renameinv inventaria o módulo Go do diretório dado: toda declaração
// nomeada (tipos, funções, métodos, campos, constantes, variáveis, parâmetros,
// locais, rótulos) com o endereço que o `rf` entende, e os grupos de interface
// — componentes conexas do grafo (interface, método) ↔ (tipo concreto, método).
//
// É a entrada dos agentes do ciclo de identificadores em inglês
// (planning/identificadores-em-ingles.md, §5 e §8) e a prova de completude do
// fechamento: o inventário do fim é comparado ao do início, e nada pode sobrar
// sem decisão. Grep por acento não prova nada; isto é go/types.
//
//	go run ./scripts/renameinv -dir . -out inventario.tsv -groups grupos.tsv
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/nishisan-dev/nishidb/scripts/renametools"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("renameinv", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "raiz do módulo a inventariar")
	out := fs.String("out", "", "TSV do inventário (obrigatório)")
	groups := fs.String("groups", "", "TSV dos grupos de interface (obrigatório)")
	pattern := fs.String("pkg", "./...", "padrão de pacotes, como em go list")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *out == "" || *groups == "" {
		fmt.Fprintln(stderr, "renameinv: -out e -groups são obrigatórios")
		return 2
	}
	inv, err := renametools.Inventory(*dir, *pattern)
	if err != nil {
		fmt.Fprintf(stderr, "renameinv: %v\n", err)
		return 1
	}
	if err := writeTSV(*out, inv.Rows); err != nil {
		fmt.Fprintf(stderr, "renameinv: %v\n", err)
		return 1
	}
	if err := writeTSV(*groups, inv.GroupRows); err != nil {
		fmt.Fprintf(stderr, "renameinv: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%d declarações, %d grupos de interface\n", len(inv.Rows)-1, inv.Groups)
	return 0
}

func writeTSV(path string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, r := range rows {
		for i, c := range r {
			if i > 0 {
				if _, err := f.WriteString("\t"); err != nil {
					return err
				}
			}
			if _, err := f.WriteString(c); err != nil {
				return err
			}
		}
		if _, err := f.WriteString("\n"); err != nil {
			return err
		}
	}
	return nil
}
