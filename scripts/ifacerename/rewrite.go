package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/nishisan-dev/nishidb/scripts/renametools"
)

// Entry é uma linha do TSV de grupos de interface: qual método de qual
// interface, e o nome novo. O grupo inteiro (spec §4.2) vem do fecho.
type Entry struct {
	Pkg, Iface, Method, New string
}

// Report é o que o aplicador lê antes de commitar.
type Report struct {
	Files   []string
	Renamed []renametools.Member
}

// edit é uma substituição por posição: o texto velho é guardado para a
// escrita conferir que o arquivo ainda é o que o go/types leu.
type edit struct {
	off       int
	old, text string
}

// Apply renomeia os grupos das entradas numa passada só. Recusa antes de
// escrever: nome novo já presente no method set de qualquer membro, ou
// entradas do mesmo grupo com nomes novos diferentes.
func Apply(dir string, entries []Entry, dryRun bool) (*Report, error) {
	pkgs, fset, err := renametools.LoadModule(dir, "./...")
	if err != nil {
		return nil, err
	}
	groups := renametools.InterfaceGroups(pkgs, fset)

	// grupo → nome novo, com a checagem de divergência.
	newByGroup := map[int]string{}
	for _, e := range entries {
		gid, ok := findGroup(groups, e)
		if !ok {
			return nil, fmt.Errorf("%s.%s.%s: método de interface não encontrado no módulo", e.Pkg, e.Iface, e.Method)
		}
		if prev, ok := newByGroup[gid]; ok && prev != e.New {
			return nil, fmt.Errorf("%s.%s.%s: entradas do mesmo grupo G%d pedem %q e %q", e.Pkg, e.Iface, e.Method, gid, prev, e.New)
		}
		newByGroup[gid] = e.New
	}

	// A ordem do mapa é aleatória, e Report.Renamed é saída para leitura
	// humana antes do commit: percorrer os grupos ordenados é o que faz duas
	// rodadas do mesmo lote imprimirem a mesma coisa.
	gids := make([]int, 0, len(newByGroup))
	for gid := range newByGroup {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	// Colisão: cada membro do grupo é um método de um tipo; o tipo não pode
	// já ter membro com o nome novo.
	newByKey := map[string]string{}
	var renamed []renametools.Member
	for _, gid := range gids {
		newName := newByGroup[gid]
		for _, m := range groups.Members[gid] {
			if err := checkCollision(m, newName); err != nil {
				return nil, err
			}
			newByKey[renametools.PosKey(fset, m.Obj.Pos())] = newName
			renamed = append(renamed, m)
		}
	}

	edits := map[string]map[int]edit{} // arquivo → offset → edição
	addEdit := func(pos token.Pos, old, newName string) {
		p := fset.Position(pos)
		if edits[p.Filename] == nil {
			edits[p.Filename] = map[int]edit{}
		}
		edits[p.Filename][p.Offset] = edit{p.Offset, old, newName}
	}

	for _, p := range renametools.ModulePackages(pkgs) {
		for id, obj := range p.TypesInfo.Defs {
			fn, ok := obj.(*types.Func)
			if !ok {
				continue
			}
			if newName, ok := newByKey[renametools.PosKey(fset, fn.Pos())]; ok {
				addEdit(id.Pos(), id.Name, newName)
			}
		}
		for id, obj := range p.TypesInfo.Uses {
			fn, ok := obj.(*types.Func)
			if !ok {
				continue
			}
			if newName, ok := newByKey[renametools.PosKey(fset, fn.Pos())]; ok {
				addEdit(id.Pos(), id.Name, newName)
			}
		}
		for _, f := range p.Syntax {
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Doc == nil {
					continue
				}
				obj := p.TypesInfo.Defs[fd.Name]
				if obj == nil {
					continue
				}
				if newName, ok := newByKey[renametools.PosKey(fset, obj.Pos())]; ok {
					docEdit(fd.Doc, fd.Name.Name, newName, addEdit)
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				it, ok := n.(*ast.InterfaceType)
				if !ok {
					return true
				}
				for _, m := range it.Methods.List {
					if m.Doc == nil || len(m.Names) == 0 {
						continue
					}
					obj := p.TypesInfo.Defs[m.Names[0]]
					if obj == nil {
						continue
					}
					if newName, ok := newByKey[renametools.PosKey(fset, obj.Pos())]; ok {
						docEdit(m.Doc, m.Names[0].Name, newName, addEdit)
					}
				}
				return true
			})
		}
	}

	rep := &Report{Renamed: renamed}
	files := make([]string, 0, len(edits))
	for f := range edits {
		files = append(files, f)
	}
	sort.Strings(files)
	rep.Files = files
	if dryRun {
		return rep, nil
	}
	for _, f := range files {
		if err := applyEdits(f, edits[f]); err != nil {
			return nil, err
		}
		if out, err := exec.Command("gofmt", "-w", f).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("gofmt %s: %v: %s", f, err, out)
		}
	}
	return rep, nil
}

// checkCollision recusa o lote quando o tipo do membro já tem um método (ou
// campo) com o nome novo.
//
// O method set de `*I`, com I interface, é VAZIO: perguntar por
// `types.NewPointer(recv)` quando o receptor é a própria interface esconderia
// justamente a colisão que importa — renomear Escritor.Apenda para Fecha
// declararia Fecha duas vezes na mesma interface. Para tipo concreto vale o
// contrário: é `*T` com addressable que traz também os métodos de receptor
// ponteiro, que são a maioria dos implementadores.
func checkCollision(m renametools.Member, newName string) error {
	recv := m.Obj.Type().(*types.Signature).Recv().Type()
	if ptr, ok := recv.(*types.Pointer); ok {
		recv = ptr.Elem()
	}
	lookIn, addressable := recv, false
	if _, isIface := recv.Underlying().(*types.Interface); !isIface {
		lookIn, addressable = types.NewPointer(recv), true
	}
	if obj, _, _ := types.LookupFieldOrMethod(lookIn, addressable, m.Obj.Pkg(), newName); obj != nil {
		return fmt.Errorf("%s.%s já tem %s: renomear %s colidiria", m.Pkg, m.Type, newName, m.Method)
	}
	return nil
}

// docEdit troca a PRIMEIRA palavra do godoc quando ela é o nome velho —
// a forma que a regra `exported` do revive cobra. Só a primeira linha do
// grupo; menções no meio da prosa são trabalho do renamecite.
func docEdit(doc *ast.CommentGroup, old, newName string, add func(token.Pos, string, string)) {
	c := doc.List[0]
	text := c.Text
	prefix := "// "
	if !strings.HasPrefix(text, prefix) {
		return
	}
	rest := text[len(prefix):]
	if rest != old && !strings.HasPrefix(rest, old+" ") && !strings.HasPrefix(rest, old+".") && !strings.HasPrefix(rest, old+",") {
		return
	}
	add(c.Pos()+token.Pos(len(prefix)), old, newName)
}

func applyEdits(file string, es map[int]edit) error {
	src, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	offs := make([]int, 0, len(es))
	for o := range es {
		offs = append(offs, o)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(offs)))
	out := append([]byte(nil), src...)
	for _, o := range offs {
		e := es[o]
		end := o + len(e.old)
		if end > len(out) || string(out[o:end]) != e.old {
			return fmt.Errorf("%s: em %d esperava %q, o arquivo mudou desde a carga", file, o, e.old)
		}
		out = append(out[:o], append([]byte(e.text), out[end:]...)...)
	}
	return os.WriteFile(file, out, 0o644)
}

func findGroup(g *renametools.Groups, e Entry) (int, bool) {
	for gid, ms := range g.Members {
		for _, m := range ms {
			if m.Role == "interface" && m.Pkg == e.Pkg && m.Type == e.Iface && m.Method == e.Method {
				return gid, true
			}
		}
	}
	return 0, false
}
