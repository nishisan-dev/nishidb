package renametools

import (
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Member é um nó do grafo de interfaces: um método de interface ou o método
// de um tipo concreto que o satisfaz.
type Member struct {
	Pkg, Type, Method, Role, Pos string
	Obj                          *types.Func
}

// Groups são as componentes conexas. ByKey liga a posição de declaração do
// método (que é a identidade que sobrevive às variantes de teste) ao id do
// grupo; Members lista cada grupo.
type Groups struct {
	ByKey   map[string]int
	Members map[int][]Member
}

// InterfaceGroups monta o grafo e o fecha por union-find.
//
// Aresta: I.M ~ T.M quando T (ou *T) implementa I. Interface que implementa
// interface também liga (I2 ⊇ I1). Duas interfaces que só compartilham o nome
// de um método, sem uma implementar a outra, ligam-se pelos implementadores
// comuns — que é exatamente o caso de query.Store/executor.Store/planner.Store
// no NishiDB, todos satisfeitos por storage.Store.
func InterfaceGroups(pkgs []*packages.Package, fset *token.FileSet) *Groups {
	type namedIn struct {
		named *types.Named
		pkg   *packages.Package
	}
	var ifaces, concretes []namedIn
	seen := map[string]bool{}
	for _, p := range ModulePackages(pkgs) {
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok {
				continue
			}
			key := PosKey(fset, tn.Pos())
			if seen[key] {
				continue
			}
			seen[key] = true
			if _, isIface := named.Underlying().(*types.Interface); isIface {
				ifaces = append(ifaces, namedIn{named, p})
			} else {
				concretes = append(concretes, namedIn{named, p})
			}
		}
	}

	parent := map[string]string{}
	members := map[string]Member{}
	var find func(string) string
	find = func(k string) string {
		if parent[k] == "" {
			parent[k] = k
			return k
		}
		if parent[k] != k {
			parent[k] = find(parent[k])
		}
		return parent[k]
	}
	union := func(a, b string) { parent[find(a)] = find(b) }
	add := func(m Member) string {
		k := PosKey(fset, m.Obj.Pos())
		if _, ok := members[k]; !ok {
			members[k] = m
			find(k)
		}
		return k
	}
	root := ModuleRoot(pkgs)
	member := func(n namedIn, fn *types.Func, role string) Member {
		p := fset.Position(fn.Pos())
		rel, _ := filepath.Rel(root, p.Filename)
		return Member{
			Pkg:    PackageDir(n.pkg, root),
			Type:   n.named.Obj().Name(),
			Method: fn.Name(),
			Role:   role,
			Pos:    fmt.Sprintf("%s:%d", filepath.ToSlash(rel), p.Line),
			Obj:    fn,
		}
	}

	for _, in := range ifaces {
		it := in.named.Underlying().(*types.Interface)
		// NumExplicitMethods/ExplicitMethod (não NumMethods/Method, que
		// resolvem embedding): uma interface que só embute outra (`type
		// Bigger interface { Escritor }`) não declara Apenda por si, e
		// atribuir a ela a posição de Escritor.Apenda pelo `add` baseado em
		// posição roubaria a atribuição de quem de fato declara o método —
		// Bigger participa como implementador/other quando quem embute É a
		// origem, não quando é ela mesma a embutida.
		if it.NumExplicitMethods() == 0 {
			continue
		}
		for i := 0; i < it.NumExplicitMethods(); i++ {
			im := it.ExplicitMethod(i)
			ik := add(member(in, im, "interface"))
			for _, c := range concretes {
				if !types.Implements(c.named, it) && !types.Implements(types.NewPointer(c.named), it) {
					continue
				}
				obj, index, _ := types.LookupFieldOrMethod(types.NewPointer(c.named), true, im.Pkg(), im.Name())
				cm, ok := obj.(*types.Func)
				if !ok {
					continue
				}
				if len(index) != 1 {
					// Método promovido de campo embutido (struct ou
					// ponteiro): c não declara Apenda, só herda; quem
					// declara é o dono do campo embutido, que aparece na
					// sua própria passada por concretes (ou não aparece, se
					// for de fora do módulo — também correto, não há
					// identificador local para renomear).
					continue
				}
				union(ik, add(member(c, cm, "implementador")))
			}
			for _, other := range ifaces {
				if other.named == in.named || !types.Implements(other.named, it) {
					continue
				}
				oit := other.named.Underlying().(*types.Interface)
				// NumExplicitMethods/ExplicitMethod aqui pela MESMA razão do
				// laço de fora: other.Method(j) resolveria embedding e
				// devolveria o Func de quem de fato declara — se essa posição
				// ainda não tiver sido reivindicada (a declarante real só
				// chega à sua própria vez mais adiante em ifaces), other
				// roubaria a atribuição por tabela, mesmo sem declarar nada.
				// Achado no repositório real: internal/query.Store (só
				// embute executor.Store e planner.Store, zero método
				// explícito) roubava Indexado de internal/query/planner.Store
				// por satisfazer de raspão internal/query/meta.Capacidades,
				// que só compartilha a FORMA do método, não a implementação.
				for j := 0; j < oit.NumExplicitMethods(); j++ {
					if om := oit.ExplicitMethod(j); om.Name() == im.Name() {
						union(ik, add(member(other, om, "interface")))
					}
				}
			}
		}
	}

	g := &Groups{ByKey: map[string]int{}, Members: map[int][]Member{}}
	roots := map[string]int{}
	keys := make([]string, 0, len(members))
	for k := range members {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := find(k)
		id, ok := roots[r]
		if !ok {
			id = len(roots) + 1
			roots[r] = id
		}
		g.ByKey[k] = id
		g.Members[id] = append(g.Members[id], members[k])
	}
	return g
}

// Rows é o TSV dos grupos.
func (g *Groups) Rows() [][]string {
	rows := [][]string{{"grupo", "pacote", "tipo", "metodo", "papel", "posicao"}}
	ids := make([]int, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		ms := g.Members[id]
		sort.Slice(ms, func(i, j int) bool {
			return strings.Compare(ms[i].Role+ms[i].Pkg+ms[i].Type, ms[j].Role+ms[j].Pkg+ms[j].Type) < 0
		})
		for _, m := range ms {
			rows = append(rows, []string{fmt.Sprintf("G%d", id), m.Pkg, m.Type, m.Method, m.Role, m.Pos})
		}
	}
	return rows
}
