package renametools

import (
	"fmt"
	"go/token"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// LoadModule carrega o módulo com testes. As variantes de teste
// (`a [a.test]`, `a_test [a.test]`) vêm junto porque os dublês de interface
// moram nelas, e um inventário que não os visse deixaria o ifacerename cego
// para metade dos implementadores.
func LoadModule(dir, pattern string) ([]*packages.Package, *token.FileSet, error) {
	fset := token.NewFileSet()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps | packages.NeedModule,
		Tests: true,
		Dir:   dir,
		Fset:  fset,
	}
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		return nil, nil, err
	}
	var errs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			errs = append(errs, e.Error())
		}
	})
	if len(errs) > 0 {
		return nil, nil, fmt.Errorf("o módulo não compila: %s", strings.Join(errs, "; "))
	}
	return pkgs, fset, nil
}

// ModulePackages filtra os pacotes do próprio módulo (o Load traz as
// dependências por causa do NeedDeps).
func ModulePackages(pkgs []*packages.Package) []*packages.Package {
	var out []*packages.Package
	seen := map[string]bool{}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if p.Module == nil || !p.Module.Main || seen[p.ID] {
			return
		}
		seen[p.ID] = true
		out = append(out, p)
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// PackageDir devolve o diretório do pacote relativo à raiz do módulo, que é
// o que o `rf` recebe como diretório corrente e o que o TSV chama de pacote.
func PackageDir(p *packages.Package, root string) string {
	if len(p.GoFiles) == 0 && len(p.CompiledGoFiles) == 0 {
		return p.PkgPath
	}
	files := p.GoFiles
	if len(files) == 0 {
		files = p.CompiledGoFiles
	}
	rel, err := filepath.Rel(root, filepath.Dir(files[0]))
	if err != nil {
		return p.PkgPath
	}
	return filepath.ToSlash(rel)
}

// ModuleRoot devolve o diretório absoluto do módulo principal (p.Module.Dir);
// o fallback "." só ocorre quando nada foi carregado.
func ModuleRoot(pkgs []*packages.Package) string {
	for _, p := range ModulePackages(pkgs) {
		if p.Module != nil && p.Module.Dir != "" {
			return p.Module.Dir
		}
	}
	return "."
}

// PosKey é a identidade de um objeto entre variantes de pacote: o mesmo
// método aparece como *types.Func distinto em `a` e em `a [a.test]`, mas na
// mesma posição do mesmo arquivo.
func PosKey(fset *token.FileSet, pos token.Pos) string {
	p := fset.Position(pos)
	return fmt.Sprintf("%s:%d", p.Filename, p.Offset)
}
