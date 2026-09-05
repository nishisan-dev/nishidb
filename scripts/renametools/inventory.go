package renametools

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Result é o inventário pronto para virar TSV.
type Result struct {
	Rows      [][]string
	GroupRows [][]string
	Groups    int
}

// decl é uma declaração inventariada.
type decl struct {
	pkg, kind, address, name, pos, exported, group string
}

// Inventory percorre Defs de todo pacote do módulo e classifica cada objeto
// pelo que ele é para o go/types e pelo lugar onde foi declarado.
func Inventory(dir, pattern string) (*Result, error) {
	pkgs, fset, err := LoadModule(dir, pattern)
	if err != nil {
		return nil, err
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	groups := InterfaceGroups(pkgs, fset)

	byPos := map[string]decl{}
	for _, p := range ModulePackages(pkgs) {
		pkgDir := PackageDir(p, root)
		// Contagem de declarações do mesmo nome por função, para marcar o
		// endereço ambíguo que o rf recusaria.
		counts := map[string]int{}
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					id, ok := n.(*ast.Ident)
					if !ok {
						return true
					}
					if obj := p.TypesInfo.Defs[id]; obj != nil {
						counts[funcAddress(fn)+"."+id.Name]++
					}
					return true
				})
			}
		}
		for id, obj := range p.TypesInfo.Defs {
			if obj == nil || id.Name == "_" {
				continue
			}
			pos := fset.Position(obj.Pos())
			if !strings.HasPrefix(pos.Filename, root) {
				continue
			}
			rel, _ := filepath.Rel(root, pos.Filename)
			// Coluna entra no endereço de exibição porque a linha sozinha
			// colide entre declarações distintas na mesma linha (parâmetros
			// de uma assinatura de uma linha só, por exemplo) — sem a coluna
			// a deduplicação de variante de teste (mesma posição em `a` e em
			// `a [a.test]`) não teria como distinguir isso de colisão real.
			posStr := fmt.Sprintf("%s:%d:%d", filepath.ToSlash(rel), pos.Line, pos.Column)
			if _, ok := byPos[posStr+":"+id.Name]; ok {
				continue // variante de teste do mesmo arquivo
			}
			r, ok := classify(p, fset, id, obj, counts)
			if !ok {
				continue
			}
			r.pkg = pkgDir
			r.pos = posStr
			if obj.Exported() {
				r.exported = "sim"
			} else {
				r.exported = "nao"
			}
			if g, ok := groups.ByKey[PosKey(fset, obj.Pos())]; ok {
				r.group = fmt.Sprintf("G%d", g)
			}
			byPos[posStr+":"+id.Name] = r
		}
	}

	rows := [][]string{{"pacote", "especie", "endereco", "nome", "posicao", "exportado", "grupo"}}
	keys := make([]string, 0, len(byPos))
	for k := range byPos {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := byPos[k]
		rows = append(rows, []string{r.pkg, r.kind, r.address, r.name, r.pos, r.exported, r.group})
	}
	return &Result{Rows: rows, GroupRows: groups.Rows(), Groups: len(groups.Members)}, nil
}

// funcAddress é `F` ou `T.M`, o prefixo de endereço rf para o que mora
// dentro da função.
func funcAddress(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return receiverTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func receiverTypeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverTypeName(t.X)
	case *ast.IndexExpr:
		return receiverTypeName(t.X)
	case *ast.IndexListExpr:
		return receiverTypeName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// classify decide espécie e endereço. Devolve ok=false para o que não é
// renomeável por endereço: campo embutido, receptor curto, o pacote em si.
func classify(p *packages.Package, fset *token.FileSet, id *ast.Ident, obj types.Object, counts map[string]int) (decl, bool) {
	r := decl{name: id.Name}
	switch o := obj.(type) {
	case *types.PkgName:
		return r, false
	case *types.TypeName:
		if o.Parent() == p.Types.Scope() {
			r.kind, r.address = "type", id.Name
			return r, true
		}
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		r.kind, r.address = "local", funcAddress(fn)+"."+id.Name
		return r, true
	case *types.Func:
		sig := o.Type().(*types.Signature)
		if sig.Recv() == nil {
			r.kind, r.address = "func", id.Name
			return r, true
		}
		recv := sig.Recv().Type()
		if ptr, ok := recv.(*types.Pointer); ok {
			recv = ptr.Elem()
		}
		named, ok := recv.(*types.Named)
		if !ok {
			return r, false
		}
		if _, isIface := named.Underlying().(*types.Interface); isIface {
			r.kind = "ifacemethod"
		} else {
			r.kind = "method"
		}
		r.address = named.Obj().Name() + "." + id.Name
		return r, true
	case *types.Const:
		if o.Parent() == p.Types.Scope() {
			r.kind, r.address = "const", id.Name
			return r, true
		}
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		r.kind, r.address = "local", funcAddress(fn)+"."+id.Name
		return r, true
	case *types.Label:
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		r.kind, r.address = "label", funcAddress(fn)+"."+id.Name
		return r, true
	case *types.Var:
		if o.IsField() {
			owner := fieldOwner(p, id)
			if owner == "" || o.Embedded() {
				return r, false
			}
			r.kind, r.address = "field", owner+"."+id.Name
			return r, true
		}
		if o.Parent() == p.Types.Scope() {
			r.kind, r.address = "var", id.Name
			return r, true
		}
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		addr := funcAddress(fn) + "." + id.Name
		switch {
		case isReceiver(fn, id):
			if len(id.Name) < 4 {
				return r, false
			}
			r.kind = "receiver"
		case isParamOrResult(fn, id):
			r.kind = "param"
		default:
			r.kind = "local"
		}
		if counts[addr] > 1 {
			r.kind += "#ambiguo"
		}
		r.address = addr
		return r, true
	}
	return r, false
}

// enclosingFunc acha a FuncDecl que contém a posição. Funções literais
// contam como parte da função que as declara — é assim que o rf endereça.
func enclosingFunc(p *packages.Package, pos token.Pos) *ast.FuncDecl {
	for _, f := range p.Syntax {
		if pos < f.Pos() || pos > f.End() {
			continue
		}
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && pos >= fn.Pos() && pos <= fn.End() {
				return fn
			}
		}
	}
	return nil
}

func isReceiver(fn *ast.FuncDecl, id *ast.Ident) bool {
	if fn.Recv == nil {
		return false
	}
	for _, f := range fn.Recv.List {
		for _, n := range f.Names {
			if n == id {
				return true
			}
		}
	}
	return false
}

func isParamOrResult(fn *ast.FuncDecl, id *ast.Ident) bool {
	lists := []*ast.FieldList{fn.Type.Params, fn.Type.Results}
	for _, l := range lists {
		if l == nil {
			continue
		}
		for _, f := range l.List {
			for _, n := range f.Names {
				if n == id {
					return true
				}
			}
		}
	}
	return false
}

// fieldOwner devolve o endereço do dono de um campo: o nome do tipo para
// struct nomeada, ou `V` para `var V struct{...}`. Struct anônima em outro
// lugar (campo de campo, parâmetro) não tem endereço rf e devolve "".
func fieldOwner(p *packages.Package, id *ast.Ident) string {
	for _, f := range p.Syntax {
		if id.Pos() < f.Pos() || id.Pos() > f.End() {
			continue
		}
		var owner string
		ast.Inspect(f, func(n ast.Node) bool {
			if owner != "" {
				return false
			}
			switch d := n.(type) {
			case *ast.TypeSpec:
				if st, ok := d.Type.(*ast.StructType); ok && contains(st, id) {
					owner = d.Name.Name
					return false
				}
			case *ast.ValueSpec:
				if st, ok := d.Type.(*ast.StructType); ok && contains(st, id) && len(d.Names) == 1 {
					owner = d.Names[0].Name
					return false
				}
			}
			return true
		})
		return owner
	}
	return ""
}

func contains(st *ast.StructType, id *ast.Ident) bool {
	for _, f := range st.Fields.List {
		for _, n := range f.Names {
			if n == id {
				return true
			}
		}
	}
	return false
}
