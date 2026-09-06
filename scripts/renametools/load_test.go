package renametools

import (
	"strings"
	"testing"
)

// Com Tests: true o go/packages traz as variantes de teste (`a [a.test]`,
// `c_test [c.test]`), e é nelas que moram os dublês de interface. Um
// carregador que não as visse deixaria o ifacerename cego para metade dos
// implementadores; este teste é o que impede alguém de "simplificar" o Load
// tirando o Tests.
func TestLoadModuleLoadsTestVariantsAndOnlyModulePackages(t *testing.T) {
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	if fset == nil {
		t.Fatal("fset nulo")
	}
	ids := map[string]bool{}
	for _, p := range ModulePackages(pkgs) {
		ids[p.ID] = true
		if p.Module == nil || !p.Module.Main {
			t.Errorf("%s não é do módulo principal", p.ID)
		}
	}
	for _, want := range []string{"exemplo.local/modulo/a", "exemplo.local/modulo/a [exemplo.local/modulo/a.test]", "exemplo.local/modulo/c_test [exemplo.local/modulo/c.test]"} {
		if !ids[want] {
			t.Errorf("variante ausente: %s (tem: %v)", want, ids)
		}
	}
	for id := range ids {
		if !strings.HasPrefix(id, "exemplo.local/modulo") {
			t.Errorf("pacote de fora do módulo entrou: %s", id)
		}
	}
}

func TestPackageDirIsRelativeToTheModuleRoot(t *testing.T) {
	pkgs, _, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	root := ModuleRoot(pkgs)
	if root == "." || root == "" {
		t.Fatalf("raiz do módulo não resolvida: %q", root)
	}
	dirs := map[string]bool{}
	for _, p := range ModulePackages(pkgs) {
		dirs[PackageDir(p, root)] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !dirs[want] {
			t.Errorf("diretório %q ausente (tem: %v)", want, dirs)
		}
	}
	if dirs["exemplo.local/modulo/a"] {
		t.Error("PackageDir devolveu import path em vez de diretório relativo")
	}
}

func TestPosKeyIsStableAcrossTestVariants(t *testing.T) {
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]int{}
	for _, p := range ModulePackages(pkgs) {
		if obj := p.Types.Scope().Lookup("Escritor"); obj != nil {
			keys[PosKey(fset, obj.Pos())]++
		}
	}
	if len(keys) != 1 {
		t.Fatalf("Escritor tem %d chaves distintas entre as variantes, esperado 1: %v", len(keys), keys)
	}
	for k, n := range keys {
		if n < 2 {
			t.Errorf("%s vista %d vez(es): as variantes `a` e `a [a.test]` deveriam repetir a chave", k, n)
		}
	}
}
