package renametools

import (
	"testing"
)

func TestInterfaceGroupsCloseTransitively(t *testing.T) {
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	g := InterfaceGroups(pkgs, fset)
	// a.Escritor.Apenda, c.SoApenda.Apenda, b.Arquivo.Apenda, a.escritorFalso.Apenda
	// e c_test.dublê.Apenda são UM grupo: SoApenda não implementa Escritor,
	// mas os dois são satisfeitos por Arquivo, e é por Arquivo que se ligam.
	var appendMembers []Member
	for _, ms := range g.Members {
		n := 0
		for _, m := range ms {
			if m.Method == "Apenda" {
				n++
			}
		}
		if n > 0 {
			if n != 5 {
				t.Fatalf("o grupo de Apenda tem %d membros, esperado 5: %+v", n, ms)
			}
			appendMembers = ms
		}
	}
	if appendMembers == nil {
		t.Fatal("nenhum grupo com Apenda")
	}
	roles := map[string]string{}
	for _, m := range appendMembers {
		roles[m.Pkg+"."+m.Type] = m.Role
	}
	if roles["a.Escritor"] != "interface" || roles["c.SoApenda"] != "interface" {
		t.Errorf("papéis das interfaces: %v", roles)
	}
	if roles["b.Arquivo"] != "implementador" || roles["c.dublê"] != "implementador" || roles["a.escritorFalso"] != "implementador" {
		t.Errorf("papéis dos implementadores (dublês de teste inclusos): %v", roles)
	}
	// a.EnvoltorioDeArquivo só embute *b.Arquivo: Apenda e Fecha são
	// promovidos, não declarados por ele. Se o método promovido roubasse a
	// atribuição de b.Arquivo (bug achado na varredura do repositório real,
	// onde o pacote do embrulho é varrido antes do dono verdadeiro), esta
	// chave apareceria no lugar da de b.Arquivo.
	if _, ok := roles["a.EnvoltorioDeArquivo"]; ok {
		t.Errorf("método promovido por campo embutido roubou a atribuição do dono real: %v", roles)
	}
}

func TestInterfaceGroupsDoNotAttributeInheritedMethodToEmbeddingInterface(t *testing.T) {
	// a.Medidor e c.Regua têm a MESMA forma (Mede(n int) bool) sem uma
	// implementar a outra — ligam-se pelo nome do método, como
	// query.Store/executor.Store/planner.Store no NishiDB real. c.Anel só
	// embute Regua (zero método explícito) e entra ANTES de Regua na
	// varredura alfabética de `ifaces` do pacote c: se o código atribuísse o
	// método promovido a quem o herda por embedding, Anel roubaria a posição
	// de Regua (bug achado na varredura do repositório real:
	// internal/query.Store, que só embute executor.Store e planner.Store,
	// roubava Indexed de planner.Store por satisfazer de raspão
	// internal/query/meta.Capacidades).
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	g := InterfaceGroups(pkgs, fset)
	var members []Member
	for _, ms := range g.Members {
		for _, m := range ms {
			if m.Method == "Mede" {
				members = ms
				break
			}
		}
	}
	if members == nil {
		t.Fatal("nenhum grupo com Mede")
	}
	roles := map[string]string{}
	for _, m := range members {
		roles[m.Pkg+"."+m.Type] = m.Role
	}
	if len(roles) != 2 || roles["a.Medidor"] != "interface" || roles["c.Regua"] != "interface" {
		t.Errorf("grupo de Mede: %v, esperado só a.Medidor e c.Regua", roles)
	}
	if _, ok := roles["c.Anel"]; ok {
		t.Errorf("interface que só embute outra (sem método explícito) roubou a atribuição de quem declara: %v", roles)
	}
}

func TestGroupsSeparateMethodsWithDifferentNames(t *testing.T) {
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	g := InterfaceGroups(pkgs, fset)
	for _, ms := range g.Members {
		names := map[string]bool{}
		for _, m := range ms {
			names[m.Method] = true
		}
		if len(names) != 1 {
			t.Errorf("grupo mistura métodos de nomes diferentes: %v", names)
		}
	}
	// Fecha é grupo próprio: Escritor + Arquivo + escritorFalso + dublê = 4.
	for _, ms := range g.Members {
		if ms[0].Method == "Fecha" && len(ms) != 4 {
			t.Errorf("Fecha tem %d membros, esperado 4", len(ms))
		}
	}
}
