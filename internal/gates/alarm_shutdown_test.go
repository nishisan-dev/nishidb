package gates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestAlarmesParamAntesDoQuiesceDosDatasets é o gate da invariante de ordem do
// encerramento.
//
// # Por que é um gate, e não um teste de corrida
//
// O laço de alarmes lê os contadores pelo campo do Store, que é leitura direta
// sem a guarda de vida — diferente da auditoria, ele não passa por
// entra()/sai(). Como o laço vive no servidor, o Close do Store não o aguarda:
// é o encerra() que tem de cancelá-lo e AGUARDÁ-LO antes do quiesce.
//
// A tentativa óbvia de provar isso — subir um servidor com alarme e encerrar
// sob -race — NÃO prova nada, e isso foi medido: a passada sobre algumas
// dezenas de grupos termina em microssegundos, então o laço já está dormindo no
// timer quando o encerramento chega, e nenhuma varredura está em voo para
// disputar o Store fechado. Invertendo a ordem no código, cinco rodadas sob
// -race passaram verdes.
//
// Ordem de chamada não é observável por corrida, mas é observável no CÓDIGO. Um
// gate que lê a AST de encerra() e confere a posição das duas chamadas é
// determinístico e não depende de escalonamento — e ler código como DADO, em
// vez de grep em prosa, é a doutrina de gate deste repositório.
func TestAlarmesParamAntesDoQuiesceDosDatasets(t *testing.T) {
	const path = "../server/server.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	corpo := methodBody(t, f, "Server", "encerra")

	posAlarms, posClose := -1, -1
	i := 0
	ast.Inspect(corpo, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "forAlarms":
			if posAlarms < 0 {
				posAlarms = i
			}
		case "closeDatasets":
			if posClose < 0 {
				posClose = i
			}
		}
		i++
		return true
	})

	// Guarda de vacuidade: um gate que não encontra as duas chamadas passaria
	// verde para sempre depois de qualquer renomeação.
	if posAlarms < 0 {
		t.Fatal("encerra() não chama forAlarms(): o laço de alarmes ficaria de pé sobre Stores fechados")
	}
	if posClose < 0 {
		t.Fatal("encerra() não chama closeDatasets(): este gate precisa das duas chamadas para comparar")
	}
	if posAlarms > posClose {
		t.Fatalf("encerra() fecha os datasets (posição %d) ANTES de parar os alarmes (posição %d): "+
			"uma varredura em voo leria um Store fechado", posClose, posAlarms)
	}
}

// methodBody devolve o corpo do método (receptor, nome) do arquivo.
func methodBody(t *testing.T, f *ast.File, receiver, name string) *ast.BlockStmt {
	t.Helper()
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != name || fn.Recv == nil || len(fn.Recv.List) != 1 {
			continue
		}
		star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		id, ok := star.X.(*ast.Ident)
		if !ok || id.Name != receiver {
			continue
		}
		return fn.Body
	}
	t.Fatalf("método (*%s).%s não encontrado", receiver, name)
	return nil
}
