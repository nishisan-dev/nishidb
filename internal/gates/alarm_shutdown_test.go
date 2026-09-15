package gates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestLacosDoServidorParamAntesDoQuiesceDosDatasets é o gate da invariante de
// ordem do encerramento.
//
// # Por que é um gate, e não um teste de corrida
//
// Os três laços que vivem no SERVIDOR — a varredura de alarmes, a emissão
// periódica e o amostrador das métricas de janela — leem os contadores pelo
// campo do Store, que é leitura direta sem a guarda de vida: diferente da
// auditoria, eles não passam por entra()/sai().
// E como vivem fora do Store, o Close de lá não os aguarda. É o encerra() que
// tem de cancelá-los e AGUARDÁ-LOS antes do quiesce.
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
func TestLacosDoServidorParamAntesDoQuiesceDosDatasets(t *testing.T) {
	const path = "../server/server.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	corpo := methodBody(t, f, "Server", "encerra")

	// Os laços que têm de parar antes, e o que os para. Um laço novo que more
	// no servidor entra nesta lista — e o gate falha enquanto ele não estiver
	// no encerra(), que é o ponto.
	paradas := []string{"forAlarms", "forEmitters", "forCounterMetrics"}
	const quiesce = "closeDatasets"

	pos := map[string]int{}
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
		if _, visto := pos[sel.Sel.Name]; !visto {
			pos[sel.Sel.Name] = i
		}
		i++
		return true
	})

	// Guarda de vacuidade: um gate que não encontra as chamadas passaria verde
	// para sempre depois de qualquer renomeação.
	posQuiesce, ok := pos[quiesce]
	if !ok {
		t.Fatalf("encerra() não chama %s(): este gate precisa dele para comparar", quiesce)
	}
	for _, parada := range paradas {
		p, ok := pos[parada]
		if !ok {
			t.Errorf("encerra() não chama %s(): o laço ficaria de pé sobre Stores fechados", parada)
			continue
		}
		if p > posQuiesce {
			t.Errorf("encerra() chama %s (posição %d) DEPOIS de %s (posição %d): "+
				"uma passada em voo leria um Store fechado", parada, p, quiesce, posQuiesce)
		}
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
