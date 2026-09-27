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
	//
	// closePublishers é o laço do publicador da replicação em Kafka: o
	// snapshot em curso lê os Stores. closeFeeds não é laço sobre o Store,
	// mas é o que deixa o publicador sair (ele drena a cauda até o fim do
	// feed), e entra na lista pela ordem asseverada abaixo.
	paradas := []string{"forAlarms", "forEmitters", "forCounterMetrics", "closeAlarmDelivery", "seriesDelivery.Close", "closeFeeds", "closePublishers"}
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
		// A chamada sobre um campo (s.http.Shutdown, s.seriesDelivery.Close)
		// leva o nome do campo: "Close" e "Shutdown" sozinhos seriam de
		// qualquer um.
		name := sel.Sel.Name
		if receiver, ok := sel.X.(*ast.SelectorExpr); ok {
			name = receiver.Sel.Name + "." + name
		}
		if _, visto := pos[name]; !visto {
			pos[name] = i
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
	if pos["forAlarms"] >= pos["closeAlarmDelivery"] {
		t.Error("as filas devem fechar depois de parar os alarmes")
	}
	if pos["forEmitters"] >= pos["seriesDelivery.Close"] {
		t.Error("Kafka producers must close after all counter emitters have stopped")
	}
	// A ordem do encerramento da replicação: o feed fecha antes do
	// http.Shutdown, para o long-poll dos seguidores sair na hora em vez de
	// gastar o prazo do Shutdown; e antes dos publicadores, que só saem
	// depois de drenar a cauda até o fim do feed.
	posShutdown, ok := pos["http.Shutdown"]
	if !ok {
		t.Fatal("encerra() não chama s.http.Shutdown: este gate precisa dele para comparar")
	}
	if pos["closeFeeds"] >= posShutdown {
		t.Error("encerra() tem de fechar os feeds de replicação ANTES do http.Shutdown: " +
			"o long-poll dos seguidores seguraria o Shutdown até o prazo")
	}
	if pos["closeFeeds"] >= pos["closePublishers"] {
		t.Error("encerra() tem de fechar os feeds de replicação ANTES dos publicadores: " +
			"cada publicador esperaria o orçamento inteiro por um fim de feed que não vem")
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
