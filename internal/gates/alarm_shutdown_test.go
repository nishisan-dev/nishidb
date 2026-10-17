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
	// feed), e entra na lista pela ordem asseverada abaixo; stopStoreProducers
	// e checkpointStores precisam dos Stores abertos.
	paradas := []string{"forAlarms", "forEmitters", "forCounterMetrics", "closeAlarmDelivery", "seriesDelivery.Close",
		"stopStoreProducers", "checkpointStores", "closeFeeds", "closePublishers"}
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
	// A ordem do encerramento da replicação, que é o que faz o reinício limpo
	// retomar a mesma geração do feed:
	//
	//	http.Shutdown < stopStoreProducers < checkpointStores < closeFeeds < closePublishers < closeDatasets < waitFeeds
	//
	// O feed fecha DEPOIS do http.Shutdown (a rota /v1/compact publica), dos
	// produtores do Store (compaction, expiração e retenção publicam e movem
	// a posição de escrita) e do checkpoint (a marca limpa não pode cobrir
	// record fora do disco): só aí a impressão dos Stores lida no closeFeeds
	// é estável. E antes dos publicadores, que só saem depois de drenar a
	// cauda até o fim do feed. O long-poll dos seguidores não segura o
	// Shutdown: o handler sai pelo ctx do Run, já cancelado.
	// waitFeeds fecha o log do feed: os publicadores ainda leem do disco a
	// cauda que não receberam até o closePublishers voltar.
	order := []string{"http.Shutdown", "stopStoreProducers", "checkpointStores", "closeFeeds", "closePublishers", quiesce, "waitFeeds"}
	for _, name := range order {
		if _, ok := pos[name]; !ok {
			t.Fatalf("encerra() não chama %s: este gate precisa dele para comparar", name)
		}
	}
	for i := 1; i < len(order); i++ {
		if pos[order[i-1]] >= pos[order[i]] {
			t.Errorf("encerra() chama %s (posição %d) depois de %s (posição %d): a ordem tem de ser %v; "+
				"invertida, uma mudança publicada depois da impressão do feed (ou depois do Close dele) "+
				"faz a próxima subida abrir uma geração nova e mandar todo consumidor ao snapshot",
				order[i-1], pos[order[i-1]], order[i], pos[order[i]], order)
		}
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

// TestStopStoreProducersParaOsTresProdutoresDeTodoStore é a metade estrutural
// da garantia do stopStoreProducers: ele percorre s.datasets (que inclui a
// tabela irmã logo depois do base) e, sobre o Store de CADA item, chama
// ForCompaction, ForExpiration e ForRetention.
//
// É gate, e não só teste de comportamento, pela mesma razão do gate de cima:
// a compaction e a retenção são canceladas pelo ctx do Run no mesmo instante
// em que o encerramento começa, e uma passada que não esteja exatamente no
// meio de uma unidade de trabalho termina sozinha. Tirar o For* da lista quase
// nunca deixa rastro observável — o dano é a janela rara em que a passada está
// em voo, que o teste de comportamento cobre para a expiração e a retenção.
func TestStopStoreProducersParaOsTresProdutoresDeTodoStore(t *testing.T) {
	const path = "../server/server.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	corpo := methodBody(t, f, "Server", "stopStoreProducers")
	want := []string{"ForCompaction", "ForExpiration", "ForRetention"}
	var laco *ast.RangeStmt
	for _, stmt := range corpo.List {
		r, ok := stmt.(*ast.RangeStmt)
		if !ok {
			continue
		}
		// for _, d := range s.datasets
		x, ok := r.X.(*ast.SelectorExpr)
		if ok && x.Sel.Name == "datasets" {
			laco = r
		}
	}
	if laco == nil {
		t.Fatal("stopStoreProducers não percorre s.datasets: a irmã e os outros datasets ficariam com os produtores de pé")
	}
	v, ok := laco.Value.(*ast.Ident)
	if !ok {
		t.Fatal("o laço de stopStoreProducers não nomeia o dataset do range")
	}
	called := map[string]bool{}
	ast.Inspect(laco.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		// <d>.st.ForX()
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		st, ok := sel.X.(*ast.SelectorExpr)
		if !ok || st.Sel.Name != "st" {
			return true
		}
		if id, ok := st.X.(*ast.Ident); ok && id.Name == v.Name {
			called[sel.Sel.Name] = true
		}
		return true
	})
	for _, name := range want {
		if !called[name] {
			t.Errorf("stopStoreProducers não chama %s.st.%s() no laço de s.datasets: o produtor seguiria publicando "+
				"(ou movendo a posição de escrita) depois de o feed ler a impressão", v.Name, name)
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
