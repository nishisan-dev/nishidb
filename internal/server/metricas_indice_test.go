package server_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// familiasDeIndice são as seis famílias da §6.6, na ordem da tabela da spec.
//
// São SEIS e não sete: a janela efetiva não pertence a esta família e continua em
// nishidb_history_index_window_seconds, rotulada pelo dataset BASE, onde o
// operador declarou a política. A razão está escrita junto das constantes, em
// internal/metrics/nomes.go, e repetir a lista aqui à mão é de propósito — se
// alguém acrescentar uma sétima série sem decidir o que ela responde, este teste
// é onde a decisão volta a ser cobrada.
var familiasDeIndice = []string{
	metrics.MetIndexSegmentos,
	metrics.MetIndexBytes,
	metrics.MetIndexReconstrucoesTotal,
	metrics.MetIndexReconstrucaoAtiva,
	metrics.MetIndexCrcFalhasTotal,
	metrics.MetIndexSegmentosConsultadosTotal,
}

// TestAsSeriesDeIndiceExistemAntesDoPrimeiroEvento é o oráculo do NASCIMENTO das
// séries, e o nome diz exatamente o que o corpo verifica.
//
// Ele já se chamou "NascemComZero", e o nome era mais forte que o teste: uma das
// seis — nishidb_index_reconstrucao_ativa — nasce legitimamente em UM num dataset
// com campo indexado, porque o §92 manda o medidor subir ANTES da goroutine, e
// quem chamou IniciaReconstrucao tem direito de ver o estado que acabou de
// instalar. Um gauge que reportasse zero para caber num nome de teste estaria
// mentindo sobre o estado, e um nome que promete mais do que o corpo confere é a
// semente do próximo teste que aprova código quebrado.
//
// A invariante REAL é dupla, e é ela que está escrita abaixo:
//
//   - toda série existe ANTES do primeiro evento, com todas as combinações de
//     rótulo. Série que só aparece quando o primeiro segmento é enumerado faz
//     `absent()` e `rate()` mentirem exatamente na janela em que o operador mais
//     precisa deles — logo depois de uma subida, que é quando ele está olhando;
//   - os CONTADORES estão em zero, que é o que faz `rate()` honesto na primeira
//     janela.
//
// A bancada é um servidor que NÃO ingeriu nada, e a premissa é conferida pelo
// próprio teste: um oráculo que raspasse depois de ingerir passaria com as séries
// nascendo tarde, que é precisamente o defeito.
func TestAsSeriesDeIndiceExistemAntesDoPrimeiroEvento(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	amostras := coletaAmostras(t, srv)

	// A premissa, conferida e não presumida: sem esta guarda o teste continuaria
	// verde se a bancada passasse a semear dado, e aí ele estaria provando que as
	// séries existem DEPOIS do primeiro evento — o oposto do que o nome diz.
	for _, ds := range []string{"eventos", "eventos_history"} {
		linhas, ok := achaSerie(amostras, metrics.MetMaterializedRows, map[string]string{metrics.LabelDataset: ds})
		if !ok {
			t.Fatalf("montagem: %s não saiu para %q", metrics.MetMaterializedRows, ds)
		}
		if linhas.Valor != 0 {
			t.Fatalf("montagem: %q já tem %v linha(s); a bancada precisa ser um processo que não processou evento nenhum",
				ds, linhas.Valor)
		}
	}

	for _, ds := range []string{"eventos", "eventos_history"} {
		rotulo := map[string]string{metrics.LabelDataset: ds}
		for _, familia := range familiasDeIndice {
			if _, ok := achaSerie(amostras, familia, rotulo); !ok {
				t.Errorf("%s não saiu para %q antes do primeiro evento: buraco no gráfico não se distingue de alvo fora do ar, "+
					"e absent() dispara sobre um processo saudável", familia, ds)
			}
		}
		// As seis COMBINAÇÕES, e não só as duas famílias: uma implementação que
		// só emitisse o estado com contagem diferente de zero publicaria
		// `estado="corrente"` e mais nada, e o painel do operador nasceria sem a
		// série `pendente` — que é justamente a que ele vai olhar durante a
		// migração para saber se o laço está avançando.
		for _, estado := range metrics.EstadosDeIndice {
			a, ok := achaSerie(amostras, metrics.MetIndexSegmentos,
				map[string]string{metrics.LabelDataset: ds, metrics.LabelEstado: estado})
			if !ok {
				t.Errorf("%s{dataset=%q,estado=%q} não saiu: a fila que nasce vazia é indistinguível da fila que nunca foi publicada",
					metrics.MetIndexSegmentos, ds, estado)
				continue
			}
			if a.Tipo != metrics.TipoMedidor {
				t.Errorf("%s{estado=%q} saiu como %v, queria medidor: é contagem corrente, não acumulado",
					metrics.MetIndexSegmentos, estado, a.Tipo)
			}
		}
		for _, modo := range metrics.ModosDeConsultaDeIndice {
			a, ok := achaSerie(amostras, metrics.MetIndexSegmentosConsultadosTotal,
				map[string]string{metrics.LabelDataset: ds, metrics.LabelModo: modo})
			if !ok {
				t.Errorf("%s{dataset=%q,modo=%q} não saiu antes da primeira consulta",
					metrics.MetIndexSegmentosConsultadosTotal, ds, modo)
				continue
			}
			if a.Tipo != metrics.TipoContador {
				t.Errorf("%s{modo=%q} saiu como %v, queria contador: o painel lê a TAXA de segmentos enumerados, "+
					"e taxa não se calcula sobre medidor", metrics.MetIndexSegmentosConsultadosTotal, modo, a.Tipo)
			}
			if a.Valor != 0 {
				t.Errorf("%s{dataset=%q,modo=%q} = %v antes da primeira consulta, queria 0: "+
					"contador que já nasce andado faz o primeiro rate() inventar tráfego",
					metrics.MetIndexSegmentosConsultadosTotal, ds, modo, a.Valor)
			}
		}
		for _, familia := range []string{metrics.MetIndexReconstrucoesTotal, metrics.MetIndexCrcFalhasTotal} {
			a, ok := achaSerie(amostras, familia, rotulo)
			if !ok {
				continue // já reportado acima
			}
			if a.Tipo != metrics.TipoContador {
				t.Errorf("%s saiu como %v, queria contador", familia, a.Tipo)
			}
			if a.Valor != 0 {
				t.Errorf("%s{dataset=%q} = %v num processo que não processou evento nenhum, queria 0",
					familia, ds, a.Valor)
			}
		}
	}

	// O medidor da reconstrução é o único que NÃO nasce em zero, e as duas
	// metades deste par são o que separa "sobe antes da goroutine" de "constante
	// disfarçada de medidor": a irmã declara `index: true` e o laço subiu; o base
	// não declara campo indexado nenhum e não tem o que reconstruir.
	ativa, ok := achaSerie(amostras, metrics.MetIndexReconstrucaoAtiva,
		map[string]string{metrics.LabelDataset: "eventos_history"})
	if !ok {
		t.Fatalf("%s não saiu para a tabela irmã", metrics.MetIndexReconstrucaoAtiva)
	}
	if ativa.Valor != 1 {
		t.Errorf("%s{dataset=\"eventos_history\"} = %v logo depois da subida, queria 1: o §92 manda o medidor subir ANTES "+
			"da goroutine, e uma leitura entre a chamada e o primeiro escalonamento não pode dizer que nada subiu",
			metrics.MetIndexReconstrucaoAtiva, ativa.Valor)
	}
	base, ok := achaSerie(amostras, metrics.MetIndexReconstrucaoAtiva,
		map[string]string{metrics.LabelDataset: "eventos"})
	if !ok {
		t.Fatalf("%s não saiu para o dataset base", metrics.MetIndexReconstrucaoAtiva)
	}
	if base.Valor != 0 {
		t.Errorf("%s{dataset=\"eventos\"} = %v num dataset sem campo indexado, queria 0: um medidor que responde 1 sempre "+
			"não distingue laço vivo de laço morto", metrics.MetIndexReconstrucaoAtiva, base.Valor)
	}
}

// TestOsLabelsDeIndiceSaoConjuntosFechados prova as duas metades de "label nunca
// vem de entrada do cliente", e nenhuma delas basta sozinha.
//
// A metade de RUNTIME confere o que sai na porta depois de um cliente ter
// mandado tudo que sabe mandar — consultas com valor arbitrário no WHERE, nome de
// dataset inexistente, coluna inexistente. Ela pega o valor que vaza. O que ela
// NÃO pega é o vazamento que depende de um dado que o teste não pensou em mandar:
// um `Valor: nomeDoCampo` continuaria emitindo `estado="status"` só quando o
// campo se chamasse assim.
//
// A metade de ORIGEM fecha isso pelo go/ast: no único arquivo que publica estas
// séries, o valor de `estado` e de `modo` só pode ser a variável de um `range`
// sobre a enumeração EXPORTADA do pacote de métricas. Não literal, não parâmetro,
// não campo de struct — e a lista fica escrita uma vez, onde os dois leitores (o
// coletor e este teste) a encontram.
func TestOsLabelsDeIndiceSaoConjuntosFechados(t *testing.T) {
	srv, _ := servidorComDadosDiretorioEStorage(t, 400, blocoPadrao, "  segmentSize: 8KiB\n")

	// Entrada de cliente, das três formas que o produto aceita: valor no WHERE,
	// dataset desconhecido e coluna desconhecida. Nenhuma pode virar rótulo.
	for _, corpo := range []string{
		`{"dataset":"d","sql":"SELECT id FROM d WHERE status = 'selado'"}`,
		`{"dataset":"d","sql":"SELECT id FROM d WHERE status = 'fora'"}`,
		`{"dataset":"nao_existe","sql":"SELECT id FROM nao_existe"}`,
		`{"dataset":"d","sql":"SELECT coluna_que_nao_existe FROM d"}`,
	} {
		resp := consulta(t, srv, corpo)
		fecha(t, resp)
	}

	amostras := coletaAmostras(t, srv)
	confereConjuntoFechado(t, amostras, metrics.MetIndexSegmentos, metrics.LabelEstado, metrics.EstadosDeIndice)
	confereConjuntoFechado(t, amostras, metrics.MetIndexSegmentosConsultadosTotal, metrics.LabelModo, metrics.ModosDeConsultaDeIndice)

	confereOrigemFechadaDosLabels(t)
}

// confereConjuntoFechado exige que os valores do rótulo no scrape sejam
// EXATAMENTE a enumeração — nem um a mais, nem um a menos.
//
// Os dois sentidos importam e por razões diferentes. Um valor A MAIS é entrada de
// cliente virando cardinalidade sem teto, que é a doutrina do datasetDesconhecido.
// Um valor A MENOS é a série que só aparece quando tem o que mostrar, e aí o
// painel do operador nasce sem a linha que ele foi olhar.
func confereConjuntoFechado(t *testing.T, amostras []amostraExposta, familia, rotulo string, fechado []string) {
	t.Helper()
	vistos := map[string]bool{}
	for _, a := range amostras {
		if a.Nome != familia {
			continue
		}
		v, ok := a.Labels[rotulo]
		if !ok {
			t.Errorf("%s saiu sem o rótulo %q: a série sem ele é um número sem pergunta", familia, rotulo)
			continue
		}
		vistos[v] = true
	}
	esperado := map[string]bool{}
	for _, v := range fechado {
		esperado[v] = true
	}
	for v := range vistos {
		if !esperado[v] {
			t.Errorf("%s trouxe %s=%q, que não está na enumeração fechada %v: label alimentado por fora da enumeração "+
				"é cardinalidade sem teto no primeiro dia em que a fonte for entrada do cliente", familia, rotulo, v, fechado)
		}
	}
	for v := range esperado {
		if !vistos[v] {
			t.Errorf("%s não trouxe %s=%q: as combinações saem TODAS, sempre — zero é uma resposta, ausência não é",
				familia, rotulo, v)
		}
	}
}

// confereOrigemFechadaDosLabels lê metricas_indice.go pelo go/ast e exige que o
// valor de `estado` e de `modo` venha SÓ da enumeração exportada.
//
// Config e código são lidos como DADO neste repositório, nunca por grep em prosa:
// um grep por `"selado"` aprovaria um arquivo que monta o valor por concatenação,
// e reprovaria um comentário que menciona a palavra.
//
// A conferência tem uma porta só de propósito — o comRotulo. Um segundo caminho
// (um metrics.Label{} montado à mão com LabelEstado) passaria por baixo desta
// checagem, e por isso ele também é recusado abaixo.
func confereOrigemFechadaDosLabels(t *testing.T) {
	t.Helper()
	const arquivo = "metricas_indice.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, arquivo, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse de %s: %v", arquivo, err)
	}

	// Primeiro passo: as variáveis de laço ligadas a um range sobre a enumeração
	// exportada. Só elas podem chegar ao rótulo.
	fechadas := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		r, ok := n.(*ast.RangeStmt)
		if !ok {
			return true
		}
		sel, ok := r.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "metrics" {
			return true
		}
		if sel.Sel.Name != "EstadosDeIndice" && sel.Sel.Name != "ModosDeConsultaDeIndice" {
			return true
		}
		if id, ok := r.Value.(*ast.Ident); ok {
			fechadas[id.Name] = sel.Sel.Name
		}
		return true
	})
	if len(fechadas) != 2 {
		t.Fatalf("%s tem %d laço(s) sobre as enumerações exportadas, esperava 2 (estados e modos): "+
			"sem eles esta conferência aprovaria qualquer coisa por vacuidade", arquivo, len(fechadas))
	}

	var conferidas int
	ast.Inspect(f, func(n ast.Node) bool {
		// Porta única: nenhum metrics.Label montado à mão pode carregar os dois
		// rótulos de enumeração, senão haveria um caminho que este teste não vê.
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "metrics" &&
				(sel.Sel.Name == "LabelEstado" || sel.Sel.Name == "LabelModo") {
				if !dentroDeComRotulo(f, sel) {
					t.Errorf("%s usa metrics.%s fora de uma chamada a comRotulo: uma segunda porta para o rótulo "+
						"é uma porta que esta conferência não enxerga", arquivo, sel.Sel.Name)
				}
			}
		}
		chamada, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		fn, ok := chamada.Fun.(*ast.Ident)
		if !ok || fn.Name != "comRotulo" || len(chamada.Args) != 3 {
			return true
		}
		conferidas++
		nome, ok := chamada.Args[1].(*ast.SelectorExpr)
		if !ok || nome.Sel.Name != "LabelEstado" && nome.Sel.Name != "LabelModo" {
			t.Errorf("%s: comRotulo recebeu um nome de rótulo que não é constante do pacote de métricas", arquivo)
			return true
		}
		valor, ok := chamada.Args[2].(*ast.Ident)
		if !ok {
			t.Errorf("%s: o VALOR de %s não é identificador — só a variável de um range sobre a enumeração exportada "+
				"pode alimentá-lo, e qualquer outra expressão é uma fonte que ninguém auditou",
				arquivo, nome.Sel.Name)
			return true
		}
		if _, fechado := fechadas[valor.Name]; !fechado {
			t.Errorf("%s: o valor de %s vem de %q, que não é a variável de um range sobre metrics.EstadosDeIndice "+
				"nem sobre metrics.ModosDeConsultaDeIndice; label nunca vem de entrada do cliente",
				arquivo, nome.Sel.Name, valor.Name)
		}
		return true
	})
	if conferidas != 2 {
		t.Fatalf("%s tem %d chamada(s) a comRotulo, esperava 2: se a publicação mudou de forma, esta conferência "+
			"parou de olhar o caminho real", arquivo, conferidas)
	}
}

// dentroDeComRotulo responde se o selector está entre os argumentos de uma
// chamada a comRotulo. É o que impede a checagem de aprovar um segundo caminho
// de publicação escrito ao lado.
func dentroDeComRotulo(raiz ast.Node, alvo ast.Node) bool {
	achou := false
	ast.Inspect(raiz, func(n ast.Node) bool {
		chamada, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if fn, ok := chamada.Fun.(*ast.Ident); !ok || fn.Name != "comRotulo" {
			return true
		}
		for _, arg := range chamada.Args {
			ast.Inspect(arg, func(m ast.Node) bool {
				if m == alvo {
					achou = true
				}
				return !achou
			})
		}
		return true
	})
	return achou
}

// TestStatsEMetricsLeemAMesmaEstrutura prova que o bloco `index` do /v1/stats e
// as séries nishidb_index_* descrevem o MESMO estado, e não dois contadores que
// por acaso concordam hoje.
//
// A bancada espera a fila DRENAR antes de comparar, e não é conveniência: com o
// laço de reconstrução em voo, `pendente` e `selado` mudam entre as duas
// requisições, e uma discordância legítima seria indistinguível do defeito que o
// teste procura. Com a fila em zero nada mais se move sem o teste mandar.
//
// A conferência de NÃO-TRIVIALIDADE vem antes das igualdades pela razão de
// sempre: dez comparações de 0 contra 0 passam com os dois lados devolvendo
// constante zero, que é exatamente o mutante mais fácil de escrever.
func TestStatsEMetricsLeemAMesmaEstrutura(t *testing.T) {
	srv, _ := servidorComDadosDiretorioEStorage(t, 400, blocoPadrao, "  segmentSize: 8KiB\n")
	esperaFilaDrenar(t, srv, "d")

	// Uma consulta indexada, para que os contadores de caminho tenham o que
	// comparar: sem ela, segmentsByIndex e segmentsByScan seriam 0 == 0.
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d WHERE status = 'Stop'"}`)
	if resp.StatusCode != http.StatusOK {
		fecha(t, resp)
		t.Fatalf("consulta indexada devolveu %d", resp.StatusCode)
	}
	fecha(t, resp)

	est := leJSON[protocolo.Estatisticas](t, srv, "/v1/stats")
	amostras := coletaAmostras(t, srv)

	var achou bool
	for _, ds := range est.Datasets {
		if ds.Name != "d" {
			continue
		}
		achou = true
		idx := ds.Index
		if idx.Bytes == 0 || idx.Current == 0 || idx.Sealed == 0 || idx.SegmentsByIndex == 0 {
			t.Fatalf("o bloco index de %q saiu trivial (%+v): sem segmento selado, sem bytes e sem consulta indexada, "+
				"as igualdades abaixo seriam 0 == 0 e um par de constantes zero passaria por elas", ds.Name, idx)
		}
		rotulo := map[string]string{metrics.LabelDataset: "d"}
		porEstado := map[string]int64{
			metrics.EstadoSelado:   idx.Sealed,
			metrics.EstadoCorrente: idx.Current,
			metrics.EstadoPendente: idx.Pending,
			metrics.EstadoFora:     idx.OutOfWindow,
		}
		for _, estado := range metrics.EstadosDeIndice {
			confereIgual(t, amostras, metrics.MetIndexSegmentos,
				map[string]string{metrics.LabelDataset: "d", metrics.LabelEstado: estado}, porEstado[estado])
		}
		porModo := map[string]int64{
			metrics.ModoIndice:     idx.SegmentsByIndex,
			metrics.ModoEnumeracao: idx.SegmentsByScan,
		}
		for _, modo := range metrics.ModosDeConsultaDeIndice {
			confereIgual(t, amostras, metrics.MetIndexSegmentosConsultadosTotal,
				map[string]string{metrics.LabelDataset: "d", metrics.LabelModo: modo}, porModo[modo])
		}
		confereIgual(t, amostras, metrics.MetIndexBytes, rotulo, idx.Bytes)
		confereIgual(t, amostras, metrics.MetIndexReconstrucoesTotal, rotulo, idx.Rebuilds)
		confereIgual(t, amostras, metrics.MetIndexCrcFalhasTotal, rotulo, idx.CRCFailures)
		var ativa int64
		if idx.RebuildRunning {
			ativa = 1
		}
		confereIgual(t, amostras, metrics.MetIndexReconstrucaoAtiva, rotulo, ativa)
	}
	if !achou {
		t.Fatal("o dataset \"d\" não apareceu no /v1/stats")
	}
}

// confereIgual casa uma série do scrape com o número que o /v1/stats publicou.
func confereIgual(t *testing.T, amostras []amostraExposta, familia string, rotulos map[string]string, quer int64) {
	t.Helper()
	a, ok := achaSerie(amostras, familia, rotulos)
	if !ok {
		t.Errorf("%s%v não saiu no /metrics, mas o /v1/stats publicou %d: o JSON e o painel deixaram de ler a mesma estrutura",
			familia, rotulos, quer)
		return
	}
	if int64(a.Valor) != quer {
		t.Errorf("%s%v: /metrics diz %d e /v1/stats diz %d — duas fontes para o mesmo fato divergem, e em produção "+
			"alguém lê o errado", familia, rotulos, int64(a.Valor), quer)
	}
}

// esperaFilaDrenar bloqueia até a reconstrução não ter mais o que fazer, ou
// falha dizendo o que ficou para trás.
//
// A espera é sobre o /v1/stats e não sobre o log porque é o número que o teste
// vai comparar depois; esperar por outro sinal deixaria a corrida de pé no
// observável que importa.
func esperaFilaDrenar(t *testing.T, srv *server.Servidor, nome string) {
	t.Helper()
	prazo := time.Now().Add(30 * time.Second)
	var ultimo protocolo.IndiceStats
	for time.Now().Before(prazo) {
		est := leJSON[protocolo.Estatisticas](t, srv, "/v1/stats")
		for _, ds := range est.Datasets {
			if ds.Name != nome {
				continue
			}
			ultimo = ds.Index
			if ultimo.Pending == 0 && ultimo.Sealed > 0 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("a fila de reconstrução de %q não drenou em 30s; último retrato: %+v", nome, ultimo)
}

// TestAAjudaDoIndiceDoHistoricoNaoNegaAPurga pina a metade PROSA de um fato que
// mudou neste ciclo.
//
// Enquanto todo índice era global, a purga de segmento não devolvia byte nenhum
// de índice, e a Ajuda de nishidb_history_index_bytes dizia isso. Com o índice
// por segmento o .sidx sai no mesmo passo que remove o .dat, e a frase virou
// mentira — que é pior que ausência: quem lê o painel dimensiona disco por ela e
// conclui que o número só sobe.
//
// O FATO está provado no storage (TestAJanelaNaoDeixaIndiceDeSegmentoAposentado e
// TestOsBytesDoIndicePorSegmentoSaoParcelaEstritaDoIndexBytes); o que falta é o
// texto que viaja até o operador não contradizê-lo. Uma métrica com o valor certo
// e a ajuda errada é lida errado — é o mesmo argumento que fez a ressalva de
// limite inferior do garbage_bytes virar asserção.
func TestAAjudaDoIndiceDoHistoricoNaoNegaAPurga(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	a, ok := achaSerie(coletaAmostras(t, srv), metrics.MetHistoryIndexBytes,
		map[string]string{metrics.LabelDataset: "eventos"})
	if !ok {
		t.Fatalf("%s não saiu no rótulo do dataset base", metrics.MetHistoryIndexBytes)
	}
	if strings.Contains(a.Ajuda, "NÃO os devolve") {
		t.Errorf("a ajuda de %s ainda afirma que a purga não devolve os bytes de índice: %q — "+
			"com o índice por segmento o .sidx sai no mesmo passo que remove o .dat, e a série CAI depois de uma passada",
			metrics.MetHistoryIndexBytes, a.Ajuda)
	}
	if !strings.Contains(a.Ajuda, metrics.MetIndexBytes) {
		t.Errorf("a ajuda de %s não aponta para %s: as duas séries medem conjuntos encaixados, e quem lê a maior "+
			"precisa saber qual parcela tem teto próprio. Ajuda = %q",
			metrics.MetHistoryIndexBytes, metrics.MetIndexBytes, a.Ajuda)
	}
}

// TestOsCabecalhosSaemNomeadosParaCadaFamilia é o oráculo do campo Familia, que
// estava vazio em todo o bloco `nishidb_history_*` e fazia dez séries chegarem ao
// operador sem `# HELP` e sem `# TYPE`.
//
// O WriteTo agrupa os cabeçalhos por Familia. Dez amostras com Familia vazia
// caem num grupo só: o par sai UMA vez, com o nome do metric VAZIO
// (`# HELP  Linhas gravadas...`), e as outras nove famílias não recebem nenhum.
// Nada nisso muda um número, e é por isso que a suíte inteira convivia com o
// defeito — o valor estava certo o tempo todo. O que estava faltando era o TIPO,
// sem o qual `rate()` não se aplica, e a AJUDA, que é por onde metade do contrato
// de várias destas séries viaja.
//
// O LIMITE DESTE TESTE, escrito em vez de presumido: a bancada não sobe consumidor
// Kafka, e o mesmo defeito continua de pé nas nove amostras de
// internal/kafka/metricas.go — inclusive na nishidb_kafka_poll_blocked_seconds_total,
// que é a série sobre a qual a medição pré-ciclo do piloto foi construída. Aquilo
// é de outro pacote e de outra tarefa; aqui fica o registro de que este oráculo
// NÃO o alcança.
func TestOsCabecalhosSaemNomeadosParaCadaFamilia(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	corpo := pega(t, srv, "/metrics")

	comCabecalho := map[string]bool{}
	for _, linha := range strings.Split(corpo, "\n") {
		depois, achou := strings.CutPrefix(linha, "# HELP ")
		if !achou {
			if depois, achou = strings.CutPrefix(linha, "# TYPE "); !achou {
				continue
			}
		}
		familia, _, _ := strings.Cut(depois, " ")
		if familia == "" {
			t.Errorf("cabeçalho com nome de métrica VAZIO: %q — é o sintoma de amostras publicadas sem o campo "+
				"Familia, e o Prometheus recusa o scrape inteiro por causa dele, não só a linha", linha)
			continue
		}
		comCabecalho[familia] = true
	}

	// Todas as famílias do histórico e do índice, que são as que esta tarefa
	// publica ou conserta.
	esperadas := append([]string{
		metrics.MetHistoryRows, metrics.MetHistoryDropped, metrics.MetHistoryDegraded,
		metrics.MetHistoryDataBytes, metrics.MetHistoryIndexBytes, metrics.MetHistoryOldestWrittenAt,
		metrics.MetHistoryIndexWindow, metrics.MetHistoryPurged, metrics.MetHistoryRetentionRunning,
	}, familiasDeIndice...)
	for _, familia := range esperadas {
		if !comCabecalho[familia] {
			t.Errorf("%s saiu sem # HELP/# TYPE nomeados: série sem tipo não aceita rate(), e série sem ajuda "+
				"chega ao operador sem a metade do contrato que viaja no texto", familia)
		}
	}
}

// nomesDasFamiliasDeIndice existe para a mensagem de erro do teste de cobertura
// abaixo sair legível e ordenada.
func nomesDasFamiliasDeIndice() string {
	nomes := append([]string(nil), familiasDeIndice...)
	sort.Strings(nomes)
	return strings.Join(nomes, ", ")
}

// TestAsSeriesDeIndiceSaemSoPelasConstantes fecha a primeira das três invariantes
// de métrica do projeto: nome só pelas constantes de nomes.go.
//
// A conferência é do LADO DO SCRAPE — toda linha `nishidb_index_*` que sai na
// porta tem de pertencer à lista de constantes. Um nome digitado à mão no coletor
// produziria uma série que o dashboard nunca casa, e o sintoma é um painel vazio
// sem ninguém perceber, que é exatamente o que o cabeçalho de nomes.go descreve.
func TestAsSeriesDeIndiceSaemSoPelasConstantes(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	conhecidas := map[string]bool{}
	for _, f := range familiasDeIndice {
		conhecidas[f] = true
	}
	var vistas int
	for _, a := range coletaAmostras(t, srv) {
		if !strings.HasPrefix(a.Nome, "nishidb_index_") {
			continue
		}
		vistas++
		if !conhecidas[a.Nome] {
			t.Errorf("o scrape trouxe %q, que não é nenhuma das constantes de nomes.go (%s): "+
				"nome digitado à mão diverge no dia em que um dos dois lugares for corrigido",
				a.Nome, nomesDasFamiliasDeIndice())
		}
	}
	if vistas == 0 {
		t.Fatal("nenhuma série nishidb_index_* no scrape: a conferência passaria por vacuidade")
	}
}
