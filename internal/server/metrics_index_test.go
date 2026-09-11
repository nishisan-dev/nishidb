package server_test

import (
	"log/slog"

	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// familiasOfIndex são as seis famílias da §6.6, na ordem da tabela da spec.
//
// São SEIS e não sete: a janela efetiva não pertence a esta família e continua em
// nishidb_history_index_window_seconds, rotulada pelo dataset BASE, onde o
// operador declarou a política. A razão está escrita junto das constantes, em
// internal/metrics/names.go, e repetir a lista aqui à mão é de propósito — se
// alguém acrescentar uma sétima série sem decidir o que ela responde, este teste
// é onde a decisão volta a ser cobrada.
var familiasOfIndex = []string{
	metrics.MetIndexSegments,
	metrics.MetIndexBytes,
	metrics.MetIndexRebuildsTotal,
	metrics.MetIndexRebuildActive,
	metrics.MetIndexCRCFailuresTotal,
	metrics.MetIndexSegmentsLookedUpTotal,
}

// TestAsSeriesDeIndiceExistemAntesDoPrimeiroEvento é o oráculo do NASCIMENTO das
// séries, e o nome diz exatamente o que o corpo verifica.
//
// Ele já se chamou "NascemComZero", e o nome era mais forte que o teste: uma das
// seis — nishidb_index_reconstrucao_ativa — nasce legitimamente em UM num dataset
// com campo indexado, porque o §92 manda o medidor subir ANTES da goroutine, e
// quem chamou StartRebuild tem direito de ver o estado que acabou de
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
	srv := serverWithHistory(t, specWithHistory)
	samples := coletaSamples(t, srv)

	// A premissa, conferida e não presumida: sem esta guarda o teste continuaria
	// verde se a bancada passasse a semear dado, e aí ele estaria provando que as
	// séries existem DEPOIS do primeiro evento — o oposto do que o nome diz.
	for _, ds := range []string{"eventos", "eventos_history"} {
		rows, ok := achaSerie(samples, metrics.MetMaterializedRows, map[string]string{metrics.LabelDataset: ds})
		if !ok {
			t.Fatalf("montagem: %s não saiu para %q", metrics.MetMaterializedRows, ds)
		}
		if rows.Value != 0 {
			t.Fatalf("montagem: %q já tem %v linha(s); a bancada precisa ser um processo que não processou evento nenhum",
				ds, rows.Value)
		}
	}

	for _, ds := range []string{"eventos", "eventos_history"} {
		rotulo := map[string]string{metrics.LabelDataset: ds}
		for _, familia := range familiasOfIndex {
			if _, ok := achaSerie(samples, familia, rotulo); !ok {
				t.Errorf("%s não saiu para %q antes do primeiro evento: buraco no gráfico não se distingue de alvo fora do ar, "+
					"e absent() dispara sobre um processo saudável", familia, ds)
			}
		}
		// As seis COMBINAÇÕES, e não só as duas famílias: uma implementação que
		// só emitisse o estado com contagem diferente de zero publicaria
		// `estado="corrente"` e mais nada, e o painel do operador nasceria sem a
		// série `pendente` — que é justamente a que ele vai olhar durante a
		// migração para saber se o laço está avançando.
		for _, state := range metrics.IndexStates {
			a, ok := achaSerie(samples, metrics.MetIndexSegments,
				map[string]string{metrics.LabelDataset: ds, metrics.LabelState: state})
			if !ok {
				t.Errorf("%s{dataset=%q,estado=%q} não saiu: a fila que nasce vazia é indistinguível da fila que nunca foi publicada",
					metrics.MetIndexSegments, ds, state)
				continue
			}
			if a.Tipo != metrics.TypeGauge {
				t.Errorf("%s{estado=%q} saiu como %v, queria medidor: é contagem corrente, não acumulado",
					metrics.MetIndexSegments, state, a.Tipo)
			}
		}
		for _, modo := range metrics.IndexLookupModes {
			a, ok := achaSerie(samples, metrics.MetIndexSegmentsLookedUpTotal,
				map[string]string{metrics.LabelDataset: ds, metrics.LabelMode: modo})
			if !ok {
				t.Errorf("%s{dataset=%q,modo=%q} não saiu antes da primeira consulta",
					metrics.MetIndexSegmentsLookedUpTotal, ds, modo)
				continue
			}
			if a.Tipo != metrics.TypeCounter {
				t.Errorf("%s{modo=%q} saiu como %v, queria contador: o painel lê a TAXA de segmentos enumerados, "+
					"e taxa não se calcula sobre medidor", metrics.MetIndexSegmentsLookedUpTotal, modo, a.Tipo)
			}
			if a.Value != 0 {
				t.Errorf("%s{dataset=%q,modo=%q} = %v antes da primeira consulta, queria 0: "+
					"contador que já nasce andado faz o primeiro rate() inventar tráfego",
					metrics.MetIndexSegmentsLookedUpTotal, ds, modo, a.Value)
			}
		}
		for _, familia := range []string{metrics.MetIndexRebuildsTotal, metrics.MetIndexCRCFailuresTotal} {
			a, ok := achaSerie(samples, familia, rotulo)
			if !ok {
				continue // já reportado acima
			}
			if a.Tipo != metrics.TypeCounter {
				t.Errorf("%s saiu como %v, queria contador", familia, a.Tipo)
			}
			if a.Value != 0 {
				t.Errorf("%s{dataset=%q} = %v num processo que não processou evento nenhum, queria 0",
					familia, ds, a.Value)
			}
		}
	}

	// O medidor da reconstrução é o único que NÃO nasce em zero, e as duas
	// metades deste par são o que separa "sobe antes da goroutine" de "constante
	// disfarçada de medidor": a irmã declara `index: true` e o laço subiu; o base
	// não declara campo indexado nenhum e não tem o que reconstruir.
	active, ok := achaSerie(samples, metrics.MetIndexRebuildActive,
		map[string]string{metrics.LabelDataset: "eventos_history"})
	if !ok {
		t.Fatalf("%s não saiu para a tabela irmã", metrics.MetIndexRebuildActive)
	}
	if active.Value != 1 {
		t.Errorf("%s{dataset=\"eventos_history\"} = %v logo depois da subida, queria 1: o §92 manda o medidor subir ANTES "+
			"da goroutine, e uma leitura entre a chamada e o primeiro escalonamento não pode dizer que nada subiu",
			metrics.MetIndexRebuildActive, active.Value)
	}
	base, ok := achaSerie(samples, metrics.MetIndexRebuildActive,
		map[string]string{metrics.LabelDataset: "eventos"})
	if !ok {
		t.Fatalf("%s não saiu para o dataset base", metrics.MetIndexRebuildActive)
	}
	if base.Value != 0 {
		t.Errorf("%s{dataset=\"eventos\"} = %v num dataset sem campo indexado, queria 0: um medidor que responde 1 sempre "+
			"não distingue laço vivo de laço morto", metrics.MetIndexRebuildActive, base.Value)
	}
}

// TestOsLabelsDeIndiceSaoConjuntosFechados prova as duas metades de "label nunca
// vem de entrada do cliente", e nenhuma delas basta sozinha.
//
// A metade de RUNTIME confere o que sai na porta depois de um cliente ter
// mandado tudo que sabe mandar — consultas com valor arbitrário no WHERE, nome de
// dataset inexistente, coluna inexistente. Ela pega o valor que vaza. O que ela
// NÃO pega é o vazamento que depende de um dado que o teste não pensou em mandar:
// um `Value: nomeDoCampo` continuaria emitindo `estado="status"` só quando o
// campo se chamasse assim.
//
// A metade de ORIGEM fecha isso pelo go/ast: no único arquivo que publica estas
// séries, o valor de `estado` e de `modo` só pode ser a variável de um `range`
// sobre a enumeração EXPORTADA do pacote de métricas. Não literal, não parâmetro,
// não campo de struct — e a lista fica escrita uma vez, onde os dois leitores (o
// coletor e este teste) a encontram.
func TestOsLabelsDeIndiceSaoConjuntosFechados(t *testing.T) {
	srv, _ := serverWithDataDiretorioEStorage(t, 400, blockPadrao, "  segmentSize: 8KiB\n")

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

	samples := coletaSamples(t, srv)
	confereConjuntoFechado(t, samples, metrics.MetIndexSegments, metrics.LabelState, metrics.IndexStates)
	confereConjuntoFechado(t, samples, metrics.MetIndexSegmentsLookedUpTotal, metrics.LabelMode, metrics.IndexLookupModes)

	confereSourceFechadaOfLabels(t)
}

// confereConjuntoFechado exige que os valores do rótulo no scrape sejam
// EXATAMENTE a enumeração — nem um a mais, nem um a menos.
//
// Os dois sentidos importam e por razões diferentes. Um valor A MAIS é entrada de
// cliente virando cardinalidade sem teto, que é a doutrina do datasetDesconhecido.
// Um valor A MENOS é a série que só aparece quando tem o que mostrar, e aí o
// painel do operador nasce sem a linha que ele foi olhar.
func confereConjuntoFechado(t *testing.T, samples []amostraExposta, familia, rotulo string, fechado []string) {
	t.Helper()
	vistos := map[string]bool{}
	for _, a := range samples {
		if a.Name != familia {
			continue
		}
		v, ok := a.Labels[rotulo]
		if !ok {
			t.Errorf("%s saiu sem o rótulo %q: a série sem ele é um número sem pergunta", familia, rotulo)
			continue
		}
		vistos[v] = true
	}
	expected := map[string]bool{}
	for _, v := range fechado {
		expected[v] = true
	}
	for v := range vistos {
		if !expected[v] {
			t.Errorf("%s trouxe %s=%q, que não está na enumeração fechada %v: label alimentado por fora da enumeração "+
				"é cardinalidade sem teto no primeiro dia em que a fonte for entrada do cliente", familia, rotulo, v, fechado)
		}
	}
	for v := range expected {
		if !vistos[v] {
			t.Errorf("%s não trouxe %s=%q: as combinações saem TODAS, sempre — zero é uma resposta, ausência não é",
				familia, rotulo, v)
		}
	}
}

// confereSourceFechadaOfLabels lê metrics_index.go pelo go/ast e exige que o
// valor de `estado` e de `modo` venha SÓ da enumeração exportada.
//
// Config e código são lidos como DADO neste repositório, nunca por grep em prosa:
// um grep por `"selado"` aprovaria um arquivo que monta o valor por concatenação,
// e reprovaria um comentário que menciona a palavra.
//
// A conferência tem uma porta só de propósito — o withLabel. Um segundo caminho
// (um metrics.Label{} montado à mão com LabelState) passaria por baixo desta
// checagem, e por isso ele também é recusado abaixo.
func confereSourceFechadaOfLabels(t *testing.T) {
	t.Helper()
	const file = "metrics_index.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse de %s: %v", file, err)
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
		if sel.Sel.Name != "IndexStates" && sel.Sel.Name != "IndexLookupModes" {
			return true
		}
		if id, ok := r.Value.(*ast.Ident); ok {
			fechadas[id.Name] = sel.Sel.Name
		}
		return true
	})
	if len(fechadas) != 2 {
		t.Fatalf("%s tem %d laço(s) sobre as enumerações exportadas, esperava 2 (estados e modos): "+
			"sem eles esta conferência aprovaria qualquer coisa por vacuidade", file, len(fechadas))
	}

	var conferidas int
	ast.Inspect(f, func(n ast.Node) bool {
		// Porta única: nenhum metrics.Label montado à mão pode carregar os dois
		// rótulos de enumeração, senão haveria um caminho que este teste não vê.
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "metrics" &&
				(sel.Sel.Name == "LabelState" || sel.Sel.Name == "LabelMode") {
				if !dentroOfWithRotulo(f, sel) {
					t.Errorf("%s usa metrics.%s fora de uma chamada a withLabel: uma segunda porta para o rótulo "+
						"é uma porta que esta conferência não enxerga", file, sel.Sel.Name)
				}
			}
		}
		chamada, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		fn, ok := chamada.Fun.(*ast.Ident)
		if !ok || fn.Name != "withLabel" || len(chamada.Args) != 3 {
			return true
		}
		conferidas++
		name, ok := chamada.Args[1].(*ast.SelectorExpr)
		if !ok || name.Sel.Name != "LabelState" && name.Sel.Name != "LabelMode" {
			t.Errorf("%s: withLabel recebeu um nome de rótulo que não é constante do pacote de métricas", file)
			return true
		}
		value, ok := chamada.Args[2].(*ast.Ident)
		if !ok {
			t.Errorf("%s: o VALOR de %s não é identificador — só a variável de um range sobre a enumeração exportada "+
				"pode alimentá-lo, e qualquer outra expressão é uma fonte que ninguém auditou",
				file, name.Sel.Name)
			return true
		}
		if _, fechado := fechadas[value.Name]; !fechado {
			t.Errorf("%s: o valor de %s vem de %q, que não é a variável de um range sobre metrics.IndexStates "+
				"nem sobre metrics.IndexLookupModes; label nunca vem de entrada do cliente",
				file, name.Sel.Name, value.Name)
		}
		return true
	})
	if conferidas != 2 {
		t.Fatalf("%s tem %d chamada(s) a withLabel, esperava 2: se a publicação mudou de forma, esta conferência "+
			"parou de olhar o caminho real", file, conferidas)
	}
}

// dentroOfWithRotulo responde se o selector está entre os argumentos de uma
// chamada a withLabel. É o que impede a checagem de aprovar um segundo caminho
// de publicação escrito ao lado.
func dentroOfWithRotulo(raiz ast.Node, target ast.Node) bool {
	achou := false
	ast.Inspect(raiz, func(n ast.Node) bool {
		chamada, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if fn, ok := chamada.Fun.(*ast.Ident); !ok || fn.Name != "withLabel" {
			return true
		}
		for _, arg := range chamada.Args {
			ast.Inspect(arg, func(m ast.Node) bool {
				if m == target {
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
// # POR QUE A BANCADA É HISTÓRICA, GRANDE E CARA
//
// A versão anterior deste teste rodava sobre um dataset MATERIALIZADO com a fila
// já drenada, e ali CINCO dos dez números são zero por construção: `Fora` porque
// a janela é infinita, `Pending` porque drenou, e `Rebuilds`, `CRCFailures` e
// `SegmentsByScan` porque nada os produz. Cinco comparações de 0 contra 0 — que é
// o defeito que o próprio godoc dizia estar impedindo. Medido: publicar
// `modo="enumeracao"`, `crc_falhas` ou `reconstrucoes` como CONSTANTE ZERO passava
// verde, e trocar os rótulos `pendente` e `fora` no mapa de samplesOfIndex
// também.
//
// A troca de rótulos é o pior modo de falha possível desta família. Durante a
// migração do piloto, `fora` é um número enorme e praticamente constante — a
// maioria dos 135 GB está fora da janela — e `pendente` é a fila encolhendo.
// Trocados, o painel mostra uma fila gigante e imóvel, que é exatamente a leitura
// que faz o operador concluir que o laço travou.
//
// Por isso a bancada monta um HISTÓRICO com teto de bytes, índices apagados e
// índices corrompidos: os dez números saem diferentes de zero e diferentes entre
// si, e aí qualquer troca de fio ou de rótulo fica vermelha sozinha.
func TestStatsEMetricsLeemAMesmaEstrutura(t *testing.T) {
	srv, _, _ := benchOfIndex(t)
	idx := indexOfDataset(t, srv, tableOfBench)
	exigeDezNumerosDistintos(t, idx)

	samples := coletaSamples(t, srv)
	rotulo := map[string]string{metrics.LabelDataset: tableOfBench}
	byState := map[string]int64{
		metrics.StateSealed:  idx.Sealed,
		metrics.StateCurrent: idx.Current,
		metrics.StatePending: idx.Pending,
		metrics.StateOutside: idx.OutOfWindow,
	}
	for _, state := range metrics.IndexStates {
		confereIgual(t, samples, metrics.MetIndexSegments,
			map[string]string{metrics.LabelDataset: tableOfBench, metrics.LabelState: state}, byState[state])
	}
	byModo := map[string]int64{
		metrics.ModeIndex:       idx.SegmentsByIndex,
		metrics.ModeEnumeration: idx.SegmentsByScan,
	}
	for _, modo := range metrics.IndexLookupModes {
		confereIgual(t, samples, metrics.MetIndexSegmentsLookedUpTotal,
			map[string]string{metrics.LabelDataset: tableOfBench, metrics.LabelMode: modo}, byModo[modo])
	}
	confereIgual(t, samples, metrics.MetIndexBytes, rotulo, idx.Bytes)
	confereIgual(t, samples, metrics.MetIndexRebuildsTotal, rotulo, idx.Rebuilds)
	confereIgual(t, samples, metrics.MetIndexCRCFailuresTotal, rotulo, idx.CRCFailures)
	var active int64
	if idx.RebuildRunning {
		active = 1
	}
	confereIgual(t, samples, metrics.MetIndexRebuildActive, rotulo, active)
}

// exigeDezNumerosDistintos é a guarda de não-trivialidade, e ela cobre os DEZ
// campos do bloco — não os quatro que por acaso não eram zero.
//
// Os nove numéricos precisam ser diferentes de zero E diferentes entre si. A
// distinção entre os dois requisitos importa: não-zero mata o fio congelado em
// constante, e a distinção mútua mata a troca de fio ou de rótulo, que é
// invisível quando dois números coincidem. O décimo campo, RebuildRunning, é 0/1
// por natureza — o par dele com `Current` (que é estruturalmente 1, porque só há
// um segmento corrente) é o único que esta bancada não consegue separar, e quem o
// separa é TestOMedidorDaReconstrucaoNaoEhOContadorDeCorrentes, no storage, onde
// existe um Store com segmento corrente e sem laço de pé.
//
// Se a distinção quebrar por causa de uma mudança no empacotamento de segmentos,
// a falha diz quais números colidiram: o conserto é retunar a bancada, nunca
// afrouxar a asserção.
func exigeDezNumerosDistintos(t *testing.T, idx protocol.IndexStats) {
	t.Helper()
	numeros := []struct {
		name string
		v    int64
	}{
		{"Sealed", idx.Sealed},
		{"Current", idx.Current},
		{"Pending", idx.Pending},
		{"OutOfWindow", idx.OutOfWindow},
		{"Bytes", idx.Bytes},
		{"Rebuilds", idx.Rebuilds},
		{"CRCFailures", idx.CRCFailures},
		{"SegmentsByIndex", idx.SegmentsByIndex},
		{"SegmentsByScan", idx.SegmentsByScan},
	}
	for _, n := range numeros {
		if n.v == 0 {
			t.Fatalf("o bloco index saiu com %s = 0 (%+v): um campo zerado transforma a comparação com o /metrics "+
				"em 0 == 0, e um fio congelado em constante zero passa por ela", n.name, idx)
		}
	}
	for i := range numeros {
		for j := i + 1; j < len(numeros); j++ {
			if numeros[i].v == numeros[j].v {
				t.Fatalf("o bloco index saiu com %s == %s == %d (%+v): com dois números iguais, trocar um fio pelo "+
					"outro fica invisível. Retune a bancada — nunca afrouxe esta asserção",
					numeros[i].name, numeros[j].name, numeros[i].v, idx)
			}
		}
	}
	if !idx.RebuildRunning {
		t.Fatalf("o bloco index saiu com RebuildRunning falso (%+v): o laço tem de estar de pé para o 0/1 significar algo", idx)
	}
}

// confereIgual casa uma série do scrape com o número que o /v1/stats publicou.
func confereIgual(t *testing.T, samples []amostraExposta, familia string, rotulos map[string]string, quer int64) {
	t.Helper()
	a, ok := achaSerie(samples, familia, rotulos)
	if !ok {
		t.Errorf("%s%v não saiu no /metrics, mas o /v1/stats publicou %d: o JSON e o painel deixaram de ler a mesma estrutura",
			familia, rotulos, quer)
		return
	}
	if int64(a.Value) != quer {
		t.Errorf("%s%v: /metrics diz %d e /v1/stats diz %d — duas fontes para o mesmo fato divergem, e em produção "+
			"alguém lê o errado", familia, rotulos, int64(a.Value), quer)
	}
}

// tableOfBench é a tabela irmã em que os dez números da bancada vivem. O
// índice por segmento é do STORE, então quem tem os números é a irmã — não o
// dataset base, que só declara a política.
const tableOfBench = "eventos_history"

// specOfBenchOfIndex é o dataset da bancada rica.
//
// Cada chave está aqui por um dos dez números:
//
//   - `segmentSize: 4KiB` produz ~21 segmentos com o volume semeado, que é o que
//     dá margem para os quatro estados terem contagens distintas;
//   - `index.maxBytes` (e NÃO `window`) é o que empurra os mais antigos para
//     `fora`. O teto de bytes foi escolhido em lugar da janela por tempo porque é
//     DETERMINÍSTICO: a fronteira por tempo anda com o relógio durante o teste, e
//     um segmento migraria de `selado` para `fora` no meio das asserções;
//   - `node` indexado é o campo sobre o qual as consultas resolvem por índice, e é
//     a AUSÊNCIA dele em parte dos eventos que produz os segmentos permanentemente
//     pendentes.
const specOfBenchOfIndex = `version: 1
name: eventos
schemaName: telecom
materialization:
  key:
    field: id
history:
  expireAfterWrite: 720h
  maxBytes: 4GiB
  segmentSize: 4KiB
  index:
    maxBytes: 64KiB
  fields:
    node:
      index: true
schema:
  include: ["*"]
  fields:
    id:
      type: string
    node:
      type: string
    severity:
      type: int64
`

// Quantos eventos com e sem o campo indexado. Os números são resultado de uma
// CALIBRAÇÃO, não gosto: eles põem os nove numéricos do bloco `index` em valores
// distintos. Mexer em qualquer um muda o empacotamento dos segmentos, e o
// exigeDezNumerosDistintos reprova dizendo quais colidiram.
const (
	eventosWithNode    = 950
	eventosWithoutNode = 220
)

var (
	// segmentosWithoutIndice têm o .sidx APAGADO antes da subida: eles entram na fila
	// por ausência e a reconstrução os publica de volta — é daí que sai Rebuilds.
	segmentosWithoutIndice = []uint32{6, 7, 8, 9, 10, 11, 12}
	// segmentosCorrompidos têm o ÚLTIMO byte trocado, que é onde o crc32c mora.
	//
	// É deliberadamente o FIM do arquivo, e não o começo: a tabela de seções
	// continua legível, então a fila de reconstrução — que não confere crc, por
	// não poder — julga o arquivo completo e NÃO o refaz. Corrompendo o cabeçalho
	// o segmento viraria "ilegível", seria reconstruído, e a corrupção sumiria
	// antes da primeira consulta. Foi o que aconteceu na primeira calibração
	// desta bancada, e o sintoma era CRCFailures ficar em zero.
	segmentosCorrompidos = []uint32{13, 14}
)

// benchOfIndex sobe um servidor cuja tabela histórica tem os quatro estados de
// segmento povoados, reconstruções feitas e índices reprovados no crc.
//
// A semeadura acontece ANTES da subida e reproduz a ordem do openHistory —
// dicionário compartilhado, spec derivada compilada contra ele, espelho, e só
// então o materializer com o histórico ligado. Reproduzir em vez de importar é o
// preço de não haver rota HTTP de ingestão; o que não pode é DIVERGIR, e por isso
// os campos indexados saem de schema.IndexedFieldNames, como no servidor.
func benchOfIndex(t *testing.T) (*server.Server, string, string) {
	t.Helper()
	a := newEnvWith(t, blockPadrao)
	a.writeDataset("eventos", specOfBenchOfIndex)
	dirIrma := seedHistory(t, a, "eventos", eventosWithNode, eventosWithoutNode)
	dir := filepath.Join(dirIrma, "data")

	for _, sec := range segmentosWithoutIndice {
		if err := os.Remove(segindex.SealedPath(dir, sec)); err != nil {
			t.Fatalf("montagem: apagar o índice do segmento %d: %v", sec, err)
		}
	}
	for _, sec := range segmentosCorrompidos {
		corruptOEndOfIndex(t, segindex.SealedPath(dir, sec))
	}

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	waitOsStatesAssentarem(t, srv)

	// Uma consulta pelo campo indexado: é ela que faz os dois contadores de
	// caminho e o de crc saírem de zero. Vem DEPOIS do assentamento, para que os
	// segmentos que a reconstrução ia publicar já tenham índice — senão os
	// números de caminho descreveriam um estado que já não existe.
	resp := consulta(t, srv, `{"dataset":"`+tableOfBench+`","sql":"SELECT id FROM `+tableOfBench+` WHERE node = 'n1'"}`)
	if resp.StatusCode != http.StatusOK {
		fecha(t, resp)
		t.Fatalf("a consulta indexada da bancada devolveu %d", resp.StatusCode)
	}
	fecha(t, resp)
	return srv, a.load().DatasetDir("eventos"), dirIrma
}

// waitOsStatesAssentarem bloqueia até o retrato parar de mudar.
//
// O critério é uma leitura ESTÁVEL por mais de uma passada do laço, e não "a fila
// esvaziou": nesta bancada ela nunca esvazia — os segmentos sem o campo indexado
// ficam pendentes de propósito (§2.4), e esperar pelo zero travaria para sempre.
//
// A espera existe porque o SegmentosPendentesOfRebuild publica o retrato no
// INÍCIO da passada, antes do trabalho dela: a primeira leitura descreve o
// dataset antes das reconstruções, e comparar contra ela mediria um instante que
// já passou.
func waitOsStatesAssentarem(t *testing.T, srv *server.Server) {
	t.Helper()
	var (
		anterior protocol.IndexStats
		desde    = time.Now()
		prazo    = time.Now().Add(90 * time.Second)
	)
	for time.Now().Before(prazo) {
		current := indexOfDataset(t, srv, tableOfBench)
		switch {
		case current != anterior:
			anterior, desde = current, time.Now()
		case time.Since(desde) > 6*time.Second:
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("os estados de segmento não assentaram em 90s; último retrato: %+v", anterior)
}

// indexOfDataset lê o bloco `index` de um dataset no /v1/stats.
func indexOfDataset(t *testing.T, srv *server.Server, name string) protocol.IndexStats {
	t.Helper()
	for _, ds := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
		if ds.Name == name {
			return ds.Index
		}
	}
	t.Fatalf("o dataset %q não apareceu no /v1/stats", name)
	return protocol.IndexStats{}
}

// corruptOEndOfIndex troca o último byte do arquivo, que é onde o crc32c mora.
func corruptOEndOfIndex(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("montagem: stat de %s: %v", path, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("montagem: abrir %s: %v", path, err)
	}
	if _, err := f.WriteAt([]byte{0xAA}, fi.Size()-1); err != nil {
		t.Fatalf("montagem: corromper %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("montagem: fechar %s: %v", path, err)
	}
}

// TestAAjudaDoContadorDeCrcNaoApontaSerieQueNaoDiscrimina afirma uma PROPRIEDADE
// do texto, e não a presença de uma palavra nele.
//
// A propriedade: a Ajuda desta série não pode indicar NENHUMA outra série da
// família como o sinal de "há corrupção agora", porque nenhuma delas discrimina.
// Quem mede isso é TestOGaugeDeEstadosEhCegoAIndicePodre, no storage: o gauge de
// estados sai idêntico entre um dataset saudável e um com .sidx podres, e a
// enumeração sobe no mesmo balde de um segmento fora da janela.
//
// A lista de proibidos sai de `familiasOfIndex`, e não de nomes escritos aqui:
// uma série NOVA que alguém acrescente entra na proibição sozinha, e é ela que
// alguém tentaria apontar. Foi exatamente essa a redação que este ciclo já
// escreveu duas vezes — primeiro "leia rate()", depois "olhe a enumeração" —, e
// as duas passariam por um oráculo de substring.
func TestAAjudaDoContadorDeCrcNaoApontaSerieQueNaoDiscrimina(t *testing.T) {
	srv := serverWithHistory(t, specWithHistory)
	a, ok := achaSerie(coletaSamples(t, srv), metrics.MetIndexCRCFailuresTotal,
		map[string]string{metrics.LabelDataset: "eventos_history"})
	if !ok {
		t.Fatalf("%s não saiu no scrape", metrics.MetIndexCRCFailuresTotal)
	}
	for _, familia := range familiasOfIndex {
		if familia == metrics.MetIndexCRCFailuresTotal {
			continue // ela pode falar de si mesma
		}
		if strings.Contains(a.Ajuda, familia) {
			t.Errorf("a ajuda de %s aponta para %s, e nenhuma série da família discrimina índice podre de segmento "+
				"legitimamente sem índice (TestOGaugeDeEstadosEhCegoAIndicePodre): o gauge de estados é cego e a "+
				"enumeração não atribui causa. Ajuda = %q", metrics.MetIndexCRCFailuresTotal, familia, a.Ajuda)
		}
	}
	// E o outro lado da propriedade: remover o conselho errado não basta, porque
	// quem lê a série quer saber o que olhar. O único sinal que existe hoje é o
	// WARN, e o texto tem de nomeá-lo — junto do débito que fecharia a lacuna.
	for _, exigido := range []string{"indice.selado_corrompido", "103"} {
		if !strings.Contains(a.Ajuda, exigido) {
			t.Errorf("a ajuda de %s não menciona %q: sem o sinal que de fato existe e sem o número do débito, ela "+
				"deixa o operador sem saída. Ajuda = %q", metrics.MetIndexCRCFailuresTotal, exigido, a.Ajuda)
		}
	}
	if strings.Contains(a.Ajuda, "não é memorizada") {
		t.Errorf("a ajuda de %s voltou a afirmar que a falha não é memorizada: o garanteSealed publica o mapeamento "+
			"reprovado no cache, então o contador é DEGRAU. Ajuda = %q", metrics.MetIndexCRCFailuresTotal, a.Ajuda)
	}
}

// TestOsBytesDeIndiceVivemNoRotuloDaIrma é o oráculo NUMÉRICO do cruzamento de
// rótulos, e ele existe porque a afirmação "parcela estrita" era falsa em todo
// rótulo em que alguém a lesse.
//
// `nishidb_history_index_bytes` é do bloco `history`: o número vem da tabela
// IRMÃ e o rótulo é o do dataset BASE, porque é no YAML do base que a política
// foi declarada. `nishidb_index_bytes` descreve o Store em que sai. Então a
// parcela e o total estão em rótulos diferentes, e a subtração que o texto antigo
// sugeria — `history_index_bytes - index_bytes`, que o PromQL casa por rótulo
// idêntico — devolve o total INTEIRO de volta.
//
// O oráculo mora no pacote `server` porque é aqui que o rótulo existe. A versão
// anterior comparava os dois números do MESMO Store, no storage, onde a contenção
// vale trivialmente e o cruzamento é invisível.
//
// Ele cobre os DOIS lados do cano — o /metrics e o /v1/stats — porque a
// proveniência é escolhida em duas funções diferentes (samplesOfHistory e
// historyStats), e apontar UMA delas para o Store errado passava verde na suíte
// inteira. É o mesmo defeito que originou a correção de prosa, do outro lado.
//
// E as asserções de PROSA moram aqui, coladas na medição, em vez de num teste de
// substring próprio: o que elas exigem do texto é derivado do que as linhas acima
// acabaram de medir — se a parcela deixar de viver noutro rótulo, a exigência de
// que o texto qualifique por rótulo cai junto.
func TestOsBytesDeIndiceVivemNoRotuloDaIrma(t *testing.T) {
	srv, dirBase, dirIrma := benchOfIndex(t)
	samples := coletaSamples(t, srv)

	value := func(familia, ds string) float64 {
		t.Helper()
		a, ok := achaSerie(samples, familia, map[string]string{metrics.LabelDataset: ds})
		if !ok {
			t.Fatalf("%s{dataset=%q} não saiu no scrape", familia, ds)
		}
		return a.Value
	}
	totalInBase := value(metrics.MetHistoryIndexBytes, "eventos")
	parcelaNaIrma := value(metrics.MetIndexBytes, tableOfBench)

	// O ORÁCULO É O DISCO, medido por fora do processo em cada um dos dois
	// diretórios. Comparar as duas SÉRIES entre si não bastava: a do base soma
	// indexes/ e checkpoint/ do base, que já são maiores que a parcela da irmã, e
	// por isso "a parcela é menor que o total" continuava verdadeiro mesmo com o
	// history_index_bytes lendo o Store ERRADO. O que discrimina é de QUAL
	// diretório o número saiu.
	ofDisk := func(dir string) float64 {
		t.Helper()
		return float64(blocosOfIndexIn(t, dir))
	}
	if quer := ofDisk(dirIrma); totalInBase != quer {
		t.Errorf("%s{dataset=\"eventos\"} = %v e o índice em disco da IRMÃ soma %v blocos: a série do bloco `history` "+
			"descreve a tabela irmã e sai no rótulo do BASE — se ela passar a descrever o próprio base, toda a prosa "+
			"sobre a parcela vira mentira de novo", metrics.MetHistoryIndexBytes, totalInBase, quer)
	}
	if ofBase := ofDisk(dirBase); totalInBase == ofBase {
		t.Errorf("%s{dataset=\"eventos\"} = %v é igual ao índice em disco do PRÓPRIO base: com os dois iguais este "+
			"oráculo não distingue de qual Store o número veio, e a bancada precisa ser retunada",
			metrics.MetHistoryIndexBytes, totalInBase)
	}
	// E a contenção, agora sabendo que os dois lados descrevem a mesma tabela por
	// rótulos diferentes.
	if parcelaNaIrma <= 0 || parcelaNaIrma >= totalInBase {
		t.Errorf("%s{dataset=%q} = %v contra o total %v: a parcela por segmento tem de ser positiva e estritamente "+
			"menor que o total, que soma também indexes/ e checkpoint/ da MESMA tabela",
			metrics.MetIndexBytes, tableOfBench, parcelaNaIrma, totalInBase)
	}
	// A conta que o texto antigo induzia, escrita como asserção: no rótulo do BASE
	// a série por segmento descreve o PRÓPRIO base, então subtraí-la ali não
	// cancela parcela nenhuma.
	if parcelaInBase := value(metrics.MetIndexBytes, "eventos"); parcelaInBase == parcelaNaIrma {
		t.Errorf("%s vale %v tanto em {dataset=\"eventos\"} quanto em {dataset=%q}: se os dois rótulos publicassem o "+
			"MESMO número, o cruzamento que esta prosa corrige não existiria",
			metrics.MetIndexBytes, parcelaInBase, tableOfBench)
	}

	// O MESMO oráculo do outro lado do cano. O bloco `history` do /v1/stats sai de
	// historyStats, uma função diferente da que emite as séries, e ela escolhe a
	// proveniência por conta própria: trocar base.irma.st por base.st ali passava
	// verde na suíte inteira.
	hist := historyOfDataset(t, srv, "eventos")
	if quer := ofDisk(dirIrma); hist.IndexBytes != int64(quer) {
		t.Errorf("/v1/stats: history.indexBytes de \"eventos\" = %d e o índice em disco da IRMÃ soma %v blocos: "+
			"o bloco `history` descreve a tabela irmã, e lê-lo do Store do base publica o dataset errado sob o "+
			"rótulo certo", hist.IndexBytes, quer)
	}
	if ofBase := ofDisk(dirBase); hist.IndexBytes == int64(ofBase) {
		t.Errorf("/v1/stats: history.indexBytes = %d é igual ao índice em disco do PRÓPRIO base: com os dois iguais "+
			"este oráculo não distingue de qual Store o número veio", hist.IndexBytes)
	}
	// E o /metrics e o /v1/stats têm de concordar, porque leem a mesma estrutura.
	if hist.IndexBytes != int64(totalInBase) {
		t.Errorf("/v1/stats diz %d e /metrics diz %v para o índice da tabela histórica: duas fontes para o mesmo "+
			"fato divergem", hist.IndexBytes, totalInBase)
	}

	// A PROSA, derivada do que acabou de ser medido. As duas séries carregam
	// números da mesma tabela sob rótulos diferentes, então o texto que as
	// relaciona só é útil se qualificar por rótulo — e não pode reinstaurar a
	// afirmação de que a purga não devolve os bytes, que foi o outro erro deste
	// mesmo texto.
	ajuda, ok := achaSerie(samples, metrics.MetHistoryIndexBytes, map[string]string{metrics.LabelDataset: "eventos"})
	if !ok {
		t.Fatalf("%s não saiu no scrape", metrics.MetHistoryIndexBytes)
	}
	if strings.Contains(ajuda.Ajuda, "NÃO os devolve") {
		t.Errorf("a ajuda de %s voltou a afirmar que a purga não devolve os bytes de índice: %q — o .sidx sai no "+
			"mesmo passo que remove o .dat", metrics.MetHistoryIndexBytes, ajuda.Ajuda)
	}
	if !strings.Contains(ajuda.Ajuda, metrics.MetIndexBytes) || !strings.Contains(ajuda.Ajuda, "dataset=") {
		t.Errorf("a ajuda de %s relaciona as duas séries sem qualificar por RÓTULO (%q): as linhas acima acabaram de "+
			"medir que a parcela vive em {dataset=%q} e este número em {dataset=\"eventos\"}, então um texto que "+
			"não diga isso induz a subtração no mesmo rótulo — que subtrai a série errada",
			metrics.MetHistoryIndexBytes, ajuda.Ajuda, tableOfBench)
	}
}

// historyOfDataset lê o bloco `history` de um dataset no /v1/stats.
func historyOfDataset(t *testing.T, srv *server.Server, name string) protocol.HistoryStats {
	t.Helper()
	for _, ds := range readJSON[protocol.Stats](t, srv, "/v1/stats").Datasets {
		if ds.Name == name {
			if ds.History == nil {
				t.Fatalf("o dataset %q não publicou o bloco history", name)
			}
			return *ds.History
		}
	}
	t.Fatalf("o dataset %q não apareceu no /v1/stats", name)
	return protocol.HistoryStats{}
}

// blocosOfIndexIn é o `du` do operador sobre tudo que conta como índice de um
// dataset: indexes/, checkpoint/ e os arquivos de índice que moram em data/.
//
// Medido em BLOCOS, como a produção mede, e por fora do processo — é a única
// forma de dizer de QUAL diretório um número publicado saiu.
func blocosOfIndexIn(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	for _, sub := range []string{"indexes", "checkpoint"} {
		total += somaBlocos(t, filepath.Join(dir, sub), nil)
	}
	total += somaBlocos(t, filepath.Join(dir, "data"), func(name string) bool {
		return strings.HasSuffix(name, ".sidx") || strings.HasSuffix(name, ".widx") || strings.HasSuffix(name, ".tmp")
	})
	return total
}

// somaBlocos percorre o diretório somando os blocos ALOCADOS dos arquivos que
// passam no filtro. Diretório ausente soma zero: é o estado normal de um dataset
// que ainda não tem aquela parte.
func somaBlocos(t *testing.T, dir string, aceita func(string) bool) int64 {
	t.Helper()
	entradas, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entradas {
		if e.IsDir() {
			total += somaBlocos(t, filepath.Join(dir, e.Name()), aceita)
			continue
		}
		if aceita != nil && !aceita(e.Name()) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			t.Fatalf("stat de %s não trouxe Stat_t: este oráculo mede BLOCOS, e sem eles ele mediria outra coisa", e.Name())
		}
		total += st.Blocks * 512
	}
	return total
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
// internal/kafka/metrics.go — inclusive na nishidb_kafka_poll_blocked_seconds_total,
// que é a série sobre a qual a medição pré-ciclo do piloto foi construída. Aquilo
// é de outro pacote e de outra tarefa; aqui fica o registro de que este oráculo
// NÃO o alcança.
func TestOsCabecalhosSaemNomeadosParaCadaFamilia(t *testing.T) {
	srv := serverWithHistory(t, specWithHistory)
	corpo := fetch(t, srv, "/metrics")

	withCabecalho := map[string]bool{}
	for _, row := range strings.Split(corpo, "\n") {
		depois, achou := strings.CutPrefix(row, "# HELP ")
		if !achou {
			if depois, achou = strings.CutPrefix(row, "# TYPE "); !achou {
				continue
			}
		}
		familia, _, _ := strings.Cut(depois, " ")
		if familia == "" {
			t.Errorf("cabeçalho com nome de métrica VAZIO: %q — é o sintoma de amostras publicadas sem o campo "+
				"Familia, e o Prometheus recusa o scrape inteiro por causa dele, não só a linha", row)
			continue
		}
		withCabecalho[familia] = true
	}

	// Todas as famílias do histórico e do índice, que são as que esta tarefa
	// publica ou conserta.
	esperadas := append([]string{
		metrics.MetHistoryRows, metrics.MetHistoryDropped, metrics.MetHistoryDegraded,
		metrics.MetHistoryDataBytes, metrics.MetHistoryIndexBytes, metrics.MetHistoryOldestWrittenAt,
		metrics.MetHistoryIndexWindow, metrics.MetHistoryPurged, metrics.MetHistoryRetentionRunning,
	}, familiasOfIndex...)
	for _, familia := range esperadas {
		if !withCabecalho[familia] {
			t.Errorf("%s saiu sem # HELP/# TYPE nomeados: série sem tipo não aceita rate(), e série sem ajuda "+
				"chega ao operador sem a metade do contrato que viaja no texto", familia)
		}
	}
}

// nomesDasFamiliasOfIndex existe para a mensagem de erro do teste de cobertura
// abaixo sair legível e ordenada.
func nomesDasFamiliasOfIndex() string {
	nomes := append([]string(nil), familiasOfIndex...)
	sort.Strings(nomes)
	return strings.Join(nomes, ", ")
}

// TestAsSeriesDeIndiceSaemSoPelasConstantes fecha a primeira das três invariantes
// de métrica do projeto: nome só pelas constantes de names.go.
//
// A conferência é do LADO DO SCRAPE — toda linha `nishidb_index_*` que sai na
// porta tem de pertencer à lista de constantes. Um nome digitado à mão no coletor
// produziria uma série que o dashboard nunca casa, e o sintoma é um painel vazio
// sem ninguém perceber, que é exatamente o que o cabeçalho de names.go descreve.
func TestAsSeriesDeIndiceSaemSoPelasConstantes(t *testing.T) {
	srv := serverWithHistory(t, specWithHistory)
	conhecidas := map[string]bool{}
	for _, f := range familiasOfIndex {
		conhecidas[f] = true
	}
	var vistas int
	for _, a := range coletaSamples(t, srv) {
		if !strings.HasPrefix(a.Name, "nishidb_index_") {
			continue
		}
		vistas++
		if !conhecidas[a.Name] {
			t.Errorf("o scrape trouxe %q, que não é nenhuma das constantes de names.go (%s): "+
				"nome digitado à mão diverge no dia em que um dos dois lugares for corrigido",
				a.Name, nomesDasFamiliasOfIndex())
		}
	}
	if vistas == 0 {
		t.Fatal("nenhuma série nishidb_index_* no scrape: a conferência passaria por vacuidade")
	}
}

// TestASerieDaReconstrucaoDesceQuandoOLacoMorre é o oráculo do DÉCIMO fio, e ele
// existe porque nenhum outro teste do repositório o cobria.
//
// # POR QUE OS OUTROS NÃO PEGAM
//
// O oráculo principal desta família compara `/metrics` contra `/v1/stats`. Quando
// uma mutação alimenta os DOIS lados da mesma fonte errada — trocar
// `umSeVerdadeiro(e.RebuildActive)` por `float64(e.States.Current)` nas duas
// funções —, a comparação vira `1 == 1` e não vê nada. E num servidor saudável os
// dois números são estruturalmente iguais: só existe um segmento corrente, o laço
// sobe sempre que há campo indexado, e `Current` só é publicado quando há campo
// indexado. Correlação perfeita, oráculo cego.
//
// TestOMedidorDaReconstrucaoNaoEhOContadorDeCorrentes, no storage, afirma sobre a
// ESTRUTURA DE ORIGEM. A camada de publicação — que é o que a bancada existe para
// gatear — ficava descoberta justamente neste fio.
//
// # O CASO QUE DISCRIMINA
//
// É o alarme que a própria Ajuda da série promete: *"ele para de vez na primeira
// falha"*. Um `.sidx` substituído por um DIRETÓRIO faz o mmap devolver ENODEV, que
// não é nenhum dos três estados não-anômalos do unreadableIndex: a passada seguinte
// falha, o laço morre e o medidor desce. O retrato de estados ANTERIOR fica de pé,
// porque uma passada que falhou não publica — então o dataset passa a ter
// `corrente=1` com `reconstrucao_ativa=0`, que é o único estado em que os dois
// fios divergem.
//
// A sabotagem vem DEPOIS da subida de propósito: plantada antes, a primeira
// passada falharia sem nunca publicar retrato nenhum, e `corrente` ficaria em zero
// junto com o medidor — os dois iguais de novo, e o oráculo cego outra vez.
//
// A CONSEQUÊNCIA OPERACIONAL justifica o teste sozinha: com o fio ecoando a
// contagem de correntes, a série responde 1 para todo dataset com dado, o laço
// morto nunca aparece e o dataset degrada para enumeração em silêncio.
func TestASerieDaReconstrucaoDesceQuandoOLacoMorre(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "  segmentSize: 8KiB\n")
	a.writeDataset("eventos", specWithWindowApertada)
	dirIrma := seedHistory(t, a, "eventos", 0, 600)

	record := &recordOfServer{}
	srv, err := server.Novo(a.load(), server.Options{Log: logging.New(record, logging.FormatText, slog.LevelInfo)})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	// Pré-condição: o laço de pé E o retrato publicado. Sem os dois, o estado
	// final abaixo não distingue "desceu" de "nunca subiu".
	waitUntil(t, func() bool {
		e := indexOfDataset(t, srv, tableOfBench)
		return e.RebuildRunning && e.Current == 1
	}, "o laço não subiu com o retrato publicado (RebuildRunning e Current=1) em 30s")

	// A sabotagem: um diretório no lugar do .sidx do segmento mais novo, que é o
	// único que a janela apertada admite — e portanto o único que a pendência
	// abre.
	selados := seladosOfDataset(t, dirIrma)
	target := indexOf(selados[len(selados)-1])
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		t.Fatalf("montagem: remover %s: %v", target, err)
	}
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("montagem: plantar diretório em %s: %v", target, err)
	}

	// A espera é pelo LOG, e não pelo medidor, e a diferença é o teste inteiro: o
	// log é o fato INDEPENDENTE de que o laço morreu, enquanto o medidor é
	// justamente o que está em julgamento. Esperar pelo medidor faria a mutação
	// reprovar por timeout, com a mensagem errada — "o laço não morreu", quando o
	// que aconteceu foi a série não ter contado.
	waitUntil(t, func() bool {
		return strings.Contains(record.String(), "event=reconstrucao.parou")
	}, "o laço não parou em 30s: o diretório plantado devia fazer a passada falhar com ENODEV, "+
		"e sem a morte dele os dois fios continuam iguais e este oráculo não discrimina")

	// O ESTADO QUE DISCRIMINA, lido no /v1/stats e no /metrics.
	e := indexOfDataset(t, srv, tableOfBench)
	if e.RebuildRunning {
		t.Errorf("/v1/stats: rebuildRunning continua verdadeiro depois de o log registrar reconstrucao.parou (%+v): "+
			"o medidor está ecoando outra coisa — zero é o alarme que a Ajuda promete", e)
	}
	if e.Current != 1 {
		t.Fatalf("corrente = %d depois de o laço morrer, queria 1 (%+v): o retrato anterior tem de ficar de pé, "+
			"senão os dois fios voltam a ser iguais e este oráculo não discrimina", e.Current, e)
	}
	rotulo := map[string]string{metrics.LabelDataset: tableOfBench}
	active, ok := achaSerie(coletaSamples(t, srv), metrics.MetIndexRebuildActive, rotulo)
	if !ok {
		t.Fatalf("%s não saiu para %q", metrics.MetIndexRebuildActive, tableOfBench)
	}
	if active.Value != 0 {
		t.Errorf("%s = %v com o laço MORTO e corrente = %d: zero é o alarme que a própria Ajuda desta série "+
			"promete, e um fio que ecoe a contagem de correntes responde 1 para todo dataset com dado — o laço "+
			"morto nunca aparece e o dataset degrada para enumeração em silêncio",
			metrics.MetIndexRebuildActive, active.Value, e.Current)
	}
}
