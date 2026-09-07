package segindex_test

import (
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/primary"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Este arquivo NÃO tem build tag, e a ausência é a decisão: os gates abaixo
// medem ACESSOS AO MAPEAMENTO, que a instrumentação do detector de corrida não
// altera. Eles rodam nas duas rodadas, e por isso não existe (nem faria sentido
// existir) uma contraparte `//go:build race` que se anuncie como SKIP — nada
// aqui é pulado. Os gates de ALOCAÇÃO deste pacote, que a instrumentação de
// fato altera, são os que têm contraparte nominal, em alloc_gate_race_test.go
// (débito 53).

// testHash devolve o hash do i-ésimo valor distinto pela MESMA função que a
// ingestão usa.
//
// Usar primary.HashKey, e não uma sequência tipo `i+2`, é o que dá sentido ao
// gate de dispersão: valores consecutivos sob máscara de potência de dois caem
// em slots consecutivos e NÃO colidem nunca, então um gate alimentado assim
// mediria o melhor caso possível da sondagem linear e aprovaria qualquer coisa.
// É também a função de quem o formato depende para o sentinela de slot vazio:
// ela nunca devolve 0 nem 1.
func testHash(buf []byte, i int) uint64 {
	return primary.HashKey(binary.LittleEndian.AppendUint64(buf[:0], uint64(i)))
}

// TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento é o gate que este pacote
// inteiro existe para satisfazer. O motor antigo varria a cadeia de probe
// inteira a cada Set para não duplicar, então inserir a N-ésima posting de um
// valor custava N sondas e inserir n postings custava O(n²). No piloto isso deu
// 1,5 ms por Set e 82% do CPU do processo.
//
// A medida é de ACESSOS AO MAPEAMENTO e não de tempo: sem relógio não há ruído
// de runner, e o gate afirma a FORMA do algoritmo em vez de um orçamento que
// precisa ser recalibrado a cada máquina.
//
// Contar acessos, e não sondas de diretório, é o que fecha o buraco que
// importa. Todas as postings são do MESMO valor — é o caso que derrubou o
// piloto (node: ~3.724 postings por valor) —, então a sondagem de diretório
// encontra o slot na PRIMEIRA tentativa em qualquer escala, e um contador que
// só soubesse de slots marcaria 1 para sempre. Ele aprovaria em silêncio a
// regressão exata do motor antigo traduzida para esta estrutura: um Set que
// percorresse a cadeia de postings antes de inserir, para não duplicar. Essa
// caminhada não tem como acontecer sem ler o mapeamento, e por isso aparece
// aqui. TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador é o que garante que
// ela não possa se esconder de w.countedRead.
//
// O teto é sobre o PIOR caso, e não sobre a média, porque aqui o custo tem de
// ser constante: são duas leituras — o slot e o estado dele — em qualquer n.
func TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento(t *testing.T) {
	const accessCeiling = 4 // amortizado; o diretório é dimensionado por DISTINTOS
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			w := openTestWIP(t)
			defer closeWIP(t, w)
			var worst int
			for i := range n {
				if err := w.Set(100, types.RowID(i)); err != nil {
					t.Fatalf("Set #%d: %v", i, err)
				}
				worst = max(worst, w.LastSetAccesses())
			}
			if worst > accessCeiling {
				t.Errorf("pior caso de %d acessos com n=%d, teto %d — a cadeia voltou a crescer com o segmento",
					worst, n, accessCeiling)
			}
		})
	}
}

// TestOCustoDaSondagemNaoAcompanhaONumeroDeValoresDistintos é a outra metade, e
// é a que exercita a sondagem de verdade: aqui TODO valor é distinto, então o
// diretório enche até os 70% de carga, rehasheia, e volta a encher — que é o
// regime de uma coluna como `identifier`.
//
// O teto é sobre a MÉDIA, e a diferença em relação ao gate irmão não é
// arbitrária. Sondagem linear tem cauda por construção: o pior caso sobre n
// inserções cresce com n (medido: 80 acessos em 10 mil, 103 em 100 mil, 135 em
// 1 milhão) porque o máximo de n amostras de uma distribuição com cauda cresce
// com n, e não porque o algoritmo piorou — a MÉDIA das mesmas três corridas fica
// em 3,85, 3,98 e 3,78. O que a estrutura promete é custo AMORTIZADO constante,
// e o número que expressa isso é a média.
//
// Por isso as duas asserções. O teto absoluto pega uma regressão que encarecesse
// a sondagem; a comparação entre escalas pega a regressão que este ciclo existe
// para impedir, que é o custo passar a ACOMPANHAR o tamanho do segmento. Um
// O(n²) reprova na segunda muito antes de a primeira ter chance.
//
// O custo do rehash não entra na conta, e é correto que não entre: ele é
// amortizado, é pago fora do Set e tem gate próprio de corretude
// (TestOWIPCresceSemPerderPosting). O que este gate mede é o que a ingestão
// paga por evento.
func TestOCustoDaSondagemNaoAcompanhaONumeroDeValoresDistintos(t *testing.T) {
	// Medido em 29/08/2026: 3,85 / 3,98 / 3,78 acessos por Set nas três
	// escalas. O teto tem folga de ~50% sobre o pior dos três; a série é
	// determinística (primary.HashKey não varia entre máquinas), então a folga
	// é para mudança de load factor, não para ruído de runner.
	const avgCeiling = 6.0
	// A média pode oscilar entre escalas sem que nada tenha piorado — o que ela
	// não pode é CRESCER com n. 1,5x é folga larga o bastante para a oscilação
	// medida (3,85 -> 3,78, ou seja 0,98x de 10 mil para 1 milhão) e estreita o
	// bastante para reprovar qualquer termo que acompanhe o tamanho do segmento.
	const growthCeiling = 1.5

	scales := []int{10_000, 100_000, 1_000_000}
	avgs := make([]float64, len(scales))
	for k, n := range scales {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			w := openTestWIPWithSlots(t, 16) // começa apertado: o diretório rehasheia dezenas de vezes
			defer closeWIP(t, w)
			var buf [8]byte
			sum := 0
			for i := range n {
				if err := w.Set(testHash(buf[:], i), types.RowID(i)); err != nil {
					t.Fatalf("Set #%d: %v", i, err)
				}
				sum += w.LastSetAccesses()
			}
			if got := w.DistinctValues(); got != uint32(n) {
				t.Fatalf("DistinctValues() = %d, quero %d — o corpus deixou de ser de valores distintos e o gate não mede mais a sondagem", got, n)
			}
			avgs[k] = float64(sum) / float64(n)
			t.Logf("média de %.3f acessos por Set com %d valores distintos", avgs[k], n)
			if avgs[k] > avgCeiling {
				t.Errorf("média de %.3f acessos por Set com n=%d, teto %.1f", avgs[k], n, avgCeiling)
			}
		})
	}
	if avgs[0] == 0 {
		t.Fatal("a menor escala não mediu nada — sem ela a comparação entre escalas não significa nada")
	}
	if ratio := avgs[len(avgs)-1] / avgs[0]; ratio > growthCeiling {
		t.Errorf("a média por Set cresceu %.2fx de n=%d para n=%d (%.3f -> %.3f), teto %.1fx — o custo voltou a acompanhar o tamanho do segmento",
			ratio, scales[0], scales[len(scales)-1], avgs[0], avgs[len(avgs)-1], growthCeiling)
	}
}

// TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador é o que torna os dois gates
// acima verdadeiros em vez de aspiracionais.
//
// Eles medem w.accesses, que só sobe dentro de w.countedRead. Um Set que
// lesse o mapeamento por outro caminho — e a regressão que interessa, a
// caminhada da cadeia de postings para deduplicar, é exatamente isso — não
// apareceria em contador nenhum, e os dois gates passariam verdes sobre o
// O(n²) de volta.
//
// A conferência é sobre o CÓDIGO, e não sobre o comportamento, porque não há
// como observar "leu o mapeamento sem contar" de fora. É o mesmo idioma dos
// gates que leem o Makefile e os manifestos como dado: config e código são
// lidos como estrutura, nunca por grep em prosa — daí o parser em vez de uma
// busca textual, que casaria com comentários e com o próprio corpo de
// countedRead.
//
// O caminho de LEITURA faz o contrário de propósito: Lookup e ForEach leem o
// mapeamento direto porque w.accesses é um int comum, e um leitor concorrente
// que o incrementasse criaria a corrida de dados que o mu existe para não ter.
func TestOCaminhoDeEscritaLeOMapeamentoSoPeloContador(t *testing.T) {
	const file = "wip.go"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse de %s: %v", file, err)
	}
	writePathMethods := map[string]bool{"Set": true, "probeOrCreate": true}
	found := map[string]bool{}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv == nil || !writePathMethods[fd.Name.Name] {
			continue
		}
		found[fd.Name.Name] = true
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "AtomicLoad64" {
				return true
			}
			t.Errorf("%s:%d: %s lê o mapeamento por AtomicLoad64 direto — o contador de acessos não veria essa leitura, e uma caminhada de cadeia acrescentada aqui passaria despercebida pelos gates de custo. Use w.countedRead.",
				file, fset.Position(sel.Pos()).Line, fd.Name.Name)
			return true
		})
	}
	// Sem esta conferência o gate passaria VAZIO no dia em que um dos dois
	// métodos fosse renomeado: nada seria inspecionado e nada seria reportado.
	for name := range writePathMethods {
		if !found[name] {
			t.Errorf("não achei o método %s em %s — o gate inspecionou menos do que promete", name, file)
		}
	}
}
