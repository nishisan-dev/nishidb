package main

import (
	"strings"
	"testing"
)

// mapping é o mapeamento de bancada: um identificador sem ambiguidade
// (`Fila`), um método (`Fila.Empurra`), um nome que dois pacotes traduzem
// IGUAL (`Compila`, que por isso não é ambíguo) e um que dois pacotes
// traduzem DIFERENTE (`Aposenta`, que é o caso do relatório).
//
// As entradas entram por addIdentRow, que é o MESMO caminho de loadMapping —
// e a coluna passada é a FOLHA (`Push`), como no TSV. A primeira redação
// escrevia `Queue.Push` na mão aqui, e com isso o fixture escondia que o
// carregador de verdade perdia o receptor.
func mapping() *Mapping {
	m := NewMapping()
	m.addIdentRow("internal/kafka", "type", "Fila", "Queue")
	m.addIdentRow("internal/kafka", "method", "Fila.Empurra", "Push")
	m.addIdentRow("internal/contadores", "func", "Compila", "Compile")
	m.addIdentRow("internal/schema", "func", "Compila", "Compile") // mesmo novo: não é ambíguo
	m.addIdentRow("internal/storage", "func", "Aposenta", "Retire")
	m.addIdentRow("internal/query", "func", "Aposenta", "Dismiss") // ambíguo com storage
	m.Tests["TestFilaRecusaCheia"] = "TestQueueRefusesWhenFull"
	m.Tests["test_fila_cheia"] = "test_queue_full"
	m.LogKeys["consumer.subiu"] = "consumer.up"
	m.Paths["internal/contadores"] = "internal/counters"
	m.Paths["internal/kafka/fila.go"] = "internal/kafka/queue.go"
	m.Packages["contadores"] = "counters"
	return m
}

func TestBacktickReplacesUnqualifiedIdentifierWithoutAmbiguity(t *testing.T) {
	out, rep := RewriteText([]byte("a `Fila` limitada e `Compila` o bloco"), ".md", mapping())
	if got := string(out); got != "a `Queue` limitada e `Compile` o bloco" {
		t.Fatalf("saída: %q", got)
	}
	if len(rep.Skipped) != 0 {
		t.Fatalf("pulou: %v", rep.Skipped)
	}
}

func TestBacktickSkipsAmbiguousAndReports(t *testing.T) {
	out, rep := RewriteText([]byte("o `Aposenta` libera"), ".md", mapping())
	if string(out) != "o `Aposenta` libera" {
		t.Fatalf("substituiu ambíguo: %q", out)
	}
	if sk := rep.Skipped; len(sk) != 1 || sk[0].Token != "Aposenta" || !strings.Contains(sk[0].Reason, "ambíguo") {
		t.Fatalf("relatório: %v", sk)
	}
}

func TestQualifiedBacktickResolvesByPackage(t *testing.T) {
	out, _ := RewriteText([]byte("`storage.Aposenta` e `query.Aposenta` e `contadores.Compila` e `kafka.Fila.Empurra`"), ".md", mapping())
	want := "`storage.Retire` e `query.Dismiss` e `counters.Compile` e `kafka.Queue.Push`"
	if string(out) != want {
		t.Fatalf("saída: %q\nquer:  %q", out, want)
	}
}

func TestBareWordInProseIsUntouched(t *testing.T) {
	src := "a fila limitada Aposenta o grupo quando Compila termina"
	out, _ := RewriteText([]byte(src), ".md", mapping())
	if string(out) != src {
		t.Fatalf("tocou prosa: %q", out)
	}
}

func TestTestNameIsReplacedAnywhere(t *testing.T) {
	out, _ := RewriteText([]byte("ver TestFilaRecusaCheia e `TestFilaRecusaCheia` e test_fila_cheia; TestFilaRecusaCheiaOutro fica"), ".md", mapping())
	want := "ver TestQueueRefusesWhenFull e `TestQueueRefusesWhenFull` e test_queue_full; TestFilaRecusaCheiaOutro fica"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestLogKeyOnlyInBackticksOrQuotes(t *testing.T) {
	out, _ := RewriteText([]byte("`consumer.subiu` e \"consumer.subiu\" e consumer.subiu solto"), ".md", mapping())
	want := "`consumer.up` e \"consumer.up\" e consumer.subiu solto"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestPathIsReplacedAnywhereLongestFirst(t *testing.T) {
	out, _ := RewriteText([]byte("internal/kafka/fila.go e internal/contadores/compila.go e [x](../internal/contadores)"), ".md", mapping())
	want := "internal/kafka/queue.go e internal/counters/compila.go e [x](../internal/counters)"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestDocLinkAndQualifiedNameInGoComment(t *testing.T) {
	src := "package x\n\n// Roda usa [Fila] e kafka.Fila.Empurra e contadores.Compila; a fila solta fica.\n// Ver TestFilaRecusaCheia.\nfunc Roda() {}\n"
	out, _ := RewriteGoComments([]byte(src), mapping())
	want := "package x\n\n// Roda usa [Queue] e kafka.Queue.Push e counters.Compile; a fila solta fica.\n// Ver TestQueueRefusesWhenFull.\nfunc Roda() {}\n"
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
}

// TestGoCommentPassDoesNotTouchCode usa `kafka.Fila` no CÓDIGO de propósito:
// é a forma que a regra do qualificado (`applyGoCommentExtras`) reescreveria
// se a passada saísse do span do comentário. Com `var Fila = 1` no lugar, que
// foi a primeira redação, nenhuma regra pegava no código e o teste passava com
// as duas mutações do span presentes.
func TestGoCommentPassDoesNotTouchCode(t *testing.T) {
	src := "package x\n\nvar q = kafka.Fila // a `Fila` de verdade\n"
	out, _ := RewriteGoComments([]byte(src), mapping())
	want := "package x\n\nvar q = kafka.Fila // a `Queue` de verdade\n"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestPathLiteralInGoIsRewritten(t *testing.T) {
	src := "package x\n\nvar p = \"internal/kafka/fila.go\"\nvar q = \"internal/contadores/x.go\"\n"
	out, _ := RewriteGoComments([]byte(src), mapping())
	want := "package x\n\nvar p = \"internal/kafka/queue.go\"\nvar q = \"internal/counters/x.go\"\n"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestSubtestOnlyInTheFirstArgumentOfRun(t *testing.T) {
	m := mapping()
	m.Subtests["x_test.go"] = map[string]string{"fila cheia": "queue full"}
	src := "package x\n\nfunc TestA(t *T) {\n\tt.Run(\"fila cheia\", nil)\n\tlog(\"fila cheia\")\n}\n"
	out, _ := RewriteGoFile("x_test.go", []byte(src), m)
	want := "package x\n\nfunc TestA(t *T) {\n\tt.Run(\"queue full\", nil)\n\tlog(\"fila cheia\")\n}\n"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

// pathMapping é o mapeamento mínimo dos testes de caminho: um caminho só, para
// a fronteira ser a única variável.
func pathMapping() *Mapping {
	m := NewMapping()
	m.Paths["internal/predicado"] = "internal/predicate"
	return m
}

// TestPathBoundaryAcceptsSentenceEndAndRejectsWordContinuation fecha o buraco
// da primeira redação, cujo conjunto de fronteira era uma lista de caracteres
// que esquecia `.`, `*`, `-` e `(`: um caminho no fim de frase ("agora em
// internal/predicado.") ficava para trás em silêncio, e o repositório tem essa
// forma em internal/query/executor/predicate.go, internal/cli/server.go,
// internal/typeparse/typeparse.go e internal/gates/jdbc_test.go.
func TestPathBoundaryAcceptsSentenceEndAndRejectsWordContinuation(t *testing.T) {
	src := "agora em internal/predicado. Ver **internal/predicado**, (internal/predicado) e `internal/predicado`;\n" +
		"internal/predicado-x, internal/predicadox e internal/predicado_y ficam."
	want := "agora em internal/predicate. Ver **internal/predicate**, (internal/predicate) e `internal/predicate`;\n" +
		"internal/predicado-x, internal/predicadox e internal/predicado_y ficam."
	out, _ := RewriteText([]byte(src), ".md", pathMapping())
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
}

// TestImportPathIsNotRewritten: a passada Go alcançava o ImportSpec pelo
// ast.Inspect e trocava o caminho do import sem trocar o qualificador no
// código, deixando um arquivo que não compila — e desmentindo o próprio godoc,
// que diz que código é do rf.
func TestImportPathIsNotRewritten(t *testing.T) {
	src := "package x\n\nimport \"github.com/nishisan-dev/nishidb/internal/predicado\"\n\n" +
		"var doc = \"internal/predicado/avalia.go\"\n\nvar _ = predicado.Avalia\n"
	want := "package x\n\nimport \"github.com/nishisan-dev/nishidb/internal/predicado\"\n\n" +
		"var doc = \"internal/predicate/avalia.go\"\n\nvar _ = predicado.Avalia\n"
	out, _ := RewriteGoComments([]byte(src), pathMapping())
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
}

// TestRawStringLiteralKeepsItsLines: reemitir com strconv.Quote achatava um
// literal de crase de várias linhas numa linha só, com \n escapado. O
// repositório é cheio deles (internal/cli/cli.go, internal/contadores/meta.go,
// internal/cli/metakafka_query_test.go).
func TestRawStringLiteralKeepsItsLines(t *testing.T) {
	src := "package x\n\nvar fixture = `linha um\ninternal/predicado/x.go\nlinha tres`\n"
	want := "package x\n\nvar fixture = `linha um\ninternal/predicate/x.go\nlinha tres`\n"
	out, _ := RewriteGoComments([]byte(src), pathMapping())
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
}

// TestSkippedLineIsThePositionOfEachOccurrence: o relatório virou guarda (é
// nele que se confere o que a ferramenta aplicou), então linha errada nele é
// defeito. A primeira redação usava strings.Index, que devolve sempre a
// PRIMEIRA ocorrência do token.
func TestSkippedLineIsThePositionOfEachOccurrence(t *testing.T) {
	src := "linha um\no `Aposenta` aqui\nmeio\ne `Aposenta` de novo\n"
	_, rep := RewriteText([]byte(src), ".md", mapping())
	sk := rep.Skipped
	if len(sk) != 2 {
		t.Fatalf("pulas: %v, esperadas 2", sk)
	}
	if sk[0].Line != 2 || sk[1].Line != 4 {
		t.Fatalf("linhas: %d e %d, esperadas 2 e 4", sk[0].Line, sk[1].Line)
	}
}

// TestSkippedInGoCommentExtrasCarriesItsLine: as pulas das regras exclusivas
// de comentário Go (doc link e qualificado) saíam com linha 0, porque
// applyGoCommentExtras não recebia posição nenhuma.
func TestSkippedInGoCommentExtrasCarriesItsLine(t *testing.T) {
	src := "package x\n\n// linha tres\n// ver [Aposenta] aqui\nfunc F() {}\n"
	_, rep := RewriteGoComments([]byte(src), mapping())
	sk := rep.Skipped
	if len(sk) != 1 {
		t.Fatalf("pulas: %v, esperada 1", sk)
	}
	if sk[0].Line != 4 {
		t.Fatalf("linha: %d, esperada 4", sk[0].Line)
	}
}

// citationMapping é o mapeamento de bancada das regras de citação de folha,
// token nu, receptor qualificado, crase composta e literal. As entradas são as
// do repositório de verdade — `Snapshot.Epoca`, `Region.LiberaBlocos`,
// `specsDoMarcador` —, porque cada uma delas é uma citação que passou por duas
// ferramentas e por uma revisão sem ser alcançada.
func citationMapping() *Mapping {
	m := NewMapping()
	m.addIdentRow("internal/types", "field", "Snapshot.Epoca", "Epoch")
	m.addIdentRow("internal/types", "field", "Snapshot.LimiteRowID", "LimitRowID")
	m.addIdentRow("internal/types", "type", "Presenca", "Presence")
	m.addIdentRow("internal/types", "const", "PresencaAusente", "PresenceAbsent")
	m.addIdentRow("internal/storage/mmap", "method", "Region.LiberaBlocos", "ReleaseBlocks")
	m.addIdentRow("internal/gates", "var", "specsDoMarcador", "markerSpecs")
	m.addIdentRow("internal/query/budget", "field", "PorQuery.Teto", "Ceiling")
	m.addIdentRow("internal/kafka", "type", "Fila", "Queue")
	m.addIdentRow("internal/kafka", "method", "Fila.Empurra", "Push")
	m.addIdentRow("internal/kafka", "field", "Fila.tamanho", "size")
	return m
}

// TestBareLeafOfFieldResolvesByItsAddress cobre o índice por FOLHA: a prosa
// cita campo e método pelo nome nu, e antes disto só o endereço inteiro
// (`Snapshot.Epoca`) era alcançado — foi por essa lacuna que
// docs/architecture.md e os dois .puml ficaram descrevendo
// `Snapshot{Epoca, Instante, LimiteRowID}`.
func TestBareLeafOfFieldResolvesByItsAddress(t *testing.T) {
	out, rep := RewriteText([]byte("o campo `Epoca` e o método `Fila.Empurra`"), ".md", citationMapping())
	if got, want := string(out), "o campo `Epoch` e o método `Queue.Push`"; got != want {
		t.Fatalf("saída: %q\nquer:  %q", got, want)
	}
	if len(rep.Skipped) != 0 {
		t.Fatalf("pulou: %v", rep.Skipped)
	}
}

// TestAmbiguousLeafIsSkippedAndReported é a contraparte: folha que dois
// pacotes traduzem DIFERENTE não resolve, e sai no relatório em vez de virar
// um chute.
func TestAmbiguousLeafIsSkippedAndReported(t *testing.T) {
	m := citationMapping()
	m.addIdentRow("internal/storage", "field", "Segmento.Epoca", "Generation")
	out, rep := RewriteText([]byte("o campo `Epoca`"), ".md", m)
	if got, want := string(out), "o campo `Epoca`"; got != want {
		t.Fatalf("substituiu folha ambígua: %q", got)
	}
	if sk := rep.Skipped; len(sk) != 1 || sk[0].Token != "Epoca" || !strings.Contains(sk[0].Reason, "ambíguo") {
		t.Fatalf("relatório: %v", sk)
	}
}

// TestBareTokenWithInnerUpperIsReplacedAnywhere: `LimiteRowID` e
// `specsDoMarcador` não são palavra de prosa em nenhuma das duas línguas do
// repositório, e por isso são substituídos fora de crase também.
func TestBareTokenWithInnerUpperIsReplacedAnywhere(t *testing.T) {
	src := "o LimiteRowID da consulta e o specsDoMarcador do gate; LiberaBlocos falha."
	want := "o LimitRowID da consulta e o markerSpecs do gate; ReleaseBlocks falha."
	out, _ := RewriteText([]byte(src), ".md", citationMapping())
	if string(out) != want {
		t.Fatalf("saída: %q\nquer:  %q", out, want)
	}
}

// TestSingleCapBareTokenIsProseInMarkdown é a contraparte negativa da regra
// acima, e é a que impede a passada de transformar prosa PT-BR em citação:
// `Epoca`, `Teto` e `Presenca` começando frase são palavra, e fora de crase
// ficam onde estão. Token todo minúsculo, idem.
func TestSingleCapBareTokenIsProseInMarkdown(t *testing.T) {
	src := "Epoca é o relógio lógico. Teto e Presenca aparecem na frase; tamanho também."
	out, rep := RewriteText([]byte(src), ".md", citationMapping())
	if string(out) != src {
		t.Fatalf("tocou prosa: %q", out)
	}
	if len(rep.Applied) != 0 {
		t.Fatalf("aplicou: %v", rep.Applied)
	}
}

// TestSingleCapBareTokenOnlyInTheOwnPackageComment: em comentário Go o mesmo
// token é citação no pacote que declarava o nome e palavra em qualquer outro.
func TestSingleCapBareTokenOnlyInTheOwnPackageComment(t *testing.T) {
	src := "package x\n\n// Epoca é a época de publicação.\nfunc F() {}\n"
	own, _ := RewriteGoFile("internal/types/types.go", []byte(src), citationMapping())
	if got, want := string(own), "package x\n\n// Epoch é a época de publicação.\nfunc F() {}\n"; got != want {
		t.Errorf("no próprio pacote: %q\nquer: %q", got, want)
	}
	other, _ := RewriteGoFile("internal/storage/store.go", []byte(src), citationMapping())
	if string(other) != src {
		t.Errorf("em outro pacote a palavra foi tocada: %q", other)
	}
}

// TestPUMLReplacesSingleCapOnlyInsideTheBox: num diagrama a caixa lista os
// campos entre chaves, e é ali — e só ali — que `Epoca` é identificador.
func TestPUMLReplacesSingleCapOnlyInsideTheBox(t *testing.T) {
	src := "st --> q : Snapshot{Epoca, LimiteRowID}\nnote right of q\n  Epoca é o relógio lógico.\nend note\n"
	want := "st --> q : Snapshot{Epoch, LimitRowID}\nnote right of q\n  Epoca é o relógio lógico.\nend note\n"
	out, _ := RewriteText([]byte(src), ".puml", citationMapping())
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
}

// TestReceiverQualifiedLeafIsReplaced: `snap.Epoca` é a forma com que a prosa
// cita campo — o receptor é variável de quem escreveu a frase, e só a folha
// muda.
func TestReceiverQualifiedLeafIsReplaced(t *testing.T) {
	m := citationMapping()
	puml, _ := RewriteText([]byte("st -> jr : Em(rowId, snap.Epoca)\n"), ".puml", m)
	if got, want := string(puml), "st -> jr : Em(rowId, snap.Epoch)\n"; got != want {
		t.Errorf(".puml: %q, quer %q", got, want)
	}
	md, _ := RewriteText([]byte("a liveness usa snap.Epoca e o orc.Teto\n"), ".md", m)
	if got, want := string(md), "a liveness usa snap.Epoch e o orc.Ceiling\n"; got != want {
		t.Errorf(".md: %q, quer %q", got, want)
	}
	src := "package x\n\n// Em(rowId, snap.Epoca) devolve a versão retida.\nfunc F() {}\n"
	gof, _ := RewriteGoFile("internal/storage/versions.go", []byte(src), m)
	if got, want := string(gof), "package x\n\n// Em(rowId, snap.Epoch) devolve a versão retida.\nfunc F() {}\n"; got != want {
		t.Errorf("comentário Go: %q, quer %q", got, want)
	}
}

// TestKnownPackageQualifierIsNotAReceiver é a contraparte: com um nome de
// PACOTE conhecido à esquerda do ponto, a forma é `pkg.Ident` e a folha de
// membro não tem nada a dizer — `kafka.Empurra` não é `Fila.Empurra`.
func TestKnownPackageQualifierIsNotAReceiver(t *testing.T) {
	out, _ := RewriteText([]byte("ver kafka.Empurra na prosa\n"), ".md", citationMapping())
	if got, want := string(out), "ver kafka.Empurra na prosa\n"; got != want {
		t.Fatalf("saída: %q, quer %q", got, want)
	}
}

// TestReceiverWithLowercaseLeafIsNotACitation é a segunda contraparte, e vem
// de uma rodada de verdade: `c.entrada`, `b.campo`, `d.erro` e `pai.chave` são
// campo de struct de tabela de teste e chave de exemplo, e a folha minúscula
// casa com qualquer campo homônimo de qualquer pacote do lote. A forma de
// receptor só vale com folha EXPORTADA.
func TestReceiverWithLowercaseLeafIsNotACitation(t *testing.T) {
	m := citationMapping()
	m.addIdentRow("internal/typeparse", "field", "Layout.campo", "field")
	src := "o caso c.campo e o pai.campo ficam\n"
	out, rep := RewriteText([]byte(src), ".md", m)
	if string(out) != src {
		t.Fatalf("tocou o campo minúsculo: %q", out)
	}
	if len(rep.Applied) != 0 {
		t.Fatalf("aplicou: %v", rep.Applied)
	}
}

// TestCompoundBacktickRewritesEachToken: a regra da crase tentava o conteúdo
// INTEIRO como endereço, não achava nada e devolvia a crase intacta — e o
// repositório tem dezenas de `Snapshot{Epoca, Instante}` e
// `presenca != PresencaAusente`.
// O token minúsculo de dentro (`tamanho`, `presenca`) NÃO é tocado: numa crase
// composta ele é palavra do exemplo, e é assim que `--param <tipo>:<valor>` e
// `"campo": null` sobrevivem à passada.
func TestCompoundBacktickRewritesEachToken(t *testing.T) {
	src := "`Fila{Empurra, tamanho}` e `presenca != PresencaAusente` e `[snap.Epoca, snap.LimiteRowID)`"
	want := "`Queue{Push, tamanho}` e `presenca != PresenceAbsent` e `[snap.Epoch, snap.LimitRowID)`"
	out, _ := RewriteText([]byte(src), ".md", citationMapping())
	if string(out) != want {
		t.Fatalf("saída: %q\nquer:  %q", out, want)
	}
}

// TestCompoundBacktickKeepsTheSyntaxOfAnExample é a contraparte com as formas
// que a rodada de verdade destruiu antes da exceção do token minúsculo:
// sintaxe de CLI, exemplo de JSON e chamada de biblioteca do cliente.
func TestCompoundBacktickKeepsTheSyntaxOfAnExample(t *testing.T) {
	m := citationMapping()
	m.addIdentRow("internal/typeparse", "field", "Layout.tipo", "typ")
	m.addIdentRow("internal/typeparse", "field", "Layout.valor", "value")
	m.addIdentRow("internal/typeparse", "field", "Layout.campo", "field")
	src := "`--param <tipo>:<valor>` e `\"campo\": null` e `Arrays.fill(tipo, null)`"
	out, rep := RewriteText([]byte(src), ".md", m)
	if string(out) != src {
		t.Fatalf("tocou o exemplo: %q", out)
	}
	if len(rep.Applied) != 0 {
		t.Fatalf("aplicou: %v", rep.Applied)
	}
}

// TestCompoundBacktickLeavesWhatIsNotACitation: comando e trecho de código
// entre crases passam pelo tokenizador e saem inteiros.
func TestCompoundBacktickLeavesWhatIsNotACitation(t *testing.T) {
	src := "rode `go test ./... -run X` e `make lint`"
	out, rep := RewriteText([]byte(src), ".md", citationMapping())
	if string(out) != src {
		t.Fatalf("tocou o comando: %q", out)
	}
	if len(rep.Applied) != 0 {
		t.Fatalf("aplicou: %v", rep.Applied)
	}
}

// TestStringLiteralCitingIdentifierIsRewritten: mensagem de produção, rótulo
// de tabela de casos e texto de falha citam método e tipo, e depois do rename
// mandam o leitor procurar o que não existe.
func TestStringLiteralCitingIdentifierIsRewritten(t *testing.T) {
	src := "package x\n\nvar a = \"%w: LiberaBlocos em %s\"\nvar b = \"PorQuery.Teto\"\nvar c = \"Presenca(\"\n"
	want := "package x\n\nvar a = \"%w: ReleaseBlocks em %s\"\nvar b = \"PorQuery.Ceiling\"\nvar c = \"Presence(\"\n"
	out, rep := RewriteGoFile("internal/storage/mmap/region.go", []byte(src), citationMapping())
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
	for _, a := range rep.Applied {
		if a.Rule != "literal" {
			t.Errorf("regra %q, esperada literal: %+v", a.Rule, a)
		}
	}
}

// TestStringLiteralWithoutCitationIsUntouched é a contraparte, e cobre as três
// exclusões que protegem DADO e MENSAGEM: literal só com palavra minúscula
// (fixture, chave de YAML, nome de subteste), literal que tem uma palavra
// PT-BR começando maiúscula sem nenhuma forma de citação em volta (`Teto do
// processo` é mensagem, e mensagem é da Fase 3) e TAG de campo, que é nome de
// chave no disco e no wire.
func TestStringLiteralWithoutCitationIsUntouched(t *testing.T) {
	src := "package x\n\nvar a = \"fila cheia\"\nvar b = \"Teto do processo\"\n\ntype T struct {\n\tE int64 `json:\"LimiteRowID\"`\n}\n"
	out, rep := RewriteGoFile("internal/types/types.go", []byte(src), citationMapping())
	if string(out) != src {
		t.Fatalf("saída:\n%s\nquer o original:\n%s", out, src)
	}
	if len(rep.Applied) != 0 {
		t.Fatalf("aplicou: %v", rep.Applied)
	}
}
