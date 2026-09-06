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
