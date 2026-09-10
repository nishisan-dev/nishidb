package materializer

import "testing"

// contadores devolve o trio da coluna derivada, que é como o `_meta.fields` o
// publica: ocorrências, divergências e coerções.
func contadores(t *testing.T, h *harness, nome string) (ocorr, diverg, coerc uint64) {
	t.Helper()
	f, ok := h.c.FieldByName([]byte(nome))
	if !ok {
		t.Fatalf("coluna %q não compilada", nome)
	}
	return f.OccurrenceCount(), f.DivergenceCount(), f.CoercionCount()
}

// A coerção é contada em `coercions` e NÃO em `typeMismatches`, e este é o teste
// que impede a mudança de cegar o diagnóstico que o contador por coluna existe
// para dar.
//
// O raciocínio: enquanto a coluna `string` RECUSAVA número, um valor divergente
// aparecia em typeMismatches e o operador via, pelo painel, que a origem estava
// inconsistente. Passando a converter, aquele contador vai a ZERO no mesmo
// cenário — a inconsistência some do painel sem ter sumido do dado. Sem um
// contador próprio, o efeito líquido da mudança seria trocar uma perda visível
// por uma inconsistência invisível.
//
// As duas asserções são igualmente necessárias: contar em coercions e ficar em
// zero em typeMismatches são afirmações separadas, e um conserto que somasse as
// duas num contador só passaria pela primeira.
func TestCoercaoEContadaSeparadaDaDivergencia(t *testing.T) {
	h := newHarness(t, descobreYAML)
	// Primeira observação fixa `doc.podName` como string.
	h.apply(`{"id":"A","doc":{"podName":"pod-1"}}`)
	if _, _, coerc := contadores(t, h, "doc.podName"); coerc != 0 {
		t.Fatalf("contador de coerção nasceu em %d", coerc)
	}

	// Número e boolean na coluna de texto: os dois convertem, e os dois são
	// COERÇÃO — o valor está lá, mas a origem escreve de duas formas.
	h.apply(`{"id":"B","doc":{"podName":7}}`)
	h.apply(`{"id":"C","doc":{"podName":true}}`)

	ocorr, diverg, coerc := contadores(t, h, "doc.podName")
	if coerc != 2 {
		t.Errorf("coercions = %d, quer 2 (o número e o boolean)", coerc)
	}
	if diverg != 0 {
		t.Errorf("typeMismatches = %d, quer 0: nada se perdeu, os dois valores entraram", diverg)
	}
	if ocorr != 3 {
		t.Errorf("occurrences = %d, quer 3", ocorr)
	}
	// E o valor está mesmo lá, com o texto-fonte.
	h.wantString(h.state("B"), "doc.podName", "7")
	h.wantString(h.state("C"), "doc.podName", "true")
}

// O valor que chega na forma NATIVA do tipo não conta coerção. Sem esta metade,
// um contador que incrementasse sempre passaria no teste acima e mediria
// ocorrência, não coerção.
func TestValorNaFormaNativaNaoContaCoercao(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"pod-1"}}`)
	h.apply(`{"id":"B","doc":{"podName":"pod-2"}}`)
	h.apply(`{"id":"C","doc":{"podName":"pod-3"}}`)
	ocorr, _, coerc := contadores(t, h, "doc.podName")
	if coerc != 0 {
		t.Errorf("coercions = %d, quer 0: as três vieram como string", coerc)
	}
	if ocorr != 3 {
		t.Errorf("occurrences = %d, quer 3", ocorr)
	}
}

// A string numérica numa coluna int64 SEMPRE foi coerção e nunca foi contada: o
// ajuste pós-amostra a aceitou por causa do Connect-Info da fonte real, e a
// partir dali o dataset convertia sem nada registrar que a origem manda número
// como texto. O contador novo fecha esse buraco junto com a direção nova.
//
// A coluna aqui é DECLARADA int64 no YAML, e não autodescoberta: é o caso em que
// o tipo foi escolhido por quem escreveu o schema, e portanto aquele em que a
// divergência de grafia da origem é mais fácil de passar despercebida.
func TestStringNumericaEmColunaInt64ContaCoercao(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"tally":7}}`)
	h.apply(`{"id":"B","doc":{"tally":"1000000000"}}`)

	ocorr, diverg, coerc := contadores(t, h, "tally")
	if coerc != 1 {
		t.Errorf("coercions = %d, quer 1 (a string numérica)", coerc)
	}
	if diverg != 0 {
		t.Errorf("typeMismatches = %d, quer 0", diverg)
	}
	if ocorr != 2 {
		t.Errorf("occurrences = %d, quer 2", ocorr)
	}
	h.wantInt(h.state("B"), "tally", 1000000000)
}

// A coerção também é contada na coluna DECLARADA `string`, e não só na
// autodescoberta: são dois sítios de chamada distintos no extract.go — a
// extração declarada e a descoberta — e um conserto que tocasse só um passaria
// pelos testes do outro.
func TestCoercaoContadaNaExtracaoDeclarada(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","doc":{"probeIpNode":"10.0.0.1"}}`)
	h.apply(`{"id":"B","doc":{"probeIpNode":42}}`)

	_, diverg, coerc := contadores(t, h, "probe_ip")
	if coerc != 1 {
		t.Errorf("coercions = %d, quer 1", coerc)
	}
	if diverg != 0 {
		t.Errorf("typeMismatches = %d, quer 0", diverg)
	}
	h.wantString(h.state("B"), "probe_ip", "42")
}

// Divergência e coerção são DISJUNTAS: o objeto não vira texto, e portanto conta
// no contador de divergência e não no de coerção.
//
// É o par que dá sentido aos dois números: um diz "o valor se perdeu, a coluna
// está incompleta", o outro diz "o valor está lá, mas a origem o escreve de duas
// formas". Somá-los apagaria a distinção que decide o que fazer.
func TestDivergenciaNaoContaComoCoercao(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"A","doc":{"podName":"pod-1"}}`)
	h.apply(`{"id":"B","doc":{"podName":{"a":1}}}`)
	h.apply(`{"id":"C","doc":{"podName":[1,2]}}`)

	ocorr, diverg, coerc := contadores(t, h, "doc.podName")
	if diverg != 2 {
		t.Errorf("typeMismatches = %d, quer 2 (o objeto e o array)", diverg)
	}
	if coerc != 0 {
		t.Errorf("coercions = %d, quer 0: nada foi convertido", coerc)
	}
	if ocorr != 3 {
		t.Errorf("occurrences = %d, quer 3", ocorr)
	}
}

// O null explícito não é nem uma coisa nem outra: não conta divergência (já era
// assim) e também não conta coerção. Ele é a origem dizendo que não há valor, e
// a coluna ficar ausente é a resposta certa — o contador de coerção não pode
// herdar o defeito que o de divergência já teve.
//
// A REMOÇÃO continua sendo emitida, que é a metade que importa para a corretude:
// sem ela, no modo merge a coluna seguiria mostrando o valor de um evento
// anterior enquanto o documento da mesma linha diz null.
func TestNullNaoContaCoercaoENemDivergencia(t *testing.T) {
	h := newHarness(t, descobreYAML)
	h.apply(`{"id":"MESMA","doc":{"podName":"pod-1"}}`)
	h.wantString(h.state("MESMA"), "doc.podName", "pod-1")

	h.apply(`{"id":"MESMA","doc":{"podName":null}}`)
	_, diverg, coerc := contadores(t, h, "doc.podName")
	if diverg != 0 || coerc != 0 {
		t.Errorf("typeMismatches=%d coercions=%d, quer os dois em 0", diverg, coerc)
	}
	h.wantAbsent(h.state("MESMA"), "doc.podName")
}

// O campo de TOPO não paga o contador, e é deliberado: os contadores por coluna
// são publicados só para as derivadas — no `_meta.fields` o topo sai com NULL —,
// e incrementar um número que ninguém lê seria custo no caminho mais quente do
// produto sem leitor do outro lado.
//
// O evento abaixo coerge um número num campo de topo declarado `string`; o teste
// afirma que nada foi contado ali.
func TestCoercaoNoTopoNaoEhContada(t *testing.T) {
	h := newHarness(t, extractYAML)
	h.apply(`{"id":"A","topo":"texto"}`)
	h.apply(`{"id":"B","topo":7}`)
	h.wantString(h.state("B"), "topo", "7")

	f, ok := h.c.FieldByName([]byte("topo"))
	if !ok {
		t.Fatal("campo `topo` não compilado")
	}
	if f.CoercionCount() != 0 || f.OccurrenceCount() != 0 {
		t.Errorf("campo de topo contou coercions=%d occurrences=%d, quer os dois em 0",
			f.CoercionCount(), f.OccurrenceCount())
	}
	// E a coluna É de topo, senão o teste afirmaria isso do campo errado.
	if f.Parent != nil {
		t.Errorf("`topo` tem Parent %v; o teste depende de ela ser de topo", f.Parent.Name)
	}
}
