package query_test

import (
	"strings"
	"testing"
)

// TestOFromEnsinaACitarCadaParteQuandoONomeQualificadoVemInteiroEntreAspas
// fixa as QUATRO formas de escrever `_meta.counters` no FROM.
//
// As três primeiras abrem a tabela: as aspas do SQL padrão envolvem UM
// identificador, então citar o namespace, a tabela ou nenhum dos dois dá o
// mesmo par (namespace, tabela). A quarta — o nome qualificado citado INTEIRO —
// recusa, e está certa em recusar: `"_meta.counters"` nomeia uma tabela cujo
// nome contém um ponto. O que se cobra aqui é a mensagem: ela tem de dizer que
// as aspas engoliram o ponto e mostrar a forma certa, em vez de mandar o
// operador procurar um dataset que ele nunca quis abrir.
func TestOFromEnsinaACitarCadaParteQuandoONomeQualificadoVemInteiroEntreAspas(t *testing.T) {
	h := newHarness(t)
	tableVirtualOfTest(t, h.engine, dataset, "_meta", "counters")

	for _, sql := range []string{
		`SELECT groupKey FROM _meta.counters`,
		`SELECT groupKey FROM "_meta".counters`,
		`SELECT groupKey FROM "_meta"."counters"`,
	} {
		if rows := h.consultaOK(sql); len(rows) == 0 {
			t.Errorf("%s não devolveu linha nenhuma", sql)
		}
	}

	_, err := h.consulta(`SELECT groupKey FROM "_meta.counters"`)
	if err == nil {
		t.Fatal(`SELECT ... FROM "_meta.counters" abriu a tabela; o nome citado inteiro é UM identificador e tem de ser recusado`)
	}
	msg := err.Error()
	// O diagnóstico antigo continua lá — ele diz o que o servidor procurou.
	if !strings.Contains(msg, `dataset "_meta.counters" desconhecido`) {
		t.Errorf("a recusa perdeu o nome procurado: %q", msg)
	}
	// E a correção, que é o que faltava.
	if !strings.Contains(msg, "citado INTEIRO") {
		t.Errorf("a recusa não diz que o nome foi citado inteiro: %q", msg)
	}
	if !strings.Contains(msg, `"_meta"."counters"`) {
		t.Errorf("a recusa não mostra a forma certa: %q", msg)
	}
}

// TestOFromNaoFalaDeAspasQuandoONomeDesconhecidoNaoTemPonto: o complemento é
// dirigido a um erro específico. Nome simples que não existe recebe a mensagem
// de sempre — inventar aspas ali mandaria o operador corrigir o que não está
// errado.
func TestOFromNaoFalaDeAspasQuandoONomeDesconhecidoNaoTemPonto(t *testing.T) {
	h := newHarness(t)
	for _, sql := range []string{
		`SELECT groupKey FROM outro`,
		`SELECT groupKey FROM "outro"`,
	} {
		_, err := h.consulta(sql)
		if err == nil {
			t.Fatalf("%s: dataset inexistente tem de ser recusado", sql)
		}
		if strings.Contains(err.Error(), "citado INTEIRO") {
			t.Errorf("%s: a recusa fala de aspas sem ponto no nome: %q", sql, err)
		}
	}
}
