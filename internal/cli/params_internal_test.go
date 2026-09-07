package cli

import (
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// Os quatro tipos de wire viram o "value" certo, e o int64/double vão
// VERBATIM.
//
// Verbatim não é detalhe: é o que faz a validação de overflow acontecer no
// protocolo, sobre o texto original. Convertê-los aqui por strconv criaria uma
// segunda gramática de número no produto, e um `9223372036854775808` que
// passasse por float64 chegaria ao servidor arredondado e ACEITO.
func TestParamMontaOValueDeWireDeCadaTipo(t *testing.T) {
	casos := []struct{ arg, tipo, value string }{
		{"string:Stop", protocol.WireTypeString, `"Stop"`},
		{"int64:42", protocol.WireTypeInt64, "42"},
		{"double:1.5", protocol.WireTypeDouble, "1.5"},
		{"boolean:true", protocol.WireTypeBoolean, "true"},
		// json.Marshal e não aspas na mão: aqui é onde " e \ viram escapes.
		{`string:a"b\c`, protocol.WireTypeString, `"a\"b\\c"`},
	}
	for _, c := range casos {
		t.Run(c.arg, func(t *testing.T) {
			var l listaDeParametros
			if err := l.Set(c.arg); err != nil {
				t.Fatalf("Set(%q): %v", c.arg, err)
			}
			if len(l) != 1 || l[0].Type != c.tipo || string(l[0].Value) != c.value {
				t.Fatalf("param = %+v, quer {%s %s}", l[0], c.tipo, c.value)
			}
		})
	}
}

// O corte é no PRIMEIRO ':', e o resto é valor.
//
// Dois-pontos é comum demais em valor real — endereço com porta, hora, URL —
// para que a flag o trate como separador em toda ocorrência.
func TestParamStringComDoisPontosNoValorNaoEhCortada(t *testing.T) {
	var l listaDeParametros
	if err := l.Set("string:http://x:9142/v1"); err != nil {
		t.Fatal(err)
	}
	if got := string(l[0].Value); got != `"http://x:9142/v1"` {
		t.Errorf("value = %s, quer a URL inteira", got)
	}
}

// A ordem da linha de comando é a ordem posicional dos ?, e nada no caminho
// pode virar map.
func TestParamRepetidoPreservaAOrdemPosicional(t *testing.T) {
	var l listaDeParametros
	for _, arg := range []string{"string:primeiro", "int64:2", "string:terceiro"} {
		if err := l.Set(arg); err != nil {
			t.Fatal(err)
		}
	}
	quer := []string{`"primeiro"`, "2", `"terceiro"`}
	if len(l) != len(quer) {
		t.Fatalf("params = %d, quer %d", len(l), len(quer))
	}
	for i := range quer {
		if string(l[i].Value) != quer[i] {
			t.Errorf("param %d = %s, quer %s", i, l[i].Value, quer[i])
		}
	}
}

func TestParamSemDoisPontosEhErroDeUsoQueNomeiaOsTipos(t *testing.T) {
	var l listaDeParametros
	err := l.Set("Stop")
	if err == nil {
		t.Fatal("quer erro: falta o <tipo>:")
	}
	for _, tipo := range []string{protocol.WireTypeString, protocol.WireTypeInt64, protocol.WireTypeDouble, protocol.WireTypeBoolean} {
		if !strings.Contains(err.Error(), tipo) {
			t.Errorf("a mensagem não nomeia %q: %v", tipo, err)
		}
	}
}

func TestParamComTipoForaDosQuatroNomeiaOsAceitos(t *testing.T) {
	var l listaDeParametros
	err := l.Set("timestamp:2026-08-23T00:00:00Z")
	if err == nil {
		t.Fatal("quer erro: timestamp não é tipo de WIRE")
	}
	if !strings.Contains(err.Error(), protocol.WireTypeString) {
		t.Errorf("a mensagem não ensina o caminho (string): %v", err)
	}
}

// O pacote flag chama String() no valor ZERO para montar o texto do --help,
// antes de qualquer Set. Um String que desreferenciasse panicaria em
// `nishidb query --help`, que é a primeira coisa que alguém faz ao descobrir
// a flag.
func TestParamStringNoValorZeroNaoPanica(t *testing.T) {
	var l listaDeParametros
	if got := l.String(); got != "" {
		t.Errorf("String() no zero = %q, quer vazio", got)
	}
}
