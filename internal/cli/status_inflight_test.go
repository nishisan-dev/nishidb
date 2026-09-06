package cli_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// statusContra roda `nishidb status` contra um /v1/stats de mentira e devolve a
// saída.
//
// O servidor de mentira é o molde de TestStatusSemUserNuncaEnviaHeaderAuthorization,
// e existe pela mesma razão aqui: a consulta em voo que o comando precisa
// mostrar é, por definição, uma que AINDA NÃO TERMINOU, e reproduzi-la contra um
// servidor de verdade obrigaria o teste a segurar um stream aberto enquanto roda
// um subcomando — um acoplamento temporal que só produziria intermitência. O que
// se afere aqui é a VISTA: dado este corpo, o comando mostra estes campos.
func statusContra(t *testing.T, e protocol.Estatisticas) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", protocol.ContentTypeJSON)
		if err := json.NewEncoder(w).Encode(e); err != nil {
			t.Errorf("escrevendo a resposta de mentira: %v", err)
		}
	}))
	defer srv.Close()

	a := novoAmbiente(t)
	if code := a.roda("status", "--server", srv.URL); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	return a.stdout.String()
}

// O `nishidb status` mostra a consulta em voo, com dataset, idade e a marca de
// longa.
//
// Até aqui a única coisa que ele dizia sobre consulta era
// `consultas: N (M ativas, K com erro)` — a mesma frase para três point lookups
// de 2 ms e para um full scan de oito minutos. Com o prazo longo virando
// cidadão de primeira classe, a segunda deixou de ser exceção, e o operador que
// vê "3 ativas" num servidor que não responde precisa saber se são três
// consultas rápidas passando ou uma varredura presa há oito minutos.
func TestStatusMostraAConsultaEmVooComIdadeEMarcaDeLonga(t *testing.T) {
	saida := statusContra(t, protocol.Estatisticas{
		Process: protocol.ProcessoStats{
			InFlight: []protocol.ConsultaEmVoo{
				{Dataset: "radius_sessions", SQLHash: "aaaaaaaaaaaa", RunningMs: 483000, Long: true},
				{Dataset: "cdr", SQLHash: "bbbbbbbbbbbb", RunningMs: 12, Long: false},
			},
		},
	})

	cabecalho, linhas := tabelaDaSaida(t, saida, "DATASET")
	if len(linhas) != 2 {
		t.Fatalf("a tabela saiu com %d linhas, quer 2:\n%s", len(linhas), saida)
	}

	// A ORDEM é a que o servidor mandou — da mais antiga para a mais nova. É a
	// linha que interessa, e ela tem de ser a primeira: uma lista reordenada
	// por dataset faria o operador procurar a consulta presa em vez de vê-la.
	if linhas[0][0] != "radius_sessions" {
		t.Errorf("a primeira linha é %q, quer a consulta MAIS ANTIGA (radius_sessions): %v", linhas[0][0], linhas)
	}

	longa := linhaDaTabela(t, linhas, "radius_sessions")
	conferePorCelula(t, "consulta longa em voo", cabecalho, longa, map[string]string{
		"DATASET": "radius_sessions",
		"SQL":     "aaaaaaaaaaaa",
		"HÁ":      "8m3s",
		"LONGA":   "sim",
	})

	curta := linhaDaTabela(t, linhas, "cdr")
	conferePorCelula(t, "consulta curta em voo", cabecalho, curta, map[string]string{
		"DATASET": "cdr",
		"SQL":     "bbbbbbbbbbbb",
		"HÁ":      "12ms",
		"LONGA":   "-",
	})
}

// Sem consulta em voo o bloco NÃO aparece.
//
// Não é economia de bytes: um cabeçalho "consultas em voo (0)" em toda chamada
// de `nishidb status` treina o operador a pular a seção, e ele a pularia
// justamente no dia em que ela tivesse conteúdo. É a mesma doutrina do aviso de
// chave sem implementação, que só sai quando a chave está no arquivo.
func TestStatusNaoImprimeBlocoDeConsultaEmVooQuandoNaoHaNenhuma(t *testing.T) {
	saida := statusContra(t, protocol.Estatisticas{})

	for _, marca := range []string{"consultas em voo", "LONGA"} {
		if strings.Contains(saida, marca) {
			t.Errorf("a saída traz %q sem nenhuma consulta em voo:\n%s", marca, saida)
		}
	}
}
