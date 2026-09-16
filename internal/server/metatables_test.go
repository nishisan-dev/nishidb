package server_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/server"
)

// idsOfMetaTables roda `SELECT id FROM _meta.tables ORDER BY id` pela rota de
// consulta do servidor e devolve os endereços juntos, na ordem em que saíram.
//
// A comparação é da LISTA INTEIRA, e não de um `Contains`: o que este teste
// cobre é a fiação, e uma asserção que só procurasse `_meta.tables` dentro da
// saída passaria com o catálogo errado ao redor dela.
func idsOfMetaTables(t *testing.T, srv *server.Server, dataset string) string {
	t.Helper()
	resp := consulta(t, srv, `{"dataset":"`+dataset+`","sql":"SELECT id FROM _meta.tables ORDER BY id"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("a consulta foi recusada (%d): %s", resp.StatusCode, failOf(t, resp).Message)
	}
	rows, reader := result(t, resp)
	if reader.End() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		var l map[string]any
		if err := json.Unmarshal([]byte(row), &l); err != nil {
			t.Fatalf("linha ilegível: %v", err)
		}
		id, ok := l["id"].(string)
		if !ok {
			t.Fatalf("linha sem id textual: %s", row)
		}
		ids = append(ids, id)
	}
	return strings.Join(ids, ",")
}

// TestMetaTablesRespondePelaRotaDeConsultaDoServidor: a tabela só existe no
// processo que responde `/v1/query` se o servidor a registrar, e é o servidor
// quem atende todo cliente SQL do produto. Sem esta asserção, apagar o
// RegisterMetaTables de server.go não derruba teste nenhum — os testes do
// pacote query montam o motor por conta própria e não passam por esta fiação.
//
// O catálogo do fixture é inteiro: a base, a `_meta.fields` que o motor
// acrescenta à parte, as duas tabelas de meta que a abertura do dataset
// registra mesmo sem contador e sem Kafka declarados, e a view `alarms`, que
// entra sob `_counters` mesmo sem alarme declarado.
func TestMetaTablesRespondePelaRotaDeConsultaDoServidor(t *testing.T) {
	srv := serverWithData(t, 4, blockPadrao)
	const want = "_counters.alarms,_meta.counter_metrics,_meta.counters,_meta.fields,_meta.kafka,_meta.tables,d"
	if got := idsOfMetaTables(t, srv, "d"); got != want {
		t.Fatalf("catálogo = %s, quero %s", got, want)
	}
}

// TestMetaTablesRespondeNaTabelaIrmaAbertaPeloServidor: a irmã histórica é um
// dataset consultável por direito e recebe a tabela por uma fiação PRÓPRIA
// (openHistory), não pela do dataset base — apagar uma das duas chamadas deixa
// a outra de pé.
//
// A lista da irmã é mais curta que a do base, e é assim HOJE: pelo servidor ela
// não recebe `_meta.counters`, `_meta.counter_metrics` nem `_meta.kafka`, e
// pelo `nishidb query --config` recebe as duas dos contadores. A assimetria é
// da fiação, está
// registrada no débito 117, e é este teste que muda no dia em que ela for
// resolvida.
func TestMetaTablesRespondeNaTabelaIrmaAbertaPeloServidor(t *testing.T) {
	srv := serverWithHistory(t, specWithHistory)
	const want = "_meta.fields,_meta.tables,eventos_history"
	if got := idsOfMetaTables(t, srv, "eventos_history"); got != want {
		t.Fatalf("catálogo da irmã = %s, quero %s", got, want)
	}
}
