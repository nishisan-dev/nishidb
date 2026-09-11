package server_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// specWithText é um dataset com uma coluna de texto LARGA e sem índice.
//
// Sem índice de propósito: o campo bitmap tem teto de 65535 bytes por valor
// (values.dict), então uma linha patológica construída sobre ele seria recusada
// pela INGESTÃO e o teste mediria outra coisa. O que se quer exercer é o
// orçamento de BUFFER DE SAÍDA, que é do caminho de consulta.
const specWithText = `version: 1
name: %s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    texto:
      type: string
`

// O orçamento de buffer chega ao CLIENTE como código nomeado, e não como
// INTERNAL nem como crescimento silencioso.
//
// É o débito 28 fim a fim. Com um maxBufferPerQuery apertado e uma linha de
// texto larga, a linha não cabe: o servidor já mandou o schema (o status é 200 e
// irreversível), então o desfecho sai na LINHA DE FECHO, com ROW_TOO_LARGE. As
// linhas que couberam continuam válidas e contadas — o cliente recebe um
// resultado parcial que se anuncia como parcial, que é a razão de a moldura
// NDJSON existir.
func TestLinhaAcimaDoOrcamentoSaiComoRowTooLargeNoFecho(t *testing.T) {
	a := newEnvWithStorage(t,
		blockPadrao+"runtime:\n  memoryTarget: 8MiB\n  query:\n    maxBufferPerQuery: 8KiB\n", "")
	a.writeDataset("d", strings.ReplaceAll(specWithText, "%s", "d"))
	seedWithTextLargo(t, a, "d")

	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id, texto FROM d"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200: o schema sai antes de a linha grande aparecer", resp.StatusCode)
	}
	n, codigo := desfechoOfStream(t, resp)
	if codigo != protocol.CodeRowTooLarge {
		t.Fatalf("fecho = %q com %d rows, quer %q", codigo, n, protocol.CodeRowTooLarge)
	}
	if n == 0 {
		t.Error("nenhuma linha saiu antes da recusa: o teste não distingue a recusa de uma consulta vazia")
	}

	// E a recusa alimenta o contador do PROCESSO, que é o número pelo qual o
	// operador decide subir a chave.
	if v := valueOfMetrica(t, srv, "nishidb_query_budget_rejected_total"); v < 1 {
		t.Errorf("nishidb_query_budget_rejected_total = %v, quer >= 1", v)
	}
}

// A MESMA consulta passa quando o orçamento comporta a linha.
//
// É o contrapositivo, e sem ele o teste acima não separa "o orçamento recusou"
// de "esta consulta não funciona": um erro qualquer no caminho produziria o
// mesmo vermelho. Só o par prova que quem decidiu foi a CHAVE.
func TestAMesmaLinhaPassaComOrcamentoQueAComporta(t *testing.T) {
	a := newEnvWithStorage(t,
		blockPadrao+"runtime:\n  memoryTarget: 8MiB\n  query:\n    maxBufferPerQuery: 1MiB\n", "")
	a.writeDataset("d", strings.ReplaceAll(specWithText, "%s", "d"))
	seedWithTextLargo(t, a, "d")

	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id, texto FROM d"}`)
	n, codigo := desfechoOfStream(t, resp)
	if codigo != "" {
		t.Fatalf("fecho = %q com 1MiB de orçamento: a linha de 64KiB cabe", codigo)
	}
	if n != 6 {
		t.Fatalf("rows = %d, quer 6 (5 pequenas + a grande)", n)
	}
}

// seedWithTextLargo materializa poucas linhas pequenas e UMA enorme.
//
// A ordem importa: as pequenas primeiro, para que o teste possa afirmar que o
// cliente recebeu linhas VÁLIDAS antes da recusa. Uma linha grande na primeira
// posição faria "0 rows + erro" — indistinguível de uma consulta que falhou na
// abertura.
func seedWithTextLargo(t *testing.T, a *env, name string) {
	t.Helper()
	cfg := a.load()
	spec, err := schema.Load(cfg.DatasetSpecPath(name), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{
		Dir:         cfg.DatasetDir(name),
		Dataset:     name,
		SegmentSize: cfg.SegmentSize(),
	})
	if err != nil {
		t.Fatal(err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	mat, err := materializer.New(compilado, st)
	if err != nil {
		t.Fatal(err)
	}
	aplica := func(ev string) {
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			_ = st.Close()
			t.Fatalf("evento: %v", r.Err)
		}
	}
	for i := 0; i < 5; i++ {
		aplica(fmt.Sprintf(`{"id":"S-%06d","texto":"curto"}`, i))
	}
	// A linha patológica: 64 KiB de texto num orçamento de 8 KiB.
	aplica(fmt.Sprintf(`{"id":"S-999999","texto":%q}`, strings.Repeat("x", 64<<10)))

	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// O orçamento VOLTA ao fim de cada consulta.
//
// Sem o Libera no defer, o memoryTarget se esgotaria monotonicamente: a
// enésima consulta de um servidor saudável começaria a receber ROW_TOO_LARGE
// num processo que não está usando memória nenhuma. É o modo de falha mais
// cruel possível, porque o sintoma aparece horas depois da causa e some no
// restart.
func TestOrcamentoVoltaAoFimDeCadaConsulta(t *testing.T) {
	// Orçamento que comporta DUAS consultas simultâneas de 64 KiB, mas não
	// vinte acumuladas.
	srv := serverWithBlockOfQuery(t, 20,
		blockPadrao+"runtime:\n  memoryTarget: 256KiB\n  query:\n    maxBufferPerQuery: 64KiB\n",
		"")

	for i := 0; i < 20; i++ {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("consulta %d: status = %d", i, resp.StatusCode)
		}
		if n, codigo := desfechoOfStream(t, resp); codigo != "" || n != 20 {
			t.Fatalf("consulta %d: %d rows, fecho %q — o orçamento não foi devolvido", i, n, codigo)
		}
	}

	// E o medidor volta a zero: um número que só sobe é a mesma falha vista do
	// lado do painel.
	//
	// ESPERA, em vez de fotografar uma vez: a devolução acontece no defer do
	// handler, DEPOIS do último byte do stream, e o cliente pode ler o fecho
	// antes de ela rodar. Uma leitura única viu 64 KiB em voo no CI sob -race
	// (05/09/2026) num servidor que devolvia tudo — falso vermelho numa
	// corrida do próprio teste, não do produto. O que o teste afirma é que o
	// medidor VOLTA a zero, não que volta antes de o cliente enxergar o fecho.
	var ultimo float64
	waitUntil(t, func() bool {
		ultimo = valueOfMetrica(t, srv, "nishidb_query_buffer_bytes")
		return ultimo == 0
	}, "nishidb_query_buffer_bytes não voltou a zero sem consulta em voo")
	if ultimo != 0 {
		t.Errorf("nishidb_query_buffer_bytes = %v sem consulta em voo, quer 0", ultimo)
	}
}
