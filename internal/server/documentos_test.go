package server_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/server"
)

func leJSON[T any](t *testing.T, srv *server.Servidor, caminho string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(pega(t, srv, caminho)), &v); err != nil {
		t.Fatalf("GET %s: %v", caminho, err)
	}
	return v
}

// O corpo do /v1/metadata é o que o DatabaseMetaData do §71 vai consumir:
// tipo, alias, índice declarado e os limites EFETIVOS.
func TestMetadataDescreveOQueODriverPrecisa(t *testing.T) {
	srv := servidorComDados(t, 3, blocoPadrao+"query:\n  defaultMaxRows: 777\n  defaultTimeout: 9s\n")

	m := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
	if len(m.Datasets) != 1 {
		t.Fatalf("datasets = %d, quer 1", len(m.Datasets))
	}
	d := m.Datasets[0]
	if d.Name != "d" || d.Key != "id" {
		t.Fatalf("dataset = %+v", d)
	}
	// Os limites são os EFETIVOS: o dataset não declara nenhum dos dois, então
	// vale o default global — que é o que vai valer na consulta.
	if d.MaxRows != 777 || d.TimeoutMs != 9000 {
		t.Fatalf("limites = %d linhas, %d ms", d.MaxRows, d.TimeoutMs)
	}

	porNome := map[string]protocolo.ColunaMeta{}
	for _, c := range d.Columns {
		porNome[c.Name] = c
	}
	casos := []struct {
		nome   string
		tipo   string
		jdbc   string
		indice string
		alias  string
	}{
		{"id", "string", protocolo.JDBCVarchar, "none", ""},
		{"status", "string", protocolo.JDBCVarchar, "bitmap", "estado"},
		{"porta", "int64", protocolo.JDBCBigint, "none", ""},
		{"ip", "ipv4", protocolo.JDBCVarchar, "none", ""},
	}
	for _, c := range casos {
		got, ok := porNome[c.nome]
		if !ok {
			t.Fatalf("coluna %q ausente do metadata", c.nome)
		}
		if got.Type != c.tipo || got.JDBC != c.jdbc || got.Index != c.indice {
			t.Fatalf("coluna %q = %+v, quer tipo %s / %s / índice %s", c.nome, got, c.tipo, c.jdbc, c.indice)
		}
		switch {
		case c.alias == "" && got.Alias != nil:
			t.Fatalf("coluna %q tem alias %q, quer nenhum", c.nome, *got.Alias)
		case c.alias != "" && (got.Alias == nil || *got.Alias != c.alias):
			t.Fatalf("coluna %q com alias %v, quer %q", c.nome, got.Alias, c.alias)
		}
	}
}

// O filtro devolve a MESMA forma, com um elemento: o cliente não aprende dois
// parsers para as duas perguntas.
func TestMetadataFiltraPorDataset(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)

	m := leJSON[protocolo.Metadados](t, srv, "/v1/metadata?dataset=d")
	if len(m.Datasets) != 1 || m.Datasets[0].Name != "d" {
		t.Fatalf("metadata = %+v", m)
	}

	resp, err := http.Get("http://" + srv.Endereco() + "/v1/metadata?dataset=fantasma")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, quer 404", resp.StatusCode)
	}
	if f := falhaDe(t, resp); f.Code != protocolo.CodDatasetDesconhecido {
		t.Fatalf("código = %q", f.Code)
	}
}

func TestRotasDeLeituraRecusamMetodoErrado(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)

	for _, caminho := range []string{"/v1/metadata", "/v1/stats"} {
		resp, err := http.Post("http://"+srv.Endereco()+caminho, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s = %d, quer 405", caminho, resp.StatusCode)
		}
		if f := falhaDe(t, resp); f.Code != protocolo.CodMetodo {
			t.Fatalf("POST %s: código = %q", caminho, f.Code)
		}
	}
}

// O /v1/stats e o /metrics precisam concordar: os dois leem a MESMA fonte no
// instante da chamada, e é essa a garantia que impede um painel de discordar do
// JSON sem nada estar quebrado.
func TestStatsConcordaComOMetrics(t *testing.T) {
	srv := servidorComDados(t, 42, blocoPadrao)

	// Uma consulta que dá certo e uma que falha, para haver o que contar.
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	if _, l := resultado(t, resp); l.Fim() == nil {
		t.Fatal("a consulta de aquecimento não fechou bem")
	}
	consulta(t, srv, `{"dataset":"d","sql":"SELECT nao_existe FROM d"}`).Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste

	e := leJSON[protocolo.Estatisticas](t, srv, "/v1/stats")
	if len(e.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(e.Datasets))
	}
	ds := e.Datasets[0]

	if ds.Storage.Rows != 42 {
		t.Fatalf("rows = %d, quer 42", ds.Storage.Rows)
	}
	if got := valorDaMetrica(t, srv, "nishidb_materialized_rows"); got != 42 {
		t.Fatalf("nishidb_materialized_rows = %v, quer 42 — o JSON e o /metrics divergiram", got)
	}
	if ds.Query.Total != uint64(valorDaMetrica(t, srv, "nishidb_query_total")) {
		t.Fatalf("total = %d contra %v no /metrics", ds.Query.Total, valorDaMetrica(t, srv, "nishidb_query_total"))
	}
	if ds.Query.Errors != uint64(valorDaMetrica(t, srv, "nishidb_query_errors")) {
		t.Fatalf("errors = %d contra %v no /metrics", ds.Query.Errors, valorDaMetrica(t, srv, "nishidb_query_errors"))
	}
	if ds.Query.Errors != 1 {
		t.Fatalf("errors = %d, quer 1", ds.Query.Errors)
	}

	// Os contadores do motor são do PROCESSO: repeti-los por dataset publicaria
	// o mesmo número várias vezes.
	if e.Process.Query.Emitted < 42 {
		t.Fatalf("emitted = %d, quer ao menos 42", e.Process.Query.Emitted)
	}
	if e.Process.Query.FullScans != int64(valorDaMetrica(t, srv, "nishidb_query_full_scans")) {
		t.Fatal("fullScans divergiu entre o JSON e o /metrics")
	}

	// Sem Kafka declarado, a ingestão é DISABLED — o mesmo vocabulário do §94,
	// para as duas rotas não descreverem a mesma realidade com palavras
	// diferentes.
	if ds.Ingestion.State != server.EstadoDesligado || ds.Ingestion.Enabled {
		t.Fatalf("ingestão = %+v", ds.Ingestion)
	}
	if h := leHealth(t, srv); h.Datasets["d"] != ds.Ingestion.State {
		t.Fatalf("health diz %q e stats diz %q", h.Datasets["d"], ds.Ingestion.State)
	}
}

// As cinco séries de consulta do §92 existem, com o tipo certo e alimentadas.
// Antes desta fase elas eram nome em nomes.go e nada mais.
func TestAsCincoSeriesDeQueryDoSpec92SaoAlimentadas(t *testing.T) {
	srv := servidorComDados(t, 5, blocoPadrao)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	resultado(t, resp)
	consulta(t, srv, `{"dataset":"d","sql":"SELECT nao_existe FROM d"}`).Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste

	scrape := pega(t, srv, "/metrics")
	for _, nome := range []string{
		"nishidb_query_total",
		"nishidb_query_active",
		"nishidb_query_errors",
		"nishidb_query_full_scans",
		"nishidb_query_latency_seconds",
	} {
		if !strings.Contains(scrape, "# TYPE "+nome+" ") {
			t.Fatalf("a série %q não está no /metrics", nome)
		}
	}

	if got := valorDaMetrica(t, srv, "nishidb_query_total"); got != 2 {
		t.Fatalf("query_total = %v, quer 2 (a recusada também foi admitida)", got)
	}
	esperaMetrica(t, srv, "nishidb_query_active", 0)
	if got := valorDaMetrica(t, srv, "nishidb_query_latency_seconds_count"); got != 2 {
		t.Fatalf("latency_count = %v, quer 2", got)
	}
	// O full scan é a métrica normativa do §64: a query sem WHERE escolheu um.
	if got := valorDaMetrica(t, srv, "nishidb_query_full_scans"); got < 1 {
		t.Fatalf("full_scans = %v, quer ao menos 1", got)
	}
	// O erro é contado com a CATEGORIA, que é o que diz se o problema é do
	// cliente ou do dataset.
	if !strings.Contains(scrape, `nishidb_query_errors{dataset="d",categoria="BIND"}`) {
		t.Fatalf("o erro não foi categorizado no scrape:\n%s", scrape)
	}
}

// O nome do dataset vindo do request NUNCA vira label: senão um cliente em
// loop com nomes aleatórios cria uma série por requisição até o processo
// morrer de memória.
func TestDatasetInexistenteNaoCriaSerie(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)

	for _, nome := range []string{"a1", "a2", "a3"} {
		consulta(t, srv, `{"dataset":"`+nome+`","sql":"SELECT id FROM x"}`).Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste
	}
	scrape := pega(t, srv, "/metrics")
	for _, nome := range []string{"a1", "a2", "a3"} {
		if strings.Contains(scrape, `dataset="`+nome+`"`) {
			t.Fatalf("o nome %q vindo do request virou label:\n%s", nome, scrape)
		}
	}
	if !strings.Contains(scrape, `dataset="?"`) {
		t.Fatalf("os erros sem dataset resolvido sumiram do scrape:\n%s", scrape)
	}
}

// TestStatsPublicaODiscoEACompactionSemSegundaFonte fecha a lacuna que o §96
// abriu: o `nishidb status` resume STORAGE, e até esta tarefa o /v1/stats
// respondia rows e segmentos mas não o disco — o operador via a curva no
// /metrics e não tinha como perguntá-la pelo comando.
//
// O oráculo é duplo de propósito. Contra o /metrics, porque as duas saídas
// leem o MESMO Store.Stats() e divergir é o defeito clássico de contador
// duplicado. E contra o FILESYSTEM, porque comparar só com a métrica
// confirmaria apenas que o número foi copiado de um campo para outro — é o
// mesmo argumento que o godoc de servidorComDadosEDiretorio registra, e é o
// que mata o mutante de publicar o tally de liveness sob o nome do disco.
func TestStatsPublicaODiscoEACompactionSemSegundaFonte(t *testing.T) {
	srv, dir := servidorComDadosEDiretorio(t, 100, blocoPadrao)

	// A subida põe o HTTP no ar ANTES de ligar a compaction (§51), então há
	// uma janela em que a rota já responde e o laço ainda não subiu. Esperar
	// pela CONDIÇÃO fecha a janela sem dormir — e o que se espera é o default
	// do §78, que traz a chave LIGADA: um YAML herdado do M1, sem o bloco
	// storage.compaction, tem de compactar.
	esperaCompactionNoAr(t, srv)

	e := leJSON[protocolo.Estatisticas](t, srv, "/v1/stats")
	if len(e.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(e.Datasets))
	}
	st := e.Datasets[0].Storage

	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob dos segmentos: %v", err)
	}
	var emDisco int64
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		emDisco += fi.Size()
	}
	if emDisco == 0 {
		t.Fatal("nenhum segmento em disco: o teste não estaria provando nada")
	}
	if st.Bytes != emDisco {
		t.Errorf("storage.bytes = %d e os arquivos somam %d: o JSON não descreve o disco", st.Bytes, emDisco)
	}
	if got := int64(valorDaMetrica(t, srv, metrics.MetStorageBytes)); st.Bytes != got {
		t.Errorf("storage.bytes = %d no JSON e %d no /metrics: duas fontes para o mesmo número", st.Bytes, got)
	}
	if got := uint64(valorDaMetrica(t, srv, metrics.MetStorageGarbageBytes)); st.GarbageBytes != got {
		t.Errorf("garbageBytes = %d no JSON e %d no /metrics", st.GarbageBytes, got)
	}

	// A compaction sai SEMPRE, inclusive antes da primeira passada — pelo
	// mesmo motivo das séries do §92: zero é uma resposta, ausência não é.
	if !st.Compaction.Running {
		t.Error("compaction.running = false com o default do §78, que traz a chave ligada")
	}
	if got := valorDaMetrica(t, srv, metrics.MetCompactionRunning); got != 1 {
		t.Errorf("compaction_running = %v no /metrics contra true no JSON", got)
	}
	if got := int64(valorDaMetrica(t, srv, metrics.MetCompactionBytesRead)); st.Compaction.BytesRead != got {
		t.Errorf("compaction.bytesRead = %d no JSON e %d no /metrics", st.Compaction.BytesRead, got)
	}
	if got := int64(valorDaMetrica(t, srv, metrics.MetCompactionBytesWritten)); st.Compaction.BytesWritten != got {
		t.Errorf("compaction.bytesWritten = %d no JSON e %d no /metrics", st.Compaction.BytesWritten, got)
	}
}

// esperaCompactionNoAr bloqueia até o /v1/stats reportar o laço de pé.
//
// Espera por condição, e não por sleep: dormir esconderia tanto a lentidão
// quanto a ausência de progresso — é a mesma regra do esperaHealth.
func esperaCompactionNoAr(t *testing.T, srv *server.Servidor) {
	t.Helper()
	prazo := time.Now().Add(15 * time.Second)
	for time.Now().Before(prazo) {
		e := leJSON[protocolo.Estatisticas](t, srv, "/v1/stats")
		if len(e.Datasets) > 0 && e.Datasets[0].Storage.Compaction.Running {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a compaction não subiu em 15s: o default do §78 traz a chave ligada")
}
