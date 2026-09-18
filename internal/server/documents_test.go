package server_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

func readJSON[T any](t *testing.T, srv *server.Server, path string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(fetch(t, srv, path)), &v); err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return v
}

// O corpo do /v1/metadata é o que o DatabaseMetaData do §71 vai consumir:
// tipo, alias, índice declarado e os limites EFETIVOS.
func TestMetadataDescreveOQueODriverPrecisa(t *testing.T) {
	srv := serverWithData(t, 3, blockPadrao+"query:\n  defaultMaxRows: 777\n  defaultTimeout: 9s\n")

	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata")
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

	byNome := map[string]protocol.ColumnMeta{}
	for _, c := range d.Columns {
		byNome[c.Name] = c
	}
	casos := []struct {
		name  string
		tipo  string
		jdbc  string
		index string
		alias string
	}{
		{"id", "string", protocol.JDBCVarchar, "none", ""},
		{"status", "string", protocol.JDBCVarchar, "bitmap", "estado"},
		{"porta", "int64", protocol.JDBCBigint, "none", ""},
		{"ip", "ipv4", protocol.JDBCVarchar, "none", ""},
	}
	for _, c := range casos {
		got, ok := byNome[c.name]
		if !ok {
			t.Fatalf("coluna %q ausente do metadata", c.name)
		}
		if got.Type != c.tipo || got.JDBC != c.jdbc || got.Index != c.index {
			t.Fatalf("coluna %q = %+v, quer tipo %s / %s / índice %s", c.name, got, c.tipo, c.jdbc, c.index)
		}
		switch {
		case c.alias == "" && got.Alias != nil:
			t.Fatalf("coluna %q tem alias %q, quer nenhum", c.name, *got.Alias)
		case c.alias != "" && (got.Alias == nil || *got.Alias != c.alias):
			t.Fatalf("coluna %q com alias %v, quer %q", c.name, got.Alias, c.alias)
		}
	}
}

// O filtro devolve a MESMA forma, com um elemento: o cliente não aprende dois
// parsers para as duas perguntas.
func TestMetadataFiltraPorDataset(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)

	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata?dataset=d")
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
	if f := failOf(t, resp); f.Code != protocol.CodeUnknownDataset {
		t.Fatalf("código = %q", f.Code)
	}
}

// As QUATRO rotas de leitura, e não duas.
//
// /metrics e /v1/health eram a exceção herdada de antes da Fase 11: o padrão
// levava o prefixo "GET " e o método errado saía com o 405 de TEXTO PURO do
// próprio mux, fora do contrato ErrorBody. Um cliente que soubesse decodificar
// a recusa das outras cinco rotas encontrava outra coisa nessas duas — e o
// driver JDBC decodifica a recusa em UM caminho só, para traduzi-la em
// SQLState.
func TestRotasDeLeituraRecusamMetodoErrado(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)

	// O Allow difere, e diz a verdade sobre cada rota: /metrics e /v1/health
	// aceitam HEAD (era o que o mux fazia sozinho quando o método vivia no
	// padrão, e um health check por HEAD não pode ter sido quebrado por uma
	// mudança que só queria uniformizar o CORPO da recusa). As outras duas
	// recusam HEAD desde sempre, e ampliá-las não é escopo daqui.
	rotas := map[string]string{
		"/v1/metadata": http.MethodGet,
		"/v1/stats":    http.MethodGet,
		"/metrics":     "GET, HEAD",
		"/v1/health":   "GET, HEAD",
	}
	for path, permitidoQuer := range rotas {
		resp, err := http.Post("http://"+srv.Endereco()+path, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("POST %s = %d, quer 405", path, resp.StatusCode)
		}
		if permitido := resp.Header.Get("Allow"); permitido != permitidoQuer {
			t.Errorf("POST %s: Allow = %q, quer %q", path, permitido, permitidoQuer)
		}
		if f := failOf(t, resp); f.Code != protocol.CodeMethod {
			t.Fatalf("POST %s: código = %q", path, f.Code)
		}
	}
}

// O /v1/stats e o /metrics precisam concordar: os dois leem a MESMA fonte no
// instante da chamada, e é essa a garantia que impede um painel de discordar do
// JSON sem nada estar quebrado.
func TestStatsConcordaComOMetrics(t *testing.T) {
	srv := serverWithData(t, 42, blockPadrao)

	// Uma consulta que dá certo e uma que falha, para haver o que contar.
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	if _, l := result(t, resp); l.End() == nil {
		t.Fatal("a consulta de aquecimento não fechou bem")
	}
	consulta(t, srv, `{"dataset":"d","sql":"SELECT nao_existe FROM d"}`).Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste

	e := readJSON[protocol.Stats](t, srv, "/v1/stats")
	if len(e.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(e.Datasets))
	}
	ds := e.Datasets[0]

	if ds.Storage.Rows != 42 {
		t.Fatalf("rows = %d, quer 42", ds.Storage.Rows)
	}
	if got := valueOfMetrica(t, srv, "nishidb_materialized_rows"); got != 42 {
		t.Fatalf("nishidb_materialized_rows = %v, quer 42 — o JSON e o /metrics divergiram", got)
	}
	if ds.Query.Total != uint64(valueOfMetrica(t, srv, "nishidb_query_total")) {
		t.Fatalf("total = %d contra %v no /metrics", ds.Query.Total, valueOfMetrica(t, srv, "nishidb_query_total"))
	}
	if ds.Query.Errors != uint64(valueOfMetrica(t, srv, "nishidb_query_errors")) {
		t.Fatalf("errors = %d contra %v no /metrics", ds.Query.Errors, valueOfMetrica(t, srv, "nishidb_query_errors"))
	}
	if ds.Query.Errors != 1 {
		t.Fatalf("errors = %d, quer 1", ds.Query.Errors)
	}

	// Os contadores do motor são do PROCESSO: repeti-los por dataset publicaria
	// o mesmo número várias vezes.
	if e.Process.Query.Emitted < 42 {
		t.Fatalf("emitted = %d, quer ao menos 42", e.Process.Query.Emitted)
	}
	if e.Process.Query.FullScans != int64(valueOfMetrica(t, srv, "nishidb_query_full_scans")) {
		t.Fatal("fullScans divergiu entre o JSON e o /metrics")
	}

	// Sem Kafka declarado, a ingestão é DISABLED — o mesmo vocabulário do §94,
	// para as duas rotas não descreverem a mesma realidade com palavras
	// diferentes.
	if ds.Ingestion.State != protocol.StateDisabled || ds.Ingestion.Enabled {
		t.Fatalf("ingestão = %+v", ds.Ingestion)
	}
	if h := readHealth(t, srv); h.Datasets["d"] != ds.Ingestion.State {
		t.Fatalf("health diz %q e stats diz %q", h.Datasets["d"], ds.Ingestion.State)
	}
}

// As cinco séries de consulta do §92 existem, com o tipo certo e alimentadas.
// Antes desta fase elas eram nome em names.go e nada mais.
func TestAsCincoSeriesDeQueryDoSpec92SaoAlimentadas(t *testing.T) {
	srv := serverWithData(t, 5, blockPadrao)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	result(t, resp)
	consulta(t, srv, `{"dataset":"d","sql":"SELECT nao_existe FROM d"}`).Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste

	scrape := fetch(t, srv, "/metrics")
	for _, name := range []string{
		"nishidb_query_total",
		"nishidb_query_active",
		"nishidb_query_errors",
		"nishidb_query_full_scans",
		"nishidb_query_latency_seconds",
	} {
		if !strings.Contains(scrape, "# TYPE "+name+" ") {
			t.Fatalf("a série %q não está no /metrics", name)
		}
	}

	if got := valueOfMetrica(t, srv, "nishidb_query_total"); got != 2 {
		t.Fatalf("query_total = %v, quer 2 (a recusada também foi admitida)", got)
	}
	waitMetrica(t, srv, "nishidb_query_active", 0)
	if got := valueOfMetrica(t, srv, "nishidb_query_latency_seconds_count"); got != 2 {
		t.Fatalf("latency_count = %v, quer 2", got)
	}
	// O full scan é a métrica normativa do §64: a query sem WHERE escolheu um.
	if got := valueOfMetrica(t, srv, "nishidb_query_full_scans"); got < 1 {
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
	srv := serverWithData(t, 1, blockPadrao)

	for _, name := range []string{"a1", "a2", "a3"} {
		consulta(t, srv, `{"dataset":"`+name+`","sql":"SELECT id FROM x"}`).Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste
	}
	scrape := fetch(t, srv, "/metrics")
	for _, name := range []string{"a1", "a2", "a3"} {
		if strings.Contains(scrape, `dataset="`+name+`"`) {
			t.Fatalf("o nome %q vindo do request virou label:\n%s", name, scrape)
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
// mesmo argumento que o godoc de serverWithDataEDiretorio registra, e é o
// que mata o mutante de publicar o tally de liveness sob o nome do disco.
func TestStatsPublicaODiscoEACompactionSemSegundaFonte(t *testing.T) {
	srv, dir := serverWithDataEDiretorio(t, 100, blockPadrao)

	// A subida põe o HTTP no ar ANTES de ligar a compaction (§51), então há
	// uma janela em que a rota já responde e o laço ainda não subiu. Esperar
	// pela CONDIÇÃO fecha a janela sem dormir — e o que se espera é o default
	// do §78, que traz a chave LIGADA: um YAML herdado do M1, sem o bloco
	// storage.compaction, tem de compactar.
	waitCompactionInAr(t, srv)

	e := readJSON[protocol.Stats](t, srv, "/v1/stats")
	if len(e.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(e.Datasets))
	}
	st := e.Datasets[0].Storage

	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob dos segmentos: %v", err)
	}
	var inDisk int64
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		inDisk += fi.Size()
	}
	if inDisk == 0 {
		t.Fatal("nenhum segmento em disco: o teste não estaria provando nada")
	}
	if st.Bytes != inDisk {
		t.Errorf("storage.bytes = %d e os arquivos somam %d: o JSON não descreve o disco", st.Bytes, inDisk)
	}
	if got := int64(valueOfMetrica(t, srv, metrics.MetStorageBytes)); st.Bytes != got {
		t.Errorf("storage.bytes = %d no JSON e %d no /metrics: duas fontes para o mesmo número", st.Bytes, got)
	}
	if got := uint64(valueOfMetrica(t, srv, metrics.MetStorageGarbageBytes)); st.GarbageBytes != got {
		t.Errorf("garbageBytes = %d no JSON e %d no /metrics", st.GarbageBytes, got)
	}

	// A compaction sai SEMPRE, inclusive antes da primeira passada — pelo
	// mesmo motivo das séries do §92: zero é uma resposta, ausência não é.
	if !st.Compaction.Running {
		t.Error("compaction.running = false com o default do §78, que traz a chave ligada")
	}
	if got := valueOfMetrica(t, srv, metrics.MetCompactionRunning); got != 1 {
		t.Errorf("compaction_running = %v no /metrics contra true no JSON", got)
	}
	if got := int64(valueOfMetrica(t, srv, metrics.MetCompactionBytesRead)); st.Compaction.BytesRead != got {
		t.Errorf("compaction.bytesRead = %d no JSON e %d no /metrics", st.Compaction.BytesRead, got)
	}
	if got := int64(valueOfMetrica(t, srv, metrics.MetCompactionBytesWritten)); st.Compaction.BytesWritten != got {
		t.Errorf("compaction.bytesWritten = %d no JSON e %d no /metrics", st.Compaction.BytesWritten, got)
	}
}

// waitCompactionInAr bloqueia até o /v1/stats reportar o laço de pé.
//
// Espera por condição, e não por sleep: dormir esconderia tanto a lentidão
// quanto a ausência de progresso — é a mesma regra do waitHealth.
func waitCompactionInAr(t *testing.T, srv *server.Server) {
	t.Helper()
	prazo := time.Now().Add(15 * time.Second)
	for time.Now().Before(prazo) {
		e := readJSON[protocol.Stats](t, srv, "/v1/stats")
		if len(e.Datasets) > 0 && e.Datasets[0].Storage.Compaction.Running {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a compaction não subiu em 15s: o default do §78 traz a chave ligada")
}

// TestStatsReportaACompactionParadaComAChaveDesligada é a outra metade do par
// do teste acima, e sem ela a primeira não prova nada: um `Running: true` fixo
// no lugar do `st.CompactionRunning` satisfaz todo oráculo que só exercita o
// laço DE PÉ — e o `waitCompactionInAr` está lá justamente para garantir que
// o outro caso nunca aconteça.
//
// O caso que falta é o que importa. O godoc de protocolo.CompactionStats
// escreve por extenso qual é: não é o Close, é a falha sticky do writer
// (§14.14), em que a goroutine para sozinha e o disco volta a crescer com o
// processo de pé. Essa falha não é provocável aqui sem avariar o writer; a
// chave `storage.compaction.enabled: false` produz o MESMO estado observável —
// laço parado, processo no ar — e é o que separa o campo de uma constante.
//
// É o mesmo par que as séries do §92 já têm no /metrics
// (TestSeriesDeCompactionSaemZeradasComAChaveDesligada). Aqui a afirmação é
// sobre o JSON, porque é o JSON que o `nishidb status` do §96 lê.
func TestStatsReportaACompactionParadaComAChaveDesligada(t *testing.T) {
	srv, _ := serverWithDataDiretorioEStorage(t, 100, blockPadrao, "  compaction:\n    enabled: false\n")

	e := readJSON[protocol.Stats](t, srv, "/v1/stats")
	if len(e.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(e.Datasets))
	}
	st := e.Datasets[0].Storage

	// Não há espera aqui, ao contrário do teste de cima: com a chave em false
	// o laço não sobe NUNCA, e esperar por ele só saberia gastar o prazo
	// inteiro antes de falhar.
	if st.Compaction.Running {
		t.Error("compaction.running = true com storage.compaction.enabled: false — " +
			"o operador leria laço de pé num processo que honrou a chave dele")
	}
	if got := valueOfMetrica(t, srv, metrics.MetCompactionRunning); got != 0 {
		t.Errorf("compaction_running = %v no /metrics contra false no JSON: duas fontes para o mesmo estado", got)
	}

	// Os dois contadores continuam SAINDO, zerados: campo que some do JSON
	// vira `undefined` no cliente, indistinguível de servidor velho — é a
	// mesma decisão que mantém as três séries no scrape com a chave
	// desligada.
	if st.Compaction.BytesRead != 0 || st.Compaction.BytesWritten != 0 {
		t.Errorf("compaction moveu %d/%d bytes com a chave desligada",
			st.Compaction.BytesRead, st.Compaction.BytesWritten)
	}
}

// columnsOfSchemaOfConsulta devolve a linha de schema de uma consulta, que é a
// outra metade do contrato de metadados — a que descreve a PROJEÇÃO.
func columnsOfSchemaOfConsulta(t *testing.T, srv *server.Server, sql string) []protocol.Column {
	t.Helper()
	resp := consulta(t, srv, `{"dataset":"d","sql":`+strconv.Quote(sql)+`}`)
	_, l := result(t, resp)
	sch := l.Schema()
	if sch == nil {
		t.Fatal("a resposta não trouxe linha de schema")
	}
	return sch.Columns
}

// Os três números do ResultSetMetaData saem na linha de schema, por coluna.
//
// Sem eles o driver teria de carregar a tabela por dentro do jar, e um tipo
// novo no servidor passaria a ser descrito por uma constante compilada num
// artefato que só se corrige com release.
func TestALinhaDeSchemaPublicaPrecisionScaleEDisplaySize(t *testing.T) {
	srv := serverWithData(t, 2, blockPadrao)

	cols := columnsOfSchemaOfConsulta(t, srv, "SELECT id, porta, ip FROM d")
	quer := []protocol.Column{
		// "id" é a CHAVE do dataset: nullable false. string não tem teto
		// declarado, então os três números são 0 — a convenção do JDBC para
		// desconhecido.
		{Name: "id", Type: "string", JDBC: protocol.JDBCVarchar, Nullable: false, Precision: 0, Scale: 0, DisplaySize: 0},
		{Name: "porta", Type: "int64", JDBC: protocol.JDBCBigint, Nullable: true, Precision: 19, Scale: 0, DisplaySize: 20},
		{Name: "ip", Type: "ipv4", JDBC: protocol.JDBCVarchar, Nullable: true, Precision: 15, Scale: 0, DisplaySize: 15},
	}
	if len(cols) != len(quer) {
		t.Fatalf("colunas = %d, quer %d", len(cols), len(quer))
	}
	for i := range quer {
		if cols[i] != quer[i] {
			t.Errorf("coluna %d = %+v, quer %+v", i, cols[i], quer[i])
		}
	}
}

// O godoc de DescriptorOf promete que o driver nunca vai "descrever a mesma
// coluna de dois jeitos conforme a rota que consultou". Ninguém cobrava.
//
// Este é o oráculo: para a MESMA coluna, /v1/metadata e a linha de schema de
// /v1/query têm de publicar os quatro campos de tipo idênticos. É o teste que
// quebra no dia em que alguém acrescentar um campo num dos dois caminhos de
// serialização e esquecer o outro — que é o modo de falha real, porque os dois
// pontos ficam em arquivos diferentes.
func TestMetadataPublicaOsMesmosNumerosQueALinhaDeSchema(t *testing.T) {
	srv := serverWithData(t, 2, blockPadrao)

	meta := readJSON[protocol.Metadata](t, srv, "/v1/metadata")
	if len(meta.Datasets) != 1 {
		t.Fatalf("datasets = %d, quer 1", len(meta.Datasets))
	}
	// A chave é o nome de SAÍDA, que é o alias quando o campo tem um.
	//
	// ARMADILHA PARA O DRIVER, e a razão de este mapa não ser byName[c.Name]:
	// as duas rotas nomeiam a mesma coluna de formas diferentes por decisão. A
	// linha de schema traz o nome que aparece na resposta — o alias, porque é
	// ele que é chave do objeto JSON de cada row —, enquanto o /v1/metadata
	// traz o nome do CAMPO e o alias num campo à parte, porque descreve o
	// schema e não uma projeção. O DatabaseMetaData.getColumns() do driver vai
	// publicar "status", e o ResultSetMetaData de SELECT * vai publicar
	// "estado", para a mesma coluna. Casar os dois por igualdade de nome não
	// funciona, e é assim que um findColumn ingênuo erra.
	bySaida := map[string]protocol.ColumnMeta{}
	for _, c := range meta.Datasets[0].Columns {
		saida := c.Name
		if c.Alias != nil {
			saida = *c.Alias
		}
		bySaida[saida] = c
	}

	// SELECT * cobre TODAS as colunas do dataset, não uma amostra escolhida —
	// é o que faz o teste continuar exaustivo quando o spec de teste crescer.
	for _, col := range columnsOfSchemaOfConsulta(t, srv, "SELECT * FROM d") {
		m, ok := bySaida[col.Name]
		if !ok {
			t.Errorf("coluna de saída %q está no schema da consulta e não no /v1/metadata", col.Name)
			continue
		}
		if m.Type != col.Type || m.JDBC != col.JDBC || m.Nullable != col.Nullable ||
			m.Precision != col.Precision || m.Scale != col.Scale || m.DisplaySize != col.DisplaySize {
			t.Errorf("coluna %q diverge entre as rotas:\n  /v1/metadata: type=%q jdbc=%q null=%v p=%d s=%d d=%d\n  /v1/query:    type=%q jdbc=%q null=%v p=%d s=%d d=%d",
				col.Name, m.Type, m.JDBC, m.Nullable, m.Precision, m.Scale, m.DisplaySize,
				col.Type, col.JDBC, col.Nullable, col.Precision, col.Scale, col.DisplaySize)
		}
	}
}

// Os campos saem SEM omitempty, e este teste trava a decisão.
//
// precision:0 é justamente o caso que carrega informação — "sem teto
// declarado" —, e com omitempty ele sumiria do JSON. O driver leria a ausência
// como zero do mesmo jeito, mas o contrato deixaria de dizer o que decidiu, e
// um cliente escrito contra o campo ausente passaria a ver o campo aparecer no
// dia em que o tipo ganhasse teto.
//
// A asserção é sobre os BYTES, e não sobre a struct decodificada, porque é a
// única forma de distinguir "0 escrito" de "campo omitido": as duas decodificam
// para o mesmo zero.
func TestPrecisionZeroSaiNoJSONEmVezDeSerOmitida(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	defer fecha(t, resp)
	bruto, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	primeira, _, _ := strings.Cut(string(bruto), "\n")
	for _, key := range []string{`"precision":0`, `"scale":0`, `"displaySize":0`} {
		if !strings.Contains(primeira, key) {
			t.Errorf("a linha de schema não traz %s — omitempty entrou no campo?\n%s", key, primeira)
		}
	}
}

// A chave sai NÃO-NULÁVEL nas duas rotas, e é o que o DBeaver lê para desenhar
// a chave primária e para gerar o DDL.
//
// A afirmação não é uma convenção: é o que o materializer garante, recusando o
// evento cuja chave venha ausente, nula ou vazia — os cinco casos estão em
// TestParseKeyAndSequenceErrorsSayTheCause. Sem essa recusa do outro lado,
// nullable:false aqui seria só uma opinião publicada no contrato.
func TestAChaveSaiNaoNulavelNasDuasRotas(t *testing.T) {
	srv := serverWithData(t, 2, blockPadrao)

	meta := readJSON[protocol.Metadata](t, srv, "/v1/metadata")
	achou := false
	for _, c := range meta.Datasets[0].Columns {
		if c.Name != meta.Datasets[0].Key {
			continue
		}
		achou = true
		if c.Nullable {
			t.Errorf("/v1/metadata publica a chave %q como nulável", c.Name)
		}
	}
	if !achou {
		t.Fatalf("a chave %q não aparece nas colunas do /v1/metadata", meta.Datasets[0].Key)
	}

	cols := columnsOfSchemaOfConsulta(t, srv, "SELECT id FROM d")
	if cols[0].Nullable {
		t.Errorf("a linha de schema publica a chave como nulável: %+v", cols[0])
	}
}

// HEAD continua servido em /metrics e /v1/health.
//
// Enquanto o padrão de registro era "GET /metrics", o mux tratava HEAD como
// GET automaticamente. Tirar o método do padrão para uniformizar o corpo do
// 405 tirou junto esse tratamento, e sem uma conferência explícita dentro do
// handler um health check por HEAD passaria a receber 405 — regressão de
// operação causada por uma mudança que só queria mexer no corpo do erro.
func TestHeadContinuaServidoEmMetricsEHealth(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)

	for _, path := range []string{"/metrics", "/v1/health"} {
		req, err := http.NewRequest(http.MethodHead, "http://"+srv.Endereco()+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("HEAD %s: %v", path, err)
		}
		fecha(t, resp)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("HEAD %s = %d, quer 200", path, resp.StatusCode)
		}
	}
}

// O teto de campos pode custar dado sob `ignore`, e o modo de falha
// inaceitável aqui é o silêncio: o tamanho corrente do dicionário e a contagem
// de descartes saem pelas DUAS rotas e precisam concordar, pela mesma razão que
// TestStatsConcordaComOMetrics nomeia para as outras.
func TestCamposDoDatasetSaemNoStatsENoMetrics(t *testing.T) {
	srv := serverWithData(t, 3, blockPadrao)

	e := readJSON[protocol.Stats](t, srv, "/v1/stats")
	if len(e.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(e.Datasets))
	}
	ds := e.Datasets[0]

	if ds.Schema.Fields == 0 {
		t.Fatal("schema.fields = 0; o dataset tem campos declarados")
	}
	if got := valueOfMetrica(t, srv, "nishidb_dataset_fields"); got != float64(ds.Schema.Fields) {
		t.Errorf("nishidb_dataset_fields = %v contra %d no JSON", got, ds.Schema.Fields)
	}
	if ds.Schema.FieldsDropped != 0 {
		t.Errorf("fieldsDropped = %d num dataset que não bateu no teto", ds.Schema.FieldsDropped)
	}
	if got := valueOfMetrica(t, srv, "nishidb_dataset_fields_dropped_total"); got != 0 {
		t.Errorf("nishidb_dataset_fields_dropped_total = %v, quer 0", got)
	}
	if ds.Schema.MaxCols != 65535 {
		t.Errorf("maxCols = %d, quer o teto físico 65535", ds.Schema.MaxCols)
	}
}

// O bloco `schema:` aceita chave desconhecida em silêncio — um `maxColls` com
// typo parseia limpo e não faz nada. Publicar o valor EFETIVO é o que dá ao
// operador onde conferir: o typo aparece como "continua no default". É a mesma
// razão pela qual o maxRows já é publicado assim.
func TestMetadataPublicaOTetoDeCamposEfetivo(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)
	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata")
	if len(m.Datasets) != 1 {
		t.Fatalf("datasets = %d", len(m.Datasets))
	}
	d := m.Datasets[0]
	if d.MaxCols != 65535 {
		t.Errorf("maxCols = %d, quer 65535", d.MaxCols)
	}
	if d.OnMaxCols != "ignore" {
		t.Errorf("onMaxCols = %q, quer ignore", d.OnMaxCols)
	}
}

// O /v1/metadata publica a coluna derivada mesmo ela ficando fora do
// SELECT *. É deliberadamente um superset: sem isto o DBeaver não a lista e
// ninguém descobre que ela existe.
func TestMetadataPublicaAColunaDerivada(t *testing.T) {
	srv := serverWithExtract(t)
	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata")
	var achou bool
	for _, d := range m.Datasets {
		for _, c := range d.Columns {
			if c.Name == "probe_ip" {
				achou = true
			}
		}
	}
	if !achou {
		t.Error("probe_ip não apareceu no /v1/metadata")
	}
}

// specWithExtract é um dataset que declara uma coluna extraída de dentro de um
// campo json.
const specWithExtract = `version: 1
name: d
schemaName: telecom
materialization:
  key:
    field: id
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    doc:
      type: json
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
`

func serverWithExtract(t *testing.T) *server.Server {
	t.Helper()
	a := newEnvWith(t, blockPadrao)
	a.writeDataset("d", specWithExtract)
	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}

// tableOf acha a linha de `tables` pelo endereço do atalho — `_meta.tables`,
// `_counters.por_olt`, ou só o nome na base —, que é o mesmo `id` pelo qual a
// `_meta.tables` endereça a tabela por SQL.
func tableOf(t *testing.T, d protocol.DatasetMeta, id string) protocol.TableMeta {
	t.Helper()
	for _, tab := range d.Tables {
		if idOfTable(tab) == id {
			return tab
		}
	}
	t.Fatalf("a tabela %q não saiu no /v1/metadata do dataset %q: %s", id, d.Name, idsOfTables(d))
	return protocol.TableMeta{}
}

// idOfTable monta o endereço do ATALHO a partir da linha, pela mesma regra da
// `_meta.tables`: namespace vazio é a base, aberta só pelo nome.
func idOfTable(tab protocol.TableMeta) string {
	if tab.Namespace == "" {
		return tab.Name
	}
	return tab.Namespace + "." + tab.Name
}

func idsOfTables(d protocol.DatasetMeta) string {
	ids := make([]string, 0, len(d.Tables))
	for _, tab := range d.Tables {
		ids = append(ids, idOfTable(tab))
	}
	return strings.Join(ids, ",")
}

// counterBlock declara UM contador sobre o campo indexado do specConsultavel.
//
// Ele existe para que a lista do catálogo tenha a linha `_counters.<nome>`: é a
// ÚNICA em que o Schema (o do dataset) difere do Namespace, e é o ramo de
// query.catalogEntries que escolhe o container da árvore. Um fixture sem
// contador deixa esse ramo fora da lista, e um teste que se anuncia como "a
// lista INTEIRA" passaria com ele quebrado.
const counterBlock = `counters:
  - name: por_status
    groupBy: [status]
`

// serverWithCounter sobe o servidor do specConsultavel com o contador declarado.
// O dataset é povoado ANTES do `counters:`, como no counters_test: é o servidor
// que constrói o contador na abertura.
func serverWithCounter(t *testing.T, n int) *server.Server {
	t.Helper()
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("d", strings.ReplaceAll(specConsultavel, "%s", "d")+counterBlock)
	seed(t, a, "d", n)
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}

// O /v1/metadata é de onde o driver JDBC monta a árvore do cliente SQL, e até
// aqui ele só publicava as colunas da tabela BASE: nem contador nem tabela de
// meta apareciam, e a lista completa só saía por `SELECT ... FROM _meta.tables`
// — que nenhuma ferramenta que monta árvore escreve sozinha.
//
// O teste cobra as três informações que a árvore precisa e que o driver não
// tem como derivar: o SCHEMA em que cada tabela mora (que para as do `_meta`
// não é o do dataset), o NAMESPACE do atalho (que não sai de (schema, nome)) e
// o código JDBC de cada coluna — calculado no servidor, pelo mesmo columnMeta
// que já serve as colunas da base.
//
// O fixture declara um contador de propósito: sem ele a lista não tem nenhuma
// linha em que Schema e Namespace divirjam, e o ramo do catálogo que põe o
// contador no schema do DATASET — e não no `_virtual` da spec sintética dele —
// não é exercitado por este canal.
func TestMetadataPublicaOSchemaEOCatalogoDeTabelas(t *testing.T) {
	srv := serverWithCounter(t, 3)

	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata")
	if len(m.Datasets) != 1 {
		t.Fatalf("datasets = %d, quer 1", len(m.Datasets))
	}
	d := m.Datasets[0]
	if d.Schema != "telecom" {
		t.Fatalf("schema = %q, quer telecom (o schemaName declarado no YAML)", d.Schema)
	}

	// A lista INTEIRA e em ordem, e não um Contains: o que se cobra é a
	// enumeração, e uma asserção que só procurasse a base passaria com o
	// catálogo errado ao redor dela. A ordem é a do Catalog — base, contadores,
	// `_meta` por nome.
	const wantIDs = "d,_counters.alarms,_counters.por_status,_meta.counter_metrics,_meta.counters,_meta.fields,_meta.kafka,_meta.tables"
	if got := idsOfTables(d); got != wantIDs {
		t.Fatalf("tabelas = %s, quero %s", got, wantIDs)
	}

	// O contador mora no schema do DATASET, ao lado da base — é tabela do
	// usuário —, e é alcançado pelo atalho `_counters`. É a única linha em que
	// os dois endereços não coincidem.
	counter := tableOf(t, d, "_counters.por_status")
	if counter.Schema != "telecom" || counter.Namespace != "_counters" || counter.Kind != "VIEW" {
		t.Errorf("o contador saiu como %+v; quero schema telecom, namespace _counters e kind VIEW", counter)
	}

	base := tableOf(t, d, "d")
	if base.Schema != "telecom" || base.Namespace != "" || base.Kind != "TABLE" {
		t.Errorf("a base saiu como %+v; quero schema telecom, namespace vazio e kind TABLE", base)
	}
	// A base publica as MESMAS colunas do campo `columns` do dataset: é o
	// mesmo elemento do catálogo servindo os dois campos. A comparação é do
	// JSON dos dois blocos porque o Alias é ponteiro, e depois de decodificado
	// cada bloco tem o seu — comparar as structs compararia endereços.
	if a, b := reserialize(t, base.Columns), reserialize(t, d.Columns); a != b {
		t.Errorf("as colunas da base divergem entre os dois campos.\ntables:\n%s\ncolumns:\n%s", a, b)
	}

	// A tabela de meta mora no schema `_meta`, e não no do dataset: é o que a
	// árvore do cliente separa em dois containers.
	fields := tableOf(t, d, "_meta.fields")
	if fields.Schema != "_meta" || fields.Namespace != "_meta" || fields.Kind != "VIEW" {
		t.Errorf("a _meta.fields saiu como %+v; quero schema e namespace _meta e kind VIEW", fields)
	}
	if fields.Description != "" {
		t.Errorf("description = %q; hoje não há onde declará-la e o catálogo não inventa uma", fields.Description)
	}

	// O `jdbc` de TODA coluna de TODA tabela: é calculado no servidor
	// (DescriptorOf) e o driver não sabe derivá-lo do nome do tipo.
	for _, tab := range d.Tables {
		if len(tab.Columns) == 0 {
			t.Errorf("a tabela %q saiu sem colunas", idOfTable(tab))
		}
		for _, c := range tab.Columns {
			if c.JDBC == "" {
				t.Errorf("a coluna %s.%s saiu sem código jdbc", idOfTable(tab), c.Name)
			}
		}
	}
	// Uma coluna nomeada, para que o laço acima não passe com um catálogo que
	// publicasse OTHER para tudo.
	name := columnOf(t, fields, "name")
	if name.Type != "string" || name.JDBC != protocol.JDBCVarchar {
		t.Errorf("_meta.fields.name = %+v, quer string/%s", name, protocol.JDBCVarchar)
	}
}

func columnOf(t *testing.T, tab protocol.TableMeta, name string) protocol.ColumnMeta {
	t.Helper()
	for _, c := range tab.Columns {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("a coluna %q não saiu na tabela %q", name, idOfTable(tab))
	return protocol.ColumnMeta{}
}

// rowsOfMetaTables roda `SELECT ... FROM _meta.tables ORDER BY id` pela rota
// de consulta e devolve uma linha por tabela, no formato
// `id|schema|name|namespace|kind|colunas`. `namespace` é NULL na base — e NULL
// é ausência no wire —, o que sai como campo vazio, do mesmo jeito que o
// /v1/metadata o publica.
func rowsOfMetaTables(t *testing.T, srv *server.Server, dataset string) []string {
	t.Helper()
	resp := consulta(t, srv, `{"dataset":"`+dataset+
		`","sql":"SELECT id, schema, name, namespace, kind, columns FROM _meta.tables ORDER BY id"}`)
	if resp.StatusCode != 200 {
		t.Fatalf("a consulta foi recusada (%d): %s", resp.StatusCode, failOf(t, resp).Message)
	}
	rows, reader := result(t, resp)
	if reader.End() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		var l map[string]any
		if err := json.Unmarshal([]byte(row), &l); err != nil {
			t.Fatalf("linha ilegível: %v", err)
		}
		text := func(col string) string {
			v, _ := l[col].(string)
			return v
		}
		columns, ok := l["columns"].(float64)
		if !ok {
			t.Fatalf("linha sem contagem de colunas: %s", row)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%s|%d",
			text("id"), text("schema"), text("name"), text("namespace"), text("kind"), int(columns)))
	}
	return out
}

// rowsOfMetadata põe o `tables` do /v1/metadata no MESMO formato, ordenado
// por id — é a única forma de os dois documentos serem comparáveis linha a
// linha.
func rowsOfMetadata(t *testing.T, srv *server.Server, dataset string) []string {
	t.Helper()
	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata?dataset="+dataset)
	if len(m.Datasets) != 1 {
		t.Fatalf("o /v1/metadata filtrado trouxe %d datasets", len(m.Datasets))
	}
	out := make([]string, 0, len(m.Datasets[0].Tables))
	for _, tab := range m.Datasets[0].Tables {
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%s|%d",
			idOfTable(tab), tab.Schema, tab.Name, tab.Namespace, tab.Kind, len(tab.Columns)))
	}
	sort.Strings(out)
	return out
}

// O /v1/metadata e a `_meta.tables` respondem à MESMA pergunta — que tabelas
// este dataset tem — por dois canais, e este projeto trata duas derivações do
// mesmo fato como a origem de "uma ferramenta que discorda do SQL sem nada
// estar quebrado". As duas saem do query.Engine.Catalog, e é isto que o teste
// cobra.
//
// A comparação é da lista INTEIRA e de todos os campos que as duas publicam, e
// não de um Contains: montar o `tables` a partir dos registros do motor em vez
// do Catalog produz uma lista PLAUSÍVEL — ela erra a linha que o Catalog
// acrescenta à parte (`_meta.fields`, que não está em registro nenhum) e o
// schema das demais, que no registro é o namespace e não o container da
// árvore.
//
// Os dois lados são exercitados nos DOIS datasets do fixture com histórico,
// porque as listas são diferentes (a irmã não recebe `_meta.counters` nem
// `_meta.kafka`, débito 117): um oráculo que valesse só para o base passaria
// com a irmã servindo o catálogo do base.
func TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo(t *testing.T) {
	srv := serverWithHistory(t, specWithHistory)
	for _, dataset := range []string{"eventos", "eventos_history"} {
		bySQL := rowsOfMetaTables(t, srv, dataset)
		byHTTP := rowsOfMetadata(t, srv, dataset)
		if len(bySQL) < 3 {
			t.Fatalf("%s: a _meta.tables trouxe %d linhas; com menos de três o oráculo não distingue nada",
				dataset, len(bySQL))
		}
		if strings.Join(bySQL, "\n") != strings.Join(byHTTP, "\n") {
			t.Errorf("%s: o /v1/metadata e a _meta.tables discordam.\npor SQL:\n%s\npor HTTP:\n%s",
				dataset, strings.Join(bySQL, "\n"), strings.Join(byHTTP, "\n"))
		}
	}
}

// previousDatasetMeta é o DatasetMeta como ele era ANTES de `schema` e
// `tables` entrarem: os mesmos nomes de chave, na mesma forma. Ele está
// escrito à mão, e não derivado do protocol.DatasetMeta, porque é exatamente
// isso que um cliente compilado contra a versão anterior tem em mãos — derivá-lo
// do tipo corrente faria o oráculo acompanhar a mudança que ele existe para
// vigiar.
type previousDatasetMeta struct {
	Name             string                `json:"name"`
	Key              string                `json:"key"`
	MaxRows          int                   `json:"maxRows"`
	TimeoutMs        int64                 `json:"timeoutMs"`
	DefaultTimeoutMs int64                 `json:"defaultTimeoutMs"`
	MaxCols          int                   `json:"maxCols"`
	OnMaxCols        string                `json:"onMaxCols"`
	MaxNestedCols    int                   `json:"maxNestedCols"`
	Columns          []protocol.ColumnMeta `json:"columns"`
}

// Os campos novos são ADITIVOS: um cliente que não os conhece continua lendo o
// documento como antes. É a condição para publicá-los sem quebrar o driver e o
// client Python já instalados — os dois decodificam por chave e ignoram o que
// não conhecem.
//
// O que o teste protege é o OUTRO lado da aditividade, que é o que se perde por
// descuido: mover informação para o bloco novo. Esvaziar `columns` porque "agora
// está em tables", ou trocar `name` pelo par (schema, name), continuaria
// decodificando limpo num cliente antigo e o deixaria sem colunas nenhuma.
func TestOClienteAnteriorAosCamposNovosContinuaLendoOMetadata(t *testing.T) {
	srv := serverWithData(t, 3, blockPadrao+"query:\n  defaultMaxRows: 777\n  defaultTimeout: 9s\n")

	var body struct {
		Datasets []previousDatasetMeta `json:"datasets"`
	}
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/metadata")), &body); err != nil {
		t.Fatalf("um cliente anterior não decodificou o documento: %v", err)
	}
	if len(body.Datasets) != 1 {
		t.Fatalf("datasets = %d, quer 1", len(body.Datasets))
	}
	d := body.Datasets[0]
	// `name` continua sendo o nome do DATASET, e não o par com o schema: é por
	// ele que o cliente anterior monta o `?dataset=` e o corpo do /v1/query.
	if d.Name != "d" || d.Key != "id" {
		t.Errorf("name/key = %q/%q, quer d/id", d.Name, d.Key)
	}
	if d.MaxRows != 777 || d.TimeoutMs != 9000 || d.DefaultTimeoutMs != 9000 {
		t.Errorf("limites = %d linhas, teto %d ms, padrão %d ms; quero 777/9000/9000",
			d.MaxRows, d.TimeoutMs, d.DefaultTimeoutMs)
	}
	if d.MaxCols != 65535 || d.OnMaxCols != "ignore" || d.MaxNestedCols == 0 {
		t.Errorf("teto de campos = %d/%q/%d", d.MaxCols, d.OnMaxCols, d.MaxNestedCols)
	}
	// `columns` continua sendo a lista COMPLETA da tabela base, e não uma lista
	// vazia com o conteúdo mudado de lugar. Os nomes saem ORDENADOS para a
	// comparação: a ordem da lista é a de FieldID, e o que este teste protege é
	// o CONJUNTO — nenhuma coluna mudou de lugar para o bloco novo.
	names := make([]string, 0, len(d.Columns))
	byName := map[string]protocol.ColumnMeta{}
	for _, c := range d.Columns {
		names = append(names, c.Name)
		byName[c.Name] = c
	}
	sort.Strings(names)
	if got := strings.Join(names, ","); got != "id,ip,porta,status" {
		t.Errorf("columns = %s, quero id,ip,porta,status", got)
	}
	// O conteúdo de cada coluna também continua inteiro: alias, código JDBC e
	// índice declarado são o que o driver anterior lê de cada uma.
	status := byName["status"]
	if status.Alias == nil || *status.Alias != "estado" {
		t.Errorf("o alias de status sumiu: %+v", status)
	}
	if status.JDBC != protocol.JDBCVarchar || status.Index != "bitmap" {
		t.Errorf("status = %+v, quer %s / bitmap", status, protocol.JDBCVarchar)
	}
}

// reserialize devolve o JSON de um valor já decodificado, para comparar blocos
// cujo conteúdo tem ponteiro — comparar as structs compararia endereços.
func reserialize(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
