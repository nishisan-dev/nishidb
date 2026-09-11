package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
