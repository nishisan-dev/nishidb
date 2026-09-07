package server_test

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

const specComHistorico = `version: 1
name: eventos
materialization:
  key:
    field: id
history:
  expireAfterWrite: 7d
  maxBytes: 4GiB
  fields:
    id:
      index: true
schema:
  include: ["*"]
  fields:
    id:
      type: string
    severity:
      type: int64
`

func servidorComHistorico(t *testing.T, spec string) *server.Servidor {
	t.Helper()
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("eventos", spec)
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}

// A tabela irmã é um dataset de pleno direito para o /v1/metadata, e é isso
// que a faz aparecer na árvore do DBeaver sem nenhuma mudança no driver.
func TestATabelaIrmaApareceNoMetadata(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	meta := leJSON[protocol.Metadata](t, srv, "/v1/metadata")
	nomes := map[string]bool{}
	for _, d := range meta.Datasets {
		nomes[d.Name] = true
	}
	if !nomes["eventos"] {
		t.Fatalf("o dataset base sumiu: %v", nomes)
	}
	if !nomes["eventos_history"] {
		t.Fatalf("a tabela irmã não aparece: %v", nomes)
	}
}

// A consulta à irmã sai de graça: ela é registrada no motor sob o próprio
// nome, e o binder confere o nome do FROM contra o Compiled dela.
func TestConsultaNaTabelaIrmaFunciona(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	resp := consulta(t, srv, `{"dataset":"eventos_history","sql":"SELECT id FROM eventos_history"}`)
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
}

// As duas tabelas têm as MESMAS colunas: é o dicionário compartilhado que
// garante isso, e é o requisito que a feature existe para cumprir.
func TestAsDuasTabelasTemAsMesmasColunas(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	meta := leJSON[protocol.Metadata](t, srv, "/v1/metadata")
	colunas := map[string][]string{}
	for _, d := range meta.Datasets {
		for _, c := range d.Columns {
			colunas[d.Name] = append(colunas[d.Name], c.Name+":"+c.Type)
		}
	}
	base, irma := colunas["eventos"], colunas["eventos_history"]
	if len(base) == 0 {
		t.Fatal("o base não publicou coluna nenhuma")
	}
	if strings.Join(base, ",") != strings.Join(irma, ",") {
		t.Fatalf("colunas divergem:\n base = %v\n irmã = %v", base, irma)
	}
}

// A irmã não tem key: sem primary não há dedup, que é a definição da tabela.
// É essa ausência que impede o planner de eleger o point lookup sobre um
// Store que não tem contra o que resolvê-lo.
func TestATabelaIrmaNaoDeclaraKey(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	meta := leJSON[protocol.Metadata](t, srv, "/v1/metadata")
	for _, d := range meta.Datasets {
		if d.Name != "eventos_history" {
			continue
		}
		if d.Key != "" {
			t.Fatalf("a irmã declara key %q", d.Key)
		}
		return
	}
	t.Fatal("a irmã não aparece no metadata")
}

// Os dois disputariam o mesmo diretório, com dois writers, dois contadores de
// rowId e dois manifests. A recusa vem ANTES de abrir qualquer coisa.
func TestNomeDaIrmaQueColideComDatasetDeclaradoRecusaASubida(t *testing.T) {
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("eventos", specComHistorico)
	a.escreveDataset("eventos_history", strings.ReplaceAll(specSemKafka, "%s", "eventos_history"))
	if _, err := server.Novo(a.carrega(), server.Opcoes{}); err == nil {
		t.Fatal("a subida foi aceita com a colisão de nome")
	} else if !strings.Contains(err.Error(), "eventos_history") {
		t.Fatalf("a mensagem precisa nomear a tabela: %v", err)
	}
}

// Sem o bloco `history` nada muda: o dataset sobe sozinho, como sempre.
func TestSemBlocoHistoryNaoHaTabelaIrma(t *testing.T) {
	srv := servidorComHistorico(t, strings.ReplaceAll(specSemKafka, "%s", "eventos"))
	meta := leJSON[protocol.Metadata](t, srv, "/v1/metadata")
	if len(meta.Datasets) != 1 {
		t.Fatalf("%d datasets, quer 1: %+v", len(meta.Datasets), meta.Datasets)
	}
}

// O /v1/stats e o /metrics precisam responder com a tabela irmã de pé: ela
// não tem materializer — quem escreve nela é o do dataset base —, e o laço
// que percorre todos os datasets passa por ela.
func TestStatsRespondeComATabelaIrma(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	st := leJSON[protocol.Stats](t, srv, "/v1/stats")
	var base, irma *protocol.DatasetStats
	for i := range st.Datasets {
		switch st.Datasets[i].Name {
		case "eventos":
			base = &st.Datasets[i]
		case "eventos_history":
			irma = &st.Datasets[i]
		}
	}
	if base == nil || irma == nil {
		t.Fatalf("datasets = %+v", st.Datasets)
	}
	// O bloco `history` vive no BASE: é nele que o operador declarou a chave.
	if base.History == nil {
		t.Fatal("o dataset base não publica o bloco history")
	}
	if base.History.Table != "eventos_history" {
		t.Errorf("History.Table = %q", base.History.Table)
	}
	if base.History.ExpireAfterWrite == "" {
		t.Error("History.ExpireAfterWrite vazio com a chave declarada")
	}
	if base.History.MaxBytes == 0 {
		t.Error("History.MaxBytes zerado com a chave declarada")
	}
	// E a irmã NÃO publica um bloco history próprio: ela é a tabela, não a
	// dona da configuração.
	if irma.History != nil {
		t.Errorf("a irmã publica um bloco history: %+v", irma.History)
	}
}

func TestMetricasDoHistoricoSaemNoRotuloDoBase(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	corpo := pega(t, srv, "/metrics")
	for _, serie := range []string{
		"nishidb_history_rows_total",
		"nishidb_history_dropped_total",
		"nishidb_history_degraded",
		"nishidb_history_data_bytes",
		"nishidb_history_index_bytes",
		"nishidb_history_index_window_seconds",
		"nishidb_history_segments_purged_total",
		"nishidb_history_retention_running",
	} {
		if !strings.Contains(corpo, serie+`{dataset="eventos"}`) &&
			!strings.Contains(corpo, serie+`{dataset="eventos",`) {
			t.Errorf("a série %s não saiu no rótulo do dataset base", serie)
		}
	}
}

// As séries do watchdog existem desde a subida, com zero.
//
// Série que só aparece quando há o que mostrar vira lacuna no gráfico, e
// lacuna não se distingue de alvo fora do ar. Alarmar em "desistiu" exige que
// ela exista ANTES de desistir — que é justamente quando ninguém está olhando.
func TestSeriesDoWatchdogNascemComZero(t *testing.T) {
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("eventos", preenche(specComKafka, "eventos", "true"))
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)

	corpo := pega(t, srv, "/metrics")
	for _, serie := range []string{
		`nishidb_kafka_watchdog_trips_total{dataset="eventos"} 0`,
		`nishidb_kafka_watchdog_desistiu{dataset="eventos"} 0`,
	} {
		if !strings.Contains(corpo, serie) {
			t.Errorf("a série não nasceu com zero: %s", serie)
		}
	}
}

// Uma janela maior que a retenção: não indexa nada a mais, porque a retenção
// aposenta o segmento antes de a janela o alcançar.
const specComJanelaInocua = `version: 1
name: eventos
materialization:
  key:
    field: id
history:
  expireAfterWrite: 1h
  maxBytes: 4GiB
  index:
    window: 7d
  fields:
    id:
      index: true
schema:
  include: ["*"]
  fields:
    id:
      type: string
    severity:
      type: int64
`

// A janela inócua ANUNCIA e não recusa, e as duas metades importam: recusar
// transformaria redundância em queda de serviço, e aceitar calado deixaria o
// operador com uma chave que não faz o que ele acha que faz.
func TestJanelaMaiorQueARetencaoSobeComWarn(t *testing.T) {
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("eventos", specComJanelaInocua)
	registro := &registroDoServidor{}
	srv, err := server.Novo(a.carrega(), server.Opcoes{
		Log: logging.New(registro, logging.FormatText, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("subida: %v — a janela inócua não pode derrubar o servidor", err)
	}
	roda(t, srv)
	// Sem espera: o aviso sai do abreHistorico, que roda DENTRO do Novo — se
	// ele não está aqui, não vai chegar depois.
	if !strings.Contains(registro.String(), "janela.maior_que_retencao") {
		t.Fatalf("o WARN nomeado não foi emitido:\n%s", registro.String())
	}
	// E ele nomeia o dataset BASE, que é onde o YAML com a chave está: nomear
	// só a tabela irmã mandaria o operador procurar num arquivo que não existe.
	if !strings.Contains(registro.String(), "dataset=eventos ") {
		t.Fatalf("o aviso não nomeia o dataset base:\n%s", registro.String())
	}
}

// E a janela declarada aparece no /v1/stats junto da cobertura REAL: é a
// comparação entre as duas que diz se o teto do índice assumiu o comando.
func TestOStatsPublicaAJanelaDeclaradaEAEfetiva(t *testing.T) {
	srv := servidorComHistorico(t, strings.Replace(specComHistorico,
		"  fields:\n    id:\n      index: true\n",
		"  index:\n    window: 6h\n  fields:\n    id:\n      index: true\n", 1))
	stats := leJSON[protocol.Stats](t, srv, "/v1/stats")
	var achou bool
	for _, d := range stats.Datasets {
		if d.History == nil {
			continue
		}
		achou = true
		if d.History.IndexWindow != "6h0m0s" {
			t.Fatalf("IndexWindow = %q, quer a janela declarada", d.History.IndexWindow)
		}
	}
	if !achou {
		t.Fatal("nenhum dataset publicou o bloco history no /v1/stats")
	}
}

// TestAReconstrucaoSobeNosDoisModosComAJanelaCerta é o que faz o débito 22
// fechar no PRODUTO e não só no storage: o laço da §2.4 tem de estar de pé, e
// com a janela certa, em cada dataset aberto — senão o operador declara
// `index: true` num campo já povoado e o campo continua enumerando para sempre.
//
// A spec declara `history.index.window`, e é isso que arma a armadilha que o
// janelaDoDataset existe para fechar: o bloco `history` de um dataset BASE
// descreve a tabela IRMÃ, e traduzi-lo para o base — que é materializado — daria
// uma janela finita que o storage recusa, com razão. Nesse caso o base não subiria
// o laço e sairia o `reconstrucao.janela_recusada` no log; as duas asserções
// abaixo são as duas metades disso.
func TestAReconstrucaoSobeNosDoisModosComAJanelaCerta(t *testing.T) {
	a := novoAmbienteCom(t, blocoPadrao)
	// O `index: true` entra nos DOIS lados: no bloco history (a irmã) e no schema
	// do base. É o que faz este teste medir os dois modos de dataset — a
	// reconstrução vale para ambos (D1), e é só no materializado que a janela tem
	// de ser trocada pela infinita.
	spec := strings.Replace(specComHistorico,
		"  fields:\n    id:\n      index: true\n",
		"  index:\n    window: 6h\n  fields:\n    id:\n      index: true\n", 1)
	spec = strings.Replace(spec, "    severity:\n      type: int64\n",
		"    severity:\n      type: int64\n      index: true\n", 1)
	a.escreveDataset("eventos", spec)
	registro := &registroDoServidor{}
	srv, err := server.Novo(a.carrega(), server.Opcoes{
		Log: logging.New(registro, logging.FormatText, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)

	// O ligaReconstrucao roda dentro do Roda, que é assíncrono — ao contrário do
	// aviso da janela inócua, que sai do Novo. Esperar pelo evento é o que separa
	// "não subiu" de "ainda não subiu".
	linha := esperaLinhaDoLog(t, registro, "reconstrucao.ligada")
	if strings.Contains(registro.String(), "reconstrucao.janela_recusada") {
		t.Fatalf("a reconstrução recusou a janela de algum dataset: a janela do materializado é infinita por "+
			"construção, e o bloco history dele descreve a IRMÃ\n%s", registro.String())
	}
	// Os nomes são conferidos com o delimitador, e não por substring solta:
	// "eventos" é prefixo de "eventos_history", então um Contains cru diria que o
	// base subiu sempre que a irmã tivesse subido — exatamente o caso que o
	// janelaDoDataset pode quebrar.
	for _, nome := range []string{"[eventos ", " eventos_history]"} {
		if !strings.Contains(linha, nome) {
			t.Errorf("o laço de reconstrução não subiu para %q: a fila daquele dataset não é drenada por ninguém, "+
				"e o índice declarado nunca alcança os segmentos já selados\n%s", nome, linha)
		}
	}
}

// esperaLinhaDoLog devolve a linha que carrega o evento nomeado, esperando por
// ela até um prazo. Falha nomeando o evento que faltou: "o evento não apareceu" e
// "apareceu sem o dataset" são falhas diferentes, e um Contains sobre o log
// inteiro as confundiria.
func esperaLinhaDoLog(t *testing.T, registro *registroDoServidor, evento string) string {
	t.Helper()
	prazo := time.Now().Add(30 * time.Second)
	for {
		log := registro.String()
		for _, linha := range strings.Split(log, "\n") {
			if strings.Contains(linha, "event="+evento) {
				return linha
			}
		}
		if time.Now().After(prazo) {
			t.Fatalf("o evento %q não saiu no log:\n%s", evento, log)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// specComJanelaApertada declara uma janela de indexação por BYTES tão pequena
// que ela admite UM segmento selado — o mais novo — e mais nenhum.
//
// Por bytes, e não por tempo, porque o corte tem de valer sem depender do
// relógio: com `maxBytes` abaixo de um bloco, o primeiro candidato entra (a
// janela nunca fica vazia, espelhando o "só sobrou o corrente" da retenção) e o
// segundo já estoura, tanto ANTES de existir `.sidx` para medir quanto depois.
// Uma janela por tempo exigiria envelhecer os records, e o `_writtenAt` sai do
// relógio da ingestão.
const specComJanelaApertada = `version: 1
name: eventos
materialization:
  key:
    field: id
history:
  expireAfterWrite: 7d
  maxBytes: 4GiB
  index:
    maxBytes: 4KiB
  fields:
    id:
      index: true
schema:
  include: ["*"]
  fields:
    id:
      type: string
    severity:
      type: int64
`

// semeiaHistorico enche a tabela IRMÃ pelo pipeline real — Materializer.Apply
// com o histórico ligado —, do mesmo jeito que o `semeia` do dataset base, e
// pela mesma razão: o que os testes medem depende de segmentos selados de
// verdade, com índice escrito pelo writer, e um arquivo montado à mão não
// exercitaria nem a selagem nem a rotação.
//
// A montagem repete a ordem do abreHistorico (dicionário do base para a irmã,
// compilação contra esse mesmo dicionário, o espelho, e só então o LigaHistorico)
// porque é ela que faz os FieldIDs das duas tabelas coincidirem — divergir aqui
// faria o payload da irmã decodificar colunas trocadas, e o teste mediria outra
// coisa. Pela mesma razão o segmentSize da irmã segue a regra do servidor: o
// `history.segmentSize` declarado, ou o global quando ele é omitido.
//
// São DOIS contadores de evento porque a presença do campo indexado é o que
// decide o estado do segmento. Os `comCampo` produzem segmentos que geram .sidx;
// os `semCampo` não produzem posting nenhuma, logo não geram arquivo, logo ficam
// permanentemente PENDENTES (§2.4) — e eles vão por último de propósito, para
// caírem entre os segmentos mais novos, que é onde a janela os mantém dentro.
//
// Devolve o diretório da irmã, que é onde moram os .sidx que os testes sabotam.
func semeiaHistorico(t *testing.T, a *ambiente, nome string, comCampo, semCampo int) string {
	t.Helper()
	cfg := a.carrega()
	spec, err := schema.Load(cfg.SpecDoDataset(nome), cfg.DefaultsDeQuery())
	if err != nil {
		t.Fatal(err)
	}
	base, err := storage.Open(storage.Options{
		Dir:                    cfg.CaminhoDoDataset(nome),
		Dataset:                nome,
		SegmentSize:            cfg.SegmentSize(),
		CamposIndexadosPorNome: schema.CamposComIndice(spec),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := base.Close(); err != nil {
			t.Errorf("Close do base: %v", err)
		}
	}()
	specIrma := spec.DerivaHistorico()
	segmentoDaIrma := cfg.SegmentSize()
	if spec.Historico != nil && spec.Historico.SegmentSize > 0 {
		segmentoDaIrma = spec.Historico.SegmentSize
	}
	dirIrma := cfg.CaminhoDoDataset(specIrma.Name)
	hist, err := storage.Open(storage.Options{
		Dir:                    dirIrma,
		Dataset:                specIrma.Name,
		SegmentSize:            segmentoDaIrma,
		Modo:                   storage.ModoHistorico,
		DictCompartilhado:      base.FieldDict(),
		CamposIndexadosPorNome: schema.CamposComIndice(specIrma),
		CampoDeEscrita:         schema.CampoInstanteDeEscrita,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := hist.Close(); err != nil {
			t.Errorf("Close da irmã: %v", err)
		}
	}()
	compilado, err := spec.Compile(base.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	compiladoIrma, err := specIrma.Compile(base.FieldDict())
	if err != nil {
		t.Fatal(err)
	}
	compilado.Espelha(compiladoIrma)
	mat, err := materializer.New(compilado, base)
	if err != nil {
		t.Fatal(err)
	}
	mat.LigaHistorico(hist, nil)
	aplica := func(ev string) {
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			t.Fatalf("evento %q: %v", ev, r.Err)
		}
	}
	for i := range comCampo {
		aplica(fmt.Sprintf(`{"id":"E-%06d","node":"n%d","severity":%d}`, i, i%3, i%7))
	}
	for i := range semCampo {
		aplica(fmt.Sprintf(`{"id":"S-%06d","severity":%d}`, i, i%4))
	}
	if err := hist.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := base.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return dirIrma
}

// seladosDoDataset devolve os segmentos selados de um dataset em ordem
// crescente — todos os `.dat` menos o último, que é o corrente (o writer sempre
// apenda no de maior id, e este teste não escreve nada depois de o servidor
// subir).
func seladosDoDataset(t *testing.T, dir string) []string {
	t.Helper()
	dats, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(dats)
	if len(dats) < 2 {
		t.Fatalf("a montagem produziu %d segmento(s) em %s: sem selado não há fila", len(dats), dir)
	}
	return dats[:len(dats)-1]
}

// apagaOsIndicesSelados põe o dataset no estado do gatilho 1 da §2.4 — dado
// gravado por um binário que não escrevia índice —, que é o estado em que a fila
// de reconstrução nasce cheia.
func apagaOsIndicesSelados(t *testing.T, dir string) {
	t.Helper()
	sidx, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.sidx"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sidx) == 0 {
		t.Fatal("a montagem não produziu índice selado nenhum: apagar nada deixaria a fila vazia por outro motivo")
	}
	for _, s := range sidx {
		if err := os.Remove(s); err != nil {
			t.Fatal(err)
		}
	}
}

// TestOLacoDeReconstrucaoObedeceAMESMAJanelaDaExpulsao é a rede da restrição 4, e
// ela guarda a DIVERGÊNCIA, não um valor.
//
// A janela entra na assinatura do IniciaReconstrucao para que quem monta o laço
// não possa esquecê-la, mas o compilador não sabe QUAL é a certa: ela tem de ser
// a mesma que a expulsão usa (`AplicaJanela`, dentro da passada de retenção). Se
// as duas discordarem, a que responde "sim" reconstrói o que a outra acabou de
// apagar: a expulsão tira o `.sidx` do segmento fora da janela, a fila — operando
// com uma janela mais larga — o relista por AUSÊNCIA do arquivo, o laço o
// reconstrói, e a passada seguinte o expulsa de novo. O ponto fixo não segura
// nada, e não é para segurar: ele se invalida exatamente quando o arquivo some,
// que é o gatilho *janela alargada*. É um moinho perpétuo, a até 512 MiB de
// leitura por segmento por volta.
//
// O ORÁCULO É O CORTE, e não a chamada: a janela declarada admite UM segmento
// selado, então o laço tem de reconstruir esse e mais nenhum. Passar qualquer
// janela mais larga — a infinita inclusive, que é a mutação óbvia — reconstrói os
// outros também, e os outros são justamente os que a expulsão apagaria.
//
// A espera é por um marcador POSITIVO, não por um prazo: a fila é percorrida em
// ordem crescente de segmento, e o admitido é o mais NOVO, logo o último. Quando
// o `.sidx` dele aparece, quem seria reconstruído a mais já teria sido.
func TestOLacoDeReconstrucaoObedeceAMESMAJanelaDaExpulsao(t *testing.T) {
	a := novoAmbienteComStorage(t, blocoPadrao, "  segmentSize: 8KiB\n")
	a.escreveDataset("eventos", specComJanelaApertada)
	semeiaHistorico(t, a, "eventos", 0, 600)

	cfg := a.carrega()
	dirIrma := cfg.CaminhoDoDataset("eventos_history")
	apagaOsIndicesSelados(t, dirIrma)
	selados := seladosDoDataset(t, dirIrma)
	if len(selados) < 3 {
		t.Fatalf("a montagem produziu %d selado(s): com menos de três não há como distinguir "+
			"\"o laço respeitou a janela\" de \"o laço reconstruiu tudo\"", len(selados))
	}
	admitido := selados[len(selados)-1] // o mais novo é o único que a janela admite

	srv, err := server.Novo(cfg, server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)

	esperaAte(t, func() bool { return existe(indiceDe(admitido)) },
		"o laço não reconstruiu nem o segmento que a janela admite: sem isso a asserção abaixo passaria por vacuidade")
	for _, fora := range selados[:len(selados)-1] {
		if existe(indiceDe(fora)) {
			t.Fatalf("o laço reconstruiu o índice de %s, que está FORA da janela declarada: a expulsão o apaga a cada "+
				"passada de retenção e a fila o relista por ausência — reconstrói, expulsa, reconstrói, para sempre, "+
				"a até 512 MiB de leitura por volta", filepath.Base(fora))
		}
	}
}

func indiceDe(dat string) string { return strings.TrimSuffix(dat, ".dat") + ".sidx" }

func existe(caminho string) bool {
	_, err := os.Stat(caminho)
	return err == nil
}

// esperaAte espera a condição virar verdadeira, e falha nomeando o que não
// aconteceu. O prazo é folgado de propósito: ele existe para o teste não pendurar
// a suíte, não para medir tempo de nada.
func esperaAte(t *testing.T, cond func() bool, oQueFalhou string) {
	t.Helper()
	prazo := time.Now().Add(30 * time.Second)
	for time.Now().Before(prazo) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(oQueFalhou)
}
