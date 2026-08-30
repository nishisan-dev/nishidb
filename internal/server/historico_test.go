package server_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/server"
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
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
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
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
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
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
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
	meta := leJSON[protocolo.Metadados](t, srv, "/v1/metadata")
	if len(meta.Datasets) != 1 {
		t.Fatalf("%d datasets, quer 1: %+v", len(meta.Datasets), meta.Datasets)
	}
}

// O /v1/stats e o /metrics precisam responder com a tabela irmã de pé: ela
// não tem materializer — quem escreve nela é o do dataset base —, e o laço
// que percorre todos os datasets passa por ela.
func TestStatsRespondeComATabelaIrma(t *testing.T) {
	srv := servidorComHistorico(t, specComHistorico)
	st := leJSON[protocolo.Estatisticas](t, srv, "/v1/stats")
	var base, irma *protocolo.DatasetStats
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
