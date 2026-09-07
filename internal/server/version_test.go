package server_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// comVersaoDeTeste carimba as três vars do buildinfo com valores fixos e
// devolve a limpeza — o piloto em produção existe para correlacionar o que
// está no ar com um commit, e as duas superfícies aqui (log de subida e
// /v1/stats) são as que a issue #11 cobra primeiro.
func comVersaoDeTeste(t *testing.T) {
	t.Helper()
	v, c, d := buildinfo.Version, buildinfo.Commit, buildinfo.Date
	buildinfo.Version, buildinfo.Commit, buildinfo.Date = "v9.9.9", "cafefeed1234", "2026-01-02T03:04:05Z"
	t.Cleanup(func() { buildinfo.Version, buildinfo.Commit, buildinfo.Date = v, c, d })
}

// TestSubidaLogaAVersaoDoBinario cobre o evento de subida do §51: as
// consultas ficam prontas ANTES da ingestão, e é esse o instante — hoje
// "query.pronta" — que carrega a versão para quem lê o log de produção.
//
// A leitura do buffer é em LAÇO, não uma checagem única após a HTTP
// responder: o listener já aceita conexão antes de a goroutine da Serve
// terminar de subir (Novo faz o net.Listen, Roda só chama Serve), então nada
// garante que a linha "query.pronta" já foi escrita no instante em que o
// /v1/health responde — checar uma vez seria uma corrida disfarçada de
// asserção.
func TestSubidaLogaAVersaoDoBinario(t *testing.T) {
	comVersaoDeTeste(t)

	a := novoAmbiente(t)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	registro := &registroDoServidor{}
	srv, err := server.Novo(a.carrega(), server.Opcoes{
		Log: logging.New(registro, logging.FormatText, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)

	prazo := time.Now().Add(5 * time.Second)
	for !strings.Contains(registro.String(), "query.pronta") {
		if time.Now().After(prazo) {
			t.Fatalf("a subida não registrou query.pronta a tempo:\n%s", registro.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(registro.String(), buildinfo.String()) {
		t.Errorf("o log de subida não traz a versão do binário (%s):\n%s", buildinfo.String(), registro.String())
	}
}

// TestStatsPublicaAVersaoDoBinario é o campo que a Task A4 vai consumir para
// confirmar, depois de um rollout, que o binário no ar é mesmo o novo — o
// caminho JSON é .process.version.
func TestStatsPublicaAVersaoDoBinario(t *testing.T) {
	comVersaoDeTeste(t)

	srv := servidorComDados(t, 1, blocoPadrao)
	e := leJSON[protocol.Stats](t, srv, "/v1/stats")
	if e.Process.Version != buildinfo.String() {
		t.Fatalf("process.version = %q, quer %q", e.Process.Version, buildinfo.String())
	}
}

// Servidor antigo ou binário sem ldflags não pode publicar campo vazio: o
// contrato aditivo do protocol.Stats prevê o valor de fallback do
// buildinfo, nunca string vazia.
func TestStatsNuncaPublicaVersaoVazia(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)
	e := leJSON[protocol.Stats](t, srv, "/v1/stats")
	if e.Process.Version == "" {
		t.Fatal("process.version veio vazio — buildinfo.String() nunca devolve string vazia")
	}
}
