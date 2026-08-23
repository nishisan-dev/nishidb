package server_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// postComAuth faz o POST com corpo e Basic OPCIONAIS (usuario == "" significa
// SEM header Authorization nenhum; corpo == "" significa SEM corpo nenhum —
// não "{}" — para exercitar a leitura tolerante a io.EOF de
// leRequisicaoDataset).
func postComAuth(t *testing.T, srv *server.Servidor, caminho, corpo, usuario, senha string) *http.Response {
	t.Helper()
	var body *strings.Reader
	if corpo == "" {
		body = strings.NewReader("")
	} else {
		body = strings.NewReader(corpo)
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+caminho, body)
	if err != nil {
		t.Fatal(err)
	}
	if usuario != "" {
		req.SetBasicAuth(usuario, senha)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// contadorDeCheckpoint lê nishidb_checkpoint_duration_seconds_count do
// scrape — é um CONTADOR, e por isso o oráculo certo para "o checkpoint
// aconteceu de verdade": ao contrário do gauge de último sucesso (segundos
// desde a época, granularidade de 1s), ele incrementa de forma determinística
// a cada chamada, mesmo dentro do mesmo segundo de relógio.
func contadorDeCheckpoint(t *testing.T, corpo, dataset string) int64 {
	t.Helper()
	prefixo := metrics.MetCheckpointDurationSeconds + `_count{dataset="` + dataset + `"} `
	for _, linha := range strings.Split(corpo, "\n") {
		if v, ok := strings.CutPrefix(linha, prefixo); ok {
			n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				t.Fatalf("contador de checkpoint ilegível: %q: %v", linha, err)
			}
			return int64(n)
		}
	}
	t.Fatalf("nishidb_checkpoint_duration_seconds_count não apareceu no scrape para o dataset %q:\n%s", dataset, corpo)
	return 0
}

// As duas rotas mutantes exigem credencial — nenhuma responde anônima, nem
// no plano exposto (o loopback, que é READ-only por construção, tem suíte
// própria em loopback_test.go).
func TestMutantesExigemAutenticacao(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	for _, caminho := range []string{"/v1/compact", "/v1/checkpoint"} {
		t.Run(caminho, func(t *testing.T) {
			resp := postComAuth(t, srv, caminho, "", "", "")
			defer fecha(t, resp)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("status = %d, quer 401", resp.StatusCode)
			}
		})
	}
}

// A primeira rota ADMIN de verdade em produção: o teste que a B3 deixou
// pronto com um handler FAKE (autenticacao_internal_test.go,
// TestMecanismoDeRotaExigePapelComHandlerDeTeste) agora tem par com handler
// de produção. papel READ não alcança rota ADMIN — 403, nunca 200 nem 401
// (a credencial É válida, só o papel não basta).
func TestMutantesRecusamPapelREAD(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	for _, caminho := range []string{"/v1/compact", "/v1/checkpoint"} {
		t.Run(caminho, func(t *testing.T) {
			resp := postComAuth(t, srv, caminho, "", "leitor", senhaLeitorAuth)
			status := resp.StatusCode // falhaDe fecha o corpo; lido antes
			if f := falhaDe(t, resp); f.Code != protocolo.CodForbidden {
				t.Errorf("código = %q, quer %q", f.Code, protocolo.CodForbidden)
			}
			if status != http.StatusForbidden {
				t.Errorf("status = %d, quer 403", status)
			}
		})
	}
}

// GET nas duas rotas mutantes é 405 com o CONTRATO CorpoErro — registradas
// SEM método no padrão, exatamente como as rotas de leitura do §67
// (TestRotasDeLeituraRecusamMetodoErrado, documentos_test.go), conferindo o
// método por dentro do handler em vez de deixar o mux gerar o 405 cru em
// texto puro.
func TestMutantesRecusamMetodoErrado(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)

	for _, caminho := range []string{"/v1/compact", "/v1/checkpoint"} {
		t.Run(caminho, func(t *testing.T) {
			resp, err := http.Get("http://" + srv.Endereco() + caminho)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusMethodNotAllowed {
				t.Fatalf("GET %s = %d, quer 405", caminho, resp.StatusCode)
			}
			if f := falhaDe(t, resp); f.Code != protocolo.CodMetodo {
				t.Fatalf("GET %s: código = %q", caminho, f.Code)
			}
		})
	}
}

// POST /v1/checkpoint com ADMIN faz um checkpoint DE VERDADE: o contador do
// §92 avança, e a resposta nomeia o dataset com a duração observada. Corpo
// AUSENTE (não "{}") é o caminho "todos os datasets" — aqui há só um, mas é
// esse caminho que o teste exercita.
func TestCheckpointComAdminAvancaOContadorEDevolveODataset(t *testing.T) {
	srv := servidorComAutenticacao(t, 3, "")

	antes := contadorDeCheckpoint(t, pegaCorpoComAuth(t, srv, "/metrics", "chefe", senhaChefeAuth), "d")

	resp := postComAuth(t, srv, "/v1/checkpoint", "", "chefe", senhaChefeAuth)
	if resp.StatusCode != http.StatusOK {
		defer fecha(t, resp)
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
	var out protocolo.RespostaCheckpoint
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("corpo ilegível: %v", err)
	}
	fecha(t, resp)

	if len(out.Datasets) != 1 || out.Datasets[0].Name != "d" {
		t.Fatalf("datasets = %+v, quer [{Name: d}]", out.Datasets)
	}
	if out.Datasets[0].DurationMs < 0 {
		t.Errorf("durationMs = %d, quer >= 0", out.Datasets[0].DurationMs)
	}

	depois := contadorDeCheckpoint(t, pegaCorpoComAuth(t, srv, "/metrics", "chefe", senhaChefeAuth), "d")
	if depois != antes+1 {
		t.Errorf("nishidb_checkpoint_duration_seconds_count foi de %d para %d, queria +1: "+
			"o checkpoint manual não é diferenciável de um no-op", antes, depois)
	}
}

// POST /v1/compact funciona com storage.compaction.enabled: false — o caso
// de uso central do modo manual. Sem garbage nenhum (dataset recém-seedado,
// sem update), a eleição não acha candidato e a resposta sai com os campos
// ZERADOS e status 200 — não é erro, é "nada para compactar agora".
func TestCompactComAdminEEnabledFalseFunciona(t *testing.T) {
	// servidorComAutenticacao não tem parâmetro de storageExtra (só de
	// maisConfig, que entra DEPOIS do bloco `server:`, e um `storage:` ali
	// seria uma SEGUNDA chave de topo colidindo com a que a bancada já
	// escreve) — desce ao helper de baixo, que aceita as duas partes
	// separadas, para acrescentar `compaction: enabled: false` DENTRO do
	// bloco `storage:` existente.
	t.Setenv(varLeitorAuth, senhaLeitorAuth)
	t.Setenv(varChefeAuth, senhaChefeAuth)
	srv, _ := servidorComDadosDiretorioEStorage(t, 3, blocoAutenticacao(""), "  compaction:\n    enabled: false\n")

	resp := postComAuth(t, srv, "/v1/compact", "", "chefe", senhaChefeAuth)
	if resp.StatusCode != http.StatusOK {
		defer fecha(t, resp)
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
	var out protocolo.RespostaCompact
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("corpo ilegível: %v", err)
	}
	fecha(t, resp)

	if len(out.Datasets) != 1 || out.Datasets[0].Name != "d" {
		t.Fatalf("datasets = %+v, quer [{Name: d}]", out.Datasets)
	}
	ds := out.Datasets[0]
	if ds.SegmentsRetired != 0 || ds.BytesRead != 0 || ds.BytesWritten != 0 {
		t.Errorf("dataset recém-seedado sem update não tem candidato: %+v, queria os três campos zerados", ds)
	}
}

// O filtro {"dataset":"..."} restringe a UM dataset — o mesmo contrato de
// ?dataset= em /v1/metadata: nome desconhecido é 404 com UNKNOWN_DATASET, e
// nome presente restringe sem tocar nos demais.
func TestCheckpointFiltraPorDatasetOuRecusaDesconhecido(t *testing.T) {
	srv := servidorComAutenticacao(t, 1, "")

	resp := postComAuth(t, srv, "/v1/checkpoint", `{"dataset":"fantasma"}`, "chefe", senhaChefeAuth)
	status := resp.StatusCode // falhaDe fecha o corpo; lido antes
	f := falhaDe(t, resp)
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, quer 404", status)
	}
	if f.Code != protocolo.CodDatasetDesconhecido {
		t.Fatalf("código = %q, quer %q", f.Code, protocolo.CodDatasetDesconhecido)
	}
}

// 409 concorrente, determinístico e SEM sleep de corrida: com o laço
// automático de compaction de pé (o default — storage.compaction.enabled
// ausente vale true), compCancela é gravado de forma SÍNCRONA por
// IniciaCompaction antes mesmo de a goroutine rodar um passo — então
// CompactaUmaVez vê "em voo" e recusa NA HORA, sem depender de nenhuma
// corrida de agendador. É a metade "laço em voo + POST" que o brief oferece
// como alternativa à corrida de dois POSTs simultâneos.
func TestCompactConcorrenteComOLacoAutomaticoDevolve409(t *testing.T) {
	srv := servidorComAutenticacao(t, 3, "") // storage.compaction ausente = enabled: true (default)

	resp := postComAuth(t, srv, "/v1/compact", "", "chefe", senhaChefeAuth)
	status := resp.StatusCode // falhaDe fecha o corpo; lido antes
	f := falhaDe(t, resp)
	if status != http.StatusConflict {
		t.Fatalf("status = %d, quer 409", status)
	}
	if f.Code != protocolo.CodCompactionBusy {
		t.Fatalf("código = %q, quer %q", f.Code, protocolo.CodCompactionBusy)
	}
}
