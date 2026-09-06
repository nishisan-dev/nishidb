package server_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
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
			if f := falhaDe(t, resp); f.Code != protocol.CodForbidden {
				t.Errorf("código = %q, quer %q", f.Code, protocol.CodForbidden)
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
			if f := falhaDe(t, resp); f.Code != protocol.CodMetodo {
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
	var out protocol.RespostaCheckpoint
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
	var out protocol.RespostaCompact
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
	if f.Code != protocol.CodDatasetDesconhecido {
		t.Fatalf("código = %q, quer %q", f.Code, protocol.CodDatasetDesconhecido)
	}
}

// servidorComDoisDatasets sobe um servidor de verdade com DOIS datasets sem
// Kafka — o suficiente para exercitar "todos os datasets" com mais de um
// elemento de verdade, sem precisar de Kafka nenhum.
func servidorComDoisDatasets(t *testing.T, blocoServer string) *server.Servidor {
	t.Helper()
	a := novoAmbienteCom(t, blocoServer)
	a.escreveDataset("alfa", strings.ReplaceAll(specSemKafka, "%s", "alfa"))
	a.escreveDataset("beta", strings.ReplaceAll(specSemKafka, "%s", "beta"))
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv
}

// TestCheckpointComTodosOsDatasetsRespeitaAOrdemDeterministicaDaSubida é o
// Important 3 da revisão da B5: "todos os datasets" (corpo ausente) com MAIS
// DE UM dataset de verdade, provando a ordem determinística (alfabética,
// config.ListaDatasets) e que os DOIS aparecem na resposta feliz — não só
// um teste de unidade com dublê (mutacoes_internal_test.go), que prova a
// LÓGICA de parar-no-primeiro-erro mas não a fiação real com s.datasets.
func TestCheckpointComTodosOsDatasetsRespeitaAOrdemDeterministicaDaSubida(t *testing.T) {
	srv := servidorComDoisDatasets(t, blocoPadrao)

	resp := postComAuth(t, srv, "/v1/checkpoint", "", "", "")
	if resp.StatusCode != http.StatusOK {
		defer fecha(t, resp)
		t.Fatalf("status = %d, quer 200", resp.StatusCode)
	}
	var out protocol.RespostaCheckpoint
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("corpo ilegível: %v", err)
	}
	fecha(t, resp)

	if len(out.Datasets) != 2 {
		t.Fatalf("datasets = %+v, quer 2", out.Datasets)
	}
	if out.Datasets[0].Name != "alfa" || out.Datasets[1].Name != "beta" {
		t.Fatalf("ordem = [%s, %s], quer [alfa, beta] (ordem alfabética da subida, config.ListaDatasets)",
			out.Datasets[0].Name, out.Datasets[1].Name)
	}
}

// TestCompactComAdminNaConfigDefaultFuncionaSemPassadaEmVoo é o Critical da
// revisão da B5: com a config DEFAULT (storage.compaction ausente ⇒
// enabled: true, o caso majoritário), a rota NÃO pode ficar morta para
// sempre — ela só falha BUSY enquanto uma passada está de fato em voo,
// nunca só porque o laço automático existe (era esse o bug: checar
// compCancela, que fica não-nil da primeira à última hora do laço).
//
// Sem garbage nenhum, a primeira passada automática (imediata, sem cochilo
// antes) é uma eleição vazia que dura microssegundos — a janela ocupada é
// curta demais para valer sleep. O oráculo é tentar de novo NA HORA, sem
// esperar nada entre tentativas: se a rota estivesse morta de verdade (o
// bug que a revisão achou), NENHUMA das tentativas teria sucesso, e o teste
// falharia alto no teto em vez de convergir.
func TestCompactComAdminNaConfigDefaultFuncionaSemPassadaEmVoo(t *testing.T) {
	srv := servidorComAutenticacao(t, 3, "") // storage.compaction ausente = enabled: true (default)

	const tentativas = 200
	var resp *http.Response
	for i := 0; i < tentativas; i++ {
		resp = postComAuth(t, srv, "/v1/compact", "", "chefe", senhaChefeAuth)
		if resp.StatusCode == http.StatusOK {
			break
		}
		fecha(t, resp)
	}
	defer fecha(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d após %d tentativas imediatas: a rota parece permanentemente ocupada na config default", resp.StatusCode, tentativas)
	}
}

// TestCompactDuasChamadasSimultaneasPeloHTTPUmaGanhaUmaRecebe409 prova o 409
// concorrente FIM A FIM pela porta: duas requisições reais, disparadas ao
// mesmo tempo por uma barreira de canal (nunca sleep de corrida), contra um
// dataset com garbage suficiente para a passada real levar dezenas de
// milissegundos (a mesma bancada de TestCompactionSoSobeComAChaveDoOperadorLigada,
// que mede ~117ms para este volume) — folga de sobra para a segunda
// requisição, que chega poucos microssegundos atrás da primeira, encontrar
// a vaga do single-flight ocupada.
//
// storage.compaction.enabled: false isola o teste do laço automático: o que
// se prova aqui é exclusão entre DUAS CHAMADAS MANUAIS — a suíte do storage
// (compact_uma_vez_test.go) já prova essa exclusão de forma exaustiva com
// gate determinístico; o que falta provar aqui é que ela sobrevive à
// viagem pela porta HTTP, com o servidor de verdade decidindo a resposta.
func TestCompactDuasChamadasSimultaneasPeloHTTPUmaGanhaUmaRecebe409(t *testing.T) {
	srv, _, _, _ := bancadaDeCompaction(t, "false")

	partida := make(chan struct{})
	resultados := make(chan int, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-partida
			resp, err := http.Post("http://"+srv.Endereco()+"/v1/compact", "", nil)
			if err != nil {
				resultados <- -1
				return
			}
			defer func() { _ = resp.Body.Close() }()
			resultados <- resp.StatusCode
		}()
	}
	close(partida) // as duas goroutines partem juntas

	a, b := <-resultados, <-resultados
	oks, conflitos := 0, 0
	for _, status := range []int{a, b} {
		switch status {
		case http.StatusOK:
			oks++
		case http.StatusConflict:
			conflitos++
		}
	}
	if oks != 1 || conflitos != 1 {
		t.Fatalf("status = %d e %d, quer exatamente um 200 (COMPACTION_BUSY nunca deveria recusar as DUAS, "+
			"nem deixar as DUAS varrerem ao mesmo tempo) e um 409", a, b)
	}
}
