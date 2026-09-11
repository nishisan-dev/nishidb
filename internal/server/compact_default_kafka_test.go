package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// TestCompactComAdminNaConfigDefaultELacoOciosoFazTrabalhoReal é o gap de
// cobertura 1 da re-revisão da B5: os outros testes de "config default"
// (mutations_test.go) provam que a rota não fica morta, mas com um dataset
// SEM garbage nenhum — nunca provam TRABALHO de verdade no corpo da
// resposta. Aqui há garbage real, a config é DEFAULT (o laço automático
// sobe), e a asserção central é sobre os NÚMEROS decodificados do JSON
// (SegmentsRetired/BytesRead > 0), não só o status.
//
// # Por que isto é DETERMINÍSTICO por construção, sem gate nenhum
//
// Uma primeira tentativa desta task semeava garbage por fora (como
// seedGarbage) e tentava "pular" a primeira passada do laço deixando
// segSize desconhecido. Não funciona: Store.Open() SEMPRE checkpointa no
// fim da recovery (storage/store.go, "Checkpoint imediato"), então segSize
// já está populado no INSTANTE em que o servidor abre o diretório — o laço
// automático compete pelos candidatos desde o primeiro nanossegundo, e
// quem chega primeiro (ele, rodando em processo, ou esta chamada HTTP,
// que paga uma viagem de rede) não é determinístico.
//
// A saída é usar um dataset KAFKA, vazio até a ingestão aplicar alguma
// coisa — e Run() (server.go) chama turnonCompaction() ANTES de subir as
// goroutines de ingestão, nessa ORDEM FIXA do código-fonte, não por sorte
// de agendador. Isso GARANTE que a primeira passada do laço automático
// aconteça sobre um dataset com ZERO rows (candidatos() não acha nada
// possível de eleger) — ela elege vazio e vai dormir por
// intervaloEntrePassadas (5s, uma constante do pacote storage, não um
// número chutado para este teste). SÓ DEPOIS que o teste produz e espera a
// ingestão aplicar o corpus de garbage é que existe candidato para alguém
// achar — e o laço só vai olhar de novo daqui a 5 segundos, folga
// enorme para um POST /v1/compact.
//
// # Retry curto (B7)
//
// A folga de 5s é grande, mas não infinita: sob pressão de CPU real (suíte
// inteira em paralelo, `-race`, CI compartilhado) o tempo entre
// waitIngestionAplicarPeloMenos retornar e o POST chegar ao servidor pode
// esticar o bastante para flakear uma tentativa seca. O POST ganhou o mesmo
// retry curto e sem sleep dos vizinhos de "config default"
// (TestCompactComAdminNaConfigDefaultFuncionaSemPassadaEmVoo, aqui perto, e
// TestCompactRemotoNaConfigDefaultEventualmenteSai0, internal/cli): a
// determinística continua sendo a estrutura (ordem fixa de Run(), não
// sorte), o retry só absorve o jitter de agendador que a estrutura sozinha
// não cobre.
func TestCompactComAdminNaConfigDefaultELacoOciosoFazTrabalhoReal(t *testing.T) {
	const topic = "b5-compact-default-kafka"
	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(1, topic),
		kfake.NumBrokers(1),
		kfake.ClusterID("b5-compact-default"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]

	a := newEnvWithStorage(t, blockPadrao, "  segmentSize: 8KiB\n  checkpoint:\n    interval: 200ms\n")
	a.writeDataset("d", fmt.Sprintf(specKafkaForCompactDefault, broker, topic))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	produtor, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("produtor: %v", err)
	}
	t.Cleanup(produtor.Close)

	const keys = 60
	const rodadas = 8
	peso := strings.Repeat("p", 300)
	for r := int64(1); r <= rodadas; r++ {
		recs := make([]*kgo.Record, 0, keys)
		for i := 0; i < keys; i++ {
			id := fmt.Sprintf("k-%04d", i)
			value := fmt.Sprintf(`{"id":%q,"seq":%d,"peso":%q}`, id, r, peso)
			recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(id), Value: []byte(value)})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := produtor.ProduceSync(ctx, recs...).FirstErr()
		cancel()
		if err != nil {
			t.Fatalf("produzir a rodada %d: %v", r, err)
		}
	}

	waitIngestionAplicarPeloMenos(t, srv, "d", keys*rodadas, 30*time.Second)

	// Retry curto, sem sleep entre tentativas — o mesmo padrão de
	// TestCompactComAdminNaConfigDefaultFuncionaSemPassadaEmVoo
	// (mutations_test.go) e de TestCompactRemotoNaConfigDefaultEventualmenteSai0
	// (internal/cli). A folga estrutural (o laço automático só volta a olhar
	// candidatos daqui a intervaloEntrePassadas, 5s) já torna uma tentativa
	// seca correta na prática, mas sob pressão de CPU real (suíte inteira em
	// paralelo, `-race`, CI compartilhado) a viagem de rede desta chamada
	// pode esbarrar num 409 se o agendador atrasar o teste o bastante para o
	// laço acordar antes do POST chegar. Se a rota estivesse morta de
	// verdade (o Critical que a revisão da B5 achou), NENHUMA das tentativas
	// teria sucesso, e o teste falha alto no teto em vez de travar.
	const tentativas = 200
	var resp *http.Response
	for i := 0; i < tentativas; i++ {
		resp = postWithAuth(t, srv, "/v1/compact", "", "", "")
		if resp.StatusCode == http.StatusOK {
			break
		}
		fecha(t, resp)
	}
	if resp.StatusCode != http.StatusOK {
		defer fecha(t, resp)
		t.Fatalf("compact: status = %d após %d tentativas imediatas, quer 200 (o laço automático só volta a olhar "+
			"daqui a 5s — folga de sobra para este POST)", resp.StatusCode, tentativas)
	}
	var out protocol.CompactResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("corpo ilegível: %v", err)
	}
	fecha(t, resp)

	if len(out.Datasets) != 1 || out.Datasets[0].Name != "d" {
		t.Fatalf("datasets = %+v, quer [{Name: d}]", out.Datasets)
	}
	ds := out.Datasets[0]
	if ds.SegmentsRetired == 0 || ds.BytesRead == 0 {
		t.Fatalf("SegmentsRetired=%d BytesRead=%d: quer trabalho REAL — a config default (laço automático ligado) "+
			"não pode impedir o compact manual de achar e aposentar candidato nenhum quando o laço está ocioso",
			ds.SegmentsRetired, ds.BytesRead)
	}
	t.Logf("trabalho real via config default: %d segmento(s), %d bytes lidos, %d bytes escritos",
		ds.SegmentsRetired, ds.BytesRead, ds.BytesWritten)
}

// specKafkaForCompactDefault é um dataset Kafka mínimo, no molde do
// e2eSpecYAML (internal/cli/compaction_e2e_test.go): mode merge + sequence
// "greater" fazem cada rodada tornar garbage a anterior inteira sobre o
// MESMO key set — a forma do dataset do §86.
const specKafkaForCompactDefault = `version: 1
name: d
source:
  type: kafka
  kafka:
    enabled: true
    bootstrapServers:
      - %s
    topic: %s
    consumer:
      groupId: b5-compact-default
      autoOffsetReset: earliest
      maxPollRecords: 500
      ingestionQueueSize: 2000
materialization:
  key:
    field: id
  mode: merge
  sequence:
    field: seq
    type: int64
    strategy: greater
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    seq:
      type: int64
    peso:
      type: string
query:
  maxRows: 100000
  timeout: 30s
`

// waitIngestionAplicarPeloMenos bloqueia até Ingestion.Applied alcançar
// alvo para o dataset nomeado, raspando /v1/stats pela porta — nunca por
// dentro do processo. É o mesmo oráculo do e2e do internal/cli
// (waitIngestionAlcancar), adaptado para não precisar decodificar rows: o
// contador de aplicados já basta para saber que o corpus inteiro chegou ao
// writer.
func waitIngestionAplicarPeloMenos(t *testing.T, srv *server.Server, dataset string, target int64, prazo time.Duration) {
	t.Helper()
	limit := time.Now().Add(prazo)
	for {
		resp, err := http.Get("http://" + srv.Endereco() + "/v1/stats")
		if err == nil {
			var e protocol.Stats
			decErr := json.NewDecoder(resp.Body).Decode(&e)
			_ = resp.Body.Close()
			if decErr == nil {
				for _, d := range e.Datasets {
					if d.Name == dataset && d.Ingestion.Applied >= target {
						return
					}
				}
			}
		}
		if time.Now().After(limit) {
			t.Fatalf("a ingestão do dataset %q não alcançou %d eventos aplicados em %v", dataset, target, prazo)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
