package server_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/twmb/franz-go/pkg/kfake"
)

// resyncMaster sobe o master replicado com o destino `cdc` num kfake. block é
// o bloco `server:` (com ou sem autenticação); accounts são criadas antes da
// subida. Devolve o servidor já no Run e o broker.
func resyncMaster(t *testing.T, block string, accounts ...account) (*server.Server, string) {
	t.Helper()
	srv, broker, _ := resyncMasterEnv(t, block, true, accounts...)
	return srv, broker
}

// resyncMasterEnv é resyncMaster com o ambiente devolvido e o Run opcional:
// sem Run, o publicador existe e nunca começa, e nada consome nem grava o
// .ckpt por conta própria.
func resyncMasterEnv(t *testing.T, block string, running bool, accounts ...account) (*server.Server, string, *env) {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]
	a := newEnvWithStorage(t, block, "")
	a.writeDataset("events", fmt.Sprintf(replicatedDataset, broker))
	appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"})
	if len(accounts) > 0 {
		provisionUsers(t, a, accounts...)
	}
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if running {
		startWebhookServer(t, srv)
	} else {
		t.Cleanup(func() {
			if err := srv.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
	return srv, broker, a
}

// resyncCounterPath é o <destino>.resync do dataset `events`.
func resyncCounterPath(a *env, destination string) string {
	return filepath.Join(replicationkafka.CheckpointDir(a.cfg.DatasetRootDir("events")), destination+".resync")
}

// readResyncCounter lê o contador do .resync pelo layout do disco (magic
// "NDBR", versão, 3 reservados, contador u64 LE, crc32c): 0 com o arquivo
// ausente.
func readResyncCounter(t *testing.T, path string) uint64 {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 20 || string(b[:4]) != "NDBR" {
		t.Fatalf(".resync com layout inesperado: %x", b)
	}
	return binary.LittleEndian.Uint64(b[8:16])
}

// headerProbe roda onHeader no instante do WriteHeader: é ali que o 202 é
// decidido, e é ali que o pedido já tem de estar no disco.
type headerProbe struct {
	*httptest.ResponseRecorder
	onHeader func(code int)
}

func (p *headerProbe) WriteHeader(code int) {
	p.onHeader(code)
	p.ResponseRecorder.WriteHeader(code)
}

// resyncPath é a rota do snapshot sob demanda com os dois parâmetros.
func resyncPath(dataset, destination string) string {
	return "/v1/replication/resync?dataset=" + dataset + "&destination=" + destination
}

// O pedido de snapshot republica o dataset inteiro no tópico: é ADMIN, como
// as outras rotas de replicação. A recusa ao anônimo e ao READ vem pareada
// com o ADMIN aceito, senão uma rota que recusasse todo mundo passaria.
func TestResyncRouteRequiresAdmin(t *testing.T) {
	srv, _ := resyncMaster(t, blockAuth(""), authAccounts()...)
	path := resyncPath("events", "cdc")

	anon := postWithAuth(t, srv, path, "", "", "")
	status := anon.StatusCode
	fecha(t, anon)
	if status != http.StatusUnauthorized {
		t.Fatalf("anônimo: status = %d, quero 401", status)
	}
	reader := postWithAuth(t, srv, path, "", "leitor", passwordReaderAuth)
	status = reader.StatusCode
	if f := failOf(t, reader); f.Code != protocol.CodeForbidden {
		t.Fatalf("leitor: código = %q, quero %q", f.Code, protocol.CodeForbidden)
	}
	if status != http.StatusForbidden {
		t.Fatalf("leitor: status = %d, quero 403", status)
	}
	admin := postWithAuth(t, srv, path, "", "chefe", passwordChefeAuth)
	defer fecha(t, admin)
	if admin.StatusCode != http.StatusAccepted {
		t.Fatalf("ADMIN: status = %d, quero 202", admin.StatusCode)
	}
}

// O 202 é o pedido gravado, e o publicador o atende com um snapshot cujo
// `reset` sai com o motivo `requested`, contado na série do motivo. O corpo é
// o literal do contrato.
func TestResyncRouteTriggersRequestedReset(t *testing.T) {
	srv, broker := resyncMaster(t, blockPadrao)
	waitFollowing(t, srv, "events", 1)

	resp := postWithAuth(t, srv, resyncPath("events", "cdc"), "", "", "")
	body, err := io.ReadAll(resp.Body)
	fecha(t, resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, quero 202: %s", resp.StatusCode, body)
	}
	if got, want := strings.TrimSpace(string(body)), `{"dataset":"events","destination":"cdc","requested":true,"state":"following"}`; got != want {
		t.Fatalf("corpo = %s, quero %s", got, want)
	}

	recs := readWholeTopic(t, broker, "cdc", func(r []topicRecord) bool {
		for _, rec := range r {
			if rec.op() == "reset" && rec.env["reason"] == "requested" {
				return true
			}
		}
		return false
	})
	requested := 0
	for _, rec := range recs {
		if rec.op() == "reset" && rec.env["reason"] == "requested" {
			requested++
		}
	}
	// Um reset por partição: o tópico do teste tem três.
	if requested != 3 {
		t.Fatalf("%d resets com o motivo requested, quero 3 (um por partição)", requested)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, "events")
		if d := rep.Delivery[0]; d.Snapshots["requested"] == 1 && d.State == "following" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("o snapshot pedido não foi contado: %+v", rep.Delivery[0])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Destino que não é um `replication.targets` do dataset, dataset sem feed e
// pedido sem os dois parâmetros são recusados, cada um com a MENSAGEM própria:
// o código UNKNOWN_DATASET é o mesmo das duas primeiras, e só a mensagem
// distingue a recusa do destino da recusa do dataset.
func TestResyncRouteRejectsUnknownDestination(t *testing.T) {
	srv, _ := resyncMaster(t, blockPadrao)
	cases := []struct {
		name, path string
		status     int
		code, msg  string
	}{
		{"destino", resyncPath("events", "nope"), http.StatusNotFound, protocol.CodeUnknownDataset,
			`destination "nope" is not a Kafka replication target of dataset "events"`},
		{"dataset", resyncPath("nope", "cdc"), http.StatusNotFound, protocol.CodeUnknownDataset,
			"replication is not enabled for this dataset"},
		{"sem destino", "/v1/replication/resync?dataset=events", http.StatusBadRequest, protocol.CodeBadRequest,
			"dataset and destination are required"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := postWithAuth(t, srv, c.path, "", "", "")
			status := resp.StatusCode
			f := failOf(t, resp)
			if status != c.status || f.Code != c.code || f.Message != c.msg {
				t.Fatalf("status %d, código %q, mensagem %q; quero %d, %q, %q", status, f.Code, f.Message, c.status, c.code, c.msg)
			}
		})
	}
	// O GET é recusado pelo método, e não atendido como pedido.
	resp, err := http.Get("http://" + srv.Endereco() + resyncPath("events", "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	status := resp.StatusCode
	if f := failOf(t, resp); status != http.StatusMethodNotAllowed || f.Code != protocol.CodeMethod {
		t.Fatalf("GET: status %d, código %q; quero 405, %q", status, f.Code, protocol.CodeMethod)
	}
}

// O 202 só sai com o pedido JÁ no disco: no instante do WriteHeader o
// contador do .resync tem o pedido. O servidor não roda (o publicador nunca
// começa), então nada além da rota mexe no arquivo; o segundo pedido sobe o
// contador de novo. Sem .ckpt gravado, o checkpointCursor da
// `_meta.replication` é NULL, e não 0.
func TestResyncRouteRecordsTheRequestBeforeThe202(t *testing.T) {
	srv, _, a := resyncMasterEnv(t, blockPadrao, false)
	path := resyncCounterPath(a, "cdc")
	for want := uint64(1); want <= 2; want++ {
		atHeader := uint64(0)
		rec := &headerProbe{ResponseRecorder: httptest.NewRecorder(), onHeader: func(int) { atHeader = readResyncCounter(t, path) }}
		srv.HandleReplicationResyncForTest(rec, httptest.NewRequest(http.MethodPost, resyncPath("events", "cdc"), nil))
		if rec.Code != http.StatusAccepted {
			t.Fatalf("pedido %d: status = %d, quero 202: %s", want, rec.Code, rec.Body.String())
		}
		if atHeader != want {
			t.Fatalf("pedido %d: o contador no instante do 202 era %d, quero %d — o 202 saiu antes da gravação", want, atHeader, want)
		}
		if got, wantBody := strings.TrimSpace(rec.Body.String()), `{"dataset":"events","destination":"cdc","requested":true,"state":"snapshotting"}`; got != wantBody {
			t.Fatalf("corpo = %s, quero %s", got, wantBody)
		}
	}
	rows, err := srv.QueryRowsForTest("events", "SELECT id, checkpointCursor FROM _meta.replication")
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	var row map[string]any
	if err := json.Unmarshal([]byte(rows[0]), &row); err != nil {
		t.Fatal(err)
	}
	if v := row["checkpointCursor"]; v != nil {
		t.Fatalf("checkpointCursor = %v sem .ckpt gravado; quero NULL", v)
	}
}

// A gravação do pedido que falha responde 500 com a mensagem própria, e nada
// fica pedido: nenhum .resync no disco.
func TestResyncRouteAnswers500WhenTheRequestCannotBeRecorded(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root escreve em diretório sem permissão")
	}
	srv, _, a := resyncMasterEnv(t, blockPadrao, false)
	path := resyncCounterPath(a, "cdc")
	// O diretório mais fundo que já existe fica sem escrita: o do publicador,
	// se já existe, senão o da replicação, onde ele seria criado.
	locked := filepath.Dir(path)
	if _, err := os.Stat(locked); errors.Is(err, os.ErrNotExist) {
		locked = filepath.Dir(locked)
	}
	if err := os.Chmod(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	rec := httptest.NewRecorder()
	srv.HandleReplicationResyncForTest(rec, httptest.NewRequest(http.MethodPost, resyncPath("events", "cdc"), nil))
	var body protocol.ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("corpo ilegível (%d): %s", rec.Code, rec.Body.String())
	}
	if rec.Code != http.StatusInternalServerError || body.Error.Code != protocol.CodeInternal ||
		!strings.HasPrefix(body.Error.Message, "the snapshot request could not be recorded: ") {
		t.Fatalf("status %d, %+v; quero 500 INTERNAL com a mensagem própria", rec.Code, body.Error)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a recusa deixou o .resync no disco: %v", err)
	}
}

// Com o publicador parado (failed: a entrada sem envelope é bug), o pedido é
// aceito e fica gravado para a próxima subida, e o 202 diz o estado: é ele
// que avisa o operador de que o `reset` não vem agora.
func TestResyncRouteRecordsTheRequestOfAFailedPublisher(t *testing.T) {
	srv, _, a := resyncMasterEnv(t, blockPadrao, true)
	waitFollowing(t, srv, "events", 1)
	srv.PublishToFeedForTest("events", replication.Schema, types.Record{RowID: 99, Key: []byte("s"), Payload: []byte("x")})
	waitPublisherState(t, srv, "events", "failed")

	resp := postWithAuth(t, srv, resyncPath("events", "cdc"), "", "", "")
	body, err := io.ReadAll(resp.Body)
	fecha(t, resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, quero 202: %s", resp.StatusCode, body)
	}
	if got, want := strings.TrimSpace(string(body)), `{"dataset":"events","destination":"cdc","requested":true,"state":"failed"}`; got != want {
		t.Fatalf("corpo = %s, quero %s", got, want)
	}
	if n := readResyncCounter(t, resyncCounterPath(a, "cdc")); n != 1 {
		t.Fatalf("contador do .resync = %d, quero 1: o pedido do publicador parado tem de ficar para a próxima subida", n)
	}
}
