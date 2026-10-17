package server_test

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// statusProxy fica entre o follower e o master e conta, por rota, os status
// das respostas que o master deu. É a resposta, e não o pedido, que prova o
// que o follower recebeu.
type statusProxy struct {
	mu       sync.Mutex
	statuses map[string]map[int]int
	srv      *httptest.Server
}

func newStatusProxy(t *testing.T, target string) *statusProxy {
	t.Helper()
	u, err := url.Parse("http://" + target)
	if err != nil {
		t.Fatal(err)
	}
	p := &statusProxy{statuses: map[string]map[int]int{}}
	rp := httputil.NewSingleHostReverseProxy(u)
	// O master fora é parte do cenário, e não ruído a imprimir.
	rp.ErrorLog = log.New(io.Discard, "", 0)
	rp.ModifyResponse = func(res *http.Response) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		route := res.Request.URL.Path
		if p.statuses[route] == nil {
			p.statuses[route] = map[int]int{}
		}
		p.statuses[route][res.StatusCode]++
		return nil
	}
	p.srv = httptest.NewServer(rp)
	t.Cleanup(p.srv.Close)
	return p
}

// count devolve quantas respostas da rota saíram com o status.
func (p *statusProxy) count(route string, status int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statuses[route][status]
}

// String é o retrato das contagens, para a mensagem de falha.
func (p *statusProxy) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return fmt.Sprint(p.statuses)
}

// reset zera as contagens: o que vale é o que acontece depois dele.
func (p *statusProxy) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statuses = map[string]map[int]int{}
}

// copyDir copia os arquivos regulares de src para dst (criado).
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
}

// failListOffsetsOf faz o broker recusar, com UNKNOWN_SERVER_ERROR, a
// listagem de offsets do tópico enquanto failing estiver ligado: é a leitura
// do topo do tópico da conferência de rewind que falha, e só ela (a ingestão
// do `src` lista offsets de outro tópico).
func failListOffsetsOf(cluster *kfake.Cluster, topic string, failing *atomic.Bool) {
	cluster.ControlKey(int16(kmsg.ListOffsets), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		lr := req.(*kmsg.ListOffsetsRequest)
		hit := false
		for _, rt := range lr.Topics {
			hit = hit || rt.Topic == topic
		}
		if !failing.Load() || !hit {
			return nil, nil, false
		}
		resp := kmsg.NewPtrListOffsetsResponse()
		resp.SetVersion(req.GetVersion())
		for _, rt := range lr.Topics {
			out := kmsg.NewListOffsetsResponseTopic()
			out.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				part := kmsg.NewListOffsetsResponseTopicPartition()
				part.Partition = rp.Partition
				part.ErrorCode = kerr.UnknownServerError.Code
				out.Partitions = append(out.Partitions, part)
			}
			resp.Topics = append(resp.Topics, out)
		}
		return resp, nil, true
	})
}

// O backup restaurado de um master COM destino Kafka, com um follower HTTP
// que já tinha aplicado sequences acima da restauração. Enquanto a
// conferência de rewind do publicador não termina (aqui, com a leitura do
// topo do tópico falhando), o master responde 503 no lote e no snapshot, e o
// follower não avança: sem isso, o feed restaurado na MESMA geração passaria
// do cursor do follower e lhe entregaria sequences reusadas como
// continuação. O cursor continua respondendo. Quando a conferência termina em
// rewind, a geração troca, e o follower refaz o snapshot nela.
func TestFollowerWaitsForTheRewindCheckOfARestoredMaster(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(1, "cdc"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]
	var failing atomic.Bool
	failListOffsetsOf(cluster, "cdc", &failing)

	m := newEnv(t)
	m.writeDataset("events", diskFeedSpec{name: "events", source: kafkaSource(broker),
		replication: masterReplication + "  targets: [cdc]\n"}.yaml())
	appendKafkaTargets(t, m, broker, [2]string{"cdc", "cdc"})

	// Primeira vida: e1 e e2, o follower em dia, encerramento limpo e backup.
	master, _, stopMaster := startDiskFeedServer(t, m, nil)
	produceTo(t, broker, "src", `{"id":"e1","status":"open"}`, `{"id":"e2","status":"open"}`)
	waitProduced(t, master, "events", 2)
	proxy := newStatusProxy(t, master.Endereco())
	follower, _, _ := startDiskFeedServer(t, followerEnv(t, proxy.srv.URL, ""), nil)
	waitFollower(t, follower, replication.ReplicaFollowing, 2)
	epoch := feedStatus(t, master, "events").Epoch
	stopMaster()
	data := filepath.Join(m.dir, "dados")
	backup := filepath.Join(t.TempDir(), "dados")
	copyDir(t, data, backup)

	// Segunda vida: retoma limpa, e3 e e4 chegam ao follower e ao tópico.
	second, _, stopSecond := startDiskFeedServer(t, m, nil)
	produceTo(t, broker, "src", `{"id":"e3","status":"open"}`, `{"id":"e4","status":"open"}`)
	waitFollower(t, follower, replication.ReplicaFollowing, 4)
	waitPublisherCursor(t, second, "events", 4)
	stopSecond()
	if err := os.RemoveAll(data); err != nil {
		t.Fatal(err)
	}
	copyDir(t, backup, data)

	// Terceira vida, sobre o backup: a leitura do topo do tópico falha, e a
	// ingestão leva o feed restaurado ALÉM do cursor do follower.
	failing.Store(true)
	proxy.reset()
	restored, _, _ := startDiskFeedServer(t, m, nil)
	if e := feedStatus(t, restored, "events").Epoch; e != epoch {
		t.Fatalf("pré-condição: o backup reabriu na epoch %s, quero a mesma %s (retomada limpa)", e, epoch)
	}
	produceTo(t, broker, "src", `{"id":"e5","status":"open"}`)
	waitProduced(t, restored, "events", 5)
	waitTopicReadFailures(t, restored, "events")
	deadline := time.Now().Add(30 * time.Second)
	for proxy.count("/v1/replication/batch", http.StatusServiceUnavailable) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("o master restaurado não respondeu 503 ao follower com a conferência pendente: %v", proxy)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n := proxy.count("/v1/replication/batch", http.StatusOK); n != 0 {
		t.Fatalf("o master restaurado entregou %d lote(s) ao follower antes da conferência de rewind", n)
	}
	if state, e, applied := follower.FollowerStateForTest("events"); applied != 4 || e != epoch || state == replication.ReplicaFollowing {
		t.Fatalf("o follower andou com a conferência pendente: %s/%s/%d, quero desconectado em %s/4", state, e, applied, epoch)
	}
	if n := proxy.count("/v1/replication/snapshot", http.StatusOK); n != 0 {
		t.Fatalf("o 503 mandou o follower ao snapshot (%d) em vez de esperar", n)
	}
	status, body := getStatus(t, restored, "/v1/replication/batch?dataset=events&epoch="+epoch+"&after=4")
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "rewind check") {
		t.Fatalf("lote com a conferência pendente: %d %s", status, body)
	}
	status, body = getStatus(t, restored, "/v1/replication/snapshot?dataset=events")
	if status != http.StatusServiceUnavailable || !strings.Contains(body, "rewind check") {
		t.Fatalf("snapshot com a conferência pendente: %d %s", status, body)
	}
	if status, body := getStatus(t, restored, "/v1/replication/cursor?dataset=events"); status != http.StatusOK {
		t.Fatalf("o cursor parou de responder com a conferência pendente: %d %s", status, body)
	}

	// A leitura volta: rewind, geração nova, e o follower a segue pelo
	// snapshot.
	failing.Store(false)
	deadline = time.Now().Add(30 * time.Second)
	for {
		state, e, _ := follower.FollowerStateForTest("events")
		if state == replication.ReplicaFollowing && e != epoch {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("o follower não seguiu a geração nova do rewind: %s/%s", state, e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e := feedStatus(t, restored, "events").Epoch; e == epoch {
		t.Fatalf("o rewind não trocou a geração do master: %s", e)
	}
	if rep := replicationOf(t, restored, "events"); rep.Delivery[0].Snapshots["rewind"] != 1 {
		t.Fatalf("o publicador não registrou o rewind: %+v", rep.Delivery[0])
	}
	if proxy.count("/v1/replication/snapshot", http.StatusOK) == 0 {
		t.Fatal("o follower seguiu a geração nova sem snapshot")
	}
	rows, err := follower.QueryRowsForTest("events", "SELECT id FROM events WHERE id = 'e5'")
	if err != nil || len(rows) != 1 {
		t.Fatalf("o follower não tem o evento do master restaurado: %v %v", rows, err)
	}
}

// waitPublisherCursor espera o cursor do único publicador do dataset chegar a
// want: o tópico já tem a sequence.
func waitPublisherCursor(t *testing.T, srv *server.Server, name string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, name)
		if rep != nil && len(rep.Delivery) == 1 && rep.Delivery[0].Cursor >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("o publicador não chegou ao cursor %d: %+v", want, rep)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitTopicReadFailures espera o publicador contar uma leitura do topo do
// tópico que falhou.
func waitTopicReadFailures(t *testing.T, srv *server.Server, name string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rep := replicationOf(t, srv, name)
		if rep != nil && len(rep.Delivery) == 1 && rep.Delivery[0].Errors["topic_read"] >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a leitura do topo do tópico não falhou: %+v", rep)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// getStatus faz o GET e devolve o status e o corpo.
func getStatus(t *testing.T, srv *server.Server, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + srv.Endereco() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}
