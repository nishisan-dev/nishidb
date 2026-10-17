package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
	"github.com/twmb/franz-go/pkg/kfake"
)

// diskFeedSpec monta o YAML de um master (ou follower) com o feed em disco.
// Os blocos opcionais entram inteiros: source, ttl (sob materialization),
// history e as linhas extras de replication.
type diskFeedSpec struct {
	name        string
	source      string
	ttl         string
	history     string
	replication string
}

func (s diskFeedSpec) yaml() string {
	return "version: 1\nname: " + s.name + "\nschemaName: schema_" + s.name + "\n" + s.source +
		"materialization:\n  key:\n    field: id\n  mode: merge\n" + s.ttl + s.history +
		"schema:\n  include: [\"*\"]\n  fields:\n    id:\n      type: string\n    status:\n      type: string\n" +
		"replication:\n  enabled: true\n" + s.replication
}

// kafkaSource é o bloco `source:` que consome o tópico src do broker dado.
func kafkaSource(broker string) string {
	return "source:\n  type: kafka\n  kafka:\n    enabled: true\n    bootstrapServers:\n      - " + broker +
		"\n    topic: src\n    consumer:\n      groupId: disk-feed\n      autoOffsetReset: earliest\n"
}

const (
	masterReplication = "  flushInterval: 0s\n"
	historyBlock      = "history:\n  expireAfterWrite: 7d\n  maxBytes: 4GiB\n"
	ttlOneHour        = "  ttl:\n    - expireAfterWrite: 1h\n"
)

// ingestOffline aplica os eventos pelo `nishidb ingest`, com o servidor
// parado: é a mutação offline que o feed não vê.
func ingestOffline(t *testing.T, a *env, dataset string, events ...string) {
	t.Helper()
	var out bytes.Buffer
	code := cli.Run([]string{"ingest", "--config", a.cfgPath, "--dataset", dataset, "--input", "-"}, cli.Context{
		Stdin:  strings.NewReader(strings.Join(events, "\n") + "\n"),
		Stdout: &out, Stderr: &out, Ctx: context.Background(),
	})
	if code != cli.ExitOK {
		t.Fatalf("nishidb ingest saiu %d:\n%s", code, out.String())
	}
}

// ingestOfflineAtKafkaStart faz o ingestOffline num dataset com source.kafka e
// grava a posição local no começo da partição 0 (`nishidb offsets --set`):
// sem posição conhecida, a ingestão recusa subir sobre um dataset com linhas.
func ingestOfflineAtKafkaStart(t *testing.T, a *env, dataset string, events ...string) {
	t.Helper()
	ingestOffline(t, a, dataset, events...)
	var out bytes.Buffer
	code := cli.Run([]string{"offsets", "--config", a.cfgPath, "--dataset", dataset, "--set", "0:0", "--yes"}, cli.Context{
		Stdin: strings.NewReader(""), Stdout: &out, Stderr: &out, Ctx: context.Background(),
	})
	if code != cli.ExitOK {
		t.Fatalf("nishidb offsets saiu %d:\n%s", code, out.String())
	}
}

// startDiskFeedServer sobe o servidor com os logs num buffer, e devolve o stop
// (que reprova o teste se o Run devolver erro).
func startDiskFeedServer(t *testing.T, a *env, before func(*server.Server)) (*server.Server, *recordOfServer, func()) {
	t.Helper()
	logs := &recordOfServer{}
	srv, err := server.Novo(a.load(), server.Options{Log: logging.New(logs, logging.FormatText, slog.LevelInfo)})
	if err != nil {
		t.Fatal(err)
	}
	if before != nil {
		before(srv)
	}
	return srv, logs, startWebhookServer(t, srv)
}

func feedStatus(t *testing.T, srv *server.Server, dataset string) replication.Status {
	t.Helper()
	var st replication.Status
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/replication/cursor?dataset="+dataset)), &st); err != nil || st.Epoch == "" {
		t.Fatalf("cursor ilegível: %v %+v", err, st)
	}
	return st
}

// feedEntries lê o feed inteiro retido, do piso ao produzido, pelo
// /v1/replication/batch (memória e disco).
func feedEntries(t *testing.T, srv *server.Server, dataset string) (replication.Status, []replication.Entry) {
	t.Helper()
	st := feedStatus(t, srv, dataset)
	var out []replication.Entry
	for after := st.AvailableAfter; after < st.Produced; {
		body := fetch(t, srv, "/v1/replication/batch?dataset="+dataset+"&epoch="+st.Epoch+"&after="+strconv.FormatUint(after, 10))
		var b replication.Batch
		if err := json.Unmarshal([]byte(body), &b); err != nil || b.To <= after {
			t.Fatalf("lote ilegível depois de %d: %v %s", after, err, body)
		}
		out = append(out, b.Entries...)
		after = b.To
	}
	return st, out
}

func waitProduced(t *testing.T, srv *server.Server, dataset string, want uint64) replication.Status {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		st := feedStatus(t, srv, dataset)
		if st.Produced >= want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("o feed de %q produziu %d, quero %d", dataset, st.Produced, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// logLine devolve a linha do log com o evento, ou "".
func logLine(logs *recordOfServer, event string) string {
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, "event="+event) {
			return l
		}
	}
	return ""
}

// requireOrigin confere a origem que a subida anunciou para o feed.
func requireOrigin(t *testing.T, logs *recordOfServer, want string) {
	t.Helper()
	line := logLine(logs, "replication.feed.resumed")
	if !strings.Contains(line, "origin="+want) {
		t.Fatalf("a subida não anunciou origin=%s: %q", want, line)
	}
}

// requireCleanShutdown confere que o encerramento não deixou nenhum aviso de
// fechamento sujo.
func requireCleanShutdown(t *testing.T, logs *recordOfServer) {
	t.Helper()
	for _, event := range []string{
		"replication.feed.close_failed", "replication.feed.closed_unclean", "replication.feed.publish_after_close",
		"replication.feed.publish_before_resume", "datasets.shutdown_checkpoint_failed",
	} {
		if line := logLine(logs, event); line != "" {
			t.Errorf("o encerramento deixou %s: %s", event, line)
		}
	}
}

// O reinício limpo retoma a MESMA geração: mesma epoch, mesma sequence, e o
// que a primeira vida publicou continua servível do disco, contíguo com o
// que a segunda publica. Um segundo Close do servidor já encerrado não avisa
// nada nem suja o feed.
func TestServerRestartResumesFeedEpoch(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"))
	if err != nil {
		t.Fatal(err)
	}
	// Depois do encerramento do servidor (Cleanup em LIFO): com o broker
	// fechado antes, a drenagem do encerramento estouraria o orçamento.
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", source: kafkaSource(broker), history: historyBlock, replication: masterReplication}.yaml())

	first, firstLogs, stopFirst := startDiskFeedServer(t, a, nil)
	requireOrigin(t, firstLogs, "fresh")
	produceTo(t, broker, "src", `{"id":"k1","status":"open"}`, `{"id":"k2","status":"open"}`)
	waitIngestionAplicarPeloMenos(t, first, "events", 2, 20*time.Second)
	// Base e histórica: duas entradas por evento.
	before := waitProduced(t, first, "events", 4)
	stopFirst()
	if err := first.Close(); err != nil {
		t.Fatalf("segundo Close: %v", err)
	}
	requireCleanShutdown(t, firstLogs)

	second, secondLogs, _ := startDiskFeedServer(t, a, nil)
	requireOrigin(t, secondLogs, "clean")
	if st := feedStatus(t, second, "events"); st.Epoch != before.Epoch || st.Produced != before.Produced {
		t.Fatalf("reinício limpo: cursor %s/%d, quero %s/%d", st.Epoch, st.Produced, before.Epoch, before.Produced)
	}
	produceTo(t, broker, "src", `{"id":"k3","status":"new"}`)
	waitProduced(t, second, "events", before.Produced+2)
	st, entries := feedEntries(t, second, "events")
	if st.AvailableAfter != 0 {
		t.Fatalf("o piso subiu para %d no reinício; quero o feed inteiro retido", st.AvailableAfter)
	}
	for i, e := range entries {
		if e.Sequence != uint64(i+1) {
			t.Fatalf("entrada %d com sequence %d: o feed retomado não é contíguo com a primeira vida", i, e.Sequence)
		}
	}
	keys := map[string]bool{}
	for _, e := range entries {
		if e.Table == "events" {
			keys[string(e.Record.Key)] = true
		}
	}
	if !keys["k1"] || !keys["k2"] || !keys["k3"] {
		t.Fatalf("o feed retomado não traz as duas vidas: %v", keys)
	}
}

// Uma passada de expiração em voo no encerramento termina ANTES de o feed
// fechar: a lápide dela entra no feed, e a reabertura é limpa. O teste exige
// as duas coisas.
//
// A passada é segurada pelo relógio do Store, na conferência do teto de tempo
// de uma linha: DEPOIS de ela conferir o ctx (o cancelamento do Run não a
// interrompe mais) e ANTES do Expira tomar o wmu. É ali que a ordem aparece:
// com o feed fechado antes dos produtores, a impressão é lida e a marca limpa
// gravada com a passada ainda presa, e a lápide chega depois do Close (a
// mutação tardia derruba a marca, e a reabertura é queda). Um gancho sob o wmu
// não mostraria nada, porque a leitura da impressão também toma o wmu e
// esperaria a lápide.
func TestShutdownStopsProducersBeforeClosingTheFeed(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", ttl: ttlOneHour, replication: masterReplication}.yaml())
	events := make([]string, 20)
	for i := range events {
		events[i] = fmt.Sprintf(`{"id":"r%02d","status":"open"}`, i)
	}
	ingestOffline(t, a, "events", events...)

	held := make(chan struct{})
	release, letGo := heldRelease()
	var once sync.Once
	clock := func() time.Time {
		// Só a chamada feita direto do callback da passada (a conferência do
		// teto de tempo por linha) segura; as do Expira, da descida e do
		// resto do Store passam.
		if pc, _, _, ok := runtime.Caller(1); ok && strings.Contains(runtime.FuncForPC(pc).Name(), "(*Store).ExpiraOnePass.func") {
			once.Do(func() {
				close(held)
				<-release
			})
		}
		return time.Now().Add(2 * time.Hour)
	}
	srv, firstLogs, stop := startDiskFeedServer(t, a, func(srv *server.Server) {
		srv.UseStoreClockForTest("events", clock)
	})
	t.Cleanup(letGo) // antes do encerramento do servidor (Cleanup em LIFO)
	select {
	case <-held:
	case <-time.After(20 * time.Second):
		t.Fatal("a passada de expiração não chegou à primeira linha")
	}
	epoch := feedStatus(t, srv, "events").Epoch
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	// A passada é solta quando o encerramento chega ao ForExpiration, seja o
	// do stopStoreProducers (ordem certa: o feed ainda está aberto), seja o do
	// Close do Store (feed fechado antes dos produtores). Sem relógio de
	// parede: os dois desfechos são determinísticos.
	waitGoroutineIn(t, "storage.(*Store).ForExpiration(")
	letGo()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("o encerramento não terminou")
	}
	requireCleanShutdown(t, firstLogs)

	again, logs, _ := startDiskFeedServer(t, a, nil)
	requireOrigin(t, logs, "clean")
	st, entries := feedEntries(t, again, "events")
	if st.Epoch != epoch {
		t.Fatalf("a reabertura abriu a epoch %s, quero a mesma %s", st.Epoch, epoch)
	}
	// Sem o relógio adiantado, a segunda vida não expira nada: a única
	// lápide do feed é a da passada que estava em voo no encerramento.
	deletes := 0
	for _, e := range entries {
		if e.Operation == replication.Delete {
			deletes++
		}
	}
	if deletes != 1 {
		t.Fatalf("o feed retomado tem %d lápide(s), quero 1: a da passada em voo no encerramento (%d entradas)", deletes, len(entries))
	}
}

// heldRelease devolve o canal que segura uma passada e a função, idempotente,
// que o solta. Quem chama registra a função no t.Cleanup LOGO DEPOIS de subir
// o servidor: os Cleanup rodam em ordem inversa, e ela tem de rodar ANTES do
// encerramento do servidor. Sem isso, um teste que falha antes de soltar
// (t.Fatal no meio) deixa a passada presa, o encerramento espera por ela para
// sempre, e o binário de teste inteiro pendura.
func heldRelease() (<-chan struct{}, func()) {
	ch := make(chan struct{})
	var once sync.Once
	return ch, func() { once.Do(func() { close(ch) }) }
}

// waitGoroutineIn espera alguma goroutine do processo estar dentro da função
// (pelo nome no stack de todas). É como o teste sabe, sem dormir, que o
// encerramento chegou a um ponto: a goroutine que espera uma passada presa
// fica parada dentro do For* do Store.
//
// A busca é no stack do PROCESSO inteiro, e não do servidor do teste: outro
// teste rodando ao mesmo tempo que estivesse num For* do Store satisfaria a
// espera por engano. Por isso os testes que usam este helper não podem usar
// t.Parallel() (nem conviver com um que use, no mesmo binário).
func waitGoroutineIn(t *testing.T, fn string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(30 * time.Second)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		if bytes.Contains(buf[:n], []byte(fn)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nenhuma goroutine chegou a %s", fn)
		}
		time.Sleep(time.Millisecond)
	}
}

// Uma passada de retenção da histórica em voo no encerramento termina ANTES
// de o feed fechar: o retain dela entra no feed, e a reabertura é limpa.
//
// A passada é segurada pelo relógio do Store da irmã no instante em que
// publica o piso novo: o piso já avançou (atômico, fora do wmu), o retain
// ainda não saiu. A retenção não toma o wmu para publicar, então a leitura da
// impressão não a espera: sem o ForRetention antes do fechamento do feed, a
// impressão (com o piso novo) e a marca limpa são gravadas, e o retain chega
// depois do Close.
func TestShutdownStopsHistoryRetentionBeforeClosingTheFeed(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", history: historyRetentionBlock, replication: masterReplication}.yaml())
	pad := strings.Repeat("x", 200)
	seeded := make([]string, 600)
	for i := range seeded {
		seeded[i] = fmt.Sprintf(`{"id":"s%03d","status":"%s"}`, i%50, pad)
	}
	ingestOffline(t, a, "events", seeded...)

	held := make(chan struct{})
	release, letGo := heldRelease()
	var once sync.Once
	clock := func() time.Time {
		if pc, _, _, ok := runtime.Caller(1); ok && strings.Contains(runtime.FuncForPC(pc).Name(), "(*Store).advanceHistoryFloor") {
			once.Do(func() {
				close(held)
				<-release
			})
		}
		return time.Now()
	}
	srv, firstLogs, stop := startDiskFeedServer(t, a, func(srv *server.Server) {
		srv.UseStoreClockForTest("events_history", clock)
	})
	t.Cleanup(letGo) // antes do encerramento do servidor (Cleanup em LIFO)
	select {
	case <-held:
	case <-time.After(20 * time.Second):
		t.Fatal("a retenção da histórica não avançou o piso")
	}
	epoch := feedStatus(t, srv, "events").Epoch
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	waitGoroutineIn(t, "storage.(*Store).ForRetention(")
	letGo()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("o encerramento não terminou")
	}
	requireCleanShutdown(t, firstLogs)

	again, logs, _ := startDiskFeedServer(t, a, nil)
	requireOrigin(t, logs, "clean")
	st, entries := feedEntries(t, again, "events")
	if st.Epoch != epoch {
		t.Fatalf("a reabertura abriu a epoch %s, quero a mesma %s", st.Epoch, epoch)
	}
	for _, e := range entries {
		if e.Table == "events_history" && e.Operation == replication.Retain {
			return
		}
	}
	t.Fatalf("o retain da passada em voo no encerramento não está no feed (%d entradas)", len(entries))
}

// O encerramento checkpointa o dataset que não tem ingestão Kafka: o manifest
// gravado cobre a posição de escrita final, inclusive as lápides que a
// expiração gravou durante a vida do servidor.
func TestShutdownCheckpointsDatasetWithoutKafkaIngestion(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", ttl: ttlOneHour, replication: masterReplication}.yaml())
	ingestOffline(t, a, "events", `{"id":"a","status":"open"}`, `{"id":"b","status":"open"}`, `{"id":"c","status":"open"}`)
	srv, logs, stop := startDiskFeedServer(t, a, func(srv *server.Server) {
		srv.UseStoreClockForTest("events", func() time.Time { return time.Now().Add(2 * time.Hour) })
	})
	waitProduced(t, srv, "events", 3)
	stop()
	requireCleanShutdown(t, logs)

	cfg := a.load()
	dir := cfg.DatasetRootDir("events")
	m, err := checkpoint.Load(filepath.Join(dir, "checkpoint", "manifest.bin"))
	if err != nil {
		t.Fatal(err)
	}
	spec, err := schema.Load(cfg.DatasetSpecPath("events"), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// Aberto como o servidor o abre; o manifest já foi lido acima, antes do
	// checkpoint que este Open faz depois da recovery.
	st, err := storage.Open(storage.Options{Dir: dir, Dataset: "events", SegmentSize: cfg.SegmentSize(),
		ActiveTTL: true, FallbackDuration: spec.Materialization.FallbackDuration,
		WriteInstantFieldName: schema.WriteInstantFieldName, TTLFieldName: schema.TTLFieldName})
	if err != nil {
		t.Fatal(err)
	}
	pos, err := st.WritePosition()
	if cerr := st.Close(); cerr != nil {
		t.Fatal(cerr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if m.CurrentSegment != pos.Segment || m.DurableOffset != uint64(pos.Offset) || m.NextRowID != pos.NextRowID {
		t.Fatalf("manifest do encerramento em %d/%d (next %d), e a posição de escrita final é %d/%d (next %d): "+
			"o dataset sem Kafka não foi checkpointado", m.CurrentSegment, m.DurableOffset, m.NextRowID, pos.Segment, pos.Offset, pos.NextRowID)
	}
}

// A ingestão offline entre duas vidas do servidor muda a posição do Store,
// e a impressão a pega: a segunda vida abre uma epoch nova. Sem a mutação, a
// terceira retoma a da segunda — o par que prova que o teste enxerga a
// igualdade.
func TestOfflineIngestBetweenRunsForcesNewEpoch(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", replication: masterReplication}.yaml())
	first, _, stop := startDiskFeedServer(t, a, nil)
	e1 := feedStatus(t, first, "events").Epoch
	stop()

	ingestOffline(t, a, "events", `{"id":"offline","status":"open"}`)
	second, logs, stop := startDiskFeedServer(t, a, nil)
	requireOrigin(t, logs, "crash")
	e2 := feedStatus(t, second, "events").Epoch
	if e2 == e1 {
		t.Fatalf("a ingestão offline não mudou a epoch (%s): o feed retomou sem a mudança", e1)
	}
	if line := logLine(logs, "replication.feed.resumed"); !strings.Contains(line, "previousEpoch="+e1) {
		t.Errorf("a subida não nomeou a epoch anterior %s: %q", e1, line)
	}
	stop()

	third, _, _ := startDiskFeedServer(t, a, nil)
	if e3 := feedStatus(t, third, "events").Epoch; e3 != e2 {
		t.Fatalf("sem mutação offline a epoch mudou de %s para %s", e2, e3)
	}
}

// Um append offline SÓ na tabela histórica, com o base intocado, também abre
// uma epoch nova: a impressão inclui a histórica publicada.
func TestOfflineHistoryOnlyMutationForcesNewEpoch(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", history: historyBlock, replication: masterReplication}.yaml())
	ingestOffline(t, a, "events", `{"id":"k1","status":"open"}`)
	first, _, stop := startDiskFeedServer(t, a, nil)
	e1 := feedStatus(t, first, "events").Epoch
	stop()

	appendHistoryOffline(t, a, "events")
	second, logs, _ := startDiskFeedServer(t, a, nil)
	requireOrigin(t, logs, "crash")
	if e2 := feedStatus(t, second, "events").Epoch; e2 == e1 {
		t.Fatalf("o append offline na histórica não mudou a epoch (%s)", e1)
	}
}

// appendHistoryOffline abre o base e a irmã como o servidor abre, apenda uma
// linha só na irmã e confere que a posição do base não andou.
func appendHistoryOffline(t *testing.T, a *env, name string) {
	t.Helper()
	cfg := a.load()
	spec, err := schema.Load(cfg.DatasetSpecPath(name), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	base, err := storage.Open(storage.Options{Dir: cfg.DatasetRootDir(name), Dataset: name, SegmentSize: cfg.SegmentSize(),
		IndexedFieldsByName: schema.IndexedFieldNames(spec)})
	if err != nil {
		t.Fatal(err)
	}
	irma := spec.DeriveHistory()
	hist, err := storage.Open(storage.Options{Dir: cfg.DatasetRootDir(irma.Name), Dataset: irma.Name, SegmentSize: cfg.SegmentSize(),
		Mode: storage.HistoryMode, SharedDictionary: base.FieldDict(), IndexedFieldsByName: schema.IndexedFieldNames(irma),
		WriteInstantFieldName: schema.WriteInstantFieldName})
	if err != nil {
		_ = base.Close()
		t.Fatal(err)
	}
	before, err := base.WritePosition()
	if err != nil {
		t.Fatal(err)
	}
	type field struct {
		id types.FieldID
		v  types.Value
	}
	var fields []field
	for name, v := range map[string]types.Value{
		"id":                         {Type: types.TypeString, Bytes: []byte("offline")},
		schema.WriteInstantFieldName: {Type: types.TypeTimestamp, I64: time.Now().UnixMilli()},
	} {
		id, ok := base.FieldDict().Lookup([]byte(name))
		if !ok {
			t.Fatalf("campo %q fora do dicionário", name)
		}
		fields = append(fields, field{id, v})
	}
	// O payload exige os campos em ordem crescente de FieldID.
	slices.SortFunc(fields, func(x, y field) int { return int(x.id) - int(y.id) })
	var payload []byte
	for _, f := range fields {
		if payload, err = encoding.AppendField(payload, f.id, f.v); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hist.Apenda(payload); err != nil {
		t.Fatal(err)
	}
	after, err := base.WritePosition()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("pré-condição: a mutação offline mexeu no base (%+v → %+v)", before, after)
	}
	if err := hist.Close(); err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
}

// historyRetentionBlock força a retenção por bytes da irmã na primeira
// passada: segmentos de 8 KiB e teto de quatro deles, com a histórica
// semeada offline bem acima disso.
const historyRetentionBlock = "history:\n  maxBytes: 32KiB\n  segmentSize: 8KiB\n"

// `publishHistory: false` tira a histórica do feed INTEIRO: nem o tail, nem o
// retain da retenção, nem o snapshot HTTP, nem o snapshot do publicador Kafka
// a levam, e o cursor anuncia historyExcluded. Com a chave no padrão, a mesma
// montagem publica tudo isso — o par que prova que a retenção rodou e que o
// tail chegou.
func TestPublishHistoryFalseKeepsHistoryOutOfFeed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		publish bool
	}{{"published", true}, {"excluded", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"), kfake.SeedTopics(3, "cdc"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cluster.Close) // depois do encerramento do servidor
			broker := cluster.ListenAddrs()[0]
			a := newEnv(t)
			appendKafkaTargets(t, a, broker, [2]string{"cdc", "cdc"})
			repl := masterReplication + "  targets: [cdc]\n"
			if !tc.publish {
				repl += "  publishHistory: false\n"
			}
			a.writeDataset("events", diskFeedSpec{name: "events", source: kafkaSource(broker), history: historyRetentionBlock, replication: repl}.yaml())
			pad := strings.Repeat("x", 200)
			seeded := make([]string, 600)
			for i := range seeded {
				seeded[i] = fmt.Sprintf(`{"id":"s%03d","status":"%s"}`, i%50, pad)
			}
			ingestOfflineAtKafkaStart(t, a, "events", seeded...)

			srv, _, _ := startDiskFeedServer(t, a, nil)
			waitUntil(t, func() bool { return purgedBySize(t, srv) > 0 }, "a retenção por bytes da histórica não rodou")
			produceTo(t, broker, "src", `{"id":"live","status":"new"}`)
			waitIngestionAplicarPeloMenos(t, srv, "events", 1, 20*time.Second)
			var st replication.Status
			var entries []replication.Entry
			counts := map[replication.Operation]int{}
			history := 0
			waitUntil(t, func() bool {
				st, entries = feedEntries(t, srv, "events")
				clear(counts)
				history = 0
				for _, e := range entries {
					if e.Table == "events_history" {
						history++
						counts[e.Operation]++
					}
				}
				live := false
				for _, e := range entries {
					live = live || (e.Table == "events" && string(e.Record.Key) == "live")
				}
				return live && (!tc.publish || (counts[replication.Insert] > 0 && counts[replication.Retain] > 0))
			}, "o feed não trouxe o tail esperado")
			snapshot := snapshotTables(t, srv)
			topic := readReplicationTopic(t, broker, "cdc", st.Epoch, func(r []cdcRecord) bool {
				return tailOf(r, "events")["live"] != nil
			})
			topicHistory := 0
			for _, r := range topic {
				if r.table() == "events_history" {
					topicHistory++
				}
			}

			if !tc.publish {
				if history != 0 {
					t.Errorf("com publishHistory: false o feed trouxe %d entrada(s) da histórica: %v", history, counts)
				}
				if v, ok := cursorDocument(t, srv)["historyExcluded"]; !st.HistoryExcluded || !ok || v != true {
					t.Errorf("o cursor não anunciou historyExcluded: %+v", st)
				}
				if snapshot["events_history"] != 0 || snapshot["events"] == 0 {
					t.Errorf("snapshot HTTP com publishHistory: false = %v, quero só a base", snapshot)
				}
				if topicHistory != 0 {
					t.Errorf("o publicador Kafka levou %d registro(s) da histórica com publishHistory: false", topicHistory)
				}
				return
			}
			// Ausente, e não false: o campo é aditivo e o ausente vale o legado.
			if _, ok := cursorDocument(t, srv)["historyExcluded"]; st.HistoryExcluded || ok {
				t.Errorf("o cursor anunciou historyExcluded com a histórica publicada: %+v", st)
			}
			if snapshot["events_history"] == 0 || snapshot["events"] == 0 {
				t.Errorf("snapshot HTTP com a histórica publicada = %v, quero as duas tabelas", snapshot)
			}
			if topicHistory == 0 {
				t.Error("o publicador Kafka não levou a histórica publicada")
			}
		})
	}
}

// cursorDocument devolve o /v1/replication/cursor como documento JSON cru.
func cursorDocument(t *testing.T, srv *server.Server) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/replication/cursor?dataset=events")), &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// purgedBySize lê o history.purgedBySize do dataset base no /v1/stats.
func purgedBySize(t *testing.T, srv *server.Server) int64 {
	t.Helper()
	var doc struct {
		Datasets []struct {
			Name    string                 `json:"name"`
			History *protocol.HistoryStats `json:"history"`
		} `json:"datasets"`
	}
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/stats")), &doc); err != nil {
		t.Fatal(err)
	}
	for _, d := range doc.Datasets {
		if d.Name == "events" && d.History != nil {
			return d.History.PurgedBySize
		}
	}
	return 0
}

// snapshotTables conta as entradas do snapshot HTTP por tabela.
func snapshotTables(t *testing.T, srv *server.Server) map[string]int {
	t.Helper()
	var snap replication.Snapshot
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/replication/snapshot?dataset=events")), &snap); err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, e := range snap.Entries {
		out[e.Table]++
	}
	return out
}

// countingProxy fica entre o follower e o master: conta os snapshots pedidos
// e guarda o cursor de cada lote, e responde 502 com o master fora.
type countingProxy struct {
	mu        sync.Mutex
	snapshots int
	afters    []uint64
	srv       *httptest.Server
}

func newCountingProxy(t *testing.T, target string) *countingProxy {
	t.Helper()
	u, err := url.Parse("http://" + target)
	if err != nil {
		t.Fatal(err)
	}
	p := &countingProxy{}
	rp := httputil.NewSingleHostReverseProxy(u)
	// O master fora é o caso de teste, e não ruído a imprimir.
	rp.ErrorLog = log.New(io.Discard, "", 0)
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		switch r.URL.Path {
		case "/v1/replication/snapshot":
			p.snapshots++
		case "/v1/replication/batch":
			after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
			p.afters = append(p.afters, after)
		}
		p.mu.Unlock()
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *countingProxy) stats() (int, []uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshots, append([]uint64(nil), p.afters...)
}

// followerEnv monta o follower do dataset events apontando para masterURL.
func followerEnv(t *testing.T, masterURL, history string) *env {
	t.Helper()
	f := newEnv(t)
	f.writeDataset("events", diskFeedSpec{name: "events", history: history,
		replication: "  role: follower\n  masterURL: " + masterURL + "\n"}.yaml())
	return f
}

func waitFollower(t *testing.T, srv *server.Server, state replication.ReplicaState, applied uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got, _, cursor := srv.FollowerStateForTest("events")
		if got == state && cursor >= applied {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower em %s/%d, quero %s/%d", got, cursor, state, applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// O follower que declara a histórica, diante do master que não a publica,
// vai para history_unavailable com ERROR, e não pede snapshot nenhum.
func TestFollowerRequiringHistoryRefusesMasterWithoutIt(t *testing.T) {
	m := newEnv(t)
	m.writeDataset("events", diskFeedSpec{name: "events", history: historyBlock, replication: masterReplication + "  publishHistory: false\n"}.yaml())
	master, _, _ := startDiskFeedServer(t, m, nil)
	proxy := newCountingProxy(t, master.Endereco())

	f := followerEnv(t, proxy.srv.URL, historyBlock)
	follower, logs, _ := startDiskFeedServer(t, f, nil)
	waitFollower(t, follower, replication.ReplicaHistoryUnavailable, 0)
	if line := logLine(logs, "replication.follower.history_unavailable"); !strings.Contains(line, "level=ERROR") {
		t.Fatalf("sem ERROR replication.follower.history_unavailable: %q", line)
	}
	if snapshots, afters := proxy.stats(); snapshots != 0 || len(afters) != 0 {
		t.Fatalf("o follower pediu %d snapshot(s) e %d lote(s) de um master sem a histórica", snapshots, len(afters))
	}
}

// O master SEM `history:` também anuncia historyExcluded: o follower que
// declara a irmã vai para history_unavailable, e o que não a declara segue o
// mesmo master normalmente.
func TestFollowerRequiringHistoryRefusesMasterWithoutHistoryBlock(t *testing.T) {
	m := newEnv(t)
	m.writeDataset("events", diskFeedSpec{name: "events", replication: masterReplication}.yaml())
	master, _, _ := startDiskFeedServer(t, m, nil)
	if v, ok := cursorDocument(t, master)["historyExcluded"]; !ok || v != true {
		t.Fatalf("o master sem history: não anunciou historyExcluded: %v", cursorDocument(t, master))
	}
	masterURL := "http://" + master.Endereco()

	withHistory, logs, _ := startDiskFeedServer(t, followerEnv(t, masterURL, historyBlock), nil)
	waitFollower(t, withHistory, replication.ReplicaHistoryUnavailable, 0)
	if line := logLine(logs, "replication.follower.history_unavailable"); !strings.Contains(line, "level=ERROR") {
		t.Fatalf("sem ERROR replication.follower.history_unavailable: %q", line)
	}
	plain, _, _ := startDiskFeedServer(t, followerEnv(t, masterURL, ""), nil)
	waitFollower(t, plain, replication.ReplicaFollowing, 0)
}

// O follower no ar atravessa o reinício limpo do master sem snapshot: o
// long-poll recebe 410, o follower desconecta, e na volta do master (mesma
// epoch) retoma o tail do cursor aplicado, que nunca anda para trás.
func TestFollowerResumesTailAcrossMasterCleanRestart(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "src"))
	if err != nil {
		t.Fatal(err)
	}
	// Depois do encerramento do servidor (Cleanup em LIFO): com o broker
	// fechado antes, a drenagem do encerramento estouraria o orçamento.
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]
	m := newEnv(t)
	m.writeDataset("events", diskFeedSpec{name: "events", source: kafkaSource(broker), replication: masterReplication}.yaml())
	master, _, stopMaster := startDiskFeedServer(t, m, nil)
	produceTo(t, broker, "src", `{"id":"e1","status":"open"}`)
	waitProduced(t, master, "events", 1)
	proxy := newCountingProxy(t, master.Endereco())

	f := followerEnv(t, proxy.srv.URL, "")
	follower, _, _ := startDiskFeedServer(t, f, nil)
	waitFollower(t, follower, replication.ReplicaFollowing, 1)
	produceTo(t, broker, "src", `{"id":"e2","status":"open"}`)
	waitFollower(t, follower, replication.ReplicaFollowing, 2)
	epoch := feedStatus(t, master, "events").Epoch

	stopMaster()
	waitFollower(t, follower, replication.ReplicaDisconnected, 2)
	again, _, _ := startDiskFeedServer(t, m, nil)
	if e := feedStatus(t, again, "events").Epoch; e != epoch {
		t.Fatalf("pré-condição: o master reiniciou com a epoch %s, quero a mesma %s", e, epoch)
	}
	produceTo(t, broker, "src", `{"id":"e3","status":"open"}`)
	waitFollower(t, follower, replication.ReplicaFollowing, 3)

	snapshots, afters := proxy.stats()
	if snapshots != 1 {
		t.Fatalf("o follower pediu %d snapshot(s); quero só o da subida: o reinício limpo do master não exige snapshot", snapshots)
	}
	for i := 1; i < len(afters); i++ {
		if afters[i] < afters[i-1] {
			t.Fatalf("o cursor do follower andou para trás: %v", afters)
		}
	}
	rows, err := follower.QueryRowsForTest("events", "SELECT id FROM events WHERE id = 'e3'")
	if err != nil || len(rows) != 1 {
		t.Fatalf("o follower não tem o evento publicado depois do reinício: %v %v", rows, err)
	}
}

// A subida abortada DEPOIS do Resume de um dataset (outro dataset não abre)
// fecha o feed dele limpo: nada mutou o Store, e a próxima subida retoma a
// mesma epoch.
func TestAbortedStartupAfterResumeKeepsFeedClean(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", replication: masterReplication}.yaml())
	a.writeDataset("zz", strings.ReplaceAll(specWithoutKafka, "%s", "zz"))
	first, _, stop := startDiskFeedServer(t, a, nil)
	epoch := feedStatus(t, first, "events").Epoch
	stop()

	// O flock de zz tomado por outro: a subida abre events (com Resume) e
	// aborta em zz, que vem depois na ordem alfabética.
	cfg := a.load()
	held, err := storage.Open(storage.Options{Dir: cfg.DatasetRootDir("zz"), Dataset: "zz", SegmentSize: cfg.SegmentSize()})
	if err != nil {
		t.Fatal(err)
	}
	logs := &recordOfServer{}
	if _, err := server.Novo(cfg, server.Options{Log: logging.New(logs, logging.FormatText, slog.LevelInfo)}); err == nil {
		t.Fatal("a subida não abortou com o flock de zz tomado")
	}
	if logLine(logs, "replication.feed.resumed") == "" {
		t.Fatal("pré-condição: a subida abortou antes do Resume de events; o teste não prova o fechamento depois dele")
	}
	requireCleanShutdown(t, logs)
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}

	again, againLogs, _ := startDiskFeedServer(t, a, nil)
	requireOrigin(t, againLogs, "clean")
	if e := feedStatus(t, again, "events").Epoch; e != epoch {
		t.Fatalf("depois da subida abortada a epoch mudou de %s para %s", epoch, e)
	}
}

// O checkpoint do encerramento que falha (aqui, o da tabela irmã, com o
// diretório do manifest dela sem permissão de escrita) não derruba o
// encerramento, avisa nomeando o dataset, e o feed fecha SEM a marca limpa: o
// writer da irmã fica falho, a posição dela não é legível, e a próxima subida
// é queda, e não uma retomada sobre uma posição que ninguém confirmou.
func TestShutdownCheckpointFailureClosesFeedUnclean(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("events", diskFeedSpec{name: "events", history: historyBlock, replication: masterReplication}.yaml())
	ingestOffline(t, a, "events", `{"id":"k1","status":"open"}`)
	_, logs, stop := startDiskFeedServer(t, a, nil)
	manifestDir := filepath.Join(a.load().DatasetRootDir("events_history"), "checkpoint")
	if err := os.Chmod(manifestDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(manifestDir, 0o755) })
	stop()
	if err := os.Chmod(manifestDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if line := logLine(logs, "datasets.shutdown_checkpoint_failed"); !strings.Contains(line, `dataset \"events\"`) {
		t.Fatalf("sem WARN do checkpoint que falhou no encerramento: %q", line)
	}
	if logLine(logs, "replication.feed.closed_unclean") == "" {
		t.Fatal("o feed não avisou que fechou sem a marca limpa")
	}

	_, again, _ := startDiskFeedServer(t, a, nil)
	requireOrigin(t, again, "crash")
}

// `publishHistory` escrito num dataset sem `history:` é anunciado na subida
// com o WARN nomeado; o mesmo dataset sem a chave não avisa nada.
func TestPublishHistoryWithoutHistoryWarnsAtStartup(t *testing.T) {
	for _, tc := range []struct {
		name string
		repl string
		warn bool
	}{{"declared", masterReplication + "  publishHistory: false\n", true}, {"absent", masterReplication, false}} {
		t.Run(tc.name, func(t *testing.T) {
			a := newEnv(t)
			a.writeDataset("events", diskFeedSpec{name: "events", replication: tc.repl}.yaml())
			_, logs, _ := startDiskFeedServer(t, a, nil)
			line := logLine(logs, "replication.publish_history_without_history")
			if got := line != ""; got != tc.warn {
				t.Fatalf("WARN publish_history_without_history presente=%v, quero %v: %q", got, tc.warn, line)
			}
			if tc.warn && (!strings.Contains(line, "level=WARN") || !strings.Contains(line, "dataset=events")) {
				t.Fatalf("WARN sem nível ou sem o dataset: %q", line)
			}
		})
	}
}
