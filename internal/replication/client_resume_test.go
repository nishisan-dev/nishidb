package replication

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeMaster é o master HTTP mínimo dos testes da retomada: cursor, snapshot
// e batch com a resposta que o teste escolher, e a contagem de cada rota.
type fakeMaster struct {
	mu       sync.Mutex
	status   Status
	snapshot Snapshot
	batch    func(after uint64) (int, *Batch)
	onCursor func()
	calls    map[string]int
	snapAt   []time.Time
	// cursorFail, quando devolve true, responde 503 ao cursor.
	cursorFail func(call int) bool
	cursorAt   []time.Time
	afters     []uint64
	transport  *httptest.Server
}

func newFakeMaster(t *testing.T, epoch string, produced uint64) *fakeMaster {
	t.Helper()
	m := &fakeMaster{
		status:   Status{Epoch: epoch, Produced: produced},
		snapshot: Snapshot{Epoch: epoch, Cursor: produced},
		calls:    map[string]int{},
	}
	m.transport = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.transport.Close)
	return m
}

func (m *fakeMaster) serve(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.calls[r.URL.Path]++
	if r.URL.Path == "/v1/replication/snapshot" {
		m.snapAt = append(m.snapAt, time.Now())
	}
	status, snap, batch := m.status, m.snapshot, m.batch
	if r.URL.Path == "/v1/replication/cursor" && m.onCursor != nil {
		m.onCursor()
	}
	if r.URL.Path == "/v1/replication/batch" {
		after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		m.afters = append(m.afters, after)
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/v1/replication/cursor":
		m.mu.Lock()
		m.cursorAt = append(m.cursorAt, time.Now())
		fail := m.cursorFail != nil && m.cursorFail(len(m.cursorAt))
		m.mu.Unlock()
		if fail {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(status)
	case "/v1/replication/snapshot":
		_ = json.NewEncoder(w).Encode(snap)
	case "/v1/replication/batch":
		after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
		code, b := http.StatusConflict, (*Batch)(nil)
		if batch != nil {
			code, b = batch(after)
		}
		if code != http.StatusOK {
			http.Error(w, "refused", code)
			return
		}
		_ = json.NewEncoder(w).Encode(b)
	default:
		http.NotFound(w, r)
	}
}

func (m *fakeMaster) count(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[path]
}

// firstAfter é o cursor do primeiro lote pedido.
func (m *fakeMaster) firstAfter() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.afters) == 0 {
		return 0
	}
	return m.afters[0]
}

func (m *fakeMaster) set(f func(m *fakeMaster)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f(m)
}

// followingClient monta um follower em following na epoch e no cursor
// dados: snapshot do fake e handshake que confirma o cursor.
func followingClient(t *testing.T, m *fakeMaster) (*Client, *[]uint64) {
	t.Helper()
	var applied []uint64
	replica := NewReplica(nil, func(e Entry) error {
		applied = append(applied, e.Sequence)
		return nil
	}, nil, nil)
	c := &Client{BaseURL: m.transport.URL, Dataset: "events", Replica: replica}
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("snapshot inicial: %v", err)
	}
	if state, _, _ := replica.State(); state != ReplicaFollowing {
		t.Fatalf("pré-condição: estado %s depois do snapshot, quero following", state)
	}
	return c, &applied
}

// batchFrom devolve o lote (after, to] com uma entrada por sequence.
func batchFrom(epoch string, to uint64) func(uint64) (int, *Batch) {
	return func(after uint64) (int, *Batch) {
		b := &Batch{Epoch: epoch, From: after + 1, To: to}
		for seq := after + 1; seq <= to; seq++ {
			b.Entries = append(b.Entries, Entry{Sequence: seq, Table: "events", Operation: Update})
		}
		return http.StatusOK, b
	}
}

// A desconexão em following com a MESMA epoch no master retoma pelo tail:
// nenhum snapshot novo, o lote continua do cursor aplicado e o follower volta
// a following. É o reinício limpo do master visto pelo follower.
func TestFollowerResumesTailAfterDisconnectWithSameEpoch(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, applied := followingClient(t, m)
	c.Replica.Disconnect()
	m.set(func(m *fakeMaster) { m.status.Produced = 5; m.batch = batchFrom("e1", 5) })
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("retomada: %v", err)
	}
	if n := m.count("/v1/replication/snapshot"); n != 1 {
		t.Fatalf("snapshots pedidos = %d, quero só o inicial: a desconexão em following tem de retomar pelo tail", n)
	}
	if after := m.firstAfter(); after != 3 {
		t.Errorf("o tail retomou de after=%d, quero o cursor aplicado 3", after)
	}
	state, epoch, cursor := c.Replica.State()
	if state != ReplicaFollowing || epoch != "e1" || cursor != 5 {
		t.Fatalf("depois da retomada: %s %s %d, quero following e1 5", state, epoch, cursor)
	}
	if got := *applied; len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("entradas aplicadas = %v, quero [4 5]", got)
	}
}

// Desconexões seguidas (o master fora por várias rodadas) mantêm a retomada
// que a primeira lembrou.
func TestFollowerKeepsResumeAcrossRepeatedDisconnects(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, _ := followingClient(t, m)
	c.Replica.Disconnect()
	c.Replica.Disconnect()
	c.Replica.Disconnect()
	m.set(func(m *fakeMaster) { m.status.Produced = 4; m.batch = batchFrom("e1", 4) })
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := m.count("/v1/replication/snapshot"); n != 1 {
		t.Fatalf("snapshots pedidos = %d depois de desconexões repetidas, quero só o inicial", n)
	}
}

// A desconexão vinda de um estado que não é catching_up nem following (aqui,
// resync_required) não retoma: o cursor não prova nada, e a rodada seguinte
// baixa um snapshot.
func TestFollowerDisconnectedFromResyncDownloadsSnapshot(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, _ := followingClient(t, m)
	c.Replica.RequireResync()
	c.Replica.Disconnect()
	m.set(func(m *fakeMaster) { m.batch = batchFrom("e1", 3) })
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := m.count("/v1/replication/snapshot"); n != 2 {
		t.Fatalf("snapshots pedidos = %d, quero 2: desconexão vinda de resync_required não retoma pelo tail", n)
	}
	if n := m.count("/v1/replication/batch"); n != 0 {
		t.Fatalf("o follower pediu %d lote(s) com o cursor de um resync", n)
	}
}

// Epoch nova no master (reinício com queda) não retoma: snapshot.
func TestFollowerDisconnectedWithNewMasterEpochDownloadsSnapshot(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, _ := followingClient(t, m)
	c.Replica.Disconnect()
	m.set(func(m *fakeMaster) {
		m.status = Status{Epoch: "e2", Produced: 0}
		m.snapshot = Snapshot{Epoch: "e2", Cursor: 0}
	})
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := m.count("/v1/replication/snapshot"); n != 2 {
		t.Fatalf("snapshots pedidos = %d, quero 2: epoch nova exige snapshot", n)
	}
	if state, epoch, _ := c.Replica.State(); state != ReplicaFollowing || epoch != "e2" {
		t.Fatalf("estado %s epoch %s, quero following e2", state, epoch)
	}
}

// Na retomada, um 409 (cursor fora do feed retido) cai no resync de hoje, e
// a rodada seguinte baixa o snapshot; um 410 (o master encerrando) é
// desconexão, e a rodada seguinte tenta o tail de novo.
func TestFollowerResumeConflictResyncsAndGoneRetriesTheTail(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, _ := followingClient(t, m)
	c.Replica.Disconnect()
	m.set(func(m *fakeMaster) {
		m.batch = func(uint64) (int, *Batch) { return http.StatusGone, nil }
	})
	var he *HTTPError
	if err := c.SyncOnce(context.Background()); !errors.As(err, &he) || he.Status != http.StatusGone {
		t.Fatalf("410 na retomada devolveu %v, quero o HTTPError 410", err)
	}
	c.Replica.Disconnect() // o que o Follow faz com o erro
	m.set(func(m *fakeMaster) {
		m.batch = func(uint64) (int, *Batch) { return http.StatusConflict, nil }
	})
	if err := c.SyncOnce(context.Background()); !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("409 na retomada devolveu %v, quero ErrResyncRequired", err)
	}
	if n := m.count("/v1/replication/batch"); n != 2 {
		t.Fatalf("lotes pedidos = %d, quero 2: depois do 410 a retomada tenta o tail de novo", n)
	}
	if state, _, _ := c.Replica.State(); state != ReplicaResyncRequired {
		t.Fatalf("estado %s depois do 409, quero resync_required", state)
	}
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := m.count("/v1/replication/snapshot"); n != 2 {
		t.Fatalf("snapshots pedidos = %d, quero 2: o 409 leva ao snapshot", n)
	}
}

// O 503 do master com a conferência de rewind pendente é desconexão no
// Follow: o follower espera com backoff, guarda o cursor e a retomada, e não
// pede snapshot por causa dele. Quando o master volta a servir, o tail
// continua do cursor aplicado, sem snapshot.
func TestFollowRetriesTheTailWhileTheMasterAnswersUnavailable(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, applied := followingClient(t, m)
	var unavailable atomic.Bool
	unavailable.Store(true)
	m.set(func(m *fakeMaster) {
		m.status.Produced = 5
		tail := batchFrom("e1", 5)
		m.batch = func(after uint64) (int, *Batch) {
			if unavailable.Load() {
				return http.StatusServiceUnavailable, nil
			}
			return tail(after)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Follow(ctx) }()
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { cancel(); <-done }) }
	defer stop()
	deadline := time.Now().Add(10 * time.Second)
	for m.count("/v1/replication/batch") < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("o follower parou de tentar o tail: %d lote(s) pedidos", m.count("/v1/replication/batch"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := m.count("/v1/replication/snapshot"); n != 1 {
		t.Fatalf("snapshots pedidos = %d com o master respondendo 503, quero só o inicial", n)
	}
	if state, epoch, cursor := c.Replica.State(); state != ReplicaDisconnected || epoch != "e1" || cursor != 3 {
		t.Fatalf("com o master respondendo 503: %s %s %d, quero disconnected e1 3", state, epoch, cursor)
	}
	unavailable.Store(false)
	deadline = time.Now().Add(10 * time.Second)
	for {
		if state, _, cursor := c.Replica.State(); state == ReplicaFollowing && cursor == 5 {
			break
		}
		if time.Now().After(deadline) {
			state, epoch, cursor := c.Replica.State()
			t.Fatalf("o tail não retomou depois do 503: %s %s %d", state, epoch, cursor)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := m.count("/v1/replication/snapshot"); n != 1 {
		t.Fatalf("snapshots pedidos = %d depois do 503, quero só o inicial: a retomada é pelo tail", n)
	}
	stop()
	if got := *applied; len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("entradas aplicadas = %v, quero [4 5]", got)
	}
}

// O follower que exige a histórica, diante do master que anuncia
// historyExcluded: ErrHistoryNotPublished, estado history_unavailable,
// nenhum snapshot nem lote pedido, e o ERROR sai uma vez por ocorrência e
// não a cada rodada. Sem a exigência, o mesmo master é seguido normalmente
// (a asserção positiva que prova que o fake é servível).
func TestFollowerRequiringHistoryRefusesMasterThatExcludesIt(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	m.set(func(m *fakeMaster) { m.status.HistoryExcluded = true })
	logs := &lockedBuffer{}
	var applied int
	c := &Client{
		BaseURL: m.transport.URL, Dataset: "events", RequireHistory: true, Logger: testLogger(logs),
		Replica: NewReplica(nil, func(Entry) error { applied++; return nil }, nil, nil),
	}
	for range 3 {
		if err := c.SyncOnce(context.Background()); !errors.Is(err, ErrHistoryNotPublished) {
			t.Fatalf("SyncOnce = %v, quero ErrHistoryNotPublished", err)
		}
	}
	if state, _, _ := c.Replica.State(); state != ReplicaHistoryUnavailable {
		t.Fatalf("estado %s, quero history_unavailable", state)
	}
	if n := m.count("/v1/replication/snapshot") + m.count("/v1/replication/batch"); n != 0 || applied != 0 {
		t.Fatalf("o follower pediu %d snapshot/lote e aplicou %d entrada(s) de um master sem a histórica", n, applied)
	}
	if n := strings.Count(logs.String(), `"replication.follower.history_unavailable"`); n != 1 {
		t.Fatalf("ERROR history_unavailable saiu %d vez(es) em 3 rodadas, quero 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Fatalf("history_unavailable sem nível ERROR:\n%s", logs.String())
	}

	free := &Client{BaseURL: m.transport.URL, Dataset: "events", Replica: NewReplica(nil, nil, nil, nil)}
	if err := free.SyncOnce(context.Background()); err != nil {
		t.Fatalf("follower sem a exigência: %v", err)
	}
	if state, _, _ := free.Replica.State(); state != ReplicaFollowing {
		t.Fatalf("follower sem a exigência ficou em %s, quero following", state)
	}
}

// No laço do Follow, a histórica indisponível não vira desconexão: o estado
// fica history_unavailable entre as retentativas (o health e as consultas
// leem esse estado, e "master offline" seria mentira).
func TestFollowKeepsHistoryUnavailableBetweenRetries(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	m.set(func(m *fakeMaster) { m.status.HistoryExcluded = true })
	c := &Client{BaseURL: m.transport.URL, Dataset: "events", RequireHistory: true, Replica: NewReplica(nil, nil, nil, nil)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Follow(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for m.count("/v1/replication/cursor") < 1 {
		if time.Now().After(deadline) {
			t.Fatal("o Follow não consultou o cursor")
		}
		time.Sleep(time.Millisecond)
	}
	// Dentro do primeiro backoff (200 ms): o estado tem de continuar o da
	// recusa.
	time.Sleep(50 * time.Millisecond)
	state, _, _ := c.Replica.State()
	cancel()
	<-done
	if state != ReplicaHistoryUnavailable {
		t.Fatalf("estado entre retentativas = %s, quero history_unavailable", state)
	}
}

// Um erro LOCAL do apply no meio de um lote não é desconexão: o follower vai
// para resync_required (e não para disconnected, que retomaria o tail e, com
// erro determinístico, falharia para sempre), e a rodada seguinte do Follow
// baixa um snapshot.
func TestFollowerApplyErrorMidBatchResyncsWithSnapshot(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	failing := errors.New("store refused the entry")
	apply := func(e Entry) error {
		if e.Sequence == 5 {
			return failing
		}
		return nil
	}
	c := &Client{BaseURL: m.transport.URL, Dataset: "events", Replica: NewReplica(nil, apply, nil, nil)}
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatalf("snapshot inicial: %v", err)
	}
	m.set(func(m *fakeMaster) { m.status.Produced = 5; m.batch = batchFrom("e1", 5) })
	if err := c.SyncOnce(context.Background()); !errors.Is(err, ErrApplyFailed) || !errors.Is(err, failing) {
		t.Fatalf("apply falho no meio do lote devolveu %v, quero ErrApplyFailed com a causa", err)
	}
	if state, _, cursor := c.Replica.State(); state != ReplicaResyncRequired || cursor != 3 {
		t.Fatalf("depois do apply falho: %s/%d, quero resync_required/3", state, cursor)
	}

	// No laço do Follow: o estado que a rodada seguinte encontra (lido no
	// handler do cursor, sem relógio) é resync_required, e ela pede o
	// snapshot. Desconectar levaria a disconnected e à retomada do tail.
	loop := &Client{BaseURL: m.transport.URL, Dataset: "events", Replica: NewReplica(nil, apply, nil, nil)}
	m.set(func(m *fakeMaster) { m.status.Produced = 3; m.batch = nil })
	if err := loop.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	seen := make(chan ReplicaState, 1)
	calls := 0
	m.set(func(m *fakeMaster) {
		m.status.Produced = 5
		m.batch = batchFrom("e1", 5)
		// A 1ª consulta do Follow leva ao lote que falha; a 2ª é a rodada
		// depois do erro.
		m.onCursor = func() {
			if calls++; calls == 2 {
				state, _, _ := loop.Replica.State()
				seen <- state
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Follow(ctx) }()
	var state ReplicaState
	select {
	case state = <-seen:
	case <-time.After(10 * time.Second):
		t.Fatal("o Follow não voltou ao master depois do apply falho")
	}
	snapshots := 0
	deadline := time.Now().Add(10 * time.Second)
	for snapshots = m.count("/v1/replication/snapshot"); snapshots < 3 && time.Now().Before(deadline); snapshots = m.count("/v1/replication/snapshot") {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if state != ReplicaResyncRequired {
		t.Fatalf("a rodada depois do apply falho encontrou %s, quero resync_required", state)
	}
	if snapshots < 3 {
		t.Fatalf("snapshots pedidos = %d, quero 3 (dois iniciais e o do resync)", snapshots)
	}
}

// Um erro de apply determinístico SÓ no tail (o snapshot no cursor 3 dá certo,
// a entrada 5 falha sempre) não vira um snapshot completo a cada volta
// mínima: o snapshot bem-sucedido não zera o backoff enquanto a falha local
// está pendente — nem o que termina em catching_up, nem o que chega a
// following (ele não passa pela entrada que falhou) —, e o intervalo entre
// snapshots cresce. O ERROR replication.follower.apply_failed sai uma vez,
// com a sequence, e não a cada volta.
func TestFollowerRepeatedApplyErrorBacksOffBetweenSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name     string
		produced uint64 // 3: o snapshot chega a following; 5: fica em catching_up
	}{{"catching_up", 5}, {"following", 3}} {
		t.Run(tc.name, func(t *testing.T) {
			m := newFakeMaster(t, "e1", tc.produced)
			m.set(func(m *fakeMaster) {
				m.snapshot = Snapshot{Epoch: "e1", Cursor: 3}
				m.batch = batchFrom("e1", 5)
			})
			logs := &lockedBuffer{}
			c := &Client{BaseURL: m.transport.URL, Dataset: "events", Logger: testLogger(logs),
				Replica: NewReplica(nil, func(e Entry) error {
					if e.Sequence == 5 {
						return errors.New("store refused the entry")
					}
					return nil
				}, nil, nil)}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Follow(ctx) }()
			deadline := time.Now().Add(10 * time.Second)
			for m.count("/v1/replication/snapshot") < 4 {
				if time.Now().After(deadline) {
					cancel()
					<-done
					t.Fatalf("o Follow pediu %d snapshot(s) em 10 s", m.count("/v1/replication/snapshot"))
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			<-done
			m.mu.Lock()
			at := append([]time.Time(nil), m.snapAt...)
			m.mu.Unlock()
			// Snapshots 1 (subida), 2 (+200 ms), 3 (+400 ms), 4 (+800 ms). Com
			// o backoff zerado a cada snapshot, os intervalos seriam de ~200 ms.
			g2, g3 := at[2].Sub(at[1]), at[3].Sub(at[2])
			if g2 < 300*time.Millisecond || g3 < 600*time.Millisecond || g3 <= g2 {
				t.Fatalf("intervalos entre snapshots = %v, %v; quero crescentes (~400 ms e ~800 ms): o snapshot que veio do erro local zerou o backoff", g2, g3)
			}
			if n := strings.Count(logs.String(), `"replication.follower.apply_failed"`); n != 1 {
				t.Fatalf("ERROR apply_failed saiu %d vez(es) em %d falhas seguidas, quero 1:\n%s", n, len(at)-1, logs.String())
			}
			if !strings.Contains(logs.String(), `"sequence":5`) || !strings.Contains(logs.String(), `"level":"ERROR"`) {
				t.Fatalf("apply_failed sem a sequence ou sem nível ERROR:\n%s", logs.String())
			}
		})
	}
}

// O tail que anda devolve o backoff ao mínimo: depois de três falhas seguidas
// (200, 400 e 800 ms), um lote aplicado, e a falha seguinte espera de novo só
// 200 ms, e não os 1,6 s do backoff acumulado.
func TestFollowerTailProgressResetsBackoff(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	c, _ := followingClient(t, m)
	var base int
	m.set(func(m *fakeMaster) {
		base = len(m.cursorAt)
		m.status.Produced = 4
		m.batch = batchFrom("e1", 4)
		// Do Follow: 1–3 falham, 4 leva ao lote 4, 5 falha, o resto responde.
		m.cursorFail = func(call int) bool {
			n := call - base
			return n <= 3 || n == 5
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Follow(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		m.mu.Lock()
		n := len(m.cursorAt) - base
		m.mu.Unlock()
		if n >= 6 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("o Follow consultou o cursor %d vez(es) em 10 s", n)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	m.mu.Lock()
	at := append([]time.Time(nil), m.cursorAt[base:]...)
	m.mu.Unlock()
	if _, _, cursor := c.Replica.State(); cursor != 4 {
		t.Fatalf("pré-condição: o lote 4 não foi aplicado (cursor %d)", cursor)
	}
	before, after := at[3].Sub(at[2]), at[5].Sub(at[4])
	if before < 600*time.Millisecond || after > time.Second {
		t.Fatalf("espera antes do progresso = %v (quero ~800 ms), depois = %v (quero ~200 ms; o teto de 1 s deixa folga para máquina carregada e ainda separa do backoff de 1,6 s sem a zeragem): o lote aplicado não zerou o backoff", before, after)
	}
}

// "Uma vez por ocorrência": o tail que volta a andar encerra a ocorrência, e
// uma falha local nova, depois dele, emite o ERROR de novo.
func TestFollowerApplyFailedLogsAgainAfterProgress(t *testing.T) {
	m := newFakeMaster(t, "e1", 3)
	logs := &lockedBuffer{}
	var mu sync.Mutex
	failures := map[uint64]int{5: 1, 7: 1} // cada uma falha uma vez
	c := &Client{BaseURL: m.transport.URL, Dataset: "events", Logger: testLogger(logs),
		Replica: NewReplica(nil, func(e Entry) error {
			mu.Lock()
			defer mu.Unlock()
			if failures[e.Sequence] > 0 {
				failures[e.Sequence]--
				return errors.New("store refused the entry")
			}
			return nil
		}, nil, nil)}
	if err := c.SyncOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.set(func(m *fakeMaster) { m.status.Produced = 5; m.batch = batchFrom("e1", 5) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Follow(ctx) }()
	waitApplied := func(want uint64) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if state, _, cursor := c.Replica.State(); state == ReplicaFollowing && cursor >= want {
				return
			}
			if time.Now().After(deadline) {
				cancel()
				<-done
				t.Fatalf("o follower não chegou a following/%d", want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitApplied(5)
	m.set(func(m *fakeMaster) {
		m.status.Produced = 7
		m.snapshot = Snapshot{Epoch: "e1", Cursor: 5}
		m.batch = batchFrom("e1", 7)
	})
	waitApplied(7)
	cancel()
	<-done
	if n := strings.Count(logs.String(), `"replication.follower.apply_failed"`); n != 2 {
		t.Fatalf("ERROR apply_failed saiu %d vez(es) para duas ocorrências separadas por progresso, quero 2:\n%s", n, logs.String())
	}
}
