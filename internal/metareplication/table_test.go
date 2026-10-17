package metareplication_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/metareplication"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// allColumns é a projeção explícita de todas as colunas, na ordem do
// contrato.
const allColumns = "id, topic, state, epoch, cursor, checkpointCursor, produced, floor, " +
	"feedDiskBytes, feedCacheBytes, feedSegments, lagSequences, lagBytes, " +
	"lastResetReason, lastSnapshotAt, lastSuccessAt, lastError"

// liveFeed é o retrato do feed aberto que os testes usam.
func liveFeed() (replication.Status, bool) {
	return replication.Status{Epoch: "e1", Produced: 127, AvailableAfter: 50, BufferedBytes: 2048, DiskBytes: 1 << 20, DiskSegments: 3}, true
}

// twoDestinations são dois publicadores vivos, com atraso em sequências e em
// bytes DIFERENTES (a coluna que trocasse um pelo outro sairia errada) e um
// deles sem histórico nenhum, nem .ckpt gravado (CheckpointCursor 80 com
// CheckpointFound false: o número não é um ponto gravado).
func twoDestinations() []replicationkafka.Stats {
	return []replicationkafka.Stats{
		{Destination: "cdc", Topic: "cdc-topic", State: "following", Epoch: "e1", Cursor: 120, CheckpointCursor: 100, CheckpointFound: true,
			LagSequences: 7, LagBytes: 4096, LastResetReason: "requested", LastSnapshotAt: 1_757_000_000.5,
			LastSuccessTimestamp: 1_757_000_010.25},
		{Destination: "tems", Topic: "tems-topic", State: "retrying", Epoch: "e1", Cursor: 90, CheckpointCursor: 80,
			LagSequences: 37, LagBytes: 9000, LastError: "kafka: broker unavailable"},
	}
}

// Uma linha por destino, as colunas de destino de cada publicador e as do
// feed repetidas em toda linha. Texto vazio e instante zero ("nunca houve")
// são NULL.
func TestMetaReplicationListsOneRowPerDestination(t *testing.T) {
	eng := engineWithReplication(t, metareplication.Source{Feed: liveFeed, Destinations: twoDestinations})
	rows, _ := querySQL(t, eng, "SELECT "+allColumns+" FROM _meta.replication ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("%d linhas, quero 2: %v", len(rows), rows)
	}
	want := map[string]string{
		"id":               "cdc,tems",
		"topic":            "cdc-topic,tems-topic",
		"state":            "following,retrying",
		"epoch":            "e1,e1",
		"cursor":           "120,90",
		"checkpointCursor": "100,<null>",
		"produced":         "127,127",
		"floor":            "50,50",
		"feedDiskBytes":    "1048576,1048576",
		"feedCacheBytes":   "2048,2048",
		"feedSegments":     "3,3",
		"lagSequences":     "7,37",
		"lagBytes":         "4096,9000",
		"lastResetReason":  "requested,<null>",
		"lastSnapshotAt":   "1757000000500,<null>",
		"lastSuccessAt":    "1757000010250,<null>",
		"lastError":        "<null>,kafka: broker unavailable",
	}
	for col, w := range want {
		if got := column(rows, col); got != w {
			t.Errorf("%s = %s, quero %s", col, got, w)
		}
	}
}

// Sem publicador no ar (o `nishidb query --config`), as linhas vêm dos .ckpt:
// o ponto confirmado gravado e o último snapshot saem, e as colunas que só um
// publicador vivo mede são NULL — inclusive a cache do feed, que é memória
// do processo. O .ckpt ilegível não some com o destino: a linha sai com o
// erro em lastError.
func TestMetaReplicationReadsCheckpointsOffline(t *testing.T) {
	snapshotAt := time.UnixMilli(1_757_000_000_500).UTC()
	offlineFeed := func() (replication.Status, bool) {
		return replication.Status{Epoch: "e1", Produced: 127, AvailableAfter: 50, DiskBytes: 1 << 20, DiskSegments: 3}, true
	}
	checkpoints := func() []metareplication.CheckpointRow {
		return []metareplication.CheckpointRow{
			{Destination: "cdc", Topic: "cdc-topic", Found: true,
				Checkpoint: replicationkafka.CheckpointRow{Epoch: "e1", Cursor: 100, LastSnapshotAt: snapshotAt}},
			{Destination: "new", Topic: "new-topic"},
			{Destination: "torn", Topic: "torn-topic", Err: errors.New("corrupt checkpoint file")},
		}
	}
	eng := engineWithReplication(t, metareplication.Source{Feed: offlineFeed, Checkpoints: checkpoints})
	rows, _ := querySQL(t, eng, "SELECT "+allColumns+" FROM _meta.replication")
	want := map[string]string{
		"id":               "cdc,new,torn",
		"topic":            "cdc-topic,new-topic,torn-topic",
		"state":            "<null>,<null>,<null>",
		"epoch":            "e1,<null>,<null>",
		"cursor":           "<null>,<null>,<null>",
		"checkpointCursor": "100,<null>,<null>",
		"produced":         "127,127,127",
		"floor":            "50,50,50",
		"feedDiskBytes":    "1048576,1048576,1048576",
		"feedCacheBytes":   "<null>,<null>,<null>",
		"feedSegments":     "3,3,3",
		"lagSequences":     "<null>,<null>,<null>",
		"lagBytes":         "<null>,<null>,<null>",
		"lastResetReason":  "<null>,<null>,<null>",
		"lastSnapshotAt":   "1757000000500,<null>,<null>",
		"lastSuccessAt":    "<null>,<null>,<null>",
		"lastError":        "<null>,<null>,checkpoint unreadable: corrupt checkpoint file",
	}
	for col, w := range want {
		if got := column(rows, col); got != w {
			t.Errorf("%s = %s, quero %s", col, got, w)
		}
	}
	// O publicador vivo é a fonte quando existe: os .ckpt não viram linhas
	// a mais ao lado dele.
	both := engineWithReplication(t, metareplication.Source{Feed: liveFeed, Destinations: twoDestinations, Checkpoints: checkpoints})
	if rows, _ := querySQL(t, both, "SELECT id FROM _meta.replication"); column(rows, "id") != "cdc,tems" {
		t.Fatalf("com publicador vivo as linhas são as dele: %v", rows)
	}
}

// O nome das colunas é contrato (o SELECT * de quem monta painel): a lista é
// LITERAL aqui, e não a do código. A ordem do SELECT * é a dos FieldIDs, que
// o compilador da spec atribui em ordem alfabética, como em qualquer dataset.
func TestMetaReplicationNamesColumnsLiterally(t *testing.T) {
	eng := engineWithReplication(t, metareplication.Source{Feed: liveFeed, Destinations: twoDestinations})
	_, names := querySQL(t, eng, "SELECT * FROM _meta.replication")
	want := []string{
		"checkpointCursor", "cursor", "epoch", "feedCacheBytes", "feedDiskBytes", "feedSegments",
		"floor", "id", "lagBytes", "lagSequences", "lastError", "lastResetReason", "lastSnapshotAt",
		"lastSuccessAt", "produced", "state", "topic",
	}
	if !slices.Equal(names, want) {
		t.Fatalf("colunas = %v\nquero      %v", names, want)
	}
}

// O tipo de cada coluna é contrato tanto quanto o nome: um timestamp que
// virasse int64 continuaria saindo número no NDJSON e mudaria de tipo no
// driver. O mapa é LITERAL, e cobre as 17 colunas (nenhuma a mais, nenhuma a
// menos), lido do schema compilado que o motor usa.
func TestMetaReplicationColumnTypesAreTheContract(t *testing.T) {
	sc, _, err := metareplication.Table(metareplication.Source{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]types.FieldType{
		"id": types.TypeString, "topic": types.TypeString, "state": types.TypeString, "epoch": types.TypeString,
		"cursor": types.TypeInt64, "checkpointCursor": types.TypeInt64, "produced": types.TypeInt64, "floor": types.TypeInt64,
		"feedDiskBytes": types.TypeInt64, "feedCacheBytes": types.TypeInt64, "feedSegments": types.TypeInt64,
		"lagSequences": types.TypeInt64, "lagBytes": types.TypeInt64,
		"lastResetReason": types.TypeString, "lastSnapshotAt": types.TypeTimestamp, "lastSuccessAt": types.TypeTimestamp,
		"lastError": types.TypeString,
	}
	got := map[string]types.FieldType{}
	for _, f := range sc.FieldsByID() {
		got[f.Name] = f.Type
	}
	if len(got) != len(want) {
		t.Errorf("%d colunas no schema, quero %d: %v", len(got), len(want), got)
	}
	for name, typ := range want {
		if g, ok := got[name]; !ok || g != typ {
			t.Errorf("coluna %s com tipo %v (presente: %v), quero %v", name, g, ok, typ)
		}
	}
}

// Dataset sem replicação (nenhum closure) responde a tabela VAZIA, nunca
// erro; e sem feed as colunas do feed são NULL, não zero.
func TestMetaReplicationWithoutReplicationAnswersNoRows(t *testing.T) {
	if rows, _ := querySQL(t, engineWithReplication(t, metareplication.Source{}), "SELECT * FROM _meta.replication"); len(rows) != 0 {
		t.Fatalf("sem replicação a tabela é vazia: %v", rows)
	}
	eng := engineWithReplication(t, metareplication.Source{Destinations: twoDestinations})
	rows, _ := querySQL(t, eng, "SELECT produced, floor, feedDiskBytes, feedCacheBytes, feedSegments FROM _meta.replication")
	for _, col := range []string{"produced", "floor", "feedDiskBytes", "feedCacheBytes", "feedSegments"} {
		if got := column(rows, col); got != "<null>,<null>" {
			t.Fatalf("%s = %s sem feed; quero NULL", col, got)
		}
	}
}

// O point lookup pelo id enxerga a mesma linha que o scan.
func TestMetaReplicationPointLookupFindsTheDestination(t *testing.T) {
	eng := engineWithReplication(t, metareplication.Source{Feed: liveFeed, Destinations: twoDestinations})
	rows, _ := querySQL(t, eng, "SELECT id, lagBytes FROM _meta.replication WHERE id = 'tems'")
	if len(rows) != 1 || rows[0]["id"] != "tems" || rows[0]["lagBytes"] != "9000" {
		t.Fatalf("lookup de tems = %v", rows)
	}
	if rows, _ := querySQL(t, eng, "SELECT id FROM _meta.replication WHERE id = 'nope'"); len(rows) != 0 {
		t.Fatalf("destino inexistente achado: %v", rows)
	}
}

// O retrato é colhido UMA vez por consulta, na abertura: as colunas de todas
// as linhas saem do mesmo Stats de cada publicador. Lido linha a linha (ou
// coluna a coluna) no presente, o lagBytes de uma linha poderia ser de outro
// instante que o cursor da mesma linha. O retrato também é devolvido no fim.
func TestMetaReplicationReadsThePublishersOncePerQuery(t *testing.T) {
	calls := 0
	src := metareplication.Source{Feed: liveFeed, Destinations: func() []replicationkafka.Stats {
		calls++
		return twoDestinations()
	}}
	_, st, err := metareplication.Table(src)
	if err != nil {
		t.Fatal(err)
	}
	eng := engineWithReplication(t, src)
	querySQL(t, eng, "SELECT "+allColumns+" FROM _meta.replication WHERE lagSequences > 0")
	if calls != 1 {
		t.Fatalf("Destinations chamado %d vezes numa consulta; quero 1", calls)
	}
	snap := st.OpenSnapshot()
	if n := metareplication.OpenPicturesForTest(st); n != 1 {
		t.Fatalf("%d retratos abertos, quero 1", n)
	}
	st.CloseSnapshot(snap)
	if n := metareplication.OpenPicturesForTest(st); n != 0 {
		t.Fatalf("o CloseSnapshot deixou %d retratos", n)
	}
	if _, _, _, err := st.GetByRowID(types.Snapshot{Epoch: snap.Epoch, LimitRowID: snap.LimitRowID}, 1, nil); err == nil || !strings.Contains(err.Error(), "is not open") {
		t.Fatalf("snapshot fechado tem de ser recusado como defeito: %v", err)
	}
}
