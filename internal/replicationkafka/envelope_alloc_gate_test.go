//go:build !race

package replicationkafka_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/replicationkafka"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestEnvelopeRenderAllocatesNothing é o gate de zero alocação por registro
// em regime: com o buffer do chamador reaproveitado e o cache de rótulos já
// aquecido (o AllocsPerRun roda uma vez antes de medir), renderizar não pode
// alocar nada.
//
// A passada medida cobre TODOS os caminhos que o publicador exercita por
// registro — insert, update, delete, snapshot, a linha da histórica, os dois
// retains, os marcadores (reset com e sem previousEpoch, snapshot_end e
// resume), o header de motivo, a key do registro Kafka e os dois lados da
// validação de json (válido inline, inválido como string) —, porque o gate só
// protege o caminho que ele exercita. As linhas têm payload real, com o json
// inline, o datetime, a coluna com alias e as colunas ocultas.
func TestEnvelopeRenderAllocatesNothing(t *testing.T) {
	r, base, hist := newGoldenRenderer(t)
	state := alarmPayload(t, base, 3)
	histPayload := payloadOf(t, hist,
		namedValue{"identifier", str("ALM-1")},
		namedValue{"severity", i64(3)},
		namedValue{schema.WriteInstantFieldName, ts(1790000001000)},
		namedValue{schema.RawFieldName, doc(`{"identifier":"ALM-1","severity":3}`)},
	)
	// O `_raw` de evento ruim exercita o caminho INVÁLIDO da validação de
	// json (sai como string); o `persistent` do alarme, o VÁLIDO (inline).
	badRaw := func(raw string) []byte {
		return payloadOf(t, hist,
			namedValue{schema.WriteInstantFieldName, ts(1790000001000)},
			namedValue{schema.RawFieldName, doc(raw)},
		)
	}
	brokenJSON := badRaw(`{"id": "A", broken`)
	brokenUTF8 := badRaw("{\"id\":\"U\xff1\"}")
	at := time.UnixMilli(1790000002456)
	entries := []replication.Entry{
		{Sequence: 41, Table: "events", Operation: replication.Insert, Record: types.Record{RowID: 7, Key: []byte("ALM-1"), Payload: state}, At: at},
		{Sequence: 42, Table: "events", Operation: replication.Update, Record: types.Record{RowID: 7, Key: []byte("ALM-1"), Payload: state}, At: at},
		{Sequence: 43, Table: "events", Operation: replication.Delete, Record: types.Record{RowID: 7, Key: []byte("ALM-1"), Payload: state}, At: at},
		{Sequence: 44, Table: "events_history", Operation: replication.Insert, Record: types.Record{RowID: 9001, Payload: histPayload}, At: at},
		{Sequence: 45, Table: "events_history", Operation: replication.Retain, Record: types.Record{RowID: 8000}, At: at},
		{Sequence: 46, Table: "events_history", Operation: replication.Insert, Record: types.Record{RowID: 9002, Payload: brokenJSON}, At: at},
		{Sequence: 47, Table: "events_history", Operation: replication.Insert, Record: types.Record{RowID: 9003, Payload: brokenUTF8}, At: at},
	}
	snapshot := []replication.Entry{
		{Table: "events", Operation: replication.Insert, Record: types.Record{RowID: 7, Key: []byte("ALM-1"), Payload: state}},
		{Table: "events_history", Operation: replication.Retain, Record: types.Record{RowID: 8000}},
	}

	buf := make([]byte, 0, 4096)
	key := make([]byte, 0, 64)
	var failure error
	allocs := testing.AllocsPerRun(200, func() {
		var err error
		for _, e := range entries {
			if buf, err = r.AppendEntry(buf[:0], goldenEpoch, e); err != nil {
				failure = err
			}
			if key, err = r.KafkaKey(key[:0], e); err != nil {
				failure = err
			}
		}
		for _, e := range snapshot {
			if buf, err = r.AppendSnapshot(buf[:0], goldenEpoch, 100, at, e); err != nil {
				failure = err
			}
		}
		if buf, err = r.AppendReset(buf[:0], goldenEpoch, "", replicationkafka.ResetGap, 100, at); err != nil {
			failure = err
		}
		// O reset com previousEpoch exercita o cache do fragmento: a primeira
		// passada (a de aquecimento do AllocsPerRun) o monta, as medidas o
		// reaproveitam.
		if buf, err = r.AppendReset(buf[:0], goldenEpoch, goldenPreviousEpoch, replicationkafka.ResetCrash, 0, at); err != nil {
			failure = err
		}
		buf = r.AppendSnapshotEnd(buf[:0], goldenEpoch, 100, 17, at)
		buf = r.AppendResume(buf[:0], goldenEpoch, 100, at)
		_ = replicationkafka.OpHeader(replicationkafka.OpSnapshotEnd)
		_ = replicationkafka.ReasonHeader(replicationkafka.ResetPeriodic)
	})
	if failure != nil {
		t.Fatalf("render falhou dentro da passada medida: %v", failure)
	}
	// A passada tem de ter exercitado os DOIS caminhos, senão o zero não
	// prova nada sobre um deles.
	valid, err := r.AppendEntry(nil, goldenEpoch, entries[1])
	if err != nil || !bytes.Contains(valid, []byte(`"persistent":{"probe"`)) {
		t.Fatalf("o caminho válido não saiu inline: %s (%v)", valid, err)
	}
	for _, e := range entries[5:] {
		invalid, err := r.AppendEntry(nil, goldenEpoch, e)
		if err != nil || !bytes.Contains(invalid, []byte(`"_raw":"`)) {
			t.Fatalf("o caminho inválido não saiu como string: %s (%v)", invalid, err)
		}
	}
	// Os marcadores novos têm de ter saído com a forma do contrato: sem isto o
	// zero poderia vir de um caminho que não escreveu nada.
	resume := r.AppendResume(nil, goldenEpoch, 100, at)
	reset, err := r.AppendReset(nil, goldenEpoch, goldenPreviousEpoch, replicationkafka.ResetCrash, 0, at)
	if err != nil || !bytes.Contains(resume, []byte(`"op":"resume"`)) ||
		!bytes.Contains(reset, []byte(`"reason":"crash","previousEpoch":"`+goldenPreviousEpoch+`"`)) ||
		string(replicationkafka.ReasonHeader(replicationkafka.ResetPeriodic)) != "periodic" {
		t.Fatalf("a passada medida não exercitou os marcadores novos: %s | %s (%v)", resume, reset, err)
	}
	if allocs != 0 {
		t.Errorf("renderizar um lote em regime alocou %.1f vezes por passada; quer 0", allocs)
	}
}
