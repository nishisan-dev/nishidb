package kafka_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
)

// Os OITO testes que o §105 marca como obrigatórios, um por função, com o
// nome da spec no comentário para que a rastreabilidade não dependa de
// memória de ninguém.

const prazo = 30 * time.Second

// §105.1 — duplicate delivery.
//
// At-least-once é consequência do §48 (`consume → materialize → persist →
// durable checkpoint → commit`): a duplicata é normal e tem de ser INÓCUA.
func TestSpec105DuplicateDelivery(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), evento("A", 2, "Stop"))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 2 }); err != nil {
		t.Fatal(err)
	}
	estado := leEstado(t, b, "A")

	// Reentrega dos MESMOS eventos, na mesma ordem de partição.
	b.produz(evento("A", 1, "Start"), evento("A", 2, "Stop"))
	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 2 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Stale != 2 {
		t.Errorf("stale = %d, quer 2: com sequence, a duplicata tem de ser descartada pelo gate do §25", d.Stale)
	}
	if depois := leEstado(t, b, "A"); depois != estado {
		t.Errorf("a reentrega mudou o estado:\n antes: %s\ndepois: %s", estado, depois)
	}
}

// §105.2 — offset recovery.
//
// O §126.9 é literal ("recuperar offsets"), e é o que separa retomar de
// reprocessar o tópico inteiro a cada reinício.
func TestSpec105OffsetRecovery(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), evento("B", 1, "Start"), evento("C", 1, "Start"))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 3 }); err != nil {
		t.Fatal(err)
	}
	offsets, existe := b.leOffsets()
	if !existe {
		t.Fatal("kafka.offsets não foi criado")
	}
	if offsets[0] != 3 {
		t.Fatalf("offset da partição 0 = %d, quer 3 (próximo a consumir)", offsets[0])
	}

	b.reinicia()

	// Nada novo no tópico: um consumidor que retomasse do zero reprocessaria
	// os três eventos.
	d, err := b.consomeAte(3*time.Second, func(d kafka.Desfecho) bool { return d.Eventos > 0 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Eventos != 0 {
		t.Errorf("eventos após reinício = %d, quer 0: o consumidor não retomou pelo offset persistido", d.Eventos)
	}
}

// §105.3 — multiple partitions.
func TestSpec105MultiplePartitions(t *testing.T) {
	b := novaBancada(t, 4)
	defer b.fecha()

	var recs []*kgo.Record
	for p := int32(0); p < 4; p++ {
		for i := 0; i < 5; i++ {
			recs = append(recs, eventoNaParticao(
				string(rune('A'+int(p))), int64(i+1), "Start", p))
		}
	}
	b.produz(recs...)

	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 20 }); err != nil {
		t.Fatal(err)
	}
	offsets, _ := b.leOffsets()
	if len(offsets) != 4 {
		t.Fatalf("offsets = %v, quer as 4 partições", offsets)
	}
	for p := int32(0); p < 4; p++ {
		if offsets[p] != 5 {
			t.Errorf("partição %d parou em %d, quer 5", p, offsets[p])
		}
	}
}

// §105.4 — crash BEFORE checkpoint.
//
// Distinto do 105.5: aqui nada foi tornado durável, então o offset não existe
// e o consumo recomeça do começo — sem buraco.
func TestSpec105CrashBeforeCheckpoint(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), evento("B", 1, "Start"))

	// Fecha o dataset SEM nunca ter consumido: é o crash antes de qualquer
	// persistência de posição.
	b.reinicia()
	if _, existe := b.leOffsets(); existe {
		t.Fatal("kafka.offsets existe sem nunca ter havido consumo")
	}

	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 2 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Aplicados != 2 {
		t.Errorf("aplicados = %d, quer 2: nenhum evento pode ter sido pulado", d.Aplicados)
	}
}

// §105.5 — crash AFTER append (e antes do checkpoint durável).
//
// O cenário que justifica a ordem manifest → offsets: os dados já estão no
// segmento, a posição ainda não. O offset REGRIDE e a duplicata resultante
// tem de ser inócua.
func TestSpec105CrashAfterAppend(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 1, "Start"), evento("A", 2, "Stop"), evento("A", 3, "Interim"))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 3 }); err != nil {
		t.Fatal(err)
	}
	estado := leEstado(t, b, "A")

	// Regride a posição à força, como um crash entre o rename do manifest e o
	// rename do kafka.offsets teria feito.
	o, _, err := b.st.SourceOffsets()
	if err != nil {
		t.Fatal(err)
	}
	o.Offsets[0] = 0
	if err := b.st.SetSourceOffsets(o); err != nil {
		t.Fatal(err)
	}
	if err := b.st.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	b.reinicia()

	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 3 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Eventos != 3 {
		t.Fatalf("eventos reprocessados = %d, quer 3", d.Eventos)
	}
	if depois := leEstado(t, b, "A"); depois != estado {
		t.Errorf("o reprocessamento NÃO foi inócuo:\n antes: %s\ndepois: %s", estado, depois)
	}
}

// §105.6 — restart.
func TestSpec105Restart(t *testing.T) {
	b := novaBancada(t, 2)
	defer b.fecha()

	b.produz(eventoNaParticao("A", 1, "Start", 0), eventoNaParticao("B", 1, "Start", 1))
	if _, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 2 }); err != nil {
		t.Fatal(err)
	}
	b.reinicia()

	b.produz(eventoNaParticao("A", 2, "Stop", 0))
	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 1 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Eventos != 1 {
		t.Errorf("eventos após restart = %d, quer 1 (só o novo)", d.Eventos)
	}
	if got := leEstado(t, b, "A"); !strings.Contains(got, "Stop") {
		t.Errorf("estado de A após restart = %s, quer o patch novo aplicado", got)
	}
}

// §105.7 — stale event.
//
// O §25 é explícito: `incoming > current` é o único gate, e o descarte por
// sequence NÃO é erro — tem métrica própria e não dispara política.
func TestSpec105StaleEvent(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	b.produz(evento("A", 5, "Stop"), evento("A", 2, "Start"), evento("A", 5, "Start"))
	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= 3 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Stale != 2 {
		t.Errorf("stale = %d, quer 2 (o menor e o igual)", d.Stale)
	}
	if d.Descartados != 0 || d.Falhas != 0 {
		t.Errorf("stale contou como descarte ou falha: %+v", d)
	}
	if got := leEstado(t, b, "A"); !strings.Contains(got, "Stop") {
		t.Errorf("estado = %s, quer o do evento de sequence maior", got)
	}
}

// §105.8 — queue backpressure.
//
// §29 e §128: a fila é limitada e o intake reduz sob saturação, sem perder
// mensagem. O que se prova aqui é o "sem perder mensagem": com fila
// minúscula, todo evento ainda chega.
func TestSpec105QueueBackpressure(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()

	// Fila de 1 registro e 1 byte: todo envio bloqueia no gate de bytes até o
	// writer drenar.
	b.cfg.QueueSize = 1
	b.cfg.QueueBytes = 1
	b.cfg.MaxPollRecords = 10

	const total = 50
	var recs []*kgo.Record
	for i := 0; i < total; i++ {
		recs = append(recs, evento(string(rune('a'+i%26))+string(rune('0'+i/26)), int64(i+1), "Start"))
	}
	b.produz(recs...)

	d, err := b.consomeAte(prazo, func(d kafka.Desfecho) bool { return d.Eventos >= total })
	if err != nil {
		t.Fatal(err)
	}
	if d.Eventos != total {
		t.Errorf("eventos = %d, quer %d: a fila limitada perdeu mensagem", d.Eventos, total)
	}
	if d.Falhas != 0 {
		t.Errorf("falhas = %d, quer 0", d.Falhas)
	}
	offsets, _ := b.leOffsets()
	if offsets[0] != total {
		t.Errorf("offset = %d, quer %d", offsets[0], total)
	}
}

// leEstado devolve o payload materializado da entidade, para comparar
// convergência antes e depois de um reprocessamento.
func leEstado(t *testing.T, b *bancada, id string) string {
	t.Helper()
	rec, _, ok, err := b.st.GetByKey(b.st.AbreSnapshot(), []byte(id), nil)
	if err != nil {
		t.Fatalf("GetByKey(%q): %v", id, err)
	}
	if !ok {
		return "<ausente>"
	}
	return string(rec.Payload)
}

// A guarda do D10: sem ela, duas instâncias no mesmo group dividem partições,
// cada dataset materializa metade e nenhuma métrica do §92 acusa.
func TestAssignmentParcialRecusaASubida(t *testing.T) {
	b := novaBancada(t, 4)
	defer b.fecha()

	// Um segundo membro no mesmo group força a divisão das partições.
	rival, err := kgo.NewClient(
		kgo.SeedBrokers(b.cluster.ListenAddrs()...),
		kgo.ConsumeTopics(b.cfg.Topic),
		kgo.ConsumerGroup(b.cfg.GroupID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rival.Close()

	// Produzir ANTES faz o poll do rival voltar na hora; sem isso ele
	// bloquearia até o prazo só para entrar no group.
	b.produz(evento("A", 1, "Start"))
	ctxRival, cancelaRival := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelaRival()
	rival.PollRecords(ctxRival, 1) // entra no group

	_, err = b.consomeAte(20*time.Second, func(kafka.Desfecho) bool { return false })
	if !errors.Is(err, kafka.ErrAssignmentParcial) {
		t.Errorf("erro = %v, quer ErrAssignmentParcial", err)
	}
}
