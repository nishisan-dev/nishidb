package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// benchColeta é o molde de internal/cli/compaction_e2e_test.go:156: um
// broker kfake real (TCP, não dublê de interface), aqui usado para provar o
// consumo DIRETO de partição — sem group, sem commit — que este pacote
// implementa por cima do franz-go.
type benchColeta struct {
	t        *testing.T
	cluster  *kfake.Cluster
	endereco string
	topic    string
}

func newBenchColeta(t *testing.T, partitions int32) *benchColeta {
	t.Helper()
	const topic = "identifier-len-teste"
	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(partitions, topic),
		kfake.NumBrokers(1),
		kfake.ClusterID("bancada-identifier-len"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return &benchColeta{t: t, cluster: cluster, endereco: cluster.ListenAddrs()[0], topic: topic}
}

// produz publica registros, opcionalmente numa partição fixa (ManualPartitioner:
// o particionador default ignora r.Partition e escolhe pela key).
func (b *benchColeta) produz(recs ...*kgo.Record) {
	b.t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.endereco),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		b.t.Fatalf("produtor: %v", err)
	}
	defer cl.Close()
	for _, r := range recs {
		r.Topic = b.topic
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		b.t.Fatalf("produzir: %v", err)
	}
}

// groupsInCluster lista os consumer groups conhecidos do broker — a prova de
// que o consumo direto de partição não criou grupo nenhum.
func (b *benchColeta) groupsInCluster(t *testing.T) []string {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.endereco))
	if err != nil {
		t.Fatalf("cliente admin: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	groups, err := adm.ListGroups(ctx)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	nomes := make([]string, 0, len(groups))
	for name := range groups {
		nomes = append(nomes, name)
	}
	return nomes
}

func event(id, identifier string) *kgo.Record {
	return &kgo.Record{
		Key:   []byte(id),
		Value: []byte(fmt.Sprintf(`{"id":%q,"identifier":%q}`, id, identifier)),
	}
}

func eventNaPartition(id, identifier string, p int32) *kgo.Record {
	r := event(id, identifier)
	r.Partition = p
	return r
}

// TestColetaHistogramaCompletoENenhumOffsetCommitado é o critério de aceite
// da Task A5 para a ferramenta: produz identifiers de tamanhos conhecidos —
// incluindo um acima do teto de 1024 (a cauda que a issue #15 investiga,
// medida no piloto em 1099 e 1074 bytes, docs/piloto-events.md) —, roda a
// Coleta contra o broker kfake e confere:
//
//  1. o histograma bate exatamente com o que foi produzido, nas duas
//     partições, incluindo os baldes sem_campo/inválido;
//  2. nenhum consumer group existe no broker depois da rodada — a prova de
//     que o consumo é direto de partição, sem group e sem commit (um group
//     que não existe não tem posição para commitar).
func TestColetaHistogramaCompletoENenhumOffsetCommitado(t *testing.T) {
	if testing.Short() {
		t.Skip("sobe um broker kfake; roda na suíte completa")
	}
	b := newBenchColeta(t, 2)

	grande := strings.Repeat("k", 1099) // acima do teto de 1024 (MaxKeySize)
	naCauda := strings.Repeat("k", 1074)
	b.produz(
		eventNaPartition("a", "0123456789", 0),             // 10 bytes
		eventNaPartition("b", strings.Repeat("x", 300), 0), // 300 bytes
		eventNaPartition("c", grande, 0),                   // 1099 bytes
		&kgo.Record{Topic: b.topic, Key: []byte("d"), Value: []byte(`{isso nao e json}`), Partition: 0},
		&kgo.Record{Topic: b.topic, Key: []byte("e"), Value: []byte(`{"outroCampo":"z"}`), Partition: 1},
		eventNaPartition("f", naCauda, 1), // 1074 bytes
		eventNaPartition("g", strings.Repeat("y", 800), 1),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	hist, err := Coleta(ctx, ConfigColeta{
		Brokers: []string{b.endereco},
		Topic:   b.topic,
		Field:   "identifier",
	}, nil)
	if err != nil {
		t.Fatalf("Coleta: %v", err)
	}

	if hist.TotalLido() != 7 {
		t.Errorf("TotalLido = %d, quer 7", hist.TotalLido())
	}
	if hist.WithField() != 5 {
		t.Errorf("WithField = %d, quer 5", hist.WithField())
	}
	if hist.WithoutField() != 1 {
		t.Errorf("WithoutField = %d, quer 1", hist.WithoutField())
	}
	if hist.Invalidos() != 1 {
		t.Errorf("Invalidos = %d, quer 1", hist.Invalidos())
	}
	if hist.Maximo() != 1099 {
		t.Errorf("Maximo = %d, quer 1099", hist.Maximo())
	}
	querTop3 := []int{1099, 1074, 800}
	if got := hist.TopN(3); !equalInts(got, querTop3) {
		t.Errorf("TopN(3) = %v, quer %v", got, querTop3)
	}

	querBuckets := map[string]int64{"≤256": 1, "≤512": 1, "≤768": 0, "≤1024": 1, ">1024": 2}
	for _, bk := range hist.Buckets() {
		if querBuckets[bk.Rotulo] != bk.Count {
			t.Errorf("bucket %q = %d, quer %d", bk.Rotulo, bk.Count, querBuckets[bk.Rotulo])
		}
	}

	if groups := b.groupsInCluster(t); len(groups) != 0 {
		t.Errorf("consumer groups no broker após a Coleta: %v — o consumo tinha de ser direto de partição, sem group nenhum", groups)
	}
}

// TestColetaSegundaRodadaReleParticaoInteiraDoZero prova a consequência
// observável de não haver commit nenhum: sem posição persistida, uma segunda
// chamada a Coleta relê o tópico inteiro desde o earliest — incluindo o que a
// primeira rodada já tinha lido. Se algum offset tivesse sido commitado (e
// honrado), a segunda rodada veria só as mensagens novas.
func TestColetaSegundaRodadaReleParticaoInteiraDoZero(t *testing.T) {
	if testing.Short() {
		t.Skip("sobe um broker kfake; roda na suíte completa")
	}
	b := newBenchColeta(t, 1)
	b.produz(event("a", "primeira-rodada"))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := ConfigColeta{Brokers: []string{b.endereco}, Topic: b.topic, Field: "identifier"}

	h1, err := Coleta(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("primeira Coleta: %v", err)
	}
	if h1.TotalLido() != 1 {
		t.Fatalf("primeira Coleta: TotalLido = %d, quer 1", h1.TotalLido())
	}

	b.produz(event("b", "segunda-rodada"))
	h2, err := Coleta(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("segunda Coleta: %v", err)
	}
	if h2.TotalLido() != 2 {
		t.Errorf("segunda Coleta: TotalLido = %d, quer 2 (releu desde earliest, sem posição commitada da primeira)", h2.TotalLido())
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
