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

// bancadaColeta é o molde de internal/cli/compaction_e2e_test.go:156: um
// broker kfake real (TCP, não dublê de interface), aqui usado para provar o
// consumo DIRETO de partição — sem group, sem commit — que este pacote
// implementa por cima do franz-go.
type bancadaColeta struct {
	t        *testing.T
	cluster  *kfake.Cluster
	endereco string
	topico   string
}

func novaBancadaColeta(t *testing.T, particoes int32) *bancadaColeta {
	t.Helper()
	const topico = "identifier-len-teste"
	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(particoes, topico),
		kfake.NumBrokers(1),
		kfake.ClusterID("bancada-identifier-len"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return &bancadaColeta{t: t, cluster: cluster, endereco: cluster.ListenAddrs()[0], topico: topico}
}

// produz publica registros, opcionalmente numa partição fixa (ManualPartitioner:
// o particionador default ignora r.Partition e escolhe pela key).
func (b *bancadaColeta) produz(recs ...*kgo.Record) {
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
		r.Topic = b.topico
	}
	ctx, cancela := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancela()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		b.t.Fatalf("produzir: %v", err)
	}
}

// gruposNoCluster lista os consumer groups conhecidos do broker — a prova de
// que o consumo direto de partição não criou grupo nenhum.
func (b *bancadaColeta) gruposNoCluster(t *testing.T) []string {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.endereco))
	if err != nil {
		t.Fatalf("cliente admin: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancela := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancela()
	grupos, err := adm.ListGroups(ctx)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	nomes := make([]string, 0, len(grupos))
	for nome := range grupos {
		nomes = append(nomes, nome)
	}
	return nomes
}

func evento(id, identifier string) *kgo.Record {
	return &kgo.Record{
		Key:   []byte(id),
		Value: []byte(fmt.Sprintf(`{"id":%q,"identifier":%q}`, id, identifier)),
	}
}

func eventoNaParticao(id, identifier string, p int32) *kgo.Record {
	r := evento(id, identifier)
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
	b := novaBancadaColeta(t, 2)

	grande := strings.Repeat("k", 1099) // acima do teto de 1024 (MaxKeySize)
	naCauda := strings.Repeat("k", 1074)
	b.produz(
		eventoNaParticao("a", "0123456789", 0),             // 10 bytes
		eventoNaParticao("b", strings.Repeat("x", 300), 0), // 300 bytes
		eventoNaParticao("c", grande, 0),                   // 1099 bytes
		&kgo.Record{Topic: b.topico, Key: []byte("d"), Value: []byte(`{isso nao e json}`), Partition: 0},
		&kgo.Record{Topic: b.topico, Key: []byte("e"), Value: []byte(`{"outroCampo":"z"}`), Partition: 1},
		eventoNaParticao("f", naCauda, 1), // 1074 bytes
		eventoNaParticao("g", strings.Repeat("y", 800), 1),
	)

	ctx, cancela := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancela()
	hist, err := Coleta(ctx, ConfigColeta{
		Brokers: []string{b.endereco},
		Topico:  b.topico,
		Campo:   "identifier",
	}, nil)
	if err != nil {
		t.Fatalf("Coleta: %v", err)
	}

	if hist.TotalLido() != 7 {
		t.Errorf("TotalLido = %d, quer 7", hist.TotalLido())
	}
	if hist.ComCampo() != 5 {
		t.Errorf("ComCampo = %d, quer 5", hist.ComCampo())
	}
	if hist.SemCampo() != 1 {
		t.Errorf("SemCampo = %d, quer 1", hist.SemCampo())
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
		if querBuckets[bk.Rotulo] != bk.Contagem {
			t.Errorf("bucket %q = %d, quer %d", bk.Rotulo, bk.Contagem, querBuckets[bk.Rotulo])
		}
	}

	if grupos := b.gruposNoCluster(t); len(grupos) != 0 {
		t.Errorf("consumer groups no broker após a Coleta: %v — o consumo tinha de ser direto de partição, sem group nenhum", grupos)
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
	b := novaBancadaColeta(t, 1)
	b.produz(evento("a", "primeira-rodada"))

	ctx, cancela := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancela()
	cfg := ConfigColeta{Brokers: []string{b.endereco}, Topico: b.topico, Campo: "identifier"}

	h1, err := Coleta(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("primeira Coleta: %v", err)
	}
	if h1.TotalLido() != 1 {
		t.Fatalf("primeira Coleta: TotalLido = %d, quer 1", h1.TotalLido())
	}

	b.produz(evento("b", "segunda-rodada"))
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
