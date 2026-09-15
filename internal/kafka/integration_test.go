//go:build integration

// Camada de INTEGRAÇÃO dos testes Kafka: broker de verdade, num container.
//
// A camada de unidade (kfake) cobre a lógica e roda em segundos, mas não
// exercita o protocolo real, o group coordinator real nem o comportamento de
// rede. Os dois cenários que só aparecem aqui são justamente os que mais
// custam em produção: retomada de posição contra um coordinator de verdade, e
// `kill -9` no meio do consumo (§126.25).
//
// O container é operado por `docker run` direto, e não por Testcontainers.
// É um DESVIO deliberado do que o plano do M1 registrou, e a razão é o custo:
// o testcontainers-go traz algumas dezenas de módulos para um projeto cuja
// identidade declarada é ter duas dependências (§97, binário único). O que se
// perde é o reaper de containers órfãos; o que se faz em troca é --rm mais
// um cleanup registrado, e um nome derivado do teste para que um órfão seja
// identificável.
package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
)

const imagemRedpanda = "redpandadata/redpanda:v24.2.18"

// produzComHash publica escolhendo a partição pelo HASH DA KEY, que é a
// recomendação do §26 (Kafka key igual à materialization key) e o que garante
// que os eventos de uma entidade caiam sempre na mesma partição.
func (b *bench) produzComHash(recs ...*kgo.Record) {
	b.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.endereco))
	if err != nil {
		b.t.Fatalf("produtor: %v", err)
	}
	defer cl.Close()
	for _, r := range recs {
		r.Topic = b.cfg.Topic
	}
	ctx, cancela := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancela()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		b.t.Fatalf("produzir: %v", err)
	}
}

// portaLivre reserva uma porta pedindo :0 ao sistema e devolvendo-a em
// seguida. Há uma janela de corrida teórica; é o padrão usado por suítes que
// precisam da porta antes do bind.
func portaLivre(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	porta := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return porta
}

// sobeRedpanda inicia um broker e devolve o endereço.
func sobeRedpanda(t *testing.T, particoes int) string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker ausente; a camada de integração precisa de um broker real")
	}
	porta := portaLivre(t)
	nome := "nishidb-it-" + strconv.Itoa(porta)

	args := []string{
		"run", "--rm", "-d", "--name", nome,
		"-p", fmt.Sprintf("%d:%d", porta, porta),
		imagemRedpanda,
		"redpanda", "start",
		"--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M",
		"--node-id", "0", "--check=false",
		"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", porta),
		"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://127.0.0.1:%d", porta),
	}
	saida, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Skipf("não foi possível subir o Redpanda: %v\n%s", err, saida)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", nome).Run()
	})

	endereco := "127.0.0.1:" + strconv.Itoa(porta)
	esperaBroker(t, endereco)
	criaTopico(t, nome, porta, particoes)
	return endereco
}

// esperaBroker espera por CONDIÇÃO (o broker responder metadados), e não por
// sleep: um sleep fixo esconde tanto a lentidão quanto a falha de subida.
func esperaBroker(t *testing.T, endereco string) {
	t.Helper()
	prazo := time.Now().Add(90 * time.Second)
	for time.Now().Before(prazo) {
		cl, err := kgo.NewClient(kgo.SeedBrokers(endereco))
		if err == nil {
			ctx, cancela := context.WithTimeout(context.Background(), 3*time.Second)
			erro := cl.Ping(ctx)
			cancela()
			cl.Close()
			if erro == nil {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("o broker em %s não respondeu a tempo", endereco)
}

func criaTopico(t *testing.T, container string, porta, particoes int) {
	t.Helper()
	saida, err := exec.Command("docker", "exec", container,
		"rpk", "topic", "create", "radius-accounting",
		"-p", strconv.Itoa(particoes),
		"--brokers", fmt.Sprintf("127.0.0.1:%d", porta)).CombinedOutput()
	if err != nil {
		t.Fatalf("criar tópico: %v\n%s", err, saida)
	}
}

// §126.2, §126.3 e §126.9 contra um broker real: conectar, consumir JSON e
// recuperar offsets.
func TestIntegracaoConsomeERetomaPelaPosicaoLocal(t *testing.T) {
	endereco := sobeRedpanda(t, 3)
	b := newBenchIn(t, endereco)
	defer b.close()

	b.produz(event("A", 1, "Start"), event("B", 1, "Start"), event("C", 1, "Start"))
	d, err := b.consomeUntil(60*time.Second, func(d kafka.Desfecho) bool { return d.Eventos >= 3 })
	if err != nil {
		t.Fatal(err)
	}
	if d.Aplicados != 3 {
		t.Fatalf("aplicados = %d, quer 3 (%+v)", d.Aplicados, d)
	}

	offsets, existe := b.readOffsets()
	if !existe {
		t.Fatal("kafka.offsets não foi criado")
	}
	var soma int64
	for _, o := range offsets {
		soma += o
	}
	if soma != 3 {
		t.Errorf("soma dos offsets = %d, quer 3: %v", soma, offsets)
	}

	// Reinício: nada novo no tópico, então um consumidor que não retomasse
	// pela posição local reprocessaria os três eventos.
	b.reinicia()
	d2, err := b.consomeUntil(20*time.Second, func(d kafka.Desfecho) bool { return d.Eventos > 0 })
	if err != nil {
		t.Fatal(err)
	}
	if d2.Eventos != 0 {
		t.Errorf("eventos após reinício = %d, quer 0", d2.Eventos)
	}
}

// §126.25 — sobreviver a `kill -9` sem perder estado confirmado.
//
// O teste é sobre o CONTRATO, não sobre o sinal: o que importa é que um
// encerramento sem oportunidade de limpeza deixe o dataset num ponto durável
// coerente, e que a retomada convirja. O `kill -9` de processo é exercitado no
// internal/cli; aqui o equivalente é abandonar o Store sem Close e reabrir,
// que é o mesmo estado em disco que o SIGKILL deixa.
func TestIntegracaoRetomadaAposEncerramentoAbrupto(t *testing.T) {
	endereco := sobeRedpanda(t, 1)
	b := newBenchIn(t, endereco)
	defer b.close()

	var recs []*kgo.Record
	for i := 1; i <= 20; i++ {
		recs = append(recs, event("A", int64(i), "Start"))
	}
	b.produz(recs...)

	if _, err := b.consomeUntil(60*time.Second, func(d kafka.Desfecho) bool { return d.Eventos >= 20 }); err != nil {
		t.Fatal(err)
	}
	estado := readState(t, b, "A")

	// Regride a posição: é o estado que um SIGKILL entre o rename do manifest
	// e o do kafka.offsets deixaria.
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

	if _, err := b.consomeUntil(60*time.Second, func(d kafka.Desfecho) bool { return d.Eventos >= 20 }); err != nil {
		t.Fatal(err)
	}
	if depois := readState(t, b, "A"); depois != estado {
		t.Errorf("o estado confirmado mudou após a retomada:\n antes: %s\ndepois: %s", estado, depois)
	}
}

// §105.3 contra particionamento real, com o produtor escolhendo pela KEY —
// que é a recomendação do §26 (Kafka key igual à materialization key).
func TestIntegracaoMultiplasParticoesComKeyReal(t *testing.T) {
	endereco := sobeRedpanda(t, 4)
	b := newBenchIn(t, endereco)
	defer b.close()

	const entidades = 40
	var recs []*kgo.Record
	for i := 0; i < entidades; i++ {
		id := fmt.Sprintf("e%03d", i)
		recs = append(recs, event(id, 1, "Start"), event(id, 2, "Stop"))
	}
	b.produzComHash(recs...)

	d, err := b.consomeUntil(90*time.Second, func(d kafka.Desfecho) bool { return d.Eventos >= 2*entidades })
	if err != nil {
		t.Fatal(err)
	}
	if d.Falhas != 0 || d.Descartados != 0 {
		t.Errorf("desfecho = %+v, quer 0 falhas e 0 descartes", d)
	}
	if got := b.st.RowCount(); got != entidades {
		t.Errorf("RowCount = %d, quer %d", got, entidades)
	}
	// Todas as entidades no estado do evento de sequence maior: é a prova de
	// que a ordem por partição foi respeitada de ponta a ponta.
	for i := 0; i < entidades; i++ {
		id := fmt.Sprintf("e%03d", i)
		if got := readState(t, b, id); !strings.Contains(got, "Stop") {
			t.Fatalf("entidade %s ficou em %s, quer o patch de _SEQ=2", id, got)
		}
	}
}

// A guarda do D10 contra um group coordinator real.
func TestIntegracaoAssignmentParcialRecusa(t *testing.T) {
	endereco := sobeRedpanda(t, 4)
	b := newBenchIn(t, endereco)
	defer b.close()

	rival, err := kgo.NewClient(
		kgo.SeedBrokers(endereco),
		kgo.ConsumeTopics(b.cfg.Topic),
		kgo.ConsumerGroup(b.cfg.GroupID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rival.Close()

	b.produz(event("A", 1, "Start"))
	ctx, cancela := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancela()
	rival.PollRecords(ctx, 1)

	_, err = b.consomeUntil(60*time.Second, func(d kafka.Desfecho) bool { return false })
	if !errors.Is(err, kafka.ErrAssignmentParcial) {
		t.Errorf("erro = %v, quer ErrAssignmentParcial", err)
	}
}
