package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/logging"
)

// TestAoAtribuirEsperaAdmProntoAntesDeTocarAdm é o oráculo do débito 60.
//
// O franz-go entrega partições (chama OnPartitionsAssigned) assim que o
// metadata loop descobre o tópico em background — não só no primeiro Poll —,
// e isso pode acontecer ANTES de kgo.NewClient devolver o cliente para
// `Abre`. Até esta correção, `aoAtribuir` tocava `c.adm` incondicionalmente, e
// `c.adm` só é atribuído DEPOIS que `kgo.NewClient` retorna: uma entrega
// nessa janela achava `c.adm` nil e o processo morria de SIGSEGV inteiro, não
// só a ingestão.
//
// O oráculo é determinístico, sem depender de timing de broker: a goroutine
// chama `aoAtribuir` com `c.admPronto` ainda ABERTO (simulando o construtor
// no meio do caminho) e um panic é capturado por `recover` — reproduzindo o
// nil deref como falha de teste em vez de derrubar o binário do `go test`
// inteiro, que é o que aconteceria sem a captura. Só depois o teste fecha
// `admPronto` (como `Abre` faria) e exige que a goroutine prossiga.
func TestAoAtribuirEsperaAdmProntoAntesDeTocarAdm(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(1, "topico-teste"),
		kfake.NumBrokers(1),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	defer cluster.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	c := &Consumidor{
		cfg:       &Config{Topic: "topico-teste", GroupID: "grupo-teste"},
		log:       logging.Descartado().Component("kafka").Dataset("teste"),
		met:       registraMetricas(nil, "teste"),
		admPronto: make(chan struct{}),
	}

	// terminou recebe o valor de recover(): nil se aoAtribuir voltou sem
	// panicar, o valor do panic caso contrário.
	terminou := make(chan any, 1)
	go func() {
		defer func() { terminou <- recover() }()
		c.aoAtribuir(context.Background(), nil, map[string][]int32{"topico-teste": {0}})
	}()

	// Antes da liberação, aoAtribuir tem de estar BLOQUEADA. Sem a barreira,
	// o toque em c.adm nil é imediato — não é preciso corrida de relógio para
	// pegar o defeito, só para confirmar que o comportamento correto (esperar)
	// se mantém por um intervalo razoável.
	select {
	case v := <-terminou:
		if v != nil {
			t.Fatalf("aoAtribuir tocou c.adm nil antes de admPronto fechar (débito 60): %v", v)
		}
		t.Fatal("aoAtribuir retornou antes de admPronto fechar; a barreira do débito 60 não está em vigor")
	case <-time.After(150 * time.Millisecond):
		// Ainda bloqueada — o esperado.
	}

	// Libera o gate como o construtor faria: adm passa a existir e só DEPOIS
	// o canal fecha.
	c.adm = kadm.NewClient(cl)
	close(c.admPronto)

	select {
	case v := <-terminou:
		if v != nil {
			t.Fatalf("aoAtribuir paniquei após a liberação do gate: %v", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("aoAtribuir não prosseguiu depois de admPronto fechar")
	}
}

// TestAjustaOffsetsEsperaOffsetsProntoAntesDeLerOMapa é o oráculo do débito
// 69 — a janela-irmã do 60, achada pela revisão do próprio fechamento dele.
//
// O franz-go chama onAssigned (aoAtribuir) e SÓ DEPOIS, numa goroutine
// própria disparada assim que aoAtribuir retorna, busca os offsets do group e
// invoca AdjustFetchOffsetsFn (ajustaOffsets). admPronto (débito 60) fecha
// logo depois de kgo.NewClient devolver — bem ANTES de resolveOrigemEPosicao
// terminar de escrever c.offsetsIniciais, que faz I/O de rede (metadata) e de
// disco (checkpoint local). Isso deixa uma janela em que aoAtribuir já
// retornou — e a goroutine de fetchOffsets já pode estar chamando
// ajustaOffsets — enquanto resolveOrigemEPosicao ainda está no meio da
// escrita do mapa: leitura concorrente com mapassign, capturada ao vivo pelo
// -race no CI (run 32601376137, TestSpec105DuplicateDelivery), não hipótese.
//
// O oráculo é o mesmo desenho do 60: determinístico, sem depender de timing
// de broker nem do detector de corrida. A goroutine chama `ajustaOffsets` com
// `c.offsetsPronto` ainda ABERTO (simulando resolveOrigemEPosicao no meio do
// caminho) e o teste exige que ela fique BLOQUEADA por um intervalo razoável
// — sem tocar o mapa, que aqui está deliberadamente nil, o estado real antes
// da escrita. Só depois o teste escreve o mapa e fecha `offsetsPronto` (como
// `Abre` faria) e exige que a goroutine prossiga e aplique o seek certo.
func TestAjustaOffsetsEsperaOffsetsProntoAntesDeLerOMapa(t *testing.T) {
	c := &Consumidor{
		cfg:           &Config{Topic: "topico-teste"},
		log:           logging.Descartado().Component("kafka").Dataset("teste"),
		met:           registraMetricas(nil, "teste"),
		offsetsPronto: make(chan struct{}),
	}

	doGroup := map[string]map[int32]kgo.Offset{
		"topico-teste": {0: kgo.NewOffset().At(5)},
	}

	type resultado struct {
		saida map[string]map[int32]kgo.Offset
		err   error
		panic any
	}
	terminou := make(chan resultado, 1)
	go func() {
		var r resultado
		defer func() { r.panic = recover(); terminou <- r }()
		r.saida, r.err = c.ajustaOffsets(context.Background(), doGroup)
	}()

	// Antes da liberação, ajustaOffsets tem de estar BLOQUEADA. c.offsetsIniciais
	// é nil neste momento — exatamente o estado que a corrida do débito 69
	// expunha ao leitor concorrente.
	select {
	case r := <-terminou:
		t.Fatalf("ajustaOffsets leu c.offsetsIniciais antes de offsetsPronto fechar (débito 69): saida=%+v err=%v panic=%v",
			r.saida, r.err, r.panic)
	case <-time.After(150 * time.Millisecond):
		// Ainda bloqueada — o esperado.
	}

	// Libera o gate como Abre faria: o mapa ganha seu valor final e só DEPOIS
	// o canal fecha.
	c.offsetsIniciais = map[int32]int64{0: 42}
	close(c.offsetsPronto)

	select {
	case r := <-terminou:
		if r.panic != nil {
			t.Fatalf("ajustaOffsets paniquei após a liberação do gate: %v", r.panic)
		}
		if r.err != nil {
			t.Fatalf("ajustaOffsets err = %v, quer nil", r.err)
		}
		got := r.saida["topico-teste"][0].EpochOffset()
		if got.Offset != 42 {
			t.Fatalf("offset ajustado = %+v, quer seek para 42 (posição local do checkpoint)", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ajustaOffsets não prosseguiu depois de offsetsPronto fechar")
	}
}
