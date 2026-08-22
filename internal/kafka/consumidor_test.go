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
