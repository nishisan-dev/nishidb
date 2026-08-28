package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// O par de contadores separa "saturado" de "sem produção", que é o que a
// profundidade da fila NÃO consegue fazer: ela é amostra instantânea e fica em
// zero nos dois casos.

// Fila cheia: quem espera é o POLLER, e só ele.
//
// Este teste também trava a propriedade que o primeiro desenho não tinha: a
// espera EM CURSO é visível. Ele lê Tempos() enquanto o poller ainda está
// bloqueado, e não depois — um contador que só somasse ao fim da espera ficaria
// imóvel durante o travamento e daria um salto no fim, ou nunca somaria se a
// espera não terminasse. Foi assim no incidente de 27/08/2026, em que o poller
// ficou preso uma hora.
func TestFilaCheiaContaEsperaDoPollerENaoDoWriter(t *testing.T) {
	f := NovaFila(1, 1<<20)
	ctx := context.Background()
	if err := f.Envia(ctx, &kgo.Record{Value: []byte("a")}); err != nil {
		t.Fatal(err)
	}
	// A fila tem capacidade 1 e já está cheia: este Envia bloqueia até alguém
	// consumir.
	pronto := make(chan error, 1)
	go func() { pronto <- f.Envia(ctx, &kgo.Record{Value: []byte("b")}) }()

	// Espera o poller estar de fato bloqueado antes de drenar, senão o teste
	// mede a corrida e não a espera.
	prazo := time.After(3 * time.Second)
	for {
		if b, _ := f.Tempos(); b > 0 {
			break
		}
		select {
		case <-prazo:
			t.Fatal("o poller não acumulou espera com a fila cheia")
		case <-time.After(time.Millisecond):
		}
	}
	// Drena um item, liberando o poller.
	f.recebeOuAcorda(ctx, nil)
	if err := <-pronto; err != nil {
		t.Fatalf("Envia: %v", err)
	}

	bloqueado, ocioso := f.Tempos()
	if bloqueado <= 0 {
		t.Fatal("espera do poller não foi contada")
	}
	if ocioso != 0 {
		t.Fatalf("writer contou %v de ociosidade sem nunca ter esperado", ocioso)
	}
}

// Fila vazia: quem espera é o WRITER, e só ele.
func TestFilaVaziaContaOciosidadeDoWriterENaoDoPoller(t *testing.T) {
	f := NovaFila(4, 1<<20)
	ctx, cancela := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancela()

	// Nada na fila: o writer bloqueia até o contexto acabar.
	if _, ok, _ := f.recebeOuAcorda(ctx, nil); ok {
		t.Fatal("recebeu item de uma fila vazia")
	}
	bloqueado, ocioso := f.Tempos()
	if ocioso <= 0 {
		t.Fatal("ociosidade do writer não foi contada")
	}
	if bloqueado != 0 {
		t.Fatalf("poller contou %v de bloqueio sem nunca ter enviado", bloqueado)
	}
}

// O caminho rápido não conta nada, e é o que mantém a instrumentação honesta:
// um contador que sobe em regime não distingue mais nada.
func TestSemEsperaNadaEContado(t *testing.T) {
	f := NovaFila(8, 1<<20)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := f.Envia(ctx, &kgo.Record{Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, ok, _ := f.recebeOuAcorda(ctx, nil); !ok {
			t.Fatal("item sumiu")
		}
	}
	if b, o := f.Tempos(); b != 0 || o != 0 {
		t.Fatalf("caminho rápido contou espera: bloqueado=%v ocioso=%v", b, o)
	}
}

// O DESPERTADOR CONTINUA SENDO VISTO COM A FILA CHEIA.
//
// Este é o teste que protege o débito 56 da instrumentação. A medição exigiu
// uma tentativa não bloqueante antes do select que espera, e a forma ingênua —
// tentar só o f.ch primeiro — faria a fila vencer sempre que tivesse item: o
// tique do despertador só seria visto com a fila vazia, e o checkpoint por
// TEMPO deixaria de disparar exatamente sob carga, que é quando ele importa.
//
// Por isso o `acorda` entra no mesmo select do caminho rápido. Aqui a fila tem
// item E o despertador está pronto; ao longo de muitas rodadas o tique tem de
// aparecer, que é a escolha aleatória do Go entre casos prontos sendo
// preservada.
func TestDespertadorNaoEEngolidoPelaFilaCheia(t *testing.T) {
	ctx := context.Background()
	var viuTique int
	const rodadas = 400
	for i := 0; i < rodadas; i++ {
		f := NovaFila(4, 1<<20)
		if err := f.Envia(ctx, &kgo.Record{Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
		tique := make(chan time.Time, 1)
		tique <- time.Now()
		if _, _, acordou := f.recebeOuAcorda(ctx, tique); acordou {
			viuTique++
		}
	}
	if viuTique == 0 {
		t.Fatalf("em %d rodadas com fila cheia E despertador pronto, o tique NUNCA foi visto; "+
			"o checkpoint por tempo deixou de disparar sob carga (débito 56)", rodadas)
	}
}
