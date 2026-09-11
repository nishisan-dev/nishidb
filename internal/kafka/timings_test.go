package kafka

import (
	"context"
	"errors"
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
	f := NewQueue(1, 1<<20)
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

// Fila vazia: quem espera é quem CONSOME a fila, e só ele.
//
// Desde a Fase 4 quem consome a fila são os DECODIFICADORES, não o writer: ele
// espera na esteira, e é de lá que sai o `nishidb_kafka_writer_idle_seconds_total`.
// Este acumulador virou o `decode_idle`, que é a espera SOMADA de K
// decodificadores — daí o nome do teste ter deixado de dizer "writer".
func TestFilaVaziaContaOciosidadeDeQuemConsomeENaoDoPoller(t *testing.T) {
	f := NewQueue(4, 1<<20)
	ctx, cancela := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancela()

	// Nada na fila: quem consome bloqueia até o contexto acabar.
	if _, ok, _ := f.recebeOuAcorda(ctx, nil); ok {
		t.Fatal("recebeu item de uma fila vazia")
	}
	bloqueado, ocioso := f.Tempos()
	if ocioso <= 0 {
		t.Fatal("ociosidade de quem consome não foi contada")
	}
	if bloqueado != 0 {
		t.Fatalf("poller contou %v de bloqueio sem nunca ter enviado", bloqueado)
	}
}

// O caminho rápido não conta nada, e é o que mantém a instrumentação honesta:
// um contador que sobe em regime não distingue mais nada.
func TestSemEsperaNadaEContado(t *testing.T) {
	f := NewQueue(8, 1<<20)
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
		f := NewQueue(4, 1<<20)
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

// Enfileirar numa fila JÁ FECHADA é erro, não panic.
//
// A barreira de quiesce é enfileirada por uma goroutine que não é o poller:
// aoRevogar roda na do group do franz-go e pode chegar depois de Run ter
// fechado a fila — o encerramento é justamente a janela em que uma revogação é
// mais provável, porque sair do group a provoca.
//
// Sem a proteção do ciclo de vida isto é `panic: send on closed channel`, que
// derruba o processo. Foi encontrado assim, pela suíte, ao mover a barreira de
// fim de lote para a revogação.
func TestEnfileirarEmFilaFechadaEErroENaoPanico(t *testing.T) {
	f := NewQueue(4, 1<<20)
	f.Fecha()

	defer func() {
		if v := recover(); v != nil {
			t.Fatalf("enfileirar em fila fechada paniquei: %v", v)
		}
	}()
	err := f.EnviaBarreira(context.Background(), newBarrier())
	if !errors.Is(err, ErrFilaFechada) {
		t.Fatalf("EnviaBarreira devolveu %v, quer ErrFilaFechada", err)
	}
}

// O quiesce só volta depois de o writer ter APLICADO tudo o que estava na
// fila. É a propriedade inteira que a barreira de fim de lote sustentava, agora
// paga uma vez por revogação em vez de uma vez por lote.
func TestQuiesceSoVoltaDepoisDeDrenar(t *testing.T) {
	st := &storeLento{demora: time.Millisecond}
	cfg := &Config{Dataset: "eventos", Topic: "eventos", GroupID: "g", Brokers: []string{"127.0.0.1:9092"}}
	fila := NewQueue(64, 1<<20)
	w := NewWriter(cfg, nil, st, fila, nil, registerMetrics(nil, "eventos"),
		map[int32]int64{0: 0}, Cadencia{Eventos: 1 << 30, Intervalo: time.Hour}, nil)

	// O writer é marcado como INICIADO sem que Run rode: é a precondição que
	// torna a afirmação deste teste possível de fazer. O Quiesce ganhou um
	// quarto caso — writer que nunca subiu devolve nil na hora, porque nada foi
	// aplicado e não há quem drene —, e sem esta marca o teste passaria por
	// aquele atalho em vez de pelo caminho da barreira.
	close(w.iniciado)

	// Enfileira barreira numa fila que já tem itens à frente dela. Sem ninguém
	// drenando, o Quiesce tem de ficar esperando.
	ctx, cancela := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancela()
	for i := 0; i < 8; i++ {
		if err := fila.Envia(ctx, &kgo.Record{Partition: 0, Offset: int64(i), Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	pronto := make(chan error, 1)
	go func() { pronto <- w.Quiesce(ctx) }()

	select {
	case err := <-pronto:
		t.Fatalf("Quiesce voltou sem ninguém ter drenado a fila: %v", err)
	case <-time.After(80 * time.Millisecond):
		// Ainda esperando — o esperado.
	}

	// Drena tudo até a barreira, como o writer faria.
	for {
		it, ok, _ := fila.recebeOuAcorda(ctx, nil)
		if !ok {
			t.Fatal("fila esvaziou sem entregar a barreira")
		}
		if it.Barreira != nil {
			it.Barreira.conclui(nil)
			break
		}
	}
	if err := <-pronto; err != nil {
		t.Fatalf("Quiesce: %v", err)
	}
	if st.chamadas != 0 {
		t.Fatalf("o dublê registrou %d checkpoints: quem checkpointa é o writer, não o Quiesce", st.chamadas)
	}
}

// O Quiesce volta na hora quando o writer NUNCA subiu.
//
// É o quarto caso, e o que faltava. O plano nomeou três — barreira pronta,
// writer que já saiu, contexto —, e o group do franz-go pode revogar partições
// durante o próprio Abre, antes de o Consumidor.Run ter lançado goroutine
// nenhuma. Nesse instante a fila aceita a barreira, ninguém a consome, `parado`
// não está fechado porque Run não retornou, e o contexto do callback não está
// cancelado: o Quiesce espera para sempre DENTRO de um callback do franz-go, o
// group cai atrás dele e o Close do cliente trava.
//
// Medido antes do conserto: 2 travamentos em 12 execuções de
// TestDatasetVazioSemOffsetsSobeNormalmente sob -race.
//
// Devolver nil não é atalho: a garantia do quiesce é que tudo o que entrou
// antes dele foi APLICADO e tornado durável, e sem writer nada foi aplicado.
func TestQuiesceVoltaNaHoraSeOWriterNuncaSubiu(t *testing.T) {
	cfg := &Config{Dataset: "eventos", Topic: "eventos", GroupID: "g",
		Brokers: []string{"127.0.0.1:9092"}, MaxPollRecords: 8, QueueSize: 8, QueueBytes: 1 << 20}
	fila := NewQueue(cfg.QueueSize, cfg.QueueBytes)
	w := NewWriter(cfg, nil, &storeLento{}, fila, nil, registerMetrics(nil, "eventos"),
		map[int32]int64{0: 0}, Cadencia{Eventos: 1 << 30, Intervalo: time.Hour}, nil)

	pronto := make(chan error, 1)
	go func() { pronto <- w.Quiesce(context.Background()) }()
	select {
	case err := <-pronto:
		if err != nil {
			t.Fatalf("Quiesce = %v, quero nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Quiesce ficou esperando um writer que nunca subiu; é assim que o " +
			"callback de revogação trava e o group cai atrás dele")
	}

	// E a barreira NÃO foi enfileirada: uma barreira órfã na fila seria
	// consumida pelo primeiro decodificador que subisse, e o writer faria um
	// checkpoint por uma revogação que já passou.
	if reg, _ := fila.Profundidade(); reg != 0 {
		t.Errorf("a fila ficou com %d item(ns); o Quiesce sem writer não pode enfileirar nada", reg)
	}
}
