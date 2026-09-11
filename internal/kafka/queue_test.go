package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func rec(value string) *kgo.Record {
	return &kgo.Record{Value: []byte(value)}
}

// §128: NO unbounded queues. O gate de registros é a capacidade do channel.
func TestFilaBloqueiaNoGateDeRegistros(t *testing.T) {
	f := NewQueue(2, 1<<30)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := f.Envia(ctx, rec("x")); err != nil {
			t.Fatal(err)
		}
	}

	ctxCurto, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := f.Envia(ctxCurto, rec("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("erro = %v, quer DeadlineExceeded: a fila aceitou além da capacidade", err)
	}
}

// O gate que a spec NÃO tem e o §79/§108 exigem: contar registros não é
// unidade de memória.
func TestFilaBloqueiaNoGateDeBytes(t *testing.T) {
	f := NewQueue(1000, 100)
	ctx := context.Background()

	// 100 bytes cabem exatamente.
	if err := f.Envia(ctx, rec(string(make([]byte, 100)))); err != nil {
		t.Fatal(err)
	}
	_, bytes := f.Profundidade()
	if bytes != 100 {
		t.Fatalf("bytes = %d, quer 100", bytes)
	}

	ctxCurto, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := f.Envia(ctxCurto, rec("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("erro = %v, quer DeadlineExceeded: a fila estourou o teto de bytes", err)
	}
}

// Sem o escape da fila vazia, um único record acima do teto nunca caberia e a
// ingestão travaria para sempre — num deadlock que nenhuma métrica explicaria.
func TestRecordMaiorQueOTetoPassaSozinho(t *testing.T) {
	f := NewQueue(10, 100)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	grande := rec(string(make([]byte, 5000)))
	if err := f.Envia(ctx, grande); err != nil {
		t.Fatalf("record acima do teto travou a fila: %v", err)
	}
	it, ok := f.Recebe(ctx)
	if !ok || len(it.Rec.Value) != 5000 {
		t.Fatalf("Recebe = (%v, %v)", it, ok)
	}
}

// Consumir tem de devolver o orçamento; senão a fila só estreita e o consumo
// para depois de N eventos, sem erro nenhum.
func TestReceberLiberaOOrcamentoDeBytes(t *testing.T) {
	f := NewQueue(10, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for i := 0; i < 50; i++ {
		if err := f.Envia(ctx, rec(string(make([]byte, 40)))); err != nil {
			t.Fatalf("envio %d travou: %v", i, err)
		}
		if _, ok := f.Recebe(ctx); !ok {
			t.Fatalf("recebimento %d falhou", i)
		}
	}
	if _, bytes := f.Profundidade(); bytes != 0 {
		t.Errorf("bytes retidos após drenar = %d, quer 0", bytes)
	}
}

// O produtor bloqueado tem de ser acordado pelo consumidor, e não por
// timeout: um poller que só desbloqueia por polling ativo queima CPU e atrasa
// o intake exatamente quando o sistema está saturado.
func TestConsumidorAcordaOProdutorBloqueado(t *testing.T) {
	f := NewQueue(1, 50)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := f.Envia(ctx, rec(string(make([]byte, 50)))); err != nil {
		t.Fatal(err)
	}

	enviou := make(chan error, 1)
	go func() { enviou <- f.Envia(ctx, rec(string(make([]byte, 50)))) }()

	select {
	case err := <-enviou:
		t.Fatalf("o segundo envio não bloqueou (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	if _, ok := f.Recebe(ctx); !ok {
		t.Fatal("Recebe falhou")
	}
	select {
	case err := <-enviou:
		if err != nil {
			t.Errorf("envio desbloqueado com erro: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("o produtor não foi acordado pelo consumidor")
	}
}

// O marcador de quiesce não pode consumir orçamento de bytes: ele existe para
// sinalizar, e cobrá-lo do teto tornaria a contabilidade de memória
// dependente de quantas revogações aconteceram.
func TestBarreiraNaoConsomeOrcamento(t *testing.T) {
	f := NewQueue(10, 100)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b := newBarrier()
	if err := f.EnviaBarreira(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, bytes := f.Profundidade(); bytes != 0 {
		t.Errorf("a barreira consumiu %d bytes do orçamento", bytes)
	}
	it, ok := f.Recebe(ctx)
	if !ok || it.Barreira == nil {
		t.Fatalf("Recebe = (%+v, %v), quer o marcador de quiesce", it, ok)
	}
}

func TestFilaRespeitaCancelamento(t *testing.T) {
	f := NewQueue(1, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, ok := f.Recebe(ctx); ok {
		t.Error("Recebe devolveu item com contexto cancelado")
	}
	if err := f.Envia(ctx, rec("x")); !errors.Is(err, context.Canceled) {
		t.Errorf("Envia = %v, quer Canceled", err)
	}
}
