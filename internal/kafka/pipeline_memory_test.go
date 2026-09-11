package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
)

// matOfGolden monta um materializer sobre o YAML do golden, com relógio fixo.
func matOfGolden(t testing.TB) *materializer.Materializer {
	t.Helper()
	spec, err := schema.Parse([]byte(goldenYAML), schema.BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	sch, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.New(sch, fakestorage.New(fakestorage.Indexes{}))
	if err != nil {
		t.Fatal(err)
	}
	mat.UsaRelogio(func() time.Time { return time.UnixMilli(1_700_000_000_000) })
	return mat
}

// Um evento grande NÃO prende memória no slot para sempre.
//
// É o flanco que o orçamento de 1 GiB do §109 não cobre hoje: os dois gates da
// fila contam registros enfileirados e bytes de payload CRU, e nenhum vê o
// arena tipado que o decode produziu. Sem o encolhimento, um evento de
// megabytes deixaria aquele slot segurando megabytes pelo resto da vida do
// processo, multiplicado pela janela — o mesmo desfecho que o débito 28 fechou
// no StreamWriter, com o mesmo remédio: soltar o ARRAY, porque comprimento não
// é o que ocupa memória.
func TestEventoGrandeNaoPrendeMemoriaNoSlot(t *testing.T) {
	mat := matOfGolden(t)
	e := newPipeline(4)
	ctx := context.Background()

	// Um documento gordo, e depois eventos minúsculos no MESMO slot.
	gordo := `{"id":"A","sev":1,"doc":{"grande":"` + strings.Repeat("x", 256<<10) + `"}}`
	magro := `{"id":"A","sev":1}`

	const limiar = 64 << 10
	aplica := func(seq int64, ev string) {
		d := e.reserva(seq)
		mat.Decode(d, []byte(ev))
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
		s, ok, _ := e.consome(ctx, nil)
		if !ok {
			t.Fatal("consome não devolveu o slot")
		}
		if r := mat.Aplica(&s.dec); r.Err != nil {
			t.Fatalf("Aplica: %v", r.Err)
		}
		e.advance(limiar)
	}

	aplica(0, gordo)
	depoisOfGordo := e.bytesRetidos()
	if depoisOfGordo > int64(limiar) {
		t.Fatalf("o slot ficou com %d bytes depois do evento grande; o limiar é %d — não encolheu",
			depoisOfGordo, limiar)
	}

	// E os eventos pequenos seguintes não reinflam nada.
	for seq := int64(1); seq <= 8; seq++ {
		aplica(seq, magro)
	}
	if got := e.bytesRetidos(); got > int64(limiar) {
		t.Errorf("a esteira retém %d bytes depois de 8 eventos pequenos, limiar %d", got, limiar)
	}
}

// O limiar por slot é DERIVADO do orçamento de bytes que o operador declarou
// para a fila, dividido pela janela.
//
// É o que impede a esteira de ser um terceiro consumidor de memória sem teto:
// com esta regra, o total retido pelo anel fica na mesma ordem do que a fila já
// podia reter, e a memória em voo da ingestão no máximo dobra em vez de crescer
// sem limite. Um número escolhido a dedo não teria essa propriedade nem
// acompanharia a declaração do operador.
func TestOLimiarPorSlotSaiDoOrcamentoDaFila(t *testing.T) {
	cfg := &Config{MaxPollRecords: 1000, QueueBytes: 64 << 20}
	// janela arredonda 1000 → 1024; 64 MiB / 1024 = 64 KiB.
	if got, quero := slotThreshold(cfg), 64<<10; got != quero {
		t.Errorf("limiarDeSlot = %d, quero %d (%d bytes de fila / janela de %d)",
			got, quero, cfg.QueueBytes, powerOfTwo(cfg.MaxPollRecords))
	}
	// E o piso: uma fila declarada minúscula não pode transformar cada evento
	// numa realocação. Abaixo de uma página, soltar o buffer não devolve nada
	// ao sistema operacional.
	pequena := &Config{MaxPollRecords: 1000, QueueBytes: 1 << 10}
	if got := slotThreshold(pequena); got != paginaOfSO {
		t.Errorf("limiarDeSlot com fila minúscula = %d, quero o piso de %d", got, paginaOfSO)
	}
}

// Abaixo do limiar o slot NÃO encolhe, e o contador não sobe.
//
// É a metade negativa que faltava. Só provar que o evento grande encolhe deixa
// passar a mutação oposta — encolher sempre —, que devolve a memória e a
// realoca no evento seguinte: churn de alocação no caminho quente, com o
// contador de slots encolhidos subindo a cada evento. As duas metades juntas
// são o que diz que o limiar governa alguma coisa.
func TestAbaixoDoLimiarOSlotNaoEncolhe(t *testing.T) {
	mat := matOfGolden(t)
	e := newPipeline(4)
	ctx := context.Background()

	const limiar = 64 << 10
	var encolhimentos int
	for seq := int64(0); seq < 12; seq++ {
		d := e.reserva(seq)
		mat.Decode(d, []byte(`{"id":"A","sev":1,"doc":{"k":"v"}}`))
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
		s, ok, _ := e.consome(ctx, nil)
		if !ok {
			t.Fatal("consome não devolveu o slot")
		}
		if r := mat.Aplica(&s.dec); r.Err != nil {
			t.Fatalf("Aplica: %v", r.Err)
		}
		if e.advance(limiar) {
			encolhimentos++
		}
	}
	if encolhimentos != 0 {
		t.Errorf("%d slots encolhidos com eventos muito abaixo do limiar de %d bytes; "+
			"encolher aqui é devolver memória para realocá-la no evento seguinte", encolhimentos, limiar)
	}
	// E o slot seguiu carregando os buffers, que é o ponto de não encolher.
	if e.bytesRetidos() == 0 {
		t.Error("os slots não retiveram buffer nenhum: não há reuso a preservar")
	}
}

// O contador de slots encolhidos acompanha o que de fato encolheu.
//
// Sem esta asserção, `nishidb_ingest_decode_slots_shrunk_total` podia ficar
// preso em zero para sempre e ninguém notaria — e é justamente o contraste
// entre ele e o gauge de arena que separa "memória presa" de "aparada sem
// parar".
func TestOContadorDeSlotsEncolhidosAcompanhaOEncolhimento(t *testing.T) {
	mat := matOfGolden(t)
	e := newPipeline(4)
	ctx := context.Background()
	gordo := `{"id":"A","sev":1,"doc":{"grande":"` + strings.Repeat("x", 256<<10) + `"}}`

	const limiar = 64 << 10
	var encolhimentos int
	for seq := int64(0); seq < 3; seq++ {
		d := e.reserva(seq)
		mat.Decode(d, []byte(gordo))
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
		s, ok, _ := e.consome(ctx, nil)
		if !ok {
			t.Fatal("consome não devolveu o slot")
		}
		if r := mat.Aplica(&s.dec); r.Err != nil {
			t.Fatalf("Aplica: %v", r.Err)
		}
		if e.advance(limiar) {
			encolhimentos++
		}
	}
	if encolhimentos != 3 {
		t.Errorf("avanca reportou %d encolhimentos em 3 eventos que passam do limiar; quero 3", encolhimentos)
	}
}
