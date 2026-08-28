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

// matDoGolden monta um materializer sobre o YAML do golden, com relógio fixo.
func matDoGolden(t testing.TB) *materializer.Materializer {
	t.Helper()
	spec, err := schema.Parse([]byte(goldenYAML), schema.DefaultsPadrao)
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
	mat := matDoGolden(t)
	e := novaEsteira(4)
	ctx := context.Background()

	// Um documento gordo, e depois eventos minúsculos no MESMO slot.
	gordo := `{"id":"A","sev":1,"doc":{"grande":"` + strings.Repeat("x", 256<<10) + `"}}`
	magro := `{"id":"A","sev":1}`

	const limiar = 64 << 10
	aplica := func(seq int64, ev string) {
		d := e.reserva(seq)
		mat.Decodifica(d, []byte(ev))
		e.publica(seq, &kgo.Record{Offset: seq}, nil)
		s, ok, _ := e.consome(ctx, nil)
		if !ok {
			t.Fatal("consome não devolveu o slot")
		}
		if r := mat.Aplica(&s.dec); r.Err != nil {
			t.Fatalf("Aplica: %v", r.Err)
		}
		e.avanca(limiar)
	}

	aplica(0, gordo)
	depoisDoGordo := e.bytesRetidos()
	if depoisDoGordo > int64(limiar) {
		t.Fatalf("o slot ficou com %d bytes depois do evento grande; o limiar é %d — não encolheu",
			depoisDoGordo, limiar)
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
	if got, quero := limiarDeSlot(cfg), 64<<10; got != quero {
		t.Errorf("limiarDeSlot = %d, quero %d (%d bytes de fila / janela de %d)",
			got, quero, cfg.QueueBytes, potenciaDeDois(cfg.MaxPollRecords))
	}
	// E o piso: uma fila declarada minúscula não pode transformar cada evento
	// numa realocação. Abaixo de uma página, soltar o buffer não devolve nada
	// ao sistema operacional.
	pequena := &Config{MaxPollRecords: 1000, QueueBytes: 1 << 10}
	if got := limiarDeSlot(pequena); got != paginaDoSO {
		t.Errorf("limiarDeSlot com fila minúscula = %d, quero o piso de %d", got, paginaDoSO)
	}
}
