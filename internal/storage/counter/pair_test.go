package counter

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// groupIDOf devolve o id do grupo cuja chave é a OLT dada.
func groupIDOf(t testing.TB, c *Counter, olt string) uint32 {
	t.Helper()
	key, err := valuekey.AppendComposite(nil, []types.Value{{Type: types.TypeString, Bytes: []byte(olt)}})
	if err != nil {
		t.Fatal(err)
	}
	id, ok := c.dict.ByKey(key)
	if !ok {
		t.Fatalf("o grupo %q não está no dicionário", olt)
	}
	return id
}

// TestGroupReadsTotalAndRowsAsOneConsistentPair é a garantia do par: um leitor
// concorrente NUNCA vê o total de uma transição com os refs de outra.
//
// A bancada é a do alarme `total * 10 <= rows`: uma linha fixa no grupo, sem a
// condição, e uma segunda linha que o writer põe e tira do grupo COM a
// condição. Os únicos estados que o grupo tem são (0, 1) e (1, 2); qualquer
// outro par lido — (0, 2), (1, 1), e sobretudo total > rows — é rasgo entre
// duas cargas ou duas somas. Os leitores contam quantas vezes viram cada um dos
// dois estados e quantas vezes viram o estado MUDAR entre duas leituras: sem
// as duas contagens altas o teste seria vacuamente concorrente, verde por
// nunca ter lido no meio de nada.
func TestGroupReadsTotalAndRowsAsOneConsistentPair(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	if err := c.Transition(0, payload(t, "A", 1, 1, false), false); err != nil {
		t.Fatal(err)
	}
	in := payload(t, "A", 1, 2, true)
	out := payload(t, "", 1, 2, true) // sem olt: fora de qualquer grupo
	if err := c.Transition(1, in, false); err != nil {
		t.Fatal(err)
	}
	id := groupIDOf(t, c, "A")

	readers := max(2, runtime.GOMAXPROCS(0)-1)
	var stop atomic.Bool
	var torn, seenLow, seenHigh, changes atomic.Int64
	var firstTorn atomic.Value
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var prev int64 = -1
			for !stop.Load() {
				_, _, total, rows, live := c.Group(id)
				if !live {
					firstTorn.CompareAndSwap(nil, "grupo não vivo")
					torn.Add(1)
					continue
				}
				switch {
				case total == 0 && rows == 1:
					seenLow.Add(1)
				case total == 1 && rows == 2:
					seenHigh.Add(1)
				default:
					firstTorn.CompareAndSwap(nil, [2]int64{total, rows})
					torn.Add(1)
					continue
				}
				if prev >= 0 && prev != total {
					changes.Add(1)
				}
				prev = total
			}
		}()
	}
	deadline := time.Now().Add(400 * time.Millisecond)
	writes := 0
	for time.Now().Before(deadline) || writes < 200_000 {
		p := out
		if writes%2 == 1 {
			p = in
		}
		if err := c.Transition(1, p, false); err != nil {
			stop.Store(true)
			wg.Wait()
			t.Fatal(err)
		}
		writes++
	}
	stop.Store(true)
	wg.Wait()

	t.Logf("leitores=%d escritas=%d leituras (0,1)=%d (1,2)=%d mudanças vistas=%d rasgos=%d",
		readers, writes, seenLow.Load(), seenHigh.Load(), changes.Load(), torn.Load())
	if n := torn.Load(); n != 0 {
		t.Fatalf("%d leituras viram um par que o grupo nunca teve (o primeiro: %v); os únicos estados são (0,1) e (1,2)",
			n, firstTorn.Load())
	}
	// O piso das observações: abaixo dele, o teste não leu no meio das
	// escritas e não provou nada sobre rasgo.
	if seenLow.Load() < 1000 || seenHigh.Load() < 1000 || changes.Load() < 1000 {
		t.Fatalf("observação concorrente insuficiente: (0,1)=%d (1,2)=%d mudanças=%d, quero ≥1000 de cada",
			seenLow.Load(), seenHigh.Load(), changes.Load())
	}
}

// TestGroupRowsCeilingRefusesTheRowBeforeAnyMutation: o teto de linhas de UM
// grupo (os refs moram nos 32 bits baixos da palavra) é recusado com erro
// próprio ANTES de mexer em qualquer coisa — refs, total, agregados, overlay.
// O teto é reduzido a três pelo campo de teste; o código é o mesmo do de
// produção.
func TestGroupRowsCeilingRefusesTheRowBeforeAnyMutation(t *testing.T) {
	c := openCounter(t, t.TempDir(), defByOLT("aut", true, 100), false)
	c.groupRowsCeiling = 3
	for r := types.RowID(0); r < 3; r++ {
		if err := c.Transition(r, payload(t, "A", 1, int64(r), true), false); err != nil {
			t.Fatal(err)
		}
	}
	before := c.Stats()
	err := c.Transition(3, payload(t, "A", 1, 3, true), false)
	if !errors.Is(err, ErrGroupRowsCeiling) {
		t.Fatalf("a quarta linha no grupo de teto 3 deu %v, quero ErrGroupRowsCeiling", err)
	}
	if !strings.Contains(err.Error(), "already holds 3 rows, the ceiling of one group") {
		t.Fatalf("a recusa não nomeia o teto do grupo: %v", err)
	}
	if total, refs := totalOf(t, c, "A"); total != 3 || refs != 3 {
		t.Fatalf("A depois da recusa: total %d refs %d, quero 3 e 3 — a recusa mexeu no grupo", total, refs)
	}
	if after := c.Stats(); after.Rows != before.Rows || after.Total != before.Total {
		t.Fatalf("agregados depois da recusa: %d/%d, eram %d/%d", after.Rows, after.Total, before.Rows, before.Total)
	}
	if e := c.entry(3); e != WithoutGroup {
		t.Fatalf("a linha recusada ficou no overlay com a entrada %#x", e)
	}
	// Trocar a condição de uma linha que JÁ está no grupo cheio não é entrada:
	// passa.
	if err := c.Transition(0, payload(t, "A", 1, 0, false), false); err != nil {
		t.Fatalf("trocar o bit no grupo cheio: %v", err)
	}
	// Uma linha sai, e a recusada cabe.
	if err := c.Transition(1, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(3, payload(t, "A", 1, 3, true), false); err != nil {
		t.Fatalf("a linha cabe depois que outra saiu: %v", err)
	}
	if total, refs := totalOf(t, c, "A"); total != 2 || refs != 3 {
		t.Fatalf("A no fim: total %d refs %d, quero 2 e 3", total, refs)
	}
	requireCoherentAggregates(t, c)
}
