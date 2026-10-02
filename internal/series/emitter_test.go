package series

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/types"
)

var t0 = time.Date(2026, 9, 12, 10, 30, 0, 0, time.UTC)

type group struct {
	olt   string
	slot  int64
	total int64
	live  bool
	// rows são os membros do grupo; zero repete o total, que é o caso de quem
	// não declara `where`. Fica por último porque há literais posicionais.
	rows int64
}

// fakeCounter é o leitor mínimo que a emissão precisa: os grupos com seus
// totais. Ao contrário do alarme, a emissão não avalia condição nenhuma e por
// isso não precisa da tabela virtual — só da chave e do total.
type fakeCounter struct {
	groups []group
	keys   [][]byte
	texts  [][]byte
}

func newFakeCounter(t *testing.T, gs ...group) *fakeCounter {
	t.Helper()
	f := &fakeCounter{groups: gs}
	for _, g := range gs {
		key, err := valuekey.AppendComposite(nil, []types.Value{
			{Type: types.TypeString, Bytes: []byte(g.olt)},
			{Type: types.TypeInt64, I64: g.slot},
		})
		if err != nil {
			t.Fatal(err)
		}
		f.keys = append(f.keys, key)
		f.texts = append(f.texts, []byte(g.olt+"|"+strconv.FormatInt(g.slot, 10)))
	}
	return f
}

func (f *fakeCounter) Limit() uint32 { return uint32(len(f.groups)) + 1 }
func (f *fakeCounter) Group(id uint32) ([]byte, []byte, int64, int64, bool) {
	i := int(id) - 1
	if i < 0 || i >= len(f.groups) || !f.groups[i].live {
		return nil, nil, 0, 0, false
	}
	g := f.groups[i]
	rows := g.rows
	if rows == 0 {
		rows = g.total
	}
	return f.keys[i], f.texts[i], g.total, rows, true
}
func (f *fakeCounter) ByText([]byte) (uint32, bool) { return 0, false }

func newTestEmitter(t *testing.T, f *fakeCounter) (*Emitter, string, *clockOfTest) {
	t.Helper()
	dir := t.TempDir()
	clock := &clockOfTest{now: t0}
	e, err := NewEmitter(Options{
		Dataset:  "servicos",
		Counter:  "autenticados_por_pon",
		Columns:  []string{"olt", "slot"},
		Reader:   f,
		Dir:      dir,
		File:     "autenticados.ndjson",
		Every:    30 * time.Second,
		MaxBytes: 1 << 20,
		Keep:     3,
		Now:      clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, dir, clock
}

type clockOfTest struct{ now time.Time }

func (c *clockOfTest) Now() time.Time          { return c.now }
func (c *clockOfTest) advance(d time.Duration) { c.now = c.now.Add(d) }

// readRows lê o arquivo da série e devolve uma linha decodificada por registro.
func readRows(t *testing.T, dir, nome string) []map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, nome))
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, l := range splitLines(string(b)) {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("linha inválida %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	inicio := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			if i > inicio {
				out = append(out, s[inicio:i])
			}
			inicio = i + 1
		}
	}
	return out
}

// TestEmitterWritesOneLinePerLiveGroup é o pedido: o valor de cada grupo, um por um,
// e não o agregado.
func TestEmitterWritesOneLinePerLiveGroup(t *testing.T) {
	f := newFakeCounter(t,
		group{olt: "OLT_A", slot: 1, total: 120, live: true},
		group{olt: "OLT_A", slot: 2, total: 80, live: true},
		group{olt: "OLT_B", slot: 1, total: 300, live: true},
	)
	e, dir, _ := newTestEmitter(t, f)
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	got := readRows(t, dir, "autenticados.ndjson")
	if len(got) != 3 {
		t.Fatalf("%d linhas, quero 3", len(got))
	}
	if got[0]["counter"] != "autenticados_por_pon" || got[0]["groupKey"] != "OLT_A|1" {
		t.Errorf("primeira linha = %+v", got[0])
	}
	if got[0]["at"] != "2026-09-12T10:30:00.000Z" {
		t.Errorf("at = %v, quero o at recebido pela passada", got[0]["at"])
	}
}

// TestGroupWithZeroTotalIsEmitted é o caso que motiva a feature: a OLT que
// apagou por inteiro. Se ela desaparecesse da série, o gráfico mostraria um
// buraco em vez de um zero — e é justamente essa a linha que interessa.
func TestGroupWithZeroTotalIsEmitted(t *testing.T) {
	f := newFakeCounter(t,
		group{olt: "OLT_A", slot: 1, total: 0, live: true},
		group{olt: "OLT_B", slot: 1, total: 300, live: true},
	)
	e, dir, _ := newTestEmitter(t, f)
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	got := readRows(t, dir, "autenticados.ndjson")
	if len(got) != 2 {
		t.Fatalf("%d linhas, quero 2 — a de total zero também sai", len(got))
	}
	if got[0]["total"] != float64(0) {
		t.Errorf("total = %v, quero 0", got[0]["total"])
	}
}

// TestRetiredGroupIsNotEmitted: ele saiu do inventário, e continuar a
// publicá-lo faria o gráfico mostrar um equipamento que não existe.
func TestRetiredGroupIsNotEmitted(t *testing.T) {
	f := newFakeCounter(t,
		group{olt: "OLT_A", slot: 1, total: 10, live: true},
		group{olt: "OLT_MORTA", slot: 9, total: 0, live: false},
	)
	e, dir, _ := newTestEmitter(t, f)
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	got := readRows(t, dir, "autenticados.ndjson")
	if len(got) != 1 || got[0]["groupKey"] != "OLT_A|1" {
		t.Fatalf("linhas = %+v", got)
	}
}

// TestGroupByColumnsAreTyped: a série vai para uma ferramenta de gráfico,
// e um slot que chega como "8" em vez de 8 obriga quem consome a converter —
// ou pior, a ordenar lexicograficamente. Aqui os valores usam o serializador
// canônico das respostas de consulta, então o tipo é o do schema.
func TestGroupByColumnsAreTyped(t *testing.T) {
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 8, total: 42, live: true})
	e, dir, _ := newTestEmitter(t, f)
	if err := e.EmitOnce(context.Background(), t0); err != nil {
		t.Fatal(err)
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	l := readRows(t, dir, "autenticados.ndjson")[0]
	if l["olt"] != "OLT_A" {
		t.Errorf("olt = %#v, quero a string", l["olt"])
	}
	if l["slot"] != float64(8) {
		t.Errorf("slot = %#v, quero o número 8", l["slot"])
	}
	if l["total"] != float64(42) {
		t.Errorf("total = %#v, quero o número 42", l["total"])
	}
}

// TestSuccessivePassesAppendToTheFile: é uma série temporal, e cada
// passada é um ponto no tempo.
func TestSuccessivePassesAppendToTheFile(t *testing.T) {
	f := newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 10, live: true})
	e, dir, _ := newTestEmitter(t, f)
	for i := 0; i < 3; i++ {
		if err := e.EmitOnce(context.Background(), t0.Add(time.Duration(i)*30*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}
	got := readRows(t, dir, "autenticados.ndjson")
	if len(got) != 3 {
		t.Fatalf("%d linhas, quero uma por passada", len(got))
	}
	instants := map[string]bool{}
	for _, l := range got {
		instants[l["at"].(string)] = true
	}
	if len(instants) != 3 {
		t.Errorf("as três passadas saíram com %d instantes distintos", len(instants))
	}
}

// TestLacoSobeEParaComOMedidorCoerente: molde do StartRetention — o medidor
// sobe FORA da goroutine, porque quem chamou Start tem direito de ver o estado
// que acabou de instalar; e o Stop devolve o controle já com ele em zero, e não
// com uma janela em que a passada ainda escreve no arquivo.
func TestLacoSobeEParaComOMedidorCoerente(t *testing.T) {
	e, _, _ := newTestEmitter(t, newFakeCounter(t, group{olt: "OLT_A", slot: 1, total: 10, live: true}))
	if e.Active() {
		t.Fatal("o medidor sobe antes do laço")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	if !e.Active() {
		t.Fatal("quem chamou Start tem direito de ver o estado que acabou de instalar")
	}
	e.Start(ctx) // idempotente: não sobe uma segunda goroutine
	e.Stop()
	if e.Active() {
		t.Fatal("o Stop devolveu o controle com o medidor ainda em 1")
	}
	e.Stop() // idempotente, para que o Close o chame sem perguntar
}
