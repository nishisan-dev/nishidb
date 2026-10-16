package counter

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func defWithRetention(name string, retention time.Duration) Definition {
	d := defByOLT(name, true, 100)
	d.RetainEmptyGroups = retention
	return d
}

type clock struct{ t time.Time }

func (r *clock) now() time.Time          { return r.t }
func (r *clock) advance(d time.Duration) { r.t = r.t.Add(d) }

func openWithClock(t *testing.T, dir string, def Definition) (*Counter, *clock) {
	t.Helper()
	c := openCounter(t, dir, def, false)
	clk := &clock{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	c.UseClock(clk.now)
	return c, clk
}

// cover simula o checkpoint: toda transição até aqui está durável.
func cover(c *Counter) { c.RecordCoveredTouches(c.Transitions()) }

// TestGrupoZeradoComRefsNaoEhAposentado é o cenário da proposta §6.5 que a
// regra intuitiva erra: 500 serviços numa OLT, nenhum autenticado. total == 0
// não diz "não há ninguém aqui" — diz "ninguém aqui satisfaz a condição".
func TestGrupoZeradoComRefsNaoEhAposentado(t *testing.T) {
	c, clk := openWithClock(t, t.TempDir(), defWithRetention("aut", 7*24*time.Hour))
	for r := types.RowID(0); r < 500; r++ {
		if err := c.Transition(r, payload(t, "X", 1, 1, false), false); err != nil {
			t.Fatal(err)
		}
	}
	cover(c)
	if total, refs := totalOf(t, c, "X"); total != 0 || refs != 500 {
		t.Fatalf("X: total %d refs %d", total, refs)
	}
	clk.advance(30 * 24 * time.Hour)
	n, err := c.RetireEmptyGroups(clk.now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("aposentou %d grupos com refs > 0", n)
	}
	if _, _, _, _, live := c.Group(1); !live {
		t.Fatal("o grupo com 500 linhas apontando para ele tem de continuar vivo")
	}
}

func TestGrupoSemRefsEhAposentadoDepoisDoPrazo(t *testing.T) {
	c, clk := openWithClock(t, t.TempDir(), defWithRetention("aut", 7*24*time.Hour))
	for r := types.RowID(0); r < 10; r++ {
		if err := c.Transition(r, payload(t, "X", 1, 1, true), false); err != nil {
			t.Fatal(err)
		}
	}
	for r := types.RowID(0); r < 10; r++ {
		if err := c.Transition(r, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	cover(c)
	if total, refs := totalOf(t, c, "X"); total != 0 || refs != 0 {
		t.Fatalf("X: %d %d", total, refs)
	}
	// Antes do prazo: continua visível, com total 0.
	clk.advance(6 * 24 * time.Hour)
	if n, err := c.RetireEmptyGroups(clk.now(), nil); err != nil || n != 0 {
		t.Fatalf("antes do prazo: %d, %v", n, err)
	}
	if _, _, _, _, live := c.Group(1); !live {
		t.Fatal("dentro do prazo o grupo vazio continua visível")
	}
	// Depois do prazo: sai.
	clk.advance(2 * 24 * time.Hour)
	if n, err := c.RetireEmptyGroups(clk.now(), nil); err != nil || n != 1 {
		t.Fatalf("depois do prazo: %d, %v", n, err)
	}
	if _, _, _, _, live := c.Group(1); live {
		t.Fatal("grupo aposentado não é vivo")
	}
	if _, ok := c.ByText([]byte("X")); ok {
		t.Fatal("grupo aposentado não resolve por texto")
	}
	if stats := c.Stats(); stats.Retired != 1 || stats.Groups != 0 {
		t.Fatalf("estatísticas = %+v", stats)
	}
	// A OLT reaparece: id NOVO, nunca o 1.
	if err := c.Transition(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if id, ok := c.ByText([]byte("X")); !ok || id != 2 {
		t.Fatalf("a OLT que reapareceu ganhou id %d, %v; quer 2", id, ok)
	}
	if total, refs := totalOf(t, c, "X"); total != 1 || refs != 1 {
		t.Fatalf("X de volta: %d %d", total, refs)
	}
}

// TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida: a transição que
// esvaziou o grupo pode estar numa cauda rasgada. Sem a cobertura de um
// checkpoint, o estado durável ainda pode dizer "a linha está em X" — e X
// aposentado seria um grupo referenciado que saiu.
func TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida(t *testing.T) {
	c, clk := openWithClock(t, t.TempDir(), defWithRetention("aut", time.Hour))
	if err := c.Transition(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	cover(c)
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	clk.advance(2 * time.Hour)
	if n, err := c.RetireEmptyGroups(clk.now(), nil); err != nil || n != 0 {
		t.Fatalf("sem checkpoint cobrindo a saída: aposentou %d, %v", n, err)
	}
	cover(c)
	if n, err := c.RetireEmptyGroups(clk.now(), nil); err != nil || n != 1 {
		t.Fatalf("com a saída coberta: aposentou %d, %v", n, err)
	}
}

func TestRetainNeverNuncaAposenta(t *testing.T) {
	c, clk := openWithClock(t, t.TempDir(), defWithRetention("aut", 0))
	if err := c.Transition(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	cover(c)
	clk.advance(365 * 24 * time.Hour)
	if n, err := c.RetireEmptyGroups(clk.now(), nil); err != nil || n != 0 {
		t.Fatalf("never aposentou %d, %v", n, err)
	}
}

// TestReinicioEstendeAVisibilidade: o instante em que o grupo esvaziou não é
// persistido; a reabertura o marca como "vazio desde agora", e o pior caso é
// o grupo viver mais um prazo inteiro — nunca menos.
func TestReinicioEstendeAVisibilidade(t *testing.T) {
	dir := t.TempDir()
	def := defWithRetention("aut", 7*24*time.Hour)
	c, clk := openWithClock(t, dir, def)
	if err := c.Transition(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	clk.advance(6 * 24 * time.Hour)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := openCounter(t, dir, def, false)
	c2.UseClock(clk.now)
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	// Seis dias já tinham passado, mas a reabertura recomeça a contar.
	clk.advance(2 * 24 * time.Hour)
	if n, err := c2.RetireEmptyGroups(clk.now(), nil); err != nil || n != 0 {
		t.Fatalf("a reabertura tinha de estender a visibilidade: aposentou %d, %v", n, err)
	}
	clk.advance(6 * 24 * time.Hour)
	if n, err := c2.RetireEmptyGroups(clk.now(), nil); err != nil || n != 1 {
		t.Fatalf("um prazo inteiro depois da reabertura: aposentou %d, %v", n, err)
	}
}

func TestAposentadoriaSobreviveAReabertura(t *testing.T) {
	dir := t.TempDir()
	def := defWithRetention("aut", time.Hour)
	c, clk := openWithClock(t, dir, def)
	if err := c.Transition(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transition(0, nil, true); err != nil {
		t.Fatal(err)
	}
	cover(c)
	clk.advance(2 * time.Hour)
	if n, _ := c.RetireEmptyGroups(clk.now(), nil); n != 1 {
		t.Fatalf("aposentou %d", n)
	}
	if err := c.ApplyOverlay(nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := openCounter(t, dir, def, false)
	if err := c2.Fold(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, live := c2.Group(1); live {
		t.Fatal("a aposentadoria é durável")
	}
	if c2.Stats().Retired != 1 {
		t.Fatal("o aposentado tem de ser contado depois da reabertura")
	}
}

// TestAposentadoriaNaoMexeNosAgregados trava o raciocínio que faz a
// aposentadoria não precisar tocar nos agregados: ela só aposenta com
// refs == 0, e totals[g] <= refs[g] por construção, então o grupo que sai
// contribuía ZERO para os dois.
//
// Existe porque o "conserto" intuitivo — descontar refs[g] e totals[g] do
// agregado ao aposentar — é plausível de escrever e derruba o agregado abaixo
// da soma dos grupos, em silêncio, meses depois da linha ter saído.
func TestAposentadoriaNaoMexeNosAgregados(t *testing.T) {
	c, clk := openWithClock(t, t.TempDir(), defWithRetention("aut", 7*24*time.Hour))
	// Um grupo que vai esvaziar, e outro que fica.
	for r := types.RowID(0); r < 3; r++ {
		if err := c.Transition(r, payload(t, "X", 1, 1, true), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Transition(9, payload(t, "Y", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	for r := types.RowID(0); r < 3; r++ {
		if err := c.Transition(r, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	cover(c)
	before := c.Stats()
	if before.Rows != 1 || before.Total != 1 {
		t.Fatalf("com X vazio e Y com uma linha: linhas %d total %d, quer 1 e 1", before.Rows, before.Total)
	}
	clk.advance(8 * 24 * time.Hour)
	if n, err := c.RetireEmptyGroups(clk.now(), nil); err != nil || n != 1 {
		t.Fatalf("aposentadoria: %d, %v", n, err)
	}
	after := c.Stats()
	if after.Rows != before.Rows || after.Total != before.Total {
		t.Fatalf("a aposentadoria mexeu nos agregados: linhas %d total %d, quer %d e %d",
			after.Rows, after.Total, before.Rows, before.Total)
	}
	requireCoherentAggregates(t, c)
}
