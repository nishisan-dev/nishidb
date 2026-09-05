package contador

import (
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func defComRetencao(nome string, retencao time.Duration) Definicao {
	d := defPorOLT(nome, true, 100)
	d.RetainEmptyGroups = retencao
	return d
}

type relogio struct{ t time.Time }

func (r *relogio) agora() time.Time       { return r.t }
func (r *relogio) avanca(d time.Duration) { r.t = r.t.Add(d) }

func abreComRelogio(t *testing.T, dir string, def Definicao) (*Contador, *relogio) {
	t.Helper()
	c := abreContador(t, dir, def, false)
	rel := &relogio{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	c.UsaRelogio(rel.agora)
	return c, rel
}

// cobre simula o checkpoint: toda transição até aqui está durável.
func cobre(c *Contador) { c.CobreMexidas(c.Transicoes()) }

// TestGrupoZeradoComRefsNaoEhAposentado é o cenário da proposta §6.5 que a
// regra intuitiva erra: 500 serviços numa OLT, nenhum autenticado. total == 0
// não diz "não há ninguém aqui" — diz "ninguém aqui satisfaz a condição".
func TestGrupoZeradoComRefsNaoEhAposentado(t *testing.T) {
	c, rel := abreComRelogio(t, t.TempDir(), defComRetencao("aut", 7*24*time.Hour))
	for r := types.RowID(0); r < 500; r++ {
		if err := c.Transiciona(r, payload(t, "X", 1, 1, false), false); err != nil {
			t.Fatal(err)
		}
	}
	cobre(c)
	if total, refs := totalDe(t, c, "X"); total != 0 || refs != 500 {
		t.Fatalf("X: total %d refs %d", total, refs)
	}
	rel.avanca(30 * 24 * time.Hour)
	n, err := c.AposentaVazios(rel.agora())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("aposentou %d grupos com refs > 0", n)
	}
	if _, _, _, _, vivo := c.Grupo(1); !vivo {
		t.Fatal("o grupo com 500 linhas apontando para ele tem de continuar vivo")
	}
}

func TestGrupoSemRefsEhAposentadoDepoisDoPrazo(t *testing.T) {
	c, rel := abreComRelogio(t, t.TempDir(), defComRetencao("aut", 7*24*time.Hour))
	for r := types.RowID(0); r < 10; r++ {
		if err := c.Transiciona(r, payload(t, "X", 1, 1, true), false); err != nil {
			t.Fatal(err)
		}
	}
	for r := types.RowID(0); r < 10; r++ {
		if err := c.Transiciona(r, nil, true); err != nil {
			t.Fatal(err)
		}
	}
	cobre(c)
	if total, refs := totalDe(t, c, "X"); total != 0 || refs != 0 {
		t.Fatalf("X: %d %d", total, refs)
	}
	// Antes do prazo: continua visível, com total 0.
	rel.avanca(6 * 24 * time.Hour)
	if n, err := c.AposentaVazios(rel.agora()); err != nil || n != 0 {
		t.Fatalf("antes do prazo: %d, %v", n, err)
	}
	if _, _, _, _, vivo := c.Grupo(1); !vivo {
		t.Fatal("dentro do prazo o grupo vazio continua visível")
	}
	// Depois do prazo: sai.
	rel.avanca(2 * 24 * time.Hour)
	if n, err := c.AposentaVazios(rel.agora()); err != nil || n != 1 {
		t.Fatalf("depois do prazo: %d, %v", n, err)
	}
	if _, _, _, _, vivo := c.Grupo(1); vivo {
		t.Fatal("grupo aposentado não é vivo")
	}
	if _, ok := c.PorTexto([]byte("X")); ok {
		t.Fatal("grupo aposentado não resolve por texto")
	}
	if est := c.Estatisticas(); est.Aposentados != 1 || est.Grupos != 0 {
		t.Fatalf("estatísticas = %+v", est)
	}
	// A OLT reaparece: id NOVO, nunca o 1.
	if err := c.Transiciona(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if id, ok := c.PorTexto([]byte("X")); !ok || id != 2 {
		t.Fatalf("a OLT que reapareceu ganhou id %d, %v; quer 2", id, ok)
	}
	if total, refs := totalDe(t, c, "X"); total != 1 || refs != 1 {
		t.Fatalf("X de volta: %d %d", total, refs)
	}
}

// TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida: a transição que
// esvaziou o grupo pode estar numa cauda rasgada. Sem a cobertura de um
// checkpoint, o estado durável ainda pode dizer "a linha está em X" — e X
// aposentado seria um grupo referenciado que saiu.
func TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida(t *testing.T) {
	c, rel := abreComRelogio(t, t.TempDir(), defComRetencao("aut", time.Hour))
	if err := c.Transiciona(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	cobre(c)
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	rel.avanca(2 * time.Hour)
	if n, err := c.AposentaVazios(rel.agora()); err != nil || n != 0 {
		t.Fatalf("sem checkpoint cobrindo a saída: aposentou %d, %v", n, err)
	}
	cobre(c)
	if n, err := c.AposentaVazios(rel.agora()); err != nil || n != 1 {
		t.Fatalf("com a saída coberta: aposentou %d, %v", n, err)
	}
}

func TestRetainNeverNuncaAposenta(t *testing.T) {
	c, rel := abreComRelogio(t, t.TempDir(), defComRetencao("aut", 0))
	if err := c.Transiciona(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	cobre(c)
	rel.avanca(365 * 24 * time.Hour)
	if n, err := c.AposentaVazios(rel.agora()); err != nil || n != 0 {
		t.Fatalf("never aposentou %d, %v", n, err)
	}
}

// TestReinicioEstendeAVisibilidade: o instante em que o grupo esvaziou não é
// persistido; a reabertura o marca como "vazio desde agora", e o pior caso é
// o grupo viver mais um prazo inteiro — nunca menos.
func TestReinicioEstendeAVisibilidade(t *testing.T) {
	dir := t.TempDir()
	def := defComRetencao("aut", 7*24*time.Hour)
	c, rel := abreComRelogio(t, dir, def)
	if err := c.Transiciona(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Msync(); err != nil {
		t.Fatal(err)
	}
	rel.avanca(6 * 24 * time.Hour)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := abreContador(t, dir, def, false)
	c2.UsaRelogio(rel.agora)
	if err := c2.Dobra(); err != nil {
		t.Fatal(err)
	}
	// Seis dias já tinham passado, mas a reabertura recomeça a contar.
	rel.avanca(2 * 24 * time.Hour)
	if n, err := c2.AposentaVazios(rel.agora()); err != nil || n != 0 {
		t.Fatalf("a reabertura tinha de estender a visibilidade: aposentou %d, %v", n, err)
	}
	rel.avanca(6 * 24 * time.Hour)
	if n, err := c2.AposentaVazios(rel.agora()); err != nil || n != 1 {
		t.Fatalf("um prazo inteiro depois da reabertura: aposentou %d, %v", n, err)
	}
}

func TestAposentadoriaSobreviveAReabertura(t *testing.T) {
	dir := t.TempDir()
	def := defComRetencao("aut", time.Hour)
	c, rel := abreComRelogio(t, dir, def)
	if err := c.Transiciona(0, payload(t, "X", 1, 1, true), false); err != nil {
		t.Fatal(err)
	}
	if err := c.Transiciona(0, nil, true); err != nil {
		t.Fatal(err)
	}
	cobre(c)
	rel.avanca(2 * time.Hour)
	if n, _ := c.AposentaVazios(rel.agora()); n != 1 {
		t.Fatalf("aposentou %d", n)
	}
	if err := c.ApplyOverlay(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2 := abreContador(t, dir, def, false)
	if err := c2.Dobra(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, vivo := c2.Grupo(1); vivo {
		t.Fatal("a aposentadoria é durável")
	}
	if c2.Estatisticas().Aposentados != 1 {
		t.Fatal("o aposentado tem de ser contado depois da reabertura")
	}
}
