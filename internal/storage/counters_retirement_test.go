package storage

import (
	"context"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestAposentadoriaDeGruposNoStore costura o ciclo de vida no Store: o
// relógio do Store é o do contador, o checkpoint cobre a saída, e a passada
// aposenta o grupo vazio depois do prazo — que some da tabela.
func TestAposentadoriaDeGruposNoStore(t *testing.T) {
	c := porOLT("aut", true)
	c.retencao = time.Hour
	s, rel := storeComContadores(t, t.TempDir(), c)
	for i := 0; i < 5; i++ {
		k := chaveDeServico(i)
		mustPut(t, s, k, 1, payloadServico(t, s, k, "X", true, rel.agora()), nil)
	}
	for i := 0; i < 5; i++ {
		k := chaveDeServico(i)
		prev, _, _, _ := s.GetByKey(s.presente(), []byte(k), nil)
		if _, _, err := s.Put([]byte(k), 2, types.FlagTombstone, payloadServico(t, s, k, "", false, rel.agora()), &prev); err != nil {
			t.Fatal(err)
		}
	}
	exigeTotal(t, s, "aut", "X", 0, 0)
	rel.avanca(2 * time.Hour)
	// Sem checkpoint, a saída não está coberta: nada sai.
	if n, err := s.AposentaGruposVazios(); err != nil || n != 0 {
		t.Fatalf("antes do checkpoint: %d, %v", n, err)
	}
	if err := s.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if n, err := s.AposentaGruposVazios(); err != nil || n != 1 {
		t.Fatalf("depois do checkpoint: %d, %v", n, err)
	}
	ct := contadorChamado(t, s, "aut")
	if _, ok := ct.ByText([]byte("X")); ok {
		t.Fatal("o grupo aposentado não pode resolver por texto")
	}
	if est := ct.Stats(); est.Retired != 1 || est.Groups != 0 {
		t.Fatalf("estatísticas = %+v", est)
	}
}

// TestLacoDeAposentadoriaSobeEPara: o laço é uma goroutine própria, parada
// pelo Close como os outros coletores — sem o join, o Close desmaparia o
// vetor sob os pés de uma passada.
func TestLacoDeAposentadoriaSobeEPara(t *testing.T) {
	c := porOLT("aut", true)
	c.retencao = time.Hour
	s, _ := storeComContadores(t, t.TempDir(), c)
	ctx, cancela := context.WithCancel(context.Background())
	defer cancela()
	s.IniciaAposentadoriaDeGrupos(ctx)
	if !s.AposentadoriaAtivaForTest() {
		t.Fatal("o laço tinha de estar ativo depois de Inicia")
	}
	s.ParaAposentadoriaDeGrupos()
	if s.AposentadoriaAtivaForTest() {
		t.Fatal("o laço tinha de parar depois de Para")
	}
	// Idempotente: parar de novo e fechar não travam.
	s.ParaAposentadoriaDeGrupos()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLacoDeAposentadoriaNaoSobeSemContadorComRetencao(t *testing.T) {
	c := porOLT("aut", true)
	c.retencao = 0
	s, _ := storeComContadores(t, t.TempDir(), c)
	s.IniciaAposentadoriaDeGrupos(context.Background())
	if s.AposentadoriaAtivaForTest() {
		t.Fatal("sem contador com retainEmptyGroups não há laço a subir")
	}
}
