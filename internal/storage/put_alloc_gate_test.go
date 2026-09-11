//go:build !race

package storage

import (
	"testing"
	"time"
)

// ceilingOfAllocsOfPut é o número de alocações por Put de UPDATE (key existente,
// mesmo payload) medido em 04/09/2026 no dataset de teste com expiração ligada
// e SEM contadores: 1,0. O gate existe porque `internal/storage` não tinha um
// único teto sobre o Put real — TestGateAllocsIngestFake mede o dublê, e
// BenchmarkApplyIngestReal é benchmark sem teto —, e o contador entra
// exatamente no caminho que ninguém media. O número não é zero (é uma
// alocação por update, anterior a este ciclo, no caminho do Put real), e o
// que o gate trava são duas coisas: ele não sobe, e os contadores NÃO o sobem
// — medido com dois contadores, continua 1,0.
const ceilingOfAllocsOfPut = 1

// TestPutNaoAlocaMaisComContadores mede o Put real de um update nos dois
// regimes e exige que os contadores não acrescentem alocação nenhuma: grupos
// já internados e linha já presente no overlay, que é o regime de uma
// ingestão em curso.
func TestPutNaoAlocaMaisComContadores(t *testing.T) {
	mede := func(t *testing.T, s *Store, rel *relogioOfTest) float64 {
		t.Helper()
		const key = "cliente-gate"
		p := payloadServico(t, s, key, "A", true, rel.agora())
		mustPut(t, s, key, 1, p, nil)
		prev, _, _, err := s.GetByKey(s.presente(), []byte(key), nil)
		if err != nil {
			t.Fatal(err)
		}
		// Aquecimento fora da medição: o primeiro update cria a versão no
		// journal e a entrada no overlay.
		mustPut(t, s, key, 2, p, &prev)
		seq := int64(3)
		return testing.AllocsPerRun(500, func() {
			if _, _, err := s.Put([]byte(key), seq, 0, p, &prev); err != nil {
				t.Fatal(err)
			}
			seq++
		})
	}

	withoutOpts := optionsWithCounters(t.TempDir())
	without := openStore(t, withoutOpts)
	relWithout := &relogioOfTest{t: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)}
	without.UsaRelogio(relWithout.agora)
	allocsWithout := mede(t, without, relWithout)

	with, relWith := storeWithCounters(t, t.TempDir(), byOLT("aut", true), byOLT("serv", false))
	allocsWith := mede(t, with, relWith)

	t.Logf("allocs/Put: sem contadores %.1f, com dois contadores %.1f", allocsWithout, allocsWith)
	if allocsWithout > ceilingOfAllocsOfPut {
		t.Errorf("Put sem contadores aloca %.1f por update, teto %d", allocsWithout, ceilingOfAllocsOfPut)
	}
	if allocsWith > allocsWithout {
		t.Errorf("os contadores subiram as alocações do Put de %.1f para %.1f", allocsWithout, allocsWith)
	}
}
