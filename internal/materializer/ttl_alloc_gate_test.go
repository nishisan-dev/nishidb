//go:build !race

package materializer

import (
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// singleRowStore é o dublê que NÃO aloca: guarda uma linha em buffers
// reusados. O fakestorage serve aos testes de comportamento, mas aloca por Put
// (mapa e cópia), e mediria a si mesmo junto do materializer.
//
// absent faz toda leitura responder "não existe", para medir o create — o
// caminho do encodePatch — repetidas vezes sobre a mesma key.
type singleRowStore struct {
	key, payload []byte
	seq          int64
	live, absent bool
}

func (s *singleRowStore) State(_, buf []byte) (types.Record, []byte, types.Presence, error) {
	if !s.live || s.absent {
		return types.Record{}, buf, types.PresenceAbsent, nil
	}
	buf = append(buf[:0], s.payload...)
	return types.Record{RowID: 1, Sequence: s.seq, Key: s.key, Payload: buf}, buf, types.PresenceLive, nil
}

func (s *singleRowStore) Put(key []byte, seq int64, _ uint8, payload []byte, _ *types.Record) (types.RowID, bool, error) {
	created := !s.live || s.absent
	s.key = append(s.key[:0], key...)
	s.payload = append(s.payload[:0], payload...)
	s.seq, s.live = seq, true
	return 1, created, nil
}

// ttlAllocCase é um caminho da decisão do prazo a medir: o último evento é o
// medido, depois de os anteriores montarem o estado.
type ttlAllocCase struct {
	name   string
	yaml   string
	absent bool
	events []string
}

// TestTTLDecisionDoesNotAllocate é o gate da decisão do prazo: do Apply até o
// `_expiresIn` gravado no payload que vai ao Put, nenhuma alocação por evento.
//
// Mede o Apply inteiro sobre um store que não aloca (singleRowStore), e não
// o Parse sozinho como os gates irmãos: a decisão do prazo mora DEPOIS do
// merge, e o caminho dela — a coleta dos slots sobre o payload montado, a
// avaliação e a reescrita dos 8 bytes — só é exercitado com o Aplica rodando.
// Cada caso força um ramo: a condicional que casa, a que não casa e cai na
// regra de fundo, a congelada que o merge segura, a soma, e o create (sem
// estado anterior, pelo encodePatch) contra o update (pelo merge).
func TestTTLDecisionDoesNotAllocate(t *testing.T) {
	runTTLAllocCases(t, []ttlAllocCase{
		{"condicional casa no update", prazoYAML, false, []string{`{"id":"a","tipo":2,"sev":1}`, `{"id":"a","sev":2}`}},
		{"cai na regra de fundo", prazoYAML, false, []string{`{"id":"a","tipo":1,"sev":1}`, `{"id":"a","sev":2}`}},
		{"congelada no record", frozenTTLYAML, false, []string{`{"id":"a","sev":5}`, `{"id":"a","sev":9}`}},
		{"soma no record", sumTTLYAML, false, []string{`{"id":"a","hits":6}`, `{"id":"a","hits":6}`}},
		{"create", prazoYAML, true, []string{`{"id":"a","tipo":2,"sev":1}`}},
	})
}

// ttlDecisions soma as decisões de prazo já tomadas, casadas ou não.
func ttlDecisions(m *Materializer) int64 {
	s := m.Metrics()
	total := s.TTLUnmatched
	for _, n := range s.TTLRuleMatches {
		total += n
	}
	return total
}

func runTTLAllocCases(t *testing.T, cases []ttlAllocCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := mustNew(t, compileYAML(t, tc.yaml), &singleRowStore{absent: tc.absent})
			evs := make([][]byte, len(tc.events))
			for i, e := range tc.events {
				evs[i] = []byte(e)
			}
			// Aquece: as primeiras passagens dimensionam os buffers reusados.
			for range 3 {
				for _, ev := range evs {
					if r := m.Apply(ev); r.Err != nil {
						t.Fatalf("aquecimento: %v", r.Err)
					}
				}
			}
			ev := evs[len(evs)-1]
			before := ttlDecisions(m)
			failed := 0
			n := testing.AllocsPerRun(300, func() {
				if r := m.Apply(ev); r.Err != nil {
					failed++
				}
			})
			// O gate só vale se o caminho medido decidiu o prazo: um Apply que
			// falhasse antes dele mediria zero sem ter passado por ele.
			if failed > 0 || ttlDecisions(m)-before < 300 {
				t.Fatalf("%d Apply falharam e %d prazos decididos: o gate não exercitou a decisão",
					failed, ttlDecisions(m)-before)
			}
			if n != 0 {
				t.Errorf("allocs/evento = %.1f, quer 0", n)
			}
		})
	}
}
