package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/query/executor"
	"github.com/nishisan-dev/nishidb/internal/replication"
	"github.com/nishisan-dev/nishidb/internal/series"
)

// SetPublisherCloseBudgetForTest encurta o orçamento da drenagem dos
// publicadores da replicação no encerramento, para o teste do estouro não
// esperar os 10 s de produção. Chamado antes do Run.
func (s *Server) SetPublisherCloseBudgetForTest(d time.Duration) { s.publisherBudget = d }

// StopSamplingAndAlarmsForTest para o laço do amostrador das métricas de janela
// e o laço de alarmes do dataset, para que o teste conduza cada passada com
// SampleCounterMetricsOnceForTest e EvaluateAlarmsOnceForTest.
//
// Existe porque as cadências reais derivam do YAML (a menor janela ÷ 60 e o
// menor sustainedFor ÷ 4): a prova de uma janela de 24 h com prazo de 5 min
// levaria minutos de relógio de parede, e SampleOnce/EvaluateOnce não são
// thread-safe — chamá-los com o laço de pé seria corrida, não teste. Os dois
// Stop aguardam a passada em voo, então nada disputa o estado depois daqui. O
// encerra() continua parando e fechando os dois (Stop e Close são idempotentes).
func (s *Server) StopSamplingAndAlarmsForTest(dataset string) {
	d := s.byNome[dataset]
	if d == nil {
		return
	}
	if d.metrics != nil {
		d.metrics.Stop()
	}
	if d.alarms != nil {
		d.alarms.Stop()
	}
}

// SampleCounterMetricsOnceForTest faz uma passada do amostrador do dataset.
// Só depois de StopSamplingAndAlarmsForTest.
func (s *Server) SampleCounterMetricsOnceForTest(dataset string) error {
	d := s.byNome[dataset]
	if d == nil || d.metrics == nil {
		return fmt.Errorf("dataset %q has no window metric sampler", dataset)
	}
	return d.metrics.SampleOnce()
}

// EvaluateAlarmsOnceForTest faz uma varredura de alarmes do dataset. Só depois
// de StopSamplingAndAlarmsForTest.
func (s *Server) EvaluateAlarmsOnceForTest(dataset string) error {
	d := s.byNome[dataset]
	if d == nil || d.alarms == nil {
		return fmt.Errorf("dataset %q has no alarm runner", dataset)
	}
	return d.alarms.EvaluateOnce()
}

// MetricsExpositionForTest devolve o texto do /metrics lido direto do
// Registry, sem HTTP. É o único jeito de observar as séries ANTES do Run: o
// HTTP só sobe no Run, e os laços sobem logo em seguida, então um scrape pela
// porta nunca garante ter chegado antes da primeira passada.
func (s *Server) MetricsExpositionForTest() string {
	var b bytes.Buffer
	if _, err := s.reg.WriteTo(&b); err != nil {
		return fmt.Sprintf("# metrics exposition failed: %v\n", err)
	}
	return b.String()
}

// QueryRowsForTest roda a consulta pelo MESMO motor, pelo MESMO orçamento e
// pelo MESMO escritor NDJSON da rota /v1/query, sem HTTP, e devolve uma linha
// por row. Existe pela razão do MetricsExpositionForTest: é o único jeito de
// ler uma tabela ANTES do Run, e portanto antes de qualquer passada dos laços.
func (s *Server) QueryRowsForTest(dataset, sql string) ([]string, error) {
	it, err := s.eng.Execute(context.Background(), dataset, sql, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	orc := s.budget.Query(s.cfg.MaxBufferPerQuery())
	if !orc.Admit() {
		return nil, errors.New("query budget refused the test query")
	}
	defer orc.Release()
	var b bytes.Buffer
	w := executor.NewStreamWriter(&b, executor.DefaultBufSize, orc)
	for it.Next() {
		if err := w.WriteRow(it.Row()); err != nil {
			return nil, err
		}
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	text := strings.TrimSpace(b.String())
	if text == "" {
		return nil, nil
	}
	return strings.Split(text, "\n"), nil
}

// StopEmittersForTest para o laço da emissão periódica de cada contador do
// dataset, para que o teste conduza cada passada com EmitOnceForTest.
//
// Existe pela razão do StopSamplingAndAlarmsForTest: o `every` mínimo do YAML é
// de 10 s, esperar a segunda passada custaria isso em relógio de parede, e
// EmitOnce não é thread-safe — chamá-lo com o laço de pé seria corrida. O Stop
// aguarda a passada em voo, e o encerra() continua parando e fechando os
// emissores (Stop e Close são idempotentes).
func (s *Server) StopEmittersForTest(dataset string) {
	d := s.byNome[dataset]
	if d == nil {
		return
	}
	for _, e := range d.emitters {
		e.Stop()
	}
}

// EmitOnceForTest faz uma passada da emissão de cada contador do dataset e
// esvazia o buffer, para que a linha esteja no arquivo quando o teste o ler.
// Só depois de StopEmittersForTest.
//
// O at é a fronteira mais recente do relógio do servidor (floor(now, every)),
// o mesmo carimbo que o laço daria a uma passada naquela fronteira.
func (s *Server) EmitOnceForTest(dataset string) error {
	d := s.byNome[dataset]
	if d == nil {
		return fmt.Errorf("dataset %q is not open", dataset)
	}
	if len(d.emitters) == 0 {
		return fmt.Errorf("dataset %q has no emitter", dataset)
	}
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	for _, e := range d.emitters {
		if err := e.EmitOnce(context.Background(), series.FloorBoundary(now(), e.Every())); err != nil {
			return err
		}
		if err := e.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// FollowerStateForTest devolve o estado, a epoch e o cursor aplicado do
// follower do dataset, que o /v1/health só mostra fora de following.
func (s *Server) FollowerStateForTest(dataset string) (replication.ReplicaState, string, uint64) {
	d := s.byNome[dataset]
	if d == nil || d.follower == nil {
		return "", "", 0
	}
	return d.follower.Replica.State()
}

// UseStoreClockForTest troca o relógio do Store do dataset (o da expiração).
// Chamado antes do Run: o prazo mínimo de TTL é de um minuto, e o teste do
// encerramento precisa de linhas vencidas na primeira passada do coletor.
func (s *Server) UseStoreClockForTest(dataset string, now func() time.Time) {
	if d := s.byNome[dataset]; d != nil {
		d.st.UsaRelogio(now)
	}
}
