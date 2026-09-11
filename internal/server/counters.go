// As duas rotas ADMIN dos contadores mantidos na escrita
// (planning/contadores-mantidos-na-escrita.md §5.6 e §11.6): POST
// /v1/counters/audit e POST /v1/counters/rebuild.
//
// São rotas, e não SQL, porque não são leitura: a auditoria varre o dataset
// inteiro segurando um snapshot, e a reconstrução segura o writer. As duas são
// síncronas — a resposta sai depois do trabalho feito —, e é por isso que
// correm sob o ctxOfMutacao: uma varredura de segundos tem de honrar o SIGTERM.
// A leitura dos contadores continua sendo `SELECT … FROM _counters.<nome>`.

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
)

// handleAuditOfCounters serve o POST /v1/counters/audit (ADMIN).
//
// Não segura o writer pela varredura: a ingestão continua. O que ela segura é
// um snapshot de leitura, e o teto do journal pode invalidá-lo — a resposta
// nesse caso é SNAPSHOT_TOO_OLD (409), e não um relatório: uma divergência
// inventada por versão perdida mandaria o operador reconstruir um contador
// certo. Divergência NÃO é erro HTTP: o relatório é o resultado, e é quem
// chama (o CLI) que decide o exit code.
func (s *Server) handleAuditOfCounters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.reject(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeMethod,
			Message: fmt.Sprintf("%s não é aceito em /v1/counters/audit; use POST", r.Method),
		})
		return
	}
	req, fail, status := s.readRequisicaoDataset(w, r)
	if fail != nil {
		s.reject(w, r, status, datasetDesconhecido, *fail)
		return
	}
	targets, fail, status := s.datasetsTarget(req.Dataset)
	if fail != nil {
		s.reject(w, r, status, datasetDesconhecido, *fail)
		return
	}
	ctx, cancel := s.ctxOfMutacao(r)
	defer cancel()

	results, parcial := processDatasets(targets, func(d *dataset) (protocol.DatasetAudit, error) {
		relatorios, err := d.st.AuditaCounters(ctx)
		if err != nil {
			return protocol.DatasetAudit{}, err
		}
		out := protocol.DatasetAudit{Name: d.nome, Counters: make([]protocol.CounterAudit, 0, len(relatorios))}
		for _, rel := range relatorios {
			out.Counters = append(out.Counters, auditCounter(rel))
			s.log.Dataset(d.nome).Info("contadores.auditoria",
				"auditoria do contador concluída",
				"contador", rel.Name, "grupos", rel.Groups, "divergentes", rel.DivergentGroups,
				"naoInternadas", rel.NotInterned, "linhas", rel.RowsScanned, "duracao", rel.Duration.String())
		}
		return out, nil
	})
	if parcial != nil {
		status, f := countersError(parcial.Err)
		f.Message = partialMessage(parcial, f.Message)
		s.reject(w, r, status, parcial.Dataset, f)
		return
	}
	s.respondeJSON(w, protocol.CountersAuditResponse{Datasets: results})
}

// handleRebuildOfCounters serve o POST /v1/counters/rebuild (ADMIN).
//
// Segura o writer pela varredura inteira — é o stall real da ingestão, dito
// no README e no DurationMs da resposta. É o remédio para uma deriva que a
// auditoria apontou: explícito, caro e raro.
func (s *Server) handleRebuildOfCounters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.reject(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.ErrorDetail{
			Code:    protocol.CodeMethod,
			Message: fmt.Sprintf("%s não é aceito em /v1/counters/rebuild; use POST", r.Method),
		})
		return
	}
	req, fail, status := s.readRequisicaoDataset(w, r)
	if fail != nil {
		s.reject(w, r, status, datasetDesconhecido, *fail)
		return
	}
	targets, fail, status := s.datasetsTarget(req.Dataset)
	if fail != nil {
		s.reject(w, r, status, datasetDesconhecido, *fail)
		return
	}
	ctx, cancel := s.ctxOfMutacao(r)
	defer cancel()

	results, parcial := processDatasets(targets, func(d *dataset) (protocol.DatasetRebuild, error) {
		recs, err := d.st.ReconstroiCounters(ctx)
		if err != nil {
			return protocol.DatasetRebuild{}, err
		}
		out := protocol.DatasetRebuild{Name: d.nome, Counters: make([]protocol.CounterRebuild, 0, len(recs))}
		for _, rec := range recs {
			out.Counters = append(out.Counters, protocol.CounterRebuild{
				Name: rec.Name, Rows: rec.Rows, Groups: rec.Groups, DurationMs: rec.Duration.Milliseconds(),
			})
		}
		return out, nil
	})
	if parcial != nil {
		status, f := countersError(parcial.Err)
		f.Message = partialMessage(parcial, f.Message)
		s.reject(w, r, status, parcial.Dataset, f)
		return
	}
	s.respondeJSON(w, protocol.CountersRebuildResponse{Datasets: results})
}

// countersError traduz o erro do storage para as duas rotas. Além dos casos
// de mutationError, o snapshot estourado — a auditoria ficou inconclusiva — e
// o contexto cancelado (o servidor encerrando, ou o cliente desistindo).
func countersError(err error) (int, protocol.ErrorDetail) {
	switch {
	case errors.Is(err, storage.ErrExpiredSnapshot):
		return http.StatusConflict, protocol.ErrorDetail{
			Code: protocol.CodeSnapshotExpired,
			Message: "a auditoria ficou inconclusiva: o journal de versões estourou runtime.query.snapshotMaxBytes durante a varredura; " +
				"tente de novo, ou suba o teto",
		}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, protocol.ErrorDetail{Code: protocol.CodeInternal, Message: "interrompida: " + err.Error()}
	}
	return mutationError(err)
}

func auditCounter(rel counter.Report) protocol.CounterAudit {
	out := protocol.CounterAudit{
		Name:            rel.Name,
		Groups:          rel.Groups,
		Maintained:      rel.Maintained,
		Recomputed:      rel.Recomputed,
		DivergentGroups: rel.DivergentGroups,
		NotInterned:     rel.NotInterned,

		AggregateRows:       rel.AggregateRows,
		AggregateTotal:      rel.AggregateTotal,
		DivergentAggregates: rel.DivergentAggregates,
		RowsRecomputed:      rel.RowsRecomputed,
		RowsScanned:         rel.RowsScanned,
		DurationMs:          rel.Duration.Milliseconds(),
	}
	for _, d := range rel.Sample {
		out.Sample = append(out.Sample, protocol.GroupDivergence{
			GroupKey: d.Text, Maintained: d.Maintained, Recomputed: d.Recomputed,
			RefsMaintained: d.RefsMaintained, RefsRecomputed: d.RefsRecomputed,
		})
	}
	return out
}
