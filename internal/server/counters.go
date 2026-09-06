// As duas rotas ADMIN dos contadores mantidos na escrita
// (planning/contadores-mantidos-na-escrita.md §5.6 e §11.6): POST
// /v1/counters/audit e POST /v1/counters/rebuild.
//
// São rotas, e não SQL, porque não são leitura: a auditoria varre o dataset
// inteiro segurando um snapshot, e a reconstrução segura o writer. As duas são
// síncronas — a resposta sai depois do trabalho feito —, e é por isso que
// correm sob o ctxDeMutacao: uma varredura de segundos tem de honrar o SIGTERM.
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

// handleAuditoriaDeContadores serve o POST /v1/counters/audit (ADMIN).
//
// Não segura o writer pela varredura: a ingestão continua. O que ela segura é
// um snapshot de leitura, e o teto do journal pode invalidá-lo — a resposta
// nesse caso é SNAPSHOT_TOO_OLD (409), e não um relatório: uma divergência
// inventada por versão perdida mandaria o operador reconstruir um contador
// certo. Divergência NÃO é erro HTTP: o relatório é o resultado, e é quem
// chama (o CLI) que decide o exit code.
func (s *Servidor) handleAuditoriaDeContadores(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.Falha{
			Code:    protocol.CodMetodo,
			Message: fmt.Sprintf("%s não é aceito em /v1/counters/audit; use POST", r.Method),
		})
		return
	}
	req, falha, status := s.leRequisicaoDataset(w, r)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	alvos, falha, status := s.datasetsAlvo(req.Dataset)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	ctx, cancela := s.ctxDeMutacao(r)
	defer cancela()

	resultados, parcial := processaDatasets(alvos, func(d *dataset) (protocol.AuditoriaDataset, error) {
		relatorios, err := d.st.AuditaContadores(ctx)
		if err != nil {
			return protocol.AuditoriaDataset{}, err
		}
		out := protocol.AuditoriaDataset{Name: d.nome, Counters: make([]protocol.AuditoriaContador, 0, len(relatorios))}
		for _, rel := range relatorios {
			out.Counters = append(out.Counters, auditoriaContador(rel))
			s.log.Dataset(d.nome).Info("contadores.auditoria",
				"auditoria do contador concluída",
				"contador", rel.Nome, "grupos", rel.Grupos, "divergentes", rel.Divergentes,
				"naoInternadas", rel.NaoInternadas, "linhas", rel.LinhasVarridas, "duracao", rel.Duracao.String())
		}
		return out, nil
	})
	if parcial != nil {
		status, f := erroDeContadores(parcial.Err)
		f.Message = mensagemParcial(parcial, f.Message)
		s.recusa(w, r, status, parcial.Dataset, f)
		return
	}
	s.respondeJSON(w, protocol.RespostaAuditoriaDeContadores{Datasets: resultados})
}

// handleReconstrucaoDeContadores serve o POST /v1/counters/rebuild (ADMIN).
//
// Segura o writer pela varredura inteira — é o stall real da ingestão, dito
// no README e no DurationMs da resposta. É o remédio para uma deriva que a
// auditoria apontou: explícito, caro e raro.
func (s *Servidor) handleReconstrucaoDeContadores(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.Falha{
			Code:    protocol.CodMetodo,
			Message: fmt.Sprintf("%s não é aceito em /v1/counters/rebuild; use POST", r.Method),
		})
		return
	}
	req, falha, status := s.leRequisicaoDataset(w, r)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	alvos, falha, status := s.datasetsAlvo(req.Dataset)
	if falha != nil {
		s.recusa(w, r, status, datasetDesconhecido, *falha)
		return
	}
	ctx, cancela := s.ctxDeMutacao(r)
	defer cancela()

	resultados, parcial := processaDatasets(alvos, func(d *dataset) (protocol.ReconstrucaoDataset, error) {
		recs, err := d.st.ReconstroiContadores(ctx)
		if err != nil {
			return protocol.ReconstrucaoDataset{}, err
		}
		out := protocol.ReconstrucaoDataset{Name: d.nome, Counters: make([]protocol.ReconstrucaoContador, 0, len(recs))}
		for _, rec := range recs {
			out.Counters = append(out.Counters, protocol.ReconstrucaoContador{
				Name: rec.Nome, Rows: rec.Linhas, Groups: rec.Grupos, DurationMs: rec.Duracao.Milliseconds(),
			})
		}
		return out, nil
	})
	if parcial != nil {
		status, f := erroDeContadores(parcial.Err)
		f.Message = mensagemParcial(parcial, f.Message)
		s.recusa(w, r, status, parcial.Dataset, f)
		return
	}
	s.respondeJSON(w, protocol.RespostaReconstrucaoDeContadores{Datasets: resultados})
}

// erroDeContadores traduz o erro do storage para as duas rotas. Além dos casos
// de erroDeMutacao, o snapshot estourado — a auditoria ficou inconclusiva — e
// o contexto cancelado (o servidor encerrando, ou o cliente desistindo).
func erroDeContadores(err error) (int, protocol.Falha) {
	switch {
	case errors.Is(err, storage.ErrSnapshotExpirado):
		return http.StatusConflict, protocol.Falha{
			Code: protocol.CodSnapshotExpirado,
			Message: "a auditoria ficou inconclusiva: o journal de versões estourou runtime.query.snapshotMaxBytes durante a varredura; " +
				"tente de novo, ou suba o teto",
		}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, protocol.Falha{Code: protocol.CodInterno, Message: "interrompida: " + err.Error()}
	}
	return erroDeMutacao(err)
}

func auditoriaContador(rel counter.Relatorio) protocol.AuditoriaContador {
	out := protocol.AuditoriaContador{
		Name:            rel.Nome,
		Groups:          rel.Grupos,
		Maintained:      rel.Mantido,
		Recomputed:      rel.Recalculado,
		DivergentGroups: rel.Divergentes,
		NotInterned:     rel.NaoInternadas,

		AggregateRows:       rel.AgregadoLinhas,
		AggregateTotal:      rel.AgregadoTotal,
		DivergentAggregates: rel.AgregadosDivergentes,
		RowsRecomputed:      rel.LinhasRecalculadas,
		RowsScanned:         rel.LinhasVarridas,
		DurationMs:          rel.Duracao.Milliseconds(),
	}
	for _, d := range rel.Amostra {
		out.Sample = append(out.Sample, protocol.DivergenciaDeGrupo{
			GroupKey: d.Texto, Maintained: d.Mantido, Recomputed: d.Recalculado,
			RefsMaintained: d.RefsMantido, RefsRecomputed: d.RefsRecalculado,
		})
	}
	return out
}
