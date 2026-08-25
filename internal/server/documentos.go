package server

import (
	"encoding/json"
	"net/http"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
	"github.com/nishisan-dev/nishidb/internal/schema"
)

// handleMetadata serve o GET /v1/metadata do §67.
//
// O §67 nomeia a rota e não define o corpo. O que sai daqui é o que o
// DatabaseMetaData do §71 vai precisar na Fase 7 — colunas com tipo, alias,
// índice declarado e os limites efetivos —, decidido agora e de uma vez: um
// corpo mínimo hoje viraria um segundo formato depois, e o driver teria de
// falar os dois.
//
// O filtro ?dataset= devolve a MESMA forma, com um elemento. Duas formas para
// as duas perguntas obrigariam o cliente a aprender dois parsers.
func (s *Servidor) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.recusaSimples(w, r, http.MethodGet)
		return
	}
	filtro := r.URL.Query().Get("dataset")
	if filtro != "" {
		d, ok := s.porNome[filtro]
		if !ok {
			s.recusa(w, r, http.StatusNotFound, datasetDesconhecido, protocolo.Falha{
				Code:    protocolo.CodDatasetDesconhecido,
				Message: "dataset " + filtro + " não está registrado neste servidor",
			})
			return
		}
		s.respondeJSON(w, protocolo.Metadados{Datasets: []protocolo.DatasetMeta{metaDoDataset(d)}})
		return
	}

	out := protocolo.Metadados{Datasets: make([]protocolo.DatasetMeta, 0, len(s.datasets))}
	for _, d := range s.datasets {
		out.Datasets = append(out.Datasets, metaDoDataset(d))
	}
	s.respondeJSON(w, out)
}

func metaDoDataset(d *dataset) protocolo.DatasetMeta {
	// FieldsParaMetadata, e não FieldsByID: a coluna derivada fica fora do
	// SELECT * e este é o lugar onde ela é descoberta. Esconder aqui também
	// deixaria o operador sem forma nenhuma de saber que ela existe.
	campos := d.sch.FieldsParaMetadata()
	m := protocolo.DatasetMeta{
		Name: d.nome,
		// MaxRows e Timeout saem do COMPILADO, que é onde o default global já
		// foi resolvido contra o que o YAML do dataset declara: quem consulta
		// precisa do teto que vai valer, não de onde ele foi escrito.
		MaxRows:       d.sch.MaxRows,
		TimeoutMs:     d.sch.Timeout.Milliseconds(),
		MaxCols:       d.sch.MaxCols,
		OnMaxCols:     d.sch.OnMaxCols,
		MaxNestedCols: d.sch.MaxNestedCols,
		Columns:       make([]protocolo.ColunaMeta, 0, len(campos)),
	}
	if d.sch.Key != nil {
		m.Key = d.sch.Key.Name
	}
	for _, f := range campos {
		m.Columns = append(m.Columns, colunaMeta(f))
	}
	return m
}

func colunaMeta(f *schema.CompiledField) protocolo.ColunaMeta {
	// O tipo é o EFETIVO: para campo auto, o que a primeira observação fixou —
	// e types.TypeNull quando nenhum evento o fixou ainda, que o protocolo
	// traduz para "unknown"/"OTHER" em vez de escolher um tipo plausível.
	t := f.EffectiveType()
	d := protocolo.DescritorDeTipo(t)
	c := protocolo.ColunaMeta{
		Name: f.Name,
		Type: protocolo.NomeDeTipo(t),
		JDBC: d.JDBC,
		// Mesma regra do executor, e a única fonte possível aqui: /v1/metadata
		// descreve o SCHEMA, então o papel do campo vem direto do compilado.
		Nullable:    !f.IsKey && !f.IsSequence,
		Precision:   d.Precision,
		Scale:       d.Scale,
		DisplaySize: d.DisplaySize,
		Index:       f.Index.String(),
		Auto:        f.IsAuto,
	}
	if f.Alias != "" {
		alias := f.Alias
		c.Alias = &alias
	}
	return c
}

// handleStats serve o GET /v1/stats do §67.
//
// Tudo aqui é lido das MESMAS estruturas que o /metrics lê no scrape, no
// instante da chamada. Nenhum contador é criado para esta rota: duas fontes
// para o mesmo número divergem, e a divergência aparece como um painel que
// discorda do JSON sem nada estar quebrado.
func (s *Servidor) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.recusaSimples(w, r, http.MethodGet)
		return
	}

	em := s.eng.Metrics()
	pm := s.eng.PlanMetrics()
	out := protocolo.Estatisticas{
		Process: protocolo.ProcessoStats{
			Version: buildinfo.String(),
			Query: protocolo.MotorStats{
				FullScans:    pm.FullScan.Load(),
				PointLookups: pm.PointLookup.Load(),
				BitmapScans:  pm.BitmapScan.Load(),
				IndexUnions:  pm.IndexUnion.Load(),
				Candidates:   em.Candidates.Load(),
				Rejected:     em.Rejected.Load(),
				DeadSkipped:  em.DeadSkipped.Load(),
				TypeMismatch: em.TypeMismatch.Load(),
				Emitted:      em.Emitted.Load(),
				Truncated:    em.Truncated.Load(),
				Canceled:     em.Canceled.Load(),
			},
		},
		Datasets: make([]protocolo.DatasetStats, 0, len(s.datasets)),
	}

	for _, d := range s.datasets {
		st := d.st.Stats()
		estado, detalhe := estadoDaIngestao(d)
		ds := protocolo.DatasetStats{
			Name: d.nome,
			// O bloco de schema sai do COMPILADO e é preenchido sempre, fora
			// de qualquer condicional de consumidor: o dicionário cresce por
			// toda rota de escrita, não só pela do Kafka.
			Schema: protocolo.SchemaStats{
				Fields:        d.sch.CamposConhecidos(),
				FieldsDropped: d.sch.CamposDescartados(),
				MaxCols:       d.sch.MaxCols,
				OnMaxCols:     d.sch.OnMaxCols,
			},
			Storage: protocolo.StorageStats{
				Rows:              st.RowCount,
				Segments:          st.SegmentsTotal,
				CurrentSegment:    st.CurrentSegmentID,
				PrimaryLoadFactor: st.PrimaryLoadFactor,
				Overlay:           st.OverlayLen,
				Bytes:             st.StorageBytes,
				// O lixo sai pela MESMA função que a série do §92 usa. Repetir
				// a subtração aqui daria duas contas para o mesmo número, e o
				// piso — que existe pela conversão de int64 negativo para
				// uint64, não por elegância — teria de ser lembrado nos dois
				// lugares.
				GarbageBytes: lixoDeDisco(st),
				Compaction: protocolo.CompactionStats{
					Running:      st.CompactionRunning,
					BytesRead:    st.CompactionBytesRead,
					BytesWritten: st.CompactionBytesWritten,
				},
			},
			Query: protocolo.RotaStats{
				Total:  s.met.total.Com(d.nome).Valor(),
				Active: int64(s.met.ativas.Com(d.nome).Valor()),
			},
			Ingestion: protocolo.IngestaoStats{
				Enabled: estado != protocolo.EstadoDesligado,
				State:   estado,
				Paused:  detalhe,
			},
		}
		ds.Query.Errors = s.errosDoDataset(d.nome)

		d.mu.Lock()
		cons := d.cons
		d.mu.Unlock()
		if cons != nil {
			desf := cons.Desfecho()
			ds.Ingestion.Events = desf.Eventos
			ds.Ingestion.Applied = desf.Aplicados
			ds.Ingestion.Stale = desf.Stale
			ds.Ingestion.Discarded = desf.Descartados
			ds.Ingestion.Failed = desf.Falhas
		}
		out.Datasets = append(out.Datasets, ds)
	}
	s.respondeJSON(w, out)
}

// errosDoDataset soma as categorias de erro do dataset.
//
// A soma acontece aqui, e não num contador próprio, porque o /metrics precisa
// da quebra por categoria (é ela que diz se o problema é sintaxe do cliente ou
// dataset corrompido) e o JSON precisa do total. Um segundo contador para o
// total seria o mesmo número mantido em dois lugares — e, no dia em que uma
// categoria nova esquecesse de somar nele, os dois discordariam.
func (s *Servidor) errosDoDataset(nome string) uint64 {
	var total uint64
	for _, a := range s.met.erros.Coleta() {
		for _, l := range a.Labels {
			if l.Nome == metrics.LabelDataset && l.Valor == nome {
				total += uint64(a.Valor)
			}
		}
	}
	return total
}

func (s *Servidor) respondeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		s.log.Warn("resposta.escrita_falhou", "resposta interrompida", "erro", err.Error())
	}
}

// recusaSimples responde método errado numa rota de leitura.
func (s *Servidor) recusaSimples(w http.ResponseWriter, r *http.Request, permitido string) {
	w.Header().Set("Allow", permitido)
	s.recusa(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocolo.Falha{
		Code:    protocolo.CodMetodo,
		Message: r.Method + " não é aceito nesta rota; use " + permitido,
	})
}
