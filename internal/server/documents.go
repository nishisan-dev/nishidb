package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
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
func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.rejectSimples(w, r, http.MethodGet)
		return
	}
	filtro := r.URL.Query().Get("dataset")
	if filtro != "" {
		d, ok := s.byNome[filtro]
		if !ok {
			s.reject(w, r, http.StatusNotFound, datasetDesconhecido, protocol.ErrorDetail{
				Code:    protocol.CodeUnknownDataset,
				Message: "dataset " + filtro + " não está registrado neste servidor",
			})
			return
		}
		meta, err := s.metaOfDataset(d)
		if err != nil {
			s.rejectCatalog(w, r, d, err)
			return
		}
		s.respondeJSON(w, protocol.Metadata{Datasets: []protocol.DatasetMeta{meta}})
		return
	}

	out := protocol.Metadata{Datasets: make([]protocol.DatasetMeta, 0, len(s.datasets))}
	for _, d := range s.datasets {
		meta, err := s.metaOfDataset(d)
		if err != nil {
			// A resposta INTEIRA cai, e não só o dataset que falhou: um
			// documento a que falta um dataset é indistinguível de um servidor
			// que não o hospeda, e quem monta árvore a partir dele apagaria as
			// tabelas dele da tela sem sintoma nenhum.
			s.rejectCatalog(w, r, d, err)
			return
		}
		out.Datasets = append(out.Datasets, meta)
	}
	s.respondeJSON(w, out)
}

// rejectCatalog recusa a rota quando o catálogo de um dataset não pôde ser
// montado. São TRÊS as falhas que chegam aqui, e nenhuma é uso:
//
//   - o dataset não estar registrado no motor (catalogEntries) — divergência
//     entre o registro do motor e a lista deste servidor, já que todo dataset
//     desta lista foi registrado na subida;
//   - a spec sintética da `_meta.fields` não compilar (meta.ColumnsOfFields):
//     ela é constante do binário, então só falha por defeito de código;
//   - o catálogo não ter tabela base (metaOfDataset) — que é a falha coberta
//     aqui e é estruturalmente inalcançável: a enumeração acrescenta a linha de
//     Kind TABLE antes de qualquer outra e a ordem a mantém em primeiro. A
//     guarda existe para que a invariante seja VERIFICADA em vez de suposta,
//     porque o que ela protege é o campo `columns` de um cliente anterior.
//
// Daí o INTERNAL: não há ação do cliente que provoque ou evite qualquer uma das
// três.
//
// A chave de log e as duas mensagens nascem em inglês (débito 116). A chave de
// campo `erro` é a exceção deliberada: ela é o vocabulário do resto do processo,
// e um mesmo campo com dois nomes no mesmo fluxo de log custa mais do que um
// campo em português.
func (s *Server) rejectCatalog(w http.ResponseWriter, r *http.Request, d *dataset, err error) {
	s.log.Warn("metadata.catalog_failed", "dataset catalog could not be assembled",
		"dataset", d.nome, "erro", err.Error())
	s.reject(w, r, http.StatusInternalServerError, d.nome, protocol.ErrorDetail{
		Code:    protocol.CodeInternal,
		Message: "the catalog of dataset " + d.nome + " could not be assembled",
	})
}

// metaOfDataset descreve um dataset para o /v1/metadata.
//
// A enumeração das tabelas sai do query.Engine.Catalog — o MESMO que a
// `_meta.tables` lê —, e não de uma varredura própria dos registros do motor.
// As duas rotas respondem à mesma pergunta por canais diferentes, e duas
// derivações do mesmo fato divergem: a divergência apareceria como uma árvore
// de cliente SQL que discorda do `SELECT ... FROM _meta.tables` sem nada estar
// quebrado. Ver TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo.
//
// O tipo JDBC de cada coluna é calculado AQUI, pelo columnMeta, porque o driver
// não sabe derivá-lo do nome do tipo — é a mesma razão de o campo existir no
// wire desde o começo.
func (s *Server) metaOfDataset(d *dataset) (protocol.DatasetMeta, error) {
	tables, err := s.eng.Catalog(d.nome)
	if err != nil {
		return protocol.DatasetMeta{}, err
	}
	m := protocol.DatasetMeta{
		Name:   d.nome,
		Schema: d.sch.SchemaName,
		// MaxRows e Timeout saem do COMPILADO, que é onde o default global já
		// foi resolvido contra o que o YAML do dataset declara: quem consulta
		// precisa do teto que vai valer, não de onde ele foi escrito.
		MaxRows: d.sch.MaxRows,
		// TimeoutMs publica o TETO, e DefaultTimeoutMs o prazo padrão. O campo
		// que já existia continua carregando o número que o servidor FAZ
		// VALER — que é o que ele já significava para o driver, cujo clamp
		// existe porque o servidor recusa acima do teto. Ver DatasetMeta.
		TimeoutMs:        d.sch.MaxTimeout.Milliseconds(),
		DefaultTimeoutMs: d.sch.Timeout.Milliseconds(),
		MaxCols:          d.sch.MaxCols,
		OnMaxCols:        d.sch.OnMaxCols,
		MaxNestedCols:    d.sch.MaxNestedCols,
		Tables:           make([]protocol.TableMeta, 0, len(tables)),
	}
	if d.sch.Key != nil {
		m.Key = d.sch.Key.Name
	}
	for _, t := range tables {
		// As colunas do catálogo são as de FieldsForMetadata, e não as de
		// FieldsByID: a coluna derivada fica fora do SELECT * e este é o lugar
		// onde ela é descoberta. Esconder aqui também deixaria o operador sem
		// forma nenhuma de saber que ela existe. Ver query.TableInfo.Columns.
		tm := protocol.TableMeta{
			Name:        t.Name,
			Schema:      t.Schema,
			Namespace:   t.Namespace,
			Kind:        string(t.Kind),
			Description: t.Description,
			Columns:     make([]protocol.ColumnMeta, 0, len(t.Columns)),
		}
		for _, f := range t.Columns {
			tm.Columns = append(tm.Columns, columnMeta(f))
		}
		// O campo Columns do dataset — o que o driver e o client Python já leem
		// — é a MESMA lista da tabela base, e não uma segunda montagem a partir
		// de d.sch: o slice é compartilhado, então não há como as duas
		// discordarem. A base é a única linha de Kind TABLE do catálogo (a
		// enumeração cria uma só, e todo o resto é VIEW).
		if t.Kind == query.KindTable {
			m.Columns = tm.Columns
		}
		m.Tables = append(m.Tables, tm)
	}
	if m.Columns == nil {
		// Catálogo sem tabela base é divergência entre o registro do motor e a
		// lista do servidor. Publicar `"columns": null` aqui entregaria a um
		// cliente anterior um dataset sem coluna nenhuma, que é uma resposta
		// plausível e errada.
		return protocol.DatasetMeta{}, fmt.Errorf("server: o catálogo de %q não tem tabela base", d.nome)
	}
	return m, nil
}

func columnMeta(f *schema.CompiledField) protocol.ColumnMeta {
	// O tipo é o EFETIVO: para campo auto, o que a primeira observação fixou —
	// e types.TypeNull quando nenhum evento o fixou ainda, que o protocolo
	// traduz para "unknown"/"OTHER" em vez de escolher um tipo plausível.
	t := f.EffectiveType()
	d := protocol.DescriptorOf(t)
	c := protocol.ColumnMeta{
		Name: f.Name,
		Type: protocol.TypeName(t),
		JDBC: d.JDBC,
		// Mesma regra do executor, e a única fonte possível aqui: /v1/metadata
		// descreve o SCHEMA, então o papel do campo vem direto do compilado.
		Nullable:    !f.IsKey && !f.IsSequence,
		Precision:   d.Precision,
		Scale:       d.Scale,
		DisplaySize: d.DisplaySize,
		Index:       f.IndexName(),
		Auto:        f.IsAuto,
	}
	if f.Alias != "" {
		alias := f.Alias
		c.Alias = &alias
	}
	return c
}

// counterStats traduz o retrato dos contadores para o wire. Lista vazia, e
// não nil, no dataset sem contador: o JSON sai como `[]`, e bloco presente é o
// que separa "sem contadores" de "servidor antigo".
func counterStats(ests []counter.Stats) []protocol.CounterStats {
	out := make([]protocol.CounterStats, 0, len(ests))
	for _, e := range ests {
		out = append(out, protocol.CounterStats{
			Name:                 e.Name,
			Rows:                 e.Rows,
			Total:                e.Total,
			Groups:               e.Groups,
			MaxGroups:            e.Ceiling,
			Retired:              e.Retired,
			Refused:              e.Refused,
			OrphanEntries:        e.Orphans,
			Bytes:                e.Bytes,
			AuditDivergentGroups: e.AuditDivergences,
			AuditLastSuccessUnix: e.AuditLastUnix,
		})
	}
	return out
}

// handleStats serve o GET /v1/stats do §67.
//
// Tudo aqui é lido das MESMAS estruturas que o /metrics lê no scrape, no
// instante da chamada. Nenhum contador é criado para esta rota: duas fontes
// para o mesmo número divergem, e a divergência aparece como um painel que
// discorda do JSON sem nada estar quebrado.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.rejectSimples(w, r, http.MethodGet)
		return
	}

	in := s.eng.Metrics()
	pm := s.eng.PlanMetrics()
	out := protocol.Stats{
		Process: protocol.ProcessStats{
			Version: buildinfo.String(),
			Query: protocol.EngineStats{
				FullScans:    pm.FullScan.Load(),
				PointLookups: pm.PointLookup.Load(),
				BitmapScans:  pm.IndexScan.Load(),
				IndexUnions:  pm.IndexUnion.Load(),
				Candidates:   in.Candidates.Load(),
				Rejected:     in.Rejected.Load(),
				DeadSkipped:  in.DeadSkipped.Load(),
				TypeMismatch: in.TypeMismatch.Load(),
				Emitted:      in.Emitted.Load(),
				Truncated:    in.Truncated.Load(),
				Canceled:     in.Canceled.Load(),
			},
			// Lido no instante da chamada, da MESMA estrutura que alimenta
			// nishidb_query_longest_running_seconds — nenhum contador novo,
			// pela regra deste handler: duas fontes para o mesmo número
			// divergem, e a divergência aparece como um painel que discorda do
			// JSON sem nada estar quebrado.
			InFlight: s.inVoo.instantaneo(time.Now()),
		},
		Datasets: make([]protocol.DatasetStats, 0, len(s.datasets)),
	}

	for _, d := range s.datasets {
		st := d.st.Stats()
		state, detalhe := stateOfIngestion(d)
		ds := protocol.DatasetStats{
			Name: d.nome,
			// O bloco de schema sai do COMPILADO e é preenchido sempre, fora
			// de qualquer condicional de consumidor: o dicionário cresce por
			// toda rota de escrita, não só pela do Kafka.
			Schema: protocol.SchemaStats{
				Fields:             d.sch.KnownFieldCount(),
				FieldsDropped:      d.sch.DiscardedFieldCount(),
				NestedTypeMismatch: d.sch.IncompatibleValueCount(),
				MaxCols:            d.sch.MaxCols,
				OnMaxCols:          d.sch.OnMaxCols,
			},
			Storage: protocol.StorageStats{
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
				GarbageBytes: garbageOfDisk(st),
				Compaction: protocol.CompactionStats{
					Running:      st.CompactionRunning,
					BytesRead:    st.CompactionBytesRead,
					BytesWritten: st.CompactionBytesWritten,
				},
			},
			Query: protocol.RouteStats{
				Total:  s.met.total.With(d.nome).Value(),
				Active: int64(s.met.ativas.With(d.nome).Value()),
			},
			Ingestion: protocol.IngestionStats{
				Enabled: state != protocol.StateDisabled,
				State:   state,
				Paused:  detalhe,
			},
			History: historyStats(d),
			// Da MESMA estrutura que alimenta as séries nishidb_index_*, pela
			// regra deste handler. O bloco sai em todos os datasets, inclusive na
			// irmã, porque o índice por segmento é mecânica do Store e não
			// política declarada no YAML do base — que é o que o bloco History
			// acima descreve.
			Index: indexStats(d.st.EstatisticasOfIndex()),
			// Do MESMO Store que alimenta as séries nishidb_query_snapshot_*,
			// pela regra deste handler.
			Snapshot: protocol.SnapshotStats{
				Bytes:          d.st.BytesRetidos(),
				OldestSeconds:  d.st.IDadeOfSnapshotMaisAntigo().Seconds(),
				PinnedSegments: d.st.SegmentosPinados(),
				ExpiredTotal:   d.st.SnapshotsExpirados(),
			},
			// Da MESMA leitura que alimenta as séries nishidb_counter_*.
			Counters: counterStats(d.st.EstatisticasOfCounters()),
		}
		// A tabela irmã não tem materializer: quem escreve nela é o do
		// dataset base, no mesmo evento. Publicar um bloco zerado ali seria
		// dizer que nenhuma política de escrita agiu, quando na verdade elas
		// agiram — do outro lado.
		if d.mat != nil {
			ds.Materialization = materializationStats(d.spec, d.mat.Metrics())
		}
		ds.Query.Errors = s.errosOfDataset(d.nome)

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

// errosOfDataset soma as categorias de erro do dataset.
//
// A soma acontece aqui, e não num contador próprio, porque o /metrics precisa
// da quebra por categoria (é ela que diz se o problema é sintaxe do cliente ou
// dataset corrompido) e o JSON precisa do total. Um segundo contador para o
// total seria o mesmo número mantido em dois lugares — e, no dia em que uma
// categoria nova esquecesse de somar nele, os dois discordariam.
func (s *Server) errosOfDataset(name string) uint64 {
	var total uint64
	for _, a := range s.met.errors.Collect() {
		for _, l := range a.Labels {
			if l.Name == metrics.LabelDataset && l.Value == name {
				total += uint64(a.Value)
			}
		}
	}
	return total
}

func (s *Server) respondeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		s.log.Warn("resposta.escrita_falhou", "resposta interrompida", "erro", err.Error())
	}
}

// rejectSimples responde método errado numa rota de leitura.
func (s *Server) rejectSimples(w http.ResponseWriter, r *http.Request, permitido string) {
	w.Header().Set("Allow", permitido)
	s.reject(w, r, http.StatusMethodNotAllowed, datasetDesconhecido, protocol.ErrorDetail{
		Code:    protocol.CodeMethod,
		Message: r.Method + " não é aceito nesta rota; use " + permitido,
	})
}
