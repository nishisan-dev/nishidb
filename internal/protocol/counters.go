package protocol

// Os corpos de POST /v1/counters/audit e POST /v1/counters/rebuild
// (planning/contadores-mantidos-na-escrita.md §5.6 e §11.6): as rotas ADMIN
// dos contadores mantidos na escrita. Aceitam o MESMO corpo opcional das
// mutações (DatasetRequest): um dataset por nome, ou nenhum campo para
// todos, na ordem determinística da subida. Nenhum código de erro novo: as
// falhas saem pelos códigos que já existem (DATASET_CORRUPT, WRITER_FAILED,
// SNAPSHOT_TOO_OLD, INTERNAL).

// CountersAuditResponse é o corpo de 200 de POST /v1/counters/audit.
type CountersAuditResponse struct {
	Datasets []DatasetAudit `json:"datasets"`
}

// DatasetAudit são os relatórios de um dataset — lista vazia no dataset
// sem contadores, que é resposta legítima e não ausência.
type DatasetAudit struct {
	Name     string         `json:"name"`
	Counters []CounterAudit `json:"counters"`
}

// CounterAudit é o resultado da auditoria de UM contador: o retrato T0
// dos totais contra o recalculado por varredura das linhas vivas.
//
// DivergentGroups conta grupos com total OU refs diferentes; NotInterned
// conta linhas vivas cujo grupo o dicionário não tem. Os dois em zero é
// "posso confiar neste número". Sample nomeia as primeiras divergências pelo
// groupKey, com teto — um contador inteiramente errado não vira um relatório
// do tamanho do inventário.
type CounterAudit struct {
	Name            string `json:"name"`
	Groups          uint32 `json:"groups"`
	Maintained      int64  `json:"maintained"`
	Recomputed      int64  `json:"recomputed"`
	DivergentGroups int    `json:"divergentGroups"`
	NotInterned     uint64 `json:"notInterned"`
	// AggregateRows e AggregateTotal são os agregados MANTIDOS pelo contador
	// em T0; comparados com rows/total recomputados, dizem se a segunda fonte
	// do mesmo fato derivou. DivergentAggregates é 0, 1 ou 2 — separado de
	// DivergentGroups porque a pergunta é outra: uma linha que trocou de grupo
	// deixa os grupos errados e o agregado certo.
	AggregateRows       int64 `json:"aggregateRows"`
	AggregateTotal      int64 `json:"aggregateTotal"`
	DivergentAggregates int   `json:"divergentAggregates"`
	// RowsRecomputed é a soma dos refs recalculados — o par de AggregateRows.
	RowsRecomputed int64             `json:"rowsRecomputed"`
	Sample         []GroupDivergence `json:"sample,omitempty"`
	RowsScanned    uint64            `json:"rowsScanned"`
	DurationMs     int64             `json:"durationMs"`
}

// GroupDivergence é um grupo que divergiu, pelo groupKey.
type GroupDivergence struct {
	GroupKey       string `json:"groupKey"`
	Maintained     int64  `json:"maintained"`
	Recomputed     int64  `json:"recomputed"`
	RefsMaintained int64  `json:"refsMaintained"`
	RefsRecomputed int64  `json:"refsRecomputed"`
}

// CountersRebuildResponse é o corpo de 200 de POST /v1/counters/rebuild.
type CountersRebuildResponse struct {
	Datasets []DatasetRebuild `json:"datasets"`
}

// DatasetRebuild são as reconstruções de um dataset.
type DatasetRebuild struct {
	Name     string           `json:"name"`
	Counters []CounterRebuild `json:"counters"`
}

// CounterRebuild é o resultado da reconstrução de UM contador. A
// reconstrução segura o writer pela varredura inteira: DurationMs é o stall
// real da ingestão.
type CounterRebuild struct {
	Name       string `json:"name"`
	Rows       uint64 `json:"rows"`
	Groups     uint32 `json:"groups"`
	DurationMs int64  `json:"durationMs"`
}
