package protocolo

// Os corpos de GET /v1/metadata e GET /v1/stats.
//
// O §67 nomeia as duas rotas e não define o conteúdo de nenhuma. O que está
// aqui é a forma que o DatabaseMetaData e o ResultSetMetaData do §71 vão
// consumir na Fase 7 — decidida agora, olhando para esse consumidor, porque um
// formato mínimo hoje viraria um SEGUNDO formato depois.

// Metadados é o corpo de GET /v1/metadata. Sem filtro traz todos os datasets;
// com ?dataset=<nome> traz só um, na mesma forma — o cliente não precisa
// aprender dois formatos para as duas perguntas.
type Metadados struct {
	Datasets []DatasetMeta `json:"datasets"`
}

// DatasetMeta descreve um dataset consultável.
//
// MaxRows e TimeoutMs são os valores EFETIVOS (o do dataset, ou o default
// global que ele herdou), e não o que está escrito no YAML dele: quem consulta
// precisa saber o teto que vai valer, não onde ele foi declarado.
type DatasetMeta struct {
	Name      string       `json:"name"`
	Key       string       `json:"key"`
	MaxRows   int          `json:"maxRows"`
	TimeoutMs int64        `json:"timeoutMs"`
	Columns   []ColunaMeta `json:"columns"`
}

// ColunaMeta descreve uma coluna do dataset.
//
// Index é o índice DECLARADO no YAML ("none", "bitmap" ou "hash"), não uma
// promessa de plano: o hash é aceito na configuração desde o M1 e o índice
// físico só chega na Fase 8, quando as queries sobre o campo deixam de cair em
// scan. É metadado de schema, e quem quiser saber o plano escolhido olha
// nishidb_query_full_scans.
//
// Auto marca o campo sem tipo declarado. Quando ele ainda não foi observado,
// Type é "unknown" e JDBC é "OTHER" — a única resposta honesta, já que o
// próximo evento pode fixá-lo como qualquer coisa.
type ColunaMeta struct {
	Name  string  `json:"name"`
	Alias *string `json:"alias"`
	Type  string  `json:"type"`
	JDBC  string  `json:"jdbc"`
	Index string  `json:"index"`
	Auto  bool    `json:"auto"`
}

// Estatisticas é o corpo de GET /v1/stats.
//
// A divisão entre Process e Datasets não é estética: o executor e o planner
// mantêm UM conjunto de contadores por Engine, compartilhado por todos os
// datasets do processo (ver internal/query/engine.go). Repetir esses números
// dentro de cada dataset publicaria o MESMO valor N vezes sob N nomes
// diferentes — e alguém, em produção, somaria os N e concluiria que houve N
// vezes mais full scans do que houve.
//
// Os números daqui saem das mesmas estruturas que o /metrics lê no scrape, no
// instante da chamada. Nenhum contador novo, nenhuma cópia de estado: duas
// fontes para o mesmo número divergem, e a divergência aparece como um painel
// que discorda do JSON sem que nada esteja quebrado.
type Estatisticas struct {
	Process  ProcessoStats  `json:"process"`
	Datasets []DatasetStats `json:"datasets"`
}

// ProcessoStats agrupa o que é do processo, não de um dataset.
type ProcessoStats struct {
	Query MotorStats `json:"query"`
}

// MotorStats são os contadores do motor de consulta (executor e planner).
//
// FullScans é a métrica NORMATIVA do §64, contada no plan-time; os demais são
// diagnóstico. Rejected > 0 é a prova de que a revalidação do §44 está fazendo
// trabalho de verdade — o índice devolve um superset e o executor descarta o
// que não casa o predicado corrente.
type MotorStats struct {
	FullScans    int64 `json:"fullScans"`
	PointLookups int64 `json:"pointLookups"`
	BitmapScans  int64 `json:"bitmapScans"`
	IndexUnions  int64 `json:"indexUnions"`
	Candidates   int64 `json:"candidates"`
	Rejected     int64 `json:"rejected"`
	DeadSkipped  int64 `json:"deadSkipped"`
	TypeMismatch int64 `json:"typeMismatch"`
	Emitted      int64 `json:"emitted"`
	Truncated    int64 `json:"truncated"`
	Canceled     int64 `json:"canceled"`
}

// DatasetStats é o que TEM granularidade por dataset.
type DatasetStats struct {
	Name      string        `json:"name"`
	Storage   StorageStats  `json:"storage"`
	Query     RotaStats     `json:"query"`
	Ingestion IngestaoStats `json:"ingestion"`
}

// StorageStats vem de storage.Store.Stats().
//
// Rows é a contagem de linhas vivas; Segments e CurrentSegment coincidem
// enquanto não houver compaction (os IDs são monotônicos desde 1, sem remoção).
// PrimaryLoadFactor acima de ~0,7 é o sinal de que o índice primário vai
// precisar crescer.
type StorageStats struct {
	Rows              uint64  `json:"rows"`
	Segments          uint32  `json:"segments"`
	CurrentSegment    uint32  `json:"currentSegment"`
	PrimaryLoadFactor float64 `json:"primaryLoadFactor"`
	Overlay           int     `json:"overlay"`
}

// RotaStats são os contadores do §92 que o protocolo alimenta, por dataset.
//
// A latência NÃO aparece aqui de propósito: ela é um histograma, e um
// histograma serializado em JSON é pior que inútil — o formato certo para ele é
// o /metrics, onde os buckets cumulativos têm quem os leia.
type RotaStats struct {
	Total  uint64 `json:"total"`
	Active int64  `json:"active"`
	Errors uint64 `json:"errors"`
}

// IngestaoStats é o estado da ingestão Kafka do dataset.
//
// State usa o vocabulário do §94 ("UP", "DOWN", "DEGRADED", "DISABLED"), o
// mesmo do /v1/health, para que as duas rotas não descrevam a mesma realidade
// com palavras diferentes. Paused traz o motivo quando a ingestão está parada
// por política de erro (§101) e fica vazio quando tudo corre bem.
type IngestaoStats struct {
	Enabled   bool   `json:"enabled"`
	State     string `json:"state"`
	Events    int64  `json:"events"`
	Applied   int64  `json:"applied"`
	Stale     int64  `json:"stale"`
	Discarded int64  `json:"discarded"`
	Failed    int64  `json:"failed"`
	Paused    string `json:"paused,omitempty"`
}
