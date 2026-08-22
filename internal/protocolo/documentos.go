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
// Rows é a contagem de linhas vivas. Segments e CurrentSegment JÁ NÃO
// coincidem: eles coincidiam enquanto os IDs eram monotônicos desde 1 e nada
// removia arquivo, premissa que a compaction do §53-55 quebrou — o ID nunca
// regride, mas o segmento é aposentado, e a distância entre os dois é
// justamente o que a compaction devolveu. PrimaryLoadFactor acima de ~0,7 é o
// sinal de que o índice primário vai precisar crescer.
//
// Bytes, GarbageBytes e Compaction entram pelo §96: o `nishidb status` resume
// STORAGE, e o disco é o assunto do M4 — o §86 dá 2 a 3 dias até o ENOSPC a
// 500 ev/s, curto demais para o operador descobrir pelo alerta do filesystem.
// Eles saem da MESMA leitura de Store.Stats() que o /metrics faz no scrape, e
// não de contador próprio: duas fontes para o mesmo número divergem, e a
// divergência aparece como um painel que discorda do JSON sem nada estar
// quebrado.
type StorageStats struct {
	Rows              uint64  `json:"rows"`
	Segments          uint32  `json:"segments"`
	CurrentSegment    uint32  `json:"currentSegment"`
	PrimaryLoadFactor float64 `json:"primaryLoadFactor"`
	Overlay           int     `json:"overlay"`
	// Bytes é a soma do tamanho dos arquivos de segmento, medida no último
	// ListSegments — que roda no checkpoint. A ressalva é de FRESCOR, não de
	// exatidão, e a direção é garantida: limite INFERIOR, nunca superior. É a
	// mesma nishidb_storage_bytes, com a ressalva escrita em
	// storage.Stats.StorageBytes.
	Bytes int64 `json:"bytes"`
	// GarbageBytes é `disco - vivo` com piso em zero. Como o vivo é limite
	// SUPERIOR (débitos 38 e 45), a diferença é limite INFERIOR do lixo: "há
	// pelo menos isto de garbage". O nome não carrega o `>=` porque o campo
	// JSON é lido por máquina; quem escreve para humano — o `nishidb status` —
	// é que marca a semântica na coluna.
	GarbageBytes uint64          `json:"garbageBytes"`
	Compaction   CompactionStats `json:"compaction"`
}

// CompactionStats é o estado do laço do §53-55 para o dataset.
//
// Running afirma que o LAÇO está de pé, não que há varredura em voo neste
// instante — a passada dura dezenas de milissegundos contra um cochilo de
// segundos, e um medidor de "varredura em voo" responderia falso em quase toda
// pergunta. O caso que importa não é o Close: é a falha sticky do writer
// (§14.14), em que a goroutine para sozinha e o disco volta a crescer com o
// processo de pé.
//
// BytesRead e BytesWritten são contadores monotônicos por processo, e a
// distância entre eles é o disco devolvido — a pergunta do §86.
type CompactionStats struct {
	Running      bool  `json:"running"`
	BytesRead    int64 `json:"bytesRead"`
	BytesWritten int64 `json:"bytesWritten"`
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
