package protocolo

// Os corpos de GET /v1/metadata e GET /v1/stats.
//
// O §67 nomeia as duas rotas e não define o conteúdo de nenhuma. O que está
// aqui é a forma que o DatabaseMetaData e o ResultSetMetaData do §71 vão
// consumir na Fase 7 — decidida agora, olhando para esse consumidor, porque um
// formato mínimo hoje viraria um SEGUNDO formato depois.

// Estado de um subsistema no /v1/health (§94).
//
// As constantes vivem AQUI, e não no servidor, pelo mesmo motivo que a Saude:
// são o vocabulário que o corpo da rota usa, e um cliente que precise
// distinguir DEGRADED de DOWN teria de redeclará-las. É o vocabulário que
// IngestaoStats.State também usa — as duas rotas descrevem a mesma realidade.
const (
	EstadoUP        = "UP"
	EstadoDown      = "DOWN"
	EstadoDegraded  = "DEGRADED"
	EstadoDesligado = "DISABLED"
)

// Saude é o corpo de GET /v1/health (§94).
//
// É o único corpo de rota do §67 que vivia dentro do servidor, e por isso o
// único que um cliente teria de redeclarar para consumir. O CLI remoto e o
// driver JDBC falam o mesmo protocolo: o contrato inteiro mora neste pacote.
type Saude struct {
	Status   string            `json:"status"`
	Kafka    string            `json:"kafka"`
	Storage  string            `json:"storage"`
	Datasets map[string]string `json:"datasets"`
	// Detalhes nomeia o problema por dataset. Não está no §94 e existe
	// porque "DOWN" sozinho manda o operador procurar no log de qual dos
	// datasets, num processo que pode hospedar vários (§82).
	Detalhes map[string]string `json:"detalhes,omitempty"`
}

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
	Name    string `json:"name"`
	Key     string `json:"key"`
	MaxRows int    `json:"maxRows"`
	// TimeoutMs é o TETO — `query.maxTimeout` —, e não o prazo padrão.
	//
	// O campo já significava isso para quem o consome: o clamp do driver JDBC
	// (NishiStatement.timeoutParaOFio) apara o setQueryTimeout da aplicação
	// contra ele justamente porque o servidor RECUSA um pedido acima do teto.
	// Enquanto teto e default eram o mesmo número, publicar um ou outro dava
	// no mesmo; com os dois separados, publicar o default aqui faria o driver
	// clampar em 30s uma consulta que o dataset autoriza a durar dez minutos —
	// e o teto novo não valeria para ninguém. Publicando o teto, o driver
	// alonga sozinho, sem uma linha de Java mudar.
	TimeoutMs int64 `json:"timeoutMs"`
	// DefaultTimeoutMs é o `query.timeout`: o prazo que vale para o request que
	// NÃO manda timeoutMs. Campo ADITIVO — um cliente que não o conhece
	// continua lendo o resto. Ele existe porque, separados os dois números,
	// "quanto tempo esta consulta tem?" deixou de ter resposta única: sem
	// publicá-lo, uma ferramenta mostraria o teto de dez minutos para uma
	// consulta que na verdade vai ser cortada em trinta segundos.
	DefaultTimeoutMs int64 `json:"defaultTimeoutMs"`
	// MaxCols e OnMaxCols são o teto de campos efetivo e o que acontece ao
	// alcançá-lo. Publicados pela mesma razão do MaxRows, e com uma a mais: o
	// bloco `schema:` do YAML aceita chave desconhecida em silêncio, então um
	// `maxColls` com typo parseia limpo e não faz nada. Ler o efetivo de volta
	// é onde o operador vê o typo, como "continua no default".
	MaxCols   int    `json:"maxCols"`
	OnMaxCols string `json:"onMaxCols"`
	// MaxNestedCols é quantas colunas derivadas uma consulta pode projetar.
	// Governa a expansão `pai.*`; ao estourar, o servidor RECUSA nomeando o
	// limite, nunca trunca.
	//
	// Não há campo equivalente na Requisicao, ao contrário do maxRows: pedir
	// MENOS colunas não é uma pergunta que o cliente precise fazer, porque
	// ele já escolhe as colunas escrevendo-as. Um teto de cliente aqui só
	// poderia transformar uma consulta que funciona em erro.
	MaxNestedCols int          `json:"maxNestedCols"`
	Columns       []ColunaMeta `json:"columns"`
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
//
// Os quatro campos de tipo (Type, JDBC, Precision, Scale, DisplaySize) saem do
// MESMO DescritorDeTipo que a linha de schema de /v1/query usa, e
// TestMetadataPublicaOsMesmosNumerosQueALinhaDeSchema cobra que as duas rotas
// descrevam a mesma coluna do mesmo jeito. Os dois pontos de serialização
// ficam em arquivos diferentes: sem esse teste, acrescentar um campo em um e
// esquecer o outro passaria.
type ColunaMeta struct {
	Name        string  `json:"name"`
	Alias       *string `json:"alias"`
	Type        string  `json:"type"`
	JDBC        string  `json:"jdbc"`
	Nullable    bool    `json:"nullable"`
	Precision   int     `json:"precision"`
	Scale       int     `json:"scale"`
	DisplaySize int     `json:"displaySize"`
	Index       string  `json:"index"`
	Auto        bool    `json:"auto"`
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
	// Version é o texto de internal/buildinfo.String() — a MESMA função que
	// o `nishidb --version`, o log de subida, o painel e o benchmark usam.
	// Campo ADITIVO: um cliente que ainda não o conhece continua lendo o
	// resto de Estatisticas sem quebrar. É esta a versão que a Task A4 lê
	// depois de um rollout, para confirmar que o binário no ar é o novo.
	Version string     `json:"version"`
	Query   MotorStats `json:"query"`
	// InFlight são as consultas correndo NESTE instante, da mais antiga para a
	// mais nova. Campo ADITIVO.
	//
	// Vive no bloco do PROCESSO, e não dentro de cada dataset, porque a
	// pergunta que ele responde é do pool: "quem está segurando as vagas?".
	// Cada entrada nomeia o dataset dela, então quem quiser filtrar filtra —
	// mas quebrar a lista por dataset esconderia o que interessa, que é a
	// consulta mais antiga do processo INTEIRO.
	InFlight []ConsultaEmVoo `json:"inFlight"`
}

// ConsultaEmVoo é uma consulta em andamento.
//
// SQLHash, e não o SQL: o texto pode carregar literais com dado do cliente, e o
// §95 proíbe publicar payload por default — mesma razão de o SQL só aparecer no
// log em nível DEBUG. O hash responde a pergunta que o operador faz de fato,
// que é se as consultas presas são a mesma ou várias diferentes.
//
// RunningMs é a idade NO INSTANTE DA CHAMADA, calculada na hora. Publicar o
// instante de início em vez da idade transferiria para o cliente a conta e o
// problema de relógio: o painel roda em outra máquina, e um skew de segundos
// viraria uma consulta "que começou no futuro".
//
// Long diz se ela ocupa uma vaga do sub-pool — isto é, se pediu prazo acima do
// `query.timeout` do dataset. É o que separa "está demorando" de "foi
// autorizada a demorar".
type ConsultaEmVoo struct {
	Dataset   string `json:"dataset"`
	SQLHash   string `json:"sqlHash"`
	RunningMs int64  `json:"runningMs"`
	Long      bool   `json:"long"`
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
	Schema    SchemaStats   `json:"schema"`
	Query     RotaStats     `json:"query"`
	Ingestion IngestaoStats `json:"ingestion"`
	// Materialization são os efeitos das políticas de escrita e das regras de
	// TTL. Fica FORA de Ingestion pela mesma razão do SchemaStats: aquele
	// bloco só é preenchido quando há consumidor Kafka, e estes números valem
	// para qualquer rota de escrita.
	Materialization MaterializacaoStats `json:"materialization"`
	// History descreve a tabela irmã append-only, e é preenchido no DATASET
	// BASE — não na irmã. É lá que o operador procura: quem declarou
	// `history:` num YAML quer saber o que aquele bloco está fazendo, e a
	// irmã aparece na lista com os próprios números de storage e query como
	// qualquer outro dataset.
	History *HistoricoStats `json:"history,omitempty"`
}

// HistoricoStats é o que o bloco `history` está fazendo.
//
// Os três primeiros campos respondem a única pergunta que importa numa tabela
// cuja razão de existir é servir de evidência: ela está COMPLETA? Linhas e
// descartadas somam os eventos que passaram; Degraded diz se ela parou de
// receber.
type HistoricoStats struct {
	// Table é o nome da tabela irmã — o mesmo que vai no FROM.
	Table string `json:"table"`
	// Rows são as linhas gravadas e Dropped os eventos que não viraram linha
	// nenhuma. Contadores de PROCESSO: zeram no restart, como os de densidade.
	Rows    int64 `json:"rows"`
	Dropped int64 `json:"dropped"`
	// Degraded diz que a tabela entrou em modo falho sticky e parou de
	// receber. A consulta a ela continua respondendo, sobre o que já gravou.
	Degraded bool `json:"degraded"`
	// DataBytes é a soma dos SEGMENTOS, que é o que o maxBytes governa e o que
	// a purga sabe devolver. IndexBytes é o resto do diretório — índices e
	// locations.idx —, publicado em separado justamente porque a purga não o
	// alcança: somá-lo ao teto faria a retenção perseguir um número que ela
	// não consegue baixar.
	DataBytes  int64 `json:"dataBytes"`
	IndexBytes int64 `json:"indexBytes"`
	// OldestWrittenAt é o instante da linha mais antiga ainda visível, em
	// millis. É a série que o operador olha primeiro: é ela que mostra o
	// maxBytes engolindo o expireAfterWrite declarado. Zero quando a tabela
	// está vazia.
	OldestWrittenAt int64 `json:"oldestWrittenAt"`
	// SegmentsPurged conta os segmentos aposentados por cada política desde a
	// subida.
	PurgedByTTL  int64 `json:"purgedByTtl"`
	PurgedBySize int64 `json:"purgedBySize"`
	// ExpireAfterWrite e MaxBytes ecoam o que o YAML declarou. Sem eles, quem
	// lê o painel não sabe contra o que comparar os números acima.
	ExpireAfterWrite string `json:"expireAfterWrite,omitempty"`
	MaxBytes         int64  `json:"maxBytes,omitempty"`
}

// MaterializacaoStats conta o que o dado gravado NÃO diz.
//
// Um valor congelado é indistinguível de um que nunca mudou; um total somado é
// indistinguível de um total que a origem mandou pronto; e uma regra de TTL
// que nunca casa parece igual a uma que casa sempre. Estes contadores são a
// única forma de o operador ver as políticas trabalhando.
type MaterializacaoStats struct {
	// FrozenCaptured é a entry emitida na ingestão e FrozenPreserved o valor
	// anterior segurado pelo merge. Captura sem preservação é coluna que nunca
	// chegou a congelar.
	FrozenCaptured  int64 `json:"frozenCaptured"`
	FrozenPreserved int64 `json:"frozenPreserved"`
	// Summed conta as somas do `merge: sum`, e SummedSaturated as que bateram
	// no teto do int64 — a partir dali o valor para de crescer.
	Summed          int64 `json:"summed"`
	SummedSaturated int64 `json:"summedSaturated"`
	// TTLRules sai na ORDEM do YAML, que é semântica: a primeira que casa
	// vence. Vazia num dataset sem regras.
	TTLRules []RegraTTLStats `json:"ttlRules,omitempty"`
	// TTLUnmatched são as escritas que não casaram com regra nenhuma — só
	// possível num dataset sem regra de fundo.
	TTLUnmatched int64 `json:"ttlUnmatched"`
}

// RegraTTLStats é uma regra de expiração e quantas escritas ela decidiu.
//
// O `when` viaja junto da contagem porque o índice sozinho obrigaria quem lê o
// painel a abrir o YAML para saber o que casou.
type RegraTTLStats struct {
	Index            int    `json:"index"`
	When             string `json:"when,omitempty"`
	ExpireAfterWrite string `json:"expireAfterWrite"`
	Matched          int64  `json:"matched"`
}

// SchemaStats é o crescimento do dicionário de campos contra o teto.
//
// Fica FORA de IngestaoStats de propósito: aquele bloco só é preenchido quando
// o dataset tem consumidor Kafka, e o dicionário cresce por qualquer rota de
// escrita — inclusive o `nishidb ingest` de arquivo. Um dataset ingerido por
// arquivo reportaria zero campos, que é pior do que não reportar.
//
// FieldsDropped só sobe sob onMaxCols ignore, e é o número que transforma a
// perda em fato contável: sem ele o campo excedente sumiria sem sintoma.
type SchemaStats struct {
	Fields        int    `json:"fields"`
	FieldsDropped uint64 `json:"fieldsDropped"`
	// NestedTypeMismatch conta os valores de coluna ANINHADA que não couberam
	// no tipo fixado. Separado de FieldsDropped porque a causa e o remédio são
	// outros: o teto se resolve subindo o número, e isto é a origem tendo
	// mudado de forma.
	//
	// O evento entra; o que fica incompleto é a coluna naquela linha, e o
	// documento preserva o valor. Ele NÃO aparece no `descartados` da
	// ingestão, que conta EVENTOS.
	NestedTypeMismatch uint64 `json:"nestedTypeMismatch"`
	MaxCols            int    `json:"maxCols"`
	OnMaxCols          string `json:"onMaxCols"`
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
// State usa o vocabulário do §94 — as constantes EstadoUP, EstadoDown,
// EstadoDegraded e EstadoDesligado deste mesmo pacote, que são as que a Saude
// do /v1/health preenche. Enquanto a Saude vivia dentro do servidor, este
// godoc só podia CITAR as palavras; agora as duas rotas apontam para as mesmas
// constantes, e um cliente que precise distinguir DEGRADED de DOWN não
// redeclara nada. Paused traz o motivo quando a ingestão está parada por
// política de erro (§101) e fica vazio quando tudo corre bem.
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
