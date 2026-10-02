package protocol

// Os corpos de GET /v1/metadata e GET /v1/stats.
//
// O §67 nomeia as duas rotas e não define o conteúdo de nenhuma. O que está
// aqui é a forma que o DatabaseMetaData e o ResultSetMetaData do §71 vão
// consumir na Fase 7 — decidida agora, olhando para esse consumidor, porque um
// formato mínimo hoje viraria um SEGUNDO formato depois.

// Estado de um subsistema no /v1/health (§94).
//
// As constantes vivem AQUI, e não no servidor, pelo mesmo motivo que a Health:
// são o vocabulário que o corpo da rota usa, e um cliente que precise
// distinguir DEGRADED de DOWN teria de redeclará-las. É o vocabulário que
// IngestionStats.State também usa — as duas rotas descrevem a mesma realidade.
const (
	StateUp       = "UP"
	StateDown     = "DOWN"
	StateDegraded = "DEGRADED"
	StateDisabled = "DISABLED"
)

// Health é o corpo de GET /v1/health (§94).
//
// É o único corpo de rota do §67 que vivia dentro do servidor, e por isso o
// único que um cliente teria de redeclarar para consumir. O CLI remoto e o
// driver JDBC falam o mesmo protocolo: o contrato inteiro mora neste pacote.
type Health struct {
	Status   string            `json:"status"`
	Kafka    string            `json:"kafka"`
	Storage  string            `json:"storage"`
	Datasets map[string]string `json:"datasets"`
	// Details nomeia o problema por dataset. Não está no §94 e existe
	// porque "DOWN" sozinho manda o operador procurar no log de qual dos
	// datasets, num processo que pode hospedar vários (§82).
	Details map[string]string `json:"detalhes,omitempty"`
}

// Metadata é o corpo de GET /v1/metadata. Sem filtro traz todos os datasets;
// com ?dataset=<nome> traz só um, na mesma forma — o cliente não precisa
// aprender dois formatos para as duas perguntas.
type Metadata struct {
	Datasets []DatasetMeta `json:"datasets"`
}

// DatasetMeta descreve um dataset consultável.
//
// MaxRows e TimeoutMs são os valores EFETIVOS (o do dataset, ou o default
// global que ele herdou), e não o que está escrito no YAML dele: quem consulta
// precisa saber o teto que vai valer, não onde ele foi declarado.
//
// Eles governam a consulta que LÊ este dataset, e não a que ENTRA por ele: o
// servidor resolve o dono do `FROM <schema>.<tabela>` antes de admitir a
// consulta e aplica os limites dele (query.Engine.TargetDataset). Numa conexão
// que abre uma tabela de outro schema, portanto, quem manda são os números
// deste documento na entrada daquele OUTRO dataset — e é o par Schema/Tables
// abaixo que diz qual é. O driver JDBC ainda clampa pelos números do dataset da
// conexão; ver o débito 118.
type DatasetMeta struct {
	Name string `json:"name"`
	// Schema é o `schemaName` declarado no YAML: o container em que TODAS as
	// tabelas deste dataset aparecem na árvore de um cliente SQL, e o
	// qualificador que o `FROM <schema>.<tabela>` aceita. Campo ADITIVO.
	//
	// Ele não é derivável do Name nem o identifica sozinho: a carga da
	// configuração recusa dois DATASETS com o mesmo schemaName
	// (config.ValidateSchemaUniqueness), mas a tabela irmã do histórico HERDA
	// o schema do base (schema.DeriveHistory) e sai neste documento como um
	// dataset próprio — dois elementos de `datasets` com o mesmo `schema` são
	// o par base/irmã, e não colisão.
	Schema  string `json:"schema"`
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
	// Não há campo equivalente na Request, ao contrário do maxRows: pedir
	// MENOS colunas não é uma pergunta que o cliente precise fazer, porque
	// ele já escolhe as colunas escrevendo-as. Um teto de cliente aqui só
	// poderia transformar uma consulta que funciona em erro.
	MaxNestedCols int `json:"maxNestedCols"`
	// Columns são as colunas da tabela BASE, e continuam sendo exatamente isso
	// — é o campo que o driver e o client Python já leem. Elas são as MESMAS
	// que saem em Tables, na linha de Kind "TABLE": as duas listas são
	// preenchidas do mesmo elemento do catálogo, não montadas duas vezes.
	Columns []ColumnMeta `json:"columns"`
	// Tables são TODAS as tabelas consultáveis deste dataset, a base inclusive:
	// os contadores e as tabelas do `_meta` só apareciam, até aqui, por SQL
	// (`_meta.tables`), e nenhuma ferramenta que monta árvore fala SQL de
	// catálogo. Campo ADITIVO — um cliente que não o conhece continua lendo
	// Columns como antes.
	//
	// A ordem é a do query.Engine.Catalog, que é contrato: a base primeiro,
	// depois os contadores e por último as tabelas do `_meta`, cada grupo por
	// nome. Sem ela, uma ferramenta que agrupa a árvore pela sequência das
	// linhas mostraria as pastas embaralhadas a cada reconexão.
	Tables []TableMeta `json:"tables"`
}

// TableMeta descreve UMA tabela consultável de um dataset. É o espelho no wire
// do query.TableInfo, e sai do MESMO Catalog que alimenta a `_meta.tables` —
// duas derivações da mesma enumeração divergiriam, e a divergência apareceria
// como uma ferramenta que discorda do SQL sem nada estar quebrado.
//
// O SIGNIFICADO dos campos mora no godoc de query.TableInfo — e o dos dois
// valores de Kind no de query.TableKind —, e mora lá SÓ: por que o Schema de
// uma linha do `_meta` não é o do dataset, por que o Namespace não é
// reconstruível a partir de (Schema, Name), por que não existe "SYSTEM TABLE" e
// por que Columns é deliberadamente um superset do que o `SELECT *` devolve.
// Repetir a explicação aqui daria dois textos normativos
// para o mesmo fato, e eles divergiriam no primeiro que alguém atualizasse.
// Abaixo ficam as chaves do JSON e o que é específico do wire.
type TableMeta struct {
	// Name é o nome da tabela dentro do Schema.
	Name string `json:"name"`
	// Schema é onde a tabela mora, e é o que um cliente JDBC publica como
	// TABLE_SCHEM. Ele viaja na LINHA em vez de ser herdado do DatasetMeta
	// justamente porque as linhas do `_meta` NÃO estão no schema do dataset.
	Schema string `json:"schema"`
	// Namespace é o espaço de nome reservado por onde o ATALHO abre a tabela
	// (`_counters`, `_meta`), e sai VAZIO para a base.
	Namespace string `json:"namespace"`
	// Kind é "TABLE" para a base e "VIEW" para o contador e para as tabelas do
	// `_meta` — os dois valores de query.TableKind, no wire como texto.
	Kind string `json:"kind"`
	// Description é o texto que o cliente mostra ao lado da tabela, e sai VAZIA
	// em toda linha hoje. O campo é string, e não ponteiro como o Alias de
	// ColumnMeta: a MESMA ausência que a `_meta.tables` publica como NULL sai
	// aqui como `""`, e um cliente que espere `null` no JSON está lendo a regra
	// da tabela SQL no lugar errado.
	Description string `json:"description"`
	// Columns são as colunas descritíveis da tabela, pela MESMA regra do campo
	// Columns do dataset (schema.Compiled.FieldsForMetadata), cada uma já com o
	// código JDBC calculado pelo servidor.
	Columns []ColumnMeta `json:"columns"`
}

// ColumnMeta descreve uma coluna do dataset.
//
// Index diz se a coluna tem índice: "bitmap" quando o YAML declara
// `index: true`, "none" quando não. Não é promessa de plano — é metadado de
// schema, e quem quiser saber o plano escolhido olha nishidb_query_full_scans.
//
// Eram TRÊS valores possíveis. O "hash" saiu porque existe um motor de índice
// só: nem a config nem o schema compilado sabem mais produzi-lo. As outras duas
// palavras ficaram como estavam — o driver JDBC decide por igualdade com "none"
// se a coluna entra no getIndexInfo, e o IT do client Python afirma "bitmap".
//
// Auto marca o campo sem tipo declarado. Quando ele ainda não foi observado,
// Type é "unknown" e JDBC é "OTHER" — a única resposta honesta, já que o
// próximo evento pode fixá-lo como qualquer coisa.
//
// Os quatro campos de tipo (Type, JDBC, Precision, Scale, DisplaySize) saem do
// MESMO DescriptorOf que a linha de schema de /v1/query usa, e
// TestMetadataPublicaOsMesmosNumerosQueALinhaDeSchema cobra que as duas rotas
// descrevam a mesma coluna do mesmo jeito. Os dois pontos de serialização
// ficam em arquivos diferentes: sem esse teste, acrescentar um campo em um e
// esquecer o outro passaria.
type ColumnMeta struct {
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

// Stats é o corpo de GET /v1/stats.
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
type Stats struct {
	Process  ProcessStats   `json:"process"`
	Datasets []DatasetStats `json:"datasets"`
}

// ProcessStats agrupa o que é do processo, não de um dataset.
type ProcessStats struct {
	// Version é o texto de internal/buildinfo.String() — a MESMA função que
	// o `nishidb --version`, o log de subida, o painel e o benchmark usam.
	// Campo ADITIVO: um cliente que ainda não o conhece continua lendo o
	// resto de Stats sem quebrar. É esta a versão que a Task A4 lê
	// depois de um rollout, para confirmar que o binário no ar é o novo.
	Version string      `json:"version"`
	Query   EngineStats `json:"query"`
	// InFlight são as consultas correndo NESTE instante, da mais antiga para a
	// mais nova. Campo ADITIVO.
	//
	// Vive no bloco do PROCESSO, e não dentro de cada dataset, porque a
	// pergunta que ele responde é do pool: "quem está segurando as vagas?".
	// Cada entrada nomeia o dataset dela, então quem quiser filtrar filtra —
	// mas quebrar a lista por dataset esconderia o que interessa, que é a
	// consulta mais antiga do processo INTEIRO.
	InFlight []InFlightQuery `json:"inFlight"`
}

// InFlightQuery é uma consulta em andamento.
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
type InFlightQuery struct {
	Dataset   string `json:"dataset"`
	SQLHash   string `json:"sqlHash"`
	RunningMs int64  `json:"runningMs"`
	Long      bool   `json:"long"`
}

// EngineStats são os contadores do motor de consulta (executor e planner).
//
// FullScans é a métrica NORMATIVA do §64, contada no plan-time; os demais são
// diagnóstico. Rejected > 0 é a prova de que a revalidação do §44 está fazendo
// trabalho de verdade — o índice devolve um superset e o executor descarta o
// que não casa o predicado corrente.
//
// BitmapScans conserva o nome de wire depois de o motor bitmap deixar de
// existir, e a fonte dele passou a ser o contador de plano IndexScan. O NÚMERO
// é o mesmo — nenhuma config produzia plano de hash desde a D5, então não havia
// segunda parcela a somar —, e é o número que o `nishidb status` e qualquer
// coletor do operador leem. Renomear a chave JSON quebraria os dois sem
// entregar informação nova.
type EngineStats struct {
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
	SeriesDelivery []SeriesDeliveryStats `json:"seriesDelivery,omitempty"`
	// AlarmDelivery é omitido quando não existem rotas nem filas antigas.
	AlarmDelivery []AlarmDeliveryStats `json:"alarmDelivery,omitempty"`
	Name          string               `json:"name"`
	Storage       StorageStats         `json:"storage"`
	Schema        SchemaStats          `json:"schema"`
	Query         RouteStats           `json:"query"`
	Ingestion     IngestionStats       `json:"ingestion"`
	// Materialization são os efeitos das políticas de escrita e das regras de
	// TTL. Fica FORA de Ingestion pela mesma razão do SchemaStats: aquele
	// bloco só é preenchido quando há consumidor Kafka, e estes números valem
	// para qualquer rota de escrita.
	Materialization MaterializationStats `json:"materialization"`
	// History descreve a tabela irmã append-only, e é preenchido no DATASET
	// BASE — não na irmã. É lá que o operador procura: quem declarou
	// `history:` num YAML quer saber o que aquele bloco está fazendo, e a
	// irmã aparece na lista com os próprios números de storage e query como
	// qualquer outro dataset.
	History *HistoryStats `json:"history,omitempty"`
	// Index é a mecânica do índice por segmento DESTE dataset, e sai em todos
	// eles — inclusive na irmã, ao contrário do bloco History acima.
	//
	// A assimetria é o desenho: `history` descreve uma POLÍTICA, declarada no
	// YAML do base, e por isso é publicada onde ela foi escrita; `index` descreve
	// o motor de índice de um Store, que existe nos dois modos de dataset
	// (decisão D1) e tem números próprios em cada um.
	//
	// Não é ponteiro porque não há dataset sem a resposta: um sem campo indexado
	// tem os quatro estados em zero e a reconstrução desligada, e isso é
	// informação — bloco omitido seria indistinguível de versão antiga do
	// servidor.
	Index IndexStats `json:"index"`
	// Snapshot é o que o isolamento de leitura está segurando neste dataset,
	// e sai da MESMA estrutura que alimenta as séries nishidb_query_snapshot_*
	// — dois contadores para o mesmo fato divergem no dia em que um caminho de
	// erro esquece de somar, e em produção alguém lê o errado.
	//
	// Não é ponteiro pela mesma razão do Index: um dataset sem consulta aberta
	// tem os quatro campos em zero, e isso é informação. Bloco omitido seria
	// indistinguível de versão antiga do servidor.
	Snapshot SnapshotStats `json:"snapshot"`
	// Counters são os contadores mantidos na escrita, na ordem declarada, da
	// MESMA leitura que alimenta as séries nishidb_counter_*. Lista vazia — e
	// não omitida — no dataset sem `counters:`, pela razão do Index.
	Counters []CounterStats `json:"counters"`
}

// SeriesDeliveryStats describes a counter's best-effort Kafka output.
type SeriesDeliveryStats struct {
	Counter              string            `json:"counter"`
	Destination          string            `json:"destination"`
	Delivered            uint64            `json:"delivered"`
	Dropped              map[string]uint64 `json:"dropped"`
	PendingRecords       int64             `json:"pendingRecords"`
	PendingBytes         int64             `json:"pendingBytes"`
	LastSuccessTimestamp float64           `json:"lastSuccessTimestamp"`
	// AdmissionWaitSeconds acumula, desde a subida, o tempo que a emissão
	// deste contador esperou vaga no destino (contrapressão). Zero sem espera.
	AdmissionWaitSeconds float64 `json:"admissionWaitSeconds"`
}

// CounterStats é o retrato de UM contador mantido na escrita
// (planning/contadores-mantidos-na-escrita.md §11.5).
//
// AuditDivergentGroups conta as divergências que a última auditoria achou:
// grupos, linhas cujo grupo não está internado e os dois agregados.
//
// Refused é o número que alarma: um grupo novo recusado pelo teto é uma linha
// que entrou e não é contada. OrphanEntries é o que a dobra da abertura achou
// apontando para grupo que já saiu. AuditDivergentGroups e
// AuditLastSuccessUnix são a última auditoria conclusiva — zero e zero até a
// primeira, e é o timestamp que separa "nunca auditado" de "auditado e certo".
type CounterStats struct {
	TotalDisabled bool   `json:"-"`
	Name          string `json:"name"`
	// Rows e Total são os agregados do contador inteiro: quantas linhas vivas
	// apontam para algum grupo (os membros), e quantas satisfazem a condição no
	// dataset inteiro. Rows abaixo do rows do dataset são as linhas SEM alguma
	// coluna do groupBy e, com `scope`, as fora do escopo — o contador não
	// conta nenhuma das duas, por desenho.
	Rows                 int64  `json:"rows"`
	Total                int64  `json:"total"`
	Groups               uint32 `json:"groups"`
	MaxGroups            uint32 `json:"maxGroups"`
	Retired              uint32 `json:"retired"`
	Refused              uint64 `json:"refused"`
	OrphanEntries        uint64 `json:"orphanEntries"`
	Bytes                int64  `json:"bytes"`
	AuditDivergentGroups int64  `json:"auditDivergentGroups"`
	AuditLastSuccessUnix int64  `json:"auditLastSuccessUnix"`
}

// SnapshotStats é o retrato do isolamento de leitura: o que as consultas
// abertas estão impedindo o dataset de descartar.
//
// Os quatro números respondem a perguntas diferentes, e é por isso que são
// quatro. Bytes diz quanto do teto está em uso AGORA; OldestSeconds diz há
// quanto tempo o passado está retido, que é o que trava a compaction e a
// retenção; PinnedSegments diz o efeito disso no disco; e ExpiredTotal conta
// as consultas que o teto já recusou — a única que alarma, porque enquanto for
// zero o teto está sobrando.
type SnapshotStats struct {
	Bytes          int64   `json:"bytes"`
	OldestSeconds  float64 `json:"oldestSeconds"`
	PinnedSegments int     `json:"pinnedSegments"`
	ExpiredTotal   uint64  `json:"expiredTotal"`
}

// IndexStats é o retrato do índice por segmento, e é a MESMA estrutura que
// alimenta as séries nishidb_index_*.
//
// A janela efetiva NÃO está aqui: ela é política do histórico e continua no
// bloco History, como EffectiveIndexWindow. Ver o comentário da família em
// internal/metrics/names.go.
type IndexStats struct {
	// Os quatro estados PARTICIONAM os segmentos do dataset. Pending e
	// OutOfWindow são duas ausências de significado OPOSTO, e é por isso que são
	// dois campos: OutOfWindow é o segmento que a janela deliberadamente não
	// cobre — num dataset grande, a maioria esmagadora, e o sistema funcionando
	// como projetado —, enquanto Pending é trabalho na fila. Somados, o número é
	// enorme, normal e não responde "o laço está avançando?".
	Sealed      int64 `json:"sealed"`
	Current     int64 `json:"current"`
	Pending     int64 `json:"pending"`
	OutOfWindow int64 `json:"outOfWindow"`
	// Bytes são os blocos do índice por segmento DESTE dataset. Separado porque é
	// este número que precisa parar de crescer com a tabela em regime.
	//
	// Ele é parcela estrita do History.IndexBytes — que soma também indexes/ e
	// checkpoint/ — mas os dois NÃO ficam no mesmo lugar, e a diferença é fácil de
	// ler errado: o bloco History é preenchido no dataset BASE com os números da
	// IRMÃ, enquanto este bloco descreve o Store do dataset em que sai. Num par
	// `X` / `X_history`, a parcela mora em `X_history`.Index.Bytes e o total em
	// `X`.History.IndexBytes. No base, Index.Bytes é o índice do PRÓPRIO base, que
	// não tem relação nenhuma com o total publicado ao lado dele.
	Bytes int64 `json:"bytes"`
	// Rebuilds conta os segmentos cujo índice a reconstrução publicou e
	// RebuildRunning é o 0/1 do laço, que para de vez na primeira falha.
	// Contador de PROCESSO: zera no restart.
	Rebuilds       int64 `json:"rebuilds"`
	RebuildRunning bool  `json:"rebuildRunning"`
	// CRCFailures conta os índices selados reprovados no crc32c. É DEGRAU e não
	// taxa: o mapeamento reprovado fica em cache, então cada arquivo podre soma uma
	// vez por processo. Nenhum campo deste bloco diz se o defeito CONTINUA de pé: o
	// corrompido conta como Sealed, e o SegmentsByScan que ele soma é
	// indistinguível do de um segmento fora da janela. O sinal disponível é o WARN
	// indice.selado_corrompido no log (débito 103).
	CRCFailures int64 `json:"crcFailures"`
	// SegmentsByIndex e SegmentsByScan contam por qual caminho os Lookups
	// resolveram cada segmento. São o único observável da degradação da §3.4 — o
	// resultado é o mesmo pelos dois caminhos, e só o custo muda.
	SegmentsByIndex int64 `json:"segmentsByIndex"`
	SegmentsByScan  int64 `json:"segmentsByScan"`
}

// HistoryStats é o que o bloco `history` está fazendo.
//
// Os três primeiros campos respondem a única pergunta que importa numa tabela
// cuja razão de existir é servir de evidência: ela está COMPLETA? Linhas e
// descartadas somam os eventos que passaram; Degraded diz se ela parou de
// receber.
type HistoryStats struct {
	// Table é o nome da tabela irmã — o mesmo que vai no FROM.
	Table string `json:"table"`
	// Rows são as linhas gravadas e Dropped os eventos que não viraram linha
	// nenhuma. Contadores de PROCESSO: zeram no restart, como os de densidade.
	Rows    int64 `json:"rows"`
	Dropped int64 `json:"dropped"`
	// Degraded diz que a tabela entrou em modo falho sticky e parou de
	// receber. A consulta a ela continua respondendo, sobre o que já gravou.
	Degraded bool `json:"degraded"`
	// DataBytes é a soma dos SEGMENTOS, que é o que o maxBytes governa.
	// IndexBytes é todo o índice do dataset — indexes/, checkpoint/ e o índice
	// POR SEGMENTO, que mora em data/ ao lado do .dat.
	//
	// Publicado em separado porque o teto declarado governa DADO e o índice tem
	// teto próprio; somá-lo ao teto mudaria o significado de uma chave de
	// configuração. A purga devolve os DOIS — o .sidx sai junto com o .dat —,
	// então este número CAI depois de uma passada, ao contrário do que valia
	// quando todo índice era global.
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
	// IndexWindow ecoa o `history.index.window` declarado e
	// EffectiveIndexWindow é o intervalo REALMENTE coberto pelos segmentos
	// indexados, em segundos. O par existe pela mesma razão do par acima: é a
	// comparação entre os dois que diz se o teto do índice
	// (`history.index.maxBytes`) assumiu o comando — do mesmo jeito que
	// OldestWrittenAt mostra o maxBytes engolindo o expireAfterWrite.
	//
	// Os dois são omitidos por razões DIFERENTES, e a assimetria é deliberada:
	// IndexWindow some quando o bloco `history.index` não declarou `window` (não
	// há intenção a ecoar), e EffectiveIndexWindow some quando não há cobertura
	// nenhuma a reportar — tabela vazia ou sem campo indexado. A cobertura é
	// publicada MESMO sem janela declarada: ela continua sendo o que o índice
	// alcança, e é o único jeito de o operador descobrir que declarar uma janela
	// não mudaria nada.
	IndexWindow          string  `json:"indexWindow,omitempty"`
	EffectiveIndexWindow float64 `json:"effectiveIndexWindowSeconds,omitempty"`
}

// MaterializationStats conta o que o dado gravado NÃO diz.
//
// Um valor congelado é indistinguível de um que nunca mudou; um total somado é
// indistinguível de um total que a origem mandou pronto; e uma regra de TTL
// que nunca casa parece igual a uma que casa sempre. Estes contadores são a
// única forma de o operador ver as políticas trabalhando.
type MaterializationStats struct {
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
	TTLRules []TTLRuleStats `json:"ttlRules,omitempty"`
	// TTLUnmatched são as escritas que não casaram com regra nenhuma — só
	// possível num dataset sem regra de fundo.
	TTLUnmatched int64 `json:"ttlUnmatched"`
	// TTLAnchorInFuture conta as âncoras posteriores ao instante da escrita,
	// limitadas a ele, e TTLExpiredOnWrite os records gravados já vencidos
	// (âncora + expireAfter no passado). Zero num dataset sem regra ancorada.
	TTLAnchorInFuture int64 `json:"ttlAnchorInFuture"`
	TTLExpiredOnWrite int64 `json:"ttlExpiredOnWrite"`
}

// TTLRuleStats é uma regra de expiração e quantas escritas ela decidiu.
//
// O `when` viaja junto da contagem porque o índice sozinho obrigaria quem lê o
// painel a abrir o YAML para saber o que casou.
//
// Numa regra ancorada, ExpireAfterWrite sai VAZIO e a duração vem em
// ExpireAfter, ao lado de Anchor: publicar 48h sob `expireAfterWrite` diria
// que a escrita renova o prazo, que é exatamente o que a âncora impede.
// AnchorAbsent só existe na regra ancorada — ponteiro para sair inclusive com
// zero nela e sumir nas outras, que assim saem idênticas ao que eram.
type TTLRuleStats struct {
	Index            int    `json:"index"`
	When             string `json:"when,omitempty"`
	ExpireAfterWrite string `json:"expireAfterWrite"`
	Matched          int64  `json:"matched"`
	Anchor           string `json:"anchor,omitempty"`
	ExpireAfter      string `json:"expireAfter,omitempty"`
	AnchorAbsent     *int64 `json:"anchorAbsent,omitempty"`
}

// SchemaStats é o crescimento do dicionário de campos contra o teto.
//
// Fica FORA de IngestionStats de propósito: aquele bloco só é preenchido quando
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

// RouteStats são os contadores do §92 que o protocolo alimenta, por dataset.
//
// A latência NÃO aparece aqui de propósito: ela é um histograma, e um
// histograma serializado em JSON é pior que inútil — o formato certo para ele é
// o /metrics, onde os buckets cumulativos têm quem os leia.
type RouteStats struct {
	Total  uint64 `json:"total"`
	Active int64  `json:"active"`
	Errors uint64 `json:"errors"`
}

// IngestionStats é o estado da ingestão Kafka do dataset.
//
// State usa o vocabulário do §94 — as constantes StateUp, StateDown,
// StateDegraded e StateDisabled deste mesmo pacote, que são as que a Health
// do /v1/health preenche. Enquanto a Health vivia dentro do servidor, este
// godoc só podia CITAR as palavras; agora as duas rotas apontam para as mesmas
// constantes, e um cliente que precise distinguir DEGRADED de DOWN não
// redeclara nada. Paused traz o motivo quando a ingestão está parada por
// política de erro (§101) e fica vazio quando tudo corre bem.
type IngestionStats struct {
	Enabled   bool   `json:"enabled"`
	State     string `json:"state"`
	Events    int64  `json:"events"`
	Applied   int64  `json:"applied"`
	Stale     int64  `json:"stale"`
	Discarded int64  `json:"discarded"`
	Failed    int64  `json:"failed"`
	Paused    string `json:"paused,omitempty"`
}

// AlarmDeliveryStats descreve uma fila de webhook, sem credenciais nem URL.
type AlarmDeliveryStats struct {
	Destination          string            `json:"destination"`
	State                string            `json:"state"`
	MemoryEvents         int               `json:"memoryEvents"`
	MemoryBytes          int64             `json:"memoryBytes"`
	DiskEvents           int               `json:"diskEvents"`
	DiskBytes            int64             `json:"diskBytes"`
	OldestPendingSeconds float64           `json:"oldestPendingSeconds"`
	Delivered            uint64            `json:"delivered"`
	FailedAttempts       uint64            `json:"failedAttempts"`
	PersistenceErrors    uint64            `json:"persistenceErrors"`
	Dropped              map[string]uint64 `json:"dropped"`
	LastSuccessTimestamp float64           `json:"lastSuccessTimestamp"`
	// AdmissionWaitSeconds acumula, desde a subida, o tempo que a varredura
	// esperou vaga em memória porque o gravador não acompanhou a rajada.
	AdmissionWaitSeconds float64 `json:"admissionWaitSeconds"`
}
