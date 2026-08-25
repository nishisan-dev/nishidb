package metrics

// Os nomes das métricas do §92, LITERAIS.
//
// Ficam em constantes por um motivo operacional: dashboard e alerta casam
// string. Um nome digitado à mão em dois lugares diverge no dia em que um
// deles é corrigido, e o sintoma é um painel que fica vazio sem ninguém
// perceber — a métrica existe, com o outro nome.
//
// A lista é a transcrição do §92, na ordem em que a spec a escreve. O que o
// M2 ainda não alimenta está marcado; nenhum nome foi inventado aqui, com a
// exceção declarada de MetKafkaErrorsTotal, MetKafkaQueueDepth,
// MetKafkaQueueBytes, MetIngestFailedTotal e MetIngestBlocked — cinco séries
// que o M2 precisa e a spec não previu (ver o plano do M2, D8, D13 e D16) —,
// e, a partir da Fase 11, de MetAuthFailuresTotal e MetTLSCertExpiryTimestamp:
// o protocolo remoto ainda não tinha autenticação nem TLS quando o §92 foi
// escrito. As duas exceções da Fase 11 têm a razão comentada junto à própria
// constante, mais abaixo, em vez de repetida aqui.
const (
	// Processo e runtime (§79, §92).
	MetProcessRSSBytes = "nishidb_process_rss_bytes"
	MetGoHeapBytes     = "nishidb_go_heap_bytes"
	MetGoHeapObjects   = "nishidb_go_heap_objects"

	// Ingestão (§92).
	MetIngestEventsTotal  = "nishidb_ingest_events_total"
	MetIngestEventsRate   = "nishidb_ingest_events_rate"
	MetIngestStaleTotal   = "nishidb_ingest_stale_total"
	MetIngestInvalidTotal = "nishidb_ingest_invalid_total"

	// Kafka (§92).
	MetKafkaLag    = "nishidb_kafka_lag"
	MetKafkaOffset = "nishidb_kafka_offset"

	// Materialização (§92).
	MetDatasetFields        = "nishidb_dataset_fields"
	MetDatasetFieldsDropped = "nishidb_dataset_fields_dropped_total"

	MetMaterializedRows       = "nishidb_materialized_rows"
	MetMaterializedTombstones = "nishidb_materialized_tombstones"

	// Storage (§92).
	MetStorageBytes        = "nishidb_storage_bytes"
	MetStorageLiveBytes    = "nishidb_storage_live_bytes"
	MetStorageGarbageBytes = "nishidb_storage_garbage_bytes"
	MetSegmentsTotal       = "nishidb_segments_total"

	// Consulta (§92).
	MetQueryTotal          = "nishidb_query_total"
	MetQueryActive         = "nishidb_query_active"
	MetQueryErrors         = "nishidb_query_errors"
	MetQueryFullScans      = "nishidb_query_full_scans"
	MetQueryLatencySeconds = "nishidb_query_latency_seconds"

	// Compaction (§92). Alimentadas desde o M4 pelo Store.Stats(): o medidor
	// diz se o laço do §53-55 está de pé, e os dois contadores somam o que as
	// varreduras leram e o copy-forward escreveu.
	MetCompactionRunning      = "nishidb_compaction_running"
	MetCompactionBytesRead    = "nishidb_compaction_bytes_read"
	MetCompactionBytesWritten = "nishidb_compaction_bytes_written"

	// Séries do coletor do idle timeout. O §92 não as previu — ele foi
	// escrito antes de a expiração existir —, então o nome carrega a razão.
	//
	// MetExpiryRunning é o 0/1 do coletor, espelhando o da compaction: sem
	// ele, "o disco não está sendo devolvido" é indistinguível de "não há
	// nada para devolver".
	MetExpiryRunning = "nishidb_expiry_running"
	// MetExpiredTotal conta entidades que ganharam lápide, pelas DUAS rotas
	// (o coletor e a descoberta oportunista da compaction).
	MetExpiredTotal = "nishidb_expired_total"
	// MetExpiryGraceTotal conta carências dadas a entidades sem instante de
	// escrita. É a série que responde "o coletor está trabalhando?" nos
	// primeiros minutos depois de ligar a expiração num dataset já
	// materializado — quando ainda não há nada a expirar e ela é a ÚNICA
	// coisa acontecendo. Sem ela, esse período é indistinguível de um coletor
	// que não subiu, que foi exatamente a confusão vivida ao ligar no piloto.
	MetExpiryGraceTotal = "nishidb_expiry_grace_total"
	// MetExpiryScannedTotal conta entradas do índice examinadas. Comparada com
	// o total de rows, diz se o cursor está dando a volta ou empacando.
	MetExpiryScannedTotal = "nishidb_expiry_scanned_total"

	// Checkpoint (§92).
	MetCheckpointDurationSeconds     = "nishidb_checkpoint_duration_seconds"
	MetCheckpointLastSuccessTimestmp = "nishidb_checkpoint_last_success_timestamp"

	// Autenticação (§74-77, Fase 11, Task B3). Não está no §92 — a exposição
	// remota não tinha autenticação quando aquela seção foi escrita.
	MetAuthFailuresTotal = "nishidb_auth_failures_total"

	// MetTLSCertExpiryTimestamp (§77-78, Fase 11, Task B4). Não está no §92
	// pela mesma razão de MetAuthFailuresTotal — o protocolo remoto não tinha
	// TLS quando aquela seção foi escrita. Epoch segundos do NotAfter do leaf
	// de server.tls, publicado sempre que TLS está ligado; o operador deriva
	// "quanto falta" no painel/alerta a partir dela, em vez de esta métrica
	// tentar decidir o que é "pouco tempo" (isso já é o WARN estruturado
	// tls.certificado_expirando, em internal/server/seguranca.go).
	MetTLSCertExpiryTimestamp = "nishidb_tls_cert_expiry_timestamp"
)

// Séries INVENTADAS no M2, com o defeito que cada uma corrige.
const (
	// MetIngestFailedTotal separa "não entrou por falha" de "não entrou por
	// ser evento ruim". O §92 só prevê nishidb_ingest_invalid_total, que é o
	// balde do evento ruim; sem esta, uma perda de dado por falha do motor
	// fica invisível num daemon, onde não existe exit code para carregá-la
	// (o plano do M2, D13).
	MetIngestFailedTotal = "nishidb_ingest_failed_total"

	// MetIngestBlocked é 1 quando a ingestão do dataset está PARADA por
	// política de erro. Sem ela, a pausa do `invalidEvent: stop` seria um
	// consumer silenciosamente parado (D6).
	MetIngestBlocked = "nishidb_ingest_blocked"

	// MetKafkaErrorsTotal é o balde de erro de BROKER/rede, que o §101 não
	// cataloga. Existe separado porque erro de rede contado como perda de
	// dado é diagnóstico errado (D8).
	MetKafkaErrorsTotal = "nishidb_kafka_errors_total"

	// MetKafkaQueueDepth e MetKafkaQueueBytes expõem os dois gates da fila
	// limitada. O §79 exige "Kafka buffers" entre os itens de memória, e
	// contar registros não é unidade de memória (D16).
	MetKafkaQueueDepth = "nishidb_kafka_queue_depth"
	MetKafkaQueueBytes = "nishidb_kafka_queue_bytes"

	// MetKafkaPartitionsAssigned contra MetKafkaPartitionsTotal é o que
	// torna visível o assignment parcial — duas instâncias no mesmo group
	// dividindo partições, cada dataset materializando metade (D10).
	MetKafkaPartitionsAssigned = "nishidb_kafka_partitions_assigned"
	MetKafkaPartitionsTotal    = "nishidb_kafka_partitions_total"
)

// Nomes de label do produto. Um label a mais multiplica séries por
// cardinalidade, e cada um aqui existe por uma razão registrada: LabelCategoria
// separa nishidb_query_errors por categoria de falha de consulta;
// LabelMotivo (Task A5, issue #15) separa nishidb_ingest_invalid_total pela
// categoria §101 do evento ruim — sem ele, EVENT_TOO_LARGE (a cauda de chave
// que estoura MaxKeySize) ficava indistinguível das outras cinco categorias
// dentro do mesmo total.
const (
	LabelDataset   = "dataset"
	LabelPartition = "partition"
	LabelCategoria = "categoria"
	LabelMotivo    = "motivo"

	// LabelReason é o motivo de UMA falha de autenticação HTTP Basic
	// (Task B3) — enum FECHADO {header, usuario, senha}, nunca o username:
	// mesma doutrina de datasetDesconhecido
	// (internal/server/metricas_query.go), que já recusa alimentar um label
	// com texto de entrada do cliente por ser cardinalidade sem teto. Chamado
	// "reason" e não "motivo" (o nome que LabelMotivo já ocupa, com outro
	// propósito) porque é a grafia que o plano da Fase 11 fixa para
	// nishidb_auth_failures_total.
	LabelReason = "reason"
)
