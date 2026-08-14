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
// única exceção declarada de MetKafkaErrorsTotal, MetKafkaQueueDepth,
// MetKafkaQueueBytes, MetIngestFailedTotal e MetIngestBlocked — cinco séries
// que o M2 precisa e a spec não previu (ver o plano do M2, D8, D13 e D16).
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

	// Compaction (§92) — sem alimentador até o M4.
	MetCompactionRunning      = "nishidb_compaction_running"
	MetCompactionBytesRead    = "nishidb_compaction_bytes_read"
	MetCompactionBytesWritten = "nishidb_compaction_bytes_written"

	// Checkpoint (§92).
	MetCheckpointDurationSeconds     = "nishidb_checkpoint_duration_seconds"
	MetCheckpointLastSuccessTimestmp = "nishidb_checkpoint_last_success_timestamp"
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

// LabelDataset e LabelPartition são os únicos nomes de label do produto. Um
// label a mais multiplica séries por cardinalidade, e a spec não pede nenhum.
const (
	LabelDataset   = "dataset"
	LabelPartition = "partition"
	LabelCategoria = "categoria"
)
