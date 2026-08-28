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
	MetIngestEventsTotal = "nishidb_ingest_events_total"
	// MetIngestEventsRate NÃO TEM ALIMENTADOR, e isso é decisão, não esquecimento.
	//
	// Taxa derivada dentro do exportador é um número cuja JANELA o operador não
	// escolheu, e que discordaria do contador ao lado sempre que a janela dele
	// fosse outra — alguém acabaria lendo o errado. `rate(nishidb_ingest_events_total[5m])`
	// é a mesma pergunta com a janela declarada no alerta, que é onde ela deve
	// ser declarada. Mesma doutrina que já recusa timer em favor do scrape.
	//
	// A omissão é travada por teste; ver internal/server/metricas_dataset_test.go.
	MetIngestEventsRate   = "nishidb_ingest_events_rate"
	MetIngestStaleTotal   = "nishidb_ingest_stale_total"
	MetIngestInvalidTotal = "nishidb_ingest_invalid_total"

	// Kafka (§92).
	//
	// As três saem do MESMO instantâneo, por um coletor só, e é isso que faz
	// `lag == high_watermark - offset` valer dentro de uma raspagem. Emitidas
	// por famílias independentes elas descreveriam instantes diferentes, e o
	// painel do piloto já pagou por isso: nishidb-stats.py derivava a produção
	// de Δ(offset + lag) e imprimiu `produz -310/s`, porque as duas parcelas
	// vinham de leituras distintas.
	MetKafkaLag           = "nishidb_kafka_lag"
	MetKafkaOffset        = "nishidb_kafka_offset"
	MetKafkaHighWatermark = "nishidb_kafka_high_watermark"

	// Os dois contadores de SEGUNDOS ACUMULADOS que separam "saturado" de
	// "sem produção".
	//
	// A profundidade da fila não consegue fazer essa distinção: ela é amostra
	// instantânea e fica em zero nos dois casos — uma fila que oscila cheia e
	// vazia entre dois scrapes é invisível para ela. A integral de tempo não
	// tem esse buraco, e rate() sobre um contador em segundos é diretamente a
	// FRAÇÃO de tempo bloqueado, ou seja, utilização.
	//
	//	rate(poll_blocked)≈1, rate(writer_idle)≈0  → o writer é o gargalo
	//	rate(poll_blocked)≈0, rate(writer_idle)≈1  → o tópico está calado
	//	                       ... com lag subindo → o poller não entrega
	MetKafkaPollBlockedSeconds = "nishidb_kafka_poll_blocked_seconds_total"
	MetKafkaWriterIdleSeconds  = "nishidb_kafka_writer_idle_seconds_total"

	// MetIngestApplySeconds é a latência de UM evento pelo Apply: parse,
	// leitura do estado, merge, Put e o Apenda do histórico.
	//
	// Medida no Writer.aplica, e NÃO dentro do Materializer, de propósito: os
	// gates de alocação e o BenchmarkApplyIngestReal medem o Materializer.Apply,
	// e instrumentar por dentro mudaria o baseline do próprio instrumento com
	// que a otimização vai ser julgada.
	//
	// O checkpoint fica FORA: ele já tem dono em
	// nishidb_checkpoint_duration_seconds, e somá-lo aqui produziria um
	// histograma bimodal cujo p99 é sempre o stall de checkpoint.
	MetIngestApplySeconds = "nishidb_ingest_apply_seconds"

	// Materialização (§92).
	MetDatasetFields        = "nishidb_dataset_fields"
	MetDatasetFieldsDropped = "nishidb_dataset_fields_dropped_total"
	MetNestedTypeMismatch   = "nishidb_dataset_nested_type_mismatch_total"

	MetMaterializedRows = "nishidb_materialized_rows"
	// MetMaterializedTombstones segue SEM ALIMENTADOR, agora como débito
	// nomeado em vez de promessa aberta.
	//
	// Alimentá-la honestamente exige um contador de lápides VIVAS mantido em
	// applyRecord — o único caminho de aplicação —, reconstruível no replay,
	// decrementado na ressurreição e ajustado pela compaction que as descarta.
	// É trabalho de milestone, não encanamento de métrica, e não é
	// pré-requisito de nenhuma otimização de ingestão.
	//
	// planning/tipos-temporais-e-expire-after-write.md prometeu alimentá-la e
	// não o fez; a promessa aberta virou débito com número.
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

	// As séries das POLÍTICAS DE ESCRITA e das regras de TTL. Elas contam o
	// que o dado gravado não diz: um valor congelado é indistinguível de um
	// que nunca mudou, e um total somado é indistinguível de um total que a
	// origem mandou pronto. Sem elas, a política é invisível em produção — e
	// contador sem leitor é o defeito que este projeto trata como tal.
	MetFrozenCaptured  = "nishidb_frozen_captured_total"
	MetFrozenPreserved = "nishidb_frozen_preserved_total"
	MetMergeSum        = "nishidb_merge_sum_total"
	// MetMergeSumSaturated é o único desfecho em que o valor gravado deixa de
	// ser verdade: a soma bateu no teto do int64 e parou ali.
	MetMergeSumSaturated = "nishidb_merge_sum_saturated_total"
	// MetTTLRuleMatched tem o rótulo `rule` com o ÍNDICE da regra no YAML,
	// porque é a ordem que decide qual vence.
	MetTTLRuleMatched = "nishidb_ttl_rule_matched_total"
	MetTTLUnmatched   = "nishidb_ttl_unmatched_total"

	// Séries da tabela histórica, publicadas no rótulo do dataset BASE — é
	// nele que o operador declarou o bloco, e é nele que vai procurar.
	//
	// MetHistoryRows e MetHistoryDropped somam os eventos que passaram, e
	// juntas respondem a única pergunta que importa numa tabela cuja razão de
	// existir é servir de evidência: ela está COMPLETA?
	MetHistoryRows    = "nishidb_history_rows_total"
	MetHistoryDropped = "nishidb_history_dropped_total"
	// MetHistoryDegraded é 1 quando a tabela entrou em modo falho sticky e
	// parou de receber. A consulta a ela continua respondendo — o que sumiu é
	// a escrita, e é isso que precisa de alarme.
	MetHistoryDegraded = "nishidb_history_degraded"
	// MetHistoryDataBytes é o que o maxBytes governa; MetHistoryIndexBytes é
	// o resto do diretório, medido em BLOCOS. Separadas porque a purga só
	// alcança a primeira: somá-las num número só faria o operador dimensionar
	// o teto contra disco que a retenção não consegue baixar.
	MetHistoryDataBytes  = "nishidb_history_data_bytes"
	MetHistoryIndexBytes = "nishidb_history_index_bytes"
	// MetHistoryOldestWrittenAt é a série que se olha primeiro: é ela que
	// mostra o maxBytes engolindo o expireAfterWrite declarado.
	MetHistoryOldestWrittenAt = "nishidb_history_oldest_written_at"
	// MetHistoryPurged tem o rótulo `reason` (ttl|size) porque qual das duas
	// políticas está decidindo é a pergunta, não quantos segmentos saíram.
	MetHistoryPurged = "nishidb_history_segments_purged_total"
	// MetHistoryRetentionRunning responde "o motor está de pé?" — ele para de
	// vez na primeira falha, e sem esta série isso é indistinguível de "não
	// há o que purgar".
	MetHistoryRetentionRunning = "nishidb_history_retention_running"
	// MetExpiryGraceTotal conta carências dadas a entidades sem instante de
	// escrita. É a série que responde "o coletor está trabalhando?" nos
	// primeiros minutos depois de ligar a expiração num dataset já
	// materializado — quando ainda não há nada a expirar e ela é a ÚNICA
	// coisa acontecendo. Sem ela, esse período é indistinguível de um coletor
	// que não subiu, que foi exatamente a confusão vivida ao ligar no piloto.
	MetExpiryGraceTotal = "nishidb_expiry_grace_total"
	// MetExpiryScannedTotal conta entradas do índice de expiração examinadas.
	//
	// A LEITURA MUDOU com a agenda de vencimentos, e o número antigo não é
	// comparável com o novo. Enquanto o coletor varria, esta série media
	// ESFORÇO: crescia com o tamanho do dataset a cada volta do cursor —
	// 24,1 milhões no piloto — e servia para saber se o cursor dava a volta
	// ou empacava. Agora ela conta as entradas da árvore em que a descida
	// entrou, e portanto mede TRABALHO: cresce com o que venceu, não com o
	// que existe.
	//
	// Consequência para quem opera: uma taxa que despenca depois da subida é
	// o resultado esperado, não uma parada do coletor. Quem quiser saber se
	// ele está vivo olha MetExpiryRunning; quem quiser saber se ele está
	// trabalhando compara esta série com as de expiradas e carências.
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

	// MetQueryLongestRunningSeconds e MetQueryLongRejectedTotal são do sub-pool
	// de consulta longa. Não estão no §92, e a razão é a mesma das duas
	// anteriores: aquela seção foi escrita quando `query.timeout` era ao mesmo
	// tempo o default e o teto, então "consulta longa" não era uma categoria
	// que o produto reconhecesse — não havia o que medir.
	//
	// MetQueryLongestRunningSeconds é a idade da consulta em voo há mais tempo.
	// É MEDIDOR e é lida no scrape (ver emVoo.maisAntiga), nunca atualizada na
	// admissão: o número que interessa CRESCE SOZINHO enquanto nada acontece, e
	// um medidor escrito só quando uma consulta começa ou termina ficaria
	// congelado exatamente no estado que ele existe para denunciar — o servidor
	// parado numa varredura que não acaba. Com o §92 sozinho, esse estado é
	// indistinguível de "3 consultas ativas, tudo bem".
	//
	// MetQueryLongRejectedTotal conta as recusas do SUB-POOL, separado de
	// nishidb_query_errors{categoria="TOO_MANY_LONG_QUERIES"} — que também as
	// registra — porque as duas respondem a perguntas diferentes: a série de
	// erros é dimensionada por dataset e serve ao diagnóstico de um deles, e
	// esta é do PROCESSO e serve a uma decisão de capacidade, que é subir
	// runtime.query.maxLongRunning. Sem ela, a pergunta "o sub-pool está
	// apertado?" só teria resposta somando categorias sobre todos os datasets,
	// que é conta que ninguém escreve num alerta.
	//
	// O débito 61 é o custo de publicar nome fora do §92 sem razão registrada;
	// esta é a razão, e ela mora aqui junto das constantes.
	MetQueryLongestRunningSeconds = "nishidb_query_longest_running_seconds"
	MetQueryLongRejectedTotal     = "nishidb_query_long_rejected_total"

	// MetQueryBufferBytes e MetQueryBudgetRejectedTotal são do orçamento de
	// memória de consulta (§80: runtime.memoryTarget e
	// runtime.query.maxBufferPerQuery). Não estão no §92 porque, quando aquela
	// seção foi escrita, as duas chaves eram parseadas e nunca lidas — não havia
	// orçamento, e por isso não havia o que medir. Era metade do débito 15.
	//
	// MetQueryBufferBytes é MEDIDOR e é lido no scrape, do próprio contador
	// atômico do orçamento: é o único número que responde "o memoryTarget que
	// eu configurei está apertado?" antes de a primeira recusa acontecer. Sem
	// ele, o operador só descobriria o teto pelo erro — e aí já perdeu uma
	// consulta para aprender o que um painel diria de graça.
	//
	// MetQueryBudgetRejectedTotal conta as linhas recusadas por não caberem no
	// orçamento (ROW_TOO_LARGE). É separada de
	// nishidb_query_errors{categoria="ROW_TOO_LARGE"} pela mesma razão que
	// MetQueryLongRejectedTotal é separada da dela: a série de erros é por
	// dataset e serve ao diagnóstico de um; esta é do PROCESSO e serve a uma
	// decisão de capacidade, que é subir a chave. Somar categorias sobre todos
	// os datasets não é conta que alguém escreva num alerta.
	MetQueryBufferBytes         = "nishidb_query_buffer_bytes"
	MetQueryBudgetRejectedTotal = "nishidb_query_budget_rejected_total"
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

	// MetKafkaWatchdogTrips conta as vezes que o watchdog recriou o
	// consumidor por falta de progresso, e MetKafkaWatchdogDesistiu marca 1
	// quando ele esgotou as tentativas.
	//
	// Duas séries e não uma: recriar é recuperação — o produto se consertou e
	// seguiu —, enquanto desistir é ingestão morta com consultas de pé. Somar
	// as duas faria um alarme disparar igual nos dois casos, e só um deles
	// pede alguém acordado.
	MetKafkaWatchdogTrips    = "nishidb_kafka_watchdog_trips_total"
	MetKafkaWatchdogDesistiu = "nishidb_kafka_watchdog_desistiu"

	// MetKafkaQueueDepth e MetKafkaQueueBytes expõem os dois gates da fila
	// limitada. O §79 exige "Kafka buffers" entre os itens de memória, e
	// contar registros não é unidade de memória (D16).
	MetKafkaQueueDepth = "nishidb_kafka_queue_depth"
	MetKafkaQueueBytes = "nishidb_kafka_queue_bytes"

	// MetIngestDecodeSerial conta os eventos que o writer teve de decodificar
	// de novo, pelo caminho completo, porque o decode paralelo é
	// somente-leitura e alcançou uma coluna a criar ou um tipo a fixar.
	//
	// É invenção da Fase 4, e o defeito que ela mede é o único jeito de a
	// paralelização do decode ficar mais lenta do que a execução serial: uma
	// origem que emita chave nova a cada evento, ou um dicionário no teto sob
	// `onMaxCols: ignore`, fazem todo evento pagar o parse dobrado. Em regime a
	// série é PLANA — o piloto tem ~1.700 colunas e não descobre mais nenhuma —,
	// então `rate()` acima de zero por muito tempo é a pergunta a fazer.
	MetIngestDecodeSerial = "nishidb_ingest_decode_serial_total"

	// MetIngestDecodeArenaBytes é a memória que os slots da esteira de
	// decodificação seguram entre eventos, e MetIngestDecodeSlotsShrunk conta
	// as vezes que um slot foi solto por passar do limiar.
	//
	// A esteira é o TERCEIRO consumidor de memória da ingestão, e os dois gates
	// da fila não o enxergam: um conta registros enfileirados, o outro conta
	// bytes de payload CRU, e nenhum vê o arena tipado que o decode produziu e
	// o slot reteve até a marca d'água. Sem esta série, um evento de 4 MiB
	// prenderia 4 MiB por slot para sempre, dentro de um orçamento de 1 GiB
	// (§109), e a única pista seria o RSS subindo sem explicação.
	//
	// As duas juntas contam a história: a primeira diz quanto está preso, a
	// segunda diz se o encolhimento está trabalhando. Só a primeira deixaria
	// "não cresce" indistinguível de "cresce e é aparado sem parar", e o
	// segundo é churn de alocação no caminho quente.
	MetIngestDecodeArenaBytes  = "nishidb_ingest_decode_arena_bytes"
	MetIngestDecodeSlotsShrunk = "nishidb_ingest_decode_slots_shrunk_total"

	// MetIngestDecodeWorkers é quantos decodificadores o dataset subiu.
	//
	// Configuração publicada como série, e não só no log da subida, porque ela
	// é o denominador de toda leitura de vazão do painel: `events_total` de um
	// dataset com 8 decodificadores e de outro com 1 não são comparáveis, e o
	// operador não tem de ir ao YAML para descobrir isso.
	MetIngestDecodeWorkers = "nishidb_ingest_decode_workers"

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
