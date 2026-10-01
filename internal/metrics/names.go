package metrics

// Kafka counter outputs, labelled by dataset, counter and destination.
const (
	MetSeriesKafkaDeliveredTotal       = "nishidb_series_kafka_delivered_total"
	MetSeriesKafkaDroppedTotal         = "nishidb_series_kafka_dropped_total"
	MetSeriesKafkaPendingRecords       = "nishidb_series_kafka_pending_records"
	MetSeriesKafkaPendingBytes         = "nishidb_series_kafka_pending_bytes"
	MetSeriesKafkaLastSuccessTimestamp = "nishidb_series_kafka_last_success_timestamp"
)

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
	// A omissão é travada por teste; ver internal/server/metrics_dataset_test.go.
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
	MetKafkaWriterIDleSeconds  = "nishidb_kafka_writer_idle_seconds_total"

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
	// MetTTLAnchorAbsent tem o mesmo rótulo `rule` e só existe para as regras
	// com `anchor`: escritas em que a regra teria decidido e a âncora faltava
	// no record, e a próxima regra foi tentada.
	MetTTLAnchorAbsent = "nishidb_ttl_anchor_absent_total"
	// MetTTLAnchorInFuture são as âncoras posteriores à escrita, limitadas a
	// ela — o sinal de uma origem com relógio adiantado.
	MetTTLAnchorInFuture = "nishidb_ttl_anchor_in_future_total"
	// MetTTLExpiredOnWrite são os records gravados já vencidos: a âncora mais
	// o prazo ficou no passado. Backlog relido é a causa típica.
	MetTTLExpiredOnWrite = "nishidb_ttl_expired_on_write_total"

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
	// todo o índice do dataset, medido em BLOCOS — inclusive o índice por
	// segmento, que mora em data/ ao lado do .dat.
	//
	// Separadas porque o teto declarado pelo operador governa DADO e o índice
	// tem teto próprio: somá-las num número só faria ele dimensionar
	// `history.maxBytes` contra bytes que aquela chave não comanda.
	//
	// A purga alcança as DUAS, e a série de índice CAI depois de uma passada:
	// o .sidx sai no mesmo passo que remove o .dat. Ela não caía quando todo
	// índice era global — era esse o motivo antigo desta separação, e ele
	// deixou de valer.
	MetHistoryDataBytes  = "nishidb_history_data_bytes"
	MetHistoryIndexBytes = "nishidb_history_index_bytes"
	// MetHistoryOldestWrittenAt é a série que se olha primeiro: é ela que
	// mostra o maxBytes engolindo o expireAfterWrite declarado.
	MetHistoryOldestWrittenAt = "nishidb_history_oldest_written_at"
	// MetHistoryIndexWindow é a irmã da MetHistoryOldestWrittenAt, do lado do
	// ÍNDICE: ela publica o intervalo REALMENTE coberto pelos segmentos
	// indexados, em segundos.
	//
	// Comparada com o `history.index.window` declarado, ela diz numa olhada se
	// o teto de bytes assumiu o comando — e é o único lugar em que o
	// conservadorismo da janela aparece. A cobrança do segmento ainda não
	// construído é pelo MAIOR índice em disco, então um .sidx atípico encolhe a
	// cobertura sem que nada mais no painel mude; sem esta série isso seria
	// consulta lenta sem explicação.
	MetHistoryIndexWindow = "nishidb_history_index_window_seconds"
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
	// tls.certificado_expirando, em internal/server/security.go).
	MetTLSCertExpiryTimestamp = "nishidb_tls_cert_expiry_timestamp"

	// MetQueryLongestRunningSeconds e MetQueryLongRejectedTotal são do sub-pool
	// de consulta longa. Não estão no §92, e a razão é a mesma das duas
	// anteriores: aquela seção foi escrita quando `query.timeout` era ao mesmo
	// tempo o default e o teto, então "consulta longa" não era uma categoria
	// que o produto reconhecesse — não havia o que medir.
	//
	// MetQueryLongestRunningSeconds é a idade da consulta em voo há mais tempo.
	// É MEDIDOR e é lida no scrape (ver inVoo.maisAntiga), nunca atualizada na
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

	// As três do ISOLAMENTO DE LEITURA. Também fora do §92, e pela mesma razão
	// das duas acima: quando aquela seção foi escrita não existia snapshot, e
	// não havia o que medir.
	//
	// Elas respondem a três perguntas diferentes, e nenhuma substitui as
	// outras. MetQuerySnapshotBytes é MEDIDOR e diz quanto do teto está em uso
	// AGORA — é o número que responde "o snapshotMaxBytes que eu configurei
	// está apertado?" antes da primeira recusa. MetQuerySnapshotOldestSeconds
	// diz HÁ QUANTO TEMPO o dataset está guardando passado, que é o que trava
	// a compaction e a retenção; ela é irmã da longest_running e não a mesma
	// coisa, porque uma consulta pode estar em voo sem reter versão nenhuma.
	// MetQuerySnapshotExpiredTotal conta as recusas por teto, e é a única das
	// três que alarma: enquanto for zero, o teto está sobrando.
	//
	// A quarta é da COMPACTION, e não da consulta, porque é lá que o efeito
	// aparece: segmento pinado é segmento que não devolve disco. Sem ela, uma
	// compaction parada por consulta longa é indistinguível de uma compaction
	// sem trabalho — as duas publicam zero aposentadorias.
	MetQuerySnapshotBytes         = "nishidb_query_snapshot_bytes"
	MetQuerySnapshotOldestSeconds = "nishidb_query_snapshot_oldest_seconds"
	MetQuerySnapshotExpiredTotal  = "nishidb_query_snapshot_expired_total"
	MetCompactionPinnedSegments   = "nishidb_compaction_segmentos_pinados"

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

// As séries do ÍNDICE POR SEGMENTO (spec do subsistema 1, §6.6). Não estão no
// §92 porque quando aquela seção foi escrita o índice era global: não havia
// segmento a classificar, não havia janela, não havia reconstrução e não havia
// caminho degradado a contar.
//
// Elas existem porque o terceiro dos três defeitos que o ciclo corrige é
// "nada avisa o operador". Os outros dois — o Set quadrático e a purga que não
// tocava o índice — são de mecanismo, e o desenho novo os torna inexprimíveis;
// este é de observabilidade, e só some se alguém publicar os números.
//
// A JANELA EFETIVA NÃO ESTÁ AQUI, e a ausência é decisão registrada na §6.6.
// Quem publica o intervalo realmente coberto pelos segmentos indexados é
// MetHistoryIndexWindow (nishidb_history_index_window_seconds), mais acima neste
// arquivo. A razão está na §4.5: a série existe espelhando a de instante mais
// antigo, que mostra o `maxBytes` engolindo o `expireAfterWrite` — e essa série
// espelhada é do bloco `history`, rotulada pelo dataset BASE, onde o operador
// declarou a política. As duas famílias respondem perguntas diferentes:
// `nishidb_history_*` diz o que a política do histórico de X está fazendo, vista
// de X, e `nishidb_index_*` diz o que a MECÂNICA de índice de cada Store está
// fazendo, saindo também na tabela irmã. A janela é política, não mecânica; pô-la
// aqui deixaria uma série rotulada pelo base dentro de uma família rotulada pela
// irmã, ou seja dois números iguais em séries que parecem falar de coisas
// diferentes. Este parágrafo é o ponteiro que impede a decisão de virar
// armadilha para quem procurar a janela nesta família.
const (
	// MetIndexSegments classifica os segmentos do dataset pelo rótulo
	// LabelState, e os quatro valores PARTICIONAM o dataset.
	//
	// Os quatro, e não três, por causa de duas ausências de significado oposto:
	// StateOutside é o segmento que a janela deixou de cobrir — deliberadamente
	// sem índice, e num dataset grande a maioria esmagadora —, enquanto
	// StatePending é o segmento dentro da janela ainda na fila. Conflá-los num
	// "ausente" daria um número enorme, normal e mudo, e o gauge deixaria de
	// responder "o laço está avançando?", que é a única pergunta que uma
	// migração faz.
	MetIndexSegments = "nishidb_index_segmentos"
	// MetIndexBytes são os BLOCOS do índice por segmento: os .sidx selados, os
	// temporários e os .widx do corrente.
	//
	// Ela é parcela estrita de MetHistoryIndexBytes — que soma também indexes/ e
	// checkpoint/ —, e as duas saem da mesma função de disco: uma definição, dois
	// leitores. Separada porque é ela que carrega a promessa central do ciclo, e
	// promessa sem número é opinião: com a tabela histórica em regime, ESTE valor
	// para de crescer. Hoje o índice é o único componente que não para, mesmo com
	// a retenção funcionando e o dataset estável.
	//
	// AS DUAS SÉRIES NÃO SE COMPARAM NO MESMO RÓTULO, e isto é o oposto de um
	// detalhe: MetHistoryIndexBytes é do bloco `history`, que descreve a tabela
	// IRMÃ e sai rotulado pelo dataset BASE, onde o operador declarou a política;
	// esta descreve a mecânica do Store em que sai, e num par X/X_history a
	// parcela vive em {dataset="X_history"}. Um `history_index_bytes -
	// index_bytes` casa por rótulo idêntico (o default do PromQL) e subtrai a
	// série ERRADA: em {dataset="X"} ela é o índice do PRÓPRIO base — zero quando
	// o base não indexa nada, e um número sem relação nenhuma com o total quando
	// indexa. A comparação certa é entre rótulos diferentes.
	MetIndexBytes = "nishidb_index_bytes"
	// MetIndexRebuildsTotal conta os segmentos cujo índice a reconstrução
	// publicou desde a abertura do dataset. É a taxa que diz se a fila está
	// drenando; a fila em si é MetIndexSegments{estado="pendente"}.
	MetIndexRebuildsTotal = "nishidb_index_reconstrucoes_total"
	// MetIndexRebuildActive é o 0/1 do laço da §2.4, no molde de
	// MetCompactionRunning e MetHistoryRetentionRunning — e pelo mesmo motivo: ele
	// para de vez na primeira falha, e sem esta série isso é indistinguível de
	// "não há o que reconstruir".
	//
	// Ela sobe ANTES da goroutine (§92): quem chamou StartRebuild tem
	// direito de ver o estado que acabou de instalar, e uma leitura entre a
	// chamada e o primeiro escalonamento não pode responder que nada subiu. Por
	// isso ela NASCE EM 1 num dataset com campo indexado — o valor verdadeiro na
	// subida — e não em zero como os contadores ao lado.
	MetIndexRebuildActive = "nishidb_index_reconstrucao_ativa"
	// MetIndexCRCFailuresTotal conta os .sidx reprovados no crc32c, e é DEGRAU —
	// não taxa.
	//
	// Ela sobe uma vez por ABERTURA do arquivo, e o garanteSealed publica no cache
	// o mapeamento reprovado, de modo que a abertura acontece uma vez por segmento
	// por processo. Um alerta escrito com rate() sobre ela dá um pico e se
	// auto-resolve em minutos, enquanto o segmento segue enumerando
	// indefinidamente — e essa foi a leitura errada que a primeira redação desta
	// série induziu.
	//
	// E NENHUMA SÉRIE RESPONDE "há índice podre AGORA", o que é um limite e não um
	// descuido. São dois fatos, os dois medidos em
	// TestOGaugeDeEstadosEhCegoAIndicePodre:
	//
	//   - MetIndexSegments é CEGO à corrupção. Os quatro estados saem idênticos
	//     entre um dataset saudável e um com dois .sidx podres, porque o arquivo
	//     reprovado continua contado como `selado`: a fila é derivada da tabela de
	//     seções e ela não confere crc (débito 103). É justamente o gauge que a
	//     migração acompanha;
	//   - MetIndexSegmentsLookedUpTotal{modo="enumeracao"} SOBE, mas sem
	//     atribuir causa: o incremento é o mesmo que um segmento fora da janela ou
	//     na fila produz, no mesmo balde e sem rótulo que os separe.
	//
	// O sinal disponível é o WARN indice.selado_corrompido no log. Esta série
	// responde "quantos arquivos já foram pegos desde a subida", que é gatilho de
	// investigar, não de acompanhar.
	MetIndexCRCFailuresTotal = "nishidb_index_crc_falhas_total"
	// MetIndexSegmentsLookedUpTotal conta, pelo rótulo LabelMode, quantos
	// segmentos cada Lookup resolveu por índice e quantos por enumeração (§3.4).
	//
	// É o ÚNICO observável da degradação: a fonte de enumeração é apenas mais um
	// iterador ascendente na mesma combinação, e planner, executor e driver não
	// têm como distingui-la. Sem esta série, um dataset inteiro respondendo por
	// enumeração é indistinguível de um dataset inteiro indexado — a mesma
	// resposta, com outro custo.
	MetIndexSegmentsLookedUpTotal = "nishidb_index_segmentos_consultados_total"
)

// Os rótulos das séries de índice, e os CONJUNTOS FECHADOS de valores que cada
// um aceita.
//
// Eles são constantes exportadas, e não literais no ponto de publicação, pela
// razão que o LabelReason já registra: label nunca vem de entrada do cliente.
// Aqui a garantia é mais forte que uma convenção, porque a enumeração está
// escrita uma vez e é o teste que a compara com o que sai na porta — um valor
// novo que aparecesse no scrape sem passar por aqui reprova.
const (
	LabelState = "estado"
	LabelMode  = "modo"
)

// Os quatro estados de MetIndexSegments. Ver o godoc da série.
const (
	StateSealed  = "selado"
	StateCurrent = "corrente"
	StatePending = "pendente"
	StateOutside = "fora"
)

// Os dois modos de MetIndexSegmentsLookedUpTotal.
const (
	ModeIndex       = "indice"
	ModeEnumeration = "enumeracao"
)

// IndexStates é a enumeração fechada de LabelState, na ordem em que as
// séries são emitidas.
//
// Ela existe como FATIA, e não só como quatro constantes, porque tem dois
// consumidores que precisam concordar: o coletor, que emite as quatro séries
// sempre — inclusive as zeradas, senão `rate()` mente até o primeiro segmento
// mudar de estado —, e o teste, que confere que o scrape não traz um quinto
// valor. Duas listas escritas à mão divergiriam na primeira adição.
var IndexStates = []string{StateSealed, StateCurrent, StatePending, StateOutside}

// IndexLookupModes é a enumeração fechada de LabelMode, pela mesma razão.
var IndexLookupModes = []string{ModeIndex, ModeEnumeration}

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
	// consumidor por falta de progresso, e MetKafkaWatchdogGaveUp marca 1
	// quando ele esgotou as tentativas.
	//
	// Duas séries e não uma: recriar é recuperação — o produto se consertou e
	// seguiu —, enquanto desistir é ingestão morta com consultas de pé. Somar
	// as duas faria um alarme disparar igual nos dois casos, e só um deles
	// pede alguém acordado.
	MetKafkaWatchdogTrips  = "nishidb_kafka_watchdog_trips_total"
	MetKafkaWatchdogGaveUp = "nishidb_kafka_watchdog_desistiu"

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

	// MetKafkaDecodeIDleSeconds é a espera SOMADA dos decodificadores pela
	// fila, e ela existe porque a ociosidade do WRITER mudou de dono.
	//
	// Até a Fase 4 o writer consumia a fila, e a espera dele era a da fila. Com
	// os decodificadores no meio, quem espera ali são eles — e são K. Publicar
	// aquele número como `writer_idle` mentiria por um FATOR: com oito
	// decodificadores parados o acumulado sobe oito segundos por segundo, e a
	// tabela de decisão de docs/operacao-kafka.md compara `rate()` com 1. O
	// writer_idle passou a sair da esteira, que é onde o writer de fato espera,
	// e esta série guarda o outro lado — dividida por
	// nishidb_ingest_decode_workers, ela é a fração de tempo em que os
	// decodificadores não tinham o que fazer.
	MetKafkaDecodeIDleSeconds = "nishidb_kafka_decode_idle_seconds_total"

	// MetKafkaPartitionsAssigned contra MetKafkaPartitionsTotal é o que
	// torna visível o assignment parcial — duas instâncias no mesmo group
	// dividindo partições, cada dataset materializando metade (D10).
	MetKafkaPartitionsAssigned = "nishidb_kafka_partitions_assigned"
	MetKafkaPartitionsTotal    = "nishidb_kafka_partitions_total"
)

// Contadores mantidos na escrita (planning/contadores-mantidos-na-escrita.md,
// §11.5). Nenhuma está no §92: o contador nasceu depois da spec. As dez saem
// por contador, com o rótulo `counter`, e nascem com zero na subida.
//
// Três delas podem alarmar, e é por elas que a família existe:
//
//   - GroupsRefusedTotal: um grupo novo foi recusado pelo teto de maxGroups. A
//     linha entrou, mas não é contada — o total daquele equipamento está
//     ERRADO para baixo até o contador ser reconstruído com teto maior. Zero
//     é o único valor aceitável em regime.
//   - OrphanEntries: entradas do vetor que apontavam para grupo aposentado ou
//     inexistente na dobra da abertura. Não é esperado fora de uma janela de
//     crash que a aposentadoria coberta por checkpoint fecha; diferente de
//     zero é sinal para auditar.
//   - AuditDivergentGroups: o que a última auditoria achou. Fica em zero até a
//     primeira, e o LastSuccessTimestamp ao lado é o que separa "nunca
//     auditado" de "auditado e certo".
//
// Groups contra GroupsMax é o inventário contra o teto (§11.2). Bytes é o que
// o contador ocupa em disco (vetor mais dicionário).
//
// Rows e CountedRows são os agregados do contador inteiro, e existem para que
// o denominador e o numerador do alarme saiam do scrape sem uma consulta e sem
// somar os grupos: Rows é quantas linhas apontam para algum grupo (os
// membros), CountedRows quantas satisfazem a condição. Rows abaixo do
// nishidb_rows do dataset é a diferença que ninguém enxergava sem varrer — as
// linhas sem alguma coluna do groupBy e, com `scope`, as fora do escopo, que o
// contador não conta por desenho.
//
// CountedRows, e não `nishidb_counter_total`: os dois são MEDIDORES — descem
// quando uma linha é lapidada ou vence — e o sufixo _total é reservado a
// contador monotônico, que é o que as duas séries `_refused_total` e
// `_retired_total` desta mesma família são. As COLUNAS de _meta.counters e os
// campos do /v1/stats continuam `rows` e `total`, que é o vocabulário do
// produto; quem muda de nome é só a série.
const (
	MetCounterGroups                    = "nishidb_counter_groups"
	MetCounterGroupsMax                 = "nishidb_counter_groups_max"
	MetCounterGroupsRefusedTotal        = "nishidb_counter_groups_refused_total"
	MetCounterGroupsRetiredTotal        = "nishidb_counter_groups_retired_total"
	MetCounterOrphanEntries             = "nishidb_counter_orphan_entries"
	MetCounterBytes                     = "nishidb_counter_bytes"
	MetCounterRows                      = "nishidb_counter_rows"
	MetCounterCountedRows               = "nishidb_counter_counted_rows"
	MetCounterAuditDivergentGroups      = "nishidb_counter_audit_divergent_groups"
	MetCounterAuditLastSuccessTimestamp = "nishidb_counter_audit_last_success_timestamp"
)

// Nomes de label do produto. Um label a mais multiplica séries por
// cardinalidade, e cada um aqui existe por uma razão registrada: LabelCategory
// separa nishidb_query_errors por categoria de falha de consulta;
// LabelInvalidReason (Task A5, issue #15) separa nishidb_ingest_invalid_total
// pela categoria §101 do evento ruim — sem ele, EVENT_TOO_LARGE (a cauda de
// chave que estoura MaxKeySize) ficava indistinguível das outras cinco
// categorias dentro do mesmo total.
const (
	LabelDataset       = "dataset"
	LabelPartition     = "partition"
	LabelCategory      = "categoria"
	LabelInvalidReason = "motivo"
	// LabelCounter é o nome do contador mantido na escrita, e o valor vem do
	// YAML do dataset — conjunto fechado, declarado na subida —, nunca de
	// entrada do cliente. Cardinalidade: o número de contadores declarados,
	// que é o número de linhas do bloco `counters:`.
	LabelCounter = "counter"

	// LabelReason é o motivo de UMA falha de autenticação HTTP Basic
	// (Task B3) — enum FECHADO {header, usuario, senha}, nunca o username:
	// mesma doutrina de datasetDesconhecido
	// (internal/server/metrics_query.go), que já recusa alimentar um label
	// com texto de entrada do cliente por ser cardinalidade sem teto. Chamado
	// "reason" e não "motivo" (o nome que LabelInvalidReason já ocupa, com outro
	// propósito) porque é a grafia que o plano da Fase 11 fixa para
	// nishidb_auth_failures_total.
	LabelReason = "reason"

	// LabelAlarm é o nome do alarme declarado no contador, e o valor vem do
	// YAML do dataset — conjunto fechado, declarado na subida —, nunca de
	// entrada do cliente. Cardinalidade: o número de alarmes declarados, que é
	// o número de itens dos blocos `alarms:`. Mesma doutrina do LabelCounter,
	// que ele acompanha em toda série de alarme.
	LabelAlarm = "alarm"

	// LabelMetric é o nome da métrica de janela declarada no contador, e o
	// valor vem do YAML do dataset — conjunto fechado, declarado na subida —,
	// nunca de entrada do cliente. Cardinalidade: o número de itens dos blocos
	// `metrics:`, limitado a schema.MaxCounterMetrics por contador. Mesma
	// doutrina do LabelAlarm, e acompanha o LabelCounter em toda série por
	// métrica.
	LabelMetric = "metric"
)

// Séries das MÉTRICAS DE JANELA dos contadores. Só nos datasets com algum
// contador que declara `metrics:`, e todas publicadas desde a subida: "a janela
// não mudou" tem de ser distinguível de "ninguém está amostrando", e é
// MetCounterMetricSampling que responde a segunda pergunta.
//
// As quatro primeiras são por (dataset, counter, metric); a do mapa de
// posições, por (dataset, counter); as três últimas, por dataset — a passada é
// do dataset inteiro.
const (
	// MetCounterMetricGroups são as posições ocupadas por grupos vivos do
	// contador — o mesmo número em todas as métricas dele.
	MetCounterMetricGroups = "nishidb_counter_metric_groups"
	// MetCounterMetricHeapBytes é a estimativa das entradas da métrica em heap.
	// Acompanha o máximo de grupos vivos simultâneos, e não a vida do contador.
	MetCounterMetricHeapBytes = "nishidb_counter_metric_heap_bytes"
	// MetCounterMetricDiskBytes é o tamanho do arquivo mapeado da métrica.
	MetCounterMetricDiskBytes = "nishidb_counter_metric_disk_bytes"
	// MetCounterMetricResetsTotal são as recriações do arquivo por cabeçalho
	// divergente ou ilegível: cada uma perdeu a janela daquela métrica.
	MetCounterMetricResetsTotal = "nishidb_counter_metric_resets_total"
	// MetCounterMetricPositionMapBytes é o mapa id → posição do CONTADOR. Sai
	// uma vez por contador, e não por métrica: o mapa é compartilhado pelas
	// métricas dele, e uma série por métrica somada no painel o contaria mais de
	// uma vez.
	MetCounterMetricPositionMapBytes = "nishidb_counter_metric_position_map_bytes"
	// MetCounterMetricSampleSeconds é a duração da última passada completa. É
	// medidor, como MetAlarmSweepSeconds: descreve a última, e não acumula.
	MetCounterMetricSampleSeconds = "nishidb_counter_metric_sample_seconds"
	// MetCounterMetricRestoredGroups são as entradas que saíram da abertura com
	// amostra na janela, somadas sobre as métricas.
	MetCounterMetricRestoredGroups = "nishidb_counter_metric_restored_groups"
	// MetCounterMetricSampling é 1 enquanto o laço de amostragem está de pé.
	// Zero com métrica declarada é a janela CONGELADA: o laço parou na primeira
	// falha, ou ainda não subiu.
	MetCounterMetricSampling = "nishidb_counter_metric_sampling"
)

// Séries dos ALARMES de contador. Todas nascem com zero na subida: "nenhum
// alarme disparou" tem de ser distinguível de "ninguém está observando", e sem
// o zero um painel vazio não diz qual dos dois é.
//
// As três primeiras são por (dataset, counter, alarm); as três últimas, por
// dataset — a passada é do dataset inteiro, e não de um alarme.
const (
	// MetAlarmGroupsTracked são os grupos em observação: a condição vale para
	// eles, tenham ou não cumprido o prazo. Acompanha o PROBLEMA, nunca o
	// tamanho do inventário — se esta série crescer com o número de grupos do
	// contador, algo está guardando estado que não deveria.
	MetAlarmGroupsTracked = "nishidb_alarm_groups_tracked"
	// MetAlarmGroupsFiring são os grupos com o alarme disparado agora.
	MetAlarmGroupsFiring = "nishidb_alarm_groups_firing"
	// MetAlarmFiredTotal e MetAlarmResolvedTotal são os acumulados do ciclo.
	MetAlarmFiredTotal    = "nishidb_alarm_fired_total"
	MetAlarmResolvedTotal = "nishidb_alarm_resolved_total"
	// MetAlarmDisqualifiedTotal são as resoluções por desqualificação — o
	// `qualifyWhen` ficou falso com a condição ainda valendo. É SUBCONJUNTO de
	// MetAlarmResolvedTotal, nunca somado a ele: a diferença entre as duas são
	// as resoluções que dizem que o problema passou ou que o grupo saiu.
	MetAlarmDisqualifiedTotal = "nishidb_alarm_disqualified_total"
	// MetAlarmEnrichFailedTotal e MetAlarmEnrichSkippedTotal são séries
	// SEPARADAS de propósito: as duas deixam o bloco de enriquecimento vazio,
	// mas pedem ações diferentes — falha manda investigar a consulta, pulo diz
	// que houve avalanche e o teto da passada agiu.
	MetAlarmEnrichFailedTotal  = "nishidb_alarm_enrich_failed_total"
	MetAlarmEnrichSkippedTotal = "nishidb_alarm_enrich_skipped_total"
	// MetAlarmWatching é 1 enquanto o laço de varredura está de pé. É a série
	// que responde "ninguém está observando".
	MetAlarmWatching = "nishidb_alarm_watching"
	// MetAlarmSweepSeconds é a duração da última passada. Uma passada que
	// passou a durar mais que a cadência apareceria só como alarme atrasado,
	// sem causa nomeada, se esta série não existisse.
	MetAlarmSweepSeconds = "nishidb_alarm_sweep_seconds"
	// MetAlarmRestoredGroups são os cronômetros que vieram do disco na subida.
	MetAlarmRestoredGroups = "nishidb_alarm_restored_groups"
)

// Séries da EMISSÃO periódica dos contadores. Por (dataset, counter), exceto a
// de atividade, que é do dataset. Também nascem com zero.
const (
	// MetSeriesRowsTotal são as linhas escritas, e MetSeriesRotationsTotal as
	// rotações: a primeira é o volume da série, a segunda diz quanto histórico
	// está sendo descartado por intervalo.
	MetSeriesRowsTotal      = "nishidb_series_rows_total"
	MetSeriesRotationsTotal = "nishidb_series_rotations_total"
	// MetSeriesErrorsTotal distingue "a série parou porque nada mudou" de "a
	// série parou porque a escrita falha".
	MetSeriesErrorsTotal = "nishidb_series_errors_total"
	// MetSeriesBytes é o tamanho do arquivo corrente.
	MetSeriesBytes = "nishidb_series_bytes"
	// MetSeriesLastSuccessTimestamp é a última passada que escreveu sem erro,
	// em segundos do epoch. É a série que um alerta de "a emissão parou"
	// observa — e é zero até a primeira passada, o que é o estado de verdade.
	MetSeriesLastSuccessTimestamp = "nishidb_series_last_success_timestamp"
	// MetSeriesEmitting é 1 enquanto algum laço de emissão do dataset está de
	// pé.
	MetSeriesEmitting = "nishidb_series_emitting"
)

// Entrega de alarmes: os labels são destinos declarados e motivos fechados.
const (
	LabelDestination                       = "destination"
	MetAlarmDeliveryMemoryEvents           = "nishidb_alarm_delivery_memory_events"
	MetAlarmDeliveryMemoryBytes            = "nishidb_alarm_delivery_memory_bytes"
	MetAlarmDeliveryDiskEvents             = "nishidb_alarm_delivery_disk_events"
	MetAlarmDeliveryDiskBytes              = "nishidb_alarm_delivery_disk_bytes"
	MetAlarmDeliveryOldestPendingSeconds   = "nishidb_alarm_delivery_oldest_pending_seconds"
	MetAlarmDeliveryDeliveredTotal         = "nishidb_alarm_delivery_delivered_total"
	MetAlarmDeliveryFailedAttemptsTotal    = "nishidb_alarm_delivery_failed_attempts_total"
	MetAlarmDeliveryPersistenceErrorsTotal = "nishidb_alarm_delivery_persistence_errors_total"
	MetAlarmDeliveryDroppedTotal           = "nishidb_alarm_delivery_dropped_total"
	MetAlarmDeliveryLastSuccessTimestamp   = "nishidb_alarm_delivery_last_success_timestamp"
	MetAlarmDeliveryActive                 = "nishidb_alarm_delivery_active"
	MetAlarmDeliveryFailed                 = "nishidb_alarm_delivery_failed"
	MetAlarmDeliveryPaused                 = "nishidb_alarm_delivery_paused"
)
