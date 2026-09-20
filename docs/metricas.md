# Métricas expostas

As **115 séries** declaradas em [`internal/metrics/names.go`](../internal/metrics/names.go),
que é a fonte ÚNICA dos nomes — nenhum literal de nome de série existe fora
dali, e `TestTodaMetricaDeclaradaEstaDocumentada` confere que esta página
descreve todas elas.

Elas saem em dois lugares que leem a **mesma estrutura**, nunca dois
contadores para o mesmo fato:

- `GET /metrics` — texto Prometheus (`version=0.0.4`), a superfície completa;
- `GET /v1/stats` — JSON, um recorte das mesmas leituras para o operador e
  para o `nishidb counters` / painel.

A raspagem tem prazo de 5 s, e é ela que atualiza a marca d'água do broker
(2 s por dataset). Um scrape que estoure omite as séries daquele dataset
naquela raspagem — ausência é ausência de medida, e nunca zero.

## Como ler esta página

- **tipo** é o que sai no `# TYPE`: `counter` só sobe (o sufixo `_total` é
  reservado a ele), `gauge` sobe e desce, `histogram` traz `_bucket`, `_sum` e
  `_count`.
- **labels** são os do produto, conjunto fechado: `dataset`, `partition`,
  `categoria`, `motivo`, `counter`, `alarm`, `metric`, `reason`, `estado`,
  `modo`. **Nenhum valor
  de label vem de entrada do cliente** — todos saem do YAML ou de enums do
  código, porque label livre é cardinalidade sem teto.
- **⚠️** marca a série que **não estava exposta** no processo de referência
  (piloto, `v0.7.0-40-g3c57cce`, 05/09/2026). Ou depende de uma feature que
  aquele dataset não declara, ou só nasce no primeiro evento, ou não é
  emitida. As três classes estão separadas ao fim. Duas séries perderam a
  marca depois daquela leitura, pela correção da issue #47.

Os textos da coluna "o que responde" são os `# HELP` reais, lidos da saída do
processo — não uma paráfrase escrita à mão que possa divergir dela.

### Processo

O que o binário inteiro consome. Sem label de dataset: a pergunta é sobre o processo.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_process_rss_bytes` | gauge | — | RSS do processo em bytes. É o número do critério de aceite do §109 — o que o sistema operacional cobra. |
| `nishidb_go_heap_bytes` | gauge | — | Bytes alocados no heap Go. NÃO inclui os índices mmap, que são a maior parte do estado. |
| `nishidb_go_heap_objects` | gauge | — | Objetos vivos no heap Go. |

### Ingestão

Quantos eventos entram e o que acontece com cada um.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_ingest_events_total` | counter | `dataset` | Eventos oferecidos à ingestão. |
| `nishidb_ingest_events_rate` ⚠️ | ? | — | **não é emitida** — ver *O que não existe* |
| `nishidb_ingest_stale_total` | counter | `dataset` | Eventos descartados por sequence (§25). Não é erro. |
| `nishidb_ingest_invalid_total` | counter | `dataset`, `motivo` | Eventos descartados por serem ruins (§101), por motivo (categoria §101; Task A5/issue #15). |
| `nishidb_ingest_apply_seconds` | histogram | `dataset` | Latência de um evento pelo Apply: parse, estado, merge, Put e o Apenda do histórico. O checkpoint fica fora — ele tem histograma próprio. |
| `nishidb_ingest_failed_total` | counter | `dataset` | Eventos que NÃO entraram por falha que não é do evento. O §92 não prevê esta série, e sem ela a perda de dado seria invisível num daemon, onde não há exit code para carregá-la. |
| `nishidb_ingest_blocked` | gauge | `dataset` | 1 quando a ingestão do dataset está PARADA por política de erro. Sem ela, a pausa do `invalidEvent: stop` seria um consumer silenciosamente parado. |
| `nishidb_ingest_decode_serial_total` | counter | `dataset` | eventos que o writer decodificou de novo, sozinho, porque o decode paralelo é somente-leitura e encontrou coluna a criar ou tipo a fixar; em regime é plana |
| `nishidb_ingest_decode_arena_bytes` | gauge | `dataset` | Bytes que os slots da esteira de decodificação seguram entre eventos. Nenhum dos dois gates da fila conta esta memória. |
| `nishidb_ingest_decode_slots_shrunk_total` | counter | `dataset` | Slots da esteira de decodificação cujos buffers foram soltos por passarem do limiar de memória retida. Crescimento constante é churn de alocação; zero com arena_bytes alto é memória presa. |
| `nishidb_ingest_decode_workers` | gauge | `dataset` | Decodificadores configurados para este dataset (source.kafka.consumer.decodeWorkers). É o denominador de qualquer leitura de vazão. |

### Kafka

A conversa com o broker: posição, atraso, fila, partições e o watchdog.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_kafka_lag` | gauge | `dataset`, `partition` | Mensagens entre a posição aplicada e o fim do log. Não depende do commit no group. |
| `nishidb_kafka_offset` | gauge | `dataset`, `partition` | Próximo offset a consumir, por partição: a posição APLICADA pelo writer. |
| `nishidb_kafka_high_watermark` | gauge | `dataset`, `partition` | Fim do log por partição, como o broker respondeu no instante desta raspagem. |
| `nishidb_kafka_poll_blocked_seconds_total` | counter | `dataset` | Segundos que o poller passou esperando espaço na fila. rate() é a fração do tempo em que o writer foi o gargalo. |
| `nishidb_kafka_writer_idle_seconds_total` | counter | `dataset` | Segundos que o writer passou esperando trabalho na esteira. rate() alto com lag alto significa que quem não entrega é o poller ou os decodificadores. |
| `nishidb_kafka_errors_total` | counter | `dataset` | Erros de broker ou rede. Balde próprio: o §101 só cataloga erro de EVENTO, e contar erro de rede como perda de dado é diagnóstico errado. |
| `nishidb_kafka_watchdog_trips_total` | counter | `dataset` | Recriações do consumidor por falta de progresso com lag pendente. |
| `nishidb_kafka_watchdog_desistiu` | gauge | `dataset` | 1 quando o watchdog esgotou as tentativas e a ingestão do dataset ficou parada. |
| `nishidb_kafka_queue_depth` | gauge | `dataset` | Registros na fila limitada poller→writer, lidos no instante do scrape. |
| `nishidb_kafka_queue_bytes` | gauge | `dataset` | Bytes retidos na fila limitada (§79), lidos no instante do scrape. |
| `nishidb_kafka_decode_idle_seconds_total` | counter | `dataset` | Segundos SOMADOS que os decodificadores passaram esperando evento na fila. Divida por nishidb_ingest_decode_workers para ler como fração. |
| `nishidb_kafka_partitions_assigned` | gauge | `dataset` | Partições atribuídas a esta instância. Comparar com o total é o que torna visível o assignment parcial. |
| `nishidb_kafka_partitions_total` | gauge | `dataset` | Partições do tópico. |

### Consulta

O protocolo remoto: o que ele admite, recusa e quanto demora.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_query_total` | counter | `dataset` | consultas admitidas pelo protocolo remoto |
| `nishidb_query_active` | gauge | `dataset` | consultas em execução neste instante |
| `nishidb_query_errors` | counter | `categoria`, `dataset` | consultas que terminaram em erro, por categoria |
| `nishidb_query_full_scans` | counter | — | planos de full scan escolhidos (§64); é do processo, não do dataset |
| `nishidb_query_latency_seconds` | histogram | `dataset` | duração da consulta, do request ao último byte |
| `nishidb_query_longest_running_seconds` | gauge | — | idade da consulta em voo há mais tempo; 0 quando não há nenhuma |
| `nishidb_query_long_rejected_total` | counter | — | consultas longas recusadas por falta de vaga no sub-pool (runtime.query.maxLongRunning) |
| `nishidb_query_snapshot_bytes` | gauge | `dataset` | heap retido pelo journal de versões do isolamento de leitura |
| `nishidb_query_snapshot_oldest_seconds` | gauge | `dataset` | há quanto tempo a consulta mais antiga segura o passado; 0 quando não há nenhuma |
| `nishidb_query_snapshot_expired_total` | counter | `dataset` | consultas invalidadas por o journal de versões estourar runtime.query.snapshotMaxBytes |
| `nishidb_query_buffer_bytes` | gauge | — | bytes de buffer de consulta reservados neste instante (runtime.memoryTarget) |
| `nishidb_query_budget_rejected_total` | counter | — | linhas recusadas por não caberem no orçamento de buffer (runtime.query.maxBufferPerQuery) |

### Schema do dataset

Colunas descobertas, descartadas e divergências de tipo.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_dataset_fields` | gauge | `dataset` | campos registrados no dicionário do dataset; o denominador é schema.maxCols, publicado em /v1/metadata |
| `nishidb_dataset_fields_dropped_total` | counter | `dataset` | campos que o teto recusou; só sobe sob onMaxCols ignore, e é o que torna a perda contável em vez de silenciosa |
| `nishidb_dataset_nested_type_mismatch_total` | counter | `dataset` | valores de coluna aninhada que não couberam no tipo fixado; o evento entrou e o documento guarda o valor, e a coluna foi REMOVIDA naquela linha — sem a remoção ela ficaria com o valor do evento anterior, no modo merge |

### Materialização

O que o merge por campo faz com o evento: `merge: sum`, `frozenFrom` e lápides.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_materialized_rows` | gauge | `dataset` | linhas vivas materializadas no dataset |
| `nishidb_materialized_tombstones` ⚠️ | ? | — | **não é emitida** — ver *O que não existe* |
| `nishidb_frozen_captured_total` | counter | `dataset` | valores capturados por coluna congelada na ingestão; sem preservação correspondente, a coluna nunca chegou a congelar |
| `nishidb_frozen_preserved_total` | counter | `dataset` | valores que o merge preservou por serem de coluna congelada |
| `nishidb_merge_sum_total` | counter | `dataset` | somas feitas pelo merge em coluna com merge: sum |
| `nishidb_merge_sum_saturated_total` | counter | `dataset` | somas que bateram no teto do int64 e pararam ali; a partir da primeira, o valor da coluna deixa de crescer |

### Storage

Bytes em disco, linhas vivas e o ciclo dos segmentos.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_storage_bytes` | gauge | `dataset` | bytes ocupados pelos segmentos do dataset em disco, medidos no último checkpoint |
| `nishidb_storage_live_bytes` ⚠️ | ? | — | **não é emitida** — ver *O que não existe* |
| `nishidb_storage_garbage_bytes` | gauge | `dataset` | bytes de garbage nos segmentos: há pelo menos isto de lixo (limite INFERIOR, porque a liveness é limite superior — débitos 38 e 45) |
| `nishidb_segments_total` | gauge | `dataset` | segmentos do dataset |

### Índice

O índice por segmento e a janela efetiva.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_index_segmentos` | gauge | `dataset`, `estado` | segmentos por estado de índice: selado (o .sidx cobre o conjunto indexado), corrente (o índice é o WIP), pendente (dentro da janela e na fila de reconstrução) e fora (a janela não o cobre, e ele responde por enumeração de propósito) |
| `nishidb_index_bytes` | gauge | `dataset` | blocos ocupados pelo índice por segmento (.sidx, temporários e .widx do corrente) DESTE dataset. Num histórico ele é a parcela por segmento de nishidb_history_index_bytes — que soma também indexes/ e checkpoint/ e sai no rótulo do dataset BASE. As duas vivem em rótulos DIFERENTES: a comparação é entre {dataset="X_history"} aqui e {dataset="X"} lá, nunca no mesmo rótulo |
| `nishidb_index_reconstrucoes_total` | counter | `dataset` | segmentos cujo índice a reconstrução publicou desde a abertura do dataset; a taxa diz se a fila está drenando |
| `nishidb_index_reconstrucao_ativa` | gauge | `dataset` | 1 enquanto o laço de reconstrução do índice por segmento está de pé; 0 sem campo indexado ou depois de ele parar, e ele para de vez na primeira falha |
| `nishidb_index_crc_falhas_total` | counter | `dataset` | índices selados reprovados no crc32c desde a subida. É DEGRAU e não taxa: o mapeamento reprovado fica em cache, então cada arquivo podre soma UMA vez por processo e um rate() sobre isto se auto-resolve enquanto o segmento segue enumerando. Nenhuma série responde "há corrupção AGORA": o gauge de estados é cego a ela (o arquivo reprovado conta como selado, porque a fila não confere crc) e a enumeração sobe sem atribuir causa, no mesmo balde de um segmento fora da janela. O sinal é o WARN indice.selado_corrompido no log. É o débito 103 |
| `nishidb_index_segmentos_consultados_total` | counter | `dataset`, `modo` | segmentos que os Lookups resolveram por cada caminho; é o único observável da degradação, porque para o planner e o executor os dois são o mesmo iterador ascendente |

### Checkpoint e writer

Durabilidade: o que já está em disco, quando, e o writer falho.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_checkpoint_duration_seconds` | histogram | `dataset` | Duração do checkpoint em segundos, da chamada ao manifest durável (inclui a espera pelo mutex do writer, que é stall real). |
| `nishidb_checkpoint_last_success_timestamp` | gauge | `dataset` | Unix timestamp do último checkpoint bem-sucedido. Fica em 0 até o primeiro. |

### Compaction

A recuperação de espaço dos segmentos.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_compaction_running` | gauge | `dataset` | 1 enquanto o laço de compaction do §53-55 está de pé para o dataset; 0 com a chave desligada ou depois de ele parar |
| `nishidb_compaction_bytes_read` | counter | `dataset` | bytes de record que as varreduras de compaction leram desde a abertura do dataset |
| `nishidb_compaction_bytes_written` | counter | `dataset` | bytes que o copy-forward da compaction reescreveu; a distância para bytes_read é o disco devolvido (§86) |
| `nishidb_compaction_segmentos_pinados` | gauge | `dataset` | segmentos que a compaction não pode aposentar por guardarem versões de consultas em voo |

### Expiração

TTL: o que venceu, o que foi preservado pela carência e o coletor.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_expiry_running` | gauge | `dataset` | 1 enquanto o coletor do idle timeout está de pé para o dataset; 0 sem expireAfterWrite declarado ou depois de ele parar |
| `nishidb_expired_total` | counter | `dataset` | entidades que ganharam lápide por inatividade, pelas duas rotas: o coletor e a descoberta oportunista da compaction |
| `nishidb_ttl_rule_matched_total` | counter | `dataset`, `rule` | escritas decididas por cada regra de ttl; o rótulo é o ÍNDICE no YAML, porque é a ordem que decide qual vence |
| `nishidb_ttl_unmatched_total` | counter | `dataset` | escritas que não casaram com regra de ttl nenhuma; só acontece em dataset sem regra de fundo |
| `nishidb_expiry_grace_total` | counter | `dataset` | carências dadas a entidades sem instante de escrita; é o que sobe nos primeiros minutos depois de ligar a expiração num dataset já materializado, quando ainda não há nada a expirar |
| `nishidb_expiry_scanned_total` | counter | `dataset` | entradas do índice de escritas examinadas pelo coletor; comparada com o total de rows, diz se o cursor dá a volta ou empaca |

### Tabela histórica

Só existem em dataset que declara `history:`.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_history_rows_total` ⚠️ | counter | — | Linhas gravadas na tabela histórica. |
| `nishidb_history_dropped_total` ⚠️ | counter | — | Eventos que não viraram linha na tabela histórica. |
| `nishidb_history_degraded` ⚠️ | gauge | — | 1 quando a tabela histórica parou de receber (modo falho sticky). |
| `nishidb_history_data_bytes` ⚠️ | gauge | — | Bytes dos segmentos da tabela histórica — o que o maxBytes governa. |
| `nishidb_history_index_bytes` ⚠️ | gauge | — | Blocos alocados pelos índices da tabela histórica; a purga também os devolve, porque o .sidx sai no mesmo passo que remove o .dat. O número é da tabela IRMÃ, publicado no rótulo do dataset BASE; a parcela por segmento dele é nishidb_index_bytes{dataset=\ |
| `nishidb_history_oldest_written_at` ⚠️ | gauge | — | Instante da linha mais antiga ainda visível, em millis. Mostra o maxBytes engolindo o prazo declarado. |
| `nishidb_history_index_window_seconds` ⚠️ | gauge | — | Intervalo realmente coberto pelos segmentos indexados. Compare com history.index.window: menor significa que o teto do índice assumiu o comando. |
| `nishidb_history_segments_purged_total` ⚠️ | counter | — | Segmentos aposentados pela retenção da tabela histórica. |
| `nishidb_history_retention_running` ⚠️ | gauge | `dataset` | 1 quando o motor de retenção está de pé. Ele para de vez na primeira falha. |

### Contadores mantidos na escrita

Dez por contador declarado no bloco `counters:`, com o rótulo `counter`.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_counter_groups` | gauge | `counter`, `dataset` | grupos vivos do contador (inclui os com total zero; exclui os aposentados) |
| `nishidb_counter_groups_max` | gauge | `counter`, `dataset` | teto de grupos vivos (maxGroups) do contador |
| `nishidb_counter_groups_refused_total` | counter | `counter`, `dataset` | grupos novos recusados pelo teto nesta abertura; a linha entrou e NÃO é contada — diferente de zero é total errado para baixo |
| `nishidb_counter_groups_retired_total` | counter | `counter`, `dataset` | grupos aposentados por ficarem sem linha há mais que retainEmptyGroups, desde a criação do contador |
| `nishidb_counter_orphan_entries` | gauge | `counter`, `dataset` | entradas do vetor apontando para grupo aposentado ou inexistente, zeradas na dobra da abertura; não é esperado fora de uma janela de crash |
| `nishidb_counter_bytes` | gauge | `counter`, `dataset` | bytes em disco do contador: vetor de grupos mais dicionário |
| `nishidb_counter_rows` | gauge | `counter`, `dataset` | linhas vivas que apontam para algum grupo deste contador; abaixo do rows do dataset são as linhas sem alguma coluna do groupBy, que o contador não conta por desenho |
| `nishidb_counter_counted_rows` | gauge | `counter`, `dataset` | linhas que satisfazem a condição deste contador no dataset inteiro; sem `where`, igual ao rows do contador |
| `nishidb_counter_audit_divergent_groups` | gauge | `counter`, `dataset` | divergências na última auditoria conclusiva — grupos, linhas sem grupo internado e os dois agregados; 0 até a primeira (ver o timestamp ao lado) |
| `nishidb_counter_audit_last_success_timestamp` | gauge | `counter`, `dataset` | Unix timestamp da última auditoria conclusiva do contador; 0 até a primeira |

### Métricas de janela do contador

Só nos datasets com algum contador que declara `metrics:`. As quatro primeiras
saem por métrica declarada, com os rótulos `counter` e `metric`; a do mapa de
posições sai uma vez por contador; as três últimas são do dataset, porque a
passada de amostragem é do dataset inteiro.

Todas saem desde a subida, antes da primeira passada: o que existe desde a
abertura é a SÉRIE, e não um valor zero. "A janela não mudou" tem de ser
distinguível de "ninguém está amostrando", e é `nishidb_counter_metric_sampling`
que responde a segunda pergunta — ela sai 0 até o laço subir. O valor de
abertura das outras depende do disco: num dataset novo, `groups`,
`restored_groups` e `sample_seconds` saem zerados; depois de um reinício,
`groups` e `restored_groups` já saem com o que a restauração recuperou, e só
`sample_seconds` espera a primeira passada. `resets_total` conta as recriações
desta abertura, e por isso pode sair diferente de zero já na subida. As de bytes
saem com o tamanho da abertura, porque o arquivo e o primeiro bloco em heap
existem desde ela.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_counter_metric_groups` | gauge | `counter`, `dataset`, `metric` | posições ocupadas por grupos vivos do contador; o mesmo número em todas as métricas dele |
| `nishidb_counter_metric_heap_bytes` | gauge | `counter`, `dataset`, `metric` | estimativa das entradas da métrica em heap, em bytes; acompanha o máximo de grupos vivos simultâneos. Não inclui o mapa de posições, que tem série própria |
| `nishidb_counter_metric_disk_bytes` | gauge | `counter`, `dataset`, `metric` | tamanho do arquivo mapeado da métrica, em bytes |
| `nishidb_counter_metric_resets_total` | counter | `counter`, `dataset`, `metric` | recriações do arquivo da métrica por cabeçalho divergente ou ilegível; cada uma perdeu a janela da métrica. Mudar a `window` no YAML muda a largura do balde e também recria |
| `nishidb_counter_metric_position_map_bytes` | gauge | `counter`, `dataset` | mapa id → posição do contador, em bytes; um por contador, compartilhado pelas métricas dele. Sai UMA vez por contador: somar uma série por métrica contaria o mesmo mapa mais de uma vez |
| `nishidb_counter_metric_sampling` | gauge | `dataset` | 1 enquanto o laço de amostragem das métricas de janela está de pé. Zero com métrica declarada é a janela congelada: o laço ainda não subiu ou parou na primeira falha |
| `nishidb_counter_metric_sample_seconds` | gauge | `dataset` | duração da última passada completa de amostragem, em segundos. Acima do intervalo de amostragem (a menor janela dividida por 60, entre 1 s e 10 s), as amostras atrasam |
| `nishidb_counter_metric_restored_groups` | gauge | `dataset` | entradas que saíram da abertura com amostra na janela, somadas sobre as métricas. Zero depois de um reinício dentro da janela significa que a janela gravada não foi recuperada |

### Alarmes de contador

Só nos datasets que declaram `alarms:` dentro de algum contador. As sete
primeiras saem por alarme declarado, com os rótulos `counter` e `alarm`; as três
últimas são do dataset, porque a passada de varredura é do dataset inteiro.

Todas nascem com zero na subida. "Nenhum alarme disparou" tem de ser
distinguível de "ninguém está observando", e é `nishidb_alarm_watching` que
responde a segunda pergunta.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_alarm_groups_tracked` | gauge | `alarm`, `counter`, `dataset` | grupos em observação: a condição vale para eles, tenham ou não cumprido o prazo. Acompanha o problema, não o tamanho do inventário — se crescer com o número de grupos do contador, algo guarda estado que não deveria |
| `nishidb_alarm_groups_firing` | gauge | `alarm`, `counter`, `dataset` | grupos com o alarme disparado agora |
| `nishidb_alarm_fired_total` ⚠️ | counter | `alarm`, `counter`, `dataset` | disparos desde a subida |
| `nishidb_alarm_resolved_total` | counter | `alarm`, `counter`, `dataset` | resoluções desde a subida, somando condição que passou, grupo aposentado e desqualificação; o motivo de cada uma está no evento de log |
| `nishidb_alarm_disqualified_total` | counter | `alarm`, `counter`, `dataset` | resoluções por desqualificação desde a subida: o `qualifyWhen` ficou falso com a condição ainda valendo. É SUBCONJUNTO de `nishidb_alarm_resolved_total`, nunca somado a ele — a diferença entre as duas são as resoluções que dizem que o problema passou ou que o grupo saiu |
| `nishidb_alarm_enrich_failed_total` ⚠️ | counter | `alarm`, `counter`, `dataset` | consultas de enriquecimento que falharam; o alarme saiu sem o bloco. Diferente de zero manda investigar a consulta declarada |
| `nishidb_alarm_enrich_skipped_total` ⚠️ | counter | `alarm`, `counter`, `dataset` | consultas puladas pelo teto da passada; o alarme saiu sem o bloco. Diferente de zero significa avalanche de disparos, não consulta ruim — é por isso que não compartilha série com a de falha |
| `nishidb_alarm_watching` | gauge | `dataset` | 1 enquanto o laço de varredura está de pé. Zero com alarme declarado significa que o laço parou na primeira falha |
| `nishidb_alarm_sweep_seconds` | gauge | `dataset` | duração da última passada. Acima da cadência (o menor `sustainedFor` dividido por quatro), os alarmes atrasam — e sem esta série o atraso apareceria sem causa |
| `nishidb_alarm_restored_groups` | gauge | `dataset` | cronômetros de sustentação lidos do disco na subida. Diferente de zero significa que o processo voltou com alarmes em curso |

### Emissão periódica de contador

Só nos datasets que declaram `emit:` dentro de algum contador. As cinco
primeiras saem por contador com destino `file`, com o rótulo `counter`.
`nishidb_series_emitting` também considera emissores exclusivamente Kafka.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_series_rows_total` | counter | `counter`, `dataset` | linhas de série escritas desde a subida |
| `nishidb_series_rotations_total` | counter | `counter`, `dataset` | rotações do arquivo desde a subida; é quanto histórico está sendo descartado por intervalo. Subindo rápido, aumente `maxBytes` ou `keep` |
| `nishidb_series_errors_total` ⚠️ | counter | `counter`, `dataset` | falhas de escrita da série; diferente de zero significa série incompleta |
| `nishidb_series_bytes` | gauge | `counter`, `dataset` | tamanho do arquivo corrente. O teto de disco de um contador é `maxBytes × (keep+1)` |
| `nishidb_series_last_success_timestamp` | gauge | `counter`, `dataset` | epoch segundos da última passada que escreveu sem erro; 0 até a primeira. É a série que um alerta de "a emissão parou" observa |
| `nishidb_series_emitting` | gauge | `dataset` | 1 enquanto algum laço de emissão do dataset está de pé |

Destinos Kafka publicam as séries abaixo por `dataset`, `counter` e
`destination`, inclusive antes da primeira entrega. A origem é a mesma de
`datasets[].seriesDelivery` no `/v1/stats`.

| série | tipo | o que responde |
|---|---|---|
| `nishidb_series_kafka_delivered_total` | counter | mensagens confirmadas pelo Kafka desde a subida |
| `nishidb_series_kafka_dropped_total` | counter | mensagens descartadas ou não confirmadas; label adicional `reason`: `buffer_full`, `message_too_large`, `timeout`, `publish_error`, `shutdown` |
| `nishidb_series_kafka_pending_records` | gauge | mensagens aguardando conclusão, incluindo as em voo |
| `nishidb_series_kafka_pending_bytes` | gauge | bytes de chave e corpo das mensagens pendentes |
| `nishidb_series_kafka_last_success_timestamp` | gauge | última confirmação em segundos do epoch; zero até a primeira |

Timeout pode ocorrer após o broker armazenar a mensagem. Os descartes medem
resultados locais, sem afirmar perda exata no tópico. Limites e garantias estão
na [operação da emissão Kafka](operacao-series-kafka.md).

### Autenticação e TLS

Só com `server.authentication` / `server.tls` configurados.

| série | tipo | labels | o que responde |
|---|---|---|---|
| `nishidb_auth_failures_total` ⚠️ | counter | `reason` | falhas de autenticação HTTP Basic no protocolo remoto, por motivo |
| `nishidb_tls_cert_expiry_timestamp` | gauge | — | epoch segundos do NotAfter do leaf de server.tls; o operador deriva quanto falta no painel/alerta |
## As três classes de ausência

Uma série que não aparece no `/metrics` não diz, sozinha, o que está
acontecendo — e as três razões pedem remédios diferentes.

### 1. Depende de uma feature que o dataset não declara

As nove `nishidb_history_*` só existem em dataset com bloco `history:`; as de
autenticação e TLS, só com `server.authentication` / `server.tls`. Nada a
fazer: o dataset não tem tabela histórica, então não há retenção para medir.

### 2. Nasce no primeiro evento, e não na subida

`nishidb_auth_failures_total` carrega o label `reason`, e uma família com
label só emite série quando um valor existe — a primeira falha de
autenticação a cria. É o desenho: publicar `reason="senha"` com zero antes de
qualquer falha inventaria uma categoria que ninguém exercitou.

**Duas séries caíam aqui sem que isso fosse o desenho, e foram corrigidas**
(issue [#47](https://github.com/nishisan-dev/nishidb/issues/47)):
`nishidb_query_long_rejected_total` e `nishidb_query_budget_rejected_total`
são declaradas SEM label, mas a série só era criada dentro de `rejectLonga()`
e `rejectByBudget()` — isto é, na primeira recusa. As duas são exatamente
as que alarmam, e enquanto não existiam, `absent()` não distinguia "nunca
recusou" de "o alvo caiu". Hoje nascem com zero na construção de
`metricsOfQuery`, como o `desistiu.Set(0)` do watchdog sempre fez, e
`TestSeriesDeRecusaDeQueryNascemComZero` trava isso raspando o `/metrics` de
um servidor recém-subido — perguntar ao registry não serviria, porque lá a
família existe desde sempre; quem não existia era a série.

### 3. Não é emitida por ninguém

| série | por quê |
|---|---|
| `nishidb_ingest_events_rate` | **Omissão deliberada, travada por teste.** Taxa se calcula no Prometheus com a janela declarada no alerta (`rate(nishidb_ingest_events_total[5m])`); uma série "rate" carregaria uma janela escolhida aqui, que discordaria do contador ao lado. |
| `nishidb_materialized_tombstones` | Promessa não cumprida do ciclo de tipos temporais: exigiria manter o número de lápides vivas, decrementado na ressurreição e ajustado pela compaction. Já é débito com número em [`debitos.md`](debitos.md). |
| `nishidb_storage_live_bytes` | Declarada com o bloco do §92 e nunca alimentada. `nishidb_storage_bytes` (tamanho dos arquivos) e a razão de liveness da compaction respondem o que ela responderia. |

Constante declarada e não emitida é dívida silenciosa: quem lê `names.go`
acha que a série existe, e quem escreve o alerta descobre que não. As três
estão aqui para que a próxima leitura já saiba.

## O que este documento não é

Não é catálogo de alertas. As séries que **alarmam** e o que cada uma
significa em operação estão em [`operacao-kafka.md`](operacao-kafka.md) (a
ingestão) e em [`architecture.md`](architecture.md) (o resto); aqui está o
inventário, para quem precisa saber o que existe antes de escolher.

Números medidos de desempenho não moram aqui — eles ficam nos `aceite-*.md`,
com a bancada que os produziu.


### Entrega de alarmes por webhook

Rótulos `dataset` e `destination`; `dropped_total` acrescenta `reason`, de um
conjunto fechado. As séries existem desde a abertura da fila e vêm da mesma
estrutura de `datasets[].alarmDelivery` em `/v1/stats`. Contadores reiniciam
com o processo; gauges de disco são recuperadas dos arquivos. Motivos de
descarte: `memory_full`, `disk_full` (bytes ou quantidade), `event_too_large`,
`encoding_error`, `destination_failed`, `closed`. Perdas numa queda antes da
persistência não têm contagem exata recuperável.

| Métrica | Tipo | Significado |
|---|---|---|
| `nishidb_alarm_delivery_memory_events` | gauge | eventos aguardando persistência, incluindo a gravação em curso |
| `nishidb_alarm_delivery_memory_bytes` | gauge | bytes dos corpos no buffer, incluindo a gravação em curso |
| `nishidb_alarm_delivery_disk_events` | gauge | eventos duráveis ainda sem remoção confirmada |
| `nishidb_alarm_delivery_disk_bytes` | gauge | bytes dos arquivos publicados, incluindo cabeçalhos |
| `nishidb_alarm_delivery_oldest_pending_seconds` | gauge | idade do evento durável mais antigo |
| `nishidb_alarm_delivery_delivered_total` | counter | entregas HTTP 2xx com remoção durável confirmada |
| `nishidb_alarm_delivery_failed_attempts_total` | counter | tentativas HTTP malsucedidas |
| `nishidb_alarm_delivery_persistence_errors_total` | counter | erros locais que suspendem a fila |
| `nishidb_alarm_delivery_dropped_total` | counter | novos eventos descartados, por motivo |
| `nishidb_alarm_delivery_last_success_timestamp` | gauge | último sucesso confirmado, em segundos do epoch; zero antes do primeiro |
| `nishidb_alarm_delivery_active` | gauge | 1 enquanto o remetente está ligado, inclusive durante retentativa |
| `nishidb_alarm_delivery_failed` | gauge | 1 após falha local que exige intervenção e reinício |
| `nishidb_alarm_delivery_paused` | gauge | 1 quando o destino foi removido e a fila foi preservada |
