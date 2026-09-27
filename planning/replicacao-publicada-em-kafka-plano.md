# Replicação publicada em Kafka por destino nomeado

## Contexto

O feed de replicação master/follower (`internal/replication`, v0.12.0) só é
servido por HTTP a followers. O usuário quer que o master também publique esse
feed num tópico Kafka, em JSON, escolhendo o destino por nome
(`replication.targets: [nome]`), reaproveitando a declaração de destinos que os
contadores já usam. O TEMS (Network Alarm Console) vai consumir esse tópico para
o dataset `events` e já abriu o contrato em `/tmp/nishidb-cdc-kafka-chat.md`.

Decisões do usuário (fechadas):
1. Perda de continuidade (restart do master = epoch nova, ou lacuna por buffer
   estourado) → o publicador emite `reset` + snapshot completo no tópico +
   `snapshot_end`, e segue o tail. O tópico se basta; o consumidor nunca precisa
   de HTTP/ADMIN.
2. Um registro Kafka por Entry, chave Kafka = key da entidade (history: rowID).
3. Delete leva o último estado da linha (vale para o feed inteiro: follower e Kafka).
4. `seriesDelivery.kafka` (já lançado no v0.11/v0.12) vira `kafkaTargets:`; a chave
   velha é RECUSADA na subida com a mensagem de rename (quebra registrada no changelog).
5. `record` omite colunas ausentes (NULL = ausência).
6. Sem tombstone Kafka; o tópico é por retenção de tempo.

Achados da revisão (entram no escopo):
- Compaction faz expiração oportunista sem `emitChange` (`internal/storage/compaction.go:217-230`)
  → deletes por TTL nunca chegam ao feed. Bug existente; corrigir.
- O snapshot HTTP (`internal/server/replication_http.go:65-107`) materializa o
  dataset inteiro em memória e não confere `it.Err()` → snapshot truncado com
  200 faz o follower podar linhas vivas. Corrigir no refactor do exportador.
- `Stream.Acquire` devolve `ErrClosed` antes de servir os lotes retidos
  (`stream.go:334`) → o shutdown perde o tail.
- `invalidateLocked` não conta a perda de intervalo (invisível hoje).

## Desenho

### Feed (`internal/replication`, `internal/storage`)
- `store.go:2032-2043`: com `FlagTombstone`, publicar `a.prevPayload` (último
  estado vivo); fallback para `a.payload` se nil, sem falhar o writer.
- `compaction.go:217`: `emitChange: expirou` (copy-forward segue false).
- `Entry.At` novo, carimbado em `Publish` (a coalescência guarda o último).
- `Acquire` serve os lotes retidos depois de `Close` e só então devolve `ErrClosed`.
- Contador de invalidações em `Status`.
- Novo `internal/replication/snapshot.go`: `ExportSnapshot(ctx, feed, tables,
  chunkRows, begin, emit)` com a interface `SnapshotSource` (métodos que o
  `*storage.Store` já tem), capturando `(epoch, cursor)` ANTES do primeiro
  `OpenSnapshot`, com snapshots curtos por fatia de rowIDs (novo
  `Store.ScanLiveFrom(snap, from)`), conferindo `it.Err()` e fechando o
  snapshot em todo caminho. O handler HTTP passa a usar o exportador em
  streaming (o wire é o mesmo); se der erro no meio, aborta com
  `http.ErrAbortHandler`.

### Config
- `internal/config/series_delivery.go` → `kafka_targets.go`:
  `Config.KafkaTargets []KafkaTargetSpec` (`yaml:"kafkaTargets"`: name,
  bootstrapServers, topic, timeout, maxMessageBytes novo, security).
  `ErrKafkaTarget` mapeado explicitamente para `ExitUsageError` em `internal/cli/cli.go`.
  Guarda que recusa `seriesDelivery:` com "renamed to kafkaTargets" (o decoder
  global ignora chave desconhecida, `config.go:291`).
- `seriesdelivery.Open` passa a ler `cfg.KafkaTargets`; `emit.target` não muda;
  as métricas `nishidb_series_kafka_*` mantêm o nome.
- `internal/schema/spec.go` (ReplicationSpec/rawReplication/parse 1199-1256):
  `Targets []string`; recusado com `enabled` falso, `role: follower`, nome duplicado ou inválido.
- Validação na subida, antes dos locks (mesmo ponto de `server.go:338`): alvo
  desconhecido, alvo compartilhado entre contador e replicação ou entre dois
  datasets, falha do `ResolveSecurity` → recusa.

### Publicador (`internal/replicationkafka`, novo; importado só por `internal/server`)
- Tipo `Publisher`, uma goroutine por dataset × destino; depende de
  `replication`, `schema`, `query/executor` (`AppendValue`/`AppendJSONString`)
  e `kafka.ResolveSecurity`. Não importa `storage`.
- Producer confiável: idempotente, `AllISRAcks`, retries padrão,
  `AllowIdempotentProduceCancellation` + `RecordDeliveryTimeout`,
  `ProducerBatchMaxBytes(maxMessageBytes)`, buffers limitados.
  Particionador: `BasicConsistentPartitioner` — controle vai à partição que o
  publicador escolhe; o resto usa o StickyKeyPartitioner (murmur2).
- Laço: se precisa de snapshot → `reset` em TODAS as partições (barreira de
  ack) → snapshot (`retain` em todas as partições antes das linhas de history)
  → `snapshot_end{cursor,count}` em todas → tail por `Acquire`. O cursor só
  avança (`release()` + `cursor=b.To`) depois do ack de TODOS os registros do
  lote; os que falharam são reproduzidos na ordem original, com backoff de
  200 ms a 5 s. `ErrResyncRequired` → novo snapshot (reason `gap`). Checagem
  periódica do número de partições (kadm) → reset `partitions_changed`.
- Baldes de erro: infra → retenta para sempre (estado `retrying`); bug/render
  → `failed`, publicador para e a ingestão segue; mensagem acima do limite →
  `blocked`, nunca pula.
- Shutdown: contexto de vida próprio; em `encerra()`: `d.replication.Close()`
  → `publisher.Close(deadline)` (drena o tail e faz `Flush` limitado) → `closeDatasets`.

### Envelope (JSON; sem `fmt`/`encoding/json` no encoder, gate de AST)
`{"epoch","sequence","dataset","table","op","key","at","record"}`
- op ∈ `insert|update|delete|snapshot|retain|reset|snapshot_end`; `Schema`/`Reset` do feed → erro de bug.
- `key`: string; na history, rowID em decimal. `at`: formato de timestamp da query (UTC, ms).
- `record` = linha do `SELECT *` (alias > nome; sem colunas de sistema nem derivadas; ausente omitido).
- Controle (`reset`, `snapshot_end`, `retain`): chave fixa `__nishidb_control__`, com `cursor`/`count`/`floor`.
- Header `nishidb-op`. As sequências têm buracos (coalescência) — documentado.

### Métricas e `/v1/stats`
- Publicador (`dataset`, `destination`): `nishidb_replication_kafka_{delivered_total,
  lag_sequences, snapshots_total{reason}, errors_total{reason}, state{state},
  last_success_timestamp}`.
- Feed: `nishidb_replication_feed_{input_entries_total, output_entries_total,
  buffered_bytes, evicted_batches_total, invalidations_total}`.
- Todas as séries nascem em zero; `DatasetStats.Replication` no `/v1/stats` lê a mesma estrutura do `/metrics`.

### Docs (no mesmo commit do código)
`docs/replication.md` (seção Kafka: config, envelope, regras do consumidor,
dimensionamento de `maxBufferedBytes` contra o livelock do snapshot, delete
com último estado), `docs/operacao-series-kafka.md`, `README.md`,
`docs/metricas.md`, `docs/glossario.md` (destino Kafka, geração do feed,
publicador da replicação, marcador de reset, fim do snapshot; corrigir
"Sem compat."), `docs/diagrams/replication_kafka_flow.puml` + saída no
`c4_container.puml`, `docs/debitos.md` (follower decodifica o snapshot inteiro;
`masterURL` aceito no master; expiração da compaction no follower pelo relógio local).

## Execução

0. Antes do código: responder o TEMS por acréscimo em
   `/tmp/nishidb-cdc-kafka-chat.md` (autor `nishidb`) com o contrato acima,
   avisar por SendMessage; copiar este plano para `planning/`; branch
   `feature/replication-kafka-publisher` a partir de `main`.
1. Refutar o plano (Opus) contra o código antes do primeiro Builder.
2. Builders (Opus, pacotes disjuntos, sem commit; glossário proibido a eles),
   Refuter por bloco; commits por bloco, com `git add` explícito:
   1. correções do feed;
   2. exportador + `ScanLiveFrom` + HTTP em streaming;
   3. rename para `kafkaTargets` + recusa da chave velha;
   4. `replication.targets` + checagens na subida;
   5. envelope + gates;
   6. publicador + testes kfake;
   7. wiring no server, stats e métricas;
   8. glossário, diagrama e docs restantes.
3. Todos os comandos Go com `GOTOOLCHAIN=go1.26.5 GOPROXY=off`.

## Verificação

- Toda garantia tem teste com mutação aplicada → vermelho → revertida → verde,
  relatada (lista de 36 testes da revisão: delete com último estado,
  expiração pela compaction, replay não publica, `Acquire` pós-Close, cursor
  capturado antes do scan, `it.Err()`, snapshot fatiado converge sob escrita,
  golden do envelope, `record` = `SELECT *`, zero alocação, controle em todas as
  partições, cursor só avança com todos os acks, reprodução preserva ordem por
  chave, oversize bloqueia sem pular, drenagem no shutdown, shutdown limitado
  com broker fora, recusa de `seriesDelivery:` com a própria mensagem, séries em zero, stats = metrics).
- `make build`, `make test`, `make test-race`, `make lint` (baseline igualada
  via `git archive`), `make test-jdbc` / `make test-python` (stats aditivo).
- E2E no ambiente de dev: NishiDB com o mapping de 10.200.20.218, consumindo
  o tópico que o nstream já espelha de prod no broker 10.200.20.219, e
  publicando num tópico novo lá. Conferir: reset/snapshot/snapshot_end em
  todas as partições, tail contínuo, delete por TTL com último estado, restart
  → nova epoch + novo snapshot, lag e métricas; comparar a contagem do snapshot
  com um `SELECT count` no NishiDB.

## Correções da refutação (prevalecem sobre o texto acima)

1. **Tamanho de mensagem.** O publicador mede key+value+headers (com margem) contra
   `maxMessageBytes` ANTES do Produce. Se estourar, entra em `blocked`, sem produzir
   aquele registro nem nada depois dele na partição. O kgo falha só o registro
   grande e deixa os seguintes passarem (`producer.go:602-613`).
2. **Reprodução.** Numa partição com falha, reproduz a partir do PRIMEIRO registro
   falho até o fim do lote, inclusive os que já tiveram ack (at-least-once,
   last-write-wins). "Só os falhos" reordena delete e update da mesma chave.
3. **Particionador.**
   - `BasicConsistentPartitioner` com fn própria: para registros de controle, a fn
     devolve `int(r.Partition)` (o kgo não honra `r.Partition` sozinho).
   - Dados usam murmur2 com o `n` FIXADO no número de partições do último reset.
     Se o kadm vir outro número, dispara reset `partitions_changed`.
4. **ExportSnapshot.**
   - Teto fixo `limit₀` (LimitRowID do primeiro snapshot); cada fatia começa em
     `max(from, snap.BaseRowID)`.
   - A fatia é COPIADA e o snapshot FECHADO antes do `emit`.
   - Erro de `it.Err()` ou de `GetByRowID` (inclusive `ErrExpiredSnapshot`) aborta.
   - O Retain da history usa o `BaseRowID` do primeiro snapshot.
5. **Envelope.**
   - `sequence` de `snapshot` = `cursor₀` (nunca `Record.Sequence` nem 0).
   - O tail descarta `Entry.Sequence <= cursor` (como `resync.go:170`).
   - `snapshot_end.count` é por partição.
   - Reset por lacuna MANTÉM a epoch; a regra de poda do consumidor é
     `(epoch, sequence) < (epoch, cursor do reset)`. O contrato já foi corrigido
     no chat do TEMS.
6. **Shutdown.**
   - Ordem: fechar os feeds (a drenagem nova do Acquire) ANTES de
     `s.http.Shutdown`, para o long-poll não consumir os 15 s.
   - O `publisher.Close` tem orçamento próprio.
   - `feed.Close` e `publisher.Close` entram na lista `paradas` do gate
     `internal/gates/alarm_shutdown_test.go`, com a ordem asseverada.
7. **Acquire com `closed`.** Sem lote > after devolve `ErrClosed` imediato, nunca
   espera em `s.changed`.
8. **Record.** Percorre o payload com `encoding.FieldReader` + `FieldByID` (sem
   `FieldsByID` por registro); `replicationkafka` importa `internal/storage/encoding`.
   A key do registro Kafka (bytes crus) é autoritativa.
9. **Config.**
   - O campo `seriesDelivery` do wire do `/v1/stats` FICA (contrato lançado),
     registrado no changelog.
   - `maxMessageBytes` ganha motor também no `seriesdelivery` (substitui o
     `maxRecordBytes` fixo quando declarado).
   - A recusa de `replication.targets` (alvo desconhecido ou compartilhado)
     segue o sentinela/exit code do precedente de `emit.target` desconhecido.
   - Com `targets` declarados, `maxBufferedBytes >= 2*maxBatchBytes` é exigido
     (recusa na subida): o lease do publicador segura até um lote.
10. **Documentar:**
    - `op=update` na ressurreição;
    - delete de chave nunca vista;
    - linha vencida e não coletada some do snapshot antes do delete;
    - consumidor lembra `(epoch, sequence)` de chaves deletadas;
    - dimensionamento do lease;
    - débito do follower: relógio local na visibilidade e na compaction.

## Ondas de execução

- A (paralela): B1 correções do feed (replication/stream.go, storage/store.go,
  storage/compaction.go) ‖ B3 rename `kafkaTargets` (internal/config,
  internal/seriesdelivery, server/metrics_series_delivery*, cli.go, README, docs de séries).
- B (paralela): B2 exportador (replication/snapshot.go, storage ScanLiveFrom,
  server/replication_http.go) ‖ B4 `replication.targets` no schema ‖ B5 envelope
  (internal/replicationkafka/envelope*.go + gates).
- C: B6 publicador + testes kfake.
- D: B7 wiring no server (subida, shutdown, stats, métricas, gate de paradas).
- E: B8 glossário, diagrama, docs restantes, débitos.
- Cada onda: Refuter (Opus) reexecuta os testes e as mutações; commit por bloco
  com `git add` explícito, feito pelo orquestrador.
