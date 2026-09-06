# M2 — Consumer Kafka, offsets duráveis e observabilidade

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** fechar a Fase 3 da spec (§115: consumer, bounded queue, offsets, checkpoint, seek, recovery) e os itens §126.2/3/8/9/11/12/25 do critério de aceite.
**Architecture:** `Kafka Poller → Bounded Channel (registros + bytes) → Single Materialization Writer → Storage`, com offsets persistidos em `checkpoint/kafka.offsets` **depois** do `manifest.bin`, e um processo `nishidb server` que hospeda consumer + query engine + `/metrics` + `/v1/health`.
**Tech Stack:** Go 1.25, `franz-go v1.21.6` (+ `kadm` para lag/commit, `kfake` para unidade), `log/slog`, exposição Prometheus escrita à mão (sem dependência nova).

---

## 1. Contexto

O M1 fechou storage, materializer, SQL e CLI, mas a ingestão só existe por arquivo/stdin (`internal/cli/ingest.go`). Não há uma linha de Kafka no repositório.

O defeito que motiva a issue #1 não é a ausência — é o **silêncio**. `internal/schema/spec.go:239-241` copia `source.type` e `source.kafka` para dentro de um `map[string]any` opaco e não valida nada: `type` pode ser qualquer string, `kafka` pode ser qualquer mapa, e `spec.Source` não é lido em lugar nenhum fora do pacote. O fixture canônico do projeto — `testdata/radius.materialization.yaml`, cópia byte-a-byte do §12 e fixture de 10 arquivos entre CLI, query, materializer e os dois benchmarks — declara `bootstrapServers: [kafka01:9092, ...]`, `groupId`, `SASL_SCRAM-SHA-512` e nenhum comando reclama. O mesmo vale para o bloco `errors:` do §101, que **sequer é parseado** (`yaml.Unmarshal` puro, sem `KnownFields`): `errors: invalidEvant: stop` com typo passa limpo.

Resultado esperado ao fim do M2: um `nishidb server` consome um tópico real, sobrevive a `kill -9` sem perder estado confirmado, retoma pelo offset do checkpoint local (autoritativo, §49) e é observável o suficiente para ser operado às 3 da manhã.

**Estado da base:** o M1 completo vive em `feature/m1-query` (263 arquivos, 30 commits à frente) e **não está em `main`** — `main` só tem storage parcial. O M2 não pode começar antes do merge.

---

## 2. Decisões fechadas com o usuário

| # | Decisão | Escolha | Alternativa rejeitada |
|---|---|---|---|
| **G1** | Base git | Merge `feature/m1-query` → `main` (fast-forward), depois `feature/m2-*` a partir de `main` | Empilhar branches longas sobre M1 não mergeado |
| **G2** | Modelo de processo (D9) | `nishidb server` hospeda consumer **e** query engine no mesmo processo, com `/metrics` e `/v1/health` | Consumer standalone: o flock (`store_lock.go:44`) faria toda `nishidb query` sair exit 3 enquanto o consumer roda |
| **D15** | Ativação | `source.kafka.enabled` (default **`false`**) **e** ato de comando (`nishidb server`). Duas travas | Presença do bloco ligar o consumer — subiria um consumer real contra `kafka01:9092` no fixture do projeto na primeira troca de binário |
| **D1** | Persistência do offset | Arquivo `checkpoint/kafka.offsets` (nome literal do §50/§83), escrito **APÓS** o `manifest.bin`, como passo 8 do `Checkpoint()` sob `wmu` | Dentro do manifest: `manifest.go:166-168` recusa `version != 1` e `:212-217` recusa `kafkaCount != 0` **e** byte extra — qualquer das duas rotas faz binário M1 reportar "dataset corrompido" (exit 3). O arquivo separado mantém `kafkaCount = 0` e `version = 1`, e a ordem torna a única falha possível o offset **regredir** (reprocessa, inócuo), nunca avançar |
| **D6** | `errors.invalidEvent: stop` | **Pausa observável**: para a ingestão do dataset, mantém query de pé (§51 sobe query antes), `"kafka":"DOWN"` no health, métrica de evento bloqueado. Não mata o processo | Encerrar o processo: derruba as queries de todos os datasets (§82) e o evento envenenado é reproduzido a cada reinício → crash loop sob supervisor |
| **D13** | `STORAGE_ERROR` | Nasce a 6ª categoria `CategoryEventTooLarge` (`EVENT_TOO_LARGE`, `BadEvent()==true`) para os tetos de FORMATO; o `CategoryStorage` residual **sempre pausa** (não avança offset, não descarta em silêncio) | Logar e seguir: o offset avança e a perda é definitiva e invisível. Não avançar sem pausar: livelock por evento envenenado |
| **D10** | Multi-instância | Recusar a subida da ingestão se o assignment não cobrir **todas** as partições do tópico, com erro nomeando o group | Só avisar: mantém o modo de falha silencioso (duas instâncias dividem partições, cada dataset materializa metade, nenhuma métrica do §92 acusa) |
| **D11** | `autoOffsetReset` + offsets perdidos | Vale **só com dataset vazio**. `RowCount > 0` sem offsets ⇒ recusa subir a ingestão (query sobe), e a saída é o comando novo `nishidb offsets` | `earliest` reprocessaria o tópico retido inteiro e `latest` pularia tudo, os dois sem erro em camada nenhuma |

### Decisões restantes (fechadas por análise; sinalize se discordar)

| # | Decisão | Escolha | Por quê |
|---|---|---|---|
| **D2** | Cadência de checkpoint | Reusar `precisaCheckpoint` (`internal/cli/ingest.go:246-257`) e `storage.checkpoint.interval` (default 30s). **Nenhuma chave nova.** WARN estruturado na subida se `storage.durability` estiver presente, nomeando `checkpoint.interval` como a janela real de reprocessamento | §48 fixa a ordem, não a periodicidade. Implementar `durability.mode` não está no §115 — fica como débito, mas o silêncio de hoje (quem lê `interval: 100ms` conclui "perco 100ms") é corrigido pelo aviso |
| **D3** | `brokerOffsetCommit` | **Chave não existe.** Sempre commita no broker, depois do checkpoint durável. Falha de commit é não-fatal (log + métrica), porque o checkpoint local é autoritativo (§49) | O valor `disabled` suprimiria o passo final do fluxo do §48, que é normativo. Uma chave INVENTADA a menos |
| **D4** | `groupId` | **Obrigatório.** Unicidade de `(bootstrapServers canônico, topic, groupId)` validada na enumeração de `datasets.directory` pelo server — fora de `schema.validate`, que só enxerga um arquivo | `schema.Parse` vê um arquivo por vez (`spec.go:150`) e `SpecDoDataset` resolve um nome por vez (`config.go:180-183`): a checagem de conjunto não tem onde nascer sem a enumeração, que o server precisa de todo jeito |
| **D5** | Tail replay × offset | **Resolvido internamente, sem campo novo.** O tail replay só move dados para frente; o offset fica no último valor checkpointado. Logo offset ≤ dados sempre, e o seek reprocessa uma janela que converge | Ver D12: reaplicar na mesma ordem de partição converge para o mesmo payload; o custo é lixo (débitos 38 e 45), não divergência |
| **D7** | Onde `errors` mora | **Raiz do YAML do dataset** (como §101/§102 mostram). `internal/cli/ingest.go` passa a lê-lo no mesmo trabalho. `deadLetterTopic`: **aceito e validado no parse, recusado na ativação** com "não implementado nesta versão" | Doutrina única para o produto, a mesma já aplicada às env vars: chave da spec ainda não implementada é aceita no parse e recusada na ativação. Recusar no parse tornaria inválido o snippet literal do §102 e contrariaria `config.go:73-78` |
| **D8** | Erro de broker/rede | Tratado **antes e fora** do `Apply`, com balde próprio (`nishidb_kafka_errors_total` + health `"kafka": "DEGRADED"`). Retry/backoff do próprio franz-go. **Nenhuma chave nova** | `classify` tem `default: return CategoryStorage` (`materializer.go:363-364`): erro de rede que passasse pelo `Apply` seria contado como perda de dado. Tuning de rede é Fase 12 (§124) |
| **D12** | Ordem Kafka como sequence (§26) | **Sem mudança de assinatura do `Apply`.** Poller único → channel único → writer único preserva a ordem de recebimento, logo a ordem por partição. Decode workers **não** são implementados (§27 diz opcional, §28 diz que 500 ev/s não justifica) | A ordem por partição é garantida estruturalmente pelo pipeline, não por metadado dentro do `Apply`. O contrato "mesma entidade na mesma partition" é do produtor (§26) e é documentado |
| **D14** | Amarrar offset à origem | O `kafka.offsets` grava a **identidade da origem** (topic, groupId, cluster id, bootstrap canônico) no cabeçalho. Divergência ⇒ recusa a subida da ingestão nomeando o campo alterado. Troca legítima passa por `nishidb offsets` | Sem isso, promover teste→prod (mesmo nome de tópico, outro cluster) faz `seek` num offset de outro fluxo: começa no meio, sem erro em camada nenhuma |
| **D16** | Teto de memória | `consumer.ingestionQueueBytes` (default `64MiB`) como **segundo gate** do bounded channel, via semáforo de bytes. O que saturar primeiro bloqueia o poller | `maxPollRecords`/`ingestionQueueSize` contam registros, não bytes: 5000 slots × payload grande estoura o orçamento de 1 GiB sem nenhuma regra reclamar. §79 exige expor "Kafka buffers" e §108 exige a rodada de 8M com Kafka real |
| **D17** | `clientId` | **Fora do M2.** A spec não o nomeia e o default cogitado (`nishidb-<name>`) é idêntico em toda instância, logo não distingue instância — a única razão declarada para o campo existir | franz-go já define um default sadio |

---

## 3. Global Constraints (valem para TODAS as tarefas)

1. **Ordem do §48, jamais invertida:** `consume → materialize → persist → durable checkpoint → commit Kafka offset`.
2. **Single writer (§27, §128 `NO multi-writer v1`):** só a goroutine do writer chama `Materializer.Apply` e `Store.Put`. O `Materializer` reusa `patch`/`prevBuf`/`outBuf` e **não é thread-safe** (`materializer.go:191-207`).
3. **`NO unbounded queues` (§29, §128):** os dois gates (registros e bytes) são obrigatórios. Sob saturação: reduzir intake, **preservar membership**, manter heartbeats, não perder mensagens.
4. **Checkpoint local é autoritativo (§49):** o offset commitado no group é secundário e nunca posiciona o consumer no boot.
5. **Recovery na ordem do §51:** queries sobem **antes** de a ingestão retomar.
6. **§95:** nunca registrar senha nem payload completo. Credenciais só por `usernameEnv`/`passwordEnv`, resolvidas na ativação, nunca no parse.
7. **Sem `map[string]any` no caminho quente (§20, §100).**
8. **Binário único sem runtime externo (§97):** nada de CGO, logo nada de `confluent-kafka-go`/`librdkafka`.
9. **Idioma e estilo:** comentários, mensagens de erro e nomes de teste em PT-BR, no estilo argumentativo já existente (o comentário explica **por quê**, cita `§NN` e o débito que corrige). Sentinela por pacote + `errors.Is` na fronteira. Stdlib `testing`, zero testify.
10. **`go test ./...` e `go test -race ./...` verdes antes de qualquer commit.** `golangci-lint` sem warnings.

---

## 4. Schema final

### 4.1 `source` (raiz do YAML do dataset)

| Caminho | Tipo | Obrigatório | Default | Origem |
|---|---|---|---|---|
| `source` | bloco | não | ausente | §12 |
| `source.type` | enum fechado: `kafka` | sim, se `source` presente | — | §12 |
| `source.kafka` | bloco | sim, se `type: kafka` | — | §12 |
| `source.kafka.enabled` | bool | não | **`false`** | INVENTADO (D15) |
| `source.kafka.bootstrapServers` | lista `host:porta` | sim | — | §12 |
| `source.kafka.topic` | string | sim | — | §12 |
| `consumer.groupId` | string | **sim** | — | §12 (D4) |
| `consumer.autoOffsetReset` | enum `earliest`\|`latest` | não | `earliest` | §12 / enum INVENTADO |
| `consumer.maxPollRecords` | int > 0 | não | `1000` | §12 / default INVENTADO |
| `consumer.ingestionQueueSize` | int > 0 | não | `5000` | §12, §29 / default INVENTADO |
| `consumer.ingestionQueueBytes` | tamanho binário | não | `64MiB` | INVENTADO (D16) |
| `security.protocol` | enum `PLAINTEXT`\|`SSL`\|`SASL_PLAINTEXT`\|`SASL_SSL` | não | `PLAINTEXT` | §12 / enum INVENTADO |
| `security.saslMechanism` | enum `PLAIN`\|`SCRAM-SHA-256`\|`SCRAM-SHA-512` | sim com `SASL_*`, proibido sem | — | §12 |
| `security.usernameEnv` / `passwordEnv` | nome de env var | sim com `SASL_*` | — | §12 |
| `security.caFile` | caminho | não, só com `SSL`/`SASL_SSL` | CAs do sistema | INVENTADO |
| `errors.invalidEvent` | enum `log-and-skip`\|`stop` | não | `log-and-skip` | §101 / default INVENTADO |
| `errors.deadLetterTopic` | string | não | — | §102 (aceito no parse, recusado na ativação) |

Fora: `clientId` (D17), `brokerOffsetCommit` (D3), multi-tópico, tuning de rede (§124), `insecureSkipVerify`, segredo inline, mTLS, `startOffset`/`resetOffsets` como campo (viraram o comando `nishidb offsets`).

### 4.2 Regras de validação (em `schema.validate`, no estilo default-explícito → whitelist → `specErrf` com caminho completo da chave)

1. `source.type` vira **enum fechado**; `type: kafka` sem bloco `kafka` é erro, e bloco `kafka` com `type` diferente é erro.
2. Cada item de `bootstrapServers` passa por `net.SplitHostPort` (que trata `[2001:db8::1]:9092` por construção) + porta numérica em 1..65535. Split ingênuo por `:` tornaria cluster IPv6-only inconfigurável **com erro de parse**, impedindo até `nishidb query` de abrir o dataset.
3. `maxPollRecords`/`ingestionQueueSize` são `*int` no raw, com a regra de `inteiroPositivo` (`config.go:108-120`): ausente herda o default, `0` e negativo são recusados.
4. Coerência SASL: mecanismo/`usernameEnv`/`passwordEnv` obrigatórios com `SASL_*` e proibidos sem; `caFile` só com `SSL`/`SASL_SSL`.
5. **Env vars não são lidas no parse** — só o nome é validado. É isso que mantém `testdata/radius.materialization.yaml` válido byte-a-byte numa suíte que roda sem `KAFKA_USERNAME`/`KAFKA_PASSWORD`.
6. **Whitelist manual de chaves desconhecidas** dentro de `source` e de `errors` (blocos novos, sem legado). `KnownFields` não serve: no `yaml.v3` ele só existe em `*Decoder`, vale para o documento inteiro, e `Node.Decode` não herda a estritude — ligá-lo contrariaria a doutrina de `config.go:73-78`.

**Compatibilidade.** *Parse:* `testdata/radius.materialization.yaml` continua válido sem alteração (todo campo novo é opcional). *Comportamento:* com `enabled` default `false`, o fixture continua inerte, que é o único jeito de a suíte e os benchmarks do M1 não mudarem de significado. Presença de `source.kafka` **não impede** `nishidb ingest` sobre o mesmo dataset — o flock arbitra a exclusividade.

---

## 5. Formato do `checkpoint/kafka.offsets`

```text
0   4  magic "NDBK"
4   1  version = 1
5   3  reserved
       topicLen u16     | topic bytes
       groupLen u16     | groupId bytes
       clusterLen u16   | cluster id bytes (do kadm; vazio se o broker não expõe)
       bootstrapLen u16 | bootstrap canônico (ordenado, separado por vírgula)
    4  partCount u32
       por partição: partition i32 | offset i64   (offset = PRÓXIMO a consumir)
    4  crc32c (Castagnoli, sobre tudo acima)
```

- Escrita por `writeAtomic` — helper **extraído** de `checkpoint.Write` (`manifest.go:238-269`: tmp → fsync → rename → fsync do dir), preservando o seam `writeTrace`. Os dois arquivos passam a usar o mesmo caminho de durabilidade provada, em vez de duplicá-lo.
- Escrito **depois** do `manifest.bin`, como passo 8 do `Checkpoint()`. Crash entre os dois renames ⇒ offset regride ⇒ reprocessa ⇒ inócuo. **A ordem inversa é proibida** e travada por teste.
- Só existe quando há offsets: dataset sem Kafka nunca cria o arquivo.
- Cabeçalho corrompido ou CRC ruim ⇒ tratado como **ausente** e a ingestão é recusada se `RowCount > 0` (D11). Nunca falha o `Open` — query tem de subir (§51).

---

## 6. Ordem de execução e branches

```text
G0  merge feature/m1-query → main (fast-forward)
                │
     ┌──────────┴───────────┐
     ▼                      ▼
feature/m2-observabilidade   feature/m2-kafka
   (O1…O6)                   (K1…K13)
     └──────────┬───────────┘
                ▼
        feature/m2-server  (S1…S6)
                ▼
          Integração (I1…I5)
```

---

## 7. Tarefas

### G — Preparação

- [ ] **G0** Merge `feature/m1-query` em `main` (fast-forward; `main` é ancestral). Confirmar `go test ./...` e `go test -race ./...` verdes em `main` antes de ramificar
- [ ] **G1** `go get github.com/twmb/franz-go@v1.21.6`, `.../pkg/kadm@v1.18.0`, `.../pkg/kfake` (test-only). Confirmar build `CGO_ENABLED=0`
- [ ] **G2** Extrair `internal/units` com `Tamanho()` e `Duracao()` a partir de `internal/cli/config.go:126-173`, e apontar `config.go` e `schema` para lá (o schema precisa de `ingestionQueueBytes`; duplicar o parser seria a segunda fonte da verdade)

### O — Observabilidade (débitos M2 #4 e #5; §92, §93, §94, §95, §112)

- [ ] **O1** `internal/logging`: wrapper sobre `log/slog` emitindo os campos literais do §95 (`timestamp level component dataset event message`), com handler JSON e texto. Gate de teste: **nenhum campo de payload nem de senha chega ao handler**
- [ ] **O2** Trocar os dois `log.Printf` de `internal/storage/store_recovery.go:33-39` pelo logger estruturado (component=`storage`, event=`recovery.divergencia`)
- [ ] **O3** `internal/metrics`: registry com Counter/Gauge/Histogram e encoder do texto de exposição Prometheus escrito à mão (~120 linhas; §97 pede binário mínimo e o projeto tem 2 dependências). Nomes **literais** do §92
- [ ] **O4** Ligar aos contadores que já existem: `materializer.MetricsSnapshot` (`materializer.go:164-189`), `executor.Metrics`, `planner.Metrics`, `storage.Store.Stats()`
- [ ] **O5** Instrumentar `Checkpoint()`: `nishidb_checkpoint_duration_seconds` e `nishidb_checkpoint_last_success_timestamp` (§92 os exige e hoje não há nem tipo onde morar)
- [ ] **O6** `-pprof <addr>` no `nishidb server`, habilitado só em modo administrativo (§112)

### K — Kafka (§115: consumer, bounded queue, offsets, checkpoint, seek, recovery)

- [ ] **K1** `internal/schema`: substituir `SourceSpec{Type string; Kafka map[string]any}` (`spec.go:30-34`) pelas structs tipadas da §4.1, com camada `raw*` espelhada e ponteiro para separar ausente de vazio. Whitelist manual de chaves desconhecidas em `source` e `errors`
- [ ] **K2** `internal/schema`: validação da §4.2, com as mensagens nomeando o caminho completo da chave. Teste obrigatório: `testdata/radius.materialization.yaml` continua válido **byte a byte** e sai com `enabled == false`
- [ ] **K3** `internal/schema`: bloco `errors` na raiz (`invalidEvent` enum + default; `deadLetterTopic` validado na forma e marcado como não implementado)
- [ ] **K4** `internal/materializer`: nasce `CategoryEventTooLarge` (`EVENT_TOO_LARGE`) entre `CategoryTypeConversion` e `CategoryWriterFailed`; `BadEvent()` passa a cobri-la; os tetos de FORMATO (`types.MaxIndexedValueBytes`, key acima do teto) migram de `CategoryStorage` para ela. Ajustar `NumCategories`, `categoryNames`, e o comentário de `materializer.go:93-105` que pedia essa categoria. **Atualizar `internal/cli/loss_test.go`**, cujo `eventoGigante()` passa a ser descarte e não perda
- [ ] **K5** `internal/storage/checkpoint`: extrair `writeAtomic` de `Write` (`manifest.go:238-269`) e criar `SourceOffsets` + `WriteOffsets`/`LoadOffsets` com o formato da §5, incluindo a identidade da origem (D14). Teste de ordem de durabilidade espelhando `write_order_test.go:9`
- [ ] **K6** `internal/storage`: `Store.SetSourceOffsets` / `SourceOffsets()` como estado **writer-only** sob `wmu` (mesmo regime de `epoch` e `segTally`); `Checkpoint()` ganha o passo 8 `sourceOffsets` **depois** de `manifest`; `recoverFrom` (`store_recovery.go:19-29`) semeia a partir do arquivo. Atualizar a sequência esperada de `cpTrace` em `store_s11_fixes_test.go:36`. Teste: **crash entre os dois renames deixa o offset atrás dos dados, nunca à frente**
- [ ] **K7** `internal/kafka/config.go`: resolução da spec para config efetiva — env vars lidas aqui com falha rápida, TLS/SASL montados, `deadLetterTopic` recusado com "não implementado nesta versão"
- [ ] **K8** `internal/kafka/queue.go`: bounded channel com os dois gates (registros e bytes, D16). Poller bloqueia no envio ⇒ para de fazer fetch ⇒ membership e heartbeats preservados pelo próprio franz-go (§29). Métricas `nishidb_kafka_queue_depth` e `nishidb_kafka_queue_bytes` (§79)
- [ ] **K9** `internal/kafka/consumer.go`: cliente franz-go, poller único, `OnPartitionsAssigned` fazendo o **seek pelos offsets locais** (§49) e a **guarda de assignment total** (D10); `OnPartitionsRevoked` drenando a fila, checkpointando e commitando. Erro de broker fica no balde próprio, fora do `Apply` (D8)
- [ ] **K10** `internal/kafka/writer.go`: a goroutine única do §27. Drena a fila → `mat.Apply` → política de erro → cadência de checkpoint (`precisaCheckpoint` reusado) → `Store.Checkpoint()` → commit no broker. O offset de uma partição só avança para registros **decididos** (aplicados, stale ou descartados por política)
- [ ] **K11** Políticas de erro no writer: `BadEvent()` ⇒ `log-and-skip` ou `stop`; `CategoryStorage` ⇒ **sempre pausa** (D13); `Fatal()` ⇒ writer sticky, pausa e health DOWN. Pausa é observável: `"kafka": "DOWN"` no health + `nishidb_ingest_blocked` + log estruturado nomeando partição e offset
- [ ] **K12** `nishidb offsets --dataset X --config C [--show | --reset earliest|latest | --set p:o,...]` (D11/D6/D14). Toma o flock, logo exige o servidor parado. Reescreve identidade e offsets, com confirmação explícita do que será sobrescrito
- [ ] **K13** `internal/cli/ingest.go` passa a **ler `errors.invalidEvent`** em vez da política hardcoded de `:184-196`. `stop` no modo batch sai pelo novo `ExitParadoPorPolitica = 8`, encaixado em `gravidadeIngest` (`ingest.go:39-55`). No mesmo trabalho, `imprimeResumo` passa a quebrar `ActionReplaced` (débito 48), já que a função é tocada de todo jeito. Atualizar `outcome_internal_test.go`, `exitcodes_test.go`, `loss_test.go`, `summary_internal_test.go`, o help de `cli.go:47-73`, o README e `readme_test.go`

### S — Server (§78 `server:`, §93, §94, §96)

- [ ] **S1** `internal/cli/config.go`: parsear o bloco `server:` (`enabled`, `bind`, `port`) e fechar os débitos de chave-sem-leitor adjacentes — `version:` do `nishidb.yaml` (débito 47) e `query.resultBatchSize` (débito 10)
- [ ] **S2** `Config.ListaDatasets()`: enumeração de `datasets.directory` (hoje inexistente — `SpecDoDataset` resolve um nome por vez). Valida unicidade de `(bootstrapServers canônico, topic, groupId)` entre datasets (D4)
- [ ] **S3** `internal/server/server.go`: ciclo de vida na ordem do §51 — abre os Stores, registra no `query.Engine`, **sobe as queries**, e só então sobe os consumers dos datasets com `enabled: true`. Shutdown gracioso: para os pollers, drena, checkpointa, commita, `Close()` (que faz quiesce e devolve o flock, `store.go:1188-1224`)
- [ ] **S4** `GET /v1/health` com o corpo do §94, incluindo a chave `"kafka"` (`UP` / `DEGRADED` / `DOWN`)
- [ ] **S5** `GET /metrics` servindo o registry do O3, com `nishidb_kafka_lag` e `nishidb_kafka_offset` alimentados via `kadm`
- [ ] **S6** `nishidb server --config ...` no dispatch de `cli.go:77-94`, com o help e a tabela de exit codes atualizados

### I — Integração e aceite

- [ ] **I1** Os 8 testes obrigatórios do §105 com `kfake` (unidade): `duplicate delivery`, `offset recovery`, `multiple partitions`, `crash before checkpoint`, `crash after append`, `restart`, `stale event`, `queue backpressure`. **4 e 5 são cenários distintos** — crash depois do append e antes do checkpoint durável (offset regride, duplicata tem de ser inócua) contra crash antes de qualquer persistência
- [ ] **I2** Testes de integração com Testcontainers/Redpanda sob a tag `//go:build integration` — o que também ressuscita `make test-integration`, hoje plumbing morto (débito 44)
- [ ] **I3** `benchmark/ingest-bench`: trocar `gen *radiusgen.Generator` (`harness.go:456-466`) por uma interface de fonte com `Next(dst []byte) []byte`, e acrescentar uma fonte Kafka real. Novos campos no `Report`: lag, profundidade e bytes da fila, `restart_time` (§125). Fecha os débitos M2 #2 e #3 — Kafka **é** a carga mista que o harness nunca produziu
- [ ] **I4** Rodada de aceite §108/§109: 8M rows sob 1 GiB **com ingestão Kafka real**, validando RSS, heap, GC, allocs/evento, mmap residency, concorrência de query e `Kafka ingestion`. Gate duro de §88 (≥ 500 ev/s)
- [ ] **I5** Documentação: `docs/architecture.md` (a seção "Observabilidade — não existe ainda" morre), `docs/diagrams/kafka_pipeline.puml` e `offset_recovery.puml`, `docs/debitos.md` (fechar M2 #1, #2, #3, #4, #5, #9, #10, 47, 48; abrir os novos), `docs/aceite-m2.md`, e o plano aprovado copiado para `planning/`

---

## 8. Desvios deliberados da spec

| Desvio | Justificativa |
|---|---|
| `source.kafka.enabled` não existe na spec | Sem ele, o fixture literal do §12 sobe um consumer real contra `kafka01:9092` na primeira troca de binário (D15) |
| `consumer.ingestionQueueBytes` não existe na spec | §79 exige expor "Kafka buffers" e §108 exige a rodada de 8M com Kafka real; contar registros não é unidade de memória (D16) |
| `invalidEvent: stop` = pausa, não término | §51 sobe query antes da ingestão e §82 permite múltiplos datasets por instância; terminar derrubaria queries alheias e criaria crash loop (D6) |
| 6ª categoria `EVENT_TOO_LARGE` (a spec lista 7 no §101) | O próprio código já pedia (`materializer.go:93-105`): teto de formato é culpa do evento e hoje cai num balde sem política nenhuma (D13) |
| Recusar assignment parcial (§29 fala de membership, não disso) | Single-writer (§27) + checkpoint local autoritativo (§49) implicam uma instância ativa por group; sem a guarda, o modo de falha é silencioso (D10) |
| `brokerOffsetCommit` e `clientId` não existem | Uma chave INVENTADA a menos cada; o commit é passo normativo do §48 e o `clientId` não distingue instância (D3, D17) |
| Novo exit code 8 (`ExitParadoPorPolitica`) | A tabela de exit codes é contrato declarado (`cli.go:18-35`); reusar o 7 ("falha que **não** é do evento") mentiria sobre a causa |

---

## 9. Riscos

| Risco | Mitigação | Teste |
|---|---|---|
| Offset avança além dos dados (perda definitiva) | Ordem `manifest.bin → kafka.offsets → commit no broker`, travada por seam de trace | I1 casos 4 e 5; teste de ordem espelhando `write_order_test.go` |
| Dataset M2 aberto por binário M1 | Branch A mantém `kafkaCount = 0` e `version = 1`: o manifest continua legível. O `kafka.offsets` é ignorado por quem não o conhece | Teste que decodifica o manifest de um dataset com offsets usando o decoder atual |
| Consumidor estoura o orçamento de 1 GiB | Gate de bytes (D16) + `Kafka buffers` no relatório do harness | I4 com gate duro de RSS |
| Reprocessamento após crash diverge o estado | Ordem por partição preservada pelo pipeline (D12); com `sequence`, o gate `incoming > current` basta | I1 `duplicate delivery` + property test de convergência |
| Troca de tópico/cluster com offset alheio | Identidade da origem no cabeçalho do `kafka.offsets`, recusa nomeando o campo (D14) | Teste de divergência de cada campo da identidade |
| Mudança de categoria (K4) quebra contrato do M1 | `loss_test.go` e `exitcodes_test.go` atualizados **no mesmo commit**, com o motivo escrito no teste | `go test ./...` + `readme_test.go` |

---

## 10. Verificação (Definition of Done)

- [ ] `go test ./...` e `go test -race ./...` verdes em todos os ramos
- [ ] `make test-integration` roda de verdade (Redpanda) e passa
- [ ] `golangci-lint run` sem warnings
- [ ] Os 8 testes do §105 existem, nomeados, e passam
- [ ] `kill -9` no `nishidb server` + reinício reproduz **estado idêntico**, com o reprocessamento duplicado inócuo (§126.25)
- [ ] Rodada de aceite: 8M rows, < 1 GiB RSS, ≥ 500 ev/s sustentados **com Kafka real** (§88, §108, §109, §126.11/12)
- [ ] `GET /metrics` expõe os nomes literais do §92; `GET /v1/health` traz a chave `"kafka"` (§93, §94)
- [ ] Logs estruturados com os campos do §95; nenhum teste consegue extrair senha ou payload completo do handler
- [ ] `testdata/radius.materialization.yaml` continua válido byte a byte e **inerte** (`enabled == false`)
- [ ] `docs/architecture.md`, os `.puml` novos e `docs/debitos.md` refletem o código; diagramas renderizam pelo proxy `uml.nishisan.dev`
- [ ] Plano aprovado copiado para `planning/m2-plano-implementacao.md`
