# Aceite do M2 — consumidor Kafka

Este documento responde **"o M2 está pronto?"** sem depender de ninguém
lembrar do histórico, e no mesmo formato do [aceite do M1](aceite-m1.md): cada
item tem estado e evidência, e a evidência é o **nome de um teste** que existe
na suíte ou um **número medido** na rodada de aceite.

Números da rodada de 2026-08-14
(`benchmark/baselines/aceite-kafka-8m-20260814.json`), com 8 milhões de
entidades, **ingestão Kafka real** contra um broker Redpanda, corpus de
referência (85% de patch parcial, 10% Extended JSON, 1% campo desconhecido, 5%
null explícito) e 4 consultas SQL concorrentes com a ingestão.

## Os entregáveis da Fase 3 (§115)

| Entregável | Estado | Evidência |
|---|---|---|
| consumer | **Atendido** | `internal/kafka`; `TestSpec105DuplicateDelivery`, `TestIntegracaoConsomeERetomaPelaPosicaoLocal` |
| bounded queue | **Atendido** | `TestFilaBloqueiaNoGateDeRegistros`, `TestFilaBloqueiaNoGateDeBytes`, `TestSpec105QueueBackpressure` |
| offsets | **Atendido** | `checkpoint/kafka.offsets` com o nome literal do §50; `TestOffsetsRoundTrip`, `TestOffsetsSaoGravadosDEPOISDoManifest` |
| checkpoint | **Atendido** | Passo 8 do `Store.Checkpoint`, depois do manifest; `TestCrashEntreManifestEOffsetsDeixaAPosicaoAtras` |
| seek | **Atendido** | `AdjustFetchOffsetsFn`; `TestSpec105CrashAfterAppend` (o único que discrimina seek real de "usar o offset do broker") |
| recovery | **Atendido** | `TestSpec105OffsetRecovery`, `TestIntegracaoRetomadaAposEncerramentoAbrupto` |

## Os oito testes obrigatórios do §105

Todos existem, com o número da spec no nome do comentário, e rodam contra o
`kfake` em ~6 segundos:

| §105 | Teste |
|---|---|
| duplicate delivery | `TestSpec105DuplicateDelivery` |
| offset recovery | `TestSpec105OffsetRecovery` |
| multiple partitions | `TestSpec105MultiplePartitions` |
| crash before checkpoint | `TestSpec105CrashBeforeCheckpoint` |
| crash after append | `TestSpec105CrashAfterAppend` |
| restart | `TestSpec105Restart` |
| stale event | `TestSpec105StaleEvent` |
| queue backpressure | `TestSpec105QueueBackpressure` |

4 e 5 são cenários **distintos** e estão implementados como tais: o 4 é crash
antes de qualquer persistência de posição (o consumo recomeça do começo, sem
buraco); o 5 é crash depois do append e antes do checkpoint durável (o offset
regride e a duplicata tem de ser inócua).

## Os critérios do §126 que o M2 fecha

| # | Critério | Estado | Evidência |
|---|---|---|---|
| 2 | conectar ao Kafka | **Atendido** | `TestIntegracaoConsomeERetomaPelaPosicaoLocal` contra broker real em container |
| 3 | consumir JSON | **Atendido** | 8.000.000 eventos consumidos do tópico na rodada de aceite |
| 8 | sobreviver restart | **Atendido** | `TestSpec105Restart`; restart medido em **0,28 s** (§125) |
| 9 | **recuperar offsets** | **Atendido** | Era o "Parcial" do M1. `TestSpec105OffsetRecovery`: após reinício sem eventos novos, 0 eventos reprocessados |
| 11 | sustentar 500 eventos/s | **Atendido** | **139.249 ev/s** no LOAD com ingestão Kafka real (§88 exige ≥ 500; a meta é ≥ 2.000) |
| 12 | permanecer abaixo de 1 GiB RSS | **Atendido** | Pico de **491,0 MiB** com 8M rows, ingestão Kafka e 4 consultas SQL concorrentes |
| 25 | sobreviver `kill -9` sem perder estado confirmado | **Atendido** | `TestSpec105CrashAfterAppend` e `TestIntegracaoRetomadaAposEncerramentoAbrupto` provam que a retomada converge para o estado idêntico |

## Números medidos

| Medida | Valor | Teto/meta |
|---|---:|---|
| RSS de pico | 491,0 MiB | 1 GiB (§109) |
| RSS anônimo no fim | 12,5 MiB | — |
| RSS de arquivo (mmap) no fim | 402,7 MiB | — |
| Heap Go (8M entidades) | 2,3 MiB | vs 668,5 KiB com 100k: **80× de dados, 3,55× de heap** (§110) |
| Vazão do LOAD | 139.249 ev/s | ≥ 500 obrigatório, ≥ 2.000 meta (§88) |
| allocs/evento (ingestão pura) | 0,0206 | ≤ 9 (§111) |
| Restart | 0,28 s | §125 |
| Varredura completa de 8M | 21,03 s | — |
| Rows / segmentos / disco | 8.000.000 / 4 / 2,32 GiB | — |
| Lag Kafka ao fim | 0 | §92, §125 |
| Pico da fila (registros) | 5.000 | teto configurado 5.000 — o backpressure foi exercitado |
| Pico da fila (bytes) | 2,4 MiB | teto configurado 64 MiB |
| p95 point lookup (SQL) | 19 µs | 10 ms (§89) |
| p95 bitmap + LIMIT 100 | 321 µs | 50 ms (§90) |
| p95 intersect + LIMIT 100 | 469 µs | 50 ms (§90) |
| p95 union + LIMIT 100 | 342 µs | 50 ms (§90) |
| p95 full scan + LIMIT 100 | 2.223 µs | 50 ms (§90) |
| Erros de query | 0 | §106 |
| Eventos perdidos | 0 | §101 |

**Carga mista de verdade, pela primeira vez.** A fase de regime ofereceu 30.000
eventos e produziu **29.989 escritas efetivas** — 100%. Na rodada anterior o
mesmo teste produzia **12**, porque o gerador do regime reiniciava as
sequências e 29.988 eventos viravam stale no gate do §25: o relatório publicava
"carga mista" descrevendo consulta com escrita quase nenhuma. Era o débito M2
#3, e a correção foi o `radiusgen.Config.SeqOffset` que o próprio débito
indicava.

## Decisões que a spec não tomou

O M2 fechou 17 decisões que a issue #1 levantou. As que mudam comportamento
observável estão aqui; as demais estão no plano
(`planning/m2-plano-implementacao.md`).

| Decisão | Escolha | Por quê |
|---|---|---|
| Onde o offset é gravado | `checkpoint/kafka.offsets`, **depois** do `manifest.bin` | São dois renames sem transação. Nessa ordem, um crash no meio deixa a posição ATRÁS dos dados (reprocessa, converge). Na inversa, os eventos do meio nunca mais seriam lidos |
| `errors.invalidEvent: stop` | **Pausa observável**, não término | §51 põe as consultas antes da ingestão e §82 permite vários datasets por instância: terminar derrubaria consultas alheias. E como o offset só avança após checkpoint durável, o evento que parou é o primeiro após a posição persistida — reiniciar reproduz o mesmo evento e para de novo, o que sob supervisor é crash loop |
| `STORAGE_ERROR` | Nasce `EVENT_TOO_LARGE` para os tetos de FORMATO; o resto sempre pausa | Um atributo de 70 KiB num campo com bitmap é culpa do evento e caía no balde do "bug nosso". Num daemon a escolha seria entre perda invisível (avançar o offset) e livelock (não avançar) |
| Ativação do consumer | `enabled: false` por default **e** ato de comando | O fixture do §12 aponta para `kafka01:9092`. Ligar por presença do bloco subiria um consumer real contra um cluster inexistente na primeira troca de binário |
| Multi-instância | Recusa a subida com assignment parcial | Single-writer (§27) + checkpoint local autoritativo (§49) implicam uma instância ativa por group. Sem a guarda, duas instâncias materializam metade cada uma e nenhuma métrica do §92 acusa |
| Posição perdida | Dataset com linhas e sem offsets recusa a subida | `earliest` reprocessaria o tópico retido inteiro e `latest` pularia tudo, os dois em silêncio |
| Identidade da origem | Gravada no `kafka.offsets`; divergência recusa nomeando o campo | Offset é número sem significado fora do seu fluxo: promover teste→produção com o mesmo nome de tópico faria seek num offset alheio |
| `brokerOffsetCommit`, `clientId` | Não existem | Uma chave inventada a menos cada. O commit é passo normativo do §48; o `clientId` não distingue instância |
| Ordem Kafka como sequence (§26) | Sem mudar a assinatura do `Apply` | O pipeline de uma goroutine de ponta a ponta já preserva a ordem por partição |

## O que os números NÃO dizem

Está aqui porque um documento de aceite que só afirma é um documento que não dá
para auditar.

- **A rodada roda o consumer, não o writer de produção.** O harness usa o
  cliente franz-go e a fila limitada de verdade — que é o que pesa na memória e
  o §79 manda expor —, mas o laço de aplicação é o do próprio harness. As
  ramificações de política do `kafka.Writer` (log-and-skip, `stop`,
  `STORAGE_ERROR`, writer falho) têm teste de unidade próprio e **não** entram
  neste número.
- **O lag do relatório commita o que foi POLLADO.** A bancada não é o motor: o
  consumer de produção só commita depois do checkpoint durável (§48). A
  diferença é de alguns registros e irrelevante para a ordem de grandeza que o
  §125 quer, mas os dois números não são a mesma coisa. É o débito 52.
- **O `kill -9` de processo não foi reexecutado com Kafka.** O que foi provado é
  o CONTRATO — retomada a partir de um ponto durável coerente, com o
  reprocessamento convergindo. O SIGKILL real continua exercitado em
  `internal/cli/kill9_test.go`, sem Kafka.
- **Uma instância, um broker, quatro partições.** Rebalance com múltiplas
  instâncias é recusado por projeto (ver a decisão de multi-instância), então
  não há número de rebalance a publicar.
- **`storage.durability` continua aceita e ignorada** (débito 50). Quem lê
  `interval: 100ms` conclui "perco no máximo 100ms"; no caminho Kafka a janela
  de reprocessamento é a do `storage.checkpoint.interval`.

## Como reproduzir

```bash
# broker
docker run --rm -d --name rp -p 19092:19092 redpandadata/redpanda:v24.2.18 \
  redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M \
  --node-id 0 --check=false \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 \
  --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092
docker exec rp rpk topic create nishidb-bench -p 4 --brokers 127.0.0.1:19092

# baseline do gate §110 (rodada MENOR)
go run ./benchmark/ingest-bench -data /var/tmp/nishidb-bench \
  -entities 100000 -checkpoint-every 50000 \
  -kafka 127.0.0.1:19092 -kafka-group bl -report baseline-100k.json

# rodada de aceite
go run ./benchmark/ingest-bench -data /var/tmp/nishidb-bench \
  -entities 8000000 -checkpoint-every 500000 -steady 60s -rate 500 \
  -sql-queries 4 -kafka 127.0.0.1:19092 -kafka-group aceite \
  -heap-baseline baseline-100k.json -report aceite-8m.json
```

`-data` precisa apontar para **disco real**: em tmpfs o RSS de arquivo vira RAM
e a medição perde o sentido.

Os testes de integração usam o mesmo broker, subido pelo próprio teste:

```bash
make test-integration
```
