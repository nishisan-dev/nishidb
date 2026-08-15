# Aceite da Fase 6 — protocolo remoto HTTP

Este documento responde **"a Fase 6 está pronta?"** no mesmo formato do
[aceite do M1](aceite-m1.md) e do [M2](aceite-m2.md): cada item tem estado e
evidência, e a evidência é o **nome de um teste** da suíte ou um **número
medido** na rodada de aceite.

Números da rodada de 2026-08-14
(`benchmark/baselines/aceite-fase6-http-20260814.json`): broker Redpanda real
com 4 partições, 3,3 milhões de eventos produzidos, `nishidb server` no ar e
**744 consultas HTTP atendidas enquanto a ingestão materializava** — de
1.272.587 para 2.186.180 eventos aplicados durante a própria medição.

## Os entregáveis da Fase 6 (§118)

| Entregável | Estado | Evidência |
|---|---|---|
| `POST /v1/query` | **Atendido** | `TestQueryRespondeAMolduraCompleta`, `TestParametroPosicionalChegaAoMotor` |
| `GET /v1/metadata` | **Atendido** | `TestMetadataDescreveOQueODriverPrecisa`, `TestMetadataFiltraPorDataset` |
| `GET /v1/health` | **Atendido no M2** | `TestHealthTemAChaveKafka` |
| `GET /v1/stats` | **Atendido** | `TestStatsConcordaComOMetrics` |

## O que a spec exige, item a item

| # | Obrigação | Seção | Estado | Evidência |
|---|---|---|---|---|
| 1 | Protocolo remoto simples e **versionado**; HTTP aceitável | §67 | **Atendido** | Todas as rotas sob `/v1/`; o caminho é a versão |
| 2 | Os quatro endpoints | §67, §118 | **Atendido** | Tabela acima |
| 3 | Request com `dataset`, `sql` e `parameters` tipados | §68 | **Atendido** | `TestOsQuatroTiposDeWireViramValor`, `TestParametroPosicionalChegaAoMotor` |
| 4 | Resposta preferencialmente streaming; NDJSON aceitável | §69 | **Atendido** | `TestAPrimeiraLinhaChegaAntesDaUltimaSerProduzida`; **primeiro byte em 1,6 ms de um total de 171,8 ms** (9,9 MB, 100 mil linhas) |
| 5 | `NO query result buffering proporcional ao resultado` | §128 | **Atendido** | `TestOCustoEmVooNaoAcompanhaOTamanhoDoResultado`: **~40 KB de heap vivo tanto para 2.000 quanto para 40.000 linhas** |
| 6 | As quatro métricas de consulta do §92 | §92 | **Atendido** | `TestAsCincoSeriesDeQueryDoSpec92SaoAlimentadas` (as quatro mais `full_scans`) |
| 7 | Logs estruturados com os campos do §95 | §95 | **Atendido** | `query.concluida`, `query.recusada`, `query.falhou`, todos com `component`/`dataset`/`event`; nenhum SQL em INFO |
| 8 | Os doze query tests do §106 **pelo caminho HTTP** | §106 | **Atendido** | `TestSpec106PeloProtocoloHTTP` (um subteste por item) e `TestSpec106RotuloDaMolduraCasaComAsRows` |

Os doze casos do §106 já existiam contra o Engine, e repeti-los pelo protocolo
não é redundância: entre o motor e o cliente há projeção, alias, serialização e
moldura. Um campo quoted que chegasse ao cliente com o rótulo errado passaria
verde na suíte do motor e quebraria o `ResultSetMetaData` do driver.

## Os dois gates, e por que eles valem

Gate que não falha quando deveria não é gate. Os dois foram **aferidos** contra
uma mutação deliberada — um servidor que monta a resposta inteira antes de
enviá-la — e os dois a pegaram, enquanto `TestQueryRespondeAMolduraCompleta`
continuou passando (o conteúdo é o mesmo; só a temporização muda):

| Gate | Mede | Sob mutação |
|---|---|---|
| `TestAPrimeiraLinhaChegaAntesDaUltimaSerProduzida` | O cliente recebe uma row com `nishidb_query_active` ainda em 1 | Falha: `query_active = 0 durante o stream` |
| `TestOCustoEmVooNaoAcompanhaOTamanhoDoResultado` | Heap vivo com o cliente parado no meio do stream, contra o mesmo servidor sem consulta | Falha: a consulta termina antes de haver custo em voo para medir |

## A rodada com Kafka real

O que esta fase precisa provar não é que a consulta responde — é que ela
responde **com a ingestão de pé**, que é exatamente o que o débito 54 dizia não
ser possível.

| Medida | Valor |
|---|---|
| Consultas atendidas durante a ingestão | 744 (3.279 no total da rodada) |
| Eventos aplicados **durante** a medição | 1.272.587 → 2.186.180 |
| Linhas materializadas durante a medição | 495.152 → 586.978 |
| Point lookup (§89, teto 10 ms) | p50 **0,72 ms**, p95 **0,95 ms**, máx 2,7 ms |
| Consulta indexada (§90, teto 50 ms) | p50 **0,94 ms**, p95 **1,38 ms**, máx 2,0 ms |
| Full scan sobre 587 mil linhas | p50 66 ms, p95 196 ms, máx 228 ms |
| Eventos perdidos / falhas de ingestão | **0 / 0** |

O full scan degrada sob ingestão concorrente (p95 de 196 ms contra 69 ms com o
consumidor ocioso) e é o comportamento esperado: ele varre o dataset inteiro
disputando o mesmo storage que o writer está alimentando. A spec não fixa teto
para full scan, e os dois que ela fixa — §89 e §90 — passam com **uma ordem de
grandeza de folga**.

## O que a rodada expôs

**O lag fica congelado com a ingestão ociosa.** Terminado o consumo, o
`nishidb_kafka_lag` marcava 499.999 e permaneceu ali: o checkpoint — e o commit
de offset que vem depois dele (§48) — é disparado **por evento**, não por
tempo, então sem evento novo nenhum checkpoint acontece. Um único evento
produzido no tópico zerou as quatro partições na hora. Não é erro de
corretude (o checkpoint local é autoritativo, §49), mas quem olha o painel lê
"meio milhão de lag" num consumidor que está em dia. Registrado como **débito
56**.

## Como reproduzir

```bash
# broker
docker run --rm -d --name rp-fase6 -p 19092:19092 redpandadata/redpanda:v24.2.18 \
  redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M \
  --node-id 0 --check=false \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 \
  --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092
docker exec rp-fase6 rpk topic create nishidb-fase6 -p 4 --brokers 127.0.0.1:19092

# dataset apontando para o tópico, com source.kafka.enabled: true
nishidb server --config /var/tmp/nishidb-fase6/nishidb.yaml --log-format texto

# consultar COM a ingestão de pé — o que o débito 54 dizia não dar
curl -sS -H 'Content-Type: application/json' \
  -d '{"dataset":"radius_sessions",
       "sql":"SELECT id, status, nas_ip FROM radius_sessions WHERE status = ? LIMIT 3",
       "parameters":[{"type":"string","value":"Stop"}]}' \
  127.0.0.1:9142/v1/query

# o mesmo pelo caminho do operador
nishidb query --server 127.0.0.1:9142 --dataset radius_sessions \
  'SELECT id, status FROM radius_sessions LIMIT 3'
```

A suíte inteira roda sem broker: os testes de protocolo usam servidor de
verdade em porta livre, com dataset materializado pelo pipeline real.

```bash
make test && make test-race && make lint
```

## O que a fase NÃO fecha

| Item | Onde fica |
|---|---|
| Autenticação e TLS (§74-77) | Fase 11. O bind default virou loopback para que a exposição seja ato explícito, mas **não há autenticação**: quem alcança a porta lê o dataset |
| Driver JDBC (§70-73) | Fase 7, que consome este protocolo |
| `runtime.memoryTarget`, `maxBufferPerQuery` (§80) | Débito 15; só `maxConcurrent` ganhou leitor |
| `nishidb_storage_bytes` e as séries de liveness | Dependem do tally por segmento, que hoje é limite superior (débitos 38 e 45) |
| Formato binário de resposta | O §69 o cita como "futuramente" |
