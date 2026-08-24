# NishiDB v1 — Especificação Técnica

## 1. Objetivo

Implementar um storage engine enxuto, escrito majoritariamente em **Go**, especializado em materialização de estado a partir de eventos Kafka.

O sistema deve:

* Consumir continuamente eventos JSON de um tópico Kafka.
* Sustentar inicialmente entre **300 e 500 eventos por segundo**.
* Manter aproximadamente **8 milhões de entidades materializadas**.
* Operar com **menos de 1 GiB de memória RSS** como meta arquitetural.
* Manter o uso de heap desacoplado da quantidade total de entidades.
* Materializar o estado corrente por uma chave configurável, por exemplo `id`.
* Aceitar eventos completos ou patches parciais.
* Persistir o estado localmente em formato binário compacto.
* Utilizar armazenamento append-only.
* Utilizar arquivos memory-mapped sempre que fizer sentido.
* Permitir seleção configurável dos campos armazenados.
* Permitir `*` para armazenar todos os campos encontrados.
* Permitir definição explícita dos tipos dos campos.
* Permitir criação declarativa de índices.
* Disponibilizar consultas através de SQL simples.
* Disponibilizar driver JDBC remoto para aplicações Java.
* Possuir autenticação e TLS para consultas remotas.
* Possuir recovery baseado no próprio log append-only e checkpoints.
* Possuir compactação automática.
* Não depender internamente de outro banco de dados ou storage engine.

O produto é deliberadamente especializado e não deve tentar se tornar um banco relacional genérico.

---

# 2. Princípio arquitetural

O NishiDB é conceitualmente:

> Um persistent materialized-state store alimentado por streams, schema-flexible, append-only, disk-first e consultável através de SQL simples.

Fluxo principal:

```text
                         Kafka
                           │
                           ▼
                  ┌────────────────┐
                  │ Kafka Consumer │
                  └───────┬────────┘
                          │
                          ▼
                  ┌────────────────┐
                  │ JSON Streaming │
                  │    Decoder     │
                  └───────┬────────┘
                          │
                          ▼
                Materialization Spec
                          │
                          ▼
                  ┌────────────────┐
                  │  Materializer  │
                  └───────┬────────┘
                          │
                          ▼
                   Single Writer
                          │
                          ▼
                Compact Binary Record
                          │
                          ▼
                 Append-only Segment
                          │
             ┌────────────┴─────────────┐
             │                          │
             ▼                          ▼
       Primary Index              Secondary Index
        mmap/disk                  mmap/disk
             │                          │
             └────────────┬─────────────┘
                          │
                          ▼
                    Query Engine
                          │
                  NishiDB Protocol
                          │
              ┌───────────┼───────────┐
              │           │           │
              ▼           ▼           ▼
            JDBC         HTTP         CLI
          Java Thin
           Driver
```

---

# 3. Filosofia de memória

O NishiDB deve ser desenvolvido segundo o princípio:

> Disk is storage. RAM is working memory and cache.

O design não pode depender de manter:

* todos os documentos em memória;
* todas as primary keys em heap;
* todos os índices em heap;
* todos os offsets em objetos Go;
* milhões de structs ou strings individuais gerenciadas pelo GC.

O seguinte tipo de arquitetura é proibido:

```go
map[string]*ServiceState
```

contendo milhões de entidades.

O uso do heap deve permanecer aproximadamente estável mesmo quando o dataset cresce de:

```text
1 milhão
```

para:

```text
8 milhões
```

ou mais entidades.

---

# 4. Meta de memória

Meta para o processo completo:

```text
RSS < 1 GiB
```

com:

```text
8.000.000 entidades materializadas
500 eventos/s
consultas concorrentes
compaction em background
```

Meta inicial de memória gerenciada pelo Go runtime:

```text
GOMEMLIMIT ≈ 384–512 MiB
```

Valor exato deve ser determinado por benchmark.

Configuração recomendada inicialmente:

```bash
GOMEMLIMIT=512MiB
GOGC=100
```

O processo deve reservar margem para:

* mmap;
* stacks;
* buffers de rede;
* Kafka;
* file cache residente;
* runtime;
* TLS;
* query buffers;
* estruturas nativas.

---

# 5. Stack

## Core

Implementar em:

```text
Go 1.25 ou superior
```

Utilizar prioritariamente:

* standard library;
* `os`
* `io`
* `bufio`
* `encoding/binary`
* `encoding/json`
* `net`
* `net/http`
* `crypto/tls`
* `hash/crc32`
* `sync`
* `sync/atomic`
* `context`
* `time`

Dependências externas devem ser mínimas.

---

# 6. Kafka

Utilizar cliente Kafka Go maduro.

Opções aceitáveis:

* franz-go;
* confluent-kafka-go, caso dependência nativa seja aceitável.

Preferência inicial:

```text
franz-go
```

para manter distribuição simples e evitar dependência de `librdkafka`.

---

# 7. YAML

Utilizar parser YAML pequeno e consolidado.

Exemplo:

```text
gopkg.in/yaml.v3
```

---

# 8. JDBC

O storage engine não será executado dentro da JVM.

O JDBC será um **thin remote driver escrito em Java**.

Arquitetura:

```text
Java Application
      │
      │ JDBC
      ▼
NishiDB JDBC Driver
      │
      │ NishiDB Protocol
      ▼
NishiDB Go Server
```

O driver JDBC não contém storage engine.

---

# 9. Escopo fora da versão 1

Não implementar:

* JOIN
* GROUP BY
* HAVING
* UNION
* CTE
* window functions
* subqueries
* foreign keys
* stored procedures
* triggers
* SQL UPDATE
* SQL INSERT
* SQL DELETE
* transações ACID multi-registro
* MVCC completo
* replicação
* clustering
* consenso distribuído
* sharding distribuído
* queries distribuídas
* PostgreSQL wire protocol
* MySQL wire protocol
* JDBC embedded
* índices full text

---

# 10. Unidade lógica

Cada entidade possui conceitualmente:

```text
serviceKey
rowId
lastSequence
state
physicalLocation
expiration
flags
```

Exemplo:

```text
serviceKey:
CMP-V0001PPCLP-013

rowId:
4183927

lastSequence:
7455409733

physicalLocation:
segment=127
offset=9283712
length=721
```

---

# 11. rowId

Cada entidade recebe um identificador numérico interno.

Tipo:

```go
uint64
```

O `rowId`:

* é criado apenas uma vez;
* permanece estável;
* não muda durante updates;
* não muda durante compaction;
* é utilizado por todos os índices secundários;
* pode ser preservado durante tombstones.

Exemplo:

```text
CMP-V0001PPCLP-013
        ↓
     4183927
```

---

# 12. Materialization Definition

Cada dataset deve possuir um YAML próprio.

Exemplo:

```text
datasets/radius.materialization.yaml
```

Exemplo:

```yaml
version: 1

name: radius_sessions

source:
  type: kafka

  kafka:
    bootstrapServers:
      - kafka01:9092
      - kafka02:9092
      - kafka03:9092

    topic: radius-accounting

    consumer:
      groupId: nishidb-radius
      autoOffsetReset: earliest
      maxPollRecords: 1000
      ingestionQueueSize: 5000

    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
      usernameEnv: KAFKA_USERNAME
      passwordEnv: KAFKA_PASSWORD

materialization:
  key:
    source: payload
    field: id
    type: string

  mode: merge

  sequence:
    field: _SEQ
    type: int64
    strategy: greater

  ttl:
    field: EventExpireDate
    type: timestamp

  tombstoneRetention: 7d

schema:
  include:
    - "*"

  unknownFields: include

  fields:
    id:
      type: string
      alias: service_id

    _id:
      type: string

    _SEQ:
      type: int64

    Acct-Authentic:
      type: string
      alias: acct_authentic
      index: bitmap

    Acct-Session-Id:
      type: string
      alias: session_id

    Acct-Status-Type:
      type: string
      alias: status
      index: bitmap

    Calling-Station-Id:
      type: string
      alias: calling_station_id

    Class:
      type: string

    Connect-Info:
      type: int64
      alias: connect_info

    Event-Timestamp:
      type: timestamp
      alias: event_timestamp

    EventExpireDate:
      type: timestamp
      alias: expiration

    Filter-Id:
      type: string
      alias: filter_id
      index: bitmap

    Framed-IPv6-Prefix:
      type: ipv6_prefix
      alias: ipv6_prefix

    Framed-Interface-Id:
      type: string

    Framed-Protocol:
      type: string
      alias: protocol
      index: bitmap

    Idle-Timeout:
      type: int64

    LastSessionStartedDate:
      type: timestamp

    NAS-IP-Address:
      type: ipv4
      alias: nas_ip
      index: hash

    NAS-Identifier:
      type: string
      alias: nas
      index: hash

    NAS-Port:
      type: int64

    NAS-Port-Id:
      type: string
      alias: nas_port_id

    NAS-Port-Type:
      type: string
      index: bitmap

    PSA-SERVER:
      type: ipv4

    Service-Type:
      type: string
      index: bitmap

    Session-Timeout:
      type: int64

    User-Name:
      type: string
      alias: username

    _insertedDate:
      type: timestamp

    _lastUpdate:
      type: timestamp

    Acct-Input-Gigawords:
      type: int64

    Acct-Input-Octets:
      type: int64

    Acct-Input-Packets:
      type: int64

    Acct-Output-Gigawords:
      type: int64

    Acct-Output-Octets:
      type: int64

    Acct-Output-Packets:
      type: int64

    Acct-Session-Time:
      type: int64

    Framed-IP-Address:
      type: ipv4
      alias: ip
      index: hash

    LastSessionAliveDate:
      type: timestamp

    Delegated-IPv6-Prefix:
      type: ipv6_prefix

    Acct-Terminate-Cause:
      type: string
      alias: terminate_cause
      index: bitmap

    LastSessionStoppedDate:
      type: timestamp

query:
  maxRows: 100000
  timeout: 30s
```

---

# 13. Seleção de campos

Todos os campos:

```yaml
schema:
  include:
    - "*"
```

Campos declarados explicitamente sobrescrevem inferências.

---

# 14. Seleção explícita

```yaml
schema:
  include:
    - id
    - _SEQ
    - Acct-Status-Type
    - Framed-IP-Address
    - NAS-Identifier
```

Campos não listados são ignorados.

---

# 15. Exclusão

```yaml
schema:
  include:
    - "*"

  exclude:
    - debug
    - temporaryData
```

---

# 16. Campos desconhecidos

Suportar:

```yaml
unknownFields: include
```

```yaml
unknownFields: ignore
```

```yaml
unknownFields: reject
```

---

# 17. Tipos

Versão 1:

```text
string
int64
double
boolean
timestamp
ipv4
ipv6
ipv4_prefix
ipv6_prefix
bytes
json
datetime
time
auto
```

`datetime` e `time` exigem `format:` — um padrão de tokens fechados, validado
no load. `datetime` é wall-clock sem fuso, resolvido para instante pelo fuso
do host; `time` é hora do dia, e não é instante.

---

# 18. Representação interna

Preferências:

```text
int64       → 8 bytes
double      → 8 bytes
boolean     → 1 byte
timestamp   → int64 epoch millis
datetime    → int64 epoch millis (resolvido em time.Local na ingestão)
time        → int64 millis desde a meia-noite, [0, 86400000)
ipv4        → uint32
ipv6        → 16 bytes
```

Strings permanecem variáveis.

---

# 19. Mongo Extended JSON

Reconhecer:

```json
{
  "$numberLong": "66952"
}
```

como:

```text
int64
```

E:

```json
{
  "$date": "2026-08-11T22:28:10.000Z"
}
```

como:

```text
timestamp
```

Os wrappers Mongo não devem ser armazenados.

---

# 20. Parsing JSON

Não utilizar como modelo principal:

```go
map[string]any
```

O parser deve evitar materializar todo evento em árvores genéricas.

Preferir:

```text
Kafka []byte
     ↓
tokenizer/streaming decoder
     ↓
field lookup
     ↓
typed encoder
     ↓
reusable record buffer
```

O objetivo é minimizar:

* allocations;
* strings temporárias;
* interfaces;
* garbage collector pressure.

---

# 21. Buffers reutilizáveis

O writer deve possuir buffers reutilizáveis.

Exemplo conceitual:

```go
type RecordBuffer struct {
    Data []byte
}
```

O processamento de eventos deve reutilizar capacidade sempre que possível.

Pode utilizar:

```go
sync.Pool
```

somente quando benchmark demonstrar benefício real.

Não utilizar pools indiscriminadamente.

---

# 22. Materialização merge

```yaml
materialization:
  mode: merge
```

Estado:

```json
{
  "id": "ABC",
  "_SEQ": 100,
  "status": "Start",
  "counter": 5000
}
```

Patch:

```json
{
  "id": "ABC",
  "_SEQ": 101,
  "counter": 9000
}
```

Resultado:

```json
{
  "id": "ABC",
  "_SEQ": 101,
  "status": "Start",
  "counter": 9000
}
```

---

# 23. Replace

```yaml
materialization:
  mode: replace
```

O evento recebido substitui completamente o estado anterior.

---

# 24. NULL

Em `merge`:

```json
{
  "Framed-IP-Address": null
}
```

significa:

```text
set field to NULL
```

Campo ausente significa:

```text
keep previous value
```

---

# 25. Sequence

```yaml
sequence:
  field: _SEQ
  type: int64
  strategy: greater
```

Aplicar somente quando:

```text
incoming > current
```

Evento igual ou inferior deve ser descartado.

---

# 26. Kafka ordering

Na ausência de sequence:

* utilizar ordem Kafka;
* exigir que eventos da mesma entidade sejam publicados para a mesma partition;
* recomendar Kafka key igual à materialization key.

---

# 27. Arquitetura de ingestão

Utilizar **Single Writer** na versão 1.

```text
Kafka Poller
    │
    ▼
Decode Workers opcional
    │
    ▼
Bounded Channel
    │
    ▼
Single Materialization Writer
    │
    ▼
Storage + Indexes
```

Somente uma goroutine realiza mutations definitivas.

---

# 28. Goroutines

Modelo sugerido:

```text
Kafka poller goroutine

N decode goroutines, se necessário

1 writer goroutine

N query goroutines

1 checkpoint coordinator

1 compaction coordinator

1 TTL coordinator
```

A ingestão alvo de 500 eventos/s não justifica múltiplos writers.

---

# 29. Backpressure

Utilizar channel limitado.

Exemplo:

```yaml
ingestionQueueSize: 5000
```

Nunca permitir crescimento ilimitado de fila.

Quando saturado:

* reduzir intake;
* preservar Kafka membership;
* manter heartbeats;
* não perder mensagens.

---

# 30. Storage

Arquivos append-only:

```text
data/
    segment-00000001.dat
    segment-00000002.dat
    segment-00000003.dat
```

Tamanho padrão:

```text
512 MiB
```

---

# 31. Record format

Formato conceitual:

```text
MAGIC
FORMAT_VERSION
FLAGS
ROW_ID
SEQUENCE
FIELD_COUNT
PAYLOAD_LENGTH
CRC32C
PAYLOAD
```

Exemplo inicial:

```text
4 bytes     magic
1 byte      formatVersion
1 byte      flags
8 bytes     rowId
8 bytes     sequence
4 bytes     fieldCount
4 bytes     payloadLength
4 bytes     CRC32C
N bytes     payload
```

---

# 32. Field dictionary

Manter:

```text
metadata/fields.dict
```

Exemplo:

```text
1 id
2 _id
3 _SEQ
4 Acct-Status-Type
5 Framed-IP-Address
```

O nome textual não deve ser repetido em todos os registros.

---

# 33. Encoding de campo

Conceitualmente:

```text
FIELD_ID
TYPE
LENGTH
VALUE
```

Campos fixed-width podem omitir `LENGTH`.

---

# 34. mmap

Arquivos apropriados devem utilizar memory mapping.

Especialmente:

```text
primary.idx
locations.idx
sequences.idx
bitmap indexes
fixed-size hash indexes
```

Os arquivos mapeados não devem ser copiados integralmente para heap.

---

# 35. Primary index

Mapeia:

```text
serviceKey -> rowId
```

Implementar hash table open-addressing.

Formato conceitual:

```go
type PrimarySlot struct {
    Hash  uint64
    RowID uint64
}
```

Tamanho:

```text
16 bytes/slot
```

Para 8 milhões de keys com load factor aproximado de 70%:

```text
~11,43 milhões de slots
~183 MB de arquivo
```

Esse arquivo deve ser disk-backed/mmap.

---

# 36. Hash collision

O hash nunca é considerado prova suficiente de igualdade.

Colisão deve ser validada contra a key real armazenada.

---

# 37. Location index

Mapeia:

```text
rowId -> physical location
```

Formato sugerido:

```go
type Location struct {
    Offset    uint64
    SegmentID uint32
    Length    uint32
}
```

Tamanho:

```text
16 bytes
```

Para 8 milhões:

```text
~128 MB
```

disk-backed.

---

# 38. Sequence index

Se habilitado:

```text
rowId -> int64 sequence
```

8 milhões:

```text
~64 MB
```

disk-backed.

---

# 39. Estado

O estado materializado vive nos segmentos.

Não manter representação completa permanente em memória.

Para update:

```text
lookup rowId
    ↓
read previous binary record
    ↓
merge using reusable buffer
    ↓
append new record
    ↓
publish new location
```

---

# 40. Índices secundários

Tipos v1:

```text
bitmap
hash
```

---

# 41. Bitmap indexes

Usar para baixa cardinalidade.

Exemplo:

```text
status
protocol
serviceType
terminateCause
```

Um bitmap de 8 milhões de rows:

```text
~1 MB
```

Os bitmaps devem ser file-backed sempre que possível.

---

# 42. Hash secondary indexes

Utilizar apenas quando explicitamente configurados.

Devem ser tratados como recursos potencialmente caros.

Campos de alta cardinalidade como:

```text
sessionId
username
callingStationId
```

não devem ganhar índice automaticamente.

---

# 43. Budget de índices

A engine deve calcular estimativa de footprint de índices.

No startup, informar:

```text
dataset: radius_sessions
estimated rows: 8,000,000

indexes:
  status(bitmap): ~3 MB
  protocol(bitmap): ~3 MB
  nas(hash): ~...
  ip(hash): ~...
```

Não é necessário impedir startup, mas deve ser possível gerar warning.

---

# 44. Query correctness

Índice produz candidatos.

O predicate final deve ser novamente validado contra o estado corrente.

---

# 45. Atomicidade

Atualização deve se tornar visível atomicamente por row.

Leitor observa:

```text
old state
```

ou:

```text
new state
```

Nunca registro parcial.

A publicação final da nova `Location` deve utilizar operações atômicas apropriadas.

---

# 46. Append order

Fluxo:

```text
encode new record
      ↓
append segment
      ↓
validate write
      ↓
update secondary indexes
      ↓
publish Location
```

A ordem exata deve ser compatível com o modelo de recovery.

---

# 47. Durability

Modos:

```text
sync
batch
async
```

Default:

```yaml
durability:
  mode: batch
  interval: 100ms
```

---

# 48. Kafka offset durability

Offsets Kafka fazem parte do checkpoint.

Nunca:

```text
commit Kafka
then persist storage
```

Fluxo:

```text
consume
   ↓
materialize
   ↓
persist
   ↓
durable checkpoint
   ↓
commit Kafka offset
```

---

# 49. Fonte de verdade para recovery

A posição persistida pelo NishiDB é autoritativa.

Kafka consumer-group committed offset é secundário.

No startup:

```text
checkpoint offset
      ↓
Kafka seek()
```

---

# 50. Checkpoint

Estrutura:

```text
checkpoint/
    manifest.bin
    kafka.offsets
    metadata/
    indexes/
```

O checkpoint deve referenciar de maneira consistente:

* current segment;
* valid segment offset;
* row count;
* primary index state;
* location index;
* sequence index;
* secondary indexes;
* field dictionary;
* Kafka offsets.

---

# 51. Recovery

```text
load checkpoint
      ↓
verify checksums
      ↓
open mmap indexes
      ↓
scan segment tail
      ↓
validate CRC
      ↓
replay complete records
      ↓
discard incomplete final record
      ↓
start queries
      ↓
seek Kafka offsets
      ↓
resume ingestion
```

---

# 52. CRC

Utilizar:

```text
CRC32C
```

por registro.

Registro incompleto ou checksum inválido no tail deve interromper replay daquele ponto.

---

# 53. Compaction

Cada segmento possui:

```text
totalBytes
liveBytes
totalRecords
liveRecords
```

Candidato quando:

```text
liveBytes / totalBytes < threshold
```

Default:

```yaml
minimumLiveRatio: 0.50
```

---

# 54. Compaction memory constraint

Compaction não pode carregar segmento completo na memória.

Deve operar streaming:

```text
read record
   ↓
check liveness
   ↓
copy if live
   ↓
next record
```

Uso de memória deve permanecer limitado.

---

# 55. Compaction flow

```text
old segment
    ↓
scan sequentially
    ↓
copy live records
    ↓
fsync new segment
    ↓
atomically update locations
    ↓
remove old segment when safe
```

Secondary indexes não precisam ser reconstruídos.

---

# 56. TTL

Configuração:

```yaml
ttl:
  field: EventExpireDate
  type: timestamp
```

Quando expirar:

* row deixa de aparecer em query;
* secondary indexes são limpos;
* tombstone é publicado;
* estado físico será removido por compaction.

---

# 57. TTL scheduler

Não criar timer por row.

Usar:

```text
time buckets
```

ou:

```text
timing wheel
```

Resolução padrão:

```text
1 minuto
```

---

# 58. Tombstones

Tombstone mantém:

```text
rowId
serviceKey
lastSequence
deleted
expiration metadata
```

Impede ressurreição por eventos antigos.

---

# 59. SQL

Grammar v1:

```text
SELECT projection
FROM dataset
[WHERE expression]
[LIMIT number]
```

---

# 60. Projection

Suportar:

```sql
SELECT *
FROM radius_sessions;
```

e:

```sql
SELECT id, status, ip
FROM radius_sessions;
```

---

# 61. Predicates

Suportar:

```text
=
!=
>
>=
<
<=
IS NULL
IS NOT NULL
```

Lógicos:

```text
AND
OR
```

Parênteses devem ser suportados.

---

# 62. Parser SQL

Implementar parser próprio e pequeno.

Não adicionar parser SQL completo externo.

Pipeline:

```text
Lexer
  ↓
Parser
  ↓
AST
  ↓
Simple Planner
  ↓
Executor
```

---

# 63. Planner

Prioridade:

```text
1 primary key lookup
2 hash index
3 bitmap index
4 index intersection/union
5 full scan
```

Não implementar optimizer baseado em custo na v1.

---

# 64. Full scan

Campos sem índice continuam consultáveis.

Full scan deve percorrer somente estados live.

A engine deve registrar métrica:

```text
query.full_scan
```

---

# 65. LIMIT

Execução deve interromper após produzir resultados suficientes.

---

# 66. Query memory

Query não deve construir lista completa de resultados antes de retornar.

Resultados devem ser streamados.

Exemplo:

```text
candidate rowIds
      ↓
evaluate row
      ↓
encode result
      ↓
socket
```

Memória deve ser aproximadamente:

```text
O(batch size)
```

e não:

```text
O(result count)
```

---

# 67. NishiDB Protocol

Criar protocolo remoto simples e versionado.

Pode inicialmente utilizar HTTP/HTTPS.

Endpoints:

```text
POST /v1/query
GET  /v1/metadata
GET  /v1/health
GET  /v1/stats
```

---

# 68. Query request

Exemplo:

```json
{
  "dataset": "radius_sessions",
  "sql": "SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
  "parameters": [
    {
      "type": "string",
      "value": "Stop"
    }
  ]
}
```

---

# 69. Query response

Preferir streaming.

Formato inicial aceitável:

```text
NDJSON
```

Exemplo:

```json
{"id":"ABC","ip":"177.1.2.3"}
{"id":"DEF","ip":"177.1.2.4"}
```

Ou formato binário próprio futuramente.

---

# 70. JDBC Driver

Implementar em Java 21+.

Artefato:

```text
nishidb-jdbc.jar
```

Connection string:

```text
jdbc:nishidb://db01:9142/radius_sessions
```

---

# 71. JDBC classes mínimas

Implementar:

```text
Driver
Connection
Statement
PreparedStatement
ResultSet
ResultSetMetaData
DatabaseMetaData
```

Métodos não suportados:

```java
throw new SQLFeatureNotSupportedException();
```

---

# 72. PreparedStatement

Obrigatório.

Exemplo:

```java
PreparedStatement statement =
    connection.prepareStatement(
        """
        SELECT service_id, ip, nas
        FROM radius_sessions
        WHERE status = ?
          AND nas = ?
        LIMIT 100
        """
    );

statement.setString(1, "Stop");
statement.setString(
    2,
    "i-br-sp-cmp-cmp-hl4-01"
);
```

Parâmetros nunca devem ser interpolados manualmente no SQL.

---

# 73. JDBC type mapping

| NishiDB     | JDBC      |
| ----------- | --------- |
| string      | VARCHAR   |
| int64       | BIGINT    |
| double      | DOUBLE    |
| boolean     | BOOLEAN   |
| timestamp   | TIMESTAMP |
| ipv4        | VARCHAR   |
| ipv6        | VARCHAR   |
| ipv4_prefix | VARCHAR   |
| ipv6_prefix | VARCHAR   |
| bytes       | VARBINARY |
| json        | VARCHAR   |
| datetime    | TIMESTAMP |
| time        | TIME      |

---

# 74. Autenticação

Query server remoto suporta:

```text
username
password
role
```

Roles:

```text
READ
ADMIN
```

---

# 75. Configuração de autenticação

```yaml
server:
  authentication:
    enabled: true

    users:
      - username: grafana
        passwordEnv: NISHIDB_GRAFANA_PASSWORD
        role: READ

      - username: admin
        passwordFile: /run/secrets/nishidb-admin
        role: ADMIN
```

Senha literal no YAML não deve ser aceita.

---

# 76. Password storage

Quando usuários forem persistidos internamente, utilizar password hashing adequado.

Preferência:

```text
Argon2id
```

Nunca armazenar senha em texto puro.

---

# 77. TLS

Configuração:

```yaml
server:
  tls:
    enabled: true
    certificateFile: /etc/nishidb/tls/server.crt
    privateKeyFile: /etc/nishidb/tls/server.key
```

Autenticação remota deve exigir TLS por padrão.

---

# 78. Engine configuration

Arquivo:

```text
nishidb.yaml
```

Exemplo:

```yaml
version: 1

runtime:
  memoryTarget: 1GiB
  managedMemoryTarget: 512MiB

storage:
  path: /var/lib/nishidb
  segmentSize: 512MiB

  durability:
    mode: batch
    interval: 100ms

  checkpoint:
    interval: 30s

  compaction:
    enabled: true
    minimumLiveRatio: 0.50

server:
  enabled: true
  bind: 0.0.0.0
  port: 9142

  tls:
    enabled: true
    certificateFile: /etc/nishidb/tls/server.crt
    privateKeyFile: /etc/nishidb/tls/server.key

  authentication:
    enabled: true

    users:
      - username: reader
        passwordEnv: NISHIDB_READER_PASSWORD
        role: READ

datasets:
  directory: /etc/nishidb/datasets

query:
  defaultMaxRows: 100000
  defaultTimeout: 30s
  resultBatchSize: 256
```

---

# 79. Memory budget awareness

A engine deve coletar e expor:

```text
process RSS
Go heap
Go heap objects
mapped bytes
direct file mappings
query buffers
Kafka buffers
```

Mmap file size não deve ser confundido com RSS residente.

---

# 80. Automatic memory protection

A engine deve possuir guardrails simples.

Exemplo:

```yaml
runtime:
  memoryTarget: 1GiB

  query:
    maxConcurrent: 16
    maxBufferPerQuery: 4MiB
```

Se o sistema atingir pressão severa de memória, novas queries podem ser temporariamente rejeitadas antes de comprometer ingestão.

---

# 81. Priority under pressure

Em caso de pressão de memória, prioridade:

```text
1 storage correctness
2 Kafka ingestion
3 checkpoint/recovery
4 primary-key queries
5 indexed queries
6 full scans
7 admin operations
```

---

# 82. Múltiplos datasets

Uma instância pode possuir múltiplos datasets.

Porém todos compartilham o mesmo memory budget.

A engine deve evitar configuração que implique obrigatoriamente manter todos os índices de todos os datasets residentes.

---

# 83. Layout de disco

Exemplo:

```text
/var/lib/nishidb/

    datasets/

        radius_sessions/

            metadata/
                fields.dict
                dataset.meta

            data/
                segment-00000001.dat
                segment-00000002.dat

            indexes/
                primary.idx
                locations.idx
                sequences.idx

                bitmap/
                    status/
                    protocol/

                hash/
                    ip/
                    nas/

            checkpoint/
                manifest.bin
                kafka.offsets
```

---

# 84. Dimensionamento esperado

Para aproximadamente 8 milhões de entidades e estado binário médio entre 700 e 1000 bytes:

```text
live state:
~5,6–8 GB
```

Core indexes aproximados:

```text
primary:
~180 MB

locations:
~128 MB

sequence:
~64 MB

TTL metadata:
~64 MB ou estrutura equivalente
```

Bitmap indexes:

```text
~1 MB por bitmap de 8M rows
```

Hash indexes dependem fortemente de cardinalidade.

---

# 85. Disco recomendado

Meta mínima funcional:

```text
30 GB
```

Recomendado:

```text
50 GB
```

Operacionalmente confortável:

```text
100 GB
```

O espaço extra suporta:

* versões mortas;
* compaction;
* checkpoints;
* crescimento;
* múltiplos datasets.

---

# 86. Write rate esperado

Estado médio de 700 bytes:

```text
500 × 700
≈ 350 KB/s
≈ 30 GB/dia escritos
```

Estado médio de 1 KB:

```text
≈ 500 KB/s
≈ 43 GB/dia escritos
```

Esse valor representa volume escrito antes de compaction, não crescimento líquido.

---

# 87. Hardware alvo inicial

Para benchmark de referência:

```text
CPU:
2–4 vCPU

RAM:
1 GiB process/container limit

Disk:
50–100 GB NVMe/SSD

Network:
suficiente para Kafka e queries
```

O projeto deve funcionar sem exigir grande número de CPUs.

---

# 88. Performance targets

## Ingestão

Obrigatório:

```text
>= 500 eventos/s sustentados
```

Meta:

```text
>= 2.000 eventos/s
```

---

# 89. Primary-key lookup

Com páginas relevantes quentes:

```text
p95 < 10 ms
```

---

# 90. Indexed lookup

Com `LIMIT 100`:

```text
p95 < 50 ms
```

---

# 91. Full scans

Não existe SLA rígido de latência.

Full scans devem:

* ser canceláveis;
* respeitar timeout;
* respeitar LIMIT;
* não crescer memória proporcionalmente ao resultado.

---

# 92. Observabilidade

Métricas:

```text
nishidb_process_rss_bytes
nishidb_go_heap_bytes
nishidb_go_heap_objects

nishidb_ingest_events_total
nishidb_ingest_events_rate
nishidb_ingest_stale_total
nishidb_ingest_invalid_total

nishidb_kafka_lag
nishidb_kafka_offset

nishidb_materialized_rows
nishidb_materialized_tombstones

nishidb_storage_bytes
nishidb_storage_live_bytes
nishidb_storage_garbage_bytes

nishidb_segments_total

nishidb_query_total
nishidb_query_active
nishidb_query_errors
nishidb_query_full_scans

nishidb_query_latency_seconds

nishidb_compaction_running
nishidb_compaction_bytes_read
nishidb_compaction_bytes_written

nishidb_checkpoint_duration_seconds
nishidb_checkpoint_last_success_timestamp
```

---

# 93. Metrics endpoint

Expor:

```text
GET /metrics
```

Preferencialmente compatível com Prometheus.

---

# 94. Health

```text
GET /v1/health
```

Deve retornar ao menos:

```json
{
  "status": "UP",
  "kafka": "UP",
  "storage": "UP",
  "datasets": {
    "radius_sessions": "UP"
  }
}
```

---

# 95. Logging

Logs estruturados.

Campos:

```text
timestamp
level
component
dataset
event
message
```

Não registrar payload completo por default.

Não registrar senha.

---

# 96. CLI

Executável principal:

```bash
nishidb
```

Comandos desejados:

```bash
nishidb server --config /etc/nishidb/nishidb.yaml

nishidb status

nishidb datasets

nishidb compact radius_sessions

nishidb checkpoint radius_sessions

nishidb inspect radius_sessions
```

---

# 97. Single binary

O core Go deve gerar um único binário distribuível:

```text
nishidb
```

Sem runtime externo obrigatório.

Container preferencial:

```text
scratch
```

ou:

```text
distroless
```

quando compatível com TLS e certificados.

---

# 98. Estrutura de projeto sugerida

```text
nishidb/

├── cmd/
│   └── nishidb/
│
├── internal/
│   ├── config/
│   ├── kafka/
│   ├── materializer/
│   ├── schema/
│   ├── storage/
│   │   ├── segment/
│   │   ├── mmap/
│   │   ├── encoding/
│   │   ├── checkpoint/
│   │   └── compaction/
│   ├── index/
│   │   ├── primary/
│   │   ├── bitmap/
│   │   └── hash/
│   ├── query/
│   │   ├── lexer/
│   │   ├── parser/
│   │   ├── planner/
│   │   └── executor/
│   ├── server/
│   ├── auth/
│   ├── metrics/
│   └── lifecycle/
│
├── pkg/
│   └── protocol/
│
├── jdbc/
│   └── nishidb-jdbc/
│
├── benchmark/
│
├── configs/
│
└── testdata/
```

---

# 99. Interfaces conceituais Go

```go
type MaterializationEngine interface {
    Apply(event Event) (ApplyResult, error)
}
```

```go
type StorageEngine interface {
    Append(record *MaterializedRecord) (PhysicalLocation, error)
    Read(location PhysicalLocation, dst []byte) ([]byte, error)
}
```

```go
type PrimaryIndex interface {
    Find(key []byte) (uint64, bool, error)
    Insert(key []byte, rowID uint64) error
}
```

```go
type LocationIndex interface {
    Get(rowID uint64) (PhysicalLocation, bool)
    Set(rowID uint64, location PhysicalLocation) error
}
```

```go
type SecondaryIndex interface {
    Lookup(predicate IndexPredicate) (RowIDIterator, error)

    Update(
        rowID uint64,
        previousValue Value,
        newValue Value,
    ) error
}
```

```go
type QueryEngine interface {
    Execute(
        ctx context.Context,
        dataset string,
        statement Statement,
        params []Value,
    ) (RowIterator, error)
}
```

---

# 100. Iterator based architecture

Evitar métodos que retornem milhões de objetos.

Preferir:

```go
type RowIterator interface {
    Next() bool
    Row() Row
    Err() error
    Close() error
}
```

Mesmo princípio para:

```text
RowIDIterator
SegmentIterator
ResultIterator
```

---

# 101. Erros de ingestão

Categorias:

```text
INVALID_JSON
SCHEMA_VALIDATION
INVALID_KEY
INVALID_SEQUENCE
TYPE_CONVERSION
STALE_EVENT
STORAGE_ERROR
```

Política:

```yaml
errors:
  invalidEvent: log-and-skip
```

Valores:

```text
log-and-skip
stop
```

---

# 102. Dead-letter

Arquitetura deve permitir futuramente:

```yaml
errors:
  deadLetterTopic: radius-accounting-dlq
```

Não é requisito bloqueante inicial.

---

# 103. Testes de materialização

Obrigatórios:

* create;
* merge;
* replace;
* NULL;
* field missing;
* wildcard;
* exclusion;
* unknown field;
* sequence greater;
* sequence equal;
* sequence lower;
* Mongo Extended JSON.

---

# 104. Testes de storage

Obrigatórios:

* append;
* read;
* segment rotation;
* CRC;
* invalid CRC;
* incomplete tail;
* restart;
* checkpoint;
* replay;
* compaction;
* mmap reopen.

---

# 105. Testes Kafka

Obrigatórios:

* duplicate delivery;
* offset recovery;
* multiple partitions;
* crash before checkpoint;
* crash after append;
* restart;
* stale event;
* queue backpressure.

---

# 106. Query tests

Obrigatórios:

* primary lookup;
* bitmap lookup;
* hash lookup;
* AND;
* OR;
* comparison;
* NULL;
* LIMIT;
* full scan;
* aliases;
* quoted fields;
* parameters.

---

# 107. JDBC tests

Obrigatórios:

* DriverManager;
* Connection;
* Statement;
* PreparedStatement;
* ResultSet;
* metadata;
* NULL;
* timestamp;
* authentication;
* TLS;
* unsupported JDBC method.

---

# 108. Memory tests

Obrigatório benchmark com:

```text
8 milhões de rows
```

sob limite:

```text
1 GiB
```

Validar:

```text
RSS
heap
GC frequency
allocations/event
mmap residency
query concurrency
compaction
Kafka ingestion
```

---

# 109. Memory acceptance criterion

Sob dataset de 8 milhões de entidades, após estabilização:

```text
process RSS MUST remain below 1 GiB
```

durante:

```text
500 events/s ingestion
+
primary key queries
+
indexed queries
```

Compaction deve ser testada separadamente sob a mesma restrição.

---

# 110. Heap scalability criterion

A diferença de heap entre:

```text
1 milhão de rows
```

e:

```text
8 milhões de rows
```

não deve crescer proporcionalmente ao número de rows.

Estruturas disk-backed podem crescer proporcionalmente.

Heap não.

---

# 111. Allocation target

No hot path de ingestão, minimizar allocations.

Meta desejável após otimização:

```text
single-digit allocations per event
```

ou melhor.

Não sacrificar clareza ou correção prematuramente apenas para atingir zero allocations.

Benchmark deve orientar otimizações.

---

# 112. CPU profiling

Benchmark deve incluir:

```text
pprof CPU
pprof heap
pprof allocs
```

Endpoints de debug podem ser habilitados somente em modo administrativo.

---

# 113. Etapas de implementação

## Fase 1 — Storage

Implementar:

* segments;
* binary records;
* CRC;
* field dictionary;
* rowId;
* primary mmap index;
* location mmap index.

Critério:

```text
write
restart
lookup by id
```

---

# 114. Fase 2 — Materializer

Implementar:

* JSON parsing;
* YAML;
* typing;
* wildcard;
* merge;
* replace;
* sequence;
* NULL.

---

# 115. Fase 3 — Kafka

Implementar:

* consumer;
* bounded queue;
* offsets;
* checkpoint;
* seek;
* recovery.

---

# 116. Fase 4 — Bitmap indexes

Implementar primeiro apenas bitmap index.

Eles oferecem:

* baixo footprint;
* implementação simples;
* excelente benefício para o dataset.

---

# 117. Fase 5 — Query engine

Implementar:

* lexer;
* parser;
* AST;
* planner;
* executor;
* streaming results.

---

# 118. Fase 6 — HTTP protocol

Implementar:

```text
/v1/query
/v1/metadata
/v1/health
/v1/stats
```

---

# 119. Fase 7 — JDBC

Implementar thin Java driver.

---

# 120. Fase 8 — Hash secondary index

Adicionar após o core funcionar.

Benchmark footprint antes de habilitar campos de alta cardinalidade.

---

# 121. Fase 9 — TTL

Implementar:

* expiration buckets;
* tombstones;
* cleanup de indexes.

---

# 122. Fase 10 — Compaction

Implementar:

* live statistics;
* candidate selection;
* streaming rewrite;
* location publication.

---

# 123. Fase 11 — Auth/TLS

Implementar:

* users;
* roles;
* secret env/file;
* TLS;
* Argon2id.

---

# 124. Fase 12 — Memory optimization

Somente após funcionalidade correta:

* profiling;
* reduce allocations;
* optimize buffers;
* tune GOMEMLIMIT;
* tune Kafka buffers;
* tune mmap access;
* tune compaction buffers.

---

# 125. Fase 13 — Benchmark de escala

Executar:

```text
8.000.000 entities

500 events/s

1 GiB memory limit
```

Medir:

```text
RSS
Go heap
GC CPU
disk usage
ingest throughput
query p50
query p95
query p99
checkpoint time
restart time
compaction throughput
Kafka lag
```

---

# 126. Critério de aceite funcional

A versão 1 será considerada funcional quando:

1. iniciar de configuração YAML;
2. conectar ao Kafka;
3. consumir JSON;
4. materializar por ID;
5. suportar patches;
6. suportar `_SEQ`;
7. armazenar estado binário;
8. sobreviver restart;
9. recuperar offsets;
10. suportar 8 milhões de rows;
11. sustentar 500 eventos/s;
12. permanecer abaixo de 1 GiB RSS;
13. consultar por primary key;
14. consultar por bitmap index;
15. consultar por hash index;
16. fazer full scan;
17. expor protocolo remoto;
18. possuir JDBC driver;
19. suportar PreparedStatement;
20. autenticar usuário remoto;
21. utilizar TLS;
22. suportar TTL;
23. suportar tombstones;
24. compactar segmentos;
25. sobreviver `kill -9` sem perder estado confirmado.

---

# 127. Ordem de prioridades

Sempre priorizar:

```text
1. correção
2. baixo footprint
3. simplicidade
4. previsibilidade
5. durability
6. performance
7. features
```

---

# 128. Regras arquiteturais obrigatórias

O projeto deve seguir:

```text
NO database dependency

NO in-memory full dataset

NO millions-of-objects architecture

NO unbounded queues

NO update in-place

NO multi-writer v1

NO JOIN

NO hidden storage engine

NO query result buffering proporcional ao resultado
```

---

# 129. Resultado esperado

Executável:

```bash
./nishidb server \
  --config /etc/nishidb/nishidb.yaml
```

Startup esperado:

```text
loading configuration
loading dataset radius_sessions
opening storage
opening mmap indexes
recovering checkpoint
replaying segment tail
starting query server
connecting Kafka
seeking persisted offsets
starting ingestion
```

---

# 130. JDBC usage

```java
Properties properties = new Properties();

properties.setProperty(
    "user",
    "reader"
);

properties.setProperty(
    "password",
    System.getenv("NISHIDB_PASSWORD")
);

try (
    Connection connection =
        DriverManager.getConnection(
            "jdbc:nishidb://localhost:9142/radius_sessions",
            properties
        );

    PreparedStatement statement =
        connection.prepareStatement(
            """
            SELECT
                service_id,
                ip,
                nas,
                status
            FROM radius_sessions
            WHERE status = ?
              AND nas = ?
            LIMIT 100
            """
        )
) {

    statement.setString(
        1,
        "Stop"
    );

    statement.setString(
        2,
        "i-br-sp-cmp-cmp-hl4-01"
    );

    try (
        ResultSet resultSet =
            statement.executeQuery()
    ) {

        while (resultSet.next()) {

            System.out.println(
                resultSet.getString("service_id")
                    + " "
                    + resultSet.getString("ip")
            );
        }
    }
}
```

---

# 131. Meta final

O núcleo funcional esperado é:

```text
                   NishiDB

              8.000.000 states

                    +

               Kafka ingest
               500 events/s

                    +

              append-only disk

                    +

               mmap indexes

                    +

                 SQL v1

                    +

              JDBC remote

                    +

               auth + TLS

                    +

              RSS < 1 GiB
```

O sucesso do projeto não será medido pela quantidade de funcionalidades implementadas.

Será medido pela capacidade de resolver esse problema específico com:

```text
baixo consumo de memória
baixo número de dependências
comportamento previsível
recovery simples
storage compacto
boa performance
```

sem transformar o NishiDB em um banco de dados genérico.

