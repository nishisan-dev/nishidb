# NishiDB

Engine de materialização e consulta para fluxos de eventos JSON parciais.

Recebe eventos, mantém o **estado corrente** de cada entidade (merge por campo,
controle de sequência) e responde consultas SQL sobre esse estado — com o
dataset inteiro cabendo num orçamento de memória fixo.

Medido com **8 milhões de entidades**: 486 MiB de RSS de pico (limite: 1 GiB),
heap Go de 994 KiB, 45 mil eventos/s de ingestão e point lookup em **16
microssegundos** pelo caminho SQL completo.

## Estado

**M1 completo.** Ingestão, materialização, storage durável com recovery,
índices, SQL e saída NDJSON — tudo pelo binário. Os 25 critérios de aceite estão
detalhados em [`docs/aceite-m1.md`](docs/aceite-m1.md): 13 atendidos, 5 parciais
com milestone dono, 7 fora do M1 por planejamento.

Próximos: **M2** (consumidor Kafka), **M3** (protocolo HTTP, driver JDBC, auth,
TLS), **M4** (índice hash físico, compaction, TTL).

## Instalação

```bash
go build -o nishidb ./cmd/nishidb
```

Binário único, sem dependência externa em runtime. Requer Go 1.25+ e Linux
(o storage usa `mmap` e `fallocate` diretamente).

## Uso

### 1. Configuração

`nishidb.yaml`:

```yaml
version: 1

storage:
  path: /var/lib/nishidb
  segmentSize: 512MiB
  checkpoint:
    interval: 30s

datasets:
  directory: /etc/nishidb/datasets

query:
  defaultMaxRows: 100000
  defaultTimeout: 30s
```

### 2. Definição do dataset

`/etc/nishidb/datasets/radius_sessions.yaml` — o nome do arquivo é o nome do
dataset:

```yaml
version: 1
name: radius_sessions

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

schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
      alias: service_id
    Acct-Status-Type:
      type: string
      alias: status
      index: bitmap
    NAS-IP-Address:
      type: ipv4
      alias: nas_ip
      index: hash
    User-Name:
      type: string
      alias: username
```

### 3. Ingerir

```console
$ cat eventos.ndjson
{"id":"CMP-1","_SEQ":1,"Acct-Status-Type":"Stop","NAS-IP-Address":"10.0.0.1","User-Name":"joao"}
{"id":"CMP-2","_SEQ":1,"Acct-Status-Type":"Start","NAS-IP-Address":"10.0.0.2","User-Name":"ana"}
{"id":"CMP-1","_SEQ":2,"Acct-Terminate-Cause":"User-Request"}

$ nishidb ingest --config nishidb.yaml --dataset radius_sessions --input eventos.ndjson
ingest: 3 lidos, 3 aplicados (2 criados, 1 atualizados, 0 stale) em 14ms (214 ev/s)
```

O terceiro evento é um **patch**: ele acrescenta um campo à sessão `CMP-1` sem
tocar nos demais. Aceita `-` para ler de stdin.

### 4. Consultar

```console
$ nishidb query --config nishidb.yaml --dataset radius_sessions \
    "SELECT id, status, nas_ip, terminate_cause FROM radius_sessions WHERE status = 'Stop'"
{"id":"CMP-1","status":"Stop","nas_ip":"10.0.0.1","terminate_cause":"User-Request"}
```

Saída em NDJSON — uma linha por row, em streaming. Colunas ausentes saem como
`null` explícito.

O erro de consulta aponta a posição:

```console
$ nishidb query --config nishidb.yaml --dataset radius_sessions \
    "SELECT id FROM radius_sessions
     WHERE status = 'Stop' AND naoexiste = 1"
query: coluna "naoexiste" não existe no dataset "radius_sessions" (posição 57)
  linha 2, coluna 27:
     WHERE status = 'Stop' AND naoexiste = 1
                              ^
```

### SQL suportado (v1)

```sql
SELECT <colunas | *> FROM <dataset>
  [WHERE <predicado>]
  [LIMIT <n>]
```

Comparadores `=`, `!=`, `>`, `>=`, `<`, `<=`; `AND`, `OR`, parênteses;
`IS NULL` / `IS NOT NULL`; parâmetros posicionais `?`; alias e campos entre
aspas duplas (obrigatórias para nomes com hífen, como `"Acct-Status-Type"`).

Repetir uma coluna na projeção (`SELECT id, id`) é recusado no bind: a linha
sairia com a chave repetida e o consumidor de JSON colapsaria as duas em
silêncio — duas colunas pedidas, uma entregue. Pedir o mesmo campo pelo nome e
pelo alias (`SELECT id, service_id`) continua valendo, porque aí são duas
chaves distintas na saída.

Fora da v1: `ORDER BY`, `GROUP BY`, `JOIN`, agregações, `NOT` genérico,
`OFFSET`.

### Exit codes

| Código | Significado |
|---:|---|
| 0 | sucesso |
| 1 | erro de uso ou configuração |
| 2 | erro de sintaxe ou bind da query |
| 3 | dataset corrompido, travado por outro processo, ou inacessível |
| 4 | writer em modo falho (reabra o dataset) |
| 5 | resultado truncado pelo teto `query.maxRows` |
| 6 | interrompido por sinal |
| 7 | eventos não entraram por falha (perda de dado): investigue e reprocesse |

O 7 é o que separa perda de sucesso. Evento **ruim** (JSON inválido, key
ausente, tipo incompatível) é descartado pela política §101 e a ingestão termina
com 0 — uma fonte ruidosa não derruba o pipeline. Evento que **não entrou por
falha nossa ou do dataset** (erro do storage, inconsistência interna) sai com 7:
o dataset está íntegro e checkpointado, mas faltam rows.

## Documentação

- [Arquitetura](docs/architecture.md) — como o sistema funciona, com os números.
- [Aceite do M1](docs/aceite-m1.md) — os 25 critérios, item a item.
- [Rodada de aceite](benchmark/baselines/aceite-8m-20260814.md) — 8M entidades
  com consultas concorrentes.

## Desenvolvimento

```bash
go test ./...          # suíte completa
go test ./... -race    # com o detector de corrida
```

A rodada de aceite **não** roda no `go test` padrão: ela grava vários GiB e
exige disco real.

```bash
go build -o ingest-bench ./benchmark/ingest-bench
GOMEMLIMIT=512MiB GOGC=100 ./ingest-bench -data /var/tmp/nb-8m \
    -entities 8000000 -steady 60s -rate 500 -queries 4 -sql-queries 4 \
    -checkpoint-every 500000 -heap-baseline rep-1m.json -report rep-8m.json
```
