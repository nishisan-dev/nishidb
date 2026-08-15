# M3 — Fase 6: protocolo remoto HTTP (§67-69, §118) — issue #2

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development` ou `superpowers:executing-plans` para executar tarefa a tarefa. Os passos usam checkbox (`- [ ]`).

**Goal:** fechar a Fase 6 da spec — `POST /v1/query`, `GET /v1/metadata`, `GET /v1/stats` (o `/v1/health` já saiu no M2) — e com ela o **débito 54**: consultar o dataset **enquanto** a ingestão Kafka roda.
**Architecture:** `cliente → http.ServeMux → semáforo (§80) → query.Engine.Execute → RowIterator → StreamWriter → ResponseWriter`, com moldura NDJSON (schema na primeira linha, rows cruas do §69 no meio, fecho na última) e zero buffer proporcional ao resultado (§128).
**Tech Stack:** Go 1.25, stdlib apenas (`net/http`, `encoding/json` fora do hot path). **Nenhuma dependência nova.**

---

## 1. Contexto

O M2 entregou o `nishidb server`: ele abre os datasets, registra todos no `query.Engine` (`internal/server/server.go:194`) e expõe `GET /metrics` e `GET /v1/health`. O motor de consulta está **de pé no processo e inalcançável de fora** — nada roteia para ele, e o `nishidb query` é barrado pelo flock exclusivo do dataset, saindo com exit 3 e nomeando o pid do servidor.

O resultado prático está registrado como débito 54: o operador **alterna**. Sobe o servidor para ingerir, encerra com SIGTERM (que drena, checkpointa e devolve o flock) e só então consulta. É a diferença entre "consome de Kafka" e "é operável como banco".

Esta fase também **desbloqueia** a Fase 7: o driver JDBC do §70 é *thin*, ou seja, cliente deste protocolo. Sem `/v1/query` ele não tem com o que falar.

**Metade do trabalho já está no repositório** e não será reescrito:

| Peça | Onde | Papel nesta fase |
|---|---|---|
| `query.Engine.Execute(ctx, dataset, sql, params []types.Value)` | `internal/query/engine.go:78` | Já aceita **parâmetros posicionais**; o `parameters` do §68 é o primeiro consumidor real do `?` |
| `executor.StreamWriter` (NDJSON, buffer fixo de 64 KiB) | `internal/query/executor/ndjson_writer.go` | É literalmente o formato do §69 |
| `executor.RowIterator` + `AppendRow` zero-alocação | `internal/query/executor/` | Streaming ponta a ponta |
| `http.Server`, mux, shutdown gracioso | `internal/server/server.go:304`, `http.go` | Basta acrescentar rotas |
| `metrics.Registry` (pull, sem duplicar contador) e `logging.Logger` (§95) | `internal/metrics`, `internal/logging` | Prontos |
| `binder.coerceParam` | `internal/query/binder/coerce.go:275` | **Define o contrato dos parâmetros** (ver §4.1) |

O trabalho é majoritariamente **contrato e fiação**, não motor novo.

---

## 2. Decisões fechadas com o usuário

| # | Decisão | Escolha | Alternativa rejeitada |
|---|---|---|---|
| **D1** | Exposição de rede antes de auth | **`server.bind` default passa a `127.0.0.1`.** Expor vira ato explícito no YAML, com WARN na subida quando o bind não é loopback e não há auth | Puxar §74-77 para cá (dobra o escopo, atrasa o JDBC); opt-in por chave nova (cerimônia sem segurança); manter `0.0.0.0` (leitura irrestrita do dataset para quem alcançar a 9142). O exemplo do §78 traz `bind: 0.0.0.0` **junto com** `tls.enabled: true` e `authentication.enabled: true` — o bind aberto da spec pressupõe auth, que é da Fase 11 |
| **D2** | Corpo do `/v1/metadata` | **Uma rota, tudo, filtrável por `?dataset=`**: colunas com nome, alias, tipo do §17, tipo JDBC do §73, índice, campo-chave, `maxRows`/`timeout` efetivos | Duas rotas (o §67 nomeia uma, e o `DatabaseMetaData` precisaria de dois round-trips); mínimo (cada campo omitido vira um segundo formato na Fase 7) |
| **D3** | Corpo do `/v1/stats` | **Por dataset**, lendo as **mesmas fontes** que o `/metrics` lê no scrape: `Store.Stats()`, `executor.Metrics`, `planner.Metrics` e o estado da ingestão. **Nenhum contador novo** | Espelho JSON do `/metrics` (formato Prometheus disfarçado); sem ingestão (duas rotas para diagnosticar lentidão) |
| **D4** | `query.resultBatchSize` (débito 10) | **Lote de flush da resposta HTTP**: a cada N linhas, `StreamWriter.Flush()` **seguido de** `http.Flusher.Flush()` | Cadência interna do executor (fecha o débito sem mudar nada para quem consome); as duas (uma chave com duas semânticas) |
| **D5** | Erro no meio do streaming | **Moldura**: 1ª linha = schema das colunas, rows **cruas** como o §69, última linha = fecho (`end` com `truncated`, ou `error` com código) | Trailer HTTP (o `java.net.http` do driver da Fase 7 não os expõe); encerrar a conexão (cliente não separa truncamento de queda); envelope por linha (contraria o exemplo do §69 e custa o `AppendRow` zero-alocação) |
| **D6** | Teto e timeout por requisição | `maxRows` e `timeoutMs` opcionais no corpo: **menor vale, maior sai 400** nomeando o teto efetivo | Clamp silencioso (nunca avisa que o pedido não foi atendido como escrito); sem chaves (o `Statement.setMaxRows()` do JDBC fica sem destino) |
| **D7** | Concorrência sob pressão (§80, §81) | **`runtime.query.maxConcurrent` (default 16)**, semáforo por processo; excedente sai **503 + `Retry-After`** sem tocar o Store | Sem limite (N conexões = N × maior-linha de RSS, débito 28, sem regra reclamando); fila de espera (segura conexões e mascara a saturação) |
| **D8** | Cliente CLI | **`nishidb query --server host:9142`** fala o protocolo em vez de abrir o dataset local; a mensagem do exit 3 passa a ensinar a flag | Só `curl` (o débito 54 fica meio aberto até a Fase 7); comando novo separado (dois comandos para a mesma pergunta) |

### Decisões menores, fechadas por análise

| # | Item | Escolha | Por quê |
|---|---|---|---|
| **D9** | Tipos aceitos em `parameters[].type` | **`string`, `int64`, `double`, `boolean` — e mais nada.** `timestamp`, `ipv4`, prefixos etc. viajam como **string** e são coagidos pela tabela dos literais | Não é limitação inventada: é o contrato de `binder.coerceParam` (`coerce.go:275-296`), que recusa parâmetro já em representação binária para manter **um** caminho de coerção e **um** conjunto de testes. O §73 concorda — `ipv4 → VARCHAR`, logo o driver manda string. `type` fora dos quatro sai 400 **nomeando a alternativa** |
| **D10** | Discriminador da moldura | Chave reservada **`nishidb`** na raiz do objeto. Coluna de saída chamada `nishidb` faz a query sair **400 antes do primeiro byte** (os nomes são conhecidos no `Open`), sugerindo alias | Prefixo `$` colide com identificador citado; envelope por linha foi rejeitado no D5. A recusa é determinística e nunca acontece depois do primeiro byte |
| **D11** | Timeout do `http.Server` | **Sem `WriteTimeout`.** O teto real é o `query.timeout` do dataset, aplicado pelo executor via ctx | `WriteTimeout` mataria stream longo legítimo — é teto de conexão, não de consulta |
| **D12** | Corpo do request | `Content-Type: application/json` obrigatório (415), corpo limitado por `http.MaxBytesReader` a **1 MiB** (413) | `NO unbounded queues` (§128) vale para o request também: sem teto, um POST de 2 GiB é um DoS de heap |
| **D13** | `nishidb_query_full_scans` | Entra junto, como **ColetorFunc** sobre `Engine.PlanMetrics().FullScan` (§64), **sem label `dataset`** | É a 5ª série de query do §92 sem alimentador e custa 6 linhas. Sai sem label porque `planner.Metrics` é **um por Engine**, compartilhado entre datasets (`engine.go:68`) — inventar um label que o dado não sustenta seria pior que não ter |

---

## 3. Constraints globais (valem para TODAS as tarefas)

1. **§128 `NO query result buffering proporcional ao resultado`.** Nada de `[]Row`, nada de `bytes.Buffer` acumulando a resposta. O `StreamWriter` já é o mecanismo; a moldura não pode introduzir um segundo.
2. **Um caminho de coerção só** (D9): o request nunca constrói `types.Value` em representação binária.
3. **Nenhum contador duplicado** (doutrina de `internal/metrics/metrics.go:10-14`): `/v1/stats` e `/metrics` leem as **mesmas** estruturas, no instante da chamada.
4. **§95:** toda linha de log com `component=query`, `dataset`, `event` estável e `message` em prosa. Nunca registrar o SQL completo em INFO nem valor de parâmetro — o SQL vai em DEBUG, os parâmetros **nunca** (podem carregar dado do cliente).
5. **Nenhuma dependência nova** (§97: binário único).
6. **Idioma e estilo:** comentários, erros e nomes de teste em PT-BR, no estilo argumentativo existente — o comentário explica **por quê**, cita `§NN` e o débito que fecha. Sentinela por pacote + `errors.Is` na fronteira. Stdlib `testing`, zero testify.
7. **`go test ./...` e `go test -race ./...` verdes, `golangci-lint run` limpo** antes de qualquer commit.

---

## 4. Contrato do wire (a parte que a Fase 7 vai ler)

### 4.1 `POST /v1/query` — request

```json
{
  "dataset": "radius_sessions",
  "sql": "SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
  "parameters": [{ "type": "string", "value": "Stop" }],
  "maxRows": 1000,
  "timeoutMs": 5000
}
```

- `dataset` e `sql` obrigatórios. `parameters` opcional; a contagem tem de bater com os `?` do SQL (o `binder.Bind` já cobra isso, `binder.go:161`).
- `type` ∈ {`string`, `int64`, `double`, `boolean`} (D9). O **tipo declarado manda**: `{"type":"int64","value":3.5}` e `{"type":"string","value":42}` saem 400 — nunca adivinhar a intenção.
- `maxRows`/`timeoutMs` opcionais, **só para reduzir** (D6).

### 4.2 Resposta — moldura NDJSON

`Content-Type: application/x-ndjson`, HTTP 200 assim que a primeira linha sai.

```json
{"nishidb":"schema","dataset":"radius_sessions","columns":[{"name":"id","type":"string","jdbc":"VARCHAR"},{"name":"ip","type":"ipv4","jdbc":"VARCHAR"}]}
{"id":"ABC","ip":"177.1.2.3"}
{"id":"DEF","ip":"177.1.2.4"}
{"nishidb":"end","rows":2,"truncated":false}
```

Fecho com falha depois de N linhas já emitidas:

```json
{"nishidb":"error","code":"MAX_ROWS_EXCEEDED","rows":100000,"message":"resultado truncado no teto de query.maxRows: 100000"}
```

- As linhas de dados são **exatamente** o que o `AppendRow` já produz — byte a byte o exemplo do §69. Nada muda no encoder.
- A linha de schema fecha o **débito 13**: o NDJSON não carrega tipos, e o `ResultSetMetaData` do §71 precisa deles **inclusive das colunas que são NULL em todas as linhas** — que o dado não revela. A fonte é `ColView.Type` (o tipo **declarado**, constante por query), já disponível no `Open`.
- Coluna auto que o dataset nunca observou tem `EffectiveType() == TypeNull`: sai `"type":"unknown","jdbc":"OTHER"`. Chamá-la de VARCHAR seria mentir para o driver.

### 4.3 Erro **antes** da primeira linha

Status HTTP + corpo JSON único, com a posição que o CLI já imprime hoje:

```json
{"error":{"code":"SYNTAX","message":"...","line":1,"column":18}}
```

| Situação | Status | Código |
|---|---|---|
| JSON malformado, `dataset`/`sql` ausente, parâmetro inválido, `maxRows` acima do teto, coluna chamada `nishidb` | 400 | `BAD_REQUEST`, `BAD_PARAMETER`, `MAX_ROWS_ABOVE_LIMIT`, `RESERVED_COLUMN` |
| `parser.ErrSyntax` / `binder.ErrBind` | 400 | `SYNTAX` / `BIND` |
| Dataset não registrado | 404 | `UNKNOWN_DATASET` |
| Método ≠ POST / `Content-Type` errado / corpo > 1 MiB | 405 / 415 / 413 | — |
| `maxConcurrent` estourado (D7) | 503 + `Retry-After: 1` | `TOO_MANY_QUERIES` |
| `query.timeout` estourado antes da primeira linha | 504 | `TIMEOUT` |
| `executor.ErrInvalidPlan` (bug nosso), `encoding.ErrCorrupt`, erro de storage | 500 | `INTERNAL`, `DATASET_CORRUPT` |

Depois do primeiro byte **o status é sempre 200** e o desfecho vai na linha de fecho — é exatamente o que a moldura existe para resolver.

### 4.4 `GET /v1/metadata` (`?dataset=` filtra)

```json
{"datasets":[{
  "name":"radius_sessions","key":"acctSessionId","maxRows":100000,"timeoutMs":30000,
  "columns":[{"name":"ip","alias":null,"type":"ipv4","jdbc":"VARCHAR","index":"bitmap","auto":false}]
}]}
```

Fonte: `Compiled.FieldsByID()`, `CompiledField{Name, Alias, EffectiveType(), Index}`, `Compiled.Key`, `Compiled.MaxRows`, `Compiled.Timeout` (`internal/schema/compile.go:71,128`).

### 4.5 `GET /v1/stats`

```json
{"datasets":[{
  "name":"radius_sessions",
  "storage":{"rows":8000000,"segments":16,"currentSegment":16,"primaryLoadFactor":0.61,"overlay":0},
  "query":{"total":1240,"active":2,"errors":3,"fullScans":11,"emitted":98211,"candidates":120400,"rejected":22189,"truncated":0},
  "ingestion":{"enabled":true,"state":"UP","events":8000000,"applied":7999880,"stale":120,"discarded":0,"failed":0,"paused":null}
}]}
```

Fontes, na ordem: `Store.Stats()` (`store.go:1233`), `Engine.Metrics()`/`PlanMetrics()`, `Consumidor.Desfecho()`/`Pausa()`. **Nenhuma cópia de estado** — leitura no instante da chamada, como o `/metrics` faz.

> **Débito conhecido a registrar, não a consertar aqui:** `Store.Stats()` engole `ErrDatasetClosed` e devolve `Stats{}` — `rows: 0` num Store fechado é indistinguível de dataset vazio. É o débito 34, que previa exatamente este momento ("quando `Stats` virar superfície de observabilidade"). Fica anotado com dono; consertá-lo mexe no contrato do `Store`, que não é desta fase.

---

## 5. Mudanças por pacote

| Arquivo | Mudança |
|---|---|
| **`internal/protocolo/protocolo.go`** *(novo pacote)* | Tipos do request/resposta/moldura, códigos de erro e a tabela §73 (`JDBCName(types.FieldType) string`). Mora fora do `server` porque o **CLI cliente também decodifica a moldura** (D8) — duas definições divergiriam no primeiro campo novo |
| `internal/query/executor/executor.go` | `RowIterator` ganha `Columns() []ColMeta`; o `iterador` já tem `it.cols` populado no `Open`. Tipo próprio (`ColMeta{Name, Type}`) em vez de expor `ColView`, que carrega `Val`/`Null` válidos só durante uma row |
| `internal/query/engine.go` | Sem mudança de assinatura — `Execute` já devolve `RowIterator` |
| `internal/config/config.go` | `serverBindPadrao` → `127.0.0.1` (D1); bloco `runtime.query.maxConcurrent` (default 16, `units.InteiroPositivo`); `ResultBatchSize()` ganha seu primeiro leitor |
| `internal/server/query.go` *(novo)* | `handleQuery`: decodifica, valida, adquire o semáforo, executa, emite a moldura, mede |
| `internal/server/metadata.go` *(novo)* | `handleMetadata` e `handleStats` |
| `internal/server/metricas_query.go` *(novo)* | Famílias `nishidb_query_total`/`_active`/`_errors`/`_latency_seconds` + ColetorFunc de `_full_scans` (D13). Nomes vêm de `internal/metrics/nomes.go`, nunca digitados à mão |
| `internal/server/server.go` | `sobeHTTP` registra as três rotas; `Novo` avisa (WARN) se o bind não é loopback |
| `internal/cli/query.go` | Flag `--server`; caminho remoto; tradução do fecho em exit code; dica de `--server` na mensagem do `ErrDatasetLocked` |
| `internal/cli/cli.go` | Texto de uso |

---

## 6. Tarefas

> **Commits por bloco** (é a convenção deste repositório, não os atômicos do CLAUDE.md global): um commit por tarefa abaixo.

- [ ] **T1 — Contrato do wire.** Pacote `internal/protocolo` com request, moldura, códigos de erro e a tabela §73. `RowIterator.Columns()` no executor. Testes de round-trip do formato e de recusa dos tipos fora dos quatro (D9).
- [ ] **T2 — Configuração.** Bind default loopback + WARN de exposição; `runtime.query.maxConcurrent`; leitor de `resultBatchSize`. Testes: default novo, chave inválida recusada com nome e valor, WARN emitido só no bind aberto.
- [ ] **T3 — `POST /v1/query`.** Handler completo: validação, semáforo (503 + `Retry-After`), execução, moldura, flush por lote (D4: `StreamWriter.Flush()` **e depois** `http.Flusher.Flush()` — sem os dois, os bytes ficam no buffer de 64 KiB e o "streaming" não sai do processo), tradução de erro em status (§4.3), logs do §95.
- [ ] **T4 — Métricas do §92.** As quatro séries de query + `full_scans`. Teste que raspa `/metrics` depois de uma query e confere nome, tipo e valor.
- [ ] **T5 — `/v1/metadata` e `/v1/stats`.** Corpos do §4.4/§4.5, com teste de coerência: o `rows` do `/v1/stats` bate com o `nishidb_materialized_rows` do `/metrics` no mesmo instante.
- [ ] **T6 — `nishidb query --server`.** Cliente CLI, tradução do fecho em exit code (truncado → 5, interrompido → 6, erro de query → 2), dica na mensagem do exit 3. Teste fim a fim contra um servidor de verdade subido no teste.
- [ ] **T7 — Aceite.** Os quatro critérios do §7 abaixo, com evidências em `docs/aceite-fase6.md`.
- [ ] **T8 — Documentação.** `README.md`, `docs/architecture.md`, `docs/operacao-kafka.md`, `docs/debitos.md` e o diagrama novo.

---

## 7. Verificação

**Automática** (`make test`, `make test-race`, `make lint`):

1. **Streaming de verdade (§69/§128).** Servidor real, resultado grande: o cliente **recebe e conta** a primeira linha antes de o servidor ter produzido a última (handler com barreira injetada entre lotes). Prova que a resposta não é montada antes de sair.
2. **Memória O(batch), não O(resultado) (§128).** Gate no padrão dos `alloc_gate_test.go` já existentes: rodar a mesma query com 10× mais linhas **não** multiplica alocações por linha nem o pico de heap.
3. **`parameters` do §68.** Um teste por tipo do D9, mais o caso que o CLI **não alcança hoje**: `?` posicional resolvido de ponta a ponta pelo HTTP. Mais as recusas: tipo fora dos quatro, `null`, contagem divergente dos `?`.
4. **Moldura.** Erro depois de N linhas emitidas produz linha `error` com o código certo, e o `truncated: true` aparece no `ErrMaxRowsExceeded`. Sem isso o driver da Fase 7 não separa "acabou" de "quebrou".
5. **Concorrência (D7).** `maxConcurrent+1` requisições simultâneas: a excedente sai 503 com `Retry-After` **sem tocar o Store**, e `nishidb_query_active` volta a zero no fim.
6. **Testes do §106 pelo caminho HTTP**, não só pelo engine.

**Manual, a rodada de aceite (T7):**

```bash
# 1. servidor com ingestão Kafka rodando (docker compose do M2)
nishidb server --config /etc/nishidb/nishidb.yaml --log-format texto

# 2. consultar ENQUANTO ingere — é o débito 54, e o motivo desta fase existir
curl -sS -H 'Content-Type: application/json' \
  -d '{"dataset":"radius_sessions","sql":"SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100","parameters":[{"type":"string","value":"Stop"}]}' \
  http://127.0.0.1:9142/v1/query

# 3. o mesmo pelo CLI, que é o caminho do operador
nishidb query --server 127.0.0.1:9142 --dataset radius_sessions 'SELECT id, ip FROM radius_sessions LIMIT 10'

# 4. metadados, stats e as métricas novas
curl -sS http://127.0.0.1:9142/v1/metadata | jq .
curl -sS http://127.0.0.1:9142/v1/stats | jq .
curl -sS http://127.0.0.1:9142/metrics | grep nishidb_query_
```

Critério: o passo 2 responde **com a ingestão de pé** (nenhum exit 3, nenhum SIGTERM no meio), o `nishidb_kafka_lag` não degrada durante as consultas, e as cinco séries `nishidb_query_*` aparecem preenchidas.

---

## 8. Documentação (Definition of Done)

- [ ] `docs/operacao-kafka.md`: **remover** o aviso das linhas 8-9 ("não há como consultar") e documentar o ciclo novo.
- [ ] `README.md` linhas 247-259: reescrever o bloco que manda esperar o M3; quickstart ganha o `curl` do `/v1/query`.
- [ ] `docs/architecture.md`: seção do protocolo remoto (linha 162 hoje diz que ele é do M3).
- [ ] `docs/diagrams/sequence_query_http.puml` novo — request, semáforo, iterador, lotes de flush, fecho — incorporado via `uml.nishisan.dev`, sintaxe validada.
- [ ] `docs/debitos.md`: **fechar** 11, 54, 10, 13 e 18; **fechar em parte** 15 (`maxConcurrent` sim; `memoryTarget` e `maxBufferPerQuery` seguem abertos) e 16 (métricas de query sim; o resto do §92 já estava). **Abrir/anotar**: débito 34 (`Stats` engole `ErrDatasetClosed`, agora que virou superfície pública) e o risco de exposição que o D1 mitiga mas não elimina, com dono na Fase 11.
- [ ] `planning/m3-fase6-plano.md`: cópia deste plano, conforme o CLAUDE.md.

---

## 9. Fora de escopo

| Item | Por quê |
|---|---|
| Driver JDBC (§70-73) | Fase 7, issue própria — **depende** desta |
| Argon2id, roles, usuários, TLS (§74-77) | Fase 11 (§123). O D1 mitiga o flanco sem antecipar a fase |
| `runtime.memoryTarget`, `query.maxBufferPerQuery` (§80) | Ficam no débito 15; o `maxBufferPerQuery` é o débito 28 (o `StreamWriter` que nunca encolhe) e mexe no encoder |
| Formato binário de resposta | O §69 o cita como "futuramente" |
| `ORDER BY`, `GROUP BY`, JOIN, agregações | Fora da v1 (§9) |
| `nishidb status`, `datasets`, `inspect` (§96) | Débito 17; o `/v1/stats` cobre a informação, não o comando |
