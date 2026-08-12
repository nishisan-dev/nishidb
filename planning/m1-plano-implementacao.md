# NishiDB v1 — Plano de Implementação (Roadmap + Milestone 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o NishiDB — storage engine Go especializado em materialização de estado a partir de eventos (Kafka na v1 completa), com 8M entidades sob RSS < 1 GiB — priorizando o núcleo consultável (storage + materializer + índices + SQL) no Milestone 1.

**Architecture:** Append-only segments + índices mmap disk-backed (primary hash, locations, bitmap), single writer, materialização merge/replace dirigida por YAML, SQL v1 próprio com resultados streaming. Disco é storage; RAM é working memory — heap desacoplado do nº de entidades.

**Tech Stack:** Go 1.25+, stdlib prioritária, `gopkg.in/yaml.v3`, `golang.org/x/sys` (mmap). Futuro: `franz-go` (M2), Java 21+ (JDBC, M3).

## Contexto

- **Por quê:** O NOC precisa consultar estado corrente de ~8M sessões RADIUS (e datasets similares) alimentado por Kafka, sem pagar o custo de um banco genérico. A spec completa está em `nishidb-spec-proposal.md` (fonte de verdade, 131 seções).
- **Critério de sucesso do usuário (explícito):** além dos itens funcionais, o sucesso real é **executar queries e filtros com baixo uso de recursos**. Por isso o M1 prioriza o núcleo consultável e valida memória desde o início — Kafka fica para o M2.
- **Estado atual:** greenfield total. O diretório não é nem repositório git; contém apenas a spec.

## Decisões já tomadas (com o usuário)

1. **Milestones:** núcleo consultável primeiro (Fases 1, 2, 4, 5 da spec no M1); Kafka no M2.
2. **Testes Kafka (M2):** `kfake` (franz-go) para unit + Testcontainers/Redpanda para integração.
3. **Module path:** `github.com/nishisan-dev/nishidb`.
4. **Dados:** gerador sintético determinístico de eventos RADIUS (sem depender de dumps reais).

## Global Constraints

Copiadas da spec — valem para TODAS as tarefas:

- Go `1.25` ou superior; prioridade absoluta à stdlib; dependências externas mínimas.
- `RSS < 1 GiB` com 8.000.000 entidades, 500 eventos/s, queries concorrentes (seções 4, 109).
- Heap ~estável entre 1M e 8M rows (seção 110). PROIBIDO `map[string]*Struct` com milhões de entradas.
- PROIBIDO: dependência de outro DB, dataset completo em memória, milhões de objetos GC, filas ilimitadas, update in-place, multi-writer, JOIN, buffering de resultado proporcional ao resultado (seção 128).
- Single Writer: apenas 1 goroutine faz mutations definitivas (seção 27).
- Query memory = O(batch size), nunca O(result count) (seção 66).
- Índices e estruturas por-row são disk-backed/mmap (seções 34-38).
- CRC32C por registro (seção 52). Publicação de Location atômica por row (seção 45).
- Prioridades: 1 correção, 2 baixo footprint, 3 simplicidade, 4 previsibilidade, 5 durability, 6 performance, 7 features (seção 127).
- Hot path de ingestão: meta single-digit allocations/evento pós-otimização; benchmark orienta, sem sacrificar clareza (seção 111).
- Sem parser JSON em `map[string]any` como modelo principal (seção 20). Sem parser SQL externo (seção 62).
- Commits atômicos; toda mudança de produção acompanhada de teste; suite verde antes de cada commit.
- Mensagens de commit na voz do time, sem referências a agente/IA.

## Roadmap de Milestones

| Milestone | Escopo (fases da spec) | Entregável testável |
|---|---|---|
| **M1 — Núcleo consultável** (este plano) | F1 Storage, F2 Materializer, F4 Bitmap, F5 Query engine + gerador sintético + harness de memória | Binário que ingere eventos JSON de arquivo/gerador, materializa 8M entidades, responde SQL via CLI local, RSS < 1 GiB comprovado |
| **M2 — Ingestão Kafka** | F3 Kafka (franz-go, bounded queue, offsets, checkpoint, seek, recovery) | Ingestão fim-a-fim de tópico real; sobrevive kill -9 sem perder estado confirmado |
| **M3 — Protocolo remoto + JDBC** | F6 HTTP, F7 JDBC thin driver, F11 Auth/TLS | Grafana/Java consultando remoto com auth+TLS |
| **M4 — Ciclo de vida + escala** | F8 Hash index, F9 TTL/tombstones, F10 Compaction, F12 Memory opt, F13 Benchmark 8M/500eps/1GiB | Critérios de aceite completos da seção 126 |

Cada milestone terá seu próprio plano detalhado; este documento detalha o **M1**. Planos aprovados são copiados para `/planning` no repositório.

---

# Milestone 1 — Detalhamento

Documentos de design completos (anexos deste plano, copiar para `/planning` junto com este arquivo na aprovação):

- **Materializer + Query:** `estamos-em-greenfield-novo-noble-firefly-agent-aplan-query-27060c299b110a31.md`
- **Storage + Índices:** `estamos-em-greenfield-novo-noble-firefly-agent-aplan-storage-21876217e42f0ad3.md` (a seção 15A registra o contrato final acordado entre os dois ramos)

## Decisões de design — Materializer + Query Engine

| # | Decisão | Escolha |
|---|---------|---------|
| D1 | Parser JSON | **Tokenizer próprio sobre `[]byte`** (`internal/jsonp`, pull-iterator, ~500-600 LOC + fuzz). `Decoder.Token()` reprovado (150–250 allocs/evento); `json/v2` reprovado (experimental, exige GOEXPERIMENT em todo build) |
| D2 | Semântica NULL | **NULL = ausência do campo no record.** Patch com `null` explícito remove o campo no merge; `IS NULL` = campo ausente. Sem flag de null no encoding |
| D3 | Layout do payload | Campos **ordenados por FieldID crescente** → merge vira merge linear de duas listas ordenadas O(n+m), deltas de índice capturados no mesmo passo |
| D4 | Re-validação §44 | Executor **sempre reavalia o WHERE completo** contra o estado corrente; índices só geram candidatos (superset). Zero cálculo de residual no planner |
| D5 | Coerção SQL | Literais coeridos em **plan-time** com tabela normativa fixa (timestamp: só RFC3339 + epoch millis; ipv4 compara como uint32; prefixes só `=`/`!=`; campo ausente compara como false) |
| D6 | Valores sem boxing | `types.Value` struct-união (`Type` + `I64` + `Bytes`), nunca `interface{}` no hot path |
| D7 | `[]byte`→`string` | Um único helper auditado `internal/unsafex.String` para callees que não retêm |
| D8 | Mongo Extended JSON | Desembrulhado inline no parse (`$numberLong`→int64, `$date` relaxed E canonical→timestamp), wrapper nunca materializado |
| D9 | Buffers | Todos reutilizáveis, owned pela goroutine única do writer; **sem** `sync.Pool` (§21). Alvo: 0–3 allocs/evento no `Apply` |
| D10 | OR no planner | OR só usa índices se **todos** os disjuncts forem indexáveis; senão full scan (união parcial perderia matches) |
| D11 | Gerador RADIUS | `internal/testutil/radiusgen`, determinístico (`rand/v2` PCG + seed), `Next(dst []byte)` ~0 alloc, cardinalidades configuráveis (8 status, ~300 NAS zipf-leve, ip 1:1 entidade) |
| D12 | Medição de memória | `/proc/self/status` (VmRSS/VmHWM) + `smaps_rollup` (separa mmap file-backed de heap anônimo, §79) + `ReadMemStats`; `b.ReportAllocs()` como gate desde a primeira tarefa |

**SQL v1:** grammar EBNF formal (SELECT/FROM/WHERE/LIMIT; `=`,`!=`,`>`,`>=`,`<`,`<=`, IS [NOT] NULL, AND>OR, parênteses, `?`, quoted idents para nomes com hífen). Lexer hand-written + recursive descent + binder (resolve alias/nome, coage literais) + planner por regras (PK lookup → hash → bitmap intersect → bitmap → union → full scan) + executor pull-based com Row zero-copy válida até o próximo `Next()`, LIMIT early-exit, ctx por batch (256), maxRows implícito. Encoder NDJSON incluído no M1.

**Harness:** `benchmark/ingest-bench` com fases LOAD (8M) → STEADY (500 ev/s + queries concorrentes) → REPORT (JSON com gates automatizados: §109 RSS<1GiB, §110 heap(8M)<2×heap(1M), §111 allocs/evento reportado). Flag `-storage fake|real` permite validar a mecânica antes do storage real.

## Decisões de design — Storage + Índices

| # | Decisão | Escolha |
|---|---------|---------|
| S1 | Endianness / CRC | Little-endian em tudo; CRC32C (Castagnoli) da stdlib, acelerado por HW |
| S2 | Record format | Header fixo 36B (magic `NDBR`, version, flags, keyLen u16, rowId, sequence, fieldCount, payloadLen, crc32c) + **key explícita** + payload ordenado por FieldID. CRC cobre header+key+payload |
| S3 | Hash da primary | FNV-1a 64 hand-rolled + finalizer fmix64 (murmur3). `maphash` rejeitado (seed não serializável), xxhash rejeitado (dep externa). `hashKind` no header permite troca com rebuild |
| S4 | primary.idx | Open-addressing linear probing, mmap, 16B/slot (hash u64 + rowId u64), load factor 0.70, pre-sizing por `EstimatedRows` (8M → 256 MiB disk-backed). Resize = rebuild em `.new` + rename + remap. Publicação de slot com store-release no hash; validação de colisão lê a key real do record |
| S5 | **Publicação atômica de Location (§45)** | **uint64 empacotado: flags(2) \| segmentID(32) \| offset(30) — SEM length** (recuperado do header CRC-protegido do record). `atomic.Store/LoadUint64` resolve a atomicidade trivialmente; locations.idx cai de 128 MB → **64 MB**. Packing 12-bit de segmentID rejeitado (esgotaria em ~50 dias a 30-43 GB/dia); seqlock/generation rejeitados (complexidade sem benefício) |
| S6 | **Durabilidade das locations** | **Overlay em heap bounded** (~15k entries ≈ 1-2 MB por intervalo de checkpoint de 30s) aplicado ao mmap **somente no checkpoint, após fdatasync dos segmentos**. Fecha o buraco de power loss (writeback de página do índice antes do fsync do segmento → dangling pointer). Invariante: página do locations.idx em disco só referencia records duráveis |
| S7 | Leitura de segments | `pread` em buffer reutilizável do caller (1º pread de 4 KiB cobre ~99% dos records; 2º só para maiores). **Sem mmap de segments** (evita expor tail parcial). Escrita: `write()` direto + fdatasync no batch |
| S8 | mmap | `golang.org/x/sys/unix` (Mmap/Msync/Munmap/**Fallocate obrigatório** — evita SIGBUS por disco cheio). `unsafe` confinado em `internal/storage/mmap.Region` com asserts de alinhamento. Remap sob RWMutex |
| S9 | Bitmap indexes | Bitmap plano file-backed (1 MB/valor p/ 8M rows), 1 arquivo por valueID + `values.dict` append-only. `atomic.Or/AndUint64` por word. Bits levemente stale OK — re-validação §44 filtra falsos positivos |
| S10 | fields.dict / values.dict | Append-only com CRC por entry; **fsync da entry ANTES do primeiro record que usa o ID** (invariante de replay). FieldID implícito pela ordem (u16) |
| S11 | Checkpoint/recovery | Manifest reescrito atomicamente (tmp+rename); checkpoint roda NO writer (fdatasync segmento → aplica overlay → msync índices → manifest). Recovery: manifest → scan dicts → mmaps → tail scan com CRC+bounds a partir de `durableOffset`, replay idempotente (insert-if-absent, overwrite incondicional), ftruncate no primeiro inválido |
| S12 | rowId | uint64 monotônico, nunca reutilizado (proíbe ABA), recuperado como max(manifest, tail) |

## Contrato acordado entre subsistemas (negociado pelos dois ramos — final)

1. **`internal/types`** (leve, zero deps): `FieldType uint8`, `FieldID uint16`, `RowID uint64`, `Value{Type; I64 int64; Bytes []byte}` (sem Float — double via `math.Float64bits` em I64), `Record{RowID; Sequence int64; Flags uint8; Key []byte; Payload []byte}`. `PhysicalLocation` fica **interno** ao storage. Interfaces declaradas nos consumidores (idiom Go).
2. **Encoding**: dono é `internal/storage/encoding` — `AppendField(dst, id, v)` + `FieldReader` zero-copy, campos ordenados por FieldID (pré-condição do merge). Tag null (0x00) **reservada e nunca codificada**: patch carrega `isNull` como flag de struct; estado materializado dropa o campo (NULL = ausência, decisão D2).
3. **`Store.Put` orquestra o write path completo** (append → validate → primary insert-if-absent → bitmap diffs via `prev` → sequence → publish location). O `Apply` do materializer vira: `GetByKey(prev) → stale-check via prev.Sequence → merge/replace → Put(key, seq, flags, payload, prev)`. Um único code path para Put e replay de recovery; o risco de postings órfãos no replace **morre por construção**.
4. **Assinaturas do Store**: `GetByKey(key, buf) (types.Record, []byte, bool, error)` (prev vem "de graça" da validação de colisão); `GetByRowID(id, buf)` (ok=false = sem location ou tombstoned = checagem de liveness); `Put(key, seq, flags, payload, prev) (RowID, created, error)`; `ScanLive() RowIDIterator` (full scan, rowIds crescentes); `LookupBitmap(fieldID, value) (RowIDIterator, error)` (ordem crescente garantida — pré-condição das interseções/uniões do planner); `Sequence(rowID) (int64, bool)`; `Sync()`; `Checkpoint()`.
5. **Logística**: T0 (go.mod, layout, Makefile, lint) é do ramo storage; ramo query adiciona `gopkg.in/yaml.v3` na primeira task dele.

## Ajustes pós-amostra real de payload (fornecida pelo usuário)

A amostra real de evento RADIUS (BLM-816P7LRL2Y-013, ~43 campos) confirma o design e força 3 ajustes:

1. **Coerção string→tipo declarado**: `Connect-Info` chega como string `"1000000000"` mas o YAML o declara `int64`. Regra v1: quando o tipo do campo é **declarado** no YAML, strings numéricas/parseáveis são coeridas (int64, double, timestamp, ipv4, ipv6, prefixes já aceitam string). Falha de parse → `TYPE_CONVERSION` com política §101. Em `type: auto`, string permanece string (sem adivinhação).
2. **Timestamps com offset**: formato real é `2026-08-12T21:00:08.000+00:00` (offset, não `Z`) — `parseRFC3339Millis` já aceita `±hh:mm`; caso de teste obrigatório.
3. **Extended JSON é opcional**: a amostra não tem wrapper Mongo algum (`_SEQ` é number puro). Detecção permanece oportunista (já era); default do gerador `ExtendedJSONRatio` cai para 0.10 e o corpus de teste inclui o shape 100% plano.

A amostra completa deve ser commitada como `testdata/radius_event_sample.json` (fixture dos testes T8/Q-T17) — está registrada na conversa e no Apêndice A deste plano.

## Ordem de execução e branches

```text
S0 (scaffolding) → S1 (internal/types) → S2 (encoding)   ← desbloqueiam os DOIS ramos
        │
        ├── branch feature/m1-storage: S3…S14 (mmap, hash, segment, primary, rowindex, dict, bitmap, Store, checkpoint, recovery, bench)
        └── branch feature/m1-query:   Q2…Q19 (unsafex, jsonp, typeconv, schema, fakes, materializer, SQL engine, radiusgen, memprof, benchmarks)
        │
        └── integração (I1–I3): materializer+query sobre Store real, CLI mínimo, rodada 8M com gates
```

Commits atômicos; S0–S2 direto na main (bootstrap), depois os dois ramos em branches paralelas com merge na main ao fim de cada bloco estável. Cada tarefa segue TDD (teste primeiro → red → green → commit). O detalhe passo a passo com código dos testes está nos dois anexos; numeração abaixo = anexo storage (S*) e anexo query (Q-T*).

## Tarefas M1 — ramo Storage + Índices (S0–S15)

- [ ] **S0** Scaffolding: `git init`, `go.mod` (`github.com/nishisan-dev/nishidb`, go 1.25), layout §98 + `cmd/nishidb` stub, `Makefile` (build/test/test-race/lint/bench/cover), `.golangci.yml`, `.gitignore`, `planning/`, `docs/diagrams/`; smoke `go build ./...`
- [ ] **S1** `internal/types` (FieldType, FieldID, RowID, Value, Record — forma final do contrato) + `internal/storage/encoding` tipos e roundtrip por tipo, uvarint bounds
- [ ] **S2** `encoding.RecordBuilder`/`ParseRecord`/`FieldIter` + `AppendField`/`FieldReader`: ordem de FieldID exigida, CRC ok/corrompido, tag null rejeitada, bounds (§104: CRC, invalid CRC)
- [ ] **S3** `internal/storage/mmap.Region`: create+Fallocate+map, grow+remap com swap sob lock, atomic ops com assert de alinhamento, msync, reopen preserva conteúdo (§104: mmap reopen)
- [ ] **S4** Hash FNV-1a 64 + fmix64: golden vectors, determinismo, smoke de distribuição
- [ ] **S5** `segment.Writer/Reader`: append+read roundtrip, rotação, reopen p/ append, CRC inválido, two-pread p/ records > 4 KiB (§104: append, read, segment rotation)
- [ ] **S6** `index/primary`: insert/find 100k, colisão forçada com validação via callback, sentinelas, Grow preserva tudo, insert-if-absent idempotente, reopen
- [ ] **S7** `rowindex` (Locations packed u64 + overlay + Sequences): pack/unpack bounds, overlay publish/apply/clear, grow lazy, reopen
- [ ] **S8** `fielddict`: IDs sequenciais, rescan com CRC, tail rasgado → ftruncate, fsync-on-new, limite u16
- [ ] **S9** `index/bitmap`: set/lookup/clear, iterator ordenado, values.dict, grow lazy, reopen
- [ ] **S10** `storage.Store` façade + write path (Put orquestrado): put/get por key e rowId, update mantém rowId, bitmap diff em update, location como commit point com leitor concorrente sob `-race`
- [ ] **S11** `checkpoint.Manifest` + `Store.Checkpoint`: encode/decode+CRC, rename atômico, corrupção → erro (§104: checkpoint)
- [ ] **S12** Recovery/tail replay: write N → checkpoint → write M → reopen → N+M visíveis (**critério Fase 1: write → restart → lookup by id**); tail incompleto → ftruncate; replay reconverge bitmaps/sequences; segmentos pós-checkpoint descobertos (§104: restart, replay, incomplete tail)
- [ ] **S13** Integração + bench: property test vs oracle com reopen aleatório; **crash test com subprocesso + SIGKILL** (`//go:build integration`); `BenchmarkPut`/`GetByKey` com `AllocsPerRun` (meta single-digit); heap-flatness 1M vs 8M
- [ ] **S14** Stats de liveness por segmento no manifest (groundwork da Fase 10; compactor fora do M1)
- [ ] **S15** Docs: `docs/architecture-storage.md` + PlantUML (write flow, recovery flow, C4 component)

## Tarefas M1 — ramo Materializer + Query (Q2–Q20)

*(Q-T0/T1/T6 do anexo foram absorvidas por S0/S1/S2; Q-T7/Q-T10 ajustadas ao contrato "Put orquestra".)*

- [ ] **Q2** `internal/unsafex.String` (único `unsafe` fora de storage/mmap, contrato documentado)
- [ ] **Q3** `internal/jsonp` tokenizer + fuzz; aceite: `BenchmarkParseRadiusEvent` = 0 allocs/op
- [ ] **Q4** `internal/materializer/typeconv`: tabela JSON→binário célula a célula, **incl. coerção string→tipo declarado** (`Connect-Info: "1000000000"` → int64) e timestamps com offset `+00:00` (parsers próprios: RFC3339→millis, dotted-quad, int; `netip` p/ ipv6)
- [ ] **Q5** `internal/schema`: YAML load + validação + `Compiled` (lookup por `[]byte` sem alloc; unknown fields via fields.dict; seção kafka aceita e ignorada no M1)
- [ ] **Q7** `internal/testutil/fakestorage`: fake in-memory da superfície do Store (GetByKey/GetByRowID/Put/ScanLive/LookupBitmap/Sequence)
- [ ] **Q8** `PatchBuffer.Parse`: evento → entries ordenadas/tipadas (wildcard, exclude, unknownFields ×3, INVALID_KEY/INVALID_SEQUENCE, Extended JSON opcional, last-wins; fixture = amostra real do Apêndice A)
- [ ] **Q9** `mergeRecords`: merge linear O(n+m) (casos §103: sobrescreve, mantém ausente, null remove, create) — 0 allocs
- [ ] **Q10** `Materializer.Apply`: GetByKey → stale-check (greater/equal/lower ×3) → merge/replace → Put; stale sem side effects; categorias de erro §101; métricas
- [ ] **Q11** `internal/query/lexer` (tokens, keywords case-insensitive, escapes, posições de erro)
- [ ] **Q12** `internal/query/parser` + AST (precedência AND>OR, IS [NOT] NULL, placeholders, quoted idents, LIMIT)
- [ ] **Q13** Binder (resolução alias/original/quoted, tabela de coerção célula a célula — mesmos parsers do typeconv, param count)
- [ ] **Q14** Planner por regras (todos os casos de seleção, incl. OR degradando para full scan; hash index só entra na M4 — regra fica com slot previsto)
- [ ] **Q15** Executor streaming (casos §106 completos, re-validação §44 com índice deliberadamente stale, LIMIT early-exit provado, maxRows implícito, timeout ctx, 0 allocs/row)
- [ ] **Q16** Encoder NDJSON (formatação por tipo, escaping, O(batch))
- [ ] **Q17** `internal/testutil/radiusgen` (determinismo byte a byte, oráculo `encoding/json`, distribuições, `ExtendedJSONRatio` default 0.10, ~0 alloc)
- [ ] **Q18** `internal/testutil/memprof` (VmRSS/VmHWM/smaps_rollup, fixtures de /proc, Linux build tag)
- [ ] **Q19** Micro-benchmarks integrados com baselines commitados (`benchmark/baselines/`)
- [ ] **Q20** `benchmark/ingest-bench` harness (LOAD→STEADY→REPORT, relatório JSON, gates §109/§110/§111; smoke com `-storage fake`)

## Tarefas M1 — Integração (I1–I3)

- [ ] **I1** Materializer + Query sobre Store real: teste de integração fim-a-fim (ingerir corpus radiusgen → restart → queries por PK/bitmap/full scan conferidas contra oracle)
- [ ] **I2** CLI mínimo no `cmd/nishidb`: `nishidb ingest --dataset <yaml> --input <ndjson>` e `nishidb query --dataset <nome> "SELECT …"` locais (sem servidor — o server HTTP é M3); é o entregável demonstrável do M1
- [ ] **I3** Rodada de aceite: `ingest-bench -storage real -entities 8000000` sob `GOMEMLIMIT=512MiB GOGC=100`; gates §109 (RSS < 1 GiB) e §110 (heap flat) verdes; baselines e relatório commitados; `docs/architecture.md` consolidado + diagramas

## Desvios deliberados da spec (para sua ciência/aprovação)

1. **Key explícita no header do record** (spec §31 não a menciona): custo ~20-30 bytes/record; necessária para validar colisão de hash sem schema (§36), tombstones sem payload (§58) e replay do tail reconstruir a primary.
2. **locations.idx com 8 bytes/entry (packed u64, sem length)** — spec §37 sugeria 16 bytes com length: resolve a atomicidade da publicação (§45) com um único `atomic.StoreUint64`, elimina o esgotamento de segmentID (u32 monotônico) e reduz o arquivo pela metade (64 MB). Length vem do header CRC-protegido do record. Consequência: `segmentSize` máx 1 GiB (offset em 30 bits) — validado na config.
3. **Overlay de locations em heap bounded** (~1-2 MB por intervalo de checkpoint): as publicações entre checkpoints só vão ao mmap após o fdatasync dos segmentos — fecha dangling pointer em power loss. Não viola a filosofia de memória (bounded pelo intervalo, não pelo dataset).
4. **Coerção string→tipo declarado no YAML** (motivada pela amostra real): a spec §17 não define; sem isso, eventos reais com `Connect-Info: "1000000000"` seriam rejeitados.

## Riscos principais (top 5 de 12 mapeados; lista completa no anexo)

1. **Semântica NULL** vazando inconsistência entre materializer/índices/`IS NULL` → decisão única D2 + testes cruzados T9/T10/T15.
2. **Alocações escondidas** (string conversions, boxing, `time.Parse`, closures) → `ReportAllocs` como gate desde T3, `unsafex` único auditado.
3. **Coerção SQL divergindo do materializer** → binder e typeconv compartilham os mesmos parsers.
4. **OR com união parcial de índices** = resultado silenciosamente incompleto → regra dura D10 + teste dedicado.
5. **Replace órfão de postings** (esquecer de ler estado anterior p/ limpar índices) → regra explícita + teste T10.

## Verificação (Definition of Done do M1)

- [ ] `go test ./... -race` verde; fuzz do jsonp ≥ 5 min sem crash.
- [ ] Todos os casos obrigatórios §103 (materialização), §104 (storage, exceto compaction = Fase 10/M4) e §106 (query, exceto hash lookup = Fase 8/M4) com teste nomeado rastreável.
- [ ] Micro-benchmarks: parser/typeconv/merge/executor-row = 0 allocs; baselines commitados.
- [ ] Crash test: subprocesso + SIGKILL no meio da ingestão → reopen sem perder estado checkpointado (S13).
- [ ] "write → restart → lookup by id" (critério da Fase 1, spec §113) demonstrado por teste de integração (S12/I1).
- [ ] CLI local funcional: `nishidb ingest` + `nishidb query` sobre a amostra real e corpus sintético (I2).
- [ ] `ingest-bench` fim-a-fim: rodada 8M entidades com storage real, `GOMEMLIMIT=512MiB GOGC=100`, gates §109 (RSS < 1 GiB) e §110 (heap 8M < 2× heap 1M) verdes (I3).
- [ ] `docs/architecture.md` + diagramas PlantUML (`docs/diagrams/`) atualizados e renderizando via uml.nishisan.dev.
- [ ] Plano + anexos copiados para `/planning` no repositório (primeiro commit após S0).

## Apêndice A — Amostra real de evento (fixture `testdata/radius_event_sample.json`)

```json
{
  "_id": "BLM-816P7LRL2Y-013",
  "Acct-Delay-Time": 0,
  "Acct-Multi-Session-Id": "i-br-pa00401012300272120899AAAaaH",
  "Acct-Session-Id": "i-br-pa00401012300272ace297AAAO6W",
  "Acct-Session-Time": 475200,
  "Acct-Status-Type": "Alive",
  "Class": "#S:Abie71#T:BLM-816P7LRL2Y-013#C:117673879#N:BLM-816P7LRL2Y-013#L:361#M:68:d4:0c:6a:f5:f1#I:",
  "Event-Timestamp": "2026-08-12T21:00:08.000+00:00",
  "EventExpireDate": "2026-08-19T03:00:00.000+00:00",
  "Framed-IP-Address": "191.31.31.162",
  "Framed-IPv6-Prefix": "2804:1b2:d102:b1d::/64",
  "Framed-Interface-Id": "0x1d4c510512bf4628",
  "Framed-Protocol": "PPP",
  "LastSessionStartedDate": "2026-08-07T09:00:08.219+00:00",
  "NAS-IP-Address": "187.100.170.241",
  "NAS-Identifier": "i-br-pa-blm-ccp-hl4-02",
  "NAS-Port": 504080,
  "NAS-Port-Id": "slot=0;subslot=0;port=0;vlanid=272;vlanid2=123;",
  "NAS-Port-Type": "Ethernet",
  "Service-Type": "Framed-User",
  "User-Name": "cliente@cliente",
  "_insertedDate": "2023-03-21T06:43:21.730+00:00",
  "_lastUpdate": "2026-08-12T21:00:13.714+00:00",
  "id": "BLM-816P7LRL2Y-013",
  "Acct-Input-Octets": 283422031,
  "Acct-Input-Packets": 951023,
  "Acct-Output-Octets": 2613239581,
  "Acct-Output-Packets": 2346108,
  "Delegated-IPv6-Prefix": "2804:1b2:d080:4ea2::/64",
  "LastSessionAliveDate": "2026-08-12T21:00:13.065+00:00",
  "Acct-Output-Gigawords": 0,
  "Acct-Terminate-Cause": "Session-Timeout",
  "LastSessionStoppedDate": "2026-08-07T09:00:04.258+00:00",
  "RemoteServerAddr": "192.168.201.114",
  "PSA-SERVER": "192.168.201.114",
  "Acct-Input-Gigawords": 0,
  "_SEQ": 7479960553,
  "Acct-Authentic": "RADIUS",
  "Calling-Station-Id": "i-br-pa-blm-ccp-hl4-02#0/0#123:272",
  "Connect-Info": "1000000000",
  "Filter-Id": "hsi",
  "Idle-Timeout": 0,
  "Session-Timeout": 766800
}
```

Pontos que a amostra valida: chave em `id` (com `_id` duplicado a armazenar como campo comum); `_SEQ` como number puro (sem wrapper Mongo); timestamps com offset `+00:00` e millis; `Connect-Info` string-numérica; `RemoteServerAddr` ausente do YAML da spec (exercita `unknownFields: include` + `type: auto`); `Acct-Output-Octets` > int32.
