# Feed de replicação em disco e checkpoint do publicador — plano de implementação

> **Para agentes:** sub-skill obrigatória: superpowers:subagent-driven-development.
> Cada tarefa tem um Builder e um Refuter Opus, e o branch inteiro passa por
> revisão no fim. Os passos usam checkbox (`- [ ]`).

**Objetivo:** o feed de replicação passa a reter as alterações em disco, com
retenção por bytes e memória limitada a um cache. Cada destino Kafka retoma o
tail do próprio checkpoint depois de um reinício limpo, sem `reset`.

**Arquitetura:**
- O `replication.Stream` grava os lotes fechados num log de segmentos dentro do
  dataset, numa goroutine própria, e serve as leituras da memória ou do disco.
- Um arquivo `state`, com a marca `clean` e a impressão dos Stores, decide na
  subida entre continuar a geração e nascer de novo (queda).
- O publicador Kafka grava um `.ckpt` por destino. Na subida, ele confere o
  topo do tópico contra o feed (backup restaurado) e decide, por uma tabela
  fixa, entre retomar e fazer `reset` com um dos dez motivos.

**Stack:**
- Go 1.26.5: prefixe TODO comando com `GOTOOLCHAIN=go1.26.5 GOPROXY=off`.
- franz-go: `kgo` e `kadm`, mais `kfake` nos testes.
- CRC32C (Castagnoli).
- `internal/storage/atomicfile`.

**Desenho:** `planning/replicacao-feed-em-disco.md`. O executor lê o desenho e
este plano. As divergências do plano em relação ao desenho, todas vindas da
refutação, estão listadas no fim do plano e já foram registradas no desenho.

## Restrições globais

- **Idioma:**
  - Prosa (godoc, comentários, `t.Errorf`, docs e commits) em PT-BR.
  - Em inglês: identificadores, nomes de teste, mensagens de erro e de log, chaves de log, config, wire e métricas.
  - Nome de teste segue o formato sujeito-ação-condição (`TestFeedResumesSameEpochAfterCleanClose`).
- **`Publish`:** nunca faz I/O, nunca bloqueia esperando leitor ou disco, e nunca devolve erro ao Store. Ele roda sob o `wmu`.
- **Caminho quente:** nada de `fmt` nem de `encoding/json` no caminho por entrada do feed nem nos `envelope*.go`. O gate de AST que já existe cobre os envelopes.
- **Mutação:** toda garantia escrita em godoc tem teste que fica VERMELHO com a mutação dela. O relatório traz quatro coisas: a mutação aplicada, o vermelho visto, a reversão e o verde.
- **Mutantes de alocação:** o mutante tem de alocar de verdade (`fmt.Sprintf`), e não uma concatenação curta.
- **Formato em disco:** o quadro, o cabeçalho de segmento, o `state`, o `.ckpt` e o `.resync` têm golden test de bytes, com o literal no próprio teste.
- **Listas fechadas** (motivos de reset, motivos de erro, ops, colunas): asseveradas como LITERAIS nos testes, nunca comparadas com a função do código.
- **Documentação no mesmo commit:** contrato novo do wire, motivo, coluna e métrica entram em `docs/glossario.md` e em `docs/replication.md`/`docs/metricas.md` no mesmo commit do código. O glossário é editado só pelo orquestrador: o Builder lista os termos no relatório.
- **Commits:** na voz do time, sem menção a agente. Sempre `git add` explícito, nunca `-A` nem `.`. O Builder NÃO commita; quem commita é o orquestrador, por bloco.
- **Antes de cada commit de bloco:** `make test`, `make test-race` e o lint ratchet verdes. A contagem do lint vale com `git archive HEAD | tar -x -C <tmp>` e o linter rodado lá. O relatório traz a contagem de testes.
- **Comando longo:** roda em foreground. Se não couber, o resultado sai marcado PENDENTE.
- **Contrato publicado:** o envelope Kafka e o `/v1/replication/*` saíram na `v0.14.x`. Toda mudança deste ciclo no wire é ADITIVA:
  - motivos novos;
  - a chave opcional `previousEpoch`, depois de `reason`;
  - o op `resume`;
  - o header `nishidb-reason`;
  - o campo `historyExcluded` do cursor, cujo ausente vale o legado.

  Tudo entra em `docs/replication.md`, na seção "Mudanças de contrato desde a v0.14".
- **Registro de rota:** é `rt.register(pattern, role, handler)` (`internal/server/auth.go:78`). O `CLAUDE.md` do projeto, que ainda diz `registra`, é corrigido no commit de docs.

## Foco de revisão

1. **Disco lento, travado ou cheio, com a ingestão no máximo.** A ingestão não desacelera e a memória do feed fica limitada. O feed só declara lacuna quando os lotes não gravados lotam `maxBufferedBytes`. Teste na Task 3: `TestPublishNeverBlocksWhenTheWriterStalls`, que assevera memória e não tempo.
2. **Backup restaurado com o Kafka fora na subida.** O destino nunca retoma: a comparação usa a posição do instante do `Resume`, e não a corrente. Teste na Task 9: `TestRewindDetectedEvenWhenFeedAdvancesBeforeTopicCheck`.
3. **Encerramento com mutação em voo** (passada de expiração, `/v1/compact`, retenção). O reopen é `Clean` E a mutação está no feed. Testes: `TestShutdownStopsProducersBeforeClosingTheFeed` (Task 6) e `TestLatePublishDuringCloseNeverEndsClean` (Task 3).
4. **Follower HTTP no ar durante o reinício limpo do master.** Ele retoma o tail sem snapshot. Teste na Task 6: `TestFollowerResumesTailAcrossMasterCleanRestart`.
5. **Reinício no meio de um snapshot.** A subida seguinte faz `reset` e nunca retoma o tail de uma posição antiga. Teste na Task 9: `TestRestartDuringSnapshotNeverResumesTail`.

---

## Mapa de arquivos

| Arquivo | Responsabilidade |
|---|---|
| `internal/replication/diskframe.go` (novo) | Codificação/decodificação do quadro (um lote) e do cabeçalho de segmento |
| `internal/replication/disklog.go` (novo) | Log de segmentos: append, rotação, índice esparso, retenção, leitura por `ReadAt` com contagem de referências |
| `internal/replication/feedstate.go` (novo) | Arquivo `state` e os tipos `Fingerprint`/`StorePosition` |
| `internal/replication/stream.go` | Disco + cache + gravador; `Open`/`Resume`/`Close(fp)`/`Wait`/`Rotate` |
| `internal/replication/client.go`, `resync.go` | Follower retoma o tail depois de desconexão; recusa master sem histórica |
| `internal/storage/store.go` | `Store.WritePosition()` |
| `internal/schema/spec.go` | `maxDiskBytes`, `publishHistory`, `periodicSnapshot` |
| `internal/replicationkafka/checkpoint.go` (novo) | `.ckpt` e `.resync` por destino |
| `internal/replicationkafka/envelope.go`, `publisher_batch.go` | Motivos novos, `previousEpoch`, `resume`, header `nishidb-reason` |
| `internal/replicationkafka/publisher_resume.go` (novo) | Conferência do topo do tópico e tabela de decisão da subida |
| `internal/replicationkafka/publisher_loop.go`, `publisher.go` | Checkpoint no fluxo, invalidação antes do `reset`, gatilhos, lag em bytes |
| `internal/server/server.go`, `history.go`, `replication_publisher.go`, `replication_http.go` | Abertura em duas fases, encerramento novo, `publishHistory`, rota de snapshot sob demanda |
| `internal/metareplication/` (novo) | `_meta.replication` |
| `internal/cli/` | `nishidb replication resync`, `_meta.replication` no `query` |
| docs | `docs/replication.md`, `docs/metricas.md`, `docs/glossario.md`, `docs/debitos.md`, `docs/diagrams/replication_kafka_flow.puml`, `docs/diagrams/replication_disk_feed.puml` (novo), `docs/diagrams/c4_container.puml`, `CLAUDE.md` |

**Ordem e paralelismo** (corrigida na refutação):
- As Tasks 1→2→3 são sequenciais e mexem nos testes de outros pacotes que chamam `replication.New` (Task 3).
- A Task 5a (`internal/schema`) e as Tasks 7–8 (`internal/replicationkafka`, arquivos novos e o envelope) podem correr em paralelo com 1–2. Nesse caso rodam só `go test` do próprio pacote, porque a Task 3 vai tocar `publisher_harness_test.go`/`publisher_test.go`. Os aceites cruzados esperam o fim da Task 3.
- A Task 4 depende da 2 (`StorePosition`).
- A Task 6 depende de 3, 4 e 5a.
- A Task 9 depende de 3, 7 e 8.
- As Tasks 10–12 dependem de 9.
- A Task 13 fecha a documentação e a 14 é o aceite.

---

### Task 1: Formato do quadro e do segmento

**Arquivos:** criar `internal/replication/diskframe.go`, `internal/replication/diskframe_test.go`

**Interfaces (produz):**
- `func appendFrame(dst []byte, b *Batch, endPos uint64) []byte`
- `func decodeFrame(src []byte) (f frame, n int, err error)`
- `type frame struct{ From, To, EndPos uint64; At time.Time; Reason string; Entries []Entry }`
- `func appendSegmentHeader(dst []byte, h segmentHeader) []byte`
- `func decodeSegmentHeader(src []byte) (segmentHeader, error)`
- `type segmentHeader struct{ Epoch [16]byte; FirstSequence, StartPos uint64 }`
- `var errCorruptFrame`
- `const segmentHeaderSize = 64`, `const frameHeaderSize = 12`

**Formato** (little-endian):
- **Segmento:**
  - magic `"NDBF"`, versão `u16 = 1` e 2 bytes reservados;
  - epoch `[16]byte`, `FirstSequence u64` e `StartPos u64`;
  - zeros até completar 64 bytes.
- **Quadro:**
  - cabeçalho: magic `"NDFR"`, `bodyLen u32` e `crc32c u32` sobre o corpo;
  - corpo: `From u64`, `To u64`, `AtUnixNano i64`, `EndPos u64`, `reasonLen u8` + reason, `count u32` e as `count` entradas;
  - cada entrada: `Sequence u64`, `Operation u8`, `tableLen u16` + table, `AtUnixNano i64`, `RowID u64`, `RecordSequence i64`, `Flags u8`, `keyLen u32` + key e `payloadLen u32` + payload.
- **`EndPos`** é a posição acumulada, em bytes de quadro (cabeçalho incluído) desde o início da geração, do FIM deste quadro.
- **Epoch:** a do Stream é hex de 16 bytes. O cabeçalho guarda os 16 bytes crus.
- **Decodificação:** `decodeFrame` copia key e payload para UMA arena por quadro e nunca aliasa `src`.

- [ ] **Passo 1: escrever os testes, que falham.**
  - `TestFrameGoldenBytes`: um lote com uma Update (com key e payload) e uma Retain (com payload vazio), comparado com um literal hex no teste.
  - `TestSegmentHeaderGoldenBytes`.
  - `TestDecodeFrameRejectsCorruption`. Cada caso da tabela resulta em `errCorruptFrame`:
    - um byte do corpo trocado;
    - truncamento em cada fronteira de campo;
    - magic errado;
    - `bodyLen` além do resto.
  - `TestFrameRoundTripPreservesEntries`: key e payload vazios, tabela histórica, e entrada única maior que 8 MiB.
- [ ] **Passo 2:** `go test ./internal/replication/ -run 'Frame|SegmentHeader' -count=1` → FAIL.
- [ ] **Passo 3:** implementar.
- [ ] **Passo 4:** PASS.
- [ ] **Passo 5: mutações.**
  - Trocar dois campos de ordem → o golden fica vermelho.
  - Ignorar o CRC → `TestDecodeFrameRejectsCorruption` fica vermelho.

### Task 2: Log de segmentos e arquivo de estado

**Arquivos:** criar `internal/replication/disklog.go`, `disklog_test.go`, `feedstate.go`, `feedstate_test.go`

**Interfaces.** Consome a Task 1. Produz:
- **Abertura:**
  - `openDiskLog(dir string, segmentBytes, maxBytes int64) (*diskLog, error)` lê os segmentos e reconstrói o índice. Não cria o diretório.
  - `createDiskLog(dir ...)` cria o diretório com fsync do pai.
- **Escrita:**
  - `(*diskLog).append(frameBytes []byte, from, to, endPos uint64) (removed []segmentInfo, err error)`.
- **Leitura:**
  - `(*diskLog).locate(after uint64) (*segmentHandle, int64, bool)` faz só a busca em memória, sem syscall, e incrementa refs;
  - `(*segmentHandle).readAt(p []byte, off int64) (int, error)` e `(*segmentHandle).release()`.
- **Consultas:**
  - `(*diskLog).floor() uint64`, `bytes() int64` e `segments() int`.
- **Manutenção:**
  - `(*diskLog).sync() error` faz fdatasync do corrente e `atomicfile.SyncDir`;
  - `(*diskLog).wipe() error`;
  - `(*diskLog).validateTail(wantTo, wantEndPos uint64) error`.
- **Tipos:**
  - `type Fingerprint struct{ Stores []StorePosition }`;
  - `type StorePosition struct{ Name string; Segment uint32; Offset int64; NextRowID uint64 }`;
  - `type feedState struct{ Epoch [16]byte; Sequence, Floor, EndPos uint64; Clean bool; Fingerprint Fingerprint }`.
- **Estado em disco:**
  - `func writeFeedState(dir string, st feedState) error` usa `atomicfile`, CRC32C e magic `"NDBT"`;
  - `func readFeedState(dir string) (feedState, bool, error)`: arquivo ausente devolve `ok=false` e nenhum erro; CRC ruim devolve erro.

**Regras:**
- **Nomes de arquivo:** segmento em `<dir>/<FirstSequence %020d>.seg`, estado em `<dir>/state`.
- **Teto do segmento:** passado pelo chamador, `clamp(maxDiskBytes/8, 1 MiB, 64 MiB)`.
- **Rotação:**
  - acontece ANTES do quadro que passaria do teto;
  - um quadro maior que o teto sai sozinho num segmento.
- **Retenção:**
  - enquanto `bytes() > maxBytes` e houver mais de um segmento, o mais antigo é apagado;
  - o piso passa a ser `FirstSequence(próximo) - 1`.
- **Índice esparso:** uma entrada `(To, offset)` a cada 1 MiB de segmento, mais o primeiro quadro de cada segmento.
- **Remoção física:** o `unlink` é imediato, e o `Close` do fd só acontece com `refs == 0`.
- **Log vazio:**
  - um diretório sem segmento é válido, e o piso é o do `state`;
  - `validateTail` com log vazio só passa se `wantEndPos` for o `StartPos` esperado e não houver quadro nenhum.
  - Isso cobre o master ocioso (`Sequence=0`) e o feed depois de um wipe por invalidação (`Floor == Sequence`).
  - O piso reaberto é `max(state.Floor, FirstSequence(primeiro) - 1)`.

- [ ] **Passo 1: escrever os testes.**
  - Rotação e retenção:
    - `TestDiskLogRotatesBeforeExceedingSegmentBytes`;
    - `TestDiskLogRetentionDeletesOldestAndRaisesFloor`.
  - Busca e índice:
    - `TestDiskLogLocateFindsFrameAfterCursor`, nos casos: cursor no meio de um quadro, cursor no limite de segmento e cursor abaixo do piso (`ok=false`);
    - `TestDiskLogSparseIndexStaysBounded`: 200 MiB em quadros pequenos geram no máximo 220 entradas.
  - Leitura concorrente com retenção:
    - `TestDiskReaderSurvivesRetentionUnlink`.
  - Validação da cauda:
    - `TestDiskLogValidateTailRejectsTornFrame`;
    - `TestDiskLogValidateTailAcceptsEmptyLog`, com os dois casos do log vazio.
  - Arquivo de estado:
    - `TestFeedStateGoldenBytes`;
    - `TestFeedStateRejectsBadCRC`.
- [ ] **Passos 2–4:** FAIL, implementar, PASS. Nenhum método segura lock durante I/O.
- [ ] **Passo 5: mutações.**
  - Retenção sem a regra "mais de um segmento" → vermelho.
  - Close do fd sem esperar refs → `TestDiskReaderSurvivesRetentionUnlink` vermelho.
  - Log vazio tratado como erro → `TestDiskLogValidateTailAcceptsEmptyLog` vermelho.

### Task 3: Stream com disco, cache e gravador

**Arquivos:**
- Modificar `internal/replication/stream.go`. O godoc do pacote deixa de dizer "volatile".
- Criar `internal/replication/stream_disk_test.go`.
- Modificar os testes existentes do pacote e os quatro chamadores de teste de `replication.New`, trocando a chamada para `Open` + `Resume`:
  - `storage/snapshot_export_test.go:145`;
  - `server/replication_snapshot_internal_test.go:63`;
  - `replicationkafka/publisher_harness_test.go:204`;
  - `replicationkafka/publisher_test.go:573`.
- `TestAcquireRequiresResyncAfterRetentionGap` (`stream_test.go:69`) é reescrito: com disco, o despejo da memória não cria lacuna. O que ele protegia passa a ser a retenção do DISCO.
- `DefaultOptions` passa a usar campos nomeados.

**Interfaces.** Consome as Tasks 1–2. Produz:

- **`Options`:**
  - ganha `MaxDiskBytes int64` e `PublishHistory bool` (este último só anunciado);
  - `Validate` exige `MaxDiskBytes ≥ 8 MiB`, `MaxDiskBytes ≥ 4 × MaxBatchBytes` e `MaxBufferedBytes ≥ 3 × MaxBatchBytes`;
  - o fator 3 cobre três lotes ao mesmo tempo: o intervalo ativo, um lote na fila do gravador e um arrendado já despejado.
- **Abertura:**
  - `func Open(dir string, o Options) (*Stream, error)` é SÓ LEITURA: lê o `state` e os segmentos e não cria nem grava nada.
- **Retomada:**
  - `func (s *Stream) Resume(fp Fingerprint) (Origin, error)`. É ele quem grava: cria o diretório se preciso e regrava o `state` com `Clean=false`, de forma durável, antes de retornar;
  - `type Origin struct{ Kind OriginKind; PreviousEpoch string; ResumedSequence uint64 }`;
  - `OriginKind` assume `OriginFresh | OriginClean | OriginCrash | OriginRotated`.
- **Fechamento:**
  - `func (s *Stream) Close(fp *Fingerprint, deadline time.Duration) error`:
    - antes do `Resume`, não toca o disco;
    - `fp == nil` fecha sem marcar `clean`;
    - se o prazo da drenagem do gravador vencer, não grava `clean` e devolve erro.
  - `func (s *Stream) Wait()`.
- **Geração:**
  - `func (s *Stream) Rotate(reason string)`: no-op depois do `Close`.
  - `func (s *Stream) Origin() Origin`.
  - `func (s *Stream) Position() (epoch string, sequence, endPos uint64)`.
- **`Batch`:** ganha `EndPos uint64`.
- **`Status`:** ganha `DiskBytes int64`, `DiskSegments int`, `DiskErrors uint64`, `EndPos uint64` e `HistoryExcluded bool` (`json:"historyExcluded,omitempty"`).

**Comportamento.** Cada item tem teste.

**Gerações internas.**
- Cada lote leva `(epoch, gen)`. O `gen` incrementa a cada invalidação e a cada `Rotate`.
- A invalidação e o `Rotate` tiram da fila do gravador os lotes ainda não gravados. A memória deles sai da conta e do heap.
- O gravador descarta lote de `gen` velho, mesmo que ele já tenha saído da fila, e o wipe é feito pelo próprio gravador. Assim, lote velho nunca é gravado depois da invalidação.

**`Publish`.**
- Não faz I/O. O lote fechado entra na fila de gravação.
- O gravador codifica e grava fora de `s.mu`, e depois marca o lote como gravado sob `s.mu`.
- Só lote gravado e sem lease sai da memória.
- Faltando espaço e restando apenas lotes não gravados, chama `invalidateLocked` (lacuna).

**Erro de I/O no gravador (ENOSPC, EIO).**
- O tratamento é o mesmo do disco lento:
  - o lote fica na memória e o gravador retenta com backoff de 100 ms a 5 s;
  - `DiskErrors++` a cada falha;
  - WARN `replication.feed.disk_error` no máximo a cada 5 s.
- A lacuna só vem quando a memória lota.
- Um quadro parcialmente gravado é truncado no offset anterior antes da nova tentativa.
- O Store nunca vê o erro.

**`Acquire`.**
- Lote no cache: lease, como hoje.
- Lote no disco:
  - `locate` sob `s.mu`, sem syscall, e `readAt` fora de `s.mu`;
  - agrupa quadros consecutivos até `MaxBatchBytes`/`MaxMessages`, aceitando sempre pelo menos um quadro;
  - o lote lido do disco é do leitor e não entra no cache.
- Cursor abaixo do piso, ou epoch/gen diferente: `ErrResyncRequired`.

**`Resume(fp)`.**
- Sem `state`: `OriginFresh`.
- Com `state.Clean`, `fp` igual ao gravado e `validateTail` ok: `OriginClean`. A epoch, a `sequence` e o `EndPos` continuam, e `ResumedSequence = state.Sequence`.
- Qualquer outro caso: `OriginCrash`. O `PreviousEpoch` é a epoch do `state`, se legível. O log é apagado, nasce epoch nova e a `sequence` volta a 0.
- Um `Publish` antes do `Resume` força `OriginCrash` e emite ERROR `replication.feed.publish_before_resume`.

**`Close(fp, deadline)` e mutação tardia.**
- Ordem do fechamento:
  1. sob `s.mu`, marca `closed` e fecha o intervalo ativo;
  2. espera o gravador, até o prazo;
  3. `sync`;
  4. grava o `state`.
- As gravações do `state` são serializadas por um `stateMu` próprio.
- Um `Publish` depois de `closed` não registra:
  - sob `s.mu`, liga `late=true`;
  - um `sync.Once` dispara uma goroutine que, sob `stateMu`, grava `Clean=false` e emite ERROR `replication.feed.publish_after_close`.
- O `Close`:
  - só grava `Clean=true` se `!late`;
  - reconfere `late` sob `stateMu` DEPOIS de gravar, e regrava `Clean=false` se ela mudou.
- O `Wait()` junta essa goroutine.
- Os leitores continuam sendo servidos, da memória e do disco, até esgotar. Esgotado, `ErrClosed`.

**`Rotate`.**
- Sob `s.mu`:
  - nasce epoch nova;
  - `gen++`;
  - a `sequence` volta a 0;
  - o `Origin` passa a `{OriginRotated, PreviousEpoch: velha, ResumedSequence: 0}`;
  - os leitores são acordados.
- O wipe vai para o gravador.

**Ganchos de teste** (`export_test.go`):
- `writeHook func([]byte) error` e `readHook func()`;
- `stopWriterWithoutState()`, que simula o processo morto: para o gravador sem tocar no `state`.

- [ ] **Passo 1: escrever os testes.**
  - **Ciclo de vida.**
    - `TestFeedResumesSameEpochAfterCleanClose`: o `Acquire` depois do reopen lê do disco o que foi publicado antes.
    - `TestFeedStartsNewEpochAfterUncleanClose`: um caso com `Close(nil)`, outro com `stopWriterWithoutState`; `PreviousEpoch` certa.
    - `TestFeedTreatsFingerprintMismatchAsCrash`.
    - `TestFeedTreatsTornTailAsCrashEvenWhenClean`.
    - `TestIdleFeedResumesCleanWithEmptyLog`: master ocioso, sem nenhum segmento.
    - `TestOpenIsReadOnlyUntilResume`: depois de `Open` e de `Close` antes do `Resume`, o diretório e o `state` ficam byte a byte iguais.
  - **Mutação tardia.**
    - `TestLatePublishAfterCloseForcesCrashOnReopen`.
    - `TestLatePublishDuringCloseNeverEndsClean`: gancho entre a drenagem e a gravação do `state`, `Publish` nesse intervalo, reopen `OriginCrash`.
    - `TestPublishBeforeResumeForcesCrash`.
  - **Memória e disco.**
    - `TestPublishNeverBlocksWhenTheWriterStalls`: com o gravador preso pelo `writeHook`, uma rajada de `Publish` termina com prazo folgado (10 s, inclusive sob `-race`). A asserção é que a memória do feed (`BufferedBytes` mais o tamanho da fila) fica ≤ `MaxBufferedBytes`, e que houve invalidação.
    - `TestRepeatedInvalidationsReleaseQueuedBatches`: com o gravador preso, cinco invalidações seguidas. `runtime.ReadMemStats` mostra o `HeapInuse` estável, dentro de 2× `MaxBufferedBytes`.
    - `TestInvalidatedBatchIsNeverWrittenAfterInvalidation`: um lote em gravação no momento da invalidação não aparece no disco depois dela.
    - `TestDiskWriteErrorRetriesAndRecovers`: `ENOSPC` três vezes e depois sucesso. `DiskErrors=3`, nenhuma invalidação, conteúdo íntegro.
  - **Leitura.**
    - `TestAcquireMergesConsecutiveDiskFrames`: com `flushInterval 0`, 1.000 entradas.
    - `TestDiskReadDoesNotHoldPublishLock`: `readHook` bloqueia e um `Publish` concorrente termina.
    - `TestEvictingPersistedBatchDoesNotCreateGap`.
    - `TestDiskReaderBatchIsBoundedPerReader`: assevera o tamanho do lote do leitor (≤ `MaxBatchBytes`, ou um quadro só quando este for maior) e, com `MaxBufferedBytes` de 1 MiB e disco de 64 MiB cheio, `BufferedBytes ≤ 1 MiB` durante a leitura completa.
  - **Rotação e prazo.**
    - `TestRotateForcesResyncAndNewEpoch`.
    - `TestCloseDeadlineSkipsCleanMark`.
  - **Pisos.**
    - `TestMinimumBufferedBytesNeverInvalidatesWithHealthyDisk`: com `MaxBufferedBytes = 3 × MaxBatchBytes` e uma carga sustentada, `Invalidations == 0`.
- [ ] **Passos 2–4:** `go test ./internal/replication/ -count=1`, também com `-race`.
- [ ] **Passo 5: mutações.**
  - Gravar `Clean=true` sem conferir `fp` → `TestFeedTreatsFingerprintMismatchAsCrash` vermelho.
  - `readAt` sob `s.mu` → `TestDiskReadDoesNotHoldPublishLock` vermelho.
  - Despejar lote não gravado → teste de conteúdo vermelho.
  - Invalidação sem esvaziar a fila → `TestRepeatedInvalidationsReleaseQueuedBatches` vermelho.
  - `Close` sem a reconferência de `late` → `TestLatePublishDuringCloseNeverEndsClean` vermelho.

**Commit do bloco (Tasks 1–3):** "Persiste o feed de replicação em disco com retenção por bytes".

### Task 4: Posição de escrita do Store

**Arquivos:** modificar `internal/storage/store.go`; criar `internal/storage/write_position_test.go`.

**Interfaces.** Consome `replication.StorePosition` (Task 2). Produz:
- `func (s *Store) WritePosition() (replication.StorePosition, error)`:
  - `Name` é o nome do Store;
  - `Segment` e `Offset` vêm de `secW.Current()`;
  - `NextRowID` vem de `s.nextRowID`;
  - lê sob `entra()` + `wmu`;
  - com o Store fechado, devolve `ErrDatasetClosed`.

- [ ] **Passo 1: escrever os testes.**
  - `TestWritePositionIsStableAcrossCleanReopen`.
  - `TestWritePositionAdvancesOnPut`.
  - `TestWritePositionChangesAfterOfflineMutation`.
  - `TestWritePositionUnchangedByOpenCheckpoint`.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** `NextRowID` cacheado no `Open` → `TestWritePositionAdvancesOnPut` vermelho.

### Task 5a: Chaves de configuração

**Arquivos:** modificar `internal/schema/spec.go` (`ReplicationSpec`, `rawReplication` e o parse em ~1216-1347); criar `internal/schema/replication_disk_test.go`.

**Interfaces (produz):**
- `ReplicationSpec.MaxDiskBytes int64`, com padrão `2 << 30`.
- `ReplicationSpec.PublishHistory bool`, com padrão `true`. No struct cru é `*bool`.
- `ReplicationSpec.PeriodicSnapshot time.Duration`, com padrão 0 (desligado).

**Regras:**
- **Onde valem:** as três chaves só valem em master habilitado. Com `role: follower` ou com `enabled: false`, cada uma é recusada nomeando a chave, pelo caminho que já recusa `targets`. O `role` omitido é master, como hoje (`spec.go:1250`).
- **`maxDiskBytes`:** passa por `units.Size`, e precisa ser ≥ 8 MiB e ≥ 4 × `maxBatchBytes`.
- **`maxBufferedBytes`:** o piso passa a ser 3 × `maxBatchBytes` em TODO master, com ou sem `targets`. O piso de 2× de `validateReplicationTargets` sai, e `TestReplicationTargetsRequireBufferOfTwiceTheBatch` é reescrito para 3×.
- **`periodicSnapshot`:** só vale com `targets`, em segundos inteiros, de 1 h a 365 d. Fora disso é recusado, nunca arredondado.
- **`publishHistory`** declarado num dataset sem `history:` gera WARN nomeado na carga.
- **Literais:** os testes asseveram `2147483648` como padrão de `maxDiskBytes` e `3 *` como piso.

- [ ] **Passo 1: escrever os testes.**
  - `TestReplicationMaxDiskBytesDefaultsToTwoGiB`.
  - `TestReplicationRejectsDiskKeysOutsideEnabledMaster`, em tabela com três chaves × (follower, `enabled: false`).
  - `TestReplicationRejectsMaxDiskBytesBelowFloor`.
  - `TestReplicationBufferFloorIsThreeBatches`.
  - `TestReplicationPublishHistoryDistinguishesAbsentFromFalse`.
  - `TestReplicationWarnsPublishHistoryWithoutHistory`.
  - `TestReplicationPeriodicSnapshotRequiresTargetsAndRange`.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** Tirar a recusa no follower → vermelho.

### Task 6: Fiação no servidor e no follower

**Arquivos:**
- Modificar em `internal/server`:
  - `server.go`: `openDataset` (~525-748), `encerra` (~1289) e a subida abortada (~446);
  - `history.go:87-107`;
  - `replication_publisher.go`: `closeFeeds` e `openReplicationPublishers` (~210-218);
  - `replication_http.go`.
- Modificar em `internal/replication`: `client.go` e `resync.go`.
- Modificar o gate `internal/gates/alarm_shutdown_test.go`.
- Testes novos em `internal/server/replication_disk_feed_test.go` e `internal/replication/client_resume_test.go`.

**Interfaces.**
- Consome:
  - `replication.Open`, `Resume`, `Close` e `Wait` (Task 3);
  - `Store.WritePosition` (Task 4);
  - as chaves da Task 5a.
- Produz:
  - `func (s *Server) feedFingerprint(d *dataset) (replication.Fingerprint, error)`, que inclui o base e, só com `PublishHistory`, a histórica;
  - `func (s *Server) stopStoreProducers()`;
  - `func (s *Server) checkpointStores() error`;
  - `replication.Client.RequireHistory bool`;
  - `replication.ErrHistoryNotPublished`;
  - o estado de réplica `ReplicaHistoryUnavailable` (grafia `history_unavailable`).

**Abertura.**
1. `storage.Open` do base, que toma o LOCK. O `OnReplicationChange` fecha sobre o campo `d.replication`, preenchido no passo 2. Nada publica durante o Open, como a refutação conferiu.
2. `replication.Open(<DatasetRootDir>/replication/feed, opts)`, só leitura.
3. A histórica, ligada ao feed só com `PublishHistory`.
4. `feed.Resume(fp)`.

Subida abortada: antes do `Resume`, `Close(nil, 0)`, que não toca o disco; depois do `Resume`, `Close(&fp, budget)`, porque nada mutou o Store.

**Encerramento**, nesta ordem. O comentário de `encerra` é reescrito.
1. **Drenagem dos handlers da replicação.** O `/v1/replication/batch` e o `/v1/replication/snapshot` usam `r.Context()` composto com o `vidaCtx` (o padrão do `ctxOfMutacao`). O cancelamento do encerramento responde **410**, o código que `TestShutdownReleasesFollowerLongPollBeforeHTTPShutdown` já exige, e nunca 409.
2. `http.Shutdown`, mais o loopback e o pprof.
3. `stopStoreProducers()`: `ForCompaction`, `ForExpiration` e `ForRetention` em todos os Stores. O comentário registra que `ForRebuild`, `ForHistoryTimeBounds` e `ForRetirementOfGroups` não publicam nem movem `secW`.
4. `checkpointStores()`: o `Checkpoint` do par base+histórica de todo dataset, com ou sem ingestão Kafka. Erro aqui faz o passo 5 ser `Close(nil, …)`.
5. `closeFeeds()`, que passa a chamar `feed.Close(&fp, 10*time.Second)`.
6. `closePublishers()`: drenagem de 10 s, com gravação do `.ckpt`.
7. Alarmes, emissores, séries e contadores, como hoje.
8. `closeDatasets()`.
9. `feed.Wait()`.

O gate passa a asseverar `Shutdown < stopStoreProducers < checkpointStores < closeFeeds < closePublishers < closeDatasets`.

**`publishHistory: false`.**
- `handleReplicationSnapshot` e `openReplicationPublishers` deixam a irmã fora das `tables`.
- O `Status` sai com `HistoryExcluded=true`.

**Follower.**
- `Replica.Disconnect` lembra se o estado anterior era `catching_up` ou `following`.
- `SyncOnce`, num estado `disconnected` que veio de um desses dois e com `epoch == remote.Epoch`, tenta o tail antes do snapshot. Um 409 cai no caminho de resync de hoje; um 410 é desconexão.
- Com `RequireHistory` e `remote.HistoryExcluded`, `SyncOnce` devolve `ErrHistoryNotPublished`, o estado vira `history_unavailable`, sai ERROR e nada é aplicado.
- O `RequireHistory` é ligado em `server.go:~482` quando `d.irma != nil`.

- [ ] **Passo 1: escrever os testes.** Servidor real em `t.TempDir()`, com `kfake` quando preciso.
  - `TestServerRestartResumesFeedEpoch`.
  - `TestShutdownStopsProducersBeforeClosingTheFeed`: um gancho segura uma passada de expiração em voo durante o encerramento. O reopen tem de ser `OriginClean` E a lápide tem de estar no feed. O teste exige as DUAS coisas.
  - `TestShutdownCheckpointsDatasetWithoutKafkaIngestion`.
  - `TestOfflineIngestBetweenRunsForcesNewEpoch`.
  - `TestOfflineHistoryOnlyMutationForcesNewEpoch`: `Apenda` direto no Store irmão, com o servidor parado.
  - `TestPublishHistoryFalseKeepsHistoryOutOfFeed`: cobre o tail, o `retain` e o snapshot HTTP.
  - `TestFollowerRequiringHistoryRefusesMasterWithoutIt`.
  - `TestFollowerResumesTailAcrossMasterCleanRestart`: o follower fica no ar enquanto o master reinicia; nenhum snapshot novo no follower e `applied` contínuo.
  - `TestShutdownReleasesFollowerLongPollBeforeHTTPShutdown`, existente: continua verde e com 410.
  - `TestAbortedStartupAfterResumeKeepsFeedClean`.
  - O gate atualizado, vermelho com a ordem antiga.
- [ ] **Passos 2–4:** `go test ./internal/server/ ./internal/gates/ ./internal/replication/ -count=1`.
- [ ] **Passo 5: mutações.**
  - `closeFeeds` antes de `stopStoreProducers` → `TestShutdownStopsProducersBeforeClosingTheFeed` vermelho (o reopen sai `Crash`).
  - Fingerprint sem a histórica → `TestOfflineHistoryOnlyMutationForcesNewEpoch` vermelho.
  - Follower sem a retomada → `TestFollowerResumesTailAcrossMasterCleanRestart` vermelho.

**Commit do bloco (Tasks 4, 5a e 6):** "Retoma a geração do feed no reinício limpo e reordena o encerramento". Leva junto `docs/replication.md` (feed em disco, `publishHistory`, encerramento, follower) e o glossário.

### Task 7: Checkpoint do publicador

**Arquivos:** criar `internal/replicationkafka/checkpoint.go` e `checkpoint_test.go`.

**Interfaces (produz):**
- `type checkpointFile struct{ Epoch, TargetEpoch string; Cursor, CursorPos uint64; Partitions int32; Topic string; Bootstrap []string; RenderHash [32]byte; Snapshotting bool; ResyncConsumed uint64; LastSnapshotEnd time.Time }`:
  - `Epoch`/`Cursor` são o ponto confirmado;
  - `TargetEpoch` é a epoch do `reset` em curso quando `Snapshotting=true`.
- `func loadCheckpoint(path string) (checkpointFile, bool, error)`.
- `func storeCheckpoint(path string, c checkpointFile) error`: grava via `atomicfile`, com CRC32C, magic `"NDBK"`, versão 1.
- **Marcador de pedido de snapshot:**
  - `func requestResync(path string) (uint64, error)`: incrementa um contador persistido em `<destino>.resync` (atomicfile, CRC) e devolve o novo valor;
  - `func pendingResync(path string, consumed uint64) (uint64, bool, error)`: o pedido está pendente quando o contador gravado é maior que `consumed`;
  - o marcador nunca é apagado.
- **Caminhos:** `<DatasetRootDir>/replication/kafka/<destino>.ckpt` e `<destino>.resync`.

- [ ] **Passo 1: escrever os testes.**
  - `TestCheckpointGoldenBytes`.
  - `TestCheckpointRejectsBadCRC`.
  - `TestCheckpointMissingIsNotAnError`.
  - `TestResyncCounterGoldenBytes`.
  - `TestResyncRequestDuringSnapshotIsNotLost`: o pedido 1 é consumido no início do snapshot, o pedido 2 chega durante ele, e ao final o pedido 2 continua pendente.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** Ignorar o CRC → vermelho.

### Task 8: Contrato do envelope

**Arquivos:** modificar `internal/replicationkafka/envelope.go` (godoc do contrato, `ResetReason`, `Op`, `AppendReset`) e `publisher_batch.go` (headers), mais `envelope_test.go` e `envelope_alloc_gate_test.go`.

**Interfaces (produz):**
- **Motivos novos.** `ResetReason` ganha seis motivos, nesta ordem e depois dos quatro atuais:

  | Constante | Grafia JSON |
  |---|---|
  | `ResetCrash` | `crash` |
  | `ResetRewind` | `rewind` |
  | `ResetRetention` | `retention` |
  | `ResetConfigChanged` | `config_changed` |
  | `ResetRequested` | `requested` |
  | `ResetPeriodic` | `periodic` |

- **`OpResume`**, com grafia `resume`, depois de `OpSnapshotEnd`.
- **`AppendReset`:** `func (r *Renderer) AppendReset(dst []byte, epoch, previousEpoch string, reason ResetReason, cursor uint64, at time.Time) ([]byte, error)`. A chave `previousEpoch` vai depois de `reason`; vazia, é omitida.
- **`AppendResume`:** `func (r *Renderer) AppendResume(dst []byte, epoch string, cursor uint64, at time.Time) []byte`.
- **Header do motivo:**
  - `const HeaderReason = "nishidb-reason"`;
  - o `reset` sai com `nishidb-op` e `nishidb-reason`, por uma tabela pré-montada por motivo, como o `opHeaders`, sem alocar por registro;
  - o `checkSizes` (`publisher_loop.go:439`) passa a somar os bytes do header de motivo.

Formas exatas (literais nos testes):

```
{"epoch":"<hex>","dataset":"<base>","op":"reset","reason":"crash","previousEpoch":"<hex>","cursor":0,"at":"<instante>"}
{"epoch":"<hex>","dataset":"<base>","op":"resume","cursor":N,"at":"<instante>"}
```

- [ ] **Passo 1: escrever os testes.**
  - `TestResetReasonsSpellTheContract` com os dez literais; os motivos inválidos passam a ser 11 e 255.
  - `TestResetOmitsEmptyPreviousEpoch`.
  - `TestResumeMarkerShape`.
  - `TestResetCarriesReasonHeader`.
  - `TestCheckSizesCountsReasonHeader`.
  - O gate de alocação cobre `AppendResume` e o header de motivo.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** Inverter dois motivos na grafia → vermelho.

**Commit do bloco (Tasks 7–8):** só código e testes, sem mudança de comportamento do publicador.

### Task 9: Publicador — conferência de rewind, decisão na subida, checkpoint, gatilhos

**Arquivos:**
- Criar `internal/replicationkafka/publisher_resume.go` e `publisher_resume_test.go`.
- Modificar `publisher_loop.go`: `run`, `snapshot`, `tailOnce` e `logLivelock`.
- Modificar `publisher.go`:
  - `Options` ganha `CheckpointDir string`, `RenderHash [32]byte` e `PeriodicSnapshot time.Duration`;
  - `Stats` ganha `CheckpointCursor uint64`, `LagBytes uint64`, `LastResetReason string` e `LastSnapshotAt float64`;
  - as listas fechadas ganham os motivos novos e os erros `ErrorCheckpointWrite = "checkpoint_write"` e `ErrorTopicRead = "topic_read"`.
- Modificar `internal/server/replication_publisher.go`: passa o diretório, o hash e o período.
- Atualizar os literais de teste:
  - `publisher_test.go:763-764`;
  - `TestLivelockAfterSnapshotIsLogged`, que passa a asseverar `maxDiskBytes`.

**Interfaces.**
- Consome as Tasks 3, 7 e 8.
- Produz:
  - `func (p *Publisher) RequestSnapshot() error`: chama `requestResync` e acorda o laço;
  - `func RenderHash(spec RenderSpec) [32]byte`;
  - `type RenderSpec struct{ Tables []string; Aliases []FieldAlias; PublishHistory bool }`, onde `Aliases` é a lista ordenada de `(table, field, alias)` declarada na spec;
  - o servidor monta o `RenderSpec` a partir das specs. O hash NÃO cobre colunas descobertas em runtime: o FieldID→nome é imutável, e descoberta de coluna não muda o rótulo de coluna existente.

**Conferência de rewind.**
- **Quando roda:** primeiro passo da subida, sempre que `Origin.Kind` é `OriginClean`. Com `Fresh`, `Crash` ou `Rotated` a epoch é nova e aleatória e não há colisão possível.
- **Sem Kafka:**
  - com `ckpt.Epoch == feed epoch` e `ckpt.Cursor > Origin.ResumedSequence`: `rewind`;
  - com `ckpt.Snapshotting` e `ckpt.TargetEpoch == feed epoch` e o `.ckpt` mais novo que o `state`: `rewind`. Para isso, o `.ckpt` grava a sequence do feed no momento.
- **Com Kafka:**
  - `kadm.ListStartOffsets` e `ListEndOffsets`;
  - para cada partição com `end > start`, um cliente consumidor `kgo` lê o último registro de dado ou controle;
  - se a leitura não devolve registro nenhum (lote de controle de transação, retenção), a partição fica DESCONHECIDA e é tratada como falha de leitura;
  - de cada registro, um parser mínimo extrai a `epoch` e a posição (`sequence` no dado, `cursor` no controle). O parser usa as chaves fixas do envelope e não usa `encoding/json` no caminho quente; aqui não é caminho quente, então `encoding/json` é aceitável.
- **É `rewind` quando:**
  - a epoch do registro ∉ {`ckpt.Epoch`, `ckpt.TargetEpoch`}. A config garante tópico exclusivo por dataset (`replication_publisher.go:110-147`), então epoch estranha é publicação nossa posterior;
  - OU a epoch é a do feed e a posição é > `Origin.ResumedSequence`.
- **Ação no `rewind`:**
  1. `feed.Rotate("rewind")`;
  2. `reset` com `rewind`;
  3. snapshot na epoch nova.
- **Falha de leitura:** `ErrorTopicRead`, estado `retrying`, backoff, e de novo. Nunca retoma nem faz `reset` na epoch antiga às cegas.

**Tabela de decisão.** Avaliada DEPOIS da conferência de rewind; a primeira linha que casa vence.

| # | Condição | Motivo |
|---|---|---|
| 1 | sem `.ckpt`; ou `Topic` diferente; ou interseção vazia entre `Bootstrap` e os servidores atuais, pela mesma regra do `sameCluster` (`replication_publisher.go:156`) | `startup` |
| 2 | `RenderHash` diferente | `config_changed` |
| 3 | `Snapshotting=true` | `snapshot_failed` |
| 4 | `pendingResync` | `requested` |
| 5 | `ckpt.Epoch ≠` epoch do feed: com `Crash` e `PreviousEpoch == ckpt.Epoch`, `crash`; com `Rotated`, `rewind`; senão, `startup` | conforme a linha |
| 6 | `Cursor <` piso do feed | `retention` |
| 7 | partições do tópico `≠ Partitions` | `partitions_changed` |
| 8 | `PeriodicSnapshot > 0` e `now − LastSnapshotEnd ≥ PeriodicSnapshot` | `periodic` |
| 9 | nenhuma | retoma: `resume` por partição, enviado IMEDIATAMENTE e sem barreira de ack; depois, o tail a partir de `Cursor` |

**Regras do checkpoint.**
- **`previousEpoch` do `reset`:** é `ckpt.Epoch` lido ANTES da gravação de `Snapshotting=true`, quando difere da epoch do `reset`. Senão, sai vazio.
- **Antes do `reset`:** `storeCheckpoint` com `Snapshotting=true`, `TargetEpoch` igual à epoch do `reset` e `ResyncConsumed` igual ao contador lido (se o motivo for `requested`, ou se houver pedido pendente). O `Epoch`/`Cursor` ficam como estavam. Se essa gravação falhar, o snapshot não começa e o estado é `retrying`.
- **Depois do `snapshot_end` confirmado:** `Snapshotting=false`, `Epoch` e `Cursor` do reset, e `LastSnapshotEnd=now`.
- **No tail:** checkpoint depois do lote confirmado, no máximo uma vez por segundo. Também na saída por `ErrClosed`, por cancelamento em `following` e por `blocked`/`failed`, sempre com o último ponto confirmado. Erro → `ErrorCheckpointWrite`, e o publicador segue.

**Em operação.**
- **`ErrResyncRequired`:** com a epoch do feed mudada, reavalia a linha 5; com a mesma epoch, é `gap`.
- **Pedidos e prazos:** `pendingResync` e o vencimento do periódico são conferidos na cadência da conferência de partições.
- **`LagBytes`:** `feed.EndPos − CursorPos` quando a epoch do feed é a do cursor e `EndPos ≥ CursorPos`; senão, 0.
- **`CursorPos`:** é o `Batch.EndPos` do último lote confirmado, ou o `endPos` do `Position()` lido junto com o cursor do snapshot.
- **ERROR de livelock:** passa a recomendar `replication.maxDiskBytes`.

- [ ] **Passo 1: escrever os testes.** Com `kfake` e feed real em `t.TempDir()`.
  - **Tabela de decisão:**
    - `TestStartupDecisionTable`: uma linha por regra 1–9, que monta o `.ckpt`, o feed e o tópico e assevera o motivo LITERAL do `reset`, ou a ausência de `reset` e a presença do `resume`.
  - **Retomada:**
    - `TestResumeContinuesSequencesWithoutReset`.
    - `TestResumeMarkerIsSentWhileTailIdle`.
    - `TestRestartDuringSnapshotNeverResumesTail`.
  - **Rewind:**
    - `TestResumeDetectsRewoundFeedFromTopicTail`: copia o diretório do dataset (backup), publica mais, restaura a cópia e reabre. Esperado: `rewind`, epoch nova e `previousEpoch` preenchido.
    - `TestRewindDetectedEvenWhenFeedAdvancesBeforeTopicCheck`: na subida, a leitura do tópico falha (gancho no cliente) enquanto o feed recebe `Publish` além do topo antigo. Quando a leitura volta, o resultado tem de ser `rewind`, e não `resume`.
    - `TestRewindWhenTopicTailHasForeignEpoch`.
    - `TestRewindPrecedesConfigChangedReset`: backup restaurado com alias mudado → `rewind`, e o snapshot sai na epoch NOVA.
    - `TestTopicReadFailureNeverResumesBlindly`.
    - `TestEmptiedPartitionIsUnknownNotClean`: `start == end > 0`.
  - **Pedidos, prazos e checkpoint:**
    - `TestRequestedSnapshotSurvivesRestart`.
    - `TestPeriodicSnapshotUsesPersistedLastSnapshotEnd`.
    - `TestCheckpointWriteFailureIsCountedAndPublisherContinues`.
    - `TestResetCarriesPreviousEpochOnlyWhenEpochChanges`.
    - `TestBootstrapServerAddedDoesNotForceStartup`.
    - `TestRenderHashIgnoresRuntimeDiscoveredColumns`.
  - **Lag:**
    - `TestLagBytesTracksFeedPositionAndClampsAfterRotate`.
  - **Literais:** `wantSnapshots` com os dez motivos e `wantErrors` com os oito.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutações.** Cada mutação abaixo tem de deixar o teste indicado vermelho.

  | Mutação | Teste que fica vermelho |
  |---|---|
  | Pular o `Snapshotting=true` | `TestRestartDuringSnapshotNeverResumesTail` |
  | Comparar com o `Produced` corrente | `TestRewindDetectedEvenWhenFeedAdvancesBeforeTopicCheck` |
  | Conferência de rewind depois da tabela | `TestRewindPrecedesConfigChangedReset` |
  | Retomar em falha de leitura | `TestTopicReadFailureNeverResumesBlindly` |
  | Hash com as colunas descobertas | `TestRenderHashIgnoresRuntimeDiscoveredColumns` |

**Commit do bloco:** "Retoma o publicador Kafka do checkpoint e acrescenta os motivos de reset". No mesmo commit:
- `docs/replication.md`, nas seções:
  - ciclo de vida e regras do consumidor (`resume` não exige ação; `previousEpoch`);
  - motivos;
  - dimensionamento por disco;
  - "Mudanças de contrato desde a v0.14";
  - limite: master só com followers HTTP não detecta rewind.
- o godoc do envelope;
- o glossário;
- `docs/diagrams/replication_kafka_flow.puml`, validado pelo servidor `uml.nishisan.dev`;
- `docs/debitos.md`:
  - o 132 é atualizado: o remédio passa a ser `maxDiskBytes`;
  - três débitos novos entram no fim: retomar depois de queda; follower que reinicia não persiste o cursor; rewind invisível para follower HTTP.

### Task 10: Snapshot sob demanda (rota e CLI)

**Arquivos:** modificar `internal/server/replication_http.go` e `internal/cli/`; criar `internal/server/replication_resync_test.go` e `internal/cli/replication_resync_test.go`.

**Interfaces.**
- Consome: `Publisher.RequestSnapshot` (Task 9).
- Produz:
  - a rota `POST /v1/replication/resync?dataset=<d>&destination=<t>`, registrada por `rt.register` com ADMIN:
    - em sucesso, responde `202` com `{"dataset":..,"destination":..,"requested":true}`;
    - dataset ou destino desconhecido usa o código de wire já existente para recurso inexistente, que o Builder confere em `internal/protocol`. Não entra código novo;
  - o comando `nishidb replication resync --server <url> --dataset <d> --destination <t>`.

- [ ] **Passo 1: escrever os testes.**
  - `TestResyncRouteRequiresAdmin`.
  - `TestResyncRouteTriggersRequestedReset`.
  - `TestResyncRouteRejectsUnknownDestination`, asseverando a mensagem própria.
  - `TestCLIReplicationResyncCallsRoute`.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** Sem ADMIN → vermelho.

### Task 11: `_meta.replication`

**Arquivos:**
- Criar `internal/metareplication/`: `registry.go`, `table.go`, `table_test.go`, `harness_test.go` e `export_test.go`, no padrão de `internal/metakafka`.
- Modificar `internal/server/server.go` (~704) e `internal/cli/query.go:209`.
- Atualizar as listas de catálogo:
  - `internal/query/catalog_test.go:25,80`;
  - `internal/query/metatables_test.go:35,51`;
  - `internal/server/metatables_test.go:54`;
  - `internal/cli/metatables_query_test.go:57`.

**Interfaces (produz):**
- `metareplication.Register(eng, dataset string, src Source)`.
- `type Source struct{ Feed func() (replication.Status, bool); Destinations func() []replicationkafka.Stats; Checkpoints func() []CheckpointRow }`. As closures são opcionais.
- No CLI offline, `Feed` lê o `state`, `Destinations` é nil e `Checkpoints` lê os `.ckpt` (`replicationkafka.LoadCheckpointRow`, exportado na Task 7 se necessário).

**Colunas:**
- **Chave:** `id`, string com o nome do destino.
- **Destino:** `topic`, `state`, `epoch`, `cursor`, `checkpointCursor`.
- **Feed:** `produced`, `floor`, `feedDiskBytes`, `feedCacheBytes`, `feedSegments`.
- **Atraso:** `lagSequences`, `lagBytes`.
- **Histórico:** `lastResetReason`, `lastSnapshotAt` (timestamp), `lastSuccessAt` (timestamp), `lastError`.
- As sequências saem como int64.

- [ ] **Passo 1: escrever os testes.**
  - `TestMetaReplicationListsOneRowPerDestination`.
  - `TestMetaReplicationReadsCheckpointsOffline`.
  - `TestMetaReplicationNamesColumnsLiterally`.
  - As listas de catálogo atualizadas.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** `lagBytes` lendo `LagSequences` → vermelho.

### Task 12: Métricas e `/v1/stats`

**Arquivos:**
- `internal/metrics/names.go` (~802-850);
- `internal/server/metrics_replication.go`;
- `internal/protocol/documents.go:339-387`;
- `docs/metricas.md:318-349`;
- `internal/server/replication_publisher_internal_test.go`.

**Séries novas:**

| Série | Tipo |
|---|---|
| `nishidb_replication_feed_disk_bytes` | gauge |
| `nishidb_replication_feed_disk_segments` | gauge |
| `nishidb_replication_feed_disk_errors_total` | counter |
| `nishidb_replication_kafka_lag_bytes` | gauge |
| `nishidb_replication_kafka_checkpoint_sequence` | gauge |

Todas nascem em zero. `/v1/stats` e `/metrics` leem a mesma `protocol.ReplicationStats`.

- [ ] **Passo 1: escrever os testes.**
  - `TestMetricsDocListsEveryReplicationReasonAndState` exige os motivos e erros novos no doc.
  - O teste do coletor confere as séries novas em zero na subida.
  - Os gates de `metrics_test.go` e `TestAmostrasDeMetricaTemFamiliaENome` ficam verdes.
- [ ] **Passos 2–4.**
- [ ] **Passo 5: mutação.** Série sem `Family` → `TestAmostrasDeMetricaTemFamiliaENome` vermelho.

**Commit do bloco (Tasks 10–12):** "Expõe o checkpoint do publicador em _meta.replication, métricas e snapshot sob demanda". Leva junto:
- o README, nos pontos de `_meta`;
- `docs/replication.md` e `docs/metricas.md`;
- o glossário;
- `CLAUDE.md`: pacote `internal/metareplication` e `rt.register`.

### Task 13: Documentação, diagrama e revisão do branch

**Arquivos:**
- Criar `docs/diagrams/replication_disk_feed.puml`. O diagrama cobre:
  - escrita, gravador, gerações, retenção e cache;
  - encerramento limpo e queda;
  - a conferência de rewind.
- Modificar `docs/replication.md`:
  - sai "A fila é volátil" e "log durável… fora desta versão";
  - entra a seção do feed em disco, com a conta de dimensionamento `maxDiskBytes / (taxa × custo por entrada)` e os números do CTP.
- Modificar `docs/diagrams/c4_container.puml:18-23,53-56`.
- Modificar `docs/architecture.md`, só com número medido.

- [ ] Rodar `TestEvidenciasCitadasNosDocumentosExistem`, `TestCitacoesDeSecaoResolvem`, os gates de glossário e citação, e validar os `.puml` pelo servidor.
- [ ] Revisão do branch inteiro por um Refuter Opus:
  - diff completo;
  - `make test` e `make test-race`;
  - lint ratchet via `git archive`;
  - contagem de testes no relatório.

### Task 14: Aceite no piloto (.228)

Quem conduz é o usuário, ou o orquestrador com autorização. Release e tag ficam com o usuário.

**Config:** `events` com:
- `replication.enabled: true` e `role: master`;
- `targets: [<destino TEMS>]`;
- `publishHistory: false`;
- `maxDiskBytes: 8GiB`.

**Roteiro:**
- [ ] A primeira subida faz `reset` com `startup`, e o snapshot termina sem `snapshot_gap`.
- [ ] Um `systemctl restart` limpo gera `resume` e nenhum `reset`, com as `sequence`s contínuas.
- [ ] Um `kill -9` gera `reset` com `crash` e com `previousEpoch` preenchido.
- [ ] Com o broker inacessível por 10 min, o tail retoma do disco sem `reset`, e `lagBytes` cai a zero.
- [ ] `nishidb replication resync` gera `reset` com `requested`.
- [ ] O RSS fica estável com o feed em disco cheio.
- [ ] Mandar à sessão do TEMS o resumo do contrato final e a versão.

---

## Correções da refutação (16/10/2026)

O revisor Opus derrubou 6 pontos bloqueantes e 15 a corrigir. Todos foram incorporados acima. Os que divergem do desenho também foram registrados lá.

1. **A conferência de rewind usa `Origin.ResumedSequence`, e não o `Produced` corrente.**
   - Ela trata a epoch estranha no topo do tópico também como rewind.
   - Ela roda ANTES da tabela.
   - Há regras sem Kafka para o `.ckpt` à frente do feed.
2. **O `RenderHash` cobre só aliases, tabelas e `publishHistory`.** Hashear o `Renderer` incluiria as colunas descobertas em runtime e dispararia `config_changed` em todo reinício.
3. **As gravações do `state` são serializadas e a flag `late` é reconferida.** Sem isso, um `Publish` tardio poderia ser coberto por `clean=true`.
4. **Os lotes levam `gen`.** A invalidação e o `Rotate` esvaziam a fila do gravador, e lote velho nunca é gravado depois deles.
5. **O feed abre DEPOIS do LOCK do Store, e o `Open` é só leitura.** A subida abortada depois do `Resume` fecha limpa.
6. **Piso de `maxBufferedBytes` = 3 × `maxBatchBytes` em todo master.** Isso diverge do desenho, que dizia 2×.
7. **O `Close` tem prazo** e o `validateTail` aceita log vazio.
8. **O pedido de snapshot virou contador** (`.resync`), com consumo registrado no `.ckpt`. Um pedido feito durante o snapshot não se perde. Isso diverge do desenho, que dizia "no `.ckpt`".
9. **`previousEpoch` só aparece quando a epoch muda**, e a leitura acontece antes da gravação.
10. **Testes vacuamente verdes foram endurecidos:**
    - o encerramento exige `Clean` E a lápide no feed;
    - há mutação só na histórica;
    - o heap é medido;
    - foi especificado o gancho de processo morto.
11. **Dependências corrigidas:** a Task 4 vem depois da 2, e os testes de outros pacotes mexidos na Task 3 ficam serializados.
12. **A premissa "sem release" era falsa:** a `v0.14.1` está publicada.
    - Toda mudança no wire passa a ser aditiva, com `historyExcluded` (ausente = legado).
    - Há uma seção de mudanças de contrato.
13. **O §6.7 do desenho estava sem implementação.** Entraram:
    - a retomada do follower depois de desconexão;
    - o 410 no encerramento;
    - o contexto do `Run` no snapshot HTTP.
14. **Leitura do tópico:**
    - `ListStartOffsets`;
    - partição esvaziada é desconhecida.
15. **Ajustes do publicador:**
    - `LagBytes` com guarda;
    - o cluster comparado por interseção;
    - o `resume` enviado na hora.
16. **ENOSPC e EIO passam a ser tratados como disco lento:** retenção em memória e retentativa, com lacuna só quando a memória lota. Isso diverge do desenho, que mandava invalidar.
17. **Correções de config e de nomes:**
    - `rt.register`;
    - chaves recusadas com `enabled: false`;
    - WARN para `publishHistory` sem `history:`;
    - `checkSizes` com o header de motivo;
    - testes existentes que mudam de sentido listados por nome.
