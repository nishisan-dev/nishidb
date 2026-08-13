# Aceite do M1 — os 25 critérios da spec §126

Este documento responde **"o M1 está pronto?"** sem depender de ninguém lembrar
do histórico. Cada critério tem o estado, a evidência (teste ou número medido)
e, quando não atendido, o milestone dono.

Números medidos na rodada de aceite de 2026-08-15
(`benchmark/baselines/aceite-8m-20260815.md`), com 8 milhões de entidades, o
corpus de referência (85% de patch parcial, 10% Extended JSON, 1% campo
desconhecido, 5% null explícito) e consultas SQL concorrentes com a ingestão.

**Cada evidência abaixo cita um teste que existe e prova o que o critério
afirma.** A revisão de fechamento encontrou duas citações que não sustentavam o
item; elas foram trocadas, e o que a suíte não exercitava (`kill -9` real)
passou a ser exercitado em vez de continuar creditado.

| # | Critério | Estado | Evidência |
|---|---|---|---|
| 1 | iniciar de configuração YAML | **Atendido** | `internal/cli/config.go`; `cli_test.go:TestFluxoCompletoIngestEQuery` usa o `nishidb.yaml` com as seções do M3 presentes e ignoradas |
| 2 | conectar ao Kafka | **Fora do M1** | **Dono: M2.** A `source.kafka` do YAML é validada sintaticamente e não conectada (`internal/schema/spec.go`) |
| 3 | consumir JSON | **Atendido** | `internal/jsonp` (tokenizer sem alocação) + `nishidb ingest` lendo NDJSON de arquivo ou stdin |
| 4 | materializar por ID | **Atendido** | `internal/materializer`; 8 M entidades materializadas na rodada, 0 evento descartado e 0 falho (`load.skipped=0`, `load.failed=0`) |
| 5 | suportar patches | **Atendido** | Merge por campo em `merge.go` (`merge_test.go:TestMergeSpec22Exemplo`, `TestMergeCampoAusenteMantemAnterior`, `TestMergeNullRemoveCampoEAusenciaEhTransitiva`, mais o oráculo da I1). O corpus da rodada tem **85% de patch parcial**, registrado no artefato em `config.load_corpus` — antes esta linha citava um corpus que a rodada não usava. **Ressalva medida:** o LOAD cria 8 M rows (inclusive por patch sobre chave nova); o merge sobre estado **existente** em escala é o item de carga mista do M2 (ver `docs/debitos.md`) |
| 6 | suportar `_SEQ` | **Atendido** | Stale check em `materializer.go:apply`; `materializer_test.go:TestApplySequenceGreaterAplica` e `TestApplySequenceEqualEMenorDescartam` cobrem os três lados da regra §25, e a rodada registra 29.950 descartes por stale no STEADY |
| 7 | armazenar estado binário | **Atendido** | `internal/storage/encoding` (wire format §31-33), com golden de bytes |
| 8 | sobreviver restart | **Atendido** | `internal/query/integracao_test.go:TestIntegracaoFimAFim` fecha e reabre o dataset e confere contra oráculo |
| 9 | recuperar offsets | **Parcial** | O recovery de **dados** está completo (manifest + tail replay, `internal/storage`). Offsets de **Kafka** não existem porque não há consumidor. **Dono: M2** |
| 10 | suportar 8 milhões de rows | **Atendido** | rodada de aceite: 8.000.000 rows, 4 segmentos, 2,31 GiB em disco com o corpus de referência (6,09 GiB na rodada de eventos completos de 2026-08-14) |
| 11 | sustentar 500 eventos/s | **Atendido com folga** | **132.931 ev/s** no LOAD (146.216 ev/s descontando o checkpoint); o STEADY roda a 500 ev/s por ser o teto do teste, não da engine |
| 12 | permanecer abaixo de 1 GiB RSS | **Atendido** | **482,9 MiB** de pico com ingestão + consultas. O RSS **anônimo** — o que o processo possui — fica em **69,8 MiB**; o resto é página de arquivo mmap, descartável |
| 13 | consultar por primary key | **Atendido** | `PointLookupPlan`; p95 **18 µs** pelo caminho SQL completo, 84.479 amostras (teto §89: 10 ms) |
| 14 | consultar por bitmap index | **Atendido** | `BitmapScanPlan` + interseção; p95 **522 µs** com LIMIT 100, 84.480 amostras (teto §90: 50 ms) |
| 15 | consultar por hash index | **Parcial** | O **planner e o executor** implementam a rota (`HashIndexPlan`, capability por type assertion) e ela é testada contra o dublê. O índice hash **físico** é da **Fase 8 — dono: M4**; no Store real a query degrada para full scan, por decisão de projeto (nunca falha em runtime) |
| 16 | fazer full scan | **Atendido** | `FullScanPlan` sobre `ScanLive`; com LIMIT 100 p95 **4,3 ms**, varredura completa de 8 M rows em **17,0 s** |
| 17 | expor protocolo remoto | **Fora do M1** | **Dono: Fase 6 / M3.** O NDJSON e o `StreamWriter` já são o formato de resposta; falta o transporte HTTP |
| 18 | possuir JDBC driver | **Fora do M1** | **Dono: M3.** O contrato de leitura está fixado (uma linha = um objeto, NULL explícito, timestamp `YYYY-MM-DDThh:mm:ss.sssZ`) |
| 19 | suportar PreparedStatement | **Parcial** | Parâmetros posicionais `?` funcionam fim a fim pela API Go (`TestSpec106Parameters`), coeridos pelo tipo da coluna. O **CLI não tem como fornecê-los** e o **statement preparado** (plano reaproveitado) é do M3, junto do driver |
| 20 | autenticar usuário remoto | **Fora do M1** | **Dono: M3.** A seção `server.authentication` do YAML é aceita e ignorada |
| 21 | utilizar TLS | **Fora do M1** | **Dono: M3.** Idem `server.tls` |
| 22 | suportar TTL | **Parcial** | O YAML declara `materialization.ttl` e o schema o compila (`CompiledField.IsTTL`); a **expiração** é da Fase 9. **Dono: M2/M4** |
| 23 | suportar tombstones | **Parcial** | O bit de tombstone existe no wire format e é honrado pelas rotas de leitura de row (`TestCandidatoTombstonedNaoSai`, `TestPointLookupHonraLiveness`). **Ressalva registrada:** `Store.Sequence` valida só a location — ver débito de tombstone × `_SEQ`. Quem os **produz** por TTL/retenção é a Fase 9 |
| 24 | compactar segmentos | **Fora do M1** | **Dono: M4.** `storage.compaction` do YAML é aceita e ignorada; o design registra o critério (`minimumLiveRatio`) |
| 25 | sobreviver `kill -9` sem perder estado confirmado | **Atendido** | `internal/cli/kill9_test.go:TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado` mata o binário com **SIGKILL** no meio da ingestão e confere que o estado confirmado sobrevive (medido: 10.000 rows confirmadas, 10.231 visíveis após o recovery — o tail replay recupera além do checkpoint). O mecanismo tem cobertura própria em `internal/storage`: `TestReopenTailIncompleto`, `TestReopenCRCCorrompidoNoTailDescartaDali`, `TestCrashEntreApplyEManifest`, `TestPhantomCommitViraVisivelNoReplay` |

## Resumo

| Estado | Quantos | Quais |
|---|---:|---|
| **Atendido** | 14 | 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 16, 25 |
| **Parcial** | 5 | 9 (dados sim, offsets Kafka não), 15 (rota sim, índice físico Fase 8), 19 (params sim, prepared no M3), 22 (declaração sim, expiração Fase 9), 23 (honrado na leitura de row, produzido na Fase 9) |
| **Fora do M1 por planejamento** | 6 | 2 (M2), 17 (M3), 18 (M3), 20 (M3), 21 (M3), 24 (M4) |

14 + 5 + 6 = 25.

**Nenhum critério está em aberto por dívida técnica.** Os parciais e os fora de
escopo têm milestone dono e a razão registrada — nenhum deles é surpresa nem
depende de trabalho não planejado.

## O que os números NÃO dizem

Está aqui porque um documento de aceite que só afirma é um documento que não dá
para auditar:

- **Não há Kafka.** A ingestão vem de corpus em memória (harness) ou de NDJSON
  (CLI). O consumidor é o M2, e o custo dele não está medido.
- **A escrita e a consulta não se sobrepõem em escala.** O LOAD escreve sem
  consulta; no STEADY, dos 30.000 eventos oferecidos apenas **50 viraram
  escrita** (os outros 29.950 são descarte por stale, §25). A medição de carga
  **mista** de verdade é item do M2. O flanco que existe hoje é a rodada
  `ingest-8m-20260813`, sem SQL, com **359.955 merges reais** e RSS
  praticamente idêntico.
- **Um writer, muitos leitores vale DENTRO do processo.** Entre processos o
  `flock` é exclusivo: com um `ingest` em voo, um `query` concorrente sai com
  exit 3. Os números de leitura concorrente vêm de leitores **in-process**
  (`benchmark/ingest-bench`).
- **O índice hash é da Fase 8:** a rota de hash degrada para full scan no Store
  real.
- **`allocs/evento` do STEADY mistura ingestão e query** (367,9 com 4 leitores
  SQL): o numerador inclui as alocações das consultas e o denominador só os
  30.000 eventos. O número de ingestão é o do LOAD: **0,0086**.

## O que o M1 entrega, em uma frase

Um evento JSON entra por `nishidb ingest`, é materializado por chave com merge
de campos e controle de sequência, gravado em segmento append-only com índices
mmap, sobrevive a restart e a `kill -9`, e sai por `nishidb query` como NDJSON
— consultado em SQL, com o dataset de 8 milhões de entidades cabendo em 483 MiB
de RSS e respondendo point lookup em 18 microssegundos.
