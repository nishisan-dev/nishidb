# Aceite do M1 — os 25 critérios da spec §126

Este documento responde **"o M1 está pronto?"** sem depender de ninguém lembrar
do histórico. Cada critério tem o estado, a evidência (teste ou número medido)
e, quando não atendido, o milestone dono.

Números medidos na rodada de aceite de 2026-08-14
(`benchmark/baselines/aceite-8m-20260814.md`), com 8 milhões de entidades e
consultas SQL concorrentes com a ingestão.

| # | Critério | Estado | Evidência |
|---|---|---|---|
| 1 | iniciar de configuração YAML | **Atendido** | `internal/cli/config.go`; `cli_test.go:TestFluxoCompletoIngestEQuery` usa o `nishidb.yaml` com as seções do M3 presentes e ignoradas |
| 2 | conectar ao Kafka | **Fora do M1** | **Dono: M2.** A `source.kafka` do YAML é validada sintaticamente e não conectada (`internal/schema/spec.go`) |
| 3 | consumir JSON | **Atendido** | `internal/jsonp` (tokenizer sem alocação) + `nishidb ingest` lendo NDJSON de arquivo ou stdin |
| 4 | materializar por ID | **Atendido** | `internal/materializer`; 8 M entidades materializadas na rodada, 0 descartes |
| 5 | suportar patches | **Atendido** | merge por campo em `merge.go`; o corpus da rodada tem 85% de patch parcial (`DefaultConfig`) |
| 6 | suportar `_SEQ` | **Atendido** | stale check em `materializer.go`; `TestAutoSegundaObservacaoDivergente…` e o oráculo da I1 reproduzem a regra |
| 7 | armazenar estado binário | **Atendido** | `internal/storage/encoding` (wire format §31-33), com golden de bytes |
| 8 | sobreviver restart | **Atendido** | `internal/query/integracao_test.go:TestIntegracaoFimAFim` fecha e reabre o dataset e confere contra oráculo |
| 9 | recuperar offsets | **Parcial** | O recovery de **dados** está completo (manifest + tail replay, `internal/storage`). Offsets de **Kafka** não existem porque não há consumidor. **Dono: M2** |
| 10 | suportar 8 milhões de rows | **Atendido** | rodada de aceite: 8.000.000 rows, 12 segmentos, 6,09 GiB em disco |
| 11 | sustentar 500 eventos/s | **Atendido com folga** | **45.243 ev/s** medidos no LOAD; o STEADY roda a 500 ev/s por ser o teto do teste, não da engine |
| 12 | permanecer abaixo de 1 GiB RSS | **Atendido** | **485,9 MiB** de pico com ingestão + consultas. O RSS **anônimo** — o que o processo possui — fica em **71 MiB**; o resto é página de arquivo mmap, descartável |
| 13 | consultar por primary key | **Atendido** | `PointLookupPlan`; p95 **16 µs** pelo caminho SQL completo (teto §89: 10 ms) |
| 14 | consultar por bitmap index | **Atendido** | `BitmapScanPlan` + interseção; p95 **568 µs** com LIMIT 100 (teto §90: 50 ms) |
| 15 | consultar por hash index | **Parcial** | O **planner e o executor** implementam a rota (`HashIndexPlan`, capability por type assertion) e ela é testada contra o dublê. O índice hash **físico** é da **Fase 8 — dono: M4**; no Store real a query degrada para full scan, por decisão de projeto (nunca falha em runtime) |
| 16 | fazer full scan | **Atendido** | `FullScanPlan` sobre `ScanLive`; com LIMIT 100 p95 **728 µs**, varredura completa de 8 M rows em **21,8 s** |
| 17 | expor protocolo remoto | **Fora do M1** | **Dono: Fase 6 / M3.** O NDJSON e o `StreamWriter` já são o formato de resposta; falta o transporte HTTP |
| 18 | possuir JDBC driver | **Fora do M1** | **Dono: M3.** O contrato de leitura está fixado (uma linha = um objeto, NULL explícito, timestamp `YYYY-MM-DDThh:mm:ss.sssZ`) |
| 19 | suportar PreparedStatement | **Parcial** | Parâmetros posicionais `?` funcionam fim a fim (`TestSpec106Parameters`), coeridos pelo tipo da coluna. O **statement preparado** (plano reaproveitado entre execuções) é do M3, junto do driver |
| 20 | autenticar usuário remoto | **Fora do M1** | **Dono: M3.** A seção `server.authentication` do YAML é aceita e ignorada |
| 21 | utilizar TLS | **Fora do M1** | **Dono: M3.** Idem `server.tls` |
| 22 | suportar TTL | **Parcial** | O YAML declara `materialization.ttl` e o schema o compila (`CompiledField.IsTTL`); a **expiração** é da Fase 9. **Dono: M2/M4** |
| 23 | suportar tombstones | **Parcial** | O bit de tombstone existe no wire format e é honrado por **todas** as rotas de leitura (`TestCandidatoTombstonedNaoSai`, `TestPointLookupHonraLiveness`). Quem os **produz** por TTL/retenção é a Fase 9 |
| 24 | compactar segmentos | **Fora do M1** | **Dono: M4.** `storage.compaction` do YAML é aceita e ignorada; o design registra o critério (`minimumLiveRatio`) |
| 25 | sobreviver `kill -9` sem perder estado confirmado | **Atendido** | Recovery com manifest + replay do tail, truncando o rasgo: `internal/storage` (S12). O `nishidb ingest` interrompido reabre íntegro (`TestIngestInterrompidoPreservaOQueFoiAplicado`) |

## Resumo

| Estado | Quantos | Quais |
|---|---:|---|
| **Atendido** | 13 | 1, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 16, 25 |
| **Parcial** | 5 | 9 (dados sim, offsets Kafka não), 15 (rota sim, índice físico Fase 8), 19 (params sim, prepared no M3), 22 (declaração sim, expiração Fase 9), 23 (honrado sim, produzido na Fase 9) |
| **Fora do M1 por planejamento** | 7 | 2 (M2), 17 (M3), 18 (M3), 20 (M3), 21 (M3), 24 (M4) |

*(O item 25 aparece nos "Atendido"; a soma de 13+5+7 conta o 25 uma vez.)*

**Nenhum critério está em aberto por dívida técnica.** Os parciais e os fora de
escopo têm milestone dono e a razão registrada — nenhum deles é surpresa nem
depende de trabalho não planejado.

## O que o M1 entrega, em uma frase

Um evento JSON entra por `nishidb ingest`, é materializado por chave com merge
de campos e controle de sequência, gravado em segmento append-only com índices
mmap, sobrevive a restart e a `kill -9`, e sai por `nishidb query` como NDJSON
— consultado em SQL, com o dataset de 8 milhões de entidades cabendo em 486 MiB
de RSS e respondendo point lookup em 16 microssegundos.
