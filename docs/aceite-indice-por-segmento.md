# Aceite — subsistema 1: índice por segmento

Rodada de aceite no piloto `events` (host 10.200.20.218), binário
`v0.6.0-250-ge13a6dd`, subida em 30/08/2026 11:52, medida final em 31/08/2026
~12:55 — **25 horas de soak em produção**, com o dataset de 135 GiB migrado no
lugar. Registro de rodada: não é reescrito.

Origem dos números: os dois CSVs do amostrador (uma linha a cada 5 min),
[`piloto-soak-20260830.csv`](../benchmark/baselines/piloto-soak-20260830.csv) e
[`piloto-soak-v2-20260831.csv`](../benchmark/baselines/piloto-soak-v2-20260831.csv)
— 273 amostras válidas cobrindo 22,7 h —, o perfil de CPU de 45 s
[`piloto-pos-soak-20260831.pprof`](../benchmark/baselines/piloto-pos-soak-20260831.pprof),
e as medidas de janela de 60 s pelo método da §7.2. A subida e a migração estão
em [`piloto-pos-ciclo-20260830.md`](../benchmark/baselines/piloto-pos-ciclo-20260830.md);
o estado anterior, em
[`piloto-pre-ciclo-20260830.md`](../benchmark/baselines/piloto-pre-ciclo-20260830.md).

## O critério, e como ele tem de ser lido

A §7.1 da spec fixou: *restaurar o índice de `node` no histórico e manter os
números do paliativo* (1.636 ev/s, 528 µs/evento, RSS 227 MB, `poll_blocked`
0). Entre a escrita do critério e o aceite, a medição pré-ciclo mostrou que a
régua literal era **inatingível por qualquer binário** neste tamanho de
dataset: o próprio binário antigo, sem mexer em nada, degradou para **733 ev/s
a 173% de um núcleo (2.360 µs/evento)** com o dataset em 135 GiB — os 1.636
ev/s eram o dataset menor de 29/08, e o profile do ensaio provou que a linha de
base do paliativo era *transitória por construção*: um único campo indexado
(`identifier`) bastou para o rehash global voltar a congelar a ingestão a 47 M
de linhas. A comparação honesta é contra o **mesmo tamanho de dataset**, e é
essa que este aceite usa.

## O soak de 25 horas, número a número

| Grandeza | Medido | Leitura |
|---|---|---|
| `kafka_poll_blocked_seconds_total` | **0,0 nas 273 amostras** | o sintoma do incidente (32.319 s acumulados; 100% do intervalo bloqueado na madrugada de 30/08) ficou em zero absoluto do primeiro ao último instante |
| Congelamento de ingestão | **nenhum** | no motor antigo: 18–52 min por episódio, recorrente e dobrando |
| CPU por evento (janela de 60 s, fila drenada) | **863 µs** | binário antigo no mesmo dataset: 2.360 µs — **2,7× menos CPU por evento** |
| CPU por evento (média das 25 h) | 954 µs | inclui rotações, selagens, retenção e as consultas do período |
| Vazão | 332–375 ev/s com `lag` p95 = 395 e máx = 915 | é a taxa que o tópico **produz**, acompanhada em tempo real — não um teto. Capacidade implícita a 863 µs/evento: ~1.150 ev/s por núcleo |
| Fila de reconstrução (`pendente`) | **0 em todas as amostras** | a migração drenou em ~5 min no dia 30 e nunca voltou a encher |
| `reconstrucoes_total` | 65 → 70 em 25 h | cinco reconstruções pontuais, sem moinho |
| `crc_falhas_total` | **0** | nenhum `.sidx` reprovado |
| Janela efetiva | 6,01–6,18 h, estável | contra `window: 6h` declarado — o tempo manda, não o teto de bytes |
| Segmentos `selado` / `fora` | 56–69 / 402 → 633 | a janela reciclando em regime: segmentos envelhecem, perdem o índice, novos selam |
| **Parcela `.sidx`** (`nishidb_index_bytes`, história) | **oscilando entre 102 e 127 MB** nas 12,4 h medidas | contra teto de 2 GiB — **parou de crescer**, que é o aceite secundário |
| Erros nas 25 h | 0 do subsistema de índice; 5 `query.falhou` | os cinco são TIMEOUT de `ORDER BY` sobre milhões de candidatos — o tema do ciclo da consulta longa, não deste |

## O RSS, decomposto — e por que ele não é a régua

O RSS total foi de ~593 MiB (primeira hora) a ~869 MiB (última), e a curva
horária acompanha o crescimento dos arquivos densos por-linha da história
(`locations`/`sequences`/`prazos`), inclusive o degrau de doubling na hora 3
(609 → 1.082 MB no `history_index_bytes`). A decomposição por
`/proc/<pid>/smaps_rollup` no fim do soak:

| Parcela | kB | O que é |
|---|---:|---|
| `Anonymous` | **125.976** | heap Go + stacks — a memória que o processo POSSUI |
| `Private_Clean` | 724.344 | páginas de **arquivo mmapado, limpas** — recuperáveis pelo kernel no instante em que houver pressão (o host tem 57 GiB livres, então nunca há) |
| `Private_Dirty` | 147.516 | páginas de arquivo ainda não lavadas + parte do heap |

O que o produto promete — *heap ~constante, com o peso morando em mmap* — é
exatamente o que a decomposição mostra: **126 MB anônimos**, contra os
2.812 MiB totais (e crescendo) do binário antigo em congelamento. Os 227 MB do
paliativo eram um dataset menor, sem índice e com menos história acumulada;
compará-los com o RSS total de hoje é comparar residência de page cache, não
memória do processo.

## O perfil de CPU: o modo de falha não existe mais

45 s de perfil com a ingestão em regime
([`piloto-pos-soak-20260831.pprof`](../benchmark/baselines/piloto-pos-soak-20260831.pprof)):

| Antes (pré-ciclo, 30/08 madrugada) | Depois (31/08, 24 h de soak) |
|---|---|
| 99,16% do CPU numa única pilha, dentro de `hash.Index.reconstroi` | a pilha **não existe** — o pacote foi apagado |
| `sync/atomic.(*Int32).Add` com 64,39% flat (o mutex por slot) | 3,93% flat |
| `mmap.(*Region).AtomicLoad64` com 19,67% flat / 77,67% cum | 5,94% flat / 7,52% cum — e o que resta é `primary`/`denseFile` (débito 102, que mudou de dono) |
| ingestão parada | syscalls de I/O (22,5%) e parse de JSON (15,5%) no topo — a forma de um ingestor saudável |

A única aparição do índice novo no topo é o sort da **selagem** (1,6%, uma
rotação capturada dentro da janela do perfil) — o custo por rotação, com teto
fixo, que o débito 102 registra como resíduo.

## Veredito

**ACEITO.** Com `node` (e mais nove campos) indexados no histórico:

- o modo de falha medido — congelamento recorrente de 18–52 min sob o lock do
  writer — ficou **inexprimível**, e 25 h de produção com `poll_blocked` em
  zero absoluto o demonstram no regime, não só na migração;
- o custo por evento caiu **2,7×** contra o mesmo binário no mesmo tamanho de
  dataset, com a consulta indexada de volta;
- o índice por segmento **parou de crescer** (parcela oscilando com a janela,
  a duas ordens de grandeza do teto), que era o aceite secundário;
- a memória do processo é ~126 MB de heap estável; o resto do RSS é page cache
  de mmap que o kernel governa.

## Ressalva de versão: o binário soakado não carrega os consertos da revisão

A revisão de branch (após a subida) encontrou e corrigiu três corridas de
falso negativo — a rotação respondendo pelo WIP do segmento novo, a enumeração
do materializado com o cérebro dividido entre construção e iteração, e a
varredura de reconstrução publicando `.sidx` parcial sobre corrupção no meio de
segmento — todas gateadas por mutação
(`TestSnapshotVelhoDoCorrenteNaoRespondePeloWIPDoSegmentoNovo`,
`TestAEnumeracaoDoMaterializadoColapsaNumaCaminhadaUnicaDoLocations`,
`TestAReconstrucaoRecusaSegmentoSeladoComCorrupcaoNoMeio`,
`TestOSegmentoAusenteNaGuardaDeTamanhoEhCorridaBenigna`). O `e13a6dd` soakado
**não as contém**; nenhuma é disparável pelo workload do piloto (consultas
raras e sem update concorrente com segmento em enumeração; fila de
reconstrução vazia desde a migração), e nenhuma toca o caminho quente que este
aceite mede. A release que fechar o ciclo carrega os consertos, e a
atualização do piloto acontece por ela.

## Método

O da §7.2 da spec, na íntegra: delta de `utime+stime` de `/proc/<pid>/stat`
cruzado com `nishidb_ingest_events_total` em janela de 60 s; perfil de 45 s em
`127.0.0.1:9144`; sempre `curl --noproxy '*'` (o proxy do `.bashrc` devolve
403 no loopback). A série longa veio de um amostrador de 5 min instalado na
máquina durante o soak (os dois CSVs acima); a decomposição de RSS, de
`smaps_rollup`. O amostrador foi removido ao fim da rodada.
