# Aceite do M4 — compaction, disco e operação

Este documento responde **"o M4 está pronto?"** no mesmo formato do
[aceite do M1](aceite-m1.md), do [M2](aceite-m2.md) e da
[Fase 6](aceite-fase6.md): cada item tem estado e evidência, e a evidência é o
**nome de um teste** da suíte ou um **número medido** na rodada de aceite.

A pergunta que o milestone existe para responder é uma só: **o disco para de
crescer?** O §86 projeta 30-43 GB escritos por dia contra 5,6-8 GB de estado
vivo a 500 ev/s — dois a três dias até o ENOSPC —, e nada no M1 devolvia esse
espaço.

## Ambiente e escala da rodada

Isto vem **antes** dos números, e não depois, porque número de rodada sem
escala declarada é o defeito que o débito 6 e a errata do M1 registram. **A
escala abaixo é a que foi executada** — não é a do §125, e a diferença está
explicada logo adiante.

| | |
|---|---|
| **Artefato** | [`benchmark/baselines/aceite-m4-1m-20260822.json`](../benchmark/baselines/aceite-m4-1m-20260822.json) |
| **Data** | 2026-08-22, 07:18:06 -03 |
| **Entidades** | **1.000.000** |
| **Eventos de regime** | **8.400.001** (8,4× o key set), a **14.000 ev/s** configurados |
| **Fonte** | **Kafka real** — Redpanda `v24.2.18`, 1 nó, 4 partições, `--smp 2 --memory 1G`, tópico `nishidb-m4-r3` |
| **Segmento** | 64 MiB (`-segment-size 67108864`) |
| **Compaction** | ligada, `minimumLiveRatio` **0,50** (o padrão do §53) |
| **Leitores concorrentes** | 2 goroutines de consulta SQL durante todo o regime |
| **Checkpoint** | a cada 500.000 eventos |
| **Runtime** | Go **1.26.5**, `GOMEMLIMIT=512MiB`, `GOGC=100` |
| **Máquina** | Intel Core 7 150U, **12 vCPU** (10 núcleos, 2 threads/núcleo), **31 GiB** de RAM |
| **Disco** | NVMe ADATA SM2P41C3 512 GB, **ext4**, disco real (não tmpfs) |
| **Kernel** | Linux 6.8.0-136-generic; Docker 29.7.2 |
| **Veredito** | **rodada aprovada** — todos os gates duros avaliados e aprovados |

**Por que não é a rodada de 8M a 500 ev/s do §125.** As duas coisas medem
coisas diferentes, e a segunda não cabe em nenhum orçamento de tempo razoável:
um segmento só vira candidato depois que sua fração viva cai abaixo do
`minimumLiveRatio`, o que custa da ordem de **0,69 × entidades** eventos de
update. Com 8 milhões de entidades a 500 ev/s, o **primeiro** segmento levaria
cerca de **três horas** para ficar elegível, e o plateau — que é o que este
milestone precisa provar — pede várias vezes isso. A escala de MEMÓRIA do §125
já está medida e publicada no [aceite do M2](aceite-m2.md), com 8 milhões de
entidades e 483 MiB de RSS; o que esta rodada acrescenta é a curva de **disco**
em regime, e para ela a variável que manda não é a taxa, é **quantas vezes o
key set inteiro foi reescrito**.

## Os entregáveis do M4

| Entregável | Estado | Evidência |
|---|---|---|
| Compaction do §53-55 rodando em background | **Atendido** | `TestCompactionEmBackgroundDevolveDiscoSemNinguemChamar`, `TestCompactionPeloBinarioEstabilizaODiscoComOOperadorVendo` |
| `storage.compaction` do §78 com leitor | **Atendido** | `TestCompactionDoExemploDoParagrafo78FicaLigada` e `TestMinimumLiveRatioForaDeZeroUmEhRecusado` (config), `TestCompactionSoSobeComAChaveDoOperadorLigada` (fiação no servidor) |
| `nishidb_storage_bytes` e `_garbage_bytes` (§92) | **Atendido** | `TestStorageBytesBateComOsArquivosEmDisco`, `TestMetricaDeDiscoEhPublicadaComSemanticaDeLimite`, `TestLixoDeDiscoNuncaPassaPorBaixoDeZero` |
| As três séries de compaction do §92 | **Atendido** | `TestSeriesDeCompactionGanhamAlimentador`, `TestSeriesDeCompactionSaemZeradasComAChaveDesligada` |
| `status`, `datasets` e `inspect` (§96, leitura) | **Atendido** | `TestStatusRemotoFuncionaComOServidorNoAr`, `TestDatasetsListaOQueOServidorServe`, `TestInspectDetalhaColunasEIndices` |
| Checkpoint por tempo no consumidor (débito 56) | **Atendido** | `TestCheckpointPorTempoDestravaOLagComIngestaoOciosa` |

## O que a spec exige, item a item

| # | Obrigação | Seção | Estado | Evidência |
|---|---|---|---|---|
| 1 | Compactar segmentos (§126.24) | §53-55 | **Atendido** | `TestCompactionDevolveDiscoSemPerderNenhumaRow`; na rodada, **3,68 GB devolvidos ao filesystem** em 104 aposentadorias |
| 2 | A compaction **não** carrega o segmento em memória | §54 | **Atendido** | `TestCopyForwardReusaOBufferDoWriterEmVezDeAlocarPorRecord`; heap estável de **1,3 MiB** com a compaction moendo 11,6 MB/s |
| 3 | Atualização atômica das locations | §55 | **Atendido** | `TestCopyForwardNaoRessuscitaPayloadAntigoSobUpdateConcorrente`, `TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica` |
| 4 | `NO update in-place` — a cópia passa pelo caminho normal de escrita | §128 | **Atendido** | `TestCopyForwardRepublicaALocationSemInflarALiveness` |
| 5 | Sobreviver `kill -9` sem perder estado confirmado (§126.25), **agora com a compaction de pé** | §52 | **Atendido** | `TestKill9DuranteACompactionNoProdutoPreservaTodasAsRows`, `TestKill9EntreORenameEOCheckpointDevolveODatasetInteiro`, `TestKill9EntreOCheckpointEOUnlinkDevolveODatasetInteiro` |
| 6 | Métricas do §92 para disco e compaction | §92 | **Atendido** | Tabela acima; na rodada, `compaction_running=true` do início ao fim |
| 7 | Comandos operacionais de leitura | §96 | **Atendido em parte** | `status`, `datasets` e `inspect`; `compact` e `checkpoint` são rotas **mutantes** e ficam para o M5, atrás do papel ADMIN |
| 8 | Um único writer (`NO multi-writer v1`) | §128 | **Atendido** | Toda mutação da compaction passa por `s.wmu`; `TestCandidatosLeOSegmentoCorrenteDEPOISDeEntrarNaSecaoCritica` |

## As medidas que o §125 pede

Todas saem do artefato da rodada. As que o §125 nomeia e esta rodada **não**
mede na escala de 8M estão marcadas com o documento em que estão.

| Medida | Valor | Onde |
|---|---|---|
| RSS (pico) | **166,8 MiB** contra o teto de 1 GiB | gate `rss_limit` |
| Go heap estável | **1,3 MiB** com 1M de rows (2,7 MiB com 250 mil) | gate `heap_decoupled` |
| GC | 357,9 coletas/min no LOAD | `load.gc_per_min` |
| Uso de disco | **295,4 MiB → 853,9 MiB**, com pico de 749,6 MiB na primeira metade | série do `disk_plateau` |
| Vazão de ingestão | **138.713 ev/s** no LOAD (1M eventos em 7,21 s) | `load.events_per_sec` |
| Query p50 / p95 / p99 | point lookup **6 / 18 / 29 µs**; bitmap 170 / 340 / 508 µs; interseção 201 / 392 / 598 µs; união 171 / 351 / 531 µs; full scan com LIMIT 327 / 939 / 1.544 µs — **3.463.742 amostras por classe** | `queries[]` |
| Checkpoint | 16 checkpoints em **0,855 s** no regime (2 em 0,174 s no LOAD) | `steady.checkpoint_sec` |
| Restart | **0,72 s** | `restart_sec` |
| Vazão da compaction | **6,98 GB lidos** e **3,30 GB escritos** na janela de 600 s de ingestão — ~**11,6 MB/s** de segmento varrido, com **3,68 GB devolvidos** | `store_stats` + soma dos `bytes_devolvidos` do log |
| Lag do Kafka ao fim | **0** | `kafka_lag_final` |
| Fila limitada (pico) | 5.000 registros / 2.544.262 bytes | `kafka_queue_*_max` |
| Eventos perdidos | **0 descartados, 0 falhas** (8.287.171 updates, 112.830 stale) | `steady` |
| Erros de consulta | **0** em 3,46 milhões de consultas concorrentes com a ingestão | `sql_errors` |

> **Dois números do artefato NÃO devem ser lidos como taxa, e a razão está na
> seção "o que a rodada expôs".** `steady.events_per_sec` sai **6.327** e
> `steady.duration_sec` sai **1.327,6 s**, quando a ingestão de fato durou os
> **600 s** configurados a 14.000 ev/s: o harness ficou **bloqueado** ao fim da
> janela, e o relógio da fase continuou correndo. Pelo mesmo motivo,
> `steady.allocs_per_event` (59,4) e `steady.gc_per_min` (1.110) contam as
> alocações dos dois leitores SQL durante um intervalo em que **nenhum evento
> foi ingerido**. A curva de disco não é afetada — não há amostra dentro do
> bloqueio —, e é por isso que o gate continua valendo.

## O gate que importa, e por que ele vale

A compaction pode passar em todo teste unitário do §53-55 e **não devolver
disco em regime**. O que discorda disso é o gate `disk_plateau`, que lê a curva
de disco da fase de REGIME e cobra que a inclinação da **segunda metade** da
janela fique abaixo de uma fração da **primeira**:

> `crescimento de 2.075.852 B/amostra na 1ª metade contra 703.183 B/amostra na
> 2ª (razão 0,339, teto 0,35) — 422 amostras, disco de 295,4 MiB a 853,9 MiB
> (pico da 1ª metade em 749,6 MiB)`

**A margem é de 3%, e isso está escrito porque é o que foi medido.** O gate
passou; ele não passou com folga. Quem repetir a rodada com menos churn vai
reprovar, e a seção seguinte mostra exatamente onde fica essa fronteira.

O gate não é decoração: ele foi **aferido** contra a série que a ausência de
compaction produz, e `TestGateDeDiscoReprovaCrescimentoLinear` reprova o
crescimento linear puro. Os dois artefatos de aferição da Task 11 —
[`gate-disco-compaction-on-50k-20260822.json`](../benchmark/baselines/gate-disco-compaction-on-50k-20260822.json)
e
[`gate-disco-compaction-off-50k-20260822.json`](../benchmark/baselines/gate-disco-compaction-off-50k-20260822.json)
— são a mesma carga com a compaction ligada e desligada. E `NÃO AVALIADO`
reprova a rodada: gate que ninguém pôde avaliar não é gate aprovado, que é a
errata do M1.

## A calibração: o que ela custou e o que ela ensinou

Foram necessárias **quatro rodadas** até uma aprovar, e as três reprovadas
estão arquivadas junto — elas são o dado, não o rascunho. A variável que decide
não é a taxa de ingestão: é **quantas vezes o key set inteiro foi reescrito
dentro da janela de regime**.

| Artefato | Entidades | Taxa | Eventos de regime | Múltiplos do key set | Razão do gate |
|---|---:|---:|---:|---:|---:|
| [`calibracao-m4-1m-46k`](../benchmark/baselines/calibracao-m4-1m-46k-20260822.json) | 1.000.000 | 46.000 ev/s | 5.519.040 | **5,5×** | 0,510 — **reprova** |
| [`calibracao-m4-2m-16k`](../benchmark/baselines/calibracao-m4-2m-16k-20260822.json) | 2.000.000 | 16.000 ev/s | 8.319.988 | **4,2×** | 0,467 — **reprova** |
| [`calibracao-m4-2m-10k`](../benchmark/baselines/calibracao-m4-2m-10k-20260822.json) | 2.000.000 | 10.000 ev/s | 7.999.994 | **4,0×** | 0,510 — **reprova** |
| [`gate-disco-compaction-on-50k`](../benchmark/baselines/gate-disco-compaction-on-50k-20260822.json) | 50.000 | 5.000 ev/s | 599.996 | **12,0×** | aprova |
| [`aceite-m4-1m`](../benchmark/baselines/aceite-m4-1m-20260822.json) | 1.000.000 | 14.000 ev/s | 8.400.001 | **8,4×** | 0,339 — **aprova** |

Baixar a taxa de 46.000 para 10.000 ev/s — cinco vezes — **não mexeu na razão**
(0,510 nas duas pontas). O que mexeu foi passar de 4-5,5× para 8,4× de churn.
Faz sentido pelo mecanismo: enquanto o key set não foi reescrito vezes o
bastante, boa parte da janela ainda é a rampa em que os segmentos do LOAD nem
viraram candidatos, e a "segunda metade" continua sendo transiente e não
regime. Está registrado como **débito 65**, porque quem repetir esta rodada
sem saber disso vai queimar meia hora por tentativa — e porque o número
mínimo não está escrito em lugar nenhum do produto.

## O que a rodada expôs

**O harness TRAVA quando o tópico esvazia dentro da janela.** A fase de regime
do `ingest-bench` produz exatamente `taxa × duração` eventos no tópico e depois
os consome na mesma taxa. Quando o consumidor alcança o fim do tópico antes de
o relógio da fase fechar — e ele alcança, porque o limitador se recupera de
qualquer atraso —, `fonteKafka.Next` bloqueia em `fila.Recebe(context.Background())`
e **não sai mais**: a checagem de duração do laço acontece antes do `Next`, e
nunca é reavaliada. A rodada ficou **12 minutos parada** nesse ponto (última
aposentadoria às 07:38:17, processo vivo e mudo até as 07:50) e só seguiu depois
que **um evento do próprio corpus foi reproduzido no tópico** para destravá-la —
é dele o `+1` em `8.400.001`, e é ele a causa do `duration_sec` de 1.327,6 s
sobre 600 s de ingestão. Não contamina nada que o gate lê, mas contamina três
campos do relatório, e está dito acima quais. Registrado como **débito 67**.

**A compaction não avisa quando não dá conta.** Nas três rodadas reprovadas o
disco cresceu em regime com a compaction viva, e nenhuma série do §92 diz isso:
`compaction_running` responde 1, os contadores de bytes sobem, e o painel fica
idêntico ao de uma rodada saudável. Faz parte do **débito 65**.

**Os três resíduos de instrumento da Fase B** — o `os.Stat` engolido no e2e, os
dois helpers do harness sem oráculo direto e a asserção do piso do plateau
calibrada com folga de 17× — estão abertos como **débitos 62, 63 e 64**.

**Os diagramas não estão renderizando, e a causa é externa.** O certificado TLS
de `git-raw.nishisan.dev` expirou em 2026-08-21 15:07 UTC, então o proxy do
PlantUML devolve HTTP 500 para todas as URLs de `architecture.md`. Os nove
fontes de `docs/diagrams/` foram validados **por dois caminhos independentes**
no dia: `plantuml -checkonly -failfast2` local (PlantUML 1.2026.2, exit 0 nos
nove) e `uml.nishisan.dev/png/<código>` (PlantUML 1.2025.8 do outro lado, HTTP
200 `image/png` nos nove). Os dois validadores foram **aferidos com um `.puml`
propositalmente quebrado**: exit 200 no local, HTTP 400 com
`x-plantuml-diagram-error: Syntax Error?` no servidor. Detalhe no **débito 39**.

## Como reproduzir

```bash
# broker
docker run --rm -d --name rp-m4 -p 19092:19092 redpandadata/redpanda:v24.2.18 \
  redpanda start --overprovisioned --smp 2 --memory 1G --reserve-memory 0M \
  --node-id 0 --check=false \
  --kafka-addr PLAINTEXT://0.0.0.0:19092 \
  --advertise-kafka-addr PLAINTEXT://127.0.0.1:19092
docker exec rp-m4 rpk topic create nishidb-m4-r3 -p 4 --brokers 127.0.0.1:19092

# a rodada de referência do §110 (menor), que reprova por construção:
# sem -heap-baseline não há o que comparar, e gate duro não avaliado não passa
go build -o ingest-bench ./benchmark/ingest-bench
GOMEMLIMIT=512MiB GOGC=100 ./ingest-bench -data /var/tmp/nb-m4-base \
    -entities 250000 -steady 60s -rate 16000 -segment-size 67108864 \
    -sql-queries 4 -kafka 127.0.0.1:19092 -kafka-topic nishidb-m4-base \
    -kafka-group nishidb-m4-base -report base-250k.json

# o ACEITE
GOMEMLIMIT=512MiB GOGC=100 ./ingest-bench -data /var/tmp/nb-m4-r3 \
    -entities 1000000 -steady 600s -rate 14000 -segment-size 67108864 \
    -sql-queries 2 -kafka 127.0.0.1:19092 -kafka-topic nishidb-m4-r3 \
    -kafka-group nishidb-m4-r3 -heap-baseline base-250k.json \
    -report aceite-m4-1m.json
```

⚠️ `-data` precisa apontar para **disco real**. Esta rodada grava vários GiB com
`fallocate` de verdade; em `/tmp` montado como tmpfs isso vira RAM, derruba a
máquina e ainda invalida a medição, porque o RSS passaria a incluir o dataset.
Cada tópico precisa ser **novo**: o consumidor começa do início, e reaproveitar
um tópico já usado faria a rodada reprocessar o corpus anterior.

Se o comando parar de imprimir com o tópico esvaziado, é o **débito 67**;
produzir um evento qualquer do corpus no tópico o destrava.

A suíte inteira, essa sim, roda sem broker:

```bash
make test && make test-race && make lint
```

## O que o M4 NÃO fecha

| Item | Onde fica |
|---|---|
| `POST /v1/compact` e `nishidb compact`/`checkpoint` | M5, atrás do papel ADMIN. Rotas **mutantes** não existem neste milestone (decisão D3 do roadmap) |
| Recuperação de tombstone | Record tombstoned é **copiado para frente** (decisão D2): a location aponta para ele. Limpar location e primary é do TTL, no M6 |
| `Stats`/`Sequence` com `Store` fechado (débito 34) | **Continua aberto** — o M4 passou sem tocar nisso, e o registro está na errata de [`debitos.md`](debitos.md). Muda a assinatura do `Store`, e portanto todo chamador |
| Posting de bitmap órfã (débito 45) | **Continua aberto**, e a compaction **não** a fecha: o copy-forward chama o `applyRecord` com o payload igual ao anterior, então nenhum bit é limpo. Quem fecha é o rebuild de índice (débito 22) |
| Aviso de compaction atrasada | Débito 65 |
| `storage.durability` (§47) | Débito 50, intocado |
| Autenticação e TLS | Fase 11, no M5. Quem alcança a porta continua lendo o dataset |
