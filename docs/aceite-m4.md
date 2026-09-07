# Aceite do M4 — compaction, disco e operação

Este documento responde **"o M4 está pronto?"** no mesmo formato do
[aceite do M1](aceite-m1.md), do [M2](aceite-m2.md) e da
[Fase 6](aceite-fase6.md): cada item tem estado e evidência, e a evidência é o
**nome de um teste** da suíte ou um **número medido** na rodada de aceite.

A pergunta que o milestone existe para responder é uma só: **o disco para de
crescer?** O §86 projeta 30-43 GB escritos por dia contra 5,6-8 GB de estado
vivo a 500 ev/s — dois a três dias até o ENOSPC —, e nada no M1 devolvia esse
espaço.

> **Errata (25/08/2026): "devolvidos ao filesystem" era LISTING, não filesystem.**
> Os 3,68 GB desta rodada foram medidos somando o diretório de dados, e o
> diretório mentia. `compactaSegmento` lê o segmento que vai aposentar pelo
> `ScanFrom`, que **cacheia o descritor**; o cache do `segment.Reader` não tinha
> despejo; e o `os.Remove` da aposentadoria removia o **nome** e nunca o
> **inode**. Os blocos seguiam alocados enquanto o processo vivesse — um inode
> preso por aposentadoria, de forma determinística. `dirBytes` do harness,
> `discoDosSegmentos` do e2e e o `Stats.StorageBytes` somam o listing, e para os
> três o arquivo tinha sumido; o próprio `bytes_devolvidos` do log da
> aposentadoria anunciava bytes que ninguém recebeu. **`df` e `du` discordavam, e
> nenhum instrumento deste projeto os consultava.**
>
> Corrigido em `Reader.CloseSegment` mais a barreira `aposMu`, e registrado como
> **débito 78** (já fechado). O número de disco desta rodada **não foi
> recalculado** — a rodada não é reproduzível a posteriori —, então leia-o como
> "bytes que a compaction desocupou logicamente", que é afirmação verdadeira, e
> não como "bytes que voltaram ao dispositivo", que não era.
>
> Isto **agrava** o veredito abaixo em vez de aliviá-lo: o disco não parou, e
> parte do que se pensava devolvido continuava alocado.

**A resposta desta rodada é "ainda não, nesta escala".** A compaction devolve
disco de verdade — 3,68 GB em 104 aposentadorias, com o crescimento
desacelerando de 91 para 26 MiB/min ao longo da janela —, mas o disco **não
parou**: ele terminou a janela de carga ainda subindo. O gate `disk_plateau`
saiu `pass` no artefato, e esse `pass` **é falso**: ele depende de UMA amostra
colhida 12 minutos depois de a ingestão ter parado. A recontagem está na seção
["O gate que importa"](#o-gate-que-importa-e-por-que-ele-não-vale-nesta-rodada),
e o oráculo que a fixa na suíte é
`TestOPlateauDoAceiteM4DependeDaAmostraColhidaForaDaJanelaDeCarga`. Esta
correção é a errata do fecho do M4, e ela existe pela mesma razão que a errata
do M1: **gate que passou por artefato de medida não é gate aprovado.**

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
| **Veredito** | **rodada aprovada pelo harness** (`passed: true`, os 12 gates avaliados, 8 deles duros) — **mas o `disk_plateau` NÃO se sustenta**, e a recontagem está em ["O gate que importa"](#o-gate-que-importa-e-por-que-ele-não-vale-nesta-rodada) |

**Por que não é a rodada de 8M a 500 ev/s do §125.** As duas coisas medem
coisas diferentes, e a segunda não cabe em nenhum orçamento de tempo razoável:
um segmento só vira candidato depois que sua fração viva cai abaixo do
`minimumLiveRatio`, o que custa da ordem de **0,69 × entidades** eventos de
update. Com 8 milhões de entidades a 500 ev/s, o **primeiro** segmento levaria
cerca de **três horas** para ficar elegível, e o plateau — que é o que este
milestone precisa provar — pede várias vezes isso. A escala de MEMÓRIA do §125
já está medida e publicada no [aceite do M2](aceite-m2.md), com 8 milhões de
entidades e **491,0 MiB** de RSS de pico; o que esta rodada acrescenta é a curva
de **disco** em regime.

> **Errata deste parágrafo:** ele publicava **483 MiB** atribuídos ao aceite do
> M2, e o 483 MiB é do [aceite do M1](aceite-m1.md) — o M2 mede **491,0 MiB**,
> com Kafka e quatro consultas SQL concorrentes por cima das mesmas 8 milhões de
> entidades. Número certo, documento errado: exatamente a classe de defeito que
> a conferência de fonte existe para apanhar, e a mesma família do débito 6.

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
| 1 | Compactar segmentos (§126.24) | §53-55 | **Atendido** | `TestCompactionDevolveDiscoSemPerderNenhumaRow`; na rodada, **3,68 GB desocupados** em 104 aposentadorias (ver a errata do topo: a medida era de listing, não de filesystem) |
| 2 | A compaction **não** carrega o segmento em memória | §54 | **Atendido** | `TestCopyForwardReusaOBufferDoWriterEmVezDeAlocarPorRecord`; heap estável de **1,3 MiB** com a compaction moendo ~5,3 MB/s |
| 3 | Atualização atômica das locations | §55 | **Atendido** | `TestCopyForwardNaoRessuscitaPayloadAntigoSobUpdateConcorrente`, `TestCopyForwardLeALocationDEPOISDeEntrarNaSecaoCritica` |
| 4 | `NO update in-place` — a cópia passa pelo caminho normal de escrita | §128 | **Atendido** | `TestCopyForwardRepublicaALocationSemInflarALiveness` |
| 5 | Sobreviver `kill -9` sem perder estado confirmado (§126.25), **agora com a compaction de pé** | §52 | **Atendido** | `TestKill9DuranteACompactionNoProdutoPreservaTodasAsRows`, `TestKill9EntreORenameEOCheckpointDevolveODatasetInteiro`, `TestKill9EntreOCheckpointEOUnlinkDevolveODatasetInteiro` |
| 6 | Métricas do §92 para disco e compaction | §92 | **Atendido** | Tabela acima; na rodada, `compaction_running=true` **no snapshot do fim**, com `compaction_bytes_read` em 6,98 GB |
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
| Uso de disco | **295,4 MiB → 887,4 MiB** na janela de carga, com pico de 749,6 MiB na primeira metade (a série cai para 853,9 MiB na amostra pós-bloqueio, que é a que **não** vale — ver adiante) | série do `disk_plateau` |
| Vazão de ingestão | **138.713 ev/s** no LOAD (1M eventos em 7,21 s) | `load.events_per_sec` |
| Query p50 / p95 / p99 | point lookup **6 / 18 / 29 µs**; bitmap 170 / 340 / 508 µs; interseção 201 / 392 / 598 µs; união 171 / 351 / 531 µs; full scan com LIMIT 327 / 939 / 1.544 µs — **3.463.742 amostras por classe** | `queries[]` |
| Checkpoint | 16 checkpoints em **0,855 s** no regime (2 em 0,174 s no LOAD) | `steady.checkpoint_sec` |
| Restart | **0,72 s** | `restart_sec` |
| Vazão da compaction | **6,98 GB lidos** e **3,30 GB escritos**, com **3,68 GB desocupados** (errata do topo) — ~**5,3 MB/s** de segmento varrido sobre os 1.327,6 s de relógio da fase | `store_stats` + soma dos `bytes_devolvidos` do log |
| Lag do Kafka ao fim | **0** | `kafka_lag_final` |
| Fila limitada (pico) | 5.000 registros / 2.544.262 bytes | `kafka_queue_*_max` |
| Eventos perdidos | **0 descartados, 0 falhas** (8.287.171 updates, 112.830 stale) | `steady` |
| Erros de consulta | **0** em **17.318.713** consultas concorrentes com a ingestão (as cinco classes somadas, ~3,46 M cada) | `sql_errors` + soma dos `queries[].count` |

> **Quatro números do artefato NÃO devem ser lidos como taxa, e a razão está na
> seção "o que a rodada expôs".** `steady.events_per_sec` sai **6.327** e
> `steady.duration_sec` sai **1.327,6 s**, quando a ingestão de fato durou os
> **600 s** configurados a 14.000 ev/s: o harness ficou **bloqueado** ao fim da
> janela, e o relógio da fase continuou correndo. Pelo mesmo motivo,
> `steady.allocs_per_event` (59,4) e `steady.gc_per_min` (1.110) contam as
> alocações dos dois leitores SQL durante um intervalo em que **nenhum evento
> foi ingerido**.
>
> **Errata: a curva de disco TAMBÉM é afetada, e é o pior dos quatro casos.**
> Este parágrafo dizia "não há amostra dentro do bloqueio — e é por isso que o
> gate continua valendo". A primeira metade da frase é verdadeira e a conclusão
> é falsa: não há amostra DENTRO do bloqueio, mas há a amostra do **FIM** dele,
> e é ela o extremo que `avaliaGateDeDisco` lê como segunda metade. Durante os
> 727,6 s parados a compaction seguiu moendo com **zero evento entrando** e
> devolveu 33,5 MiB, de modo que a série termina 33,5 MiB **abaixo** da última
> amostra sob carga. Não é um trecho plano injetado no extremo — é uma
> **queda**. A recontagem está na seção seguinte.

## O gate que importa, e por que ele NÃO vale nesta rodada

A compaction pode passar em todo teste unitário do §53-55 e **não devolver
disco em regime**. O que discorda disso é o gate `disk_plateau`, que lê a curva
de disco da fase de REGIME e cobra que a inclinação da **segunda metade** da
janela fique abaixo de uma fração da **primeira**. O que o artefato publica é
isto:

> `crescimento de 2.075.852 B/amostra na 1ª metade contra 703.183 B/amostra na
> 2ª (razão 0,339, teto 0,35) — 422 amostras, disco de 295,4 MiB a 853,9 MiB
> (pico da 1ª metade em 749,6 MiB)`

**E esse número não prova plateau nenhum.** A 422ª amostra — o extremo que
`taxaPorAmostra` usa como fim da segunda metade — foi colhida em
`elapsed = 1.936,70 s`, com 8.400.001 eventos. A 421ª está em
`elapsed = 1.209,05 s`, com 8.400.000. São **727,65 s de vão** com **um** evento
de avanço: é o bloqueio do débito 67 por inteiro, e o único evento é o que foi
reproduzido à mão no tópico para destravar o harness. Nesse intervalo a
compaction moeu com **zero ingestão**, e a série cai de 887,4 MiB para
853,9 MiB.

Recontado com as mesmas funções do produto — é o que
`TestOPlateauDoAceiteM4DependeDaAmostraColhidaForaDaJanelaDeCarga` roda, sobre
o artefato commitado:

| Série lida | 1ª metade | 2ª metade | Razão | Teto 0,35 |
|---|---:|---:|---:|---|
| 422 amostras (o publicado) | 2.075.852 B/amostra | 703.183 B/amostra | **0,339** | passa |
| 421 amostras (sem a amostra pós-bloqueio) | 2.036.351 B/amostra | 919.621 B/amostra | **0,452** | **REPROVA** |

Sendo o `disk_plateau` um gate **DURO**, a rodada inteira sairia `passed: false`.
Os 3% de margem que este documento apresentava como honestidade **não existem**:
eles são fabricados por 12 minutos de compaction rodando sem carga.

E 0,339 não é sequer um ponto estável. Truncando de 1 a 7 amostras do fim, a
razão sai 0,452 / 0,424 / **0,262** / 0,371 / 0,483 / 0,427 / 0,431: a maioria
reprova, **k = 3 volta a aprovar**, e o veredito do gate duro **oscila conforme
qual amostra calhou de ser a última**. Um critério que muda de resposta com o
recorte não está decidindo sobre a curva; está decidindo sobre o recorte. É essa
oscilação — e não uma reprovação uniforme, que seria falsa — que
`TestOPlateauDoAceiteM4DependeDaAmostraColhidaForaDaJanelaDeCarga` fixa.

Quem responde sobre a **curva** é um estimador que não dá peso especial a
extremo nenhum: a inclinação por **mínimos quadrados** em cada metade dá razão
**0,473** com a amostra contaminada e **0,478** sem ela — reprova nas duas, e a
proximidade entre os dois valores é a prova de que a oscilação acima é do
estimador do gate, não da curva medida. Está em
`TestOPlateauDoAceiteM4TambemReprovaPorMinimosQuadrados`, com o estimador
aferido contra uma rampa de inclinação conhecida antes de ser usado.

**O que a rodada de fato mostra**, lendo a série da janela de carga em janelas
de 60 s (média do disco por janela, em MiB):

| Janela | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Média | 425,2 | 515,7 | 579,1 | 637,6 | 694,3 | 741,0 | 782,1 | 809,6 | 845,9 | 871,9 |

A compaction **está trabalhando** — o crescimento desacelera de 91 MiB/min na
primeira transição para 26 MiB/min na última —, mas o disco **não estabilizou**:
a média sobe em **cada uma das nove transições** entre as dez janelas, sem
exceção, e a rodada terminou ainda subindo. Isto é uma resposta parcial ao §86, não a resposta que o milestone
precisa: desacelerar adia o ENOSPC, parar é o que o evita. A monotonia das dez
janelas é o que `TestODiscoDoAceiteM4CresceEmTodaJanelaDeSessentaSegundos`
cobra.

O gate, esse, não é decoração: ele foi **aferido** contra a série que a ausência
de compaction produz, e `TestGateDeDiscoReprovaCrescimentoLinear` reprova o
crescimento linear puro. O que lhe falta é conferir **de onde vem cada ponto**
que ele lê — um extremo colhido fora da janela de carga entra na conta como
qualquer outro, e nenhum teste percebia. Está aberto como **débito 68**. Os dois
artefatos de aferição da Task 11 —
[`gate-disco-compaction-on-50k-20260822.json`](../benchmark/baselines/gate-disco-compaction-on-50k-20260822.json)
e
[`gate-disco-compaction-off-50k-20260822.json`](../benchmark/baselines/gate-disco-compaction-off-50k-20260822.json)
— são a mesma carga com a compaction ligada e desligada. E `NÃO AVALIADO`
reprova a rodada: gate que ninguém pôde avaliar não é gate aprovado, que é a
errata do M1.

## A calibração: o que ela custou e o que ela ensinou

Foram **cinco rodadas** com segmento de 64 MiB — as reprovadas são o dado, não o
rascunho —, e a tabela abaixo traz as cinco mais a aferição de 4 MiB da Task 11,
que entra como sexta linha justamente para contraste. **A coluna de taxa é
DUPLA de propósito**: a versão anterior desta tabela declarava
`calibracao-m4-1m-46k` a "46.000 ev/s", que é a taxa **alcançada**, enquanto a
linha do aceite declarava "14.000 ev/s", que é a **configurada**. Misturar as
duas na mesma coluna faz quem tentar reproduzir a linha de 46k com `-rate 46000`
rodar outra coisa: o `-rate` daquela rodada foi **50.000**.

| Artefato | Entidades | `-rate` | Alcançada | Segmento | Eventos de regime | Múltiplos do key set | Razão do gate |
|---|---:|---:|---:|---:|---:|---:|---:|
| [`aceite-m4-base-250k`](../benchmark/baselines/aceite-m4-base-250k-20260822.json) | 250.000 | 16.000 | 16.000 | 64 MiB | 959.994 | **3,8×** | −0,324 — **aprova** |
| [`calibracao-m4-2m-10k`](../benchmark/baselines/calibracao-m4-2m-10k-20260822.json) | 2.000.000 | 10.000 | 10.000 | 64 MiB | 7.999.994 | **4,0×** | 0,510 — **reprova** |
| [`calibracao-m4-2m-16k`](../benchmark/baselines/calibracao-m4-2m-16k-20260822.json) | 2.000.000 | 16.000 | 16.000 | 64 MiB | 8.319.988 | **4,2×** | 0,467 — **reprova** |
| [`calibracao-m4-1m-46k`](../benchmark/baselines/calibracao-m4-1m-46k-20260822.json) | 1.000.000 | **50.000** | 45.992 | 64 MiB | 5.519.040 | **5,5×** | 0,510 — **reprova** |
| [`aceite-m4-1m`](../benchmark/baselines/aceite-m4-1m-20260822.json) | 1.000.000 | 14.000 | 14.000 | 64 MiB | 8.400.001 | **8,4×** | 0,452 — **reprova** (0,339 publicado, ver acima) |
| [`gate-disco-compaction-on-50k`](../benchmark/baselines/gate-disco-compaction-on-50k-20260822.json) | 50.000 | 5.000 | 5.000 | **4 MiB** | 599.996 | **12,0×** | −0,664 — aprova |

A taxa alcançada do aceite é 14.000 ev/s porque a ingestão de fato durou os
600 s configurados; o `steady.events_per_sec` de 6.327 é o relógio contaminado
pelo bloqueio, e não a vazão.

**Errata: a regra causal que este documento publicava está errada.** A versão
anterior afirmava que a variável que decide é o **churn** — quantas vezes o key
set inteiro foi reescrito — e o sidecar da rodada chegava a escrever a
fronteira: "abaixo de ~6× o key set reescrito, reprova". O conjunto de artefatos
**deste mesmo commit** a desmente por duas pontas:

- a rodada de referência do §110, `aceite-m4-base-250k`, tem o **menor** churn
  de todos (3,8×) e o `disk_plateau` dela **passa**, com −0,324. É o
  contraexemplo direto da fronteira de ~6×, e está fixado em
  `TestOChurnNaoSeparaAprovacaoDeReprovacaoNosArtefatosDoM4`;
- a única outra linha que aprova, `gate-disco-compaction-on-50k`, roda com
  segmentos de **4 MiB** contra 64 MiB de todas as demais. O churn dela estava
  sendo comparado com o das outras como se a variável fosse uma só, quando as
  duas pontas diferem também em tamanho de segmento **e** em número de
  entidades (50 mil contra 1-2 milhões).

O que o conjunto de fato sustenta é mais modesto, e cabe em duas frases.
**Primeiro:** baixar a taxa de 50.000 para 10.000 — cinco vezes — não mexeu na
razão (0,510 nas duas pontas), então a taxa realmente não é a variável.
**Segundo:** entre as cinco rodadas de 64 MiB, subir o churn de 4,0× para 8,4×
melhorou a razão de 0,510 para 0,452 — melhorou, e **não** chegou ao teto de
0,35. Das cinco rodadas de 64 MiB, as **quatro** de 1 a 2 milhões de entidades
reprovam; a única que aprova é a de **250 mil**, cuja janela inteira cabe em
**três** segmentos (`store_stats.segments_total = 3`, contra 13 a 21 das
outras). A outra aprovação do conjunto é a de segmento de **4 MiB**. As duas são
janelas em que **uma única aposentadoria domina a curva** — e é por isso que
nenhuma das duas responde a pergunta do §86.

Qual é a variável que decide continua **em aberto**, e o débito 65 foi reescrito
para dizer isso em vez de afirmar a fronteira que não se sustenta. Fechar essa
pergunta pede rodadas novas com o tamanho de segmento e o número de entidades
variados **separadamente** — que é experimento, não documentação, e não entra
neste commit.

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
sobre 600 s de ingestão. **Contamina os quatro campos ditos acima E o gate**:
a amostra colhida no fim do bloqueio é o extremo que o `disk_plateau` lê, e é
ela que inverteu o veredito. Registrado como **débito 67** — e o que o gate
faz com ela, como **débito 68**.

> **Errata:** este parágrafo dizia "não contamina nada que o gate lê". Era a
> afirmação mais custosa do documento, porque é ela que sustentava o aceite
> inteiro, e ela era falsa. Vale registrar como o erro se produziu, porque a
> forma se repete: a checagem feita foi "existe amostra dentro do bloqueio?", a
> resposta foi "não", e a conclusão tirada foi "então a série está limpa". A
> pergunta certa era outra — "de onde vem o ponto que o gate usa como extremo?"
> —, e ela tem resposta oposta. É a mesma família do débito 6 e da errata do M1:
> instrumento que responde a pergunta que lhe foi feita, sobre um recorte que
> ninguém conferiu.

**A compaction não avisa quando não dá conta.** Nas **quatro** rodadas de 64 MiB
que reprovam o plateau — a de aceite inclusive, uma vez recontada — o disco
cresceu em regime com a compaction viva, e nenhuma série do §92 diz isso:
`compaction_running` responde 1, os contadores de bytes sobem, e o painel fica
idêntico ao de uma rodada saudável. Faz parte do **débito 65**.

**O artefato não traz série de compaction, só o snapshot do fim.** A struct
`Sample` carrega disco, RSS, heap, objetos e GC, e **nenhum campo de
compaction**; `store_stats` é lido uma vez, ao final. Por isso este documento
não afirma mais "`compaction_running=true` do início ao fim" — o que está
provado é o valor **no fim**, com 6,98 GB lidos, e o resto seria afirmação sem
instrumento. Faz parte do **débito 68**.

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

# a linha de 46k da tabela de calibração, que é `-rate 50000` e NÃO `-rate 46000`:
# 46.000 ev/s é o que a máquina alcançou, não o que foi pedido
GOMEMLIMIT=512MiB GOGC=100 ./ingest-bench -data /var/tmp/nb-m4-r1 \
    -entities 1000000 -steady 120s -rate 50000 -segment-size 67108864 \
    -sql-queries 0 -kafka 127.0.0.1:19092 -kafka-topic nishidb-m4-r1 \
    -kafka-group nishidb-m4-r1 -heap-baseline base-250k.json \
    -report calibracao-m4-1m-46k.json
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
| **O plateau de disco em 1M de entidades com segmento de 64 MiB** | **Não provado.** A compaction devolve disco e desacelera o crescimento, mas as **quatro** rodadas de 64 MiB com 1 a 2 milhões de entidades reprovam o `disk_plateau` com o extremo da série honesto (0,452 a 0,510). A única aprovação de 64 MiB é a de 250 mil entidades, com **três** segmentos na janela. Falta o experimento que separa tamanho de segmento de número de entidades — débito 65 |
| O gate ler de onde vem cada amostra | Débito 68. `avaliaGateDeDisco` usa `serie[len-1]` como extremo sem conferir se aquela amostra foi colhida sob carga |
| `POST /v1/compact` e `nishidb compact`/`checkpoint` | M5, atrás do papel ADMIN. Rotas **mutantes** não existem neste milestone (decisão D3 do roadmap) |
| Recuperação de tombstone | Record tombstoned é **copiado para frente** (decisão D2): a location aponta para ele. Limpar location e primary é do TTL, no M6 |
| `Stats`/`Sequence` com `Store` fechado (débito 34) | **Continua aberto** — o M4 passou sem tocar nisso, e o registro está na errata de [`debitos.md`](debitos.md). Muda a assinatura do `Store`, e portanto todo chamador |
| Posting de bitmap órfã (débito 45) | **Continua aberto**, e a compaction **não** a fecha: o copy-forward chama o `applyRecord` com o payload igual ao anterior, então nenhum bit é limpo. Quem fecha é o rebuild de índice (débito 22) |
| Aviso de compaction atrasada | Débito 65 |
| `storage.durability` (§47) | Débito 50, intocado |
| Autenticação e TLS | Fase 11, no M5. Quem alcança a porta continua lendo o dataset |
