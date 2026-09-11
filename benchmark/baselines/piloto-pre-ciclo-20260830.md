# Piloto `events` — medição pré-ciclo, 30/08/2026

Rodada de referência do estado ANTERIOR ao subsistema 1, tirada do piloto real
(host `10.200.20.218`) com o binário que ainda tem o `internal/index/hash`
global. Registro de rodada: não é reescrito.

## Estado no momento da medida

| Grandeza | Valor |
|---|---|
| Linhas no histórico | 46.976.204 |
| Dataset em disco | 135 GiB |
| `nishidb_history_index_bytes` | 3.716.259.840 (3,46 GiB) |
| RSS do processo | 2.688 MiB |
| Uptime do processo | 18 h 13 min |
| Profundidade da fila do Kafka | 5.000 — **no teto** |
| `nishidb_kafka_poll_blocked_seconds_total` | 28.145 s (7 h 49 min) |
| **Eventos ingeridos em 30 s** | **0** |
| CPU em 30 s | 30,4 s — um núcleo saturado |

A ingestão está **parada**: um núcleo inteiro consumido, fila no teto, e nenhum
evento comprometido na janela inteira da medida.

## Onde o CPU está

`pprof` de 30 s, 95,76% de amostras aproveitadas
(`piloto-pre-ciclo-20260830.pprof`). Uma única pilha responde por 99,16%:

```
kafka.Writer.Roda → aplica → materializer.Aplica → apendaInHistory
  → storage.Store.Apenda → applyRecord → applyHashSets → setHash
    → hash.Index.Set → talvezReconstrua → hash.Index.reconstroi
```

| flat | símbolo |
|---|---|
| 64,39% | `sync/atomic.(*Int32).Add` |
| 19,67% | `mmap.(*Region).AtomicLoad64` |
| 7,59% | `mmap.(*Region).word` |
| 4,70% | `hash.(*Index).reconstroi` |

Cumulativos: `sync.RWMutex.RLock` 33,73%, `RUnlock` 32,09%.

## O mecanismo

`talvezReconstrua` dispara quando `(usados+lápides+1)` ultrapassa o fator de
carga. `reconstroi` **dobra** a tabela e varre `oldSlots` inteiros fazendo um
`AtomicLoad64` por slot. Cada `AtomicLoad64` toma o mutex por slot do
`mmap/region.go`, e um `RWMutex.RLock` é um `atomic.Int32.Add` na mesma linha de
cache — daí os 64% num único `Add`.

Com o índice em 3,46 GiB, dobrar significa varrer centenas de milhões de slots,
cada um sob mutex contendido. E `reconstroi` corre **debaixo do `wmu`**, que é o
lock que serializa toda escrita: enquanto o rehash não termina, nada entra.

Não é o custo de sondar o índice. É o custo de **redimensioná-lo**, e ele cresce
com o tamanho total da tabela, não com o trabalho do evento que por acaso
disparou o limiar.

## Por que isto é a justificativa do ciclo

Um índice por segmento selado é escrito uma vez, ordenado, dimensionado exato, e
**nunca cresce**. Não existe fator de carga, não existe lápide, não existe
tabela para dobrar — logo não existe `reconstroi`. O modo de falha desta página
não fica mais raro no desenho novo: fica impossível de exprimir.

## Adendo — 06:43, dezoito minutos depois

Segunda leitura dos mesmos contadores, sem tocar no processo:

| Contador | 06:25 | 06:43 | Δ |
|---|---|---|---|
| `nishidb_ingest_events_total` | 46.976.204 | 46.976.204 | **0** |
| `nishidb_kafka_queue_depth` | 5.000 | 5.000 | 0 (no teto) |
| `nishidb_kafka_poll_blocked_seconds_total` | 28.145,7 | 29.225,8 | **+1.080,1 s** |

O poll bloqueado cresceu 1.080 s em 1.080 s de relógio: bloqueado **100% do
tempo**. E nenhum evento comprometido no intervalo inteiro.

Isto separa as duas hipóteses que a primeira medida deixava em aberto. Não é um
rehash lento que ainda vai terminar: é uma parada que já dura pelo menos dezoito
minutos com um núcleo saturado. O trabalho que o processo faz não avança o
contador de ingestão em nenhuma unidade.

## Correção — 07:36: era congelamento longo, não parada definitiva

O adendo acima concluiu "é parada, não lentidão". **A conclusão estava errada**,
e a medida seguinte a desmente: às 07:36 o processo voltou a ingerir.

| Instante | `ingest_events_total` | Leitura |
|---|---|---|
| 06:25 | 46.976.204 | fila no teto, 0 ev/s |
| 06:43 | 46.976.204 | 18 min sem um único evento |
| ~07:35 | 48.356.182 | destravou em algum ponto do intervalo |
| 07:36 | 48.400.172 | **733 ev/s**, 173% de um núcleo, RSS 2.812 MiB |

O que os dados sustentam é mais estreito do que eu escrevi, e mais útil: o
rehash **termina**. Ele congelou a ingestão por algo entre dezoito e cinquenta e
dois minutos e depois liberou. Não é um travamento permanente; é uma parada
longa e recorrente.

Recorrente é a palavra que importa. O gatilho é o fator de carga, e a resposta é
**dobrar** a tabela — então cada congelamento seguinte percorre o dobro de slots
do anterior. O intervalo entre eles também dobra, o que faz o sintoma parecer
intermitente e "resolvido" entre um episódio e outro, exatamente o padrão que
leva a diagnosticar problema de rede ou do broker.

O segundo número desta medida diz o resto: **733 ev/s** contra os 1.636 ev/s
medidos em 29/08 com o dataset menor. Menos da metade, com o mesmo binário e o
mesmo tópico. O custo por evento cresce com o tamanho acumulado do dataset mesmo
fora dos episódios de rehash.

Fica registrado assim, com o erro à vista, porque o adendo anterior tirou de
dezoito minutos de observação uma conclusão que exigia mais tempo do que eu
tinha olhado.
