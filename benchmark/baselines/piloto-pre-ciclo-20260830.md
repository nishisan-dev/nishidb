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
kafka.Writer.Roda → aplica → materializer.Aplica → apendaNoHistorico
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
