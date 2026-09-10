# Ingestão: folga de capacidade com o history ligado

## Context

Com a tabela histórica ligada, o piloto `events` passou a exibir lag crescente, dando a
impressão de que a ingestão não acompanha a produção em tempo real. Investiguei o piloto
em produção (release `v0.6.0-160-g8caad2c`, host `10.200.20.218`) com profile de CPU,
dumps de goroutine e amostragem de métricas a 1 Hz. O que encontrei muda o enunciado do
problema em dois pontos, e confirma um terceiro.

### 1. Boa parte do "lag que cresce" é a métrica, não a ingestão

`nishidb_kafka_lag` é atualizada no scrape por `AtualizaLag`
([consumer.go:623](internal/kafka/consumer.go:623)), que pergunta ao broker o lag do
**offset commitado no consumer group**. O commit só acontece **depois do checkpoint
durável** ([writer.go:300-324](internal/kafka/writer.go:300)) — a cada 30 s no piloto.
Entre um checkpoint e outro o offset fica congelado e a série sobe exatamente à taxa de
produção, mesmo com o consumidor perfeitamente em dia.

Medido: o lag exibido sobe até **14.167** e cai para **515** num único segundo em que
apenas **478** eventos foram consumidos. O crescimento entre commits é de **450 ev/s** —
idêntico aos 452 ev/s de produção documentados. O lag real, lido logo após cada commit,
fica entre **515 e 994 mensagens** (~1–2 s de atraso), sem tendência de crescimento ao
longo de 181 s de observação.

### 2. A folga real é de ~3×, e um core é o teto

Medição de 60 s via `/proc/<pid>/stat` cruzada com `nishidb_ingest_events_total`:

| Grandeza | Medido |
|---|---:|
| CPU por evento | **902 µs** |
| Consumo | 372–430 ev/s |
| CPU do processo | **0,336 de um core** (o host tem 32) |
| Teto com um core saturado | **~1.109 ev/s** |

Todo o pipeline — poll, parse, merge, `Put`, `Apenda` — roda numa **goroutine única**,
serializada por um mutex por dataset. O produto não fica sem CPU: fica sem paralelismo.
A meta de 2.000 ev/s está **acima** do que um core entrega, então ela não é alcançável
sem quebrar essa serialização.

### 3. Onde vão os 902 µs

Profile de CPU de 45 s em produção (13,64 s de amostras):

| Componente | % do CPU | µs/evento |
|---|---:|---:|
| `PatchBuffer.Parse` (parse JSON + descoberta) | 33% | 298 |
| `Store.ExpiraUmaPassada` (varredura de TTL, fundo) | **19%** | 171 |
| `Store.Put` (tabela base) | 16% | 144 |
| `apendaNoHistorico` → `Store.Apenda` | **11%** | 99 |
| Kafka, métricas, GC, resto | 21% | 190 |

O history custa **~17% a mais no caminho de ingestão** — real, mas não é o dominante. O
custo dele que realmente pesa é **disco**.

Medi o custo em disco em **regime estável** (120 s, 46.800 linhas), o que fecha uma
pendência que o próprio [docs/piloto-events.md](docs/piloto-events.md) lista em aberto — o
número publicado hoje foi colhido durante o replay do backlog:

| | Replay (documentado) | **Regime estável (medido agora)** |
|---|---:|---:|
| Bytes por linha | 2.767 | **2.394** |
| Janela real em 250 GiB | ~2,5 dias | **~3,3 dias** |
| Escrita do history | — | **80,6 GB/dia** |

Na mesma janela o dataset base **encolheu 19,8 MB** — a compaction está ganhando do
crescimento, o que é um dado favorável contra a preocupação registrada no débito 65.
O `expireAfterWrite: 30d` do YAML continua decorativo, e a documentação precisa ser
atualizada com o número de regime.

A varredura de TTL é a segunda maior fatia e é **inteiramente do dataset base**: a tabela
histórica não roda expiração por entidade, e o storage recusa que ela rode
([store.go:2510](internal/storage/store.go:2510) — *"ModoHistorico não tem expiração por
inatividade; a retenção é por tempo de escrita e por tamanho"*). Confirmado no piloto:
`expiry_scanned_total{events_history} = 0` contra `24.188.814` no base. O que dispara a
cadência alta é a regra condicional `type = 2 → 3m` do base, que obriga o coletor a dar
uma volta completa nas 4,57M linhas a cada ~2,5 minutos para achar as ~0,4% vencidas.

### 4. O documento é varrido 25 vezes por evento

Dentro dos 33% do parse há uma redundância pura, que não é trade-off de nada.
`extraiDerivadas` ([extract.go:66](internal/materializer/extract.go:66)) chama
`buscaCaminho` **uma vez por chave declarada em `extract`**, e `buscaCaminho`
([extract.go:158-201](internal/materializer/extract.go:158)) faz `Reset(doc)` e varre o
documento **inteiro até o fim** — não pode parar cedo porque implementa *last-wins* dentro
do documento.

O piloto declara **25 paths em `extract`** no campo `persistent`, todos de nível 1 e todos
sob o mesmo pai. O documento é varrido 25 vezes por evento. O profile confirma:

```
buscaCaminho           2,94s   21,55% cum   ← 100% chamado por extraiDerivadas
  └─ jsonp.Parser.Next 2,75s   93,54%       ← é tudo re-parse
```

São **194 µs dos 902 µs por evento** — quase o dobro do custo inteiro da tabela histórica.

A decomposição do `Parse` mostra que o rótulo "parse custa 33%" é enganoso: o parse de
verdade custa 1,76%, e dois terços do bloco são a mesma varredura repetida.

| Dentro de `PatchBuffer.Parse` (32,55% do CPU) | % do total |
|---|---:|
| `buscaCaminho` — as 25 varreduras redundantes | **21,55%** |
| `SortStableFunc` das entries do patch | 3,23% |
| `Convert` | 2,27% |
| `descobreAninhados` | 1,83% |
| **Parse real do evento** (`jsonp.Parser.Next`) | **1,76%** |

### Sintomas secundários confirmados

- **A barreira de fim de lote elimina o overlap.** O poller bloqueia a cada lote até o
  writer drenar tudo ([consumer.go:566-579](internal/kafka/consumer.go:566)).
  Amostragem de 60 dumps de goroutine: o poller passa 28% do tempo parado nessa barreira,
  e o writer passa 72% ocioso em `Fila.recebeOuAcorda`. A fila entre os dois
  (`nishidb_kafka_queue_depth`) fica quase sempre em 0.
- **Stalls reais de até 2,4 s** com a ingestão totalmente parada, fora de checkpoint.
- Checkpoint segura o mutex do writer do início ao fim, incluindo todos os `msync`.

### Resultado pretendido e modelo de capacidade

Sustentar **~2.000 ev/s por dataset com o history ligado**, usando 3–4 cores em vez de
0,3; devolver os ~19% de CPU gastos em varredura de TTL; e fazer o painel dizer a verdade,
para que o ganho seja demonstrável e a próxima regressão seja vista.

Projeção a partir dos 902 µs/evento medidos, aplicando as frentes em ordem:

| Estado | µs CPU/evento | Teto com 1 core | Ganho |
|---|---:|---:|---|
| Hoje | 902 | ~1.109 ev/s | — |
| + passada única no `extract` | ~717 | ~1.395 ev/s | +26% |
| + índice de prazos no TTL | ~547 | ~1.828 ev/s | +65% |
| + paralelismo (3–4 cores efetivos) | ~547 | **5.000+ ev/s** | +350% |

As duas primeiras linhas são otimização local, sem mudança de contrato nem de
concorrência, e sozinhas já chegam perto da meta. A terceira é a que dá folga de verdade —
e é a razão pela qual as duas primeiras vêm antes: paralelizar trabalho redundante
multiplicaria o desperdício por N workers em vez de eliminá-lo.

---

## Fase 0 — Bloqueadores: um bug de corretude e a medição honesta

Nada depois disto é avaliável, e o primeiro item não é performance — é um processo que pode
morrer.

### 0.1 Corrida de dados em `Writer.checkpoint` (bug, verificado)

`checkpoint` ([writer.go:307-330](internal/kafka/writer.go:307)) lê `w.offsets` (mapa) e
escreve `w.desdeCP`/`w.ultimoCP` **sem lock** — `w.mu` protege apenas `desfecho` e `pausa`.
`avanca` ([writer.go:265-269](internal/kafka/writer.go:265)) escreve no mesmo mapa, também
sem lock. E `checkpoint` é chamado de **duas goroutines**:

- a do writer, via `talvezCheckpoint` — inclusive acordado pelo ticker do despertador
  ([writer.go:150-158](internal/kafka/writer.go:150));
- a do group manager do franz-go, via `aoRevogar` →
  [consumer.go:424](internal/kafka/consumer.go:424).

A barreira de fim de lote esvazia a **fila**, não para a **goroutine** do writer — o
comentário de `aoRevogar` descreve corretamente a fila e conclui erradamente sobre a
goroutine. Leitura e escrita concorrentes de mapa em Go é `fatal error: concurrent map read
and map write`: um throw não recuperável que **derruba o processo**, não apenas um
apontamento do `-race`.

A janela é estreita (exige coincidir revogação com o tique de checkpoint), mas revogações
acontecem em rebalance, mudança de liderança e timeout de sessão.

**Procurei no journal do piloto e não há evidência de que já tenha ocorrido:** zero
ocorrências de `fatal error`, `concurrent map` ou `panic:` em todo o journal retido. Então a
corrida está confirmada **por leitura de código**, não por manifestação — e **não fecha o
débito 90**. O único desligamento com status 2 registrado (27/08 07:53) coincide com a
janela em que a flag `--pprof` entrou na unit, e `INVALIDARGUMENT` aponta para erro de
argumento na subida, não para throw do runtime.

**Decidido: correção imediata em branch própria**, antes de qualquer fase deste plano —
`w.mu`, que já existe e já protege `desfecho`/`pausa`, passa a cobrir também
`offsets`/`desdeCP`/`ultimoCP`, com teste de revogação concorrente sob `-race`. A Fase 3
depois substitui o lock pelo `Quiesce`, que elimina a corrida por construção em vez de por
exclusão mútua.

### 0.2 `nishidb_kafka_lag` passa a medir a posição real

Trocar `kadm.Lag` (três requisições: `DescribeGroups` → `FetchOffsets` → `ListEndOffsets`)
por **`ListEndOffsets`** — uma requisição. O custo cai a um terço do que produção já paga,
então não há trade-off a negociar. O lag vira `max(0, highWatermark − offset aplicado)`,
usando a posição que `avanca` já publica em `nishidb_kafka_offset`.

Três decisões que evitam repetir defeitos já conhecidos:

- **Não usar o HWM da resposta de fetch**, embora venha de graça: o franz-go descarta fetch
  vazio, então o valor congelaria exatamente quando o poller trava — o mesmo defeito que o
  commit `cc8aaeb` acabou de corrigir na profundidade de fila, com outro nome.
- **Emitir `offset`, `high_watermark` e `lag` do mesmo coletor**, para que
  `lag == hwm − offset` seja invariante dentro de uma raspagem. Hoje isso não vale, e
  `scripts/nishidb-stats.py` chegou a imprimir `produz -310/s` no piloto por somar parcelas
  de instantes diferentes.
- **Sob falha do broker, omitir a série — nunca congelar, nunca zerar.** É a doutrina já
  escrita em `internal/kafka/metrics.go:44-58` e travada por teste. `absent()` é um alerta
  que o operador consegue escrever; zero é uma medição falsa.

**O watchdog tem a mesma mentira dentro dele.** `lagPendente`
([watchdog.go:172-194](internal/kafka/watchdog.go:172)) usa o mesmo lag commitado. Com
`watchdogTimeout` no piso de 10 s e checkpoint de 30 s, existe janela real em que o tópico
silencia, os eventos param, o commit ainda não saiu e o broker reporta lag > 0 sobre um
consumidor em dia — com fila vazia, que é exatamente o veredito `vereditoRecria`. Isso é
**recriação espúria de consumidor saudável**. Reescrever `lagPendente` sobre a mesma
primitiva fecha isso por construção.

**Risco a tratar no mesmo commit:** alertas calibrados no número inflado param de disparar
(o lag cai de ~14.000 para ~515), e `scripts/nishidb-update.sh` usa "o lag caiu" como sinal
de vida — com o lag real ele fica pequeno e constante, e pode não cair. O disjunto
`nishidb_ingest_events_total` avançou continua valendo, mas o gate
`internal/gates/rollback_update_test.go` precisa passar a afirmar isso explicitamente.

### 0.3 A bancada mente — débitos 67, 6 e 68

Sem isto, nenhum número de laboratório vale:

- **67 (o `ingest-bench` trava para sempre).** `fonteKafka.Next` bloqueia em
  `fila.Recebe(context.Background())` — contexto sem prazo — e a checagem de duração roda
  *antes* do `Next`, nunca sendo reavaliada. A fase de aceite do M4 publicou
  `duration_sec` de **1.327,6 s para 600 s** de ingestão. Correção: a interface `fonte`
  passa a receber `ctx`, e `PhaseReport` ganha `StopReason`
  (`duration | events | source_drained`) — hoje o relatório não consegue dizer se
  `events_per_sec` é vazão ou fim de corpus.
- **6 (a bancada não confere resultado).** Um bug que fizesse toda query devolver 0 linhas
  passa em todos os gates **com p95 melhor** — e otimização de performance é exatamente o
  tipo de mudança que produz esse bug. Cada rota SQL declara a contagem esperada (point
  lookup = 1, `LIMIT 100` = 100, predicado que não casa = 0), com gate duro. A prova é por
  mutação: fazer o executor devolver 0 linhas e confirmar que a rodada fica **vermelha**.
- **68 (gate de disco lê o extremo sem perguntar de onde veio).** `Sample` ganha campos de
  compaction e `EventsDelta`; amostra fora da janela de carga é descartada; sobrando menos
  que o mínimo, o gate vai a NÃO AVALIADO — que reprova e manda repetir. Testar contra os
  artefatos **já versionados** `gate-disco-compaction-{on,off}-50k-20260822.json`, que são
  registro de execução e não podem ser regerados.
- **52** vem junto de 0.2, senão o projeto passa a ter duas definições de lag outra vez.

### 0.4 Instrumentação nova

- **`nishidb_ingest_apply_seconds`** — histograma, medindo **só** `mat.Apply` dentro de
  `Writer.aplica` ([writer.go:194](internal/kafka/writer.go:194)). Ali, e não dentro do
  `Materializer`, porque os gates de alocação e o `BenchmarkApplyIngestReal` medem o
  `Materializer.Apply` — instrumentar por dentro mudaria o baseline do próprio instrumento
  com que a otimização será julgada. Buckets próprios a partir de 5 µs (o `DefaultBuckets`
  começa em 50 µs, grosseiro para uma mediana de ~8,6 µs) e topo em 1 s, que é o que torna
  os **stalls de 2,4 s** atribuíveis a `Apply` em vez de a "algo".
- **`nishidb_kafka_poll_blocked_seconds_total`** e **`nishidb_kafka_writer_idle_seconds_total`** —
  contadores de segundos acumulados, com tentativa não bloqueante primeiro para não pagar
  `time.Now()` no caminho rápido. São eles que separam "saturado" de "sem produção", que
  hoje é impossível: `queue_depth` é amostra instantânea e fica em 0 nos dois casos.

  | `rate(poll_blocked)` | `rate(writer_idle)` | lag | Leitura |
  |---|---|---|---|
  | ≈1 | ≈0 | subindo | Saturado — o writer é o gargalo |
  | ≈0 | ≈1 | ≈0 | Sem produção — o tópico está calado |
  | ≈0 | ≈1 | subindo | Poller não entrega — é o incidente de 27/08 |

- **Alocação por evento no writer (verificada).** `avanca` chama `w.met.offset(p)` →
  `famOffset.With(dataset, strconv.Itoa(p))` → `chave(valores)` =
  `strings.Join(valores, "\xff")` ([metrics.go:197](internal/metrics/metrics.go:197)) —
  **string nova a cada evento**, mais o slice variádico. Nenhum gate cobre esse trecho: os
  de alocação medem `PatchBuffer.Parse` e `Materializer.Apply`, e o writer Kafka está fora
  dos dois. Trocar por um mapa imutável `partição → *atomic.Int64` publicado por
  `atomic.Pointer`, e travar com gate próprio.
- **Ligar `block` e `mutex` profile por flag.** O piloto expõe `/debug/pprof/block` e
  `/debug/pprof/mutex`, mas ambos vêm com `sampling period=0` — nunca são habilitados, e
  portanto são inúteis para diagnosticar contenção de lock, que é o problema deste plano.
- **Três séries órfãs decididas**, no formato que `nishidb_storage_live_bytes` já usa
  (omissão deliberada + teste que trava a omissão): `nishidb_ingest_events_rate` fica sem
  alimentador por decisão (taxa derivada no exportador tem janela que o operador não
  escolheu, e discordaria do contador ao lado); `nishidb_materialized_tombstones` vira
  débito nomeado, porque alimentá-la honestamente exige contador de lápides vivas
  reconstruível no replay — trabalho de milestone, não encanamento de métrica.

> **A Fase 0.4 precisa estar em produção e raspada por pelo menos um dia inteiro antes da
> primeira otimização.** Sem isso não existe "antes" contra o qual comparar.

---

## Fase 1 — Uma passada só sobre o documento aninhado

A maior economia do plano, e a mais barata: é trabalho redundante, não trade-off.
Nenhum contrato muda, nenhuma concorrência é tocada.

### Estado atual

`extraiDerivadas` ([extract.go:42](internal/materializer/extract.go:42)) faz, por pai visto:

1. `descobreAninhados(p)` ([extract.go:221](internal/materializer/extract.go:221)) — **uma**
   varredura completa, com parser próprio (`b.descobridor`), que para cada chave chama
   `declarado(p.campo, f.Key)` e **pula** as declaradas;
2. o laço `for _, d := range p.campo.DerivedColumns` ([extract.go:65-66](internal/materializer/extract.go:65)) —
   **mais 25** varreduras completas, uma por path, todas até o fim do documento.

São **26 varreduras** do mesmo documento por evento. E `declarado`
([extract.go:379-386](internal/materializer/extract.go:379)) é busca linear sobre as 25
derivadas, para cada chave.

### To-be

Uma varredura só. A estrutura para isso já existe — `descobreAninhados` já visita cada
chave e já pergunta se ela é declarada. Em vez de pular a declarada, **resolvê-la ali
mesmo**:

- para cada chave do documento, um lookup no índice `path → *CompiledField`, pré-construído
  no `Compile` ao lado de `pai.DerivedColumns` ([compile.go:143](internal/schema/compile.go:143),
  populado em [compile.go:846](internal/schema/compile.go:846)) — não por evento, e
  substituindo a busca linear de `declarado`;
- chave declarada → `Convert` + entry, como hoje;
- chave não declarada e `DiscoverNested` ligado → caminho atual de descoberta;
- *last-wins* dentro do documento sai de graça: a passada é sequencial e a última
  ocorrência sobrescreve, que é exatamente o que o laço de
  [extract.go:184-190](internal/materializer/extract.go:184) faz hoje à mão.

**Dois pontos que exigem cuidado:**

1. **O buffer `b.folha`.** Hoje é único e reusado porque só há uma busca viva por vez — o
   godoc do `jsonp` avisa que `Field.Raw` só vale até o `Next` seguinte
   ([extract.go:186-188](internal/materializer/extract.go:186)). Com N derivadas resolvidas
   na mesma passada, N valores ficam vivos ao mesmo tempo. Resolver com arena (o
   `PatchBuffer` já tem uma) e não com buffer por derivada, para não realocar por evento.
2. **Caminhos multinível.** O piloto só tem nível 1, mas o produto aceita `a.b.c`
   ([extract.go:200](internal/materializer/extract.go:200) recursa). Agrupar por primeiro
   segmento e recursar **uma vez por subdocumento distinto**, não uma vez por path.

### Verificação

- O gate de zero alocação (`internal/materializer/alloc_gate_test.go:54,72`) não pode
  regredir — o índice é construído no `Compile`, não no hot path.
- Ordem de resolução muda (ordem de aparição no documento, não de FieldID). A ordem final
  é preservada pelo `SortStableFunc` + `dedupLastWins`
  ([patch.go:250-259](internal/materializer/patch.go:250)), mas precisa de teste explícito.
- `d.CountOccurrence()` ([extract.go:71](internal/materializer/extract.go:71)) conta quando
  a chave **aparece**, antes da conversão — semântica a preservar, com teste.
- `BenchmarkParseComDescoberta` e `BenchmarkPatchParseRadiusEvent`
  (`internal/materializer/patch_bench_test.go`) com baseline em `benchmark/baselines/`,
  usando um corpus com muitos paths declarados — o corpus atual não expõe o defeito.
- Teste de regressão que trave a contagem de varreduras: com N paths declarados, o parser
  do documento deve ser reiniciado **uma** vez, não N.

**Ganho esperado:** 21,55% → ~1,8% do CPU. De 902 para ~717 µs/evento; teto de um core de
1.109 para ~1.395 ev/s.

---

## Fase 2 — Agenda de vencimentos, no lugar da varredura

### A estrutura

**Min-tree B-ária sobre um array denso de vencimentos em mmap.** O que decide contra as
alternativas é o requisito que aparece em *todo evento*: **atualizar** o prazo de uma
entidade. Só esta família dá atualização `O(1)` esperada, separando o valor (denso,
endereçado por `rowID`, escrito no lugar) da ordenação (sumário hierárquico de mínimos, que
quase nunca precisa ser recomputado).

Descartadas, com o motivo: **heap persistente** (sift de `O(log n)` = ~22 escritas
aleatórias por evento, e crash no meio quebra a invariante de forma não detectável);
**timing wheel em memória** (não é durável, e um nó por entidade custaria >250 MB de heap
nas dezenas de milhões — contra a tensão central do projeto, que é manter o peso em mmap);
**buckets por janela de tempo** (a mais tentadora, morre no espaço: com `expireAfterWrite:
90d` e renovação a cada evento, cada escrita insere uma entrada que vive 90 dias);
**runs LSM ordenados** (resolve o espaço ao custo de uma segunda máquina de compactação ao
lado da que já existe).

Layout: **um `denseFile` por nível**, `B = 64`. Um arquivo por nível — e não um arquivo
contíguo — porque assim crescer as folhas **não recomputa nada**: o bloco novo nasce com
sumário `0`, que é lido como `∞`, o valor certo para linhas que ainda não existem. A 4,57M
linhas são 4 níveis e ~37 MiB de folhas; o sumário inteiro é `n/63` entradas.

**A invariante é a corretude inteira:** `nivel_i[b] ≤ min{efetivo(filhos)}` em todo
instante, para um leitor sem lock. O nó é limite **inferior**, nunca superior — falso
positivo custa uma descida desperdiçada (e `Expira` já revalida sob `wmu`), falso negativo
significa que **a linha nunca é recuperada**. Isso fixa uma ordem de escrita assimétrica:
abaixando, escreve os ancestrais de cima para baixo e a folha **por último**; subindo,
escreve a folha primeiro e recomputa de baixo para cima, só onde a linha *era* o mínimo.
Custo esperado: ~1,02 recomputes por evento.

### O que isso apaga

A varredura some: o coletor pergunta `NextExpiry()` e **dorme exatamente até lá**,
em vez de acordar a cada minuto e varrer 4,57M linhas. O `locs.Get`, que é 39% do custo da
passada de hoje, sai do laço. E o pré-filtro de tombstone vira código morto — a linha
tombstonada recebe o sentinela "nunca" e sai da agenda de vez.

### Migração: construir na abertura, sem exigir recriação

**Isto não é o débito 22, e a distinção precisa ficar escrita.** O débito 22 é sobre bitmap
e hash — índices que mapeiam um valor *extraído do payload*, cuja reconstrução exige ler
todo record dos segmentos. A agenda deriva de `expiry.idx` e `prazos.idx`, que **já estão
populados**, com um `u64` por linha, lidos sequencialmente em mmap: zero records lidos, zero
dicionário tocado. É a mesma classe de operação que o `hash.Index` já faz ao recontar no
Open, e que `primary.grow()` já faz ao reconstruir por rehash. **O piloto não precisa da
janela de 18,8 h.**

A decisão adotar × reconstruir fica toda gravada numa TLV **crítica** no manifest
(`duracaoDeFundoMs | blockShift | niveis`) — crítica porque um binário que a ignorasse
manteria uma agenda derivada de uma duração de fundo que já não honra, e linhas **nunca
venceriam, em silêncio**. Efeito colateral de honestidade: mudar a duração de fundo hoje
move em silêncio o vencimento de toda linha com prazo desconhecido; com a agenda vira um
rebuild explícito, logado e com custo medido.

**Mudança de comportamento a declarar:** hoje a carência das linhas legadas escorre ao longo
de várias passadas; com a agenda, todas afloram de uma vez na primeira. O freio é o
`MaxPorPassada` que já existe mais o `daCarencia` em lote. Medir numa cópia do dataset do
piloto antes do rollout.

### Redução da disputa pelo `wmu`

**Fase zero obrigatória: instrumentar antes de otimizar.** Não existe hoje nenhuma
instrumentação por dono do `wmu`, e os stalls de 2,4 s que medi **não têm atribuição**. A
lição do débito 65 é exatamente essa — quatro rodadas otimizando sem a medida discriminante,
e "qual é a variável que decide continua em aberto". Primeira tarefa:
`nishidb_writer_lock_held_seconds{holder="put"|"checkpoint"|"expira"|"carencia"|"compaction"|"apenda"}`.

**Dois suspeitos que estavam fora da lista, um deles quantificado no piloto:**

- **`primary.grow()`** — rehash da tabela inteira para um arquivo novo, mais o `Msync` dele,
  disparado de dentro do `Put`, **sob `wmu`**. Medi no piloto: `primary.idx` está em
  **exatamente 128 MiB** (2²³ slots × 16 B), com 4.570.919 linhas — **54,5% de ocupação**. O
  rehash dispara aos 70%, ou seja, a ~1,3 milhão de entidades daqui. Quando disparar, aloca
  268 MiB, reescreve todo slot vivo e msynca, com a ingestão parada. É um stall futuro
  previsível e grande. **Não é a causa do stall de 2,4 s que observei** — o arquivo não
  cresceu durante a medição —, mas é a próxima parada longa a caminho.
- **`mmap.Region.Grow`** — `fallocate` do dobro com alocação real, sob `wmu`, dentro do
  `denseFile.store`. `expiry.idx`, `prazos.idx` e `sequences.idx` estão todos em 48 MiB e
  dobram em ~6,29M linhas.

**Checkpoint:** os msync **não precisam do `wmu`**, e é neles que moram os 0,5–1,5 s. O §6.1
("página de índice nunca referencia record não durável") vale só para *locations*, e essas
estão protegidas pelo overlay, que é heap. `escritas`/`prazos`/`vencimentos`/`sequences`
guardam valores, não referências; postings de bitmap e hash podem ser superconjunto (§44).
Reorganizar em quatro fases, com um `cpMu` novo tomado **só** pelo checkpoint e **sempre
antes** do `wmu` — a ordem vira `vida → cpMu → wmu → aposMu`, ainda total e sem ciclo,
porque nada sob o `wmu` toma o `cpMu`. Isso entra no godoc normativo do `aposMu`, não num
comentário solto.

Dois pontos que a suíte vai pegar e que precisam ser tratados de propósito: o teste de ordem
de durabilidade fixa a *sequência* dos passos, e é preciso um evento de trace novo
(`wmu.solto`) para poder afirmar que o passo do manifest rodou com o lock solto — sem isso,
reverter a mudança deixaria a suíte verde. E `PartialCheckpointForTest` **já está errado
hoje**: não msynca `prazos` nem `escritas`, um buraco latente no teste de crash.

**Compaction:** a aquisição por record **já solta** entre records, então ela não é a fonte de
um stall longo — é problema de **vazão** (a compaction fica com o `wmu` numa fração alta do
tempo), e só lotear tornaria os stalls mais longos e mais raros sem reduzir a fração. Por
isso o lote vem acompanhado de **pacing**, com orçamento de ciclo de trabalho medido direto
do histograma novo.

### Disciplina de mutação

Para cada invariante nova, conferir que **quebrá-la deixa a suíte vermelha** — é a doutrina
do débito 53, "gate que passa com a ferramenta ausente é carimbo". Cinco mutações nomeadas:
inverter a ordem do caminho de abaixamento; remover a condição do recompute; remover um dos
msync novos; remover o rebuild quando a duração de fundo muda; mover o msync de volta para
dentro do `wmu` (o trace `wmu.solto` tem de acusar).

**Risco principal:** fazer `prazoVencido` ler o vencimento materializado transforma bug de
agenda em bug de query. Mitigado por teste de concordância cruzada sobre carga aleatória, por
um modo de auditoria no Open que reconstrói para região de rascunho e compara entrada a
entrada, e pelo fato de a folha ser escrita no mesmo ponto e a partir das mesmas entradas que
os dois arrays que ela substitui no predicado.

---

## Fase 3 — Pipelining: mover a barreira do fim de lote para a revogação

Hoje o poller bloqueia a cada lote até o writer drenar tudo
([consumer.go:566-579](internal/kafka/consumer.go:566)), o que elimina o overlap: o
writer fica 72% ocioso.

**A garantia que a barreira protege é sobre o commit, e ela é forma forte demais.** O
franz-go já impede, estruturalmente, que um callback de revogação rode enquanto o poller
segura um lote não-vazio — `PollRecords` adiciona o poller dentro do `fill` e só o remove
em `AllowRebalance()`. O que a barreira acrescenta é apenas que o writer tenha drenado
antes da revogação poder rodar. Essa propriedade não exige drenar a cada lote: exige drenar
**quando a revogação chega**.

Mover o dreno para dentro de `aoRevogar` preserva a propriedade exatamente e a tira do
caminho de regime. Reforço secundário: o commit do franz-go carrega member/generation, e
commit de geração vencida é **recusado pelo broker**, não aplicado em silêncio.

**E não toca o §26.** A ordem por partição não vem da barreira — vem de "uma FIFO, um
consumidor". Isso precisa estar no comentário, senão a próxima revisão reintroduz a
barreira "por segurança".

Mudanças: `Item.Marca` vira marcador de **quiesce** (não de fim de lote), carregando erro
de volta; `Writer.Quiesce(ctx)` roda checkpoint **incondicional** ao alcançar a barreira;
`aoRevogar` troca `w.checkpoint(ctx)` por `w.Quiesce(ctx)` — o que **fecha a corrida da
Fase 0.1** como efeito colateral, porque o checkpoint volta para a goroutine do writer.

**O modo de falha mais perigoso desta fase:** um writer que já saiu por `ErrPausado` travaria
o `Quiesce` dentro de um callback do franz-go — o group cai, as partições são revogadas e a
ingestão morre em silêncio. O `select` do `Quiesce` precisa de **três** braços: barreira
pronta, `w.parado`, `ctx.Done()`.

Riscos: o rebalance timeout passa a cobrir o dreno (cota calculada: ~1,3 s a 2.000 ev/s,
folgado, mas tem de estar no doc e observado por `OnPartitionsCallbackBlocked`); e
`queue_depth` deixa de ser ~0, invertendo o sentido de qualquer alerta que trate
"profundidade zero" como saudável.

**Aceite:** writer ocupado sobe de ~28% para >90%; teto do writer vai de ~430 para
**~1.540 ev/s**.

---

## Fase 4 — Decode workers preservando ordem

Quebrada em três subfases, sendo as duas primeiras **preservadoras de comportamento** — é
isso que torna a terceira segura.

**4.1 — Partir `Apply` em `Decodifica` + `Aplica`, ainda numa goroutine.** Novo tipo
`Decodificado`, dono dos buffers que o parse reusa, para que N deles coexistam. Dois
cuidados que a divisão obriga e que quebram coisas em silêncio se esquecidos:

- `m.metrics.events.Add(1)` sai da entrada do `Apply` e vai para `Aplica`. Não é cosmético:
  esse contador alimenta o watchdog. Contando no decode, um writer morto com decoders
  ocupados pareceria saudável e o watchdog **nunca dispararia**.
- **Recarimbo do relógio.** `Parse` carimba `_writtenAt` e as congeladas com o `agora`
  recebido. Decodificar adiantado moveria `_writtenAt` para o instante do *decode* — e é
  ele que governa o idle timeout. Sem recarimbar em `Aplica`, o relógio de expiração
  desloca em silêncio e entidades morrem cedo.

**4.2 — Decode read-only: a descoberta de campos volta para o writer.** Esta é a decisão
central. Deixar N workers descobrirem campos quebra **correção**, não só desempenho:
`LookupOrCreate` atribui FieldID na ordem de chamada, FieldID é formato de disco e a
posição da coluna no `SELECT *` é contrato do driver JDBC — com workers, a ordem vira a de
quem ganha o mutex. E "o tipo é fixado na primeira observação" viraria "na primeira a
chegar no lock", não "no menor offset". Existe teste irmão que fixa exatamente essa
propriedade (`internal/cli/determinism_test.go:19`), cujo comentário registra que a
violação já foi vista.

Desenho: leitores puros que devolvem `precisa=true` quando teriam de criar coluna; o worker
aborta antes de qualquer efeito colateral e marca o slot; o writer reprocessa esse evento
pelo caminho completo, **em ordem de offset**. Custo: um parse dobrado por evento de
descoberta, que em regime é ~zero (o piloto já tem ~1.700 colunas). Risco nomeado: fonte que
emite chave nova a cada evento degrada para a vazão de hoje — **nunca pior** — e precisa de
contador próprio com WARN.

**4.3 — A esteira de reordenação e os N workers.** Anel indexado por `seq & mask`, em que
reordenação, reciclagem de slot e backpressure viram **um** mecanismo. "O writer vê `seq` em
ordem" passa a ser verdade por construção. Mutex + cond, não lock-free: a 2.000 ev/s o
orçamento por evento é 500 µs e um sinal de cond é ~100 ns — o lock-free não compra nada e
custa uma prova.

A afirmação de correção a testar é mais forte que "preserva ordem por partição": o poller
atribui `seq` monotônico dentro do `EachRecord`, então reordenar por `seq` reproduz
**exatamente** a sequência que a goroutine única veria hoje.

**Memória — o flanco que o orçamento de 1 GiB não cobre hoje:** cada slot guarda buffers que
crescem até a marca d'água e nunca encolhem. Daí: janela de reordenação com default
`maxPollRecords` (1.000) e não `ingestionQueueSize` (5.000); gauge somando a capacidade das
arenas; e trim do arena acima de um limiar, para que um evento de 4 MiB não prenda 4 MiB × K
para sempre — mesmo desfecho do débito 28.

Config seguindo a convenção que o débito 95 acabou de estabelecer para `query.scanWorkers`:
chave nova com **default 1**, benchmark de 1/2/4/8 workers com baseline em
`benchmark/baselines/`, e `1` passando pela esteira do mesmo jeito — um caminho de código,
dimensionado diferente.

**O golden desta fase:** mesmo corpus, `decodeWorkers: 1` e `decodeWorkers: 8`, comparando
`SELECT *` byte a byte **e** `_meta.fields` — é o segundo que pega divergência de FieldID.
Mais o teste que reprova quem "otimizar" a descoberta para dentro dos workers: coluna auto
nova com offsets N (int) e N+1 (string), 8 workers, 500 repetições, e o tipo fixado tem de
ser **sempre** o de N.

**Aceite:** ≥2.000 ev/s sustentados, com ~2,2 cores em regime.

---

## Fase 5 — Só se a medição pós-Fase 4 disser que o `wmu` virou o teto

**Este é o critério de parada, e precisa estar no plano aprovado.** A aritmética diz que
Fases 3+4 já entregam a meta; a Fase 5 só se justifica com medição.

- **5.1** Encolher a seção crítica do `Put`: `ValidateRecord`, `validatePayloadAgainstDataset`
  e `BuildRecord` são funções puras de `(key, payload)` e podem sair de dentro do `wmu`.
  Semanticamente no-op, mensurável no `benchmark/micro`.
- **5.2** Batchar o lock do coletor de TTL — em boa parte tornado desnecessário pela Fase 2.
- **5.3** `Put` e `Apenda` em paralelo. **Podem**: são `Store` distintos, com `wmu`, `segW` e
  índices distintos; o único mutável compartilhado é o dicionário, que já tem lock próprio.
  Não por fork/join por evento (o join reserializa), mas por goroutine própria do histórico
  com anel ordenado. Duas coisas exigem decisão antes: a política de anel cheio muda a
  semântica do histórico de "todo evento, ou degradado-sticky" para "degradado quando não
  acompanha"; e o checkpoint da irmã precisa drenar o anel antes de durabilizar, porque é a
  ordem irmã→base que garante "histórico à frente, nunca atrás" (débito 88).

---

## Sequência e critérios de aceite

| # | Fase | Muda comportamento? | Aceite |
|---|---|---|---|
| **−1** | **Corrida em `Writer.checkpoint`** — branch `fix/` própria, **antes de tudo** | corrige bug | teste de revogação concorrente sob `-race` |
| 0.2 | Lag real + watchdog | **sim** (série encolhe) | `lag == hwm − offset` numa raspagem; broker fora → série **some** |
| 0.3 | Bancada (67, 6, 68, 52) | não | prova por mutação: executor devolvendo 0 linhas deixa a rodada **vermelha** |
| 0.4 | Instrumentação | não | um dia inteiro raspado no piloto **antes** de otimizar |
| 1 | Passada única no `extract` | não | 902 → ~717 µs/evento; teto 1 core ~1.395 ev/s |
| 2 | Agenda de vencimentos | não (semântica igual) | suíte de expiração passa **sem alteração**; `expiry_scanned_total` deixa de crescer com a contagem de linhas |
| 3 | Pipelining | sim | writer ocupado 28% → >90%; teto ~1.540 ev/s |
| 4 | Decode workers | sim | golden `workers:1` × `workers:8` idêntico; **≥2.000 ev/s** |
| 5 | Encolher `wmu` | condicional | **só executar se a medição pós-fase 4 disser que o `wmu` é o teto** |

A ordem não é negociável em três pontos: a **fase −1** sai numa branch `fix/` própria antes
de tudo; **0.3 e 0.4 vêm antes das otimizações** (a bancada mente e o painel mente —
otimizar sem consertá-los é repetir o que já aconteceu); e **1 e 2 vêm antes de 3 e 4**
(paralelizar trabalho redundante multiplicaria o desperdício por N workers em vez de
eliminá-lo).

**Validação: fase a fase no piloto.** Cada fase sobe sozinha via `scripts/nishidb-update.sh`
(que faz rollback do par binário+config) e roda ao menos um dia, com profile de CPU de 45 s
e µs/evento por `/proc` medidos antes e depois. É mais deploys, mas cada ganho fica
atribuído a uma mudança — e foi exatamente a falta dessa atribuição que deixou o débito 65
em aberto por quatro rodadas.

## Verificação

- `make test`, `make test-race`, `make test-integration`, `make lint` — lembrando que o gate
  de lint quebra **nos dois sentidos**: contagem abaixo da baseline também falha, então o
  arquivo tem de ser igualado no mesmo PR.
- `make bench-micro` com `NISHIDB_BENCH_DIR` em disco real, e baseline novo em
  `benchmark/baselines/` a cada fase.
- **No piloto**, com o pprof que já está de pé em `127.0.0.1:9144`: profile de CPU de 45 s
  antes e depois de cada fase, e a medição de µs/evento por `/proc/<pid>/stat` cruzada com
  `nishidb_ingest_events_total` — que é como os 902 µs deste diagnóstico foram obtidos.
- Gate de regressão em CI (`bench-guard`): teto de ns/op versionado, em **ratchet** — quando
  a otimização entra, o teto desce no mesmo PR. Orçamento com teto explícito, e não
  `benchstat` contra baseline, porque runners do GitHub são ruidosos e um gate que pisca é
  pior que nenhum.

## Documentação (parte do entregável)

- `docs/piloto-events.md` — a quinta medição, no formato "o que foi medido, não o que se
  esperava" que o documento já usa: o lag que mentia, os 2.394 B/linha do history em regime
  estável contra os 2.767 do replay, e a ocupação de 54,5% da `primary.idx`.
- `docs/architecture.md`, `docs/operacao-kafka.md` — a tabela de decisão saturado × sem
  produção, e o `absent(nishidb_kafka_lag)` como alerta.
- `docs/diagrams/` — `kafka_pipeline.puml` redesenhado e `expiracao_agenda.puml` novo,
  validados com `plantuml -checkonly` (atenção ao débito 92: `if` sem `endif` **não** falha,
  então a aferição do validador precisa usar uma linha que não é sintaxe nenhuma).
- `docs/debitos.md` — fecha 6, 52, 67, 68 e o remédio do 40; registra a fronteira contra o
  débito 22 (a agenda **não** precisa de backfill); débitos novos a partir do 96.
- `planning/` — este plano.
