# Spec — emissão de série alinhada à fronteira e envio Kafka com contrapressão

Branch: `feature/series-aligned-paced-emission` (a partir de `origin/main` = v0.13.0, `1cba0a3`).

## Motivação (consumidor: metric-probe do tems → ngrrd)

O ngrrd encaixa cada amostra num slot de `every` (300 s) SEM interpolar. Dois defeitos
da emissão de contadores (`emit:` com destino Kafka do `seriesDelivery`) impedem o uso:

- **P1 — descarte por buffer cheio.** `internal/seriesdelivery/delivery.go:193-210`: o
  `Publish` não espera; com `records >= MaxRecords (10.000)` ou bytes acima de
  `MaxBytes (16 MiB)` POR DESTINO, conta `buffer_full` e devolve. A passada
  (`internal/series/emitter.go:286-309`) é um laço apertado: na .218, slot (21.402
  grupos) e pon (253.483) perderiam a maior parte de cada passada.
- **P5 — período com atraso fixo e fase livre.** `emitter.go:497-521`: primeira passada
  imediata (`NewTimer(0)`) e `t.Reset(Every)` DEPOIS da passada; `at = now()` no início
  (`:282`). O período real é `every + duração`, e a fase muda a cada restart: uma fração
  `1 − every/P` dos slots do ngrrd fica vazia (medido por eles: 3% com passada de 10 s,
  17% com 60 s).

## Decisões do usuário (fechadas — não reabrir)

1. **P1 = contrapressão (pacing)**: quando o destino está cheio, o `Publish` ESPERA vaga
   em vez de descartar. Memória continua limitada pelos mesmos `MaxRecords`/`MaxBytes`.
   SEM limite configurável.
2. **P5 = taxa fixa alinhada**: passadas nas fronteiras múltiplas de `every`, e `at` =
   a fronteira (não o `now()`).
3. **Passada que atrasa além da fronteira seguinte**: a próxima passada começa LOGO que a
   atrasada termina, com `at` = a fronteira perdida mais recente (`floor(fim, every)`).
   No máximo uma passada fica pendente; as fronteiras do meio são PULADAS e contadas
   numa métrica.

## Comportamento especificado

### Fronteira

- Fronteira = instante UTC cujo `UnixNano` é múltiplo de `every.Nanoseconds()` (alinhado
  à ÉPOCA UNIX, não ao `time.Time.Truncate`, que alinha ao ano 1 e diverge para `every`
  que não divide o dia — ex.: `every: 7m`). Uma função pura, testada com `every` que
  divide e que não divide 24 h.
- `every` mínimo continua `schema.MinEmitEvery` (10 s); nada muda no YAML.

### Laço (substitui `run`)

Estado: `last` = `at` da última passada iniciada (zero antes da primeira).

1. **Primeira passada:** na primeira fronteira `>= now()` (se `now()` cai exatamente numa
   fronteira, é ela). NÃO há mais passada imediata na subida. (Decisão do orquestrador:
   emitir na subida com `at = floor(now)` duplicaria o slot já emitido pelo processo
   anterior com valores de outro instante.)
2. **Depois de cada passada**, com `end = now()` e `next = last + every`:
   - `end < next` → dorme até `next`; a passada usa `at = next`.
   - `end >= next` → passada IMEDIATA com `at = floor(end)`; as fronteiras estritamente
     entre `last` e `floor(end)` — `(floor(end) − last)/every − 1` delas — são
     **puladas**: somam no contador de fronteiras puladas e geram UM log WARN por
     ocorrência (não por fronteira) `series.boundaries_skipped` com dataset, counter,
     `skipped`, `at` da passada e a duração da passada anterior.
3. **`at` é estritamente crescente** na vida do emissor, mesmo com o relógio de parede
   voltando (NTP): se `now()` < `last` (relógio voltou), o laço espera até `last + every`
   pelo relógio de parede e nunca reemite um `at` <= `last`. Documentar: com relógio
   voltando, a espera pode passar de `every`.
4. O temporizador acorda pelo relógio de parede recalculado a cada volta (nada de
   acumular `Reset(every)`); um despertar antecipado (timer impreciso) volta a dormir até
   a fronteira — a passada NUNCA começa antes de `at`.
5. `at` vai para: o campo `at` do arquivo e do Kafka, o `Timestamp` do record Kafka, e o
   `atMillis` das medidas (`MeasureCount(id, i, atMillis)`). `lastSuccess` (arquivo)
   continua sendo o relógio real do fim da passada.
6. Cancelamento: o laço e a espera pela fronteira terminam com o `ctx` (Stop) sem
   esperar o timer.

### Contrapressão no `seriesDelivery`

- A interface muda para propagar cancelamento: `Output.Publish(ctx, key, body, at) error`
  (ou equivalente). Devolve erro SÓ quando `ctx` foi cancelado durante a espera (o
  chamador então aborta a passada; `run` vê `ctx.Err() != nil` e sai sem logar
  `series.stopped`). Falha de Kafka continua NÃO sendo erro do `Publish`.
- Admissão: com destino fechado → `shutdown` (descarta na hora, como hoje); registro
  acima de `maxRecordBytes` → `message_too_large` (na hora, como hoje); capacidade
  cheia (`records >= MaxRecords` ou `n > MaxBytes − bytes`) → **ESPERA** até a
  liberação por um callback, o fechamento do destino (→ `shutdown`) ou o cancelamento do
  `ctx` (→ conta `shutdown` e devolve o erro). A cada despertar todas as condições são
  reavaliadas.
- Sem espera ativa (busy-wait) nem `sleep` com polling: o callback que libera vaga
  acorda os que esperam (`sync.Cond` + `context.AfterFunc`, ou canal de geração — escolha
  do Builder, justificada no godoc). Vários emissores podem esperar o mesmo destino ao
  mesmo tempo (contadores diferentes no mesmo destino global); justiça não é exigida,
  progresso é.
- O tempo esperado acumula por (contador, destino) em
  `protocol.SeriesDeliveryStats.AdmissionWaitSeconds` (json `admissionWaitSeconds`) e
  vira a métrica `nishidb_series_kafka_admission_wait_seconds_total{dataset,counter,destination}`
  (constante em `internal/metrics/names.go`, nasce em zero, mesma estrutura do
  `/v1/stats` — molde: `MetAlarmDeliveryAdmissionWaitSecondsTotal` e
  `internal/alarmdelivery/queue.go:139-193`).
- `buffer_full` continua no vocabulário de rótulos (contrato; nasce em zero), mas a
  admissão não o produz mais; só o mapeamento de `kgo.ErrMaxBuffered` em `dropReason`
  ainda pode produzi-lo. Documentar isso.
- Destino Kafka fora do ar: os registros em voo expiram em `timeout` e liberam vaga, então
  a passada ANDA (≈ grupos/10.000 × timeout; ex.: pon 253 mil × 5 s ≈ 2 min) e pode
  atrasar fronteiras (→ política acima). Um destino lento ATRASA a passada e os demais
  destinos do mesmo contador, mas não a interrompe — atualizar a frase de
  `docs/operacao-series-kafka.md` que hoje diz que a falha não interrompe os demais.
- `Manager.Close` continua seguro com `Publish` em espera (o emissor é parado antes, mas
  o Close não pode travar se houver espera: fechamento acorda os que esperam).

### Observabilidade do emissor

- `series.Stats` ganha `SkippedBoundaries uint64` (acumulado desde a subida) e
  `LastPassDurationMs int64` (duração da última passada concluída). Métricas:
  `nishidb_series_skipped_boundaries_total{dataset,counter}` e
  `nishidb_series_last_pass_duration_seconds{dataset,counter}` — constantes em
  `names.go`, nascem com zero, lidas da MESMA estrutura que o `/v1/stats` expõe (achar
  onde `series.Stats` vira `/v1/stats` e `/metrics` hoje: `internal/server/metrics_alarm.go`
  ~l.200 e a montagem do stats do dataset). Família + nome em toda amostra
  (`TestAmostrasDeMetricaTemFamiliaENome`).

## Garantias (cada uma com teste que fica VERMELHO com a mutação indicada)

| # | Garantia | Mutante que o teste tem de pegar |
|---|---|---|
| G1 | `at` de toda passada é múltiplo de `every` na época Unix | `at = now()` |
| G2 | Alinhamento à época, não ao ano 1, para `every: 7m` | trocar pela `Truncate` |
| G3 | Primeira passada na primeira fronteira `>= now`, não imediata | `NewTimer(0)` |
| G4 | Período fixo: passada que dura `d < every` não desloca a próxima (`at` consecutivos diferem exatamente `every`) | `Reset(every)` depois da passada |
| G5 | Passada atrasada além de 1 fronteira: próxima IMEDIATA com `at = floor(end)`; `SkippedBoundaries` += fronteiras do meio; 1 WARN | pular sem emitir / emitir com `at` da 1ª perdida / não contar |
| G6 | Atraso que termina antes da fronteira seguinte: nenhuma pulada | contar pulada quando `end == next − ε` |
| G7 | `at` estritamente crescente com relógio voltando | aceitar `at <= last` |
| G8 | Passada nunca começa antes de `at` (despertar antecipado volta a dormir) | executar no primeiro despertar |
| G9 | Stop durante a espera da fronteira devolve sem esperar o timer | ignorar o ctx na espera |
| G10 | Publish com destino cheio ESPERA e entrega tudo: N registros > MaxRecords com produtor fake que confirma em lotes → `Delivered == N`, `Dropped[buffer_full] == 0` | o `buffer_full` antigo |
| G11 | Espera acorda pela liberação (sem polling): teste com produtor fake que só libera ao ser mandado; o Publish retorna logo após a liberação | `time.Sleep` em laço |
| G12 | Stop (ctx) com Publish em espera → Publish devolve erro, `shutdown` contado, passada aborta, `run` sai SEM `series.stopped` | ignorar ctx / logar stopped |
| G13 | `Manager.Close` com Publish em espera não trava e conta `shutdown` | Close sem acordar os que esperam |
| G14 | Dois emissores no mesmo destino cheio progridem os dois | Signal em vez de Broadcast (se Cond) |
| G15 | `AdmissionWaitSeconds` > 0 só quando houve espera; 0 sem espera | acumular sempre / nunca |
| G16 | Métricas novas nascem em zero e saem em `/metrics` e `/v1/stats` iguais | contador duplicado |
| G17 | Sem alocação nova por grupo no caminho sem espera: o gate de `AllocsPerRun` existente da emissão continua verde (e o Publish com ctx não aloca na admissão sem espera) | `context.WithTimeout`/AfterFunc fora do caminho de espera alocando por registro — medir antes de afirmar |

G17: hoje `Publish` já aloca o record e as cópias (necessário). Não piorar: o
`context.AfterFunc` (se usado) só no caminho de espera.

Testes do laço sem sono real: injetar relógio E temporizador (`Options.Now` já existe;
acrescentar um hook de temporizador exclusivo de teste no mesmo molde, ex.
`After func(time.Duration) <-chan time.Time` ou um fake clock local ao pacote). Nenhum
teste espera `every` real.

## Contrato e documentação (mesmo commit do código)

- `docs/operacao-series-kafka.md`: `at` = fronteira alinhada à época; primeira passada na
  primeira fronteira; política de atraso; contrapressão (o buffer passa a ser limite de
  memória com espera, não descarte); `buffer_full` deixa de ser produzido pela admissão;
  nova métrica de espera; destino lento atrasa a passada; recomendação de tópico
  particionado criado já com o número final de partições (mudar depois remapeia chaves).
- Doc da emissão em arquivo (achar: `internal/schema/emit.go` godoc, README seção `emit`,
  `docs/metricas.md#emissão-periódica-de-contador`): `at` alinhado, sem passada na subida.
- `docs/metricas.md`: as três métricas novas.
- `docs/architecture.md` só se citar o período/fase (conferir).
- Diagrama PlantUML que cite o laço de emissão (procurar em `docs/diagrams/`); atualizar
  se descrever `Reset(every)`/passada imediata.
- Termos novos para o glossário vêm no RELATÓRIO (o Builder NÃO edita
  `docs/glossario.md`): fronteira, fronteira pulada, contrapressão do seriesDelivery.
- Identificadores PT-BR tocados: renomear no mesmo PR e registrar em
  `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`.
- Copiar esta spec para `planning/series-aligned-paced-emission.md`.

## Fora de escopo

- Paralelizar a varredura ou ter vários produtores por destino.
- Persistir a última fronteira emitida entre restarts.
- Mudar `MaxRecords`/`MaxBytes`.
- Ligar `emit` na .218 (deploy combinado com o usuário depois).

## Emendas da refutação do plano (PREVALECEM sobre o texto acima)

E1 (BLOQUEANTE — buffer_full escondido no kgo). O kgo chama o callback do usuário ANTES
de decrementar o próprio contador (franz-go v1.21.6 `pkg/kgo/producer.go:854` promise;
`bufferedRecords--`/`bufferedBytes--` em :865-866; checagem em :619-624). Com os limites
idênticos (`delivery.go:137`), o acordado pela vaga local cai no `ErrMaxBuffered`.
Correção: o limite do kgo passa a ser FOLGA acima do local — `kgo.MaxBufferedRecords(2*MaxRecords)`
e `kgo.MaxBufferedBytes(2*MaxBytes)` —, e a contabilidade local é o limite que vale. Godoc
com o argumento (worker de promessas único, `producer.go:745-761`). Teste G10b com kfake
(o repo já usa `kfake`? conferir; se usa, usar) e N = 3×MaxRecords exigindo
`Dropped[buffer_full] == 0` e `Delivered == N`. Mutante: limites do kgo de volta iguais.

E2 (BLOQUEANTE — onde as métricas do emissor aparecem). `series.Stats` NÃO está no
`/v1/stats` (só `SeriesDelivery` em `documents.go:285-287`); vira só `/metrics` em
`samplesOfSeries` (`internal/server/metrics_alarm.go`), que hoje pula emissor sem arquivo
(`if !st.File { continue }`, ~l.151-153). Decisão: as duas métricas novas do EMISSOR
saem SÓ no `/metrics` (sem bloco novo no wire), para TODO emissor — inclusive só-Kafka —,
ANTES do `continue` do arquivo. Nomes: `nishidb_series_skipped_boundaries_total`
(counter) e `nishidb_series_pass_seconds` (gauge, duração da última passada concluída;
molde `nishidb_alarm_sweep_seconds`). Já `admissionWaitSeconds` vai no
`SeriesDeliveryStats` (que já está no `/v1/stats`) e na métrica
`nishidb_series_kafka_admission_wait_seconds_total`. Atualizar `docs/metricas.md`
(inclusive a frase ~l.276-278 "as cinco primeiras saem por contador com destino file"),
o gate `TestTodaMetricaDeclaradaEstaDocumentada` exige a linha, e
`internal/server/metrics_series_delivery_test.go`. G16 vira: métricas nascem em zero e o
`admissionWaitSeconds` do `/v1/stats` é o MESMO valor da métrica.

E3 (contrato do EmitOnce). `EmitOnce(ctx context.Context, at time.Time) error`: o `at`
vem do laço, que é o dono da política. Atualizar todos os chamadores (gates
`emitter_gate_test.go`, `emitter_test`, `stats_test`, `emitter_metrics_test`,
`emitter_outputs_test`, `EmitOnceForTest` em `internal/server/export_test.go:131` — este
passa a fronteira `floor(now, every)` do relógio do servidor). G1/G2/G3/G4 testam o LAÇO.

E4 (testes do servidor que ficariam intermitentes). `alarms_test.go:174
TestDeclaredEmitWritesTheSeries`, `series_delivery_test.go:24` e `:142` (TLS/SASL)
dependem de passada na subida com relógio real e prazo de 2–5 s. Reescrever para NÃO
depender de fronteira real: parar os laços (`StopEmittersForTest`, se existir, ou criar)
e chamar `EmitOnceForTest`. Proibido "resolver" aumentando prazo. Testes que quebram por
contrato mudado, legitimamente: `delivery_test.go:43 TestCapacityIncludes…` (vira G10/G11);
`emitter_test.go:278 TestLacoEscreveNaPrimeiraPassada…` continua verde por acaso
(relógio congelado numa fronteira) — reescrever e renomear para o comportamento novo.
`clockOfTest` (`emitter_test.go:93`) não é seguro entre goroutines: relógio do teste do
laço tem de ser atômico (molde `testClock`, `counter_metrics_test.go:109`).

E5 (mutantes que precisam de desenho de teste discriminante).
- G1/G3: subida FORA da fronteira e timer falso que acorda ATRASADO (+ alguns ms);
  senão `at = now()` passa verde.
- G14: limite de BYTES com tamanhos mistos, ou cancelar o ctx de A com B esperando
  (Signal acordaria o errado e G12 travaria) — o teste tem de ficar vermelho com
  Signal no lugar de Broadcast (se a escolha for Cond).
- G11 sem tempo: contar as reavaliações/despertares por gancho de teste, não por prazo.

E6 (G17 cobre o caminho Kafka). Dois gates de alocação novos (`//go:build !race`, com
contraparte SKIP sob race, como os existentes): (a) emissor com `Output` fake que não
aloca → zero alocações por passada além do que já é hoje com Outputs; (b) `Publish` SEM
espera com contagem FIXA medida (o que a cópia do record exige hoje), para um
`context.AfterFunc` no caminho rápido reprovar. Medir antes de fixar o número.

E7 (bordas do laço).
- A regra 2 se REAPLICA AO ACORDAR: se, ao acordar para `next`, `now >= next + every`
  (sono passou da conta: VM pausada, NTP para frente), a passada usa `at = floor(now)` e
  as do meio contam como puladas — nunca sai um `at` velho.
- WARN `series.boundaries_skipped` só quando `skipped > 0`. Passada que atrasa sem pular
  (`end ∈ [next, next+every)`) não loga; aparece só no `nishidb_series_pass_seconds`.
- G7 tem de exercitar `end < last` (relógio voltou abaixo do último `at`).
- Bordas a testar: `end == next` exato (imediata, `at = next`, 0 puladas); passada de
  duração exatamente `every`; `every` que não divide 24 h.

E8 (passada abortada pelo ctx). Checar o ctx na ENTRADA do `Publish` (cancelado não
admite mais nada). Passada abortada: NÃO soma `Errors`, NÃO atualiza `lastSuccess` nem a
duração da passada; as linhas já no buffer do arquivo são gravadas pelo `Close`
(`writer.go:191-195`) — documentar que o arquivo pode terminar com uma passada parcial
daquele `at` no encerramento.

E9 (medidas). `measures.go:266` usa `max(m.epoch, at/width)`; com `at` = fronteira (que
pode estar no passado na passada atrasada), documentar que `at` é o slot NOMINAL e os
valores são lidos durante a passada.

E10 (destino fora do ar — documentar sem prometer). O kgo falha por lote olhando o ctx
do primeiro registro (`sink.go:380-410`, `1921-1930`); com `timeout` até 5 min
(`config/series_delivery.go:34-36`) uma passada da PON com o broker fora pode levar
horas, e ATRASA também o ARQUIVO do mesmo contador. `docs/operacao-series-kafka.md`
~l.147-148 hoje diz o contrário: corrigir e dizer explicitamente. Não estimar número que
não foi medido.

E11 (docs). Acrescentar: godoc do `Start` (`emitter.go:459-463`, "primeira passada
imediata"), `docs/architecture.md` seção da emissão (~l.2943+). Nenhum `.puml` descreve
o laço — sem diagrama a atualizar. `testOutput` (`delivery_test.go:38`) monta
`destination` à mão: inicializar o que a espera precisar.
