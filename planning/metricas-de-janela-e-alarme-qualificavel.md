# Métricas de janela no contador e alarme qualificável

## Context

O alarme publicado na v0.8.0 (`counters[].alarms[]`) só enxerga o valor
instantâneo do grupo (`total`). Com isso ele não distingue "OLT que nunca teve
ninguém" de "OLT que tinha gente e esvaziou", e não suaviza oscilação. Este ciclo
traz três coisas:

- **Métricas de janela por grupo:** avg/min/max de `total` em janelas
  declaradas, expostas como colunas `<metric>_<aggregate>` em `_counters.<nome>`.
- **A tabela `_meta.counter_metrics`**, com a definição de cada métrica
  referenciando o contador pai.
- **`qualifyWhen` no alarme:** uma condição de elegibilidade, ao lado de `when` +
  `sustainedFor`.

A issue #61 (razão entre contadores) fica para o ciclo seguinte. O desenho não a
bloqueia: `source` é um enum que poderá citar um contador derivado depois.

### Contrato aprovado

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    metrics:
      - name: auth_5m          # nome livre; as colunas saem <name>_<aggregate>
        type: window
        source: total
        window: 5m
        aggregates: [avg]
      - name: auth_24h
        type: window
        source: total
        window: 24h
        aggregates: [max]
    alarms:
      - name: primary_quase_vazia
        qualifyWhen: "auth_24h_max > 10"   # mesmo nome da coluna do SQL
        when: "auth_5m_avg < 1"
        sustainedFor: 5m
```

### Decisões fechadas com o usuário

| Tema | Decisão |
|---|---|
| Onde declara | `metrics:` dentro do contador existente, irmão de `alarms:`/`emit:` |
| Nome na condição | `auth_5m_avg`, idêntico à coluna do SQL. Sem gramática nova: `a.b` já é campo aninhado (`parser.go:505`, `binder.go:440`) |
| Colisão | Recusada na subida: nome de métrica igual a coluna, métrica ou alarme do contador; coluna gerada igual a `groupKey`, `total`, coluna do `groupBy` ou outra coluna gerada. No SELECT, repetição e `AS` passam pelo `normalizeProjectionNames` (`internal/query/binder/projection_names.go:13`), que numera com `_1`, `_2` |
| Janela incompleta | Agregado das amostras que existem; NULL só sem nenhuma amostra na janela |
| `qualifyWhen` falso com o alarme disparado | **Resolve com motivo `disqualified`** (novo, distinto de `condition_cleared` e `group_retired`) |
| `alarms.state` | Sobe para a **v2** sem leitor da v1: a v1 cai em `ErrCorruptAlarmState`, com WARN `alarm.state_unreadable` e cronômetros do zero. Registrar na release |
| Memória | **Arquivo mmap por métrica**, com heap pequeno e limitado |

## Desenho

### 1. Schema — `internal/schema/counter_metric.go` (novo)

- **Leitura do YAML:** `metricsFromNode` segue o molde de `alarmsFromNode`
  (`internal/schema/alarm.go:102`). A whitelist via `block()` aceita `name`,
  `type`, `source`, `window` e `aggregates`. `metrics` entra na lista de
  `countersFromNode` (`counter.go:118`).
  `schema.CounterMetricSpec{Name, Type, Source, Window time.Duration, Aggregates []string}`
  fica em `CounterSpec.Metrics`.
- **Recusas**, todas via `specErrf` / `ErrInvalidSpec` (mesmo exit do alarme):
  - lista presente e vazia; mais de `MaxCounterMetrics = 8` itens;
  - `name` fora de `[a-z0-9_]+`, ou repetido;
  - `type` diferente de `window`; `source` diferente de `total`;
  - `window`:
    - lida por `units.DurationWithDays`;
    - `never` e zero recusados;
    - menor que `MinMetricWindow = 1m` ou maior que `MaxMetricWindow = 30d`;
    - **não múltiplo de 1m** (a largura do balde, janela ÷ 60, sai em segundos inteiros; nada é arredondado);
  - `aggregates` vazio, repetido ou fora de {`avg`, `min`, `max`}.
- **Colisão num lugar só:** uma função única monta o conjunto de nomes do
  contador (`groupKey`, `total`, `groupBy`, colunas geradas, nomes de métrica e
  de alarme) e recusa repetição. `validateGroupingColumn` (`counter.go:317`) e
  `TableSchema` (`internal/counters/table.go:81-93`) passam a usar essa função,
  para não haver duas listas.
- **`AlarmSpec.QualifyWhen string`:** opcional; se presente, não pode ser branco;
  normalizado como o `when` (`alarm.go:180-224`); entra na whitelist do alarme.

### 2. Storage da janela — `internal/storage/metricwindow` (novo, dono do formato em disco)

- **Arquivo:** `<idx>/counter_metrics/<counter>/<metric>.win`, fora do diretório
  do contador para não acoplar à reconstrução dele. O caminho sai de
  `Store.CounterMetricsDir()`, no molde de `Store.AlarmStatePath()`
  (`store.go:2455`).
- **Layout colunar**, via `mmap.Region` (`internal/storage/mmap/region.go`:
  `Open`, `Grow`, `AtomicLoad64`/`AtomicStore64`, `Msync`, `ReleaseBlocks`):
  - cabeçalho: magic `NDBT` (`NDBW` já é o WIP do índice por segmento), versão 1,
    `bucketCount` = 60, `bucketWidthMs`, `source`, `capacity`, crc32c do cabeçalho;
  - coluna de identidade por id de grupo: `keyHash u64` e `validFromEpoch u64`;
  - 60 colunas de balde, `slot × capacity`. Cada entrada tem 32 B:
    `epoch u32 | owner u16 | count u16`, `sum`, `min`, `max`. O `owner` é a
    marca de dono (`OwnerTag(keyHash)`, 16 bits). Um balde gravado por um dono
    novo não passa como do antigo quando, num crash, a página de identidade não
    chegou ao disco. A contagem de 16 bits cabe: balde máximo de 12 h com
    amostra a cada 1 s ou mais dá no máximo 43.200 amostras.
  - **Por que colunar:** um balde só é escrito **quando fecha**. Fechar o
    balde k suja só a coluna k. Com o layout por grupo, cada passada sujaria todas
    as páginas, e o writeback do kernel regravaria o arquivo inteiro a cada ~30 s
    (~32 GB/dia no piloto).
- **Sem relógio no pacote.** O chamador passa o epoch do balde
  (`unixMs / widthMs`), então o slot é `epoch % 60`. O epoch gravado em cada
  entrada torna uma escrita parcial num crash inofensiva: entrada de epoch velho
  fica fora da janela.
- **Crescimento:** reescrita para capacidade 2× (tmp → rename). Só o amostrador
  toca o arquivo, então não há leitor concorrente no mmap. O teto é
  `maxGroups + 1`.
- **Fora do §48:** não entra na ordem de checkpoint. O godoc justifica como o
  `alarms.state`: não descreve dado materializado nem posição de consumo.
- **Golden de bytes** do cabeçalho e de uma entrada.

### 3. Amostrador — `internal/countermetrics` (novo)

Mora fora de `internal/counters` e de `storage`, porque tem relógio. É o mesmo
argumento de `internal/alarm` e `internal/series`.

**Abertura** (por dataset):
- abre um `metricwindow` por métrica;
- remove arquivo órfão (métrica que saiu do YAML) com WARN `counter_metric.orphans`;
- cabeçalho com `bucketWidthMs`/`source` divergente recria o arquivo, com WARN
  `counter_metric.reset`; mudar `aggregates` não recria, porque o balde guarda
  todos os campos.

**Estado em heap**, por grupo e por métrica, ~112 B (piloto: ~630 KB; pior caso
com `maxGroups` default: ~15 MB):
- balde aberto;
- cache dos baldes fechados (`sum`/`count`/`min`/`max`), recalculado só no
  fechamento de balde;
- `validFromEpoch`;
- valor publicado.

**Publicação:** um **seqlock** por entrada (`seq`, `samples`, `avg` em bits,
`min`, `max`), em chunks de tamanho fixo que nunca se movem; o diretório de chunks
sai por `atomic.Pointer`. Leitores (SQL e alarme) não tomam lock nem alocam, e
nunca veem uma linha rasgada (avg de uma passada com max de outra).

**Passada** (`SampleOnce`, pública para os testes, relógio injetado):
- **Intervalo:** `clamp(menorJanela/60, 1s, 10s)`.
- Para cada contador com métricas, e para cada id em `1..Limit()`, lê
  `reader.Group(id)`: só o lock folha e atômicos (`storage/counter/counter.go:633`).
  - **Grupo não vivo:** zera o publicado (aposentado).
  - **Hash da chave ≠ identidade gravada** (grupo novo ou contador reconstruído):
    `validFromEpoch = epoch atual`, e os baldes anteriores são ignorados.
  - **Epoch mudou:** o balde aberto vai para a coluna do slot, o cache de fechados
    é recalculado (60 leituras) e um balde novo é aberto.
  - Soma a amostra ao balde aberto e publica (O(1)).
- **Zero alocação por grupo**, com gate `AllocsPerRun` e contraparte `-race` com
  SKIP explícito.

**Restauração na abertura:** calcula o publicado a partir das colunas, relativo ao
agora. O alarme, na primeira passada depois de um reinício, já vê
`auth_24h_max`; é o que impede o `qualifyWhen` de ficar cego por 24 h a cada
deploy.

**`Stop`:** grava os baldes abertos no slot e faz `Msync`. Um reinício dentro do
mesmo epoch recarrega o balde aberto do disco.

**Erro de infra** (grow, msync):
- o laço para, com ERROR `counter_metric.stopped` e o gauge
  `nishidb_counter_metric_sampling` indo a 0;
- o publicado congela, com `sampledAt` visível;
- é o mesmo precedente de `alarm`/`series` (`loop.go:141`).

### 4. Tabelas — `internal/counters`

- **`TableSchema(def, metrics)` e `Table(def, reader, values)`:**
  - as colunas geradas entram depois de `total`: `avg` como `double`, `min`/`max`
    como `int64`;
  - `values` é uma interface definida no consumidor e implementada por
    `countermetrics`;
  - sem amostra, a coluna fica ausente do payload (NULL é ausência);
  - o array de pilha de `record()` (`table.go:228`) cresce para
    `maxGroupingColumns + 2 + MaxCounterMetrics*3`;
  - gate `TestTabelaDoContadorNaoAlocaPorLinha` com métricas presentes.
- **`_meta.counter_metrics`** (novo, `internal/counters/metrics_meta.go`, molde de
  `meta.go`), uma linha por métrica, registrada **sempre** em `counters.Register`
  (`table.go:160`):

  | coluna | tipo | observação |
  |---|---|---|
  | `id` | string | key: `<counter>\|<metric>` |
  | `counter` | string | contador pai |
  | `name` | string | |
  | `type` | string | |
  | `source` | string | |
  | `windowMs` | int64 | |
  | `bucketMs` | int64 | |
  | `aggregates` | string | na ordem declarada |
  | `columns` | string | ex.: `auth_5m_avg` |
  | `groups` | int64 | |
  | `heapBytes` | int64 | |
  | `diskBytes` | int64 | |
  | `sampledAt` | timestamp | NULL antes da 1ª amostra |

  - Gate de zero alocação.
  - O catálogo e a árvore do JDBC descobrem a tabela sozinhos
    (`catalog.go:139`). Quebram as listas fixas de teste:
    - `server/documents_test.go:759`
    - `server/metatables_test.go:53`
    - `cli/metatables_query_test.go:57,97`
    - `query/metatables_test.go:42`
    - `query/metatables_gate_test.go:24`
    - `server/counters_test.go:203`
    - `query/meta_counters_routing_test.go:20,35`
    - `counters/meta_test.go:191`

### 5. Alarme — `internal/alarm`

- **Compilação:** `qualifyWhen` passa por `predicate.CompileCondition` contra o
  mesmo `TableSchema`, agora com as colunas das métricas (`compile.go:121,140`).
  A tabela própria do runner (`runner.go:165`) recebe `values`.
- **Máquina de estado** (`state.go`):
  - `observation` ganha `qualified`, e há veredito novo `verdictDisqualified`.
  - Pending sem qualificação volta a Absent, sem evento.
  - Firing sem qualificação resolve com `reason: disqualified`.
  - Precedência na mesma passada: `group_retired` > `condition_cleared` >
    `disqualified`.
- **Custo:** o `when` é avaliado primeiro, e o `qualifyWhen` só quando o `when`
  vale ou já existe tracker. O gate de zero alocação do grupo saudável
  (`runner_gate_test.go`) continua valendo.
- **`alarms.state` v2** (`storage/checkpoint/alarms.go`):
  - `AlarmEntry` ganha `QualifyWhen` e `MetricsFingerprint`: texto canônico e
    ordenado das métricas citadas por `when` ou `qualifyWhen`, com
    `name:type:source:window`, obtido dos `Slots.IDs`.
  - Uma janela alterada no YAML invalida o cronômetro, como já acontece com
    `when` e `sustainedFor`.
  - A v1 é recusada como ilegível.
- **Evento e sink:**
  - `Event` ganha `QualifyCondition` e `Metrics []Column`: os valores das colunas
    de métrica do grupo no instante da decisão.
  - O log sai no grupo `metrics` (sem colisão com os atributos soltos),
    `qualifyCondition` só quando declarado, e o motivo `disqualified`.
- **`_meta.alarms`** ganha a coluna `qualifyCondition` (NULL se não declarada).

### 6. Servidor e CLI

- **Subida:** abrir `countermetrics` antes de `counters.Register`
  (`server.go:491`) e da compilação do alarme (`server.go:499`); dar `Start`
  antes dos alarmes (`turnonAlarms`, `server.go:854`).
- **`encerra()`:** `forCounterMetrics` junto de `forAlarms`/`forEmitters`, antes
  de `closeDatasets` (`server.go:1039`). A lista `paradas` do gate AST
  (`internal/gates/alarm_shutdown_test.go:32`) ganha o item.
- **CLI local** (`cli/query.go:129`): abre os arquivos com
  `mmap.OpenReadOnly` e publica uma vez, pelo MESMO código da restauração, sem
  amostrar.

### 7. Observabilidade

Famílias novas em `internal/metrics/names.go`, todas nascendo com zero, no
coletor no molde de `server/metrics_alarm.go`:

- por (dataset, counter, metric): `nishidb_counter_metric_groups`,
  `_heap_bytes`, `_disk_bytes` e `_resets_total`;
- por dataset: `nishidb_counter_metric_sample_seconds`, `_restored_groups` e
  `_sampling` (1 ativo, 0 parado);
- por (dataset, counter, alarm): `nishidb_alarm_disqualified_total`, subconjunto
  do `resolved_total`.

Inventário em `docs/metricas.md` (gate `internal/gates/metrics_test.go`).

### Fora de escopo (registrar no fim de `docs/debitos.md` ou em issue)

- Colunas de métrica na linha do `emit:` (`internal/series`).
- Coluna de métrica no `bind` do enriquecimento.
- Razão entre contadores (#61).
- Métrica cuja fonte não seja `total`.

## Blocos de commit (na voz do time, docs no mesmo commit do código)

1. `schema`: `metrics:` e `qualifyWhen`, com as recusas e a função única de colisão.
2. `metricwindow`: formato colunar em mmap, crescimento e golden de bytes.
3. `countermetrics`: amostrador, seqlock, restauração, órfãos e reset, gate de alocação.
4. `counters`: colunas em `_counters.<nome>` e `_meta.counter_metrics`, com as listas fixas atualizadas.
5. `alarm`: `qualifyWhen`, `disqualified`, evento e `_meta.alarms`, `alarms.state` v2.
6. `server`/`cli`: fiação, ordem de subida e parada, gate AST.
7. Métricas e `docs/metricas.md`.
8. Docs:
   - `architecture.md`: seção nova de métricas de janela e seção de alarmes atualizada;
   - README: YAML, colunas e lista de `_meta`;
   - `glossario.md`, com desambiguação: "agregado da janela" ≠ agregado do
     contador (`glossario.md:534`); "janela da métrica" ≠ janela do rowindex
     (`:95`); "qualificação do alarme" ≠ qualificador de coluna (`:451`); além de
     "balde", "amostra" e "desqualificado";
   - `docs/diagrams/counter_metrics_flow.puml` novo e `alarmes_flow.puml`
     atualizado, validados por `uml.nishisan.dev`;
   - `docs/aceite-metricas-de-janela.md`;
   - cópia deste plano em `planning/metricas-de-janela-e-alarme-qualificavel.md`.

Identificadores novos em inglês. Identificador PT-BR tocado (ex.:
`AvaliadorOfPayload`, `NovosSlots` em `internal/predicate`) é renomeado no mesmo
PR, com o par em `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`.

## Verificação

- Antes de cada commit: `make test` e `make test-race`; `make lint` com a
  baseline igualada nos dois sentidos; `make test-integration`.
- Clients: `make test-jdbc` e `make test-python-it`, com o corpus de
  `scripts/it-comum.sh` ganhando uma métrica.
- Conferir `git status` contra resíduo de mutação.

**Mutação obrigatória:** aplicar, ver vermelho, reverter, ver verde, relatar os dois.

| Mutação | Fica vermelho |
|---|---|
| Firing ignora `qualified` | Teste "OLT morta há mais de 24h resolve com `disqualified`" |
| Janela parcial devolve NULL | Teste de agregado com uma amostra só |
| Não descartar balde fora da janela | Restauração depois de parada maior que a janela |
| Remover a checagem de `keyHash` | Contador reconstruído com baldes velhos vazando para o grupo novo |
| `Stop` não grava o balde aberto | Reinício dentro do mesmo epoch |
| Aceitar `alarms.state` v1 | Teste de versão |
| Fingerprint sem a definição da métrica | Janela alterada no YAML preservando o cronômetro |
| Remover a recusa de colisão | Métrica ou coluna colidindo com `groupBy`/`total`/alarme |
| Escrever balde a cada passada, não no fechamento | Teste que conta escritas de coluna por passada |
| Publicar sem seqlock | Estresse concorrente com `min ≤ avg ≤ max` por linha lida, que só vale com leitor e amostrador de fato intercalados (medir quantas intercalações ocorreram) |
| Tirar `forCounterMetrics` de `encerra()` | Gate AST |

**Ponta a ponta** (teste de servidor com relógio injetado):
- dataset com contador por OLT e as duas métricas do exemplo;
- `SELECT groupKey, auth_5m_avg, auth_5m_avg AS x, auth_5m_avg FROM _counters.c`
  sai com os nomes `groupKey, auth_5m_avg, x, auth_5m_avg_1`;
- `_meta.counter_metrics` pelo JDBC e pelo Python;
- alarme sustenta, dispara, é desqualificado depois de 24h simuladas e resolve;
- reinício no meio de um alarme ativo, conferindo `auth_24h_max` restaurado.
