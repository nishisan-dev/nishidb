# Issue #67 — a linha do `emit:` leva as colunas das métricas de janela

## Context

O PR #66 (v0.9.0, merge `21af36c`) entregou as métricas de janela do contador:
`metrics:` no YAML, amostrador em `internal/countermetrics`, formato em disco em
`internal/storage/metricwindow`, e as colunas `<metric>_<aggregate>`
(`auth_5m_avg`, `auth_24h_max`…) servidas por `_counters.<nome>`.

A emissão periódica ficou de fora por escopo e virou o **débito 124**
(`docs/debitos.md:354`). Hoje `internal/series` escreve por grupo vivo
`counter`, `groupKey`, as colunas do `groupBy`, `total` e `at` — o valor
**instantâneo**. Quem quer o agregado da janela precisa sair da série e
consultar `_counters.<nome>` por SQL, o que quebra o caso de uso da série:
alimentar uma ferramenta de gráfico com um arquivo NDJSON por contador.

Esta issue fecha o débito: a linha passa a levar, ao lado do `total`, as colunas
geradas pelas métricas de janela daquele contador.

**Estado do repositório:** o `main` local está em `39a43da` (PR #65) e o
`origin/main` está em `21af36c` (PR #66). **O primeiro passo é `git pull`** —
nada abaixo existe na árvore local.

## Decisões de contrato (respondidas pelo usuário)

1. **Ausência sai como `null` explícito.** A chave da coluna está sempre na
   linha; sem amostra na janela (`samples == 0`) o valor é `null`. É o que
   `executor.AppendRow` já faz para coluna nula, então a série passa a casar,
   campo a campo, com o que um `SELECT * FROM _counters.<nome>` devolve.
2. **Sem `sampledAt`.** A linha mantém só o `at` da emissão. A defasagem entre
   o instante da amostra e o da emissão é de no máximo um intervalo (a
   amostragem é `clamp(janela÷60, 1s, 10s)` e o `every` mínimo é 10s) e fica
   documentada, não emitida.
3. **Sem chave nova no `emit:`.** As colunas entram sempre que o contador
   declara `metrics:`. Quem não declara tem a linha inalterada byte a byte.

Corolário da decisão 1, alinhado ao contrato de `counters.MetricSource`
(`internal/counters/table.go:85`): **o formato da linha é função apenas do
YAML**, nunca de o amostrador estar de pé. Sem fonte de métrica, as colunas
continuam na linha, todas `null` — o mesmo que a tabela faz com `src == nil`.

## Desenho

### 1. `internal/series` — a interface e o pré-cálculo

`internal/series/emitter.go` declara a interface mínima **no consumidor**, pelo
mesmo motivo já documentado no `Reader` existente (`emitter.go:17-26`) — a
emissão não avalia condição, então não precisa do `Usage()` que a tabela pede:

```go
// MetricReader lê o valor publicado de UMA métrica de janela.
type MetricReader interface {
    Aggregate(id uint32) (samples uint64, avg float64, minimum, maximum int64)
}

// Metric é uma métrica de janela e os agregados que ela gera na linha.
// Reader nil é válido: as colunas saem, todas null.
type Metric struct {
    Name       string
    Aggregates []string // ordem declarada
    Reader     MetricReader
}
```

`counters.MetricReader` (`internal/counters/table.go:56`) tem `Aggregate` **e**
`Usage`, então um valor dele é atribuível a `series.MetricReader` sem conversão
nem import — exatamente como `counters.Reader` já é atribuído a `series.Reader`
em `openEmitters` hoje.

`Options` ganha `Metrics []Metric`.

`NewEmitter` pré-calcula, na ordem declarada, um prefixo escapado por coluna —
o mesmo tratamento que `prefix`/`colPrefix` já recebem (`emitter.go:95-102`),
que é o que mantém o gate de zero alocação verde:

```go
type metricColumn struct {
    prefix []byte    // `,"auth_5m_avg":` já escapado
    agg    aggregate // enum local, para a linha sair por switch e não por texto
}
type metricBinding struct {
    reader  MetricReader
    columns []metricColumn
}
```

O nome da coluna vem de **`schema.MetricColumnName(m.Name, a)`**
(`internal/schema/counter_metric.go:87`) e o agregado das constantes
`schema.AggregateAvg/Min/Max` — nunca de uma segunda composição local. Isso faz
`internal/series` passar a importar `internal/schema`; **verificado que não há
ciclo**: `internal/schema` importa apenas `units`, `keyformat`, `typeparse` e
`types`, e nenhum deles alcança `internal/series`.

O enum `aggregate` e o `aggregateOf` local espelham
`internal/counters/table.go:233-253`. É duplicação deliberada de três constantes
(não do nome da coluna, que tem dono único) — a alternativa seria exportar o
enum de `counters` e criar a aresta `series → counters` que o pacote foi
desenhado para não ter.

### 2. `appendLine` — as colunas na linha

`appendLine` (`emitter.go:160`) passa a receber o `id` do grupo (é a mesma
chave que `Aggregate` usa — `counters/table.go:449` a chama com o id do grupo) e
emite as colunas **entre `total` e `at`**, para que `at` continue sendo o último
campo e a linha de hoje seja um prefixo da linha nova:

```go
for _, m := range e.metrics {
    var samples uint64
    var avg float64
    var lowest, highest int64
    if m.reader != nil {
        samples, avg, lowest, highest = m.reader.Aggregate(id)
    }
    for _, c := range m.columns {
        dst = append(dst, c.prefix...)
        if samples == 0 {
            dst = append(dst, "null"...)
            continue
        }
        switch c.agg {
        case aggregateAvg:
            dst = executor.AppendValue(dst, types.Value{
                Type: types.TypeDouble, I64: int64(math.Float64bits(avg))})
        case aggregateMin:
            dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: lowest})
        case aggregateMax:
            dst = executor.AppendValue(dst, types.Value{Type: types.TypeInt64, I64: highest})
        }
    }
}
```

`executor.AppendValue` é o serializador canônico já usado pela linha, e trata
double não-finito como `null` (`internal/query/executor/ndjson.go:70-76`) — a
mesma política da tabela, sem conferência por linha.

### 3. Fiação no servidor

`openEmitters` (`internal/server/server.go:1275`) passa a receber a
`counters.MetricSource` e a montar `series.Metric` por métrica declarada,
recusando na subida a métrica que a fonte não resolve — a mesma regra de
`bindMetrics` (`internal/counters/table.go:280`), e pelo mesmo motivo: calar uma
métrica declarada seria pior que não subir. Mensagem nova em **inglês**, no
molde da de `counters`.

Com `src == nil` (nenhum contador do dataset declara `metrics:`), `Reader` fica
nil e as colunas saem `null` — o caminho que sustenta a decisão de contrato 3.

A ordem de subida já é a certa e não muda: o amostrador abre em
`server.go:520`, antes de `counters.Register` (`:530`) e de `openEmitters`
(`:550`).

### 4. Documentação

- `docs/debitos.md:354` — item 124 marcado FECHADO no padrão da própria tabela
  ("Sem milestone"): título em `~~riscado~~` seguido de `— **FECHADO em ...**`
  com o apontamento de onde fechou (exemplos no arquivo: itens 28, 44 e 91). A
  numeração não muda.
- `docs/architecture.md`, seção "Alarmes e emissão periódica de contador"
  (cabeçalho `:2828`, parágrafo da emissão `:2992-3001`) — a linha passa a levar
  as colunas de janela; `null` é ausência de amostra; a defasagem de até um
  intervalo entre amostra e emissão; e o efeito no orçamento de disco
  (`maxBytes × (keep+1)` passa a guardar menos histórico para quem declara
  `metrics:` junto com `emit:`).
- `README.md`, seção "Emissão periódica: a série de cada grupo" (`:1745`) — o
  exemplo da linha (`:1763-1768`) ganha as colunas, e a nota cruzada na seção
  "Métricas de janela do contador" (`:1537`).
- `docs/glossario.md` já tem "janela → window" (`:95`), "emissão → emission /
  emit" (`:553`) e "série → series" (`:554`). Nada a acrescentar.
- Nenhum diagrama cobre a emissão hoje (`alarmes_flow.puml` é só do alarme), e
  esta mudança não cria estrutura nova — sem `.puml`.

### Idioma

`internal/series` já tem identificadores em inglês; **não há rename a fazer**,
logo nada entra em `mapeamento-2-manual.tsv`. Os arquivos novos de teste nascem
em inglês, no precedente do PR #66 (`table_metrics_test.go`,
`TestCounterTableDoesNotAllocatePerRowWithMetrics`) — por isso vão em arquivos
próprios, e não dentro dos arquivos PT-BR existentes, que ficam intocados.

## Blocos de commit

**0. `git pull`** e branch `feature/colunas-de-janela-no-emit`.

**1. `series`: a linha leva as colunas das métricas de janela**
`internal/series/emitter.go` — `MetricReader`, `Metric`, `Options.Metrics`,
pré-cálculo em `NewEmitter`, `appendLine` com o `id`.
Teste novo `internal/series/emitter_metrics_test.go` (inglês), com um
`fakeMetricReader`/`fakeMetricSource` no molde de
`internal/counters/table_metrics_test.go:29-53`, cobrindo: colunas presentes na
ordem declarada; grupo sem amostra com `null`; `Reader` nil com todas as colunas
`null`; `avg` como número fracionário e `min`/`max` como inteiros; contador sem
`metrics:` com a linha byte a byte igual à de hoje.

**2. `series`: o gate de alocação exerce as colunas de janela**
`internal/series/emitter_metrics_gate_test.go` (`//go:build !race`) e a
contraparte `..._gate_race_test.go` com `t.Skip` explícito, no molde de
`internal/counters/table_gate_race_test.go`. O gate mede `EmitOnce` sobre 64
grupos **com** duas métricas ligadas e um grupo sem amostra, exigindo zero
alocação. O gate existente (`emitter_gate_test.go`, PT-BR) não é tocado.

**3. `server`: o emissor recebe a fonte das métricas**
`openEmitters` com a `counters.MetricSource`, recusa da métrica não resolvida,
e teste de servidor E2E que sobe um dataset com `metrics:` + `emit:` e lê o
arquivo da série conferindo as colunas.

**4. `docs`: o débito 124 fecha**
`docs/debitos.md`, `docs/architecture.md`, `README.md`.

## Verificação

Toda execução com o toolchain do CI, nunca o `go` do snap:

```bash
cd /home/lucas/Projects/nishisan/nishidb && GOTOOLCHAIN=go1.26.5 GOPROXY=off make test
```

```bash
cd /home/lucas/Projects/nishisan/nishidb && GOTOOLCHAIN=go1.26.5 GOPROXY=off make test-race
```

```bash
cd /home/lucas/Projects/nishisan/nishidb && GOTOOLCHAIN=go1.26.5 GOPROXY=off make lint
```

Os três são obrigatórios e nenhum substitui o outro: `make test` roda os gates
de medida (que são `//go:build !race`), `make test-race` roda o detector com
esses gates em SKIP. O `make lint` é ratchet bidirecional — a contagem tem de
bater com `.golangci-baseline` (sair != 0 enquanto a baseline > 0 é o esperado).

**Prova por mutação** (doutrina de verificação — teste que passa com o defeito
presente não prova nada). Para cada item: aplicar, ver vermelho, reverter, ver
verde, e relatar os dois:

1. Trocar o `null` de `samples == 0` por `0` → o teste de ausência tem de
   reprovar.
2. Trocar o prefixo pré-calculado por uma composição por linha
   (`[]byte(",\""+name+"\":")`) → o gate de alocação tem de reprovar.
3. Emitir os agregados na ordem inversa à declarada → o teste de ordem tem de
   reprovar.
4. Fazer `openEmitters` ignorar a métrica não resolvida em vez de recusar → o
   teste de servidor tem de reprovar.

**Aferição manual** do arquivo da série, com um dataset que declare `metrics:` e
`emit:`, conferindo que a linha traz `auth_5m_avg` ao lado de `total` e que um
grupo recém-nascido sai com `null` antes da primeira amostra.

## Fora de escopo

- As 16 mensagens de erro em PT-BR de `internal/series` (`emitter.go:83,86,164`
  e `writer.go`). A regra de migração cobre **identificador**, e o pacote já está
  em inglês; abrir a frente das mensagens aqui misturaria duas coisas no mesmo
  PR. As mensagens **novas** deste trabalho nascem em inglês.
- Débito 125 (o `bind` do enriquecimento não liga coluna de métrica de janela) e
  débito 126 (a métrica de janela só tem a fonte `total`).
- Métrica Prometheus nova: `nishidb_series_bytes` e
  `nishidb_series_rotations_total` já medem o crescimento do arquivo e a
  aceleração do giro.
