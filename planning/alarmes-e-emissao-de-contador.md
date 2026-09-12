# Issue #37 — Contadores que empurram: alarme por condição sustentada e emissão periódica por linha

## Contexto

Hoje os contadores só respondem quando perguntados: `_counters.<nome>` e
`_meta.counters` entregam os valores por SQL, e quem quer alarme mantém um laço
na aplicação consultando de tempos em tempos. A issue #37 pede o inverso —
**empurrar**:

1. **Alarme por condição sustentada.** Declarar no YAML que uma condição sobre
   uma linha de contador, valendo por um prazo mínimo, gera um aviso. Dispara
   uma vez quando a condição se firma, avisa de novo quando passa, e não repete
   no meio. A sustentação é o que separa alarme de rajada.
2. **Emissão periódica por linha.** A cada intervalo, escrever o valor de
   **cada grupo** individualmente (inclusive os com total zero, que são os que
   interessam) para alimentar gráfico por OLT.

O caso de uso é gerar eventos e séries a partir de contadores materializados,
que hoje só existem dentro do processo.

## Decisões tomadas no brainstorm

| Decisão | Escolha |
|---|---|
| Canal do alarme | **Log estruturado** agora; Kafka e webhook entram depois, pelo mesmo evento |
| Papel da consulta de enriquecimento | **Só enriquece, nunca barra** o alarme |
| Razão entre dois contadores | **Fora deste ciclo** — issue nova |
| Reinício com condição já valendo | **Persistir o "desde quando"** |
| Lacuna longa de processo parado | **Dispara, e o evento informa quanto tempo o processo esteve fora** |
| Rotação do arquivo de série | **Tamanho + quantos guardar**, declarados |
| Forma no YAML | **Aninhada dentro do contador** |

## O que já existe e será reusado (nada disso se reescreve)

| Peça | Onde | Para quê |
|---|---|---|
| Avaliador de predicado compartilhado | `internal/predicate` — `CompileCondition`, `NewAvaliadorOfPayload` | A condição do alarme. **Zero gramática nova**: é o mesmo código do `WHERE` e do `when:` do TTL |
| Tabela do contador gera payload de record real | `internal/counters/table.go:215` (`source.record`) | É sobre esse payload que o predicado roda — `total < 100` funciona hoje |
| Schema compilado da tabela do contador | `internal/counters/table.go:54` (`Table`) | Compilar a condição contra as colunas `groupKey`, as do `groupBy` e `total` |
| Iteração de grupos sem SQL | `counter.Limit()` + `counter.Group(id)` | Varredura: só `dict.mu.RLock` + atômicos, **nada do Store**, zero alocação (gate em `counter_gate_test.go:54`) |
| Bind de parâmetro posicional | `query.Engine.Execute(ctx, dataset, sql, params []types.Value)` | Enriquecimento **sem** interpolação de texto. O binder copia os bytes (`binder.go:184`) |
| Molde de laço periódico | `StartRetention`/`ForRetention`/`RetentionActive` (`internal/storage/retention.go:78`) | Ciclo de vida: primeira passada imediata, medidor atômico, cancela-e-aguarda |
| Máquina de estado pura fora do laço | `vigilante.evaluate` (`internal/kafka/watchdog.go:139`) | Decidir transição sem I/O, testável direto |
| Estado durável fora do dataset | `checkpoint/offsets.go` + `writeAtomic` (`manifest.go:679`) | Formato do estado de alarme: magic+version+crc32c, temp→fsync→rename→dirsync |
| Tri-estado na carga | `Store.loadOffsets` (`store.go:1354`) | ausente ≠ ilegível ≠ válido; nenhum impede a subida |
| Arquivo auxiliar no dataset | `internal/query/spill` | Criação, permissão, limpeza na subida, remoção idempotente |
| Rotação por tamanho | `segment.Writer.Append` (`segment.go:198`) | Selar e abrir o próximo ao cruzar o teto |
| Serialização NDJSON sem `fmt`/`encoding/json` | `executor.AppendValue`, `executor.AppendJSONString` | Linha da série, mantendo zero alocação |
| Parsing de duração | `units.Duration` / `units.DurationWithDays` | `sustainedFor`, `every` |
| Whitelist de chave YAML | `schema.block()` (`source.go:232`) | Bloco novo recusa chave desconhecida |

## Desenho

### Configuração (contrato novo)

```yaml
counters:
  - name: authenticated_per_olt
    groupBy: [olt]
    where: authenticated = true

    alarms:
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
        enrich:
          - sql: "SELECT id, ip, model FROM inventory WHERE olt = ? LIMIT 5"
            bind: [olt]

    emit:
      every: 30s
      file: series/authenticated.ndjson
      maxBytes: 64MiB
      keep: 5
```

`sustainedFor` em vez de `for`: segue `expireAfterWrite` / `retainEmptyGroups`,
que nomeiam o que a duração mede. Sem release publicada, o contrato ainda é
livre (memória: quebra de contrato pré-autorizada até a release) — então vale
escolher o nome certo agora.

**Recusas na subida** (`internal/schema/counter.go`, ao lado de
`validateCounters`; nunca arredondar nem cair no default em silêncio):

- `alarms` vazio; `name` fora de `[a-z0-9_]+`; `name` repetido no mesmo contador
- `when` ausente ou em branco; `sustainedFor` ausente, zero explícito, abaixo de
  `ExpiryResolution`, ou não parseável
- `enrich[].sql` vazio; `bind` com nome que não é coluna do `groupBy` nem
  `total`; contagem de `?` diferente de `len(bind)`; `LIMIT` literal ausente ou
  acima do teto (`LIMIT ?` é recusado pelo parser de propósito — `parser.go:725`)
- `emit.every` / `maxBytes` / `keep` fora de faixa; `file` com `..`, absoluto,
  ou colidindo entre dois contadores

Sentinelas próprias `schema.ErrInvalidAlarm` e `schema.ErrInvalidEmit`,
registradas em `errorCode` (`internal/cli/cli.go:216`) → **ExitDataset (3)**.
Sem isso caem no default 1 e mandam o operador olhar a linha de comando quando
o problema está no YAML — é o defeito que `ErrInvalidIndexWindow` já documenta.

### Parte 1 — alarme

**Pacote novo `internal/alarm`.** Mora fora de `internal/counters` e de
`internal/query` porque precisa dos dois — mesmo argumento de
`internal/counters` e `internal/metakafka`.

**Compilação (subida).** Para cada alarme: pega o `*schema.Compiled` que
`counters.Table` já produz para aquele contador e compila `when` por
`predicate.CompileCondition("counters[i].alarms[j].when", …)`. O enriquecimento
é parseado e bindado contra o dataset-alvo **na subida** — erro de nome de campo
é recusado quando o processo sobe, não descoberto de madrugada quando o alarme
dispara. Roda depois de `counters.Register` (`server.go:458`), que é quando o
Engine conhece as tabelas.

**Máquina de estado — função pura, sem I/O** (molde do `vigilante`):

```
absent  --condição verdadeira-->        pending(since)
pending --prazo cumprido-->             firing   ⇒ evento FIRING
pending --condição falsa-->             absent   (nenhum evento: nunca disparou)
firing  --condição falsa-->             absent   ⇒ evento RESOLVED(condition_cleared)
firing  --grupo aposentado-->           absent   ⇒ evento RESOLVED(group_retired)
```

O último caso é o que impede alarme eterno: grupo aposentado sai do universo, e
sem essa cláusula o estado ficaria ativo para sempre em memória e em disco.

**Identidade de um alarme ativo: `(counter, alarm, chave canônica em bytes)`** —
a chave composta do `valuekey`, **não** o `groupKey`. O `groupKey` é texto
derivado do `keyFormat`, e o `keyFormat` pode mudar sem reconstruir o contador
(`counter.go:60-63`); usá-lo como identidade faria uma troca de molde perder
todo o estado de alarme em silêncio. O `groupKey` vai no **evento**, que é onde
ele serve.

**Laço** (molde `StartX`/`ForX`/`XActive`, com `alarmMu`/`alarmCancel`/
`alarmActive`/`alarmInVoo`): cadência derivada do menor `sustainedFor`
declarado dividido por 4, com piso — é o cálculo do `watchdogInterval`
(`watchdog.go:39`), pelo mesmo motivo: amostrar mais fino que o prazo que se
quer medir. Não sobe goroutine se nenhum contador declara alarme. A passada
inteira é exposta como método público (`EvaluateOnce`) para o teste chamá-la
direto, como `counters_retirement_test.go:31` faz com `AposentaGroupsVazios`.

**Ciclo de vida — a parte perigosa.** `Store.Counters()` lê o campo direto, sem
`entra()`/`sai()`: não guarda contra `Close()` concorrente. Como o laço vive no
servidor e não no Store, o servidor tem de **parar o alarme antes de
`closeDatasets()`** (`server.go:923`). Invariante nova, com teste sob `-race`.

**Estado durável** — `internal/storage/checkpoint/alarms.go`, ao lado de
`offsets.go`, reusando `writeAtomic`. Arquivo `<dataset>/checkpoint/alarms.state`.

- Guarda, por alarme ativo: chave canônica, estado, `since`. No cabeçalho:
  `savedAt`, e por alarme o par `(when, sustainedFor)` com que o estado foi
  gravado.
- **Escrito nas transições, não a cada passada** — volume baixo por construção.
- **Não entra na ordem do passo 8** (`manifest → offsets`). Justificativa a
  escrever no godoc: este arquivo não descreve posição de consumo nem dado
  materializado; adiantá-lo ou atrasá-lo só muda quando um alarme dispara,
  nunca a integridade de nada. Sem isso, um leitor futuro presume que a ordem
  do §48 se aplica aqui.
- **Carga na subida, tri-estado**: ausente (normal, primeira subida) /
  ilegível (WARN `alarm.state_unreadable`, cronômetros do zero, **subida não é
  impedida**, doutrina do §51) / válido.
- `(when, sustainedFor)` divergente do YAML **invalida o estado daquele
  alarme** — prazo antigo não pode assombrar condição nova.
- Restaurado e condição ainda valendo ⇒ dispara com `restored: true` e
  `downtimeMs` = subida − `savedAt`. Restaurado e condição **não** mais
  valendo ⇒ estado descartado em silêncio: nunca houve disparo a resolver.

**Enriquecimento.** No disparo (nunca na passada de avaliação), fora de
qualquer lock do Store: `eng.Execute` com os valores do grupo ligados como
parâmetros, deadline próprio e teto de linhas do `LIMIT` literal. Falha, timeout
ou resultado vazio ⇒ **o evento sai sem aquele enriquecimento**, com o motivo
num campo e um contador incrementado. Nunca barra o alarme — é a decisão
fechada, e o modo de falha certo: alarme que desaparece porque uma consulta
auxiliar falhou é o pior defeito possível neste subsistema.

Teto de enriquecimentos por passada (declarável, default baixo): o excedente sai
**sem** enriquecimento mais `alarm_enrich_skipped_total`. Milhares de grupos
colapsando de uma vez não podem virar milhares de consultas. `Engine.Execute`
reparseia e replaneja a cada chamada — não há cache de plano —, e por isso o
custo por disparo não é desprezível.

**Evento e canal.** Um tipo de evento, um `Sink` com uma implementação (log).
Kafka e webhook depois publicam **o mesmo evento**, não uma segunda versão dele.

> **Armadilha descoberta no mapeamento, e ela vaza segredo:** o handler de log
> redige automaticamente chaves chamadas `value`, `record`, `payload`, `body`,
> `token`, `senha` e outras (`logging.go:63`), mas a redação **não entra dentro**
> de um valor serializado por `slog.Any`. Ou seja: o campo do enriquecimento não
> pode se chamar `record`/`value` (sairia `[REDACTADO]` inteiro), **e** uma
> coluna enriquecida chamada `password` sairia em claro. Correção: exportar de
> `internal/logging` um `IsSensitiveKey(string) bool` e aplicá-lo ao montar o
> enriquecimento — uma lista, um lugar. Duplicá-la seria a divergência que a
> casa proíbe.

Ordem de emissão determinística (alarme, depois chave) para o log ser diffável —
mesmo argumento da ordem de partições em `offsets.go:129`.

**`_meta.alarms`** — tabela virtual pelo registro que já existe
(`Engine.RegisterVirtual`), uma linha por alarme ativo: contador, alarme,
`groupKey`, estado, `since`, `firedAt`. É o que permite inspecionar o estado
corrente sem ler log, e o padrão já está pronto em `internal/metakafka`.

### Parte 2 — emissão periódica por linha

**Pacote novo `internal/series`.** Dois laços separados do alarme, de propósito:
intervalos independentes e falhas independentes — emissão parada não pode parar
alarme.

**Escritor com rotação.** Ao cruzar `maxBytes`: fecha, desloca `file.N-1 →
file.N`, `file → file.1`, apaga acima de `keep`. Deslocamento numérico em vez de
nome com timestamp porque é o que coletor de log entende. **Sem fsync** — é
telemetria, e um crash perder a cauda é o desfecho certo (mesmo argumento do
`spill.go:10`). Permissão `0644`, não `0600` do spill: aqui o coletor precisa
ler. Resíduo acima de `keep` limpo na subida, depois do flock.

**Linha** (uma por grupo por intervalo, montada com `executor.AppendValue` e
`AppendJSONString`, zero alocação com gate): `counter`, `groupKey`, as colunas
do `groupBy` abertas, `total`, `at`. Grupo com total zero **sai** — é o caso que
motiva a feature.

A varredura usa `Limit()`/`Group(id)`, que não toca o writer. A regra da issue
("a emissão não pode segurar o writer") sai satisfeita por construção, não por
cuidado.

### Métricas

Famílias novas em `internal/metrics/names.go`, labels `dataset`, `counter`,
`alarm` — todos vindos do YAML, nunca de entrada de cliente, como `LabelCounter`
já faz. **Todas nascem com zero na subida** (série que alarma não pode nascer no
primeiro alarme): declarados, ativos, disparos, resoluções, falhas e pulos de
enriquecimento, duração da passada, estado restaurado; linhas emitidas,
rotações, bytes, último sucesso, erros. `/v1/stats` lê as **mesmas** estruturas
— nunca um segundo contador para a rota.

## Blocos de commit

Cada bloco é uma responsabilidade lógica, na voz do time:

1. `schema`: `alarms:` e `emit:` — parse, whitelist, recusas, sentinelas
2. `internal/alarm`: máquina de estado pura + testes sem I/O
3. `checkpoint`: formato e carga do `alarms.state`, com golden de bytes
4. `internal/alarm`: compilação de condição e de enriquecimento na subida
5. `internal/alarm`: laço, evento, sink de log, integração e ordem de parada
6. `internal/alarm`: execução do enriquecimento com teto e degradação
7. `internal/logging`: `IsSensitiveKey` exportada e aplicada no enriquecimento
8. `internal/series`: escritor NDJSON com rotação e limpeza
9. `internal/series`: laço de emissão e integração
10. métricas, `/v1/stats`, séries nascendo com zero
11. `_meta.alarms`
12. docs, diagrama PlantUML, README, glossário

**É um ciclo grande.** Ponto de parada natural e útil: a **parte 1 completa**
(blocos 1–7, 10, 11 e a doc correspondente) já entrega o alarme ponta a ponta;
a parte 2 (8, 9) é independente e pode virar o PR seguinte sem nada pela metade.

**Idioma:** todo identificador novo nasce em inglês; mensagens e godoc em PT-BR.
Se um bloco tocar identificador PT-BR existente (`internal/storage/counters.go`
é o mais carregado), o rename entra no mesmo PR e o par vai para
`planning/identificadores-em-ingles/mapeamento-2-manual.tsv`.

## Verificação

`make test` e `make test-race` antes de cada commit; `make lint` com a baseline
igualada nos dois sentidos. Gate de alocação (`AllocsPerRun`) para a linha da
série e para a passada de avaliação.

**Mutação obrigatória por invariante** — aplicar, ver vermelho, reverter, ver
verde, relatar os dois (a doutrina existe porque o ciclo do índice por segmento
pagou vinte vezes por ignorá-la):

| Mutação | O que tem de ficar vermelho |
|---|---|
| Ignorar `sustainedFor` e disparar na primeira passada | Teste de sustentação |
| Aceitar estado salvo com `(when, sustainedFor)` divergente | Teste de prazo alterado no YAML |
| Usar `groupKey` como identidade do estado | Teste que troca `keyFormat` e exige o estado preservado |
| Fazer erro de enriquecimento barrar o alarme | Teste "enriquecimento nunca decide" |
| Interpolar o valor no SQL em vez de ligar parâmetro | Teste com valor de grupo contendo `'` e `|` |
| Remover a cláusula de grupo aposentado | Teste de alarme que resolve ao grupo sair |
| Rotacionar só acima de `maxBytes + 1` | Teste de teto de disco (`maxBytes × keep`) |
| Emitir o enriquecimento sob a chave `record` | Teste que prova que o campo chega ao writer |
| Tirar a parada do laço antes de `closeDatasets` | Teste sob `-race` (use-after-close) |

**Ponta a ponta**, com o binário real: dataset com contador por OLT, alarme de
`total < 100` por 2m e emissão de 30s; relógio injetado (`UsaRelogio`) para
avançar sem dormir; conferir evento de disparo, evento de resolução, ausência de
repetição no meio, arquivo de série rotacionando e o `_meta.alarms`. Reinício no
meio de um alarme ativo, conferindo `restored` e `downtimeMs`.

Docs: `docs/` atualizado no mesmo commit do código, diagrama novo em
`docs/diagrams/` validado pelo servidor `uml.nishisan.dev` (não há `plantuml`
local), e os gates de citação (`§NN`, evidências citadas) verdes.

## Fora de escopo, e registrado

- **Razão entre dois contadores** (`autenticados ÷ serviços` da mesma OLT) —
  issue nova. O desenho aninhado dentro do contador não acomoda um alarme que
  cite dois contadores; quando a razão entrar, ela precisará de lugar próprio.
  Vale abrir a issue citando essa restrição.
- **Kafka e webhook** como canal — o ponto de extensão (`Sink` + evento único)
  fica pronto, as implementações não entram.
- Emissão do agregado global do contador (`rows`/`total` da issue #36).
