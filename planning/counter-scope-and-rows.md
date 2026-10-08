# Escopo do contador, coluna `rows` e comparação linear no alarme (N1, issue #61)

Pedido da integração service-view → `service_state` (30/09/2026). Decisão do
usuário: "contador com 2 agregados + comparação linear inteira entre colunas da
mesma linha". Forma concreta revisada e aprovada pelo consumidor (agente do
service-view) em 30/09/2026. Bloqueia o go do shadow.

**Revisão 2** — incorpora a refutação do plano (15 objeções, seção
"Decisões da refutação" no fim). Onde as seções A–C e a seção de decisões
divergirem, VALE A SEÇÃO DE DECISÕES.

## O achado que dimensiona o bloco

O contador JÁ mantém dois números por grupo (`internal/storage/counter/counter.go`):
`refs[g]` — linhas no grupo, independente do `where` — e `totals[g]` — as que
têm o bit de condição. O vetor por linha (4 B: 31 bits de id + 1 bit de
condição) não muda. Ambos são refeitos na abertura (`Fold`) e reconciliados
pela auditoria. `refs` só não é exposto por grupo (todos os chamadores de
`Group()` o descartam). Logo, o "segundo agregado" é EXPOR `refs`, e o filtro
de membro é uma chave nova.

## Contrato (YAML) aprovado

```yaml
counters:
  - name: spl1
    groupBy: [olt, idPrimary]
    keyFormat: "OLT_SPL_1.{olt}.{idPrimary}"
    scope: "eligible = true AND authenticated IS NOT NULL"   # quem é membro -> rows
    where: "authenticated = true"                             # quem conta -> total
    maxGroups: 1200000
    alarms:
      - name: spl1_auth_drop
        when: "total * 10 <= rows"
        qualifyWhen: "rows >= 6"
        sustainedFor: 2m
        notify: [service_view]
```

## Peças

### A. `scope:` (schema + storage)
- Chave opcional nova no item de `counters:` (`internal/schema/counter.go`,
  `rawCounter`/`CounterSpec`/`validateCounters`/`block()`), compilada com a
  MESMA gramática e o MESMO avaliador do `where` (`internal/predicate`), contra o
  schema do dataset. Erro de compilação recusa a subida nomeando o contador.
- Semântica: avaliada sobre o payload PÓS-merge, no mesmo ponto do `where`
  (`Derive`, `counter.go:~332-371`), ANTES de devolver `hasGroup=true`. Linha
  fora do escopo sai `hasGroup=false` — idêntica a "coluna do groupBy ausente":
  não interna grupo, não conta em `refs` nem em `totals`, entra no mesmo caminho
  de decremento/`emptySince`/aposentadoria. O `Deriver` da auditoria
  (`NewDeriver`) carrega o MESMO avaliador.
- Identidade persistida: o scope decide o CONTEÚDO do vetor, então mudar o
  scope tem de forçar reconstrução, exatamente como mudar o `where`
  (`sameDefinition`, `internal/storage/counters.go:~186`). **Rollback para um
  binário antigo também tem de reconstruir**, nunca adotar um vetor construído
  com scope. Decisão de formato: NÃO criar seção nova no manifest. A identidade
  textual gravada em `CounterInfo.Where` passa a ser a forma normalizada que
  combina os dois quando há scope (por exemplo `SCOPE(<scope>) WHERE(<where>)` —
  o Builder escolhe a forma, documenta no godoc de `CounterInfo` e garante que
  NENHUM `where` sozinho normalizado possa produzir o mesmo texto). Sem scope, o
  texto continua byte a byte o de hoje (dataset existente não reconstrói ao
  trocar de binário). Um binário antigo lê o texto combinado, compara com o seu
  `where` e reconstrói. O Refuter vai conferir essas três afirmações.
- `_meta.counters` ganha a coluna `scope` (texto normalizado; NULL sem scope),
  ao lado de `where`. `describeCounters` (log `contadores.abertos`) mostra o
  scope.

### B. Coluna `rows` por grupo
- `_counters.<nome>`: coluna `rows` int64, logo depois de `total`
  (`internal/counters/table.go` `TableSchema` e `source.record`; o
  `maxRowColumns` sobe 1). Valor = `refs[g]`, lido na MESMA chamada `Group()`
  que dá o `total`. Nunca NULL (mesmo com `total: false`).
- `rows` entra em `schema.CounterColumns` como nome reservado (groupBy/metric/
  measure chamado `rows` passa a ser recusado com mensagem nomeando o
  conflito) e na lista reservada do Kafka se aplicável.
- Alarme: `rows` visível em `when`/`qualifyWhen`/`value:`/`enrich.bind`; o
  evento (`internal/alarm/event.go`, `sink.go`, `runner.go`) ganha `Rows`, que
  sai como chave `rows` no corpo de `alarm.firing` E `alarm.resolved` (e no
  log), igual ao `total`. Para grupo aposentado sai 0, como o total.
- Emissão periódica e destino Kafka dos contadores (`internal/series/`): a linha
  emitida leva `rows`. Seguir o mesmo caminho do `total`.
- A doc diz explicitamente: `rows` do contador = linhas DO GRUPO (membros pelo
  scope com o groupBy completo), não linhas do dataset.

### C. Comparação linear inteira entre colunas (só alarme)
- Forma aceita em `when`/`qualifyWhen` de alarme:
  `<col> [* <int>] <op> <col> [* <int>]`, `<op>` ∈ `= != < <= > >=`,
  combinável com AND/OR/parênteses e com os termos `coluna <op> literal` de
  hoje. Multiplicador: literal inteiro positivo ≥ 1 (recusar 0, negativo,
  float). Só colunas int64 (`total`, `rows`, colunas int64 do groupBy, min/max
  de janela); coluna double ou string recusada na SUBIDA com mensagem nomeando a
  coluna e o tipo.
- O WHERE do SQL, o `where`/`scope` do contador e o `when` do TTL NÃO ganham
  essa forma: o parser continua recusando coluna à direita fora do modo alarme
  ("comparação entre colunas não é suportada na v1"). Implementar como opção
  explícita do parse/bind (no molde de `refusesParams`), ligada só pelo
  `compileAlarm`.
- Avaliação exata, sem float e sem overflow silencioso: produto em 128 bits
  (`math/bits`) ou equivalente provado. Coluna NULL/ausente → termo FALSE para
  qualquer operador (regra existente).
- Zero alocação por avaliação (há gates de alocação no caminho do alarme; o
  caminho novo pede gate NOVO ao lado dos velhos, com mutante que aloque de
  verdade, ex.: `fmt.Sprintf`).

## Garantias — cada uma com teste que fica VERMELHO com a mutação indicada

1. Linha fora do scope não interna grupo nem conta (`refs`, `totals`, `Limit`
   inalterados). Mutação: ignorar o scope no `Derive`.
2. Transição dentro→fora (patch que torna `eligible=false`; e expiração por TTL)
   decrementa `rows` e, se havia bit, `total` do grupo antigo na MESMA
   aplicação. Mutação: não decrementar o lado antigo quando sai por scope.
3. Scope sobre estado PÓS-merge: patch sem `authenticated` sobre linha com
   `authenticated=true` mantém a linha no escopo. Mutação: avaliar o scope
   sobre o patch cru.
4. Replay/recovery chega ao mesmo vetor (crash no meio, reabertura): os
   contadores pós-recovery batem com os de uma aplicação contínua, e a
   auditoria dá zero divergência. Mutação: Deriver da auditoria sem o scope →
   auditoria acusa divergência.
5. Mudar o scope no YAML reconstrói; manter, não reconstrói. Sem scope, o texto
   de identidade é byte a byte o atual (golden do manifest de um dataset
   pré-existente abre sem reconstruir). Mutação: excluir o scope da
   identidade.
6. Rollback: um manifest gravado com scope, lido por uma definição sem scope e
   mesmo `where` (simula o binário antigo), não é adotado — reconstrói.
   Mutação: identidade combinada que reduz ao `where` quando ele é igual.
7. `rows` na tabela = `refs` do grupo, por SQL (`SELECT groupKey, total, rows
   FROM _counters.x`), incluindo `total: false` (rows presente, total NULL).
   Gates de alocação existentes da tabela continuam verdes e cobrem a coluna
   nova (mutante que aloca na montagem do `rows` → vermelho).
8. `when: "total * 10 <= rows"` dispara exatamente na fronteira: com rows=10,
   total=1 vale; total=2 não vale; rows=0,total=0 vale. Overflow:
   `total * 4611686018427387904 <= rows` com valores grandes decide certo
   (mutação: multiplicar em int64 → vermelho). NULL → FALSE.
9. Recusas na subida, cada uma asseverando a MENSAGEM PRÓPRIA da guarda nova:
   coluna à direita no WHERE do SQL continua recusada; no `where`/`scope` do
   contador também; no alarme, coluna double recusada; multiplicador 0/negativo/
   float recusado; `rows` como nome de groupBy recusado.
10. Corpo do webhook/log de `alarm.firing` e `alarm.resolved` traz `rows` e
    `total` (teste no formato do existente de projeção compartilhada
    log/JSON). Mutação: omitir `rows` no resolved.
11. Emissão periódica/Kafka leva `rows`.
12. Ponta a ponta: dataset com scope+where+alarme `total * 10 <= rows` +
    `qualifyWhen: rows >= 6`, eventos de ingestão levando um grupo de 10
    membros a 1 autenticado → firing com `total=1, rows=10` no corpo; voltando
    a 2 → resolved `condition_cleared`; `eligible=false` em massa → resolved
    `disqualified`.

## Docs (mesmo bloco)
README (seção de contadores e de alarmes: `scope`, `rows`, comparação linear,
exemplo SPL), `docs/architecture.md` se descrever o vetor/contador, aceite novo
`docs/aceite-escopo-do-contador.md` (rodada, testes, mutações vermelho/verde,
custo medido do scope por evento), `docs/debitos.md` só se algo ficar
para depois (item novo no FIM). Glossário: o orquestrador edita; o Builder
lista os termos.

## Decisões da refutação (normativas; prevalecem sobre A–C)

R1. **Identidade NÃO mora em `Definition.Where`.** `def.Where` fica intocado
(alimenta a coluna `condition` de `_meta.counters`, `describeCounters`, a
mensagem de reconstrução, a impressão digital de contexto do alarme e a guarda
`def.Where != "" && def.NewEvaluator == nil`). Novos campos
`Definition.Scope` (texto normalizado) e `NewScopeEvaluator` (fábrica, no molde
do `where`). Uma função única de identidade (ex.: `identityOf(def)`), usada SÓ
por `infoOfCounters` e `sameDefinition`, que grava em `CounterInfo.Where`:
sem scope → `def.Where` byte a byte; com scope → `<where>\n<scope>` (o `\n` é
eliminado pela normalização `strings.Join(strings.Fields(..), " ")` de ambos,
então a não-colisão sai da normalização). Com `where` vazio e scope, a
identidade é `"\n<scope>"`, nunca `""`. A coluna nova de `_meta.counters` é
`scope`, ao lado de `condition` (não existe coluna `where`). Godoc de
`CounterInfo` documenta o formato e o motivo (rollback reconstrói).

R2. **Par consistente `total`/`rows` — palavra de 64 bits por grupo.** Hoje
`refs[g]` e `totals[g]` são duas cargas/somas atômicas separadas e o leitor
pode ver (T,R+1), (T−1,R), (T,R−1) — inclusive `total > rows` — o que gera
`resolved`/`disqualified` espúrio no alarme `total*10 <= rows` (a resolução não
tem `sustainedFor`). Decisão: `refs` e `totals` de cada grupo passam a viver
numa ÚNICA palavra int64/uint64 em heap, com refs nos 32 bits baixos e totals
nos 32 altos (ou o arranjo que o Builder provar correto), atualizada com UMA
soma atômica por grupo por transição (entrada: `+1 refs` e `+1 total` se bit;
saída: simétrico; troca de bit no mesmo grupo: só total). Leitura: uma carga.
Teto: 2^32−1 linhas por grupo, recusado com erro nomeado ANTES de estourar (é
bug/infra, nunca dado ruim — decida o balde e documente; teste exercita o teto
com um dublê/limite reduzido, não com 4 bilhões de linhas). `DisableTotal`
continua sem custo semântico (os 32 altos ficam zero e não são lidos). Todos os
consumidores (`Group`, `AuditSnapshot`, `Fold`, `Snapshot`, aposentadoria,
`emptySince`, measures) migram. Nada muda em disco (os totais são dobra do
vetor). Teste concorrente NÃO vacuamente concorrente: escritor alternando uma
linha dentro/fora do grupo com bit ligado e leitores em laço asseverando
`total <= rows` e o par sempre ∈ {(T,R),(T+1,R+1)}; mutante: voltar a duas
somas separadas → vermelho (prove que o teste pega o rasgo; rode com
`-count` suficiente e diga quantas iterações/observações). Medir custo do
caminho quente antes/depois (`counter_gate_test`/bench existente).

R3. **Evento usa a leitura que decidiu.** `Event.Total` e o novo `Event.Rows`
são lidos do `rec.Payload` avaliado (molde de `firedValueOf`), não da primeira
chamada `rc.reader.Group` do `sweep`. Aposentado continua 0/0. Teste com um
Reader dublê que muda entre as duas chamadas; mutante: ler da primeira chamada
→ vermelho.

R4. **Scope entra nas identidades derivadas:** no `.source` das métricas de
janela por taxa (`internal/countermetrics/measure_source.go`) e na impressão
digital `context` do alarme (`internal/alarm/compile.go:~221`). Teste: mudar
SÓ o scope gera `counter_metric.source_changed` e invalida o cronômetro
restaurado do alarme. Declare na doc que o scope restringe também as measures
(linha fora do escopo é `WithoutGroup` e não conta evento).

R5. **Recusas do scope espelham as do where:** campo com `merge: sum` recusado
no scope (mensagem própria `<contador>.scope`), guarda de scope sem avaliador,
e a recusa de coluna à direita do modo não-alarme vale para o scope.

R6. **Gate de alocação novo** em `Transition`/`Derive` COM scope declarado,
linha dentro e fora do escopo, com mutante que aloque de verdade
(`fmt.Sprintf` no ramo do scope). O gate velho não cobre o caminho novo.

R7. **`rows` sempre presente — decisão do usuário (contrato).** Em todo
contador: tabela `_counters.<nome>` (logo após `total`), linha da emissão
periódica (arquivo e Kafka, na mesma posição relativa do `SELECT *`: `total`,
`rows`, measures, métricas) e corpo/log de `alarm.firing`/`alarm.resolved`.
`TestACounterWithoutMetricsEmitsTheSameLineAsBefore` deixa de ser verdade:
reescreva-o (renomeie para o que passa a afirmar) com a linha nova LITERAL, e
atualize o godoc de `Options.Metrics`, `docs/operacao-series-kafka.md` e os
exemplos do README. A quebra aditiva é registrada no aceite para as notas da
release. `ReservedEventKey` (`internal/alarm/sink.go`) ganha `rows`.

R8. **Golden de manifest com bytes LITERAIS** (hex escrito à mão no teste) da
seção de contadores, para: (i) contador sem scope (prova "byte a byte igual ao
de hoje" — derive o literal do formato documentado, não do encoder); (ii) com
scope. Nunca gerado pelo encoder nem pela função de identidade em teste.

R9. **Comparação 128 bits COM SINAL.** Colunas int64 podem ser negativas
(groupBy int64, min/max de janela). Testes: negativos dos dois lados,
multiplicador dos dois lados, literal `9223372036854775807`, e o overflow
real `total=2` × 2^62. Mutante: `bits.Mul64` sobre complemento de dois sem
tratar sinal → vermelho. As colunas da direita entram em `Slots.IDs` (o
`Coleta` e o `metricsFingerprintOf` dependem disso).

R10. **Textos normativos a corrigir** (passam a mentir com o scope): godoc de
`Stats.Rows` e de `WithoutGroup` (`internal/storage/counter/counter.go`),
`internal/counters/meta.go` (descrição de rows), `internal/protocol/documents.go`
(`CounterStats.Rows`), `internal/metrics/names.go` e o HELP em
`internal/server/metrics_dataset.go`; `docs/operacao-webhook.md` (lista de
campos do evento) e `docs/architecture.md` (seção do contador). Procure golden
de HELP do `/metrics` e atualize se existir.

R11. **Garantia 3 vira fixação de propriedade existente** (o contador só recebe
o payload pós-merge do record): teste ponta a ponta, declarado como fixação,
sem mutante.

R12. **Avaliador por Deriver:** mesma FÁBRICA, instância própria por Deriver
(o avaliador tem scratch de um dono — compartilhar é data race).

R13. **Gramática C fixada:** aceito SÓ `<col> [* k] <op> <col> [* k]` com `k`
inteiro ≥ 1 à DIREITA da coluna. Recusados com mensagem própria: `k * col`,
`col * k <op> literal`, literal negativo/zero/float como multiplicador, coluna
double (inclui min/max de janela com `source` de measure, que são double) ou
string. `total * 10 <= 100` é recusado (use `total <= 10`).

R14. **Cronômetros na troca de scope:** mudar o scope reconstrói o contador e
zera measures; pela R4 também invalida os cronômetros restaurados dos alarmes
desse contador. Documente.

R15. **Rename incremental (regra do CLAUDE.md):** identificador PT-BR em função
que o bloco TOCA é renomeado para inglês no mesmo bloco (ex.: `declarados`,
`recria`, `motivo`, `byNome`, `reconstroiCountersPendentes`,
`varreRowsVivasInCounter` se tocados), cada par velho → novo acrescentado em
`planning/identificadores-em-ingles/mapeamento-2-manual.tsv`; rode
`TestRenamedIdentifiersAreNotCitedByTheirOldNames` e `scripts/renamecite`
(veja o README do script) e corrija godoc/doc/diagrama citados. NÃO renomeie
o que o bloco não toca. Contratos (chave de log, nome em disco, wire) NÃO
mudam.

Fora de escopo registrado: `strings.Fields` colapsa espaços dentro de literal
string na normalização de `where`/`when`/`scope` (pré-existente) — vira issue
própria, não corrigir aqui.

## Fora deste bloco
- Batching do fsync de grupo novo (débito 108) — bloco próprio, depois da
  medida na .218.
- `rows` como `source` de métrica de janela.
- Comparação entre colunas no SQL.
