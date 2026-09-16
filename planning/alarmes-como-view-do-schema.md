# Alarmes como view do schema (`<schema>.alarms`) + `firingValue`/`firedAt`

## Contexto

Hoje os alarmes em observação aparecem em `_meta.alarms`, ao lado das tabelas que descrevem o motor. Só que eles derivam dos contadores, e os contadores já ficam na árvore do schema (`telecom.por_olt`). Por isso a tabela de alarmes passa a ser a view `telecom.alarms`, com `alarms` como nome reservado.

A tabela também não mostra com que valor o alarme disparou; hoje isso só aparece no evento de log. Entram três colunas:
- `firingValue`: o valor sustentado que disparou o alarme;
- `valueColumn`: de qual coluna esse valor foi lido;
- `firedAt`: quando o disparo foi anunciado. Estava prevista em `planning/alarmes-e-emissao-de-contador.md:197` e nunca foi implementada.

Decisões do usuário em 16/09/2026:
- **Valor:** nova chave opcional `value:` no alarme, com default `total`.
- **`_meta.alarms`:** é **removida**, sem legado. A quebra fica registrada, porque a v0.9.1 já foi publicada.
- **`firedAt`:** entra.

O plano foi refutado contra o código antes do primeiro Builder. As correções aparecem abaixo marcadas com ⚠.

## Desenho

### 1. Namespace `_counters`, tabela `alarms`
Registrar a view com `RegisterVirtual(dataset, schema.CountersNamespace, "alarms", …)`. A constante `CountersNamespace` fica em `internal/schema/counter.go:49`.

Isso já basta: `catalogEntries` (`internal/query/catalog.go:175-203`) publica toda virtual fora de `_meta` como `(schemaName, alarms)` com `Kind = VIEW`. Sem tocar parser, resolvedor, catálogo ou clientes, a view passa a responder por:
- `telecom.alarms`, via `engine.go:305`;
- `FROM alarms` na conexão por schema, via `engine.go:449`;
- o atalho `_counters.alarms`, via `engine.go:524`;
- `/v1/metadata`, `_meta.tables` e o JDBC.

O namespace é declarado mesmo em dataset sem contador (`counters/table.go:321` e `engine.go:151`).

**Efeitos a documentar:**
- `_meta.tables WHERE namespace='_counters'` passa a trazer `alarms`.
- A recusa de `_counters.<inexistente>` passa a listar `alarms`.
- Dentro de `rankVirtual`, `alarms` ordena antes dos contadores.

**Irmã histórica:**
- No servidor ela já fica sem a view (`internal/server/history.go:104-112`).
- ⚠ No CLI ela recebe a view hoje (`internal/cli/query.go:196,222`; teste em `metatables_query_test.go:97`). A view passa a ser registrada só para a base (`spec != nil`), como já acontece com `metakafka`.

### 2. Nome reservado `alarms`
Uma constante única em `internal/schema`: `ReservedAlarmsTable = "alarms"`. As recusas acontecem na subida, com mensagem em inglês:
- contador chamado `alarms`, em `validateCounters` (`internal/schema/counter.go:180`);
- dataset chamado `alarms`:
  - ⚠ a spec sintética da própria view se chama `alarms` e passa por `validate` (`spec.go:397`);
  - por isso a recusa isenta `SchemaName == SyntheticSchemaName`, no mesmo padrão de `spec.go:785`;
- irmã histórica cujo `name + suffix` dê `alarms` (`internal/schema/history.go:142/201-212`).

A série do `emit:` grava arquivo, não tabela, então não colide.

### 3. `internal/metaalarm` → `internal/alarmtable`
Renomeações:
- `TableMetaAlarms` → `TableName` (igual a `schema.ReservedAlarmsTable`);
- em `table.go`, os identificadores PT-BR passam para inglês: `abertos`→`open`, `proxEpoca`→`nextEpoch`, `retratoOf`→`snapshotOf`, `epoca`→`epoch`;
- em `internal/alarm/snapshot.go:64`, `vazio`→`empty`.

Mapeamento em `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`:
- entram as linhas novas com `pkg = internal/alarmtable`;
- ⚠ as linhas antigas com `pkg = internal/metaalarm` (`tsv:963-966`) passam para `internal/alarmtable`. Sem isso, o gate (`dangling_citations_test.go:365`) deixa de cobrá-las.

Call sites:
- `internal/server/server.go:596`;
- `internal/cli/query.go:222`.

Citações a atualizar:
- godoc em `internal/alarm/runner.go:72,212`, `snapshot.go:13`, `snapshot_test.go:13` e `internal/metausers/table.go:7`;
- `docs/glossario.md:545,551,588,591`.

### 4. Chave `value:` no alarme
Na spec (`internal/schema/alarm.go`):
- `schema.AlarmSpec` ganha `Value string`;
- a chave entra na lista de chaves aceitas (`alarm.go:120`);
- presente e vazia é recusada;
- ausente vale `total`, resolvido na compilação.

Na compilação (`internal/alarm/compile.go:139/178`), contra o `sc` de `counters.TableSchema`:
- ⚠ a validação é **por nome**, não por tipo, porque o groupBy aceita int64 e boolean (`counter.go:360`);
- são aceitos só `total` e as colunas de métrica `<métrica>_<avg|min|max>`;
- `groupKey`, colunas do groupBy e nomes inexistentes são recusados com erro que lista as opções;
- o resultado compilado é `valueColumn` + FieldID (`sc.FieldByName`, os mesmos IDs do `metricColumns`, `runner.go:221-235`) + tipo (`total`, `_min` e `_max` são int64; `_avg` é double).

`value` fica **fora** da assinatura do estado salvo (`runner.go:269`). Trocar `value` não zera o cronômetro.

### 5. Captura no disparo
**Onde capturar:**
- ⚠ A captura mora em `Runner.decide`, quando `d.verdict == verdictFiring` (`runner.go:428/443`). É lá que `payload` e `now` estão disponíveis; `tracker.evaluate` não recebe o payload.
- Os valores capturados ficam no tracker: `firedAt time.Time`, `firingValue float64`, `hasFiringValue bool`.

**O que ler:**
- ⚠ Lê **sempre do payload** pelo FieldID, inclusive para `total`.
- O `total` que o `sweep` tem em mãos vem de outra leitura do `Group` (`runner.go:336` × `counters/table.go:393`) e pode divergir do payload que decidiu.
- Uma coluna ausente no payload deixa `hasFiringValue=false`, que vira NULL.
- A conversão de int64 para double perde precisão acima de 2^53; isso fica documentado no godoc.

**Saída de firing:**
- ⚠ Não exige reset. Sair de firing leva a `phaseAbsent`, e o tracker é apagado do mapa (`runner.go:418-422`).
- O teste afirma que a linha some do retrato.

**Retrato:**
- `ActiveGroup` ganha `ValueColumn`, `FiredAt`, `FiringValue` e `HasFiringValue`, preenchidos em `publishSnapshot`.
- O disparo já marca `dirty` (`runner.go:319`).

**Reinício:**
- `alarms.state` v2 **não muda**. A fase não é persistida (`state.go:115-119`): o alarme restaurado volta como pending e dispara de novo com `restored=true`.
- `firedAt` e `firingValue` passam a descrever esse novo disparo. Isso fica documentado.

**Evento de log:**
- O disparo ganha as chaves `valueColumn` e `firingValue`, lidas do mesmo valor capturado, em `sink.go:38`. É uma mudança aditiva.
- O risco de uma coluna do groupBy ter o mesmo nome de uma chave de topo já existe hoje (`sink.go:67`) e não é tratado aqui. Entra como débito novo no fim de `docs/debitos.md`.

### 6. Colunas da view (`tableSpec`)
As colunas atuais ficam, e entram três:

| Coluna | Tipo | Quando é NULL |
|---|---|---|
| `valueColumn` | string | nunca |
| `firedAt` | timestamp | enquanto pending |
| `firingValue` | double | enquanto pending, ou se a coluna estava ausente no disparo |

NULL fica fora do payload, no padrão de `table.go:289`. A ordem por FieldID é resolvida uma vez (`table.go:146`).

### 7. Documentação e contrato
**`README.md`** (1642, 1738, 1950):
- a view `telecom.alarms` e o atalho `_counters.alarms`;
- as colunas novas e a chave `value:`;
- `alarms` como nome reservado;
- a semântica de reinício;
- no client Python, usar `telecom.alarms` ou `_counters.alarms`, porque `FROM alarms` seria lido como dataset;
- uma nota de quebra: `_meta.alarms` foi removida.
  - Não há CHANGELOG, e a release usa `--generate-notes` (`scripts/release.py:70`).
  - Por isso a quebra também vai na descrição do PR.

**Outros documentos:**
- `docs/architecture.md` (2828, 3042);
- `docs/diagrams/alarmes_flow.puml`, validado pelo servidor `uml.nishisan.dev`;
- `docs/glossario.md`: view de alarmes, valor de disparo, coluna do valor, instante do disparo e nome reservado;
- `docs/debitos.md`: um débito novo, a colisão de chave no log;
- registros de rodada e planos antigos não são reescritos.

O plano aprovado é salvo em `planning/alarmes-como-view-do-schema.md`.

## Testes
Cada garantia tem mutação: vermelho → verde, com os dois resultados relatados.

**`internal/schema`:**
- `TestCounterNamedAlarmsIsRefused`
- `TestDatasetNamedAlarmsIsRefused`
- `TestSyntheticAlarmsSpecIsNotRefused`
- `TestHistorySiblingResolvingToAlarmsIsRefused`
- `value` presente e vazio é recusado.

**`internal/alarm`:**
- `TestFiringCapturesTheDeclaredValueColumn`: uma `_avg` (double) e uma `_max` (int64).
- `TestValueDefaultsToTotal`
- `TestFiringValueIsReadFromTheDecidingPayload`: o `Group` devolve um total diferente do payload, e o valor capturado tem de ser o do payload.
- `TestMissingValueColumnYieldsNoFiringValue`
- `TestResolvedGroupLeavesTheSnapshot`
- `TestChangingValueDoesNotResetTheTimer`
- `TestRestoredAlarmRefiresWithFreshFiredAt`
- Compile: recusa `groupKey`, coluna do groupBy int64 e nome inexistente.
- ⚠ **Gate de alocação novo** em `runner_gate_test.go`, com contraparte race: passada com grupos **pending** estáveis, sem transição, e zero alocação. O mutante lê o valor com `fmt.Sprintf` a cada passada e tem de ficar vermelho.

**`internal/alarmtable`:**
- `table_test.go` é adaptado para `_counters.alarms`.
- `TestFiringRowCarriesFiredAtAndFiringValue`
- `TestPendingRowHasNullFiredAtAndFiringValue`
- `TestFiringValueIsNullWhenAbsent`

**`internal/server` e `internal/cli`:**
- Atualizar os casos existentes:
  - `documents_test.go:759`;
  - `metatables_test.go:53`;
  - `counters_test.go:203`, onde a lista "existem:" muda;
  - `internal/cli/metatables_query_test.go:57,97`: a irmã deixa de ter a view;
  - `internal/query/metatables_test.go`;
  - o helper `alarmStates` (`counter_metrics_test.go:326`).
- Teste de ponta a ponta novo:
  - `telecom.alarms` é VIEW em `/v1/metadata?schema=telecom`;
  - `FROM alarms` via conexão por schema;
  - `_meta.alarms` responde tabela inexistente;
  - o disparo expõe `firedAt`, `valueColumn` e `firingValue`.

**Gates e varredura final:**
- `TestRenamedIdentifiersAreNotCitedByTheirOldNames`, `TestCitacoesDeSecaoResolvem` e `TestEvidenciasCitadasNosDocumentosExistem`.
- `grep -rn "_meta.alarms\|metaalarm\|TableMetaAlarms" --exclude-dir=.claude`. Só podem sobrar o TSV e o histórico em `planning/` e nos aceites.

## Execução (orquestração)
**Preparação:**
- Worktree: `EnterWorktree`, depois `git branch -m feature/alarmes-como-view-do-schema`.
- Builders em Sonnet e Refuters em Opus.
- Todo Builder recebe o mesmo ambiente:
  - `GOTOOLCHAIN=go1.26.5 GOPROXY=off`;
  - pacotes disjuntos;
  - sem commit;
  - `docs/glossario.md` proibido;
  - comandos longos em foreground;
  - identificadores em inglês e prosa em PT-BR.

**Blocos:**
- **A:** `internal/schema`, com a reserva e a chave `value`.
- **B:** `internal/alarm`, com compile, captura, snapshot, evento e gate. Depende de A.
- **C:** rename para `internal/alarmtable`, colunas, server/cli, TSV e testes de catálogo. Depende de B.
- **D:** docs, diagrama, glossário e débito, feitos pelo orquestrador.

Cada bloco passa por um Refuter, que reexecuta os testes e as mutações. Commit por bloco, com `git add` explícito e conferência de `git status` antes (resíduo de mutação).

## Verificação
- `GOTOOLCHAIN=go1.26.5 GOPROXY=off make test`, `make test-race`, `make test-integration` e `make lint`. A contagem do lint tem de ficar igual à baseline; se cair, a baseline é ajustada no mesmo PR.
- Conferir a contagem de testes, não só o resultado `ok`.
- `make test-jdbc`: a view aparece na árvore do schema como VIEW.
- `make test-python-it`
- Manual: subir o binário com um alarme de `sustainedFor: 10s` e `value: <métrica>_avg`, rodar `nishidb query --server --schema telecom "SELECT alarm, state, since, firedAt, valueColumn, firingValue FROM alarms"` e conferir se o valor bate com a chave `firingValue` do WARN `alarm.firing`.
