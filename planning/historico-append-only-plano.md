# Tabela histórica append-only (`events_history`)

## Contexto

O NishiDB materializa **estado corrente**: cada evento é mergeado no record da
entidade e a versão anterior vira garbage que a compaction recupera. O que a
origem mandou não sobrevive — e no piloto `events` isso já custou trabalho de
diagnóstico: descobrir por que uma coluna tem determinado valor exige inferir o
merge a partir do resultado, sem nunca ver a mensagem que o produziu.

A feature acrescenta uma **segunda tabela por dataset**, `events_history`, que
recebe uma linha por evento — sem deduplicação, sem merge, sem update — nas
**mesmas colunas tipadas** do dataset base, incluindo as colunas derivadas de
campos aninhados (`persistent.*`). Ela é ferramenta de debug, então tem
retenção própria: expira por tempo após a escrita e purga por tamanho.

O resultado esperado é operacional: da **mesma conexão JDBC**, `SELECT ... FROM
events` responde sobre o estado e `SELECT ... FROM events_history` responde
sobre o fluxo, com as mesmas colunas, os mesmos tipos e a mesma gramática.

## Decisões travadas

| Decisão | Escolha |
|---|---|
| Conteúdo da linha | **Evento cru mapeado** — só os campos que vieram na mensagem, tipados pelo mesmo schema |
| Declaração | Sub-bloco `history:` no YAML do dataset base; o dataset irmão é derivado, não escrito à mão |
| Falha de escrita do histórico | **Degrada e não para** a ingestão do dataset principal |
| Consulta | Índices secundários próprios + poda de segmento por faixa de tempo |
| Coluna `_raw` | **Opcional** (`rawPayload: true`), e é ela que permite gravar o evento que nem parseia |
| Precisão do TTL | **Exata na leitura** — a linha some no prazo, não quando o segmento é purgado |

## Desenho

### 1. Superfície de configuração

Bloco novo no YAML do dataset base, validado com whitelist de chave
(`bloco()`, `internal/schema/source.go:171`) — o mesmo padrão de `source:` e
`errors:`, que dá recusa de chave desconhecida de graça:

```yaml
name: events
history:
  suffix: _history          # default; a tabela é <name><suffix>
  expireAfterWrite: 7d      # opcional
  maxBytes: 40GiB           # opcional; teto dos SEGMENTOS DE DADOS
  segmentSize: 128MiB       # opcional; granularidade da purga por tamanho
  rawPayload: false         # opcional; acrescenta a coluna _raw
  fields:                   # opcional; só overrides de índice
    identifier: { index: hash }
    node:       { index: hash }
    severity:   { index: bitmap }
```

Regras de validação (em `internal/schema/history.go`, novo):

- Pelo menos um entre `expireAfterWrite` e `maxBytes` é **obrigatório**. Um
  histórico sem retenção é disco que cresce para sempre, e aceitar isso em
  silêncio é a "configuração que mente" que o débito 37 registra.
- `maxBytes >= 4 × segmentSize`. Com menos, a purga nunca consegue ficar abaixo
  do teto sem tocar o segmento corrente, que a D6 proíbe.
- `expireAfterWrite` reusa `units.DurationWithDays` e o piso de
  `ExpiryResolution` (1 min), como as regras de TTL.
- `history.fields` só aceita `index:`, e só sobre campo que **existe** no
  schema base. Tipo, `format`, `merge` e `frozenFrom` são recusados ali — o
  histórico espelha, não redefine.
- O nome derivado não pode colidir com um dataset declarado em
  `datasets.directory`. Conferido na subida, junto de `ValidateGroupUniqueness`
  (`internal/config/config.go:669`).
- Se `rawPayload: true`, o schema base não pode ter campo chamado `_raw`.

### 2. Superfície SQL — sai de graça

Nada de lexer, parser, binder, planner, executor, protocolo, JDBC ou client
Python precisa mudar para a consulta funcionar. O motivo é concreto:

- `NishiStatement.java:76` já roteia pelo nome do FROM:
  `escaneado.dataset() == null ? conexao.datasetPadrao() : escaneado.dataset()`.
  `SELECT ... FROM events_history` numa conexão a `events` já manda
  `dataset: "events_history"` no request.
- `query.Engine.Register(nome, sch, st)` (`internal/query/engine.go:55`) é um
  mapa plano que aceita **qualquer** `query.Store`. Basta registrar o histórico
  sob o próprio nome.
- `binder.Bind` confere `stmt.Dataset == sc.Name` (`binder.go:196`), e como o
  histórico tem `Compiled` próprio com `Name = "events_history"`, a conferência
  passa sem relaxamento nenhum.

Único ajuste no lado da consulta: **o planner precisa parar de escolher o
point lookup quando o store não tem primary** (ver §7).

### 3. Identidade de colunas entre as duas tabelas

Este é o ponto que decide se a feature cumpre "a query funciona nas duas
tabelas". A regra: **um dicionário, dois `Compiled`.**

- O histórico **não tem `fields.dict` próprio**. Ele recebe o
  `*fielddict.Dict` do dataset base por `storage.Options.DictCompartilhado`.
  Com o mesmo dicionário, os FieldIDs e os `typeHint` são os mesmos por
  construção, e o payload escrito pelo patch do base decodifica no histórico
  sem tradução.
- O `Compiled` do histórico é derivado do spec base por
  `spec.DeriveHistory()` (novo, em `internal/schema/history.go`) e
  compilado contra **esse mesmo dicionário**. Ele difere do base em quatro
  pontos, e só neles: `Name`, ausência de `Key`/`Sequence`, ausência das
  colunas congeladas e de `_expiresIn`, e os `index:` de `history.fields`.
- **Colunas descobertas em runtime são espelhadas.** `ResolveUnknown` e
  `ResolveNested` (`compile.go:1044` e `:815`) ganham um callback
  `AoDescobrirCampo`, no mesmo molde de `OnFieldDiscarded` /
  `OnIncompatibleValue` que `server.abreDataset` já instala. Sem isso, uma
  chave promovida a coluna depois da subida seria consultável em `events` e
  invisível em `events_history` até o restart — exatamente a divergência que a
  feature existe para não ter. O mesmo callback propaga `FixAutoType`.

Por que as congeladas ficam de fora: `capturaCongeladas`
(`internal/materializer/frozen.go:31`) roda dentro do `Parse` e emite a
entry com o valor **corrente** da origem. No histórico isso seria
`originalSeverity == severity` em toda linha — duplicação com cara de
informação. Preservar o primeiro valor é trabalho do merge, e o histórico não
tem merge.

### 4. Caminho de escrita

O enxerto é em `materializer.Apply`, **logo depois do `Parse` bem-sucedido e
antes do stale check** (`internal/materializer/materializer.go:364-390`):

```
Parse(event)          ← já produz entries ORDENADAS por FieldID, sem duplicatas,
                        com as derivadas (patch.go:224), as congeladas (:231)
                        e _writtenAt injetado (:241)
  └─ historico.Apenda(encodePatchHistorico(entries, filtro))   ← NOVO
stale check
emitePrazo
merge / encodePatch
store.Put
```

Três razões para o ponto ser esse:

1. `patch.entries` **é** o evento cru mapeado — inclusive as colunas derivadas
   de `persistent`, que é o requisito. Nenhum decode a mais.
2. Antes do stale check, o histórico registra também o evento que a `sequence`
   descartaria — que é justamente o que se quer ver ao depurar descarte.
3. `_expiresIn` ainda não foi emitido, então ele não entra no payload sem
   filtro nenhum.

`encodePatchHistorico` é `encodePatch` (`internal/materializer/merge.go:177`)
com um filtro que pula entry cujo `CompiledField.FrozenFrom != ""`. A ordem por
FieldID e a ausência de duplicatas se preservam sob filtro, então
`ValidatePayload` continua satisfeito.

**Com `rawPayload: true`**, uma entry a mais com o campo `_raw` (`type: json`)
carregando os bytes do evento. E o caminho de erro do `Apply` — `failure()`,
`materializer.go:466` — grava, no balde de **evento ruim** apenas, uma linha
só com `_writtenAt` e `_raw`. Falha de infra não gera linha: não é o evento que
está errado.

**Ordem com o commit de offset.** O `kafka.Store` (`internal/kafka/writer.go:34`)
é uma interface com `Checkpoint()`, e o §48 já exige checkpoint durável antes
do commit no broker. Passa-se um adaptador que faz `hist.Checkpoint()` **antes**
de `base.Checkpoint()` — o segundo é o que durabiliza os offsets. Um crash entre
os dois deixa o offset atrás dos dois datasets: o replay reprocessa e o
histórico ganha duplicatas, nunca buracos. O mesmo adaptador vale para o
`nishidb ingest` (`internal/cli/ingest.go`). O pacote `internal/kafka` não muda.

### 5. O modo de storage

`storage.Options` ganha `Mode storage.StoreMode` (`ModoMaterializado` default,
`HistoryMode`), fiado nos **dois** call sites que abrem dataset:
`internal/server/server.go:322` e `internal/cli/cli.go:237`.

O modo é gravado no manifest numa **tag TLV crítica** (`tag >= 0x8000`,
`internal/storage/checkpoint/manifest.go:44-64`): um binário que não conheça o
modo recusa o dataset em vez de abri-lo como materializado e tratar log de
eventos como estado corrente.

O que `ModoHistorico` desliga e liga em `Open` / `Put` / `applyRecord` — o mesmo
padrão condicional que o TTL já estabeleceu (`if s.escritas != nil`):

| Componente | `ModoHistorico` |
|---|---|
| `primary.idx` | **não abre** — dedup por identifier é o que a tabela não faz |
| `sequences.idx` | **não abre** — não há estado a sobrescrever |
| `expiry.idx` / `prazos.idx` | **não abrem** — o prazo é do dataset, não da linha |
| `locations.idx` | abre; continua o commit point de visibilidade |
| `fields.dict` | **não cria** — usa o `SharedDictionary` do base |
| Compaction | **desligada** — sem update não há garbage; o disco volta por descarte de segmento |
| Coletor de expiração | **desligado** — substituído pelo motor de retenção (§6) |
| Consumer Kafka | **nenhum** — a escrita vem do materializer do base |

`Put` é substituído por `Apenda(payload []byte) (types.RowID, error)`: sem
lookup de key, sem `prev`, `rowID = nextRowID.Add(1)`, `BuildRecord` com key
vazia e sequence 0, `segW.Append`, `applyRecord` com `hadPrevious = false`, sem
`prim.Insert` e **sem a fase de CLEAR** dos secundários (`store.go:1763`,
`:1865`) — nada sai de um índice numa tabela que não atualiza linha.

### 6. Retenção — tempo e tamanho

Goroutine por dataset histórico, no molde de `IniciaCompaction` /
`IniciaExpiracao`: `Inicia`/`Para` com `WaitGroup`, medidor atômico observável,
cancelamento por contexto, e `Close` que cancela e **aguarda antes** do quiesce.
Arquivo novo: `internal/storage/retention.go`.

**A invariante que torna tudo barato:** no modo histórico os appends são
serializados pelo `wmu` e `_writtenAt` é o relógio da ingestão, então
`_writtenAt` é **não-decrescente por rowId**, e cada segmento possui um
intervalo **contíguo** de rowIds.

**Visibilidade exata, sem varredura.** O Store mantém uma marca-d'água
`rowIDBase`: `ScanLive` começa nela em vez de zero, e `GetByRowID(id <
rowIDBase)` devolve `live=false`. A cada passada, `rowIDBase` avança para o
primeiro rowId cujo `_writtenAt` ainda não venceu — achado por busca binária
sobre `createdAtMs` dos segmentos (já no header, `segment.go:29`) seguida de
uma busca binária dentro de um único segmento. É O(log n) por passada, e é o
que faz `expireAfterWrite: 7d` significar sete dias e não "sete dias mais o
resto do segmento". `rowIDBase` é persistido no manifest.

**Purga por tempo.** O segmento N está inteiramente vencido quando
`createdAtMs(N+1) < agora − expireAfterWrite` — os records de N foram todos
escritos antes de N+1 nascer. Nenhum estado novo em disco para isso.

**Purga por tamanho.** Enquanto `soma(sizeBytes dos segmentos) > maxBytes`,
aposenta o segmento selado mais antigo. As três fontes de tamanho já existem
(`ListSegments().SizeBytes`, o mapa `s.segSize`, o espelho atômico
`diskBytesAll`).

**A retirada reusa `aposentaSegmento` (`compaction.go:492`) inteiro**, sem o
`compactaSegmento` antes: checkpoint → rename para `.retired` → fsync do
diretório → poda dos mapas → checkpoint → fecha descritor e unlink → fsync. As
duas janelas de crash já são reparadas por `reparaAposentados`
(`store_recovery.go:237`), que não sabe nem precisa saber por que o segmento
foi aposentado. O segmento **corrente nunca** é candidato (D6).

**O que sobra em disco depois da purga**, e fica escrito: `locations.idx` é
denso por rowId e a purga só avança `rowIDBase`, então o prefixo morto continua
alocado. A passada de retenção chama `fallocate(FALLOC_FL_PUNCH_HOLE)` sobre o
intervalo `[headerSize + base_antigo*8, headerSize + base_novo*8)`, devolvendo
os blocos sem mudar o tamanho aparente do arquivo. Consequência que o §9
registra: a contabilidade de disco do histórico tem de ler **blocos**
(`st_blocks`), não `Size()`.

**`maxBytes` governa os segmentos de dados, e só eles** — é o que a purga sabe
devolver. Índices e `locations.idx` são publicados **em separado** no
`/v1/stats`, para o operador dimensionar o disco sem que o teto minta.

### 7. Índices

`history.fields` declara `index: hash | bitmap` por coluna, e o histórico abre
esses índices sobre os mesmos FieldIDs do base. Eles ficam **mais baratos** que
no dataset materializado: só a fase de SET, nunca a de CLEAR, e o hash
secundário dispensa a máquina de lápides (`index/hash/index.go:362-434`).

Duas consequências que precisam de código:

1. **O planner não pode escolher o point lookup sem primary.** Hoje a regra R1
   não tem capability check — só as R2/R3 têm ("schema declara E store
   confirma", `planner.go:222-224`). Acrescenta-se `PrimaryCapable` /
   `TemPrimary() bool` no mesmo molde, detectado por type assertion uma vez em
   `planner.New`. Sem isso, `WHERE identifier = 'x'` sobre o histórico cairia
   em `GetByKey` e devolveria no máximo uma linha — resultado errado, sem erro.
   É por isso que `identifier` ganha `index: hash` no exemplo do §1: no
   histórico ele é uma coluna de alta cardinalidade como outra qualquer.
2. **Postings órfãs por purga.** Um posting que aponta para rowId `< rowIDBase`
   resolve para `live=false` e o executor já o filtra — a tolerância a superset
   é contrato (`planner/plan.go:1-30`). O que precisa mudar é a contagem de
   postings **vivas** que dimensiona a reconstrução do hash
   (`hash/index.go:475`, `talvezReconstrua`): ela passa a tratar `rowId <
   rowIDBase` como morto, e é assim que o arquivo volta a encolher. Para os
   bitmaps, que são densos por rowId, vale o mesmo `PUNCH_HOLE` do prefixo.

### 8. Falha e degradação

O histórico é ferramenta; o produto é o `events`. Um erro em `Apenda` **não**
propaga para o `ApplyResult`:

- Conta em `nishidb_history_dropped_total{dataset}`.
- Põe o histórico em modo degradado **sticky** — o mesmo desenho do
  `failWriter` (`store.go:1611`): morto até `Close` e reabertura com recovery.
  Sem sticky, cada evento retentaria um disco cheio e o log viraria uma linha
  por evento.
- Loga **uma vez** por entrada em degradação, com a causa.
- Publica `nishidb_history_degraded{dataset}` e o bloco `history.degraded` no
  `/v1/stats`.

A consulta ao histórico degradado continua respondendo — sobre o que já foi
gravado. Um histórico que some quando quebra é pior que um histórico truncado
com a marca de que truncou.

### 9. Observabilidade

`/v1/stats` do dataset base ganha o bloco `history`, e `/metrics` as séries
correspondentes — as duas saídas da **mesma** leitura de snapshot, como
`materializacaoStats` e `amostrasDeMaterializacao` já fazem
(`internal/server/materialization.go`):

| Série | Responde |
|---|---|
| `nishidb_history_rows_total` | quantas linhas foram gravadas |
| `nishidb_history_data_bytes` | os segmentos — o que o `maxBytes` governa |
| `nishidb_history_index_bytes` | índices + `locations.idx`, **em blocos** |
| `nishidb_history_oldest_written_at` | a janela real que o histórico cobre |
| `nishidb_history_segments_purged_total{reason=ttl\|size}` | qual política está decidindo |
| `nishidb_history_dropped_total` | eventos que não viraram linha |
| `nishidb_history_degraded` | o histórico está morto? |

`nishidb_history_oldest_written_at` é a série que o operador olha primeiro: é
ela que diz se o `maxBytes` está engolindo o `expireAfterWrite` declarado.

O histórico entra em `s.datasets` para que `/v1/metadata`, `/v1/health` e as
métricas por dataset o tratem como qualquer outro — e é isso que o faz aparecer
na árvore do DBeaver via `NishiDatabaseMetaData.getTables`
(`clients/jdbc/.../NishiDatabaseMetaData.java:179`), sem tocar no driver. A
struct `dataset` ganha uma marca de que ele não tem consumer, não tem compaction
e não tem coletor.

## Fases de implementação

Cada fase fecha com a suíte verde. TDD: o teste que trava o contrato vem antes.

**Fase A — schema e configuração**
`internal/schema/history.go` (novo): `HistorySpec`, `validateHistory` com
`bloco()`, `DeriveHistory()`. Campo `History *HistorySpec` em
`DatasetSpec` e `yaml.Node` por valor em `rawSpec` (`spec.go:199-214`). Recusa
de colisão de nome na subida. Testes no molde de `ttl_rule_test.go` e
`frozen_test.go`.

**Fase B — dicionário compartilhado e espelho de descoberta**
`storage.Options.DictCompartilhado`; `Open` deixa de criar `fields.dict` no
modo histórico. Callback `AoDescobrirCampo` em `Compiled`, disparado por
`ResolveUnknown`/`ResolveNested`/`FixAutoType`, instalado em
`server.abreDataset`. Teste de paridade: descobrir campo em runtime e consultar
a coluna nas **duas** tabelas sem restart.

**Fase C — modo histórico no storage**
`Options.Mode`, tag TLV crítica no manifest, ramos condicionais em
`Open`/`applyRecord`, método `Apenda`. Testes de recovery pelo tail e de recusa
do modo errado, no molde de `store_recovery_test.go`.

**Fase D — escrita**
`encodePatchHistorico` com filtro de congeladas; enxerto em `Apply`; coluna
`_raw` e a linha de evento ruim; degradação sticky; adaptador de `Checkpoint`
para o writer Kafka e para o `ingest`. Teste que prova que um `Apenda` que
falha **não** muda o `ApplyResult`.

**Fase E — retenção**
`internal/storage/retention.go`: `rowIDBase` persistido, busca binária da
marca-d'água, purga por tempo, purga por tamanho, `PUNCH_HOLE`, reuso de
`aposentaSegmento`. Testes de janela de crash usando o seam `apTrace`, no molde
de `compaction_crash_test.go`; teste com relógio injetado (`UsaRelogio`).

**Fase F — consulta e índices**
`PrimaryCapable` no planner; índices declarados por `history.fields`; contagem
de postings vivas ciente de `rowIDBase`; registro no `Engine`; entrada em
`s.datasets`. Testes no molde de `internal/query/harness_test.go` e
`internal/server/spec106_http_test.go`.

**Fase G — observabilidade e documentação**
Bloco `history` no `/v1/stats`, séries no `/metrics` (fonte única).
`docs/architecture.md` (seção nova + entrada em "O que ainda não existe"),
`docs/diagrams/historico_flow.puml` e `historico_layout.puml`, `README.md`,
`docs/piloto-events.md` com o plano de subida no piloto.

## Verificação

```bash
mvn -v >/dev/null 2>&1; make test && make test-race && make lint
```

- `go test ./internal/schema/ -run TestHistorico -v` — validação do bloco.
- `go test ./internal/storage/ -run 'TestHistorico|TestRetencao' -race -v` —
  modo, recovery e janelas de crash da purga.
- `go test ./internal/query/... -run TestHistorico -v` — paridade de colunas
  entre as duas tabelas e recusa do point lookup sem primary.
- `go test ./internal/cli/ -run TestHistoricoE2E -v` — ciclo completo pelo
  binário, com relógio injetado, provando que a linha some no prazo e que o
  segmento sai por `maxBytes`.
- `make test-jdbc` — `SELECT ... FROM events_history` da mesma conexão a
  `events`, e a tabela aparecendo em `getTables`.
- Fim a fim manual: subir com um dataset de teste, ingerir NDJSON, conferir
  `nishidb_history_oldest_written_at` andando e `df` devolvendo blocos depois
  de uma purga por tamanho.
- Gate de lint: igualar `.golangci-baseline` **antes** do PR — a contagem
  abaixo da baseline também quebra.

## Riscos e limites conhecidos

- **Custo de escrita.** Dobra o número de appends por evento. O append do
  histórico é mais barato que o do base (sem lookup de primary, sem merge, sem
  CLEAR de índice), mas não é grátis. Medir com `BenchmarkMergeRadiusPatch` e
  `benchmark/ingest-bench` antes e depois; publicar a razão na baseline.
- **Volume no piloto.** Com 4,38 eventos por entidade e ~17,8M entidades, o
  histórico do backlog do `events` passa de 70M linhas. O `maxBytes` não é
  opcional na prática.
- **Não há backfill.** O histórico começa vazio no momento em que é ligado —
  e não há como preenchê-lo, porque o base guarda estado mergeado, não os
  eventos. Ligar a chave **não** exige recriar o `events`, que é a diferença
  em relação às congeladas (débito 83).
- **`_writtenAt` é o instante da INGESTÃO**, não o do evento. Num reprocessamento
  do tópico, o histórico inteiro nasce com a data do replay e a retenção por
  tempo conta a partir dali. É a mesma pegadinha já registrada no piloto.
- **Duplicatas na retomada.** Crash entre o checkpoint do histórico e o do base
  faz o replay reapender a janela. A direção é deliberada: o histórico fica à
  frente, nunca atrás.
- **`PUNCH_HOLE` é local.** Já vale a restrição de deployment que o `flock`
  impõe (disco local), mas o requisito passa a incluir um filesystem que suporte
  buraco esparso (ext4/xfs/btrfs). Sem ele, a purga devolve os segmentos e não
  devolve o prefixo do `locations.idx` — degradação de espaço, não de correção.
- **`_raw` dobra o disco** do histórico em eventos JSON grandes como os do
  piloto. Por isso o default é `false`.
