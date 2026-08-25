# Campos aninhados consultáveis no NishiDB

## Context

O campo `persistent` do dataset `events` guarda o documento inteiro como **um**
valor `json`. Isso preserva o dado, mas nada lá dentro é consultável: o SQL não
tem sintaxe de caminho e coluna `json` não é comparável — o binder recusa em
[coerce.go:87](internal/query/binder/coerce.go:87). Quem precisa filtrar por
chave interna traz o documento inteiro e filtra na aplicação.

O pedido é declarar que certas chaves de dentro viram colunas de verdade, com
tipo e índice. O rascunho
[planning/feature-mapeamento-de-campos-aninhados.md](planning/feature-mapeamento-de-campos-aninhados.md)
mediu o piloto e cresceu para seis frentes separáveis. A decisão foi **fazer
todas, faseadas num plano só**, com o bug do teto do dicionário **antes** da
feature e a reserva do prefixo `_` **agora**.

No caminho, o plano **fecha o débito 22** ("backfill não existe no M1",
[debitos.md:219](docs/debitos.md:219), citado em
[architecture.md:631](docs/architecture.md:631)) — não abre item novo para a
mesma coisa.

## O que a leitura do código estabeleceu

O rascunho veio de medição do piloto e de leitura não reproduzida. Quatro
afirmações não se sustentam, e uma omissão bloqueava a frente mais valiosa.

### Refutado

| Afirmação do rascunho | O que o código faz |
|---|---|
| Bitmaps em `indexes/bitmap/<segmento>/<campo>.bm`, logo o backfill itera segmentos selados | Ficam em `indexes/bitmap/<fieldID>/<valueID>.bm` ([store.go:692](internal/storage/store.go:692), [bitmap.go:119](internal/index/bitmap/bitmap.go:119)) — um arquivo por **valor distinto**, global ao dataset, bit indexado por `rowID` absoluto. Não há granularidade de segmento, e a presença do arquivo não diz nada sobre progresso |
| O encoder pula o valor da coluna nula | Emite `null` **explícito** ([ndjson.go:41-43](internal/query/executor/ndjson.go:41)). O custo `nome+8` está certo; o mecanismo não |
| `exclude` só esconde da projeção | Campo excluído **não consome `fieldID`** — o `continue` vem antes do `LookupOrCreate` ([compile.go:264-266](internal/schema/compile.go:264)). Ele só não devolve ID já gasto |
| Campo `auto` não indexa porque `f.Index` nunca é `bitmap` | Quem atua é `f.IsAuto` em [planner.go:198](internal/query/planner/planner.go:198), antes de `bitmapEligible`. [planner_test.go:570-591](internal/query/planner/planner_test.go:570) existe para provar que a guarda não é decoração |

**A omissão:** declarar índice mais restart **não promove — o dataset não abre**.
[store.go:858-872](internal/storage/store.go:858) recusa quando um índice é
declarado num dataset com `RowCount > 0`. A guarda está certa; o backfill a
**transforma**, não a remove.

### Confirmado

- **O teto de 65.535 para o writer, permanentemente**, nos oito elos de
  [fielddict.go:205](internal/storage/fielddict/fielddict.go:205) até `w.para()`
  em [writer.go:219](internal/kafka/writer.go:219). O offset não avança, então o
  restart reproduz.
- **A chave do índice inclui o byte de tipo**
  ([bitmap.go:77-89](internal/index/bitmap/bitmap.go:77)).
- **`maxRows` é molde completo** de teto que o cliente encurta e o servidor
  recusa acima ([query.go:189-208](internal/server/query.go:189)), com o driver
  omitindo em vez de errar
  ([NishiStatement.java:121-127](clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiStatement.java:121)).
- **`FROM _meta.fields` quebra o driver em silêncio**: `parteDeIdent` não inclui
  `.` ([SqlScanner.java:216](clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/SqlScanner.java:216)).

### Achados que mudaram o desenho

1. **`nextRowID` é zero no laço dos bitmaps.** O laço está em
   [store.go:691-698](internal/storage/store.go:691) e o `switch` de recovery só
   começa em `:699`; quem semeia o contador é
   [store_recovery.go:23](internal/storage/store_recovery.go:23). Um estado de
   backfill criado ali com `Limite=0` se declararia **completo sem um único
   bit** — `HasBitmap` true, plano indexado, e `WHERE x='y'` devolvendo zero
   linhas num dataset cheio. O estado nasce **depois** do switch e antes do
   `s.Checkpoint()` de [store.go:752](internal/storage/store.go:752).
2. **`onMaxCols: stop` por categoria dentro de `BadEvent()` não pararia**:
   [writer.go:206-217](internal/kafka/writer.go:206) consulta a política para
   toda a faixa, e o default é descartar. A categoria nova fica **fora** dela.
3. **A guarda do `maxCols` corrige o bug do dicionário sozinha.** Checar
   `dict.Len()` antes do `LookupOrCreate` torna `ErrDictFull` inalcançável por
   `ResolveUnknown` — some uma peça inteira do plano, e
   `TestApplyFalhaDoDicionarioEhFailed` passa sem alteração, provando que a
   correção não alargou.
4. **Pai `json` legitimamente array ou `null` derrubaria o evento inteiro** (as
   ~49 colunas de topo): `typeconv.go:111` aceita array, mas `jsonp.go:119`
   exige `{`, e o erro viraria log-and-skip. A segunda passada **nunca** propaga
   erro do jsonp.
5. **`BitmapFieldNames` tem quatro chamadores**, não um
   ([server.go:300](internal/server/server.go:300),
   [cli.go:244](internal/cli/cli.go:244),
   [offsets.go:94](internal/cli/offsets.go:94), benchmark). Sem unificar, o
   servidor cria o dataset e o `nishidb query` seguinte não abre.
6. **`partial` é impossível como marca no evento**: [merge.go:44](internal/materializer/merge.go:44)
   preserva campo ausente do patch, então a marca nunca poderia ser limpa nem
   atribuída. Responde a questão aberta 1 do rascunho.
7. **`_meta` é nome válido hoje** — `isIdentName` aceita `[a-z0-9_]+`
   ([spec.go:336](internal/schema/spec.go:336)); nenhum fixture usa `_` inicial.
8. **`ErrorCategory` nunca é serializada** — métricas usam o nome
   ([kafka/metricas.go:65](internal/kafka/metricas.go:65)). Inserir categoria é
   seguro. O precedente é `CategoryEventTooLarge`, acrescentada um ciclo antes
   pelo mesmo motivo.
9. **`ScanLive()`** ([store.go:1609](internal/storage/store.go:1609)) é a
   primitiva do backfill: rowID crescente, **limite superior em snapshot** — o
   que separa sozinho o histórico do que o writer vivo já grava.
10. **`ipv4` não aceita bitmap** ([spec.go:308](internal/schema/spec.go:308)) e
    `index: hash` não tem motor ([compile.go:34-45](internal/schema/compile.go:34)).
    O exemplo do rascunho (`probe_ip: ipv4 + hash`) seria inerte nas duas pontas.
11. **A baseline de lint é `388`**, lida de `.golangci-baseline` com ratchet
    bidirecional. **O próximo débito é o 76.**

## Errata (25/08/2026): a motivação do campo json era outra

O plano abaixo foi escrito supondo que `persistent` existia para tornar
consultável a chave que alguém filtraria — daí a forma "declarado é o que
precisa de índice, o resto fica no json", e daí o corte por frequência (259
colunas cobrindo 97% das ocorrências) como critério de escolha.

**A motivação real era preservar TUDO para estudo e debug depois.** Com ela, o
corte por frequência mede a coisa errada: para investigação, a cauda de chave
vista uma vez só é justamente o que se quer, e descartar 83% do vocabulário
porque ele responde por 3% das aparições otimiza consulta às custas do
propósito.

Duas consequências:

1. **A preservação já estava atendida, e sempre esteve.** O documento é gravado
   verbatim; nada se perde nem se perdia. O que a promoção a coluna acrescenta
   não é preservação, é poder ESCREVER a pergunta — sem coluna,
   `WHERE persistent.x = 'y'` não é sequer expressável.
2. **O universo de chaves não é governável pelo dataset.** Quem produz os
   eventos é a área de integração, que pode passar a emitir um campo novo do
   dia para a noite. Escolher hoje a lista do que virará coluna é apostar
   contra isso.

Daí a fase F8, que não estava no plano: descoberta automática das chaves de
dentro do documento (`unknownFields: include` no campo). As duas decisões que a
tornaram viável já estavam tomadas aqui por outras razões — a coluna derivada
fica fora do `SELECT *`, e falha de extração nunca derruba o evento.

O que a descoberta NÃO resolve, e continua sendo o backfill: campo descoberto
não indexa, então o filtro sobre ele é varredura. Registrado como evolução
futura, junto com tornar o armazenamento do documento opcional — hoje ele e as
colunas são guardados os dois.

## Decisões tomadas

| # | Decisão | Escolha |
|---|---|---|
| D1 | `SELECT persistent.*` e `maxNestedCols` | **Entram**, em fase própria (F5) |
| D2 | `extract` em dataset com linhas | **Recusar a subida** — é decisão de criação |
| D3 | Default de `onMaxCols` | **`ignore`** — descarta o campo, conta, loga, segue |
| D4 | Bloco `schema:` recusa chave desconhecida | **Não** — mantém permissivo |
| D5 | Manifest v2 | Só a partir da primeira declaração com backfill (preserva rollback) |
| D6 | Capability do índice | **Dinâmica** — transição monotônica `false→true`, sem restart |
| D7 | `frequency` em `_meta.fields` | **Fora da v1** — seria varredura, ou contador que nasce zerado |

**Mitigação de D4.** Sem recusa de chave desconhecida, `maxColls` com typo vira
no-op silencioso. A compensação é a mesma forma do `maxRows`: **publicar o valor
efetivo** de `maxCols`/`onMaxCols` em `/v1/metadata`
([documentos.go:56](internal/server/documentos.go:56) é o molde) e no log de
subida, para que o typo apareça como "continua no default" num lugar conferível.

---

## Fases

Cada fase entrega algo sozinha e é commitável.

### F0 — Reserva do prefixo `_` em nome de dataset

`if` próprio em [spec.go:336](internal/schema/spec.go:336), **fora** de
`isIdentName` (que também valida alias em `:465`, e alias `_x` continua aceito).
A mensagem nomeia a regra e faz referência cruzada ao comentário de
[spec.go:279-282](internal/schema/spec.go:279), que fala de nome de **campo** —
sem isso os dois se leem como contradição.

**Entrega:** o espaço `_` fica protegido antes de existir dataset que o ocupe.
**Testes:** caso na tabela de recusa de `spec_test.go`; alias `_x` aceito.
**Risco:** nenhum.

### F1 — Teto de campos: `maxCols`, `onMaxCols` e o bug do dicionário

Ordem interna:

1. `MaxCols`/`OnMaxCols` em `rawSchema` ([spec.go:145](internal/schema/spec.go:145))
   e em `Compiled`, validados no molde de [spec.go:581-588](internal/schema/spec.go:581).
   Default `maxCols = 65535` (qualquer outro número aplicaria política a quem não
   a escolheu).
2. Guarda `c.dict.Len() >= c.MaxCols` em `ResolveUnknown`, **antes** de
   [compile.go:490](internal/schema/compile.go:490), devolvendo
   `schema.ErrTetoDeCampos`.
3. Ramo `continue` em [patch.go:91-93](internal/materializer/patch.go:91) para
   `ignore`.
4. `CategoryFieldsExhausted` declarada **depois** de `CategoryWriterFailed` —
   fora da faixa de `BadEvent()`, que não muda. Cai em
   [writer.go:219](internal/kafka/writer.go:219) e para de forma observável, que
   é a semântica de `stop`.
5. Sentinela `ErrDictFull` em [fielddict.go:206](internal/storage/fielddict/fielddict.go:206)
   como rede de segurança nos caminhos de abertura.
6. Contador em `Compiled` + `nishidb_dataset_fields{,_dropped_total}` no coletor
   pull de [metricas_dataset.go:76-181](internal/server/metricas_dataset.go:76);
   os números vão para `DatasetStats`/`StorageStats`, **fora** do `if cons != nil`
   de [documentos.go:156](internal/server/documentos.go:156). Log uma vez por dataset.
7. `maxCols`/`onMaxCols` efetivos publicados em `/v1/metadata` (mitigação de D4).

**Entrega:** o dataset que chega ao teto **deixa de parar a ingestão para
sempre**. É o bug corrigido, sem feature nova visível.
**Testes:** bancada Kafka com construtor novo `novaBancadaCom(t, particoes, opts)`
— o atual chama `carregaSpec()`+`abre()` dentro do construtor
([harness_test.go:98-102](internal/kafka/harness_test.go:98)), então setar o
campo depois chega tarde. Dois testes: `maxCols` no YAML com dict real
(`Aplicados == 3`, entidade sem o excedente) e dublê provando o ramo de rede.
`TestApplyFalhaDoDicionarioEhFailed` passa **sem alteração**.
**Risco:** médio, concentrado na categoria nova — mitigado por ficar fora da faixa.

### F2 — `_meta.fields`: introspecção por SQL

- **Lexer:** `TokenDot` em [token.go](internal/query/lexer/token.go);
  [token_test.go:124](internal/query/lexer/token_test.go:124) fixa 28 tipos com
  `Fatalf` → vira 29. As guardas de mensagem (`FROM x.`, `FROM .x`) entram
  **antes** do token, senão a regressão de diagnóstico do `.` passa despercebida.
- **Parser:** `Namespace` aditivo em [ast.go:18](internal/query/parser/ast.go:18).
  Continua LL(1): o `.` **é** o lookahead — ao contrário do `BY` recusado em
  [clausulas.go:5-9](internal/query/parser/clausulas.go:5).
- **Motor:** pacote `internal/query/meta` com Store virtual codificando row sob
  demanda; um desvio em [engine.go:82-94](internal/query/engine.go:82) serve
  servidor e CLI de uma vez. O `*binder.Error` é interceptado e tem o nome
  reescrito para `_meta.fields`, preservando `Pos` e o `Unwrap()`.
- **Colunas:** `name, type, auto, index, parent, indexed, id`. Sem `frequency`
  (D7). `id` documentado como **identidade estável**, nunca "ordem de
  descoberta" — [compile.go:252](internal/schema/compile.go:252) faz
  `sort.Strings` antes do `LookupOrCreate`, então o FieldID inicial é
  alfabético. `index: hash` sai `indexed=false` e o texto tem de dizer que não
  há motor hash.
- **JDBC:** `SqlScanner` abandona o palpite diante do `.` (com `boolean vistoFrom`),
  em vez de extrair `_meta`. `getSchemas()` continua vazio e `NishiUrl` continua
  recusando caminho com mais de um segmento — `_meta` é namespace de tabela
  virtual, não catálogo.

**Entrega:** `SELECT name, type, auto, index FROM _meta.fields` responde "o que
este dataset descobriu", que hoje não tem resposta por SQL nenhuma. **Dá o
`TokenDot` de graça para F3 e F5.**
**Por que antes de F3:** sem ela o operador declara `extract` às cegas e descobre
o erro quando recriar o dataset já não cabe na janela de ~18,8 h.
**Risco:** baixo-médio — o real é a regressão de diagnóstico do `.`.

### F3 — `extract`: declarar, compilar, materializar

```yaml
fields:
  persistent:
    type: json
    extract:
      - path: probeIpNode
        as: probe_ip
        type: string      # ipv4 NÃO aceita bitmap
```

- **Schema:** `ExtractSpec` em [spec.go:93](internal/schema/spec.go:93);
  `CompiledField.Parent/Path`; `byPath` **fora** de `byName`.
  Recusas novas: pai excluído, pai fora do include, `as` colidindo com nome,
  alias, outro `as`, ou **nome já no dicionário como campo de topo
  autodescoberto** (senão a derivada adota o histórico de outra coluna).
  `extract` só sobre `type: json`.
- **`SELECT *` não expande:** segundo predicado em `camposOrdenados`
  ([compile.go:436-438](internal/schema/compile.go:436) já filtra `IsSystem`),
  mais `FieldsParaMetadata()` — o gate de paridade de
  `documentos_test.go:464-476` é unidirecional, então metadata como superset é
  legal, e a coluna derivada aparece no DBeaver mesmo fora do `*`.
- **Materialização:** segunda passada em [patch.go](internal/materializer/patch.go)
  entre `:156` e `:162`, com jsonp sobre o `f.Value` do pai. Regras duras:
  pai `null` → todas as derivadas `null` sem abrir parser; **qualquer** falha do
  jsonp em qualquer nível vira `null` + contador, **nunca** erro propagado;
  chave do pai duplicada usa a **última** ocorrência (last-wins do `Parse`),
  senão a derivada contradiz a coluna json do mesmo record.
  Guarda no ponto e forma do campo de sistema ([patch.go:80-84](internal/materializer/patch.go:80)):
  chave de topo homônima **não** escreve na derivada.
- **Listas:** `path` que resolve para array vira `null` + contador (achatar em
  linhas seria outra feature, e não há JOIN para remontar).
- **D2:** recusa de subida em dataset com `RowCount > 0`, feita **antes** do
  `storage.Open` — o `RowCount` vem do manifest via `checkpoint.Load` e o
  fielddict abre read-only. Sem isso, uma única subida abortada grava o `as` no
  dicionário para sempre, porque `resolveBitmapFields`
  ([store.go:682](internal/storage/store.go:682)) faz `LookupOrCreate` com fsync
  **antes** da validação de `:687`.

**Entrega:** dataset novo com coluna derivada projetável, `_writtenAt`/merge
coerentes, documento original íntegro na coluna json.
**Testes:** variante do alloc gate com fixture que declara `extract` (a segunda
passada não pode alocar por evento).
**Risco:** médio-alto, concentrado na segunda passada.

### F4 — `extract` indexado + fiação única de bitmap

- `schema.CamposComBitmap(spec) []string` em `internal/schema`, já incluindo os
  `as`, chamada pelos **quatro** sítios ([server.go:300](internal/server/server.go:300),
  [cli.go:244](internal/cli/cli.go:244), [offsets.go:94](internal/cli/offsets.go:94),
  benchmark). Sem isso o servidor cria o dataset com o índice e o `nishidb query`
  seguinte bate em `ErrDatasetCorrupt` pela divergência (1) de
  [store.go:838-851](internal/storage/store.go:838).
- `index` no extract reusando o switch de [spec.go:476-491](internal/schema/spec.go:476).
- Aviso de `index: hash` sem motor, estendendo `avisaChavesSemMotor`
  ([server.go:467](internal/server/server.go:467)).
- `origem=` no REMARKS do JDBC + campo `Source` em `ColunaMeta`.

**Entrega:** `WHERE probe_ip = 'x'` usa bitmap num dataset novo, e o mesmo
dataset abre pelo CLI e pelo servidor. **Corta aqui e não antes** porque a fiação
única é o que impede o modo de falha cruzado servidor↔CLI.
**Risco:** médio, quase todo em call sites.

### F5 — `SELECT persistent.*` e `maxNestedCols`

- **Projeção:** `parser.projecao()` aceita `ident '.' '*'`, reusando o `TokenDot`
  de F2. `SELECT *, x` continua recusado ([parser.go:127-129](internal/query/parser/parser.go:127)),
  e a mesma regra vale para `persistent.*`.
- **Binder:** expande por `FieldsByParent(p)`, no molde de
  [binder.go:251-266](internal/query/binder/binder.go:251). Pai sem derivadas é
  recusa nomeando o pai, não projeção vazia.
- **Ambiguidade:** `SELECT "persistent.probeIpNode"` (quoted) já é válido hoje e
  designa um **nome literal de coluna**; `persistent.probeIpNode` sem aspas passa
  a designar o **caminho**. Se um `as` contiver ponto, os dois colidem — recusar
  ponto em `as` na compilação (F3) fecha a categoria inteira.
- **`maxNestedCols`:** molde completo do `maxRows` nas quatro pontas — YAML
  ponteiro, validação `> 0`, `Compiled`, `/v1/metadata`, campo na `Requisicao`
  ([protocolo.go:44-50](internal/protocolo/protocolo.go:44)), função de teto no
  servidor que **recusa acima e nunca clampa** ([query.go:189-208](internal/server/query.go:189)),
  e `setMaxNestedCols` no `Statement` omitindo quando o pedido excede
  ([NishiStatement.java:121-127](clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiStatement.java:121)).
  Ao estourar, **recusar nomeando o limite** — truncar entregaria resultado com
  colunas faltando e cara de completo.

**Entrega:** `SELECT persistent.*` devolve as derivadas do pai sem que o usuário
saiba os nomes, com teto que o cliente encurta e o servidor recusa acima.
**Risco:** médio — gramática, mas com o token já provado em F2.

### F6 — Backfill de índice bitmap (fecha o débito 22)

- **Manifest v2** com `{BackfillCursor, BackfillLimit}` em `BitmapInfo`
  ([manifest.go:89](internal/storage/checkpoint/manifest.go:89)), gravado **só a
  partir da primeira declaração com backfill** (D5), com o decodificador
  aceitando v1 e v2. Os dois testes existentes entram nominalmente:
  [manifest_test.go:127](internal/storage/checkpoint/manifest_test.go:127)
  (`raw[4] != 1`) e `:104-110` (fórmula `96 + segs*36 + bitmaps*10` com `cap` exata).
- **Guarda transformada**, não removida: `Options.BackfillBitmaps` autoriza a
  divergência (2) de [store.go:858-872](internal/storage/store.go:858),
  preservando a assimetria com `AllowBitmapDrop`
  ([store_sc_lacunas_test.go:167](internal/storage/store_sc_lacunas_test.go:167)).
- **O estado nasce depois do `switch` de recovery** e antes de
  [store.go:752](internal/storage/store.go:752) — `nextRowID` é zero no laço dos
  bitmaps, e `(0,0)` não pode significar "completo".
- **`internal/storage/backfill.go`** no molde do coletor de expiração
  ([expiracao.go:283-365](internal/storage/expiracao.go:283)): cursor persistente,
  `MaxPorPassada`/`TempoPorPassada`, reagendamento imediato ao bater o teto.
  Roda **como trabalho do writer, sob o `wmu`** — `Set/Clear` são exclusivos do
  writer e compartilham `ix.scratch` ([bitmap.go:115](internal/index/bitmap/bitmap.go:115)),
  então lock novo seria pior. Varredura por `ScanLive()` sem o filtro de prazo.
- **`fsyncDir` dos diretórios de índice** dentro de `msyncBitmaps`: `bitmap.go:125`
  faz `MkdirAll` sem fsync e os `.bm` nascem via mmap, então sem isso o manifest
  declara completo um índice cujas entradas de diretório podem não ter sobrevivido.
- **`ErrTooLarge`:** campo autodescoberto pode ter valores acima de
  `MaxIndexedValueBytes` — `validatePayloadAgainstDataset` só valida campos já em
  `s.bitmaps` ([store.go:1365](internal/storage/store.go:1365)). Ramo próprio:
  não é sticky, não é pulado em silêncio; a passada aborta e o campo fica
  incompleto. **Declarar o índice passa a recusar eventos que ontem entravam** —
  isso precisa estar escrito na documentação.
- **Tipo fixado** validado dentro de `resolveBitmapFields`, antes de
  `bitmap.Open` — mover para o `Compile` a faria rodar depois de o Open já ter
  criado o índice e gravado o manifest.
- **Capability dinâmica** (D6) por `atomic.Bool` com mapa imóvel: `HasBitmap` lê
  hoje um mapa sem lock ([store.go:898](internal/storage/store.go:898)) e é
  consultado por query, e mutá-lo seria a mesma família dos débitos 60/69/70.
- **Planner:** `eqTermOf` passa a usar `EffectiveType()` com **nil check
  primeiro** (o receiver é ponteiro e desreferencia em `compile.go:127`; o
  contrato diz "NUNCA entra em panic"). O par de asserções de
  [planner_test.go:570-591](internal/query/planner/planner_test.go:570) é
  **substituído**, não apagado: a invariante nova é "campo auto sem índice
  completo degrada; com índice completo, indexa".
- **Remoção:** manifest primeiro, arquivos depois, mais varredura de órfãos —
  diretório de campo presente em `campos` mas ausente do manifest de entrada é
  removido antes do `bitmap.Open`, senão redeclarar depois de dropar adota o
  `values.dict` antigo.
- **Observabilidade:** quatro séries + `/v1/stats`, na forma da compaction e da
  expiração ([metricas_dataset.go:123-178](internal/server/metricas_dataset.go:123)).

**Nota:** a compaction tombstona oportunisticamente
([compaction.go:186-193](internal/storage/compaction.go:186)), então a asserção
é "bits idênticos a menos das rows tombstonadas", não "nenhum bit invalidado".

**Entrega:** `index: bitmap` sobre campo autodescoberto num dataset com linhas
passa a ser possível, e a coluna `indexed` de F2 deixa de ser redundante.
**Última por risco:** é a única fase que toca formato durável, recovery e
concorrência do writer ao mesmo tempo.
**Risco:** alto.

### F7 — NDJSON esparso: débito 76 (investigação, sem código)

Registrar o critério de entrada e o defeito latente: `Arrays.fill(tipo, null)` em
[StreamCursor.java:255](clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/StreamCursor.java:255)
faz a coluna ausente ler o valor da **linha anterior**. Implementar inverteria
três invariantes travadas (`ndjson_test.go`, o fuzz com `encoding/json` como
oráculo, e `StreamCursorTest.colunaAusenteNaLinhaFalhaAlto`). O desperdício só
existe com 545+ colunas por linha; o `events` tem 49, e a decisão de que
`SELECT *` não expande derivadas já o evita.

---

## Fora de escopo, com razão

- **`extract` retroativo** (coluna derivada em linhas antigas): exigiria
  reescrever payloads gravados — é reingestão, não backfill de índice.
- **Índice hash físico:** é a Fase 8. Entra só o **aviso** de que a chave não
  tem efeito.
- **Marca `partial` e campo de transbordo:** a marca morre na semântica do merge
  ([merge.go:44](internal/materializer/merge.go:44)); o transbordo recria dentro
  de uma coluna o crescimento sem teto que o teto existe para conter.
- **Reconstrução do fielddict:** append-only, a compaction não o toca. F1 compra
  tempo e observabilidade, não reversibilidade.
- **`_meta.datasets` e `getSchemas()`:** listar o namespace sem rota que enumere
  suas tabelas põe o catálogo no cliente.
- **Consulta a caminho sem extração declarada** (`WHERE persistent.x = ...`):
  exigiria parsear documento por linha durante a varredura.

## Verificação

**Por fase, antes de cada commit:** `make test`, `make test-race`, `make lint`
com o arquivo `.golangci-baseline` (hoje **388**) atualizado no mesmo PR — o
ratchet **recusa nas duas direções**.

**Ponta a ponta, com o binário:**

1. **F0/F1:** subir com `name: _x` e conferir a recusa nomeando a regra; subir
   com `maxCols` baixo, ingerir evento com chave excedente, e conferir em
   `/v1/stats` que o evento entrou e o contador de descarte subiu — o writer
   **não** para.
2. **F2:** `nishidb query "SELECT name, type, auto, index FROM _meta.fields"`, e
   a mesma consulta pelo DBeaver via JDBC (é o caso que hoje quebraria em
   silêncio).
3. **F3/F4:** dataset novo com `extract` e `index: bitmap`; ingerir; conferir que
   `SELECT *` **não** traz a derivada, que `SELECT probe_ip` traz, e que
   `WHERE probe_ip = 'x'` usa `BitmapScan` (pelo plano). Abrir o mesmo dataset
   pelo CLI **e** pelo servidor — é o modo de falha cruzado da F4.
4. **F5:** `SELECT persistent.*`; pedir `maxNestedCols` acima do teto e conferir
   a recusa nomeando o limite; conferir que o driver **omite** em vez de errar.
5. **F6:** dataset com linhas, declarar índice, subir com `BackfillBitmaps`;
   durante o backfill a query **varre e acerta**; ao terminar, passa a indexar;
   comparar o conjunto de linhas antes e depois — tem de ser idêntico a menos das
   tombstonadas. Matar o processo no meio do backfill e conferir que o reopen não
   declara completo.

**Documentação (DoD):** `architecture.md`, os `.puml` afetados
(`recovery_flow.puml:40` cita o débito 22), `debitos.md` (fecha o **22**, abre o
**76**) e `README.md`. Regra de sequência: documento que cita `TestXxx` entra
**depois** do teste existir, no mesmo commit — [evidencias_test.go:26-33](internal/cli/evidencias_test.go:26)
verifica isso.

**Ao aprovar:** copiar este plano para `planning/` e substituir o rascunho v2,
que tem quatro afirmações refutadas registradas acima.
