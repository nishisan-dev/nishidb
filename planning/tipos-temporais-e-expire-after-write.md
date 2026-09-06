# Tipos temporais (`datetime`/`time`) e expiração por escrita (`expireAfterWrite`)

## Contexto

Duas necessidades vindas do piloto `events` (alarmística Netcool, host `10.200.20.218`, ~17,8M rows, `mode: merge`, key `identifier`):

**1. `firstSeen`/`lastSeen` são texto.** Chegam como `"Aug 22, 2026 08:54:02 PM"` — o `DateFormat.MEDIUM` do Java em locale `en_US`, sem fuso. Hoje estão declarados `type: string`, e string compara com `bytes.Compare` ([coerce.go:124](internal/query/binder/coerce.go:124)): a ordem lexicográfica desse formato **não é cronológica** (`Aug` < `Dec` < `Feb`…), então filtro por janela temporal é hoje impossível, não apenas lento. O tipo `timestamp` existente não resolve: seu parser ([typeparse.go:170](internal/typeparse/typeparse.go:170)) exige RFC3339 com offset obrigatório e recusa esse texto com `errTsNoTZ`.

**2. O estado vivo cresce sem limite.** [`docs/piloto-events.md:135`](docs/piloto-events.md:135) já registra: *"Sem TTL (M6): entidade materializada nunca expira."* O `ttl:{field,type}` do §56 é parseado, validado e **ignorado** — zero consumidores. `expireAfterWrite` (idle timeout, renovado a cada escrita) é o que casa com `mode: merge`: alarme ativo nunca expira, alarme abandonado sai sozinho.

**Resultado esperado:** range query por data nos campos temporais, e um mecanismo de expiração que estabiliza o key set vivo sem descartar entidade ativa.

### Decisões tomadas com o usuário

| # | Decisão |
|---|---|
| 1 | **Novos field types** `datetime` e `time` — não ampliar a gramática do `timestamp` |
| 2 | **`format:` como padrão declarável** com subconjunto **fechado** de tokens, compilado no load |
| 3 | **Fuso = `time.Local` do host**, honrando a env `TZ`, sem declaração no YAML |
| 4 | **`expireAfterWrite` = idle timeout**, renovado a cada escrita |

---

# Parte 1 — Tipos `datetime` e `time`

## 1.1 Representação física: duas tags novas

```go
// internal/types/types.go
TypeDateTime FieldType = 0x0C // 8B LE, epoch millis UTC (wall-clock resolvido por time.Local na ingestão)
TypeTime     FieldType = 0x0D // 8B LE, millis desde a meia-noite, [0, 86_400_000)
```

Ambos de largura 8 com valor em `Value.I64` — [fields.go:60](internal/storage/encoding/fields.go:60) (`AppendField`) e [:128](internal/storage/encoding/fields.go:128) (`Next`) já tratam `w == 8` genericamente e **não mudam**; só `fieldWidth` ([:28-47](internal/storage/encoding/fields.go:28)) ganha as duas entradas.

**Por que `time` obriga tag própria:** millis-do-dia não é instante. Reusando `0x04`, a `celula` já casaria `timestamp × string` e `WHERE hora = '2026-08-13T12:00:00Z'` compararia `1786…` contra `68042000` — false sempre, **sem erro de bind**. Pior, [ndjson.go:88](internal/query/executor/ndjson.go:88) mandaria `68042000` para `appendTimestamp` e emitiria `"1970-01-01T18:54:02.000Z"`: uma mentira plausível, exatamente o que o comentário de [ndjson.go:50](internal/query/executor/ndjson.go:50) proíbe.

**Por que `datetime` também ganha tag**, ainda que fisicamente idêntico a `timestamp` depois de resolvido o fuso: com tag distinta, `fieldTypeNames` e `NomeDeTipo` ([protocol.go:210](internal/protocol/protocol.go:210)) publicam o nome declarado **de graça** em `/v1/metadata` e na linha de schema do `/v1/query`. Um tipo lógico sobre `0x04` exigiria um sistema de tipos sombra (`CompiledField.LogicalType` + `ColView` + `ColunaDe` + dois call sites) e faria `coerceLiteral` ramificar no tipo lógico *dentro* de um case chaveado pelo físico — "duas tabelas que só funcionam lidas juntas", o oposto do dono único que [coerce.go:66](internal/query/binder/coerce.go:66) declara.

A rigidez de `s.val.Type == in.want` ([predicate.go:100](internal/query/executor/predicate.go:100)) **não** é exercida entre colunas: `parser.Operand` ([ast.go:68](internal/query/parser/ast.go:68)) só carrega literal ou placeholder, e a v1 não compara coluna com coluna.

**Indexabilidade: nenhum dos dois, e sem escrever código.** `bitmapIndexable`/`hashIndexable` ([spec.go:204-210](internal/schema/spec.go:204)) comparam nomes YAML contra listas fechadas; `datetime`/`time` não entram e já são recusados pela mensagem existente. É a decisão certa: o bitmap é denso (~2,2 MB por valor distinto com 17,8M rows), e `time` com resolução de segundo tem 86.400 distintos ≈ 190 GB. Exige **teste explícito** — `default:` correto por acidente vira errado na próxima refatoração.

**`auto` nunca infere os novos tipos.** `InferType` ([typeconv.go:136](internal/materializer/typeconv.go:136)) não muda: string continua `string`, como já vale para `ipv4` ([architecture.md:694](docs/architecture.md:694)). Campo `auto` não tem `format:`, logo não teria parser — inferir `datetime` produziria `CompiledField` com função nil e panic no binder. Precisa de guard no `Compile`.

## 1.2 A gramática do `format:`

Padrão declarável, tokens **fechados**, compilado uma vez no load para um plano de parsing zero-alloc.

| Token | Significado |
|---|---|
| `yyyy` | ano, 4 dígitos |
| `MM` / `M` | mês numérico, 2 / 1-2 dígitos |
| `MMM` | mês abreviado en_US (`Jan`..`Dec`), qualquer caixa |
| `dd` / `d` | dia, 2 / 1-2 dígitos |
| `HH` / `H` | hora 24h, 2 / 1-2 dígitos |
| `hh` / `h` | hora 12h, 2 / 1-2 dígitos |
| `mm` / `ss` | minuto / segundo, 2 dígitos |
| `SSS` | milissegundos, 3 dígitos |
| `a` | meridiano `AM`/`PM`, qualquer caixa |
| `'texto'` | literal escapado |
| outros chars | literais (`,` `:` `/` `-` espaço…) |

O piloto vira `format: "MMM d, yyyy hh:mm:ss a"`.

**Regras de validação, todas no LOAD** (em `validate()`, no laço de campos [spec.go:311-347](internal/schema/spec.go:311)):

1. `datetime`/`time` **sem** `format:` → recusa. Sem default: um `format` esquecido escolheria um parser que recusa 100% dos eventos e, sob `log-and-skip` (§101), isso é um dataset que não ingere nada — só WARNs.
2. Token desconhecido ou token de fuso (`Z`, `X`, `z`) → recusa nomeando os suportados e dizendo que fuso vem de `time.Local`; campo com offset na origem deve usar `type: timestamp`.
3. Token repetido → recusa.
4. Dois tokens de largura variável **adjacentes sem literal entre eles** → recusa por ambiguidade (`dM` não é parseável).
5. `h`/`hh` exige `a` presente; `HH`/`H` proíbe `a`.
6. `datetime` exige ano+mês+dia+hora+minuto; `time` **recusa** tokens de data.
7. `format:` em campo cujo tipo não o usa → recusa. **Crítico:** `FieldSpec` é decodificado sem `KnownFields` ([spec.go:150](internal/schema/spec.go:150)), então assim que `Format` existir no struct, `format: x` num `type: string` decodifica com sucesso e não faz nada — configuração sem leitor, o pecado que o débito 37 registra.

O compilador de padrão e a lista de tokens têm **um dono**: `internal/typeparse`. O `schema` valida chamando o compilador, não com uma segunda lista que envelheceria em silêncio. É a mesma disciplina da `celula` e a mitigação estrutural do risco R3.

## 1.3 O parser

Arquivo novo `internal/typeparse/datetime.go`, no estilo artesanal de `RFC3339Millis` — cursor sobre `[]byte`, zero alocação, erros estáticos pré-alocados.

```go
// Layout é o padrão compilado. Construído UMA vez no load; o parse é uma
// interpretação sobre []byte que não aloca.
type Layout struct { /* toks []tok; literais []byte; hasMeridiem, hasDate bool */ }

func CompileLayout(padrao string, t types.FieldType) (*Layout, error)
func SupportedTokens() []string // para a mensagem de recusa

// DateTime resolve o instante em loc. loc é PARÂMETRO, não time.Local
// capturado dentro: um teste precisa fixar o fuso sem mexer num global do
// processo, e a assinatura deixa visível que o resultado depende do fuso —
// a propriedade mais fácil de esquecer deste tipo.
func (l *Layout) DateTime(raw []byte, loc *time.Location) (int64, error)

// TimeOfDay devolve millis desde a meia-noite. NÃO recebe fuso: hora do dia
// não é instante, não há DST e não há nada a resolver.
func (l *Layout) TimeOfDay(raw []byte) (int64, error)

const MillisPerDay = 86_400_000
```

Helpers novos ao lado de `Digits2`/`Digits4` ([typeparse.go:333-348](internal/typeparse/typeparse.go:333)): `digitos1a2`, `digitos3`, `monthAbbrEnUS` (array `[12][3]byte` em minúsculas, folda a caixa — 12 comparações de 3 bytes custam menos que um hash e um map alocaria na init).

**12h→24h:** `h24 := h12 % 12; if pm { h24 += 12 }`. Zera o 12 AM e preserva 1..11. É a única regra deste formato que não sai certa por acidente.

**O núcleo compartilhado — e a sutileza que quase todo mundo erra:**

```go
// montaInstanteLocal valida a data e resolve o instante em loc.
//
// A validade da DATA é conferida em UTC, não em loc. time.Date normaliza
// componente inválido (30/02 vira 02/03) e a validação canônica é comparar o
// resultado com a entrada — mas em loc essa comparação tem falso positivo:
// existem zonas cuja transição de DST é às 23h ou à meia-noite
// (America/Santiago, America/Havana, Asia/Beirut). Numa delas "23:30" numa
// noite de adiantamento normaliza para 00:30 do DIA SEGUINTE, e o round-trip
// acusaria "data inexistente" para uma data válida. UTC não tem DST.
//
// DST no INSTANTE é aceito como time.Date o resolve, e isso é decisão:
//   - hora AMBÍGUA (recuo): escolhe a PRIMEIRA ocorrência. Uma hora por ano
//     fica deslocada e não há como acertar — a informação que desempataria
//     não está no dado;
//   - hora INEXISTENTE (adiantamento): normaliza para frente.
// Recusar os dois transformaria uma hora por ano de saída legítima em
// TYPE_CONVERSION sob log-and-skip — descarte silencioso, pior que gravar o
// instante determinístico. O que importa é ingestão e binder chegarem ao
// MESMO número para o mesmo texto (R3), e time.Date garante isso.
func montaInstanteLocal(ano, mes, dia, h, min, seg, millis int, loc *time.Location) (int64, error)
```

## 1.4 Query

**`celula`** ([coerce.go:122-159](internal/query/binder/coerce.go:122)) ganha duas linhas; `admitsOp` ([:163](internal/query/binder/coerce.go:163)) **não muda** (ambos totalmente ordenados, caem no `default: true`):

| Coluna | literal string | literal int | ops |
|---|---|---|---|
| `datetime` | parse pelo `format:` do campo, em `time.Local` → millis UTC | epoch millis | todos |
| `time` | parse pelo `format:` do campo → millis do dia | millis do dia, `[0, 86400000)` | todos |

**O literal SQL usa o `format:` do campo — não RFC3339, e não os dois.** É a razão de existir do `internal/typeparse` compartilhado: a propriedade que `TestIngestaoEQueryConcordamPorTipo` ([types_test.go:82](internal/query/types_test.go:82)) prova é *o texto que o produtor mandou, escrito como literal SQL, acha a row*. Aceitar RFC3339 **também** cria um texto que a query aceita e a ingestão recusa — a divergência exata que o R3 nomeia. A saída independente de formato já existe e é o inteiro (`momento >= 1787000000000`), simétrico ao `timestamp` que já aceita epoch millis ([coerce.go:213](internal/query/binder/coerce.go:213)).

`coerceLiteral` ([:178-263](internal/query/binder/coerce.go:178)) já recebe `f *schema.CompiledField` — zero fiação nova. `coerceParam` ([:275](internal/query/binder/coerce.go:275)) não muda: delega ao `coerceLiteral`.

`erroLiteral` ([binder.go:474](internal/query/binder/binder.go:474)) passa a nomear o formato declarado — sem isso a mensagem é inacionável.

## 1.5 Saída

**NDJSON** — `datetime` sai byte a byte igual a `timestamp` (reusa `appendTimestamp`, [ndjson.go:307](internal/query/executor/ndjson.go:307)). `time` ganha `appendHoraDoDia` → `"18:54:02.000"`, **sem `Z`**: não há instante e não há fuso a anunciar; reusa `append2`/`append3` ([:351](internal/query/executor/ndjson.go:351)).

> **Consequência a documentar em letras grandes: `datetime` não faz round-trip textual.** Entra `Aug 22, 2026 08:54:02 PM`, sai `"2026-08-22T23:54:02.000Z"` sob `TZ=America/Sao_Paulo`. É o preço de resolver a ambiguidade na porta. Ecoar o `format:` obrigaria `AppendValue` a receber schema (quebrando a assinatura pura que mantém o arquivo sem `fmt`); renderizar em local faria o mesmo dataset servido de dois hosts produzir bytes diferentes.

**JDBC** ([jdbc.go:31-53](internal/protocol/jdbc.go:31)): `datetime` → `TIMESTAMP`, `time` → `TIME` (constante nova). `formatValue` do planner ([plan.go:168](internal/query/planner/plan.go:168)) precisa das duas tags no case de `FormatInt`, senão o diagnóstico imprime hex de `Bytes` nil.

## 1.6 Tasks

| # | Task | Arquivos |
|---|---|---|
| T1 | Tags + `fieldTypeNames` | [types.go:81-113](internal/types/types.go:81) |
| T2 | Higiene preventiva: fechar o `default:` de largura fixa | [store.go:1249-1258](internal/storage/store.go:1249), [fakestorage.go:132-142](internal/testutil/fakestorage/fakestorage.go:132) |
| T3 | `fieldWidth` → 8 nas duas tags | [fields.go:28-47](internal/storage/encoding/fields.go:28) |
| T4 | Compilador de layout + parsers (independente, pode ir em paralelo) | `internal/typeparse/datetime.go` (novo) |
| T5 | `FieldSpec.Format`, nomes YAML, as 7 regras de validação | [spec.go:63-67](internal/schema/spec.go:63), [:160-179](internal/schema/spec.go:160), [:311-347](internal/schema/spec.go:311) |
| T6 | `CompiledField.Format`/`Layout` + resolução no laço + guard de `auto` temporal | [compile.go:89-103](internal/schema/compile.go:89), [:229-280](internal/schema/compile.go:229) |
| T7 | `Convert` com layouts + call site do `PatchBuffer` | [typeconv.go:39](internal/materializer/typeconv.go:39), [patch.go:117](internal/materializer/patch.go:117) |
| T8 | `celula` + `coerceLiteral` + mensagem de erro | [coerce.go:122-263](internal/query/binder/coerce.go:122), [binder.go:474](internal/query/binder/binder.go:474) |
| T9 | `AppendValue` + `appendHoraDoDia`; `formatValue` | [ndjson.go:62-142](internal/query/executor/ndjson.go:62), [plan.go:168](internal/query/planner/plan.go:168) |
| T10 | `JDBCTime` + `JDBCDeTipo` | [jdbc.go:31-53](internal/protocol/jdbc.go:31) |
| T11 | Suíte de testes (§1.7) | vários |
| T12 | Docs: `architecture.md:680-700`, spec §17/§18/§73, §3.6 do design | vários |
| T13 | **Separável:** `typeHint` durável para campo **declarado** + recusa de retipo na subida | [compile.go](internal/schema/compile.go), [fielddict.go](internal/storage/fielddict/fielddict.go) |

**Sobre T2:** o `default: bytes.Equal(a.Bytes, b.Bytes)` faria duas tags novas de largura fixa compararem `nil == nil → true`. Verificado: `valueEqual` só é chamado quando `s.bitmaps[pid] != nil` ([store.go:1282](internal/storage/store.go:1282)) e `datetime`/`time` não são indexáveis — então o defeito é **latente, não ativo**. Fechar mesmo assim custa uma linha e evita que a próxima tag de largura fixa indexável o ative.

**Sobre T13:** o tipo de um campo **declarado** não é gravado em lugar nenhum do dataset — o `typeHint` do `fields.dict` só é escrito para campo `auto` ([compile.go:126](internal/schema/compile.go:126)). Então trocar `type:` no YAML e reiniciar é aceito **em silêncio**. Fazer o `Compile` gravar o tipo declarado no `typeHint` e recusar conflito na abertura reusa a máquina pronta (`ErrTypeHintConflict`, registro append-only, [fielddict.go:60-66](internal/storage/fielddict/fielddict.go:60)); datasets existentes têm hint `0`, então a primeira abertura só grava. Transforma "dataset meio quebrado em silêncio" em "recusa na subida com a causa nomeada".

## 1.7 Testes

Nomes em português descrevendo o fenômeno, como manda a convenção do projeto.

**`internal/typeparse`** — `TestLayoutAceitaAAmostraRealDoPiloto`, `TestJavaMediumConverteMeiaNoiteEMeioDia` (12 AM→00, 12 PM→12), `TestAceitaDiaEHoraComUmDigito`, `TestAceitaMesEMeridianoEmQualquerCaixa`, `TestRecusaMesEmOutroIdioma`, `TestRecusaFusoColadoNoFim`, `TestRecusaDataInexistente`, `TestDateTimeResolveNoFusoRecebido` (mesmo texto em SP e UTC difere por 3h), `TestDateTimeEmHoraAmbiguaDeDSTEscolheAPrimeiraOcorrencia`, **`TestDataValidaNaViradaDeDSTDeMeiaNoiteNaoEhRecusada`** (`America/Santiago` 23:30 — o caso que só a validação em UTC salva), **`TestTimeOfDayNaoDependeDeFusoNemDeData`**, `TestLayoutRecusaTokensAdjacentesAmbiguos`, `TestParsersDeDataNaoAlocam` (`AllocsPerRun == 0`), `TestParsersDeDataNaoRetemEntrada`.

**`internal/schema`** — `TestDatetimeSemFormatEhRecusadoNoLoad`, `TestTokenDesconhecidoEhRecusadoNomeandoOsSuportados`, `TestFormatEmTipoQueNaoOUsaEhRecusado`, `TestTimeRecusaTokensDeData`, `TestDatetimeETimeNaoSaoIndexaveis`, **`TestCampoAutoComHintTemporalEhRecusadoNoCompile`**.

**`internal/materializer`** — `TestConvertDatetimeUsaOLayoutDoCampo`, `TestConvertDatetimeRecusaTextoEmOutroFormato` (RFC3339 num campo `MMM…` é TYPE_CONVERSION), `TestConvertTimeRecusaMillisForaDoDia`.

**`internal/protocol`** — estender `TestATabelaDeTiposDoSpec73` ([jdbc_test.go:13](internal/protocol/jdbc_test.go:13)) e criar **`TestTodaTagComNomeTemJDBCEspecifico`**: varrer `0x00..0x1F` e exigir `JDBCDeTipo != OTHER` para toda tag nomeada. Hoje a promessa de [jdbc.go:28-30](internal/protocol/jdbc.go:28) — *"acrescentar um FieldType sem passar por aqui tem de ser decisão, não esquecimento"* — **não é cobrada por nada**.

**O cruzado** — [`testdata/todos_os_tipos.yaml`](testdata/todos_os_tipos.yaml) ganha 4 campos (`datetime`/`time` × dois padrões) e `TestIngestaoEQueryConcordamPorTipo` os casos correspondentes. **Atenção de implementação:** a tabela de casos hoje é de strings constantes ([types_test.go:85-89](internal/query/types_test.go:85)); o caso `datetime = <epoch millis>` **não pode ser constante** (depende do `time.Local` do host) e precisa montar o `where` em runtime — é mudança de forma na tabela, não uma linha nova. Mais **`TestLiteralNoFormatoErradoNaoAchaEmSilencio`** (RFC3339 num campo `MMM…` tem de ser **erro de bind**, nunca resultado vazio).

**Subprocesso** — **`TestOFusoDoHostVemDaVariavelTZ`**: `time.Local` é resolvido uma vez por processo, então `t.Setenv("TZ", …)` depois disso não muda nada. Exige subprocesso com `TZ=` no env; padrão já existe em `internal/gates/`.

---

# Parte 2 — `expireAfterWrite`

## 2.1 A decisão que governa todas as outras

**A expiração é um PREDICADO DE LEITURA primeiro, e um coletor em background depois.**

A liveness já é decidida em três pontos — [store.go:862](internal/storage/store.go:862) (`lookup`), [:899](internal/storage/store.go:899) (`GetByRowID`), [:1386](internal/storage/store.go:1386) (`liveIterator`) — todos lendo a flag da location. Acrescentar `writtenAt + d > agora` nesses três lugares custa uma carga de 8 bytes em mmap por row e compra:

- nenhuma row aparece em query depois do prazo, **mesmo com o scheduler parado, atrasado ou nunca ligado**;
- `nishidb query` e `nishidb server` respondem a mesma coisa sobre o mesmo dataset — resolve o débito 66 para esta feature sem ligar goroutine no CLI;
- o scheduler deixa de ser mecanismo de **correção** e vira mecanismo de **espaço**.

O pacote `internal/query` não muda uma linha.

## 2.2 Ancoragem do instante de escrita

**`_writtenAt` (campo de sistema, fonte da verdade durável) + `expiry.idx` (acelerador derivado e reconstruível).**

`_writtenAt`, tipo `timestamp`, FieldID normal do dicionário, custo **11 bytes/record**. Injetado **no materializer** ([merge.go:44](internal/materializer/merge.go:44) e [:94](internal/materializer/merge.go:94)), nunca no Store — nos dois caminhos, merge e replace.

**`_writtenAt` e não `_expiresAt`:** o campo registra um **fato** (quando o writer escreveu), não uma **política**. Mudar `expireAfterWrite: 2h` → `4h` no YAML passa a valer imediatamente para as 17,8M rows existentes; com `_expiresAt` cada row carregaria a política congelada e o operador reescreveria o dataset inteiro por uma mudança de config.

- **`SELECT *`:** não aparece. Filtra-se dentro de `FieldsByID` ([compile.go:360](internal/schema/compile.go:360)), cujos dois únicos consumidores são exatamente os que devem filtrar (`binder.projecao()`, `metaDoDataset`), com um `FieldsByIDIncluindoSistema()` para quem precisa do conjunto todo. `SELECT _writtenAt` **funciona** — convenção `ROWID`/`xmin`.
- **Evento que traga `_writtenAt`:** recusa explícita em `PatchBuffer.Parse` ([patch.go:73](internal/materializer/patch.go:73)) — sobrescreveria o relógio do writer. E `schema.fields` com nome iniciado por `_` é recusado no `validate`: prefixo reservado.

**`expiry.idx`**, magic `NDBE`, `int64` de millis por rowId, no molde exato de `Sequences` ([rowindex.go:258-289](internal/storage/rowindex/rowindex.go:258)) — mesmo `openDense` ([:56](internal/storage/rowindex/rowindex.go:56)), mesmo header de 64B.

O que o torna **reconstruível por construção**: é gravado dentro de `applyRecord` ([store.go:1064](internal/storage/store.go:1064)), ao lado de `s.seqs.Set` ([:1142](internal/storage/store.go:1142)). `applyRecord` é o **único** caminho de aplicação de estado, chamado pelo `Put` **e** pelo replay do tail ([store_recovery.go:353](internal/storage/store_recovery.go:353)) — o replay o preenche a partir do `_writtenAt` do próprio record.

*Alternativas rejeitadas:* **header v2 com writeTime** — recusa alta na abertura, e recriar o dataset pelo tópico mata exatamente os alarmes de vida longa (os que importam) e preserva os efêmeros (os que a feature existe para matar); além disso datasets sem TTL pagariam 8 bytes/record para sempre. **`expiry.idx` sozinho** — sem fonte no record, o replay não o reconstrói: row nascida no tail lê `0`, e não existe interpretação segura de `0` (se "sem prazo", vazamento permanente; se "vencido", **apaga row viva num crash de rotina**).

**Migração sem recriar o dataset** — o prêmio desta escolha: entrada `0` significa **desconhecido**, não "vencido". Row desconhecida ganha `agora + d` gravado preguiçosamente na primeira varredura. Ligar `expireAfterWrite` num dataset de 17,8M rows já materializadas dá a todas **um prazo cheio de carência**, sem reprocessar um byte do Kafka. Cobre também `expiry.idx` apagado por fora: degrada para carência, nunca para perda.

## 2.3 O scheduler

**Duração uniforme ⟹ ordem de escrita = ordem de vencimento.** Toda escrita produz prazo ≥ o da anterior, então a estrutura correta não é heap nem timing wheel: é uma **deque de baldes de 1 minuto**, com append sempre na cauda (os *time buckets* do §57, na resolução que ele fixa).

**Sobrevive ao `mode: merge`?** Sobrevive, e é onde quase se erra. Em merge o mesmo rowId reaparece na deque uma vez por escrita; a entrada antiga fica obsoleta. Tratamento: ao drenar o balde, relê `expiry.Get(rowId)` — se o prazo corrente já venceu, expira; senão **descarta e pronto**, sem reenfileirar, porque a escrita que renovou já enfileirou na cauda. Válido só porque *toda* escrita enfileira.

**Varredura completa por minuto está fora.** 142 MB de mmap sequencial ≈ 28 ms/passada é aceitável hoje, mas `expiry.idx` é indexado por rowId, rowIds nunca são reutilizados ([store.go:974](internal/storage/store.go:974)) e a primary nunca é purgada: a varredura escala com *rowIds já alocados na história*, não com *rows vivas*. Em 500M rowIds vira 4 GB/min para um key set vivo que a própria feature mantém pequeno — o custo cresce quando a feature funciona melhor. **Varredura completa existe uma vez só, na abertura, para semear a deque** (`O(n)`, sem `sort`, sem abrir record).

Fila = 8 B por entrada, comprimento = escritas na janela: @452 ev/s, 2h → 3,25M entradas ≈ **26 MB de heap**. Vira métrica.

> **Correção (23/08/2026): a deque não escala para prazo longo, e o piloto escolheu 90 dias.**
> A 452 ev/s, 90 dias são 3,51 **bilhões** de escritas na janela — **28 GB de heap**. A deque foi
> desenhada para prazos curtos e o dimensionamento não foi testado contra o caso real.
>
> Para prazo longo a varredura completa, que eu havia descartado, é a estratégia **certa**: 8 B por
> rowId alocado dá 28,8 MB hoje e 142 MB nas 17,8M rows do piloto anterior — e com prazo de 90 dias
> a resolução de 1 minuto do §57 é irrelevante, então ela roda de hora em hora em vez de por minuto.
> O custo por passada é o mesmo, mas o número de passadas cai 60×.
>
> **A Fase 4 escolhe a estratégia pela duração declarada**: deque quando a janela de escritas couber
> num teto de memória declarado, varredura periódica com intervalo proporcional quando não couber.
> O teto vira configuração com default, e a escolha vai para log na subida — um agendador que
> escolhe sozinho e não conta qual escolheu é o que torna um problema de memória indiagnosticável.

Ciclo de vida decalcado de `IniciaCompaction`/`ParaCompaction` ([compaction.go:623](internal/storage/compaction.go:623), [:669](internal/storage/compaction.go:669)), mesmo single-flight. `Close()` chama `ParaExpiracao()` **antes** de `ParaCompaction()` e do `vida.Lock()`. Registro em `ligaExpiracao(ctx)` ao lado de [server.go:461](internal/server/server.go:461), lendo a duração **por dataset**. `nishidb ingest` roda **uma** passada antes do `Checkpoint` final — fecha metade do débito 66 sem tocar no laço de 5s.

## 2.4 A expiração

`Store.Expira(rowID)` — método próprio no molde de `reapendaSeVivo` ([compaction.go:120](internal/storage/compaction.go:120)), **não** uma chamada a `Put`.

**O tombstone é um RECORD DURÁVEL, não um bit na location.** `applyRecord` deriva o packed de `a.flags & FlagTombstone` ([store.go:1066](internal/storage/store.go:1066)) e o replay reconstrói locations a partir dos records — um tombstone só na location seria apagado pelo primeiro replay e a row ressuscitaria.

Tudo em **uma** seção crítica de `s.wmu`:

```
1. wmu.Lock(); defer Unlock(); checa stickyErr
2. packed, had := locs.Get(rowID)              ← DENTRO do lock
   !had → return          |  flags&Tombstone → return (idempotente)
3. rec := ReadRecord(...); writtenAt := rec.Payload[_writtenAt]   ← DENTRO do lock
   writtenAt + d > agora → return (RENOVADA, aborta sem escrever)
4. tomb := AppendField(nil, _writtenAt, writtenAt)
   buf := BuildRecord(..., rowID, lastSeq, FlagTombstone, rec.Key, tomb)
5. segW.Append(buf)                            ← erro é STICKY
6. applyRecord{flags: FlagTombstone, payload: tomb, prevPayload: rec.Payload,
               hadPrevious: true, countsAsNew: false}
7. expiry.Set(rowID, 0)
```

O passo 6 entrega **de graça**, pelo código que já existe: baixa de liveness do segmento de origem ([:1085-1108](internal/storage/store.go:1085)) — é o que o torna candidato à compaction; `seqs.Set` preservando o `lastSequence` do §58 ([:1142](internal/storage/store.go:1142)); commit da location com o bit ([:1155](internal/storage/store.go:1155)); e, na fase de CLEAR ([:1163](internal/storage/store.go:1163)), como o payload novo não tem campo indexado, **todo** campo do prev cai no ramo "campo que sai" e é limpo — literalmente o *"secondary indexes são limpos"* do §56, sem uma linha nova.

**A primary NÃO é tocada** — a entrada precisa sobreviver para que `GetByKey` ache a row tombstonada e o stale check rode. `slotDeleted` ([primary/index.go:38](internal/index/primary/index.go:38)) continua reservado e não usado: **remoção em open-addressing não é pré-requisito desta feature.**

**Corrida com evento novo para a mesma key:** não existe janela. `Put` ([store.go:933](internal/storage/store.go:933)) e `Expira` tomam `wmu`, e o `Expira` relê location **e** `writtenAt` dentro do lock. Put vence → o `Expira` vê o prazo renovado e aborta. Expira vence → o `Put` acha a row tombstonada e faz ressurreição legítima ou descarta como stale (§2.5). O teste que fixa isso **não pode ser sob `-race`**: mover a leitura para fora do lock não produz acesso não sincronizado, só perturba escalonamento — mesmo argumento de [compaction.go:96-100](internal/storage/compaction.go:96). Precisa de seam.

## 2.5 Débitos 24 e 25 — vêm ANTES, não junto

`GetByKey` ([store.go:875](internal/storage/store.go:875)) devolve `ok=false` para **ausente** e para **tombstonada**; o `lookup` interno já calcula os dois separadamente ([:862](internal/storage/store.go:862)) e joga a distinção fora três linhas depois.

```go
// internal/types — mora aqui porque dois provedores implementam a interface:
// o Store real e o fakestorage.
type Presenca uint8
const (
    PresenceAbsent Presenca = iota // key nunca existiu
    PresenceLive                    // estado corrente legível
    PresenceExpired                // rowId e sequence válidos; payload é lápide
)
GetByKey(key, buf []byte) (types.Record, []byte, types.Presence, error)
Sequence(id types.RowID) (int64, types.Presence)
```

`PresenceExpired` cobre **os dois** casos — bit gravado **ou** prazo vencido sem tombstone publicado. Sem unificar, o materializer teria duas noções de "morta" e a que vale dependeria de o scheduler ter passado.

Em [materializer.go:276-314](internal/materializer/materializer.go:276): `found` vira `presenca != PresenceAbsent` (**é a correção do débito 24, e é uma linha** — o stale check de [:285](internal/materializer/materializer.go:285) passa a rodar); o merge de [:294](internal/materializer/materializer.go:294) exige `PresenceLive`, porque merge sobre a lápide arrastaria campos do alarme anterior para o alarme reaberto; desfecho novo `ActionRessuscitada` + contador, senão "alarme reabriu" sai como `updated` e fica invisível no painel.

> **O piloto cai num limite que precisa estar escrito.** Sem `materialization.sequence` — e o piloto não a declara —, [materializer.go:285](internal/materializer/materializer.go:285) já exige `Seq != nil`, então **não há stale check nenhum** e um evento reentregue pelo Kafka reabre a entidade. Não é regressão (sem sequence não existe informação de ordem), mas com TTL ligado o sintoma passa de "update redundante" para "alarme fechado reabre sozinho". **Não recusar** a combinação (bloquearia o piloto); WARN nomeado na subida, no estilo de [server.go:465](internal/server/server.go:465).

**Ressurreição legítima: rowId REUSADO**, e não é preferência. A primary tem um slot por key ([primary/index.go:177](internal/index/primary/index.go:177)); alocar rowId novo deixaria o antigo com location publicada e nenhuma key que o alcance — órfão que a compaction copiaria para frente para sempre. Não conflita com a invariante de [store.go:974](internal/storage/store.go:974) (que proíbe reciclar id para key **diferente**). A ressurreição é um `Put` normal com `flags=0`: a location volta sem o bit e o tombstone vira garbage. **Zero código especial.**

## 2.6 `ttl.field` do §56: mutuamente exclusivos

```yaml
materialization:
  ttl:
    expireAfterWrite: 2h        # idle timeout (implementado)
  # ou, mutuamente exclusivo:
  ttl:
    field: EventExpireDate      # deadline absoluto (§56, segue sem motor)
    type: timestamp
```

Precisam de **schedulers diferentes**: toda a otimização de §2.3 depende de duração uniforme; com `ttl.field` o prazo vem do payload (pode vir no passado ou daqui a três anos), a deque vira heap e a semeadura `O(n)` vira `O(n log n)`. Aninhar dentro de `ttl` torna a exclusão estrutural e local, validada onde [spec.go:293-303](internal/schema/spec.go:293) já valida o bloco. `expireAfterWrite` por `units.Duration` ([units.go:80](internal/units/units.go:80)), com **piso de 1 minuto** — abaixo da resolução do §57 a chave mentiria; recusa, não arredondamento, mesma doutrina de `razaoDeLiveness` ([config.go:266](internal/config/config.go:266)).

`tombstoneRetention` (hoje parseado e descartado, [spec.go:272](internal/schema/spec.go:272)): **parsear, validar e emitir WARN de que não tem motor**. Implementá-lo é fase separada — é o que exige `slotDeleted` e purga da primary. Custo permanente enquanto não existir, a registrar como débito novo: uma key vista uma vez custa para sempre ~47 B (primary 23 + location 8 + sequence 8 + expiry 8); com 100M identificadores distintos ao longo da vida, **~4,7 GB de índice** que nada devolve.

## 2.7 Observabilidade

Novas: `nishidb_expiry_running`, `_expired_total`, `_resurrected_total`, `_expiry_queue_depth`, `_expiry_lag_seconds`, `_expiry_sweep_seconds`. `MetMaterializedTombstones` ([names.go:37](internal/metrics/names.go:37)), hoje declarada e sem alimentador, passa a ser alimentada.

**`nishidb_expiry_lag_seconds`** = `agora − prazo do balde mais antigo não drenado`. É a única série que separa *"a expiração não roda"* de *"não dá conta"* — a lacuna exata que o débito 65 deixou aberta para a compaction, e que aqui nasce fechada por um `int64`.

**Correção que a feature torna urgente:** `MetMaterializedRows` é publicada como *"linhas vivas"* ([metrics_dataset.go:94](internal/server/metrics_dataset.go:94)) mas alimentada por `st.RowCount`, que é *rows criadas* ([store.go:1522](internal/storage/store.go:1522)). Hoje é imprecisão; com expiração vira mentira que cresce sem teto (17,8M "vivas" com 200 mil de fato). Entra `LiveRows` de verdade em `storage.Stats` — é o número que a feature existe para controlar.

`/v1/stats` ganha bloco `expiry` irmão de `compaction`, lido de `Store.Stats()` e nunca de contador próprio ([documents.go:90-93](internal/server/documents.go:90)). `/v1/metadata` ganha `expireAfterWrite` em `DatasetMeta`.

## 2.8 Fases

| Fase | Entrega | Isolada? | Tam. |
|---|---|---|---|
| **1 — Schema** | `ttl.expireAfterWrite` validado, exclusão mútua, piso de 1 min, prefixo `_` reservado, WARN do `tombstoneRetention` | Sim, nada muda em runtime | 1× |
| **2 — `_writtenAt`** | Campo de sistema injetado no merge e no replace, oculto no `*`, recusado se vier do evento | **Sim, e é o beachhead**: o dataset registra o instante **sem expirar nada**; quando a Fase 4 subir, as rows já têm prazo real | 3× |
| **3 — `expiry.idx` + débitos 24/25 + `Store.Expira`** | Índice no `applyRecord` (write **e** replay), predicado de leitura nos 3 pontos, `Presence`, `Expira(rowID)` sem scheduler | Sim — ao fim, o tombstone é produzível e tudo a jusante está correto | 4× |
| **4 — Scheduler** | Deque de baldes, semeadura na abertura, wiring no server, passada única no `ingest` | Milestone | 5× |
| **5 — Observabilidade** | 6 séries, bloco `expiry`, correção do `MetMaterializedRows` | Sim | 1,5× |
| **6 — `tombstoneRetention`** | Purga da primary (`slotDeleted`), compaction descartando tombstone vencido | **Fora deste escopo** | 6× |

**Ordenação obrigatória: a Fase 3 vem ANTES de qualquer coisa capaz de produzir tombstone.** O roadmap ([roadmap-m4-m5-m6.md:286](planning/roadmap-m4-m5-m6.md:286)) diz "na mesma unidade"; lendo o código, é mais forte: entre o primeiro tombstone e a correção do `GetByKey`, o dataset ressuscita rows por evento antigo em silêncio, com CRC batendo — indistinguível de escrita legítima, exatamente o que o §58 proíbe.

## 2.9 Testes

Além dos nomes já citados: `TestRowExpiradaNaoRessuscitaPorEventoAntigo`, `TestRessurreicaoNaoFazMergeSobreOPayloadDoTombstone`, `TestGetByKeyDistingueAusenteDeExpirada`, `TestExpiracaoPublicaTombstoneDuravelENaoSoOBitDaLocation`, `TestExpiracaoLimpaTodasAsPostingsDeBitmapDaRow`, `TestExpiracaoNaoRemoveAKeyDaPrimary`, **`TestExpiracaoLeOInstanteDeEscritaDEPOISDeEntrarNaSecaoCritica`** (com seam, não `-race`), `TestExpiryIdxEhReconstruidoPeloReplayDoTail`, **`TestRowSemInstanteDeEscritaGanhaPrazoCheioEmVezDeExpirarNaPrimeiraVarredura`** (o caminho de migração das 17,8M), `TestEntradaObsoletaDaFilaEhDescartadaSemExpirarARowRenovada`, `TestRowVencidaSemTombstoneNaoApareceEmQuery`, `TestDatasetSemSequenceAvisaQueEventoReentregueReabreAEntidade`.

**Crash** (molde: `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado`): `TestKill9NoMeioDaExpiracaoPreservaOEstadoConfirmado`, `TestKill9EntreOAppendDoTombstoneEOCheckpointNaoRessuscitaARow`.

**Gate novo `live_rows_plateau`** (molde: [harness.go:1282](benchmark/ingest-bench/harness.go:1282)): key set que **cresce** indefinidamente, duração curta, exigência de que rows vivas estabilizem em plateau enquanto `nextRowID` cresce linear. ⚠️ O débito 65 registra que a regra causal do `disk_plateau` do M4 está **errada** e que *"qual é a variável que decide continua em aberto"* — os gates novos precisam de **calibração própria**, não podem herdar a fronteira de churn do M4.

---

# Riscos operacionais

1. **O `format:` está inferido de UMA amostra.** [`piloto-events.md:100-103`](docs/piloto-events.md:100) é duas páginas sobre exatamente isso dando errado: *"tratar máximo observado como piso, nunca como teto"*. Se um produtor emitir outra grafia, esses eventos viram TYPE_CONVERSION sob `log-and-skip` — **descarte silencioso, por evento, permanente**. A tolerância de 1-2 dígitos em `d`/`h` mitiga a variante mais provável, não todas. **Ensaio obrigatório:** ingerir uma fatia real do tópico num dataset de rascunho e exigir **zero** no contador de conversão antes de tocar o que está no ar.

2. **Retipar `firstSeen`/`lastSeen` exige recriar o dataset — e isso trunca o histórico de todos os outros campos.** Trocar `type:` no YAML e reiniciar não é opção: os bytes gravados carregam tag `0x09`, toda row anterior conta `typeMismatch` e não casa com nada, e na projeção rows antigas saem como string crua e novas como instante — **duas formas JSON na mesma coluna do mesmo result set, sem um único erro**. Recriar é viável em tempo (backlog entra em ~3h30 contra janela de 18,8h), mas o piloto é `merge` **sem sequence**: a entidade reconstruída é o merge só dos eventos **dentro** da janela, e campos nunca reenviados voltam **NULL**. → **Agende o retipo junto da próxima recriação inevitável** (a que o débito 22 vai forçar quando o conjunto de bitmaps mudar).

3. **DST em `datetime`:** uma hora por ano de instantes potencialmente deslocados, sem conserto possível sem offset no dado. Documentar na tabela de tipos com a saída nomeada (`type: timestamp` com offset do produtor).

4. **`datetime` não faz round-trip textual** — decisão consciente, mas é a primeira pergunta que o operador fará ao ver a saída.

---

# Verificação

**Por task**
```bash
go test ./internal/typeparse/... ./internal/schema/... ./internal/materializer/... ./internal/query/... -run 'Data|Hora|Datetime|Time|Layout|Formato'
```

**Antes de commitar** — os dois são complementares e nenhum substitui o outro ([Makefile:24-34](Makefile:24)):
```bash
make test && make test-race
```
```bash
make lint
```
`golangci-lint v2.12.2 / go1.26.5` fixo, contra a linha de base de **390** apontamentos, com `max-issues-per-linter` e `max-same-issues` em **0** — versão solta mede outra linha de base (débito 53).

**Prova ponta a ponta do parser** — dataset de rascunho com o YAML novo, ingestão de uma fatia real do tópico, e:
```bash
curl -s --noproxy '*' 127.0.0.1:9142/v1/stats
```
Exigência: `nishidb_ingest_invalid_total` e o contador de TYPE_CONVERSION em **zero**. (O `--noproxy` não é opcional neste host — [piloto-events.md:142](docs/piloto-events.md:142).)

Depois, a query que hoje é impossível:
```bash
nishidb query --dataset events "SELECT identifier, lastSeen FROM events WHERE lastSeen >= 'Aug 22, 2026 08:00:00 PM' LIMIT 20"
```

**Prova do `expireAfterWrite`** — carga sustentada com key set crescente, `expireAfterWrite` curto, e a curva de `LiveRows` estabilizando em plateau enquanto `nextRowID` cresce linear. Mais o crash real (SIGKILL no meio da varredura) conferindo que nenhuma row viva perdeu prazo e nenhuma row expirada ressuscitou.

**Documentação (DoD do projeto):** `docs/architecture.md` (tabela de tipos + seção de expiração), `docs/debitos.md` (fechamento dos 24/25; débito novo do `tombstoneRetention`), `storage_layout.puml` (o `expiry.idx`), spec §17/§18/§56/§73, README. `TestEvidenciasCitadasNosDocumentosExistem` continua valendo.
