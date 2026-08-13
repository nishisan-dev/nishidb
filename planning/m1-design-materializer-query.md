# NishiDB — Milestone 1: Design do Materializer + Query Engine

Escopo: Fase 2 (materializer) + Fase 5 (query engine) da spec (`nishidb-spec-proposal.md`), mais gerador sintético RADIUS e harness de benchmark de memória. Kafka fora deste milestone. Storage engine é contrato externo (interfaces da seção 99, planejado por outro agente).

Module path: `github.com/nishisan-dev/nishidb`, Go 1.25+, stdlib + `gopkg.in/yaml.v3`.

---

## 0. Decisões estruturantes (resumo executivo)

| # | Decisão | Escolha |
|---|---------|---------|
| D1 | Parser JSON | **Tokenizer próprio sobre `[]byte`** (subset), API pull-iterator zero-alloc. Nem `Decoder.Token()` (100–200 allocs/evento), nem `json/v2` (experimental, exige `GOEXPERIMENT` em todo build) |
| D2 | Representação de NULL | **NULL = ausência do campo no registro materializado.** Patch com `null` explícito remove o campo no merge. `IS NULL` = campo ausente. Sem flag de null no encoding |
| D3 | Layout do payload | Campos **ordenados por fieldID crescente** → merge vira merge linear de duas listas ordenadas, O(n+m), zero estrutura auxiliar |
| D4 | Re-validação de predicado (§44) | Executor **sempre reavalia o WHERE completo** contra o estado corrente. Índices só geram candidatos (superset). Elimina cálculo de "residual predicate" no planner |
| D5 | Coerção SQL | Literais coeridos **em plan-time** (uma vez), segundo tabela fixa (§3.6). Erro de tipo = erro de query antes da execução |
| D6 | Valores sem boxing | `Value` struct união (`I64 int64` + `Bytes []byte` + `Type`), nunca `interface{}` no hot path |
| D7 | Conversão `[]byte`→`string` | Um único helper auditado com `unsafe.String` (`internal/unsafex`) para chamadas que não retêm a string (ex.: `netip.ParseAddr`) |
| D8 | Gerador RADIUS | `internal/testutil/radiusgen` (usável por testes e por `benchmark/`), determinístico via `math/rand/v2` PCG, escreve JSON em buffer reutilizável (~0 alloc/evento) |
| D9 | Medição RSS | `/proc/self/status` (VmRSS/VmHWM) + `/proc/self/smaps_rollup` (residência de mmap separada de heap anônimo) + `runtime.ReadMemStats` |
| D10 | Alocações desde o dia 1 | `b.ReportAllocs()` nos micro-benchmarks desde a primeira tarefa; não deixar para a Fase 12 da spec |
| D11 | Write path | **`storage.Put` orquestra** append→diff de índices→publicação atômica (contraproposta do plan-storage, aceita em 2026-08-12). O Apply só monta o payload; recovery tail-replay reusa a mesma orquestração sem materializer |

---

## 1. Parser JSON de baixa alocação (spec §19–20)

### 1.1 Avaliação das três opções

**(a) `encoding/json` `Decoder.Token()` — REPROVADO.**
Cada `Token()` devolve `json.Token` (`interface{}`): todo token é boxed (1 alloc), strings alocam o conteúdo (2ª alloc), números viram `float64` boxed ou `json.Number` (string alocada). Um evento RADIUS típico (~35 campos) tem ~75+ tokens → **150–250 allocations/evento**, ~25–50× acima da meta de single-digit (§111). Além disso `Token()` não expõe os bytes crus de objetos aninhados — para tratar nested object como json opaco seria preciso re-serializar. Inviável.

**(b) `encoding/json/v2` / `jsontext` (GOEXPERIMENT=jsonv2, Go 1.25) — REPROVADO para v1.**
Tecnicamente atraente: `jsontext.Decoder.ReadValue()` devolve `jsontext.Value` apontando para o buffer interno (quase zero-copy), tokens de número/bool não alocam. Mas: (1) é **experimental** — API pode mudar entre releases; (2) exige `GOEXPERIMENT=jsonv2` em **todos** os ambientes de build (dev, CI, Dockerfile, contribuidores) — custo operacional permanente e violação do espírito "previsibilidade" (§127); (3) não dá controle sobre o tratamento inline de Mongo Extended JSON. Reavaliar quando estabilizar (provável Go 1.26+); como o parser fica atrás de API própria, a troca futura é contida.

**(c) Tokenizer próprio sobre `[]byte` — ESCOLHIDO.**
O subset necessário é pequeno e fechado: objeto top-level com chaves string; valores string/number/bool/null; objetos/arrays aninhados tratados como bytes JSON opacos (skip estrutural) — exceto detecção de wrapper Mongo. Estimativa: ~500–600 LOC + fuzz. Controle total de alocações (subslices do input, scratch reutilizável), zero flags de build, trivialmente fuzzeável com `go test -fuzz`.

### 1.2 API do parser — pull iterator (não callback)

Pull ao invés de visitor/callback: evita alocação de closures, deixa o materializador dirigir o loop e abortar cedo (ex.: `unknownFields: reject`).

```go
// internal/jsonp/jsonp.go
package jsonp

type Kind uint8

const (
    KindString Kind = iota + 1
    KindNumber
    KindBool
    KindNull
    KindObject // valor aninhado: Raw = bytes JSON completos do objeto
    KindArray  // idem
)

type Ext uint8

const (
    ExtNone       Ext = iota
    ExtNumberLong     // era {"$numberLong":"..."} → Raw = dígitos, Kind=KindNumber
    ExtDate           // era {"$date": ...}        → Raw = conteúdo, Kind=KindString ou KindNumber
)

type Field struct {
    Key []byte // nome do campo, unescaped (fast path: subslice do input; slow path: scratch)
    Kind Kind
    Ext  Ext
    Raw  []byte // string: conteúdo unescaped; number: dígitos; bool: "true"/"false";
               // null: nil; object/array: bytes JSON crus (com chaves/colchetes)
}

type Parser struct {
    data    []byte
    pos     int
    scratch []byte // buffer de unescape, reutilizado; cresce até high-water mark
}

func (p *Parser) Reset(data []byte)        // zero alloc; valida que começa com '{'
func (p *Parser) Next() (Field, bool, error) // false = fim do objeto top-level
```

Regras de implementação:

- **Lifetime**: `Field.Key`/`Field.Raw` são válidos **somente até o próximo `Next()`/`Reset()`** (podem apontar para `scratch`). O materializador copia o que precisa para seus próprios buffers no mesmo passo — contrato documentado no doc comment.
- **Strings**: fast path sem escapes → subslice direto do input. Slow path (`\`) → unescape para `scratch` (um único scratch, reutilizado; sobrevive até o próximo Next, suficiente).
- **Números**: retornados como dígitos crus (`Raw`); a conversão int64/double é do typeconv (§2.3), que sabe o tipo alvo. Evita parse duplo.
- **Nested object/array**: skip estrutural contando profundidade e respeitando strings (aspas/escapes). `Raw` = janela `[início, fim]` do input — zero-copy, vira valor `json`.
- **Chave duplicada** no mesmo objeto: last-wins (comportamento documentado; teste obrigatório).
- **Erros**: posição byte + categoria `INVALID_JSON` (§101).

### 1.3 Mongo Extended JSON inline (§19)

Quando o valor de um campo é objeto, antes de tratá-lo como opaco o parser espia o conteúdo (sem alocar):

1. Objeto com **exatamente 1 chave** `$numberLong` e valor string de dígitos → emite `Kind=KindNumber, Ext=ExtNumberLong, Raw=dígitos`.
2. Objeto com **exatamente 1 chave** `$date`:
   - valor string → `Kind=KindString, Ext=ExtDate, Raw=conteúdo` (formato relaxed, ex. `"2026-08-11T22:28:10.000Z"`);
   - valor `{"$numberLong":"<millis>"}` (formato canonical) → `Kind=KindNumber, Ext=ExtDate, Raw=dígitos`.
3. Qualquer outro shape (2+ chaves, outro `$tipo`) → objeto opaco normal (`KindObject`).

O wrapper nunca chega ao materializador — só o valor desembrulhado (§19: "wrappers não devem ser armazenados"). O typeconv usa `Ext` como dica: `ExtNumberLong` força int64; `ExtDate` força timestamp mesmo com `type: auto`.

Detecção barata: comparar a primeira chave com `$numberLong`/`$date` via `bytes.Equal` — só quando `data[pos] == '{'` e o primeiro byte da chave é `$`.

### 1.4 Testes e fuzz (tarefa A2)

Unit: objeto plano; escapes (`\n`, `\uXXXX`, surrogate pairs); números (int, frac, exp, negativos); bool/null; nested opaco (com strings contendo `}`); Extended JSON (3 shapes + shapes inválidos); chave duplicada; whitespace; UTF-8 multibyte; truncado/malformado → `INVALID_JSON`. Fuzz: `FuzzParser` comparando "não entra em pânico e termina" + round-trip com `encoding/json` como oráculo de aceitação/rejeição.

Benchmark de aceite: `BenchmarkParseRadiusEvent` com evento de 35 campos → **0 allocs/op** após warm-up do scratch.

---

## 2. Pipeline de materialização (spec §12–25)

### 2.1 Pacote `internal/schema` — spec YAML compilada

Duas camadas: structs YAML crus (espelham o arquivo, via `yaml.v3`) e a forma **compilada** usada no hot path.

```go
// internal/schema/spec.go — carga + validação
type DatasetSpec struct {
    Version int
    Name    string
    Key     KeySpec      // source: payload, field, type
    Mode    Mode         // MergeMode | ReplaceMode (default merge)
    Sequence *SequenceSpec // field, type (int64), strategy (greater)
    TTL      *TTLSpec
    Schema   SchemaSpec   // include, exclude, unknownFields, fields
    Query    QueryLimits  // maxRows, timeout (defaults do nishidb.yaml)
}
```

Validações com erros claros (arquivo:campo): `version` suportada; `name` não vazio e `[a-z0-9_]`; `key.field` obrigatório; tipo de campo ∈ {string,int64,double,boolean,timestamp,ipv4,ipv6,ipv4_prefix,ipv6_prefix,bytes,json,auto}; `sequence.type` só int64 e `strategy` só greater (v1); alias único e não colidindo com nome de outro campo; `unknownFields` ∈ {include,ignore,reject}; `exclude` só faz sentido com `include: ["*"]`; campo com `index:` deve ter tipo indexável (bitmap: string/int64/boolean; hash: string/int64/ipv4/ipv6). A seção `source.kafka` é aceita e **ignorada** neste milestone (validação sintática apenas).

Forma compilada:

```go
// internal/schema/compile.go
// FieldID mora em internal/types (tipo próprio uint16, não alias): até 65k
// campos por dataset, alinhado com fields.dict.

type CompiledField struct {
    ID    types.FieldID
    Name  string      // nome original (ex.: "Acct-Status-Type")
    Alias string      // "" se ausente (ex.: "status")
    Type  types.FieldType
    Index IndexKind   // IndexNone | IndexBitmap | IndexHash
    IsKey, IsSequence, IsTTL bool
}

type Compiled struct {
    Name        string
    Mode        Mode
    IncludeAll  bool
    Unknown     UnknownPolicy
    Key, Seq, TTL *CompiledField

    byName  map[string]*CompiledField // nome original E alias apontam p/ mesmo campo
    exclude map[string]struct{}
    fields  []*CompiledField          // indexado por FieldID (denso)
}

// Lookup no hot path: m[string(keyBytes)] — o compilador Go elide a alocação
// da conversão em lookups de map (special case). Zero alloc.
func (c *Compiled) FieldByName(name []byte) (*CompiledField, bool)
func (c *Compiled) FieldByID(id FieldID) (*CompiledField, bool)
```

Campos desconhecidos com `include: ["*"]` + `unknownFields: include`: o compiled delega ao **field dictionary** do storage (`LookupOrCreate(name []byte) (FieldID, error)`) e cacheia o `*CompiledField` inferido (`type: auto`) no `byName`. O cache cresce com a cardinalidade de *nomes de campos* (dezenas/centenas), não com entidades — heap OK.

### 2.2 Tipos compartilhados e contrato com o storage — FECHADO (2026-08-12)

Contrato negociado com o plan-storage (registrado no plano dele como §15A). Pacote `internal/types` (leve, sem dependências) contém **só tipos de dados**; interfaces são declaradas no consumidor (padrão Go — `*storage.Store` as satisfaz):

```go
type FieldType uint8  // TypeString, TypeInt64, TypeDouble, TypeBool, TypeTimestamp,
                      // TypeIPv4, TypeIPv6, TypeIPv4Prefix, TypeIPv6Prefix,
                      // TypeBytes, TypeJSON  (auto resolve-se no parse)
type FieldID uint16   // tipo próprio, não alias: o compilador barra a troca com RowID
type RowID uint64     // idem — a API inteira trafega RowID, nunca uint64 cru

type Value struct {   // união sem boxing — NUNCA interface{} no hot path
    Type  FieldType
    I64   int64   // int64, timestamp(millis), bool(0/1), double(math.Float64bits)
    Bytes []byte  // string, ipv4(4B BE), ipv6(16B), prefixes(5B/17B), bytes, json
}

type Record struct {
    RowID    RowID
    Sequence int64
    Flags    uint8  // constantes em internal/types (fonte única): FlagTombstone = 1<<0,
                    // bits 1-7 reservados (zero). Apply sempre escreve 0 no M1
    Key      []byte // key explícita no record (validação de colisão §36 + tombstone sem payload)
    Payload  []byte // campos encodados ordenados por FieldID (§2.4)
}
```

`PhysicalLocation` **não faz parte do contrato público**: o storage publica a location como u64 empacotado (`flags|segmentID|offset`) via `atomic.StoreUint64` — 16 bytes não seriam publicáveis atomicamente — e o length vem do header CRC-protegido do record. Materializer e executor nunca tocam em locations; todo acesso a dados é via API do Store.

API do Store consumida pelo **materializer** (interface declarada em `internal/materializer`):

```go
type Store interface {
    // Lê o record corrente da key (resolve colisão §36 internamente).
    // buf reutilizado pelo caller; Record.Key/Payload zero-copy sobre buf.
    GetByKey(key, buf []byte) (rec types.Record, out []byte, ok bool, err error)

    // Orquestra TODO o write path (D11): append → validação (ordem crescente de
    // FieldID, ausência de tag 0x00, fieldCount) → diff de campos indexados
    // prev↔novo (merge linear) → secondary updates → publicação atômica da
    // location (§45/46). prev != nil evita re-lookup (usa prev.RowID);
    // prev == nil com key existente → Put resolve sozinho (fallback de correção).
    Put(key []byte, seq int64, flags uint8, payload []byte,
        prev *types.Record) (rowID types.RowID, created bool, err error)
}

type FieldDictionary interface {
    LookupOrCreate(name []byte) (types.FieldID, error) // ID novo só retorna pós-fdatasync
    Name(id types.FieldID) (string, bool)
}
```

Consequências da adoção do `Put` orquestrador (vs. a versão inicial deste plano):

- **Não existem** `RowIDAllocator` nem chamadas diretas a PrimaryIndex/LocationIndex/SequenceIndex a partir do materializer — o Put encapsula tudo. Motivos aceitos: (a) o recovery tail-replay do storage re-executa a mesma orquestração append→índices→publish **sem** materializer — a ordem §46 precisa viver em um único lugar; (b) o overlay de locations do storage tem invariante de durabilidade (aplicar ao mmap só após fdatasync dos segmentos) que não pode depender de disciplina do caller.
- O `sink *indexDeltas` do merge **morre**: o Put faz o diff prev↔novo internamente, no mesmo passo linear em que valida o payload (~µs extra, irrelevante a 500–2000 ev/s; fast-path com deltas pré-computados só se a Fase 12 provar necessidade).
- R6 (replace órfão de postings) morre por construção — o Put diffa sempre, merge ou replace.
- `sequences.idx` (§38) existe no storage (o recovery usa), mas o hot path usa `rec.Sequence` do GetByKey — mesmo custo, um code path só.

### 2.3 Conversão de tipos JSON→binário — `internal/materializer/typeconv.go`

Representação binária (little-endian para inteiros/double; IPs em network byte order):

| Tipo | Encoding | Entradas aceitas (JSON) |
|------|----------|--------------------------|
| string | bytes UTF-8, comprimento variável | string |
| int64 | 8B LE | number inteiro (sem frac/exp com parte fracionária); `$numberLong`; **string de dígitos NÃO** (erro) |
| double | 8B LE IEEE-754 | number (int ou frac/exp) |
| boolean | 1B (0/1) | true/false |
| timestamp | int64 epoch **millis**, 8B LE | string RFC3339/RFC3339Nano; number = epoch millis; `$date` (relaxed/canonical) |
| ipv4 | 4B network order | string dotted-quad |
| ipv6 | 16B | string (RFC 4291, aceita mapped/compressed) |
| ipv4_prefix | 5B (4B addr + 1B len) | string `"a.b.c.d/nn"` |
| ipv6_prefix | 17B | string `"xxxx::/nn"` |
| bytes | comprimento variável | string base64 (std encoding) |
| json | bytes JSON crus, variável | object/array/qualquer valor |
| auto | inferido | string→string; number inteiro→int64; number frac→double; bool→boolean; null→null; object/array→json; `$numberLong`→int64; `$date`→timestamp |

Regras de alocação:
- `int64`/`double`: parser numérico próprio sobre `[]byte` (`strconv.ParseInt` exige string; `strconv.ParseFloat` idem → usar `unsafex.String(raw)` no ParseFloat — não retém — e parser int próprio de ~20 linhas).
- `timestamp` RFC3339: **parser próprio `parseRFC3339Millis([]byte) (int64, error)`** (~80 linhas, aceita `Z` e offset `±hh:mm`, frações até ns truncadas p/ millis). `time.Parse` custa alocações e é hot path (todo evento tem 3–6 timestamps).
- `ipv4`: parser dotted-quad próprio (~25 linhas). `ipv6`/prefixes: `netip.ParseAddr`/`ParsePrefix` via `unsafex.String` (não retêm o argumento) — 0 alloc (netip.Addr é value type).
- Falha de conversão → erro categoria `TYPE_CONVERSION` com nome do campo, tipo alvo e valor truncado (§101).

### 2.4 Encoding do payload de campos — FECHADO (dono: `internal/storage/encoding`, plano do storage)

Formato acordado (idêntico nos dois planos; o materializador é consumidor da API):

```text
payload := field*                     (ordenado por FieldID crescente, sem duplicatas)
field   := FIELD_ID(uint16 LE) TYPE(uint8) [LENGTH(uvarint)] VALUE
           LENGTH presente só para tipos variáveis (string, bytes, json)
           tipos fixos: int64/double/timestamp=8B, bool=1B, ipv4=4B, ipv6=16B,
           ipv4_prefix=5B, ipv6_prefix=17B
```

API acordada:

```go
func AppendField(dst []byte, id types.FieldID, v types.Value) ([]byte, error)
type FieldReader struct{ /* cursor sobre payload */ }
func (r *FieldReader) Reset(payload []byte)
func (r *FieldReader) Next() (id types.FieldID, v types.Value, ok bool) // v.Bytes = subslice do payload
```

Refinamentos fechados com o storage:
- **Tag de tipo `0x00` (null) é RESERVADA e nunca codificada** — coerente com D2 (NULL = ausência): `AppendField` com TypeNull retorna erro; `FieldReader` encontrando `0x00` retorna erro de corrupção. O `isNull` vive só como flag de struct no PatchBuffer.
- Ordem crescente de FieldID, **sem duplicatas**, é invariante do payload — o storage também depende dela (diff de campos indexados do Put é merge linear) e a valida no Put.
- O framing do record (header MAGIC/CRC32C/§31) é interno ao Put — o materializer monta **só o payload**.

A ordenação por FieldID é pré-condição do merge (D3) e permite ao executor coletar N campos do WHERE em um único passo linear.

### 2.5 PatchBuffer — evento parseado e tipado (reutilizável)

O writer (goroutine única) possui um conjunto fixo de buffers, todos `Reset()` a cada evento — sem `sync.Pool` (§21: só com benchmark provando benefício; com single writer não há contenção que o justifique):

```go
// internal/materializer/patch.go
type patchEntry struct {
    id     FieldID
    isNull bool        // null explícito no JSON
    val    types.Value // val.Bytes aponta para o arena
}

type PatchBuffer struct {
    entries []patchEntry // reutilizado; sort in-place por id (insertion sort p/ n≤32, senão slices.SortFunc — ambos 0 alloc)
    arena   []byte       // cópia dos bytes variáveis (valores do parser morrem no próximo Next())
    key     []byte       // subslice do arena
    seq     int64
    hasSeq  bool
}
```

`Parse(ev []byte, schema *Compiled, dict FieldDictionary) error` faz **um único passo** sobre o evento:
1. `jsonp.Parser.Next()` por campo top-level.
2. Resolução do campo: `schema.FieldByName(key)`; ausente → política `unknownFields` (include → `dict.LookupOrCreate` com tipo auto; ignore → skip consumindo o valor; reject → erro `SCHEMA_VALIDATION`). `exclude` → skip.
3. Conversão tipada (§2.3) direto para o arena; entry apontando para o arena.
4. Campo key → captura bytes; campo sequence → captura int64 (também vira entry normal — o valor é armazenado no estado).
5. Ao final: sort por `id`; validação `key` presente (senão `INVALID_KEY`) e, se sequence configurada, `hasSeq` (senão `INVALID_SEQUENCE`).

Duplicata pós-sort (chave JSON duplicada ou alias/nome apontando ao mesmo id): last-wins — dedup no sort estável.

### 2.6 Algoritmo de MERGE — merge linear de listas ordenadas

```go
// internal/materializer/merge.go  — IMPLEMENTADO (Q9)
// prevPayload: payload do record anterior (campos ordenados por FieldID)
// patch: entries ordenadas por FieldID, com isNull
// out: buffer reutilizável, semântica de append (passe out[:0])
// fieldCount e diff de campos indexados são do storage.Put (D11/§2.2) — não deste código

func mergeRecords(prevPayload []byte, patch []patchEntry, out []byte) ([]byte, error)

// Replace e create caem no mesmo lugar: sem estado anterior, o payload é só o
// patch. Uma função, não duas — não há nada a decidir entre os dois casos.
func encodePatch(patch []patchEntry, out []byte) ([]byte, error)
```

O erro existe porque nem toda entrada é confiável: payload anterior ilegível (corrupção vinda do storage) e patch fora de ordem (bug do PatchBuffer) viram `ErrInternalState`, categoria que o `Apply` contabiliza **separada** das cinco de evento ruim. O `out` volta mesmo no erro, para o caller reter o buffer já crescido.

O campo não tocado é copiado CRU via `encoding.FieldReader.RawField()` — o span completo (FIELD_ID+TYPE+[LENGTH]+VALUE), sem decode/re-encode. Campo corrompido não devolve span (o `fail()` zera o offset do campo corrente), então nem meio campo vaza para a saída.

Pseudocódigo (dois ponteiros):

```text
pf, pok := prev.Next()
j := 0
loop:
  caso só prev restante        → copia campo prev p/ out (raw copy, sem decode)
  caso só patch restante       → se !isNull: AppendField(out, patch[j])
  caso pf.id < patch[j].id     → copia prev; avança prev
  caso pf.id > patch[j].id     → se !isNull: escreve patch[j]; j++
  caso pf.id == patch[j].id    →
        se patch[j].isNull     → NÃO escreve (campo removido = NULL, decisão D2)
        senão                  → escreve patch[j]
        avança ambos
```

Propriedades: O(n+m), um único buffer de saída, cópia raw dos campos não tocados (sem decode/re-encode). O diff para índices secundários acontece dentro do `Put` (que recebe `prev` e o payload novo — mesma passada linear da validação).

**Semântica NULL (D2, §24)**: `null` explícito → campo ausente do novo record; campo ausente no patch → valor anterior preservado (copiado). Consistência total: um patch posterior sem o campo mantém a ausência (= continua NULL). `IS NULL` no executor = campo ausente do record. Não existe terceiro estado.

### 2.7 Sequence check (§25) e descarte de stale

Fluxo no `Apply` antes de qualquer escrita:

```text
prev, ok := store.GetByKey(key, prevBuf)
se ok && sequence configurada:
    se patch.seq <= prev.Sequence → ApplyResult{Stale},
        métrica nishidb_ingest_stale_total, FIM (sem merge, sem Append, sem índices)
```

Nota de correção do design inicial: "stale check sem ler o record" **não existe fisicamente** — a validação de colisão da primary (§36) obriga comparar a key real, que vive no record; todo lookup by-key já faz esse read. O `GetByKey` devolve o record lido de qualquer forma, então `prev.Sequence` dá o stale check com **zero I/O adicional**. O que o stale evita é o custo de merge + append + índices + publicação.

Sem sequence configurada: ordem de chegada vale (Kafka ordering §26 — fora deste milestone; o Apply não checa nada).

### 2.8 Fluxo REPLACE (§23)

O record novo é construído **só do patch** (`encodePatch`, nulls omitidos), sem merge. Sequence check idêntico ao §2.7. A limpeza de postings antigos dos índices secundários é responsabilidade do `Put` (diffa `prev` ↔ payload novo sempre, merge ou replace) — o Apply só precisa repassar o `prev` que o GetByKey já devolveu. O antigo risco R6 (esquecer de ler o estado anterior no replace) morre por construção.

### 2.9 `Materializer.Apply` — orquestração e interface com o storage

```go
// internal/materializer/materializer.go  — IMPLEMENTADO (Q10)
type Materializer struct {
    schema *schema.Compiled
    store  Store           // interface do §2.2 (GetByKey + Put) — *storage.Store satisfaz

    // buffers reutilizáveis (owned pela goroutine do writer — NÃO thread-safe, documentado)
    patch   PatchBuffer  // contém o jsonp.Parser
    prevRec types.Record // estado corrente lido; janela sobre prevBuf
    prevBuf []byte       // destino do GetByKey
    outBuf  []byte       // payload de saída

    metrics Metrics
}

type Action uint8        // Created | Updated | Replaced | Stale | Skipped | Failed
type ErrorCategory uint8 // as 5 de §101 + WriterFailed | Storage | Internal

type ApplyResult struct {
    Action   Action
    RowID    types.RowID
    Category ErrorCategory
    Err      error
}

func (r ApplyResult) Fatal() bool  // writer em modo falho: o caller PARA
func (m *Materializer) Apply(event []byte) ApplyResult
func (m *Materializer) Metrics() MetricsSnapshot
```

O `dict` saiu do struct: quem registra campo desconhecido é o `schema.Compiled` (via `ResolveUnknown`), que já carrega o dicionário desde o Compile — um segundo ponteiro aqui só daria duas fontes para a mesma verdade.

Sequência completa:

```text
1. patch.Parse(event)                      → erros INVALID_JSON/SCHEMA_VALIDATION/INVALID_KEY/
                                             INVALID_SEQUENCE/TYPE_CONVERSION
2. prevRec, prevBuf, ok := store.GetByKey(patch.key, prevBuf[:0])
3. ok && sequence: patch.seq <= prevRec.Sequence → Stale (fim, §2.7)
4. payload := mergeRecords(prevRec.Payload, patch.entries, outBuf[:0])   [merge, ok]
   payload := encodePatch(patch.entries, outBuf[:0])                     [replace ou create]
5. rowID, created := store.Put(patch.key, patch.seq, flags=0, payload, prevOrNil)
      ← o Put faz: append, validação, diff de índices, publicação atômica (D11)
6. Action: created → Created; senão mode==merge → Updated, mode==replace → Replaced
```

`prev` é o endereço de um CAMPO do materializer, não de uma variável local: passar o endereço de uma local por interface a faria escapar para o heap a cada evento, e o gate de alocação cairia.

Política de erro (§101) em **três baldes**, não dois:

| Balde | Categorias | Action | Por quê |
|---|---|---|---|
| evento ruim | INVALID_JSON, SCHEMA_VALIDATION, INVALID_KEY, INVALID_SEQUENCE, TYPE_CONVERSION | `Skipped` | log-and-skip: a fonte é ruidosa, a ingestão segue |
| writer morto | WRITER_FAILED (`types.ErrWriterFailed`) | `Failed` + `Fatal()` | sticky (§14.14): reprocessar é inútil até reabrir o dataset |
| defeito nosso | STORAGE_ERROR (`types.ErrInvalidArgument`, leitura corrompida, falha do dicionário), INTERNAL_STATE | `Failed` | contá-los como evento ruim esconderia o bug na métrica de fonte ruidosa |

Métricas (`atomic.Int64`, lidas por outra goroutine): `EventsTotal`, `CreatedTotal`, `UpdatedTotal`, `ReplacedTotal`, `StaleTotal`, `InvalidTotal` (soma das 5 de evento ruim), `FailedTotal` e `ByCategory[...]` para todas. Endpoint Prometheus é Fase 6.

Alocações do `Apply` em steady state: **0 allocs/evento**, com gate executável na suíte (`TestApplyNaoAlocaEmSteadyState`, contra um store mínimo; fora do build `-race`, onde a instrumentação altera a contagem). O alvo original do plano era 0–3.

---

## 3. SQL Engine (spec §59–66)

Pacotes: `internal/query/lexer`, `internal/query/parser` (inclui AST), `internal/query/planner`, `internal/query/executor` — estrutura da spec §98.

### 3.1 Grammar formal v1 (EBNF)

```ebnf
query        = select_stmt [ ";" ] EOF ;
select_stmt  = "SELECT" projection "FROM" identifier
               [ "WHERE" expr ] [ "LIMIT" int_literal ] ;
projection   = "*" | column { "," column } ;
column       = identifier ;
expr         = or_expr ;
or_expr      = and_expr { "OR" and_expr } ;
and_expr     = primary { "AND" primary } ;
primary      = "(" expr ")" | predicate ;
predicate    = identifier comp_op operand
             | identifier "IS" [ "NOT" ] "NULL" ;
comp_op      = "=" | "!=" | ">" | ">=" | "<" | "<=" ;
operand      = string_literal | number_literal | "TRUE" | "FALSE" | "?" ;
identifier   = bare_ident | quoted_ident ;
bare_ident   = letter | "_" , { letter | digit | "_" } ;
quoted_ident = '"' { any_char_except_dquote | '""' } '"' ;   (* p/ nomes com '-': "Acct-Status-Type" *)
string_literal = "'" { any_char_except_squote | "''" } "'" ;
number_literal = [ "-" ] digits [ "." digits ] [ ("e"|"E") [sign] digits ] ;
```

Decisões: keywords case-insensitive; identificadores case-sensitive; precedência AND > OR (padrão SQL); `NOT` só existe em `IS NOT NULL` (sem NOT genérico na v1); coluna só do lado esquerdo do comparador (sem `literal op coluna`, sem `col op col`); LIMIT só literal inteiro ≥ 0 (não aceita `?`); `<>` NÃO suportado (spec lista só `!=`; adicionar depois é 1 linha).

### 3.2 Lexer — `internal/query/lexer`

Hand-written, sobre `string`, sem regex. Token:

```go
type TokenType uint8 // tkSelect, tkFrom, tkWhere, tkLimit, tkAnd, tkOr, tkIs, tkNot, tkNull,
                     // tkTrue, tkFalse, tkIdent, tkQuotedIdent, tkString, tkNumber,
                     // tkPlaceholder, tkComma, tkStar, tkLParen, tkRParen,
                     // tkEq, tkNeq, tkGt, tkGte, tkLt, tkLte, tkSemicolon, tkEOF
type Token struct { Type TokenType; Text string; Pos int }
```

Keywords resolvidas por lookup case-insensitive (switch sobre len + comparação ASCII-folded — sem `strings.ToUpper` alocante; queries não são hot path como ingestão, mas manter o hábito). Erros com posição de byte (`syntax error at position 23: unexpected '#'`). Escapes `''` (string) e `""` (ident) resolvidos no lexer (Text já unescaped — aloca, aceitável: 1 parse por query).

### 3.3 Parser + AST — `internal/query/parser`

Recursive descent direto da grammar (uma função por produção). AST:

```go
type SelectStmt struct {
    Star       bool
    Projection []string // nomes como escritos (alias ou original); vazio se Star
    Dataset    string
    Where      Expr  // nil se ausente
    Limit      int64 // -1 se ausente
    ParamCount int   // nº de '?' na ordem em que aparecem
}

type Expr interface{ isExpr() }

type LogicalExpr struct{ Op LogicalOp; Left, Right Expr } // opAnd | opOr
type Comparison  struct{ Column string; Op CmpOp; Operand Operand; Pos int }
type NullCheck   struct{ Column string; Not bool; Pos int }

type Operand struct {
    Kind OperandKind // opString | opInt | opFloat | opBool | opParam
    S string; I int64; F float64; B bool
    ParamIdx int     // índice posicional do '?'
}
```

Números: tenta `int64` primeiro; com `.`/`e` → float. Placeholders numerados na ordem léxica (`ParamIdx`).

### 3.4 Binder (análise semântica) — dentro do planner

Antes de planejar, resolve nomes e coage literais **uma única vez**:

```go
type BoundComparison struct {
    Field *schema.CompiledField // resolvido: alias OU nome original OU quoted original
    Op    CmpOp
    Val   types.Value // literal já convertido p/ representação binária do tipo do campo
    IsParam bool; ParamIdx int  // params são coeridos no bind de execução, mesma rotina
}
```

Erros de bind (antes de tocar dados): coluna inexistente; tipo incomparável (tabela §3.6); LIMIT negativo; `ParamCount` ≠ len(params) na execução. Projeção `*` expande para todos os campos do schema na ordem de FieldID (nome de saída = alias se houver, senão original).

### 3.5 Planner por regras (§63) — `internal/query/planner`

Graças à decisão D4 (executor SEMPRE reavalia o WHERE completo contra o estado corrente da row — §44), o planner escolhe apenas a **fonte de candidatos** (que deve ser um *superset* dos matches). Nenhum residual é calculado.

Análise da AST:
1. **Flatten**: se a raiz (após remover parênteses) é uma cadeia de ANDs, colete os conjuncts folha; se é cadeia de ORs, colete disjuncts.
2. Classifique cada conjunct folha: `EqOnField(f)` se `Comparison{Op: =}` com campo `f`; senão `Opaque`.

Seleção (primeira regra que casar):

| Ordem | Regra de detecção | Plano |
|-------|-------------------|-------|
| 1 | Algum conjunct AND é `key = literal/param` (campo `IsKey`) | `PointLookupPlan{keyValue}` — `primary.Find` + 1 read |
| 2 | Algum conjunct AND é `f = v` com `f.Index == IndexHash` | `HashIndexPlan{field, value}` → `RowIDIterator` |
| 3 | ≥2 conjuncts AND são `f = v` com `f.Index == IndexBitmap` | `BitmapIntersectPlan{[](field,value)}` — interseção de bitmaps |
| 4 | 1 conjunct AND é bitmap-eq | `BitmapScanPlan{field, value}` |
| 5 | Raiz é OR e **todos** os disjuncts são `f = v` indexáveis (bitmap ou hash) | `IndexUnionPlan{[]lookup}` — união com dedup (via bitmap de união ou merge de iteradores ordenados) |
| 6 | Caso contrário | `FullScanPlan` — itera rows live via LocationIndex; incrementa `query.full_scan` |

Notas:
- Regra 5: se **qualquer** disjunct não for indexável, OR degrada para full scan (união parcial perderia candidatos — incorreto).
- Disjunct OR aninhado dentro de AND: o AND ainda pode usar outro conjunct indexado (ex.: `status='Stop' AND (a=1 OR b=2)` → BitmapScan em status; o OR é resolvido pela re-validação total). É por isso que candidatos = superset funciona: qualquer conjunct AND indexado é um filtro válido de superset.
- IS NULL / IS NOT NULL, `!=`, `>` etc. nunca usam índice na v1 → contribuem só para a re-validação.
- Empate nas regras 2–4: preferir hash a bitmap (seletividade maior); entre bitmaps, todos entram na interseção.
- `EXPLAIN` fora do escopo, mas o plano expõe `String()` para testes e logging.
- **Hash index é Fase 8 da spec** (storage entrega depois do M1): o planner implementa a regra 2 desde já e os testes §106 de hash lookup rodam contra o fake (a interface `LookupHash` é declarada no consumidor). No runtime real do M1, campo `index: hash` sem suporte do storage faz a regra 2 ser pulada (cai em bitmap ou full scan) — capability check no plan-time via schema+store.

### 3.6 Coerção de tipos nas comparações (tabela normativa v1)

Coerção acontece no **bind** (literal/param → representação do tipo da COLUNA). Célula vazia = erro de bind (`type mismatch: column "x" (ipv4) cannot be compared to boolean literal`).

| Tipo da coluna | literal string | literal int | literal float | literal bool | ops permitidos |
|---|---|---|---|---|---|
| string | bytes UTF-8 | — | — | — | todos (ordem: `bytes.Compare`) |
| int64 | — | int64 | promove AMBOS p/ float64 | — | todos |
| double | — | → float64 | float64 | — | todos |
| boolean | — | — | — | 0/1 | `=`, `!=` |
| timestamp | parse RFC3339 → millis | epoch millis | — | — | todos (numérico) |
| ipv4 | dotted-quad → uint32 BE | — | — | — | todos (numérico — range scan futuro) |
| ipv6 | parse → 16B | — | — | — | todos (`bytes.Compare`) |
| ipv4/ipv6_prefix | parse → 5B/17B | — | — | — | só `=`, `!=` |
| bytes | — | — | — | — | só `IS [NOT] NULL` |
| json | — | — | — | — | só `IS [NOT] NULL` |

Regras adicionais:
- Comparação com campo **ausente** na row (= NULL, D2): resultado **false** para qualquer comparador; `IS NULL` → true. Sem NOT genérico na v1, a lógica trivalente não vaza (false é seguro sob AND/OR).
- int64 vs literal float com parte fracionária: ambos promovidos a float64 (documentar perda de precisão > 2^53 — aceitável v1).
- Param `?`: coerido pelo tipo da coluna com as mesmas regras (param "string" p/ coluna ipv4 = parse dotted-quad, etc.), na execução.

### 3.7 Executor streaming — `internal/query/executor`

API do Store consumida pelo executor (interface declarada em `internal/query/executor`; contrato fechado com o storage):

```go
type Store interface {
    // ok=false → row morta/tombstoned (é a checagem de liveness dos caminhos indexados;
    // quando TTL entrar na Fase 9, passa a filtrar expirados sem mudar assinatura)
    GetByRowID(rowID types.RowID, buf []byte) (types.Record, []byte, bool, error)
    GetByKey(key, buf []byte) (types.Record, []byte, bool, error) // PointLookupPlan
    ScanLive() types.RowIDIterator                                // full scan: rowIds live, CRESCENTES, overlay incluso
    LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error) // rowIds CRESCENTES
}

// Capability opcional (Fase 8) — NÃO existe no *storage.Store do M1. Detecção por
// type assertion no plan-time (o fake do M1 implementa; o Store real só na Fase 8).
// Assinatura reservada no §15A do plano do storage.
type HashLookuper interface {
    LookupHash(field types.FieldID, v types.Value) (types.RowIDIterator, error)
}
```

A ordem crescente de rowIds nos iteradores é garantida pelo storage — pré-condição das interseções/uniões por merge de iteradores (dedup natural na união).

Pipeline pull-based, memória O(batch):

```text
CandidateSource (RowIDIterator do plano)
   → RowFetcher    (GetByRowID em rowBuf REUTILIZADO; ok=false pula a row)
   → FieldCollector (um passo linear no payload coleta os campos do WHERE + projeção
                     em slots fixos pré-computados no bind — array reutilizado)
   → PredicateEval  (avalia a árvore WHERE completa — D4/§44)
   → Projector      (monta Row com os slots da projeção)
   → LIMIT counter  (early-exit: fecha iteradores upstream — §65)
```

```go
type RowIterator interface { Next() bool; Row() Row; Err() error; Close() error }

// Row é VÁLIDA SOMENTE até o próximo Next() — zero-copy sobre rowBuf.
// O encoder (NDJSON/protocolo, Fase 6) serializa antes de avançar.
type Row struct {
    cols []ColView // slots na ordem da projeção
}
type ColView struct { Name string; Type types.FieldType; Null bool; Val types.Value }
```

- **Timeout/cancel**: `ctx.Err()` verificado a cada row candidata no full scan e a cada batch (`resultBatchSize`, default 256) nos planos indexados; deadline = min(ctx, `query.timeout` do dataset). Cancelamento fecha a cadeia (§91).
- **maxRows** (`query.maxRows`, default 100k): LIMIT implícito — aplicado como teto mesmo sem LIMIT na query.
- **Full scan**: itera rows live (API do storage/LocationIndex — ponto de coordenação: precisa de `LiveRowIterator` que percorra rowIds com location válida e não-tombstoned). Nunca materializa lista de rowIds.
- **PointLookup**: no máximo 1 row; re-valida WHERE (a row pode ter outros conjuncts).
- **Alocações**: 1 query = parse+plan alocam (frio, ok); por row em steady: 0 allocs (rowBuf, slots e Row reutilizados).
- **Encoder NDJSON** (`internal/query/executor/ndjson.go`): incluído no M1 para o harness e testes de streaming — `AppendRow(dst []byte, row Row) []byte` com `strconv.Append*`; formata ipv4/ipv6/prefix como string dotted/compressed, timestamp como RFC3339 UTC millis, bytes como base64, json inline cru.

### 3.8 QueryEngine — fachada

```go
type Engine struct{ /* schema + storage handles por dataset */ }
func (e *Engine) Execute(ctx context.Context, dataset, sql string, params []types.Value) (RowIterator, error)
```

Fluxo: lookup dataset → lexer→parser (AST) → bind (schema + params) → plan → executor. Sem cache de prepared statements na v1 (JDBC re-envia SQL; otimização futura).

---

## 4. Gerador sintético de eventos RADIUS

**Localização**: `internal/testutil/radiusgen` — importável por testes unitários, integração e pelos mains de `benchmark/` (mesmo módulo). `benchmark/` fica só com os harnesses (mains + análise), não com a lógica de geração.

```go
type Config struct {
    Seed              uint64
    Entities          int     // nº de entidades distintas (ex.: 8_000_000)
    PatchRatio        float64 // fração de eventos que são patch parcial (default 0.85)
    ExtendedJSONRatio float64 // fração de eventos usando wrappers Mongo (default 0.30)
    UnknownFieldRatio float64 // fração com campo extra desconhecido (default 0.01)
    NullFieldRatio    float64 // fração de patches com um campo = null explícito (default 0.05)
    StatusValues      int     // default 8  (Start/Stop/Interim-Update/…)
    NASCount          int     // default 300
    TerminateCauses   int     // default 10
}

type Generator struct{ /* rng rand.PCG; seqs []uint32; cfg */ }

func New(cfg Config) *Generator
// Appende UM evento JSON em dst e retorna o slice. ~0 alloc/evento em steady state.
func (g *Generator) Next(dst []byte) []byte
// Determinismo: mesmo Config ⇒ mesma sequência byte a byte (teste obrigatório).
```

Design:
- **Determinismo**: `math/rand/v2` `PCG(seed, seed^0x9e3779b97f4a7c15)`. Todo sampling deriva do rng na mesma ordem — mesma seed ⇒ corpus idêntico (testável byte a byte).
- **Distribuições de cardinalidade**: entidade sorteada ~uniforme; `id = fmt "CMP-V%04dPPCLP-%03d"` derivado do índice; `Framed-IP-Address` = `10.0.0.0 + idx` (cardinalidade ~milhões, 1:1 com entidade); `NAS-Identifier` = `idx % NASCount` (centenas, distribuição zipf-leve: 20% dos NAS levam 80% via `rng.ExpFloat64` truncado); `Acct-Status-Type` ∈ 8 valores com pesos fixos (Interim-Update dominante); `session_id` derivado de (idx, seq).
- **Sequência**: `_SEQ` monotônico por entidade — array `seqs []uint32` (8M × 4B = 32 MiB no processo do *gerador*; aceitável e documentado — é o harness, não a engine).
- **Shapes**: evento completo = ~35 campos (espelho do YAML da spec §12); patch = {id, `_SEQ`, Acct-Status-Type, contadores Acct-*-Octets/Packets, LastSessionAliveDate} (6–10 campos). Extended JSON: `_SEQ` como `{"$numberLong":"…"}`, datas como `{"$date":"…"}` (e ~10% no shape canonical `{"$date":{"$numberLong":"…"}}`). Campo desconhecido: `"X-Vendor-Attr-<n>": "…"`.
- **Escrita**: `strconv.Append*` direto em `dst` — zero `fmt.Sprintf` no steady state (allocs do gerador contaminariam a medição de allocs/evento do harness macro).
- **Corpus pré-gerado**: helper `GenerateCorpus(cfg, n) [][]byte` para micro-benchmarks `testing.B` (o custo de geração fica fora do laço medido).
- **Taxa** (eventos/s) NÃO é responsabilidade do gerador — é do harness (rate limiter por `time.Ticker`).

---

## 5. Harness de benchmark de memória (spec §108–111)

**Layout**: `benchmark/ingest-bench/main.go` (binário standalone) + `internal/testutil/memprof` (medições reutilizáveis).

### 5.1 Medição — `internal/testutil/memprof`

```go
type Sample struct {
    VmRSSBytes, VmHWMBytes uint64 // /proc/self/status — RSS real do processo (critério §109)
    FileRSSBytes, AnonRSSBytes uint64 // /proc/self/smaps_rollup — separa residência de mmap
                                      // (file-backed) do heap anônimo (§79: não confundir)
    HeapAlloc, HeapObjects, Mallocs, NumGC uint64 // runtime.ReadMemStats
    Timestamp time.Time
}
func Read() (Sample, error)
```

- **RSS de verdade**: parse de `VmRSS:`/`VmHWM:` em `/proc/self/status`. Linux-only (build tag), suficiente — o alvo é Linux.
- **Residência mmap**: `/proc/self/smaps_rollup` → `Anonymous:` vs `Rss:` (file-backed = Rss − Anonymous). Distingue "índices mmap quentes" de "heap Go" no relatório.
- **allocations/evento (macro)**: `ΔMemStats.Mallocs / Δeventos` no steady state — pega alocações que micro-benchmarks não veem (caminho completo).
- **allocations/evento (micro)**: `testing.B` + `b.ReportAllocs()` em `Materializer.Apply` com corpus pré-gerado; acompanhamento com `benchstat` entre commits (guardar baselines em `benchmark/baselines/`).
- **GC frequency**: ΔNumGC/Δt; GC CPU via `debug.GCStats`/`/memory/classes` (runtime/metrics).
- **pprof** (§112): harness expõe `net/http/pprof` em porta local opt-in (`-pprof :6060`) e grava `heap`/`allocs`/`cpu` profiles ao final de cada fase.

### 5.2 Cenário 8M sem Kafka — `benchmark/ingest-bench`

```text
nishidb-ingest-bench \
  -dataset configs/radius.materialization.yaml \
  -data /var/tmp/nishidb-bench \
  -entities 8000000 -rate 500 -steady 10m -queries 8 -seed 42 \
  -report report.json
```

Fases:
1. **LOAD**: 8M eventos completos (rate ilimitado) — mede throughput máximo de ingestão (meta §88: ≥2.000 ev/s) e RSS pós-carga.
2. **STEADY**: 500 ev/s de patches (85%) + eventos completos (15%) por N minutos, com **queries concorrentes**: K goroutines alternando primary-key lookup (p95 < 10ms, §89), bitmap query com LIMIT 100 (p95 < 50ms, §90) e 1 full scan periódico com LIMIT. Amostra `memprof.Read()` a cada 5s.
3. **REPORT**: JSON com séries temporais + agregados: RSS max, VmHWM, heap estável, allocs/evento, latências p50/p95/p99 por classe de query, GC/min.

Critérios de aceite automatizados (exit code ≠ 0 se violar):
- §109: `max(VmRSS) < 1 GiB` durante STEADY. Ambiente: `GOMEMLIMIT=512MiB GOGC=100` (§4 da spec); opcionalmente sob `systemd-run --scope -p MemoryMax=1G` para limite duro de cgroup.
- §110: rodada `-entities 1000000` vs `-entities 8000000`; `HeapAlloc` estável (após `runtime.GC()` forçado) não pode crescer ~8× — asserção: `heap(8M) < 2 × heap(1M)`.
- §111: allocs/evento macro ≤ 9 no steady state (meta pós-otimização; no M1 reportar sem falhar o build, virar gate na Fase 12).
- Latências §89/§90 reportadas (gate soft no M1 — dependem do storage real).

Dependência: este harness precisa do storage engine real (Fase 1, outro plano). Enquanto não existir, o harness roda com a implementação fake para validar a mecânica (números sem valor de aceite) — flag `-storage fake|real`.

---

## 6. Decomposição em tarefas TDD (ordem de execução)

Convenção: cada tarefa = testes primeiro (red) → implementação (green) → refactor. Testes de materialização mapeiam §103; de query, §106. Dependências indicadas. Storage real NÃO bloqueia nada até T17: usar fakes.

**T0 — Bootstrap do módulo — MOVIDO para o plano do storage** (atribuição do lead)
git init, `go.mod` (`github.com/nishisan-dev/nishidb`, go 1.25), layout §98, Makefile, `.golangci.yml`. **Não criar go.mod aqui.** Minha primeira tarefa após o scaffolding commitado: adicionar `gopkg.in/yaml.v3` (T5).

**T1 — review/consumo de `internal/types`** *(pacote é CRIADO PELO STORAGE junto do scaffolding — o encoding dele depende disso imediatamente; contrato §2.2)*
Não criar o pacote. Aqui: validar o entregue contra o contrato (FieldType, FieldID, Value sem boxing, Record, FlagTombstone) e contribuir testes de round-trip de `Value` (I64 pack/unpack de double via `math.Float64bits`) se faltarem. Interfaces ficam nos consumidores, não em types.

**T2 — `internal/unsafex`: `String(b []byte) string`**
Teste + doc: contrato "callee não retém". Único lugar com `unsafe` no M1.

**T3 — `internal/jsonp`: tokenizer** *(dep: T1)*
Testes da §1.4 + `FuzzParser`. Aceite: `BenchmarkParseRadiusEvent` = 0 allocs/op.

**T4 — `internal/materializer/typeconv`** *(dep: T1, T2, T3)*
Tabela de testes célula a célula da §2.3: cada tipo × entradas válidas/inváli­das; timestamps (RFC3339, nano, offset, epoch, `$date` relaxed/canonical); ipv4/ipv6/prefixes; base64; erros TYPE_CONVERSION. Bench: 0 allocs.

**T5 — `internal/schema`: load YAML + validação + Compiled** *(dep: T1)*
Testes: YAML da spec §12 carrega; defaults (mode=merge); erros claros (tipo inválido, alias duplicado, key ausente, unknownFields inválido, index em tipo não indexável); include explícito §14; exclude §15; `FieldByName` resolve nome E alias; lookup com `[]byte` sem alloc (bench).

**T6 — testes de contrato do encoding (consumidor)** *(dep: T1 + entrega de `internal/storage/encoding` pelo storage)*
O encoding é implementado e testado pelo plano do storage (formato fechado no §2.4). Aqui: testes de contrato do ponto de vista do consumidor — round-trip por tipo via AppendField/FieldReader, ordenação, payload vazio, tag 0x00 → erro, zero-copy de Bytes — + golden files compartilhados com o storage para detectar drift. Bench: 0 allocs no reader. Se o storage ainda não tiver entregue quando eu chegar aqui, implemento seguindo o formato fechado e o storage assume a implementação depois (formato é contrato, dono é logística).

**T7 — `internal/testutil/fakestorage`** *(dep: T1, T6)*
**Um único fake `Store`** in-memory (maps simples — é teste, escala pequena) implementando as duas interfaces de consumidor: `GetByKey`/`Put` (materializer, §2.2) e `GetByRowID`/`ScanLive`/`LookupBitmap`/`LookupHash` (executor, §3.7), + `FieldDictionary` fake. O Put do fake faz o diff prev↔novo e registra postings/chamadas para asserções (inclusive verificação de que recebeu `prev` correto). Testes mínimos do próprio fake.

**T8 — `PatchBuffer.Parse`** *(dep: T3, T4, T5)*
Testes: evento RADIUS completo → entries ordenadas/tipadas; wildcard §13 (§103-wildcard); include explícito ignora extras; exclusão (§103-exclusion); unknownFields include/ignore/reject (§103-unknown); key ausente → INVALID_KEY; sequence ausente → INVALID_SEQUENCE; null explícito → isNull; Extended JSON (§103-mongo); chave JSON duplicada last-wins.

**T9 — `mergeRecords`** *(dep: T6, T8)* — ENTREGUE
Testes §103: merge campo sobrescreve (§22 exemplo literal da spec); campo ausente mantém (§103-missing); null remove (§103-null, D2) e a ausência é transitiva; patch só com campos novos (nas duas pontas); campos intercalados; prev vazio (create); patch vazio; 11 tipos round-trip; corrupção do prev e patch fora de ordem → `ErrInternalState`. Deltas de índice saíram do escopo com o Put orquestrador (§2.2). Bench: 0 allocs.

**T10 — `Materializer.Apply` fluxo completo** *(dep: T7, T9)* — ENTREGUE
Testes §103: create; merge; replace (§103-replace — asserção de que o `prev` correto foi repassado ao Put, que é quem limpa postings antigos, e de que os postings do valor antigo somem); sequence greater aplica / equal descarta / lower descarta (§103-sequence×3); stale não escreve nada (fake sem Put registrado); dataset sem sequence aplica tudo; unknownFields include/ignore/reject fim-a-fim; erros por categoria + ApplyResult.Action nos três baldes (evento ruim → Skipped; writer morto → Failed+Fatal; defeito nosso → Failed); métricas contadas; fixture real (evento cheio → patch parcial → estado conferido campo a campo). Gate: 0 allocs/evento na suíte.

**T11 — `internal/query/lexer`** *(sem dep)*
Testes: todos os tokens; keywords case-insensitive; `''`/`""` escapes; números (int/float/negativo/exp); posições de erro; query completa da spec §68.

**T12 — `internal/query/parser` + AST** *(dep: T11)*
Testes §106: projeção `*` e lista; WHERE com AND/OR/parênteses (precedência: `a=1 OR b=2 AND c=3` → OR(a, AND(b,c))); IS NULL/IS NOT NULL; todos comparadores; placeholders numerados; quoted idents (`"Acct-Status-Type"`); LIMIT; erros de sintaxe com posição; `LIMIT ?` rejeitado.

**T13 — binder** *(dep: T5, T12)*
Testes: resolução alias/original/quoted; tabela de coerção §3.6 célula a célula (válidas + erros de mismatch); param count mismatch; projeção com coluna inexistente; `*` expande na ordem de FieldID com aliases.

**T14 — planner** *(dep: T13)*
Testes de seleção de plano (schema com key + 1 hash + 2 bitmaps): `key=?` em AND profundo → PointLookup; `nas_ip=x AND status=y` → HashIndex (hash vence bitmap); `status=x AND protocol=y` → BitmapIntersect; `status=x` → BitmapScan; `status=x OR protocol=y` → IndexUnion; `status=x OR username=z` (não indexado) → FullScan; `status!=x` → FullScan; `ip>x` → FullScan; WHERE ausente → FullScan.

**T15 — executor** *(dep: T7, T10, T14 — usa fakes semeados via Materializer)*
Testes §106 completos: primary lookup; bitmap lookup; hash lookup; AND; OR (com dedup de rowIds na união); comparison (cada op, cada tipo coercível); NULL (IS NULL acha campo ausente E campo anulado por patch — cruza com D2); LIMIT early-exit (fonte instrumentada prova que iteração parou); maxRows implícito; full scan; aliases na projeção e no WHERE; quoted fields; parameters (tipagem por coluna); timeout via ctx cancelado no meio do full scan; **re-validação §44** (candidato do índice cujo estado atual não satisfaz mais o WHERE não sai no resultado — semear via fake com índice "stale" de propósito); row zero-copy (Row inválida após Next — teste documental). Bench: 0 allocs/row em scan.

**T16 — encoder NDJSON** *(dep: T15)*
Testes: cada tipo formatado (timestamp RFC3339 UTC, ipv4 dotted, ipv6 compressed, base64, json cru, null); escaping JSON de strings; streaming O(batch) (buffer não cresce com resultado).

**T17 — `internal/testutil/radiusgen`** *(dep: nenhuma técnica; depois de T10 p/ round-trip)*
Testes: determinismo (mesma seed ⇒ bytes idênticos); todo evento parseia no jsonp E no `encoding/json` (oráculo); distribuições aproximadas (χ² frouxo: 8 status, N nas); PatchRatio/ExtendedJSONRatio/NullFieldRatio respeitados (contagem em 10k eventos); `_SEQ` monotônico por entidade; bench ~0 allocs.

**T18 — `internal/testutil/memprof`** *(sem dep; Linux build tag)*
Testes: parse de fixtures de `/proc` (arquivos de exemplo em testdata); leitura real smoke test.

**T19 — micro-benchmarks integrados** *(dep: T10, T15, T17)*
`BenchmarkApplyCreate/ApplyMergePatch/ApplyStale/QueryPointLookup/QueryBitmapLimit100/FullScan1k` com corpus radiusgen + fakes; baselines commitados; alvo M1: Apply ≤ 10 allocs (fake storage), documentado o esperado com storage real.

**T20 — `benchmark/ingest-bench` harness** *(dep: T17, T18, T19; storage real quando disponível)*
Teste: smoke run 10k entidades / 5s steady com `-storage fake` valida mecânica, relatório JSON e gates. Rodada 8M real fica agendada para quando a Fase 1 (storage) entregar — gates §109/§110 ligados aí.

Caminho crítico: T0→T1→T3→T4→T8→T9→T10 (materializer) e T11→T12→T13→T14→T15 (query, paralelizável com o ramo do materializer a partir de T5). T6/T7 destravam cedo e são pequenos.

---

## 7. Riscos e armadilhas (onde é mais fácil errar)

| # | Risco | Mitigação |
|---|-------|-----------|
| R1 | **Semântica NULL no merge** (null vs ausente vs "nunca existiu") vazando inconsistência entre materializer, índices e `IS NULL` | Decisão única D2 (NULL = ausência) aplicada nas 3 camadas; testes cruzados T9/T10/T15 (campo anulado por patch precisa: sumir do record, remover posting do índice, e casar `IS NULL`) |
| R2 | **Alocações escondidas**: `string(b)` fora de map-lookup, boxing em `interface{}`, closure de `sort.Slice`, `time.Parse`, `fmt` no hot path, growth de slices | `Value` sem interface (D6); `unsafex.String` único e auditado (D7); parsers próprios (RFC3339, ipv4, int); `b.ReportAllocs()` como gate desde T3 — não deixar para a Fase 12 |
| R3 | **Coerção de tipos SQL** — matriz grande, fácil divergir do materializer (ex.: timestamp aceita formato X na ingestão e Y na query) | Tabela normativa §3.6; binder e typeconv compartilham os MESMOS parsers (RFC3339, dotted-quad); testes célula a célula T13 |
| R4 | **Formatos de timestamp ambíguos** (epoch seconds vs millis; formatos não-RFC3339) | v1 aceita SÓ RFC3339(Nano) e número = epoch millis. Sem heurística de segundos. Documentado; erro claro caso contrário |
| R5 | **OR no planner**: união parcial de índices perde candidatos (resultado silenciosamente incompleto) | Regra dura: OR só usa índices se TODOS os disjuncts forem indexáveis; senão full scan. Teste T14 dedicado. Dedup na união (bitmap de união) — teste T15 |
| R6 | **Replace com índice secundário** esquece de ler o estado anterior → postings órfãos | **Mitigado por construção** com D11: o `Put` diffa prev↔novo sempre (merge ou replace); T10 garante que o Apply repassa o `prev` do GetByKey |
| R7 | **Re-validação §44 esquecida** em algum plano (PointLookup é o típico: "achou pela key, retorna") | Executor tem UM caminho de avaliação de predicado para todos os planos (D4); teste T15 com índice deliberadamente stale |
| R8 | **Lifetime de buffers zero-copy**: `Field.Raw` do parser e `Row` do executor válidos só até o próximo Next — uso após avanço = corrupção silenciosa | Contrato em doc comment + cópia imediata p/ arena no PatchBuffer; testes documentais; considerar `-race` + testes com reuso agressivo de buffer (sobrescrever com 0xAA no Reset em builds de teste) |
| R9 | **Desalinhamento com o plano de storage** | **Contrato FECHADO em 2026-08-12** (§2.2/§2.4/§3.7; §15A no plano do storage). Risco residual: drift de implementação — mitigado por testes de contrato + golden files (T6) |
| R10 | **Chave/sequence ausentes ou JSON duplicado** em eventos reais sujos | Categorias §101 mapeadas em T8; política log-and-skip testada; last-wins documentado |
| R11 | **int64 vs double no `auto`**: número "5000" vira int64, depois patch traz "5000.5" → conflito de tipo no mesmo campo | Regra v1: tipo do campo auto é fixado na PRIMEIRA observação (registrada no dicionário); conversão posterior incompatível = TYPE_CONVERSION log-and-skip. Documentar |
| R12 | **Gerador contaminando a medição** (allocs do harness contadas como allocs da engine) | Gerador ~0 alloc (D8) + corpus pré-gerado nos micro-benchmarks |

---

## 8. Contrato com o plano do storage — FECHADO (2026-08-12; §15A no plano do storage)

Acordos (detalhados em §2.2, §2.4 e §3.7):

1. `internal/types` só com tipos de dados: FieldType, FieldID=uint16, `Value` união sem boxing, `Record{RowID, Sequence, Flags, Key, Payload}`. Interfaces declaradas no consumidor. `PhysicalLocation` fora do contrato (interna ao storage; publicação atômica via u64 empacotado).
2. Encoding do payload: dono `internal/storage/encoding` (plano do storage), formato campo-a-campo ordenado por FieldID exatamente como §2.4; tag 0x00 reservada; framing/CRC internos ao Put.
3. `FieldDictionary.LookupOrCreate` (ID novo só retorna pós-fdatasync) — sim. `RowIDAllocator` não existe (absorvido pelo Put).
4. **Write path orquestrado pelo `Put`** (D11): Apply faz GetByKey → stale check → merge/replace → `Put(key, seq, flags, payload, prev)`. Publicação atômica por row (§45) acontece como último passo do Put; ordem §46 vive em um lugar só (também usada pelo recovery tail-replay).
5. Executor: `GetByRowID` (ok=false = morta/tombstoned, vira filtro de TTL na Fase 9 sem mudar assinatura), `ScanLive()`, `LookupBitmap(fieldID, value)` — todos os iteradores com rowIds em ordem crescente. `LookupHash` fica para a Fase 8 (fake no M1; planner com capability check).
6. `sequences.idx` existe (§38, recovery), mas o hot path usa `prev.Sequence` do GetByKey.

Confirmações finais do plan-storage (2026-08-12, registradas no §15A dele):
- Assinaturas com buffer do caller confirmadas: `GetByKey(key, buf)`/`GetByRowID(rowID, buf)` com Record.Key/Payload zero-copy sobre o buf devolvido (2º retorno, possivelmente crescido), válido até a próxima chamada com o mesmo buf.
- `Record.Flags`: constantes em `internal/types` (fonte única): `FlagTombstone uint8 = 1<<0`, bits 1-7 reservados (zero). M1 sempre escreve 0.
- `LookupHash`: assinatura reservada; capability por type assertion consumer-side (`HashLookuper`), fake do M1 implementa.
- `internal/types` é criado pelo STORAGE junto do scaffolding (T0/T1 dele); meu T1 vira review/consumo.

Pendência aberta (única, não bloqueia planejamento): aviso do plan-storage quando o scaffolding (git init + go.mod + layout + internal/types) estiver commitado — destrava minhas T1/T5. Plano dele aprovado; execução do T0 em andamento.

---

## 9. Definition of Done do M1

- [ ] `go test ./... -race` verde; fuzz do jsonp rodado (≥ 5 min sem crash).
- [ ] Todos os casos §103 (materialização) e §106 (query) com teste nomeado rastreável.
- [ ] Micro-benchmarks com baselines commitados; parser/typeconv/merge/executor-row = 0 allocs; `Apply` ≤ 10 allocs com fakes (meta single-digit §111 vira gate com storage real na integração).
- [ ] `radiusgen` determinístico e validado por oráculo `encoding/json`.
- [ ] `ingest-bench` roda fim-a-fim com `-storage fake` (mecânica + relatório JSON + gates implementados); rodada 8M/1GiB agendada para a integração com a Fase 1.
- [ ] `docs/` atualizado: `architecture.md` (materializer + query) e diagramas PlantUML (`sequence_apply.puml`, fluxo de query) conforme padrão do repositório.
