# NishiDB M1 — Design do Subsistema de Storage

Fonte de verdade: `/home/lucas/Projects/nishisan/nishidb/nishidb-spec-proposal.md` (seções 30-58, 98-99, 104, 113, 116).
Escopo M1: Fase 1 (segments, records, CRC32C, field dictionary, rowId, primary mmap index, location index) + Fase 4 (bitmap) + checkpoint/recovery para "write → restart → lookup by id".

## 0. Decisões-chave (resumo)

| Decisão | Escolha | Alternativas rejeitadas |
|---|---|---|
| Endianness | Little-endian em tudo | BE (sem benefício; LE é nativo em amd64/arm64) |
| Hash da primary | FNV-1a 64 + finalizer fmix64, hand-rolled | maphash (seed não serializável), xxhash (dep externa) |
| Publicação atômica de Location | uint64 empacotado `flags(2)|segmentID(32)|offset(30)`, SEM length | 12 bits de segmentID (esgota em ~50 dias), seqlock (complexo), generation counter |
| Durabilidade de locations | Overlay em heap (bounded pelo intervalo de checkpoint) + apply no checkpoint | Escrita direta no mmap (risco de dangling pointer em power loss) |
| Leitura de segments | pread (ReadAt) em buffer reutilizável | mmap de segments (torn tail visível, remap constante) |
| mmap | golang.org/x/sys/unix (Mmap/Msync/Munmap/Fallocate) | syscall pkg (frozen), CGo |
| Key no record | Key explícita no header do record (keyLength u16 + bytes) | Key só como field do payload (impede validação de colisão sem schema) |
| Concorrência remap | RWMutex em volta do ponteiro da região; RLock por acesso | epoch/refcount (complexidade), leak-until-close (RSS de arquivos deletados) |
| Length de campos varlen | uvarint | u32 fixo (~12% de overhead no estado médio) |
| rowId | uint64 monotônico, nunca reutilizado, começa em 0 | reuso (ABA em locations) |

## 1. Convenções globais

- Todos os inteiros multi-byte em **little-endian** (`encoding/binary`).
- CRC: **CRC32C (Castagnoli)** via `hash/crc32.MakeTable(crc32.Castagnoli)` — acelerado por HW na stdlib.
- Magics de 4 bytes ASCII: `NDBR` (record), `NDBS` (segment), `NDBP` (primary), `NDBL` (locations), `NDBQ` (sequences), `NDBB` (bitmap), `NDBD` (fields.dict), `NDBV` (values.dict), `NDBM` (manifest).
- Todos os arquivos mmap têm header de 64 bytes (1 cache line) — mantém entries alinhadas a 8/16 bytes.
- Plataforma alvo M1: Linux amd64/arm64.

## 2. Record format (seções 31-33)

```
off  tam  campo
0    4    magic "NDBR"
4    1    formatVersion = 1
5    1    flags (bit0 = tombstone; demais reservados)
6    2    keyLength  (u16, máx configurável, default 1 KiB)
8    8    rowId      (u64)
16   8    sequence   (i64)
24   4    fieldCount (u32)
28   4    payloadLength (u32; só payload, sem key)
32   4    crc32c sobre bytes [4..32) ++ key ++ payload
36   K    key (serviceKey bytes crus)
36+K P    payload (campos codificados)
```

Header fixo = 36 bytes. CRC cobre header (exceto magic e o próprio CRC), key e payload — um header rasgado com payloadLength plausível não engana o tail scan.

**Key explícita no record** (desvio deliberado da spec, que não a menciona): necessária para (a) validação de colisão da primary sem conhecimento de schema (seção 36), (b) tombstones sem payload (seção 58), (c) replay do tail reconstruir a primary sem decodificar semanticamente o payload. Custo ~20-30 bytes/record sobre estados de 700-1000 bytes.

### 2.1 Encoding de campo

```
fieldID  u16 (dicionário)
type     u8
[fixed-width]  valor direto
[varlen]       length uvarint + bytes
```

Type tags e larguras:

| tag | tipo | wire |
|---|---|---|
| 0x00 | null | 0 bytes |
| 0x01 | int64 | 8 LE |
| 0x02 | double | 8 IEEE754 LE |
| 0x03 | boolean | 1 (0/1) |
| 0x04 | timestamp | 8 LE (epoch millis) |
| 0x05 | ipv4 | 4 (network order) |
| 0x06 | ipv6 | 16 |
| 0x07 | ipv4_prefix | 5 (4 addr + 1 prefixLen) |
| 0x08 | ipv6_prefix | 17 |
| 0x09 | string | uvarint + UTF-8 |
| 0x0A | bytes | uvarint + raw |
| 0x0B | json | uvarint + JSON UTF-8 |

**Campos ordenados por fieldID crescente, sem duplicatas.** Merge (M2) vira two-pointer linear; extração de campo único (compaction, replay de bitmap) vira scan curto/early-exit; encoding determinístico.

**NULL vs ausente:**
- Wire suporta `type=0x00` (null explícito) — usado em *patches codificados em memória* (M2), onde null = "set NULL" e ausente = "mantém anterior" (seção 24).
- **Records materializados gravados em segmento NUNCA contêm campos null-tagged**: merge/replace remove o campo. Estado armazenado: ausente ≡ NULL. Economiza bytes e torna `IS NULL` = "campo ausente".

## 3. Segment file (seção 30)

`data/segment-%08d.dat`, IDs a partir de 1, monotônicos, nunca reutilizados.

```
Header 64 bytes: magic "NDBS" | version u8 | reserved[3] | segmentID u64 | createdAtUnixMs i64 | segmentSizeHint u64 | reserved até 64
Records back-to-back a partir do offset 64.
```

- Rotação: record deve caber inteiro; se `offset + len > segmentSize` → sela o atual (nada gravado no arquivo; manifest registra) e abre `ID+1`.
- `segmentSize` default 512 MiB, **máximo 1 GiB** (limite do packing de offset — validar na config).
- Escrita: `write()` direto no fd (buffer do builder já é o record completo); `Sync()` = `fdatasync`. Leitura: `pread` em buffer do caller — primeiro pread de `min(4096, resto)` bytes, parse do header, segundo pread só se o record for maior. Page cache serve como cache de leitura; sem mmap de segments (evita expor tail parcial e remap contínuo).
- `MaxRecordSize` default 4 MiB (sanity bound para tail scan e alocação de buffer).

## 4. fields.dict (seção 32)

`metadata/fields.dict`, append-only.

```
Header 8 bytes: "NDBD" | version u8 | reserved[3]
Entry: nameLen u16 | typeHint u8 | name bytes | crc32c u32 (sobre nameLen..name)
```

- fieldID implícito = ordem da entry (1-based; 0 reservado). Máx 65535 campos (u16).
- Open: scan integral validando CRC; prefixo válido define o dict; entry rasgada no fim → `ftruncate` no último boundary válido.
- Em memória: `map[string]uint16` + `[]entry` — bounded pela cardinalidade de campos (dezenas/centenas), permitido pela filosofia de memória.
- **Regra de durabilidade: entry nova é appendada e `fdatasync`ada ANTES do primeiro record que usa o fieldID ser appendado ao segmento.** Garante que replay do tail sempre resolve fieldIDs. Campos novos são raros → custo irrelevante.

## 5. primary.idx (seções 35-36)

Hash table open-addressing, mmap, **linear probing**, slots de 16 bytes.

```
Header 64 bytes: "NDBP" | version u8 | hashKind u8 (1 = fnv1a64+fmix64) | reserved u16 | slotCount u64 (potência de 2) | seed u64 (0 na v1) | reserved até 64
Slot (offset 64 + i*16): hash u64 | rowId u64
```

- **Hash: FNV-1a 64 hand-rolled (~8 linhas, zero-alloc, determinístico entre restarts) + finalizer fmix64 (murmur3)** para corrigir a dispersão dos bits baixos do FNV sob máscara de potência de 2. `maphash` é descartado porque o Seed é opaco e não serializável (a tabela é persistente); xxhash seria dep externa. `hashKind` no header permite trocar com rebuild.
- Sentinelas no campo hash: `0` = EMPTY, `1` = DELETED (reservado, sem remoção na v1 — tombstone TTL mantém a key na primary). Hash real `< 2` → `+= 2` (falso positivo eventual é resolvido pela validação de key).
- Load factor: resize ao atingir **0.70**. Pre-sizing: `slotCount = nextPow2(max(1<<20, EstimatedRows/0.7))`. Para 8M rows → 16Mi slots → 256 MiB de arquivo (disk-backed; residência parcial).
- **Resize/rehash: arquivo novo `primary.idx.new` com 2× slots; re-insere todos os slots ocupados; fdatasync; rename sobre `primary.idx`; remap com swap de ponteiro sob Lock.** Executa no writer (single writer é o único mutador); ingestão pausa alguns segundos no pior caso — mitigado pelo pre-sizing; logar/metricar.
- **Publicação de slot (insert):** escreve `rowId` (store simples), depois `hash` com `atomic.StoreUint64` (release). Leitor: `atomic.LoadUint64(hash)` (acquire); se bate, lê `rowId`. Como o hash é o marcador de ocupação e slots nunca são movidos fora do rehash, não há torn read.
- **Validação de colisão (caminho de leitura):** probe → slot com hash igual → `rowId` → `locations.Get(rowId)` → pread do header do record (36 bytes + key) → compara `key` byte a byte com a procurada. Diferente → continua o probe. Igual → achou. O hash de 64 bits torna a leitura de validação ~sempre conclusiva na primeira tentativa; a validação existe por correção (seção 36).
- Insert é **insert-if-absent** (retorna rowId existente se a key já está lá) — torna o replay idempotente.

## 6. locations.idx (seções 37, 45) — DECISÃO CRÍTICA

**Escolha: (a) uint64 empacotado, modificado — SEM length no packed word.**

```
packed u64: [63..62] flags (2 bits; bit63 reservado p/ tombstone) | [61..30] segmentID (32 bits) | [29..0] offset (30 bits)
```

- A sugestão original (`segmentID 12 | offset 30 | length 22`) **não fecha**: segmentIDs são monotônicos e nunca reutilizados; a 500 ev/s × 700B-1KB ≈ 30-43 GB/dia gravados (seção 86) = 60-86 segmentos/dia → 4096 IDs esgotam em ~50-60 dias. Reuso de ID criaria ABA (location velha apontando para segmento novo).
- Removendo o length do packed word sobram 32 bits para segmentID (esgota nunca) e 30 bits para offset (segmentos até 1 GiB; default 512 MiB ok). O length é recuperado do header do record (protegido por CRC); o caminho de leitura faz 1 pread de 4 KiB que cobre 99% dos records (média 700-1000B) e um segundo pread apenas para records maiores. Custo marginal ~0 com page cache.
- **Bônus: entry de 8 bytes → locations.idx cai de 128 MB para 64 MB** com 8M rows.
- Publicação atômica trivial e correta: `atomic.StoreUint64` / `atomic.LoadUint64` em word alinhada. Sem seqlock, sem generation counter.
- `packed == 0` = "sem location" (segmentID real começa em 1; nenhum valor válido é zero).
- Seqlock (b) e generation (c) rejeitados: exigem 24 bytes/entry ou striping + loops de retry com ordering manual — complexidade sem benefício depois que o length sai do word.

```
Arquivo: header 64 bytes ("NDBL" | version | entrySize=8 | reserved) + array denso u64 indexado por rowId (offset da entry = 64 + rowId*8).
```

### 6.1 Overlay de locations (durabilidade contra power loss)

Problema: se o writer publica direto no mmap, o kernel pode fazer writeback de uma página do locations.idx ANTES do fdatasync do segmento correspondente. Em power loss, o índice em disco aponta para um record que nunca chegou ao disco (dangling) e o estado anterior da row foi sobrescrito — perda irrecuperável.

Solução: **as publicações de Location entre checkpoints vivem num overlay em heap** (`map[uint64]uint64` + RWMutex), aplicadas ao mmap somente no checkpoint, DEPOIS do fdatasync dos segmentos:

- Bounded: 500 ev/s × intervalo de checkpoint 30s ≈ 15k entries (~1-2 MB) — independe do dataset, respeita a filosofia de memória.
- Leitura: overlay primeiro (RLock), senão atomic load no mmap.
- Invariante resultante: **qualquer página do locations.idx que chegue ao disco só referencia records já duráveis**. Elimina a classe inteira de dangling pointers.
- primary.idx, sequences.idx e bitmaps escrevem direto no mmap: análise de crash mostra que replay do tail converge esses índices (ver §10); um slot vazado da primary vira, no pior caso, slot lixo inofensivo (a validação de key falha e o probe segue).

## 7. sequences.idx (seção 38)

```
Header 64 bytes: "NDBQ" | version | entrySize=8 | reserved
Array denso i64 indexado por rowId. 8M rows → 64 MB.
```

- `atomic.StoreInt64`/`LoadInt64`. Sem sentinela: sequence só tem significado para rowIds com location publicada.
- Replay de recovery grava incondicionalmente (`sequences[r] = record.sequence`) — corrige qualquer valor vazado maior que o estado real (senão a re-entrega Kafka seria descartada indevidamente por staleness).

## 8. Bitmap indexes (seções 41, 116)

Por campo indexado: `indexes/bitmap/<field>/`:

```
values.dict  (append-only): header 8B "NDBV" | entries: type u8 | valLen u16 | valBytes | crc32c
             valueID implícito sequencial (1-based).
<valueID>.bm : header 64B "NDBB" | version | fieldID u16 | valueID u32 | reserved
               + array de words u64; bit da rowId = word[rowId/64], bit (rowId%64).
```

- Bitmap plano (não comprimido): 8M rows ≈ 1 MB por valor distinto — dentro do budget da spec; roaring é complexidade desnecessária na v1.
- Em memória: `map[valueKey]valueID` por índice (cardinalidade baixa por definição de bitmap index).
- Escrita (single writer): update de row com campo indexado → `Clear(oldValue, rowId)` + `Set(newValue, rowId)` usando `atomic.OrUint64` / `atomic.AndUint64` (Go 1.23+). Leitores fazem `atomic.LoadUint64` por word.
- Consistência: bits podem estar levemente stale para leitores — aceitável porque **o predicate é sempre revalidado contra o estado corrente** (seção 44). Falso positivo é filtrado; falso negativo não ocorre no caminho normal porque o writer só limpa o bit antigo ao publicar o novo estado, e o replay pós-crash reconverge (§10).
- Crescimento: lazy — `Set` além do tamanho atual → fallocate+remap; `Lookup` além do tamanho → zeros.
- `values.dict` segue a mesma regra do fields.dict: fsync antes do primeiro uso do valueID novo.
- Iterator: scan de words; word ≠ 0 → emite rowIds em ordem crescente.

## 9. manifest.bin (checkpoint M1, seção 50)

Arquivo pequeno, reescrito atomicamente (`manifest.bin.tmp` → fdatasync → rename → fsync do diretório).

```
"NDBM" | version u8 | reserved[3]
manifestEpoch u64
createdAtUnixMs i64
nextRowID u64
rowCount u64                      (advisory)
currentSegmentID u32
durableOffset u64                 (S: tudo ≤ S no segmento corrente é durável E refletido nos índices mmap)
segmentCount u32 + por segmento: segmentID u32 | sizeBytes u64 | totalRecords u64 | liveRecords u64 | liveBytes u64
primarySlotCount u64 | primaryUsed u64 | hashKind u8
fieldDictValidBytes u64           (advisory; dict é validado por CRC no scan)
bitmapFieldCount u16 + por campo: fieldID u16 | valuesDictValidBytes u64
kafkaSection: count=0             (reservado p/ M3)
crc32c u32
```

Stats de liveness por segmento já são mantidas (baratas) para a Fase 10 (compaction), mas o compactor fica fora do M1.

## 10. Fluxo de escrita, checkpoint e recovery

### 10.1 Put (writer único; ordem da seção 46)

```
1. encode record (RecordBuilder, buffer reutilizável)
2. rotação se necessário; write() no segmento          (append)
3. re-parse/CRC do buffer + offset accounting           (validate)
4. primary.Insert(hash, rowId) se key nova              (store-release no slot)
5. bitmap diffs: itera campos indexados de prev vs novo (Or/And atômicos)
6. sequences[rowId] = seq                               (atomic store)
7. locations overlay: publish(rowId, packed)            ← COMMIT POINT de visibilidade
```

O `prev *Record` é passado pelo caller (materializer já o leu para o merge) — evita segunda leitura. Leitor concorrente vê estado antigo ou novo, nunca parcial: a location é o único commit point (seção 45).

Visibilidade ≠ durabilidade: leitores podem ver estado ainda não fsyncado (semântica aceita — recovery via replay do tail + re-consumo Kafka do offset checkpointado cobre a janela).

### 10.2 Checkpoint (executa NO goroutine do writer, M1)

```
1. fdatasync segmento corrente  → S' = offset do tail
2. aplica overlay de locations no mmap (atomic stores)
3. msync(MS_SYNC) primary, locations, sequences, bitmaps
4. escreve manifest (durableOffset = S') com rename atômico
5. limpa overlay (swap de map sob Lock)
```

Rodar no writer elimina concorrência writer×checkpoint; a fila de ingestão (5000) absorve a pausa (dezenas de ms). Crash em qualquer ponto → manifest antigo (S) vale; replay de S→tail é idempotente:

- primary: insert-if-absent ✓
- locations: replay re-publica na ordem dos records; valor final = último record válido ✓
- sequences: overwrite incondicional ✓
- bitmaps: para cada record replayed, `prev = read(locations[r])` está sempre correto durante o replay forward (base = estado S ou record já replayed); todas as transições do tail são re-executadas → estado final dos bits converge, sobrando no máximo falso positivo transitório que a revalidação de predicate filtra ✓

### 10.3 Recovery (Open)

```
1. lê manifest (CRC). Ausente + diretório vazio → dataset novo. Ausente/corrupto com dados → erro fatal (rebuild total fica p/ fase futura).
2. scan fields.dict e values.dict (prefixo CRC-válido; ftruncate de entry rasgada).
3. abre mmaps (fallocate garante alocação real), abre segmentos (manifest + dir listing p/ segmentos criados pós-S).
4. tail scan do segmento corrente a partir de S (e segmentos seguintes do início):
   record válido = magic + version + bounds sane (keyLen, fieldCount, payloadLength ≤ MaxRecordSize, fim ≤ fileSize) + CRC ok
   → replay (mesmo caminho do Put, direto no mmap — sem leitores ainda)
   primeiro inválido/incompleto → ftruncate do arquivo naquele offset e para (seções 51-52)
5. nextRowID = max(manifest.nextRowID, maxRowIdVisto+1)
6. checkpoint imediato (S = novo tail) → estado limpo; libera queries; (M3: seek Kafka)
```

## 11. mmap em Go

- **`golang.org/x/sys/unix`**: `unix.Mmap(fd, 0, size, PROT_READ|PROT_WRITE, MAP_SHARED)`, `unix.Msync(b, MS_SYNC)`, `unix.Munmap`, `unix.Fallocate`. O pacote `syscall` está frozen; x/sys é o caminho suportado e já é dependência aceita.
- **`Fallocate` obrigatório em create/grow** (não só ftruncate): página sparse + disco cheio no writeback = SIGBUS no processo. Pré-alocar elimina.
- **Crescimento**: fallocate novo tamanho → novo `unix.Mmap` do tamanho total → swap do ponteiro da região sob `Lock` → `Munmap` da antiga ainda sob `Lock` (nenhum leitor em voo, pois leitores usam `RLock`). Crescimento é raro (doubling + pre-sizing); RLock não contende na prática (~20ns).
- **Alinhamento/unsafe**: mapeamentos são page-aligned; headers de 64B mantêm words alinhadas a 8. Todo `unsafe` confinado em `internal/storage/mmap`: `Region` expõe `AtomicLoad64(off)`, `AtomicStore64`, `Or64`, `And64`, `Bytes(off,len)`, com assert `off%8==0`. Nenhuma slice apontando para dentro do mmap escapa da API pública (Record.Key/Payload apontam para buffer de pread do caller).
- **Consistência leitor×writer**: pares release/acquire (`sync/atomic`) nos pontos de publicação (slot hash da primary, word do locations, words de bitmap, sequence). O conteúdo do record vai por pread: o `write()` do segmento completa antes do publish atômico (program order do writer) e a coerência page-cache do kernel garante read-after-write.
- `msync` sempre sobre a slice inteira do mapeamento (range page-aligned); só páginas sujas são escritas.
- Opcional (M12): `madvise(MADV_RANDOM)` na primary.

## 12. Interfaces Go públicas (contrato para M2+)

Pacotes: `internal/storage` (façade `Store`), `internal/storage/{encoding,segment,mmap,fielddict,rowindex,checkpoint}`, `internal/index/{primary,bitmap}`. (`fielddict`/`rowindex` são acréscimos à estrutura sugerida na seção 98; `hash/` e `compaction/` ficam vazios até M-futuro.)

```go
package storage

type RowID uint64

type Flags uint8
const FlagTombstone Flags = 1 << 0

type Location struct { SegmentID uint32; Offset uint32 }

type Record struct {
    RowID    RowID
    Sequence int64
    Flags    Flags
    Key      []byte // fatia do buf retornado
    Payload  []byte // fatia do buf retornado; campos ordenados por fieldID
}

type Options struct {
    Dir           string
    SegmentSize   int64    // default 512 MiB; máx 1 GiB
    EstimatedRows uint64   // pre-sizing primary/locations/sequences
    MaxRecordSize int      // default 4 MiB
    MaxKeySize    int      // default 1 KiB
    BitmapFields  []string // nomes de campos com index: bitmap
    SyncEveryPut  bool     // modo durability=sync (testes)
}

func Open(opts Options) (*Store, error)          // executa recovery completo
func (s *Store) Put(key []byte, seq int64, flags Flags, payload []byte, prev *Record) (RowID, error)
func (s *Store) GetByKey(key, buf []byte) (Record, []byte, bool, error)   // devolve buf (possivelmente crescido)
func (s *Store) GetByRowID(id RowID, buf []byte) (Record, []byte, bool, error)
func (s *Store) LookupBitmap(field string, v encoding.Value) (RowIDIterator, error)
func (s *Store) FieldDict() *fielddict.Dict
func (s *Store) Sync() error        // fdatasync do segmento corrente (loop de durability batch chama)
func (s *Store) Checkpoint() error  // chamar do goroutine writer
func (s *Store) Stats() Stats
func (s *Store) Close() error

type RowIDIterator interface { Next() bool; RowID() RowID; Err() error; Close() error }
```

```go
package encoding

type FieldType uint8 // TypeNull=0x00 ... TypeJSON=0x0B (tabela §2.1)

type Value struct {  // sem interface{} — zero-alloc no hot path
    Type  FieldType
    Num   int64   // int64, timestamp, boolean(0/1)
    Float float64
    Bytes []byte  // string, bytes, json, ipv4(4), ipv6(16), prefixes
}
func (v Value) Equal(o Value) bool

type RecordBuilder struct{ /* buf reutilizável */ }
func (b *RecordBuilder) Reset(rowID uint64, seq int64, flags uint8, key []byte)
func (b *RecordBuilder) AppendField(fieldID uint16, v Value) error // exige fieldID crescente
func (b *RecordBuilder) Finish() ([]byte, error)                   // fecha header + CRC

type Header struct { RowID uint64; Sequence int64; Flags uint8; FieldCount uint32; Key, Payload []byte }
func ParseRecord(buf []byte) (Header, error) // magic+bounds+CRC

type FieldIter struct{ ... }
func NewFieldIter(payload []byte) FieldIter
func (it *FieldIter) Next() bool
func (it *FieldIter) FieldID() uint16
func (it *FieldIter) Value() Value
func (it *FieldIter) Err() error
```

```go
package primary

func HashKey(key []byte) uint64 // fnv1a64 + fmix64
type ValidateFunc func(rowID uint64) (bool, error) // compara key real via Store
func Open(path string, slotCount uint64) (*Index, error)
func (ix *Index) Find(hash uint64, v ValidateFunc) (rowID uint64, ok bool, err error)
func (ix *Index) Insert(hash uint64, rowID uint64, v ValidateFunc) (existing uint64, inserted bool, err error)
func (ix *Index) LoadFactor() float64
func (ix *Index) Grow() error // rebuild em .new + rename + remap
func (ix *Index) Msync() error
func (ix *Index) Close() error
```

```go
package rowindex

func Pack(segmentID, offset uint32, flags uint8) uint64
func Unpack(p uint64) (segmentID, offset uint32, flags uint8)

type Locations struct{ /* mmap + overlay */ }
func (l *Locations) Get(rowID uint64) (packed uint64, ok bool)  // overlay-first
func (l *Locations) Publish(rowID uint64, packed uint64)        // overlay (writer)
func (l *Locations) ApplyOverlay() error                        // checkpoint
func (l *Locations) SetDirect(rowID uint64, packed uint64)      // replay/recovery

type Sequences struct{ /* mmap */ }
func (q *Sequences) Get(rowID uint64) int64
func (q *Sequences) Set(rowID uint64, seq int64)
```

```go
package bitmap

func Open(dir string, fieldID uint16) (*Index, error)
func (ix *Index) Set(v encoding.Value, rowID uint64) error
func (ix *Index) Clear(v encoding.Value, rowID uint64) error
func (ix *Index) Lookup(v encoding.Value) (Iterator, bool, error)
func (ix *Index) Msync() error
func (ix *Index) Close() error
```

As interfaces conceituais da seção 99 (`StorageEngine`, `PrimaryIndex`, `LocationIndex`, `SecondaryIndex`) serão declaradas nos CONSUMIDORES (materializer/query, idiom Go); `*storage.Store` e os índices acima as satisfazem.

## 13. Tarefas TDD ordenadas

Cada tarefa: teste primeiro → implementação → refactor. `make test` verde antes de cada commit atômico.

- **T0 — Scaffolding** (sem dep): git init; `go.mod` (`github.com/nishisan-dev/nishidb`, go 1.25); estrutura de dirs (§12 + `cmd/nishidb` stub compilável, `configs/`, `testdata/`, `planning/`, `docs/diagrams/`); `Makefile` (build/test/test-race/lint/bench/cover); `.golangci.yml` (govet, staticcheck, errcheck, revive, gofmt); `.gitignore`. Smoke: `go build ./...`.
- **T1 — encoding: tipos e Value** (dep T0): roundtrip encode/decode de cada tipo (int64, double, bool, timestamp, ipv4, ipv6, prefixes, string, bytes, json, null); uvarint bounds. Arquivos: `internal/storage/encoding/{types.go,types_test.go}`.
- **T2 — encoding: RecordBuilder/ParseRecord/FieldIter** (dep T1): build+iterate N campos; ordem violada → erro; **CRC ok / CRC corrompido (flip de byte) → erro** [spec104: CRC, invalid CRC]; null tag presente em patch, regra "record materializado não guarda null"; bounds (keyLen, MaxRecordSize).
- **T3 — mmap.Region** (dep T0): create+fallocate+map; grow+remap com swap; atomic ops; msync; **reopen preserva conteúdo** [spec104: mmap reopen]; assert de alinhamento.
- **T4 — hash** (dep T0): golden vectors fnv1a+fmix64; determinismo; smoke de distribuição (buckets ~uniformes p/ 100k keys).
- **T5 — segment.Writer/Reader** (dep T2): **append+read roundtrip** [spec104: append, read]; **rotação cria segment-00000002** [spec104: segment rotation]; reopen p/ append; leitura com CRC inválido → erro; two-pread p/ record > 4 KiB.
- **T6 — index/primary** (dep T3,T4): insert/find 100k; colisão forçada (hash function injetável em teste) valida via callback; sentinelas 0/1; load factor → Grow preserva tudo; reopen; insert-if-absent idempotente.
- **T7 — rowindex (Locations+Sequences)** (dep T3): pack/unpack bounds (offset 2^30-1, segmentID 2^32-1, flags); zero = missing; overlay: publish→get antes do apply, apply→mmap, clear; grow lazy; reopen.
- **T8 — fielddict** (dep T0): IDs sequenciais; reopen rescan; **tail rasgado → prefixo válido + ftruncate**; fsync-on-new; limite u16.
- **T9 — index/bitmap** (dep T1,T3): set/lookup/clear; iterator ordenado; valor novo cria dict entry + arquivo; grow lazy; lookup além do tamanho = vazio; reopen.
- **T10 — storage.Store (façade + write path)** (dep T5,T6,T7,T8,T9): put/get roundtrip por key e por rowId; update mantém rowId e troca estado; bitmap diff em update (A→B: lookup A perde a row, B ganha); Put com prev; visibilidade: location é commit point (teste com leitor concorrente + `-race`).
- **T11 — checkpoint.Manifest + Store.Checkpoint** (dep T10): encode/decode+CRC; rename atômico; manifest corrompido → erro; **checkpoint** persiste S/nextRowID/stats [spec104: checkpoint].
- **T12 — recovery/tail replay** (dep T11): write N → checkpoint → write M → reopen → N+M visíveis (**critério do M1: write → restart → lookup by id**) [spec104: restart, replay]; **tail incompleto (truncar bytes finais) → descarta último record + ftruncate** [spec104: incomplete tail]; CRC corrompido no tail → para ali; replay reconverge bitmaps/sequences (cenários do §10.2); segmentos pós-S criados por rotação são descobertos e replayed; reopen de todos os mmaps [spec104: mmap reopen].
- **T13 — integração + bench** (dep T12): property test — sequência aleatória de puts/gets vs oracle `map` do teste, com reopen aleatório; teste de crash real: subprocesso + SIGKILL + reopen (`//go:build integration`); `BenchmarkPut`/`BenchmarkGetByKey` + `testing.AllocsPerRun` (meta: single-digit allocs/op); bench de heap-flatness 1M vs (opcional, `-short` skip) 8M rows.
- **T14 — stats de liveness** (dep T10, opcional no M1): contadores totalBytes/liveBytes/liveRecords por segmento no manifest (groundwork da Fase 10, sem compactor).
- **T15 — docs** (dep T12): `docs/architecture-storage.md` + `docs/diagrams/{storage_write_flow.puml,storage_recovery_flow.puml,c4_storage_component.puml}` (padrão seção 7 do CLAUDE.md global).

`compaction` do spec104 fica explicitamente fora do M1 (Fase 10); T14 prepara as stats.

## 14. Riscos e pontos de atenção

1. **Torn reads em mmap** — mitigado por publicação atômica word-aligned + location como único commit point + overlay. Manter TODO acesso a words via `mmap.Region` (asserts de alinhamento); rodar suite com `-race`.
2. **Writeback antecipado de páginas mmap vs fsync do segmento (power loss)** — é o risco mais sutil; o overlay de locations é a defesa. Não "otimizar" removendo o overlay sem revisar §10.
3. **SIGBUS por disco cheio** — usar `Fallocate` real em todo create/grow; nunca só ftruncate.
4. **Rehash da primary pausa ingestão** — pre-sizing por `EstimatedRows`; logar duração; fila de 5000 absorve; documentar métrica.
5. **CRC no tail: falso válido** — probabilidade ~2^-32 já filtrada por magic+bounds; sempre `ftruncate` no cut para não deixar lixo entre records após reuso do tail.
6. **fields.dict/values.dict sem fsync antes do record** — quebra replay; regra de ordem é invariante de código (teste dedicado).
7. **segmentSize > 1 GiB** — offset não cabe em 30 bits; validar na config com erro claro.
8. **Slices sobre mmap escapando após remap/munmap** — API pública só devolve dados via buffers de pread; unsafe confinado.
9. **ABA de rowId** — proibido reutilizar rowId (nextRowID monotônico no manifest, recuperado como max(manifest, tail)).
10. **msync no checkpoint pausa writer** — aceito no M1 (dezenas de ms); se benchmark M13 acusar, mover para coordinator com quiesce (fase futura).
11. **Go race detector não cobre ordering em memória não-heap** — usar sempre `sync/atomic` (nunca load/store simples) nos pontos de publicação, mesmo onde amd64 "perdoaria".

## 15A. Contrato acordado com plan-query (materializer/query) — deltas sobre o §12

Negociado em resposta aos 6 pontos do plan-query (plano dele: `/home/lucas/.claude/plans/estamos-em-greenfield-novo-noble-firefly-agent-aplan-query-27060c299b110a31.md`, §2.2/2.4/2.9/3.7/8). Convergência independente confirmada: D2 dele (NULL = ausência) e D3 (payload ordenado por fieldID) são idênticos ao meu design.

1. **Pacote `internal/types`** (leve, zero deps) passa a ser a casa dos tipos de dados compartilhados:
   - `FieldType uint8`, `FieldID uint16`, `RowID uint64`
   - `Value{Type FieldType; I64 int64; Bytes []byte}` — **adoto a versão dele, sem campo Float** (double via `math.Float64bits` em I64; bool 0/1 em I64; string/ips/prefixes/bytes/json em Bytes). Mais enxuta que a minha do §12.
   - `Record{RowID RowID; Sequence int64; Flags uint8; Key []byte; Payload []byte}` — substitui `MaterializedRecord` (ganha Key e Flags; FieldCount sai — o Store conta durante a validação do Put).
   - **`PhysicalLocation` NÃO entra no contrato público** — com o Put orquestrando (item 3), nem materializer nem query tocam locations; o tipo fica interno a `internal/storage`.
   - Interfaces da §99: declaradas nos consumidores (idiom Go), não em types. Não-bloqueante se ele preferir centralizar.
2. **Encoding**: dono confirmado `internal/storage/encoding`; formato exatamente como no §2 deste plano (== §2.4 do plano dele). API payload-level: `AppendField(dst []byte, id types.FieldID, v types.Value) []byte` + `FieldReader{Reset(payload); Next() (FieldID, Value, ok)}` zero-copy. Framing do record (header+CRC) é interno ao `Store.Put`. **Tag 0x00 (null) fica reservada e nunca é codificada**: o PatchBuffer dele carrega `isNull` como flag em struct (não encoda patch para wire), e estado materializado dropa o campo — `AppendField` com TypeNull = erro; `FieldReader` em 0x00 = erro. (Substitui a ideia anterior de null-tag em patches codificados.)
3. **Orquestração do write path é do Store (Option A — acordado como ponto 4 da negociação)**: `Apply` do materializer vira `GetByKey(prev) → stale-check via prev.Sequence → merge/replace → Put(key, seq, flags, payload, prev)`. `RowIDAllocator`, `StorageEngine.Append`, `SecondaryIndex.Update` e `LocationIndex.Set` saem do contrato público. Motivos: replay do recovery re-executa o mesmo diff de índices do Put (um único code path); invariantes do overlay de locations ficam num só pacote; R6 do plano dele (replace esquecendo postings) morre por construção.
   - `Put` confia em `prev.RowID` quando `prev != nil` (não re-faz Find — evita probe+pread duplicados); `prev == nil` com key existente → Store lê o prev sozinho (fallback de correção, 1 read extra).
   - `Put` retorna `(RowID, created bool, error)`; valida no mesmo passo do diff de bitmap: fieldIDs crescentes sem duplicata, ausência de tag 0x00, IDs conhecidos no dict, limites de tamanho; conta fieldCount.
4. **Assinaturas finais do Store** (revisa o §12):
   - `GetByKey(key, buf []byte) (types.Record, []byte, bool, error)` — a validação de colisão da primary (spec §36) já lê o record, então o prev vem "de graça"; `prev.Sequence` cobre o stale-check sem I/O extra.
   - `GetByRowID(id types.RowID, buf []byte) (types.Record, []byte, bool, error)` — ok=false para row sem location ou tombstoned = checagem de liveness dos caminhos indexados.
   - `Put(key []byte, seq int64, flags uint8, payload []byte, prev *types.Record) (types.RowID, bool, error)`
   - `ScanLive() RowIDIterator` — full scan: rowIds crescentes com location válida e sem tombstone (overlay incluso); TTL filtrará aqui na Fase 9.
   - `LookupBitmap(field types.FieldID, v types.Value) (RowIDIterator, error)` — por FieldID (não nome); iteração em ordem crescente de rowId garantida (pré-condição das interseções/uniões do planner dele).
   - `Sequence(rowID types.RowID) (int64, bool)` — sequences.idx confirmado no M1; recomendado usar prev.Sequence (um só code path), mas o índice existe (spec §38 + replay).
5. **Logística**: T0 (go.mod, layout, Makefile, golangci) é meu; plan-query não cria go.mod e adiciona `gopkg.in/yaml.v3` na primeira task dele após meu T0.

Confirmações finais (plan-query aceitou o item 3/Option A integralmente):

6. **Contrato de buffers**: `GetByKey(key, buf []byte) (types.Record, []byte, bool, error)` e `GetByRowID(rowID, buf)` idem — `Record.Key/Payload` são zero-copy sobre o buf devolvido (possivelmente crescido) como segundo valor de retorno; o caller reutiliza (prevBuf/rowBuf). Válido até a próxima chamada com o mesmo buf.
7. **Bit layout de Flags** (constantes em `internal/types`, fonte única): `FlagTombstone uint8 = 1 << 0`; bits 1-7 reservados (zero). No packed location: bit63 = tombstone, bit62 reservado. M1 sempre grava Flags=0.
8. **LookupHash reservado para Fase 8**: assinatura futura `LookupHash(field types.FieldID, v types.Value) (RowIDIterator, error)` — NÃO existe no *storage.Store do M1. Capability check do planner via type assertion consumer-side (`interface{ LookupHash(...) }`); o fake dele implementa, o Store real só na Fase 8.
9. **`internal/types` é criado no MEU T0/T1** (o encoding depende dele imediatamente): T0 cria o pacote com os tipos acordados, T1 testa (roundtrip Value etc.). plan-query não cria o pacote — consome.

Impacto nas tarefas: T0/T1 ganham a criação de `internal/types` (movendo Value/tipos para lá); T10 usa as assinaturas acima; demais inalteradas.

## 15. Desvios deliberados da spec (justificados)

1. **Key explícita no record** (+2 bytes de header, key duplicada): validação de colisão e replay sem schema; tombstones sem payload.
2. **locations.idx com 8 bytes/entry sem length** (spec sugeria 16 com length): resolve a publicação atômica com um único u64, corrige o esgotamento de segmentID e reduz o arquivo de 128 MB para 64 MB; length vem do header CRC-protegido do record.
3. **Overlay de locations em heap (bounded)**: não viola a filosofia de memória (limite = eventos por intervalo de checkpoint, não dataset) e fecha o buraco de durabilidade em power loss.
