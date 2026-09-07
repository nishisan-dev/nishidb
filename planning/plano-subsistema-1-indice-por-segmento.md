# Plano de implementação — Subsistema 1: índice por segmento

> **Para executores agênticos:** SUB-SKILL OBRIGATÓRIA — use
> `superpowers:subagent-driven-development` (recomendado) ou
> `superpowers:executing-plans` para implementar tarefa a tarefa. Os passos usam
> caixa (`- [ ]`) para rastreio.

**Objetivo:** substituir os índices secundários globais (hash e bitmap) por um
índice único por segmento, com postings agrupadas por valor, para que a inserção
seja O(1), a purga do segmento leve o índice junto e o índice pare de crescer.

**Arquitetura:** um motor novo em `internal/index/segindex`, com duas formas —
WIP mutável (só o segmento corrente) e selada imutável (todo segmento
aposentado). O `Store` passa a manter um WIP por campo do segmento corrente,
selar na rotação e apagar na aposentadoria. O `Lookup` compõe uma fonte por
segmento e as une; segmento sem índice responde enumerando as linhas dele, o que
é correto porque o contrato já é superconjunto com revalidação (§44).

**Stack:** Go 1.25+ (o CI pina 1.26.5), sem dependência nova. `mmap` e
`fallocate` reais (Linux, ext4/xfs/btrfs). `golangci-lint` v2.12.2.

**Spec:** [`planning/spec-subsistema-1-indice-por-segmento.md`](spec-subsistema-1-indice-por-segmento.md)
— o plano argumenta a partir dela; leia as duas.

## Restrições globais

Valem para **toda** tarefa deste plano.

- **Idioma:** PT-BR em tudo — identificadores, nomes de teste, comentários, logs,
  docs e mensagens de commit. Mensagens de commit na voz do time, sem menção a
  agente.
- **Build:** `make build` é a ÚNICA fonte do build. Nunca crie um segundo comando.
- **Suíte antes de commitar:** `make test` (com os gates de medida) e
  `make test-race` (gates viram SKIP). Nenhum substitui o outro.
- **Lint em ratchet BIDIRECIONAL:** `.golangci-baseline` está em **387**.
  Contagem acima **e abaixo** reprova. Toda tarefa que fecha apontamento abaixa
  o número no MESMO commit. Nunca escreva a contagem em comentário.
- **`internal/storage/mmap` é o único pacote autorizado a usar `mmap`/`unsafe`.**
  `segindex` consome `mmap.Region`; não abre mapeamento por conta própria.
- **Formato de disco tem golden de bytes.** Todo arquivo novo persistente nasce
  com um.
- **Zero alocação por linha** no caminho quente é propriedade gateada.
- **Ordem de locks:** `vida` → `wmu` → `aposMu`. Nada sob o `aposMu` toma `wmu`.
- **Três baldes de erro (§101):** dado ruim (pula e conta) ≠ falha de infra
  (para) ≠ writer falho (sticky).
- **Falso positivo é tolerado; falso NEGATIVO nunca.** Toda dúvida degrada para
  enumeração ou full scan.
- **Nada de fallback para legado.** Onde este plano mantém os dois motores vivos
  por algumas tarefas, isso é andaime de migração dentro da branch, e a Tarefa 9
  o remove. Não é estado entregável.
- **Docs no MESMO commit que o código.** `docs/architecture.md` só afirma número
  medido.
- Ao fazer grep no repositório, excluir `.claude/worktrees/`.

## Sequência e por que ela é essa

| Fase | Tarefas | Entregável |
|---|---|---|
| **A — o motor** | 1–5 | `internal/index/segindex` completo e testado, sem tocar em `storage` |
| **B — o storage** | 6–9 | o `Store` mantém, lê e reconstrói; o motor antigo é deletado |
| **C — a superfície** | 10–12 | `index: true`, planner unificado, débito 22 fechado |
| **D — a política** | 13–14 | a janela e a goroutine de reconstrução |
| **E — o fecho** | 15–17 | mutações, métricas, docs e aceite |

**A ordem não é negociável em três pontos.** A Fase A vem inteira antes da B,
porque um motor de formato de disco testado isoladamente é o que torna a troca
do `storage` uma tarefa de encanamento em vez de uma reescrita. A Tarefa 8 vem
antes da 9 porque é ela que roda a suíte inteira de `storage`/`query` — que não
muda de nome nem de asserção — contra o motor novo: é o oráculo diferencial do
plano. E a Fase D vem depois da C porque a janela e a reconstrução operam sobre
a superfície de configuração que a C estabelece.

## Estrutura de arquivos

**Criar — `internal/index/segindex/`:**

| Arquivo | Responsabilidade |
|---|---|
| `format.go` | magics, versões, offsets de header, largura de posting. O ÚNICO lugar que conhece bytes |
| `writer.go` | `Writer`: acumula postings e emite o `.sidx`. O ÚNICO lugar que decide o layout selado |
| `sealed.go` | `Selado`: abre, valida CRC preguiçosamente, busca binária, devolve fatia |
| `wip.go` | `WIP`: diretório + array encadeado, `Set` O(1), crescimento |
| `seal.go` | WIP → `Writer` → selado, com re-base |
| `union.go` | `Union`: concatenação sobre faixas disjuntas, merge k-way com dedup sobre sobrepostas |

**Criar — `internal/storage/`:**

| Arquivo | Responsabilidade |
|---|---|
| `indices.go` | posse dos índices pelo `Store`: abrir, manter no `applyRecord`, selar, apagar, `Lookup` |
| `indices_enum.go` | as duas fontes de enumeração (faixa do histórico, filtro do `locations.idx`) |
| `indices_rebuild.go` | a goroutine de reconstrução e a construção a partir dos records |
| `indices_window.go` | a janela: expulsão na passada de retenção e a janela efetiva |

**Remover integralmente:** `internal/index/hash/`, `internal/index/bitmap/`.

**Modificar:** `internal/storage/store.go` (posse, `applyRecord`, `Lookup`),
`internal/storage/compaction.go` (aposentadoria), `internal/storage/retention.go`
(janela), `internal/storage/checkpoint/manifest.go` (v3),
`internal/query/planner/planner.go` e `plan.go`,
`internal/query/executor/candidate.go` e `store.go`, `internal/schema/`,
`internal/config/`, `internal/metrics/names.go`,
`internal/testutil/fakestorage/`.

---

# Fase A — o motor, isolado de `storage`

## Tarefa 1: formato e escritor da forma selada

**Arquivos:**
- Criar: `internal/index/segindex/format.go`
- Criar: `internal/index/segindex/writer.go`
- Testar: `internal/index/segindex/writer_test.go`
- Testar: `internal/index/segindex/golden_test.go`
- Criar: `internal/index/segindex/testdata/golden.sidx`

**Interfaces:**
- Consome: `types.FieldID`, `types.RowID` (`internal/types`); `crc32.Castagnoli`.
- Produz:
  ```go
  const (
      SealedMagic   = "NDBX"
      SealedVersion  = 1
      SealedHeaderSize  = 64
      SectionEntrySize      = 24
      DirSlotSize    = 16
  )
  type Writer struct{ ... }
  func NewWriter() *Writer
  func (e *Writer) Add(campo types.FieldID, h uint64, row types.RowID)
  func (e *Writer) Vazio() bool
  func (e *Writer) WriteSealed(caminho string, seg uint32) error
  ```

O `Writer` é o **único lugar do repositório que decide o layout selado**. A
selagem (Tarefa 4) e a reconstrução (Tarefa 7) o alimentam por caminhos
diferentes, e é por isso que "incremental idêntico a reconstruído" é verdade por
construção — sobra para o golden provar que a ALIMENTAÇÃO é equivalente, que é
o risco real.

- [ ] **Passo 1: escrever `format.go` com o layout e o porquê**

```go
// Package segindex implementa o índice secundário POR SEGMENTO do NishiDB:
// valor -> conjunto ordenado de rowIds dentro de um segmento.
//
// Ele substitui os dois motores globais (hash e bitmap) por um só, e a
// unificação não é economia de código: POR SEGMENTO a diferença entre os dois
// some. Os dois são "valor -> conjunto ordenado de rowIds", e densidade vira
// decisão de codificação, não de motor.
//
// # Duas formas
//
// A forma WIP (wip.go) é mutável e existe para o segmento CORRENTE apenas:
// diretório open-addressing sobre valores distintos, mais um array de postings
// encadeado para trás. Inserir é uma sondagem amortizada e duas escritas —
// O(1), que é o defeito que este pacote existe para corrigir.
//
// A forma selada (este arquivo, writer.go e sealed.go) é IMUTÁVEL e cobre
// todo segmento já rotacionado: diretório ordenado com busca binária e postings
// contíguas por valor. Imutável compra três coisas que a WIP não tem: não há
// load factor a respeitar (o diretório tem exatamente D entradas), o Lookup
// devolve uma fatia do mmap sem copiar nada, e o RWMutex do mmap.Region deixa
// de importar — ele existe para proteger contra o remap do Grow, e um arquivo
// selado nunca cresce.
package segindex

// Layout da forma SELADA, little-endian (arquivo data/segment-%08d.sidx):
//
//	0   4  magic "NDBX"
//	4   1  version = 1
//	5   1  flags (reservado, 0)
//	6   2  secCount u16
//	8   8  segmentID u64
//	16  8  baseRowID u64   — o MENOR rowId do segmento; base dos deltas
//	24  8  maxRowID u64
//	32  1  postingWidth u8 — 4 ou 8
//	33  31 reservado
//	64  .. tabela de seções: secCount x 24 bytes, ORDENADA por fieldID
//	          fieldID u16 | pad u16 | distintos u32 | dirOff u64 | postOff u64
//	    .. por seção, na ordem da tabela:
//	          diretório: distintos x 16 bytes, ORDENADO por hash
//	              hash u64 | postIdx u32 | count u32
//	          postings: array de deltas de postingWidth bytes, contíguo por
//	              valor e ascendente dentro do grupo
//	EOF-4  crc32c u32 sobre [0, EOF-4)
//
// O crc fica no FIM e cobre tudo antes dele, que é o idioma que o manifest já
// usa (checkpoint/manifest.go). Cobrir o header inclusive é o ponto: um header
// corrompido que passasse pelo crc levaria a busca binária a offsets
// arbitrários dentro do mapeamento.
//
// NÃO existe campo minRowID: baseRowID É o menor rowId da seção. A mesma
// informação em dois lugares é uma chance de divergirem, e quem divergisse
// produziria delta negativo — que em u32 vira um rowId gigante e um falso
// NEGATIVO silencioso, o único desfecho que este produto não tolera.
const (
	// SealedMagic identifica o arquivo selado.
	SealedMagic = "NDBX"
	// SealedVersion é a versão do formato selado.
	SealedVersion = 1
	// SealedHeaderSize é o tamanho do header em bytes.
	SealedHeaderSize = 64
	// SectionEntrySize é o tamanho de uma entrada da tabela de seções.
	SectionEntrySize = 24
	// DirSlotSize é o tamanho de uma entrada de diretório.
	DirSlotSize = 16
	// CRCSize é o crc32c no fim do arquivo.
	CRCSize = 4
)

var (
	// ErrFormat é a família de erros de arquivo de índice ilegível.
	ErrFormat = errors.New("segindex: formato inválido")
	// ErrCRC marca divergência de checksum. É SEPARADO do ErrFormat porque o
	// chamador reage diferente: formato inválido é bug ou versão, e para;
	// crc divergente é corrupção de mídia, e o remédio é apagar e reconstruir.
	ErrCRC = errors.New("segindex: crc divergente")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// SealedPath devolve o caminho do índice selado do segmento dentro de dir.
// Ele mora ao lado do .dat de propósito: a aposentadoria remove os dois no
// mesmo passo, e é isso que faz "purgar o segmento leva o índice junto" ser
// uma linha de código em vez de uma promessa.
func SealedPath(dir string, seg uint32) string {
	return filepath.Join(dir, fmt.Sprintf("segment-%08d.sidx", seg))
}

// widthFor devolve a menor largura de posting que cobre o intervalo.
func widthFor(span uint64) uint8 {
	if span < 1<<32 {
		return 4
	}
	return 8
}
```

- [ ] **Passo 2: escrever o teste que falha — agrupamento, ordenação e dedup**

Crie `internal/index/segindex/writer_test.go`:

```go
package segindex_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/index/segindex"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// TestOEscritorAgrupaOrdenaEDeduplicaAsPostings fixa as três propriedades que
// tornam o Lookup selado uma fatia contígua: as postings de um valor ficam
// juntas, ascendentes e sem repetição. A entrada é dada FORA DE ORDEM e com
// duplicata de propósito — as duas coisas acontecem no dataset materializado,
// onde um update traz um rowId arbitrário e a mesma linha pode receber dois
// records com o mesmo valor dentro de um segmento.
func TestOEscritorAgrupaOrdenaEDeduplicaAsPostings(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NewWriter()
	// campo 7, valor A (hash 100): rows 30, 10, 30 (duplicata), 20
	e.Add(7, 100, 30)
	e.Add(7, 100, 10)
	e.Add(7, 100, 30)
	e.Add(7, 100, 20)
	// campo 7, valor B (hash 50): row 15  -- hash MENOR, adicionado depois
	e.Add(7, 50, 15)
	// campo 3 (fieldID MENOR, adicionado por último)
	e.Add(3, 999, 12)

	caminho := segindex.SealedPath(dir, 42)
	if err := e.WriteSealed(caminho, 42); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}

	s, err := segindex.OpenSealed(caminho)
	if err != nil {
		t.Fatalf("OpenSealed: %v", err)
	}
	defer s.Fecha()

	if got, want := s.Campos(), []types.FieldID{3, 7}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Campos() = %v, quero %v (ordenados por fieldID)", got, want)
	}
	if got, want := s.MinRowID(), types.RowID(10); got != want {
		t.Errorf("MinRowID() = %d, quero %d", got, want)
	}
	if got, want := s.MaxRowID(), types.RowID(30); got != want {
		t.Errorf("MaxRowID() = %d, quero %d", got, want)
	}

	it, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup(7, 100) não encontrou o valor")
	}
	var rows []types.RowID
	for it.Next() {
		rows = append(rows, it.RowID())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterador: %v", err)
	}
	want := []types.RowID{10, 20, 30}
	if len(rows) != len(want) {
		t.Fatalf("Lookup(7,100) = %v, quero %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("Lookup(7,100) = %v, quero %v", rows, want)
		}
	}
}

// TestOEscritorRebaseiaParaAMenorLarguraQueCabe prova a promessa de disco da
// spec: o WIP guarda rowId absoluto em 8 bytes por precaução, e a selagem
// re-baseia para 4 porque a incerteza já acabou. Sem isto a conta de
// "16 x distintos + 4 x linhas" seria mentira.
func TestOEscritorRebaseiaParaAMenorLarguraQueCabe(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NewWriter()
	// rowIds ALTOS, mas com intervalo pequeno: o delta cabe em u32 mesmo com
	// o rowId absoluto passando de 2^32. É exatamente o caso do histórico com
	// meses no ar, e é o caso em que uma implementação que ignorasse o base
	// escolheria 8 bytes sem precisar.
	base := types.RowID(1) << 35
	e.Add(7, 100, base)
	e.Add(7, 100, base+1000)

	caminho := segindex.SealedPath(dir, 1)
	if err := e.WriteSealed(caminho, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	b, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if got := b[32]; got != 4 {
		t.Fatalf("postingWidth = %d, quero 4 (o intervalo é 1000, cabe em u32)", got)
	}
}

// TestOEscritorVazioNaoCriaArquivo: um segmento sem nenhuma linha indexada não
// tem índice, e "arquivo ausente" é o estado que o Lookup já sabe tratar por
// enumeração. Criar um .sidx vazio faria a fila de reconstrução considerá-lo
// pronto e o segmento nunca ganharia índice de verdade.
func TestOEscritorVazioNaoCriaArquivo(t *testing.T) {
	dir := t.TempDir()
	e := segindex.NewWriter()
	if !e.Vazio() {
		t.Fatal("escritor recém-criado deveria ser Vazio()")
	}
	caminho := segindex.SealedPath(dir, 1)
	if err := e.WriteSealed(caminho, 1); err != nil {
		t.Fatalf("WriteSealed: %v", err)
	}
	if _, err := os.Stat(caminho); !os.IsNotExist(err) {
		t.Fatalf("Stat(%s) = %v, quero IsNotExist", filepath.Base(caminho), err)
	}
}
```

- [ ] **Passo 3: rodar e confirmar que falha por compilação**

```bash
go test ./internal/index/segindex/ -run 'TestOEscritor' -count=1
```

Esperado: falha de build — `undefined: segindex.NewWriter`. É a falha
correta: o teste nomeia a API antes de ela existir.

- [ ] **Passo 4: implementar `writer.go`**

```go
package segindex

// entrada é uma posting ainda não ordenada. 24 bytes, e a ordem dos campos é
// para o alinhamento — 224 mil linhas por segmento vezes o número de campos
// indexados vivem aqui durante a selagem.
type entrada struct {
	hash  uint64
	row   types.RowID
	campo types.FieldID
}

// Writer acumula postings e emite o arquivo selado. É o ÚNICO lugar que
// decide o layout selado: a selagem do WIP e a reconstrução a partir dos
// records alimentam ESTE tipo, e é isso que torna "incremental idêntico a
// reconstruído" verdade por construção em vez de por vigilância.
//
// Não é seguro para uso concorrente. Cada selagem ou reconstrução usa o seu.
type Writer struct {
	ents []entrada
}

// NewWriter devolve um escritor vazio.
func NewWriter() *Writer { return &Writer{} }

// Add registra a posting. Duplicatas e desordem são aceitas: o WriteSealed
// ordena e deduplica.
func (e *Writer) Add(campo types.FieldID, h uint64, row types.RowID) {
	e.ents = append(e.ents, entrada{hash: h, row: row, campo: campo})
}

// Vazio reporta que nada foi adicionado.
func (e *Writer) Vazio() bool { return len(e.ents) == 0 }

// WriteSealed emite o arquivo em caminho, por .tmp + fsync + rename + fsync do
// diretório. Writer vazio NÃO cria arquivo: "sem índice" é um estado que o
// Lookup trata por enumeração, e um arquivo vazio faria a reconstrução
// considerar o segmento pronto.
func (e *Writer) WriteSealed(caminho string, seg uint32) error {
	if e.Vazio() {
		return nil
	}
	slices.SortFunc(e.ents, func(a, b entrada) int {
		if c := cmp.Compare(a.campo, b.campo); c != 0 {
			return c
		}
		if c := cmp.Compare(a.hash, b.hash); c != 0 {
			return c
		}
		return cmp.Compare(a.row, b.row)
	})
	e.ents = slices.Compact(e.ents)

	base, maxRow := e.ents[0].row, e.ents[0].row
	for _, en := range e.ents {
		base = min(base, en.row)
		maxRow = max(maxRow, en.row)
	}
	largura := widthFor(uint64(maxRow - base))

	corpo := e.montaCorpo(base, largura)

	buf := make([]byte, 0, SealedHeaderSize+len(corpo)+CRCSize)
	buf = append(buf, SealedMagic...)
	buf = append(buf, SealedVersion, 0)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(e.contaCampos()))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(seg))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(base))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(maxRow))
	buf = append(buf, largura)
	buf = append(buf, make([]byte, SealedHeaderSize-len(buf))...)
	buf = append(buf, corpo...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))

	return writeAtomic(caminho, buf)
}
```

`buildBody` percorre `e.ents` (já ordenado) montando, em duas passadas: a
primeira conta campos e valores distintos por campo para dimensionar a tabela
de seções e os diretórios; a segunda escreve. `contaCampos` conta fieldIDs
distintos. `writeAtomic` faz `os.WriteFile(caminho+".tmp")`, `Sync`,
`os.Rename`, e `fsync` do diretório — sem isto o índice pode sobreviver ao
crash com o nome temporário e o `Open` o trataria como órfão.

- [ ] **Passo 5: rodar até passar**

```bash
go test ./internal/index/segindex/ -run 'TestOEscritor' -count=1
```
Esperado: PASS nos três. (`OpenSealed` vem na Tarefa 2 — até lá, o primeiro
teste continua vermelho por `undefined: OpenSealed`. **Comente-o com
`t.Skip("OpenSealed chega na Tarefa 2")` e remova o skip na Tarefa 2**; os
outros dois passam agora e não dependem do leitor.)

- [ ] **Passo 6: golden de bytes**

`golden_test.go` alimenta o escritor com um corpus fixo (um valor de cada
largura de posting, dois campos, um valor com uma posting só e um com muitas),
escreve, e compara com `testdata/golden.sidx` byte a byte. Regenerar exige a
variável `NISHIDB_REGRAVA_GOLDEN=1`, e o teste FALHA quando ela está ligada —
para que regravar seja um ato deliberado e nunca aconteça no CI.

```bash
NISHIDB_REGRAVA_GOLDEN=1 go test ./internal/index/segindex/ -run TestGoldenDoSelado -count=1   # gera e falha
go test ./internal/index/segindex/ -run TestGoldenDoSelado -count=1                            # confere e passa
```

- [ ] **Passo 7: commitar**

```bash
git add internal/index/segindex/
git commit -m "Cria o escritor do índice selado por segmento

O escritor é o único lugar que decide o layout do arquivo selado. A selagem
do WIP e a reconstrução a partir dos records vão alimentá-lo por caminhos
diferentes, e é isso que torna 'incremental idêntico a reconstruído'
verdade por construção — sobra para o golden provar que a alimentação é
equivalente, que é o risco de verdade.

O re-base para a menor largura acontece aqui porque é aqui que o mínimo e o
máximo do segmento são conhecidos. Sem ele a conta de disco da spec seria
mentira: o WIP guarda rowId absoluto por precaução, e a precaução deixa de
ser necessária na selagem."
```

---

## Tarefa 2: leitura da forma selada, sem lock e sem cópia

**Arquivos:**
- Modificar: `internal/storage/mmap/region.go`
- Testar: `internal/storage/mmap/region_test.go`
- Criar: `internal/index/segindex/sealed.go`
- Testar: `internal/index/segindex/sealed_test.go`
- Testar: `internal/index/segindex/alloc_gate_test.go`

**Interfaces:**
- Consome: `Writer.WriteSealed` (Tarefa 1); `mmap.Region`.
- Produz:
  ```go
  // internal/storage/mmap
  func OpenReadOnly(caminho string) (*Region, error)
  func (r *Region) Bytes() []byte   // pânico se a região não for somente-leitura

  // internal/index/segindex
  type Selado struct{ ... }
  func OpenSealed(caminho string) (*Selado, error)
  func (s *Selado) Campos() []types.FieldID
  func (s *Selado) HasField(campo types.FieldID) bool
  func (s *Selado) Lookup(campo types.FieldID, h uint64) (Fonte, bool)
  func (s *Selado) MinRowID() types.RowID
  func (s *Selado) MaxRowID() types.RowID
  func (s *Selado) SegmentID() uint32
  func (s *Selado) Fecha() error

  type Fonte interface {
      types.RowIDIterator
      MinRowID() types.RowID
      MaxRowID() types.RowID
  }
  ```

`Fonte` é declarada aqui porque o `Selado` é o primeiro a produzi-la. Contrato:
**`MinRowID`/`MaxRowID` limitam os rowIds que a fonte VAI emitir, e uma fonte
que não vai emitir nada reporta `Min > Max`.** É o que permite ao `Union`
(Tarefa 5) descartar as vazias antes de decidir sobre disjunção, sem consumir
nenhuma.

- [ ] **Passo 1: teste do mapeamento somente-leitura sem descritor retido**

Em `internal/storage/mmap/region_test.go`:

```go
// TestAbreSomenteLeituraNaoRetemDescritor é a razão de esta função existir:
// um dataset de 250 GiB tem 500 segmentos, e um descritor por segmento
// estouraria o ulimit -n típico de 1024. No Linux o mapeamento sobrevive ao
// close do fd, e um arquivo selado nunca precisa do fd de volta — não cresce,
// não faz msync e não faz punch-hole.
func TestAbreSomenteLeituraNaoRetemDescritor(t *testing.T) {
	caminho := filepath.Join(t.TempDir(), "r.bin")
	if err := os.WriteFile(caminho, []byte("abcdefgh"), 0o644); err != nil {
		t.Fatal(err)
	}
	antes := countOpenDescriptors(t)
	r, err := mmap.OpenReadOnly(caminho)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if depois := countOpenDescriptors(t); depois != antes {
		t.Errorf("descritores abertos: %d antes, %d depois — quero iguais", antes, depois)
	}
	if got := string(r.Bytes()); got != "abcdefgh" {
		t.Errorf("Bytes() = %q, quero %q — o mapeamento tem de sobreviver ao close", got, "abcdefgh")
	}
}

// countOpenDescriptors conta as entradas de /proc/self/fd.
func countOpenDescriptors(t *testing.T) int {
	t.Helper()
	e, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("sem /proc/self/fd: %v", err)
	}
	return len(e)
}

// TestRegiaoDeEscritaRecusaBytes trava a fronteira: entregar o slice cru de uma
// região que o Grow pode remapear devolveria memória liberada ao chamador
// depois do primeiro crescimento. Este é o tipo de erro que só aparece em
// produção, e o pânico o transforma em falha de teste.
func TestRegiaoDeEscritaRecusaBytes(t *testing.T) {
	r, err := mmap.Open(filepath.Join(t.TempDir(), "w.bin"), 64)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer func() {
		if recover() == nil {
			t.Error("Bytes() numa região de escrita deveria entrar em pânico")
		}
	}()
	_ = r.Bytes()
}
```

- [ ] **Passo 2: rodar e ver falhar**

```bash
go test ./internal/storage/mmap/ -run 'TestAbreSomenteLeitura|TestRegiaoDeEscritaRecusaBytes' -count=1
```
Esperado: `undefined: mmap.OpenReadOnly`.

- [ ] **Passo 3: implementar em `region.go`**

`OpenReadOnly` abre com `O_RDONLY`, faz `unix.Mmap(fd, 0, tamanho,
unix.PROT_READ, unix.MAP_SHARED)`, **fecha o descritor** e devolve a `Region`
com `readOnly = true` e `f = nil`. `Grow`, `Msync`, `AtomicStore64`,
`AtomicOr64`, `AtomicAnd64` e `LiberaAte` passam a devolver erro (ou entrar em
pânico, nos que não devolvem erro) quando `readOnly`. `Close` só faz
`munmap`. `Bytes()` devolve `r.data` e entra em pânico se não for
somente-leitura — o comentário registra que devolver o slice cru de uma região
crescível entregaria memória liberada depois do primeiro `Grow`.

- [ ] **Passo 4: rodar até passar; rodar a suíte do pacote**

```bash
go test ./internal/storage/mmap/ -count=1
go test -race ./internal/storage/mmap/ -count=1
```

- [ ] **Passo 5: escrever `sealed.go` e ligar o primeiro teste da Tarefa 1**

`OpenSealed` faz `OpenReadOnly`, valida magic e versão, lê o header e a
tabela de seções para um slice em heap (`secCount` entradas — dezenas, não
milhares), e **não verifica o CRC**. A verificação é preguiçosa, na primeira
consulta, por `sync.Once`: conferir 2,25 GB na subida custaria segundos por
nada.

```go
// Lookup devolve a fonte das postings do hash no campo.
//
// ok=false significa que este segmento não responde por este campo — ou o
// campo não é indexado, ou a seção ainda não foi construída porque ele foi
// acrescentado ao schema depois. NÃO significa "valor ausente": valor ausente
// devolve ok=true com uma fonte vazia. A distinção decide se o chamador
// enumera o segmento (§3.2 da spec) ou aceita zero linhas dele.
func (s *Selado) Lookup(campo types.FieldID, h uint64) (Fonte, bool) {
	if err := s.confereCRC(); err != nil {
		return nil, false
	}
	sec, ok := s.secao(campo)   // busca binária na tabela de seções
	if !ok {
		return nil, false
	}
	idx, count, achou := s.buscaNoDiretorio(sec, h)
	if !achou {
		return emptySource{}, true
	}
	return &sealedIter{s: s, sec: sec, prox: idx, fim: idx + count}, true
}
```

`sealedIter.Next` avança `prox` e lê o delta em `s.reg.Bytes()` com
`binary.LittleEndian` — **sem lock e sem atômico**, porque o arquivo é
imutável. `RowID()` soma o `baseRowID`.

Remova o `t.Skip` de `TestOEscritorAgrupaOrdenaEDeduplicaAsPostings`.

- [ ] **Passo 6: teste de corrupção**

```go
// TestCRCDivergenteNaoDerrubaAConsulta: índice é dado derivado, e a doutrina do
// planner ("toda dúvida degrada") vale aqui. O Lookup devolve ok=false, que o
// chamador lê como "este segmento não responde por este campo" e resolve por
// enumeração — resposta correta, só lenta. Derrubar a consulta trocaria
// corrupção de um arquivo reconstruível por indisponibilidade.
func TestCRCDivergenteNaoDerrubaAConsulta(t *testing.T) {
	caminho := writeTestSealed(t)   // helper: 1 campo, 1 valor, 3 rows
	b, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xFF // corrompe o crc
	if err := os.WriteFile(caminho, b, 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := segindex.OpenSealed(caminho)
	if err != nil {
		t.Fatalf("OpenSealed não deve falhar por crc: %v", err)
	}
	defer s.Fecha()
	if _, ok := s.Lookup(7, 100); ok {
		t.Error("Lookup sobre arquivo com crc divergente devolveu ok=true")
	}
	if !errors.Is(s.IntegrityErr(), segindex.ErrCRC) {
		t.Errorf("IntegrityErr() = %v, quero ErrCRC — o chamador precisa saber que é corrupção, e não campo ausente", s.IntegrityErr())
	}
}
```

`IntegrityErr()` existe porque `ok=false` sozinho é ambíguo entre "campo
ausente" e "arquivo corrompido", e só o segundo dispara reconstrução, ERROR e
métrica.

- [ ] **Passo 7: o gate de alocação — o primeiro do caminho de leitura**

```go
//go:build !race

// TestIterarSeladoNaoAlocaPorLinha: o resultado é fatia contígua do mmap, e
// alocação POR LINHA é sinal de cópia indevida. A medida é da ITERAÇÃO, não do
// Lookup: montar a fonte custa um boxing de interface por segmento, que é
// custo de abertura e não de varredura — medi-los juntos esconderia justamente
// a regressão que este gate existe para pegar.
func TestIterarSeladoNaoAlocaPorLinha(t *testing.T) {
	s := openSealedWithNPostings(t, 1000)
	defer s.Fecha()
	it, ok := s.Lookup(7, 100)
	if !ok {
		t.Fatal("Lookup não encontrou")
	}
	media := testing.AllocsPerRun(100, func() {
		for it.Next() {
			_ = it.RowID()
		}
		it.(interface{ Rewind() }).Rewind()
	})
	if media != 0 {
		t.Errorf("AllocsPerRun = %v, quero 0", media)
	}
}
```

`Rewind()` é método só do `sealedIter`, e existe para o gate poder repetir a
varredura sem reabrir. A contraparte sob `-race` vive em
`alloc_gate_race_test.go` com `//go:build race` e um `t.Skip` explícito, no
molde de `internal/storage/rowindex/schedule_gate_race_test.go` — gate que some
em silêncio sob `-race` é carimbo (débito 53).

- [ ] **Passo 8: rodar tudo e commitar**

```bash
go test ./internal/index/segindex/ ./internal/storage/mmap/ -count=1
go test -race ./internal/index/segindex/ ./internal/storage/mmap/ -count=1
git add internal/index/segindex/ internal/storage/mmap/
git commit -m "Lê o índice selado sem lock, sem cópia e sem descritor retido

O arquivo selado é imutável, então três coisas que o motor antigo pagava
deixam de existir: não há load factor a respeitar na busca, o resultado é
uma fatia do mapeamento em vez de um slice materializado e ordenado, e o
RWMutex do mmap.Region sai do caminho — ele existe para proteger contra o
remap do Grow, e o selado nunca cresce.

O mapeamento somente-leitura fecha o descritor logo após o mmap. Com 500
segmentos por dataset, um descritor por segmento estouraria o ulimit -n
típico; no Linux o mapeamento sobrevive ao close e o selado nunca precisa
do descritor de volta.

CRC divergente devolve campo-ausente em vez de derrubar a consulta: índice
é dado derivado, o segmento passa a responder por enumeração e a
reconstrução o refaz."
```

---

## Tarefa 3: a forma WIP — o `Set` O(1)

**Arquivos:**
- Criar: `internal/index/segindex/wip.go`
- Testar: `internal/index/segindex/wip_test.go`
- Testar: `internal/index/segindex/cost_gate_test.go`
- Testar: `internal/index/segindex/custo_gate_race_test.go`
- Criar: `internal/index/segindex/export_test.go`

**Interfaces:**
- Consome: `mmap.Open` / `Region.Grow` / `AtomicLoad64` / `AtomicStore64`.
- Produz:
  ```go
  type WIP struct{ ... }
  func OpenWIP(dir string, seg uint32, campo types.FieldID, slotsMin uint32) (*WIP, error)
  func (w *WIP) Set(h uint64, row types.RowID) error
  func (w *WIP) Lookup(h uint64) []types.RowID
  func (w *WIP) Campo() types.FieldID
  func (w *WIP) Postings() uint32
  func (w *WIP) DistinctValues() uint32
  func (w *WIP) Fecha() error
  func (w *WIP) Remove() error
  func WIPPath(dir string, seg uint32, campo types.FieldID) string
  func ListWIP(dir string, seg uint32) ([]types.FieldID, error)
  ```
  E, em `export_test.go`: `func (w *WIP) LastSetAccesses() int`.

- [ ] **Passo 1: o gate de custo, escrito ANTES da implementação**

Este é o gate que define a tarefa. Ele é **determinístico** — conta sondas, não
nanossegundos — então roda sempre, inclusive sob `-race`.

```go
// TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento é o gate que este pacote
// inteiro existe para satisfazer. O motor antigo varria a cadeia de probe
// inteira a cada Set para não duplicar, então inserir a N-ésima posting de um
// valor custava N sondas e inserir n postings custava O(n²). No piloto isso deu
// 1,5 ms por Set e 82% do CPU do processo.
//
// A medida é de SONDAS e não de tempo: sem relógio não há ruído de runner, e
// o gate afirma a FORMA do algoritmo em vez de um orçamento que precisa ser
// recalibrado a cada máquina.
//
// Todas as postings são do MESMO valor de propósito — é o caso que derrubou o
// piloto (node: ~3.724 postings por valor).
func TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento(t *testing.T) {
	const tetoDeSondas = 4 // amortizado; o diretório é dimensionado por DISTINTOS
	for _, n := range []int{10_000, 100_000, 1_000_000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			w := openTestWIP(t)
			defer w.Fecha()
			var pior int
			for i := range n {
				if err := w.Set(100, types.RowID(i)); err != nil {
					t.Fatalf("Set #%d: %v", i, err)
				}
				pior = max(pior, w.LastSetAccesses())
			}
			if pior > tetoDeSondas {
				t.Errorf("pior caso de %d sondas com n=%d, teto %d — a cadeia voltou a crescer com o segmento",
					pior, n, tetoDeSondas)
			}
		})
	}
}
```

- [ ] **Passo 2: o gate de alocação do `Set`**

```go
//go:build !race

// TestSetNoWIPNaoAloca: é o caminho quente da ingestão, chamado uma vez por
// campo indexado por evento. internal/storage/ não tinha nenhum gate de
// alocação até aqui; este é o primeiro da família.
func TestSetNoWIPNaoAloca(t *testing.T) {
	w := openTestWIP(t)
	defer w.Fecha()
	var i int
	media := testing.AllocsPerRun(1000, func() {
		i++
		_ = w.Set(uint64(i%997)+2, types.RowID(i))
	})
	if media != 0 {
		t.Errorf("AllocsPerRun(Set) = %v, quero 0", media)
	}
}

// TestLookupNoWIPAlocaExatamenteUmaVez. A igualdade é deliberada: 0 significa
// que não materializou (e a ordem sai errada, porque a cadeia é percorrida ao
// contrário); 2 significa que o slice cresceu (e o tamanho exato veio da
// contagem errada). Os dois desvios são bugs diferentes, e um teto de "<= 1"
// esconderia o primeiro.
func TestLookupNoWIPAlocaExatamenteUmaVez(t *testing.T) {
	w := openTestWIP(t)
	defer w.Fecha()
	for i := range 100 {
		if err := w.Set(100, types.RowID(i)); err != nil {
			t.Fatal(err)
		}
	}
	media := testing.AllocsPerRun(1000, func() { _ = w.Lookup(100) })
	if media != 1 {
		t.Errorf("AllocsPerRun(Lookup) = %v, quero exatamente 1", media)
	}
}
```

- [ ] **Passo 3: testes de comportamento**

```go
// TestOWIPDevolveOrdemEstritamenteCrescenteComRowIdsForaDeOrdem cobre o
// dataset materializado, onde um update traz para o segmento corrente um rowId
// ARBITRÁRIO — a cadeia percorrida ao contrário NÃO sai ascendente ali. E a
// mesma linha pode receber dois records com o mesmo valor dentro de um
// segmento, o que exige compactar: o contrato do iterador é ESTRITAMENTE
// crescente, e um id repetido faria o unionIter do executor emitir a mesma
// linha duas vezes num OR.
func TestOWIPDevolveOrdemEstritamenteCrescenteComRowIdsForaDeOrdem(t *testing.T) {
	w := openTestWIP(t)
	defer w.Fecha()
	for _, r := range []types.RowID{50, 10, 30, 10, 40} {
		if err := w.Set(100, r); err != nil {
			t.Fatal(err)
		}
	}
	got := w.Lookup(100)
	want := []types.RowID{10, 30, 40, 50}
	if !slices.Equal(got, want) {
		t.Fatalf("Lookup = %v, quero %v", got, want)
	}
}

// TestOWIPCresceSemPerderPosting: o array de postings dobra e o diretório
// rehasheia, e nenhum dos dois pode perder uma posting. O rehash reescreve SÓ o
// diretório — as postings não se movem, porque o índice 1-based delas é o que
// a cadeia guarda.
func TestOWIPCresceSemPerderPosting(t *testing.T) {
	w := openTestWIPWithSlots(t, 16) // força vários rehashes
	defer w.Fecha()
	const n = 5000
	for i := range n {
		if err := w.Set(uint64(i)+2, types.RowID(i)); err != nil { // n valores DISTINTOS
			t.Fatal(err)
		}
	}
	for i := range n {
		got := w.Lookup(uint64(i) + 2)
		if len(got) != 1 || got[0] != types.RowID(i) {
			t.Fatalf("Lookup(%d) = %v, quero [%d] — o crescimento perdeu a posting", i+2, got, i)
		}
	}
	if got := w.Postings(); got != n {
		t.Errorf("Postings() = %d, quero %d", got, n)
	}
}
```

- [ ] **Passo 4: rodar e ver falhar**

```bash
go test ./internal/index/segindex/ -run 'WIP|Set' -count=1
```
Esperado: `undefined: segindex.OpenWIP`.

- [ ] **Passo 5: implementar `wip.go`**

```go
// Layout da forma WIP, little-endian (data/segment-%08d-%05d.widx):
//
//	0   4  magic "NDBW"
//	4   1  version = 1
//	5   1  reservado
//	6   2  fieldID u16
//	8   4  dirSlots u32 (potência de 2)
//	12  4  postCount u32
//	16  48 reservado
//	64  .. diretório: dirSlots x 16 bytes
//	          hash u64 | estado u64, com estado = (contagem << 32) | ultimo
//	.. .. postings: postCount x 16 bytes
//	          rowID u64 | anterior u64
//
// ultimo e anterior são índices 1-BASED no array de postings; 0 é "não há".
// hash == 0 marca slot vazio, e isso é seguro porque primary.HashKey nunca
// devolve 0 nem 1 (ela remapeia os dois valores reservados).
//
// O rowId é ABSOLUTO e a posting tem 16 bytes. Não há base nem largura
// variável aqui, e a razão é o dataset materializado: um update traz para o
// segmento corrente uma linha de rowId arbitrário, possivelmente MENOR que a
// do primeiro record dele, então não existe base conhecida no momento em que
// o arquivo é criado. Os 16 bytes também mantêm tudo alinhado em 8, que é o
// que o mmap.Region exige das cargas atômicas. O desperdício é de UM segmento
// (3,6 MB para 224 mil linhas) e a selagem re-baseia para 4 bytes.

// Set liga a posting (h, row). O(1): uma sondagem amortizada de diretório —
// cuja carga é o número de valores DISTINTOS do segmento, não o de postings —
// mais duas escritas.
//
// Exclusivo do writer único.
func (w *WIP) Set(h uint64, row types.RowID) error {
	if err := w.talvezCresce(); err != nil {
		return err
	}
	slot := w.sondaOuCria(h) // conta as sondas em w.sondas
	idx := w.postCount       // 0-based; publicado como idx+1
	off := w.postOff(idx)
	estado := w.reg.AtomicLoad64(w.slotOff(slot) + 8)

	// A posting é gravada ANTES de o estado ser publicado. Um leitor que
	// enxerga a cabeça nova necessariamente enxerga o rowId e o elo dela.
	w.reg.AtomicStore64(off, uint64(row))
	w.reg.AtomicStore64(off+8, uint64(uint32(estado))) // anterior = ultimo atual

	// Contagem e cabeça viajam na MESMA palavra: publicá-las em duas escritas
	// deixaria um leitor ver a cabeça nova com a contagem velha, e ele
	// dimensionaria a fatia com um elemento a menos que a caminhada produz.
	novo := (uint64(uint32(estado>>32)+1) << 32) | uint64(idx+1)
	w.reg.AtomicStore64(w.slotOff(slot)+8, novo)

	w.postCount++
	return nil
}

// Lookup devolve os rowIds do hash em ordem ESTRITAMENTE crescente.
//
// UMA alocação, de tamanho exato, vinda da contagem publicada junto da cabeça.
// A caminhada é de trás para frente e o preenchimento também, o que já entrega
// ordem crescente no histórico — lá o rowId é monotônico. No materializado não
// é, e por isso o IsSorted: ele é O(n) sem alocação e passa sempre no
// histórico, então o caso quente não paga pelo caso geral.
func (w *WIP) Lookup(h uint64) []types.RowID {
	slot, ok := w.busca(h)
	if !ok {
		return nil
	}
	estado := w.reg.AtomicLoad64(w.slotOff(slot) + 8)
	n, cur := uint32(estado>>32), uint32(estado)
	if n == 0 {
		return nil
	}
	out := make([]types.RowID, n)
	for i := int(n) - 1; i >= 0 && cur != 0; i-- {
		off := w.postOff(cur - 1)
		out[i] = types.RowID(w.reg.AtomicLoad64(off))
		cur = uint32(w.reg.AtomicLoad64(off + 8))
	}
	if !slices.IsSorted(out) {
		slices.Sort(out)
	}
	return slices.Compact(out)
}
```

`maybeGrow` dobra o array de postings via `Region.Grow` quando `postCount`
alcança a capacidade, e rehasheia o diretório quando os distintos passam de 70%
dos slots. **O rehash reescreve só o diretório**: as postings não se movem,
porque o que a cadeia guarda é o índice delas.

- [ ] **Passo 6: rodar até passar**

```bash
go test ./internal/index/segindex/ -count=1
go test -race ./internal/index/segindex/ -count=1   # gates de alocação viram SKIP
```

- [ ] **Passo 7: a contraparte sob `-race`**

`custo_gate_race_test.go` com `//go:build race` anuncia o SKIP dos gates de
ALOCAÇÃO nominalmente. O gate de SONDAS **não** é pulado: ele é determinístico
e roda nos dois modos. Gate que some em silêncio é carimbo (débito 53).

- [ ] **Passo 8: commitar**

```bash
git add internal/index/segindex/
git commit -m "Acrescenta a forma WIP, com inserção O(1)

O motor antigo varria a cadeia de probe inteira a cada Set para não
duplicar, então inserir n postings de um valor custava O(n²). Aqui as
postings de um valor formam uma lista própria e inserir é uma sondagem
amortizada mais duas escritas — e a sondagem é sobre o número de valores
DISTINTOS do segmento, não sobre o de postings.

Contagem e cabeça da lista viajam na mesma palavra de 64 bits. Em duas
escritas um leitor veria a cabeça nova com a contagem velha e dimensionaria
a fatia menor que a caminhada produz.

O gate de custo conta sondas e não nanossegundos: sem relógio não há ruído
de runner, ele roda também sob -race, e afirma a forma do algoritmo em vez
de um orçamento a recalibrar por máquina."
```

---

## Tarefa 4: selagem — WIP para selado

**Arquivos:**
- Criar: `internal/index/segindex/seal.go`
- Testar: `internal/index/segindex/seal_test.go`

**Interfaces:**
- Consome: `WIP.Lookup`, `ListWIP`, `OpenWIP` (Tarefa 3); `Writer` (Tarefa 1);
  `OpenSealed` (Tarefa 2).
- Produz:
  ```go
  func Seal(dir string, seg uint32) error
  func RemoveWIPs(dir string, seg uint32) error
  ```

`Seal` lê **todos** os `.widx` do segmento, alimenta um `Writer` e emite o
`.sidx`. É idempotente: chamada sobre um segmento que já tem `.sidx` e não tem
`.widx` é no-op, o que importa porque um crash entre o `rename` e o `unlink` dos
`.widx` deixa exatamente esse estado.

- [ ] **Passo 1: escrever o teste que falha**

```go
// TestSelarPreservaExatamenteOQueOWIPRespondia é a propriedade central da
// selagem: a conversão muda o layout, não o conjunto. Ela roda sobre um corpus
// com as três formas que quebram implementações diferentes — valor com uma
// posting só, valor com muitas, e rowIds fora de ordem (o caso do materializado).
func TestSelarPreservaExatamenteOQueOWIPRespondia(t *testing.T) {
	dir := t.TempDir()
	corpus := map[types.FieldID]map[uint64][]types.RowID{
		7: {100: {50, 10, 30, 40}, 200: {25}},
		9: {300: {11, 12, 13}},
	}
	antes := map[[2]uint64][]types.RowID{}
	for campo, valores := range corpus {
		w, err := segindex.OpenWIP(dir, 42, campo, 16)
		if err != nil {
			t.Fatal(err)
		}
		for h, rows := range valores {
			for _, r := range rows {
				if err := w.Set(h, r); err != nil {
					t.Fatal(err)
				}
			}
		}
		for h := range valores {
			antes[[2]uint64{uint64(campo), h}] = slices.Clone(w.Lookup(h))
		}
		if err := w.Fecha(); err != nil {
			t.Fatal(err)
		}
	}

	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	s, err := segindex.OpenSealed(segindex.SealedPath(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Fecha()
	for chave, want := range antes {
		campo, h := types.FieldID(chave[0]), chave[1]
		it, ok := s.Lookup(campo, h)
		if !ok {
			t.Fatalf("Lookup(%d, %d) devolveu ok=false depois da selagem", campo, h)
		}
		var got []types.RowID
		for it.Next() {
			got = append(got, it.RowID())
		}
		if !slices.Equal(got, want) {
			t.Errorf("Lookup(%d, %d) = %v, quero %v", campo, h, got, want)
		}
	}
}

// TestSelarDesligaOsWIPs: um .widx que sobrevive à selagem seria adotado pela
// próxima abertura como se o segmento ainda fosse o corrente, e ele passaria a
// receber postings que o .sidx nunca teria.
func TestSelarDesligaOsWIPs(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, 42, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Set(100, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Fecha(); err != nil {
		t.Fatal(err)
	}
	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatal(err)
	}
	campos, err := segindex.ListWIP(dir, 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(campos) != 0 {
		t.Errorf("ListWIP = %v, quero vazio", campos)
	}
}

// TestSelarEIdempotente cobre a janela de crash entre o rename do .sidx e o
// unlink dos .widx — que é exatamente o estado "tem .sidx e não tem .widx".
func TestSelarEIdempotente(t *testing.T) {
	dir := t.TempDir()
	w, err := segindex.OpenWIP(dir, 42, 7, 16)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Set(100, 1)
	_ = w.Fecha()
	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatal(err)
	}
	primeiro, err := os.ReadFile(segindex.SealedPath(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	if err := segindex.Seal(dir, 42); err != nil {
		t.Fatalf("segunda selagem: %v", err)
	}
	segundo, err := os.ReadFile(segindex.SealedPath(dir, 42))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(primeiro, segundo) {
		t.Error("a segunda selagem alterou o arquivo — ela deveria ser no-op")
	}
}
```

- [ ] **Passo 2: rodar e ver falhar**

```bash
go test ./internal/index/segindex/ -run TestSelar -count=1
```
Esperado: `undefined: segindex.Seal`.

- [ ] **Passo 3: implementar `seal.go`**

```go
// Seal converte a forma WIP do segmento na forma selada, e desliga os .widx.
//
// A ORDEM é a garantia: escreve o .sidx.tmp, fsync, rename, fsync do
// diretório, e SÓ ENTÃO remove os .widx. Na ordem inversa um crash no meio
// deixaria o segmento sem índice nenhum, e ele responderia por enumeração até
// a reconstrução alcançá-lo — correto, mas é trabalho que não precisava
// existir.
//
// Idempotente: sem .widx e com .sidx presente, no-op. É o estado que um crash
// entre o rename e o unlink deixa.
func Seal(dir string, seg uint32) error {
	campos, err := ListWIP(dir, seg)
	if err != nil {
		return err
	}
	if len(campos) == 0 {
		return nil
	}
	e := NewWriter()
	for _, campo := range campos {
		w, err := OpenWIP(dir, seg, campo, 0) // 0 = não dimensiona, só abre
		if err != nil {
			return err
		}
		err = w.ForEach(func(h uint64, row types.RowID) {
			e.Add(campo, h, row)
		})
		w.Fecha()
		if err != nil {
			return err
		}
	}
	if err := e.WriteSealed(SealedPath(dir, seg), seg); err != nil {
		return err
	}
	return RemoveWIPs(dir, seg)
}
```

`WIP.ForEach` é novo e varre o **array de postings**, não o diretório — cada
posting aparece exatamente uma vez, e a varredura é sequencial em vez de saltar
por cadeias. Para recuperar o `hash` de cada posting sem caminhar a cadeia,
`ForEach` faz uma primeira passada pelo diretório montando
`índiceDaPosting → hash` a partir das cadeias, e uma segunda pelo array. Se
isso ficar caro, a alternativa é caminhar as cadeias direto — mas então
documente que a saída não é sequencial no arquivo.

**Nota para quem implementar:** o `Writer` ordena e deduplica, então
`ForEach` **não** precisa entregar nada ordenado. Não reordene aqui: seria a
mesma ordenação duas vezes, e o segundo lugar divergiria do primeiro no dia em
que alguém mudasse o critério.

- [ ] **Passo 4: rodar até passar e commitar**

```bash
go test ./internal/index/segindex/ -count=1 && go test -race ./internal/index/segindex/ -count=1
git add internal/index/segindex/
git commit -m "Sela o índice do segmento na rotação

A selagem escreve o .sidx antes de remover os .widx. Na ordem inversa um
crash no meio deixaria o segmento sem índice nenhum, e ele responderia por
enumeração até a reconstrução alcançá-lo — correto, mas trabalho que não
precisava existir.

É idempotente porque a janela de crash entre o rename e o unlink deixa
exatamente o estado 'tem selado e não tem WIP', e ele precisa ser
atravessável sem intervenção."
```

---

## Tarefa 5: a união das fontes

**Arquivos:**
- Criar: `internal/index/segindex/union.go`
- Testar: `internal/index/segindex/union_test.go`

**Interfaces:**
- Consome: `Fonte` (Tarefa 2).
- Produz:
  ```go
  func Union(fontes []Fonte) types.RowIDIterator
  ```
  E, em `export_test.go`: `func StrategyOf(it types.RowIDIterator) string`
  devolvendo `"concatenacao"` ou `"merge"`.

- [ ] **Passo 1: escrever os testes que falham**

```go
// TestUneConcatenaFaixasDisjuntas: é o caso do histórico, onde o rowId é
// monotônico e os segmentos particionam o espaço. Concatenar custa ZERO
// comparações, contra ~9 por linha de um heap de 500 fontes.
func TestUneConcatenaFaixasDisjuntas(t *testing.T) {
	it := segindex.Union([]segindex.Source{
		fakeSource{10, 20, 30},
		fakeSource{40, 50},
		fakeSource{60},
	})
	if got := segindex.StrategyOf(it); got != "concatenacao" {
		t.Errorf("estratégia = %q, quero \"concatenacao\"", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40, 50, 60}) {
		t.Errorf("drena = %v", got)
	}
}

// TestUneMergeiaFaixasSobrepostasEDeduplica: é o caso do materializado, onde a
// mesma linha pode ter posting em dois segmentos — gravada com o valor x no
// segmento 5 e regravada com o MESMO x no segmento 9, ao atualizar outro campo.
// Emiti-la duas vezes faria o unionIter do executor devolver a mesma linha
// duplicada num OR.
func TestUneMergeiaFaixasSobrepostasEDeduplica(t *testing.T) {
	it := segindex.Union([]segindex.Source{
		fakeSource{10, 30, 50},
		fakeSource{20, 30, 40},
	})
	if got := segindex.StrategyOf(it); got != "merge" {
		t.Errorf("estratégia = %q, quero \"merge\"", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40, 50}) {
		t.Errorf("drena = %v, quero sem repetir o 30", got)
	}
}

// TestUneDescartaFontesVazias: uma fonte vazia reporta Min > Max justamente
// para não participar do teste de disjunção. Sem o descarte, uma fonte vazia
// com Min=0,Max=0 empurraria o histórico inteiro para o caminho de merge.
func TestUneDescartaFontesVazias(t *testing.T) {
	it := segindex.Union([]segindex.Source{
		fakeSource{10, 20},
		emptyTestSource{},
		fakeSource{30, 40},
	})
	if got := segindex.StrategyOf(it); got != "concatenacao" {
		t.Errorf("estratégia = %q, quero \"concatenacao\" — a vazia não pode forçar o merge", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30, 40}) {
		t.Errorf("drena = %v", got)
	}
}

// TestUneNaoConcatenaQuandoAsFaixasSeTocam trava a fronteira exata: faixas
// [10,20] e [20,30] compartilham o 20 e NÃO são disjuntas. Um `<=` no lugar de
// `<` aqui produziria a mesma linha duas vezes, silenciosamente.
func TestUneNaoConcatenaQuandoAsFaixasSeTocam(t *testing.T) {
	it := segindex.Union([]segindex.Source{fakeSource{10, 20}, fakeSource{20, 30}})
	if got := segindex.StrategyOf(it); got != "merge" {
		t.Errorf("estratégia = %q, quero \"merge\"", got)
	}
	if got := drena(t, it); !slices.Equal(got, []types.RowID{10, 20, 30}) {
		t.Errorf("drena = %v, quero o 20 uma vez só", got)
	}
}

// TestUnePropagaOPrimeiroErro: uma fonte que falha no meio não pode virar
// "resultado menor". Falso negativo é o único desfecho que este produto não
// tolera, e resultado truncado sem erro é exatamente isso.
func TestUnePropagaOPrimeiroErro(t *testing.T) {
	quebrada := brokenSource{rows: []types.RowID{10}, err: errors.New("mmap sumiu")}
	it := segindex.Union([]segindex.Source{fakeSource{1, 2}, quebrada})
	for it.Next() {
	}
	if it.Err() == nil {
		t.Fatal("Err() = nil, quero o erro da fonte quebrada")
	}
}
```

Os dublês `fakeSource` (slice ordenado; `MinRowID`/`MaxRowID` do primeiro e do
último), `emptyTestSource` (`Min=1, Max=0`) e `brokenSource` ficam no mesmo
arquivo de teste.

- [ ] **Passo 2: rodar e ver falhar**

```bash
go test ./internal/index/segindex/ -run TestUne -count=1
```
Esperado: `undefined: segindex.Union`.

- [ ] **Passo 3: implementar `union.go`**

```go
// Union combina as fontes de vários segmentos num iterador ascendente único.
//
// A estratégia vem dos DADOS — das faixas que as fontes declaram —, e nunca do
// modo do dataset. A diferença importa: um predicado apoiado no modo transforma
// uma premissa quebrada em resultado silenciosamente errado; um apoiado no dado
// degrada para o caminho mais lento e continua certo.
//
// Faixas disjuntas e ordenadas viram CONCATENAÇÃO: zero comparações, e a ordem
// crescente é herdada. É o histórico, sempre — o rowId é monotônico e os
// segmentos particionam o espaço.
//
// Faixas sobrepostas viram MERGE k-way por heap, com dedup de cabeças iguais.
// É o materializado, sempre: a mesma linha pode ter posting em dois segmentos,
// porque um update a regrava com o mesmo valor num segmento novo sem que nada
// remova a posting antiga (é a fase de Clear que deixou de existir).
func Union(fontes []Fonte) types.RowIDIterator {
	vivas := make([]Fonte, 0, len(fontes))
	for _, f := range fontes {
		if f.MinRowID() > f.MaxRowID() {
			f.Close() // vazia por declaração: não participa da disjunção
			continue
		}
		vivas = append(vivas, f)
	}
	switch len(vivas) {
	case 0:
		return vazio{}
	case 1:
		return vivas[0]
	}
	slices.SortFunc(vivas, func(a, b Fonte) int { return cmp.Compare(a.MinRowID(), b.MinRowID()) })
	if disjuntas(vivas) {
		return &concatIter{fontes: vivas}
	}
	return newMergeIter(vivas)
}

// disjuntas exige separação ESTRITA. Faixas que se tocam num rowId comum não
// são disjuntas: concatená-las emitiria esse rowId duas vezes, e o contrato do
// iterador é estritamente crescente.
func disjuntas(f []Fonte) bool {
	for i := 1; i < len(f); i++ {
		if f[i-1].MaxRowID() >= f[i].MinRowID() {
			return false
		}
	}
	return true
}
```

`concatIter.Next` avança na fonte corrente e passa à próxima no fim, guardando
o primeiro erro e parando. `mergeIter` mantém um heap binário de fontes por
`RowID()` corrente, emite o menor e **descarta as cabeças iguais** antes de
devolver. `Close` de qualquer um fecha todas as fontes restantes — vazar
iterador de índice num caminho de erro é o tipo de coisa que só aparece como
descritor esgotado em produção.

- [ ] **Passo 4: teste de propriedade**

```go
// TestUneEhEquivalenteAUniaoOrdenada compara contra um oráculo trivial sobre
// entrada aleatória: junta tudo, ordena, compacta. É este teste que reprova a
// mutação M3 (concatenar sempre) e a M2 (merge sem dedup) da spec.
func TestUneEhEquivalenteAUniaoOrdenada(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		var fontes []segindex.Source
		var esperado []types.RowID
		for range 1 + rng.IntN(6) {
			var rows []types.RowID
			for range rng.IntN(30) {
				rows = append(rows, types.RowID(rng.IntN(100)))
			}
			slices.Sort(rows)
			rows = slices.Compact(rows)
			esperado = append(esperado, rows...)
			fontes = append(fontes, fakeSource(rows))
		}
		slices.Sort(esperado)
		esperado = slices.Compact(esperado)
		if got := drena(t, segindex.Union(fontes)); !slices.Equal(got, esperado) {
			t.Fatalf("Union = %v, quero %v", got, esperado)
		}
	}
}
```

- [ ] **Passo 5: rodar tudo, conferir a Fase A e commitar**

```bash
go test ./internal/index/segindex/ -count=1
go test -race ./internal/index/segindex/ -count=1
make lint   # sai != 0 enquanto a baseline > 0; confira que a CONTAGEM é 387
git add internal/index/segindex/
git commit -m "Une as fontes de vários segmentos num iterador ascendente

A estratégia vem das faixas que as fontes declaram, nunca do modo do
dataset. Um predicado apoiado no modo transformaria uma premissa quebrada
em resultado silenciosamente errado; apoiado no dado, ele degrada para o
merge e continua certo.

Faixas que se TOCAM não são disjuntas. Um <= no lugar do < emitiria o rowId
compartilhado duas vezes, e o contrato do iterador é estritamente crescente
porque o unionIter do executor conta com isso num OR."
```

**Fim da Fase A.** `internal/index/segindex` está completo e testado sem que uma
linha de `internal/storage` tenha mudado.

---

# Fase B — o `Store`

> **Andaime declarado.** As Tarefas 6 e 7 mantêm os dois motores vivos ao mesmo
> tempo. Isso NÃO é fallback para legado: é o oráculo diferencial da migração, e
> a Tarefa 9 o remove dentro da mesma branch. Nenhum commit desta fase é
> entregável sozinho.

## Tarefa 6: o `Store` mantém o índice de segmento

**Arquivos:**
- Criar: `internal/storage/indices.go`
- Modificar: `internal/storage/store.go` (campos do `Store`, `Open`, `applyRecord`,
  os três `curSegID.Store`), `internal/storage/compaction.go`
  (`aposentaSegmento`), `internal/storage/expiry.go`
- Testar: `internal/storage/indices_test.go`

**Interfaces:**
- Consome: todo o `segindex` da Fase A.
- Produz (não exportado, salvo indicado):
  ```go
  func (s *Store) abreIndicesDeSegmento(campos []types.FieldID) error
  func (s *Store) aposAppend(novoSeg uint32) error   // substitui os curSegID.Store
  func (s *Store) aplicaIndiceSets(row types.RowID, prev, novo []byte) error
  func (s *Store) fechaIndices() error
  func (s *Store) CamposIndexados() []types.FieldID  // exportado: usado pelos testes e pelo _meta
  ```

- [ ] **Passo 1: os testes que falham**

```go
// TestOSegmentoCorrenteGanhaWIPPorCampoIndexado
func TestOSegmentoCorrenteGanhaWIPPorCampoIndexado(t *testing.T) {
	s, dir := abreStoreIndexado(t, []string{"node", "type"}) // helper novo
	defer s.Close()
	poeLinhas(t, s, 10)
	campos, err := segindex.ListWIP(filepath.Join(dir, "data"), s.SegmentoCorrente())
	if err != nil {
		t.Fatal(err)
	}
	if len(campos) != 2 {
		t.Fatalf("ListWIP = %v, quero um .widx por campo indexado", campos)
	}
}

// TestARotacaoSelaOSegmentoAnterior: a selagem é disparada pelo Append que
// rotaciona, e não por um temporizador nem pelo checkpoint. Se ela dependesse
// de qualquer outra coisa, existiria um intervalo em que o segmento anterior
// não é mais o corrente e ainda tem .widx — e o Open o descartaria, jogando
// fora um índice inteiro por nada.
func TestARotacaoSelaOSegmentoAnterior(t *testing.T) {
	s, dir := abreStoreIndexadoComSegmentoPequeno(t, []string{"node"}, 4<<10)
	defer s.Close()
	primeiro := s.SegmentoCorrente()
	poeLinhasAteRotacionar(t, s, primeiro)
	if s.SegmentoCorrente() == primeiro {
		t.Fatal("o segmento não rotacionou; o teste não exercita nada")
	}
	if _, err := os.Stat(segindex.SealedPath(filepath.Join(dir, "data"), primeiro)); err != nil {
		t.Fatalf("o segmento %d rotacionou sem selar o índice: %v", primeiro, err)
	}
	campos, _ := segindex.ListWIP(filepath.Join(dir, "data"), primeiro)
	if len(campos) != 0 {
		t.Errorf("o segmento selado %d ainda tem .widx: %v", primeiro, campos)
	}
}

// TestAAposentadoriaLevaOIndiceJunto é a segunda dor da spec, e o teste que
// prova que ela foi resolvida por CONSTRUÇÃO. Antes, nada removia as postings
// de um segmento aposentado, e cada órfã alongava a cadeia de quem estava vivo.
func TestAAposentadoriaLevaOIndiceJunto(t *testing.T) {
	s, dir := abreStoreHistoricoIndexado(t, []string{"node"}, 4<<10)
	defer s.Close()
	alvo := s.SegmentoCorrente()
	poeLinhasAteRotacionar(t, s, alvo)
	caminho := segindex.SealedPath(filepath.Join(dir, "data"), alvo)
	if _, err := os.Stat(caminho); err != nil {
		t.Fatalf("pré-condição: o segmento %d deveria estar selado: %v", alvo, err)
	}
	if _, err := s.RetencaoUmaPassada(t.Context(), storage.LimitesDeRetencao{MaxBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(caminho); !os.IsNotExist(err) {
		t.Errorf("Stat(%s) = %v, quero IsNotExist — o índice tem de sair com o .dat", filepath.Base(caminho), err)
	}
}
```

- [ ] **Passo 2: rodar e ver falhar**

```bash
go test ./internal/storage/ -run 'TestOSegmentoCorrenteGanha|TestARotacaoSela|TestAAposentadoriaLeva' -count=1
```

- [ ] **Passo 3: campos novos no `Store` e abertura**

Em `store.go`, ao lado de `bitmaps`/`hashes` (que **continuam vivos nesta
tarefa**):

```go
	// wips são os índices de segmento MUTÁVEIS — um por campo indexado, todos
	// do segmento corrente. Mapa e não slice porque o applyRecord endereça
	// por FieldID.
	wips map[types.FieldID]*segindex.WIP
	// selados são os índices imutáveis, abertos SOB DEMANDA e mantidos
	// mapeados. Não há LRU: o mapeamento somente-leitura não retém descritor,
	// e 500 VMAs num teto padrão de 65.530 não é pressão.
	selados map[uint32]*segindex.Sealed
	selMu   sync.RWMutex
	// camposIndexados é o conjunto ordenado, resolvido no Open e IMUTÁVEL na
	// vida do Store.
	camposIndexados []types.FieldID
```

`abreIndicesDeSegmento` roda no `Open`, DEPOIS de `resolveCamposIndexados` (a
ordem de `LookupOrCreate` continua contratual — ver o comentário de
`store.go:898`, que não pode ser reordenado), e abre um `WIP` por campo para
`s.curSegID.Load()`.

- [ ] **Passo 4: um único ponto de rotação**

Os três `s.curSegID.Store(novoSeg)` (`store.go:1475`, `compaction.go:206`,
`expiry.go:117`) passam a chamar:

```go
// aposAppend registra o segmento devolvido pelo Append e, quando ele mudou,
// sela o anterior e abre os WIPs do novo.
//
// Um ponto só, e não três, porque a rotação acontece DENTRO do
// segment.Writer.Append (segment.go:195) e é invisível a quem chama — os três
// callers só veem o segID de volta. Três cópias divergiriam, e a que
// esquecesse de selar deixaria um segmento com .widx que o Open descarta:
// um índice inteiro jogado fora, sem erro e sem log.
//
// Chamado sob wmu, como os três Store que substitui.
func (s *Store) aposAppend(novoSeg uint32) error {
	anterior := s.curSegID.Swap(novoSeg)
	if anterior == novoSeg || anterior == 0 {
		return nil
	}
	if err := s.fechaWIPs(); err != nil {
		return err
	}
	if err := segindex.Seal(s.dirDeDados(), anterior); err != nil {
		return err
	}
	return s.abreWIPsDoCorrente()
}
```

- [ ] **Passo 5: a fase de SET**

Em `applyRecord`, ao lado das chamadas existentes (passo 5, **antes** do commit
da location):

```go
	if len(s.wips) > 0 {
		if err := s.aplicaIndiceSets(a.rowID, a.prevPayload, a.payload); err != nil {
			return err
		}
	}
```

`aplicaIndiceSets` percorre os campos do payload novo, e para cada
`FieldID ∈ camposIndexados` calcula `primary.HashKey(valuekey.Append(buf[:0], v))`
e chama `wip.Set`. O buffer é do `Store` e é reatribuído (`buf[:0]`, sempre
reatribuindo o slice devolvido) — é o que mantém o gate de zero alocação.

**Não há fase de CLEAR.** Escreva o comentário agora, porque é o que impede a
próxima revisão de "consertar" a ausência:

```go
	// NÃO existe fase de CLEAR para o índice de segmento, e a ausência é o
	// desenho. Quando uma linha troca de valor, a posting antiga mora no
	// índice do segmento ANTIGO: ela vira falso positivo, que a revalidação do
	// §44 já descarta, e morre quando a compaction ou a retenção aposenta
	// aquele segmento. With o Clear somem a lápide e o falso negativo que ela
	// existia para evitar. O preço é uma taxa de falso positivo governada por
	// liveRecords/totalRecords — o mesmo número que a compaction já persegue.
```

- [ ] **Passo 6: a aposentadoria leva o índice**

Em `aposentaSegmento` (`compaction.go:492`), no passo 4, junto de
`fechaODescritorEApaga`: fechar o `Selado` daquele segmento (se mapeado),
removê-lo de `s.selados` e `os.Remove(segindex.SealedPath(...))`. Erro de
remoção do índice **não** falha a aposentadoria: o `.dat` já saiu e o disco já
voltou; fica WARN nomeado e o `Open` limpa o órfão.

- [ ] **Passo 7: rodar tudo**

```bash
go test ./internal/storage/ -count=1
go test -race ./internal/storage/ -count=1
make test
```
A suíte inteira tem de continuar verde: nesta tarefa nada LÊ do motor novo.

- [ ] **Passo 8: commitar**

```bash
git add internal/storage/ && git commit -m "O Store passa a manter o índice por segmento

O índice do segmento corrente nasce e morre com ele: os WIPs abrem no Open,
selam na rotação e o .sidx sai no mesmo passo da aposentadoria que remove o
.dat. É isso que faz 'purgar o segmento leva o índice junto' ser código em
vez de promessa — antes, nada removia as postings de um segmento
aposentado, e cada órfã alongava a cadeia de quem ainda estava vivo.

A rotação ganha um ponto único. Ela acontece dentro do Append e é invisível
a quem chama, então as três cópias do curSegID.Store divergiriam, e a que
esquecesse de selar jogaria fora um índice inteiro sem erro e sem log.

Não há fase de Clear, e o comentário diz por quê: com o índice dentro do
segmento a posting antiga vira falso positivo e morre com o segmento.

O motor antigo continua vivo e continua respondendo às consultas. É andaime
da migração e sai na tarefa que remove os dois pacotes."
```

---

## Tarefa 7: recovery e construção a partir dos records

**Arquivos:**
- Criar: `internal/storage/indices_rebuild.go`
- Modificar: `internal/storage/store.go` (`Open`), `internal/storage/store_recovery.go`
- Testar: `internal/storage/indices_recovery_test.go`

**Interfaces:**
- Produz:
  ```go
  func (s *Store) ConstroiIndiceDoSegmento(ctx context.Context, seg uint32, campos []types.FieldID) error
  func (s *Store) segmentosSemIndice() ([]uint32, error)
  ```

- [ ] **Passo 1: os testes que falham**

```go
// TestOWIPNaoEhAdotadoAtravesDeUmRestart. O .widx é dado derivado e nunca é
// confiado através de um restart — reconstruí-lo do próprio segmento custa uma
// leitura sequencial de no máximo 512 MiB e elimina, de uma vez, o msync do
// índice no checkpoint, a invariante de ordem contra o durableOffset e a classe
// inteira de janelas de crash do índice secundário.
func TestOWIPNaoEhAdotadoAtravesDeUmRestart(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, []string{"node"})
	poeLinhas(t, s, 100)
	s.Close()

	// Corrompe o .widx de propósito: se ele fosse adotado, o Lookup mentiria.
	caminho := segindex.WIPPath(filepath.Join(dir, "data"), 1, campoDe(t, s, "node"))
	if err := os.WriteFile(caminho, bytes.Repeat([]byte{0xAA}, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node"})
	defer s2.Close()
	rows := drenaLookup(t, s2, "node", "n1")
	if len(rows) == 0 {
		t.Fatal("o índice do segmento corrente não foi reconstruído na abertura")
	}
}

// TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP é o golden que torna a
// reconstrução confiável. Os dois caminhos alimentam o MESMO Writer, então a
// igualdade é por construção — o que este teste prova é que a ALIMENTAÇÃO é
// equivalente, que é o risco de verdade: um caminho que pulasse tombstones ou
// lesse o payload anterior produziria um índice silenciosamente diferente.
func TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoComSegmentoPequeno(t, dir, []string{"node", "type"}, 8<<10)
	alvo := s.SegmentoCorrente()
	poeLinhasVariadasAteRotacionar(t, s, alvo) // inclui update, tombstone e campo ausente
	s.Close()

	caminho := segindex.SealedPath(filepath.Join(dir, "data"), alvo)
	porSelagem, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(caminho); err != nil {
		t.Fatal(err)
	}

	s2 := abreStoreIndexadoEm(t, dir, []string{"node", "type"})
	defer s2.Close()
	if err := s2.ConstroiIndiceDoSegmento(t.Context(), alvo, s2.CamposIndexados()); err != nil {
		t.Fatal(err)
	}
	porReconstrucao, err := os.ReadFile(caminho)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(porSelagem, porReconstrucao) {
		t.Error("selagem e reconstrução produziram arquivos diferentes")
	}
}
```

- [ ] **Passo 2: rodar e ver falhar; depois implementar**

`ConstroiIndiceDoSegmento` varre o segmento com o mesmo iterador de records que
o replay do tail usa (`store_recovery.go`), alimenta um `segindex.Writer` com
`(campo, hash, rowID)` de cada record e chama `WriteSealed`. Ele **não** consulta
`locations.idx`, **não** filtra tombstone e **não** decide liveness: o índice é
superconjunto por contrato, e filtrar aqui introduziria uma segunda definição de
"vivo" que divergiria da do executor.

No `Open`, depois do replay do tail:

1. `RemoveWIPs` de **todo** segmento que não é o corrente, e enfileira esses
   segmentos para reconstrução se não tiverem `.sidx`;
2. `RemoveWIPs` do corrente e `ConstroiIndiceDoSegmento` dele **em memória** —
   ou seja, reabre os WIPs vazios e realimenta lendo o segmento inteiro, antes
   de o primeiro `Put` ser aceito.

- [ ] **Passo 3: rodar, incluindo os testes de crash já existentes**

```bash
go test ./internal/storage/ -run 'Recovery|Crash|Indice' -count=1
go test -race ./internal/storage/ -count=1
make test
```

- [ ] **Passo 4: commitar**

```bash
git add internal/storage/ && git commit -m "Reconstrói o índice do segmento em vez de confiar no WIP

O .widx nunca atravessa um restart. Reconstruí-lo do próprio segmento custa
uma leitura sequencial de no máximo 512 MiB e elimina de uma vez o msync do
índice no checkpoint, a invariante de ordem contra o durableOffset e a
classe inteira de janelas de crash do índice secundário. Índice é dado
derivado: a recuperação mais barata que está correta é derivá-lo de novo.

A construção a partir dos records alimenta o mesmo escritor que a selagem,
então os dois caminhos dão o mesmo arquivo por construção. O golden prova
que a alimentação é equivalente, que é onde o erro moraria: um caminho que
filtrasse tombstone ou lesse o payload anterior produziria um índice
silenciosamente diferente."
```

---

## Tarefa 8: o `Lookup` passa a ler do índice de segmento

Esta é a tarefa-oráculo do plano. `LookupHash` e `LookupBitmap` **mantêm o nome
e a assinatura** e passam a responder pelo motor novo — de modo que a suíte
inteira de `storage` e `query`, que já testa o contrato (superconjunto, ordem
crescente, reentrância no `Close`), roda contra ele **sem uma linha de teste
mudada**. A renomeação vem só na Tarefa 11.

**Arquivos:**
- Criar: `internal/storage/indices_enum.go`
- Modificar: `internal/storage/store.go` (`LookupHash`, `LookupBitmap`),
  `internal/storage/indices.go`
- Testar: `internal/storage/indices_enum_test.go`

**Interfaces:**
- Produz:
  ```go
  func (s *Store) fontesDoCampo(campo types.FieldID, h uint64) ([]segindex.Source, error)
  type fonteFaixa struct{ ... }      // histórico: [primeiro, primeiroDoSeguinte)
  type fonteLocations struct{ ... }  // materializado: filtro do locations.idx
  ```

- [ ] **Passo 1: os testes que falham**

```go
// TestSegmentoSemIndiceRespondePorEnumeracao é a resposta da spec à Q3: o
// contrato do Lookup já é superconjunto com revalidação, então enumerar as
// linhas de um segmento sem índice é uma resposta CORRETA — só lenta. No
// histórico isso torna "fora da janela" e "gravado por binário anterior" o
// mesmo estado, e é o que faz a migração não exigir recriar dataset.
func TestSegmentoSemIndiceRespondePorEnumeracao(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreHistoricoIndexadoEm(t, dir, []string{"node"}, 8<<10)
	alvo := s.SegmentoCorrente()
	poeLinhasAteRotacionar(t, s, alvo)
	s.Close()

	// Simula o dataset antigo: o segmento existe, o índice não.
	if err := os.Remove(segindex.SealedPath(filepath.Join(dir, "data"), alvo)); err != nil {
		t.Fatal(err)
	}
	s2 := abreStoreHistoricoIndexadoEm(t, dir, []string{"node"}, 8<<10)
	defer s2.Close()
	s2.ParaReconstrucao() // isola a enumeração da reconstrução

	rows := drenaLookup(t, s2, "node", "n1")
	esperado := drenaFullScanFiltrado(t, s2, "node", "n1")
	if !slices.Equal(rows, esperado) {
		t.Errorf("Lookup por enumeração = %v, quero o mesmo que o full scan filtrado = %v", rows, esperado)
	}
}

// TestAEnumeracaoDoMaterializadoUsaOLocationsENaoOsRecords: no materializado os
// rowIds de um segmento não formam faixa contígua, então a fonte tem de sair do
// locations.idx. Ler os records para descobrir quem mora ali seria pagar
// exatamente o custo que a enumeração existe para adiar até a revalidação.
func TestAEnumeracaoDoMaterializadoUsaOLocationsENaoOsRecords(t *testing.T) {
	// ... monta base com updates espalhando rowIds por dois segmentos,
	// remove o .sidx do mais antigo, e afirma que o conjunto devolvido é
	// exatamente {rowId | location aponta para aquele segmento}.
}

// TestOResultadoNaoMudaComOSegmentoIndexadoOuNao é a afirmação mais forte da
// tarefa e o oráculo da migração: a MESMA consulta sobre o MESMO dado devolve o
// MESMO conjunto, indexado ou enumerado. A diferença é só de custo.
func TestOResultadoNaoMudaComOSegmentoIndexadoOuNao(t *testing.T) {
	// ... roda um corpus de N consultas com todos os .sidx presentes,
	// guarda os resultados, apaga todos os .sidx, reabre com a reconstrução
	// parada, roda de novo e compara conjunto a conjunto.
}
```

- [ ] **Passo 2: rodar e ver falhar; implementar `indices_enum.go`**

```go
// fonteFaixa enumera os rowIds de um segmento do HISTÓRICO. Ela é possível por
// uma propriedade que só a tabela histórica tem: os appends são serializados
// pelo mutex do writer e o rowId é monotônico, então os segmentos particionam
// o espaço de rowId em faixas contíguas e ordenadas. É a mesma propriedade que
// primeiroRowIDDoSeguinte (retention.go:380) já explora, com o mesmo pread.
type fonteFaixa struct{ prox, fim types.RowID }

// fonteLocations enumera os rowIds cuja location aponta para um segmento, no
// dataset MATERIALIZADO — onde a propriedade acima não vale, porque um update
// traz para o segmento corrente uma linha de rowId arbitrário.
//
// A varredura é sobre o locations.idx, que é array denso em mmap: 8 milhões de
// linhas são 64 MB sequenciais. Ela é PREGUIÇOSA e não materializa nada — o
// consumo é constante, independente de quantas linhas casem —, e a ordem
// crescente sai da própria caminhada.
//
// MinRowID/MaxRowID são a marca-d'água e o nextRowID, e não o intervalo real
// dos rowIds que moram no segmento: descobrir o real exigiria a varredura que
// ainda não aconteceu. É um limite CORRETO e frouxo, e frouxo empurra a união
// para o merge — que é sempre certo, só mais caro.
type fonteLocations struct{ s *Store; seg uint32; prox, fim types.RowID }
```

`fontesDoCampo` monta, em ordem crescente de segmento:

```go
func (s *Store) fontesDoCampo(campo types.FieldID, h uint64) ([]segindex.Source, error) {
	refs, err := s.listaSegmentos()
	if err != nil {
		return nil, err
	}
	corrente := s.curSegID.Load()
	fontes := make([]segindex.Source, 0, len(refs))
	for _, ref := range refs {
		switch {
		case ref.ID == corrente:
			fontes = append(fontes, s.fonteDoWIP(campo, h))
		default:
			sel, err := s.seladoDe(ref.ID) // abre sob demanda; nil quando ausente
			if err != nil {
				return nil, err
			}
			if sel != nil {
				if f, ok := sel.Lookup(campo, h); ok {
					fontes = append(fontes, f)
					continue
				}
				// ok=false com o arquivo presente significa seção ausente: o
				// campo entrou no schema depois e a reconstrução ainda não
				// alcançou este segmento. Enumera, como se não houvesse índice.
			}
			f, err := s.fonteDeEnumeracao(ref.ID)
			if err != nil {
				return nil, err
			}
			fontes = append(fontes, f)
		}
	}
	return fontes, nil
}
```

`LookupHash` e `LookupBitmap` passam a ser:

```go
func (s *Store) LookupHash(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	return s.lookupIndexado(field, v)
}
func (s *Store) LookupBitmap(field types.FieldID, v types.Value) (types.RowIDIterator, error) {
	return s.lookupIndexado(field, v)
}
```

`lookupIndexado` valida que o campo é indexado (erro `ErrInvalidArgument` se
não), calcula o hash, monta as fontes, chama `segindex.Union` e embrulha no mesmo
`hashIterator` que já existe — que é quem dá a reentrância contra o `Close` do
dataset. **Não mexa nesse embrulho:** ele é o que faz um `Close` no meio da
varredura sair com `ErrDatasetClosed` em vez de pânico, e o executor mistura as
fontes no mesmo iterador k-way.

- [ ] **Passo 3: rodar a suíte INTEIRA — é este o momento do oráculo**

```bash
make test
make test-race
make test-integration
```

Nenhum teste de `storage/` ou `query/` deve ter sido editado nesta tarefa. Se
algum falhar, **não ajuste o teste**: ou o motor novo diverge do contrato, ou o
teste pegava um detalhe do arquivo antigo. Separe os dois antes de mexer em
qualquer linha.

- [ ] **Passo 4: commitar**

```bash
git add internal/storage/ && git commit -m "As consultas passam a ler do índice por segmento

LookupHash e LookupBitmap mantêm nome e assinatura de propósito: assim a
suíte inteira de storage e query, que já testa o contrato de superconjunto
com ordem crescente, roda contra o motor novo sem uma linha de teste
mudada. É o oráculo da migração.

Segmento sem índice responde enumerando as linhas dele, o que é correto
porque o contrato já é superconjunto com revalidação. No histórico isso faz
'fora da janela' e 'gravado por binário anterior' serem o mesmo estado, e é
por isso que nenhum dataset precisa ser recriado."
```

---

## Tarefa 9: remover o motor antigo

**Arquivos:**
- Remover: `internal/index/hash/`, `internal/index/bitmap/` (pacotes inteiros,
  com os testes)
- Modificar: `internal/storage/store.go` (campos `bitmaps`/`hashes`,
  `applyBitmapSets/Clears`, `applyHashSets/Clears`, `clearFailures`,
  `HasBitmap`, `HasHash`, `BitmapFields`, `HashFields`,
  `validaBitmapsContraManifest`, `validaHashesContraManifest`),
  `internal/storage/checkpoint/manifest.go`
- Remover os testes que atacam os arquivos antigos:
  `store_hash_test.go`, `store_bitmapset_test.go`, `bitmap_order_test.go`,
  `close_bitmap_test.go`, `store_bitmap_orphan_residue_test.go`,
  `store_recovery_bitmap_test.go` — **um a um, conferindo se a propriedade
  testada já é coberta em `internal/index/segindex` ou em `indices_test.go`.
  Propriedade que não estiver, MIGRE antes de apagar o arquivo.**

- [ ] **Passo 1: inventário antes de apagar**

```bash
grep -rn "func Test" internal/storage/store_hash_test.go internal/storage/store_bitmapset_test.go \
  internal/storage/bitmap_order_test.go internal/storage/close_bitmap_test.go \
  internal/storage/store_bitmap_orphan_residue_test.go internal/storage/store_recovery_bitmap_test.go
```

Escreva a lista num rascunho e marque, para cada teste, onde a propriedade
passa a viver. **Nenhum nome sai da lista sem destino.** É o passo que separa
"removi o motor antigo" de "removi o motor antigo e a rede de proteção dele".

- [ ] **Passo 2: manifest v3**

```go
	// manifestV3 remove a seção de hashes e zera hashKind/bitmapCount no corpo:
	// os dois descreviam tabelas GLOBAIS que deixaram de existir.
	//
	// É a VERSÃO que bloqueia o binário anterior, e não uma tag: remover uma
	// tag crítica não faz ninguém recusar nada. Um binário v2 que abrisse este
	// dataset manteria os índices globais que ele conhece e ignoraria os de
	// segmento — e os de segmento reapareceriam INCOMPLETOS, que é o modo de
	// falha que o AllowBitmapDrop existia para tornar deliberado.
	manifestV3 = 3

	// tagPerSegmentIndex declara o formato do índice de segmento. Ela é
	// crítica pela mesma razão das outras, e existe para que a PRÓXIMA mudança
	// de formato do índice tenha onde ser declarada sem outro bump de versão
	// do manifest — que é exatamente o que a cauda TLV foi criada para evitar.
	tagPerSegmentIndex = 0x8004
```

`hashKind` e `bitmapCount` continuam ocupando os mesmos bytes, gravados como
zero e ignorados na leitura, para não reescrever o parser do corpo. **Escreva
isso no comentário do campo**: reservado que ninguém sabe por que existe é como
nasce a próxima leitura errada.

- [ ] **Passo 3: apagar e compilar**

```bash
git rm -r internal/index/hash internal/index/bitmap
go build ./... 2>&1 | head -40
```
Resolva os erros um a um. `internal/testutil/fakestorage/` também implementa
`LookupHash`/`HasBitmap` e precisa acompanhar.

- [ ] **Passo 4: suíte, lint e baseline**

```bash
make test && make test-race && make test-integration
make lint    # a contagem vai MUDAR: apagar dois pacotes tira apontamentos
```

**A baseline cai, e o gate quebra nos dois sentidos.** Meça a contagem nova e
escreva-a em `.golangci-baseline` **no mesmo commit**. Não escreva o número em
nenhum comentário.

- [ ] **Passo 5: commitar**

```bash
git add -A && git commit -m "Remove os índices globais de hash e bitmap

Os dois motores globais saem inteiros, com os testes que atacavam o formato
deles. As propriedades que eles cobriam e que continuam valendo foram
migradas para internal/index/segindex e para indices_test.go antes da
remoção — o inventário está no histórico desta branch.

O manifest vai para v3. É a versão que bloqueia o binário anterior, e não
uma tag: remover uma tag crítica não faz ninguém recusar nada. Um binário
v2 abrindo este dataset manteria os índices globais que conhece e ignoraria
os de segmento, e os de segmento reapareceriam incompletos.

Baseline do lint reajustada no mesmo commit, porque o gate quebra também
para baixo."
```

**Fim da Fase B.** O andaime acabou: existe um motor só.

---

# Fase C — a superfície

## Tarefa 10: `index: true` é a única grafia

**Arquivos:**
- Modificar: `internal/schema/compile.go` (`IndexKind`, `indexKindDe`,
  `CompiledField.Index`), o decodificador do YAML de campo
- Testar: `internal/schema/compile_test.go`
- Modificar: `README.md`, `CHANGELOG.md`

**Interfaces:**
- Produz: `CompiledField.Indexado bool` no lugar de `Index IndexKind`.
  `IndexKind`, `IndexNone`, `IndexBitmap`, `IndexHash` e `indexKindDe`
  desaparecem.

- [ ] **Passo 1: os testes que falham**

```go
// TestIndexTrueEhAceito
func TestIndexTrueEhAceito(t *testing.T) {
	c := compilaOK(t, "campos:\n  node:\n    tipo: string\n    index: true\n")
	if !c.Campo("node").Indexado {
		t.Error("index: true não marcou o campo como indexado")
	}
}

// TestAsGrafiasAntigasSaoRecusadas. With um motor só, `hash` e `bitmap`
// construiriam a mesma coisa — e config que declara uma escolha que o motor
// ignora é o expireAfterWrite decorativo que o projeto já carrega como débito.
// Aceitá-las como sinônimo seria pior: o operador continuaria escrevendo uma
// distinção que não existe, e o WARN vira ruído que ninguém lê.
func TestAsGrafiasAntigasSaoRecusadas(t *testing.T) {
	for _, grafia := range []string{"hash", "bitmap"} {
		t.Run(grafia, func(t *testing.T) {
			_, err := schema.Compile([]byte("campos:\n  node:\n    tipo: string\n    index: " + grafia + "\n"))
			if err == nil {
				t.Fatalf("index: %s foi aceito", grafia)
			}
			// A mensagem tem de dizer o que ESCREVER, e não só o que está errado:
			// quem lê isso está com o servidor fora do ar.
			if !strings.Contains(err.Error(), "index: true") {
				t.Errorf("a mensagem não ensina a grafia nova: %v", err)
			}
		})
	}
}
```

E o gate de exit code, em `internal/cli`: dataset com `index: hash` sai com
**exit 3** (config recusada), não 1. Erro novo precisa de sentinela +
`errors.Is` em `codigoDoErro`, senão cai no default 1.

- [ ] **Passo 2 a 4:** rodar, implementar, rodar. `grep -rn "IndexBitmap\|IndexHash"
  --include=*.go internal/ | grep -v .claude/worktrees/` tem de sair vazio no
  fim.

- [ ] **Passo 5: documentar no MESMO commit**

`README.md` (tabela de configuração e de exit codes) e `CHANGELOG.md`, com a
frase que o operador precisa: *"`index: hash` e `index: bitmap` foram
substituídos por `index: true`; a subida recusa as grafias antigas com exit 3"*.

- [ ] **Passo 6: commitar**

```bash
git add -A && git commit -m "index: true passa a ser a única grafia

Com um motor só, hash e bitmap construiriam a mesma coisa. Aceitá-las como
sinônimo deixaria o operador escrevendo uma distinção que não existe mais —
é o expireAfterWrite decorativo que o projeto já carrega como débito, na
porta de entrada. A subida recusa com exit 3 e a mensagem ensina a grafia
nova, porque quem a lê está com o servidor fora do ar."
```

---

## Tarefa 11: unificar planner, executor e tipos de plano

**Arquivos:**
- Modificar: `internal/query/planner/planner.go`, `internal/query/planner/plan.go`,
  `internal/query/executor/store.go`, `internal/query/executor/candidate.go`,
  `internal/query/executor/executor.go`, `internal/storage/store.go`,
  `internal/testutil/fakestorage/fakestorage.go`
- Testar: `internal/query/planner/planner_test.go`, `internal/query/executor/*_test.go`

**Interfaces:**
- Produz:
  ```go
  // internal/storage
  func (s *Store) Lookup(field types.FieldID, v types.Value) (types.RowIDIterator, error)
  func (s *Store) Indexado(field types.FieldID) bool

  // internal/query/planner
  type Store interface { Indexado(field types.FieldID) bool }
  type IndexScanPlan struct { Field *binder.Field; Val types.Value }
  type IndexIntersectPlan struct { Terms []IndexTerm }
  type IndexUnionPlan struct { Terms []IndexTerm }   // já existe; perde o Kind
  ```
- Desaparecem: `LookupHash`, `LookupBitmap`, `HasBitmap`, `HasHash`,
  `HashLookuper`, `HashCapable`, `BitmapScanPlan`, `HashIndexPlan`,
  `BitmapIntersectPlan`, `IndexTerm.Kind`.

- [ ] **Passo 1: o teste que fixa o ganho**

```go
// TestDoisCamposIndexadosAgoraIntersectam. Antes a regra R2 devolvia UM plano
// de hash e parava, então `node='x' AND type=2` indexava node e revalidava type
// linha a linha. With um motor só a assimetria some, e ela some para MELHOR: os
// dois termos entram na interseção.
func TestDoisCamposIndexadosAgoraIntersectam(t *testing.T) {
	p := planejaOK(t, "SELECT * FROM d WHERE node = 'x' AND type = 2") // ambos indexados
	ip, ok := p.(*planner.IndexIntersectPlan)
	if !ok {
		t.Fatalf("plano = %T, quero *IndexIntersectPlan", p)
	}
	if len(ip.Terms) != 2 {
		t.Errorf("Terms = %d, quero 2", len(ip.Terms))
	}
}

// TestAPrioridadeDaKeyContinuaAcimaDoIndice: R1 antes de tudo, e a unificação
// não pode movê-la. `node='x' AND id='y'` continua point lookup.
func TestAPrioridadeDaKeyContinuaAcimaDoIndice(t *testing.T) {
	p := planejaOK(t, "SELECT * FROM d WHERE node = 'x' AND id = 'y'")
	if _, ok := p.(*planner.PointLookupPlan); !ok {
		t.Fatalf("plano = %T, quero *PointLookupPlan", p)
	}
}
```

- [ ] **Passo 2 a 4:** rodar, implementar, rodar. A regra vira **R1 primary →
  R2 índice (1 termo = `IndexScanPlan`, ≥2 = `IndexIntersectPlan`) → full scan**.
  A prioridade continua **fixa** e o planner continua **nunca falhando**: toda
  dúvida degrada para `FullScan`.

**Não** introduza ordenação por seletividade aqui. Ela ficou nomeada como
alavanca na §3.5 da spec e é mudança de natureza — trocar prioridade fixa por
decisão baseada em custo —, não de constante.

- [ ] **Passo 5: rodar tudo e commitar**

```bash
make test && make test-race && make test-integration && make lint
git add -A && git commit -m "Unifica a superfície de índice no planner e no executor

Com um motor só, LookupHash e LookupBitmap eram o mesmo método com dois
nomes, e as capabilities HasBitmap/HasHash/HashLookuper/HashCapable, quatro
formas de perguntar a mesma coisa.

A assimetria da regra de planejamento some para melhor: antes a regra do
hash devolvia um plano e parava, então node='x' AND type=2 indexava node e
revalidava type linha a linha. Agora os dois termos entram na interseção.

A prioridade continua fixa e o planner continua nunca falhando."
```

---

## Tarefa 12: fechar o débito 22

**Arquivos:**
- Modificar: `internal/storage/store.go` (as duas validações contra o manifest e
  os dois `Allow*Drop`), `internal/storage/indices_rebuild.go`
- Testar: `internal/storage/indices_backfill_test.go`
- Modificar: `docs/debitos.md`

- [ ] **Passo 1: os testes que falham**

```go
// TestAcrescentarIndiceEmDatasetPovoadoAgendaReconstrucao. A recusa existia
// porque não havia backfill. A reconstrução da Tarefa 7 É o backfill: manter a
// recusa seria ter a capacidade e negá-la.
func TestAcrescentarIndiceEmDatasetPovoadoAgendaReconstrucao(t *testing.T) {
	dir := t.TempDir()
	s := abreStoreIndexadoEm(t, dir, nil) // sem campo indexado
	poeLinhas(t, s, 500)
	s.Close()

	s2 := abreStoreIndexadoEm(t, dir, []string{"node"}) // acrescenta agora
	defer s2.Close()
	segs, err := s2.SegmentosPendentesDeReconstrucao()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) == 0 {
		t.Fatal("acrescentar índice num dataset povoado não agendou reconstrução")
	}
	rodaReconstrucaoAteEsvaziar(t, s2)
	if got := drenaLookup(t, s2, "node", "n1"); len(got) == 0 {
		t.Error("o backfill não populou o índice")
	}
}

// TestRemoverIndiceApagaAsSecoes: remover é só parar de escrever a seção e
// apagar as que existem. O AllowHashDrop/AllowBitmapDrop existia para tornar
// deliberada uma perda que agora não acontece.
func TestRemoverIndiceApagaAsSecoes(t *testing.T) { /* ... */ }
```

- [ ] **Passo 2 a 4:** rodar, implementar, rodar. `validaBitmapsContraManifest` e
  `validaHashesContraManifest` saem; `AllowBitmapDrop` e `AllowHashDrop` saem
  das `Options`.

- [ ] **Passo 5: `docs/debitos.md` no mesmo commit**

Marque o **22** como `~~...~~ **FECHADO**`, com o texto explicando o quê e o
porquê: a reconstrução por (segmento, campo) que a janela exigia é o mesmo
mecanismo do backfill, e o que era impossível ("exige ler todo record de todo
segmento") passou a ser um trabalho de fundo limitado por segmento, com
pacing e métrica. **A numeração é estável — não renumere nada.**

- [ ] **Passo 6: commitar**

```bash
git add -A && git commit -m "Fecha o débito 22: índice pode ser acrescentado a dataset povoado

A recusa existia por falta de backfill. A reconstrução que a janela exige É
o backfill — mesmo código, gatilho diferente —, então manter a recusa seria
ter a capacidade e negar o uso dela. Acrescentar um campo indexado agenda a
reconstrução dos segmentos; remover apaga as seções. Os dois opt-out saem
junto com as validações que eles destravavam."
```

---

# Fase D — a política

## Tarefa 13: a janela

**Arquivos:**
- Criar: `internal/storage/indices_window.go`
- Modificar: `internal/schema/history.go`, `internal/config/config.go`,
  `internal/storage/retention.go`
- Testar: `internal/storage/indices_window_test.go`,
  `internal/schema/history_test.go`

**Interfaces:**
- Produz:
  ```go
  type JanelaDeIndice struct {
      Duracao  time.Duration // 0 = infinita
      MaxBytes int64         // 0 = sem teto
  }
  func (j JanelaDeIndice) Infinita() bool
  func (s *Store) AplicaJanela(ctx context.Context, j JanelaDeIndice) (expulsos int, err error)
  func (s *Store) JanelaEfetiva() time.Duration
  ```

- [ ] **Passo 1: os testes de admissão de config**

```go
func TestAdmissaoDaJanela(t *testing.T) {
	casos := []struct {
		nome, yaml string
		recusa     bool
	}{
		{"ausente é infinita", "history:\n  maxBytes: 100MiB\n", false},
		{"index sem history", "index:\n  window: 6h\n", true},
		{"window zero é ambíguo", "history:\n  maxBytes: 100MiB\n  index:\n    window: 0\n", true},
		{"maxBytes sem window é válido", "history:\n  maxBytes: 100MiB\n  index:\n    maxBytes: 1GiB\n", false},
		{"window maior que a retenção passa com WARN", "history:\n  expireAfterWrite: 1h\n  index:\n    window: 7d\n", false},
	}
	// ... e o último caso confere que o WARN nomeado foi emitido: janela maior
	// que a retenção não está errada, é inócua — a retenção já limita antes —,
	// e recusá-la seria transformar redundância em queda de serviço.
}
```

- [ ] **Passo 2: os testes de comportamento**

```go
// TestEstreitarAJanelaExpulsaOsIndicesQueSairam
// TestAlargarAJanelaAgendaReconstrucao — é por isto que a reconstrução existe;
//   sem ela alargar não faria nada e a janela seria um botão de mão única.
// TestOTetoDeBytesVenceOPrazoDeclarado — espelha o maxBytes engolindo o
//   expireAfterWrite na retenção, e a JanelaEfetiva é quem mostra isso.
// TestFronteiraDaJanelaUsaORecordMaisNOVO — um segmento que ATRAVESSA a
//   fronteira mantém o índice. Usar o mais antigo tiraria o índice de dados
//   que a janela promete servir rápido.
```

- [ ] **Passo 3 a 5:** rodar, implementar, rodar. A expulsão roda **dentro da
  passada de retenção**, que já é de um minuto, já é exclusiva do histórico e já
  tem em mãos a lista de segmentos e os tamanhos. A fronteira por tempo reusa o
  `pread` de `primeiroRowIDDoSeguinte`: o record mais novo do segmento N é o
  imediatamente anterior ao primeiro do N+1.

- [ ] **Passo 6: commitar**

```bash
git add -A && git commit -m "A janela de indexação, em tempo com teto em bytes

A simetria com a retenção é deliberada: history.maxBytes limita o dado e
history.index.maxBytes limita o índice, e nos dois casos o teto de bytes é
a política forte que engole o prazo declarado. Quem entendeu uma entende a
outra sem documentação nova.

A fronteira usa o record mais NOVO do segmento: a janela promete consultar
rápido as últimas N horas, e um segmento que atravessa a fronteira precisa
manter o índice. É a assimetria espelhada da retenção, que só aposenta
quando o segmento está inteiramente coberto.

A expulsão roda na passada de retenção em vez de num laço próprio — ela já
é de um minuto, já é exclusiva do histórico e já tem a lista na mão."
```

---

## Tarefa 14: a goroutine de reconstrução

**Arquivos:**
- Modificar: `internal/storage/indices_rebuild.go`
- Testar: `internal/storage/indices_rebuild_test.go`

**Interfaces:**
- Produz:
  ```go
  func (s *Store) IniciaReconstrucao(ctx context.Context)
  func (s *Store) ParaReconstrucao()
  func (s *Store) ReconstrucaoAtiva() bool
  func (s *Store) SegmentosPendentesDeReconstrucao() ([]uint32, error)
  ```

- [ ] **Passo 1: os testes**

```go
// TestOMedidorSobeAntesDaGoroutine é a regra do §92: quem chamou tem direito de
// ver o estado que acabou de instalar. Subir o medidor DENTRO da goroutine
// deixa uma janela em que IniciaReconstrucao já voltou e ReconstrucaoAtiva
// ainda diz false.
// TestCloseCancelaEEsperaAReconstrucao — o Close aguarda antes do quiesce, como
//   a compaction e a retenção fazem.
// TestAReconstrucaoResolveTodosOsCamposPendentesNaMesmaLeitura — ler o segmento
//   duas vezes para dois campos pagaria o dobro pelo I/O, que é o custo inteiro
//   da operação.
// TestAReconstrucaoSoTomaOWmuNoRename — a leitura, a extração e a escrita do
//   temporário acontecem fora dele. Sem isto ela vira o terceiro laço de fundo
//   disputando o writer, que é o defeito que o ciclo anterior mediu.
```

- [ ] **Passo 2 a 4:** rodar, implementar, rodar. Molde exato de
  `IniciaCompaction`/`IniciaRetencao`: primeira passada imediata, medidor
  atômico observável, cancelamento por contexto, `Close` cancela e **aguarda**.
  Pacing pelo mesmo orçamento de ciclo de trabalho da compaction.

- [ ] **Passo 5: commitar**

```bash
git add -A && git commit -m "Sobe a goroutine de reconstrução do índice de segmento

Ela fecha o ciclo dos quatro gatilhos: dataset gravado por binário
anterior, janela alargada, crc divergente e campo acrescentado ao schema.
Todos são o mesmo trabalho — ler o segmento e emitir o índice dele —, e por
isso são um mecanismo só.

Os campos pendentes de um mesmo segmento são resolvidos na mesma leitura:
ler duas vezes para dois campos pagaria o dobro pelo I/O, que é o custo
inteiro da operação. O wmu só é tomado no rename."
```

---

# Fase E — o fecho

## Tarefa 15: disciplina de mutação

Não escreve funcionalidade nenhuma. Ela **prova que a suíte sabe reprovar**, que
é a doutrina do débito 53: *"gate que passa com a ferramenta ausente não é gate,
é carimbo"*. O produto desta tarefa é um registro, e o registro é o entregável.

**Arquivos:**
- Criar: `docs/mutacoes-indice-por-segmento.md`
- Modificar: os testes que reprovarem de menos

**Procedimento, por mutação:** aplicar no código de produção, rodar `make test`,
anotar **qual** teste reprovou e **com que mensagem**, `git checkout` da
mutação. Nada é commitado com mutação aplicada.

- [ ] **M1 — o `Set` volta a varrer a cadeia antes de inserir**

Em `wip.go`, antes do append, percorra a cadeia procurando `row` e retorne cedo
se achar.

Deve reprovar: `TestOCustoDoSetNaoAcompanhaOTamanhoDoSegmento`.
**Este é o sinal a procurar em todo o plano.** A mutação produz resultados
**certos** — não duplicar é semanticamente inócuo — e apenas lentos. Se algum
teste de CORRETUDE reprovar aqui, a suíte não sabe separar as duas coisas, e é
o teste de corretude que está errado, não a mutação.

- [ ] **M2 — remover o dedup do merge k-way** (`union.go`)

Deve reprovar: `TestUneMergeiaFaixasSobrepostasEDeduplica` e
`TestUneEhEquivalenteAUniaoOrdenada`. Se só o primeiro reprovar, o teste de
propriedade não está gerando sobreposição suficiente — aumente a densidade do
gerador.

- [ ] **M3 — `disjoint` devolve sempre `true`**

Deve reprovar: `TestUneNaoConcatenaQuandoAsFaixasSeTocam` e o de propriedade.

- [ ] **M4 — o `Open` adota o `.widx` de segmento não corrente**

Deve reprovar: `TestOWIPNaoEhAdotadoAtravesDeUmRestart`.

- [ ] **M5 — a aposentadoria não remove o `.sidx`**

Deve reprovar: `TestAAposentadoriaLevaOIndiceJunto`. **Atenção:** se ele passar,
provavelmente o teste checa só o `.dat`. É a mutação mais provável de escapar,
porque a órfã não muda nenhuma resposta — só faz o disco não voltar.

- [ ] **M6 — a reconstrução emite as postings na ordem de leitura, sem ordenar**

Remova o `slices.SortFunc` de `Writer.WriteSealed`. Deve reprovar:
`TestGoldenDoSelado`, `TestConstruirDoSegmentoDaOMesmoArquivoQueSelarOWIP` e
`TestOEscritorAgrupaOrdenaEDeduplicaAsPostings`.

- [ ] **M7 — crc divergente devolve erro em vez de degradar**

Deve reprovar: `TestCRCDivergenteNaoDerrubaAConsulta`.

- [ ] **Registrar e commitar**

`docs/mutacoes-indice-por-segmento.md` recebe uma tabela com mutação → teste que
reprovou → mensagem, **e o registro honesto do que a suíte NÃO alcança**, no
estilo que `durabilizaODiretorio` já usa: por exemplo, que nenhum teste deste
repositório afirma que um `fsync` levou a entrada de diretório ao meio físico.

```bash
git add -A && git commit -m "Registra as sete mutações do índice por segmento

Cada invariante nova precisa de prova de que quebrá-la deixa a suíte
vermelha. A mutação que importa é a primeira: fazer o Set varrer a cadeia
outra vez produz resultados CERTOS e lentos, e ela reprova no gate de custo
e em nenhum de corretude. É o sinal de que os testes sabem a diferença
entre um bug e uma regressão de desempenho.

O documento registra também o que a suíte não alcança, para que o limite
fique escrito em vez de presumido."
```

---

## Tarefa 16: métricas

**Arquivos:**
- Modificar: `internal/metrics/names.go`, `internal/metrics/metrics.go`,
  `internal/storage/indices*.go`, `internal/query/meta/`
- Testar: `internal/metrics/*_test.go`, `internal/storage/indices_metrics_test.go`

- [ ] **Passo 1: as séries, só pelas constantes de `names.go`**

| Série | Tipo | Labels |
|---|---|---|
| `nishidb_index_segmentos` | gauge | `estado` ∈ {selado, corrente, ausente, reconstruindo} |
| `nishidb_index_bytes` | gauge | — |
| `nishidb_index_janela_efetiva_segundos` | gauge | — |
| `nishidb_index_reconstrucoes_total` | counter | — |
| `nishidb_index_reconstrucao_ativa` | gauge | — |
| `nishidb_index_crc_falhas_total` | counter | — |
| `nishidb_index_segmentos_consultados_total` | counter | `modo` ∈ {indice, enumeracao} |

- [ ] **Passo 2: os testes**

```go
// TestAsSeriesDeIndiceNascemComZero: todas podem alarmar, e série que só
// aparece depois do primeiro evento faz absent() e rate() mentirem na janela em
// que o operador mais precisa deles — logo depois de uma subida.
// TestOsLabelsDeIndiceSaoConjuntosFechados: label nunca vem de entrada do
//   cliente, e `estado`/`modo` são enumerações do código.
// TestStatsEMetricsLeemAMesmaEstrutura: /v1/stats e /metrics nunca duplicam
//   contador.
```

- [ ] **Passo 3 a 5:** rodar, implementar, rodar, commitar.

```bash
git add -A && git commit -m "Publica as séries do índice por segmento

A degradação que derrubou o piloto foi silenciosa até o poll_blocked chegar
a 97,8%. As sete séries existem para que ela deixe de ser: quantos
segmentos respondem por índice e quantos por enumeração, quanto disco o
índice ocupa, e qual intervalo de tempo a janela está REALMENTE cobrindo —
que é o que mostra o teto de bytes assumindo o comando sobre o prazo
declarado.

Todas nascem com zero na subida, porque série que aparece depois do
primeiro evento faz absent() e rate() mentirem exatamente na janela em que
o operador mais precisa deles."
```

---

## Tarefa 17: documentação, baseline e aceite no piloto

**Arquivos:**
- Modificar: `docs/architecture.md`, `docs/debitos.md`, `docs/piloto-events.md`,
  `README.md`
- Criar: `docs/diagrams/indice_por_segmento.puml`, `docs/aceite-indice-por-segmento.md`
- Criar: `benchmark/baselines/segindex-<AAAAMMDD>.txt`

- [ ] **Passo 1: o diagrama**

`docs/diagrams/indice_por_segmento.puml` com os quatro estados do segmento e as
quatro fontes do `Lookup`. Incorporado no Markdown via `uml.nishisan.dev` com a
URL raw fixando **`main`** — fixar a branch de trabalho quebraria no merge.

**Validação:** não há `plantuml` local. Use o encoder deflate+base64 contra
`uml.nishisan.dev/png/` e afira o validador com uma linha que **não é sintaxe
nenhuma** — o débito 92 registra que `if` sem `endif` não falha, então um
validador aferido só com sintaxe malformada de fluxo aprova qualquer coisa.

- [ ] **Passo 2: `docs/architecture.md` — só número MEDIDO**

Nada de projeção. Cada número entra com origem em `benchmark/baselines/` ou em
`docs/aceite-indice-por-segmento.md`. A conta de disco da §1.3 da spec é
**projeção** e entra rotulada como tal, ou é substituída pelo medido no piloto.

- [ ] **Passo 3: `docs/debitos.md`**

- **22** — FECHADO (já feito na Tarefa 12; confira que ficou).
- **102** — **não fecha**: o `RWMutex` do `mmap.Region` saiu da rota crítica
  deste índice por construção, mas continua valendo para o `primary` e o
  `denseFile`. Reescreva a entrada dizendo exatamente isso, com o antes e o
  depois medidos. Fechá-lo seria afirmar que o problema sumiu, e ele mudou de
  dono.
- Débitos **novos a partir do 103**. Candidatos que este ciclo deixa nomeados:
  o crescimento sem teto do bitmap **não** é mais um deles (foi resolvido), mas
  **Roaring** (a alavanca de 392 MB da D4) e a **ordenação da interseção por
  seletividade** (§3.5) são, cada um com o número que os justificaria.
  **A numeração é estável: item novo entra no fim, nada é renumerado.**

- [ ] **Passo 4: baseline de micro-benchmark**

```bash
NISHIDB_BENCH_DIR=/caminho/em/DISCO/REAL make bench-micro
```
Disco real, nunca tmpfs — o `fallocate` é de verdade e são centenas de MiB por
rodada. No laptop a dispersão é de 15–27%: registre os **mínimos de 8–10
rodadas**, nunca a mediana de 3.

- [ ] **Passo 5: aceite no piloto — o critério afiado**

Suba pelo `scripts/nishidb-update.sh` (que faz rollback do par binário+config),
restaure `index: true` em `node` no histórico e deixe rodar ao menos um dia.

O paliativo alcançou o baseline **removendo** o índice. O aceite é **restaurar o
índice e manter os números**:

| | pós-paliativo (sem índice) | exigido |
|---|---:|---|
| Vazão | 1.636 ev/s | ≥ |
| CPU por evento | 528 µs | ≤ |
| RSS | 227 MB | ≤ |
| `poll_blocked` | 0 | = |

Método (o mesmo do ciclo anterior; `curl --noproxy '*'` sempre, porque o
`.bashrc` do root exporta proxy e o loopback responde 403):

```bash
# CPU por evento: delta de utime+stime cruzado com o contador, em 60 s
ssh root@10.200.20.218 'read _ _ _ _ _ _ _ _ _ _ _ _ _ u1 s1 _ < /proc/$(pgrep -x nishidb)/stat
  e1=$(curl -s --noproxy "*" 127.0.0.1:9142/metrics | awk "/^nishidb_ingest_events_total/{print \$2}")
  sleep 60
  read _ _ _ _ _ _ _ _ _ _ _ _ _ u2 s2 _ < /proc/$(pgrep -x nishidb)/stat
  e2=$(curl -s --noproxy "*" 127.0.0.1:9142/metrics | awk "/^nishidb_ingest_events_total/{print \$2}")
  echo "us/evento: $(( (u2-u1+s2-s1)*10000 / (e2-e1) ))"'

# profile de CPU de 45 s, antes e depois
curl -s --noproxy '*' -o antes.pprof 'http://127.0.0.1:9144/debug/pprof/profile?seconds=45'
```

**Aceite secundário, que o desenho promete:** com a tabela histórica em regime,
`nishidb_index_bytes` **para de crescer**. Hoje o índice é o único componente
que não para, mesmo com a retenção funcionando e o dataset estável em 250 GiB.
Meça ao longo de pelo menos um ciclo completo de retenção.

- [ ] **Passo 6: `docs/piloto-events.md` — a sétima medição**

No formato que o documento já usa: **o que foi medido, não o que se esperava.**
Se algum número não bater, ele entra assim mesmo, com a explicação. Registro de
rodada não é reescrito.

- [ ] **Passo 7: verificação final e commit**

```bash
make build && make test && make test-race && make test-integration && make lint
make test-jdbc && make test-python        # os gates leem os manifestos
```

`make lint` sai != 0 enquanto a baseline > 0 — é o esperado. Confira que a
**contagem** bate com `.golangci-baseline`.

```bash
git add -A && git commit -m "Documenta o índice por segmento e registra a sétima medição

O aceite não é 'melhorar em relação ao estado quebrado': o paliativo já
tinha alcançado 1.636 ev/s removendo o índice de node do histórico. O
aceite é devolver a consulta indexada sem pagar por ela.

O débito 102 não fecha. O RWMutex do mmap.Region saiu da rota crítica deste
índice por construção — arquivo selado não cresce, então não há o que
proteger —, mas continua valendo para o primary e o denseFile. Fechá-lo
seria afirmar que o problema sumiu, e ele mudou de dono."
```

---

## Autorrevisão do plano

Feita depois de escrever tudo, contra a spec.

**1. Cobertura da spec.**

| Seção da spec | Tarefa |
|---|---|
| §1.1 forma WIP | 3 |
| §1.2 forma selada | 1, 2, 4 |
| §1.3 conta de disco | 17 (medida) |
| §1.4 descritores e mapeamentos | 2 |
| §1.5 corrupção | 2 (leitura), 14 (reconstrução) |
| §2.1–2.3 estados, selagem, purga | 6 |
| §2.4 reconstrução | 7 (mecanismo), 14 (goroutine) |
| §2.5 o WIP não atravessa restart | 7 |
| §2.6 débito 22 | 12 |
| §3.1 superfície única | 11 |
| §3.2 fonte por segmento | 8 |
| §3.3 concatenação × merge | 5 |
| §3.4 degradação invisível | 8 (comportamento), 16 (métrica) |
| §3.5 o que não entra | 11 (registrado como não-alvo) |
| §4 a janela | 13 |
| §5.1–5.3 ordem de escrita e durabilidade | 6, 7 |
| §5.4 manifest v3 | 9 |
| §6.1 gate de custo | 3 |
| §6.2 gates de alocação | 2, 3 |
| §6.3 goldens | 1, 7 |
| §6.4 testes de propriedade | 5, 8 |
| §6.5 mutações | 15 |
| §6.6 métricas | 16 |
| §6.7 guarda de cardinalidade | 16 (vira informação) |
| §7 aceite | 17 |

Sem lacuna.

**2. Placeholders.** Nenhum "TBD", "TODO" ou "tratar erros apropriadamente". As
duas passagens mais curtas são deliberadas e nomeadas: os corpos de teste
elididos com `// ...` nas Tarefas 8, 13 e 14 vêm **sempre** acompanhados da
propriedade exata a afirmar e do porquê ela existe — o executor sabe o que
provar, e a montagem do fixture segue os helpers já nomeados nas tarefas
anteriores.

**3. Consistência de tipos.** `Fonte` é declarada na Tarefa 2 e consumida nas 5
e 8 com a mesma assinatura. `Writer.Adiciona(campo, h, row)` é a mesma nas 1,
4 e 7. `SealedPath`/`WIPPath`/`ListWIP` são usados nas 4, 6, 7 e 8 com a
forma declarada na 1 e na 3. `CamposIndexados()` é produzida na 6 e consumida na
7 e na 12.

**4. Ponto de atenção para quem executar.** A Tarefa 8 é a única em que a suíte
inteira roda contra o motor novo **sem nenhum teste editado**. Se ela ficar
vermelha, a tentação vai ser ajustar o teste. Não ajuste antes de separar as
duas causas possíveis: o motor diverge do contrato, ou o teste prendia um
detalhe do arquivo antigo. Só a segunda justifica mexer no teste.
