# Colunas congeladas e TTL condicional — Plano de Implementação

> **Para executores agênticos:** SUB-SKILL OBRIGATÓRIA: use `superpowers:subagent-driven-development` (recomendado) ou `superpowers:executing-plans` para executar tarefa a tarefa. Os passos usam checkbox (`- [ ]`).

**Goal:** Dar ao schema duas capacidades novas — colunas que congelam o primeiro valor visto e nunca mudam, e expiração cujo prazo é escolhido por uma condição sobre o próprio documento, cancelável quando a condição deixa de valer.

**Architecture:** Uma primitiva compartilhada — entrada de patch com política de escrita — atende as duas. As colunas congeladas ganham um ramo no merge que preserva o valor anterior. O TTL condicional avalia um `WHERE` na ingestão, grava a duração resultante como coluna de sistema `_expiresIn`, e o storage a projeta numa tabela densa que a conta de prazo passa a consultar. O avaliador de predicado sai do executor para um pacote compartilhado, para que `type = 2` na regra e `type = 2` na consulta sejam o mesmo código.

**Tech Stack:** Go, sem dependência nova. `gopkg.in/yaml.v3` (já presente), mmap próprio (`internal/storage/mmap`), testes com `testing` puro.

**Spec:** [planning/colunas-congeladas-e-ttl-condicional.md](colunas-congeladas-e-ttl-condicional.md) — leia antes da primeira tarefa. O plano argumenta a partir dela.

## Global Constraints

- **Branch:** `feature/colunas-congeladas-e-ttl-condicional`. Não commitar em `main`.
- **Gate de testes:** `make test` verde antes de cada commit. Após refatoração, também `make test-race`.
- **Gate de lint:** `make lint`. A contagem tem de **bater exatamente** com `.golangci-baseline` — ficar ABAIXO da baseline também quebra o gate. Se a contagem mudar, iguale o arquivo antes de abrir PR.
- **Granularidade de commit:** um commit por **tarefa**, não por passo. Este projeto aceita commits por bloco.
- **Mensagens de commit:** voz do time, imperativo, PT-BR. Sem referência a agente, IA ou ferramenta.
- **Sem fallback para legado:** onde a spec diz que uma forma antiga sai, ela sai. Não manter as duas.
- **Idioma do código:** identificadores e comentários em PT-BR, seguindo o que já está em cada arquivo. Comentários explicam o PORQUÊ, não o quê — é o padrão da base inteira.
- **Duração mínima declarável:** 1 minuto (`schema.ResolucaoDaExpiracao`). O literal `never` é a única exceção e significa imortal.
- **Sentinela do índice de prazos:** `PrazoImortal = 1`. Zero significa DESCONHECIDO, nunca imortal.

---

## Estrutura de arquivos

**Criados**

| Arquivo | Responsabilidade |
|---|---|
| `internal/predicate/predicate.go` | `Slot`, `Coleta`, `Programa`, `Compila`, `Avalia` — o avaliador, movido do executor |
| `internal/predicate/predicate_test.go` | Testes do avaliador, movidos de `predicate_prova_test.go` |
| `internal/schema/frozen.go` | Validação e compilação de `frozenFrom` |
| `internal/schema/frozen_test.go` | Testes de validação de `frozenFrom` |
| `internal/schema/ttl_rule.go` | Validação e compilação da lista `ttl:` |
| `internal/schema/ttl_rule_test.go` | Testes das regras |
| `internal/materializer/frozen.go` | Terceira passada do patch: captura das congeladas |
| `internal/materializer/frozen_test.go` | Testes da captura e do congelamento |
| `internal/materializer/deadline.go` | Avaliação das regras na ingestão e emissão de `_expiresIn` |
| `internal/materializer/deadline_test.go` | Testes da avaliação |
| `internal/storage/deadlines_test.go` | Testes do índice de prazos e da conta |

**Modificados**

| Arquivo | Mudança |
|---|---|
| `internal/query/executor/collect.go` | Esvaziado; `slot`/`coleta` viram alias do pacote novo |
| `internal/query/executor/predicate.go` | Esvaziado; `programa`/`compila` viram alias do pacote novo |
| `internal/query/parser/parser.go` | `ParseCondicao` |
| `internal/query/binder/binder.go` | `BindCondicao` + `BoundCondicao` |
| `internal/schema/spec.go` | `FieldSpec.FrozenFrom`; `TTLSpec` vira `[]RegraTTLSpec`; `_expiresIn` em `nomesDeSistema` |
| `internal/schema/compile.go` | `CompiledField.FrozenFrom`; `Compiled.Congeladas`, `Compiled.RegrasTTL`, `Compiled.Prazo`; remoção de `IsTTL` |
| `internal/materializer/patch.go` | `patchEntry.congelada`; chamadas das duas passadas novas |
| `internal/materializer/merge.go` | Ramo congelado |
| `internal/materializer/materializer.go` | Passa `prevPayload` para a avaliação de prazo |
| `internal/storage/rowindex/rowindex.go` | `Prazos` + `PrazoImortal` |
| `internal/storage/store.go` | Options, abertura do índice, `applyRecord`, `prazoVencido` |
| `internal/storage/expiry.go` | Conta do prazo, pré-filtro de tombstone |
| `internal/cli/cli.go`, `internal/cli/offsets.go`, `internal/server/server.go` | Montagem das Options |

---

# FASE A — Extrair o avaliador

Refatoração pura. Critério de aceite: nenhuma asserção de teste existente muda.

### Task 1: Pacote `internal/predicate`

**Files:**
- Create: `internal/predicate/predicate.go`, `internal/predicate/predicate_test.go`
- Modify: `internal/query/executor/collect.go` (arquivo inteiro), `internal/query/executor/predicate.go` (arquivo inteiro), `internal/query/executor/executor.go`, `internal/query/executor/sort.go`, `internal/query/executor/topn.go`, `internal/query/executor/row.go`

**Interfaces:**
- Produces:
  ```go
  package predicado

  type Slot struct {
      ID      types.FieldID
      Val     types.Value
      Present bool
  }
  func NovosSlots(ids []types.FieldID) []Slot
  func Coleta(slots []Slot, fr *encoding.FieldReader, payload []byte) error

  type Programa struct{ /* prog []instr */ }
  func Compila(e binder.BoundExpr, nSlots int) (*Programa, error)

  // Avalia roda o programa. mismatches é incrementado a cada campo cujo tipo
  // físico não bate com o do literal — o executor o publica como métrica, a
  // ingestão o ignora passando nil.
  func (p *Programa) Avalia(slots []Slot, mismatches *int64) bool

  var ErrProgramaInvalido = errors.New("predicado: programa inválido")
  ```

- [ ] **Passo 1: mover os arquivos preservando o histórico**

```bash
git mv internal/query/executor/collect.go internal/predicate/predicate.go
git mv internal/query/executor/predicate.go internal/predicate/program.go
git mv internal/query/executor/predicate_prova_test.go internal/predicate/predicate_test.go
```

Ajuste `package executor` → `package predicado` no topo dos três arquivos.

`candidate_internal_test.go` fica onde está: confira com
`grep -l 'programa\|slot' internal/query/executor/*_test.go` quais outros testes internos
tocam os identificadores movidos, e mova SÓ esses.

- [ ] **Passo 2: exportar os identificadores movidos**

Renomeie no pacote novo: `slot`→`Slot` (e os campos `id`/`val`/`present`→`ID`/`Val`/`Present`), `novosSlots`→`NovosSlots`, `coleta`→`Coleta`, `programa`→`Programa`, `compila`→`Compila`, `eval`→`Avalia`. `instr`, `opKind`, `tarefa`, `testa`, `emite`, `ordemInt`, `ordemFloat`, `ordemBytes` continuam privados.

Troque `ErrInvalidPlan` (que ficou no executor) por um erro do próprio pacote:

```go
// ErrProgramaInvalido marca predicado que o binder produziu mal formado.
// É bug de plan-time, não entrada do usuário: compilar em silêncio um
// programa quebrado devolveria resultado vazio varrendo o dataset inteiro.
var ErrProgramaInvalido = errors.New("predicado: programa inválido")
```

- [ ] **Passo 3: trocar `*contadores` por `*int64` na assinatura**

`contadores` é do executor e a ingestão não tem os mesmos. `testa` só usa `cnt.typeMismatch++`, então a dependência inteira some:

```go
// Avalia roda o programa. Sem alocação, sem chamada recursiva, sem pilha.
//
// mismatches conta os campos cujo tipo físico não bate com o do literal —
// payload heterogêneo na mesma coluna. É ponteiro e aceita nil porque os dois
// chamadores têm necessidades opostas: o executor o publica como métrica de
// query, e a ingestão avalia uma regra por evento e não tem onde pendurar um
// contador por avaliação.
func (p *Programa) Avalia(slots []Slot, mismatches *int64) bool

func (in *instr) testa(slots []Slot, mismatches *int64) bool {
	// ...
	if s.Val.Type != in.want {
		if mismatches != nil {
			*mismatches++
		}
		return false
	}
	// ...
}
```

- [ ] **Passo 4: religar o executor**

No `executor`, crie os apelidos locais para não reescrever os laços de `sort.go`/`topn.go`/`row.go`:

```go
// collect.go, agora com 4 linhas
package executor

import "github.com/nishisan-dev/nishidb/internal/predicate"

// O avaliador mora em internal/predicate desde a extração: `type = 2` numa
// regra de TTL e `type = 2` num WHERE têm de ser o MESMO código, senão as
// duas divergem em silêncio nos casos de coerção.
type slot = predicado.Slot

var novosSlots = predicado.NovosSlots
```

Nos usos de campo, `s.id`/`s.val`/`s.present` viram `s.ID`/`s.Val`/`s.Present`. No ponto onde o executor chama `p.eval(slots, cnt)`, passe `&cnt.typeMismatch`.

- [ ] **Passo 5: compilar e rodar a suíte inteira**

Run: `go build ./... && make test`
Expected: PASS, com **zero** asserção alterada. Qualquer teste que precise mudar de expectativa é sinal de que a extração mudou comportamento — pare e investigue.

- [ ] **Passo 6: rodar com detector de corrida e o bench de query**

Run: `make test-race`
Run: `go test ./internal/query/executor/ -bench=. -benchmem -run=XXX`
Expected: sem regressão de alocação por linha. O avaliador não passou a alocar.

- [ ] **Passo 7: lint e commit**

```bash
make lint
git add -A
git commit -m "Extrai o avaliador de predicado para um pacote próprio"
```

---

# FASE B — Colunas congeladas

### Task 2: `frozenFrom` no spec e suas validações

**Files:**
- Create: `internal/schema/frozen.go`, `internal/schema/frozen_test.go`
- Modify: `internal/schema/spec.go` (`FieldSpec`, laço de validação em `spec.go:761`)

**Interfaces:**
- Produces: `FieldSpec.FrozenFrom string` (yaml `frozenFrom`); `schema.OrigemRelogio = "@writeTime"`; `func validaCongeladas(fields map[string]FieldSpec, keyField, modo string) error`

- [ ] **Passo 1: escrever os testes de recusa**

```go
// internal/schema/frozen_test.go
package schema_test

func TestFrozenFromComTypeEhRecusado(t *testing.T) {
	_, err := carrega(t, `
version: 1
name: d
materialization: {key: {source: payload, field: id, type: string}, mode: merge}
schema:
  fields:
    id: {type: string}
    sev: {type: int64}
    origSev: {type: int64, frozenFrom: sev}
`)
	exigeErroContendo(t, err, "frozenFrom", "type")
}

func TestFrozenFromParaCampoInexistenteEhRecusado(t *testing.T)   { /* frozenFrom: naoExiste */ }
func TestFrozenFromParaSiMesmaEhRecusado(t *testing.T)            { /* origSev: {frozenFrom: origSev} */ }
func TestFrozenFromEmCadeiaEhRecusado(t *testing.T)               { /* b: {frozenFrom: a}, c: {frozenFrom: b} */ }
func TestFrozenFromParaAKeyEhRecusado(t *testing.T)               { /* frozenFrom: id */ }
func TestFrozenFromEmModoReplaceEhRecusado(t *testing.T)          { /* mode: replace */ }
func TestFormatJuntoDeFrozenFromEhRecusado(t *testing.T)          { /* format: "yyyy" */ }
func TestFrozenFromParaASequenceEhAceito(t *testing.T)            { /* não deve dar erro */ }
func TestFrozenFromRelogioEhAceito(t *testing.T)                  { /* frozenFrom: "@writeTime" */ }
```

Reaproveite os helpers já usados em `internal/schema/expiry_test.go` para carregar YAML de string e conferir a mensagem; se os helpers forem privados àquele arquivo, extraia-os para um `helpers_test.go` no mesmo pacote de teste como parte desta tarefa.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/schema/ -run TestFrozenFrom -v`
Expected: FAIL — `frozenFrom` ainda nem é campo do struct, então o YAML decodifica em silêncio e nenhum erro sai.

- [ ] **Passo 3: acrescentar o campo ao `FieldSpec`**

Em `internal/schema/spec.go`, no struct `FieldSpec` (junto de `Format`):

```go
	// FrozenFrom nomeia a ORIGEM de uma coluna congelada: outra coluna
	// declarada, ou OrigemRelogio. A coluna recebe o primeiro valor que a
	// origem tiver e nunca mais muda.
	//
	// A tag é explícita como todas as outras: sem ela o yaml.v3 procuraria
	// "frozenfrom" e a chave camelCase do arquivo não casaria com nada, em
	// silêncio — este struct é decodificado sem KnownFields.
	FrozenFrom string `yaml:"frozenFrom"`
```

- [ ] **Passo 4: escrever as validações**

```go
// internal/schema/frozen.go
package schema

// OrigemRelogio é a origem especial que entrega o instante da escrita em vez
// do valor de outra coluna. É o que faz um `createdAt` sair da mesma máquina
// que o `originalSeverity`, sem caso especial em lugar nenhum.
const OrigemRelogio = "@writeTime"

// validaCongeladas confere as regras de frozenFrom que dependem de OUTROS
// campos, e por isso não cabem no laço por campo: cadeia, autorreferência e
// origem inexistente só são decidíveis com o mapa inteiro na mão.
func validaCongeladas(fields map[string]FieldSpec, keyField, modo string) error {
	for nome, f := range fields {
		if f.FrozenFrom == "" {
			continue
		}
		if modo == ModeReplace {
			// No replace o estado anterior é descartado inteiro e encodePatch
			// nem o lê. Uma coluna que sobrevivesse ao replace contradiria o
			// que replace significa, e sustentá-la custaria uma leitura de
			// record por evento no modo cuja razão de existir é não pagá-la.
			return specErrf("campo %q: frozenFrom não se aplica a mode: replace — "+
				"no replace o estado anterior é descartado inteiro", nome)
		}
		if f.Type != "" {
			return specErrf("campo %q: frozenFrom e type juntos — o tipo é herdado da origem %q",
				nome, f.FrozenFrom)
		}
		if f.Format != "" {
			return specErrf("campo %q: format não se aplica a frozenFrom — "+
				"o formato é do parsing da origem", nome)
		}
		if f.FrozenFrom == OrigemRelogio {
			continue
		}
		if f.FrozenFrom == nome {
			return specErrf("campo %q: frozenFrom aponta para si mesmo", nome)
		}
		if f.FrozenFrom == keyField {
			return specErrf("campo %q: frozenFrom não aceita a key %q — "+
				"a key identifica a entidade e já é constante", nome, keyField)
		}
		origem, existe := fields[f.FrozenFrom]
		if !existe {
			return specErrf("campo %q: frozenFrom nomeia %q, que não está declarado em schema.fields",
				nome, f.FrozenFrom)
		}
		if origem.FrozenFrom != "" {
			// A cadeia faria a ordem de avaliação virar contrato implícito, e
			// o valor dependeria de qual das duas foi capturada primeiro.
			return specErrf("campo %q: frozenFrom aponta para %q, que também é congelada — "+
				"cadeia de congelamento não é suportada", nome, f.FrozenFrom)
		}
	}
	return nil
}
```

No laço de validação de campos (`internal/schema/spec.go:761`), o `isValidTypeName(f.Type)` recusa tipo vazio. Uma coluna congelada não declara tipo, então **pule a checagem de tipo quando `f.FrozenFrom != ""`**:

```go
		if f.FrozenFrom == "" && !isValidTypeName(f.Type) {
			return nil, specErrf("campo %q: tipo %q inválido", name, f.Type)
		}
```

E chame `validaCongeladas` depois do laço, onde `raw.Materialization.Key.Field` e `Mode` já estão resolvidos.

- [ ] **Passo 5: rodar os testes**

Run: `go test ./internal/schema/ -run TestFrozenFrom -v`
Expected: PASS, todos.

- [ ] **Passo 6: rodar o pacote inteiro e commitar**

Run: `go test ./internal/schema/`
Expected: PASS — nenhum dataset existente passou a ser recusado.

```bash
make lint
git add internal/schema/
git commit -m "Valida frozenFrom na carga da spec"
```

---

### Task 3: `FrozenFrom` no schema compilado

**Files:**
- Modify: `internal/schema/compile.go` (`CompiledField`, `Compiled`, laço de compilação em `compile.go:490`)
- Test: `internal/schema/frozen_test.go`

**Interfaces:**
- Consumes: `FieldSpec.FrozenFrom`, `OrigemRelogio` (Task 2)
- Produces:
  ```go
  // CompiledField
  FrozenFrom *CompiledField // origem do valor congelado; nil se não é congelada
  DoRelogio  bool           // congelada cuja origem é o relógio do writer

  // Compiled
  Congeladas []*CompiledField // congeladas em ordem de FieldID
  ```

- [ ] **Passo 1: escrever o teste**

```go
func TestCompilaCongeladaHerdaOTipoDaOrigem(t *testing.T) {
	c := compila(t, `... sev: {type: int64}, origSev: {frozenFrom: sev} ...`)
	f, ok := c.FieldByName("origSev")
	if !ok {
		t.Fatal("origSev não compilou")
	}
	if f.Type != types.TypeInt64 {
		t.Errorf("tipo herdado = %v, quero int64", f.Type)
	}
	if f.FrozenFrom == nil || f.FrozenFrom.Name != "sev" {
		t.Error("a origem não foi religada")
	}
	if f.IsSystem {
		t.Error("congelada virou campo de sistema; ela é dado do usuário e tem de sair no SELECT *")
	}
	if len(c.Congeladas) != 1 || c.Congeladas[0] != f {
		t.Error("a lista pré-computada não tem a congelada")
	}
}

func TestCompilaCongeladaDeOrigemAutoTambemEhAuto(t *testing.T) { /* sev: {type: auto} */ }
func TestCompilaCongeladaDoRelogioEhTimestamp(t *testing.T)     { /* frozenFrom: "@writeTime" */ }
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/schema/ -run TestCompilaCongelada -v`
Expected: FAIL — `FrozenFrom` não existe em `CompiledField`.

- [ ] **Passo 3: acrescentar os campos e religar**

Em `CompiledField` (junto de `Parent`/`Path`):

```go
	// FrozenFrom é a coluna de onde o valor congelado vem; nil quando esta
	// não é congelada. DoRelogio marca a congelada cuja origem é o relógio do
	// writer, que não tem CompiledField para apontar.
	//
	// A congelada é uma coluna NORMAL em tudo o que importa ao binder, ao
	// planner e ao encoder — tem FieldID, tipo e índice, aparece no SELECT *
	// e no /v1/metadata. O par governa uma coisa só: quem escreve o valor.
	FrozenFrom *CompiledField
	DoRelogio  bool
```

No laço de compilação, **depois** de todos os campos terem `CompiledField` (a origem pode aparecer depois da congelada na ordem alfabética), uma segunda passada religa e herda:

```go
	// A religação roda DEPOIS do laço porque a origem pode ter sido compilada
	// depois da congelada — os nomes são percorridos em ordem alfabética, e
	// `originalSeverity` vem antes de `severity`.
	for name, spec := range s.Schema.Fields {
		if spec.FrozenFrom == "" {
			continue
		}
		cf, ok := c.byName[name]
		if !ok {
			continue // excluída do dataset
		}
		if spec.FrozenFrom == OrigemRelogio {
			cf.Type, cf.DoRelogio = types.TypeTimestamp, true
		} else {
			origem, ok := c.byName[spec.FrozenFrom]
			if !ok {
				return nil, specErrf("campo %q: origem %q não compilou (excluída?)", name, spec.FrozenFrom)
			}
			cf.FrozenFrom = origem
			cf.Type, cf.IsAuto = origem.Type, origem.IsAuto
		}
		c.Congeladas = append(c.Congeladas, cf)
	}
	sort.Slice(c.Congeladas, func(i, j int) bool { return c.Congeladas[i].ID < c.Congeladas[j].ID })
```

`Congeladas` é pré-computada e ordenada pela mesma razão que `Derivadas`: o materializer a consulta por evento e não pode varrer o mapa no caminho quente.

- [ ] **Passo 4: rodar**

Run: `go test ./internal/schema/ -v`
Expected: PASS

- [ ] **Passo 5: lint e commit**

```bash
make lint
git add internal/schema/
git commit -m "Compila a coluna congelada herdando o tipo da origem"
```

---

### Task 4: Captura na ingestão

**Files:**
- Create: `internal/materializer/frozen.go`, `internal/materializer/frozen_test.go`
- Modify: `internal/materializer/patch.go` (struct em `:25`, guarda em `:101`, chamada em `:208`)

**Interfaces:**
- Consumes: `Compiled.Congeladas`, `CompiledField.FrozenFrom`, `CompiledField.DoRelogio` (Task 3)
- Produces: `patchEntry.congelada bool`; `func (b *PatchBuffer) capturaCongeladas(c *schema.Compiled, agora time.Time)`

- [ ] **Passo 1: escrever os testes**

```go
// internal/materializer/frozen_test.go
func TestCongeladaCapturaDoEvento(t *testing.T) {
	// evento {id:"a", sev:5} → payload tem origSev=5
}
func TestCongeladaNaoEmiteQuandoAOrigemNaoVeioNoEvento(t *testing.T) {
	// evento {id:"a"} → nenhuma entry de origSev no patch
}
func TestCongeladaNaoEmiteQuandoAOrigemVeioNull(t *testing.T) {
	// evento {id:"a", sev:null} → nenhuma entry de origSev
}
func TestEventoQueTrazONomeDaCongeladaEhRecusado(t *testing.T) {
	// evento {id:"a", sev:5, origSev:9} → erro schema.ErrUnknownField
}
func TestCongeladaDoRelogioUsaOMesmoInstanteQueOWrittenAt(t *testing.T) {
	// as duas entries têm o mesmo I64
}
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/materializer/ -run TestCongelada -v`
Expected: FAIL — `capturaCongeladas` não existe.

- [ ] **Passo 3: acrescentar o bit ao `patchEntry`**

Em `internal/materializer/patch.go:25`:

```go
type patchEntry struct {
	field  *schema.CompiledField
	isNull bool
	// congelada faz o merge PRESERVAR o valor anterior em vez de sobrescrever.
	// É a terceira política de escrita, ao lado de "sobrescreve" (default) e
	// "remove" (isNull).
	congelada bool
	val       types.Value
}
```

- [ ] **Passo 4: escrever a captura**

```go
// internal/materializer/frozen.go
package materializer

import (
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// capturaCongeladas emite as entries das colunas congeladas a partir do que o
// EVENTO trouxe.
//
// Roda depois da extração das derivadas — para que uma congelada possa ter
// origem numa coluna aninhada, cujo valor só existe depois daquela passada — e
// antes da ordenação, para que participe do sort e do dedup como qualquer
// outra entry.
//
// Origem ausente do evento, ou presente com null explícito, NÃO emite nada: a
// coluna fica vazia e é preenchida no primeiro evento que trouxer a origem. É
// a semântica de "primeiro valor visto", e é o que torna a feature tolerante a
// fonte que manda evento parcial.
//
// A busca da origem é linear sobre as entries já montadas. Com uma ou duas
// congeladas sobre algumas dezenas de entries é ruído; um dataset que declare
// muitas e cujo bench acuse deve guardar o índice da entry durante o parsing,
// não trocar isto por um mapa alocado por evento.
func (b *PatchBuffer) capturaCongeladas(c *schema.Compiled, agora time.Time) {
	for _, cf := range c.Congeladas {
		if cf.DoRelogio {
			b.entries = append(b.entries, patchEntry{
				field:     cf,
				val:       types.Value{Type: types.TypeTimestamp, I64: agora.UnixMilli()},
				congelada: true,
			})
			continue
		}
		for i := range b.entries {
			e := &b.entries[i]
			if e.field != cf.FrozenFrom || e.isNull {
				continue
			}
			b.entries = append(b.entries, patchEntry{
				field: cf, val: e.val, congelada: true,
			})
			break
		}
	}
}
```

O `val` aponta para a mesma janela do arena que a entry de origem. É seguro e não custa cópia: o arena vive até o próximo evento, e `encoding.AppendField` copia os bytes na serialização.

- [ ] **Passo 5: chamar a captura e estender a guarda**

Em `patch.go`, logo depois do bloco `extraiDerivadas()` e **antes** do bloco `if c.Escrita != nil` (linha 208):

```go
	// As congeladas entram depois das derivadas e antes da ordenação: podem
	// ter origem numa coluna aninhada, e precisam participar do sort e do
	// dedup como qualquer outra entry.
	b.capturaCongeladas(c, agora)
```

E a guarda de `patch.go:101` passa a cobrir as duas famílias:

```go
		if found && (cf.IsSystem || cf.FrozenFrom != nil || cf.DoRelogio) {
			// O evento não pode escrever nem o relógio do writer nem uma
			// coluna congelada. No primeiro caso o produtor controlaria quando
			// a entidade expira; no segundo, decidiria qual é o valor
			// "original" — e a coluna passaria a mentir sem erro nenhum.
			//
			// É diferente da coluna derivada, ignorada em silêncio logo
			// abaixo: lá o nome de topo pode existir legitimamente no evento
			// com outro significado, aqui o nome foi inventado pelo operador
			// para esta finalidade.
			return fmt.Errorf("%w: campo %q é reservado ao writer e não pode vir no evento",
				schema.ErrUnknownField, f.Key)
		}
```

- [ ] **Passo 6: rodar**

Run: `go test ./internal/materializer/ -run TestCongelada -v`
Expected: PASS. `TestEventoQueTrazONomeDaCongeladaEhRecusado` também.

- [ ] **Passo 7: lint e commit**

```bash
make lint
git add internal/materializer/
git commit -m "Captura o valor da coluna congelada na ingestão"
```

---

### Task 5: O ramo congelado no merge

**Files:**
- Modify: `internal/materializer/merge.go:66`
- Test: `internal/materializer/merge_test.go`

**Interfaces:**
- Consumes: `patchEntry.congelada` (Task 4)

- [ ] **Passo 1: escrever o teste no estilo do arquivo**

```go
func TestMergeEntryCongeladaPreservaOAnterior(t *testing.T) {
	prev := payload(t, campo{1, i64(5)})               // origSev=5
	patch := []patchEntry{{field: f(1), val: i64(9), congelada: true}}
	out, err := mergeRecords(prev, patch, nil)
	if err != nil {
		t.Fatal(err)
	}
	exigeCampos(t, out, campo{1, i64(5)}) // o 9 foi descartado
}

func TestMergeEntryCongeladaEscreveQuandoOAnteriorNaoTem(t *testing.T) {
	prev := payload(t)                                  // vazio
	patch := []patchEntry{{field: f(1), val: i64(9), congelada: true}}
	out, _ := mergeRecords(prev, patch, nil)
	exigeCampos(t, out, campo{1, i64(9)})
}

func TestEncodePatchIgnoraOBitCongelada(t *testing.T) {
	// caminho do create: sem estado anterior, a congelada é escrita normalmente
	out, _ := encodePatch([]patchEntry{{field: f(1), val: i64(9), congelada: true}}, nil)
	exigeCampos(t, out, campo{1, i64(9)})
}
```

Use os helpers já existentes em `merge_test.go`; se `campo`/`payload`/`exigeCampos` tiverem outro nome no arquivo, siga o nome de lá.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/materializer/ -run TestMergeEntryCongelada -v`
Expected: FAIL no primeiro teste — o merge sobrescreve, devolve 9 e não 5.

- [ ] **Passo 3: acrescentar o ramo**

Em `mergeRecords`, no bloco de `merge.go:66`:

```go
		if pok && pid == id {
			if e.congelada {
				// Entry congelada com o campo JÁ no estado: preserva o
				// anterior cru e descarta a nova. É o congelamento inteiro,
				// e é o único lugar onde ele acontece — a política viaja na
				// entry, e nem o replace nem o create precisam saber dela.
				out = append(out, r.RawField()...)
				pid, _, pok = r.Next()
				continue
			}
			// Sobrescrita (ou remoção): o campo anterior é descartado.
			pid, _, pok = r.Next()
		}
```

O `continue` pula o `AppendField` e o teste de `isNull` — correto: uma entry congelada nunca é null (a captura não a emite nesse caso).

- [ ] **Passo 4: rodar**

Run: `go test ./internal/materializer/ -v`
Expected: PASS

- [ ] **Passo 5: bench do merge**

Run: `go test ./internal/materializer/ -bench=BenchmarkMerge -benchmem -run=XXX`
Expected: sem regressão. O ramo novo é uma comparação de bool por campo em comum.

- [ ] **Passo 6: lint e commit**

```bash
make lint
git add internal/materializer/
git commit -m "Preserva o valor da coluna congelada no merge"
```

---

### Task 6: Colunas congeladas de ponta a ponta

**Files:**
- Test: `internal/materializer/frozen_test.go` (acrescenta), `internal/cli/` (teste e2e no estilo dos existentes)

- [ ] **Passo 1: escrever os testes de ciclo**

```go
func TestCongeladaSobreviveATresUpdatesDaOrigem(t *testing.T) {
	// insere {id:a, sev:5}; atualiza sev para 3, 1, 7
	// consulta: sev=7 e origSev=5
}
func TestCongeladaCapturaNoSegundoEventoQuandoOPrimeiroNaoTrouxeAOrigem(t *testing.T) {
	// insere {id:a}; depois {id:a, sev:3}; origSev=3 e não muda mais
}
func TestCongeladaSobreviveAoReplayDoTail(t *testing.T) {
	// grava, mata o store sem checkpoint, reabre com recovery,
	// confere que origSev é o valor de nascimento, não o corrente
}
func TestCongeladaComIndiceBitmapEhFiltravel(t *testing.T) {
	// origSev com index: bitmap; WHERE origSev = 5 devolve a entidade
	// mesmo depois de sev ter mudado
}
```

O teste de replay é o que prova a durabilidade: o valor congelado é um campo do record como qualquer outro, então o replay o repõe sem código novo — mas isso precisa ser *provado*, não assumido.

- [ ] **Passo 2: rodar**

Run: `go test ./internal/materializer/ ./internal/cli/ -run Congelada -v`
Expected: PASS

- [ ] **Passo 3: documentar**

Acrescente a `README.md` uma seção sobre `frozenFrom`, no estilo das outras: o YAML, a semântica de primeiro-valor-visto, a recusa em `mode: replace` e a ausência de preenchimento retroativo. Este último é o que vira surpresa se não estiver escrito.

- [ ] **Passo 4: suíte inteira, lint e commit**

Run: `make test && make lint`

```bash
git add -A
git commit -m "Prova a coluna congelada de ponta a ponta e documenta a chave"
```

---

# FASE C — TTL condicional

### Task 7: Compilar uma condição solta

**Files:**
- Modify: `internal/query/parser/parser.go`, `internal/query/binder/binder.go`
- Test: `internal/query/binder/binder_test.go`

**Interfaces:**
- Produces:
  ```go
  // parser
  func ParseCondicao(sql string) (Expr, error)

  // binder
  type BoundCondicao struct {
      Expr  BoundExpr
      Slots Slots
  }
  func BindCondicao(e parser.Expr, sc *schema.Compiled) (*BoundCondicao, error)
  ```

- [ ] **Passo 1: escrever os testes**

```go
func TestBindCondicaoResolveSlots(t *testing.T) {
	e, err := parser.ParseCondicao("type = 2 AND sev > 3")
	// bc.Slots.IDs tem os dois ids em ordem; as comparações têm Slot preenchido
}
func TestParseCondicaoRecusaTextoDepoisDaExpressao(t *testing.T) {
	_, err := parser.ParseCondicao("type = 2 LIMIT 5")
	// erro: sobra de tokens
}
func TestBindCondicaoRecusaParametro(t *testing.T) {
	e, _ := parser.ParseCondicao("type = ?")
	_, err := binder.BindCondicao(e, sc)
	// erro: não há quem forneça parâmetro
}
func TestBindCondicaoRecusaColunaInexistente(t *testing.T) { /* propaga o erro do binder */ }
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/query/... -run Condicao -v`
Expected: FAIL — nenhuma das duas funções existe.

- [ ] **Passo 3: `ParseCondicao`**

```go
// ParseCondicao parseia uma expressão booleana SOLTA — sem SELECT, sem FROM —
// com a MESMA gramática do WHERE.
//
// Existe para que a condição de uma regra de TTL seja escrita e entendida
// exatamente como um WHERE. Envolver a condição num SELECT sintético daria o
// mesmo resultado ao custo de mensagens de erro apontando para colunas de uma
// query que o operador não escreveu.
func ParseCondicao(sql string) (Expr, error) {
	p := &parser{}
	p.lx.Init(sql)
	if err := p.advance(); err != nil {
		return nil, err
	}
	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.cur.Type != lexer.TokenEOF {
		return nil, p.errAtCur("sobra de entrada depois da condição")
	}
	return e, nil
}
```

Se o parser não tiver `errAtCur`, use o mesmo construtor de erro posicional que `parseSelect` já usa para token inesperado.

- [ ] **Passo 4: `BindCondicao`**

```go
// BoundCondicao é uma condição resolvida contra o schema, com seus próprios
// slots. É o que o TTL condicional consome.
type BoundCondicao struct {
	Expr  BoundExpr
	Slots Slots

	arena []byte // bytes dos literais; as janelas em Val.Bytes apontam para cá
}

// BindCondicao resolve uma condição solta contra o schema, pelo MESMO caminho
// do WHERE: mesma coerção, mesma tabela de comparabilidade, mesmo tratamento
// de coluna auto. É essa identidade que garante que `type = 2` numa regra e
// `type = 2` numa consulta signifiquem a mesma coisa.
//
// Parâmetro posicional é recusado: a condição vem do YAML e não há quem o
// forneça.
func BindCondicao(e parser.Expr, sc *schema.Compiled) (*BoundCondicao, error) {
	if e == nil || sc == nil {
		return nil, errNoPos("bind de condição sem expressão ou schema")
	}
	b := &binder{sc: sc, usados: map[types.FieldID]struct{}{}}
	expr, err := b.expr(e)
	if err != nil {
		return nil, err
	}
	if b.nParams > 0 {
		return nil, errNoPos("condição não aceita parâmetro posicional")
	}
	bc := &BoundCondicao{Expr: expr, Slots: b.slots()}
	b.preencheSlotsDaExpr(bc.Expr, bc.Slots)
	for _, j := range b.pendente {
		j.cmp.Val.Bytes = b.arena[j.start:j.end:j.end]
	}
	bc.arena = b.arena
	return bc, nil
}
```

`preencheSlotsDaExpr` é o percurso com pilha explícita que hoje vive dentro de `preencheSlots` (`binder.go:794`). **Extraia-o** para que os dois chamadores usem o mesmo código:

```go
func (b *binder) preencheSlotsDaExpr(e BoundExpr, s Slots) {
	if e == nil {
		return
	}
	pilha := []BoundExpr{e}
	// ... corpo idêntico ao que está em preencheSlots hoje ...
}
```

e `preencheSlots` passa a chamá-lo depois de resolver `Columns` e `OrderBy`.

Atenção: `b.stmt` fica nil em `BindCondicao`. Confira que `b.expr` e `b.folha` não o tocam — se tocarem (para posição de erro), passe um `stmt` vazio não-nil em vez de nil.

- [ ] **Passo 5: rodar**

Run: `go test ./internal/query/... -v`
Expected: PASS, e nenhum teste de query existente muda.

- [ ] **Passo 6: lint e commit**

```bash
make lint
git add internal/query/
git commit -m "Compila uma condição solta pela mesma gramática do WHERE"
```

---

### Task 8: `ttl:` vira lista de regras

**Files:**
- Create: `internal/schema/ttl_rule.go`, `internal/schema/ttl_rule_test.go`
- Modify: `internal/schema/spec.go` (`TTLSpec` → `[]RegraTTLSpec`, `nomesDeSistema`), `internal/schema/expiry_test.go` (adaptar ao formato novo)

**Interfaces:**
- Produces:
  ```go
  type RegraTTLSpec struct {
      When             string `yaml:"when"`
      ExpireAfterWrite string `yaml:"expireAfterWrite"`
  }
  type RegraTTL struct {
      When     string        // "" na regra de fundo
      Duracao  time.Duration // 0 == never
  }
  const CampoDePrazo = "_expiresIn"
  const DuracaoNunca = "never"

  // DatasetSpec ganha a forma validada, lida pelos montadores de Options
  RegrasTTL      []RegraTTL
  DuracaoDeFundo time.Duration // da regra sem `when`; 0 se não há
  ```

  **Sai junto:** o método `DatasetSpec.ExpiraApos()`, hoje lido por
  `cli.go:243`, `offsets.go:93` e `server.go:332`. Ele devolvia a duração única
  da forma escalar, que deixou de existir. Os três chamadores são religados na
  Task 12 — deixe-os quebrados até lá, o compilador é a lista de pendências.

- [ ] **Passo 1: escrever os testes**

```go
func TestTTLListaVaziaEhRecusada(t *testing.T)                 { /* ttl: [] */ }
func TestRegraSemWhenNoMeioEhRecusada(t *testing.T)            { /* fundo antes de uma condicional */ }
func TestDuasRegrasSemWhenSaoRecusadas(t *testing.T)           {}
func TestRegraSemExpireAfterWriteEhRecusada(t *testing.T)      {}
func TestDuracaoAbaixoDaResolucaoEhRecusada(t *testing.T)      { /* 30s */ }
func TestZeroNumericoEhRecusadoMasNeverEhAceito(t *testing.T)  { /* 0 recusa, never aceita */ }
func TestWhenQueNaoCompilaEhRecusado(t *testing.T)             { /* "type ==== 2" */ }
func TestWhenSobreColunaInexistenteEhRecusado(t *testing.T)    {}
func TestWhenSobreWrittenAtEhRecusado(t *testing.T)            {}
func TestWhenSobreExpiresInEhRecusado(t *testing.T)            {}
func TestWhenComParametroEhRecusado(t *testing.T)              { /* "type = ?" */ }
func TestFormaEscalarAntigaEhRecusada(t *testing.T)            { /* ttl: {expireAfterWrite: 7d} */ }
func TestFormaFieldTypeAntigaEhRecusada(t *testing.T)          { /* ttl: {field: X, type: timestamp} */ }
```

Os dois últimos são o gate da migração: a forma antiga tem de dar erro **nomeando a forma nova**, não decodificar em silêncio.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/schema/ -run TTL -v`
Expected: FAIL — `TTLSpec` ainda é objeto.

- [ ] **Passo 3: trocar a forma no spec**

Em `internal/schema/spec.go`, `MaterializationSpec.TTL` passa a `[]RegraTTLSpec`, e `TTLSpec` sai junto com `SequenceSpec.Type`-style resíduos:

```go
// RegraTTLSpec é uma regra de expiração como escrita no YAML.
//
// A lista substituiu o objeto único, e a forma `{field, type}` — deadline
// absoluto vindo do payload — foi REMOVIDA: ela era parseada e validada sem
// nenhum consumidor, e manter configuração que não faz nada ao lado de um
// mecanismo que funciona é o defeito que este projeto já trata como tal.
type RegraTTLSpec struct {
	// When é a condição, com a mesma gramática do WHERE. Vazio marca a regra
	// de FUNDO: no máximo uma, obrigatoriamente a última.
	When string `yaml:"when"`
	// ExpireAfterWrite é o idle timeout desta regra, ou o literal "never".
	ExpireAfterWrite string `yaml:"expireAfterWrite"`
}

// DuracaoNunca é o literal que declara imortalidade. `0` numérico é recusado:
// `never` diz a intenção, `0` parece um esquecimento.
const DuracaoNunca = "never"

// CampoDePrazo carrega a DURAÇÃO de ociosidade que vale para a entidade, em
// millis. Duração, e não prazo absoluto, para que se componha com o
// CampoInstanteDeEscrita e para que reordenar as regras no YAML não mude o
// significado do que já está gravado.
const CampoDePrazo = "_expiresIn"
```

E `nomesDeSistema` ganha a entrada:

```go
var nomesDeSistema = map[string]string{
	CampoInstanteDeEscrita: "é injetado pelo writer a cada escrita",
	CampoDePrazo:           "é calculado pelo writer a partir das regras de ttl",
}
```

- [ ] **Passo 4: escrever a validação**

```go
// internal/schema/ttl_rule.go
package schema

// RegraTTL é uma regra validada. Duracao zero significa IMORTAL — a regra
// casou e decidiu que a entidade não expira —, e é distinto de "nenhuma regra
// casou", que o chamador representa não achando regra nenhuma.
type RegraTTL struct {
	When    string
	Duracao time.Duration
}

func validaRegrasTTL(regras []RegraTTLSpec) ([]RegraTTL, error) {
	if regras == nil {
		return nil, nil
	}
	if len(regras) == 0 {
		return nil, specErrf("materialization.ttl: lista vazia — remova a chave ou declare uma regra")
	}
	out := make([]RegraTTL, 0, len(regras))
	for i, r := range regras {
		if r.When == "" && i != len(regras)-1 {
			// A regra de fundo casa com tudo. Colocá-la antes de uma
			// condicional tornaria a condicional inalcançável, e a ordem é
			// semântica aqui: primeira que casa vence.
			return nil, specErrf("materialization.ttl[%d]: regra sem `when` só pode ser a última", i)
		}
		if r.ExpireAfterWrite == "" {
			return nil, specErrf("materialization.ttl[%d]: expireAfterWrite é obrigatório", i)
		}
		var d time.Duration
		if r.ExpireAfterWrite != DuracaoNunca {
			var err error
			if d, err = units.Duracao(r.ExpireAfterWrite, 0); err != nil {
				return nil, specErrf("materialization.ttl[%d]: expireAfterWrite: %v", i, err)
			}
			if d < ResolucaoDaExpiracao {
				return nil, specErrf(
					"materialization.ttl[%d]: expireAfterWrite %s é menor que a resolução do coletor (%s) — "+
						"use %q para declarar que a entidade não expira",
					i, r.ExpireAfterWrite, ResolucaoDaExpiracao, DuracaoNunca)
			}
		}
		out = append(out, RegraTTL{When: r.When, Duracao: d})
	}
	return out, nil
}
```

`units.Duracao("0", 0)` devolve zero sem erro, e zero é menor que a resolução — então a recusa do `0` numérico cai da checagem que já existe, com a mensagem apontando para `never`.

A validação do `when` (compilação e recusa dos campos de sistema) **não cabe aqui**: ela precisa do schema compilado, que só existe depois do dicionário. Vai na Task 9.

- [ ] **Passo 5: adaptar os testes de expiração existentes**

`internal/schema/expiry_test.go` está escrito contra a forma escalar. Reescreva cada caso para a lista. `TestExpireAfterWriteEFieldNoMesmoBlocoSaoRecusados` perde o motivo de existir (o `field` não existe mais) — troque-o por `TestFormaFieldTypeAntigaEhRecusada`.

- [ ] **Passo 6: rodar**

Run: `go test ./internal/schema/ -v`
Expected: PASS

- [ ] **Passo 7: lint e commit**

```bash
make lint
git add internal/schema/
git commit -m "Troca o bloco ttl por uma lista de regras condicionais"
```

---

### Task 9: Compilar as regras e registrar `_expiresIn`

**Files:**
- Modify: `internal/schema/compile.go` (`Compiled`, `compile.go:453`, `compile.go:490`, remoção de `IsTTL`)
- Test: `internal/schema/ttl_rule_test.go`

**Interfaces:**
- Consumes: `RegraTTL` (Task 8), `binder.BindCondicao` (Task 7), `predicado.Compila` (Task 1)
- Produces:
  ```go
  // schema.Compiled — guarda a forma VALIDADA, com o `when` ainda em texto
  RegrasTTL []RegraTTL
  Prazo     *CompiledField // o _expiresIn; nil sem regras

  // internal/predicate — a forma COMPILADA
  type RegraCompilada struct {
      Prog    *Programa // nil na regra de fundo (casa com tudo)
      Slots   binder.Slots
      Duracao time.Duration
  }
  func CompilaRegras(regras []schema.RegraTTL, sc *schema.Compiled) ([]RegraCompilada, error)
  ```

**Por que a regra compilada NÃO mora em `schema`:** compilar exige o `binder`, e o
`binder` importa `schema` — o ciclo seria imediato. `internal/predicate` é o único ponto
da árvore que pode importar os dois. O `Compiled` fica com a forma validada em texto, e
quem chama `CompilaRegras` é o **materializer**, no construtor, que importa os três sem
problema.

- [ ] **Passo 1: escrever os testes**

```go
func TestCompilaRegistraOCampoDePrazoQuandoHaRegras(t *testing.T) {
	// c.Prazo != nil, c.Prazo.IsSystem, c.Prazo.Type == types.TypeInt64
}
func TestSemRegrasNaoRegistraOCampoDePrazo(t *testing.T)   { /* c.Prazo == nil */ }
func TestCampoDePrazoFicaForaDoSelectStar(t *testing.T)    { /* camposOrdenados sem sistema */ }
func TestCompilaRegrasRecusaWhenSobreCampoDeSistema(t *testing.T) { /* em predicado */ }
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/schema/ ./internal/predicate/ -run Prazo -v`
Expected: FAIL

- [ ] **Passo 3: registrar o nome no conjunto a compilar**

Em `compile.go:453`, ao lado do `_writtenAt`:

```go
	// A ordem é contratual, e a razão é a do comentário do Open: o
	// LookupOrCreate queima ids na ordem em que é chamado, e essa ordem decide
	// o mapeamento nome→id de um dataset NOVO, gravado para sempre. _writtenAt
	// primeiro, _expiresIn depois, sempre — nunca reordene.
	if len(c.RegrasTTL) > 0 {
		names = append(names, CampoInstanteDeEscrita, CampoDePrazo)
	}
```

O `sort.Strings(names)` logo abaixo reordena para a compilação, mas o `LookupOrCreate` do **Open** é quem fixa os ids; é lá que a ordem importa (Task 12).

E no laço, ao lado do ramo do `_writtenAt` (`compile.go:503`):

```go
		if name == CampoDePrazo {
			// int64, e NÃO timestamp: ela carrega uma DURAÇÃO em millis, e
			// timestamp faria `SELECT _expiresIn` renderizar 3 minutos como
			// 1970-01-01T00:03:00Z — uma data que não é uma data.
			cf.Type, cf.IsSystem = types.TypeInt64, true
			c.Prazo = cf
			c.byName[cf.Name] = cf
			c.byID[cf.ID] = cf
			continue
		}
```

- [ ] **Passo 4: remover `IsTTL`**

Tire `IsTTL` de `CompiledField` (`compile.go:115`), a atribuição em `:501`, o uso em `:559` e a asserção em `compile_test.go:127`. Confirmado: não há consumidor fora do próprio `schema`.

- [ ] **Passo 5: `predicado.CompilaRegras`**

```go
// CompilaRegras transforma as regras validadas em programas avaliáveis.
//
// Mora AQUI, e não em schema, porque compilar exige o binder — e o binder
// importa schema. Este pacote é o único lugar da árvore que pode importar os
// dois sem fechar ciclo.
func CompilaRegras(regras []schema.RegraTTL, sc *schema.Compiled) ([]RegraCompilada, error) {
	out := make([]RegraCompilada, 0, len(regras))
	for i, r := range regras {
		if r.When == "" {
			out = append(out, RegraCompilada{Duracao: r.Duracao})
			continue
		}
		e, err := parser.ParseCondicao(r.When)
		if err != nil {
			return nil, fmt.Errorf("ttl[%d]: condição %q: %w", i, r.When, err)
		}
		bc, err := binder.BindCondicao(e, sc)
		if err != nil {
			return nil, fmt.Errorf("ttl[%d]: condição %q: %w", i, r.When, err)
		}
		for _, id := range bc.Slots.IDs {
			// A duração não pode depender de si mesma, nem do instante que ela
			// própria ajuda a interpretar.
			if f, ok := sc.FieldByID(id); ok && f.IsSystem {
				return nil, fmt.Errorf("ttl[%d]: condição %q referencia o campo de sistema %q",
					i, r.When, f.Name)
			}
		}
		prog, err := Compila(bc.Expr, bc.Slots.Len())
		if err != nil {
			return nil, fmt.Errorf("ttl[%d]: condição %q: %w", i, r.When, err)
		}
		out = append(out, RegraCompilada{Prog: prog, Slots: bc.Slots, Duracao: r.Duracao})
	}
	return out, nil
}
```

- [ ] **Passo 6: rodar**

Run: `go test ./internal/schema/ ./internal/predicate/ -v`
Expected: PASS

- [ ] **Passo 7: lint e commit**

```bash
make lint
git add internal/schema/ internal/predicate/
git commit -m "Compila as regras de ttl e registra o campo de prazo"
```

---

### Task 10: Avaliar na ingestão e emitir `_expiresIn`

**Files:**
- Create: `internal/materializer/deadline.go`, `internal/materializer/deadline_test.go`
- Modify: `internal/materializer/patch.go`, `internal/materializer/materializer.go`

**Interfaces:**
- Consumes: `predicado.RegraCompilada`, `predicado.CompilaRegras` (Task 9)
- Produces: `func (m *Materializer) duracaoDe(prevPayload []byte, entries []patchEntry) time.Duration`

- [ ] **Passo 1: escrever os testes**

```go
func TestPrazoUsaAPrimeiraRegraQueCasa(t *testing.T)              {}
func TestPrazoCaiNaRegraDeFundoQuandoNenhumaCasa(t *testing.T)    {}
func TestPrazoZeroQuandoNenhumaCasaESemRegraDeFundo(t *testing.T) {}
func TestPrazoNeverDaZeroMesmoComRegraDeFundo(t *testing.T)       {}
func TestPrazoVeOValorDoEstadoQuandoOEventoNaoTrazOCampo(t *testing.T) {
	// estado tem type=2; evento só traz outro campo → 3m
}
func TestPrazoVeOValorDoEventoQuandoEleSobrescreveOEstado(t *testing.T) {
	// estado type=2, evento type=1 → deixa de casar
}
func TestPrazoTrataNullExplicitoComoAusente(t *testing.T) {
	// estado type=2, evento type:null → a condição type=2 não casa
}
func TestExpiresInEhEmitidoEmTodaEscrita(t *testing.T) {
	// duas escritas seguidas com a MESMA duração: as duas trazem a entry
}
```

O último é o teste que protege o cancelamento. Se alguém "otimizar" a emissão para só quando muda, ele quebra.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/materializer/ -run Prazo -v`
Expected: FAIL

- [ ] **Passo 3: escrever a avaliação**

```go
// internal/materializer/deadline.go
package materializer

// duracaoDe devolve a duração de ociosidade que vale para o estado que ESTA
// escrita vai produzir, avaliando as regras sobre o valor PÓS-MERGE de cada
// campo que elas mencionam:
//
//	valor pós-merge de F = valor de F no patch, se presente e não-null
//	                       senão o valor de F no estado anterior
//	                       senão ausente
//
// A leitura do estado anterior é DIRIGIDA: uma passada de FieldReader que
// colhe só os ids que as regras citam. Ela não é a mesma passada do merge de
// propósito — misturar a coleta com o mergeRecords ganharia uma varredura e
// custaria a clareza do arquivo onde a semântica de null mora. Se o bench
// acusar, é lá que a otimização entra, com medição na mão.
func (m *Materializer) duracaoDe(prevPayload []byte, entries []patchEntry) (time.Duration, error) {
	for i := range m.regras {
		r := &m.regras[i]
		if r.Prog == nil {
			return r.Duracao, nil // regra de fundo: casa com tudo
		}
		slots := m.slots[i]
		if err := m.preencheSlots(slots, prevPayload, entries); err != nil {
			return 0, err
		}
		if r.Prog.Avalia(slots, nil) {
			return r.Duracao, nil
		}
	}
	return 0, nil
}

// preencheSlots resolve o valor pós-merge de cada slot: o patch tem prioridade
// e o null explícito apaga, senão vale o estado anterior.
func (m *Materializer) preencheSlots(slots []predicado.Slot, prevPayload []byte, entries []patchEntry) error {
	for i := range slots {
		slots[i].Present = false
	}
	// 1. Estado anterior, em uma passada; para quando os slots acabam.
	m.fr.Reset(prevPayload)
	i := 0
	for i < len(slots) {
		id, v, ok := m.fr.Next()
		if !ok {
			if err := m.fr.Err(); err != nil {
				return err
			}
			break
		}
		for i < len(slots) && slots[i].ID < id {
			i++
		}
		if i < len(slots) && slots[i].ID == id {
			slots[i].Val, slots[i].Present = v, true
			i++
		}
	}
	// 2. O patch por cima. entries está ordenada por FieldID e sem duplicatas
	// neste ponto (o dedup já rodou), mas o laço não depende disso.
	for j := range entries {
		e := &entries[j]
		k, ok := slotDe(slots, e.field.ID)
		if !ok {
			continue
		}
		if e.isNull {
			slots[k].Present = false
			continue
		}
		slots[k].Val, slots[k].Present = e.val, true
	}
	return nil
}
```

`m.slots` é um `[][]predicado.Slot`, um por regra, alocado UMA vez no construtor via `predicado.NovosSlots(r.Slots.IDs)` — a avaliação roda por evento e não pode alocar. `m.fr` é um `encoding.FieldReader` reusado, como todo buffer deste pacote. `slotDe` é uma busca binária sobre `slots` por `ID`.

- [ ] **Passo 4: emitir a entry**

Em `patch.go`, o bloco do `_writtenAt` ganha um irmão. Como a duração depende do `prevPayload`, que o `Parse` não tem, a emissão fica no `Apply` do materializer, **antes** do merge e depois do `Parse`:

```go
	// materializer.go, depois de m.patch.Parse e antes do merge
	if m.schema.Prazo != nil {
		d, err := m.duracaoDe(prevPayload, m.patch.entries)
		if err != nil {
			return err
		}
		// Emitida em TODA escrita, e não só quando o resultado muda. O merge
		// preserva campo que o evento não menciona, então a emissão
		// condicional deixaria o prazo curto de quando a condição valia
		// sobreviver ao evento que a quebra — e a entidade morreria logo
		// depois de o alarme reabrir. São 8 bytes por versão de record para
		// tornar impossível a única falha que arruinaria a feature.
		m.patch.emitePrazo(m.schema.Prazo, d)
	}
```

`emitePrazo` acrescenta a entry e **reordena**, porque o sort do `Parse` já rodou:

```go
func (b *PatchBuffer) emitePrazo(cf *schema.CompiledField, d time.Duration) {
	b.entries = append(b.entries, patchEntry{
		field: cf,
		val:   types.Value{Type: types.TypeInt64, I64: d.Milliseconds()},
	})
	// A entry entra depois do sort do Parse, então reordena. Como ela é UMA e
	// o resto já está ordenado, uma inserção por deslocamento basta — e é o
	// que evita um sort O(n log n) por evento.
	for i := len(b.entries) - 1; i > 0 && b.entries[i-1].field.ID > b.entries[i].field.ID; i-- {
		b.entries[i-1], b.entries[i] = b.entries[i], b.entries[i-1]
	}
}
```

- [ ] **Passo 5: rodar**

Run: `go test ./internal/materializer/ -v`
Expected: PASS

- [ ] **Passo 6: bench de ingestão**

Run: `go test ./internal/materializer/ -bench=. -benchmem -run=XXX`
Expected: alocações por evento **inalteradas**. Se subirem, o culpado é `m.slots` ou `m.fr` não reusado.

- [ ] **Passo 7: lint e commit**

```bash
make lint
git add internal/materializer/
git commit -m "Avalia as regras de ttl na ingestão e grava a duração no documento"
```

---

### Task 11: O índice de prazos

**Files:**
- Modify: `internal/storage/rowindex/rowindex.go`
- Test: `internal/storage/rowindex/rowindex_test.go`

**Interfaces:**
- Produces:
  ```go
  const PrazoImortal uint64 = 1
  type Prazos struct{ /* dense *denseFile */ }
  func OpenPrazos(path string, minRows uint64) (*Prazos, error)
  func (p *Prazos) Get(rowID types.RowID) (millis int64, conhecido bool)
  func (p *Prazos) Set(rowID types.RowID, millis int64) error
  func (p *Prazos) Msync() error
  func (p *Prazos) Close() error
  ```

- [ ] **Passo 1: escrever os testes**

```go
func TestPrazosDistingueImortalDeDesconhecido(t *testing.T) {
	p := abrePrazos(t)
	if _, conhecido := p.Get(1); conhecido {
		t.Error("entrada nunca gravada deveria ser desconhecida")
	}
	if err := p.Set(1, 0); err != nil {
		t.Fatal(err)
	}
	ms, conhecido := p.Get(1)
	if !conhecido || ms != 0 {
		t.Errorf("Get depois de Set(0) = (%d,%v), quero (0,true)", ms, conhecido)
	}
}
func TestPrazosGuardaDuracao(t *testing.T)          { /* Set(1, 180000) → Get = 180000,true */ }
func TestPrazosSobreviveAoReabrir(t *testing.T)     { /* Close e OpenPrazos de novo */ }
func TestPrazosMagicaPropriaRecusaArquivoDeEscritas(t *testing.T) { /* abrir expiry.idx como prazos falha */ }
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/storage/rowindex/ -run Prazos -v`
Expected: FAIL

- [ ] **Passo 3: implementar**

```go
// PrazoImortal é o valor gravado para a entidade cuja regra decidiu que ela
// NÃO expira.
//
// Existe porque denseFile.load devolve ok=false quando o valor é zero, então
// "gravei zero" e "nunca gravei" seriam o mesmo byte — e aqui os dois não
// podem significar a mesma coisa. Desconhecido cai no prazo de fundo; imortal
// não pode cair em lugar nenhum. Sem a distinção, a entidade que casa com uma
// regra `never` num dataset que TAMBÉM declara regra de fundo herdaria o prazo
// de fundo, e o alarme que o operador mandou nunca expirar sairia sozinho.
//
// Não há colisão possível com uma duração real: a menor declarável é um
// minuto, 60000 ms.
const PrazoImortal uint64 = 1

// Prazos é o índice rowId→duração de ociosidade, em millis.
//
// É DERIVADO, como o Escritas: a fonte da verdade é o campo de sistema no
// payload, e o replay do tail o reconstrói pelo mesmo applyRecord que o Put
// usa. Zero significa DESCONHECIDO — entidade anterior à adoção das regras —,
// nunca imortal; para imortal existe o PrazoImortal.
type Prazos struct {
	dense *denseFile
}

var prazosMagic = [4]byte{'N', 'D', 'B', 'P'}

func OpenPrazos(path string, minRows uint64) (*Prazos, error) {
	d, err := openDense(path, prazosMagic, minRows)
	if err != nil {
		return nil, err
	}
	return &Prazos{dense: d}, nil
}

// Get devolve a duração em millis, e conhecido=false quando a entrada nunca
// foi gravada. Duração zero COM conhecido=true é a entidade imortal.
func (p *Prazos) Get(rowID types.RowID) (millis int64, conhecido bool) {
	v, ok := p.dense.load(rowID)
	if !ok || v == 0 {
		return 0, false
	}
	if v == PrazoImortal {
		return 0, true
	}
	return int64(v), true
}

// Set grava a duração. Exclusivo do writer.
func (p *Prazos) Set(rowID types.RowID, millis int64) error {
	v := uint64(millis)
	if millis <= 0 {
		v = PrazoImortal
	}
	return p.dense.store(rowID, v)
}

func (p *Prazos) Msync() error { return p.dense.region.Msync() }
func (p *Prazos) Close() error { return p.dense.region.Close() }
```

- [ ] **Passo 4: rodar**

Run: `go test ./internal/storage/rowindex/ -v`
Expected: PASS

- [ ] **Passo 5: lint e commit**

```bash
make lint
git add internal/storage/rowindex/
git commit -m "Acrescenta o índice de prazos com sentinela para o imortal"
```

---

### Task 12: O storage passa a usar a duração por linha

**Files:**
- Create: `internal/storage/deadlines_test.go`
- Modify: `internal/storage/store.go` (Options `:115`, abertura `:721`, dicionário `:749`, applyRecord `:1467`, `prazoVencido` `:1528`), `internal/storage/store_checkpoint.go:102`, `internal/storage/expiry.go:256`

**Interfaces:**
- Consumes: `rowindex.Prazos`, `rowindex.PrazoImortal` (Task 11); `schema.CampoDePrazo` (Task 8)
- Produces (Options):
  ```go
  TTLAtivo       bool          // há ao menos uma regra; substitui ExpireAfterWrite
  DuracaoDeFundo time.Duration // da regra sem `when`; 0 se não há
  CampoDeEscrita string
  CampoDePrazo   string        // obrigatório quando TTLAtivo
  ```

- [ ] **Passo 1: escrever os testes**

```go
func TestPrazoVencidoUsaADuracaoDaLinha(t *testing.T)              {}
func TestLinhaSemExpiresInCaiNaDuracaoDeFundo(t *testing.T)        { /* a prova da migração */ }
func TestLinhaSemExpiresInESemFundoEhImortal(t *testing.T)         {}
func TestLinhaImortalNaoHerdaADuracaoDeFundo(t *testing.T)         { /* o sentinela */ }
func TestReplayDoTailReconstroiOsPrazos(t *testing.T)              {}
func TestPrazoVencidoEscondeALinhaComOColetorParado(t *testing.T)  {}
```

```go
func TestCompactionDescartaRecordVencidoPorRegraCondicional(t *testing.T) {}
```

`TestLinhaImortalNaoHerdaADuracaoDeFundo` é o teste do sentinela e o mais importante do conjunto: sem ele, um `Get` que colapse imortal com desconhecido passa despercebido.

**`montaLapide` NÃO muda.** Ela hoje preserva só o `campoDeEscrita`, e é para continuar
assim: a lápide não precisa de prazo, e não preservar o `_expiresIn` devolve os bytes. A
entrada em `prazos.idx` fica com o valor antigo, o que é inofensivo — a linha já está
tombstonada e `Expira` é idempotente. Se você sentir vontade de acrescentá-lo ali, a
resposta é o pré-filtro da Task 13, não um campo a mais na lápide.

Use `UsaRelogio` (`expiry.go:16`) para avançar o tempo sem dormir.

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/storage/ -run Prazo -v`
Expected: FAIL

- [ ] **Passo 3: trocar as Options**

Substitua `ExpireAfterWrite time.Duration` por:

```go
	// TTLAtivo liga a expiração: o dataset declarou ao menos uma regra. Sem
	// ela nem os índices são abertos — o dataset não paga dois mmaps para não
	// fazer nada.
	TTLAtivo bool
	// DuracaoDeFundo é a duração da regra SEM `when`, ou zero quando não há.
	//
	// Ela vale para a linha cuja entrada em prazos.idx é DESCONHECIDA — a que
	// foi materializada antes de as regras existirem. É o que permite adotar
	// as regras num dataset já populado sem que ele fique imortal da noite
	// para o dia, e é o que faz a migração da forma escalar não mudar
	// comportamento nenhum.
	DuracaoDeFundo time.Duration
	// CampoDePrazo é o NOME do campo de sistema que carrega a duração,
	// resolvido pelo dicionário no Open. Obrigatório quando TTLAtivo.
	CampoDePrazo string
```

- [ ] **Passo 4: abrir o segundo índice**

Em `store.go:721`:

```go
	if opts.TTLAtivo {
		if opts.CampoDeEscrita == "" || opts.CampoDePrazo == "" {
			return nil, fmt.Errorf("%w: TTLAtivo exige CampoDeEscrita e CampoDePrazo", ErrInvalidArgument)
		}
		if s.escritas, err = rowindex.OpenEscritas(filepath.Join(idxDir, "expiry.idx"), minRows); err != nil {
			return nil, err
		}
		if s.prazos, err = rowindex.OpenPrazos(filepath.Join(idxDir, "prazos.idx"), minRows); err != nil {
			return nil, err
		}
		s.duracaoDeFundo = opts.DuracaoDeFundo
	}
```

E em `store.go:749`, **na ordem**:

```go
	if s.escritas != nil {
		// LookupOrCreate e não Lookup, e nesta ORDEM. O id sai daqui e tem de
		// ser o MESMO que o schema.Compile resolve depois, e a ordem das
		// chamadas decide o mapeamento nome→id de um dataset NOVO, gravado
		// para sempre: _writtenAt, _expiresIn, bitmaps, hashes. Nunca reordene.
		if s.campoDeEscrita, err = s.dict.LookupOrCreate([]byte(opts.CampoDeEscrita)); err != nil {
			return nil, fmt.Errorf("storage: registrando %q no dicionário: %w", opts.CampoDeEscrita, err)
		}
		if s.campoDePrazo, err = s.dict.LookupOrCreate([]byte(opts.CampoDePrazo)); err != nil {
			return nil, fmt.Errorf("storage: registrando %q no dicionário: %w", opts.CampoDePrazo, err)
		}
	}
```

`LookupOrCreate` devolve `(types.FieldID, error)`; ajuste se a assinatura no arquivo for outra.

- [ ] **Passo 5: preencher o índice no applyRecord**

Em `store.go:1467`, ao lado do instante:

```go
	if s.prazos != nil {
		// Lido do payload pela mesma razão do instante: applyRecord é o único
		// caminho de aplicação de estado e roda tanto no Put quanto no replay
		// do tail, então depois de um crash o replay repõe as MESMAS durações.
		if ms, ok := campoInt64(a.payload, s.campoDePrazo); ok {
			if err := s.prazos.Set(a.rowID, ms); err != nil {
				return err
			}
		}
	}
```

`campoInt64` é o gêmeo de `instanteDeEscrita` (`store.go:1502`) para `types.TypeInt64`. Extraia o corpo comum numa função que recebe o tipo esperado, em vez de duplicar a varredura.

- [ ] **Passo 6: a conta do prazo**

```go
// duracaoEfetiva devolve por quanto tempo esta linha pode ficar sem escrita.
//
// Entrada DESCONHECIDA cai na duração de FUNDO, e é isso que faz a adoção das
// regras num dataset já materializado não mudar comportamento: as entidades
// que não têm _expiresIn no documento continuam expirando pelo prazo que a
// regra de fundo declara, e passam a obedecer as condicionais na primeira
// escrita seguinte.
//
// Zero devolvido COM conhecido=true é a entidade imortal, e ela NÃO cai no
// fundo — é para essa distinção que o PrazoImortal existe.
func (s *Store) duracaoEfetiva(rowID types.RowID) time.Duration {
	if s.prazos == nil {
		return 0
	}
	if ms, conhecido := s.prazos.Get(rowID); conhecido {
		return time.Duration(ms) * time.Millisecond
	}
	return s.duracaoDeFundo
}

func (s *Store) prazoVencido(rowID types.RowID) bool {
	if s.escritas == nil {
		return false
	}
	d := s.duracaoEfetiva(rowID)
	if d <= 0 {
		return false
	}
	ms, conhecido := s.escritas.Get(rowID)
	if !conhecido {
		return false
	}
	return s.agora().UnixMilli()-ms >= d.Milliseconds()
}
```

Remova o campo `s.expireAfterWrite`.

- [ ] **Passo 7: o coletor usa a mesma conta**

Em `expiry.go:256`, troque a comparação inline por `s.prazoVencido(id)` — os dois lugares faziam a mesma conta duplicada, e agora ela tem um dono:

```go
		case s.prazoVencido(id):
			expirou, err := s.Expira(id)
```

O `case !conhecido` da carência continua olhando `s.escritas.Get(id)`, que é sobre o INSTANTE, não sobre a duração.

- [ ] **Passo 8: checkpoint e Close**

`store_checkpoint.go:102` faz `Msync` do `escritas`; acrescente o `prazos`. Idem no `Close`.

- [ ] **Passo 9: religar os três montadores de Options**

`internal/cli/cli.go:243`, `internal/cli/offsets.go:93`, `internal/server/server.go:332`:

```go
		TTLAtivo:       len(spec.RegrasTTL) > 0,
		DuracaoDeFundo: spec.DuracaoDeFundo,
		CampoDeEscrita: schema.CampoInstanteDeEscrita,
		CampoDePrazo:   schema.CampoDePrazo,
```

- [ ] **Passo 10: rodar tudo**

Run: `make test`
Expected: PASS

- [ ] **Passo 11: lint e commit**

```bash
make lint
git add -A
git commit -m "Faz o prazo da linha vir da duração gravada no documento"
```

---

### Task 13: Pré-filtro de tombstone no coletor

**Files:**
- Modify: `internal/storage/expiry.go` (`ExpiraUmaPassada`)
- Test: `internal/storage/expiry_test.go`

- [ ] **Passo 1: escrever o teste**

```go
func TestColetorNaoTentaExpirarLinhaJaTombstonada(t *testing.T) {
	// expira a linha; roda outra passada; r.Renovadas deve ser 0 e r.Expiradas 0,
	// provando que Expira não foi chamada de novo
}
```

- [ ] **Passo 2: rodar e ver falhar**

Run: `go test ./internal/storage/ -run TestColetorNaoTenta -v`
Expected: FAIL — hoje a linha tombstonada é contada em `Renovadas` a cada passada.

- [ ] **Passo 3: acrescentar o pré-filtro**

No laço de `ExpiraUmaPassada`, antes do `switch`:

```go
		// A lápide preserva o instante de escrita de propósito, então a linha
		// tombstonada continua "vencida" para sempre e o coletor a reexaminaria
		// a cada passada, chamando Expira — que TOMA O wmu só para ver a flag e
		// devolver false.
		//
		// Com prazo de dias isso é ruído. Com prazo de MINUTOS, que é o que as
		// regras condicionais trouxeram, é disputa contínua pelo lock de
		// escrita com a ingestão — e o freio de MaxPorPassada não protege,
		// porque ele conta expirações efetivas, não chamadas.
		//
		// A leitura é sem lock, e o pré-filtro é seguro porque tombstone não se
		// desfaz: a decisão só pode ficar mais conservadora, e o Expira
		// continua revalidando tudo sob o lock.
		if packed, tem := s.locs.Get(id); tem {
			if _, _, flags := rowindex.Unpack(packed); flags&types.FlagTombstone != 0 {
				id++
				if id >= limite {
					id, r.DeuVolta = 0, true
				}
				continue
			}
		}
```

Cuidado com o avanço do cursor: o `continue` tem de repetir o incremento que está no fim do laço. Se preferir, extraia o avanço para um closure e chame nos dois pontos — mas não deixe o `continue` pular o incremento, que trava a passada num laço infinito sobre a mesma linha.

- [ ] **Passo 4: rodar**

Run: `go test ./internal/storage/ -v`
Expected: PASS

- [ ] **Passo 5: lint e commit**

```bash
make lint
git add internal/storage/
git commit -m "Poupa o lock de escrita pulando linha já tombstonada na varredura"
```

---

### Task 14: Contadores e log de subida

**Files:**
- Modify: `internal/materializer/materializer.go`, `internal/materializer/deadline.go`, `internal/schema/compile.go` ou o ponto de subida do dataset

- [ ] **Passo 1: escrever o teste**

```go
func TestContadoresDeRegraContamPorRegra(t *testing.T) {
	// 3 eventos: dois casam a regra 0, um cai no fundo
	// m.Stats(): regra[0]=2, regra[1]=1, semRegra=0
}
func TestContadorDeCongeladaSeparaCapturaDePreservacao(t *testing.T) {}
```

- [ ] **Passo 2: implementar**

Contadores por dataset, ao lado dos que a expiração já publica:

| Contador | Onde incrementa | Responde |
|---|---|---|
| `ttl.regra[i].casou` | `duracaoDe`, no return de cada regra | Qual regra está pegando o quê |
| `ttl.semRegra` | `duracaoDe`, no return final | Quantas escritas não casaram com nada |
| `congelada.capturada` | `capturaCongeladas`, ao emitir | Se a captura está acontecendo |
| `congelada.preservada` | `mergeRecords`, no ramo congelado | Se o congelamento está segurando |

Siga o padrão de `atomic` já usado em `internal/storage/store.go` (`s.expiradas.Add(1)`).

- [ ] **Passo 3: log de subida**

Quando há regras, uma linha `Info` na abertura do dataset listando cada regra, sua duração e se existe regra de fundo:

```go
	s.opts.Log.Info("ttl.regras",
		"regras de expiração carregadas",
		"total", len(regras), "temFundo", temFundo, "regras", descreveRegras(regras))
```

É o que permite ao operador conferir que o YAML que ele escreveu é o que o processo entendeu — inclusive a ordem, que aqui é semântica.

- [ ] **Passo 4: rodar, lint, commit**

Run: `make test && make lint`

```bash
git add -A
git commit -m "Publica contadores por regra e registra as regras na subida"
```

---

### Task 15: Ciclo completo e migração do piloto

**Files:**
- Test: `internal/cli/` (e2e no estilo dos existentes)
- Modify: `README.md`, `docs/piloto-events.md`, `docs/debitos.md`

- [ ] **Passo 1: escrever o e2e do cancelamento**

```go
func TestCicloCancelamentoDoTTLCondicional(t *testing.T) {
	// regras: [{when: "type = 2", 3m}, {7d}]
	// t0     evento {id:a, type:2} → _expiresIn = 180000
	// t0+90s evento {id:a, type:1} → _expiresIn = 604800000
	// t0+4m  passada do coletor    → a entidade CONTINUA visível
	// prova que o alarme reaberto não morreu
}
func TestCicloExpiracaoQuandoACondicaoPersiste(t *testing.T) {
	// t0 evento type=2; sem mais eventos; t0+4m → some da consulta E ganha lápide
}
func TestCicloNeverNaoExpiraComRegraDeFundoPresente(t *testing.T) {}
func TestCicloRenovacaoEmpurraOPrazo(t *testing.T) {
	// type=2 a cada 60s por 5 minutos → nunca expira (a decisão 1, provada)
}
```

O último documenta em teste o risco R1 da spec. Ele não é um bug: é o comportamento escolhido, e ter um teste com esse nome impede que alguém o "conserte" por engano.

- [ ] **Passo 2: rodar**

Run: `go test ./internal/cli/ -run TestCiclo -v`
Expected: PASS

- [ ] **Passo 3: migrar o YAML do piloto**

Localize a spec do piloto `events` (`configs/` ou `testdata/`) e troque o bloco escalar pela lista. Sem a migração o piloto não sobe — o que é intencional: a forma antiga é recusada com mensagem nomeando a nova.

- [ ] **Passo 4: documentar**

- `README.md`: seção do `ttl:` reescrita para a lista, com `when`, `never` e a regra de fundo. Deixe explícito que a ordem é semântica.
- `docs/piloto-events.md`: a linha *"Sem TTL (M6): entidade materializada nunca expira"* está obsoleta desde o `expireAfterWrite`; atualize-a para descrever as regras condicionais.
- `docs/debitos.md`: registre o que ficou de fora — preenchimento retroativo das congeladas, teto de idade absoluta por regra (R1), precisão sub-minuto.

- [ ] **Passo 5: gate final**

Run: `make test`
Run: `make test-race`
Run: `make lint`

A contagem do lint tem de **bater exatamente** com `.golangci-baseline`. Se mudou — para mais ou para menos —, atualize o arquivo antes de abrir o PR.

- [ ] **Passo 6: commit**

```bash
git add -A
git commit -m "Prova o ciclo do ttl condicional e migra a spec do piloto"
```

---

## Ordem e paralelismo

A Fase A é pré-requisito da Task 7 (o `predicado` precisa existir para compilar as regras) e de mais nada. **A Fase B inteira é independente da Fase C** e pode ser executada em paralelo por outro agente, desde que os dois partam da Fase A já commitada — elas só se encontram em `patch.go` e `merge.go`, em pontos distintos.

Dentro da Fase C a ordem é estrita: 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15. A Task 11 é a única exceção; ela não depende de 7-10 e pode sair a qualquer momento depois da Fase A.

## Riscos de execução

| Risco | Sinal | O que fazer |
|---|---|---|
| A extração da Fase A muda comportamento | Um teste do executor precisa de asserção nova | **Pare.** A extração é para ser invisível. Investigue o que mudou |
| `binder.BindCondicao` estoura com `b.stmt` nil | Panic em `b.folha` ou `b.expr` | Passe um `&parser.SelectStmt{}` vazio em vez de nil |
| Alocação por evento sobe na Task 10 | Bench de ingestão com `allocs/op` maior | `m.slots` ou `m.fr` não estão sendo reusados entre eventos |
| O pré-filtro da Task 13 trava a passada | Teste do coletor não termina | O `continue` pulou o incremento do cursor |
| A ordem do `LookupOrCreate` muda ids de dataset existente | Teste de reabertura falha | Dataset existente não é afetado (dict é append-only). Se falhou, o nome novo está sendo registrado ANTES do `_writtenAt` |
