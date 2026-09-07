# Identificadores em inglês — plano de implementação

> **Para quem executa:** SUB-SKILL OBRIGATÓRIA: `superpowers:subagent-driven-development`
> (recomendada) ou `superpowers:executing-plans`, tarefa a tarefa. Os passos usam
> `- [ ]` para acompanhamento. Trabalhar no worktree
> `.claude/worktrees/identificadores-em-ingles`, branch
> `refactor/identificadores-em-ingles` (já criados). As Fases 2, 3 e 4 rodam por
> workflow de agentes (§9 do spec); o script do workflow está na Tarefa 2.0 e é
> reutilizado com argumentos diferentes.

**Objetivo:** todo identificador, nome de pacote, arquivo, teste, mensagem, chave
de log e contrato do NishiDB em inglês, com a prosa (docs, comentários, commits,
mensagens de falha de teste) em PT-BR, sem nada renomeado à mão.

**Arquitetura:** três ferramentas provadas em spike — `rf` em lote por pacote,
`scripts/ifacerename` (própria, só para grupos de interface, fecho transitivo por
`go/types`) e `gopls rename` mais `git mv` para pacotes — mais a passada de prosa
`scripts/renamecite`. O mapeamento em TSV é o artefato: agentes propõem, juiz
consolida, refutadores atacam, um aplicador único aplica, verificador e crítico
de completude fecham.

**Stack:** Go 1.26 (CI pina 1.26.5), `rsc.io/rf@v0.0.0-20260710193434-85dbf8d53aee`,
`gopls v0.23.0`, `golang.org/x/tools/go/packages` (dependência nova do módulo,
usada só por `scripts/`), `golangci-lint v2.12.2`.

**Spec:** `planning/identificadores-em-ingles.md` — o plano argumenta a partir
dele; quem executa lê os dois.

## Restrições globais

- Prosa em PT-BR, identificadores e emissões em inglês (spec §1). Código NOVO
  deste ciclo (as três ferramentas, o gate do glossário) já nasce assim:
  identificadores em inglês, godoc e comentários em PT-BR.
- Sem compat: nenhum nome velho é aceito em paralelo (spec §1).
- Nada é renomeado à mão. Todo rename passa por `rf`, `ifacerename`, `gopls`
  ou `renamecite`, a partir de um TSV versionado em
  `planning/identificadores-em-ingles/` (spec §5).
- O que já está em inglês não muda (spec §6).
- Estilo Go: iniciais `ID`, `URL`, `HTTP`, `JSON`, `TLS`, `CRC`, `TTL`, `SQL`,
  `NDJSON`, `CLI`, `RSS`; sem prefixo `Get`; sem sufixo numérico para colisão.
- Comportamento não muda. Um rename que exija mudança de lógica para compilar
  é erro de mapeamento (spec §2).
- Lint é ratchet nos dois sentidos: `make lint` e `.golangci-baseline`
  igualada no fechamento de CADA fase, no mesmo commit.
- Teste que passa com o defeito presente não prova nada: toda ferramenta nova
  tem a mutação aplicada, vista vermelha, revertida, vista verde, e as duas
  relatadas no aceite (spec §8).
- Grep no repositório exclui `.claude/worktrees/` e `.superpowers/`.
- O guarda do worktree recusa comandos bash compostos com git. Um comando git
  por chamada; laços e scripts vão para arquivo e rodam com `bash arquivo.sh`.
- Commits na voz do time, por bloco lógico, em PT-BR, sem menção a agente.

## Estrutura de arquivos

| Arquivo | Responsabilidade |
|---|---|
| `docs/glossario.md` | normativo: termo PT-BR ↔ identificador inglês; regras de estilo |
| `internal/gates/glossario_test.go` | o glossário tem as duas colunas em toda linha e nenhum termo duplicado |
| `scripts/renameinv/main.go`, `inventory.go`, `groups.go`, `*_test.go`, `testdata/modulo/` | inventário do módulo em TSV: toda declaração com endereço `rf`, e os grupos de interface |
| `scripts/ifacerename/main.go`, `closure.go`, `rewrite.go`, `*_test.go`, `testdata/modulo/` | rename de grupo de interface, fecho transitivo, passada única |
| `scripts/renamecite/main.go`, `rules.go`, `gofile.go`, `textfile.go`, `*_test.go` | passada de prosa: crases, doc links, qualificados, `TestXxx`, caminhos, chaves de log, subtestes |
| `scripts/rename-apply.sh` | aplicador: exige `rf` e `gopls`, roda o script `rf` de um pacote, guarda o `-diff`, build e vet |
| `internal/gates/rename_tools_test.go` | o aplicador recusa alto sem `rf` nomeando o `go install` |
| `planning/identificadores-em-ingles/*.tsv` | mapeamentos, inventário inicial e final |
| `docs/aceite-identificadores-em-ingles.md` | fechamento |

Formato dos TSV (cabeçalho na primeira linha, tab como separador, sem aspas):

| Arquivo | Colunas |
|---|---|
| `inventario-*.tsv` | `pacote` `especie` `endereco` `nome` `posicao` `exportado` `grupo` |
| `grupos-*.tsv` (saída do inventário) | `grupo` `pacote` `tipo` `metodo` `papel` `posicao` |
| `mapeamento-<fase>-<pacote>.tsv` (entrada do `rf`) | `pacote` `especie` `endereco` `novo` `motivo` |
| `grupos-de-interface-<fase>.tsv` (entrada do `ifacerename`) | `pacote` `interface` `metodo` `novo` |
| `mapeamento-pacotes.tsv`, `mapeamento-arquivos.tsv` | `velho` `novo` (caminhos relativos à raiz) |
| `mapeamento-testes-<pacote>.tsv` | `pacote` `velho` `novo` |
| `subtestes-<pacote>.tsv` | `arquivo` `velho` `novo` |
| `mapeamento-chaves-de-log.tsv` | `velho` `novo` |
| `mapeamento-contratos.tsv` | `onde` `velho` `novo` |

`especie` ∈ `type func method field const var param local label ifacemethod`.
`endereco` é o endereço `rf`: `Name`, `T.M`, `T.F`, `F.x`, `T.M.x`, `T.M.r`
(receptor). `posicao` é `arquivo:linha:coluna` relativo à raiz (a coluna é o
que o `gopls rename -w` recebe). `grupo` é `G<n>` ou vazio. `especie` pode
carregar o sufixo `#ambiguo` (endereço declarado mais de uma vez na função)
ou `#gopls` (campo de struct local): os dois significam "não vai ao rf; o
aplicador usa gopls por posição".

---

## Fase 0 — Preparação

### Tarefa 0.1: Glossário e norma nova

**Arquivos:**
- Criar: `docs/glossario.md`
- Criar: `internal/gates/glossario_test.go`
- Modificar: `CLAUDE.md:8-10` (parágrafo "Todo o repositório é em PT-BR")

**Interfaces:**
- Produz: `docs/glossario.md` com uma tabela por seção, colunas `PT-BR | inglês | onde aparece`; é o que todo tradutor e juiz das fases seguintes cita.

- [ ] **Passo 1: Escrever o gate, que falha porque o glossário não existe**

`internal/gates/glossario_test.go`:

```go
// Package gates — este arquivo confere o glossário PT-BR ↔ inglês.
package gates

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// O glossário é normativo (spec de identificadores em inglês, §1): comentário
// novo cita o termo PT-BR dele, identificador novo usa o termo inglês dele.
// Um glossário com célula vazia ou com o mesmo termo em duas linhas devolve
// duas respostas para a mesma pergunta, e aí cada agente escolhe a sua — que
// é o que este gate existe para impedir. Ele lê a tabela como dado, linha a
// linha, e não por grep em prosa.
func TestGlossarioTemAsDuasColunasESemTermoDuplicado(t *testing.T) {
	caminho := filepath.Join(raizDoRepo, "docs", "glossario.md")
	f, err := os.Open(caminho)
	if err != nil {
		t.Fatalf("o glossário não existe: %v", err)
	}
	defer f.Close()

	vistos := map[string]int{}
	linhas, tabela := 0, 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		linhas++
		l := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(l, "|") || strings.HasPrefix(l, "|---") || strings.HasPrefix(l, "| PT-BR") {
			continue
		}
		celulas := strings.Split(strings.Trim(l, "|"), "|")
		if len(celulas) < 2 {
			t.Errorf("linha %d: tabela com menos de duas colunas: %q", linhas, l)
			continue
		}
		ptbr := strings.TrimSpace(celulas[0])
		en := strings.TrimSpace(celulas[1])
		if ptbr == "" || en == "" {
			t.Errorf("linha %d: célula vazia: %q", linhas, l)
		}
		if antes, ok := vistos[ptbr]; ok {
			t.Errorf("linha %d: termo %q repetido (já na linha %d)", linhas, ptbr, antes)
		}
		vistos[ptbr] = linhas
		tabela++
	}
	if tabela < 40 {
		t.Fatalf("o glossário tem %d termos; o spec semeia mais de 40 — ou a tabela não foi lida, ou foi esvaziada", tabela)
	}
}
```

`raizDoRepo` já existe em `internal/gates` (é usado por `jdbc_test.go:223`).

- [ ] **Passo 2: Rodar e ver vermelho**

Run: `go test ./internal/gates/ -run TestGlossarioTemAsDuasColunasESemTermoDuplicado -count=1`
Esperado: FAIL com "o glossário não existe".

- [ ] **Passo 3: Escrever `docs/glossario.md`**

Conteúdo: o §1 do spec (tabela "quem lê"), depois o §6 inteiro (estilo Go;
"o que já está em inglês não muda"; regra de teste; chaves de log;
mensagens; contratos; colisão), com a tabela de domínio expandida para uma
linha por termo e a coluna "onde aparece" preenchida (pacote ou docs). Além
dos 42 termos do spec, entram os que o inventário da Tarefa 0.7 vai cobrar
e que já se sabe: `apenda→append`, `lápide→tombstone`, `orçamento→budget`,
`transbordo→spill`, `dobra→fold`, `denso→dense`, `esparso→sparse`,
`selado→sealed`, `sela→seal`, `união→union`, `avaliador→evaluator`,
`programa→program`, `regra→rule`, `moldura→frame`, `leitor→reader`,
`escritor→writer`, `fonte→source`, `iterador→iterator`, `tabela→table`,
`registro→registry` (o de virtuais) / `record` (a linha), `abertura→open`,
`compila→compile`, `coleta→collect`, `extração→extract`,
`estatísticas→stats`, `relatório→report`, `posição→position`,
`cadência→cadence`, `política→policy`, `corrida→race`, `desfecho→outcome`,
`perda→loss`, `resumo→summary`, `vazão→throughput`, `emvoo→inflight`,
`carência→grace`, `lacuna→gap`, `resíduo→residue`, `espelho→mirror`,
`órfã→orphan`, `paridade→parity`, `comportamento→behavior`,
`chave-texto→textkey`, `capacidades→capabilities`, `consumido→consumed`,
`contado→counted`, `derivadas→derived`, `congelada→frozen`,
`vencimentos→expiries`, `prazos→deadlines`, `janela→window`.

Encerrar com a seção "Como usar": comentário cita o PT-BR; identificador usa
o inglês; termo novo entra aqui ANTES de ser usado.

- [ ] **Passo 4: Rodar e ver verde**

Run: `go test ./internal/gates/ -run TestGlossarioTemAsDuasColunasESemTermoDuplicado -count=1`
Esperado: PASS.

- [ ] **Passo 5: Mutação do gate**

Duplicar uma linha da tabela em `docs/glossario.md`, rodar o gate, ver
vermelho com "termo ... repetido"; reverter; ver verde. Registrar os dois
resultados no aceite (Tarefa 6.3).

- [ ] **Passo 6: Trocar a norma no CLAUDE.md**

Substituir, em `CLAUDE.md`, o parágrafo:

```
**Todo o repositório é em PT-BR** — código, identificadores, nomes de teste
(`TestTopNReaproveitaOBufferDaEntradaDespejada`), comentários, logs, docs e
mensagens de commit. Commits na voz do time, por bloco lógico.
```

por:

```
**Idioma é decidido por quem lê** (`docs/glossario.md` é normativo): prosa em
PT-BR — docs, godoc e comentários, mensagens de commit, mensagens de falha de
teste (`t.Errorf`); tudo que o compilador, o operador ou um cliente lê em
inglês — identificadores, pacotes e arquivos `.go`, nomes de teste
(`TestTopNReaproveitaOBufferDaEntradaDespejada` é o formato: sujeito, ação,
condição), mensagens de erro e de log, chaves de log, config, wire, métricas.
O glossário liga o termo PT-BR da prosa ao identificador inglês. Commits na
voz do time, por bloco lógico.
```

O nome de teste citado continua existindo até a Fase 4, que o troca pelo
mapeamento; `TestEvidenciasCitadasNosDocumentosExistem` lê o CLAUDE.md e é
por isso que a citação não pode ser inventada aqui.

- [ ] **Passo 7: Suíte dos gates e commit**

Run: `go test ./internal/gates/ ./internal/cli/ -run 'Glossario|Evidencias|Citacoes' -count=1`
Esperado: PASS.

```bash
git add docs/glossario.md internal/gates/glossario_test.go CLAUDE.md
git commit -m "docs: glossário PT-BR ↔ inglês e a norma de idioma por quem lê"
```

### Tarefa 0.2: Dependência `golang.org/x/tools` e esqueleto de `scripts/renameinv`

**Arquivos:**
- Modificar: `go.mod`, `go.sum`
- Criar: `scripts/renameinv/main.go`
- Criar: `scripts/renameinv/testdata/modulo/go.mod`, `a/a.go`, `a/a_test.go`, `b/b.go`, `c/c.go`, `c/c_test.go`

**Interfaces:**
- Produz: o módulo de teste `exemplo.local/modulo`, reutilizado pelo `ifacerename` (Tarefa 0.4) — os dois leem `testdata/modulo` do `renameinv` por caminho relativo `../renameinv/testdata/modulo`.

- [ ] **Passo 1: Adicionar a dependência**

```bash
go get golang.org/x/tools/go/packages@v0.49.0
```

Run: `go build ./... && go mod tidy && git diff --stat go.mod go.sum`
Esperado: `go.mod` ganha `golang.org/x/tools v0.49.0` (e os indiretos que o
tidy puxar); `nishidb` continua sem dependência de runtime nova — `go list
-deps ./cmd/nishidb | grep x/tools` devolve vazio.

- [ ] **Passo 2: Escrever o módulo de teste**

`scripts/renameinv/testdata/modulo/go.mod`:

```
module exemplo.local/modulo

go 1.25
```

`a/a.go`:

```go
// Package a declara a interface e o consumidor.
package a

// Escritor recebe registros. Fecha libera o que Apenda reteve.
type Escritor interface {
	// Apenda apenda um registro.
	Apenda(chave []byte) error
	Fecha() error
}

// Contagem é o que Roda devolve.
type Contagem struct {
	// Total é o total apendado.
	Total int
	erros int
}

// Roda apenda em e e conta.
func Roda(e Escritor, chaves [][]byte) Contagem {
	var c Contagem
	for _, chave := range chaves {
		if err := e.Apenda(chave); err != nil {
			c.erros++
			continue
		}
		c.Total++
	}
	return c
}
```

`a/a_test.go`:

```go
package a

import "testing"

type escritorFalso struct{ n int }

func (f *escritorFalso) Apenda(chave []byte) error { f.n++; return nil }
func (f *escritorFalso) Fecha() error              { return nil }

func TestRodaConta(t *testing.T) {
	f := &escritorFalso{}
	c := Roda(f, [][]byte{[]byte("x"), []byte("y")})
	if c.Total != 2 || f.n != 2 {
		t.Fatalf("Total=%d n=%d", c.Total, f.n)
	}
	t.Run("vazio conta zero", func(t *testing.T) {
		if Roda(f, nil).Total != 0 {
			t.Fatal("contou")
		}
	})
}
```

`b/b.go`:

```go
// Package b implementa a.Escritor em outro pacote.
package b

// Arquivo é um Escritor de mentira.
type Arquivo struct {
	// Linhas guarda o que Apenda recebeu.
	Linhas [][]byte
}

// Apenda guarda a chave. Ver a.Escritor.
func (arq *Arquivo) Apenda(chave []byte) error {
	arq.Linhas = append(arq.Linhas, chave)
	return nil
}

// Fecha não faz nada.
func (arq *Arquivo) Fecha() error { return nil }

// Abre devolve um Arquivo vazio.
func Abre() *Arquivo {
	var tamanho int
	tamanho = 0
	_ = tamanho
	return &Arquivo{}
}
```

`c/c.go`:

```go
// Package c tem uma interface que se SOBREPÕE a a.Escritor: só Apenda.
package c

import (
	"exemplo.local/modulo/a"
	"exemplo.local/modulo/b"
)

// SoApenda é satisfeita por tudo que satisfaz a.Escritor.
type SoApenda interface {
	Apenda(chave []byte) error
}

// Usa recebe as duas formas.
func Usa(s SoApenda, e a.Escritor) {
	_ = s.Apenda(nil)
	_ = e.Apenda(nil)
	arq := b.Abre()
	_ = arq.Apenda([]byte("z"))
}
```

`c/c_test.go`:

```go
package c_test

import (
	"testing"

	"exemplo.local/modulo/a"
	"exemplo.local/modulo/c"
)

type dublê struct{}

func (dublê) Apenda(chave []byte) error { return nil }
func (dublê) Fecha() error              { return nil }

func TestUsaAceitaOsDois(t *testing.T) {
	var e a.Escritor = dublê{}
	c.Usa(dublê{}, e)
}
```

Run: `cd scripts/renameinv/testdata/modulo && go vet ./... && go test ./...`
Esperado: PASS (o módulo é válido antes de qualquer rename).

`go build ./...` da raiz IGNORA `testdata/` — é por isso que o módulo pode
morar ali sem entrar no build do nishidb.

- [ ] **Passo 3: Esqueleto do comando**

`scripts/renameinv/main.go`:

```go
// Command renameinv inventaria o módulo Go do diretório dado: toda declaração
// nomeada (tipos, funções, métodos, campos, constantes, variáveis, parâmetros,
// locais, rótulos) com o endereço que o `rf` entende, e os grupos de interface
// — componentes conexas do grafo (interface, método) ↔ (tipo concreto, método).
//
// É a entrada dos agentes do ciclo de identificadores em inglês
// (planning/identificadores-em-ingles.md, §5 e §8) e a prova de completude do
// fechamento: o inventário do fim é comparado ao do início, e nada pode sobrar
// sem decisão. Grep por acento não prova nada; isto é go/types.
//
//	go run ./scripts/renameinv -dir . -out inventario.tsv -groups grupos.tsv
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("renameinv", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "raiz do módulo a inventariar")
	out := fs.String("out", "", "TSV do inventário (obrigatório)")
	groups := fs.String("groups", "", "TSV dos grupos de interface (obrigatório)")
	pattern := fs.String("pkg", "./...", "padrão de pacotes, como em go list")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *out == "" || *groups == "" {
		fmt.Fprintln(stderr, "renameinv: -out e -groups são obrigatórios")
		return 2
	}
	inv, err := Inventory(*dir, *pattern)
	if err != nil {
		fmt.Fprintf(stderr, "renameinv: %v\n", err)
		return 1
	}
	if err := writeTSV(*out, inv.Rows); err != nil {
		fmt.Fprintf(stderr, "renameinv: %v\n", err)
		return 1
	}
	if err := writeTSV(*groups, inv.GroupRows); err != nil {
		fmt.Fprintf(stderr, "renameinv: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%d declarações, %d grupos de interface\n", len(inv.Rows)-1, inv.Groups)
	return 0
}

func writeTSV(path string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, r := range rows {
		for i, c := range r {
			if i > 0 {
				if _, err := f.WriteString("\t"); err != nil {
					return err
				}
			}
			if _, err := f.WriteString(c); err != nil {
				return err
			}
		}
		if _, err := f.WriteString("\n"); err != nil {
			return err
		}
	}
	return nil
}
```

`Inventory` vem na Tarefa 0.3; até lá o pacote não compila, e é assim que a
tarefa seguinte começa vermelha.

- [ ] **Passo 4: Commit do módulo de teste e da dependência**

```bash
git add go.mod go.sum scripts/renameinv/testdata scripts/renameinv/main.go
git commit -m "scripts: dependência x/tools e módulo de teste para as ferramentas de rename"
```

### Tarefa 0.3: `scripts/renameinv` — inventário e grupos de interface

**Arquivos:**
- Criar: `scripts/renameinv/inventory.go`, `scripts/renameinv/groups.go`
- Criar: `scripts/renameinv/inventory_test.go`, `scripts/renameinv/groups_test.go`

**Interfaces:**
- Consome: `testdata/modulo` (Tarefa 0.2).
- Produz: `func Inventory(dir, pattern string) (*Result, error)` com
  `Result{Rows, GroupRows [][]string; Groups int}` (primeira linha de cada é o
  cabeçalho) e `func LoadModule(dir, pattern string) ([]*packages.Package, *token.FileSet, error)`
  e `func InterfaceGroups(pkgs []*packages.Package, fset *token.FileSet) *Groups`
  com `Groups{ByKey map[string]int; Members map[int][]Member}` e
  `Member{Pkg, Type, Method, Role, Pos string; Obj *types.Func}` — o
  `ifacerename` (Tarefa 0.4) importa esta lógica copiando `groups.go` para o
  seu pacote (são `package main` separados; a cópia é deliberada e o teste de
  paridade da Tarefa 0.4 garante que os dois arquivos são idênticos).

- [ ] **Passo 1: Escrever os testes do inventário**

`scripts/renameinv/inventory_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func linha(t *testing.T, rows [][]string, especie, endereco string) []string {
	t.Helper()
	for _, r := range rows[1:] {
		if r[1] == especie && r[2] == endereco {
			return r
		}
	}
	t.Fatalf("inventário sem a linha %s %s", especie, endereco)
	return nil
}

func TestInventarioEnderecaCadaEspecieComoORfEntende(t *testing.T) {
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(inv.Rows[0], " "); got != "pacote especie endereco nome posicao exportado grupo" {
		t.Fatalf("cabeçalho: %q", got)
	}
	casos := []struct{ especie, endereco, pacote, exportado string }{
		{"type", "Escritor", "a", "sim"},
		{"ifacemethod", "Escritor.Apenda", "a", "sim"},
		{"type", "Contagem", "a", "sim"},
		{"field", "Contagem.Total", "a", "sim"},
		{"field", "Contagem.erros", "a", "nao"},
		{"func", "Roda", "a", "sim"},
		{"param", "Roda.e", "a", "nao"},
		{"param", "Roda.chaves", "a", "nao"},
		{"local", "Roda.c", "a", "nao"},
		{"local", "Roda.chave", "a", "nao"},
		{"method", "Arquivo.Apenda", "b", "sim"},
		{"param", "Arquivo.Apenda.chave", "b", "nao"},
		{"local", "Abre.tamanho", "b", "nao"},
		{"type", "escritorFalso", "a", "nao"},
		{"method", "escritorFalso.Apenda", "a", "sim"},
		{"func", "TestRodaConta", "a", "sim"},
		{"type", "dublê", "c", "nao"},
		{"method", "dublê.Fecha", "c", "sim"},
	}
	for _, c := range casos {
		r := linha(t, inv.Rows, c.especie, c.endereco)
		if r[0] != c.pacote || r[5] != c.exportado {
			t.Errorf("%s %s: pacote=%q exportado=%q, esperado %q %q", c.especie, c.endereco, r[0], r[5], c.pacote, c.exportado)
		}
	}
}

func TestInventarioNaoEnderecaReceptorCurtoNemCampoEmbutido(t *testing.T) {
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range inv.Rows[1:] {
		if r[2] == "Arquivo.Apenda.arq" {
			t.Errorf("receptor de três letras entrou: %v — receptor só entra quando tem quatro letras ou mais", r)
		}
		if r[1] == "field" && r[3] == "" {
			t.Errorf("campo embutido entrou sem nome: %v", r)
		}
	}
}

func TestInventarioMarcaLocalAmbiguo(t *testing.T) {
	// Roda declara `chave` uma vez (no for). Abre declara `tamanho` uma vez.
	// Nenhum é ambíguo; o teste é o inverso: um endereço declarado duas vezes
	// na mesma função ganha o sufixo #ambiguo na especie, e o rf não o aceita
	// — cai para gopls por posição (spec §4.1).
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range inv.Rows[1:] {
		if strings.HasSuffix(r[1], "#ambiguo") {
			t.Errorf("o módulo de teste não tem local repetido, mas %v saiu ambíguo", r)
		}
	}
}

func TestInventarioDeduplicaAsVariantesDeTeste(t *testing.T) {
	// Com Tests: true, go/packages carrega `a`, `a [a.test]` e `a.test`; a
	// mesma declaração aparece nas três com a mesma posição. Uma linha só.
	inv, err := Inventory("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	vistos := map[string]int{}
	for _, r := range inv.Rows[1:] {
		vistos[r[4]]++
	}
	for pos, n := range vistos {
		if n > 1 {
			t.Errorf("posição %s inventariada %d vezes", pos, n)
		}
	}
}
```

- [ ] **Passo 2: Ver vermelho**

Run: `go test ./scripts/renameinv/ -count=1`
Esperado: FAIL de compilação — `Inventory` não existe.

- [ ] **Passo 3: Escrever `inventory.go`**

```go
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Result é o inventário pronto para virar TSV.
type Result struct {
	Rows      [][]string
	GroupRows [][]string
	Groups    int
}

// LoadModule carrega o módulo com testes. As variantes de teste
// (`a [a.test]`, `a_test [a.test]`) vêm junto porque os dublês de interface
// moram nelas, e um inventário que não os visse deixaria o ifacerename cego
// para metade dos implementadores.
func LoadModule(dir, pattern string) ([]*packages.Package, *token.FileSet, error) {
	fset := token.NewFileSet()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps | packages.NeedModule,
		Tests: true,
		Dir:   dir,
		Fset:  fset,
	}
	pkgs, err := packages.Load(cfg, pattern)
	if err != nil {
		return nil, nil, err
	}
	var errs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			errs = append(errs, e.Error())
		}
	})
	if len(errs) > 0 {
		return nil, nil, fmt.Errorf("o módulo não compila: %s", strings.Join(errs, "; "))
	}
	return pkgs, fset, nil
}

// modulePackages filtra os pacotes do próprio módulo (o Load traz as
// dependências por causa do NeedDeps).
func modulePackages(pkgs []*packages.Package) []*packages.Package {
	var out []*packages.Package
	seen := map[string]bool{}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if p.Module == nil || !p.Module.Main || seen[p.ID] {
			return
		}
		seen[p.ID] = true
		out = append(out, p)
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// packageDir devolve o diretório do pacote relativo à raiz do módulo, que é
// o que o `rf` recebe como diretório corrente e o que o TSV chama de pacote.
func packageDir(p *packages.Package, root string) string {
	if len(p.GoFiles) == 0 && len(p.CompiledGoFiles) == 0 {
		return p.PkgPath
	}
	files := p.GoFiles
	if len(files) == 0 {
		files = p.CompiledGoFiles
	}
	rel, err := filepath.Rel(root, filepath.Dir(files[0]))
	if err != nil {
		return p.PkgPath
	}
	return filepath.ToSlash(rel)
}

// row é uma declaração inventariada.
type row struct {
	pkg, especie, endereco, nome, pos, exportado, grupo string
}

// Inventory percorre Defs de todo pacote do módulo e classifica cada objeto
// pelo que ele é para o go/types e pelo lugar onde foi declarado.
func Inventory(dir, pattern string) (*Result, error) {
	pkgs, fset, err := LoadModule(dir, pattern)
	if err != nil {
		return nil, err
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	groups := InterfaceGroups(pkgs, fset)

	byPos := map[string]row{}
	for _, p := range modulePackages(pkgs) {
		pkgDir := packageDir(p, root)
		// Contagem de declarações do mesmo nome por função, para marcar o
		// endereço ambíguo que o rf recusaria.
		counts := map[string]int{}
		for _, f := range p.Syntax {
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					id, ok := n.(*ast.Ident)
					if !ok {
						return true
					}
					if obj := p.TypesInfo.Defs[id]; obj != nil {
						counts[funcAddress(fn)+"."+id.Name]++
					}
					return true
				})
			}
		}
		for id, obj := range p.TypesInfo.Defs {
			if obj == nil || id.Name == "_" {
				continue
			}
			pos := fset.Position(obj.Pos())
			if !strings.HasPrefix(pos.Filename, root) {
				continue
			}
			rel, _ := filepath.Rel(root, pos.Filename)
			posStr := fmt.Sprintf("%s:%d", filepath.ToSlash(rel), pos.Line)
			if _, ok := byPos[posStr+":"+id.Name]; ok {
				continue // variante de teste do mesmo arquivo
			}
			r, ok := classify(p, fset, id, obj, counts)
			if !ok {
				continue
			}
			r.pkg = pkgDir
			r.pos = posStr
			if obj.Exported() {
				r.exportado = "sim"
			} else {
				r.exportado = "nao"
			}
			if g, ok := groups.ByKey[posKey(fset, obj.Pos())]; ok {
				r.grupo = fmt.Sprintf("G%d", g)
			}
			byPos[posStr+":"+id.Name] = r
		}
	}

	rows := [][]string{{"pacote", "especie", "endereco", "nome", "posicao", "exportado", "grupo"}}
	keys := make([]string, 0, len(byPos))
	for k := range byPos {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := byPos[k]
		rows = append(rows, []string{r.pkg, r.especie, r.endereco, r.nome, r.pos, r.exportado, r.grupo})
	}
	return &Result{Rows: rows, GroupRows: groups.Rows(), Groups: len(groups.Members)}, nil
}

// funcAddress é `F` ou `T.M`, o prefixo de endereço rf para o que mora
// dentro da função.
func funcAddress(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return receiverTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func receiverTypeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverTypeName(t.X)
	case *ast.IndexExpr:
		return receiverTypeName(t.X)
	case *ast.IndexListExpr:
		return receiverTypeName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// classify decide especie e endereço. Devolve ok=false para o que não é
// renomeável por endereço: campo embutido, receptor curto, o pacote em si.
func classify(p *packages.Package, fset *token.FileSet, id *ast.Ident, obj types.Object, counts map[string]int) (row, bool) {
	r := row{nome: id.Name}
	switch o := obj.(type) {
	case *types.PkgName:
		return r, false
	case *types.TypeName:
		if o.Parent() == p.Types.Scope() {
			r.especie, r.endereco = "type", id.Name
			return r, true
		}
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		r.especie, r.endereco = "local", funcAddress(fn)+"."+id.Name
		return r, true
	case *types.Func:
		sig := o.Type().(*types.Signature)
		if sig.Recv() == nil {
			r.especie, r.endereco = "func", id.Name
			return r, true
		}
		recv := sig.Recv().Type()
		if ptr, ok := recv.(*types.Pointer); ok {
			recv = ptr.Elem()
		}
		named, ok := recv.(*types.Named)
		if !ok {
			return r, false
		}
		if _, isIface := named.Underlying().(*types.Interface); isIface {
			r.especie = "ifacemethod"
		} else {
			r.especie = "method"
		}
		r.endereco = named.Obj().Name() + "." + id.Name
		return r, true
	case *types.Const:
		if o.Parent() == p.Types.Scope() {
			r.especie, r.endereco = "const", id.Name
			return r, true
		}
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		r.especie, r.endereco = "local", funcAddress(fn)+"."+id.Name
		return r, true
	case *types.Label:
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		r.especie, r.endereco = "label", funcAddress(fn)+"."+id.Name
		return r, true
	case *types.Var:
		if o.IsField() {
			owner := fieldOwner(p, id)
			if owner == "" || o.Embedded() {
				return r, false
			}
			r.especie, r.endereco = "field", owner+"."+id.Name
			return r, true
		}
		if o.Parent() == p.Types.Scope() {
			r.especie, r.endereco = "var", id.Name
			return r, true
		}
		fn := enclosingFunc(p, id.Pos())
		if fn == nil {
			return r, false
		}
		addr := funcAddress(fn) + "." + id.Name
		switch {
		case isReceiver(fn, id):
			if len(id.Name) < 4 {
				return r, false
			}
			r.especie = "receiver"
		case isParamOrResult(fn, id):
			r.especie = "param"
		default:
			r.especie = "local"
		}
		if counts[addr] > 1 {
			r.especie += "#ambiguo"
		}
		r.endereco = addr
		return r, true
	}
	return r, false
}

// enclosingFunc acha a FuncDecl que contém a posição. Funções literais
// contam como parte da função que as declara — é assim que o rf endereça.
func enclosingFunc(p *packages.Package, pos token.Pos) *ast.FuncDecl {
	for _, f := range p.Syntax {
		if pos < f.Pos() || pos > f.End() {
			continue
		}
		for _, d := range f.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && pos >= fn.Pos() && pos <= fn.End() {
				return fn
			}
		}
	}
	return nil
}

func isReceiver(fn *ast.FuncDecl, id *ast.Ident) bool {
	if fn.Recv == nil {
		return false
	}
	for _, f := range fn.Recv.List {
		for _, n := range f.Names {
			if n == id {
				return true
			}
		}
	}
	return false
}

func isParamOrResult(fn *ast.FuncDecl, id *ast.Ident) bool {
	lists := []*ast.FieldList{fn.Type.Params, fn.Type.Results}
	for _, l := range lists {
		if l == nil {
			continue
		}
		for _, f := range l.List {
			for _, n := range f.Names {
				if n == id {
					return true
				}
			}
		}
	}
	return false
}

// fieldOwner devolve o endereço do dono de um campo: o nome do tipo para
// struct nomeada, ou `V` para `var V struct{...}`. Struct anônima em outro
// lugar (campo de campo, parâmetro) não tem endereço rf e devolve "".
func fieldOwner(p *packages.Package, id *ast.Ident) string {
	for _, f := range p.Syntax {
		if id.Pos() < f.Pos() || id.Pos() > f.End() {
			continue
		}
		var owner string
		ast.Inspect(f, func(n ast.Node) bool {
			if owner != "" {
				return false
			}
			switch d := n.(type) {
			case *ast.TypeSpec:
				if st, ok := d.Type.(*ast.StructType); ok && contains(st, id) {
					owner = d.Name.Name
					return false
				}
			case *ast.ValueSpec:
				if st, ok := d.Type.(*ast.StructType); ok && contains(st, id) && len(d.Names) == 1 {
					owner = d.Names[0].Name
					return false
				}
			}
			return true
		})
		return owner
	}
	return ""
}

func contains(st *ast.StructType, id *ast.Ident) bool {
	for _, f := range st.Fields.List {
		for _, n := range f.Names {
			if n == id {
				return true
			}
		}
	}
	return false
}
```

- [ ] **Passo 4: Escrever os testes dos grupos**

`scripts/renameinv/groups_test.go`:

```go
package main

import (
	"testing"
)

func TestGruposDeInterfaceFechamTransitivamente(t *testing.T) {
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	g := InterfaceGroups(pkgs, fset)
	// a.Escritor.Apenda, c.SoApenda.Apenda, b.Arquivo.Apenda, a.escritorFalso.Apenda
	// e c_test.dublê.Apenda são UM grupo: SoApenda não implementa Escritor,
	// mas os dois são satisfeitos por Arquivo, e é por Arquivo que se ligam.
	var apendas []Member
	for _, ms := range g.Members {
		n := 0
		for _, m := range ms {
			if m.Method == "Apenda" {
				n++
			}
		}
		if n > 0 {
			if n != 5 {
				t.Fatalf("o grupo de Apenda tem %d membros, esperado 5: %+v", n, ms)
			}
			apendas = ms
		}
	}
	if apendas == nil {
		t.Fatal("nenhum grupo com Apenda")
	}
	papeis := map[string]string{}
	for _, m := range apendas {
		papeis[m.Pkg+"."+m.Type] = m.Role
	}
	if papeis["a.Escritor"] != "interface" || papeis["c.SoApenda"] != "interface" {
		t.Errorf("papéis das interfaces: %v", papeis)
	}
	if papeis["b.Arquivo"] != "implementador" || papeis["c.dublê"] != "implementador" || papeis["a.escritorFalso"] != "implementador" {
		t.Errorf("papéis dos implementadores (dublês de teste inclusos): %v", papeis)
	}
}

func TestGruposSeparamMetodosDeNomesDiferentes(t *testing.T) {
	pkgs, fset, err := LoadModule("testdata/modulo", "./...")
	if err != nil {
		t.Fatal(err)
	}
	g := InterfaceGroups(pkgs, fset)
	for _, ms := range g.Members {
		nomes := map[string]bool{}
		for _, m := range ms {
			nomes[m.Method] = true
		}
		if len(nomes) != 1 {
			t.Errorf("grupo mistura métodos de nomes diferentes: %v", nomes)
		}
	}
	// Fecha é grupo próprio: Escritor + Arquivo + escritorFalso + dublê = 4.
	for _, ms := range g.Members {
		if ms[0].Method == "Fecha" && len(ms) != 4 {
			t.Errorf("Fecha tem %d membros, esperado 4", len(ms))
		}
	}
}
```

- [ ] **Passo 5: Escrever `groups.go`**

```go
package main

import (
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// Member é um nó do grafo de interfaces: um método de interface ou o método
// de um tipo concreto que o satisfaz.
type Member struct {
	Pkg, Type, Method, Role, Pos string
	Obj                          *types.Func
}

// Groups são as componentes conexas. ByKey liga a posição de declaração do
// método (que é a identidade que sobrevive às variantes de teste) ao id do
// grupo; Members lista cada grupo.
type Groups struct {
	ByKey   map[string]int
	Members map[int][]Member
}

// posKey é a identidade de um objeto entre variantes de pacote: o mesmo
// método aparece como *types.Func distinto em `a` e em `a [a.test]`, mas na
// mesma posição do mesmo arquivo.
func posKey(fset *token.FileSet, pos token.Pos) string {
	p := fset.Position(pos)
	return fmt.Sprintf("%s:%d", p.Filename, p.Offset)
}

// InterfaceGroups monta o grafo e o fecha por union-find.
//
// Aresta: I.M ~ T.M quando T (ou *T) implementa I. Interface que implementa
// interface também liga (I2 ⊇ I1). Duas interfaces que só compartilham o nome
// de um método, sem uma implementar a outra, ligam-se pelos implementadores
// comuns — que é exatamente o caso de query.Store/executor.Store/planner.Store
// no NishiDB, todos satisfeitos por storage.Store.
func InterfaceGroups(pkgs []*packages.Package, fset *token.FileSet) *Groups {
	type namedIn struct {
		named *types.Named
		pkg   *packages.Package
	}
	var ifaces, concretes []namedIn
	seen := map[string]bool{}
	for _, p := range modulePackages(pkgs) {
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok {
				continue
			}
			key := posKey(fset, tn.Pos())
			if seen[key] {
				continue
			}
			seen[key] = true
			if _, isIface := named.Underlying().(*types.Interface); isIface {
				ifaces = append(ifaces, namedIn{named, p})
			} else {
				concretes = append(concretes, namedIn{named, p})
			}
		}
	}

	parent := map[string]string{}
	members := map[string]Member{}
	var find func(string) string
	find = func(k string) string {
		if parent[k] == "" {
			parent[k] = k
			return k
		}
		if parent[k] != k {
			parent[k] = find(parent[k])
		}
		return parent[k]
	}
	union := func(a, b string) { parent[find(a)] = find(b) }
	add := func(m Member) string {
		k := posKey(fset, m.Obj.Pos())
		if _, ok := members[k]; !ok {
			members[k] = m
			find(k)
		}
		return k
	}
	root := moduleRoot(pkgs)
	member := func(n namedIn, fn *types.Func, role string) Member {
		p := fset.Position(fn.Pos())
		rel, _ := filepath.Rel(root, p.Filename)
		return Member{
			Pkg:    packageDir(n.pkg, root),
			Type:   n.named.Obj().Name(),
			Method: fn.Name(),
			Role:   role,
			Pos:    fmt.Sprintf("%s:%d", filepath.ToSlash(rel), p.Line),
			Obj:    fn,
		}
	}

	for _, in := range ifaces {
		it := in.named.Underlying().(*types.Interface)
		if it.NumMethods() == 0 {
			continue
		}
		for i := 0; i < it.NumMethods(); i++ {
			im := it.Method(i)
			ik := add(member(in, im, "interface"))
			for _, c := range concretes {
				if !types.Implements(c.named, it) && !types.Implements(types.NewPointer(c.named), it) {
					continue
				}
				obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(c.named), true, im.Pkg(), im.Name())
				cm, ok := obj.(*types.Func)
				if !ok {
					continue
				}
				union(ik, add(member(c, cm, "implementador")))
			}
			for _, other := range ifaces {
				if other.named == in.named || !types.Implements(other.named, it) {
					continue
				}
				oit := other.named.Underlying().(*types.Interface)
				for j := 0; j < oit.NumMethods(); j++ {
					if om := oit.Method(j); om.Name() == im.Name() {
						union(ik, add(member(other, om, "interface")))
					}
				}
			}
		}
	}

	g := &Groups{ByKey: map[string]int{}, Members: map[int][]Member{}}
	roots := map[string]int{}
	keys := make([]string, 0, len(members))
	for k := range members {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := find(k)
		id, ok := roots[r]
		if !ok {
			id = len(roots) + 1
			roots[r] = id
		}
		g.ByKey[k] = id
		g.Members[id] = append(g.Members[id], members[k])
	}
	return g
}

func moduleRoot(pkgs []*packages.Package) string {
	for _, p := range modulePackages(pkgs) {
		if p.Module != nil && p.Module.Dir != "" {
			return p.Module.Dir
		}
	}
	return "."
}

// Rows é o TSV dos grupos.
func (g *Groups) Rows() [][]string {
	rows := [][]string{{"grupo", "pacote", "tipo", "metodo", "papel", "posicao"}}
	ids := make([]int, 0, len(g.Members))
	for id := range g.Members {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		ms := g.Members[id]
		sort.Slice(ms, func(i, j int) bool {
			return strings.Compare(ms[i].Role+ms[i].Pkg+ms[i].Type, ms[j].Role+ms[j].Pkg+ms[j].Type) < 0
		})
		for _, m := range ms {
			rows = append(rows, []string{fmt.Sprintf("G%d", id), m.Pkg, m.Type, m.Method, m.Role, m.Pos})
		}
	}
	return rows
}
```

- [ ] **Passo 6: Ver verde**

Run: `go test ./scripts/renameinv/ -count=1 -v`
Esperado: PASS nos seis testes.

- [ ] **Passo 7: Mutação do fecho**

Em `groups.go`, comentar o laço `for _, other := range ifaces` (a ligação
interface ⊇ interface). O teste `TestGruposDeInterfaceFechamTransitivamente`
continua verde? Ele DEVE continuar verde — `SoApenda` liga a `Escritor` por
`Arquivo`, não por implementação direta. Então a mutação que prova o teste é
outra: comentar o `union(ik, add(member(c, cm, "implementador")))`. Rodar,
ver vermelho ("tem 2 membros, esperado 5"); reverter; verde. Relatar no
aceite.

- [ ] **Passo 8: Rodar sobre o repositório real e commitar**

```bash
go run ./scripts/renameinv -dir . -out planning/identificadores-em-ingles/inventario-inicial.tsv -groups planning/identificadores-em-ingles/grupos-inicial.tsv
```

Esperado: stdout com "N declarações, 28 grupos de interface" ou perto disso
(o dimensionamento do spec achou 28 interfaces com métodos; grupos são
componentes por MÉTODO, então o número é maior — cada nome de método é
grupo próprio). Conferir que `grupos-inicial.tsv` tem `storage.Store` e
`fakestorage.Store` no mesmo grupo de `materializer.Store.Put` que
`query.Store`, `executor.Store` e `planner.Store` dividem os grupos de
`Lookup`.

```bash
git add scripts/renameinv planning/identificadores-em-ingles/inventario-inicial.tsv planning/identificadores-em-ingles/grupos-inicial.tsv
git commit -m "scripts: renameinv, o inventário do módulo com endereços rf e grupos de interface"
```

### Tarefa 0.4: `scripts/ifacerename` — rename de grupo de interface

**Arquivos:**
- Criar: `scripts/ifacerename/main.go`, `scripts/ifacerename/groups.go` (cópia byte a byte de `scripts/renameinv/groups.go`), `scripts/ifacerename/inventory_helpers.go` (cópia de `LoadModule`, `modulePackages`, `packageDir`, `moduleRoot`, `posKey` — as funções que `groups.go` usa), `scripts/ifacerename/rewrite.go`
- Criar: `scripts/ifacerename/rewrite_test.go`, `scripts/ifacerename/paridade_test.go`

**Interfaces:**
- Consome: `Groups`/`Member`/`LoadModule` (mesma forma da Tarefa 0.3); `testdata/modulo` do `renameinv`.
- Produz: `func Apply(dir string, entries []Entry, dryRun bool) (*Report, error)` com `Entry{Pkg, Iface, Method, New string}` e `Report{Files []string; Renamed []Member}`; o comando `go run ./scripts/ifacerename -dir . -groups grupos-de-interface-2.tsv [-dry-run]`.

- [ ] **Passo 1: Teste de paridade das cópias**

`scripts/ifacerename/paridade_test.go`:

```go
package main

import (
	"os"
	"testing"
)

// groups.go é copiado do renameinv porque os dois são `package main` e não
// podem se importar. A cópia divergir em silêncio é o defeito clássico de
// "dois lugares para o mesmo número"; este teste é o gate da doutrina para
// ele. Quem mudar um, muda o outro no mesmo commit.
func TestGroupsEhCopiaFielDoRenameinv(t *testing.T) {
	for _, par := range [][2]string{
		{"groups.go", "../renameinv/groups.go"},
	} {
		a, err := os.ReadFile(par[0])
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(par[1])
		if err != nil {
			t.Fatal(err)
		}
		if string(a) != string(b) {
			t.Errorf("%s divergiu de %s", par[0], par[1])
		}
	}
}
```

- [ ] **Passo 2: Testes do rewrite**

`scripts/ifacerename/rewrite_test.go`:

```go
package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// copiaModulo leva o módulo de teste para um diretório descartável: o
// rewrite escreve em disco, e o testdata não pode mudar entre testes.
func copiaModulo(t *testing.T) string {
	t.Helper()
	dst := t.TempDir()
	src := filepath.Join("..", "renameinv", "testdata", "modulo")
	err := filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func leArquivo(t *testing.T, dir, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func buildEVet(t *testing.T, dir string) error {
	t.Helper()
	for _, args := range [][]string{{"build", "./..."}, {"vet", "./..."}} {
		cmd := exec.Command("go", args...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOFLAGS=-buildvcs=false")
		if out, err := cmd.CombinedOutput(); err != nil {
			return &erroDeBuild{args[0], string(out)}
		}
	}
	return nil
}

type erroDeBuild struct{ etapa, saida string }

func (e *erroDeBuild) Error() string { return e.etapa + ": " + e.saida }

func TestRenomeiaOGrupoInteiroEOBuildFicaVerde(t *testing.T) {
	dir := copiaModulo(t)
	rep, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := buildEVet(t, dir); err != nil {
		t.Fatalf("o módulo não compila depois do rename: %v", err)
	}
	for _, rel := range []string{"a/a.go", "a/a_test.go", "b/b.go", "c/c.go", "c/c_test.go"} {
		s := leArquivo(t, dir, rel)
		if strings.Contains(s, "Apenda(") {
			t.Errorf("%s ainda tem Apenda(", rel)
		}
	}
	// Uso por seletor, em outro pacote, e o dublê do pacote externo de teste.
	if !strings.Contains(leArquivo(t, dir, "c/c.go"), "arq.Append(") {
		t.Error("c.go: o seletor arq.Apenda não virou arq.Append")
	}
	if !strings.Contains(leArquivo(t, dir, "c/c_test.go"), "func (dublê) Append(") {
		t.Error("c_test.go: o dublê do pacote c_test não foi renomeado")
	}
	if len(rep.Renamed) != 5 {
		t.Errorf("renomeados: %d, esperado 5 (2 interfaces + 3 implementadores)", len(rep.Renamed))
	}
}

func TestReescreveAPrimeiraPalavraDoGodoc(t *testing.T) {
	dir := copiaModulo(t)
	if _, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"}}, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(leArquivo(t, dir, "a/a.go"), "// Append apenda um registro.") {
		t.Error("a.go: o godoc do método da interface não começa por Append")
	}
	if !strings.Contains(leArquivo(t, dir, "b/b.go"), "// Append guarda a chave. Ver a.Escritor.") {
		t.Error("b.go: o godoc do implementador não começa por Append (e a menção a.Escritor fica: é prosa)")
	}
	// A menção em godoc de OUTRA declaração fica para o renamecite.
	if !strings.Contains(leArquivo(t, dir, "a/a.go"), "Fecha libera o que Apenda reteve") {
		t.Error("a.go: o ifacerename mexeu em prosa alheia; isso é trabalho do renamecite")
	}
}

func TestRecusaColisaoNoMethodSet(t *testing.T) {
	dir := copiaModulo(t)
	_, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Fecha"}}, false)
	if err == nil || !strings.Contains(err.Error(), "já tem Fecha") {
		t.Fatalf("esperava recusa por colisão, veio %v", err)
	}
	if strings.Contains(leArquivo(t, dir, "b/b.go"), "Fecha(chave") {
		t.Error("escreveu apesar da recusa")
	}
}

func TestRecusaNovosDivergentesNoMesmoGrupo(t *testing.T) {
	dir := copiaModulo(t)
	_, err := Apply(dir, []Entry{
		{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"},
		{Pkg: "c", Iface: "SoApenda", Method: "Apenda", New: "Push"},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "mesmo grupo") {
		t.Fatalf("esperava recusa por nomes divergentes, veio %v", err)
	}
}

func TestDryRunNaoEscreve(t *testing.T) {
	dir := copiaModulo(t)
	antes := leArquivo(t, dir, "b/b.go")
	rep, err := Apply(dir, []Entry{{Pkg: "a", Iface: "Escritor", Method: "Apenda", New: "Append"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if leArquivo(t, dir, "b/b.go") != antes {
		t.Error("dry-run escreveu")
	}
	if len(rep.Files) != 5 {
		t.Errorf("dry-run listou %d arquivos, esperado 5", len(rep.Files))
	}
}
```

- [ ] **Passo 3: Ver vermelho**

Run: `go test ./scripts/ifacerename/ -count=1`
Esperado: FAIL de compilação — `Apply`, `Entry` não existem.

- [ ] **Passo 4: Copiar `groups.go` e os helpers**

```bash
cp scripts/renameinv/groups.go scripts/ifacerename/groups.go
```

`scripts/ifacerename/inventory_helpers.go` recebe, copiados de
`scripts/renameinv/inventory.go`, exatamente `LoadModule`, `modulePackages`
e `packageDir` (e `moduleRoot`, `posKey` já vêm em `groups.go`). O teste de
paridade cobre só `groups.go`; os helpers são pequenos e estáveis.

- [ ] **Passo 5: Escrever `rewrite.go`**

```go
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// Entry é uma linha do TSV de grupos de interface: qual método de qual
// interface, e o nome novo. O grupo inteiro (spec §4.2) vem do fecho.
type Entry struct {
	Pkg, Iface, Method, New string
}

// Report é o que o aplicador lê antes de commitar.
type Report struct {
	Files   []string
	Renamed []Member
}

// edit é uma substituição por posição: o texto velho é guardado para a
// escrita conferir que o arquivo ainda é o que o go/types leu.
type edit struct {
	off       int
	old, text string
}

// Apply renomeia os grupos das entradas numa passada só. Recusa antes de
// escrever: nome novo já presente no method set de qualquer membro, ou
// entradas do mesmo grupo com nomes novos diferentes.
func Apply(dir string, entries []Entry, dryRun bool) (*Report, error) {
	pkgs, fset, err := LoadModule(dir, "./...")
	if err != nil {
		return nil, err
	}
	groups := InterfaceGroups(pkgs, fset)

	// grupo → nome novo, com a checagem de divergência.
	newByGroup := map[int]string{}
	for _, e := range entries {
		gid, ok := findGroup(groups, e)
		if !ok {
			return nil, fmt.Errorf("%s.%s.%s: método de interface não encontrado no módulo", e.Pkg, e.Iface, e.Method)
		}
		if prev, ok := newByGroup[gid]; ok && prev != e.New {
			return nil, fmt.Errorf("%s.%s.%s: entradas do mesmo grupo G%d pedem %q e %q", e.Pkg, e.Iface, e.Method, gid, prev, e.New)
		}
		newByGroup[gid] = e.New
	}

	// Colisão: cada membro do grupo é um método de um tipo; o tipo não pode
	// já ter membro com o nome novo.
	oldByKey := map[string]string{}
	newByKey := map[string]string{}
	var renamed []Member
	for gid, novo := range newByGroup {
		for _, m := range groups.Members[gid] {
			recv := m.Obj.Type().(*types.Signature).Recv().Type()
			if ptr, ok := recv.(*types.Pointer); ok {
				recv = ptr.Elem()
			}
			if obj, _, _ := types.LookupFieldOrMethod(types.NewPointer(recv), true, m.Obj.Pkg(), novo); obj != nil {
				return nil, fmt.Errorf("%s.%s já tem %s: renomear %s colidiria", m.Pkg, m.Type, novo, m.Method)
			}
			k := posKey(fset, m.Obj.Pos())
			oldByKey[k] = m.Method
			newByKey[k] = novo
			renamed = append(renamed, m)
		}
	}

	edits := map[string]map[int]edit{} // arquivo → offset → edição
	addEdit := func(pos token.Pos, old, novo string) {
		p := fset.Position(pos)
		if edits[p.Filename] == nil {
			edits[p.Filename] = map[int]edit{}
		}
		edits[p.Filename][p.Offset] = edit{p.Offset, old, novo}
	}

	for _, p := range modulePackages(pkgs) {
		for id, obj := range p.TypesInfo.Defs {
			fn, ok := obj.(*types.Func)
			if !ok {
				continue
			}
			if novo, ok := newByKey[posKey(fset, fn.Pos())]; ok {
				addEdit(id.Pos(), id.Name, novo)
			}
		}
		for id, obj := range p.TypesInfo.Uses {
			fn, ok := obj.(*types.Func)
			if !ok {
				continue
			}
			if novo, ok := newByKey[posKey(fset, fn.Pos())]; ok {
				addEdit(id.Pos(), id.Name, novo)
			}
		}
		for _, f := range p.Syntax {
			for _, decl := range f.Decls {
				fd, ok := decl.(*ast.FuncDecl)
				if !ok || fd.Doc == nil {
					continue
				}
				obj := p.TypesInfo.Defs[fd.Name]
				if obj == nil {
					continue
				}
				if novo, ok := newByKey[posKey(fset, obj.Pos())]; ok {
					docEdit(fset, fd.Doc, fd.Name.Name, novo, addEdit)
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				it, ok := n.(*ast.InterfaceType)
				if !ok {
					return true
				}
				for _, m := range it.Methods.List {
					if m.Doc == nil || len(m.Names) == 0 {
						continue
					}
					obj := p.TypesInfo.Defs[m.Names[0]]
					if obj == nil {
						continue
					}
					if novo, ok := newByKey[posKey(fset, obj.Pos())]; ok {
						docEdit(fset, m.Doc, m.Names[0].Name, novo, addEdit)
					}
				}
				return true
			})
		}
	}

	rep := &Report{Renamed: renamed}
	files := make([]string, 0, len(edits))
	for f := range edits {
		files = append(files, f)
	}
	sort.Strings(files)
	rep.Files = files
	if dryRun {
		return rep, nil
	}
	for _, f := range files {
		if err := applyEdits(f, edits[f]); err != nil {
			return nil, err
		}
		if out, err := exec.Command("gofmt", "-w", f).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("gofmt %s: %v: %s", f, err, out)
		}
	}
	return rep, nil
}

// docEdit troca a PRIMEIRA palavra do godoc quando ela é o nome velho —
// a forma que a regra `exported` do revive cobra. Só a primeira linha do
// grupo; menções no meio da prosa são trabalho do renamecite.
func docEdit(fset *token.FileSet, doc *ast.CommentGroup, old, novo string, add func(token.Pos, string, string)) {
	c := doc.List[0]
	text := c.Text
	prefix := "// "
	if !strings.HasPrefix(text, prefix) {
		return
	}
	rest := text[len(prefix):]
	if rest != old && !strings.HasPrefix(rest, old+" ") && !strings.HasPrefix(rest, old+".") && !strings.HasPrefix(rest, old+",") {
		return
	}
	add(c.Pos()+token.Pos(len(prefix)), old, novo)
}

func applyEdits(file string, es map[int]edit) error {
	src, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	offs := make([]int, 0, len(es))
	for o := range es {
		offs = append(offs, o)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(offs)))
	out := append([]byte(nil), src...)
	for _, o := range offs {
		e := es[o]
		fim := o + len(e.old)
		if fim > len(out) || string(out[o:fim]) != e.old {
			return fmt.Errorf("%s: em %d esperava %q, o arquivo mudou desde a carga", file, o, e.old)
		}
		out = append(out[:o], append([]byte(e.text), out[fim:]...)...)
	}
	return os.WriteFile(file, out, 0o644)
}

func findGroup(g *Groups, e Entry) (int, bool) {
	for gid, ms := range g.Members {
		for _, m := range ms {
			if m.Role == "interface" && m.Pkg == e.Pkg && m.Type == e.Iface && m.Method == e.Method {
				return gid, true
			}
		}
	}
	return 0, false
}
```

`main.go`:

```go
// Command ifacerename renomeia métodos de interface com o fecho transitivo
// que nem o rf nem o gopls fazem entre pacotes (spec de identificadores em
// inglês, §4.2): interface, todas as interfaces que a compartilham por
// implementador comum, e todos os tipos concretos que as satisfazem —
// dublês de teste inclusos — numa passada só, com o godoc de cada declaração
// começando pelo nome novo. Recusa o lote inteiro antes de escrever se um
// membro já tiver o nome novo no method set.
//
//	go run ./scripts/ifacerename -dir . -groups planning/identificadores-em-ingles/grupos-de-interface-2.tsv
//
// Quem chama roda `go build ./... && go vet ./...` em seguida; se ficar
// vermelho, `git checkout -- .` e o mapeamento volta ao juiz.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ifacerename", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "raiz do módulo")
	groupsPath := fs.String("groups", "", "TSV: pacote interface metodo novo (obrigatório)")
	dryRun := fs.Bool("dry-run", false, "só lista o que mudaria")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *groupsPath == "" {
		fmt.Fprintln(stderr, "ifacerename: -groups é obrigatório")
		return 2
	}
	entries, err := readEntries(*groupsPath)
	if err != nil {
		fmt.Fprintf(stderr, "ifacerename: %v\n", err)
		return 2
	}
	rep, err := Apply(*dir, entries, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "ifacerename: recusado: %v\n", err)
		return 1
	}
	for _, m := range rep.Renamed {
		fmt.Fprintf(stdout, "%s\t%s.%s\t%s\n", m.Role, m.Pkg, m.Type, m.Method)
	}
	fmt.Fprintf(stdout, "%d métodos em %d arquivos\n", len(rep.Renamed), len(rep.Files))
	return 0
}

func readEntries(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Entry
	sc := bufio.NewScanner(f)
	first := true
	for sc.Scan() {
		l := sc.Text()
		if first {
			first = false
			if strings.HasPrefix(l, "pacote\t") {
				continue
			}
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		c := strings.Split(l, "\t")
		if len(c) < 4 {
			return nil, fmt.Errorf("%s: linha com %d colunas, esperado 4: %q", path, len(c), l)
		}
		out = append(out, Entry{Pkg: c[0], Iface: c[1], Method: c[2], New: c[3]})
	}
	return out, sc.Err()
}
```

- [ ] **Passo 6: Ver verde**

Run: `go test ./scripts/ifacerename/ -count=1 -v`
Esperado: PASS nos seis testes (paridade mais cinco do rewrite).

- [ ] **Passo 7: Mutações**

1. Em `rewrite.go`, dentro do laço de `Uses`, trocar `newByKey[posKey(fset, fn.Pos())]` por uma condição que só aceite `fn.Pkg() == p.Types` (mesmo pacote). Rodar: `TestRenomeiaOGrupoInteiroEOBuildFicaVerde` vermelho com "o módulo não compila depois do rename". Reverter, verde.
2. Remover a checagem `LookupFieldOrMethod(... novo)`. `TestRecusaColisaoNoMethodSet` vermelho. Reverter, verde.
3. Em `docEdit`, devolver sem editar. `TestReescreveAPrimeiraPalavraDoGodoc` vermelho. Reverter, verde.

Relatar as três no aceite.

- [ ] **Passo 8: Commit**

```bash
git add scripts/ifacerename
git commit -m "scripts: ifacerename renomeia grupos de interface com fecho transitivo numa passada"
```

### Tarefa 0.5: `scripts/renamecite` — passada de prosa

**Arquivos:**
- Criar: `scripts/renamecite/main.go`, `scripts/renamecite/rules.go`, `scripts/renamecite/gofile.go`, `scripts/renamecite/textfile.go`
- Criar: `scripts/renamecite/rules_test.go`, `scripts/renamecite/gofile_test.go`, `scripts/renamecite/textfile_test.go`

**Interfaces:**
- Produz: `type Mapping struct{ Idents []IdentEntry; Tests, LogKeys, Paths map[string]string; Subtests map[string]map[string]string }`, `IdentEntry{Pkg, Old, New string}` (`Old` é o último elemento do endereço, com o prefixo `T.` para métodos e campos: `Store.Apenda`), `func RewriteGoComments(src []byte, m *Mapping) ([]byte, []Skipped)`, `func RewriteText(src []byte, ext string, m *Mapping) ([]byte, []Skipped)`, `Skipped{Line int; Token, Motivo string}`; o comando `go run ./scripts/renamecite -dir . -idents a.tsv [-idents b.tsv] [-tests t.tsv] [-paths p.tsv] [-logkeys l.tsv] [-subtests s.tsv] -report relatorio.txt`.

- [ ] **Passo 1: Testes das regras**

`scripts/renamecite/rules_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

func mapa() *Mapping {
	m := NewMapping()
	m.AddIdent("internal/kafka", "Fila", "Queue")
	m.AddIdent("internal/kafka", "Fila.Empurra", "Queue.Push")
	m.AddIdent("internal/contadores", "Compila", "Compile")
	m.AddIdent("internal/schema", "Compila", "Compile") // mesmo novo: não é ambíguo
	m.AddIdent("internal/storage", "Aposenta", "Retire")
	m.AddIdent("internal/query", "Aposenta", "Dismiss") // ambíguo com storage
	m.Tests["TestFilaRecusaCheia"] = "TestQueueRefusesWhenFull"
	m.Tests["test_fila_cheia"] = "test_queue_full"
	m.LogKeys["consumer.subiu"] = "consumer.up"
	m.Paths["internal/contadores"] = "internal/counters"
	m.Paths["internal/kafka/fila.go"] = "internal/kafka/queue.go"
	m.Packages["contadores"] = "counters"
	return m
}

func TestCraseSubstituiIdentificadorNaoQualificadoSemAmbiguidade(t *testing.T) {
	out, sk := RewriteText([]byte("a `Fila` limitada e `Compila` o bloco"), ".md", mapa())
	if got := string(out); got != "a `Queue` limitada e `Compile` o bloco" {
		t.Fatalf("saída: %q", got)
	}
	if len(sk) != 0 {
		t.Fatalf("pulou: %v", sk)
	}
}

func TestCraseNaoSubstituiAmbiguoEReporta(t *testing.T) {
	out, sk := RewriteText([]byte("o `Aposenta` libera"), ".md", mapa())
	if string(out) != "o `Aposenta` libera" {
		t.Fatalf("substituiu ambíguo: %q", out)
	}
	if len(sk) != 1 || sk[0].Token != "Aposenta" || !strings.Contains(sk[0].Motivo, "ambíguo") {
		t.Fatalf("relatório: %v", sk)
	}
}

func TestCraseQualificadaResolvePeloPacote(t *testing.T) {
	out, _ := RewriteText([]byte("`storage.Aposenta` e `query.Aposenta` e `contadores.Compila` e `kafka.Fila.Empurra`"), ".md", mapa())
	want := "`storage.Retire` e `query.Dismiss` e `counters.Compile` e `kafka.Queue.Push`"
	if string(out) != want {
		t.Fatalf("saída: %q\nquer:  %q", out, want)
	}
}

func TestPalavraSoltaNaProsaNaoEhTocada(t *testing.T) {
	src := "a fila limitada Aposenta o grupo quando Compila termina"
	out, _ := RewriteText([]byte(src), ".md", mapa())
	if string(out) != src {
		t.Fatalf("tocou prosa: %q", out)
	}
}

func TestNomeDeTesteSubstituiEmQualquerLugar(t *testing.T) {
	out, _ := RewriteText([]byte("ver TestFilaRecusaCheia e `TestFilaRecusaCheia` e test_fila_cheia; TestFilaRecusaCheiaOutro fica"), ".md", mapa())
	want := "ver TestQueueRefusesWhenFull e `TestQueueRefusesWhenFull` e test_queue_full; TestFilaRecusaCheiaOutro fica"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestChaveDeLogSoEntreCrasesOuAspas(t *testing.T) {
	out, _ := RewriteText([]byte("`consumer.subiu` e \"consumer.subiu\" e consumer.subiu solto"), ".md", mapa())
	want := "`consumer.up` e \"consumer.up\" e consumer.subiu solto"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestCaminhoSubstituiEmQualquerLugarEOMaisLongoPrimeiro(t *testing.T) {
	out, _ := RewriteText([]byte("internal/kafka/fila.go e internal/contadores/compila.go e [x](../internal/contadores)"), ".md", mapa())
	want := "internal/kafka/queue.go e internal/counters/compila.go e [x](../internal/counters)"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestDocLinkEQualificadoEmComentarioGo(t *testing.T) {
	src := "package x\n\n// Roda usa [Fila] e kafka.Fila.Empurra e contadores.Compila; a fila solta fica.\n// Ver TestFilaRecusaCheia.\nfunc Roda() {}\n"
	out, _ := RewriteGoComments([]byte(src), mapa())
	want := "package x\n\n// Roda usa [Queue] e kafka.Queue.Push e counters.Compile; a fila solta fica.\n// Ver TestQueueRefusesWhenFull.\nfunc Roda() {}\n"
	if string(out) != want {
		t.Fatalf("saída:\n%s\nquer:\n%s", out, want)
	}
}

func TestComentarioGoNaoTocaCodigo(t *testing.T) {
	src := "package x\n\nvar Fila = 1 // a `Fila` de verdade\n"
	out, _ := RewriteGoComments([]byte(src), mapa())
	want := "package x\n\nvar Fila = 1 // a `Queue` de verdade\n"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestLiteralDeCaminhoEmGoEhReescrito(t *testing.T) {
	src := "package x\n\nvar p = \"internal/kafka/fila.go\"\nvar q = \"internal/contadores/x.go\"\n"
	out, _ := RewriteGoComments([]byte(src), mapa())
	want := "package x\n\nvar p = \"internal/kafka/queue.go\"\nvar q = \"internal/counters/x.go\"\n"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}

func TestSubtesteSoNoPrimeiroArgumentoDeRun(t *testing.T) {
	m := mapa()
	m.Subtests["x_test.go"] = map[string]string{"fila cheia": "queue full"}
	src := "package x\n\nfunc TestA(t *T) {\n\tt.Run(\"fila cheia\", nil)\n\tlog(\"fila cheia\")\n}\n"
	out, _ := RewriteGoFile("x_test.go", []byte(src), m)
	want := "package x\n\nfunc TestA(t *T) {\n\tt.Run(\"queue full\", nil)\n\tlog(\"fila cheia\")\n}\n"
	if string(out) != want {
		t.Fatalf("saída: %q", out)
	}
}
```

- [ ] **Passo 2: Ver vermelho**

Run: `go test ./scripts/renamecite/ -count=1`
Esperado: FAIL de compilação.

- [ ] **Passo 3: Escrever `rules.go`**

```go
package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// IdentEntry é um identificador do mapeamento: pacote (diretório), nome
// velho (`Fila`, `Fila.Empurra`) e nome novo.
type IdentEntry struct {
	Pkg, Old, New string
}

// Mapping é tudo que a passada de prosa sabe substituir.
type Mapping struct {
	Idents   []IdentEntry
	Tests    map[string]string
	LogKeys  map[string]string
	Paths    map[string]string
	Packages map[string]string            // nome curto velho → novo (contadores → counters)
	Subtests map[string]map[string]string // arquivo → velho → novo
}

// Skipped é uma ocorrência que casou com um nome do mapeamento mas ficou
// de fora por regra (ambiguidade, palavra solta). Vai para o relatório.
type Skipped struct {
	Line   int
	Token  string
	Motivo string
}

func NewMapping() *Mapping {
	return &Mapping{Tests: map[string]string{}, LogKeys: map[string]string{}, Paths: map[string]string{}, Packages: map[string]string{}, Subtests: map[string]map[string]string{}}
}

func (m *Mapping) AddIdent(pkg, old, novo string) {
	m.Idents = append(m.Idents, IdentEntry{pkg, old, novo})
}

// resolve decide o nome novo de um token entre crases. `qual` é o
// qualificador (pacote curto ou tipo), vazio quando o token vem nu.
//
// Nu: substitui só se todo pacote que declara `old` concorda no `new`.
// Qualificado por pacote (velho ou novo nome curto): só as entradas daquele
// pacote. Qualificado por tipo (`Fila.Empurra`): o próprio Old da entrada.
func (m *Mapping) resolve(qual, old string) (string, string, bool) {
	var candidatos []IdentEntry
	for _, e := range m.Idents {
		if e.Old != old {
			continue
		}
		if qual == "" || shortPkg(e.Pkg) == qual || m.Packages[shortPkg(e.Pkg)] == qual {
			candidatos = append(candidatos, e)
		}
	}
	if len(candidatos) == 0 {
		return "", "", false
	}
	novo := candidatos[0].New
	for _, c := range candidatos[1:] {
		if c.New != novo {
			return "", fmt.Sprintf("ambíguo: %s vira %q em %s e %q em %s", old, novo, candidatos[0].Pkg, c.New, c.Pkg), false
		}
	}
	return novo, "", true
}

func shortPkg(dir string) string {
	if i := strings.LastIndex(dir, "/"); i >= 0 {
		return dir[i+1:]
	}
	return dir
}

// rewriteToken reescreve o CONTEÚDO de uma crase: `X`, `pkg.X`, `T.M`,
// `pkg.T.M`, `(*T).M`. Devolve o texto novo e se mudou.
func (m *Mapping) rewriteToken(tok string) (string, *Skipped) {
	t := strings.TrimPrefix(tok, "(*")
	t = strings.Replace(t, ").", ".", 1)
	parts := strings.Split(t, ".")
	switch len(parts) {
	case 1:
		if novo, motivo, ok := m.resolve("", parts[0]); ok {
			return novo, nil
		} else if motivo != "" {
			return tok, &Skipped{Token: tok, Motivo: motivo}
		}
	case 2:
		// pkg.X ou T.M
		if novo, motivo, ok := m.resolve(parts[0], parts[1]); ok {
			return m.pkgOrType(parts[0]) + "." + novo, nil
		} else if motivo != "" {
			return tok, &Skipped{Token: tok, Motivo: motivo}
		}
		if novo, motivo, ok := m.resolve("", parts[0]+"."+parts[1]); ok {
			return novo, nil
		} else if motivo != "" {
			return tok, &Skipped{Token: tok, Motivo: motivo}
		}
		// só o tipo muda (`Fila.Len` com Len já em inglês)
		if novo, _, ok := m.resolve("", parts[0]); ok {
			return novo + "." + parts[1], nil
		}
	case 3:
		// pkg.T.M
		if novo, motivo, ok := m.resolve(parts[0], parts[1]+"."+parts[2]); ok {
			return m.pkgOrType(parts[0]) + "." + novo, nil
		} else if motivo != "" {
			return tok, &Skipped{Token: tok, Motivo: motivo}
		}
		if novo, _, ok := m.resolve(parts[0], parts[1]); ok {
			return m.pkgOrType(parts[0]) + "." + novo + "." + parts[2], nil
		}
	}
	return tok, nil
}

func (m *Mapping) pkgOrType(q string) string {
	if novo, ok := m.Packages[q]; ok {
		return novo
	}
	if novo, _, ok := m.resolve("", q); ok {
		return novo
	}
	return q
}

var (
	reCrase   = regexp.MustCompile("`([^`\n]+)`")
	reAspas   = regexp.MustCompile(`"([a-z][a-z0-9_.]*\.[a-z][a-z0-9_]*)"`)
	reTestGo  = regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]*`)
	reTestPy  = regexp.MustCompile(`\btest_[a-z0-9_]+`)
	reDocLink = regexp.MustCompile(`\[([A-Za-z_][A-Za-z0-9_.*()]*)\]`)
	reQualif  = regexp.MustCompile(`\b([a-z][a-z0-9]*)\.([A-Z][A-Za-z0-9_]*(?:\.[A-Z][A-Za-z0-9_]*)?)\b`)
)

// applyCommon são as regras válidas em qualquer texto: crases, nomes de
// teste, chaves de log entre crases ou aspas, caminhos.
func (m *Mapping) applyCommon(s string, sk *[]Skipped, line func(int) int) string {
	s = reCrase.ReplaceAllStringFunc(s, func(x string) string {
		inner := x[1 : len(x)-1]
		if novo, ok := m.LogKeys[inner]; ok {
			return "`" + novo + "`"
		}
		if novo, ok := m.Tests[inner]; ok {
			return "`" + novo + "`"
		}
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			skipped.Line = line(strings.Index(s, x))
			*sk = append(*sk, *skipped)
		}
		return "`" + out + "`"
	})
	s = reAspas.ReplaceAllStringFunc(s, func(x string) string {
		inner := x[1 : len(x)-1]
		if novo, ok := m.LogKeys[inner]; ok {
			return `"` + novo + `"`
		}
		return x
	})
	s = reTestGo.ReplaceAllStringFunc(s, func(x string) string {
		if novo, ok := m.Tests[x]; ok {
			return novo
		}
		return x
	})
	s = reTestPy.ReplaceAllStringFunc(s, func(x string) string {
		if novo, ok := m.Tests[x]; ok {
			return novo
		}
		return x
	})
	return m.applyPaths(s)
}

// applyPaths troca caminhos, o mais longo primeiro, para
// internal/kafka/fila.go não virar internal/kafka/fila.go pela metade.
func (m *Mapping) applyPaths(s string) string {
	keys := make([]string, 0, len(m.Paths))
	for k := range m.Paths {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, k := range keys {
		s = replacePath(s, k, m.Paths[k])
	}
	return s
}

// replacePath substitui o caminho só quando ele termina em fronteira
// (fim, `/`, `)`, crase, aspas, espaço, `:` de linha), para
// internal/contadores não casar dentro de internal/contadores_x.
func replacePath(s, old, novo string) string {
	var b strings.Builder
	for {
		i := strings.Index(s, old)
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		end := i + len(old)
		ok := end == len(s) || strings.ContainsRune("/)`\" :,;\n\t]>", rune(s[end]))
		b.WriteString(s[:i])
		if ok {
			b.WriteString(novo)
		} else {
			b.WriteString(old)
		}
		s = s[end:]
	}
}

// applyGoCommentExtras são as regras que só valem em comentário Go: doc
// link `[Ident]` e o qualificado `pkg.Ident` fora de crase.
func (m *Mapping) applyGoCommentExtras(s string, sk *[]Skipped) string {
	s = reDocLink.ReplaceAllStringFunc(s, func(x string) string {
		inner := x[1 : len(x)-1]
		out, skipped := m.rewriteToken(inner)
		if skipped != nil {
			*sk = append(*sk, *skipped)
		}
		return "[" + out + "]"
	})
	return reQualif.ReplaceAllStringFunc(s, func(x string) string {
		out, skipped := m.rewriteToken(x)
		if skipped != nil {
			*sk = append(*sk, *skipped)
		}
		return out
	})
}
```

- [ ] **Passo 4: Escrever `textfile.go` e `gofile.go`**

`textfile.go`:

```go
package main

import "strings"

// RewriteText aplica as regras comuns a um arquivo que não é Go: .md, .puml,
// .sh, .py, .yml, Makefile. Nada além de crase, aspas (chave de log), nome
// de teste e caminho é tocado — prosa solta é do glossário.
func RewriteText(src []byte, ext string, m *Mapping) ([]byte, []Skipped) {
	var sk []Skipped
	s := string(src)
	line := func(off int) int { return 1 + strings.Count(s[:max(0, min(off, len(s)))], "\n") }
	out := m.applyCommon(s, &sk, line)
	return []byte(out), sk
}
```

`gofile.go`:

```go
package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
)

// RewriteGoComments reescreve só os COMENTÁRIOS e os literais de caminho de
// um arquivo Go. Código é do rf; um identificador em código que ainda esteja
// em PT-BR depois do rf é erro de mapeamento, não trabalho desta passada.
func RewriteGoComments(src []byte, m *Mapping) ([]byte, []Skipped) {
	return RewriteGoFile("", src, m)
}

// RewriteGoFile é RewriteGoComments mais os subtestes do arquivo dado.
func RewriteGoFile(name string, src []byte, m *Mapping) ([]byte, []Skipped) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return src, []Skipped{{Motivo: "não parseia: " + err.Error()}}
	}
	type span struct {
		start, end int
		text       string
	}
	var spans []span
	var sk []Skipped
	for _, g := range f.Comments {
		for _, c := range g.List {
			start := fset.Position(c.Pos()).Offset
			end := fset.Position(c.End()).Offset
			body := c.Text
			line := func(int) int { return fset.Position(c.Pos()).Line }
			out := m.applyCommon(body, &sk, line)
			out = m.applyGoCommentExtras(out, &sk)
			if out != body {
				spans = append(spans, span{start, end, out})
			}
		}
	}
	subs := m.Subtests[name]
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return true
			}
			val, err := strconv.Unquote(x.Value)
			if err != nil {
				return true
			}
			if novo := m.applyPaths(val); novo != val {
				spans = append(spans, span{fset.Position(x.Pos()).Offset, fset.Position(x.End()).Offset, strconv.Quote(novo)})
			}
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Run" || len(x.Args) == 0 || subs == nil {
				return true
			}
			lit, ok := x.Args[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			val, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			if novo, ok := subs[val]; ok {
				spans = append(spans, span{fset.Position(lit.Pos()).Offset, fset.Position(lit.End()).Offset, strconv.Quote(novo)})
			}
		}
		return true
	})
	if len(spans) == 0 {
		return src, sk
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start > spans[j].start })
	out := append([]byte(nil), src...)
	for _, s := range spans {
		out = append(out[:s.start], append([]byte(s.text), out[s.end:]...)...)
	}
	return out, sk
}
```

- [ ] **Passo 5: Escrever `main.go`**

```go
// Command renamecite é a passada de prosa do ciclo de identificadores em
// inglês (spec §4.4): reescreve citações — token entre crases, doc link,
// `pkg.Ident` em comentário Go, `TestXxx`/`test_xxx`, caminho de arquivo ou
// pacote, chave de log entre crases ou aspas, nome de subteste em t.Run —
// a partir dos TSV de mapeamento, e relata o que casou mas ficou de fora
// (ambiguidade). Palavra solta na prosa nunca é tocada.
//
//	go run ./scripts/renamecite -dir . \
//	    -idents planning/identificadores-em-ingles/mapeamento-2-G1-types.tsv \
//	    -paths planning/identificadores-em-ingles/mapeamento-pacotes.tsv \
//	    -report /tmp/renamecite.txt
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

var extTexto = map[string]bool{".md": true, ".puml": true, ".sh": true, ".py": true, ".yml": true, ".yaml": true, ".java": true}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("renamecite", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "raiz do repositório")
	var idents, paths, tests, logkeys, subtests multi
	fs.Var(&idents, "idents", "TSV pacote/especie/endereco/novo/motivo (repetível)")
	fs.Var(&paths, "paths", "TSV velho/novo de caminhos (repetível)")
	fs.Var(&tests, "tests", "TSV pacote/velho/novo de testes (repetível)")
	fs.Var(&logkeys, "logkeys", "TSV velho/novo de chaves de log (repetível)")
	fs.Var(&subtests, "subtests", "TSV arquivo/velho/novo de subtestes (repetível)")
	report := fs.String("report", "", "arquivo do relatório do que ficou de fora (obrigatório)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *report == "" {
		fmt.Fprintln(stderr, "renamecite: -report é obrigatório")
		return 2
	}
	m, err := loadMapping(idents, paths, tests, logkeys, subtests)
	if err != nil {
		fmt.Fprintf(stderr, "renamecite: %v\n", err)
		return 2
	}
	rep, err := os.Create(*report)
	if err != nil {
		fmt.Fprintf(stderr, "renamecite: %v\n", err)
		return 1
	}
	defer rep.Close()
	tocados := 0
	err = filepath.WalkDir(*dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", ".claude", ".superpowers", "target", ".venv", "__pycache__", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		ext := filepath.Ext(p)
		base := d.Name()
		if ext != ".go" && !extTexto[ext] && base != "Makefile" && base != "CLAUDE.md" {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(*dir, p)
		var out []byte
		var sk []Skipped
		if ext == ".go" {
			out, sk = RewriteGoFile(filepath.ToSlash(rel), src, m)
		} else {
			out, sk = RewriteText(src, ext, m)
		}
		for _, s := range sk {
			fmt.Fprintf(rep, "%s:%d\t%s\t%s\n", rel, s.Line, s.Token, s.Motivo)
		}
		if string(out) != string(src) {
			tocados++
			return os.WriteFile(p, out, 0o644)
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(stderr, "renamecite: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%d arquivos reescritos; relatório em %s\n", tocados, *report)
	return 0
}

func loadMapping(idents, paths, tests, logkeys, subtests []string) (*Mapping, error) {
	m := NewMapping()
	for _, f := range idents {
		if err := eachRow(f, 5, func(c []string) {
			switch strings.SplitN(c[1], "#", 2)[0] {
			case "param", "local", "label", "receiver":
				return // prosa não cita local por endereço; e "mantido" tem novo == velho
			}
			if c[3] == "" || strings.HasSuffix(c[2], "."+c[3]) || c[2] == c[3] {
				return
			}
			m.AddIdent(c[0], c[2], c[3])
		}); err != nil {
			return nil, err
		}
	}
	for _, f := range paths {
		if err := eachRow(f, 2, func(c []string) {
			m.Paths[c[0]] = c[1]
			if !strings.Contains(c[0], ".go") {
				m.Packages[shortPkg(c[0])] = shortPkg(c[1])
			}
		}); err != nil {
			return nil, err
		}
	}
	for _, f := range tests {
		if err := eachRow(f, 3, func(c []string) { m.Tests[c[1]] = c[2] }); err != nil {
			return nil, err
		}
	}
	for _, f := range logkeys {
		if err := eachRow(f, 2, func(c []string) { m.LogKeys[c[0]] = c[1] }); err != nil {
			return nil, err
		}
	}
	for _, f := range subtests {
		if err := eachRow(f, 3, func(c []string) {
			if m.Subtests[c[0]] == nil {
				m.Subtests[c[0]] = map[string]string{}
			}
			m.Subtests[c[0]][c[1]] = c[2]
		}); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func eachRow(path string, cols int, fn func([]string)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	first := true
	for sc.Scan() {
		l := sc.Text()
		if first {
			first = false
			continue // cabeçalho
		}
		if strings.TrimSpace(l) == "" {
			continue
		}
		c := strings.Split(l, "\t")
		if len(c) < cols {
			return fmt.Errorf("%s: linha com %d colunas, esperado %d: %q", path, len(c), cols, l)
		}
		fn(c)
	}
	return sc.Err()
}
```

No `-idents`, o `endereco` `Fila.Empurra` entra como `Old` inteiro
(`T.M`), e `Compila` como nome nu; `loadMapping` descarta param/local/label/
receiver porque prosa não os cita por endereço.

- [ ] **Passo 6: Ver verde**

Run: `go test ./scripts/renamecite/ -count=1 -v`
Esperado: PASS nos onze testes.

- [ ] **Passo 7: Mutações**

1. Em `rules.go`, `resolve`: devolver `novo` mesmo com candidatos divergentes. `TestCraseNaoSubstituiAmbiguoEReporta` vermelho. Reverter, verde.
2. Em `applyCommon`, trocar `reCrase` por uma regex sem as crases (palavra nua). `TestPalavraSoltaNaProsaNaoEhTocada` vermelho. Reverter, verde.
3. Em `gofile.go`, aplicar `applyCommon` ao arquivo inteiro em vez de comentário a comentário. `TestComentarioGoNaoTocaCodigo` vermelho. Reverter, verde.

- [ ] **Passo 8: Commit**

```bash
git add scripts/renamecite
git commit -m "scripts: renamecite, a passada de prosa que só reescreve citações"
```

### Tarefa 0.6: Aplicador `scripts/rename-apply.sh` e o gate da ferramenta ausente

**Arquivos:**
- Criar: `scripts/rename-apply.sh`
- Criar: `internal/gates/rename_tools_test.go`

**Interfaces:**
- Produz: `scripts/rename-apply.sh <dir-do-pacote> <script.rf> <diff-de-saida>`; exit 3 sem `rf`, 4 sem `gopls`, 1 se o `rf` recusar, 2 se build ou vet falharem; escreve o `-diff` do `rf` no arquivo dado ANTES de aplicar.

- [ ] **Passo 1: Gate**

`internal/gates/rename_tools_test.go`:

```go
// Package gates — este arquivo confere o aplicador de renames.
package gates

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// "Gate que passa com a ferramenta ausente não é gate, é carimbo" (débito
// 53). O aplicador do ciclo de identificadores em inglês depende de rf e
// gopls; sem eles tem de falhar ALTO no stderr nomeando o go install exato,
// nunca cair para um caminho que "faça o que der".
func TestOAplicadorDeRenameRecusaSemRfNomeandoOGoInstall(t *testing.T) {
	script := filepath.Join(raizDoRepo, "scripts", "rename-apply.sh")
	vazio := t.TempDir()
	cmd := exec.Command("bash", script, "internal/types", "/dev/null", filepath.Join(vazio, "d.diff"))
	cmd.Dir = raizDoRepo
	cmd.Env = append(os.Environ(), "PATH="+vazio)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("passou sem rf no PATH:\n%s", out)
	}
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 3 {
		t.Fatalf("exit %v, esperado 3:\n%s", err, out)
	}
	if !strings.Contains(string(out), "go install rsc.io/rf@v0.0.0-20260710193434-85dbf8d53aee") {
		t.Fatalf("o stderr não nomeia o go install do rf:\n%s", out)
	}
}
```

- [ ] **Passo 2: Ver vermelho**

Run: `go test ./internal/gates/ -run TestOAplicadorDeRenameRecusaSemRfNomeandoOGoInstall -count=1`
Esperado: FAIL — o script não existe (bash devolve 127, não 3).

- [ ] **Passo 3: Escrever o aplicador**

`scripts/rename-apply.sh`:

```bash
#!/usr/bin/env bash
# Aplicador do ciclo de identificadores em inglês (planning/identificadores-em-ingles.md §4.6).
#
#   scripts/rename-apply.sh <dir-do-pacote> <script.rf> <diff-de-saida>
#
# Exige rf e gopls no PATH e falha alto sem eles (doutrina dos gates: ferramenta
# ausente nomeia o que instalar). Guarda o -diff do rf ANTES de aplicar, aplica,
# e roda build e vet do módulo inteiro. O rf é tudo-ou-nada por script: se ele
# recusar, nada foi escrito.
set -u
RF_VERSION="rsc.io/rf@v0.0.0-20260710193434-85dbf8d53aee"
GOPLS_VERSION="golang.org/x/tools/gopls@v0.23.0"

if [ $# -ne 3 ]; then
  echo "uso: $0 <dir-do-pacote> <script.rf> <diff-de-saida>" >&2
  exit 2
fi
dir=$1; script=$2; diff_out=$3

if ! command -v rf >/dev/null 2>&1; then
  echo "rename-apply: rf não está no PATH. Instale com: go install $RF_VERSION" >&2
  exit 3
fi
if ! command -v gopls >/dev/null 2>&1; then
  echo "rename-apply: gopls não está no PATH. Instale com: go install $GOPLS_VERSION" >&2
  exit 4
fi

export GOFLAGS="${GOFLAGS:-} -buildvcs=false"
raiz=$(pwd)
cd "$dir" || exit 2
conteudo=$(cat "$script")

rf -diff "$conteudo" > "$diff_out" 2>"$diff_out.err"
if [ $? -ne 0 ]; then
  echo "rename-apply: o rf recusou o script em $dir:" >&2
  cat "$diff_out.err" >&2
  exit 1
fi
rf "$conteudo" 2>"$diff_out.err"
if [ $? -ne 0 ]; then
  echo "rename-apply: o rf falhou ao aplicar em $dir (nada foi escrito):" >&2
  cat "$diff_out.err" >&2
  exit 1
fi
cd "$raiz" || exit 2
if ! go build ./... ; then
  echo "rename-apply: build vermelho depois de $dir; reverta com git checkout -- . e devolva o mapeamento ao juiz" >&2
  exit 2
fi
if ! go vet ./... ; then
  echo "rename-apply: vet vermelho depois de $dir; reverta com git checkout -- . e devolva o mapeamento ao juiz" >&2
  exit 2
fi
echo "rename-apply: $dir aplicado; diff em $diff_out"
```

```bash
chmod +x scripts/rename-apply.sh
```

- [ ] **Passo 4: Ver verde e mutação**

Run: `go test ./internal/gates/ -run TestOAplicadorDeRenameRecusaSemRfNomeandoOGoInstall -count=1`
Esperado: PASS.

Mutação: trocar `exit 3` por `exit 0` no bloco do rf. Vermelho ("passou sem
rf no PATH"). Reverter, verde.

- [ ] **Passo 5: Prova real com script vazio**

Criar `/tmp/claude-1000/.../scratchpad/vazio.rf` com uma linha de comentário
`# nada` e rodar:

Run: `scripts/rename-apply.sh internal/types /caminho/vazio.rf /caminho/vazio.diff`
Esperado: "internal/types aplicado" e exit 0 (a primeira carga do `rf` leva
~1m40s; as seguintes, ~2 s).

- [ ] **Passo 6: Commit**

```bash
git add scripts/rename-apply.sh internal/gates/rename_tools_test.go
git commit -m "scripts: aplicador de rename que exige rf e gopls e guarda o diff antes de aplicar"
```

### Tarefa 0.7: Fechamento da Fase 0

- [ ] **Passo 1: Suíte completa, race e lint**

Run: `make test`
Esperado: PASS.
Run: `make test-race`
Esperado: PASS.
Run: `make lint`
Esperado: a contagem impressa. Se diferente de `.golangci-baseline`
(368), escrever o número novo no arquivo (as ferramentas novas podem
acrescentar `errcheck` em `defer f.Close()`; o certo é corrigir o
apontamento — `defer func() { _ = f.Close() }()` — e não subir a baseline).

- [ ] **Passo 2: Commit da baseline se mudou**

```bash
git add .golangci-baseline
git commit -m "lint: linha de base após as ferramentas de rename"
```

- [ ] **Passo 3: Merge da Fase 0 em `main`**

Abrir PR `refactor/identificadores-em-ingles` → `main` com o título
"Identificadores em inglês — Fase 0: glossário, norma e ferramentas". Após
o merge, as três branches abertas (`chore/doutrina-de-verificacao`,
`refactor/passada-unica-extract`, `worktree-wf_2de0ca0b-60a-1`) rebaseiam
sobre `main`. Nenhuma branch de código nova nasce até a Fase 6.

---

## Fase 1 — Pacotes e arquivos

### Tarefa 1.1: Os seis pacotes

**Arquivos:**
- Criar: `planning/identificadores-em-ingles/mapeamento-pacotes.tsv`
- Mover: `internal/contadores` → `internal/counters`, `internal/predicado` → `internal/predicate`, `internal/protocolo` → `internal/protocol`, `internal/query/orcamento` → `internal/query/budget`, `internal/storage/versoes` → `internal/storage/versions`, `internal/storage/contador` → `internal/storage/counter`
- Modificar: todo importador (o `gopls` faz), `internal/gates/citacoes_test.go:25`, `internal/gates/jdbc_test.go:173,223,302,305`, `CLAUDE.md:84,89,100`, `docs/architecture.md:697,962,1075,2237`, `docs/debitos.md:275`, `clients/python/src/nishidb/{leitor,erros,documentos,tipos}.py` (docstrings), `internal/config/config.go:130,562`, `internal/metakafka/registro.go:6,62`, `internal/schema/compile.go:343`, `internal/query/executor/{collect,predicate}.go:5`, todos os `planning/*.md` que citam os caminhos (a passada de prosa faz)

**Interfaces:**
- Produz: os import paths novos, que todo mapeamento das fases seguintes usa na coluna `pacote`.

- [ ] **Passo 1: TSV de pacotes**

`planning/identificadores-em-ingles/mapeamento-pacotes.tsv`:

```
velho	novo
internal/contadores	internal/counters
internal/predicado	internal/predicate
internal/protocolo	internal/protocol
internal/query/orcamento	internal/query/budget
internal/storage/versoes	internal/storage/versions
internal/storage/contador	internal/storage/counter
```

- [ ] **Passo 2: Script de rename de pacote (no scratchpad, não versionado)**

`$SCRATCH/rename-package.sh`:

```bash
#!/usr/bin/env bash
# rename-package.sh <dir-velho> <dir-novo>   (relativos à raiz do worktree)
# gopls reescreve a cláusula package, os imports e os qualificadores; o
# diretório é movido com git mv em seguida (o gopls em CLI não move).
set -eu
velho=$1; novo=$2
nome_novo=$(basename "$novo")
export GOFLAGS="${GOFLAGS:-} -buildvcs=false"
arquivo=$(ls "$velho"/*.go | grep -v _test.go | head -1)
linha=$(grep -n '^package ' "$arquivo" | head -1 | cut -d: -f1)
gopls rename -w "$arquivo:$linha:9" "$nome_novo"
git mv "$velho" "$novo"
gofmt -l "$novo" internal cmd benchmark scripts | head
go build ./...
go vet ./...
echo "ok: $velho -> $novo"
```

Se o `gopls` recusar por conflito ("would conflict with local variable
`budget`" e afins), renomear a variável local antes com um `rf` de uma
linha no pacote importador (`mv Func.budget Func.memBudget`), commitá-la
como "prepara o rename do pacote", e repetir.

- [ ] **Passo 3: Um pacote por vez, um commit por pacote**

Para cada linha do TSV, na ordem do arquivo:

```bash
bash $SCRATCH/rename-package.sh internal/contadores internal/counters
```

Run: `go test ./internal/counters/ ./internal/cli/ -count=1`
Esperado: PASS.

```bash
git add -A internal cmd benchmark scripts
git commit -m "counters: o pacote internal/contadores passa a se chamar counters"
```

Repetir para os outros cinco (mensagens: "predicate: o pacote
internal/predicado passa a se chamar predicate", e assim por diante).
`storage/contador` → `storage/counter` e `contadores` → `counters` são
pacotes distintos com nomes distintos: nenhum conflito de import.

- [ ] **Passo 4: Passada de prosa dos caminhos**

```bash
go run ./scripts/renamecite -dir . -paths planning/identificadores-em-ingles/mapeamento-pacotes.tsv -report $SCRATCH/renamecite-pacotes.txt
```

Esperado: reescreve `CLAUDE.md`, `docs/architecture.md`, `docs/debitos.md`,
os `planning/*.md`, os `.py` do client, `internal/gates/jdbc_test.go`
(literais `internal/protocolo/...`), `internal/gates/citacoes_test.go`
(`internal/protocolo/documentos.go`), e os comentários Go. Conferir com
`git diff --stat` que NENHUM arquivo fora dessa lista mudou e que o
relatório está vazio.

Run: `go test ./internal/gates/ ./internal/cli/ -count=1`
Esperado: PASS (o gate de citações confere que os caminhos listados
existem).

```bash
git add -A
git commit -m "docs, gates: caminhos dos seis pacotes renomeados"
```

### Tarefa 1.2: Arquivos `.go` em PT-BR

**Arquivos:**
- Criar: `planning/identificadores-em-ingles/mapeamento-arquivos.tsv`
- Mover: os 267 arquivos listados abaixo
- Modificar: `internal/gates/citacoes_test.go` (lista `arquivosDoIndicePorSegmento`), `internal/gates/jdbc_test.go` (`internal/protocol/jdbc.go` não muda de nome), `CLAUDE.md` (`internal/metrics/nomes.go`, `store.go`), `docs/*.md` e `planning/*.md` que citam arquivos (passada de prosa)

- [ ] **Passo 1: TSV de arquivos**

`planning/identificadores-em-ingles/mapeamento-arquivos.tsv` (cabeçalho
`velho	novo`; caminhos já com os pacotes da Tarefa 1.1):

A lista abaixo é o que a Fase 1 aplicou, e uma linha dela foi SUPERADA: o juiz
do G2 fechou `prazo → TTL` (Ruling R30), e por isso `internal/storage/
deadlines_test.go` e `internal/storage/rowindex/deadlines_test.go` foram
renomeados de novo, para `ttls_test.go`. O TSV ganhou as duas linhas do segundo
salto (`deadlines_test.go → ttls_test.go`) em vez de reescrever as do primeiro:
é o segundo salto que a passada de caminhos precisa enxergar para alcançar as
citações que já estavam em `deadlines_test.go`.

```
benchmark/ingest-bench/gate_pulado_test.go	benchmark/ingest-bench/gate_skipped_test.go
benchmark/ingest-bench/serie_do_aceite_test.go	benchmark/ingest-bench/acceptance_series_test.go
benchmark/micro/contadores_bench_test.go	benchmark/micro/counters_bench_test.go
internal/cli/congelada_e2e_test.go	internal/cli/frozen_e2e_test.go
internal/cli/contadores_query_test.go	internal/cli/counters_query_test.go
internal/cli/corrupcao_test.go	internal/cli/corruption_test.go
internal/cli/credencial.go	internal/cli/credential.go
internal/cli/credencial_internal_test.go	internal/cli/credential_internal_test.go
internal/cli/credencial_test.go	internal/cli/credential_test.go
internal/cli/defaults_globais_test.go	internal/cli/global_defaults_test.go
internal/cli/desfecho_internal_test.go	internal/cli/outcome_internal_test.go
internal/cli/determinismo_test.go	internal/cli/determinism_test.go
internal/cli/entrada_interrompida_test.go	internal/cli/interrupted_input_test.go
internal/cli/evidencias_test.go	internal/cli/evidence_test.go
internal/cli/mutacao.go	internal/cli/mutation.go
internal/cli/mutacao_test.go	internal/cli/mutation_test.go
internal/cli/operacao.go	internal/cli/operation.go
internal/cli/operacao_internal_test.go	internal/cli/operation_internal_test.go
internal/cli/operacao_test.go	internal/cli/operation_test.go
internal/cli/parametros_e2e_test.go	internal/cli/params_e2e_test.go
internal/cli/parametros.go	internal/cli/params.go
internal/cli/parametros_internal_test.go	internal/cli/params_internal_test.go
internal/cli/perda_test.go	internal/cli/loss_test.go
internal/cli/query_remota.go	internal/cli/query_remote.go
internal/cli/query_remota_internal_test.go	internal/cli/query_remote_internal_test.go
internal/cli/query_remota_test.go	internal/cli/query_remote_test.go
internal/cli/resumo_internal_test.go	internal/cli/summary_internal_test.go
internal/cli/statusemvoo_test.go	internal/cli/status_inflight_test.go
internal/cli/ttl_condicional_e2e_test.go	internal/cli/ttl_conditional_e2e_test.go
internal/cli/vazao_race_test.go	internal/cli/throughput_race_test.go
internal/cli/vazao_test.go	internal/cli/throughput_test.go
internal/config/config_seguranca.go	internal/config/config_security.go
internal/config/querylonga_internal_test.go	internal/config/longquery_internal_test.go
internal/counters/abertura.go	internal/counters/open.go
internal/counters/compila.go	internal/counters/compile.go
internal/counters/compila_test.go	internal/counters/compile_test.go
internal/counters/tabela_gate_race_test.go	internal/counters/table_gate_race_test.go
internal/counters/tabela_gate_test.go	internal/counters/table_gate_test.go
internal/counters/tabela.go	internal/counters/table.go
internal/counters/tabela_test.go	internal/counters/table_test.go
internal/gates/citacoes_test.go	internal/gates/citations_test.go
internal/gates/metricas_test.go	internal/gates/metrics_test.go
internal/gates/verificacao_test.go	internal/gates/verification_test.go
internal/index/segindex/custo_gate_test.go	internal/index/segindex/cost_gate_test.go
internal/index/segindex/custo_tempo_gate_test.go	internal/index/segindex/cost_time_gate_test.go
internal/index/segindex/escritor.go	internal/index/segindex/writer.go
internal/index/segindex/escritor_test.go	internal/index/segindex/writer_test.go
internal/index/segindex/formato.go	internal/index/segindex/format.go
internal/index/segindex/selado.go	internal/index/segindex/sealed.go
internal/index/segindex/selado_test.go	internal/index/segindex/sealed_test.go
internal/index/segindex/sela.go	internal/index/segindex/seal.go
internal/index/segindex/sela_test.go	internal/index/segindex/seal_test.go
internal/index/segindex/uniao.go	internal/index/segindex/union.go
internal/index/segindex/uniao_test.go	internal/index/segindex/union_test.go
internal/index/valuekey/composta.go	internal/index/valuekey/composite.go
internal/index/valuekey/composta_test.go	internal/index/valuekey/composite_test.go
internal/index/valuekey/percorre_test.go	internal/index/valuekey/walk_test.go
internal/kafka/cadencia_test.go	internal/kafka/cadence_test.go
internal/kafka/consumidor.go	internal/kafka/consumer.go
internal/kafka/consumidor_test.go	internal/kafka/consumer_test.go
internal/kafka/corrida_checkpoint_test.go	internal/kafka/checkpoint_race_test.go
internal/kafka/decodificadores.go	internal/kafka/decoders.go
internal/kafka/esteira_gate_race_test.go	internal/kafka/pipeline_gate_race_test.go
internal/kafka/esteira_gate_test.go	internal/kafka/pipeline_gate_test.go
internal/kafka/esteira.go	internal/kafka/pipeline.go
internal/kafka/esteira_janela_test.go	internal/kafka/pipeline_window_test.go
internal/kafka/esteira_memoria_test.go	internal/kafka/pipeline_memory_test.go
internal/kafka/esteira_test.go	internal/kafka/pipeline_test.go
internal/kafka/fila.go	internal/kafka/queue.go
internal/kafka/fila_metrica_test.go	internal/kafka/queue_metric_test.go
internal/kafka/fila_test.go	internal/kafka/queue_test.go
internal/kafka/instantaneo.go	internal/kafka/snapshot.go
internal/kafka/instantaneo_test.go	internal/kafka/snapshot_test.go
internal/kafka/integracao_test.go	internal/kafka/integration_test.go
internal/kafka/metricas.go	internal/kafka/metrics.go
internal/kafka/metricas_test.go	internal/kafka/metrics_test.go
internal/kafka/politicas_test.go	internal/kafka/policies_test.go
internal/kafka/posicao_gate_race_test.go	internal/kafka/position_gate_race_test.go
internal/kafka/posicao_gate_test.go	internal/kafka/position_gate_test.go
internal/kafka/tempos_test.go	internal/kafka/timings_test.go
internal/kafka/vigia.go	internal/kafka/watchdog.go
internal/kafka/vigia_test.go	internal/kafka/watchdog_test.go
internal/materializer/acumula_test.go	internal/materializer/accumulate_test.go
internal/materializer/coercao_test.go	internal/materializer/coercion_test.go
internal/materializer/congelada.go	internal/materializer/frozen.go
internal/materializer/congelada_test.go	internal/materializer/frozen_test.go
internal/materializer/contagem.go	internal/materializer/count.go
internal/materializer/datahora_test.go	internal/materializer/datetime_test.go
internal/materializer/decodifica.go	internal/materializer/decode.go
internal/materializer/decodifica_test.go	internal/materializer/decode_test.go
internal/materializer/descoberta_test.go	internal/materializer/discovery_test.go
internal/materializer/escrita_test.go	internal/materializer/write_test.go
internal/materializer/historico.go	internal/materializer/history.go
internal/materializer/historico_test.go	internal/materializer/history_test.go
internal/materializer/prazo.go	internal/materializer/deadline.go
internal/materializer/prazo_test.go	internal/materializer/deadline_test.go
internal/materializer/somenteleitura_test.go	internal/materializer/readonly_test.go
internal/metakafka/registro.go	internal/metakafka/registry.go
internal/metakafka/tabela.go	internal/metakafka/table.go
internal/metakafka/tabela_test.go	internal/metakafka/table_test.go
internal/metrics/nomes.go	internal/metrics/names.go
internal/metrics/processo.go	internal/metrics/process.go
internal/metrics/processo_test.go	internal/metrics/process_test.go
internal/predicate/avaliador_gate_race_test.go	internal/predicate/evaluator_gate_race_test.go
internal/predicate/avaliador_gate_test.go	internal/predicate/evaluator_gate_test.go
internal/predicate/avaliador.go	internal/predicate/evaluator.go
internal/predicate/avaliador_test.go	internal/predicate/evaluator_test.go
internal/predicate/predicado.go	internal/predicate/predicate.go
internal/predicate/predicado_test.go	internal/predicate/predicate_test.go
internal/predicate/programa.go	internal/predicate/program.go
internal/predicate/regra.go	internal/predicate/rule.go
internal/predicate/regra_test.go	internal/predicate/rule_test.go
internal/protocol/contadores.go	internal/protocol/counters.go
internal/protocol/documentos.go	internal/protocol/documents.go
internal/protocol/mutacoes.go	internal/protocol/mutations.go
internal/protocol/parametros.go	internal/protocol/params.go
internal/protocol/parametros_test.go	internal/protocol/params_test.go
internal/protocol/protocolo.go	internal/protocol/protocol.go
internal/query/auto_integracao_test.go	internal/query/auto_integration_test.go
internal/query/colunas_test.go	internal/query/columns_test.go
internal/query/concorrencia_test.go	internal/query/concurrency_test.go
internal/query/fuso_test.go	internal/query/timezone_test.go
internal/query/integracao_test.go	internal/query/integration_test.go
internal/query/mecanica_test.go	internal/query/mechanics_test.go
internal/query/meta_counters_roteamento_test.go	internal/query/meta_counters_routing_test.go
internal/query/revalidacao_hash_test.go	internal/query/revalidation_hash_test.go
internal/query/revalidacao_test.go	internal/query/revalidation_test.go
internal/query/sistema_test.go	internal/query/system_test.go
internal/query/tipos_test.go	internal/query/types_test.go
internal/query/binder/aninhado_test.go	internal/query/binder/nested_test.go
internal/query/binder/condicao.go	internal/query/binder/condition.go
internal/query/binder/condicao_test.go	internal/query/binder/condition_test.go
internal/query/binder/escala_test.go	internal/query/binder/scale_test.go
internal/query/binder/profundidade_test.go	internal/query/binder/depth_test.go
internal/query/binder/projecao_test.go	internal/query/binder/projection_test.go
internal/query/executor/orcamento_writer_test.go	internal/query/executor/budget_writer_test.go
internal/query/executor/ordem.go	internal/query/executor/order.go
internal/query/meta/fonte.go	internal/query/meta/source.go
internal/query/budget/orcamento.go	internal/query/budget/budget.go
internal/query/budget/orcamento_test.go	internal/query/budget/budget_test.go
internal/query/parser/cadeia_longa_test.go	internal/query/parser/long_chain_test.go
internal/query/parser/clausulas.go	internal/query/parser/clauses.go
internal/query/parser/clausulas_test.go	internal/query/parser/clauses_test.go
internal/query/parser/condicao.go	internal/query/parser/condition.go
internal/query/parser/posicoes_test.go	internal/query/parser/positions_test.go
internal/schema/congelada.go	internal/schema/frozen.go
internal/schema/congelada_test.go	internal/schema/frozen_test.go
internal/schema/contador.go	internal/schema/counter.go
internal/schema/contador_test.go	internal/schema/counter_test.go
internal/schema/dicionario_memoria.go	internal/schema/memory_dict.go
internal/schema/dicionario_memoria_test.go	internal/schema/memory_dict_test.go
internal/schema/expiracao_test.go	internal/schema/expiry_test.go
internal/schema/formato_test.go	internal/schema/format_test.go
internal/schema/historico.go	internal/schema/history.go
internal/schema/historico_test.go	internal/schema/history_test.go
internal/schema/regrattl.go	internal/schema/ttl_rule.go
internal/schema/regrattl_test.go	internal/schema/ttl_rule_test.go
internal/server/autenticacao.go	internal/server/auth.go
internal/server/autenticacao_internal_test.go	internal/server/auth_internal_test.go
internal/server/autenticacao_test.go	internal/server/auth_test.go
internal/server/contadores.go	internal/server/counters.go
internal/server/contadores_rotas_test.go	internal/server/counters_routes_test.go
internal/server/contadores_test.go	internal/server/counters_test.go
internal/server/documentos.go	internal/server/documents.go
internal/server/documentos_test.go	internal/server/documents_test.go
internal/server/emvoo.go	internal/server/inflight.go
internal/server/historico.go	internal/server/history.go
internal/server/historico_test.go	internal/server/history_test.go
internal/server/indice_hash_test.go	internal/server/index_hash_test.go
internal/server/materializacao.go	internal/server/materialization.go
internal/server/materializacao_http_test.go	internal/server/materialization_http_test.go
internal/server/materializacao_internal_test.go	internal/server/materialization_internal_test.go
internal/server/metricas_contadores_test.go	internal/server/metrics_counters_test.go
internal/server/metricas_dataset.go	internal/server/metrics_dataset.go
internal/server/metricas_dataset_internal_test.go	internal/server/metrics_dataset_internal_test.go
internal/server/metricas_dataset_test.go	internal/server/metrics_dataset_test.go
internal/server/metricas_indice.go	internal/server/metrics_index.go
internal/server/metricas_indice_test.go	internal/server/metrics_index_test.go
internal/server/metricas_query.go	internal/server/metrics_query.go
internal/server/metricas_query_test.go	internal/server/metrics_query_test.go
internal/server/mutacoes.go	internal/server/mutations.go
internal/server/mutacoes_internal_test.go	internal/server/mutations_internal_test.go
internal/server/mutacoes_test.go	internal/server/mutations_test.go
internal/server/orcamento_test.go	internal/server/budget_test.go
internal/server/querylonga_test.go	internal/server/longquery_test.go
internal/server/seguranca.go	internal/server/security.go
internal/server/seguranca_internal_test.go	internal/server/security_internal_test.go
internal/server/seguranca_test.go	internal/server/security_test.go
internal/server/versao_test.go	internal/server/version_test.go
internal/storage/agenda.go	internal/storage/schedule.go
internal/storage/agenda_test.go	internal/storage/schedule_test.go
internal/storage/aposentadoria_contadores_test.go	internal/storage/counters_retirement_test.go
internal/storage/auditoria_contadores_test.go	internal/storage/counters_audit_test.go
internal/storage/bitmap_ordem_test.go	internal/storage/bitmap_order_test.go
internal/storage/checkpoint/historico_test.go	internal/storage/checkpoint/history_test.go
internal/storage/checkpoint/manifest_contadores_test.go	internal/storage/checkpoint/manifest_counters_test.go
internal/storage/close_indice_test.go	internal/storage/close_index_test.go
internal/storage/coletor_test.go	internal/storage/collector_test.go
internal/storage/compact_uma_vez_test.go	internal/storage/compact_once_test.go
internal/storage/counter/aposentadoria.go	internal/storage/counter/retirement.go
internal/storage/counter/aposentadoria_test.go	internal/storage/counter/retirement_test.go
internal/storage/counter/auditoria.go	internal/storage/counter/audit.go
internal/storage/counter/chavetexto.go	internal/storage/counter/textkey.go
internal/storage/counter/chavetexto_test.go	internal/storage/counter/textkey_test.go
internal/storage/counter/contador_gate_race_test.go	internal/storage/counter/counter_gate_race_test.go
internal/storage/counter/contador_gate_test.go	internal/storage/counter/counter_gate_test.go
internal/storage/counter/contador.go	internal/storage/counter/counter.go
internal/storage/counter/contador_test.go	internal/storage/counter/counter_test.go
internal/storage/counter/dicionario_gate_race_test.go	internal/storage/counter/dict_gate_race_test.go
internal/storage/counter/dicionario_gate_test.go	internal/storage/counter/dict_gate_test.go
internal/storage/counter/dicionario.go	internal/storage/counter/dict.go
internal/storage/counter/dicionario_test.go	internal/storage/counter/dict_test.go
internal/storage/counter/dobra_bench_test.go	internal/storage/counter/fold_bench_test.go
internal/storage/contadores_bench_test.go	internal/storage/counters_bench_test.go
internal/storage/contadores.go	internal/storage/counters.go
internal/storage/contadores_test.go	internal/storage/counters_test.go
internal/storage/derivadas_test.go	internal/storage/derived_test.go
internal/storage/dictcompartilhado_test.go	internal/storage/shared_dict_test.go
internal/storage/expiracao.go	internal/storage/expiry.go
internal/storage/expiracao_test.go	internal/storage/expiry_test.go
internal/storage/export_contadores_test.go	internal/storage/export_counters_test.go
internal/storage/gate_disco_expiracao_test.go	internal/storage/expiry_disk_gate_test.go
internal/storage/historico.go	internal/storage/history.go
internal/storage/historico_test.go	internal/storage/history_test.go
internal/storage/indice_completude_test.go	internal/storage/index_completeness_test.go
internal/storage/indices_janela.go	internal/storage/indices_window.go
internal/storage/indices_janela_test.go	internal/storage/indices_window_test.go
internal/storage/indices_metricas.go	internal/storage/indices_metrics.go
internal/storage/indices_metricas_test.go	internal/storage/indices_metrics_test.go
internal/storage/indices_reconstroi.go	internal/storage/indices_rebuild.go
internal/storage/indices_reconstroi_test.go	internal/storage/indices_rebuild_test.go
internal/storage/inode_preso_test.go	internal/storage/inode_pinned_test.go
internal/storage/medicao_carencia_test.go	internal/storage/grace_measurement_test.go
internal/storage/mmap/region_atomico32_test.go	internal/storage/mmap/region_atomic32_test.go
internal/storage/prazos_test.go	internal/storage/deadlines_test.go
internal/storage/retencao.go	internal/storage/retention.go
internal/storage/retencao_test.go	internal/storage/retention_test.go
internal/storage/rowindex/agenda_gate_race_test.go	internal/storage/rowindex/schedule_gate_race_test.go
internal/storage/rowindex/agenda_gate_test.go	internal/storage/rowindex/schedule_gate_test.go
internal/storage/rowindex/agenda.go	internal/storage/rowindex/schedule.go
internal/storage/rowindex/agenda_test.go	internal/storage/rowindex/schedule_test.go
internal/storage/rowindex/denso32_gate_race_test.go	internal/storage/rowindex/dense32_gate_race_test.go
internal/storage/rowindex/denso32_gate_test.go	internal/storage/rowindex/dense32_gate_test.go
internal/storage/rowindex/denso32.go	internal/storage/rowindex/dense32.go
internal/storage/rowindex/denso32_test.go	internal/storage/rowindex/dense32_test.go
internal/storage/rowindex/escritas_test.go	internal/storage/rowindex/writes_test.go
internal/storage/rowindex/prazos_test.go	internal/storage/rowindex/deadlines_test.go
internal/storage/store_bitmap_orfa_residuo_test.go	internal/storage/store_bitmap_orphan_residue_test.go
internal/storage/store_contrato_test.go	internal/storage/store_contract_test.go
internal/storage/store_liveness_espelho_test.go	internal/storage/store_liveness_mirror_test.go
internal/storage/store_liveness_residuo_test.go	internal/storage/store_liveness_residue_test.go
internal/storage/store_recovery_janelas_test.go	internal/storage/store_recovery_window_test.go
internal/storage/store_recovery_segmento_test.go	internal/storage/store_recovery_segment_test.go
internal/storage/store_sc_lacunas_test.go	internal/storage/store_sc_gaps_test.go
internal/storage/versions/versoes.go	internal/storage/versions/versions.go
internal/storage/versions/versoes_test.go	internal/storage/versions/versions_test.go
internal/testutil/fakestorage/comportamento_test.go	internal/testutil/fakestorage/behavior_test.go
internal/testutil/fakestorage/paridade_test.go	internal/testutil/fakestorage/parity_test.go
internal/typeparse/datahora.go	internal/typeparse/datetime.go
internal/typeparse/datahora_test.go	internal/typeparse/datetime_test.go
scripts/identifier-len/coleta.go	scripts/identifier-len/collect.go
scripts/identifier-len/coleta_test.go	scripts/identifier-len/collect_test.go
scripts/identifier-len/estatisticas.go	scripts/identifier-len/stats.go
scripts/identifier-len/estatisticas_test.go	scripts/identifier-len/stats_test.go
scripts/identifier-len/extracao.go	scripts/identifier-len/extract.go
scripts/identifier-len/extracao_test.go	scripts/identifier-len/extract_test.go
scripts/identifier-len/relatorio.go	scripts/identifier-len/report.go
```

Nenhum nome novo pode terminar em sufixo de GOOS/GOARCH antes do `_test.go`
(`_linux`, `_windows`, `_darwin`, `_amd64`, `_arm64`, `_386`...): o Go leria o
arquivo como restrito àquele sistema e ele sumiria da suíte em silêncio.
Conferir com `grep -E '_(linux|darwin|windows|freebsd|amd64|arm64|386|arm)(_test)?\.go$'`
sobre a coluna `novo`: só `memprof_linux.go`, que não muda. A primeira
versão desta tabela dizia `store_recovery_windows_test.go` e caiu nessa
armadilha — a checagem da Tarefa 1.2 pegou; o nome é `store_recovery_window_test.go`,
no singular.

- [ ] **Passo 2: Aplicar com um script no scratchpad**

`$SCRATCH/mv-arquivos.sh`:

```bash
#!/usr/bin/env bash
set -eu
tail -n +2 planning/identificadores-em-ingles/mapeamento-arquivos.tsv | while IFS=$'\t' read -r velho novo; do
  [ -f "$velho" ] || { echo "ausente: $velho" >&2; exit 1; }
  git mv "$velho" "$novo"
done
go build ./...
go vet ./...
```

Run: `bash $SCRATCH/mv-arquivos.sh`
Esperado: sem saída de erro; build e vet verdes (mover arquivo não muda
código).

- [ ] **Passo 3: Passada de prosa dos caminhos de arquivo**

```bash
go run ./scripts/renamecite -dir . -paths planning/identificadores-em-ingles/mapeamento-arquivos.tsv -report $SCRATCH/renamecite-arquivos.txt
```

Essa passada cobre citação por caminho completo, mas deixa para trás citação
solta por nome-base em prosa e comentário ("ver `escritor.go`"). Uma segunda
passada de `renamecite -paths`, agora com
`planning/identificadores-em-ingles/mapeamento-arquivos-base.tsv` (nome-base
velho → nome-base novo, injetiva nas duas colunas — verificado por
`cut -f1 | sort | uniq -d` e `cut -f2 | sort | uniq -d` vazios), pega essas
citações remanescentes. As renomeações de arquivo da Fase 5 (classes JDBC,
módulos Python) precisam das mesmas duas passadas.

Conferir em `git diff` que `internal/gates/citations_test.go` teve a lista
`arquivosDoIndicePorSegmento` reescrita (`indices_window.go`,
`indices_metrics.go`, `indices_rebuild.go`, `metrics_index.go`,
`history.go`, `documents.go`, `names.go`) e que `CLAUDE.md` cita
`internal/metrics/names.go`.

Run: `make test`
Esperado: PASS — inclusive `TestAsCitacoesDeTesteNosComentariosExistem`,
que confere que todo arquivo da lista existe.

- [ ] **Passo 4: Commit**

```bash
git add -A
git commit -m "Arquivos .go em PT-BR passam a ter nome em inglês; citações de caminho atualizadas"
```

### Tarefa 1.3: Fechamento da Fase 1

- [ ] **Passo 1: Race, lint, integração**

Run: `make test-race`
Esperado: PASS.
Run: `make test-integration`
Esperado: PASS.
Run: `make lint`
Esperado: contagem igual à baseline (mover arquivo e renomear pacote não
cria apontamento). Se diferente, investigar antes de mexer na baseline.

- [ ] **Passo 2: Merge**

PR "Identificadores em inglês — Fase 1: pacotes e arquivos" → `main`.
Branches abertas rebaseiam.

---

## Fase 2 — Engine

### Tarefa 2.0: O workflow de ciclo por grupo

**Arquivos:**
- Criar (no scratchpad, não versionado): `$SCRATCH/ciclo-de-grupo.js`

**Interfaces:**
- Consome: `planning/identificadores-em-ingles/inventario-inicial.tsv`, `grupos-inicial.tsv`, `docs/glossario.md`, `scripts/rename-apply.sh`, `scripts/ifacerename`, `scripts/renamecite`.
- Produz: `planning/identificadores-em-ingles/mapeamento-<fase>-<pacote>.tsv` por pacote, `grupos-de-interface-<fase>-<grupo>.tsv`, e os commits do grupo. É chamado com `args = {fase: 2, grupo: "G1", pacotes: [...], scratch: "..."}`; nas Fases 3 e 4 muda o `fase` e os prompts trocam de papel (Tarefas 3.0 e 4.0 dizem como).

O script é uma sequência de papéis (spec §9). Agentes leem e escrevem
arquivos por conta própria; o script só carrega os resultados
estruturados de um papel ao seguinte.

- [ ] **Passo 1: Escrever o script**

```javascript
export const meta = {
  name: 'ciclo-de-grupo',
  description: 'Fase de identificadores em inglês: propor, julgar, refutar, aplicar, prosa e verificar um grupo de pacotes',
  phases: [
    { title: 'Propor', detail: 'um tradutor por pacote, a partir do inventário e do glossário' },
    { title: 'Julgar', detail: 'consolida contra o glossário, resolve colisões, separa grupos de interface' },
    { title: 'Refutar', detail: 'três lentes por pacote: sentido, convenção Go, colisão/grupo' },
    { title: 'Aplicar', detail: 'um aplicador só: ifacerename, gopls por posição, rf e renamecite por pacote, commits' },
    { title: 'Prosa', detail: 'um revisor de prosa por pacote (sem git); um agente commita' },
    { title: 'Verificar', detail: 'make test, test-race, lint, gates; crítico de completude' },
  ],
}

const { fase, grupo, pacotes, scratch } = args
const INV = `planning/identificadores-em-ingles/inventario-antes-${grupo}.tsv`
const GRUPOS = `planning/identificadores-em-ingles/grupos-antes-${grupo}.tsv`
const GLOSSARIO = 'docs/glossario.md'
const SPEC = 'planning/identificadores-em-ingles.md'
const PROTEGIDOS = 'planning/identificadores-em-ingles/protegidos.txt'
const CAMINHOS = '-paths planning/identificadores-em-ingles/mapeamento-pacotes.tsv -paths planning/identificadores-em-ingles/mapeamento-arquivos.tsv'
const tsvDe = (p) => `planning/identificadores-em-ingles/mapeamento-${fase}-${p.replaceAll('/', '_')}.tsv`
const tsvGrupos = `planning/identificadores-em-ingles/grupos-de-interface-${fase}-${grupo}.tsv`

const PROPOSTA = {
  type: 'object',
  required: ['pacote', 'entradas', 'grupos', 'mantidos'],
  properties: {
    pacote: { type: 'string' },
    entradas: { type: 'array', items: { type: 'object', required: ['especie', 'endereco', 'novo', 'motivo'],
      properties: { especie: { type: 'string' }, endereco: { type: 'string' }, novo: { type: 'string' }, motivo: { type: 'string' } } } },
    grupos: { type: 'array', items: { type: 'object', required: ['grupo', 'interface', 'metodo', 'novo'],
      properties: { grupo: { type: 'string' }, interface: { type: 'string' }, metodo: { type: 'string' }, novo: { type: 'string' } } } },
    mantidos: { type: 'array', items: { type: 'object', required: ['endereco', 'motivo'],
      properties: { endereco: { type: 'string' }, motivo: { type: 'string' } } } },
  },
}
const VEREDICTO = {
  type: 'object', required: ['refutadas'],
  properties: { refutadas: { type: 'array', items: { type: 'object', required: ['endereco', 'motivo', 'sugestao'],
    properties: { endereco: { type: 'string' }, motivo: { type: 'string' }, sugestao: { type: 'string' } } } } },
}
const RELATORIO = { type: 'object', required: ['ok', 'saida'], properties: { ok: { type: 'boolean' }, saida: { type: 'string' } } }

const ambiente = `Ambiente: worktree /home/lucas/Projects/nishisan/nishidb/.claude/worktrees/identificadores-em-ingles, branch
refactor/identificadores-em-ingles; nunca toque em /home/lucas/Projects/nishisan/nishidb. O guarda do worktree recusa comandos bash
compostos com git: UM comando git por chamada; laços vão para um arquivo .sh rodado com "bash arquivo.sh". Prosa em PT-BR,
identificadores em inglês, mensagens de commit em PT-BR na voz do time, sem menção a agente.`

const regras = `Regras (spec ${SPEC} §6, glossário ${GLOSSARIO}): identificadores em inglês idiomático Go
(MixedCaps; iniciais ID, URL, HTTP, JSON, TLS, CRC, TTL, SQL, NDJSON, CLI, RSS; sem Get; nada de sufixo numérico);
o que já está em inglês NÃO muda (entra em "mantidos" com motivo "já em inglês"); nome híbrido traduz só a
metade PT-BR (ErrOrcamentoExcedido → ErrBudgetExceeded); termos de domínio SÓ do glossário — termo ausente
vira proposta de linha nova no glossário, no campo motivo, prefixada por "GLOSSARIO:"; receptores de até
três letras não mudam; método que aparece na coluna grupo do inventário NÃO entra em "entradas": entra em
"grupos" com o nome novo, e o mesmo grupo recebe o mesmo nome em todos os pacotes. Espécies com sufixo
"#ambiguo" ou "#gopls" são renomeadas por posição (gopls), não pelo rf: proponha o nome novo do mesmo jeito,
mantendo a especie com o sufixo. Nomes de teste (func Test*/Benchmark*/Fuzz*) são da Fase 4: ignore-os.
Um nome cujo godoc precisaria descrevê-lo com o verbo da operação irmã (Return explicado com "libera", verbo de
Release) é sinal de nome errado: julgue contra a operação irmã, não só contra o glossário.`

// ---------- Inventário ----------
phase('Propor')
const inventario = await agent(`Você gera o inventário do grupo ${grupo} sobre a árvore ATUAL (os grupos anteriores mudaram
nomes e deslocaram colunas nos chamadores; posições velhas não valem). ${ambiente}
Rode "go run ./scripts/renameinv -dir . -out ${INV} -groups ${GRUPOS}" (2 a 3 minutos), confira que os dois arquivos
existem e têm cabeçalho, e devolva a linha de stdout ("N declarações, M grupos de interface"). Não faça commit.`,
  { label: `inventario:${grupo}`, phase: 'Propor', model: 'sonnet' })
log(`inventário: ${String(inventario).slice(0, 120)}`)

// ---------- Propor ----------
const propostas = (await parallel(pacotes.map(p => () =>
  agent(`Você é o tradutor do pacote ${p} na fase ${fase} (grupo ${grupo}) do ciclo de identificadores em inglês.
Leia ${INV} e filtre as linhas cujo pacote é exatamente "${p}" (todas as espécies, exportadas ou não; ignore
as de especie "func" cujo nome começa por Test, Benchmark ou Fuzz — são da Fase 4). Leia ${GRUPOS} para saber
o fecho de cada grupo G<n> citado. Leia ${GLOSSARIO} inteiro. Abra os arquivos do pacote para entender o que
cada nome significa antes de traduzir: o nome novo tem de preservar o SENTIDO, não a forma. Campos de struct
anônima (tabelas de casos de teste: "casos := []struct{ campo string; quer int }") aparecem no inventário como
"field#gopls" com posição — traduza-os como qualquer campo; se algum não estiver no inventário, procure no código
e inclua-o com especie "field#gopls" e a posição real.
${regras}
${ambiente}
Devolva a proposta completa: uma entrada por linha do inventário que muda, um "mantido" por linha que fica
(o inventário tem MUITAS linhas de locais já em inglês — err, ok, i, n, buf —, que entram em "mantidos" com
motivo "já em inglês"). Nenhuma linha do inventário do pacote pode ficar sem decisão.`,
  { label: `traduz:${p}`, phase: 'Propor', schema: PROPOSTA, model: 'opus' })))).filter(Boolean)
log(`${propostas.length}/${pacotes.length} propostas`)

// ---------- Julgar ----------
phase('Julgar')
const juizo = await agent(`Você é o juiz do grupo ${grupo} (fase ${fase}). Recebe as propostas abaixo, uma por pacote, e produz os TSV finais.
Propostas: ${JSON.stringify(propostas)}
${regras}
${ambiente}
Tarefas, nesta ordem:
1. Consistência: o mesmo termo PT-BR vira o mesmo termo inglês em todos os pacotes; conflito resolve pelo glossário e, se
   o glossário não decide, pelo nome mais específico. Registre no motivo.
2. Colisão: dentro de um pacote, dois endereços não podem receber o mesmo nome novo no mesmo escopo (tipo, função,
   pacote). Para conferir, rode "rf -diff" no diretório do pacote com o script montado (uma linha "mv <endereco> <novo>"
   por entrada sem sufixo #ambiguo/#gopls; rf está no PATH) e leia a saída: "already have" é colisão. Resolva pelo nome
   mais específico. Não aplique nada: -diff só mostra.
3. Grupos de interface: toda entrada com grupo vai para ${tsvGrupos} (cabeçalho "pacote\\tinterface\\tmetodo\\tnovo"), UMA linha
   por grupo, com o mesmo novo em todos os pacotes; conferir em ${GRUPOS} que o fecho está coberto. Rode
   "go run ./scripts/ifacerename -dir . -groups ${tsvGrupos} -dry-run" e cole a saída no seu relatório. Se não houver grupo,
   crie o arquivo só com o cabeçalho.
4. Glossário: toda proposta "GLOSSARIO:" vira linha nova em ${GLOSSARIO} (mesmo formato de tabela, três colunas). A
   célula "inglês" leva SÓ o termo (ou os termos separados por " / "), nunca uma frase: a razão de uma escolha difícil
   vai para a seção §4 do glossário em prosa, uma vez. Dois sentidos qualificados do mesmo termo PT-BR não recebem o
   mesmo termo inglês.
5. Escreva, por pacote, ${tsvDe('<pacote>')} com cabeçalho "pacote\\tespecie\\tendereco\\tnovo\\tmotivo", uma linha por
   entrada (novo = nome novo; especie com o sufixo #ambiguo/#gopls preservado quando houver), e linhas de "mantidos" com
   novo igual ao nome atual e motivo "mantido: <motivo>". Motivo TERSO (uma frase); uma razão compartilhada por várias
   linhas é escrita uma vez e as outras citam "mesmo motivo de <endereco>". Para as linhas #ambiguo/#gopls acrescente ao
   motivo a posição "arquivo:linha:coluna" copiada do inventário, que o aplicador usa.
   Edição manual que o rename exige (godoc cujo sentido vira errado com a troca da primeira palavra, literal de string
   com o nome do tipo, valor de tabela de teste com o nome velho) entra no motivo NO FORMATO FIXO
   "ATENÇÃO arquivo:linha: <texto que some> → <texto novo>" — o aplicador aplica e confere que o texto sumiu.
6. Garanta que ${PROTEGIDOS} existe (crie vazio se não existir).
Não faça commit: o aplicador commita. Devolva um resumo: número de entradas por pacote, colisões resolvidas, termos novos
no glossário, e a saída do dry-run.`,
  { label: `juiz:${grupo}`, phase: 'Julgar', model: 'opus' })
log(`juiz: ${String(juizo).slice(0, 300)}`)

// ---------- Refutar (até 3 rodadas) ----------
phase('Refutar')
const LENTES = [
  ['sentido', 'o nome novo muda o SIGNIFICADO do velho, perde uma distinção que o código faz (dois conceitos PT-BR distintos colapsados no mesmo inglês), ou só se explica com o verbo da operação irmã'],
  ['convencao', 'viola convenção Go: Id/Url/Http em vez de ID/URL/HTTP, prefixo Get, sufixo numérico, receptor renomeado para mais de três letras sem motivo, nome de erro fora de ErrXxx/XxxError, ou termo fora do glossário; ou um identificador já em inglês foi alterado sem motivo'],
  ['colisao', 'colide no escopo (rode rf -diff no pacote e procure "already have"), ou um método de interface entrou em entradas em vez de grupos, ou o grupo não cobre o fecho de ' + GRUPOS + ', ou uma linha do inventário do pacote ficou sem decisão, ou uma linha ATENÇÃO não está no formato fixo "ATENÇÃO arquivo:linha: <some> → <novo>"'],
]
for (let rodada = 1; rodada <= 3; rodada++) {
  const vereditos = (await parallel(pacotes.flatMap(p => LENTES.map(([lente, criterio]) => () =>
    agent(`Você é o refutador "${lente}" do pacote ${p}. Leia ${tsvDe(p)}, ${tsvGrupos}, ${GLOSSARIO} e o código do pacote.
Tente DERRUBAR entradas com este critério: ${criterio}. Para cada uma que cair, dê o motivo e uma sugestão concreta.
Só refute com evidência (cite arquivo e linha); dúvida não é refutação. Não edite nada. Devolva a lista, vazia se nada cair.`,
      { label: `refuta:${lente}:${p}`, phase: 'Refutar', schema: VEREDICTO, model: 'opus' }))))).filter(Boolean)
  const caidas = vereditos.flatMap(v => v.refutadas)
  log(`rodada ${rodada}: ${caidas.length} refutações`)
  if (caidas.length === 0) break
  await agent(`Você é o corretor do grupo ${grupo}. Estas entradas foram refutadas: ${JSON.stringify(caidas)}.
Para cada uma, corrija o TSV correspondente (${pacotes.map(tsvDe).join(', ')} ou ${tsvGrupos}) aplicando a sugestão
ou uma alternativa melhor, mantendo a consistência entre pacotes e o glossário (${GLOSSARIO}). ${regras}
Não faça commit. Devolva o que mudou.`, { label: `corrige:${grupo}:${rodada}`, phase: 'Refutar', model: 'opus' })
}

// ---------- Aplicar (um só) ----------
phase('Aplicar')
const aplicacao = await agent(`Você é o ÚNICO aplicador do grupo ${grupo} (fase ${fase}). Ninguém mais edita código enquanto você roda.
${ambiente}
Ordem obrigatória (spec §4.6), com commit ao fim de cada passo:
0. Commit do mapeamento: "git add planning/identificadores-em-ingles docs/glossario.md" e
   "git commit -m '${grupo}: mapeamento de identificadores'".
1. Se ${tsvGrupos} tem linhas além do cabeçalho: "go run ./scripts/ifacerename -dir . -groups ${tsvGrupos}", depois
   "go build ./..." e "go vet ./...". Vermelho → "git checkout -- ." e PARE devolvendo ok=false com a saída.
   Verde → "git add -A" e "git commit -m '${grupo}: métodos de interface em inglês'".
2. Para cada pacote, na ordem ${JSON.stringify(pacotes)}:
   a) Renames por posição PRIMEIRO (antes de o rf mexer nos arquivos, quando as posições do inventário ainda valem): para
      cada linha de ${tsvDe('<pacote>')} com especie terminada em "#ambiguo" ou "#gopls" e novo ≠ nome atual, rode
      "gopls rename -w <arquivo>:<linha>:<coluna> <novo>" (a posição está no motivo). Processe em ordem DECRESCENTE de
      linha e, na mesma linha, decrescente de coluna, para um rename não deslocar a coluna do seguinte. Use um daemon:
      antes do primeiro, 'gopls -listen="unix;/run/user/1000/gopls-ciclo.sock" >/dev/null 2>&1 &' e depois
      'gopls -remote="unix;/run/user/1000/gopls-ciclo.sock" rename -w ...' (~2 s por rename); mate o daemon ao fim do
      grupo. Se o gopls recusar um rename (conflito), pule-o, anote no relatório com o motivo e siga.
   b) Monte ${scratch}/<pacote com / trocado por _>.rf com uma linha "mv <endereco> <novo>" por entrada SEM sufixo cuja
      novo ≠ nome atual (pule as "mantido"). Rode "scripts/rename-apply.sh <pacote> <script.rf> <script.diff>". Leia o diff.
      Exit ≠ 0 → nada foi escrito por ele; leia o stderr: se for "already have" ou tipo, tire a(s) linha(s) ofensiva(s)
      para um arquivo <pacote>.recusadas.txt no scratch, anote, e rode de novo sem elas; se falhar de novo, devolva ok=false.
   c) Edições ATENÇÃO: para cada motivo do TSV do pacote que começa por "ATENÇÃO arquivo:linha:", aplique a edição e
      confira com grep que o "<texto que some>" não existe mais no arquivo; se ainda existir, corrija; registre cada uma.
   d) Passada de prosa DO PACOTE: "go run ./scripts/renamecite -dir . -idents ${tsvDe('<pacote>')} ${CAMINHOS} -protect ${PROTEGIDOS} -report ${scratch}/renamecite-<pacote>.txt".
      Leia as linhas "aplicado" e "pulado": toda "pulado ... ambíguo" é resolvida qualificando a citação à mão (é prosa);
      toda "aplicado" cujo token não era citação de identificador (nome de coluna, chave YAML, arquivo em disco, palavra
      de prosa) é revertida à mão e o token entra em ${PROTEGIDOS} (um por linha) no mesmo commit.
   e) "go build ./...", "go vet ./...", "gofmt -l ." vazio → "git add -A" e "git commit -m '<pacote>: identificadores em inglês'".
3. Gate: "go test ./internal/gates/ -run 'Renamed|Citacoes|Evidencias|Glossary' -count=1". Vermelho → conserte a citação
   pendurada apontada (é prosa/literal, não código de lógica) e faça "git add -A" + "git commit -m '${grupo}: citações que
   as regras não alcançaram'". Verde → siga.
Nunca edite código Go de lógica à mão. Devolva ok e a saída resumida de cada passo, inclusive quantos renames por posição,
quantas linhas recusadas e quantas edições ATENÇÃO.`,
  { label: `aplica:${grupo}`, phase: 'Aplicar', schema: RELATORIO, model: 'opus' })
if (!aplicacao || !aplicacao.ok) {
  log(`aplicação falhou: ${aplicacao ? aplicacao.saida.slice(0, 500) : 'sem retorno'}`)
  return { ok: false, etapa: 'aplicar', saida: aplicacao && aplicacao.saida }
}

// ---------- Prosa (paralelo, sem git; um commit ao fim) ----------
phase('Prosa')
const prosa = (await parallel(pacotes.map(p => () =>
  agent(`Você é o revisor de prosa do pacote ${p} depois do rename (grupo ${grupo}). ${ambiente}
NÃO rode nenhum comando git além de "git log" e "git show"/"git diff" (só leitura); outro agente commita depois.
Ache o commit "<pacote>: identificadores em inglês" de ${p} com "git log --oneline -40" e leia "git show <sha> -- ${p}".
Edite SÓ arquivos dentro de ${p}/ (nunca de outro pacote), consertando: (1) frases de godoc/comentário em que o
identificador velho era o substantivo-cabeça e a troca da primeira palavra quebrou a frase ("// Format de saída." →
"// Format é o formato de saída."; parâmetro renomeado citado pelo nome velho em prosa: "teto <= 0" → "ceiling <= 0");
(2) menções ao nome velho que a ferramenta não alcançou (compare com ${tsvDe(p)}); (3) linhas de comentário que
cresceram além de 80 colunas por causa do nome novo (re-wrap do parágrafo, sem mudar o texto); (4) godoc de
declaração exportada que não começa pelo nome novo. Prosa continua em PT-BR; não traduza prosa; não toque em código.
Devolva ok=true e a lista de arquivos e linhas editadas (ou "nada a fazer").`,
    { label: `prosa:${p}`, phase: 'Prosa', schema: RELATORIO, model: 'opus' })))).filter(Boolean)
log(`prosa: ${prosa.filter(r => r.ok).length}/${pacotes.length} pacotes revisados`)
const commitProsa = await agent(`Você commita a revisão de prosa do grupo ${grupo}. ${ambiente}
Rode "gofmt -l ." (tem de sair vazio; se não, rode gofmt -w nos arquivos listados), "go build ./...", "go vet ./...",
"go test ./internal/gates/ -run 'Renamed|Citacoes|Evidencias|Glossary' -count=1". Se algum for vermelho, devolva ok=false
com a saída (não conserte). Se "git status --short" estiver vazio, devolva ok=true "nada a commitar". Senão "git add -A" e
"git commit -m '${grupo}: prosa ajustada após o rename'" e devolva ok=true com o sha.`,
  { label: `commit-prosa:${grupo}`, phase: 'Prosa', schema: RELATORIO, model: 'sonnet' })
if (!commitProsa || !commitProsa.ok) {
  log(`commit da prosa falhou: ${commitProsa ? commitProsa.saida.slice(0, 500) : 'sem retorno'}`)
  return { ok: false, etapa: 'prosa', saida: commitProsa && commitProsa.saida }
}

// ---------- Verificar ----------
phase('Verificar')
const [verificacao, completude] = await parallel([
  () => agent(`Você é o verificador do grupo ${grupo}. ${ambiente}
Rode, nesta ordem, e cole a saída resumida de cada um:
"make build", "make test", "make test-race", "go test ./internal/gates/ -count=1", "make lint" (a contagem impressa
tem de ser igual ao conteúdo de .golangci-baseline; se for maior, liste os apontamentos novos com arquivo:linha).
Qualquer vermelho: ok=false, com a saída COMPLETA do que falhou e o seu diagnóstico (mapeamento errado? citação morta?
godoc que não começa pelo nome novo?). Não corrija nada.`, { label: `verifica:${grupo}`, phase: 'Verificar', schema: RELATORIO, model: 'opus' }),
  () => agent(`Você é o crítico de completude do grupo ${grupo}. ${ambiente}
Rode "go run ./scripts/renameinv -dir . -out ${scratch}/inv-${grupo}.tsv -groups ${scratch}/grp-${grupo}.tsv" e, para cada
pacote em ${JSON.stringify(pacotes)}, compare as linhas do pacote no inventário novo com ${tsvDe('<pacote>')}: todo nome
que ainda é o velho (coluna endereco do TSV) e não está marcado "mantido" no TSV é uma falta; todo "mantido" cujo
motivo não é "já em inglês" nem "receptor curto" nem "nome de teste, Fase 4" é suspeito e entra na lista; toda linha
do inventário novo com nome em PT-BR (acento, ou palavra que o glossário lista como termo PT-BR, ou morfologia
-ção/-mento/-dade/-ncia/-ável/-ível) fora de Test*/Benchmark*/Fuzz* é falta; toda linha ATENÇÃO do TSV cujo "<texto que
some>" ainda existe no arquivo é falta. Liste as faltas com endereço. ok=true só com zero faltas. Não corrija nada.`,
    { label: `completude:${grupo}`, phase: 'Verificar', schema: RELATORIO, model: 'opus' }),
])
return { ok: !!(verificacao && verificacao.ok && completude && completude.ok), verificacao, completude }
```

- [ ] **Passo 2: Inventário pós-Fase 1 e lista de protegidos**

O `inventario-inicial.tsv` foi gerado antes da Fase 1 e traz pacotes e
arquivos com os nomes velhos nas colunas `pacote` e `posicao`. O workflow lê
um inventário regenerado sobre a árvore já renomeada:

```bash
go run ./scripts/renameinv -dir . -out planning/identificadores-em-ingles/inventario-pos-fase1.tsv -groups planning/identificadores-em-ingles/grupos-pos-fase1.tsv
touch planning/identificadores-em-ingles/protegidos.txt
git add planning/identificadores-em-ingles planning/identificadores-em-ingles-plano.md
git commit -m "planning: inventário pós-Fase 1 e lista de protegidos para o workflow da Fase 2"
```

O inicial continua sendo a base de completude do fechamento (os nomes das
declarações não mudaram na Fase 1; só o lugar onde moram).

- [ ] **Passo 3: Ensaio no grupo menor**

Rodar o workflow com `args = {fase: 2, grupo: "G0", pacotes: ["internal/units", "internal/unsafex"], scratch: "$SCRATCH"}`.
Os dois pacotes têm poucas declarações (`units` tem quatro funções
exportadas); o ensaio prova o encanamento inteiro — TSV escrito, `rf`
aplicado, commit feito, verificador verde — antes de gastar agentes no G1.

Esperado: `ok: true`; `git log --oneline -3` mostra os commits do G0.

### Tarefa 2.1 a 2.6: Os seis grupos

Para cada linha da tabela, rodar o workflow da Tarefa 2.0 com o `grupo` e
os `pacotes` dados, depois os passos de fechamento abaixo. Os pacotes já
carregam os nomes da Fase 1.

| Tarefa | Grupo | Pacotes |
|---|---|---|
| 2.1 | G1 | `internal/types`, `internal/typeparse`, `internal/jsonp`, `internal/logging`, `internal/memprof`, `internal/buildinfo`, `internal/storage/mmap`, `internal/storage/checkpoint`, `internal/query/lexer`, `internal/query/budget`, `internal/testutil/radiusgen`, `internal/gates` |
| 2.2 | G2 | `internal/index/valuekey`, `internal/metrics`, `internal/storage/encoding`, `internal/storage/fielddict`, `internal/index/primary`, `internal/index/segindex`, `internal/protocol`, `internal/query/parser`, `internal/query/spill`, `internal/storage/rowindex`, `internal/storage/segment`, `internal/storage/versions` |
| 2.3 | G3 | `internal/schema`, `internal/config`, `internal/testutil/fakestorage`, `internal/query/binder`, `internal/query/planner`, `internal/storage/counter` |
| 2.4 | G4 | `internal/kafka`, `internal/predicate`, `internal/materializer`, `internal/query/executor`, `internal/query`, `internal/query/meta` |
| 2.5 | G5 | `internal/metakafka`, `internal/counters`, `internal/storage` |
| 2.6 | G6 | `internal/server`, `internal/cli`, `cmd/nishidb`, `benchmark/ingest-bench`, `benchmark/micro`, `scripts/certgen`, `scripts/identifier-len`, `scripts/renameinv`, `scripts/ifacerename`, `scripts/renamecite` |

Grupos de interface conhecidos por grupo (de `grupos-inicial.tsv`; o juiz
confere a lista completa): G1 nenhum; G2 `metrics.Collector`,
`fielddict.dictFile`, `segindex.Fonte`, `types.RowIDIterator` (25
implementadores, em nove pacotes — o `ifacerename` renomeia todos de uma
vez mesmo os pacotes estando em grupos posteriores, e é isso que o spec
§4.6 chama de "grupos primeiro"); G3 `schema.FieldDictionary`,
`planner.Plan`, `planner.Store`, `binder.BoundExpr`, `counter.Avaliador`,
`counter.arquivoDoDicionario`, `fakestorage.consumerStore` e os dois
`fakestorage_test.*`; G4 `materializer.Store`,
`materializer.EscritorDeHistorico`, `kafka.Store`, `executor.Store`,
`executor.RowIterator`, `executor.DiretorioDeSpill`, `executor.rowSource`,
`meta.Capacidades`, `query.Store`; G5 `counters.Contado`, `counters.Leitor`,
`metakafka.Consumido`; G6 `ingest-bench.fonte`.

Fechamento de cada grupo:

- [ ] **Passo 1: Workflow verde** — `ok: true` no retorno; senão, ler o
  `journal.jsonl` do run, corrigir o TSV apontado (é o juiz que erra, não o
  `rf`) e relançar com `resumeFromRunId`.
- [ ] **Passo 2: Integração** quando o grupo toca `kafka`, `cli` ou `server`
  (G4, G6): `make test-integration`, PASS.
- [ ] **Passo 3: Baseline** — `make lint`; se a contagem mudou, entender por
  quê (`revive exported`: godoc que não começa pelo nome novo é erro do
  `rf`/`ifacerename` e se corrige no código; `var-naming`: iniciais erradas
  no mapeamento), corrigir, e só então igualar `.golangci-baseline`, no
  mesmo commit "lint: linha de base após <grupo>".
- [ ] **Passo 4: Merge** — PR "Identificadores em inglês — Fase 2, <grupo>"
  → `main`. Branches abertas rebaseiam.

### Tarefa 2.7: Fechamento da Fase 2

- [ ] **Passo 1: Inventário intermediário**

```bash
go run ./scripts/renameinv -dir . -out planning/identificadores-em-ingles/inventario-pos-fase2.tsv -groups planning/identificadores-em-ingles/grupos-pos-fase2.tsv
```

Um agente crítico compara com `inventario-inicial.tsv`: fora `Test*`,
`Benchmark*`, `Fuzz*` (Fase 4), toda linha com nome ainda em PT-BR é
falta. Zero faltas, ou volta ao grupo dono.

- [ ] **Passo 2: Commit**

```bash
git add planning/identificadores-em-ingles/inventario-pos-fase2.tsv planning/identificadores-em-ingles/grupos-pos-fase2.tsv
git commit -m "planning: inventário após a Fase 2 dos identificadores em inglês"
```

---

## Fase 3 — Emissões e contratos

### Tarefa 3.0: Workflow de emissões

Reutiliza `$SCRATCH/ciclo-de-grupo.js` com `fase: 3`, trocando os três
prompts de papel (o script é copiado para `$SCRATCH/ciclo-de-emissoes.js`
com estas diferenças; o resto — julgar, refutar, aplicar, verificar — é
igual):

- **Tradutor** (por pacote): "Liste toda emissão do pacote: `errors.New`,
  `fmt.Errorf`, `log.Info/Warn/Error/Debug` (a chave, primeiro argumento, e
  a mensagem), `slog.String/Int/...` (atributo), `http.Error`, e toda
  asserção de teste sobre esse texto (`strings.Contains(err.Error(), ...)`,
  `strings.Contains(out, ...)`, `t.Run` com o texto). Proponha o texto novo
  em inglês, minúsculas, com o prefixo `pacote:` e o `%w` preservados;
  chaves de log em `dominio.evento` snake_case inglês. Entradas: `especie`
  ∈ `msg|logkey|attr|assert`, `endereco` = `arquivo:linha`, `novo` = texto
  novo." O TSV do pacote vira `mapeamento-3-<pacote>.tsv` com essas
  colunas, e as chaves de log vão TAMBÉM para
  `planning/identificadores-em-ingles/mapeamento-chaves-de-log.tsv`
  (`velho`, `novo`), que o `renamecite -logkeys` consome nos docs. Antes da
  primeira passada de prosa desta fase, os nomes de contrato que NÃO mudam
  de nome (colunas das tabelas `_meta`, chaves YAML e JSON já em inglês,
  códigos do wire, nomes de arquivo em disco que ficam) entram em
  `planning/identificadores-em-ingles/protegidos.txt`, e todo `renamecite`
  do ciclo recebe `-protect` com esse arquivo.
- **Aplicador**: edita os arquivos à mão (é texto, não identificador), um
  pacote por commit "`<pacote>`: mensagens e chaves de log em inglês", e
  roda `go test ./<pacote>/ -count=1` após cada um: a asserção que ficou
  para trás é vermelho imediato. Depois `renamecite -logkeys` para
  `docs/operacao-kafka.md`, `docs/architecture.md`, `docs/metricas.md`,
  `README.md`, `docs/diagrams/*.puml`.
- **Refutadores**: lentes `sentido` (a mensagem nova diz o que a velha
  dizia, inclusive o remédio — "reopen the dataset"), `contrato` (sentinela,
  código de exit, código do wire, nome de métrica NÃO mudaram de valor),
  `asserção` (toda asserção sobre texto foi atualizada; `grep -rn` do texto
  velho em `_test.go` devolve vazio).

Rodar por grupo na mesma ordem G1→G6 da Fase 2, com merge por grupo.

### Tarefa 3.1: Contratos

**Arquivos:**
- Criar: `planning/identificadores-em-ingles/mapeamento-contratos.tsv`
- Modificar: `internal/config/config.go:81-82,322,325,511` (YAML `expiracao`, `maxPorPassada`, `tempoPorPassada` e `Config.Expiracao`), `internal/server/server.go:765-784`, `README.md:156-158`, `docs/architecture.md:1470`, `docs/diagrams/expiracao_agenda.puml:129`; `internal/protocol/*.go` (tag `json:"detalhes,omitempty"`), `clients/python/src/nishidb/documentos.py:134,149`, `clients/python/tests/conexao_test.py:238,248`, `scripts/nishidb-stats.py:265-266`; `internal/metrics/names.go:282,341,365,376,403,412,430-439`, `docs/metricas.md:144-149,169`, `README.md:126-127,763-783`, `docs/architecture.md:426,2032-2065`, `internal/server/metrics_index.go`, `internal/storage/indices_rebuild.go`, `internal/storage/indices_enum.go` (comentários que citam as séries: passada de prosa); `internal/cli/server.go:84` (`texto` → `text`) e o README onde documenta `--log-format`; `internal/storage/rowindex/schedule.go:158` (`vencimentos.idx` → `expiries.idx`), `internal/storage/store.go:1131` (`prazos.idx` → `ttls.idx`), `internal/storage/counter/counter.go:62-63` (`grupos.dict` → `groups.dict`, `grupos.idx` → `groups.idx`), `docs/architecture.md` (mapa de arquivos do dataset); `internal/types/types.go:188` (`"ausente"` → `"absent"`), `internal/server/mutations.go:146` (`"nenhum"` → `"none"`), `internal/cli/operation.go:269` (`"corrente"` → `"current"`); `scripts/it-comum.sh:118`
- Modificar: `docs/debitos.md` (item novo no fim)

- [ ] **Passo 1: TSV**

```
onde	velho	novo
yaml	storage.expiracao	storage.expiry
yaml	maxPorPassada	maxPerSweep
yaml	tempoPorPassada	timePerSweep
json	detalhes	details
metric	nishidb_compaction_segmentos_pinados	nishidb_compaction_pinned_segments
metric	nishidb_index_segmentos	nishidb_index_segments
metric	nishidb_index_reconstrucoes_total	nishidb_index_rebuilds_total
metric	nishidb_index_reconstrucao_ativa	nishidb_index_rebuild_active
metric	nishidb_index_crc_falhas_total	nishidb_index_crc_failures_total
metric	nishidb_index_segmentos_consultados_total	nishidb_index_segments_scanned_total
label	estado=selado	state=sealed
label	estado=corrente	state=current
label	estado=pendente	state=pending
label	estado=fora	state=outside
label	modo=indice	mode=index
label	modo=enumeracao	mode=enumeration
cli	--log-format texto	--log-format text
disk	grupos.dict	groups.dict
disk	grupos.idx	groups.idx
disk	prazos.idx	ttls.idx
disk	vencimentos.idx	expiries.idx
wire	ausente	absent
wire	nenhum	none
cli	corrente	current
```

- [ ] **Passo 2: Um commit por linha `onde`**

Para cada bloco (`yaml`, `json`, `metric`+`label`, `cli`, `disk`, `wire`),
com testes vermelhos ANTES da mudança:

1. Escrever ou ajustar o teste que afirma o nome NOVO (ex.: em
   `internal/config/config_internal_test.go`, um YAML com `storage.expiry.maxPerSweep: 5` é lido; com
   `storage.expiracao` a subida recusa com WARN de chave sem motor — o
   comportamento "config recusa na subida" do CLAUDE.md).
2. Rodar, ver vermelho.
3. Fazer a mudança no código, no consumidor (client Python para
   `details`; `nishidb-stats.py` para `details`; `it-comum.sh` para
   `invalid`) e no doc que cita.
4. Rodar `go test` do pacote, `go test ./internal/gates/` (o gate de
   métricas cobra o `docs/metricas.md`), e, para `json`, `make test-python`.
5. Commit: "config: chaves de expiração em inglês", "protocol: campo
   details", "metrics: séries e rótulos do índice por segmento em inglês",
   "cli: --log-format text", "storage: nomes dos arquivos de índice em
   inglês", "protocol, cli: valores enumerados em inglês".

Os rótulos `estado`/`modo` viram `state`/`mode` no NOME do rótulo também
(spec §2 item 5); `docs/metricas.md` e os dashboards do piloto citam os
dois.

- [ ] **Passo 3: Débito do dataset**

Acrescentar ao fim de `docs/debitos.md`:

```
| <N+1> | **Dataset do piloto recriado pelo rename dos arquivos de índice** | `grupos.dict`, `grupos.idx`, `prazos.idx` e `vencimentos.idx` mudaram de nome sem compat (identificadores em inglês, Fase 3, <data>). Dataset anterior a essa release não abre; o piloto foi recriado do offset zero. Não há migração e não haverá: tipos e índices são imutáveis por premissa (CLAUDE.md), e nome de arquivo entra na mesma premissa. | aberto até o deploy da Fase 6 |
```

O `<N+1>` é o próximo número livre; a numeração é estável, nada é
renumerado.

- [ ] **Passo 4: Fechamento**

`make test`, `make test-race`, `make lint` (baseline igualada), `make
test-python`, `make test-jdbc-unit`. PR "Identificadores em inglês — Fase
3: emissões e contratos" → `main`.

---

## Fase 4 — Testes

### Tarefa 4.0: Workflow de testes

Cópia de `$SCRATCH/ciclo-de-grupo.js` em `$SCRATCH/ciclo-de-testes.js`
com `fase: 4` e estas diferenças:

- **Tradutor** (por pacote): "Liste toda função `Test*`, `Benchmark*`,
  `Fuzz*` do pacote (inventário, especie `func`) e todo `t.Run("...")`.
  Proponha o nome novo preservando a FRASE — sujeito, ação, condição —
  como o spec §6 manda: `TestTopNReaproveitaOBufferDaEntradaDespejada` →
  `TestTopNReusesTheBufferOfTheEvictedEntry`. Subtestes em minúsculas, na
  mesma regra. Duas funções do pacote não podem virar o mesmo nome." Saída:
  `planning/identificadores-em-ingles/mapeamento-testes-<pacote>.tsv`
  (`pacote`, `velho`, `novo`) e `subtestes-<pacote>.tsv` (`arquivo`,
  `velho`, `novo`).
- **Aplicador**: por pacote, um script `rf` com `mv TestVelho TestNovo` por
  função (função de teste é declaração de topo; se o `rf` recusar um
  arquivo `package x_test`, cair para `gopls rename -w` por posição);
  depois `renamecite -tests <todos os mapeamento-testes-*.tsv> -subtests
  <todos os subtestes-*.tsv>` sobre o repositório INTEIRO — as citações em
  `docs/`, `planning/`, `README.md`, `CLAUDE.md`, `.puml` e comentários
  mudam neste passo, e é por isso que a Fase 4 aplica os pacotes todos
  antes da passada de prosa, num único commit "Testes: nomes em inglês e
  citações atualizadas" por grupo.
- **Refutadores**: lentes `frase` (o nome novo perdeu a condição ou o
  sujeito), `convencao` (`TestXxx` com iniciais certas, sem `_`), `citação`
  (rode `go test ./internal/gates/ ./internal/cli/ -run
  'Citacoes|Evidencias' -count=1` e `grep -rn` de cinco nomes velhos ao
  acaso fora de `planning/identificadores-em-ingles/`: tudo vazio).

Rodar por grupo G1→G6 com merge por grupo.

### Tarefa 4.1: O gate de citações cobre o repositório inteiro

**Arquivos:**
- Modificar: `internal/gates/citations_test.go` (era `citacoes_test.go`)

- [ ] **Passo 1: Plantar uma citação morta e ver que o gate NÃO a vê**

Em `internal/kafka/queue.go`, num comentário qualquer, escrever
`// Ver TestQueEuInventeiAgora.`. Rodar
`go test ./internal/gates/ -run TestAsCitacoesDeTesteNosComentariosExistem -count=1`:
PASS — o arquivo está fora de `arquivosDoIndicePorSegmento`. É a lacuna
que esta tarefa fecha.

- [ ] **Passo 2: Remover o escopo**

Em `citations_test.go`: apagar `arquivosDoIndicePorSegmento`, o godoc "# O
ESCOPO É UMA LISTA, E ISSO É UM LIMITE, NÃO UM DESENHO" (substituir por um
parágrafo dizendo que a lista foi removida no ciclo de identificadores em
inglês, quando toda citação do repositório passou pelo mapeamento e as
mortas herdadas foram corrigidas), o mapa `noEscopo` e o `if
!noEscopo[caminho]`, e o laço `for f := range noEscopo`. A varredura
continua pulando `.git`, `.claude` e `clients`. O `t.Fatalf` de vacuidade
passa a exigir `len(citadas) > 100` (o repositório tem 667 citações).

- [ ] **Passo 3: Ver vermelho com a citação plantada**

Run: `go test ./internal/gates/ -run TestAsCitacoesDeTesteNosComentariosExistem -count=1`
Esperado: FAIL citando `internal/kafka/queue.go` e `TestQueEuInventeiAgora`
— e, junto, toda citação morta herdada que a Fase 4 não tiver pego. Cada
uma é corrigida no comentário (apontar para o teste que existe e prova o
fato, ou remover a afirmação de cobertura) — sem inventar nome.

- [ ] **Passo 4: Remover a plantada, ver verde, commit**

```bash
git add internal/gates/citations_test.go internal/kafka/queue.go
git commit -m "gates: o gate de citações de teste em comentários cobre o repositório inteiro"
```

Mais um commit por pacote cujas citações herdadas foram corrigidas.

### Tarefa 4.2: Fechamento da Fase 4

- [ ] `make test`, `make test-race`, `make test-integration`, `make lint` com
  baseline igualada; `grep -rnE '\bTest[A-Z][A-Za-z]*(Ao|Cao|Nao|Que|Com|Sem|Da|Do|De)[A-Z]' --include='*.go' --include='*.md' . | grep -v 'identificadores-em-ingles'` devolve vazio (heurística: partículas PT-BR
  dentro de nome de teste; não prova, mas acusa).
- [ ] PR "Identificadores em inglês — Fase 4: testes" → `main`.

---

## Fase 5 — Clients

### Tarefa 5.1: JDBC

**Arquivos:**
- Criar: `planning/identificadores-em-ingles/mapeamento-clients.tsv`
- Mover: `clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/wire/{Codigos→Codes, LeitorNdjson→NdjsonReader, Molduras→Frames, Temporais→Temporals, TiposJdbc→JdbcTypes, Transporte→Transport}.java`, `jdbc/json/JsonTipo→JsonKind.java`, e os testes `*Test.java` correspondentes (`LeitorNdjsonTest`, `TemporaisTest`, `TiposJdbcTest`, `TransporteTest`, `AssinaturasTest→SignaturesTest`, `IntegracaoObrigatoriaTest→RequiredIntegrationTest`)
- Modificar: `internal/gates/jdbc_test.go` (caminho `wire/TiposJdbc.java` → `wire/JdbcTypes.java`), `internal/gates/verification_test.go` se citar classes, `docs/aceite-*.md` que citem (passada de prosa com `-idents` de um TSV `pacote=clients/jdbc`)

- [ ] **Passo 1: TSV** com as linhas acima (`pacote`, `especie=type`, `endereco`, `novo`, `motivo`) mais os métodos e campos PT-BR que um agente cartógrafo listar lendo os `.java` (não há inventário `go/types` aqui; o agente lista `class|interface|record|enum` e `public|private ... nome(`).
- [ ] **Passo 2: Aplicar** com um script no scratchpad: `git mv` de cada arquivo, depois `sed -i 's/\bVelho\b/Novo/g'` sobre `clients/jdbc/src/**/*.java` para cada classe (nomes de classe Java são distintos o bastante para fronteira de palavra ser segura; conferir com `git diff` que nenhum literal de string mudou por engano — `"Transporte"` em mensagem de log é texto e entra na mesma regra das emissões).
- [ ] **Passo 3: Verificar** — `make build-jdbc`, `make test-jdbc-unit`, `make test-jdbc` (IT real, exige JDK 21), `go test ./internal/gates/ -run JDBC -count=1`.
- [ ] **Passo 4: Commit** "jdbc: classes internas em inglês".

### Tarefa 5.2: Python

**Arquivos:**
- Mover: `clients/python/src/nishidb/{conexao→connection, documentos→documents, erros→errors, cliente→client, linha→row, leitor→reader, tipos→types, transporte→transport}.py` e os testes `clients/python/tests/*_test.py` correspondentes
- Modificar: `clients/python/src/nishidb/__init__.py` (imports e `__all__`: `ColunaMeta→ColumnMeta`, `Estatisticas→Stats`, `ConsultaEmVoo→InFlightQuery`, `Saude→Health`, `ENV_SENHA→ENV_PASSWORD`), classes internas (`Abertura`, `Capacidades`, `Coercoes`, `ConstrutoresDaPep`, ...), `clients/python/README.md`, `docs/aceite-client-python.md` (citações), `scripts/python-it.sh` se citar módulo, `internal/gates/python_test.go` (lê `erros.py`? conferir o caminho parseado e trocar)

- [ ] **Passo 1: TSV** — um agente cartógrafo lista módulos, classes, funções e nomes públicos (`__all__`) em PT-BR; o tradutor propõe; um refutador com a lente `api pública` confere que `__all__` continua exportando os nomes PEP 249 intocados (`connect`, `Connection`, `Cursor`, `Error`, ...).
- [ ] **Passo 2: Aplicar** — `git mv` dos módulos; `sed -i` com fronteira de palavra sobre `clients/python/src` e `clients/python/tests` para `from .conexao import` → `from .connection import` e para cada classe; `test_xxx` renomeados pelo mesmo mapeamento e citados em `docs/aceite-client-python.md` pela passada de prosa (`-tests`).
- [ ] **Passo 3: Verificar** — `make lint-python`, `make test-python`, `make test-python-it`, `go test ./internal/gates/ -run Python -count=1`, `go test ./internal/cli/ -run Evidencias -count=1`.
- [ ] **Passo 4: Commit** "python: módulos, classes e nomes públicos em inglês, sem alias".

### Tarefa 5.3: Fechamento da Fase 5

- [ ] `make test`, `make test-race`, `make lint`; PR "Identificadores em inglês — Fase 5: clients" → `main`.

---

## Fase 6 — Fechamento

### Tarefa 6.1: Docs e diagramas

- [ ] `docs/architecture.md`: rodar `renamecite` com TODOS os TSV do ciclo (`-idents` de cada `mapeamento-2-*.tsv`, `-paths` dos dois de caminhos, `-tests`, `-logkeys`) sobre o repositório inteiro uma última vez — é idempotente — e ler o relatório: toda linha "ambíguo" é resolvida à mão qualificando a citação.
- [ ] `docs/diagrams/*.puml`: um agente lê cada diagrama e troca participantes e notas que citam identificador (`Store.Apenda` → `Store.Append`), deixando a prosa; validar cada um pelo servidor `uml.nishisan.dev` (a memória do projeto diz como: encoder deflate+base64 e uma linha que não é sintaxe para provar que o validador discrimina).
- [ ] `docs/doutrina-de-verificacao.md`: parágrafo novo com a classe de defeito que este ciclo fechou ("citação morta em comentário fora do escopo") e o gate que a cobre.
- [ ] Commit "docs: architecture, diagramas e doutrina após os identificadores em inglês".

### Tarefa 6.2: Inventário final e lint

- [ ] `go run ./scripts/renameinv -dir . -out planning/identificadores-em-ingles/inventario-final.tsv -groups planning/identificadores-em-ingles/grupos-final.tsv`; um agente crítico compara com o inicial: TODA linha do inicial tem, em algum `mapeamento-*.tsv`, uma entrada com `novo` ou um `mantido: já em inglês` / `mantido: receptor curto`. Zero faltas.
- [ ] `make lint`; `.golangci-baseline` igualada; commit "lint: linha de base final do ciclo de identificadores em inglês".

### Tarefa 6.3: Aceite

- [ ] Escrever `docs/aceite-identificadores-em-ingles.md` no molde de `docs/aceite-contadores.md`: tabela de números (declarações no inventário inicial, renomeadas, mantidas por já serem inglês, testes renomeados, citações reescritas por `renamecite` por fase, mensagens e chaves de log, contratos), a lista de TODAS as mutações do ciclo com o vermelho e o verde relatados (Tarefas 0.1, 0.3, 0.4, 0.5, 0.6, 4.1 e as da Fase 3), tempo por fase, o que ficou de fora e por quê, e os débitos abertos.
- [ ] Adicionar o aceite à lista `docs` de `TestEvidenciasCitadasNosDocumentosExistem` em `internal/cli/evidence_test.go`.
- [ ] `git add docs/aceite-identificadores-em-ingles.md internal/cli/evidence_test.go` e commit "docs: aceite do ciclo de identificadores em inglês".

### Tarefa 6.4: Deploy no piloto

- [ ] Release por `gh release create` ANTES da tag, com o changelog dizendo: nomes de config novos (`storage.expiry.maxPerSweep`, `timePerSweep`), campo `details`, séries e rótulos de métrica novos, `--log-format text`, dataset recriado.
- [ ] No host do piloto: parar o serviço, mover o dataset antigo para fora (ver a memória do projeto: dados descartáveis), atualizar `current/nishidb.yaml` para as chaves novas, subir a release, confirmar por `/v1/metadata` e `/metrics` (as séries `nishidb_index_segments{state=...}` aparecem, as velhas não), atualizar os dashboards.
- [ ] Fechar o débito da Tarefa 3.1 em `docs/debitos.md` com a data do deploy; commit "docs: fecha o débito do dataset recriado".

---

## Auto-revisão do plano

- **Cobertura do spec:** §1 norma → 0.1; §2 escopo itens 1–7 → Fases 2, 1, 4, 3, 3.1, 5, e a passada de prosa em todas; §4.1 `rf` → 0.6 e 2.0; §4.2 ferramenta própria → 0.4; §4.3 pacotes → 1.1; §4.4 prosa → 0.5; §4.5 emissões → 3.0; §4.6 ordem → 2.0 passo "Aplicar"; §5 artefatos → TSV em cada tarefa, aceite em 6.3; §6 regras → `regras` no workflow e glossário em 0.1; §7 fases → seções; §8 mutações → 0.1, 0.3, 0.4, 0.5, 0.6, 4.1, 3.1; inventário como prova → 0.3, 2.7, 6.2; §9 papéis → 2.0; §10 riscos → merge por fase e rebase em cada fechamento, `-diff` guardado pelo aplicador, baseline por fase, piloto em 6.4.
- **Placeholders:** nenhum `TBD`; os únicos "um agente lista/propõe" são as Fases 3–5, cujo conteúdo é produzido em execução e cujo FORMATO (TSV, colunas, lentes, comandos de verificação) está fixado aqui.
- **Consistência de nomes:** `LoadModule`, `InterfaceGroups`, `Groups`, `Member`, `posKey`, `modulePackages`, `packageDir`, `moduleRoot` são os mesmos em 0.3 e 0.4 (cópia com teste de paridade); `Apply`/`Entry`/`Report` em 0.4; `Mapping`/`RewriteText`/`RewriteGoComments`/`RewriteGoFile`/`Skipped` em 0.5; `rename-apply.sh <dir> <script> <diff>` em 0.6 e 2.0; nomes de arquivo pós-Fase 1 (`citations_test.go`, `names.go`, `queue.go`, `evidence_test.go`, `metrics_index.go`, `operation.go`, `mutations.go`, `schedule.go`, `counter.go`) usados nas Fases 3–6.
