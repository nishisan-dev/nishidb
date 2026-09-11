# Schema declarado e catálogo JDBC — plano de implementação

> **Para quem executa:** use `superpowers:subagent-driven-development` ou
> `superpowers:executing-plans` para tocar tarefa a tarefa. Os passos são
> checkbox (`- [ ]`).

**Goal:** o DBeaver mostra os contadores e as tabelas de meta na árvore, e o
SQL que ele gera sozinho continua válido — porque cada dataset declara o schema
em que tudo dele vive.

**Architecture:** o YAML do dataset ganha `schemaName` obrigatório. O `Engine`
ganha UMA função de catálogo, com dois consumidores: o `/v1/metadata` e a tabela
virtual `_tables`. O driver JDBC passa a ler as tabelas virtuais do mesmo
documento que já lê hoje. A gramática de dois níveis não muda: o schema ocupa o
primeiro nível, e `_counters.x` / `_meta.x` continuam como abreviação do dataset
da requisição.

**Tech Stack:** Go 1.25+ (CI pina 1.26.5), `gopkg.in/yaml.v3`, JDK 21+ para o
driver, `golangci-lint` v2.12.2.

**Spec:** [`planning/schema-declarado-e-catalogo-jdbc.md`](schema-declarado-e-catalogo-jdbc.md)

## Global Constraints

- **Prosa em PT-BR, identificador em inglês.** Nome de teste em PT-BR
  (sujeito-ação-condição). Mensagem de erro e de log, chave de config, campo do
  wire e nome de métrica em inglês.
- **Quebrar contrato é permitido e tem de ser REGISTRADO** — não há release
  publicada. Nunca escrever fallback para o formato velho.
- **Config recusa na subida.** Valor fora de faixa é recusado, não arredondado;
  a mensagem nomeia a chave e mostra um exemplo.
- **Zero alocação por linha** no scan e nas tabelas virtuais é propriedade
  gateada — o texto de cada linha é pré-calculado na montagem.
- **Doutrina de verificação:** para cada invariante, aplique a mutação, veja
  vermelho, reverta, veja verde, e **relate os dois**.
- **Identificador PT-BR que a tarefa TOCAR é renomeado no mesmo PR**, e o par
  velho → novo entra em
  `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`.
- Antes de commitar: `make test` E `make test-race` (nenhum substitui o outro) e
  `make lint` **igual** à baseline — ela quebra nos dois sentidos.
- Excluir `.claude/worktrees/` de qualquer grep.

**Base:** este ciclo começa DEPOIS de a issue #38 entrar em `main`. As duas
tocam `internal/schema/counter.go` e `internal/counters/`, e rebasear é mais
barato que resolver conflito no fim.

---

### Task 1: Spike — o DBeaver monta a árvore como o desenho pressupõe?

O desenho inteiro depende de software de terceiro fazer duas coisas: montar a
árvore a partir de `getSchemas` + `getTables`, e gerar `SELECT * FROM
schema.tabela`. Isso é pressuposto, não fato. **Descobrir isso no fim custaria o
ciclo inteiro.**

Saída: uma resposta, não código que fica. O que for construído aqui é
descartável e tem de ser rotulado assim.

**Files:**
- Create (descartável): `/tmp/.../FakeMetaData.java`

- [ ] **Step 1: subclasse de `NishiDatabaseMetaData` com resposta fixa**

Estender a classe do driver sobrescrevendo três métodos, sem tocar no driver:

```java
// DESCARTÁVEL — spike da Task 1, não entra no repositório.
getSchemas()    -> uma linha: TABLE_SCHEM="telecom", TABLE_CATALOG=null
getTableTypes() -> TABLE, VIEW, SYSTEM TABLE
getTables()     -> telecom/service_view/TABLE,
                   telecom/autenticados_por_slot/VIEW,
                   telecom/_fields/SYSTEM TABLE
```

- [ ] **Step 2: registrar o driver falso e abrir no DBeaver**

Apontar para o piloto (`10.200.20.218:9143`, `tls=true`, `user=reader`).

- [ ] **Step 3: responder as três perguntas, por escrito**

1. A árvore mostra `telecom` como schema, com as três tabelas dentro?
2. O "View Data" gera `SELECT ... FROM telecom.autenticados_por_slot`?
3. As `SYSTEM TABLE` vão para uma pasta separada?

- [ ] **Step 4: registrar o achado no fim da spec, em "Riscos"**

Se a resposta 2 for não — se o DBeaver gerar só `autenticados_por_slot`, sem
qualificar —, **PARE e reabra o desenho**: sem a qualificação, o SQL gerado cai
no dataset default e o ganho principal evapora.

---

### Task 2: `schemaName` na spec do dataset

**Files:**
- Modify: `internal/schema/spec.go` (o `rawSpec`, o `DatasetSpec` e a validação
  de `Name`, hoje em `spec.go:682`)
- Test: `internal/schema/spec_test.go`

**Interfaces:**
- Produces: `DatasetSpec.SchemaName string` — sempre não vazio depois de
  `Parse`; `[a-z0-9_]+`, nunca com prefixo `_`.

- [ ] **Step 1: escrever os testes que falham**

```go
// TestSpecExigeSchemaName: o schema é o container de tudo que o dataset
// publica, e sem ele a árvore do cliente não tem onde pendurar as tabelas.
// Adivinhar um default reintroduziria a schema nula que esta mudança veio
// tirar — então recusa, com a chave e um exemplo na mensagem.
func TestSpecExigeSchemaName(t *testing.T) {
	_, err := Parse([]byte("version: 1\nname: servicos\n"+materializationMinima), BuiltinDefaults)
	if err == nil {
		t.Fatal("spec sem schemaName tem de ser recusada")
	}
	for _, quero := range []string{"schemaName", "exemplo"} {
		if !strings.Contains(err.Error(), quero) {
			t.Errorf("a mensagem não traz %q — recusa que não ensina a correção só transfere o trabalho: %v", quero, err)
		}
	}
}

func TestSpecRecusaSchemaNameInvalido(t *testing.T) {
	for nome, valor := range map[string]string{
		"prefixo reservado": "_meta",
		"maiuscula":         "Telecom",
		"com ponto":         "tele.com",
		"vazio":             `""`,
	} {
		t.Run(nome, func(t *testing.T) {
			_, err := Parse([]byte("version: 1\nname: servicos\nschemaName: "+valor+"\n"+materializationMinima), BuiltinDefaults)
			if err == nil {
				t.Fatalf("schemaName %s tem de ser recusado", valor)
			}
		})
	}
}

func TestSpecAceitaSchemaName(t *testing.T) {
	spec, err := Parse([]byte("version: 1\nname: servicos\nschemaName: telecom\n"+materializationMinima), BuiltinDefaults)
	if err != nil {
		t.Fatal(err)
	}
	if spec.SchemaName != "telecom" {
		t.Fatalf("SchemaName = %q", spec.SchemaName)
	}
}
```

`materializationMinima` é a constante que os testes do pacote já usam para o
bloco obrigatório; se não existir com esse nome, reaproveitar a fixture vizinha
em vez de inventar outra.

- [ ] **Step 2: rodar e ver falhar**

`go test ./internal/schema/ -run 'SchemaName' -count=1`
Esperado: FAIL — `SchemaName` não existe.

- [ ] **Step 3: implementar**

Em `rawSpec`, ao lado de `Name`:

```go
SchemaName string `yaml:"schemaName"`
```

Em `DatasetSpec`, ao lado de `Name`:

```go
// SchemaName é o schema em que TODAS as tabelas deste dataset vivem — a
// base, os contadores e as de meta. Obrigatório: um default faria
// `servicos.servicos`, e a ausência faria schema nula; as duas foram
// recusadas no desenho.
SchemaName string
```

Na validação, logo depois da recusa do prefixo `_` em `Name` (`spec.go:682`):

```go
if raw.SchemaName == "" {
	return nil, specErrf(
		"schemaName obrigatório: é o schema em que as tabelas deste dataset aparecem para o cliente SQL (exemplo: `schemaName: telecom`)")
}
if !isIdentName(raw.SchemaName) {
	return nil, specErrf("schemaName %q inválido (esperado [a-z0-9_]+)", raw.SchemaName)
}
if strings.HasPrefix(raw.SchemaName, "_") {
	return nil, specErrf(
		"schemaName %q: o prefixo \"_\" é reservado aos espaços de nome do produto (_meta, %s); escolha outro nome",
		raw.SchemaName, CountersNamespace)
}
s.SchemaName = raw.SchemaName
```

- [ ] **Step 4: rodar e ver passar**

`go test ./internal/schema/ -count=1`

Toda fixture YAML do repositório que não declare `schemaName` vai falhar agora.
**Isso é esperado e é o ponto.** Corrigir cada uma acrescentando a linha —
`grep -rl "^name:" --include=*.yaml . | grep -v .claude/worktrees` mais as
specs embutidas em `_test.go` e em `testdata/`.

- [ ] **Step 5: mutação**

Trocar a recusa por um default (`s.SchemaName = raw.Name` quando vazio).
`TestSpecExigeSchemaName` tem de ficar vermelho. Reverter.

- [ ] **Step 6: commit**

```bash
git add internal/schema/ testdata/ configs/
git commit -m "schema: o dataset declara o schemaName, e a subida recusa sem ele"
```

---

### Task 3: o schema é único no servidor

Dois datasets no mesmo schema seriam duas árvores misturadas, e o
`FROM telecom.x` deixaria de identificar um dataset.

**Files:**
- Modify: o ponto que carrega os datasets do diretório (`internal/server` e
  `internal/cli` passam pelo mesmo carregador — localizar com
  `grep -rn "ListDatasets" --include=*.go internal/ | grep -v _test`)
- Test: no pacote do carregador

**Interfaces:**
- Consumes: `DatasetSpec.SchemaName` (Task 2)

- [ ] **Step 1: teste que falha**

```go
// TestDoisDatasetsNoMesmoSchemaSaoRecusados: o schema é o que identifica o
// dataset num `FROM telecom.tabela`. Repetido, a mesma consulta responderia de
// um dataset ou de outro conforme a ordem de carga — e a ordem de carga não é
// contrato.
func TestDoisDatasetsNoMesmoSchemaSaoRecusados(t *testing.T) {
	// dois YAML no diretório, ambos com `schemaName: telecom`
	err := carrega(t, dir)
	if err == nil {
		t.Fatal("schema repetido entre datasets tem de ser recusado")
	}
	for _, quero := range []string{"telecom", "servicos", "eventos"} {
		if !strings.Contains(err.Error(), quero) {
			t.Errorf("a mensagem precisa nomear o schema E os DOIS datasets: %v", err)
		}
	}
}
```

- [ ] **Step 2: rodar e ver falhar**
- [ ] **Step 3: implementar** — mapa `schema → nome do dataset` durante a carga,
      recusando na segunda ocorrência com os dois nomes na mensagem.
- [ ] **Step 4: rodar e ver passar**
- [ ] **Step 5: mutação** — trocar a recusa por "o último vence"; o teste fica
      vermelho. Reverter.
- [ ] **Step 6: commit**

---

### Task 4: o nome do contador não pode colidir dentro do schema

Duas recusas, e as duas fecham a colisão por construção em vez de detectá-la
tarde.

**Files:**
- Modify: `internal/schema/counter.go` (`validateCounters`, a validação de
  `r.Name` por volta da linha 166)
- Test: `internal/schema/counter_test.go`

- [ ] **Step 1: testes que falham**

```go
// TestCountersRecusaPrefixoReservado: dentro do schema declarado, a tabela do
// contador divide o espaço de nomes com `_fields`, `_counters` e `_kafka`.
// Reservar o `_` para o motor — como o nome do DATASET já reserva — torna a
// colisão irrepresentável, em vez de algo a conferir.
func TestCountersRecusaPrefixoReservado(t *testing.T) {
	_, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: _fields
    groupBy: [olt]
`)), BuiltinDefaults)
	if err == nil || !strings.Contains(err.Error(), "_") {
		t.Fatalf("contador com prefixo reservado tem de ser recusado: %v", err)
	}
}

// TestCountersRecusaNomeDoProprioDataset: é a ÚNICA colisão que o prefixo não
// fecha — `telecom.servicos` seria a tabela base e o contador ao mesmo tempo.
func TestCountersRecusaNomeDoProprioDataset(t *testing.T) {
	_, err := Parse([]byte(yamlWithCounters(`
counters:
  - name: servicos
    groupBy: [olt]
`)), BuiltinDefaults)
	if err == nil || !strings.Contains(err.Error(), "servicos") {
		t.Fatalf("contador homônimo do dataset tem de ser recusado: %v", err)
	}
}
```

(`yamlWithCounters` já existe no arquivo e monta a spec sobre o dataset
`servicos`.)

- [ ] **Step 2: rodar e ver falhar**

- [ ] **Step 3: implementar**

`validateCounters` já recebe `key` e `seq`; passar também o nome do dataset.
Logo depois do `isIdentName(r.Name)`:

```go
if strings.HasPrefix(r.Name, "_") {
	return nil, specErrf(
		"%s.name %q: o prefixo \"_\" é reservado às tabelas do motor dentro do schema (_fields, _counters, _kafka, _tables)",
		keyPath, r.Name)
}
if r.Name == dataset {
	return nil, specErrf(
		"%s.name %q é o nome do próprio dataset: no schema declarado os dois seriam a mesma tabela", keyPath, r.Name)
}
```

- [ ] **Step 4: rodar e ver passar** — `go test ./internal/schema/ -count=1`
- [ ] **Step 5: mutação** — remover a recusa do prefixo; os dois testes têm de
      distinguir qual caiu. Reverter.
- [ ] **Step 6: commit**

---

### Task 5: o catálogo no Engine — a fonte única

**Files:**
- Create: `internal/query/catalog.go`
- Test: `internal/query/catalog_test.go`

**Interfaces:**
- Produces:

```go
// TableKind é como a tabela se apresenta a um cliente SQL.
type TableKind string

const (
	KindTable       TableKind = "TABLE"        // a tabela base do dataset
	KindView        TableKind = "VIEW"         // _counters.<nome>
	KindSystemTable TableKind = "SYSTEM TABLE" // as tabelas do _meta
)

// TableInfo descreve UMA tabela consultável.
type TableInfo struct {
	Schema      string
	Name        string
	Kind        TableKind
	Description string
	Columns     []*schema.CompiledField
}

// Catalog devolve TODA tabela consultável do dataset, em ordem estável:
// a base primeiro, depois as views e as de sistema, cada grupo por nome.
func (e *Engine) Catalog(dataset string) ([]TableInfo, error)
```

- [ ] **Step 1: teste que falha**

```go
// TestCatalogoListaBaseViewsESistema: é a fonte ÚNICA do que existe — o
// /v1/metadata e a tabela _tables saem os dois daqui. Duas derivações do mesmo
// fato divergem, e a divergência aparece como uma ferramenta que discorda do
// SQL sem nada estar quebrado.
func TestCatalogoListaBaseViewsESistema(t *testing.T) {
	eng := engineComDatasetEContadores(t) // base `servicos`, contadores por_olt e por_pon
	tabelas, err := eng.Catalog("servicos")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, tb := range tabelas {
		got = append(got, string(tb.Kind)+":"+tb.Schema+"."+tb.Name)
	}
	quero := []string{
		"TABLE:telecom.servicos",
		"VIEW:telecom.por_olt",
		"VIEW:telecom.por_pon",
		"SYSTEM TABLE:telecom._counters",
		"SYSTEM TABLE:telecom._fields",
		"SYSTEM TABLE:telecom._kafka",
		"SYSTEM TABLE:telecom._tables",
	}
	if !slices.Equal(got, quero) {
		t.Fatalf("catálogo = %v, quero %v", got, quero)
	}
}

// A ordem é do contrato: uma ferramenta que confia nela para agrupar mostraria
// a árvore embaralhada sem isto.
func TestCatalogoTemOrdemEstavel(t *testing.T) { /* duas chamadas, mesma ordem */ }

func TestCatalogoRecusaDatasetDesconhecido(t *testing.T) { /* erro nomeando o dataset */ }
```

- [ ] **Step 2: rodar e ver falhar**

- [ ] **Step 3: implementar** — varrer `e.datasets[dataset]`: o `record.sch` dá
      a base; `record.virtuais` dá as views e as de sistema. O schema vem do
      `Compiled`, que passa a carregar o `SchemaName` (Task 2 o põe na spec; a
      compilação o copia).

- [ ] **Step 4: rodar e ver passar**

- [ ] **Step 5: mutação** — devolver as views em ordem de mapa (`for k := range`);
      `TestCatalogoTemOrdemEstavel` tem de ficar vermelho **de forma
      intermitente**, o que já é motivo para o teste comparar a lista inteira e
      não só o tamanho. Reverter.

- [ ] **Step 6: commit**

---

### Task 6: `_tables`

**Files:**
- Create: `internal/query/meta/tables.go`
- Test: `internal/query/meta/tables_test.go`

**Interfaces:**
- Consumes: `Engine.Catalog` (Task 5)
- Produces: a tabela `_tables` registrada sob o dataset.

Colunas: `name` (key), `schema`, `kind`, `columns` (int64), `description`.

- [ ] **Step 1: teste que falha**

```go
// TestMetaTablesListaOCatalogo: é a resposta por SQL para "que tabelas
// existem" — o `_meta` deste produto já responde `fields`, `counters` e
// `kafka`, e faltava a que lista as tabelas.
func TestMetaTablesListaOCatalogo(t *testing.T) {
	eng := engineComDatasetEContadores(t)
	rows := consulta(t, eng, "SELECT name, schema, kind FROM _tables ORDER BY name")
	if got := column(rows, "kind"); !strings.Contains(got, "VIEW") || !strings.Contains(got, "SYSTEM TABLE") {
		t.Fatalf("kind = %s", got)
	}
	if got := column(rows, "schema"); strings.Contains(got, ",,") {
		t.Fatal("nenhuma linha pode sair com schema vazio")
	}
}

// A tabela se inclui: perguntar "que tabelas existem" e não ver a própria
// pergunta listada deixa o catálogo incompleto na primeira linha.
func TestMetaTablesSeInclui(t *testing.T) { /* `_tables` está entre as linhas */ }

func TestMetaTablesNaoAlocaPorLinha(t *testing.T) {
	// molde: TestTabelaMetaCountersNaoAlocaPorLinha. Os textos são calculados
	// UMA vez na montagem.
}
```

- [ ] **Step 2: rodar e ver falhar**
- [ ] **Step 3: implementar** (molde: `internal/counters/meta.go`)
- [ ] **Step 4: rodar e ver passar**
- [ ] **Step 5: mutação** — montar a linha com `fmt.Sprintf` por linha; o gate
      de alocação fica vermelho. Reverter.
- [ ] **Step 6: commit**

---

### Task 7: `FROM telecom.tabela` resolve, e resolve o dataset certo

**Files:**
- Modify: `internal/query/engine.go` (`Execute`, a partir de 226)
- Test: `internal/query/schema_routing_test.go`

**Interfaces:**
- Consumes: `Engine.Catalog` (Task 5)

- [ ] **Step 1: testes que falham**

```go
// TestSchemaQualificadoResolveODatasetCerto é o ganho que a mudança existe
// para entregar: hoje `FROM _counters.x` não nomeia dataset nenhum e cai no
// default da conexão — com dois datasets no servidor, os contadores de um
// deles são inalcançáveis daquela conexão.
func TestSchemaQualificadoResolveODatasetCerto(t *testing.T) {
	eng := engineComDoisDatasets(t) // telecom/servicos e rede/eventos
	rows := consulta(t, eng, "rede", "SELECT name FROM telecom._counters")
	if len(rows) == 0 {
		t.Fatal("o schema qualificado tem de alcançar o outro dataset")
	}
}

// O atalho continua valendo, e continua significando "o dataset da requisição".
func TestAtalhoSemSchemaUsaODatasetDaRequisicao(t *testing.T) { /* `_counters.x` */ }

// Schema que não existe tem de LISTAR os que existem — um "não encontrado" sem
// a lista manda o operador adivinhar.
func TestSchemaDesconhecidoListaOsExistentes(t *testing.T) { /* mensagem com telecom e rede */ }
```

- [ ] **Step 2: rodar e ver falhar**
- [ ] **Step 3: implementar** — antes da comparação dataset/FROM, resolver o
      `stmt.Namespace` contra os schemas declarados; casando, o dataset da
      consulta passa a ser o dono daquele schema.
- [ ] **Step 4: rodar e ver passar**
- [ ] **Step 5: mutação** — resolver o schema DEPOIS da comparação; o primeiro
      teste fica vermelho. Reverter.
- [ ] **Step 6: commit**

---

### Task 8: o protocolo publica o schema e as tabelas

**Files:**
- Modify: `internal/protocol/documents.go` (`DatasetMeta`),
  `internal/server/documents.go` (`metaOfDataset`)
- Test: `internal/server/documents_test.go`

**Interfaces:**
- Consumes: `Engine.Catalog` (Task 5)
- Produces:

```go
// Schema é o schema declarado do dataset. O campo é ADITIVO.
Schema string `json:"schema"`
// Tables são TODAS as tabelas consultáveis deste dataset, a base inclusive.
// Aditivo: um cliente que não o conhece continua lendo Columns como antes.
Tables []TableMeta `json:"tables"`

type TableMeta struct {
	Name        string       `json:"name"`
	Kind        string       `json:"kind"`
	Description string       `json:"description"`
	Columns     []ColumnMeta `json:"columns"`
}
```

- [ ] **Step 1: teste que falha** — `/v1/metadata` traz `schema` e as tabelas,
      cada view com as colunas e o código `jdbc` preenchido.
- [ ] **Step 2: rodar e ver falhar**
- [ ] **Step 3: implementar** — `metaOfDataset` passa a chamar `Catalog`.
      **Não** recalcular tipo JDBC aqui: reusar o `columnMeta` que já existe.
- [ ] **Step 4: rodar e ver passar**
- [ ] **Step 5: ler os gates de paridade dos clients** (`internal/gates`) e
      rodá-los. Se algum cobrar o Python, resolver ANTES de seguir.
- [ ] **Step 6: mutação** — montar `Tables` direto do `record.virtuais` em vez
      do `Catalog`; um teste tem de acusar a divergência com `_tables`.
      Reverter.
- [ ] **Step 7: commit**

---

### Task 9: o driver JDBC mostra a árvore

**Files:**
- Modify:
  `clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiDatabaseMetaData.java`
- Test: `clients/jdbc/src/test/java/.../NishiDatabaseMetaDataTest.java`

**Interfaces:**
- Consumes: `schema` e `tables` do `/v1/metadata` (Task 8)

- [ ] **Step 1: testes que falham** (contra o servidor de mentira que os testes
      do driver já sobem)

```java
// getTables traz a base como TABLE, os contadores como VIEW e as de meta como
// SYSTEM TABLE, todas com TABLE_SCHEM preenchido — é o TABLE_SCHEM que faz o
// DBeaver gerar `FROM telecom.x`, que é SQL que o motor aceita.
@Test void listaBaseViewsESistemaComSchema() { }

// Filtrar por tipo tem de funcionar: o DBeaver pede só VIEW ao abrir a pasta.
@Test void filtraPorTipoDeTabela() { }

// getColumns cobre as virtuais pelo mesmo documento.
@Test void colunasDaViewSaemComTipoJdbc() { }

// Servidor sem o campo `tables` (binário anterior): nenhuma view, nenhum erro.
@Test void metadataSemTablesDegradaSemErro() { }
```

- [ ] **Step 2: rodar e ver falhar** — `make test-jdbc-unit`
- [ ] **Step 3: implementar** `getSchemas`, `getTableTypes`, `getTables`,
      `getColumns`. Manter a ordenação do contrato (TABLE_TYPE, TABLE_CAT,
      TABLE_SCHEM, TABLE_NAME) — o `getTables` de hoje já a comenta.
- [ ] **Step 4: rodar e ver passar** — `make test-jdbc` (o IT roda contra o
      binário real)
- [ ] **Step 5: abrir no DBeaver de verdade** e repetir as três perguntas da
      Task 1, agora com o driver real. **É este passo que aceita a tarefa**, não
      o teste de unidade.
- [ ] **Step 6: mutação** — devolver `TABLE_SCHEM` nulo nas views; o teste da
      árvore fica vermelho e o SQL gerado pelo DBeaver passa a cair no dataset
      default. Reverter.
- [ ] **Step 7: commit**

---

### Task 10: documentação, aceite e o piloto

**Files:**
- Modify: `README.md`, `docs/architecture.md`, `docs/glossario.md`,
  `internal/cli/evidence_test.go` (a lista de documentos do gate)
- Create: `docs/aceite-schema-declarado.md`
- Modify: `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`

- [ ] **Step 1: README** — `schemaName` no YAML do dataset, a árvore que sai
      disso, e que `_counters.x` é abreviação do dataset da requisição.
- [ ] **Step 2: arquitetura** — o catálogo como fonte única, e o porquê de a
      gramática de dois níveis decidir o mapeamento JDBC.
- [ ] **Step 3: glossário** — catálogo ↔ catalog, tabela de sistema ↔ system
      table.
- [ ] **Step 4: aceite** com a bancada e as mutações desta lista, **e o
      resultado do DBeaver**, que é a evidência que nenhum teste Go dá.
- [ ] **Step 5: acrescentar o aceite à lista de `TestEvidenciasCitadasNosDocumentosExistem`**
      e provar que ele cobra — inverter um nome de teste, ver falhar, reverter.
- [ ] **Step 6: renomear os identificadores PT-BR tocados** e registrar os pares
      no TSV.
- [ ] **Step 7: `make test`, `make test-race`, `make lint` na baseline**
- [ ] **Step 8: piloto** — acrescentar `schemaName` ao `service_view.yaml`
      (senão ele não sobe), subir a release e conferir a árvore pelo DBeaver
      contra `10.200.20.218:9143`.
- [ ] **Step 9: commit**

---

## Auto-revisão do plano

**Cobertura da spec:** as seis peças do desenho (config obrigatória, unicidade,
colisão por construção, catálogo como fonte única, `_tables`, protocolo aditivo,
driver) têm tarefa. O risco de ferramenta virou a Task 1, antes de tudo, como a
spec exige. O risco de config está coberto na Task 2 (fixtures) e na Task 10
(piloto).

**Sem placeholder:** os testes têm código; onde escrevi molde (`_tables`,
paridade Python) apontei o arquivo a copiar em vez de descrever.

**Consistência de tipos:** `TableKind`/`TableInfo`/`Catalog` (Task 5) são
consumidos com os mesmos nomes nas Tasks 6, 7 e 8; `TableMeta` (Task 8) é o que
a Task 9 lê.

**Lacuna conhecida, dita:** a Task 3 depende de achar o carregador de datasets,
que não localizei ao escrever — o passo traz o `grep` que o encontra em vez de
chutar o caminho.
