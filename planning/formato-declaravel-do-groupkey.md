# Issue #38 — formato declarável do `groupKey` do contador

## Contexto

Hoje a chave textual de um grupo é fixa: as colunas do `groupBy` na ordem declarada,
separadas por `|`, com `\` e `|` escapados (`PRCTA_G1M10|1|9`). Quem consome
`_counters.<nome>` — o laço de alarme, um painel, um evento gerado a partir do
contador — precisa do nome **como o inventário o escreve** (`BR_SPOAM_OLT.08`:
separador escolhido, número com zero à esquerda) e hoje não tem como pedir isso.
Como `groupKey` é também a key da tabela, é por ele que a aplicação consulta
(`WHERE groupKey = '…'`), então formatar o texto é o que faz a aplicação consultar
pelo nome que ela já usa.

A alternativa da issue — coluna derivada no dataset, que serviria de `groupBy` como
qualquer outra e apareceria no `SELECT` da tabela base — **fica fora deste ciclo**
(entra na materialização: FieldID, merge, payload, tipos imutáveis, formato de
disco). Este plano faz só o formato no contador.

### O que a investigação estabeleceu

- `internal/storage/counter/textkey.go` (`KeyText`) é o **único** produtor do texto.
- **O texto não vai para disco.** `grupos.dict` grava só a chave composta binária;
  `texts[id]` e `byText` nascem em `catchUpWithDict` (`counter.go:222`) na abertura
  e a cada grupo novo. `sameDefinition` (`storage/counters.go:165`) compara só nome,
  colunas e `where`. Logo **mudar o formato não reconstrói o contador** — era a
  pergunta explícita da issue, e a resposta é sim, basta reabrir.
- `WHERE groupKey = '…'` é `PointLookupPlan` → `ByText` → lookup de mapa. Trocar o
  texto troca junto a chave de busca, sem tocar planner nem executor.
- Nenhuma release expõe `groupKey` (contadores são todos posteriores à `v0.7.0`) e
  nenhum cliente JDBC/Python o consome. O piloto não usa `counters:`.

## Desenho

Chave nova no bloco `counters:`, um **molde** com as colunas nomeadas:

```yaml
counters:
  - name: autenticados_por_pon
    groupBy: [olt, slot]
    keyFormat: "{olt}.{slot:02}"     # as aspas são obrigatórias: { abre mapa em YAML
```

`keyFormat` ausente = molde default `{c0}|{c1}|…|{cn-1}`. O molde pode reordenar em
relação ao `groupBy` e ter literais de prefixo e sufixo (`"OLT-{olt}.{slot:02}"`).

### Gramática

Literais + marcadores `{nome}` ou `{nome:0N}`; `{{` e `}}` são as chaves literais.

### Recusas na subida

| # | Regra | Porquê |
|---|---|---|
| R1 | toda coluna do `groupBy` aparece **exatamente uma vez**; nome desconhecido e nome que existe no dataset mas não está no `groupBy` têm mensagens distintas | coluna omitida faz dois grupos distintos renderem o mesmo texto |
| R2 | dois marcadores não podem ser adjacentes (literal entre eles não vazio) | sem separador, a fronteira entre os dois componentes é indecidível |
| R3 | o literal que segue um marcador `int64` não pode começar com dígito nem `-` | senão a varredura gulosa do decimal não sabe onde o número acaba |
| R4 | `:0N` só em `int64`, forma exata `0` seguido de 1–2 dígitos, `N` em [1,20]; valor com mais dígitos sai inteiro, sem truncar | zero à esquerda em string/boolean não quer dizer nada; truncar perderia informação |
| **R5** | **nenhum literal pode conter `\`** | **é o furo que a revisão achou — ver abaixo** |
| R6 | literal não aceita caractere de controle (`\n`, `\t`, …) | em YAML de aspas duplas viram o caractere de verdade, e o `groupKey` fica impossível de digitar num `WHERE` |
| R7 | molde até 256 bytes | o texto de cada grupo é guardado duas vezes em heap (`texts[id]` e a chave de `byText`); sem teto, um molde de 1 KiB × `maxGroups: 65536` são gigabytes decididos por uma linha de YAML |
| R8 | marcador malformado: `{}`, `{ olt }`, `{OLT}`, `{olt:}`, `{olt:2}`, `{olt:0x}`, `{` sem fechar, `}` solto | nomes de coluna são `[a-z0-9_]+`; sem regra explícita o parser da recusa e o do render divergem |
| R9 | `keyFormat: ""` tem mensagem própria ("omita a chave") | precedente do `where: ""` (`schema/counter.go:174`) — cair em R1 daria a mensagem errada |

`keyFormat: "{olt}"` com `groupBy` de uma coluna é **legal** e idêntico ao default.

### R5 — o furo que a revisão adversarial achou

Sem R5 o molde `"{olt}\{descricao}"` (literal `\` entre dois marcadores) passa R1–R4
e **colide**: o conjunto escapado de `olt` é `{\}`, e

| tupla | `olt` | `descricao` | texto |
|---|---|---|---|
| A | `x\` | `y` | `x\\` + `\` + `y` |
| B | `x` | `\y` | `x` + `\` + `\\y` |

são o mesmo `x\\\y`, byte a byte. E não é acadêmico: `"{host}\{share}"` é o molde que
alguém escreve para caminho SMB. R5 (proibir `\` em literal) fecha isso em uma linha,
e só **com R5** a premissa da rede de segurança — "colisão só pode ser bug" — passa a
valer.

### Renderização

- **string**: escapa sempre `\`, e mais o **primeiro caractere do literal que segue
  imediatamente aquele marcador** (o último componente não tem literal depois).
  Não escapa os demais caracteres dos literais — é o que faz `BR_SPOAM_OLT` sair
  inteiro sob `"OLT-{olt}.{slot:02}"` em vez de virar `BR_SPOAM_\O\L\T`.
- **int64**: decimal, com zero à esquerda se `:0N`. Sem escape.
- **boolean**: `true`/`false`. Sem escape (as duas renderizações são prefix-free).

### O default muda o texto de um caso, e isso é decisão explícita

`KeyText` hoje (`textkey.go:37-42`) escapa `|` em **todo** componente string,
inclusive o último. A regra nova não escapa no último, porque ali não há fronteira a
proteger. Então, sob o molde default:

| valores | hoje | regra nova |
|---|---|---|
| `["PRCTA", "a\|b"]` | `PRCTA\|a\\|b` | `PRCTA\|a\|b` |

**Os sete casos de `TestTextoDaChaveEhGolden` continuam verdes nas duas regras** —
nenhum deles tem string com `|` na última posição. É exatamente "teste que passa com
o defeito presente não prova nada". Então, antes de qualquer implementação, o golden
ganha o caso `["a", "b|c"]` com o valor de **hoje**; ele fica vermelho quando a regra
nova entra, e é essa mudança de valor que registra a decisão em vez de deixá-la
silenciosa.

A decisão: **adota-se a regra nova**. Escapar o último componente protege uma
fronteira que não existe; nada está publicado, nenhum cliente consome `groupKey` e o
piloto não usa contadores. O repositório permite mudar contrato com decisão explícita
e registrada — esta é a decisão, e ela vai para o aceite.

### Rede de segurança — antes do `Intern`, nunca depois

`Dict.Intern` (`dict.go:230-238`) faz `WriteAt` + **`fsync` antes de o id existir em
memória**, e `applyRecord` passo 7c diz literalmente "**Não é sticky**"
(`store.go:2005`). Uma colisão detectada *depois* do `Intern` já estaria durável em
`grupos.dict`, e como o id nunca é reemitido, toda abertura seguinte falharia — o
dataset ficaria **impossível de abrir**.

Então a guarda vai para **antes** do `Intern`, no caminho de grupo novo:
`Dict.ByKey` já é o atalho na entrada do `Intern` (`dict.go:209`), então renderizar e
conferir `byText` só no **miss** não custa nada no caminho quente (grupo conhecido
segue sem render, como hoje). Colidiu: recusa, e nada foi a disco.

A mensagem nomeia o molde, os dois ids, o texto colidente e as duas chaves
decodificadas, e diz em uma frase que trocar `keyFormat` resolve **sem reconstruir**.
A guarda de `catchUpWithDict` continua, como segunda linha para a abertura.

### O que entra no manifest, e o que não entra

`keyFormat` **não** entra em `sameDefinition` — não decide o conteúdo do vetor, e é
isso que garante que trocar o molde não reconstrói o contador.

Mas **entra em `checkpoint.CounterInfo`**, só para ser lembrado: na abertura, molde
gravado diferente do molde declarado emite um WARN nomeando os dois. Sem isso, o
texto de todos os grupos muda em silêncio e o suporte não tem como saber o que o
cliente via ontem. A seção do manifest é ignorável (`0x0001`), então o binário antigo
segue abrindo o dataset.

`describeCounters` (`storage/counters.go:625`) passa a logar o molde efetivo junto de
`groupBy`, `where` e `maxGroups`.

## Execução

### Bloco 1 — `internal/keyformat` (pacote folha, TDD)

Novo pacote, dependendo só de `internal/types`. É folha pelo mesmo motivo de
`internal/typeparse`: o **mesmo** parser serve à recusa na subida (`internal/schema`)
e à renderização (`internal/storage/counter`) — duas cópias é como as duas divergem.

- `Column{Name, Kind}`, `Parse(mold string, cols []Column) (*Format, error)`,
  `Default(cols []Column) *Format`, `(*Format) String() string` (o molde efetivo),
  `(*Format) Append(dst []byte, vals []types.Value) ([]byte, error)`.
- `Append` não aloca: escreve no `dst[:0]` do chamador, como `KeyText` hoje.
- Testes: golden do exemplo da issue; um caso por recusa R1–R9; e uma **propriedade
  de injetividade** — tuplas aleatórias sobre um alfabeto que inclui `\`, os literais
  do molde e a string vazia, todos os textos distintos. A propriedade é o que teria
  pego o furo do R5 sozinha.

### Bloco 2 — ligar ao contador

- `counter.Definition` ganha `Format *keyformat.Format`; `validateDefinition`
  (`counter.go:203`) recusa nil — `counter.Open` não pode confiar em ter vindo pelo
  `schema`.
- `textkey.go`: `KeyText` delega ao `Format`. O godoc inteiro é reescrito; a frase
  "roda … a cada linha na tabela virtual" **já está errada hoje** (a tabela lê
  `texts[id]`) e sai na mesma passada.
- `Transition` (`counter.go:370`): render + guarda de colisão no caminho de grupo
  novo, **antes** do `Intern`.
- `catchUpWithDict` (`counter.go:222`): a guarda na abertura.

### Bloco 3 — config e compilação

- `internal/schema/counter.go`: `keyFormat` na lista de `block(...)` (linha 96) — com
  caso de teste novo provando que a 6ª chave é aceita e que uma 7ª ainda é recusada —,
  campo em `rawCounter`, case no switch, `CounterSpec.KeyFormat`, chamada a
  `keyformat.Parse` na validação com o caminho `counters[i].keyFormat: …`. Os tipos
  declarados das colunas já estão à mão em `validateGroupingColumn`. O exemplo YAML
  do godoc do pacote (linhas 16-25) ganha a chave.
- `internal/counters/compile.go`: monta o `Format` (declarado ou default) e o põe na
  `Definition`.
- `checkpoint.CounterInfo` ganha o molde; `infoOfCounters` o grava; `sameDefinition`
  **não** o compara; `openCounters` emite o WARN de molde trocado.

### Bloco 4 — `_meta.counters` diz o formato

Coluna `keyFormat` com o molde efetivo (o default sai expandido, para o operador
sempre ver um molde). Toca `metaTableSpec`, as constantes `columnMeta`,
`metaColumnNames` e `metaRow` — texto calculado uma vez, o gate de zero alocação
por linha continua valendo.

A coluna `groupBy` continua sendo a lista de nomes na ordem **declarada**, junta por
`|`. O godoc de `GroupBySeparator` (`meta.go:117`) e o de `meta.go:50` hoje afirmam
que esse `|` ensina "como a chave da tabela é escrita" — com molde declarável isso
passa a mentir (e mente mais ainda quando o molde reordena). Os dois textos são
corrigidos: `groupBy` é a lista de colunas; quem diz como a key é escrita é
`keyFormat`.

### Bloco 5 — renomeação incremental (débito 116)

Os arquivos tocados carregam identificadores PT-BR que a regra manda renomear no
mesmo PR: `GroupBySeparator`, `metaColumnNames`, `metaTableSpec`,
`cmRetired`/`cmRefused`/`cmOrphans`/`cmAuditDivergent`/`cmAuditAt`, `byNome`,
`maxColumnsOfAgrupamento`, `nameOfTipo`, `sameDefinition`, `describeCounters`. Cada
par velho → novo entra em `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`,
senão o gate `TestRenamedIdentifiersAreNotCitedByTheirOldNames` não conhece o nome
novo. Escopo: só o que estes blocos editam. Commit próprio, separado da feature.

O MCP do GoLand está de pé e indexado — `search_symbol` e `analyze_calls` para achar
os usos, `lint_files` para imports órfãos. Com as ressalvas já pagas nesta base:
rename **não** propaga para implementadores de interface e **não** toca referência
dentro de string. Depois de cada rename: `grep -rn '"nomeVelho"'` e conferir a
**contagem de testes** da suíte, não só o BUILD SUCCESS.

### Bloco 6 — documentação

Tudo que hoje afirma que o separador é `|` e passaria a mentir:

- `README.md:1218-1220`, `:1228-1234` (a lista do que reconstrói — `keyFormat` entra
  na lista do que **não** reconstrói, com a ressalva de que muda o texto) e `:1258`.
- `docs/architecture.md:2222-2226`.
- `docs/glossario.md` — molde ↔ `format`, marcador ↔ `placeholder`; a entrada de
  `texts`/`byText` cita o molde.
- `internal/counters/table_test.go:227` — o nome
  `TestTabelaPorOLTTemUmComponenteSemSeparador` fica falso com um molde que tenha
  literais em volta de um componente só.
- `docs/aceite-formato-do-groupkey.md` novo, com a bancada e as mutações.

## Verificação

Doutrina do repositório: **aplique a mutação, veja vermelho, reverta, veja verde — e
relate os dois.**

| Invariante | Mutação que tem de dar vermelho |
|---|---|
| R1 fecha a ambiguidade | aceitar molde sem uma das colunas do `groupBy` |
| R2 fecha a fronteira | aceitar `"{olt}{slot}"` |
| R3 fecha o decimal | aceitar `"{slot}0{port}"` |
| **R5 fecha o escape** | aceitar `"{olt}\{descricao}"` — a propriedade de injetividade tem de acusar as duas tuplas da tabela acima |
| escape só do primeiro caractere do literal | escapar todos os caracteres dos literais (`BR_SPOAM_OLT` vira `BR_SPOAM_\O\L\T`) |
| o golden vê a mudança do default | rodar o caso `["a","b\|c"]` com o valor de hoje antes da regra nova entrar |
| **trocar o molde não reconstrói** | pôr o molde em `sameDefinition` — o teste de reabertura tem de acusar contador recriado |
| **a colisão não fica durável** | mover a guarda para depois do `Intern` — o teste tem de acusar `grupos.dict` com a chave colidente e a reabertura falhando |
| guarda de colisão | `Format` sem escape construído no teste do próprio pacote, dois grupos que colidam: recusa antes do `Intern` |

Ponta a ponta, além dos unitários:

1. `make test` e `make test-race` (nenhum substitui o outro); `make lint` com a
   baseline igualada — ela quebra nos dois sentidos.
2. Integração: YAML com `keyFormat: "{olt}.{slot:02}"`, ingestão, e
   `SELECT groupKey, total FROM _counters.autenticados_por_pon WHERE groupKey = 'BR_SPOAM_OLT.08'`
   devolvendo a linha por **point lookup** — confirmado pelo contador de plano, como o
   aceite dos contadores já faz —, mais `ORDER BY groupKey`.
3. Reabertura: ingerir com o default, fechar, reabrir com o molde declarado —
   dicionário e totais preservados, `Born() == false`, WARN do molde trocado no log, e
   o texto novo respondendo o point lookup. É a afirmação central da issue.
4. Gates de alocação existentes (`TestTabelaDoContadorNaoAlocaPorLinha` e a
   contraparte sob `-race`) verdes: o texto segue pré-renderizado, e o render extra da
   guarda só acontece no miss do `ByKey`.

## Arquivos principais

- novo: `internal/keyformat/` (parser, renderizador, testes)
- `internal/storage/counter/textkey.go`, `internal/storage/counter/counter.go`
  (`Transition:370`, `catchUpWithDict:222`, `validateDefinition:203`)
- `internal/schema/counter.go` (godoc 16-25, `rawCounter` 70-127, validação 135-194)
- `internal/counters/compile.go`, `internal/counters/meta.go`
- `internal/storage/counters.go` (`sameDefinition`, `infoOfCounters`,
  `describeCounters`), `internal/storage/checkpoint/manifest.go`
- README, `docs/architecture.md`, `docs/glossario.md`,
  `docs/aceite-formato-do-groupkey.md`
- `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`

## Desvios na execução

Dois pontos do plano não sobreviveram ao contato com o código, e ficam
registrados porque em ambos o plano estava errado, não o código.

**O molde não é gravado no manifest.** O plano mandava gravá-lo em
`CounterInfo` — fora da comparação que reconstrói — só para a abertura poder
avisar que ele mudou. O decodificador da seção dos contadores exige tamanho
EXATO, entrada por entrada: um campo novo faria este binário recusar, como
manifest corrompido, todo manifest escrito pelo binário anterior. Trocar
formato de disco para produzir uma mensagem de log não se paga. O aviso virou
o log de subida, que passa a sair com o molde efetivo ao lado de `groupBy`,
`where` e `maxGroups`; comparar dois boots responde a mesma pergunta. Se o
aviso explícito fizer falta, ele cabe numa seção NOVA ignorável, que um
manifest antigo simplesmente não tem — sem tocar na que existe.

**A regra R8 partia de uma premissa falsa.** O plano dizia recusar `{OLT}`
porque "nomes de coluna são `[a-z0-9_]+`". Não são: `spec.go` documenta que
nome de campo no NishiDB nunca foi restrito a minúsculas, e a fixture da spec
tem `Acct-Status-Type`. Inventar um alfabeto ali recusaria coluna legítima.
O nome dentro do marcador passou a ser casado LITERALMENTE contra as colunas
do `groupBy`, e `{ olt }` cai na recusa de nome desconhecido, que é a mensagem
certa. Pela mesma razão o molde default NÃO atravessa o parser: uma coluna com
`:`, `{` ou `}` no nome não teria como ser escrita num molde, e fazer o default
compilar a gramática quebraria dataset que hoje funciona.
