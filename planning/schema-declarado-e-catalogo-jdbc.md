# Schema declarado por dataset e catálogo para o JDBC

Proposta escrita em 11/09/2026. Estado: **desenho aprovado nas decisões, spec
para revisão**. Origem: no DBeaver, os contadores não aparecem em lugar nenhum,
e não há como perguntar ao servidor que tabelas existem.

## O problema, em três fatos do código

1. **O driver não esconde as tabelas virtuais — ele não sabe que existem.**
   `/v1/metadata` (`internal/server/documents.go:25`) publica os datasets base e
   suas colunas, e nada mais. `_counters.<nome>`, `_meta.fields`,
   `_meta.counters` e `_meta.kafka` não atravessam o fio. O
   `NishiDatabaseMetaData.getTables` lista dataset como `TABLE`, `getSchemas`
   devolve vazio e `getTableTypes` devolve só `TABLE`.

2. **As tabelas virtuais estão presas ao dataset da requisição.** O driver
   extrai o dataset do próprio `FROM` (`NishiStatement:76`), com o da URL como
   fallback — então uma conexão alcança a tabela base de QUALQUER dataset do
   servidor. Mas `FROM _counters.x` não nomeia dataset nenhum e cai no default.
   Com dois datasets no servidor, os contadores de um deles são inalcançáveis
   daquela conexão. **É uma limitação real de hoje, não uma consequência desta
   proposta.**

3. **A gramática tem dois níveis, e isso é decisão de produto.**
   `parser.go:148`: "nome no FROM tem no máximo dois níveis (namespace.tabela);
   o NishiDB não tem catálogo". Qualquer coisa que vire schema consome um
   nível, e não sobra um terceiro.

O fato 3 é a restrição dura: ele elimina de saída qualquer desenho em que a
ferramenta gere `catalogo.schema.tabela`.

## O desenho

O YAML do dataset declara o schema, e ele é **obrigatório**:

```yaml
version: 1
name: service_view
schemaName: telecom
```

Tudo daquele dataset passa a viver nele:

```
telecom.service_view            -- a tabela base
telecom.autenticados_por_slot   -- um contador
telecom.autenticados_por_pon    -- outro contador

_meta.fields                    -- o dicionário de campos
_meta.counters                  -- a lista de contadores
_meta.kafka                     -- o estado da fonte
_meta.tables                    -- NOVA: a lista das tabelas
```

**As tabelas de meta ficam no `_meta`, e não dentro do schema do dataset.**
A primeira versão deste desenho as punha em `telecom._fields`, tipadas
`SYSTEM TABLE` para o cliente separá-las. O spike da Task 1 mostrou que o
DBeaver separa **escondendo**: objeto de sistema não aparece na árvore por
default, e a opção que o revela não foi encontrada na interface. Uma tabela
que existe para ser descoberta, invisível por default, é a tabela errada.

`_meta` já é namespace reservado, o parser já o aceita e o SQL dele já
funciona — `SELECT * FROM _meta.counters` responde hoje, sem depender do
roteamento de schema. Expô-lo como schema próprio na árvore é o caminho mais
curto e o que gera SELECT válido desde já. Inventar um `_system` ao lado seria
um segundo nome para a mesma coisa.

O preço: `_meta` descreve UM dataset — o da requisição. Com vários datasets no
servidor, o `_meta` da árvore é o do dataset default da conexão. É o
comportamento de hoje, mas passa a ficar visível, e por isso entra na
documentação.

Dois níveis exatos, sem schema nula, sem ambiguidade entre datasets, e a árvore
do DBeaver com a forma que a ferramenta espera.

`_counters.<nome>` e `_meta.<nome>` continuam valendo, como **abreviação do
dataset da requisição**. São contrato publicado — o README, o CLI e a
documentação os usam —, e quebrá-los não se paga. A forma canônica na
documentação passa a ser a qualificada.

### Por que obrigatório, e o que isso custa

Opcional com default deixaria `service_view.service_view`, e opcional sem
default manteria a árvore mista que esta proposta veio consertar. Obrigatório é
coerente com a doutrina do repositório: **config recusa na subida, nunca
adivinha**.

**Todo YAML de dataset existente para de abrir até ganhar a linha**, inclusive o
do piloto — e isso foi decidido com o custo à vista (11/09/2026): o produto está
em desenvolvimento, não há release publicada, e quebrar config aqui é barato.
A mensagem de recusa ainda tem de nomear a chave e mostrar um exemplo; ensinar a
correção não é zelo, é o que separa uma recusa útil de um "faltando".

### A colisão fica impossível por construção, não detectada

Com as tabelas de meta no `_meta`, dentro do schema do dataset sobram a tabela
base e os contadores. A colisão entre eles é tornada irrepresentável, e não
conferida:

- **nome de dataset** já é recusado com prefixo `_` (`spec.go:682`);
- **nome de contador** passa a ser recusado com prefixo `_` — hoje não é
  (`validateCounters` só chama `isIdentName`, que aceita o underscore), e é uma
  linha;
- **nome de contador** também recusa o mesmo nome do dataset — é a única
  colisão que o prefixo não fecha, e é recusa na subida nomeando os dois.

A recusa do prefixo `_` em contador deixou de ser carga estrutural quando as
tabelas de meta saíram do schema do dataset. Fica como higiene: o `_` é a
reserva do produto, e um contador chamado `_fields` seria confusão gratuita.

### `schemaName`, e não `schema_name`

A convenção do YAML deste produto é camelCase: `groupBy`, `maxGroups`,
`retainEmptyGroups`, `bootstrapServers`, `expireAfterWrite`. Chave de config é
contrato, e um `schema_name` no meio delas seria a única fora do padrão.

Regras: `[a-z0-9_]+`, não pode começar com `_`, e é **único no servidor** — dois
datasets com o mesmo schema seriam duas árvores misturadas, e a subida recusa
nomeando os dois datasets.

## As peças

### Motor — a fonte única

Uma função no `Engine` que devolve o catálogo de um dataset: por tabela, o
schema, o nome, o tipo (`TABLE` / `VIEW` / `SYSTEM TABLE`) e as colunas já
compiladas. Ela é a **única** fonte, com dois consumidores: o handler do
`/v1/metadata` e a tabela `_tables`. Nunca duas derivações do mesmo fato — é a
mesma disciplina que o `/v1/stats` e o `/metrics` já seguem.

O roteamento passa a resolver o schema declarado como qualificador do dataset,
ao lado dos namespaces reservados. O binder hoje exige que o dataset do
argumento bata com o do `FROM` (`engine.go:226`); a regra continua, agora
resolvendo o schema antes de comparar.

### `_tables`

Uma linha por tabela consultável: `name` (key), `schema`, `type`, `columns`
(contagem) e `description`. O `_meta` deste produto já é o information_schema —
`fields`, `counters`, `kafka` — e faltava exatamente a tabela que lista as
tabelas.

### Protocolo

`DatasetMeta` ganha o `schema` e a lista de tabelas do dataset, cada uma com
nome, tipo e colunas no mesmo `ColumnMeta` que já existe — inclusive o código
`jdbc`, que o servidor calcula e o driver não saberia derivar.

É **aditivo**: driver velho contra servidor novo ignora o campo; driver novo
contra servidor velho não o encontra e não mostra view nenhuma. Degrada sozinho,
sem negociação de versão. O `protocolo` tem quatro falantes, e os gates que
cobram paridade entre os clients precisam ser lidos ANTES, não depois.

### Driver JDBC

- `getSchemas` devolve os schemas declarados dos datasets do servidor;
- `getTableTypes` devolve `TABLE`, `VIEW` e `SYSTEM TABLE`;
- `getTables` passa a listar as virtuais com `TABLE_SCHEM` preenchido — os
  contadores como `VIEW` no schema do dataset (derivada, somente leitura, sem
  armazenamento próprio) e as de meta como `VIEW` no schema `_meta`. **Não**
  `SYSTEM TABLE`: o spike provou que isso as esconde;
- `getColumns` cobre as virtuais pelo mesmo documento.

O que o DBeaver gera a partir disso é `SELECT * FROM telecom.autenticados_por_slot`,
que é SQL válido. **Esse é o teste que decide o desenho**, e ele é feito na
ferramenta de verdade, não só em teste de unidade.

## O que NÃO entra

- Terceiro nível no `FROM`. A gramática de dois níveis é premissa de produto.
- `INFORMATION_SCHEMA` com os nomes e as colunas do padrão SQL. O pedido foi
  explícito: "tipo, mas não igual".
- Client Python. Fora do escopo funcional; só os gates de paridade são
  consultados.

## Verificação

Doutrina: para cada invariante, aplicar a mutação, ver vermelho, reverter, ver
verde — e relatar os dois.

| Invariante | Mutação que tem de dar vermelho |
|---|---|
| `schemaName` ausente recusa na subida | aceitar o YAML sem a chave |
| Schema repetido entre datasets recusa | dois datasets com o mesmo `schemaName` |
| `schemaName` com prefixo `_` recusa | `schemaName: _meta` |
| Contador com prefixo `_` recusa | `name: _fields` num contador |
| Contador homônimo do dataset recusa | contador `service_view` no dataset `service_view` |
| A tabela qualificada resolve, e resolve o dataset CERTO | `FROM telecom.autenticados_por_slot` contra um servidor com dois datasets, conferindo que o outro não responde |
| O atalho continua valendo | `_counters.x` sem qualificar |
| O catálogo tem fonte única | fazer `_tables` derivar de outro lugar que não a função do Engine — a divergência tem de aparecer num teste |
| O documento é aditivo | driver novo contra metadata sem o campo: nenhuma view, nenhum erro |

Ponta a ponta: `make test`, `make test-race`, `make lint` na baseline,
`make test-jdbc` (o IT roda contra o binário real) e **a árvore aberta no
DBeaver de verdade**, com um `SELECT` gerado pela ferramenta em cada tipo de
tabela.

## Riscos

**O de config saiu da lista.** Tornar `schemaName` obrigatório derruba a subida
de todo dataset existente, e isso foi aceito explicitamente: até a primeira
release, quebrar contrato de config, wire ou nome em disco é decisão barata e
não precisa ser negociada caso a caso. No piloto é uma linha e um restart.

**O risco de ferramenta foi MEDIDO em 11/09/2026, e o desenho passou.** O spike
da Task 1 (driver com metadata fixa, contra o piloto) respondeu:

1. **Sim** — o DBeaver monta `telecom` como schema, com pastas `Tables` e
   `Views` e as tabelas dentro.
2. **Sim** — e é o que decide o desenho: a ferramenta gera o SELECT
   **qualificado**, `SELECT * FROM telecom.autenticados_por_pon`.
3. **As `SYSTEM TABLE` não apareceram na árvore.** O DBeaver esconde objeto de
   sistema por default (a opção "Show system objects" da conexão). A separação
   que o desenho pediu acontece — mas por ocultação, não por pasta ao lado.
   Decidir se `_fields`/`_counters`/`_tables` devem ficar escondidas por
   default é decisão de produto, e está registrada aqui porque só o spike podia
   revelá-la.

O SELECT qualificado ainda FALHA contra o servidor — `dataset
"autenticados_por_pon" desconhecido (este handle é de "service_view")` —, e
isso é o estado esperado: o spike falsifica só o lado do driver, e o
roteamento do schema no motor é a Task 7. O erro confirma o diagnóstico do
desenho: o parser toma `telecom` como namespace e `autenticados_por_pon` como
DATASET, e o binder o compara com o dataset da requisição. É exatamente esse
passo que a Task 7 troca.

**O risco que sobra é o de ferramenta.** O desenho pressupõe que o DBeaver monte a
árvore a partir de `getSchemas` + `getTables` e gere `schema.tabela` no SELECT.
É o comportamento esperado, mas é pressuposto sobre software de terceiro — e por
isso ele é verificado cedo, com um protótipo do `getTables` antes de o resto ser
construído, e não no fim.
