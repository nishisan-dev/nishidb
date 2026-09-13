# Issue #57 — conexão pelo schema (servidor + CLI + JDBC; Python fica para depois)

## Contexto

Hoje a URL do JDBC nomeia o **dataset** (`jdbc:nishidb://host:9142/service_view`), e a
árvore da ferramenta SQL mostra **todos os schemas do servidor**. O `/v1/metadata` sem
filtro lista todos os datasets. O `_meta`, porém, mostra só o contexto da conexão. A
árvore mistura escala de servidor com escala de conexão, e isso é ambíguo.

O que se quer:
1. a URL nomeia o schema: `jdbc:nishidb://host:9142/telecom`;
2. a árvore mostra só o schema da conexão, mais o `_meta`;
3. o SQL passa a dizer o mesmo que a árvore: numa conexão `telecom`, o `FROM fault.events`
   é recusado com uma mensagem que nomeia o schema da conexão. Essa é a recomendação da
   issue, e ela preserva a promessa do `Catalog`.

**Escopo decidido pelo usuário:**
- Servidor, CLI `query --server` e JDBC migram juntos.
- O client Python não é tocado. A única exceção é a entrada obrigatória do código de erro
  novo em `erros.py`, que o gate exige.
- O servidor mantém compatibilidade com o Python. O caminho `dataset` do wire continua
  funcionando exatamente como hoje, e a remoção dele vira débito novo, com critério de
  entrada "o Python migrou".
- A URL velha do JDBC não tem fallback: ela é recusada com uma mensagem que ensina a forma
  nova.

## Desenho

### Wire (`internal/protocol`)
- `Request` ganha `Schema string \`json:"schema"\``. A requisição manda exatamente um dos
  dois, `schema` ou `dataset`. Os dois juntos, ou nenhum, dão `BAD_REQUEST`.
- `/v1/metadata` aceita `?schema=<s>` e devolve os datasets daquele schema, na mesma forma
  de hoje: o base e a irmã do histórico, que herda o schema. `?schema=` junto com
  `?dataset=` dá `BAD_REQUEST`. `?dataset=` e a chamada sem filtro ficam como estão, para
  compatibilidade.
- Código novo `CodeUnknownSchema = "UNKNOWN_SCHEMA"` (HTTP 404), usado pelo `/v1/query` e
  pelo `/v1/metadata`.
  - **Recusa que ensina:** se o valor é o nome de um dataset registrado, a mensagem diz
    `"service_view" é um dataset; a conexão nomeia o schema — use "telecom"`.
  - Senão, a mensagem lista os schemas existentes.
  - A redação sai de uma função única, reusada pelas duas rotas.

### Servidor (`internal/server`)
- `server.go`: um mapa `bySchema` montado no mesmo laço de registro. Ele recebe **só o
  base**, nunca `d.irma`. Esse base é o handle da conexão por schema.
- `query.go` (`handleQuery` / `readRequisicao`):
  - com `schema`, resolve o handle por `bySchema` e chama o motor com escopo;
  - com `dataset`, o caminho segue idêntico ao atual.
  - Os limites continuam saindo do dataset lido, pela variante com escopo de
    `TargetDataset`.
- `documents.go` (`handleMetadata`): o ramo `?schema=` percorre `s.datasets` filtrando por
  `d.sch.SchemaName` e reusa `metaOfDataset`.

### Motor (`internal/query/engine.go`, `catalog.go`)
- `Engine.ExecuteInSchema(ctx, dataset, schemaName, sql, params)` e
  `Engine.TargetDatasetInSchema(...)`. As duas compartilham o corpo do `Execute` atual,
  extraído para uma função interna que recebe o escopo (vazio = servidor inteiro, o
  legado). Com escopo:
  - `FROM x` (sem qualificação) resolve o par `(schemaDaConexão, x)`. Assim abrem a base,
    a irmã `x_history` e os contadores pelo nome, exatamente o que a árvore mostra no
    schema.
  - `FROM _ns.x` continua sendo atalho sobre o handle (o base), sem mudança.
  - `FROM s.x` com `s == schemaDaConexão` resolve normalmente.
  - `FROM s.x` com `s != schemaDaConexão` é recusado com `binder.ErrorAt` na posição do
    schema (`BIND`, 42000). A mensagem nomeia o schema da conexão e lista os endereços
    válidos, `telecom.<tabelas>` e `_meta.<tabelas>`.
- `resolveSchemaQualified` recebe o escopo e, quando ele existe, percorre só os datasets
  daquele schema. Continua lendo `catalogEntries`, sem segunda varredura.
- Godoc de `Catalog`, `Execute` e `resolveSchemaQualified` reescritos. A promessa
  continua: "o que a árvore da conexão lista é o que o Execute da conexão abre, e nada
  além". O godoc também deixa escrito que o caminho sem escopo existe só pelo client
  Python e cita o débito novo.

### CLI (`internal/cli/query.go`, `query_remote.go`)
- `query --server` passa a exigir `--schema`. `--server` com `--dataset` é recusado com
  "o --server endereça o schema: use --schema".
- `query --config --dataset` (local) não muda.
- `remoteQuery` manda `protocol.Request{Schema: ...}`, e `serverHint` é atualizado.
- `UNKNOWN_SCHEMA` cai no mesmo grupo de exit code do `UNKNOWN_DATASET`, porque o
  `errorCode` já trata esse caso no default; isso é conferido e travado por teste.

### Driver JDBC (`clients/jdbc`)
- **`NishiUrl`:** o único segmento do caminho vira `schema()`. O Javadoc e as mensagens
  passam a dizer `host[:porta]/schema`, e somem os comentários velhos de "não tem catálogo
  nem schema".
- **`ConnectionConfig`:** `schema()` no lugar de `dataset()`.
- **`NishiConnection`:**
  - o connect faz `GET /v1/metadata?schema=`;
  - os limites são lidos do elemento base, o que tem `name` sem ser irmã: o primeiro
    elemento, na ordem de `s.datasets`;
  - quando o servidor responde `UNKNOWN_SCHEMA`, o driver acrescenta
    `a URL é jdbc:nishidb://host[:porta]/<schema>` à mensagem do servidor;
  - `getSchema()` devolve o schema da conexão;
  - `setSchema(igual)` não faz nada, e `setSchema(outro)` lança `SQLException` 3F000
    dizendo que o schema é fixado pela URL.
  - `datasetPadrao` sai.
- **`NishiStatement`:** o corpo passa a ser `{"schema": <conexão>, "sql": ...}`.
  - O palpite de dataset pelo `FROM` deixa de existir: o `SqlScanner` perde `dataset()` e
    fica só com `placeholders()`, que o `NishiPreparedStatement` usa.
  - Os testes do scanner sobre o nome do `FROM` saem junto.
- **`NishiDatabaseMetaData`:**
  - `datasets()` busca `/v1/metadata?schema=<conexão>`;
  - `getSchemas` passa a dar `[_meta, telecom]`;
  - `getTables` e `getColumns` recortam naturalmente;
  - a dedup do `_meta` continua, porque base e irmã publicam `_meta` e o base vem
    primeiro;
  - **sai o fallback para servidor sem `tables`** (`metadataSemTablesDegradaSemErro`),
    porque servidor e driver saem juntos e o `?schema=` já não existe em servidor antigo.
- **`wire/Codigos.java` e `SqlStates.java`:** `UNKNOWN_SCHEMA` → `3F000`
  (`SQLNonTransientException`), com assert explícito em `SqlStatesTest`.
- **Python:** só a constante `UNKNOWN_SCHEMA` e a entrada `ProgrammingError` em
  `clients/python/src/nishidb/erros.py`, exigidas por
  `TestTodoCodigoDoProtocoloTemExcecaoNoClientPython`.

### Identificadores
Nas funções que a feature reescreve, os identificadores PT-BR são renomeados para inglês
(ex.: `filtro`, `readRequisicao`, `remoteQuery(endereco…)`), e os pares Go entram em
`planning/identificadores-em-ingles/mapeamento-2-manual.tsv`. Código novo nasce em inglês.
Contratos existentes (`dataset` no wire, `datasets` no documento) não mudam, porque o
Python depende deles.

## Testes (e mutação: vermelho → reverter → verde, relatados)

- **Go, servidor** (`documents_test.go`, `query_test.go`), com dois datasets em schemas
  diferentes mais uma irmã:
  - `?schema=` devolve só base e irmã do schema;
  - schema desconhecido e nome de dataset passado como schema dão 404 `UNKNOWN_SCHEMA` com
    a dica;
  - `schema` + `dataset` juntos dão `BAD_REQUEST`;
  - o caminho `dataset` segue idêntico (os testes atuais continuam verdes sem edição).
- **Gate novo** `TestTheConnectionTreeAndTheConnectionSqlAgree`: para cada schema, toda
  linha não-`_` do `/v1/metadata?schema=S` abre por `FROM S.name` **e** por `FROM name`
  com `schema=S`, e a tabela de outro schema é recusada.
  - Mutação 1: tirar o filtro de escopo do `resolveSchemaQualified` → vermelho.
  - Mutação 2: tirar o filtro do `?schema=` → vermelho.
- **Motor:** testes de `ExecuteInSchema` para as quatro formas do `FROM`, incluindo a
  mensagem da recusa cross-schema.
- **CLI:** `--server` sem `--schema`, `--server --dataset` recusado e a requisição enviada
  com `schema`.
- **JDBC unit** (`NishiUrlTest`, `NishiConnectionTest`, `NishiDatabaseMetaDataTest`,
  `SqlStatesTest`, `SqlScannerTest`): as fixtures passam a responder `?schema=`, o corpo
  leva `"schema"`, e `getSchema`/`setSchema` e a recusa ensinada são cobertos.
- **JDBC IT** (`DriverIT`; `scripts/jdbc-it.sh` passa a usar a URL `/telecom`):
  - `getSchemas` é exatamente `{_meta, telecom}`;
  - `getSchema()` devolve `telecom`;
  - a URL velha `/todos_os_tipos` falha com 3F000 e a mensagem contém `telecom`;
  - `FROM todos_os_tipos` sem qualificação executa;
  - o `scripts/it-comum.sh` compartilhado não muda.

## Documentação
- `README.md`: seção de clients (URL, árvore, `getSchema`), `nishidb query --server
  --schema`, e a quebra registrada.
- `nishidb-spec-proposal.md` §70: o exemplo da URL passa a nomear o schema.
- Novo `docs/aceite-conexao-por-schema.md`: critérios, evidências (nomes de teste) e a
  quebra de contrato da URL e do CLI.
  - `docs/aceite-schema-declarado.md` é ajustado onde afirma "endereço de servidor" e o
    fallback do driver.
- `docs/debitos.md`:
  - **120** — remover o caminho `dataset` do `/v1/query` e do `/v1/metadata` quando o
    Python migrar, com o critério de entrada.
  - **118** — atualizado ou fechado: com a resolução restrita ao schema, conexão e dataset
    lido só diferem na irmã; conferir se ela herda os limites do base.
  - **119** — atualizado: o driver passa a pedir um schema só.
- `docs/glossario.md`: "schema da conexão | connection schema".
- `docs/diagrams/sequence_query_http.puml` e `query_flow.puml`: o `schema` na requisição e
  o recorte, validados pelo `uml.nishisan.dev`.

## Execução
- Worktree **antes** de criar a branch `feature/conexao-por-schema`, porque há alteração
  alheia em `internal/alarm/enrich.go` na árvore principal.
- Commits por bloco:
  1. protocol + motor
  2. servidor + gate
  3. CLI
  4. JDBC
  5. `erros.py`
  6. docs e débitos, cada bloco já com os seus docs
- Plano copiado para `planning/conexao-por-schema.md`.

## Verificação
- `make build`, `make test` (com contagem de testes), `make test-race`.
- `make lint`, com a baseline igualada nos dois sentidos.
- `make build-jdbc`, `make test-jdbc-unit`, `make test-jdbc` (IT contra o binário real).
- `make test-python` e **`make test-python-it`** verdes, sem tocar no Python além de
  `erros.py`. É isso que prova a compatibilidade.
- Mutações do gate relatadas, e `git status` limpo de resíduo antes de cada commit.

## Riscos
- **`setSchema(outro)` lançando 3F000:** se o DBeaver chamar `setSchema("_meta")` ao
  selecionar a pasta, a conexão reclama. Alternativa se aparecer no uso: ignorar em
  silêncio o `_meta`. Fica anotado no aceite como ponto a observar no piloto.
- A quebra da URL atinge o piloto (`service_view` → `telecom`), e o operador precisa
  trocar a URL no DBeaver após o deploy.
