# Aceite — aliases de projeção (issue #41)

Rodada local de 14/09/2026, Linux amd64, Go 1.26.7, JDK 25.0.4.
Plano: [aliases por consulta](../planning/issue-41-aliases-projecao.md).

## Resultado e compatibilidade

`SELECT campo AS alias` e `SELECT campo alias` publicam o rótulo nos dados
e nos metadados, preservando tipo e nulabilidade. O mesmo contrato vale
para colunas aninhadas e tabelas virtuais.

A decisão aprovada nesta rodada amplia a issue: qualquer repetição recebe
um sufixo, sem perder colunas. Aliases declarados na consulta, com ou sem
`AS`, têm prioridade sobre nomes sem alias; entre aliases iguais, vence o
primeiro. Todos os nomes pedidos são reservados antes de numerar.

`ORDER BY` usa os nomes finais, inclusive sufixos, antes do schema.
`WHERE` usa somente o schema; caminhos qualificados continuam aninhados.
Consultas antes recusadas por nomes repetidos passam a retornar todas as
colunas com rótulos únicos. Não há mudança de storage, YAML ou protocolo.

## Evidências funcionais

| Critério | Prova |
|---|---|
| Duas grafias, aspas, escapes e posições de erro | `TestProjectionAliasSyntax`, `TestProjectionAliasErrorsHavePositions` |
| Prioridade, sufixos e campos/slots preservados | `TestProjectionAliasesAndSuffixes` |
| Nomes futuros reservados, projeção grande e AST reutilizável | `TestLargeAliasProjectionReservesNamesWithoutMutatingAST`, `TestLargeProjectionDuplicatesReceiveSuffixes` |
| Caminhos aninhados, sobreposição com curinga e limite de colunas | `TestNestedProjectionAliasesAndSuffixes` |
| ORDER BY, sufixos, precedência e WHERE independente | `TestOrderByUsesFinalProjectionNames`, `TestAliasOrderingAndWhereRemainIndependent` |
| Tabelas virtuais | `TestAliasesOnVirtualTables` |
| Metadados, inclusive sem linhas | `TestEmptyResultRetainsAliasMetadata`, `TestProjectionAliasesReachStreamMetadataAndRows` |
| Nome reservado do protocolo | `TestProjectionAliasCannotHideReservedFrameKey`, `TestReservedOutputColumnCanBeAliased` |
| JDBC: nomes, tipos, nulabilidade e acesso por rótulo | `DriverIT.projectionAliasesAndSuffixesReachMetadataAndLookup` |
| Python: description e acesso por nome | `test_projection_aliases_and_suffixes` |

## Memória

`TestAliasScanAndEncodingAllocateOnlyPerQuery` mediu **31 alocações por
consulta**, tanto com 1.000 quanto com 8.000 linhas, usando alias com aspas
internas e colisão que gera sufixo. O encoder isolado teve **zero alocação
por linha**, com buffer previamente reservado.

O teste existente `TestScanAllocsIndependenteDoNumeroDeRows` continuou em
**38 alocações por consulta**, sem elevar seu teto. As duas medições usam
consultas diferentes e não representam comparação de desempenho entre si.

## Provas por mutação

Cada defeito abaixo foi aplicado temporariamente e fez o teste correspondente
falhar por asserção; nenhuma falha de compilação foi contada como prova.
Depois de restaurar os arquivos, a rodada conjunta dos testes passou.

| Defeito introduzido | Resultado com defeito / após restauração |
|---|---|
| Ignorar o alias da coluna simples | vermelho / verde |
| Retirar a prioridade do alias sobre nome sem alias | vermelho / verde |
| Dar prioridade ao último alias repetido | vermelho / verde |
| Ocupar um sufixo já pedido mais adiante | vermelho / verde |
| Desligar a normalização de duplicatas | vermelho / verde |
| Ignorar nomes finais no ORDER BY | vermelho / verde |
| Consultar aliases da projeção na resolução geral de campos | vermelho / verde |
| Aceitar palavras contextuais reservadas sem aspas | vermelho / verde |
| Aceitar alias em curinga aninhado | vermelho / verde |
| Interpretar DISTINCT como coluna com alias implícito | vermelho / verde |
| Copiar o nome original do campo para a saída | vermelho / verde |
| Zerar o tipo dos metadados | vermelho / verde |
| Zerar a nulabilidade dos metadados | vermelho / verde |
| Copiar o nome com alocação em cada linha do encoder | vermelho / verde |
| Ignorar aliases nas projeções simples e aninhadas | vermelho / verde |
| Fazer alias literal com ponto tomar o lugar do caminho qualificado | vermelho / verde |

Na mutação de alocação, o gate passou de 31 constantes para **3.031**
alocações com 1.000 linhas e **24.031** com 8.000, detectando a regressão.
Todas as mutações foram revertidas.

## Suítes executadas

- `make test`: passou, incluindo gates de alocação e vazão.
- `make test-race`: passou; os gates de medida se anunciam como skip.
- Unitários JDBC e Python: passaram.
- `make test-jdbc`: 209 testes, zero falhas, erros ou skips.
- `make test-python-it`: 26 testes, zero falhas, contra o servidor real.
- Lint Go 2.12.2: **330 apontamentos**, iguais à baseline existente;
  nenhum apontamento novo, sem alterar o orçamento.
- `make lint-python`: Ruff e mypy passaram nas versões fixadas no projeto.
- Testes de aliases após as mutações e nova execução com race dos pacotes
  binder/parser: passaram.
