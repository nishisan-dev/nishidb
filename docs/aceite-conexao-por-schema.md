# Aceite — conexão pelo schema (issue #57)

## O que mudou

A URL do driver JDBC passou a nomear o **schema da conexão**, e não o dataset:

```
jdbc:nishidb://db01:9142/telecom
```

A árvore que a ferramenta SQL mostra passou a ser só a do schema da conexão e a
do `_meta`, e o servidor passou a resolver o `FROM` no MESMO escopo. Antes, a
árvore misturava duas escalas: os schemas eram endereço de SERVIDOR (o
`/v1/metadata` publicava todos os datasets e `FROM fault.events` abria de
qualquer conexão), e o `_meta` era endereço de CONEXÃO. Quem abria a conexão em
`service_view` via os schemas `telecom` e `fault` — e só um deles era "o que a
conexão enxerga".

A decisão em aberto da issue foi resolvida pela recomendação: **o schema da
conexão é também o escopo de resolução**. Esconder o schema alheio só da árvore,
com o SQL ainda abrindo o servidor inteiro, teria invertido a promessa do
`Catalog` ("toda tabela listada É aberta, e tabela que se abre não fica de
fora") — a consulta abriria o que a árvore esconde.

| endereço numa conexão `telecom` | resolve |
|---|---|
| `FROM radius_sessions` | a tabela do schema `telecom` — a base, a irmã do histórico ou um contador, pelo nome da árvore |
| `FROM telecom.radius_sessions` | a mesma |
| `FROM _meta.tables`, `FROM _counters.por_olt` | o atalho sobre o dataset base do schema, sem mudança |
| `FROM fault.events` | **recusado** (BIND, SQLSTATE 42000), nomeando o schema da conexão e os endereços que ela abre |

As três camadas:

- **Wire.** O `POST /v1/query` ganhou o campo `schema`, e o `GET /v1/metadata`
  o filtro `?schema=`. O código novo `UNKNOWN_SCHEMA` (404) cobre o schema que
  nenhum dataset declara; o driver o traduz para SQLSTATE 3F000.
- **Servidor e motor.** O handle da conexão é o dataset BASE do schema
  (`Server.bySchema`, sem a irmã). O `Engine.ExecuteInSchema` e o
  `Engine.TargetDatasetInSchema` resolvem com escopo; a decisão de rota é uma só
  (`resolveFrom`), para que a consulta e os limites que o servidor aplica não
  discordem sobre o dono.
- **Clientes.** O driver JDBC manda o schema da URL em toda consulta e pede o
  metadata recortado; o `SqlScanner` perdeu o palpite de dataset e ficou só com
  a contagem de `?`. O `nishidb query --server` passou a exigir `--schema`.

## Quebra de contrato

Aceita na issue — o produto não tem release com compatibilidade a honrar —, e
feita sem fallback:

- **A URL do JDBC.** `jdbc:nishidb://host:porta/<dataset>` deixou de resolver. O
  connect com ela falha com SQLSTATE 3F000, e a mensagem **ensina**: o servidor
  reconhece o nome de dataset e diz o schema (`"service_view" is a dataset, and
  the connection names its schema; use "telecom"`), e o driver acrescenta a
  forma da URL. O piloto precisa trocar a URL no DBeaver depois do deploy.
- **`nishidb query --server`.** Passou a exigir `--schema`; `--server` com
  `--dataset` é recusado nomeando a flag certa. A consulta local
  (`--config --dataset`) não mudou.
- **O driver exige o servidor novo.** O fallback para o servidor sem o campo
  `tables` saiu: o `?schema=` e o `tables` saem do mesmo servidor.
- **`setSchema`.** Com outro schema, é recusado (3F000): o escopo é fixado pela
  URL, e aceitar em silêncio faria a ferramenta acreditar que trocou.

## O que NÃO mudou: o client Python

O client Python ficou fora do escopo, e continua funcionando sem alteração: o
servidor mantém o caminho por dataset (`dataset` no `/v1/query`, `?dataset=` e a
chamada sem filtro no `/v1/metadata`, `Engine.Execute` sem escopo). Os dois
caminhos são exclusivos por requisição — `schema` e `dataset` juntos dão
`BAD_REQUEST`. A remoção do caminho antigo, junto com a migração do Python, é o
débito 123. A única linha tocada no Python é a constante `UNKNOWN_SCHEMA` em
`erros.py`, que o gate de códigos exige de todo código novo do protocolo.

## Critérios e evidências

| critério | evidência |
|---|---|
| A árvore da conexão e o SQL da conexão dizem a mesma coisa: tudo o que o `/v1/metadata?schema=` publica abre pelo endereço qualificado E pelo nome simples, e toda tabela de outro schema é recusada pelos dois | `TestTheConnectionTreeAndTheConnectionSqlAgree` (as tabelas dos outros schemas saem do metadata sem filtro, e não de uma lista escrita no teste) |
| O recorte traz o base e a irmã, com o base primeiro — o driver lê os tetos do primeiro | `TestMetadataBySchemaListsTheBaseBeforeTheSister` |
| O nome de dataset no lugar do schema recebe o schema certo, nas duas rotas e também pelo nome da irmã; o nome que não é nada lista os schemas | `TestUnknownSchemaTeachesTheSchemaOfADatasetName` |
| `schema` e `dataset` são exclusivos, e um dos dois é obrigatório | `TestSchemaAndDatasetAreExclusive` |
| As quatro formas do `FROM` na conexão por schema | `TestConnectionSchemaOpensCounterByBareName`, `TestConnectionSchemaAcceptsItsOwnQualifier`, `TestConnectionSchemaRefusesAnotherSchema` (posição no qualificador; o mesmo SQL abre pelo caminho por dataset), `TestConnectionSchemaKeepsTheReservedShortcutOnTheHandle`, `TestConnectionSchemaBareNameOutsideTheSchemaIsRefused` |
| Os limites que o servidor aplica saem do dataset que a consulta com escopo abre | `TestTargetDatasetInSchemaFollowsTheConnectionScope` |
| O caminho por dataset segue idêntico | as suítes de `internal/server` e `internal/query` anteriores, sem edição nos corpos `dataset` |
| O CLI remoto endereça o schema, e a flag trocada é recusada nomeando a certa | `TestRemoteQueryAddressesTheSchemaNotTheDataset`, `TestOsExitCodesDaConsultaRemota` (o dataset no lugar do schema sai 1 ensinando o schema) |
| O driver manda o schema da URL em toda consulta, e nunca `dataset` | `NishiConnectionTest#oSchemaDaUrlVaiEmTodaConsulta` |
| `getSchema` devolve o schema da URL; `setSchema` com outro é recusado | `NishiConnectionTest#oSchemaDaConexaoEhFixadoPelaUrl` |
| O schema inexistente falha no connect com 3F000 e a forma da URL | `NishiConnectionTest#schemaInexistenteFalhaNoConnectEnsinandoAFormaDaUrl`, `SqlStatesTest#schemaDesconhecidoEhDaClasse3F` |
| A árvore do driver é a do schema da conexão, e toda chamada ao metadata leva o recorte | `NishiDatabaseMetaDataTest#listaBaseEViewsComSchema`, `NishiDatabaseMetaDataTest#osSchemasSaemDistintosEOrdenados` |
| Contra o binário REAL: `getSchemas` é exatamente `_meta` e o schema da conexão, o nome inexistente e o schema alheio saem 42000 | `DriverIT#aSequenciaQueUmaFerramentaFazAoAbrirAConexao`, `DriverIT#oErroDoServidorChegaTraduzido` |
| Contra o binário REAL: a URL da forma anterior falha ensinando o schema | `DriverIT#aUrlQueNomeiaODatasetEnsinaOSchema` |

## As mutações: o vermelho e o verde de cada uma

Cada defeito foi aplicado sobre uma cópia guardada do arquivo, a suíte ficou
vermelha, a cópia foi restaurada e a suíte voltou ao verde; nenhum resíduo ficou
no código (conferido por `grep` e `git status`).

| mutação aplicada | reprovou em | o que a falha disse |
|---|---|---|
| A recusa do qualificador alheio é desarmada (`if false && ...` no `resolveFrom`) | `TestConnectionSchemaRefusesAnotherSchema` | a recusa deixou de nomear o schema pedido; o gate do servidor ficou VERDE, e é o esperado: sem a recusa, o par é resolvido como `(schema da conexão, tabela)`, que não existe, e a consulta continua recusada por outro motivo — um oráculo de concordância não é oráculo da mensagem |
| A conexão por schema passa a resolver como o servidor inteiro (o escopo é ignorado) — a forma DOMINANTE do defeito que a issue descreve | `TestTheConnectionTreeAndTheConnectionSqlAgree` e quatro testes do motor | `"SELECT * FROM schema_servicos.servicos LIMIT 1" fora da árvore respondeu "", quer BIND`; e, do outro lado, `a árvore lista eventos_history e "SELECT * FROM eventos_history" recusa com BIND` |
| O `?schema=` deixa de recortar (`if false && ...` no filtro do `handleMetadata`) | `TestTheConnectionTreeAndTheConnectionSqlAgree` | `conexão schema_eventos: a árvore traz o dataset servicos do schema schema_servicos` |
| A recusa do schema desconhecido perde a dica do nome de dataset | `TestUnknownSchemaTeachesTheSchemaOfADatasetName` | `a mensagem não contém "use \"schema_eventos\""`, nas duas rotas e pelo nome da irmã |

## Verificação (13/09/2026)

| comando | resultado |
|---|---|
| `make build` | ok |
| `make test` (com os gates de medida) | ok |
| `make test-race` | ok |
| `make lint` | 330 apontamentos, igual à `.golangci-baseline` (os dois de `gofmt` que a mudança introduziu foram corrigidos) |
| `make test-jdbc` (IT contra o binário real) | 208 testes JUnit, 0 falhas, 0 SKIP — os 11 do `DriverIT` rodaram |
| `make test-python` | 155 testes, 0 falhas (25 SKIP de integração, que o `test-python-it` cobre) |
| `make test-python-it` | 25 testes, 0 falhas — o client Python, intocado, atravessa o servidor novo: é a prova da compatibilidade |
| `plantuml -checkonly` nos dois diagramas alterados | sem erro; aferido com um arquivo de controle quebrado, que acusou `Some diagram description contains errors` |

## Pontos a observar no piloto

- **`setSchema` no DBeaver.** Se a ferramenta chamar `setSchema("_meta")` ao
  selecionar a pasta do `_meta` como schema ativo, a conexão recusa com 3F000. O
  caminho alternativo, se aparecer no uso, é aceitar em silêncio o `_meta`
  (que não muda o escopo de resolução) — não foi feito às cegas.
- **Troca de URL.** `service_view` → `telecom` na conexão do DBeaver do piloto.
