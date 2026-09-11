# `_meta.counters` — listar os contadores por SQL

> **Estado: aprovado em 04/09/2026, executado na branch `feature/meta-counters`.**

## O que se quer

`SELECT * FROM _meta.counters` lista os contadores mantidos na escrita do
dataset consultado — nome, chave de agrupamento, condição, grupos vivos, teto,
aposentados, recusados, órfãs, bytes e a última auditoria. Hoje a lista só
sai pelo `nishidb counters` e pelo `/v1/stats`; por SQL, só por engano (o erro
de tabela desconhecida nomeia as que existem).

## Desenho

- **Uma tabela virtual registrada no motor** sob o namespace `_meta`, pelo
  mesmo `RegisterVirtual` que serve `_counters.<nome>`. Mora em
  `internal/counters`, e não em `internal/query/meta`: o `meta` descreve o
  dataset pela definição estática e não conhece o storage; os números vivos
  vêm de `EstatisticasOfCounters`, e o pacote que já cola schema, motor e
  storage é o `counters`.
- **Roteamento**: `_meta.fields` continua indo para o pacote `meta`; qualquer
  outra tabela do `_meta` vai ao registro de tabelas virtuais. A recusa de
  tabela desconhecida em `_meta` lista `fields` mais as registradas.
- **A tabela existe em todo dataset**, vazia quando não há `counters:` — "quais
  contadores existem" tem resposta mesmo quando é nenhum.
- **Colunas**: `name` (key), `groupBy` (`olt` ou `olt|slot|port`, o mesmo
  separador do `groupKey`), `condition` (não `where`: é palavra reservada do
  SQL), `groups`, `maxGroups`, `retired`, `refused`, `orphans`, `bytes`,
  `auditDivergent` e `auditAt` (timestamp; os dois são NULL até a primeira
  auditoria conclusiva desta abertura).
- **Lê o presente**, como o `_counters`: uma linha por contador, o conjunto é
  fixo pela vida do processo (contador só nasce na abertura), os números são
  o retrato no instante da linha. Zero alocação por linha (gate).

## Tarefas

1. `internal/counters/meta.go`: `Contado`, `TableMeta`, fonte; `Retain`
   registra `_meta.counters` sempre.
2. `internal/query/engine.go`: roteamento e mensagem de recusa do `_meta`.
3. Testes: tabela (lista, vazia, auditoria nula até a primeira, point lookup),
   gate de alocação com contraparte `-race`, roteamento no motor, servidor.
4. README, `docs/architecture.md`.
5. `make test`, `make test-race`, `make lint` (baseline igualada), PR.
