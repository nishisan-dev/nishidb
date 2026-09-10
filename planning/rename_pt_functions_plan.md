## Descrição do Objetivo
O objetivo desta tarefa é padronizar a base de código renomeando diversas funções na pasta `internal/` que atualmente estão escritas em português (ou misturando português e inglês) para o inglês. Isso ajudará a manter a consistência com o restante do código que, em sua grande maioria, já utiliza o inglês.

## Revisão do Usuário Necessária
> [!IMPORTANT]
> Muitas funções não exportadas e algumas exportadas foram identificadas. Renomear funções afetará todas as suas chamadas ao longo do projeto. O plano abaixo propõe traduções diretas e idiomáticas para os nomes. Por favor, revise as traduções propostas e avise se preferir nomes diferentes para alguma delas.
> Como a quantidade de funções é muito grande (aproximadamente 100 funções), farei a renomeação utilizando ferramentas de substituição semântica (`gopls` rename, se disponível, ou substituições cuidadosas por pacote). 

## Perguntas em Aberto
> [!WARNING]
> Você prefere que eu realize a renomeação de todas as funções encontradas de uma vez, ou que eu faça em etapas menores (ex: pacote por pacote) criando commits separados para cada um? (Recomendo commits separados por pacote para facilitar a revisão, seguindo a diretriz de commits atômicos).

## Alterações Propostas

Abaixo estão as principais funções identificadas por pacote e suas respectivas propostas de novos nomes. (Nota: esta é uma lista parcial das mais proeminentes, a refatoração abrangerá todas as funções com nomes em português identificadas nos pacotes).

---

### `internal/cli`
Renomearemos as funções auxiliares do CLI que manipulam erros, logs e formatação.

#### [MODIFY] internal/cli/query.go
- `comLimit` ➔ `withLimit`
- `inicioDaClausula` ➔ `clauseStart`
- `ehEspaco` ➔ `isSpace`
- `imprimeErroDeQuery` ➔ `printQueryError`

#### [MODIFY] internal/cli/counters.go
- `contadorDoStorage` ➔ `storageCounter`
- `contadorDoWire` ➔ `wireCounter`
- `escreveContadores` ➔ `writeCounters`
- `escreveAuditoria` ➔ `writeAudit`
- `desfechoDaAuditoria` ➔ `auditOutcome`
- `escreveReconstrucao` ➔ `writeReconstruction`

#### [MODIFY] internal/cli/server.go
- `formatoDeLog` ➔ `logFormat`
- `nivelDeLog` ➔ `logLevel`

#### [MODIFY] internal/cli/operation.go
- `exigeServidor` ➔ `requiresServer`
- `recusaRemota` ➔ `remoteRefusal`
- `consultaRemota` ➔ `remoteQuery`
- `normalizaEndereco` ➔ `normalizeAddress`

#### [MODIFY] internal/cli/query_remote.go
- `leCorpoErro` ➔ `readErrorBody`
- `dicaDeEsquema` ➔ `schemaHint`
- `recusaDoServidor` ➔ `serverRefusal`
- `leStream` ➔ `readStream`
- `dicaDeServidor` ➔ `serverHint`

#### [MODIFY] internal/cli/offsets.go
- `origemDaSpec` ➔ `specOrigin`
- `imprimeOffsets` ➔ `printOffsets`
- `formataOffsets` ➔ `formatOffsets`

#### [MODIFY] internal/cli/ingest.go
- `desfechoMaisGrave` ➔ `mostSevereOutcome`
- `precisaCheckpoint` ➔ `needsCheckpoint`
- `imprimeResumo` ➔ `printSummary`
- `linhaPorCategoria` ➔ `rowByCategory`

#### [MODIFY] internal/cli/mutation.go
- `postaMutacao` ➔ `postMutation`
- `escreveCompact` ➔ `writeCompact`
- `escreveCheckpoint` ➔ `writeCheckpoint`

---

### `internal/server`
Renomearemos as funções de utilidade do servidor HTTP, métricas e segurança.

#### [MODIFY] internal/server/mutations.go
- `erroDeMutacao` ➔ `mutationError`
- `processaDatasets` ➔ `processDatasets`
- `mensagemParcial` ➔ `partialMessage`

#### [MODIFY] internal/server/metrics_index.go
- `comRotulo` ➔ `withLabel`
- `indiceStats` ➔ `indexStats`

#### [MODIFY] internal/server/security.go
- `resolveCredenciais` ➔ `resolveCredentials`
- `resolveSenha` ➔ `resolvePassword`
- `validaCertificadoNaSubida` ➔ `validateCertificateOnUpload`
- `enderecoLoopback` ➔ `loopbackAddress`

#### [MODIFY] internal/server/query.go
- `ehJSON` ➔ `isJSON`
- `tetoDeLinhas` ➔ `rowCeiling`
- `traduzErroDeAbertura` ➔ `translateOpenError`

#### [MODIFY] internal/server/counters.go
- `erroDeContadores` ➔ `countersError`

#### [MODIFY] internal/server/server.go
- `fechandoStore` ➔ `closingStore`
- `nomesOrdenados` ➔ `sortedNames`

#### [MODIFY] internal/server/materialization.go
- `materializacaoStats` ➔ `materializationStats`
- `duracaoDaRegra` ➔ `ruleDuration`

#### [MODIFY] internal/server/history.go
- `validaNomesDeHistorico` ➔ `validateHistoryNames`

---

### `internal/kafka`
Renomearemos os utilitários de consumo e writer do Kafka.

#### [MODIFY] Vários arquivos em internal/kafka/
- `novoPoolDeDecode` ➔ `newDecodePool`
- `NovoWriter` ➔ `NewWriter`
- `precisaCheckpoint` ➔ `needsCheckpoint`
- `valorDoCampo` ➔ `fieldValue`
- `novaEsteira` ➔ `newPipeline`
- `potenciaDeDois` ➔ `powerOfTwo`
- `limiarDeSlot` ➔ `slotThreshold`
- `intervaloDaVigia` ➔ `watchdogInterval`
- `registraMetricas` ➔ `registerMetrics`
- `novaBarreira` ➔ `newBarrier`
- `marcaEspera` ➔ `markWait`
- `emCurso` ➔ `inProgress`
- `NovaFila` ➔ `NewQueue`

---

### `internal/storage`
Renomearemos funções internas de controle de índices e persistência.

#### [MODIFY] Vários arquivos em internal/storage/
- `limparIndicesGlobaisLegados` ➔ `cleanLegacyGlobalIndices`
- `instanteDeEscrita` ➔ `writeInstant`
- `validaDerivadasContraDicionario` ➔ `validateDerivedAgainstDictionary`
- `validaOpcoesDoModo` ➔ `validateModeOptions`
- `oSegmentoSaiuDoDataset` ➔ `segmentLeftDataset`
- `indiceIlegivel` ➔ `unreadableIndex`
- `idDoNomeAposentado` ➔ `retiredNameId`

---

### `internal/metakafka`
#### [MODIFY] Vários arquivos em internal/metakafka/
- `Tabela` ➔ `Table`
- `igualAChave` ➔ `equalsKey`
- `EstadoDeclarado` ➔ `DeclaredState`
- `Registra` ➔ `Register`
- `erroDeColuna` ➔ `columnError`

## Plano de Verificação

### Testes Automatizados
Após a refatoração, o pacote de testes deverá ser executado para garantir que nenhuma referência foi quebrada durante o processo de renomeação.

```bash
make test
make lint
```
(Assegurar que `go build ./...` e `go test ./...` passam sem erros).

### Verificação Manual
O usuário pode revisar os diffs gerados ou os commits atômicos para checar se a nomenclatura e a formatação se mantêm corretas.
