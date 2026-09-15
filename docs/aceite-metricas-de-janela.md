# Aceite — métricas de janela do contador e alarme qualificável

Rodada de aceite em 15/09/2026, na branch `feature/metricas-de-janela`, sobre o
HEAD `34a23c2` mais o bloco de documentação. Máquina: Intel Core 7 150U (12
threads), NVMe com ext4, Linux 6.8, Go 1.26.5 (`GOTOOLCHAIN=go1.26.5`).
Registro de rodada: não é reescrito.

Origem do que está aqui: o plano
([`planning/metricas-de-janela-e-alarme-qualificavel.md`](../planning/metricas-de-janela-e-alarme-qualificavel.md));
a bancada `BenchmarkSampleOncePilotShape` e o teste
`TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups`, rodados NESTA rodada, com
o comando e a saída colados abaixo; e as mutações relatadas pelos Builders e
Refuters de cada bloco do ciclo, consolidadas pelo nome do teste que reprovou.
A arquitetura está em [`architecture.md`](architecture.md), seções "Métricas de
janela do contador" e "Alarmes e emissão periódica de contador".

## O critério, e como ele tem de ser lido

O plano fixou três coisas: as colunas `<métrica>_<agregado>` em
`_counters.<nome>` com a tabela `_meta.counter_metrics`; o `qualifyWhen` com o
motivo `disqualified`; e a memória — **arquivo mmap por métrica, com heap
pequeno e limitado**. A terceira mudou de forma durante o ciclo, e a mudança é o
que mais importa ler aqui: o primeiro desenho endereçava o estado pelo id do
grupo, e a revisão mostrou que o id de um grupo aposentado nunca é reaproveitado
— heap e disco cresceriam com todo id que o contador já emitiu. O estado passou
a ser endereçado pela POSIÇÃO de grupo vivo, reaproveitada no aposentamento, e o
custo ficou limitado pelo máximo de grupos vivos simultâneos mais 4 B por id por
contador. Abaixo, cada número traz a origem: o estado do amostrador sob
rotatividade e o heap vivo no formato do piloto são medidos; os bytes que o
`Sampler.Stats` publica são contabilidade, e aparecem rotulados como tal.

## Critérios e as evidências

| Critério (plano) | Provado por |
|---|---|
| Schema: `metrics:` com as recusas (lista vazia, teto de 8, nome, `type`, `source`, janela fora de 1m–30d ou não múltipla de 1m, agregados) | `TestCounterMetricsRefusals`, `TestCounterMetricsContractParsesInDeclaredOrder`, `TestMetricWindowAcceptsTheBoundaries`, `TestCounterMetricsAcceptUpToTheCeiling`, `TestMetricNameAcceptsLetterOrUnderscoreStart`, `TestCounterWithoutMetricsBlockHasNilMetrics` |
| Colisão num lugar só: coluna gerada × `groupKey`, `total`, `groupBy` ou outra gerada; nome de métrica × coluna, métrica ou alarme; alarme com nome de coluna continua aceito | `TestCounterColumnsListFixedThenGroupByThenGenerated`, `TestFindCounterColumnCollisionNamesBothOrigins`, `TestCounterMetricsRefusals`, `TestCountersRecusas`, `TestAlarmNamedAfterCounterColumnIsStillAccepted`, `TestTableSchemaRefusesMetricColumnCollidingWithGroupingColumn`, `TestTableSchemaRefusesGroupingColumnNamedAfterFixedColumn` |
| Formato `metricwindow`: colunar, golden de bytes, crescimento preservando identidade e todo slot, reset por cabeçalho divergente ou ilegível, teto de capacidade, acesso sem alocação | `TestGoldenBytesOfHeaderAndEntry`, `TestFileRoundTripsBucketsAndIdentityAcrossReopen`, `TestEnsureCapacityPreservesIdentityAndEveryBucketSlotWhenGrowing`, `TestOpenGrowsAnExistingFileToTheRequestedMinimumAndNeverShrinks`, `TestOpenResetsTheFileWhenTheHeaderDiverges`, `TestOpenResetsAndOpenReadOnlyRefusesAnUnreadableHeader`, `TestEnsureCapacityRefusesAnIDAtOrAboveTheMaximum`, `TestOpenRefusesAnInvalidShapeOrCapacityRange`, `TestFileAccessorsDoNotAllocate` |
| Escrita parcial num crash: ordem das palavras, marca de dono, `validFromEpoch`, tmp residual, fsync do diretório, fechamento idempotente | `TestWriteBucketZeroesWordZeroFirstAndWritesItLast`, `TestSetIdentityWritesValidFromEpochBeforeKeyHash`, `TestBucketOwnerDisagreesWithAStaleIdentityOfAReusedID`, `TestWriteBucketHonorsTheOwnerTagItReceives`, `TestALeftoverDirtyTemporaryFileNeverLeaksIntoTheRewrittenFile`, `TestOpenOfAValidFileRemovesALeftoverTemporaryFileWithoutRewriting`, `TestGrowthWhoseDirectorySyncFailsKeepsTheFileOnTheNewInode`, `TestFailedReopenAfterGrowthClosesTheFileAndEnsureCapacityReturnsErrClosed`, `TestCloseIsIdempotentAndAClosedFileRefusesUse` |
| O balde só vai ao disco quando fecha; o `Stop` grava o aberto que mudou, e repetido não regrava | `TestPassesWithinTheSameEpochDoNotWriteBuckets`, `TestRepeatedStopDoesNotRewriteOpenBuckets`, `TestRestartWithinTheSameEpochReloadsTheOpenBucket` |
| Janela parcial é o agregado do que existe; a janela desliza em 60 baldes; o cache dos fechados anda com a época | `TestSingleSamplePublishesItsOwnAggregate`, `TestSlidingWindowDropsTheSampleAfterSixtyBuckets`, `TestClosedCacheMovesWithTheEpochEvenWhenTheOpenBucketIsEmpty` |
| Identidade pelo hash da chave: contador reconstruído, aposentadoria, troca de dono na mesma época, marca de dono colidindo | `TestRebuiltCounterReusingAnIdDoesNotLeakTheOldBuckets`, `TestRetiredGroupHasNoValueAndReturningKeyStartsFromZero`, `TestOwnerSwapWithinTheSameEpochDiscardsTheOldOpenBucket`, `TestClosedBucketsBeforeValidFromEpochAreExcludedEvenWithCollidingOwnerTags`, `TestKeyHashMatchesTheFNV1aGolden` |
| Restauração antes da primeira passada; parada maior que a janela; balde de outro dono ignorado | `TestRestartWithinTheWindowRestoresBeforeAnySample`, `TestRestartBeyondTheWindowPublishesNoValue`, `TestRestoreIgnoresBucketsWrittenUnderAnotherOwnerTag` |
| Estado por POSIÇÃO de grupo vivo, reaproveitada; custo limitado pelos vivos simultâneos mais 4 B por id; consistência da lista livre e do mapa | `TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups`, `TestReusedPositionStartsWithoutTheRetiredOwnerSamples`, `TestRestoreAfterRotationRecoversLiveGroupsAndFreesPositionsRetiredWhileStopped`, `TestRestoreFreesBothPositionsOfADuplicatedHash`, `TestRetiredIdsBeyondMaxGroupsKeepTheSamplerRunning`, `TestSampleRefusesALimitAboveTheDictionaryCeiling`, `TestAcquireFailureLeavesTheFreeListTheNextPositionAndTheMapUnchanged`, `TestMetricsDisagreeingOnAPositionInvalidateIt`, `TestReadOnlyRestoreNeverWritesWhileFreeingPositions`, `TestFileCapacityLimitCoversEveryDictionaryIDAsAPowerOfTwo` |
| Seqlock: o leitor nunca vê linha rasgada nem o valor de outro grupo numa posição reusada; passada e leitura sem alocação; avg sempre finito | `TestConcurrentReadersNeverSeeATornAggregate`, `TestAggregateNeverReturnsTheValueOfTheGroupThatReusedThePosition`, `TestReadersNeverSeeAnotherGroupsValueWhilePositionsAreReused`, `TestSampleOnceAndAggregateDoNotAllocate`, `TestPublishedAvgIsFiniteForExtremeTotals` |
| Intervalo `clamp(menor janela / 60, 1 s, 10 s)`; a contagem de 16 bits cabe no piso e satura | `TestSamplingIntervalIsClampedBetweenFloorAndCeiling`, `TestOpenBucketCountFitsInSixteenBitsAtTheSamplingFloor`, `TestOpenBucketCountSaturatesAtTheSixteenBitCeiling` |
| Órfãos e reset: o `Open` limpa com métrica declarada, `RemoveOrphans` limpa sem ela, link simbólico nunca é seguido, cabeçalho divergente recria com WARN | `TestOpenRemovesOrphanFilesAndDirectories`, `TestOrphanCleanupNeverFollowsOrRemovesSymlinks`, `TestRemoveOrphansWithoutSamplerRemovesWhatIsNoLongerDeclared`, `TestRemoveOrphansOnAMissingDirectoryCreatesNothing`, `TestRemoveOrphansNeverFollowsASymlinkedDirectory`, `TestRemovingTheLastWindowMetricRemovesItsFilesOnTheNextStart`, `TestOpenRecreatesFileWithDivergentHeaderAndWarns` |
| Ciclo de vida do laço: falha de infra para e congela; Start e Stop concorrentes nunca sobem dois laços; idempotência | `TestLoopStopsAtTheFirstInfraFailureAndMetricKeepsAnswering`, `TestStartDuringStopDoesNotSpawnASecondLoop`, `TestConcurrentStartAndStopNeverRunTwoLoops`, `TestStopAndCloseAreIdempotent`, `TestStartDoesNotSpawnTheLoopWithoutMetrics`, `TestOpenRefusesOptionsThatWouldShareOrLoseFiles` |
| Colunas em `_counters.<nome>`: tipos, NULL sem amostra, projeção, filtro e ordem, sufixo `_1`, teto da linha sem alocar | `TestCounterTableWithMetricsPublishesTypedNullableMetricColumns`, `TestCounterTableWithMetricsReadsPublishedAggregates`, `TestCounterTableWithMetricsAnswersProjectionFilterAndOrder`, `TestCounterTableWithoutMetricSourceLeavesMetricColumnsNull`, `TestConditionOnMetricColumnCompilesAgainstTableSchema`, `TestTableSchemaRefusesMoreMetricsThanTheCeiling`, `TestTableSchemaRefusesUnknownAggregate`, `TestTableRefusesMetricSourceWithoutDeclaredMetric`, `TestCounterTableAtRowCeilingPublishesEveryColumn`, `TestCounterTableDoesNotAllocatePerRowWithMetrics`, `TestCounterTableAtRowCeilingDoesNotAllocatePerRow`, `TestTabelaDoContadorNaoAlocaPorLinha` |
| `_meta.counter_metrics`: definição na ordem declarada, uso lido da fonte, NULL sem fonte, registrada mesmo sem métrica, sem alocação por linha | `TestCounterMetricsMetaListsDefinitionsInDeclaredOrder`, `TestCounterMetricsMetaReadsUsageFromMetricSource`, `TestCounterMetricsMetaIsNullWithoutMetricSource`, `TestCounterMetricsMetaWithoutMetricsIsEmptyTable`, `TestCounterMetricsMetaRefusesMetricDeclaredTwice`, `TestCounterMetricsMetaRefusesSourceWithoutDeclaredMetric`, `TestRegisterPublishesCounterMetricsMetaTableWithoutMetrics`, `TestRegisterPublishesCounterMetricColumnsAndMetaTable`, `TestRegisterRefusesOpenCounterWithoutDeclaredSpec`, `TestCounterMetricsMetaTableDoesNotAllocatePerRow` |
| Alarme: só o grupo qualificado arma; pendente que perde a qualificação volta sem evento; disparado resolve `disqualified`; só volta depois de requalificar e sustentar; precedência; NULL não qualifica | `TestQualifiedAlarmOnlyFiresForQualifiedGroups`, `TestPendingGroupThatLosesQualificationReturnsToAbsentWithoutEvent`, `TestPendingGroupThatLosesQualificationReturnsToAbsentSilently`, `TestFiringGroupThatLosesQualificationResolvesAsDisqualified`, `TestFiringTrackerThatLosesQualificationResolvesAsDisqualified`, `TestDisqualifiedAlarmFiresAgainOnlyAfterRequalifyingAndSustaining`, `TestSameSweepPrecedenceIsRetiredThenClearedThenDisqualified`, `TestClearedConditionWinsOverDisqualificationInTheSamePass`, `TestMissingMetricSampleNeverQualifies` |
| Custo do alarme: o `qualifyWhen` só é avaliado com o `when` valendo, e a passada não aloca por grupo | `TestQualifyWhenIsOnlyEvaluatedWhenTheConditionHolds`, `TestSweepWithQualifyWhenDoesNotAllocatePerGroup` |
| `alarms.state` v2: assinatura com `qualifyWhen` e impressão das métricas citadas; v1 recusada; teto do texto recusado na subida | `TestPersistedStateCarriesTheQualificationSignature`, `TestRestoreDropsTheTimerWhenTheQualificationSignatureChanges`, `TestMetricsFingerprintListsOnlyCitedMetricsByName`, `TestAlarmStateV1FileIsRefusedAsUnknownVersion`, `TestAlarmStateWithoutQualificationRoundTripsEmptyTexts`, `TestAlarmTextAboveTheStateCeilingIsRefusedAtStartup`, `TestMetricsFingerprintAboveTheStateCeilingIsRefused` |
| Evento e `_meta.alarms`: `qualifyCondition` só quando declarada, grupo `metrics` no log, motivo no log, coluna NULL sem declaração | `TestAlarmEventCarriesQualifyConditionAndMetrics`, `TestDisqualifiedResolutionIsLoggedWithItsReason`, `TestQualifyConditionIsLoggedOnlyWhenDeclared`, `TestQualifyConditionColumnIsNullWhenNotDeclared` |
| Servidor: a MESMA fonte para a tabela e o alarme; restauração antes da primeira passada; desqualificação com 24 h simuladas; séries desde a subida; sem métrica, sem amostrador; falha de abertura não deixa `.win` mapeado | `TestWindowMetricsFeedTheTableTheAlarmAndSurviveARestart`, `TestCounterMetricSeriesArePublishedBeforeTheFirstPass`, `TestDatasetWithoutWindowMetricsOpensNoSampler`, `TestDatasetFailingAfterTheSamplerOpenedLeavesNoWindowFileMapped`, `TestAbortedStartupLeavesNoWindowFileOfAnOpenedDatasetMapped`, `TestMetricSourceReadsThePublishedAggregate`, `TestMetricSourceReturnsNilInterfaceForUndeclaredMetric`, `TestNilSamplerHasNilMetricSource`, `TestCountersWithMetricsSkipsCountersWithoutMetrics`, `TestCountersWithMetricsRefusesACounterThatIsNotOpen`, `TestCounterMetricsDirIsSiblingOfTheCountersDir` |
| Ordem de parada `forAlarms` → `forEmitters` → `forCounterMetrics` → `closeDatasets` | `TestLacosDoServidorParamAntesDoQuiesceDosDatasets` |
| Consulta local só lê: não escreve nem cria diretório, NULL sem arquivo e sem log, nunca limpa órfão, WARN de arquivo ilegível no stderr | `TestLocalQueryReadsRestoredWindowMetricsWithoutWritingTheFiles`, `TestLocalQueryWithoutWindowMetricFilesAnswersNull`, `TestLocalQueryNeverRemovesWindowFilesOfARemovedMetric`, `TestLocalQueryWarnsOnStderrWhenAWindowFileIsUnreadable`, `TestReadOnlyPublishesFromDiskWithoutWriting`, `TestMetricAnswersEveryDeclaredMetricInBothModes` |
| Observabilidade: toda amostra com família e nome; inventário de `metricas.md` | `TestAmostrasDeMetricaTemFamiliaENome`, `TestTodaMetricaDeclaradaEstaDocumentada`, `TestODocDeMetricasNaoInventaSerie` |
| Drivers: `_meta.counter_metrics` no catálogo e os nomes `rows_5m_avg`, `x`, `rows_5m_avg_1` | `DriverIT#windowMetricReachesTheCatalogAndTheProjectionNames`, `test_window_metric_reaches_the_catalog_and_projection_names` |

## As mutações: o vermelho e o verde de cada uma

Doutrina de verificação: o defeito foi aplicado, a suíte ficou vermelha, o
defeito foi revertido, a suíte voltou ao verde. Os relatos são dos Builders e
Refuters de cada bloco; nenhuma mutação foi reaplicada nesta rodada de
documentação.

### Schema

| Mutação | Reprovou em |
|---|---|
| Recusa de colisão desligada | `TestCounterMetricsRefusals` (colunas geradas × `groupBy`, `total` e `groupKey`) e `TestCountersRecusas` |
| Janela não múltipla de 1m aceita (`90s`, `5m500ms`) | `TestCounterMetricsRefusals` |
| Nome de métrica sem checar o primeiro caractere | `TestCounterMetricsRefusals` |
| `TableSchema` sem checar colisão | `TestTableSchemaRefusesGroupingColumnNamedAfterFixedColumn`, que discrimina pela mensagem |

Achado do Refuter, corrigido no bloco: os casos antigos de `TestCountersRecusas`
passavam pela recusa "coluna não declarada" mesmo com a recusa de colisão
desligada — o teste não provava o que dizia.

### Formato em disco (`metricwindow`)

| Mutação | Reprovou em |
|---|---|
| Crescimento sem copiar as colunas de balde | `TestEnsureCapacityPreservesIdentityAndEveryBucketSlotWhenGrowing` |
| Offsets, cabeçalho ou campos permutados | `TestGoldenBytesOfHeaderAndEntry` — em três das seis variantes, SÓ o golden pegou, o que prova que ele não se autocumpre |
| Reset ignorando largura do balde, fonte ou `bucketCount` | `TestOpenResetsTheFileWhenTheHeaderDiverges` |
| Sem checar truncamento, crc ou versão | `TestOpenResetsAndOpenReadOnlyRefusesAnUnreadableHeader` |
| Sem o teto de capacidade | `TestEnsureCapacityRefusesAnIDAtOrAboveTheMaximum` |
| Alocação em `Bucket` | `TestFileAccessorsDoNotAllocate` (1 alocação por chamada) |
| fsync do diretório antes da reabertura | `TestGrowthWhoseDirectorySyncFailsKeepsTheFileOnTheNewInode` |
| Sem `ErrClosed` e sem piso na dobra | `TestFailedReopenAfterGrowthClosesTheFileAndEnsureCapacityReturnsErrClosed` (não voltou em 5 s) |
| `WriteBucket` derivando a marca da identidade em vez de honrar a recebida | `TestWriteBucketHonorsTheOwnerTagItReceives` |
| Remoção do tmp como no-op; `Open` sem remover o tmp residual | `TestALeftoverDirtyTemporaryFileNeverLeaksIntoTheRewrittenFile`, `TestOpenOfAValidFileRemovesALeftoverTemporaryFileWithoutRewriting` |
| `Close` sem zerar a região | `TestCloseIsIdempotentAndAClosedFileRefusesUse` |
| Ordem de escrita das palavras invertida | `TestWriteBucketZeroesWordZeroFirstAndWritesItLast`, `TestSetIdentityWritesValidFromEpochBeforeKeyHash` |

Defeitos que o Refuter confirmou e que foram corrigidos no bloco: o File ficava
no inode antigo quando o fsync do diretório falhava depois do rename; o
`EnsureCapacity` entrava em laço infinito depois de uma reabertura que falhou;
faltava a marca de dono no balde (crash entre páginas); o `WriteBucket` ignorava
a marca recebida; o tmp residual sujo contaminava o crescimento; o `Close` não
era idempotente.

### Amostrador (`countermetrics`)

| Mutação | Reprovou em |
|---|---|
| Sem a checagem do hash da chave | `TestRebuiltCounterReusingAnIdDoesNotLeakTheOldBuckets` (3 amostras, avg 668,3; esperado 1 amostra, avg 5) |
| `Stop` sem gravar o balde aberto | `TestRestartWithinTheSameEpochReloadsTheOpenBucket` (0 amostras; esperado 2) |
| Balde gravado a cada passada, e não no fechamento | `TestPassesWithinTheSameEpochDoNotWriteBuckets` (15 gravações em 5 passadas; esperado 0) |
| Publicação sem seqlock; seq par durante a escrita; leitor sem reconferir a seq | `TestConcurrentReadersNeverSeeATornAggregate` (15.496 a 30.437 leituras rasgadas por rodada sem seqlock) |
| Janela parcial devolvendo NULL | `TestSingleSamplePublishesItsOwnAggregate` |
| Balde aberto do dono antigo gravado com a marca do novo | `TestOwnerSwapWithinTheSameEpochDiscardsTheOldOpenBucket` |
| Piso de amostragem de 100 ms | `TestOpenBucketCountFitsInSixteenBitsAtTheSamplingFloor` (432.001 amostras, acima de 65.535) |
| Contagem sem saturação | `TestOpenBucketCountSaturatesAtTheSixteenBitCeiling` (70.535; esperado 65.535) |
| Cache dos fechados renovado só no fechamento do balde | `TestClosedCacheMovesWithTheEpochEvenWhenTheOpenBucketIsEmpty` |
| Limpeza de órfãos seguindo link simbólico | `TestOrphanCleanupNeverFollowsOrRemovesSymlinks` |
| Sem `época do balde >= validFromEpoch` | `TestClosedBucketsBeforeValidFromEpochAreExcludedEvenWithCollidingOwnerTags` |
| Restauração sem conferir a marca de dono | `TestRestoreIgnoresBucketsWrittenUnderAnotherOwnerTag` (4 amostras; esperado 0) |
| Teto de capacidade de volta a `MaxGroups+1` | `TestRetiredIdsBeyondMaxGroupsKeepTheSamplerRunning`, `TestMetricAnswersEveryDeclaredMetricInBothModes` |
| `Stop` soltando o lock antes de aguardar o laço | `TestStartDuringStopDoesNotSpawnASecondLoop`; sob `-race`, `TestConcurrentStartAndStopNeverRunTwoLoops` |
| `Stop` regravando balde já gravado | `TestRepeatedStopDoesNotRewriteOpenBuckets` (6 gravações; esperado 3) |
| Posição não liberada na aposentadoria | `TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups` (capacidade 2.048 e 2 blocos; esperado 1.024 e 1) |
| Reuso de posição sem a troca de dono | `TestReusedPositionStartsWithoutTheRetiredOwnerSamples` (3 amostras, avg 668,3; esperado 1, avg 5) |
| Restauração sem liberar o hash sem dono vivo | `TestRestoreAfterRotationRecoversLiveGroupsAndFreesPositionsRetiredWhileStopped` |
| Leitor sem conferir o dono | `TestAggregateNeverReturnsTheValueOfTheGroupThatReusedThePosition`, `TestReadersNeverSeeAnotherGroupsValueWhilePositionsAreReused` (725 a 1.570 leituras com valor de outro grupo) |
| Posição tirada da lista livre antes de garantir a capacidade | `TestAcquireFailureLeavesTheFreeListTheNextPositionAndTheMapUnchanged` |
| Conflito entre métricas numa posição ignorado | `TestMetricsDisagreeingOnAPositionInvalidateIt` |
| Modo leitura liberando posição sem dono | `TestReadOnlyRestoreNeverWritesWhileFreeingPositions` (panic de escrita em região somente-leitura) |

Duas mutações ficaram verdes por construção e são inofensivas: ler o dono fora
do seqlock (N5) e zerar o mapa antes do publicado (N6), porque a aposentadoria
publica dono 0 entre um dono e outro. O teste concorrente se prova não vácuo pelo
número de releituras que ele mesmo registra e cobra acima de zero — as rodadas
relatadas tiveram de 8 a 55.798, inclusive presas a um núcleo com
`taskset -c 0` —, e o estresse de posição reusada leu 2.628.841 valores sem
nenhum de outro grupo.

Registro do Refuter da quarta rodada: o godoc de `restore` dizia que a chave
herdava a janela "sempre que" o amostrador não observasse a aposentadoria, e uma
sonda mostrou que, com a chave voltando só DEPOIS da abertura, não há herança. O
texto foi corrigido para as duas condições que a arquitetura descreve.

### Tabelas (`counters`)

| Mutação | Reprovou em |
|---|---|
| Colunas escritas com zero amostras | `TestCounterTableWithMetricsReadsPublishedAggregates`, `TestCounterTableWithMetricsAnswersProjectionFilterAndOrder` |
| `_meta.counter_metrics` não registrada sem métrica | `TestRegisterPublishesCounterMetricsMetaTableWithoutMetrics`, `TestMetaTablesRespondePelaRotaDeConsultaDoServidor`, `TestQueryLocalListaMetaCountersNaIrmaQueOServidorNaoLista` |
| Alocação no laço da linha | gate de alocação por linha da tabela (2 alocações por linha; o relato não nomeia qual dos gates do arquivo reprovou) |
| avg gravado via `float32` | `TestCounterTableWithMetricsReadsPublishedAggregates`, `TestCounterTableWithMetricsAnswersProjectionFilterAndOrder`, `TestCounterTableAtRowCeilingPublishesEveryColumn` (comparação bit a bit) |
| Teto da linha `maxRowColumns - 1` | `TestCounterTableAtRowCeilingDoesNotAllocatePerRow`, `TestCounterTableAtRowCeilingPublishesEveryColumn` (panic de índice) |

Achado do Refuter, devolvido e corrigido: na primeira rodada a mutação `float32`
ficou VERDE, porque nenhum teste discriminava a precisão do avg; a correção
passou a comparar os bits do double, e a mesma mutação ficou vermelha nos três
testes da tabela.

### Alarme

| Mutação | Reprovou em |
|---|---|
| Disparado ignora `qualified` | `TestFiringTrackerThatLosesQualificationResolvesAsDisqualified`, `TestFiringGroupThatLosesQualificationResolvesAsDisqualified`, `TestSameSweepPrecedenceIsRetiredThenClearedThenDisqualified` |
| Aceitar o `alarms.state` v1 | `TestAlarmStateV1FileIsRefusedAsUnknownVersion` — só a asserção de versão pega, porque o arquivo v1 também seria recusado por tamanho |
| Impressão só com o nome da métrica | `TestRestoreDropsTheTimerWhenTheQualificationSignatureChanges` (os dois subtestes de janela alterada restauraram o cronômetro) |
| `qualifyWhen` avaliado antes do `when`, ou antes do `continue` do grupo saudável | `TestQualifyWhenIsOnlyEvaluatedWhenTheConditionHolds` (64 avaliações; esperado 0) — os gates de alocação ficam verdes, porque avaliar não aloca |
| Precedência invertida | `TestSameSweepPrecedenceIsRetiredThenClearedThenDisqualified` — o teste do runner fica verde, porque a passada nunca combina `when` falso com grupo não qualificado |
| String vazia em vez de NULL em `qualifyCondition` | `TestQualifyConditionColumnIsNullWhenNotDeclared` |
| Cronômetro para grupo inelegível | `TestSweepWithQualifyWhenDoesNotAllocatePerGroup` |
| Amostra de métrica sem `Family` | `TestAmostrasDeMetricaTemFamiliaENome` |
| Texto acima de `checkpoint.MaxAlarmStateText` aceito na subida | `TestAlarmTextAboveTheStateCeilingIsRefusedAtStartup` (subtestes `when` e `qualifyWhen`) |

O relato do Refuter registra mais mutações vermelhas sem o nome do teste que
reprovou — restauração sem a impressão ou sem o `qualifyWhen`, `disqualified`
não incrementado, `disqualified` sem `resolved`, runner ignorando a fonte das
métricas, linha ausente em `metricas.md`, truncar texto em vez de recusar e
colunas de métrica vazias no evento —, e uma verde, inofensiva: avaliar o
`qualifyWhen` de grupo rastreado com `when` falso custa uma avaliação e a
precedência mantém o motivo.

### Servidor e consulta local

| Mutação | Reprovou em |
|---|---|
| Sem `forCounterMetrics` no `encerra()`, ou depois do `closeDatasets` | `TestLacosDoServidorParamAntesDoQuiesceDosDatasets` |
| Fonte nil no `counters.Register` e no alarme | `TestWindowMetricsFeedTheTableTheAlarmAndSurviveARestart` (`auth_5m_avg = <nil>`, esperado 12) |
| Restauração sem publicar | `TestWindowMetricsFeedTheTableTheAlarmAndSurviveARestart` (`auth_24h_max = <nil>` antes da primeira passada) |
| Série `sampling` só com o laço de pé; mapa de posições emitido por métrica | `TestCounterMetricSeriesArePublishedBeforeTheFirstPass` |
| Adaptador embrulhando `(*Metric)(nil)` | `TestMetricSourceReturnsNilInterfaceForUndeclaredMetric` |
| Consulta local abrindo em modo escrita | `TestLocalQueryWithoutWindowMetricFilesAnswersNull` (criou o diretório) — o teste de bytes do `.win` continuou verde; quem pega é o do diretório |
| Servidor sem `RemoveOrphans` quando não há métrica | `TestRemovingTheLastWindowMetricRemovesItsFilesOnTheNextStart` |
| Consulta local limpando órfãos | `TestLocalQueryNeverRemovesWindowFilesOfARemovedMetric` |
| Abertura que falha sem fechar o amostrador; subida abortada sem `forCounterMetrics` | `TestDatasetFailingAfterTheSamplerOpenedLeavesNoWindowFileMapped`, `TestAbortedStartupLeavesNoWindowFileOfAnOpenedDatasetMapped` (as duas ficaram VERDES na primeira suíte e só a sonda do Refuter pegou; os testes nasceram dessa reprovação) |
| Logger da consulta local descartando o WARN | `TestLocalQueryWarnsOnStderrWhenAWindowFileIsUnreadable` |
| Arquivo ausente voltando a gerar log | `TestMetricAnswersEveryDeclaredMetricInBothModes`, `TestLocalQueryWithoutWindowMetricFilesAnswersNull` |

## A bancada, número a número

Cada número desta seção traz a ORIGEM, e as duas não se misturam:

- **medido** é o que a rodada observou no processo ou no disco: a duração da
  passada (`ns/op`), o heap vivo por `runtime.MemStats.HeapAlloc`, o tamanho e
  os blocos alocados de um `.win` pelo `stat`, e o estado que o teste lê do
  amostrador (capacidade do arquivo, quantidade de blocos de entradas);
- **contabilizado** é a conta do `Sampler.Stats` — blocos alocados × tamanho do
  bloco —, a mesma que alimenta a coluna `heapBytes` de `_meta.counter_metrics`
  e a série `nishidb_counter_metric_heap_bytes`. Ela não observa memória
  nenhuma, e a bancada a publica ao lado do medido para expor a diferença.

O diretório temporário do Go fica em `/tmp`, que nesta máquina é ext4 sobre
NVMe (não tmpfs).

### Custo por posição sob rotatividade

```
$ GOTOOLCHAIN=go1.26.5 GOPROXY=off go test ./internal/countermetrics/ \
    -run '^TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups$' -count=1 -v
=== RUN   TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups
    positions_test.go:50: ids emitidos 100100, capacidade 1024, blocos de entradas 1, heap 139264 B, mapa 409600 B
--- PASS: TestRotationKeepsFileAndHeapAtThePeakOfLiveGroups (0.15s)
PASS
ok  	github.com/nishisan-dev/nishidb/internal/countermetrics	0.161s
```

Com 10 grupos vivos e 100.100 ids emitidos ao longo das passadas, o teste lê do
amostrador a capacidade do arquivo no piso (1.024 posições) e UM bloco de
entradas alocado — estado medido. Os bytes da mesma linha são contabilizados
pelo `Stats`: `heap 139264 B` é 1 bloco × 1.024 entradas × 136 B (o tamanho do
tipo da entrada), e `mapa 409600 B` é 25 blocos de 4.096 ids × 4 B. Só o mapa
cresce com os ids; endereçado pelo id, o arquivo precisaria de capacidade para
os 100.100.

### Passada, heap e disco no formato do piloto

`BenchmarkSampleOncePilotShape`: 2.830 grupos vivos, as duas métricas do plano
(`auth_5m`, 5m com `avg`; `auth_24h`, 24h com `max`), duas passadas de
aquecimento antes da medida. Três regimes: `sameEpoch` (o relógio não anda,
nenhum balde fecha), `pilotCadence` (o relógio anda o intervalo real de 5 s, e
toda passada fecha o balde da métrica de 5m) e `bothClose` (o relógio anda 24
min, e as duas métricas fecham balde em toda passada).

A medida de heap (`heap_measured_B`) é o crescimento do `HeapAlloc` entre
antes do `Open` e depois das duas passadas de aquecimento, com `runtime.GC()`
antes de cada leitura: é o heap vivo que o amostrador retém, com os temporários
da restauração já coletados. Contador, relógio, opções e logger nascem antes da
primeira leitura e ficam fora dela. `heap_accounted_B` é a contabilidade do
`Stats` (as entradas das duas métricas mais o mapa de posições, contado uma
vez), e `map_accounted_B` é só o mapa. `win_B` e `win_alloc_B` vêm do `stat` de
um `.win`.

```
$ GOTOOLCHAIN=go1.26.5 GOPROXY=off go test ./internal/countermetrics/ -run '^$' \
    -bench '^BenchmarkSampleOncePilotShape$' -benchmem -count=3
goos: linux
goarch: amd64
pkg: github.com/nishisan-dev/nishidb/internal/countermetrics
cpu: Intel(R) Core(TM) 7 150U
BenchmarkSampleOncePilotShape/sameEpoch-12         	    3982	    269476 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861816 heap_measured_B	       152.3 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/sameEpoch-12         	    4376	    269951 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861304 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/sameEpoch-12         	    4462	    270014 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861304 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/pilotCadence-12      	      93	  11774753 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861544 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/pilotCadence-12      	     100	  11855834 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861528 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/pilotCadence-12      	     100	  11801179 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861304 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/bothClose-12         	      45	  23393284 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861304 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/bothClose-12         	      49	  23476253 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861528 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
BenchmarkSampleOncePilotShape/bothClose-12         	      50	  23463726 ns/op	    851968 heap_accounted_B	       150.5 heap_accounted_B/group-metric	    861528 heap_measured_B	       152.2 heap_measured_B/group-metric	     16384 map_accounted_B	   7933952 win_B	   7938048 win_alloc_B	       0 B/op	       0 allocs/op
PASS
ok  	github.com/nishisan-dev/nishidb/internal/countermetrics	10.933s
```

| Grandeza | Valor | Origem | Leitura |
|---|---|---|---|
| Passada sem balde fechando (`sameEpoch`) | **0,27 ms** (269,5–270,0 µs, 3 rodadas); 0 allocs/op | medido | ≈ 48 ns por grupo × métrica: ler o grupo, dobrar a amostra e publicar sob seqlock |
| Passada na cadência real (`pilotCadence`, a cada 5 s) | **11,8 ms** (11,77–11,86 ms); 0 allocs/op | medido | toda passada fecha o balde de `auth_5m` para os 2.830 grupos e relê os 60 slots de cada um (169.800 leituras de balde); 11,8 ms a cada 5 s é ≈ 0,24 % de um núcleo, e a série `nishidb_counter_metric_sample_seconds` avisaria se passasse do intervalo |
| Passada com as duas métricas fechando (`bothClose`) | **23,4 ms** (23,39–23,48 ms); 0 allocs/op | medido | o teto: acontece uma vez a cada 24 min na janela de 24h, coincidindo com um fechamento da de 5m |
| Heap vivo retido pelo amostrador (duas métricas e o mapa de posições) | **861.304–861.816 B** (841 KiB); **152,2–152,3 B por grupo × métrica** | medido (`HeapAlloc` após `runtime.GC()`) | é o número a usar para dimensionar: 5.660 pares grupo × métrica |
| Heap contabilizado pelo `Stats` | 851.968 B; 150,5 B por grupo × métrica | contabilizado | três blocos de 1.024 entradas de 136 B (417.792 B) por métrica mais o mapa. Fica 9.336 a 9.848 B abaixo do medido: o diretório de blocos, as structs fixas e as fatias de trabalho não entram na conta. Só as entradas de UMA métrica, que é o que `heapBytes` publica, dão 417.792 B, ou 147,6 B por grupo |
| Mapa id → posição do contador | 16.384 B | contabilizado | um bloco de 4.096 ids × 4 B; está dentro do heap medido acima |
| Tamanho de um `.win` com 2.830 grupos | **7.933.952 B** (7,57 MiB) | medido (`stat`) | bate com a conta do formato para a capacidade 4.096 (a potência de 2 acima de 2.830): 4.096 B de cabeçalho + 4.096 × 16 B de identidade + 60 × 4.096 × 32 B de baldes |
| Blocos alocados em disco por um `.win` | **7.938.048 B** (7,57 MiB) | medido (`stat`, blocos × 512 B) | duas métricas: 15.876.096 B, ou 15,14 MiB em disco |

**O que a bancada não mede.** O leitor é o contador em memória dos testes, com
um mutex do contador inteiro tomado a cada `Group`, e não o `*counter.Counter`
do storage: o número de tempo é o custo do amostrador mais um lock por grupo. A
passada em `pilotCadence` inclui as primeiras escritas nas páginas de cada
coluna de balde, que a bancada não separa das seguintes. O heap medido é o do
regime depois do aquecimento; o pico transitório da restauração numa abertura
sobre disco povoado não é medido.

## O que não foi verificado

- **Durabilidade numa queda da máquina.** Nem o msync do `Stop` nem o fsync do
  diretório no crescimento foram exercitados contra queda de energia; a falha do
  fsync do diretório na criação e no reset do arquivo também não. Os testes
  cobrem a queda do PROCESSO e a ordem de escrita.
- **O leitor real na bancada** (acima).
- **`make lint-python`** não roda nesta máquina (faltam `ruff` e `mypy`). NÃO
  VERIFICADO — não passou nem falhou.
- **Os ITs dos clients** (`make test-jdbc`, `make test-python-it`) passaram nas
  rodadas do Builder e do Refuter dos blocos 6 e 7 (`DriverIT` com 13 testes e 0
  falhas; 27 OK no Python) e não foram rodados de novo nesta rodada de
  documentação.
- **O binário no piloto.** Nenhum deploy foi feito neste ciclo; o ponta a ponta é
  o teste de servidor com relógio injetado.

## O que ficou como débito

Registrados em [`debitos.md`](debitos.md): as colunas das métricas na linha do
`emit:` (124), a coluna de métrica no `bind` do enriquecimento (125) e a fonte de
métrica além de `total` (126). A razão entre contadores é a issue #61.

## Método

`GOTOOLCHAIN=go1.26.5 GOPROXY=off` em todo comando. A bancada e o teste de
rotatividade estão transcritos acima. Os diagramas
[`counter_metrics_flow.puml`](diagrams/counter_metrics_flow.puml) e
[`alarmes_flow.puml`](diagrams/alarmes_flow.puml) foram validados pelo servidor
`uml.nishisan.dev` (deflate cru e base64 no alfabeto do PlantUML): a forma
válida de cada um volta HTTP 200 sem erro no `/txt`, e uma cópia com uma linha
que não é sintaxe volta HTTP 400 com `Syntax Error?` — o validador discrimina.
