# Aceite — medidas de taxa dos contadores

Rodada de 20/09/2026, Go 1.26.5, Linux amd64, Intel Core 7 150U.
Entrega conjunta com a emissão para destinos Kafka nomeados, na v0.11.0.
Plano: [counter-measures-issue75.md](../planning/counter-measures-issue75.md).

## Contrato verificado

- `TestMeasuresCountEventsAndRecoverWithoutCountingCompaction`: 1.800 eventos
  na mesma entidade, total habilitado/desabilitado, replay e copy-forward sem
  duplicação; expiração da janela e auditoria dos refs.
- `TestMeasuresRecoverAtEveryCheckpointBoundary`: interrupções após segSync,
  aplicação do overlay, msync dos contadores, snapshot das medidas e manifest.
- `TestMeasureCheckpointUncommittedGenerationIsIgnored`: geração não
  referenciada é ignorada e seu tail é reaplicado uma única vez.
- `TestMeasureSnapshotMissingOrCorruptRefusesOpen`: estado durável exigido
  ausente ou corrompido impede a abertura.
- `TestChangingMeasureWindowResetsWithoutBackfill` e
  `TestTogglingTotalPreservesMeasuresAndRebuildDoesNotCountEvents`: alterações
  de configuração e reconstrução não inventam ocorrências.
- `TestMeasuresExpireAtBucketBoundariesAndRetainEmptyGroups`: 70.000 eventos
  no mesmo balde, limites de janela, total sem vetor e aposentadoria apenas
  depois de todas as medidas expirarem.
- `TestMeasuresConcurrentReadersAndWriter`: leitores concorrentes e escrita.
- `TestMeasureSourceRestoresAndResetsOnlyWhenItsMeaningChanges` e
  `TestMeasureAlarmFingerprintChangesWithRateDefinition`: histórico amostrado
  e sustentação acompanham a definição da taxa.
- `TestMeasuresReachSQLFileKafkaAndDecayWithoutNewEvents`: ingestão com
  sequência, patches, condição, SQL, arquivo, Kafka real no broker de teste,
  alarme com valor fracionário e resolução após cessarem os eventos.
- `TestDisabledCounterTotalsAreNullOnAdministrativeResponses`: total nulo
  preservado nas respostas administrativas, com referências auditáveis.
- Integração Python: 28 testes contra o servidor real, incluindo valor
  fracionário, metadados DOUBLE e total NULL.
- JDBC: 211 testes, incluindo servidor real, getObject/getDouble/wasNull e
  metadados dos agregados de taxas.

## Validação final

`make test`, `make test-race`, `make build`, `make test-python`,
`make test-python-it`, `make test-jdbc` e `make lint-python` concluídos.
Lint Go 2.12.2 medido em cópia isolada: mesma contagem de `.golangci-baseline`,
sem apontamentos novos. Formatação e referências documentais verificadas.

A primeira rodada concorrida expôs a dependência temporal de um teste existente
de compactação: 200 ms não produziram eventos suficientes para selar um segmento.
O teste isolado e as suítes completas posteriores passaram sem alteração desse
harness. O gate do glossário apontou um cabeçalho duplicado, corrigido nesta
entrega antes da rodada final.

O primeiro CI apontou diferença intermitente de 28 B/query no gate existente
`TestResolucaoQualificadaNaoAlocaPorCampoDoDataset`. A medição de TotalAlloc
passou a aquecer o caminho e usar um único P, como testing.AllocsPerRun, mantendo
a igualdade exata entre 40 e 4.000 campos. Trinta execuções posteriores passaram.
Reintroduzir a montagem de colunas no roteamento continuou reprovando o gate:
2.880 contra 35.296 B/query. O código de produção de consultas não mudou.

## Custo medido

[Saída bruta do microbenchmark](../benchmark/baselines/2026-09-20-counter-measures.txt),
três rodadas de um milhão de atualizações por caso:

| Caminho em regime estável | Tempo por operação | Alocações |
|---|---:|---:|
| Manutenção do grupo sem ocorrência | 124–130 ns | 0 B / 0 |
| Mesmo grupo, duas medidas | 283–296 ns | 0 B / 0 |

É o núcleo do contador com grupo previamente internado; não mede ingestão
completa, criação de grupos, disco ou checkpoint. O estado da janela usa
60 × 16 = 960 bytes por posição/medida em mmap; mapa de posições em heap e
snapshot são custos adicionais. O snapshot é escrito por blocos, com um
vetor temporário de ids dos grupos; não é copiado inteiro para o heap.

Os gates `TestMeasuresDoNotAllocatePerEventOrRead`,
`TestCounterTableDoesNotAllocatePerRowWithMeasures` e
`TestAPassWithMeasuresDoesNotAllocatePerGroup` medem os novos caminhos,
separadamente dos gates anteriores de população e métricas históricas.

## Prova por mutação

Cada defeito foi introduzido isoladamente, o teste ficou vermelho e o código
original foi restaurado. A suíte correta voltou a passar.

| Mutação | Evidência que falhou |
|---|---|
| Contar apenas quando muda a entrada do vetor | 1 ocorrência em vez de 1.800 |
| Adotar a geração seguinte sem consultar o manifest | 3 ocorrências em vez de 2 |
| Não expirar baldes antigos | Janela vencida manteve 70.000 ocorrências |
| Emitir total zero quando desativado | Arquivo divergiu do NULL do SQL |
| Formatar o prefixo da medida por grupo com fmt.Sprintf | 128 alocações por passada de 64 grupos |
| Remover as medidas do teto do array de colunas | Acesso fora do array na linha com 50 colunas |

## Limites operacionais

A medida usa tempo de aplicação, não um timestamp arbitrário do payload.
Os 60 baldes limitam a resolução a window/60; o divisor é a janela inteira.
Sem sequência, uma reentrega aceita conta como outra aplicação. Sem eventos,
a taxa expira para zero. Rebuild e auditoria de população não recompõem o
histórico de eventos já descartado pela compactação.

A extensão crítica do manifest impede leitura por binário anterior; rollback
exige backup anterior à ativação. Ocorrências têm durabilidade do checkpoint
mais o tail íntegro. O histórico amostrado de avg/min/max mantém a política
anterior, inclusive possível perda do balde aberto num crash. A emissão Kafka
continua assíncrona, limitada e sem replay durável de saída.
