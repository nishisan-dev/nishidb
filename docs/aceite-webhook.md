# Aceite — webhook durável de alarmes (#62)

Verificação em 19/09/2026, com Go 1.26.5 (a versão do CI) e golangci-lint
2.12.2. O [plano aprovado](../planning/webhook-de-alarmes.md) e o
[contrato operacional](operacao-webhook.md) registram escopo e limites.

## Resultados

- `GOTOOLCHAIN=go1.26.5 make test`: aprovado, incluindo os gates de medida.
- `GOTOOLCHAIN=go1.26.5 make test-race`: aprovado.
- Ajuste final da indicação de atividade após cancelamento: pacote de entrega
  e integração do webhook revalidados sob `-race`.
- Lint comparado entre HEAD e a árvore modificada em cópia temporária: mesmos
  330 apontamentos preexistentes, zero adicionados e zero removidos. O exit
  code do linter continua 1 pela baseline existente; nenhum apontamento novo
  foi incorporado à baseline.
- Diagrama validado localmente com PlantUML 1.2025.4, `-checkonly` aprovado.
  O renderizador remoto respondeu 403, portanto a validação usou o JAR local.
- A instalação padrão desta máquina usa Go 1.27.1; a validação definitiva usa
  o toolchain do CI. A diferença de escaping JSON observada com 1.27.1 está
  fora deste ciclo e não foi alterada no serializador de consultas.

## Provas principais

`TestAlarmWebhookSurvivesServerRestartAndPublishesSharedStats` produz um evento
pela varredura real, recebe 403, encerra o servidor e entrega o mesmo corpo
pela segunda instância, mesmo após remover a regra do dataset. Confere também
Bearer, `Idempotency-Key`, séries iniciais em zero e equivalência com `/v1/stats`.

`TestFanoutKeepsOtherDestinationRunningAndRemovedRouteStillDrains` prova que
uma fila quebrada não impede a outra e exercita pausa por remoção do destino,
retomada pelo mesmo nome e mudança de URL com pendência.

`TestRecoveryAtEveryPublicationStep` interrompe cada etapa da gravação atômica
real. Temporários desaparecem na recuperação; arquivos publicados preservam
corpo e identidade. A bancada simula interrupção do processo, não falha física
do filesystem nem queda de energia de hardware.

`TestQueueLimitsRejectOnlyNewEvents` bloqueia o disco e exerce separadamente os
limites de quantidade e bytes, em memória e em disco. Os eventos antigos
sobrevivem à reabertura. `TestSuccessfulDeliveryReclaimsAdmissionCapacity`
prova que a confirmação libera capacidade para novas admissões.

`TestCloseCancelsHTTPAndPreservesUnsentEvents` mantém o receptor bloqueado e
verifica cancelamento e recuperação das pendências. O teste concorrente de
admissão e Close inclui tanto um evento aceito quanto uma recusa posterior ao
encerramento para evitar um aceite vazio.

## Mutações

Cada mutação foi aplicada isoladamente, o teste indicado ficou vermelho, o
código original foi restaurado e a suíte voltou a ficar verde. Nenhuma mutação
permaneceu na árvore.

| Defeito introduzido | Teste que o detectou |
|---|---|
| Publicar na fila sem executar a gravação durável | `TestOfferNeverWaitsForDiskAndHTTPWaitsForDurablePublication` |
| Ignorar CRC32C de arquivo publicado | `TestCorruptPublishedRecordRefusesRecovery` |
| Desabilitar teto de disco/quantidade | `TestQueueLimitsRejectOnlyNewEvents` |
| Tratar 403 como confirmação | `TestPersistentHTTPErrorsKeepFIFOAndIdenticalBodies` |
| Avançar sem sincronizar a remoção | `TestSenderWaitsForDirectorySyncBeforeNextEvent` |
| Enviar o último pendente antes do primeiro | `TestHTTPAckWithoutDurableRemovalReplaysSameEventBeforeNext` |
| Suprimir a redação de chaves sensíveis no JSON | `TestJSONAndLogShareEventProjectionAndRedaction` |
| Omitir fechamento das filas no encerramento do servidor | `TestLacosDoServidorParamAntesDoQuiesceDosDatasets` |

Não há medição nova de vazão nem promessa de latência fixa para a varredura.
A propriedade verificada é não esperar pelo I/O de entrega; o custo prévio do
log e das consultas de enriquecimento permanece.

## Rodada de 30/09/2026 — rajada sem descarte por memória

Verificação com Go 1.26.5 (a versão do CI). O
[plano aprovado](../planning/alarm-delivery-burst.md) registra a decisão:
com o buffer de memória cheio, a admissão espera o gravador em vez de
descartar com `memory_full`. Os demais descartes não mudam, e `memory_full`
continua publicado, sempre em zero. A garantia antiga ("a admissão nunca
espera o disco") passa a valer só enquanto houver vaga em memória. A admissão
continua sem nunca esperar o HTTP.

O registro de 19/09 acima descreve `TestQueueLimitsRejectOnlyNewEvents`
exercendo também os limites de memória. Nesta rodada esse teste ficou só com
os limites de disco, e os de memória passaram a
`TestMemoryLimitsMakeAdmissionWaitForTheWriter`, que prova a espera.

### Resultados

- `GOTOOLCHAIN=go1.26.5 make test`: aprovado, 62 pacotes, incluindo os gates
  de medida.
- `GOTOOLCHAIN=go1.26.5 make test-race`: entrega de alarmes e servidor
  aprovados. A única falha da rodada foi
  `TestCompactionEmDatasetReabertoDevolveDiscoDeVerdade`, em
  `benchmark/ingest-bench`, que depende do relógio. Rodado isolado sob `-race`,
  passou três vezes seguidas. O pacote não usa a entrega de alarmes.
- golangci-lint 2.12.2 nos pacotes tocados: nenhum apontamento novo. O único
  apontamento é preexistente, num arquivo não alterado.

### Provas

| Garantia | Teste |
|---|---|
| Rajada de 1.024 eventos (quatro vezes o buffer) com os limites reais (256 eventos / 8MiB) não descarta, chega inteira ao disco e a recuperação a reencontra em ordem | `TestBurstLargerThanMemoryIsPersistedWithoutDrops` |
| Os dois tetos de memória fazem esperar o gravador, e a espera é contabilizada | `TestMemoryLimitsMakeAdmissionWaitForTheWriter` |
| O fechamento da fila acorda quem espera (`closed`), com o gravador ainda preso no disco, e a espera que terminou em descarte é contabilizada | `TestCloseWakesAdmissionWaitingForMemory` |
| A espera conta do primeiro instante sem vaga até a admissão, atravessando despertares sem vaga | `TestAdmissionWaitIsCountedAcrossWakeupsWithoutSpace` |
| O Stop no meio de uma rajada lenta de disparos volta sem decidir mais nenhum grupo depois do cancelamento, não resolve grupo não visitado, deixa a view com os disparos já emitidos, grava o cronômetro de todos, e a subida seguinte anuncia como restaurado o que venceu o prazo e mantém pendente o que não venceu | `TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups` |
| O Stop no meio de um lote de resoluções (300 grupos disparados que somem na mesma passada) volta sem decidir mais nenhuma depois do cancelamento; as resolvidas saem do estado e da view, as demais ficam gravadas e, ainda ausentes na subida seguinte, são descartadas sem evento | `TestStopDuringRetirementBatchAbortsPhaseTwo` |
| A passada não aloca por grupo também pelo caminho interrompível que o laço usa | `TestInterruptiblePassDoesNotAllocatePerHealthyGroup`, `TestInterruptibleSweepWithQualifyWhenDoesNotAllocatePerGroup`, `TestInterruptiblePendingGroupsWithDeclaredValueDoNotAllocatePerPass` |
| A falha local acorda quem espera (`destination_failed`), venha do gravador ou do remetente | `TestQueueFailureWakesAdmissionWaitingForMemory` |
| Sem vaga em disco o descarte é imediato, com a memória livre ou cheia | `TestDiskLimitsDropWithoutWaiting` |
| Receptor que nunca responde não segura uma rajada quatro vezes maior que o buffer | `TestBurstNeverWaitsForHTTP` |
| 2.000 grupos disparando na mesma passada chegam todos ao receptor, sem descarte; a espera nasce em zero e sai igual no `/v1/stats` e no `/metrics` | `TestAlarmBurstInOnePassReachesWebhookWithoutDrops` |
| Projeção da série de espera | `TestDeliveryMetricsProjectDistinctValuesAndStates` |

### Mutações

Cada mutação foi aplicada isoladamente sobre o código de produção. O teste
indicado ficou vermelho, o original foi restaurado e o mesmo teste voltou a
ficar verde. Nenhuma mutação permaneceu na árvore.

| Defeito introduzido | Teste que o detectou |
|---|---|
| Restaurar o descarte `memory_full` no lugar da espera | `TestBurstLargerThanMemoryIsPersistedWithoutDrops` (764 descartes em 1.024), `TestMemoryLimitsMakeAdmissionWaitForTheWriter`, `TestAlarmBurstInOnePassReachesWebhookWithoutDrops` (1.728 descartes) |
| `close` sem acordar quem espera | `TestCloseWakesAdmissionWaitingForMemory` |
| Falha da fila sem acordar quem espera | `TestQueueFailureWakesAdmissionWaitingForMemory/sender` |
| Falha sem acordar e gravador acordando só no sucesso | `TestQueueFailureWakesAdmissionWaitingForMemory/writer` |
| Esperar também por vaga em disco | `TestDiskLimitsDropWithoutWaiting` (os quatro casos) |
| Conferir o disco só depois de haver vaga em memória | `TestDiskLimitsDropWithoutWaiting` (casos com memória cheia) |
| Devolver a vaga em memória só na confirmação HTTP | `TestBurstNeverWaitsForHTTP` |
| Não acumular o tempo de espera | `TestMemoryLimitsMakeAdmissionWaitForTheWriter`, `TestBurstLargerThanMemoryIsPersistedWithoutDrops`, `TestAlarmBurstInOnePassReachesWebhookWithoutDrops` |
| `/metrics` lendo outro campo para a espera | `TestDeliveryMetricsProjectDistinctValuesAndStates`, `TestAlarmBurstInOnePassReachesWebhookWithoutDrops` |
| Série de espera ausente | `TestDeliveryMetricsProjectDistinctValuesAndStates`, `TestAlarmBurstInOnePassReachesWebhookWithoutDrops` |
| Não contabilizar a espera que termina em descarte | `TestCloseWakesAdmissionWaitingForMemory` |
| Reiniciar a contagem da espera a cada despertar | `TestAdmissionWaitIsCountedAcrossWakeupsWithoutSpace` (0,105 s contados de 0,3 s esperados) |
| Gravador acordando quem espera só na falha | `TestBurstLargerThanMemoryIsPersistedWithoutDrops`, por prazo, com mensagem e sem travar o pacote |
| Varredura que não observa o cancelamento do Stop | `TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups` (Stop em 2,9 s, prazo 1 s, rajada inteira 3 s; corrigido, 16 a 18 ms) |
| Passada abortada que ainda roda a fase dois | `TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups` (`resolved` espúrio para um grupo já disparado não visitado). Depois que a fase dois passou a observar o cancelamento, este mutante sozinho fica verde: a fase dois para antes da primeira resolução. Somado à fase dois sem o cancelamento, fica vermelho de novo (50 decisões a mais depois do cancelamento) |
| Fase dois sem observar o cancelamento | `TestStopDuringRetirementBatchAbortsPhaseTwo` (Stop em 2,9 s, prazo 1 s) |
| Cancelamento observado só a cada 16 grupos | `TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups` (31 emissões contra 20 no cancelamento) |
| Cancelamento observado só a cada 64 grupos | `TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups` (63 emissões contra 20 no cancelamento) |
| Passada abortada sem republicar a view | `TestStopDuringBurstAbortsThePassWithoutResolvingUnvisitedGroups` (grupo disparado aparece pendente), `TestStopDuringRetirementBatchAbortsPhaseTwo` (grupo resolvido continua na view) |
| `fmt.Sprintf` por grupo só no caminho interrompível | os três gates `TestInterruptible…` (56 alocações por passada); os três gates antigos continuam verdes |

O subteste `writer` da falha não fica vermelho só com a falha sem acordar. A
falha do gravador é seguida da liberação da vaga do evento que falhou, e essa
liberação também acorda quem espera. O subteste `sender` é o que discrimina
essa mutação: o remetente falha com o gravador preso no disco.

A segunda revisão reprovou quatro pontos da correção do encerramento:
- a fase dois não era interrompível;
- os gates de alocação mediam só a passada sem cancelamento;
- a interrupção "antes do próximo grupo" não era aferida com contagem;
- a republicação da view na passada abortada não tinha teste.

O dublê agora segura a emissão de número 20 até o teste cancelar. Assim o
cancelamento acontece com uma contagem conhecida, e o teste exige que nenhuma
emissão a mais apareça depois dele. Na rodada dos mutantes, uma execução do
teste revertido falhou uma vez por prazo e não se reproduziu: o mesmo teste
passou 20 vezes seguidas e mais duas pela bancada de mutantes. Há suspeita de
carga na máquina, NÃO VERIFICADA.

A primeira revisão reprovou o encerramento. O servidor para os alarmes antes
de fechar as filas, e o Stop esperava a passada em voo até o fim. Numa rajada
com contrapressão, isso custaria eventos × destinos × gravação durável, e com
50 mil eventos passaria do `TimeoutStopSec` do serviço. O processo levaria
SIGKILL sem gravar o `alarms.state`. A varredura passou a observar o
cancelamento entre grupos e a abandonar a passada inteira, sem a fase dois. O
teste usa um destino dublê com 10 ms por emissão e 300 disparos. O fechamento da
fila continua acordando quem espera, mas no servidor ele chega com a varredura
já parada.

O diagrama `docs/diagrams/alarmes_flow.puml` passou a separar "buffer cheio:
espera o gravador" de "disco cheio: descarta e conta" e a anotar a interrupção
da passada. A validação usou o PlantUML local
(`jebbs.plantuml-2.18.1-universal/plantuml.jar -checkonly`), sem erro. O
validador foi aferido com uma cópia contendo uma linha fora de qualquer
sintaxe: ele respondeu `Some diagram description contains errors` (exit 200).
A cópia foi apagada em seguida.

O limite de corpo (1MiB) abaixo do buffer (8MiB) é verificado na compilação.
Um corpo maior que o buffer vazio esperaria para sempre. Com
`maxEventBytes = 9 << 20`, o pacote não compila (`constant -1048576 overflows
uint`).

### Vazão medida

`TestBurstPersistenceThroughputIsMeasured` (gate de medida, fora do `-race`)
mede a admissão e a persistência de uma rajada com os limites reais, sem
remetente. No `make test` a rajada tem 2.000 eventos. A de 10.000 eventos,
medida abaixo, roda com `NISHIDB_BENCH_DIR` (disco real escolhido) ou com
`NISHIDB_ALARM_BURST_FULL=1` (em `t.TempDir()`). O log informa o tamanho e o
diretório usados. Com a contrapressão, esse é o tempo que a passada leva numa rajada
desse tamanho. O teste não fixa teto; ele recusa uma medida sobre rajada que
perdeu evento.

```
NISHIDB_ALARM_BURST_FULL=1 GOTOOLCHAIN=go1.26.5 GOPROXY=off \
  go test ./internal/alarmdelivery/ \
  -run TestBurstPersistenceThroughputIsMeasured -count=3 -v
```

Medido em `t.TempDir()` (`/tmp`), num ext4 sobre NVMe local da máquina de
desenvolvimento, sem `NISHIDB_BENCH_DIR`:

| Rodada | Admissão + persistência | Eventos/s | Espera acumulada |
|---|---|---|---|
| 1 | 28,5 s | 351 | 27,9 s |
| 2 | 28,9 s | 346 | 28,1 s |
| 3 | 35,6 s | 281 | 30,3 s |

Cada evento custa uma gravação durável (temp, fsync, rename, fsync do
diretório), entre 2,8 e 3,6 ms nesse disco. Pelo laço real do servidor,
`TestAlarmBurstInOnePassReachesWebhookWithoutDrops` mediu a passada com 2.000
disparos entre 6,2 e 6,6 s, dos quais cerca de 6,0 s de espera. As duas
medidas são coerentes: sem gravação em grupo, uma rajada de 10.000 disparos
segura a varredura por cerca de meio minuto.
