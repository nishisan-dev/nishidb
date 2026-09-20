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
