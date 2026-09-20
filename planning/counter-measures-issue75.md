# Medidas de taxa e release conjunta — issue #75

Plano aprovado para a branch `codex/kafka-counter-emission`:

1. Contar toda aplicação aceita cujo estado resultante satisfaz o `where`,
   inclusive atualizações repetidas da mesma entidade. Usar o instante
   persistido, sem contar compactação, reconstrução ou eventos descartados.
2. Adicionar `measures` independentes de `emit` e `metrics`, com 60 baldes,
   até oito medidas, janelas de 1m a 30d e unidades segundo/minuto/hora/dia.
3. Manter `total` habilitado por padrão. `total: false` elimina o acumulador
   e mantém a coluna como NULL em todas as projeções.
4. Persistir gerações com o checkpoint; restaurar antes do replay; recusar
   estado durável ausente/corrompido. Alterações de definição começam vazias.
5. Permitir `source` de métricas apontar para uma medida. Preservar os tipos
   das métricas de população e usar DOUBLE para taxas e seus agregados.
6. Integrar SQL, alarmes, arquivo, Kafka e clientes; verificar persistência,
   concorrência, alocações, mutações, suíte completa e CI.
7. Abrir um PR conjunto com a emissão Kafka, integrar após CI verde e publicar
   `v0.11.0` pelo workflow de release existente.

Os comentários da issue pedem preservar `total` como campo mesmo desativado
e publicar medidas nos destinos de emissão; o contrato escolhido é NULL.
