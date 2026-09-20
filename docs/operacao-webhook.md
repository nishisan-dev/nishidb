# Entrega de alarmes por webhook

O webhook publica `alarm.firing` e `alarm.resolved` com os mesmos campos de
negócio do log. `notify` escolhe destinos nomeados por alarme; sem essa chave,
o alarme continua saindo apenas no log. Kafka não é um destino nesta versão.

As condições, o prazo e a escolha do valor publicado estão na
[referência de alarmes](../README.md#alarme-por-condição-sustentada), incluindo
o [exemplo de alarme por taxa com `measures`](../README.md#alarme-por-taxa-de-eventos-measures).
O mesmo `notify` pode ser usado nesses alarmes.

## Configuração

No `nishidb.yaml`:

```yaml
alarmDelivery:
  webhooks:
    - name: noc
      url: https://noc.example.com/alarms
      bearerTokenEnv: NISHIDB_NOC_TOKEN
      timeout: 5s
      queue:
        maxBytes: 256MiB
```

No alarme do YAML do dataset:

```yaml
alarms:
  - name: olt_down
    when: "total = 0"
    sustainedFor: 5m
    notify: [noc]
```

Nomes de destino aceitam `[a-z0-9_]+`, até 64 caracteres. Uma lista em `notify`
pode selecionar vários destinos; cada entrega progride independentemente.
A URL aceita HTTP ou HTTPS, sem usuário/senha embutidos nem fragmento. HTTPS
usa a validação TLS padrão. Redirecionamentos não são seguidos.

O token é opcional. Quando `bearerTokenEnv` está presente, a variável precisa
existir e conter um valor não vazio; o valor não vai para a fila nem para logs.
A configuração é aplicada na subida. URL indisponível não impede a subida;
configuração inválida, referência desconhecida e fila corrompida impedem.

Colunas de `groupBy` dos alarmes com `notify` não podem colidir com campos do
evento (`eventId`, `event`, `dataset`, `counter`, `alarm`, `groupKey`, `total`,
`condition`, `sustainedFor`, `since`, `at`, `reason`, `qualifyCondition`,
`valueColumn`, `firingValue`, `restored`, `downtimeMs`, `metrics`, `enrichment`)
ou do logger (`timestamp`, `level`, `component`, `message`, `time`, `msg`).
A recusa impede JSON com chaves ambíguas. Alarmes sem push mantêm a
compatibilidade anterior.

## Contrato e confirmação

Cada POST tem `Content-Type: application/json`, `Idempotency-Key` igual ao
`eventId` do corpo e, quando configurado, `Authorization: Bearer <token>`.
O identificador também sai no log. Campos próprios do logger — nível,
componente, timestamp do log e mensagem textual — não integram o corpo HTTP.
A supressão de chaves sensíveis, os tipos e a ausência dos campos opcionais
seguem o log. Métricas do evento mantêm seus valores textuais.

Em alarmes sobre `measures`, `valueColumn` identifica a medida escolhida em
`value` e `firingValue` é sua taxa numérica no disparo. As medidas disponíveis
também aparecem em `metrics`, como `"metrics":{"stop_rate":"30"}`.
Com `total: false`, o campo `total` é `null`; configure `value: stop_rate`
para que o disparo registre a taxa, pois o padrão de `value` continua `total`.
Consulte a unidade (`second`, `minute`, `hour` ou `day`) na configuração da
medida para interpretar o limiar e o valor recebido.

Somente HTTP 2xx confirma a entrega. Timeout, erro de rede e todos os outros
status, inclusive 400/401/403, retêm o evento e provocam nova tentativa.
O recuo cresce exponencialmente de 1 segundo até 5 minutos, com espera aleatória
entre metade e o valor atual. Um `Retry-After` válido em 429/503 adia a tentativa
pelo menos até o prazo indicado, mesmo acima de cinco minutos.

A fila é FIFO por **dataset e destino**. Um evento recusado pelo receptor
bloqueia os seguintes dessa fila até a correção. Não há expiração automática
nem fila de falhas separada. O receptor precisa deduplicar `eventId`: se ele
aceitar o POST e o processo cair antes de registrar a remoção durável local,
o mesmo corpo será reenviado com o mesmo identificador.

## Persistência e limites

A varredura apenas admite os corpos serializados em memória; um gravador
independente publica os arquivos em
`<dataset>/alarm-delivery/<destino>/`. São arquivos numerados de 0600 em
diretórios de 0700, sob o flock do dataset. Cada arquivo contém cabeçalho
versionado, tamanho, checksum CRC32C e corpo JSON imutável.

O POST só começa depois de `temp → fsync → rename → fsync do diretório`.
Um sucesso HTTP remove o arquivo e sincroniza o diretório antes de enviar o
próximo. O remetente e o gravador são independentes, então lentidão HTTP não
impede persistir os demais eventos.

| Limite por fila | Valor |
|---|---|
| Arquivos e reservas em disco | `queue.maxBytes`, default 256MiB |
| Quantidade de eventos e reservas | 50.000 |
| Buffer de memória, incluindo gravação em curso | 256 eventos e 8MiB |
| Corpo de um evento | 1MiB |
| Timeout de cada POST | `timeout`, default 5s, máximo 5m |

Os bytes de disco incluem o cabeçalho de 16 bytes de cada arquivo. O tamanho
mínimo configurável acomoda um corpo máximo mais seu cabeçalho. Blocos,
metadados e inodes do filesystem podem ocupar mais espaço que a soma dos bytes
dos arquivos. O limite de quantidade também restringe esse custo.

Quando qualquer limite impede admitir um evento, **o novo evento é descartado
somente para aquele destino**. Os anteriores permanecem. Um corpo grande
demais é recusado por inteiro, sem truncar enriquecimento. O log do alarme
continua sendo emitido. Reduzir `maxBytes` abaixo das pendências existentes
preserva os arquivos e impede novas admissões até a fila baixar.

A garantia durável começa na publicação confirmada do arquivo. Uma queda pode
perder eventos ainda em memória ou em gravação; não há contagem recuperável
exata dessas perdas. Não há transação entre a fila e `alarms.state`.
No encerramento normal, o buffer é persistido e HTTP é cancelado, sem esperar
pelo receptor. Uma falha de disco durante esse processo é devolvida ao operador.

## Reinício e intervenção

A subida valida os arquivos publicados e retoma pendências em ordem, mesmo
que o alarme tenha sido removido. Temporários de publicação interrompida são
removidos; um arquivo publicado inválido impede a abertura e é preservado para
investigação. Erros de gravação, leitura ou remoção durável durante a operação
suspendem o destino e exigem corrigir o armazenamento e reiniciar. Os demais
destinos e a varredura continuam funcionando. Novas emissões para a fila falha
são recusadas e contadas. Uma escrita que falhou depois do rename pode reaparecer
na recuperação, por isso não é contabilizada como perda certa.

O **nome** identifica o destino. Alterar a URL encaminha suas pendências à
nova URL. Remover o destino global pausa e preserva a fila; reintroduzir o mesmo
nome a retoma. Remover apenas `notify` interrompe novas admissões, mas permite
drenar pendências enquanto o destino continuar configurado. A recuperação
opera sobre datasets ainda declarados no servidor; um dataset removido da
configuração mantém seus arquivos sem remetente.

O motor mantém seu comportamento de reanunciar um alarme ativo restaurado:
esse anúncio tem novo `eventId` e `restored: true`. É diferente de uma
retentativa da fila, que preserva a identidade anterior. Para consultar o estado
corrente, use `<schema>.alarms` ou `_counters.alarms` no dataset da consulta.

## Diagnóstico

`/v1/stats` acrescenta `datasets[].alarmDelivery`, uma lista por destino. Os
campos vêm da mesma estrutura que alimenta as séries de
[entrega de alarmes](metricas.md#entrega-de-alarmes-por-webhook). Gauges refletem
a recuperação; contadores acumulam desde a subida. A idade considera o evento
durável mais antigo, a partir do seu `at`.

Observe `disk_events`, `oldest_pending_seconds`, `failed_attempts_total`,
`persistence_errors_total` e `dropped_total`. `active=1` significa remetente
ligado, inclusive quando está esperando para retentar; não garante receptor
saudável. Estados possíveis no JSON: `ready`, `running`, `paused`, `failed`,
`closed`. As séries numéricas existem desde a abertura, inclusive com zero.

Logs operacionais: `alarm.delivery.retry` informa o status HTTP (zero para
falhas de transporte), `alarm.delivery.failed` informa falha local,
`alarm.delivery.paused` informa destino removido e `alarm.delivery.dropped`
resume descartes acumulados. O resumo é emitido pelo gravador a cada cinco
segundos quando houver mudança e também no encerramento; a varredura não
espera por esse diagnóstico. Tokens, URLs, corpos e respostas HTTP não entram
nesses logs.
