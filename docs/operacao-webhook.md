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
`rows`, `condition`, `sustainedFor`, `since`, `at`, `reason`, `qualifyCondition`,
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

Todo corpo — `alarm.firing` e `alarm.resolved` — traz `total` e `rows`: o
total do grupo e os seus membros (as linhas com o `groupBy` completo e dentro
do `scope` do contador, satisfaçam ou não o `where`), os dois lidos da MESMA
linha que decidiu o evento. `rows` nunca é `null`, nem com `total: false`; para
o grupo aposentado (`group_retired`) os dois são `0`. Com
`when: "total * 10 <= rows"`, o corpo do disparo diz de quantos membros o total
é fração, sem consulta extra.
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

A vaga no buffer de memória é devolvida quando o arquivo é publicado, não
quando o receptor confirma. Por isso a varredura nunca espera o HTTP.

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

Com o **buffer de memória cheio** (eventos ou bytes), a admissão **espera** o
gravador publicar um arquivo e liberar vaga, em vez de descartar. A espera
termina pela vaga liberada ou pela falha local do destino (o evento é contado
em `destination_failed`). O tempo esperado acumula em
`admission_wait_seconds_total`. Um disco lento para o alarme aparece ali, e
não como perda.

Numa rajada, como milhares de grupos disparando na mesma passada, a varredura
passa a durar aproximadamente **eventos × destinos × custo de uma gravação
durável**. Os destinos multiplicam porque cada evento é oferecido aos destinos
do `notify` em série, um depois do outro: a varredura só passa ao próximo
destino depois de admitir no anterior. Na medição do
[aceite](aceite-webhook.md), uma gravação custou de 2,8 a 3,6 ms,
cerca de 350 eventos por segundo por destino; 10.000 disparos para um destino
seguraram a varredura por perto de 30 segundos, e para dois destinos seriam
perto de 60.

Quando um limite **de disco** (bytes ou quantidade, contando as reservas em
memória) impede admitir um evento, **o novo evento é descartado somente para
aquele destino**, sem esperar. É o que impede um receptor fora do ar por dias
de travar a varredura: as pendências enchem o disco, e daí em diante os novos
eventos são descartados. O mesmo vale para um corpo grande demais, recusado por
inteiro, sem truncar enriquecimento. Os anteriores permanecem. O log do alarme
continua sendo emitido. Reduzir `maxBytes` abaixo das pendências existentes
preserva os arquivos e impede novas admissões até a fila baixar.

O motivo `memory_full` não é mais emitido desde que o buffer cheio passou a
esperar. A série continua publicada, sempre em zero, para não quebrar painéis
e alertas que a referenciam.

A garantia durável começa na publicação confirmada do arquivo. Uma queda pode
perder eventos ainda em memória ou em gravação; não há contagem recuperável
exata dessas perdas. Não há transação entre a fila e `alarms.state`.

No encerramento normal, a varredura em curso é **interrompida** e não levada
até o fim. A interrupção acontece antes do próximo grupo, enquanto a varredura
percorre os grupos do contador, ou antes da próxima resolução, enquanto ela
resolve os grupos que sumiram do contador. O que já foi decidido fica
coerente com o que foi emitido. O que ficou sem decisão não é resolvido nem
descartado. O estado é gravado em `alarms.state` com o cronômetro de todos
os grupos ainda acompanhados.

Na subida seguinte, o que venceu o prazo é anunciado como restaurado, com novo
`eventId` e `restored: true`, e o que ainda não venceu continua pendente. O
arquivo guarda só o cronômetro, não se o alarme já tinha disparado. Por isso,
um grupo disparado que sumiu do contador e cuja resolução não chegou a sair
antes da parada é descartado na subida **sem** evento `resolved`, como
acontece com todo grupo disparado que some com o processo parado.

Depois disso as filas são fechadas: o buffer de memória é persistido e o HTTP é
cancelado, sem esperar pelo receptor. Uma falha de disco durante esse processo é
devolvida ao operador.

O custo do encerramento numa rajada é, por dataset, o que está em curso (os
eventos do grupo corrente, um por alarme do contador que mude de estado, ou
uma resolução, cada um com até uma gravação durável por destino) mais a
persistência do que estiver no buffer: até 256 gravações por destino, cerca de
um segundo por destino no disco medido. Ele não depende do tamanho da rajada,
nem da quantidade de disparos nem da de resoluções, e por isso cabe no prazo de
parada do serviço. Como a varredura já parou quando as filas fecham, nenhuma
admissão está esperando nesse momento.

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

**Depois de toda subida do servidor, o receptor deve reconciliar os alarmes que
tem abertos contra essa view.** Há `alarm.firing` que nunca recebem o
`alarm.resolved` correspondente: o grupo disparado que some com o processo
parado, e — com mais frequência — os grupos de um lote de aposentadorias que o
encerramento interrompeu na fase dois. Em ambos, o grupo volta pendente na
subida e é descartado sem evento, porque o `alarms.state` guarda só o
cronômetro, não a fase. O alarme que o receptor ainda tem aberto e que não
aparece mais na view está resolvido.

## Diagnóstico

`/v1/stats` acrescenta `datasets[].alarmDelivery`, uma lista por destino. Os
campos vêm da mesma estrutura que alimenta as séries de
[entrega de alarmes](metricas.md#entrega-de-alarmes-por-webhook). Gauges refletem
a recuperação; contadores acumulam desde a subida. A idade considera o evento
durável mais antigo, a partir do seu `at`.

Observe `disk_events`, `oldest_pending_seconds`, `failed_attempts_total`,
`persistence_errors_total`, `dropped_total` e `admission_wait_seconds_total`
(`admissionWaitSeconds` no JSON). Esta última crescendo indica que a gravação
em disco não acompanhou uma rajada e que a varredura esperou por isso. `active=1` significa remetente
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
