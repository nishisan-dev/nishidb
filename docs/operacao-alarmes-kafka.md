# Entrega de alarmes para Kafka

Os eventos `alarm.firing` e `alarm.resolved` podem ser publicados num tópico
Kafka, com a mesma garantia do [webhook](operacao-webhook.md): fila durável em
disco por dataset e destino, ordem FIFO, nova tentativa sem fim enquanto o
Kafka estiver fora e o mesmo `eventId` em todas as tentativas. A mensagem é o
mesmo corpo JSON que o webhook recebe.

Esta entrega não é a [emissão periódica de contadores](operacao-series-kafka.md).
A série publica amostras sem garantia de entrega; os alarmes passam pela fila
durável.

## Configuração

Declare o destino no `nishidb.yaml`, ao lado dos webhooks:

```yaml
alarmDelivery:
  webhooks:
    - name: noc
      url: https://noc.example.com/alarms
  kafka:
    - name: olt_alarms
      bootstrapServers: ["kafka01:9092"]
      topic: olt_alarms
      timeout: 5s
      security:
        protocol: PLAINTEXT
      queue:
        maxBytes: 256MiB
```

Selecione o destino no alarme pelo nome, como um webhook:

```yaml
alarms:
  - name: olt_down
    when: "total = 0"
    sustainedFor: 5m
    notify: [olt_alarms, noc]
```

Webhooks e destinos Kafka dividem o **mesmo espaço de nomes**: um nome
repetido entre os dois é recusado na subida, e `notify` aceita qualquer um dos
dois tipos. Os nomes aceitam `[a-z0-9_]+`, até 64 caracteres.

| Chave | Default | Regra |
|---|---|---|
| `bootstrapServers` | — | lista não vazia de `host:porta` |
| `topic` | — | `[A-Za-z0-9._-]`, até 249 caracteres; não pode ser `.` nem `..` |
| `timeout` | `5s` | prazo de UMA tentativa, de 0 (exclusive) até `5m` |
| `security` | `PLAINTEXT` | mesmo contrato da ingestão e da série (abaixo) |
| `queue.maxBytes` | `256MiB` | mesmo default e mesmo piso do webhook |

Chave desconhecida no bloco, valor fora da faixa, referência desconhecida em
`notify`, credencial local ausente e certificado CA ilegível impedem a subida.
Broker indisponível **não** impede: o evento fica na fila até o broker voltar.

O tópico tem de existir. O produtor não pede criação automática, nem com o
broker configurado para criar tópicos; uma tentativa contra tópico inexistente
falha com a classe `unknown_topic`. Crie o tópico já com o número final de
partições: aumentar depois remapeia as chaves, e o disparo e a resolução de um
mesmo grupo podem passar a cair em partições diferentes.

### Segurança

O bloco `security` é o mesmo da [ingestão Kafka](operacao-kafka.md) e da
[série](operacao-series-kafka.md#segurança): `PLAINTEXT` (default), `SSL`,
`SASL_PLAINTEXT` ou `SASL_SSL`, com `PLAIN`, `SCRAM-SHA-256` ou
`SCRAM-SHA-512`. Credenciais são **nomes** de variáveis de ambiente, lidas na
subida; variável ausente ou vazia recusa a subida, e o erro nomeia a variável,
nunca o valor.

```yaml
security:
  protocol: SASL_SSL
  saslMechanism: SCRAM-SHA-512
  usernameEnv: NISHIDB_ALARM_KAFKA_USERNAME
  passwordEnv: NISHIDB_ALARM_KAFKA_PASSWORD
  caFile: /etc/nishidb/kafka-ca.pem
```

## Mensagem, chave e cabeçalhos

**Valor.** O mesmo corpo JSON que o webhook POSTa, byte a byte: o mesmo
`eventId`, os mesmos campos de negócio, `total` e `rows` no disparo e na
resolução. O contrato dos campos está no [guia do webhook](operacao-webhook.md#contrato-e-confirmação).

**Chave.** O array JSON compacto `[dataset, counter, groupKey]`:

```json
["service_state","pon","OLT_PON.BR_X_OLT01.4.14"]
```

O `groupKey` é o texto canônico do grupo, o mesmo do campo `groupKey` do
corpo. Mesmo grupo, mesma chave, mesma partição: o disparo e a resolução de um
grupo chegam em ordem. A chave distingue contadores homônimos em datasets
diferentes.

A chave **não** é igual à da série de contadores, que leva os valores do
`groupBy` tipados (`[dataset, counter, "OLT01", 4]`). A fila do alarme guarda
só o corpo do evento, onde os valores das colunas são texto; a identidade do
grupo, que é o que a partição precisa, é o `groupKey`.

**Cabeçalhos.** `eventId` (o mesmo valor do corpo e do `Idempotency-Key` do
webhook) e `event` (`alarm.firing` ou `alarm.resolved`). O consumidor pode
filtrar e deduplicar sem abrir o corpo.

**Timestamp do registro.** É o instante da produção, carimbado pelo cliente
Kafka, e **não** o `at` do evento. Um evento que esperou na fila durante uma
queda do broker sai com o horário da entrega; o instante do alarme está no
campo `at` do corpo.

## Entrega e garantia

A entrega é **pelo menos uma vez**. Cada tentativa produz um registro e espera
a confirmação das réplicas em sincronia (`acks=all`), com o prazo de
`timeout`. Só a confirmação remove o arquivo da fila (com o fsync do
diretório), e só então o próximo evento é tentado. Não há retentativa interna
do cliente Kafka: quem retenta é a fila, com recuo de 1 segundo até 5 minutos
e espera aleatória, igual ao webhook.

Há duas situações em que o mesmo evento chega duas vezes ao tópico, sempre com
o mesmo `eventId`:

- **timeout ambíguo** — o broker gravou o registro, mas a confirmação não
  chegou no prazo. A fila não tem como saber e tenta de novo;
- **queda depois do ack** — o processo caiu entre a confirmação e a remoção
  durável do arquivo; na subida o evento é reenviado.

**O consumidor deduplica pelo `eventId`.** Não há transação nem entrega
exatamente uma vez.

A fila é FIFO por **dataset e destino**. Um evento que o Kafka não aceita
segura os seguintes daquela fila até a correção; os outros destinos e a
varredura continuam.

### Queda do broker

Com o broker fora do ar, os eventos continuam sendo admitidos e gravados em
disco. `failedAttempts` cresce, o log `alarm.delivery.retry` informa a classe
da falha, e nada expira: quando o broker volta, a fila é entregue na ordem,
com os corpos e os `eventId` originais. Os limites de disco e o descarte com
`disk_full` são os mesmos do webhook.

### Erro permanente

Autorização negada (`unauthorized`), tópico inexistente (`unknown_topic`) e
registro recusado pelo broker (`rejected`) **prendem a fila para sempre**, como
um 4xx no webhook: não há fila de falhas separada nem descarte automático. O
operador vê `failedAttempts` crescendo e o WARN com a classe, corrige a causa
(ACL, tópico, limite do broker) e a fila anda sozinha, sem reinício.

## Limites

Valem os [limites do webhook](operacao-webhook.md#persistência-e-limites): o
buffer de memória, a contrapressão da admissão, os tetos de disco e o corpo
máximo de 1MiB. O Kafka acrescenta um teto próprio: **chave, corpo e
cabeçalhos somados acima de 999.884 bytes** são descartados na admissão como
`event_too_large`, sem chegar ao disco, para caber no lote de 1.000.012 bytes
do cliente. Sem esse teto o evento entraria na fila, seria recusado em toda
tentativa e prenderia os seguintes. Um limite menor do broker
(`max.message.bytes`) também recusa, mas só na tentativa, com a classe
`rejected`.

## Destino removido e troca de tipo

A fila é do **nome**. Remover o destino do `nishidb.yaml` pausa a fila e
preserva os arquivos (log `alarm.delivery.paused`); reintroduzir o nome a
retoma. Trocar o tipo mantendo o nome (um webhook `olt_alarms` que vira um
destino Kafka `olt_alarms`) entrega ao tópico o que estava pendente para o
HTTP, porque o corpo é o mesmo. Um evento que entrou na fila como webhook e
passa do teto do Kafka fica preso com a classe `rejected` depois da troca.

O transporte Kafka exige no corpo os campos que montam a chave e o cabeçalho
(`event`, `dataset`, `counter` e `groupKey`). Todo evento gravado pelo nishidb
os tem; um arquivo sem eles na cabeça da fila suspende o destino (estado
`failed`, como um arquivo inválido) e é preservado para investigação.

## Encerramento

No encerramento a tentativa em curso é cancelada sem esperar o broker, o
buffer de memória é persistido e o cliente Kafka é fechado. O evento que
estava em voo continua no disco e é reenviado na subida seguinte, com o mesmo
`eventId` — se o broker já o tinha gravado, é a duplicata descrita acima.

## Diagnóstico

O destino Kafka aparece em `datasets[].alarmDelivery` do `/v1/stats` com
`"kind": "kafka"` (o webhook sai com `"kind": "webhook"`; a fila pausada, sem
`kind`). Os campos e as [métricas de entrega de alarmes](metricas.md#entrega-de-alarmes)
são os mesmos do webhook, lidos da mesma estrutura, e nascem em zero; o tipo
não é rótulo de métrica.

O log `alarm.delivery.retry` traz o `destination` e uma `class` estável:

| Classe | Significado |
|---|---|
| `timeout` | a tentativa esgotou o `timeout`, inclusive host inalcançável ou broker que não responde |
| `unknown_topic` | o tópico não existe no cluster |
| `unauthorized` | o broker respondeu negando a autorização do tópico ou do cluster, ou a autenticação SASL; uma negociação SASL que o cliente repete até esgotar o prazo aparece como `timeout` |
| `rejected` | o broker recusou o registro de forma definitiva (por exemplo, acima do `max.message.bytes`) |
| `broker_unavailable` | conexão recusada, broker sem líder ou sem réplicas suficientes, e demais falhas transitórias |

Corpo, bootstrap e credenciais não entram nesses logs.
