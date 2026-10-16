# Emissão de contadores para Kafka

Cada intervalo de `emit` publica uma mensagem JSON por grupo vivo do contador,
incluindo grupos com total zero. Um contador com 100 grupos emite 100 mensagens
em cada tópico selecionado. Grupos aposentados deixam de ser emitidos.

## Cadência: passadas nas fronteiras de `every`

As passadas acontecem nas **fronteiras** de `every`: os instantes UTC múltiplos
de `every` contados a partir da época Unix (1970-01-01T00:00:00Z). Com
`every: 300s`, as passadas saem às 15:00:00, 15:05:00, 15:10:00, e assim por
diante, qualquer que seja o horário da subida. Um `every` que não divide o dia
(`7m`, por exemplo) também se alinha à época, e não à meia-noite.

- **Subida.** Não há passada imediata. A primeira passada acontece na primeira
  fronteira igual ou posterior à subida. Emitir na subida repetiria o slot que o
  processo anterior já tinha emitido, com valores de outro instante.
- **Período fixo.** Uma passada que termina antes da fronteira seguinte não
  desloca a próxima: o período é `every`, e não `every` mais a duração da
  passada.
- **Passada atrasada.** Se a passada termina depois da fronteira seguinte, a
  próxima começa logo em seguida, com `at` igual à fronteira mais recente já
  alcançada. Fica no máximo uma passada pendente. As fronteiras intermediárias
  são **puladas**: não terão amostra. Elas somam em
  `nishidb_series_skipped_boundaries_total`, e cada ocorrência gera um único
  log WARN `series.boundaries_skipped`, com `dataset`, `counter`, `skipped`
  (quantas foram puladas), `at` da passada e `previous_pass_ms` (duração da
  passada anterior). Uma passada que atrasa sem pular fronteira não gera log; a
  duração aparece em `nishidb_series_pass_seconds`.
- **Relógio.** O `at` é estritamente crescente durante a vida do processo,
  inclusive se o relógio do sistema voltar (correção de NTP). Nesse caso, a
  espera vai até a fronteira seguinte à última emitida e pode ser maior que
  `every`. Se o processo ficar parado além da conta (VM pausada, relógio
  adiantado), a passada usa a fronteira mais recente e as anteriores são
  contadas como puladas.
- **Encerramento.** Uma passada interrompida pelo encerramento não é falha:
  não gera log de parada, não soma em `nishidb_series_errors_total` e não move
  o último sucesso. As linhas já gravadas no buffer do arquivo são escritas no
  fechamento, então o arquivo pode terminar com uma passada parcial daquele
  `at`.

A série em arquivo segue a mesma cadência e o mesmo `at`.

## Configuração

Declare os destinos na lista global `kafkaTargets:` do `nishidb.yaml`:

```yaml
kafkaTargets:
  - name: metricas_olt
    bootstrapServers: ["kafka01:9092"]
    topic: nishidb.contadores.olt
    timeout: 5s
    maxMessageBytes: 1MiB
    security:
      protocol: PLAINTEXT

  - name: metricas_backup
    bootstrapServers: ["kafka02:9092"]
    topic: nishidb.contadores.backup
```

A mesma lista serve à publicação da replicação em Kafka (`replication.targets`,
descrita em [replication.md](replication.md#publicação-em-kafka)). O destino
que é `emit.target` de algum contador não pode ser também destino da
replicação: o tópico levaria dois formatos de mensagem, e a subida recusa.

### Migração de `seriesDelivery`

Até a v0.14.0, os destinos ficavam em `seriesDelivery.kafka`. A chave foi
renomeada para a lista global `kafkaTargets:`, porque os mesmos destinos
nomeados servem a mais de um recurso. Não há período de convivência: um
`nishidb.yaml` que ainda traga `seriesDelivery:` é **recusado na subida**, com
exit 1 e a mensagem `seriesDelivery was renamed to kafkaTargets`. Para migrar,
mova os itens de `seriesDelivery.kafka` para `kafkaTargets:` sem alterar os
campos; os nomes usados em `emit.target` continuam os mesmos. A migração também
traz a chave opcional `maxMessageBytes`, descrita em
[Entrega e limites](#entrega-e-limites).

Selecione os destinos no contador do YAML do dataset:

```yaml
emit:
  every: 300s
  maxBytes: 5120MiB
  keep: 10
  target: [file, metricas_olt, metricas_backup]
```

`target` aceita um nome ou uma lista não vazia de nomes distintos. Ausente
equivale a `[file]`, preservando a emissão existente. `file` é reservado para o
arquivo local. Os demais nomes referenciam destinos globais e aceitam
`[a-z0-9_]+`, até 64 caracteres. O mesmo tópico pode receber vários contadores
e datasets; cada destino global utilizado possui um produtor compartilhado.

Para Kafka sem arquivo:

```yaml
emit:
  every: 300s
  target: metricas_olt
```

`file`, `maxBytes` e `keep` são recusados quando `file` não está selecionado.
Esses campos controlam exclusivamente o arquivo local. A retenção dos tópicos
é configurada no Kafka. No exemplo com arquivo, o teto local é
`5120MiB × (10 + 1)`; ele não limita o armazenamento do Kafka.

As configurações são aplicadas na subida. Referências desconhecidas,
configuração inválida, credenciais locais ausentes ou certificado CA inválido
impedem a subida. Broker indisponível não impede. Os tópicos devem existir;
o produtor não solicita criação automática.

### Segurança

O bloco `security` tem o mesmo contrato da ingestão Kafka. O protocolo default
é `PLAINTEXT`; também aceita `SSL`, `SASL_PLAINTEXT` e `SASL_SSL`.
Mecanismos SASL: `PLAIN`, `SCRAM-SHA-256` e `SCRAM-SHA-512`.

```yaml
security:
  protocol: SASL_SSL
  saslMechanism: SCRAM-SHA-512
  usernameEnv: NISHIDB_KAFKA_USERNAME
  passwordEnv: NISHIDB_KAFKA_PASSWORD
  caFile: /etc/nishidb/kafka-ca.pem
```

Credenciais são nomes de variáveis de ambiente; os valores são lidos na subida.
TLS valida o certificado e o nome do broker; sem `caFile`, usa as CAs do sistema.

## Mensagem e chave

```json
{"dataset":"telecom","counter":"autenticados_por_olt","groupKey":"BR_SPOAM_OLT_14","olt":"BR_SPOAM_OLT_14","total":812,"rows":905,"auth_5m_avg":807.5,"at":"2026-09-20T15:00:00.000Z"}
```

`total` são os serviços do grupo que satisfazem o `where` (812 autenticados);
`rows` são os membros do grupo — com o `groupBy` completo e dentro do `scope`,
satisfaçam ou não o `where` (905 serviços na OLT). `rows` nunca é `null`.

Cada mensagem contém seus nomes de coluna e sua identificação. Não há mensagem
separada de cabeçalho nem lote de linhas dentro do JSON. Os valores mantêm seus
tipos; métricas de janela sem amostras aparecem como `null`.
Medidas de taxa também entram na mesma linha, por nome, como números `double`.
Com `total: false`, a chave `total` permanece e seu valor é `null`. Por exemplo,
1.800 ocorrências em uma janela de um minuto, por segundo:

```json
{"dataset":"telecom","counter":"stops_by_olt","groupKey":"OLT_A","olt":"OLT_A","total":null,"rows":41,"stop_rate":30,"stops_24h_avg":12.5,"stops_24h_min":0,"stops_24h_max":30,"at":"2026-09-20T15:00:00.000Z"}
```

A persistência das ocorrências permite retomar o cálculo após reinício, mas não
altera a entrega desta série: mensagens de saída perdidas não são reemitidas.

`at` é a fronteira da passada em UTC, com milissegundos: sempre um múltiplo de
`every` contado a partir da época Unix (ver [Cadência](#cadência-passadas-nas-fronteiras-de-every)).
É o slot **nominal** da amostra. Os valores são lidos durante a passada, que
numa passada atrasada acontece depois do `at`. O mesmo `at` vai para o campo
`at` do arquivo e do Kafka, para o timestamp do record Kafka e para o instante
de cálculo das medidas de taxa. A passagem pelos destinos usa os mesmos valores
e timestamp, sem reler o contador por destino. O arquivo continua com o formato
anterior, sem acrescentar `dataset`.

A chave Kafka é o array JSON compacto `[dataset, counter, ...groupBy]`.
Por exemplo, para `groupBy: [olt, slot]`:

```json
["telecom","autenticados_por_pon","BR_SPOAM_OLT_14",2]
```

Os valores do grupo são tipados e seguem a ordem declarada. A chave independe
de `keyFormat` e distingue contadores homônimos em datasets diferentes. O
particionamento usa essa chave; a ordenação é por partição. Mudar o número de
partições pode mudar o destino de uma chave. Por isso, crie o tópico já com o
número final de partições: aumentar depois remapeia as chaves, e a série de um
mesmo grupo passa a ser dividida entre duas partições. Colunas que colidam com campos
fixos (`dataset`, `counter`, `groupKey`, `total`, `rows`, `at`) são recusadas para emissão
Kafka, evitando JSON ambíguo.

## Entrega e limites

Publicação assíncrona, com confirmação das réplicas em sincronia (`acks=all`).
Não há retentativa de publicação, transação, fila persistente nem replay após
reinício. A próxima passada produz amostras novas. O cliente pode atualizar
metadados e restabelecer conexões para essas novas publicações.

Por destino global, o buffer de transporte admite até **10.000 registros** e
**16 MiB de chaves e corpos**, incluindo registros em voo. Estruturas do cliente
e buffers de rede têm custo adicional. O buffer é um **limite de memória com
espera** (contrapressão), e não de descarte: quando ele está cheio, a passada
espera até que um registro em voo seja concluído (confirmado, expirado ou
recusado) e então continua. Nenhuma amostra é descartada por buffer cheio. A
capacidade é reservada antes de copiar a mensagem. Vários contadores que usam o
mesmo destino esperam juntos e todos avançam. O tempo de espera aparece em
`admissionWaitSeconds` e em `nishidb_series_kafka_admission_wait_seconds_total`.
`maxMessageBytes` define, por destino, o tamanho máximo do lote do produtor e,
por consequência, da mensagem. Aceita bytes puros ou unidade binária (`1MiB`,
`512KiB`), na faixa de 1 KiB a 64 MiB; valor fora da faixa, com unidade
decimal (`1MB`), zero ou vazio é recusado na subida, nunca arredondado. Ausente,
vale 1.000.012 bytes, o teto de lote padrão do cliente Kafka (franz-go) usado
antes de a chave existir; não confundir com o `message.max.bytes` do broker,
cujo default é 1.048.588. Chave mais corpo acima de `maxMessageBytes − 128`
bytes são descartados como `message_too_large` antes do envio, para reservar
espaço ao enquadramento do lote; com o default, o limite é 999.884 bytes. Como
uma amostra nunca cabe num buffer de 16 MiB se for maior que ele, o limite de
admissão também não passa de 16 MiB: com `maxMessageBytes` acima de 16 MiB, a
amostra maior que 16 MiB também é descartada como `message_too_large`, e não
como `buffer_full`. Subir o valor só adianta se o
broker e o tópico aceitarem mensagens desse tamanho (`message.max.bytes`,
`max.message.bytes`): limites menores do broker também recusam mensagens. Não
há truncamento.

`timeout` tem default de 5 segundos e aceita valores positivos até 5 minutos.
O prazo de cada mensagem começa quando ela entra no buffer, depois de qualquer
espera por espaço, e não no `at`. Isso é necessário porque o `at` é a fronteira
da passada, que já ficou no passado. Também há limites no dial e nas
requisições de rede, com margem de 100 ms e piso de 100 ms por requisição. O
cliente verifica cancelamento em pontos de execução; a conclusão de uma operação
em voo pode acontecer depois do prazo.

Erro de publicação, timeout ou mensagem excessiva encerram aquela entrega.
Um timeout significa **entrega não confirmada**: o broker pode ter armazenado
a mensagem antes de a resposta se perder. Os descartes não são uma contagem
exata de mensagens ausentes no Kafka.

Falha de um destino Kafka não interrompe a passada, mas um destino **lento ou
que não responde a atrasa**. A passada espera espaço no buffer daquele destino.
Esse atraso vale para os demais destinos do mesmo contador e para o arquivo
dele, porque todos recebem os grupos na mesma passada. O efeito depende de como
o destino falha:

- **Conexão recusada** (o host está de pé e o processo do broker não escuta).
  O cliente falha os registros logo, sem esperar o `timeout`. O espaço do
  buffer é liberado na hora, e a passada anda no ritmo normal, descartando as
  amostras daquele destino como `publish_error`. Numa verificação pontual
  durante o desenvolvimento, sem teste que a reproduza, as falhas chegaram em
  milissegundos.
- **Broker que aceita a conexão e não responde, ou host inalcançável** (o host
  sumiu ou a rede caiu: a conexão espera até o `timeout` de dial). Os registros
  em voo só expiram depois de `timeout`, e só então liberam espaço. A passada anda, mas
  no ritmo do `timeout`: até 10.000 registros por `timeout`. O cliente expira
  os registros em lote: todo o lote expira quando o primeiro registro atinge o
  prazo. Num contador com centenas de milhares de grupos e `timeout` alto, uma
  única passada pode levar horas. Esse tempo não foi medido.

As fronteiras puladas seguem a política de
[passada atrasada](#cadência-passadas-nas-fronteiras-de-every).
Falha no arquivo desativa apenas o arquivo até reiniciar; Kafka continua. No
modo exclusivamente em arquivo, a falha mantém a parada anterior do emissor.
No encerramento, os emissores param antes dos produtores e do armazenamento.
Uma passada que estava esperando espaço é interrompida. As pendências Kafka são
canceladas sem esperar pela recuperação do broker.

## Observabilidade

`/v1/stats` inclui `datasets[].seriesDelivery`, com uma entrada por contador e
destino: `counter`, `destination`, `delivered`, `dropped`, `pendingRecords`,
`pendingBytes`, `lastSuccessTimestamp` (segundos do epoch) e
`admissionWaitSeconds` (tempo acumulado esperando espaço no buffer do destino).
O campo do wire mantém o nome `seriesDelivery` após a migração para `kafkaTargets:`.
Os números nascem em zero e os contadores reiniciam com o processo.

Motivos de descarte: `buffer_full`, `message_too_large`, `timeout`,
`publish_error` e `shutdown`. `buffer_full` continua no vocabulário, mas a
entrada no buffer não o produz mais, porque espera em vez de descartar. Ele só
apareceria se o cliente Kafka recusasse por buffer cheio, e o limite do cliente
tem folga acima do limite local justamente para evitar isso. `shutdown` também
conta a mensagem que estava esperando espaço quando o encerramento a
interrompeu. Entregas confirmadas são contadas somente após o
callback de sucesso do Kafka. Os mesmos números alimentam as
[métricas de emissão](metricas.md#emissão-periódica-de-contador).

O andamento das passadas aparece em `nishidb_series_pass_seconds` (duração da
última passada concluída) e em `nishidb_series_skipped_boundaries_total`
(fronteiras puladas). As duas existem para todo contador com `emit`, inclusive
os que enviam somente para Kafka. Passadas mais longas que `every` atrasam as
seguintes e acabam pulando fronteiras.

O log `series.delivery.dropped` resume mudanças nos descartes a cada cinco
segundos e no encerramento, identificando dataset, contador, destino e motivos.
Corpos e credenciais não aparecem no diagnóstico. `series.file_stopped` indica
falha do arquivo quando outros destinos continuam ativos.
