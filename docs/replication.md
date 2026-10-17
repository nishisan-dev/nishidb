# Replicação master/follower

Um dataset pode publicar uma fila de alterações para followers. A fila é
limitada e o master nunca bloqueia a ingestão por causa de um follower lento.
O bloco é fechado pelo primeiro limite atingido: intervalo, quantidade de
mensagens recebidas, tamanho do lote ou memória total retida.

```yaml
replication:
  enabled: true
  role: master             # ou follower
  masterURL: https://...   # obrigatório no follower
  usernameEnv: NISHI_REPL_USER # opcional no follower
  passwordEnv: NISHI_REPL_PASS # opcional no follower
  flushInterval: 5s       # 0s a 60s; começa na primeira alteração
  maxMessages: 10000      # conta antes da deduplicação
  maxBatchBytes: 8MiB
  maxBufferedBytes: 64MiB  # no master habilitado, pelo menos 3 × maxBatchBytes
  maxDiskBytes: 2GiB       # só no master; mínimo 8MiB e 4 × maxBatchBytes
  publishHistory: true     # só no master; padrão true
  targets: [replica_kafka] # opcional, só no master; nomes de kafkaTargets:
  periodicSnapshot: 24h    # opcional, só no master e só com targets; 1h a 8760h
```

Em todo master habilitado, com ou sem `targets`, `maxBufferedBytes` precisa ser
pelo menos o triplo de `maxBatchBytes`: três lotes coexistem no buffer (o
intervalo ativo, um lote na fila de gravação e um lote arrendado). Um lote
arrendado (pelo publicador enquanto o copia para o próprio buffer, ou por um
follower enquanto o envia) continua contando no buffer até ser solto, mesmo
depois de descartado; sem essa folga, a próxima escrita invalidaria o
intervalo retido. A carga recusa o dataset que não cumpre o piso.

As três chaves abaixo só valem em master habilitado (`enabled: true`,
`role: master`); num follower ou com `enabled: false` a carga as recusa,
nomeando a chave, mesmo `publishHistory: false`.

- `maxDiskBytes`: teto de disco do feed (ver [Feed em disco](#feed-em-disco)),
  com padrão de 2 GiB. Precisa ser de pelo menos 8 MiB e de pelo menos
  4 × `maxBatchBytes`; valor vazio ou abaixo do piso é recusado.
- `publishHistory`: padrão `true`; aceita só booleano.
- `periodicSnapshot`: de `1h` a `8760h` (365 dias), em segundos inteiros.
  Exige `targets`. Omitida, fica desligada, e `0s` é recusado: não desliga,
  nem é arredondado. Fração de segundo, valor fora da faixa e sufixo `d`
  também são recusados.

### Feed em disco

O master grava o feed em `<dataset>/replication/feed/`, num log de segmentos:
cada lote fechado vira um quadro com CRC, e cada segmento tem até
`maxDiskBytes/8` (entre 1 MiB e 64 MiB). A gravação é de uma goroutine própria
do feed, nunca da ingestão. A retenção é por bytes: quando o log passa de
`maxDiskBytes`, os segmentos mais antigos são apagados inteiros e o piso do
feed sobe. É esse piso, e não a memória, que limita até onde um follower ou um
publicador pode estar atrasado.

A memória (`maxBufferedBytes`) é um cache dos lotes mais recentes. Um lote só
sai dela depois de gravado no disco, e sair dela não cria lacuna: quem pedir um
cursor anterior lê do disco, com quadros consecutivos agrupados até
`maxBatchBytes` e `maxMessages`. Erro de gravação (disco cheio, EIO) não para a
ingestão: o lote fica na memória e a gravação é retentada, de 100 ms a 5 s; a
lacuna só vem se a memória lotar de lotes ainda não gravados. Uma falha de
`fdatasync` declara a lacuna na hora, porque o log fica inservível até ser
apagado.

`targets` lista destinos Kafka que também recebem o feed, além dos followers.
Cada item é o nome de um destino declarado na lista global `kafkaTargets:` do
`nishidb.yaml`, com a mesma regra de nome (`a-z`, `0-9` e `_`, até 64
caracteres). `file` não vale aqui: a replicação não tem saída em arquivo. A
chave só é aceita com `enabled: true` e `role: master`; lista vazia, item
repetido ou item que não seja texto são recusados na carga do dataset. O
comportamento da publicação é descrito em
[Publicação em Kafka](#publicação-em-kafka).

Updates consecutivos da mesma entidade são consolidados no estado completo já
materializado. A consolidação nunca cruza outra operação: `insert, update,
update, delete` produz `insert, update, delete`, enquanto `update, delete,
insert, update` conserva as quatro operações. Linhas de histórico têm IDs
distintos e não são fundidas.
O avanço da retenção histórica viaja como `retain`, inclusive no snapshot, para
que o follower mantenha a mesma marca de visibilidade do master.

Cada entrada leva o instante da publicação em `at`; na consolidação vale o da
última alteração. O `delete` carrega o último estado vivo da linha, e não a
lápide, tanto na expiração pelo coletor quanto na expiração oportunista da
compaction; a cópia da compaction e o replay da recuperação não publicam nada.
Voltar a escrever uma chave apagada publica `update`, não `insert`: a linha
mantém o mesmo `rowID`. Uma linha cujo TTL já passou, mas que o coletor ainda
não recolheu, já some das consultas e do snapshot. Por isso um consumidor pode
receber o `delete` de uma chave que nunca viu.

O follower começa pelo `GET /v1/replication/cursor`. Se a geração e o cursor
ainda estiverem cobertos, ele lê `GET /v1/replication/batch` e confirma cada
lote depois de aplicá-lo. Se a geração mudou ou a retenção criou uma lacuna,
recebe conflito e deve chamar `GET /v1/replication/snapshot`.

Depois que a fila é fechada, os lotes ainda retidos continuam sendo entregues,
inclusive o último, fechado pelo próprio encerramento. Quando não resta nenhum
lote depois do cursor, a leitura responde na hora que a fila foi fechada, sem
esperar.

O campo `invalidations` do status conta as perdas do intervalo inteiro, o
ativo e o retido. Isso acontece por invalidação explícita; quando um registro
não cabe na memória nem depois de liberar os lotes já gravados; quando o
disco não consegue gravar um lote (quadro grande demais, falha de `fdatasync`);
ou quando um lote retido no disco não pode ser lido (quadro corrompido). A
remoção de lotes antigos da memória conta em `evictedBatches` e não cria
lacuna; a retenção do disco sobe o piso sem contar aqui. Followers que ficaram
atrás do piso precisam ressincronizar.

O snapshot carrega as versões visíveis do dataset e da tabela histórica junto
com um cursor. O follower aplica esse estado pelo writer normal, reaplica o
tail depois do cursor e libera consultas apenas quando chega a `following`;
durante o resync o estado anterior não é exposto. Linhas históricas preservam
seu `rowID`, portanto uma sobreposição entre snapshot e tail é idempotente. Um
follower novo fica indisponível até concluir o primeiro snapshot.

O snapshot sai em streaming, sem ser montado inteiro na memória do master. O
cursor é lido uma vez, antes da varredura, e a exportação anda em blocos
curtos, limitados em linhas e em bytes, cada um com um snapshot próprio,
fechado antes de o bloco ser enviado. Assim a exportação não segura versões
antigas nem a retenção enquanto a rede anda, e a memória dela fica limitada a
um bloco. As linhas criadas depois do início ficam para o tail, e uma linha
alterada no meio da exportação pode sair num estado mais novo que o cursor: o
tail reaplica as mudanças seguintes por cima, e o resultado converge. Toda
entrada do snapshot leva `sequence` igual ao cursor e o mesmo `at`, o instante
em que a exportação começou. Se a exportação falhar depois de a resposta
começar, o master derruba a conexão em vez de fechar o JSON: a conexão cai
(sem resposta ou com corpo truncado), o follower não aplica nem poda nada, e
pede o snapshot de novo.

O estado da réplica passa por `empty`, `syncing`, `catching_up`, `following`,
`disconnected` e `resync_required`. O instante de escrita de cada linha vem no
payload do master, mas o instante com que o follower o compara é o do relógio
LOCAL: a visibilidade pelo TTL nas consultas e a expiração oportunista da
compaction do follower andam pelo relógio dele, e não pelo `at` recebido do
master. Uma desconexão longa, ou relógios desalinhados, fazem o follower
esconder ou expirar linhas que o master ainda mantém (débito 131).

As rotas de replicação exigem ADMIN, usam o TLS/autenticação do servidor e
retornam conflito em vez de pular uma lacuna silenciosamente. O cliente
reconecta com backoff quando o master fica offline. Promoção a master e
failover permanecem fora desta versão.

No modo `follower`, o servidor não consome Kafka nem executa o coletor de TTL,
alarmes, emissões ou reconstruções dependentes do relógio local; ele aplica os
lotes pelo writer do Store. Compaction local continua permitida para recuperar
espaço, e é ela que pode expirar uma linha pelo relógio local (ver acima).
O follower também não publica nada em Kafka: `targets` só vale no master.

## Publicação em Kafka

Destinos de replicação também não podem usar o tópico de um destino Kafka de alarmes em uso (`alarmDelivery.kafka`) no mesmo cluster. A subida recusa a mistura dos envelopes.

O master pode publicar o mesmo feed num tópico Kafka, em JSON, além de servi-lo
aos followers por HTTP. O publicador funciona como um follower interno: lê os
mesmos lotes, na mesma ordem, e escreve um registro Kafka por entrada (o
`retain` vira um registro em cada partição). Ele nunca
segura a ingestão. Se o Kafka ficar lento ou fora do ar, o feed segue gravando
no disco dentro de `maxDiskBytes`, e o publicador lê do disco o que atrasou; se
a retenção apagar o que ele ainda não leu, ele se recupera republicando o
estado inteiro no próprio tópico. O consumidor do tópico nunca precisa das rotas HTTP nem de
credencial ADMIN.

![Publicação da replicação em Kafka: reset, snapshot, snapshot_end e tail, com os caminhos de lacuna, falha de entrega e mudança de partições](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/replication_kafka_flow.puml)

### Configuração

Os destinos são declarados uma vez, na lista global `kafkaTargets:` do
`nishidb.yaml`. É a mesma lista que o `emit.target` dos contadores usa
([operacao-series-kafka.md](operacao-series-kafka.md)):

```yaml
kafkaTargets:
  - name: alarms_cdc
    bootstrapServers: ["broker:9092"]
    topic: fault.alarms.cdc
    timeout: 5s               # opcional; acima de 0 e até 5m, default 5s
    maxMessageBytes: 1MiB     # opcional; de 1KiB a 64MiB, default 1.000.012 bytes
    security:                 # opcional; TLS/SASL, igual ao consumidor
      protocol: PLAINTEXT
```

O dataset aponta para o destino pelo nome:

```yaml
replication:
  enabled: true
  role: master
  maxBatchBytes: 8MiB
  maxBufferedBytes: 64MiB   # pelo menos 3 × maxBatchBytes no master habilitado
  targets: [alarms_cdc]
```

Cada nome da lista vira um publicador próprio, com um produtor próprio. A
subida recusa, com exit 1 e uma mensagem que nomeia o problema:

- `targets` num dataset sem `enabled: true` ou com `role: follower`;
- lista vazia, nome repetido, nome inválido ou `file`;
- `maxBufferedBytes` menor que o triplo de `maxBatchBytes` (vale para todo
  master habilitado, com ou sem `targets`);
- destino que não existe em `kafkaTargets:`;
- destino que também é `emit.target` de algum contador. Um tópico carrega um
  formato de envelope só;
- destino usado pela replicação de dois datasets. Os marcadores de um dataset
  fariam o consumidor apagar as linhas do outro, então cada dataset precisa do
  seu tópico;
- destino da replicação que, com OUTRO nome, aponta para o mesmo tópico e o
  mesmo cluster de outro destino em uso, pela replicação ou por um contador
  (a regra vale quando ao menos um dos dois lados é destino da replicação);
- destino cujo tópico é o tópico de ingestão (`source.kafka.topic`) de algum
  dataset no mesmo cluster. O NishiDB reingeriria o próprio envelope como
  evento.

"Mesmo cluster" quer dizer ao menos um `host:porta` em comum entre os
`bootstrapServers`, comparados como texto. Dois nomes de DNS para o mesmo
broker, ou IP de um lado e nome do outro, não são detectados (débito 133).

O NishiDB não cria o tópico. Com o tópico ausente, o publicador fica em
`retrying` até ele aparecer. O tópico deve usar retenção por tempo
(`cleanup.policy=delete`): o publicador não escreve tombstone Kafka, e um
tópico compactado nunca apagaria as keys removidas. Fixe o número de
partições na criação. Aumentar esse número mais tarde força um novo snapshot
completo, porque a partição de cada key muda.

Um broker fora do ar não impede a subida: montar o publicador não fala com o
broker.

### O envelope

Um registro de dado tem sempre estes campos, nesta ordem:

```json
{"epoch":"0123456789abcdef0123456789abcdef","sequence":42,"dataset":"events","table":"events","op":"update","key":"ALM-1","at":"2026-09-21T14:13:22.456Z","record":{"identifier":"ALM-1","persistent":{"probe":"10.0.0.1","state":"open","tags":["a","b"]},"raisedAt":"2026-09-21T14:13:20.123Z","sev":5}}
{"epoch":"0123456789abcdef0123456789abcdef","sequence":43,"dataset":"events","table":"events","op":"delete","key":"ALM-1","at":"2026-09-21T14:13:22.456Z","record":{"identifier":"ALM-1","persistent":{"probe":"10.0.0.1","state":"open","tags":["a","b"]},"raisedAt":"2026-09-21T14:13:20.123Z","sev":3}}
```

As duas linhas são bytes do teste de referência do envelope
(`internal/replicationkafka/envelope_test.go`). No schema desse teste,
`severity` tem o alias `sev`, `persistent` é uma coluna `json` e `probe_ip`,
extraída de `persistent`, é uma coluna derivada, que não entra no `record`.

- `epoch`: a geração do feed, 32 caracteres hexadecimais aleatórios. Muda a
  cada reinício do master, e só nele. O valor não tem ordem: a geração corrente
  é a do último `reset` visto na partição.
- `sequence`: a posição da entrada no feed. Cresce dentro de uma geração, mas
  tem buracos. Updates seguidos da mesma entidade são consolidados no último
  estado, e a posição dos intermediários some. Não use a contiguidade para
  detectar perda.
- `dataset`: o nome do dataset base, inclusive nas linhas da tabela histórica.
- `table`: o dataset base ou a tabela histórica dele.
- `op`: `insert`, `update`, `delete` ou `snapshot` nos registros de dado;
  `reset`, `snapshot_end` e `retain` nos marcadores de controle.
- `key`: na tabela base, a key da entidade; na histórica, o `rowID` em
  decimal. A key do registro Kafka é a autoritativa: ela leva os bytes crus
  da key, e o `key` do JSON troca byte de UTF-8 inválido por U+FFFD, de modo
  que duas keys binárias diferentes podem sair com o mesmo `key` no JSON.
- `at`: o instante da publicação no feed, em UTC com milissegundos, na mesma
  grafia dos timestamps das consultas. Na consolidação vale o da última
  alteração. No snapshot, todos os registros levam o mesmo `at`, o instante em
  que a exportação começou.
- `record`: a linha como o `SELECT *` da tabela a devolve. O nome é o alias
  quando existe, os valores saem com os mesmos formatos da saída NDJSON, e as
  colunas de sistema e as derivadas ficam de fora. Coluna ausente é omitida,
  e não sai como `null`: no NishiDB, NULL é ausência. Um campo `datetime`
  sai como instante ISO em UTC; um campo `string` sai como veio no evento.

O `record` difere do `SELECT *` em mais um caso. Uma coluna `json` sai inline
apenas quando o valor é JSON válido e UTF-8 válido; senão sai como string JSON,
com o byte de UTF-8 inválido trocado por U+FFFD. O caso
real é o `_raw` da tabela histórica para um evento que não pôde ser lido: ele
guarda os bytes do evento como chegaram, e inline tornaria o registro inteiro
um JSON inválido, que quebraria todo consumidor do tópico. O consumidor
distingue os casos pelo tipo do valor: objeto ou array é o documento; string é
o texto cru de um documento inválido.

O `delete` leva o último estado vivo da linha, tanto na expiração pelo coletor
de TTL quanto na expiração oportunista da compaction. Quando não há estado vivo
anterior (o delete de uma key que nunca existiu, ou de uma key já apagada), o
feed só tem a lápide, e o `record` sai vazio: `{}`.

Os marcadores de controle não têm `record`:

```json
{"epoch":"0123456789abcdef0123456789abcdef","dataset":"events","op":"reset","reason":"gap","cursor":100,"at":"2026-09-21T14:13:29.000Z"}
{"epoch":"0123456789abcdef0123456789abcdef","dataset":"events","op":"snapshot_end","cursor":100,"count":17,"at":"2026-09-21T14:13:29.000Z"}
{"epoch":"0123456789abcdef0123456789abcdef","sequence":46,"dataset":"events","table":"events_history","op":"retain","floor":8000,"at":"2026-09-21T14:13:22.456Z"}
```

- `reset` abre uma reconstrução. `reason` é `startup`, `gap`,
  `snapshot_failed` ou `partitions_changed`; `cursor` é o ponto do feed de onde
  o tail recomeça.
- `snapshot_end` fecha a reconstrução, com o mesmo `cursor` do `reset` e o
  `count` de registros `snapshot` que foram para AQUELA partição.
- `retain` avisa que a tabela histórica apagou as linhas com `rowID` menor que
  `floor`.

Os três usam a key fixa `__nishidb_control__` e são publicados uma vez em
CADA partição do tópico. Todo registro, de dado ou de controle, leva o header
`nishidb-op` com o `op`, para o consumidor rotear sem abrir o JSON. O `reset`
leva, além dele, o header `nishidb-reason` com o motivo (a mesma grafia do
`reason` do corpo).

### Ciclo de vida: reset, snapshot e tail

1. O publicador consulta quantas partições o tópico tem e fixa esse número.
2. Lê o cursor do feed e copia o primeiro bloco do snapshot; só então publica
   `reset` em todas as partições, e espera a confirmação de todas antes de
   publicar qualquer linha.
3. Publica o estado vivo inteiro como `op: snapshot`, um registro por linha: a
   tabela base primeiro e depois a histórica, precedida de um `retain` com o
   piso dela. Todo registro do snapshot sai com `sequence` igual ao `cursor`
   do `reset`.
4. Publica `snapshot_end` em todas as partições.
5. Segue o tail, lote a lote, com `insert`, `update`, `delete` e `retain`. O
   que o snapshot já cobriu (`sequence` até o cursor) não se repete.

O snapshot anda em blocos curtos, de até 4.096 linhas e com teto em bytes. Cada
bloco lê o dataset por um snapshot de leitura próprio, fechado antes de o bloco
ir para o Kafka, e no máximo dois blocos ficam em voo. Uma linha alterada
durante a exportação pode sair num estado mais novo que o cursor; o tail
reaplica as alterações seguintes por cima, e o estado converge.

Uma nova reconstrução começa por quatro motivos, que são o `reason` do `reset`:

- `startup`: a subida do master. **Todo reinício do master republica o dataset
  inteiro no tópico**, com geração nova. Não há checkpoint do publicador, e o
  tópico e o consumidor devem ser dimensionados para isso;
- `gap`: o feed descartou alterações que o publicador ainda não tinha lido,
  ou invalidou o intervalo retido, porque o Kafka ficou lento ou fora e o
  buffer do feed encheu. A geração
  continua a MESMA, e o `cursor` salta;
- `snapshot_failed`: um snapshot anterior falhou no meio, depois de já ter
  publicado alguma coisa;
- `partitions_changed`: a conferência periódica (uma vez por minuto) achou um
  número de partições diferente do fixado.

Um snapshot interrompido deixa no tópico um `reset` sem o `snapshot_end`
correspondente. O próximo `reset` recomeça a reconstrução do zero.

### Regras para o consumidor

Guarde UM estado por `(table, key)`, e não por key: a key da histórica é o
`rowID` e pode coincidir com a key de uma entidade da tabela base. Junto do
estado, guarde o `(epoch, sequence)` e a PARTIÇÃO do último registro aplicado.

- `insert`, `update` e `snapshot` são upsert. Um `update` de key desconhecida é
  válido: a ressurreição de uma key apagada chega como `update`, e não como
  `insert`, porque a linha mantém o mesmo `rowID`.
- Um `delete` de key desconhecida também é válido. Uma linha com TTL vencido e
  ainda não recolhida já some das consultas e do snapshot, e o `delete` dela
  chega depois.
- As gerações não têm ordem no valor, mas aparecem na MESMA ordem em todas as
  partições, porque cada `reset` sai em todas elas. Guarde a ordem em que os
  `reset` de cada geração foram vistos (em qualquer partição) e trate como mais
  antigo o registro cuja geração já foi superada por um `reset` visto. Descarte
  o registro de geração mais antiga que a do último aplicado àquele
  `(table, key)`, e o de mesma geração com `sequence` menor ou igual. Sem isso,
  uma key que muda de partição junto com uma troca de geração (partições
  aumentadas com o master parado) pode ser sobrescrita por um registro velho
  da partição antiga e depois podada. O mesmo critério de ordem vale para o
  "menor que `(epoch, cursor)`" da poda abaixo. Lembre também o `(epoch, sequence)` dos
  `(table, key)` apagados, até o próximo `reset`: uma nova tentativa do
  produtor pode reentregar um `update` antigo depois do `delete`, e sem essa
  memória a entidade volta a existir.
- Ao ver `reset` na partição P, entre em reconstrução nela. Ao ver o
  `snapshot_end` de P com o mesmo `cursor`, remova os `(table, key)` cujo
  último registro aplicado veio de P e cujo `(epoch, sequence)` seja menor que
  o `(epoch, cursor)` do `reset`. Todo `(table, key)` que apareceu no snapshot
  tem `sequence` igual ao cursor e fica. Não use "não apareceu nesta geração"
  como regra: num `reset` por lacuna a geração é a mesma, e os deletes
  perdidos na lacuna nunca seriam podados.
- A condição "veio de P" é o que protege a key que mudou de partição. Depois
  de `partitions_changed`, uma key pode sair do snapshot da partição antiga e
  entrar no da nova; se a nova terminar primeiro, o `snapshot_end` da antiga,
  sem essa condição, apagaria uma key viva.
- `count` permite conferir se a partição recebeu o snapshot inteiro: compare-o
  com os `(table, key)` distintos que chegaram como `snapshot` entre o `reset`
  e o `snapshot_end` daquela partição.
- Logo depois de uma reconstrução, uma key pode voltar por um instante a um
  estado anterior, quando o tail reaplica uma alteração que o snapshot já
  tinha alcançado. O estado final converge.
- A tabela histórica não tem ordem por entidade: linhas da mesma entidade caem
  em partições diferentes. Quem não precisa dela filtra por `table`.

### Entrega e ordem

A entrega é **at-least-once**. O produtor é idempotente e espera a confirmação
de todas as réplicas em sincronia (`acks=all`), mas uma nova tentativa depois de
timeout ou de cancelamento pode duplicar um registro. A ordem é garantida por
key dentro da partição; não há ordem entre keys diferentes.

A partição de um registro de dado é o murmur2 da key sobre o número de
partições fixado no último `reset`. O publicador a escolhe antes de produzir,
e é essa mesma escolha que o `count` do `snapshot_end` conta.

O cursor do publicador só avança depois de TODOS os registros do lote
confirmados. Se parte do lote falha, o publicador espera (de 200 ms a 5 s, em
progressão) e reproduz, em cada partição com falha, tudo a partir do PRIMEIRO
registro que falhou, inclusive os já confirmados. Reproduzir só os que falharam
poderia pôr um `update` antigo depois do `delete` da mesma key.

O publicador copia o lote para o próprio buffer e devolve o lote ao feed antes
de esperar o Kafka. Um broker travado não prende memória do feed, que os
followers compartilham. Descartar esse lote da memória enquanto a entrega tenta
de novo não cria lacuna: ele já está gravado no disco. A continuidade se perde,
com `reset` por lacuna, só quando a retenção do disco apaga lotes que o
publicador ainda não leu, ou o feed invalida o intervalo retido.

### Estados e falhas

O publicador está sempre num destes estados:

| Estado | Significado |
|---|---|
| `snapshotting` | reset, snapshot e snapshot_end em curso |
| `following` | tail acompanhando o feed |
| `retrying` | a entrega falhou por infraestrutura (broker fora, timeout, tópico ausente, mensagem recusada pelo broker); tenta de novo para sempre |
| `blocked` | um registro passou de `maxMessageBytes`; parado até o reinício |
| `failed` | uma entrada do feed não pôde ser convertida em JSON (bug); parado até o reinício |
| `stopped` | parado pelo encerramento do servidor |

Nenhum estado de falha afeta a ingestão nem os followers HTTP.

**`blocked`.** Antes de produzir um lote, o publicador mede cada registro (key,
valor, header e uma folga de 128 bytes para o enquadramento do Kafka) contra
`maxMessageBytes`. Se um passar do limite, nada do lote é produzido: pular o
registro deixaria no tópico um buraco que o consumidor não enxerga. O log
`replication.kafka.blocked` nomeia a key, o tamanho e o limite. Para sair,
suba `kafkaTargets[].maxMessageBytes` e o `max.message.bytes` do tópico, e
reinicie o servidor. A subida faz um snapshot novo.

**Recusa pelo broker.** Se o registro cabe em `maxMessageBytes` mas o broker o
recusa por tamanho (o `max.message.bytes` do tópico é menor), o erro é de
infraestrutura: o publicador fica em `retrying`, conta
`errors{reason="message_too_large"}` e volta sozinho quando o limite do tópico
for aumentado, sem reinício.

**`failed`.** O log `replication.kafka.failed` nomeia a `sequence` e a tabela
da entrada. É bug: pular a entrada faria o tópico divergir do dataset em
silêncio.

### Dimensionamento

Enquanto o snapshot anda, o feed tem de reter todas as alterações feitas desde
o cursor do `reset`: são elas que o tail entrega depois. Se o disco do feed
não comportar o que a ingestão produz durante um snapshot inteiro, o cursor se
perde, o publicador recomeça com `reset` por lacuna, e o próximo snapshot tende
a acabar do mesmo jeito. O publicador então republica o dataset em laço e nunca
alcança o tail.

A conta a fazer:

    maxDiskBytes > duração do snapshot × alterações por segundo × custo por entrada

O custo de uma entrada no disco é de cerca de 44 bytes, mais o nome da tabela,
a key e o payload dela (na memória, cerca de 192 bytes mais os mesmos campos). A consolidação de updates seguidos da mesma entidade reduz a conta. A
duração e o número de linhas de cada snapshot saem no log
`replication.kafka.snapshot.completed`. Em todo master habilitado,
`maxBufferedBytes` também precisa ser pelo menos o triplo de `maxBatchBytes`.

Quando o laço acontece, ele aparece de duas formas:

- o contador `errors{reason="snapshot_gap"}` sobe a cada snapshot abortado no
  meio porque o cursor se perdeu;
- o log de ERROR `replication.kafka.livelock` sai no máximo a cada 5 s, com
  `stage=snapshot` (perdeu no meio) ou `stage=tail_start` (perdeu entre o
  `snapshot_end` e o primeiro lote do tail), e com a duração do snapshot.

O remédio é subir `maxDiskBytes`, que é o que limita a janela retida (a
mensagem do log ainda cita `maxBufferedBytes`). A proteção atual só detecta o
laço.
Com `stage=snapshot`, o publicador espera 200 ms antes do próximo snapshot;
com `stage=tail_start`, recomeça na hora (débito 132).

### Observação

O `/v1/stats` traz, no dataset master com replicação ligada, o bloco
`replication`: `feed` (geração, sequência produzida, piso, bytes retidos,
entradas antes e depois da consolidação, lotes descartados, invalidações) e
`delivery`, um item por destino (estado, geração e cursor confirmados, atraso
em sequências, registros entregues, snapshots por motivo, erros por motivo,
última entrega e último erro). As métricas leem a mesma estrutura:

- por destino (`dataset`, `destination`):
  `nishidb_replication_kafka_delivered_total`,
  `nishidb_replication_kafka_lag_sequences`,
  `nishidb_replication_kafka_snapshots_total` (`reason`),
  `nishidb_replication_kafka_errors_total` (`reason`),
  `nishidb_replication_kafka_state` (`state`: uma série por estado, com 1 no
  corrente) e `nishidb_replication_kafka_last_success_timestamp`;
- do feed: `nishidb_replication_feed_input_entries_total`,
  `nishidb_replication_feed_output_entries_total`,
  `nishidb_replication_feed_buffered_bytes`,
  `nishidb_replication_feed_evicted_batches_total` e
  `nishidb_replication_feed_invalidations_total`.

Os motivos de snapshot e de erro e os estados existem desde a subida, em
zero, para que um alerta sobre eles funcione antes da primeira ocorrência. Os
detalhes de cada série estão em [metricas.md](metricas.md).

### Mudanças de contrato desde a v0.14

O envelope Kafka e o `/v1/replication/*` saíram na v0.14.x. Toda mudança de
contrato feita depois dela é aditiva: um consumidor que não conhece o item novo
continua lendo o que lia.

- **Header `nishidb-reason` no `reset`.** Todo `reset` sai agora com DOIS
  headers, `nishidb-op: reset` e `nishidb-reason: <motivo>`, o motivo na mesma
  grafia do `reason` do corpo (hoje `startup`, `gap`, `snapshot_failed` ou
  `partitions_changed`). O consumidor decide sem abrir o JSON. Os demais
  registros seguem só com o `nishidb-op`. Mudança aditiva: o corpo do `reset`
  não mudou.

**Defeito conhecido da v0.14.x.** Quando um lote do feed fechava por tamanho
(`maxBatchBytes`) antes de a alteração seguinte entrar, ele anunciava a
numeração dela sem incluí-la, e a alteração da fronteira se perdia no
publicador Kafka e no follower HTTP. O defeito foi corrigido a partir desta
versão. A v0.14.x, de uso interno, não recebe correção.

### Encerramento

No encerramento, os feeds fecham primeiro: o fechamento entrega o lote que
estava aberto, e os publicadores drenam o que resta e param. A drenagem tem
orçamento próprio de 10 s para todos os publicadores juntos, separado do prazo
do servidor HTTP. Se o prazo vence (broker fora, tópico lento), sai um WARN
`replication.publisher.close_failed` e o encerramento continua. Nada se perde:
o estado está no disco, e a próxima subida publica `reset` e um snapshot
completo. Um snapshot em curso no encerramento continua dentro do orçamento;
se o prazo vence antes do fim, ele é abandonado sem `snapshot_end`.
