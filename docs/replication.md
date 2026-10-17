# Replicação master/follower

Um dataset pode publicar um feed de alterações para followers e para tópicos
Kafka. O feed é gravado em disco, limitado por bytes (`maxDiskBytes`), com os
lotes mais recentes num cache em memória (`maxBufferedBytes`), e o master
nunca bloqueia a ingestão por causa de um follower lento, de um broker fora ou
do disco do feed. O lote é fechado pelo primeiro limite atingido: intervalo,
quantidade de mensagens recebidas ou tamanho do lote.

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
  maxDiskBytes: 2GiB       # só no master; de 8MiB (e 4 × maxBatchBytes) a 256GiB
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
  4 × `maxBatchBytes`, e de no máximo 256 GiB; valor vazio, abaixo do piso ou
  acima do teto é recusado, e não arredondado. O teto existe porque cada
  segmento do log mantém um descritor de arquivo aberto: com segmentos de
  64 MiB, 256 GiB são 4.096 descritores por dataset, e 1 TiB seriam 16.384.
  Para escolher o valor, veja [Dimensionamento](#dimensionamento).
- `publishHistory`: padrão `true`; aceita só booleano. Com `false`, a tabela
  histórica sai do feed inteiro: nem as linhas dela, nem o `retain` da
  retenção, nem o snapshot (HTTP e Kafka) a levam, e o
  `GET /v1/replication/cursor` anuncia `"historyExcluded": true`. O snapshot
  passa a ter só a base. Escrita num dataset sem `history:`, a chave não tem
  efeito (sem histórica o cursor já anuncia `historyExcluded`), e a subida
  avisa com o WARN `replication.publish_history_without_history`.
  Mudar a chave exige reinício, e o reinício abre uma geração nova (ver
  [Reinício limpo](#reinício-limpo)).
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

O feed não faz fsync durante a operação: os segmentos só são sincronizados na
rotação (o que fecha) e no encerramento limpo. Numa queda o feed é descartado
de qualquer jeito (ver [Reinício limpo](#reinício-limpo)). A memória do feed
fica limitada a `maxBufferedBytes`, mais o buffer de codificação do gravador
(até 2 × `maxBatchBytes`, retido entre um lote e outro; um quadro maior só
existe durante a própria gravação), mais um lote, e o buffer de leitura dele,
por leitor que lê do disco. Isso vale qualquer que seja `maxDiskBytes`: o
índice em memória tem uma entrada por MiB de segmento, e não uma por lote.

![Feed de replicação em disco: Publish sob o wmu, fila do gravador, gerações, log de segmentos com rotação e retenção por bytes, cache em memória, leitura da memória ou do disco, encerramento limpo, queda, subida (fresh, clean, crash) e rotação](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/replication_disk_feed.puml)

### Reinício limpo

O feed sobrevive ao reinício limpo do master: a geração (`epoch`) e a
numeração (`sequence`) continuam, e o que a vida anterior gravou continua
servível do disco. Um follower e um consumidor que estavam em dia seguem do
mesmo cursor.

No encerramento o feed grava, junto do estado dele, a marca de fechamento
limpo e a **impressão dos Stores**: a posição de escrita do dataset base e,
quando a histórica é publicada, a da tabela histórica (segmento, fim do último
registro, próximo `rowID` e piso da retenção). Na subida, depois da
recuperação dos dois Stores, a impressão gravada é comparada com a posição
recuperada. A geração continua só quando a marca existe, a impressão bate e o
fim do log confere. Qualquer outro caso é tratado como queda: o log é apagado,
nasce uma geração nova, a numeração volta a 0, e followers e publicadores
refazem o snapshot. Isso inclui:

- queda do processo, ou encerramento que não conseguiu gravar a marca limpa
  (WARN `replication.feed.close_failed` ou `replication.feed.closed_unclean`);
- mutação feita com o servidor parado, como um `nishidb ingest`, inclusive uma
  que mexa só na tabela histórica publicada;
- alteração que chegou ao feed depois de ele fechar (ERROR
  `replication.feed.publish_after_close`) ou antes de ele decidir a geração
  (ERROR `replication.feed.publish_before_resume`);
- troca de `publishHistory` num dataset com `history:`, que muda o que entra
  na impressão.

A decisão sai no log de subida, no INFO `replication.feed.resumed`, com o
`origin` (`fresh` na primeira subida, `clean` na retomada, `crash` na geração
nova depois de queda; `rotated` é a geração aberta em operação, e não sai na
subida), a `epoch`, a `sequence` e, quando a geração anterior é
conhecida e foi descartada, a `previousEpoch`. A subida que aborta depois de um dataset já ter decidido a
geração (outro dataset não abre, por exemplo) fecha o feed dele limpo: nada
mudou o Store, e a próxima subida retoma a mesma geração.

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
recebe conflito (409) e deve chamar `GET /v1/replication/snapshot`.

O follower que fica no ar enquanto o master reinicia de forma limpa atravessa o
reinício sem snapshot. No encerramento do master, o long-poll do batch responde
410 na hora (o master está saindo), e o follower vai para `disconnected`,
lembrando que estava em `catching_up` ou `following`. Quando o master volta com
a MESMA geração, o follower retoma o tail do cursor que já aplicou; com geração
nova, ou depois de uma desconexão vinda de outro estado, baixa o snapshot. Se a
retomada receber 409, cai no resync de sempre; um 410 é outra desconexão, e a
próxima tentativa retoma o tail de novo.

Num master com destino Kafka (`targets`), a retomada limpa só volta a servir
os followers depois da [conferência de rewind](#conferência-de-rewind) de cada
destino. O feed reaberto pode ser um backup restaurado, na MESMA geração e no
passado, e um follower que já aplicou `sequence`s acima da reabertura
receberia, como continuação, `sequence`s reusadas com outro conteúdo. Enquanto
a conferência não termina, o `GET /v1/replication/batch` e o
`GET /v1/replication/snapshot` respondem 503 (`INTERNAL`, com `Retry-After: 1`
e a mensagem dizendo que a conferência de rewind está em curso); o
`GET /v1/replication/cursor` continua respondendo. Para o follower, o 503 é
desconexão: ele guarda o cursor, tenta de novo com backoff e não pede snapshot
por causa dele. Se a conferência acha o rewind, o feed troca de geração ANTES
de voltar a servir, e o follower recebe o 409 da geração nova e refaz o
snapshot; senão, retoma o tail. Com a leitura do tópico falhando, a espera dura
o tempo da falha: o master não serve o feed às cegas. O feed novo, o que nasce
de queda e o master sem destino Kafka não esperam conferência nenhuma; neste
último o rewind continua invisível para o follower (débito 141).

Só falha de rede ou resposta HTTP de erro é desconexão. Uma alteração que o
Store do follower recusa (erro local) leva a `resync_required` e a um snapshot
novo, e não à retomada do tail que falhou; sai o ERROR
`replication.follower.apply_failed`, com a `sequence` e a tabela da entrada,
uma vez por ocorrência. Enquanto a falha se repete, o intervalo entre as
tentativas cresce de 200 ms até 5 s, mesmo com os snapshots dando certo, e só
volta ao mínimo quando o tail volta a andar.

O follower que declara `history:` exige que o master publique a histórica.
Diante de um master que anuncia `historyExcluded` (o master sem `history:` ou
com `publishHistory: false`), ele vai para o estado
`history_unavailable`, emite o ERROR `replication.follower.history_unavailable`
(uma vez por ocorrência) e não aplica nada: nem snapshot nem tail. Continua
consultando o cursor com backoff, e sai do estado quando o master volta a
publicar a histórica, pelo snapshot. Assim a irmã do follower não fica vazia em
silêncio.

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

O snapshot carrega as versões visíveis do dataset e, quando ela é publicada, da
tabela histórica, junto com um cursor. O follower aplica esse estado pelo writer normal, reaplica o
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
`disconnected`, `resync_required` e `history_unavailable`. O instante de escrita de cada linha vem no
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
estado inteiro no próprio tópico. O publicador guarda, por destino, até onde
o Kafka já confirmou (o checkpoint), e o reinício limpo do master retoma dali,
sem republicar o dataset. O consumidor do tópico nunca precisa das rotas HTTP nem de
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

- `epoch`: a geração do feed, 32 caracteres hexadecimais aleatórios. Muda
  quando o master sobe depois de queda (ou de mutação com ele parado); o
  reinício limpo mantém a mesma (ver [Reinício limpo](#reinício-limpo)). O
  valor não tem ordem: a geração corrente é a do último `reset` visto na
  partição.
- `sequence`: a posição da entrada no feed. Cresce dentro de uma geração, mas
  tem buracos. Updates seguidos da mesma entidade são consolidados no último
  estado, e a posição dos intermediários some. Não use a contiguidade para
  detectar perda.
- `dataset`: o nome do dataset base, inclusive nas linhas da tabela histórica.
- `table`: o dataset base ou a tabela histórica dele.
- `op`: `insert`, `update`, `delete` ou `snapshot` nos registros de dado;
  `reset`, `snapshot_end`, `resume` e `retain` nos marcadores de controle.
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
{"epoch":"fedcba9876543210fedcba9876543210","dataset":"events","op":"reset","reason":"crash","previousEpoch":"0123456789abcdef0123456789abcdef","cursor":0,"at":"2026-09-21T14:20:00.000Z"}
{"epoch":"0123456789abcdef0123456789abcdef","dataset":"events","op":"snapshot_end","cursor":100,"count":17,"at":"2026-09-21T14:13:29.000Z"}
{"epoch":"0123456789abcdef0123456789abcdef","dataset":"events","op":"resume","cursor":250,"at":"2026-09-21T15:02:11.318Z"}
{"epoch":"0123456789abcdef0123456789abcdef","sequence":46,"dataset":"events","table":"events_history","op":"retain","floor":8000,"at":"2026-09-21T14:13:22.456Z"}
```

- `reset` abre uma reconstrução. `reason` é um dos dez motivos de
  [Motivos do reset](#motivos-do-reset); `cursor` é o ponto do feed de onde o
  tail recomeça. `previousEpoch`, logo depois de `reason`, é opcional: sai só
  quando o `reset` troca de geração, com a última geração cujo `reset` já saiu
  neste destino (inclusive a de um snapshot que falhou no meio), e é omitida
  no primeiro `reset` do destino e no `reset` dentro da mesma geração (`gap`,
  por exemplo).
- `snapshot_end` fecha a reconstrução, com o mesmo `cursor` do `reset` e o
  `count` de registros `snapshot` que foram para AQUELA partição.
- `resume` marca a retomada do tail depois do reinício limpo do master, na
  MESMA geração: nenhum snapshot se segue, e `cursor` é o último ponto que o
  Kafka confirmou antes do reinício (o tail continua com `sequence` acima
  dele). Não tem `sequence` nem `record`.
- `retain` avisa que a tabela histórica apagou as linhas com `rowID` menor que
  `floor`.

Os quatro usam a key fixa `__nishidb_control__` e são publicados uma vez em
CADA partição do tópico. Todo registro, de dado ou de controle, leva o header
`nishidb-op` com o `op`, para o consumidor rotear sem abrir o JSON. O `reset`
leva, além dele, o header `nishidb-reason` com o motivo (a mesma grafia do
`reason` do corpo).

### Ciclo de vida: reset, snapshot e tail

Uma reconstrução anda assim:

1. O publicador consulta quantas partições o tópico tem e fixa esse número.
2. Lê o cursor do feed e copia o primeiro bloco do snapshot. Grava no
   checkpoint do destino que há um `reset` em curso, e só então publica o
   `reset` em todas as partições. Espera a confirmação de todas antes de
   publicar qualquer linha.
3. Publica o estado vivo inteiro como `op: snapshot`, um registro por linha: a
   tabela base primeiro e depois a histórica, precedida de um `retain` com o
   piso dela. Todo registro do snapshot sai com `sequence` igual ao `cursor`
   do `reset`.
4. Publica `snapshot_end` em todas as partições e, confirmado, grava o
   checkpoint com o cursor do `reset`.
5. Segue o tail, lote a lote, com `insert`, `update`, `delete` e `retain`. O
   que o snapshot já cobriu (`sequence` até o cursor) não se repete.

O snapshot anda em blocos curtos, de até 4.096 linhas e com teto em bytes. Cada
bloco lê o dataset por um snapshot de leitura próprio, fechado antes de o bloco
ir para o Kafka, e no máximo dois blocos ficam em voo. Uma linha alterada
durante a exportação pode sair num estado mais novo que o cursor; o tail
reaplica as alterações seguintes por cima, e o estado converge.

Um snapshot interrompido deixa no tópico um `reset` sem o `snapshot_end`
correspondente. O próximo `reset` recomeça a reconstrução do zero.

#### O checkpoint do destino

Cada destino tem dois arquivos em `<dataset>/replication/kafka/`, ao lado do
feed:

- `<destino>.ckpt`: a geração e o cursor que o Kafka confirmou, a posição em
  bytes desse cursor no feed, o tópico, os `bootstrapServers`, o número de
  partições, a impressão da renderização, o instante do último `snapshot_end`,
  a marca de `reset` em curso e a régua pendente da conferência de rewind
  (abaixo). É gravado com CRC, de forma atômica: depois de
  uma queda, vale o anterior inteiro ou o novo inteiro.
- `<destino>.resync`: o contador de pedidos de snapshot (ver
  [Snapshot periódico e sob demanda](#snapshot-periódico-e-sob-demanda)).

O checkpoint é gravado antes de cada `reset` (com a marca de `reset` em
curso), depois de cada `snapshot_end` confirmado, no tail no máximo uma vez
por segundo, e na parada do publicador, com o último ponto confirmado. Ele
pode ficar atrás do confirmado: a retomada reentrega o intervalo, e a entrega
já é at-least-once. Cada tentativa de gravação que falha conta uma vez
`errors{reason="checkpoint_write"}`: antes do `reset`, o snapshot não começa
(o publicador fica em `retrying` e tenta de novo); no tail e depois do
`snapshot_end`, só é contada, e o publicador segue. Com o disco falhando, o
tail tenta no máximo uma gravação por segundo.

A **impressão da renderização** cobre o que muda o `record` e que a
configuração declara: as tabelas publicadas, os aliases de `schema.fields` e
`publishHistory`. Colunas descobertas em execução (`unknownFields: include`)
ficam de fora: descobrir uma coluna não muda o nome de nenhuma coluna que já
existe.

#### Decisão na subida

Na subida, cada destino escolhe entre retomar o tail e reconstruir. Primeiro
vem a conferência de rewind (abaixo); depois, a tabela, em que vale a primeira
linha que casa:

| # | Situação | Ação |
|---|---|---|
| 0 | a conferência de rewind achou o feed atrás do que o destino já recebeu, ou não conseguiu conferir o topo do tópico | geração nova do feed e `reset` com `rewind` |
| 1 | sem checkpoint (primeira subida, destino novo, checkpoint ilegível); tópico diferente; ou nenhum `host:porta` em comum com os `bootstrapServers` gravados | `reset` com `startup` |
| 2 | impressão da renderização diferente | `reset` com `config_changed` |
| 3 | o checkpoint marca um `reset` em curso (o snapshot anterior morreu no meio) | `reset` com `snapshot_failed` |
| 4 | pedido de snapshot pendente | `reset` com `requested` |
| 5 | o feed está noutra geração: queda (a geração anterior do feed é a do checkpoint) | `reset` com `crash` |
| 5 | o feed está noutra geração: rotacionado (o rewind de outro destino do mesmo dataset) | `reset` com `rewind` |
| 5 | o feed está noutra geração, outro caso | `reset` com `startup` |
| 6 | o cursor do checkpoint está abaixo do piso do feed | `reset` com `retention` |
| 7 | o número de partições do tópico difere do gravado | `reset` com `partitions_changed` |
| 8 | `periodicSnapshot` venceu desde o último `snapshot_end` | `reset` com `periodic` |
| 9 | nenhuma das anteriores | **retoma o tail**: `resume` em todas as partições, e o tail continua do cursor do checkpoint |

Acrescentar um broker à lista não troca o cluster: basta um `host:porta` em
comum. O `resume` sai na hora, sem esperar a confirmação nem o primeiro lote
do tail. A decisão que depende do broker (a conferência de rewind e as
partições) espera o broker: com ele fora, o publicador fica em `retrying` e
tenta de novo, e nunca retoma nem publica `reset` às cegas.

#### Conferência de rewind

O feed e o checkpoint moram na pasta do dataset. Um backup restaurado traz os
dois coerentes, mas no passado, e retomar dali reusaria `(epoch, sequence)` já
entregues com outro conteúdo. Por isso, quando o feed reabre de um
fechamento limpo, o publicador confere, antes de qualquer outra regra:

- no próprio checkpoint, sem ler o tópico: o cursor confirmado, ou o cursor do
  `reset` em curso, acima da régua (só o feed voltou no tempo). Só vale com o checkpoint do MESMO tópico e cluster;
- no tópico: o último registro de cada partição. É rewind se ele tem a geração
  do feed com posição (`sequence` no dado, `cursor` no controle) acima da
  régua, ou se tem uma geração que o checkpoint não
  conhece (o tópico é exclusivo do dataset, então ela é publicação posterior
  ao backup);
- no tópico, a partição sem registro legível: esvaziada pela retenção do
  tópico (início igual ao fim, acima de zero) ou, com o checkpoint deste
  tópico, nunca escrita (fim zero) entre as partições que ele registra: é o
  tópico recriado vazio. A conferência não consegue provar que o feed não
  voltou no tempo, e por isso não retoma nem fica esperando: é rewind. A
  partição acrescentada com o master parado (índice além das partições do
  checkpoint, nunca escrita) não recebeu registro nosso e não conta: a
  subida segue pela tabela, com `partitions_changed`.

O checkpoint de OUTRO tópico ou cluster (o destino trocado no YAML) conta como
ausente na conferência: ele não diz nada sobre o tópico conferido. O destino
trocado para um tópico vazio, portanto, segue pela tabela, com `startup`, sem
rotacionar o feed.

Sem checkpoint (destino renomeado, já que o checkpoint é por nome, ou
checkpoint apagado à mão) ou com o checkpoint de outro tópico, toda geração
presente no tópico é desconhecida, e o resultado também é rewind, e não
`startup`. Sem checkpoint não dá para saber
se a geração do feed já apareceu no tópico e foi superada por outra: um
`reset` nela seria descartado pelo consumidor como geração antiga. Sem
checkpoint e com o tópico vazio, não há o que conferir, e o destino é novo
(`startup`).

A **régua** da conferência é a `sequence` em que o feed reabriu, e nunca a
corrente: a ingestão já roda, e um feed restaurado alcançaria o topo antigo
do tópico enquanto a leitura não volta. Ela é gravada no checkpoint ANTES da
primeira leitura do tópico, e sobrevive a uma vida que não chega a decidir:
uma subida que reabre o feed restaurado, fica com o tópico ilegível a vida
inteira, ingere e fecha limpo deixa a régua gravada, e a vida seguinte, que
reabre com uma `sequence` maior, confere com a menor das duas. A régua
gravada nunca sobe; a de outra geração (o feed nasceu de queda desde então)
é descartada. Ela sai do checkpoint quando a decisão termina: na retomada,
logo depois do `resume`, e na marca de `reset` em curso. Se ela não puder
ser gravada, a conta é `errors{reason="checkpoint_write"}`, o publicador fica
em `retrying`, e a conferência não roda. Com o rewind, o feed abre uma geração
nova e o destino é reconstruído nela (o `reset` leva `previousEpoch`). A
troca de geração é condicionada: se o feed fechou no meio da conferência (o
encerramento chegou enquanto a leitura falhava), nada é rotacionado e nada é
publicado, e a próxima subida confere de novo. O `reset` do rewind nunca sai
na geração antiga.

A falha de infraestrutura na leitura do tópico (broker fora, timeout, ou o
último registro que não chega no prazo, como o último offset num marcador de
transação) conta `errors{reason="topic_read"}` e é repetida com o backoff da
entrega. Ela não é ausência comprovada de registro, e nunca vira rewind, nem
retomada, nem `reset` na geração antiga. Limites conhecidos:

- num dataset de pouco tráfego, com reinícios espaçados mais que a retenção
  do tópico, a partição esvaziada faz a subida limpa publicar `rewind` e um
  snapshot completo. Com `periodicSnapshot` abaixo da retenção do tópico,
  toda partição recebe um marcador de controle antes de esvaziar;
- só o publicador Kafka confere. Num master com destino Kafka, os followers
  HTTP esperam a conferência (503 no lote e no snapshot) e seguem a geração
  nova do rewind (ver o follower em
  [Reinício limpo](#reinício-limpo)); a conferência que falha por
  infraestrutura os deixa esperando pelo mesmo tempo. Um master com apenas
  followers HTTP não detecta o rewind (débito 141);
- o rewind pode sair sem divergência real, do lado seguro. Uma vida que não
  chega a decidir (o tópico ilegível a vida inteira) deixa no checkpoint a
  régua da reabertura dela, mas continua ingerindo, e fecha limpa com o feed
  ADIANTE da régua. Um backup frio tirado ENTRE essa vida e a seguinte guarda
  os dois assim. A vida seguinte confere, retoma e publica no tópico as
  alterações acima da régua. Restaurado esse backup, o topo do tópico está
  acima da régua gravada, e a subida dá rewind, mesmo quando o tópico só
  recebeu, acima da régua, o que o feed restaurado também tem. O custo é um
  snapshot completo a mais; retomar com o feed realmente atrás do tópico
  nunca acontece.

#### Motivos do reset

| Motivo | Quando |
|---|---|
| `startup` | primeira subida do destino, destino trocado (tópico ou cluster) para um tópico vazio, checkpoint ilegível com o feed não limpo ou o tópico vazio, ou feed numa geração sem relação com a do checkpoint |
| `crash` | o feed abriu uma geração nova depois de queda do master (ou de mutação com ele parado) |
| `rewind` | o feed voltou no tempo em relação ao que o destino já recebeu (backup restaurado), ou foi rotacionado pelo rewind de outro destino. Também o feed reaberto limpo quando o topo do tópico não pode ser conferido (partição esvaziada pela retenção do tópico, ou tópico recriado vazio com checkpoint), e diante de um tópico com registros sem checkpoint legível (destino renomeado, checkpoint apagado) |
| `retention` | o cursor do checkpoint já saiu do disco do feed |
| `gap` | em operação: o feed descartou alterações que o publicador ainda não tinha lido, ou invalidou o intervalo retido; a geração continua a MESMA, e o `cursor` salta |
| `snapshot_failed` | um snapshot anterior falhou no meio, depois de já ter publicado alguma coisa, inclusive o que o reinício interrompeu |
| `partitions_changed` | o número de partições do tópico mudou (na subida, inclusive a partição acrescentada com o master parado, ou na conferência periódica, uma vez por minuto) |
| `config_changed` | a impressão da renderização mudou: tabelas publicadas, aliases ou `publishHistory` |
| `requested` | o operador pediu um snapshot do destino |
| `periodic` | `periodicSnapshot` venceu |

#### Snapshot periódico e sob demanda

Com `replication.periodicSnapshot`, todo destino Kafka do dataset republica o
estado inteiro quando passa esse intervalo desde o último `snapshot_end`
confirmado. O instante está no checkpoint, e por isso o prazo atravessa o
reinício. Use um valor abaixo da retenção do tópico: um consumidor que começa
do início do tópico sempre encontra um snapshot completo.

O pedido de snapshot sob demanda incrementa um contador em
`<destino>.resync`. O pedido é durável quando registrado, e sobrevive a um
reinício que aconteça antes do snapshot. O checkpoint guarda o valor do
contador que o último snapshot atendeu: um pedido feito durante um snapshot
fica acima dele e gera outro. O publicador confere o contador e o prazo do
periódico a cada conferência de partições (uma vez por minuto), e o pedido
feito pelo próprio processo é conferido na hora.

O pedido é feito pela rota ADMIN
`POST /v1/replication/resync?dataset=<dataset>&destination=<destino>`, com o
servidor no ar, ou pelo comando equivalente:

```
nishidb replication resync --server <host:porta> --dataset <dataset> --destination <destino>
```

O comando aceita a mesma credencial e o mesmo TLS dos outros comandos remotos
(`--user`, `NISHIDB_PASSWORD` ou `--password-file`, `--tls-ca`). A rota
responde `202` só depois de o contador estar gravado, com o corpo
`{"dataset":"<dataset>","destination":"<destino>","requested":true,"state":"<estado>"}`:
o `202` confirma o PEDIDO, e não o fim do snapshot. `state` é o estado do
publicador no instante do pedido, e o comando o imprime. Com o publicador
ativo (`snapshotting`, `following` ou `retrying`), o `reset` sai em seguida
com o motivo `requested`. Com o publicador parado (`blocked`, `failed` ou
`stopped`), o pedido fica gravado do mesmo jeito e é atendido na próxima
subida. O andamento se lê em `_meta.replication` (`lastResetReason`,
`lastSnapshotAt`) ou em `snapshots_total{reason="requested"}`.
Dataset sem feed e destino que não está em `replication.targets` do dataset
respondem `404` com o código `UNKNOWN_DATASET`, e as duas recusas se
distinguem pela mensagem; sem os dois parâmetros, `400`; método diferente de
POST, `405`; a gravação do contador que falha, `500`, e nada fica pedido. Pelo
listener de loopback a rota responde `403`, como toda rota ADMIN.

### Regras para o consumidor

O `resume` não exige nada do consumidor: ele só registra que o master
reiniciou de forma limpa e que o tail continua na mesma geração, a partir do
`cursor` dele. O `previousEpoch` do `reset` também é só informação: as regras
abaixo não dependem dele.

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
| `retrying` | a entrega falhou por infraestrutura (broker fora, timeout, tópico ausente, mensagem recusada pelo broker), a leitura do tópico na subida falhou (`topic_read`) ou o checkpoint não foi gravado antes do `reset` (`checkpoint_write`); tenta de novo para sempre |
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

O disco do feed (`maxDiskBytes`) é o que o publicador pode atrasar sem perder a
continuidade: com o broker fora, ele lê do disco tudo o que o feed reteve, e o
`reset` por `gap` (ou por `retention`, na subida) só vem quando a retenção
apaga o que ele ainda não entregou. A memória (`maxBufferedBytes`) não entra
nessa conta: ela é só o cache dos lotes recentes, e despejar dela não cria
lacuna. A janela que o disco cobre, em segundos, é

    maxDiskBytes / (alterações por segundo × custo por entrada)

e é ela que deve passar da maior queda do broker que o destino precisa
atravessar sem snapshot.

O custo de uma entrada no disco é de cerca de 44 bytes, mais o nome da tabela,
a key e o payload dela (na memória, cerca de 192 bytes mais os mesmos campos).
A consolidação de updates seguidos da mesma entidade reduz a conta. Na
prática, meça no próprio dataset: num destino atrasado, `lagBytes` dividido
por `lagSequences` (em `_meta.replication` ou no `/v1/stats`) dá, com boa
aproximação, o custo por alteração produzida, com a consolidação já
descontada (o atraso em bytes não conta o intervalo ainda aberto).

**Exemplo: o piloto CTP.** Medido em 16/10/2026: 412 alterações por segundo em
média, 476 por segundo na hora de pico da tarde, e storms estimados em 2 a 3
vezes a média. Cada entrada custa cerca de 2,3 KB, o que dá cerca de 0,95 MB/s,
ou cerca de 3,4 GB por hora. A conta, com 1 GiB = 1.073.741.824 bytes:

| `maxDiskBytes` | média (412/s) | pico (476/s) | storm 2× (824/s) | storm 3× (1.236/s) |
|---|---|---|---|---|
| 2 GiB (padrão) | ~38 min | ~33 min | ~19 min | ~13 min |
| 8 GiB | ~2 h 31 min | ~2 h 11 min | ~1 h 15 min | ~50 min |
| 256 GiB (teto) | ~80 h | ~70 h | ~40 h | ~27 h |

O padrão é modesto de propósito: num dataset do porte do CTP ele não
atravessa uma manutenção de broker de uma hora. Escolha o valor pela queda que
precisa atravessar, e confira o espaço livre do disco do dataset.

Enquanto o snapshot anda, o feed tem de reter todas as alterações feitas desde
o cursor do `reset`: são elas que o tail entrega depois. Se o disco do feed
não comportar o que a ingestão produz durante um snapshot inteiro, o cursor se
perde, o publicador recomeça com `reset` por lacuna, e o próximo snapshot tende
a acabar do mesmo jeito. O publicador então republica o dataset em laço e nunca
alcança o tail.

A conta a fazer:

    maxDiskBytes > duração do snapshot × alterações por segundo × custo por entrada

A duração e o número de linhas de cada snapshot saem no log
`replication.kafka.snapshot.completed`. No CTP, um snapshot de 30 minutos na
hora de pico produz cerca de 476 × 1.800 × 2,3 KB ≈ 2,0 GB de feed: o padrão
de 2 GiB fica no limite, e o valor certo ali é maior. Em todo master
habilitado, `maxBufferedBytes` também precisa ser pelo menos o triplo de
`maxBatchBytes`, mas subir `maxBufferedBytes` não alarga a janela retida.

Quando o laço acontece, ele aparece de duas formas:

- o contador `errors{reason="snapshot_gap"}` sobe a cada snapshot abortado no
  meio porque o cursor se perdeu;
- o log de ERROR `replication.kafka.livelock` sai no máximo a cada 5 s, com
  `stage=snapshot` (perdeu no meio) ou `stage=tail_start` (perdeu entre o
  `snapshot_end` e o primeiro lote do tail), e com a duração do snapshot.

O remédio é subir `maxDiskBytes`, que é o que limita a janela retida, e é o
que a mensagem do log recomenda. A proteção atual só detecta o laço.
Com `stage=snapshot`, o publicador espera 200 ms antes do próximo snapshot;
com `stage=tail_start`, recomeça na hora (débito 132).

### Observação

O `/v1/stats` traz, no dataset master com replicação ligada, o bloco
`replication`: `feed` (geração, sequência produzida, piso, bytes retidos,
entradas antes e depois da consolidação, lotes descartados, invalidações e,
do feed em disco, `diskBytes`, `diskSegments` e `diskErrors`) e `delivery`, um
item por destino (estado, geração e cursor confirmados, atraso em sequências e
em bytes (`lagBytes`), o cursor do último checkpoint gravado
(`checkpointCursor`), registros entregues, snapshots por motivo, erros por
motivo, última entrega e último erro). As métricas leem a mesma estrutura:

- por destino (`dataset`, `destination`):
  `nishidb_replication_kafka_delivered_total`,
  `nishidb_replication_kafka_lag_sequences`,
  `nishidb_replication_kafka_snapshots_total` (`reason`),
  `nishidb_replication_kafka_errors_total` (`reason`),
  `nishidb_replication_kafka_state` (`state`: uma série por estado, com 1 no
  corrente), `nishidb_replication_kafka_last_success_timestamp`,
  `nishidb_replication_kafka_lag_bytes` (o atraso em bytes do feed em disco)
  e `nishidb_replication_kafka_checkpoint_sequence` (a sequence do último
  checkpoint gravado: de onde a próxima subida retoma);
- do feed: `nishidb_replication_feed_input_entries_total`,
  `nishidb_replication_feed_output_entries_total`,
  `nishidb_replication_feed_buffered_bytes`,
  `nishidb_replication_feed_evicted_batches_total`,
  `nishidb_replication_feed_invalidations_total`,
  `nishidb_replication_feed_disk_bytes`,
  `nishidb_replication_feed_disk_segments` e
  `nishidb_replication_feed_disk_errors_total`.

Os motivos de snapshot e de erro e os estados existem desde a subida, em
zero, para que um alerta sobre eles funcione antes da primeira ocorrência; as
séries do feed em disco e do checkpoint também nascem em zero. Os detalhes de
cada série estão em [metricas.md](metricas.md).

#### `_meta.replication`

A mesma leitura sai por SQL, uma linha por destino de `replication.targets`,
em `SELECT * FROM _meta.replication` (no schema do dataset, pela rota de
consulta). O dataset sem replicação, o follower e o master sem destino Kafka
respondem a tabela vazia, e nunca erro. A tabela histórica não a recebe: o
feed e os publicadores são do dataset base.

| Coluna | Tipo | O que diz |
|---|---|---|
| `id` | string | o destino (o `name` em `kafkaTargets:`); é a chave |
| `topic` | string | o tópico do destino na configuração |
| `state` | string | o estado do publicador (`snapshotting`, `following`, `retrying`, `blocked`, `failed`, `stopped`) |
| `epoch` | string | a geração do último ponto confirmado |
| `cursor` | int64 | a sequence confirmada pelo Kafka, em memória |
| `checkpointCursor` | int64 | a sequence do último checkpoint gravado; fica atrás de `cursor` (no máximo uma gravação por segundo no tail); NULL enquanto o destino não tem `.ckpt` |
| `produced` | int64 | a última sequence produzida pelo feed |
| `floor` | int64 | o piso do feed: ele serve as sequences acima dele |
| `feedDiskBytes` | int64 | o tamanho do feed em disco, cabeçalhos incluídos |
| `feedCacheBytes` | int64 | os bytes retidos em memória pelo feed |
| `feedSegments` | int64 | o número de segmentos do feed em disco |
| `lagSequences` | int64 | o que o feed produziu além do cursor |
| `lagBytes` | int64 | o mesmo atraso, em bytes do feed em disco |
| `lastResetReason` | string | o motivo do último `reset` publicado nesta vida |
| `lastSnapshotAt` | timestamp | o último `snapshot_end` confirmado (persistido no checkpoint) |
| `lastSuccessAt` | timestamp | a última entrega completa (lote do tail ou snapshot) |
| `lastError` | string | o último erro de entrega |

As colunas do feed (`produced`, `floor` e as três `feed*`) são do dataset, e
repetem em toda linha. Valor ausente é NULL, e nunca zero: texto vazio e
instante "nunca houve" saem NULL, e `checkpointCursor` é NULL enquanto o
destino não tem `.ckpt`. Os números de uma consulta são colhidos uma vez, na
abertura da consulta.

`lastSnapshotAt` vem do `.ckpt` gravado depois do `snapshot_end` confirmado, e
o publicador passa a `following` ANTES dessa gravação (o fsync não atrasa o
retrato). Por isso há uma janela curta em que a linha mostra `following` com
`lastSnapshotAt` ainda NULL (no primeiro snapshot) ou com o valor do snapshot
anterior. Se essa gravação falha (`errors{reason="checkpoint_write"}`), o valor
velho fica até a próxima gravação bem-sucedida do `.ckpt`.

Com o servidor parado, `nishidb query --config` responde a tabela com o que
está no disco: cada destino de `replication.targets` sai do seu `.ckpt`
(`epoch`, `checkpointCursor`, `lastSnapshotAt`) com o tópico da
configuração, e as colunas do feed saem do `state` só quando ele tem a marca
de fechamento limpo (a posição que a próxima subida retoma; sem a marca, são
NULL). O que só um publicador vivo mede — `state`, `cursor`, os dois atrasos,
`feedCacheBytes`, `lastResetReason`, `lastSuccessAt` — é NULL. Um `.ckpt`
ilegível não some com o destino: a linha sai com `lastError` =
`checkpoint unreadable: …`.

### Mudanças de contrato desde a v0.14

O envelope Kafka e o `/v1/replication/*` saíram na v0.14.x. Toda mudança de
contrato feita depois dela é aditiva: um consumidor que não conhece o item novo
continua lendo o que lia.

- **`historyExcluded` no `GET /v1/replication/cursor`.** O cursor anuncia
  `"historyExcluded": true` sempre que o master não publica uma tabela
  histórica: com `publishHistory: false` e também no dataset sem `history:`. O
  campo é omitido quando a histórica é publicada, e o ausente vale o legado:
  a histórica é publicada.
- **Estado `history_unavailable` do follower.** O follower que declara
  `history:` e encontra um master com `historyExcluded` (sem histórica ou com
  `publishHistory: false`) para nesse estado, sem aplicar nada, em vez de
  seguir com a histórica vazia. O follower sem `history:` segue o mesmo
  master normalmente. Limite: um master v0.14.x não manda o campo, e um
  follower novo com `history:` diante de um master v0.14.x SEM `history:`
  continua com a tabela histórica vazia, como antes.
- **A geração sobrevive ao reinício limpo.** Antes, toda subida do master abria
  uma `epoch` nova; agora o reinício limpo mantém a `epoch` e a `sequence`, e
  um `reset` de `startup` pode sair com a mesma `epoch` da vida anterior. As
  regras do consumidor já cobrem o caso (um `reset` na mesma geração, como o
  de `gap`).
- **Header `nishidb-reason` no `reset`.** Todo `reset` sai agora com DOIS
  headers, `nishidb-op: reset` e `nishidb-reason: <motivo>`, o motivo na mesma
  grafia do `reason` do corpo. O consumidor decide sem abrir o JSON. Os demais
  registros seguem só com o `nishidb-op`.
- **Seis motivos novos no `reset`.** Além de `startup`, `gap`,
  `snapshot_failed` e `partitions_changed`, o `reason` pode ser `crash`,
  `rewind`, `retention`, `config_changed`, `requested` e `periodic` (ver
  [Motivos do reset](#motivos-do-reset)). O consumidor trata todo `reset` do
  mesmo jeito, qualquer que seja o motivo, e o que não conhece um motivo
  continua lendo o `reset` pelo `op`.
- **`previousEpoch` no `reset`.** Chave opcional, logo depois de `reason`, com
  a geração que o `reset` substitui; omitida quando não há troca de geração.
- **Op `resume`.** Marcador de controle novo, em todas as partições, na
  retomada depois do reinício limpo. Não exige ação: o consumidor que não o
  conhece pode ignorá-lo. Com ele, o reinício limpo do master deixa de
  republicar o dataset inteiro.
- **Rota `POST /v1/replication/resync`.** Rota ADMIN nova, que pede o snapshot
  sob demanda de um destino (ver
  [Snapshot periódico e sob demanda](#snapshot-periódico-e-sob-demanda)), e o
  comando `nishidb replication resync`. Nenhum código de erro novo: as
  recusas usam `UNKNOWN_DATASET`, `BAD_REQUEST`, `METHOD_NOT_ALLOWED` e
  `INTERNAL`.
- **Tabela `_meta.replication`.** Tabela virtual nova, registrada em todo
  dataset base (ver [`_meta.replication`](#_metareplication)). Ela entra no
  catálogo (`_meta.tables`, `/v1/metadata`): quem compara a lista inteira de
  tabelas passa a ver uma a mais.
- **Campos novos no `/v1/stats`.** `replication.feed` ganha `diskBytes`,
  `diskSegments` e `diskErrors`, e cada item de `replication.delivery` ganha
  `lagBytes` e `checkpointCursor`.
- **Chaves novas em `replication:`.** `maxDiskBytes` (padrão 2 GiB, de 8 MiB
  a 256 GiB), `publishHistory` (padrão `true`, o legado) e `periodicSnapshot`
  (omitida, desligada), as três só no master habilitado. Omitidas, o dataset
  se comporta como antes, a não ser pelo feed em disco, que passa a existir
  com o padrão.
- **Piso de `maxBufferedBytes` sobe de 2× para 3× `maxBatchBytes`.** Uma das
  duas mudanças que RECUSAM uma configuração que a v0.14.x aceitava: um
  master habilitado com `maxBufferedBytes` entre o dobro e o triplo de
  `maxBatchBytes` deixa de subir, com a mensagem que nomeia as duas chaves. O
  terceiro lote é o da fila do gravador em disco (ver o começo desta página).
- **Broker com vírgula em `bootstrapServers` é recusado.** A outra mudança que
  RECUSA uma configuração aceita pela v0.14.x: um item como `"a,b:9092"`
  em `source.kafka.bootstrapServers`, `kafkaTargets[].bootstrapServers` ou
  `alarmDelivery.kafka[].bootstrapServers` deixa de carregar, com a mensagem
  que pede um broker por item. O item nunca conectava (o host seria `a,b`),
  e o checkpoint do destino, que guarda os brokers juntados por vírgula, não
  conseguiria gravá-lo.
- **503 no lote e no snapshot durante a conferência de rewind.** No master com
  destino Kafka que retoma limpo, o `GET /v1/replication/batch` e o
  `GET /v1/replication/snapshot` respondem 503 até a conferência de rewind
  terminar (ver [Reinício limpo](#reinício-limpo)). Nenhum código de erro
  novo: a recusa usa `INTERNAL`, e o follower decide pelo status HTTP. Um
  follower v0.14.x trata o 503 como qualquer erro HTTP: desconecta, espera
  com backoff e tenta de novo, sem aplicar nada.
- **Feed em disco em `<dataset>/replication/`.** O diretório `feed/`
  (segmentos `.seg` e o `state`) e o `kafka/` (`<destino>.ckpt` e
  `<destino>.resync`) são novos na pasta do dataset. Backup e restauração
  levam os dois junto do dataset (ver
  [Conferência de rewind](#conferência-de-rewind)).
- **Retenção do feed por bytes de disco.** O piso anunciado em
  `availableAfter` pelo `GET /v1/replication/cursor` passa a subir com a
  retenção do disco, e não mais com o despejo da memória, e o
  `evictedBatches` do status deixa de significar perda: despejar da memória
  não cria lacuna. Os campos e o 409 abaixo do piso são os mesmos.
- **Séries novas.** `nishidb_replication_feed_disk_bytes`,
  `nishidb_replication_feed_disk_segments`,
  `nishidb_replication_feed_disk_errors_total`,
  `nishidb_replication_kafka_lag_bytes` e
  `nishidb_replication_kafka_checkpoint_sequence`, todas desde a subida, em
  zero.

**Defeito conhecido da v0.14.x.** Quando um lote do feed fechava por tamanho
(`maxBatchBytes`) antes de a alteração seguinte entrar, ele anunciava a
numeração dela sem incluí-la, e a alteração da fronteira se perdia no
publicador Kafka e no follower HTTP. O defeito foi corrigido a partir desta
versão. A v0.14.x, de uso interno, não recebe correção.

### Encerramento

O encerramento do master segue uma ordem, e é ela que deixa o reinício limpo
retomar a mesma geração:

1. A ingestão para, e os long-polls dos followers e os snapshots HTTP em curso
   saem: o long-poll responde 410, e o snapshot que ainda não começou a
   responder também.
2. O servidor HTTP para, inclusive as rotas que mudam o dataset
   (`/v1/compact`).
3. Compaction, expiração e retenção param em todo dataset. Uma passada em voo
   termina antes, e o que ela publica entra no feed.
4. Todo dataset, com ou sem ingestão Kafka, faz checkpoint (a tabela histórica
   primeiro). Se o checkpoint falha, sai o WARN
   `datasets.shutdown_checkpoint_failed`, o encerramento continua e a próxima
   subida reaplica a cauda.
5. O feed fecha: entrega o lote que estava aberto, grava o que falta e grava a
   marca limpa com a impressão dos Stores. Se a posição de um Store não é
   legível (o checkpoint que falhou no fsync deixa o writer falho), o feed
   fecha sem a marca, com o WARN `replication.feed.closed_unclean`, e a
   próxima subida abre uma geração nova. Os feeds de todos os datasets fecham
   em paralelo, com um prazo de 10 s para todos juntos; o feed cujo gravador
   não termina no prazo fecha sem a marca, com o WARN
   `replication.feed.close_failed`.
6. Os publicadores drenam o que resta e param. A drenagem tem orçamento
   próprio de 10 s para todos os publicadores juntos, separado do prazo do
   servidor HTTP. Se o prazo vence (broker fora, tópico lento), sai um WARN
   `replication.publisher.close_failed` e o encerramento continua. Nada se
   perde: cada publicador grava no checkpoint o último ponto confirmado, e a
   próxima subida retoma dali, entregando de novo o que faltou. Um snapshot em
   curso no encerramento continua dentro do orçamento; se o prazo vence antes
   do fim, ele é abandonado sem `snapshot_end`, e a próxima subida publica
   `reset` com `snapshot_failed`.
7. Os datasets fecham, e só então o log do feed.
