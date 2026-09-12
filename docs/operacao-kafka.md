# Operar o consumo de Kafka

Guia do caminho completo: build, configuração, subida contra um Kafka real,
verificação e os modos de falha que o operador vai encontrar. Tudo aqui foi
executado contra um broker real antes de ser escrito.

> **Consultar com o servidor no ar é pelo protocolo remoto.** O `nishidb
> query --config` abre o dataset localmente e continua barrado pelo flock
> (exit 3, nomeando o pid do servidor) — isso não mudou nem vai mudar, porque o
> flock é exclusivo por processo. O que mudou é que agora existe o outro
> caminho: `POST /v1/query` (§67-69) e o `nishidb query --server`, que falam
> com o processo que já tem o dataset aberto. Foi assim que o débito 54
> fechou; a rodada que mediu isso está em [`aceite-fase6.md`](aceite-fase6.md).

## 1. Build

```bash
go build -o nishidb ./cmd/nishidb
```

Binário único, estático, sem dependência em runtime — o cliente Kafka é Go
puro, então `CGO_ENABLED=0` produz um executável que roda em `scratch`
(§97):

```bash
CGO_ENABLED=0 go build -o nishidb ./cmd/nishidb
file nishidb   # ELF 64-bit LSB executable, statically linked
```

Requer Go 1.25+ e Linux (o storage usa `mmap` e `fallocate` diretamente).

## 2. `nishidb.yaml`

```yaml
version: 1

storage:
  path: /var/lib/nishidb          # DISCO REAL; tmpfs invalida o orçamento de memória
  segmentSize: 512MiB
  checkpoint:
    interval: 30s                 # a janela de reprocessamento em caso de crash

datasets:
  directory: /etc/nishidb/datasets

server:
  bind: 127.0.0.1
  port: 9142

query:
  defaultMaxRows: 100000
  defaultTimeout: 30s
```

`storage.checkpoint.interval` **é** a janela de reprocessamento: um crash faz o
consumo retomar do último checkpoint durável, e tudo depois dele é reprocessado
(inócuo, mas custa tempo e lixo em disco). Diminuir o intervalo estreita a
janela e aumenta a frequência de fsync.

`storage.durability` continua **aceita e ignorada** (débito 50): quem a lê
conclui uma garantia que o binário não dá. A janela real é a do
`checkpoint.interval` acima.

## 3. YAML do dataset

Um arquivo por dataset em `datasets.directory`, e **o nome do arquivo é o nome
do dataset** (`radius_sessions.yaml` → `name: radius_sessions`).

```yaml
version: 1

name: radius_sessions
schemaName: telecom

source:
  type: kafka
  kafka:
    enabled: true                          # sem isto, o bloco descreve e não liga
    bootstrapServers: [kafka01:9092, kafka02:9092, kafka03:9092]
    topic: radius-accounting
    consumer:
      groupId: nishidb-radius
      autoOffsetReset: earliest            # só vale com o dataset VAZIO
      maxPollRecords: 1000
      ingestionQueueSize: 5000             # gate de registros
      ingestionQueueBytes: 64MiB           # gate de BYTES
      decodeWorkers: 1                     # decodificadores em paralelo; 1 é o default
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
      usernameEnv: KAFKA_USERNAME          # o NOME da variável, nunca o valor
      passwordEnv: KAFKA_PASSWORD
      caFile: /etc/nishidb/tls/kafka-ca.pem

errors:
  invalidEvent: log-and-skip

materialization:
  key:
    field: id
  mode: merge
  sequence:
    field: _SEQ
    type: int64
    strategy: greater

schema:
  include: ["*"]
  unknownFields: include
  fields:
    id: { type: string }
    _SEQ: { type: int64 }
    Acct-Status-Type: { type: string, alias: status, index: true }
    Framed-IP-Address: { type: ipv4, alias: ip }
```

Três coisas que o parse recusa e que valem saber antes do primeiro deploy:

- **Chave desconhecida** dentro de `source` e de `errors` é erro, com o nome da
  chave e a lista das conhecidas. É deliberado: `invalidEvant: stop` com typo
  cairia no default e a política que você escreveu não existiria.
- **`usernameEnv`/`passwordEnv` recebem o NOME da variável.** Escrever a senha
  ali é recusado no parse, com a mensagem dizendo isso — o YAML é versionado e
  o §95 proíbe registrar senha.
- **Credencial ausente ou vazia** é falha de ativação, não de parse: o YAML
  carrega numa máquina sem as variáveis (por isso a suíte roda), e o servidor é
  que recusa subir, nomeando a variável faltante.

## 4. Subir

```bash
export KAFKA_USERNAME=... KAFKA_PASSWORD=...
nishidb server --config /etc/nishidb/nishidb.yaml
```

Flags: `--log-format json|texto` (default `json`), `--log-level`, e
`--pprof <addr>` para os endpoints de debug **num listener próprio** — o §112
os autoriza só em modo administrativo, então não exponha essa porta.

Numa unidade systemd, as credenciais entram por `EnvironmentFile=` com modo
`0600`; o processo nunca as escreve no log.

Subida saudável:

```
level=INFO event=query.pronta       datasets=1 endereco=127.0.0.1:9142
level=INFO event=posicao.inicial    dataset=radius_sessions autoOffsetReset=earliest
level=INFO event=ingestao.subiu     dataset=radius_sessions topico=radius-accounting group=nishidb-radius
level=INFO event=particoes.atribuidas dataset=radius_sessions particoes=3
```

## 5. Verificar

```bash
curl -s localhost:9142/v1/health
curl -s localhost:9142/metrics | grep nishidb_ingest_events_total
```

Ou por SQL, sem HTTP nem painel — os mesmos números, lidos das mesmas
estruturas, uma linha por partição:

```bash
nishidb query --server 127.0.0.1:9142 --dataset eventos "SELECT partition, appliedOffset, highWatermark, lag, lastBatchAt, queueRecords, state FROM _meta.kafka ORDER BY partition"
```

`highWatermark` e `lag` saem **NULL** quando a marca d'água do broker venceu ou
a partição não foi respondida — a consulta busca no broker só quando a marca
está vencida, com o mesmo prazo do scrape. Um zero ali diria "está em dia" a
quem perdeu o broker, e é por isso que a ausência é ausência. Com o servidor
PARADO, o `nishidb query --config` responde a mesma tabela com o que existe
fora dele: tópico, grupo, bootstrap e o offset durável do checkpoint, com as
colunas vivas e o `state` em NULL.

O health responde **503** quando `status` não é `UP`, para um balanceador
tirar a instância de rotação sem interpretar o corpo. A chave `kafka` AGREGA:
basta um dataset parado para ela sair de `UP`, e `detalhes` nomeia qual.

As métricas que respondem "está ingerindo?": `nishidb_ingest_events_total`,
`nishidb_kafka_offset` (por partição), `nishidb_kafka_high_watermark` e
`nishidb_kafka_lag`. As que respondem "está saudável?":
`nishidb_ingest_blocked` (1 = ingestão parada por política),
`nishidb_ingest_failed_total`, `nishidb_kafka_errors_total`, e
`nishidb_kafka_partitions_assigned` contra `nishidb_kafka_partitions_total`.

As do **decode paralelo**, que só dizem alguma coisa junto de
`nishidb_ingest_decode_workers` — o denominador de qualquer leitura de vazão,
porque a mesma taxa de eventos com 1 e com 8 decodificadores significa coisas
diferentes:

| Série | O que ela responde |
|---|---|
| `nishidb_ingest_decode_serial_total` | Eventos que o writer teve de decodificar sozinho, por trazerem coluna ou tipo novo. Em regime é **plana**; `rate()` acima de zero por muito tempo diz que a origem não para de inventar chave — ou que o dicionário está no teto sob `onMaxCols: ignore`, e aí todo evento paga o parse dobrado |
| `nishidb_ingest_decode_arena_bytes` | Memória que os slots da esteira seguram, tanto entre eventos quanto em voo. É o terceiro consumidor de memória da ingestão, e nenhum dos dois gates da fila o conta — o de registros porque o record já saiu da fila, o de bytes porque ele devolveu o orçamento no dequeue. **É a única medida do pico**, e o remédio quando ela incomoda é baixar `maxPollRecords` |
| `nishidb_kafka_decode_idle_seconds_total` | Espera SOMADA dos decodificadores pela fila. Dividida por `nishidb_ingest_decode_workers`, é a fração de tempo em que eles não tinham o que fazer — e o contraste com `nishidb_kafka_writer_idle_seconds_total` diz de que lado está a folga |
| `nishidb_ingest_decode_slots_shrunk_total` | Slots que passaram do limiar e tiveram os buffers soltos. Zero com `arena_bytes` alto é memória presa; crescimento constante é churn de alocação, e aí o limiar está apertado demais para o tamanho dos eventos |

**As três da posição saem do MESMO instantâneo**, por um coletor só, e é isso
que faz `lag == high_watermark - offset` valer dentro de uma raspagem. A
produção do tópico é `rate(nishidb_kafka_high_watermark)` e o consumo é
`rate(nishidb_ingest_events_total)`; comparar os dois é o veredito de "dá
conta". Antes o painel derivava a produção de `Δ(offset + lag)` e chegou a
imprimir `produz -310/s` no piloto, porque as parcelas vinham de instantes
diferentes.

**O lag mede a posição APLICADA, não a commitada.** Até 28/08/2026 ele vinha de
`kadm.Lag`, que devolve o lag do offset commitado no consumer group — e o
commit só sai depois do checkpoint durável (§48), a cada 30 s. Entre dois
commits o offset do group ficava parado e a série crescia à taxa de produção
com o consumidor em dia: medido no piloto, subia até **14.167** e caía para
**515** num único segundo em que **478** eventos foram consumidos. Quem tiver
alerta calibrado no número inflado precisa recalibrar — o lag real do piloto é
da ordem de **centenas**, não de milhares.

**Ausência é resposta.** Se o broker não responde, `nishidb_kafka_lag` e
`nishidb_kafka_high_watermark` **não são emitidas** naquela raspagem, e
`nishidb_kafka_errors_total` sobe. Zero seria uma medição falsa; ausência é
honesta, e `absent(nishidb_kafka_lag)` é um alerta que se consegue escrever.
`nishidb_kafka_offset` continua saindo — ela não depende do broker.

### Saturado, ou sem produção?

`nishidb_kafka_queue_depth` **não** responde isso: é amostra instantânea, e uma
fila que oscila cheia e vazia entre dois scrapes lê zero nos dois casos. Quem
responde é o par de contadores de segundos acumulados, porque `rate()` sobre um
contador em segundos é diretamente a *fração de tempo* — ou seja, utilização:

| `rate(nishidb_kafka_poll_blocked_seconds_total)` | `rate(nishidb_kafka_writer_idle_seconds_total)` | lag | Leitura |
|---|---|---|---|
| ≈ 1 | ≈ 0 | subindo | **Saturado** — a fila enche, o writer é o gargalo |
| ≈ 0 | ≈ 1 | ≈ 0 | **Sem produção** — o tópico está calado |
| ≈ 0 | ≈ 1 | subindo | **O poller não entrega** — é a assinatura do travamento de 27/08; jurisdição do watchdog |
| ≈ 0 | ≈ 0 | ≈ 0 | Fluindo |

Os dois incluem a espera **em curso**, e não só a que já terminou: um lado preso
agora aparece no scrape de agora. Um contador que só somasse ao fim da espera
ficaria imóvel durante uma hora de travamento — que foi exatamente a duração do
incidente de 27/08.

**As duas séries mudaram de dono na Fase 4, e o número na tabela continua sendo
1.** O writer não consome mais a fila: quem espera nela são os
DECODIFICADORES, e a espera deles virou
`nishidb_kafka_decode_idle_seconds_total`, que é uma SOMA — com oito
decodificadores parados, `rate()` é 8, não 1. Para lê-la como fração, divida
por `nishidb_ingest_decode_workers`. O `writer_idle` passou a sair da esteira,
que é onde o writer de fato espera, e por isso a tabela acima segue valendo
sem ajuste de limiar.

Com os dois na mão, a terceira linha ganha um desdobramento útil:
`writer_idle ≈ 1` com `decode_idle/workers ≈ 0` é o writer esperando os
decodificadores — o único cenário em que subir `decodeWorkers` ajuda. Com os
dois altos, quem não entrega está antes: é poller ou broker, e mais
decodificadores não mudam nada.

`nishidb_ingest_apply_seconds` é a latência de um evento pelo `Aplica`
(estado, merge, `Put`, `Apenda`) — e **não** inclui mais o decode, que passou a
ser concorrente: somá-lo aqui faria o histograma medir a soma de trabalhos
paralelos como se fosse latência serial. A pergunta que este número responde é
"o writer é o gargalo?", e o writer é o `Aplica`. O checkpoint fica fora dele de propósito: tem
histograma próprio em `nishidb_checkpoint_duration_seconds`, e somá-lo aqui
produziria um bimodal cujo p99 é sempre o stall de checkpoint. O bucket de topo
é 1 s, o que torna stalls de segundos atribuíveis ao Apply em vez de a "algo".

### Perfis de contenção

`--pprof` passou a ligar também o **block** e o **mutex** profile
(`SetBlockProfileRate`, `SetMutexProfileFraction`). As duas rotas já existiam no
índice do pprof e respondiam `sampling period=0` — perfil vazio —, o que as
tornava inúteis justamente para o que este produto precisa diagnosticar:
contenção do lock do writer. A amostragem custa em toda aquisição de lock, e
por isso só é ligada junto do pprof, que já é modo administrativo.

```bash
curl -s --noproxy '*' 'http://127.0.0.1:9144/debug/pprof/mutex?debug=1' | head -30
curl -s --noproxy '*' 'http://127.0.0.1:9144/debug/pprof/block?debug=1' | head -30
```

## 6. Encerrar

`SIGTERM` (ou `SIGINT`): o servidor para os pollers, drena a fila, faz um
checkpoint final e devolve o flock. **Não use `SIGKILL` por rotina** — ele é
sobrevivível (o §126.25 é testado), mas custa o reprocessamento da janela
desde o último checkpoint.

Só depois de encerrado o dataset fica consultável:

```bash
nishidb query --config /etc/nishidb/nishidb.yaml --dataset radius_sessions \
  'SELECT id, status, ip FROM radius_sessions LIMIT 10'
```

## 7. Modos de falha, e o que fazer

O servidor **não morre** quando a ingestão de um dataset não pode subir: as
consultas continuam registradas e o problema aparece no health (§51). Em todos
os casos abaixo o dataset fica `DOWN` com o motivo em `detalhes`.

| Sintoma no health / log | Causa | O que fazer |
|---|---|---|
| `dataset tem linhas e não tem offsets locais` | O `kafka.offsets` sumiu ou veio de restore parcial | `nishidb offsets --show` para ver o estado, depois `--set` (retomar de uma posição conhecida) ou `--reset earliest\|latest` (aceitar reprocessar ou pular). Nunca é decidido sozinho: `earliest` reprocessaria o tópico retido inteiro e `latest` pularia tudo |
| `a origem mudou desde o último checkpoint: topic mudou (...)` | O YAML aponta para outro tópico, group ou cluster | Se a troca é intencional (promoção teste→produção), `nishidb offsets --reset` autoriza. Se não é, corrija o YAML — o offset gravado é de outro fluxo |
| `assignment parcial: o group entregou 2 de 4 partições` | Outra instância no mesmo `groupId` | Derrube a outra. O v1 é single-writer com checkpoint local autoritativo: duas instâncias materializam metade cada uma |
| `ingestão PARADA` + `nishidb_ingest_blocked 1` | `errors.invalidEvent: stop` encontrou um evento ruim, ou houve `STORAGE_ERROR` | O offset persistido aponta **exatamente** para o evento problemático. Investigue-o no tópico; para pular, `nishidb offsets --set <particao>:<offset+1> --yes` |
| `variável de ambiente KAFKA_PASSWORD não está definida` | Credencial ausente no ambiente do processo | Corrija o `EnvironmentFile`. A mensagem nomeia a variável e nunca o valor |
| `datasets X e Y consomem o tópico T com o mesmo consumer.groupId` | Dois YAMLs copiados sem trocar o group | Recusa na carga do `datasets.directory` — derruba a subida do processo **e** os comandos locais (ver 7.1). Com o group compartilhado, cada dataset materializaria estado parcial e nenhuma métrica acusaria |
| `datasets X e Y declaram o mesmo schemaName S` | Dois YAMLs copiados sem trocar o schema | Mesma recusa, mesma abrangência. Dê a cada dataset um `schemaName` próprio |

### 7.1. Defeito no diretório barra também as ferramentas

O `datasets.directory` é lido **inteiro** por quem quer que o abra: o servidor,
o `nishidb offsets`, o `nishidb ingest`, o `nishidb query` e o
`nishidb counters` entram todos pela mesma porta. Quatro defeitos são de
CONJUNTO e por isso não pertencem a arquivo nenhum:

| Defeito | Mensagem |
|---|---|
| Um YAML que não parseia, em qualquer lugar do diretório | o erro de parse, nomeando o arquivo |
| `name:` divergente do nome do arquivo | `dataset "X": o YAML declara name "Y"` |
| `consumer.groupId` repetido no mesmo tópico e cluster | `datasets X e Y consomem o tópico T ...` |
| `schemaName` repetido | `datasets X e Y declaram o mesmo schemaName S` |

Qualquer um deles derruba o comando **mesmo quando o `--dataset` pedido é
outro** — é a doutrina de configuração do projeto: recusa nomeando o defeito,
nunca contorno em silêncio. Até a Task 3 do ciclo do schema declarado só a
subida do servidor era barrada; agora as ferramentas também são.

**A consequência prática:** a ferramenta de reparo pode estar barrada pelo
mesmo defeito que se quer reparar. Um `groupId` repetido impede a subida, e o
`nishidb offsets --reset` que resolveria o outro dataset não roda enquanto a
colisão estiver no diretório. **A saída é sempre a mesma e é barata:** conserte
o diretório primeiro — a mensagem nomeia os dois arquivos e a chave —, e só
então rode a ferramenta. Se o reparo precisa acontecer antes da correção do
YAML (por exemplo, um dataset que será removido de qualquer forma), mova o YAML
ofensor para FORA do `datasets.directory`; o diretório volta a ser válido, o
`offsets` roda, e o arquivo pode voltar depois de corrigido. Mover o YAML não
toca em dado nenhum: o estado do dataset mora em `storage.path`, não no
diretório de specs.

## 8. Reposicionar

```bash
nishidb offsets --config nishidb.yaml --dataset radius_sessions --show
nishidb offsets --config nishidb.yaml --dataset radius_sessions --set 0:1500,1:900 --yes
nishidb offsets --config nishidb.yaml --dataset radius_sessions --reset earliest --yes
```

Exige o **servidor parado** — o comando toma o flock. É proposital: mexer na
posição com o consumer de pé faria o writer sobrescrever a decisão no
checkpoint seguinte. `--set` e `--reset` pedem `--yes`, porque uma reposição
acidental reprocessa ou pula uma janela inteira do tópico.

Exige também um `datasets.directory` **válido como conjunto**: o comando lê o
diretório inteiro, e um dos quatro defeitos de 7.1 o barra antes de ele abrir o
dataset. Conserte o diretório primeiro; 7.1 diz o que fazer quando isso não é
possível de imediato.

## 9. O que é seguro mudar com o dataset já em produção

| Mudança | Seguro? |
|---|---|
| `ingestionQueueSize`, `ingestionQueueBytes` | Sim. Só afeta o intake |
| `maxPollRecords` | Sim, mas **desde a Fase 4 ele não afeta só o intake**: é ele que dimensiona a esteira de decodificação, arredondado para cima até a potência de dois. Subi-lo multiplica os slots do anel, e cada slot retém buffers entre eventos; baixá-lo é o remédio quando `nishidb_ingest_decode_arena_bytes` incomoda. O limiar por slot acompanha, porque é `ingestionQueueBytes` dividido por essa janela |
| `decodeWorkers` | Sim, e o resultado **não muda o dado**: o golden `workers:1 × workers:8` compara `SELECT *` e `_meta.fields` byte a byte. O que muda é memória — cada slot da esteira retém buffers até a marca d'água —, e o ganho satura cedo: medido em laboratório, 2 dão 1,75× e 8 REGRIDEM para 1,53×. Suba de 1 para 2 e meça; de 4 em diante, só com medição do próprio dataset |
| `errors.invalidEvent` | Sim |
| `enabled: false` | Sim — é o botão de pausa declarativo de UM dataset |
| `autoOffsetReset` | Inócuo depois da primeira subida: com posição local, ele não é consultado |
| Acrescentar campo ao `schema.fields` | Sim, com a ressalva do M1 sobre índices (o conjunto de campos com bitmap é imutável nas duas direções) |
| `topic`, `groupId`, `bootstrapServers` | **Não sem `nishidb offsets`.** A identidade da origem é conferida contra a persistida, e a subida é recusada nomeando o campo que mudou |
