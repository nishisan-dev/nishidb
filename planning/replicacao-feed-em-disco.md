# Feed de replicação em disco e checkpoint do publicador — desenho

Estado: **rascunho em revisão** (16/10/2026). Plano de implementação em
`planning/replicacao-feed-em-disco-plano.md`, escrito depois da aprovação deste
desenho.

## 1. Por que

No piloto CTP (v0.14.1), ligar a publicação em Kafka do dataset `events`
(28,5 M rowIds, ~412 alterações/s, tabela histórica com 962 M linhas) entrou em
laço de `reset` por lacuna. O feed vive em memória e é limitado por
`maxBufferedBytes`, que ali cobria ~20 s de alterações. O snapshot completo
dura muito mais que isso, então o cursor do `reset` era descartado antes do fim
e o publicador recomeçava, sem nunca alcançar o tail (débito 132). Além disso,
TODO reinício do processo gera geração nova e snapshot completo, porque nem o
feed nem a posição do publicador sobrevivem ao processo.

O paliativo em produção é um dataset dedicado `active_alarms`, sem histórica.
Este ciclo resolve a causa.

## 2. Intenção (palavras do usuário)

> "Feed em disco, com disk spill pro primeiro snapshot faz sentido, produz e
> depois segue online, limita por tamanho e não por tempo faz sentido pra mim,
> Checkpoint do publicador: justo... acho que precisa mesmo".

A filosofia é a do binlog do MySQL. O feed é um log em disco limitado por
bytes. Cada leitor guarda a própria posição e retoma dela enquanto ela estiver
retida. O snapshot só sai na carga inicial, quando a posição cai fora da
retenção, ou quando a continuidade se perdeu de verdade.

## 3. Objetivos

1. O feed passa a reter as alterações em disco. A retenção é limitada por
   **tamanho** (`replication.maxDiskBytes`), e não por tempo. Durante um
   snapshot, as alterações que chegam vão para o disco, e o tail segue online
   a partir do cursor do `reset` quando o snapshot termina.
2. A memória do feed vira cache limitado. O heap deixa de crescer com a janela
   de retenção. `maxBufferedBytes` continua sendo o teto de memória do feed,
   mas despejar um lote já gravado da memória deixa de criar lacuna.
3. A geração (`epoch`) e a `sequence` sobrevivem ao **reinício limpo**.
4. Cada destino Kafka persiste a última posição confirmada pelos acks
   (checkpoint do publicador). No reinício limpo, o publicador retoma o tail
   dali, sem `reset` e sem snapshot, enquanto a posição estiver retida.
5. Chave no master para tirar a tabela histórica do feed:
   `replication.publishHistory` (padrão `true`, o comportamento de hoje).
6. A tabela virtual `_meta.replication` mostra, por destino, a posição
   confirmada, o atraso e o estado do feed. As métricas leem a mesma estrutura.

## 4. Não objetivos

- **Retomar depois de queda** (kill -9, OOM, falta de energia, queda do SO).
  **Decisão do usuário (opção A):** a queda descarta o feed, nasce geração nova
  e todo destino faz snapshot completo, com o motivo `crash`. Retomar depois de
  queda exigiria um feed gravado de forma síncrona com o checkpoint do Store e
  a recovery reemitindo o tail reaplicado (o replay hoje não publica,
  `store_recovery.go:425`). Fica para outro ciclo, e não muda o contrato deste.
- Persistir o cursor do follower HTTP. O follower que reinicia continua fazendo
  snapshot.
- Remover estruturalmente o laço do débito 132 (consumir o tail em paralelo à
  exportação). O feed em disco torna o remédio barato: subir `maxDiskBytes`.
  A detecção existente continua.
- Fsync do feed durante a operação. Ele só importa no encerramento limpo,
  porque numa queda o feed é descartado de qualquer jeito.

## 5. Anti-objetivos (falha mesmo que "funcione")

- A ingestão esperar pelo disco do feed, por um leitor ou pelo Kafka. O
  `Publish` continua sem I/O e sem bloqueio, sob o `wmu` do Store.
- Retomar o tail com o feed incompleto. Isso vale para uma mutação do Store que
  não entrou no feed: no encerramento, por um comando offline, ou por uma
  restauração de backup. O consumidor ficaria divergente sem nenhum sinal.
  Diante de qualquer dúvida sobre a completude, o caminho é a queda: geração
  nova e snapshot.
- Reusar uma `(epoch, sequence)` para conteúdo diferente do que já foi
  entregue. O consumidor deduplica por `(epoch, sequence)` e descartaria o
  conteúdo novo.
- Heap proporcional à retenção em disco, por exemplo um índice com uma entrada
  por lote.

## 6. Desenho

### 6.1 Feed em disco

- Mora em `<dataset>/replication/feed/`, sob o `LOCK` do Store base. A tabela
  histórica é outro Store, com outro diretório, mas publica no MESMO feed, que
  pertence ao base.
- O feed é um log de **segmentos**. Cada lote fechado (`flushLocked`) vira um
  **quadro** com cabeçalho e CRC32C. O cabeçalho leva `from`, `to`, o instante,
  o motivo do fechamento, o número de entradas e a posição em bytes. O quadro
  carrega as entradas já consolidadas. O segmento fecha quando passa do teto
  dele (*minha escolha:* constante de 64 MiB, ou `maxDiskBytes/8` se for
  menor). O nome do arquivo é a primeira `sequence` do segmento, com 20 dígitos.
- A retenção é feita no fim do log: quando o total passa de `maxDiskBytes`, os
  segmentos mais antigos são apagados inteiros, e o piso do feed vira o `to`
  do último quadro apagado. A retenção nunca espera leitor nenhum, nem o
  checkpoint de um publicador. É exatamente o binlog.
- **Gravação fora do `Publish`** (*minha escolha*): o lote fechado entra numa
  fila de gravação, e uma goroutine do feed o grava com `write(2)`, sem fsync.
  Um lote só pode sair da memória depois de gravado. Se os lotes ainda não
  gravados lotam `maxBufferedBytes` (disco travado ou lento), o feed invalida o
  intervalo, como hoje, e declara lacuna. A ingestão não para.
- **Erro de I/O** (cheio, EIO): o feed conta o erro
  (`errors{reason="disk"}`), emite WARN com limite de frequência, invalida o
  intervalo (lacuna) e tenta um segmento novo no próximo lote. Erro do feed
  nunca deixa o writer do Store falho, nem para a ingestão.
- **Leitura.** O `Acquire(epoch, after)` serve da memória quando o lote está
  no cache. Senão, lê do disco. *Minha escolha:* quadros consecutivos lidos do
  disco são agrupados num único lote de leitura, até `maxBatchBytes` e
  `maxMessages`. Sem isso, um feed com `flushInterval: 0s`, que tem um quadro
  por alteração, alcançaria o tail a uma ida e volta do Kafka por alteração. O
  lote lido do disco é do leitor: sai da memória dele no `release` e não entra
  na conta do cache. O pico extra é de um lote por leitor ativo.
- **Índice esparso em heap** (*minha escolha*): uma entrada
  `(sequence, offset)` a cada ~1 MiB de segmento, mais o primeiro e o último
  `to` de cada segmento. Com 1 GiB, são ~1.024 entradas. A leitura busca a
  entrada mais próxima e varre os quadros a partir dela.
- **Posição em bytes.** Cada quadro sabe a posição acumulada do fim dele desde
  o início da geração. É o que dá o atraso em bytes: posição produzida menos a
  posição do cursor confirmado.
- A consolidação de updates da mesma entidade continua igual: dentro do
  intervalo ativo e nunca cruzando outra operação. Quadros lidos do disco e
  agrupados não são consolidados entre si.

### 6.2 Estado do feed, encerramento limpo e queda

- `<dataset>/replication/feed/state` é gravado por `atomicfile`, com CRC.
  Guarda a versão, a `epoch`, a última `sequence`, o piso, a posição em bytes,
  a marca `clean` e a **impressão do Store** no instante do fechamento.
- **Impressão do Store** (*minha escolha*): é a posição de escrita do base e
  da histórica (segmento corrente e offset do fim do último record válido)
  mais o `NextRowID` de cada um. Na subida, depois da recovery dos dois Stores,
  ela é comparada com a posição recuperada. Qualquer diferença quer dizer que
  alguém mexeu no Store fora do feed: um comando offline sob o mesmo `LOCK`,
  uma restauração de backup, ou um record que o fsync final não levou ao
  disco. A diferença é tratada como queda.
- **Encerramento limpo**, na ordem:
  1. param todos os produtores de mudança no Store: ingestão, coletor,
     compaction, retenção, rotas de mutação e contadores;
  2. checkpoint dos Stores (base e histórica), com o fsync dos segmentos;
  3. o feed fecha o intervalo ativo, grava o que falta, faz fsync dos
     segmentos e do diretório, e grava `state` com `clean=true` e a impressão;
  4. os publicadores drenam o resto do feed, dentro do orçamento atual de
     10 s, e gravam o checkpoint deles;
  5. fecham os Stores.

  Hoje o feed fecha ANTES de os Stores pararem. As mudanças feitas entre um e
  outro (expiração, compaction, retenção) ficam fora do feed. Com retomada,
  esse buraco viraria divergência, por isso a ordem muda.
- **Guarda contra mutação tardia:** um `Publish` depois do fechamento do feed
  não é ignorado em silêncio. Ele regrava `state` com `clean=false` (e emite
  ERROR), e a próxima subida segue o caminho da queda.
- **Subida:**
  1. o feed abre e lê `state`;
  2. os Stores abrem e se recuperam. O replay não publica, como hoje. Nenhum
     `Publish` pode acontecer antes do passo 3; o plano confere cada caminho
     da abertura;
  3. o feed decide. Se `clean=true` e a impressão bate, valida o fim do último
     segmento (CRC e continuidade das sequences), regrava `state` com
     `clean=false` de forma durável ANTES do primeiro `Publish` e continua com
     a mesma `epoch` e `sequence`. Senão, é queda: apaga os segmentos, gera
     `epoch` nova, a `sequence` volta a 0, e o feed guarda o motivo (`crash`).
- **Lacuna em operação** (memória de lotes não gravados lotada, erro de I/O,
  registro maior que o teto): a mesma `epoch`, o piso salta e os segmentos
  anteriores são descartados. É o `gap` de hoje.

### 6.3 Checkpoint do publicador Kafka

- O arquivo é `<dataset>/replication/kafka/<destino>.ckpt`, gravado por
  `atomicfile` com CRC. Guarda a versão, a `epoch`, o `cursor` confirmado, as
  partições fixadas, o tópico e a impressão do cluster (os `bootstrapServers`
  normalizados como na conferência do débito 133).
- **Quando grava:**
  - depois do `snapshot_end` confirmado em todas as partições;
  - depois de lotes do tail confirmados, no máximo uma vez por segundo
    (*minha escolha*);
  - no encerramento, depois da drenagem.

  O checkpoint pode ficar atrás do confirmado: a retomada reentrega o
  intervalo, e a entrega já é at-least-once.
- **Antes do `reset`**, o checkpoint é invalidado de forma durável. Um
  reinício no meio do snapshot não pode retomar o tail de uma posição antiga,
  porque deixaria o consumidor em reconstrução sem `snapshot_end`.
- **Decisão na subida:**

  | Situação | Ação |
  |---|---|
  | sem checkpoint (primeira vez, destino novo) | `reset` com `startup` |
  | tópico ou cluster diferentes do checkpoint | `reset` com `startup` |
  | checkpoint invalidado por snapshot interrompido | `reset` com `snapshot_failed` |
  | feed nasceu de queda (epoch diferente) | `reset` com `crash` |
  | mesma epoch, cursor abaixo do piso do feed | `reset` com `retention` |
  | número de partições diferente | `reset` com `partitions_changed` |
  | mesma epoch, cursor retido, mesmas partições | **retoma o tail**, sem registro de controle novo* |

  \* Pedido do TEMS (16/10): sai um registro de controle `resume` por
  partição, com `epoch` e `cursor` (a última posição confirmada; o tail segue
  com `sequence > cursor`). Serve só para observabilidade, e o consumidor não
  precisa fazer nada com ele. Não tem barreira de ack: ele vai no primeiro
  lote do tail.
- **Contrato do `reset`**, a pedido do TEMS:
  - o `reason` vai também no header Kafka `nishidb-reason`;
  - `previousEpoch` vai no corpo quando a epoch muda. Ele é a última epoch
    publicada NESTE destino, guardada no `.ckpt` mesmo quando o checkpoint é
    invalidado. Fica vazio quando não há nenhuma.
- **Impressão da renderização** (*minha escolha*): o `.ckpt` guarda um hash
  do que muda o `record` renderizado, ou seja, as tabelas publicadas, o rótulo
  de cada FieldID e `publishHistory`. Se o hash diverge na subida, o `reset`
  sai com o motivo próprio `config_changed`, que o TEMS pediu em 16/10 para
  distinguir "mudou o formato" de "primeira subida". Sem isso, um alias trocado no
  YAML deixaria o consumidor com rótulos velhos e novos misturados.
- Na operação, nada muda: `gap` para lacuna e `partitions_changed` na
  conferência periódica. O ERROR de livelock passa a recomendar
  `maxDiskBytes`.

### 6.4 `publishHistory`

- `replication.publishHistory`: padrão `true`, o legado. Com `false`, a
  histórica não entra no feed: nem no snapshot, nem no tail, nem os `retain`.
  Isso vale para Kafka e followers HTTP juntos.
- O `/v1/replication/cursor` anuncia se a histórica é publicada. Um follower
  que declara histórica, ligado a um master que não a publica, vai para um
  estado de erro nomeado e não fica com a tabela vazia em silêncio
  (*minha escolha*).
- Mudar a chave exige reinício. Ela entra na impressão da renderização (6.3),
  então o destino faz `reset` com `config_changed`. Com `false`, o `count` do
  `snapshot_end` conta só a base.

### 6.5 Configuração

| Chave | Padrão | Regra |
|---|---|---|
| `replication.maxDiskBytes` | 2 GiB | ≥ 4 × teto do segmento e ≥ 2 × `maxBatchBytes`; recusa fora da faixa |
| `replication.maxBufferedBytes` | 64 MiB (igual) | teto de memória do feed (cache + lotes ainda não gravados) |
| `replication.publishHistory` | `true` | `*bool`: distingue ausente de explícito |

`maxDiskBytes` só vale no master. No follower, é recusado na subida.

**Dimensionamento** (medido pelo TEMS em 16/10/2026 no CTP): 412 alterações/s
em média, 476/s numa hora de tarde, e storms estimados em 2–3× a média. Cada
entrada custa ~2,3 KB, o que dá ~0,95 MB/s, ou ~3,4 GB por hora. A cobertura
em horas é `maxDiskBytes / (taxa × custo por entrada)`. O padrão de 2 GiB cobre
~35 min a essa taxa. O padrão é modesto de propósito, e a doc ensina a conta. No piloto, o
`events` foi recriado em 16/10, e a .228 ficou com 142 GB livres. O aceite
pode usar um valor maior, por exemplo 8 GiB, que dá ~2 h 20 min.

### 6.6 Observação

- **`_meta.replication`**: uma linha por `(dataset, destino)`, com:
  - `state`, `epoch`;
  - `cursor` (confirmado em memória) e `checkpoint_cursor` (gravado);
  - `produced`, `floor`, `lag_sequences` e `lag_bytes`;
  - `feed_disk_bytes`, `feed_cache_bytes` e `feed_segments`;
  - `last_reset_reason`, `last_success_at`.

  Os nomes finais saem no plano, com o glossário.
- **Métricas novas**, todas nascendo em zero:
  - `nishidb_replication_feed_disk_bytes`;
  - `nishidb_replication_feed_disk_segments`;
  - `nishidb_replication_feed_disk_errors_total`;
  - `nishidb_replication_kafka_lag_bytes`;
  - `nishidb_replication_kafka_checkpoint_sequence`;
  - os motivos `crash` e `retention` em `snapshots_total{reason}`.
- `/v1/stats` e `/metrics` leem a mesma estrutura, como já é regra.

### 6.7 Followers HTTP

Com a `epoch` estável, um follower que continua no ar enquanto o master
reinicia de forma limpa retoma o tail sem snapshot. Leitura do disco, piso e
`409` seguem a mesma lógica do `Acquire`.

## 7. Garantias que o plano tem de provar com teste e mutação

1. **Queda no meio da gravação:** um quadro rasgado no fim do segmento é
   detectado pelo CRC. Sem `clean=true`, o feed inteiro é descartado. Com
   `clean=true`, um quadro inválido também leva à queda, nunca a uma
   retomada truncada.
2. **Leitor concorrente:** o leitor no disco e a retenção apagando o segmento
   que ele lê. O fd aberto continua válido, e o próximo `Acquire` abaixo do
   piso devolve `ErrResyncRequired`.
3. **Ciclo de vida:** criação, rotação de segmento, retenção, encerramento
   limpo, reabertura com a mesma `epoch`/`sequence`, reabertura depois de
   queda com `epoch` nova, mutação tardia e mutação offline.
4. **Falha de infra:** ENOSPC/EIO na gravação viram lacuna e contador, sem
   parar a ingestão. Falha ao gravar o checkpoint do publicador não para o
   publicador: ela conta, e a retomada seguinte vem de um checkpoint mais
   antigo.
5. **Modo somente leitura e comandos offline:** não tocam o feed, e a
   impressão do Store pega a mutação deles.
6. Nenhuma `(epoch, sequence)` é reaproveitada com conteúdo diferente.
7. Heap limitado: memória do feed ≤ `maxBufferedBytes` + um lote por leitor,
   independente de `maxDiskBytes`.

## 8. O que fica para o builder

Codificação binária do quadro e das entradas (com golden test de bytes, como o
`valuekey`), o tamanho exato do índice esparso, os nomes finais das colunas e
das métricas (com o glossário), o desenho interno da fila de gravação, e onde
mora o código da `_meta.replication`. Pelo padrão do `internal/metakafka`, deve
ser um pacote próprio que importa `replication` e `replicationkafka`.

## 9. Pontos do código que o desenho exige (mapeados em 16/10)

- O encerramento hoje não faz checkpoint de dataset sem ingestão Kafka. O
  `Store.Close` não checkpointa, então o passo 2 de 6.2 é explícito.
- Coletor, compaction e retenção só param de fato no `Store.Close`. O passo 1
  usa os `For*` públicos e idempotentes do Store (`ForExpiration`,
  `ForCompaction`, `ForRetention`). O `http.Shutdown` também tem de vir antes
  do fechamento do feed, porque `/v1/compact` publica.
- O gate `TestLacosDoServidorParamAntesDoQuiesceDosDatasets` fixa a ordem atual
  e muda no mesmo PR.
- O Store ganha um método exportado que devolve a posição de escrita (segmento
  corrente, offset e `NextRowID`), para a impressão de 6.2.
- Os deletes de TTL vencidos com o processo parado saem pelo coletor na subida,
  pelo `Expira`, com o último estado vivo (`prevPayload = rec.Payload`), igual
  aos deletes normais.

## 10. Decisões do usuário (16/10/2026)

### 10.1 Restauração de backup: conferência no tópico (opção A)

O feed e os `.ckpt` moram na pasta do dataset. Um backup frio restaurado traz
os dois coerentes, mas no passado. Retomar dali reusaria `(epoch, sequence)`
já entregues com outro conteúdo (anti-objetivo 3).

- Antes de retomar, o publicador lê o último registro de cada partição do
  tópico. Os registros de dado e de controle carregam `epoch` e `sequence` ou
  `cursor`.
- Se algum registro da MESMA `epoch` tiver posição acima da `sequence`
  produzida pelo feed, o feed voltou no tempo. Nesse caso o destino não
  retoma: sai um `reset` com o motivo `rewind`, e o feed troca de `epoch`.
  Sem a troca, as `sequence`s seguintes continuariam colidindo.
- O custo é uma leitura por partição na subida, e só quando há retomada.
- Se a leitura falha por infraestrutura, a retomada espera e tenta de novo,
  como a entrega. Ela nunca retoma às cegas.
- Limite conhecido, a documentar: o tópico vazio (retenção venceu) não tem o
  que conferir.

### 10.2 Snapshot sob demanda e periódico

- **Sob demanda:** uma rota ADMIN pede o snapshot de um destino. Ela é
  registrada por `rt.registra`, com o caminho final definido no plano e
  distinto do `GET /v1/replication/snapshot` dos followers. O CLI ganha o
  comando equivalente por `--server`. O `reset` sai com o motivo `requested`.
  O pedido é registrado no `.ckpt` antes de responder, e por isso sobrevive a
  um reinício que aconteça antes do snapshot.
- **Periódico:** `replication.periodicSnapshot` (duração; padrão desligado,
  *minha escolha:* vale para todos os destinos Kafka do dataset). O relógio
  conta a partir do último `snapshot_end` confirmado do destino, gravado no
  `.ckpt`, e por isso também sobrevive ao reinício. O `reset` sai com o motivo
  `periodic`. A doc recomenda um valor abaixo da retenção do tópico (o TEMS
  usa 7 dias).
- O TEMS tolera motivo desconhecido, então os motivos novos entram sem
  coordenar o deploy.

### 10.3 Aceite no piloto

O usuário esclareceu que o `events` é o próprio dataset de alarmes ativos.
Em 16/10, às 18:14, o `events` da .228 foi recriado (grupo
`nishidb-events-2`, `earliest`) com 0,7–1,6 M linhas vivas esperadas, e o
`active_alarms` foi aposentado. A `events_history` fica, com 80 GiB e 2 dias.
O CDC está desligado até esta release.
Proposta de aceite, na .228, com o `events` e `publishHistory: false`:

- o reinício limpo retoma sem snapshot (`resume` no tópico);
- o kill -9 faz snapshot com `crash`;
- um Kafka fora por alguns minutos é atravessado sem snapshot;
- o snapshot sob demanda funciona;
- o heap fica estável com o feed em disco cheio.

### Lista final de motivos do `reset`

`startup | crash | rewind | retention | gap | snapshot_failed |
partitions_changed | config_changed | requested | periodic`.
