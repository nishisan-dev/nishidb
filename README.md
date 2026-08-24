# NishiDB

Engine de materialização e consulta para fluxos de eventos JSON parciais.

Recebe eventos, mantém o **estado corrente** de cada entidade (merge por campo,
controle de sequência) e responde consultas SQL sobre esse estado — com o
dataset inteiro cabendo num orçamento de memória fixo.

Medido com **8 milhões de entidades**: 483 MiB de RSS de pico (limite: 1 GiB),
133 mil eventos/s de ingestão e point lookup em **18 microssegundos** pelo
caminho SQL completo. O heap Go da ingestão fica em **396 KiB com 8 milhões de
rows**, contra 389 KiB com 1 milhão — 8× de dados, 1,9% de heap a mais.

## Estado

**M1, M2 e a Fase 6 do M3 completos; o M4 entregue, com uma ressalva que
vale ler; a Fase 11 (autenticação e TLS) do M5 entregue.** Ingestão por
arquivo e por **Kafka**, materialização, storage durável com recovery de
dados E de posição no tópico, índices, SQL, saída NDJSON, um servidor que
**consulta enquanto ingere** pelo protocolo remoto do §67, a **compaction**
que devolve ao filesystem o espaço dos records superados, e agora
**autenticação HTTP Basic sobre TLS com papéis READ/ADMIN** no protocolo
remoto — tudo pelo binário. Na rodada de aceite do M4 a compaction devolveu
**3,68 GB** e derrubou o crescimento de disco de 91 para 26 MiB/min; **parar
de crescer, ela ainda não parou** nessa escala, e o
[aceite do M4](docs/aceite-m4.md) mostra a recontagem. Os critérios de aceite
estão em
[`docs/aceite-m1.md`](docs/aceite-m1.md),
[`docs/aceite-m2.md`](docs/aceite-m2.md),
[`docs/aceite-fase6.md`](docs/aceite-fase6.md),
[`docs/aceite-m4.md`](docs/aceite-m4.md) e
[`docs/aceite-m5.md`](docs/aceite-m5.md).

**Autenticação e TLS (§74-77) chegaram, mas o piloto ainda fala com a versão
sem senha.** O mecanismo está pronto e testado — a seção "Segurança" abaixo
mostra o exemplo mínimo de config —, mas ligá-lo em produção (certificado da
CA do piloto, usuários reais, a porta exposta com firewall restrito) é a
**Fase C** do ciclo, ainda não rodada; [`docs/aceite-m5.md`](docs/aceite-m5.md)
registra o que falta.

Próximos, na ordem do roadmap: a **Fase 7** (driver JDBC, §70-73), que agora
pode seguir — publicá-la antes da auth multiplicaria os pontos de acesso não
autenticado em vez de fechar um, e essa condição já não vale mais. Depois vem
o **M6** (TTL e tombstones por retenção, índice hash físico).

## Instalação

```bash
go build -o nishidb ./cmd/nishidb
```

Binário único, sem dependência externa em runtime. Requer Go 1.25+ e Linux
(o storage usa `mmap` e `fallocate` diretamente).

## Uso

### 1. Configuração

`nishidb.yaml`:

```yaml
version: 1

storage:
  path: /var/lib/nishidb
  segmentSize: 512MiB
  checkpoint:
    interval: 30s
  compaction:
    enabled: true
    minimumLiveRatio: 0.50
  expiracao:
    maxPorPassada: 1000     # marcações por passada do coletor
    tempoPorPassada: 5s     # teto de tempo da passada; vence o que bater primeiro

datasets:
  directory: /etc/nishidb/datasets

query:
  defaultMaxRows: 100000
  defaultTimeout: 30s
```

`query.defaultMaxRows` e `query.defaultTimeout` são os **defaults globais** do
teto de linhas e do timeout de consulta: valem para o dataset que **omite**
`query.maxRows`/`query.timeout` no YAML dele, e perdem para o dataset que os
declara — o global é default, não teto. Omitir o bloco `query` inteiro vale o
padrão da spec (100000 e 30s). Valor não positivo é **recusado** no
carregamento, com a chave e o valor nomeados no erro.

`version`, `query.resultBatchSize` e o bloco `server` **passaram a ter leitor
no M2**: `version` diferente de 1 é recusado no carregamento, `resultBatchSize`
não positivo também, e `server.enabled: false` faz o `nishidb server` recusar a
subida em vez de ignorar a chave. `server.tls`, `server.authentication` e
`server.loopback` **ganharam leitor na Fase 11** (§74-77) — ver "Segurança:
autenticação e TLS" abaixo. O que ainda não tem implementação
(`runtime`, `storage.durability`) continua aceito e ignorado de propósito,
para que o arquivo do M3 continue válido para este binário —
`storage.durability` está registrada como débito 50, porque quem lê
`interval: 100ms` conclui uma garantia que não existe.

`storage.compaction` **ganhou leitor no M4** e é o que decide se o disco para de
crescer (§78). `enabled` **ausente vale `true`**: o exemplo do §78 traz a chave
ligada, e exigir declaração explícita inverteria o default seguro — todo YAML
herdado do M1 acumularia garbage até o ENOSPC que o §86 projeta em 2 a 3 dias.
Só `enabled: false` explícito desliga, e a subida **avisa** quando desliga, para
que a decisão apareça no log. `minimumLiveRatio` é a fração de bytes vivos
abaixo da qual um segmento **selado** deixa de valer a pena manter como está;
vale `0.50` quando ausente, e é recusado no carregamento fora de `(0, 1)`
exclusive — inclusive `.nan`, que a faixa sozinha não pegaria e que faria a
política nunca eleger ninguém, com o arquivo dizendo o contrário.

O bloco `server` é lido só pelo comando `server`; `ingest` e `query` o ignoram.
`enabled` **ausente vale `true`** — rodar `nishidb server` já é o ato de ligar,
e exigir a chave também seria cerimônia dupla. Só `enabled: false` explícito
recusa.

`storage.checkpoint.interval` só age enquanto o checkpoint periódico estiver
ligado: `nishidb ingest --checkpoint-every 0` desliga o disparo por contagem
**e** o por tempo, e o intervalo acima vira letra morta.

### 2. Definição do dataset

`/etc/nishidb/datasets/radius_sessions.yaml` — o nome do arquivo é o nome do
dataset:

```yaml
version: 1
name: radius_sessions

materialization:
  key:
    source: payload
    field: id
    type: string
  mode: merge
  sequence:
    field: _SEQ
    type: int64
    strategy: greater

schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
      alias: service_id
    # O campo de sequence precisa ser declarado como qualquer outro: sem esta
    # entrada o dataset não abre.
    _SEQ:
      type: int64
    Acct-Status-Type:
      type: string
      alias: status
      index: bitmap
    Acct-Terminate-Cause:
      type: string
      alias: terminate_cause
    NAS-IP-Address:
      type: ipv4
      alias: nas_ip
      index: hash
    User-Name:
      type: string
      alias: username
```

Um campo **não declarado** aqui continua sendo aceito (`unknownFields:
include`) e vira coluna `auto`, mas só é consultável pelo **nome original** —
o alias existe apenas para campo declarado.

O `index: bitmap` dos campos é **definitivo**: acrescentar um bitmap passa a ser
recusado assim que o manifest registra a primeira row, e **remover** um bitmap
existente é recusado desde a criação — os dois com exit 3, num dataset íntegro,
e o tamanho dos valores não tem nada a ver com isso. Não há backfill nem
migração in-place: a única saída é reprocessar a fonte para um dataset com o
conjunto definitivo. Vale escolher agora — a mecânica e os dois caminhos
possíveis estão em [`docs/architecture.md`](docs/architecture.md), seção
"Operar isto".

`mode: replace` (no lugar de `merge`) faz cada evento substituir o estado
inteiro da chave. Ele funciona, mas **não aparece no resumo do `ingest`**: a
quebra impressa é criados/atualizados/stale, e a substituição não cai em
nenhuma delas — num dataset `replace` o total de aplicados fica certo e o
detalhe não fecha com ele (débito 48).

### 3. Ingerir

```console
$ cat eventos.ndjson
{"id":"CMP-1","_SEQ":1,"Acct-Status-Type":"Stop","NAS-IP-Address":"10.0.0.1","User-Name":"joao"}
{"id":"CMP-2","_SEQ":1,"Acct-Status-Type":"Start","NAS-IP-Address":"10.0.0.2","User-Name":"ana"}
{"id":"CMP-1","_SEQ":2,"Acct-Terminate-Cause":"User-Request"}

$ nishidb ingest --config nishidb.yaml --dataset radius_sessions --input eventos.ndjson
ingest: 3 lidos, 3 aplicados (2 criados, 1 atualizados, 0 substituídos, 0 stale) em 10ms (289 ev/s)
```

O terceiro evento é um **patch**: ele acrescenta um campo à sessão `CMP-1` sem
tocar nos demais. Aceita `-` para ler de stdin.

**`--checkpoint-every N`** (padrão **500.000**) controla o checkpoint periódico,
e é a flag que decide o consumo de memória da carga. O overlay de locations vive
no heap até um checkpoint aplicá-lo ao mmap: sem checkpoint, a memória acompanha
o **tamanho do dataset** em vez de ficar constante.

- O disparo é por contagem **ou** por tempo, o que vier primeiro — o tempo é o
  `storage.checkpoint.interval` do `nishidb.yaml` (30s no exemplo acima).
- **`--checkpoint-every 0` desliga os dois**, contagem e tempo: o
  `checkpoint.interval` fica inerte e a carga inteira acumula overlay no heap.
  O comando aceita, avisa no stderr e segue — é para casos em que a carga é
  curta e a pausa do `msync` incomoda mais que a memória.
- Valor negativo é recusado (exit 1). O checkpoint final, no fim da entrada,
  acontece sempre — inclusive com `0`.

### 4. Consultar

```console
$ nishidb query --config nishidb.yaml --dataset radius_sessions \
    "SELECT id, status, nas_ip, terminate_cause FROM radius_sessions WHERE status = 'Stop'"
{"id":"CMP-1","status":"Stop","nas_ip":"10.0.0.1","terminate_cause":"User-Request"}
```

Saída em NDJSON — uma linha por row, em streaming. Colunas ausentes saem como
`null` explícito.

O erro de consulta aponta a posição:

```console
$ nishidb query --config nishidb.yaml --dataset radius_sessions \
    "SELECT id FROM radius_sessions
     WHERE status = 'Stop' AND naoexiste = 1"
query: coluna "naoexiste" não existe no dataset "radius_sessions" (posição 62)
  linha 2, coluna 32:
         WHERE status = 'Stop' AND naoexiste = 1
                                   ^
```

**`--limit N`** sobrepõe o `LIMIT` da query: `--limit 3` sobre
`SELECT id FROM t LIMIT 1` devolve 3 linhas, não 1. `--limit 0` é válido e
devolve zero linhas; sem a flag, o `LIMIT` escrito na query é o que vale.

Dois detalhes que a flag exige saber:

- Ela **reescreve o texto** da consulta — corta a cláusula `LIMIT` existente e
  anexa a sua —, porque a v1 não tem serializador de AST. O corte usa o mesmo
  lexer do `Execute`, então **query que não tokeniza não é cortada**: a flag é
  só anexada e o comando sai com o erro de sintaxe na posição real. O caminho
  definitivo (parâmetro do plano) é o débito 46.
- O `query.maxRows` do dataset continua valendo como **teto**: `--limit` acima
  dele não levanta o teto, e resultado truncado por ele sai com exit 5.

### Ingestão por Kafka

O YAML do dataset descreve a fonte (§12). O consumer só sobe com
`enabled: true` **e** por ato de comando — descrever não é ligar:

```yaml
source:
  type: kafka
  kafka:
    enabled: true
    bootstrapServers: [kafka01:9092, kafka02:9092]
    topic: radius-accounting
    consumer:
      groupId: nishidb-radius
      autoOffsetReset: earliest    # só vale com dataset VAZIO
      maxPollRecords: 1000
      ingestionQueueSize: 5000     # gate de registros da fila limitada
      ingestionQueueBytes: 64MiB   # gate de BYTES — registros não são memória
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
      usernameEnv: KAFKA_USERNAME  # o NOME da variável, nunca o valor
      passwordEnv: KAFKA_PASSWORD

errors:
  invalidEvent: log-and-skip       # ou `stop`, que PARA a ingestão
```

```bash
nishidb server --config nishidb.yaml
```

O servidor abre os datasets no **mesmo processo** em que consome, porque o
flock do dataset é exclusivo por processo. Expõe o protocolo remoto do §67 —
`POST /v1/query`, `GET /v1/metadata`, `GET /v1/stats` e `GET /v1/health` —
mais `GET /metrics` (§92/§93), e aceita `--pprof <addr>` para os endpoints de
debug num listener próprio.

O bind default é **`127.0.0.1`**. Fora do loopback, o servidor agora EXIGE
uma decisão sobre autenticação — ver a próxima seção — em vez de só avisar.

```bash
# consultar COM a ingestão de pé
curl -sS -H 'Content-Type: application/json' \
  -d '{"dataset":"radius_sessions",
       "sql":"SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
       "parameters":[{"type":"string","value":"Stop"}]}' \
  127.0.0.1:9142/v1/query

# o mesmo pelo CLI, com o parâmetro (as flags vêm ANTES da query)
nishidb query --server 127.0.0.1:9142 --dataset radius_sessions \
  --param string:Stop \
  'SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100'
```

A resposta é NDJSON com moldura: a primeira linha descreve as colunas com
tipo (o `ResultSetMetaData` do §71 precisa deles, e uma coluna nula em todas as
linhas não os revela), as do meio são as rows do §69, e a última diz como o
resultado terminou — `end` com `truncated`, ou `error` com o código. É isso que
permite distinguir "acabou" de "quebrou na linha 10.000", já que o status HTTP
viaja com o primeiro byte.

> **`nishidb query --config` continua barrado pelo flock** com o servidor no
> ar, e isso não é limitação a remover: o flock é exclusivo por processo. Use
> `--server`, que é o caminho para o processo que já tem o dataset aberto — a
> mensagem do exit 3 o sugere.

Um dataset cuja ingestão não pode subir **continua consultável** — a ordem do
§51 é `start queries` antes de `resume ingestion`. Quatro situações recusam a
subida da ingestão, todas visíveis no health: posição desconhecida (dataset com
linhas e sem `kafka.offsets`), posição ilegível, origem divergente (o YAML
aponta para outro tópico, group ou cluster), e assignment parcial (outra
instância no mesmo group).

### Segurança: autenticação e TLS (§74-77)

Exemplo mínimo com os três blocos — TLS, usuários e o listener loopback
opcional:

```yaml
server:
  bind: 0.0.0.0
  port: 9142
  tls:
    enabled: true
    certificateFile: /etc/nishidb/tls/server.crt
    privateKeyFile: /etc/nishidb/tls/server.key
  authentication:
    enabled: true
    users:
      - username: reader
        passwordFile: /etc/nishidb/secrets/reader   # chmod 600, senão a subida recusa
        role: READ
      - username: admin
        passwordEnv: NISHIDB_ADMIN_PASSWORD          # nome da variável, nunca o valor
        role: ADMIN
  loopback:
    enabled: true
    port: 9143   # painel/script/cron locais continuam sem credencial aqui
```

Bind fora do loopback exige uma decisão EXPLÍCITA sobre `authentication` — a
seção ausente do arquivo **recusa a subida** (o typo mais caro seria
`authentification:` sumindo em silêncio), e `authentication.enabled: true`
sem `server.tls.enabled: true` também recusa: HTTP Basic sem TLS transmitiria
a senha em texto claro pela rede. O listener **loopback** é opcional
(`server.loopback.enabled`, default desligado) e serve o MESMO protocolo sem
TLS nem credencial, só em `127.0.0.1` — é o que mantém painel, script de
atualização e cron locais funcionando sem senha mesmo com a porta exposta
trancada. Uma rota **ADMIN** (`/v1/compact`, `/v1/checkpoint`) responde 403
também no loopback: esse listener é READ-only por construção.

```bash
# rota de leitura, autenticada, sobre TLS
curl -sS --cacert /etc/nishidb/tls/ca.pem -u reader:senha \
  https://127.0.0.1:9142/v1/health

# o mesmo pelo CLI
nishidb status --server 127.0.0.1:9142 --tls-ca /etc/nishidb/tls/ca.pem \
  --user reader --password-file /etc/nishidb/secrets/reader-cli

# rota mutante, exige ADMIN — READ sai 403
nishidb compact --server 127.0.0.1:9142 --tls-ca /etc/nishidb/tls/ca.pem \
  --user admin   # senha por NISHIDB_PASSWORD ou --password-file, nunca por flag
```

Sem lockout — um contador por IP ou usuário abriria a porta para trancar a
conta de outra pessoa —, mas toda falha de autenticação paga um atraso fixo
(~100ms) antes da resposta, e o 401 é byte-idêntico entre "usuário não
existe" e "senha errada". `--user` sem senha nenhuma, ou com duas fontes ao
mesmo tempo, é recusado pelo CLI antes de qualquer rede; `--password` como
flag não existe (apareceria em `ps`) — a senha só entra por
`NISHIDB_PASSWORD` ou `--password-file`. `--insecure`/`InsecureSkipVerify`
não existe em nenhum cliente do produto: `--tls-ca` aponta a CA que o
cliente deve confiar, e um redirect de qualquer tipo é recusado, nunca
seguido com a credencial junto.

**O que este README não cobre é a rodada no piloto** — certificado de uma CA
real, usuários de verdade, a porta liberada no firewall. Isso é a Fase C do
ciclo e está registrada, pendente, em [`docs/aceite-m5.md`](docs/aceite-m5.md).

### Reposicionar no tópico

```bash
nishidb offsets --config nishidb.yaml --dataset radius_sessions --show
nishidb offsets --config nishidb.yaml --dataset radius_sessions --set 0:1500,1:900 --yes
nishidb offsets --config nishidb.yaml --dataset radius_sessions --reset earliest --yes
```

Exige o servidor **parado** (toma o flock) e `--yes` para escrever. É a saída
para `invalidEvent: stop`, para um restore parcial, e para a troca legítima de
tópico ou cluster.

### Compaction: fazer o disco parar de crescer

Segmentos são **append-only**: todo update apenda um record novo e deixa o
anterior como garbage. A 500 ev/s contínuos, o §86 projeta **30-43 GB escritos
por dia** contra 5,6-8 GB de estado vivo — num disco de 50-100 GB são 2 a 3 dias
até o ENOSPC. A compaction do §53-55 é o que devolve esse espaço, e ela roda
**sozinha**, em background, dentro do `nishidb server`:

- **elege** os segmentos **selados** cuja fração viva caiu abaixo do
  `minimumLiveRatio`. O segmento **corrente nunca** é eleito — compactá-lo seria
  copiar dentro de si mesmo;
- **varre** o candidato record a record, em streaming, sem carregar o segmento
  em memória (§54), e **copia para frente** apenas os records que a location
  ainda nomeia. A verificação e a cópia acontecem na **mesma seção crítica** do
  writer: fora dela, um update que chegasse no meio faria o payload antigo
  vencer o novo — sem erro, sem log e com o CRC batendo;
- **aposenta** o arquivo por **rename** para `.retired`, e só remove os bytes
  depois que o manifest concorda em não precisar deles. Um `kill -9` no meio
  disso deixa um estado que a próxima abertura repara sozinha, restaurando ou
  removendo o arquivo conforme o manifest.

**O que foi medido, e até onde.** Na rodada de aceite do M4 — 1 milhão de
entidades, 8,4× o key set reescrito, segmentos de 64 MiB — a compaction devolveu
**3,68 GB** em 104 aposentadorias e derrubou o crescimento de disco de 91 para
26 MiB/min ao longo da janela. **O disco não parou de crescer nessa escala**, e
o gate `disk_plateau` reprova quando recontado sem a amostra colhida fora da
janela de carga. O número, a recontagem e o que falta medir estão no
[aceite do M4](docs/aceite-m4.md).

Isso é o laço AUTOMÁTICO: a política é a configuração, e o ato do operador é
ligá-la ou desligá-la. Desde a Fase 11 existe também o modo MANUAL —
`POST /v1/compact` e `nishidb compact --server`, atrás do papel ADMIN —, para
disparar uma passada sob demanda (funciona inclusive com
`storage.compaction.enabled: false`, o caso de uso central do modo manual).
Uma passada em voo, do laço automático OU de outra chamada manual, faz uma
segunda chamada devolver **409** (`COMPACTION_BUSY`) em vez de enfileirar —
ver "Segurança" acima para a credencial. O que se acompanha em regime são as
séries do §92 — `nishidb_storage_bytes`, `nishidb_storage_garbage_bytes`,
`nishidb_compaction_running` e os dois contadores de bytes — e o `status`
abaixo.

> **O garbage publicado é limite INFERIOR**, e a ressalva viaja junto do número
> em toda superfície. A liveness por segmento é limite *superior* (débitos 38 e
> 45), então `total − vivo` diz "há **pelo menos** isto de lixo em disco, nunca
> menos". É afirmação verdadeira e acionável; tratá-la como exata levaria a
> dimensionar disco por um número que não promete o que se leu nele.

### Ver o estado do servidor no ar

```bash
nishidb status  --server 127.0.0.1:9142
nishidb datasets --server 127.0.0.1:9142
nishidb inspect --server 127.0.0.1:9142 radius_sessions
```

Os três comandos de **leitura** do §96. Eles nascem **remotos**, e não é
economia: o flock do dataset é exclusivo por processo, então um diagnóstico que
só soubesse abrir o dataset local exigiria **derrubar o servidor** para
perguntar o estado dele — a pergunta e a resposta se excluiriam.

`status` traz, por dataset, rows, segmentos, **segmento corrente**, carga do
primário, overlay, disco, garbage, o estado da compaction com os bytes lidos e
escritos, as consultas e a ingestão; e, do processo, os contadores do motor de
consulta. Segmentos e corrente aparecem **separados** de propósito: o ID nunca
regride e a compaction aposenta arquivos, então a distância entre os dois é o
que ela já devolveu.

`datasets` lista o que este servidor serve; `inspect <dataset>` detalha as
colunas com tipo, alias e índice declarado, mais os limites **efetivos** de
`maxRows` e `timeout` — o do dataset, ou o default global que ele herdou.

`compact` e `checkpoint`, que o §96 também lista, são **rotas mutantes** e
chegaram na Fase 11, atrás do papel ADMIN — ver "Compaction" acima e
"Segurança" mais acima para a credencial. `nishidb checkpoint --server
127.0.0.1:9142 [dataset]` dispara um checkpoint síncrono; sem `dataset`, roda
para todos, na ordem determinística da subida, e para no primeiro que falhar
nomeando quem já tinha sido processado com sucesso antes dele.

### SQL suportado (v1)

```sql
SELECT <colunas | *> FROM <dataset>
  [WHERE <predicado>]
  [LIMIT <n>]
```

Comparadores `=`, `!=`, `>`, `>=`, `<`, `<=`; `AND`, `OR`, parênteses;
`IS NULL` / `IS NOT NULL`; alias e campos entre aspas duplas (obrigatórias para
nomes com hífen, como `"Acct-Status-Type"`).

Parâmetros posicionais `?` são fornecidos pelo `parameters` do §68, no
`POST /v1/query`. Os tipos de wire são quatro — `string`, `int64`, `double` e
`boolean` —, e não é limitação do protocolo: é o contrato do binder, que aceita
um caminho de coerção só. Timestamp, endereços e prefixos viajam como `string`
e são convertidos contra o tipo da coluna, pela mesma tabela dos literais SQL
(que é o que o §73 já pressupõe ao mapear `ipv4` para `VARCHAR`).

Pelo CLI, os valores vão em `--param <tipo>:<valor>`, repetível e na ordem dos
`?`. A flag vale nos DOIS caminhos, `--config` e `--server` — um script que
troca um pelo outro não muda mais nada. **As flags vêm antes da query**, porque
o parser de linha de comando para no primeiro argumento posicional. O corte é
no primeiro `:`, então `--param string:http://x:9142` é a URL inteira.

Não há sintaxe de `--param` que produza NULL, e é deliberado: `col = ?` com `?`
nulo não tem semântica sem lógica trivalente, e o que se quer dizer é
`IS NULL` — escrito na query, não passado como valor.

Repetir uma coluna na projeção (`SELECT id, id`) é recusado no bind: a linha
sairia com a chave repetida e o consumidor de JSON colapsaria as duas em
silêncio — duas colunas pedidas, uma entregue. Pedir o mesmo campo pelo nome e
pelo alias (`SELECT id, service_id`) continua valendo, porque aí são duas
chaves distintas na saída.

Fora da v1: `ORDER BY`, `GROUP BY`, `JOIN`, agregações, `NOT` genérico,
`OFFSET`. As sete cláusulas ausentes — `ORDER BY`, `GROUP BY`, `OFFSET`,
`JOIN`, `HAVING`, `DISTINCT`, `UNION` — são recusadas com o código
`UNSUPPORTED_CLAUSE`, nomeando qual delas foi escrita. É o que separa "isto não
existe aqui" de "você digitou errado" para quem escreve SQL numa ferramenta.
Uma coluna chamada `order` continua consultável: as palavras não viraram
keywords do lexer, e a recusa só acontece onde a query já ia falhar.

### Exit codes

| Código | Significado |
|---:|---|
| 0 | sucesso |
| 1 | erro de uso ou configuração |
| 2 | erro de sintaxe ou bind da query |
| 3 | dataset corrompido, travado por outro processo, ou inacessível |
| 4 | writer em modo falho (reabra o dataset) |
| 5 | resultado truncado pelo teto `query.maxRows` |
| 6 | interrompido por sinal |
| 7 | eventos não entraram por falha (perda de dado): investigue e reprocesse |
| 8 | ingestão parada por `errors.invalidEvent: stop` — veja o evento e decida |

O 7 é o que separa perda de sucesso. Evento **ruim** (JSON inválido, key
ausente, tipo incompatível) é descartado pela política §101 e a ingestão termina
com 0 — uma fonte ruidosa não derruba o pipeline. Evento que **não entrou por
falha nossa ou do dataset** (erro do storage, inconsistência interna) sai com 7:
o dataset está íntegro e checkpointado, mas faltam rows.

**Entrada interrompida também é 7**: se a leitura do NDJSON falhar no meio
(erro de I/O, linha acima do teto de 8 MiB do scanner), o que vinha depois não
foi ingerido — a ação é investigar e reprocessar, não conferir a linha de
comando. O erro aparece no stderr como `ENTRADA INTERROMPIDA`.

Quando uma execução acumula mais de um desfecho, o código de saída é o **mais
grave**, não o último ocorrido — na ordem `4 > 3 > 7 > 6 > 1`. O writer em modo
falho vem primeiro porque é o único que exige reabrir o dataset antes de
qualquer reprocessamento; a perda vem antes da interrupção por sinal porque o
sinal foi pedido por quem chamou, e a perda é notícia. O desfecho que perdeu a
precedência continua descrito no stderr.

## Documentação

- [Arquitetura](docs/architecture.md) — como o sistema funciona, com os números.
- [Aceite do M1](docs/aceite-m1.md) — os 25 critérios, item a item.
- [Aceite do M4](docs/aceite-m4.md) — a compaction em regime, com a escala e o
  ambiente da rodada declarados.
- [Aceite do M5](docs/aceite-m5.md) — autenticação e TLS (Fase 11); a rodada
  operacional no piloto (Fase C) segue pendente.
- [Rodada de aceite](benchmark/baselines/aceite-8m-20260815.md) — 8M entidades
  com consultas concorrentes, corpus de referência.
- [Débitos abertos](docs/debitos.md) — o que ficou para o M2, M3, M4 e M5, com dono.

## Desenvolvimento

```bash
go test ./...          # suíte completa, com os gates de medida
go test ./... -race    # detector de corrida, SEM os gates de medida
```

**Os dois alvos são complementares e nenhum substitui o outro** — `-race` não é
"a suíte completa mais uma verificação". Os gates de medida (os de alocação, que
usam `testing.AllocsPerRun`, e o de vazão da ingestão pelo CLI) são
`//go:build !race` por construção: a instrumentação do detector muda a contagem
de alocações e infla o laço de ingestão sem inflar o checkpoint, o que
transformaria cada gate em ruído. Sob `-race` eles ficam **fora do build** e se
anunciam como SKIP. Quem roda só `-race` não está medindo o orçamento de
alocações; quem roda só `go test ./...` não está procurando corrida. O `Makefile`
tem os dois (`make test`, `make test-race`) com essa mesma nota.

A rodada de aceite **não** roda no `go test` padrão: ela grava vários GiB e
exige disco real.

```bash
go build -o ingest-bench ./benchmark/ingest-bench
GOMEMLIMIT=512MiB GOGC=100 ./ingest-bench -data /var/tmp/nb-8m \
    -entities 8000000 -steady 60s -rate 500 -queries 4 -sql-queries 4 \
    -checkpoint-every 500000 -heap-baseline rep-1m.json -report rep-8m.json
```
