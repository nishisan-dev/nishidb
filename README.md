# NishiDB

Engine de materialização e consulta para fluxos de eventos JSON parciais.

Recebe eventos, mantém o **estado corrente** de cada entidade (merge por campo,
controle de sequência) e responde consultas SQL sobre esse estado — com o
dataset inteiro cabendo num orçamento de memória fixo.

Medido com **8 milhões de entidades**: 483 MiB de RSS de pico (limite: 1 GiB),
133 mil eventos/s de ingestão e point lookup em **18 microssegundos** pelo
caminho SQL completo. O heap Go da ingestão fica em **396 KiB com 8 milhões de
rows**, contra 389 KiB com 1 milhão — 8× de dados, 1,9% de heap a mais.

## Releases e driver JDBC

As [GitHub Releases](https://github.com/nishisan-dev/nishidb/releases) oferecem
`nishidb-vx.y.z-linux-amd64` e `nishidb-jdbc.jar` juntos. O driver exige Java 21
ou superior e não precisa de dependências adicionais no classpath. Sua versão
interna acompanha a release: em `v0.8.1`, o JAR e os metadados JDBC informam
`0.8.1`.

Para publicar, crie uma tag no commit desejado **que já contenha o novo workflow**
e envie essa tag ao GitHub, por exemplo:

```sh
git tag -a v0.8.1 -m "NishiDB 0.8.1"
git push origin v0.8.1
```

Use uma tag nova no formato exato `vx.y.z`, sem zeros à esquerda ou sufixos.
A criação apenas local não dispara o workflow; atualizações e exclusões de tags
são ignoradas. Envie cada tag individualmente: o GitHub não emite eventos de
push quando mais de três tags são enviadas juntas. Tags criadas por outro
workflow com `GITHUB_TOKEN` também não disparam esse fluxo automaticamente;
para essa automação, use um token de GitHub App ou pessoal. Esses limites
são descritos na [documentação de eventos do GitHub](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#push).

O workflow **Release** executa a suíte Go e os testes JDBC contra o servidor
real, compila ambos os artefatos e os anexa a um rascunho com notas geradas
automaticamente. A release só é publicada após confirmar os dois uploads.
Publicação e limpeza compartilham uma fila entre todas as tags (até 100
execuções pendentes, limite do GitHub).

Após cada publicação automática bem-sucedida, permanecem as **5 releases mais
recentes pela data de publicação**, incluindo pré-releases manuais. Rascunhos
não entram na contagem. As demais releases e seus anexos são excluídos,
**mas todas as tags permanecem**, permitindo consultar ou recompilar versões
anteriores. Releases antigas já existentes entram nessa contagem; o JDBC não
é anexado retroativamente a elas. Publicações manuais só entram na limpeza
quando ocorrer a próxima publicação automática.

Se houver falha, use **Re-run failed jobs** na execução do workflow: o processo
retoma o rascunho ou atualiza os anexos da release existente sem alterar suas
notas nem sua data original de publicação. Falhas de build/upload impedem a
limpeza; uma limpeza parcialmente concluída pode ser retomada. Não é preciso
excluir ou recriar a tag. A fila, os logs e eventuais erros ficam em **Actions →
Release**.

Para compilar ou testar o JDBC localmente com uma versão específica:

```sh
make test-jdbc JDBC_VERSION=0.8.1
make build-jdbc JDBC_VERSION=0.8.1
```

Sem `JDBC_VERSION`, o Maven usa `0.1.0-SNAPSHOT`. O resultado continua em
`clients/jdbc/target/nishidb-jdbc.jar`. A distribuição é pela GitHub Release;
não há publicação em repositório Maven.

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

**Autenticação e TLS (§74-77) estão no ar no piloto** desde 23/08: TLS +
Basic auth com papéis READ/ADMIN num plano seguro, e o plano loopback
mantido só-leitura para painel e scripts. O que resta da **Fase C** é a
exposição à rede (firewall com origem restrita), pendente de terceiros;
[`docs/aceite-m5.md`](docs/aceite-m5.md) registra o desenho e a seção
"Segurança" abaixo mostra a config mínima.

**O índice secundário virou POR SEGMENTO — e o motivo foi medido em
produção.** Os dois motores globais (`hash` e `bitmap`) foram apagados; no
lugar, um índice por segmento em duas formas — mutável só no segmento
corrente, selado e **imutável** nos demais — com janela de indexação
declarável na tabela histórica (`history.index`). O defeito que motivou: o
rehash do índice global, correndo sob o lock do writer, congelava a ingestão
do piloto por **18 a 52 minutos** por episódio, recorrente
([`benchmark/baselines/piloto-pre-ciclo-20260830.md`](benchmark/baselines/piloto-pre-ciclo-20260830.md)).
No desenho novo não há tabela para dobrar: o modo de falha é inexprimível. Na
subida em produção, com 135 GiB migrados no lugar: RSS de 2.812 para
**432 MiB**, índice de 3.716 para **614 MB**, espera bloqueada do consumidor
**zerada**
([`benchmark/baselines/piloto-pos-ciclo-20260830.md`](benchmark/baselines/piloto-pos-ciclo-20260830.md)).
`index: hash` e `index: bitmap` foram substituídos por `index: true`, e a
subida **recusa** as grafias antigas — quem tem dataset anterior lê a nota de
upgrade logo abaixo antes de trocar o binário.

**Campos aninhados consultáveis.** Chaves de dentro de um campo `json` podem
ser declaradas como colunas de verdade (`extract`), com tipo e índice próprios
e sem tocar no documento original — é o que torna `WHERE persistent.probeIpNode
= ...` possível, já que coluna `json` não é comparável. Elas ficam fora do
`SELECT *` e são pedidas por nome, por `persistent.*` ou pelo caminho. O
namespace `_meta` responde por SQL o que o dataset tem. No caminho, o teto de
65.535 campos do dicionário deixou de **derrubar o writer para sempre**: ele
tem `maxCols`, política de estouro e contador. O **backfill** do índice fechou:
declarar um campo indexado sobre um dataset que já tem linhas deixou de ser
recusado — a consulta responde certo desde o primeiro instante —, os segmentos
antigos são agendados para reconstrução campo a campo e um laço em segundo plano
drena essa fila sozinho (débito 22). Acrescentar `extract` a um dataset com
linhas continua recusado, e ali a razão é outra: o valor extraído não existe nos
records já gravados.

**Dois clients do protocolo remoto, entregues.** A **Fase 7** (driver JDBC,
§70-73) saiu — ela esperava a autenticação, porque publicar um driver antes
dela multiplicaria os pontos de acesso não autenticado em vez de fechar um — e
ao lado dela entrou o **client Python** (DB-API 2.0). Os dois são só leitura,
sem dependência de runtime, e provados contra o binário real; ver
[Clients](#clients).

**Snapshot de leitura com isolamento, entregue.** Uma consulta enxerga o
dataset como ele estava quando ela abriu: o mesmo conjunto de linhas do começo
ao fim, cada uma na versão daquele instante, com o relógio da expiração
congelado. A ingestão, a compaction e a retenção continuam correndo por baixo —
o que elas não fazem é apagar o passado que uma consulta ainda alcança.

Custa 24 bytes por atualização enquanto há consulta aberta, e nada quando não
há: os segmentos são append-only, então a versão anterior de uma linha já está
em disco e o que se guarda é o endereço dela, não uma cópia. O teto é
`runtime.query.snapshotMaxBytes`; estourado, a consulta mais antiga é recusada
com `SNAPSHOT_TOO_OLD` e a ingestão nunca para. O `/v1/stats` e o `/metrics`
publicam o que está retido, há quanto tempo, e quantos segmentos a compaction
deixou de aposentar por causa disso.

Próximo, na ordem do roadmap: os subsistemas 4-5 da direção pós-incidente (jobs
assíncronos, tabela de resultado em disco) e o que sobrou do ciclo da consulta
longa — o scan paralelo do débito 95, onde todo full scan ainda usa um núcleo
só.

### Upgrade: dataset criado antes do índice por segmento

O formato migra sozinho na primeira abertura — manifest v1/v2 → v3, os índices
globais saem do disco — e a migração é **irreversível**: binário anterior
recusa o dataset com "version desconhecida", e voltar é restaurar backup. Dois
pré-requisitos de config, os dois aprendidos em ensaio contra a config real do
piloto; sem o primeiro a subida **não acontece**, sem o segundo ela custa uma
varredura completa:

1. **Converta `index: hash` e `index: bitmap` para `index: true` ANTES de
   subir.** O binário recusa as grafias antigas com exit 3, nomeando o campo e
   a grafia nova. Um YAML não convertido não sobe.
2. **Declare `history.index` ANTES de subir, se o dataset tem tabela
   histórica.** Sem o bloco a janela de indexação é **infinita** — a leitura
   honesta de "declarei campos indexados e nenhum limite" — e, num dataset
   migrado, é a reconstrução de fundo varrendo o histórico INTEIRO para
   reindexá-lo (135 GB, no caso do piloto). Com a janela declarada, a primeira
   passada só toca os segmentos que ela admite — foi o que fez a migração do
   piloto drenar em ~5 minutos, com a ingestão correndo ao lado:

   ```yaml
   history:
     index:
       window: 6h
       maxBytes: 2GiB
   ```

Acompanhe a migração por `nishidb_index_segmentos{estado="pendente"}` caindo a
zero e `nishidb_index_reconstrucoes_total` estabilizando; a mecânica das séries
está na seção da janela de indexação, abaixo.

## Instalação

```bash
go build -o nishidb ./cmd/nishidb
```

Binário único, sem dependência externa em runtime. Requer Go 1.25+ e Linux
(o storage usa `mmap` e `fallocate` diretamente).

### Rodar como serviço (systemd)

A unit está versionada em
[`packaging/systemd/nishidb.service`](packaging/systemd/nishidb.service), para
ser empacotada com a distribuição:

```bash
sudo cp packaging/systemd/nishidb.service /lib/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now nishidb
```

Ela pressupõe o layout de releases com symlink que
[`scripts/nishidb-update.sh`](scripts/nishidb-update.sh) opera
(`/app/nishidb/current -> releases/<versão>`), e escreve o log estruturado em
`/var/log/nishidb/service.log` — **não** no journal. `journalctl -u nishidb`
mostra apenas `Started`/`Stopped`; evento de alarme, de ingestão e de consulta
estão no arquivo:

```bash
tail -F /var/log/nishidb/service.log | grep '"component":"alarm"'
```

Os caminhos são os do piloto. Torná-los configuráveis pede `EnvironmentFile`, o
que muda o contrato de operação — não está feito.

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
  defaultMaxTimeout: 30s   # teto do timeoutMs do request; ausente = o defaultTimeout de cada dataset

runtime:
  memoryTarget: 512MiB
  query:
    maxConcurrent: 16      # consultas simultâneas
    maxLongRunning: 2      # quantas delas podem ser LONGAS (sub-pool)
    maxBufferPerQuery: 4MiB
```

`query.defaultMaxRows` e `query.defaultTimeout` são os **defaults globais** do
teto de linhas e do timeout de consulta: valem para o dataset que **omite**
`query.maxRows`/`query.timeout` no YAML dele, e perdem para o dataset que os
declara — o global é default, não teto. Omitir o bloco `query` inteiro vale o
padrão da spec (100000 e 30s). Valor não positivo é **recusado** no
carregamento, com a chave e o valor nomeados no erro.

### Consulta longa: `query.timeout` deixou de ser default E teto

Até esta fase `query.timeout` acumulava dois papéis incompatíveis. Era o prazo
de quem não pede nada **e** o teto que o `timeoutMs` do request não podia
ultrapassar — o request só sabia **encurtar**. A consequência é que dar prazo
maior a UMA consulta exigia subir o default de TODAS, inclusive o das que nunca
chamaram `setQueryTimeout`: elas passariam a poder segurar uma vaga do §80 por
minutos sem nunca terem pedido isso.

Agora são duas chaves, no dataset (`query.timeout` / `query.maxTimeout`) e no
global (`query.defaultTimeout` / `query.defaultMaxTimeout`):

| chave | o que é |
|---|---|
| `query.timeout` | o prazo que vale quando o request **não** manda `timeoutMs` |
| `query.maxTimeout` | o **teto** que o `timeoutMs` do request pode alcançar |

**`maxTimeout` ausente vale exatamente o `timeout` resolvido daquele dataset** —
não o default global, não 30s. É isso que faz todo `nishidb.yaml` escrito antes
da separação produzir comportamento idêntico: um dataset com `query.timeout: 5m`
continua com teto de 5m, e não passa a ser recusado por um teto global menor.
`maxTimeout` abaixo de `timeout` é **recusado na abertura**, nomeando as duas
chaves com os dois valores.

`/v1/metadata` publica `timeoutMs` = **o teto** e acrescenta `defaultTimeoutMs`
= o prazo padrão. O campo `timeoutMs` já significava "o número que o servidor faz
valer" para quem o consome — é contra ele que o driver JDBC apara o
`setQueryTimeout` da aplicação, justamente porque o servidor **recusa** acima
dele. Publicar o teto ali é o que faz o driver alongar sozinho, **sem uma linha
de Java mudar**.

### O sub-pool da consulta longa

Prazo longo por padrão torna obrigatória uma contenção que antes seria opcional:
com 16 vagas e consultas de dez minutos, bastariam 16 relatórios para **toda**
consulta interativa receber 503 por dez minutos. `runtime.query.maxLongRunning`
(default 2, dentro do `maxConcurrent`) é o sub-pool: uma consulta cujo prazo
efetivo passa do `query.timeout` do dataset toma **duas** vagas — a do pool geral
e a do pool longo. A recusa do sub-pool tem código próprio,
`TOO_MANY_LONG_QUERIES`, com `Retry-After` maior que o do pool geral: o pool
geral esvazia em milissegundos e este pode levar minutos, então o conselho ao
cliente é outro — baixar o `timeoutMs` para caber no prazo padrão e entrar pelo
pool geral.

O default do sub-pool é limitado pelo pool, e a chave explícita não é:
`maxConcurrent: 1` é configuração legítima e existente, e um default fixo de 2
recusaria, na primeira troca de binário, um arquivo que ninguém editou.

### Orçamento de memória de consulta

`runtime.memoryTarget` e `runtime.query.maxBufferPerQuery` eram **parseados e
nunca lidos** — metade do débito 15. A chave não era só inócua: quem escreve
`memoryTarget: 1GiB` dimensiona a **máquina** por ela, e não havia teto nenhum.
Agora os dois têm leitor, com os defaults do §80 (512 MiB e 4 MiB), e saíram da
lista de chaves que a subida nomeia como sem efeito — continuar avisando sobre
uma chave que passou a valer mentiria na direção oposta.

O orçamento tem dois níveis, e cada um recusa uma coisa diferente:

| nível | o que impede | quando recusa |
|---|---|---|
| `runtime.memoryTarget` | que N consultas **somadas** estourem a máquina | na **admissão**, com 503 + `Retry-After` |
| `runtime.query.maxBufferPerQuery` | que **uma** consulta patológica consuma o que era de todas | por **linha**, com `ROW_TOO_LARGE` |

A reserva é de **admissão**, e não conforme o buffer cresce, porque a alternativa
é cruel: com reserva incremental o orçamento do processo pode acabar na linha
quarenta mil, e o cliente perde o resultado por causa da memória de *outra*
consulta. Reservado na entrada, ou a consulta tem o orçamento dela inteiro antes
do primeiro byte, ou recebe um 503 honesto. O efeito colateral é que o caminho
quente do writer não chama o orçamento nenhuma vez — sem CAS por linha, sem
contenção entre as consultas simultâneas.

**Primeiro consumidor real: o `StreamWriter`** (débito 28). Ele nunca encolhia:
uma linha de 1 MiB esticava o buffer e o `buf[:0]` mantinha a capacidade pela
vida do writer, mesmo que as 100 mil linhas seguintes fossem pequenas — e com N
consultas simultâneas o pior caso era N × maiorLinha de RSS residente. O sintoma
não era um erro, era memória, num processo que ninguém ligava àquela consulta.
Hoje a capacidade não passa do teto, e a linha que não cabe vira `ROW_TOO_LARGE`
nomeando o próprio tamanho e a chave a ajustar.

Duas séries novas: `nishidb_query_buffer_bytes` (o medidor que responde "o
`memoryTarget` está apertado?" **antes** da primeira recusa) e
`nishidb_query_budget_rejected_total`.

### Ver a consulta longa em andamento

`nishidb status` e `/v1/stats` passam a listar as consultas **em voo** — dataset,
hash do SQL, há quanto tempo e se é longa —, da mais antiga para a mais nova.
`consultas: N (M ativas, K com erro)` é a mesma frase para três point lookups de
2 ms e para um full scan de oito minutos, e as duas situações pedem ações
opostas. O SQL **cru** não é publicado (§95: ele carrega literais com dado do
cliente); o hash responde a pergunta que o operador faz de fato, que é se as
consultas presas são a mesma ou várias diferentes.

Duas séries novas acompanham: `nishidb_query_longest_running_seconds` (medidor
lido no **scrape**, porque o número cresce sozinho enquanto nada acontece — que é
exatamente o estado que ele existe para denunciar) e
`nishidb_query_long_rejected_total`.

`version`, `query.resultBatchSize` e o bloco `server` **passaram a ter leitor
no M2**: `version` diferente de 1 é recusado no carregamento, `resultBatchSize`
não positivo também, e `server.enabled: false` faz o `nishidb server` recusar a
subida em vez de ignorar a chave. `server.tls`, `server.authentication` e
`server.loopback` **ganharam leitor na Fase 11** (§74-77) — ver "Segurança:
autenticação e TLS" abaixo. O que ainda não tem implementação
(`runtime.managedMemoryTarget`, `storage.durability`) continua aceito e ignorado
de propósito,
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
schemaName: telecom

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
      index: true
    Acct-Terminate-Cause:
      type: string
      alias: terminate_cause
    NAS-IP-Address:
      type: ipv4
      alias: nas_ip
      index: true
    User-Name:
      type: string
      alias: username
```

`schemaName` é o schema SQL em que **todas** as tabelas deste dataset
aparecem para o cliente — a tabela base, a histórica, as de contador e as de
meta. É **obrigatório e não tem default**: o dataset sem a chave **não sobe**.
Adivinhar o nome do dataset entregaria `radius_sessions.radius_sessions`, e
deixar em branco entregaria a schema nula que o cliente mostra hoje; as duas
foram consideradas e recusadas, porque a chave existe justamente para desfazer
a ambiguidade entre datasets de um mesmo servidor. A gramática é `[a-z0-9_]+`,
e o prefixo `_` é recusado — ele é reservado aos espaços de nome do produto
(`_meta`, `_counters`). Não confunda com o bloco `schema:` logo abaixo, que
declara os **campos**: um é o container das tabelas, o outro é o conteúdo de
uma delas.

O schema é **único no servidor**: dois datasets do mesmo `datasets.directory`
não podem declarar o mesmo `schemaName`, e o par repetido derruba a carga
nomeando o schema e os **dois** datasets. A razão é que o schema é o container
em que as tabelas de cada dataset **serão apresentadas no catálogo** que o
cliente SQL lê — repetido, o catálogo teria dois containers de mesmo nome com
tabelas diferentes dentro, e qual deles o cliente enxergaria dependeria da
ordem em que o diretório foi lido, que nunca foi contrato. E o schema é
**endereço**: `SELECT * FROM telecom.por_olt` abre a tabela do dataset dono do
schema `telecom`, seja qual for o dataset da conexão — repetido, a mesma
consulta responderia de um ou de outro. A recusa do `schemaName` repetido vale
para o servidor e para os comandos locais (`ingest`, `query`, `counters`,
`offsets`), que leem o mesmo diretório: ela é da **carga** da configuração, não
do SQL.

No SQL, um qualificador que não é schema declarado é **recusado**, nomeando os
que existem — isso é do motor de consulta, e só os comandos que executam SQL
(`query`, e o servidor) chegam a ele.

A tabela **histórica não entra na conta do `schemaName` único**: ela herda o
schema do dataset que a declara, porque é outra tabela do mesmo dataset. Ela
entra, sim, na conta dos **nomes de tabela dentro do schema** — um contador não
pode se chamar como ela (`events_history`, pelo `history.suffix`) nem como o
próprio dataset, e o YAML é recusado na subida se isso acontecer.

Um campo **não declarado** aqui continua sendo aceito (`unknownFields:
include`) e vira coluna `auto`, mas só é consultável pelo **nome original** —
o alias existe apenas para campo declarado.

Um dataset pode declarar **contadores mantidos na escrita** — quantas
entidades estão num certo estado, agrupadas por uma chave que você escolhe:

```yaml
counters:
  # Quantos serviços existem em cada OLT. Sem `where`: conta todo mundo.
  - name: servicos_por_olt
    groupBy: [olt]

  # Destes, quantos estão autenticados.
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    maxGroups: 65536          # opcional; teto de grupos vivos (default 65536, até 2^31-1)
    retainEmptyGroups: 7d     # opcional; `never` mantém o grupo vazio para sempre

  # O nome do grupo como o inventário o escreve, em vez de `olt|slot`.
  - name: autenticados_por_pon
    groupBy: [olt, slot]
    keyFormat: "{olt}.{slot:02}"   # opcional; BR_SPOAM_OLT.08
```

`groupBy` aceita colunas declaradas de tipo `string`, `int64` ou `boolean`
(de topo ou extraídas), nunca a key nem a sequence; `where` tem a gramática do
WHERE e não pode tocar campo com `merge: sum` nem campo de sistema;
`keyFormat` é o molde do `groupKey`, descrito com as regras em "O formato do
`groupKey`". Como se
lê, o que custa e como se audita está em "Contadores mantidos na escrita",
mais abaixo.

#### Chaves de dentro de um campo `json`

Um campo `json` guarda o documento inteiro como **um** valor. Isso preserva o
dado, mas nada lá dentro é consultável: coluna `json` não é comparável, e o
binder recusa `WHERE`. `extract` promove uma chave interna a coluna de verdade,
com tipo e índice próprios:

```yaml
    persistent:
      type: json
      extract:
        - path: probeIpNode
          as: probe_ip
          type: string
          index: true
        - path: alarme.id       # caminho de mais de um nível
          as: alarme_id
          type: string
        - path: frontAlarmId    # sem `as`, o nome é o próprio path
          type: string
```

Para **descobrir sozinho** as chaves em vez de listá-las, use a mesma palavra
que o bloco `schema` já usa um nível acima:

```yaml
    persistent:
      type: json
      unknownFields: include     # toda chave do documento vira coluna
```

Serve para o caso em que você não controla o que recebe — a origem pode passar
a produzir um campo novo do dia para a noite, e o valor da coluna não é ela ter
sido prevista, é ela existir quando alguém for investigar. A descoberta é de
**um nível**: objeto aninhado vira coluna `json`, inteiro, e quem precisa da
chave lá dentro declara o caminho. O nome da coluna é qualificado
(`persistent.probeIpNode`), e o tipo é fixado na primeira observação — valor
divergente depois é **removido da coluna**, com o documento preservando-o.

**A leitura correta de uma coluna derivada é "o último valor visto para esta
chave"**, e não "o valor no documento desta linha". No modo `merge`, campo
ausente do patch preserva o valor anterior, e uma chave que simplesmente
DESAPARECE do documento não é removida da coluna: corrigir isso exigiria emitir
uma remoção por coluna conhecida e ausente, o que num documento de 11 chaves
sobre ~1.650 colunas seriam ~1.639 entries por evento — inviável no caminho
quente. O que **é** corrigido são os casos em que o documento contradiz a
coluna: valor de tipo divergente, `null` explícito, e documento que chega com
forma errada (array, null, escalar) — nos três a coluna é removida, porque a
frequência deles é baixa e o custo, portanto, pagável.

Campo descoberto **não indexa** (é a mesma restrição do campo autodescoberto de
topo), então filtro sobre ele é varredura. Declarar continua sendo o caminho
para a chave que precisa ser rápida.

Regras que valem a pena saber antes de declarar:

- **Extrair não é mover.** O documento original continua inteiro na coluna
  `persistent`, e quem já o lia continua lendo.
- **A coluna derivada fica fora do `SELECT *`.** Ela é projetável pelo nome,
  por `persistent.*` ou pelo caminho (`persistent.probeIpNode`), e aparece no
  `/v1/metadata` — que é onde ela é descoberta. Ficar fora do curinga é o que
  impede um documento de 500 chaves multiplicar a largura de toda linha.
- **`extract` é decisão de CRIAÇÃO do dataset.** A extração roda na ingestão,
  então acrescentá-la a um dataset que já tem linhas é recusado na subida: as
  linhas antigas ficariam sem a coluna e o filtro sobre ela as omitiria em
  silêncio.
- **Índice:** `index: true` é a **única** grafia aceita — `index: hash` e
  `index: bitmap` foram substituídos por um índice único e a subida **recusa**
  as duas com exit 3. O conjunto indexável é a união do que os dois motores
  antigos aceitavam: `string`, `int64`, `boolean`, `ipv4` e `ipv6`.
- **O que ENTRA na coluna mesmo vindo noutra forma:** numa coluna `string`,
  número e `true`/`false` são gravados como o **texto-fonte** (`5` → `"5"`,
  `1.50` → `"1.50"`, `true` → `"true"`); numa coluna `int64` ou `double`, a
  string numérica é convertida. A regra é uma só — converte-se quando a
  conversão é **sem perda** —, e o que ela recupera é o valor que antes se
  perdia inteiro. **A ressalva importa:** a coerção recupera o VALOR e **não
  unifica a CONSULTA**. Uma coluna `string` que recebe `"07"` num evento e `7`
  noutro passa a guardar `"07"` e `"7"` — duas strings distintas para a mesma
  coisa. A coluna fica completa e continua **não filtrável por um único
  literal**. É honesto, porque na origem os dois valores eram mesmo diferentes,
  mas quem escreve o filtro precisa saber. A coluna `coercions` do
  `_meta.fields` (abaixo) é como esse caso é medido.
- **O que não vira coluna:** caminho ausente, valor que é lista, valor de tipo
  incompatível com o declarado — hoje isso quer dizer **object ou array numa
  coluna escalar**, e `5.7` numa coluna `int64`, que continua recusado porque
  truncar em `5` gravaria um valor que ninguém mandou —, `null` explícito, e
  documento que chega com forma errada. Em todos a coluna fica **nula** e o
  evento entra normalmente —
  uma chave interna nunca derruba o evento. Só duas coisas o derrubam, e
  nenhuma delas é dado: **falha de infra** ao registrar a coluna ou a gravar o
  tipo (disco cheio, EIO), que sai como `STORAGE_ERROR`; e o **teto de campos
  sob `onMaxCols: stop`**, que sai como `FIELDS_EXHAUSTED`. Bug e infra nunca
  entram no balde de dado ruim.
- **O contador de valor incompatível conta divergência de TIPO, e só ela.**
  `null` explícito remove a coluna sem contar — é a origem dizendo que não há
  valor, não uma perda por tipo. Documento com forma errada conta **uma vez**,
  no campo pai: o que divergiu foi um documento, e as colunas nem estavam nele.

#### Colunas congeladas: o primeiro valor visto, para sempre

Uma coluna com `frozenFrom` guarda o **primeiro** valor que a origem teve e
nunca mais muda. É como se registra o estado de nascimento de uma entidade que
o merge, por definição, sobrescreve — a severidade com que um alarme abriu, o
IP da primeira autenticação, o instante em que a entidade apareceu:

```yaml
schema:
  fields:
    id:
      type: string
    severity:
      type: int64
    # Congela o valor de outra coluna. NÃO declara `type`: ele é herdado da
    # origem, e declarar os dois abriria a porta para eles divergirem.
    originalSeverity:
      frozenFrom: severity
      index: true            # é coluna normal: indexável e consultável
    # Congela o RELÓGIO do writer — o mesmo instante do `_writtenAt` do evento
    # que criou a entidade.
    createdAt:
      frozenFrom: "@writeTime"
```

A semântica, em quatro fatos:

- **Primeiro valor VISTO, não primeiro evento.** Se o evento de criação não
  trouxer a origem, ou a trouxer como `null`, a coluna fica vazia e é
  preenchida pelo primeiro evento que trouxer um valor. É o que torna a chave
  utilizável com fonte que manda evento parcial.
- **O evento não pode escrevê-la.** Um evento que traga o nome da coluna
  congelada é recusado com `SCHEMA_VALIDATION`: quem decide qual é o valor
  "original" é o writer, não o produtor.
- **Não há preenchimento retroativo.** Declarar `frozenFrom` num dataset que já
  tem dados NÃO preenche o passado — as entidades existentes ganham a coluna na
  próxima escrita que trouxer a origem, e as que nunca mais receberem evento
  ficam sem ela. Para preencher o histórico é preciso reprocessar a fonte.
- **Não vale em `mode: replace`.** No replace o estado anterior é descartado
  inteiro, e uma coluna que sobrevivesse a isso contradiria o que replace
  significa. O YAML é recusado na subida.

Cadeia (`b` congela `a`, `c` congela `b`), autorreferência, congelar a própria
key e `format:` junto de `frozenFrom` são todos recusados no load, com a
mensagem nomeando o motivo.

#### Colunas que acumulam: `merge: sum`

Por padrão o merge **sobrescreve**: o valor do evento substitui o que estava.
Para o campo que a origem emite como *parcela* — um contador de correlação, uma
contagem de repetições —, isso perde o acumulado. `merge: sum` troca a política
desse campo:

```yaml
schema:
  fields:
    matchedCounter:
      type: int64
      merge: sum      # o valor do evento é SOMADO ao que já está no estado
```

O que precisa estar claro antes de ligar:

- **Não é idempotente, e isso é o custo real.** Reprocessar o mesmo evento soma
  de novo. Um crash entre o commit do offset e a queda faz a janela reentregue
  somar duas vezes, e o total fica **acima** do real. O campo passa a ser
  aproximado sob crash. Onde a exatidão importa mais que o acumulado, a
  política certa continua sendo a sobrescrita, com a origem mandando o total.
- **Só `int64`.** Soma de ponto flutuante acumula erro a cada parcela, e um
  total que deriva do próprio histórico não pode depender da ordem de chegada.
  Campo `auto` também é recusado: sem tipo declarado, a soma valeria ou não
  conforme o primeiro evento.
- **O teto satura, não dá a volta.** Ao bater em `MaxInt64` o valor para ali e
  `nishidb_merge_sum_saturated_total` sobe — o mesmo número aparece no
  `/v1/stats`, em `materialization.summedSaturated`. Recusar o evento travaria
  a entidade para sempre, porque todo evento seguinte estouraria igual.
- **`null` continua apagando**, e a coluna recomeça do zero no próximo evento.
  Entidade que expira e volta também recomeça: a encarnação nova não herda o
  total da anterior.
- **Chave repetida no mesmo evento continua last-wins.** A soma é sobre o
  estado anterior, não sobre as ocorrências de um evento malformado.

Não vale em `mode: replace`, na key, na sequence, nem junto de `frozenFrom` —
esta e a congelada são políticas de escrita opostas do mesmo campo.

#### Expiração: regras de TTL condicionais

O bloco `ttl:` é uma **lista de regras**. Cada evento é avaliado contra elas na
ordem do arquivo, e a **primeira que casa** decide por quanto tempo a entidade
pode ficar sem receber escrita:

```yaml
materialization:
  ttl:
    # `when` é uma condição com a MESMA gramática do WHERE, avaliada sobre o
    # estado que ESTA escrita vai produzir (o valor pós-merge).
    - when: "severity = 2"
      expireAfterWrite: 3m
    - when: "kind = 'audit'"
      expireAfterWrite: never     # casou e decidiu que não expira
    # Regra sem `when` é a de FUNDO: casa com tudo, e só pode ser a última.
    - expireAfterWrite: 7d
```

O que precisa estar claro antes de escrever a lista:

- **A ordem é semântica.** Primeira que casa vence, então reordenar as regras
  muda o comportamento. Uma regra de fundo antes de uma condicional tornaria a
  condicional inalcançável, e por isso é recusada no load.
- **A condição vê o estado PÓS-MERGE.** O valor de cada campo é o que o evento
  trouxe; se ele não mencionar o campo, vale o que já estava no estado; `null`
  explícito apaga. É por isso que um evento parcial não muda o prazo por
  acidente.
- **O prazo é reavaliado a cada escrita, e isso é o cancelamento.** Um alarme
  que fecha ganha 3 minutos; se reabrir em 90 segundos, a escrita nova o
  devolve para os 7 dias. O prazo escolhido é gravado no documento como o campo
  de sistema `_expiresIn` (uma duração em millis, fora do `SELECT *`).
- **`never` não é `0`.** `never` declara imortalidade e vence a regra de fundo;
  `0` é recusado, porque parece esquecimento.
- **A duração mínima é 1 minuto** — a resolução do coletor. Abaixo dela o
  prazo real seria maior que o declarado, e uma chave que mente é pior que uma
  chave recusada.
- **Adotar as regras num dataset já populado não exige reprocessar.** As
  entidades materializadas antes não têm `_expiresIn`, e caem no prazo da regra
  de fundo até a primeira escrita seguinte. Sem regra de fundo, elas não
  expiram.
- **Renovar empurra o prazo indefinidamente.** Uma entidade que casa com uma
  regra de 3 minutos mas recebe evento a cada 60 segundos nunca expira. É o
  comportamento escolhido (não há teto de idade absoluta), e ele está fixado em
  teste.

A forma antiga — `ttl:` como bloco único com `expireAfterWrite`, ou com
`field`/`type` — é **recusada na subida**, com a mensagem apontando a lista.

#### Teto de campos

O dicionário de campos de um dataset é append-only e tem um teto físico de
65.535. Sobre uma grandeza que cresce com os eventos, alcançá-lo é questão de
tempo, e o que acontece ali é configurável:

```yaml
schema:
  maxCols: 10000
  onMaxCols: ignore   # ignore (default) | stop
```

Com `ignore`, a chave excedente não vira coluna, o evento é gravado com todas
as outras, e a perda é contável — `nishidb_dataset_fields_dropped_total` e o
bloco `schema` do `/v1/stats`. Com `stop`, a ingestão para de forma observável.
O teto efetivo é publicado no `/v1/metadata`.

O `index:` dos campos **deixou de ser definitivo**. Acrescentar um índice a um
dataset povoado e removê-lo de um campo já indexado passaram a ser aceitos: as
duas recusas com exit 3 saíram, porque o dano que elas evitavam — um índice que
cobrisse só parte do dataset e devolvesse MENOS linhas do que existem — não é
mais construível. O índice mora dentro de cada segmento, e segmento sem a seção
do campo devolve as linhas dele como candidatas para o filtro revalidar.

**Os segmentos antigos entram na fila de reconstrução:** a pendência é decidida
por (segmento, campo), então um índice gravado antes da declaração — que tem
arquivo e não tem a seção do campo novo — é agendado para ser refeito. Remover um
campo entra pela mesma porta, no sentido inverso: a seção que sobrou sai na
reescrita.

**A ressalva é de custo, e vale conhecer antes de declarar:** a fila é drenada
por um laço em segundo plano, um segmento por vez, com cochilo entre as
passadas. No **histórico** o custo melhora segmento a segmento, conforme a
reconstrução alcança cada um. No **materializado** ele é um DEGRAU: enquanto
existir UM segmento sem índice, a consulta pelo campo colapsa numa caminhada
única do `locations.idx` — o custo de um full scan de candidatos, pago por
inteiro até o último segmento pendente ganhar índice, e zerado dali em diante.
A resposta é a mesma desde o primeiro instante; o preço não.

**`index: hash` e `index: bitmap` foram substituídos por `index: true`.** A
distinção de cardinalidade que as duas grafias carregavam desapareceu com os
dois motores globais que a justificavam — hoje é uma lista de postings por
segmento, e densidade é decisão de codificação, não de configuração. Aceitá-las
como sinônimo de `index: true` deixaria o operador escrevendo uma distinção que
não existe mais, então a subida **recusa as duas com exit 3**, com a mensagem
ensinando a grafia nova.

#### Guardar o evento cru: a tabela histórica

O dataset materializa **estado corrente**, e o que a origem mandou não
sobrevive ao merge. O bloco `history` acrescenta uma **segunda tabela**, com as
mesmas colunas tipadas e uma linha por evento — sem deduplicação, sem merge e
sem update:

```yaml
name: events
history:
  suffix: _history          # default; a tabela é events_history
  expireAfterWrite: 7d      # prazo de uma linha, contado da escrita
  maxBytes: 40GiB           # teto dos segmentos de dados
  segmentSize: 128MiB       # opcional; granularidade da purga
  rawPayload: false         # opcional; acrescenta a coluna _raw
  fields:                   # opcional; só overrides de índice
    identifier: { index: true }
  index:                    # opcional; quanto do histórico vale INDEXAR
    window: 6h              #   intenção, em tempo
    maxBytes: 2GiB          #   teto do ÍNDICE; vence quando os dois discordam
```

Da **mesma conexão**, as duas tabelas respondem:

```sql
SELECT severity, lastSeen FROM events         WHERE node = 'sw-01'
SELECT severity, lastSeen FROM events_history WHERE node = 'sw-01' ORDER BY _writtenAt
```

As colunas são as mesmas, com os mesmos tipos — inclusive as derivadas de
`extract`. O que a irmã **não** tem: as colunas congeladas (são produto do
merge, e não há merge ali) e a key (sem primary não há dedup, que é a definição
da tabela). Uma coluna `merge: sum` guarda a **parcela** do evento, não o
acumulado.

Declarar o bloco **sem** `expireAfterWrite` nem `maxBytes` é recusado: um
histórico sem retenção é disco que cresce para sempre. Os dois convivem, e vence
o primeiro a bater — a série `nishidb_history_oldest_written_at` é a que mostra
o teto de tamanho engolindo o prazo declarado.

##### A janela de indexação: `history.index`

O bloco `index` declara **quanto do histórico vale indexar**. Fora da janela a
consulta **continua respondendo** — por enumeração, que é correta e mais lenta.
A janela decide custo, nunca resultado.

A simetria com a retenção é deliberada: `history.maxBytes` limita o **dado**,
`history.index.maxBytes` limita o **índice**, e nos dois casos o teto de bytes é
a política forte que engole o prazo declarado. Quem entendeu uma entende a
outra.

| Caso | Resposta |
|---|---|
| bloco `index:` ausente | **janela infinita** — indexa tudo que a retenção mantém. É o default |
| `index:` fora do bloco `history:` | recusado, **exit 3** — a janela é da tabela histórica |
| `window: 0` | recusado, **exit 3** — ambíguo entre "nada" e "tudo"; desligar é omitir o bloco |
| `window` maior que `expireAfterWrite` | aceito, com **WARN** `window.maior_que_retencao`: é inócuo, a retenção limita antes |
| `index.maxBytes` sem `window` | aceito — teto puro é declaração válida |

A fronteira por tempo usa o record **mais novo** de cada segmento: um segmento
que atravessa a fronteira mantém o índice, porque a janela promete consultar
rápido as últimas N horas. Estreitar a janela expulsa os índices que saíram na
próxima passada de retenção (≤ 1 min); **alargar volta a indexar**, de forma
progressiva.

**A série de disco e o teto NÃO medem a mesma coisa**, e vale saber antes de
dimensionar: `nishidb_history_index_bytes` soma *todo* o índice do dataset — os
`.sidx` selados, os temporários, os `.widx` do segmento corrente, mais `indexes/`
e `checkpoint/` —, enquanto `history.index.maxBytes` governa **só os `.sidx`
selados**, que é a única parcela que a expulsão sabe devolver. Ver a série
permanentemente acima do teto é o esperado, não defeito: é o mesmo princípio que
mantém o `locations.idx` fora do `history.maxBytes` — *um teto que a purga não
consegue perseguir seria um teto que mente*.

`nishidb_history_index_window_seconds` publica o intervalo **realmente** coberto
pelos segmentos indexados. Compare com o `window` declarado: menor significa que
o teto de bytes assumiu o comando. Ela é a irmã da
`nishidb_history_oldest_written_at`, do lado do índice, e é o único lugar em que
a cobrança conservadora do teto aparece.

Essa cobrança existe porque o teto precisa dar um preço a um segmento que **ainda
não tem índice** — e cobrar zero dele o faria caber sempre, ser reconstruído e
ser expulso na passada seguinte, para sempre. O preço é
`max(maior .sidx em disco, o que aquele segmento custou quando foi expulso)`, e
as **duas** parcelas são necessárias: só a primeira não serve, porque a própria
expulsão apaga arquivos e portanto baixa o maior — o segmento recém-expulso seria
cobrado por menos do que custou e voltaria a caber. A segunda parcela é
lembrada em memória e some num restart, que então paga uma reconstrução
desperdiçada por segmento atípico antes de estabilizar.

**O que acompanhar enquanto a janela trabalha** são as seis séries do índice por
segmento, que saem em **todo** dataset — o motor de índice é um só nos dois
modos, então elas não pertencem ao bloco `history`:

| Série | Responde |
|---|---|
| `nishidb_index_segmentos{estado}` | quantos segmentos em cada estado de índice |
| `nishidb_index_bytes` | o disco do índice por segmento |
| `nishidb_index_reconstrucoes_total` | a fila está drenando? |
| `nishidb_index_reconstrucao_ativa` | o laço está de pé? |
| `nishidb_index_crc_falhas_total` | quantos `.sidx` já foram reprovados no crc32c desde a subida |
| `nishidb_index_segmentos_consultados_total{modo}` | quantos responderam por índice, quantos por enumeração |

`estado` tem quatro valores, e a distinção entre dois deles é a que importa:
`pendente` é o segmento **dentro** da janela ainda na fila — trabalho por fazer —
e `fora` é o segmento que a janela deliberadamente não cobre, que num dataset
grande é a maioria esmagadora e é o sistema funcionando como projetado. Os
outros dois são `selado` (o `.sidx` cobre o conjunto indexado) e `corrente` (o
índice é o WIP). Somar `pendente` com `fora` daria um número enorme, normal e
constante, que não responde *"o laço está avançando?"*.

`nishidb_index_segmentos_consultados_total{modo}` é o único jeito de ver a
degradação: o resultado da consulta é **idêntico** pelos dois caminhos — é essa
a tese do desenho —, e só o contador distingue um dataset indexado de um dataset
que está enumerando.

`nishidb_index_crc_falhas_total` é **degrau**, e não taxa: o mapeamento reprovado
fica em cache e cada arquivo soma uma vez por processo, então um alerta por
`rate()` sobre ele se auto-resolve em minutos enquanto o segmento continua
enumerando. E **nenhuma série diz se ainda há índice podre**: o gauge de estados é
cego a ela — o arquivo reprovado continua contado como `selado` —, e a enumeração
sobe sem separar a causa de um segmento legitimamente fora da janela. O sinal
disponível é o WARN `indice.selado_corrompido` no log; a lacuna é o débito 103.

**`nishidb_index_bytes` e `nishidb_history_index_bytes` não se comparam no mesmo
rótulo**, e a diferença morde quem escrever a subtração óbvia. A segunda é do
bloco `history`: ela descreve a tabela **irmã** e sai rotulada pelo dataset
**base**, onde o operador declarou a política. A primeira descreve o Store em que
sai. Num par `eventos` / `eventos_history`:

```promql
nishidb_index_bytes{dataset="eventos_history"}          # a parcela por segmento
nishidb_history_index_bytes{dataset="eventos"}          # ela mais indexes/ e checkpoint/
nishidb_history_index_bytes - nishidb_index_bytes       # ERRADO: casa por rótulo igual
```

A última linha subtrai a série **errada**: em `{dataset="eventos"}` a série de
índice por segmento descreve o índice do **próprio base** — zero quando o base
não indexa nada, e um número sem relação nenhuma com o total quando indexa. É
também no rótulo da irmã que se acompanha a promessa do ciclo: é
`eventos_history` que precisa parar de crescer.

`rawPayload: true` acrescenta a coluna `_raw` com os bytes originais do evento,
e é ela que permite gravar também o evento que **não parseia** — hoje um JSON
inválido é apenas um número num contador. Ela dobra o disco da tabela em eventos
grandes, e por isso o default é `false`.

Dois limites que valem saber antes de ligar: **não há backfill** (a tabela
começa vazia, e o dataset base não guarda os eventos que a preencheriam — mas
ligá-la **não** exige recriar o dataset), e o `_writtenAt` é o instante da
**ingestão**, não o do evento. A mecânica inteira está em
[`docs/architecture.md`](docs/architecture.md), seção "A tabela histórica".

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
      decodeWorkers: 1             # decodificadores em paralelo; 1 é o default
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
      usernameEnv: KAFKA_USERNAME  # o NOME da variável, nunca o valor
      passwordEnv: KAFKA_PASSWORD

errors:
  invalidEvent: log-and-skip       # ou `stop`, que PARA a ingestão
```

**`decodeWorkers` paraleliza o DECODE, nunca a escrita.** O parse do JSON, a
resolução contra o schema e a conversão de tipos passam a rodar em N goroutines;
o estado anterior, o merge e o `Put` continuam numa só, e uma esteira devolve os
decodes ao writer na ordem de chegada — a mesma ordem que a execução serial
produziria. O resultado é idêntico: há um golden que compara `SELECT *` e
`_meta.fields` byte a byte entre 1 e 8 decodificadores.

O default é `1`, e subir é decisão de MEDIÇÃO. O ganho satura cedo, porque a
metade que escreve continua serial: em laboratório, 2 decodificadores dão 1,75×,
4 não acrescentam e **8 regridem para 1,53×** por contenção
(`benchmark/baselines/decode-workers-20260828b.txt`). Cada worker a mais também é
memória: os slots da esteira retêm os buffers do decode, publicados em
`nishidb_ingest_decode_arena_bytes`. Suba para 2, meça, e só então considere mais.

```bash
nishidb server --config nishidb.yaml
```

O servidor abre os datasets no **mesmo processo** em que consome, porque o
flock do dataset é exclusivo por processo. Expõe o protocolo remoto do §67 —
`POST /v1/query`, `GET /v1/metadata`, `GET /v1/stats` e `GET /v1/health` —
mais `GET /metrics` (§92/§93), e aceita `--pprof <addr>` para os endpoints de
debug num listener próprio. O inventário das séries — as 91, por subsistema,
com tipo, labels e o que cada uma responde — está em
[`docs/metricas.md`](docs/metricas.md).

O bind default é **`127.0.0.1`**. Fora do loopback, o servidor agora EXIGE
uma decisão sobre autenticação — ver a próxima seção — em vez de só avisar.

```bash
# consultar COM a ingestão de pé
curl -sS -H 'Content-Type: application/json' \
  -d '{"schema":"telecom",
       "sql":"SELECT id, ip FROM radius_sessions WHERE status = ? LIMIT 100",
       "parameters":[{"type":"string","value":"Stop"}]}' \
  127.0.0.1:9142/v1/query

# o mesmo pelo CLI, com o parâmetro (as flags vêm ANTES da query); o remoto
# endereça o SCHEMA da conexão, como o driver JDBC — o local (--config) segue
# abrindo o dataset por --dataset
nishidb query --server 127.0.0.1:9142 --schema telecom \
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

#### Quando o consumidor para de receber

Um consumidor pode parar de entregar registros sem erro nenhum: a conexão
segue de pé, o processo segue saudável, o `/v1/health` responde UP — e nada
chega. Aconteceu no piloto em 27/08/2026, e a ingestão ficou parada por uma
hora até alguém olhar.

O watchdog recria o consumidor quando isso acontece:

```yaml
source:
  kafka:
    consumer:
      watchdogTimeout: 60s      # default; `never` desliga; piso de 10s
      watchdogTentativas: 3     # default
```

**O gatilho é uma conjunção de três fatos, e são três porque dois não bastam:**

| Fato | Por que sozinho não serve |
|---|---|
| os eventos não avançaram na janela | um tópico legitimamente parado produz o mesmo sintoma |
| o broker declara mensagens pendentes | diz que há o que consumir, não diz de quem é a culpa |
| a fila poller→writer está **vazia** | é este que separa as duas causas |

O terceiro é a guarda contra **backpressure da persistência**. Fila vazia
significa que o writer não tem o que fazer, logo quem não entrega é o poller.
Fila com itens significa o contrário — disco lento, checkpoint longo, storage
em modo falho —, e aí recriar a conexão seria errado e destrutivo: abortaria
trabalho em voo por um problema que não é de conexão, e a fila voltaria a
encher do mesmo jeito. Nesse caso o watchdog **não age** e registra
`ingestao.sem_progresso`.

O lag é perguntado ao broker pelo cliente **admin**, que é uma conexão
distinta da do consumo — foi por isso que ele continuou respondendo durante
todo o travamento, e é o que torna este watchdog construível. Se um dia os
dois travarem juntos, o watchdog se abstém: agir sobre o que não se sabe seria
recriar o consumidor por causa de uma consulta que falhou.

**A recriação passa pelo mesmo `kafka.Open` da subida.** O §49 põe o
checkpoint local como autoritativo e lista quatro situações que recusam subir
a ingestão; um caminho de reconexão que redescobrisse a posição por conta
própria seria um segundo boot, menos testado, que divergiria do primeiro no
dia em que alguém mexesse num dos dois.

A contagem é de tentativas **consecutivas** e zera quando o consumidor volta a
andar — sem isso, meses de uptime acumulariam três blips não relacionados e a
ingestão desistiria para sempre. Esgotadas as tentativas, o dataset cai no
estado que a ingestão falha já tem hoje: **consultas de pé**, health
reportando, e `ingestao.desistiu` no log. Duas séries acompanham,
`nishidb_kafka_watchdog_trips_total` e `nishidb_kafka_watchdog_desistiu` —
separadas porque recriar é recuperação e desistir é alguém acordado.

### Segurança: autenticação e TLS (§74-77)

Exemplo mínimo com os três blocos — TLS, autenticação e o listener loopback
opcional. Os usuários não aparecem aqui: moram na tabela de usuários do
servidor (abaixo).

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
trancada. Uma rota **ADMIN** (`/v1/compact`, `/v1/checkpoint`, a gestão de
usuários) responde 403 também no loopback: esse listener é READ-only por
construção.

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

#### Usuários

Os usuários moram na **tabela de usuários do servidor**, em
`<storage.path>/_system/users.state` — a única fonte de quem autentica. Na
primeira subida com `authentication.enabled: true` o servidor cria o ADMIN
`admin`, grava a senha em `<storage.path>/_system/bootstrap-admin.password`
(modo 0600) e registra só o caminho no log (evento `users.bootstrap`). Trocar
a senha desse usuário, ou removê-lo, apaga o arquivo. Uma tabela ilegível
recusa a subida: o servidor nunca regera um ADMIN por cima dela.

```bash
# primeira subida: a senha gerada está no arquivo que o log apontou
cat /app/nishidb/dados/_system/bootstrap-admin.password

# criar um usuário com o servidor no ar — a senha nova vem de arquivo ou do
# stdin (-), nunca de flag
NISHIDB_PASSWORD="$(cat /app/nishidb/dados/_system/bootstrap-admin.password)" \
  nishidb users create --server 127.0.0.1:9142 --tls-ca /etc/nishidb/tls/ca.pem \
  --user admin --name reader --role READ --new-password-file /etc/nishidb/secrets/reader

# trocar a senha do admin de bootstrap (o arquivo gerado some)
nishidb users password --server 127.0.0.1:9142 --tls-ca /etc/nishidb/tls/ca.pem \
  --user admin --name admin --new-password-file -

# listar — só ADMIN, nunca com o hash
nishidb query --server 127.0.0.1:9142 --tls-ca /etc/nishidb/tls/ca.pem --user admin \
  --schema <qualquer> 'SELECT username, role, bootstrap, updatedAt, updatedBy FROM _meta.users'

# recuperar a senha de um ADMIN perdida: com o servidor PARADO
nishidb users password --config nishidb.yaml --name admin --new-password-file -
```

Um usuário READ troca a **própria** senha pela mesma rota; ADMIN troca a de
qualquer um, cria, muda papel e remove. O último ADMIN não é removido nem
rebaixado. A troca vale a partir do request seguinte — não há sessão, o Basic
é verificado a cada request. A senha é guardada como hash argon2id com sal,
nunca em texto claro, e nenhuma resposta, log ou `/v1/stats` a repete; cada
mutação sai no log estruturado com quem fez (`actor`), sobre quem (`user`) e
de onde (`remote`). Nome de usuário é `[a-z0-9_.-]`, até 64 bytes; senha, de
12 a 1024 bytes. Com o servidor parado, `nishidb users … --config` faz o mesmo
diretamente na tabela e cria a credencial de bootstrap se ela ainda não
existir — é o caminho de provisionar antes da primeira subida.

#### Migração do `server.authentication.users` (quebra de contrato)

Até a v0.7.0 os usuários vinham do YAML. A partir desta versão o bloco
`server.authentication.users` é **recusado na subida**, com o roteiro na
própria mensagem — nunca ignorado em silêncio. Com o servidor parado:

```bash
# 1. recriar cada usuário do YAML na tabela, reaproveitando a senha
nishidb users create --config nishidb.yaml --name reader --role READ \
  --new-password-file /etc/nishidb/secrets/reader            # quem usava passwordFile
printf '%s' "$NISHIDB_ADMIN_PASSWORD" | nishidb users create --config nishidb.yaml \
  --name ops --role ADMIN --new-password-file -              # quem usava passwordEnv

# 2. a primeira gestão criou também o "admin" de bootstrap: troque a senha ou remova
nishidb users delete --config nishidb.yaml --name admin

# 3. apagar o bloco users do nishidb.yaml e subir o servidor
```

Senha com menos de 12 bytes e nome fora de `[a-z0-9_.-]` são recusados na
criação — a migração é o momento de trocá-los.

**Rollback.** O `nishidb-update.sh` troca o par binário + YAML da release, e
um binário anterior a esta versão autentica pelo bloco `users` do YAML dele,
ignorando `_system/`. Usuários criados ou senhas trocadas depois da
atualização não existem para ele; um YAML novo com o binário antigo recusa a
subida — falha fechada, nunca subida sem senha (débito 122).

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

### Contadores mantidos na escrita

Um contador declarado no YAML (bloco `counters:`, ver "Definição do dataset")
é um **índice derivado**: mantido enquanto se escreve, lido em tempo
constante, sem varrer o dataset. Ele conta **estados, não mensagens** — cada
evento é uma transição do que a entidade era para o que ela passou a ser, e o
contador ajusta as duas pontas na mesma operação. Reprocessar o mesmo evento
é uma transição de X para X: ajuste zero. É o que torna o número correto sob
a entrega "pelo menos uma vez" do Kafka — que o `merge: sum` não é, por
contrato.

A leitura é SQL, numa tabela por contador no namespace `_counters`:

```bash
nishidb query --server 127.0.0.1:9142 --schema <schemaName de servicos> "SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC LIMIT 5"
```

```bash
nishidb query --server 127.0.0.1:9142 --schema <schemaName de servicos> "SELECT groupKey, total FROM _counters.autenticados_por_olt WHERE groupKey = 'PRCTA_G1M10'"
```

```bash
nishidb query --server 127.0.0.1:9142 --schema <schemaName de servicos> "SELECT olt, slot, port, total FROM _counters.autenticados_por_pon WHERE olt = 'PRCTA_G1M10' ORDER BY total ASC LIMIT 3"
```

Cada tabela tem `groupKey` (o texto do grupo — a própria OLT num `groupBy` de
uma coluna, `olt|slot|port` num composto), as colunas do `groupBy` **abertas e
tipadas**, e `total`. `groupKey`
é a key da tabela: `WHERE groupKey = '…'` é busca direta; as outras colunas
varrem os grupos (milhares, não milhões de linhas). **Um grupo que chega a
zero permanece na tabela com `total: 0`** — o equipamento que apagou por
inteiro é justamente o caso que interessa — e só sai quando nenhuma linha
aponta mais para ele há mais que `retainEmptyGroups` (nunca por `total == 0`:
um grupo com 500 linhas não autenticadas tem total zero e 500 linhas). Linha
sem alguma coluna do `groupBy` (ausente ou nula) não é contada. Os totais são
lidos no **presente**: eles não entram no snapshot de leitura da consulta — o
que a consulta congela é o conjunto de grupos.

#### O formato do `groupKey`

O separador default é `|`, mas o texto pode ser declarado por contador — é o
que faz a aplicação consultar o grupo **pelo nome que o inventário escreve**:

```yaml
counters:
  - name: autenticados_por_pon
    groupBy: [olt, slot]
    keyFormat: "{olt}.{slot:02}"    # BR_SPOAM_OLT.08
```

As aspas são obrigatórias: `{` abre um mapa no YAML. O molde é feito de
literais e de marcadores `{coluna}`, com `{coluna:0N}` para número com zero à
esquerda (só em `int64`; valor mais largo que `N` sai inteiro, sem truncar).
`{{` e `}}` são as chaves literais. Ele pode reordenar as colunas em relação
ao `groupBy` e ter prefixo e sufixo: `"OLT-{olt}.{slot:02}"`.

Dois grupos distintos nunca podem render o mesmo texto — `groupKey` é a key, e
a busca por ela tem de identificar UM grupo. Por isso a subida recusa o molde
que não usa toda coluna do `groupBy` exatamente uma vez, o que cola dois
marcadores sem literal entre eles, o que põe dígito ou `-` logo depois de um
marcador `int64`, e o que tem `\` ou caractere de controle num literal. Dentro
de um valor, o texto escapa com `\` a barra invertida e o primeiro caractere
do literal que vem logo depois daquele marcador — e só ele: sob
`"OLT-{olt}.{slot:02}"` a OLT `BR_SPOAM_OLT` sai inteira, e só um `.` no meio
do nome seria escapado.

`_meta.counters` tem a coluna `keyFormat` com o molde efetivo — quem não
declarou vê o default já expandido (`{olt}|{slot}`), e nunca precisa adivinhar
por qual texto consultar.

Mudar `groupBy` ou `where` de um contador, ou declarar um contador novo num
dataset que já tem linhas, **reconstrói o contador na abertura** varrendo as
linhas vivas (o log `counter.construido` diz o motivo e quanto custou — a
ordem de grandeza de uma varredura completa). Mudar `maxGroups` não
reconstrói; um teto abaixo dos grupos em uso é recusado na subida. **Mudar
`keyFormat` também não reconstrói**: o que está em disco é a chave do grupo, e
o texto é derivado dela em toda abertura — mas o `groupKey` muda, e com ele o
valor que as consultas salvas procuram. O log de subida sai com o molde
efetivo, para a troca não ser silenciosa. Um grupo
novo acima do teto é recusado em runtime: a linha entra, **não é contada**, e
`nishidb_counter_groups_refused_total` sobe — é o alarme.

```bash
nishidb counters --server 127.0.0.1:9142 --dataset servicos
```

Quais contadores um dataset tem, e como estão, também sai por SQL — a tabela
`_meta.counters` existe em todo dataset, vazia quando não há `counters:`:

```bash
nishidb query --server 127.0.0.1:9142 --schema <schemaName de servicos> "SELECT name, rows, total, groups, maxGroups, refused, auditDivergent, auditAt FROM _meta.counters"
```

`rows` é quantas linhas vivas apontam para algum grupo do contador e `total`
quantas satisfazem a condição, no dataset inteiro — o denominador e o
numerador do alarme, em tempo constante, sem somar os grupos no cliente e sem
varrer. `rows` **menor** que o `rows` do dataset (`/v1/stats`) é diagnóstico
direto: são as linhas sem alguma coluna do `groupBy` (o serviço sem `olt`),
que o contador não conta por desenho e que antes ninguém enxergava sem uma
varredura. Sem `where`, `total` é igual a `rows`.

`groupBy` é a lista das colunas de agrupamento na ordem declarada, junta por
`|`, e `keyFormat` é o molde efetivo da key (os dois são independentes: o
molde escolhe o separador dele e pode reordenar); `condition`
é o `where` do YAML (NULL no denominador, que não tem condição); `retired`,
`refused` e `orphans` são os mesmos números do `nishidb counters`;
`auditDivergent` e `auditAt` são NULL até a primeira auditoria conclusiva
desde a subida — zero divergências e "nunca auditado" são afirmações
diferentes. `rows` e `total`, ao contrário desses dois, nunca são NULL: zero
linhas é um fato medido.

```bash
nishidb counters --server 127.0.0.1:9142 --dataset servicos --audit
```

```bash
nishidb counters --config nishidb.yaml --dataset servicos --rebuild
```

Sem ação, `counters` lista cada contador: grupos vivos, teto, aposentados,
recusados pelo teto, órfãs e a última auditoria. `--audit` recalcula por
varredura e compara com o mantido — exit 3 se algum grupo divergir, é
corrupção de índice derivado. `--rebuild` reconstrói: **segura o writer pela
varredura inteira**, a ingestão para até acabar. `--server` usa as rotas ADMIN
`POST /v1/counters/audit` e `POST /v1/counters/rebuild` no servidor no ar;
`--config` faz o mesmo com o servidor **parado** (toma o flock, como
`offsets`).

A auditoria com `--server` corre sob um snapshot de leitura **sem parar a
ingestão**: congela os totais e abre o snapshot no mesmo instante, depois
varre. "Viva" para o contador é linha com location e sem lápide — a linha cujo
prazo venceu e que o coletor ainda não lapidou continua contada até o coletor
chegar, e a auditoria a conta também. Se o journal do snapshot estourar o teto
no meio, a auditoria é **inconclusiva** (exit 5), nunca um relatório
inventado.

Pré-requisitos do caso de uso que motivou o desenho, fora deste repositório: o
produtor precisa publicar **com chave** (senão eventos da mesma entidade caem
em partições diferentes e chegam fora de ordem) e com um **número de sequência
inteiro no primeiro nível** do documento, para que o stale check descarte o
evento atrasado. Sem isso o contador está certo sobre um estado errado.

### Métricas de janela do contador

O `total` de um grupo é o valor de agora. Ele não diz se uma OLT com zero
autenticados **nunca** teve ninguém ou se tinha gente e esvaziou. As métricas
de janela respondem a isso com média, mínimo e máximo do `total` de cada grupo
numa janela de tempo:

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    metrics:
      - name: auth_5m
        type: window
        source: total
        window: 5m
        aggregates: [avg]
      - name: auth_24h
        type: window
        source: total
        window: 24h
        aggregates: [max, min]
```

Cada agregado dá nome a uma coluna `<name>_<aggregate>`, na ordem declarada:
`auth_5m_avg`, `auth_24h_max`, `auth_24h_min`. **Nesta versão o bloco só é
validado na subida:** a amostragem ainda não existe, as colunas não aparecem em
`_counters.<name>` e nenhuma condição consegue citá-las.

A subida recusa, nomeando o caminho da chave:

- `metrics` vazio, ou mais de **8** métricas no mesmo contador;
- `name` ausente, repetido ou fora de `[a-z_][a-z0-9_]*`. O nome começa com
  letra ou `_`, como um identificador do SQL, porque as colunas geradas são
  citadas sem aspas: `name: 5m` daria `5m_avg`, que nenhuma condição consegue
  citar;
- `type` ausente ou diferente de `window`, e `source` ausente ou diferente de
  `total`, os únicos valores de cada chave;
- `window` ausente, `never`, zero, menor que `1m`, maior que `30d` ou **não
  múltipla de `1m`**. A janela é dividida em 60 baldes, e só uma janela
  múltipla de `1m` dá a cada balde um número inteiro de segundos: `90s` é
  recusado, e não arredondado;
- `aggregates` ausente, vazio, com repetição ou com valor fora de `avg`, `min`
  e `max`.

Dentro do contador, os nomes não podem colidir. Uma coluna gerada não pode
repetir `groupKey`, `total`, uma coluna do `groupBy` ou outra coluna gerada, e
o nome da métrica não pode ser igual a nenhuma dessas colunas, a outra métrica
ou a um alarme do mesmo contador. A mensagem nomeia as duas pontas:

```
counters[0].metrics[0]: column "auth_5m_avg" generated by metrics[0] collides with groupBy column "auth_5m_avg"; rename the metric
```

O nome de um **alarme** igual ao de uma coluna continua aceito, como já era.

### Alarme por condição sustentada

O contador responde quando perguntado; o alarme **avisa**. Declarado dentro do
contador, ele observa uma condição sobre a linha de cada grupo e emite um evento
quando ela se sustenta pelo prazo:

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    alarms:
      - name: olt_down
        when: "total < 100"
        sustainedFor: 2m
        enrich:
          - name: servicos_afetados
            sql: "SELECT customerId, ip FROM servicos WHERE olt = ? LIMIT 5"
            bind: [olt]
```

O `when` fala a **mesma linguagem do `WHERE`** — é o mesmo avaliador — sobre as
colunas da tabela do contador: `groupKey`, as do `groupBy` e `total`. Um nome de
coluna errado **derruba a subida**, com a coluna nomeada.

`sustainedFor` é o que separa alarme de rajada: mínimo de 10 s, e `never` é
recusado (um alarme que nunca dispara não é um alarme). A varredura roda a cada
`sustainedFor ÷ 4`.

`qualifyWhen` é opcional: uma segunda condição, na mesma linguagem do `when`,
que diz se o grupo é **elegível** ao alarme. O caso típico é citar uma métrica de
janela (`qualifyWhen: "auth_24h_max > 10"`), para alarmar só a OLT que teve
gente; as colunas das métricas vêm do amostrador de métricas de janela.
Presente, não pode estar em branco; ausente, todo grupo é elegível.

- O prazo só começa a contar com o `when` valendo **e** o grupo qualificado.
- Um grupo que ainda contava o prazo e deixa de ser qualificado volta ao início
  **sem evento**: nunca houve disparo a resolver.
- Um alarme **disparado** cujo grupo deixa de ser qualificado resolve com
  `"reason":"disqualified"`, e só volta a disparar depois de se requalificar e
  sustentar o prazo inteiro outra vez. É a OLT morta há mais de um dia: continua
  vazia, mas `auth_24h_max` caiu abaixo do limiar e ela deixou de ser o assunto
  do alarme.
- Na mesma passada, a precedência dos motivos é `group_retired` >
  `condition_cleared` > `disqualified`: uma OLT que voltou e ficou inelegível ao
  mesmo tempo sai como `condition_cleared`.
- Coluna sem amostra na janela é NULL, e NULL compara falso: `auth_24h_max > 10`
  sem amostra **não qualifica**.

Mudar o `qualifyWhen`, ou a janela de uma métrica que o `when` ou o
`qualifyWhen` cita, invalida o cronômetro salvo daquele alarme, como mudar o
`when` ou o `sustainedFor`. Por isso o `checkpoint/alarms.state` está na versão
2, **sem leitor da anterior**: na primeira subida depois da v0.8.x o arquivo
antigo sai no log como `alarm.state_unreadable`, e os alarmes em curso recomeçam
a contar o prazo, uma vez.

O `enrich` acompanha o evento com o resultado de consultas parametrizadas pelo
grupo que alarmou — `bind` liga cada `?` a uma coluna do contador (`total` e
`groupKey` incluídos). O valor vai como **parâmetro**, nunca concatenado: um nome
de OLT com apóstrofo não reescreve a consulta. `LIMIT` é obrigatório, no máximo
100. Cada consulta precisa de `name`, que é como o bloco é identificado no
evento.

**A consulta nunca barra o alarme.** Se ela falhar, o evento sai com o bloco
vazio e o motivo. Coluna cujo nome seja sensível (`token`, `senha`, …) sai
redigida.

O disparo é um `WARN` e a resolução um `INFO`:

```json
{"event":"alarm.firing","counter":"autenticados_por_olt","alarm":"olt_down",
 "groupKey":"BR_SPOAM_OLT_14","total":42,"condition":"total < 100",
 "sustainedFor":"2m0s","since":"2026-09-12T03:14:00.000Z","at":"2026-09-12T03:16:00.000Z",
 "olt":"BR_SPOAM_OLT_14",
 "enrichment":{"servicos_afetados":{"rows":[{"customerId":"cli-4471","ip":"10.0.0.9"}]}}}
{"event":"alarm.resolved","reason":"condition_cleared","groupKey":"BR_SPOAM_OLT_14",
 "total":812,"since":"2026-09-12T03:14:00.000Z","at":"2026-09-12T03:25:00.000Z"}
```

`at − since` é a duração do problema. O `reason` distingue **`condition_cleared`**
(o problema passou) de **`group_retired`** (o grupo saiu do contador) — um
equipamento removido do inventário não é um equipamento recuperado.

O cronômetro **sobrevive ao reinício**: o "desde quando" é gravado em
`checkpoint/alarms.state`, e um alarme restaurado dispara com `"restored":true` e
`"downtimeMs"`. Num encerramento gracioso esse número é o tempo de parada; num
crash é "desde quando não há registro", que é maior — a lacuna real é
desconhecida, e um número grande é a resposta honesta. Mudar o `when` ou o
`sustainedFor` no YAML invalida o cronômetro salvo daquele alarme.

Para inspecionar o que está em curso, sem ler log:

```bash
nishidb query --server 127.0.0.1:9142 --schema <schemaName de servicos> "SELECT groupKey, state, since FROM _meta.alarms ORDER BY since ASC"
```

`state` é `firing` (já anunciado) ou `pending` (ainda contando o prazo).

### Emissão periódica: a série de cada grupo

Para alimentar gráfico por equipamento, o contador pode escrever o valor de
**cada grupo** num arquivo NDJSON, periodicamente:

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    emit:
      every: 30s                     # mínimo 10s
      file: autenticados.ndjson      # opcional; default <nome do contador>.ndjson
      maxBytes: 64MiB                # opcional
      keep: 5                        # opcional
```

O arquivo fica em `<dataset>/series/<file>` — o diretório é fixo, e `file` é só o
nome. Uma linha por grupo vivo, a cada intervalo, **inclusive os com total zero**
(o equipamento que apagou por inteiro é justamente a linha que interessa):

```json
{"counter":"autenticados_por_olt","groupKey":"BR_SPOAM_OLT_14","olt":"BR_SPOAM_OLT_14","total":812,"at":"2026-09-12T03:30:00.000Z"}
{"counter":"autenticados_por_olt","groupKey":"BR_SPOAM_OLT_09","olt":"BR_SPOAM_OLT_09","total":0,"at":"2026-09-12T03:30:00.000Z"}
```

Os valores são **tipados**: `total` e as colunas `int64` saem como número, não
como texto. A rotação é por tamanho, com o sufixo crescendo para o passado
(`file`, `file.1`, …) — o teto de disco é `maxBytes × (keep+1)`. Não há fsync:
é telemetria, e um crash perde a cauda da série, que é o desfecho certo.

Grupo aposentado deixa de ser emitido. Baixar o `keep` devolve o disco na subida
seguinte.

### SQL suportado (v1)

```sql
SELECT <colunas | *> FROM <tabela>
  [WHERE <predicado>]
  [ORDER BY <chave> [, <chave>]*]
  [LIMIT <n>]
  [OFFSET <n>]

<tabela> := <nome>                   -- a base do dataset da conexão
          | <schema> "." <nome>      -- endereço completo, qualquer dataset
          | "_counters" "." <nome>   -- atalho: no dataset da conexão
          | "_meta" "." <nome>       -- atalho: no dataset da conexão

<chave> := <coluna> [ASC | DESC] [NULLS (FIRST | LAST)]

<predicado> := <coluna> <comparador> <literal>
             | <coluna> IS [NOT] NULL
             | <coluna> [NOT] LIKE <padrão>
             | <predicado> (AND | OR) <predicado>
             | "(" <predicado> ")"
```

Comparadores `=`, `!=`, `>`, `>=`, `<`, `<=`; `AND`, `OR`, parênteses;
`IS NULL` / `IS NOT NULL`; `LIKE` e `NOT LIKE`; alias e campos entre aspas
duplas (obrigatórias para nomes com hífen, como `"Acct-Status-Type"`).

**O `FROM` qualificado pelo schema é endereço de servidor, não de conexão.**
O par `schema.tabela` endereça uma tabela em qualquer dataset:
`SELECT * FROM telecom.por_olt` abre o contador `por_olt`
do dataset que declara o schema `telecom`, mesmo que a conexão tenha sido
aberta contra outro dataset. Que o par endereça **uma** tabela e não duas vem
de três recusas na **carga da configuração** — `schemaName` único entre
datasets, e nome de contador distinto do nome do dataset e do da tabela
histórica —, não da forma do catálogo; por isso o motor ainda recusa um par
ambíguo em runtime, como rede. Vale para a base e para as views, e é a forma que
um cliente SQL gera sozinho a partir do catálogo (é o que o DBeaver monta ao
clicar na tabela). O dataset da requisição continua sendo o default das formas
NÃO qualificadas — `FROM <nome>` e os atalhos `_counters.<nome>` /
`_meta.<nome>`, que sempre significam "neste dataset".

Qualificador que não é schema declarado é **recusado**, listando os que
existem; schema certo com tabela que não existe é recusado listando as tabelas
daquele schema. Nenhum dos dois é erro interno: o cliente recebe erro de bind
com a posição do nome ofensor.

**As aspas envolvem UM identificador, e isso vale também no `FROM`.**
`FROM "telecom.por_olt"` pede uma tabela cujo *nome* contém um ponto, e não a
`por_olt` do schema `telecom` — para qualificar, cite cada parte:
`FROM "telecom"."por_olt"`. As formas `telecom.por_olt` e `"telecom".por_olt`
também valem; o que não vale é o nome inteiro entre aspas, e a recusa **mostra
a forma certa** em vez de só dizer que o dataset não existe. É o erro de quem
copia o nome já qualificado de uma ferramenta gráfica e o cola entre aspas.

A mesma correção sai pelos **dois** pontos em que essa consulta pode ser
recusada: o bind, quando o nome citado inteiro não é o do handle, e o
`404 UNKNOWN_DATASET` do `POST /v1/query`, quando o nome chega no campo
`dataset` do corpo — que é o caminho do driver JDBC, porque ele lê o nome do
`FROM` e o manda no corpo, e aí a recusa acontece antes de o SQL ser analisado.
Nome desconhecido **sem** ponto recebe a mensagem de sempre, sem falar de aspas.

Os limites `query.maxRows`, `query.timeout` e `query.maxTimeout` são do
**dataset lido**, não do da conexão: numa consulta `FROM telecom.tabela` valem
os declarados no YAML de quem é dono do schema `telecom`, venha a requisição
pelo handle que vier. Uma conexão com teto alto não vira porta para ler, sob
esse teto, um dataset que declarou teto baixo.

`LIKE` vale só para coluna de **texto** — outro tipo é recusado no bind, com a
posição da coluna, nunca com resultado vazio. `%` casa qualquer sequência e `_`
casa **um caractere** (não um byte: `_` casa o `ç` inteiro). **Não há cláusula
`ESCAPE`**, e a consequência precisa estar dita: procurar um `%` ou um `_`
literal é impossível. O padrão é compilado uma vez no bind — inclusive quando
vem por parâmetro (`LIKE ?`) — e o casamento não aloca por linha.

`LIKE` **não usa índice** e degrada para varredura, sempre, inclusive
`'prefixo%'`: os valores de string vão inline no payload e o índice é chaveado
pelo HASH do valor exato, que não preserva ordem nem prefixo. O índice do OUTRO
termo de um `AND` continua valendo — `WHERE status = 'a' AND nome LIKE 'x%'` usa
o índice de `status` e revalida o `LIKE`.

Colunas extraídas de um campo `json` (ver `extract` acima) são projetáveis pelo
nome, por `<campo>.*` — que expande as derivadas daquele documento — ou pelo
caminho:

```sql
SELECT id, persistent.* FROM eventos
SELECT persistent.probeIpNode FROM eventos WHERE probe_ip = '10.0.0.1'
```

`schema.maxNestedCols` limita quantas colunas derivadas uma consulta projeta;
ao estourar, o servidor **recusa nomeando o limite** em vez de truncar.

O namespace `_meta` responde o que o dataset tem, por SQL, sem precisar de
suporte em cliente nenhum:

```sql
SELECT id, name, schema, kind, columns FROM _meta.tables ORDER BY id
SELECT name, type, auto, index, indexed, parent, occurrences, typeMismatches, coercions FROM _meta.fields
SELECT name FROM _meta.fields WHERE parent = 'persistent'
SELECT name, rows, total, groupBy, condition, groups, refused FROM _meta.counters
SELECT partition, appliedOffset, highWatermark, lag, state FROM _meta.kafka ORDER BY partition
```

`_meta.tables` lista **toda tabela consultável do dataset**, a começar por ela
mesma: a base como `TABLE` no schema declarado no YAML, os contadores como
`VIEW` no mesmo schema, e as tabelas de meta como `VIEW` no schema `_meta`. É a
primeira pergunta de quem chega a um dataset que não conhece, e a resposta não
exige rota HTTP nem suporte em cliente nenhum.

`id` é o endereço do **atalho** — `_counters.por_olt`, `_meta.kafka`, ou só o
nome na tabela base —, e é a chave da tabela: `name` sozinho não é único,
porque um contador pode se chamar `tables` e aparecer ao lado desta. O `FROM`
aceita também o par `schema.name`, que é o endereço completo (ver abaixo).
`namespace` é NULL só na tabela base, que não tem atalho com prefixo; `columns`
é a contagem de colunas descritíveis (os nomes estão na `_meta.fields`); e
`description` é NULL em toda linha hoje — nada declara descrição de tabela, e o
catálogo não inventa uma. NULL aqui é ausência: a chave simplesmente não sai na
linha NDJSON.

O `GET /v1/metadata` publica **a mesma lista**, por dataset, no campo `tables`
— com `name`, `schema`, `namespace`, `kind`, `description` e as colunas de cada
tabela, cada uma já com o código `jdbc` que o servidor calcula. O dataset ganhou
também `schema`, o `schemaName` declarado no YAML. É de onde o driver JDBC monta
a árvore de um cliente SQL, que não escreve SQL de catálogo sozinho. As duas
saídas vêm da mesma enumeração do motor: o que a `_meta.tables` responde por SQL
e o que o `/v1/metadata` publica no wire não podem divergir.

Divergem na FORMA de uma ausência, e só nisso: `namespace` e `description`, que
a `_meta.tables` publica como NULL, saem no JSON como `""` — os campos do
documento são strings, não ponteiros, e no wire deste documento só o `alias` de
uma coluna é nulo. Quem ler os dois canais em sequência não deve esperar `null`
no JSON.

Os dois campos são **aditivos**: `columns` continua sendo a lista completa da
tabela base — as mesmas colunas que saem na linha de `kind: TABLE` —, e um
cliente compilado antes deles lê o documento como lia.

`_meta.counters` lista os contadores mantidos na escrita do dataset, com a
definição e os números vivos; `_counters.<nome>`, uma tabela por contador
declarado no YAML, lê os totais (ver "Contadores mantidos na escrita" acima).

`_meta.alarms` lista os alarmes em observação, um por par (alarme, grupo): qual
grupo, desde quando, e se o prazo já se cumpriu (ver "Alarme por condição
sustentada" acima). Vazia num dataset sem alarme declarado — e no
`nishidb query --config` ela é sempre vazia, porque quem varre é o servidor.

`_meta.kafka` responde o estado da fonte Kafka do dataset, **uma linha por
partição** — offset aplicado, offset durável do checkpoint, fim do log, lag,
mais tópico, grupo, bootstrap, estado da ingestão, profundidade da fila, os
contadores desde a subida e o watchdog. É a resposta a "a ingestão está em
dia?" sem HTTP e sem painel, lida das mesmas estruturas que alimentam o
`/metrics` e o `/v1/stats`.

Num dataset sem Kafka, com `enabled: false` ou com o consumidor ainda sem
partição atribuída, ela responde **uma linha de resumo** (`id = 'source'`) com
as colunas de partição NULL — nunca erro. Coluna NULL ali é ausência de medida,
e não zero: sem consumidor, fila e contadores são NULL; `highWatermark` e `lag`
são NULL quando a marca d'água do broker venceu, porque um zero diria "está em
dia" a quem perdeu o broker. No `nishidb query --config`, com o servidor
parado, saem a identidade do fluxo e o offset durável, e o `state` é NULL —
aquele processo não observa ingestão nenhuma.

Um `FROM _outro.x` é recusado nomeando os namespaces que existem, e um
`FROM _meta.x` nomeia as tabelas. As duas recusas são erro **do cliente** —
código `BIND`, HTTP 400, com a posição do nome ofensor —, da mesma família de
"coluna desconhecida": o FROM aponta para o que não existe, e o servidor não
tem por que reportar falha interna a quem digitou um nome errado.

`occurrences` é quantas vezes a chave apareceu no documento; `typeMismatches`,
quantas dessas não couberam no tipo da coluna; `coercions`, quantas chegaram
**noutra forma JSON e ainda assim entraram** — o número numa coluna de texto, a
string numérica numa coluna `int64`. São **três colunas cruas e não uma razão**,
porque a razão não compõe: `2.315 de 2.383` e `93 de 3.334` são problemas de
ordens diferentes, e o numerador sozinho não os distingue. As três são **NULL
para campo de topo** — zero diria "a chave nunca apareceu", que é outra
afirmação —, então `WHERE typeMismatches > 0` já filtra o topo. As N piores
colunas saem por `ORDER BY typeMismatches DESC`.

`coercions` fica **separada** de `typeMismatches`, e não somada a ela, porque as
duas afirmações pedem remédios diferentes: divergiu é "o valor se perdeu, a
coluna está incompleta"; coergiu é "o valor está lá, mas a origem escreve a mesma
coisa de duas formas, e um único literal não a encontra inteira". Ela é também o
que impede a coerção de cegar o painel — sem ela, uma coluna que passa a aceitar
número teria o `typeMismatches` zerado e a inconsistência da origem sumiria da
vista sem ter sumido do dado. `ORDER BY coercions DESC` lista as colunas cujo
filtro por um literal só tem mais chance de vir incompleto.

**São contadores de PROCESSO e zeram no restart**, como os demais do produto.
Isso é aceitável para a pergunta que respondem — "esta coluna é confiável?" —,
mas precisa estar dito: quem lê `0` num dataset recém-reiniciado está lendo
"ainda não medi", não "estável".

A resposta é **estado corrente, não declaração**: campo autodescoberto fixa o
tipo na primeira observação, então duas execuções em momentos diferentes podem
devolver conjuntos diferentes. Nome de dataset não pode começar com `_` — o
prefixo é reservado a esses espaços de nome.

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

Aliases por consulta aceitam `AS` ou a forma implícita, inclusive em colunas
aninhadas e tabelas virtuais:

```sql
SELECT olt AS equipamento, total quantidade
FROM _counters.autenticados_por_olt
ORDER BY equipamento;
```

O alias é o nome publicado no NDJSON, nos metadados JDBC e no `description`
do Python. O tipo e a nulabilidade continuam sendo os do campo de origem.
`WHERE` usa os nomes do schema, incluindo aliases YAML; o alias da consulta
não cria um novo nome para filtros nem para outros itens do `SELECT`.

**Todas as colunas pedidas são preservadas.** Nomes de saída repetidos recebem
`_1`, `_2` etc. Aliases declarados na consulta, com ou sem `AS`, têm
prioridade sobre nomes sem alias; entre aliases iguais, o primeiro mantém o nome. A ordem da projeção não muda:

| Projeção | Nomes de saída |
|---|---|
| `a, a` | `a, a_1` |
| `b, a AS b` | `b_1, b` |
| `a AS x, b AS x` | `x, x_1` |
| `a AS x, b AS x, c AS x_1` | `x, x_2, x_1` |

Todos os nomes pedidos ficam reservados antes da numeração, que usa o menor
índice livre. Os nomes distinguem maiúsculas e minúsculas. Essa regra substitui
a antiga recusa de duplicatas na projeção (issue #41).

`ORDER BY` resolve primeiro os nomes finais da saída, incluindo sufixos, e
depois os nomes do schema. Em `SELECT b, a AS b ORDER BY b`, ordena por `a`;
`ORDER BY b_1` ordena pelo `b` original. Caminhos como `doc.campo` continuam
resolvendo a coluna aninhada; para ordenar pelo alias literal contendo ponto,
use `ORDER BY "doc.campo"`.

Aliases vazios ou aplicados a `*`/`pai.*` são recusados. Palavras reservadas
exigem aspas duplas, inclusive o vocabulário contextual (`AS`, `ORDER`, `BY`,
`ASC`, `DESC`, `NULLS`, `FIRST`, `LAST`, `OFFSET`, `LIKE`, `GROUP`, `JOIN`,
`HAVING`, `DISTINCT`, `UNION`). Isso não muda nomes de campos já aceitos.
Aspas internas se escrevem duplicadas: `SELECT id AS "com""aspas" FROM tabela`.
A saída HTTP continua reservando `nishidb` para as molduras do protocolo;
renomeie um campo com esse nome, por exemplo `nishidb AS valor`.
As provas funcionais, de memória e por mutação estão no
[aceite de aliases de projeção](docs/aceite-aliases-projecao.md).

`ORDER BY` aceita até 16 chaves, com `ASC` (padrão) ou `DESC`. **Campo ausente
vai por ÚLTIMO nas duas direções**, e `NULLS FIRST` inverte só os ausentes.
Isso diverge de PostgreSQL e Oracle (que invertem o lugar do nulo no `DESC`) e
de MySQL e SQLite (que tratam NULL como o menor), e é deliberado: é o único
default em que `ORDER BY x DESC LIMIT 10` significa "os 10 maiores" e não "10
linhas sem valor". O desempate final é sempre por rowId crescente — sem ordem
total, duas páginas de um resultado com empates se sobrepõem e omitem linhas.

Ordenar por coluna **fora da projeção** vale. Colunas `bytes`, `json` e prefixo
de rede não têm ordem definida e são recusadas no bind: a ordem de bytes de um
prefixo é endereço-e-máscara, que *parece* ordem de rede e não é. Ordenar por
posição (`ORDER BY 1`) fica de fora — com `SELECT *` a posição endereçaria um
conjunto de colunas que muda entre execuções.

**`ORDER BY` é um operador BLOQUEANTE, e isso muda o contrato**: a fonte é
drenada por inteiro antes da primeira linha sair, porque a linha certa pode ser
a última lida. O `LIMIT` deixa de ser teto de *trabalho* e passa a ser teto de
*memória*. Quando o teto de memória do operador estoura, o lote corrente é
gravado **ordenado em disco** e a acumulação recomeça; ao fim, os runs são
mesclados. O transbordo vive em `<dataset>/spill/`, sob o mesmo flock do
dataset, e os arquivos são removidos ao fechar a consulta — inclusive quando ela
falha ou é cancelada. Resíduo de um processo morto é limpo na subida seguinte.

O transbordo **não é durável** (não há `fsync`): um crash perde o rascunho de
uma resposta que ninguém ia receber. Se não houver lugar declarado para
transbordar, o estouro **recusa a query** em vez de entregar resultado parcial —
"as N primeiras por rowId, depois ordenadas" não é prefixo de resposta nenhuma.

**`ORDER BY … LIMIT` pequeno não acumula o dataset.** Quando `OFFSET + LIMIT`
cabe em **4.096 linhas**, o operador resolve a consulta com um **heap limitado**:
retém só as N melhores vistas até agora e descarta o resto na hora, sem copiar.
O resultado é *idêntico* ao do caminho completo — mesmas chaves, mesma política
de ausentes, mesmo desempate por rowId —, e é isso que
`TestTopNDaOMesmoResultadoQueOSortCompleto` prova, comparando os dois caminhos
linha a linha sobre um corpus cheio de empates.

O que muda é o custo. Medido sobre 100 mil linhas varridas
(`benchmark/baselines/topn-20260826.txt`, `-count=6` com benchstat), com
`LIMIT 1000`: **83% menos tempo** no caso realista e **43% menos** no pior
formato de dado possível — aquele em que toda linha varrida entra no heap e
despeja outra. Em memória a diferença é de outra ordem: **200 KiB retidos contra
91,6 MiB** materializados pelo caminho completo. Depois de aquecido, o heap
**não aloca por linha** (`TestTopNReaproveitaOBufferDaEntradaDespejada`).

Acima de 4.096 vale o caminho completo. O teto é de **linhas**, e é decidido na
abertura da consulta — quando ainda não se sabe quantas linhas serão varridas.
Foi medido, e o que o fixa é o dataset PEQUENO: com 20 mil linhas varridas,
4.096 ainda economiza 35% de tempo no pior formato, e 16.384 já não tem
diferença estatística. Acima do teto a pergunta deixa de ser "quanto custa
ordenar" e passa a ser "e se não couber" — e quem responde essa é o transbordo,
não um heap maior.

Se o teto de **memória** estourar dentro do modo rápido — N linhas de JSON
grande, por exemplo —, a consulta **degrada para o caminho completo** e segue
transbordando, em vez de ser recusada: um modo novo não pode fazer falhar uma
consulta que já respondia.

`OFFSET` pula linhas que **passaram** no predicado, e o `LIMIT` continua
contando só as **entregues** — `LIMIT 2 OFFSET 1` devolve duas linhas. A ordem
das cláusulas é fixa (`LIMIT` antes de `OFFSET`), e sem `ORDER BY` a paginação é
sobre a ordem de rowId, que é estável só para linhas que não mudam.

Fora da v1: `GROUP BY`, `JOIN`, agregações, `NOT` genérico, e a cláusula
`ESCAPE` do `LIKE`. As cinco cláusulas ausentes — `GROUP BY`,
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

Config do DATASET recusada na subida também sai **3** — inclusive `index: hash`
e `index: bitmap`, que a subida recusa desde que foram substituídos por
`index: true` — pela mesma razão do manifest ilegível: o problema está no
YAML do dataset, não na linha de comando.

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

### Clients

O protocolo remoto (§67-69) tem dois clients de primeira classe, no mesmo
repositório e falando o mesmo fio.

**Java — driver JDBC** ([clients/jdbc](clients/jdbc)), artefato
`nishidb-jdbc.jar`, JDK 21+, zero dependência de runtime. É o que faz DBeaver e
IntelliJ funcionarem:

```
jdbc:nishidb://db01:9142/telecom?tls=true&tlsCaFile=/etc/nishidb/ca.pem
```

**A URL nomeia o schema da conexão** — o `schemaName` que um dataset declara —,
e não o dataset. A árvore mostra só esse schema e o `_meta`, e o servidor
resolve o `FROM` no MESMO escopo: `FROM radius_sessions` é a tabela do schema
da conexão, `FROM telecom.radius_sessions` também, e `FROM fault.events` numa
conexão `telecom` é recusado (SQLSTATE 42000), nomeando o schema da conexão e os
endereços que ela abre. Para consultar outro schema, abre-se outra conexão.
`getSchema()` devolve o schema da URL; `setSchema` com outro é recusado
(3F000), porque o escopo é fixado pela URL.

> **Quebra de contrato (conexão por schema, issue #57):** a URL que nomeava o
> dataset (`jdbc:nishidb://db01:9142/radius_sessions`) deixou de resolver, sem
> fallback. O connect com ela falha com SQLSTATE 3F000 e a mensagem diz qual
> schema usar — `"radius_sessions" is a dataset, and the connection names its
> schema; use "telecom"` — mais a forma da URL.

A árvore que a ferramenta mostra sai do `schemaName` e do catálogo publicado no
`/v1/metadata?schema=<schema da conexão>` — o driver não descobre tabela por
conta própria. Para o dataset acima, se ele declarasse dois contadores:

```
telecom                        (schema: o schemaName do YAML)
├── Tables
│   └── radius_sessions        a tabela base
└── Views
    ├── autenticados_por_olt   um contador declarado em `counters:`
    └── autenticados_por_pon   outro
_meta                          (schema do motor; descreve o dataset da conexão)
└── Views
    ├── counters
    ├── fields
    ├── kafka
    └── tables
```

As quatro do `_meta` existem em todo dataset declarado, com contador ou sem,
com Kafka ou sem: "este dataset não consome de Kafka" é uma resposta, e
`SELECT * FROM _meta.kafka` nunca pode ser erro. A tabela **histórica** é a
exceção — ela publica só `fields` e `tables`, porque contador e ingestão são do
dataset base.

O `TABLE_SCHEM` de cada linha é preenchido, e é ele que faz a ferramenta gerar
`SELECT ... FROM telecom.autenticados_por_olt` ao clicar na tabela — a forma
qualificada, que o servidor resolve dentro do schema da conexão. Não há
catálogo: o endereço tem dois níveis, o de cima é o schema, e `getCatalogs`
responde vazio. As tabelas do `_meta` saem como `VIEW`, e nunca como
`SYSTEM TABLE`: objeto de sistema é escondido por default nas ferramentas
gráficas, e tabela que existe para ser descoberta não pode nascer invisível.

O driver exige um servidor com a conexão por schema: contra um binário anterior
o connect falha, em vez de montar uma árvore que o servidor não sustenta.

**Python — client DB-API 2.0** ([clients/python](clients/python)), pacote
`nishidb`, Python 3.10+, também sem dependência de runtime. É o que faz
`pandas.read_sql` e as ferramentas de BI funcionarem.

Ainda **não está no PyPI**: instala-se a partir do repositório, e como é stdlib
pura nenhuma das formas baixa dependência nenhuma.

```bash
pip install ./clients/python
```

```python
import nishidb

with nishidb.connect("nishidb://db01:9142/radius_sessions", user="leitor") as cx:
    cur = cx.cursor()
    cur.execute(
        "SELECT user_name, nas_ip FROM radius_sessions WHERE status = ? LIMIT 100",
        ("Stop",),
    )
    for linha in cur:
        print(linha["user_name"], linha["nas_ip"])
```

A senha nunca entra na string de conexão — use `password=`, `password_file=` ou
a variável `NISHIDB_PASSWORD`. Os dois clients recusam redirect, ignoram proxy
do ambiente e **não têm caminho para desligar a verificação de certificado**.

Os dois são **só leitura**, porque é isso que o NishiDB é: os dados entram por
ingestão. E os dois são provados contra o binário de verdade, com o MESMO
corpus — `make test-jdbc` e `make test-python-it` compartilham a montagem em
[scripts/it-comum.sh](scripts/it-comum.sh).

Detalhes de cada um em [clients/python/README.md](clients/python/README.md).

## Documentação

- [Arquitetura](docs/architecture.md) — como o sistema funciona, com os números.
- [Client Python](clients/python/README.md) — DB-API 2.0, tipos, erros e limites.
- [Aceite do M1](docs/aceite-m1.md) — os 25 critérios, item a item.
- [Aceite do M4](docs/aceite-m4.md) — a compaction em regime, com a escala e o
  ambiente da rodada declarados.
- [Aceite do M5](docs/aceite-m5.md) — autenticação e TLS (Fase 11); a rodada
  operacional no piloto (Fase C) segue pendente.
- [Aceite do schema declarado](docs/aceite-schema-declarado.md) — o `schemaName`,
  o catálogo, a `_meta.tables` e a árvore do cliente SQL; o aceite pelo DBeaver
  real segue pendente.
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
