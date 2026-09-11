# Piloto: o tópico `events` em produção

Primeiro dataset real do NishiDB, subido em 22/08/2026 sobre alarmística
Netcool/Omnibus. Este documento registra o que foi **medido** — não o que se
esperava —, porque três medições contrariaram a previsão feita a partir da
spec.

O runbook operacional vive no servidor, em `/app/nishidb/RUNBOOK.md`.

## O que está no ar

| Item | Valor |
|---|---|
| Host | `10.200.20.218`, `/app/nishidb`, unit `nishidb.service` |
| Binário | `/app/nishidb/current/nishidb` (symlink para `releases/<versão>/`) |
| Tópico | `events`, **1 partição**, group `nishidb-events-piloto-1` |
| Brokers | `10.113.144.229, .230, .237, .238:9093` (texto claro, sem TLS) |
| Chave | `identifier`, `mode: merge`, **sem `sequence`** |
| Bind | `127.0.0.1:9142` (leitura, sem credencial) e `127.0.0.1:9143` (TLS + Basic auth) — nada exposto à rede |

## As três medições que contrariaram a previsão

### 1. A razão eventos/entidade cresce com a janela de observação

Previ que a compaction ficaria **ociosa** neste dataset. Errado, e o erro foi de
amostragem: extrapolei da cauda do tópico.

| Janela amostrada | Eventos/entidade | Fração viva | Elege? (limiar 0,50) |
|---|---:|---:|---|
| 30 mil mensagens (cauda) | 1,7 | ~59% | **não** |
| 80 mil mensagens | 2,28 | ~44% | sim |
| Replay do backlog | **4,38 e subindo** | ~23% | **sim, com folga** |

A causa é simples depois de vista: um alarme é atualizado ao longo da sua vida,
e uma janela curta pega cada `identifier` poucas vezes. Quanto maior a janela,
mais atualizações por entidade — e a compaction depende exatamente disso.

**A lição de método:** razão eventos/entidade medida numa janela curta **não
extrapola**. Para decidir indexação ou elegibilidade de compaction, meça sobre
uma janela comparável à que o dataset vai viver.

### 2. Nenhum campo serve de `sequence`, e o tópico não precisa de um

O stale check do §25 descarta evento cuja sequence não avança. Medido sobre
80.443 eventos (45.188 com `identifier` repetido):

| Campo | Subiu | Empatou | **Regrediu** | Veredito |
|---|---:|---:|---:|---|
| `receiveTime` | 45.144 | 7 | **37** | 0,08% seriam descartados |
| `procTime` | 41.091 | 9 | **29** | 0,07% seriam descartados |
| `matchedCounter` | 1.325 | 42.512 | **1.351** | 94% empata — inútil |

Declarar qualquer um com `strategy: greater` sumiria com ~20 mil atualizações
legítimas ao longo do backlog, **sem erro e sem log**.

A saída é o próprio tópico: **uma partição**, logo a ordem de offset é ordem
total. O `sequence` existe para entrega fora de ordem (§26), que não é o caso.
Omitindo-o, vale a ordem de chegada. Medição depois de 2 milhões de eventos:
`stale = 0`, `descartados = 0`.

### 3. O bitmap é denso — o custo é (distintos × rows / 8)

O índice bitmap é **um arquivo de bits denso por valor distinto** (§41). Com
~17,8M rows, cada valor custa ~2,2 MB de arquivo:

| Campo | Distintos | Custo estimado | Decisão |
|---|---:|---:|---|
| `severity`, `type`, `workgroup`, `acknowledged` | 3–6 | ~35 MB | bitmap |
| `technology` | 39 | ~86 MB | bitmap |
| `manager` | 91 | ~200 MB | bitmap |
| `alertGroup` | 404 | ~890 MB | fora |
| **`node`** | **4.376+** | **~9,6 GB** | **`index: hash`** |

`node` é a dimensão de consulta mais valiosa e a mais cara como bitmap. Ficou
com `index: hash`, e o **índice hash físico existe** — o débito 19 fechou. Um
`WHERE node = 'x'` que varria 17,8M linhas e morria com `context deadline
exceeded` depois de 1.175.015 candidatos passa a resolver pelo índice.

**Dataset já criado não ganha o índice retroativamente.** A abertura RECUSA
declarar `index: hash` sobre um dataset com linhas, pelo mesmo motivo do bitmap:
o índice cobriria só o que vier daqui pra frente e a query omitiria as linhas
antigas em silêncio. Para o piloto, isso significa recriar o dataset.

**O conjunto de índices secundários é imutável depois da primeira row** (débito
22, que agora vale para os dois motores): mudar exige reprocessar o tópico para
um dataset novo — e ver a janela de retenção
abaixo antes de contar com isso.

### 4. A cauda da distribuição de chaves estoura o teto do storage

Medido depois, com o piloto já no ar: **2 eventos em 4 milhões foram descartados**
por `EVENT_TOO_LARGE` — `identifier` de **1099 e 1074 bytes**, contra o teto de
`MaxKeySize` de **1024**.

A sonda que dimensionou a chave viu **máximo de 320 bytes** em 80 mil eventos.
Não errou a medida: errou a extrapolação, pelo mesmo motivo do item 1 — a cauda
de uma distribuição não aparece numa janela curta.

O efeito é pequeno em volume (0,00005%) e **permanente em consequência**: esses
alarmes nunca são materializados, e o descarte é por política (`log-and-skip`),
com WARN no log e o contador `nishidb_ingest_invalid_total` subindo. Não há
corrupção — o dataset segue íntegro.

As saídas possíveis, nenhuma gratuita: elevar `MaxKeySize` é mudança de formato;
derivar a chave por hash muda a semântica e quebra a consulta por `identifier`
literal; aceitar e monitorar é o que está feito hoje.

**A lição de método é a mesma dos itens 1 e 2**, e vale escrevê-la de uma vez:
neste projeto, toda decisão tomada a partir de uma amostra de tópico precisa
declarar o tamanho da janela — e tratar máximo observado como piso, nunca como
teto.

## A janela de retenção é de ~18,8 horas

Medido: **452 msg/s** de produção, **30,6 milhões** de mensagens retidas.

Esse número governa toda decisão de migração. Reconstruir o dataset do zero
pelo tópico só recupera **as últimas ~19 horas**; o que é mais antigo não existe
mais no Kafka. Qualquer mudança que exija reprocessar — conjunto de bitmaps,
troca de chave, migração de formato do M6 — precisa caber nessa janela, ou
aceita-se perder o histórico anterior a ela.

A ingestão drena a ~2.700 ev/s contra 452 de produção, então o backlog inteiro
entra em ~3h30 — com folga dentro da janela.

## O que o piloto vai responder

**O plateau de disco em escala real.** A rodada de aceite do M4 não o provou
([`aceite-m4.md`](aceite-m4.md)): o gate passou por uma amostra colhida fora da
janela de carga, e recontado reprova. Aqui o dataset tem padrão de update de
verdade, em vez de corpus gerado — se o disco estabilizar com o backlog inteiro
ingerido e a ingestão em regime, é a prova que faltou.

Primeiras leituras, com ~2 milhões de eventos ingeridos: disco oscilando em
torno de 1 GiB **e caindo** durante as passadas, 2,5 GiB devolvidos ao
filesystem, garbage em 29%.

## Retipo de `firstSeen`/`lastSeen` para `datetime` (23/08/2026)

Os dois campos chegavam como texto — não estavam declarados no YAML, então
caíam em `auto` e viravam `string`. O formato da origem é o
`DateFormat.MEDIUM` do Java em locale `en_US`: `Aug 23, 2026 07:54:59 PM`.
Como texto isso **não ordena cronologicamente** (`Aug` < `Dec` < `Feb`), então
filtro por janela de tempo era impossível, não apenas lento.

Agora são `type: datetime` com `format: "MMM d, yyyy h:mm:ss a"`, e o instante
é resolvido pelo `TZ` do host — `America/Sao_Paulo`.

**O ensaio veio antes, e é o que dá confiança na grafia.** A lição dos itens 1
a 4 é que amostra curta não extrapola, então a validação foi feita sobre o
corpus materializado, não sobre uma janela de tópico: **200.000 valores reais,
zero fora do padrão**. Dias `01`/`05`/`09` e horas `01`–`09` aparecem todos
zero-padded, ou seja, a origem emite `dd`/`hh`. Ainda assim o `format`
declara as formas **variáveis** (`d`, `h`), que aceitam as duas grafias pelo
mesmo preço: uma troca de provider de locale na JVM do produtor emitiria um
dígito só, e sob `log-and-skip` isso seria descarte silencioso por evento.

Depois, 100.000 eventos reais foram ingeridos num dataset de rascunho com o
schema novo: **100.000 aplicados, zero descartados**. Só então o piloto foi
tocado.

**O retipo exigiu recriar o dataset**, e é o preço conhecido: os bytes já
gravados carregam a tag de `string`, e o predicado exige igualdade exata de
tag — sem recriar, toda row anterior contaria `typeMismatch` e a mesma coluna
sairia como texto nas rows velhas e como instante nas novas, sem nenhum erro.
Foram descartados **3.628.004 identifiers / 15,3 GB**, e o que voltou é o que
cabe na janela de retenção do tópico. O YAML e o checkpoint antigos ficaram
em `/app/nishidb/backup-<stamp>/` no host.

**`datetime` não faz round-trip textual**: entra `Aug 23, 2026 07:54:59 PM` e
sai `"2026-08-23T22:54:59.000Z"`. O texto de relógio de parede é consumido
para resolver o instante e não é preservado — renderizar em horário local
faria o mesmo dataset servido de dois hosts produzir bytes diferentes.

A janela agora é consultável:

```bash
nishidb query --server 127.0.0.1:9142 --dataset events \
  "SELECT identifier, lastSeen FROM events WHERE lastSeen >= 'Aug 23, 2026 08:00:00 PM' LIMIT 20"
```

Literal em outra grafia é **erro de bind**, com a mensagem nomeando o format
declarado — nunca resultado vazio, que pareceria resposta.

## Recriação com índice hash e tipos declarados (26/08/2026)

Quarta recriação do dataset, e a primeira que serve a duas coisas de uma vez.

### O que mudou

**`node` passou a ter índice hash de verdade.** Ele sempre esteve declarado
`index: hash`, e até aqui isso era metadado sem efeito — o planner tinha a
regra, o executor sabia consumir, e o `Store` não implementava a capability
(débito 19). Toda igualdade sobre `node` degradava para varredura, que é a
causa do `SQL Error [57014] ... context deadline exceeded` visto em produção.

Medido depois da recriação, com 31 mil linhas: a mesma consulta responde em
**11 ms pelo índice contra 585 ms pela varredura**, com resultado idêntico. A
razão cresce linearmente com o dataset — em 2,4 milhões de linhas é da ordem de
mil vezes.

O custo em disco é o argumento que o `index: hash` sempre teve: o índice ocupa
**~1 MB por milhão de postings**, contra os **~9,6 GB** que um bitmap custaria
para os 4.376+ valores distintos de `node`.

### As 25 chaves declaradas, e a regra que decidiu quase todas

O `persistent` ganhou um bloco `extract` com 25 caminhos. A escolha NÃO foi
"qual tipo é maioria": ela saiu da assimetria da coerção, que é o contrário do
que a intuição sugere.

**`int64` e `double` aceitam número JSON E string numérica; `string` aceita só
string.** Então para uma chave que chega ora como `5`, ora como `"5"`, declarar
o tipo NUMÉRICO recupera as duas formas, e declarar `string` recupera metade.

Recuperação medida sobre 30.000 documentos reais, antes de tocar o piloto:

| chave | perdia | recupera | tipo |
|---|---:|---:|---|
| `codigoGrupoResponsavel` | 84,5% | **100%** | `int64` |
| `ti` | 54,6% | **100%** | `int64` |
| `severidade` | 41,3% | **100%** | `int64` |
| `dataClear` | 25,5% | **98,9%** | `datetime` |
| `dataFalha` | 14,0% | **98,9%** | `datetime` |
| `slot` | 6,9% | 76,5% | `string` |

**`slot` fica `string` de propósito**, mesmo com 23% de `int64` a recuperar: os
valores têm zero à esquerda (`"07"`), e o `int64` os aceita NORMALIZANDO para
`7` — o que tornaria `WHERE slot = '07'` impossível. Recuperar 23% a mais não
paga perder a grafia.

**As datas viraram `datetime`** com o mesmo `format` de `firstSeen`/`lastSeen`.
Isso recupera o valor E dá ordem cronológica, que `string` não daria: como
texto, `Aug` < `Dec` < `Feb`.

**Os contadores (`curtoTally`, TTLs) ficam `int64` e a perda é aceita.** São
`int64` em 97%, com `double` na cauda que o `int64` recusa — 0,8% medido.
Declarar `double` recuperaria esses 0,8% ao preço de transformar todo contador
em ponto flutuante, e um Tally é inteiro.

### O resultado agregado

`nestedTypeMismatch` caiu de **0,21 para 0,047 por evento** — 78% de redução.
O documento de melhorias previa ~0,01 declarando as dez piores; declaramos 25 e
chegamos a 0,047. A diferença é honesta e tem explicação: aquela previsão saiu
de uma distribuição em que `dataFalha` sozinho respondia por 65% da perda, e a
recriação refixou os tipos por sorte de qual evento chegou primeiro — a
distribuição de hoje é outra.

### Como a decisão foi tomada, e por que ela é repetível agora

Pela primeira vez a lista quente saiu de **SQL sobre o dataset inteiro**, e não
de um script descartável sobre amostra:

```sql
SELECT name, type, occurrences, typeMismatches FROM _meta.fields
WHERE parent = 'persistent' AND typeMismatches > 0 ORDER BY typeMismatches DESC
```

Ela contradisse a tabela do documento original em três pontos:
`codigoGrupoResponsavel` e `frontSATotal` precisavam ser **numéricos** e não
`string`, e `ti` e `severidade` **não estavam na lista** — o script de amostra
nunca as tinha visto.

O contador diz QUE o tipo está errado, não QUAL é o certo; para isso ainda foi
preciso uma amostra dirigida de 30.000 documentos, agora só sobre as chaves que
o SQL apontou. É muito menos trabalho que a varredura cega anterior, e é o que
a "Pergunta em aberto 3" do documento de melhorias descreve como alternativa ao
contador por (coluna, tipo).

## A tabela histórica: o que subiu, e o que a primeira medição disse

**Subiu em 26/08/2026, 22:53**, na release `v0.6.0-155-g381cc1f`, com
`expireAfterWrite: 30d`, `maxBytes: 250GiB` e índice hash em `identifier` e
`node`. O dataset `events` **não** foi recriado: a tabela irmã nasce vazia e o
base segue como estava, o que é a diferença em relação às congeladas (débito
83).

### A medição que contrariou o dimensionamento

Sessenta segundos depois da subida, com o backlog ainda em replay:

| Grandeza | Medido |
|---|---:|
| Linhas gravadas | 84.181 |
| Bytes dos segmentos | 232.978.556 |
| **Bytes por linha** | **2.767** |
| Descartadas / degradado | 0 / não |

O número por linha é o que fecha a conta, e ele **derruba o prazo declarado**:

| Pergunta | Resposta |
|---|---|
| Quantas linhas cabem em 250 GiB? | 268.435.456.000 / 2.767 = **97 milhões** |
| Quanto tempo isso é, a 452 ev/s (a taxa de produção medida acima)? | **~2,5 dias** |
| Quanto disco custariam 30 dias? | 1,17 bilhão × 2.767 = **3,24 TB** |
| Quanto o host tem livre? | **1,1 TB** |

Ou seja: **os 30 dias não são alcançáveis neste host**, nem subindo o teto. O
`maxBytes` manda sozinho, e a janela real é de dias. Isso não é defeito de
configuração — as duas políticas convivem por desenho e vence a primeira que
bate —, mas o `expireAfterWrite: 30d` do YAML é hoje decorativo, e quem ler o
arquivo sem ler esta tabela vai acreditar nele.

A série que diz a verdade é `nishidb_history_oldest_written_at`. É por ela que
o bloco deve ser reajustado — com número medido, não estimado.

**A estimativa anterior errou por 5×, e o registro fica.** Antes de subir, duas
contas indiretas davam 683 B/linha (pelo estado vivo do base) e 3.510 B/linha
(pela contagem de segmentos menos o copy-forward da compaction). O valor real
ficou perto do teto do intervalo. A lição é a mesma que a razão eventos/entidade
já tinha ensinado nesta página: **dimensionamento de tabela histórica não sai de
aritmética sobre o dataset materializado.** Suba com teto conservador, meça, e
só então declare o prazo.

### A primeira consulta

O alarme `SBC-TROP-ALERTKEY_PAPIE_NGNTRO_SBC05_192586005` aparece no `events`
como `severity: 5, type: 1` — aberto e crítico. No histórico, pelo índice hash:

```
severity 5, type 1
severity 5, type 1
severity 0, type 2
severity 5, type 1
severity 0, type 2
severity 5, type 1
```

Ele está **flapando**: abre crítico, resolve, reabre. O estado corrente não
tinha como mostrar isso, e é exatamente o que a tabela existe para responder.

### O que ficou registrado do plano original

O piloto é o caso que a feature existe para servir: com 4,38 eventos por
entidade e ~17,8M entidades, o `events` guarda o estado corrente de cada alarme
e descarta o caminho que ele percorreu. Depurar "por que esta coluna está
assim" exige inferir o merge a partir do resultado.

O que segue é o raciocínio de subida, preservado porque o critério de escolha
dos índices continua valendo.

### O YAML que subiu

```yaml
history:
  expireAfterWrite: 30d
  maxBytes: 250GiB
  fields:
    identifier:
      index: hash
    node:
      index: hash
```

`identifier` ganha índice **aqui** e não no base: lá ele é a key e a primary já
o indexa, e o schema recusa `index:` sobre a key por isso. No histórico não há
primary, e ele é a coluna de consulta mais provável de todas.

`node` repete a decisão que a recriação de 26/08 já tomou pelo mesmo motivo —
4.376+ valores distintos custariam ~9,6 GB como bitmap e ~1 MB por milhão de
postings como hash. A tabela irmã tem mais linhas que o base, então a razão
entre os dois só piora.

`alertGroup` e `manager` ficam de fora do histórico mesmo tendo bitmap no base:
o bitmap custa (distintos × rows / 8), e as rows aqui são as de EVENTOS, não as
de entidades. É por isso que o índice não é herdado — herdar multiplicaria o
custo sem ninguém pedir.

**Os índices podiam ser declarados só na criação, e isso mudou no M6.** A
abertura recusava `index:` sobre uma tabela com linhas, porque o índice global
cobriria só o que viesse dali para frente — foi por isso que os dois subiram
junto com a tabela, e não numa segunda passada depois de medir. Com o índice por
segmento a recusa saiu: declarar um campo depois é aceito, responde certo desde o
primeiro instante (segmento sem a seção enumera) e agenda a reconstrução dos
segmentos antigos. Quem CONSOME essa fila é o laço de reconstrução, que subiu no
ciclo do índice por segmento e fechou o débito 22 — os segmentos anteriores à
declaração varrem só até a reconstrução alcançar cada um deles.

### A pegadinha do reprocessamento

Vale a mesma que a recriação de 26/08 registrou, e aqui ela morde mais forte: o
`_writtenAt` de todo evento é o instante da **ingestão**, não o do evento
original. Num replay do tópico do início, o histórico inteiro nasce com a data
do replay — e a retenção por tempo conta a partir dali, não da data real do
alarme.

Consequência prática: subir o histórico **junto** de um reprocessamento faz os
78M eventos do backlog entrarem todos dentro da janela de prazo ao mesmo tempo,
e quem decide o corte passa a ser o `maxBytes`. Subir sobre o dataset já
materializado é o contrário — a tabela começa vazia e cresce no ritmo do
tópico, que é o regime para o qual o prazo foi escolhido.

**Ligar o histórico NÃO exige recriar o `events`**, e é a diferença em relação
às congeladas (débito 83): a tabela irmã é um dataset novo e vazio, e o base
segue como está.

### O que continuar medindo

Nesta ordem, porque cada uma responde a pergunta que a anterior abre:

1. `nishidb_history_rows_total` contra `nishidb_ingest_events_total` — a tabela
   está recebendo tudo?
2. `nishidb_history_dropped_total` e `nishidb_history_degraded` — perdeu
   alguma coisa, e por quê?
3. `nishidb_history_oldest_written_at` — a janela real que ela cobre. Se ficar
   bem abaixo dos 3 dias declarados, o `maxBytes` está mandando.
4. `nishidb_history_segments_purged_total{reason}` — confirma qual das duas
   políticas decidiu.
5. `nishidb_history_data_bytes` contra `nishidb_history_index_bytes` — quanto
   do disco a purga NÃO consegue devolver.

A medida que fechava o dimensionamento — bytes por linha — **saiu**, e está no
topo desta seção: 2.767. O que falta agora é a mesma medida no regime ESTÁVEL,
depois que o replay do backlog terminar: o evento de replay e o de fluxo normal
não têm por que ter o mesmo tamanho, e é o segundo que dimensiona o disco a
longo prazo.

## O travamento de 27/08/2026, e o que ele ensinou

A ingestão ficou **parada por uma hora** sem que nada acusasse. O
`/v1/health` respondia UP, `ingestion.state` dizia UP, não havia erro no log,
e o lag subia. Fica registrado com o método, porque o diagnóstico errado veio
antes do certo.

### O que parecia, e o que era

| Sintoma | Conclusão apressada | O que era de fato |
|---|---|---|
| `%CPU` do `ps` em 104% | "está girando" | `%CPU` é a média da VIDA do processo, não o instantâneo. Nove horas de ingestão dão isso. Não havia spin |
| só `expiry_scanned_total` se movia, a 18 mil/s | "o coletor monopolizou" | era o único componente COM trabalho a fazer. 460 milhões de varreduras é o cursor circular com a fila de expirações cheia — comportamento documentado |
| `queue_depth` em 479 | "acordar perdido na nossa fila" | a série estava **congelada**. Ver abaixo |

A causa real: o poller ficou dentro do `kgo.PollRecords` e nunca voltou. Todo
o resto estava correto e ocioso — writer esperando num channel vazio,
compaction, expiração e retenção dormindo entre passadas, **nenhuma goroutine
bloqueada em lock**, `AllowRebalance` chamado em todos os caminhos. Não era
deadlock nem contenção: era fome, vinda de cima do storage.

### A série que mentiu

`nishidb_kafka_queue_depth` e `queue_bytes` eram gravadas **dentro** do laço de
poll. Com o poller travado, elas pararam no último valor amostrado enquanto o
writer seguia drenando — e o painel passou a afirmar 479 registros esperando
sobre uma fila vazia, no minuto exato em que a pergunta era se o gargalo
estava antes ou depois dela.

**Série congelada é pior que série ausente.** Ela não diz "não sei"; afirma um
número velho com a mesma cara de um número novo. Hoje a profundidade sai de um
coletor, lida no instante do scrape — e a prova de que a correção pegou é que
três leituras seguidas em produção dão números diferentes (159 → 206 → 254).

### O que mudou por causa disso

**O watchdog** (release `v0.6.0-160-g8caad2c`, ligado por padrão: 60 s,
3 tentativas). O mecanismo está em [`architecture.md`](architecture.md), seção
"O watchdog". O que importa aqui: se o `PollRecords` travar de novo, o
consumidor é recriado em até um minuto em vez de uma hora parada.

**O pprof, em loopback.** O diagnóstico custou um `kill -QUIT` — a única forma
de ver as pilhas num binário sem pprof, e ela mata o processo. Foi aceitável
porque a ingestão já estava morta e o restart era a mitigação de qualquer
forma, mas transformou uma consulta numa interrupção. A unit passou a subir
com:

```
--pprof 127.0.0.1:9144
```

O endereço é explícito **de propósito**: o código binda onde mandarem, sem
forçar loopback. Exposto, o pprof entrega despejo de heap e o
`/debug/pprof/profile` vira vetor de negação de serviço. Foi conferido de fora
do host — a porta recusa conexão, enquanto a de consulta TLS na mesma máquina
responde 401.

O despejo que custou uma parada virou:

```bash
curl -s "http://127.0.0.1:9144/debug/pprof/goroutine?debug=2"
```

A unit NÃO é versionada neste repositório; o backup do estado anterior ficou
em `/etc/systemd/system/nishidb.service.bak-20260827-084139`.

### O que continua em aberto

**Por que o `PollRecords` travou** é interno ao franz-go e não foi isolado —
débito 90. O watchdog trata o sintoma, e trata bem. Se ele disparar mais de
uma vez sem que a recriação resolva, o problema é outro e precisa de
instrumentação dentro do poll.

E vale a ressalva honesta: **o watchdog nunca disparou em produção.** Está
testado em unidade, com as guardas escritas, mas o primeiro disparo real ainda
não aconteceu. `nishidb_kafka_watchdog_trips_total` subindo com a ingestão
voltando confirma o desenho; subindo sem voltar, refuta.

## A quarta medição: o lag que mentia (28/08/2026)

A investigação começou por uma pergunta de operação — "com o `history` ligado a
instância não dá conta, o lag cresce" — e a primeira coisa medida contrariou o
enunciado.

`nishidb_kafka_lag` vinha de `kadm.Lag`, que devolve o lag do offset
**commitado no consumer group**. O commit só sai **depois do checkpoint
durável** (§48), a cada 30 s. Entre dois commits o offset do group fica parado
e a série cresce exatamente à taxa de produção — com o consumidor
perfeitamente em dia.

Amostrando a 1 Hz durante 181 s:

| O que a série fazia | Medido |
|---|---:|
| Pico entre commits | **14.167** |
| Valor logo após o commit | **515** |
| Eventos consumidos no segundo da queda | **478** |
| Crescimento da série entre commits | **450 ev/s** |
| Produção documentada do tópico | **452 ev/s** |

O lag despencava 13.652 num segundo em que 478 eventos foram consumidos: não
era drenagem, era o denominador se movendo. E o crescimento entre commits
batia com a produção, que é a assinatura de um numerador congelado.

**O lag real, lido logo depois de cada commit, ficou entre 515 e 994
mensagens** — 1 a 2 segundos de atraso, sem tendência de crescimento na janela
inteira.

### O que mais a mesma rodada mediu

| Grandeza | Medido | Como |
|---|---:|---|
| CPU por evento | **902 µs** | `/proc/<pid>/stat` × `nishidb_ingest_events_total`, 60 s |
| CPU do processo | **0,336 de um core** | o host tem 32 |
| Teto com um core saturado | **~1.109 ev/s** | 1/902 µs |
| Consumo | 372–430 ev/s | — |

O pipeline inteiro — poll, parse, merge, `Put`, `Apenda` — roda numa goroutine
única. O produto não fica sem CPU: fica sem paralelismo.

Onde vão os 902 µs, por profile de 45 s em produção:

| Componente | % do CPU |
|---|---:|
| `buscaPath` — a extração declarada, varrendo o documento uma vez por path | **21,6%** |
| `ExpiraOnePass` — varredura de TTL, em background | **18,8%** |
| `Store.Put` — tabela base | 16,0% |
| `Apenda` — tabela histórica | **11,4%** |
| parse do evento em si | 1,8% |

A tabela histórica custa ~17% a mais no caminho de ingestão. É real, e não é o
dominante — a varredura repetida da extração custava mais que ela, e era
trabalho puramente redundante (ver `docs/architecture.md`).

### Bytes por linha do histórico, em REGIME

O número publicado na medição anterior (2.767 B/linha) foi colhido durante o
replay do backlog, e o próprio documento registrava que faltava medi-lo em
regime. Medido agora, sobre 120 s e 46.800 linhas:

| | Replay | **Regime** |
|---|---:|---:|
| Bytes por linha | 2.767 | **2.394** |
| Janela real em 250 GiB | ~2,5 dias | **~3,3 dias** |
| Escrita do histórico | — | **80,6 GB/dia** |

Na mesma janela o dataset base **encolheu 19,8 MB**: a compaction está ganhando
do crescimento nesta escala.

### E uma parada longa que ainda não chegou

`primary.idx` está em **exatamente 128 MiB** (2²³ slots × 16 B) com 4.570.919
linhas — **54,5% de ocupação**. O rehash dispara aos 70%, a ~1,3 milhão de
entidades daqui, e quando disparar vai alocar 268 MiB e reescrever todo slot
vivo **sob o lock do writer**, com a ingestão parada. Não é a causa dos stalls
de até 2,4 s já observados — o arquivo não cresceu durante a medição —, mas é a
próxima parada longa a caminho.

## A quinta medição: o gargalo mudou de lugar (28/08/2026)

Medição feita para servir de "antes" da subida do ciclo de folga de capacidade.
Ela acabou **cancelando a subida**, porque o que ela mostrou não é o que o
diagnóstico daquele ciclo descreve. Fica aqui no formato que este documento já
usa: o que foi medido, não o que se esperava.

Binário no ar: `v0.6.0-160-g8caad2c`. Nenhuma fase do ciclo estava em produção.

### O piloto está saturado em um core

Quatro janelas de 60 s, `/proc/<pid>/stat` cruzado com
`nishidb_ingest_events_total` — o mesmo método que produziu os 902 µs/evento do
diagnóstico:

| Janela | eventos | CPU | µs/evento | ev/s | cores | teto de 1 core |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 19.771 | 58,27 s | **2.947** | 329 | 0,969 | 339 ev/s |
| 2 | 18.626 | 63,80 s | **3.425** | 309 | 1,060 | 292 ev/s |
| 3 | 19.419 | 67,23 s | **3.462** | 323 | 1,117 | 289 ev/s |
| 4 | 17.980 | 58,07 s | **3.230** | 299 | 0,965 | 310 ev/s |

Contra o diagnóstico de 26/08: **902 µs/evento e 0,336 de um core**. O custo por
evento **triplicou** e o processo passou de um terço de core para um core
inteiro. Ele não tem mais folga para consumir: a produção documentada é de ~452
ev/s e o consumo medido ficou entre 299 e 329.

Estado do dataset na medição: 5.086.149 linhas no base (eram 4.570.919),
10.327.180 no histórico, `expiry_scanned_total` em 651.686.022.

### E não é onde o ciclo estava olhando

Profile de CPU de 45 s (38,65 s de amostras), versionado em
`benchmark/baselines/piloto-perfil-20260828.pb.gz` — abra com
`go tool pprof -top benchmark/baselines/piloto-perfil-20260828.pb.gz`:

| Componente | % do CPU |
|---|---:|
| `hash.(*Index).Set` (cumulativo) | **82,66%** |
| └─ `mmap.(*Region).AtomicLoad64` | 77,67% |
| &nbsp;&nbsp;&nbsp;└─ `sync/atomic.(*Int32).Add` (plano) | **50,66%** |
| `jsonp.(*Parser).Next` — o parse | 2,46% |
| `PatchBuffer.buscaPath` — a segunda passada | 2,25% |

Metade do CPU do processo é **um `atomic.Add` de 32 bits**, e ele vem do
`RLock`/`RUnlock` que `mmap.Region.AtomicLoad64` toma **a cada leitura de 64
bits**:

```go
func (r *Region) AtomicLoad64(off int64) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return atomic.LoadUint64(r.word(off))
}
```

`hash.Index.Set` faz sondagem linear e chama isso **uma vez por sonda**. Cada
par `RLock`/`RUnlock` são duas operações atômicas de leitura-modificação-escrita
sobre a MESMA palavra (`readerCount` do RWMutex), compartilhada por todas as
goroutines que tocam a região — writer, consultas, compaction e coletor. É
disputa de linha de cache no caminho mais quente que existe.

O índice em disco: um arquivo, `00010.hidx`, 32 MiB = **2^21 slots**, com
reconstrução declarada em 70% de ocupação efetiva. Como cada update faz um `Set`
e um `Clear`, a coluna acumula lápides sem ganhar postings — e lápide conta para
a ocupação efetiva, alongando a cadeia de sondagem até a próxima reconstrução
passar a faxina.

### O que isso significa para o ciclo de folga de capacidade

O ciclo foi desenhado contra um perfil em que o parse era 33% e a varredura de
TTL 19%. Nesse perfil, hoje, o parse é **2,5%** e a varredura não aparece no
topo. A Fase 4 paraleliza o decode: ela atacaria 2,5% do CPU atual.

Isso não invalida o ciclo — o lag honesto, a instrumentação e a agenda de
vencimentos continuam valendo por si. O que muda é a expectativa: **subir o
ciclo hoje não faria o piloto acompanhar a produção**, e medir a subida contra
este "antes" mediria outra coisa.

### A subida foi primeiro cancelada, e depois feita de outro jeito

Dois fatos cancelaram a subida na forma em que ela estava planejada:

1. O gargalo medido não é o que o ciclo ataca (acima).
2. **Ela é porta de mão única.** A agenda de vencimentos grava a seção `0x8003`
   no manifest, e ela é CRÍTICA: um binário que não a conheça recusa o dataset,
   e o servidor recusa subir com um dataset que não abre. O rollback automático
   do `nishidb-update.sh` — que é o que torna cada fase reversível — voltaria o
   symlink, o binário velho falharia ao abrir e o serviço ficaria FORA, não
   degradado.

A decisão foi então **recriar o dataset e subir sobre ele**, que é o que a
seção seguinte registra. Com dataset novo o argumento (2) muda de forma: o
caminho de volta deixa de ser o rollback do script e passa a ser o dataset
ANTERIOR, movido de lado em vez de apagado.



## A sexta medição: o ciclo no ar, com dataset novo (28/08/2026)

Subida de `v0.6.0-188-gf41ef67` — o ciclo de folga de capacidade inteiro exceto
a Fase 1 (passada única no `extract`), que ficou noutra branch. O dataset
anterior foi MOVIDO para `/app/nishidb/dados.pre-ciclo-20260828`, não apagado:
28 GB num disco com 1 TB livre, e a volta é um `mv` mais a troca do symlink.

O consumidor retomou perto da ponta do tópico, e não do começo. Com dataset
vazio e sem offsets locais, `ajustaOffsets` devolve os offsets DO GRUPO — o
`autoOffsetReset: earliest` só vale para partição sem commit, e o grupo
`nishidb-events-piloto-8` vinha commitando havia 50 h. Sem replay.

### O que a subida provou

**O lag parou de mentir.** Antes: serrote de 1.481 a 9.712 com o consumidor em
dia. Depois: `nishidb_kafka_lag` em **0**, com `high_watermark` publicado pelo
mesmo coletor, e picos isolados e legíveis (203, 39, 22) quando o writer de fato
não acompanhou por um instante.

**A divisão da Fase 4 é real, e a proporção também.** No profile de 45 s
(`benchmark/baselines/piloto-perfil-fase4-20260828.pb.gz`):

| Cadeia | % cumulativo |
|---|---:|
| `decodificadores.roda` → `Decode` → `PatchBuffer.Parse` | **33,2%** |
| `Writer.aplica` → `Materializer.Aplica` | **33,8%** |

Meio a meio. É a medida do teto que o godoc de `decodeWorkers` afirmava por
teoria: tirar 33% do caminho do writer vale ~1,5×, e não mais.

**O diagnóstico original estava certo — para um dataset NOVO.** O mesmo profile
traz `PatchBuffer.Parse` em 33,20% e `buscaPath` em 19,65%, quase idênticos
aos 32,55% e 21,55% de 26/08. E `hash.Index.Set` caiu de 82,66% para 10,94%.
O gargalo não é "o parse" nem "o índice hash": é o índice hash **conforme o
dataset cresce**.

### A curva de degradação

Doze janelas de 2 minutos, do dataset com 48 mil linhas até 91 mil:

| hora | linhas | µs/evento | cores | lag | arena (KiB) | reprocessos |
|---|---:|---:|---:|---:|---:|---:|
| 21:25 | 48.784 | 858 | 0,270 | 0 | 31.469 | 1.064 |
| 21:27 | 54.075 | 854 | 0,271 | 0 | 31.815 | 1.068 |
| 21:29 | 57.865 | 868 | 0,233 | 0 | 31.897 | 1.070 |
| 21:31 | 62.109 | 1.014 | 0,299 | 0 | 32.101 | 1.074 |
| 21:33 | 66.385 | 923 | 0,284 | 6 | 32.161 | 1.078 |
| 21:35 | 70.058 | 1.133 | 0,342 | 203 | 32.285 | 1.085 |
| 21:37 | 73.800 | 1.012 | 0,308 | 22 | 32.406 | 1.086 |
| 21:39 | 77.427 | 948 | 0,269 | 0 | 32.442 | 1.090 |
| 21:41 | 81.056 | 1.019 | 0,305 | 0 | 32.568 | 1.091 |
| 21:43 | 84.251 | 891 | 0,261 | 3 | 32.630 | 1.091 |
| 21:45 | 88.208 | 1.050 | 0,325 | 0 | 32.699 | 1.109 |
| 21:47 | 91.283 | 1.013 | 0,308 | 39 | 32.755 | 1.109 |

**O que ela mostra, e o que ela NÃO mostra.** O dataset quase dobrou (48 mil →
91 mil linhas) e o custo por evento subiu de ~860 para ~1.010 µs. É subida real,
mas ela é da MESMA ordem da dispersão da própria série (854 a 1.133 na janela
inteira): nesta faixa a curva ainda é rasa. O joelho que leva aos 3.200 µs
medidos com 5,09 milhões de linhas está muito mais adiante, e este piloto vai
atravessá-lo sozinho nos próximos dias — é o que há para observar.

**Duas afirmações da Fase 4 se sustentaram fora da bancada:**

- Os reprocessos por descoberta subiram **45 em ~130 mil eventos** (0,03%),
  contra os 1.064 do aquecimento inicial. A promessa era "em regime tende a
  zero", e é o que a série faz.
- A memória da esteira ficou **parada** em ~32 MiB durante a janela inteira, com
  `slots_shrunk_total` em zero: nada passou do limiar de 64 KiB por slot. O
  gauge nasceu de desconfiança e serviu para mostrar que a desconfiança não se
  confirmou, que também é resposta.

### O que fica para observar

O piloto está com 0,3 de um core e lag zero. O dataset anterior, com 5,09
milhões de linhas, estava com 1,0 core e não acompanhava. **A variável que
separa os dois estados é o tamanho do dataset**, e o piloto vai percorrer essa
distância sozinho. O que vale acompanhar, nessa ordem:

1. `nishidb_kafka_lag` deixando de voltar a zero — é o primeiro sinal honesto
   de que a folga acabou;
2. o custo por evento contra o número de linhas, para localizar o joelho;
3. `nishidb_ingest_decode_serial_total`, que deve continuar plano — se ele
   passar a acompanhar a contagem de eventos, é a origem inventando chave nova,
   não descoberta em curso.


## Limites conhecidos deste piloto

- **Sem autenticação e sem TLS** (Fase 11). Loopback é a única proteção, e por
  isso o bind não deve mudar.
- **Expiração por regras condicionais** (a chave `ttl:` deixou de ser um bloco
  único e virou uma LISTA). O YAML do piloto precisa migrar antes da próxima
  subida — a forma antiga é recusada no load, com a mensagem apontando a nova:

  ```yaml
  materialization:
    ttl:
      # Alarme RESOLVIDO sai em minutos, não em meses: a condição usa a mesma
      # gramática do WHERE e é avaliada sobre o estado pós-merge.
      - when: "type = 2"
        expireAfterWrite: 3m
      # Regra de fundo: o prazo que o piloto já tinha, para todo o resto.
      - expireAfterWrite: 90d
  ```

  **Foi para produção em 26/08/2026**, junto de duas colunas congeladas que
  guardam o estado de NASCIMENTO do alarme — o que o merge, por definição,
  sobrescrevia:

  ```yaml
  schema:
    fields:
      originalSeverity:
        frozenFrom: severity     # a severidade com que o alarme abriu
        index: bitmap
      originalFirstSeen:
        frozenFrom: firstSeen    # o primeiro firstSeen visto
  ```

  Congelada não tem preenchimento retroativo (débito 83), então o dataset foi
  **recriado** e o consumer group trocado (`nishidb-events-piloto-7`) para
  reprocessar o tópico do início. Consequência de reprocessar: o `_writtenAt`
  de todo evento é o instante da INGESTÃO, não o do evento original, então todo
  alarme já resolvido no histórico é materializado e expira três minutos
  depois — o estado vivo converge para os alarmes não resolvidos.

  Cada escrita reavalia as regras e regrava o prazo, então o alarme que REABRE
  volta ao prazo longo — o prazo curto é cancelado pela própria escrita. Alarme
  ativo nunca expira; alarme abandonado sai da query depois do prazo da regra
  que casou. O **predicado de leitura** vale desde já; o **coletor** é a Fase 4,
  e até ele entrar o disco não é devolvido — a entidade some da consulta e o
  record fica.

  As entidades já materializadas não têm o campo `_expiresIn` no documento e
  caem na duração da regra de **fundo** — os 90 dias de sempre —, passando a
  obedecer as condicionais na primeira escrita seguinte. A migração não exige
  reprocessar nada.
  - Ligar não exigiu recriar o dataset: as entidades materializadas ANTES
    ficam sem `_writtenAt`, o que o índice lê como prazo **desconhecido** —
    e desconhecido nunca vence. Elas ganham o campo assim que receberem
    qualquer atualização, e a semeadura da Fase 4 dará carência às que
    sobrarem. No corte havia 33.383 nessa condição contra 8.395 já com prazo.
  - **Como saber se o coletor está trabalhando.** As séries são
    `nishidb_expiry_running` (0/1), `nishidb_expired_total`,
    `nishidb_expiry_grace_total` e `nishidb_expiry_scanned_total`. Nos
    primeiros meses `expired_total` fica em **zero por estar certo** — nada
    tem 3 meses de inatividade —, e quem mostra vida é `scanned_total`
    subindo. **Não meça pela consulta `_writtenAt IS NULL`**: a carência
    grava no ÍNDICE de prazos e não no payload, então a entidade continua
    aparecendo nessa consulta depois de já ter prazo. Medir por ali leva à
    conclusão errada de que o coletor não subiu.
  - **A duração escolhida muda o desenho do agendador.** A 452 ev/s, 90 dias
    são 3,51 bilhões de escritas na janela: a deque de baldes do §57 custaria
    28 GB de heap. Para prazo longo a varredura completa é a estratégia certa
    — 8 B por rowId alocado, e com 90 dias a resolução de 1 minuto é
    irrelevante, então ela roda de hora em hora.
- ~~**Sem índice hash físico** (débito 19): consulta por `node` é full scan.~~ **O débito 19 FECHOU**: o índice hash físico existe e a consulta por `node` passa pelo índice. Vale para dataset CRIADO com o campo declarado — a abertura recusa acrescentar `index: hash` a um dataset que já tem linhas, porque o índice cobriria só o que vier depois.
- **Sem `GROUP BY` nem agregações** (§9). `ORDER BY`, `OFFSET` e `LIKE` entraram no ciclo M6.
- **O binário não sabe dizer sua versão**: não há `--version`, então não há como
  correlacionar o que está no ar com um commit.

## Armadilha do ambiente: o proxy corporativo e o loopback

`/root/.bashrc` neste host exporta `http_proxy=http://10.215.39.5:8080`, e o
`.bashrc` só vale em shell **interativo**. Qualquer ferramenta que honre essa
variável manda a consulta a `127.0.0.1:9142` para o proxy da empresa, que
responde **403** — e o sintoma engana, porque parece o servidor fora do ar.

O detalhe cruel é que o erro **não aparece por ssh não-interativo**
(`ssh host 'comando'` não lê o `.bashrc`), só para quem digita o comando no
terminal. Diagnóstico que funciona para o script e falha para a pessoa.

O `scripts/nishidb-stats.py` desliga a descoberta de proxy por construção
(`ProxyHandler({})`). Para `curl` na mão, use `--noproxy '*'`:

```bash
curl -s --noproxy '*' 127.0.0.1:9142/v1/stats
```

## A sétima medição: o índice por segmento em produção (30-31/08/2026)

A subida do `v0.6.0-250-ge13a6dd` em 30/08 às 11:52 trocou os dois motores
globais de índice pelo índice por segmento, com o dataset de 135 GiB migrado
**no lugar** (manifest v2 → v3, irreversível). O YAML precisou de duas mudanças
que só o ensaio revelou — as 10 declarações `index: hash`/`index: bitmap`
viraram `index: true`, e o `history` ganhou `index: {window: 6h, maxBytes:
2GiB}` — e `node` voltou a ser indexado no histórico, desfazendo o paliativo de
28/08.

O registro da subida está em
[`piloto-pos-ciclo-20260830.md`](../benchmark/baselines/piloto-pos-ciclo-20260830.md)
e o aceite completo, com as 25 horas de soak, em
[`aceite-indice-por-segmento.md`](aceite-indice-por-segmento.md). O que fica
aqui é o que muda a leitura deste piloto:

- **O congelamento acabou.** `poll_blocked` ficou em **0,0 durante as 25 h**
  — nas 273 amostras do soak, sem exceção. Na madrugada anterior à subida o
  mesmo contador crescia 1:1 com o relógio havia dezoito minutos.
- **A taxa que se lê mudou de significado.** 332–375 ev/s com `lag ≈ 0` é o
  tópico sendo acompanhado em tempo real; os 733 ev/s do binário antigo eram
  o sistema correndo atrás do atraso que os próprios congelamentos criavam.
  Por CPU: 863 µs/evento contra 2.360 µs do binário antigo no mesmo dataset —
  2,7× menos.
- **O índice parou de crescer.** A parcela por segmento oscilou entre 102 e
  127 MB com a janela de 6 h reciclando (`selado` 56–69, `fora` 402 → 633),
  contra o teto de 2 GiB. O `nishidb_history_index_bytes` total continua
  subindo — são os arquivos densos por-linha da história, que acumulam até a
  marca-d'água dos 15 dias de retenção; é comportamento anterior a este ciclo
  e a série da janela não o governa.
- **RSS pede decomposição, não susto:** ~869 MiB totais ao fim, dos quais
  **126 MB anônimos** (heap) — o resto é page cache de mmap que o kernel
  recolhe sob pressão, e este host tem 57 GiB livres.
- Os únicos erros das 25 h foram **5 `query.falhou`** por TIMEOUT de `ORDER
  BY` sobre milhões de candidatos — o tema do ciclo da consulta longa, que
  este piloto continua evidenciando a cada consulta grande.

## Snapshot de leitura, a janela do índice e a primeira purga (31/08–01/09/2026)

Três coisas aconteceram nesta janela, e a ordem em que estão escritas não é a
de importância: o que subiu foi o **snapshot de leitura com isolamento**, mas o
que mudou a vida do piloto foi um achado que apareceu no caminho — **91% do
histórico estava sem índice**, e ninguém sabia.

### O que subiu

`v0.6.0-281-g4ada9d8`, por troca de symlink com verificação automática
(`nishidb-update.sh`), vindo de `v0.6.0-250-ge13a6dd`. Junto foram os 24
commits da `main` que o piloto nunca tinha visto — em sua maioria documentação
e gates do ciclo do índice por segmento, mais quatro correções de código do
storage.

A verificação pós-start fechou sozinha: health UP, versão confirmada, ingestão
viva, `invalid_total` com Δ=0.

**Soak de ~20 h sem reinício**, com 23,2 milhões de eventos ingeridos, lag do
Kafka voltando a zero a cada oscilação e o heap anônimo estável entre **120 e
130 MB** (o RSS total de 2,3–2,6 GB é page cache de mmap: `RssFile`, que o
kernel recolhe sob pressão).

As quatro séries novas nasceram com zero nos dois datasets, como o gate exige.

### O achado: a janela do índice cobria 6 h de um dado que vive 15 dias

A pergunta que destravou isto foi operacional — *"por que esta consulta dá
timeout?"*:

```sql
SELECT node, alertKey, manager, firstSeen, lastSeen, summary, description, type, severity
FROM events_history WHERE identifier = '<uma chave>';
```

`identifier` **é** declarado com `index: true` no histórico. O que a derrubava
era o bloco `history.index`, com `window: 6h` contra `expireAfterWrite: 15d`:

| | antes |
|---|---|
| Segmentos com índice | 65 selados + 1 corrente |
| Segmentos **fora** da janela | **667** |
| Lookups resolvidos por índice | 132 |
| Lookups resolvidos por **enumeração** | **1330** |

O prazo foi aumentado três vezes antes de a causa aparecer, e cada aumento
mediu a mesma coisa por outro ângulo — a progressão é linear porque a consulta
estava lendo o histórico inteiro, uma linha por vez:

| Prazo | Candidatos varridos | Desfecho |
|---|---:|---|
| 30 s | 4.248.320 | timeout |
| 600 s | 55.040.000 | timeout, 1 linha entregue |
| 900 s | 81.187.072 | timeout, 1 linha entregue |
| — | ~90.500.000 | seria preciso ~1000 s, e o histórico cresce |

**A lição de método é a mesma da §1 deste documento, por outra porta:** subir o
prazo mede o problema, não o resolve. O número que denunciava a causa já estava
publicado — `nishidb_index_segmentos{estado="fora"}` em 667 — e ninguém o
estava lendo.

### O conserto, e o que ele custou

`window: 6h` → `15d`. A reconstrução processou os **734 segmentos** em ~25
minutos, em background, com o lock por segmento: o lag do Kafka oscilou (picos
de ~500) e voltou a zero a cada vez. A ingestão nunca ficou para trás.

| | estimado | medido |
|---|---:|---:|
| Índice do histórico | ~1,3 GB | **1,33 GB** |
| Teto declarado (`index.maxBytes`) | | 2 GiB |
| Segmentos fora da janela | | **0** |

E a consulta que não terminava em 900 s:

| | |
|---|---:|
| Logo após a reconstrução | **3,1 s** |
| Com o cache quente, no dia seguinte | **206 ms** |

Outras latências medidas na mesma rodada: lookup por `node` com 100 linhas em
918 ms, consulta indexada no `events` em 181 ms, `_meta.fields` em 31 ms.

### A primeira purga do histórico, e a interação que o snapshot introduziu

O dataset bateu o teto de 250 GiB em 01/09 às 16:34, com o crescimento medido
em 1,80 GB/h. A purga disparou sozinha e passou a devolver **um segmento
(268 MB) por passada**, a cada ~5 minutos:

```json
{"event":"retencao.passada","porTempo":0,"porTamanho":1,
 "indicesExpulsos":0,"bytes":268435279,"markDagua":118351}
```

Em ~1 h e 20 segmentos purgados, o dado ficou **ancorado no teto**, oscilando
±30 MB em torno dele. Nenhum `retencao.sem_punch_hole`: o ext4 devolveu os
blocos de verdade, e o teto não mente.

**A interação com o snapshot foi exercitada de propósito**, porque as 17
primeiras purgas rodaram sem consulta nenhuma em voo e o painel de isolamento
marcava zero em tudo. Com uma varredura longa aberta sobre o histórico, a
passada seguinte registrou:

```json
{"event":"retencao.marca_segurada_por_consulta",
 "marca_pelo_prazo":2265975,"marca_aplicada":2150138,"linhas_seguradas":115837}
```

**115.837 linhas seguradas** — as que, antes deste ciclo, teriam desaparecido do
meio do resultado sem erro e sem log. E a contraprova, que importa tanto quanto:
fechada a consulta, a passada seguinte avançou livre para a marca 2.387.143 e
devolveu os 268 MB. Segura enquanto precisa, libera assim que pode.

Um achado de brinde, que não estava em teste nenhum: o cliente foi **morto
abruptamente** no meio da varredura, e o snapshot voltou a zero mesmo assim — o
servidor detectou a desconexão e devolveu o que estava retido.

### A configuração que passou a valer

Duas mudanças na config do servidor e uma no YAML do dataset:

```yaml
# nishidb.yaml
runtime:
  memoryTarget: 8GiB
  query:
    maxBufferPerQuery: 64MiB     # era 4 MiB (padrão)
    snapshotMaxBytes: 512MiB     # teto do journal de versões
query:
  defaultTimeout: 900s           # era 30s

# datasets/events.yaml
history:
  index:
    window: 15d                  # era 6h
```

**Sobre o orçamento de memória, o que é honesto dizer:** o host tem 62 GB e 56
deles já estavam em page cache servindo os 228 GB do dataset. Estas chaves
governam o buffer da linha de **saída** — o que recusa uma consulta com
`ROW_TOO_LARGE` —, e não a varredura. Elevá-las não acelera leitura; o que faz
a leitura andar é o page cache, que não se configura por YAML.

**Uma armadilha operacional que custou uma medição errada:** o host tem DOIS
`nishidb.yaml`. O de `/app/nishidb/` é órfão do layout anterior a
`releases/<versão>/`, ninguém o lê, e ele estava desatualizado em muito mais que
o timeout — sem TLS e sem autenticação. O que vale é
`/app/nishidb/current/nishidb.yaml`, apontado explicitamente no `ExecStart` da
unit. A confirmação de qualquer prazo se faz pelo que o servidor **publica** em
`/v1/metadata`, nunca pelo YAML que se encontra primeiro.

### O que este ciclo NÃO provou

- **Os dois tetos ainda não se encontraram.** O índice está em 1,68 GB contra o
  teto de 2 GiB, e `indicesExpulsos` continua zero. Quando ele encostar, a
  janela passará a expulsar por cima enquanto a purga corta por baixo, e a
  janela efetiva vai estabilizar no menor dos dois limites — o número que dirá
  qual venceu é `nishidb_history_index_window_seconds`.
- **O journal de versões nunca reteve nada em produção.** Todos os painéis
  marcaram `bytes: 0`: no histórico a linha nunca é atualizada (custo zero por
  construção), e no `events` não houve consulta longa concorrente com
  atualização da mesma linha. O caminho está provado por teste e por mutação,
  não por carga.
- **`expiredTotal` continua zero** — nenhuma consulta foi recusada por estourar
  o teto do journal, o que significa que o teto está sobrando e ainda não
  sabemos como ele se comporta sob pressão.
