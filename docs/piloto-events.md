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
| Tópico | `events`, **1 partição**, group `nishidb-events-piloto` |
| Brokers | `10.113.144.229, .230, .237, .238:9093` (texto claro, sem TLS) |
| Chave | `identifier`, `mode: merge`, **sem `sequence`** |
| Bind | `127.0.0.1:9142` — sem autenticação, loopback é a única proteção |

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

## Limites conhecidos deste piloto

- **Sem autenticação e sem TLS** (Fase 11). Loopback é a única proteção, e por
  isso o bind não deve mudar.
- **Expiração por regras condicionais** (a chave `ttl:` deixou de ser um bloco
  único e virou uma LISTA). O YAML do piloto precisa migrar antes da próxima
  subida — a forma antiga é recusada no load, com a mensagem apontando a nova:

  ```yaml
  materialization:
    ttl:
      # Alarme FECHADO sai em minutos, não em meses: a condição usa a mesma
      # gramática do WHERE e é avaliada sobre o estado pós-merge.
      - when: "severidade = 0"
        expireAfterWrite: 30m
      # Regra de fundo: o prazo que o piloto já tinha, para todo o resto.
      - expireAfterWrite: 90d
  ```

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
