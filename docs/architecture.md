# Arquitetura do NishiDB — estado do M1

Escrito para quem chega agora. Os números são medidos, não estimados, e vêm da
rodada de aceite de 2026-08-15 (`benchmark/baselines/aceite-8m-20260815.md`).

## O problema

Materializar o **estado corrente** de milhões de entidades a partir de um fluxo
de eventos JSON parciais, e permitir consulta SQL sobre esse estado — cabendo
num orçamento de memória fixo, independente do tamanho do dataset.

O caso de origem é accounting RADIUS: cada sessão recebe um evento de início,
dezenas de interim-updates parciais e um evento de fim. O que interessa é o
estado corrente da sessão, não o histórico.

A tensão central: **8 milhões de entidades e menos de 1 GiB de RSS.** Guardar o
estado em heap não fecha. É essa restrição que explica quase todas as decisões
abaixo.

## Visão geral

![Containers do M1: CLI, materializer, engine SQL e storage, e os arquivos do dataset em disco](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/c4_container.puml)

> **Sobre as URLs dos diagramas.** O repositório é **privado**, então
> `raw.githubusercontent.com` responde 404 a quem não está autenticado — e o
> proxy do PlantUML busca o `.puml` anonimamente. Por isso **todos** os
> diagramas deste documento — o número deles muda a cada fase, e um número
> escrito aqui só serviria para envelhecer errado —
> passam por `git-raw.nishisan.dev`, que serve o conteúdo do repositório
> privado ao proxy. Os fontes estão versionados em
> [`docs/diagrams/`](diagrams/) e renderizam em qualquer servidor PlantUML.
> As URLs fixam **`main`** de propósito: fixar a branch de trabalho quebraria
> os links no merge.
>
> **Se as imagens não aparecerem, o problema é o certificado, não o fonte.** Em
> 2026-08-22 o certificado TLS de `git-raw.nishisan.dev` estava **expirado
> desde 2026-08-21 15:07 UTC**, e sem conseguir buscar o `.puml` o proxy
> devolve **HTTP 500** para todas as URLs desta página. O conserto é renovar o
> certificado no host — não há nada a mudar aqui. Os fontes em si continuam
> válidos, e isso é verificável sem o proxy: `plantuml -checkonly -failfast2`
> em `docs/diagrams/*.puml`. Registrado no débito 39.

Um binário só, sem dependência externa em runtime. Dois comandos: `ingest`
materializa NDJSON num dataset; `query` executa SQL e escreve NDJSON.

## Ingestão

![Fluxo de ingestão: do evento JSON à location publicada, com a fase SET do índice antes do commit point e sem fase de CLEAR](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/ingest_flow.puml)

### Ingestão por Kafka

O caminho por arquivo (`nishidb ingest`) e o por tópico (`nishidb server`)
compartilham materializer e storage; o que o Kafka acrescenta é a **posição**.
O pipeline é o do §27 — poller único, fila limitada, writer único — com os
**decode workers** que o §27 marca como opcionais, ligados por
`source.kafka.consumer.decodeWorkers` e desligados por padrão (`1`).

**O writer continua sendo um só, e isso não é negociável.** O que passou a ser
paralelo é o DECODE: parse do JSON, resolução contra o schema e conversão de
tipos, que o profile do piloto mediu em 298 dos 902 µs de CPU por evento. A
aplicação — estado anterior, sequence check, merge, `Put`, `Apenda` — segue numa
goroutine única, porque é ela que escreve, e o §28 continua valendo para ela.

A ordem por partição do §26 também não depende mais de "uma goroutine de ponta a
ponta", e a garantia que a substituiu é MAIS FORTE. Cada item recebe um número
de sequência no ENVIO, que é sequencial; uma **esteira** — anel indexado por
`seq & mask` — devolve os decodes ao writer nessa ordem. Consumir por `seq`
crescente reproduz exatamente a sequência que a goroutine única veria: os mesmos
vizinhos e o mesmo estado anterior em cada `Put`. A ordem por partição sai disso
como caso particular.

**A descoberta de campos NÃO é paralela, e a razão é de correção, não de
desempenho.** `LookupOrCreate` atribui o FieldID na ordem de chamada, o FieldID
é formato de disco e a posição da coluna no `SELECT *` é contrato do driver
JDBC — com N decodificadores, essa ordem viraria a de quem ganha o mutex, e "o
tipo é fixado na primeira observação" viraria "na primeira a chegar no lock". O
decode é somente-leitura sobre o schema: ele para no primeiro metadado que teria
de criar e devolve o evento intacto, e o writer o refaz pelo caminho completo,
na ordem de offset. Em regime isso não acontece — o piloto tem ~1.700 colunas e
não descobre mais nenhuma —, e o custo, quando acontece, é um parse dobrado por
EVENTO de descoberta, medido em `nishidb_ingest_decode_serial_total`.

**Quantos workers usar é medida, não intuição.** O ganho é limitado pela fração
que continua serial, então ele satura cedo. Medido em laboratório com store em
memória (`benchmark/baselines/decode-workers-20260828b.txt`, mínimo de 10
rodadas): **2 workers dão 1,75×; 4 não acrescentam; 8 regridem para 1,53×** por
contenção. Duas rodadas do mesmo dia diferiram por menos que a dispersão desta
máquina (30% a 46%) e concordaram na forma — é a forma que este número afirma,
não a terceira casa. Em produção a aplicação é mais cara, então
a saturação chega antes, nunca depois. O default é `1` e subir para `2` é a
única mudança que esse baseline sustenta sem medição própria do dataset.

**A esteira é o terceiro consumidor de memória da ingestão**, e nenhum dos dois
gates da fila o enxerga: um conta registros enfileirados, o outro conta bytes de
payload cru, e nenhum vê o arena tipado que o decode produziu e o slot reteve
até a marca d'água. A janela do anel é o `maxPollRecords` (o trabalho em voo) e
não o `ingestionQueueSize` (a rajada de entrada), **arredondado para cima até a
potência de dois** — o índice do anel é `seq & mask`, e a máscara só existe
assim; com o padrão de 1.000, a janela efetiva é 1.024. O slot que passa do
limiar tem os buffers SOLTOS ao ser liberado — o array, não o comprimento, que é
a lição do débito 28 —, e o limiar por slot é derivado: `ingestionQueueBytes`
dividido por essa janela efetiva, com piso de uma página.

**Soltar o buffer e soltar a referência são coisas diferentes**, e confundi-las
custou um conserto. O slot devolve SEMPRE as janelas sobre o evento — a direta,
a do documento do pai e o `data` dos parsers —, porque essa memória é do buffer
de fetch do franz-go e segurá-la num slot ocioso mantém vivo o lote inteiro. Já
os buffers do decode, que são nossos, só são soltos acima do limiar, e aí a
`key` vai junto do arena: ela é uma janela sobre ele, e esquecê-la manteria o
array vivo enquanto o gauge afirmaria que a memória voltou. `nishidb_ingest_decode_arena_bytes` e
`nishidb_ingest_decode_slots_shrunk_total` são as duas metades da história.

**Duas séries mudaram de dono junto, e a troca seria invisível.** O
`nishidb_kafka_writer_idle_seconds_total` saía do acumulador da FILA, porque era
o writer quem a consumia; com os decodificadores no meio, quem espera na fila
são eles, e são K. Continuar publicando aquele número como ociosidade do writer
mentiria por um fator — com oito decodificadores parados, `rate()` seria 8 — e a
espera do writer, que é a metade que decide se ele é o gargalo, deixaria de ser
medida. O `writer_idle` passou a sair da esteira; a espera dos decodificadores
virou `nishidb_kafka_decode_idle_seconds_total`, com a divisão por
`nishidb_ingest_decode_workers` documentada em `operacao-kafka.md`.

**O poller não espera mais o writer a cada lote.** Até 28/08/2026 ele emitia um
marcador de fim de lote e bloqueava até o writer alcançá-lo, para só então
liberar o rebalance. A garantia que isso comprava é real — nenhum commit pode
rodar para uma partição depois de a revogação dela ter sido reconhecida —, mas
a barreira era forma forte demais: ela exige drenar **quando a revogação
chega**, não a cada volta do poll. Medido no piloto, o preço era o overlap
inteiro entre as duas goroutines: o writer ficava **72% do tempo ocioso** e o
poller **28% parado** nessa espera.

Hoje o dreno mora em `aoRevogar`, que chama `Writer.Quiesce`: uma barreira vai
para o fim da fila, e o quiesce só volta depois que o writer a alcança, tendo
aplicado tudo o que estava à frente e tornado a posição durável. Duas coisas de
graça vieram junto — o checkpoint voltou a rodar só na goroutine do writer, o
que elimina por construção a corrida sobre o mapa de offsets, e o quiesce
carrega o erro de volta, coisa que o marcador não fazia.

O `select` do quiesce tem **três** braços: a barreira, o writer que já
encerrou, e o contexto. O segundo não é zelo — ele roda dentro de um callback
do franz-go, e um writer que saiu por `ErrPausado` nunca alcançaria a barreira:
o callback travaria, o group cairia e a ingestão morreria em silêncio.

A fila tem **dois** gates. O de registros é o `ingestionQueueSize` da spec; o
de bytes é inventado, e existe porque contar registros não é unidade de
memória — 5000 slots com payload grande estouram o orçamento de 1 GiB sem que
nenhuma contagem reclame. Um record acima do teto passa sozinho com a fila
vazia, senão a ingestão travaria para sempre num deadlock que nenhuma métrica
explicaria.

![Pipeline de ingestão Kafka: fetch, fila limitada com backpressure, decode em K goroutines, esteira de reordenação por seq, writer único, e a ordem persist → checkpoint durável → commit do offset](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/kafka_pipeline.puml)

**A ordem do §48 é a única proteção contra perda.** O `manifest.bin` e o
`kafka.offsets` são dois arquivos e dois renames, sem transação entre eles. Na
ordem certa, um crash no meio deixa a posição ATRÁS dos dados: o seek
reprocessa uma janela já aplicada, que converge (com `sequence`, o gate do §25
descarta; sem ela, reaplicar na mesma ordem de partição produz o mesmo
payload). O custo é lixo — records órfãos e o resíduo de liveness já
registrado. Na ordem inversa, os eventos entre os dois pontos nunca mais
seriam lidos.

### Retomada da posição

O checkpoint local é autoritativo (§49); o offset commitado no group é
secundário e nunca posiciona o consumer no boot. Quatro situações RECUSAM a
subida da ingestão — e nas quatro as **consultas seguem de pé**, porque o §51
põe `start queries` antes de `resume ingestion`:

![Retomada da posição: as quatro recusas de subida da ingestão e o ponto exato do seek pela posição local](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/offset_recovery.puml)

A saída de todas elas é o comando `nishidb offsets`, que exige o servidor
parado (toma o flock) e confirmação explícita para escrever.

### O watchdog: quando o consumidor para de receber

![Watchdog da ingestão: os três fatos do gatilho, a guarda contra backpressure e a escada de recriação](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/watchdog_ingestao.puml)

Um consumidor pode parar de entregar registros **sem erro nenhum**. A conexão
segue de pé, o processo segue saudável, o `/v1/health` responde UP — e nada
chega. Aconteceu no piloto em 27/08/2026: o poller ficou dentro do
`PollRecords` e a ingestão ficou parada por uma hora, com 3,2 milhões de
mensagens pendentes no broker, até alguém olhar.

**O gatilho é uma conjunção de TRÊS fatos, e são três porque dois não bastam.**

| Fato | Por que sozinho não serve |
|---|---|
| os eventos não avançaram na janela | um tópico legitimamente parado produz o MESMO sintoma |
| o broker declara mensagens pendentes | diz que há o que consumir, não de quem é a culpa |
| a fila poller→writer está **vazia** | é este que separa as duas causas |

O terceiro é a **guarda contra backpressure da persistência**, e é o que
impede o watchdog de tratar um problema de disco como se fosse de rede:

- **fila vazia** — o writer não tem o que fazer, logo quem não entrega é o
  poller. Recriar o cliente é o remédio certo.
- **fila com itens** — o writer não está dando conta (disco lento, checkpoint
  longo, storage em modo falho). Recriar a conexão aqui seria **errado e
  destrutivo**: abortaria trabalho em voo por um problema que não é de
  conexão, e a fila voltaria a encher do mesmo jeito.

Um item só já basta para se abster: a asserção é "o writer tem o que fazer",
não "a fila está cheia".

**O lag é perguntado ao BROKER, e não lido da série do `/metrics`.** Aquela é
atualizada pelo *scrape* (`AtualizaLag`), então num processo que ninguém raspa
ela fica velha e a decisão sairia de um número de idade desconhecida. A
pergunta vai pelo cliente **admin**, que é uma conexão distinta da do consumo
— e é exatamente por isso que este watchdog é construível: no incidente, o
admin seguiu respondendo o tempo todo em que o `PollRecords` estava travado.
Se um dia os dois travarem juntos, o watchdog **se abstém**, porque agir sobre
o que não se sabe seria recriar o consumidor por causa de uma consulta que
falhou.

**A recriação passa pelo MESMO `kafka.Open` da subida.** O §49 põe o
checkpoint local como autoritativo e lista quatro situações que recusam subir
a ingestão; um caminho de reconexão que redescobrisse a posição por conta
própria seria um segundo boot, menos testado, que divergiria do primeiro no
dia em que alguém mexesse num deles. Há uma implementação de posicionamento,
não duas.

A contagem é de tentativas **consecutivas** e zera quando o consumidor volta a
andar — sem isso, meses de uptime somariam três blips não relacionados e a
ingestão desistiria para sempre. Esgotadas, o dataset cai no estado que a
ingestão falha já tem: **consultas de pé** (§51), health reportando, e
`ingestao.desistiu` no log.

`nishidb_kafka_watchdog_trips_total` e `nishidb_kafka_watchdog_desistiu` são
duas séries e não uma, porque recriar é **recuperação** — o produto se
consertou e seguiu — enquanto desistir é ingestão morta. Somá-las faria um
alarme disparar igual nos dois casos, e só um deles pede alguém acordado. As
duas nascem com zero na subida: série que só aparece quando há o que mostrar
não se distingue de alvo fora do ar.

> **A lição que veio junto, e ela é sobre instrumentação.** A profundidade da
> fila era gravada pelo laço de poll. Com o poller travado, os dois medidores
> CONGELARAM no último valor: o `/metrics` afirmava "479 registros esperando"
> sobre uma fila que o writer já tinha esvaziado, e a série apontou a
> investigação para o componente errado. Hoje ela sai de um coletor, lida no
> instante do scrape. **Série congelada é pior que série ausente** — ela não
> diz "não sei", afirma um número velho com a cara de um número novo.

Um evento passa por: tokenização sem cópia → resolução de campos contra o
schema → conversão tipada para um arena reutilizado → merge com o estado
anterior → gravação.

Quatro decisões que moldam esse caminho:

**NULL é ausência, nas três camadas.** Um patch com `"campo": null` **remove** o
campo do estado. Não existe "valor nulo" gravado: não no payload, não no índice,
não na query. É o que faz `IS NULL` e "campo ausente" serem a mesma pergunta.

**A sequência decide, não a ordem de chegada — no dataset que declara uma.**
Com `materialization.sequence` no YAML, evento com `_SEQ` menor ou igual ao
gravado é descartado inteiro, e é isso que impede um reprocessamento fora de
ordem de sobrescrever estado novo com estado velho. É propriedade da
**configuração**, não garantia da engine: o stale check exige `schema.Seq != nil`
(`internal/materializer/materializer.go:276`), então o dataset **sem** bloco
`sequence:` aplica tudo na ordem de chegada — inclusive o reprocessamento antigo
sobre estado novo. Quem não declara sequence não tem a proteção deste parágrafo.

**Evento ruim não para a ingestão.** A política de três baldes (§101) separa
*dado ruim* (JSON inválido, tipo inconvertível, key ausente → pula e conta) de
*falha de infra* (disco, dicionário → conta como falha) de *writer em modo
falho* (para tudo; reprocessar é inútil até reabrir). Essa fronteira é
defendida por teste: uma falha de I/O do dicionário já foi classificada como
"evento ruim" e o pipeline pulava em silêncio, para sempre, com `Failed=0`.

**O tipo de um campo não declarado é fixado na primeira observação e
persistido.** Sem isso o mesmo campo teria significados diferentes conforme a
ordem de chegada, e nenhuma query sobre ele seria confiável. O tipo vive num
registro append-only do `fields.dict` e sobrevive ao restart.

Custo medido: **0,0086 alocações por evento** e **132.931 eventos/s** na fase de
carga (146.216 ev/s descontando o tempo de checkpoint).

**Chaves de dentro de um campo `json` viram colunas de verdade.** Um campo
`json` guarda o documento inteiro como um valor, e nada lá dentro é
consultável — coluna `json` não é comparável e o binder recusa `WHERE`. O bloco
`extract` declara caminhos que ganham FieldID, tipo e índice próprios, numa
SEGUNDA passada do parser sobre o documento do pai. O documento original não é
tocado: extrair não é mover.

Essa passada **nunca propaga erro de DADO**, e é a regra que mais importa nela.
O pai pode ser legitimamente um array ou um null — o conversor de `json` aceita
os dois —, o caminho pode trazer outro tipo, o valor pode ser uma lista. Deixar
qualquer uma dessas falhas subir faria o evento inteiro cair como
`INVALID_JSON`, e a política `log-and-skip` o descartaria: as dezenas de
colunas de TOPO iriam junto por causa de uma chave interna. Custo: **zero
alocações por evento**, com gate.

Ela **propaga** exatamente duas coisas, e nenhuma delas é dado: falha de INFRA
ao registrar a coluna ou gravar o tipo (`STORAGE_ERROR`) e o teto de campos sob
`onMaxCols: stop` (`FIELDS_EXHAUSTED`). É a mesma doutrina de três baldes que o
caminho de topo aplica — bug e infra nunca entram no balde de dado ruim —, e ela
não valia aqui: um `continue` seco tratava disco cheio como "a coluna não existe
neste evento", e rebaixava silenciosamente a política `stop` a `ignore`.

**Ausente e removida não são a mesma coisa**, e a distinção é o que impede a
coluna de mentir. No modo `merge`, campo ausente do patch PRESERVA o valor
anterior — então calar sobre uma coluna que o documento corrente contradiz a
deixaria respondendo por um evento passado, com a query devolvendo valor velho
com cara de corrente. Onde o documento contradiz a coluna, a passada emite
REMOÇÃO explícita: valor de tipo divergente, `null` explícito, e documento com
forma errada (array, null, escalar), este último removendo todas as colunas
daquele pai, declaradas e descobertas. Onde a chave apenas SOME de um documento
que continua sendo objeto, a coluna segue o merge e preserva — corrigir isso
custaria ~1.639 entries por evento num documento de 11 chaves sobre ~1.650
colunas, e a diferença entre os dois casos é a frequência: o primeiro é raro (no
piloto, 3.000 de 3.000 documentos são objeto), o segundo acontece em todo
evento.

A coluna derivada fica FORA do `SELECT *`, pelo mesmo mecanismo que esconde os
campos de sistema. Sem isso o `events` do piloto nasceria com centenas de
colunas por linha — a medição contou 545 chaves distintas em 1.000 linhas — e o
custo de fio passaria a ser proporcional ao dicionário em vez de ao que foi
pedido.

**O dicionário tem teto e política.** Ele é append-only e limitado a 65.535
campos por construção (o FieldID é `uint16`). Até o M5, alcançá-lo derrubava o
writer para sempre: o erro subia sem sentinela, era classificado como
`STORAGE_ERROR`, e o offset não avançava — o restart reproduzia o mesmo
estouro. Hoje `schema.maxCols` e `schema.onMaxCols` dão dono à decisão, e o
default (`ignore`) descarta a chave excedente, grava o evento com todas as
outras e conta a perda.

## Storage

![Layout em disco de um dataset, com os nomes exatos de diretório e de arquivo](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/storage_layout.puml)

Segmentos **append-only** para os dados; **mmap** para os índices. É essa
divisão que resolve a tensão do orçamento: os índices crescem com o dataset,
mas como página de **arquivo** — o kernel as descarta sob pressão, e elas não
são memória que o processo possui.

Na rodada de 8M: **386 MiB de RSS de arquivo** contra **70 MiB de RSS anônimo**,
com 2,31 GiB em disco (6,09 GiB na rodada de eventos completos — o corpus de
referência cria a maioria das rows por patch parcial, e elas nascem menores).

O heap Go da ingestão fica em **396 KiB com 8 milhões de rows**, contra **389
KiB com 1 milhão**: 8× de dados, 1,9% de heap a mais. Esta é a medida mais
limpa do desacoplamento — a razão do gate §110 (1,02×) é medida no regime com
os 8 leitores, e inclui os buffers deles.

Índices:

- **primary** — hash aberto com linear probing; a colisão é resolvida lendo a
  key do record, porque hash igual não é key igual;
- **locations** — um `u64` atômico por rowId (segmento, offset, flags). A
  location é publicada **por último**: até ela existir, a row não é visível;
- **sequences** — um `int64` por rowId, para o stale check;
- **índice secundário, por segmento** — um por (segmento, campo), ao lado do
  `.dat`. `index: true` é a única grafia que declara indexação (D5:
  `index: hash` e `index: bitmap` são recusados na subida, exit 3); a
  estrutura por trás é uma lista de postings esparsa que mapeia hash do valor
  → rowIds daquele segmento, em ordem crescente. Densidade deixou de ser
  escolha de motor e virou decisão de codificação, e a distinção baixa/alta
  cardinalidade deixou de existir na configuração.

  Duas propriedades explicam o resto do desenho. Não há fase de **CLEAR**: a
  posting do valor antigo mora no índice do segmento antigo, vira candidato a
  mais e morre quando a compaction ou a retenção aposenta aquele segmento — com
  ela somem a lápide e o falso negativo que a lápide existia para evitar. E o
  índice **não atravessa um restart**: o arquivo mutável do segmento corrente é
  descartado na abertura e reconstruído a partir dos próprios records, o que
  tira o `msync` do índice do checkpoint e elimina a classe inteira de janelas
  de crash do índice secundário. Segmento sem índice — fora da janela, ou
  gravado por um binário anterior — responde ENUMERANDO as linhas dele como
  candidatas, o que é correto pelo mesmo contrato de superconjunto do §44.

A chave do índice inclui o **byte de tipo** — `internal/index/valuekey` é o dono
único dela —, e é por isso que o planner recusa usar índice com valor de tipo
promovido: um `double` jamais casaria postings gravadas como `int64`, e o
resultado seria vazio em silêncio.

### O ciclo de vida do índice por segmento

![Índice por segmento: os quatro estados do segmento e as quatro fontes do Lookup, com a união decidida pelo dado](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/indice_por_segmento.puml)

O desenho substituiu os dois motores globais (`hash` e `bitmap`) porque o
global tinha um modo de falha medido em produção, e a medida está em
[`benchmark/baselines/piloto-pre-ciclo-20260830.md`](../benchmark/baselines/piloto-pre-ciclo-20260830.md):
com 47 M de linhas e o índice em 3,46 GiB, a recópia do rehash — que corre sob
o `wmu` — congelou a ingestão por **18 a 52 minutos**, com 99,16% do CPU numa
única pilha e 64% dentro de um só `atomic.Int32.Add` (o mutex por slot do
`mmap`, tomado uma vez por slot varrido). O episódio era **recorrente e
dobrava** de duração a cada ocorrência, porque a resposta ao fator de carga é
dobrar a tabela. No `.sidx` selado não existe fator de carga, não existe lápide
e não existe tabela para dobrar: o modo de falha não ficou raro, ficou
**inexprimível**.

O que a troca custou e devolveu no mesmo piloto, no mesmo dia, com o dataset de
135 GiB migrado em produção
([`benchmark/baselines/piloto-pos-ciclo-20260830.md`](../benchmark/baselines/piloto-pos-ciclo-20260830.md)):
RSS de 2.812 para **432 MiB** na subida; bytes de índice de 3.716 para
**614 MB**; `poll_blocked` de 32.319 s acumulados para **exatamente 0**; fila e
lag **zerados**. A migração dos segmentos dentro da janela drenou sozinha em
~5 minutos (`pendente` 63 → 0, com a ingestão correndo ao lado), e os 372
segmentos fora da janela **não foram tocados** — é a janela que torna a
migração barata.

O soak de **25 horas** que fechou o aceite
([`docs/aceite-indice-por-segmento.md`](aceite-indice-por-segmento.md)) provou
o regime: `poll_blocked` em **0,0 nas 273 amostras**, nenhum congelamento,
**863 µs/evento** contra 2.360 µs do binário antigo no mesmo dataset (2,7×
menos CPU), a parcela de índice **oscilando entre 102 e 127 MB** com a janela
de 6 h reciclando — parou de crescer, que era o aceite secundário — e heap
anônimo estável em **126 MB** (o resto do RSS é page cache de mmap, que o
kernel governa).

**A janela é barata na migração e cara na consulta, e as duas coisas são a
mesma escolha.** Os 372 segmentos fora da janela não serem tocados é o que
torna a migração de minutos; e é exatamente o que faz uma consulta sobre eles
enumerar. No piloto isso ficou por meses invisível e custou um timeout que
parecia de prazo: com `window: 6h` sobre um histórico de 15 dias, **667 de 733
segmentos** respondiam por enumeração, e uma busca por chave indexada varria 90
milhões de linhas. Subir o prazo de 30 s para 900 s mediu o problema três vezes
sem resolvê-lo; alargar a janela levou a mesma consulta a **206 ms**. O registro
completo, com as contas de custo, está em
[`docs/piloto-events.md`](piloto-events.md).

A regra que sai daí: **a janela do índice é uma declaração sobre o padrão de
CONSULTA, não sobre o volume de dado.** Ela precisa cobrir o intervalo em que
alguém vai buscar por chave; guardar 15 dias e indexar 6 horas é guardar o que
não se consegue procurar. E o número que denuncia o descompasso já é publicado:
`nishidb_index_segmentos{estado="fora"}` contra os demais estados.

**Um writer, muitos leitores — DENTRO do processo.** O writer é serializado por
mutex; as leituras são concorrentes com ele. O `Close` faz **quiesce**: espera
as operações em voo e só então libera o lock do dataset, e todo iterador
devolvido reentra no Store a cada avanço, para que fechar no meio de uma
varredura vire erro e não panic.

**Entre processos o acesso é EXCLUSIVO.** Um `flock(LOCK_EX|LOCK_NB)` sobre o
arquivo **`LOCK`** dentro do diretório do dataset garante um processo por vez:
com um `ingest` em voo, um `query` concorrente sai com exit 3 e a mensagem
nomeia o pid que segura o lock.

É esse flock que decide a arquitetura do `nishidb server`: um consumer de
longa duração num processo SEPARADO tornaria o dataset inconsultável. Por isso
o servidor abre os datasets no MESMO processo em que consome, onde o Store já é
um writer com muitos leitores concorrentes — que é o modelo do §27.

**A outra metade chegou na Fase 6.** Ter consumo e consulta no mesmo processo
era metade do problema; a outra era alcançar esse processo de fora, e é o que o
protocolo remoto do §67 faz. `nishidb query --config` continua barrado pelo
flock — isso é consequência do lock, não defeito a corrigir —, mas o
`POST /v1/query` e o `nishidb query --server` falam com quem já tem o dataset
aberto. Foi assim que o débito 54 fechou; ver [o protocolo
remoto](#o-protocolo-remoto) e a rodada em [`aceite-fase6.md`](aceite-fase6.md).

> **`LOCK` residual não é dataset travado.** O arquivo é criado na primeira
> abertura e **nunca é removido** — nem no `Close`, nem no crash: apagá-lo no
> release abriria a janela em que outro processo já criou e travou o *seu*
> `LOCK` e teria a entrada de diretório removida por baixo. O pid gravado
> dentro dele é **só diagnóstico**, e fica lá depois que o processo morre. Quem
> encontrar um `LOCK` com pid inexistente não precisa fazer nada: o lock de
> verdade é o `flock`, e o kernel o libera sozinho quando o processo termina —
> a próxima abertura simplesmente reescreve o pid.

> **Restrição de deployment.** O `flock` trava a **descrição de abertura** do
> arquivo `LOCK` — o que dois processos precisam enxergar em comum é o inode
> **desse arquivo**, não o do diretório. Ele **não é confiável em NFS**, nem em
> alguns FUSE, CIFS e overlayfs. No NFS o comportamento depende da montagem: a
> opção **`local_lock=flock` (ou `all`) torna o `flock` LOCAL a cada cliente** —
> nesse caso dois hosts travam cada um o seu, e os dois acham que ganharam. Sem
> ela o lock é submetido ao servidor, o que costuma funcionar, mas depende de
> versão e implementação — não é garantia sobre a qual valha apostar um dataset.
> Dois writers no mesmo dataset significam dois overlays de
> location, dois contadores de rowId e dois checkpoints — corrupção garantida.
> **Mantenha o diretório do dataset em disco local.**

## Compaction

![Compaction de segmento: eleição, varredura streaming, republicação condicional sob o mutex do writer, os quatro passos da aposentadoria e os dois pontos de reparo do Abre](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/compaction_flow.puml)

Append-only sem compaction é disco que cresce para sempre: todo update apenda um
record novo e deixa o anterior como garbage. O §86 projeta **30-43 GB escritos
por dia** contra 5,6-8 GB de estado vivo a 500 ev/s — dois a três dias de disco,
e nada no M1 devolvia esse espaço.

**O desenho é copy-forward, e não "arquivo novo".** A varredura re-apenda o
record vivo pelo caminho **normal** de escrita, no segmento corrente. A
alternativa — montar um arquivo novo com ID próprio — esbarra em três
restrições que já existiam: o `checkHeader` amarra o `segmentID` ao nome do
arquivo, o `openNew` usa `O_EXCL`, e os IDs vêm de `secID+1` no writer. O
copy-forward faz as três desaparecerem e reusa rotação, tally e durabilidade já
provadas.

**Vivo é "a location ainda nomeia este record"** (§55), e não "a row é visível".
A distinção decide um caso real: um record **tombstoned** precisa ser copiado
também, porque a location aponta para ele — não copiá-lo deixaria a location
pendurada num arquivo que a aposentadoria vai remover, e o `Open` seguinte
recusaria o dataset inteiro. Recuperar tombstone exige limpar location e
primary, que é trabalho do TTL.

**A verificação e o `Append` acontecem na MESMA seção crítica do writer.** É a
corretude inteira do mecanismo: entre ler a location e apendar, a ingestão pode
ter gravado um update, e fora do lock a cópia do payload **antigo** seria
publicada **depois** do novo. O dataset voltaria no tempo sem erro, sem log e
com o CRC batendo — nem a recovery distinguiria o estrago de uma escrita
legítima. É por isso que a compaction não chama `Put`.

**O tally do segmento esvaziado é zerado por AUTORIDADE da varredura**, e não
deduzido dos decrementos. O tally é limite superior por construção (débitos 38 e
45): o record órfão que o replay do tail deixa não é nomeado por location
nenhuma, então nenhum decremento o encontra e o candidato jamais chegaria a zero
por subtração. A varredura viu **todos** os records e é a única leitura exata do
segmento que existe no produto. Varredura **cancelada ou incompleta não tem essa
autoridade** — e um segmento selado cujo prefixo válido não cobre o arquivo
inteiro é corrupção, recusada em vez de compactada: zerar ali transformaria a
perda de um record na perda do segmento.

**A aposentadoria é em quatro passos, e a ordem é a garantia.** Checkpoint (as
republicações viram duráveis), rename para nome **não canônico** — que o
`ListSegments` já ignorava de propósito, de modo que o segmento sai do dataset
sem que um byte saia do disco —, checkpoint (o manifest deixa de nomeá-lo) e só
então unlink. Inverter os dois últimos é a troca que perde dado: um unlink com o
manifest ainda nomeando o segmento produz "listado e ausente", que o `Open`
trata como `ErrDatasetCorrupt`.

**O unlink sozinho não devolve os blocos, e isso custou uma errata.** O
`os.Remove` do quarto passo remove o **nome**; enquanto qualquer descritor
mantiver o inode aberto, os blocos seguem alocados. E o cache de descritores do
`segment.Reader` não tem despejo — o descritor que a própria varredura abriu
para ler o segmento ficaria aberto pela vida do processo. Por isso a
aposentadoria **fecha o descritor** (`Reader.CloseSegment`) antes do unlink.
Antes disso, a compaction devolvia disco ao *listing* do diretório e nada ao
dispositivo, e os três instrumentos do projeto — `dirBytes`, `diskOfSegmentos`
e o `Stats.StorageBytes` — somam o listing e não viam a diferença (débito 78, e
a errata do [aceite do M4](aceite-m4.md)).

Fechar o descritor **remove um acidente que a leitura usava**: com o inode ainda
aberto, um `pread` sobre o arquivo já removido devolvia os bytes certos, com CRC
válido. Quem põe algo no lugar é o `aposMu`, o terceiro lock do `Store`, que
guarda a janela entre **resolver uma location e ler os bytes que ela nomeia**. A
seção crítica da aposentadoria é só fechar o descritor e apagar o arquivo — os
dois checkpoints e a poda ficam de fora, porque tomam o `wmu`, e a invariante é
que **`aposMu` é sempre o lock mais interno**: invertida, a ordem seria
`wmu→aposMu` de um lado e `aposMu→wmu` do outro, que é deadlock. O rename fica
de fora por outra razão: um descritor já aberto sobrevive a ele, porque o rename
move o nome e não o inode.

Os dois intervalos entre esses passos são **janelas de crash**, e o `Open` as
repara pelo manifest: segmento **listado** com o canônico ausente é
**restaurado**; segmento **não listado** é **removido**. O reparo roda antes de
qualquer conclusão sobre o conjunto de segmentos — conferir o conjunto primeiro
faria o `Open` condenar como corrompido um dataset íntegro.

**Ela é background, não comando.** A goroutine sobe com o servidor (depois do
HTTP, antes do consumer, que é a ordem do §51 aplicada a mais um consumidor de
disco), faz a primeira passada imediatamente e dorme entre passadas. O `Close`
cancela e **aguarda** antes de tomar o lock de vida — segurar o quiesce pela
varredura inteira deixaria o encerramento preso por minutos. E ela **para de
vez** na primeira falha: o erro que chega ali é o modo falho sticky do writer ou
a corrupção recusada, e nos dois casos a passada seguinte reencontraria o mesmo
estado. Quem observa isso é `nishidb_compaction_running` — o `compaction.parou`
no log fala com quem já foi ler o log.

## Recovery

![Abertura do dataset: criação da estrutura, flock, validações fatais e recovery pelo tail](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/recovery_flow.puml)

O manifest marca um ponto consistente; o que veio depois é reconstruído lendo o
tail dos segmentos e reindexando. Um rasgo de crash só pode estar no fim, e é
truncado ali.

Onde a recovery **não** adivinha, ela recusa: manifest ausente com dados,
manifest ilegível, segmento sumido ou buraco na cadeia são erro fatal com
mensagem, não reconstrução heurística. Toda alternativa (truncar, pular,
recriar) descarta dado durável em silêncio.

## Consulta

![Fluxo de consulta: o consumidor puxa o RowIterator, copia ou serializa cada Row e faz o Flush final](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/query_flow.puml)

Cinco camadas: lexer → parser → binder → planner → executor, com o encoder
NDJSON na saída.

**O plano escolhe apenas a fonte de candidatos.** O executor **sempre** reavalia
o WHERE completo contra o estado corrente — inclusive no point lookup. O índice
devolve um superset por construção (posting órfão é estado tolerado), então
confiar nele produziria linhas que não satisfazem o filtro. Falso positivo o
executor filtra; falso negativo ninguém recupera.

**A prioridade do planner é fixa** (primary → índice → união → full scan), sem
otimizador por custo. Toda dúvida degrada para full scan, que é sempre correto —
só mais lento. Há **um** motor de índice secundário, e o `IndexIntersectPlan` e o
`IndexUnionPlan` fundem vários iteradores dele: é por isso que o `Lookup` devolve
rowIds em ordem **estritamente crescente** — desordenado, a interseção k-way
pararia cedo e o resultado sairia incompleto sem erro.

**Nada do resultado é materializado.** O predicado é compilado uma vez para um
programa linear com saltos, os campos são coletados por merge linear numa
passada, e a linha é serializada direto para o writer. O que está medido é
**zero alocação por linha**: o gate `TestScanAllocsIndependenteDoNumeroDeRows`
roda a mesma query sobre **1.000 e 8.000 rows** e exige **igualdade exata** entre
as duas contagens — a diferença, se houvesse, seria alocação por row.

O custo **fixo** de uma query é outro assunto, e sobre ele circulam dois números
de naturezas diferentes — confundi-los já pôs "38 alocações por query" neste
documento como se fosse o custo de qualquer consulta:

- **38 é TETO**, não retrato. É o custo fixo de abertura da query do gate
  (`SELECT id, nome, n FROM linhas WHERE nome = 'linha' AND n >= 0`), colado sem
  folga para acusar crescimento. Medido nesta máquina: **38, 38, 38** em três
  execuções de `go test ./internal/query/ -run TestScanAllocsIndependenteDoNumeroDeRows -v -count=1`.
- **O custo fixo varia com a query**, e é o benchmark que o mede: **30
  allocs/op** no full scan de 1.000 rows, **20** no full scan sem WHERE e **29**
  no point lookup — três execuções de `go test ./internal/query/ -run XXX -bench
  'BenchmarkFullScan1k|BenchmarkFullScanSemWhere1k|BenchmarkPointLookup'
  -benchtime 300x -count=3`, com as três repetições idênticas em cada
  benchmark e iguais à baseline de 2026-08-13
  (`benchmark/baselines/query-20260813.txt`).

Ou seja: por query o custo é da ordem de algumas dezenas de alocações e depende
da forma da consulta; **por linha é zero**, e é essa a propriedade gateada.

Latências medidas com 8M rows, com as consultas concorrentes a uma **tentativa**
de escrita — não a escrita sustentada (ver a ressalva logo abaixo da tabela).
A rodada é de 14/08, **anterior ao índice por segmento**: as linhas "bitmap" e
"interseção" atravessavam os motores globais que o subsistema 1 substituiu, e
valem como registro daquele motor contra os tetos da spec. O caminho indexado
novo tem medida própria em
[`benchmark/baselines/segindex-20260830.txt`](../benchmark/baselines/segindex-20260830.txt)
(corpus micro, não comparável ao de 8M): mínimo de **272 µs** na consulta
indexada de baixa cardinalidade com LIMIT 100, contra o teto de 50 ms do §90.

| Rota | p95 (SQL completo) | p95 (Store direto) | Teto da spec |
|---|---:|---:|---:|
| point lookup | **18 µs** | 6 µs | 10 ms (§89) |
| bitmap + LIMIT 100 | **522 µs** | 434 µs | 50 ms (§90) |
| interseção + LIMIT 100 | 753 µs | — | 50 ms |
| união + LIMIT 100 | 533 µs | — | 50 ms |
| full scan + LIMIT 100 | 4,3 ms | — | — |
| varredura completa (8M) | 17,0 s | — | — |

A diferença entre as duas colunas — cerca de 12 µs no point lookup — é o custo
de parse, bind, plan e serialização NDJSON.

> **Estas latências não dimensionam carga mista.** Na fase STEADY da rodada, dos
> **30.000 eventos oferecidos apenas 50 viraram escrita** — os outros 29.950
> foram descartados por stale (§25), porque o gerador do STEADY reinicia as
> sequências. O que a tabela mede é consulta concorrente com **tentativa** de
> escrita; escrita sustentada disputando com consulta **não foi medida** e é o
> **débito 3** ([`debitos.md`](debitos.md)), dono M2. Quem dimensionar capacidade
> de leitura+escrita simultâneas a partir daqui está extrapolando. O flanco de
> escrita que existe hoje é a rodada `ingest-8m-20260813`, sem SQL.

**O `--limit` do CLI reescreve o texto da query, e isso mudou de comportamento
na errata do M1.** A flag corta a cláusula `LIMIT` que a query já tiver e anexa
a sua, porque a v1 não tem serializador de AST — não há como reescrever o plano.
Quem separa cláusula de literal passou a ser o **lexer de verdade**, o mesmo que
o `Execute` usa, em vez de uma varredura textual própria que só conhecia a aspa
simples.

A consequência que o operador precisa saber: **query que não tokeniza deixou de
ser cortada**. Antes, um texto malformado que tivesse um `LIMIT` legítimo antes
do trecho ilegível — `SELECT id FROM t LIMIT 3 AND status = 'x`, com a aspa
aberta e nunca fechada — era cortado nesse `LIMIT`, e o comando executava
`SELECT id FROM t LIMIT <n>`: uma query **diferente da que o usuário escreveu**,
que emitia linhas e não acusava nada. Hoje o `--limit` é apenas anexado, e a
query sai com erro de sintaxe na posição real (`ExitQuery`, código 2 — medido, e
o mesmo código que a query sairia **sem** a flag; a flag deixou de alterar o
veredito). É perda de conveniência num caso que só existia por acidente, em
troca de o comando nunca mais executar texto que ninguém escreveu.

Essa reescrita textual tem prazo: no M2 o `--limit` deve virar parâmetro do
plano, já que o binder conhece o `LIMIT` da AST (débito 46).

### O snapshot de leitura: uma consulta vê um instante do banco

Uma consulta abre um `Snapshot` e o devolve no `Close`. Enquanto ele existe, a
consulta enxerga o dataset como ele estava naquele instante: o mesmo conjunto
de linhas do começo ao fim, cada uma na versão daquele momento, com o relógio
da expiração parado. A ingestão, a compaction e a retenção continuam correndo —
o que elas deixam de fazer é apagar o passado que a consulta ainda alcança.

![Snapshot de leitura](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/main/docs/diagrams/snapshot_isolamento.puml)

O snapshot congela quatro coisas, e nenhuma substitui as outras:

| | O que congela | O que quebra sem ele |
|---|---|---|
| `Epoch` | a versão de cada linha | o resultado mistura estados: as linhas alcançadas antes de uma atualização saem velhas, as de depois saem novas |
| `Instant` | o relógio da expiração | uma linha viva quando a consulta abriu some do MEIO do resultado |
| `LimitRowID` | o teto de rowId | nada hoje: as fontes do índice por segmento já fixam o seu na construção. O campo torna o teto explícito e comum aos quatro planos |
| `BaseRowID` | o piso | a purga do histórico abre buraco por baixo da varredura em curso |

Ele é **tipo de valor e viaja em parâmetro**, nunca em campo do `Store`: dois
leitores concorrentes têm universos diferentes, e um campo os fundiria num só.

**O isolamento não copia dado.** Os segmentos são append-only, então a versão
anterior de uma linha continua fisicamente onde estava até a compaction
aposentá-la — o que falta para alcançá-la é só o endereço, e o writer já o tem
na mão no passo do `applyRecord` que desconta a liveness do segmento antigo.
`internal/storage/versions` guarda `(rowId, location anterior, época)`: 24 bytes
por atualização, e **zero enquanto nenhuma consulta está aberta**. Sem leitor
vivo o journal não recebe uma entrada sequer, e a ingestão paga apenas a
comparação de um atômico.

Três decisões separam o que parece igual:

- **Put e `Expira` versionam; o copy-forward da compaction não.** Ele também
  publica location nova com `hadPrevious`, mas só move bytes: o conteúdo é
  idêntico e o leitor deve seguir a location nova. Se versionasse, cada passada
  criaria versões apontando para o segmento que ela está aposentando, e o pin
  travaria a própria passada que as gerou.
- **A busca devolve a versão de menor época maior que a do leitor.** Com uma
  atualização só, "a primeira que achar" acerta por acidente; com duas, só a
  regra certa acerta.
- **A expiração da versão entregue é derivada do payload** — os índices
  descrevem a versão corrente. Mas só quando o payload declara o instante: a
  linha legada não tem o campo, e o instante dela é o da carência, semeado pelo
  coletor, que não existe em record algum.

**O que o snapshot obriga os outros a respeitar.** A compaction não elege
segmento com versão retida (a recusa é na eleição, não na aposentadoria: varrer
para descobrir no fim que ele não pode sair seria pagar o I/O por nada), e a
descoberta oportunista da expiração decide pelo instante do snapshot mais
antigo — com o relógio de agora, ela apagava o payload que o relógio congelado
tinha acabado de preservar. A purga do histórico para no piso da consulta mais
antiga, e o que ela deixou de avançar sai no log: uma retenção segurada e uma
retenção sem trabalho são estados diferentes e não podem publicar o mesmo
silêncio.

**O teto.** A memória retida é proporcional a (taxa de escrita × duração da
consulta mais longa viva), e `runtime.query.snapshotMaxBytes` a limita — por
padrão, um oitavo do `memoryTarget`, no máximo 64 MiB. Estourado, é a consulta
mais antiga que é invalidada, nunca a ingestão que é barrada: o dataset existe
para ingerir, e uma consulta recusada com `SNAPSHOT_TOO_OLD` é um resultado que
o cliente sabe tratar. Degradar em silêncio para a versão corrente seria o
único desfecho inaceitável — as duas respostas são indistinguíveis no wire e só
uma é verdadeira.

**O que ele custa, medido.** A ingestão não regrediu: `BenchmarkApplyIngestReal`
sai de 32,3 µs para 28,5 µs por evento (dentro do ruído da bancada), com os
mesmos 0 allocs/op — sem consulta aberta, o journal não recebe nada. A leitura
paga 3,5% no full scan: `BenchmarkFullScan1k` sai de 634,5 µs para 656,9 µs,
medido alternando as duas árvores no mesmo diretório de bancada com o dataset
recriado a cada troca. São ~22 ns por linha varrida, e a causa não foi isolada
— está registrada como débito 106, com o que já foi descartado por medição.

**O que fica de fora, e é semântica pretendida:** o journal é heap e morre com
o processo, então consulta não atravessa reinício; a tabela histórica não
versiona (linhas só nascem); e o `_meta` ignora o snapshot por já ser um
retrato do materializer.

### `ORDER BY`: um operador, três modos

`ORDER BY` é o único operador **bloqueante** do motor: a fonte é drenada por
inteiro antes da primeira linha sair, porque a linha certa pode ser a última
lida. Isso inverte o papel do `LIMIT`, que deixa de ser teto de *trabalho* e
passa a ser teto de *memória*.

O operador é **um só** (`internal/query/executor/sort.go`), com três modos que
compartilham a mesma função de comparação. O compartilhamento é requisito, não
economia: duas regras de ordenação divergiriam no primeiro empate, e o resultado
passaria a depender de qual modo foi escolhido.

| Modo | Quando | Memória |
|---|---|---|
| **completo** | default | O(resultado): arena que cresce por append |
| **transbordo** | o orçamento de memória estourou e há lugar em disco | O(lote) + runs em `<dataset>/spill/`, merge k-way na saída |
| **top-N** | `OFFSET + limite efetivo` ≤ **4.096**, decidido no `Open` | O(N): um heap limitado |

**O top-N é um heap de máximos segundo a ordem PEDIDA** — a raiz é a *pior* das
retidas, e é ela que sai quando entra alguém melhor. Com `ORDER BY x ASC LIMIT
10`, a raiz é o maior dos dez menores. O `OFFSET` entra no **tamanho** do heap,
não é aplicado antes: as linhas puladas também precisam ser as certas.

O ponto que não pode ser "simplificado" é o **reuso do buffer da entrada
despejada**. O caminho completo copia tudo para um arena único porque nada é
removido dele; no top-N a entrada é substituída *individualmente*, e o espaço
dela fica no meio do arena. Escrever a linha nova no fim faria a memória crescer
com o número de linhas **varridas** em vez de com N — o modo deixaria de ser
limitado, que é a única razão de ele existir. Por isso cada entrada tem o seu
buffer, reusado na substituição, e o gate
`TestTopNReaproveitaOBufferDaEntradaDespejada` trava isso por **igualdade de
alocações** entre dois tamanhos de corpus, no formato de dado em que toda linha
é aceita.

O teto de 4.096 é **medido**, não escolhido
(`benchmark/baselines/topn-20260826.txt`, `-count=6` com benchstat). Sobre 100
mil linhas o heap ganha em toda a faixa testada, inclusive em N=16.384 — então
não é lá que o teto se decide. Quem o decide é o corpus **curto**: com 20 mil
linhas varridas, N=4.096 (20% do varrido) ainda economiza 35% de tempo no pior
formato, e N=16.384 (82% do varrido) não tem diferença estatística. O número de
linhas varridas é desconhecido no `Open`, então o teto tem de ser fixo e tem de
ser o maior que ainda ganha quando o dataset é pequeno.

O teto de **bytes** (`Options.SortMemory`) continua valendo dentro do modo
rápido, e a saída dele é **degradar**, não recusar: as N linhas retidas voltam
para o caminho completo, que transborda. A degradação é correta e não aproximada
— nenhuma linha fora das N melhores de um prefixo pode estar nas N melhores do
total, então o que o heap já descartou não muda a resposta. Provas:
`TestTopNDegradaEmVezDeRecusarQuandoOOrcamentoEstoura` (resultado idêntico ao do
caminho completo) e `TestTopNDegradadoRecusaQuandoNaoHaOndeTransbordar` (sem
disco, a degradação termina onde o caminho completo termina).

Os contadores `TopN` e `TopNDegradado` (`executor.Metrics`) dizem qual modo cada
consulta usou. Eles existem porque sem eles a escolha é invisível de fora, e um
diferencial entre os dois caminhos passaria por **vacuidade** — comparando um
caminho consigo mesmo — sem que nada acusasse.

### Introspecção pelo próprio SQL

`_meta.tables` responde "que tabelas existem" — a base como `TABLE` no schema
declarado, os contadores como `VIEW` no mesmo schema, e as tabelas de meta como
`VIEW` no `_meta`:

```sql
SELECT id, name, schema, kind, columns FROM _meta.tables ORDER BY id
```

Duas decisões a sustentam.

**Ela se inclui.** Perguntar "que tabelas existem" e não ver a própria pergunta
listada deixaria o catálogo incompleto na primeira linha. O que garante isso é
o momento da montagem: as linhas saem do `Engine.Catalog` **a cada consulta**,
não do catálogo congelado no registro — por isso a ordem em que ela é
registrada, em relação à `_meta.counters` e à `_meta.kafka`, não muda a
resposta. O congelamento existe, mas é POR CONSULTA: o snapshot fixa o catálogo
de T0, e a varredura não vê tabela aparecer no meio.

**A key é o endereço do atalho, não o nome.** `id` é a forma que o `FROM`
aceita sem nomear schema (`_counters.por_olt`, `_meta.tables`, ou só o nome na
base) — o par `schema.name`, que as outras duas colunas dão, é o endereço
completo. O nome sozinho não serve
de key porque não é único: a compilação da spec recusa quatro colisões no nome
de um contador — o prefixo `_`, o nome do próprio dataset, o nome da tabela irmã
do histórico e o nome repetido entre dois contadores —, e nenhuma delas alcança
o nome de uma tabela de meta. Um
contador `tables` é aceito e teria o mesmo `name` que esta tabela; com duas
linhas de mesma key, o point lookup devolveria uma e a varredura devolveria
duas — o motor discordando de si mesmo conforme o plano.

Ela mora em `internal/query`, e não em `internal/query/meta` ao lado da
`fields`, porque o conteúdo dela é o `Catalog` e o pacote `meta` não pode
importar `query`; e não num pacote à parte como `internal/counters` e
`internal/metakafka`, porque não costura o motor a subsistema nenhum.

### O mesmo catálogo no `/v1/metadata`

O `/v1/metadata` publica, por dataset, o `schema` declarado e o campo `tables`
— a enumeração inteira, cada linha com `name`, `schema`, `namespace`, `kind`,
`description` e as colunas já traduzidas para o wire pelo mesmo `columnMeta` que
serve as colunas da base (o código `jdbc` é calculado no servidor; o driver não
sabe derivá-lo). É o canal de quem monta ÁRVORE: um cliente SQL não escreve
`SELECT ... FROM _meta.tables` sozinho.

**Os dois canais saem do mesmo `Engine.Catalog`**, e isso é a decisão, não um
detalhe: duas derivações da mesma enumeração divergem, e a divergência aparece
como uma ferramenta que discorda do SQL sem nada estar quebrado — a mesma classe
do painel que discorda do JSON no `/v1/stats`. A prova é
`TestMetadataEMetaTablesNaoPodemDiscordarDoCatalogo`, que compara as duas saídas
linha a linha, no dataset base e na irmã histórica (as listas são diferentes, e
um oráculo de um dataset só passaria com a irmã servindo o catálogo do base).

Os campos são **aditivos**, e o que o teste de aditividade protege é o lado que
se perde por descuido — mover informação para o bloco novo: `columns` continua
sendo a lista completa da tabela base, e `name` continua sendo o nome do
dataset, e não o par com o schema
(`TestOClienteAnteriorAosCamposNovosContinuaLendoOMetadata`). O `namespace` viaja
na linha porque não é reconstruível a partir de `(schema, name)`, e para as
tabelas do `_meta` ele é o único endereço que existe.

O que **não** foi resolvido aqui é o clamp do driver: `maxRows` e `timeoutMs`
são por dataset, o servidor aplica os do dataset LIDO e o `NishiConnection`
ainda clampa pelos do dataset da conexão. O mapa que faltava — qual dataset é
dono de cada schema — passa a existir com estes campos; usá-lo é trabalho do
driver, registrado no débito 118.

### O `FROM` qualificado pelo schema

O `FROM` tem três formas, e cada uma resolve o dataset alvo de um jeito:
`FROM <nome>` é a base do dataset da requisição; `FROM _ns.<nome>` é o ATALHO,
que significa "a tabela virtual do dataset da requisição" e não nomeia dataset
nenhum; `FROM <schema>.<nome>` é o **endereço completo** — o par endereça uma
tabela em qualquer dataset, e a resolve venha a requisição pelo handle que
vier. É a forma que um cliente SQL gera sozinho a partir do catálogo, e o spike
do DBeaver confirmou que é a que ele monta.

**A unicidade do par é sustentada pela CONFIGURAÇÃO, e não pela forma do
catálogo.** São três recusas, todas na subida: dois datasets não declaram o
mesmo `schemaName` (`config.ValidateSchemaUniqueness`), e um contador não se
chama como o próprio dataset nem como a tabela irmã do histórico
(`schema.validateCounters`) — a irmã herda o schemaName do base, e o nome dela
sai de um sufixo configurável, de modo que `name: events` + `history:` +
`counters: [{name: events_history}]` publicava o mesmo par duas vezes num
arquivo só, e era aceito. O motor continua recusando o par ambíguo na
resolução, mas isso é **rede**: um YAML aceito não chega lá, e quem pode chegar
é um `RegisterVirtual` que não passa por nenhuma das três
(`TestCountersRecusaNomeDaTabelaHistorica`).

**A resolução lê a ENUMERAÇÃO do catálogo, não o catálogo montado.** O
`Catalog` público é a enumeração — schema, nome, namespace, kind — mais as
colunas de cada tabela; a resolução usa só a enumeração. A separação é medida,
não estética: montar as colunas chama `FieldsForMetadata`, que aloca um slice
com todos os campos e o ordena, e a resolução as montava para jogá-las fora.
Num dataset de 4.000 colunas a forma qualificada custava ~479 µs e ~35 KiB de
lixo contra ~4 µs e ~1,5 KiB da simples — 113x —, e é a qualificada que o
DBeaver gera sozinho. Depois da separação são ~1,5x nos três tamanhos medidos
(40, 400 e 4.000 campos) e 2.216 B por consulta, constante no tamanho do
dicionário (`BenchmarkExecuteFromQualified`,
`TestResolucaoQualificadaNaoAlocaPorCampoDoDataset`). Origem dos números, com o
ambiente da rodada e a saída crua: [`aceite-schema-declarado.md`](aceite-schema-declarado.md),
seção "A bancada", e
[`benchmark/baselines/from-qualified-20260911.txt`](../benchmark/baselines/from-qualified-20260911.txt).

**Quem resolve o par é a enumeração do `Engine.Catalog`, não uma varredura
própria do roteamento.** A enumeração já é a única derivação de "que tabelas
cada dataset publica, em que schema cada uma aparece e por qual namespace cada uma abre", e
o godoc dele já prometia que toda tabela listada é aberta pelo `Execute`. Uma
segunda varredura no roteamento faria dessa promessa uma coincidência: na
primeira tabela que entrasse só num dos dois lados, a ferramenta mostraria uma
tabela que a consulta recusa — ou o contrário — sem nada estar quebrado. Com a
resolução SAINDO do catálogo, a promessa vira mecanismo.

**A regra "o dataset do argumento tem de bater com o do `FROM`" continua
valendo, e roda depois da resolução.** Ela existe contra endereço AMBÍGUO —
`FROM y` com o handle `x` são dois nomes e nenhum jeito de saber qual o usuário
quis. O qualificado não tem essa ambiguidade, então o handle deixa de ser o
alvo e passa a ser só o default das formas não qualificadas, que é o papel do
"current schema" numa conexão SQL. Não há fronteira de autorização a
atravessar: os papéis do servidor são READ e ADMIN **por rota**, não por
dataset, e quem pode consultar já podia mandar outro dataset no campo da
requisição. Provas: `TestSchemaQualificadoResolveODatasetCerto` (a view e a
base do OUTRO dataset abrem pelo schema) e
`TestAtalhoSemSchemaUsaODatasetDaRequisicao` (o atalho não virou busca global).

**Os limites de consulta são do dataset LIDO, não do handle.** `query.maxRows`,
`query.timeout` e `query.maxTimeout` são declarados por dataset no YAML, e com
o `FROM` qualificado o handle da requisição e o dataset lido podem ser
diferentes. O servidor resolve o alvo antes de admitir a consulta
(`Engine.TargetDataset`, que reusa o mesmo parser e a mesma resolução do
`Execute`) e aplica os limites do dono — sem isso, um dataset com teto alto
seria a porta para ler, sob o teto dele, outro cujo YAML declara teto baixo
(`TestOsLimitesSaoDoDatasetLidoENaoDoHandle`). O que continua saindo do handle
é a identificação: o label das métricas, o dataset do log e o registro de
queries em voo.

**Ler o qualificador é também recusar o que não é schema.** Até aqui
`FROM qualquercoisa.tabela` era aceito e idêntico a `FROM tabela` — o prefixo
era descartado. Agora qualificador que não é schema declarado é recusado
listando os que existem, e schema certo com tabela inexistente é recusado
listando as tabelas daquele schema. As duas são erro de quem escreveu a query e
saem por `binder.ErrorAt`, como as recusas de namespace
(`TestQualificadorQueNaoEhSchemaEhRecusado`,
`TestSchemaDesconhecidoListaOsExistentes`,
`TestSchemaConhecidoSemATabelaListaAsDoSchema`).

`_meta.fields` responde "o que este dataset tem" — nome, tipo, se é
autodescoberto, índice declarado, índice físico, campo de origem e a
**densidade** de cada coluna derivada — como uma tabela qualquer:

```sql
SELECT name, type, occurrences, typeMismatches, coercions FROM _meta.fields
WHERE parent = 'persistent' AND typeMismatches > 0
```

O mesmo namespace lista os contadores mantidos na escrita em
`_meta.counters` (ver "Contadores mantidos na escrita"), e os totais de cada
um saem em `_counters.<nome>`.

![_meta.kafka: uma consulta, um instante — o retrato colhido na abertura, a marca d'água buscada só quando vencida, e as linhas lidas do retrato](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/meta_kafka.puml)

**`_meta.kafka` responde o estado da fonte, uma linha por partição** — offset
aplicado, offset durável, fim do log, lag, mais tópico, grupo, bootstrap,
estado da ingestão, profundidade da fila, os contadores desde a subida e o
watchdog:

```sql
SELECT partition, appliedOffset, highWatermark, lag, state FROM _meta.kafka ORDER BY partition
```

Três decisões a sustentam.

**Duas classes de coluna, uma tabela.** As de PARTIÇÃO — `partition`,
`appliedOffset`, `committedOffset`, `highWatermark`, `lag`, `markAt` — são o
que só existe por partição; as de DATASET — identidade, estado, fila,
desfecho, watchdog — repetem em toda linha, porque uma linha por partição que
não trouxesse o estado do dataset obrigaria uma segunda consulta para
responder "e a fila, está cheia?". Quando não há partição nenhuma — dataset
sem Kafka, `enabled: false`, consumidor ainda sem atribuição — a tabela
responde UMA linha de resumo, com `id = 'source'` e as colunas de partição
NULL. `SELECT *` nunca é erro: "não consome" é uma resposta.

**NULL é ausência, e ausência não é zero.** Sem consumidor, fila e desfecho
são NULL — "não há o que medir" e "medi e deu zero" são afirmações diferentes,
e é a mesma doutrina do coletor da fila. `highWatermark` e `lag` são NULL
quando a marca d'água do broker venceu ou a partição não foi respondida: um
zero ali diria "está em dia" a quem perdeu o broker. `watchdogTrips` é NULL
onde não há goroutine de ingestão, porque "não há watchdog" não é "nunca
recriou".

**Um instante só por consulta.** Aqui a tabela diverge do `_counters`, que
congela o conjunto e lê os totais no presente. As colunas desta são uma
medição com carimbo — `markAt` diz quando o broker respondeu, e
`lag == highWatermark - appliedOffset` é invariante dentro de uma leitura —,
então o retrato inteiro é colhido na abertura da consulta. Lendo no presente
linha a linha, um resultado de três partições carregaria três `markAt` e um lag
que não fecha com o HWM da mesma linha, que é o painel discordando de si mesmo
(`TestMetaKafkaMostraUmInstanteSo`).

Os números saem das MESMAS estruturas que alimentam o `/metrics` e o
`/v1/stats` — `TestOInstantaneoEAsSeriesDaPosicaoConcordam` compara os três
por partição —, e a marca d'água só é buscada no broker quando venceu, com o
mesmo prazo do scrape. O ponteiro do consumidor troca a cada recriação do
watchdog, então a tabela o resolve por closure a cada consulta: capturá-lo no
registro descreveria para sempre um consumidor morto.

**Recusa de roteamento é erro do cliente, e por isso é tipada.** Namespace
inexistente, namespace sem tabela nenhuma e tabela inexistente saem por
`binder.ErrorAt` com a posição do nome depois do ponto, o que as põe na família
`binder.ErrBind` — o servidor as classifica como `BIND`/400 pelo `case` que já
existia, o driver JDBC as traduz para o SQLState 42000 e o cliente Python para
`ProgrammingError`, sem código novo no wire. Enquanto eram `fmt.Errorf` cru
caíam no `default` da classificação: um `SELECT a FROM _meta.nada` respondia
**500 / `INTERNAL`** e subia `nishidb_query_errors{categoria="INTERNAL"}`, que
foi o que o piloto viu em 05/09/2026. O teste que cobria o caso só exigia
`status != 200`, e por isso passou verde com a resposta errada — hoje ele
afirma o código (`TestCountersTabelaDesconhecidaNomeiaAsQueExistem`), ao lado
de `TestTabelaVirtualDesconhecidaSaiComoErroDoCliente` e
`TestRecusaDeRoteamentoVirtualEhErroDeBind`.

**`occurrences` e `typeMismatches` existem porque o contador por DATASET é
alarme sem endereço.** Ele diz "2.085.138 valores perdidos" e não diz qual das
~1.700 colunas respondeu por isso; a tabela que atribuía esse número só existiu
uma vez, calculada por um script descartável sobre uma amostra de 20.000
documentos. Com as duas colunas a mesma pergunta é respondida por SQL, sobre o
dataset inteiro.

**`coercions` é a terceira, e existe porque a coerção de tipo CEGA as outras
duas.** Enquanto a coluna `string` recusava número, um valor divergente subia em
`typeMismatches` e apontava a coluna; a partir do momento em que o número entra,
aquele contador vai a **zero** no mesmo cenário — a inconsistência da origem some
do painel sem ter sumido do dado. `coercions` conta a ocorrência que chegou
**noutra forma JSON e ainda assim entrou**: número ou boolean numa coluna de
texto, string numérica numa coluna `int64`/`double` (esta última sempre existiu e
nunca foi contada). É ela que torna MEDÍVEL a ressalva da referência de tipos —
`"07"` e `"7"` na mesma coluna são duas strings distintas
(`TestCoercoesSaemPorSQLSeparadasDasDivergencias`).

São **três colunas cruas e não uma razão**: a razão não compõe, e `2.315 de
2.383` é problema de outra ordem que `93 de 3.334`. `coercions` fica **separada**
de `typeMismatches` e não somada a ela porque as duas afirmações pedem remédios
diferentes — divergiu é "o valor se perdeu, a coluna está incompleta"; coergiu é
"o valor está lá, mas a origem escreve a mesma coisa de duas formas". As três são
**NULL para campo de topo**, pelo mesmo argumento que mantém `parent` ausente ali
— zero diria "a chave nunca apareceu". E são **contadores de processo**, que
zeram no restart: `0` num dataset recém-subido é "ainda não medi", não
"estável".

O custo está medido, e não raciocinado: o incremento roda uma vez por coluna
derivada por evento, no caminho mais quente do produto, e a comparação contra
`benchmark/baselines/segunda-passada-20260825.txt` não distingue o antes do
depois (`p=0.310`), com zero alocação. `coercions` foi medida contra a MESMA
bancada, e com o mesmo desfecho: a comparação da coerção — que acrescenta um
teste de forma por coluna derivada por evento e um segundo `atomic.Add` nas que
coergem — também não distingue o antes do depois (`p=0.818` com descoberta,
`p=0.974` sem, `-count=6`), e os dois lados seguem em zero alocação.

Por SQL, e não por rota HTTP nova, porque uma tabela **não exige suporte em
cliente nenhum**: DBeaver, CLI e driver já sabem executar `SELECT` e desenhar o
resultado. Uma rota precisaria de código em cada um.

Por dentro ela é um Store VIRTUAL atrás do mesmo binder, planner e executor de
qualquer consulta, e a spec dela é escrita no mesmo YAML de um dataset de
verdade. É isso que lhe dá `WHERE`, `LIMIT` e projeção sem gramática nova nem
caminho de execução paralelo, e o que faz coluna inexistente ser recusada com a
mesma mensagem do resto do produto.

A resposta é **estado corrente, não declaração**: campo autodescoberto fixa o
tipo na primeira observação, então duas execuções em momentos diferentes podem
devolver conjuntos diferentes — a mesma ressalva que o `SELECT *` já carrega.

O nome qualificado custou um token novo no lexer, e o cuidado foi com o
diagnóstico que ele ameaçava levar junto: `.5` e `SELECT t.a` morriam no lexer
com "caractere inesperado '.'", que nomeia o problema. As duas guardas de
mensagem entraram JUNTO com o token, não depois. Nome de dataset não pode
começar com `_`: o prefixo fica reservado a esses espaços de nome, e a recusa
entrou antes de existir qualquer dataset que o ocupasse — uma linha agora,
"recrie o dataset" depois.

## Formato de saída

NDJSON: uma linha por row, terminada em LF. Colunas **sempre presentes**, com
`null` explícito — os clients mapeiam POSIÇÃO para valor e decodificam cada
linha num passe só.

A posição, e não o nome, porque **chave repetida é legal**: `SELECT a, a`
produz `{"a":1,"a":1}`, e um cliente que indexasse a linha por um mapa de nome
perderia a segunda coluna em silêncio. É o `null` explícito que torna o
casamento posicional possível — com o campo ausente, o número de chaves da row
deixaria de ser o número de colunas do schema.

Dois cuidados que o framing exige: o `json` armazenado guarda a janela crua do
evento, então um documento pretty-printed chega com quebra de linha dentro (é
sanitizada byte a byte); e uma string materializada **contém** control char cru,
porque o tokenizer decodifica `\n` ao gravar (é escapada). Sem os dois, uma
linha vira duas e o cliente dessincroniza.

## Os clients

O protocolo tem TRÊS falantes do lado de fora, e é por isso que
`internal/protocol` existe como pacote próprio em vez de morar dentro do
servidor: o `nishidb query --server`, o driver JDBC (`clients/jdbc`) e o client
Python (`clients/python`). Com os tipos dentro do servidor, cada cliente
redeclararia o formato e as definições divergiriam no primeiro campo
acrescentado, sem nada em camada nenhuma reclamar.

| | JDBC | Python |
|---|---|---|
| Artefato | `nishidb-jdbc.jar` | wheel `nishidb`, montado do repositório |
| Piso | JDK 21 | Python 3.10 |
| Contrato | `java.sql` | PEP 249 (DB-API 2.0) |
| Dependências de runtime | nenhuma | nenhuma |
| Destrava | DBeaver, IntelliJ | pandas, SQLAlchemy, BI |

**As mesmas quatro doutrinas de cliente nos dois**, herdadas de
`internal/cli/credential.go`: proxy do ambiente ignorado (o incidente do piloto
em que o proxy corporativo respondia 403 e o operador culpava o servidor),
**todo 3xx é erro** (a política padrão das bibliotecas preserva o
`Authorization` em redirect same-host, e o NishiDB não redireciona), nenhum
caminho para desligar a verificação de certificado, e senha que nunca é
argumento de processo.

E a mesma divisão de prova: os unitários de cada um falam com um servidor HTTP
da própria plataforma, com respostas escritas à mão — eles provam que o client
faz o que quem o escreveu ACHA que o servidor emite. A outra metade é
`make test-jdbc` e `make test-python-it`, que sobem o BINÁRIO com TLS e
credencial e o alimentam com o mesmo corpus (`scripts/it-comum.sh`). A distância
entre as duas metades não é teórica: o bug de reuso de conexão do client Python
passou pela suíte inteira e só apareceu contra o servidor real, porque o de
verdade responde `/v1/query` em `chunked` e o de mentira respondia com
`Content-Length`.

**O que nenhum dos dois faz**, porque é o que o NishiDB é: escrita (os dados
entram por ingestão), transação, e cursor rolável.

### A árvore do cliente SQL

O `getSchemas`, o `getTables` e o `getColumns` do driver JDBC saem do campo
`tables` do `/v1/metadata`, e não mais da lista de datasets: a base como
`TABLE` no schema declarado, o contador como `VIEW` no mesmo schema, e as
tabelas do `_meta` como `VIEW` no schema `_meta`. O código do tipo de cada
coluna é o `jdbc` que o servidor calculou — o driver não o rededuz do nome do
tipo, pela razão de sempre: duas tabelas de tipos divergem no primeiro tipo
novo.

**É o `TABLE_SCHEM` que faz a ferramenta gerar `FROM <schema>.<tabela>`.** Foi
o que o spike do DBeaver mediu, e é por isso que ele é o campo que paga o
ciclo: publicá-lo nulo faria o SQL que a ferramenta monta cair no dataset da
conexão, que para toda tabela de outro schema é outra tabela. O
`supportsSchemasInDataManipulation` responde `true` pela mesma razão, e é a
única dos cinco `supportsSchemasIn*` que responde — as outras quatro são DDL,
procedimento e privilégio, que o produto não tem.

**O `TABLE_CAT` continua nulo em toda linha**, e o `getCatalogs` continua
vazio. Não é omissão: **é a gramática do `FROM` que decide o mapeamento JDBC.**
O parser aceita no máximo `qualificador.tabela` — dois níveis —, e o JDBC tem
três nomes para gastar (`TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`). Um deles
sobra, e a escolha de qual é forçada em cadeia:

- o nível de baixo é a tabela, e não há discussão;
- o de cima tem de ser o `TABLE_SCHEM`, porque **é ele que a ferramenta usa para
  qualificar o `FROM` que gera** — o spike mediu isso. Gastá-lo no `TABLE_CAT`
  deixaria o `TABLE_SCHEM` nulo, e a ferramenta geraria `FROM <tabela>` sem
  qualificador, que cai no dataset da conexão;
- logo o `TABLE_CAT` é o que sobra vazio, e o `getCatalogs` responde vazio por
  coerência com ele.

O caminho contrário — inventar um terceiro nível para o produto ter catálogo —
mudaria a gramática do SQL por causa de um cliente, e não há o que ganhar: com
um catálogo só, todo nome viria qualificado por um prefixo constante. As cinco
`supportsCatalogsIn*` respondem `false`, e o `getCatalogs` devolve o ResultSet
de uma coluna e nenhuma linha, que é a forma que o contrato pede para "não há".

**O `getTableTypes` responde `TABLE` e `VIEW`, e nunca `SYSTEM TABLE`.** O
terceiro rótulo seria o natural para as tabelas do `_meta`, e é justamente o
que não se pode usar — o mesmo spike mostrou que o DBeaver esconde objeto de
sistema por default e que a opção que o revela não é achável na interface.
Tabela que existe para ser descoberta não pode nascer invisível. A lista é
fixa: ela diz o que o driver sabe publicar, e é respondida antes de a
ferramenta perguntar quais tabelas existem.

**As linhas do `_meta` saem uma vez, não uma por dataset.** O documento as
publica dentro de cada elemento de `datasets`, porque o atalho `_meta.x`
significa "no dataset da requisição" e o catálogo de cada dataset o inclui;
elas descrevem a mesma tabela, então o driver corta a repetição pelo par
(schema, nome).

**Servidor anterior aos campos novos: a árvore de sempre.** `schema` e `tables`
são aditivos, e um binário anterior responde sem os dois. Aí cada dataset volta
a ser uma linha `TABLE` com `TABLE_SCHEM` nulo e as colunas do campo `columns`,
que é exatamente o que o driver listava antes — nenhuma view inventada e nada
lançado (`NishiDatabaseMetaDataTest#metadataSemTablesDegradaSemErro`, que fala
com um fixture só seu — o resto da suíte usa o documento novo). A citação vem
qualificada pela classe porque é assim que o
`TestEvidenciasCitadasNosDocumentosExistem` confere um teste JUnit: um nome
camelCase solto é indistinguível de um identificador qualquer citado em prosa.

## O protocolo remoto

![Sequência do POST /v1/query: validação, semáforo, linha de schema, lotes de flush e o fecho que distingue fim de erro](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/sequence_query_http.puml)

O §67 pede um protocolo remoto simples e versionado; a versão é o caminho
(`/v1/`), e o transporte é HTTP. São quatro rotas: `POST /v1/query`,
`GET /v1/metadata`, `GET /v1/stats` e o `GET /v1/health` que o M2 já entregou.

**A resposta tem moldura, e ela resolve um problema de mecanismo.** O status
HTTP viaja com o primeiro byte, então uma consulta que quebra na linha 10.000
não tem mais status para usar. O corpo é NDJSON com três papéis: a primeira
linha descreve as colunas, as do meio são as rows exatamente como o §69 as
mostra, e a última diz como terminou — `end` com `truncated`, ou `error` com
código. Depois do schema o status é sempre 200, e um stream que termina **sem**
fecho é falha de transporte, não resultado curto.

A linha de schema fecha o débito 13: o NDJSON não carrega tipos, e o
`ResultSetMetaData` do §71 precisa deles — inclusive das colunas que são NULL
em todas as linhas, que o dado não revela por definição. A fonte é o tipo
declarado, conhecido no `Open`, antes da primeira row.

**Truncar tem dois significados que não se confundem.** O teto de
`query.maxRows` do dataset é involuntário e sai como `error`
(`MAX_ROWS_EXCEEDED`), o mesmo código que o CLI traduz em exit 5. O `maxRows`
do request é o cliente pedindo menos, e sai como `end` com `truncated: true` —
atender ao que foi pedido não é falha. Pedir **mais** que o teto do dataset é
recusado com 400 nomeando o valor efetivo, nunca reduzido em silêncio.

**Os parâmetros do §68 têm quatro tipos de wire** — `string`, `int64`,
`double` e `boolean` — e a lista curta não é escolha do protocolo: é o contrato
do binder, que recusa parâmetro em representação binária para manter um caminho
de coerção só. Endereços, prefixos e timestamps viajam como `string` e são
convertidos contra o tipo da coluna pela mesma tabela dos literais SQL, que é o
que o §73 já pressupõe ao mapear `ipv4` para `VARCHAR`.

**Streaming é verificado, não afirmado.** `query.resultBatchSize` ganhou seu
primeiro leitor e virou o lote de flush — e são dois flushes por lote, o do
`StreamWriter` (que tira as linhas do buffer fixo de 64 KiB) e o do
`http.Flusher` (que entrega o chunk ao fio); só o primeiro deixaria os bytes
parados dentro do processo. Dois gates cobrem o §128, e os dois foram aferidos
contra um servidor que monta a resposta antes de enviá-la: o de temporização
(a primeira linha chega com a consulta ainda em voo) e o de memória (~40 KB de
heap vivo tanto para 2.000 quanto para 40.000 linhas).

**Sob pressão, consulta é rejeitada** (§80, §81): `runtime.query.maxConcurrent`
(default 16) é um semáforo por processo, e o excedente sai 503 com
`Retry-After` sem tocar o Store. É por processo, e não por dataset, porque o
recurso protegido é do processo — o §82 já diz que todos os datasets dividem o
mesmo orçamento de memória.

**A consulta longa é cidadã de primeira classe, e por isso tem sub-pool.**
`query.timeout` deixou de acumular os papéis de default e de teto: o teto agora é
`query.maxTimeout`, e o request pode ALONGAR até ele — antes só sabia encurtar,
e dar prazo maior a uma consulta exigia subir o default de todas. `maxTimeout`
ausente vale o `timeout` resolvido do próprio dataset, que é o que preserva o
comportamento de todo arquivo anterior à separação.

A contenção que isso torna obrigatória é `runtime.query.maxLongRunning`
(default 2, DENTRO do `maxConcurrent`): uma consulta cujo prazo efetivo passa do
`query.timeout` do dataset toma uma vaga de CADA pool. Sem ela, `maxConcurrent`
relatórios de dez minutos ocupariam o semáforo inteiro e toda consulta
interativa receberia 503 por dez minutos — o teto do §80 protegeria a ingestão
entregando, no lugar dela, a consulta que responde em milissegundos. A recusa do
sub-pool sai como `TOO_MANY_LONG_QUERIES`, separada de `TOO_MANY_QUERIES` porque
a ação do cliente é outra: o pool geral esvazia em milissegundos, e este pode
levar minutos.

**O prazo tem UMA fonte.** O executor tinha um `defaultQueryTimeout` de 30s próprio, e
aplicava o `bq.Timeout` do dataset SEMPRE — inclusive por cima de um contexto que
já trazia deadline. Como o menor deadline vence, um servidor que tivesse
resolvido cinco minutos contra `query.maxTimeout` veria o prazo cortado de volta
para 30s, e a consulta morreria em TIMEOUT com o teto configurado dizendo que ela
podia. Hoje `executor.Open` só aplica o prazo do dataset quando o chamador NÃO
trouxe deadline, e recusa uma query ligada sem prazo com `ErrInvalidPlan` em vez
de cair num default próprio.

**A memória de consulta virou orçamento, e não mais uma chave sem leitor.**
`runtime.memoryTarget` e `runtime.query.maxBufferPerQuery` eram parseados e nunca
lidos (metade do débito 15). `internal/query/budget` é o teto: dois
`atomic.Int64`, sem mutex e sem alocação — o consumidor é chamado uma vez por
row, e o gate de alocação do executor exige igualdade exata entre 1.000 e 8.000
linhas. O `Global` recusa na ADMISSÃO (503 + `Retry-After`, ao lado do semáforo)
e o `PerQuery` recusa POR LINHA, dentro do writer. A reserva é do teto inteiro na
entrada: com reserva incremental o orçamento do processo pode acabar na linha
quarenta mil, e o cliente perde o resultado por causa da memória de outra
consulta.

**Primeiro consumidor real: o `StreamWriter`** — o débito 28, cujo próprio texto
já dizia que o remédio era o `maxBufferPerQuery` do §80. Ele nunca encolhia: uma
linha de 1 MiB fixava a capacidade pela vida do writer, e o pior caso era
N × maiorLinha de RSS residente. Hoje o teto vale sobre a CAPACIDADE (é o array
que ocupa memória, e era ele que o `buf[:0]` preservava), a linha que não cabe
sai como `ROW_TOO_LARGE` com o próprio tamanho e a chave na mensagem, e o array
grande vira lixo na hora — recusar sem soltar a memória teria evitado o sintoma
errado. O buffer inicial fica em METADE do teto quando o teto é apertado: o
append que cruza o limiar de flush é o que estica o array, e sem folga um
`maxBufferPerQuery` abaixo dos 64 KiB default do stream viraria "nenhuma consulta
funciona".

**Quem está segurando as vagas fica visível.** Um registro de consultas em voo
(`{dataset, início, hash do SQL}`, alimentado ao lado de `s.met.admite` e limpo
no defer) alimenta o `/v1/stats` e o `nishidb status`. O medidor
`nishidb_query_active` responde "quantas"; com prazo longo virando padrão, a
pergunta do operador passou a ser "qual, e há quanto tempo".
`nishidb_query_longest_running_seconds` é lida no SCRAPE, e não escrita na
admissão, porque o valor cresce sozinho enquanto nada acontece — um medidor
escrito na admissão ficaria congelado exatamente no estado que ele existe para
denunciar.

**O bind default é loopback.** Até o M2 o servidor só expunha `/metrics` e
`/v1/health`; com `/v1/query` ele passa a servir os dados. A autenticação e o
TLS do §74-77 chegaram na Fase 11 e estão descritos na seção seguinte. Não
contradiz o §78: o exemplo de lá abre o bind no mesmo arquivo em que liga TLS
e `authentication`. Expor continua possível e virou ato explícito, com aviso
na subida — ou recusa, quando a decisão do operador não está no arquivo (ver
abaixo).

## Segurança: autenticação e TLS (Fase 11)

O débito 57 (`/v1/query` sem autenticação — porta exposta equivale a um banco
sem senha) fechou nesta fase com `internal/config/config_security.go`
(forma no parse), `internal/server/security.go` (ativação e a matriz de
recusa) e `internal/server/auth.go` (o middleware e os papéis).

**O plano é duplo, e a razão é o piloto.** O listener EXPOSTO passa a exigir
TLS **e** autenticação em TODAS as rotas — `/v1/health` e `/metrics`
inclusive, não só `/v1/query` — porque uma exceção "métricas não precisam de
senha" seria uma porta sem tranca do mesmo jeito. Um segundo listener,
opcional (`server.loopback`, default desligado), serve o MESMO conjunto de
rotas sem TLS nem autenticação, só em `127.0.0.1`: é o que mantém painel,
script de atualização e cron locais funcionando sem credencial mesmo com o
listener principal fechado a cadeado. Os dois listeners compartilham a
mesma FUNÇÃO de definição de rota (`Server.buildMux`,
`internal/server/auth.go`), chamada uma vez por listener — nunca
duas listas que poderiam divergir.

**A matriz de recusa do §77** decide se o servidor sobe, olhando bind, TLS e
autenticação juntos (`evaluateExposicaoEAuth`,
`internal/server/security.go`):

| bind | `authentication` | TLS | resultado |
|---|---|---|---|
| loopback | qualquer | qualquer | sobe (avisa `auth.sem_tls` se autenticação ligada sem TLS — HTTP Basic em texto claro, mesmo que só alcançável pelo próprio host) |
| exposto | **ausente** do arquivo | — | **recusa**, pedindo decisão explícita |
| exposto | `enabled: false` **explícito** | — | sobe com o aviso `bind.exposto` (a decisão já foi tomada) |
| exposto | ligada | desligado | **recusa** (§77): Basic sem TLS transmitiria a senha em texto claro pela rede aberta |
| exposto | ligada | ligado | sobe, sem aviso — a combinação que o §77 pede |

A célula "ausente" existe porque o `KnownFields` global do parser continua
desligado (doutrina do M1): sem ela, um `authentification:` (typo) sumiria em
silêncio e o servidor subiria exposto e sem senha nenhuma, com o operador
achando que tinha autenticação. É a mesma classe de defeito que a issue #1
denuncia, só que na chave mais cara do arquivo.

**Assimetria deliberada: autenticação desligada torna as rotas ADMIN
anônimas também no plano PRINCIPAL, não só no loopback.** Isso vale na
célula 3 da matriz (bind exposto, `enabled: false` explícito) e, do mesmo
jeito, na config DEFAULT (bind loopback, `server.authentication` nem
declarado): `AuthenticationEnabled()` falsa faz `withAuth`
(`internal/server/auth.go`) devolver o mux CRU como passthrough
puro, sem checar papel nenhum — `POST /v1/compact` e `POST /v1/checkpoint`
respondem sem credencial no MESMO listener que serve `/v1/query`. É
comportamento testado, não descuido: o caso de uso central do §96 é o
operador que desligou o laço automático de compaction e opera na mão (modo
manual), e a config que a maioria dos deploys usa hoje — sem
`server.authentication` nenhuma — não pode deixar essa rota morta.

A exceção são as rotas de gestão de usuário (`/v1/users/*`, issue #40): o
passthrough também as alcança, mas o handler exige um principal autenticado e,
sem ele, responde FORBIDDEN — não existe gestão de usuário anônima, nem com a
autenticação desligada nem no plano loopback. Ver "A tabela de usuários",
abaixo.

**HTTP Basic sobre TLS**, não um esquema próprio — mapeia 1:1 para
`DriverManager` do JDBC (Fase 7) e para `basic_auth` de ferramentas de
scrape. A senha do request é verificada contra o hash argon2id da tabela de
usuários (`users.Store.Authenticate`, `internal/users/verifier.go`, seção
abaixo). Usuário inexistente paga o MESMO KDF, contra um hash fantasma com os
parâmetros dos hashes reais, e a resposta de falha é **uma função só**
(`rejectAuth`) para os três casos — header malformado, usuário inexistente,
senha errada —, sempre com a MESMA mensagem e os MESMOS headers: o 401 é
byte-idêntico entre "não existe" e "senha errada", provado por comparação de
`Header()` completo, não só de duas chaves nomeadas.

**Sem lockout, com atraso fixo.** Não há contador por IP nem por usuário —
isso abriria a porta para auto-DoS contra um username conhecido. Em vez
disso, toda falha de autenticação paga o MESMO atraso fixo (~100ms,
`atrasoOfFail`), antes de qualquer escrita de resposta, o que mitiga o
timing attack de enumeração sem criar um estado que um atacante possa usar
para trancar a conta de outra pessoa. O rate-limit por IP fica registrado
como débito (ver `debitos.md`).

**Papéis: READ e ADMIN, com ADMIN ⊇ READ** (`roleAuthorized`,
`internal/server/auth.go`) — um usuário ADMIN alcança toda rota READ;
um usuário READ nunca alcança rota ADMIN. O enum é fechado
(`config.RoleRead`/`config.RoleAdmin`) e comparado por string EXATA: `read` ou
`Read` são recusados pela tabela de usuários (`users.ValidateRole`), não
normalizados.

**O registro de rota é fail-closed por construção.** `router.registra` é a
ÚNICA função que grava o papel exigido junto com o handler
(`rt.papeis[padrao]`); o middleware (`withAuth`) consulta essa mesma
tabela para decidir 403. Uma rota que casasse no mux SEM ter passado por
`registra` — por exemplo, um `rt.mux.HandleFunc` chamado direto por engano —
não fica acessível sem checagem: é tratada como BUG DE REGISTRO e recusada
com 500 genérico ao cliente (o detalhe — qual rota, e a dica de usar
`registra` — vai só para o log). A alternativa mais óbvia (deixar passar
quando não há papel registrado) foi descartada porque mascararia o bug até
alguém notar em produção; o 500 é alto e imediato, tanto em teste quanto em
log.

**O plano loopback é READ-only por construção, não por convenção.** O
segundo listener não tem middleware de autenticação, mas uma rota cujo papel
registrado é ADMIN responde 403 nele também (`muxLoopback`,
`rejectLoopbackAdmin`) — nunca anônima. A razão é o host do piloto ter
múltiplos usuários locais: `compact`/`checkpoint` anônimos por qualquer um
deles seria stall de ingestão sem privilégio nenhum de por meio. Rotas READ
continuam servidas sem credencial no loopback, intocadas.

Essa garantia é EXCLUSIVA do plano loopback, e não se estende ao principal:
com autenticação desligada (célula 3/config default, seção acima), o
principal não tem `muxLoopback` nem `rejectLoopbackAdmin` no meio —
`withAuth` é puro passthrough, e as rotas ADMIN respondem anônimas
ali também. O loopback é fail-closed por construção porque o piloto tem
mais de um usuário local no HOST; o principal sem autenticação segue a
regra oposta — o operador desligou a checagem inteira, não só o TLS —, e é
essa mesma ausência de checagem que faz `POST /v1/compact` funcionar sem
credencial no caso de uso central do §96 (modo manual).

### A tabela de usuários (issue #40)

Os usuários NÃO moram no YAML. A fonte única de quem autentica é a tabela de
usuários do servidor, `internal/users`, em `<storage.path>/_system/users.state`;
o antigo `server.authentication.users` é recusado na subida com o roteiro de
migração na mensagem (`validateServerSecurity`,
`internal/config/config_security.go`).

![Tabela de usuários: bootstrap, gestão com o servidor no ar, verificação com cache e a barreira ADMIN do _meta.users](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/usuarios_flow.puml)

**Arquivo, não dataset.** São poucas linhas, e um Store traria dezenas de MiB
de mmap, recovery e presença no catálogo, no health e na compactação — cada
um pedindo exceção. O arquivo é regravado INTEIRO a cada mutação pelo
`atomicfile` (tmp → fsync → rename → fsync do diretório), o mesmo caminho
durável do `manifest.bin`, com magic, versão e crc32c. Dá a garantia de
durabilidade que o dado tem: uma troca de senha que sobreviveu a um crash
não volta à anterior, e uma que não chegou ao rename nunca foi confirmada
(`TestQuedaNaGravacaoDaTabelaPreservaOQueChegouAoDisco`). O diretório tem flock
próprio (`_system/LOCK`): a gestão offline com o servidor no ar sai com exit
3.

**Ordem de toda mutação: valida → monta a tabela nova → grava durável →
publica o ponteiro → audita.** Publicar o ponteiro (`atomic.Pointer`) é o
commit de visibilidade e vem por último, como a location no storage: ninguém
autentica com uma senha que ainda não está no disco. Falha de gravação deixa
a tabela FALHA para mutação até reabrir (`users.ErrWriterFailed`, a mesma
doutrina do stickyErr): a queda pode ter vindo depois do rename, e a próxima
mutação, montada a partir da memória, apagaria em silêncio uma mudança que
talvez esteja no disco. A autenticação segue servindo pelo que publicou.

**Bootstrap.** Com a autenticação ligada e a tabela ausente, a abertura gera
o ADMIN `admin` com 24 bytes de `crypto/rand`, grava a senha em
`_system/bootstrap-admin.password` (0600) e só DEPOIS grava a tabela — uma
queda entre as duas deixa só o arquivo de senha, e a próxima subida
sobrescreve os dois. O log (`users.bootstrap`) aponta o caminho, nunca a
senha, e o stdout não recebe nada: a unit do systemd grava o stdout num
arquivo que fica. Trocar a senha desse usuário, ou removê-lo, apaga o arquivo.
Tabela ILEGÍVEL recusa a subida (`users.ErrCorruptUsers`) e nunca regera
credencial: seria entregar um ADMIN novo a quem estragou o arquivo
(`TestTabelaDeUsuariosIlegivelRecusaASubidaDoServidor`).

**Verificação a cada request, sem pagar o KDF a cada request.** O hash é
argon2id com sal próprio e os parâmetros gravados na linha (t=2, 19 MiB,
p=1), o que deixa um custo novo entrar sem migração. O verificador guarda,
por usuário, um HMAC-SHA256 da senha que já autenticou, com chave aleatória
do processo, e a GERAÇÃO da linha naquele instante: um acerto compara em
tempo constante e dispensa o KDF; qualquer mutação na linha muda a geração e
invalida a entrada, inclusive a de uma autenticação que estava no meio do KDF
quando a senha mudou (`TestAutenticacaoEmCursoDuranteATrocaNaoEnvenenaOCache`).
Só o SUCESSO toma o atalho: senha errada paga o KDF mesmo com cache quente,
senão o tempo diria que aquela conta existe
(`TestSoOSucessoDispensaOKDFEOsFracassosPagamOMesmoPreco`). No máximo duas
derivações correm ao mesmo tempo (~38 MiB transitórios), e a disputa desse
teto por tentativa inválida em massa é o débito 120.

**Gestão com o servidor no ar.** `POST /v1/users/create`, `/v1/users/role` e
`/v1/users/delete` são ADMIN; `/v1/users/password` é READ, porque todo
usuário troca a PRÓPRIA senha — e a regra "só a própria, salvo ADMIN" mora no
handler, que conhece o nome do principal que `withAuth` pôs no context. A
recusa para READ vem antes de consultar a tabela, para que FORBIDDEN e
UNKNOWN_USER não revelem se um nome existe. Sem principal — autenticação
desligada ou plano loopback — toda rota de usuário responde FORBIDDEN: não
existe gestão anônima. O último ADMIN não é removido nem rebaixado
(`LAST_ADMIN`). Cada mutação sai no log estruturado (`users.created`,
`users.password_changed`, `users.role_changed`, `users.deleted`) com `actor`,
`user`, `role` e `remote`; a gestão offline registra `actor` como
`local:<usuário do sistema>`. Não há sessão: o Basic é verificado a cada
request, então a troca vale a partir do request seguinte.

**`_meta.users` é só-ADMIN no motor, não no servidor.** A tabela
(`internal/metausers`) lista nome, papel, marca de bootstrap e quem criou e
alterou quando, sem coluna de hash. Ela é registrada com
`Engine.RegisterAdminVirtual`, e o motor a recusa com `query.ErrForbidden`,
antes do bind, a toda consulta cujo context não traga `query.WithAdmin` — marca
que só o `handleQuery` põe, e só para principal ADMIN. O plano loopback, o
usuário READ e o chamador que esquecer caem na recusa sem código extra
(`TestSemAMarcaDeAdminATabelaDeUsuariosEhRecusada`).

**TLS.** O certificado é carregado uma vez na subida
(`validateCertificateAtStartup`, `tls.LoadX509KeyPair`) e nunca relido do disco
depois — rotação é reiniciar o processo. `MinVersion: tls.VersionTLS12` é
explícito no `tls.Config` (mesmo precedente de `internal/kafka/config.go`). O
leaf é parseado na subida para publicar `nishidb_tls_cert_expiry_timestamp`
(epoch segundos do `NotAfter`) e logar `tls.certificado_expirando` (WARN,
menos de 30 dias) ou `tls.certificado_expirado` (ERROR, já vencido) — nos
dois casos o servidor SOBE mesmo assim: recusar por causa da data derrubaria
as consultas por um problema que o operador corrige sem downtime se o aviso
chegar a tempo.

**As rotas mutantes do §96 — `POST /v1/compact` e `POST /v1/checkpoint`,
ambas ADMIN** — são as primeiras do protocolo remoto que mudam estado, e a
semântica é sempre a mesma: SÍNCRONA (a resposta só sai depois do trabalho
feito, nenhuma das duas enfileira ou devolve 202) e IDEMPOTENTE POR EFEITO,
não por resultado — repetir `checkpoint` sobre estado inalterado paga o
custo real do mutex do writer, mas não muda o manifest de forma observável;
repetir `compact` sobre um dataset já compactado é uma varredura que não
acha candidato e devolve zero. `POST /v1/compact` usa o MESMO
`storage.compaction.minimumLiveRatio` que o laço automático usaria e
funciona mesmo com `storage.compaction.enabled: false` (é o caso de uso do
modo manual); quando uma passada — do laço automático OU de outra chamada
manual — já está em voo, a rota devolve **409** (`COMPACTION_BUSY`) em vez de
enfileirar, porque as duas disputam o MESMO single-flight do `Store`
(`compPassMu`/`compPassInVoo`, `internal/storage/store.go`).

O corpo de ambas aceita `{"dataset": "..."}` opcional; ausente significa
TODOS os datasets, na ordem determinística da subida. **O contrato de falha
parcial** (`processaDatasets`/`ErrParcial`,
`internal/server/mutations.go`) é stop-no-primeiro-erro, mas a resposta de
erro nomeia o dataset que falhou E os nomes dos que já tinham sido
processados com sucesso ANTES dele — o efeito colateral (checkpoint gravado,
disco já devolvido) aconteceu de verdade para eles, e escondê-lo do operador
seria pior do que expor a lista.

## Operar isto: o que morde primeiro

Decisões conscientes que **não** são bug, mas que surpreendem quem opera sem
conhecê-las. Cada uma tem dono em [`debitos.md`](debitos.md).

- **Um processo por dataset.** Ver a restrição de `flock` acima. Rotina de
  monitoração que consulte durante a carga falha com exit 3 até o M3.
- **Corrupção é detectada na LEITURA, não na abertura.** Nenhum CRC é
  verificado antes do `DurableOffset` no `Open`: um byte adulterado numa região
  já selada aparece no meio de uma query, que aborta com exit 3 **depois** de
  ter emitido as linhas anteriores. Quem consome a saída precisa tratar exit ≠ 0
  como "resultado incompleto", mesmo tendo recebido linhas válidas.
- **Perda de evento sai com exit 7, não com 0.** Evento ruim é descartado
  (política §101) e a ingestão termina com 0; evento que não entrou por falha
  de infraestrutura sai com 7. Um pipeline com retry deve ligar a política ao
  exit code, não ao texto do resumo.
- **Entrada interrompida na leitura também sai com 7.** Erro de I/O no NDJSON
  ou linha acima do teto de 8 MiB do scanner param a ingestão, e o resto do
  arquivo não entra: o `ingest` durabiliza o que já aplicou, imprime
  `ENTRADA INTERROMPIDA` no stderr e sai 7. Saía 1 ("erro de uso") antes, o que
  mandava o operador conferir a linha de comando.
- **Com mais de um desfecho, o exit code é o MAIS GRAVE**, na ordem
  `4 > 3 > 7 > 6 > 1`, e não o do último evento ocorrido: uma ingestão que
  perde eventos e depois recebe SIGTERM sai 7, não 6. Cada retorno antecipado
  que pulava o teste de perda transformava perda de dado num código que fala de
  outra coisa — a razão de o `ingest` ter um ponto de saída único.
- **Campo indexado tem teto de 64 KiB** (`types.MaxIndexedValueBytes`, limite do
  `u16` do dicionário de valores). Valor acima disso faz o evento inteiro
  falhar.
- **O conjunto de índices secundários passou a ser MUTÁVEL nas duas direções, e
  o que resta é custo, não recusa.** Foi assim até o M6: as duas guardas do
  débito 22 recusavam acrescentar `index` a um dataset com `RowCount > 0` e
  recusavam abrir sem um campo que o manifest declarasse indexado, saindo com
  **exit 3** num dataset íntegro. As duas saíram junto com os motores globais.

  O que as justificava era um índice GLOBAL que cobrisse só um pedaço do dataset
  e devolvesse **menos** linhas do que existem — falso negativo, que nenhuma
  camada acima recupera. O índice por segmento não consegue produzir isso, e a
  resposta ao segmento sem a seção do campo tem uma forma por modo: no
  **histórico**, o segmento enumera a própria faixa de rowIds como candidata; no
  **materializado**, UM segmento sem índice **colapsa a consulta inteira numa
  caminhada única do `locations.idx`** — todas as linhas com location viram
  candidatas, como num full scan (a forma por segmento tinha uma corrida de
  falso negativo com updates concorrentes, e a caminhada única a fecha lendo
  cada location uma vez só). Nos dois modos o executor revalida o predicado
  contra o payload corrente (§44): o resultado é o mesmo do full scan filtrado,
  e a diferença é só de custo.

  - **Acrescentar** um campo indexado a um dataset povoado é aceito, e a consulta
    responde certo desde o primeiro instante, inclusive sobre as linhas gravadas
    antes da declaração.
  - **Remover** também é aceito: as seções param de ser escritas, e as que
    existem saem na reescrita do índice daquele segmento.

  **E os segmentos antigos passaram a ser AGENDADOS**, que é a primeira metade
  do débito 22. A pendência de reconstrução é decidida por **(segmento,
  campo)**: a fila compara o conjunto de seções do `.sidx` com o conjunto
  indexado do dataset, nos dois sentidos — seção que falta agenda a
  reconstrução, seção de campo que deixou de ser indexado agenda a reescrita que
  a apaga. Como o `.sidx` é imutável, "acrescentar uma seção" é reescrever o
  arquivo inteiro, e a reconstrução cobre a **união** dos campos pendentes com os
  que o arquivo já cobria e continuam declarados: sem isso, a passada seguinte
  reescreveria com o campo antigo e perderia o novo.

  **E a outra metade fechou o débito 22:** uma goroutine por dataset CONSOME a
  fila — primeira passada imediata, cochilo pelo mesmo orçamento de ciclo de
  trabalho da compaction, cancelamento por contexto e `Close` que cancela e
  aguarda. O servidor a liga em todos os datasets, e não só nos históricos: a
  reconstrução vale para os dois modos, e o backfill de um campo novo acontece
  no materializado, onde não há passada de retenção nenhuma.

  A unidade de trabalho é (segmento, campo) e o processamento é por SEGMENTO —
  os campos pendentes de um segmento saem na mesma leitura sequencial, porque
  ler duas vezes para dois campos pagaria o dobro pelo I/O, que é o custo
  inteiro da operação. O laço **não trata a fila como trabalho que encolhe**:
  ela é um superconjunto por construção (um segmento em que um campo declarado
  não aparece em record nenhum fica pendente para sempre), então ele guarda o
  conjunto de seções que deixou em cada arquivo e só refaz quando o disco deixa
  de ser aquilo — é isso que o faz convergir sem deixar de reagir à janela
  alargada, que remove o `.sidx` de um segmento vivo.

- **Duas pausas visíveis, aceitas no M1:** o rehash da primary **pausa a
  ingestão** enquanto redistribui os slots, e o `msync` do checkpoint **pausa o
  writer**. Com `checkpoint.interval: 30s`, isso aparece como serrilha
  periódica no gráfico de vazão.
- **Phantom commit:** um `Put` que devolveu **erro** ao chamador pode virar
  **visível** depois do restart, se o record já tinha sido gravado no segmento
  quando a falha ocorreu. É decisão consciente (o replay do tail não distingue
  os dois casos) e está testada; quem trata erro de escrita como "nada foi
  gravado" vai divergir do dataset no próximo restart.
- **`mode: replace` existe, mas some do resumo do `ingest`.** Além do `merge`
  (default), a materialização aceita `mode: replace`, em que cada evento
  substitui o estado inteiro da chave — as postings do valor antigo **ficam**
  no índice do segmento em que nasceram, como candidato a mais que a
  revalidação do §44 descarta, e morrem quando aquele segmento é aposentado
  (não há fase de CLEAR, §5.2). O
  contador dessa ação (`ActionReplaced`) **não é impresso**: o resumo só quebra
  em criados/atualizados/stale, e a substituição não cai em nenhum deles.
  Medido com o binário, 3 eventos da mesma chave num dataset `mode: replace`:
  `3 lidos, 3 aplicados (1 criados, 0 atualizados, 0 stale)` — as duas
  substituições existem no total e somem do detalhe, que deixa de fechar com
  `aplicados`. É o **débito 48**.

## Políticas de escrita por campo

O merge do §2.6 tem UMA regra para todo campo — o valor do evento substitui o
do estado —, e três exceções declaráveis. Todas viajam na `patchEntry`, e é o
merge o único lugar que as conhece: nem o create nem o replace precisam saber
que existem.

| Política | Declaração | O que o merge faz |
|---|---|---|
| Sobrescreve | (default) | o valor do evento substitui o anterior |
| Remove | `null` no evento | o campo sai do estado (D2/§24) |
| Congela | `frozenFrom:` | preserva o anterior e descarta o novo |
| Acumula | `merge: sum` | grava anterior + parcela do evento |

A soma custa uma propriedade que o resto do pipeline tem: **idempotência**.
Sobrescrever o mesmo evento duas vezes dá o mesmo estado; somar não. O replay
do tail continua seguro (ele reaplica RECORDS já materializados, não eventos),
mas a reentrega do Kafka depois de um crash soma de novo a janela entre o
último offset commitado e a queda. Onde há `sequence`, o stale check do §25
barra o reprocessamento; sem ela, o valor fica aproximado — acima do real, nunca
abaixo.

**Como se enxerga isso em produção.** O dado gravado não diz qual política o
produziu: um valor congelado é indistinguível de um que nunca mudou, e um total
somado é indistinguível de um que a origem mandou pronto. Por isso cada
política publica contagem própria, no bloco `materialization` do `/v1/stats` e
nas séries correspondentes do `/metrics`:

| Série | Responde |
|---|---|
| `nishidb_frozen_captured_total` / `_preserved_total` | a congelada está capturando? está segurando? |
| `nishidb_merge_sum_total` | quantas somas o merge fez |
| `nishidb_merge_sum_saturated_total` | o teto foi alcançado — a partir daí a coluna não cresce mais |
| `nishidb_ttl_rule_matched_total{rule}` | qual regra está decidindo o quê, pelo índice no YAML |
| `nishidb_ttl_unmatched_total` | escritas sem regra nenhuma (só em dataset sem regra de fundo) |

Os dois lados saem da MESMA leitura do snapshot do materializer: dois
contadores para o mesmo fato divergem no dia em que um caminho de erro esquece
de somar, e em produção alguém lê o errado.

O valor anterior é redecodificado do `RawField` no ramo aditivo, em vez de vir
do `Next()` do laço. Não é desperdício: guardar o valor de todo campo do estado
só para usá-lo nesse ramo custou **43% do merge** quando medido (415→595 ns/op
no `BenchmarkMergeRadiusPatch`), num laço cujo caso normal é copiar dezenas de
campos crus e tocar poucos.

## Expiração por inatividade

`materialization.ttl` é uma **lista de regras**: cada uma tem uma condição
(`when`, com a gramática do WHERE) e um idle timeout, e a **primeira que casa**
decide o prazo da entidade. A regra sem `when` é a de fundo e só pode ser a
última. Cada escrita renova o prazo — entidade ativa nunca expira, entidade
abandonada sai sozinha —, e é isso que impede o estado vivo de crescer com o
espaço de chaves; a compaction não segura isso, porque ela recupera versões
superadas, não entidades inteiras.

São DOIS campos de sistema no payload, e a divisão é deliberada:

| Campo | Tipo | O que guarda |
|---|---|---|
| `_writtenAt` | timestamp | o instante da última escrita — um **fato** |
| `_expiresIn` | int64 (millis) | a **duração** que a regra escolheu para esta entidade |

A duração é gravada em vez do prazo absoluto por duas razões. A primeira é
composição: com o instante ao lado, mudar a regra no YAML vale na hora para as
linhas existentes. A segunda é ordem: reordenar as regras no arquivo não muda o
significado do que já está gravado.

`_expiresIn` é emitido em **toda** escrita, e não só quando o valor muda. O
merge preserva campo que o evento não menciona, então uma emissão condicional
deixaria o prazo curto de quando a condição valia sobreviver ao evento que a
quebra — e a entidade morreria logo depois de o alarme reabrir. São 8 bytes por
versão de record para tornar impossível a única falha que arruinaria a feature.

O índice `prazos.idx` (rowId→duração) é DERIVADO do campo, como o de escritas:
o replay do tail o reconstrói pelo mesmo `applyRecord`. Zero nele significa
**desconhecido**, e por isso a entidade imortal — a que casou com uma regra
`never` — é gravada com um sentinela (`rowindex.ImmortalTTL`). Sem essa
distinção, ela herdaria o prazo da regra de fundo e morreria sozinha.

**A visibilidade não depende do coletor.** Os três pontos de liveness checam o
prazo, então a entidade vencida some da consulta mesmo com o coletor parado,
atrasado ou nunca ligado. O coletor é mecanismo de **espaço**, não de
correção — e é por isso que `nishidb query` e `nishidb server` respondem a
mesma coisa sobre o mesmo dataset.

A coleta tem **duas rotas**, e elas cobrem coisas diferentes:

| Rota | Como acha | O que ela não alcança |
|---|---|---|
| **Oportunista**, na compaction | Ela já lê todo record do segmento que elegeu, então conferir o prazo custa uma carga de 8 bytes. Ao achar um vencido, grava a lápide direto no destino em vez de copiar o payload | Segmento sem garbage. Garbage vem de update, e entidade abandonada não recebe update — então o segmento onde ela mora nunca é candidato |
| **Coletor**, por passada | Desce a **agenda de vencimentos** e trata só quem venceu | Nada; é ele que cobre o ponto cego acima |

A passada do coletor tem freio duplo, em `storage.expiracao`, e vence o
primeiro a bater: `maxPorPassada` (default 1000) e `tempoPorPassada` (default
5s). São dois porque os riscos são opostos — **marcar é um append**, então uma
leva grande escreve muito antes de a compaction recuperar qualquer coisa e
disputa o lock de escrita com a ingestão; e uma descida que entre em vários
ramos sem achar nada não escreve nada, mas seguraria a goroutine.

### A agenda de vencimentos

![Agenda de vencimentos: a min-tree que troca varrer por perguntar, a ordem de escrita assimétrica e a construção na abertura](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/expiracao_agenda.puml)

O coletor **pergunta** quem venceu, em vez de procurar. Antes ele varria o
índice de escritas a partir de um cursor circular, lendo toda linha do dataset
para achar as poucas vencidas — e o custo disso foi medido no piloto: **19% do
CPU da ingestão, 171 µs por evento**, com `expiry_scanned_total` em **24,1
milhões** no dataset base contra **zero** no histórico. O que dispara a cadência
é a regra condicional `type = 2 → 3m`, que obriga uma volta completa nas 4,57M
linhas a cada ~2,5 minutos para achar as ~0,4% vencidas.

A estrutura é uma **min-tree B-ária sobre array denso em mmap**, indexada por
`rowId`: `vencimentos.idx` guarda o instante de vencimento de cada linha, e
cada nível de sumário (`vencimentos.s1.idx` em diante, B = 64, três níveis)
guarda um limite inferior do mínimo dos seus 64 filhos.

A família foi escolhida pelo requisito que aparece em **todo evento**:
atualizar o prazo de uma entidade. Só ela dá atualização O(1) esperada, porque
separa o valor (denso, escrito no lugar) da ordenação (o sumário, que quase
nunca precisa ser recomputado). Descartados: **heap persistente**, que paga
~log(n) escritas aleatórias por evento e cujo sift interrompido por crash
quebra a invariante sem sintoma; **buckets por janela de tempo**, que morrem no
espaço, porque com renovação a cada evento cada escrita insere uma entrada que
vive o prazo inteiro; **timing wheel em memória**, que não é durável e poria um
nó por entidade no heap.

**A invariante é a corretude inteira:** `nivel[i][b] ≤ min{filhos}`, em todo
instante, para um leitor sem lock. O nó é limite **inferior**, nunca superior, e
a assimetria é deliberada:

| Estado do nó | Custo |
|---|---|
| **abaixo** do mínimo real (frouxo) | uma descida desperdiçada; o writer revalida cada linha sob o lock antes de marcar |
| **acima** do mínimo real | a descida pula o ramo e a linha vencida **nunca mais é recuperada** |

É essa assimetria que fixa a ordem de escrita — abaixando, os ancestrais vêm de
cima para baixo e a folha por último; subindo, a folha primeiro e o recompute
de baixo para cima, parando no primeiro nó cujo mínimo vinha de outro filho — e
é ela também que torna a estrutura segura contra crash **sem journal nenhum**:
toda interrupção no meio de uma atualização deixa nós menores que o exato, que é
o lado barato. Medido em `TestRenovacaoNaoRecomputaAArvoreInteira`: **1,0165
escritas e 0,0165 recomputes por renovação**.

Zero significa **infinito**, e não `MaxUint64`, para que crescer a agenda não
recompute nada: o bloco novo nasce zerado pelo `fallocate`, e zero já é a
resposta certa para linhas que ainda não existem.

**Duas coisas somem junto com a varredura.** O cursor, e com ele a pergunta "de
onde recomeçar depois de um restart". E o pré-filtro de tombstone: a linha
tombstonada recebe o sentinela "nunca" no mesmo `applyRecord` que grava a
lápide e sai da árvore, em vez de continuar vencida para sempre e ser
reexaminada em toda volta.

O laço também deixa de acordar por relógio: dorme até o vencimento que a agenda
conhece, com teto de um minuto por cima. O teto existe porque uma linha pode
entrar na agenda com vencimento anterior àquele em que o coletor foi dormir — o
writer atualiza a árvore, não a goroutine adormecida. Acordá-lo a partir do
write path fecharia isso por construção e custaria um acoplamento que o teto
compra por nada: o ganho é não **varrer**, e ler o topo da árvore uma vez por
minuto é da ordem de microssegundos.

`prazoVencido` **não** lê a agenda. Ele continua lendo `expiry.idx` e
`prazos.idx`, e a agenda decide apenas quando o coletor olha. Fazê-la responder
pela visibilidade transformaria um bug de árvore num bug de query — a linha
sumiria do `SELECT` por causa de uma entrada de sumário errada. O preço dessa
separação é ter de provar que as duas concordam, e é o que
`TestAgendaConcordaComPrazoVencidoSobCargaAleatoria` faz.

**A adoção não exige recriação, e isto NÃO é o débito 22.** O débito 22 era sobre
o índice secundário: ele indexa um valor extraído do payload, e reconstruí-lo
exige ler todo record de todo segmento — conta que o índice por segmento partiu
em uma varredura por segmento, cancelável e paga em segundo plano, mas não
eliminou. A agenda deriva de `expiry.idx`,
`prazos.idx` e `locations.idx`, que já estão populados, densos e em mmap — zero
records lidos, zero dicionário tocado. Um dataset existente ganha a agenda ao
subir o binário novo.

A abertura reconstrói em três situações: manifest sem a seção (a migração);
parâmetros divergentes; e arquivos que não descrevem o dataset. Os parâmetros
viajam numa seção **crítica** do manifest (`0x8003`:
`duracaoDeFundoMs | blockShift | niveis`), e a criticidade é pelo modo de falha
mais silencioso das três seções críticas — um binário que a ignorasse manteria
uma árvore derivada de uma duração de fundo que ele já não honra, e as linhas
simplesmente **nunca venceriam**, com o CRC batendo e sem erro, log ou métrica.
Efeito colateral de honestidade: mudar `expireAfterWrite` no YAML deixa de mover
vencimentos em silêncio e vira um rebuild explícito, logado e com custo medido.

> **Para quem opera:** `nishidb_expiry_scanned_total` muda de significado. Ela
> media **esforço** (crescia com o tamanho do dataset a cada volta do cursor) e
> passa a medir **trabalho** (cresce com o que venceu). Uma taxa que despenca
> depois da subida é o resultado esperado, não uma parada do coletor — quem
> quiser saber se ele está vivo olha `nishidb_expiry_running`.

Entidade materializada **antes** de a expiração ser ligada não tem `_writtenAt`,
o que o índice lê como instante **desconhecido** — e desconhecido nunca vence. A
primeira passada do coletor lhe dá carência, contando um prazo cheio a partir
dali. É o que permite ligar a chave num dataset já materializado sem
reprocessar nada, e o que impede essas entidades de ficarem imortais.

Já a entidade que tem instante mas não tem `_expiresIn` — a materializada antes
das REGRAS — cai na duração da regra de **fundo**. Sem regra de fundo, ela não
expira até a primeira escrita seguinte, que lhe grava um prazo.

O que **não** volta é a chave: a lápide é copiada para frente enquanto a
primary nomear a key, e esquecê-la exige `tombstoneRetention`, que ainda não
tem motor. Cada entidade expirada deixa um resíduo do tamanho da lápide
(header + key + 11 bytes).

## A tabela histórica

![Tabela histórica: do evento cru à linha, o que o Apenda não faz, e a retenção por prazo e por tamanho](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/historico_flow.puml)

O produto materializa **estado corrente**, e é isso que o torna cego ao que a
origem mandou: cada evento é mergeado no record da entidade e a versão anterior
vira garbage que a compaction recupera. Descobrir por que uma coluna tem
determinado valor exige inferir o merge a partir do resultado.

O bloco `history` acrescenta uma **segunda tabela por dataset**, com as mesmas
colunas tipadas e uma linha por evento:

```yaml
history:
  suffix: _history          # default; a tabela é <name><suffix>
  expireAfterWrite: 7d      # opcional
  maxBytes: 40GiB           # opcional; teto dos SEGMENTOS DE DADOS
  segmentSize: 128MiB       # opcional; granularidade da purga
  rawPayload: false         # opcional; acrescenta a coluna _raw
  fields:                   # opcional; só overrides de índice
    identifier: { index: true }
```

Declarar o bloco sem nenhuma das duas políticas de retenção é **recusado**: um
histórico que não expira e não tem teto é disco que cresce para sempre, e
aceitá-lo em silêncio é a configuração que mente do débito 37. O teto precisa
comportar quatro segmentos, porque o corrente nunca é purgado.

**A consulta às duas tabelas sai de graça.** A irmã é registrada no motor sob o
próprio nome, e o driver JDBC já roteia pelo nome que está no FROM
(`NishiStatement.java:76`). `SELECT ... FROM events_history` funciona da mesma
conexão a `events` sem uma linha de mudança em lexer, parser, binder,
protocolo, client Python ou driver.

### Um dicionário, dois Compiled

É o ponto que decide se as duas tabelas têm de fato as mesmas colunas.

A irmã **não tem `fields.dict` próprio**: ela usa o do dataset base. O payload
que ela guarda é o mesmo que o patch do base montou, endereçado por FieldID —
dois dicionários independentes atribuiriam ids pelo mesmo algoritmo
determinístico e coincidiriam até o primeiro campo autodescoberto, o primeiro
teto de campos alcançado num só dos dois, ou a primeira recriação de um deles.
A partir dali a mesma coluna teria ids diferentes nas duas tabelas e o payload
decodificaria colunas trocadas, **sem erro e com o CRC batendo**.

O `Compiled` é próprio, porque três coisas diferem: o nome (o binder o confere
contra o FROM), os índices declarados em `history.fields`, e a ausência da key,
da sequence e das colunas congeladas.

O dicionário resolve o restart; ele não resolve a sessão. Num dataset de
universo aberto, a chave promovida a coluna **depois** da subida só existiria no
`Compiled` do base — a mesma consulta responderia numa tabela e diria "coluna
não existe" na outra até o processo reiniciar. `Compiled.AddMirror` registra o
par, e `ResolveUnknown`, `ResolveNested` e `FixAutoType` propagam.

### O que entra na linha, e o que não entra

O enxerto fica em `Apply`, **depois do Parse e antes do stale check**. Três
consequências, e as três são o desenho:

| Fato | Por quê |
|---|---|
| `patch.entries` **é** o evento cru mapeado | as colunas derivadas de `extract` já estão resolvidas e o `_writtenAt` já foi injetado — nenhum decode a mais |
| o evento **stale** também é gravado | é justamente o que se quer ver ao depurar um descarte por sequence |
| o `_expiresIn` não entra | ele ainda não foi emitido, e o prazo por entidade não significa nada aqui |

**As congeladas saem.** A captura emite a entry com o valor CORRENTE da origem,
e é o merge que preserva o primeiro; sem merge, `originalSeverity` seria igual a
`severity` em toda linha — duplicação com cara de informação.

**`merge: sum` guarda a PARCELA**, não o acumulado. É o que a origem mandou, que
é o que se quer ver ao depurar um total.

**A coluna `_raw`** é opcional e entra na posição do FieldID dela, não no fim: o
payload exige ordem estritamente crescente, e toda coluna descoberta depois do
Compile da irmã ganha um id acima do dela. É ela que permite gravar o evento que
**não parseia** — o ponto cego em que um JSON inválido é hoje apenas um número
num contador. Falha de infra não vira linha: não é o evento que está errado.

### O modo é durável, e a divergência é recusa

O modo viaja numa **seção crítica** da cauda TLV do manifest (`0x8002`), ao lado
da marca-d'água de retenção. Crítica porque o modo de falha que ela impede não é
índice incompleto: é abrir um log append-only como se fosse estado corrente,
com cada entidade aparecendo uma vez por evento e o CRC batendo em todas. A
direção inversa é pior — sem primary, o primeiro `Apenda` duplicaria uma
entidade de forma durável.

O dataset materializado tem modo e marca zerados e continua gravando um
manifest **v1**, byte a byte igual ao de antes desta seção existir: o rollback
segue possível para quem nunca declarou `history`.

No modo histórico, `primary.idx` e `sequences.idx` **não abrem** — a primary É o
mecanismo de dedup e a sequence só serve ao stale check. `expiry.idx` e
`prazos.idx` também não: o prazo ali é do dataset, não da linha. Sobra o
`locations.idx`, que é o commit point de visibilidade.

### Retenção: exata na leitura, por segmento no disco

`expireAfterWrite: 7d` significa sete dias, e não "sete dias mais o resto do
segmento". A visibilidade é decidida por uma **marca-d'água** de rowId, achada
por busca binária a cada passada — O(log n) preads, 27 leituras numa tabela de
100 milhões de linhas.

A busca binária é possível por uma propriedade que só esta tabela tem: os
appends são serializados pelo mutex do writer e o `_writtenAt` é o relógio da
ingestão, então ele é **não-decrescente por rowId**. No dataset materializado a
mesma pergunta exige varrer o índice de escritas, porque cada update reescreve o
instante de uma linha qualquer.

Instante **desconhecido nunca vence**, e é a mesma regra do coletor do §57:
presumir que venceu apagaria dado por omissão.

O disco volta por **descarte de segmento inteiro**, reusando os quatro passos da
aposentadoria que a compaction já tinha — sem o copy-forward antes, porque aqui
não há record vivo a copiar. A ordem é a garantia: a marca anda ANTES de
qualquer byte sair do disco, e o primeiro dos quatro passos é o `Checkpoint` que
a torna durável. Na ordem inversa, um crash deixaria locations vivas apontando
para um arquivo que já não existe.

O prefixo morto do `locations.idx` sai por `fallocate(PUNCH_HOLE)`. É a exceção
deliberada à regra do `allocate`, e ela tem argumento: a regra existe porque
página esparsa mais disco cheio no writeback mata o processo com SIGBUS — mas
isso vale para página que ainda vai ser **escrita**, e rowId nunca regride nem é
reutilizado. O que sobra é leitura, e ler um buraco não aloca. Filesystem sem
suporte degrada em **espaço**, nunca em correção.

**`maxBytes` governa os segmentos de dados, e só eles**, porque é o que a purga
sabe devolver. Índices e `locations.idx` são publicados em separado — um teto
que a purga não consegue perseguir seria um teto que mente. E quando o teto
morde, ele **empurra a marca**: honrar a promessa sobre disco significa deixar
de enxergar linhas que o prazo ainda não tinha vencido.

### Falha do histórico não derruba a ingestão

O histórico é ferramenta; o produto é o dataset base. Um erro em `Apenda` conta,
marca a degradação como **sticky** e segue — sem a marca, cada evento retentaria
um disco cheio e o log viraria uma linha por evento sobre uma condição que não
muda sozinha. O `ApplyResult` não muda.

O checkpoint da irmã acontece **antes** do base, que é a ordem do §48 aplicada a
mais um arquivo: o do base torna a posição da fonte durável, então um crash
entre os dois deixa o offset atrás dos dois e o replay produz **duplicatas,
nunca buracos**. A falha da irmã não bloqueia o checkpoint do base — bloqueá-la
travaria o avanço do offset, e a ingestão de produção pararia por causa da
tabela de debug.

### Como se enxerga isso em produção

O bloco `history` do `/v1/stats` e as séries correspondentes do `/metrics` saem
da MESMA leitura de snapshot, pela razão de sempre: dois contadores para o mesmo
fato divergem no dia em que um caminho de erro esquece de somar.

| Série | Responde |
|---|---|
| `nishidb_history_rows_total` / `_dropped_total` | a tabela está COMPLETA? |
| `nishidb_history_degraded` | ela parou de receber? |
| `nishidb_history_data_bytes` | o que o `maxBytes` governa |
| `nishidb_history_index_bytes` | o disco de índice, medido em blocos — a purga também o devolve; `nishidb_index_bytes` é a parcela por segmento |
| `nishidb_history_oldest_written_at` | a janela real que a tabela cobre |
| `nishidb_history_index_window_seconds` | a janela real que o ÍNDICE cobre |
| `nishidb_history_segments_purged_total{reason}` | qual das duas políticas está decidindo |
| `nishidb_history_retention_running` | o motor está de pé? |

`nishidb_history_oldest_written_at` é a que se olha primeiro: é ela que mostra o
`maxBytes` engolindo o `expireAfterWrite` declarado.

O `history.index.maxBytes` persegue uma parcela ESTRITAMENTE MENOR do que a
série `nishidb_history_index_bytes` publica: o teto governa só os `.sidx`
selados — a única parcela que a expulsão da janela sabe devolver —, e a série
soma também `.sidx.tmp`, `.widx`, `indexes/` e `checkpoint/`. A série acima do
teto é o estado normal, e não sinal de que o teto não funciona. É o mesmo
princípio que já mantém o `locations.idx` fora do `history.maxBytes`: um teto que
a purga não consegue perseguir seria um teto que mente.

`nishidb_history_index_window_seconds` é a irmã dela do lado do índice, e faz o
mesmo serviço para o par `history.index.window` / `history.index.maxBytes`:
comparada com o `window` declarado, ela diz se o teto de bytes assumiu o
comando. Ela é o único observável em que aparece o conservadorismo da janela, e a
consequência dele é uma cobertura menor que a que o teto declarado sugeriria.

O conservadorismo tem uma causa precisa, e ela vale escrever porque a
simplificação óbvia reintroduz um defeito. O teto tem de dar um preço ao segmento
que **ainda não tem índice**; cobrar zero o faria caber sempre, entrar na fila,
ser reconstruído e ser expulso na passada seguinte — um moinho que se alimenta
sozinho. O preço cobrado é

    max( maior .sidx em disco,  o que ESTE segmento custou ao ser expulso )

e as **duas** parcelas são necessárias. Só a primeira **não é monótona**: a
própria expulsão apaga arquivos, então apagar o maior BAIXA a estimativa, e quem
ela passa a subestimar é exatamente o segmento que acabou de sair — ele volta a
caber, é reconstruído, estoura o teto de novo e é expulso de novo. A segunda
parcela (`Store.sizeExpulso`) é gravada em memória no instante do `unlink`;
para o caso do moinho ela não é estimativa nenhuma, é o tamanho que o arquivo de
fato teve. Ela não é persistida — um metadado a menos é uma janela de crash a
menos —, e o preço é uma reconstrução desperdiçada por segmento atípico depois
de um restart.

Sem nada medido (dataset migrado, nenhum `.sidx` em disco) o teto admite **só o
segmento mais novo**: admitir todos mandaria a reconstrução varrer o dataset
inteiro antes de o primeiro corte acontecer.

Os bytes de índice são medidos em **blocos** (`st_blocks`), e não pelo tamanho
aparente. A diferença não é cosmética: a purga libera o prefixo morto com
punch-hole, o que devolve blocos sem mudar o tamanho do arquivo. Somar `Size()`
mostraria disco que já voltou ao dispositivo — a mesma classe de erro do débito
78, com o sinal trocado.

### Limites conhecidos

- **Não há backfill.** A tabela começa vazia no momento em que é ligada, e não
  há como preenchê-la: o base guarda estado mergeado, não os eventos. Ligar a
  chave **não** exige recriar o dataset, que é a diferença em relação às
  congeladas (débito 83).
- **`_writtenAt` é o instante da INGESTÃO**, não o do evento. Num
  reprocessamento do tópico, o histórico inteiro nasce com a data do replay.
- **A irmã não abre sozinha.** Ela usa o dicionário do base, então quem a abrir
  precisa abrir o base primeiro — e é por isso que ela não aparece em
  `datasets.directory`.
- **`_raw` dobra o disco** em eventos JSON grandes. Por isso o default é
  `false`.

## Referência de tipos de campo

O que `type:` aceita no YAML do dataset, o que cada um aceita como **entrada** e
onde pode ser usado. Sem esta tabela, escrever um YAML novo é tentativa e erro.

| `type:` | Entrada aceita no evento | `index: true` |
|---|---|:---:|
| `string` | string JSON, e ela precisa ser **UTF-8 válido**; **número e `true`/`false` também entram**, gravados como o **texto-fonte** (`1.50` → `"1.50"`, `007` → `"007"`, `true` → `"true"`) — não há normalização e nada se perde. **Object e array continuam recusados** | sim |
| `int64` | número inteiro (sem `.`, `e` nem `E`), `{"$numberLong":"<dígitos>"}` **ou string de dígitos** — `-?[0-9]+`, sem espaço em volta, sem sinal `+`, sem fração e sem expoente; zero à esquerda é aceito (`"007"` → 7) e estouro do range de `int64` é **recusado** | sim |
| `double` | número JSON **ou string com a mesma gramática de número JSON** (opcionalmente com fração e expoente). São recusados `NaN`, `Inf`, hexadecimal, separador `_`, sinal `+`, zero à esquerda (`"01"`), ponto sem dígito (`"1."`) e espaço em volta | não |
| `boolean` | `true`/`false` JSON — a **string** `"true"` não serve | sim |
| `timestamp` | **RFC3339(Nano)** com fuso **obrigatório** (`Z` ou `±hh:mm`) e fração **truncada em milissegundos**; **número = epoch millis** (inteiro); `{"$date":"<rfc3339>"}` e `{"$date":{"$numberLong":"<ms>"}}` | não |
| `ipv4` | string `a.b.c.d`, octeto decimal **sem zero à esquerda** e sem espaço em volta | sim |
| `ipv6` | string RFC 4291, inclusive a forma comprimida e `::ffff:a.b.c.d`; endereço **v4 puro é recusado** (`10.0.0.1` não vira v4-mapped), e zona (`%eth0`) também | sim |
| `ipv4_prefix` | string `a.b.c.d/len`, com os **host bits preservados** — `10.0.0.1/24` é gravado como `10.0.0.1/24`, não mascarado | não |
| `ipv6_prefix` | string `addr/len`, mesmas regras do `ipv4_prefix`; prefixo v4 é recusado, e prefixo v6 é recusado no `ipv4_prefix` | não |
| `bytes` | string **base64** (`StdEncoding`: alfabeto padrão, com padding). base64url é recusado | não |
| `json` | **só object ou array** (guardado cru, exatamente como veio); escalar — string, número, `true`/`false` — é **recusado** | não |
| `datetime` | string no **`format:` declarado** (obrigatório), **sem fuso** — o instante é resolvido pelo fuso do **host**; **número = epoch millis**, como no `timestamp` | não |
| `time` | string no **`format:` declarado** (obrigatório): hora do dia, sem data e sem fuso; **número = millis desde a meia-noite**, e fora de `[0, 86400000)` é recusado | não |
| `auto` | qualquer entrada, mas o tipo inferido sai de um conjunto **restrito**: string→`string`, número inteiro→`int64`, número com fração ou expoente→`double`, `true`/`false`→`boolean`, object/array→`json`, `$numberLong`→`int64`, `$date`→`timestamp`. **Nunca** infere `ipv4`, `ipv6`, prefixo ou `bytes` — uma string com cara de IP vira `string`. O tipo é **fixado na primeira observação** e persistido | não |

Restrições da v1: a **key** (`materialization.key.type`) só pode ser `string`, e
a **sequence** (`materialization.sequence.type`) só pode ser `int64`. Campo
`auto` não é indexável — o tipo só existe depois do primeiro evento. Alias é
opcional e precisa casar `[a-z0-9_]+`.

### `datetime` e `time`: o `format:` é declarado, e o fuso vem do host

`timestamp` cobre origem que manda RFC3339 com offset. `datetime` e `time`
existem para a origem que manda **wall-clock sem fuso** — o caso do piloto
`events`, cujo `firstSeen`/`lastSeen` chega como `Aug 22, 2026 08:54:02 PM`.

O `format:` é **obrigatório** nos dois e usa um vocabulário **fechado** de
tokens, validado no load:

| Token | Significado | | Token | Significado |
|---|---|---|---|---|
| `yyyy` | ano, 4 dígitos | | `hh` / `h` | hora 12h, 2 / 1-2 dígitos |
| `MM` / `M` | mês numérico, 2 / 1-2 dígitos | | `mm` | minuto |
| `MMM` | mês abreviado en_US (`Jan`..`Dec`) | | `ss` | segundo |
| `dd` / `d` | dia, 2 / 1-2 dígitos | | `SSS` | milissegundos |
| `HH` / `H` | hora 24h, 2 / 1-2 dígitos | | `a` | `AM`/`PM` |

Texto fixo com letras precisa de aspas simples (`yyyy-MM-dd'T'HH:mm:ss`), como
no CLDR: letra nua é recusada para que um token digitado errado não vire texto
fixo em silêncio. O piloto declara `format: "MMM d, yyyy hh:mm:ss a"`.

```yaml
    firstSeen:
      type: datetime
      format: "MMM d, yyyy hh:mm:ss a"
```

**A forma de largura fixa é estrita, a variável não.** `dd` exige dois dígitos;
`d` aceita um ou dois. Como o padrão CLDR do produtor pode não ser
zero-padded, declarar as formas variáveis (`d`, `h`) é a escolha tolerante — e
é a recomendada quando a grafia da origem foi vista numa amostra só, porque um
texto que o `format:` não aceita é erro de conversão, que sob `log-and-skip`
descarta o evento inteiro.

**O fuso vem do host.** Um wall-clock sem offset não tem instante até alguém
escolher um fuso, e a escolha é o `time.Local` do processo, honrando a env
`TZ`. Vale igual para a ingestão e para o literal SQL, e é isso que os mantém
concordando sobre o mesmo texto. A consequência a conhecer: **o mesmo evento
reingerido num host com outro `TZ` produz outro instante**, sem erro e sem
log. Quem precisa de instante independente de ambiente pede offset à origem e
declara o campo como `timestamp`.

**Horário de verão.** Hora ambígua (a que acontece duas vezes, no recuo do
relógio) fica na primeira ocorrência; hora inexistente (a pulada, no
adiantamento) normaliza para frente. Recusar as duas transformaria uma hora
por ano de saída legítima em descarte silencioso; o que importa é que
ingestão e query cheguem ao **mesmo** número para o mesmo texto
(`TestHoraAmbiguaDeDSTEhResolvidaDeFormaDeterministica`). A validade da
**data** é conferida em UTC justamente para não recusar data válida em zona
cuja virada desloca a meia-noite
(`TestDataValidaNaViradaDeDSTNaoEhRecusada`).

**O literal SQL usa o mesmo `format:` do campo, não RFC3339.** A propriedade
que vale é *o texto que o produtor mandou, escrito como literal, acha a row*
(`TestIngestaoEQueryConcordamPorTipo`). Uma grafia diferente é **erro de
bind**, nunca resultado vazio — vazio pareceria resposta
(`TestLiteralNoFormatoErradoNaoAchaEmSilencio`). Quem quer grafia independente
de formato escreve o número: epoch millis em `datetime`, millis do dia em
`time`.

```sql
SELECT identifier, lastSeen FROM events
 WHERE lastSeen >= 'Aug 22, 2026 08:00:00 PM' LIMIT 20
```

**`datetime` não faz round-trip textual.** Entra `Aug 22, 2026 08:54:02 PM` e
sai `"2026-08-22T23:54:02.000Z"` (com `TZ=America/Sao_Paulo`): o texto de
relógio de parede foi consumido para resolver o instante e não é preservado.
Renderizar em horário local faria o mesmo dataset servido de dois hosts
produzir bytes diferentes. `time` sai como `"20:54:02.000"`, **sem `Z`** —
não há instante, e anunciar fuso o faria parecer um timestamp de 1970.

**Nenhum dos dois é indexável, e nenhum é inferido por `auto`.** O bitmap é
denso: um campo `time` com resolução de segundo tem 86.400 valores distintos.
E `auto` não tem `format:` declarado, então uma string com cara de data
continua `string` — a mesma regra que já vale para IP.

**Por que `int64` e `double` aceitam string.** É ajuste pós-amostra, não
leniência: a fonte real manda `Connect-Info` como `"1000000000"`. A coerção é
estreita de propósito — a string precisa casar a mesma gramática que o número
casaria, então `"1e3"` vira 1000 num `double` e é recusado num `int64`, e
`"NaN"` não entra em lugar nenhum (`TestCoerceDeclaredStringToNumeric`).
Recusa é erro de conversão, e no §101 erro de conversão descarta o **evento
inteiro**.

**E por que `string` aceita número e boolean.** É a outra metade da mesma
simetria, e a regra que governa as duas é uma só: **converte-se quando a
conversão é sem perda**. Para número e boolean o `Raw` do parser é o
**texto-fonte** — os dígitos como a origem os escreveu, e o literal `true`/`false`
—, então gravá-lo numa coluna de texto não altera um byte: `{"x": 1.50}` vira
`"1.50"`, com o zero à direita preservado, e `{"x": 007}` vira `"007"`
(`TestNumeroEmColunaDeTextoGravaOTextoFonte`,
`TestBooleanEmColunaDeTextoGravaOLiteral`). Antes disto o valor era **recusado**:
a coluna ficava nula e o dado só sobrevivia dentro do documento, fora do alcance
de qualquer `WHERE`. Perder o dado é pior que preservá-lo noutra forma.

**A coerção recupera o VALOR e não unifica a CONSULTA.** É o limite da mudança e
precisa estar escrito, porque o motivo de a coluna existir é filtrar: uma coluna
`string` que recebe `"07"` de um evento e `7` de outro passa a guardar `"07"` e
`"7"` — **duas strings distintas para a mesma coisa**. A coluna fica completa e
continua **não filtrável por um único literal**. Isso é honesto (os dois valores
ERAM diferentes na origem, e inventar uma normalização seria gravar um valor que
ninguém mandou), mas quem lê esta página precisa saber. É para MEDIR esse caso
que existe a coluna `coercions` do `_meta.fields`.

**O que a simetria NÃO alcança, nas duas direções.** `int64` continua recusando
`5.7`: truncar em `5` gravaria um valor que ninguém mandou, a soma bateria
diferente e o filtro por igualdade encontraria a linha errada, sem nada no
caminho acusando — perder o valor é ruim, inventar outro é pior. E `string`
continua recusando **object e array**: enfiar um documento inteiro numa coluna de
texto destrói a razão de ela existir, que é ser comparável, e faria a coluna
misturar escalar com estrutura sem nada distinguindo os dois na leitura. Quem
quer o documento declara `type: json`
(`TestObjetoEArrayContinuamRecusadosEmColunaDeTexto`).

**O `json` é mais estreito do que o nome sugere, e isso custa evento.** Ele
aceita **apenas** object e array (`internal/materializer/typeconv.go`,
`TestConvertJSON`), e ainda exige que a janela seja JSON bem-formado e UTF-8
válido — a validade é conferida na porta de armazenamento porque o valor sai
verbatim no NDJSON. Qualquer escalar num campo declarado `json` é erro de
conversão, e no §101 erro de conversão **descarta o evento inteiro**, não o
campo: o resto do patch nem chega ao merge. A perda não é silenciosa (o resumo
do `ingest` mostra `TYPE_CONVERSION=N`), mas é perda. `null` continua sendo
ausência — remove o campo, como em qualquer outro tipo.

Trocar `json` por `auto` **não** resolve campo de shape variável: o `auto`
também fixa o tipo na primeira observação, e a observação divergente vira o
mesmo descarte (`TestAutoSegundaObservacaoDivergenteCoerceOuFalha`). Na v1 não
há tipo que aceite objeto e escalar no mesmo campo — se a fonte varia, o
tratamento é na origem.

## Observabilidade

**Log estruturado (§95).** `internal/logging` emite os campos literais da spec
— `timestamp level component dataset event message` — sobre `log/slog`, que
batiza `time`/`msg` e deixaria o log fora da spec por descuido de biblioteca.
A proibição de registrar senha e payload vive no HANDLER, e não na disciplina
de quem escreve a chamada: chave sensível tem o valor suprimido em qualquer
profundidade de grupo, e o tipo `Secret` cobre o caso oposto — chave inocente
com valor que não é. `event` é o identificador estável que alerta casa;
`message` é a prosa para humanos, e separá-los é o que permite mudar o texto
sem quebrar alerta.

**Métricas (§92, §93).** `internal/metrics` é um registry PULL com exposição
Prometheus escrita à mão. À mão porque o §97 exige binário único em
scratch/distroless e o projeto tem duas dependências; PULL porque os
contadores que já existem (materializer, executor, planner, `Store.Stats`) não
são copiados e sim lidos no scrape — duplicar o estado criaria um segundo
contador com o mesmo nome de spec, que é bug de observabilidade. Os nomes do
§92 ficam em constantes, num lugar só: dashboard e alerta casam string, e um
nome digitado à mão em dois lugares diverge no dia em que um deles é
corrigido.

**O inventário das 91 séries está em [`metricas.md`](metricas.md)** — por
subsistema, com tipo, labels e o `# HELP` real de cada uma, mais as três
classes de ausência: a série que depende de uma feature não declarada, a que
só nasce no primeiro evento e a que não é emitida por ninguém. Duas cobranças
o mantêm honesto: `TestTodaMetricaDeclaradaEstaDocumentada` exige que toda
constante de `names.go` apareça lá, e `TestODocDeMetricasNaoInventaSerie` o
lado inverso — série citada no documento que não existe no código é nome
renomeado que o operador levaria para um alerta que nunca dispara.

**As cinco séries de consulta do §92 nasceram com o protocolo.** Até a Fase 6
elas eram nome em `names.go` sem alimentador nenhum, e não por esquecimento: o
motor não sabe quantas consultas chegaram, quantas estão em voo nem quanto
tempo cada uma levou de ponta a ponta — ele conhece iteradores, não requisições.
`nishidb_query_full_scans` é a exceção que já tinha fonte (o contador normativo
do §64, no plan-time) e entrou como coletor pull, **sem** o label `dataset`:
`planner.Metrics` é um por Engine, compartilhado por todos os datasets, e um
label que o dado não sustenta é pior que label nenhum. Pela mesma leitura de
`Store.Stats()` que o `/v1/stats` faz, `nishidb_materialized_rows` e
`nishidb_segments_total` também ganharam alimentador.

**As cinco séries de disco e compaction do §92 nasceram com o M4.**
`nishidb_storage_bytes` é soma de **tamanho de arquivo** e não herda a
imprecisão do tally; `nishidb_storage_garbage_bytes` é `total − vivo` e sai
publicada como **limite INFERIOR**, com a semântica escrita na `Ajuda` da série
— a liveness é limite superior (débitos 38 e 45), logo "há **pelo menos** isto
de lixo" é a única afirmação verdadeira que se pode fazer com esses dois
números, e ela já é acionável. `nishidb_compaction_running` é 0/1 do **laço**
por dataset, não da varredura em voo: a passada dura dezenas de milissegundos
contra um cochilo de segundos, de modo que um medidor de "varredura agora"
responderia 0 em quase toda raspagem e o painel não distinguiria a compaction
viva da desligada. `nishidb_compaction_bytes_read` e `_bytes_written` são
**contadores** monotônicos por processo, e a distância entre eles é o disco
devolvido — contadores, e não medidores, porque o painel pede a TAXA de bytes
movidos, e taxa não se calcula sobre um número que zera a cada passada.

**As seis séries do índice por segmento** existem porque o terceiro dos três
defeitos que aquele subsistema corrige é *"nada avisa o operador"*. Os outros
dois são de mecanismo e o desenho novo os torna inexprimíveis; este só some se
alguém publicar os números. Elas saem em **todo** dataset, materializado e
histórico, porque o motor de índice é um só nos dois modos.

| Série | Responde |
|---|---|
| `nishidb_index_segmentos{estado}` | quantos segmentos estão em cada estado de índice |
| `nishidb_index_bytes` | o disco do índice por segmento — o número que precisa parar de crescer |
| `nishidb_index_reconstrucoes_total` | a fila está drenando? |
| `nishidb_index_reconstrucao_ativa` | o laço está de pé? Ele para de vez na primeira falha |
| `nishidb_index_crc_falhas_total` | quantos `.sidx` já foram reprovados no crc32c desde a subida |
| `nishidb_index_segmentos_consultados_total{modo}` | quantos segmentos responderam por índice e quantos por enumeração |

`estado` tem **quatro** valores e a partição é o produto inteiro da série:
`selado` (o `.sidx` cobre o conjunto indexado), `corrente` (o índice é o WIP),
`pendente` (dentro da janela e na fila) e `fora` (a janela não o cobre, e ele
responde por enumeração **de propósito**). Um rótulo só para as duas ausências
somaria, num dataset grande, a maioria esmagadora dos segmentos com a fila de
trabalho — o número seria enorme, normal e constante, e o gauge deixaria de
responder *"o laço está avançando?"*, que é a única pergunta que uma migração
faz.

`nishidb_index_crc_falhas_total` é **degrau, não taxa**: o `garanteSealed` publica
no cache o mapeamento reprovado — publicar mantém o `sync.Once` do crc cumprido, e
recusar obrigaria toda consulta a remapear e revarrer o arquivo podre inteiro —,
então cada arquivo soma uma vez por processo. Um alerta por `rate()` sobre ela dá
um pico e se auto-resolve em minutos, enquanto o segmento continua enumerando.

**E nenhuma série responde se ainda há índice podre**, por duas razões que se
somam. O gauge `nishidb_index_segmentos` é **cego** à corrupção: os quatro estados
saem idênticos entre um dataset saudável e um com `.sidx` podres, porque o arquivo
reprovado continua contado como `selado` — a fila é derivada da tabela de seções e
ela não confere crc. E a enumeração **sobe sem atribuir causa**: o incremento é o
mesmo que um segmento fora da janela ou na fila produz, no mesmo balde e sem
rótulo que os separe. O sinal disponível é o WARN `indice.selado_corrompido` no
log, e a lacuna é o **débito 103**. Fica escrito em vez de presumido, pela mesma
razão que o `durabilizaODiretorio` registra o que nenhum teste deste repositório
afirma. Medido em `TestOGaugeDeEstadosEhCegoAIndicePodre`.

`nishidb_index_segmentos_consultados_total{modo}` é o **único** observável da
degradação: a fonte de enumeração é apenas mais um iterador ascendente na mesma
combinação, e planner, executor e driver não têm como distingui-la. Sem essa
série, um dataset inteiro respondendo por enumeração é indistinguível de um
dataset inteiro indexado — a mesma resposta, com outro custo.

`nishidb_index_bytes` é parcela **estrita** de `nishidb_history_index_bytes`,
que soma também `indexes/` e `checkpoint/`, e as duas saem da mesma função de
disco — uma definição, dois leitores. **Mas elas não se comparam no mesmo
rótulo**: a segunda é do bloco `history`, descreve a tabela IRMÃ e sai rotulada
pelo dataset BASE (é lá que a política foi declarada), enquanto a primeira
descreve o Store em que sai. Num par `X`/`X_history` a parcela vive em
`{dataset="X_history"}` e o total em `{dataset="X"}`, então
`nishidb_history_index_bytes - nishidb_index_bytes` — que casa por rótulo
idêntico, o default do PromQL — subtrai a série errada: no rótulo do base ela
descreve o índice do PRÓPRIO base, que é zero quando ele não indexa nada e um
número sem relação com o total quando indexa. A **janela efetiva não está nesta
família**: quem a publica é `nishidb_history_index_window_seconds`, rotulada
pelo dataset BASE. A razão é que a janela é *política* declarada num YAML, e a
série existe espelhando `nishidb_history_oldest_written_at`, que é do mesmo
bloco; esta família descreve a *mecânica* de índice de cada Store e sai também
na tabela irmã. Pôr a janela aqui deixaria uma série rotulada pelo base dentro
de uma família rotulada pela irmã.

O nome de dataset vindo do request **nunca** vira label: label alimentado por
entrada do cliente é uma série nova por requisição, e um cliente em loop
consumiria a memória do processo. Erro antes de o dataset ser resolvido é
contado sob o valor fixo `?`. Nas séries de índice a garantia é mais forte que
uma convenção: `estado` e `modo` são enumerações exportadas de `names.go`, o
coletor só pode alimentá-las percorrendo essas listas, e
`TestOsLabelsDeIndiceSaoConjuntosFechados` confere as duas coisas — o conjunto
que sai na porta e, pelo `go/ast`, a origem do valor no código.

**Cabeçalho por família.** Toda `metrics.Sample` precisa preencher `Family`,
e não só `Name`. O `WriteTo` agrupa `# HELP`/`# TYPE` por aquele campo, então
amostras com `Family` vazia caem num grupo só: o par sai uma vez, **com o nome
do metric vazio**, e as demais famílias chegam ao operador sem tipo e sem ajuda.
Nada disso muda um número — foi por isso que o bloco `nishidb_history_*`
conviveu com a omissão —, mas sem `# TYPE` o `rate()` não se aplica, e metade do
contrato de várias séries viaja no texto da ajuda.
`TestOsCabecalhosSaemNomeadosParaCadaFamilia` cobre o bloco do histórico e o do
índice.

**`/v1/stats` não duplica contador.** Ele lê as mesmas estruturas que o
`/metrics` lê, no instante da chamada. A divisão entre `process` e `datasets`
segue o que o dado sustenta: os contadores do executor e do planner são de
PROCESSO, e repeti-los dentro de cada dataset publicaria o mesmo número N vezes
sob N nomes diferentes.

Cinco séries são INVENTADAS, e cada uma corrige um silêncio:
`nishidb_ingest_failed_total` (o §92 só prevê o balde do evento ruim, e sem
esta a perda de dado é invisível num daemon, onde não há exit code para
carregá-la), `nishidb_ingest_blocked`, `nishidb_kafka_errors_total`,
`nishidb_kafka_queue_depth`/`_bytes` e
`nishidb_kafka_partitions_assigned`/`_total`.

**Health (§94).** `GET /v1/health` traz `status`, `kafka`, `storage` e
`datasets`. A chave `kafka` AGREGA: basta um dataset com ingestão parada para
ela sair de `UP`, e o corpo nomeia qual — num processo que hospeda vários
datasets (§82), `DOWN` sozinho manda o operador procurar no log. Responde 503
quando o status não é `UP`, para que um balanceador tire a instância de
rotação sem interpretar o corpo.

**pprof (§112).** `nishidb server --pprof <addr>` sobe os endpoints de debug
num listener PRÓPRIO. Separado de propósito: o §112 os autoriza apenas em modo
administrativo, e expô-los na porta das métricas os tornaria alcançáveis por
quem só deveria raspar `/metrics`.

## Contadores mantidos na escrita

![Contador mantido na escrita: os quatro momentos, o checkpoint, a abertura e a auditoria](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/contadores_flow.puml)

**O que é.** Um contador declarado no YAML (`counters:`) responde "quantas
entidades vivas satisfazem uma condição, agrupadas por uma chave" sem varrer o
dataset — a leitura é `SELECT … FROM _counters.<nome>`. É um índice derivado no
sentido exato da agenda de vencimentos: alimentado no `applyRecord`,
reconstruível dos records, fora da durabilidade do §6.1. A proposta que o
originou está em
[`planning/contadores-mantidos-na-escrita.md`](../planning/contadores-mantidos-na-escrita.md);
o que a leitura do código mudou nela está no plano ao lado
([`planning/contadores-mantidos-na-escrita-plano.md`](../planning/contadores-mantidos-na-escrita-plano.md)),
e o aceite em [`aceite-contadores.md`](aceite-contadores.md).

**Conta estados, não mensagens.** Somar por mensagem exige que cada mensagem
chegue exatamente uma vez, e o Kafka promete pelo menos uma — é o defeito que
o `merge: sum` carrega por contrato. O contador é alimentado pela
**transição** do estado da linha: do que ela era para o que passou a ser.
Reprocessar o mesmo evento é uma transição de X para X, ajuste zero; mudar de
OLT desconta da antiga e soma na nova na mesma operação, sem ninguém avisar
que houve mudança.

**Três peças por contador**, em `indexes/contadores/<nome>/`. O vetor denso
`grupos.idx` guarda por rowId `bit31 = condição valia | bits 0..30 = id do
grupo` — quatro bytes por linha (`rowindex.Dense32`, o primeiro vetor de 32
bits do produto: 32 MB por 8M linhas, e o contador pode existir várias vezes
sobre o mesmo dataset). O dicionário `grupos.dict` interna a chave composta do
grupo → id, append-only com CRC por registro e **fsync antes de devolver o id**
(cópia do `fielddict`, pela mesma invariante: nenhuma entrada do vetor pode
apontar para um id que a reabertura não resolva); aposentar um grupo é um
registro nele. Os totais e os `refs` (quantas linhas apontam para o grupo,
independente da condição) vivem em heap, atômicos, e **não são persistidos**.
A chave composta nasce no `valuekey`, dono único da chave canônica: `u16
tamanho | chave simples` por componente, com golden de bytes.

**Por que os totais são dobra, e o delta vem do vetor.** No replay da
recovery o estado anterior da linha em geral **não existe** (`prevPayload` é
nil quando a location já está à frente do cursor, o caso típico de crash).
Um contador mantido por delta sobre o payload antigo convergiria para um
número errado — permanentemente, como o resíduo de liveness do débito 38. A
saída: na abertura os totais são **dobra** do vetor em disco, antes do replay;
e cada transição aplica `delta = f(nova) − f(vetor[rowId])`, com a entrada
antiga lida do próprio vetor. Assim `totais == fold(vetor)` vale em qualquer
instante, e o replay reaplicando uma transição já refletida no vetor vê
`antiga == nova` e não mexe em nada.

**Por que o vetor tem overlay.** Escrito em place no mmap, o vetor ficaria à
frente do log durável numa cauda rasgada — a página descarregada pelo kernel
descreveria um record que o crash levou. `escritas`, `prazos` e a agenda
aceitam essa classe de resíduo porque ela é benigna na leitura; no contador
seria número publicado errado. Então a transição vai para um overlay em heap,
e só o `ApplyOverlay` do checkpoint, **depois do fdatasync dos segmentos**, a
leva ao mmap — a mesma invariante das locations.

**Os quatro momentos.** Um evento chega: passo 7c do `applyRecord`, depois do
commit da location e ao lado da agenda, nos dois modos (write e replay). Uma
linha expira: a lápide é um record e passa pelo mesmo 7c com "sem grupo" — a
entrada antiga é quem lembra de onde a linha saiu; a expiração oportunista da
compaction entra pela mesma porta. A retenção remove a lápide: no dataset
materializado ela nunca some (o copy-forward a leva adiante), então não há
terceiro decremento a esquecer; a tabela histórica purga por marca-d'água sem
`applyRecord`, e por isso **recusa `counters:`** na subida. O processo sobe de
novo: dobra, replay, e o contador está no ponto certo.

**Adotar ou reconstruir.** O manifest grava por contador, numa seção
**ignorável** (`0x0001`), o que decide o conteúdo do vetor: nome, colunas e o
texto do `where`. Seção ausente, colunas ou `where` diferentes, ou vetor que
não cobre as linhas: reconstrói, recriando os **dois** arquivos — chaves de
outras colunas não podem poluir um dicionário cujos ids nunca são reemitidos.
A reconstrução é uma varredura das linhas vivas, logada com motivo e duração,
e é o que faz "declarar um contador num dataset povoado" ser um reinício e não
uma recriação. Mudar `maxGroups` não reconstrói; só é recusado quando menor
que os grupos em uso. A tag é ignorável de propósito: "seção ausente ⇒
reconstruir" já é a resposta certa, e uma tag crítica só bloquearia o rollback
do piloto.

**O ciclo de vida de um grupo.** Um grupo com `total == 0` **não** está vazio:
500 serviços não autenticados numa OLT dão total zero e 500 entradas apontando
para ela. Apagá-lo e reemitir o id faria o próximo incremento daquelas linhas
cair no equipamento errado, sem sintoma. A regra tem três cláusulas:
`refs == 0`, vazio há mais que `retainEmptyGroups` (é a visibilidade do
colapso — o equipamento que apagou por inteiro continua com total zero na
tabela até alguém ter olhado), e **a última mexida do grupo coberta por um
checkpoint** — a transição que o esvaziou pode estar numa cauda rasgada, e
nesse caso o estado durável ainda diz "a linha está aqui". Aposentar é registro
durável, o id nunca é reemitido, e o reinício só estende a visibilidade (o
instante em que o grupo esvaziou não é persistido).

**A tabela virtual.** `_counters.<nome>` é um schema sintético compilado do
mesmo YAML de um dataset de verdade, como o `_meta`, com a key `groupKey` (o
texto do grupo — a key da v1 é uma coluna string única, e um
`groupBy` composto não tem coluna que identifique a linha), as colunas do
`groupBy` abertas e tipadas, e `total`. Diferente do `_meta`, ela é
**registrada no motor** sob o dataset e corre pelo pipeline inteiro — planner
com métricas, top-N, orçamento —, porque `ORDER BY total ASC LIMIT 5` sobre
milhares de grupos é exatamente a consulta do caso de uso. Os totais são lidos
no presente; o que o snapshot congela é o conjunto de grupos. A lista dos
contadores sai pelo mesmo caminho: `_meta.counters` é uma tabela virtual
registrada sob o dataset — uma linha por contador, com a definição (`name`,
`groupBy`, `condition`) e o retrato vivo de `EstatisticasOfCounters`
(`rows`, `total`, `groups`, `maxGroups`, `retired`, `refused`, `orphans`,
`bytes`, `auditDivergent`, `auditAt`, os dois últimos NULL até a primeira
auditoria), mais `keyFormat`, o molde efetivo da key.

**O texto do grupo é derivado, nunca guardado.** O que vai a disco é a chave
composta; `groupKey` nasce em `internal/keyformat` na abertura, para todos os
grupos, e a cada grupo novo — a tabela depois lê o texto pronto, e não
renderiza por linha. Duas consequências caem daí. A primeira é que trocar o
molde (`keyFormat` no bloco `counters:`) **não reconstrói** o contador: ele
fica fora do manifest e da comparação que decide adotar ou reconstruir, pelo
mesmo critério do `maxGroups` — decide como o grupo é escrito, não o que foi
contado. Quem avisa que mudou é o log de subida, que sai com o molde efetivo.
A segunda é que a não-ambiguidade é responsabilidade do molde: `groupKey` é a
key, e a busca por ela tem de identificar UM grupo. As recusas de
`keyformat.Parse` (toda coluna do `groupBy` exatamente uma vez, nenhum par de
marcadores colados, nenhum dígito ou `-` logo depois de um `int64`, nenhum
`\` em literal) mais o escape do primeiro caractere do literal seguinte fazem
o texto ter uma leitura única da esquerda para a direita. Embaixo disso há uma
rede: um texto que colidisse com o de outro grupo vivo é recusado ANTES do
`Intern`, porque o dicionário fsynca a chave antes de devolver o id e o id
nunca é reemitido — uma chave colidente que chegasse ao disco faria toda
abertura seguinte falhar.
Ela mora em `internal/counters`, não no pacote `meta`, porque o `meta`
descreve o dataset pela definição estática e não conhece o storage; o motor
manda `_meta.fields` para o `meta` e qualquer outra tabela do `_meta` para o
registro. Existe em todo dataset, vazia quando não há `counters:`.

**Os agregados do contador.** `rows` e `total` são a soma dos `refs` e a soma
dos `totais` de todos os grupos, mantidas no próprio contador como dois
atômicos ajustados dentro do `Transition` — o delta já é conhecido ali e é
sempre ±1. Somá-los na leitura custaria O(grupos) por linha de `_meta.counters`
e criaria uma segunda maneira de calcular o mesmo fato. Eles são a MESMA dobra
do vetor que os totais por grupo, acumulada no mesmo laço, então nada de novo
vai a disco e a consistência através de crash vale para eles inteira. A
aposentadoria não os toca: ela só aposenta com `refs == 0`, e
`totais[g] <= refs[g]` por construção, logo o grupo que sai contribuía zero
para os dois — o que um teste trava, porque descontar ao aposentar é o
"conserto" que qualquer um escreveria. Os mesmos dois números saem em
`/v1/stats`, no `nishidb counters` e nas séries `nishidb_counter_rows` e
`nishidb_counter_counted_rows` (medidores: descem quando uma linha é lapidada
ou vence, e por isso não levam o sufixo `_total`, que é de contador
monotônico).

**A auditoria.** `POST /v1/counters/audit` (ADMIN) congela os totais e abre o
snapshot **no mesmo instante, sob o `wmu` por microssegundos** — os dois
agregados vêm nesse mesmo retrato, porque é a coincidência de instante que ela
compara —, e varre sem o writer. Ela também é o que reconcilia os agregados
com os vetores por grupo, conferindo-os contra o RECALCULADO e contando essas
divergências à parte (`TestAuditoriaAcusaOAgregadoContraORecalculado`): uma
linha que trocou de grupo por deriva deixa os grupos errados e o agregado
certo, e somar as duas contagens esconderia a distinção. A varredura tem enumerador próprio, com a definição do contador —
linha com location e sem lápide na versão T0, pelo journal de versões —, e
não a visibilidade da consulta: `ScanLive` lê a location corrente e filtra por
prazo vencido, e reportaria como divergência a linha vencida que o coletor
ainda não lapidou (contada por desenho até ele chegar). Snapshot estourado é
**inconclusiva**, nunca relatório. A reconstrução (`/v1/counters/rebuild`)
segura o writer pela varredura, e o custo é dito.

**O que custa, medido** (bancada de 04/09/2026,
[`aceite-contadores.md`](aceite-contadores.md), saída crua em
[`contadores-20260904.txt`](../benchmark/baselines/contadores-20260904.txt)).
Em regime — grupos internados, linha no overlay — dois contadores custam
**+0,5 µs por evento** (3,99 → 4,51 µs, +13 %, n = 6), com **zero alocação**
nos dois lados; o gate `TestPutNaoAlocaMaisComContadores` trava que o `Put`
real não aloca mais com contadores do que sem (1,0 nos dois). A carga fria é
outro número: cada grupo novo paga o fsync do dicionário, ≈ 0,6 ms — 600
grupos em 366 ms na bancada. A dobra de um milhão de linhas custa **38–52 ms**
e fica no caminho do `Open`; a reconstrução em operação varre um milhão de
linhas vivas em **0,8 s por contador** com o writer parado, e a reconstrução
por mudança de parâmetro recria o dicionário e paga o fsync por grupo de novo
(265 grupos em 299 ms no ponta a ponta).

**Limites conhecidos.** O fsync por grupo novo é bounded pelo inventário, mas
no nível de PON ou splitter (10⁵ a 10⁶ grupos) o custo da carga inicial não
foi medido (débito 108). `_counters` fica fora do isolamento de snapshot
(débito 109), e `WHERE olt = …` num contador composto varre os grupos em vez
de usar índice (débito 110).

## Alarmes e emissão periódica de contador

Os contadores respondem quando perguntados. Estes dois subsistemas **empurram**:
o alarme avisa quando uma condição declarada se sustenta por um prazo, e a
emissão escreve o valor de cada grupo num arquivo, periodicamente. Os dois são
declarados DENTRO do contador, no YAML do dataset.

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    alarms:
      - name: olt_down
        when: "total < 100"          # a mesma linguagem do WHERE
        sustainedFor: 2m             # o prazo que separa alarme de rajada
        enrich:
          - name: servicos_afetados
            sql: "SELECT customerId FROM servicos WHERE olt = ? LIMIT 5"
            bind: [olt]
    emit:
      every: 30s                     # file, maxBytes e keep têm default
```

![Alarme por condição sustentada: a passada, a decisão e o que sai](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/alarmes_flow.puml)

**A condição não tem gramática nova.** A tabela `_counters.<nome>` já monta cada
grupo como um payload de record de verdade, e o `when` passa pelo avaliador
compartilhado de `internal/predicate` — o mesmo do `WHERE` de uma consulta e do
`when:` de uma regra de TTL. `total < 100` num alarme significa exatamente o que
significaria num `SELECT`, e não há uma segunda implementação de coerção para
divergir da primeira.

**A varredura não disputa nada com a ingestão.** Ler os grupos de um contador
toma o lock FOLHA do dicionário e lê atômicos; nada do `Store`. A exigência de
que a emissão não segure o writer sai satisfeita por construção, não por
cuidado. A passada tem **zero alocação por grupo saudável**, gateada por
`AllocsPerRun` — o estado em memória acompanha o problema, nunca o tamanho do
inventário.

**A cadência é o menor `sustainedFor` dividido por quatro**, com piso de um
segundo. É o cálculo do `watchdogInterval` do Kafka, pelo mesmo motivo: a
sustentação só é reconhecida entre DUAS observações, e com passada igual ao
prazo um alarme de dois minutos levaria até quatro para sair.

**Dispara uma vez, e avisa de novo quando passa.** O evento de resolução traz o
motivo, e os dois motivos são distintos de propósito: `condition_cleared` é "o
problema passou", `group_retired` é "o grupo saiu do contador". Equipamento
removido do inventário não é equipamento recuperado, e quem gera evento a partir
disso trata os dois de formas diferentes. Resolução sem disparo anterior nunca
sai — é o que faz toda rajada curta não emitir nada, em vez de anunciar um
"voltou" que ninguém viu abrir.

**O enriquecimento nunca barra o alarme.** As consultas declaradas rodam no
disparo, fora de qualquer lock, pelo MESMO motor que serve as consultas remotas
— mesmo binder, mesmo planner, mesmo orçamento. O valor do grupo viaja como
PARÂMETRO, nunca concatenado no texto: um nome de equipamento é dado vindo do
cliente, e um apóstrofo dentro dele reescreveria o `WHERE`. Falha, prazo
estourado ou teto da passada deixam o bloco vazio com o motivo, e o alarme sai
igual. O teto é de dezesseis consultas por passada, não configurável — ele
protege o processo de uma avalanche, não expressa política.

A coluna cujo NOME cai na supressão de log (`token`, `senha`, `value`, …) sai
redigida na montagem do bloco, e é obrigatório fazê-lo ali: a redação do handler
percorre atributos e grupos, mas não entra num valor já serializado, e o nome da
coluna vem do SQL que o operador escreveu. `logging.IsSensitiveKey` existe para
que haja uma lista só, e o teste dela usa o handler de verdade como oráculo.

**O reinício preserva o cronômetro.** O "desde quando" vai para
`checkpoint/alarms.state`, escrito nas transições pelo caminho de durabilidade do
manifest. Ele NÃO entra na ordem do passo 8 (§48): não descreve dado
materializado nem posição de consumo, e escrevê-lo antes ou depois só muda
quando um alarme dispara depois de um reinício. A identidade do grupo é a chave
CANÔNICA em bytes, não o texto do `groupKey` — o texto é derivado do
`keyFormat`, que pode mudar sem reconstruir o contador, e usá-lo como identidade
faria uma troca de molde descartar em silêncio todos os cronômetros. Ausência é
distinta de corrupção, e nenhuma das duas impede a abertura (§51): o que se
perde são os cronômetros, e cada alarme recomeça a contar.

Um alarme restaurado dispara marcado, com a **lacuna** no evento. Sem ela, um
disparo sobre um período que ninguém acompanhou seria indistinguível de um
observado de ponta a ponta.

O que a lacuna mede depende de como o processo saiu, e a distinção foi aprendida
no piloto: o encerramento gracioso GRAVA o estado, então o número é o tempo de
parada; num crash sobra o último save, feito na última transição, e o número
passa a ser "desde quando não há registro" — maior que a parada, e a resposta
honesta, porque a lacuna real é desconhecida. Antes de o encerramento gravar,
os dois casos se comportavam como o segundo, e um reinício de segundos depois de
nove minutos quietos anunciava nove minutos de lacuna.

**A emissão escreve uma linha por grupo vivo**, e o grupo com total ZERO sai —
é o equipamento que apagou por inteiro, e é justamente a linha que interessa num
gráfico: desaparecer da série viraria um buraco onde deve haver um zero. Os
valores usam o serializador canônico das respostas de consulta, então o `slot`
sai como o número 8, não como a string `"8"`. O arquivo fica em
`<dataset>/series/<file>`, com rotação por tamanho e sufixo numérico crescendo
para o passado — o teto de disco é `maxBytes × (keep+1)`, calculável por
multiplicação. Não há fsync: é telemetria, e um crash perder a cauda é o desfecho
certo, o mesmo argumento do spill do ORDER BY.

**Ordem de parada.** Os dois laços vivem no SERVIDOR, não no `Store`, porque o
enriquecimento precisa do motor de consulta — que o `Store` não conhece. Eles
leem os contadores sem a guarda de vida, então o `encerra()` os cancela e
AGUARDA antes do quiesce. A prova disso é um gate que lê a AST, e a escolha tem
medida por trás: subir com alarme e encerrar sob `-race` NÃO prova a ordem — a
passada sobre dezenas de grupos termina em microssegundos, o laço já está
dormindo no timer, e com a ordem invertida cinco rodadas passaram verdes.

**Observabilidade.** Nove séries para o alarme e seis para a emissão (ver
[`metricas.md`](metricas.md)), todas nascendo com zero. Falha e pulo de
enriquecimento são séries separadas porque pedem ações opostas. E a tabela
virtual `_meta.alarms` dá o detalhe que os agregados não dão:

```sql
SELECT groupKey, state, since, condition FROM _meta.alarms WHERE state = 'firing' ORDER BY since ASC
```

`state = 'pending'` é o alarme que ainda conta o prazo — "vai alarmar em
quarenta segundos" é informação que só existe antes do disparo.

**O que NÃO existe aqui.** O canal é o log estruturado; tópico e webhook entram
como outro `Sink` sobre o MESMO evento, e o ponto de extensão está pronto. A
condição fala de UM contador: a razão entre dois (autenticados ÷ serviços da
mesma OLT) não é expressável, e o desenho aninhado dentro do contador não
acomoda um alarme que cite dois — quando a razão entrar, precisará de lugar
próprio. `/v1/stats` não publica estes números; o coletor do `/metrics` é a
fonte única, e expô-los no documento do dataset sairá de lá.

## O que ainda não existe

**Tombstones por retenção** — a expiração por inatividade existe e está
descrita acima; o que não existe é o tombstone que devolveria o **disco** da
entidade expirada, que hoje some da consulta e continua nos segmentos — e o
**backfill da tabela histórica** (ela
começa vazia quando é ligada, e o dataset base não guarda os eventos que a
preencheriam). O **backfill do índice secundário** (débito 22) SAIU desta lista:
declarar um campo indexado sobre um dataset que já tem linhas deixou de ser
recusado, os segmentos antigos são agendados por (segmento, campo) e a goroutine
de reconstrução drena a fila sozinha, em todos os datasets. O que sobrou dali é
um caso só, e ele tem número próprio (débito 103): o `.sidx` reprovado no
`crc32c` não é reagendado, porque a fila lê a tabela de seções sem conferir crc —
o segmento responde por enumeração até alguém apagar o arquivo. Acrescentar
`extract` a um dataset com linhas continua sendo recusado, e ali a razão é outra,
que nenhuma reconstrução resolve: o valor extraído não existe nos records já
gravados, e não há de onde derivá-lo. **A compaction saiu desta lista no M4** e está
descrita acima; **autenticação e TLS saíram nesta fase (Fase 11)** e estão
descritas em "Segurança: autenticação e TLS" — o que falta ali é
operacional, não de mecanismo: o deploy no piloto com certificado e usuários
reais é a Fase C do ciclo, ainda não rodada (ver [`aceite-m5.md`](aceite-m5.md)).
O estado item a item está em [`aceite-m1.md`](aceite-m1.md),
[`aceite-m2.md`](aceite-m2.md), [`aceite-fase6.md`](aceite-fase6.md),
[`aceite-m4.md`](aceite-m4.md) e [`aceite-m5.md`](aceite-m5.md).

## Como os números foram obtidos

`benchmark/ingest-bench` sobre disco real, com `GOMEMLIMIT=512MiB`, corpus
determinístico e checkpoint periódico. O comando exato de reprodução está no
cabeçalho da baseline.
