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

**A recriação passa pelo MESMO `kafka.Abre` da subida.** O §49 põe o
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

![Compaction de segmento: eleição, varredura streaming, republicação condicional sob o mutex do writer, os quatro passos da aposentadoria e os dois pontos de reparo do Open](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/compaction_flow.puml)

Append-only sem compaction é disco que cresce para sempre: todo update apenda um
record novo e deixa o anterior como garbage. O §86 projeta **30-43 GB escritos
por dia** contra 5,6-8 GB de estado vivo a 500 ev/s — dois a três dias de disco,
e nada no M1 devolvia esse espaço.

**O desenho é copy-forward, e não "arquivo novo".** A varredura re-apenda o
record vivo pelo caminho **normal** de escrita, no segmento corrente. A
alternativa — montar um arquivo novo com ID próprio — esbarra em três
restrições que já existiam: o `checkHeader` amarra o `segmentID` ao nome do
arquivo, o `openNew` usa `O_EXCL`, e os IDs vêm de `segID+1` no writer. O
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
aposentadoria **fecha o descritor** (`Reader.FechaSegmento`) antes do unlink.
Antes disso, a compaction devolvia disco ao *listing* do diretório e nada ao
dispositivo, e os três instrumentos do projeto — `dirBytes`, `discoDosSegmentos`
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
de escrita — não a escrita sustentada (ver a ressalva logo abaixo da tabela):

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

`_meta.fields` responde "o que este dataset tem" — nome, tipo, se é
autodescoberto, índice declarado, índice físico, campo de origem e a
**densidade** de cada coluna derivada — como uma tabela qualquer:

```sql
SELECT name, type, occurrences, typeMismatches, coercions FROM _meta.fields
WHERE parent = 'persistent' AND typeMismatches > 0
```

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
`internal/protocolo` existe como pacote próprio em vez de morar dentro do
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
`internal/cli/credencial.go`: proxy do ambiente ignorado (o incidente do piloto
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

**O prazo tem UMA fonte.** O executor tinha um `timeoutPadrao` de 30s próprio, e
aplicava o `bq.Timeout` do dataset SEMPRE — inclusive por cima de um contexto que
já trazia deadline. Como o menor deadline vence, um servidor que tivesse
resolvido cinco minutos contra `query.maxTimeout` veria o prazo cortado de volta
para 30s, e a consulta morreria em TIMEOUT com o teto configurado dizendo que ela
podia. Hoje `executor.Open` só aplica o prazo do dataset quando o chamador NÃO
trouxe deadline, e recusa uma query ligada sem prazo com `ErrInvalidPlan` em vez
de cair num default próprio.

**A memória de consulta virou orçamento, e não mais uma chave sem leitor.**
`runtime.memoryTarget` e `runtime.query.maxBufferPerQuery` eram parseados e nunca
lidos (metade do débito 15). `internal/query/orcamento` é o teto: dois
`atomic.Int64`, sem mutex e sem alocação — o consumidor é chamado uma vez por
row, e o gate de alocação do executor exige igualdade exata entre 1.000 e 8.000
linhas. O `Global` recusa na ADMISSÃO (503 + `Retry-After`, ao lado do semáforo)
e o `PorQuery` recusa POR LINHA, dentro do writer. A reserva é do teto inteiro na
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
sem senha) fechou nesta fase com `internal/config/config_seguranca.go`
(forma no parse), `internal/server/seguranca.go` (ativação e a matriz de
recusa) e `internal/server/autenticacao.go` (o middleware e os papéis).

**O plano é duplo, e a razão é o piloto.** O listener EXPOSTO passa a exigir
TLS **e** autenticação em TODAS as rotas — `/v1/health` e `/metrics`
inclusive, não só `/v1/query` — porque uma exceção "métricas não precisam de
senha" seria uma porta sem tranca do mesmo jeito. Um segundo listener,
opcional (`server.loopback`, default desligado), serve o MESMO conjunto de
rotas sem TLS nem autenticação, só em `127.0.0.1`: é o que mantém painel,
script de atualização e cron locais funcionando sem credencial mesmo com o
listener principal fechado a cadeado. Os dois listeners compartilham a
mesma FUNÇÃO de definição de rota (`Servidor.montaMux`,
`internal/server/autenticacao.go`), chamada uma vez por listener — nunca
duas listas que poderiam divergir.

**A matriz de recusa do §77** decide se o servidor sobe, olhando bind, TLS e
autenticação juntos (`avaliaExposicaoEAutenticacao`,
`internal/server/seguranca.go`):

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
declarado): `AutenticacaoHabilitada()` falsa faz `comAutenticacao`
(`internal/server/autenticacao.go`) devolver o mux CRU como passthrough
puro, sem checar papel nenhum — `POST /v1/compact` e `POST /v1/checkpoint`
respondem sem credencial no MESMO listener que serve `/v1/query`. É
comportamento testado, não descuido: o caso de uso central do §96 é o
operador que desligou o laço automático de compaction e opera na mão (modo
manual), e a config que a maioria dos deploys usa hoje — sem
`server.authentication` nenhuma — não pode deixar essa rota morta.

**HTTP Basic sobre TLS**, não um esquema próprio — mapeia 1:1 para
`DriverManager` do JDBC (Fase 7) e para `basic_auth` de ferramentas de
scrape. A senha do request é hasheada (SHA-256) e comparada contra o digest
guardado com `subtle.ConstantTimeCompare` — os dois lados sempre 32 bytes,
para que a comparação em tempo constante não vaze o comprimento da senha
certa pela diferença de tamanho de slice. Usuário inexistente compara contra
um digest fantasma fixo (`credencialFantasma`), e a resposta de falha é
**uma função só** (`recusaAutenticacao`) para os três casos — header
malformado, usuário inexistente, senha errada —, sempre com a MESMA
mensagem e os MESMOS headers: o 401 é byte-idêntico entre "não existe" e
"senha errada", provado por comparação de `Header()` completo, não só de
duas chaves nomeadas.

**Sem lockout, com atraso fixo.** Não há contador por IP nem por usuário —
isso abriria a porta para auto-DoS contra um username conhecido. Em vez
disso, toda falha de autenticação paga o MESMO atraso fixo (~100ms,
`atrasoDeFalha`), antes de qualquer escrita de resposta, o que mitiga o
timing attack de enumeração sem criar um estado que um atacante possa usar
para trancar a conta de outra pessoa. O rate-limit por IP fica registrado
como débito (ver `debitos.md`).

**Papéis: READ e ADMIN, com ADMIN ⊇ READ** (`papelAutorizado`,
`internal/server/autenticacao.go`) — um usuário ADMIN alcança toda rota READ;
um usuário READ nunca alcança rota ADMIN. O enum é fechado desde o parse
(`config.RoleRead`/`config.RoleAdmin`) e comparado por string EXATA: `role:
read` ou `role: Read` no YAML são recusados na subida, não normalizados.

**O registro de rota é fail-closed por construção.** `roteador.registra` é a
ÚNICA função que grava o papel exigido junto com o handler
(`rt.papeis[padrao]`); o middleware (`comAutenticacao`) consulta essa mesma
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
`recusaLoopbackAdmin`) — nunca anônima. A razão é o host do piloto ter
múltiplos usuários locais: `compact`/`checkpoint` anônimos por qualquer um
deles seria stall de ingestão sem privilégio nenhum de por meio. Rotas READ
continuam servidas sem credencial no loopback, intocadas.

Essa garantia é EXCLUSIVA do plano loopback, e não se estende ao principal:
com autenticação desligada (célula 3/config default, seção acima), o
principal não tem `muxLoopback` nem `recusaLoopbackAdmin` no meio —
`comAutenticacao` é puro passthrough, e as rotas ADMIN respondem anônimas
ali também. O loopback é fail-closed por construção porque o piloto tem
mais de um usuário local no HOST; o principal sem autenticação segue a
regra oposta — o operador desligou a checagem inteira, não só o TLS —, e é
essa mesma ausência de checagem que faz `POST /v1/compact` funcionar sem
credencial no caso de uso central do §96 (modo manual).

**Resolução de segredos na subida, nunca no parse.** O YAML só declara ONDE
a senha mora — `passwordEnv` (nome da variável) ou `passwordFile` (caminho),
nunca o valor — e a B1 garante XOR entre os dois; ler o valor de verdade é
ativação (`resolveCredenciais`/`resolveSenha`,
`internal/server/seguranca.go`), fail-fast ANTES de qualquer listener abrir.
Uma env var ausente ou definida-e-vazia recusa a subida nomeando a variável
(nunca o valor); um `passwordFile` group- ou world-readable (`modo&0o077 !=
0`) recusa nomeando caminho e modo — uma senha não é uma CA pública, o
descuido de permissão custa mais caro aqui do que no Kafka, de onde o molde
veio. O conteúdo do arquivo tem o `\n`/`\r\n` final aparado (todo `echo
senha > arquivo` termina em quebra de linha) e, se sobrar vazio depois de
aparar, a subida recusa — um `touch senha.txt` de provisionamento não pode
virar uma conta que autentica com qualquer senha em branco. Não há
recarga a quente: `passwordEnv` não é recarregável por construção, e
`passwordFile` fica para o débito nomeado "SIGHUP-reload" — o restart mede
~0,28s no piloto, o que barateia a alternativa.

**TLS.** O certificado é carregado uma vez na subida
(`validaCertificadoNaSubida`, `tls.LoadX509KeyPair`) e nunca relido do disco
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
(`compPassadaMu`/`compPassadaEmVoo`, `internal/storage/store.go`).

O corpo de ambas aceita `{"dataset": "..."}` opcional; ausente significa
TODOS os datasets, na ordem determinística da subida. **O contrato de falha
parcial** (`processaDatasets`/`ErroParcial`,
`internal/server/mutacoes.go`) é stop-no-primeiro-erro, mas a resposta de
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
  camada acima recupera. O índice por segmento não consegue produzir isso: um
  segmento cujo índice não tem a seção do campo **enumera** as linhas dele como
  candidatas, e o executor revalida o predicado contra o payload corrente (§44).
  O resultado é o mesmo do full scan filtrado, e a diferença é só de custo.

  - **Acrescentar** um campo indexado a um dataset povoado é aceito, e a consulta
    responde certo desde o primeiro instante, inclusive sobre as linhas gravadas
    antes da declaração.
  - **Remover** também é aceito: as seções param de ser escritas, e as que
    existem saem na reescrita do índice daquele segmento.

  **E os segmentos antigos passaram a ser AGENDADOS**, que é metade do que o
  débito 22 pede. A pendência de reconstrução é decidida por **(segmento,
  campo)**: a fila compara o conjunto de seções do `.sidx` com o conjunto
  indexado do dataset, nos dois sentidos — seção que falta agenda a
  reconstrução, seção de campo que deixou de ser indexado agenda a reescrita que
  a apaga. Como o `.sidx` é imutável, "acrescentar uma seção" é reescrever o
  arquivo inteiro, e a reconstrução cobre a **união** dos campos pendentes com os
  que o arquivo já cobria e continuam declarados: sem isso, a passada seguinte
  reescreveria com o campo antigo e perderia o novo.

  **A outra metade não existe, e é o que mantém o débito 22 ABERTO:** nada
  CONSOME a fila. Enquanto a goroutine de reconstrução não sobe, os segmentos
  agendados **enumeram indefinidamente** — correto e mais caro, que é o mesmo
  desfecho de antes por outro motivo —, e só é drenado o que alguém drenar
  chamando a reconstrução na mão.

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
  substitui o estado inteiro da chave e as postings antigas são limpas. O
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
`never` — é gravada com um sentinela (`rowindex.PrazoImortal`). Sem essa
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

**A adoção não exige recriação, e isto NÃO é o débito 22.** O débito 22 é sobre
o índice secundário: ele indexa um valor extraído do payload, e reconstruí-lo
exige ler todo record de todo segmento — conta que o índice por segmento partiu
em uma varredura por segmento, mas não eliminou. A agenda deriva de `expiry.idx`,
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
não existe" na outra até o processo reiniciar. `Compiled.Espelha` registra o
par, e `ResolveUnknown`, `ResolveAninhado` e `FixAutoType` propagam.

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
| `nishidb_history_index_bytes` | o disco de índice, medido em blocos — a purga também o devolve |
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
comando. Ela é o único observável em que aparece o conservadorismo da janela —
um segmento ainda sem índice é cobrado do orçamento pelo MAIOR `.sidx` em disco,
para que reconstruir e expulsar não virem um ciclo, e a consequência é uma
cobertura menor que a que o teto declarado sugeriria.

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
profundidade de grupo, e o tipo `Segredo` cobre o caso oposto — chave inocente
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

**As cinco séries de consulta do §92 nasceram com o protocolo.** Até a Fase 6
elas eram nome em `nomes.go` sem alimentador nenhum, e não por esquecimento: o
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

O nome de dataset vindo do request **nunca** vira label: label alimentado por
entrada do cliente é uma série nova por requisição, e um cliente em loop
consumiria a memória do processo. Erro antes de o dataset ser resolvido é
contado sob o valor fixo `?`.

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

## O que ainda não existe

TTL e tombstones por retenção (M6), o **backfill da tabela histórica** (ela
começa vazia quando é ligada, e o dataset base não guarda os eventos que a
preencheriam), e o **backfill do índice secundário** (débito 22). Este último
andou e não chegou: declarar um campo indexado sobre um dataset que já tem linhas
deixou de ser recusado, e os segmentos antigos passaram a ser AGENDADOS para
reconstrução, por (segmento, campo) — mas **nada consome a fila** até a goroutine
de reconstrução subir, então eles enumeram indefinidamente. Acrescentar `extract`
a um dataset com linhas continua sendo recusado, e ali a razão é outra, que
nenhuma reconstrução resolve: o valor extraído não existe nos records já
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
