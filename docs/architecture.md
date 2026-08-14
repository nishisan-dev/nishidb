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
> proxy do PlantUML busca o `.puml` anonimamente. Por isso os cinco diagramas
> passam por `git-raw.nishisan.dev`, que serve o conteúdo do repositório
> privado ao proxy. Os fontes estão versionados em
> [`docs/diagrams/`](diagrams/) e renderizam em qualquer servidor PlantUML.
> As URLs fixam **`main`** de propósito: fixar a branch de trabalho quebraria
> os links no merge.

Um binário só, sem dependência externa em runtime. Dois comandos: `ingest`
materializa NDJSON num dataset; `query` executa SQL e escreve NDJSON.

## Ingestão

![Fluxo de ingestão: do evento JSON à location publicada, com o commit point entre a fase SET e a fase CLEAR dos bitmaps](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/ingest_flow.puml)

### Ingestão por Kafka

O caminho por arquivo (`nishidb ingest`) e o por tópico (`nishidb server`)
compartilham materializer e storage; o que o Kafka acrescenta é a **posição**.
O pipeline é o do §27 sem desvio — poller único, fila limitada, writer único —
e os decode workers que o §27 marca como opcionais não existem: além de o §28
dizer que 500 ev/s não justifica múltiplos writers, é o pipeline de uma
goroutine de ponta a ponta que preserva a ordem por partição, que o §26 usa
como critério de correção quando o dataset não declara `sequence`.

A fila tem **dois** gates. O de registros é o `ingestionQueueSize` da spec; o
de bytes é inventado, e existe porque contar registros não é unidade de
memória — 5000 slots com payload grande estouram o orçamento de 1 GiB sem que
nenhuma contagem reclame. Um record acima do teto passa sozinho com a fila
vazia, senão a ingestão travaria para sempre num deadlock que nenhuma métrica
explicaria.

![Pipeline de ingestão Kafka: fetch, fila limitada com backpressure, writer único, e a ordem persist → checkpoint durável → commit do offset](https://uml.nishisan.dev/proxy?src=https://git-raw.nishisan.dev/nishisan-dev/nishidb/main/docs/diagrams/kafka_pipeline.puml)

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
- **bitmaps** — posting list por valor, com dicionário de valores. A chave
  inclui o **byte de tipo**, e é por isso que o planner recusa usar índice com
  valor de tipo promovido: um `double` jamais casaria postings gravados como
  `int64`, e o resultado seria vazio em silêncio.

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

**O que ainda falta para isso virar utilidade.** O `query.Engine` está
registrado no processo do servidor, mas nada o EXPÕE: o protocolo remoto é do
M3 (§67-69), e `nishidb query` continua sendo barrado pelo mesmo flock. Na
prática, com o servidor no ar não há como consultar — o operador alterna entre
ingerir e consultar. Ter os dois no mesmo processo é a metade que o M2 podia
entregar; a outra metade é o endpoint, e está registrada como débito 54.

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

**A prioridade do planner é fixa** (primary → hash → bitmap → interseção/união →
full scan), sem otimizador por custo. Toda dúvida degrada para full scan, que é
sempre correto — só mais lento.

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

## Formato de saída

NDJSON: uma linha por row, terminada em LF. Colunas **sempre presentes**, com
`null` explícito — o driver JDBC mapeia posição para valor e decodifica cada
linha num passe só.

Dois cuidados que o framing exige: o `json` armazenado guarda a janela crua do
evento, então um documento pretty-printed chega com quebra de linha dentro (é
sanitizada byte a byte); e uma string materializada **contém** control char cru,
porque o tokenizer decodifica `\n` ao gravar (é escapada). Sem os dois, uma
linha vira duas e o cliente dessincroniza.

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
- **O conjunto de bitmaps é IMUTÁVEL depois da primeira row CHECKPOINTADA, nas
  duas direções** — e a remoção, na prática, desde a criação do dataset. O gate
  de acréscimo é o `RowCount` do **manifest**, não as rows em disco: rows
  escritas e ainda não checkpointadas não o alimentam. O tamanho dos valores
  **não entra em nenhuma das duas decisões**:
  - **Acrescentar** `index: bitmap` a um campo é recusado assim que o manifest
    registra `RowCount > 0` (`internal/storage/store.go:420-438`), qualquer que seja
    o valor gravado: o índice cobriria só os records futuros e a query indexada
    omitiria as rows antigas. Backfill não existe no M1 (débito 22). Enquanto o
    dataset está vazio, acrescentar é permitido.
  - **Remover** `index: bitmap` de um campo já indexado é recusado sem
    condição nenhuma de contagem (`store.go:404-418`): basta o manifest declarar
    o índice, o que acontece já no checkpoint da criação. O índice pararia de
    receber sets/clears e reapareceria incompleto se o campo voltasse depois. O
    `Store` tem o opt-out `AllowBitmapDrop`, mas **o CLI não tem flag para ele**
    — pelo binário, a remoção é simplesmente impossível.

  Nos dois casos o comando seguinte — `ingest` ou `query` — sai com **exit 3**
  ("dataset corrompido") num dataset perfeitamente íntegro, e a mensagem nomeia
  o campo pelo **FieldID numérico**, não pelo nome do YAML. Medido com o
  binário, num dataset cujo único valor indexado tem 5 bytes (`"Start"`):
  `campo(s) [3] declarado(s) com índice bitmap num dataset que já tem 1 rows` ao
  adicionar, e `o manifest declara índice bitmap para o(s) campo(s) [1],
  ausente(s) desta abertura` ao remover — este segundo reproduzido também num
  dataset **com zero rows**, criado com o campo indexado e reaberto sem ele.

  **O que fazer quando o conjunto precisar mudar:** reprocessar a fonte para um
  dataset com o conjunto definitivo. São os dois únicos caminhos que o código
  permite hoje — (a) criar um dataset **novo** (outro `name`, outro YAML, outro
  diretório em `storage.path/<nome>`), ingerir a fonte nele e trocar o nome nas
  consultas; ou (b) apagar o diretório do dataset e reingerir a fonte sob o
  mesmo nome, o que **destrói** o estado atual e só serve se a fonte inteira
  ainda existir. Não há rebuild, backfill nem migração in-place. Escolha o
  conjunto de bitmaps na criação do dataset.
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

## Referência de tipos de campo

O que `type:` aceita no YAML do dataset, o que cada um aceita como **entrada** e
onde pode ser usado. Sem esta tabela, escrever um YAML novo é tentativa e erro.

| `type:` | Entrada aceita no evento | `index: bitmap` | `index: hash` |
|---|---|:---:|:---:|
| `string` | string JSON, e ela precisa ser **UTF-8 válido** | sim | sim |
| `int64` | número inteiro (sem `.`, `e` nem `E`), `{"$numberLong":"<dígitos>"}` **ou string de dígitos** — `-?[0-9]+`, sem espaço em volta, sem sinal `+`, sem fração e sem expoente; zero à esquerda é aceito (`"007"` → 7) e estouro do range de `int64` é **recusado** | sim | sim |
| `double` | número JSON **ou string com a mesma gramática de número JSON** (opcionalmente com fração e expoente). São recusados `NaN`, `Inf`, hexadecimal, separador `_`, sinal `+`, zero à esquerda (`"01"`), ponto sem dígito (`"1."`) e espaço em volta | não | não |
| `boolean` | `true`/`false` JSON — a **string** `"true"` não serve | sim | não |
| `timestamp` | **RFC3339(Nano)** com fuso **obrigatório** (`Z` ou `±hh:mm`) e fração **truncada em milissegundos**; **número = epoch millis** (inteiro); `{"$date":"<rfc3339>"}` e `{"$date":{"$numberLong":"<ms>"}}` | não | não |
| `ipv4` | string `a.b.c.d`, octeto decimal **sem zero à esquerda** e sem espaço em volta | não | sim |
| `ipv6` | string RFC 4291, inclusive a forma comprimida e `::ffff:a.b.c.d`; endereço **v4 puro é recusado** (`10.0.0.1` não vira v4-mapped), e zona (`%eth0`) também | não | sim |
| `ipv4_prefix` | string `a.b.c.d/len`, com os **host bits preservados** — `10.0.0.1/24` é gravado como `10.0.0.1/24`, não mascarado | não | não |
| `ipv6_prefix` | string `addr/len`, mesmas regras do `ipv4_prefix`; prefixo v4 é recusado, e prefixo v6 é recusado no `ipv4_prefix` | não | não |
| `bytes` | string **base64** (`StdEncoding`: alfabeto padrão, com padding). base64url é recusado | não | não |
| `json` | **só object ou array** (guardado cru, exatamente como veio); escalar — string, número, `true`/`false` — é **recusado** | não | não |
| `auto` | qualquer entrada, mas o tipo inferido sai de um conjunto **restrito**: string→`string`, número inteiro→`int64`, número com fração ou expoente→`double`, `true`/`false`→`boolean`, object/array→`json`, `$numberLong`→`int64`, `$date`→`timestamp`. **Nunca** infere `ipv4`, `ipv6`, prefixo ou `bytes` — uma string com cara de IP vira `string`. O tipo é **fixado na primeira observação** e persistido | não | não |

Restrições da v1: a **key** (`materialization.key.type`) só pode ser `string`, e
a **sequence** (`materialization.sequence.type`) só pode ser `int64`. Campo
`auto` não é indexável — o tipo só existe depois do primeiro evento. Alias é
opcional e precisa casar `[a-z0-9_]+`.

**Por que `int64` e `double` aceitam string.** É ajuste pós-amostra, não
leniência: a fonte real manda `Connect-Info` como `"1000000000"`. A coerção é
estreita de propósito — a string precisa casar a mesma gramática que o número
casaria, então `"1e3"` vira 1000 num `double` e é recusado num `int64`, e
`"NaN"` não entra em lugar nenhum (`TestCoerceDeclaredStringToNumeric`).
Recusa é erro de conversão, e no §101 erro de conversão descarta o **evento
inteiro**.

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

Protocolo HTTP e driver JDBC (M3), índice hash físico, TTL,
compaction e tombstones por retenção (M4/Fase 8-9). O estado item a item está em
[`aceite-m1.md`](aceite-m1.md).

## Como os números foram obtidos

`benchmark/ingest-bench` sobre disco real, com `GOMEMLIMIT=512MiB`, corpus
determinístico e checkpoint periódico. O comando exato de reprodução está no
cabeçalho da baseline.
