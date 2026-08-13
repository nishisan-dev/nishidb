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

![Containers](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/main/docs/diagrams/c4_container.puml)

> **Os cinco diagramas deste documento ainda não renderizam.** As URLs apontam
> para `raw.githubusercontent.com/nishisan-dev/nishidb/main/…`, e o repositório
> **nunca foi publicado**: `git remote -v` é vazio, então não existe URL raw para
> o proxy buscar e todas respondem 404. Publicar é ação do dono do repositório e
> está registrada em [`debitos.md`](debitos.md). Enquanto isso não acontece, os
> fontes estão versionados em [`docs/diagrams/`](diagrams/) — os cinco `.puml`
> renderizam em qualquer servidor PlantUML. As URLs fixam **`main`** de
> propósito: fixar a branch de trabalho quebraria os links no merge.

Um binário só, sem dependência externa em runtime. Dois comandos: `ingest`
materializa NDJSON num dataset; `query` executa SQL e escreve NDJSON.

## Ingestão

![Fluxo de ingestão](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/main/docs/diagrams/ingest_flow.puml)

Um evento passa por: tokenização sem cópia → resolução de campos contra o
schema → conversão tipada para um arena reutilizado → merge com o estado
anterior → gravação.

Quatro decisões que moldam esse caminho:

**NULL é ausência, nas três camadas.** Um patch com `"campo": null` **remove** o
campo do estado. Não existe "valor nulo" gravado: não no payload, não no índice,
não na query. É o que faz `IS NULL` e "campo ausente" serem a mesma pergunta.

**A sequência decide, não a ordem de chegada.** Evento com `_SEQ` menor ou igual
ao gravado é descartado inteiro. Sem isso, um reprocessamento fora de ordem
sobrescreveria estado novo com estado velho.

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

![Layout em disco](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/main/docs/diagrams/storage_layout.puml)

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

**Entre processos o acesso é EXCLUSIVO.** Um `flock(LOCK_EX|LOCK_NB)` no
diretório do dataset garante um processo por vez: com um `ingest` em voo, um
`query` concorrente sai com exit 3 e a mensagem nomeia o pid que segura o lock.
Como o M1 entrega dois comandos separados, a concorrência descrita acima **só é
alcançável in-process** (é assim que o harness de benchmark mede) — o acesso
concorrente de clientes distintos chega com o servidor do M3.

> **Restrição de deployment.** O `flock` é por inode e **não garante exclusão
> mútua em NFS** (sem `local_lock=flock|all`), nem em alguns FUSE, CIFS e
> overlayfs. Dois writers no mesmo dataset significam dois overlays de location,
> dois contadores de rowId e dois checkpoints — corrupção garantida. **Mantenha
> o diretório do dataset em disco local.**

## Recovery

![Recovery](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/main/docs/diagrams/recovery_flow.puml)

O manifest marca um ponto consistente; o que veio depois é reconstruído lendo o
tail dos segmentos e reindexando. Um rasgo de crash só pode estar no fim, e é
truncado ali.

Onde a recovery **não** adivinha, ela recusa: manifest ausente com dados,
manifest ilegível, segmento sumido ou buraco na cadeia são erro fatal com
mensagem, não reconstrução heurística. Toda alternativa (truncar, pular,
recriar) descarta dado durável em silêncio.

## Consulta

![Fluxo de consulta](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/main/docs/diagrams/query_flow.puml)

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
passada, e a linha é serializada direto para o writer. Medido: **38 alocações
por query, constantes** de mil a cem mil linhas — zero por linha.

Latências medidas com 8M rows e ingestão concorrente:

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
- **Campo indexado tem teto de 64 KiB** (`types.MaxIndexedValueBytes`, limite do
  `u16` do dicionário de valores). Valor acima disso faz o evento inteiro
  falhar. E há uma segunda porta: declarar `index: bitmap` num campo que **já
  acumulou** valores maiores torna o dataset inabrível até reabrir sem o campo.
- **Duas pausas visíveis, aceitas no M1:** o rehash da primary **pausa a
  ingestão** enquanto redistribui os slots, e o `msync` do checkpoint **pausa o
  writer**. Com `checkpoint.interval: 30s`, isso aparece como serrilha
  periódica no gráfico de vazão.
- **Phantom commit:** um `Put` que devolveu **erro** ao chamador pode virar
  **visível** depois do restart, se o record já tinha sido gravado no segmento
  quando a falha ocorreu. É decisão consciente (o replay do tail não distingue
  os dois casos) e está testada; quem trata erro de escrita como "nada foi
  gravado" vai divergir do dataset no próximo restart.
- **`mode: replace` existe.** Além do `merge` (default), a materialização aceita
  `mode: replace`, em que cada evento substitui o estado inteiro da chave e as
  postings antigas são limpas.

## Referência de tipos de campo

O que `type:` aceita no YAML do dataset, o que cada um aceita como **entrada** e
onde pode ser usado. Sem esta tabela, escrever um YAML novo é tentativa e erro.

| `type:` | Entrada aceita no evento | `index: bitmap` | `index: hash` |
|---|---|:---:|:---:|
| `string` | string JSON | sim | sim |
| `int64` | número inteiro ou `{"$numberLong":"<dígitos>"}` | sim | sim |
| `double` | número | não | não |
| `boolean` | `true`/`false` | sim | não |
| `timestamp` | **RFC3339(Nano)** ou **número = epoch millis**; `{"$date":"<rfc3339>"}` e `{"$date":{"$numberLong":"<ms>"}}` | não | não |
| `ipv4` | string `a.b.c.d` | não | sim |
| `ipv6` | string RFC 4291 (inclusive forma comprimida) | não | sim |
| `ipv4_prefix` | string `a.b.c.d/len` | não | não |
| `ipv6_prefix` | string `addr/len` | não | não |
| `bytes` | string **base64** (padrão, com padding) | não | não |
| `json` | **só object ou array** (guardado cru, exatamente como veio); escalar — string, número, `true`/`false` — é **recusado** | não | não |
| `auto` | qualquer um dos acima; o tipo é **fixado na primeira observação** e persistido | não | não |

Restrições da v1: a **key** (`materialization.key.type`) só pode ser `string`, e
a **sequence** (`materialization.sequence.type`) só pode ser `int64`. Campo
`auto` não é indexável — o tipo só existe depois do primeiro evento. Alias é
opcional e precisa casar `[a-z0-9_]+`.

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

**Não existe ainda.** Sem métricas Prometheus, sem `/metrics`, sem
`/v1/health`, sem `pprof` e sem log estruturado — o diagnóstico disponível é a
linha de resumo do `ingest` e o exit code. As §92-95 da spec têm dono
registrado em [`debitos.md`](debitos.md), com o **log estruturado (§95) puxado
para o M2** por ser barato e independente do servidor HTTP.

## O que ainda não existe

Kafka (M2), protocolo HTTP e driver JDBC (M3), índice hash físico, TTL,
compaction e tombstones por retenção (M4/Fase 8-9). O estado item a item está em
[`aceite-m1.md`](aceite-m1.md).

## Como os números foram obtidos

`benchmark/ingest-bench` sobre disco real, com `GOMEMLIMIT=512MiB`, corpus
determinístico e checkpoint periódico. O comando exato de reprodução está no
cabeçalho da baseline.
