# Arquitetura do NishiDB — estado do M1

Escrito para quem chega agora. Os números são medidos, não estimados, e vêm da
rodada de aceite de 2026-08-14 (`benchmark/baselines/aceite-8m-20260814.md`).

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

![Containers](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/feature/m1-query/docs/diagrams/c4_container.puml)

Um binário só, sem dependência externa em runtime. Dois comandos: `ingest`
materializa NDJSON num dataset; `query` executa SQL e escreve NDJSON.

## Ingestão

![Fluxo de ingestão](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/feature/m1-query/docs/diagrams/ingest_flow.puml)

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

Custo medido: **0,0086 alocações por evento** e **45.243 eventos/s** na fase de
carga.

## Storage

![Layout em disco](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/feature/m1-query/docs/diagrams/storage_layout.puml)

Segmentos **append-only** para os dados; **mmap** para os índices. É essa
divisão que resolve a tensão do orçamento: os índices crescem com o dataset,
mas como página de **arquivo** — o kernel as descarta sob pressão, e elas não
são memória que o processo possui.

Na rodada de 8M: **386 MiB de RSS de arquivo** contra **71 MiB de RSS anônimo**,
com 6,09 GiB em disco. O heap Go fica em **994 KiB**.

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

**Um writer, muitos leitores.** O writer é serializado por mutex; as leituras
são concorrentes. O `Close` faz **quiesce** — espera as operações em voo e só
então libera o lock do dataset.

## Recovery

![Recovery](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/feature/m1-query/docs/diagrams/recovery_flow.puml)

O manifest marca um ponto consistente; o que veio depois é reconstruído lendo o
tail dos segmentos e reindexando. Um rasgo de crash só pode estar no fim, e é
truncado ali.

Onde a recovery **não** adivinha, ela recusa: manifest ausente com dados,
manifest ilegível, segmento sumido ou buraco na cadeia são erro fatal com
mensagem, não reconstrução heurística. Toda alternativa (truncar, pular,
recriar) descarta dado durável em silêncio.

## Consulta

![Fluxo de consulta](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishidb/feature/m1-query/docs/diagrams/query_flow.puml)

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
| point lookup | **16 µs** | 6 µs | 10 ms (§89) |
| bitmap + LIMIT 100 | **568 µs** | 367 µs | 50 ms (§90) |
| interseção + LIMIT 100 | 571 µs | — | 50 ms |
| união + LIMIT 100 | 573 µs | — | 50 ms |
| full scan + LIMIT 100 | 728 µs | — | — |
| varredura completa (8M) | 21,8 s | — | — |

A diferença entre as duas colunas — cerca de 10 µs no point lookup — é o custo
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

## O que ainda não existe

Kafka (M2), protocolo HTTP e driver JDBC (M3), índice hash físico, TTL,
compaction e tombstones por retenção (M4/Fase 8-9). O estado item a item está em
[`aceite-m1.md`](aceite-m1.md).

## Como os números foram obtidos

`benchmark/ingest-bench` sobre disco real, com `GOMEMLIMIT=512MiB`, corpus
determinístico e checkpoint periódico. O comando exato de reprodução está no
cabeçalho da baseline.
