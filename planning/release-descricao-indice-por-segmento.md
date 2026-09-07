# Descrição da release — índice por segmento

Texto pronto para o `gh release create` (o changelog vive na descrição da
release; não há arquivo de changelog). Sugestão de versão: **v0.7.0** — a
release muda contrato de configuração e formato de dataset.

---

## Índice por segmento: o rehash global não existe mais

Esta release substitui os dois motores globais de índice secundário (`hash` e
`bitmap`) por **um índice por segmento**. O motivo foi medido em produção: com
47 M de linhas e o índice global em 3,46 GiB, a recópia disparada pelo fator de
carga — correndo sob o lock do writer — congelou a ingestão por **18 a 52
minutos por episódio**, de forma recorrente e dobrando a cada ocorrência
(`benchmark/baselines/piloto-pre-ciclo-20260830.md`). No desenho novo o índice
de um segmento aposentado é um arquivo selado: escrito uma vez, ordenado,
dimensionado exato, imutável. Não há fator de carga, não há lápide, não há
tabela para dobrar — o modo de falha não ficou raro, ficou impossível de
exprimir.

Resultado no piloto (135 GiB migrados em produção, sem recriar nada; subida em
`benchmark/baselines/piloto-pos-ciclo-20260830.md`, aceite com 25 h de soak em
`docs/aceite-indice-por-segmento.md`):

| Grandeza | Antes | Depois |
|---|---|---|
| Espera bloqueada do consumidor | 32.319 s acumulados, crescendo | **0,0 nas 25 h de soak** |
| Congelamentos de ingestão | 18–52 min, recorrentes | **nenhum** |
| CPU por evento (mesmo dataset) | 2.360 µs | **863 µs** (2,7× menos) |
| Bytes de índice por segmento | 3,46 GiB, só crescia | **102–127 MB**, oscilando com a janela |
| Heap do processo | RSS 2,8 GiB em congelamento | **126 MB anônimos**, estável |
| Migração dos 135 GiB | — | **~5 min**, com a ingestão correndo ao lado |

## Upgrade obrigatório — leia antes de trocar o binário

A migração de formato (manifest v1/v2 → v3) acontece sozinha na primeira
abertura e é **irreversível**: binário anterior recusa o dataset; voltar é
restaurar backup. Dois pré-requisitos de config:

1. **`index: hash` e `index: bitmap` foram substituídos por `index: true`** e
   as grafias antigas são **recusadas na subida com exit 3** (a mensagem nomeia
   o campo e ensina a grafia nova). Converta o YAML antes de subir.
2. **Declare `history.index` antes de subir** se o dataset tem tabela
   histórica. Sem o bloco a janela de indexação é infinita e a reconstrução de
   fundo varre o histórico inteiro; com ela, a migração toca só os segmentos
   dentro da janela:

   ```yaml
   history:
     index:
       window: 6h
       maxBytes: 2GiB
   ```

Acompanhe a migração por `nishidb_index_segmentos{estado="pendente"}` caindo a
zero e `nishidb_index_reconstrucoes_total` estabilizando.

## Mudanças

- **Um motor de índice só, por segmento** (`internal/index/segindex`): forma
  mutável apenas no segmento corrente; `.sidx` selado e imutável nos demais;
  segmento sem índice responde por enumeração — correto, só mais lento. A
  distinção de cardinalidade (hash × bitmap) deixou de existir.
- **Janela de indexação declarável** (`history.index.window`/`maxBytes`):
  quanto do histórico vale indexar. Fora da janela a consulta continua
  respondendo.
- **O conjunto de índices ficou mutável nas duas direções** (fecha o débito
  22): acrescentar `index: true` a um dataset povoado agenda a reconstrução em
  segundo plano; remover apaga as seções na reescrita. As duas recusas com
  exit 3 saíram.
- **Fase de CLEAR removida**: a posting do valor antigo fica no segmento em
  que nasceu, vira candidato a mais (que o executor já revalida) e morre com o
  segmento. Somem a lápide e o falso negativo mais delicado do motor antigo.
- **Seis séries novas** `nishidb_index_*`: estados por segmento
  (selado/corrente/pendente/fora), bytes, reconstruções, laço ativo, falhas de
  crc, segmentos consultados por modo (índice × enumeração).
- **O índice não participa mais do checkpoint**: o WIP é descartado e refeito
  na abertura; somem o `msync` do índice e a classe inteira de janelas de
  crash do índice secundário.

## Correções

- **Três corridas de falso negativo**, achadas pela revisão adversarial da
  branch e fechadas com prova por mutação: a rotação de segmento podia
  responder pelo índice do segmento errado numa consulta em voo; a enumeração
  do materializado podia perder uma linha movida por update concorrente (a
  consulta agora colapsa numa caminhada única do `locations.idx` quando algum
  segmento está sem índice); e a reconstrução podia publicar um índice parcial
  sobre corrupção no meio de um segmento selado.
- Métricas com `Family` obrigatória em toda amostra (o scrape do Prometheus
  recusa o payload inteiro quando o cabeçalho sai vazio), com gates de
  doutrina novos: citações de spec resolvem, amostras têm família.
- Débito 102 (RLock por carga de 64 bits) muda de dono: sai da rota do índice
  secundário por construção; continua no primary/denseFile.
- Débitos novos registrados: 103 (`.sidx` reprovado no crc32c não se cura
  sozinho — enumera para sempre), 104 (Roaring, nomeado com o número que o
  justificaria) e 105 (interseção ordenada por seletividade, idem).
- Wire e clients intactos: nenhum código de erro novo; JDBC e Python passam
  sem mudança.

## Verificação

`make test`, `make test-race`, `make test-integration`, `make lint` (baseline
372 exata), `make test-jdbc`, `make test-python` — tudo verde. Disciplina de
mutação do ciclo em `docs/mutacoes-indice-por-segmento.md`; aceite no piloto em
`docs/aceite-indice-por-segmento.md`; baseline de micro-benchmark em
`benchmark/baselines/segindex-20260830.txt`.

**Nota para o piloto:** o binário soakado (`v0.6.0-250-ge13a6dd`) não contém as
três correções de corrida — a atualização para esta release as leva.
