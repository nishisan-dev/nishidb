# Direção: índices por segmento, janela de indexação e consulta longa

Documento de direção de produto e fechamento de escopo, escrito em 29/08/2026 a
partir do que foi **medido no piloto**. Não é plano de implementação: ele fixa
as decisões, nomeia os subsistemas, ordena as dependências e registra o que
ficou em aberto. Cada subsistema ganha spec e plano próprios.

## 1. O que aconteceu, e por que a direção mudou

O ciclo anterior (`planning/ingestao-folga-de-capacidade-com-history.md`)
entregou seis frentes e elas funcionaram: o writer saiu de 72% ocioso para
99,7% ocupado, a extração ficou 63% mais barata, o lag passou a dizer a
verdade. Com o dataset pequeno, o piloto sustentava a carga.

**Quando o histórico passou de 85 mil para 16,3 milhões de linhas, o gargalo
mudou de lugar** — e a instrumentação da fase 0.4 é que permitiu vê-lo em
minutos:

| | 28/08 (85 mil linhas) | 29/08 (16,3 milhões) |
|---|---:|---:|
| CPU por evento | 902 µs | **4.333 µs** |
| Vazão | 430 ev/s | **274 ev/s** |
| RSS | 360 MB | **1,65 GB** |
| `poll_blocked` | ~0 | **97,8% do tempo** |
| `queue_depth` | 0 | **5000 (cheia)** |
| Lag real | 515–994 | **25,8 milhões, crescendo** |

O profile apontou um único responsável: `hash.(*Index).Set` = **82% do CPU**.

### A causa

O godoc do índice hash a descreve sem saber que a descreve:

> *"Todas as postings de um valor caem na MESMA cadeia de probe, porque partem
> do mesmo hash; o Lookup varre a cadeia inteira até o primeiro slot vazio"*

O `Set` percorre essa cadeia inteira para não duplicar. Logo, inserir a
N-ésima posting de um valor custa N probes, e inserir *n* postings de um valor
custa **O(n²)**.

No piloto, `node` tem ~4.376 valores distintos sobre 16,3M linhas — **~3.724
postings por valor**. Medido: **1,5 ms por `Set`**. `identifier`, com ~3,8M
distintos, tem cadeia de ~4,3 e permanece barato. A diferença entre as duas
colunas é de ~866× no comprimento da cadeia.

Isso não estabiliza: **piora a cada linha inserida**.

### Dois agravantes de implementação

1. **`Region.AtomicLoad64` toma RWMutex por slot**
   ([region.go:146](../internal/storage/mmap/region.go)). Medido: `RLock` +
   `RUnlock` = **67,8%** do custo de ler um slot; a leitura em si é 8%. O
   `Set` já segura `ix.mu.RLock()` — o lock interno é desperdício puro em
   qualquer sondagem.
2. **A purga de segmento não toca o índice.** Verificado em
   `internal/storage/retencao.go`: nada remove as postings de um segmento
   aposentado, e o histórico não tem compaction (é append-only). **O índice
   cresce para sempre**, mesmo com a retenção funcionando e o dataset estável
   em 250 GiB. Em regime estacionário ele é o único componente que não para de
   crescer — e cada posting órfã alonga a cadeia de quem ainda está vivo.

### A ação tomada (paliativo, já em produção)

Removido o índice hash de `node` no histórico, com recriação da tabela irmã.
Resultado medido:

| | antes | depois | |
|---|---:|---:|---|
| Vazão | 274 ev/s | **1.636 ev/s** | 6,0× |
| CPU por evento | 4.333 µs | **528 µs** | 8,2× |
| RSS | 1,65 GB | **227 MB** | 7,3× |
| `poll_blocked` | 97,8% | **0** | |

**Isto contornou o problema; não o resolveu.** O desenho continua degradando
para qualquer coluna de baixa cardinalidade, e nada avisa o operador.

Rollback disponível: `events_history.bak-20260829-115342` (35 GB),
`events.yaml.bak-20260829-115328`, posição anterior `0=56788559384`.

## 2. Direção do produto

As decisões abaixo foram tomadas nesta sessão e valem para os próximos ciclos.

### 2.1 A premissa que governa o trade-off

> Um banco enxuto, capaz de consultar milhões de linhas **mesmo que devagar**.
> Priorizar a ingestão faz sentido; deixar a consulta lenta é aceitável desde
> que ela continue **possível e correta**, e desde que o operador tenha
> mecanismo para escolher onde fica cada uma.

Consequência: nada pode ficar **inconsultável**. Degradar para varredura é
aceitável; devolver resultado errado ou impossibilitar a pergunta não é.

### 2.2 O índice passa a ser por segmento

O produto já segmenta os dados. O que não é segmentado é justamente o índice —
ele é global, um arquivo por coluna cobrindo o dataset inteiro. Mover o índice
para dentro do segmento resolve três coisas de uma vez:

1. a cadeia de inserção fica limitada ao segmento corrente — some o O(n²);
2. purgar o segmento leva o índice junto — some a posting órfã, por construção;
3. o índice para de crescer em regime — o dataset fica de fato estável.

É o padrão de postings imutáveis por segmento (Lucene/Elasticsearch) combinado
com índice local por partição (Prometheus, InfluxDB, TimescaleDB).

O preço é que o `Lookup` passa a consultar N segmentos e unir os resultados —
o que o executor já sabe tratar, porque ele já aceita superset e revalida pelo
§44.

### 2.3 As postings de um valor ficam agrupadas

Dentro de um segmento, as postings de um mesmo valor deixam de morar na cadeia
de probe e passam a formar uma lista própria. Inserir vira **append: O(1)**.

Numa tabela append-only o `rowID` é monotônico, então a lista sai **ordenada de
graça** — busca binária e range scan vêm junto, sem custo de manutenção.

Registrado o que **não** fazer: um arquivo por valor. Para `node` seriam 4.376
arquivos; para `identifier`, 3,8 milhões. O agrupamento acontece **dentro** do
arquivo do segmento.

### 2.4 A indexação vira política declarada, com janela

O dataset declara **quanto de si vale indexar**. Fora da janela, a consulta
continua respondendo — por varredura.

```yaml
history:
  index:
    window: 7d        # unidade EM ABERTO — ver §4
```

| | dentro da janela | fora dela |
|---|---|---|
| Consulta | índice — rápida | varredura — lenta, mas possível |
| Ingestão | sempre barata (só o segmento corrente indexa) | — |
| Memória | limitada pela janela, **para de crescer** | zero |

### 2.5 Orçamento continua sendo do PROCESSO, não do dataset

Decisão deliberada, e ela contraria a formulação inicial da ideia. O produto já
decidiu isso para a consulta, com razão escrita em
[orcamento.go](../internal/query/budget/orcamento.go):

> *"É por processo, e não por dataset, pela mesma razão que o §82 dá ao
> `maxConcurrent`: todos os datasets de uma instância dividem o mesmo orçamento
> de memória, e um teto por dataset multiplicaria o pior caso pelo número
> deles."*

Reabrir isso faria `memoryTarget` deixar de dimensionar a máquina.

Some-se um limite técnico: **índice em mmap não admite orçamento no sentido de
recusar bytes**. Não se aloca — tocam-se páginas, e o kernel decide a
residência. Não há ponto de reserva como o `StreamWriter` oferece à consulta, e
foi por isso que o RSS chegou a 1,65 GB sem nada reclamar.

Por isso o que o dataset declara é **política** (`window`), não orçamento. O
teto de memória segue sendo do processo.

### 2.6 Consulta longa precisa de snapshot, e snapshot precisa de decisão

Consulta que dura horas ou dias exige coerência. Hoje ela não existe: o débito
94 registra que **`Store.Snapshot()` não existe** — verificado — e que a mesma
consulta devolve conjuntos diferentes conforme o planner escolha índice ou
varredura. Com prazo de 30 s era janela estreita; num job de dias é a diferença
entre um relatório e um número inventado.

**Boa notícia:** snapshot no *histórico* é barato. Append-only e imutável, um
snapshot é "rowID ≤ N sobre este conjunto de segmentos" — sem merge, update,
tombstone ou expiração por entidade para reconciliar. A dificuldade do débito
94 é o dataset *base*.

**Conflito a decidir (§4):** um snapshot lógico não impede a purga **física**.
Uma consulta de três dias pode ter segmentos apagados debaixo dela. Pinar
segmentos contra a aposentadoria briga de frente com `maxBytes`, que existe
para respeitar o teto de disco.

## 3. Decomposição

São **cinco** subsistemas, cada um com spec e plano próprios. Enfiá-los num
documento só os tornaria impossíveis de revisar.

| # | Subsistema | Depende de | Estado |
|---|---|---|---|
| **1** | Índice por segmento + postings agrupadas + janela | — | **próxima spec** |
| 2 | Snapshot de leitura no histórico | — | débito 94; barato no append-only |
| 3 | Pin de segmento vs. retenção | 2 | conflito de requisitos em aberto |
| 4 | Jobs assíncronos de consulta | 2, 3 | novo |
| 5 | Tabela de resultado em disco | 4 | novo |

**A ordem importa:** 4 e 5 não fazem sentido sem 2. Um job de dias que devolve
resultado incoerente é pior que não ter job nenhum — ele produz um relatório
com cara de correto.

O subsistema 1 é o único que resolve dor medida hoje, é autocontido e não
depende de nenhum dos outros. É por ele que se começa.

## 4. Questões em aberto

Precisam de decisão antes ou durante a spec do subsistema 1.

**Q1 — A unidade da janela.** Tempo, bytes ou segmentos?

| Unidade | A favor | Contra |
|---|---|---|
| `7d` | é como o operador pensa | com taxa variável, ocupa espaço diferente a cada semana; RSS deixa de ser previsível |
| `2GiB` | previsibilidade de memória e disco | tira a intuição temporal: "quanto tempo eu consigo consultar rápido?" não tem resposta direta |
| `últimos 40 segmentos` | é a unidade que o produto já purga | é a que o operador menos entende |

Preferência sinalizada: **tempo**. A consequência (RSS variável) precisa ser
aceita explicitamente, ou mitigada com um teto secundário em bytes.

**Q2 — Retenção durante consulta longa** (subsistema 3). Três saídas:
pinar e estourar `maxBytes`; deixar a retenção vencer e a consulta falhar
quando o dado sumir; ou pin com prazo máximo. É decisão de produto.

**Q3 — Migração.** Índice por segmento muda formato de disco e não há backfill
(débitos 22/86). Datasets existentes precisam ser recriados, ou o produto
precisa saber conviver com segmentos antigos sem índice — o que, curiosamente,
é o mesmo mecanismo da janela: segmento sem índice responde por varredura.
**Vale investigar se a migração é de graça pela própria janela.**

## 5. O que a spec do subsistema 1 precisa cobrir

1. **Layout do índice dentro do segmento** — como as postings de um valor são
   agrupadas e encadeadas quando crescem; header, versionamento, e o que
   acontece com um segmento gravado por binário anterior.
2. **Ciclo de vida** — o índice nasce e sela junto do segmento; a purga leva os
   dois; o segmento corrente é o único mutável.
3. **`Lookup` sobre N segmentos** — como os resultados são unidos, e como a
   união degrada para varredura fora da janela sem que o chamador precise saber.
4. **A janela** — chave de configuração, unidade (Q1), e o que acontece quando
   ela muda num dataset existente.
5. **Ordem de escrita e crash recovery** — o índice do segmento corrente
   precisa reconvergir pelo replay do tail, como os índices atuais fazem.
6. **Gates** — inserção O(1) provada por teste que mede o custo contra o
   tamanho do segmento (o análogo de
   `TestOEsforcoDaPassadaNaoAcompanhaOTamanhoDoDataset`, da agenda de
   vencimentos), e gate de alocação no caminho quente, que hoje **não existe em
   `internal/storage/`**.
7. **Disciplina de mutação** — cada invariante nova precisa de prova de que
   quebrá-la deixa a suíte vermelha. É a doutrina do débito 53, e foi assim que
   a agenda de vencimentos foi validada.

## 6. Itens soltos, independentes de tudo acima

- **`Region.AtomicLoad64` toma RWMutex por slot.** Vale ~68% do custo de
  qualquer sondagem. Com `node` fora do histórico deixou de doer, mas continua
  desperdício. Correção local, sem mudança de formato.
- **Guarda de cardinalidade.** O índice poderia medir o comprimento médio da
  cadeia e publicar; acima de um limiar, WARN nomeando a coluna. Transformaria
  degradação silenciosa em sinal. Vale mesmo depois do subsistema 1, para
  qualquer índice futuro.
- **`nishidb_ingest_events_rate`, `nishidb_materialized_tombstones`** seguem
  declaradas sem alimentador, por decisão registrada e travada por teste.
