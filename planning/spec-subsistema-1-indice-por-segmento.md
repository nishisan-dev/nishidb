# Spec — Subsistema 1: índice por segmento

Spec normativa do primeiro dos cinco subsistemas decompostos em
[`direcao-indices-por-segmento-e-consulta-longa.md`](direcao-indices-por-segmento-e-consulta-longa.md).
Escrita em 29/08/2026, a partir do incidente medido no piloto `events`.

Cobre **só** o subsistema 1. Snapshot de leitura (2), pin de segmento contra a
retenção (3), jobs assíncronos (4) e tabela de resultado em disco (5) têm ciclo
próprio; a §3 do documento de direção explica por que a ordem importa.

## O que este subsistema resolve

Três defeitos medidos, um deles fatal:

1. **O `Set` do índice hash é O(n²) no número de postings de um valor.** Todas
   as postings de um valor caem na mesma cadeia de probe e o `Set` varre a
   cadeia inteira para não duplicar
   ([hash/index.go:293](../internal/index/hash/index.go:293)). No piloto,
   `node` com ~4.376 valores distintos sobre 16,3M linhas deu ~3.724 postings
   por valor e **1,5 ms por `Set`** — 82% do CPU do processo. Não estabiliza:
   piora a cada linha.
2. **A purga de segmento não toca o índice**
   ([storage/retencao.go](../internal/storage/retencao.go)). O histórico não
   tem compaction, então o índice cresce para sempre mesmo com a retenção
   funcionando e o dataset estável em 250 GiB. Cada posting órfã alonga a
   cadeia de quem ainda está vivo.
3. **Nada avisa o operador.** A degradação é silenciosa até o `poll_blocked`
   chegar a 97,8%.

O paliativo em produção (remover o índice de `node` do histórico) contornou o
problema sem resolvê-lo: o desenho continua degradando para qualquer coluna de
baixa cardinalidade.

## Decisões tomadas antes desta spec

Seis, todas fechadas em sessão de brainstorm e todas com consequência de
desenho. Ficam registradas aqui porque a spec inteira se apoia nelas.

### D1 — O índice por segmento vale para os DOIS modos de dataset

Um motor só, sempre por segmento — materializado e histórico. A **janela** de
indexação é política exclusiva do histórico: no dataset materializado ela não
tem sentido, porque a linha vive no segmento corrente independente da idade
dela.

Descartado *"só o histórico"*: faria `index:` significar duas coisas com dois
formatos de disco, duas recuperações e dois conjuntos de teste — e deixaria o
O(n²) vivo no base, onde 4,57M entidades sobre 4.376 valores distintos dariam
~1.044 sondas por `Set`. Limitado, mas ruim.

O caminho já existe: `applyRecord` conhece o segmento de destino (`a.seg`) e lê
o segmento anterior para o tally de liveness
([store.go:1655](../internal/storage/store.go:1564)); e a compaction reaplica o
record copiado pelo MESMO `applyRecord`, com o segmento novo
([compaction.go:215](../internal/storage/compaction.go:215)). As postings
migram junto com o record, sem código de migração.

### D2 — A janela declara TEMPO, com teto secundário em BYTES

```yaml
history:
  index:
    window: 7d
    windowMaxBytes: 2GiB   # opcional; VENCE quando os dois discordam
```

Não é mecanismo novo: é a forma que a retenção já tem (`expireAfterWrite` +
`maxBytes`, com o teto de bytes engolindo o prazo declarado —
[retencao.go:262](../internal/storage/retencao.go:261)). O operador escreve a
intenção em tempo e o limite em bytes, e a métrica diz qual dos dois está
mandando.

**Correção ao enunciado da Q1 do documento de direção:** o preço de declarar em
tempo não é RSS imprevisível. Com o índice por segmento, o que fica residente na
ingestão é o índice do **segmento corrente** apenas, e uma consulta toca ~1
página por segmento. Os 1,65 GB de RSS medidos vinham de sondar 372 MB de tabela
global a cada `Set` — isso desaparece por construção, com ou sem janela. O que
varia com a taxa de eventos é **disco**.

### D3 — Hash e bitmap entram juntos

O `bitmap` sofre dois dos três males: a retenção também não limpa os bits dele,
e ele também cresce para sempre — cada valor distinto tem um arquivo denso
dimensionado pelo maior rowId já escrito
([bitmap.go:25](../internal/index/bitmap/bitmap.go:25)). Deixá-lo de fora
tornaria falsa a afirmação central da direção ("o índice para de crescer em
regime").

### D4 — Uma estrutura só: lista de postings esparsa, sempre

Por segmento e por campo: valor → lista ordenada de rowIds, delta `u32` do rowId
base do segmento. A mesma estrutura serve aos dois motores, porque **por
segmento a diferença entre eles some**: os dois são "valor → conjunto ordenado
de rowIds dentro do segmento", e densidade vira decisão de codificação.

Descartado **Roaring** (contêiner por densidade): recupera ~390 MB no pior caso
de cardinalidade baixíssima — 0,15% de um dataset de 250 GiB — ao custo de uma
máquina de estados de conversão de contêiner, dentro de uma spec que já muda
formato de disco, ciclo de vida e interface de índice. Fica **nomeado como
alavanca futura, com o número medido**, não como omissão.

Descartado **duas seções** (esparsa para `hash`, densa para `bitmap`): o denso
QUEBRA no dataset materializado, onde os rowIds de um segmento não formam faixa
contígua — um bitmap denso ali seria tão grande quanto o global de hoje,
replicado por segmento.

### D5 — `index: true` é a única grafia; `hash` e `bitmap` são RECUSADAS

Com um motor só, as duas palavras construiriam a mesma coisa, e config que
declara uma escolha que o motor ignora é exatamente o `expireAfterWrite: 30d`
decorativo que o projeto já carrega como débito.

Recusa na subida com **exit 3**, não aceitação com WARN: um alias que ainda
funciona é fallback para legado, e o operador continuaria escrevendo uma
distinção que não existe. É mudança de contrato de configuração e entra
declarada no CHANGELOG e no README.

**Consequência no planner, e ela é uma simplificação.** Hoje a regra R2 (hash)
devolve UM plano e a R3/R4 (bitmap) faz interseção
([planner.go:99-114](../internal/query/planner/planner.go:99)). Com um motor a
assimetria some para melhor: `WHERE node='x' AND type=2` passa a intersectar os
dois em vez de indexar `node` e revalidar `type` linha a linha.

### D6 — Segmento dentro da janela sem índice é RECONSTRUÍDO em segundo plano

A regra é uma só, para os dois modos: **um segmento tem índice se e somente se
está dentro da janela de indexação** — sendo a janela do dataset materializado
infinita por construção.

Isso responde de quebra o que a §5.4 da direção pergunta ("o que acontece quando
a janela muda num dataset existente"): sem reconstrução, alargar a janela não
faria nada, e ela seria um botão de mão única que só sabe apertar.

Descartado *"reconstrução bloqueante no Open"*: assimetria explícita entre os
modos e subida proporcional ao dataset (~40 s para 8M entidades). Descartado
*"recriar o dataset"*: o base migrado responderia por enumeração possivelmente
para sempre, porque **um segmento do base sem garbage nunca é compactado**.

## Resposta à Q3 da direção: a migração é de graça, e a razão é o contrato

O contrato de `LookupHash` já é *superconjunto de candidatos, ordem crescente,
executor revalida* ([store.go:2337](../internal/storage/store.go:2337)). Então
"segmento sem índice" tem resposta correta e barata: **enumerar as linhas
daquele segmento como candidatos**. O executor filtra, o resultado é certo — só
lento.

A enumeração é exata e não lê record nenhum:

- **Histórico**: a faixa de rowIds do segmento. O rowId é monotônico e a
  retenção já lê a fronteira com um único `pread`
  ([retencao.go:380](../internal/storage/retencao.go:380)).
- **Materializado**: os rowIds cuja location aponta para aquele segmento,
  obtidos por uma passada sequencial sobre o `locations.idx` — array denso, 8M
  linhas são 64 MB, ~50 ms.

**No histórico, "fora da janela" e "gravado por binário anterior" são o mesmo
estado**, e por isso a migração sai de graça pela própria janela, sem recriar
dataset. No materializado a enumeração também é correta e barata, mas não é
estado final: a D6 manda reconstruir.

---

## 1. Layout do índice dentro do segmento

Referência: §5.1 do documento de direção.

O índice de um segmento existe em **duas formas**. É essa separação que dispensa
um alocador dentro do arquivo — que seria a parte cara e frágil do desenho.

### 1.1 Forma WIP — só o segmento corrente, mutável

Um arquivo por campo: `data/segment-%08d-%05d.widx`.

```
header 64 B: magic "NDBW" | version u8 | fieldID u16
             | dirSlots u32 | postCount u32

diretório (open addressing, sobre VALORES DISTINTOS)   16 B por slot:
    [hash u64][estado u64]        estado = (contagem << 32) | ultimo

postings (array append-only)                           16 B por posting:
    [rowID u64][anterior u64]
```

`ultimo` e `anterior` são índices 1-based; `0` é "não há". `hash == 0` marca
slot vazio, e isso é seguro porque `primary.HashKey` nunca devolve 0 nem 1.

**`contagem` e `ultimo` moram na MESMA palavra de 64 bits, e isso é a
linearização.** Publicá-los em duas escritas deixaria um leitor ver a cabeça
nova com a contagem velha — ele dimensionaria a fatia com um elemento a menos
que a caminhada produz. Uma escrita atômica de `estado`, depois de a posting
estar visível, publica os dois juntos.

**O rowId é ABSOLUTO na forma WIP, e a posting tem 16 bytes.** Não há
`baseRowID` nem `postingWidth` aqui, e a razão é o dataset materializado: um
`update` traz para o segmento corrente uma linha de rowId arbitrário,
possivelmente menor que o do primeiro record dele, então não existe base
conhecida antes do fato. Os 16 bytes também mantêm tudo alinhado em 8, que é o
que o `mmap.Region` exige das cargas atômicas. O desperdício é de UM segmento —
3,6 MB para 224 mil linhas — e a forma selada, que é quem dimensiona o disco,
re-baseia para 4 bytes (§1.2).

`anterior` encadeia as postings do mesmo valor para trás. `ultimo` é a cabeça
da lista. Inserir a posting `(v, rowID)` é:

1. sondar o diretório pelo hash de `v`. **A carga do diretório é o número de
   valores DISTINTOS do segmento, não o de postings** — é aqui que o O(n²)
   morre, e ele morre *dentro* do segmento também, não só entre segmentos;
2. gravar `[delta, slot.ultimo]` na ponta do array de postings;
3. `slot.ultimo = novoÍndice; slot.contagem++`.

Três escritas, sem laço sobre a cadeia, sem alocação, sem alocador.

**Por que a lista encadeada para trás, e não blocos encadeados para frente:** um
bloco de tamanho fixo desperdiça o bloco inteiro para um valor com uma posting
só — para `identifier`, com ~1 posting por valor, seriam centenas de bytes
perdidos por linha. Blocos que dobram resolveriam o desperdício ao custo de um
alocador dentro do arquivo, com lista de livres, e de uma janela de crash por
operação dele. O ponteiro de 4 bytes por posting custa o dobro do espaço da
forma selada, e isso só existe **para um segmento por vez**: 224 mil linhas são
1,8 MB. É o lugar certo para pagar.

O `Lookup` sobre a forma WIP percorre a cadeia de `ultimo` para trás, o que
devolve as postings em ordem **decrescente** de inserção; materializa
`contagem` elementos e preenche de trás para frente, produzindo ordem crescente
com uma alocação de tamanho exato. É a mesma alocação que o `Lookup` de hoje já
admite carregar ([hash/index.go:428](../internal/index/hash/index.go:428)), e
agora ela existe para **um** segmento em vez de para o dataset inteiro.

Crescimento: o array de postings dobra por `mmap.Region.Grow`; o diretório
recresce por rehash quando a ocupação passa do load factor — e o rehash reescreve
só o diretório (16 B × slots), nunca as postings.

**A ordem da caminhada não é crescente no materializado.** No histórico o rowId
é monotônico e a cadeia percorrida ao contrário já sai ascendente. No
materializado não: um `update` insere um rowId arbitrário. Então o `Lookup` do
WIP preenche de trás para frente, confere `slices.IsSorted` — que é O(n) sem
alocação e passa sempre no histórico — e só ordena quando precisa. Depois
compacta, porque a mesma linha pode receber dois records com o mesmo valor
dentro de um segmento e o contrato do iterador é ESTRITAMENTE crescente.

Na forma selada nada disso sobra: o escritor ordena e deduplica na construção.

### 1.2 Forma selada — todo segmento aposentado, imutável

Um arquivo por segmento, **todos os campos indexados dentro**:
`data/segment-%08d.sidx`.

```
header 64 B: magic "NDBX" | version u8 | flags u8 | secCount u16
             | segmentID u64 | baseRowID u64 | maxRowID u64
             | postingWidth u8 | reservado até 64

tabela de seções, secCount × 24 B:
    fieldID u16 | pad u16 | distintos u32 | dirOff u64 | postOff u64

por seção:
    diretório ORDENADO por hash (busca binária)   16 B por valor distinto:
        [hash u64][postIdx u32][count u32]
    postings: array de deltas (postingWidth B), contíguo por valor,
              ascendente dentro do grupo

EOF-4: crc32c u32 sobre [0, EOF-4)
```

Duas precisões que a Tarefa 2 lê do `mmap` sem parser tolerante a desvio, e
que por isso não podem ficar implícitas:

- **O `crc32c` fica no FIM do arquivo e cobre tudo antes dele**, header
  inclusive — não dentro do header. É o idioma que o `manifest.bin` já usa, e
  cobrir o header é o ponto: um header corrompido que passasse pelo checksum
  levaria a busca binária a offsets arbitrários dentro do mapeamento.
- **`postIdx` é o ÍNDICE DO ELEMENTO** dentro do array de postings da seção,
  não um offset em bytes. O endereço é `postOff + postIdx × postingWidth`.
  Chamá-lo de offset faria a leitura errar por um fator igual à largura da
  posting — e errar para dentro do arquivo, sem estourar limite nenhum.

A rotação do segmento converte WIP → selado: ordena, agrupa, **re-baseia**,
escreve `.sidx.tmp`, `fsync`, renomeia e desliga os `.widx`. Para 224 mil linhas
e três campos são ~3 MB copiados uma vez a cada 512 MiB gravados.

O re-base é o que torna a conta da §1.3 verdadeira nos DOIS modos: na selagem o
mínimo e o máximo dos rowIds do segmento já são conhecidos, então `baseRowID`
passa a ser o mínimo real e `postingWidth`, a menor largura que cobre
`maxRowID − baseRowID`. O WIP do materializado paga 8 bytes por posting por
precaução; o selado paga 4, porque a incerteza acabou. **`baseRowID` É o menor
rowId do segmento** — não há campo `minRowID` separado, que seria a mesma
informação duas vezes e uma chance de divergirem.

**Um arquivo por segmento e não por (segmento, campo)** porque 500 segmentos ×
N campos estouraria o `ulimit -n` típico de 1024. A forma WIP pode se dar ao
luxo de um arquivo por campo justamente porque existe para um segmento só.

O que a forma selada compra, e nenhum desses itens é estético:

- **Busca binária no lugar de sondagem.** O arquivo é imutável, então não há
  load factor a respeitar: o diretório tem exatamente `D` entradas, e não
  `D/0,7` arredondado para cima.
- **`Lookup` sem alocação nenhuma.** O resultado é uma **fatia contígua** do
  array de postings, já ascendente. Não há `slices.Sort`, não há
  `slices.Compact`, não há materialização.
- **O RWMutex do `mmap.Region` deixa de importar no caminho quente.** Ele existe
  só para proteger contra o remap do `Grow`
  ([region.go:95](../internal/storage/mmap/region.go:95)); arquivo selado nunca
  cresce. O **débito 102** sai da rota crítica por construção — o item solto da
  §6 da direção continua válido para o `primary` e o `denseFile`, mas deixa de
  ser o gargalo deste índice.
- **Bytes determinísticos**, o que torna o arquivo testável por golden, como o
  `valuekey` já é.

### 1.3 A conta de disco

`16 × distintos + 4 × linhas`, por campo e por segmento. No histórico em regime
— 250 GiB, segmentos de 512 MiB, 2.394 B/linha medidos, 224 mil linhas por
segmento, 500 segmentos, 112 milhões de linhas:

| Campo | distintos/segmento | **Novo** | Hash global de hoje | Bitmap denso |
|---|---:|---:|---:|---:|
| `type` (4 distintos) | 4 | **449 MB** | 2,56 GB | 56 MB |
| `node` (4.376 distintos) | 4.376 | **484 MB** | 2,56 GB | **61 GB** |
| `identifier` (quase única) | ~224.000 | **2,25 GB** | 2,56 GB | inviável |

`identifier` é o piso do desenho, não uma regressão: uma coluna quase-única
custa 16 B por linha para ser indexada por valor em qualquer motor. Ele fica
**12% menor** que hoje; `node` fica **5,3× menor**; e `type`, que a conta do
bitmap denso tornava inindexável no histórico, passa a caber.

O custo **não depende da cardinalidade** — é uma entrada por linha por campo
indexado, mais o diretório. Foi a conta do bitmap denso (61 GB) que empurrou
`node` para o hash e causou o incidente.

### 1.4 Descritores e mapeamentos

500 arquivos selados por dataset, resolvidos sem cache nem LRU: **`open` →
`mmap` → `close`**. No Linux o mapeamento sobrevive ao fechamento do descritor,
e um arquivo selado nunca precisa do `fd` de volta — não cresce, não faz
`msync`, não faz punch-hole. Zero descritores retidos; 500 VMAs contra um teto
padrão de 65.530.

O mapeamento é **preguiçoso**: um segmento nunca consultado não custa nada, e
a residência é decidida pelo kernel página a página, como no resto do produto.

### 1.5 Corrupção

O `crc32c` é verificado na **primeira consulta** da seção, e não no `Open`:
verificar 2,25 GB na subida custaria segundos por nada.

Divergência **não** derruba a consulta. O arquivo é apagado, o segmento volta
para a fila de reconstrução da D6 e responde por enumeração até ser refeito,
com ERROR nomeado e métrica própria. Índice é dado derivado; a resposta continua
correta o tempo inteiro, e o dataset se cura sozinho.

---

## 2. Ciclo de vida

Referência: §5.2 do documento de direção.

### 2.1 Quatro estados, e só um segmento mutável

| Estado | Arquivos | Quem escreve |
|---|---|---|
| **Corrente** | `.widx`, um por campo | o writer único, a cada `applyRecord` |
| **Selado** | `.sidx`, um por segmento | ninguém — imutável |
| **Ausente** | nenhum | — (responde por enumeração) |
| **Em reconstrução** | `.sidx.tmp` | a goroutine de reconstrução |

**O segmento corrente é o único mutável.** Não é escolha de layout: é
consequência de `applyRecord` sempre appendar no segmento corrente, inclusive
durante a compaction ([compaction.go:203](../internal/storage/compaction.go:202))
e durante o replay da recovery.

### 2.2 Selagem

Na rotação do segmento: ordena, agrupa, escreve `.sidx.tmp`, `fsync`, `rename`,
`fsync` do diretório, desliga os `.widx`.

Crash no meio deixa `.widx` órfão de um segmento que já não é o corrente. Ele é
**descartado, nunca adotado** — ver §2.5.

### 2.3 Purga

O `.sidx` sai junto com o `.dat`, no **passo 4** da aposentadoria que já existe
(`checkpointDasLocations → rename → checkpointDoManifest → unlink`,
[compaction.go:492](../internal/storage/compaction.go:492)). Os dois arquivos do
segmento morrem no mesmo lugar, e é isso que faz *"purgar o segmento leva o
índice junto"* ser uma linha de código em vez de uma promessa.

As duas janelas de crash são inofensivas, e vale escrever por quê:

- **índice sem dado** devolve candidatos que não resolvem — o executor os
  descarta na revalidação (§44), como já descarta posting órfã hoje;
- **dado sem índice** enumera, que é o estado normal de qualquer segmento fora
  da janela.

Órfão remanescente é limpo no `Open`, que já faz exatamente isso para `.retired`.

### 2.4 Reconstrução

Goroutine no molde da compaction e da retenção: primeira passada imediata,
medidor atômico observável, cancelamento por contexto, `Close` cancela e
AGUARDA antes do quiesce.

A **unidade de trabalho é (segmento, campo)** — é o que a §2.6 exige, porque um
campo pode ser acrescentado ao schema depois —, mas o **processamento é por
segmento**: os campos pendentes de um mesmo segmento são resolvidos na MESMA
leitura sequencial dos records. Ler o segmento duas vezes para dois campos
seria pagar o dobro pelo I/O que é o custo inteiro da operação.

Ela toma o `wmu` **só no `rename`** do `.sidx.tmp` para o nome canônico, mais o
`fsync` do diretório. A leitura dos records, a extração dos valores, a ordenação
e a escrita do arquivo temporário acontecem inteiramente fora dele. Pacing pelo mesmo orçamento de ciclo de trabalho da
compaction — sem isso ela vira o terceiro laço de fundo disputando o writer, que
é o defeito que o ciclo anterior mediu e nomeou.

Gatilhos, todos o mesmo mecanismo:

1. formato novo sobre dataset antigo (migração);
2. janela alargada;
3. `crc32c` divergente (§1.5);
4. **campo acrescentado ao schema** — ver §2.6.

### 2.5 O `.widx` nunca é confiado através de um restart

No `Open`:

- segmento **não corrente** com `.widx` e sem `.sidx` → apaga o `.widx` e entra
  na fila de reconstrução;
- segmento **corrente** → apaga o `.widx` e reconstrói lendo o próprio segmento
  (≤ 512 MiB, ~1 s), antes do primeiro `Put`;
- `.widx` de segmento que **não tem mais `.dat`** → apaga, e nada entra em fila.

O terceiro caso não é hipotético e não estava nesta lista: um crash entre a
rotação e a selagem deixa `.widx` de um segmento que já não é o corrente, e se
esse segmento for aposentado depois, o `.dat` e o `.sidx` saem enquanto o
`.widx` fica. Sem esta regra ele ficaria em disco **para sempre** — e como o
`IndexBytes` passou a contar os arquivos de índice que moram em `data/`, ele
viraria disco visível que nada explica, que é exatamente o modo de falha que a
contagem existe para eliminar.

A remoção é por **nome**, com `ListaWIP` mais `os.Remove` — nunca por
`ApagaWIPs`, que abre e valida o arquivo. Um `.widx` rasgado por crash é
justamente o que não se deixa abrir, e a limpeza não pode depender de o lixo
estar íntegro.

Isso elimina de uma vez o `msync` do índice no checkpoint, a invariante de ordem
entre ele e o `durableOffset`, e a classe inteira de janelas de crash do índice
secundário. A justificativa é que o índice é **dado derivado**: a recuperação
mais barata que está correta é derivá-lo de novo — e é o MESMO código da
reconstrução da §2.4, não um segundo caminho.

O §6.1 ("página de índice nunca referencia record não durável") não é afrouxado
por isto: ele vale para *locations*, e postings podem ser superconjunto (§44).

### 2.6 O débito 22 fecha aqui

A guarda de hoje recusa declarar índice sobre um dataset com `RowCount > 0`
porque não existe backfill, com `AllowHashDrop`/`AllowBitmapDrop` como opt-out e
exit 3 nos dois sentidos.

Com a reconstrução da §2.4 o backfill passa a existir, e manter a recusa seria
arbitrário: o produto teria a capacidade e recusaria usá-la. Então:

- **acrescentar** `index: true` a um campo de dataset povoado é aceito, e agenda
  a reconstrução dos segmentos dentro da janela;
- **remover** desliga a seção e apaga as existentes;
- as duas guardas e os dois opt-outs saem.

Consequência de implementação: a reconstrução é por **(segmento, campo)**, e não
por segmento, e o `.sidx` selado precisa poder ser reescrito com uma seção a
mais. Como ele é imutável, "reescrever" é gerar um `.sidx.tmp` completo e
renomear — a mesma operação da selagem, sem caminho novo.

---

## 3. `Lookup` sobre N segmentos

Referência: §5.3 do documento de direção.

### 3.1 Uma superfície só

`LookupHash` e `LookupBitmap` colapsam em:

```go
func (s *Store) Lookup(field types.FieldID, v types.Value) (types.RowIDIterator, error)
```

E com elas colapsam as capabilities do planner e do executor —
`HasBitmap`/`HasHash`/`HashLookuper`/`HashCapable` viram `Indexado(field) bool`
mais o `Lookup` acima ([planner.go:12-30](../internal/query/planner/planner.go:12)).
Os tipos de plano seguem junto: `BitmapScanPlan` e `HashIndexPlan` viram
`IndexScanPlan`; `BitmapIntersectPlan` vira `IndexIntersectPlan`; o campo `Kind`
de `IndexTerm` desaparece, porque não há mais dois motores a distinguir.

O contrato do iterador **não muda**: superconjunto de candidatos, ordem
ESTRITAMENTE crescente, executor revalida o WHERE inteiro (§44).

### 3.2 Uma fonte por segmento

Em ordem crescente de segmento:

| Estado do segmento | Fonte | Alocação |
|---|---|---|
| Selado, **com** seção do campo | fatia contígua do array de postings | **zero** |
| Corrente (WIP) | caminhada da cadeia, materializada em `contagem` elementos | uma, de tamanho exato |
| Sem índice — histórico | a faixa `[primeiro, primeiroDoSeguinte)` | zero |
| Sem índice — materializado | rowIds cuja location aponta para o segmento | zero |

"Sem índice" inclui o **segmento selado cuja tabela de seções não tem o campo** —
o que acontece entre acrescentar o campo ao schema (§2.6) e a reconstrução
alcançar aquele segmento. Não é um quinto estado: é a ausência da seção, e a
fonte é a mesma.

As duas fontes de enumeração são **preguiçosas**, e isso não é detalhe de
implementação. A do materializado NÃO materializa o conjunto: ela caminha
`[rowIDBase, nextRowID)` sobre o `locations.idx` — que é array denso em mmap —
e emite o rowId quando a location aponta para o segmento em questão. A ordem
crescente sai da própria caminhada, e o consumo de memória é constante,
independente de quantas linhas casem.

### 3.3 A união: concatenação quando dá, merge quando precisa

Cada seção selada carrega `minRowID`/`maxRowID` no header, e as fontes de
enumeração conhecem os seus por construção.

- **Faixas disjuntas e ordenadas** → **concatenação**: zero comparações, e a
  ordem crescente é herdada. É o caso do histórico, sempre.
- **Faixas que se sobrepõem** → **merge k-way** por heap binário, com dedup de
  cabeças iguais. É o caso do materializado, sempre: ~9 comparações por linha
  com 500 segmentos.

A escolha vem dos **headers**, não de `EhHistorico()`. A diferença importa: um
predicado que se apoia no modo transforma uma premissa quebrada em resultado
silenciosamente errado; um que se apoia no dado degrada para o caminho mais
lento e continua certo.

**Por que o dedup só existe no merge.** No materializado o mesmo rowId pode
aparecer em dois segmentos — a linha foi gravada com o valor `x` no segmento 5 e
regravada com o MESMO `x` no segmento 9, ao atualizar outro campo. As duas
postings existem, e emitir a linha duas vezes quebraria o `unionIter` do
executor num `OR`. No caminho de concatenação a disjunção é **afirmada**, não
presumida: faixas disjuntas não repetem rowId.

### 3.4 A degradação que o chamador não enxerga

A fonte de enumeração é apenas mais um `types.RowIDIterator` na mesma
combinação. Planner, executor e driver veem um iterador ascendente e nada mais.
O único observável é métrica:
`nishidb_index_segmentos_consultados{modo="indice"|"enumeracao"}`.

**E não há regressão contra o full scan.** Para um segmento enumerado, o
executor faz exatamente o que faria varrendo: recebe rowIds crescentes e chama
`GetByRowID` em cada um. O caminho degradado é o caminho de hoje, restrito a
menos segmentos.

### 3.5 O que NÃO entra

**Interseção por segmento.** Intersectar dentro do segmento antes de concatenar
podaria mais cedo, mas é fator constante e moveria conhecimento de índice para
dentro do executor. O `intersectIter` que já existe
([candidate.go:110](../internal/query/executor/candidate.go:109)) combina os
iteradores do `Store` sem saber que eles são compostos.

**Ordenar a interseção pelo comprimento medido das listas.** O comprimento passa
a ser conhecido (`count` no diretório), e ordenar da lista mais curta para a mais
longa ajudaria. Fica **nomeado como alavanca**, fora desta spec: a regra do
planner é prioridade fixa e ele nunca falha, e trocá-la por uma decisão baseada
em custo é mudança de natureza, não de constante.

---

## 4. A janela

Referência: §5.4 do documento de direção. Decisão D2.

### 4.1 Configuração

```yaml
history:
  expireAfterWrite: 7d
  maxBytes: 250GiB
  index:
    window: 6h        # intenção, em tempo
    maxBytes: 2GiB    # teto do ÍNDICE; vence quando os dois discordam
```

A simetria é deliberada: `history.maxBytes` limita o **dado**,
`history.index.maxBytes` limita o **índice**, e nos dois casos o teto de bytes é
a política forte, que engole o prazo declarado. É a mesma forma de
[retencao.go:262](../internal/storage/retencao.go:261), e o operador que já
entendeu uma entende a outra sem ler documentação nova.

O teto governa bytes de **índice**, e não bytes de dado indexado, pela razão que
o próprio comentário de `bytesDosSegmentos` dá: *"um teto que a purga não
consegue perseguir seria um teto que mente"*. O que a expulsão da janela devolve
é `.sidx`.

**Regras de admissão** (doutrina: recusa na subida, nunca ignora em silêncio):

| Caso | Resposta |
|---|---|
| bloco `index:` ausente | **janela infinita** — indexa tudo que a retenção mantém. É o default e o comportamento de hoje |
| `index:` sob dataset sem `history:` | recusa, exit 3 |
| `window: 0` | recusa, exit 3 — ambíguo entre "nada" e "tudo"; desligar é omitir o bloco |
| `window` > `expireAfterWrite` | aceita com **WARN nomeado**: não está errado, é inócuo, porque a retenção já limita antes |
| `index.maxBytes` sem `window` | aceita — teto puro é declaração válida |

A janela é **do dataset**, não do campo. Não há evidência de demanda por janela
por campo, e uma janela por campo multiplicaria os estados de segmento por N.

### 4.2 Onde a janela é aplicada

Na passada de retenção, que já roda a cada minuto, já é exclusiva do histórico
(`IniciaRetencao` retorna cedo fora dele) e já tem em mãos a lista de segmentos
e os tamanhos. Um laço de fundo a menos.

A **reconstrução** é goroutine própria porque vale para os dois modos.

### 4.3 O que "fora da janela" significa, exatamente

Pelo tempo: o segmento está fora quando o record **mais novo** dele é anterior a
`agora − window`.

O mais novo, e não o mais antigo, porque a janela promete *"consigo consultar
rápido as últimas 6 horas"* — um segmento que atravessa a fronteira precisa
manter o índice. É a assimetria espelhada da retenção, que aposenta só quando o
segmento está **inteiramente** coberto ([retencao.go:341](../internal/storage/retencao.go:341)).

E não custa metadado novo: o record mais novo do segmento N é imediatamente
anterior ao primeiro do segmento N+1, que `primeiroRowIDDoSeguinte` já lê com um
`pread` ([retencao.go:380](../internal/storage/retencao.go:380)).

Pelos bytes: soma os `.sidx` do mais novo para o mais antigo e expulsa a partir
de onde a soma passa de `index.maxBytes`.

### 4.4 O que acontece quando a janela muda

| Mudança | Efeito | Quando |
|---|---|---|
| **Estreitada** | expulsa os `.sidx` que saíram | próxima passada de retenção (≤ 1 min) |
| **Alargada** | os segmentos que voltaram entram na fila de reconstrução | progressivo, observável por métrica |

É por isto que a D6 existe. Sem reconstrução, alargar a janela não faria nada e
ela seria um botão de mão única, que só sabe apertar.

### 4.5 Qual das duas políticas está mandando

Espelhando a série de instante mais antigo que já mostra o `maxBytes` engolindo
o `expireAfterWrite`: `nishidb_index_janela_efetiva_segundos` publica o intervalo
de tempo REALMENTE coberto pelos segmentos indexados. Comparado com o `window`
declarado, ele diz numa olhada se o teto de bytes assumiu o comando — e é
alertável sem que o operador precise inferir nada.

---

## 5. Ordem de escrita e crash recovery

Referência: §5.5 do documento de direção. A §2.5 já respondeu a maior parte;
sobra o que muda em `applyRecord` e no manifest.

### 5.1 A fase de SET não muda de lugar

Continua no **passo 5** do §10.1, ANTES do commit da location. A janela entre
índice e location continua sendo superconjunto — falso positivo, filtrado pela
revalidação (§44); nunca falso negativo.

### 5.2 A fase de CLEAR é REMOVIDA

`applyBitmapClears`, `applyHashClears` e o contador `clearFailures` saem
([store.go:1686](../internal/storage/store.go:1686)).

O motivo é a localidade por segmento, e ele merece estar escrito: quando uma
linha do dataset materializado troca de valor, a posting antiga mora no índice do
segmento **antigo**. Ela vira falso positivo — que o executor já descarta — e
morre quando a compaction aposenta aquele segmento. Nada precisa apagá-la.

Com o `Clear` some, por construção:

- o mecanismo de **lápide**, e com ele a regra "nunca esvaziar o slot, senão a
  cadeia de probe dos vizinhos é cortada";
- o **falso negativo mais delicado do índice atual** — velho e novo caindo no
  mesmo slot fazem o `Set` não fazer nada e o `Clear` apagar a posting corrente,
  documentado em [hash/index.go:339](../internal/index/hash/index.go:362);
- a reconstrução disparada por acúmulo de lápides.

O índice passa a ser **append-only dentro do segmento**, que é o modelo de
postings imutáveis por segmento do Lucene.

**O preço, com teto conhecido.** A taxa de falso positivo cresce com as postings
obsoletas de um segmento. Mas as postings de um segmento são exatamente os
records dele, então a precisão do índice é governada por
`liveRecords/totalRecords` — **o mesmo número que a compaction já persegue e que
o manifest já publica**. Não nasce dimensão nova de tuning, e o painel para
observá-la já existe.

### 5.3 Durabilidade

- O `.widx` **não participa de nada**: nem do checkpoint, nem do `msync`, nem da
  ordem do §48. É descartado no `Open` (§2.5).
- O `.sidx` é `fsync`ado antes do `rename`, e o diretório depois — a mesma
  sequência da selagem e da reconstrução.
- **O filesystem é a verdade** sobre quais segmentos têm índice. Nada no
  manifest lista isso, seguindo a doutrina que `primeiroRowIDDoSeguinte` já
  escreve: *"um metadado a menos é uma janela de crash a menos"*. O conjunto
  desejado vem da config; o conjunto real vem da tabela de seções de cada
  `.sidx`; a diferença entre os dois é a fila de reconstrução.

### 5.4 Manifest

Vai para **v3**, e é a versão — não uma tag — que bloqueia o binário anterior:
[manifest.go:353](../internal/storage/checkpoint/manifest.go:353) recusa versão
desconhecida.

- `tagHashes` (0x8001) é **removida**. Ela descrevia `slotCount` de uma tabela
  global que deixou de existir.
- `hashKind` e `bitmapCount` no corpo passam a ser gravados como zero e ignorados
  na leitura. Os bytes ficam para não reescrever o parser do corpo, e isso fica
  dito no comentário — campo reservado que ninguém sabe por que existe é como
  nasce a próxima leitura errada.
- Entra `tagIndicePorSegmento` (0x8004), **crítica**, carregando
  `formatoDoIndice u8`. Ela não é o que bloqueia o binário antigo (a versão já
  faz isso); ela existe para que a PRÓXIMA mudança de formato do índice tenha
  onde ser declarada sem outro bump de versão do manifest.

Rollback: um binário anterior recusa o dataset com "version desconhecida", que é
a resposta certa — ele não sabe manter o índice que o dataset tem. Voltar exige
restaurar o backup, como o piloto já tem documentado.

---

## 6. Gates e disciplina de mutação

Referência: §5.6 e §5.7 do documento de direção.

### 6.1 O gate de custo: inserção O(1)

Análogo de `TestOEsforcoDaPassadaNaoAcompanhaOTamanhoDoDataset`, da agenda de
vencimentos, em **duas camadas**:

**Determinístico, sempre roda.** O `Set` do WIP faz uma sondagem de diretório
amortizada e duas escritas. Um contador de sondas exportado por `export_test.go`
permite afirmar `sondas/insert < k` para N = 10 mil, 100 mil e 1 milhão de
postings do MESMO valor. Sem relógio, sem ruído de runner, sem `!race`.

**De tempo, `//go:build !race`.** Razão `ns/op(1M) / ns/op(10k)` com teto
explícito, e contraparte que se anuncia como SKIP sob `-race`. Teto versionado
em ratchet, como o `bench-guard`.

As duas camadas existem porque só a primeira prova a *forma* do algoritmo e só a
segunda pega regressão que venha de fora dele.

### 6.2 Os gates de alocação que hoje NÃO existem em `internal/storage/`

Primeiros do pacote. `testing.AllocsPerRun`:

| Caminho | Exigido | Por quê exatamente esse número |
|---|---:|---|
| `Set` no WIP | **0** | é o caminho quente da ingestão |
| Iterar postings de seção selada | **0 por linha** | o resultado é fatia contígua do mmap; alocação por linha é sinal de cópia indevida |
| `Lookup` sobre o WIP | **exatamente 1** | a materialização de tamanho exato. `0` significaria que não materializou (ordem errada); `2` significaria que o slice cresceu (tamanho errado) |

O `1` é afirmado como igualdade, e não como teto, de propósito: os dois desvios
possíveis são bugs diferentes e o gate precisa separar os dois.

**A medida do selado é POR LINHA ITERADA, não por `Lookup`.** Montar a lista de
fontes de uma consulta custa um boxing de interface por segmento — 500 alocações
por consulta com o dataset cheio, e não por linha. É um custo de abertura, da
mesma família da abertura de iterador que o executor já paga hoje; medi-lo junto
com a iteração esconderia justamente a regressão que importa, que é a cópia por
linha.

### 6.3 Goldens

- **Bytes do `.sidx`**, no molde do golden do `valuekey`: é formato de disco.
- **Incremental idêntico a reconstruído.** O mesmo segmento indexado pelo `Put`
  e pela reconstrução tem de dar arquivos **byte a byte iguais**. É este gate
  que torna a D6 confiável: se os dois caminhos pudessem divergir, a
  reconstrução produziria um índice silenciosamente diferente.

### 6.4 Testes de propriedade

Sobre corpora aleatórios, nos **quatro** estados de segmento (selado, WIP,
enumeração-histórico, enumeração-materializado):

- `Lookup(v)` ⊇ `{rows | campo == v}` — superconjunto, nunca falso negativo;
- ordem **estritamente** crescente, sem repetição;
- concatenação nunca escolhida sobre faixas sobrepostas.

### 6.5 Disciplina de mutação (débito 53)

Cada invariante nova precisa de prova de que quebrá-la deixa a suíte VERMELHA.
Sete mutações nomeadas, com o gate que deve reprovar cada uma:

| # | Mutação | Quem reprova |
|---|---|---|
| M1 | `Set` volta a varrer a cadeia antes de inserir | §6.1 — **gate de CUSTO, não de corretude**: o resultado continua certo |
| M2 | remover o dedup do merge k-way | §6.4, ordem estrita, no materializado |
| M3 | concatenar sempre, ignorando `min/maxRowID` | §6.4, faixas sobrepostas |
| M4 | adotar o `.widx` de segmento não corrente no `Open` | §6.3, incremental × reconstruído após crash |
| M5 | não apagar o `.sidx` no passo 4 da aposentadoria | teste de órfão: o disco não volta ao baseline |
| M6 | reconstrução emite postings na ordem de leitura, sem ordenar | §6.3, golden de bytes |
| M7 | `crc32c` divergente derruba a consulta em vez de degradar | teste de corrupção injetada |

**M1 reprovando num gate de CUSTO e não de corretude é o sinal a procurar**: a
mutação produz resultados certos e lentos, e a suíte precisa saber a diferença.
Foi assim que a agenda de vencimentos foi validada.

### 6.6 Métricas

Só pelas constantes de `internal/metrics/nomes.go`; `/v1/stats` e `/metrics`
lendo a MESMA estrutura; nascendo com zero na subida, porque todas podem alarmar.

| Série | Tipo |
|---|---|
| `nishidb_index_segmentos{estado="selado"\|"corrente"\|"ausente"\|"reconstruindo"}` | gauge |
| `nishidb_index_bytes` | gauge |
| `nishidb_index_janela_efetiva_segundos` | gauge |
| `nishidb_index_reconstrucoes_total` | counter |
| `nishidb_index_reconstrucao_ativa` | gauge (§92: sobe antes da goroutine) |
| `nishidb_index_crc_falhas_total` | counter |
| `nishidb_index_segmentos_consultados_total{modo="indice"\|"enumeracao"}` | counter |

Label nunca vem de entrada do cliente; `estado` e `modo` são conjuntos fechados.

### 6.7 A guarda de cardinalidade da §6 da direção

Ela deixa de ser necessária como alarme e vira **informação**: com postings
agrupadas não há mais "comprimento médio da cadeia" a vigiar. O `count` do
diretório dá a cardinalidade real por segmento de graça, e é isso que
`nishidb_index_segmentos_consultados_total` mais o `count` publicam. A
degradação silenciosa que ela existia para pegar foi eliminada na origem.

---

## 7. Aceite

### 7.1 O critério, e ele é afiado

O paliativo em produção atingiu o baseline **removendo** o índice de `node` do
histórico:

| | pós-paliativo, sem índice |
|---|---:|
| Vazão | 1.636 ev/s |
| CPU por evento | 528 µs |
| RSS | 227 MB |
| `poll_blocked` | 0 |

**O aceite é restaurar o índice de `node` e MANTER esses números.** Não é
"melhorar em relação ao estado quebrado" — é entregar a consulta indexada de
volta sem pagar por ela.

Aceite secundário, que o desenho promete e precisa ser medido: com a tabela
histórica em regime, `nishidb_index_bytes` **para de crescer** — hoje é o único
componente que não para, mesmo com a retenção funcionando e o dataset estável.

### 7.2 Como medir

No piloto (10.200.20.218), pelo método do ciclo anterior:

- CPU por evento: delta de `utime+stime` em `/proc/<pid>/stat` cruzado com
  `nishidb_ingest_events_total` numa janela de 60 s;
- profile de CPU de 45 s em `127.0.0.1:9144` antes e depois;
- `nishidb_ingest_apply_seconds` para latência, e o par
  `poll_blocked`/`writer_idle` para saber de que lado está o gargalo (tabela de
  decisão em `docs/operacao-kafka.md`);
- sempre `curl --noproxy '*'` — o `.bashrc` do root exporta proxy e o loopback
  responde 403.

No laptop a dispersão é de 15–27%: usar os **mínimos de 8–10 rodadas**, nunca a
mediana de 3.

### 7.3 Verificação

- `make test`, `make test-race`, `make test-integration`, `make lint` — lembrando
  que o gate de lint quebra **nos dois sentidos**: `.golangci-baseline` está em
  **387** e a contagem abaixo dela também reprova, então o arquivo é igualado no
  mesmo PR;
- `make bench-micro` com `NISHIDB_BENCH_DIR` em disco real, baseline novo em
  `benchmark/baselines/`;
- os gates de manifesto dos clients (`clients/jdbc`, `clients/python`) continuam
  passando: o código de erro do wire não muda, então não há SQLSTATE novo.

### 7.4 Documentação, no mesmo commit

- `docs/architecture.md` — só número **medido**, com origem em
  `benchmark/baselines/` ou num `docs/aceite-*.md`;
- `docs/diagrams/indice_por_segmento.puml` — estados do segmento e as quatro
  fontes do `Lookup`, incorporado via `uml.nishisan.dev` com URL raw fixando
  `main`;
- `docs/debitos.md` — fecha o **22** (§2.6) e tira o **102** da rota crítica
  (§1.2, com o item solto da §6 da direção continuando válido para `primary` e
  `denseFile`); débitos novos a partir do 103;
- `README.md` e CHANGELOG — a mudança de contrato de configuração da D5
  (`index: hash`/`index: bitmap` recusadas, exit 3).
