# Feature request (RASCUNHO v2) — mapear campos dentro de um objeto complexo

> **Estado: rascunho para discussão.** Não é plano aprovado e não tem faseamento.
> O que foi medido está marcado; o resto é proposta.
>
> A v1 deste documento propunha lista estática de caminhos, justificada por uma
> premissa errada — a de que o conjunto de colunas de um dataset é fixo. Ele não
> é: `internal/schema/compile.go:412` registra que "o universo de campos cresce
> com os eventos" num dataset com wildcard e `unknownFields: include`, e
> `ResolveUnknown` registra campo novo em runtime. A imutabilidade vale para o
> TIPO de um campo declarado, não para o conjunto de colunas. Esta v2 parte
> disso.

## O problema

Um campo `json` guarda o documento inteiro como UM valor. É certo para preservar
o dado, e é o que o `events` faz com `persistent`. O custo é que nada lá dentro
é consultável: o SQL não tem sintaxe de caminho e coluna `json` não é
comparável — o binder recusa. Quem precisa filtrar por chave interna traz o
documento inteiro e filtra na aplicação.

O pedido é declarar que certas chaves de dentro viram colunas de verdade, com
tipo e índice.

---

## O que foi medido (piloto `events`, 2026-08-24, leitura pelo plano loopback)

### A forma do documento

| Medida | Valor |
|---|---|
| Tamanho | mín 2 B, mediana **473 B**, p95 3.266 B, máx 4.245 B |
| Chaves por linha | mín 0, mediana **11**, máx 114 |
| Profundidade | 1 a 3 |
| Chaves distintas em 3.000 linhas | **723** |
| Chaves em ≥50% das linhas | **11** |

Os tipos **não são estáveis por chave**: `frontPodName` aparece como string
2.493 vezes e booleano 14; seis contadores (`curtoTally`, `curtoTTL`, `longoTTL`,
`longoTally`, `medioTTL`, `medioTally`) aparecem como inteiro ~486 vezes, float 1
e string 6-7. `aliasValue` é lista.

### A cauda NÃO satura

Amostras de 1k, 5k, 20k, 50k e 100k linhas. Chaves novas por duplicação da
amostra: ~118, ~181, ~160, ~122 — **série constante, não decrescente**. Dois
ajustes com R² ≈ 0,99 (logarítmico e Heaps), **ambos ilimitados**.

A evidência decisiva: das 122 chaves novas do último degrau, **78 aparecem em uma
única linha**. A fronteira do crescimento é nome visto uma vez só.

A causa é estrutural: as chaves raras não são derivadas de valor (só 30 de 1.256
têm dígito; nenhuma tem espaço ou dois-pontos). São nomes de campo de
fornecedores, drivers e scripts — **catálogo de integração, que cresce enquanto
houver integração nova**.

Extrapolação para 1,25 M de linhas: **2.100 a 2.850 chaves**, e é PISO — a
consulta devolve prefixo contíguo, não amostra aleatória.

### O ponto em que dói

O encoder emite `nome + 8` bytes por coluna **nula** em cada linha (o `continue`
pula o valor, mas a chave já saiu), e `*` expande para o dicionário inteiro
(`binder.go:252`).

**A dor começa em ~450 colunas** — onde a linha do `SELECT *` passa de 10 KB
para transportar ~1 KB de dado real. Se as chaves do `persistent` fossem
promovidas hoje, o dataset **nasceria além desse ponto**: 545 chaves já com 1.000
linhas.

### O que 82,9% das chaves representam

Chaves em menos de 1% das linhas são 82,9% do vocabulário e respondem por
**2,91% das ocorrências**. Corte por frequência ≥1% da janela: **259 colunas
cobrindo 97,09% das ocorrências**.

---

## Achado independente: o teto de hoje derruba a ingestão

O dicionário de campos já tem limite (**65.535**), e ao bater ele **não degrada —
para o writer do dataset**. Caminho rastreado no código: o erro do dicionário
sobe sem sentinela (`patch.go:91`), cai no `default` da classificação
(`CategoryStorage`), não é reconhecido por `BadEvent()`, e vira `ActionFailed`.

E é irreversível: `fields.dict` é append-only, não há remoção, a compaction não o
toca, e o reopen recria as colunas varrendo `dict.Len()`. `exclude` só esconde da
projeção — o `fieldID` e os valores materializados ficam.

**Isto vale como issue própria, independente desta feature**, e precisa ser
corrigido de qualquer forma: sobre uma grandeza que cresce sem teto, bater no
limite não é anomalia — é questão de tempo.

*Ressalva: rastreado lendo o código, não reproduzido. Reproduzir antes de abrir.*

---

## Proposta

### Dois controles, porque são dois problemas

Bater num deles perde dado para sempre; bater no outro recusa uma consulta. As
severidades são diferentes e os controles também.

| | `maxCols` | `maxNestedCols` |
|---|---|---|
| Camada | persistência (dicionário) | projeção (consulta) |
| Ao bater | política configurável, **nunca para** | recusa a consulta |
| Reversível | não — o dicionário é append-only | sim, é só refazer a query |

### `SELECT *` não expande aninhado

O curinga devolve as colunas de topo e **não** as derivadas de objeto aninhado.
Elas só aparecem quando pedidas: `SELECT persistent.*`.

É o que mantém o `SELECT *` do `events` nas suas 49 colunas, o grid de uma
ferramenta utilizável, e o custo de fio proporcional ao que foi pedido. Sem isso
a medição acima diz que o dataset já nasce além do ponto de dor.

### `maxNestedCols` tem a forma do `maxRows`

Teto no dataset; o cliente pode pedir **menos** no corpo da requisição; o
servidor **recusa acima**. É a mesma forma já implementada e testada, inclusive
do lado do driver JDBC — que já sabe omitir o campo quando o pedido excede o
teto, em vez de transformar um default de ferramenta em erro. Um
`Statement.setMaxNestedCols` cairia ao lado do `setMaxRows` sem desenho novo.

Ao estourar, **recusar nomeando o limite**, e não truncar: truncar entregaria um
resultado com colunas faltando e cara de completo, que é o defeito que o produto
passou o ciclo fechando em outros lugares. O erro é acionável — pede-se as
colunas pelo nome.

### `maxCols` com política de estouro configurável

O número não é a decisão importante; **o comportamento ao bater é**. Nenhuma das
opções para a ingestão:

```yaml
schema:
  maxCols: 10000
  onMaxCols: ignore        # ignore | partial
```

- **`ignore`** — a chave excedente não é promovida a coluna. O evento é gravado
  com todas as outras. No caso aninhado **nada se perde**, porque o documento
  json continua inteiro na sua coluna; no caso de topo, a chave fica de fora.
- **`partial`** — grava o evento e registra que ele veio incompleto, para que a
  perda seja *contável* e não descoberta por acaso.

Nos dois casos: contador por dataset e log, porque o modo de falha inaceitável é
o silêncio. Isso segue a forma da política de erro que o produto já tem
(`log-and-skip`), em vez de inventar outra.

**Sobre o número:** a medição diz que 10.000 é inerte nesta forma de dado —
exigiria ~3×10⁸ linhas. Ele funciona como backstop contra uma forma patológica
(chave derivada de identificador), não como controle do caso real. Quem faz o
trabalho no caso real é o corte por frequência, abaixo.

### O que de fato vira coluna

Híbrido, mas com os papéis invertidos em relação à v1:

```yaml
fields:
  persistent:
    type: json
    extract:
      - path: probeIpNode
        as: probe_ip
        type: ipv4
        index: hash
      - path: frontAlarmId
        type: string
        index: bitmap
```

**Declarado** é o que precisa de índice. **O resto fica no json**, onde não custa
nada estrutural — e continua legível pela aplicação, porque o documento nunca
foi removido.

Se um dia se quiser promoção automática, o limiar está no dado e não é redondo:
**frequência ≥1% da janela → 259 colunas, 97% das ocorrências**.

---

## Índice declarativo sobre campo autodescoberto

### O problema que isto resolve

Campo autodescoberto **não indexa hoje**, e por isso filtro sobre ele é varredura
completa, sempre. Não é limitação profunda: índice se declara no YAML por campo,
e campo descoberto não está no YAML. O `f.IsAuto` do guard do planner
(`planner.go:198`) é cinto e suspensório — quem de fato barra é `f.Index` nunca
ser `bitmap`.

Sem isto, a descoberta dinâmica entrega nome no `SELECT *` e um lugar tipado, mas
**não entrega consulta rápida** — que era o que motivou o pedido inteiro.

### A regra: declarar promove a índice, NUNCA muda tipo

O tipo continua sendo o que a observação fixou. A declaração no YAML só liga ou
desliga índice.

```yaml
fields:
  probeIpNode:        # nasceu autodescoberto
    index: hash       # a única coisa que a declaração pode dizer
```

- Declarar `type:` que **bate** com o tipo já fixado: aceito, é redundância.
- Declarar `type:` que **diverge** do fixado: **recusado na compilação do
  schema**, com mensagem nomeando o tipo que o dataset já fixou. É esta linha que
  impede a imutabilidade de se perder por esta porta.
- Declarar só `index:`: o caso normal.

### Backfill em SEGUNDO PLANO, não bloqueando o consumo

O planner **já sabe conviver com índice declarado que não existe fisicamente**. O
comentário de `bitmapEligible` diz por quê: o conjunto de bitmaps é decidido na
criação do dataset, e existe `AllowBitmapDrop` para abrir sem um índice
declarado. Sem o check de `HasBitmap`, a query FALHARIA; com ele, ela DEGRADA
para varredura.

Isso significa que não há janela de resposta errada: enquanto o índice não
existe, a consulta varre e acerta; quando ele aparece, passa a usar.

Bloquear o startup até o backfill terminar seria a forma óbvia e é pior: o
runbook do piloto trata restart como barato (~0,28 s) e reversível, e é essa
premissa que sustenta o procedimento de exposição em quatro passos. Um startup
proporcional ao tamanho do dataset por índice novo a quebraria.

**Os bitmaps são por segmento** (`indexes/bitmap/<segmento>/<campo>.bm`), e
segmento selado é imutável. O backfill é então um trabalho bem-comportado:
iterar os segmentos selados e escrever o arquivo que falta, sabendo exatamente
onde parou — o segmento tem o arquivo ou não tem.

### A invariante que não pode falhar

**`HasBitmap` só vira verdadeiro quando o índice está COMPLETO.** Índice parcial
tratado como completo devolve menos linhas sem erro nenhum — resultado incompleto
com cara de completo, o pior defeito possível e o que este produto passou o ciclo
evitando em toda parte.

Para a primeira versão, a unidade de "pronto" é o CAMPO: liga só quando todos os
segmentos têm. Usar índice onde existe e varrer o resto seria melhor em latência
e pior em complexidade de plano — fica para depois, se doer.

**O backfill constrói com o TIPO FIXADO.** A chave do índice inclui o byte de
tipo — é a condição de correção nº 6 do planner, a mais importante do pacote. Um
índice construído com outro tipo faz o lookup devolver vazio, e a consulta mente
com resultado incompleto.

### Remover a declaração remove o índice

Decisão tomada, e ela contraria a tendência do produto de não remover nada — com
razão. O dicionário de campos é append-only porque os valores estão
materializados contra o `fieldID`: apagar a entrada tornaria o dado ilegível. Um
índice é **dado derivado**: apagá-lo não perde nada, só devolve a consulta para a
varredura, que é o comportamento correto e já suportado.

---

## O que a medição DERRUBOU — e o que a seção acima devolve

**Promover chave a coluna, sozinho, não compra velocidade de consulta.** Campo
`auto` não é elegível a bitmap, então filtro sobre coluna meramente descoberta é
varredura completa.

Isso derruba a motivação da descoberta dinâmica **isolada** — e é exatamente o
que o índice declarativo acima resolve: o campo nasce descoberto, e quando se
descobre que ele importa, uma declaração de índice mais um restart o promovem,
sem recriar o dataset e sem mudar tipo.

A ordem de trabalho que isso sugere é: descobrir barato, indexar o pouco que
importa, deixar o resto no json.

---

## Introspecção do sub-schema pelo próprio SQL

### Por que pelo SQL, e não por rota HTTP nova

Uma tabela virtual não exige suporte em cliente nenhum: DBeaver, CLI e driver já
sabem executar `SELECT` e desenhar o resultado. Uma rota nova precisaria de
código em cada um. É a razão principal de a ideia valer.

### Espaço de nomes reservado com `_`

**Regra: dataset não pode começar com `_`.** O prefixo fica reservado para
espaços de nome do produto, e `_meta` é o primeiro.

A convenção já existe — os campos de sistema (`_SEQ`, `_writtenAt`) usam `_` com
o mesmo sentido de "isto é do produto, não seu". Estender para namespaces reusa
uma marca em vez de criar uma segunda.

**A validação precisa entrar ANTES da implementação do `_meta`.** É barata agora
(uma recusa na compilação do schema, nomeando a regra) e cara depois: quem já
tiver um dataset assim só descobre na atualização, e a correção é recriar o
dataset — o custo mais alto do produto.

### A forma: uma tabela, com filtro

**Recomendada:**

```sql
SELECT name, type, auto, index, frequency
FROM _meta.fields
WHERE parent = 'persistent'
```

`parent` vazio para campo de topo, preenchido para aninhado. `SELECT * FROM
_meta.fields` responde "tudo o que existe neste dataset".

**Alternativa registrada** (era a proposta original): `_meta.<campo>`, uma tabela
por campo aninhado, mais `_meta.all_fields`. Rejeitada por duas razões: um campo
chamado `all_fields` tornaria `_meta.all_fields` ambíguo, e o binder passaria a
resolver nome de tabela dinâmico. A forma recomendada custa uma cláusula `WHERE`
a mais e elimina a categoria inteira de ambiguidade.

### As colunas

`type` e `auto` são **duas colunas**, não uma — e isso reusa o vocabulário que o
`/v1/metadata` já publica. Dizer só `auto` jogaria fora a informação que se foi
buscar ali: campo autodescoberto TEM tipo fixado depois da primeira observação. O
par responde às duas perguntas sem sobrecarregar uma coluna:

| coluna | significado |
|---|---|
| `type` | `unknown` enquanto nada foi observado; o tipo real depois |
| `auto` | descoberto (`true`) ou declarado no YAML (`false`) |
| `index` | `none`, `bitmap` ou `hash` — o DECLARADO, não o plano escolhido |
| `frequency` | em que fração das linhas o campo aparece |

**`frequency` é a coluna que faz a tabela valer.** A medição mostra por quê: 723
chaves, das quais 11 aparecem em mais da metade das linhas e 601 em menos de 5%.
Uma lista sem densidade não ajuda ninguém a decidir o que promover a índice — que
é a pergunta que leva alguém a consultar isto.

Custo a decidir: `frequency` sobre que janela? Calcular sobre o dataset inteiro é
varredura; sobre uma janela recente é barato e suficiente para a decisão, mas
precisa dizer qual janela na própria resposta.

### O que a tabela responde, e o que não

Ela é **estado corrente, não declaração**: para campo autodescoberto o tipo se
fixa na primeira observação, então duas execuções em momentos diferentes podem
devolver conjuntos diferentes. É a mesma ressalva que `compile.go:412` já faz
sobre `SELECT *`, e precisa estar dita na documentação da tabela.

### Impacto no driver JDBC — nomeado porque quebra em silêncio

O `SqlScanner` do driver extrai o nome do `FROM` para mandar no corpo da
requisição, e lê um identificador **bare**. Como `_` é início de identificador
válido no lexer, `FROM _meta.fields` faria ele extrair **`_meta`** e parar no
ponto — mandando `dataset: "_meta"` e recebendo recusa do binder por divergência.

O driver continuaria compilando e os testes continuariam verdes; quebraria só na
primeira consulta ao metadado. Precisa entrar junto com a feature, não depois.

Decisão pendente do mesmo bloco: com um espaço `_meta` existindo,
`DatabaseMetaData.getSchemas()` continua devolvendo vazio ou passa a listá-lo? O
driver responde vazio hoje porque "o NishiDB não tem catálogo nem schema" — e o
`NishiUrl` recusa caminho com mais de um segmento pela mesma razão.

### Custo no parser

Nome qualificado no `FROM` é gramática nova num parser declaradamente LL(1), e o
`FROM` hoje aceita exatamente um identificador. É a parte mais cara desta seção,
e a que justifica considerá-la depois das outras.

---

## Investigação à parte, que paga mais que qualquer teto

**NDJSON esparso: não emitir a chave da coluna nula.** Hoje o custo da resposta é
proporcional ao tamanho do DICIONÁRIO; com formato esparso passa a ser
proporcional ao dado real. A justificativa está na medição: 82,9% das chaves
respondem por 2,91% das ocorrências — o dado é intrinsecamente esparso e o
formato de fio não é.

Custo: muda o contrato do fio, e portanto o driver JDBC. Hoje o cursor casa linha
e schema **por posição**, justamente porque toda coluna vem em toda linha. Com
formato esparso o casamento passa a ser por nome, e a decisão de "coluna ausente
é protocolo violado" se inverte. Não é ajuste — é mudança de contrato, com o
driver junto.

Vale investigar separadamente, e antes de qualquer teto.

---

## Perguntas em aberto

1. **`partial` significa o quê, exatamente?** Marcar o evento? Guardar as chaves
   excedentes num campo de transbordo? A diferença importa para quem consome.
2. **Sintaxe.** `SELECT "persistent.probeIpNode"` já é válido hoje (identificador
   entre aspas aceita ponto). Mas `persistent.*` como expansão é gramática nova
   num parser declaradamente LL(1).
3. **Conflito de nome** entre coluna extraída e coluna de topo — recusar na
   compilação, presumo, mas é decisão.
4. **Listas.** `aliasValue` é lista. Indexar por posição é pobre; achatar em
   linhas seria outra feature (e não há JOIN para remontar).
5. **Profundidade 3** foi o máximo medido em 3.000 linhas. Vale confirmar em
   amostra maior antes de fixar limite.
6. **Backfill e compaction.** A compaction reescreve segmentos. O índice do
   segmento novo é carregado ou reconstruído? E o que acontece se a compaction
   rodar no meio de um backfill?
7. **Onde o progresso do backfill é observável.** Precisa de métrica e de campo
   no `/v1/stats`: um trabalho que roda em segundo plano e não se anuncia é um
   trabalho que ninguém sabe que travou.
8. **Concorrência de backfills.** Declarar cinco índices de uma vez dispara cinco
   varreduras. Serializar, limitar, ou deixar solto?

## Fora de escopo, deliberadamente

Consulta a caminho dentro do json em tempo de query (`WHERE persistent.x = ...`
sem extração declarada). Exigiria o executor parsear documento por linha durante
a varredura — o oposto do desenho colunar, e sem índice possível.

## Procedência

Amostras colhidas com `SELECT persistent FROM events` (1k a 100k linhas), pelo
plano loopback do piloto, em 2026-08-24. Só agregados saíram do host; nenhum
valor de evento. As amostras são prefixos contíguos, não aleatórias — a cauda
medida é piso, nunca teto. As afirmações sobre código foram rastreadas por
leitura, não por execução; o caminho do teto de 65.535 em particular **precisa
ser reproduzido** antes de virar issue.
