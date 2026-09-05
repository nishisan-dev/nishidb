# Contadores por chave configurável, mantidos na escrita

> **Estado: implementado em 04/09/2026.** O plano que a executou, com as
> decisões que fecharam a seção 11 e o que a leitura do código mudou no
> desenho (o replay não tem o estado anterior; o delta sai do próprio vetor e
> os totais são dobra na abertura), está em
> [`contadores-mantidos-na-escrita-plano.md`](contadores-mantidos-na-escrita-plano.md);
> os números e as mutações, em [`docs/aceite-contadores.md`](../docs/aceite-contadores.md).
> O texto abaixo é a proposta como foi lida e criticada, sem reescrita.
>
> O caso que motivou o documento é externo: uma plataforma de accounting RADIUS
> que hoje mantém, em memória, a contagem de clientes autenticados por
> equipamento. O desenho abaixo é geral, mas as medidas todas vêm desse caso.

---

## 1. O que se quer, em uma frase

Saber, a qualquer momento e sem esperar, **quantas entidades de um dataset estão
num certo estado, agrupadas por uma chave que quem configura escolhe**.

No caso concreto: quantos clientes estão autenticados em cada OLT. Amanhã,
talvez, quantos estão autenticados em cada porta PON — sem mexer em código, só
trocando uma linha de configuração.

---

## 2. Por que a forma óbvia não funciona

A forma óbvia é a que qualquer um escreveria primeiro: mantenha um número por
OLT, e a cada mensagem que chegar ajuste esse número. Chegou um `Start`, soma
um. Chegou um `Stop`, subtrai um. Chegou um `Alive` de alguém que você ainda
não conhecia, soma um.

Essa forma tem um problema que aparece na primeira pergunta que a pessoa que a
escreveu se faz: *"e como eu sei quais mensagens eu já contei?"*

A pergunta não tem resposta boa. Ela existe porque somar por mensagem exige que
**cada mensagem chegue exatamente uma vez** — nem zero, nem duas. E o Kafka não
promete isso. Ele promete *pelo menos* uma vez. Uma reconexão do consumidor, um
rebalanceamento de partição, um reprocessamento depois de uma falha: qualquer um
desses reentrega mensagens que já foram vistas. O contador soma de novo, e ele
sobe para sempre, sem que nada denuncie.

O NishiDB já tem uma política de campo que faz exatamente isso — `merge: sum` —
e o comentário dela no código não disfarça:

> "NÃO É IDEMPOTENTE, e isso é contrato: reprocessar o mesmo evento soma de
> novo. (…) o valor fica ACIMA do real — aproximado, nunca exato."
> — `internal/schema/merge.go`

Ela existe para o caso em que um número aproximado serve. Aqui não serve: esse
número dispara alarme de queda de autenticação.

E há um segundo defeito, mais sorrateiro. Quando um cliente **muda de OLT** — uma
migração de porta, uma troca de equipamento —, o contador de mensagens soma um na
OLT nova e **nunca subtrai da antiga**. A OLT antiga fica com um cliente
fantasma, para sempre, e ninguém percebe, porque não houve mensagem nenhuma
dizendo "saí daqui".

---

## 3. A virada: contar estados, não mensagens

A saída é parar de contar mensagens.

O NishiDB não guarda mensagens. Ele guarda **o estado corrente de cada
entidade** — é literalmente para isso que ele existe. Cada evento que chega não
é somado a nada; ele *sobrescreve* o que se sabia daquela entidade.

Isso muda a natureza do problema:

| Você pensou assim | Vira assim |
|---|---|
| chegou `Start` → soma 1 | a entidade passa a ter estado `Start` |
| chegou `Alive` e eu não tinha `Start` → soma 1 | a entidade passa a ter estado `Alive` |
| chegou `Stop` → subtrai 1 | a entidade passa a ter estado `Stop` |
| *"quais mensagens eu já contei?"* | **a pergunta deixa de existir** |

E deixa de existir por um motivo mecânico, não por otimismo: se a mesma mensagem
chegar duas vezes, na segunda vez o estado já é o que ela quer escrever. Antes e
depois são iguais. Não há nada a ajustar.

Daí sai a ideia central deste documento:

> **O contador não é alimentado pela mensagem. Ele é alimentado pela
> _transição_ do estado — do que a entidade era para o que ela passou a ser.**

Reprocessou o mesmo evento? A transição é de X para X. Ajuste zero. O cliente
mudou de OLT? A transição é de "OLT antiga" para "OLT nova", e as duas pontas são
ajustadas na mesma operação, sem ninguém precisar avisar que houve mudança.

O que o Kafka precisa garantir passa a ser *pelo menos uma vez* — que é
exatamente o que ele garante.

---

## 4. Como fica para quem configura

A ideia é que o contador seja **declarado**, não programado. No YAML do dataset:

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
```

Três coisas: um nome, a chave que agrupa, e a condição que define o que conta.

Para descer um nível na topologia — de OLT para porta PON — muda-se uma linha, e
nada mais.

A leitura sai pelo SQL de sempre, por uma tabela virtual num namespace próprio,
uma tabela por contador:

```sql
SELECT olt, total FROM _counters.autenticados_por_olt WHERE olt = 'PRCTA_G1M10'
```

Isso encaixa em coisa que já existe: o parser **já reserva** o prefixo `_` inteiro
para namespaces virtuais, e `_meta.fields` é hoje a única tabela desse espaço —
não o limite dele.

A seção 5 mostra a forma inteira, com as saídas e com as escolhas que elas
forçaram.

### E isto não é `GROUP BY`

Vale ser explícito, porque parece.

O NishiDB recusa `GROUP BY` por decisão de produto, e a decisão continua de pé.
`GROUP BY` é uma **dobra em tempo de consulta**: cada leitura percorre o dataset,
o custo cresce com o tamanho da base, e a memória precisa de admissão e de
transbordo.

Um contador declarado é outra coisa: é um **índice derivado**. Ele é mantido
enquanto se escreve, é lido em tempo constante, e vive no disco como qualquer
outro índice. Nenhuma consulta dobra nada.

A diferença não é de grau. É de natureza.

---

## 5. Como isso se parece na prática

Esta seção é o desenho visto de fora: o que se escreve, o que se pergunta e o
que volta. Os valores das saídas são **ilustrativos** — servem para mostrar a
forma, não são medida de nada.

### 5.1 A declaração

O caso real precisa de **dois** contadores, não um. O alarme de queda não olha o
número absoluto de autenticados; ele olha a *diferença* entre quantos serviços
existem no equipamento e quantos estão autenticados. Então:

```yaml
counters:
  # Quantos serviços existem em cada OLT. Sem `where`: conta todo mundo.
  - name: servicos_por_olt
    groupBy: [olt]

  # Destes, quantos estão autenticados.
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
```

`where` é opcional, e a ausência dele é o que dá o denominador.

### 5.2 A pergunta que o laço de alarme faz

Duas consultas por ciclo, uma para cada contador:

```console
$ nishidb query --server 127.0.0.1:9142 --dataset servicos \
    "SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC LIMIT 5"
{"olt":"BR_SPOMO_OLT14","total":0}
{"olt":"PRCTA_G1I85","total":3}
{"olt":"BR_UTBLZ_OLT01","total":11}
{"olt":"BR_SPOMO_OLT10","total":2790}
{"olt":"PRCTA_G1M10","total":2802}
```

A subtração fica com quem consulta — não há junção. São dois conjuntos de alguns
milhares de linhas, casados por `olt` num mapa na aplicação. É trabalho
desprezível perto dos dezessete segundos que uma varredura custaria.

Para um equipamento específico, é busca direta:

```console
$ nishidb query --server 127.0.0.1:9142 --dataset servicos \
    "SELECT olt, total FROM _counters.autenticados_por_olt WHERE olt = 'PRCTA_G1M10'"
{"olt":"PRCTA_G1M10","total":2802}
```

### 5.3 Descer um nível é trocar uma linha

```yaml
  - name: autenticados_por_pon
    groupBy: [olt, slot, port]
    where: authenticated = true
```

E a consulta ganha as colunas novas, tipadas:

```console
$ nishidb query --server 127.0.0.1:9142 --dataset servicos \
    "SELECT olt, slot, port, total FROM _counters.autenticados_por_pon
     WHERE olt = 'PRCTA_G1M10' ORDER BY total ASC LIMIT 3"
{"olt":"PRCTA_G1M10","slot":1,"port":9,"total":0}
{"olt":"PRCTA_G1M10","slot":1,"port":12,"total":47}
{"olt":"PRCTA_G1M10","slot":3,"port":2,"total":58}
```

### 5.4 Três decisões que estes exemplos forçaram

Escrever a saída expôs escolhas que a prosa tinha deixado implícitas. Vale
registrar as três, porque nenhuma é óbvia e todas mudam o resultado.

**Primeira: uma tabela por contador, com as colunas do `groupBy` abertas.**

A alternativa era uma tabela única — algo como `_counters.todos`, com uma coluna
de texto guardando a chave composta:

```
{"counter":"autenticados_por_pon","groupKey":"PRCTA_G1M10|1|9","total":0}
```

É mais simples de implementar e pior de usar. Filtrar por OLT viraria `LIKE
'PRCTA_G1M10|%'`, `slot` e `port` deixariam de ser números, e a ordenação por
componente sumiria. Uma tabela por contador custa montar o schema a partir do
`groupBy` declarado — o que o pacote `meta` já sabe fazer, porque ele compila a
tabela virtual a partir de um YAML como qualquer dataset de verdade.

*Nota de sintaxe que decidiu a nomenclatura:* uma coluna chamada `group` seria
uma armadilha — o parser reconhece essa palavra para publicar "GROUP BY não é
suportado". Com as colunas do agrupamento tendo os nomes reais (`olt`, `slot`,
`port`), o problema não chega a existir.

**Segunda: grupo que chega a zero permanece na tabela.**

Repare na primeira linha do exemplo: `BR_SPOMO_OLT14` com `total: 0`. É
justamente o caso que interessa — o equipamento que apagou por inteiro. Se a
linha sumisse ao zerar, o alarme não conseguiria distinguir "caiu tudo" de
"nunca existiu", e o pior incidente sairia da vista exatamente quando começasse.

Essa distinção já é doutrina aqui: o `_meta.fields` devolve `NULL` em vez de `0`
para as colunas de topo pelo mesmo motivo — zero é uma afirmação, ausência é
outra.

Isso **não** quer dizer que o grupo seja eterno. Ele permanece enquanto houver
linha apontando para ele, e por um prazo configurável depois disso — o ciclo de
vida está na subseção 6.5, e é ele que impede o dicionário de crescer sem fim.

**Terceira: namespace próprio, `_counters`, e não uma tabela dentro de `_meta`.**

`_meta` responde "o que este dataset **tem**" — esquema, tipos, índices. Contador
é dado derivado, outra natureza de pergunta. Separar também evita que um contador
chamado `fields` colida com a tabela que já existe. O parser reserva o prefixo
`_` inteiro, então o espaço já está disponível.

### 5.5 O expurgo, visto de fora

Este é o caso que abriu a discussão: um `Start` que chega e depois silêncio.
Acompanhando **um** cliente e o contador da OLT dele, com prazo de 48 horas:

| Quando | O que aconteceu | Estado do cliente | `total` da OLT |
|---|---|---|---|
| dia 10, 09h | chega o `Start` | autenticado, vence dia 12 às 09h | 2.802 |
| dia 10, 09h | *o mesmo evento é reentregue* | inalterado | **2.802** |
| dia 11 | silêncio | inalterado, vence dia 12 às 09h | 2.802 |
| dia 12, 09h | prazo vence; o coletor publica a lápide | fora do dataset | **2.801** |

A segunda linha é o ponto inteiro do desenho: a reentrega **não mexe no número**,
porque o estado antes e depois é o mesmo. Nenhuma contabilidade de "quais
mensagens já contei" foi consultada, porque nenhuma existe.

E a quarta linha é o expurgo acontecendo sem que ninguém tenha varrido nada: o
coletor foi direto à linha vencida pela agenda.

> **O `total` cai quando o coletor chega, não no minuto exato das 48 horas.** O
> coletor trabalha com freio — por quantidade e por tempo — para não disputar o
> lock de escrita com a ingestão. Na prática isso não incomoda aqui: as 48 horas
> são regra de faxina, não de detecção. A detecção é o `Stop`, que é imediato.

### 5.6 A auditoria

O contador mantido pode divergir da verdade se algum caminho esquecer de
decrementar. O comando de auditoria recalcula por varredura e compara:

```console
$ nishidb counters --config nishidb.yaml --dataset servicos --audit
contador                 grupos    mantido    recalculado    divergência
servicos_por_olt          3.914    8.104.219    8.104.219              0
autenticados_por_olt      3.914    7.881.006    7.881.006              0
```

É caro — é a varredura completa — e por isso é comando, não rotina. Ele existe
para responder "posso confiar neste número?" depois de um incidente, e para ser
o oráculo dos testes de mutação descritos na subseção 10.

> **Um detalhe operacional que este exemplo deixa em aberto.** O trava do dataset
> é exclusiva por processo: com o servidor no ar, um comando `--config` não
> consegue abrir o dataset. Como a pergunta da auditoria é justamente sobre um
> sistema em operação, ou ela vira uma rota administrativa do servidor, ou ela
> exige o servidor parado — que é o oposto do momento em que se quer perguntar.
> A primeira opção parece a certa e implica papel `ADMIN`; fica registrado na
> subseção 11.6 em vez de decidido aqui.

## 6. Como funciona por dentro

### 6.1 A pergunta que decide o desenho

Para ajustar o contador numa transição, é preciso saber **em que grupo a entidade
estava antes**. Essa informação nem sempre está à mão — e o caso em que ela
falta é o mais importante de todos: a expiração.

Quando uma entidade expira, o NishiDB grava uma **lápide** — um registro que diz
"esta linha acabou". E a lápide é deliberadamente vazia: ela guarda o instante da
escrita e mais nada. É assim de propósito, porque é isso que devolve espaço em
disco. Só que uma lápide vazia não diz de qual OLT aquele cliente era.

A saída é o contador manter, ele mesmo, uma tabelinha lateral: **para cada linha,
em que grupo ela está e se ela satisfazia a condição**. Um vetor denso indexado
pelo mesmo identificador de linha que o resto do storage já usa.

Isso não é invenção nova: é exatamente a forma da agenda de vencimentos, que já
existe no projeto e guarda, num vetor denso indexado por linha, o instante em que
cada uma vence.

Com essa tabelinha, todo ajuste vira uma consulta local, sem reler registro
nenhum do disco.

### 6.2 Os quatro momentos

Existem quatro momentos em que o contador precisa mexer. Três tiram entidades de
cena, e é aí que mora o risco.

**Momento 1 — chegou um evento.** Dentro do caminho único de aplicação de estado
do NishiDB (`applyRecord`), com o estado antigo e o novo à mão:

1. lê da tabelinha o grupo anterior e se a condição valia antes;
2. calcula o grupo novo e avalia a condição sobre o estado novo;
3. se a condição mudou de valor, ajusta o `total` dos grupos envolvidos;
4. se o **grupo** mudou, ajusta também o `refs` dos dois — a contagem de linhas
   que governa o ciclo de vida da subseção 6.5;
5. atualiza a tabelinha e empurra o prazo dos grupos tocados.

Se nada mudou — o caso do evento reprocessado — os passos 3 e 4 não fazem nada.

**Momento 2 — a entidade expirou.** O NishiDB tem um coletor que trabalha por
agenda: ele **pergunta** qual é o próximo vencimento em vez de sair procurando.
Quando encontra uma linha vencida, publica a lápide. A lápide é um registro
durável e atravessa o **mesmo** `applyRecord`. Então o contador a vê como
qualquer outra escrita: lê o grupo na tabelinha, desconta do `total` se a
condição valia, desconta do `refs` sempre, e limpa a entrada.

**Momento 3 — a retenção removeu a lápide.** Passado o prazo de retenção, a
lápide some de vez. Como o contador já tinha decrementado no momento 2, aqui não
há nada a fazer — desde que a entrada da tabelinha já tenha sido limpa. Isto é
menos um passo do que uma verificação a não esquecer.

**Momento 4 — o processo subiu de novo.** O NishiDB não relê o dataset inteiro ao
abrir: ele parte de um checkpoint e reaplica só a ponta. Então o contador precisa
viajar no checkpoint, como os índices. Reaberto, ele reaplica a ponta pelo
`applyRecord` e volta ao ponto certo sozinho.

### 6.3 Nomeando os grupos

`groupBy` aceita mais de uma coluna, então o "grupo" pode ser um valor composto.
Duas decisões:

- **A chave composta nasce no `valuekey`**, que já é o dono único da codificação
  canônica de valores em disco. Uma segunda codificação seria o mesmo formato
  escrito em dois lugares — que é como as duas divergem.
- **Os valores de grupo são internados**: cada valor distinto ganha um número
  pequeno, e a tabelinha guarda o número, não o texto. É a mesma ideia do
  dicionário de campos, e traz consigo a mesma necessidade — um teto declarado,
  que **recusa na subida** em vez de arredondar em silêncio.

- **O grupo sobrevive ao próprio total.** Um grupo cujo total chega a zero
  permanece na tabela com `total: 0` — a subseção 5.4 explica por quê: o
  equipamento que apagou por inteiro é justamente o caso que interessa. Quando e
  como ele finalmente sai é a subseção 6.5.

Entidade sem a coluna de agrupamento (ausente ou nula) recebe um grupo reservado
e **não é contada**. Isso precisa estar escrito, não deduzido.

O tamanho do dicionário é limitado pelo inventário, não pelo tráfego: um grupo
por OLT, por PON ou por splitter que exista de fato. Ele não cresce com o número
de eventos.

### 6.4 O caminho de escrita não pode alocar

A escrita aqui é caminho quente, com propriedade verificada por teste: a bancada
mede **28,5 µs por evento com zero alocações**, e existe gate que quebra se
alocar.

Consequência prática: a montagem da chave composta tem de acontecer em buffer
reaproveitado. Não é detalhe de implementação — é requisito de desenho. O projeto
já faz assim ao qualificar nome de campo aninhado, pela mesma razão e com a razão
escrita no godoc.

---

### 6.5 O ciclo de vida de um grupo

Sem alguma forma de esquecimento, o dicionário de grupos só cresce. Com a forma
errada de esquecimento, o contador passa a mentir em silêncio. Esta subseção é
sobre a diferença.

#### O gatilho intuitivo está errado

A regra que ocorre primeiro é: *grupo cujo total chegou a zero e ficou assim por
um tempo pode ser apagado*. Ela é atraente e tem um defeito grave.

A tabelinha lateral guarda o grupo de **toda** linha, não só das que satisfazem a
condição — ela precisa, porque qualquer linha pode passar a satisfazer, e é aí
que o incremento acontece.

Então imagine uma OLT com 500 serviços, todos não autenticados:

```
autenticados_por_olt → total = 0     (parece morto)
tabelinha lateral    → 500 linhas apontando para este grupo
```

Se o grupo for apagado e o identificador reaproveitado por uma OLT nova, no
instante em que **um** daqueles 500 autenticar, o incremento cai no equipamento
errado. E cai sem sintoma.

`total == 0` não afirma "não há ninguém aqui". Afirma "ninguém aqui satisfaz a
condição". São duas frases diferentes, e só uma autoriza esquecer.

#### São dois relógios

| | Visibilidade | Recuperação do espaço |
|---|---|---|
| Pergunta que responde | por quanto tempo `total: 0` continua legível? | quando o grupo pode sair? |
| Condição | prazo **configurável** | `refs == 0`, sem exceção |
| Quem depende | o alarme, que precisa enxergar o colapso | o dicionário, que não pode crescer sem fim |

`refs` é quantas linhas apontam para o grupo, **independente da condição**. Ele é
interno ao dicionário — não dá para depender de quem configura ter declarado um
contador sem `where`, ainda que seja exatamente esse número.

Como `total <= refs` sempre, `refs == 0` já implica `total == 0`. Não há um
segundo caso a tratar.

#### A regra

O prazo é empurrado a cada mexida no grupo — entrou linha, saiu linha, mudou de
grupo. Um grupo com `refs == 0` e prazo vencido sai. Um grupo com `refs > 0`
nunca sai, mesmo com `total` zerado há meses.

```yaml
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    retainEmptyGroups: 7d      # visibilidade após esvaziar
```

A estrutura é a mesma da agenda de vencimentos — prazo empurrado na escrita,
coletor que pergunta em vez de procurar —, mas é uma **segunda instância**, não
um reuso do mesmo código: aquela indexa linhas, esta indexa grupos. A escala é
outra também, e a favor: milhares de entradas contra milhões.

#### Como escolher o prazo

Há um critério, e ele não é arbitrário. Quando uma OLT apaga por inteiro, as
entidades dela expiram pelo prazo de ociosidade, `refs` chega a zero, e o grupo
fica elegível. Se ele sair antes de alguém ter olhado, o pior incidente some da
vista justamente depois de acontecer.

> **O prazo de visibilidade tem de ser maior que o tempo entre o colapso e a
> ação humana sobre ele.** Não é o prazo de expiração das entidades; é o que vem
> depois dele.

E há um limite do que esta camada consegue dizer: um equipamento **removido do
inventário** e um equipamento **apagado há dois dias** chegam ao mesmo estado —
`refs == 0`. O contador não os distingue, e não deveria tentar. Quem separa os
dois é o inventário, na camada de alarme.

#### Aposentar ou reciclar

Duas formas de o grupo sair, com riscos diferentes:

- **Aposentar** — libera a memória e o número nunca é reemitido. Trivialmente
  seguro: nenhuma referência velha pode ser reinterpretada.
- **Reciclar** — o número volta ao estoque. Limita o espaço de identificadores,
  mas exige **provar** que nenhuma referência sobrou, e a prova é o tipo que
  falha em produção e não em teste.

A recomendação é começar aposentando. Grupo aqui é inventário físico, com
rotatividade lenta — nada parecido com o crescimento de vocabulário que forçou o
teto do dicionário de campos. Reciclagem entra depois, com medida de rotatividade
na mão, se entrar.

---

## 7. O que isso custa, e por que compensa

O argumento inteiro está numa razão de tamanho: **o contador é um resumo de
0,1% do dado**. Uns dez mil grupos contra oito milhões de entidades.

E o que ele evita está medido (`docs/architecture.md`):

| Rota | p95 medido |
|---|---:|
| busca de um estado por chave | **18 µs** |
| consulta por índice + `LIMIT 100` | 522 µs |
| varredura completa (8M linhas) | **17,0 s** |

São seis ordens de grandeza entre ler um estado e dobrar o dataset. Num ciclo de
alarme de um minuto, uma varredura consome mais de um quarto do ciclo — e não é
só CPU: ela segura um snapshot de leitura aberto por dezessete segundos, e o
snapshot **segura a compaction e a retenção** enquanto durar. Ciclo de trinta
segundos não fecha a conta de jeito nenhum.

O custo em memória, por outro lado, é conhecido e comportado:

| Nível de agrupamento | Grupos (ordem de grandeza, ~8M serviços) |
|---|---|
| por OLT | 10³ – 10⁴ |
| por porta PON | ~10⁵ |
| por splitter | ~10⁶ |

Some-se a tabelinha lateral, que é proporcional ao número de linhas, não ao de
grupos: com o grupo cabendo em quatro bytes, são ~32 MB para 8M de linhas. É
comparável ao que a agenda de vencimentos já ocupa, e é a parte do custo que
merece ser medida antes de ser aceita.

> Estas grandezas de grupo são **estimativas de topologia** (ordem de 64 a 128
> assinantes por PON), não medidas. A amostra disponível é fina demais para
> extrapolar. O número real sai do inventário.

---

## 8. O que precisa mudar fora do NishiDB

Duas correções no produtor. Ambas são pequenas, e **as duas bloqueiam tudo**.

**7.1 — Publicar com chave.** Hoje o `service-view` publica assim:

```java
kafkaTemplate.send("serviceView", service);   // ServiceProducer.java:35
```

Um argumento só. Sem chave, o Kafka distribui as mensagens em rodízio, e dois
eventos do **mesmo cliente** podem cair em partições diferentes e ser processados
fora de ordem. A garantia de ordem por partição do NishiDB não socorre, porque a
ordem já se perdeu antes de chegar.

Isso é defeito latente hoje, independentemente deste projeto.

**7.2 — Um número de sequência no topo do documento.** O NishiDB sabe descartar
evento atrasado, mas para isso precisa de um campo declarado que só cresça. A
exigência é dura: o campo tem de estar **no primeiro nível** do documento e ser
inteiro. Uma coluna extraída de dentro de um objeto aninhado **não serve** — a
validação recusa.

No documento de hoje, o `_SEQ` existe, mas está enterrado em
`lastAuthPacket.attributes._SEQ`. Os únicos inteiros de primeiro nível são `slot`
e `port`, que são topologia.

Sem esses dois ajustes, o contador estaria **certo sobre um estado errado** — que
é a pior categoria de defeito, porque parece funcionar.

---

## 9. O que este desenho NÃO faz

Vale delimitar, porque a tentação de crescer o escopo é grande.

- **Não decide alarme.** Histerese, limiares por tipo de elemento, estado de
  "já alertado" — tudo isso continua na aplicação. O NishiDB entrega o número.
- **Não enriquece.** O NishiDB não faz junção e não conhece inventário. Ele
  precisa que o `olt` já venha no evento. *(Verificado: o OLT não é dedutível dos
  atributos RADIUS — na amostra, 86 dos 265 `NAS-Identifier` aparecem associados a
  mais de um OLT.)*
- **Não permite trocar a chave a quente.** Índice é imutável depois da primeira
  linha, e não há backfill. Mudar `groupBy` significa recriar o dataset. Isso
  atrita com o "configurável" do título, e a fricção é real — está listada como
  pergunta aberta, não resolvida por decreto.

---

## 10. Riscos, e como cada um se fecha

**A deriva silenciosa é o modo de falha principal.** Uma entidade sai de cena por
três caminhos — sobrescrita, expiração e retenção. Se **um** deles esquecer de
decrementar, o contador afasta-se da verdade devagar e nada avisa.

O fechamento tem duas partes:

1. **Auditoria**: um comando que recalcula por varredura e compara com o
   mantido. Existe precedente direto — `AuditaAgenda` faz isso para a agenda de
   vencimentos.
2. **Prova por mutação**, na doutrina do repositório: remover o decremento de
   **cada um** dos três caminhos, um de cada vez, ver o teste ficar vermelho,
   reverter, ver ficar verde. Relatar os dois lados. Um teste que passa com o
   defeito presente não prova nada.

**Apagar um grupo ainda referenciado corrompe o contador — e sem sintoma.** Este
é o risco que a subseção 6.5 existe para fechar, e ele merece constar aqui porque
a regra errada é a intuitiva. Se um grupo sair enquanto linhas ainda apontam para
ele, e o identificador for reaproveitado, o próximo incremento daquelas linhas cai
no equipamento errado.

O fechamento é o mesmo padrão: a auditoria confere `refs` contra a contagem real
de linhas por grupo, e a mutação a ser aplicada é **trocar a condição de saída de
`refs == 0` para `total == 0`**. O teste tem de ficar vermelho com um grupo cheio
de linhas que não satisfazem a condição — que é exatamente o cenário que passa
despercebido se o teste for escrito com todas as linhas satisfazendo.

**A condição não pode depender de campo somado.** Se o `where:` tocar um campo com
`merge: sum`, o estado deixa de ser função pura do último evento e a
idempotência evapora. Isso deve ser **recusado na subida**, não documentado.

**O relógio da expiração reinicia com qualquer escrita.** Está verificado: o
instante de escrita é injetado pelo writer, não vem do evento. Não existe modo de
dizer "só pacote RADIUS reinicia o relógio".

Hoje isso é inofensivo por acidente: o `service-view` só publica quando o pacote
é novo e vem do RADIUS. Mas é **dependência não declarada**. No dia em que ele
passar a publicar por enriquecimento de inventário, a regra de idade deixa de
significar o que se pensa — e deixa em silêncio. Precisa estar escrito como
premissa.

---

## 11. O que ainda está em aberto

**11.1 — O prazo de 48 horas é premissa, não medida. E há indício de que é curto.**

A regra pretendida é: cliente que passa 48 horas sem nenhum pacote sai da
contagem. A mecânica para isso existe e é limpa. O *número* é que não está
sustentado.

O que a amostra mostra:

- o maior grupo de `Session-Timeout` é de **cerca de oito dias** (290 de 1.045
  registros); só 92 registros negociam uma hora;
- o maior `Acct-Session-Time` observado é de **198,6 horas** — mais de oito dias;
- o `EventExpireDate` que o próprio produtor carrega é sempre **sete dias** à
  frente;
- entre os 480 serviços autenticados, **três** têm o último `Alive` com mais de
  48 horas, e outros cinco estão na faixa de 24 a 48.

Ou seja: existe sessão viva que passa dias sem emitir `Alive`. Se o intervalo de
accounting intermediário estiver desligado para uma fatia da base, um prazo de 48
horas **derruba cliente vivo** — e ele reaparece na conta como queda de
autenticação. O erro é para o lado caro.

> **Ressalva importante sobre esta evidência.** A amostra é um trecho do tópico,
> ou seja, só contém entidades que **acabaram de emitir**. Ela não consegue medir
> quantos serviços ficam 48 horas em silêncio total, porque quem está calado não
> aparece nela por definição. Os três casos são **indício, não medida**. Calibrar
> o prazo exige olhar o estado — a base do `service-view` responde isso hoje, com
> a distribuição da idade do último pacote por serviço.

Se o prazo único não servir, o NishiDB já aceita regra condicional — lista de
regras, primeira que casa vence, a sem condição por último:

```yaml
ttl:
  - when: session_timeout > 86400
    expireAfterWrite: 10d
  - expireAfterWrite: 48h
```

Prazo longo para quem negocia sessão longa, curto para o resto. Isso não é
feature a pedir; já existe.

**11.2 — Qual o teto de grupos, e qual o prazo de visibilidade.** O ciclo de vida
da subseção 6.5 tira o crescimento sem fim do caminho, mas deixa dois números por
definir. O teto ainda precisa existir e **recusar na subida** — é a proteção
contra um `groupBy` declarado sobre coluna de alta cardinalidade por engano. E o
prazo de `retainEmptyGroups` tem o critério escrito (maior que o tempo entre o
colapso e a ação humana), não o valor.

Ambos saem da mesma medida: a distribuição real de grupos por nível de
agrupamento, que vem do inventário e não da amostra.

**11.3 — A fricção do backfill.** Aceitar "trocar a chave é recriar o dataset", ou
abrir exceção e permitir construir um contador novo por varredura na abertura,
pagando tempo de subida? A segunda opção contraria a doutrina de não fazer
backfill, e por isso precisa de decisão explícita, não de conveniência.

**11.4 — Onde os arquivos do contador vivem** e como a geometria deles viaja no
manifest, para que uma abertura com parâmetros diferentes seja detectada em vez de
endereçar o lugar errado. A agenda de vencimentos resolve isso gravando a
geometria no manifest; o mesmo caminho parece servir, mas não foi verificado.

**11.5 — Quais métricas nascem com o contador.** No mínimo: tamanho por contador,
grupos em uso contra o teto, e deriva detectada pela auditoria. Séries que podem
alarmar nascem com zero na subida.

**11.6 — Como a auditoria alcança um dataset em operação.** A trava é exclusiva
por processo, então a varredura de conferência não pode abrir o dataset por fora
enquanto o servidor está no ar. Uma rota administrativa parece o caminho, e nesse
caso ela entra pelo registro de rotas com papel `ADMIN` e fecha por padrão. Falta
decidir se ela é síncrona (a resposta demora o tempo da varredura) ou se devolve
um identificador e a conferência corre ao fundo.

---

## 12. Evidência

Medições feitas sobre a amostra `service-view-topic-dump-sample.json` (1.045
registros, 1º de setembro de 2026). A amostra **não é versionada**.

| O que | Resultado |
|---|---|
| `authenticated` equivale a "último pacote não é `Stop`" | **0 divergências** em 1.045 |
| clientes distintos | 1.010 |
| OLTs distintas | 382 |
| `NAS-Identifier` associados a mais de uma OLT | **86 de 265** |
| `_SEQ` no primeiro nível do documento | **ausente** |
| inteiros de primeiro nível | só `slot` e `port` |
| maior `Acct-Session-Time` | 715.009 s (198,6 h) |
| autenticados com último `Alive` > 48 h | 3 de 480 |

Leituras de código que sustentam afirmações do texto:

| Afirmação | Onde |
|---|---|
| `merge: sum` não é idempotente, por contrato | `internal/schema/merge.go` |
| chave e sequência exigem campo de primeiro nível declarado | `internal/schema/spec.go` |
| lápide é registro durável e passa pelo `applyRecord` | `internal/storage/expiracao.go` |
| coletor tem freio por quantidade e por tempo | `internal/storage/expiracao.go` |
| agenda é min-tree densa, atualização em tempo constante | `internal/storage/rowindex/agenda.go` |
| instante de escrita é injetado pelo writer | `internal/schema/spec.go` |
| prefixo `_` inteiro é reservado para namespace virtual | `internal/query/meta/meta.go` |
| publicação sem chave | `service-view`, `ServiceProducer.java:35` |
| publica só quando o pacote é novo e vem do RADIUS | `service-view`, `MicroProcessorThread.java:157` |

Números de bancada citados: tabela de latência de consulta e
`BenchmarkApplyIngestReal`, ambos em `docs/architecture.md`.
