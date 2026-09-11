# Melhorias: tipo declarado das chaves quentes, e densidade por coluna

> Documento de entrada para uma sessão própria. Duas melhorias separáveis, que
> se reforçam: a primeira recupera dado que hoje se perde, a segunda torna a
> primeira **repetível sem script**.
>
> Estado: proposta. Nada aqui foi implementado.

## Context

O dataset `events` do piloto declara `persistent` como `json` com
`unknownFields: include`, então toda chave de dentro do documento vira coluna
automaticamente. O tipo de cada uma é fixado na **primeira observação**.

> **Errata (25/08, mesma sessão):** este documento nasceu afirmando que o valor
> divergente "fica nulo na coluna e o documento preserva". **Era falso no modo
> merge**, que é o do piloto: a segunda passada saía com `continue` seco, e
> `mergeRecords` preserva campo ausente do patch — a coluna ficava com o valor
> do evento ANTERIOR enquanto o documento da mesma linha trazia o novo. Coluna
> e documento se contradiziam dentro do mesmo record, e a query devolvia valor
> velho com cara de corrente. Corrigido: a segunda passada passou a emitir
> remoção explícita. A afirmação abaixo já vale.

Isso é o desenho certo — o dataset não governa o que recebe, e a integração
pode passar a produzir um campo novo do dia para a noite. Mas tem um custo
medido: quando o primeiro evento a trazer uma chave manda um tipo que não é o
predominante, **todas as ocorrências do tipo majoritário viram nulo naquela
coluna**. O documento preserva o valor; a coluna, não. E um filtro sobre ela
devolve menos linhas do que existem.

### O que foi medido (piloto, 25/08/2026)

Contador do produto, sobre 10.082.226 eventos:

| | |
|---|---|
| Eventos perdidos por datatype | **0** (`TYPE_CONVERSION = 0`) |
| Valores de coluna perdidos por datatype | **2.085.138** (~0,21 por evento) |

Ou seja: **não se perde evento por tipo — se perde valor de coluna.**

Amostragem de 20.000 documentos cruzando o tipo FIXADO de cada coluna com os
tipos que de fato chegam. Só 32 chaves, de 1.141 na amostra, respondem por toda
a perda:

| chave | tipo fixado | perde | de | % | tipos vistos |
|---|---|---:|---:|---:|---|
| `dataFalha` | `int64` | 2.315 | 2.383 | **97,1%** | int64 68, string 2.278, double 37 |
| `dataClear` | `string` | 348 | 2.030 | 17,1% | int64 317, string 1.682, double 31 |
| `curtoTally` | `int64` | 93 | 3.334 | 2,8% | int64 3.241, string 54, double 39 |
| `curtoTTL` | `int64` | 93 | 3.334 | 2,8% | int64 3.241, string 54, double 39 |
| `slot` | `string` | 92 | 789 | 11,7% | string 697, int64 92 |
| `longoTally` | `int64` | 90 | 3.334 | 2,7% | int64 3.244, string 50, double 40 |
| `longoTTL` | `int64` | 90 | 3.334 | 2,7% | int64 3.244, string 50, double 40 |
| `frontPersistence` | `string` | 89 | 1.826 | 4,9% | string 1.737, int64 89 |
| `medioTally` | `int64` | 88 | 3.332 | 2,6% | int64 3.244, string 48, double 40 |
| `medioTTL` | `int64` | 88 | 3.332 | 2,6% | int64 3.244, string 48, double 40 |
| `codigoGrupoResponsavel` | `int64` | 33 | 239 | 13,8% | int64 206, string 33 |
| `dataBaixa` | `int64` | 27 | 206 | 13,1% | int64 179, string 27 |
| `frontSATotal` | `string` | 21 | 23 | 91,3% | string 2, int64 21 |
| `frontPodName` | `string` | 21 | 13.477 | 0,2% | string 13.456, boolean 21 |

Perda total na amostra: **3.580 em 20.000 documentos** = 0,179 por documento.
O contador do produto mede 0,21 por evento. **As duas medições, independentes,
concordam** — é isso que dá confiança no número.

**`dataFalha` sozinho é 65% de toda a perda**, e é um acidente puro: o campo é
uma data em texto em 97% das ocorrências, mas o primeiro evento a trazê-lo
mandou um número.

### Procedência

Amostragem por `SELECT persistent FROM events LIMIT 20000` pelo plano loopback,
com agregação no próprio host — só agregados saíram. O script está em
`/tmp/tipos.py` no piloto e é descartável: a melhoria B abaixo o torna
desnecessário.

---

## Melhoria A — declarar o tipo das chaves quentes

**Declaração vence descoberta.** Declarar o caminho com o tipo majoritário faz
o tipo ser escolhido por medição em vez de por sorte.

```yaml
    persistent:
      type: json
      unknownFields: include        # a descoberta CONTINUA, para o resto
      extract:
        - path: dataFalha
          type: string              # data em texto em 97% das ocorrências
        - path: dataClear
          type: string
        - path: slot
          type: string
        - path: frontPersistence
          type: string
        - path: codigoGrupoResponsavel
          type: string              # ver "a decisão do tipo", abaixo
        - path: dataBaixa
          type: string
        - path: frontSATotal
          type: string
```

Declarar as dez piores recuperaria **~95%** da perda medida.

### A decisão do tipo, chave a chave

Não é mecânico, e o critério não é só "qual é maioria":

- **Datas em texto** (`dataFalha`, `dataClear`, `dataBaixa`): `string` recupera
  quase tudo. Vale considerar `datetime` com `format`, se o padrão for estável
  — aí ordenam cronologicamente, que é o que `firstSeen`/`lastSeen` já fazem
  no topo. Exige medir o formato antes.
- **Contadores** (`curtoTally`, `curtoTTL`, `longoTally`, `longoTTL`,
  `medioTally`, `medioTTL`): inteiros em ~97%, com `string` e `double` na cauda.
  `int64` mantém a perda em 2,7%; `string` inverteria o problema. **Nenhum dos
  dois torna a coluna confiável para filtro exato** — para esses, aceite a
  perda e trate o documento como a verdade.
- **`frontPodName`**: 0,2% de perda em 13.477 ocorrências. Não vale declarar.
- **`frontSATotal`**: 91,3% de perda, mas em 23 ocorrências. Alto em proporção,
  irrelevante em volume. É o caso que mostra por que o denominador importa.

### Custo

`extract` é decisão de **criação** de dataset: acrescentá-lo a um dataset com
linhas é recusado na abertura, porque as linhas antigas ficariam sem a coluna.
Aplicar exige apagar `dados/events/*`, trocar o `groupId` e reler a retenção
(~4h de backlog). Nesta fase do projeto isso já foi feito três vezes e é aceito.

### Verificação

Depois de reingerir, o contador do produto responde sozinho:

```
curl -s --noproxy '*' http://127.0.0.1:9142/v1/stats \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["datasets"][0]["schema"])'
```

O `nestedTypeMismatch` por evento deve cair de ~0,21 para ~0,01.

---

## Melhoria B — densidade e divergência POR COLUNA

O contador de hoje é **por dataset**. Ele diz 2.085.138 e não diz quais das
~1.700 colunas respondem por isso. Sem atribuição, o número é alarme sem
endereço, e a tabela acima só existiu porque foi calculada por um script
descartável, sobre uma amostra.

O alvo é responder por SQL, sobre o dataset **inteiro**:

```sql
SELECT name, type, occurrences, typeMismatches
FROM _meta.fields
WHERE parent = 'persistent' AND typeMismatches > 0
```

### Errata: isto é o `frequency` do desenho original

O rascunho v2 previa uma coluna `frequency` em `_meta.fields`, e a descrevia
como **"a coluna que faz a tabela valer"** — sem densidade, uma lista de nomes
não ajuda ninguém a decidir o que promover.

Eu a cortei na fase de planejamento (decisão D7 do plano), por dois motivos.
**Os dois morreram:**

1. *"Seria varredura"* — só se calculada sob demanda percorrendo o dataset. Um
   contador incremental no materializer não é varredura, e já existem dois
   assim (`discardedFieldCount`, `incompatibleValueCount`), ambos passando no gate
   de zero alocação por evento.
2. *"Contador que nasce zerado"* — num dataset **existente**, um campo presente
   em 100% das linhas reportaria 0%. O argumento caiu quando `extract` e a
   descoberta viraram decisão de criação: o dataset é recriado, e o contador
   conta desde o primeiro evento.

E há uma razão nova, que não existia então: **a contagem de divergência precisa
de denominador.** `2.315 de 2.383` e `93 de 3.334` são coisas muito diferentes,
e o numerador sozinho não distingue.

### Duas colunas, não uma

- **`occurrences`** — quantas vezes a chave apareceu no documento. É o
  `frequency` do desenho original, em valor absoluto; a fração se obtém contra
  o total de eventos, que o `/v1/stats` já publica.
- **`typeMismatches`** — quantas não couberam no tipo fixado.

A divisão fica com quem consulta. Duas colunas cruas compõem; uma coluna de
razão, não.

### Desenho

1. **`CompiledField`** ([internal/schema/compile.go](internal/schema/compile.go))
   ganha dois `atomic.Uint64`. O struct já tem `autoType atomic.Uint32` e o
   godoc já exige uso por ponteiro, então não há regra nova. ~1.700 campos × 16
   bytes é irrelevante.
2. **`CountIncompatibleValue`** já recebe o `*CompiledField` — incrementa o
   segundo contador ali, sem mudança de assinatura.
3. **`occurrences`** incrementa em toda materialização bem-sucedida de coluna
   derivada, em `extractDerived`/`descobreAninhados`
   ([internal/materializer/extract.go](internal/materializer/extract.go)).
4. **Exposição** em `_meta.fields`: duas colunas `int64` na `especOfTable` e
   dois `vals` em `record()`
   ([internal/query/meta/source.go](internal/query/meta/source.go)). Os valores
   são ordenados por FieldID antes do `AppendField` — a ordem já é resolvida lá.
5. **Painel**: o total já aparece. As N piores colunas são trabalho do SQL, não
   do painel — ele é panorama, e isto é diagnóstico.

### O que precisa ser MEDIDO antes de prometer

**`occurrences` incrementa no caminho mais quente do produto** — toda coluna
derivada de todo evento, ~1.700 colunas × 500 ev/s no piloto. Um `atomic.Add`
não aloca, e o writer é goroutine única (sem contenção), mas o efeito na vazão
precisa sair de medição, não de raciocínio. O gate de alocação
([internal/materializer/alloc_gate_test.go](internal/materializer/alloc_gate_test.go))
cobre alocação, não custo de CPU: use `benchmark/` para a vazão.

Se o custo doer, a saída é amostrar (contar 1 em cada N) e publicar o fator na
própria resposta — nunca contar em silêncio por amostragem sem dizer.

### Concorrência

As queries de `_meta.fields` leem esses contadores enquanto o writer os
incrementa. Atômicos resolvem, mas **é exatamente a família dos débitos 60, 69
e 70**, que já mordeu o projeto três vezes. Nada de `int64` simples.

### A chave que SOME do documento continua seguindo o merge

Caso distinto do acima, e deliberadamente NÃO corrigido: quando uma chave
desaparece do documento de um evento, a coluna derivada preserva o último valor
visto, porque é o que o modo merge manda.

A consequência é a mesma família: a coluna pode responder por uma chave que o
documento da linha corrente não tem. Corrigir exigiria emitir uma remoção por
coluna conhecida e ausente — num documento de 11 chaves sobre ~1.650 colunas,
seriam ~1.639 entries por evento, o que é inviável no caminho quente.

**A leitura correta de uma coluna derivada é "o último valor visto para esta
chave", e não "o valor no documento desta linha".** Isso precisa estar dito na
documentação da tabela e do `extract`. Travado por
`TestChaveQueSomeDoDocumentoSegueOMerge`.

### Reset no restart

São contadores de processo, como os outros, e zeram no restart. Aceitável para
a pergunta que respondem ("esta coluna é confiável?"), desde que esteja **dito
na documentação da tabela** — senão alguém lê 0 e conclui "estável".

---

## Perguntas em aberto

1. **Há rota de perda ainda NÃO contada?** Se houver, os 2.085.138 são piso e
   não total. Os candidatos a auditar em
   [extract.go](internal/materializer/extract.go) são os `continue` que não
   passam por `CountIncompatibleValue`: pai não-objeto, caminho ausente, null
   JSON, falha do jsonp em nível interno, `FixAutoType` falhando, nome tomado.
   Alguns são legítimos (caminho ausente não é perda), outros talvez não.
   **Isto precisa ser resolvido antes de tratar o número como total.**
2. **`datetime` com `format` para as datas** recuperaria o valor E daria ordem
   cronológica. Exige medir o formato emitido antes de declarar.
3. **Contagem por tipo divergente**, e não só o total: saber que `dataFalha`
   recebe `string` 2.278 vezes e `double` 37 mudaria o tipo escolhido. Custa
   um contador por (coluna, tipo) — provavelmente caro demais; a alternativa é
   o log, que já nomeia os dois tipos na primeira divergência.

## Ordem sugerida

**B antes de A.** A melhoria B torna a A verificável sobre o dataset inteiro em
vez de sobre uma amostra, e as duas exigem recriar o dataset de qualquer forma —
fazer B primeiro significa uma recriação a menos, e a decisão de tipo de cada
chave passa a sair de `SELECT` em vez de script.
