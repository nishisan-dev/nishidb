# Colunas congeladas e TTL condicional

## Contexto

Duas necessidades vindas do piloto `events` (alarmística Netcool, `mode: merge`, key
`identifier`, ~17,8M entidades):

**1. Campos que registram o estado de nascimento.** `severity` muda ao longo da vida do
alarme. O operador quer `originalSeverity` — a severidade com que o alarme apareceu —
gravada como coluna de verdade, filtrável e indexável, que nenhum evento posterior
altera. O caso é genérico: colunas escritas em função da inserção, imunes ao update.

**2. Expiração dirigida pelo conteúdo.** O `expireAfterWrite` de hoje é único para o
dataset inteiro: toda entidade tem o mesmo prazo de ociosidade. O operador precisa de
prazos diferentes por estado — alarme resolvido (`type = 2`) sai em 3 minutos, o resto
segue com o prazo longo — e precisa que a expiração **se cancele sozinha** quando o
alarme reabre e o `type` volta para 1.

**O que já existe e é reaproveitado:**

| Peça | Onde | O que entrega |
|---|---|---|
| Campo de sistema injetado pelo writer | `_writtenAt`, [spec.go:403](../internal/schema/spec.go:403) | Precedente completo: FieldID pelo dicionário durável, gravado no payload, recusado se vier no evento |
| Índice denso rowId→valor | [`rowindex.LastWrites`](../internal/storage/rowindex/rowindex.go:303) | 8 bytes por linha em mmap, derivado do payload, reconstruído pelo replay |
| Coletor de expiração | [expiry.go](../internal/storage/expiry.go) | Varredura circular, lápide durável, freio duplo (quantidade e tempo) |
| Releitura do prazo sob o lock | [`Expira`](../internal/storage/expiry.go:37) | **O cancelamento**: decisão do agendador é revalidada dentro do `wmu` |
| Predicado compilado | [predicate.go](../internal/query/executor/predicate.go), [collect.go](../internal/query/executor/collect.go) | Programa linear com saltos, sem recursão e sem alocação por linha |

### Decisões tomadas com o usuário

| # | Decisão | Consequência |
|---|---|---|
| 1 | **Relógio renovado a cada escrita** — o prazo é ociosidade, não deadline fixo desde a virada | O prazo continua sendo `últimaEscrita + duração`; o índice de escritas segue servindo |
| 2 | **A condição é um `WHERE`**, com a mesma sintaxe e a mesma semântica da consulta | Exige extrair o avaliador do executor para um pacote compartilhado |
| 3 | **A coluna congelada guarda o primeiro valor VISTO**, não o do nascimento | Implementação é "grava só se ausente"; tolera evento parcial |
| 4 | **Imprecisão de até um minuto é aceitável** | O agendador de hoje serve sem mudança; mínimo declarável segue 1 minuto |
| 5 | **A regra é avaliada na INGESTÃO** e o resultado é gravado no documento | `storage` não conhece predicado; o prazo vira consultável |

---

# Parte 1 — Colunas congeladas

## 1.1 A superfície

```yaml
schema:
  fields:
    severity:         { type: int64 }
    originalSeverity: { frozenFrom: severity }
    createdAt:        { frozenFrom: "@writeTime" }
```

`frozenFrom` nomeia a origem do valor: outra coluna declarada, ou o literal `@writeTime`
— o relógio do escritor, que entrega um "instante de nascimento" com a mesma máquina.

**O tipo é herdado da origem e não é declarável.** `type` junto de `frozenFrom` é
recusado no load. Duas declarações do mesmo fato divergem em silêncio, e é o mesmo
defeito que a validação de `format` já previne. Para `@writeTime` o tipo é `timestamp`,
fixo. Origem `auto` produz coluna congelada `auto`: o tipo se resolve na captura, como
em qualquer coluna auto.

A coluna congelada **não** é `IsSystem`. O nome foi escolhido pelo operador e o valor é
dado do usuário — ela aparece no `SELECT *`, no `/v1/metadata`, aceita `index:` e é
filtrável como qualquer outra. Nada no motor de consulta precisa saber que ela existe.

## 1.2 Validações, todas no LOAD

No laço de campos de `validate()` ([spec.go:761](../internal/schema/spec.go:761)):

1. `frozenFrom` **e** `type` na mesma coluna → recusa.
2. `frozenFrom` nomeando coluna inexistente → recusa listando o nome.
3. `frozenFrom` apontando para a própria coluna → recusa.
4. `frozenFrom` apontando para **outra coluna congelada** → recusa. A cadeia faria a
   ordem de avaliação virar contrato implícito, e o valor dependeria de qual das duas
   foi capturada primeiro.
5. `frozenFrom` apontando para a **key** → recusa. A key identifica a entidade e já é
   constante por construção; congelá-la gasta uma coluna para duplicar um valor que
   nunca muda. A **sequence** é permitida — ela muda, e "a sequence de nascimento" é um
   fato legítimo.
6. `frozenFrom` em dataset `mode: replace` → recusa. Ver §1.5.
7. `format:` junto de `frozenFrom` → recusa: o formato é do parsing da origem, e a
   congelada não parseia nada.

Origem que seja **coluna derivada** (extraída de um documento aninhado) é **permitida**:
a extração roda antes da captura, então o valor já está montado. Ver §1.3.

## 1.3 O que muda na ingestão

`patchEntry` ([patch.go:25](../internal/materializer/patch.go:25)) ganha um bit:

```go
type patchEntry struct {
	field     *schema.CompiledField
	isNull    bool
	congelada bool // grava só se a coluna ainda não existe no estado anterior
	val       types.Value
}
```

`Compiled` ganha a lista das congeladas em ordem de FieldID, pré-computada no
`Compile` — a mesma disciplina que `Derivadas` já segue, porque o materializer a
consulta por evento e não pode varrer o mapa.

Em `PatchBuffer.Parse`, uma **terceira passada**, depois de `extraiDerivadas()` e antes
da ordenação ([patch.go:208](../internal/materializer/patch.go:208)): para cada coluna
congelada declarada, procura a entry da origem entre as já montadas e emite

```go
b.entries = append(b.entries, patchEntry{field: cf, val: <valor da origem>, congelada: true})
```

Origem ausente do evento, ou presente com null explícito → **não emite nada**, e a
coluna continua vazia. `@writeTime` emite sempre, com o mesmo `agora` que o `_writtenAt`
usa — os dois têm que sair do mesmo relógio, senão a mesma escrita produziria dois
instantes diferentes.

Ordem das três passadas: campos do evento → derivadas → congeladas → sort → dedup. É o
que permite congelar uma derivada, e é o que faz a congelada participar do sort e do
`dedupLastWins` como qualquer outra entry.

O valor é copiado por referência ao arena da própria entry de origem, sem cópia nova: o
arena vive até o próximo evento, e `encoding.AppendField` copia os bytes na
serialização. A busca da origem é linear sobre `b.entries`; com uma ou duas congeladas
sobre ~35 entries é ruído. Se um dataset declarar dezenas de congeladas e o benchmark
acusar, a saída é registrar o índice da entry durante o laço de parsing.

**Evento que traz o nome da coluna congelada é RECUSADO** (`SCHEMA_VALIDATION`), pela
mesma guarda e pelo mesmo motivo que o `_writtenAt`
([patch.go:101](../internal/materializer/patch.go:101)): aceitar deixaria o produtor
decidir qual é o valor "original", e a coluna passaria a mentir sem erro nenhum. É
diferente da coluna derivada, que é ignorada em silêncio — lá o nome de topo pode
existir legitimamente no evento com outro significado; aqui o nome foi inventado pelo
operador para esta finalidade.

## 1.4 O que muda no merge

Em `mergeRecords` ([merge.go:66](../internal/materializer/merge.go:66)), o ramo em que o
estado anterior já tem o campo:

```go
if pok && pid == id {
    if e.congelada {
        out = append(out, r.RawField()...) // preserva o anterior, descarta a entry
        pid, _, pok = r.Next()
        continue
    }
    pid, _, pok = r.Next()
}
```

Quatro linhas, e o custo é uma comparação de bool por campo que o patch e o estado têm
em comum — só nas entries congeladas o ramo é tomado.

`encodePatch` ([merge.go:94](../internal/materializer/merge.go:94)) — o caminho do create
— não muda: sem estado anterior, a entry congelada é escrita como qualquer outra.

## 1.5 Por que `mode: replace` é recusado

No replace o payload anterior é descartado inteiro e `encodePatch` sequer o lê. Uma
coluna que sobrevivesse ao replace contradiria o que replace significa — "o evento é a
verdade completa" —, e sustentá-la obrigaria o caminho do replace a ler o estado
anterior só para isso, pagando uma leitura de record por evento no modo cuja razão de
existir é não pagá-la.

Recusar na subida é honesto e custa uma validação. Um dataset que precise das duas
coisas está pedindo `mode: merge`.

## 1.6 Ligar a coluna num dataset que já tem linhas: não há retroativo

A captura olha o **evento**, não o estado. Declarar `originalSeverity` num dataset com
17,8M entidades materializadas não vai atrás do `severity` já gravado: cada entidade
ganha a coluna no primeiro evento seguinte que trouxer a origem.

Para alarme ativo isso é questão de segundos. Para entidade que parou de receber evento,
a coluna fica vazia para sempre — e o `SELECT` a lê como NULL, que é a leitura correta:
o banco não sabe qual era a severidade original dela.

Fazer diferente exigiria, a cada evento, uma varredura do estado anterior atrás do campo
de origem mesmo quando o evento não o traz — uma passada a mais sobre o payload no
caminho quente, para um caso que só ocorre na janela de adoção. **Não entra.** Se o
retroativo importar, ele é um comando de manutenção que varre o dataset uma vez, fora do
caminho de ingestão, e é trabalho separado.

---

# Parte 2 — TTL condicional

## 2.1 A superfície

```yaml
materialization:
  mode: merge
  ttl:
    - when: "type = 2"
      expireAfterWrite: 3m
    - when: "severity >= 4"
      expireAfterWrite: never  # alarme crítico não sai por ociosidade
    - expireAfterWrite: 7d     # regra de fundo
```

**Primeira regra que casa vence.** A regra sem `when` é a regra de fundo: no máximo uma,
obrigatoriamente a última. Nenhuma regra casou e não há regra de fundo → a linha é
imortal.

`when` é a **mesma sintaxe do `WHERE`** da consulta, compilada pelo mesmo parser e pelo
mesmo binder — inclusive `AND`/`OR`, `IN`, `LIKE`, `IS NULL`, comparações de ordem.

### Validações no load

1. `ttl:` presente e vazio → recusa.
2. Regra sem `when` que não seja a última, ou mais de uma → recusa.
3. `expireAfterWrite` ausente em qualquer regra → recusa.
4. Duração menor que `ResolucaoDaExpiracao` (1 minuto,
   [spec.go:85](../internal/schema/spec.go:85)) → recusa. O motivo de hoje continua
   valendo: o coletor varre a cada minuto, e uma chave que promete 30 segundos mente.
   O literal **`never`** é aceito e significa imortal — é o que permite escrever "alarme
   crítico não sai por ociosidade, todo o resto sai em 7 dias" sem abrir mão da regra de
   fundo. `0` como número é recusado: `never` diz a intenção, `0` parece um esquecimento.
5. `when` que não compila → recusa propagando o erro do binder, que já nomeia coluna
   inexistente, operador inválido e tipo incomparável.
6. `when` referenciando `_writtenAt` ou `_expiresIn` → recusa. A duração passaria a
   depender de si mesma, ou do instante que ela própria ajuda a interpretar.
7. Parâmetro posicional (`?`) no `when` → recusa: não há quem os forneça.

## 2.2 A migração do `ttl:` de hoje

`ttl:` deixa de ser um objeto e passa a ser uma **lista**. A forma escalar atual vira
uma lista de um item:

```yaml
# antes                          # depois
ttl:                             ttl:
  expireAfterWrite: 7d             - expireAfterWrite: 7d
```

E a forma `ttl: {field, type}` — o prazo absoluto vindo do payload — é **removida**.
Ela é parseada e validada hoje sem nenhum consumidor: `TTLSpec.Field` só alimenta
`IsTTL` ([compile.go:501](../internal/schema/compile.go:501)), que ninguém lê fora do
próprio `schema`. Mantê-la ao lado de um mecanismo que funciona é a configuração sem
leitor que o projeto já trata como defeito. `IsTTL` sai junto.

Não há forma de compatibilidade: o piloto migra o YAML.

## 2.3 Onde a regra roda e o que ela enxerga

Na **ingestão**, sobre o valor que cada campo terá **depois do merge**:

```
valor pós-merge do campo F = valor de F no patch, se presente e não-null
                             senão o valor de F no estado anterior
                             senão ausente (NULL para o predicado)
```

Como a regra menciona poucos campos, é uma leitura dirigida do estado anterior — uma
passada de `FieldReader` colhendo só os FieldIDs que as regras citam, no mesmo padrão de
[`instanteDeEscrita`](../internal/storage/store.go:1502), interrompida quando o cursor
passa do maior id procurado.

`mergeRecords` já percorre os dois lados e vê o valor final de cada campo enquanto o
emite. Se o benchmark de merge acusar a passada extra, a otimização é colher os slots
ali dentro. **Só com medição** — ela mistura duas responsabilidades num arquivo cuja
clareza vale mais que a passada.

## 2.4 O que fica gravado

A avaliação produz uma **duração**, e ela vira uma coluna de sistema no documento:
`_expiresIn`, entrando em `nomesDeSistema`
([spec.go:395](../internal/schema/spec.go:395)) ao lado do `_writtenAt`.

O tipo físico é **`int64`**, e não `timestamp`. Ela carrega uma DURAÇÃO em millis, e
`timestamp` faria `SELECT _expiresIn` renderizar 3 minutos como
`1970-01-01T00:03:00Z` — um valor que o operador leria como data e que não é uma.

Duração, e não prazo absoluto, por duas razões. Ela se compõe com o `_writtenAt` que já
está no documento, e a linha fica legível — *"esta entidade morre 3 minutos depois da
última escrita"*. E ela sobrevive a reordenar as regras no YAML, o que um ordinal de
regra não faria.

**Ela é emitida em TODA escrita**, como o `_writtenAt`, e não só quando o resultado da
regra muda. É o que faz o cancelamento funcionar por construção: o merge preserva campo
que o evento não menciona, então uma emissão condicional deixaria o `3m` de quando o
alarme estava em `type=2` sobreviver ao evento que o devolve para `type=1` — e a
entidade morreria três minutos depois de reabrir. São 8 bytes por versão de record para
tornar impossível a única falha que arruinaria a feature.

Ela é `IsSystem`: fica fora do `SELECT *` e do `/v1/metadata`, e `SELECT _expiresIn`
pelo nome funciona — que é a observabilidade prometida:

```sql
SELECT identifier, _writtenAt, _expiresIn WHERE _expiresIn > 0
```

### O registro do nome no dicionário

`_expiresIn` entra pelos dois caminhos que o `_writtenAt` já usa, e a **ordem importa**:

- `Compile` acrescenta o nome ao conjunto a compilar quando há regras
  ([compile.go:453](../internal/schema/compile.go:453));
- `Open` faz `LookupOrCreate` **imediatamente depois** do `_writtenAt` e **antes** dos
  campos de bitmap e hash ([store.go:749](../internal/storage/store.go:749)).

A posição é contratual pela razão que o comentário daquele trecho já registra: o
`LookupOrCreate` queima ids na ordem em que é chamado, e essa ordem decide o mapeamento
nome→id de um dataset **novo**, gravado para sempre. Dataset existente não é afetado —
o dicionário é append-only e o nome novo ganha o próximo id livre.

## 2.5 A segunda tabela

`rowindex.TTLs`, arquivo `indexes/prazos.idx`: o mesmo `denseFile` de 8 bytes por
linha que `LastWrites` já usa ([rowindex.go:52](../internal/storage/rowindex/rowindex.go:52)),
com assinatura própria. No piloto são os mesmos ~142 MB do `expiry.idx` que já existe.

Preenchida em `applyRecord` ([store.go:1467](../internal/storage/store.go:1467)), ao
lado do instante de escrita, lendo `_expiresIn` do payload pelo mesmo caminho e pelo
mesmo motivo: `applyRecord` é o único ponto de aplicação de estado e roda tanto no `Put`
quanto no replay do tail, então depois de um crash o replay repõe exatamente as mesmas
durações.

### Imortal precisa ser distinguível de desconhecido

`denseFile.load` devolve `ok=false` quando o valor gravado é zero, então "gravei zero" e
"nunca gravei" são o mesmo byte. Aqui os dois **não** podem significar a mesma coisa:
desconhecido cai no prazo de fundo (§2.6), e imortal não pode cair em lugar nenhum.

Quem torna a distinção obrigatória é o `expireAfterWrite: never` (§2.1). Uma entidade
que casa com uma regra `never` grava duração zero; num dataset que TAMBÉM declara regra
de fundo, o índice leria esse zero como desconhecido e lhe daria o prazo de fundo — o
alarme crítico que o operador mandou nunca expirar sairia em 7 dias, sem erro nenhum.

`TTLs` guarda então um **sentinela**: `ImmortalTTL = 1`. Zero segue significando
desconhecido, `1` significa "avaliado, e nenhuma regra dá prazo", e qualquer outro valor
é a duração em millis. Não há colisão possível — a menor duração declarável é um minuto,
60000 ms.

## 2.6 A conta do prazo

```
prazos.Get(linha) = 0            → DESCONHECIDA  (linha anterior à adoção das regras)
                  = ImmortalTTL  → 0             (avaliada; nenhuma regra dá prazo)
                  = v            → v             (duração em millis)

duração efetiva(linha) = a duração acima, se a entrada for conhecida
                         senão a duração da regra de fundo, se houver
                         senão zero

vencido(linha) = duração efetiva > 0 && agora − escritas.Get(linha) ≥ duração efetiva
```

**A linha do prazo de fundo é o que faz a migração não mudar comportamento.** As entidades já
materializadas não têm `_expiresIn` no documento e, sem o fallback, virariam imortais no
dia em que o piloto migrasse o YAML. Com ele, elas seguem expirando pelo prazo de fundo
exatamente como hoje, e passam a obedecer as regras condicionais na primeira escrita
seguinte.

## 2.7 O que muda no motor: quase nada

[`prazoVencido`](../internal/storage/store.go:1528) passa a fazer a conta de §2.6 em vez
da conta fixa — uma leitura de mmap a mais. Os três consumidores herdam a mudança sem
tocar em nada:

| Consumidor | Onde | O que herda |
|---|---|---|
| Visibilidade | `prazoVencido` na leitura | A linha some da consulta no instante em que vence, com o coletor parado, atrasado ou nunca ligado |
| Coletor | [expiry.go:256](../internal/storage/expiry.go:256) | Marca pela duração da própria linha |
| Compaction | [compaction.go:189](../internal/storage/compaction.go:189) | Descarta record de linha vencida na reescrita |

`Store.Options` troca `ExpireAfterWrite time.Duration` por: `TTLAtivo bool`,
`DuracaoDeFundo time.Duration` e `CampoDePrazo string` (o nome de `_expiresIn`, resolvido
pelo dicionário no Open, como o `CampoDeEscrita` já é). Os três montadores de opts
acompanham: [cli.go:243](../internal/cli/cli.go:243),
[offsets.go:93](../internal/cli/offsets.go:93),
[server.go:332](../internal/server/server.go:332).

`montaLapide` ([expiry.go:161](../internal/storage/expiry.go:161)) **não** preserva
o `_expiresIn`. A lápide não precisa de prazo, e não preservá-lo devolve os bytes. A
entrada em `prazos.idx` permanece com o valor antigo, o que é inofensivo: a linha já
está tombstonada e `Expira` é idempotente. Ver §2.9 para a consequência operacional.

## 2.8 O cancelamento sai de graça

[`Expira`](../internal/storage/expiry.go:37) já relê o prazo **dentro** do `wmu`,
exatamente para não marcar uma linha renovada entre a decisão do agendador e a marca.
Com a duração dentro dessa releitura, o cancelamento não custa uma linha nova:

```
10:00:00  evento type=2   →  _expiresIn = 3m,  _writtenAt = 10:00:00   vence 10:03:00
10:01:30  evento type=1   →  _expiresIn = 0,   _writtenAt = 10:01:30   imortal
10:02:00  coletor varre   →  duração 0: não vence, e a linha nem é candidata
```

E na corrida apertada — coletor decide marcar às 10:01:29 e só toma o lock às 10:01:31 —
ele relê a duração já zerada, `prazoVencido` devolve false, e `Expira` retorna
`expirou=false` sem escrever nada. É o mesmo caminho que hoje conta como `Renovadas`.

## 2.9 A consequência de prazos curtos: rotatividade de lápide

Prazo de 3 minutos produz ordens de magnitude mais lápides que prazo de 7 dias, e isso
expõe um custo que hoje é invisível.

Uma linha tombstonada mantém `_writtenAt` (a lápide o preserva de propósito) e continua
com a entrada antiga em `prazos.idx`, então `prazoVencido` segue devolvendo true. O
coletor a reexamina a cada passada e chama `Expira`, que **toma o `wmu`**, vê a flag e
devolve false. Com prazos de dias isso é ruído; com prazos de minutos, é disputa
contínua pelo lock de escrita com a ingestão, e o freio de `MaxPorPassada` não protege —
ele conta expirações efetivas, não chamadas.

**Mitigação, e ela entra no plano:** o coletor consulta a flag de tombstone em `locs`
**antes** de chamar `Expira`. É uma leitura sem lock, e o pré-filtro é seguro porque
tombstone não se desfaz — a decisão só pode ficar mais conservadora, e `Expira` continua
revalidando tudo sob o lock.

Prazos curtos também aumentam a pressão de compaction. O motor já existe e é dirigido
por liveness de segmento; o efeito esperado é compaction rodando com mais frequência, o
que é o comportamento correto. Fica registrado para leitura das métricas no piloto.

---

# Parte 3 — O trabalho estrutural: extrair o avaliador

O `when` só significa a mesma coisa que o `WHERE` se for o **mesmo código**. Uma segunda
implementação de comparação divergiria nos casos que importam — coluna de texto
comparada com literal numérico, coluna `auto` ainda sem tipo resolvido, ordem de bytes
versus ordem numérica — e divergiria em silêncio.

Pacote novo `internal/predicate`, recebendo:

- `slot` e `coleta` de [collect.go](../internal/query/executor/collect.go);
- `programa`, `instr`, `compila`, `emite`, `eval`, `testa` e os comparadores de
  [predicate.go](../internal/query/executor/predicate.go).

Dependências resultantes: `query/parser`, `query/binder`, `schema`, `types`,
`storage/encoding`. **Nenhuma delas importa `storage` nem `materializer`** — verificado:
`binder` importa apenas parser/schema/types, `schema` não importa `storage`, e
`collect.go` só precisa de `storage/encoding`, que é folha. Não há circularidade.

`executor` passa a consumir o pacote em vez de hospedar o código. É refatoração pura, e
[predicate_prova_test.go](../internal/query/executor/predicate_prova_test.go) e os testes
de binder existentes são a prova de que nada mudou de comportamento.

O `contadores` que o `eval` recebe hoje é do executor. Ele vira uma interface mínima no
pacote novo, ou um ponteiro opcional — a ingestão não tem os mesmos contadores e não
deve inventá-los.

---

# Parte 4 — Observabilidade

Contadores por dataset, ao lado dos que a expiração já publica (`expiradas`,
`examinadas`, `carencias`, `renovadas`):

| Contador | Onde incrementa | Responde |
|---|---|---|
| `ttl.regra[i].casou` | Na avaliação, na ingestão | Qual regra está pegando o quê |
| `ttl.semRegra` | Idem | Quantas escritas não casaram com nada |
| `congelada.capturada` | Na terceira passada do patch | Se a captura está de fato acontecendo |
| `congelada.preservada` | No ramo congelado do merge | Se o congelamento está segurando |

Log de subida: quando há regras, uma linha `Info` listando cada regra, sua duração e se
existe regra de fundo. É o que permite ao operador conferir que o YAML que ele escreveu é
o que o processo entendeu — inclusive a ordem, que é semântica.

---

# Riscos

| # | Risco | Mitigação |
|---|---|---|
| R1 | **Clear reenviado em laço nunca sai.** Com o relógio renovado a cada escrita, uma fonte que reenvia `type=2` a cada 30s nunca completa os 3 minutos de ociosidade, e a linha é imortal | Decisão consciente (decisão 1). Se doer, a saída é um teto de idade absoluta por regra — trabalho separado, fora deste escopo |
| R2 | **Rotatividade de lápide com prazo curto** disputa o `wmu` com a ingestão | Pré-filtro de tombstone no coletor (§2.9), dentro deste escopo |
| R3 | **Trocar a duração no YAML não reescreve as linhas.** A duração nova vale da próxima escrita em diante | Documentado. Para alarme ativo, segundos. Para entidade parada, ela morre com a duração antiga |
| R4 | **142 MB a mais** de índice no piloto | Mesmo custo do índice que já existe; consistente com o modelo de custo do produto |
| R5 | **A extração do avaliador toca o caminho quente da consulta** | Refatoração sem mudança de comportamento, com os testes existentes como rede. Bench de query antes e depois |
| R6 | **Regra sobre coluna `auto`** pode não casar antes de o tipo se resolver | O binder já trata `auto` via `Comparable(col, isAuto, …)`. Documentado, não recusado |

---

# O que NÃO entra

- **Preenchimento retroativo de coluna congelada** (§1.6) — comando de manutenção, depois.
- **Teto de idade absoluta por regra** (R1) — só se o piloto mostrar necessidade.
- **Precisão sub-minuto** — exigiria trocar a varredura circular por fila de prioridade.
- **`frozenFrom` em `mode: replace`** (§1.5) — recusado por contradizer o modo.
- **Compatibilidade com o `ttl:` escalar e com `ttl: {field, type}`** (§2.2) — a forma
  nova é a definitiva; o piloto migra o YAML.

---

# Plano de testes

**Colunas congeladas**

1. Congela no primeiro evento e sobrevive a três updates que mudam a origem.
2. Origem ausente no primeiro evento → coluna vazia; aparece no segundo → captura ali e
   congela.
3. Origem com null explícito não captura.
4. Evento trazendo o nome da congelada é recusado com `SCHEMA_VALIDATION`.
5. Congelada sobre coluna derivada (aninhada) captura o valor extraído.
6. Congelada com `index: bitmap` é filtrável, e a posting não muda no update da origem.
7. `@writeTime` produz o mesmo instante que o `_writtenAt` da mesma escrita.
8. Cada validação de §1.2 recusa no load com mensagem que nomeia o campo.
9. Replay do tail depois de crash reproduz o valor congelado, não o corrente.

**TTL condicional**

10. `type=2` → expira no prazo curto; `type=1` → não expira.
11. **Cancelamento:** entidade com prazo curto recebe evento que quebra a condição antes
    do vencimento e não é marcada. Com relógio de teste
    ([`UsaRelogio`](../internal/storage/expiry.go:16)), sem dormir.
12. **Corrida:** `Expira` chamado com a decisão tomada antes de uma renovação que zera a
    duração devolve `expirou=false` sem escrever. Com seam, como o teste de renovação
    existente.
13. Primeira regra que casa vence, com duas regras que ambas casariam.
14. Regra de fundo aplicada quando nenhuma casa.
15. Linha sem `_expiresIn` (legado) expira pela duração de fundo — a prova da migração.
16. Linha sem `_expiresIn` e sem regra de fundo é imortal.
16b. **O sentinela:** dataset com regra `never` **e** regra de fundo — a entidade que casa
    com a `never` não expira, e a legada sem `_expiresIn` expira pelo prazo de fundo. É a
    prova de que imortal e desconhecido não colapsaram.
16c. Entidade sai de uma regra com prazo para a regra `never` e deixa de vencer; e volta.
17. `prazoVencido` esconde a linha da consulta com o coletor **parado**.
18. Replay do tail reconstrói `prazos.idx` com as mesmas durações.
19. Compaction descarta record de linha vencida por regra condicional.
20. Pré-filtro de tombstone: coletor não chama `Expira` para linha já tombstonada.
21. Cada validação de §2.1 recusa no load.

**Extração do avaliador**

22. A suíte de `executor` e de `binder` passa sem alteração de asserção — é o critério de
    que a extração não mudou comportamento.
23. Bench de query antes e depois, sem regressão.
