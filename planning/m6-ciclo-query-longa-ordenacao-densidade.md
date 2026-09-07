# Ciclo M6 — Query longa, ordenação, e densidade de campos

## Estado da execução (26/08/2026)

| Bloco | Estado | Validação |
|---|---|---|
| **C0+C1** vazamento de inode | **no ar** (`v0.6.0-83`) | 459 fds / 123,21 GB presos no piloto; depois do deploy, 115 GB devolvidos e contador em zero |
| **A1–A4** rotas de perda | **em `main`** | quatro correções, cada uma provada nos dois sentidos |
| **B1–B2** densidade por coluna | **em `main`** | custo do contador indistinguível na bancada (`p=0.310`) |
| **D0** `LIKE`/`NOT LIKE` | **em `main`** | matcher a 3,8 ns no prefixo, zero alocação |
| **D1** `OFFSET` + `--limit` | **em `main`** | o `--limit` apagava a paginação em silêncio |
| **D2** `ORDER BY` | **em `main`** | desempate por rowId; paginação contígua verificada no piloto |
| **S1+S2** spill e sort externo | **no ar** (`v0.6.0-96`) | diferencial memória × disco idêntico linha a linha |
| **D5** índice hash físico | em curso | |
| **C3+C4** query longa e orçamento | em curso | |
| **D3** top-N com heap | **não feito** | o `ORDER BY` funciona pelo caminho completo; sem o atalho, `LIMIT 10` ainda acumula até o teto |
| **C2** snapshot de leitura | **não feito** | query longa ainda vê mistura temporal |
| **D6** scan paralelo | **não feito** | |
| **E** tipos declarados | **não feito** | depende de recriar o dataset |

**A consulta que costura as três frentes responde em produção:**

```sql
SELECT name, type, occurrences, typeMismatches FROM _meta.fields
WHERE parent = 'persistent' ORDER BY typeMismatches DESC LIMIT 12
```

E já entregou achado que a amostragem original não tinha: `persistent.severidade`
(37% de divergência) e `persistent.classificacao` **não estavam na tabela do
documento** — o script de amostra nunca as viu. O `dataFalha` fixou como
`string` desta vez, contra `int64` na medição original, o que confirma na
prática que o tipo sai por sorte de qual evento chega primeiro.

---

> **Antes do ciclo, uma correção urgente e separada:** os blocos **C0 e C1** saem
> numa branch `fix/` própria, mergeada antes do resto. Motivo no achado 8: a
> compaction hoje não devolve disco ao filesystem enquanto o processo vive, e o
> piloto acumula inodes presos agora. A correção exige C1 **inteiro** de qualquer
> forma — fechar o fd sem a barreira mata query em voo.
>
> Ao aprovar: copiar este plano para `planning/`, conforme a convenção do projeto.

## Context

Três frentes chegaram juntas nesta sessão, e a investigação mostrou que elas são
**uma coisa só**, não três:

1. **`ORDER BY`, `OFFSET` e `LIKE`** — pedido direto do usuário. Hoje as três são
   recusadas com `UNSUPPORTED_CLAUSE` ([clauses.go:16](internal/query/parser/clauses.go:16)),
   e o driver JDBC tem teste que **afirma** a recusa
   ([DriverIT.java:268](clients/jdbc/src/test/java/dev/nishisan/nishidb/jdbc/DriverIT.java:268)).
2. **Query longa com spill em disco** — decisão de produto do usuário: *"pensar
   mais no nishidb como uma engine que pode rodar query longas quando
   necessário, então o desafio é atomicidade"*. Nasceu de um erro real em
   produção: `SQL Error [57014]: executor: query interrompida após 1175015
   candidatos: context deadline exceeded [depois de 0 linha(s) entregue(s)]`.
3. **[Melhorias de tipo e densidade de campos](planning/melhorias-tipo-e-densidade-de-campos.md)**
   — contadores por coluna em `_meta.fields`, e tipo declarado para as chaves
   quentes do piloto.

**O que costura as três:** a Melhoria B promete responder *"as N piores colunas"*
por SQL, e sem `ORDER BY` não existe "as N piores". A frente 1 é pré-requisito da
frente 3. E a frente 2 é o que torna a 1 possível sem estourar memória.

O ciclo inteiro roda num único milestone, por decisão do usuário.

---

## O que a leitura do código estabeleceu

### Refutado

Quatro premissas com que esta sessão começou — três minhas, uma do documento de
melhorias — não sobreviveram à leitura:

| Afirmação | O que o código faz |
|---|---|
| "Spill em disco resolve o `57014`" | **Não resolve.** O erro é *timeout*, não memória. O executor já é streaming puro, memória O(batch) e nunca O(resultado) ([executor.go:176](internal/query/executor/executor.go:176)); no caminho de filtro **não existe buffer para transbordar**. Escrever em disco o que já não cabe no tempo só acrescenta I/O ao gargalo |
| "Strings são dicionarizadas, então `LIKE` pode ser avaliado por valor distinto" | **Não são.** Existem três coisas chamadas "dicionário": `fields.dict` mapeia *nome de campo* → FieldID; `values.dict` existe **só** para campos com `index: bitmap` declarado, é chaveado por valor exato e não é ordenado; e o payload real grava os bytes UTF-8 **inline** em cada linha ([encoding/fields.go:26-46](internal/storage/encoding/fields.go:26)). Não há atalho de índice para `LIKE`, nem para `'prefixo%'` |
| "A Melhoria A precisa de suporte a `type:` no `extract`" | **Já existe, e é obrigatório.** `validateExtracts` recusa `type:` ausente ([spec.go:440](internal/schema/spec.go:440)) e recusa `type: auto` de propósito ([spec.go:443](internal/schema/spec.go:443)) — *"a coluna derivada existe para ter tipo"*. `datetime`/`time` já exigem `format:`. **A Melhoria A é YAML mais recriação de dataset: zero linha de Go** |
| "Os 2.085.138 valores perdidos são um piso" (documento de melhorias, Pergunta 1) | **Não são piso nem teto — são um número com viés nos DOIS sentidos.** Ver "Achados" abaixo |

### Confirmado

- **O executor é streaming puro.** `Next()` emite linha a linha, `Options.BatchSize`
  é cadência de check de contexto e não buffer, e o pacote declara em godoc:
  *"A memória do executor é O(batch), nunca O(resultado)"*.
- **A causa raiz do `57014` é o plano.** O planner só usa termos de **igualdade**
  para escolher índice ([planner.go:192-205](internal/query/planner/planner.go:192));
  faixa, `!=`, `IS NULL` e `OR` com qualquer termo não indexado caem em `FullScan`.
  E `hashEligible` exige `p.hash != nil` ([planner.go:220](internal/query/planner/planner.go:220)) —
  o `Store` real **não implementa `LookupHash`**, só o dublê de teste. É o
  **débito 19**, e a coluna `node` do piloto (declarada `index: hash`, e chamada
  na documentação de *"a dimensão de consulta mais valiosa"*) está exatamente
  nessa condição.
- **A conta fecha:** `ScanLive()` é sequencial, uma goroutine, um `GetByRowID` +
  decode por linha. 1.175.015 candidatos em 30s ≈ **39 mil linhas/s**; sobre
  ~17,8M linhas a query precisaria de **~450s**, e o timeout a mata com 6,6%
  varridos.
- **`CountIncompatibleValue` já recebe o `*CompiledField`**
  ([compile.go:270-280](internal/schema/compile.go:270)), então o segundo contador
  da Melhoria B entra sem mudança de assinatura, como o documento previu.
- **`_meta.fields` absorve colunas novas sem lista paralela**: `colunasIDs` sai de
  `FieldsByID()` ([meta.go:160](internal/query/meta/meta.go:160)) e a ordenação por
  FieldID já está resolvida no `sort.Slice` de
  [source.go:158](internal/query/meta/source.go:158).

### Achados que mudaram o desenho

1. **O `extract` sem `as:` muda o NOME da coluna.** `- path: dataFalha` produz a
   coluna `dataFalha`, não `persistent.dataFalha`
   ([spec.go:478-504](internal/schema/spec.go:478)). `SELECT persistent.dataFalha`
   e `WHERE persistent.dataFalha = 'x'` continuam funcionando (o binder resolve
   por `FieldByPath`, que casa em `f.Path` e não em `f.Name`), e `persistent.*`
   continua incluindo a coluna. Quebra quem cita o nome literal qualificado
   (`SELECT "persistent.dataFalha"`), e muda o `name` que `_meta.fields` reporta.
   Declarar `as: persistent.dataFalha` é **impossível** — `as` com ponto é
   recusado em [spec.go:492](internal/schema/spec.go:492).

2. **Há seis rotas de perda NÃO contadas, e uma delas é a errata pela porta que
   ficou aberta.** A auditoria de `extraiDerivadas`/`descobreAninhados`
   ([extract.go](internal/materializer/extract.go)) respondeu a Pergunta 1 do
   documento com um "sim, e pior":

   | Rota | Onde | O que acontece | Gravidade |
   |---|---|---|---|
   | **E3** | [extract.go:47-51](internal/materializer/extract.go:47) | Pai chega como **array, null ou escalar** → `continue` seco para TODAS as derivadas daquele pai, **sem entry de remoção**. No modo merge isso preserva o valor do evento ANTERIOR — coluna e documento se contradizem dentro do mesmo record | 🔴 **Mesma família da errata de 25/08**, corrigida para tipo divergente e não para forma divergente |
   | **D6** | [extract.go:193-195](internal/materializer/extract.go:193) | `FixAutoType` falhou → `continue` seco. As causas são conflito de dicionário **ou falha de I/O ao persistir o hint** (disco cheio, EIO). Uma falha de disco vira coluna ausente, sem erro, sem log, sem métrica | 🔴 Viola a doutrina do projeto: *"Bug e infra nunca entram no balde de dado ruim"* — que o caminho de topo respeita em [patch.go:132-149](internal/materializer/patch.go:132) |
   | **D4-`stop`** | [extract.go:181-186](internal/materializer/extract.go:181) | `ResolveNested` devolve `ErrFieldCeiling` sob política `stop`, e o `continue` **engole o erro**. A política `stop` é silenciosamente rebaixada a `ignore` no caminho aninhado | 🔴 `ResolveUnknown` propaga corretamente ([compile.go:862](internal/schema/compile.go:862)); o aninhado não |
   | **D2** | [extract.go:170-174](internal/materializer/extract.go:170) | Documento malformado no nível 0 aborta a descoberta de **todas** as chaves restantes | 🔴 Perda em bloco, silenciosa |
   | **B2** | [extract.go:111-116](internal/materializer/extract.go:111) | Erro de `jsonp` num nível interno é indistinguível de "chave ausente" | 🟡 |
   | **B3** | [extract.go:138-140](internal/materializer/extract.go:138) | Nó intermediário não é objeto | 🟡 Mesma família do E3 |
   | **D5** | [extract.go:189-192](internal/materializer/extract.go:189) | `null` JSON não fixa tipo → `continue`; no merge preserva o anterior | 🟡 |

3. **E o contador atual tem um falso POSITIVO.** `Convert` devolve
   `Value{Type: TypeNull}` **sem erro** para `KindNull`
   ([typeconv.go:46-48](internal/materializer/typeconv.go:46)), então
   `"dataFalha": null` é contado como `typeMismatch`. Null explícito não é
   divergência de tipo. As rotas do item 2 **deflacionam** o número; este
   **infla**. Por isso os 2.085.138 têm viés nos dois sentidos — e a Melhoria B,
   que existe para dar endereço a esse número, precisa que a auditoria venha
   antes.

4. **Nenhum benchmark exercita o caminho aninhado.** O YAML dos benchmarks
   (`testdata/radius.materialization.yaml`) **não tem nenhum campo `type: json`**,
   e nenhum dos benchmarks do materializer usa extract ou descoberta. O documento
   de melhorias exige que o custo de `occurrences` no caminho quente *"saia de
   medição, não de raciocínio"* — **e o instrumento não existe**. Ele precisa ser
   construído antes da promessa.

5. **A spec proíbe por escrito o que a frente 2 pede.** §66
   (`nishidb-spec-proposal.md:1670`) manda streamar e exige memória `O(batch)`, e
   §128 (`:3005`) lista `NO query result buffering proporcional ao resultado`
   entre as **regras arquiteturais obrigatórias**. O gate
   `TestScanAllocsIndependenteDoNumeroDeRows`
   ([alloc_gate_test.go:26](internal/query/alloc_gate_test.go:26)) trava isso por
   construção, exigindo **igualdade exata** de alocações entre 1.000 e 8.000
   linhas. Nada disso impede o ciclo — mas exige **errata assinada**, não
   contorno silencioso.

6. **`SELECT order FROM x` é query válida hoje**, travada por
   `TestColunaChamadaOrderContinuaValida`
   ([clauses_test.go:77](internal/query/parser/clauses_test.go:77)) e
   documentada em `README.md:641-643` (*"as palavras não viraram keywords do
   lexer"*). Tornar `ORDER` keyword do lexer quebra contrato publicado e
   possivelmente queries do piloto.

7. **Truncar por `query.maxRows` ANTES de ordenar produz resultado errado
   apresentado como certo.** "As 100 mil primeiras por rowId, depois ordenadas" ≠
   "as 100 mil maiores" — e é exatamente o defeito que
   [executor.go:259-266](internal/query/executor/executor.go:259) declara que o
   projeto não tolera.

8. **🔴 A compaction não devolve disco ao filesystem enquanto o processo vive.**
   Achado fora do pedido, confirmado no código, e determinístico — não é corrida.
   `compactaSegmento` lê o segmento que vai aposentar por
   `s.segR.ScanFrom` ([compaction.go:287](internal/storage/compaction.go:287));
   `ScanFrom` chama `r.file(seg)`, que **cacheia** o fd
   ([segment.go:307](internal/storage/segment/segment.go:307)); o cache **não tem
   despejo** — o único `delete(r.fds, id)` vive dentro de `Reader.Close()`
   ([segment.go:444](internal/storage/segment/segment.go:444)), que só roda no
   `Close` do Store ([store.go:1823](internal/storage/store.go:1823)); e
   `aposentaSegmento` faz `os.Remove`
   ([compaction.go:490](internal/storage/compaction.go:490)), cujo unlink tira o
   **nome** e não o **inode**.

   **Nenhum instrumento do projeto enxerga isso**: `dirBytes` do harness,
   `discoDosSegmentos` do e2e e o `Stats.StorageBytes` medem o *listing* do
   diretório, e para os três o arquivo sumiu. Os "3,68 GB em 104 aposentadorias"
   do aceite do M4 são medida de listing, não de filesystem. Isso muda a leitura
   do débito 65 e atinge o piloto, que existe para provar o plateau de disco.

   **MEDIDO NO PILOTO (25/08/2026), e o número é maior do que a teoria sugeria.**
   Com 5h58 de processo: **459 descritores presos, segurando 123,21 GB**. E a
   conta fecha de forma exata — `compaction.bytesRead = 123.210.990.025` contra
   **123.211.019.401** bytes presos, 29 KB de diferença. **Cada byte que a
   compaction já leu é um byte ainda preso**, porque todo segmento que ela lê é
   um segmento que ela aposenta. `currentSegment=507` menos `segments=48` dá
   exatamente os 459. O produto reportava `storage.bytes = 12,7 GB` com ocupação
   real de ~136 GB; a taxa era de **20,6 GB/h ≈ 495 GB/dia**, e o horizonte até
   ENOSPC, **~45 horas**. Corrigido e no ar em `v0.6.0-83-gce35a40`: o deploy
   devolveu **115 GB** ao filesystem (285G→170G usados) e o contador de
   descritores presos foi a zero.

   **E a correção ingênua quebra query.** Hoje o fd aberto é o que salva a janela
   entre `locs.Get` ([store.go:1016](internal/storage/store.go:1016)) e
   `segR.ReadRecord` ([store.go:1024](internal/storage/store.go:1024)): com o
   nome já removido, o `pread` sobre o inode ainda devolve bytes válidos com CRC
   correto. Fechar o fd sem barreira faz a leitura seguinte cair em `os.ErrClosed`
   ou ENOENT e abortar a query. **A ordem é obrigatória: barreira primeiro,
   fechamento depois.**

9. **O `OFFSET` quebra o `--limit` do CLI — o débito 46 vira pré-requisito, não
   dívida.** `comLimit` ([query.go:156](internal/cli/query.go:156)) reescreve o
   **texto** do SQL: corta na última cláusula `LIMIT` e anexa a sua. Com
   `OFFSET`, `SELECT ... LIMIT 10 OFFSET 20` vira `SELECT ... OFFSET 20 LIMIT 5`
   — ordem inválida, e o usuário recebe erro de sintaxe sobre um texto que não
   escreveu. O próprio godoc do débito 46 já nomeia a saída definitiva (*"o
   `--limit` virar parâmetro do plano"*, `docs/debitos.md:91`), e o `ORDER BY`
   depois do `LIMIT` agrava o mesmo flanco. **Precisa entrar antes do Bloco D.**

10. **`runtime.query.maxConcurrent` é um canal de 16 vagas**
   ([config.go:184](internal/config/config.go:184),
   [server.go:240](internal/server/server.go:240)). Com prazo longo por padrão
   (D1b), as 16 podem ficar ocupadas por minutos e o servidor responde 503 a
   todo mundo. A política de vaga deixa de ser detalhe.

11. **Métrica nova exige justificativa registrada.** Os nomes vivem em
   [metrics/names.go](internal/metrics/names.go) e a doutrina é que cada um
   *"existe por uma razão registrada"*; o débito 61 mostra o custo de publicar
   nome fora do §92. As séries de snapshot e spill precisam dessa justificativa
   por escrito, não só do código.

12. **§9 exclui `MVCC completo` e `transações ACID multi-registro` da v1**
   (`nishidb-spec-proposal.md:264-290`). O snapshot de leitura que a frente 2
   pede **não é** nenhum dos dois, e a errata precisa dizer isso com essas
   palavras — senão parece que o ciclo está revogando o §9 inteiro.

13. **Débito 77 é a armadilha operacional das duas melhorias.** Ligar
   `unknownFields: include` num dataset com linhas **não é recusado** hoje. A
   Melhoria A troca colunas descobertas por colunas declaradas, e o nome novo
   **é** pego pela guarda `validaDerivadasContraDicionario`
   ([store.go:1857](internal/storage/store.go:1857)) — a recriação é cobrada
   corretamente. Mas o débito continua aberto ao lado.

---

## Decisões

| # | Decisão | Escolha |
|---|---|---|
| D0 | O vazamento de inode | **Extraído como correção urgente** (`fix/`), antes do ciclo — por decisão do usuário |
| D1 | Sequência das frentes | **Tudo num ciclo só, em cinco blocos commitáveis**, por decisão do usuário. O gate de aceite é **por bloco**, não um só no fim — é o que impede o ciclo longo de tornar o rollback grosso |
| D1b | Política de query longa | **Prazo longo por padrão, para todo mundo** — por decisão do usuário. `query.defaultTimeout` sobe de 30s para a ordem de minutos. **Isto torna obrigatórias três mitigações** que de outra forma seriam opcionais: teto de retenção do snapshot, métrica da query mais antiga em voo, e política explícita de vaga de concorrência (`runtime.query.maxConcurrent`, hoje 503 + `Retry-After` em [query.go:96-107](internal/server/query.go:96)) — porque uma query mal escrita passa a segurar snapshot, vaga e disco de spill por minutos, com a compaction esperando junto |
| D2 | Semântica de memória do `ORDER BY` | **Ordenação completa com spill em disco**, por decisão do usuário — não top-N. O heap top-N entra como **caminho rápido dentro** do sort completo quando o resultado cabe no orçamento, nunca como substituto |
| D3 | Alcance do `LIKE` | **`LIKE` e `NOT LIKE`, com `%` e `_`. Sem cláusula `ESCAPE`** — por decisão do usuário. Limitação registrada: buscar `%` ou `_` literal fica impossível. O desenho não fecha a porta para acrescentá-la depois |
| D4 | `OFFSET` | **Entra junto**, porque só tem sentido sobre ordenação estável |
| D5 | Auditoria de perda antes da Melhoria B | **Sim.** As seis rotas do achado 2 e o falso positivo do achado 3 mudam o número que a Melhoria B existe para endereçar. Contar melhor um número enviesado é dar endereço ao erro |
| D6 | Onde `occurrences` incrementa | **Na ocorrência da chave no documento**, não na materialização bem-sucedida — em [extract.go:58](internal/materializer/extract.go:58) e [extract.go:187](internal/materializer/extract.go:187). Assim `typeMismatches ⊆ occurrences` e a leitura `2.315 de 2.383` sai direta do SQL, sem soma na cabeça de quem consulta |
| D2b | Alcance da atomicidade | **Snapshot de LEITURA** — imunidade à compaction, conjunto de rows fixo, relógio congelado. **Isolamento de snapshot (C6) fica fora**, por decisão do usuário: entregar cada linha na versão de T0 exigiria journal de before-images no commit point do writer, e o §127 põe correção antes de feature |
| D2c | "Spill geral, inclusive para filtros" | **Honrado onde há acumulação de verdade, e dito onde não há.** No caminho de filtro nada acumula — o executor é O(1) e não existe buffer para transbordar. O que existe e é real é o `StreamWriter`, que **nunca encolhe**: uma linha de 1 MiB fixa a capacidade pela vida do writer (débito 28). Ele passa a viver sob `runtime.query.maxBufferPerQuery` em **C4**. A primitiva de spill nasce geral e reutilizável em **C5**, com o `ORDER BY` como primeiro consumidor. **Não** se acrescenta I/O a um caminho que hoje não gasta nenhum |
| D8 | Palavras da cláusula | **Nenhuma vira keyword do lexer.** Reconhecimento contextual por posição. Continuam 11 keywords e 29 tokens; `TestColunaChamadaOrderContinuaValida` passa sem alteração |
| D9 | `LIKE` na AST | **Nó novo (`LikeExpr`/`BoundLike`), não `CmpOp` novo** — um `CmpOp` novo cairia no `default: // CmpLte` de três funções e seria avaliado como `<=` com a suíte verde |
| D10 | `NULL` na ordenação | **Ausente é SEMPRE o último, em ASC e em DESC.** `NULLS FIRST`/`LAST` explícitos existem para quem quiser outra convenção. **Diverge de Postgres/Oracle** (que invertem no DESC) e de MySQL/SQLite (NULL como menor), e é deliberado: é o único default em que `ORDER BY x DESC LIMIT 10` significa "os 10 maiores" e não "10 linhas sem valor". A divergência vai por escrito no README |
| D11 | Desempate | **rowID crescente, sempre, como última chave implícita.** Sem ordem total, `LIMIT 10 OFFSET 10` sobre coluna com empates devolve páginas que se **sobrepõem e omitem** linhas |
| D12 | Teto de `maxRows` com `ORDER BY` | **Limita a SAÍDA do sort, nunca a ENTRADA.** A entrada é limitada por orçamento de memória e de spill, e estourá-los **recusa a query**. Truncar a entrada daria "as N primeiras por rowId, ordenadas" — resposta errada com cara de certa. **Consequência dura: com `ORDER BY` a fonte é sempre drenada até o fim**, e o `LIMIT` deixa de ser teto de trabalho |
| D13 | `_` do `LIKE` | **Um CARACTERE (rune), não um byte.** JSON é UTF-8 por definição, e `_` casando meio `ç` seria indefensável |
| D14 | Backfill de índice hash | **Recusado, como no bitmap.** O débito 22 **não fecha** — passa a ter dois motores |
| D15 | Default de `query.scanWorkers` | **1 (serial) no primeiro corte.** A funcionalidade entra completa; virar o default é decisão separada, com o número da medição na mão — ligar por default mudaria o perfil de alocação do gate sem número que o justifique |
| D7 | `occurrences` para campo de TOPO | **NULL, não zero.** Segue o precedente de `parent` ([source.go:139-144](internal/query/meta/source.go:139)): *"NULL é ausência"*. Zero mentiria, dizendo "nunca apareceu" |

---

## Fases

### Bloco A — Auditoria e correção das rotas de perda

**Por que primeiro:** a Melhoria B dá endereço a um número, e o número está com
viés nos dois sentidos. Contar melhor antes de contar certo é ampliar o erro.
Este bloco não depende de nenhuma das outras frentes e é commitável sozinho.

#### A1 — O pai que não é objeto passa a emitir remoção

`Ordem interna:`
1. Em [extract.go:47-51](internal/materializer/extract.go:47), trocar o `continue`
   seco por emissão de **entry de remoção** para cada derivada daquele pai —
   exatamente o que o commit `f966649` fez para o valor de tipo divergente.
2. Decidir se conta em `CountIncompatibleValue`: **sim**, com o `veio` sendo o
   kind do pai — é divergência de forma, mesma família.
3. Estender `TestPaiArrayNaoDerrubaOEvento`
   ([extract_test.go:73](internal/materializer/extract_test.go:73)) e
   `TestPaiNuloNaoDerrubaOEvento` ([:85](internal/materializer/extract_test.go:85)),
   que hoje só verificam entidade **nova** e nunca merge.

**Entrega:** a coluna deixa de responder pelo valor de um evento anterior quando
o documento corrente traz o pai como array ou null. É a errata de 25/08 fechada
pela porta que ficou aberta.
**Testes:** `TestPaiArrayNaoCongelaOAnterior`,
`TestPaiNuloNaoCongelaOAnterior` — no molde de
`TestValorDivergenteNaoCongelaOAnterior`
([discovery_test.go:216](internal/materializer/discovery_test.go:216)), com duas
`apply` na MESMA key.
**Risco:** baixo. Mesmo mecanismo já provado no commit anterior.

#### A2 — Falha de infra deixa de virar coluna ausente

`Ordem interna:`
1. Em [extract.go:193-195](internal/materializer/extract.go:193), separar as duas
   causas de `FixAutoType` falhar: conflito de tipo do dicionário (dado) versus
   falha de I/O ao persistir o hint (infra).
2. Infra propaga como `STORAGE_ERROR`, no molde de
   [patch.go:132-149](internal/materializer/patch.go:132).
3. Em [extract.go:181-186](internal/materializer/extract.go:181), deixar de engolir
   `ErrFieldCeiling` sob política `stop` — propagar, como `ResolveUnknown` já faz
   em [compile.go:862](internal/schema/compile.go:862).

**Entrega:** disco cheio deixa de virar coluna silenciosamente ausente, e a
política `stop` deixa de ser rebaixada a `ignore` no caminho aninhado.
**Testes:** `TestFalhaDeIoAoFixarTipoNaoViraColunaAusente`,
`TestTetoSobStopParaAIngestaoTambemNoCaminhoAninhado`.
**Risco:** médio — muda o que derruba a ingestão. Mitigado por ser a doutrina
já escrita e já aplicada no caminho de topo.

#### A3 — `null` explícito deixa de contar como divergência de tipo

`Ordem interna:`
1. Em [extract.go:61-77](internal/materializer/extract.go:61) e
   [:198-216](internal/materializer/extract.go:198), distinguir
   `InferType(f) == TypeNull` de divergência real antes de chamar
   `CountIncompatibleValue`.
2. A remoção explícita **continua** sendo emitida (null no documento deve limpar
   a coluna); o que muda é só a contagem.

**Entrega:** o contador passa a medir divergência de tipo, e não presença de
null.
**Testes:** `TestNullExplicitoNaoContaComoTipoIncompativel`,
`TestNullExplicitoAindaLimpaAColuna`.
**Risco:** baixo. Muda um número publicado — precisa de nota na documentação.

#### A4 — Benchmark do caminho aninhado

`Ordem interna:`
1. YAML de bench com um campo `type: json` e `unknownFields: include`, no molde
   de `descobreYAML` ([discovery_test.go:9](internal/materializer/discovery_test.go:9)).
2. `BenchmarkParseComDescoberta` sobre um evento de ~11 chaves, no molde de
   `BenchmarkPatchParseRadiusEvent`
   ([patch_bench_test.go:26](internal/materializer/patch_bench_test.go:26)).
3. Rodar e **registrar a baseline** antes de qualquer contador novo.

**Entrega:** o instrumento que a promessa do documento de melhorias exige.
**Por que antes de B:** sem ele, "o efeito na vazão precisa sair de medição" não
tem como ser cumprido.
**Risco:** nenhum — é instrumento, não produção.

---

### Bloco B — Densidade por coluna (Melhoria B)

#### B1 — Os dois contadores no `CompiledField`

`Ordem interna:`
1. Dois `atomic.Uint64` em `CompiledField`, ao lado de
   `autoType atomic.Uint32` ([compile.go:152](internal/schema/compile.go:152)). O
   godoc já exige uso por ponteiro, então **não há regra nova**. ~1.700 campos ×
   16 bytes é irrelevante.
2. `CountIncompatibleValue` incrementa o segundo em
   [compile.go:275](internal/schema/compile.go:275) — **sem mudança de
   assinatura**, ela já recebe o `*CompiledField`.
3. Acessores exportados (o pacote `meta` não vê campos não-exportados).
4. `occurrences` incrementa em [extract.go:58](internal/materializer/extract.go:58)
   e [:187](internal/materializer/extract.go:187), conforme D6.

**Entrega:** os contadores existem e sobem. Nada os expõe ainda.
**Testes:** no molde de `TestValorIncompativelEContado`
([discovery_test.go:162](internal/materializer/discovery_test.go:162)).
**Gate obrigatório:** `TestExtracaoNaoAloca` e `TestDescobertaNaoAlocaEmRegime`
exigem **zero** alocação, não "poucas". Um `atomic.Add` não aloca — mas o gate
mede alocação e **não CPU**: rodar o A4 antes e depois é o que fecha a promessa.
**Concorrência:** `atomic.Uint64`, nunca `int64` simples. É a família dos débitos
60/69/70, cuja lição registrada é que *"degrada com segurança"* não sobreviveu ao
`-race`. `make test-race` é o gate.
**Risco:** médio, concentrado na vazão do caminho quente.

#### B2 — As duas colunas em `_meta.fields`

`Ordem interna:`
1. `occurrences, typeMismatches` no `include` de
   [meta.go:122](internal/query/meta/meta.go:122).
2. Bloco `fields:` com os dois `type: int64`, após
   [meta.go:137](internal/query/meta/meta.go:137).
3. Dois pares no `vals` de `record()`, **dentro do `if c.Parent != nil`**
   ([source.go:139-144](internal/query/meta/source.go:139)), conforme D7.
4. Helper `inteiro(n uint64) types.Value` após
   [source.go:183](internal/query/meta/source.go:183).
5. Nada mais muda: o `sort.Slice` de [source.go:158](internal/query/meta/source.go:158)
   e o `AppendField` absorvem as colunas novas, e `colunasIDs` também.

**Entrega:** a consulta do documento de melhorias passa a responder:
```sql
SELECT name, type, occurrences, typeMismatches
FROM _meta.fields WHERE parent = 'persistent' AND typeMismatches > 0
```
**Testes:** molde de `linhas(t, sql, c)`
([meta_test.go:51](internal/query/meta/meta_test.go:51)).
**Nota:** os FieldIDs da tabela virtual mudam ao acrescentar colunas ao `include`
(a ordem é alfabética, [compile.go:364](internal/schema/compile.go:364)) —
inofensivo, o dicionário dela é em memória, sem disco.
**Risco:** baixo.

#### B3 — Documentação da tabela e do `extract`

`Ordem interna:`
1. `README.md:599-610` (`_meta.fields`): as duas colunas, o **reset no restart**,
   e a leitura correta de coluna derivada — *"o último valor visto para esta
   chave"*, e não *"o valor no documento desta linha"*.
2. `README.md:220-221`: a frase *"valor divergente depois fica nulo na coluna"*
   já está imprecisa desde o commit `f966649` (hoje é **remoção explícita**).
3. `README.md:242-244` ("O que não vira coluna"): as rotas do Bloco A.
4. `docs/architecture.md:395-417` (Introspecção pelo próprio SQL).
5. `planning/feature-campos-aninhados-plano.md:146` — errata da **decisão D7**,
   que cortou o `frequency`; este ciclo a reverte, e a reversão precisa ficar
   registrada lá também.

**Risco:** nenhum, mas é DoD: o CLAUDE.md do usuário exige documentação e
diagramas atualizados no mesmo PR.

---

### Bloco C — Snapshot, orçamento de memória e query longa

**A palavra "atomicidade" cobre três coisas com custos separados por ordens de
grandeza.** O bloco entrega as duas primeiras e deixa a terceira atrás de
medição:

| | O que é | Estado |
|---|---|---|
| **Imunidade à compaction** | a query não morre porque um segmento foi aposentado debaixo dela | **C1** — e hoje só não quebra por acidente (o fd que vaza) |
| **Snapshot de leitura** | conjunto de rows fixo e relógio congelado: a query vê o mesmo universo do começo ao fim | **C2** |
| **Isolamento de snapshot** | cada linha na versão vigente em T0 — o resultado corresponde a **um instante** do banco | **C6, opcional** — exige journal de before-images no commit point do writer |

#### C1 — Barreira de aposentadoria e devolução real do disco

`Ordem interna:`
1. `aposMu sync.RWMutex` novo no `Store`, ao lado de `vida`
   ([store.go:332](internal/storage/store.go:332)). O godoc registra o que ele
   **não** é: `vida` guarda ciclo de vida ([store.go:546](internal/storage/store.go:546)),
   `wmu` serializa o writer, e `aposMu` guarda **a janela entre resolver uma
   location e ler os bytes que ela nomeia**.
2. `RLock` nos quatro pares (Get, Read): `lookup`
   ([store.go:937](internal/storage/store.go:937)), `GetByRowID`
   ([:1016](internal/storage/store.go:1016)), `Expira`
   ([expiry.go:60](internal/storage/expiry.go:60)) e `reapendaSeVivo`
   ([compaction.go:130](internal/storage/compaction.go:130)).
3. `Reader.CloseSegment(seg)` novo em
   [segment.go](internal/storage/segment/segment.go): `delete(r.fds, seg)` +
   `Close()` sob o `mu` que já existe.
4. `aposentaSegmento` segura `aposMu.Lock()` do `os.Rename`
   ([compaction.go:455](internal/storage/compaction.go:455)) até depois do
   `os.Remove` ([:490](internal/storage/compaction.go:490)), com `CloseSegment`
   entre os dois. **Os dois `Checkpoint()` ficam FORA** — eles tomam `wmu` e podem
   levar 30s.
5. `podaSegmentoAposentado` ([compaction.go:556](internal/storage/compaction.go:556))
   sai da seção de `aposMu` e vai para depois do unlink.
6. Rótulo `fechaFd` em `apTrace` ([store.go:445](internal/storage/store.go:445)),
   emitido **de dentro** do helper — pela doutrina de que rótulo ao lado da
   chamada não é oráculo.

> **⚠️ Inversão de ordem de lock — a armadilha desta fase.** `Put` toma `wmu` e,
> dentro dele, faz `lookup` → `ReadRecord`
> ([store.go:1087](internal/storage/store.go:1087)) — ou seja, tomaria
> `aposMu.RLock` **com `wmu` na mão**. E `podaSegmentoAposentado` toma `wmu`
> ([compaction.go:561](internal/storage/compaction.go:561)). Se a poda ficasse
> dentro da seção de `aposMu`, seria `aposMu → wmu` de um lado e `wmu → aposMu`
> do outro: **deadlock**. A invariante fica escrita e vira teste — *`aposMu` é
> sempre o lock mais interno; nada tomado sob ele pode tomar `wmu`*. É o passo 5.

**Entrega:** a compaction devolve disco ao **filesystem**, e não só ao listing.
**Testes:** `TestSegmentoAposentadoDevolveOsBlocosAoFilesystem`,
`TestAposentadoriaEsperaALeituraEmVooAntesDeFecharOFd`,
`TestQueryLongaNaoMorreComACompactionAposentandoDebaixoDela`,
`TestAposMuNuncaEhTomadoAntesDoWmu`,
`TestApagarOFechamentoDoFdDeixaOTraceSemORotulo`.
**Risco:** **alto — o mais alto do ciclo.** Mexe no único código do projeto que
apaga arquivo, e põe um lock no caminho quente de leitura. Mitigação: a suíte de
crash da aposentadoria ([compaction_crash_test.go](internal/storage/compaction_crash_test.go),
789 linhas) **não deve precisar mudar** — se precisar, a mudança saiu do lugar.

#### C0 — Caracterização (antes de C1)

Nenhuma linha de produto muda. Cinco fatos que hoje são opinião viram asserção:
o vazamento de inode medido por `statfs` (e não por listing), a varredura que
entrega a versão nova de row atualizada à frente do cursor, o plano indexado que
enxerga row nascida depois do `Open` enquanto o full scan não, a row que vence no
meio da varredura sem ninguém escrever, e `BenchmarkGetByRowID` como linha de
base do custo que C1 vai acrescentar.
**Risco:** nenhum ao produto. Roda sob a tag `integration` — o teste de disco
exige filesystem real, não `tmpfs`.

#### C2 — Snapshot de leitura

`Ordem interna:`
1. `storage.Snapshot` — tipo de valor com `LimitRowID` e `Instant`.
   `Store.Snapshot()` o produz com dois loads, zero alocação.
2. `prazoVencido` ([store.go:1333](internal/storage/store.go:1333)) ganha
   parâmetro `em time.Time` e deixa de chamar `s.agora()`. Os leitores passam o
   instante do snapshot; `reapendaSeVivo` continua passando `s.agora()`, porque a
   expiração oportunista é do writer.
3. `executor.Store` ganha `GetByRowIDEm`/`ScanLiveEm`/`LookupBitmapEm`; o
   `fakestorage` acompanha, senão o alloc gate passa a testar outra coisa.
4. `executor.Open` toma o snapshot **antes** de `fonteDoPlano`
   ([executor.go:125](internal/query/executor/executor.go:125)). `ScanLive` perde
   o cálculo do limite ([store.go:1632](internal/storage/store.go:1632)) — quem o
   fixa passa a ser o executor, para os quatro planos.
5. `candidateSource.advance` filtra `id >= snap.LimitRowID` **antes** do
   `GetByRowID`, o que dá limite uniforme a bitmap, hash, interseção e união de
   graça.
6. O snapshot viaja em `Options`, nunca em campo do `Store`: dois leitores
   concorrentes têm snapshots diferentes, e um campo os fundiria.

**Entrega:** a mesma query devolve o mesmo conjunto de rows em qualquer plano, e
uma query longa sobre dataset com TTL não vê linhas evaporarem no meio da
varredura.

> **Corrige uma falha de previsibilidade que é anterior a este ciclo:** o full
> scan tem limite de rowID ([store.go:1632](internal/storage/store.go:1632)), o
> scan de bitmap **não** — `Iterator` fixa `nWords` pelo tamanho do arquivo
> ([bitmap.go:360](internal/index/bitmap/bitmap.go:360)), e bit ligado dentro de
> uma word já mapeada aparece na varredura em curso. Hoje a mesma query devolve
> conjuntos diferentes conforme o planner escolha índice ou full scan.

**Testes:** `TestOsDoisPlanosEnxergamOMesmoConjuntoDeRows`,
`TestRowQueVenceNoMeioDaVarreduraContinuaVisivelAteOFimDaQuery`,
`TestRowNascidaDepoisDoOpenNaoApareceNemPeloIndice`,
`TestSnapshotNaoAlocaNoOpen`, `TestDoisLeitoresConcorrentesTemSnapshotsIndependentes`.
**Risco:** médio — a mudança de assinatura de `prazoVencido` toca os três pontos
de liveness que o débito 25 fechou.

#### C3 — Query longa como cidadã

`Ordem interna:`
1. `query.maxTimeout` no schema do dataset, **com default igual a
   `query.timeout`** — um `nishidb.yaml` de hoje produz comportamento byte a byte
   idêntico. Validação: `maxTimeout >= timeout`, senão recusa nomeando as duas
   chaves.
2. `contextoDaConsulta` ([query.go:212](internal/server/query.go:212)) compara
   contra `MaxTimeout` em vez de `Timeout`; a ausência de `timeoutMs` passa a usar
   `Timeout` **explicitamente** — o fallback de
   [executor.go:51](internal/query/executor/executor.go:51) é uma segunda fonte de
   verdade e morre aqui.
3. `/v1/metadata` publica `timeoutMs = MaxTimeout` e acrescenta
   `defaultTimeoutMs`. **Zero mudança em Java**: o clamp de
   [NishiStatement.java:141](clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiStatement.java:141)
   passa a clampar contra o teto novo sozinho.
4. Sub-pool `runtime.query.maxLongRunning` (default 2) dentro de `maxConcurrent`:
   query longa toma **duas** vagas, e a recusa do pool longo tem código próprio
   `TOO_MANY_LONG_QUERIES`. **Sem isso, 16 queries longas ocupam o semáforo
   inteiro e toda query curta recebe 503** — que é exatamente o risco que a
   decisão D1b criou.
5. Registro de queries em voo (`{dataset, início, snapshot, hash do SQL}`),
   alimentando `/v1/stats` e o `nishidb status`, que hoje só imprime
   `consultas: N` ([operation.go:276](internal/cli/operation.go:276)).
6. Três métricas novas, declaradas como exceções ao §92 com a razão junto da
   constante: `nishidb_query_longest_running_seconds`,
   `nishidb_query_snapshot_oldest_seconds`, `nishidb_query_long_rejected_total`.

> **Achado que encurta esta fase:** o caminho do timeout longo **já existe
> inteiro** e está represado num único número. `query.timeout` é ao mesmo tempo o
> default e o teto; subir o teto dá o prazo a todo cliente que esqueceu de chamar
> `setQueryTimeout`. Separar as duas chaves é a fase inteira.

**Testes:** `TestTimeoutDoRequestPodeAlongarAteOMaxTimeoutENuncaAlemDele`,
`TestDatasetSemMaxTimeoutSeComportaExatamenteComoHoje`,
`TestQueryLongaNaoOcupaAUltimaVagaDaQueryCurta`,
`TestStatusMostraAQueryLongaEmAndamentoEAIdadeDoSnapshot`,
`TestClienteQueDesisteLiberaAVagaEOSnapshot`.
**Risco:** baixo-médio.

#### C4 — Orçamento de memória (fecha metade do débito 15 e o débito 28)

`runtime.memoryTarget` e `runtime.query.maxBufferPerQuery` são parseados e
**nunca lidos** hoje ([config.go:135-139](internal/config/config.go:135)) — só
nomeados na subida por `IgnoredRuntimeKeys`. Ganham leitor, com os
literais do §80 como default.

**O primeiro consumidor real é o caminho de SELECT**, e é aqui que a sua decisão
de "spill geral, inclusive para filtros" ganha substância verdadeira: o
`StreamWriter` ([ndjson_writer.go](internal/query/executor/ndjson_writer.go))
**nunca encolhe** — uma linha de 1 MiB fixa a capacidade pela vida do writer. É o
débito 28, cujo próprio texto diz *"é o `runtime.query.maxBufferPerQuery` do
§80"*. O writer passa a recusar crescer além do orçamento, e a linha que não cabe
vira erro nomeado (`ROW_TOO_LARGE`) em vez de crescimento silencioso.

**Testes:** `TestMaxBufferPerQueryTemLeitorEValidacao`,
`TestStreamWriterRecusaCrescerAlemDoOrcamento`,
`TestOrcamentoGlobalRecusaAQuintaQueryQuandoAsQuatroJaOcupamOMemoryTarget`,
`TestOrcamentoNaoAlocaNoCaminhoQuente`.
**Risco:** baixo.

#### C5 — `internal/query/spill`: a primitiva

- **API**, com o contrato de cópia no próprio nome do método:
  `AcrescentaCopiando(row)`, `Rewind()`, `Next()`, `Close()`. É
  `AcrescentaCopiando` e não `Acrescenta` porque `ColView.Val.Bytes` vale só até
  o próximo `Next()` ([row.go:7-8](internal/query/executor/row.go:7)) — é a
  armadilha número um do componente, e um nome neutro convidaria a reter a slice.
- **Onde vivem os arquivos:** `<Dir>/spill/`, ao lado de `data/`, `indexes/` e
  `checkpoint/`. Dentro do dataset por três razões: fica sob o mesmo `flock`
  exclusivo, então nenhum outro processo pode confundir os arquivos; herda a
  política de filesystem do dataset; e o operador que dimensionou o disco pelo
  §85 não precisa dimensionar um segundo. Chave `query.spill.dir` para quem
  quiser outro dispositivo, mas o default é dentro.
- **Limpeza em crash:** no `Open`, apaga tudo em `spill/`. É seguro **por
  construção** — o `flock` já garantiu que nenhum outro processo tem o dataset,
  então todo arquivo ali é resíduo de processo morto.
- **Formato:** `encoding.BuildRecord`/`ParseRecord` reusados — já têm CRC32C,
  tetos aplicados antes de qualquer alocação, e um leitor de streaming que para
  no primeiro record inválido. Formato próprio custaria um segundo encoder, um
  segundo fuzz, e a primeira divergência entre os dois seria bug de resultado. O
  que **não** se reusa é o header de 64 bytes de segmento: arquivo de spill não é
  segmento e não pode parecer um no `ListSegments`.
- **O gate de alocação:** `TestScanAllocsIndependenteDoNumeroDeRows` **não
  afrouxa** — continua com igualdade exata e `tetoPorQuery = 38`. Muda o **nome**
  e o godoc, para dizer que governa o caminho **sem operador acumulador**. O gate
  novo, `TestMemoriaDoAcumuladorEhLimitadaPeloOrcamentoENaoPeloResultado`, é uma
  afirmação diferente e mede **bytes de heap** com o resultado variando 10×.

> **Nota sobre o gate atual, que vale registrar:** ele roda contra `fakestorage`
> ([harness_test.go:48](internal/query/harness_test.go:48)), então **nunca viu o
> storage real** — qualquer alocação por row abaixo do executor é invisível para
> ele, apesar do comentário afirmar "a memória do executor é O(batch), NUNCA
> O(resultado)".

**Testes:** `TestAcumuladorCopiaAsBytesDaRowEmVezDeReterOBuffer` — cujo oráculo é
**mutar o buffer do iterador** depois do `Acrescenta` e exigir que o valor relido
não mude; um teste que só releia passaria com a slice retida, porque na maioria
dos casos o buffer ainda não foi reescrito —,
`TestAcumuladorTransbordaExatamenteQuandoOOrcamentoEstoura`,
`TestArquivoDeSpillOrfaoDeCrashEhRemovidoNoOpen`,
`TestArquivoDeSpillNaoApareceNoListSegments`.
**Risco:** médio, concentrado na cópia zero-copy.

#### C6 — Isolamento de snapshot `[opcional, decisão pendente]`

Entrega que **toda linha do resultado é a versão vigente em T0**. Mecanismo:
journal de before-images. No commit point
([store.go:1282-1292](internal/storage/store.go:1282)) o writer já tem
`oldPacked` ([:1208](internal/storage/store.go:1208)) e `prevPayload`
([:1165](internal/storage/store.go:1165)) **na mão, de graça** — copia para um
ring pré-alocado por snapshot vivo, sem uma leitura de disco a mais. Quem faz I/O
é a goroutine do leitor; o writer só faz `memcpy`, porque o §81 põe ingestão
acima de query. Ring cheio invalida o snapshot (`SNAPSHOT_TOO_OLD`) e **nunca**
atrasa o `Put`.

Duas armadilhas registradas: o copy-forward passa `prevPayload == rec.Payload` de
propósito ([compaction.go:221](internal/storage/compaction.go:221)), e o journal
precisa de um campo explícito para distinguir "a row mudou" de "a row mudou de
lugar"; e o `escritas.idx` é sobrescrito em lugar
([rowindex.go:329](internal/storage/rowindex/rowindex.go:329)), então row
renovada durante a query teria `_writtenAt` novo e o predicado de expiração
decidiria sobre o instante errado.

**Risco:** **alto.** Toca o commit point do write path e cobra zero alocação num
caminho que passa a fazer trabalho condicional.

---

### Bloco D — `ORDER BY`, `OFFSET`, `LIKE`, índice hash e scan paralelo

> **A refutação que simplifica o bloco inteiro: nenhuma palavra vira keyword do
> lexer.** `ORDER`, `BY`, `ASC`, `DESC`, `NULLS`, `FIRST`, `LAST`, `OFFSET` e
> `LIKE` são `TokenIdent` reconhecidos **por posição**, via `ASCIIEqualFold` —
> exatamente a máquina que `unsupportedClause`
> ([clauses.go:41](internal/query/parser/clauses.go:41)) já usa. Elas só
> aparecem onde hoje **só cabe erro** (`parser.go:45-55` e `:372-375`), então o
> parser **se compromete** ao ver a palavra e depois **exige** o resto — consumo,
> não lookahead. Continuam **11 keywords e 29 tipos de token**; os gates duros
> `keyword_test.go:28` e `token_test.go:126` **não mudam**, e
> `TestColunaChamadaOrderContinuaValida` **passa sem alteração**. Ela deixa de ser
> obstáculo e vira a prova de que a rota está certa.

#### Gramática v2

```ebnf
select      = "SELECT" projection "FROM" table
              [ where ] [ order_by ] [ limit ] [ offset ] [ ";" ] EOF ;
order_by    = ORDER BY sort_key { "," sort_key } ;        (* <= 16 chaves *)
sort_key    = column_ref [ ASC | DESC ] [ NULLS ( FIRST | LAST ) ] ;
offset      = OFFSET uint_literal ;                        (* sem parâmetro, sem sinal *)
predicate   = column_ref ( comparison | null_check | like ) ;
like        = [ "NOT" ] LIKE ( string_literal | "?" ) ;
```
Entre aspas = `TokenType` do lexer. Sem aspas = `TokenIdent` por posição.

#### D0 — `LIKE` e `NOT LIKE`

A palavra mais barata primeiro: prova a máquina de reconhecimento contextual em
**uma** posição antes de o `ORDER BY` precisar dela em seis.

`Ordem interna:`
1. **`default` que falha alto em `preencheSlots`**
   ([binder.go:648-660](internal/query/binder/binder.go:648)) — **antes** de
   existir nó novo. É um `switch` sem `default`: um `BoundLike` sem `case`
   deixaria `Slot = 0` e o LIKE testaria **o campo de menor FieldID da query**,
   devolvendo linhas erradas sem erro, sem log e sem métrica. Não há linter
   `exhaustive` no `.golangci.yml`.
2. `LikeExpr` em [ast.go](internal/query/parser/ast.go) — **nó novo, não `CmpOp`
   novo**. `ordemInt`/`ordemFloat`/`ordemBytes`
   ([predicate.go:130-184](internal/query/executor/predicate.go:130)) terminam em
   `default: // parser.CmpLte`: um `CmpLike` que chegasse ali seria avaliado como
   **`<=`**, com a suíte verde. Já `emite` ([:274](internal/query/executor/predicate.go:274))
   e `folha` ([binder.go:498](internal/query/binder/binder.go:498)) terminam em
   `default:` que **devolve erro**. O nó novo cai no caminho barulhento.
3. Reconhecimento em `parsePredicate` ([parser.go:350](internal/query/parser/parser.go:350)),
   depois do bloco de `IS` e antes de `cmpOp`. `MaxLikePatternBytes = 1024` como
   teto de recurso, no molde de `MaxWhereTerms`.
4. Mensagens: `parsePrimary` ([:298](internal/query/parser/parser.go:298)) passa a
   dizer *"NOT só é suportado em IS NOT NULL e NOT LIKE"*; `parsePredicate`
   ([:374](internal/query/parser/parser.go:374)) passa a
   *"esperado comparador, LIKE, IS NULL ou IS NOT NULL"*. O caso de
   [error_test.go:41](internal/query/parser/error_test.go:41) sai da tabela de
   erro e vira caso de sucesso.
5. Binder: `admiteLike(col) = (col == TypeString)` ao lado de `admitsOp`; padrão
   **compilado no bind** com os bytes copiados no arena. `LIKE ?` compila do
   parâmetro — o `Bind` já os recebe ([binder.go:152](internal/query/binder/binder.go:152)).
6. Compilação classifica em `PatternEqual`/`PatternPrefix`/`PatternSuffix`/`PatternContains`/`PatternGeneral`, com
   `minBytes` para rejeição O(1). Os quatro casos fechados viram
   `bytes.Equal`/`HasPrefix`/`HasSuffix`/`Contains`; o geral é glob de dois
   ponteiros com **um** ponto de backtrack — sem recursão, sem regexp, sem
   `string()`.
7. Planner: **nada**. `flatten` ([planner.go:242](internal/query/planner/planner.go:242))
   trata nó não-`BoundLogical` como conjunct opaco, e `eqTermOf` exige
   `*BoundComparison`. `WHERE status='a' AND nome LIKE 'x%'` **continua usando o
   bitmap de `status`** e revalida o LIKE.

**Entrega:** `LIKE`/`NOT LIKE` respondem, em varredura, com **zero alocação por
linha**. E `_meta.fields` ganha o operador de graça — `meta.Abre` reusa o mesmo
binder, planner e executor ([meta.go:60-81](internal/query/meta/meta.go:60)).
**Testes:** `TestSublinhadoCasaUmCaractereEhNaoUmByte` (com `'ç'` e com byte UTF-8
inválido), `TestNotLikeSobreCampoAusenteEhFalso`,
`TestLikeSobreColunaNaoStringEhRecusadoNoBind`,
`TestBitmapContinuaValendoComLikeNoMesmoAnd`,
`TestNoNovoSemCasoNoPreencheSlotsFalhaAlto`,
`TestLexerContinuaCom11KeywordsE29Tokens`, `TestColunaChamadaLikeContinuaValida`.
O gate `TestScanAllocsIndependenteDoNumeroDeRows` **ganha uma query com LIKE** —
senão o zero-alocação do matcher não é medido.
**Risco:** baixo.

#### D1 — `OFFSET`, e o conserto do `--limit`

> **O achado que torna esta fase obrigatória e não opcional.** `comLimit`
> ([query.go:151](internal/cli/query.go:151)) corta em `ultimoLimit` e **descarta
> tudo à direita**. Com `... LIMIT 5 OFFSET 10`, o `--limit 3` produz
> `... LIMIT 3`: a paginação **evapora** e o comando devolve as 3 **primeiras**
> linhas como se fossem a página pedida — resultado errado com cara de certo. Com
> `... OFFSET 10` sem LIMIT, o `LIMIT 3` é anexado **depois** do OFFSET e vira
> erro de sintaxe.

`Ordem interna:` AST aditiva (`HasOffset`/`Offset`/`OffsetPos`, molde exato de
`HasLimit`); `parseOffset` reusando as recusas de `parseLimit`
([:469](internal/query/parser/parser.go:469)); `offset` sai de
`unsupportedClauses`; contador de skip antes de `it.emitidas++`
([executor.go:222](internal/query/executor/executor.go:222)); e `comLimit` passa a
usar **`parser.Parse`**, cortando em `min(LimitPos, OffsetPos)` e **preservando o
OFFSET**. Isso paga a metade local do **débito 46**.

**Testes:** `TestLimitDoCliPreservaOOffsetDaQuery`,
`TestLimitDoCliEmQueryComOffsetSemLimitNaoQuebraASintaxe`,
`TestOffsetAntesDeLimitEhRecusado`, `TestColunaChamadaOffsetContinuaValida`.

#### D2 — `compara`, o varredor extraído, e `ORDER BY` em memória

`Ordem interna:`
1. **`varredor`** (`executor/varredor.go`): extrair o corpo por candidato de
   [executor.go:185-233](internal/query/executor/executor.go:185) para um struct
   com estado **por worker** (`slots`, `fr`, `rowBuf`, `cnt`, `mask`) e referência
   **compartilhada** ao `*programa` — que é imutável depois do `Open`
   ([predicate.go:57-80](internal/query/executor/predicate.go:57)) e portanto
   compartilhável sem cópia. Primeiro passo, sozinho, sem mudança de
   comportamento: é a espinha de D2 e de D5.
2. **`compara(kind, a, b) int`** em `executor/order.go` — **código novo e
   exclusivo do sort**. Os três `ordem*` do predicado **ficam intactos**, e a
   razão é técnica: IEEE-754 tem um quarto desfecho, *unordered*, que um `int` de
   três vias não representa — `NaN != 1` é **true** e `NaN < 1`, `NaN > 1`,
   `NaN = 1` são todos **false**. Qualquer `ordemFloat` derivada de `compara`
   erraria pelo menos um dos quatro. Um teste diferencial prova a concordância
   nos casos não-NaN.
3. Binder: `admitsOrdering(col)` no **mesmo arquivo** de `admitsOp`, com o
   contraste comentado; `BoundOrderKey`; **`b.usa(f.ID)` para toda chave**
   ([binder.go:431](internal/query/binder/binder.go:431)) — sem isso a coluna de
   ordenação não entra em `Slots` e o executor não a lê.
4. `parseOrderBy` entre WHERE e LIMIT ([parser.go:131](internal/query/parser/parser.go:131)).
5. **`ordenador`** (`executor/sort.go`): serializa os slots presentes com
   `encoding.AppendField` em ordem crescente de FieldID num arena, e guarda
   `{chaves, rowID, off, len}`. A releitura é o **mesmo `coleta`**
   ([collect.go:32](internal/query/executor/collect.go:32)) — `AppendField` e
   `coleta` já formam um par round-trip, então **não há serialização nova a
   inventar**.
6. Orçamento `query.sortMemory` (default 64 MiB), no molde completo de `maxRows`.
   Estouro → `SORT_BUDGET_EXCEEDED` / SQLState `53400`, com o número na mensagem.
7. **`it.eff` deixa de fechar a fonte cedo** ([:225-231](internal/query/executor/executor.go:225))
   e passa a limitar só a emissão; a mensagem de timeout passa a **nomear a fase**.

**Testes:** `TestAusenteVaiPorUltimoNasDuasDirecoes`,
`TestEmpateDesempataPorRowIDCrescente`,
`TestOrdemDeSaidaEhRowIDCrescenteSemOrderBy` (o de hoje, renomeado para dizer a
condição), `TestOrcamentoEstouradoRecusaENaoTrunca`,
`TestOrdemFloatMantemSemanticaDeNaN` (os quatro desfechos),
`TestTimeoutDuranteAOrdenacaoNomeiaAFase`.
**Risco:** médio-alto, concentrado na extração do `varredor` — mitigada por ser o
primeiro passo, sem mudança de comportamento, com a suíte inteira como oráculo.

#### D3 — Top-N com heap (caminho rápido **dentro** do sort)

Escolhido no `Open` quando `offset + limite` cabe no orçamento; senão o **mesmo
operador** cai no sort de D2. Heap de máximos segundo a ordem pedida, com o
buffer do despejado reaproveitado — zero alocação em regime.
**Teste que decide:** `TestTopNDaOMesmoResultadoQueOSortCompleto`, diferencial
sobre corpus aleatório com os dois caminhos forçados no mesmo dado. É o que
impede o caminho rápido de virar um segundo produto.

#### D4 — Sort externo (consome a primitiva de spill do Bloco C)

> **Achado que solta a ordem das fases:** como a tupla é **materializada no
> dreno**, o sort **não relê linha nenhuma** e portanto **não depende da
> estabilidade de leitura do snapshot** — só do limite superior compartilhado, que
> `ScanLive()` já dá hoje. **D2, D3 e D4 sobem sem o snapshot pronto.**

Run em memória cheio → ordena → grava em ordem → sela → zera o arena. Ao fim,
merge k-way com heap de k entradas, cada uma decodificando as chaves **uma única
vez** ao virar cabeça do seu run.

**O contrato que peço à primitiva do Bloco C:** bytes **opacos** (o enquadramento
é do spill, não meu — dois framings divergem no primeiro registro patológico);
`Next()` sem alocação por registro; estouro devolve sentinela e **nunca trunca
em silêncio**; **sem fsync** (spill não é durável, e crash perder a query é o
desfecho certo); lixo de crash limpo pela primitiva na subida; K leitores
concorrentes em K runs distintos; `NewReader()` só depois de `Seal()`.

#### D5 — Índice hash físico (fecha o **débito 19**) — *independente de D0..D4*

`Ordem interna:`
1. **Chave canônica com um dono só:** extrair `canonicalValue`/`ValidateValue`
   ([bitmap.go:57-89](internal/index/bitmap/bitmap.go:57)) para
   `internal/index/valuekey`, usada por bitmap **e** hash — duas cópias
   divergiriam no primeiro tipo novo, e `eqTermOf`
   ([planner.go:201](internal/query/planner/planner.go:201)) depende do byte de
   tipo estar lá. Golden de bytes provando que a chave do `values.dict` **não
   mudou**: é movimento puro ou nada.
2. **Cauda TLV do manifest**, em commit próprio: `(tag u16, len u32, bytes)`, tags
   `< 0x8000` ignoráveis e `>= 0x8000` críticas. O backfill do débito 22 também
   vai querer bump de manifest — **com TLV o bump acontece uma vez só e nunca
   mais**. O encoder só emite a cauda quando há conteúdo, o que preserva rollback.
3. **`internal/index/hash`** no molde de
   [primary/index.go](internal/index/primary/index.go): open-addressing sobre
   mmap, header de 64 B, publicação atômica, rehash por `.new` + rename +
   `fsyncDir`, ocupação **recontada no `Open`**. Diferença obrigatória em relação
   à primary: o hash secundário **precisa remover** (`Clear` marca `DELETED`),
   porque um update muda o valor da coluna.
4. **`LookupHash` coleta as postings do valor, ORDENA e itera.** É invariante, não
   estética: `intersectIter` e `unionIter`
   ([candidate.go:175](internal/query/executor/candidate.go:175), [:254](internal/query/executor/candidate.go:254))
   assumem iteradores **ascendentes**, e `planner.uniao` já mistura termos hash e
   bitmap no mesmo plano. Desordenado produziria **interseção incompleta em
   silêncio**.
5. **Sem validação de colisão no índice** — devolve superset e a revalidação do
   §44 filtra, que é o contrato que o executor já cumpre para bitmap. Elimina o
   `ValidateFunc` reentrante e o risco de deadlock com rehash que
   [store.go:928-932](internal/storage/store.go:928) documenta para a primary.
6. `schema.CamposComHash` ao lado de `CamposComBitmap`, chamada nos **mesmos
   quatro sítios** — o modo de falha cruzado servidor↔CLI que `CamposComBitmap`
   existe para impedir é idêntico aqui.
7. **Backfill recusado**, como no bitmap. O débito 22 **não fecha**; passa a ter
   dois motores, e isso vai escrito nele.

**Entrega:** `WHERE node = 'x'` com `index: hash` deixa de degradar para full
scan. **É a cura do `57014` que abriu esta sessão**, e o §126.15 passa a ser
cumprido pelo binário em vez de pelo dublê.
**Testes:** `TestLookupHashDevolveRowIDsCrescentes`,
`TestColisaoDeHashDevolveSupersetEOExecutorFiltra`,
`TestClearRemovePostingDeValorAntigoNoUpdate`,
`TestReplayDoTailReconstroiPostingsDeHash`,
`TestDeclararHashEmDatasetComLinhasEhRecusado`,
`TestManifestV1ContinuaSendoLido`, `TestChaveCanonicaNaoMudouGolden`.
**Risco:** **alto** — é a única fase que toca formato durável, recovery e
concorrência do writer ao mesmo tempo. Por isso a cauda TLV e a extração da chave
canônica são passos **isolados e anteriores**.

#### D6 — Varredura paralela por faixa — *depende só do passo 1 de D2*

`ScanLiveShards(n)` parte **um** snapshot de `nextRowID` em faixas contíguas e
disjuntas. Pool de `n` varredores, cada um com **seu** `rowBuf`, todos
compartilhando o `*programa`. Dois coletores: com `ORDER BY`, canal desordenado
direto para o `ordenador` (balanceamento perfeito, e o resultado segue
determinístico pelo desempate por rowID); sem `ORDER BY`, anel limitado por
worker consumido **em ordem de faixa**, o que preserva
`TestOrdemDeSaidaEhRowIDCrescente` sem exceção.

**`query.scanWorkers` nasce com default 1 (serial).** A funcionalidade entra
completa; o que fica pendente é **virar o default**, que é decisão separada e
exige o número na mão — ligar por default mudaria o perfil de alocação do gate
sem medição que o justifique. O benchmark de 1/2/4/8 workers e a baseline em
`benchmark/baselines/` fazem parte desta fase.

**Teste que pega o defeito real:** `TestCadaWorkerTemSeuRowBuf`, sob `-race`, com
linhas de tamanhos alternados.

#### D7 — Metadata do driver e errata publicada

`supportsOrderByUnrelated()` → **true**; `getMaxColumnsInOrderBy()` → **16** (hoje
`0`); `supportsLikeEscapeClause()` fica `false` — e agora isso é **verdade**, não
vacuidade. Só o `DriverIT.java:266-270` executa `ORDER BY` contra servidor real e
precisa mudar; os outros quatro arquivos usam `"ORDER BY"` como **fixture de
payload** opaco, continuam verdes, e trocar o exemplo para `GROUP BY` é higiene.

---

## A errata de spec, item por item

| Onde | O que passa a dizer |
|---|---|
| **§9** | **Nada sai da lista** — ela nunca teve `ORDER BY`, `OFFSET` nem `LIKE`; quem os punha fora era o **README**. Ganha só nota cruzada: as recusadas com `UNSUPPORTED_CLAUSE` passam de sete para **cinco** |
| **§59** | Gramática v2 acima, mais o parágrafo que hoje não existe e é o que importa: **as palavras da cláusula não são keywords do lexer**, são reconhecidas por posição — e por isso uma coluna chamada `order` continua consultável |
| **§61** | `LIKE`/`NOT LIKE`: só coluna `string`; `_` casa **um caractere**, não um byte; **sem `ESCAPE`**, logo `%`/`_` literais inatingíveis; campo ausente é false **inclusive em `NOT LIKE`** |
| **§65** | Hoje diz *"execução deve interromper após produzir resultados suficientes"*. Passa a dizer: **com `ORDER BY` isso não vale** — a fonte é drenada por inteiro, porque a linha certa pode ser a última lida. O `LIMIT` deixa de ser teto de **trabalho** e passa a ser teto de **memória**, pelo caminho de top-N |
| **§66** | Passa a valer **para toda query sem `ORDER BY`**. Com `ORDER BY`, a memória é **O(orçamento configurado)**, o excedente vai a disco em runs ordenados, e o resultado **nunca** é materializado inteiro em RAM |
| **§128** | A regra fica, com a qualificação: o buffer da ordenação é proporcional ao **orçamento**, não ao resultado; passar do orçamento de disco **recusa a query** — não trunca |
| **§42/§43** | §42 deixa de ser aspiracional: o hash **existe**. §43 ganha a linha de budget do hash, que a própria spec já desenhava e ninguém podia preencher |
| **§36** | *"O hash nunca é prova de igualdade"* fica; acrescenta que no hash **secundário** a prova é a revalidação do §44, não comparação de chave no índice |
| **README:635-643** | *"Fora da v1: `GROUP BY`, `JOIN`, agregações, `NOT` genérico. `ORDER BY`, `OFFSET` e `LIKE` entraram."* Mais: `NULL` por último nas duas direções, `LIKE` sem `ESCAPE`, e `ORDER BY` drena a fonte inteira |
| **`architecture.md`** | "Índice hash físico" **sai** de "O que ainda não existe"; a seção Consulta ganha o parágrafo do operador bloqueante e a tabela de latências ganha a linha da varredura **ordenada** de 8M, medida |

---

### Bloco E — Melhoria A: tipo declarado das chaves quentes

**Zero linha de Go.** É YAML mais recriação do dataset.

`Ordem interna:`
1. Rodar a consulta do B2 sobre o dataset **inteiro** (não sobre amostra de
   20.000) e ordenar por `typeMismatches DESC` — o que exige o Bloco D pronto.
2. Decidir o tipo chave a chave. O critério **não é só "qual é maioria"**:
   contadores (`curtoTally`, `longoTTL`, …) são inteiros em ~97% e nenhum tipo
   torna a coluna confiável para filtro exato — aceite a perda e trate o
   documento como a verdade. `frontPodName` perde 0,2% em 13.477 ocorrências:
   não vale declarar. `frontSATotal` perde 91,3% em 23 ocorrências: alto em
   proporção, irrelevante em volume.
3. Para as datas (`dataFalha`, `dataClear`, `dataBaixa`): **medir o formato
   emitido antes** de escolher entre `string` e `datetime` com `format`. O
   mecanismo já existe e é obrigatório para `datetime`
   ([spec.go:446](internal/schema/spec.go:446)); o precedente é o retipo de
   `firstSeen`/`lastSeen`, validado sobre **200.000 valores reais** antes de
   tocar o piloto (`docs/piloto-events.md:141-152`).
4. Aplicar: apagar `dados/events/*`, trocar o `groupId`, reler a retenção (~4h de
   backlog, dentro da janela de ~18,8h do tópico).

**Atenção ao achado 1:** sem `as:`, a coluna passa a se chamar `dataFalha` e não
`persistent.dataFalha`. `SELECT persistent.dataFalha` continua funcionando, mas o
`name` em `_meta.fields` muda e `SELECT "persistent.dataFalha"` quebra.

**Verificação:** o `nestedTypeMismatch` por evento deve cair de ~0,21 para ~0,01
— mas só depois do Bloco A, que corrige o viés do numerador.

---

## Ordem de execução

```
fix/  →  C0 + C1                    (urgente, antes do ciclo)
────────────────────────────────────────────────────────────
A1 → A2 → A3 → A4                   (rotas de perda + instrumento)
C2 → C3 → C4 → C5                   (snapshot, query longa, orçamento, spill)
D0 → D1 → D2 → D3 → D4              (LIKE → OFFSET → sort → top-N → externo)
D5                                   (hash físico — independente, pode paralelizar)
D6                                   (scan paralelo — depende só do passo 1 de D2)
B1 → B2 → B3                        (densidade, precisa de A)
E                                    (tipos declarados, precisa de B2 e de D2)
D7                                   (driver e errata)
```

Três independências que soltam o cronograma: **D4 não depende do snapshot** (a
tupla é materializada no dreno, então o sort não relê linha nenhuma); **D5 não
depende de D0..D4**; e **D6 depende só da extração do `varredor`**, que é o
primeiro passo de D2.

Uma dependência que não solta: **E precisa de D2**, porque escolher o tipo de
cada chave por medição em vez de por script é literalmente
`ORDER BY typeMismatches DESC`.

## Fora de escopo, com razão

- **`GROUP BY`, agregações, `JOIN`, `HAVING`, `UNION`, `DISTINCT`** — continuam no
  §9 e continuam recusadas com `UNSUPPORTED_CLAUSE`, agora em número de cinco.
- **Isolamento de snapshot (C6)** — desenhado no Bloco C, fora do ciclo por
  decisão. Volta quando alguém **medir** que a mistura temporal causa dano, e não
  por inércia de plano.
- **`ESCAPE` do `LIKE`** — escolha do usuário. **Consequência em letra grande:
  procurar `%` ou `_` literais fica impossível.** Vira débito novo, e
  `supportsLikeEscapeClause() == false` passa a ser verdade em vez de vacuidade.
- **`ILIKE`** — toda comparação da v1 é case-sensitive, e abrir exceção aqui
  criaria duas regras de folding: exatamente o defeito que `asciiEqualFold`
  existe para evitar.
- **`ORDER BY <expressão>` e `ORDER BY <posição>`** — não há expressões na v1, e
  ordenar por posição com `SELECT *` ordenaria por um conjunto de colunas que
  muda entre execuções.
- **Índice ordenado (B-tree) para servir `ORDER BY` sem sort** — outra estrutura
  durável, outro custo de escrita; o §40-43 desenha só bitmap e hash.
- **Atalho de índice para `LIKE 'prefixo%'`** — não existe estrutura a consultar:
  strings vão inline no payload e o `values.dict` do bitmap não é ordenado.
- **Cursor / paginação com estado no servidor** — `OFFSET` relê e reordena a cada
  página; cursor exigiria segurar snapshot e spill entre requisições. Frente
  própria, vira débito.
- **Backfill de índice (débito 22)** — este ciclo **não o fecha** e o **estende**
  para dois motores.
- **Débito 46 completo** — D1 conserta o corte e paga a metade local; o `--limit`
  virar parâmetro do plano no caminho **remoto** exige campo novo no protocolo.
- **Débito 77** — ligar `unknownFields: include` em dataset com linhas continua
  não sendo recusado. É a armadilha operacional ao lado, e a correção honesta
  exige bump de manifest, que a cauda TLV de D5 passa a viabilizar.
- **Migração de dataset** — segue sendo tool futura, por premissa de produto. As
  melhorias A e B exigem recriação, e isso é aceito.

## Verificação

**Por fase, antes de cada commit:** `make test`, `make test-race` e `make lint`
com `.golangci-baseline` (hoje **388**, lido do arquivo e nunca da prosa)
atualizado no mesmo PR — o ratchet reprova nas **duas** direções.

**Gates que precisam continuar verdes SEM edição** — é a prova de que D8 está
certo: `lexer/keyword_test.go:28` (11 keywords), `lexer/token_test.go:126` (29
tokens), `lexer/alloc_gate_test.go:15` (exatamente 0),
`parser/clauses_test.go:77-91` (`TestColunaChamadaOrderContinuaValida`).

**Suítes que este ciclo tem obrigação de NÃO perturbar** — se alguma precisar
mudar, a mudança saiu do escopo:
[compaction_crash_test.go](internal/storage/compaction_crash_test.go) (as janelas
de crash da aposentadoria), [expiry_test.go](internal/storage/expiry_test.go)
(a doutrina *"visibilidade não depende do agendador"* do débito 25) e
[close_quiesce_test.go](internal/storage/close_quiesce_test.go).

**Gates que mudam, e como:**
- `TestScanAllocsIndependenteDoNumeroDeRows` **não afrouxa** — igualdade exata e
  `tetoPorQuery = 38` intactos. Muda o **nome** e o godoc, para dizer que governa
  o caminho **sem operador acumulador**, e **ganha uma query com `LIKE`**.
- Gate novo para o sort, com outra propriedade: `SortBytes` proporcional às
  linhas e **`SortBytes`/linha constante** — é o que separa "buffer que cresce
  dentro do orçamento" de "vazamento por linha".
- `TestExtracaoNaoAloca` e `TestDescobertaNaoAlocaEmRegime` continuam exigindo
  **zero**. Eles medem alocação e **não CPU** — a promessa de vazão do Bloco B
  fecha com o benchmark de A4, rodado antes e depois.

**Cinco diferenciais — são os testes que decidem se o plano está certo:**
1. `compara` × `ordemInt`/`ordemBytes` em todos os operadores.
2. Top-N × sort completo, mesmo corpus, os dois caminhos forçados.
3. Sort externo × sort em memória, orçamento forçado a 1 run e a N runs.
4. Scan paralelo × serial, mesmo dado, com e sem `ORDER BY`.
5. **AST antes × depois da mudança de gramática:** rodar o corpus de
   `parser/fuzz_test.go` e exigir que toda query que **parseia hoje** produza a
   **mesma AST** depois. É a prova mecânica de que nenhuma query em produção
   mudou de significado.

**Prova física do disco (a que justifica C1, e que nenhum instrumento do projeto
sabe fazer hoje):** `statfs` do dispositivo antes e depois de uma rodada com 20+
aposentadorias e uma query longa em voo. Os blocos precisam voltar. Sob a tag
`integration` — exige filesystem real, não `tmpfs`.

**Ponta a ponta, com o binário:**
1. `--limit 3` sobre `... LIMIT 5 OFFSET 10` e sobre `... OFFSET 10` — as duas
   formas que hoje quebram, uma delas em silêncio.
2. `ORDER BY` sobre o dataset de 8M com `query.sortMemory` pequeno para forçar
   spill; conferir que o schema chega **antes** do dreno (o grid do DBeaver
   desenha as colunas enquanto a consulta roda); estourar o orçamento e conferir
   que **recusa** em vez de entregar resultado parcial; matar o processo durante
   o spill e conferir que não sobra arquivo.
3. Dataset novo com `index: hash`; conferir `HashIndexPlan` pelo plano; `kill -9`
   no meio da ingestão e conferir que o replay reconstrói as postings; abrir o
   mesmo dataset pelo **CLI e pelo servidor** (o modo de falha cruzado da fiação
   única); conferir que um manifest v1 anterior **continua abrindo**.
4. Rodada com ingestão Kafka viva, uma query longa autorizada e compaction
   ligada: a query termina, o `nishidb status` mostra a idade dela do começo ao
   fim, o `df` do dispositivo não sobe monotonicamente, e uma query curta
   disparada no meio **não** recebe 503.
5. `SELECT name, type, occurrences, typeMismatches FROM _meta.fields WHERE
   parent = 'persistent' ORDER BY typeMismatches DESC LIMIT 20` — a consulta que
   costura as três frentes e que só existe com o ciclo inteiro no ar.

**Documentação (DoD do CLAUDE.md):** `architecture.md`, `README.md`,
`nishidb-spec-proposal.md` (a errata tabelada acima), `debitos.md` (**fecha o
19** e o **28**; **estende o 22**; **errata no 74**; paga a metade local do
**46**; abre os novos de `ESCAPE`, cursor e progresso no NDJSON; e registra o
vazamento de inode já fechado pela `fix/`), e os `.puml` afetados —
`query_flow.puml`, cuja nota *"Memória O(batch), nunca O(resultado): não há
buffer de rows"* é **literalmente invalidada** por este ciclo, e
`recovery_flow.puml`, que ganha o hash. Regra de sequência do projeto: documento
que cita `TestXxx` entra **depois** do teste existir, no mesmo commit —
`internal/cli/evidence_test.go:26-33` verifica.
