# Aceite — TTL ancorado num campo do documento

Rodada de aceite em 01/10/2026, na branch `feature/ttl-anchor`, sobre
`origin/feature/counter-scope-rows` em `aca4ffe` (PR #90). Atende o pedido da
integração service-view → `service_state` de 30/09/2026, com o contrato YAML
aprovado pelo consumidor. O plano normativo é
[`planning/ttl-anchor.md`](../planning/ttl-anchor.md), com a "Revisão 2"
(T1–T16) prevalecendo sobre o texto anterior. Registro de rodada: não é
reescrito.

## O critério

O prazo do TTL era sempre contado da última escrita, e toda escrita aceita o
renovava. Um evento que não mexe na autenticação — o dying gasp do TEMS —
mantinha vivo para sempre um serviço sem Radius, enquanto o legado o remove
quando o último pacote passa de 2 dias. O contrato:

```yaml
materialization:
  ttl:
    - when: "lastAuthAt IS NULL"
      expireAfterWrite: 1h
    - anchor: lastAuthAt
      expireAfter: 48h
```

A regra com `anchor` vence em `min(âncora, _writtenAt) + expireAfter`, e uma
escrita que não muda a âncora não a renova. O storage continua somando
`_writtenAt + _expiresIn` sem conhecer campo de negócio: o materializer
resolve a âncora sobre o record e grava o `_expiresIn` correspondente.

## Correção de comportamento, para as notas da release (T1/T7)

Antes da âncora entrou, em commit próprio, a decisão do prazo sobre o
**payload final** da escrita. A regra era avaliada sobre "o estado anterior
com o patch por cima", e isso divergia do record em quatro casos:

- `when` sobre coluna **congelada** de campo via a captura do evento, não o
  valor congelado;
- `when` sobre congelada de **relógio** via o instante desta escrita, não o da
  primeira;
- `when` sobre coluna **`merge: sum`** via a parcela, não a soma;
- em **`mode: replace`** e sobre a linha **vencida sem lápide** (que renasce do
  vazio), a condição casava com campos que o record novo não tem.

Datasets com `when` sobre congelada ou sobre `merge: sum` passam a ter o prazo
decidido pelo valor gravado a partir da **próxima escrita de cada linha**; os
`_expiresIn` já gravados ficam. É o código se alinhando ao que o README já
afirmava ("A condição vê o estado PÓS-MERGE").

## Critérios e as evidências

| Garantia | Provado por |
|---|---|
| T1. O prazo é decidido sobre o payload que vai ao `Put`: replace, congelada de campo, congelada de relógio, `merge: sum`, linha vencida sem lápide | `TestReplaceRuleDoesNotSeeFieldOnlyInPreviousState`, `TestRuleOnFrozenFieldReadsFrozenValue`, `TestRuleOnFrozenClockReadsFirstWriteInstant`, `TestRuleOnSummedFieldReadsTheSum`, `TestExpiredRowWithoutTombstoneRuleSeesOnlyTheNewRecord` |
| T1/12. A decisão do prazo não aloca (do Apply ao `_expiresIn` gravado), em cada ramo | `TestTTLDecisionDoesNotAllocate` |
| 12. A regra ancorada não aloca, em cada desfecho (gate novo ao lado do anterior) | `TestTTLAnchorDecisionDoesNotAllocate` |
| 1. Escrita que não muda a âncora não renova | `TestAnchorWriteThatKeepsTheAnchorDoesNotRenew` |
| 2. Escrita que muda a âncora move o prazo para `nova + d` | `TestAnchorWriteThatMovesTheAnchorMovesTheDeadline` |
| 3. Âncora ausente cai na próxima regra; sem próxima, não expira; a série conta só depois do `when` (T14) | `TestAnchorAbsentFallsToTheNextRule`, `TestAnchorAbsentWithoutNextRuleDoesNotExpire`, `TestAnchorAbsentCountsOnlyWhenTheConditionMatched` |
| 4. Âncora vencida: invisível a partir de `_writtenAt + 2 ms`, recolhida pelo coletor, nunca imortal | `TestAnchorExpiredOnWriteStoresTheMinimumMortalTTL`, `TestAnchorExpiredOnWriteIsInvisibleAndCollected` |
| T3. A conta não estoura: `MinInt64`, fronteira exata, 1 ms antes dela, futuro, `MaxInt64`, escrita perto de `MinInt64` | `TestAnchoredExpiresInBoundaries` |
| 5. Âncora no futuro: prazo = `expireAfter` da escrita, série sobe | `TestAnchorInTheFutureIsClampedToTheWrite` |
| 6/T9. O prazo é função do record: a conta parte do `_writtenAt` carimbado, não de um relógio relido | `TestAnchorDeadlineIsAFunctionOfTheRecord` |
| 7. Regra ancorada na última posição não é fundo; linha legada não ganha o prazo dela | `TestTTLAnchorRuleInLastPositionIsNotTheFallback`, `TestAnchorLastRuleGivesNoDeadlineToLegacyRows` |
| T2. Predicado único de fundo (`IsFallback`/`HasFallback`) no compilador das regras, no `expiresInOf`, na descrição e no `temFundo` do log | `TestCompileRulesResolvesAnchorAndFallback`, `TestHasFallbackSeesNeverFallback`, `TestStartupLogSeesNeverFallback` |
| 8. Recusas de forma, cada uma com a mensagem própria | `TestTTLAnchorShapeRefusalsNameTheirOwnRule` |
| 8. Recusas do campo âncora no compile (não temporal, derivada não temporal, `auto`, congelada de origem `auto`, inexistente, excluída, de sistema), cada uma com a mensagem própria | `TestTTLAnchorFieldRefusalsNameTheirOwnRule` |
| T14. `auto` é recusado pela declaração, mesmo com tipo observado temporal | `TestTTLAnchorRefusesAutoEvenWhenObservedTemporal` |
| Campos aceitos: timestamp, datetime, congelada `@writeTime`, congelada de campo timestamp, derivada timestamp | `TestTTLAnchorAcceptsTemporalFields` |
| 9. `when` + `anchor` exigem as duas | `TestAnchorWithConditionRequiresBoth`, `TestDescribeRulesShowsAnchorAndWhen` |
| 11. Âncora sobre congelada `@writeTime` é teto de idade absoluta | `TestAnchorOnFrozenWriteTimeCapsAbsoluteAge`, `TestAnchorOnFrozenWriteTimeExpiresDespiteWrites` |
| 13/T5. Ponta a ponta com o contrato e um contador com `scope`: autenticado + dying gasps vence em `lastAuthAt + 48h`; só dying gasp vive 1h renovável; nascida vencida é contada até a lápide e renasce do vazio | `TestServiceStateContractEndToEnd` |
| T8. `/v1/stats`: regra ancorada com `anchor`/`expireAfter`/`anchorAbsent` e `expireAfterWrite` vazio; as outras byte a byte como antes; séries novas nascem com zero | `TestMaterializationStatsPublishesAnchoredRuleUnderItsOwnKeys`, `TestMaterializationStatsPublishesZeroAnchorAbsent`, `TestMaterializationSamplesPublishAnchorSeries`, `TestStatsAndMetricsPublishTheAnchoredRule` |
| Reescrita do int64 no lugar | `TestOverwriteInt64RewritesOnlyTheCurrentValue`, `TestOverwriteInt64RefusesOtherTypesAndLeavesPayloadIntact` |

A coluna derivada (`extract:`) tipada `timestamp`/`datetime` é **aceita** como
âncora: ela é coluna do record como as outras, e com o prazo decidido sobre o
payload final ela já está lá quando a regra olha.

A recusa de `merge: sum` sobre a âncora saiu da spec (T11): é inalcançável,
porque `sum` exige `int64` e a âncora exige tipo temporal.

## As mutações: o vermelho e o verde de cada uma

O defeito foi aplicado no código de produção, o teste ficou vermelho com a
mensagem abaixo, o defeito foi revertido e o teste voltou ao verde. Nenhuma
ficou no código.

| Garantia | Mutante | Vermelho |
|---|---|---|
| T1 | avaliar sobre o estado anterior com o patch por cima (o código de antes) | os cinco testes do T1: "prazo 3m0s depois do replace sem `tipo`, quero 7d", "prazo 168h0m0s, quero 3m: a regra leu a captura do evento (9) em vez do valor congelado (5)", "… leu o instante desta escrita em vez do congelado", "… leu a parcela (6) em vez da soma (12)", "prazo 3m0s, quero 7d (fundo): a regra casou com o `tipo` da encarnação vencida" |
| T1/12 | `fmt.Sprintf` no laço das regras | 1,0 a 2,0 alocações por evento nos cinco casos |
| 12 | `fmt.Sprintf` antes da busca da âncora | 1,0 alocação nos quatro casos ancorados — e o gate anterior continuou verde |
| 12 | `fmt.Sprintf` antes da conta ancorada | 2,0 alocações nos três casos que chegam à conta |
| 1/11 | ancorar em `_writtenAt` | "vencimento … depois da escrita sem âncora, quero …: a escrita renovou o prazo"; no storage, "a linha passou do teto de 1h desde a primeira escrita" |
| 3/T2 | o ramo sem programa casa sem checar a âncora | "prazo 48h0m0s, quero 1h (a regra seguinte)" |
| 3 | âncora ausente decide a regra | "prazo 48h0m0s, quero 0 (não expira)" |
| T14 | contar a ausência antes do `when` | "anchorAbsent = [2 0], quero [1 0]" |
| 9 | a regra ancorada ignora o `when` | "só a âncora: prazo 47h0m0s, quero 1h" |
| 5 | sem o `min` (âncora futura) | "prazo 51h0m0s, quero 48h: a âncora futura comprou vida além da regra" |
| 4/T4 | sem trava (vencida grava a conta crua) | "prazo -24h0m0s, quero 2ms"; no storage, "a linha nascida vencida está visível 2 ms depois da escrita" |
| 4/T4 | mínimo = 1 (o sentinela de imortal) | "prazo 1ms, quero 2ms"; no storage, a linha some da consulta mas "o coletor não deu a lápide": o índice a lê como imortal |
| T3 | sem piso para o resto de 1 ms | "anchoredExpiresIn = (1, 0), quero (2, 0)" |
| 6/T9 | o materializer relê o relógio em vez do `_writtenAt` carimbado | "vencimento … 10:00, quero … 11:00: a conta leu um relógio diferente do carimbado no record" |
| 7 | `fallbackDuration` pega a última regra | "duração de fundo = 48h0m0s, quero 0"; no storage, "a linha legada expirou pelo prazo de uma regra ancorada que nunca a avaliou" |
| T2 | `IsFallback` = sem `when` | a lista com âncora e fundo é recusada: "a fallback rule … must be the last one" |
| T2 | `CompileRules` marca fundo por "sem `when`" | "regra 1 = {prog:false anchor:4 fallback:true …}" |
| T2 | `temFundo` pela duração de fundo | "log de subida sem temFundo=true com fundo never" |
| 8 | cada guarda de forma desligada (anchor+expireAfterWrite, expireAfter sem anchor, anchor sem expireAfter, anchor+never, piso, zero) | a recusa some ou sai com a mensagem de outra guarda, ex. "precisa ser a da guarda própria (\"cannot be combined with anchor\"): … anchor requires expireAfter" |
| 8 | cada guarda do campo desligada (sistema, excluída, auto, tipo) e a validação inteira desligada | "âncora \"hits\" foi aceita", "… is not a field of the dataset" no lugar de "is excluded", "has type null" no lugar de "is auto-typed" |
| T14 | `auto` aceito quando o tipo observado é temporal | "a mensagem precisa ser a da guarda de auto" |
| T8 | duração ancorada sob `expireAfterWrite` | "expireAfterWrite da regra ancorada = 48h0m0s (presente=true), quero presente e vazio" |
| T8 | `anchorAbsent` em toda regra | os bytes das regras antigas mudam; "regra de fundo = map[anchorAbsent:0 …]" |
| T8 | série de âncora ausente para toda regra | "série de âncora ausente publicada para a regra 0, que não tem âncora" |
| T8 | série de vencidas lendo o contador de futuras | "nishidb_ttl_expired_on_write_total = 5, quero 6" |

Um mutante sobreviveu na primeira passada e mudou o código, não o teste: a
guarda por NOME dos campos de sistema era redundante com a do `IsSystem` (os
dois campos estão no schema compilado sempre que há regra) e foi removida.
Outro sobreviveu e mudou o teste: a série de vencidas lendo o contador de
futuras passava porque os dois valiam zero na amostra; os valores agora são
distintos.

## Números medidos

| Medida | Valor | Origem |
|---|---|---|
| Alocações por evento na decisão do prazo, cinco ramos | 0 | `TestTTLDecisionDoesNotAllocate` |
| Alocações por evento na regra ancorada, quatro desfechos | 0 | `TestTTLAnchorDecisionDoesNotAllocate` |

## Rodada

Em 01/10/2026, Go 1.26.5, num host de build (não o notebook de
desenvolvimento), sobre a árvore do commit da âncora:

| Comando | Resultado |
|---|---|
| `make test` | exit 0, 62 pacotes `ok` |
| `make test-race` | exit 0, 62 pacotes `ok` (os gates de alocação se anunciam como SKIP) |
| `go test -count=1 -v ./...` | 2.991 testes de topo PASS, 0 FAIL, 1 SKIP; 4.950 PASS contando subtestes |
| `make build` | exit 0 |
| `make test-python` | 158 testes, OK (28 skipped) |
| testes novos com `-count=20`, e com `-race -count=5` | `ok` nos quatro pacotes |
| `golangci-lint` sobre `git archive HEAD` | igual a `.golangci-baseline` |

## Fora

- Prazo absoluto ("expira EM" o valor do campo) — não pedido.
- Relógio do follower (#82).
- `docs/replication.md` ainda afirma que o TTL usa o instante do master; a
  correção está em outro branch (débito 131) e não foi tocada aqui.
- O teto contado de quando a condição passou a valer (débito 84): a âncora
  sobre `@writeTime` conta do nascimento da entidade, e o item fica aberto
  como atendimento parcial.
