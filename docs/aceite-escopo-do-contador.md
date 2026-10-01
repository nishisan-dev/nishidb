# Aceite — escopo do contador, coluna `rows` e comparação linear no alarme

Rodada de aceite em 30/09/2026, na branch `feature/counter-scope-rows`, sobre
`origin/main` em `7f15b3e`. Atende a issue #61 (pedido N1 da integração
service-view → `service_state`). O plano normativo é
[`planning/counter-scope-and-rows.md`](../planning/counter-scope-and-rows.md),
com as decisões da refutação (R1–R15) prevalecendo sobre as seções A–C.
Registro de rodada: não é reescrito.

## O critério

O consumidor precisa alarmar uma **proporção** por grupo: "no máximo 10% dos
membros elegíveis autenticados, em grupos com ao menos seis membros". Isso pede
três peças:

1. **`scope`** — um filtro de membro, irmão do `where`: a linha fora dele não
   existe para o contador;
2. **`rows`** — o número de membros do grupo, ao lado do `total`, na tabela, na
   série e no evento do alarme;
3. **comparação linear inteira entre colunas** — `total * 10 <= rows` — só na
   condição de alarme, exata, sem float e sem estouro.

E uma quarta que a refutação acrescentou: o par `total`/`rows` tem de ser lido
**consistente** — duas cargas atômicas separadas deixavam o leitor ver
`total > rows` e resolveriam o alarme sem motivo.

## Critérios e as evidências

| Garantia | Provado por |
|---|---|
| 1. Linha fora do scope não interna grupo nem conta (`refs`, total, `Limit` inalterados) | `TestOutOfScopeRowInternsNoGroupAndCountsNowhere` |
| 2. Sair do escopo (patch) desconta `rows` e `total` na mesma aplicação; a lápide da expiração também | `TestLeavingTheScopeDecrementsRowsAndTotalInTheSameTransition`, `TestScopeLeavesTheGroupOnPatchAndOnTTLExpiry` |
| 3. Scope sobre o estado pós-merge (fixação de propriedade existente, sem mutante) | `TestScopedProportionAlarmFromIngestionToEventBody` |
| 4. Recovery por dobra + replay chega ao estado contínuo; a auditoria dá zero divergência | `TestScopedCounterRecoversToTheContinuousStateAndAuditsClean` |
| 5. Mudar o scope reconstrói, manter adota; sem scope o manifest é byte a byte o de antes | `TestScopeIdentityDecidesAdoptOrRebuild`, `TestCountersManifestSectionBytesAreLiteral`, `TestIdentityOfKeepsTheWhereAloneAndSplitsBack` |
| 6. Rollback: definição sem scope e com o mesmo where reconstrói | `TestScopeIdentityDecidesAdoptOrRebuild` |
| 7. `rows` na tabela = `refs` do grupo, também com `total: false`; os gates de alocação da tabela cobrem a coluna | `TestCounterTableRowsIsTheGroupMembersEvenWithTotalDisabled`, `TestTabelaDoContadorNaoAlocaPorLinha`, `TestCounterTableAtRowCeilingPublishesEveryColumn` |
| 8. `total * 10 <= rows` decide na fronteira, NULL é falso, 128 bits com sinal; com os produtos iguais os operadores estritos são falsos e os largos verdadeiros | `TestLinearColumnComparisonDecidesAtTheBoundary`, `TestLinearColumnComparisonOperatorsAtExactEquality`, `TestLinearColumnComparisonIsExactBeyondInt64`, `TestProportionAlarmFiresResolvesAndDisqualifiesOnTotalAndRows` |
| 9. Recusas na subida, cada uma com a mensagem própria | `TestLinearColumnComparisonRefusesWithItsOwnMessage`, `TestColumnComparisonStaysRefusedOutsideTheAlarm`, `TestCompileRefusesColumnComparisonInTheScope`, `TestSpecRefusesAnEmptyScopeAndRowsAsAColumnName` |
| 10. `rows` e `total` no corpo/log de `alarm.firing` e `alarm.resolved` | `TestFiringAndResolvedCarryRowsAndTotalInLogAndBody` |
| 11. Emissão periódica (arquivo e Kafka) leva `rows` | `TestOneReadingFeedsFileAndAllKafkaOutputs`, `TestACounterWithoutMetricsEmitsTheFixedColumnsWithRows`, `TestTheEmittedLineCarriesTheWindowColumnsTheTableServes` |
| 12. Ponta a ponta: ingestão → firing `total=1, rows=10` → `condition_cleared` → `disqualified` | `TestScopedProportionAlarmFromIngestionToEventBody` |
| R2. Par consistente numa palavra de 64 bits; teto de linhas por grupo recusado antes de mutar | `TestGroupReadsTotalAndRowsAsOneConsistentPair`, `TestGroupRowsCeilingRefusesTheRowBeforeAnyMutation` |
| R3. O evento lê `total`/`rows` do payload que decidiu | `TestEventTotalAndRowsAreReadFromTheDecidingPayload` |
| R4/R14. Scope na descrição das métricas por taxa e na assinatura do alarme | `TestMeasureSourceRestoresAndResetsOnlyWhenItsMeaningChanges`, `TestChangingOnlyTheScopeInvalidatesTheRestoredTimer` |
| R4. Sem scope, a assinatura do alarme sobre medida e a descrição `.source` são byte a byte as de antes (golden literal, conferido também sobre `origin/main`) | `TestMeasureAlarmFingerprintIsByteForByteTheSameWithoutScope`, `TestMeasureSourceDescriptionIsByteForByteTheSameWithoutScope` |
| R5. Recusas do scope espelham as do where | `TestCompileRefusesScopeOverASummedField`, `TestValidateRefusesScopeWithoutAnEvaluator`, `TestCompileRefusesColumnComparisonInTheScope` |
| R6. Transição com scope não aloca, em cada ramo | `TestScopedTransitionInsideTheScopeDoesNotAllocate`, `TestScopedTransitionLeavingTheScopeDoesNotAllocate` |
| C. Comparação entre colunas não aloca | `TestLinearColumnComparisonDoesNotAllocate` |
| R12. Um avaliador do scope por Deriver | `TestEachDeriverGetsItsOwnScopeEvaluator` |
| `rows` em `value:` e em `enrich.bind` | `TestRowsIsAValueColumnAndABindableColumn` |
| `_meta.counters` mostra o scope ao lado da condição | `TestMetaCountersShowsTheScopeBesideTheCondition` |

## As mutações: o vermelho e o verde de cada uma

O defeito foi aplicado no código de produção, a suíte ficou vermelha, o defeito
foi revertido, a suíte voltou ao verde. Nenhuma ficou no código.

| Garantia | Mutante | Vermelho |
|---|---|---|
| R2 | a entrada no grupo vira duas somas atômicas (refs, depois total) | 5/5 rodadas, 48.156 a 127.065 leituras rasgadas, a primeira `(0, 2)` |
| R2 | `Group` carrega a palavra duas vezes | 3/3 rodadas, 7.301 e 8.848 leituras rasgadas, a primeira `(1, 1)` |
| R2 | guarda do teto de linhas desligada | "a quarta linha no grupo de teto 3 deu <nil>" |
| R3 | o evento usa o par da leitura da varredura | "disparo com total 999 rows 7, quero 50 e 80" |
| 10 | `rows` só no disparo | log e corpo da resolução sem `rows` |
| 11 | a série grava o total no lugar do `rows` | Kafka e arquivo com `rows:7`, quero 9; linha literal diverge |
| 8 | produto em `int64` | `total * 4611686018427387904 <= rows` com total 2 e rows 5 deu verdadeiro (e mais dois casos) |
| 8 | `bits.Mul64` sobre o complemento de dois, sem tratar o sinal | cinco casos com negativo, ex. `slot * 3 < total * 2` com slot −3 deu falso |
| 8 | `<` avaliado como `<=` (M6) | `total * 10 < rows` com total 1 e rows 10 deu verdadeiro (e os outros casos de igualdade) |
| 8 | `>` avaliado como `>=` (M6b) | `total * 10 > rows` com total 1 e rows 10 deu verdadeiro (idem) |
| 9 | guarda de tipo `int64` do binder desligada | a recusa sai da guarda do predicado ("programa inválido"), não da própria |
| 9 | guarda `k >= 1` do parser desligada | a recusa sai do binder, com outra mensagem |
| 9 | recusa de `k * coluna` desligada | "esperado nome de coluna…" / "conteúdo inesperado…" |
| 9 | recusa de `coluna * k <op> literal` desligada | `total * 10 <= 100` aceito |
| 9 | `CompileCondition` liga a comparação entre colunas | `total <= rows` aceito fora do alarme |
| C | `fmt.Sprintf` na comparação entre colunas | 2,0 alocações por avaliação |
| 7 | a coluna `rows` recebe o total | "rows = 5,47,0,58, quero 5,47,3,58" |
| 7 | `fmt.Sprintf` na montagem do `rows` | 1,0 alocação em três gates da tabela |
| 1 | o `Derive` ignora o scope | "Limit = 2 depois de uma linha fora do escopo, quero 1" (e o ponta a ponta: "total 4 rows 13, quero 1 e 10") |
| 2 | a saída sem lápide não desconta o grupo antigo | "A depois de sair do escopo: total 1 refs 2" |
| R6 | `fmt.Sprintf` só no ramo FORA do escopo do `Derive` (M15) | 1,0 alocação por transição no gate de saída; o de dentro fica verde |
| R6 | `fmt.Sprintf` só no ramo DENTRO do escopo (M15b) | 1,0 alocação por transição no gate de dentro; o de saída fica verde |
| R12 | o Deriver novo reusa o avaliador do writer | "instanciou 1 avaliadores no total, quero 2" |
| 4 | o Deriver da auditoria sem scope | a auditoria acusa 2 grupos divergentes |
| 5 | a identidade exclui o scope | "trocar o scope reconstruiu 0 contadores, quero 1"; golden diverge |
| 6 | a definição sem scope adota quando o where é igual | "a definição sem scope ADOTOU o vetor construído com scope" |
| R8 | separador `" | "`; identidade sem scope com `\n` no fim | golden diverge nos dois |
| R5 | guarda `merge: sum` do scope desligada | scope sobre campo somado aceito |
| R5 | guarda de scope sem avaliador desligada | o `Open` aceita |
| 9 | guarda de scope vazio desligada; `rows` fora das colunas fixas | os dois aceitos |
| R4/R14 | scope fora da assinatura do alarme | "trocar só o scope restaurou 1 cronômetros, quero 0" |
| R4 | scope fora do `.source` da métrica por taxa | nenhum `counter_metric.source_changed` |
| R4 | sem o `omitempty` do Scope no contexto da assinatura (M8) | a assinatura sem scope ganha `,"Scope":""` |
| R4 | sem o `omitempty` do Scope na descrição `.source` (M9) | a descrição sem scope ganha `,"Scope":""` |
| — | `bind: [rows]` liga o total; `value: rows` recusado | parâmetro 1, quero 10; recusa na compilação |
| — | a coluna `scope` de `_meta.counters` recebe o where | "scope = authenticated = true,…" |

O teste concorrente do par não é vacuamente concorrente: com 11 leitores e de
200 mil a 370 mil escritas por rodada, os leitores viram de 1,2 a 3 milhões de
leituras e de 177 mil a 438 mil MUDANÇAS de estado entre duas leituras, com
zero rasgos; o teste exige ao menos mil de cada.

## Custo medido

Bancada: notebook Intel Core 7 150U, Go 1.26.5, `BenchmarkTransitionHotPath`
(regime do gate de alocação: grupos internados, linha já no overlay, grupo e
condição alternando), oito rodadas intercaladas antes/depois, 1 s cada. O
notebook tem cauda térmica; as últimas rodadas de cada lado saíram até 75%
mais lentas e ficam fora da faixa.

| Caminho | Antes (`7f15b3e`) | Depois | Leitura |
|---|---|---|---|
| Transição do contador, sem scope (palavra de 64 bits) | 191,8–204,2 ns (7 rodadas) | 194,7–197,2 ns (8 rodadas) | dentro do ruído; zero alocação nos dois |
| Dobra de um milhão de linhas (`BenchmarkDobra1M`) | 37,4–37,7 ms | 34,0–35,4 ms | ≈ −8%: uma soma atômica por linha em vez de duas |
| Transição com o avaliador dublê do scope (`BenchmarkTransitionHotPathScope`) | 139,5–146,0 ns sem scope | 212,2–215,1 ns com scope | +≈ 73 ns por evento |
| Transição com o avaliador REAL compilado do YAML (`BenchmarkCompiledScopeTransition`) | 177,7–188,4 ns sem scope | 244,6–248,8 ns com scope | +≈ 65 ns por evento, zero alocação |

O scope custa uma avaliação de predicado a mais por evento e por contador que
o declara — a mesma ordem de grandeza do `where`. A linha fora do escopo sai
antes de montar a chave e de avaliar o where, então é mais barata que a de
dentro. A comparação entre colunas não entra no caminho de escrita: roda na
passada do alarme, uma vez por grupo.

## Quebras aditivas de contrato (para as notas da release)

- **Série do contador (arquivo e Kafka):** toda linha ganha `"rows"` logo
  depois de `"total"`, inclusive para contador sem `metrics:`. Quem lê por nome
  não muda nada; quem compara a linha inteira passa a ver a chave nova.
- **Evento de alarme (log e webhook):** `alarm.firing` e `alarm.resolved` ganham
  `"rows"`. `rows` passa a ser chave reservada do evento.
- **Tabela `_counters.<nome>`:** coluna `rows` depois de `total`; `rows` é nome
  reservado (groupBy, medida ou métrica com esse nome são recusados na subida).
- **`_meta.counters`:** coluna `scope`, ao lado de `condition`.
- **`total` do evento** passa a ser lido do payload que decidiu, e não da
  leitura da varredura — na prática o mesmo número, sem a janela de divergência.

## O que fica de fora

- `strings.Fields` colapsa espaços dentro de literal string na normalização de
  `where`/`when`/`scope` — pré-existente, vira issue própria.
- `rows` como `source` de métrica de janela; comparação entre colunas no SQL;
  batching do fsync de grupo novo (débito 108).
