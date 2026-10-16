# TTL ancorado num campo do payload

Pedido da integração service-view → `service_state` (30/09/2026), aprovado pelo
usuário; contrato YAML revisado e aprovado pelo consumidor em 30/09/2026.

## Problema

Hoje o prazo é ancorado em `_writtenAt` (relógio da ingestão) e TODA escrita
aceita renova. Um patch que não mexe na autenticação (dying gasp vindo do TEMS)
mantém vivo para sempre um serviço sem Radius. O legado remove o serviço cujo
último pacote tem mais de 2 dias.

## Contrato (YAML aprovado)

```yaml
materialization:
  ttl:
    - when: "lastAuthAt IS NULL"
      expireAfterWrite: 1h
    - anchor: lastAuthAt
      expireAfter: 48h
```

- Regra com `anchor` só casa se o campo estiver PRESENTE no estado pós-merge
  (ausente → próxima regra). `anchor` combina com `when` (as duas condições).
- `expireAfter` (relativo à âncora) e `expireAfterWrite` (relativo à escrita)
  são mutuamente exclusivos; um dos dois é obrigatório.
- Prazo = `min(anchor, _writtenAt) + expireAfter` — âncora no futuro é
  LIMITADA ao instante da escrita e contada numa métrica nova.
- Regra de FUNDO passa a ser "sem `when` E sem `anchor`", e só pode ser a
  última (hoje é "sem `when`"). `fallbackDuration` / `FallbackDurationMs` só
  olham a regra de fundo; uma regra-âncora na última posição NÃO é fundo.
- `anchor` + `never` é recusado na subida (a âncora seria irrelevante).
- O piso `ExpiryResolution` (1 min) vale para `expireAfter`.
- Validação do campo âncora no COMPILE (não no parse do YAML): tipo
  `timestamp` ou `datetime` — inclusive coluna congelada de tipo herdado
  (`frozenFrom: "@writeTime"` vira timestamp no compile); recusar `auto`,
  campo excluído, campo de sistema, campo inexistente; decidir e documentar se
  coluna derivada (`extract:`) é aceita (preferência: aceitar se tipada
  timestamp/datetime, porque ela é coluna do estado pós-merge como as outras —
  confirme que `fillSlots`/deadline a enxerga).
- `merge: sum` sobre o campo âncora é impossível (tipo temporal); recuse se a
  combinação existir.

## Onde o prazo vive (decisão: reaproveitar `_expiresIn`, opção B)

O storage hoje calcula `vencimento = _writtenAt + _expiresIn` e é agnóstico ao
schema; a agenda é reconstruída de `expiry.idx` + `prazos.idx` sem ler records.
A âncora é resolvida no MATERIALIZER, sobre o payload pós-merge, e gravada como
`_expiresIn = min(anchor, writtenAt) + d − writtenAt` — função pura do record
(o `_writtenAt` é carimbado no payload na ingestão), então replay e
reconstrução dão o mesmo prazo. O storage NÃO muda de semântica de cálculo e
NÃO lê campo de negócio.

**Trava obrigatória:** `TTLs.Set` grava `millis <= 0` como imortal
(`rowindex/rowindex.go:~451`), `durationOfPayload` devolve 0 (imortal) para
`ms <= 0` (`store.go:~2122`) e `1` é o sentinela `ImmortalTTL`. Uma âncora
vencida na escrita daria `_expiresIn <= 0` e viraria IMORTAL. O materializer
tem de levar o resultado ao mínimo representável que significa "já vencido"
(≥ 2 ms, ou o sentinela correto — confira `FirstRealExpiry`/`Grace` em
`rowindex/schedule.go`) e o teste prova que a linha some da consulta logo após o
commit e vira lápide na varredura seguinte. Estouro de `anchor + d` (int64)
saturado, com teste.

A semântica documentada de `_expiresIn` muda de "duração de ociosidade" para
"prazo contado a partir de `_writtenAt`" — atualizar godoc
(`internal/schema/ttl_rule.go`), glossário e README (§ TTL). Testes que fixam
"toda escrita renova" continuam valendo para regras sem `anchor`; godoc do
`TestCicloRenovacaoEmpurraOPrazo` e o README ("não há teto de idade absoluta")
mudam de texto: há teto quando se usa `anchor` sobre `frozenFrom: "@writeTime"`
— verifique se isso fecha o débito 84 e, se fechar, marque-o no
`docs/debitos.md` (nunca renumere).

## Pré-requisito: defeito latente no `when` do TTL (corrigir PRIMEIRO, commit próprio)

`fillSlots` (`internal/materializer/deadline.go:~52-87`) sobrepõe a entry
do patch sem olhar `e.congelada`, e a congelada de relógio emite entry com
`agora` em TODO evento (`internal/materializer/frozen.go:~32-37`); só o merge
preserva o valor anterior (`merge.go:~89`). Consequência: um `when` de TTL (e a
âncora) sobre coluna congelada lê o valor do EVENTO, não o congelado; o mesmo
para `merge: sum` (`e.acumula`) — o slot vê a parcela, não a soma. Corrija para
que o slot veja o valor PÓS-merge real (o mesmo que vai para o record), com
teste vermelho hoje (regra `when: "first > X"` sobre congelada; regra sobre
campo `merge: sum`) e mutante.

## Observabilidade (aditivo)
- `/v1/stats` `ttlRules[]` ganha `anchor` e `expireAfter` (omitempty); log
  `ttl.regras` idem.
- Métricas novas (constantes em `internal/metrics/names.go`, nascem com zero,
  `docs/metricas.md`): âncora ausente que caiu para a próxima regra (por
  regra), âncora no futuro limitada, prazo já vencido na escrita. Mesma
  estrutura para `/v1/stats` e `/metrics`.

## Garantias — teste que fica VERMELHO com a mutação

1. Escrita que NÃO muda a âncora não renova o prazo (dois eventos: o segundo
   sem `lastAuthAt` não move o vencimento). Mutante: ancorar em `_writtenAt`.
2. Escrita que muda a âncora move o prazo para `nova + d`.
3. Âncora ausente cai na próxima regra; sem próxima, não expira (como hoje).
4. Âncora vencida na escrita: linha invisível na consulta logo após o commit e
   lápide na varredura — NUNCA imortal. Mutante: tirar a trava.
5. Âncora no futuro: prazo = `writtenAt + d`, métrica incrementa. Mutante: sem
   `min`.
6. Replay/recovery: o prazo pós-recovery é o mesmo (prazos.idx/agenda
   reconstruídos batem byte a byte com a aplicação contínua).
7. Regra-âncora na última posição não vira fundo (`FallbackDurationMs` não
   muda; linhas legadas sem `_expiresIn` não ganham o prazo da âncora).
   Mutante: `fallbackDuration` pega a última regra.
8. Recusas na subida, cada uma asseverando a MENSAGEM PRÓPRIA: `anchor` +
   `expireAfterWrite`; `expireAfter` sem `anchor`; `anchor` + `never`; âncora
   não temporal / `auto` / inexistente / excluída / de sistema; regra de fundo
   fora da última posição continua recusada.
9. Combinação `when` + `anchor`.
10. Defeito latente: `when` sobre congelada e sobre `merge: sum` lê o valor
    pós-merge (vermelho hoje).
11. Âncora sobre congelada `@writeTime` dá teto de idade absoluta (não renova).
12. Zero alocação no caminho do materializer com regra-âncora (gate novo ao
    lado dos existentes; mutante que aloque de verdade).
13. Ponta a ponta com o YAML do contrato: serviço autenticado + dying gasps
    sem `lastAuthAt` expira em `lastAuthAt + 48h`; serviço só com dying gasp
    vive 1h renovável; ambos interagem certo com um contador com
    `scope: authenticated IS NOT NULL`.

## Docs
README (TTL), `docs/architecture.md` se descrever o prazo, aceite novo
`docs/aceite-ttl-ancorado.md` (rodada, testes, mutações vermelho/verde),
`docs/metricas.md`, glossário (termos listados no relatório; o orquestrador
edita). Divergência registrada pela pesquisa: `docs/replication.md:~42-44`
ainda afirma que o TTL usa o instante do master (o débito 131, em outro branch,
diz que é falso) — NÃO corrigir aqui; só não piorar.

## Fora
- Prazo absoluto (`expireAt`, "expira EM" o valor do campo) — não pedido.
- Relógio do follower (#82).

---

# Revisão 2 — decisões da refutação (NORMATIVAS; prevalecem sobre o texto acima)

T1. **O prazo é decidido sobre o payload que vai ao `Put`, e só sobre ele.**
Hoje `durationOf(m.prevRec.Payload, d.entries)` roda para qualquer presença
(`materializer.go:~495`), mas o merge só acontece com `PresenceLive && !replace`
(`:~515`); numa linha VENCIDA SEM LÁPIDE e em `mode: replace` o `when` enxerga
campos-fantasma. Desenho: emitir a entry `_expiresIn` com valor provisório,
rodar `mergeRecords`/`encodePatch`, avaliar as regras com `FieldReader` sobre o
`out` final e sobrescrever no lugar os 8 bytes do int64 (largura fixa,
`internal/storage/encoding/fields.go:~79-80`). Isso SUBSTITUI a correção do
`fillSlots` da seção "Pré-requisito": o `when` e a âncora passam a ver o
estado real pós-merge (congelada, `merge: sum`, presença, replace) sem
reimplementar política. Commit próprio, ANTES da âncora, com testes-mutante:
(a) vencida sem lápide + patch sem âncora → regra 1 (mutante: avaliar sobre o
anterior); (b) replace com campo só no anterior → `when` não casa; (c) `when`
sobre congelada de relógio e de campo lê o valor congelado; (d) `when` sobre
`merge: sum` lê a soma. Zero alocação: o caminho do prazo (`Aplica` até o
`_expiresIn` gravado, sobre store falso) ganha gate de `AllocsPerRun` NOVO —
hoje não existe (`alloc_gate_test.go` só mede o `Parse`) — com mutante
`fmt.Sprintf`.

T2. **Predicado único de fundo:** `TTLRule.IsFallback()` = sem `when` E sem
`anchor`, usado por `validateTTLRules`, `fallbackDuration`, `CompileRules`
(`internal/predicate/rule.go:~27`, que hoje gera `Prog nil` para `When==""`),
o ramo `Prog nil` do `durationOf` (`deadline.go:~31`, "casa com tudo"),
`DescribeRules` (`ttl_rule.go:~173`) e o `temFundo` do log de subida
(`server.go:~559`, que já erra hoje com fundo `never`). `rulesFromNode`
(`ttl_rule.go:~141-147`) tem de ler `anchor`/`expireAfter` em campos próprios
(hoje o alvo padrão é `&r.When`). Mutante: o ramo `Prog nil` sem checar a
âncora.

T3. **Conta sem estouro, por comparação:** se `anchor <= writtenAt − d`
(compare sem subtrair de forma que estoure; `writtenAt − d` com d ≥ 1 min e
writtenAt epoch atual é seguro, mas prove/guarde) → já vencido → valor mínimo
de T4; senão `expiresIn = anchor + d − writtenAt` ∈ (0, d] quando
`anchor ≤ writtenAt`; e `anchor > writtenAt` → `expiresIn = d` (limite ao
instante da escrita, métrica "âncora no futuro"). Testes: âncora `MinInt64`,
fronteira exata `anchor == writtenAt − d`, âncora no futuro, `MaxInt64`.

T4. **Valor mínimo = constante nomeada `ImmortalTTL + 1` (= 2 ms).** O domínio é
DURAÇÃO (`ImmortalTTL=1`, `rowindex.go:~411`); `Never/Grace/FirstRealExpiry`
são do domínio do instante da agenda e não se aplicam. Com 1 o índice diria
imortal e o payload vencido (linha invisível que o coletor nunca recolhe).
Garantia 4 reescrita: invisível para todo instante ≥ `writtenAt + 2 ms` e
recolhida pelo coletor. Mutantes: "sem trava" e "mínimo = 1". Corrija o godoc
do `ImmortalTTL` ("a menor declarável é um minuto").

T5. **Linha que nasce vencida — semântica ACEITA pelo consumidor** (backlog
relido: serviço antigo nasce vencido): a escrita seguinte vê
`PresenceExpired` e parte do vazio; o contador conta a linha até a lápide (o
coletor drena ≤ 1000 por passada). Teste próprio; doc e aceite; a garantia 13
fixa o valor do contador ANTES e DEPOIS da passada do coletor.

T6. **Débito 84 NÃO fecha:** a congelada `@writeTime` registra a primeira
escrita da ENTIDADE, não a primeira vez que a condição valeu. Registre no
próprio item como atendimento parcial (teto desde o nascimento), sem
renumerar.

T7. **A correção T1 muda comportamento** de datasets com `when` sobre congelada
(de relógio OU de campo) ou `merge: sum`, a partir da próxima escrita de cada
linha (os `_expiresIn` gravados ficam). É o código se alinhando ao README ("A
condição vê o estado PÓS-MERGE"). Registre no aceite como correção de
comportamento, para as notas da release.

T8. **`/v1/stats` e log:** `TTLRuleStats.ExpireAfterWrite` (sem omitempty, wire
publicado) continua presente e fica VAZIO (`""`) numa regra-âncora — nunca
publica 48h sob `expireAfterWrite`. Campos novos `anchor` e `expireAfter` com
omitempty (aditivo). Regras existentes saem byte a byte iguais. Ajuste
`materialization_http_test` e o log `ttl.regras`/`temFundo` (via T2).

T9. **Toda garantia nomeia o seu mutante.** A garantia 6 (replay) é vazia como
escrita — o replay lê `_expiresIn` do record; reformule: o prazo é função do
record (mutante: o materializer relendo `m.agora()` em vez do carimbo
`_writtenAt` → dois relógios diferentes dão prazos diferentes para o mesmo
record). Gate 12 = T1.

T10. **Textos normativos a reescrever** (todos dizem "duração de ociosidade" ou
"sem `when` = fundo"): `internal/schema/ttl_rule.go:~18-22,~30-31,~132`
(mensagem de exemplo), `internal/schema/compile.go:~369,~376`,
`internal/storage/store.go:~201,~2120-2121`, `rowindex.go:~409-413` (godoc de
`ImmortalTTL`/`TTLs`), `docs/architecture.md:~1705-1711,~1717,~1735-1739,~1856`,
README `~:800-818`, Help de `MetTTLUnmatched`.

T11. A recusa de `merge: sum` na âncora sai da spec (inalcançável: `sum` exige
int64, a âncora exige temporal).

T12. **Idioma:** mensagens de erro/log e Help de métrica NOVOS em inglês;
mensagens existentes nas funções que o bloco REESCREVE vão para inglês junto
(não são contrato), e os testes que as asseveram acompanham. Chaves de log e
nomes de métrica existentes não mudam.

T13. **Rename incremental** dos identificadores PT-BR tocados, pares no
`mapeamento-2-manual.tsv` (ex.: o preenchimento dos slots, que já tinha par no
binder; o prazo, a lista de regras compiladas, o compilador delas e os
contadores por regra — os que o bloco tocar). Os nomes velhos desta lista foram
trocados pelos novos na passada do `scripts/renamecite`; os pares estão no TSV.

T14. **Métrica "âncora ausente":** avaliação é `when` primeiro; conta só quando
o `when` casou (ou não existe) e a âncora está ausente, por regra (rótulo =
índice no YAML, como as existentes). Recusa de `auto` olha a DECLARAÇÃO
(`IsAuto`), nunca o tipo observado.

T15. Doc: com âncora no futuro (limitada ao `writtenAt`), uma reentrega do Kafka
empurra o prazo para frente — uma linha na doc.

T16. Doc de operação: trocar um YAML que TINHA regra de fundo por um sem fundo
zera `FallbackDurationMs`, reconstrói a agenda e deixa imortais as linhas
legadas sem `_expiresIn` — avise no README (o `service_state` nasce novo e não
é afetado).
