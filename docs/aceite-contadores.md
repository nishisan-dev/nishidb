# Aceite — contadores mantidos na escrita

Rodada de aceite em 04/09/2026, na branch `feature/contadores-mantidos-na-escrita`,
sobre o binário `v0.7.0-19-g021603e-dirty` (o `021603e` é o commit da
documentação; o `-dirty` é a bancada deste documento). Máquina: Intel Core 7
150U (12 threads), NVMe com ext4, Linux 6.8. Registro de rodada: não é reescrito.

Origem dos números: a saída crua das bancadas em
[`contadores-20260904.txt`](../benchmark/baselines/contadores-20260904.txt)
e a rodada ponta a ponta com o binário real sobre a amostra `samples/` (1 045
registros de uma view de serviços RADIUS; a amostra não é versionada, e o
script que a ingere está transcrito na seção "Ponta a ponta" abaixo).

## O critério, e como ele tem de ser lido

A proposta ([`planning/contadores-mantidos-na-escrita.md`](../planning/contadores-mantidos-na-escrita.md),
seção 10) fixou as invariantes, e o plano
([`planning/contadores-mantidos-na-escrita-plano.md`](../planning/contadores-mantidos-na-escrita-plano.md))
fixou as réguas: **alocações por evento iguais** com e sem contadores; custo por
evento em **microssegundos** (uma passada extra pelo payload por contador);
dobra de um milhão de linhas em **dezenas de milissegundos**; reconstrução na
**ordem do full scan**. As quatro estão medidas abaixo, e a segunda tem uma
ressalva que a bancada revelou e que este documento registra: o custo em regime
e o custo da carga fria são números diferentes, e o primeiro relatório os
confundiu.

## Critérios e as evidências

| Critério (proposta, seção 10) | Provado por |
|---|---|
| Conta **estados**, não mensagens: reprocessar o mesmo evento é transição X → X, ajuste zero | `TestReprocessarOMesmoEventoNaoMexe`, `TestCobreMexidasAcompanhaAsTransicoes` |
| Mudar de grupo desconta do antigo e soma no novo na **mesma** operação | `TestMudarDeGrupoAjustaAsDuasPontas` |
| A lápide desconta e **preserva** o grupo; a ressurreição volta a contar | `TestLapideDescontaEPreservaOGrupo`, `TestRessurreicaoVoltaAContar` |
| Os três caminhos de saída de cena (sobrescrita, expiração, expiração oportunista da compaction) convergem no `applyRecord`; o copy-forward não mexe | `TestContadorAcompanhaPutLapideECopyForward`, `TestExpiracaoOportunistaDescontaDoContador`, `TestCopyForwardNaoMexeNoContador` |
| `totais == fold(vetor)`: os totais são dobra do vetor **antes** do replay, e o crash converge com e sem checkpoint no meio | `TestDobraReproduzOsTotais`, `TestContadorRecuperaDoTailSemCheckpoint` (duas variantes), `TestContadorSobreviveAoCheckpointEReabertura` |
| O vetor em disco nunca fica à frente do log durável (overlay aplicado depois do `segSync`) | `TestOverlayNaoVaiAoMmapAntesDoApply` |
| Dicionário: fsync **antes** de devolver o id; cauda rasgada truncada; id nunca reemitido; teto por grupos vivos. **Nota de 01/10/2026:** o fsync por grupo novo deu lugar ao fsync do dicionário no checkpoint (débito 108); o primeiro teste foi renomeado e passou a provar a ordem nova, não a desta rodada | `TestDictInternDefersTheSyncToTheCheckpoint`, `TestDicionarioTruncaCaudaRasgada`, `TestDicionarioAposentaENuncaReemite`, `TestDicionarioRecusaOTeto`, `TestTetoDeGruposRecusaEConta` |
| Entrada órfã na dobra é zerada e contada | `TestDobraTrataIdAposentadoComoOrfa` |
| Adotar ou reconstruir pelo manifest; contador removido é apagado; `maxGroups` não reconstrói e é recusado abaixo do uso; sem callback ou no histórico, recusa | `TestContadorNovoEhConstruidoNaAbertura`, `TestContadorComWhereDiferenteEhReconstruido`, `TestContadorRemovidoDoYamlEhApagado`, `TestMudarMaxGroupsNaoReconstroi`, `TestMaxGroupsMenorQueOsGruposEmUsoEhRecusado`, `TestAberturaSemCallbackRecusaDatasetComContadores`, `TestContadorEhRecusadoNoHistorico` |
| A seção do manifest é ignorável, não muda os bytes do dataset sem contadores e recusa truncamento | `TestManifestComContadoresRoundTrip`, `TestManifestSemContadoresNaoEmiteASecao`, `TestASecaoDosContadoresEhIgnoravel`, `TestCaudaSaiEmOrdemCrescenteDeTagComContadores`, `TestSecaoDosContadoresTruncadaEhRecusada` |
| Auditoria sob snapshot sem segurar o writer; "viva" = location e sem lápide; detecta deriva; snapshot estourado é inconclusiva; honra o contexto | `TestAuditoriaSemDivergenciaComIngestaoConcorrente`, `TestAuditoriaCopiaOsTotaisNoMesmoInstanteDoSnapshot`, `TestAuditoriaContaLinhaVencidaSemLapide`, `TestAuditoriaDetectaDeriva`, `TestAuditoriaInconclusivaQuandoOSnapshotEstoura`, `TestAuditoriaRespeitaOContexto`, `TestReconstruirZeraADeriva` |
| Ciclo de vida por `refs == 0` (nunca `total == 0`), prazo de visibilidade, cobertura do checkpoint, `never`, reinício que só estende | `TestGrupoZeradoComRefsNaoEhAposentado`, `TestGrupoSemRefsEhAposentadoDepoisDoPrazo`, `TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida`, `TestRetainNeverNuncaAposenta`, `TestReinicioEstendeAVisibilidade`, `TestAposentadoriaSobreviveAReabertura`, `TestAposentadoriaDeGruposNoStore`, `TestLacoDeAposentadoriaSobeEPara` |
| O bloco `counters:` aceita a forma da proposta, aplica os defaults e recusa na subida | `TestCountersAceitaAFormaDaProposta`, `TestCountersRecusas`, `TestCountersAceitaColunaExtraida`, `TestCountersNormalizaOWhere`, `TestOPrefixoReservadoNomeiaOsDoisNamespaces` |
| Compilação: `where` pelo mesmo caminho das regras de TTL; recusa `merge: sum` e campo de sistema; o schema compilado dentro do `Open` é o do motor | `TestCompilaRecusaWhereSobreCampoSomado`, `TestCompilaRecusaGroupBySobreCampoSomado`, `TestCompilaRecusaWhereComCampoDeSistema`, `TestCompiladorDevolveOMesmoSchemaQueOCallbackCompilou` |
| `_counters.<nome>` responde as consultas da proposta pelo pipeline inteiro; lê o presente; grupo zerado aparece, aposentado não | `TestTabelaDoContadorRespondeAsConsultasDaProposta`, `TestTabelaLeOPresenteEOConjuntoDeGruposCongelaPorConsulta`, `TestExecuteRoteiaTabelaVirtualRegistrada`, `TestNamespaceDesconhecidoListaOsExistentes`, `TestTabelaVirtualDesconhecidaListaAsDoDataset`, `TestCountersRespondePelaRotaDeQuery`, `TestQueryLocalRespondeCountersMantidosNaIngestao` |
| Zero alocação no caminho quente, camada a camada, e o `Put` real não aloca mais com contadores | `TestDenso32NaoAlocaNoCaminhoQuente`, `TestDicionarioNaoAlocaNoLookup`, `TestContadorNaoAlocaNoCaminhoQuente`, `TestAvaliadorDePayloadNaoAloca`, `TestPercorreCompostaVisitaEmOrdemSemAlocar`, `TestTabelaDoContadorNaoAlocaPorLinha`, `TestPutNaoAlocaMaisComContadores` |
| Formato de disco com golden de bytes (chave composta, `groupKey`, vetor de 4 bytes) | `TestChaveCompostaEhGoldenDeDisco`, `TestChaveCompostaNaoEhAmbigua`, `TestTextoDaChaveEhGolden`, `TestDenso32RecusaArquivoDeOitoBytes`, `TestAtomicos32EnderecamMeiaWord` |
| Operação: séries nascem com zero, `/v1/stats` e `/metrics` leem o mesmo, rotas ADMIN fail-closed, CLI local e remoto com os exit codes do contrato | `TestMetricasDeContadorNascemComZero`, `TestStatsEMetricsLeemOMesmoContador`, `TestAuditoriaDeContadoresPelaRota`, `TestReconstrucaoDeContadoresPelaRota`, `TestRotasDeContadoresExigemAutenticacaoEPapelADMIN`, `TestCountersLocalListaAuditaEReconstroi`, `TestCountersRemotoChamaAsRotas`, `TestDesfechoDaAuditoriaDivergenteSaiComExitDataset` |

## As mutações: o vermelho e o verde de cada uma

Doutrina de verificação: para cada invariante, o defeito foi aplicado, a suíte
ficou vermelha, o defeito foi revertido, a suíte voltou ao verde (inclusive sob
`-race` nos pacotes `storage` e `counter`). Nenhuma delas ficou no código
(conferido por `git status` antes de cada commit).

| Mutação aplicada | Reprovou em | O que a falha disse |
|---|---|---|
| Dicionário sem o `Sync` antes de devolver o id (**nota de 01/10/2026:** a garantia mudou com o débito 108, e o teste renomeado prova hoje a ordem nova — o grupo novo sem fsync e o fsync único no checkpoint) | `TestDictInternDefersTheSyncToTheCheckpoint` | com pânico injetado entre o write e o sync, a reabertura resolvia um id que não deveria existir |
| Sem o decremento de `refs` na troca de grupo | `TestMudarDeGrupoAjustaAsDuasPontas`, `TestDobraReproduzOsTotais` | a ponta antiga ficava com `refs` a mais, e a dobra numa segunda instância deixou de bater com o mantido |
| Lápide sem o decremento do `total` | `TestLapideDescontaEPreservaOGrupo`, `TestRessurreicaoVoltaAContar` | total 1 depois da lápide; ressurreição somou 2 |
| `Transition` gravando direto no mmap, sem overlay | `TestOverlayNaoVaiAoMmapAntesDoApply` | uma segunda instância dobrava totais de transições ainda não checkpointadas |
| Sem o early-return de `antiga == nova` (aplicada nesta rodada de aceite) | `TestReprocessarOMesmoEventoNaoMexe`, `TestLapideDescontaEPreservaOGrupo`, `TestLinhaSemColunaDeAgrupamentoNaoConta`, `TestCobreMexidasAcompanhaAsTransicoes` | "reprocessar não é transição: transições = 3, quer 1". O plano previa que esta mutação ficasse **verde** — para os totais ela é inofensiva por construção, porque incrementar e decrementar o mesmo grupo dá zero — mas o contador de transições alimenta a cláusula de cobertura da aposentadoria, e os testes o travam: reprocessamento não é mexida |
| Passo 7c do `applyRecord` ignorando lápides | `TestContadorAcompanhaPutLapideECopyForward`, `TestExpiracaoOportunistaDescontaDoContador`, `TestContadorRecuperaDoTailSemCheckpoint` | as três portas de saída de cena (Put/Expira, expiração oportunista, replay) deixaram de descontar |
| Contadores abertos (e dobrados) só **depois** do replay | `TestContadorRecuperaDoTailSemCheckpoint` (variante sem checkpoint) | o replay transicionou sobre um vetor que ainda não tinha sido dobrado: totais divergentes da instância de referência |
| Auditoria copiando os totais **fora** do `wmu`, depois de abrir o snapshot | `TestAuditoriaCopiaOsTotaisNoMesmoInstanteDoSnapshot` | divergência de 1 num grupo. Registrado: a ingestão concorrente de `TestAuditoriaSemDivergenciaComIngestaoConcorrente` **não** acerta a janela (microssegundos), e por isso o teste que a prova usa um seam determinístico que injeta uma escrita entre o snapshot e a cópia |
| Enumerador da auditoria trocado por `GetByRowID` (visibilidade da consulta) | `TestAuditoriaContaLinhaVencidaSemLapide` | a linha vencida sem lápide sumiu do recalculado e virou divergência |
| `total == 0` no lugar de `refs == 0` na aposentadoria | `TestGrupoZeradoComRefsNaoEhAposentado` | 500 linhas fora da condição, total zero, grupo aposentado com 500 entradas apontando para ele. Registrado: a mutação **só** de `refs` para `total` foi mascarada por `emptySince` (o instante de esvaziamento só é gravado quando `refs` chega a zero), e a mutação que expõe a regra intuitiva inteira ignora esse instante também |
| Sem a cláusula `lastTouch <= coveredTouches` | `TestGrupoSoEhAposentadoDepoisDoCheckpointQueCobreASaida` | o grupo foi aposentado antes de o checkpoint cobrir a transição que o esvaziou |

## A bancada, número a número

Todas as bancadas de ingestão rodaram em disco real (`NISHIDB_BENCH_DIR` em
ext4), 20 000 eventos por rodada, seis rodadas, comparadas por `benchstat`.
O corpus é o misto de referência do `benchmark/micro` (85 % patch, 300 NAS
distintos), com dois contadores por `NAS-Identifier` — o denominador e o
numerador `where status = 'Start'`, a mesma forma do caso de uso.

| Grandeza | Medido | Leitura |
|---|---|---|
| `Put` real de update, alocações por chamada | **1,0 sem / 1,0 com** dois contadores | o gate `TestPutNaoAlocaMaisComContadores` trava as duas coisas: o número não sobe, e os contadores não o sobem. A alocação que existe é anterior a este ciclo |
| Ingestão **em regime** — grupos internados, linha no overlay (`BenchmarkApplyIngestRealRegime` × `…RegimeComContadores`) | **3,99 µs → 4,51 µs por evento** (+13 %, p = 0,041, n = 6); **0 B/op e 0 allocs/op nos dois** | ≈ 0,26 µs por contador por evento: uma passada pelo payload, um lookup no dicionário, dois atômicos. É o que uma ingestão 24/7 paga |
| Ingestão **fria** — aquecimento de 512 eventos, como o `BenchmarkApplyIngestReal` (`…Frio` × `…FrioComContadores`) | **13,12 µs → 31,40 µs por evento** (+139 %, p = 0,002) | 300 NAS × 2 contadores = 600 grupos internados **com fsync** dentro dos 20 000 eventos: ≈ 366 ms a mais no total, **≈ 0,6 ms por grupo novo**. É o custo da carga inicial, bounded pelo inventário — o débito 108 mede exatamente isto no nível de PON |
| Dobra de 1M linhas, 2 048 grupos (`BenchmarkDobra1M`) | **37,9 / 38,2 / 51,7 ms** (3 rodadas); 1 alocação de 64 B | ≈ 36–49 ns por linha. Fica no caminho do `Open`: em 8M linhas, ~0,3–0,4 s por contador |
| Reconstrução **em operação**, 1M linhas vivas, 2 contadores (`BenchmarkReconstroiContadores1M`) | **1,675 s por rebuild** (3 rodadas); 790–873 ms por contador | ≈ 0,8 µs por linha por contador — a ordem do full scan, com o writer parado. O dicionário é mantido (`Clear`), então não há fsync por grupo |
| Reconstrução por **mudança de `where`**, 1 010 linhas, 265 grupos (ponta a ponta) | **299 ms** | recria o dicionário: um fsync por grupo, ≈ 1,1 ms cada. Numa reconstrução por parâmetro, o inventário manda, não as linhas |
| Auditoria, 1 010 linhas (ponta a ponta, local e pela rota) | 2–3 ms por contador | |

**O que o primeiro relatório errou, para que ninguém reerre.** A primeira
rodada da variante com contadores saiu com **36 µs por evento numa única
amostra**: o `INFO` de subida dos contadores (`contadores.abertos`) foi para o
stderr no meio da linha de resultado e quebrou cinco das seis, e a única
sobrevivente media a carga fria — o aquecimento de 512 eventos não cobre os
300 NAS, e os 20 000 eventos medidos pagavam a internação. A bancada passou a
silenciar o logger e a separar as duas perguntas em dois pares de benchmarks;
os dois números estão na tabela.

## Ponta a ponta com o binário

`make build`, e sobre a amostra `samples/service-view-topic-dump-sample.json`
(1 046 registros pretty-printed; o último está cortado no fim do arquivo e o
`jq -c .` o descarta — 1 045 eventos entram, 1 010 entidades, 35 atualizações).
O dataset:

```yaml
version: 1
name: service_view
materialization:
  key: {source: payload, field: customerId, type: string}
  mode: merge
schema:
  include: ["*"]
  unknownFields: include
  fields:
    customerId: {type: string}
    authenticated: {type: boolean}
    lastAuthPacket:
      type: json
      extract:
        - {path: attributes.NAS-Identifier, as: olt, type: string}
counters:
  - name: servicos_por_olt
    groupBy: [olt]
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
```

| Passo | Resultado |
|---|---|
| `nishidb ingest` | 1 045 lidos, 1 045 aplicados (1 010 criados, 35 atualizados); log `contadores.abertos` com os dois contadores |
| `SELECT olt, total FROM _counters.autenticados_por_olt ORDER BY total ASC LIMIT 5` (local) | cinco OLTs com total 0 — existem no denominador e não têm ninguém autenticado: a linha do alarme |
| `SELECT olt, total FROM _counters.servicos_por_olt ORDER BY total DESC LIMIT 3` | `i-br-pr-cta-gt2-hl4-04` 156, `…cta-hl4-02` 133, `…kpm-hl4-01` 61 — o primeiro confere com `jq \| sort \| uniq -c` (156) |
| `WHERE groupKey = 'i-br-pr-cta-gt2-hl4-04'` | point lookup, 156 |
| `nishidb counters --config` (lista e `--audit`) | 265 grupos, teto 65 536; auditoria 1 010/1 010 e 461/461, **0 divergências**, 2 ms, exit 0 |
| Servidor com TLS e usuário ADMIN; `query --server` e `counters --server --audit` | mesmos números pela rota; 0 divergências em 3 ms |
| `/metrics` | as **8 séries × 2 contadores**, `audit_last_success_timestamp` preenchido depois da auditoria |
| Log da subida | **sem** `counter.construido`: adoção pelo manifest |
| SIGTERM | exit 0 |
| Reinício | adoção de novo, sem construção |
| `where` mudado para `authenticated = false` e reinício | `counter.construido` com motivo `os parâmetros mudaram (groupBy [4] → [4], where "authenticated = true" → "authenticated = false")`, 1 010 linhas, 265 grupos, 299 ms; auditoria **549/549**, 0 divergências (461 + 549 = 1 010) |
| `counters --server --rebuild` | 1 010 linhas, 265 grupos, 3 ms; SIGTERM, exit 0 |

O que a rodada **não** exercitou: SIGTERM no meio de uma auditoria — sobre 1 010
linhas ela termina em 3 ms, e não há janela para o sinal. A prova de que a
auditoria honra o cancelamento é `TestAuditoriaRespeitaOContexto`, e a rota
corre sob o mesmo `ctxOfMutacao` de `compact` e `checkpoint`.

## Veredito

**ACEITO.** As quatro réguas do plano:

- **alocação**: 0 allocs/op com e sem contadores na bancada de ingestão, e o
  `Put` real gateado em 1,0 nos dois regimes;
- **custo por evento em regime**: +0,5 µs por evento para dois contadores
  (+13 %), sem alocação; a carga fria paga ≈ 0,6 ms por grupo novo, uma vez por
  grupo, e isso está dito no README e no débito 108;
- **dobra**: 38–52 ms por milhão de linhas, no caminho do `Open`;
- **reconstrução**: 0,8 s por milhão de linhas por contador em operação — a
  ordem do full scan, com o writer parado, como o plano fixou.

E as invariantes da proposta estão provadas por teste, com onze mutações
aplicadas, vermelhas e revertidas.

## O que ficou como débito

Registrados em [`debitos.md`](debitos.md) durante o ciclo: a corrida do
`OpenSnapshot` das consultas fora do `wmu` (107, pré-existente, achado na
validação), o fsync por grupo novo no nível de PON ou splitter (108),
`_counters` fora do isolamento de snapshot (109), a tabela virtual sem índice
para `WHERE olt = …` em contador composto (110) e o `ScanLive` não versionado
para lápides (111, pré-existente).

## Método

Bancadas: `NISHIDB_BENCH_DIR=<ext4> go test ./benchmark/micro/ -run='^$'
-bench='BenchmarkApplyIngestReal(Regime|Frio)' -benchmem -benchtime=20000x
-count=6`, pares comparados com `benchstat`; `go test ./internal/storage/counter/
-bench=BenchmarkDobra1M -count=3`; `go test ./internal/storage/
-bench=BenchmarkReconstroiContadores1M -benchtime=3x`. As saídas cruas estão em
[`contadores-20260904.txt`](../benchmark/baselines/contadores-20260904.txt).
O ponta a ponta é o script transcrito acima, com certificado e senha efêmeros
gerados como em `scripts/it-comum.sh`.
