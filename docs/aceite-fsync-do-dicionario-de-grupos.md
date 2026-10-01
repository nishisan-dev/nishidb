# Aceite — fsync do dicionário de grupos no checkpoint e overlay que devolve memória

Rodada de aceite em 01/10/2026, na branch `feature/counter-dict-fsync-at-checkpoint`,
sobre `feature/counter-scope-rows` (PR #90) em `aca4ffe`. Fecha o débito 108 e o
problema 1 da issue #88. O plano normativo é
[`planning/counter-dict-fsync-at-checkpoint.md`](../planning/counter-dict-fsync-at-checkpoint.md),
com a Revisão 2 (F1–F10) prevalecendo sobre o texto anterior. Registro de
rodada: não é reescrito.

## O critério

Medido no piloto `.218` em 30/09/2026 (issue #88), dois defeitos bloqueavam a
integração service-view → `service_state` em produção:

1. **O grupo novo custava um fsync.** O `Intern` sincronizava o `grupos.dict`
   antes de devolver o id: ~1,95 ms por grupo, ~80 min de writer parado para
   os ~2,46M grupos da carga fria;
2. **O overlay não devolvia memória.** O mapa do Go não encolhe com `delete`, e
   a construção de um contador deixava ~319 MB presos até o restart.

O critério é derrubar os dois sem abrir janela de crash nova — e a refutação do
plano achou, no caminho, um terceiro defeito anterior ao bloco: a reabertura
adotava o vetor de uma construção interrompida.

## O desenho, e por que ele é diferente do que o débito 108 descrevia

O débito 108 falava em "reconstrução quando o vetor referenciar id além do
dicionário". Não entrou, porque o replay NÃO garante os mesmos ids (a
aposentadoria não é record, a recusa pelo teto depende de quantos grupos estão
vivos, e a construção interna por rowId): comparar o id do vetor com o tamanho
do dicionário não discrimina nada. O que entrou:

- **R1 — a invariante.** Nenhum artefato durável referencia id de grupo além do
  prefixo já fsyncado do `grupos.dict`. Nenhum record carrega id de grupo; os
  únicos referenciadores duráveis são o vetor (só recebe id no `ApplyOverlay`) e
  o snapshot das medidas (gravado depois dele). O `Intern` faz só o append, e o
  fsync de todos os appends pendentes é a **primeira coisa do `ApplyOverlay`**
  de cada contador (F6) — o único caminho que grava id no vetor —, antes do
  primeiro `Set` e antes do `SaveMeasures`, portanto antes do manifest e do
  `kafka.offsets` (§48 intacto). Dicionário limpo não sincroniza.
- **F1 — abertura sincronizada.** O page cache sobrevive à morte do processo: a
  cauda que um processo anterior escreveu sem fsync é lida pela abertura como
  se estivesse em disco, o replay resolve as linhas aos MESMOS ids e nada mais a
  sincronizaria. O `OpenDict` faz um fsync depois da varredura sempre que o
  arquivo tem mais que o header, e o `Close` sincroniza o append pendente — o
  que cobre o rollback com parada limpa para o binário anterior, que nunca
  sincroniza cauda herdada.
- **R4/F3 — writer falho.** A falha desse fsync, e a do registro de
  aposentadoria (que leva junto os appends pendentes), saem como
  `counter.ErrDictSync`, e o Store as manda para o `failWriter`. Uma retentativa
  "bem-sucedida" deixaria buraco no meio do arquivo (fsyncgate).
- **R5/F7 — piso de durabilidade.** O checkpoint grava, por contador, o tamanho
  fsyncado do `grupos.dict` numa seção TLV nova e IGNORÁVEL do manifest
  (0x0002), à parte da 0x0001, cujo decode de tamanho exato faria o binário
  anterior recusar o manifest. Na abertura, antes do `OpenMeasures`, um prefixo
  válido MENOR que o piso é dado durável perdido: o contador é reconstruído,
  com o log `counter.dict_below_durable_floor`, em vez de adotado com as
  entradas do vetor zeradas como órfãs. O piso também fecha o fsyncgate
  depois de um restart: a página que um EIO derrubou do disco, vista no page
  cache pelo processo seguinte, vira um piso maior que o real; depois do
  reboot o prefixo fica abaixo dele, e a abertura reconstrói.
- **R6 — chave viva duplicada** no dicionário é tratada como cauda rasgada (a
  abertura trunca ali), e não sobrescreve o mapa em silêncio.
- **F2/R8 — marcador de construção.** A reconstrução em operação zera o vetor
  in place num mmap compartilhado, e a construção na abertura só o completa no
  checkpoint do fim do `Open`. Um processo que morresse no meio deixava, com os
  mesmos parâmetros no manifest, um vetor zerado ou parcial que a reabertura
  adotava (`Covers` verdadeiro, `Born` falso): contagem baixa sem órfã, sem log
  e sem divergência. Defeito anterior a este bloco, confirmado vermelho antes da
  correção. O arquivo `building` no diretório do contador é gravado (fsync do
  arquivo e do diretório) antes de zerar ou construir e removido só no fim do
  checkpoint bem-sucedido que adota; presente na abertura, força a
  reconstrução.
- **R7/F9 — overlay que devolve memória.** Depois de drenar tudo com sucesso,
  o `ApplyOverlay` troca o mapa por um novo quando o dreno passa de 65.536
  entradas E de 4 vezes a média móvel dos drenos anteriores (peso 1/4 para o
  dreno novo). O limiar: 65.536 entradas são ~1–2 MB de mapa, capacidade presa
  que não importa, e mantêm os gates de alocação do caminho quente longe da
  troca. A histerese: num regime que toca mais que o limiar por intervalo,
  trocar a cada checkpoint seria churn — a média alcança o regime em um ou dois
  checkpoints, e daí em diante só o pico (construção, reconstrução, ingestão
  fria) passa de 4 vezes ela. O `clear` do `Clear` continua sem encolher, e
  não precisa: a reconstrução enche o mapa de novo e o checkpoint seguinte o
  troca.

## Garantias e evidências

| Garantia | Provado por |
|---|---|
| O grupo novo não paga fsync; o checkpoint faz exatamente 1 fsync por dicionário sujo e 0 por limpo; a aposentadoria sincroniza e leva o pendente | `TestDictInternDefersTheSyncToTheCheckpoint`, `TestApplyOverlaySyncsTheDictOnceAndBeforeTheFirstSet` |
| O fsync do dicionário vem antes do primeiro `Set` do vetor (o dublê olha o vetor no instante do fsync) | `TestApplyOverlaySyncsTheDictOnceAndBeforeTheFirstSet` |
| No checkpoint, `syncDictContadores` antes de `applyOverlayContadores`, `msyncContadores`, `counterMeasures` e `manifest` | `TestCheckpointSyncsTheCounterDictBeforeTheVectorAndTheMeasures`, `TestMeasuresRecoverAtEveryCheckpointBoundary` (fronteira nova) |
| Crash entre o append e o checkpoint: reabertura sem erro, sem órfã, igual à reconstrução do zero, com aposentadoria intercalada e teto mudando os ids no replay | `TestCrashBeforeTheCheckpointLosesOnlyUnreferencedGroups`, `TestCrashBeforeTheCheckpointKeepsEveryCheckpointedGroup` |
| Crash entre o fsync do dicionário e o manifest, nas três fronteiras | `TestCrashBetweenTheDictSyncAndTheManifestIsRecovered` |
| Construção a frio sem fsync por grupo, interrompida antes do checkpoint do `Open`: reconstruída na reabertura | `TestColdBuildCrashBeforeItsCheckpointIsRebuiltOnReopen` |
| Abertura sincronizada; cauda herdada do page cache durável antes de o vetor a referenciar | `TestOpenDictSyncsTheTailItInherits`, `TestProcessCrashTailIsMadeDurableBeforeTheVectorReferencesIt` |
| `Close` sincroniza o append pendente | `TestDictCloseSyncsAPendingAppend` |
| Falha de fsync no checkpoint: `ErrDictSync`, nenhum `Set`, writer falho, manifest intacto | `TestDictSyncFailureIsReportedAsDictSync`, `TestApplyOverlayWithAFailedDictSyncWritesNothing`, `TestFailedCounterDictSyncFailsTheWriter` |
| Falha de fsync da aposentadoria: writer falho | `TestFailedRetirementSyncFailsTheWriter` |
| Piso: abaixo dele, reconstrução com log nomeado, sem tocar no arquivo; acima, adota e trunca a cauda; sem a seção, abre como antes | `TestOpenDictBelowTheFloorRefusesWithoutTruncating`, `TestCounterDictBelowTheDurableFloorIsRebuilt`, `TestCounterDictAboveTheDurableFloorIsAdopted`, `TestManifestWithoutTheFloorOpensAsBefore` |
| Seção 0x0002: ida e volta, bytes literais, ignorável, nome não declarado ignorado, nome repetido/seção duplicada/tamanho inconsistente recusados | `TestCounterDictFloorRoundTrip`, `TestCounterDictFloorSectionGoldenBytes`, `TestCounterDictFloorSectionIsIgnorableByAnOlderBinary`, `TestCounterDictFloorOfAnUndeclaredCounterIsIgnored`, `TestCounterDictFloorSectionInconsistentIsRefused`, `TestCaudaSaiEmOrdemCrescenteDeTagComContadores` |
| Chave viva duplicada é cauda rasgada | `TestOpenDictTreatsADuplicateLiveKeyAsATornTail` |
| Marcador de construção: reconstrução em operação ou construção na abertura interrompidas são refeitas; adotada por checkpoint, o marcador sai | `TestOnlineRebuildInterruptedBeforeTheCheckpointIsRebuiltOnReopen`, `TestOnlineRebuildFollowedByACheckpointIsAdoptedOnReopen`, `TestOpenTimeBuildInterruptedBeforeItsCheckpointIsRebuiltOnReopen` |
| O dreno grande devolve o heap; abaixo do limiar o mapa é o mesmo; em regime acima do limiar não troca a cada checkpoint; falha no meio não troca | `TestOverlayDrainAfterABuildReturnsTheHeap`, `TestSmallOverlayDrainKeepsTheMap`, `TestOverlayMapIsReplacedOnlyByADrainFarAboveTheSteadyState`, `TestFailedOverlayDrainKeepsTheMap` |

O binário anterior lendo o manifest COM a seção 0x0002 foi conferido também
fora da suíte, contra o decode real da `v0.12.0` (`git archive v0.12.0` do
pacote `checkpoint`, decodificando um manifest gravado por este commit): abre,
e os dois contadores voltam inteiros, sem piso.

## As mutações: o vermelho e o verde de cada uma

Cada mutante foi aplicado no código de produção, a suíte do pacote rodou
vermelha com a mensagem abaixo, o mutante foi revertido e ela voltou verde
(conferido por `git status` antes de cada commit).

| Mutação aplicada | Reprovou em | O que a falha disse |
|---|---|---|
| Marcador ignorado na abertura | `TestOnlineRebuildInterruptedBeforeTheCheckpointIsRebuiltOnReopen`, `TestOpenTimeBuildInterruptedBeforeItsCheckpointIsRebuiltOnReopen`, `TestColdBuildCrashBeforeItsCheckpointIsRebuiltOnReopen` | "a reabertura reconstruiu 0 contadores"; `aut[A]: total 0 refs 0, quer 5 e 10` |
| Sem marcador antes do `Clear` da reconstrução em operação | `TestOnlineRebuildInterruptedBeforeTheCheckpointIsRebuiltOnReopen` | "tem de deixar o marcador até o checkpoint que a adota" |
| Marcador nunca removido | `TestOnlineRebuildFollowedByACheckpointIsAdoptedOnReopen` | "o checkpoint depois da reconstrução tem de remover o marcador" |
| Abertura sem fsync | `TestOpenDictSyncsTheTailItInherits`, `TestProcessCrashTailIsMadeDurableBeforeTheVectorReferencesIt` | "chamadas de durabilidade da abertura = [], quer [Sync]"; "3 entradas órfãs depois da queda de energia" |
| Abertura sincronizando o arquivo só com header | `TestOpenDictSyncsTheTailItInherits` | "o arquivo só com o header não tem o que sincronizar" |
| fsync de volta no `Intern` | `TestDictInternDefersTheSyncToTheCheckpoint`, `TestApplyOverlaySyncsTheDictOnceAndBeforeTheFirstSet` | "[WriteAt Sync WriteAt Sync WriteAt Sync], quer [WriteAt WriteAt WriteAt]" |
| fsync depois dos `Set`s | `TestApplyOverlaySyncsTheDictOnceAndBeforeTheFirstSet`, `TestApplyOverlayWithAFailedDictSyncWritesNothing` | "o id chegou ao vetor antes de ficar durável" |
| `ApplyOverlay` sem o fsync do dicionário | `TestCheckpointSyncsTheCounterDictBeforeTheVectorAndTheMeasures`, `TestCrashBeforeTheCheckpointKeepsEveryCheckpointedGroup`, `TestCrashBetweenTheDictSyncAndTheManifestIsRecovered` | "não passou por syncDictContadores"; "8 entradas órfãs na dobra da reabertura" |
| Erro do fsync sem `ErrDictSync` | `TestDictSyncFailureIsReportedAsDictSync`, `TestApplyOverlayWithAFailedDictSyncWritesNothing` | "Sync com fsync falho = dict sync: EIO simulado, quer ErrDictSync" |
| Checkpoint tratando `ErrDictSync` como retentável | `TestFailedCounterDictSyncFailsTheWriter` | "quer ErrWriterFailed com ErrDictSync na cadeia" |
| Aposentadoria com fsync falho sem `failWriter` | `TestFailedRetirementSyncFailsTheWriter` | idem |
| `Close` sem fsync | `TestDictCloseSyncsAPendingAppend` | "chamadas até o Close = [WriteAt], quer [WriteAt Sync]" |
| Chave viva duplicada aceita | `TestOpenDictTreatsADuplicateLiveKeyAsATornTail` | "limite 4 depois da chave duplicada; quer 2" |
| Piso ignorado | `TestOpenDictBelowTheFloorRefusesWithoutTruncating`, `TestCounterDictBelowTheDurableFloorIsRebuilt` | "OpenDict abaixo do piso = nil"; "reconstruiu 0 contadores" |
| Piso não gravado no manifest | `TestCounterDictBelowTheDurableFloorIsRebuilt` | "o manifest gravou o piso … DictValidBytes:0" |
| `delete` sem troca do mapa (o comportamento anterior) | `TestOverlayDrainAfterABuildReturnsTheHeap` | "o heap ficou 21766144 bytes acima do patamar anterior, de 21766144" |
| Troca sempre | `TestSmallOverlayDrainKeepsTheMap` | "o dreno de 1000 entradas (limiar 65536) trocou o mapa" |
| Troca sem histerese | `TestOverlayMapIsReplacedOnlyByADrainFarAboveTheSteadyState` | "rodada 2 de regime … trocou o mapa: a troca a cada checkpoint é churn" |

O cenário com aposentadoria intercalada
(`TestCrashBeforeTheCheckpointLosesOnlyUnreferencedGroups`) fica VERDE sob o
mutante "ApplyOverlay sem o fsync": o fsync próprio da aposentadoria leva
junto os grupos que o checkpoint deixou de sincronizar. É por isso que existe
o irmão sem aposentadoria (`TestCrashBeforeTheCheckpointKeepsEveryCheckpointedGroup`),
que reprova.

## Como o crash é simulado

Abandonar o Store em processo não perde nada: o page cache e o mmap
compartilhado sobrevivem à morte do processo. Os testes de crash truncam o
`grupos.dict` explicitamente no último tamanho que um fsync tornou durável,
medido do lado de fora — pelo `os.Stat` no seam do fsync do Store
(`SetCounterDictSyncHookForTest`) ou pelo dublê de arquivo no pacote do
contador —, e não pela contabilidade do próprio `Dict`. O seam
`PartialCheckpointWithCountersForTest` passa pelo mesmo `ApplyOverlay`, e o
fsync do dicionário vem junto: sem ele o seam produziria um estado que o
produto não alcança.

O oráculo dos crashes é por groupKey, nunca por id: o contador reaberto é igual
a uma reconstrução do zero sobre o estado final, a auditoria não acha
divergência nem nos grupos nem nos agregados, não há órfã, e as linhas sem
grupo internado são exatamente as que o teto recusou.

## A bancada, número a número

Na `.217` (Xeon Cascadelake, ext4, Linux 6.8, Go 1.26.5), com
`NISHIDB_BENCH_DIR` em disco real. Saída crua em
[`counter-dict-fsync-20261001.txt`](../benchmark/baselines/counter-dict-fsync-20261001.txt).

| Medida | Antes (fsync por grupo) | Depois |
|---|---|---|
| Grupo novo até o checkpoint que o torna durável, 2.000 grupos (`BenchmarkNewGroupUntilCheckpoint`) | **1,40–1,71 ms** (n = 3) | **4,4–9,2 µs** (n = 6) |
| Idem, 200.000 grupos | — | **4,6–5,2 µs** (n = 6) |
| Abertura que constrói um contador de 4.096 grupos, uma linha por grupo (`BenchmarkCounterColdBuild`) | **5,5–6,5 s** (n = 3) | **100–120 ms** (n = 6) |
| Abertura do mesmo dataset sem contador | 70–134 ms | 58–79 ms |
| Heap da construção de 600 mil linhas, e depois do checkpoint (`TestOverlayDrainAfterABuildReturnsTheHeap`) | o pico ficava (sob o mutante do `delete`, no notebook: +21,8 MB depois) | **+23,2–23,8 MB → +0–16 KB** (n = 3) |

Por grupo, a construção a frio caiu de ~1,37 ms para ~12 µs (a diferença entre
as duas aberturas dividida pelos 4.096 grupos), e o que sobra é a varredura, a
internação em memória e o texto do groupKey. Na carga do piloto — 7,07M de
linhas grandes e 777 mil grupos no `probe_spl1`, 47m53s de construção, ~22 min
de varredura —, os ~25 min de fsync somem e a construção fica dominada pela
varredura. Esta é a projeção a partir da bancada, NÃO uma medida no piloto.

Os fsyncs por checkpoint: exatamente um por contador com grupo novo desde o
checkpoint anterior, zero para os demais (provado pelo dublê, não medido em
produção).

## Veredito

Os dois defeitos da issue #88 estão fechados com prova por mutação, e o
defeito do marcador, anterior ao bloco, também. O débito 108 fecha com um
desenho diferente do que ele descrevia, registrado no próprio item.

## O que ficou como débito

- **129** — a construção a frio ainda passa todas as linhas pelo overlay: o
  PICO de memória da abertura continua (o mapa é devolvido depois do checkpoint).
- **130** — `RebuildCounters` com erro no meio deixa o contador parcial na
  sessão (a reabertura reconstrói, pelo marcador).
- **131** — `FieldDictValidBytes` é gravado e nunca conferido na abertura: o
  dicionário de campos não tem a paridade do piso.
- **132** — o `msync` do vetor dos contadores não deixa o writer falho, com uma
  justificativa ("derivado, reconstruível") que não vale para ele.

## O que NÃO foi verificado

- A medida no piloto: os números acima são da bancada na `.217`, e a projeção
  para a carga do `service_view` é aritmética, não rodada.
- O fsyncgate de verdade: os testes injetam a falha do fsync pelo seam; nenhum
  EIO de writeback real foi provocado.
- A troca do mapa sob o perfil real de checkpoints do piloto: a histerese foi
  provada com limiar reduzido em teste, não observada num regime de produção.
