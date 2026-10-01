# Fsync do dicionário de grupos no checkpoint (débito 108) e overlay que devolve memória (#88)

Bloqueia produção da integração service-view → `service_state` (decisão do
usuário em 30/09/2026: medir primeiro; medido no piloto .218 — issue #88):
~1,95 ms de fsync por grupo novo, ~2,46M grupos ≈ 80 min de writer parado na
carga fria e na rampa de ingestão; e ~320 MB de heap presos por contador
construído, até o restart.

Base: branch `feature/counter-scope-rows` (PR #90 — par de 64 bits por grupo).

## Fatos que dimensionam (pesquisa de 30/09, com `path:linha` no relatório)

- O "desenho no plano" citado pelo débito 108 NÃO existe em lugar nenhum; o
  plano dos contadores fixa o OPOSTO (fsync por entrada, tarefa B1,
  `TestDictInternDefersTheSyncToTheCheckpoint`). `store_checkpoint.go:30-34` proíbe
  batching de dicionário (falando de fields/values.dict).
- Nenhum RECORD carrega id de grupo. Os únicos referenciadores DURÁVEIS do id
  são o vetor (`grupos.idx`, só via `ApplyOverlay` no checkpoint) e o snapshot
  das medidas (`SaveMeasures` no checkpoint). Alarmes, métricas de janela,
  emitter e auditoria referenciam a CHAVE, não o id, ou só em memória.
- O replay NÃO garante os mesmos ids (aposentadoria a cada minuto não é record;
  recusa pelo teto depende de `liveGroups`; rebuild online e construção na
  abertura internam por rowID). Logo a correção NÃO pode depender de igualdade
  de ids no replay.
- `over` (overlay do vetor) é lido/escrito só sob o `wmu`; não há leitor
  concorrente.

## Decisões

**R1 — Invariante que substitui o fsync por entrada:** *nenhum artefato
durável referencia id de grupo além do prefixo do `grupos.dict` já
fsyncado.* `Intern` passa a fazer só o append (WriteAt) e devolver o id; o
fsync do dicionário de cada contador com append pendente acontece no
checkpoint ANTES de `applyOverlayContadores` E ANTES de `counterMeasures`
(e portanto antes do manifest e do `sourceOffsets` — §48 intacto). O id em
memória antes do checkpoint é como a location no overlay: visível aos
leitores, não durável; um crash pode fazê-lo sumir e o replay re-internar a
chave (talvez com outro id) — correto porque nada durável apontava para ele.
Escreva isso no godoc de `Intern`, `Dict`, `Counter` e no passo do checkpoint.

**R2 — Registro de aposentadoria** continua com fsync próprio (raro, ≤ 1/min);
como o fsync é por arquivo, ele leva junto os appends pendentes — documente.

**R3 — Construção a frio na abertura** (`rebuildPendingCounters` →
`Transition` → `Intern`) deixa de pagar fsync por grupo pela mesma regra: o
contador recriado só é adotado no checkpoint do fim do `Open`, que faz o
fsync do dicionário antes do `ApplyOverlay`. Confirme no código e prove por
teste de crash no meio da construção.

**R4 — Falha de fsync do dicionário no checkpoint** = falha de durabilidade:
`failWriter` (stickyErr), nunca retentativa (fsyncgate: uma retentativa "bem
sucedida" deixaria buraco no meio do arquivo, a varredura pararia nele e os
grupos posteriores, já referenciados pelo vetor, sumiriam). Teste com dublê
de arquivo que falha o Sync.

**R5 — Piso de durabilidade no manifest (detecção de corrupção):** o
checkpoint grava, por contador, o tamanho fsyncado do `grupos.dict`
(`validBytes`) numa TAG NOVA IGNORÁVEL do manifest (não dentro da
`tagCounters`, cujo decode exige tamanho exato — o binário anterior recusaria
o manifest e o rollback quebraria; confira a convenção de bit crítico das
tags, ex.: `tagCounterMeasures = 0x8005` é crítica). Na abertura, ANTES do
`OpenMeasures`: se o prefixo varrido do dicionário for MENOR que `validBytes`,
o dicionário perdeu dado durável → o contador é RECONSTRUÍDO (recreate),
nunca adotado com órfãs zeradas em silêncio (o `Fold` zera órfã e o contador
passaria a contar menos sem erro). Prefixo maior que `validBytes` é normal
(appends depois do último checkpoint) e é mantido. Manifest antigo sem a tag
→ sem piso (comportamento de hoje).

**R6 — Endurecer `readRecords`:** chave VIVA duplicada no dicionário é tratada
como cauda rasgada (trunca ali), não sobrescreve `byKey` em silêncio
(`dict.go:186-193`). Teste com arquivo forjado.

**R7 — #88: overlay devolve memória.** Em `ApplyOverlay`, depois de drenar
TUDO com sucesso, se o mapa chegou a mais que um limiar (constante documentada,
ex.: 65.536 entradas — escolha e justifique), troca por um mapa novo
(`clear`/`delete` não encolhem o mapa do Go). Abaixo do limiar, nada muda
(os gates de alocação do caminho quente medem linhas já presentes no overlay e
NÃO podem passar a exercitar a troca). Falha parcial: não troca.

**R8 — Construção com `Clear()` in place sob manifest que adota o contador**
(`counters.go:~133-140` e `RebuildCounters` online): a pesquisa suspeita
(NÃO VERIFICADO) que um crash no meio deixa um vetor zerado/parcial que a
reabertura adota (`Covers` verdadeiro, `Born` falso). Escreva PRIMEIRO o teste
de crash no meio desses dois caminhos. Se ficar vermelho: corrija (marcador de
construção em curso no diretório do contador, removido só depois do checkpoint
que adota; ou recriar sempre por rename) — commit próprio. Se ficar verde,
registre a prova no aceite.

**Fora deste bloco (registrar como débito novo no FIM de `docs/debitos.md`):**
escrita direta no vetor durante a construção a frio (reduziria o PICO de
memória do overlay na abertura; exige o marcador do R8 e argumento de
durabilidade próprio).

## Garantias — teste que fica VERMELHO com a mutação

1. `Intern` não faz fsync (dublê de arquivo conta Syncs: 0 por grupo novo);
   o checkpoint faz exatamente 1 Sync por dicionário sujo e 0 por limpo.
   Mutante: Sync no Intern (o teste de custo) e checkpoint sem Sync (o teste
   de ordem abaixo).
2. ORDEM no checkpoint: Sync do dicionário antes do primeiro `Set` do vetor no
   `ApplyOverlay` e antes do `SaveMeasures`. Use o trace de passos do teste de
   fronteiras existente (`counter_measures_test.go:~204`) com passo novo.
   Mutante: mover o Sync para depois do `applyOverlayContadores`.
3. Crash entre o append e o checkpoint (dicionário perde a cauda não
   fsyncada): reabertura sem erro, contadores = aplicação contínua, auditoria
   com zero divergência, nenhuma órfã. Inclua aposentadoria intercalada e grupo
   recusado pelo teto (os casos em que o replay muda ids).
4. Crash entre o Sync do dicionário e o manifest (cp parcial): idem.
5. R5: dicionário truncado ABAIXO do `validBytes` → contador reconstruído, com
   log nomeado; nunca adotado com órfãs. Mutante: ignorar o piso. Manifest sem
   a tag (binário anterior) abre como hoje. Binário anterior lendo manifest COM
   a tag: abre (tag ignorável) — prove com o decode do formato anterior.
6. R4: Sync do dicionário falha → writer falho (stickyErr), checkpoint não
   grava manifest.
7. R6: chave viva duplicada → truncada como cauda rasgada.
8. R7: após construir N linhas (N ≫ limiar) e checkpoint, o `HeapInuse` (ou o
   tamanho alocado do mapa medido de forma estável) volta perto do patamar
   anterior; mutante: o `delete` atual. Gates de alocação existentes continuam
   verdes; abaixo do limiar o mapa é o mesmo (mutante: trocar sempre → o gate
   de alocação ou um teste de identidade do mapa reprova).
9. R8: teste de crash no meio da construção in place (ver decisão).
10. Medida: o custo por grupo novo cai de ~ms para ~µs (bench com fsync real em
    `NISHIDB_BENCH_DIR`, rodado na .217 — disco real) e a construção a frio de
    N grupos fica dominada pela varredura. Número no aceite com origem.

## Textos normativos que mudam no mesmo bloco
`store_checkpoint.go:30-34` (estreitar para fields/values.dict e explicar a
assimetria: records carregam fieldIDs, não ids de grupo), godoc de `dict.go`
(`:~202-204`, `:~230-232`), `counter.go:~484-490` (argumento da colisão cita
"o Intern fsynca a chave"), `TestDictInternDefersTheSyncToTheCheckpoint` (vira o
teste da nova ordem — renomeie para inglês e registre o par no
`mapeamento-2-manual.tsv`), `docs/debitos.md` item 108 (marcar como resolvido
segundo a convenção do arquivo, sem renumerar), `docs/architecture.md` se
descrever o fsync por grupo, README se citar, aceite novo
`docs/aceite-fsync-do-dicionario-de-grupos.md`. Identificadores PT-BR que o
bloco TOCA são renomeados (pares no TSV); contratos (nomes em disco, chaves
de log) não mudam.

---

# Revisão 2 — decisões da refutação (NORMATIVAS; prevalecem sobre o texto acima)

F1. **Page cache sobrevive ao crash do PROCESSO.** Num kill -9/OOM/panic a cauda
suja do `grupos.dict` continua no page cache; a reabertura a lê como se
estivesse em disco, o replay resolve as chaves aos MESMOS ids, o dicionário não
tem append pendente e o checkpoint do `Open` não sincroniza — o vetor passa a
referenciar ids que não estão no disco; numa queda de energia depois, órfãs
zeradas em silêncio pelo `Fold`. Decisão: `OpenDict` faz `Sync` depois da
varredura/truncagem sempre que o arquivo tiver mais que o header (1 fsync por
contador por abertura), e `Dict.Close` sincroniza se sujo (cobre o rollback com
parada limpa para o binário anterior, que nunca sincroniza cauda herdada).
Garantia 1 vira "0 Sync por dicionário limpo DESDE A ABERTURA SINCRONIZADA".
Teste: crash em processo com cauda presente → reabrir → checkpoint → truncar o
`grupos.dict` no último tamanho SINCRONIZADO (registrado pelo dublê) → reabrir →
`Orphans == 0`. Mutante: abertura sem Sync.

F2. **O R8 é REAL e anterior a este bloco — vira decisão, commit PRÓPRIO e
PRIMEIRO.** (a) `RebuildCounters` (`counters.go:~561`) chama `c.Clear()` e
`Dense32.Clear` zera o mmap IN PLACE (`dense32.go:~73-81`); crash antes do
`msyncContadores` → reabertura vê `sameDefinition`, `Born=false`,
`Covers=true` e ADOTA páginas zeradas: contagem baixa em silêncio, sem órfã e
sem log. (b) No checkpoint do fim do `Open`, o `ApplyOverlay` percorre o mapa
em ordem aleatória e o primeiro `Set` alto CRESCE o arquivo; crash entre ele e
o fim do `msyncContadores` → `Covers=true`, `Born=false`, vetor parcial
adotado. Correção: marcador `building` no diretório do contador (fsync do
arquivo e do diretório), criado ANTES do `Clear`/construção, removido só depois
que o checkpoint que adota RETORNAR com sucesso (no caminho em operação, flag no
Store sob o `wmu`); marcador presente na abertura força `recreate=true`. Rename
sozinho NÃO resolve (manifest com os mesmos parâmetros adotaria o vetor novo
parcial). Testes: (a) vermelho determinístico em processo — abandonar depois do
`RebuildCounters` sem checkpoint e reabrir (MAP_SHARED expõe os zeros); (b)
ponto de injeção no `Counter.ApplyOverlay` depois de k `Set`s. Registrar como
débito novo (fora): `RebuildCounters` com erro no meio deixa o contador
zerado/parcial na sessão.

F3. **R4 estendido:** erro de fsync do registro de aposentadoria (`Retire`)
também vai para `failWriter` — hoje só para o laço (`counters.go:~612-616`,
`~666-673`), e depois de um EIO o próximo fsync do checkpoint "passa" com
entradas perdidas (fsyncgate por outra porta).

F4. **Oráculo das garantias 3/4:** comparar por GROUPKEY, nunca por id. Com teto
e aposentadoria, "= aplicação contínua" é falso (a aposentadoria é aplicada na
abertura antes do replay; grupos fantasma do page cache contam). Oráculo:
igualdade com uma reconstrução DO ZERO sobre o estado final, mais
`DivergentGroups == 0`, `DivergentAggregates == 0`, `Orphans == 0`, e
`NotInterned` == número de linhas cuja chave o dicionário final não tem.
Igualdade com a aplicação contínua só nos cenários sem teto.

F5. **Simular o crash:** abandonar o Store em processo NUNCA perde a cauda
(page cache/MAP_SHARED). As garantias 3/4 truncam o `grupos.dict`
explicitamente no último tamanho SINCRONIZADO. O seam
`PartialCheckpointWithCountersForTest` (`export_counters_test.go:~14-30`)
ganha o passo novo — senão produz estado impossível em produção.

F6. **Ordem ESTRUTURAL:** o `Sync` do dicionário é a PRIMEIRA coisa dentro de
`Counter.ApplyOverlay` (o único caminho que grava id no vetor), devolvendo
um erro sentinela que o checkpoint mapeia para `failWriter`; erro de `Set`
continua retentável como hoje. Nenhum chamador (seam, caminho futuro) consegue
gravar no vetor antes do Sync. Emitir `traceStep` próprio (o trace dispara
DEPOIS do passo, então prova a ordem entre passos; a ordem Sync→primeiro `Set`
dentro do `ApplyOverlay` se prova com dublê de arquivo/vetor). O Sync antes do
`SaveMeasures` fica garantido porque o `applyOverlayContadores` vem antes do
`counterMeasures` — teste de ordem cobre.

F7. **Mecânica do R5 (piso):**
- O piso é passado ao `counter.Open`/`OpenDict`; se o prefixo varrido `< piso`,
  NÃO truncar e devolver sentinela; o Store converte em `recreate=true`
  (rename para `.old`, preserva a evidência), ANTES do `OpenMeasures` (com
  reset a checagem de id é pulada; sem reset um id `>= Limit` derruba a
  abertura com `ErrDatasetCorrupt`).
- `validBytes` = `d.size` no instante do Sync, sob o `wmu`.
- Tag nova 0x0002 (ignorável; o v0.12.0 pula tag ignorável desconhecida —
  confirmado em `git show v0.12.0:internal/storage/checkpoint/manifest.go`):
  por contador `nome + u64`, decode de tamanho exato; nome que não está na
  `tagCounters` → entrada ignorada; nome repetido → manifest recusado como
  corrompido (escreva e teste). Goldens de ordem do TLV atualizados
  (`TestCaudaSaiEmOrdemCrescenteDeTagComContadores` e afins) com bytes literais.
- O aceite cita que o R5 também fecha o fsyncgate depois de restart (página
  que o EIO derrubou do disco, vista no page cache pelo processo novo →
  piso maior que o real → reboot → prefixo < piso → reconstrói).
- `FieldDictValidBytes` é gravado e NUNCA conferido na abertura: registre como
  débito novo (não há paridade).

F8. **Débito novo (fora deste bloco):** `msyncContadores` não é sticky
(`store_checkpoint.go:~138-147`) com a justificativa "derivado, reconstruível",
que é falsa — página perdida num EIO + msync "bem-sucedido" = vetor velho
adotado, nada dispara reconstrução.

F9. **Limiar do R7 com histerese:** se as linhas tocadas por intervalo de
checkpoint passam do limiar em regime, trocar o mapa a cada checkpoint é churn.
Troque só quando o pico drenado for muito maior que o regime (ex.: maior que
o limiar E maior que 4× a média móvel dos últimos drenos) — escolha, justifique
e teste as duas pontas (construção → troca; regime acima do limiar → não troca a
cada checkpoint). `Clear` usa `clear(c.over)` e não encolhe — irrelevante (a
reconstrução enche de novo), mas documente.

F10. **Textos normativos adicionais:** "ids nunca são reemitidos"
(`dict.go:~75-78`, `retirement.go:~31-32`) passa a valer só entre ids
DURÁVEIS (um id perdido no crash pode ser reemitido para outra chave; o id não
é exposto externamente); `dict.go:~58-60,~202,~230-232`,
`collision_test.go:~74`, `counter_test.go:~555`,
`docs/architecture.md:~2440-2441,~2606-2613`. `docs/aceite-contadores.md:~37`
cita `TestDictInternDefersTheSyncToTheCheckpoint`: descubra como o repositório
trata teste renomeado/removido citado em aceite antigo (registro de rodada não
se reescreve; veja `TestRenamedIdentifiersAreNotCitedByTheirOldNames`,
`scripts/renamecite` e o mapeamento) e siga o mecanismo existente; relate a
decisão. O débito 108 registra que o desenho entregue (fsync no checkpoint +
piso por `validBytes`) é DIFERENTE do texto original ("reconstrução quando o
vetor referenciar id além do dicionário").
