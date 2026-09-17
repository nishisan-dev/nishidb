# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## O que é

NishiDB: engine de materialização e consulta para fluxos de eventos JSON parciais.
Recebe eventos (arquivo NDJSON ou Kafka), mantém o estado corrente de cada
entidade (merge por campo, controle de sequência) e responde SQL sobre esse
estado. A tensão que explica quase toda decisão de projeto: **8M de entidades em
menos de 1 GiB de RSS** — heap ~constante, com o peso morando em mmap. Binário Go
único, sem runtime externo (§97). Linux-only: o storage usa `mmap` e `fallocate`
reais (dataset só em ext4/xfs/btrfs; nunca NFS). Go 1.25+; o CI pina 1.26.5.

**Idioma é decidido por quem lê** (`docs/glossario.md` é normativo): prosa em
PT-BR — docs, godoc e comentários, mensagens de commit, mensagens de falha de
teste (`t.Errorf`); tudo que o compilador, o operador ou um cliente lê em
inglês — identificadores, pacotes e arquivos `.go`, nomes de teste
(`TestTopNReaproveitaOBufferDaEntradaDespejada` é o formato: sujeito, ação,
condição), mensagens de erro e de log, chaves de log, config, wire, métricas.
O glossário liga o termo PT-BR da prosa ao identificador inglês. Commits na
voz do time, por bloco lógico.

**A migração é incremental** (ciclo fechado em 11/09/2026, débito 116,
`docs/aceite-identificadores-em-ingles.md`): a base ainda tem identificadores,
nomes de teste, mensagens, chaves de log e contratos em PT-BR. Código novo
nasce em inglês. Identificador PT-BR existente é renomeado **quando a feature
o toca, no mesmo PR** — e o par velho → novo entra em
`planning/identificadores-em-ingles/mapeamento-2-manual.tsv` (é o que faz o
gate `TestRenamedIdentifiersAreNotCitedByTheirOldNames` e o
`scripts/renamecite` conhecerem o nome novo e cobrarem godoc, doc e diagrama).
Termo novo entra no glossário. Contratos (chave YAML, campo do wire, nome de
métrica, nome em disco, chave de log) só mudam com decisão explícita: depois
da primeira release têm compatibilidade a considerar. Não misturar os dois
idiomas num arquivo novo.

## Comandos

```
make build              # ÚNICA fonte do build: go build ./... (verificação) + ./nishidb com os
                        # ldflags do buildinfo. Nunca crie um segundo comando de build — release.yml,
                        # scripts/it-comum.sh e os gates dependem desta linha.
make test               # suíte completa, COM os gates de medida (allocs/vazão, //go:build !race).
                        # Rodar antes de commitar.
make test-race          # -race; os gates de medida viram SKIP. Complementar ao test — nenhum
                        # substitui o outro.
make test-integration   # go test -race -tags integration ./...
make build-deb          # empacota o .deb (depende de build; DEB_ARCH/DEB_VERSION
                        # controlam arquitetura/versão — cross-compila com
                        # GOARCH=arm64 CGO_ENABLED=0 DEB_ARCH=arm64 make build-deb)
make lint               # golangci-lint v2.12.2 pinado, tetos zerados
make bench-micro        # exige NISHIDB_BENCH_DIR apontando para DISCO REAL (fallocate de verdade,
                        # centenas de MiB por rodada; em tmpfs trava a máquina)
```

Teste único: `go test ./internal/<pacote>/ -run '<nome-do-teste>' -count=1`.

**Lint é ratchet bidirecional**: `.golangci-baseline` guarda a contagem e o CI
compara — acima **e abaixo** da baseline falham. PR que fecha apontamento abaixa
o número no mesmo PR. `make lint` local sai != 0 enquanto a baseline > 0 (é o
esperado, não é quebra). Subir versão do linter ou do Go = re-medir a baseline na
mesma passada. Nunca escrever a contagem corrente em comentário (diverge).
Com uma worktree em `.claude/worktrees/`, o linter local a varre junto e o
recorte por arquivo do relatório engana (239 dos 330 apontamentos vinham de lá,
e concluir daí que o CI veria 91 é errado — o total local bate com o do CI). Só
`git archive HEAD | tar -x -C <tmpdir>` e rodar o linter lá afere a contagem que
o CI vai medir.

**Clients** (`make test` NÃO roda os testes Java/Python — só os gates Go que
leem os manifestos):

```
make build-jdbc / test-jdbc-unit / test-jdbc     # JDK 21+; o IT roda contra o binário real
make test-python / test-python-it / lint-python  # python3 puro, ZERO pip install no test-python
```

Os ITs (`scripts/jdbc-it.sh`, `scripts/python-it.sh`) compartilham a montagem de
`scripts/it-comum.sh` (binário real com TLS + credencial + corpus com um campo de
cada tipo) — nunca duplicar. Eles setam `NISHIDB_IT_REQUIRED=1`, que transforma
skip de integração em falha; `NISHIDB_IT_KEEP=1` preserva o tmpdir para debug.

## Arquitetura

Fluxo: evento NDJSON → `internal/materializer` (merge/replace por key, TTL,
`frozenFrom`, `merge: sum`, tabela histórica) → `internal/storage` (segmentos
append-only + índices mmap) → `internal/query` (SQL v1) → saída NDJSON.

Pacotes que estruturam o todo:

- `internal/storage` — `Store` é a fachada única: segmentos, fielddict,
  checkpoint/manifest, recovery, compaction, expiração, retenção. `storage/mmap`
  é o ÚNICO pacote autorizado a usar mmap/unsafe; `fielddict` é append-only com
  teto de 65.535 campos. `storage/contador` é o contador mantido na escrita:
  vetor denso de 4 bytes por linha (`rowindex.Dense32`) + dicionário de grupos
  append-only + totais em heap, transicionado no passo 7c do `applyRecord`.
  Os agregados do contador inteiro (`linhas`, `total`) andam COLADOS aos
  vetores por grupo, dentro das mesmas guardas — nunca são somados na leitura,
  e a auditoria é o que os reconcilia.
- `internal/index` — `primary` (open addressing sobre mmap), `segindex` (índice
  secundário POR SEGMENTO, motor ÚNICO que substituiu os dois globais: por
  segmento a diferença entre hash e bitmap some, porque os dois são "valor →
  conjunto ordenado de rowIds" e densidade vira decisão de codificação, não de
  motor; duas formas, WIP mutável para o segmento corrente e selada imutável
  para os rotacionados), `valuekey` (dono ÚNICO da chave canônica
  `[tipo][payload]` — é formato de disco, com golden test de bytes).
- `internal/query` — lexer → parser → binder → planner → executor, mais
  `orcamento` (admissão de memória), `spill` (transbordo do ORDER BY) e `meta`
  (namespace `_meta` como Store virtual atrás do mesmo pipeline). Tabelas
  virtuais registradas por namespace (`Engine.RegisterVirtual`) correm pelo
  mesmo pipeline; `internal/counters` compila o bloco `counters:` contra o
  schema e serve `_counters.<nome>` e `_meta.counters` — é a cola entre
  schema/predicado e o storage, e por isso mora fora dos dois (o motor manda
  `_meta.fields` para o pacote `meta` e qualquer outra tabela do `_meta` para
  o registro de virtuais).
  `internal/predicate` é o avaliador compartilhado entre o WHERE e o `when:` das
  regras de TTL — os dois têm de ser o MESMO código. `internal/typeparse` é
  pacote folha usado pela ingestão E pelo binder: é o que impede as duas coerções
  de divergir.
- `internal/kafka` — poller → fila limitada (DOIS gates: registros e bytes) →
  writer único; uma goroutine de ponta a ponta preserva ordem por partição.
  Watchdog recria o consumidor só com a conjunção de três fatos (eventos parados
  + lag no broker + fila vazia), pelo MESMO `kafka.Open` da subida.
- `internal/metakafka` — a tabela virtual `_meta.kafka`. Mora fora de
  `internal/kafka` e de `internal/query` porque precisa dos dois, e nenhum dos
  dois pode importar o outro (mesmo argumento de `internal/counters`).
- `internal/server` + `internal/protocol` — protocolo remoto `/v1/*`.
  `protocolo` é pacote próprio porque tem quatro falantes (servidor, CLI remoto,
  JDBC, Python) que não podem redeclarar o formato.
- `internal/cli` — `cli.Run(args, Context)` devolve exit code sem `os.Exit`.
- `internal/gates` — testes que verificam o próprio ferramental (Makefile, CI,
  manifestos dos clients).
- `clients/jdbc` e `clients/python` — read-only, **zero dependência de runtime**;
  gates Go leem `pom.xml`/`pyproject.toml` como dado, então mexer nesses
  manifestos pode quebrar `go test ./...` do engine.

### Invariantes que não podem quebrar

- **flock do dataset é exclusivo por processo** — por isso servidor, ingestão e
  consulta vivem no mesmo processo. `nishidb query --config` com o servidor no ar
  sai com exit 3 (use `--server`); `nishidb offsets` exige o servidor parado.
- **Single-writer é mecanismo**: `wmu` serializa Put/Apenda/Sync/Checkpoint;
  buffers do caminho quente são reutilizados (`buf[:0]`, sempre reatribuindo o
  slice devolvido). `Materializer.Apply` não é thread-safe de propósito.
- **Ordem de durabilidade (§48)**: persist → checkpoint durável → commit do
  offset Kafka. O checkpoint local (`manifest.bin` + `kafka.offsets`) é
  autoritativo sobre o offset do group (§49).
- **`applyRecord` é o único code path de aplicação de estado** — Put e replay da
  recovery passam por ele. Índice/estado novo tem de ser alimentado ali e ser
  reconstruível a partir do payload do record (nunca do relógio).
- **Publicar a location é o commit de visibilidade** e vem por último; o
  índice por segmento faz SET antes do commit e NÃO há fase de CLEAR (a
  posting antiga vira falso positivo no segmento antigo e morre com ele, spec
  §5.2 do subsistema 1). Falso positivo é tolerado (o executor SEMPRE reavalia
  o WHERE completo, §44); falso negativo nunca.
- **Contador mantido na escrita é delta do PRÓPRIO vetor, nunca de
  `prevPayload`** (no replay ele não existe): os totais são dobra do vetor
  ANTES do replay, o vetor só vai ao mmap no `ApplyOverlay` do checkpoint
  (depois do `segSync`), e aposentar um grupo exige `refs == 0` (nunca
  `total == 0`) mais a cobertura da última mexida por um checkpoint. "Viva"
  para o contador é linha com location e sem lápide — não a visibilidade da
  consulta. `nishidb counters --config` exige o servidor parado; `--server`
  usa as rotas ADMIN.
- **Planner nunca falha**: prioridade fixa primary → índice → união de OR na
  raiz → full scan; toda dúvida degrada para FullScan. Não há desempate entre
  motores — com um motor de índice só, não existe "qual dos dois" a decidir, e
  a elegibilidade sai de um único `Indexed(field)`.
- **Zero alocação por linha** no scan/NDJSON é propriedade gateada
  (`testing.AllocsPerRun`); não existe buffer de rows (§66) e o encoder NDJSON é
  proibido de importar `fmt` e `encoding/json` (há teste).
- **Ordem de locks**: `vida` → `wmu` → `aposMu`; `aposMu` é sempre o mais
  interno — nada sob ele pode tomar `wmu` (deadlock).
- **stickyErr**: erro de durabilidade pós-Append deixa o writer FALHO até
  reabrir o dataset; retentativa "bem-sucedida" de fsync mentiria.
- **NULL é ausência nas três camadas**: `"campo": null` REMOVE o campo; campo
  ausente do patch preserva; `IS NULL` = ausente; ausente compara FALSE para
  qualquer operador (sem lógica trivalente).
- **Stale check só existe com `sequence:` declarada** no YAML do dataset.
- **Três baldes de erro (§101)**: dado ruim (pula e conta) ≠ falha de infra
  (para) ≠ writer falho (sticky). Bug e infra nunca entram no balde de dado ruim.
- **Tipos e índices são imutáveis** depois da primeira row; não há backfill nem
  migração — o remédio é recriar o dataset e reingerir (premissa de produto).
- **Config recusa na subida, nunca ignora em silêncio**: chave sem motor gera
  WARN nomeado; valor fora de faixa é recusado, não arredondado; ponteiros
  (`*bool` etc.) distinguem "ausente" (default) de "explícito".
- **Exit codes são contrato** (0–8, precedência `4 > 3 > 7 > 6 > 1`; tabela no
  README). Erro novo precisa de sentinela + `errors.Is` em `errorCode`, senão
  cai no default 1.
- **Rota HTTP nova entra por `rt.registra(padrao, papel, handler)`**
  (READ/ADMIN, fail-closed). Códigos de erro do wire são estáveis; código novo
  exige SQLSTATE no driver JDBC e exceção mapeada no client Python (gate cobra).
- **SQL v1**: SELECT/WHERE/LIKE/ORDER BY (até 16 chaves; operador bloqueante com
  três modos — completo, top-N ≤ 4.096, transbordo para `<dataset>/spill/`)/
  LIMIT/OFFSET. `GROUP BY`, `JOIN`, `HAVING`, `DISTINCT`, `UNION` são recusados
  com `UNSUPPORTED_CLAUSE` — premissa de produto, não bug.
- **Métricas**: nomes só pelas constantes de `internal/metrics/names.go`;
  `/v1/stats` e `/metrics` leem a MESMA estrutura (nunca duplicar contador);
  séries que podem alarmar nascem com zero na subida; label nunca vem de entrada
  do cliente.

### Doutrina dos gates

"Gate que passa com a ferramenta ausente não é gate, é carimbo" (débito 53):
ferramenta ausente FALHA alto no stderr nomeando o que instalar, nunca SKIP
silencioso. Nunca o mesmo número/versão em dois lugares — os gates comparam duas
EXTRAÇÕES (Makefile ↔ workflow ↔ pom), e config é lida como dado (YAML/XML/AST),
nunca por grep em prosa. Gates de medida são `//go:build !race` com contrapartes
que se anunciam como SKIP explícito sob `-race`.

### Doutrina de verificação

**Teste que passa com o defeito presente não prova nada.** Para toda invariante
nova, aplique a mutação, veja vermelho, reverta, veja verde — e relate os dois.
O ciclo do índice por segmento pagou vinte vezes por ignorar isso (golden
autocumprido, teto nunca exercitado, teste concorrente vacuamente concorrente,
bancada de métricas com 5 de 10 números zerados).

Quatro classes de defeito que nenhum compilador pega, e como este repo as fecha:

- **Texto normativo que mente** — godoc/comentário/doc afirmando barreira que o
  código não estabelece. **Abra a fonte antes de afirmar.**
- **Citação que não resolve** — `§NN` para seção inexistente. Gate:
  `TestCitacoesDeSecaoResolvem`.
- **Amostra de métrica sem `Family`** — `# HELP`/`# TYPE` sai com nome vazio e
  o Prometheus recusa o scrape inteiro. Gate:
  `TestAmostrasDeMetricaTemFamiliaENome`, que também recusa `Name` de outra
  família (a exceção é o sufixo de histograma).
- **Resíduo de mutação** — constante hardcoded ou lock deixado por agente de
  revisão. Confira `git status` antes de commitar.

O método completo está em `docs/doutrina-de-verificacao.md`. Os gates fecham a
classe barata (citar o que não existe); julgar se o teste prova o critério e se
o texto diz a verdade continua sendo revisão humana.

**O gate também é código, e cai na primeira classe.** Os dois nasceram cegos ao
que diziam cobrir — um só enxergava o literal com o tipo escrito e perdia 23 dos
58; o outro lia `.go` como bytes crus, contra a doutrina de ler código como
DADO. Gate de doutrina que não discrimina é pior que gate nenhum: mute o defeito
NA FORMA DOMINANTE do repositório, não só na que você escreveu no teste.

**Gate de `AllocsPerRun` só cobre o caminho que EXERCITA, e o mutante tem de
alocar de verdade.** As duas metades doeram no mesmo ciclo (colunas de janela na
linha do `emit:`). O gate da emissão media a passada SEM métrica nenhuma, e
passou verde com o nome da coluna sendo composto por grupo — 576 alocações por
passada sobre 64 grupos. Caminho novo pede gate novo, ao lado do velho, e não
confiança no velho. E o primeiro mutante escolhido para reprová-lo — concatenar
o nome da coluna com a vírgula e os dois-pontos por linha — passou: concatenação
curta que não escapa vai para o buffer de 32 bytes do stack do runtime, e não
aloca, então ele não provava nada sobre o gate. Use
um mutante que aloque inequivocamente (`fmt.Sprintf`), senão o verde prova que o
mutante é fraco, e não que o gate é forte.

**O inverso também vale: gate de alocação não prova "só roda na transição".**
No ciclo da view de alarmes, o godoc atribuía a guarda de uma leitura do payload
a "evitar alocação", e o gate de alocação se declarava o protetor dela. Mas a
leitura não alocava (`go build -gcflags=-m`: não escapa), e o mutante que a
chamava em toda passada passou verde. Antes de afirmar que algo aloca, meça;
e o que a guarda realmente protege (custo, valor congelado) pede teste que
observe ISSO.

**Estado publicado só em transição mascara teste de estado interno.** O retrato
dos alarmes só é republicado numa passada com alguma transição: um teste que
muda o valor de um grupo JÁ disparado e lê o retrato enxerga o retrato velho e
passa verde com o mutante que recaptura o valor a cada passada. O teste
precisa forçar uma transição em OUTRO grupo na mesma passada para expor o
estado do primeiro.

## Convenções de documentação

- **`§NN`** cita seção de spec, e o marcador serve a MAIS DE UM documento: a
  normativa `nishidb-spec-proposal.md` (raiz), os dois designs de M1 em
  `planning/` e a spec do subsistema 1 numeram as suas seções cada um por si —
  `store.go` cita §92 da raiz, §10.2 do design de storage e §2.4 do subsistema.
  O conjunto é FECHADO e declarado em `markerSpecs`
  (`internal/gates/verification_test.go`), com o preço do achatamento nomeado no
  godoc; documento novo que passe a ser citado entra na lista. **"débito NN"**
  cita `docs/debitos.md`, cuja numeração é ESTÁVEL: item novo entra no fim, nada
  é renumerado. Os comentários longos explicando o porquê são normativos — siga
  o estilo e cite §/débito quando aplicável.
- Docs mudam no MESMO commit que o código. `docs/architecture.md` só afirma
  número medido (com origem em `benchmark/baselines/` ou num `docs/aceite-*.md`);
  registros de rodada não são reescritos. `TestEvidenciasCitadasNosDocumentosExistem`
  confere que todo teste citado nos docs existe; `TestQuickstartDoREADMEExecuta`
  executa o quickstart do README.
- Planos aprovados ficam em `planning/`.
- Diagramas PlantUML em `docs/diagrams/`, incorporados via `uml.nishisan.dev`
  com URL raw fixando `main` (fixar branch de trabalho quebraria no merge);
  validar com `plantuml -checkonly`.

## Worktrees e agentes

Toda regra aqui foi aprendida quebrando. Para a sessão do agente, o worktree não
é só um recurso do git: é uma FRONTEIRA DE ISOLAMENTO. A sessão fica presa a um
worktree, e todo subagente dela fica preso ao mesmo worktree.

- **Worktree extra para subagente não funciona.** Um worktree criado com
  `git worktree add` para um subagente é inútil: escrever lá e rodar git lá são
  recusados. `EnterWorktree(path)` chamado pelo subagente responde sucesso e não
  libera nada.
  - Paralelismo DENTRO da sessão é feito por pacotes DISJUNTOS no mesmo worktree,
    sem commit.
  - O arquivo que todos tocariam (`docs/glossario.md`) fica proibido aos
    subagentes; os termos vêm listados no relatório.
  - Os aceites rodam primeiro por pacote, e cada falha fora do pacote é atribuída.
  - O orquestrador commita cada bloco com `git add` explícito.
  - Paralelismo em worktrees DISTINTOS é outra sessão, não subagente.
- **`isolation: "worktree"` parte de `origin/main`**, não do branch local. Só serve
  para trabalho que não depende de commit ainda não publicado (ex.: pacote novo).
  Para integrar: `git diff <base> <topo> --output=<patch>` e depois
  `git apply --3way <patch>`.
- **Entre no worktree ANTES de criar branch ou escrever arquivo.** Outras sessões
  trabalham na árvore principal ao mesmo tempo: um `git checkout -b` lá troca o
  branch debaixo delas, e um `git add` largo de outra sessão já engoliu arquivo
  não rastreado. O worktree nasce no branch `worktree-<nome>`: renomeie para a
  convenção com `git branch -m feature/<descricao>`.
- **A guarda recusa `git -C`, até para leitura, Bash composto com git e `$(git ...)`
  embutido.** Use um comando git por chamada; laço e script vão para um arquivo no
  scratchpad, executado como `bash <arquivo>`. Os objetos são compartilhados:
  `git show <sha>` de dentro do próprio worktree lê commit feito noutro.
- **A pilha de `stash` é compartilhada** entre worktrees e sessões. Nunca use
  `git stash` cru; prefira commit WIP.
- **O cache do golangci-lint guarda arquivos de worktree removido** e infla a
  contagem acima da baseline. Cura: `golangci-lint cache clean`.
- Ao fazer grep no repositório, exclua `.claude/worktrees/` (cópias da árvore).

## Ciclo com agentes: plano, spec e revisão

Estas lições vêm do ciclo das métricas de janela. O retrabalho mais caro ali veio
de premissa errada no PLANO, não de erro de código, e só apareceu na revisão do
código, que é o ponto mais caro para descobrir.

- **Refute o plano antes do primeiro Builder.** Todo teto, chave de estado e
  ciclo de vida que o plano fixa é conferido contra o código por um revisor cujo
  papel é derrubá-lo.
  - Exemplo: o plano limitou a capacidade de um arquivo indexado por id de grupo
    a `maxGroups + 1`. Mas o contador nunca reaproveita o id de um grupo
    aposentado, e o mapeamento inicial já dizia isso. O erro custou três rodadas
    de revisão e um redesenho.
- **Spec de bloco com estado persistente ou concorrente traz a lista de
  garantias**, uma a uma:
  - queda no meio da escrita (páginas diferentes chegam ao disco em momentos
    diferentes);
  - leitor concorrente;
  - ciclo de vida: criação, aposentadoria, reuso e reabertura;
  - falha de infra no meio da operação;
  - modo somente leitura.

  Contrato de API ambíguo vira escolha do Builder. No ciclo, a spec deixou em
  aberto quem define o dono de um balde gravado, e o resultado atribuía dado de
  um grupo a outro sem erro nenhum.
- **Toda garantia escrita em godoc tem teste que fica vermelho com a mutação
  dela, e a spec exige isso desde o começo.** A maioria das reprovações do ciclo
  foi "código certo, teste ausente". Quando essa cobrança só aparece na revisão,
  cada garantia vira uma rodada.
- **O Builder recebe o ambiente na PRIMEIRA mensagem, não aos poucos:**
  - o toolchain pinado no CI via `GOTOOLCHAIN` sempre que o Go local divergir;
  - as regras da seção anterior (worktree, pacotes disjuntos, arquivo
    compartilhado proibido);
  - comandos longos em foreground, com o resultado marcado PENDENTE se não
    couberem. Subagente que espera monitor ou tarefa em background para fechar o
    relatório fica parado até alguém cutucar.
- **Dimensione pela superfície, não pelo enunciado.** "avg/min/max numa janela",
  com heap limitado e estado que sobrevive a reinício, é um subsistema de
  storage: formato em disco, queda no meio da escrita, crescimento, leitura sem
  lock, restauração, órfãos e rotatividade. O número de blocos e de rodadas de
  revisão sai daí.

## Release

Release/tag é ato manual, mas o disparo é automático: um push de tag
`v*.*.*` no branch dispara `release.yml` — não há `gh release create` antes
da tag, é o workflow quem cria a release (rascunho primeiro, publicada só
depois do upload confirmado). O job `validate` recusa tag fora do formato
estrito antes de qualquer build; o job `anexa-binario` roda a suíte Go e o
teste de `scripts/release.py`, roda `make test-jdbc` (driver contra o
servidor real, antes de qualquer artefato de release), builda amd64 e arm64
(`make build`, NUNCA um `go build` próprio — sem os ldflags o
`scripts/nishidb-update.sh` dispara rollback espúrio no piloto) e empacota
o `.deb` de cada arquitetura (`make build-deb`, que depende de `make build`
pelo mesmo motivo — o binário que vai para `dist/` é copiado DEPOIS do
`build-deb`, para ser o mesmo que foi para dentro do `.deb`), builda o
driver JDBC, e por fim `scripts/release.py publish` cria/atualiza a release
com `--generate-notes` e sobe os cinco assets (dois binários, dois `.deb`,
o jar), mantendo as cinco últimas publicações sem apagar tag nenhuma.
