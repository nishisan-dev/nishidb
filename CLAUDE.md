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

**Todo o repositório é em PT-BR** — código, identificadores, nomes de teste
(`TestTopNReaproveitaOBufferDaEntradaDespejada`), comentários, logs, docs e
mensagens de commit. Commits na voz do time, por bloco lógico.

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
  vetor denso de 4 bytes por linha (`rowindex.Denso32`) + dicionário de grupos
  append-only + totais em heap, transicionado no passo 7c do `applyRecord`.
  Os agregados do contador inteiro (`linhas`, `total`) andam COLADOS aos
  vetores por grupo, dentro das mesmas guardas — nunca são somados na leitura,
  e a auditoria é o que os reconcilia.
- `internal/index` — `primary` (open addressing sobre mmap), `bitmap` (baixa
  cardinalidade), `hash` (alta cardinalidade, remoção por lápide), `valuekey`
  (dono ÚNICO da chave canônica `[tipo][payload]` — é formato de disco, com
  golden test de bytes).
- `internal/query` — lexer → parser → binder → planner → executor, mais
  `orcamento` (admissão de memória), `spill` (transbordo do ORDER BY) e `meta`
  (namespace `_meta` como Store virtual atrás do mesmo pipeline). Tabelas
  virtuais registradas por namespace (`Engine.RegistraVirtual`) correm pelo
  mesmo pipeline; `internal/contadores` compila o bloco `counters:` contra o
  schema e serve `_counters.<nome>` e `_meta.counters` — é a cola entre
  schema/predicado e o storage, e por isso mora fora dos dois (o motor manda
  `_meta.fields` para o pacote `meta` e qualquer outra tabela do `_meta` para
  o registro de virtuais).
  `internal/predicado` é o avaliador compartilhado entre o WHERE e o `when:` das
  regras de TTL — os dois têm de ser o MESMO código. `internal/typeparse` é
  pacote folha usado pela ingestão E pelo binder: é o que impede as duas coerções
  de divergir.
- `internal/kafka` — poller → fila limitada (DOIS gates: registros e bytes) →
  writer único; uma goroutine de ponta a ponta preserva ordem por partição.
  Watchdog recria o consumidor só com a conjunção de três fatos (eventos parados
  + lag no broker + fila vazia), pelo MESMO `kafka.Abre` da subida.
- `internal/server` + `internal/protocolo` — protocolo remoto `/v1/*`.
  `protocolo` é pacote próprio porque tem quatro falantes (servidor, CLI remoto,
  JDBC, Python) que não podem redeclarar o formato.
- `internal/cli` — `cli.Run(args, Ambiente)` devolve exit code sem `os.Exit`.
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
- **Planner nunca falha**: prioridade fixa primary → hash → bitmap →
  interseção/união → full scan; toda dúvida degrada para FullScan.
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
  README). Erro novo precisa de sentinela + `errors.Is` em `codigoDoErro`, senão
  cai no default 1.
- **Rota HTTP nova entra por `rt.registra(padrao, papel, handler)`**
  (READ/ADMIN, fail-closed). Códigos de erro do wire são estáveis; código novo
  exige SQLSTATE no driver JDBC e exceção mapeada no client Python (gate cobra).
- **SQL v1**: SELECT/WHERE/LIKE/ORDER BY (até 16 chaves; operador bloqueante com
  três modos — completo, top-N ≤ 4.096, transbordo para `<dataset>/spill/`)/
  LIMIT/OFFSET. `GROUP BY`, `JOIN`, `HAVING`, `DISTINCT`, `UNION` são recusados
  com `UNSUPPORTED_CLAUSE` — premissa de produto, não bug.
- **Métricas**: nomes só pelas constantes de `internal/metrics/nomes.go`;
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
- **Amostra de métrica sem `Familia`** — `# HELP`/`# TYPE` sai com nome vazio e
  o Prometheus recusa o scrape inteiro. Gate:
  `TestAmostrasDeMetricaTemFamiliaENome`, que também recusa `Nome` de outra
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

## Convenções de documentação

- **`§NN`** cita seção de spec, e o marcador serve a MAIS DE UM documento: a
  normativa `nishidb-spec-proposal.md` (raiz), os dois designs de M1 em
  `planning/` e a spec do subsistema 1 numeram as suas seções cada um por si —
  `store.go` cita §92 da raiz, §10.2 do design de storage e §2.4 do subsistema.
  O conjunto é FECHADO e declarado em `specsDoMarcador`
  (`internal/gates/verificacao_test.go`), com o preço do achatamento nomeado no
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
- Ao fazer grep no repositório, excluir `.claude/worktrees/` (cópias da árvore).

## Release

Release/tag é ato manual: `gh release create` ANTES da tag. `release.yml`
dispara em `release: published`, roda `make build` (nunca um `go build` próprio —
sem os ldflags o `scripts/nishidb-update.sh` dispara rollback espúrio no piloto)
e anexa o binário.
