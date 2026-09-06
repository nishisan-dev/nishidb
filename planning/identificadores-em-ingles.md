# Identificadores em inglês — design do ciclo

> **Estado:** design aprovado em conversa em 05/09/2026. O plano de execução
> mora em `planning/identificadores-em-ingles-plano.md`. Branch
> `refactor/identificadores-em-ingles`, em worktree.

## 1. Objetivo e norma nova

O repositório nasceu inteiro em PT-BR: identificadores, nomes de teste,
comentários, logs, docs e mensagens de commit (CLAUDE.md, "O que é"). Para o
projeto evoluir com quem não lê português, a norma passa a ser dividida em
duas metades, e o critério que separa as duas é **quem lê**:

| Lê quem | Idioma | Exemplos |
|---|---|---|
| Desenvolvedor lendo prosa | **PT-BR** | docs, planning, godoc e comentários, mensagens de commit, mensagens de falha de teste (`t.Errorf`, `t.Fatalf`), texto dos débitos e aceites |
| Compilador, operador, usuário, cliente remoto | **inglês** | identificadores (tipos, funções, métodos, campos, parâmetros, locais, constantes), nomes de pacote e de arquivo `.go`, nomes de teste e subteste, mensagens de erro e de log, chaves e atributos de log estruturado, chaves de config, campos do wire, métricas, valores de CLI, nomes de arquivo em disco, código dos clients |

A regra tem uma consequência que o glossário resolve: a prosa em PT-BR fala de
"lápide", "fila" e "orçamento", e o código diz `tombstone`, `queue` e `budget`.
O `docs/glossario.md` é a ponte, e passa a ser normativo — comentário novo
cita o termo PT-BR do glossário, identificador novo usa o termo inglês do
glossário.

**Sem compat.** Nada foi lançado. Chaves de YAML, campo do wire, métricas,
valor de CLI, nomes públicos do client Python e nomes de arquivo em disco
mudam de nome direto, sem aceitar os dois por um ciclo. O dataset do piloto é
recriado no deploy (autorização permanente registrada; os dados são
descartáveis).

## 2. Escopo

**Dentro**, na ordem em que a conversa fechou:

1. Identificadores Go: exportados e privados, tipos, funções, métodos, campos,
   parâmetros, receptores quando tiverem nome PT-BR, variáveis locais,
   constantes, variáveis de pacote, rótulos. Inclui código de teste
   (helpers, dublês, fixtures) e os pacotes fora de `internal/` (`cmd/`,
   `benchmark/`, `scripts/`).
2. Pacotes e diretórios em PT-BR: `internal/contadores`, `internal/predicado`,
   `internal/protocolo`, `internal/query/orcamento`, `internal/storage/versoes`,
   `internal/storage/contador`. Nomes de arquivo `.go` em PT-BR
   (`expiracao.go`, `vigia.go`, `esteira.go`, ...).
3. Nomes de teste (`TestXxx`, `BenchmarkXxx`, `FuzzXxx`) e nomes de subteste
   (`t.Run("...")`), em fase própria e por último.
4. Emissões do produto: texto de `errors.New`/`fmt.Errorf`, mensagens e
   chaves de log estruturado, atributos de log, respostas de erro HTTP.
5. Resíduos PT-BR nos contratos: YAML (`expiracao`, `maxPorPassada`,
   `tempoPorPassada`), JSON (`detalhes`), seis métricas
   (`nishidb_compaction_segmentos_pinados`, `nishidb_index_segmentos`,
   `nishidb_index_reconstrucoes_total`, `nishidb_index_reconstrucao_ativa`,
   `nishidb_index_crc_falhas_total`,
   `nishidb_index_segmentos_consultados_total`), os VALORES de rótulo dessas
   séries (`estado` ∈ `selado|corrente|pendente|fora`, `modo` ∈
   `indice|enumeracao`, em `internal/metrics/nomes.go:430-439`), valores
   enumerados que saem pelo wire ou pela CLI em PT-BR (`"ausente"` em
   `types.Presence.String`, `"nenhum"` em `server/mutacoes.go:146`,
   `"corrente"` em `cli/operacao.go:269`), `--log-format texto`, e os quatro
   nomes em disco (`grupos.dict`, `grupos.idx`, `prazos.idx`,
   `vencimentos.idx`).
6. Clients: classes internas do JDBC (`LeitorNdjson`, `Molduras`,
   `Temporais`, `TiposJdbc`, `Transporte`, `Codigos`, `JsonTipo`) e do Python
   (módulos `conexao`, `documentos`, `erros`, `cliente`, `linha`, `leitor`,
   `tipos`, `transporte`; classes internas; e os nomes **públicos** em
   `__all__`: `ColunaMeta`, `Estatisticas`, `ConsultaEmVoo`, `Saude`,
   `ENV_SENHA`), sem alias.
7. Citações: todo identificador, nome de teste, chave de log e caminho de
   arquivo citado em `.md`, `.puml`, comentário, script, Makefile e workflow
   é atualizado pelo mapeamento. A prosa em volta não é reescrita.

**Fora**, e fica fora de propósito:

- Traduzir prosa: docs, planning, comentários, godoc, mensagens de commit,
  mensagens de falha de teste, texto de débitos e aceites.
- Renomear arquivos de documentação (`docs/*.md`, `docs/diagrams/*.puml`,
  `planning/*.md`). Dentro dos diagramas, só o que cita identificador muda.
- Qualquer mudança de comportamento. Um rename que precise de mudança de
  lógica para compilar é sinal de erro no mapeamento, não de oportunidade.
- Limpezas de passagem em código alheio. O que aparecer vira débito.

## 3. Terreno medido

Medido em 05/09/2026 sobre `main` em `b6fb817`, fora `.claude/` e
`.superpowers/`:

| Superfície | Tamanho |
|---|---|
| Arquivos `.go` / linhas | 566 / ~147 mil |
| Tipos exportados / privados | 269 / 215 |
| Funções exportadas / privadas | 160 / 1.200 |
| Métodos exportados / privados | 745 / 570 |
| Constantes e variáveis de pacote | 1.031 |
| Campos de struct | 2.197 |
| Funções de teste | 2.423, citadas 1.137 vezes em `.md` e 667 vezes em comentários `.go` |
| Identificadores distintos entre crases nos `.md` | 3.309 (8.948 ocorrências) |
| Pontos de emissão | 101 `errors.New`, 654 `fmt.Errorf`, 76 chamadas de log; 64 chaves de log distintas |
| Asserções de teste sobre texto de mensagem | 391 |
| Interfaces com métodos / métodos declarados | 28 / 89, com 633 ligações método×implementador (dublês de teste inclusos); `query.Store`, `executor.Store`, `planner.Store`, `meta.Capacidades` e `fakestorage.consumerStore` compartilham implementadores |
| Branches não mergeadas | 3, de um commit cada, 5–6 arquivos |

Nada de nome Go vaza para disco ou para o wire: o manifest e os offsets são
TLV próprio (sem `encoding/gob`), e toda struct serializada em `protocolo`,
`config`, `server`, `schema`, `cli` e `checkpoint` tem tag em todo campo
exportado. Os contratos já são majoritariamente em inglês: os 31 códigos de
erro do wire, os subcomandos e flags da CLI, as colunas das tabelas `_meta`.
O que sobra em PT-BR está listado no §2, item 5.

## 4. Mecanismo

Três ferramentas, cada uma provada em spike sobre o repositório real antes
deste design, mais uma passada de prosa. Nada é renomeado à mão.

### 4.1 `rf` em lote, por pacote (identificadores)

`rsc.io/rf` (commit de 10/07/2026) aplica um script de `mv` ao pacote do
diretório corrente e atualiza os importadores no módulo. Provado:

- renomeia função, tipo, método, campo, parâmetro (`Compila.spec`) e variável
  local (`Store.applyRecord.a`) por endereço, sem offset;
- atualiza a primeira palavra do godoc da declaração renomeada (regra
  `exported` do revive, que está ligada e conta na baseline);
- recusa conflito de escopo (`already have Store.applyRecord.s`) e recusa
  qualquer comando que deixe erro de tipo no pacote **ou nos importadores**,
  inclusive os `_test.go`;
- custo: ~1m40s na primeira carga (compila o módulo inteiro para export data)
  e ~2,3 s por invocação quente, independente de quantos `mv` o script tem.

Limites que este design contorna:

- **`rf` checa tipos após cada comando, não ao fim do script**, e não tem o
  `-f` que a documentação descreve. Renomear um método que satisfaz interface
  em outro pacote falha no primeiro `mv`. Ver §4.2.
- Não move pacote. Ver §4.3.
- Não toca menções em comentários de outras declarações nem em `.md`. Ver
  §4.4.
- Endereço ambíguo (a mesma variável local declarada duas vezes na mesma
  função, em blocos diferentes) cai para `gopls rename` por posição.

### 4.2 Ferramenta própria para grupos de interface

Nem o `rf` nem o `gopls` propagam o rename de um método para as
implementações em outros pacotes: o `gopls` a partir da interface renomeou só
os dublês do mesmo pacote e deixou `storage.Store` e `fakestorage.Store`
quebrados, e recusou o passo seguinte "porque `server.go` tem erros". A
solução é uma ferramenta pequena, `scripts/ifacerename`, sobre
`golang.org/x/tools/go/packages` e `go/types`, com um único trabalho:

1. carrega o módulo inteiro com `Tests: true`;
2. para cada interface com métodos, calcula o **fecho**: os tipos concretos
   (valor ou ponteiro, inclusive dublês em `_test.go` e em `benchmark/`) que a
   implementam, via `types.Implements`, e as interfaces que embutem outras.
   O fecho é **transitivo**: `storage.Store.Lookup` satisfaz `query.Store`,
   `executor.Store`, `planner.Store` e `fakestorage.consumerStore` ao mesmo
   tempo, então renomear `Lookup` em uma delas arrasta as outras e todos os
   implementadores de todas. O grupo é a componente conexa do grafo
   (interface, método) ↔ (tipo concreto, método), e é assim que o TSV de
   grupos o descreve;
3. recebe o mapeamento `interface.Método → NovoNome` e renomeia, numa passada
   só, todos os `ast.Ident` cujo objeto (`Defs`, `Uses`, `Selections`) é o
   método da interface ou o método correspondente de qualquer tipo do fecho;
4. reescreve a primeira palavra do godoc de cada declaração tocada;
5. recusa o lote inteiro se algum tipo do fecho já tiver método ou campo com
   o nome novo, ou se um método do fecho for citado num mapeamento de `rf`;
6. termina com `go build ./... && go vet ./...` verde, senão desfaz.

É código do repositório e cai na doutrina: testes com mutação
(implementador de fora do fecho não renomeado → build vermelho; colisão no
method set → recusa; godoc que não começa pelo nome novo → vermelho no gate
de lint). Depois do ciclo a ferramenta fica em `scripts/`, como o
`identifier-len`, por ser útil em qualquer rename futuro de interface.

### 4.3 `gopls rename` mais `git mv` para pacotes

`gopls rename` sobre a cláusula `package` reescreve a cláusula, os import
paths e as referências qualificadas em todo o módulo; o diretório é movido
com `git mv` em seguida (o `gopls` em modo CLI tenta escrever no caminho novo
e não o cria). Provado nos seis pacotes-alvo do §2. Nomes de arquivo `.go`
são `git mv` puro, seguidos de `gofmt` para o nome de pacote não mudar.

### 4.4 Passada de prosa

Uma ferramenta de mapeamento (`scripts/renamecite`) lê as tabelas do §5 e
reescreve as citações em `.go` (comentários), `.md`, `.puml`, `.sh`, `.py`,
`Makefile`, `.github/workflows/*.yml` e `CLAUDE.md`. Regras estreitas, e cada
uma é testada com a contraparte que **não** pode ser tocada:

| Substitui | Não substitui |
|---|---|
| token entre crases que casa exatamente com um identificador do mapeamento, com ou sem qualificador (`` `Compila` ``, `` `contadores.Compila` ``, `` `Store.Apenda` ``) | palavra solta na prosa ("a fila limitada", "orçamento de memória") |
| doc link `[Ident]`, `[pkg.Ident]`, `[T.M]` | texto entre crases que não está no mapeamento (chave YAML, SQL, nome de métrica) |
| `pkg.Ident` qualificado em comentário `.go` | `Ident` não qualificado em comentário `.go` fora de crase, exceto `TestXxx` |
| `\bTestXxx\b` e `test_xxx` do mapeamento de testes, em qualquer arquivo | nomes de teste que não estão no mapeamento |
| caminho de arquivo ou pacote renomeado (`internal/protocolo/jdbc.go`) | prosa que descreve o pacote ("o pacote de protocolo") |
| chave de log citada entre crases (`` `consumer.subiu` ``) | a mesma chave fora de crase |

Menção não qualificada de identificador em prosa ("o Aposenta libera...")
fica, e é exatamente o caso que o glossário cobre. A ferramenta emite um
relatório do que ficou de fora por cair nas colunas da direita, e o
relatório é revisado por agente antes do commit.

### 4.5 Emissões e contratos são edição, não rename

Mensagens de erro e de log, chaves de log e os resíduos dos contratos não
têm identidade para uma ferramenta seguir: são edição de texto por agente,
por pacote, com as asserções de teste (`strings.Contains(err.Error(), ...)`)
atualizadas no mesmo commit, e com os documentos e scripts que citam a
chave ou o nome (`scripts/it-comum.sh` grepa `inválid`; `docs/metricas.md`,
`docs/operacao-kafka.md`, `README.md` e `docs/architecture.md` citam chaves
de log; o client Python lê `detalhes`) tratados pela passada de prosa quando
citação, e por edição quando código.

### 4.6 Ordem de aplicação dentro de uma fase de código

1. `scripts/ifacerename` no módulo inteiro, com o mapeamento de grupos de
   interface da fase. Build e vet verdes.
2. `rf` por pacote, em ordem topológica (folhas primeiro, §7 Fase 2), um
   script por pacote, `-diff` guardado como artefato de revisão antes de
   aplicar. Build e vet verdes após cada pacote.
3. Passada de prosa com os mapeamentos da fase.
4. `make test`, `make test-race`, `make lint`, gates.

Se o `rf` recusar um `mv` porque um método satisfaz interface, o mapeamento
está errado (o método pertencia ao passo 1) e volta ao juiz. Essa recusa é
um gate gratuito, não um obstáculo.

## 5. Artefatos versionados

| Artefato | Papel |
|---|---|
| `docs/glossario.md` | normativo; termo PT-BR → identificador inglês; regras de estilo Go; escrito na Fase 0 e alimentado por toda fase |
| `planning/identificadores-em-ingles/mapeamento-<fase>-<pacote>.tsv` | o que os agentes propuseram, o juiz consolidou, o `rf` aplicou; colunas: `endereco_rf`, `novo`, `especie` (type/func/method/field/param/local/const/var), `arquivo`, `motivo` quando o nome fugir do glossário |
| `planning/identificadores-em-ingles/grupos-de-interface-<fase>.tsv` | entrada do `scripts/ifacerename`: `pacote`, `interface`, `metodo`, `novo`, `fecho` |
| `planning/identificadores-em-ingles/mapeamento-pacotes.tsv` e `mapeamento-arquivos.tsv` | Fase 1 |
| `planning/identificadores-em-ingles/mapeamento-chaves-de-log.tsv`, `mapeamento-contratos.tsv` | Fase 3; são o que a passada de prosa consome para docs |
| `planning/identificadores-em-ingles/mapeamento-testes.tsv` | Fase 4; `velho`, `novo`, `arquivo`, mais os subtestes |
| `planning/identificadores-em-ingles/mapeamento-clients.tsv` | Fase 5 |
| `docs/aceite-identificadores-em-ingles.md` | fechamento: números, mutações aplicadas e relatadas, o que ficou de fora e por quê |

Os mapeamentos ficam no repositório depois do ciclo: são a única forma de um
leitor de `aceite-m1.md` ou de um commit antigo chegar ao nome atual.

## 6. Regras de tradução

**Estilo Go, sem exceção:** MixedCaps; iniciais em maiúsculas (`ID`, `URL`,
`HTTP`, `JSON`, `TLS`, `CRC`, `TTL`, `SQL`, `NDJSON`, `CLI`, `RSS`); nada de
prefixo `Get`; receptores de uma ou duas letras não mudam; sentinelas
`ErrXxx`, tipos de erro `XxxError`; construtores `New`/`Open`. O revive
(`exported`, `var-naming`) é quem cobra, e a baseline é re-medida no
fechamento de cada fase.

**O que já está em inglês não muda.** `Store`, `Put`, `Open`, `Apply`,
`ErrDatasetLocked`, `FullScanPlan` ficam. Nome híbrido traduz só a metade
PT-BR: `ErrOrcamentoExcedido` → `ErrBudgetExceeded`.

**Glossário de domínio**, semente que a Fase 0 fecha (o juiz pode ampliar,
nunca contradizer):

| PT-BR | inglês | PT-BR | inglês |
|---|---|---|---|
| lápide | tombstone | fila | queue |
| apenda / apendar | append | esteira | pipeline |
| aposentar / aposentadoria | retire / retirement | vigia | watchdog |
| orçamento | budget | instantâneo | snapshot |
| transbordo | spill | janela | window |
| congelada / congelar | frozen / freeze | passada | sweep |
| prazo | deadline | vencimento | expiry |
| agenda | schedule | moldura | frame |
| em voo | in-flight | contador | counter |
| grupo | group | linha | row |
| campo | field | chave | key |
| dicionário | dict | segmento | segment |
| reconstrução | rebuild | descartado | discarded |
| recusa / recusar | refuse | falho (writer) | failed |
| ingestão | ingestion | consulta | query |
| materialização | materialization | histórico | history |
| retenção | retention | expiração | expiry |
| autenticação | auth | credencial | credential |
| parâmetro | param | operação | operation |
| mutação | mutation | evidência | evidence |
| citação | citation | gate | gate |

**Nomes de teste** preservam a frase: sujeito, ação, condição.
`TestTopNReaproveitaOBufferDaEntradaDespejada` →
`TestTopNReusesTheBufferOfTheEvictedEntry`. Sem limite de comprimento; a
frase importa mais que a brevidade, e é isso que o repositório sempre fez.
Subtestes seguem a mesma regra em minúsculas.

**Chaves de log** ficam `domínio.evento` em snake_case inglês:
`consumer.subiu` → `consumer.up`, `particoes.atribuidas` →
`partitions.assigned`, `ingestao.sem_progresso` → `ingestion.stalled`. Os
atributos já são em inglês; o único em PT-BR fica listado no mapeamento.

**Mensagens** ficam em inglês, minúsculas, com o prefixo de pacote e o `%w`
preservados: `storage: writer em modo falho (reabra o dataset)` →
`storage: writer in failed mode (reopen the dataset)`. Sentinelas, códigos de
exit e códigos do wire não mudam de valor.

**Contratos**, proposta inicial que o glossário fecha na Fase 0:

| Onde | De | Para |
|---|---|---|
| YAML | `expiracao`, `maxPorPassada`, `tempoPorPassada` | `expiry`, `maxPerSweep`, `timePerSweep` |
| JSON | `detalhes` | `details` |
| métricas | `..._segmentos_pinados`, `..._index_segmentos`, `..._reconstrucoes_total`, `..._reconstrucao_ativa`, `..._crc_falhas_total`, `..._segmentos_consultados_total` | `..._pinned_segments`, `..._index_segments`, `..._rebuilds_total`, `..._rebuild_active`, `..._crc_failures_total`, `..._segments_scanned_total` |
| CLI | `--log-format texto` | `--log-format text` |
| disco | `grupos.dict`, `grupos.idx`, `prazos.idx`, `vencimentos.idx` | `groups.dict`, `groups.idx`, `deadlines.idx`, `expiries.idx` |
| Python público | `ColunaMeta`, `Estatisticas`, `ConsultaEmVoo`, `Saude`, `ENV_SENHA` | `ColumnMeta`, `Stats`, `InFlightQuery`, `Health`, `ENV_PASSWORD` |

**Colisão** resolve por nome mais específico (`Fila` da kafka e `Fila` do
executor não viram os dois `Queue` se estiverem no mesmo pacote), nunca por
sufixo numérico.

## 7. Fases

Cada fase termina verde por si (`make build`, `make test`, `make test-race`,
`make lint` com baseline igualada, gates) e é mergeável em `main`. As três
branches abertas rebaseiam depois de cada merge; branches de código novas
não nascem durante o ciclo.

### Fase 0 — Preparação

- `docs/glossario.md` com o §6 fechado e a regra "quem lê".
- CLAUDE.md: o parágrafo "Todo o repositório é em PT-BR" dá lugar à tabela
  do §1; os exemplos de teste citados nele são atualizados na Fase 4 pelo
  mapeamento.
- `scripts/renameinv`, `scripts/ifacerename` e `scripts/renamecite` com
  testes e mutações relatadas, mais `scripts/rename-apply.sh` (o aplicador,
  que exige `rf` e `gopls` e falha alto sem eles) com o gate correspondente.
- Inventário: `scripts/renameinv` lista, por `go/packages`, toda declaração
  (com o endereço que o `rf` entende), todo identificador local e todo grupo
  de interface do módulo, em TSV; é a entrada dos agentes das fases
  seguintes e a base de cálculo dos números do aceite.

### Fase 1 — Pacotes e arquivos

- Os seis pacotes do §2 via §4.3. Cada rename de pacote é um commit.
- Arquivos `.go` em PT-BR via `git mv`, um commit por pacote.
- A lista `arquivosDoIndicePorSegmento` do gate de citações, os caminhos
  citados em gates (`internal/protocolo/jdbc.go` em `jdbc_test.go`),
  Makefile, workflows, docs e CLAUDE.md pela passada de prosa.

### Fase 2 — Engine

Grupos de pacotes em ordem topológica, folhas primeiro; dentro de um grupo
os pacotes não se importam e os agentes propõem em paralelo:

| Grupo | Pacotes |
|---|---|
| G1 | `types`, `units`, `unsafex`, `typeparse`, `jsonp`, `logging`, `memprof`, `buildinfo`, `storage/mmap`, `storage/checkpoint`, `query/lexer`, `query/budget`, `testutil/radiusgen`, `gates` (helpers) |
| G2 | `index/valuekey`, `metrics`, `storage/encoding`, `storage/fielddict`, `index/primary`, `index/segindex`, `protocol`, `query/parser`, `query/spill`, `storage/rowindex`, `storage/segment`, `storage/versions` |
| G3 | `schema`, `config`, `testutil/fakestorage`, `query/binder`, `query/planner`, `storage/counter` |
| G4 | `kafka`, `predicate`, `materializer`, `query/executor`, `query`, `query/meta` |
| G5 | `metakafka`, `counters`, `storage` |
| G6 | `server`, `cli`, `cmd/nishidb`, `benchmark/*`, `scripts/*` |

Por grupo: §4.6 inteiro. Um commit por pacote (`storage: identificadores em
inglês`), mais um por grupo para a prosa. Inclui helpers e dublês dos
`_test.go`; exclui só os nomes `TestXxx`, que são da Fase 4.

### Fase 3 — Emissões e contratos

- Por pacote, na mesma ordem da Fase 2: mensagens de erro e de log, chaves e
  atributos de log, asserções de teste sobre texto.
- Contratos do §6, cada um com o consumidor no mesmo commit: YAML com
  `internal/config`/`internal/schema` e os exemplos em docs e README; JSON
  com `internal/protocolo` e o client Python (`documentos.py`); métricas com
  `internal/metrics/nomes.go`, `docs/metricas.md` (gate
  `TestTodaMetricaDeclaradaEstaDocumentada`) e `scripts/nishidb-stats.py`;
  CLI com `internal/cli/server.go` e o README; disco com
  `storage/rowindex`, `storage/store.go`, `storage/counter` e o
  `docs/architecture.md` (mapa de arquivos).
- `scripts/it-comum.sh` deixa de grepar `inválid`.
- Débito novo em `docs/debitos.md` registrando que o dataset do piloto foi
  recriado por causa dos nomes em disco, com a data.

### Fase 4 — Testes

- Mapeamento dos 2.423 nomes mais subtestes, proposto por pacote, julgado
  contra a regra de frase do §6.
- Aplicação por `rf` (função de teste é declaração de topo) e, para
  subtestes, pela passada de prosa sobre o literal de `t.Run`.
- Passada de prosa sobre `.md`, comentários, CLAUDE.md, `.puml`.
- `TestAsCitacoesDeTesteNosComentariosExistem` passa a cobrir o repositório
  inteiro: a lista `arquivosDoIndicePorSegmento` e o godoc que a justifica
  saem, as citações mortas herdadas são corrigidas no mesmo commit, e a
  mutação (citação morta plantada → vermelho) é relatada no aceite.

### Fase 5 — Clients

- JDBC: classes internas de `jdbc.wire` e `jdbc.json`, testes, e o gate
  `TestOsTiposNaoPesquisaveisDoDriverSaoOsNomesQueOGoProduz`, que lê
  `TiposJdbc.java` por caminho. `make test-jdbc` com o IT real.
- Python: módulos, classes internas e os nomes públicos, `__all__`,
  README do client, `docs/aceite-client-python.md` pelas citações,
  `test_xxx` renomeados e o gate de evidências. `make test-python` e
  `make test-python-it`.

### Fase 6 — Fechamento

- `docs/architecture.md`, diagramas `.puml` (participantes que citam
  identificador), `docs/doutrina-de-verificacao.md`, `docs/debitos.md`.
- Baseline do lint re-medida uma última vez; `.golangci-baseline` no mesmo
  commit.
- `docs/aceite-identificadores-em-ingles.md`: números antes e depois
  (identificadores PT-BR restantes = 0 por inventário, não por grep),
  mutações relatadas, tempo por fase, o que ficou de fora.
- Deploy no piloto com dataset recriado e dashboards atualizados para as
  métricas novas.

## 8. Verificação

A doutrina do repositório vale inteira: teste que passa com o defeito
presente não prova nada.

**Mecânica, por pacote e por fase:** build e vet após cada pacote; `make
test`, `make test-race`, `make lint`, `go test ./internal/gates/` por fase;
`make test-integration` nas fases que tocam `kafka`, `cli` e `server`; ITs
dos clients na Fase 5. O `-diff` do `rf` de cada pacote é guardado e lido
por agente antes de aplicar.

**Mutações obrigatórias, relatadas no aceite:**

| Onde | Mutação | Esperado |
|---|---|---|
| `scripts/ifacerename` | tirar um implementador do fecho | build vermelho e recusa do lote |
| `scripts/ifacerename` | nome novo já existente no method set | recusa antes de escrever |
| `scripts/renamecite` | comentário com "a fila" fora de crase | intocado |
| `scripts/renamecite` | `` `kafka.Fila` `` entre crases | reescrito |
| gate de citações estendido | citação de teste morta plantada | vermelho |
| gate de métricas | métrica renomeada só no código | vermelho até o doc |
| Fase 3 | mensagem traduzida sem a asserção | teste vermelho |

**Revisão adversarial por agentes**, sobre cada mapeamento antes de aplicar,
cada refutador com uma lente: sentido (tradução muda o significado do nome),
convenção Go (`Id`, `Url`, `Get`), colisão dentro do pacote e nos
importadores, termo fora do glossário, identificador já inglês alterado sem
motivo, grupo de interface incompleto. Um mapeamento só é aplicado quando
nenhum refutador o derruba; o que cai volta ao tradutor com o motivo.

**Inventário como prova de completude:** o TSV da Fase 0 é regenerado no
fechamento e comparado: nenhum identificador do inventário original pode
sobrar sem entrada em mapeamento ou sem justificativa (`já em inglês`).
Grep por acento não prova nada; o inventário é por `go/types`.

## 9. Execução com workflow de agentes

Por fase, um workflow com papéis fixos:

| Papel | Faz | Paralelismo |
|---|---|---|
| Cartógrafo | lê o inventário do pacote e devolve a lista de endereços `rf`, grupos de interface e citações | um por pacote, em paralelo |
| Tradutor | propõe `novo` para cada endereço, citando o glossário | um por pacote, em paralelo |
| Juiz | consolida contra o glossário, resolve colisões, separa grupos de interface, escreve os TSV | um por grupo de pacotes |
| Refutadores | tentam derrubar o TSV pelas lentes do §8 | três por TSV |
| Aplicador | roda `ifacerename`, `rf`, `renamecite`, build, vet, commit | **um só**, sequencial, no worktree |
| Verificador | `make test`, `make test-race`, `make lint`, gates; relata o vermelho com saída | um por fase |
| Crítico de completude | compara inventário antes e depois, procura o que ficou de fora | um por fase |

O loop repete até secar: refutação zerada e inventário fechado. Um worktree,
uma branch, commits por pacote. O aplicador é único porque `rf` e `gopls`
escrevem em arquivos de vários pacotes ao mesmo tempo e dois aplicadores
conflitariam no mesmo lote.

## 10. Riscos

| Risco | Resposta |
|---|---|
| Sessões paralelas na árvore principal e as 3 branches abertas | Ciclo em worktree; congelamento de branches de código durante o ciclo; merge por fase para a janela de conflito ser curta; as 3 branches rebaseiam após cada merge |
| `rf` é "rough" por declaração do autor | `-diff` guardado e revisado; atomicidade por comando com build entre pacotes; tudo reversível por git; `gopls` como fallback pontual |
| Prosa que mente após o rename | passada de prosa com regras estreitas e relatório do que ficou; gate de citações estendido ao repositório; glossário para o termo solto |
| Baseline do lint | re-medida e commitada no fechamento de cada fase, nos dois sentidos |
| Piloto | dataset recriado (nomes em disco, YAML), dashboards com métricas novas, greps de operação com chaves novas; tudo listado no aceite |
| Clients públicos mudam sem alias | decisão registrada no §1; README e aceite do client atualizados |
| Tradução que muda o sentido do nome | refutador de sentido; glossário; nomes de teste preservam a frase |
| Tamanho da tradução (2.423 testes, milhares de locais) | é custo de agente, não de risco; o inventário garante que nada sobra |

## 11. Decisões registradas na conversa de 05/09/2026

1. Testes entram, em fase própria por último.
2. Pacotes e arquivos entram.
3. Os dois clients entram, em fase separada.
4. Chaves de log, mensagens de erro e de log, e resíduos dos contratos
   entram.
5. Sem compat: nada foi lançado.
6. Parâmetros, campos e variáveis locais entram.
7. Os quatro arquivos em disco mudam de nome; o piloto é recriado.
8. Mensagens de falha de teste continuam em PT-BR.
9. Merge por fase.
10. Mecanismo: `rf` em lote, ferramenta própria só para grupos de interface,
    `gopls` mais `git mv` para pacotes, passada de prosa por mapeamento.
