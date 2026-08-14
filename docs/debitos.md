# Débitos abertos ao fim do M1

Varredura dos riscos do §14 do design de storage, dos riscos do design de
materializer/query e do que as revisões deixaram registrado — incluindo a
revisão de fechamento, cuja unidade foi a **fronteira entre camadas** e não a
tarefa. Cada item aberto tem **milestone dono**; nenhum depende de trabalho não
planejado.

Os itens **37 a 48** entraram na errata pós-fechamento: são coisas que estavam
abertas e não tinham registro nenhum — nem em documento, nem em comentário, nem
em teste. **45** e **46** vieram da segunda rodada da errata, que foi atrás
justamente do que a primeira deixou passar; **47** e **48** vieram da
verificação da documentação, que conferiu prosa contra código e achou duas
promessas que o binário não cumpre. A numeração é
**estável** (documentos e comentários citam "débito N"), então item novo entra
no fim da seção a que pertence, e nada é renumerado.

## Resolvidos durante o M1 (registro, para não voltarem)

| Item | O que era | Onde fechou |
|---|---|---|
| Storage §14.21 | `Close()` sem quiesce: fechar com operação em voo produzia **panic** e devolvia o lock com escrita em voo | I2 — `RWMutex` de vida + iterador reentrante + `Region.Close` zerando o tamanho. **A I2 fechou 3 das 4 rotas**: o iterador de bitmap continuou cru até o fecho do M1, onde virou `bitmapIterator` reentrante |
| R11 (materializer) | Campo `auto` perdia o tipo no restart e sumia do `Compiled` | QP + fix round — `typeHint` durável no `fields.dict` v2 e rota read-only no `Compile` |
| Formato do `fields.dict` | v2 não travado: mutação de layout produzia arquivo que o leitor canônico truncava | QP fix round — golden de bytes calculado do layout documentado |
| Política de três baldes (materializer) | Falha de I/O do dicionário contabilizada como "evento ruim"; pipeline pulava em silêncio | QP fix round — só o conflito de tipo é evento ruim |
| Política de três baldes (CLI) | Os dois baldes colapsados na saída do `ingest`: perda de dado saía com **exit 0**, e `INTERNAL_STATE` nem aparecia no resumo | Fecho do M1 — linha própria por balde e `ExitPerdaDeDados` (7) |
| Exit code de corrupção | `encoding.ErrCorrupt` caía no default e saía como **exit 1** ("erro de uso") | Fecho do M1 — mapeado para `ExitDataset` (3) |
| Retorno antecipado do `ingest` | Erro de leitura da entrada saía com **exit 1** e retornava ANTES do teste de perda: a perda apurada sumia do `$?`, e o mesmo valia para o writer falho, a falha de checkpoint e o sinal | Verificação da doc do M1 — ponto de saída único (`encerra`) e precedência por gravidade (`desfechoMaisGrave`, `4 > 3 > 7 > 6 > 1`); entrada interrompida virou perda (7) |
| FieldID não-determinístico | Datasets criados do mesmo YAML ficavam com ordem de coluna diferente em `SELECT *`, gravada para sempre | Fecho do M1 — `Open` ordena `BitmapFieldNames` antes de resolver |
| Dublê × real | O `fakestorage` aceitava key e record acima dos tetos que o Store real recusa | I1 — `ValidateRecord` no dublê + roteiro de comportamento compartilhado |
| Curto-circuito do AND | Desligá-lo fazia `A AND B` devolver o resultado de B, com a suíte verde | Reconciliação Q15/Q16 — diferencial árvore × programa |
| Teto de termos no WHERE | 1 M de termos aceitos, ~180 MB de AST retidos | Fix round Q13/Q14 — `parser.MaxWhereTerms` |
| Projeção repetida | `SELECT a, a` emitia chave repetida no objeto JSON e o consumidor colapsava as duas em silêncio | Decisão do fecho do M1 — recusada no binder, apontando a segunda ocorrência |
| Corpus fraco na medição | O gerador do LOAD era montado com `Config` literal parcial: zero patch, zero Extended JSON, zero null — enquanto o aceite citava 85% de patch | Fecho do M1 — `DefaultConfig()` no LOAD, perfil gravado no artefato e teste comparando com a referência |
| Documentação nunca executada | 4 defeitos no quickstart do README e 2 evidências citando teste que não provava o critério | Fecho do M1 — **parcialmente**: o quickstart do README é EXECUTADO por `TestQuickstartDoREADMEExecuta` (o YAML de config, o do dataset, o `ingest` e as duas queries, com a saída conferida linha a linha), e `TestEvidenciasCitadasNosDocumentosExistem` confere que todo **nome de teste** citado em `aceite-m1.md`, `architecture.md` e `debitos.md` existe na suíte. O verificador para EXATAMENTE aí: casa `func Test` e mais nada — não abre `.puml`, não resolve URL de diagrama, não confere número medido contra o JSON da rodada e não executa bloco de comando fora do README. Ver débito 41 |
| `kill -9` sem teste | O critério 25 era creditado por um teste de encerramento **limpo** | Fecho do M1 — `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado` mata o binário com SIGKILL de verdade |
| `--limit` cortava dentro de aspas duplas | A varredura textual que achava a cláusula LIMIT conhecia literal `'…'` e não identificador `"…"`: `--limit 2 'SELECT id, "no limit aqui" FROM t'` cortava no meio do identificador e saía erro de sintaxe | Errata do M1 — `ultimoLimit` passou a tokenizar com o `lexer` (o mesmo do `Execute`) e a pegar o último `TokenLimit`; some a segunda gramática, e com ela o escape por aspa duplicada e o boundary de identificador (`TestComLimitNaoCortaDentroDeLiteralNemDeIdentificador`, `TestComLimitNaoCortaQueryQueNaoTokeniza`, `TestFlagLimitNaoCortaDentroDeIdentificadorCitado`) |
| Vazão do CLI gateada por relógio, e depois por razão instável | O piso absoluto de 4.000 ev/s media hardware e instrumentação junto com o código. A troca por uma razão entre as duas pernas medidas **em sequência** não resolveu: a razão só é adimensional se a carga for a mesma nas duas janelas, e numa suíte com pacotes em paralelo ela não é — 0,30× a 1,62× em 12 execuções **sem mutação**, e um revisor pegou 3,27× (vermelho) em 1 de 6 | Errata do M1 — `TestVazaoDaIngestaoPeloCLI` passou a intercalar as duas pernas em 5 rodadas curtas e a decidir pela **mediana das razões pareadas**, com confirmação em segunda etapa. Ver débito 40 |
| 37 (errata) | **`query.defaultMaxRows` e `query.defaultTimeout` do `nishidb.yaml` eram parseados, validados pela metade e NUNCA lidos**: o teto e o timeout efetivos vinham só do YAML do dataset, os padrões coincidiam (100000 e 30s), e `defaultMaxRows` negativo passava em silêncio — a configuração mentia sem avisar | Errata do M1 — as duas viraram o **default global** do dataset que OMITE `query.maxRows`/`query.timeout`. O que era constante dentro do `validate` virou parâmetro (`schema.Defaults`), e `Config.DefaultsDeQuery()` é a costura que alimenta `schema.Load`; o dataset que declara a chave continua vencendo, porque global é default e não teto. Valor não positivo é recusado no carregamento, com chave e valor no erro. Provas: `TestDefaultMaxRowsGlobalValeParaDatasetQueOmite`, `TestDatasetQueDeclaraMaxRowsVenceOGlobal` e `TestDefaultMaxRowsNaoPositivoEhRecusado` (fim a fim pelo CLI), `TestDatasetSemQueryHerdaOsDefaultsGlobais` e `TestDefaultsGlobaisInvalidosSaoRecusados` (`internal/schema`), `TestDefaultsDeQueryCarregaAsChavesGlobais` (a costura). O **timeout** não ganhou teste de relógio de propósito: a cadeia fecha pela costura mais o `TestTimeoutDoDataset` (`internal/query`), que usa atraso injetado em vez de esperar deadline. Segue aberto o `query.resultBatchSize`, que é o débito 10 |
| Gate duro pulado contava como aprovado | No harness de benchmark, um gate com `hard: true` que não podia ser AVALIADO saía do artefato com `"skipped": true` **e** `"passed": true`, e não reprovava a rodada — os `aceite-1m-*.json` e `ingest-1m-*.json` em `benchmark/baselines` estão gravados assim, com a rodada inteira `"passed": true`. Três textos afirmavam o contrário (o comentário do `evaluateGates`, o do teste do §110 e o plano em `planning/`) | Errata do M1 — o veredito de gate virou **um campo com três estados** (`GateStatus`: `pass`/`fail`/`skip`), e não dois booleanos: não é mais possível representar pulado E aprovado. A rodada só é aprovada quando **todo gate duro foi avaliado e passou**, o que faz a rodada de referência do §110 reprovar por construção — ela vale pelo report que produz, não pelo veredito. O resumo separa violado de NÃO AVALIADO, porque as ações do operador são diferentes. Provas: `TestGateDuroPuladoNaoEhGateAprovado`, `TestArtefatoNaoRepresentaGatePuladoComoAprovado` (lê o JSON e recusa até a presença do antigo booleano) e `TestResumoDeGatesSeparaVioladoDeNaoAvaliado`. Os JSON arquivados não foram reescritos: são registro de execução, e a ressalva está em `benchmark/baselines/aceite-8m-20260815.md` |
| Tabela de tipos do operador desalinhada do `typeconv` | A referência de tipos do `architecture.md` — a que o operador lê para escrever um YAML novo — dizia que `int64` aceita "número inteiro ou `$numberLong`" e que `double` aceita "número", quando os dois **coercem string** desde o ajuste pós-amostra; e dizia que `auto` infere "qualquer um dos acima", quando o conjunto inferido nunca inclui `ipv4`, `ipv6`, prefixo nem `bytes` | Errata do M1 — a tabela foi conferida **linha a linha** contra `internal/materializer/typeconv.go`, `internal/typeparse` e as regras de indexabilidade de `internal/schema/spec.go`, e não só nas duas linhas apontadas: entraram também o UTF-8 obrigatório do `string`, o fuso obrigatório e a fração truncada em millis do `timestamp`, a recusa de zero à esquerda no `ipv4`, a recusa de v4 puro e de zona no `ipv6`, os host bits preservados nos prefixos e a recusa de base64url no `bytes`. As colunas `index: bitmap` e `index: hash` foram conferidas contra `bitmapIndexable`/`hashIndexable` e já estavam corretas |

## Abertos, com dono

### M2 — consumidor Kafka — FECHADO

Entregue: consumer, bounded queue com os dois gates, offsets duráveis,
checkpoint, seek e recovery (§115); `nishidb server` com `/metrics` e
`/v1/health`; log estruturado; `nishidb offsets`. Os oito testes do §105
existem, e a rodada de aceite passou a medir com ingestão Kafka real.

Fechados nesta fase: **1** (consumo e recuperação de offsets), **2** (custo de
memória do consumidor no orçamento medido), **3** (carga mista de verdade — o
Kafka É a sobreposição), **4** (§95 logging estruturado), **5** (§112 pprof),
**9** (política `stop` e a chave `errors:`, que agora é parseada, validada e
lida pelos dois caminhos de ingestão), **10** (`query.resultBatchSize`), **44**
(`make test-integration` deixou de ser plumbing morto), **47** (`version:` do
`nishidb.yaml`) e **48** (`ActionReplaced` no resumo).

| # | Débito | Nota |
|---|---|---|
| 6 | **Oráculo do harness** | O `ingest-bench` mede latência e ausência de erro, e **não confere nenhum resultado**: um bug que fizesse toda query devolver 0 linhas passaria em todos os gates com p95 melhor. Também não valida a **presença** do conjunto de gates, nem gateia `row_count` |
| 7 | **3 rotas de leitura do harness descartam o erro** (`_, out, _, _ := st.GetByKey(...)`) | Um erro sistemático nelas produziria "0 erros" e latência melhor. Por isso a linha de zero erro do aceite fala de consultas **SQL** |
| 8 | **§14.20 — nenhum CRC verificado antes do `DurableOffset` no `Open`** | Corrupção em região selada só aparece na leitura, no meio do resultado |
| 46 | **A reescrita textual de SQL do `--limit` deve morrer** | O CLI corta a cláusula `LIMIT` do texto e anexa a sua, porque a v1 não tem serializador de AST. O corte já usa o lexer de verdade, mas continua sendo edição de texto: query que não tokeniza não é cortada, e a flag deixa de valer nela. O **binder já conhece o `LIMIT` da AST**: a saída definitiva é o `--limit` virar parâmetro do plano |
| 49 | **`errors.deadLetterTopic` é aceito no parse e recusado na ativação** | §102 não o torna bloqueante. A doutrina escolhida para o produto é uma só: chave que a spec nomeia é aceita e validada na FORMA pelo parse, e recusada na ATIVAÇÃO — recusar no parse tornaria inválido o snippet literal da spec, e aceitar em silêncio repetiria o defeito da issue #1. Implementá-lo puxa produção Kafka, que é escopo novo |
| 50 | **`storage.durability` continua aceita e ignorada** | O §47 define `mode: sync\|batch\|async` com `interval`, e nada disso é implementado — só o `SyncEveryPut`, que o CLI nunca liga. O risco é de LEITURA: quem lê `interval: 100ms` conclui "perco no máximo 100ms", enquanto no caminho Kafka a janela de reprocessamento é a do `storage.checkpoint.interval`. Não entrou no M2 porque o §115 não a lista |
| 51 | **A guarda de assignment total não distingue "instância rival" de "partição sem líder"** | O consumer recusa a subida quando o group não lhe entrega TODAS as partições do tópico (que é o que single-writer + checkpoint autoritativo exigem), mas a mensagem atribui a causa a outra instância. Um broker com partição sem líder produziria o mesmo sintoma com outra causa |
| 53 | **O `golangci-lint` NUNCA rodou neste projeto** | A configuração listava `gofmt` em `linters`, e na v2 ele é FORMATTER: o binário recusava o arquivo inteiro com `can't load config: gofmt is a formatter`. Como o alvo `lint` do Makefile cai para `go vet` quando o `golangci-lint` não está instalado, ninguém via o erro — o projeto passou o M1 achando que tinha lint. A configuração foi corrigida no M2 e o lint voltou a rodar; o **código do M2 está com 0 apontamentos**, e sobraram **114 no código do M1** (50 errcheck, 50 revive, 12 staticcheck, 1 gofmt, 1 unused — errcheck e revive estão no teto de 50 por linter, então o total real é maior). Não foram corrigidos aqui porque varrer o M1 inteiro é trabalho de outra unidade, e misturá-lo ao M2 tornaria o diff do consumer impossível de revisar |
| 52 | **O lag do relatório de benchmark commita o que foi POLLADO** | A bancada não é o motor: ela commita antes de perguntar o lag, senão o número seria sempre o total produzido. O consumer de produção só commita depois do checkpoint durável (§48). A diferença é de alguns registros e irrelevante para a ordem de grandeza do §125, mas está registrada para que ninguém compare os dois números como se fossem a mesma coisa |

### M3 — protocolo e driver

| # | Débito | Nota |
|---|---|---|
| 11 | Protocolo HTTP `POST /v1/query` (§126.17) | O NDJSON e o `StreamWriter` já são o formato; falta o transporte |
| 12 | Driver JDBC (§126.18) e `PreparedStatement` real (§126.19) | Parâmetros posicionais já funcionam pela API Go; o CLI não tem como fornecê-los |
| 13 | **Metadados de tipo fora de banda** | O NDJSON **não carrega tipos**, e o `ResultSetMetaData` precisa deles — inclusive das colunas que são NULL em todas as linhas, que não dão para derivar do dado. A fonte é `binder.BoundQuery.Columns` |
| 14 | Autenticação (§126.20) e TLS (§126.21) | Seções do YAML já são aceitas e ignoradas |
| 15 | **§80/§81 — `runtime.memoryTarget`, `query.maxConcurrent`, `query.maxBufferPerQuery`** e a prioridade sob pressão | A seção `runtime:` do YAML é aceita e **silenciosamente ignorada**. Com N conexões, o pior caso do débito 28 (o `StreamWriter` que nunca encolhe) é N × maior linha, sem teto |
| 16 | **§92-94 — métricas, `GET /metrics`, `GET /v1/health`** | 25 métricas nomeadas na spec; zero ocorrência de `prometheus` no código |
| 17 | **§96 — comandos `status`, `datasets`, `checkpoint`, `inspect`** | A spec pede 6 comandos operacionais; o M1 entrega 2, e nenhum dos 2 está na lista da spec. `compact` tem dono no M4; estes quatro são baratos (o `Store` já expõe `Stats()` e `Checkpoint()`) |
| 18 | **Acesso concorrente entre processos** | O `flock` é exclusivo: com um `ingest` em voo, um `query` sai com exit 3. A concorrência real chega com o servidor |

### M4 / Fases 8-9

| # | Débito | Nota |
|---|---|---|
| 19 | Índice **hash físico** (§126.15) | Planner e executor já implementam a rota e a testam contra o dublê; no real degrada para full scan |
| 20 | Expiração por **TTL** (§126.22) e tombstones por retenção (§126.23) | Declaração e bit já existem e são honrados na leitura |
| 21 | **Compaction** de segmentos (§126.24) | `minimumLiveRatio` já registrado no design |
| 22 | Backfill de índice bitmap (§14.17) | Hoje o conjunto de bitmaps é fixado na criação do dataset e **imutável nas duas direções** depois da primeira row: acrescentar um `index: bitmap` com `RowCount > 0` é recusado por falta de backfill, e removê-lo de um campo já indexado é recusado porque o índice reapareceria incompleto (`AllowBitmapDrop` existe no `Store`, mas o CLI não expõe flag). Os dois saem com **exit 3** num dataset íntegro. Sem backfill, mudar o conjunto exige reprocessar a fonte para um dataset novo — ver `architecture.md`, "Operar isto" |
| 23 | Índice derivado removido por fora não é detectado (§14.18) | Nenhuma validação cobre apagar o diretório de índice |
| 24 | **Tombstone fura o stale check do `_SEQ`** | `GetByKey` mascara row tombstoned como **ausente**, e o materializer usa esse `found` como guarda do §25: com `found == false` o stale check não roda, o evento segue pelo caminho de create e o `Put` reusa o rowId — a row **ressuscita**. Latente hoje (nada produz tombstone); vira real quando a Fase 9 ligar o TTL |
| 25 | **`Store.Sequence` não honra tombstone** | Valida só a location. Mesma família do 24 |
| 38 | **A liveness não converge depois de um crash** | Quando a row já existia no ponto S e o tail traz um update cuja location está **à frente** do cursor do replay, o record antigo continua contado como vivo. Medido: **2 records / 127 bytes onde o certo é 1 / 63** logo após o replay, e **2 / 128 onde o certo é 1 / 64** depois de um terceiro update com checkpoint completo — ou seja, **permanente**: o decremento do `applyRecord` identifica o garbage por `s.locs.Get(rowID)`, location que já aponta para o record do tail, então cada update seguinte desconta o anterior a ELE e nunca o órfão. É a direção segura (superestimar adia a compaction; subestimar faria a compaction descartar dado vivo). Fato registrado em `TestResiduoDeLivenessDoReplayNaoConverge`, no comentário de `store_recovery.go` e — porque é o contrato que o M4 vai ler — no godoc de `checkpoint.SegmentInfo`, que declara `LiveRecords`/`LiveBytes` como **limite superior**, não como contagem exata; quem reconstrói o tally por varredura é a compaction do débito 21 |
| 45 | **A posting de bitmap também não converge depois de um crash** — irmão do 38, mesmo ramo de código | O `replayRecord` roda `applyBitmapSets` com o prev desconhecido, o que liga as postings do payload novo e **não limpa nenhuma**: o bit do valor que a row tinha no ponto S fica ligado para sempre. Medido em `TestPostingOrfaDoReplayNaoConverge`: depois do replay a row aparece nas postings do valor antigo **e** do novo; depois de um terceiro update com checkpoint completo, a posting do valor intermediário é limpa (esse prev o replay conhecia) e a do valor de S **continua lá** — cada update limpa o prev que a location nomeia, que é o record do tail e nunca o de antes dele. **Não é problema de corretude, é de custo**: o índice devolve um SUPERSET e o executor revalida o predicado contra o payload corrente (§44), então nenhuma query responde errado — o que se paga é candidato a mais varrido. Falso NEGATIVO, esse sim, não ocorre, e é o que `exigeBitmapCompleto` guarda em todos os cenários de recovery. O cenário exige que as páginas do bitmap não tenham chegado ao disco (o §10.2 msynca as locations antes do manifest), que é a mesma montagem do `TestReplayRestauraPostingComBitmapAtrasDasLocations`. Quem fecha a conta é a **varredura** do M4 — compaction (débito 21) ou rebuild de índice (débito 22) —, o mesmo dono do 38 |

### Sem milestone — decisões registradas que podem virar trabalho

| # | Item | Quando reabrir |
|---|---|---|
| 26 | **Penhasco posicional do flatten** em 67 conjuncts: a mesma query com termos reordenados muda de `BitmapScan` para `FullScan` | Se aparecer query real com dezenas de conjuncts |
| 27 | **Grupo OR indexável dentro de AND** não vira união (a regra 5 vale só na raiz), e `key='a' OR key='b'` não vira união de point lookups | Otimização; ambos são superset correto hoje |
| 28 | **`StreamWriter` nunca encolhe**: uma linha de 1 MiB fixa a capacidade pela vida do writer | Não é curiosidade local: **é o `runtime.query.maxBufferPerQuery` do §80**, e vira teto por conexão no M3 (ver débito 15) |
| 29 | **Alarme falso do teto exato**: resultado com exatamente `maxRows` linhas reporta truncamento | Distinguir exigiria varrer o resto do dataset |
| 30 | **Divergência textual do `'g'`** contra o `encoding/json` em ~27% dos doubles (mesmo `float64`, grafia diferente) | Se algum cliente comparar a saída byte a byte com o encoder padrão |
| 31 | **Mensagens de erro de rede trocáveis** entre os parsers de ipv4/ipv6/prefixo | Dívida pré-existente registrada na QP |
| 32 | **`allocs/evento` do STEADY mistura ingestão e query** quando há leitores SQL | Separar exigiria contabilidade por goroutine |
| 33 | **Erro de escrita no stdout vira exit 1** (`WriteRow`/`Flush`): `query \| head -1` reporta "erro de configuração", e disco cheio no redirecionamento também | Junto com a tabela-verdade de exit codes do M2 |
| 34 | **`Stats` e `Sequence` engolem `ErrDatasetClosed`** e devolvem `Stats{}` / `(0,false)`: `RowCount: 0` num Store fechado é indistinguível de dataset vazio | Quando `Stats` virar superfície de observabilidade (débito 16) |
| 35 | **`FieldDictValidBytes`/`ValuesDictValidBytes` são gravados a cada checkpoint e ninguém os lê** | Campo de consistência sem verificador convida a próxima revisão a assumir uma checagem que não existe |
| 36 | **`ExitWriter` (4) não tem teste fim a fim** — é o único código do contrato sem cobertura, e o de semântica mais cara | Junto com o teste de ENOSPC no meio da ingestão |
| 39 | **Os diagramas do `architecture.md` não renderizavam: o repositório é privado.** O proxy do PlantUML busca o `.puml` anonimamente, e `raw.githubusercontent.com` responde 404 para repositório privado | **RESOLVIDO** — o repositório foi publicado em `git@github.com:nishisan-dev/nishidb.git` (privado) e as cinco URLs passaram a apontar para `git-raw.nishisan.dev`, que serve o conteúdo privado ao proxy. Os cinco respondem **HTTP 200 `image/png`**. As URLs fixam `main` de propósito. A sintaxe dos cinco fontes foi validada de duas formas independentes: `plantuml -checkonly -failfast2` local (exit 0 nos cinco) e o próprio `uml.nishisan.dev` pelo endpoint `/png/<texto codificado>`. Os dois validadores foram aferidos com um `.puml` propositalmente quebrado: o local sai com **exit 200**, o servidor responde **HTTP 400** com `X-PlantUML-Diagram-Error: Syntax Error?` |
| 40 | **O gate de vazão do CLI é relativo e só roda em `make test`**: `TestVazaoDaIngestaoPeloCLI` intercala 5 rodadas de 5.000 eventos entre o comando e o caminho interno, e cobra da mediana das razões pareadas no máximo **1,4×**. Fica **fora do build com `-race`** (`vazao_race_test.go` se anuncia como SKIP) | Calibrado na suíte inteira (`go test ./... -count=1`, 12 vCPU): sem mutação 22 execuções, mediana de 0,96× a 1,24×, zero vermelhos; `checkpointEventosPadrao` 500.000→500, 12 execuções, 1,78× a 2,01×, 12 vermelhos; bug de unidade em `duracao` (intervalo do YAML 1000× menor), 12 execuções, 1,88× a 2,16×, 12 vermelhos. **Sob `-race` a mesma mutação de checkpoint cai para 1,12×–1,30× (12 execuções) contra 0,96×–1,13× sem mutação (16 execuções)** — as populações se tocam e nenhum fator as separa, porque o detector infla o laço de ingestão e não infla o checkpoint. (Quanto ele infla varia com máquina, carga e versão do Go — medições nesta máquina foram de ~3,7× a ~5× conforme a condição —, por isso o que fica registrado é a sobreposição medida, não a razão.) O que o gate **não** pega, também medido: regressão que some menos de ~40% ao laço (checkpoint a cada 2.500 dá 1,26×–1,34×; `json.Unmarshal` por evento, 1,13×–1,40×), e regressão no storage, que derruba os dois lados na mesma proporção. Essa é a jurisdição de `benchmark/micro` e `benchmark/baselines`, e é o que precisa existir em CI para o flanco fechar |
| 41 | **O verificador de documentação só confere nome de teste.** `TestEvidenciasCitadasNosDocumentosExistem` casa `func Test` e mais nada | Não abre `.puml`, não resolve URL de diagrama, não executa bloco de comando fora do README (o quickstart é executado, o resto não) e não confere número medido — nem a data do rótulo — contra o JSON da rodada. **O flanco do `.puml` já cobrou:** a revisão dos cinco diagramas achou três caminhos de arquivo que não existem em disco (`dataset.lock`, `indexes/bitmaps/<campo>/`, `segments/*.dat`), a ordem SET/commit/CLEAR do write path invertida e o executor desenhado como push — nada disso quebra build, teste ou renderização. A checagem barata que fecharia a primeira classe é gramatical: extrair de cada `.puml` os literais com `/` ou extensão conhecida e exigir que existam como `filepath.Join` no código. Reabrir quando um documento voltar a citar o que não existe; o custo de cada checagem nova é baixo, o do silêncio não |
| 42 | **S13 em aberto**: property test com sequência aleatória de puts/gets contra oráculo `map` **com reopen aleatório**, e os benchmarks nomeados `BenchmarkPut`/`BenchmarkGetByKey` | Não é falta de cobertura, é granularidade: o caminho de escrita e o point lookup são medidos por `BenchmarkApplyIngestReal` e `BenchmarkQueryPointLookup` (ambos **0 allocs/op**, baseline em `benchmark/baselines/micro-20260813.txt`), e o crash real está em `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado`. O que falta mesmo é o **aleatório com reopen**, que é a única forma de pegar ordem de operações que ninguém pensou em escrever |
| 43 | **S15 em aberto**: `docs/architecture-storage.md` e o `c4_storage_component.puml` prometidos no design de storage | O `architecture.md` cobre a visão de sistema e o layout em disco; falta o detalhe por componente do storage (write path, recovery, C4 de componente) |
| 44 | **`make test-integration` é plumbing morto**: roda `go test -race -tags integration ./...` e **nenhum arquivo declara a tag** — é alias byte a byte de `make test-race` | Três saídas: dar conteúdo à tag (o crash test com subprocesso do S13 é o candidato natural), remover o alvo, ou deixá-lo documentado como está. Enquanto ninguém decide, o alvo promete uma suíte que não existe |

## Riscos de operação (não são débito de código, são de conhecimento)

Documentados em [`architecture.md`](architecture.md), seção "Operar isto":
`flock` inválido em NFS/FUSE/overlayfs (e o arquivo `LOCK`, que nunca é
removido), detecção de corrupção só na leitura, teto de 64 KiB em campo
indexado, **conjunto de bitmaps imutável depois da primeira row checkpointada,
nas duas direções** — acrescentar `index: bitmap` num dataset cujo manifest
registra `RowCount > 0` e
removê-lo de um campo já indexado são recusados igualmente, com **exit 3** num
dataset íntegro, e o tamanho dos valores não entra na decisão —, pausas do
rehash e do `msync`, e o phantom commit.

## O que NÃO ficou em aberto

Nenhum item da lista acima é dívida técnica no sentido de "atalho que vai
cobrar juros". Os resolvidos foram corrigidos com teste que prova a correção;
os abertos são **escopo de milestone futuro** ou **decisão registrada com a
razão**. A distinção importa: dívida esquecida vira surpresa, e decisão
registrada vira ponto de partida.
