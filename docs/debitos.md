# Débitos abertos ao fim do M1

Varredura dos riscos do §14 do design de storage, dos riscos do design de
materializer/query e do que as revisões deixaram registrado — incluindo a
revisão de fechamento, cuja unidade foi a **fronteira entre camadas** e não a
tarefa. Cada item aberto tem **milestone dono**; nenhum depende de trabalho não
planejado.

Os itens **37 a 44** entraram na errata pós-fechamento: são coisas que estavam
abertas e não tinham registro nenhum — nem em documento, nem em comentário, nem
em teste. A numeração é **estável** (documentos e comentários citam "débito N"),
então item novo entra no fim da seção a que pertence, e nada é renumerado.

## Resolvidos durante o M1 (registro, para não voltarem)

| Item | O que era | Onde fechou |
|---|---|---|
| Storage §14.21 | `Close()` sem quiesce: fechar com operação em voo produzia **panic** e devolvia o lock com escrita em voo | I2 — `RWMutex` de vida + iterador reentrante + `Region.Close` zerando o tamanho. **A I2 fechou 3 das 4 rotas**: o iterador de bitmap continuou cru até o fecho do M1, onde virou `bitmapIterator` reentrante |
| R11 (materializer) | Campo `auto` perdia o tipo no restart e sumia do `Compiled` | QP + fix round — `typeHint` durável no `fields.dict` v2 e rota read-only no `Compile` |
| Formato do `fields.dict` | v2 não travado: mutação de layout produzia arquivo que o leitor canônico truncava | QP fix round — golden de bytes calculado do layout documentado |
| Política de três baldes (materializer) | Falha de I/O do dicionário contabilizada como "evento ruim"; pipeline pulava em silêncio | QP fix round — só o conflito de tipo é evento ruim |
| Política de três baldes (CLI) | Os dois baldes colapsados na saída do `ingest`: perda de dado saía com **exit 0**, e `INTERNAL_STATE` nem aparecia no resumo | Fecho do M1 — linha própria por balde e `ExitPerdaDeDados` (7) |
| Exit code de corrupção | `encoding.ErrCorrupt` caía no default e saía como **exit 1** ("erro de uso") | Fecho do M1 — mapeado para `ExitDataset` (3) |
| FieldID não-determinístico | Datasets criados do mesmo YAML ficavam com ordem de coluna diferente em `SELECT *`, gravada para sempre | Fecho do M1 — `Open` ordena `BitmapFieldNames` antes de resolver |
| Dublê × real | O `fakestorage` aceitava key e record acima dos tetos que o Store real recusa | I1 — `ValidateRecord` no dublê + roteiro de comportamento compartilhado |
| Curto-circuito do AND | Desligá-lo fazia `A AND B` devolver o resultado de B, com a suíte verde | Reconciliação Q15/Q16 — diferencial árvore × programa |
| Teto de termos no WHERE | 1 M de termos aceitos, ~180 MB de AST retidos | Fix round Q13/Q14 — `parser.MaxWhereTerms` |
| Projeção repetida | `SELECT a, a` emitia chave repetida no objeto JSON e o consumidor colapsava as duas em silêncio | Decisão do fecho do M1 — recusada no binder, apontando a segunda ocorrência |
| Corpus fraco na medição | O gerador do LOAD era montado com `Config` literal parcial: zero patch, zero Extended JSON, zero null — enquanto o aceite citava 85% de patch | Fecho do M1 — `DefaultConfig()` no LOAD, perfil gravado no artefato e teste comparando com a referência |
| Documentação nunca executada | 4 defeitos no quickstart do README e 2 evidências citando teste que não provava o critério | Fecho do M1 — **parcialmente**: o quickstart do README é EXECUTADO por `TestQuickstartDoREADMEExecuta` (o YAML de config, o do dataset, o `ingest` e as duas queries, com a saída conferida linha a linha), e `TestEvidenciasCitadasNosDocumentosExistem` confere que todo **nome de teste** citado em `aceite-m1.md`, `architecture.md` e `debitos.md` existe na suíte. O verificador para EXATAMENTE aí: casa `func Test` e mais nada — não abre `.puml`, não resolve URL de diagrama, não confere número medido contra o JSON da rodada e não executa bloco de comando fora do README. Ver débito 41 |
| `kill -9` sem teste | O critério 25 era creditado por um teste de encerramento **limpo** | Fecho do M1 — `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado` mata o binário com SIGKILL de verdade |
| `--limit` cortava dentro de aspas duplas | A varredura textual que achava a cláusula LIMIT conhecia literal `'…'` e não identificador `"…"`: `--limit 2 'SELECT id, "no limit aqui" FROM t'` cortava no meio do identificador e saía erro de sintaxe | Errata do M1 — `ultimoLimit` passou a tokenizar com o `lexer` (o mesmo do `Execute`) e a pegar o último `TokenLimit`; some a segunda gramática, e com ela o escape por aspa duplicada e o boundary de identificador (`TestComLimitNaoCortaDentroDeLiteralNemDeIdentificador`, `TestComLimitNaoCortaQueryQueNaoTokeniza`, `TestFlagLimitNaoCortaDentroDeIdentificadorCitado`) |
| Vazão do CLI gateada por relógio | O piso absoluto de 4.000 ev/s media hardware e instrumentação junto com o código: sob `-race` com a máquina carregada a medida cai a ~3.100 ev/s **sem defeito nenhum**, e ainda deixava passar uma regressão de 3,7× | Errata do M1 — `TestVazaoDaIngestaoPeloCLI` mede a MESMA ingestão pelo caminho interno no mesmo processo e instante, e cobra do CLI no máximo 3× sobre essa referência. Ver débito 40 |

## Abertos, com dono

### M2 — consumidor Kafka, e o que ele exige antes

| # | Débito | Nota |
|---|---|---|
| 1 | Consumo de Kafka (§126.2) e recuperação de **offsets** (§126.9) | O recovery de dados está completo; falta o de posição no tópico |
| 2 | Custo de memória do consumidor (buffers, `ingestionQueueSize`) não está no orçamento medido | A rodada de aceite ingere de corpus em memória |
| 3 | **Medir carga MISTA de verdade** | Hoje LOAD (escrita pura) e STEADY (consulta, com 50 escritas efetivas em 30.000 eventos) nunca se sobrepõem, porque o gerador do STEADY reinicia as sequências e 29.950 eventos viram stale. Kafka **é** a sobreposição. Correção conhecida: expor `radiusgen.Config.SeqOffset` (ou pré-carregar as sequências) e re-rodar |
| 4 | **§95 — logging estruturado**, puxado para o M2 | Não depende do HTTP do M3, é barato, e é o que separa "dá para operar" de "dá para depurar às 3 da manhã". Hoje o CLI escreve texto livre em stderr |
| 5 | **§112 — `pprof`** (`-pprof :6060` + profiles ao fim de cada fase, prometido no design) | Zero ocorrência no código; sem profile, a primeira regressão de memória do consumidor é cega |
| 6 | **Oráculo do harness** | O `ingest-bench` mede latência e ausência de erro, e **não confere nenhum resultado**: um bug que fizesse toda query devolver 0 linhas passaria em todos os gates com p95 melhor. Também não valida a **presença** do conjunto de gates (um relatório com 8 em vez de 13 passa), nem gateia `row_count` |
| 7 | **3 rotas de leitura do harness descartam o erro** (`_, out, _, _ := st.GetByKey(...)`) | Um erro sistemático nelas produziria "0 erros" e latência melhor. Por isso a linha de zero erro do aceite fala de consultas **SQL** |
| 8 | **§14.20 — nenhum CRC verificado antes do `DurableOffset` no `Open`** | Corrupção em região selada só aparece na leitura, no meio do resultado |
| 9 | **§101 política `stop`** (a chave `errors:` sequer é parseada) e **§102 dead-letter** | Nenhum é bloqueante; ambos são **silêncio**, que é o problema |
| 10 | **§78 `query.resultBatchSize`** não parseado | Provavelmente só a chave falta |
| 37 | **`query.defaultMaxRows` e `query.defaultTimeout` do `nishidb.yaml` são parseados, validados e NUNCA lidos** | `internal/cli/config.go:37-38,44,80` faz o parse e recusa valor inválido; não existe leitor. O teto e o timeout **efetivos** vêm do YAML do **dataset** (`internal/schema/spec.go:409-425`: `query.maxRows` e `query.timeout`). Como os defaults coincidem (100000 e 30s), quem edita a chave global não vê efeito **nem aviso** — a configuração mente em silêncio. O README deixou de mostrá-las; fechar o débito é ler a chave global como default do dataset, ou recusá-la. Mesma família do 10 e do 15 |

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
| 22 | Backfill de índice bitmap (§14.17) | Hoje o conjunto de bitmaps é fixado na criação do dataset |
| 23 | Índice derivado removido por fora não é detectado (§14.18) | Nenhuma validação cobre apagar o diretório de índice |
| 24 | **Tombstone fura o stale check do `_SEQ`** | `GetByKey` mascara row tombstoned como **ausente**, e o materializer usa esse `found` como guarda do §25: com `found == false` o stale check não roda, o evento segue pelo caminho de create e o `Put` reusa o rowId — a row **ressuscita**. Latente hoje (nada produz tombstone); vira real quando a Fase 9 ligar o TTL |
| 25 | **`Store.Sequence` não honra tombstone** | Valida só a location. Mesma família do 24 |
| 38 | **A liveness não converge depois de um crash** | Quando a row já existia no ponto S e o tail traz um update cuja location está **à frente** do cursor do replay, o record antigo continua contado como vivo. Medido: **2 records / 127 bytes onde o certo é 1 / 63** logo após o replay, e **2 / 128 onde o certo é 1 / 64** depois de um terceiro update com checkpoint completo — ou seja, **permanente**: o decremento do `applyRecord` identifica o garbage por `s.locs.Get(rowID)`, location que já aponta para o record do tail, então cada update seguinte desconta o anterior a ELE e nunca o órfão. É a direção segura (superestimar adia a compaction; subestimar faria a compaction descartar dado vivo). Fato registrado em `TestResiduoDeLivenessDoReplayNaoConverge` e no comentário de `store_recovery.go`; quem reconstrói o tally por varredura é a compaction do débito 21 |

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
| 39 | **Os diagramas do `architecture.md` não renderizam: o repositório nunca foi publicado.** `git remote -v` é vazio, então `raw.githubusercontent.com/nishisan-dev/nishidb/main/…` responde 404 nos cinco `.puml` | **Ação do dono do repositório**, não de código: publicar o remoto. As URLs já apontam para `main` (apontavam para `feature/m1-query`, que some no merge) e os cinco fontes estão versionados em `docs/diagrams/` — a sintaxe dos cinco foi validada contra um servidor PlantUML |
| 40 | **O gate de vazão do CLI é relativo, e de propósito**: `TestVazaoDaIngestaoPeloCLI` mede a mesma ingestão pelo caminho interno no mesmo processo e cobra do CLI no máximo **3×** sobre ela | A razão é adimensional — `-race` e carga penalizam os dois lados juntos, e por isso o gate sobrevive nas duas trilhas (`make test` e `make test-race`), onde o piso absoluto ficava vermelho sem defeito. O que ele **não** pega: regressão no storage, que derruba os dois lados na mesma proporção e mantém a razão. Essa é a jurisdição de `benchmark/micro` e `benchmark/baselines`, e é o que precisa existir em CI para o flanco fechar |
| 41 | **O verificador de documentação só confere nome de teste.** `TestEvidenciasCitadasNosDocumentosExistem` casa `func Test` e mais nada | Não abre `.puml`, não resolve URL de diagrama, não executa bloco de comando fora do README (o quickstart é executado, o resto não) e não confere número medido — nem a data do rótulo — contra o JSON da rodada. Reabrir quando um documento voltar a citar o que não existe; o custo de cada checagem nova é baixo, o do silêncio não |
| 42 | **S13 em aberto**: property test com sequência aleatória de puts/gets contra oráculo `map` **com reopen aleatório**, e os benchmarks nomeados `BenchmarkPut`/`BenchmarkGetByKey` | Não é falta de cobertura, é granularidade: o caminho de escrita e o point lookup são medidos por `BenchmarkApplyIngestReal` e `BenchmarkQueryPointLookup` (ambos **0 allocs/op**, baseline em `benchmark/baselines/micro-20260813.txt`), e o crash real está em `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado`. O que falta mesmo é o **aleatório com reopen**, que é a única forma de pegar ordem de operações que ninguém pensou em escrever |
| 43 | **S15 em aberto**: `docs/architecture-storage.md` e o `c4_storage_component.puml` prometidos no design de storage | O `architecture.md` cobre a visão de sistema e o layout em disco; falta o detalhe por componente do storage (write path, recovery, C4 de componente) |
| 44 | **`make test-integration` é plumbing morto**: roda `go test -race -tags integration ./...` e **nenhum arquivo declara a tag** — é alias byte a byte de `make test-race` | Três saídas: dar conteúdo à tag (o crash test com subprocesso do S13 é o candidato natural), remover o alvo, ou deixá-lo documentado como está. Enquanto ninguém decide, o alvo promete uma suíte que não existe |

## Riscos de operação (não são débito de código, são de conhecimento)

Documentados em [`architecture.md`](architecture.md), seção "Operar isto":
`flock` inválido em NFS/FUSE/overlayfs, detecção de corrupção só na leitura,
teto de 64 KiB em campo indexado (e a segunda porta: declarar `index:` num campo
que já acumulou valores grandes torna o dataset inabrível), pausas do rehash e
do `msync`, e o phantom commit.

## O que NÃO ficou em aberto

Nenhum item da lista acima é dívida técnica no sentido de "atalho que vai
cobrar juros". Os resolvidos foram corrigidos com teste que prova a correção;
os abertos são **escopo de milestone futuro** ou **decisão registrada com a
razão**. A distinção importa: dívida esquecida vira surpresa, e decisão
registrada vira ponto de partida.
