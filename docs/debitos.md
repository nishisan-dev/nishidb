# Débitos abertos ao fim do M1

Varredura dos riscos do §14 do design de storage, dos riscos do design de
materializer/query e do que as revisões deixaram registrado — incluindo a
revisão de fechamento, cuja unidade foi a **fronteira entre camadas** e não a
tarefa. Cada item aberto tem **milestone dono**; nenhum depende de trabalho não
planejado.

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
| Documentação nunca executada | 4 defeitos no quickstart do README e 2 evidências citando teste que não provava o critério | Fecho do M1 — o README é executado por teste, e as citações de teste dos documentos são verificadas por teste |
| `kill -9` sem teste | O critério 25 era creditado por um teste de encerramento **limpo** | Fecho do M1 — `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado` mata o binário com SIGKILL de verdade |

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

### M3 — protocolo e driver

| # | Débito | Nota |
|---|---|---|
| 11 | Protocolo HTTP `POST /v1/query` (§126.17) | O NDJSON e o `StreamWriter` já são o formato; falta o transporte |
| 12 | Driver JDBC (§126.18) e `PreparedStatement` real (§126.19) | Parâmetros posicionais já funcionam pela API Go; o CLI não tem como fornecê-los |
| 13 | **Metadados de tipo fora de banda** | O NDJSON **não carrega tipos**, e o `ResultSetMetaData` precisa deles — inclusive das colunas que são NULL em todas as linhas, que não dão para derivar do dado. A fonte é `binder.BoundQuery.Columns` |
| 14 | Autenticação (§126.20) e TLS (§126.21) | Seções do YAML já são aceitas e ignoradas |
| 15 | **§80/§81 — `runtime.memoryTarget`, `query.maxConcurrent`, `query.maxBufferPerQuery`** e a prioridade sob pressão | A seção `runtime:` do YAML é aceita e **silenciosamente ignorada**. Com N conexões, o pior caso do débito 20 é N × maior linha, sem teto |
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
