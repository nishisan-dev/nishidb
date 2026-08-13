# Débitos abertos ao fim do M1

Varredura dos riscos do §14 do design de storage, dos riscos do design de
materializer/query e do que as revisões deixaram registrado. Cada item aberto
tem **milestone dono**; nenhum depende de trabalho não planejado.

## Resolvidos durante o M1 (registro, para não voltarem)

| Item | O que era | Onde fechou |
|---|---|---|
| Storage §14.21 | `Close()` sem quiesce: fechar com operação em voo produzia **panic** e devolvia o lock com escrita em voo | I2 — `RWMutex` de vida + iterador reentrante + `Region.Close` zerando o tamanho |
| R11 (materializer) | Campo `auto` perdia o tipo no restart e sumia do `Compiled` | QP + fix round — `typeHint` durável no `fields.dict` v2 e rota read-only no `Compile` |
| Formato do `fields.dict` | v2 não travado: mutação de layout produzia arquivo que o leitor canônico truncava | QP fix round — golden de bytes calculado do layout documentado |
| Política de três baldes | Falha de I/O do dicionário contabilizada como "evento ruim"; pipeline pulava em silêncio | QP fix round — só o conflito de tipo é evento ruim |
| Dublê × real | O `fakestorage` aceitava key e record acima dos tetos que o Store real recusa | I1 — `ValidateRecord` no dublê + roteiro de comportamento compartilhado |
| Curto-circuito do AND | Desligá-lo fazia `A AND B` devolver o resultado de B, com a suíte verde | Reconciliação Q15/Q16 — diferencial árvore × programa |
| Teto de termos no WHERE | 1 M de termos aceitos, ~180 MB de AST retidos | Fix round Q13/Q14 — `parser.MaxWhereTerms` |
| Projeção repetida | `SELECT a, a` emitia chave repetida no objeto JSON e o consumidor colapsava as duas em silêncio | Decisão do fecho do M1 — recusada no binder, apontando a segunda ocorrência |

## Abertos, com dono

### M2 — consumidor Kafka

| # | Débito | Nota |
|---|---|---|
| 1 | Consumo de Kafka (§126.2) e recuperação de **offsets** (§126.9) | O recovery de dados está completo; falta o de posição no tópico |
| 2 | Custo de memória do consumidor (buffers, `ingestionQueueSize`) não está no orçamento medido | A rodada de aceite ingere de corpus em memória |

### M3 — protocolo e driver

| # | Débito | Nota |
|---|---|---|
| 3 | Protocolo HTTP `POST /v1/query` (§126.17) | O NDJSON e o `StreamWriter` já são o formato; falta o transporte |
| 4 | Driver JDBC (§126.18) e `PreparedStatement` real (§126.19) | Parâmetros posicionais já funcionam fim a fim |
| 5 | **Metadados de tipo fora de banda** | O NDJSON **não carrega tipos**, e o `ResultSetMetaData` precisa deles — inclusive das colunas que são NULL em todas as linhas, que não dão para derivar do dado. A fonte é `binder.BoundQuery.Columns` |
| 6 | Autenticação (§126.20) e TLS (§126.21) | Seções do YAML já são aceitas e ignoradas |

### M4 / Fases 8-9

| # | Débito | Nota |
|---|---|---|
| 8 | Índice **hash físico** (§126.15) | Planner e executor já implementam a rota e a testam contra o dublê; no real degrada para full scan |
| 9 | Expiração por **TTL** (§126.22) e tombstones por retenção (§126.23) | Declaração e bit já existem e são honrados na leitura |
| 10 | **Compaction** de segmentos (§126.24) | `minimumLiveRatio` já registrado no design |
| 11 | Backfill de índice bitmap (§14.17) | Hoje o conjunto de bitmaps é fixado na criação do dataset |
| 12 | Índice derivado removido por fora não é detectado (§14.18) | Nenhuma validação cobre apagar o diretório de índice |

### Sem milestone — decisões registradas que podem virar trabalho

| # | Item | Quando reabrir |
|---|---|---|
| 13 | **Penhasco posicional do flatten** em 67 conjuncts: a mesma query com termos reordenados muda de `BitmapScan` para `FullScan` | Se aparecer query real com dezenas de conjuncts |
| 14 | **Grupo OR indexável dentro de AND** não vira união (a regra 5 vale só na raiz), e `key='a' OR key='b'` não vira união de point lookups | Otimização; ambos são superset correto hoje |
| 15 | **`StreamWriter` nunca encolhe**: uma linha de 1 MiB fixa a capacidade pela vida do writer; com N queries concorrentes o pior caso é N × maior linha | Se o RSS por conexão importar na Fase 6 |
| 16 | **Alarme falso do teto exato**: resultado com exatamente `maxRows` linhas reporta truncamento | Distinguir exigiria varrer o resto do dataset |
| 17 | **Divergência textual do `'g'`** contra o `encoding/json` em ~27% dos doubles (mesmo `float64`, grafia diferente) | Se algum cliente comparar a saída byte a byte com o encoder padrão |
| 18 | **Mensagens de erro de rede trocáveis** entre os parsers de ipv4/ipv6/prefixo | Dívida pré-existente registrada na QP |
| 19 | **`allocs/evento` do STEADY mistura ingestão e query** quando há leitores SQL | Separar exigiria contabilidade por goroutine |

## O que NÃO ficou em aberto

Nenhum item da lista acima é dívida técnica no sentido de "atalho que vai
cobrar juros". Os resolvidos foram corrigidos com teste que prova a correção;
os abertos são **escopo de milestone futuro** ou **decisão registrada com a
razão**. A distinção importa: dívida esquecida vira surpresa, e decisão
registrada vira ponto de partida.
