# Glossário PT-BR ↔ inglês

Este documento é normativo para o ciclo "identificadores em inglês"
(`planning/identificadores-em-ingles.md`). Ele fixa a ponte entre a prosa em
PT-BR do repositório e os identificadores em inglês que o código passa a usar:
comentário novo cita o termo PT-BR daqui, identificador novo usa o termo
inglês daqui. `internal/gates/glossary_test.go` lê a tabela de termos de
domínio como dado — célula vazia ou termo PT-BR repetido é falha de build da
documentação, não só de revisão humana.

## 1. Norma: idioma é decidido por quem lê

O repositório nasceu inteiro em PT-BR. A partir deste ciclo a norma passa a
ser dividida em duas metades, e o critério que separa as duas é **quem lê**:

| Lê quem | Idioma | Exemplos |
|---|---|---|
| Desenvolvedor lendo prosa | PT-BR | docs, planning, godoc e comentários, mensagens de commit, mensagens de falha de teste (`t.Errorf`, `t.Fatalf`), texto dos débitos e aceites |
| Compilador, operador, usuário, cliente remoto | inglês | identificadores (tipos, funções, métodos, campos, parâmetros, locais, constantes), nomes de pacote e de arquivo `.go`, nomes de teste e subteste, mensagens de erro e de log, chaves e atributos de log estruturado, chaves de config, campos do wire, métricas, valores de CLI, nomes de arquivo em disco, código dos clients |

A regra tem uma consequência que este glossário resolve: a prosa em PT-BR
fala de "lápide", "fila" e "orçamento", e o código diz `tombstone`, `queue` e
`budget`.

**Sem compat.** Nada foi lançado. Chaves de YAML, campo do wire, métricas,
valor de CLI, nomes públicos do client Python e nomes de arquivo em disco
mudam de nome direto, sem aceitar os dois por um ciclo.

## 2. Regras de tradução

**Estilo Go, sem exceção**: MixedCaps; iniciais em maiúsculas (`ID`, `URL`,
`HTTP`, `JSON`, `TLS`, `CRC`, `TTL`, `SQL`, `NDJSON`, `CLI`, `RSS`); nada de
prefixo `Get`; receptores de uma ou duas letras não mudam; sentinelas
`ErrXxx`, tipos de erro `XxxError`; construtores `New`/`Open`. O revive
(`exported`, `var-naming`) é quem cobra, e a baseline de lint é re-medida no
fechamento de cada fase.

**O que já está em inglês não muda.** `Store`, `Put`, `Open`, `Apply`,
`ErrDatasetLocked`, `FullScanPlan` ficam como estão. Nome híbrido traduz só a
metade PT-BR: `ErrOrcamentoExcedido` → `ErrBudgetExceeded`.

**Nomes de teste** preservam a frase: sujeito, ação, condição.
`TestTopNReaproveitaOBufferDaEntradaDespejada` →
`TestTopNReusesTheBufferOfTheEvictedEntry`. Sem limite de comprimento; a
frase importa mais que a brevidade, e é isso que o repositório sempre fez.
Subtestes seguem a mesma regra em minúsculas.

**Chaves de log** ficam `domínio.evento` em snake_case inglês:
`consumer.subiu` → `consumer.up`, `particoes.atribuidas` →
`partitions.assigned`, `ingestao.sem_progresso` → `ingestion.stalled`. Os
atributos já são em inglês; o único em PT-BR fica listado no mapeamento da
Tarefa 0.7.

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
sufixo numérico. É o mesmo mecanismo usado abaixo para separar os dois
sentidos de "registro".

## 3. Termos de domínio

Uma linha por termo. A coluna "onde aparece" é o pacote ou documento onde o
termo vive hoje (achado por grep no repositório, nunca por suposição); depois
da tradução, ela deixa de ser o nome PT-BR e passa a ser o pacote/arquivo
inglês correspondente.

| PT-BR | inglês | onde aparece |
|---|---|---|
| lápide | tombstone | internal/storage |
| fila | queue | internal/kafka |
| apenda | append | internal/storage |
| apendar | append | internal/storage |
| esteira | pipeline | internal/kafka |
| aposentar | retire | internal/storage/counter |
| aposentadoria | retirement | internal/storage/counter/retirement.go |
| vigia | watchdog | internal/kafka/watchdog.go |
| orçamento | budget | internal/query/budget |
| instantâneo | snapshot | internal/kafka/snapshot.go |
| transbordo | spill | internal/query/spill |
| janela | window | internal/storage/rowindex/schedule.go |
| congelada | frozen | internal/materializer/frozen.go |
| congelar | freeze | internal/materializer/frozen.go |
| passada | sweep | internal/config/config.go |
| prazo | deadline | internal/storage/rowindex/schedule.go |
| vencimento | expiry | internal/storage/rowindex/schedule.go |
| agenda | schedule | internal/storage/schedule.go |
| moldura | frame | internal/protocol |
| em voo | in-flight | clients/python |
| contador | counter | internal/storage/counter |
| grupo | group | internal/storage/counter |
| linha | row | internal/storage/rowindex |
| campo | field | internal/schema |
| campo (componente de data/hora) | component | internal/typeparse/datetime.go |
| chave | key | internal/index/valuekey |
| dicionário | dict | internal/storage/counter/dict.go |
| segmento | segment | internal/storage/segment |
| reconstrução | rebuild | internal/storage/indices_rebuild.go |
| descartado | discarded | internal/materializer/materializer.go |
| recusa | refuse | internal/config/config.go |
| recusar | refuse | internal/config/config.go |
| falho (writer) | failed | internal/storage/store.go |
| ingestão | ingestion | internal/cli/ingest.go |
| consulta | query | internal/query |
| materialização | materialization | internal/materializer |
| histórico | history | internal/materializer/history.go |
| retenção | retention | internal/storage/retention.go |
| expiração | expiry | internal/storage/expiry.go |
| autenticação | auth | internal/server/auth.go |
| credencial | credential | internal/cli/credential.go |
| parâmetro | param | internal/protocol/params.go |
| operação | operation | internal/cli/operation.go |
| mutação | mutation | internal/cli/mutation.go |
| evidência | evidence | internal/cli/evidence_test.go |
| citação | citation | internal/gates/citations_test.go |
| gate | gate | internal/gates |
| dobra | fold | internal/storage/counter |
| denso | dense | internal/storage/rowindex/dense32.go |
| esparso | sparse | docs/architecture.md |
| selado | sealed | internal/index/segindex/sealed.go |
| sela | seal | internal/index/segindex/seal.go |
| união | union | internal/index/segindex/union.go |
| avaliador | evaluator | internal/predicate/evaluator.go |
| programa | program | internal/predicate/program.go |
| regra | rule | internal/predicate/rule.go |
| leitor | reader | internal/config/config.go |
| escritor | writer | internal/index/segindex/writer.go |
| fonte | source | internal/query/meta/source.go |
| iterador | iterator | internal/index/segindex/union.go |
| tabela | table | internal/counters/table.go |
| registro de virtuais | registry | internal/query/engine.go |
| registro (linha) | record | internal/storage/encoding/record.go |
| abertura | open | internal/counters/open.go |
| compila | compile | internal/counters/compile.go |
| coleta | collect | internal/query/executor/collect.go |
| extração | extract | internal/materializer/patch.go |
| estatísticas | stats | internal/counters/meta.go |
| relatório | report | internal/server/counters.go |
| posição | position | internal/cli/offsets.go |
| cadência | cadence | internal/kafka/watchdog.go |
| política | policy | internal/config/config.go |
| corrida | race | internal/index/segindex/writer.go |
| desfecho | outcome | internal/cli/outcome_internal_test.go |
| perda | loss | internal/cli/loss_test.go |
| resumo | summary | internal/metakafka/table.go |
| vazão | throughput | internal/cli/throughput_test.go |
| emvoo | inflight | internal/server/server.go |
| carência | grace | internal/storage/grace_measurement_test.go |
| lacuna | gap | internal/gates/citations_test.go |
| resíduo | residue | internal/index/segindex/wip.go |
| espelho | mirror | internal/schema/history.go |
| órfã | orphan | internal/schema/history.go |
| paridade | parity | internal/query/executor/ndjson.go |
| comportamento | behavior | internal/config/config.go |
| chave-texto | textkey | internal/storage/counter/textkey.go |
| capacidades | capabilities | internal/server/auth.go |
| consumido | consumed | internal/jsonp/jsonp.go |
| contado | counted | internal/cli/loss_test.go |
| derivadas (colunas do histórico) | derived | internal/materializer/history.go |
| derivar / derivação (reconstruir o logger a partir da raiz) | derive / derivation | internal/logging/logging.go |
| vencimentos | expiries | internal/storage/schedule.go |
| prazos | deadlines | internal/storage/schedule.go |
| padrão (valor herdado quando a chave está ausente) | default — `Default` e `default` em nome composto (`DefaultMaxRows`, `defaultTimeout`); `defaultValue` em parâmetro ou local solto, porque `default` é palavra reservada em Go | internal/units |
| padrão (formato de data, de rota ou de LIKE) | pattern | internal/typeparse/datetime.go, internal/server/auth.go, internal/query/parser/parser.go |
| entrada (de teste) | input | internal/units/units_test.go |
| casos (de teste) | cases | internal/units/units_test.go |
| quer (esperado em teste) | want | internal/units/units_test.go |
| unidade (de tamanho) | unit | internal/units/units.go |
| sufixo | suffix | internal/units/units.go |
| carrega (transporta um valor no próprio tipo) | carries | internal/types/types.go |
| carrega (lê de arquivo ou de config) | load | internal/config/config.go |
| presença | presence | internal/types/types.go |
| ausente | absent | internal/types/types.go |
| vivo / viva | live | internal/storage, internal/types/types.go |
| expirado / expirada | expired | internal/types/types.go, internal/storage/snapshot.go |
| época (contador de publicações do writer) | epoch | internal/types/types.go, internal/storage/store.go, internal/storage/versions |
| época (geração de checkpoint do manifest) | manifest epoch | internal/storage/checkpoint/manifest.go, internal/storage/store.go |
| instante | instant | internal/types/types.go, internal/storage/store.go |
| domínio (do padrão de data: datetime ou time) | domain | internal/typeparse/datetime.go |
| meridiano | meridiem | internal/typeparse/datetime.go |
| escapado (texto fixo entre aspas simples no padrão de data) | quoted | internal/typeparse/datetime.go |
| prova (o round-trip em UTC que confere se a data de calendário existe) | round-trip | internal/typeparse/datetime.go |
| piloto | pilot | internal/typeparse/datetime_test.go |
| retido | retained | internal/jsonp/jsonp.go, internal/storage/snapshot.go, internal/kafka/pipeline.go |
| entrada (documento que o parser percorre) | input | internal/jsonp/jsonp.go |
| entrada (de diretório) | entry | internal/gates/verification_test.go |
| encolher / encolhimento | shrink | internal/jsonp/jsonp.go, internal/kafka/pipeline.go |
| soltar (referência ou recurso) | release | internal/jsonp/jsonp.go |
| liberar | release | internal/storage/mmap/region.go, internal/query/budget/budget.go |
| formato (de saída do log) | format | internal/logging/logging.go |
| redactado | redacted | internal/logging/logging.go |
| sensível (chave ou valor que nunca vai ao log) | sensitive | internal/logging/logging.go |
| evento (o payload bruto do evento, chave sensível de log) | event_payload | internal/logging/logging.go |
| segredo | secret | internal/logging/logging.go |
| redigir (suprimir o valor de chave sensível no log) | redact | internal/logging/logging.go |
| descartado (logger que não escreve nada) | discard | internal/logging/logging.go |
| nível (de log) | level | internal/logging/logging.go |
| nível (da árvore da agenda) | level | internal/storage/checkpoint/manifest.go, internal/storage/rowindex/schedule.go |
| raiz | root | internal/logging/logging.go, internal/gates |
| origem (logger do qual se deriva) | source | internal/logging/logging_test.go |
| linha (de texto, de log ou de arquivo) | line | internal/logging, internal/memprof, internal/gates |
| senha | password | internal/logging/logging_test.go |
| corpo (de payload, de requisição ou de arquivo) | body | internal/logging/logging_test.go, internal/storage/checkpoint |
| tem (prefixo de booleano de presença) | has | internal/memprof/memprof_linux.go, internal/typeparse/datetime.go |
| tem (obtido em teste) | got | internal/storage/checkpoint/offsets_test.go |
| separação (do RSS em anônimo e file-backed) | separation | internal/memprof/memprof_linux.go |
| anônimo (a parte do RSS não respaldada por arquivo) | anon | internal/memprof/memprof_linux.go |
| partes (tokens de uma linha de texto) | parts | internal/memprof/memprof_linux.go |
| lixo (alocação descartável em teste) | junk | internal/memprof/memprof_test.go |
| valor (o dado de um par nome/valor de texto) | value | internal/memprof/memprof_linux.go, internal/gates, internal/testutil/radiusgen |
| soma (adição aritmética) | sum | internal/memprof/memprof_test.go |
| soma (concatenação de strings no AST) | concat | internal/gates/verification_test.go |
| data (de calendário) | date | internal/buildinfo/buildinfo.go, internal/typeparse/datetime.go |
| dia | day | internal/types/types.go, internal/typeparse/datetime.go |
| somente-leitura | read-only | internal/storage/mmap/region.go |
| vazio | empty | internal/storage/mmap/region.go |
| exigir | require | internal/storage/mmap/region.go |
| mutável | mutable | internal/storage/mmap/region.go, internal/storage/indices_rebuild.go |
| imutável | immutable | internal/storage/indices.go, internal/storage/indices_rebuild.go, internal/index/segindex |
| bloco (de filesystem) | block | internal/storage/mmap/region.go, docs/architecture.md |
| meia-word | half-word | internal/storage/mmap/region.go |
| descritor (de arquivo) | descriptor | internal/storage/mmap/region_test.go |
| caminho | path | internal/gates, internal/storage/mmap/region.go, internal/storage/mmap/region_test.go |
| antes / depois (medição de teste) | before / after | internal/memprof/memprof_test.go, internal/storage/mmap/region_test.go |
| anterior (o valor anterior) | prev | internal/query/lexer, internal/testutil/radiusgen, internal/buildinfo/buildinfo_internal_test.go |
| fundo (regra ou duração de fundo: a regra de TTL sem `when`) | fallback | internal/schema/ttl_rule.go, internal/storage/checkpoint/manifest.go |
| coluna | column | internal/counters, internal/storage/checkpoint/manifest.go |
| índice | index | internal/index, internal/storage/checkpoint/manifest.go |
| cauda (TLV do manifest) | tail | internal/storage/checkpoint/manifest.go |
| origem (do fluxo dos offsets Kafka) | source | internal/storage/checkpoint/offsets.go |
| canonizar / canônico | canonical | internal/storage/checkpoint/offsets.go, internal/metakafka/registry.go |
| divergência | divergence | internal/storage/checkpoint/offsets.go, internal/storage/counters.go |
| partição | partition | internal/kafka, internal/storage/checkpoint/offsets.go |
| passo (de workflow ou de durabilidade) | step | internal/gates/ci_test.go, internal/storage/checkpoint |
| passo (do layout do manifest: bytes por registro de segmento) | bytes-per-segment | internal/storage/checkpoint/manifest_test.go |
| passo (crescimento em bytes causado pelas seções a mais) | growth | internal/storage/checkpoint |
| marcador (§NN, ou marca de busca em bytes) | marker | internal/gates/verification_test.go, internal/storage/checkpoint |
| cláusula | clause | internal/query/lexer/errors.go |
| grafia (a caixa com que a keyword aparece no fonte) | casing | internal/query/lexer/keyword_test.go |
| teto | ceiling | internal/query/budget/budget.go, internal/testutil/radiusgen, internal/gates/lint_test.go |
| admissão / admitir | admission / admit | internal/query/budget/budget.go |
| reservar / reserva | reserve | internal/query/budget/budget.go |
| devolver (dar de volta ao orçamento a quantia exata reservada) | return | internal/query/budget/budget.go |
| usado (bytes reservados contra o teto) | used | internal/query/budget/budget.go |
| sequência (controle de sequência por entidade) | sequence | internal/testutil/radiusgen/radiusgen.go |
| evento (o registro que a ingestão consome; e a chave estável `domínio.evento` do log) | event | internal/testutil/radiusgen, internal/logging/logging.go |
| entidade | entity | internal/testutil/radiusgen |
| vistas (quantas vezes cada chave apareceu) | appearances | internal/testutil/radiusgen/radiusgen_test.go |
| visto (presença: a chave já foi vista) | seen | internal/testutil/radiusgen/radiusgen_test.go |
| nome | name | internal/testutil/radiusgen, internal/gates |
| prefixo | prefix | internal/testutil/radiusgen/radiusgen_test.go |
| alvo (do Makefile, de citação ou de sorteio) | target | internal/gates, internal/testutil/radiusgen |
| alvo (elemento procurado num slice) | item | internal/gates/lint_test.go |
| bruto | raw | internal/gates |
| achado (de regexp) | match | internal/gates |
| linha de base | baseline | internal/gates/ci_test.go |
| piso (versão mínima) | floor | internal/gates |
| dublê (de teste) | fake | internal/gates |
| raspagem | scrape | internal/gates/rollback_update_test.go |
| local (da citação) | location | internal/gates/verification_test.go |
| cerca (de bloco de código) | fence | internal/gates/verification_test.go |
| seção | section | internal/gates/verification_test.go |
| cabeçalho (markdown) | heading | internal/gates/verification_test.go |
| amostra (de métrica) | sample | internal/metrics, internal/gates/verification_test.go |
| família (de métrica) | family | internal/metrics, internal/gates/verification_test.go |
| série (de métrica) | series | internal/metrics, internal/gates/metrics_test.go |
| marcador (de item de lista) | bullet | internal/gates/verification_test.go |
| marcador (arquivo de sincronização entre dublê e teste) | marker file | internal/gates/rollback_update_test.go |
| elidido (literal sem tipo escrito) | elided | internal/gates/verification_test.go |
| prazo (de verificação do script de atualização) | timeout | internal/gates/rollback_update_test.go |
| derrubar (o servidor, no meio da corrida) | close | internal/gates/rollback_update_test.go |
| menor (predicado de ordenação) | less | internal/gates/verification_test.go |
| proibido (nome vetado por spec ou por gate) | banned | internal/logging/logging_test.go, internal/gates/jdbc_test.go, internal/query/executor/ndjson_test.go |

## 4. Como usar

- Comentário novo cita o termo PT-BR desta lista; identificador novo usa o
  termo inglês correspondente.
- Termo novo entra nesta lista ANTES de ser usado em código — nunca depois.
- O juiz de cada fase pode AMPLIAR o glossário (adicionar linha), nunca
  CONTRADIZER uma linha existente (mudar a tradução de um termo já fechado).
- Colisão entre dois sentidos do mesmo termo PT-BR (como os dois "registro"
  acima) resolve qualificando o termo PT-BR com o sentido entre parênteses —
  nunca dando aos dois o mesmo termo inglês e nunca usando sufixo numérico.

Escolhas que o juiz do G1 fechou e que valem para os grupos seguintes:

- **teto → ceiling.** Três pacotes propuseram três palavras (`limit`,
  `ceiling`, `cap`). `limit` já é a cláusula `LIMIT` do SQL dentro de
  `internal/query` (`bq.Limit`, `HasLimit`), e `cap` e `max` são builtins que
  o revive acusa quando sombreados. `ceiling` é o mais específico e não
  colide com nada; "limite" (palavra geral) continua virando `limit`
  (`LimitRowID`), e o `Max` de nome composto já consagrado (`MaxRows`,
  `SnapshotMaxBytes`) não muda.
- **antes/depois vs. anterior.** A medição de teste ("colhida antes das
  alocações") é `before`/`after`; "o anterior" (o token anterior, o ip já
  registrado) é `prev`, mesmo quando a prosa PT-BR diz "antes".
- **alvo**, **campo**, **carrega**, **descartado**, **entrada**, **época**,
  **evento**, **grafia**, **linha**, **marcador**, **nível**, **origem**,
  **passo**, **prazo**, **soma**, **tem**, **valor** e **visto** têm mais de um
  sentido cada, separados por qualificação na tabela — nunca pelo mesmo termo
  inglês.
- **época: publicação ≠ checkpoint.** São dois contadores distintos, e o
  código já os separava por acidente de idioma. `epocaPub`
  (`internal/storage/store.go`) conta PUBLICAÇÕES do writer e é o relógio
  lógico do isolamento; ela é a época que `types.Snapshot.Epoch` carrega. O
  `Epoch` do manifest (`internal/storage/checkpoint/manifest.go`) é a geração
  de CHECKPOINT, uma por checkpoint gravado. Dentro de `checkpoint` não há
  ambiguidade e `Manifest.Epoch` fica como está; onde os dois se encontram —
  `internal/storage`, onde `s.epoch` espelha o do manifest e `snap.Epoch` é o
  da publicação — o do manifest é quem qualifica (`manifestEpoch`).
- **cauda TLV ≠ tail do CRC.** Em `internal/storage/checkpoint` o
  identificador `tail` já nomeia os 4 bytes finais do CRC
  (`manifest.go`, `offsets.go`), que ficam FORA do corpo; a cauda TLV mora
  DENTRO dele. A família da cauda qualifica com o prefixo que o nome
  exportado já usa: `TLVTailSize`, `appendTLVTail`, `decodeTLVTail`,
  `tlvTail`.
- **fundo → fallback.** A "duração de fundo" é a regra de TTL sem `when`, a
  que casa com tudo; `background` leria como assíncrono.
- **evento: a chave do log ≠ o payload.** `event` é o identificador estável
  que o §95 fixa (`kafka.offset.commit`), e é ele que sai em toda linha de log
  (`internal/logging/logging.go`). O `"evento"` que mora em `chavesSensiveis`
  é o outro sentido — o payload bruto que nunca pode ir ao log —, e o inglês
  dele já está escrito uma linha abaixo, `"event_payload"`. Na Fase 3 a
  grafia PT-BR daquela lista é DESCARTADA, não traduzida: `"event"` dentro de
  `sensitiveKeys` redigiria o campo de toda linha, e `logging_test.go` fica
  vermelho se isso passar.
- **usado: reserva ≠ ocupação.** Em `internal/query/budget`, `used` é quanto
  está RESERVADO contra o teto — a admissão guarda o teto inteiro, sem ter
  alocado um byte. O `ordenador.usado` de `internal/query/executor/sort.go` é
  outra coisa: a memória que o operador de fato ocupa, recalculada a cada
  chamada. Ele ganha linha própria (e termo próprio — `footprint` ou `inUse`)
  quando o executor for mapeado; o que não pode é os dois virarem `used`.
- **proibido → banned.** `forbidden` já é o 403 do wire
  (`protocol.CodForbidden`); o "proibido" dos gates é nome vetado por spec ou
  por gate, e é o mesmo `banned` nos três pacotes que o usam.
- **prova → round-trip, nunca probe.** `probe` já nomeia a cadeia de sondagem
  do open addressing (`internal/index/primary`, `internal/storage`), e a
  "prova" de `internal/typeparse` é verificação — o `time.Date` em UTC
  construído só para o round-trip que confere se a data de calendário existe.
- **Verbos comuns não entram na tabela** (lê → read, escreve → write, roda →
  run, monta → setup ou new conforme seja preparação de ambiente ou
  construtor, confere → check, contém → contains, eh → is): o motivo de cada
  linha do mapeamento é que registra a escolha.
