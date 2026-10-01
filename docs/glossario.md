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
| disco | `grupos.dict`, `grupos.idx`, `prazos.idx`, `expiry.idx`, `vencimentos.idx` | `groups.dict`, `groups.idx`, `ttls.idx`, `last_writes.idx`, `expiries.idx` |
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
| instantâneo (e o retrato T0 dos totais do contador que a auditoria compara) | snapshot | internal/kafka/snapshot.go, internal/storage/counter |
| transbordo | spill | internal/query/spill |
| janela | window | internal/storage/rowindex/schedule.go |
| congelada (a coluna cujo valor para de mudar na escrita) | frozen | internal/materializer/frozen.go, internal/schema |
| congelar | freeze | internal/materializer/frozen.go |
| passada | sweep | internal/config/config.go |
| vencimento | expiry | internal/storage/rowindex/schedule.go |
| agenda | schedule | internal/storage/schedule.go |
| moldura | frame | internal/protocol |
| em voo | in-flight | clients/python |
| contador | counter | internal/storage/counter |
| grupo | group | internal/storage/counter |
| linha | row | internal/storage/rowindex |
| campo | field | internal/schema |
| schema (o container SQL em que TODAS as tabelas do dataset aparecem para o cliente; não confundir com o bloco `schema:` do YAML, que declara os campos) | schema | internal/schema/spec.go |
| campo (componente de data/hora) | component | internal/typeparse/datetime.go |
| chave | key | internal/index/valuekey |
| dicionário | dict | internal/storage/counter/dict.go |
| segmento | segment | internal/storage/segment |
| reconstrução / reconstruído | rebuild / rebuilt | internal/storage/indices_rebuild.go, internal/storage/encoding/fields_test.go |
| descartado | discarded | internal/materializer/materializer.go |
| recusa (o ato de recusar) | refuse | internal/config/config.go |
| recusar | refuse | internal/config/config.go |
| falho (writer) | failed | internal/storage/store.go |
| ingestão | ingestion | internal/cli/ingest.go |
| consulta | query | internal/query |
| materialização | materialization | internal/materializer |
| histórico | history | internal/materializer/history.go |
| retenção | retention | internal/storage/retention.go |
| expiração | expiry | internal/storage/expiry.go |
| autenticação (o prefixo curto de identificadores e de arquivos) | auth | internal/server/auth.go |
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
| escritor | writer | internal/index/segindex/writer.go, internal/testutil/fakestorage |
| fonte | source | internal/query/meta/source.go |
| iterador | iterator | internal/index/segindex/union.go |
| tabela | table | internal/counters/table.go |
| catálogo (a enumeração de toda tabela consultável de um dataset) | catalog | internal/query/catalog.go |
| schema declarado (o valor da chave obrigatória `schemaName` no YAML do dataset, único no servidor; é dele que sai o schema em que as tabelas daquele dataset aparecem) | schemaName | internal/schema/spec.go, internal/config/config.go |
| espécie da tabela (TABLE para a base, VIEW para contador e tabela de meta) | kind | internal/query/catalog.go |
| tabela de meta (a tabela do namespace `_meta` que descreve o dataset: `fields`, `counters`, `kafka`, `tables`) | meta table | internal/query/metatables.go, internal/query/meta |
| tabela de sistema (o rótulo `SYSTEM TABLE` do JDBC, que este produto NÃO publica: as tabelas de meta saem como VIEW, porque o cliente gráfico esconde objeto de sistema por default) | system table | clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiDatabaseMetaData.java |
| schema da conexão (o schema que a URL do JDBC e o `nishidb query --server --schema` nomeiam; é o escopo da árvore e da resolução do FROM) | connection schema | internal/query/engine.go, internal/server/connection_schema.go, clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiUrl.java |
| árvore (do cliente SQL: schemas → tabelas → colunas que o driver JDBC publica a partir do catálogo) | tree | clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/NishiDatabaseMetaData.java |
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
| molde (o formato declarado do `groupKey`) | format | internal/keyformat |
| marcador (o `{coluna}` dentro do molde) | placeholder | internal/keyformat |
| literal (o texto entre dois marcadores) | literal | internal/keyformat |
| capacidades | capabilities | internal/server/auth.go |
| consumido | consumed | internal/jsonp/jsonp.go |
| contado | counted | internal/cli/loss_test.go |
| derivadas (colunas do histórico; e as colunas aninhadas derivadas de `extract`) | derived | internal/materializer/history.go, internal/query/binder, internal/schema |
| derivar / derivação (reconstruir o logger a partir da raiz; e calcular grupo e condição a partir do payload) | derive / derivation | internal/logging/logging.go, internal/storage/counter, internal/schema |
| vencimentos | expiries | internal/storage/schedule.go |
| prazo / prazos (a DURAÇÃO de ociosidade da linha, em millis: por quanto tempo ela pode ficar sem escrita — o TTL) | TTL / TTLs | internal/storage/rowindex |
| padrão (valor herdado quando a chave está ausente) | default — `Default` e `default` em nome composto (`DefaultMaxRows`, `defaultTimeout`); `defaultValue` em parâmetro ou local solto, porque `default` é palavra reservada em Go | internal/units |
| padrão (formato de data, de rota ou de LIKE) | pattern | internal/typeparse/datetime.go, internal/server/auth.go, internal/query/parser/parser.go |
| entrada (de teste) | input | internal/units/units_test.go |
| casos (de teste) | cases | internal/units/units_test.go |
| quer (esperado em teste) | want | internal/units/units_test.go |
| porque (a justificativa do caso de teste, que a mensagem de falha imprime) | reason | internal/storage/versions/versions_test.go |
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
| reter / retido | retain / retained | internal/jsonp/jsonp.go, internal/storage/snapshot.go, internal/kafka/pipeline.go, internal/storage/versions |
| entrada (documento que o parser percorre) | input | internal/jsonp/jsonp.go |
| entrada (de diretório, de tabela, de vetor denso ou do journal de versões) | entry | internal/gates/verification_test.go, internal/index/segindex, internal/storage/rowindex, internal/storage/versions |
| encolher / encolhimento | shrink | internal/jsonp/jsonp.go, internal/kafka/pipeline.go |
| soltar (referência ou recurso) | release | internal/jsonp/jsonp.go |
| liberar | release | internal/storage/mmap/region.go, internal/query/budget/budget.go |
| formato (de saída do log; e a string de formatação printf) | format | internal/logging/logging.go, internal/index/segindex/wip_test.go |
| redactado | redacted | internal/logging/logging.go |
| sensível (chave ou valor que nunca vai ao log) | sensitive | internal/logging/logging.go |
| evento (o payload bruto do evento, chave sensível de log) | event_payload | internal/logging/logging.go |
| segredo | secret | internal/logging/logging.go |
| redigir (suprimir o valor de chave sensível no log) | redact | internal/logging/logging.go |
| descartado (logger que não escreve nada) | discard | internal/logging/logging.go |
| nível (de log) | level | internal/logging/logging.go |
| nível (da árvore da agenda) | level | internal/storage/checkpoint/manifest.go, internal/storage/rowindex/schedule.go |
| raiz | root | internal/logging/logging.go, internal/gates, internal/config |
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
| valor (o dado de um par nome/valor de texto, de um label, de um parâmetro ou de uma entrada de índice) | value | internal/memprof/memprof_linux.go, internal/gates, internal/testutil/radiusgen, internal/metrics, internal/protocol, internal/storage/rowindex |
| soma (adição aritmética) | sum | internal/memprof/memprof_test.go |
| soma (concatenação de strings no AST) | concat | internal/gates/verification_test.go |
| data (de calendário) | date | internal/buildinfo/buildinfo.go, internal/typeparse/datetime.go |
| dia | day | internal/types/types.go, internal/typeparse/datetime.go |
| somente-leitura | read-only | internal/storage/mmap/region.go |
| vazio | empty | internal/storage/mmap/region.go |
| exigir | require | internal/storage/mmap/region.go |
| mutável | mutable | internal/storage/mmap/region.go, internal/storage/indices_rebuild.go |
| imutável | immutable | internal/storage/indices.go, internal/storage/indices_rebuild.go, internal/index/segindex |
| bloco (de filesystem; e o mapa do YAML lido com whitelist de chaves) | block | internal/storage/mmap/region.go, docs/architecture.md, internal/schema |
| bloco (o pedaço varrido por vez numa passada sobre a região) | chunk | internal/storage/rowindex/dense32.go |
| meia-word | half-word | internal/storage/mmap/region.go |
| descritor (de arquivo; e o de tipo que o ResultSetMetaData publica) | descriptor | internal/storage/mmap/region_test.go, internal/protocol/jdbc.go |
| caminho | path | internal/gates, internal/storage/mmap/region.go, internal/storage/mmap/region_test.go |
| diretório (do filesystem) | dir | internal/query/spill, internal/storage |
| diretório (a tabela hash do WIP do índice por segmento) | directory | internal/index/segindex |
| antes / depois (medição de teste) | before / after | internal/memprof/memprof_test.go, internal/storage/mmap/region_test.go, internal/storage/fielddict/typehint_test.go |
| antes (o gancho que o dublê roda no instante do fsync, antes de delegar) | before sync | internal/storage/fielddict/typehint_test.go |
| anterior (o valor anterior) | prev | internal/query/lexer, internal/testutil/radiusgen, internal/buildinfo/buildinfo_internal_test.go |
| fundo (regra ou duração de fundo: a regra de TTL sem `when`) | fallback | internal/schema/ttl_rule.go, internal/storage/checkpoint/manifest.go |
| coluna | column | internal/counters, internal/storage/checkpoint/manifest.go |
| índice | index | internal/index, internal/storage/checkpoint/manifest.go |
| cauda (TLV do manifest) | tail | internal/storage/checkpoint/manifest.go |
| origem (do fluxo dos offsets Kafka; e a coluna de origem da congelada) | source | internal/storage/checkpoint/offsets.go, internal/schema, internal/config |
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
| amostra (de métrica; e as primeiras divergências que o relatório da auditoria nomeia; e a leitura do total de um grupo que o amostrador dobra no balde) | sample | internal/metrics, internal/gates/verification_test.go, internal/storage/counter, internal/countermetrics |
| família (de métrica) | family | internal/metrics, internal/gates/verification_test.go |
| série (de métrica) | series | internal/metrics, internal/gates/metrics_test.go |
| marcador (de item de lista) | bullet | internal/gates/verification_test.go |
| marcador (arquivo de sincronização entre dublê e teste) | marker file | internal/gates/rollback_update_test.go |
| elidido (literal sem tipo escrito) | elided | internal/gates/verification_test.go |
| prazo (de verificação do script de atualização) | timeout | internal/gates/rollback_update_test.go |
| derrubar (o servidor, no meio da corrida) | close | internal/gates/rollback_update_test.go |
| menor (predicado de ordenação) | less | internal/gates/verification_test.go |
| proibido (nome vetado por spec ou por gate) | banned | internal/logging/logging_test.go, internal/gates/jdbc_test.go, internal/query/executor/ndjson_test.go |
| composta / composto (chave de mais de um componente) | composite | internal/index/valuekey/composite.go |
| percorrer / percurso | walk | internal/index/valuekey/composite.go |
| visita / visitante (callback de varredura) | visit / visitor | internal/index/valuekey/composite.go, internal/storage/rowindex/schedule.go |
| fronteira (ponto de encontro entre dois componentes da chave) | boundary | internal/index/valuekey/composite_test.go |
| corte (ponto em que um buffer é truncado) | cut | internal/index/valuekey/composite_test.go, internal/storage/versions |
| corte (instante-limite de uma varredura) | cutoff | internal/storage/rowindex/schedule_test.go |
| sentinela (erro comparado por errors.Is) | sentinel | internal/index/valuekey/walk_test.go |
| booleano (o helper que monta o valor de TypeBool) | boolean | internal/index/valuekey/composite_test.go, internal/query/meta, internal/metakafka |
| largura (de payload, de posting ou de valor renderizado) | width | internal/index/valuekey, internal/index/segindex, internal/protocol |
| tamanho (contagem de bytes) | size | internal/index/valuekey, internal/index/segindex, internal/storage/fielddict |
| início | start | internal/index/valuekey, internal/index/segindex |
| fim (limite superior de faixa; a moldura que fecha o stream; e o fim de um run de spill) | end | internal/index/segindex, internal/protocol, internal/storage/rowindex, internal/query/spill |
| fim / pronto (o canal que avisa a goroutine que pode sair) | done | internal/metrics, internal/index/segindex |
| pronto (o canal fechado para soltar as goroutines juntas) | ready | internal/metrics |
| limite (o instante em que uma espera desiste) | deadline | internal/index/segindex/wip_test.go |
| registrar (instalar no registry: família de métrica, coletor ou tabela virtual) | register | internal/metrics, internal/query/engine.go |
| primeiro | first | internal/index/segindex, internal/metrics, internal/query/spill |
| dados | data | internal/index/segindex |
| arquivo | file | internal/index/segindex |
| versão | version | internal/index/segindex, internal/storage/versions |
| média | average / avg | internal/index/segindex, internal/storage/rowindex |
| achados / achou (o que uma busca ou varredura entregou) | found | internal/index/segindex, internal/storage/rowindex, internal/storage/versions |
| mínimo / menor (o mínimo de um conjunto) | min | internal/index/segindex, internal/storage/rowindex, internal/storage/versions |
| maior (o máximo de um conjunto; `max` está descartado por ser builtin, que o revive acusa quando sombreado) | largest | internal/index/segindex, internal/testutil/radiusgen |
| corrente / atual | current | internal/metrics/names.go, internal/index/segindex, internal/storage/fielddict |
| estado (do segmento no índice, do subsistema no /v1/health, a palavra do slot do WIP, ou o estado corrente da entidade que o Store devolve junto com a presença) | state | internal/metrics/names.go, internal/protocol, internal/index/segindex, internal/materializer, internal/testutil/fakestorage |
| gravado (valor escrito no arquivo) | stored | internal/index/segindex |
| calculado | computed | internal/index/segindex |
| cabeçalho (de registro em disco; e o preâmbulo do YAML que o helper de teste monta) | header | internal/storage/encoding, internal/query/spill, internal/config |
| folha (nó terminal da AST; e o nível 0 da árvore da agenda) | leaf | internal/query/parser, internal/storage/rowindex/schedule.go |
| corrompido / estragar (mutilar o arquivo no teste) | corrupt | internal/index/segindex, internal/protocol |
| apagar / apaga | remove | internal/index/segindex |
| montar (construir bytes ou estrutura) | build | internal/index/segindex/writer.go |
| intacto (buffer que nenhuma escrita alcançou) | unchanged | internal/storage/encoding/fields_test.go |
| não tocado / intocado (campo que o patch não mexeu e o merge copia sem decodificar) | untouched | internal/storage/encoding/fields.go, internal/materializer |
| ajuda (o texto do `# HELP` da métrica) | help | internal/metrics |
| medidor (de métrica) | gauge | internal/metrics |
| limites (do histograma: os limites superiores dos buckets, e a borda solta) | buckets / bound | internal/metrics |
| sozinha (a série única da família sem labels) | unlabeled | internal/metrics |
| filho / filhos (as séries de uma família, indexadas pela chave de labels; os nós filhos de um bloco da agenda; e o filho de uma chave no nó do YAML) | child / children | internal/metrics, internal/storage/rowindex/schedule.go, internal/config, internal/query/binder |
| ordem (de emissão: a fatia que fixa a ordem estável) | order | internal/metrics, internal/storage/counter |
| ordem (chave de comparação da agenda, com ∞ no lugar do sentinela) | order key | internal/storage/rowindex/schedule.go |
| exposição (o texto do formato de exposição do Prometheus) | exposition | internal/metrics/metrics_test.go |
| saída (o texto produzido, em teste) | output | internal/metrics/metrics_test.go |
| modo (de resolução do Lookup: índice ou enumeração; e o `merge` ou `replace` da materialização) | mode | internal/metrics/names.go, internal/schema |
| fora (o segmento que a janela de indexação deixou de cobrir; e o valor que vive fora do arena) | outside | internal/metrics/names.go, internal/query/binder, internal/schema |
| enumeração (resolver o segmento varrendo, sem índice) | enumeration | internal/metrics/names.go |
| consulta (do índice: o Lookup) | lookup | internal/metrics/names.go |
| desistiu (o watchdog esgotou as tentativas) | gave up | internal/metrics/names.go |
| pendente (o segmento dentro da janela ainda na fila de reconstrução; e a janela do arena ainda por materializar) | pending | internal/metrics/names.go, internal/query/binder |
| categoria (de falha de consulta) | category | internal/metrics/names.go |
| motivo (a categoria §101 do evento ruim que a ingestão recusou) | invalid reason | internal/metrics/names.go |
| motivo (da falha de autenticação HTTP Basic; e a explicação de por que um nome é reservado) | reason | internal/metrics/names.go, internal/schema |
| motor | engine | internal/metrics, internal/protocol, internal/query/engine.go |
| processo | process | internal/metrics, internal/protocol |
| metadados | metadata | internal/protocol |
| rota | route | internal/protocol, internal/server |
| desligado | disabled | internal/protocol |
| auditoria | audit | internal/protocol/counters.go |
| resposta | response | internal/protocol |
| requisição | request | internal/protocol |
| código (de erro do protocolo) | code | internal/protocol |
| tipo | type | internal/protocol, internal/types |
| tipo de wire (o nome de tipo que o cliente DECLARA em `Param.Type`, distinto do FieldType da coluna) | wire type | internal/protocol/params.go |
| forma (a espécie JSON do valor: string, objeto, lista, booleano, null, número) | shape | internal/protocol/params.go |
| qual (o discriminador que diz de que moldura se trata) | which | internal/protocol/stream.go |
| fecho (a linha que fecha o stream: fim ou erro) | closing | internal/protocol/stream.go |
| trecho (o pedaço que a saída ou a mensagem TEM de conter) | snippet | internal/gates, internal/protocol, internal/storage/fielddict |
| texto | text | internal/protocol, internal/query/spill |
| aceito | accepted | internal/protocol/params.go |
| recebido | received | internal/protocol/stream_test.go |
| falha (o fecho de erro do stream, que Reader.Failure expõe) | failure | internal/protocol/stream.go |
| falha (o objeto de erro que sai ANTES da primeira linha, valor da chave `error` de ErrorBody) | error detail | internal/protocol/protocol.go |
| desconhecido | unknown | internal/protocol |
| não suportado | unsupported | internal/protocol, internal/query/parser |
| reservado | reserved | internal/protocol |
| sintaxe | syntax | internal/protocol, internal/query/lexer |
| cheio (o endereço IPv6 com todos os bits em 1) | all ones | internal/protocol/jdbc_test.go |
| grande | large | internal/protocol |
| condição (expressão booleana solta declarada em configuração) | condition | internal/query/parser, internal/predicate, internal/counters |
| termo (cada comparação ou IS NULL contado no teto do WHERE) | term | internal/query/parser |
| altura (da árvore da AST) | height | internal/query/parser |
| palavra (de cláusula) | word | internal/query/parser |
| vez (a n-ésima ocorrência de um token na query, contada a partir de 1) | occurrence | internal/query/parser/positions_test.go |
| run (sequência de registros que o operador acumulador despeja em disco e relê em ordem) | run | internal/query/spill |
| limpeza (remoção de resíduo de disco na subida do dataset) | cleanup | internal/query/spill |
| conhecido (a entrada do índice já foi gravada, zero é desconhecido; e o campo já registrado no dicionário) | known | internal/storage/rowindex, internal/schema, internal/config |
| nascer / nasceu (esta abertura criou o arquivo, em vez de reencontrá-lo) | born | internal/storage/rowindex |
| zerar (apagar todas as entradas mantendo header e tamanho) | clear | internal/storage/rowindex/dense32.go |
| imortal (entidade que uma regra `never` declarou que não expira) | immortal | internal/storage/rowindex |
| nunca (o vencimento da linha que não expira: o ∞ codificado como zero na agenda) | never | internal/storage/rowindex/schedule.go |
| sumário (nó interno da agenda: limite inferior do mínimo dos filhos) | summary | internal/storage/rowindex/schedule.go |
| nó (entrada de um nível da agenda: folha ou sumário; e o nó do documento YAML) | node | internal/storage/rowindex/schedule.go, internal/schema, internal/config |
| topo (o nível mais alto da agenda, varrido por inteiro) | top | internal/storage/rowindex/schedule.go |
| antigo (o valor da folha ANTES desta atualização, oposto a "anterior" → prev; e o arquivo posto de lado por uma abertura com recreate) | old | internal/storage/rowindex/schedule.go, internal/storage/counter |
| vencido (linha cujo vencimento agendado já passou) | expired | internal/storage/rowindex/schedule.go |
| tocadas (as entradas em que a descida entrou) | touched | internal/storage/rowindex/schedule.go |
| descer / descida (a caminhada do topo às folhas da agenda) | descend / descent | internal/storage/rowindex/schedule.go |
| recomputado (o mínimo do nó recalculado na subida, comparado com o corrente) | recomputed | internal/storage/rowindex/schedule.go |
| alto (rowId muito além da capacidade inicial) | high | internal/storage/rowindex |
| renovação (empurrar o vencimento da linha para a frente) | renewal | internal/storage/rowindex/schedule_test.go |
| cadeia (a sequência de segmentos que a recovery percorre; a cadeia de erros embrulhados; e a cadeia de termos AND da AST) | chain | internal/storage, internal/query/binder |
| journal (o registro das locations anteriores que dá isolamento de leitura) | journal | internal/storage/versions |
| poda / podar (descartar do journal as versões que nenhum leitor alcança) | prune | internal/storage/versions |
| pinado / pin (segmento que guarda versão retida e por isso não pode ser aposentado) | pinned / pin | internal/storage/versions |
| escala (do gate de custo) | scale | internal/index/segindex |
| acesso (leitura do mapeamento contada pelo gate de custo, via w.countedRead — não o slot examinado) | access | internal/index/segindex |
| distintos (o número de valores distintos que ocupam o diretório) | distinct values | internal/index/segindex |
| razão (entre duas medidas) | ratio | internal/index/segindex |
| estratégia | strategy | internal/index/segindex/union.go |
| miolo (parte central do nome do arquivo) | middle | internal/index/segindex |
| vida (contagem de leitores que seguram o mapeamento) | ref | internal/index/segindex/sealed.go |
| desmontar (o mapeamento) | unmap | internal/index/segindex/sealed.go |
| fechar / fecha | close | internal/index/segindex, internal/query/spill |
| longe (rowId distante que força a largura 8) | far | internal/index/segindex |
| meio (da busca binária) | mid | internal/index/segindex/sealed.go |
| contexto (rótulo do caso na mensagem de falha) | label | internal/index/segindex |
| potência de dois | power of two | internal/index/segindex/wip.go |
| carga (do diretório) | load factor | internal/index/segindex/wip.go |
| deslocamento (do array de postings no rehash) | shift | internal/index/segindex/wip.go |
| último | last | internal/index/segindex/wip.go |
| queixa | complaint | internal/index/segindex/wip_test.go |
| parcial | partial | internal/index/segindex/wip_test.go |
| pior (caso) | worst | internal/index/segindex |
| melhor (a menor das medidas cronometradas) | best | internal/index/segindex |
| melhor (o mínimo de um conjunto de vencimentos, não uma medida cronometrada) | min | internal/storage/rowindex/schedule.go |
| decorrido | elapsed | internal/index/segindex |
| rodada | round | internal/index/segindex |
| par | pair | internal/index/segindex/wip_test.go |
| intruso (arquivo alheio no espaço de nomes) | intruder | internal/index/segindex/wip_test.go |
| vistas / visitas (quantas vezes a varredura visitou) | visits | internal/index/segindex/wip_test.go, internal/storage/rowindex |
| visitado | visited | internal/index/segindex, internal/index/valuekey |
| disjunta / disjunção | disjoint | internal/index/segindex/union.go |
| quebrada (fonte que falha) | broken | internal/index/segindex/union_test.go |
| rebobinar | rewind | internal/index/segindex/sealed.go |
| livre (o campo auto que ainda não teve o tipo fixado) | free | internal/schema |
| aninhado (a coluna promovida de uma chave de DENTRO de um documento json) | nested | internal/schema, internal/query/binder |
| pai (o campo json de onde a coluna aninhada é extraída; e o nó de mapeamento que contém a chave procurada) | parent | internal/schema, internal/query/binder, internal/config |
| relógio (a fonte de instante que o Store propaga e os testes congelam) | clock | internal/schema, internal/storage/counter |
| recusa (o erro devolvido pela política, ou o fato de a spec ter sido recusada, como substantivo) | refusal | internal/schema |
| ocorrência (quantas vezes a chave da coluna apareceu no documento) | occurrence | internal/schema |
| coerção (o valor que chegou noutra forma JSON e ainda assim entrou na coluna) | coercion | internal/schema |
| incompatível (o valor que não coube no tipo fixado da coluna) | incompatible | internal/schema, internal/query/binder |
| veio (o tipo que o EVENTO trouxe, oposto ao fixado) | arrived | internal/schema |
| tomado (nome já ocupado por outra coluna) | taken | internal/schema |
| indexado (cada autoridade consultada por si: o store ATENDE o índice daquele campo, ou o schema o DECLAROU — nunca as duas juntas) | indexed | internal/schema, internal/testutil/fakestorage, internal/query/planner |
| indexável (a CONJUNÇÃO das duas: o schema declara E o store atende; o termo pode virar lookup) | indexable | internal/query/planner |
| indexável (o TIPO declarado aceita `index: true`, decidido no load sem consultar store nem schema) | indexable type | internal/schema |
| qualificado / qualificada / qualificador (o "pai." que precede o nome da coluna) | qualified / qualifier | internal/schema, internal/query/binder |
| qualificado pelo schema (o "schema." que precede o nome da TABELA no FROM: endereço completo, que a configuração mantém único no servidor, distinto do atalho "_ns.") | schema-qualified | internal/query |
| grafia (a palavra escrita no YAML para o valor de uma chave) | spelling | internal/schema |
| visibilidade (por quanto tempo o grupo vazio continua visível) | visibility | internal/schema |
| duração | duration | internal/schema, internal/storage/counter |
| físico (o teto que a construção do formato impõe) | physical | internal/schema |
| agrupamento | grouping | internal/schema |
| colidir / colisão (dois nomes disputando a mesma entrada; dois listeners na mesma porta e na mesma interface) | collide / collision | internal/schema, internal/config |
| bom / ruim (o YAML aceito e o recusado, em teste) | good / bad | internal/schema |
| descoberta / descobrir (as colunas aninhadas que aparecem conforme os eventos trazem as chaves) | discovery / discover | internal/schema, internal/query/binder |
| obsoleto | obsolete | internal/schema |
| efetivo | effective | internal/schema, internal/query/binder, internal/config |
| prazo (do histórico: o expireAfterWrite da tabela irmã) | expiry | internal/schema |
| universo (o conjunto de campos que o dataset pode materializar) | universe | internal/schema |
| neta | grandchild | internal/schema |
| infinito (a janela sem fronteira, o default) | infinite | internal/schema |
| menor (a medida menor que outra, numa comparação) | smaller | internal/schema |
| mínimo (a forma mais enxuta de uma fixture que ainda é válida) | minimal | internal/schema |
| segundo / segunda (o segundo de uma sequência, ou a segunda de duas chamadas comparadas) | second | internal/schema, internal/query/binder |
| tentativa | attempt | internal/schema |
| porta (a porta TCP do bootstrap server ou do listener) | port | internal/schema, internal/config |
| inteiro | integer | internal/schema |
| embutido (o valor compilado no binário) | builtin | internal/schema |
| dono (o campo que declarou o alias) | owner | internal/schema |
| resolução (o passo do agendador de expiração) | resolution | internal/schema |
| declarada (a seção apareceu no documento, com qualquer valor de enabled) | declared | internal/config |
| habilitado / habilitada | enabled | internal/config |
| autenticação (o bloco `server.authentication` e os métodos que o consultam) | authentication | internal/config |
| endereço (host:porta de bind ou de listener) | addr | internal/config |
| exposto (bind que alcança fora do loopback) | exposed | internal/config |
| ignorada (chave aceita, sem leitor, que a subida nomeia; e a coluna que a política `ignore` do teto de campos descarta em silêncio) | ignored | internal/config, internal/schema |
| tópico (Kafka) | topic | internal/config, internal/kafka |
| unicidade (dois datasets não podem consumir o mesmo tópico com o mesmo `consumer.groupId` no mesmo cluster) | uniqueness | internal/config |
| segurança (os blocos `server.tls`, `server.authentication` e `server.loopback`, e a validação de FORMA deles) | security | internal/config, internal/server |
| usuário / usuários (uma linha da tabela de usuários do servidor) | user | internal/users, internal/server |
| tabela de usuários (a fonte única de quem autentica, em `<storage.path>/_system/users.state`) | user table | internal/users |
| credencial de bootstrap (o ADMIN gerado na primeira subida, com a senha num arquivo 0600) | bootstrap credential | internal/users |
| verificador (o cache de verificação por geração e o teto de derivações argon2id) | verifier | internal/users |
| geração (a versão em memória de uma linha da tabela de usuários, que invalida o cache de verificação) | generation | internal/users |
| último ADMIN (o que não pode ser removido nem rebaixado) | last admin | internal/users, internal/protocol |
| ator (quem pede a mutação na tabela de usuários, para a auditoria) | actor | internal/users |
| principal (quem o middleware autenticou naquele request) | principal | internal/server |
| gravação atômica (tmp → fsync → rename → fsync do diretório) | atomic write | internal/storage/atomicfile |
| documento (o documento YAML que o yaml.v3 entrega, cujo nó de topo é o DocumentNode) | document | internal/config |
| faltando (as chaves obrigatórias que o bloco ligado não trouxe) | missing | internal/config, internal/typeparse |
| caminho (a trilha de chaves dentro do documento YAML que a recusa nomeia) | key path | internal/config, internal/schema |
| limite (palavra geral) | limit | internal/types, internal/storage/counter |
| sob teste (a superfície comum que o dublê e o real precisam satisfazer, exercitada pelo mesmo roteiro) | under test | internal/testutil/fakestorage |
| real (o componente de verdade, oposto ao dublê) | real, sempre em nome composto (`realStore`, `realDict`, `openReal`), porque `real` nu é builtin predeclarado que o revive acusa quando sombreado | internal/testutil/fakestorage, internal/query/planner |
| limpar (desligar um bit que a operação Set irmã ligou) | unset | internal/testutil/fakestorage |
| pequeno | small | internal/testutil/fakestorage |
| conteúdo (os bytes de um valor, independentes do tipo que os rotula) | content | internal/testutil/fakestorage |
| usados (o conjunto de campos que a query REFERENCIA e o executor precisa coletar) | referenced | internal/query/binder |
| espinha (a cadeia à esquerda de AND/OR que o percurso iterativo consome) | spine | internal/query/binder, internal/query/planner |
| pilha (a pilha explícita do percurso iterativo) | stack | internal/query/binder |
| preencher (atribuir o índice do slot depois que o conjunto está fechado) | fill | internal/query/binder |
| limiar (o tamanho de projeção em que a checagem troca varredura por mapa) | threshold | internal/query/binder |
| repetido / repetição (nome que aparece mais de uma vez na projeção ou no ORDER BY) | duplicate | internal/query/binder |
| alias de projeção (rótulo por consulta, antes da numeração) | projection alias | internal/query/parser, internal/query/binder |
| nome final de saída (alias ou nome, com sufixo se repetido) | final projection name | internal/query/binder |
| célula (a entrada da tabela de comparabilidade coluna × tipo de literal) | cell | internal/query/binder |
| restrito (tipo cujo conjunto de operadores é limitado a `=` e `!=`) | restricted | internal/query/binder |
| ordenação (as chaves do ORDER BY resolvidas contra o schema) | ordering | internal/query/binder |
| acrescenta (quantas colunas o item da projeção acrescenta ao teto) | adds, em nome composto `addedCols`, porque `adds` nu lê como verbo ao lado do operando na soma e a contagem se anuncia como contagem | internal/query/binder |
| casar (o valor satisfaz o padrão de LIKE) | match | internal/query/binder |
| estrela (o curinga `%` do LIKE, e o `*` da projeção) | star | internal/query/binder |
| sublinhado (o curinga `_` do LIKE) | underscore | internal/query/binder |
| cópia / copiar (duplicar os bytes num array próprio) | clone | internal/query/binder, internal/storage/counter |
| reiniciar / reinício (a subida seguinte do dataset, com o dicionário já povoado) | restart | internal/query/binder |
| espião (dublê que falha o teste se um método proibido for chamado) | spy | internal/query/planner |
| candidatos (as rows que o plano oferece ao executor, sempre um superset do resultado) | candidates | internal/query/planner |
| penhasco (a fronteira exata em que o budget de flatten degrada o plano) | cliff | internal/query/planner |
| raso / fundo (a profundidade do termo na espinha de AND: perto do topo × lá embaixo) | shallow / deep | internal/query/planner |
| ruído (o termo não indexável que enche a espinha de AND) | noise | internal/query/planner |
| ordenado (a cópia já em ordem de uma fatia) | sorted | internal/query/planner |
| morta (linha com lápide; oposto de viva) | dead | internal/query/planner |
| promovido (o valor cujo tipo o binder subiu na comparação) | promoted | internal/query/planner |
| sintético (a estrutura montada à mão em teste, que o caminho normal não produz) | synthetic | internal/query/planner |
| degradar / degradado (cair para FullScan quando a dúvida impede o índice) | degrade / degraded | internal/query/planner |
| quando (o instante: a coluna do evento em teste e o instante passado a uma operação) | when | internal/query/planner, internal/storage/counter |
| recalculado (o valor que a varredura da auditoria recomputa) | recomputed | internal/storage/counter |
| mantido (o valor que o contador mantém na escrita, confrontado pela auditoria) | maintained | internal/storage/counter |
| internar / internação (dar id ao grupo no dicionário) | intern / interning | internal/storage/counter |
| agregado (o par de números do contador INTEIRO: linhas e total, colado aos vetores por grupo) | aggregate | internal/storage/counter |
| vetor (o vetor denso rowId → entrada, sobre rowindex.Dense32) | vector | internal/storage/counter |
| mexida (a transição que alterou um grupo do contador) | touch | internal/storage/counter |
| transição / transicionar (a mudança de estado de uma linha, de um grupo para outro) | transition | internal/storage/counter |
| definição (o contador como declarado no YAML e compilado contra o schema) | definition | internal/storage/counter |
| registrar (guardar o resultado de uma medição para as métricas) | record | internal/storage/counter |
| espia (a segunda instância aberta sobre os mesmos arquivos só para olhar o que está em disco) | peeker | internal/storage/counter |
| alarme (a condição declarada sobre a linha de um contador, com prazo de sustentação) | alarm | internal/alarm |
| sustentação / sustentar-se (o prazo pelo qual a condição tem de valer antes de disparar) | sustain / sustainedFor | internal/alarm, internal/schema |
| disparar / disparado (o alarme anunciado depois de a condição se sustentar) | fire / firing | internal/alarm |
| resolver / resolvido (o alarme que deixou de valer) | resolve / resolved | internal/alarm |
| pendente (a condição que vale e ainda não cumpriu o prazo) | pending | internal/alarm, internal/alarmtable |
| cronômetro (o instante em que a condição passou a valer, e o que dele se persiste) | tracker | internal/alarm |
| enriquecimento (o resultado das consultas que acompanham o evento de disparo) | enrichment | internal/alarm |
| ligar (dar valor a um parâmetro `?` da consulta de enriquecimento) | bind | internal/alarm, internal/query/binder |
| canal (o destino do evento de alarme: o log hoje, tópico e webhook depois) | sink | internal/alarm |
| lacuna (quanto tempo o processo ficou sem observar, reportada no alarme restaurado) | downtime | internal/alarm |
| retrato (a lista imutável de alarmes em observação, publicada a cada passada) | snapshot | internal/alarm, internal/alarmtable |
| aposentado (o grupo que saiu do contador, e o motivo de resolução que ele produz) | retired | internal/alarm, internal/storage/counter |
| emissão (a escrita periódica do valor de cada grupo num arquivo) | emission / emit | internal/series, internal/schema |
| série (o arquivo NDJSON com uma linha por grupo por intervalo) | series | internal/series |
| rotação / rotacionar (girar o arquivo de série ao cruzar o teto de tamanho) | rotate / rotation | internal/series |
| guardar (quantos arquivos rotacionados são mantidos) | keep | internal/series, internal/schema |
| métrica de janela (avg, min ou max do total de cada grupo numa janela de tempo, declarada no bloco `metrics:` do contador) | window metric | internal/schema, internal/counters |
| leitor da métrica (lê o valor publicado de uma métrica de janela) | MetricReader | internal/counters |
| fonte das métricas (resolve o leitor da métrica pelo nome) | MetricSource | internal/counters |
| contador listado (o que `_meta.counters` precisa de cada contador) | ListedCounter | internal/counters |
| uso da métrica (grupos, bytes em heap e em disco, instante da amostra) | usage | internal/counters |
| janela da métrica (o intervalo de tempo que a métrica de janela cobre; distinta da janela do rowindex) | metric window | internal/schema, internal/storage/metricwindow |
| agregado da janela (avg, min ou max sobre a janela da métrica; distinto do agregado do contador inteiro) | window aggregate | internal/schema |
| balde (cada uma das fatias de largura igual em que a janela da métrica é dividida, e onde as amostras de um grupo são dobradas) | bucket | internal/schema, internal/storage/metricwindow |
| largura do balde (a janela da métrica dividida pela quantidade de baldes) | bucket width | internal/storage/metricwindow |
| slot (a coluna do arquivo da janela da métrica que guarda um balde) | slot | internal/storage/metricwindow |
| época do balde (o número do balde desde a época Unix: unixMs / largura) | epoch | internal/storage/metricwindow |
| identidade (o hash da chave do grupo dono de uma posição da métrica e a época a partir da qual os baldes da posição são dele) | identity | internal/storage/metricwindow |
| capacidade (quantas posições da métrica o arquivo da janela endereça; não é o número de ids de grupo) | capacity | internal/storage/metricwindow |
| marca de dono (os 16 bits do hash da chave gravados em cada balde) | owner tag | internal/storage/metricwindow |
| amostrador (quem lê o total de cada grupo a cada intervalo e dobra a amostra no balde aberto de cada métrica de janela) | sampler | internal/countermetrics |
| posição da métrica (o índice denso da entrada de um grupo VIVO no arquivo da janela e no heap do amostrador, reaproveitado quando o grupo se aposenta; distinta do id do grupo, do slot de balde e da posição de consumo do Kafka) | position | internal/countermetrics, internal/storage/metricwindow |
| mapa de posições (o mapa id do grupo → posição da métrica, um por contador, compartilhado pelas métricas) | position map | internal/countermetrics |
| dono da entrada (o id do grupo que ocupa a posição da métrica, conferido pelo leitor dentro do seqlock) | owner | internal/countermetrics |
| agregado publicado (avg, min e max de um grupo, publicados para leitura concorrente sem lock) | published aggregate | internal/countermetrics |
| seqlock (a publicação em que o escritor marca o número de sequência ímpar enquanto grava e o leitor relê se o número mudou) | seqlock | internal/countermetrics |
| releitura (a volta do leitor do seqlock quando a sequência mudou ou estava ímpar) | retry | internal/countermetrics |
| balde aberto (o balde da época corrente, ainda em heap, gravado no arquivo só quando a época vira ou no Stop) | open bucket | internal/countermetrics |
| baldes fechados (o cache em heap da soma, contagem, mínimo e máximo dos baldes gravados que ainda estão na janela) | closed buckets | internal/countermetrics |
| adotar (o amostrador assumir uma posição para um dono novo: identidade nova antes de qualquer balde, balde aberto anterior descartado) | adopt | internal/countermetrics |
| órfão da métrica (o arquivo `.win` de uma métrica que saiu do YAML, removido na abertura) | orphan | internal/countermetrics |
| restauração da janela (a publicação dos agregados a partir dos baldes gravados, na abertura, antes da primeira amostra) | restore | internal/countermetrics |
| saturação da contagem (a contagem do balde que para em 65.535, com a soma parando junto para o avg continuar exato) | count saturation | internal/countermetrics |
| bloco de entradas da métrica (o pedaço de tamanho fixo do estado em heap do amostrador, que nunca se move; distinto do bloco varrido numa passada sobre a região) | chunk | internal/countermetrics |
| coluna gerada (a coluna `<métrica>_<agregado>` que uma métrica de janela produz na tabela do contador) | generated column | internal/schema |
| coluna fixa (`groupKey` e `total`, presentes em toda tabela de contador) | fixed column | internal/schema, internal/counters |
| origem da coluna (o trecho do contador, colunas fixas, `groupBy` ou `metrics[i]`, que produz a coluna) | origin | internal/schema |
| qualificação do alarme / qualificar (a condição `qualifyWhen` que habilita o alarme para o grupo; distinta do qualificador de coluna) | qualify / qualifyWhen | internal/schema, internal/alarm, internal/alarmtable, internal/storage/checkpoint |
| qualificado (o grupo elegível ao alarme porque o `qualifyWhen` vale, ou não foi declarado) | qualified | internal/alarm |
| desqualificado (o alarme disparado que resolve porque o `qualifyWhen` deixou de valer; motivo distinto de `condition_cleared` e `group_retired`) | disqualified / ReasonDisqualified | internal/alarm |
| condição de qualificação (o texto do `qualifyWhen` levado ao evento e à view de alarmes) | QualifyCondition | internal/alarm, internal/alarmtable |
| impressão das métricas (o texto canônico das métricas de janela citadas pela condição, gravado no estado para invalidar o cronômetro quando a janela muda) | metrics fingerprint / MetricsFingerprint | internal/alarm, internal/storage/checkpoint |
| diretório das métricas de janela (o `counter_metrics/` dentro de `indexes/`, irmão do diretório dos contadores, com um `<contador>/<métrica>.win` por métrica) | counter metrics directory / CounterMetricsDir | internal/storage, internal/server, internal/cli |
| amostragem (o laço do amostrador de pé, e a série que diz se ele está) | sampling | internal/server, internal/metrics |
| view de alarmes (a tabela `<schema>.alarms`, atalho `_counters.alarms`, que lista os alarmes em observação no schema de todo dataset) | alarms view / alarmtable | internal/alarmtable, internal/server, internal/cli |
| nome reservado (o nome `alarms`, recusado para contador, dataset e tabela histórica porque a view de alarmes o ocupa em todo schema) | reserved name / ReservedAlarmsTable | internal/schema |
| coluna do valor (a coluna da tabela do contador, escolhida pela chave `value:` do alarme, cujo valor o disparo guarda; `total` por default) | value column / valueColumn | internal/schema, internal/alarm, internal/alarmtable |
| valor de disparo (o valor da coluna do valor lido no payload que decidiu o disparo, congelado enquanto o alarme dispara) | firing value / firingValue | internal/alarm, internal/alarmtable |
| instante do disparo (quando o disparo foi anunciado; distinto do `since`, que é quando a condição passou a valer) | fired at / firedAt | internal/alarm, internal/alarmtable |
| pacote Debian (o `.deb` de cada arquitetura que a release publica ao lado do binário solto; layout FHS, distinto do layout de symlink do piloto) | Debian package / deb | scripts/build-deb.sh, packaging/deb, internal/gates |
| empacotador (o `scripts/build-deb.sh`, que monta o pacote Debian com dpkg-deb a partir do binário que `make build` emitiu) | packager / build-deb | scripts/build-deb.sh, Makefile |
| scripts de mantenedor (`postinst`, `prerm` e `postrm`, que o dpkg executa na instalação, no upgrade, na remoção e na purga) | maintainer scripts | packaging/deb, internal/gates |
| arquivo de configuração do pacote (o `/etc/nishidb/nishidb.yaml` listado em `conffiles`, que o dpkg preserva quando o operador o editou e não entra no `md5sums`) | conffile | scripts/build-deb.sh, internal/gates |
| dublê de comando (o executável de mentira que o sandbox de PATH dos gates põe no lugar de `systemctl`, `useradd` e afins, só registrando a chamada) | command stub / stubTools | internal/gates |

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

Escolhas que o juiz do G2 fechou e que valem para os grupos seguintes:

- **entrada, um inglês só.** Três pacotes chegaram com sentidos novos de
  "entrada" — o item de uma tabela de seções (`internal/index/segindex`), a
  célula do vetor denso endereçada por rowId (`internal/storage/rowindex`) e a
  linha do journal de versões (`internal/storage/versions`) —, e todos querem
  `entry`. Em vez de três linhas com o mesmo inglês, que a regra de colisão
  proíbe, a linha de "entrada (de diretório)" foi AMPLIADA para cobrir os
  quatro sentidos. "entrada (de teste) → input" continua separada, porque ali
  o inglês é outro.
- **gravado: stored ≠ covered.** Em `internal/index/segindex`, "gravado" é o
  valor que ESTÁ no arquivo, confrontado com o recalculado, e vira `stored`. O
  `gravado` de `rowindex.Dense32.Get` é outra coisa e o teste denuncia: o bool
  é `false` só quando o rowId cai ALÉM do arquivo, e entrada nunca gravada sai
  com `true`. Ele vira `covered`, pela operação irmã `Cobre` → `Covers`;
  traduzi-lo por `stored` carimbaria a mentira que o teste existe para expor.
- **trecho → snippet, nunca want.** `internal/storage/fielddict` e
  `internal/protocol` têm o mesmo laço que `internal/gates`: cada pedaço que a
  saída ou a mensagem TEM de conter, conferido por `strings.Contains`. `want`
  é o esperado por IGUALDADE (`got != want`), e usá-lo aqui apagaria a
  diferença entre "é isto" e "contém isto"; entre `snippet` e `fragment` vence
  o que o G0/G1 já tinha fixado em `internal/gates`, porque o critério é um
  termo PT-BR, um termo inglês.
- **nome errado é corrigido, não traduzido.** Dois casos do G2 só se
  explicavam com o verbo da operação irmã. `metrics.Registry.Coletor` é
  substantivo, mas faz o que a função `register` faz para as famílias —
  instalar no registry —, e `Collector` daria a um MÉTODO o nome do TIPO que
  ele recebe: vira `Register`. `protocol.Leitor.Erro` é o fecho de FALHA da
  consulta, e existe só para não ser confundido com `Err()`, a falha de
  TRANSPORTE; o campo que ele expõe já se chama `falha`, então o acessor vira
  `Failure` — `Error` poria os dois a uma letra um do outro e anunciaria uma
  interface que o tipo não implementa.
- **teto → ceiling vence o valor de wire.** `CodMaxRowsAcimaDoTeto` vira
  `CodeMaxRowsAboveCeiling` mesmo com o valor `MAX_ROWS_ABOVE_LIMIT` do wire
  intacto: a linha `teto → ceiling` está fechada desde o G1 e o juiz não a
  contradiz. Identificador e valor já divergiam em `CodMetodo`
  (`METHOD_NOT_ALLOWED`) e `CodCorpoGrande` (`REQUEST_TOO_LARGE`).
- **vez → occurrence, porque → reason.** Os dois termos entraram na limpeza
  dos campos de struct anônima que o inventário não enxergava (as tabelas de
  caso de teste). "vez" é a n-ésima ocorrência de um token dentro da query que
  `positionOf` procura — `nth` diria o ordinal e não a coisa contada. "porque"
  é a justificativa que o caso carrega para a mensagem de falha; `reason` já é
  o inglês do "motivo" da falha de autenticação, e a tabela já tem precedente
  de dois termos PT-BR com o mesmo inglês (`soltar` e `liberar` → `release`) —
  o que a regra de colisão proíbe é o contrário, dois sentidos do MESMO termo
  PT-BR virarem a mesma palavra.
- **run fica run.** O `Arquivo` de `internal/query/spill` é definido pelo
  godoc como "UM run", e todo o pacote fala assim (o `runs` do executor, a
  mensagem `spill: criar run %s`, o nome do teste). `File` traduziria o
  SUPORTE e obrigaria o godoc a explicar o tipo com uma palavra que não é o
  nome dele; a tabela já tem precedente de linha com os dois lados iguais
  (`gate`).
- **mensagem de falha: o que NOMEIA muda, o que DESCREVE fica.** Dentro de um
  `t.Errorf`, a palavra que nomeia um retorno nomeado ou uma variável do teste
  é CITAÇÃO de identificador e acompanha o rename — se `achou` vira `found`, a
  mensagem que diz `achou = true` também vira. A palavra que descreve um
  retorno ANÔNIMO não nomeia nada que exista no código: é prosa, e prosa fica
  em PT-BR. A distinção é operacional, não estética: a mensagem que nomeia é
  conferível contra a assinatura, e mantê-la em PT-BR mandaria o leitor
  procurar um identificador que não existe; traduzir a que descreve inventa um
  nome que a assinatura não tem.

Escolhas que o juiz do G3 fechou e que valem para os grupos seguintes:

- **indexado ≠ indexável.** São duas perguntas diferentes e o código já as
  separava: `Store.Indexado` (o grupo de interface G15) pergunta se o STORE
  atende o índice daquele campo, e `CompiledField.Indexado` diz se o SCHEMA o
  declarou — os dois viram `Indexed`. O `Planner.indexavel` é a CONJUNÇÃO dos
  dois, e o godoc dele existe só para registrar que um sem o outro não basta:
  ele vira `indexable`. Dar a ambos o mesmo inglês apagaria a distinção que o
  godoc existe para guardar. Pelo MESMO critério, o `indexavel` de
  `internal/schema` não é a conjunção e não pode levar `indexable`: ele é
  predicado puro sobre o NOME DE TIPO declarado, decidido no load
  (`bitmapIndexable(t) || hashIndexable(t)`, `spec.go:464`), sem consultar
  store nem `index:`. Vira `isIndexableType`, com linha própria qualificada na
  tabela — dois sentidos do mesmo termo PT-BR nunca recebem o mesmo termo
  inglês (§4). O prefixo `is` não é enfeite: `indexableType` é sintagma
  nominal de núcleo `Type` e o pacote já tem uma função com esse núcleo que de
  fato DEVOLVE o tipo (`tipoIndexavelDoHistorico → historyIndexableType`,
  `history.go:447`); duas funções com o mesmo núcleo, uma devolvendo o tipo e
  a outra um booleano, é a mentira de forma que o bullet "voz do verbo é
  significado" existe para fechar. O termo `indexable type` fica inteiro
  dentro do nome, e os irmãos `bitmapIndexable`/`hashIndexable` (`spec.go:450`
  e `:454`) seguem com o adjetivo como núcleo.
- **usados → referenced.** Terceiro sentido de "usado" no repositório, depois
  de "reserva" (`internal/query/budget`, `used`) e "ocupação" (o `ordenador` do
  executor, ainda por nomear). O `usados` do binder é o conjunto de campos que
  a query REFERENCIA e que o executor precisa coletar; `referenced` é o termo
  próprio que o ruling do G1 já previa para o terceiro sentido.
- **recusa: o ato ≠ o erro.** `recusar` (o verbo) continua `refuse`, e a linha
  antiga foi qualificada como "o ato de recusar". O substantivo — o erro que a
  política do teto devolveu, o fato de a spec ter sido recusada — ganha
  `refusal`, que é o que `requireRefusal` e `createUnknown.refusal` nomeiam.
- **cópia → clone.** `copy` nu é builtin e o revive acusa quando sombreado,
  pelo mesmo motivo já registrado para `max`. O helper do binder vira `clone`,
  a forma que `bytes.Clone` consagra; em nome composto o termo continua sendo
  o mesmo, e o `copia` do dicionário do contador vira `keyClone`.
- **espião → spy convive com dublê → fake.** Não são o mesmo papel: `fake` é o
  dublê que RESPONDE (o `fakestorage.Store`), `spy` é o que DENUNCIA — o
  `espiao` do planner chama `t.Fatalf` se o planner tocar em dados no
  plan-time. O pacote usa os dois lado a lado, com o fake DENTRO do spy.
- **nome errado é corrigido, não traduzido** (regra do G2, seis casos no G3).
  `preenche` vira `seedField`, porque o godoc a define como o estado que uma
  execução anterior deixou no dicionário — é semear a fixture, e `fill` não
  diria o quê. `espelhaExtracoes` vira `cloneExtracts`, porque o corpo é um
  `make`+`copy` e `mirror` a confundiria com a máquina de espelhos do
  `Compiled` — o verbo corrigido é o do godoc ("copia as extrações do campo
  pai"), e o inglês dele é o `clone` da linha `cópia / copiar`, que o bullet
  `cópia → clone` acima fecha também para nome composto.
  `Compiled.Espelha` vira `AddMirror`, porque `Mirror(m)` leria como "c vira
  espelho de m", o inverso do que faz. `Config.SpecDoDataset` vira
  `DatasetSpecPath`, porque devolve o CAMINHO do YAML e `DatasetSpec` é o nome
  de um tipo que ele não devolve. `Store.LimpaTombstone` vira
  `UnsetTombstone`, e NÃO `ClearTombstone`: `Set`/`Unset` é par tão idiomático
  quanto `Set`/`Clear` para bit de flag, e `clear` está tomado duas vezes —
  pela linha `zerar (apagar todas as entradas mantendo header e tamanho) →
  clear`, que o G2 fechou e `rowindex.Dense32.Clear` já usa, e pela fase de
  CLEAR de postings que o índice por segmento NÃO tem e que o godoc do próprio
  dublê repete (`fakestorage.go:240` e `:360`, `indices.go:347`). Um
  `ClearTombstone` que não limpa posting nenhuma, num arquivo cujo texto
  normativo diz que não há fase de CLEAR, é a classe "texto normativo que
  mente" criada pelo rename. A linha `limpar` passa a `unset`. E
  `semEsseBitmap` vira `withoutThatIndex`: "bitmap" nomeava um dos dois
  motores que a D5 colapsou num só, e traduzir a palavra carregaria para o
  inglês um motor que este binário não tem.
- **limite (palavra geral) → limit ganha linha.** O ruling do G1 vivia só na
  prosa; com `Counter.Limite`, `Dict.Limite` e o `Leitor.Limite` do grupo G17
  todos virando `Limit`, a linha entra na tabela para que a decisão seja
  legível como dado, sem contradizer `teto → ceiling`.
- **numeral por extenso não é sufixo numérico.** `ceilingIntersectTwo` e
  `ceilingUnionThree` carregam a ARIDADE do caso medido, que é parte do nome —
  diferente do sufixo numérico que a §6 proíbe, que serve para desempatar duas
  traduções. A convenção do arquivo já era essa (`quatroConjuncts`).
- **`counter.Counter` não estuta.** O `checkStutter` do revive só acusa quando
  o nome do tipo é MAIOR que o do pacote (`len(name) <= len(pkg)` devolve
  cedo), que é o mesmo caso de `list.List` na stdlib. `contador → counter`
  vale sem exceção.
- **grupo de interface sem rename fica FORA do TSV de grupos.** O
  `ifacerename` recusa o lote quando o receptor já tem um método com o nome
  novo, e um grupo cujo fecho inteiro já diz `Put` cairia exatamente nessa
  guarda. Os quinze grupos do G3 que não mudam de nome (G2, G4–G8, G12–G14,
  G16, G28–G32, G35, G45–G50) entram nos mapeamentos como "mantido", com a
  nota de que o TSV de grupos fica sem linha para eles.
- **"já em inglês" não isenta builtin.** Quatro locais de
  `internal/testutil/fakestorage` tinham entrado como "mantido: já em inglês"
  chamando-se `real`, e o revive os acusa HOJE nos quatro
  (`golangci-lint run ./internal/testutil/fakestorage/...` aponta
  `redefines-builtin-id` em `behavior_test.go:191`, `dict_contract_test.go:36`
  e `:83`, e `parity_test.go:125`). `max`, `new` e `copy` também eram inglês e
  foram descartados pelo mesmo critério. Eles viram `realStore` e `realDict`, a
  forma composta que `abreReal → openReal` já usava — e fechar os quatro derruba
  a baseline de lint de 368 para 364, que tem de baixar no MESMO PR, porque o
  gate falha também para BAIXO.
- **contagem se anuncia como contagem.** `Compiled.CamposConhecidos` devolve
  `int` e vira `KnownFieldCount`, não `KnownFields`; `Relatorio.Divergentes`
  conta GRUPOS e vira `DivergentGroups`, deixando o núcleo `Divergences` para o
  somatório que `RegistraAuditoria` recebe. Plural nu anuncia coleção e apaga a
  diferença entre a parcela e o total — que é justamente o que o godoc de
  `AgregadosDivergentes → DivergentAggregates` existe para guardar. A mesma
  regra corrige quatro nomes que a primeira passada do G3 tinha dado ao plural
  nu: `Compiled.CamposDescartados` e `Compiled.ValoresIncompativeis` devolvem
  `uint64` e viram `DiscardedFieldCount` e `IncompatibleValueCount` (com os
  campos privados atrás deles), e os dois `derivadas` do binder são `int` e
  viram `derivedCount` — em nenhum dos três o código guarda a coleção que o
  plural prometia. A regra vale igual para a PARCELA por coluna, que a
  primeira passada tinha deixado no plural nu: `CompiledField.Ocorrencias`,
  `.Divergencias` e `.Coercoes` devolvem `uint64` sobre três `atomic.Uint64` e
  viram `OccurrenceCount`, `DivergenceCount` e `CoercionCount` (com os campos
  privados atrás deles) — o mesmo `ContaValorIncompativel` alimenta a parcela e
  o total, e anunciá-los por formas opostas é exatamente o que o ruling apaga.
  Os verbos que falam de UMA ocorrência (`ContaOcorrencia → CountOccurrence`,
  `ContaCoercao → CountCoercion`) ficam no singular. Pelo mesmo critério, o
  nome tem de anunciar o que a função DEVOLVE: `CamposComIndice` devolve
  `[]string` e vira `IndexedFieldNames`, não `IndexedFields` — em
  `internal/schema` todo `Fields*` devolve `[]*CompiledField`, e as três irmãs
  de `[]string` já levam o marcador `Names` (`columnNames`, `frozenNames`,
  `DerivedColumnNames`) —, e o `const colunas = 200` dos dois testes de
  projeção grande do binder é `int` e vira `columnCount`, para não ficar a uma
  inicial de distância do `bq.Columns` que a mesma expressão compara.
- **voz do verbo é significado.** `TetoIgnorou` é ATIVO em PT-BR — o teto, que
  foi honrado, ignorou a coluna —, e `CeilingIgnored` diria o contrário (o teto
  desrespeitado): vira `IgnoredByCeiling`, com o teto como agente. Pelo mesmo
  critério um PREDICADO leva `is` (`repetido` → `isDuplicate`, ao lado de
  `ehSoPorcento` → `isAllStars`), porque `duplicate` em posição de função lê
  como o verbo "duplicar", e um método que só marca estado leva o verbo
  (`binder.usa` → `markReferenced`, não `reference`, que leria como acessor e
  disputaria a raiz com a `parser.ColumnRef` do mesmo corpo). O núcleo daquele
  predicado é `star`, e não `percent`: a linha de "estrela" já fecha o inglês
  do curinga para ESTE pacote, `glob.estrelaP`/`estrelaI` → `starP`/`starI` o
  usam 24 linhas abaixo no mesmo arquivo, e um segundo inglês para o mesmo
  termo PT-BR é o que a §4 proíbe — no resto do repositório `percent` só
  significa porcentagem.
- **caminho ≠ diretório vale também dentro do mesmo tipo.**
  `Config.SpecDoDataset` ficou `DatasetSpecPath` porque devolve o caminho de um
  ARQUIVO; por isso `Config.CaminhoDoDataset`, que devolve um DIRETÓRIO — e o
  MESMO valor de `DirDoDataset` —, não pode virar `DatasetPath`. Também não
  pode virar `DatasetDataDir`, ainda que seja a tradução literal do godoc: o
  valor é a RAIZ do dataset, e `data/` é um dos cinco filhos que o storage cria
  dentro dela (`store.go:1021-1025`), com o identificador `dataDir` já ocupado
  por esse filho em código de produção (`compaction.go:495`, `indices.go:35`) —
  `DatasetDataDir` ao lado de `DatasetDir` afirmaria uma hierarquia que é falsa
  aqui e verdadeira no storage. Vira `DatasetRootDir`, que continua dizendo
  DIRETÓRIO sem reivindicar o filho. As duas serem duplicata funcional é débito
  de limpeza (§2, "o que aparecer vira débito"), não licença para inventar no
  nome uma distinção que o código não faz.
- **congelada (a coluna) ≠ congelado (o retrato do contador).** O
  `counter.Congelado` é a cópia COERENTE dos números num instante — "o retrato
  T0 ... todos do MESMO instante" (`counter.go:561-566`) —, e não a coluna que
  para de mudar na escrita. Dar-lhe `Frozen` poria o mesmo inglês em dois
  sentidos do mesmo termo PT-BR (§4), e `[]counter.Frozen` seria lido como "as
  congeladas do contador" num binário em que `Frozen` é a feature de produto.
  O tipo vira `AuditSnapshot` e `Contador.Congela` vai junto: `Freeze`
  anunciaria que o contador PARA de contar, e ele não para — o método tira a
  cópia sob o `wmu` e a escrita segue. A linha `congelada` fica qualificada à
  coluna, e `snapshot` ganha o segundo referente na linha de `instantâneo`.
- **adjetivo nu anuncia predicado.** `Compiled.Congeladas` é
  `[]*CompiledField` e vira `FrozenColumns`, não `Frozen`: a pergunta booleana
  correspondente EXISTE (`FrozenFrom != nil || DoRelogio`), e ao lado de
  `CompiledField.Indexado → Indexed` o adjetivo nu deixaria de discriminar o
  bool da coleção — é o argumento que `CompiledField.Derivadas →
  DerivedColumns` já tinha registrado no mesmo arquivo. Em posição de MÉTODO o
  adjetivo solto erra a voz: `binder.qualificada` CONSOME uma referência que já
  vem qualificada do parser, e `b.qualified(cols, ref, …)` diria que o método
  qualifica os argumentos; vira `qualifiedProjection`, com `qualified` de volta
  a modificador e o produto no nome, ao lado de `maybeQualifiedColumn`.
- **`sem → without`, e política não é fato.** O `sem` fechado em
  `semEsseBitmap → withoutThatIndex` vale para o grupo inteiro: o `semIndex` do
  `fakestorage` vira `withoutIndexID`, com o sufixo `ID` dos irmãos `statusID`
  e `nameID`. Onde o `sem` não é ausência e sim POLÍTICA, o nome muda de
  família: `binder.semParams` marca que AQUELE caminho de bind RECUSA parâmetro
  posicional — `Bind` nunca o liga, nem com zero parâmetro —, então `noParams`
  afirmaria um fato falso e ficaria a uma letra do `nParams` da mesma struct.
  Vira `refusesParams`, pelo `recusar → refuse` já fechado. "Vale para o grupo
  inteiro" é literal, e corrige quatro nomes que a primeira passada tinha
  mandado para a família `no*`: os três locais do planner são ausência de fato
  — `semWhere` é a query SEM WHERE, `semStore` é `planner.New(nil, m)` e
  `semMetrics` é `New(…, nil)`, os dois declarados aceitos pelo godoc de `New`
  —, e viram `withoutWhere`, `withoutStore` e `withoutMetrics`; e o
  `counter.SemGrupo`, sentinela zero cujo godoc é "a entrada do vetor de uma
  linha que não está em grupo nenhum", vira `WithoutGroup`. Dois ingleses para
  o mesmo `sem` dentro do MESMO pacote (`withoutThatIndex` ao lado de
  `noWhere`) é o que a regra existe para impedir.
- **termo cunhado precisa de linha antes do uso, inclusive em dublê.** O
  `escritor` do `fakestorage` vira `writerUnderTest`, não
  `writeSurfaceUnderTest`: "superfície" não tem linha na tabela (a linha `sob
  teste` só a usa dentro do qualificador), e a §4 exige o termo na lista ANTES
  do código. A raiz `writer` continua fechada e não colide — a única ocorrência
  da palavra nos dois pacotes é PROSA, e sobreposição de comentário não é o
  clash de namespace que corrigiu `protocol.Leitor.Erro → Failure`. A forma
  composta é a mesma de `realStore`/`realDict`, e faz as três irmãs
  (`storeUnderTest`, `dictUnderTest`, `writerUnderTest`) se lerem juntas.
- **o NOME da coluna leva `Name`; o campo compilado leva `Field`.** As três
  constantes de `internal/schema` guardam a STRING do nome de uma coluna de
  sistema, e o `*CompiledField` correspondente é outro identificador, a poucas
  linhas dali (`if name == CampoInstanteDeEscrita { … c.Escrita = cf }`,
  `compile.go:554-559`). Separá-los por transposição — `FieldWriteInstant`
  contra `WriteInstantField`, as mesmas duas palavras em ordem trocada — é o
  arranjo que o ruling do G2 sobre `protocol.Leitor.Erro → Failure` já
  recusou, e nenhum dos dois marcadores dizia o que a constante é. As
  constantes viram `WriteInstantFieldName`, `TTLFieldName` e `RawFieldName`, e
  `Compiled.Escrita` e `Compiled.Prazo` ficam com `WriteInstantField` e
  `TTLField`, agora sem ambiguidade.
- **valor de enum de YAML se nomeia `<chave><valor>`.** `OrigemRelogio` é o
  literal `"@writeTime"` que a chave `frozenFrom` aceita, e `ClockSource` erra
  duas vezes: `source` neste pacote já é a fonte de EVENTOS
  (`DatasetSpec.Source`, `SourceSpec`, `SourceKafka`), então `ClockSource` ao
  lado de `SourceKafka` se leria como um segundo valor de `source.type`; e a
  linha `relógio (a fonte de instante que o Store propaga)` já traz "fonte"
  dentro da definição, o que faz de `ClockSource` uma tautologia que anuncia
  um provedor de instante. Vira `FrozenFromClock`, pela convenção que o
  próprio pacote usa (`SourceKafka`, `AutoOffsetResetEarliest`,
  `InvalidEventStop`, `OnMaxColsStop`) e que casa com `CompiledField.DoRelogio
  → FromClock`.
- **`want` é igualdade; `snippet` é conteúdo — inclusive com qualificador.** O
  ruling do G2 (`trecho → snippet, nunca want`) não abre exceção para campo de
  tabela de casos: o `naFalha` de `internal/config` é consumido UMA vez, em
  `strings.Contains(err.Error(), "é "+caso.naFalha)`, e nunca por igualdade.
  Vira `valueSnippet` — o qualificador `value` guarda o que o motivo original
  queria proteger (é o VALOR renderizado pelo `%v`, não a frase da faixa) e o
  núcleo continua sendo o termo fechado. Mesma tabela, mesmo laço e mesmo
  papel que o `snippet` de `internal/protocol/params_test.go:101`.
- **a default EFETIVA não se chama como a constante.** Em `internal/config`, o
  local que a `CarregaConfig` calcula não é a default do sub-pool: é a default
  já limitada pelo pool (`padraoLongo := maxLongRunningPadrao; if padraoLongo
  > c.maxConcurrent { … }`), e o comentário de nove linhas acima e o
  `longquery_internal_test.go` existem só para guardar essa diferença.
  `defaultLongRunning := defaultMaxLongRunning` poria duas palavras a uma
  partícula de distância, com o `Max` do lado que justamente NÃO é limitado, e
  dropar o `Max` ainda desligaria o nome da chave
  `runtime.query.maxLongRunning` de que ele é a default. Vira
  `effectiveDefaultLongRunning`, e a linha `efetivo → effective` é ampliada
  para `internal/config`.
- **um só nome por objeto dentro do pacote.** Em `internal/query/planner`,
  `Key` significa o VALOR da chave e `Field` significa a COLUNA — é o contrato
  do tipo público (`PointLookupPlan{Field *schema.CompiledField; Key
  types.Value}`) —, e o local que segura o `*CompiledField` de `id` não pode
  ser `key`: vira `keyField`. Pelo mesmo critério o campo string que guarda a
  coluna reapresentada no binder vira `duplicateColumn`, deixando `duplicate`
  nu para os dois booleanos do mesmo arquivo; e o local de
  `ValidaUnicidadeDeGroup` (`internal/config`) que hoje se chama `o`, inicial
  do tipo `origem`, não pode ficar "mantido: já em inglês" quando esse tipo
  vira `sourceKey` — o mnemônico morre com o rename, e a variável vira `key`,
  o termo fechado.
- **verbo imperativo afirma a ação; se o método só registra, o verbo é
  `record`.** `Contador.CobreMexidas` não cobre nada — quem cobre é `segSync →
  ApplyOverlay → msync`, e o godoc diz "REGISTRA que todas as transições até n
  estão duráveis" —, então `CoverTouches` afirmaria o que o corpo (uma
  marca-d'água monotônica) não faz, e ficaria a uma letra do predicado
  verdadeiro `Covers` no MESMO receptor. Vira `RecordCoveredTouches`, pelo
  `registrar (guardar o resultado de uma medição) → record` que o pacote já
  usa em `RegistraAuditoria → RecordAudit`, e o getter `MexidasCobertas` fica
  `CoveredTouches` por si, sem depender do verbo do irmão. Pelo mesmo critério
  `AposentaVazios` vira `RetireEmptyGroups`: `Empty` nu é adjetivo sem núcleo,
  e o núcleo `groups` já está estabelecido nas duas superfícies vizinhas
  (`Definicao.RetainEmptyGroups` e `nishidb_counter_groups_retired_total`).
- **o núcleo do nome é o que a função produz.** `TextoDaChave` consome a chave
  composta e produz o TEXTO; `TextKey` promoveria `Key` a núcleo justamente no
  pacote em que `chave`/`Key` é sempre a chave composta binária
  (`Dicionario.Key`, `ByKey`, `keyBuf`, `keyClone`) e `texto`/`Text` é sempre
  o `groupKey` renderizado (`texts`, `byText`, `textBuf`) — e `Contador.Grupo`
  devolve os dois lado a lado. O retorno só pode ir para `ByText`, nunca para
  `ByKey`, e as duas recebem `[]byte`: trocá-las compila. Vira `KeyText`; o
  arquivo `textkey.go`, que é o que a linha `chave-texto → textkey` nomeia,
  fica como está.


### Entrega de alarmes

| Termo na prosa | Identificador / contrato | Pacote |
|---|---|---|
| destino nomeado de webhook | webhook / `alarmDelivery.webhooks` | internal/config |
| destinos selecionados pelo alarme | `notify` / Notify | internal/schema |
| identidade da emissão | `eventId` / Event.ID | internal/alarm |
| fila durável de entrega | delivery queue / Manager | internal/alarmdelivery |
| retrato da entrega por destino | AlarmDeliveryStats | internal/protocol |
| descarte de evento novo | dropped / `alarm.delivery.dropped` | internal/alarmdelivery |

## Medidas de contadores (issue #75)

| PT-BR | Identificador/contrato | Dono |
|---|---|---|
| medida de taxa | `measures` / CounterMeasureSpec | internal/schema |
| materialização do total desativada | `total: false` / DisableTotal | internal/schema |
| ocorrência aceita | FlagCounterEvent / ApplyEvent | internal/types, internal/storage/counter |
| numerador da janela | MeasureCount | internal/storage/counter |
| geração de medidas | `measures.<epoch>.snapshot` / MeasureCounters | internal/storage/counter, internal/storage/checkpoint |
| identidade da fonte amostrada | `.win.source` / reconcileSource | internal/countermetrics |

## Escopo do contador e comparação linear (issue #61)

| PT-BR | Identificador/contrato | Dono |
|---|---|---|
| escopo do contador (o predicado sobre a linha que decide se ela é membro de um grupo; fora dele a linha fica sem grupo) | `scope` / Definition.Scope | internal/schema, internal/storage/counter, internal/counters |
| avaliador do escopo (instância própria por Deriver, da mesma fábrica) | NewScopeEvaluator | internal/storage/counter, internal/counters |
| membros do grupo (as linhas no grupo, dentro do escopo e com o groupBy completo — não as linhas do dataset) | `rows` / CounterRowsColumn | internal/schema, internal/counters, internal/alarm, internal/series |
| identidade do contador (o texto gravado no manifest que decide adotar ou reconstruir o vetor) | identityOf / identitySeparator / splitIdentity | internal/storage |
| palavra do grupo (total e membros numa única palavra de 64 bits, lida e somada de uma vez) | pairs / MaxGroupRows / ErrGroupRowsCeiling | internal/storage/counter |
| comparação linear entre colunas (`<col> [* k] <op> <col> [* k]`, só na condição de alarme) | ColumnComparison / ColumnTerm / BoundColumnComparison / CompileAlarmCondition | internal/query/parser, internal/query/binder, internal/predicate, internal/alarm |
| opções da condição (o que o parse e o bind aceitam além da gramática do WHERE) | ConditionOptions / ParseConditionWith / BindConditionWith | internal/query/parser, internal/query/binder |
| par do evento (total e membros lidos do payload que decidiu o alarme) | groupCounts / countsOf | internal/alarm |
