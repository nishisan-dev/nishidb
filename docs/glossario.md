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
| instantâneo | snapshot | internal/kafka/snapshot.go |
| transbordo | spill | internal/query/spill |
| janela | window | internal/storage/rowindex/schedule.go |
| congelada | frozen | internal/materializer/frozen.go |
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
| campo (componente de data/hora) | component | internal/typeparse/datetime.go |
| chave | key | internal/index/valuekey |
| dicionário | dict | internal/storage/counter/dict.go |
| segmento | segment | internal/storage/segment |
| reconstrução / reconstruído | rebuild / rebuilt | internal/storage/indices_rebuild.go, internal/storage/encoding/fields_test.go |
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
| bloco (de filesystem) | block | internal/storage/mmap/region.go, docs/architecture.md |
| meia-word | half-word | internal/storage/mmap/region.go |
| descritor (de arquivo; e o de tipo que o ResultSetMetaData publica) | descriptor | internal/storage/mmap/region_test.go, internal/protocol/jdbc.go |
| caminho | path | internal/gates, internal/storage/mmap/region.go, internal/storage/mmap/region_test.go |
| diretório (do filesystem) | dir | internal/query/spill, internal/storage |
| diretório (a tabela hash do WIP do índice por segmento) | directory | internal/index/segindex |
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
| estado (do segmento no índice, do subsistema no /v1/health, ou a palavra do slot do WIP) | state | internal/metrics/names.go, internal/protocol, internal/index/segindex |
| gravado (valor escrito no arquivo) | stored | internal/index/segindex |
| calculado | computed | internal/index/segindex |
| cabeçalho (de registro em disco) | header | internal/storage/encoding, internal/query/spill |
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
| filhos (as séries de uma família, indexadas pela chave de labels; e os nós filhos de um bloco da agenda) | children | internal/metrics, internal/storage/rowindex/schedule.go |
| ordem (de emissão: a fatia que fixa a ordem estável) | order | internal/metrics |
| ordem (chave de comparação da agenda, com ∞ no lugar do sentinela) | order key | internal/storage/rowindex/schedule.go |
| exposição (o texto do formato de exposição do Prometheus) | exposition | internal/metrics/metrics_test.go |
| saída (o texto produzido, em teste) | output | internal/metrics/metrics_test.go |
| modo (de resolução do Lookup: índice ou enumeração) | mode | internal/metrics/names.go |
| fora (o segmento que a janela de indexação deixou de cobrir) | outside | internal/metrics/names.go |
| enumeração (resolver o segmento varrendo, sem índice) | enumeration | internal/metrics/names.go |
| consulta (do índice: o Lookup) | lookup | internal/metrics/names.go |
| desistiu (o watchdog esgotou as tentativas) | gave up | internal/metrics/names.go |
| pendente (o segmento dentro da janela ainda na fila de reconstrução) | pending | internal/metrics/names.go |
| categoria (de falha de consulta) | category | internal/metrics/names.go |
| motivo (a categoria §101 do evento ruim que a ingestão recusou) | invalid reason | internal/metrics/names.go |
| motivo (da falha de autenticação HTTP Basic) | reason | internal/metrics/names.go |
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
| conhecido (a entrada do índice já foi gravada; zero é desconhecido, nunca vencido) | known | internal/storage/rowindex |
| nascer / nasceu (esta abertura criou o arquivo, em vez de reencontrá-lo) | born | internal/storage/rowindex |
| zerar (apagar todas as entradas mantendo header e tamanho) | clear | internal/storage/rowindex/dense32.go |
| imortal (entidade que uma regra `never` declarou que não expira) | immortal | internal/storage/rowindex |
| nunca (o vencimento da linha que não expira: o ∞ codificado como zero na agenda) | never | internal/storage/rowindex/schedule.go |
| sumário (nó interno da agenda: limite inferior do mínimo dos filhos) | summary | internal/storage/rowindex/schedule.go |
| nó (entrada de um nível da agenda: folha ou sumário) | node | internal/storage/rowindex/schedule.go |
| topo (o nível mais alto da agenda, varrido por inteiro) | top | internal/storage/rowindex/schedule.go |
| antigo (o valor da folha ANTES desta atualização, oposto a "anterior" → prev) | old | internal/storage/rowindex/schedule.go |
| vencido (linha cujo vencimento agendado já passou) | expired | internal/storage/rowindex/schedule.go |
| tocadas (as entradas em que a descida entrou) | touched | internal/storage/rowindex/schedule.go |
| descer / descida (a caminhada do topo às folhas da agenda) | descend / descent | internal/storage/rowindex/schedule.go |
| renovação (empurrar o vencimento da linha para a frente) | renewal | internal/storage/rowindex/schedule_test.go |
| cadeia (a sequência de segmentos que a recovery percorre; e a cadeia de erros embrulhados) | chain | internal/storage |
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
