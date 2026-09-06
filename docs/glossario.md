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
| derivadas | derived | internal/materializer/history.go |
| vencimentos | expiries | internal/storage/schedule.go |
| prazos | deadlines | internal/storage/schedule.go |
| padrão (valor herdado quando a chave está ausente) | default — `Default` e `default` em nome composto (`DefaultMaxRows`, `defaultTimeout`); `defaultValue` em parâmetro ou local solto, porque `default` é palavra reservada em Go | internal/units |
| padrão (formato de data, de rota ou de LIKE) | pattern | internal/typeparse/datetime.go, internal/server/auth.go, internal/query/parser/parser.go |
| entrada (de teste) | input | internal/units/units_test.go |
| casos (de teste) | cases | internal/units/units_test.go |
| quer (esperado em teste) | want | internal/units/units_test.go |
| unidade (de tamanho) | unit | internal/units/units.go |
| sufixo | suffix | internal/units/units.go |

## 4. Como usar

- Comentário novo cita o termo PT-BR desta lista; identificador novo usa o
  termo inglês correspondente.
- Termo novo entra nesta lista ANTES de ser usado em código — nunca depois.
- O juiz de cada fase pode AMPLIAR o glossário (adicionar linha), nunca
  CONTRADIZER uma linha existente (mudar a tradução de um termo já fechado).
- Colisão entre dois sentidos do mesmo termo PT-BR (como os dois "registro"
  acima) resolve qualificando o termo PT-BR com o sentido entre parênteses —
  nunca dando aos dois o mesmo termo inglês e nunca usando sufixo numérico.
