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
| aposentar | retire | internal/storage/contador |
| aposentadoria | retirement | internal/storage/contador/aposentadoria.go |
| vigia | watchdog | internal/kafka/vigia.go |
| orçamento | budget | internal/query/orcamento |
| instantâneo | snapshot | internal/kafka/instantaneo.go |
| transbordo | spill | internal/query/spill |
| janela | window | internal/storage/rowindex/agenda.go |
| congelada | frozen | internal/materializer/congelada.go |
| congelar | freeze | internal/materializer/congelada.go |
| passada | sweep | internal/config/config.go |
| prazo | deadline | internal/storage/rowindex/agenda.go |
| vencimento | expiry | internal/storage/rowindex/agenda.go |
| agenda | schedule | internal/storage/agenda.go |
| moldura | frame | internal/protocolo |
| em voo | in-flight | clients/python |
| contador | counter | internal/storage/contador |
| grupo | group | internal/storage/contador |
| linha | row | internal/storage/rowindex |
| campo | field | internal/schema |
| chave | key | internal/index/valuekey |
| dicionário | dict | internal/storage/contador/dicionario.go |
| segmento | segment | internal/storage/segment |
| reconstrução | rebuild | internal/storage/indices_reconstroi.go |
| descartado | discarded | internal/materializer/materializer.go |
| recusa | refuse | internal/config/config.go |
| recusar | refuse | internal/config/config.go |
| falho (writer) | failed | internal/storage/store.go |
| ingestão | ingestion | internal/cli/ingest.go |
| consulta | query | internal/query |
| materialização | materialization | internal/materializer |
| histórico | history | internal/materializer/historico.go |
| retenção | retention | internal/storage/retencao.go |
| expiração | expiry | internal/storage/expiracao.go |
| autenticação | auth | internal/server/autenticacao.go |
| credencial | credential | internal/cli/credencial.go |
| parâmetro | param | internal/protocolo/parametros.go |
| operação | operation | internal/cli/operacao.go |
| mutação | mutation | internal/cli/mutacao.go |
| evidência | evidence | internal/cli/evidencias_test.go |
| citação | citation | internal/gates/citacoes_test.go |
| gate | gate | internal/gates |
| dobra | fold | internal/storage/contador |
| denso | dense | internal/storage/rowindex/denso32.go |
| esparso | sparse | docs/architecture.md |
| selado | sealed | internal/index/segindex/selado.go |
| sela | seal | internal/index/segindex/sela.go |
| união | union | internal/index/segindex/uniao.go |
| avaliador | evaluator | internal/predicado/avaliador.go |
| programa | program | internal/predicado/programa.go |
| regra | rule | internal/predicado/regra.go |
| leitor | reader | internal/config/config.go |
| escritor | writer | internal/index/segindex/escritor.go |
| fonte | source | internal/query/meta/fonte.go |
| iterador | iterator | internal/index/segindex/uniao.go |
| tabela | table | internal/contadores/tabela.go |
| registro de virtuais | registry | internal/query/engine.go |
| registro (linha) | record | internal/storage/encoding/record.go |
| abertura | open | internal/contadores/abertura.go |
| compila | compile | internal/contadores/compila.go |
| coleta | collect | internal/query/executor/collect.go |
| extração | extract | internal/materializer/patch.go |
| estatísticas | stats | internal/contadores/meta.go |
| relatório | report | internal/server/contadores.go |
| posição | position | internal/cli/offsets.go |
| cadência | cadence | internal/kafka/vigia.go |
| política | policy | internal/config/config.go |
| corrida | race | internal/index/segindex/escritor.go |
| desfecho | outcome | internal/cli/desfecho_internal_test.go |
| perda | loss | internal/cli/perda_test.go |
| resumo | summary | internal/metakafka/tabela.go |
| vazão | throughput | internal/cli/vazao_test.go |
| emvoo | inflight | internal/server/server.go |
| carência | grace | internal/storage/medicao_carencia_test.go |
| lacuna | gap | internal/gates/citacoes_test.go |
| resíduo | residue | internal/index/segindex/wip.go |
| espelho | mirror | internal/schema/historico.go |
| órfã | orphan | internal/schema/historico.go |
| paridade | parity | internal/query/executor/ndjson.go |
| comportamento | behavior | internal/config/config.go |
| chave-texto | textkey | internal/storage/contador/chavetexto.go |
| capacidades | capabilities | internal/server/autenticacao.go |
| consumido | consumed | internal/jsonp/jsonp.go |
| contado | counted | internal/cli/perda_test.go |
| derivadas | derived | internal/materializer/historico.go |
| vencimentos | expiries | internal/storage/agenda.go |
| prazos | deadlines | internal/storage/agenda.go |

## 4. Como usar

- Comentário novo cita o termo PT-BR desta lista; identificador novo usa o
  termo inglês correspondente.
- Termo novo entra nesta lista ANTES de ser usado em código — nunca depois.
- O juiz de cada fase pode AMPLIAR o glossário (adicionar linha), nunca
  CONTRADIZER uma linha existente (mudar a tradução de um termo já fechado).
- Colisão entre dois sentidos do mesmo termo PT-BR (como os dois "registro"
  acima) resolve qualificando o termo PT-BR com o sentido entre parênteses —
  nunca dando aos dois o mesmo termo inglês e nunca usando sufixo numérico.
