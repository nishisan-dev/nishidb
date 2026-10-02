# Spec — entrega de alarmes de contador para Kafka

Branch: `feature/alarm-delivery-kafka` (a partir de `origin/main` com o #95, `178523c`).

## Motivação

Um cliente quer consumir alarmes de contador de um tópico Kafka (`olt_alarms` no Kafka
dev da .219). Hoje `alarmDelivery` só tem `webhooks` (`internal/config/alarm_delivery.go`,
`internal/alarmdelivery/`). O envio Kafka que existe (`seriesDelivery`) é da SÉRIE de
contadores, melhor esforço, e não serve para alarme.

## Decisões do usuário (fechadas)

1. **Garantia de entrega igual à do webhook**: mesmo corpo JSON (`alarm.firing` /
   `alarm.resolved`, com `total` e `rows`), mesma fila durável em disco por destino
   (`internal/alarmdelivery/queue.go` + `disk.go`), FIFO, retentativa sem fim com backoff
   enquanto o Kafka estiver fora, e mesma identidade (`eventId`) em todas as tentativas.
2. **Produção com a filosofia do `seriesDelivery`**: destinos GLOBAIS com nome no
   `nishidb.yaml`, com `bootstrapServers`, `topic`, `timeout` e o mesmo bloco `security`
   (`schema.SecuritySpec`, resolvido por `kafka.ResolveSecurity`, credenciais por nome de
   variável de ambiente); um produtor (kgo) por destino usado; o alarme seleciona o
   destino pelo nome em `notify:`, exatamente como seleciona um webhook; chave Kafka no
   formato da série.

## Contrato

### Configuração (`nishidb.yaml`)

```yaml
alarmDelivery:
  webhooks: [...]          # inalterado
  kafka:
    - name: olt_alarms
      bootstrapServers: ["10.200.20.219:9092"]
      topic: olt_alarms
      timeout: 5s          # default 5s, faixa (0, 5m] — prazo de UMA tentativa
      security: {protocol: PLAINTEXT}   # mesmo contrato da ingestão e do seriesDelivery
      queue:
        maxBytes: 256MiB   # mesmo default e mesmo piso do webhook
```

- O nome compartilha o ESPAÇO de nomes com `webhooks` (mesma validação
  `ValidDestinationName`); nome repetido entre os dois é recusado na subida. `notify:`
  aceita qualquer um dos dois tipos.
- Validação na subida no mesmo molde de `ValidateSeriesDelivery` (tópico pela mesma regex,
  bootstrap por `schema.ValidateKafkaBootstrap`, timeout, segurança resolvida, credencial
  local ausente recusa). Broker indisponível NÃO impede a subida. Chave desconhecida no
  bloco é recusada (`knownKeys`), como hoje no `webhooks`.
- Reaproveitar o tipo/validação do `SeriesKafkaSpec` onde der sem acoplar os dois blocos
  (ex.: struct de conexão comum); não duplicar a regex nem a validação de bootstrap.
- O tópico tem de existir; o produtor não pede criação automática (igual à série).

### Mensagem

- **Valor**: o MESMO corpo JSON que o webhook POSTa (o `json.Marshal(e)` do
  `Manager.Emit`, bytes idênticos aos gravados na fila).
- **Chave**: o array JSON compacto `[dataset, counter, ...valores do groupBy tipados na
  ordem declarada]` — o mesmo formato da chave da série (`docs/operacao-series-kafka.md`,
  "Mensagem e chave"). Mesmo grupo → mesma partição → disparo e resolução do grupo em
  ordem. Os valores tipados vêm de `alarm.Event.Columns`; a montagem tem de produzir
  bytes IGUAIS aos da série para o mesmo grupo (teste comparando com a função/forma da
  série, ou um golden compartilhado; nunca uma segunda renderização divergente — usar
  `executor.AppendValue`/`AppendJSONString` como a série).
- **Headers**: `eventId` (= `Idempotency-Key` do webhook) e `event`
  (`alarm.firing`/`alarm.resolved`). O consumidor deduplica por `eventId`.
- **Timestamp do record**: NÃO usar o `at` do evento. Lição do #95: o kgo mede o
  `RecordDeliveryTimeout` a partir do Timestamp do record, e um evento que ficou na fila
  durante uma queda teria Timestamp antigo e expiraria antes de sair. Deixar o kgo
  carimbar o instante da produção (ou não configurar `RecordDeliveryTimeout` e usar só o
  ctx por tentativa — decidir e justificar no godoc com path:linha do franz-go
  v1.21.6). O instante do alarme está no corpo (`at`).

### Entrega

- O laço de envio é o MESMO do webhook (`sendLoop`): lê o registro da cabeça da fila do
  disco, tenta, e só no sucesso apaga o arquivo + fsync do diretório e avança; falha →
  `FailedAttempts++`, WARN `alarm.delivery.retry` (sem corpo, sem credencial, sem
  bootstrap; com o destino e uma CLASSE de erro estável, ex. `timeout`,
  `unknown_topic`, `broker_unavailable`, `rejected`) e backoff 1s→5min com jitter.
- Refatoração esperada: o `queue` deixa de conhecer `*endpoint` HTTP e passa a usar uma
  abstração de transporte com dois implementadores (HTTP e Kafka). O comportamento do
  webhook NÃO muda (todos os testes atuais do webhook continuam verdes sem alteração de
  asserção; renomeações mecânicas são aceitáveis e registradas).
- Tentativa Kafka = produção síncrona de UM registro (`ProduceSync` ou equivalente) com
  ctx de `timeout`, `acks=all`. Sucesso só com o ack. Retentativas internas do kgo
  desligadas ou limitadas (a política de retentativa é a da fila, para manter FIFO e a
  contagem de `FailedAttempts` honestas). Idempotência do produtor: decidir e justificar
  (com uma tentativa por vez e retentativa feita pela fila, duplicata é possível no
  timeout com o broker tendo gravado — o `eventId` cobre; documentar).
- Destino removido do config com fila no disco → pausado, como o webhook hoje (o
  diretório da fila é por nome; trocar o TIPO de um destino mantendo o nome reaproveita
  a fila — o corpo é o mesmo). Documentar.
- Observabilidade: a MESMA `protocol.AlarmDeliveryStats` por destino (já no `/v1/stats`
  e em `/metrics`, `internal/server/metrics_alarm_delivery.go`). Se for útil distinguir
  o tipo, acrescentar campo `kind` (`webhook`/`kafka`) ao wire e como label? —
  DECISÃO: só no wire (`/v1/stats`), NÃO como label de métrica (evita mudar as séries
  existentes). Métricas existentes valem para os dois tipos sem mudança de nome.
- Encerramento: `Close` cancela o envio (Kafka sem esperar broker), drena a memória para
  o disco e fecha o cliente kgo. Nenhum evento admitido se perde no restart.

## Garantias (cada uma com teste VERMELHO com o mutante indicado)

| # | Garantia | Mutante |
|---|---|---|
| K1 | Corpo no Kafka é byte a byte o corpo do webhook para o mesmo evento | re-serializar com outro encoder / campo a menos |
| K2 | Chave = `[dataset,counter,...groupBy tipado]`, bytes iguais aos da série para o mesmo grupo (slot int sai `4`, não `"4"`) | valor do groupBy como string |
| K3 | Headers `eventId` e `event` presentes e iguais ao corpo | header ausente |
| K4 | Sucesso só com ack: arquivo da fila apagado (e dir fsync) só depois do ack | apagar antes de produzir |
| K5 | Kafka fora → evento fica no disco, `FailedAttempts` cresce, WARN com classe, e é entregue quando o broker volta, na ordem (FIFO) | descartar na falha / reordenar |
| K6 | Restart com fila pendente → reentrega na ordem com o MESMO `eventId` | regenerar id |
| K7 | Evento antigo na fila (at muito anterior) é entregue depois de queda longa — não expira por Timestamp | Timestamp do record = `at` com `RecordDeliveryTimeout` configurado |
| K8 | Config: nome duplicado entre webhook e kafka recusado; tópico/bootstrap/timeout/segurança inválidos recusados com mensagem própria da guarda; chave desconhecida recusada; `notify` para destino kafka aceito | remover cada guarda (asseverar a mensagem PRÓPRIA, não só "falhou") |
| K9 | Credencial por variável de ambiente ausente recusa a subida; valor nunca aparece em log/erro | logar o valor |
| K10 | Destino removido com fila pendente → pausado (WARN `alarm.delivery.paused`), nada se perde | apagar fila órfã |
| K11 | Webhook inalterado: suíte atual do `alarmdelivery` e do servidor verde | — (regressão) |
| K12 | Stats/métricas do destino kafka nascem em zero e saem no `/v1/stats` e no `/metrics` da mesma estrutura | contador paralelo |

Testes contra broker real em memória com `kfake` (o repo já usa em
`internal/seriesdelivery`); controlar a queda do broker pelo kfake (fechar/reabrir ou
`ControlKey` que falha requisições). Nenhum teste dependente de prazo curto — usar
gatilhos determinísticos.

## Docs (mesmo commit)

- `docs/operacao-alarmes-kafka.md` NOVO (configuração, mensagem/chave/headers,
  garantia at-least-once e deduplicação por `eventId`, ordem por grupo, queda do broker,
  pausado, tópico particionado criado com o número final de partições), referenciado
  de `docs/operacao-webhook.md` e do README (seção de alarmes).
- `docs/architecture.md` (seção de entrega de alarmes) e `docs/metricas.md` se algo
  mudar no texto das métricas de entrega (passam a valer para os dois tipos).
- Diagrama PlantUML que mostre a entrega de alarmes (procurar em `docs/diagrams/`);
  atualizar se só mostrar HTTP.
- Termos para o glossário vêm no relatório (Builder não edita `docs/glossario.md`).
- Identificador PT-BR tocado: renomear e registrar em
  `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`.

## Fora de escopo

- Transação/exactly-once.
- Entregar a série de contadores por esta via.
- Ligar na .218 (feito pelo orquestrador depois da release).

## Emendas da refutação do plano (PREVALECEM sobre o texto acima)

A1 (BLOQUEANTE 1 — chave). `alarm.Column.Value` é string (`internal/alarm/event.go:36-39`,
`runner.go:596`), e a fila só guarda o corpo (`disk.go:23-31`): a chave tipada da série
não é reconstruível sem mudar o formato do registro em disco. DECISÃO do orquestrador:
a chave Kafka do alarme é o array JSON compacto **`[dataset, counter, groupKey]`**
(`["service_state","pon","OLT_PON.BR_X_OLT01.4.14"]`), derivado do PRÓPRIO corpo no
envio. Mantém a identidade por grupo (mesma partição, ordem por grupo), NÃO muda o
formato em disco (registro v1 continua; troca de tipo mantendo o nome continua segura)
e não depende da decodificação do grupo. Não é igual byte a byte à chave da série; a
doc diz isso. `eventIdentity` (`disk.go:33-36`) passa a ler também `event`, `dataset`,
`counter` e `groupKey` do corpo (o `readRecord` já faz o `json.Unmarshal`); registro sem
esses campos é inválido para o transporte Kafka (falha a fila como hoje um registro
inválido — documentar). Montagem com `executor.AppendJSONString` (escape canônico).
K2 vira: golden da chave com groupKey que exige escape (aspas, barra, não-ASCII) e o
grupo aposentado; mutante: concatenar sem escapar.

A2 (BLOQUEANTE 2 — evento grande trava a fila). O kgo recusa registro acima de
~999.884 bytes (`seriesdelivery/delivery.go:34`); o webhook aceita até `maxEventBytes`
(1 MiB, `queue.go:31`). Teto POR TRANSPORTE na admissão (`offer`): para destino Kafka,
chave + corpo + headers acima do teto do kgo → descarte `event_too_large` (rótulo já
existente, `DropTooLarge`), nunca entra na fila. Garantia K16, mutante: sem o teto (o
teste prova que um evento grande na frente não trava o seguinte).

A3 (BLOQUEANTE 3 — produtor). Configuração FIXA, a da série (`delivery.go:183-191` no
main), justificada no godoc com path:linha do franz-go: `DisableIdempotentWrite()` (com
idempotência o kgo não cancela registro em voo e ignora o limite de retentativas —
`sink.go:588-596`, `:1082`, `config.go:1191-1215` — e o timeout por tentativa e o Close
deixariam de valer), `RequiredAcks(AllISRAcks())`, `RecordRetries(0)`,
`UnknownTopicRetries(0)`, `ProduceRequestTimeout(max(timeout,100ms))`,
`RequestTimeoutOverhead(100ms)`, `DialTimeout(timeout)`, `StickyKeyPartitioner(nil)`,
SEM `RecordDeliveryTimeout`. `Timestamp` zero (o kgo carimba `time.Now()`,
`sink.go:1611-1612`). Um `kgo.Record` NOVO a cada tentativa (`ProduceSync` muta o
registro). Duplicata no timeout ambíguo é aceita e documentada (o `eventId` deduplica).

A4 (BLOQUEANTE 4 — dono do produtor). UM cliente kgo POR FILA (dataset × destino),
criado no `Open` da fila e fechado no `close` dela. Ciclo de vida igual ao do
`http.Client` de hoje; nada global, nada novo na ordem do `encerra()`.

A5 (interface). `queue` deixa de conhecer HTTP: transporte
`send(ctx, body, identity) attempt` (attempt = `ok`, `retryAfter`, atributos de log
estáveis — `status` no HTTP, `class` no Kafka) + `close()`. Os atrasos de retentativa
(`initialDelay`/`maxDelay`) passam para a `queue` (os testes que os ajustam continuam
podendo); "pausado" = transporte nil, como hoje. A mensagem do WARN deixa de dizer
"webhook" ("delivery attempt failed; retaining event for retry"). `CloseIdleConnections`
vira o `close()` do transporte HTTP. `queue_test.go:379` (`q.post`) pode ser ajustado
mecanicamente.

A6 (validação). Parametrizar a validação de conexão Kafka da série (sentinela e caminho
`seriesDelivery.kafka[i]` / `alarmDelivery.kafka[i]`) em vez de duplicar; os erros do
bloco de alarme usam `ErrAlarmDelivery` (exit `ExitUsageError`, `cli.go:268`).
`Limits()` deixa de dizer "webhook" no erro quando servir aos dois. `Validate` (que
roda global, `server.go:292`) resolve segurança e credencial dos destinos Kafka;
`Open` conhece os dois tipos.

A7 (garantias novas/ajustadas).
- K4: segurar o Produce (kfake `SleepControl`) e conferir que o arquivo da fila EXISTE e
  `DiskEvents == 1` enquanto o ack não veio; mutante: apagar antes de produzir.
- K6: depois do restart, asseverar também a CHAVE e os headers.
- K7: asseverar que o `Timestamp` do record é >= o início da tentativa (não o `at`);
  mutante: `Timestamp = at`.
- K13: `Close` com Produce segurado pelo broker devolve sem esperar o broker
  (mutante: idempotência ligada / sem cancelar o ctx).
- K14: timeout ambíguo → duas cópias com o MESMO `eventId` no tópico (documenta a
  duplicata; mutante: regenerar id).
- K15: broker inacessível (porta fechada) não impede o `Open` nem a subida.
- K16: ver A2.
- Simulação de queda: `ControlKey(Produce)` + `KeepControl` + flag atômica, ou
  `SleepControl` até liberar canal (molde: `internal/seriesdelivery/delivery_test.go`).
  Sem prazo curto como critério.

A8 (wire). `kind` (`webhook` | `kafka`) em `AlarmDeliveryStats`, json `kind`,
`omitempty`; destino pausado sai sem `kind`. Nenhum gate/cliente lê o bloco — não exige
SQLSTATE. Corrigir o godoc "fila de webhook" (`protocol/documents.go:659`).

A9 (docs a corrigir além das listadas). HELP "tentativas HTTP malsucedidas"
(`internal/server/metrics_alarm_delivery.go:20`, `docs/metricas.md:395`) passa a valer
para os dois tipos; `docs/architecture.md:3250` ("tópico entra depois");
`docs/diagrams/alarmes_flow.puml:163-172` (só HTTP 2xx) — validar o diagrama
renderizando pelo servidor `https://uml.nishisan.dev/png/` (não há plantuml local);
README ~l.2045-2053 ("destinos HTTP"). Doc de operação: erro PERMANENTE (autorização,
tópico inexistente, registro recusado) prende a fila para sempre, como o 4xx do webhook —
o operador vê `FailedAttempts` crescendo e o WARN com a classe; trocar o tipo mantendo o
nome entrega ao tópico o que estava pendente para HTTP.
