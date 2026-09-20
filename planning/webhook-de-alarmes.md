# Issue #62 — Webhook de alarmes com fila durável

Plano aprovado na conversa de 19/09/2026. Implementação restrita a webhook;
Kafka fica para outro ciclo.

## Decisões

- Destinos nomeados no arquivo global, selecionados por `notify` em cada alarme.
- Autenticação Bearer opcional, com segredo em variável de ambiente.
- Persistência assíncrona: a varredura não espera por disco nem HTTP.
- Fila por dataset e destino, FIFO, com limite de bytes e quantidade.
- Ao saturar, preservar antigos e descartar novos, com métrica e log.
- Retentar qualquer falha, inclusive 400/401/403, até HTTP 2xx; sem expiração.
- Entrega admite duplicatas. O receptor deduplica pelo `eventId`, compartilhado
  com o log e preservado em todas as tentativas do mesmo evento.
- Queda antes da publicação durável pode perder a cauda em memória. O contrato
  não promete entrega de todo evento observado nem contagem exata dessa perda.

## Implementação

A projeção do evento mora em `internal/alarm`: log e webhook compartilham
campos, tipos e supressão. Corpo e enriquecimento ficam congelados. As colunas
que colidem com campos reservados são recusadas somente quando há push.

`internal/alarmdelivery` implementa o Sink composto, rotas, admissão limitada,
gravador, recuperação e remetente. Um arquivo por evento, com cabeçalho
versionado e checksum, usa a gravação atômica existente. O remetente só lê o
arquivo publicado; só avança depois de excluir e sincronizar o diretório.
O buffer reserva capacidade de disco antes de aceitar o corpo.

O servidor valida referências e credenciais antes de abrir os datasets,
recupera filas antigas mesmo sem a regra original e inicia HTTP antes da
varredura. Na parada, encerra a varredura, drena memória, cancela HTTP e fecha
as filas antes de liberar o flock. Falha de persistência suspende somente a
fila afetada e é propagada no encerramento.

O nome identifica o destino: trocar URL redireciona pendências; retirar o nome
pausa e preserva seus arquivos. Reintroduzir o nome retoma. O reanúncio de um
alarme restaurado continua existindo e recebe identidade nova.

Defaults: disco 256MiB; 50.000 eventos; memória 256 eventos/8MiB; corpo 1MiB;
timeout 5s; recuo exponencial de 1s a 5m com variação aleatória. Timeout e teto
de bytes em disco são configuráveis. `/metrics` e `/v1/stats` leem o mesmo
retrato; labels não incluem grupo, URL nem identidade do evento.

## Aceite

- Equivalência entre log e corpo HTTP, incluindo campos opcionais e redação.
- Mesmo identificador e corpo após retentativa e recuperação.
- Falhas em cada etapa de publicação e confirmação, com arquivos preservados.
- Fila cheia, erro permanente e receptor lento não bloqueiam a varredura nem
  outros destinos; nenhuma capacidade aceita ultrapassa seus limites.
- Recuperação e entrega por uma segunda instância real do servidor.
- Cancelamento de HTTP e persistência do buffer no encerramento sob `-race`.
- Configuração inválida recusada com diagnóstico próprio; métricas começam em
  zero e correspondem aos campos de `/v1/stats`.
- Testes completos, detector de corrida, lint sem aumento da baseline e
  mutações que comprovem as guardas de durabilidade, ordem e limites.

A [documentação operacional](../docs/operacao-webhook.md) descreve o contrato
implementado e os procedimentos de recuperação. `alarms.state`, consultas SQL
e clientes permanecem com seus formatos anteriores; `/v1/stats` ganha apenas
o bloco aditivo da entrega.
