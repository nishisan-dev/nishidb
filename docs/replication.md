# Replicação master/follower

Um dataset pode publicar uma fila de alterações para followers. A fila é
volátil e limitada: o master nunca bloqueia a ingestão por causa de um
follower lento. O bloco é fechado pelo primeiro limite atingido: intervalo,
quantidade de mensagens recebidas, tamanho do lote ou memória total retida.

```yaml
replication:
  enabled: true
  role: master             # ou follower
  masterURL: https://...   # obrigatório no follower
  usernameEnv: NISHI_REPL_USER # opcional no follower
  passwordEnv: NISHI_REPL_PASS # opcional no follower
  flushInterval: 5s       # 0s a 60s; começa na primeira alteração
  maxMessages: 10000      # conta antes da deduplicação
  maxBatchBytes: 8MiB
  maxBufferedBytes: 64MiB
```

Updates consecutivos da mesma entidade são consolidados no estado completo já
materializado. A consolidação nunca cruza outra operação: `insert, update,
update, delete` produz `insert, update, delete`, enquanto `update, delete,
insert, update` conserva as quatro operações. Linhas de histórico têm IDs
distintos e não são fundidas.
O avanço da retenção histórica viaja como `retain`, inclusive no snapshot, para
que o follower mantenha a mesma marca de visibilidade do master.

Cada entrada leva o instante da publicação em `at`; na consolidação vale o da
última alteração. O `delete` carrega o último estado vivo da linha, e não a
lápide, tanto na expiração pelo coletor quanto na expiração oportunista da
compaction; a cópia da compaction e o replay da recuperação não publicam nada.
Voltar a escrever uma chave apagada publica `update`, não `insert`: a linha
mantém o mesmo `rowID`. Uma linha cujo TTL já passou, mas que o coletor ainda
não recolheu, já some das consultas e do snapshot. Por isso um consumidor pode
receber o `delete` de uma chave que nunca viu.

O follower começa pelo `GET /v1/replication/cursor`. Se a geração e o cursor
ainda estiverem cobertos, ele lê `GET /v1/replication/batch` e confirma cada
lote depois de aplicá-lo. Se a geração mudou ou a retenção criou uma lacuna,
recebe conflito e deve chamar `GET /v1/replication/snapshot`.

Depois que a fila é fechada, os lotes ainda retidos continuam sendo entregues,
inclusive o último, fechado pelo próprio encerramento. Quando não resta nenhum
lote depois do cursor, a leitura responde na hora que a fila foi fechada, sem
esperar.

O campo `invalidations` do status conta as perdas do intervalo inteiro, o
ativo e o retido. Isso acontece por invalidação explícita, ou quando um
registro não cabe na memória nem depois de liberar todos os lotes retidos. A
remoção de lotes antigos por memória conta em `evictedBatches`, não aqui. Nos
dois casos, followers que ficaram atrás do novo piso precisam ressincronizar.

O snapshot carrega as versões visíveis do dataset e da tabela histórica junto
com um cursor. O follower aplica esse estado pelo writer normal, reaplica o
tail depois do cursor e libera consultas apenas quando chega a `following`;
durante o resync o estado anterior não é exposto. Linhas históricas preservam
seu `rowID`, portanto uma sobreposição entre snapshot e tail é idempotente. Um
follower novo fica indisponível até concluir o primeiro snapshot.

O estado da réplica passa por `empty`, `syncing`, `catching_up`, `following`,
`disconnected` e `resync_required`. O instante lógico usado por TTL e janelas é
o instante recebido do master, de modo que uma desconexão não envelhece dados
por conta própria.

As rotas de replicação exigem ADMIN, usam o TLS/autenticação do servidor e
retornam conflito em vez de pular uma lacuna silenciosamente. O cliente
reconecta com backoff quando o master fica offline. Promoção a master, failover
e log durável de replicação permanecem fora desta versão.

No modo `follower`, o servidor não consome Kafka nem executa TTL, alarmes,
emissões ou reconstruções dependentes do relógio local; ele aplica os lotes pelo
writer do Store. Compaction local continua permitida para recuperar espaço.
