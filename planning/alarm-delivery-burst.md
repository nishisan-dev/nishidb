# Rajada de alarmes sem descarte por memória cheia (N2)

Pedido da integração service-view → `service_state` (30/09/2026), decisão do
usuário: "corrigir já". Bloqueia a ida para produção.

## O defeito

`queue.offer` (`internal/alarmdelivery/queue.go`) admite no máximo 256 eventos
/ 8 MiB em memória e DESCARTA o excedente com `reason="memory_full"`. O gravador
persiste um evento por vez (`atomicfile.Write`: temp, fsync, rename, fsync do
diretório). Numa passada em que 10 mil grupos disparam juntos, só ~256 + o que
o gravador persistir durante a passada entram; o resto some. Um
`alarm.firing` descartado NUNCA é reemitido: o tracker já está em `firing` e o
estado durável (`alarms.state`) também. Nenhum teste exercita rajada
(`docs/aceite-webhook.md`: "Não há medição nova de vazão").

## Decisão

**Contrapressão na admissão, só por memória.** Quando a memória da fila está
cheia (eventos OU bytes), `offer` ESPERA vaga em vez de descartar. A espera
termina por: vaga liberada pelo gravador; fila fechada (`DropClosed`); fila
falha (`DropFailed`). Os outros descartes NÃO mudam: `disk_full` (teto de
disco/quantidade — é o que impede um receptor fora do ar por dias de travar a
varredura), `event_too_large`, `encoding_error`, `destination_failed`,
`closed`.

Por que é seguro bloquear a varredura:
- a memória só depende do GRAVADOR (disco local), nunca do HTTP — receptor
  fora do ar enche o disco e cai em `disk_full`, não trava a varredura;
- a varredura (`Runner.sweep`) não segura lock de ingestão entre grupos:
  `Counter.Group` toma `dict.mu.RLock` por chamada e solta;
- o encerramento já espera o gravador (`queue.close` faz `<-q.writerDone`);
  um disco travado já trava o encerramento hoje, então a espera não cria uma
  classe nova de travamento. `close` precisa ACORDAR quem espera.

A garantia antiga "offer nunca espera o disco" vira "offer nunca espera o
disco enquanto houver vaga em memória, e nunca espera o HTTP". O godoc de
`offer`, o comentário do pacote e `docs/operacao-webhook.md` /
`docs/aceite-webhook.md` mudam no MESMO commit.

**Contrato de série:** o webhook saiu na v0.10.0, então o label
`reason="memory_full"` é contrato publicado. Ele CONTINUA no conjunto fechado
(`DropReasons`, série nascendo com zero) e passa a ser documentado como "não
emitido desde esta versão; mantido para não quebrar painéis e alertas". Não
renomear nem remover.

**Métrica nova:** tempo acumulado de espera por vaga, por destino —
`nishidb_alarm_delivery_admission_wait_seconds_total{dataset,destination}`
(counter, nasce com zero), constante em `internal/metrics/names.go`, campo no
`protocol.AlarmDeliveryStats` (lido pelo `/v1/stats` E pelo `/metrics`, mesma
estrutura), linha em `docs/metricas.md`. É o sinal operacional de "o disco do
alarme não acompanha a rajada".

**Vazão (medir, não otimizar ainda):** com contrapressão, a duração da passada
numa rajada passa a ser ~N × custo de uma gravação durável. Medir com 10.000
eventos em disco real e registrar o número no aceite. A gravação em grupo
(vários arquivos, um fsync de diretório) só entra se a medida mostrar que
precisa — decisão do orquestrador com o número na mão.

## Garantias e o teste que fica vermelho com a mutação de cada uma

1. **Rajada não descarta por memória.** 10.000 `offer` seguidos numa fila com
   os limites REAIS (256 / 8 MiB) e destino pausado ou lento: no fim,
   `Dropped["memory_full"] == 0`, `DiskEvents == 10.000`, e recovery após
   `close` + `openQueue` reencontra os 10.000 em ordem.
   Mutação: restaurar o `DropMemoryFull` → vermelho.
2. **Espera acorda com `close`.** `offer` bloqueado em memória cheia retorna
   quando a fila fecha, contando `closed`. Mutação: `close` sem acordar → o
   teste trava (use prazo explícito, falha com mensagem, não timeout do go
   test).
3. **Espera acorda com falha do gravador.** Gravador falha com eventos em
   espera → `offer` retorna contando `destination_failed`. Mutação análoga.
4. **Sem vaga de DISCO continua descartando sem esperar.** Teto de disco
   atingido → `offer` volta imediatamente com `disk_full`. Mutação: esperar
   também no disco → vermelho (prazo curto).
5. **HTTP nunca é esperado.** Receptor que nunca responde + rajada maior que a
   memória → todas as admissões terminam (disco tem vaga), nenhuma espera pelo
   POST. Mutação: liberar memória só após entrega → vermelho.
6. **Métrica de espera.** Após uma rajada que esperou, o campo e a série são >
   0, e são o MESMO número no `/v1/stats` e no `/metrics` (siga o padrão dos
   testes existentes de stats compartilhados). Série presente com zero na
   subida.
7. **Ponta a ponta pelo laço de alarme:** um contador com ≥ 2.000 grupos que
   disparam na MESMA passada, webhook apontando para um receptor de teste que
   registra `eventId`: todos chegam, nenhum descarte. (Pode ficar no pacote
   `internal/server` seguindo `alarm_delivery_test.go`.)

Gate de medida da vazão: `//go:build !race`, com contraparte que se anuncia
SKIP sob `-race`, usando `NISHIDB_BENCH_DIR` quando definido (disco real) — se
ausente, mede em `t.TempDir()` e diz no log onde mediu. O número vai para
`docs/aceite-webhook.md`, não para comentário de código.

## Fora deste bloco

- Gravação em grupo (só com a medida).
- Dead-letter / limite de tentativas (o desenho FIFO com retentativa infinita
  não muda).
