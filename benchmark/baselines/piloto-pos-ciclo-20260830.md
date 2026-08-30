# Piloto `events` — subida do índice por segmento, 30/08/2026

Binário `v0.6.0-250-ge13a6dd`, compilado de clone limpo. Migração do dataset de
135 GiB **em produção**, sem recriar nada: 54,3 M de linhas no histórico,
manifest v2 → v3, os dois índices globais apagados do disco.

Config acrescentada ao `history` (valores do exemplo canônico da §4.1):
`index: {window: 6h, maxBytes: 2GiB}`, e `node` devolvido com `index: true`.

## Antes e depois

| Grandeza | Antes (v0.6.0-188) | Depois (v0.6.0-250) | |
|---|---|---|---|
| RSS | 2.812 MiB | **432 MiB** | 6,5× menor |
| `nishidb_history_index_bytes` | 3.716 MB | **614 MB** | 6× menor |
| `kafka_poll_blocked_seconds_total` | 32.319 s acumulados | **0** | |
| `kafka_queue_depth` | 5.000 (no teto) | **0** | |
| `kafka_lag` | backlog | **0** | alcançado |
| Congelamento de ingestão | 18–52 min, recorrente | **nenhum** | |

## A migração

Primeira passada de reconstrução, acompanhada pelas séries novas:

| Instante | `pendente` | `selado` | `reconstrucoes_total` |
|---|---|---|---|
| subida + 1 min | 63 | 1 | 25 |
| subida + 5 min | **0** | **65** | 64 |
| +45 s | 0 | 65 | 64 | *(estável — a fila drenou e ficou)* |

64 segmentos reconstruídos em ~5 minutos, sobre um dataset de 135 GiB, com a
ingestão correndo ao lado. **A fila drenou e não voltou a encher**: é a
convergência do laço demonstrada em produção, não em bancada.

## A janela fazendo o que foi desenhada para fazer

`fora = 372`, `selado = 65`. A esmagadora maioria dos segmentos está fora da
janela e **deve mesmo** estar sem índice — é isso que torna a migração barata: o
laço não tocou os 372, só os 64 que a janela admite.

`nishidb_history_index_window_seconds = 22.747` (6,32 h) contra `window: 6h`
declarado. A cobertura real excede o prazo declarado porque um segmento que
atravessa a fronteira mantém o índice (§4.3). Com 614 MB contra o teto de 2 GiB,
quem manda é o **tempo**, não os bytes — e a série diz isso sem o operador ter
de inferir.

## A taxa, e por que ela não é o que parece

387 ev/s contra os 733 ev/s medidos no binário antigo às 07:36. **O número menor
é o resultado melhor**, e o `lag = 0` é o que explica: não há mais backlog. Os
733 ev/s eram o sistema correndo atrás do atraso que os próprios congelamentos
acumularam. Agora ele acompanha o tópico em tempo real, com fila vazia e zero
espera bloqueada — 387 ev/s é a taxa que o `events` **produz**, não um teto.

O custo por evento (2.646 µs) inclui a primeira passada de reconstrução
correndo junto e não é comparável à linha de base; refazer a medida com a fila
drenada.

## O que esta rodada NÃO prova

Dez minutos de observação. Não prova estabilidade de RSS ao longo de horas, nem
a ausência de um episódio de parada — que é estruturalmente impossível no
desenho novo (não há tabela para dobrar), mas cuja ausência só se demonstra com
tempo. O soak continua.
