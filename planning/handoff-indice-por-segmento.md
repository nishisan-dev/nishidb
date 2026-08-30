# Handoff — subsistema 1: índice por segmento

Estado em 30/08/2026. Branch `feature/indice-por-segmento`, 59 commits sobre
`main`, 166 arquivos, +21.322/−5.378.

## O que este ciclo faz

Substitui os **dois motores globais de índice secundário** (`hash` e `bitmap`)
por **um índice por segmento**, unificado em `internal/index/segindex`. Os
pacotes `internal/index/hash` e `internal/index/bitmap` foram **apagados**.

O defeito que motivou tudo, medido no piloto e registrado em
`benchmark/baselines/piloto-pre-ciclo-20260830.md`: o índice global é uma tabela
única que **dobra e recopia** quando enche. Com 47 M de linhas e o índice em
3,46 GiB, a recópia congelou a ingestão por entre 18 e 52 minutos, com 99,16% do
CPU numa só pilha (`hash.Index.reconstroi`) e 64% dentro de um único
`atomic.Int32.Add` — o mutex por slot do `mmap/region.go`, tomado uma vez por
slot lido. A recópia corre sob `wmu`, então nada entra enquanto ela dura.

No desenho novo **não existe rehash**: um `.sidx` selado é escrito uma vez,
ordenado, dimensionado exato, `PROT_READ`, imutável. O modo de falha não fica
mais raro — fica impossível de exprimir.

## Documentos que mandam

| Papel | Arquivo |
|---|---|
| **Spec normativa** (autoridade) | `planning/spec-subsistema-1-indice-por-segmento.md` |
| Plano de implementação (17 tarefas) | `planning/plano-subsistema-1-indice-por-segmento.md` |
| **Diário do ciclo** (rulings, medições, erros) | `.superpowers/sdd/plano-subsistema-1-indice-por-segmento/progress.md` |
| Direção de produto | `planning/direcao-indices-por-segmento-e-consulta-longa.md` |
| Medição pré-ciclo do piloto | `benchmark/baselines/piloto-pre-ciclo-20260830.md` |

O diário é o mapa de recuperação: ele tem ~40 `Ruling:` com o porquê de cada
decisão, e os commits que ele nomeia existem no git mesmo que o contexto não
lembre de tê-los criado. **Leia-o antes de decidir qualquer coisa.**

## Onde está o trabalho

- `internal/index/segindex/` — formato, escritor, selado, WIP, selagem, união.
  Duas formas: **WIP** (mutável, só o segmento corrente) e **selado** (imutável,
  um arquivo por segmento, diretório ordenado + busca binária).
- `internal/storage/indices*.go` — `aposAppend` (ponto único de rotação sobre
  **quatro** sítios de `Append`), `aplicaIndiceSets`, `ConstroiIndiceDoSegmento`,
  `SegmentosPendentesDeReconstrucao`, `AplicaJanela`, `JanelaEfetiva`.
- `internal/server/historico.go` — `ligaRetencao`, `ligaReconstrucao`, métricas.

## Tarefas: 16 de 17 feitas

| # | Tarefa | Estado |
|---|---|---|
| 1–5 | O motor (`segindex`) | ✅ |
| 6–9 | Storage; apaga `hash` e `bitmap` | ✅ |
| 10–12 | Superfície: recusa, interseção, unificação interna | ✅ |
| 13 | A janela de indexação | ✅ |
| 14 | Laço de reconstrução — **fecha o débito 22** | ✅ |
| 15 | **Disciplina de mutação** (documento) | ❌ **falta** |
| 16 | Métricas | ✅ (2 rodadas de correção; 20 achados) |
| 17 | **Docs + aceite no piloto** | ❌ **falta** |

## O que fazer, em ordem

### 1. Piloto — JÁ SUBIU em 30/08 11:52. O soak está correndo.

Binário `v0.6.0-250-ge13a6dd` no ar, dataset de 135 GiB migrado em produção
(manifest v2 → v3, índices globais apagados do disco). Resultado registrado em
`benchmark/baselines/piloto-pos-ciclo-20260830.md`:

| Grandeza | Antes | Depois |
|---|---|---|
| RSS | 2.812 MiB | **432 MiB** |
| Bytes de índice | 3.716 MB | **614 MB** |
| `poll_blocked` | 32.319 s | **0** |
| Fila / lag | 5.000 (teto) / backlog | **0 / 0** |
| Congelamentos | 18–52 min, recorrentes | **nenhum** |

A migração drenou sozinha: `pendente` 63 → **0** e `selado` 1 → **65** em ~5
minutos, com a ingestão correndo ao lado, e ficou drenada. `fora = 372` — a
janela poupou a esmagadora maioria dos segmentos, que é o que tornou a migração
barata.

**Cuidado ao ler a taxa:** 387 ev/s contra 733 do binário antigo. O número menor
é o resultado melhor — o `lag` zerou. Os 733 eram o sistema correndo atrás do
atraso que os próprios congelamentos acumulavam.

**O que falta aqui:** acompanhar o soak por ~1 dia e refazer a medida de custo
por evento com a fila drenada (a de 2.646 µs incluía a primeira passada de
reconstrução correndo junto, e não é comparável). Confirmar que o RSS fica
plano e que nenhum episódio de parada aparece.

**Se precisar subir de novo**, o que segue continua valendo:

Acesso: `ssh root@10.200.20.218`, direto. Dados são **descartáveis por
autorização permanente do usuário** — pode apagar, resetar offset, remandar o
tópico do início.

**Cuidado com o proxy:** o `.bashrc` do root exporta `http_proxy`, e o loopback
devolve 403. Use **sempre** `curl --noproxy '*'`. Métricas em `127.0.0.1:9142`,
pprof em `127.0.0.1:9144`.

Layout: `/app/nishidb/current` → symlink para `releases/<versão>`; unidade
systemd `nishidb.service`; runbook em `/app/nishidb/RUNBOOK.md`.

**Dois pré-requisitos de config, descobertos por ensaio — sem eles não sobe:**

1. `index: hash` e `index: bitmap` são **recusados**. O binário sai nomeando o
   campo: *"foram substituídos por um índice único — escreva `index: true`"*. O
   YAML do piloto (`/app/nishidb/datasets/events.yaml`) tem **10** declarações a
   converter.
2. Sem o bloco `history.index`, a janela nasce **infinita** e a reconstrução
   listaria os 135 GB inteiros. Declare antes de subir:
   ```yaml
   history:
     index:
       window: 6h
       maxBytes: 2GiB
   ```
   São os valores do exemplo canônico da §4.1 da spec.

Também devolva `node` ao `history.fields` com `index: true` — é o que o aceite
exige medir.

**Compile a partir de um clone limpo**, não da árvore: há um arquivo de outra
sessão modificado nela, e o binário sai `-dirty`. `make build` é a **única**
fonte do build (os ldflags importam: sem eles o `nishidb-update.sh` dispara
rollback espúrio).

Procedimento: snapshot do YAML datado + `offsets --show` guardado → `systemctl
stop nishidb` → nova pasta em `releases/` com binário e YAML → repontar
`current` → `systemctl start` → acompanhar.

**ARMADILHA que já derrubou uma partida:** a unidade systemd roda
`ExecStart=/app/nishidb/current/nishidb server --config /app/nishidb/current/nishidb.yaml`
— o `nishidb.yaml` tem de estar **dentro da pasta da release**, não em
`/app/nishidb/`. Copie-o da release anterior. Há também um
`/app/nishidb/bin/nishidb` **obsoleto** que nem lê o YAML atual (erro de
`TTLSpec`); use sempre o binário da release corrente para `offsets`.

Rollback em `/app/nishidb/`: `datasets/events.yaml.bak-20260830-114938`,
`offsets.bak-20260830-114938` (`0=56842934477`), `snapshot-20260830-114938/`
com os manifests, e a release anterior `v0.6.0-188-gf41ef67` intacta. Note que o
manifest v3 é irreversível para binário antigo.

**Critério de aceite:** com `node` indexado no histórico, manter ou melhorar
1.636 ev/s e RSS ~227 MB, `poll_blocked` sem crescer, e **nenhum congelamento**.
Compare contra `benchmark/baselines/piloto-pre-ciclo-20260830.md` — mas note
que o piloto degradou para **733 ev/s** com o dataset maior, então a comparação
honesta é contra o número do mesmo tamanho de dataset, não contra o de ontem.

O RUNBOOK da máquina está desatualizado em três pontos que a subida deve
corrigir: afirma "sem autenticação e sem TLS" (o YAML tem os dois), diz que
`node` está indexado no histórico (o paliativo o removeu), e lista o débito 22
como limite conhecido (a tarefa 14 o fechou).

### 2. Tarefa 15 — disciplina de mutação

Pura documentação, não bloqueia nada, **cabe dentro do soak**. Produz
`docs/mutacoes-indice-por-segmento.md`. As sete mutações estão especificadas no
plano (linhas ~2489-2566). Acrescente o registro honesto do que a suíte **não**
alcança.

**Acrescente também a contagem do padrão recorrente** (abaixo) — é o achado mais
valioso do ciclo e o plano não o previa.

### 3. Tarefa 17 — docs e aceite

`docs/architecture.md` (só número medido, com origem em `benchmark/baselines/`
ou num `docs/aceite-*.md`), `docs/diagrams/indice_por_segmento.puml`,
`docs/debitos.md`, README, descrição da release. **Não há CHANGELOG** — o
changelog vive na descrição da release (§8 do CLAUDE.md global).

A nota de upgrade **tem de** conter os dois pré-requisitos de config do item 1.

### 4. Revisão de branch inteira, depois `superpowers:finishing-a-development-branch`

## O achado central do ciclo

**Vinte vezes** um teste passou com o defeito presente. Não é anedota — é o
padrão dominante, e foi encontrado por mutação, nunca por leitura.

Exemplos: golden autocumprido (gerado pelo escritor que ele deveria conferir);
teto de sondagem nunca exercitado; teste concorrente vacuamente concorrente;
bancada de métricas onde 5 dos 10 números eram zero, permitindo trocar dois
rótulos sem ficar vermelho; teste de convergência cego porque a fixture não
tinha variedade **e folga**.

**Consequência prática para quem continuar:** para toda invariante nova, aplique
a mutação, veja vermelho, reverta, veja verde — e relate os dois. Um teste de
convergência que passa com o defeito presente não prova nada.

## Convenções que quebram o build se ignoradas

- **Tudo em PT-BR**: código, identificadores, nomes de teste, comentários, logs,
  docs, mensagens de commit. Commits na voz do time, **sem menção a agente/IA**.
- **Lint é ratchet bidirecional**: `.golangci-baseline` está em **372**. O CI
  falha **acima e abaixo**. `make lint` local sai != 0 enquanto a baseline > 0 —
  é o esperado. Quem move a contagem ajusta o arquivo no mesmo PR com a razão.
- **Sem fallback para legado.** Migração vai sempre para o definitivo.
- **`make build` é a única fonte do build.** Nunca um `go build` próprio.
- **Três arquivos NÃO são desta branch e não podem ser tocados nem commitados:**
  `planning/m6-ciclo-query-longa-ordenacao-densidade.md` (modificado),
  `CLAUDE.md` e `planning/melhorias-performance-fase1-leitura-scan.md`. Eles
  aparecem sujos no `git status` de propósito. **Nunca `git add -A`** — sempre
  por caminho explícito.
- Ao fazer grep, excluir `.claude/worktrees/`.
- Docs mudam no MESMO commit que o código.
- **Agentes de revisão já deixaram mutação na árvore.** Confira `git status`
  antes de commitar, e desconfie de constante hardcoded ou lock acrescentado.

## Invariantes do desenho que não podem quebrar

- **Contrato de superconjunto**: `Lookup` devolve candidatos em ordem
  estritamente crescente, e o executor **sempre** reavalia o WHERE completo
  (§44). **Falso positivo é tolerado; falso NEGATIVO nunca.**
- **Não existe fase `Clear`** (§5.2 da spec): a posting do valor antigo fica no
  índice do segmento ANTIGO, vira falso positivo tolerado, e morre quando o
  segmento se aposenta. Isso removeu o mecanismo de lápide.
- **Ordem de locks**: `vida → wmu → aposMu`, com `retPassadaMu` **acima** da
  cadeia. A coleta de métrica **não pede lock nenhum** — quem tem o lock publica
  em atômico.
- **Planner nunca falha**: toda dúvida degrada para FullScan.
- Segmento sem índice responde por **enumeração** — correto, só lento.
- **A decisão de união vem do DADO** (min/maxRowID nos cabeçalhos), nunca de
  `EhHistorico()`.

## Débitos abertos por este ciclo

- **103** — o `.sidx` reprovado no `crc32c` não é curado por ninguém: responde
  por enumeração para sempre. É o gatilho 3 da §2.4, e ele **não tem dono**: a
  fila lê a tabela de seções sem conferir crc, então o arquivo corrompido parece
  completo. Consequência limitada e correta (enumeração), nunca falso negativo.
- **102** — a selagem sob `wmu` a cada rotação ainda toca a ingestão.
- `internal/kafka/metricas.go` — nove amostras sem o campo `Familia`, então os
  cabeçalhos `# HELP`/`# TYPE` saem com nome vazio. Não urgente: **não há
  Prometheus no piloto**. Fora do escopo deste ciclo por decisão registrada.

## Em aberto, sem decisão

- **Passada de reconstrução sem teto de segmentos.** O pacing é entre passadas,
  como na compaction. Numa base migrada a primeira passada varre tudo em
  sequência, e o operador não vê progresso. O revisor julgou que o remédio certo
  é **observabilidade primeiro** (tarefa 16) e teto só depois — teto sem o
  indicador da fila troca opacidade por lentidão opaca. Reavalie depois do soak.
