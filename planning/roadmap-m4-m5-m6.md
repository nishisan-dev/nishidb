# Próximos passos do NishiDB — M4, M5 e M6

## Context

O NishiDB fechou M1, M2 e a Fase 6: ingestão por arquivo e por Kafka, materialização,
storage durável com recovery de dados e de posição no tópico, índices bitmap, SQL, e o
protocolo remoto (`POST /v1/query`, `GET /v1/metadata`, `GET /v1/stats`,
`GET /v1/health`) servindo consulta **enquanto** a ingestão roda.

O objetivo agora é **operacionalizar**: ingestão Kafka contínua em ambiente real, com
acesso remoto seguro e diagnóstico. O cenário confirmado é o do §86 — **~500 eventos/s,
24/7**.

Esse número reordena o roadmap. O README anuncia hoje "Próximos: Fase 7 (JDBC), Fase 11
(auth/TLS), M4". A ordem correta é outra, e o motivo é aritmético:

- Segmentos são **append-only**; todo update apenda um record novo e deixa o anterior
  como garbage. **Não existe compaction em nenhum ponto do código.**
- A 500 ev/s, o §86 projeta **30–43 GB escritos por dia**.
- O estado vivo fica em **5,6–8 GB** (§84); o disco recomendado é **50–100 GB** (§85).
- Nada devolve a diferença: são **2 a 3 dias até ENOSPC**.

Um driver JDBC apontando para um processo que morreu de disco cheio não entrega nada. A
compaction vem primeiro — não por ordem de spec, mas porque é o que mantém o processo
de pé.

## Decomposição

Três milestones sequenciais. **Cada um recebe spec e plano de implementação próprios em
`planning/`**; este documento é o mapa, não o spec de nenhum deles.

| Milestone | Conteúdo | Fecha |
|---|---|---|
| **M4 — Sustentar o dataset vivo** | Compaction (§53-55), métricas de disco (§92), checkpoint por tempo, comandos de leitura do §96 | Débitos 21, 56, 34, 38, 45, parte do 17 |
| **M5 — Acesso remoto seguro** | Fase 11 (auth §74-76 + TLS §77), depois Fase 7 (driver JDBC §70-73) | Débitos 57, 14, 12, 58, resto do 17 |
| **M6 — Retenção e performance** | Fase 9 (TTL + tombstones §56-58), Fase 8 (índice hash físico), Fase 12 | Débitos 20, 24, 25, 19 |

**Por que M4 antes de M5:** o disco é a restrição que aparece primeiro — 2 a 3 dias,
contra o horizonte indefinido de "ainda não temos JDBC".

**Por que Fase 11 antes de Fase 7 dentro do M5:** o driver é *cliente* do protocolo
(§70, thin driver). Construí-lo antes da auth obriga a refazer a camada de conexão
depois — propriedades de `DriverManager`, trust store, handshake. E um driver que só
alcança loopback sem senha não é usável de fato (débito 57).

**Por que TTL depois de compaction:** o §56 é explícito — quando a row expira, "estado
físico será removido por **compaction**". TTL publica tombstone, e o tombstone continua
ocupando bytes no segmento. A compaction é a primitiva; o TTL é política sobre ela.

## Estado ao fim do ciclo do M4 (22/08/2026)

O **M4 foi entregue** — compaction completa, provada ponta a ponta pelo binário
e devolvendo disco — **com uma ressalva registrada**: o plateau em escala não
foi provado. O gate da rodada de aceite passou por uma amostra colhida fora da
janela de carga e, recontado, reprova ([`docs/aceite-m4.md`](../docs/aceite-m4.md)).

O que o ciclo entregou além do escopo planejado:

| Achado | Onde ficou |
|---|---|
| O gate de lint do projeto era **cego** desde o M2: os "114 apontamentos" eram saída truncada pelos tetos default, e a baseline real é **390** | Débitos 53 (errata) e 59 (novo) |
| O `Stats()` passou a ler espelhos atômicos e nunca bloqueia atrás do checkpoint | Task 1b |
| O primeiro dataset real em produção, com três medições que contrariaram a previsão | [`docs/piloto-events.md`](../docs/piloto-events.md) |

**A prova do plateau agora depende do piloto**, não de uma rodada sintética: o
tópico `events` tem padrão de update real e o dataset drena o backlog em ~3h30.

### O que muda para o M5, à luz do piloto

1. **A Fase 11 ganhou urgência concreta.** Há um dataset real no ar, alcançável
   só por loopback porque a porta não tem senha. Consultar de outra máquina hoje
   exige túnel SSH.
2. **O M6/TTL saiu do abstrato.** O payload do `events` traz `expireTime`,
   `ttlEspecial` e `sendClearOnExpiration`: os dados pedem TTL, e sem ele o
   estado vivo cresce com o número de alarmes distintos. A compaction recupera
   versões superadas, não entidades.
3. **Migração de formato tem prazo.** A retenção do tópico é de **~18,8 horas**.
   Qualquer mudança que exija reprocessar precisa caber nessa janela, ou aceita
   perder o histórico anterior a ela. Isso vale para o M6, que muda a semântica
   de tombstone.
4. **Falta o básico de operação de long run**: o binário não tem `--version`, e
   não há mecanismo de atualização com verificação e rollback.

---

## Decisões fechadas com o usuário

| # | Decisão | Escolha |
|---|---|---|
| **D1** | Ordem dos milestones | M4 → M5 (auth/TLS → JDBC) → M6 |
| **D2** | Cliente do JDBC | **Aplicação Java própria e ferramenta de SQL (DBeaver/IntelliJ)**, com `DatabaseMetaData` completo. **BI (Metabase/Superset) fica registrado como débito**, não como escopo |
| **D3** | Rotas mutantes antes da auth | **Não existem.** A compaction é tarefa de background regida por `storage.compaction.enabled` (§78). O M4 expõe só rotas de **leitura**; `POST /v1/compact` e `/v1/checkpoint` entram no M5, atrás do papel ADMIN |
| **D4** | Onde vive o driver JDBC | **Mesmo repositório, em `clients/jdbc/`** — contrato de wire e driver evoluem no mesmo commit |

Sobre **D2**: o §59 define a gramática v1 como `SELECT / FROM / WHERE / LIMIT`, e o §9
exclui `GROUP BY`, subqueries e `UNION` explicitamente — `ORDER BY` sequer existe no
lexer, cujas 11 keywords estão em [token.go:33](internal/query/lexer/token.go:33).
Ampliar a gramática colide com o §128 (`NO query result buffering proporcional ao
resultado`). O limite entra na documentação com dono.

Sobre **D4**: o repo passa a ser poliglota. O §97 ("binário único, sem runtime externo")
continua valendo para o `nishidb` — o `.jar` é artefato de cliente, não do engine.

---

## M4 — Sustentar o dataset vivo

A varredura do `internal/storage` mostrou que **metade da compaction já está no
repositório** como primitiva provada. O trabalho não é motor novo: é ciclo de vida de
arquivo e coordenação com o writer.

### O que já existe e será reusado

| Peça do §53-55 | Onde |
|---|---|
| Varredura streaming record-a-record, buffer reutilizado, CRC validado | `segment.scanFrom` — [segment.go:412](internal/storage/segment/segment.go:412) |
| `SizeBytes`/`TotalRecords`/`LiveRecords`/`LiveBytes` por segmento, persistidos | `checkpoint.SegmentInfo` — [manifest.go:79](internal/storage/checkpoint/manifest.go:79); tally em [store.go:788](internal/storage/store.go:788) |
| Publicação **atômica** de location (seg+offset+flags num u64) | `Pack` + `AtomicStore64` + `Publish` — [rowindex.go:32](internal/storage/rowindex/rowindex.go:32), [:171](internal/storage/rowindex/rowindex.go:171) |
| Quiesce e ciclo de vida | `vida` RWMutex + `entra/sai` + `Close` — [store.go:153](internal/storage/store.go:153), [:1268](internal/storage/store.go:1268) |
| Troca atômica de arquivo (construir novo → msync → rename → swap sob Lock) | `primary.Index.grow()` — [index.go:245](internal/index/primary/index.go:245); `escreveAtomico` — [manifest.go:250](internal/storage/checkpoint/manifest.go:250) |

O comentário de [manifest.go:250](internal/storage/checkpoint/manifest.go:250) declara
`escreveAtomico` como **o único caminho de durabilidade provado do projeto**, com
instrução explícita de não duplicá-lo. A compaction deve passar por ele.

### Tarefa 1 — tornar a curva visível (antes de tudo)

`nishidb_storage_bytes` é **exato hoje**: soma de `SegmentInfo.SizeBytes`, que é o
tamanho do arquivo. Só `live_bytes` e `garbage_bytes` dependem do tally que superestima.
Hoje [metricas_dataset.go:16](internal/server/metricas_dataset.go:16) deixa os **três**
sem publicar de propósito — o comentário argumenta, corretamente, que publicar tally
impreciso "seria uma métrica que mente".

**O argumento não alcança `storage_bytes`.** Publicá-lo é barato e é exatamente o alarme
que antecede o ENOSPC. Com 2 a 3 dias de margem, ter o gráfico é o que separa uma janela
de manutenção de um incidente — por isso vai primeiro, e sozinho.

`garbage_bytes` entra junto se for publicado pelo que é: como `live` é limite
**superior**, `total - live` é limite **inferior** do garbage. "Há pelo menos isto de
lixo" é afirmação verdadeira e útil, desde que a documentação diga isso.

### Tarefa 2 — a compaction

Três restrições estruturais encontradas na varredura governam o desenho:

1. **Renumeração é proibida:** `checkHeader` exige que o `segmentID` do header case com
   o nome do arquivo ([segment.go:70](internal/storage/segment/segment.go:70)), e
   `openNew` usa `O_EXCL`.
2. **Reescrita in-place é fatal:** segmento menor que o registrado no manifest é erro de
   recovery ([store_recovery.go:67](internal/storage/store_recovery.go:67)).
3. **`Publish`, `ApplyOverlay` e `segTally` são writer-only por contrato explícito**
   ([rowindex.go:170](internal/storage/rowindex/rowindex.go:170)) — uma goroutine
   própria republicando locations viola a invariante.

Abordagem recomendada, a validar no design do M4:

- **Copy-forward pelo writer.** Em vez de construir arquivo novo com ID próprio, a
  compaction varre o segmento candidato e **re-apenda os records vivos pelo caminho
  normal de escrita**, que já rotaciona, já mantém tally e já é durável. As três
  restrições acima deixam de existir. O custo é reescrever os bytes vivos no tail — que
  é o que a compaction faz de qualquer forma.
- **Republicação condicional sob `wmu`.** Varredura e cópia ficam fora do lock; a
  republicação toma `wmu` ([store.go:656](internal/storage/store.go:656)) e, por row, só
  publica se a location ainda apontar para o record de origem. Se a ingestão já moveu a
  row, a cópia vira garbage e a location corrente vence. Não exige primitiva CAS nova.
- **Remoção do segmento esgotado.** Terminado o copy-forward, a origem tem
  `liveRecords == 0` **por construção**. A janela a fechar é a de crash entre o unlink e
  a reescrita do manifest. Candidato: `ListSegments` **já ignora nomes não-canônicos**
  ([segment.go:252](internal/storage/segment/segment.go:252)), então renomear para
  `.retired` retira o segmento do manifest seguinte sem apagá-lo, e a regra de recovery
  correspondente — restaurar o `.retired` se o manifest ainda o listar — é pequena e
  testável com crash real.

Buraco na cadeia **abaixo de `CurrentSegment` não é detectado**
([store_recovery.go:82](internal/storage/store_recovery.go:82)), então remover segmento
antigo é estruturalmente tolerado desde que o manifest concorde.

Como consequência da varredura, os **débitos 38 e 45 fecham**: o tally passa a ser
reconstruído do que foi efetivamente copiado, e as postings órfãs do bitmap somem junto
com o segmento de origem. Vale notar que eles nunca bloquearam a compaction — o tally
impreciso serve só para *eleger candidato* (§53), e superestimar liveness apenas
**adia**, que é a direção segura já registrada no débito 38.

### Tarefa 3 — resto do escopo

- `storage.compaction: {enabled, minimumLiveRatio}` — hoje aceita e ignorada
  ([config.go:29](internal/config/config.go:29)). Seguir a doutrina do débito 49: forma
  validada no parse, comportamento na ativação.
- Métricas `nishidb_compaction_running`, `_bytes_read`, `_bytes_written` — nomes já
  existem sem alimentador em [nomes.go:48](internal/metrics/nomes.go:48).
- `Store.Stats()` precisa expor liveness e contagem real de segmentos: `SegmentsTotal`
  hoje é derivado do ID corrente e assume "sem remoção"
  ([store.go:1245](internal/storage/store.go:1245)), premissa que a compaction quebra.
  Fecha o **débito 34** junto, que já tem M4 como dono.
- **Débito 56** — checkpoint disparado por tempo no laço do consumidor, para o lag parar
  de congelar com a ingestão ociosa.
- **§96, parte de leitura** — `status`, `datasets`, `inspect`. Hoje o CLI tem 4 comandos
  e só `server` é do §96 ([cli.go:113](internal/cli/cli.go:113)). Nascem **remotos**
  (`--server`), pelo argumento do flock exclusivo — se forem só locais, repetem o débito
  54: o operador teria que derrubar o servidor para perguntar o status dele. `/v1/stats`
  é a base, e `internal/cli/query_remota.go` é o molde do cliente.

---

## M5 — Acesso remoto seguro

### Fase 11 — auth e TLS

Não existe **nenhuma** camada de middleware no servidor: o mux vai direto para
`http.Server.Handler` ([server.go:373](internal/server/server.go:373)). Isso é bom — a
auth entra envolvendo o mux num ponto só. O listener também é criado num ponto único
([server.go:174](internal/server/server.go:174)) e o `Serve` é isolado
([server.go:382](internal/server/server.go:382)), então `ServeTLS` entra em um lugar só.

- **Config:** `server.tls.{enabled,certificateFile,privateKeyFile}` e
  `server.authentication.{enabled,users[]}` com `username`, `passwordEnv`|`passwordFile`
  e `role: READ|ADMIN`. Hoje as duas seções são aceitas e ignoradas **em silêncio** —
  nem o aviso `config.sem_efeito` que as chaves de `runtime` recebem
  ([config.go:246](internal/config/config.go:246)). O §75 exige mais: "senha literal no
  YAML não deve ser aceita" precisa ser **erro de carregamento nomeando a chave**.
- **Argon2id (§76) fica fora, e a razão importa.** O §76 condiciona: "*quando usuários
  forem persistidos internamente*". No §75 a senha vem de env ou arquivo — o NishiDB
  não persiste usuário nenhum. A verificação correta é comparação em tempo constante com
  `crypto/subtle`, que é stdlib, e Argon2id passa a valer se algum dia existir user
  store. Isso preserva a doutrina de **zero dependências novas** que a Fase 6 sustentou
  (`golang.org/x/crypto` está no `go.mod` só como indirect).
- **§77 — "autenticação remota deve exigir TLS por padrão":** auth ligada + TLS
  desligada + bind não-loopback deve **recusar a subida**. Sem isso a senha viaja em
  claro, e o aviso de exposição atual
  ([server.go:208](internal/server/server.go:208)) vira falsa tranquilidade. Há
  precedente de `tls.Config` com `MinVersion: TLS12` no cliente Kafka
  ([config.go:107](internal/kafka/config.go:107)).
- **Papéis:** READ nas rotas de leitura; ADMIN nas mutantes. É aqui que entram
  `POST /v1/compact` e `POST /v1/checkpoint`, fechando o resto do §96 (D3).
- **CLI:** `nishidb query --server` ganha credencial — por **variável de ambiente ou
  arquivo**, nunca flag de senha, que apareceria em `ps`. `normalizaEndereco` já aceita
  `https://` ([query_remota.go:64](internal/cli/query_remota.go:64)).

Fecha os débitos **57** e **14**.

### Fase 7 — driver JDBC

O wire está pronto: `internal/protocolo` é declaradamente o ponto de encontro do
servidor, do CLI remoto e do futuro driver
([protocolo.go:6](internal/protocolo/protocolo.go:6)). O consumidor de referência a
portar para Java é `protocolo.Leitor`
([stream.go:45](internal/protocolo/stream.go:45)); a tabela §73 já é única no produto
([jdbc.go:7](internal/protocolo/jdbc.go:7)); e a tradução código→exit code
([query_remota.go:124](internal/cli/query_remota.go:124)) vira código→`SQLState`.

**Ajustes no lado Go, antes do Java** (senão o driver nasce com buracos):

- `nullable`, `precision` e `displaySize` para o `ResultSetMetaData` — nem
  `protocolo.Coluna`, nem `ColunaMeta`, nem `executor.ColMeta` carregam isso.
- Versão do produto, para `DatabaseMetaData.getDatabaseProductVersion()` — não existe
  rota nem campo.
- Mover `Saude` de [http.go:38](internal/server/http.go:38) para o pacote `protocolo`:
  é o único corpo de rota do §67 que um cliente teria de redeclarar.
- Uniformizar o 405: `/v1/health` e `/metrics` caem no 405 do mux com `text/plain`, fora
  do contrato `protocolo.CorpoErro` ([server.go:361](internal/server/server.go:361)).
- **Débito 58** — `query --server` passa a mandar `parameters`. Além de fechar o débito,
  é o que dá teste ponta a ponta do caminho de parâmetros **pelo próprio produto**.
- **Mensagem de cláusula não suportada.** Hoje `ORDER BY` sai como *"conteúdo inesperado
  após o fim da query"* ([parser.go:46](internal/query/parser/parser.go:46)), porque a
  keyword não existe no lexer. Acrescentar `ORDER`, `GROUP`, `OFFSET`, `JOIN`, `HAVING`,
  `DISTINCT`, `UNION` ao lexer **apenas para produzir erro específico** — sem ampliar a
  gramática — é barato e é o que separa "cláusula não suportada" de "erro de digitação"
  para quem escreve SQL no DBeaver. Pede um código novo, `UNSUPPORTED_CLAUSE`.

**O driver:** `clients/jdbc/`, Java 21+, classes mínimas do §71, tudo o mais lançando
`SQLFeatureNotSupportedException`. Connection string
`jdbc:nishidb://db01:9142/radius_sessions`, com TLS e credenciais via `Properties`.
Testes do §107.

**`setNull` não tem representação de wire** — o protocolo recusa `null` em `parameters[]`
por decisão explícita ([parametros.go:52](internal/protocolo/parametros.go:52)), e a
única forma de testar ausência é `IS NULL` na query. O driver lança
`SQLFeatureNotSupportedException` e o limite fica registrado.

---

## M6 — Retenção e performance

Escopo esboçado; ganha spec própria quando o M5 fechar.

- **Fase 9 — TTL e tombstones (§56-58).** O bit `FlagTombstone` já existe e é honrado na
  leitura em três pontos ([store.go:585](internal/storage/store.go:585),
  [:622](internal/storage/store.go:622), [:1095](internal/storage/store.go:1095)), mas
  **nada em produção o cria**. Ligar o TTL torna reais os **débitos 24 e 25**: hoje
  `GetByKey` mascara tombstone como ausente, o stale check do `_SEQ` não roda e a row
  **ressuscita** — exatamente o que o §58 diz que o tombstone deve impedir. Os dois
  precisam ser corrigidos **na mesma unidade** que liga o TTL.
- **Fase 8 — índice hash físico (débito 19).** Planner e executor já implementam a rota
  e a testam contra o dublê; no real degrada para full scan.
- **Fase 12 — memory optimization.**

---

## Correções pontuais a incorporar

Encontradas na varredura; pequenas, mas são prosa que não bate com o código:

- [config.go:71](internal/config/config.go:71) e
  [:245](internal/config/config.go:245) afirmam que a subida avisa sobre
  `storage.durability` "como já faz". **Não há campo nem aviso** — a chave é aceita e
  ignorada em silêncio. Ou implementar o aviso (fechando parte do débito 50) ou corrigir
  o comentário.
- [manifest.go:35](internal/storage/checkpoint/manifest.go:35) descreve 12 bytes por
  segmento; o `encode` grava 36 desde os stats de liveness, e o `make` de capacidade em
  [:110](internal/storage/checkpoint/manifest.go:110) usa o `*12` desatualizado (só custo
  de realloc).
- README: a linha "Próximos: Fase 7, Fase 11, M4" passa a refletir a ordem D1.

---

## Verificação

**M4**

- `go test ./...` e `go test ./... -race` verdes; `golangci-lint run` sem apontamento no
  código novo, conferido contra a linha de base de **390** — com `max-issues-per-linter`
  e `max-same-issues` em **0**, no `.golangci.yml` e no alvo `lint`, e com o instrumento
  fixo — **golangci-lint v2.12.2 / go1.26.5**, a versão que o `go install` do alvo instrui
  e que o débito 53 registra: versão solta mede outra linha de base. Os **114** que esta
  linha citava saíram de circulação: eram a saída **truncada** pelos tetos default, e um
  linter estourado no teto absorve o apontamento novo em silêncio, de modo que "114
  antes, 114 depois" seguia verde com código sujo. O que o M2 e a Fase 6 alegaram sob o
  instrumento truncado continua sem prova — débitos 53 e 59 em `docs/debitos.md`.
- **Crash real no meio da compaction**, no molde de
  `TestKill9NoMeioDaIngestaoPreservaOEstadoConfirmado` — mata o binário com SIGKILL
  durante o copy-forward e confere que o estado confirmado sobrevive e que nenhum
  segmento `.retired` deixa o dataset inconsistente.
- **A prova que importa é a curva de disco.** O harness já mede `DiskBytes`
  ([harness.go:289](benchmark/ingest-bench/harness.go:289),
  [:492](benchmark/ingest-bench/harness.go:492)). O gate novo: carga sustentada de
  **updates sobre key set fixo**, com o disco **estabilizando em plateau** em vez de
  crescer linearmente. Sem esse gate, a compaction pode "funcionar" nos testes unitários
  e não devolver disco em regime.
- Rodada de aceite com Kafka real, no molde de `docs/aceite-fase6.md`, com o novo
  `docs/aceite-m4.md`.

**M5**

- Teste de **recusa de subida**: auth ligada + TLS desligada + bind exposto.
- Teste de 401 e de 403 por papel, incluindo ADMIN nas rotas mutantes.
- Teste fim a fim pelo CLI com credencial por env, conferindo que a senha não aparece em
  `ps` nem em log (§95 já proíbe registrar parâmetro).
- Testes do §107 para o driver, e um teste de integração que suba o servidor e conecte o
  `.jar` de verdade.

**Documentação (Definition of Done do projeto)**

Cada milestone atualiza `docs/architecture.md`, os `.puml` afetados
(`storage_layout`, `c4_container`, e um `compaction_flow.puml` novo), `docs/debitos.md`
com os fechamentos e os itens novos, e o README. `TestEvidenciasCitadasNosDocumentosExistem`
continua valendo para os nomes de teste citados.
