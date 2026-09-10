# Plano do ciclo — M5 + fundação de operação

## Context

O M4 fechou: a compaction devolve disco, provada ponta a ponta com `kill -9`, com a
ressalva registrada do plateau em escala. O primeiro dataset real está em produção — o
piloto do tópico `events` (host `10.200.20.218`, `/app/nishidb`, unit `nishidb.service`,
bind `127.0.0.1:9142` sem auth, ~500 ev/s, retenção do tópico ~18,8 h). O roadmap
(`planning/roadmap-m4-m5-m6.md`) fixou o M5 como **Fase 11 (auth §74-76 + TLS §77) →
Fase 7 (driver JDBC §70-73)**, decisões D1–D4 fechadas. O piloto acrescentou as
urgências das issues #10 (plateau), #11 (`--version` + atualização com rollback),
#14 (CI inexistente) e #15 (chave >1 KiB descartada em silêncio).

**Decisões do usuário (2026-08-22):**
1. Escopo: **M5 + fundação de operação** — CI, `--version` e atualização com rollback antes da Fase 11.
2. O ciclo **fecha com a Fase 11 implantada no piloto** (bind exposto, TLS, usuários reais).
3. Issue #15: neste ciclo **só medição do backlog inteiro + alerta**; mudança de formato depois.
4. Observabilidade pós-auth: **plano duplo** — porta exposta com TLS+auth em TODAS as
   rotas (health inclusive); listener loopback opcional (`server.loopback`, default
   false) com o mesmo mux sem auth/TLS, preservando painel, script e cron intocados.

Ordem do ciclo: **Fase A (Fundação) → Fase B (Fase 11) → Fase C (Deploy piloto) →
Fase D (Fase 7 JDBC)**, com a **Trilha P** (plateau, observação) em paralelo desde o
início. Validada sem defeito estrutural; a Fase D pode começar após o merge da B,
em paralelo com a janela de observação da C.

Fatos verificados que corrigem os documentos: o débito 44 está desatualizado
(`internal/kafka/integration_test.go` declara a tag `integration`); as correções
"comentário do manifest" e "README com ordem D1" do roadmap já foram feitas no M4;
o teto `MaxKeySize = 1<<10` é decisão de implementação
([record.go:36](internal/storage/encoding/record.go:36)), não de spec; não existe
nenhuma tag git; a suíte roda sem broker externo (kfake em memória).

> Convenção do projeto: ao aprovar, copiar este plano para `planning/` (ex.:
> `planning/m5-plano-ciclo.md`) no primeiro commit do ciclo.

---

## Fase A — Fundação de operação

**Pronto quando:** CI verde obrigatório em PR; `--version` em todas as superfícies;
script de atualização com rollback provado por teste; primeira release/tag publicada;
issues arrumadas; distribuição do identifier medida e registrada.

### A1 — Housekeeping de issues (sem código)
- Fechar **#1** (aponta `docs/aceite-m2.md` + seção M2 de `docs/debitos.md`) e **#2**
  (aponta `docs/aceite-fase6.md`).
- **#4**/**#5**: título/rótulo de "M4" para **M6** (conteúdo é M6 pelo roadmap).
- **#3**/**#7**/**#12**: comentário registrando que este ciclo os executa na ordem D1.

### A2 — CI (#14): `.github/workflows/ci.yml`
- Jobs em PR + push main: `test` (`go test ./...`, inclui gates de alocação e e2e
  lentos — poucos minutos no total), `race` (`go test ./... -race`, complementar por
  construção), `lint` (golangci-lint **exatamente v2.12.2** — mesma instrução do
  [Makefile:60](Makefile) —, `GOLANGCI_LINT_CACHE` isolado, tetos zerados, comparação
  contra baseline **390** em arquivo versionado `.golangci-baseline` com **ratchet
  bidirecional**: maior reprova, menor reprova pedindo para abaixar a baseline no
  mesmo PR), `fmt` (`gofmt -l` vazio). Go **pinado em 1.26.5** (a baseline não se
  separa do instrumento — débito 53).
- `bench` só por `workflow_dispatch`; **`ingest-bench` com Kafka real NÃO entra em CI**
  (rodada de aceite manual, como hoje).
- **Oráculo do próprio CI**: `internal/gates/ci_test.go` no molde de `lint_test.go`,
  lendo o workflow como dado — versão do golangci-lint byte a byte igual à do Makefile,
  os dois alvos de teste presentes, versão de Go fixada. Sem isso o workflow diverge do
  Makefile em silêncio (o defeito do débito 53 por outra porta).
- `release.yml` disparado por release publicada: builda com os ldflags do A3 e anexa
  `nishidb-<versão>-linux-amd64`. Release/tag é ato manual (`gh release create` antes
  da tag, convenção do usuário). Primeira tag no fecho da Fundação (recomendação:
  `v0.5.0`; sem tags retroativas de M1/M2).

### A3 — `--version` (#11, parte 1)
- Pacote novo `internal/buildinfo`: `Version`, `Commit`, `Date` via `-ldflags -X`, com
  fallback `runtime/debug.ReadBuildInfo()` (`vcs.revision`/`vcs.time`).
- Superfícies (teste primeiro em cada): `nishidb --version`/`version` no dispatch de
  [cli.go](internal/cli/cli.go); log de subida em `runServer`; campo em
  `protocol.Stats` ([documents.go:63](internal/protocol/documents.go:63))
  alimentado por `handleStats` — **isto adianta o
  `DatabaseMetaData.getDatabaseProductVersion()` da Fase 7**; cabeçalho do painel
  ([nishidb-stats.py](scripts/nishidb-stats.py)); campo `nishidb_version` no `Report`
  do harness ([harness.go:222](benchmark/ingest-bench/harness.go:222)).
- Alvo `build` do Makefile ganha os ldflags (mesma linha que o `release.yml` usa).

### A4 — Script de atualização com rollback (#11, parte 2)
- **A unidade de instalação e rollback é o par (binário, config)** —
  `/app/nishidb/releases/<versão>/{nishidb, nishidb.yaml}` com symlink `current`
  apontando para o **diretório** (desvio deliberado do layout `bin/` da issue #11).
  Motivo: o binário velho ignora `server.tls`/`server.authentication` **em silêncio**
  ([config.go:165](internal/config/config.go:165), `KnownFields` desligado) — rollback
  só de binário com config nova subiria **exposto, sem auth, health UP, verde**. O pior
  estado possível é silenciosamente verde.
- `scripts/nishidb-update.sh`: pré-checagem (versão corrente via `/v1/stats` com
  `curl --noproxy '*'` — armadilha do proxy documentada em `docs/piloto-events.md`) →
  instala release → `systemctl stop` → troca symlink → start → **verificação com
  prazo**: `/v1/health` UP, `nishidb_ingest_events_total` avançando,
  `nishidb_ingest_invalid_total` parado, e **versão nova confirmada via `/v1/stats`**
  (única defesa contra binário-velho-config-nova) → **rollback automático** em
  qualquer falha, conferindo que a versão velha voltou saudável, exit ≠ 0 nomeando o
  que falhou. Fala com o **plano loopback** (sem credencial, por decisão 4).
- **Oráculo do rollback**: override `NISHIDB_UPDATE_SYSTEMCTL`; teste Go executa o
  script com `systemctl` dublê e binário que falha a saúde de propósito, afirmando
  symlink revertido e exit ≠ 0. Rollback sem prova é mecanismo-sem-oráculo.

### A5 — #15: medição + alerta
- Ferramenta read-only `scripts/identifier-len/main.go` (`go run`, franz-go já é
  dependência, group descartável, sem commit de offset): histograma do tamanho de
  `identifier` sobre o backlog inteiro (~30,6 M msgs; leitura a ~2.700 ev/s cabe na
  janela) — buckets ≤256/≤512/≤768/≤1024/>1024 + p50/p95/p99/máximo. Resultado entra
  em `docs/piloto-events.md` (medição 4) e na issue #15 para a decisão futura.
- Alerta: o painel já marca `invalid_total > 0` com "← investigar"
  ([nishidb-stats.py:288](scripts/nishidb-stats.py:288)); acrescentar quebra por
  rótulo `motivo` (o contador já o tem) e destaque de **taxa** no `--watch` (invalid
  subindo entre amostras é o sinal). Entrada no RUNBOOK na Fase C.

### A6 — Correção pontual
- [config.go:92](internal/config/config.go:92) promete aviso de `storage.durability`
  que não existe: implementar o aviso `config.sem_efeito` (fecha parte do débito 50;
  chave aceita em silêncio é configuração que mente).

---

## Fase B — Fase 11: autenticação e TLS

**Pronto quando:** oráculos abaixo verdes em CI; baseline de lint mantida; docs DoD;
entrega no ciclo só conta com a Fase C concluída.

### B1 — Config: forma no parse
Structs novas em `internal/config/config.go`: `server.tls{enabled, certificateFile,
privateKeyFile}`, `server.authentication{enabled, users[]{username, passwordEnv,
passwordFile, role}}`, `server.loopback{enabled, port}` (default false; porta
colidindo com o bind recusada). Recusas de carregamento, uma por teste, com a chave no
erro: **senha literal no YAML** (campo `password` presente = erro nomeando a chave,
§75); `role` fora de {READ, ADMIN}; exatamente um entre `passwordEnv`/`passwordFile`;
username duplicado; `tls.enabled` sem cert/key; `authentication.enabled` com `users`
vazio; **chave desconhecida dentro de `tls`/`authentication` é erro** (bloco de
segurança não engole typo — `enabld: true` engolido é o typo mais caro do arquivo).

### B2 — Ativação: segredos e a matriz do §77
Resolução na subida, fail-fast, molde de `kafka.Resolve`
([config.go:154](internal/kafka/config.go:154)): env ausente/vazia recusa nomeando a
variável; `passwordFile` ilegível recusa; **group/world-readable recusa** nomeando
caminho e modo; trailing `\n` aparado. Sem recarga a quente (`passwordEnv` não é
recarregável por construção; restart custa ~0,28 s) — SIGHUP-reload vira débito nomeado.

**Matriz de recusa** (substitui o aviso de [server.go:216](internal/server/server.go:216)):

| bind | auth | TLS | resultado |
|---|---|---|---|
| loopback | qualquer | qualquer | sobe (aviso se auth sem TLS) |
| exposto | ligada | ligada | sobe |
| exposto | ligada | desligada | **recusa** (§77), nomeando as três chaves |
| exposto | **ausente** | — | **recusa** pedindo decisão explícita (defesa contra typo engolido) |
| exposto | `enabled: false` explícito | — | sobe com o aviso atual |

Reusar `cfg.BindExposed()` ([config.go:297](internal/config/config.go:297)). Oráculo
central: `TestAuthSemTLSComBindExpostoRecusaASubida`.

### B3 — Middleware e papéis
Wrapper único em torno do mux em `sobeHTTP`
([server.go:391](internal/server/server.go:391)). **HTTP Basic sobre TLS** (stdlib
`r.BasicAuth()`, zero dependência; mapeia 1:1 para `DriverManager` do JDBC e
`basic_auth` de Prometheus). Comparação: `sha256` dos dois lados antes de
`subtle.ConstantTimeCompare` (comprimento não vaza); usuário inexistente compara
contra credencial-fantasma — **401 byte-idêntico** para user inexistente vs senha
errada. **Sem lockout** (auto-DoS contra usernames conhecidos); em vez disso atraso
fixo ~100 ms em toda falha, **antes** da vaga do §80. Papéis: READ nas rotas de
leitura; **ADMIN ⊇ READ** (travado por teste). Ordem do middleware testada: 401 antes
de ler corpo, antes de tomar vaga, antes de revelar 404. Códigos novos em
[protocol.go:162](internal/protocol/protocol.go:162): `UNAUTHENTICATED` (401 +
`WWW-Authenticate`), `FORBIDDEN` (403), `WRITER_FAILED`, `COMPACTION_BUSY`; mapeados
em `codigoDoProtocolo` ([query_remote.go:124](internal/cli/query_remote.go:124)).
Observabilidade: `nishidb_auth_failures_total{reason: header|usuario|senha}` (nunca
label de username — cardinalidade aberta), evento `auth.recusada` sem username cru
(loga `user` só se casa com um configurado), header `Authorization` nunca logado.

### B4 — TLS no listener + plano loopback
`ServeTLS` no ponto único ([server.go:414](internal/server/server.go:414)),
`MinVersion: TLS12` (precedente [kafka/config.go:108](internal/kafka/config.go:108)).
Cert carregado no `Novo` (erro na subida nomeando o caminho — nunca no primeiro
request); leaf parseado na subida: WARN com <30 dias de validade, **ERRO no log +
métrica de expiry sem recusar a subida** se expirado (recusar derrubaria as
consultas). Plano loopback: segundo `http.Server` com o **mesmo mux sem middleware**
(precedente `sobePprof`, [http.go:133](internal/server/http.go:133)). `ErrorLog` do
`http.Server` roteado para o logger estruturado (scanners corporativos vão gerar
ruído de handshake na porta exposta).

### B5 — Rotas mutantes (D3, resto do débito 17)
Ambas ADMIN, síncronas, corpo `{"dataset": "..."}` opcional (ausente = todos):
- `POST /v1/checkpoint`: usa `Store.Checkpoint()`
  ([store_checkpoint.go:37](internal/storage/store_checkpoint.go:37)); idempotente por
  efeito; writer falho → 500 `WRITER_FAILED` → `ExitWriter` (4). Resposta:
  `{"datasets":[{"name","durationMs"}]}`.
- `POST /v1/compact`: novo `Store.CompactaUmaVez(ctx, ratio)` exportado sob o
  single-flight existente (`compMu`,
  [compaction.go:616](internal/storage/compaction.go:616)) — passada em voo → **409
  `COMPACTION_BUSY`** (não enfileira); **funciona com `storage.compaction.enabled:
  false`** (é o caso de uso do modo manual); `ctx` = `r.Context()`. Resposta com
  `segmentsRetired`, `bytesRead`, `bytesWritten`, `durationMs`.
- CLI `nishidb compact --server` e `checkpoint --server` no molde de
  [operation.go](internal/cli/operation.go).

### B6 — Credencial nos clientes
CLI (`query --server`, `status`, `datasets`, `inspect`, `compact`, `checkpoint`):
`--user` + senha **somente** por `NISHIDB_PASSWORD` ou `--password-file` (nunca flag —
`ps`); `--tls-ca <path>`. **Proibido `--insecure`/`InsecureSkipVerify` em qualquer
cliente do produto.** Os dois clientes remotos ganham `Transport` com `Proxy: nil`
(mesma decisão do painel — com bind exposto, o `http_proxy` corporativo intercepta IP
não-loopback e o sintoma engana); dica de `https://` quando a porta TLS responde ao
`http://` default. Painel: suporte a `https`+credencial é opcional neste ciclo (o
plano loopback o mantém funcionando); fica registrada a condição de remoção do plano
loopback: painel e script com credencial.

### B7 — Testes adversariais adicionais + docs DoD
Credencial vazia em todas as formas (sem header, Basic vazio, senha vazia, base64
inválido, sem `:`); senha contendo `:` funciona (round-trip); TLS 1.0/1.1 recusado;
cert/chave trocados ou ilegíveis = erro na subida nomeando; e2e real: servidor
TLS+auth, CLI com `--tls-ca`, streaming NDJSON completo, 403 de READ em `/v1/compact`,
409 com duas passadas concorrentes. Docs: `architecture.md` (seção segurança),
`c4_container.puml`, `debitos.md` (fecham **57**, **14**, resto do **17**; abrem:
rate-limit por IP, SIGHUP-reload, health atrás de auth se um dia houver LB probe),
README, `docs/aceite-m5.md`.

---

## Fase C — Deploy no piloto (fecha o ciclo)

**Pedidos com lead time corporativo já na PRIMEIRA SEMANA do ciclo** (durante a Fase A):
certificado à CA interna (se existir PKI) e chamado de firewall para `9142/tcp` com
origem restrita às subnets consumidoras (nunca "any").

1. **Certificado** — plano B provável: **CA própria do piloto** (chave offline, fora do
   host) assinando o cert do servidor. Requisitos: SAN com `IP:10.200.20.218` **e**
   `IP:127.0.0.1` (necessário para o passo 3 abaixo), validade 1 ano, comando de
   emissão reproduzível + `NotAfter` no RUNBOOK. Distribuição do `ca.pem`: CLI, curl,
   futuro trustStore JDBC.
2. **Ensaio no-op**: rodar o `nishidb-update.sh` atualizando para a mesma versão —
   primeiro uso real do mecanismo sem a variável auth junto.
3. **Ordem de exposição em 4 passos reversíveis** (cada um = editar config + restart ~0,28 s):
   1. Binário novo, config intocada (paridade de comportamento).
   2. TLS+auth em `127.0.0.1:9143` + plano legado em 9142 — valida auth/TLS **em
      produção sem exposição** (curl com `--cacert` e `-u`, 401 sem credencial, 403 de
      READ em `/v1/compact`), painel e ingestão intocados.
   3. Expor: `bind: 10.200.20.218, port: 9142` (**IP específico, nunca `0.0.0.0`**) +
      `server.loopback: {enabled: true, port: <porta local>}`. Teste de outra máquina
      com `no_proxy` configurado.
   4. Liberar firewall (último: rollback depende de terceiros).
4. Usuários `reader` e `admin` via `passwordFile` 0600 em `/app/nishidb/secrets/`.
5. **RUNBOOK atualizado**: rollback = par binário+config como unidade ("o risco de
   rollback desta fase é exclusivamente o downgrade silencioso de segurança");
   `no_proxy` para o IP do servidor (e `http.nonProxyHosts` da JVM para o JDBC);
   entrada do #15; credenciais do painel.
6. Fecho: `docs/piloto-events.md` perde os limites "sem auth/TLS"; evidências datadas
   entram em `docs/aceite-m5.md` (versão no `/v1/stats` do piloto, log do script,
   consulta TLS de outra máquina, 401 da rede).

---

## Fase D — Fase 7: driver JDBC

Pode começar após o merge da Fase B, em paralelo com a janela de observação da C.

### D0 — Ajustes no lado Go antes do Java (cada um com teste primeiro)
1. `nullable`/`precision`/`displaySize` em `protocol.Column`, `ColumnMeta` e
   `executor.ColMeta` (semântica v1: toda coluna nullable; precision/displaySize por
   tipo da tabela §73). Campos JSON aditivos.
2. Versão do produto: **já entregue no A3** — aqui só o driver consome.
3. Mover `Health` de [http.go:38](internal/server/http.go:38) para `internal/protocol`.
4. Uniformizar o 405: `/metrics` e `/v1/health` registrados sem método no padrão,
   conferindo por dentro — todo erro sai `protocol.ErrorBody`.
5. **Débito 58**: `query --server` ganha `--param <tipo>:<valor>` repetível, 1:1 com os
   4 tipos de wire de `protocol.Param` — prova ponta a ponta do caminho de
   parâmetros pelo produto.
6. **`UNSUPPORTED_CLAUSE` — desvio deliberado do roadmap**: NÃO acrescentar keywords ao
   lexer (quebraria `SELECT order FROM t`, hoje ident válido). Detectar no sítio do
   erro do parser ([parser.go:46](internal/query/parser/parser.go:46)): token
   inesperado que casa case-insensitive com {ORDER, GROUP, OFFSET, JOIN, HAVING,
   DISTINCT, UNION} emite o código novo nomeando a cláusula. Oráculos: `ORDER BY`
   produz o código; `SELECT "order"` e `WHERE order = 1` seguem funcionando.
7. `setNull`: sem wire por decisão ([params.go:52](internal/protocol/params.go:52))
   — driver lança `SQLFeatureNotSupportedException`, limite documentado.

### D1 — O driver (`clients/jdbc/`, decisão D4: mesmo repositório)
- Java 21+, **zero dependência de runtime** (`java.net.http`, parser JSON mínimo
  próprio) — a doutrina do binário único traduzida em jar sem transitive hell. Build:
  Maven wrapper. Artefato `nishidb-jdbc.jar`, `META-INF/services/java.sql.Driver`.
- Connection string `jdbc:nishidb://host:9142/<dataset>` + `Properties` `user`,
  `password`, `tls`, `tlsCaFile`/trustStore (herda o contrato Basic da Fase 11).
- `ResultSet` **streaming** portando `protocol.Reader`
  ([stream.go:45](internal/protocol/stream.go:45)): linha de schema, dados linha a
  linha, fecho obrigatório — stream sem fecho = `SQLException` de transporte (§128
  vale para o cliente também). Tabela código→`SQLState` derivada de
  `codigoDoProtocolo`, testada dos dois lados.
- `PreparedStatement` com os 4 tipos; `DatabaseMetaData` de `/v1/metadata` +
  `/v1/stats`; método não suportado lança `SQLFeatureNotSupportedException` — com
  teste de que **não** devolve resultado vazio em silêncio.
- **Testes §107** + integração real: alvo `make test-jdbc` builda o binário Go, sobe
  com TLS+auth de teste, roda `mvnw test`, derruba. Job `jdbc` no `ci.yml` (setup-java
  21), fixado pelo `internal/gates/ci_test.go`.
- Validação manual DBeaver: conecta, lista datasets/colunas, SELECT com parâmetro, e a
  mensagem de `UNSUPPORTED_CLAUSE` para `ORDER BY`.

---

## Trilha P — #10, prova do plateau (paralela, sem código)

Com o backlog drenado e ingestão em regime: cron no host raspando `/metrics` pelo
plano loopback (`curl --noproxy '*'`, sem credencial — decisão 4) para CSV; série de
`nishidb_storage_bytes` + `compaction_bytes_read/written` em **janela contínua ≥24-48 h**
(a lição do débito 65: janela se mede contra o padrão de update real). Veredito no
fecho: estabilizou → errata resolve a ressalva de `docs/aceite-m4.md` citando a série;
não estabilizou → o achado alimenta o débito 65/M6. A issue #10 fecha nos dois casos,
com registro em `docs/piloto-events.md`.

---

## Verificação do ciclo

1. `make test`, `make test-race`, `make lint` (baseline mantida/abaixada com o arquivo
   junto) — agora também em CI, que é o próprio entregável do #14.
2. Fase 11: oráculos de recusa de subida, 401/403 por papel, `ps`/log limpos, e2e
   TLS+auth — todos nomeados em `docs/aceite-m5.md`.
3. Deploy: evidências operacionais datadas (versão no `/v1/stats` do piloto, log do
   script, consulta TLS remota, 401 da rede).
4. Fase 7: §107 verde em CI + integração `.jar`↔servidor + DBeaver; testes Java citados
   no aceite com a ressalva de que `TestEvidenciasCitadasNosDocumentosExistem` não os
   cobre (ou o verificador ganha a extensão — nomear a escolha).
5. DoD de documentação: `architecture.md`, `.puml` afetados, `debitos.md` (fecham 57,
   14, 12, 58, resto do 17; abrem os novos), README.
6. #10: veredito do plateau registrado (as duas respostas fecham a issue).

## Riscos e decisões nomeadas

| # | Item | Posição |
|---|---|---|
| 1 | **Débito 34** (`Stats`/`Sequence` engolem `ErrDatasetClosed`) tem dono M5 em `docs/debitos.md:230` mas não está no escopo decidido (muda assinatura do `Store` e todos os chamadores) | Não entra por padrão; nomear no aceite-m5 como adiado conscientemente ou reatribuído — silêncio repetiria a errata do débito 10 |
| 2 | Toolchain: `go.mod` diz 1.25.0, baseline medida com go1.26.5 | CI pina 1.26.5; subir versão é ato deliberado com re-medição |
| 3 | Ratchet da baseline 390 em CI | Arquivo versionado, atualizado no mesmo PR, recusa nas duas direções |
| 4 | Certificado: CA interna vs CA do piloto | Pedir à CA interna na semana 1; plano B (CA do piloto) já especificado |
| 5 | Rate-limit por IP e SIGHUP-reload | Fora de escopo; débitos nomeados com a razão |
| 6 | Flakiness de e2e/TLS em runner de CI | Revisar timeouts na primeira semana de CI se acusar |
| 7 | Esquema de versão | `v0.5.0` no fecho da Fundação; sem tags retroativas |
| 8 | Condição de remoção do plano loopback | Painel + script + cron com credencial e CA; registrada no RUNBOOK |
