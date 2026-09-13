# Issue #40 — Usuários numa tabela do NishiDB: credencial de bootstrap e gestão com o servidor no ar

## Contexto

Hoje os usuários do servidor são declarados no `nishidb.yaml`
(`server.authentication.users`, com `passwordEnv`/`passwordFile` e papel
READ/ADMIN). As senhas são lidas uma vez na subida e ficam em memória como
SHA-256 sem sal (`internal/server/security.go:53`), e não há recarga: trocar uma
senha exige reiniciar. A issue #40 pede três coisas:

- a credencial de administração gerada na primeira subida;
- gestão de usuários com o servidor no ar;
- a tabela consultável, sem expor segredo.

## Decisões tomadas

| Decisão | Escolha |
|---|---|
| Onde mora | **Arquivo de sistema** `<storage.path>/_system/users.state`, gravado inteiro pelo mesmo caminho atômico do checkpoint. Tem flock próprio em `_system/LOCK`. Não é Store: um Store custaria uns 16 MiB de mmap e apareceria no catálogo, no health e na compactação |
| Senha do bootstrap | **Arquivo 0600** `<storage.path>/_system/bootstrap-admin.password`. O log só dá o caminho. O arquivo some quando a senha desse admin é trocada ou quando ele é removido. Não vai para o stdout, porque a unit do systemd grava o stdout em `/var/log/nishidb/service.log` |
| YAML | **Obsoleto, fonte única = tabela.** `server.authentication.users` deixa de existir. Se aparecer, a subida é recusada com mensagem que ensina a migrar. Não há período de convivência nem leitura das duas fontes (quebra de contrato decidida explicitamente; vai registrada na release) |
| `_meta.users` | **Só ADMIN**, nunca com o hash. Recusa fail-closed também no plano loopback |
| Papéis por dataset | **Fora do ciclo** (vira issue nova) |
| Sessões abertas | Não existem: o Basic é verificado a cada request. Uma troca vale a partir do próximo request, e a consulta já em curso termina |

## O que já existe e será reusado

| Peça | Onde | Para quê |
|---|---|---|
| Escrita atômica e durável | `writeAtomic` (`internal/storage/checkpoint/manifest.go:679`), com ordem gateada por `TestWriteOrdemDeDurabilidade` | Persistir `users.state`. Uma troca que sobreviveu a crash não volta |
| Formato magic+versão+crc32c, e ausente ≠ corrompido | `checkpoint/alarms.go` (`encode`/`decodeAlarmState`/`LoadAlarmState`) | Formato de `users.state` |
| Flock exclusivo com pid | `internal/storage/store_lock.go:38` | `_system/LOCK` |
| Router com papel, fail-closed | `rt.register` (`internal/server/auth.go:80`), `withAuth` `:176`, `muxLoopback` `:223` | Rotas novas; a gestão já fica recusada no loopback |
| 401 uniforme, credencial fantasma, atraso fixo | `authenticate`/`rejectAuth` (`auth.go:293,341`) | Mantidos. Só troca o verificador |
| Tabela virtual por closure sobre fatia imutável | `internal/metaalarm/table.go` (`Register` `:152`), ligada em `server.go:536` | `_meta.users` |
| Rotas ADMIN mutantes | `internal/server/counters.go`, `mutations.go` (`readRequisicaoDataset`, `mutationError`, `s.reject`) | Molde dos handlers de usuário |
| Credencial do CLI sem flag de senha | `internal/cli/credential.go` (`NISHIDB_PASSWORD` / `--password-file`), `mutation.go:112 postMutation` | `nishidb users … --server` |
| Modo `--config` xor `--server` | `internal/cli/counters.go:31` | `nishidb users` |
| Redação de chave sensível no log | `logging.IsSensitiveKey`, `logging.Secret` (`internal/logging/logging.go:63,97`) | Garantia de que nada vaza |

## Desenho

### 1. Escrita atômica vira pacote próprio (refactor, sem mudança de comportamento)

- `writeAtomic` e `syncDir` saem de `checkpoint/manifest.go` para `internal/storage/atomicfile` (`Write(path, data, perm)`).
- Os hooks `step`/`writeTrace` vão junto, e `TestWriteOrdemDeDurabilidade` se muda com eles.
- `checkpoint.Write`, `WriteOffsets` e `WriteAlarmState` passam a chamar o pacote novo.
- Assim continua havendo um único caminho durável provado, sem uma quarta cópia.
- O parâmetro `perm` existe porque `users.state` precisa de 0600 (hoje o arquivo temporário nasce 0644).

### 2. `internal/users` — o dono da tabela

- **`record.go`**: `User{Username, Role, Hash, CreatedAt, CreatedBy, UpdatedAt, UpdatedBy, PasswordChangedAt, Bootstrap bool}`.
- **`hash.go`**: argon2id de `golang.org/x/crypto/argon2`, que sobe de indireta para direta no `go.mod`.
  - O registro guarda algoritmo, `m`, `t`, `p`, sal de 16 bytes e hash de 32 bytes, então os parâmetros podem mudar depois sem migração.
  - Default: t=2, m=19 MiB, p=1 (mínimo OWASP).
  - Senha nova tem de ter no mínimo 12 bytes; abaixo disso é recusada, nunca completada.
  - Arquivo de senha com `\r\n` no fim é aparado, como hoje.
- **`state.go`**: formato binário magic + versão + crc32c.
  - Usuários ordenados por nome; o golden test de bytes fixa o formato de disco.
  - Carga tri-estado:
    - arquivo ausente → primeira subida;
    - ilegível ou corrompido → **recusa a subida**, sentinela `ErrCorruptUsers`, exit 3. Nunca regera um admin, porque isso seria reset de privilégio;
    - válido → usa.
- **`store.go`**: `Open(dir)` pega o flock (`ErrUsersLocked`, exit 3).
  - O estado em memória é um `atomic.Pointer[table]` imutável.
  - Toda mutação segue a ordem: `mu.Lock` → valida → monta a tabela nova → `atomicfile.Write` → publica o ponteiro → loga a auditoria.
  - Publicar o ponteiro é o commit de visibilidade e vem por último, como a location no storage.
  - Operações: `Create`, `SetPassword`, `SetRole`, `Delete`, `Snapshot()`.
  - Invariante: **sempre existe ao menos um ADMIN**. Remover ou rebaixar o último é recusado (`ErrLastAdmin`).
- **`bootstrap.go`**: com a autenticação ligada e `users.state` ausente, o bootstrap roda nesta ordem:
  1. Gera `admin` com 24 bytes de `crypto/rand` em base64url.
  2. Grava `bootstrap-admin.password` com `O_EXCL`, 0600, fsync e fsync do diretório.
  3. Grava `users.state`.
  4. Loga WARN `users.bootstrap` com o caminho do arquivo.
  - A ordem importa: um crash entre 2 e 3 deixa só o arquivo de senha, e a próxima subida o sobrescreve com um par novo e consistente.
  - `SetPassword` ou `Delete` do usuário com `Bootstrap=true` apaga o arquivo depois de gravar o estado.
- **`verifier.go`**: o Basic é verificado a cada request, e argon2id por request estouraria latência e RSS. Por isso:
  - **Cache de verificação**: `username → HMAC-SHA256(chave aleatória do processo, senha)` mais a geração do registro.
    - Um acerto compara em tempo constante e não chama o KDF.
    - Qualquer mutação no usuário muda a geração e invalida a entrada. A entrada nunca é reaproveitada depois de troca de senha ou de papel.
  - **Semáforo de KDF** (2 vagas): teto de ~38 MiB transitórios, coerente com o orçamento de RSS.
  - Usuário inexistente roda o KDF contra um **hash fantasma** com os mesmos parâmetros, mantendo o caminho idêntico ao de senha errada, como faz hoje `ghostCredential`.

### 3. Servidor

- **Config**:
  - `internal/config/config_security.go` perde `UserSpec` e `AuthenticationSpec.Users`.
  - A chave `users` ganha recusa **nomeada**: "server.authentication.users was removed; users now live in <storage.path>/_system/users.state — remove the block, start the server and read the bootstrap credential at …, then recreate users with `nishidb users create`".
  - `enabled: true` deixa de exigir usuários.
  - `passwordEnv`/`passwordFile` saem dos testes de config.
- **`server.New`**:
  - Com a autenticação ligada, abre `users.Store` e roda o bootstrap **antes do bind**, no lugar de `resolveCredentials`.
  - `security.go` perde `credencial`, `resolveCredentials` e `resolvePassword`, e o campo `s.credenciais` vira `s.users`.
  - Os pares velho → novo entram em `planning/identificadores-em-ingles/mapeamento-2-manual.tsv`.
  - `encerra` fecha o Store de usuários por último.
- **Principal no contexto**: `withAuth` passa a guardar `principal{username, role}` no `context` do request.
  - Os handlers de usuário leem dali para decidir auto-troca e para registrar a auditoria (`actor`).
  - Com a autenticação desligada não há principal, e toda rota de usuário responde 403 `FORBIDDEN` ("user management requires authentication"). Não há gestão anônima.
- **Rotas** (POST, corpo JSON com `DisallowUnknownFields`):

  | Rota | Papel no router | Regra no handler |
  |---|---|---|
  | `/v1/users/create` `{username, role, password}` | ADMIN | nome `[a-z0-9_.-]{1,64}` |
  | `/v1/users/password` `{username, password}` | READ | só ADMIN troca a senha de outro; READ só troca a própria, e o Basic do request é a credencial corrente |
  | `/v1/users/role` `{username, role}` | ADMIN | último ADMIN protegido |
  | `/v1/users/delete` `{username}` | ADMIN | último ADMIN protegido; remover a si mesmo é permitido desde que não seja o último |

  - A resposta nunca ecoa a senha.
  - A listagem é por SQL em `_meta.users`, sem rota GET própria.
- **Auditoria**: `users.created`, `users.password_changed`, `users.role_changed`, `users.deleted`, `users.bootstrap`.
  - Chaves: `actor`, `user`, `role`, `remote`. Nunca senha nem hash; teste captura o log e procura a senha.
  - No modo offline do CLI, `actor` = `local:<usuário do SO>`.

### 4. Protocolo e clients

- Códigos novos em `internal/protocol/protocol.go`: `USER_EXISTS`, `UNKNOWN_USER`, `LAST_ADMIN`, `WEAK_PASSWORD`. Corpo malformado usa o código de requisição inválida já existente.
- Cada código novo entra em:
  - `clients/jdbc/.../wire/Codigos.java` e `SqlStates.java` (`USER_EXISTS` 23505, `UNKNOWN_USER` 42704, `LAST_ADMIN` e `WEAK_PASSWORD` 42501/22023, com `SQLNonTransientException`);
  - `clients/python/src/nishidb/erros.py` (`ProgrammingError`/`OperationalError`) e o `__all__` de `__init__.py`.
- Os gates `TestTodoCodigoDoProtocoloTem*` cobram o mapeamento.
- Os tipos do wire ficam em `internal/protocol/users.go`.

### 5. `_meta.users`, só para ADMIN

- **`internal/metausers`** (molde `metaalarm`).
  - Colunas: `username`, `role`, `bootstrap`, `createdAt`, `createdBy`, `updatedAt`, `updatedBy`, `passwordChangedAt`. **Não há coluna de hash.**
  - É registrada em cada dataset e na tabela histórica (`history.go:112`), com a closure lendo `users.Store.Snapshot()`.
- **Barreira no motor, fail-closed**:
  - `Engine.RegisterVirtual` ganha a opção `RequiresAdmin`.
  - `Engine.Execute` recusa com `query.ErrForbidden` quando a tabela exige ADMIN e o `ctx` não traz a marca `query.WithAdmin(ctx)`.
  - Só `handleQuery` põe a marca, e só para um principal ADMIN. Com isso o loopback anônimo e o usuário READ caem na recusa sem código extra.
  - O servidor mapeia a recusa para 403 `FORBIDDEN`.

### 6. CLI `nishidb users`

- **Subcomandos**: `create`, `password`, `role`, `delete`. Nome e papel por flag: `--name`, `--role`.
- A senha nova vem **só** por `--new-password-file PATH` (`-` = stdin), nunca por flag.
- **Modos**: `--server` (credencial por `--user` + `NISHIDB_PASSWORD`/`--password-file`, via `postMutation`) xor `--config` (servidor parado).
- O modo `--config` usa o mesmo `users.Store`. Servidor no ar dá exit 3 (`ErrUsersLocked` em `errorCode`, `internal/cli/cli.go:216`). Sem estado, ele inicializa sem gerar bootstrap.
- O modo offline é a **recuperação de senha de admin perdida** (quem tem o disco já é root) e é o provisionamento determinístico dos ITs.
- Texto de uso em `cli.go:148-170`, e a tabela de exit codes no README.

### 7. Update e rollback

- `scripts/nishidb-update.sh` fala pelo plano loopback sem credencial e não muda.
- O rollback troca o par `releases/<versão>/{nishidb, nishidb.yaml}`, e o YAML da release antiga ainda traz o bloco `users`, então o binário antigo sobe com os usuários dele e ignora `_system/`.
- Fica documentado:
  - usuários criados ou alterados depois da atualização não existem para o binário antigo;
  - um YAML novo (sem `users`) com binário antigo recusa a subida. É fail-closed: nunca sobe sem senha.

## Débitos novos (fim de `docs/debitos.md`, a partir do 120)

- Semáforo de KDF disputável por tentativa inválida em massa: um usuário legítimo com cache frio espera. O rate-limit do débito 71 é o remédio.
- Papéis por dataset.
- O débito 72 (reload de credencial via SIGHUP) é atualizado: usuário não precisa mais de reinício, só o certificado TLS.

## Documentação (no mesmo PR)

- README, seção "Segurança: autenticação e TLS" (`README.md:1106`):
  - bootstrap, `nishidb users`, `_meta.users`, recuperação offline;
  - **nota de quebra** com o roteiro de migração do bloco `users`;
  - rollback.
- `docs/architecture.md:1321`: fonte da verdade, cache de verificação, ordem de durabilidade.
- `docs/glossario.md`: tabela de usuários, credencial de bootstrap, cache de verificação, último ADMIN.
- Diagrama `docs/diagrams/sequence_user_management.puml`: bootstrap, troca de senha, invalidação do cache. Validado pelo servidor `uml.nishisan.dev`, porque não há plantuml local.
- Métricas: nenhuma nova (`nishidb_auth_failures_total` continua).
- O plano aprovado vai para `planning/usuarios-na-tabela.md`.

## Blocos de commit (worktree `feature/usuarios-na-tabela`, criada antes do branch)

1. `atomicfile` extraído + `perm` (refactor, suíte verde).
2. `internal/users` (formato, hash, store, bootstrap, verificador) + testes.
3. Servidor: config sem `users`, bootstrap na subida, verificador em `authenticate`, principal no contexto, renames + TSV.
4. Rotas + protocolo + códigos novos + JDBC/Python.
5. `_meta.users` + `RequiresAdmin` no motor.
6. CLI `nishidb users` + exit codes.
7. ITs (`scripts/it-comum.sh` provisiona `leitor` via `nishidb users create --config` antes de subir) + docs, glossário, débitos e diagrama.

## Verificação

Para cada invariante, aplicar a mutação, ver vermelho, reverter, ver verde, e relatar os dois:

- **Durabilidade**:
  - com injeção de falha nos `step` de `atomicfile`, falha antes do rename → reabrir aceita a senha antiga;
  - falha depois do rename → aceita só a nova.
  - Mutação: publicar o ponteiro antes de gravar → vermelho.
- **Cache**: trocar a senha e reusar a antiga no request seguinte → 401. Mutação: não mudar a geração → vermelho.
- **Último ADMIN**: remover ou rebaixar é recusado com `LAST_ADMIN`.
- **Bootstrap**:
  - arquivo 0600;
  - senha ausente do log e do stdout (log capturado);
  - arquivo removido na troca;
  - `users.state` corrompido → subida recusada, exit 3, sem arquivo novo.
- **Acesso**:
  - READ troca a própria senha e não a de outro;
  - `_meta.users` → FORBIDDEN para READ e no loopback, OK para ADMIN;
  - rotas de usuário no loopback → 403;
  - autenticação desligada → 403.
  - Mutação: retirar `RequiresAdmin` → vermelho.
- **Timing**: usuário inexistente passa pelo KDF fantasma (teste conta as chamadas do KDF nos dois caminhos).
- **Config**: bloco `users` presente → recusa com a mensagem de migração.
- **Comandos**:
  - `make build`, `make test`, `make test-race`, `make test-integration`;
  - `make lint` (baseline igualada no mesmo PR);
  - `make test-jdbc-unit test-jdbc test-python test-python-it`.
  - Conferir a contagem de testes, não só o verde.
- **Ponta a ponta manual**:
  1. subir com diretório vazio;
  2. ler o arquivo de bootstrap;
  3. `nishidb users create --server …`;
  4. consultar `_meta.users` com ADMIN e com READ;
  5. trocar a senha do bootstrap e ver o arquivo sumir;
  6. `kill -9` logo depois de uma troca, reabrir e confirmar a senha nova;
  7. offline `nishidb users password --config` com o servidor no ar → exit 3.
