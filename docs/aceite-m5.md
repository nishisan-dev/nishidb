# Aceite do M5 — Fase 11: autenticação e TLS

Este documento responde **"a Fase 11 está pronta?"** no mesmo formato do
[aceite do M1](aceite-m1.md), do [M2](aceite-m2.md), da [Fase 6](aceite-fase6.md)
e do [M4](aceite-m4.md): cada item tem estado e evidência, e a evidência é o
**nome de um teste** da suíte.

**Esqueleto, não rodada fechada.** A Fase 11 não tem uma rodada de benchmark
própria — não há número de vazão, latência ou memória a medir aqui que a Fase
11 mude; o que ela muda é o MECANISMO de acesso ao protocolo remoto, e o
mecanismo se prova por teste, não por medição. O que falta para fechar este
documento é a **rodada operacional**: o deploy no piloto com certificado de
uma CA real, usuários reais e a porta liberada no firewall — isso é a **Fase
C** do ciclo (`planning/m5-plano-ciclo.md`) e **ainda não rodou**. A seção
["O que falta: a Fase C"](#o-que-falta-a-fase-c) descreve exatamente o que
essa rodada precisa produzir, sem antecipar nenhum resultado.

## A pergunta que a fase existe para responder

O débito 57 (`docs/debitos.md`) dizia: com o `/v1/query` exposto e sem
autenticação, a porta 9142 é equivalente a um banco sem senha. A Fase 11
fecha isso com um **plano duplo**: o listener exposto passa a exigir TLS e
HTTP Basic em TODA rota (health e métricas inclusive), e um segundo listener,
opcional e só em loopback, mantém painel/script/cron locais funcionando sem
credencial. A descrição completa do mecanismo está em
[`architecture.md`, "Segurança: autenticação e TLS"](architecture.md#segurança-autenticação-e-tls-fase-11).

## Os entregáveis da Fase 11

| Entregável | Estado | Evidência |
|---|---|---|
| `server.tls`/`server.authentication`/`server.loopback` com forma validada no parse (§74-75) | **Atendido** | `TestTLSLigadoSemCertificadoOuChaveEhRecusado`, `TestChaveDesconhecidaNosBlocosDeSegurancaEhRecusada`, `TestLoopbackNaMesmaPortaDoServerPrincipalEhRecusado`, `TestLoopbackNaMesmaPortaDoDefaultImplicitoEhRecusado`, `TestBlocoUsersNoYAMLEhRecusadoComORoteiroDeMigracao`. Os testes de forma do antigo `server.authentication.users` saíram com a issue #40, que tirou os usuários do YAML |
| A matriz de recusa do §77 (bind × auth × TLS decide se o servidor sobe) | **Atendido** | `TestAuthSemTLSComBindExpostoRecusaASubida` (oráculo central), `TestBindExpostoSemSecaoDeAuthenticationRecusaASubida`, `TestBindExpostoComAutenticacaoETLSSobeSemRecusa`, `TestBindExpostoComAuthenticationSecaoVaziaSobeComAviso`, `TestLoopbackComAutenticacaoSemTLSSobeComAviso`, `TestLoopbackComAutenticacaoETLSSobeSemAviso` |
| Resolução de segredos na subida, fail-fast | **Substituído (issue #40)** | A senha deixou de vir de env/arquivo declarado no YAML: a tabela de usuários é lida antes do bind, e ilegível recusa a subida. `TestTabelaCorrompidaRecusaAAberturaSemGerarCredencial`, `TestPrimeiraAberturaGeraAdminDeBootstrapComArquivo0600` |
| Middleware HTTP Basic com papéis READ/ADMIN | **Atendido** | `TestMutantesExigemAutenticacao` (401), `TestMutantesRecusamPapelREAD` (403), `TestPapelReadEAdminAlcancamTodasAsRotasDeLeitura`, `TestPapelAutorizadoAdminAlcancaRead` |
| 401 byte-idêntico entre usuário inexistente e senha errada | **Atendido** | `TestAutenticacao401EhByteIdenticoEntreUsuarioInexistenteESenhaErrada` |
| Sem lockout; atraso fixo em toda falha | **Atendido** | `TestAtrasoFixoDeAproximadamente100msEmTodaFalhaDeAutenticacao` |
| Fail-closed no registro de rota (bug de registro não vira acesso anônimo) | **Atendido** | `TestRotaRegistradaPorForaDeRegistraEhRecusadaFailClosed`, `TestRecusaRegistroSemPapelLogaODetalheQueNaoVaiAoCliente` |
| `ServeTLS` com `MinVersion: TLS12`; aviso de validade do certificado sem recusar a subida | **Atendido** | `TestServeTLSRecusaVersaoAbaixoDeTLS12`, `TestCertificadoOuChaveInvalidosRecusamASubidaNomeandoOCaminho`, `TestSubidaComCertificadoExpiradoNaoRecusaELogaErro` |
| Plano loopback: sem TLS/auth, mas READ-only por construção | **Atendido** | `TestLoopbackDesligadoNaoAbrePorta`, `TestLoopbackHabilitadoSemPortaRecusaASubidaDoServidor`, `TestLoopbackRespondeSemCredencialComPrincipalAutenticadoSimultaneamente`, `TestLoopbackRecusaRotaAdminMasRotaReadContinuaAnonima`, `TestShutdownFechaListenerPrincipalELoopbackSemVazarGoroutine` |
| Paridade do piloto: bind loopback sem nada de segurança declarado não muda comportamento | **Atendido** | `TestBindEmLoopbackNaoAvisa`, `TestBindEmLoopbackSemNadaDeSegurancaDeclaradoNaoLogaNada` (silêncio total do log de subida, não só ausência de uma substring) |
| `POST /v1/compact` e `POST /v1/checkpoint`, ADMIN, síncronas | **Atendido** | `TestCheckpointComAdminAvancaOContadorEDevolveODataset`, `TestCompactComAdminEEnabledFalseFunciona`, `TestCompactComAdminNaConfigDefaultELacoOciosoFazTrabalhoReal` |
| 409 fino: passada em voo (laço automático OU outra chamada manual) recusa, não enfileira | **Atendido** | `TestCompactDuasChamadasSimultaneasPeloHTTPUmaGanhaUmaRecebe409` (fim a fim pela porta), `TestCompactaUmaVezComPassadaDoLacoEmVooDevolveBusy`, `TestCompactaUmaVezDuasChamadasConcorrentesUmaGanhaUmaRecebeBusy` |
| Contrato de falha parcial multi-dataset (nomeia quem falhou e quem já tinha sido processado) | **Atendido** | `TestCheckpointComTodosOsDatasetsRespeitaAOrdemDeterministicaDaSubida`, `TestCompactECheckpointRecusamMaisDeUmDataset` |
| Credencial nos seis comandos remotos do CLI (`--user`, `NISHIDB_PASSWORD`/`--password-file`, `--tls-ca`) | **Atendido** | `TestQueryRemotaComTLSEAuthFuncionaDePontaAPonta`, `TestStatusRemotoComTLSEAuthFuncionaComPasswordFile`, `TestCompactRemotoComUsuarioAdminFuncionaDePontaAPonta`, `TestCompactRemotoComUsuarioReadSaiComExit1EMencionaOPapel`, `TestStatusSemUserNuncaEnviaHeaderAuthorization` |
| Sem `--insecure`/`InsecureSkipVerify` em nenhum cliente; sem redirect seguido com credencial | **Atendido** | `TestRecusaRedirectNomeiaOStatusEODestino`, `TestRecusaRedirectSemResponseAindaAssimRecusa` |

## Os oráculos da fase, por categoria

O brief desta task pediu a lista organizada por categoria de propriedade —
não só a tabela acima, que é por entregável:

- **Recusa de subida** (a matriz do §77 e as recusas de forma do parse):
  `TestAuthSemTLSComBindExpostoRecusaASubida`,
  `TestBindExpostoSemSecaoDeAuthenticationRecusaASubida`,
  `TestLoopbackHabilitadoSemPortaRecusaASubidaDoServidor`,
  `TestCertificadoOuChaveInvalidosRecusamASubidaNomeandoOCaminho`.
- **401/403**: `TestMutantesExigemAutenticacao` (401 em rota mutante sem
  credencial), `TestMutantesRecusamPapelREAD` (403, papel insuficiente),
  `TestPapelReadEAdminAlcancamTodasAsRotasDeLeitura`.
- **Byte-identidade** (usuário inexistente vs. senha errada, resposta E
  headers): `TestAutenticacao401EhByteIdenticoEntreUsuarioInexistenteESenhaErrada`.
- **Fail-closed** (rota registrada por fora do mecanismo não fica sem
  checagem): `TestRotaRegistradaPorForaDeRegistraEhRecusadaFailClosed`.
- **Loopback READ-only** (rota ADMIN sai 403 mesmo sem nenhuma autenticação
  configurada no listener): `TestLoopbackRecusaRotaAdminMasRotaReadContinuaAnonima`.
- **409 fino** (passada em voo recusa, não enfileira, testado pela porta HTTP
  de verdade, duas requisições reais): `TestCompactDuasChamadasSimultaneasPeloHTTPUmaGanhaUmaRecebe409`.
- **E2E TLS+auth pelo CLI** (certificado real, HTTP Basic, streaming NDJSON
  completo): `TestQueryRemotaComTLSEAuthFuncionaDePontaAPonta`.

`TestEvidenciasCitadasNosDocumentosExistem` confere que todo identificador de
teste citado neste arquivo existe na suíte — por isso esta lista só nomeia
teste que roda hoje, nunca um planejado.

## O que a Fase 11 NÃO fecha

| Item | Onde fica |
|---|---|
| **A rodada operacional no piloto** (certificado da CA real, usuários reais, porta liberada no firewall) | **Fase C do ciclo — ainda não rodou.** Ver a seção seguinte |
| Rate-limit por IP nas falhas de autenticação (inclui o `time.Sleep` do atraso fixo não observar `r.Context().Done()`) | Débito **71** (`docs/debitos.md`) |
| SIGHUP-reload de credencial ou certificado (sem recarga a quente hoje; rotação é restart) | Débito **72** |
| Condição de remoção do plano loopback (painel/script/cron precisam migrar para o listener exposto com credencial e CA) | Débito **73** |
| `Stats`/`Sequence` engolindo `ErrDatasetClosed` (débito 34) | **Continua aberto** — adiado conscientemente também nesta fase (não é escopo de auth/TLS, e muda a assinatura do `Store`). **Dono: M6** |
| Suporte a `https`+credencial no painel (`nishidb-stats.py`) | Não entrou nesta fase (decisão da Task B6, verificada — o painel só tem teste da função pura de desenho, não da camada de rede) |
| Driver JDBC (Fase 7, §70-73) | Próximo do M5, e pode seguir agora que a autenticação existe |

## O que falta: a Fase C

A Fase C (`planning/m5-plano-ciclo.md`) é o deploy no piloto — o host
`10.200.20.218`, hoje rodando o bind legado sem TLS/auth
(`docs/piloto-events.md`). Ela precisa produzir, e nenhum destes números
existe ainda:

1. **Certificado** emitido por uma CA (própria do piloto ou corporativa), com
   SAN cobrindo `10.200.20.218` e `127.0.0.1`, `NotAfter` registrado no
   RUNBOOK.
2. **Ensaio no-op** do script de atualização (`nishidb-update.sh`) contra a
   mesma versão, antes de qualquer mudança de segurança.
3. **Exposição em passos reversíveis**: TLS+auth primeiro num bind alternativo
   (validado em produção sem exposição — `curl --cacert` + `-u`, 401 sem
   credencial, 403 de READ em `/v1/compact`), só depois o bind exposto de
   verdade, e só por último o firewall liberado.
4. **Usuários `reader` e `admin` reais**, via `passwordFile` 0600 em
   `/app/nishidb/secrets/`.
5. **RUNBOOK atualizado** com o par binário+config como unidade de rollback,
   `no_proxy` para o IP do servidor, e a nova seção de credenciais.
6. **Evidências datadas** substituindo esta seção: a versão do `/v1/stats` do
   piloto pós-deploy, o log do script de atualização, uma consulta TLS feita
   de outra máquina, e um 401 de rede real (não de teste).

Nenhum número de medição foi inventado para preencher esta seção — a fase não
tem rodada de benchmark própria, e a rodada operacional é trabalho da Fase C,
não desta task.

## Comandos de verificação

A suíte inteira roda sem broker Kafka externo (usa `kfake` em memória) e sem
certificado real (os testes geram o próprio par autoassinado):

```bash
go test ./... -count=1
go test ./internal/server/... ./internal/config/... ./internal/storage/... \
    ./internal/cli/... ./internal/protocol/... -race -count=1
make lint
```
