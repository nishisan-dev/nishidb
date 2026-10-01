# Prazo de requisição no driver JDBC (issue #91)

Achado na integração service-view (30/09/2026). Hoje nenhuma requisição HTTP do
driver tem prazo no cliente: `Transporte.clienteHttp` só configura
`connectTimeout`; `requisicao` não chama `.timeout(...)`; `isValid(segundos)`
ignora o argumento; `setQueryTimeout` só vira `timeoutMs` para o servidor;
`abort(Executor)` e `setNetworkTimeout` lançam. Um servidor que aceita e não
responde trava a thread do cliente sem limite — e o pool (Hikari valida com
`isValid`) esgota.

## Contrato

1. **`isValid(segundos)`**: o GET `/v1/health` leva `HttpRequest.timeout`
   de `segundos` (0 = sem prazo, como o contrato JDBC); estouro → `false`, nunca
   exceção. Volta em ≈ `segundos`.
2. **Prazo de leitura por requisição**: parâmetro novo de URL/Properties
   `socketTimeoutMs` (nome a confirmar contra o padrão dos parâmetros já
   existentes do driver, ex.: `connectTimeoutMs`) — default **0 = sem prazo**
   (comportamento atual preservado; ninguém é surpreendido). Quando > 0, vale
   para TODA requisição (metadata, health fora do isValid, query, catálogo).
3. **Consulta com `setQueryTimeout(s)`**: o prazo do cliente para AQUELA
   requisição é `s` + folga fixa (documentada; ex.: 5 s, para o servidor
   cancelar e responder com o erro próprio antes) — o maior entre isso e o
   `socketTimeoutMs`... decida e documente a precedência, com teste. Estouro no
   cliente → `SQLTimeoutException` com SQLSTATE coerente com o do timeout do
   servidor (veja `SqlStates` e o mapeamento existente do erro de timeout do
   wire); a conexão continua utilizável? Decida (o HttpClient pode reaproveitar;
   a requisição presa é abandonada) e prove.
4. **`setNetworkTimeout(executor, ms)` / `getNetworkTimeout()`**: passam a
   funcionar e a ajustar o prazo de leitura da conexão (o mesmo do item 2).
   Hikari chama isso; hoje lança.
5. **`abort(Executor)`**: implementado — marca a conexão fechada e fecha o
   HttpClient (interrompendo requisições em voo), sem esperar o servidor,
   executado no executor dado conforme o contrato JDBC; permissão
   `SQLPermission("callAbort")` se houver SecurityManager (provavelmente
   irrelevante no JDK 21 — documente). Requisição presa em outra thread sai
   com `SQLException` de conexão fechada.

## Garantias e testes (IT/unit com servidor dublê que aceita e nunca responde)

Cada uma com mutante → vermelho → reverter → verde, relatados:
- `isValid(1)` volta `false` em ≤ ~2 s (mutante: sem `.timeout`).
- `executeQuery` com `setQueryTimeout(1)` lança `SQLTimeoutException` no prazo
  (mutante: prazo só no servidor).
- `socketTimeoutMs=500` faz `getMetaData`/consulta sem queryTimeout estourar
  (mutante: parâmetro lido e não aplicado).
- `setNetworkTimeout` altera o prazo efetivo (mutante: no-op).
- `abort` destrava requisição presa em outra thread (mutante: abort só marca
  flag).
- Default 0: sem parâmetro, comportamento idêntico ao de hoje (a requisição
  espera — teste com prazo do TESTE curto e asserção de que ainda não voltou).
- Todo teste que espera usa prazo explícito com mensagem; nada pode travar a
  suíte (um mutante que trava tem de reprovar com mensagem).

## Restrições
- Driver é **zero dependência de runtime**; testes podem usar só o que o
  `pom.xml` já tem em escopo de teste (não acrescentar dependência sem
  necessidade; se precisar de HikariCP para um IT, NÃO — teste o contrato JDBC
  direto).
- Gates Go leem o `pom.xml` como dado: mexer nele pode quebrar `go test ./...`.
- Parâmetro novo de URL entra na doc do driver (README seção JDBC e o que mais
  documentar parâmetros) e, se houver gate que confere parâmetros documentados
  × aceitos, ele tem de passar.
- Idioma: o driver Java hoje usa identificadores em PT-BR (ex.: `Transporte`,
  `requisicao`, `SqlStates.VALOR_FORA_DE_FAIXA`). Siga a regra do CLAUDE.md:
  código NOVO em inglês; identificador PT-BR que o bloco TOCA é renomeado no
  mesmo bloco, com o par em
  `planning/identificadores-em-ingles/mapeamento-2-manual.tsv` — mas só se o
  gate/mapeamento cobrir Java; se não cobrir, não renomeie e relate.
  Mensagens de exceção do driver: siga o idioma que o driver já usa para elas
  e relate a divergência se houver.
