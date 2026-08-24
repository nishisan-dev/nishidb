# Fase 7 — Driver JDBC do NishiDB

## Context

O roadmap coloca a **Fase 7 (driver JDBC, §70-73)** como o próximo item depois do M5, e o
[README.md:41](README.md:41) já registra que ela "agora pode seguir" — a condição que a
segurava (publicar um driver antes da autenticação multiplicaria os pontos de acesso não
autenticado) deixou de valer quando a Fase 11 entregou Basic + TLS + papéis.

**Resposta à pergunta que abriu a sessão: o projeto está pronto para começar, mas não para
começar pelo Java.** O contrato de wire está maduro — `internal/protocolo` foi escrito
declaradamente como ponto de encontro de três falantes (servidor, CLI remoto e "o driver
JDBC da Fase 7"), a tabela de tipos §73 já é única no produto
([jdbc.go:35](internal/protocolo/jdbc.go:35)), o `protocolo.Leitor`
([stream.go:45](internal/protocolo/stream.go:45)) é o consumidor de referência a portar, e
`TestNullExplicito` ([ndjson_test.go:253](internal/query/executor/ndjson_test.go:253)) já
protege a invariante de NULL *citando o driver JDBC na justificativa*. O que falta são
**seis buracos no lado Go** que, se não forem fechados antes, viram constante hard-coded num
`.jar` já distribuído — e constante em jar distribuído não se corrige sem release.

Este plano cobre o ciclo inteiro: fechar o contrato (Fase A), escrever o driver (Fases B-G) e
provar contra o binário real e no DBeaver (Fases H-I).

O ambiente local está pronto: JDK 21 e 25 instalados, Maven 3.6.3 (o wrapper baixa o pinado).

---

## Decisões fechadas

| # | Decisão | Justificativa |
|---|---|---|
| **D1** | Escopo: ciclo completo — D0 (Go) + driver + `make test-jdbc` + job de CI + validação DBeaver | Congelar o contrato pela metade num jar distribuído é o custo que o faseamento economizaria |
| **D2** | O driver **extrai o dataset do `FROM`**, com o da URL como fallback | [binder.go:157](internal/query/binder/binder.go:157) recusa `dataset` ≠ `FROM`. Sem o extrator, o DBeaver lista todos os datasets e só consegue consultar um. **O palpite errado nunca devolve dado de outra tabela** — o servidor recusa no bind. O mesmo scanner conta os `?` |
| **D3** | **Errata do §71**: `SQLFeatureNotSupportedException` vale para capacidades de *execução* (updates, batch, transações, cursor rolável, `setNull`). **Introspecção responde "não" no vocabulário do JDBC** — `supportsX()` devolve `false`, getters devolvem `ResultSet` vazio com as colunas certas | O DBeaver chama dezenas de métodos de `DatabaseMetaData` ao abrir a conexão; exceção onde o contrato JDBC já prevê um "não" faz a ferramenta parecer quebrada, contrariando a decisão D2 do roadmap (DBeaver/IntelliJ são o cliente-alvo) |
| **D4** | `nullable` publica **`false` para as colunas chave e sequence**, `true` para o resto | É o que é verdade e é provável: [patch.go:138-152](internal/materializer/patch.go:138) recusa evento com chave nula (`ErrInvalidKey`), chave vazia ou sequence nula (`ErrInvalidSequence`). Publicar `true` para a chave seria publicar o falso |
| **D5** | `precision`/`scale`/`displaySize` são **função pura do tipo**, no MESMO `switch` de `JDBCDeTipo`, promovido a devolver um `DescritorJDBC` | Duas tabelas divergem no primeiro tipo novo. O switch único já é cobrado por `TestTodaTagComNomeTemJDBCEspecifico` |
| **D6** | `executor.ColMeta` ganha **só** `Nullable bool`; precision/scale/displaySize são derivados na serialização | Os três são função de `Type`, que `ColMeta` já carrega, e derivá-los em `ColunaDe` custa zero alocação (roda uma vez por query, fora do laço de rows). `Nullable` é função do *campo* (`IsKey`/`IsSequence`) e só o executor tem o `*schema.CompiledField` na mão |
| **D7** | `UNSUPPORTED_CLAUSE` **não acrescenta keyword ao lexer**; `lexer.SyntaxError` ganha um campo `Clausula string`, vazio no erro comum | Keyword nova quebraria `SELECT order FROM t`, hoje válido. Campo (em vez de tipo com `Unwrap`) torna impossível o bug de ordem de `switch` em `traduzErroDeAbertura`, que degradaria em silêncio para `SYNTAX` |
| **D8** | `--param` vale para os **dois** caminhos, `--config` e `--server` | O caminho local já aceita `[]types.Value` em `eng.Execute`. Preserva a promessa de [query_remota.go:26](internal/cli/query_remota.go:26): "um script que troca `--config` por `--server` não muda mais nada" |
| **D9** | JUnit 5 em `scope=test` é aceitável sob "zero dependência de runtime", com **dois gates** provando | `scope=test` nunca entra no `Class-Path` do consumidor. Harness próprio custaria semanas e não fecharia risco real |
| **D10** | Maven Wrapper `distributionType=only-script` — **nenhum `.jar` versionado**, `distributionUrl` com versão exata + `distributionSha256Sum` | Commitar um blob opaco é o oposto da doutrina do binário único. Versão de instrumento é configuração, como o `golangci-lint@v2.12.2` |

---

## Correções ao plano de ciclo original

O `planning/m5-plano-ciclo.md:255-300` está certo em 6 dos 7 itens. Estas quatro afirmações
foram verificadas no código e **mudam o escopo**:

1. **A tabela §73 não tem `precision`/`displaySize`.** As palavras `precision`, `displaySize`
   e `nullable` **não aparecem em lugar nenhum** de `nishidb-spec-proposal.md`. Os números
   são decisão nova do projeto, não transcrição — precisam de tabela justificada (abaixo) e
   de oráculo, não de um "conforme o §73".
2. **Falta `scale`.** `getScale()` é a terceira perna do mesmo tripé e é o que faz o DBeaver
   escrever `TIMESTAMP(3)`. Sai da mesma tabela e dos mesmos testes; deixá-lo para depois é
   um segundo campo aditivo num contrato que o driver já congelou. **Incluído.**
3. **"Toda coluna nullable" é falso hoje** — ver D4.
4. **O item 2 (versão do produto) está entregue**: `protocolo.ProcessoStats.Version`
   ([documentos.go:75](internal/protocolo/documentos.go:75)) alimentado por
   `buildinfo.String()`. Resta só o driver consumir.

E três notas de fato que reescrevem requisitos do driver, todas verificadas:

- **NULL vem como `null` explícito**, não como campo ausente
  ([ndjson.go:41](internal/query/executor/ndjson.go:41)). O oráculo já existe
  (`TestNullExplicito`) e sua justificativa cita o driver. Logo o número de chaves da row é
  igual ao de colunas do schema, na mesma ordem.
- **Coluna `json` sai inline e verbatim**, não como string
  (`appendJSONInline`, [ndjson.go:181](internal/query/executor/ndjson.go:181)). O parser Java
  precisa pular/capturar valor JSON aninhado arbitrário.
- **Chave duplicada é legal**: `SELECT a, a` produz `{"a":1,"a":1}`. O casamento row↔schema
  tem de ser **posicional/ordenado**, nunca por mapa de nome.

---

## A tabela `precision` / `scale` / `displaySize`

Números derivados do **encoder real** ([ndjson.go](internal/query/executor/ndjson.go)), não
de manual. `precision` de texto = caracteres; de numérico = dígitos; de data/hora =
comprimento da representação em String. `displaySize` = largura *normal* máxima.

| Tipo | JDBC | prec | scale | display | Derivação |
|---|---|---|---|---|---|
| `int64` | BIGINT | 19 | 0 | 20 | `9223372036854775807`; display inclui o `-` |
| `double` | DOUBLE | 15 | 0 | 24 | `DBL_DIG`; display cobre `-1.2345678901234567e-308` (23) + folga |
| `boolean` | BOOLEAN | 1 | 0 | 5 | `"false"` |
| `timestamp` | TIMESTAMP | 24 | 3 | 24 | `appendTimestamp` emite `2026-08-23T21:53:04.123Z`; `append3` fixa 3 dígitos |
| `datetime` | TIMESTAMP | 24 | 3 | 24 | byte a byte igual ao timestamp, por decisão do encoder ([ndjson.go:88](internal/query/executor/ndjson.go:88)) |
| `time` | TIME | 12 | 3 | 12 | `23:59:59.999` |
| `string`, `json`, `bytes` | VARCHAR/VARBINARY | 0 | 0 | 0 | Sem teto declarado no schema; `0` é a convenção JDBC para desconhecido |
| `ipv4` | VARCHAR | 15 | 0 | 15 | `255.255.255.255` |
| `ipv6` | VARCHAR | **39** | 0 | 39 | **Não 45.** 45 é a forma mista, que o nosso encoder só usa para `Is4In6` — e essa é *mais curta* (`::ffff:255.255.255.255` = 22). O máximo do fio é 8×4+7 |
| `ipv4_prefix` | VARCHAR | 18 | 0 | 18 | 15 + `/32` |
| `ipv6_prefix` | VARCHAR | 43 | 0 | 43 | 39 + `/128`; sem `Masked()`, host bits ficam |
| `unknown` | OTHER | 0 | 0 | 0 | Não sabemos o tipo, muito menos a largura |

**Por que `0` e não `Integer.MAX_VALUE` em VARCHAR:** o DBeaver escreveria
`VARCHAR(2147483647)` no "Generate SQL" e o DDL seria inaceitável em qualquer destino. Com
`0` ele escreve `VARCHAR` e dimensiona pelo conteúdo.

**Armadilha:** o `switch` atual agrupa cinco tipos numa cláusula (`string, ipv4, ipv6,
prefixos, json → VARCHAR`). Com o descritor eles **deixam de ser iguais**. O agrupamento tem
de ser desfeito com um caso por tipo, ou os endereços saem com `precision: 0` em silêncio.

---

## Fase A — Ajustes no lado Go (D0)

Cada task é TDD: teste primeiro. Trilhas A2→A4, A5→A6 e A7, A8 são independentes e podem ir
em commits separados.

### A1 — Errata de `docs/debitos.md` (sem código)
Marcar **24 e 25 como fechados** (o PR #21 os fechou no código: `7a213b6`, `44b0c26`);
corrigir o texto do **15** (`runtime.query.maxConcurrent` **é** honrado,
[server.go:248](internal/server/server.go:248); `memoryTarget`/`maxBufferPerQuery` já têm
aviso `config.sem_efeito`); corrigir o **44**; registrar a dívida de BI (Metabase/Superset)
que a decisão D2 do roadmap prometeu e nunca foi escrita.
*Pronto quando:* `TestEvidenciasCitadasNosDocumentosExistem` verde e nenhuma entrada afirma o
que o código contradiz.

### A2 — `DescritorJDBC`: a tabela única
`internal/protocolo/jdbc.go`. `JDBCDeTipo` vira casca fina sobre `DescritorDeTipo(t)
DescritorJDBC{JDBC, Precision, Scale, DisplaySize}` — os três testes atuais de `jdbc_test.go`
passam **sem edição**.
*Testes:* `TestODescritorDeCadaTipoTemPrecisionScaleEDisplaySize` (a tabela caso a caso);
**`TestDisplaySizeDeCadaTipoCasaComALarguraQueOEncoderEmite`** — o oráculo que importa:
renderiza o valor de largura máxima por `executor.AppendValue` e afirma
`len(saída) - 2 == DisplaySize` (não há ciclo de import: `internal/query/**` não importa
`internal/protocolo`); `TestTipoSemNomeTemDescritorZerado`.

### A3 — Os campos no wire (`Coluna`, `ColunaMeta`)
[protocolo.go:112](internal/protocolo/protocolo.go:112),
[documentos.go:41](internal/protocolo/documentos.go:41), `ColunaDe`,
[server/documentos.go:69](internal/server/documentos.go:69). **Sem `omitempty`** —
`precision:0` e `nullable:false` são justamente os casos que carregam informação.
*Testes:* `TestALinhaDeSchemaPublicaPrecisionScaleEDisplaySize`;
**`TestMetadataPublicaOsMesmosNumerosQueALinhaDeSchema`** (a não-divergência entre as duas
rotas, que o godoc de `JDBCDeTipo` promete e ninguém cobra);
`TestPrecisionZeroSaiNoJSONEmVezDeSerOmitida`, afirmando sobre **bytes**, não sobre a struct.
*Compatibilidade:* os clientes atuais decodificam sem `DisallowUnknownFields` — campo aditivo
é seguro para CLI velho contra servidor novo.

### A4 — `nullable` (D4)
`executor.ColMeta` e `ColView` ganham `Nullable bool` (precedente exato de `Type`, que já
está lá "serve a metadados (ResultSetMetaData), não à formatação"); propagação em
[executor.go:91](internal/query/executor/executor.go:91) a partir do `*CompiledField`.
*Testes:* `TestColunaChaveSaiComoNaoNulavelNaLinhaDeSchema`,
`TestColunaDeSequenceSaiComoNaoNulavel`, `TestColunaComumSaiComoNulavel`,
`TestMetadataEConsultaConcordamSobreNullable`, e o que **sustenta a afirmação**:
`TestOMaterializerRecusaEventoComChaveNulaEComSequenceNula` (ou citar cobertura equivalente
existente em `internal/materializer`).
*Gate:* rodar `TestScanAllocsIndependenteDoNumeroDeRows` antes e depois — `tetoPorQuery = 38`
é colado, sem folga.
*A verificar durante a implementação:* se o binder resolve `_writtenAt` numa projeção
explícita. Se resolver, ele também é NOT NULL (sempre gravado,
[patch.go:159](internal/materializer/patch.go:159)); se não, não há caso. **Ler o binder, não
deduzir.**

### A5 — `Saude` migra para `internal/protocolo`
[http.go:41-51](internal/server/http.go:41) → `internal/protocolo/documentos.go`. As
constantes `EstadoUP`/`Down`/`Degraded`/`Desligado` **vão junto** — deixá-las para trás faria
o driver casar `"DEGRADED"` contra uma constante que o pacote de contrato não declara.
*Teste:* `TestOCorpoDoHealthNaoMudouComAMudancaDePacote`, comparando bytes (o
`SetIndent("","  ")` torna isso estável). O corpo não pode mudar um byte.
*Armadilha de lint:* mover código entre pacotes não muda a contagem. O risco é o oposto — se
alguém "aproveitar" para consertar um `errcheck` vizinho, a contagem **cai** e o ratchet
reprova. Não consertar nada de passagem.

### A6 — Uniformizar o 405
[autenticacao.go:136-137](internal/server/autenticacao.go:136): `"GET /metrics"` → `"/metrics"`,
`"GET /v1/health"` → `"/v1/health"`, com a conferência de método **dentro** do handler.
*Testes:* estender `TestRotasDeLeituraRecusamMetodoErrado`
([documentos_test.go:100](internal/server/documentos_test.go:100)) de 2 para 4 rotas;
`TestMetricsComMetodoErradoNaoConsultaOBroker`;
`TestMetricsComMetodoErradoNoPlanoLoopbackTambemSaiComCorpoErro`.
*Armadilhas:* (1) a checagem de método é a **primeira** linha de `handleMetrics`, antes do
laço de `AtualizaLag` — senão cada POST inválido dispara chamada ao broker com 5 s de
timeout; (2) `papelDaRota` passa a devolver `existe=true` para esses padrões — ler
`autenticacao_internal_test.go` antes; (3) mudança observável: método errado em `/metrics`
passa a incrementar o contador de erro exposto pelo próprio `/metrics`. Não é bug, mas vai
para o comentário e para o aceite, não para a descoberta de alguém olhando um gráfico do
piloto.

### A7 — `UNSUPPORTED_CLAUSE` (D7)
`lexer.SyntaxError` ganha `Clausula string`; novo `internal/query/parser/clausulas.go`;
`protocolo.CodClausulaNaoSuportada`; `traduzErroDeAbertura`
([server/query.go:387](internal/server/query.go:387)); `codigoDoProtocolo`
([query_remota.go:175](internal/cli/query_remota.go:175)) → `ExitQuery`, **não** o vala-comum
`ExitUso` do `default`.

**Dois sítios de interceptação:**
- **Sítio 1** — [parser.go:45-47](internal/query/parser/parser.go:45), o
  `else if p.cur.Type != lexer.TokenEOF`. Cobre `ORDER`, `GROUP`, `OFFSET`, `JOIN`, `HAVING`,
  `UNION`. Regra dupla: **só `TokenIdent` bare** (`TokenQuotedIdent` nunca dispara — "Quoted
  NUNCA é keyword") e comparação **ASCII** case-insensitive, não `strings.EqualFold` (que faz
  folding Unicode; o godoc de `keywordType` já registra o caso do Kelvin sign). Exportar
  `lexer.ASCIIEqualFold`; a lista fica no parser.
- **Sítio 2** — `DISTINCT` **não chega ao sítio 1**: `SELECT DISTINCT a FROM t` faz
  `identifier()` aceitá-lo como nome de coluna e o erro nasce apontando para `a`. O guard vai
  **dentro de `parseProjection`**, guardando o `lexer.Token` **antes** de `identifier()` —
  fazer isso sobre `st.Projection[0].Name` dispararia também para `SELECT "distinct" x FROM t`,
  porque `ColumnRef` não registra se veio entre aspas.

Não exigir `BY` depois de `ORDER`/`GROUP`: custaria um segundo lookahead num parser que se
declara LL(1).

*Testes:* `TestAsSeteClausulasNaoSuportadasSaoNomeadasUmaAUma` (com asserção de `Pos`
apontando para o token da cláusula); os três de **não-regressão, que são o coração da
decisão**: `TestSelectDeColunaChamadaOrderSemAspasContinuaValido`,
`TestColunaChamadaOrderEntreAspasContinuaValida`,
`TestWhereSobreColunaChamadaOrderContinuaValido`; mais
`TestColunaChamadaDistinctEntreAspasSeguidaDeOutraNaoDisparaAClausula`;
`TestClausulaNaoSuportadaContinuaSendoErroDeSintaxeParaOsClassificadores`
(`errors.Is(err, parser.ErrSyntax)` continua true);
`TestUnsupportedClauseNaoCaiNoValaComumDoExitCode`. `TestParseAllocsTripwire` sob 64,
inalterado.

### A8 — `--param` (débito 58, D8)
`--param <tipo>:<valor>` repetível, split no **primeiro** `:` (`--param string:a:b` é a string
`a:b`), 1:1 com os 4 tipos de wire. `string` → `json.Marshal`; `int64`/`double` → texto
**verbatim** como `json.RawMessage` (é o que faz `--param int64:9223372036854775808` receber
a mensagem de overflow de [parametros.go:106](internal/protocolo/parametros.go:106) em vez de
virar float64 e perder precisão — a mesma razão de `Parametro.Value` ser `RawMessage` e não
`any`). Validação chama `protocolo.Valores`, **a mesma função do servidor**, nunca uma segunda
gramática.
*Testes:* os quatro tipos ponta a ponta; `TestParamRepetidoPreservaAOrdemPosicional`;
`TestParamStringComDoisPontosNoValorNaoEhCortada`;
`TestParamInt64AcimaDoTetoRecusaComAMensagemDoProtocolo`;
`TestParamFuncionaTambemNoCaminhoLocalComConfig`; e `TestParamNuloEnsinaIsNull` — o oráculo do
lado Go para o item `setNull`: **não existe** sintaxe de `--param` que produza null.
*Armadilhas:* (1) `flag` chama `String()` no valor zero para montar o `-h` — receptor por
valor, tolerante ao zero, ou `--help` panica; (2) `flag.Parse` para no primeiro posicional, e
`--param` é a primeira flag que um humano vai instintivamente pôr **depois** da query — a
mensagem de `NArg != 1` deve sugerir "as flags vêm antes da query", e o README fixa a ordem;
(3) **lint**: [query.go](internal/cli/query.go) tem 11 `fmt.Fprintln` bare e o comentário da
linha 37 já registra que código novo usa `_, _ =`. "Limpar" as vizinhas derruba a contagem e o
ratchet reprova.

### A9 — Flush após a linha de schema
Um `flusher.Flush()` logo depois do `EscreveLinha(LinhaSchema)` em `server/query.go`. Custa um
chunk por consulta e paga: `executeQuery()` devolve `ResultSetMetaData` sem esperar o primeiro
lote de `resultBatchSize`, e o erro precoce é detectado antes.

---

## Fase B — Esqueleto do módulo Java

```
clients/jdbc/
├── pom.xml                                   groupId dev.nishisan, artifactId nishidb-jdbc
├── mvnw, mvnw.cmd, .mvn/wrapper/*.properties só scripts, nenhum .jar (D10)
├── src/main/java/dev/nishisan/nishidb/jdbc/  + subpacotes .json e .wire
├── src/main/resources/META-INF/services/java.sql.Driver
└── src/test/java/…
```

`<maven.compiler.release>21</maven.compiler.release>` (nunca `source`/`target`),
`<finalName>nishidb-jdbc</finalName>` para que o artefato seja literalmente o nome do §70,
`Automatic-Module-Name` no MANIFEST e **sem** `module-info.java` (ferramentas carregam driver
por `URLClassLoader` no classpath).

**Errata de coordenadas:** a spec §98 desenha `jdbc/nishidb-jdbc/`; a decisão D4 do roadmap diz
`clients/jdbc/` e é mais nova. Corrigir a spec — não deixar duas coordenadas vivas.

Classes: `NishiDriver` (`acceptsURL` devolve **`false`**, nunca lança, para o `DriverManager`
seguir tentando; `connect` devolve `null` se a URL não é nossa), `NishiUrl` (recusa userinfo
embutido — credencial em URL é o que o §95 evita), `ConnectionConfig` (precedência
`Properties` > URL, explícita e testada).

Makefile: `build-jdbc`, `test-jdbc-unit`, `test-jdbc`. **`make test` de Go fica intocado** — a
suíte Go não pode passar a exigir JVM. Ausência de JDK 21 **falha alto e nomeia o que
instalar**, nunca vira SKIP (doutrina do [Makefile:61-71](Makefile:61)).

`.gitignore`: `clients/jdbc/target/` (escopado, nunca `target/` solto), e acrescentar `target`
à lista de diretórios que `testesDoRepositorio`
([evidencias_test.go:55](internal/cli/evidencias_test.go:55)) pula junto de `.git` e `.claude`.

**Gate D9** — `internal/gates/jdbc_test.go` lê `clients/jdbc/pom.xml` **como dado** (mesma
doutrina de `ci_test.go` lendo os workflows) e falha se qualquer `<dependency>` não tiver
`<scope>test</scope>`. Sobrevive a alguém trocar o pom sem rodar Maven.

---

## Fase C — Parser JSON próprio

**Escopo.** Duas gramáticas de consumo: (a) documentos pequenos — linha de schema,
`/v1/metadata`, `/v1/stats`, `/v1/health`, `{"error":{…}}` — em DOM completo, com aninhamento,
`null` explícito (`ColunaMeta.Alias` é `*string`) e mapas `string→string`; (b) linhas de dados
— objeto no topo cujos valores podem ser **objeto/array aninhado arbitrário** (coluna `json`
inline). Mais escrita: escaping de string e números.

**Recusar, com offset do byte na mensagem:** `NaN`, `Infinity`, `+1`, `007`, `.5`, `5.`, `1e`,
hexadecimal, aspas simples, chave sem aspas, vírgula sobrando, comentários, byte de controle
cru dentro de string, escape desconhecido, `\u` truncado, conteúdo após o valor de topo, valor
de topo que não é objeto numa linha de dados, e profundidade acima de 200 — para que documento
hostil vire `SQLException`, nunca `StackOverflowError`.
**Não recusar chave duplicada** (`SELECT a, a` é legal), com comentário explicando por quê.

**Números — onde parsers ingênuos erram.** O scanner grava o *span* e um bit `integral`, e
**nunca converte para `double`**. `getLong` parseia os dígitos ASCII com detecção de overflow:
`9223372036854775807` sai exato, `…808` vira `SQLDataException` (22003), nunca um double
arredondado em silêncio — o espelho exato de por que `Parametro.Value` é `json.RawMessage`.
Para doubles em grafia `'g'` do Go (`1e-07`, `1.5e+21`): **nós validamos a gramática RFC 8259
sobre o span, o JDK arredonda** (`Double.parseDouble` do ASCII já validado — ele sozinho
aceitaria `NaN`, `0x1p3` e sufixos `d/f`). O débito 30 não é risco de corretude: as duas
conversões decimal→binary64 são corretamente arredondadas por IEEE-754. A regra é **nunca
comparar double como texto**, e o teste é round-trip de bits.

**Desenho do cursor de linha — nem DOM nem streaming puro, mas spans:**

```java
final class StreamCursor implements RowCursor {
    private byte[] linha;            // reusado; cresce, nunca encolhe
    private final int[] ini, fim;    // por coluna, alocados UMA vez
    private final byte[] kind;       // STRING|NUMBER|BOOL|NULL|RAW
}
```

Por row: uma leitura para dentro de buffer reusado + uma varredura que preenche três arrays de
tamanho fixo. **Zero alocação por row**; `String`/`Long`/`byte[]` só nascem no getter. É o §128
aplicado ao cliente.

**Bytes, não `char`** — quebrar em `\n` cru é seguro porque o encoder escapa todo byte < 0x20
dentro de string e troca controles por espaço no json inline; nenhum LF sobrevive dentro de um
valor. Isso também evita `BodyHandlers.ofLines()`, que alocaria uma `String` por row e
destruiria os spans.

**Casamento row↔schema ordenado**, avançando o ponteiro do schema até o nome bater — absorve
chave repetida e recusa **alto** uma chave que não casa nenhuma coluna restante (protocolo
violado, não linha vazia). Teto de linha de 64 MiB para falhar em vez de OOM.

---

## Fase D — Transporte e porta do `protocolo.Leitor`

`Transporte` é o **único** ponto que monta `HttpRequest`. Doutrinas herdadas de
[credencial.go](internal/cli/credencial.go), todas literais:

- **`.proxy(HttpClient.Builder.NO_PROXY)` sempre, sem opção.** O `HttpClient` default honra
  `http.proxyHost`/`nonProxyHosts`, e o DBeaver seta essas propriedades globalmente. No piloto
  há proxy corporativo que responde **403 e o sintoma engana**
  ([piloto-events.md:211-227](docs/piloto-events.md:211)). *Efeito colateral a alinhar:* o
  RUNBOOK do ciclo M5 manda configurar `http.nonProxyHosts` para o JDBC — com `NO_PROXY` fixo
  essa linha fica desnecessária e **deve ser corrigida**, ou alguém vai depurar o proxy errado.
- **`.version(HTTP_1_1)` fixo.** O servidor usa `ServeTLS` sem `NextProtos` próprio, então o Go
  habilita h2 via ALPN e o `HttpClient` prefere HTTP/2. Sem multiplexação não perdemos nada —
  uma conexão JDBC executa uma consulta por vez.
- **`BodyHandlers.ofInputStream()`** é o único handler correto. `ofString`/`ofByteArray`
  bufferizam o resultado inteiro (violação direta do §128). O javadoc é explícito: o stream tem
  de ser consumido ou fechado, senão a conexão nunca é liberada — é o vazamento que aparece
  como "o DBeaver trava depois de 20 consultas".
- **3xx é erro `08P01`**, não corpo válido.
- **Nenhum `trustAll`/`insecure`, nem por `Properties`.** Uma propriedade opcional vira o modo
  permanente de operação em três semanas.

`LeitorNdjson` porta `protocolo.Leitor` com os mesmos três desfechos separados: `schema()`,
`fim()`, `erro()` e falha de transporte. **Stream que acaba sem fecho é falha**, com a mesma
mensagem. Um fecho de erro depois de N rows lança a exceção mapeada **citando as N rows já
entregues** — jamais devolve `false`, que apresentaria resultado parcial como completo.

**Espelhar `dicaDeEsquema`** ([query_remota.go:66](internal/cli/query_remota.go:66)): porta TLS
respondendo a texto puro devolve `Client sent an HTTP request to an HTTPS server`, e a mensagem
deve sugerir `tls=true` — senão o usuário de DBeaver vê "400 sem corpo legível" e culpa o
servidor.

### Tabela código → `SQLState`

| Código | SQLState | Subclasse |
|---|---|---|
| `SYNTAX` | 42601 | `SQLSyntaxErrorException` |
| `BIND`, `RESERVED_COLUMN` | 42000 | `SQLSyntaxErrorException` |
| `UNKNOWN_DATASET` | 42S02 | `SQLSyntaxErrorException` |
| `UNSUPPORTED_CLAUSE` | 0A000 | `SQLFeatureNotSupportedException` |
| `BAD_PARAMETER`, `MAX_ROWS_ABOVE_LIMIT` | 22023 | `SQLDataException` |
| `MAX_ROWS_EXCEEDED` | 53400 | `SQLNonTransientException` |
| `TOO_MANY_QUERIES` | 53000 | `SQLTransientException` |
| `REQUEST_TOO_LARGE` | 54000 | `SQLNonTransientException` |
| `TIMEOUT` | 57014 | `SQLTimeoutException` |
| `COMPACTION_BUSY` | 55006 | `SQLTransientException` |
| `UNAUTHENTICATED` | 28000 | `SQLInvalidAuthorizationSpecException` |
| `FORBIDDEN` | 42501 | `SQLNonTransientException` |
| `BAD_REQUEST`, `METHOD_NOT_ALLOWED`, `UNSUPPORTED_MEDIA_TYPE` | 08P01 | `SQLNonTransientConnectionException` |
| `DATASET_CORRUPT` | XX001 | `SQLNonTransientException` |
| `WRITER_FAILED` | 58030 | `SQLNonTransientException` |
| `INTERNAL` | XX000 | `SQLNonTransientException` |

Fora do wire: conexão/DNS/TLS → 08001; stream sem fecho / linha ilegível → 08006
(`SQLRecoverableException`); método JDBC não suportado → 0A000; **código desconhecido → HY000
com o código cru na mensagem** (jamais degradar para `INTERNAL`, que apagaria a novidade).

**Gate dos dois lados** — `internal/gates/jdbc_test.go` parseia `internal/protocolo/protocolo.go`
com `go/ast`, coleta **toda** constante `Cod*` de valor string e falha se algum literal não
aparecer em `SqlStates.java`. Sem duplicar a lista em lugar nenhum — a mesma técnica de
`TestVersaoDoGolangciLintNoWorkflowBateComAInstrucaoDoMakefile`: comparar duas *extrações*,
nunca um valor escrito à mão. **Código novo em Go sem SQLState reprova no CI Go, antes de
qualquer JVM.**

---

## Fase E — `ResultSet` streaming e conversão de tipos

`NishiResultSet` é **uma implementação só**, sobre a SPI `RowCursor` — `StreamCursor` (NDJSON)
e `ArrayCursor` (`Object[][]`, para os sintéticos do `DatabaseMetaData`) são as duas fontes.
Duplicar significaria que uma correção de conversão vale só num dos dois.

**Regra geral: `getString` devolve o texto do fio** — grafia exata que o servidor emitiu.
Lossless, grátis (temos o span), e resolve o problema de fuso sem mentir.

**Os quatro pontos delicados:**

- **`wasNull()`** — o token é `null` explícito. Todo getter, ao ver `null`, grava a flag e
  devolve o default do tipo; **ao ver valor, limpa a flag**. Manter a flag entre chamadas é o
  bug clássico.
- **`timestamp`/`datetime`** — o valor **é um instante** UTC. `new Timestamp(millisUTC)`
  preserva o instante exatamente; só `toString()` renderiza no fuso da JVM.
  `getTimestamp(int, Calendar)` **ignora o Calendar** e devolve o mesmo instante — o contrato do
  `Calendar` é reinterpretar um wall clock sem fuso, e aqui não há wall clock a reinterpretar.
  Divergência deliberada, documentada. Escape lossless: `getObject(col, Instant.class)`.
  **Teste obrigatório com `TimeZone.setDefault("Asia/Tokyo")`.**
- **`time`** — millis desde a meia-noite, não é instante. `new Time(millisDoDia)` **desloca o
  relógio pelo offset da JVM**, e `Time.valueOf(LocalTime)` **descarta os millis**. Construção
  correta: `LocalTime.ofNanoOfDay(millis * 1_000_000L)` → `LocalDate.EPOCH.atTime(lt)` →
  `atZone(systemDefault())`. Caminho recomendado ao usuário: `getObject(col, LocalTime.class)`.
- **`bytes`** — `Base64.getDecoder()` (alfabeto std com padding, espelhando
  `base64.StdEncoding.AppendEncode`). A recusa de lixo à direita vira `SQLDataException`, não
  array truncado.
- **`OTHER`/`unknown`** — coluna `auto` que nenhum evento fixou. A ingestão é concorrente: a
  coluna pode ser anunciada `OTHER` no bind e trazer um inteiro na row. `getObject` devolve
  conforme o token efetivo; `getString` sempre funciona. **Precisa de teste** — é a única coluna
  cujo metadata pode discordar do dado.

**`setMaxRows` vs `maxRows` do wire** (débito 29, decidido aqui): o wire **recusa** `maxRows`
acima do teto do dataset (`MAX_ROWS_ABOVE_LIMIT`) em vez de aparar, e o DBeaver seta
`maxRows=200` por default. Regra: `n <= teto` → envia `n`; `n >= teto` ou `n == 0` → **omite o
campo** (o teto do dataset já satisfaz "no máximo n"). O teto vem do `/v1/metadata` buscado no
connect. E os dois "truncado" são distintos: `{"nishidb":"end","truncated":true}` por *nosso*
`maxRows` é sucesso; `MAX_ROWS_EXCEEDED` no fecho de erro é o teto do dataset.
**`MAX_ROWS_EXCEEDED` vira `SQLWarning`, nunca exceção** — porque o caso de fronteira (resultado
com exatamente `maxRows` linhas) é falso positivo conhecido, e o custo de distingui-lo no
servidor é varrer o dataset inteiro, exatamente o que o teto existe para evitar.

**Três relógios distintos, e confundi-los mata stream legítimo:** `connectTimeout` (só
handshake, default 10 s); `HttpRequest.timeout()` **não setar por default** — ele estoura mesmo
com bytes fluindo; `Statement.setQueryTimeout(s)` vai no wire em `timeoutMs`, **clampado com
`min` contra `DatasetMeta.TimeoutMs`** (o servidor recusa acima do teto).

**`Connection.close()`** — ordem obrigatória: `ResultSet` abertos → `Statement` → `HttpClient`.
`HttpClient` só virou `AutoCloseable` no **Java 21** (motivo técnico concreto do piso da §70).
`close()` bloqueia; usar `shutdownNow()` + `awaitTermination` curto para nunca pendurar a UI.

**`setFetchSize`** é hint pelo contrato JDBC e pode ser aceito em silêncio — única exceção
legítima à regra "nunca ignore em silêncio", e merece comentário para não parecer descuido.
`setMaxFieldSize`, `setCursorName`, `addBatch` e `setNull` lançam.

---

## Fase F — `PreparedStatement`

Contagem de `?` fora de literais (mesmo scanner do extrator de `FROM`, D2), os 4 tipos de wire,
índice fora de faixa recusado no cliente com número **1-based** (como o binder).

**`setTimestamp`/`setTime` viajam como `int64`**, não como string. Verificado em
[coerce.go:240](internal/query/binder/coerce.go:240): uma string contra coluna `datetime` é
interpretada no `format:` **declarado do campo, resolvido em `time.Local` do host do
servidor** — um `setTimestamp` que enviasse ISO-8601 funcionaria para `timestamp` e falharia
para `datetime`. `celula()` aceita `OperandInt` para os três, então epoch millis (e millis do
dia para `time`) é o único caminho uniforme.

`setNull` → `SQLFeatureNotSupportedException` nomeando `IS NULL`, com teste de que **não**
devolve resultado vazio em silêncio.

O exemplo do §130 tem de rodar literalmente.

---

## Fase G — `DatabaseMetaData` (errata D3)

**De `/v1/metadata`:** `getTables` (com casamento LIKE implementado no driver, 10 colunas
canônicas, `TABLE_TYPE="TABLE"`); `getColumns` (24 colunas; `REMARKS` carrega o que o wire tem
e o JDBC não modela: `alias=…; index=bitmap; auto=true`; `NULLABLE` de D4);
`getPrimaryKeys` de `DatasetMeta.Key` — ganho real, o DBeaver desenha a chave; `getIndexInfo`
de `ColunaMeta.Index != "none"`, com a ressalva do godoc (índice **declarado**, não plano
escolhido) no `REMARKS`; `getTypeInfo` com `SEARCHABLE` honesto (`bytes` e `json` →
`typePredNone`, porque o binder recusa comparação); `getCatalogs`/`getSchemas` **vazios com a
forma certa**.

**De `/v1/stats`:** `getDatabaseProductVersion` = `process.version`. Major/minor extraem `vX.Y`
do começo; **não parseável → 0, nunca exceção**.

**`supportsX() = false`:** todos os `supportsGroupBy*`, `*OuterJoins`, `supportsSubqueries*`,
`supportsUnion*`, `supportsOrderByUnrelated`, `supportsTransactions` (com
`getDefaultTransactionIsolation() = TRANSACTION_NONE`), `supportsStoredProcedures`,
`supportsBatchUpdates`, `supportsColumnAliasing` (não há `AS`), `supportsANSI92*`,
`supportsSavepoints`, `supportsPositionedUpdate/Delete`.
**`true`:** `allTablesAreSelectable`, `supportsMixedCaseIdentifiers` (identificadores são
case-sensitive), `isReadOnly`, `supportsResultSetType(TYPE_FORWARD_ONLY)`.
`getIdentifierQuoteString()` = `"`; `getRowIdLifetime()` = `ROWID_UNSUPPORTED`;
`getSQLStateType()` = `sqlStateSQL`.

**`findColumn`**: identificadores são case-sensitive mas o JDBC promete case-insensitive.
Regra: exato primeiro, depois case-insensitive; ambiguidade no segundo passo → `SQLException`.

**`AssinaturasTest`** — a tabela exaustiva de todos os métodos de
`Connection`/`Statement`/`PreparedStatement`/`ResultSet`, cada um classificado `IMPLEMENTADO`
ou `NAO_SUPORTADO`, comparada com `Class.getMethods()`. Método novo não classificado
**reprova**; todo `NAO_SUPORTADO` é invocado e tem de lançar. É o teste literal do §107.

---

## Fase H — Integração e CI

**Corpus:** reusar [testdata/todos_os_tipos.yaml](testdata/todos_os_tipos.yaml) — já tem um
campo de cada tipo do wire, incluindo os temporais com formatos distintos e o `json` aninhado.

**`scripts/jdbc-it.sh`**, `set -euo pipefail` + `trap` de limpeza:
1. Pré-voo: binário Go e `java` ≥ 21 — ausência **falha alto e nomeia**, nunca SKIP.
2. Tudo num `mktemp -d`, removido no trap. Nada escrito na árvore.
3. **Certificado**: `go run ./scripts/certgen` (~60 linhas, molde de
   [tls_test.go:44](internal/server/tls_test.go:44): EC P-256, SAN `127.0.0.1` + `localhost`,
   validade curta). Nenhuma ferramenta externa nova, nenhum segredo versionado.
4. **Senha** gerada em runtime, `passwordFile` 0600 no tmpdir, entregue à JVM por env — nunca
   por flag (§95).
5. **Porta**: `server.port: 0` é recusado pela config, então sortear em 20000–60000 e tentar
   subir (o `net.Listen` falha rápido e claro). *Alternativa mais limpa, uma linha de Go:*
   trocar `s.cfg.Endereco()` por `s.Endereco()` no log `query.pronta`
   ([server.go:371](internal/server/server.go:371)) tornaria a porta efêmera utilizável e
   mataria a corrida de vez.
6. **Prontidão**: aguardar a linha JSON `query.pronta` no stderr, prazo de 30 s, despejando o
   log ao estourar. **Nada de `sleep`.**
7. `./mvnw -B test` com as variáveis de ambiente; trap derruba e limpa.

**Contra o SKIP silencioso** (o pecado do débito 53 traduzido para JUnit): as classes `*IT` usam
`@EnabledIfEnvironmentVariable` para que `mvnw test` avulso funcione, **e** existe um teste que
**falha** quando `NISHIDB_IT_REQUIRED=1` está setada e a URL não está. O `make test-jdbc` seta
`REQUIRED=1`. Harness quebrado reprova; não passa verde com zero testes rodados.

**Job `jdbc` no `ci.yml`** (`setup-java@v4`, `temurin`, `21`, `cache: maven`) rodando
`make test-jdbc`. Extensões a `internal/gates/ci_test.go`, no estilo do arquivo: o job existe e
roda o alvo; `java-version` é **exata** (nunca `21.x`) e `distribution` é fixada; e **a
`java-version` do workflow bate com `<maven.compiler.release>` do `pom.xml`**, comparando as
duas extrações — nunca um `"21"` escrito à mão no teste, que reabriria o defeito que o teste
fecha.

**Prova do §128 no cliente:** `SELECT * FROM todos_os_tipos` sobre >100k rows com `-Xmx64m` no
surefire.

**TLS:** conecta com `tlsCaFile`; **e um teste de que sem a CA a conexão FALHA** — a prova de
que não há `InsecureSkipVerify` escondido.

---

## Fase I — DBeaver, documentação e aceite

Roteiro manual: conecta, lista datasets e colunas, `SELECT` com parâmetro, e a mensagem de
`UNSUPPORTED_CLAUSE` para `ORDER BY`.

`docs/aceite-fase7.md` (novo) — **e acrescentá-lo à lista de
[evidencias_test.go:30](internal/cli/evidencias_test.go:30) no mesmo commit**, senão o documento
nasce fora do único verificador que o projeto tem, com liberdade de citar teste inexistente.

**Duas armadilhas de gate existentes:**
- `TestEvidenciasCitadasNosDocumentosExistem` indexa só `*_test.go` e casa o regexp
  `\bTest[A-Z]`. Convenção: classes Java **sufixadas** `*Test.java`/`*IT.java` (default do
  surefire, não casa o regexp), **e** estender `testesDoRepositorio` para indexar métodos Java,
  para que o aceite possa citar evidência Java com honestidade.
- `TestQuickstartDoREADMEExecuta` ([readme_test.go:74](internal/cli/readme_test.go:74))
  **executa** os blocos ```yaml``` e ```console``` do README **por índice**. A seção JDBC tem de
  vir **depois** do quickstart e usar ```java```/```xml```/```properties``` — um ```console```
  novo antes dele quebra o gate.

Atualizar `README.md`, `docs/architecture.md`, os `.puml` afetados e `docs/debitos.md`
(fecham 12 e 58; abrem os novos limites: sem `setNull`, sem cursor, sem `ORDER BY`, `Calendar`
ignorado em `getTimestamp`). Errata da spec: §71 (D3) e §98 (coordenadas do módulo).

---

## Débitos: o que entra e o que fica de fora

| Débito | Veredito | Razão |
|---|---|---|
| **58** (`--param`) | **DENTRO** (A8) | É o único caminho de parâmetros sem prova ponta a ponta pelo próprio produto. Sem ele, testar o Java contra um caminho Go nunca exercitado por um cliente Go é testar duas coisas de uma vez |
| **29** (teto exato) | **Decisão no driver**, sem código Go | O sintoma é pior que o registrado: sai como moldura de **erro** `MAX_ROWS_EXCEEDED`, não como flag. Distinguir exige varrer até a próxima linha viva que casa — potencialmente o dataset inteiro. Resolvido virando `SQLWarning` (Fase E) |
| **30** (grafia `'g'`) | **FORA**, e o comentário pode ser fechado | Não é risco de corretude: `ParseFloat` e `Double.parseDouble` são ambos corretamente arredondados por IEEE-754. Vira uma linha de doutrina no driver ("nunca comparar double como texto") + teste de round-trip de bits |
| **34** (`Stats` engole `ErrDatasetClosed`) | **FORA** — mas **corrigir a justificativa do débito** | O `debitos.md` o adia por tamanho ("arrasta todos os chamadores"); são só 3 chamadores não-teste. A razão de verdade é melhor: o sintoma é **inalcançável pela superfície pública** — `encerra()` faz `http.Shutdown` (que drena as requisições em voo) **antes** de `fechaDatasets()`. A justificativa fraca é por que o item já rolou M4→M5→M6 por inércia |
| **15/28** (`StreamWriter`, `maxBufferPerQuery`) | **FORA** | O pior caso do 28 não vale no servidor: o `StreamWriter` é criado **por requisição** e morre com ela. O "N × maior linha" do 15 é bounded por `maxConcurrent` (honrado) e `MaxRecordSize` (4 MiB). Assunto de M6 |
| **19** (índice hash físico) | **FORA** | Fase 8/M6. Consequência a documentar: consulta por `node` no piloto é full scan, e um usuário de DBeaver vai sentir |

---

## Verificação

1. `make test`, `make test-race`, `gofmt -l .` vazio.
2. **Contagem de lint bate `.golangci-baseline` exatamente** — ratchet bidirecional, e cada
   `.go` novo (`certgen`, `clausulas.go`, `jdbc_test.go`) muda o número:
   ```bash
   golangci-lint run --max-issues-per-linter=0 --max-same-issues=0 --output.json.path=/tmp/l.json; jq '.Issues|length' /tmp/l.json
   ```
   A baseline é re-medida e atualizada **no mesmo PR**. *(Nota: o arquivo tem `389`, mas
   `Makefile:58` e o débito 53 ainda dizem `390` — corrigir a prosa na A1.)*
3. `TestScanAllocsIndependenteDoNumeroDeRows` com `tetoPorQuery` **inalterado** em 38;
   `TestParseAllocsTripwire` sob 64.
4. **Gate de não-divergência:** `/v1/metadata` e a linha de schema de `/v1/query` publicam
   `type`, `jdbc`, `nullable`, `precision`, `scale`, `displaySize` idênticos para a mesma coluna.
5. **Gate de honestidade:** `displaySize` de cada tipo de largura fixa casa com o que
   `executor.AppendValue` de fato emite.
6. `make test-jdbc` verde do zero em máquina limpa e no runner; §107 coberto item a item.
7. `-Xmx64m` sobre >100k rows não estoura heap.
8. Matriz de conversão passa em UTC **e** em `Asia/Tokyo`.
9. Gate Go de cobertura código→SQLState: nenhuma constante `Cod*` sem SQLState.
10. Contra o piloto (loopback, `curl --noproxy '*'`): `POST /metrics` devolve `CorpoErro`;
    `ORDER BY` devolve `UNSUPPORTED_CLAUSE`;
    `nishidb query --server … --param string:Stop 'SELECT … WHERE status = ?'` devolve linhas.
11. DBeaver: conecta, navega, consulta com parâmetro, e a mensagem de `ORDER BY` é acionável.

---

## Riscos

| # | Risco | Prob. | Mitigação |
|---|---|---|---|
| R1 | O agrupamento de 5 tipos no `switch` sobrevive à A2 e `ipv4`/`ipv6`/prefixos saem com `precision: 0` | **Alta** — é o caminho de menor esforço | `TestDisplaySizeDeCadaTipoCasaComALarguraQueOEncoderEmite`, que só passa com o número real |
| R2 | Alguém acrescenta `ORDER`/`GROUP` ao `keywordType` "para simplificar" a A7 | Média — é a solução óbvia | `SELECT order FROM t` funciona **hoje** e pode estar em produção no piloto. Os três testes de não-regressão são a primeira coisa escrita da task |
| R3 | Ratchet de lint reprova por **queda**, porque alguém consertou um `errcheck` vizinho em `internal/cli/query.go` | **Alta** — o arquivo tem 11 `Fprintln` bare convidativos | Código novo com `_, _ =`, vizinhança intocada. `git diff --stat` antes do push |
| R4 | `omitempty` entra nos campos novos num refactor de "limpeza" e derruba `nullable:false` e `precision:0` | Média | `TestPrecisionZeroSaiNoJSONEmVezDeSerOmitida`, afirmando sobre **bytes** |
| R5 | `handleMetrics` ganha a checagem de método **depois** do refresh de lag | Média | `TestMetricsComMetodoErradoNaoConsultaOBroker` |
| R6 | O guard de `DISTINCT` dispara para `SELECT "distinct" x FROM t` | Média se feito em `parseSelect` | Implementar em `parseProjection`, guardando o token antes de `identifier()` |
| R7 | `nullable:false` para a chave se revela falso num caminho não lido (replace, compaction, campo de sistema projetável) | Baixa | O teste amarra a afirmação à recusa real; e a A4 obriga a **ler o binder** sobre `_writtenAt` antes de decidir |
| R8 | `ofString`/`ofLines` entram no `Transporte` por conveniência e o §128 morre no cliente | Média | O teste `-Xmx64m` sobre 100k rows é o único jeito de provar; roda no CI |
| R9 | `getTime` desloca o relógio pelo fuso da JVM, ou perde os millis | **Alta** — os dois construtores óbvios estão errados | Testes com `TimeZone.setDefault("Asia/Tokyo")` na matriz inteira |
| R10 | `docs/aceite-fase7.md` nasce fora de `evidencias_test.go` | Média | Item explícito no "Pronto quando" da Fase I |
| R11 | O RUNBOOK continua mandando configurar `http.nonProxyHosts` depois de o driver fixar `NO_PROXY` | Média | Correção do RUNBOOK é entregável da Fase I, não nota de rodapé |

---

## Arquivos críticos

- [internal/protocolo/jdbc.go](internal/protocolo/jdbc.go) — a tabela única; a A2 inteira
- [internal/protocolo/protocolo.go](internal/protocolo/protocolo.go) — `Coluna` (112), códigos de erro (162-203), tipos de wire de parâmetro
- [internal/protocolo/stream.go](internal/protocolo/stream.go) — o contrato a portar para Java
- [internal/query/executor/ndjson.go](internal/query/executor/ndjson.go) — o **produtor** de tudo que o parser Java vai ler
- [internal/query/parser/parser.go](internal/query/parser/parser.go) — os dois sítios da A7
- [internal/server/query.go](internal/server/query.go) — linha de schema (254-266), tradução de erro (385-410), flush da A9
- [internal/cli/query.go](internal/cli/query.go) — `--param` e a armadilha do ratchet
- [internal/cli/credencial.go](internal/cli/credencial.go) — as doutrinas de cliente a herdar
- [internal/gates/ci_test.go](internal/gates/ci_test.go) — molde dos gates novos (arquivo lido como dado, versões comparadas por extração)

---

## Estado em 2026-08-24 — Fases A a G concluídas

Branch `feature/driver-jdbc`, 21 commits sobre `main`. **Tudo verde**: `go test ./...`,
`go test -race ./...`, `gofmt -l .` vazio, `make test-jdbc-unit` com **195 testes Java** e a
contagem de lint **igualada em 388**.

| Fase | Estado | Onde |
|---|---|---|
| A (D0, lado Go) | **completa**, 9 tasks | descritor de tipo, nullable, Saude no protocolo, 405 uniforme, UNSUPPORTED_CLAUSE, `--param`, flush do schema |
| B (módulo Maven) | **completa** | `clients/jdbc/`, wrapper only-script pinado, gates em `internal/gates/jdbc_test.go` |
| C (JSON próprio) | **completa** | `…/jdbc/json/`: scanner com spans, DOM para documentos pequenos, escritor |
| D (transporte + leitor + SQLState) | **completa** | `…/jdbc/wire/`: `Codigos`, `SqlStates`, `Molduras`, `LeitorNdjson`, `Transporte` |
| E (ResultSet e conversões) | **completa**, 4 commits | `SqlScanner`, `RowCursor`/`StreamCursor`/`ArrayCursor`, `Temporais`, `TiposJdbc`, `NishiResultSet(MetaData)`, `NishiDriver`, `ConnectionConfig`, `NishiConnection`, `NishiStatement` |
| F (PreparedStatement) | **completa** | `NishiPreparedStatement`, `NishiParameterMetaData` |
| G (DatabaseMetaData) | **completa** | `NishiDatabaseMetaData`, `AssinaturasTest` |
| **H e I** | **a fazer** | integração contra o binário + CI; DBeaver, documentação e aceite |

### Decisões tomadas na execução de E a G, que o plano não fixava

1. **A conversão é dirigida pelo TOKEN; o tipo declarado só entra onde o token é
   ambíguo.** Texto que é instante, texto que é hora do dia e texto que é base64 têm a
   mesma cara no JSON: os dois primeiros são separados pela GRAMÁTICA (e por isso
   `getTimestamp` sobre uma coluna `time` falha sozinho, sem caso especial), e o binário
   é separado pelo TIPO. Duas exceções nomeadas: `getObject` segue o tipo declarado
   (senão um `double` de valor inteiro, que sai do encoder como `1`, apareceria como
   `Long`), e a coluna OTHER segue o token.

2. **`getDate` mudou de semântica em relação ao que o plano insinuava.** O
   `java.sql.Date` sai NORMALIZADO (contrato do tipo) e a divisão em dias é feita em
   **UTC**, não no fuso da JVM — 21:53Z cai no dia 23 em São Paulo e no 24 em Tóquio, e
   um relatório não pode mudar de dia conforme a máquina. Daí a regra geral do Calendar:
   **honrado onde a resposta exige escolher um calendário** (`getTime`, `getDate`) e
   **ignorado onde a resposta é o instante** (`getTimestamp`).

3. **`getBytes` só vale na coluna binária**, contra a convenção do JDBC. Uma coluna
   `bytes` que o dataset ainda não fixou chega anunciada OTHER, e a convenção faria o
   mesmo método devolver ora o binário decodificado, ora os caracteres do base64.

4. **`getTypeInfo` é DERIVADO do `/v1/metadata`**, e não uma segunda tabela de tipos no
   driver — que divergiria da do servidor no primeiro tipo novo. Preço declarado: tipo
   que nenhum dataset usa não aparece.

5. **O `AssinaturasTest` do §107 classifica por COMPORTAMENTO OBSERVADO**, não por tabela
   escrita à mão: cada método é invocado por reflexão e o único desfecho aceito é
   "respondeu" ou "lançou SQLException". Uma tabela de quinhentas assinaturas envelheceria
   em silêncio; esta reprova sozinha quando o JDBC ganha um método novo.

### Achados da execução de E a G

- **O gate de alocação do cliente existe e funciona.** `getThreadAllocatedBytes` mede
  bytes alocados por thread — métrica que não depende de GC — e o teto é 64 bytes por
  linha. Verificado quebrando o cursor de propósito: materializar a linha levou a conta a
  88 bytes e o gate reprovou.
- **O `AssinaturasTest` achou três defeitos reais na primeira execução**:
  `prepareStatement(null)` e `executeQuery(null)` estouravam `NullPointerException`, e os
  quatro métodos de citação de nome herdados da interface faziam o mesmo. Todos viraram
  `SQLException`, e a citação passou a seguir as regras do lexer deste servidor.
- **A falha do cursor precisava ser sticky.** Quem engolisse a exceção e chamasse
  `next()` de novo recebia `false` e concluía que o resultado tinha terminado bem —
  apresentando como completo um resultado cortado no meio.
- **O span de um TEXTO está escapado**, e a conversão numérica sobre ele precisa
  desescapar antes; sem isso um inteiro escrito com escape unicode saía como "fora do
  intervalo de int64".
- **`closeOnCompletion` precisou de um guard**: trocar de resultado numa execução nova
  passa pelo mesmo `close()` que o usuário chamaria, e sem ele o Statement se fechava
  sozinho na segunda consulta.
- **Quatro nulos do fio NÃO são NULL** e não são distinguíveis no cliente: double
  não-finito, `time` fora do dia, `json` vazio e valor fisicamente malformado. Ficam
  registrados no javadoc do ResultSet — o lugar de procurar é o dado, não o driver.

### Ponto de partida da Fase H

O driver está completo e testado contra o servidor HTTP do próprio JDK
(`com.sun.net.httpserver`), com corpo de requisição conferido byte a byte. Falta prová-lo
contra o BINÁRIO real: `scripts/certgen`, `scripts/jdbc-it.sh`, as classes `*IT` com o
guard contra SKIP silencioso (`NISHIDB_IT_REQUIRED=1`), o job `jdbc` no `ci.yml` com as
extensões de `internal/gates/ci_test.go`, e a prova de heap com `-Xmx64m` sobre 100k
linhas. Tudo já está desenhado na seção "Fase H — Integração e CI" acima; nada dela foi
contradito pela execução de E a G.

---

## Estado em 2026-08-24 — Fases A a D concluídas

Branch `feature/driver-jdbc`, 14 commits sobre `main`. **Tudo verde**: `go test ./...`,
`go test -race ./...`, `gofmt -l .` vazio, `make test-jdbc-unit` (70 testes Java) e a
contagem de lint **igualada em 388** (o arquivo `.golangci-baseline` foi abaixado de 389
para 388 no commit do `--param`, porque a mensagem do `NArg` foi reescrita e código
reescrito segue a disciplina `_, _ =`).

| Fase | Estado | Onde |
|---|---|---|
| A (D0, lado Go) | **completa**, 9 tasks | descritor de tipo, nullable, Saude no protocolo, 405 uniforme, UNSUPPORTED_CLAUSE, `--param`, flush do schema |
| B (módulo Maven) | **completa** | `clients/jdbc/`, wrapper only-script pinado, gates em `internal/gates/jdbc_test.go` |
| C (JSON próprio) | **completa** | `…/jdbc/json/`: scanner com spans, DOM para documentos pequenos, escritor |
| D (transporte + leitor + SQLState) | **completa** | `…/jdbc/wire/`: `Codigos`, `SqlStates`, `Molduras`, `LeitorNdjson`, `Transporte` |
| **E a I** | **a fazer** | ResultSet/conversões, PreparedStatement, DatabaseMetaData, integração+CI, docs |

### Achados durante a execução, que NÃO estavam no plano original

Os três mudam decisões das fases seguintes.

1. **As duas rotas de metadados nomeiam a mesma coluna de formas diferentes, por decisão.**
   A linha de schema de `/v1/query` traz o nome de **saída** (o alias, porque é ele que é
   chave do objeto JSON de cada row); `/v1/metadata` traz o nome do **campo** e o alias num
   campo à parte, porque descreve schema e não projeção. Para o campo com alias `estado`,
   `getColumns()` vai dizer `status` e o `ResultSetMetaData` de `SELECT *` vai dizer
   `estado`. **Casar os dois por igualdade de nome não funciona** — é o que quem
   implementar `findColumn` e o `DatabaseMetaData` (Fase G) precisa saber. Está registrado
   em `TestMetadataPublicaOsMesmosNumerosQueALinhaDeSchema`.
   Regra completa: projeção explícita usa o que o usuário escreveu; `*` usa o alias
   (`TestONomeDoMetadadoEhODeSaida`).

2. **O RUNBOOK ficou desatualizado.** O `planning/m5-plano-ciclo.md:249` manda configurar
   `http.nonProxyHosts` da JVM para o JDBC. Com o `NO_PROXY` fixo do `Transporte` isso é
   desnecessário, e **quem seguir a instrução vai depurar o proxy errado**. Corrigir na
   Fase I.

3. **O pré-processador do Java resolve escapes unicode antes do compilador**, inclusive
   dentro de comentários e de literais de String. Escrever a sequência literal num teste
   faz o compilador convertê-la no caractere, e o teste passa a exercitar o texto já
   desescapado — **verificando o oposto do que afirma, sem que nada acuse**. A convenção
   adotada é montar a barra em runtime (`String.valueOf((char) 92)`), e há um comentário
   explicando isso em `JsonScannerTest.B`.

### Correções ao próprio plano, verificadas no código

- **`_writtenAt` é NULLABLE** (o plano mandava verificar em vez de deduzir, e a resposta
  foi essa): ele só existe quando o dataset liga `expireAfterWrite`, e as rows gravadas
  antes disso não o têm.
- **NULL vem como `null` explícito** no NDJSON, não como campo ausente — e o oráculo já
  existia (`TestNullExplicito`), citando o driver JDBC na justificativa. Logo o número de
  chaves da row é igual ao de colunas do schema, **na mesma ordem**, e o casamento do
  cursor pode ser posicional.
- **Coluna `json` sai inline e verbatim**, não como string.
- O `displaySize` de `ipv6` é **39**, e não os 45 do manual — medido contra o encoder.
- O teste `TestMetricsComMetodoErradoNaoConsultaOBroker`, previsto no plano, **não foi
  escrito**: o efeito é latência e chamada de rede, e não há oráculo determinístico barato
  (um teste de tempo contra broker morto passaria por acidente, já que o connect falha em
  microssegundos). A ordem correta está fixada por comentário no handler.

### Ponto de partida da Fase E

O `LeitorNdjson` entrega a linha corrente como span (`buffer()`, `inicio()`,
`fimDaLinha()`), e o `JsonScanner` percorre esse span sem alocar
(`abreObjeto`/`proximoCampo`/`tipo`/`valorIni`/`valorFim`). O `StreamCursor` da Fase E se
apoia nos dois: casamento **posicional** row↔schema, avançando o ponteiro do schema até o
nome bater, e recusando alto uma chave que não casa nenhuma coluna restante.
