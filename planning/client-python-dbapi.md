# Client Python do NishiDB — PEP 249 (DB-API 2.0)

## Context

O NishiDB já tem um cliente de primeira classe para o mundo Java: o driver JDBC de
`clients/jdbc/`, entregue pela Fase 7 e hoje em `main`. O que falta é o equivalente para
Python — hoje quem quer consultar o NishiDB de Python tem duas saídas ruins: chamar o
binário `nishidb query --server` por `subprocess` e parsear NDJSON na mão, ou reimplementar
o protocolo do zero em cada script.

O trabalho é **muito mais barato do que foi o JDBC**, e por uma razão concreta: a Fase 7
não foi só escrever Java. Ela foi **fechar o contrato de wire** — o `JDBCDescriptor` com
`precision`/`scale`/`displaySize`, o `nullable` no executor, o `UNSUPPORTED_CLAUSE`, o
`--param`, o flush depois da linha de schema. Tudo isso já está no servidor. O client
Python entra num contrato **maduro, congelado e cobrado por gates dos dois lados**.

O equivalente Python do JDBC é a **PEP 249 (DB-API 2.0)**: é ela que faz `pandas.read_sql`,
SQLAlchemy, Superset e Metabase funcionarem, do mesmo jeito que o JDBC destrava DBeaver e
IntelliJ. E há um encaixe feliz: a **linha de schema de `/v1/query` foi desenhada para o
`ResultSetMetaData` do JDBC e mapeia 1:1 na tupla de 7 campos de `Cursor.description`** —
`name`, `type_code`, `display_size`, `internal_size`, `precision`, `scale`, `null_ok`.
Não falta um número.

Resultado esperado: `pip install nishidb`, e então

```python
import nishidb

with nishidb.connect("nishidb://db01:9142/radius_sessions", user="leitor") as cx:
    cur = cx.cursor()
    cur.execute("SELECT user_name, nas_ip FROM radius_sessions WHERE status = ?", ("Stop",))
    for linha in cur:
        ...
```

---

## Decisões fechadas

| # | Decisão | Justificativa |
|---|---|---|
| **D1** | Núcleo **PEP 249** + camada pythônica por cima, no mesmo pacote | O DB-API é o que dá o ecossistema; a camada alta (`Row` que é mapping e sequence, iteração direta, `client.datasets()`) é o que faz o dia a dia não doer. Uma só implementação de streaming embaixo das duas |
| **D2** | **Zero dependência de runtime — só a stdlib** | É a doutrina do produto em três instâncias já: o binário único (§97), o JDBC com parser JSON próprio (gate `TestOPomDoDriverNaoTemDependenciaDeRuntime`), e o painel `scripts/nishidb-stats.py`. Um client de banco que arrasta `requests` não é instalável no host que se está diagnosticando |
| **D3** | Transporte é **`http.client`**, não `urllib.request` | Três ganhos concretos: (a) `http.client` **não lê `http_proxy` do ambiente**, então a armadilha do proxy corporativo do piloto ([piloto-events.md:142](docs/piloto-events.md:142)) some por construção, sem o `ProxyHandler({})` que o painel precisou; (b) keep-alive numa conexão persistente por `Connection` DB-API, que é o modelo certo — uma conexão executa uma consulta por vez; (c) HTTP/1.1 por padrão, a mesma escolha do `Transporte.java`; (d) `response.readline()` dá o stream NDJSON sem materializar |
| **D4** | O **dataset sai do `FROM`**, com o do DSN como fallback | Idêntico à D2 do JDBC, pela mesma razão: [binder.go:157](internal/query/binder/binder.go:157) recusa `dataset` ≠ `FROM`. Sem o extrator, uma conexão só consulta um dataset. O palpite errado nunca devolve dado de outra tabela — o servidor recusa no bind. O mesmo scanner conta os `?` |
| **D5** | A conversão é dirigida pelo **token JSON**; o tipo declarado só entra onde o token é ambíguo | Não é invenção: é a lição nº 1 da execução das Fases E-G do JDBC, paga com bug. Texto que é instante, texto que é hora do dia e texto que é base64 têm a mesma cara no JSON. Duas exceções nomeadas: coluna `bytes` (o binário é separado pelo TIPO) e coluna `OTHER` (segue o token, porque metadata e dado podem discordar) |
| **D6** | `timestamp`/`datetime` viram **`datetime` AWARE em UTC**; `time` vira **`datetime.time` naive** | O primeiro **é um instante** — `tzinfo=timezone.utc` preserva-o exatamente e deixa o usuário renderizar no fuso que quiser. O segundo **não é** instante: é hora do dia, e carimbar fuso nele seria inventar dado. É a mesma divisão que o JDBC pagou caro para descobrir (risco R9 do plano da Fase 7) |
| **D7** | `ipv4`/`ipv6`/prefixos viram **`str`**, não `ipaddress.*` | O §73 os mapeia para VARCHAR e o JDBC entrega String. Enriquecer o tipo em UM client faz os dois clients discordarem sobre a mesma coluna. `ipaddress.ip_address(v)` é uma chamada de distância, e fica documentado |
| **D8** | Coluna `json` vira **objeto Python** (`dict`/`list`), com o texto cru alcançável pela camada baixa | É o que um usuário de Python espera, e o DB-API não tem conceito de "JSON verbatim". A perda (double dentro do json passa por `float`) é a mesma de qualquer `json.loads` e fica no docstring; quem precisa dos bytes exatos usa o passthrough NDJSON da camada pythônica |
| **D9** | Doutrinas de cliente **portadas literalmente** de [credential.go](internal/cli/credential.go): sem proxy, **todo 3xx é erro**, **nunca** `InsecureSkipVerify`/`verify=False` — nem por parâmetro | O redirect same-host preserva o `Authorization` nas bibliotecas padrão, e o NishiDB não redireciona: um 3xx só pode ser configuração errada no meio do caminho. Um parâmetro `insecure` opcional vira o modo permanente de operação em três semanas |
| **D10** | Senha **nunca** é argumento de linha de comando; entra por `NISHIDB_PASSWORD` ou arquivo | §95, e o mesmo desenho do CLI: flag aparece em `ps aux` para qualquer usuário da máquina. Como parâmetro de `connect()` em código Python é legítimo (não vaza no `ps`), e é isso que o DB-API espera |
| **D11** | Testes em **`unittest` da stdlib**; `ruff` e `mypy` pinados, rodando só no CI | `make test-python` continua rodando com zero `pip install` em qualquer máquina com `python3` — que é a mesma promessa do painel e do binário único, e é o que o job `test` do CI já faz hoje sem etapa de instalação. Lint e tipagem ganham job próprio, com versão fixada na disciplina do `golangci-lint@v2.12.2` |
| **D12** | API pública em **inglês** (a PEP 249 manda), módulos internos em **português** | É exatamente o que o JDBC faz: interfaces `java.sql` em inglês, e `Transporte`, `LeitorNdjson`, `Molduras`, `Codigos` em português por dentro |
| **D13** | Versão **própria**, separada da do engine, com gate | O JDBC já abre o precedente (`0.1.0-SNAPSHOT` no pom, cobrado por `TestAVersaoAnunciadaPeloDriverEhADoArtefato`). O client não muda de versão porque o engine mudou |
| **D14** | Escopo: **só leitura** — `/v1/query`, `/v1/metadata`, `/v1/stats`, `/v1/health` | Decisão do usuário. `/v1/compact` e `/v1/checkpoint` (papel ADMIN) ficam de fora; são dois POSTs triviais sobre o mesmo transporte e entram depois sem redesenho |

---

## O que o client herda de graça (e por que isso muda o custo)

Verificado no código, não suposto:

- **`Cursor.description` sai pronto da linha de schema.** `protocol.Column`
  ([protocol.go:126](internal/protocol/protocol.go:126)) tem `name`, `type`, `jdbc`,
  `nullable`, `precision`, `scale`, `displaySize` — os sete campos da tupla do DB-API, sem
  faltar nem sobrar. Nenhuma tabela de tipos nova no client, e um tipo novo no servidor
  **não exige release do pacote**.
- **NULL vem como `null` explícito**, não como campo ausente. Logo o número de chaves da row
  é igual ao de colunas do schema, **na mesma ordem**, e o casamento pode ser posicional.
- **Chave duplicada é legal** (`SELECT a, a` produz `{"a":1,"a":1}`). O casamento tem de ser
  posicional/ordenado, **nunca por dict de nome** — um `json.loads` ingênuo perde a coluna
  repetida em silêncio.
- **Os três desfechos do stream já estão separados** em `protocol.Reader`
  ([stream.go:45](internal/protocol/stream.go:45)): schema, fecho de sucesso, fecho de erro,
  e **stream que acaba sem fecho é falha**. É o contrato a portar, linha a linha.
- **A tabela código→exceção tem oráculo do lado Go.** `internal/gates/jdbc_test.go` já parseia
  `protocol.go` com `go/ast` e cobra que toda constante `Cod*` tenha SQLState. O gate Python
  é o mesmo teste com outro arquivo de destino.
- **O harness de integração já existe e é reusável**: `scripts/certgen` (par EC P-256 efêmero),
  o modelo de `scripts/jdbc-it.sh` (porta sorteada, prontidão pela linha de log `query.pronta`,
  nunca `sleep`), e o corpus [testdata/todos_os_tipos.yaml](testdata/todos_os_tipos.yaml) com
  um campo de cada tipo do wire.

E cinco fatos do servidor que o client **tem de respeitar**, todos verificados:

- **O servidor usa `DisallowUnknownFields`** ([query.go:231](internal/server/query.go:231)).
  Um campo a mais no corpo de `/v1/query` é **400 `BAD_REQUEST`**, não um campo ignorado. O
  serializador do client monta exatamente os cinco campos da `Request` e nada mais.
- **`maxRows <= 0` é 400**, e `maxRows` acima do teto do dataset é 400 `MAX_ROWS_ABOVE_LIMIT`:
  o servidor **nunca apara em silêncio**. É por isso que a regra é omitir o campo, não mandar 0.
- **Toda falha de autenticação paga 100 ms fixos** de atraso
  ([auth.go:253](internal/server/auth.go:253)). O Basic tem de ser
  **preemptivo** — desafio-resposta pagaria esse atraso em toda requisição.
- **`/v1/health` responde 503 quando `status != "UP"`**, com corpo válido. O client **não pode**
  tratar isso como erro de transporte: é a resposta, e o estado está no corpo.
- **O dialeto cresceu desde a Fase 7**: `ORDER BY` (até 16 chaves, `NULLS FIRST|LAST`), `LIMIT`,
  `OFFSET` e `[NOT] LIKE` **existem hoje**. `UNSUPPORTED_CLAUSE` cobre agora `GROUP BY`, `JOIN`,
  `HAVING`, `DISTINCT` e `UNION`. Qualquer texto do client que liste o que não existe tem de sair
  daqui, não do plano do JDBC — que está desatualizado nesse ponto.

---

## Tabela de conversão — tipo do wire → tipo Python

| Tipo | `jdbc` | Python | Nota |
|---|---|---|---|
| `int64` | BIGINT | `int` | Sem perda: o `int` do Python é arbitrário. **Parsear do span, nunca via `float`** |
| `double` | DOUBLE | `float` | `float()` sobre o texto ASCII já validado; ambos os lados arredondam por IEEE-754 |
| `boolean` | BOOLEAN | `bool` | |
| `string` | VARCHAR | `str` | |
| `timestamp`, `datetime` | TIMESTAMP | `datetime` **aware, UTC** | D6. Os dois saem byte a byte iguais do encoder |
| `time` | TIME | `datetime.time` **naive** | D6. Não é instante |
| `bytes` | VARBINARY | `bytes` | `base64.b64decode(validate=True)` — lixo à direita vira `DataError`, nunca array truncado |
| `json` | VARCHAR | `dict`/`list`/escalar | D8. Sai **inline e verbatim** no NDJSON, não como string |
| `ipv4`, `ipv6`, `ipv4_prefix`, `ipv6_prefix` | VARCHAR | `str` | D7 |
| `unknown` | OTHER | segue o token | Coluna `auto` que nenhum evento fixou. É a única cujo metadata pode discordar do dado |
| `null` (valor) | — | `None` | |

**Quatro nulos do fio que NÃO são NULL** e não são distinguíveis no cliente (achado registrado
na execução do JDBC): double não-finito, `time` fora do dia, `json` vazio e valor fisicamente
malformado. Vão para o docstring do `Cursor` — o lugar de procurar é o dado, não o client.

## Tabela código do protocolo → exceção PEP 249

| Código | Exceção |
|---|---|
| `SYNTAX`, `BIND`, `RESERVED_COLUMN`, `UNKNOWN_DATASET` | `ProgrammingError` |
| `UNSUPPORTED_CLAUSE` | `NotSupportedError` |
| `BAD_PARAMETER`, `MAX_ROWS_ABOVE_LIMIT` | `DataError` |
| `TIMEOUT`, `TOO_MANY_QUERIES`, `TOO_MANY_LONG_QUERIES`, `COMPACTION_BUSY`, `REQUEST_TOO_LARGE`, `ROW_TOO_LARGE`, `WRITER_FAILED`, `UNAUTHENTICATED`, `FORBIDDEN` | `OperationalError` |
| `DATASET_CORRUPT`, `INTERNAL` | `InternalError` |
| `BAD_REQUEST`, `METHOD_NOT_ALLOWED`, `UNSUPPORTED_MEDIA_TYPE` | `InterfaceError` |
| `MAX_ROWS_EXCEEDED` | **`warnings.warn(Warning)`, nunca exceção** |
| conexão/DNS/TLS, stream sem fecho, linha ilegível | `OperationalError` |
| **código desconhecido** | `DatabaseError` **com o código cru na mensagem** — jamais degradar para `INTERNAL`, que apagaria a novidade |

`MAX_ROWS_EXCEEDED` vira aviso pela razão que o JDBC já fixou: o caso de fronteira (resultado
com exatamente `maxRows` linhas) é falso positivo conhecido, e distingui-lo no servidor
custaria varrer o dataset inteiro — exatamente o que o teto existe para evitar.

Toda exceção carrega `.code` (o código do wire), `.line`/`.column` quando existem (SYNTAX,
BIND e UNSUPPORTED_CLAUSE), `.rows_delivered` quando a falha veio no fecho depois de N linhas,
e **`.transient` + `.retry_after`**. **Jamais devolver stream vazio para um fecho de erro** —
apresentaria resultado cortado como completo.

O `.transient` não é enfeite e não é opinião do client: é a informação que o JDBC codifica
escolhendo entre `SQLTransientException` e `SQLNonTransientException`, e que a PEP 249 **não
tem onde guardar** — sua hierarquia não distingue "repita" de "repetir não adianta". Sem o
campo, a informação se perderia na tradução e todo usuário de Python reinventaria a tabela.
São transientes exatamente três: `TOO_MANY_QUERIES` (503, `Retry-After: 1`),
`TOO_MANY_LONG_QUERIES` (503, **`Retry-After: 30`** — as vagas de consulta longa podem estar
ocupadas por minutos, e repetir em um segundo quase certamente falha de novo) e
`COMPACTION_BUSY` (409). **O client não tem retry embutido** — mesma política do JDBC: quem
decide repetir é a aplicação, e o client só entrega o que precisa para decidir.

---

## Fase A — Módulo, empacotamento e gates

```
clients/python/
├── pyproject.toml            name=nishidb (livre no PyPI, verificado), requires-python>=3.10
├── README.md                 o que o PyPI mostra
├── src/nishidb/
│   ├── __init__.py           connect, apilevel/threadsafety/paramstyle, exceções, type objects
│   ├── py.typed
│   └── (módulos das fases B-G)
└── tests/
```

`apilevel = "2.0"`, `threadsafety = 1` (o módulo é compartilhável; a conexão não — é uma
conexão HTTP), `paramstyle = "qmark"` (o NishiDB usa `?`).

`src` layout e `pyproject.toml`, sem `setup.py`. **Nenhuma entrada em `[project].dependencies`** —
só `[project.optional-dependencies].dev` com `ruff` e `mypy` pinados.

**Makefile:** `test-python` passa a rodar o painel **e** a suíte do client (as duas sem
instalar nada); novo `test-python-it` chama `scripts/python-it.sh`. Ausência de `python3`
**falha alto e nomeia**, nunca vira SKIP — a doutrina de [Makefile:88](Makefile:88) e do
`exige-java`.

**`.gitignore`:** `clients/python/dist/`, `.mypy_cache/`, `__pycache__/` escopados — nunca
soltos. E acrescentar os diretórios à lista que `testesOfRepositorio`
([evidence_test.go:55](internal/cli/evidence_test.go:55)) pula.

**`internal/gates/python_test.go`** — novo, no molde exato de
[jdbc_test.go](internal/gates/jdbc_test.go), lendo os arquivos **como dado**:
1. `pyproject.toml` não declara nenhuma dependência de runtime (o gêmeo de
   `TestOPomDoDriverNaoTemDependenciaDeRuntime`);
2. a versão do `pyproject.toml` bate com `nishidb.__version__` (gêmeo de
   `TestAVersaoAnunciadaPeloDriverEhADoArtefato`);
3. **paridade Go↔Python dos códigos**: parseia `internal/protocol/protocol.go` com `go/ast`,
   coleta toda constante `Cod*` de valor string, e reprova se algum literal não aparecer em
   `erros.py`. **Código novo em Go sem exceção Python reprova no CI Go, antes de qualquer
   intérprete Python.**

> **Armadilha do ratchet de lint.** Cada `.go` novo (o `python_test.go`) muda a contagem, e o
> `.golangci-baseline` reprova **nos dois sentidos** — abaixo da baseline também falha. Re-medir
> e atualizar o arquivo **no mesmo PR**, e não "aproveitar" para consertar apontamento vizinho.

## Fase B — Transporte

`transporte.py` é o **único** ponto que monta uma requisição HTTP. Doutrinas literais de
[credential.go](internal/cli/credential.go):

- `http.client.HTTPConnection`/`HTTPSConnection`, **uma por `Connection` DB-API**, keep-alive.
  Sem proxy por construção (D3).
- **Todo 3xx é erro**, mesmo same-host. Nunca seguir com o `Authorization` embarcado.
- TLS por `ssl.create_default_context(cafile=...)` com `minimum_version = TLSv1_2`.
  **`check_hostname` e `verify_mode` nunca são afrouxados**, e não existe parâmetro para isso.
- `Authorization: Basic` **preemptivo**, montado à mão — nunca o handler de auth do `urllib`,
  que gasta um round-trip de 401 antes de mandar a credencial. Não é micro-otimização: **toda
  falha de autenticação custa 100 ms fixos no servidor**, e desafio-resposta pagaria isso em
  toda requisição.
- Corpo de erro: decodifica `protocol.ErrorBody`; quando não é JSON, **espelhar
  `dicaDeEsquema`** ([query_remote.go:110](internal/cli/query_remote.go:110)) — porta TLS
  respondendo a texto puro devolve `Client sent an HTTP request to an HTTPS server`, e a
  mensagem tem de sugerir o `nishidbs://`/`tls=true`. Sem isso o usuário vê "400 sem corpo
  legível" e culpa o servidor.
- **Três relógios distintos**, e confundi-los mata stream legítimo: `connect_timeout` (só o
  handshake, default 10 s); **nenhum read timeout por default** no stream de `/v1/query` — o
  teto é o `query.timeout` do dataset, aplicado pelo servidor; e `timeout_ms` por consulta,
  que vai no wire **clampado com `min` contra `DatasetMeta.TimeoutMs`** lido no connect
  (o servidor recusa acima do teto).

`dsn.py`: `nishidb://host[:9142]/dataset[?tls=true&tls_ca=…]`, espelhando `NishiUrl.java`:
porta default **9142**, IPv6 entre colchetes, **exatamente um** segmento de path (não há
catálogo nem schema), valor inválido numa opção nossa é **recusado** (nunca "falso por
default"), opção desconhecida é ignorada. `tls_ca` **exige** `tls=true`.

Duas recusas, e a segunda é uma **correção deliberada de uma brecha do JDBC**:
1. **userinfo embutido** (`nishidb://user:senha@host`) — credencial em URL é o que o §95 evita;
   o JDBC já recusa.
2. **`?password=` na query string** — o JDBC *documenta* que não aceita senha na URL, mas o
   `escolhe()` faz fallback para o mapa da URL e na prática ela funciona. Aqui a recusa é
   explícita e testada. A senha entra por argumento de `connect()` (que não vaza no `ps`),
   por `NISHIDB_PASSWORD` ou por arquivo — nunca por texto que acaba em log de aplicação,
   histórico de shell ou string de conexão versionada.

Precedência: argumentos de `connect()` > DSN, explícita e testada (o espelho de
`Properties > URL` do JDBC).

## Fase C — Leitor NDJSON

`leitor.py` porta `protocol.Reader` com os **mesmos três desfechos separados**: schema, fecho
de sucesso, fecho de erro — mais falha de transporte, que é distinta. Regras que o Go já fixa
e o Python tem de repetir:

- A primeira linha **tem** de ser o schema; qualquer outra coisa é protocolo violado.
- Segunda linha de schema no mesmo stream é erro.
- **Stream que acaba sem fecho é falha**, com a mesma mensagem — é o que faz uma conexão
  cortada no meio deixar de ser indistinguível de um resultado que terminou.
- Moldura desconhecida é erro, não linha de dados.
- Discriminação por **prefixo de bytes** (`{"nishidb":`), como o Go, antes de qualquer parse.

Consumo por `readline()` sobre a resposta, **uma linha por vez**. `readlines()`, `read()` e
qualquer materialização do corpo inteiro são violação direta do §128 — e são o bug que
aparece como "o script comeu 8 GB de RAM".

**Casamento row↔schema posicional**, avançando o ponteiro do schema até o nome bater; chave
que não casa nenhuma coluna restante **recusa alto** (protocolo violado), nunca vira linha
vazia. É o que absorve `SELECT a, a`.

## Fase D — Scanner de SQL

`sql.py`: extrai o `FROM` e conta os `?` **fora de literais e de identificadores citados** —
o mesmo par de responsabilidades do `SqlScanner.java` (D4), portado com os mesmos casos de teste.

**É um extrator de palpite, e nunca levanta exceção**: quando não sabe, devolve `None` e o
dataset do DSN vale. Regras do lexer que ele copia: literal `'…'` com aspa duplicada como
escape, identificador `"…"` idem, identificador bare ASCII `[_A-Za-z][_A-Za-z0-9]*`,
comparação de `FROM` **ASCII-only** (não `casefold`/`lower` com folding Unicode — é o caso do
Kelvin sign que o lexer do servidor já registra), sem comentários (a linguagem não tem), e
nome qualificado (`_meta.fields`) **abandona o palpite**. Um `"from"` entre aspas não é a
keyword; um `?` dentro de string não é parâmetro.

## Fase E — Tipos e conversão

`tipos.py`: a tabela de conversão acima, **dirigida pelo token** (D5). `int` parseado dos
dígitos, nunca via `float`. Temporais pela D6. `bytes` com `validate=True`, e **só em coluna
`VARBINARY`** — decodificar base64 de uma coluna que não é binária devolveria lixo plausível
em vez de erro (foi decisão explícita do JDBC, contra a convenção do próprio JDBC).

Detalhes do encoder que o parser tem de aguentar, verificados em
[ndjson.go](internal/query/executor/ndjson.go): double na grafia `'g'` do Go, que emite
`1e-07` (e não o `1e-7` do `encoding/json`); NaN e Inf saem como `null`; timestamp **sempre em
UTC, sempre com 3 dígitos de fração**, ano com ≥4 dígitos; `time` **sem `Z`**; prefixos **sem
`Masked()`**, logo `10.0.0.1/24` sobrevive com os host bits; `bytes` em base64 **StdEncoding
com padding**; e `<`, `>`, `&` **não são escapados** dentro de string. Doutrina de teste:
**nunca comparar double como texto** — o oráculo é round-trip de bits.

Teste obrigatório da matriz inteira **com `TZ=Asia/Tokyo`**, além de UTC — é o teste que pegou
os dois construtores errados no JDBC (risco R9), e o Python tem armadilhas equivalentes
(`datetime.fromtimestamp` sem `tz` usa o fuso local e é o erro óbvio a evitar).

## Fase F — DB-API 2.0

`conexao.py` e `cursor.py`.

- **`connect()` faz um `GET /v1/metadata?dataset=X` antes de qualquer consulta**, como o JDBC:
  é dali que saem `maxRows` e `timeoutMs` do dataset. Efeito colateral desejado — dataset
  inexistente falha ao **abrir a conexão** (`ProgrammingError`), não na primeira query, que é
  onde o usuário já perdeu o contexto do erro.
- `Cursor.description`: as 7-tuplas, direto da linha de schema.
- `Cursor.rowcount`: **-1 enquanto o stream corre**; o número real depois do fecho, que é
  quando ele passa a ser conhecido. PEP 249 autoriza -1 para "não determinável".
- `fetchone`/`fetchmany`/`__iter__` consomem **incrementalmente**; só `fetchall` materializa —
  que é o que o método significa.
- `execute(sql, params)` com `paramstyle="qmark"`; índice fora de faixa recusado **no cliente**,
  com número **1-based** (como o binder). Os 4 tipos de wire (`string`/`int64`/`double`/`boolean`).
- **`datetime`/`time` como parâmetro viajam como `int64`**, não como string. Verificado em
  [coerce.go:240](internal/query/binder/coerce.go:240): uma string contra coluna `datetime` é
  interpretada no `format:` declarado do campo, resolvido no fuso do **host do servidor** — um
  parâmetro que enviasse ISO-8601 funcionaria para `timestamp` e falharia para `datetime`.
  Epoch millis (e millis do dia para `time`) é o único caminho uniforme.
- **`None` como parâmetro → `NotSupportedError` nomeando `IS NULL`**, com teste de que **não**
  devolve resultado vazio em silêncio. Não existe sintaxe de parâmetro nulo no wire.
- `max_rows`: `n <= teto` → envia; `n >= teto` ou `n == 0` → **omite o campo** (o teto do
  dataset já satisfaz "no máximo n"). O teto vem do `/v1/metadata` buscado no connect.
- `commit()` é **no-op** (a PEP manda implementar sem efeito quando não há transação);
  `rollback()`, `executemany()` e as rotas de escrita lançam `NotSupportedError`. É a errata D3
  do JDBC traduzida: **responder "não" no vocabulário da API** onde a API prevê um "não", e
  lançar só onde é capacidade de execução.
- `Connection.close()` fecha cursores abertos → conexão HTTP, nessa ordem. Context manager nos
  dois. Uso depois do close → `InterfaceError`.
- **A falha do cursor é sticky**: quem engolir a exceção e chamar `fetchone()` de novo **não**
  pode receber `None` e concluir que o resultado terminou bem. Foi bug real no JDBC.
- Busca de coluna por nome (o `Row["nome"]` da Fase G): **exato primeiro, depois sem caixa**;
  ambiguidade no segundo passo é erro, não a primeira que aparecer. Os identificadores do
  NishiDB são case-sensitive, e é isso que resolve a distância entre o servidor e o hábito
  de Python.

## Fase G — Camada pythônica

`cliente.py`: `Client` sobre a mesma `Connection`.
- `query(sql, *params)` devolve iterador de `Row` — mapping **e** sequence (`linha["nome"]`,
  `linha[0]`, `dict(linha)`), com o casamento posicional por baixo.
- `datasets()`, `metadata(nome)`, `stats()`, `health()` sobre as três rotas GET, devolvendo
  dataclasses tipadas que espelham `protocolo.DatasetMeta`, `Stats` e `Health`.
  **`health()` trata 503 como resposta, não como erro** — é o corpo que diz o estado.
- `describe(dataset)` como açúcar sobre `SELECT … FROM _meta.fields`, que traz o que o
  `/v1/metadata` não tem: `indexed` (o índice **físico**, contra o `index` **declarado**),
  `parent`, `occurrences`, `typeMismatches` e `coercions`.
- **Armadilha a documentar, não a esconder:** as duas rotas nomeiam a mesma coluna de formas
  diferentes, por decisão. A linha de schema de `/v1/query` traz o nome de **saída** (o alias,
  porque é ele que é chave do objeto JSON de cada row); `/v1/metadata` traz o nome do **campo**,
  com o alias num campo à parte. Para um campo `status` com alias `estado`, `metadata()` diz
  `status` e o `description` de `SELECT *` diz `estado`. **Casar os dois por igualdade de nome
  não funciona** — foi achado na execução das Fases E-G do JDBC.
- `query_raw(sql, *params)` → iterador das **linhas NDJSON cruas**, o passthrough que preserva
  a grafia exata do servidor (a saída do `nishidb query`, byte a byte) e é o escape da D8.

## Fase H — Integração contra o binário real, e CI

`scripts/python-it.sh`, `set -euo pipefail` + `trap` de limpeza, **clonado de
`scripts/jdbc-it.sh`** — mesma estrutura, seis passos:
1. Pré-voo: binário Go e `python3`; ausência **falha alto e nomeia**, nunca SKIP.
2. Tudo num `mktemp -d`, removido no trap. Nada escrito na árvore.
3. `go run ./scripts/certgen` (o mesmo já usado pelo JDBC — nenhuma ferramenta nova).
4. Corpus a partir de `testdata/todos_os_tipos.yaml`: linhas de borda (int64 no limite, double
   grafia `'g'`, 4-in-6, prefixo com host bits, json aninhado, dois formatos temporais) + volume.
5. Servidor com TLS + Basic, porta sorteada, senha gerada em runtime em arquivo 0600 entregue
   por **env**, prontidão pela linha de log `query.pronta` — **nada de `sleep`**.
6. Suíte de integração com `NISHIDB_IT_URL`, `NISHIDB_IT_USER`, `NISHIDB_IT_PASSWORD`,
   `NISHIDB_IT_REQUIRED=1`.

**Contra o SKIP silencioso** (o débito 53 traduzido para Python): os testes de integração se
auto-pulam quando `NISHIDB_IT_URL` está ausente, **e** existe um teste que **falha** quando
`NISHIDB_IT_REQUIRED=1` e a URL não está. Harness quebrado reprova; não passa verde com zero
testes rodados.

**Prova do §128 no cliente** — a que substitui o `-Xmx64m` do surefire: `SELECT * FROM
todos_os_tipos` sobre >100 k linhas com **`tracemalloc` afirmando pico limitado**, independente
do número de linhas. É o gate que reprova sozinho no dia em que alguém trocar `readline()` por
`read()`.

**TLS:** conecta com a CA; **e um teste de que sem a CA a conexão FALHA** — a prova de que não
há verificação desligada escondida.

**CI (`ci.yml`):** o job `test` continua chamando `make test-python` (o gate
`TestWorkflowDeCIRodaOTestePython` em [ci_test.go:125](internal/gates/ci_test.go:125) exige que
o passo não suma). Dois jobs novos: `python-it` (`setup-python` com versão **exata**, roda
`make test-python-it`) e `python-lint` (`ruff check` + `ruff format --check` + `mypy --strict`,
versões pinadas). Extensões a `internal/gates/ci_test.go` no estilo do arquivo: os jobs existem,
rodam os alvos do Makefile, e a `python-version` do workflow **bate com o `requires-python` do
`pyproject.toml`** — comparando as duas extrações, nunca um número escrito à mão no teste.

## Fase I — Documentação, aceite e PyPI

- Seção no `README.md`. **Armadilha:** `TestQuickstartDoREADMEExecuta`
  ([readme_test.go:74](internal/cli/readme_test.go:74)) **executa** os blocos ```` ```console ````
  e ```` ```yaml ```` **por índice**. A seção Python tem de vir **depois** do quickstart e usar
  ```` ```python ````/```` ```toml ```` — um ```` ```console ```` novo antes dele quebra o gate.
- `docs/aceite-client-python.md`, **acrescentado à lista de
  [evidence_test.go:30](internal/cli/evidence_test.go:30) no mesmo commit** — senão o
  documento nasce fora do único verificador que o projeto tem, com liberdade de citar teste
  inexistente. E **estender `testesOfRepositorio` para indexar `def test_*` de arquivos Python**,
  como já foi feito para os métodos Java, para que o aceite possa citar evidência Python com
  honestidade.
- `docs/architecture.md` e `docs/diagrams/c4_container.puml`: o client Python entra ao lado do
  driver JDBC como consumidor do protocolo §67-69.
- `docs/debitos.md`: registrar os limites novos (sem parâmetro nulo, sem escrita, sem
  transações, rotas ADMIN fora do escopo) e fechar a dívida de "só dá para consultar de Python
  por subprocess".
- Errata da spec: o §70-73 nomeia só o JDBC; acrescentar o client Python ao roadmap e à §98.
- **PyPI:** `release-python.yml` novo, disparado por `release: [published]`, **guardado para tags
  `python-v*`** (a versão do client é própria — D13), publicando por **Trusted Publishing (OIDC,
  `permissions: id-token: write`)**, sem token em secret. Gate: a tag bate com a versão do
  `pyproject.toml`.
  **Passo manual seu, antes do primeiro release:** criar o projeto `nishidb` no PyPI e cadastrar
  o publisher confiável (`nishisan-dev/nishidb`, workflow `release-python.yml`). O nome está
  livre — verificado.

---

## Verificação

1. `make test`, `make test-race`, `gofmt -l .` vazio.
2. **Contagem de lint bate `.golangci-baseline` exatamente** — ratchet bidirecional, e o
   `python_test.go` novo muda o número. Re-medir e atualizar no mesmo PR:
   ```bash
   golangci-lint run --max-issues-per-linter=0 --max-same-issues=0 --output.json.path=/tmp/l.json; jq '.Issues|length' /tmp/l.json
   ```
3. `make test-python` verde numa máquina com `python3` e **zero `pip install`**.
4. `make test-python-it` verde do zero em máquina limpa e no runner.
5. `ruff check`, `ruff format --check` e `mypy --strict` sem apontamento.
6. **Gate de paridade**: nenhuma constante `Cod*` de `protocol.go` sem exceção em `erros.py`.
7. **Gate de zero dependência**: `pyproject.toml` sem `[project].dependencies`.
8. **Gate de memória**: pico de `tracemalloc` sobre >100 k linhas independente do número de linhas.
9. Matriz de conversão passa em **UTC e em `Asia/Tokyo`**.
10. Sem a CA, a conexão TLS **falha**.
11. `pandas.read_sql(sql, conexao)` devolve DataFrame com os dtypes certos — a prova de que o
    DB-API está honesto de verdade, sem escrever uma linha de integração com pandas.
12. Contra o piloto (loopback): `SELECT` com parâmetro devolve linhas; `ORDER BY` inexistente
    devolve `NotSupportedError` com mensagem acionável; `SELECT` num dataset que não existe
    devolve `ProgrammingError`.

---

## Riscos

| # | Risco | Prob. | Mitigação |
|---|---|---|---|
| R1 | `json.loads(linha)` entra "porque é uma linha só" e **perde a coluna repetida** de `SELECT a, a`, além de materializar | **Alta** — é o caminho de menor esforço em Python | Casamento posicional desde o primeiro commit da Fase C, com teste de `SELECT a, a` |
| R2 | Materialização do corpo (`read()`, `readlines()`, `json.load`) mata o §128 no cliente | **Alta** | O gate de `tracemalloc` sobre 100 k linhas é o único jeito de provar; roda no CI |
| R3 | `datetime.fromtimestamp(ms/1000)` sem `tz` — desloca pelo fuso local e perde precisão no float | **Alta** — é o construtor óbvio | Matriz inteira sob `TZ=Asia/Tokyo`; construção por `timedelta` sobre a epoch em UTC, nunca por float |
| R4 | Alguém acrescenta `verify=False`/`insecure=True` "para testar contra o piloto" | Média | Não existe o parâmetro, e o teste de que a conexão falha sem a CA reprova quem o acrescentar |
| R5 | O ratchet de lint reprova por **queda**, porque alguém "limpou" um apontamento vizinho ao mexer nos gates | **Alta** — já mordeu neste repo | Código novo com a disciplina do arquivo, vizinhança intocada, `git diff --stat` antes do push |
| R6 | A seção Python entra no README **antes** do quickstart e quebra `TestQuickstartDoREADMEExecuta`, que casa blocos por índice | Média | Item explícito no "pronto quando" da Fase I; nenhum bloco ```` ```console ```` novo antes do quickstart |
| R7 | O client reimplementa a tabela de tipos em vez de ler `jdbc`/`precision`/`scale` da linha de schema, e diverge do servidor no primeiro tipo novo | Média | `description` é construída **só** da linha de schema; teste que afirma que um `jdbc` desconhecido não quebra o cursor |
| R8 | `int` grande parseado via `float` e chegando errado sem erro nenhum | Média | Parse dos dígitos do span; teste com `9223372036854775807` exato |
| R9 | A falha do cursor não é sticky e um resultado cortado é apresentado como completo | Média — foi bug real no JDBC | Teste que engole a exceção, chama `fetchone()` de novo e exige que ela volte |
| R10 | `docs/aceite-client-python.md` nasce fora de `evidence_test.go` | Média | Item explícito no "pronto quando" da Fase I |

---

## Arquivos críticos

**A ler (contrato, não mexer):**
- [internal/protocol/protocol.go](internal/protocol/protocol.go) — `Column` (126), molduras, códigos de erro (180-256)
- [internal/protocol/documents.go](internal/protocol/documents.go) — `/v1/metadata` e `/v1/stats`, `Health`
- [internal/protocol/stream.go](internal/protocol/stream.go) — **o contrato a portar**, linha a linha
- [internal/protocol/jdbc.go](internal/protocol/jdbc.go) — a tabela única de tipos
- [internal/cli/query_remote.go](internal/cli/query_remote.go) — o cliente Go de referência
- [internal/cli/credential.go](internal/cli/credential.go) — as doutrinas de cliente a herdar
- [scripts/nishidb-stats.py](scripts/nishidb-stats.py) — o precedente de Python neste repo
- `clients/jdbc/src/main/java/dev/nishisan/nishidb/jdbc/wire/` — `LeitorNdjson`, `Transporte`, `Codigos`, `SqlScanner`: o mesmo trabalho, já feito uma vez

**A criar:** `clients/python/**`, `scripts/python-it.sh`, `internal/gates/python_test.go`,
`.github/workflows/release-python.yml`, `docs/aceite-client-python.md`

**A editar:** `Makefile`, `.github/workflows/ci.yml`, `internal/gates/ci_test.go`, `README.md`,
`docs/architecture.md`, `docs/debitos.md`, `docs/diagrams/c4_container.puml`, `.gitignore`,
`internal/cli/evidence_test.go`, `.golangci-baseline`
