# nishidb — client Python

Client [DB-API 2.0 (PEP 249)](https://peps.python.org/pep-0249/) do
[NishiDB](https://github.com/nishisan-dev/nishidb). É o irmão do driver JDBC:
fala o mesmo protocolo, com as mesmas garantias.

**Zero dependência de runtime.** Só a stdlib — pela mesma razão que o engine é
um binário único: um client de banco tem de ser instalável no host em que se
está diagnosticando.

## Instalação

O pacote **ainda não está no PyPI**: por ora ele é instalado a partir do
repositório. Escolha o caminho conforme o que você está fazendo.

**Usando o client numa aplicação sua**, a partir de um clone:

```bash
pip install ./clients/python
```

**Sem clonar**, direto do git (o `subdirectory` é o que aponta para o pacote
dentro do monorepo):

```bash
pip install "git+https://github.com/nishisan-dev/nishidb@main#subdirectory=clients/python"
```

**Desenvolvendo o próprio client** — modo editável, para que a edição valha sem
reinstalar:

```bash
pip install -e ./clients/python
```

**Distribuindo internamente** um artefato fechado (é o que substitui o PyPI
hoje: o wheel é publicável num índice interno, num S3 ou anexado a uma release):

```bash
python -m build clients/python
# gera clients/python/dist/nishidb-0.1.0-py3-none-any.whl
pip install clients/python/dist/nishidb-0.1.0-py3-none-any.whl
```

**Sem instalar nada.** Como o client é stdlib pura, o diretório `src/` funciona
sozinho no `PYTHONPATH` — é o caminho para um host onde você não quer (ou não
pode) mexer no ambiente Python, e é o mesmo espírito de copiar o binário único
para a máquina:

```bash
PYTHONPATH=/caminho/para/nishidb/clients/python/src python3 meu_script.py
```

Nenhuma das formas acima baixa dependência nenhuma — não há o que resolver.

## Uso

```python
import nishidb

with nishidb.connect("nishidb://db01:9142/radius_sessions", user="leitor") as cx:
    cur = cx.cursor()
    cur.execute(
        "SELECT user_name, nas_ip FROM radius_sessions WHERE status = ? LIMIT 100",
        ("Stop",),
    )
    for linha in cur:
        print(linha["user_name"], linha["nas_ip"])
```

A senha nunca vai na string de conexão. Use `password=`, `password_file=` ou a
variável `NISHIDB_PASSWORD` — uma e só uma delas.

### Camada pythônica

```python
with nishidb.client("nishidb://db01:9142/radius_sessions", user="leitor") as c:
    for linha in c.query("SELECT * FROM radius_sessions LIMIT 10"):
        print(dict(linha))

    print(c.health().status)
    for ds in c.datasets():
        print(ds.name, ds.key, ds.max_rows)
```

Cada linha é ao mesmo tempo **sequência** (`linha[0]`, e é o que a PEP 249
exige) e **mapping** (`linha["coluna"]`, `dict(linha)`).

### pandas

O DB-API basta — não há integração a instalar:

```python
import pandas as pd

with nishidb.connect("nishidb://db01:9142/eventos") as cx:
    df = pd.read_sql("SELECT * FROM eventos LIMIT 1000", cx)
```

## String de conexão

```
nishidb://host[:9142]/dataset[?tls=true&tls_ca=/etc/nishidb/ca.pem]
```

| Opção | Default | O que faz |
|---|---|---|
| `tls` | `false` | `true` liga HTTPS. Só `true`/`false`; outro valor é erro, nunca "falso por default" |
| `tls_ca` | — | PEM da CA que valida o certificado do servidor. Exige `tls=true` |
| `user` | — | usuário do HTTP Basic |
| `connect_timeout_ms` | `10000` | prazo do **handshake**; não limita a duração da consulta |

Argumentos de `connect()` vencem a string de conexão. IPv6 vai entre colchetes:
`nishidb://[2001:db8::1]:9142/eventos`.

**Não são aceitos**, e a recusa é explícita: credencial embutida
(`nishidb://user:senha@host`) e `?password=` na query string. Uma string de
conexão acaba em log de aplicação, histórico de shell e arquivo versionado.

## Tipos

| Tipo do NishiDB | Python |
|---|---|
| `string` | `str` |
| `int64` | `int` (exato, inclusive nos limites do int64) |
| `double` | `float` |
| `boolean` | `bool` |
| `timestamp`, `datetime` | `datetime` **aware, em UTC** |
| `time` | `datetime.time` **naive** (é hora do dia, não instante) |
| `bytes` | `bytes` |
| `json` | `dict` / `list` / escalar |
| `ipv4`, `ipv6`, `ipv4_prefix`, `ipv6_prefix` | `str` |
| coluna `auto` ainda não fixada | o que o valor for |
| NULL | `None` |

Parâmetros aceitam `str`, `int`, `float`, `bool`, `Decimal`, `datetime`
(**precisa de fuso**) e `time`. `None` é recusado nomeando `IS NULL` — não
existe parâmetro nulo no protocolo.

## Erros

Hierarquia da PEP 249, com o código do protocolo preservado:

```python
try:
    cur.execute("SELECT a FROM t GROUP BY a")
except nishidb.NotSupportedError as e:
    print(e.code)       # UNSUPPORTED_CLAUSE
    print(e.line, e.column)
except nishidb.OperationalError as e:
    if e.transient:     # TOO_MANY_QUERIES, TOO_MANY_LONG_QUERIES, COMPACTION_BUSY
        time.sleep(e.retry_after or 1)
```

`transient` existe porque a hierarquia da PEP 249 não tem onde guardar a
distinção entre "repita" e "repetir não adianta". **O client não tenta de novo
sozinho**: quem decide repetir é a aplicação.

## O que este client NÃO faz

Não é omissão — é o que o NishiDB é:

- **Só leitura.** Os dados entram por ingestão (Kafka ou `nishidb ingest`); não
  há `INSERT`/`UPDATE`/`DELETE`, nem `executemany()`.
- **Sem transação.** `commit()` é no-op (a PEP 249 manda que seja);
  `rollback()` levanta, porque não há o que desfazer.
- **Sem cursor rolável.** Para frente apenas, uma passada.
- **Sem `GROUP BY`, `JOIN`, `HAVING`, `DISTINCT` ou `UNION`** — recusados pelo
  servidor com `UNSUPPORTED_CLAUSE`, nomeando a cláusula. `SELECT`, `WHERE`,
  `LIKE`, `ORDER BY`, `LIMIT` e `OFFSET` existem.
- **Sem `verify=False`.** Não há caminho para desligar a verificação de
  certificado, nem por parâmetro.

## Streaming

`fetchone()`, `fetchmany()` e a iteração consomem o resultado **uma linha por
vez**; só `fetchall()` materializa. Um `SELECT` de milhões de linhas atravessa
o client com memória constante — há um teste que reprova se deixar de ser
verdade.

## Desenvolvimento

```bash
make test-python      # unitários, sem servidor e sem pip install
make test-python-it   # contra o binário real, com TLS e credencial
make lint-python      # ruff + mypy (exige o extra `dev`)
```
