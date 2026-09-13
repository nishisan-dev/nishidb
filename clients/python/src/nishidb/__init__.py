"""Client Python do NishiDB — DB-API 2.0 (PEP 249).

    import nishidb

    with nishidb.connect("nishidb://db01:9142/radius_sessions", user="leitor") as cx:
        cur = cx.cursor()
        cur.execute("SELECT user_name, nas_ip FROM radius_sessions WHERE status = ?",
                    ("Stop",))
        for linha in cur:
            print(linha["user_name"], linha["nas_ip"])

Ou, pela camada pythônica:

    with nishidb.client("nishidb://db01:9142/radius_sessions", user="leitor") as c:
        for linha in c.query("SELECT * FROM radius_sessions LIMIT 10"):
            print(dict(linha))

**Só leitura.** O NishiDB não aceita escrita por SQL: os dados entram por
ingestão (Kafka ou `nishidb ingest`). Não há transação, `INSERT`, `UPDATE`,
`DELETE` nem `executemany()`.

**Zero dependência de runtime** — só a stdlib, pela mesma razão que o engine é
um binário único: o client tem de ser instalável no host em que se está
diagnosticando.
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from .cliente import Client
from .conexao import ENV_SENHA, Connection, connect
from .cursor import Cursor
from .documentos import (
    ColunaMeta,
    ConsultaEmVoo,
    DatasetMeta,
    Estatisticas,
    Saude,
)
from .erros import (
    BAD_PARAMETER,
    BAD_REQUEST,
    BIND,
    COMPACTION_BUSY,
    DATASET_CORRUPT,
    FORBIDDEN,
    INTERNAL,
    LAST_ADMIN,
    MAX_ROWS_ABOVE_LIMIT,
    MAX_ROWS_EXCEEDED,
    METHOD_NOT_ALLOWED,
    REQUEST_TOO_LARGE,
    RESERVED_COLUMN,
    ROW_TOO_LARGE,
    SYNTAX,
    TIMEOUT,
    TOO_MANY_LONG_QUERIES,
    TOO_MANY_QUERIES,
    UNAUTHENTICATED,
    UNKNOWN_DATASET,
    UNKNOWN_USER,
    UNSUPPORTED_CLAUSE,
    UNSUPPORTED_MEDIA_TYPE,
    USER_EXISTS,
    WEAK_PASSWORD,
    WRITER_FAILED,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from .linha import Row
from .tipos import BINARY, DATETIME, NUMBER, ROWID, STRING

#: A versão do CLIENT, própria e separada da do engine — o client não muda de
#: versão porque o servidor mudou. Tem de bater com `[project].version` do
#: pyproject.toml, e `internal/gates/python_test.go` reprova a divergência.
__version__ = "0.1.0"

# ----------------------------------------------------------- globais da PEP 249

#: A versão do DB-API que este módulo implementa.
apilevel = "2.0"

#: 1 = "o MÓDULO pode ser compartilhado entre threads, a conexão não". É a
#: verdade: por baixo de cada Connection há UMA conexão HTTP, que executa uma
#: requisição por vez. Declarar 2 ou 3 convidaria a um compartilhamento que
#: corromperia o stream.
threadsafety = 1

#: O NishiDB usa `?` posicional.
paramstyle = "qmark"


# ------------------------------------------------------------- os construtores
#
# A PEP 249 exige que o módulo os exponha. Onde ela deixa o fuso em aberto, nós
# escolhemos UTC e dizemos que escolhemos: o alternativa seria o fuso da
# máquina, e um relatório não pode mudar de dia conforme onde roda.


def Date(year: int, month: int, day: int) -> _dt.datetime:  # noqa: N802
    """Uma data, como o instante da MEIA-NOITE EM UTC.

    Não devolve `datetime.date`: o wire não tem tipo data, e um `date` não
    identifica instante nenhum sem um fuso. Fixar UTC aqui é a mesma escolha
    que o driver JDBC faz ao dividir os dias em UTC — e a única que faz a
    mesma consulta responder igual em São Paulo e em Tóquio.
    """
    return _dt.datetime(year, month, day, tzinfo=_dt.timezone.utc)


def Time(hour: int, minute: int, second: int) -> _dt.time:  # noqa: N802
    """Uma hora do dia. Naive de propósito: não é um instante."""
    return _dt.time(hour, minute, second)


def Timestamp(  # noqa: N802
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> _dt.datetime:
    """Um instante, em UTC."""
    return _dt.datetime(year, month, day, hour, minute, second, tzinfo=_dt.timezone.utc)


def DateFromTicks(ticks: float) -> _dt.datetime:  # noqa: N802
    return _dt.datetime.fromtimestamp(ticks, tz=_dt.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def TimeFromTicks(ticks: float) -> _dt.time:  # noqa: N802
    return _dt.datetime.fromtimestamp(ticks, tz=_dt.timezone.utc).time()


def TimestampFromTicks(ticks: float) -> _dt.datetime:  # noqa: N802
    return _dt.datetime.fromtimestamp(ticks, tz=_dt.timezone.utc)


def Binary(valor: Any) -> bytes:  # noqa: N802
    """Bytes.

    Exigido pela PEP 249 e presente para que código portável importe. Como
    PARÂMETRO ele é recusado, e não por descuido: o binder do servidor não
    compara coluna binária, então não existe consulta em que um parâmetro de
    bytes teria sentido.
    """
    if isinstance(valor, str):
        return valor.encode("utf-8")
    return bytes(valor)


def client(dsn: str | None = None, **kwargs: Any) -> Client:
    """Abre a camada pythônica. Mesmos argumentos de `connect()`."""
    return Client.connect(dsn, **kwargs)


# Agrupado por PAPEL, e não em ordem alfabética — daí o noqa. Esta lista é o
# índice da superfície pública, e a pergunta que se faz a ela é "o que o módulo
# oferece?", não "onde está o nome X". Ordenar misturaria os construtores da
# PEP 249 com os códigos do protocolo e com as dataclasses das rotas.
__all__ = [  # noqa: RUF022
    # PEP 249
    "apilevel",
    "threadsafety",
    "paramstyle",
    "connect",
    "Connection",
    "Cursor",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    # camada pythônica
    "client",
    "Client",
    "Row",
    # documentos das rotas
    "ColunaMeta",
    "DatasetMeta",
    "Estatisticas",
    "ConsultaEmVoo",
    "Saude",
    # códigos do protocolo
    "BAD_REQUEST",
    "BAD_PARAMETER",
    "MAX_ROWS_ABOVE_LIMIT",
    "RESERVED_COLUMN",
    "SYNTAX",
    "BIND",
    "UNSUPPORTED_CLAUSE",
    "UNKNOWN_DATASET",
    "METHOD_NOT_ALLOWED",
    "UNSUPPORTED_MEDIA_TYPE",
    "REQUEST_TOO_LARGE",
    "TOO_MANY_QUERIES",
    "TOO_MANY_LONG_QUERIES",
    "TIMEOUT",
    "MAX_ROWS_EXCEEDED",
    "ROW_TOO_LARGE",
    "DATASET_CORRUPT",
    "INTERNAL",
    "UNAUTHENTICATED",
    "FORBIDDEN",
    "WRITER_FAILED",
    "COMPACTION_BUSY",
    "USER_EXISTS",
    "UNKNOWN_USER",
    "LAST_ADMIN",
    "WEAK_PASSWORD",
    "ENV_SENHA",
    "__version__",
]
