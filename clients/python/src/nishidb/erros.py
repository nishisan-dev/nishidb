"""A hierarquia de exceções da PEP 249 e a tradução dos códigos do protocolo.

Os NOMES das classes são ditados pela PEP 249 e por isso estão em inglês; o
resto do módulo segue o português do repositório (a mesma divisão do driver
JDBC, cujas interfaces java.sql são inglesas e cujo `wire` é português).

A tabela CODIGOS é o espelho de SqlStates.java, e existe pela mesma razão: o
cliente casa por CÓDIGO, nunca pelo texto da mensagem, que é prosa para humanos
e pode ser reescrita (ver o comentário de internal/protocol/protocol.go).

`internal/gates/python_test.go` parseia `internal/protocol/protocol.go` com
go/ast, colhe toda constante `Cod*` de valor string e reprova se algum literal
não aparecer AQUI. Código novo em Go sem exceção Python reprova no CI Go, antes
de qualquer intérprete Python rodar.
"""

from __future__ import annotations

import builtins

# --------------------------------------------------------------- os códigos do wire
#
# Um por constante Cod* de internal/protocol/protocol.go. Ficam como
# constantes de módulo, e não soltos na tabela, para que o usuário possa
# comparar `err.code == nishidb.BAD_PARAMETER` sem digitar a string.

BAD_REQUEST = "BAD_REQUEST"
BAD_PARAMETER = "BAD_PARAMETER"
MAX_ROWS_ABOVE_LIMIT = "MAX_ROWS_ABOVE_LIMIT"
RESERVED_COLUMN = "RESERVED_COLUMN"
SYNTAX = "SYNTAX"
BIND = "BIND"
UNSUPPORTED_CLAUSE = "UNSUPPORTED_CLAUSE"
UNKNOWN_DATASET = "UNKNOWN_DATASET"
FOLLOWER_NOT_READY = "FOLLOWER_NOT_READY"
UNKNOWN_SCHEMA = "UNKNOWN_SCHEMA"
METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED"
UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE"
REQUEST_TOO_LARGE = "REQUEST_TOO_LARGE"
TOO_MANY_QUERIES = "TOO_MANY_QUERIES"
TOO_MANY_LONG_QUERIES = "TOO_MANY_LONG_QUERIES"
TIMEOUT = "TIMEOUT"
MAX_ROWS_EXCEEDED = "MAX_ROWS_EXCEEDED"
ROW_TOO_LARGE = "ROW_TOO_LARGE"
SNAPSHOT_TOO_OLD = "SNAPSHOT_TOO_OLD"
DATASET_CORRUPT = "DATASET_CORRUPT"
INTERNAL = "INTERNAL"
UNAUTHENTICATED = "UNAUTHENTICATED"
FORBIDDEN = "FORBIDDEN"
WRITER_FAILED = "WRITER_FAILED"
COMPACTION_BUSY = "COMPACTION_BUSY"
USER_EXISTS = "USER_EXISTS"
UNKNOWN_USER = "UNKNOWN_USER"
LAST_ADMIN = "LAST_ADMIN"
WEAK_PASSWORD = "WEAK_PASSWORD"


# O sufixo `Error` que o N818 pede não cabe: esta classe nunca é LEVANTADA. Ela
# é a base privada que carrega os campos, e as duas que descendem dela é que são
# a exceção (`Error`) e o aviso (`Warning`) — chamá-la de `_DetalhesError`
# afirmaria que é um erro, e ela não é.
class _Detalhes(Exception):  # noqa: N818
    """O que toda falha nossa carrega além da mensagem.

    Herda de `Exception` — e não é mixin puro — para que o `super().__init__`
    abaixo tenha um destino declarado. Em runtime a MRO já o levava ao lugar
    certo (`builtins.Warning` ou `Exception`, conforme a classe concreta), mas
    um verificador de tipos só enxerga `object` num mixin solto e acusa
    "argumentos demais". A linearização não muda: as duas classes concretas
    continuam com `Exception` no fim da cadeia.

    A PEP 249 não define construtor para as exceções, então acrescentar campos
    é legítimo — e necessário: a hierarquia dela **não tem onde guardar** a
    distinção "repita" / "repetir não adianta", que o JDBC codifica escolhendo
    entre SQLTransientException e SQLNonTransientException. Sem `transient`, a
    informação se perderia na tradução e todo usuário de Python reinventaria a
    tabela.
    """

    def __init__(
        self,
        message: str = "",
        *,
        code: str | None = None,
        line: int | None = None,
        column: int | None = None,
        rows_delivered: int | None = None,
        transient: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        #: O código do wire (`SYNTAX`, `TIMEOUT`, …), ou None para falha nascida
        #: no cliente (transporte, DNS, TLS, protocolo violado).
        self.code = code
        #: 1-based, e só para SYNTAX, BIND e UNSUPPORTED_CLAUSE — os únicos que
        #: têm posição no texto da query.
        self.line = line
        self.column = column
        #: Quantas linhas VÁLIDAS chegaram antes da falha, quando ela veio no
        #: fecho de um stream já começado.
        self.rows_delivered = rows_delivered
        #: True só para os três de TOO_MANY_QUERIES / TOO_MANY_LONG_QUERIES /
        #: COMPACTION_BUSY. O client NÃO tenta de novo sozinho: quem decide
        #: repetir é a aplicação, e aqui só entregamos o que ela precisa.
        self.transient = transient
        #: Segundos sugeridos pelo header Retry-After, quando o servidor mandou.
        self.retry_after = retry_after


# ------------------------------------------------------------- hierarquia da PEP 249


class Warning(_Detalhes, builtins.Warning):
    """Aviso. O nome — sem sufixo Error, sombreando o builtin — é exigência
    literal da PEP 249, não descuido.

    Herda do `Warning` do Python, e não direto de `Exception`: a PEP 249 só
    exige que seja uma exceção, mas `warnings.warn()` RECUSA uma categoria que
    não descenda do builtin — e é por `warnings.warn` que o
    MAX_ROWS_EXCEEDED sai. Como o builtin já é subclasse de `Exception`, as
    duas exigências são satisfeitas de uma vez.
    """


class Error(_Detalhes, Exception):
    """Raiz de toda falha. Pegar isto pega tudo o que o client levanta."""


class InterfaceError(Error):
    """Defeito no uso da interface, não no banco: cursor fechado, requisição
    malformada pelo próprio client."""


class DatabaseError(Error):
    """Falha vinda do servidor."""


class DataError(DatabaseError):
    """Problema com o DADO: valor fora de faixa, base64 inválido, parâmetro que
    não cabe no tipo."""


class OperationalError(DatabaseError):
    """Falha de operação, fora do controle do programador: rede, TLS, prazo,
    servidor cheio, credencial recusada."""


class IntegrityError(DatabaseError):
    """Violação de integridade referencial.

    Declarada porque a PEP 249 exige que o módulo exponha o nome inteiro da
    hierarquia, e porque `except nishidb.IntegrityError` tem de compilar em
    código portável. O NishiDB **nunca a levanta**: não há escrita por SQL,
    logo não há restrição de integridade a violar.
    """


class InternalError(DatabaseError):
    """Defeito do lado de lá: dataset corrompido, erro interno do servidor."""


class ProgrammingError(DatabaseError):
    """Erro de quem escreveu a query: sintaxe, coluna inexistente, dataset
    que não existe."""


class NotSupportedError(DatabaseError):
    """A capacidade não existe nesta versão — e é isso que a distingue de um
    erro de digitação. `ORDER BY` já existe; `GROUP BY` cai aqui."""


# --------------------------------------------------------------------- a tradução

#: Os três códigos que valem repetir. Espelha a escolha de
#: SQLTransientException em SqlStates.java.
#:
#: TOO_MANY_QUERIES sai com Retry-After: 1 — as consultas que ocupam o pool
#: geral duram milissegundos. TOO_MANY_LONG_QUERIES sai com Retry-After: 30,
#: porque quem ocupa as vagas de consulta LONGA pode estar lá por minutos, e
#: repetir em um segundo é quase certo que falha de novo.
TRANSIENTES = frozenset({TOO_MANY_QUERIES, TOO_MANY_LONG_QUERIES, COMPACTION_BUSY, FOLLOWER_NOT_READY})

#: Código do protocolo -> classe da PEP 249.
#:
#: MAX_ROWS_EXCEEDED aponta para Warning e NÃO para uma exceção, de propósito:
#: o caso de fronteira (resultado com exatamente maxRows linhas) é falso
#: positivo conhecido, e distingui-lo no servidor custaria varrer o dataset
#: inteiro — exatamente o que o teto existe para evitar. Quem o consome é o
#: cursor, que emite warnings.warn() e PRESERVA as linhas já entregues.
CODIGOS: dict[str, type[Warning] | type[Error]] = {
    # Erro de quem escreveu a query.
    SYNTAX: ProgrammingError,
    BIND: ProgrammingError,
    RESERVED_COLUMN: ProgrammingError,
    UNKNOWN_DATASET: ProgrammingError,
    FOLLOWER_NOT_READY: OperationalError,
    UNKNOWN_SCHEMA: ProgrammingError,
    # "Isto não existe nesta versão" — separado de "você digitou errado".
    UNSUPPORTED_CLAUSE: NotSupportedError,
    # Problema com o valor.
    BAD_PARAMETER: DataError,
    MAX_ROWS_ABOVE_LIMIT: DataError,
    # Operação: prazo, lotação, tamanho, credencial.
    TIMEOUT: OperationalError,
    TOO_MANY_QUERIES: OperationalError,
    TOO_MANY_LONG_QUERIES: OperationalError,
    COMPACTION_BUSY: OperationalError,
    REQUEST_TOO_LARGE: OperationalError,
    ROW_TOO_LARGE: OperationalError,
    # SNAPSHOT_TOO_OLD é falha de RECURSO do servidor, não do SQL nem do dado:
    # o isolamento não conseguiu guardar o passado até o fim da consulta.
    # OperationalError é a classe da PEP 249 para isso, e é a mesma dos outros
    # tetos — a diferença é que aqui repetir de fato resolve, porque a consulta
    # nova abre um snapshot que ainda não retém nada.
    SNAPSHOT_TOO_OLD: OperationalError,
    WRITER_FAILED: OperationalError,
    # Credencial recusada e papel insuficiente não têm classe própria na PEP
    # 249; OperationalError é onde ela documenta "authentication error".
    UNAUTHENTICATED: OperationalError,
    FORBIDDEN: OperationalError,
    # Gestão de usuário. O client é só leitura e não chama essas rotas, mas a
    # tabela cobre o protocolo inteiro. Nome repetido e último ADMIN são
    # violação de restrição da tabela — IntegrityError, a classe da PEP 249
    # para isso.
    USER_EXISTS: IntegrityError,
    LAST_ADMIN: IntegrityError,
    UNKNOWN_USER: ProgrammingError,
    WEAK_PASSWORD: DataError,
    # Defeito do lado de lá.
    DATASET_CORRUPT: InternalError,
    INTERNAL: InternalError,
    # O client montou uma requisição que o servidor não aceita — é defeito
    # NOSSO, não do banco, e por isso InterfaceError.
    BAD_REQUEST: InterfaceError,
    METHOD_NOT_ALLOWED: InterfaceError,
    UNSUPPORTED_MEDIA_TYPE: InterfaceError,
    # Não é exceção. Ver o comentário acima.
    MAX_ROWS_EXCEEDED: Warning,
}


def de_codigo(
    codigo: str,
    mensagem: str,
    *,
    line: int | None = None,
    column: int | None = None,
    rows_delivered: int | None = None,
    retry_after: float | None = None,
) -> Error:
    """Monta a exceção do código do wire.

    Código DESCONHECIDO vira DatabaseError com o código cru na mensagem, e
    jamais degrada para INTERNAL: degradar apagaria justamente a novidade que
    um servidor mais novo está tentando contar. É a mesma regra do HY000 do
    driver JDBC.
    """
    classe = CODIGOS.get(codigo)
    if classe is None:
        return DatabaseError(
            f"{mensagem} [código desconhecido do servidor: {codigo}]",
            code=codigo,
            line=line,
            column=column,
            rows_delivered=rows_delivered,
            retry_after=retry_after,
        )
    if classe is Warning:
        # Chamador errado: MAX_ROWS_EXCEEDED não é exceção. Levantar um
        # DatabaseError aqui seria mentir sobre a natureza do desfecho, então
        # avisamos o programador na linguagem dele.
        raise AssertionError(f"{codigo} é aviso, não exceção — use o caminho de warning do cursor")
    return classe(  # type: ignore[return-value]
        mensagem,
        code=codigo,
        line=line,
        column=column,
        rows_delivered=rows_delivered,
        transient=codigo in TRANSIENTES,
        retry_after=retry_after,
    )
