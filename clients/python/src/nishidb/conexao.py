"""A `Connection` da PEP 249 e a função `connect()`.

A resolução de credencial é portada de `internal/cli/credential.go`, com as
mesmas quatro recusas — inclusive a distinção entre "variável ausente" e
"variável definida e VAZIA", que é o caso do container cujo orquestrador injeta
a chave sem valor. Sem essa recusa, o Basic sairia com senha vazia e o 401 do
servidor não daria pista nenhuma de qual das duas aconteceu.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote

from . import documentos, erros
from . import dsn as _dsn
from .cursor import Cursor
from .transporte import Transporte

#: O ÚNICO nome de variável de ambiente aceito para a senha. Fixo, ao contrário
#: do `passwordEnv` do servidor (que NOMEIA uma variável por usuário), para que
#: a documentação possa dizer "NISHIDB_PASSWORD" sem indireção.
ENV_SENHA = "NISHIDB_PASSWORD"


def _resolve_password(user: str | None, password: str | None, password_file: str | None) -> str:
    """Valida as fontes de senha e lê a escolhida. Nenhuma mensagem ecoa VALOR."""
    password_env = os.environ.get(ENV_SENHA)
    has_env = password_env is not None
    has_file = password_file is not None
    has_kwarg = password is not None
    sources = sum((has_env, has_file, has_kwarg))

    if not user:
        if sources:
            raise erros.InterfaceError(
                f"password/password_file/{ENV_SENHA} exigem user — "
                "a senha sozinha não identifica ninguém"
            )
        # Caminho anônimo: nenhum header Authorization é enviado. TLS e
        # autenticação são decisões independentes do servidor (§77).
        return ""

    if sources == 0:
        raise erros.InterfaceError(
            f"user exige uma senha por password=, password_file= ou {ENV_SENHA}"
        )
    if sources > 1:
        raise erros.InterfaceError(
            f"password, password_file e {ENV_SENHA} são exclusivos: escolha UMA fonte"
        )

    if has_kwarg:
        return password or ""
    if has_file:
        try:
            with open(password_file, encoding="utf-8") as f:  # type: ignore[arg-type]
                raw = f.read()
        except OSError as e:
            raise erros.InterfaceError(f"password_file: {e}") from e
        # Mesma aparagem do servidor: todo `echo senha > arquivo` termina em
        # LF ou CRLF.
        cleaned = raw.rstrip("\r\n")
        if not cleaned:
            raise erros.InterfaceError(
                f"password_file {password_file} está vazio (ou só tem quebra de linha)"
            )
        return cleaned

    if password_env == "":
        raise erros.InterfaceError(
            f"{ENV_SENHA} está definida e VAZIA — não é a mesma coisa que ausente; "
            "preencha a variável ou use password_file"
        )
    return password_env or ""


_resolve_senha = _resolve_password


class Connection:
    """Uma conexão. NÃO é thread-safe — é `threadsafety = 1`, e por baixo há
    uma conexão HTTP que executa uma requisição por vez."""

    # A PEP 249 recomenda expor a hierarquia como atributos da conexão, para
    # que quem tem só o objeto consiga escrever `except cx.ProgrammingError`.
    Warning = erros.Warning
    Error = erros.Error
    InterfaceError = erros.InterfaceError
    DatabaseError = erros.DatabaseError
    DataError = erros.DataError
    OperationalError = erros.OperationalError
    IntegrityError = erros.IntegrityError
    InternalError = erros.InternalError
    ProgrammingError = erros.ProgrammingError
    NotSupportedError = erros.NotSupportedError

    def __init__(self, destination: _dsn.Destino, user: str | None, password: str) -> None:
        self._destination = destination
        self._transport = Transporte(destination, user, password)
        self._closed = False
        self._cursors: list[Cursor] = []
        self._limits: dict[str, tuple[int, int]] = {}

        # Os limites do dataset são buscados ANTES de qualquer consulta, como o
        # JDBC faz. Efeito colateral desejado: dataset inexistente falha ao
        # ABRIR a conexão, e não na primeira query — que é onde o usuário já
        # perdeu o contexto do erro.
        try:
            metadata = self.metadata(destination.dataset)
        except erros.Error:
            self._transport.close()
            self._closed = True
            raise
        self._dataset_meta = metadata

    # --------------------------------------------------------------- estado

    @property
    def dataset(self) -> str:
        """O dataset da string de conexão — o fallback de toda query cujo
        `FROM` o scanner não reconheceu."""
        return self._destination.dataset

    @property
    def transport(self) -> Transporte:
        return self._transport

    transporte = transport

    def require_open(self) -> Connection:
        if self._closed:
            raise erros.InterfaceError("a conexão está fechada")
        return self

    exige_aberta = require_open

    def limits_for(self, dataset: str) -> tuple[int, int]:
        """(maxRows, maxTimeoutMs) efetivos, ou (0, 0) quando desconhecidos.

        Só o dataset da conexão é buscado na abertura. Para outro, devolvemos
        "não sei" e deixamos o servidor decidir — buscar metadado de todo
        dataset citado num FROM custaria uma viagem de rede por consulta nova,
        para evitar um erro que o servidor já dá com clareza.
        """
        return self._limits.get(dataset, (0, 0))

    tetos_de = limits_for

    # ------------------------------------------------------------- as rotas

    def metadata(self, dataset: str | None = None) -> list[documentos.DatasetMeta]:
        """`GET /v1/metadata`, de todos os datasets ou de um só."""
        self.require_open()
        path = "/v1/metadata"
        if dataset is not None:
            path += "?dataset=" + quote(dataset, safe="")
        dataset_meta = documentos.datasets_de(self._transport.documento(path))
        for item in dataset_meta:
            self._limits[item.name] = (item.max_rows, item.timeout_ms)
        return dataset_meta

    def stats(self) -> documentos.Estatisticas:
        """`GET /v1/stats`."""
        self.require_open()
        return documentos.Estatisticas.de_json(self._transport.documento("/v1/stats"))

    def health(self) -> documentos.Saude:
        """`GET /v1/health`.

        Sai com 503 quando o processo não está UP, e isso **não é erro** aqui:
        é a resposta, e o estado está no corpo.
        """
        self.require_open()
        return documentos.Saude.de_json(self._transport.documento("/v1/health"))

    # -------------------------------------------------------------- PEP 249

    def cursor(self) -> Cursor:
        self.require_open()
        cursor = Cursor(self)
        self._cursors.append(cursor)
        return cursor

    def commit(self) -> None:
        """No-op.

        A PEP 249 manda implementar `commit()` "com funcionalidade vazia" nos
        módulos que não têm transação — e é o certo: `with conexao:` chama
        commit ao sair, e lançar aqui quebraria todo código portável que só
        estava fechando o bloco. Toda escrita no NishiDB entra por ingestão.
        """

    def rollback(self) -> None:
        """Lança, e a assimetria com `commit()` é deliberada.

        Um commit vazio não mente: não havia nada pendente a gravar. Um
        rollback vazio mentiria — quem o chama acredita estar DESFAZENDO
        alguma coisa, e sair em silêncio deixaria essa crença de pé.
        """
        raise erros.NotSupportedError("o NishiDB não tem transação: não há o que desfazer")

    def close(self) -> None:
        """Fecha os cursores abertos e depois a conexão HTTP, nessa ordem."""
        if self._closed:
            return
        for cursor in list(self._cursors):
            cursor.close()
        self._cursors.clear()
        self._transport.close()
        self._closed = True

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def connect(
    dsn: str | None = None,
    *,
    host: str | None = None,
    port: int | None = None,
    dataset: str | None = None,
    user: str | None = None,
    password: str | None = None,
    password_file: str | None = None,
    tls: bool | None = None,
    tls_ca: str | None = None,
    connect_timeout_ms: int | None = None,
) -> Connection:
    """Abre uma conexão.

    `dsn` é `nishidb://host[:9142]/dataset[?tls=true&tls_ca=/caminho]`. Os
    argumentos nomeados **vencem** o que a string de conexão diz — a mesma
    precedência de `Properties > URL` do driver JDBC.

    A senha nunca vem na string de conexão (uma string de conexão acaba em log
    e em arquivo versionado). Use `password=`, `password_file=` ou a variável
    `NISHIDB_PASSWORD` — uma e só uma delas.
    """
    if dsn is not None:
        destination = _dsn.parse(dsn)
    else:
        if not host or not dataset:
            raise erros.InterfaceError("sem dsn, host= e dataset= são obrigatórios")
        destination = _dsn.Destino(host=host, port=port or _dsn.PORTA_PADRAO, dataset=dataset)

    overrides: dict[str, Any] = {}
    if host is not None:
        overrides["host"] = host
    if port is not None:
        overrides["port"] = port
    if dataset is not None:
        overrides["dataset"] = dataset
    if user is not None:
        overrides["user"] = user
    if tls is not None:
        overrides["tls"] = tls
    if tls_ca is not None:
        overrides["tls_ca"] = tls_ca
    if connect_timeout_ms is not None:
        overrides["connect_timeout_ms"] = connect_timeout_ms
    if overrides:
        from dataclasses import replace

        destination = replace(destination, **overrides)

    if destination.tls_ca is not None and not destination.tls:
        raise erros.InterfaceError("tls_ca exige tls=True")

    user = destination.user
    password = _resolve_password(user, password, password_file)
    return Connection(destination, user, password)
