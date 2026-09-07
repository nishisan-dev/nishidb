"""Percorre uma resposta de `/v1/query`. Porte de `internal/protocol/stream.go`.

O contrato que ele impõe é o mesmo do Go: a primeira linha é o schema, as do
meio são dados, e a última é SEMPRE um fecho. **Um stream que acaba sem fecho é
falha** — é assim que uma conexão cortada no meio deixa de ser indistinguível
de um resultado que terminou.

As linhas de dados são entregues CRUAS, sem decodificar, exatamente como o
`Reader.Linha()` do Go: quem quer o passthrough NDJSON não paga decodificação,
e quem quer valores decodifica uma vez só.

E os três desfechos ficam SEPARADOS: `fim` (a consulta terminou bem), `erro`
(o servidor disse que ela quebrou) e a exceção de transporte (o servidor sumiu
no meio da frase). Confundir os dois últimos é o que faz um resultado cortado
ser apresentado como completo.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO

from . import erros

#: A chave reservada que distingue uma linha de MOLDURA de uma de dados
#: (`protocol.FrameKey`). O servidor RECUSA, antes do primeiro byte, a
#: query cuja projeção produza uma coluna com este nome — é o que torna a
#: discriminação por prefixo segura.
CHAVE_MOLDURA = "nishidb"

_PREFIXO = b'{"' + CHAVE_MOLDURA.encode() + b'":'

MOLDURA_SCHEMA = "schema"
MOLDURA_FIM = "end"
MOLDURA_ERRO = "error"

#: Teto de uma linha, espelhando o do LeitorNdjson.java. Existe para que um
#: corpo hostil (ou um bug do servidor) vire exceção em vez de consumir a
#: memória da máquina.
TETO_LINHA = 64 * 1024 * 1024


@dataclass(frozen=True)
class Coluna:
    """Uma coluna da projeção, como a linha de schema a descreve.

    Os sete campos são exatamente os da tupla de `Cursor.description` da PEP
    249 — a linha de schema foi desenhada para o ResultSetMetaData do JDBC e
    encaixa 1:1. **O client não recalcula nenhum destes números**, e é por isso
    que um tipo novo no servidor não exige release deste pacote.
    """

    name: str
    type: str
    jdbc: str
    nullable: bool
    precision: int
    scale: int
    display_size: int

    @classmethod
    def de_json(cls, d: dict[str, Any]) -> Coluna:
        # `.get` com default para todo campo: um servidor MAIS VELHO que não
        # publique um deles tem de continuar utilizável, e o default é o mesmo
        # "desconhecido" que o próprio protocolo usa. O jdbc ausente vira
        # OTHER, nunca VARCHAR — chamá-la de texto seria afirmar um tipo.
        return cls(
            name=str(d.get("name", "")),
            type=str(d.get("type", "unknown")),
            jdbc=str(d.get("jdbc", "OTHER")),
            nullable=bool(d.get("nullable", True)),
            precision=int(d.get("precision", 0)),
            scale=int(d.get("scale", 0)),
            display_size=int(d.get("displaySize", 0)),
        )


@dataclass(frozen=True)
class Fim:
    """O fecho de SUCESSO.

    `truncated` distingue "acabou" de "parou no maxRows que o PRÓPRIO cliente
    pediu" — e atender ao que foi pedido não é falha.
    """

    rows: int
    truncated: bool


@dataclass(frozen=True)
class Falha:
    """O fecho de ERRO: a consulta quebrou DEPOIS de o primeiro byte já ter
    saído, quando o status HTTP 200 já foi enviado e não há mais status a usar.

    `rows` diz quantas linhas válidas o cliente recebeu antes da falha.
    """

    code: str
    rows: int
    message: str


class _Coletor:
    """Captura os pares do objeto de TOPO preservando chave repetida.

    `SELECT a, a` produz `{"a":1,"a":1}`, que é legal. Um `json.loads` comum
    devolveria um dict com UMA chave e a coluna repetida sumiria em silêncio —
    o casamento com o schema tem de ser posicional.

    O `object_pairs_hook` é chamado de dentro para fora, então a ÚLTIMA chamada
    é a do objeto de topo. Os aninhados viram `dict` normalmente, que é o que a
    coluna `json` deve entregar.
    """

    __slots__ = ("topo",)

    def __init__(self) -> None:
        self.topo: list[tuple[str, Any]] = []

    def __call__(self, pares: list[tuple[str, Any]]) -> dict[str, Any]:
        self.topo = pares
        return dict(pares)


def pares_da_linha(linha: bytes) -> list[tuple[str, Any]]:
    """Decodifica uma linha de DADOS em pares (nome, valor), na ordem do fio."""
    coletor = _Coletor()
    try:
        topo = json.loads(linha, object_pairs_hook=coletor)
    except ValueError as e:
        raise erros.OperationalError(f"linha de dados ilegível: {e}") from e
    if not isinstance(topo, dict):
        raise erros.OperationalError(
            "linha de dados não é um objeto JSON — o protocolo foi violado"
        )
    return coletor.topo


class Leitor:
    """Consome o corpo de `/v1/query`, uma linha por vez."""

    def __init__(self, stream: BinaryIO) -> None:
        self._stream = stream
        self._schema: list[Coluna] | None = None
        self._dataset: str | None = None
        self._fim: Fim | None = None
        self._erro: Falha | None = None
        self._encerrado = False

    # ------------------------------------------------------------- desfechos

    @property
    def schema(self) -> list[Coluna] | None:
        """As colunas, disponíveis assim que a primeira linha é lida."""
        return self._schema

    @property
    def dataset(self) -> str | None:
        return self._dataset

    @property
    def fim(self) -> Fim | None:
        """O fecho de sucesso, ou None se a consulta terminou em erro."""
        return self._fim

    @property
    def erro(self) -> Falha | None:
        """O fecho de falha, ou None se a consulta terminou bem."""
        return self._erro

    # ---------------------------------------------------------------- leitura

    def _proxima_linha(self) -> bytes | None:
        bruto = self._stream.readline(TETO_LINHA + 1)
        if not bruto:
            return None
        if len(bruto) > TETO_LINHA:
            raise erros.OperationalError(
                f"linha acima do teto de {TETO_LINHA} bytes — corpo hostil ou defeito do servidor"
            )
        return bruto

    def linhas(self) -> Iterator[bytes]:
        """Itera as linhas de DADOS, cruas e sem o LF.

        Ao terminar, `fim` ou `erro` está preenchido. Falha de transporte,
        moldura ilegível e stream sem fecho levantam.
        """
        while True:
            if self._encerrado:
                return
            bruto = self._proxima_linha()
            if bruto is None:
                self._encerrado = True
                # EOF sem fecho: o servidor morreu, a rede caiu ou alguém
                # truncou o corpo. Sem esta checagem o cliente apresentaria um
                # resultado PARCIAL como completo.
                raise erros.OperationalError("o stream terminou sem a linha de fecho")

            linha = bruto.rstrip(b"\r\n")
            if not linha:
                continue

            if not linha.startswith(_PREFIXO):
                if self._schema is None:
                    self._encerrado = True
                    raise erros.OperationalError("a primeira linha não é o schema")
                yield linha
                continue

            if not self._moldura(linha):
                return

    def _moldura(self, linha: bytes) -> bool:
        """Consome uma linha de moldura. True = continue lendo (era o schema)."""
        try:
            doc = json.loads(linha)
        except ValueError as e:
            self._encerrado = True
            raise erros.OperationalError(f"moldura ilegível: {e}") from e
        if not isinstance(doc, dict):
            self._encerrado = True
            raise erros.OperationalError("moldura não é um objeto JSON")

        qual = doc.get(CHAVE_MOLDURA)
        if qual == MOLDURA_SCHEMA:
            if self._schema is not None:
                self._encerrado = True
                raise erros.OperationalError("segunda linha de schema no mesmo stream")
            colunas = doc.get("columns")
            if not isinstance(colunas, list):
                self._encerrado = True
                raise erros.OperationalError("a linha de schema não traz a lista de colunas")
            self._schema = [Coluna.de_json(c) for c in colunas if isinstance(c, dict)]
            self._dataset = doc.get("dataset")
            return True

        if qual == MOLDURA_FIM:
            self._fim = Fim(rows=int(doc.get("rows", 0)), truncated=bool(doc.get("truncated")))
        elif qual == MOLDURA_ERRO:
            self._erro = Falha(
                code=str(doc.get("code", "")),
                rows=int(doc.get("rows", 0)),
                message=str(doc.get("message", "")),
            )
        else:
            self._encerrado = True
            raise erros.OperationalError(f"moldura {qual!r} desconhecida")

        self._encerrado = True
        return False
