"""A camada pythônica, por cima da mesma `Connection` da PEP 249.

O DB-API é o que dá o ecossistema (pandas, SQLAlchemy, ferramentas de BI); esta
camada é o que faz o uso direto não doer. **Uma só implementação de streaming
por baixo das duas** — duplicar faria uma correção de conversão valer só num
dos caminhos.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from . import documentos
from .conexao import Connection, connect
from .linha import Row


class Client:
    """Fachada de leitura sobre uma conexão."""

    def __init__(self, conexao: Connection) -> None:
        self._cx = conexao

    @classmethod
    def conecta(cls, dsn: str | None = None, **kwargs: Any) -> Client:
        return cls(connect(dsn, **kwargs))

    @property
    def connection(self) -> Connection:
        """A conexão da PEP 249, para quem precisa descer um nível."""
        return self._cx

    # --------------------------------------------------------------- consulta

    def query(self, sql: str, *params: Any, dataset: str | None = None) -> Iterator[Row]:
        """Executa e ITERA o resultado, uma linha por vez.

        Os parâmetros são posicionais e viajam soltos, para que o caso comum
        não exija montar uma tupla:

            for linha in cliente.query("SELECT a FROM t WHERE b = ?", "x"):
                ...

        O gerador segura o stream: consuma-o (ou saia do laço, que fecha o
        cursor) antes de disparar outra consulta na mesma conexão.
        """
        cur = self._cx.cursor()
        try:
            cur.execute(sql, params, dataset=dataset)
            yield from cur
        finally:
            cur.close()

    def query_raw(self, sql: str, *params: Any, dataset: str | None = None) -> Iterator[bytes]:
        """O mesmo, entregando as linhas NDJSON cruas — a saída byte a byte do
        `nishidb query`."""
        cur = self._cx.cursor()
        try:
            cur.execute(sql, params, dataset=dataset)
            yield from cur.linhas_cruas()
        finally:
            cur.close()

    def one(self, sql: str, *params: Any, dataset: str | None = None) -> Row | None:
        """A primeira linha, ou None. Fecha o stream sem consumir o resto."""
        for linha in self.query(sql, *params, dataset=dataset):
            return linha
        return None

    # -------------------------------------------------------------- metadados

    def datasets(self) -> list[documentos.DatasetMeta]:
        """Todos os datasets consultáveis deste servidor."""
        return self._cx.metadata()

    def metadata(self, dataset: str) -> documentos.DatasetMeta | None:
        metas = self._cx.metadata(dataset)
        return metas[0] if metas else None

    def describe(self, dataset: str | None = None) -> list[Row]:
        """As colunas por `_meta.fields`, que traz o que `/v1/metadata` não tem:
        `indexed` (o índice FÍSICO, contra o `index` DECLARADO), `parent`,
        `occurrences`, `typeMismatches` e `coercions`.

        `_meta.fields` é uma tabela VIRTUAL sobre "este dataset": o nome dele
        não aparece no `FROM` e não existe como coluna para filtrar. Por isso o
        alvo viaja no campo `dataset` do corpo, e não num `WHERE`.

        A resposta é ESTADO CORRENTE, não declaração — para campo
        autodescoberto o tipo se fixa na primeira observação, então duas
        chamadas em momentos diferentes podem devolver conjuntos diferentes.
        """
        return list(
            self.query(
                "SELECT name, type, auto, index, indexed, parent, id, "
                "occurrences, typeMismatches, coercions FROM _meta.fields",
                dataset=dataset or self._cx.dataset,
            )
        )

    def stats(self) -> documentos.Estatisticas:
        return self._cx.stats()

    def health(self) -> documentos.Saude:
        return self._cx.health()

    # ------------------------------------------------------------------ ciclo

    def close(self) -> None:
        self._cx.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
