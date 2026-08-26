"""A linha de resultado: sequência para a PEP 249, mapping para quem usa.

`Row` é subclasse de `tuple`, então satisfaz literalmente o "a single sequence"
que a PEP 249 exige de `fetchone()` — o pandas e qualquer ferramenta que trate
o resultado como tupla continuam funcionando. E como ela também expõe `keys()`,
`dict(linha)` funciona: o construtor de `dict` usa o protocolo de mapping
quando o objeto tem `keys`.

**Os nomes moram na CLASSE, não na instância.** `fabrica` cria uma subclasse
por resultado, com `_nomes` e `_indices` como atributos de classe e
`__slots__ = ()`. Uma subclasse de `tuple` não aceita `__slots__` não-vazio, e
guardar os nomes por instância daria a cada linha um `__dict__` — cerca de uma
centena de bytes por row, gastos para repetir a mesma informação N vezes. Assim
uma linha custa exatamente o que custa a tupla dos seus valores, que é o §128
aplicado ao cliente.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, ClassVar

from . import erros


def indices_de(nomes: list[str]) -> dict[str, int]:
    """Mapa nome -> posição, com a PRIMEIRA ocorrência vencendo.

    Chave repetida é legal (`SELECT a, a`), e o acesso por nome tem de escolher
    uma: a primeira é a única escolha que não depende de quantas repetições
    vieram. As duas continuam alcançáveis por posição.
    """
    mapa: dict[str, int] = {}
    for i, nome in enumerate(nomes):
        mapa.setdefault(nome, i)
    return mapa


class Row(tuple):  # type: ignore[type-arg]
    """Uma linha. Imutável, como o resultado que ela representa.

    Não instancie diretamente: use `fabrica(nomes)` para obter a classe deste
    resultado.
    """

    __slots__ = ()

    _nomes: ClassVar[tuple[str, ...]] = ()
    _indices: ClassVar[dict[str, int]] = {}

    def keys(self) -> list[str]:
        """Os nomes das colunas, na ordem da projeção. É o que faz
        `dict(linha)` funcionar."""
        return list(self._nomes)

    def _posicao(self, nome: str) -> int:
        """Exato primeiro, depois sem caixa.

        Os identificadores do NishiDB são case-sensitive, mas o hábito de
        Python é tolerante — a ordem resolve a distância sem perder a exatidão.
        Ambiguidade no SEGUNDO passo é erro, e não "a primeira que aparecer":
        devolver uma coluna arbitrária seria pior que recusar.
        """
        pos = self._indices.get(nome)
        if pos is not None:
            return pos
        alvo = nome.lower()
        achados = [i for i, n in enumerate(self._nomes) if n.lower() == alvo]
        if not achados:
            raise erros.InterfaceError(
                f"não há coluna {nome!r} no resultado; há {list(self._nomes)}"
            )
        primeiro = achados[0]
        if any(self._nomes[i] != self._nomes[primeiro] for i in achados):
            raise erros.InterfaceError(
                f"o nome {nome!r} casa, sem diferenciar caixa, com mais de uma coluna: "
                f"{sorted({self._nomes[i] for i in achados})}"
            )
        return primeiro

    def __getitem__(self, chave: Any) -> Any:
        if isinstance(chave, str):
            return tuple.__getitem__(self, self._posicao(chave))
        return tuple.__getitem__(self, chave)

    def get(self, nome: str, default: Any = None) -> Any:
        """O valor da coluna, ou `default` se ela não está no resultado."""
        try:
            return self[nome]
        except erros.InterfaceError:
            return default

    def as_dict(self) -> dict[str, Any]:
        """Cópia como dict. Coluna repetida colapsa — é o que um dict faz;
        quem precisa das duas usa a posição."""
        return dict(zip(self._nomes, self, strict=True))

    def items(self) -> Iterator[tuple[str, Any]]:
        """Pares (nome, valor) na ordem da projeção, SEM colapsar repetição."""
        return zip(self._nomes, self, strict=True)

    def __repr__(self) -> str:
        corpo = ", ".join(f"{n}={v!r}" for n, v in self.items())
        return f"Row({corpo})"


def fabrica(nomes: list[str]) -> type[Row]:
    """A classe de linha DESTE resultado, com os nomes fixados na classe."""
    return type(
        "Row",
        (Row,),
        {"__slots__": (), "_nomes": tuple(nomes), "_indices": indices_de(nomes)},
    )
