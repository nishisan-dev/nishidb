"""Os corpos de `/v1/metadata`, `/v1/stats` e `/v1/health`.

Espelham `internal/protocolo/documentos.go`. Todo campo é lido com `.get` e um
default: um servidor MAIS VELHO que ainda não publique um campo aditivo tem de
continuar utilizável, e um MAIS NOVO que publique campos que este client não
conhece não pode quebrá-lo. É a mesma tolerância que o Go pratica ao decodificar
sem `DisallowUnknownFields` do lado do cliente.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# O vocabulário de estado do §94. As quatro constantes existem para que quem
# precise distinguir DEGRADED de DOWN não redeclare a string.
UP = "UP"
DOWN = "DOWN"
DEGRADED = "DEGRADED"
DISABLED = "DISABLED"


@dataclass(frozen=True)
class ColunaMeta:
    """Uma coluna do dataset, como `/v1/metadata` a descreve.

    ATENÇÃO ao `name`: aqui ele é o nome do CAMPO, e o alias vem à parte. Já a
    linha de schema de `/v1/query` traz o nome de SAÍDA (o alias, quando há um),
    porque é ele que é chave do objeto JSON de cada row. Para um campo `status`
    com alias `estado`, esta rota diz `status` e o `description` de `SELECT *`
    diz `estado` — **casar os dois por igualdade de nome não funciona**.
    """

    name: str
    alias: str | None
    type: str
    jdbc: str
    nullable: bool
    precision: int
    scale: int
    display_size: int
    #: O índice DECLARADO no YAML ("none", "bitmap", "hash"), não o plano
    #: escolhido. Quem quer o plano olha `_meta.fields.indexed`.
    index: str
    #: Campo sem tipo declarado. Enquanto nenhum evento o fixa, `type` é
    #: "unknown" e `jdbc` é "OTHER".
    auto: bool

    @classmethod
    def de_json(cls, d: dict[str, Any]) -> ColunaMeta:
        alias = d.get("alias")
        return cls(
            name=str(d.get("name", "")),
            alias=str(alias) if alias is not None else None,
            type=str(d.get("type", "unknown")),
            jdbc=str(d.get("jdbc", "OTHER")),
            nullable=bool(d.get("nullable", True)),
            precision=int(d.get("precision", 0)),
            scale=int(d.get("scale", 0)),
            display_size=int(d.get("displaySize", 0)),
            index=str(d.get("index", "none")),
            auto=bool(d.get("auto", False)),
        )


@dataclass(frozen=True)
class DatasetMeta:
    """Um dataset consultável, com os limites EFETIVOS (o do dataset, ou o
    default global que ele herdou) — não o que está escrito no YAML dele."""

    name: str
    key: str
    max_rows: int
    #: O TETO (`query.maxTimeout`), e não o prazo padrão. É contra ele que o
    #: `timeout_ms` de uma consulta é aparado, porque o servidor RECUSA um
    #: pedido acima do teto.
    timeout_ms: int
    #: O prazo que vale para a consulta que NÃO manda `timeout_ms`
    #: (`query.timeout`). Separado do teto: sem ele, "quanto tempo esta
    #: consulta tem?" não teria resposta única.
    default_timeout_ms: int
    max_cols: int
    on_max_cols: str
    max_nested_cols: int
    columns: list[ColunaMeta] = field(default_factory=list)

    @classmethod
    def de_json(cls, d: dict[str, Any]) -> DatasetMeta:
        colunas = d.get("columns") or []
        return cls(
            name=str(d.get("name", "")),
            key=str(d.get("key", "")),
            max_rows=int(d.get("maxRows", 0)),
            timeout_ms=int(d.get("timeoutMs", 0)),
            default_timeout_ms=int(d.get("defaultTimeoutMs", 0)),
            max_cols=int(d.get("maxCols", 0)),
            on_max_cols=str(d.get("onMaxCols", "")),
            max_nested_cols=int(d.get("maxNestedCols", 0)),
            columns=[ColunaMeta.de_json(c) for c in colunas if isinstance(c, dict)],
        )


def datasets_de(doc: Any) -> list[DatasetMeta]:
    """Lê o corpo de `/v1/metadata`."""
    if not isinstance(doc, dict):
        return []
    brutos = doc.get("datasets") or []
    return [DatasetMeta.de_json(d) for d in brutos if isinstance(d, dict)]


@dataclass(frozen=True)
class Saude:
    """O corpo de `/v1/health` (§94).

    Sai com status HTTP 503 quando `status != "UP"`, para que um load balancer
    não precise ler o corpo — e é por isso que o transporte trata o 503 desta
    rota como resposta, não como falha.
    """

    status: str
    kafka: str
    storage: str
    datasets: dict[str, str] = field(default_factory=dict)
    #: Nomeia o problema POR DATASET. Não está no §94 e existe porque "DOWN"
    #: sozinho manda o operador procurar em qual dos datasets, num processo que
    #: pode hospedar vários.
    detalhes: dict[str, str] = field(default_factory=dict)

    @property
    def up(self) -> bool:
        return self.status == UP

    @classmethod
    def de_json(cls, d: Any) -> Saude:
        if not isinstance(d, dict):
            return cls(status=DOWN, kafka=DOWN, storage=DOWN)
        return cls(
            status=str(d.get("status", DOWN)),
            kafka=str(d.get("kafka", DISABLED)),
            storage=str(d.get("storage", DOWN)),
            datasets=dict(d.get("datasets") or {}),
            detalhes=dict(d.get("detalhes") or {}),
        )


@dataclass(frozen=True)
class ConsultaEmVoo:
    """Uma consulta em andamento.

    `sql_hash`, e não o SQL: o texto pode carregar literais com dado do
    cliente, e o §95 proíbe publicar payload por default.
    """

    dataset: str
    sql_hash: str
    running_ms: int
    #: Se ocupa uma vaga do SUB-POOL de consulta longa — o que separa "está
    #: demorando" de "foi autorizada a demorar".
    long: bool


@dataclass(frozen=True)
class Estatisticas:
    """O corpo de `/v1/stats`.

    Tipamos o que tem consumidor — a versão do processo e as consultas em voo —
    e preservamos o resto em `bruto`. Espelhar em dataclass as ~40 métricas de
    storage/schema/ingestão dobraria a manutenção (todo contador novo no
    servidor exigiria release deste pacote) para entregar o que um `dict` já
    entrega: elas são números para ler, não estruturas sobre as quais o client
    decide algo. `/metrics` continua sendo o formato certo para histograma.
    """

    version: str
    in_flight: list[ConsultaEmVoo] = field(default_factory=list)
    bruto: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def de_json(cls, d: Any) -> Estatisticas:
        if not isinstance(d, dict):
            return cls(version="", bruto={})
        processo = d.get("process") or {}
        voo = processo.get("inFlight") or []
        return cls(
            version=str(processo.get("version", "")),
            in_flight=[
                ConsultaEmVoo(
                    dataset=str(c.get("dataset", "")),
                    sql_hash=str(c.get("sqlHash", "")),
                    running_ms=int(c.get("runningMs", 0)),
                    long=bool(c.get("long", False)),
                )
                for c in voo
                if isinstance(c, dict)
            ],
            bruto=d,
        )
