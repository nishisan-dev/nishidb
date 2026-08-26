"""Servidor HTTP programável, para os testes de unidade.

É o equivalente do `com.sun.net.httpserver` que os testes do driver JDBC usam:
sobe em 127.0.0.1 numa porta EFÊMERA, responde o que o teste mandar responder e
GUARDA as requisições recebidas, para que o corpo enviado possa ser conferido
byte a byte.

Ele prova que o client faz o que o autor ACHA que o servidor emite. A prova
contra o servidor de verdade é outra, e mora nos testes de integração.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

#: Um dataset mínimo, o bastante para `connect()` descobrir os tetos.
META_PADRAO: dict[str, Any] = {
    "datasets": [
        {
            "name": "t",
            "key": "id",
            "maxRows": 1000,
            "timeoutMs": 600000,
            "defaultTimeoutMs": 30000,
            "maxCols": 512,
            "onMaxCols": "ignore",
            "maxNestedCols": 64,
            "columns": [
                {
                    "name": "id",
                    "alias": None,
                    "type": "string",
                    "jdbc": "VARCHAR",
                    "nullable": False,
                    "precision": 0,
                    "scale": 0,
                    "displaySize": 0,
                    "index": "none",
                    "auto": False,
                }
            ],
        }
    ]
}


@dataclass
class Requisicao:
    metodo: str
    caminho: str
    cabecalhos: dict[str, str]
    corpo: bytes


@dataclass
class Resposta:
    """O que o servidor devolve na próxima chamada de um caminho."""

    status: int = 200
    corpo: bytes = b""
    content_type: str = "application/json"
    cabecalhos: dict[str, str] = field(default_factory=dict)
    #: Alternativa a `corpo`: uma fábrica de iterador de pedaços, para emitir
    #: um corpo GRANDE sem materializá-lo. É o que permite ao gate de memória
    #: medir o cliente, e não o servidor de mentira.
    pedacos: Callable[[], Iterator[bytes]] | None = None
    #: Content-Length quando `pedacos` é usado — calculado, nunca medido.
    tamanho: int = 0
    #: Responde com Transfer-Encoding: chunked, como o servidor real faz em
    #: `/v1/query`. Ligado por `responde_query*`; os documentos pequenos
    #: (`/v1/metadata` e cia.) saem com Content-Length, como lá.
    chunked: bool = False


def linhas_ndjson(*docs: Any) -> bytes:
    """Serializa documentos como NDJSON, um por linha."""
    return b"".join(json.dumps(d, separators=(",", ":")).encode("utf-8") + b"\n" for d in docs)


def schema(*colunas: dict[str, Any], dataset: str = "t") -> dict[str, Any]:
    """Uma linha de schema com os sete campos por coluna."""
    return {"nishidb": "schema", "dataset": dataset, "columns": list(colunas)}


def coluna(nome: str, jdbc: str = "VARCHAR", **extras: Any) -> dict[str, Any]:
    base = {
        "name": nome,
        "type": extras.pop("type", "string"),
        "jdbc": jdbc,
        "nullable": extras.pop("nullable", True),
        "precision": extras.pop("precision", 0),
        "scale": extras.pop("scale", 0),
        "displaySize": extras.pop("displaySize", 0),
    }
    base.update(extras)
    return base


def fim(rows: int, truncated: bool = False) -> dict[str, Any]:
    return {"nishidb": "end", "rows": rows, "truncated": truncated}


def erro(code: str, rows: int = 0, message: str = "quebrou") -> dict[str, Any]:
    return {"nishidb": "error", "code": code, "rows": rows, "message": message}


class ServidorFalso:
    """Sobe num thread, em porta efêmera. Use como context manager."""

    def __init__(self) -> None:
        self.requisicoes: list[Requisicao] = []
        #: caminho -> lista de respostas, consumidas em ordem. Esgotada a
        #: lista, a última resposta se repete.
        self.respostas: dict[str, list[Resposta]] = {}
        self.responde("/v1/metadata", Resposta(corpo=json.dumps(META_PADRAO).encode()))

        servidor = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_: Any) -> None:
                pass  # sem ruído no output do teste

            def _atende(self, metodo: str) -> None:
                tamanho = int(self.headers.get("Content-Length") or 0)
                corpo = self.rfile.read(tamanho) if tamanho else b""
                caminho = self.path
                servidor.requisicoes.append(Requisicao(metodo, caminho, dict(self.headers), corpo))
                resp = servidor._proxima(caminho.split("?")[0])
                self.send_response(resp.status)
                self.send_header("Content-Type", resp.content_type)
                for k, v in resp.cabecalhos.items():
                    self.send_header(k, v)

                if resp.chunked:
                    # É assim que o servidor de VERDADE responde /v1/query: ele
                    # faz flush depois do schema e a cada lote, então o Go usa
                    # Transfer-Encoding: chunked e não há Content-Length.
                    #
                    # A diferença não é cosmética. Com Content-Length o
                    # `http.client` fecha a resposta sozinho ao consumir o
                    # último byte; com chunked ele só fecha ao ler o chunk
                    # terminador, que um leitor de linhas nunca alcança. Foi
                    # exatamente essa distância que deixou passar o bug de
                    # reuso de conexão: a suíte inteira verde e a segunda
                    # consulta falhando contra o binário real.
                    self.send_header("Transfer-Encoding", "chunked")
                    self.end_headers()
                    for pedaco in self._corpo(resp):
                        if pedaco:
                            self.wfile.write(f"{len(pedaco):X}\r\n".encode() + pedaco + b"\r\n")
                    self.wfile.write(b"0\r\n\r\n")
                    return

                tamanho = resp.tamanho if resp.pedacos else len(resp.corpo)
                self.send_header("Content-Length", str(tamanho))
                self.end_headers()
                for pedaco in self._corpo(resp):
                    self.wfile.write(pedaco)

            def _corpo(self, resp: Resposta) -> Iterator[bytes]:
                if resp.pedacos is not None:
                    yield from resp.pedacos()
                else:
                    yield resp.corpo

            def do_GET(self) -> None:
                self._atende("GET")

            def do_POST(self) -> None:
                self._atende("POST")

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        # poll curto: o `shutdown()` só retorna no fim de um ciclo de poll, e o
        # default de 0,5 s somaria meio segundo POR TESTE — 30 s numa suíte de
        # 60. A suíte tem de ser rápida o bastante para rodar a cada save.
        self._thread = threading.Thread(
            target=lambda: self._httpd.serve_forever(poll_interval=0.01), daemon=True
        )

    # ------------------------------------------------------------- programação

    def responde(self, caminho: str, *respostas: Resposta) -> None:
        self.respostas[caminho] = list(respostas)

    def responde_query(self, *docs: Any, status: int = 200) -> None:
        """Programa `/v1/query` com um stream NDJSON."""
        self.responde(
            "/v1/query",
            Resposta(
                status=status,
                corpo=linhas_ndjson(*docs),
                content_type="application/x-ndjson",
                chunked=status == 200,
            ),
        )

    def responde_query_repetida(self, schema_doc: Any, linha: dict[str, Any], n: int) -> None:
        """Programa `/v1/query` com `n` cópias da MESMA linha, emitidas uma a
        uma. O corpo nunca existe inteiro em lugar nenhum — nem aqui, nem no
        cliente —, que é o que torna a medida de memória honesta."""
        cabeca = json.dumps(schema_doc, separators=(",", ":")).encode() + b"\n"
        corpo_linha = json.dumps(linha, separators=(",", ":")).encode() + b"\n"
        rodape = json.dumps(fim(n), separators=(",", ":")).encode() + b"\n"

        def pedacos() -> Iterator[bytes]:
            yield cabeca
            # Em blocos, para não pagar uma syscall por linha: o que importa
            # para o teste é que nem o servidor nem o cliente segurem o TOTAL.
            bloco = corpo_linha * 256
            for _ in range(n // 256):
                yield bloco
            yield corpo_linha * (n % 256)
            yield rodape

        self.responde(
            "/v1/query",
            Resposta(
                content_type="application/x-ndjson",
                pedacos=pedacos,
                tamanho=len(cabeca) + len(corpo_linha) * n + len(rodape),
                chunked=True,
            ),
        )

    def responde_query_cru(self, corpo: bytes, status: int = 200) -> None:
        """Programa `/v1/query` com bytes literais — para testar corpo malformado."""
        self.responde(
            "/v1/query",
            Resposta(
                status=status,
                corpo=corpo,
                content_type="application/x-ndjson",
                chunked=status == 200,
            ),
        )

    def responde_erro(
        self, caminho: str, status: int, code: str, message: str = "não deu", **extras: Any
    ) -> None:
        falha: dict[str, Any] = {"code": code, "message": message}
        falha.update(extras)
        self.responde(
            caminho,
            Resposta(status=status, corpo=json.dumps({"error": falha}).encode()),
        )

    def _proxima(self, caminho: str) -> Resposta:
        fila = self.respostas.get(caminho)
        if not fila:
            return Resposta(status=404, corpo=b'{"error":{"code":"UNKNOWN_DATASET","message":"?"}}')
        return fila.pop(0) if len(fila) > 1 else fila[0]

    # ------------------------------------------------------------------ ciclo

    @property
    def dsn(self) -> str:
        host, porta = self._httpd.server_address[:2]
        return f"nishidb://{host}:{porta}/t"

    def ultima(self, caminho: str) -> Requisicao:
        """A última requisição recebida naquele caminho."""
        for r in reversed(self.requisicoes):
            if r.caminho.split("?")[0] == caminho:
                return r
        raise AssertionError(f"nenhuma requisição em {caminho}: {self.requisicoes}")

    def __enter__(self) -> ServidorFalso:
        self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)
