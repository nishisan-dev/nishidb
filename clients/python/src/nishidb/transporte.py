"""O ÚNICO ponto que monta uma requisição HTTP.

As doutrinas abaixo são portadas literalmente de `internal/cli/credential.go` e
de `wire/Transporte.java`. Nenhuma delas é configurável, e isso é a decisão:
uma opção "só para depurar" vira o modo permanente de operação em três semanas.

**Por que `http.client` e não `urllib.request`.** O `urllib` honra
`http_proxy`/`https_proxy` do ambiente, e num host corporativo isso manda uma
consulta a `127.0.0.1` para o proxy da empresa — que responde 403 e faz o
operador culpar o servidor. Foi o que aconteceu no piloto
(`docs/piloto-events.md:142`), e é por isso que `scripts/nishidb-stats.py`
precisou de um `ProxyHandler({})` explícito. O `http.client` **não lê proxy
nenhum do ambiente**, então aqui a armadilha some por construção — não por
lembrança de quem escreveu o código.
"""

from __future__ import annotations

import base64
import contextlib
import http.client
import json
import ssl
from typing import Any, BinaryIO

from . import erros
from .dsn import Destino

#: O texto EXATO que o net/http do Go escreve quando um cliente fala texto puro
#: com uma porta que só aceita TLS: o handshake nunca chega a acontecer e o
#: servidor devolve isto cru — HTTP/1.0, sem JSON — na única oportunidade que
#: tem antes de fechar a conexão.
_HTTP_ONDE_ESPERAVA_HTTPS = b"Client sent an HTTP request to an HTTPS server"


def _contexto_tls(ca: str | None) -> ssl.SSLContext:
    """Contexto TLS. **Não há caminho para desligar a verificação.**

    Nem parâmetro, nem variável de ambiente, nem `insecure=True`. O molde é o
    do cliente Kafka (`internal/kafka/config.go:109`) e o do CLI: um pool com o
    PEM apontado, TLS 1.2 como piso. Quem precisa depurar um handshake usa
    `openssl s_client`.
    """
    ctx = ssl.create_default_context(cafile=ca)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def basic(usuario: str, senha: str) -> str:
    par = f"{usuario}:{senha}".encode()
    return "Basic " + base64.b64encode(par).decode("ascii")


class Transporte:
    """Uma conexão HTTP persistente, uma por `Connection` da PEP 249.

    É o modelo certo: uma conexão de banco executa uma consulta por vez, então
    não há o que multiplexar — e keep-alive poupa o handshake a cada query.
    """

    def __init__(self, destino: Destino, usuario: str | None, senha: str) -> None:
        self._destino = destino
        self._autorizacao = basic(usuario, senha) if usuario else None
        self._conn: http.client.HTTPConnection | None = None
        #: Resposta em voo cujo corpo o chamador ainda está lendo. Enquanto ela
        #: existir, a conexão não pode receber outra requisição.
        self._em_voo: http.client.HTTPResponse | None = None

    # ------------------------------------------------------------- a conexão

    def _abre(self) -> http.client.HTTPConnection:
        d = self._destino
        # O timeout aqui vale para o HANDSHAKE. Ele é retirado do socket logo
        # depois (ver `_envia`), porque um prazo de leitura mataria um stream
        # longo LEGÍTIMO — quem limita a duração de uma consulta é o
        # `query.timeout` do dataset, aplicado no servidor, que é o lado que
        # sabe se ainda há trabalho acontecendo.
        prazo = d.connect_timeout_ms / 1000.0
        if d.tls:
            return http.client.HTTPSConnection(
                d.host, d.port, timeout=prazo, context=_contexto_tls(d.tls_ca)
            )
        return http.client.HTTPConnection(d.host, d.port, timeout=prazo)

    def _conexao(self) -> http.client.HTTPConnection:
        if self._em_voo is not None:
            # O chamador abandonou um stream no meio. A conexão está suja (há
            # bytes de corpo não lidos no fio) e reusá-la faria a resposta
            # seguinte começar no meio da anterior. Derruba e reabre.
            self._descarta()
        if self._conn is None:
            self._conn = self._abre()
        return self._conn

    def _descarta(self) -> None:
        self._em_voo = None
        if self._conn is not None:
            # A falha ao fechar é irrelevante: já decidimos abandonar esta
            # conexão, e o socket morre com ela de qualquer forma.
            with contextlib.suppress(OSError):
                self._conn.close()
            self._conn = None

    # ------------------------------------------------------------ requisições

    def _envia(
        self, metodo: str, caminho: str, corpo: bytes | None, content_type: str | None
    ) -> http.client.HTTPResponse:
        conn = self._conexao()
        cabecalhos: dict[str, str] = {}
        if self._autorizacao is not None:
            # PREEMPTIVO, sem desafio-resposta. Não é micro-otimização: toda
            # falha de autenticação paga 100 ms FIXOS no servidor
            # (`internal/server/auth.go:253`), e o ciclo 401→repete
            # pagaria esse atraso em TODA requisição.
            cabecalhos["Authorization"] = self._autorizacao
        if content_type is not None:
            cabecalhos["Content-Type"] = content_type

        try:
            conn.request(metodo, caminho, body=corpo, headers=cabecalhos)
            resp = conn.getresponse()
        except (OSError, http.client.HTTPException) as e:
            self._descarta()
            d = self._destino
            raise erros.OperationalError(f"falando com {d.host}:{d.port}: {e}") from e

        # A partir daqui o corpo pode demorar o que a consulta demorar.
        if conn.sock is not None:
            conn.sock.settimeout(None)

        if 300 <= resp.status < 400:
            # TODO 3xx é erro, same-host incluído. A política default das
            # bibliotecas remove o Authorization em redirect CROSS-host mas o
            # PRESERVA em same-host; o NishiDB não redireciona, então um 3xx só
            # pode ser configuração errada no meio do caminho (proxy reverso,
            # endereço trocado) — nunca algo a seguir com a credencial a
            # tiracolo.
            destino = resp.getheader("Location", "?")
            resp.read()
            self._descarta()
            raise erros.OperationalError(
                f"o servidor respondeu com redirect ({resp.status}) para {destino} — "
                "o NishiDB não redireciona; verifique o endereço"
            )
        return resp

    def _falha_do_servidor(self, resp: http.client.HTTPResponse) -> erros.Error:
        """Traduz uma resposta de erro que saiu ANTES da primeira linha."""
        bruto = resp.read()
        retry = _retry_after(resp)
        try:
            doc = json.loads(bruto)
            falha = doc["error"]
            codigo = str(falha["code"])
        except (ValueError, KeyError, TypeError):
            return erros.OperationalError(
                f"o servidor respondeu {resp.status} sem corpo legível{_dica(resp.status, bruto)}"
            )
        return erros.de_codigo(
            codigo,
            str(falha.get("message", "")),
            line=falha.get("line"),
            column=falha.get("column"),
            retry_after=retry,
        )

    def consulta(self, corpo: bytes) -> BinaryIO:
        """`POST /v1/query`. Devolve o corpo como STREAM, nunca materializado.

        Ler o corpo inteiro aqui violaria o §128 diretamente: uma consulta pode
        emitir milhões de linhas, e o resultado inteiro em memória é o bug que
        aparece como "o script comeu 8 GB de RAM".
        """
        resp = self._envia("POST", "/v1/query", corpo, "application/json")
        if resp.status != 200:
            falha = self._falha_do_servidor(resp)
            self._descarta()
            raise falha
        self._em_voo = resp
        return resp

    def fim_do_stream(self) -> None:
        """Fecha o corpo em voo, devolvendo a conexão ao estado ocioso.

        **Marcar `None` não basta**, e a falta disto foi bug real: o
        `http.client` só considera a conexão livre quando o objeto de resposta
        está FECHADO. Enquanto ele não estivesse, a requisição seguinte era
        aceita por `request()` e explodia em `getresponse()` com
        `ResponseNotReady` — ou seja, a segunda consulta de uma conexão falhava,
        e o sintoma não apontava para lugar nenhum perto da causa.

        O `read()` antes do `close()` drena o que sobrou. Nesta rota não sobra
        nada — só se chega aqui depois da linha de fecho, que é a última do
        corpo —, mas é ele que garante que um byte de folga não fique no socket
        para contaminar a resposta seguinte. Quem abandona um stream no MEIO
        não passa por aqui: cai no `_descarta()` de `_conexao()`, que derruba a
        conexão em vez de tentar aproveitá-la.
        """
        resp, self._em_voo = self._em_voo, None
        if resp is None:
            return
        try:
            resp.read()
            resp.close()
        except (OSError, http.client.HTTPException):
            self._descarta()

    def documento(self, caminho: str) -> Any:
        """GET de um documento pequeno (`/v1/metadata`, `/v1/stats`, `/v1/health`).

        Estes têm tamanho limitado pelo número de datasets e colunas, então
        materializá-los é correto — ao contrário do resultado de uma consulta.
        """
        resp = self._envia("GET", caminho, None, None)
        # 503 NÃO é erro em /v1/health: é A resposta, e o estado está no corpo.
        # Tratá-lo como falha de transporte esconderia justamente o diagnóstico
        # que a rota existe para dar.
        if resp.status != 200 and not (resp.status == 503 and caminho.startswith("/v1/health")):
            falha = self._falha_do_servidor(resp)
            self._descarta()
            raise falha
        bruto = resp.read()
        try:
            return json.loads(bruto)
        except ValueError as e:
            raise erros.OperationalError(f"{caminho} devolveu corpo ilegível: {e}") from e

    def close(self) -> None:
        self._descarta()


def _retry_after(resp: http.client.HTTPResponse) -> float | None:
    bruto = resp.getheader("Retry-After")
    if not bruto:
        return None
    try:
        return float(bruto)
    except ValueError:
        # A RFC também permite uma data HTTP. Não a interpretamos: o servidor
        # só emite segundos, e um parser de data aqui seria código sem
        # chamador. None diz "não sei", que é honesto.
        return None


def _dica(status: int, corpo: bytes) -> str:
    """Troca "400 sem corpo legível" — mensagem sem pista nenhuma — pela causa
    mais provável: a porta fala TLS e faltou `tls=true` na string de conexão.
    Espelha `schemaHint` de `internal/cli/query_remote.go:110`."""
    if status == 400 and _HTTP_ONDE_ESPERAVA_HTTPS in corpo:
        return "\n  a porta parece falar TLS: tente nishidb://host:porta/dataset?tls=true"
    return ""
