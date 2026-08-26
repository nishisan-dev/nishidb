"""Parse da string de conexão `nishidb://host[:porta]/dataset[?opções]`.

Espelha NishiUrl.java, com UMA divergência deliberada, marcada no lugar: aqui
`?password=` é RECUSADO. O JDBC documenta que não aceita senha na URL, mas o
fallback de `ConnectionConfig.escolhe()` para o mapa da query string faz com
que na prática ela funcione. Uma string de conexão acaba em log de aplicação,
histórico de shell e arquivo de configuração versionado; a senha entra por
argumento de `connect()`, por NISHIDB_PASSWORD ou por arquivo (§95).
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, unquote, urlsplit

from . import erros

ESQUEMA = "nishidb"

#: A mesma do §70 e do driver JDBC. Um client que escolhesse outra faria o
#: mesmo host responder em portas diferentes conforme a linguagem.
PORTA_PADRAO = 9142

#: As opções que a query string aceita. Opção DESCONHECIDA é ignorada; valor
#: inválido numa opção NOSSA é recusado. A assimetria é a do JDBC e é
#: deliberada: tolerar o que não entendemos, nunca adivinhar o que entendemos.
OPCOES = frozenset({"tls", "tls_ca", "user", "connect_timeout_ms"})

#: Recusadas por nome, para que a mensagem ensine em vez de ignorar em silêncio.
PROIBIDAS = frozenset({"password", "passwd", "pwd"})


@dataclass(frozen=True)
class Destino:
    """O que uma string de conexão determina."""

    host: str
    port: int
    dataset: str
    tls: bool = False
    tls_ca: str | None = None
    user: str | None = None
    connect_timeout_ms: int = 10_000

    @property
    def base(self) -> str:
        """`https://host:porta` ou `http://host:porta`, sem barra final."""
        esquema = "https" if self.tls else "http"
        return f"{esquema}://{self.host}:{self.port}"


def _booleano(nome: str, bruto: str) -> bool:
    """`true`/`false` e nada mais.

    Um valor estranho NÃO vira False silencioso: `tls=1` seria lido como
    "sem TLS" e a senha sairia em texto claro na rede sem ninguém reclamar.
    """
    if bruto == "true":
        return True
    if bruto == "false":
        return False
    raise erros.InterfaceError(
        f"opção {nome}={bruto!r} não é válida: use exatamente 'true' ou 'false'"
    )


def _inteiro(nome: str, bruto: str, minimo: int) -> int:
    try:
        n = int(bruto)
    except ValueError:
        raise erros.InterfaceError(f"opção {nome}={bruto!r} não é um inteiro") from None
    if n < minimo:
        raise erros.InterfaceError(f"opção {nome}={n} tem de ser >= {minimo}")
    return n


def _hospedeiro(netloc: str) -> tuple[str, int]:
    """Separa host e porta, tratando o IPv6 entre colchetes.

    O host de um IPv6 volta COM os colchetes, porque é assim que ele entra numa
    URL e num header Host — tirá-los aqui obrigaria a recolocá-los em todo
    ponto de uso.
    """
    if "@" in netloc:
        # userinfo embutido. Recusado, não ignorado: credencial em URL é
        # exatamente o que o §95 evita, e ignorá-la faria a conexão sair
        # ANÔNIMA sem ninguém notar.
        raise erros.InterfaceError(
            "credencial embutida na URL não é aceita — passe user=/password= a connect(), "
            "ou use a variável NISHIDB_PASSWORD"
        )

    if netloc.startswith("["):
        fim = netloc.find("]")
        if fim < 0:
            raise erros.InterfaceError(f"endereço IPv6 sem ']' de fecho: {netloc!r}")
        host = netloc[: fim + 1]
        resto = netloc[fim + 1 :]
        if not resto:
            return host, PORTA_PADRAO
        if not resto.startswith(":"):
            raise erros.InterfaceError(f"lixo depois do endereço IPv6: {resto!r}")
        porta_bruta = resto[1:]
    elif ":" in netloc:
        host, _, porta_bruta = netloc.partition(":")
    else:
        return netloc, PORTA_PADRAO

    if not host:
        raise erros.InterfaceError("a URL não nomeia o host")
    porta = _inteiro("porta", porta_bruta, 1)
    if porta > 65535:
        raise erros.InterfaceError(f"porta {porta} está fora de [1, 65535]")
    return host, porta


def parse(url: str) -> Destino:
    """Converte a string de conexão num Destino, ou levanta InterfaceError."""
    if not url.startswith(f"{ESQUEMA}://"):
        raise erros.InterfaceError(
            f"a string de conexão tem de começar por {ESQUEMA}://, e veio {url!r}"
        )

    partes = urlsplit(url)
    host, porta = _hospedeiro(partes.netloc)

    # Exatamente UM segmento de caminho: não há catálogo nem schema no NishiDB,
    # e aceitar /a/b faria o segundo sumir sem aviso.
    caminho = partes.path.strip("/")
    if not caminho:
        raise erros.InterfaceError(
            "a URL não nomeia o dataset — use nishidb://host:porta/<dataset>"
        )
    if "/" in caminho:
        raise erros.InterfaceError(
            f"a URL nomeia mais de um segmento ({caminho!r}); o NishiDB tem só o dataset"
        )
    dataset = unquote(caminho)

    tls = False
    tls_ca: str | None = None
    user: str | None = None
    connect_timeout_ms = 10_000

    # keep_blank_values: `?tls=` tem de virar erro de valor, não sumir.
    for chave, valor in parse_qsl(partes.query, keep_blank_values=True):
        if chave in PROIBIDAS:
            raise erros.InterfaceError(
                f"a opção {chave!r} não é aceita na URL — uma string de conexão acaba em log, "
                "histórico de shell e arquivo versionado; passe password= a connect() ou use "
                "a variável NISHIDB_PASSWORD"
            )
        if chave not in OPCOES:
            continue  # desconhecida: ignorada de propósito (ver OPCOES)
        if chave == "tls":
            tls = _booleano("tls", valor)
        elif chave == "tls_ca":
            tls_ca = valor
        elif chave == "user":
            user = valor
        elif chave == "connect_timeout_ms":
            connect_timeout_ms = _inteiro("connect_timeout_ms", valor, 1)

    if tls_ca is not None and not tls:
        # Sem esta recusa, apontar a CA e esquecer `tls=true` conectaria em
        # texto claro com a sensação de ter configurado TLS.
        raise erros.InterfaceError("tls_ca exige tls=true")

    return Destino(
        host=host,
        port=porta,
        dataset=dataset,
        tls=tls,
        tls_ca=tls_ca,
        user=user,
        connect_timeout_ms=connect_timeout_ms,
    )
