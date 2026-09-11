"""Conversão entre os valores do fio e os do Python, nos dois sentidos.

**A conversão de LEITURA é dirigida pelo token JSON; o tipo declarado só entra
onde o token é ambíguo.** Não é invenção: é a lição que a execução do driver
JDBC pagou com bug. Texto que é instante, texto que é hora do dia e texto que é
base64 têm exatamente a mesma cara no JSON, e só o tipo da coluna os separa.
Fora desses três, o token manda — inclusive na coluna OTHER, cujo metadado pode
discordar do dado (é a coluna `auto` que o dataset ainda não fixou, e a
ingestão é concorrente).

**Quatro nulos do fio NÃO são NULL** e não são distinguíveis aqui: double
não-finito (NaN/Inf saem como `null`), `time` fora do dia, `json` vazio e valor
fisicamente malformado. Quem procurar a causa tem de olhar o dado, não o
client.
"""

from __future__ import annotations

import base64
import binascii
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any

from . import erros

# ------------------------------------------------------------ nomes JDBC do §73
#
# Os literais de internal/protocol/jdbc.go. O client NÃO redeclara larguras
# nem precisão: elas vêm prontas na linha de schema, e é por isso que um tipo
# novo no servidor não exige release deste pacote.

VARCHAR = "VARCHAR"
BIGINT = "BIGINT"
DOUBLE = "DOUBLE"
BOOLEAN = "BOOLEAN"
TIMESTAMP = "TIMESTAMP"
VARBINARY = "VARBINARY"
TIME = "TIME"
OTHER = "OTHER"


class TipoDBAPI:
    """Objeto de tipo da PEP 249.

    A PEP exige que o `type_code` de `Cursor.description` compare igual a um
    destes. Como o nosso `type_code` é o nome JDBC que veio do servidor, a
    comparação é contra um conjunto de nomes.
    """

    def __init__(self, nome: str, *valores: str) -> None:
        self._nome = nome
        self.valores = frozenset(valores)

    def __eq__(self, outro: object) -> bool:
        if isinstance(outro, TipoDBAPI):
            return self.valores == outro.valores
        return outro in self.valores

    def __hash__(self) -> int:
        return hash(self.valores)

    def __repr__(self) -> str:
        return f"<{self._nome}>"


STRING = TipoDBAPI("STRING", VARCHAR)
BINARY = TipoDBAPI("BINARY", VARBINARY)
NUMBER = TipoDBAPI("NUMBER", BIGINT, DOUBLE)
DATETIME = TipoDBAPI("DATETIME", TIMESTAMP, TIME)
#: O NishiDB não tem rowid consultável. Declarado porque a PEP 249 manda o
#: módulo expor o nome, e um `except`/comparação em código portável tem de
#: funcionar; o conjunto é vazio, então nada casa com ele.
ROWID = TipoDBAPI("ROWID")


# --------------------------------------------------------------------- temporais

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _fracao_para_micros(digitos: str) -> int:
    """Normaliza de 0 a 9 dígitos de fração para microssegundos.

    O encoder emite SEMPRE 3 (`append3`), mas aceitar a faixa inteira é o que o
    `Temporais.java` faz e o que impede um formato futuro de quebrar o client.
    """
    if not digitos:
        return 0
    return int((digitos + "000000")[:6])


def para_timestamp(texto: str) -> datetime:
    """`2026-08-23T21:53:04.123Z` -> datetime AWARE em UTC.

    O valor **é um instante**, e é assim que ele é devolvido: com `tzinfo=utc`,
    preservando-o exatamente e deixando o usuário renderizar no fuso que
    quiser. Devolver naive obrigaria a adivinhar um fuso que o dado não tem.

    O `Z` é EXIGIDO — o encoder só emite UTC, e aceitar um offset explícito
    faria o client acolher um formato que o servidor não produz.
    """
    bruto = texto
    if not bruto.endswith("Z"):
        raise erros.DataError(
            f"timestamp {texto!r} não termina em 'Z': o NishiDB só emite instantes em UTC"
        )
    corpo = bruto[:-1]
    data, sep, hora = corpo.partition("T")
    if not sep:
        raise erros.DataError(f"timestamp {texto!r} não tem o separador 'T'")

    segundos, ponto, fracao = hora.partition(".")
    if ponto and (not fracao.isdigit() or len(fracao) > 9):
        raise erros.DataError(f"timestamp {texto!r} tem fração de segundo inválida")

    try:
        ano, mes, dia = (int(p) for p in data.split("-"))
        h, m, s = (int(p) for p in segundos.split(":"))
        return datetime(ano, mes, dia, h, m, s, _fracao_para_micros(fracao), tzinfo=timezone.utc)
    except ValueError as e:
        # Inclui o ano fora de [1, 9999], que o datetime do Python não
        # representa. Falhar aqui é melhor que devolver um instante errado.
        raise erros.DataError(f"timestamp {texto!r} não é uma data/hora válida: {e}") from e


def para_time(texto: str) -> time:
    """`23:59:59.999` -> datetime.time NAIVE.

    Hora do dia **não é instante**: não há data e não há fuso, e carimbar um
    seria inventar dado. Sem `Z`, e a ausência dele é o que separa este caso do
    timestamp no fio — a gramática, não o tipo declarado.
    """
    segundos, ponto, fracao = texto.partition(".")
    if ponto and (not fracao.isdigit() or len(fracao) > 9):
        raise erros.DataError(f"time {texto!r} tem fração de segundo inválida")
    try:
        h, m, s = (int(p) for p in segundos.split(":"))
        return time(h, m, s, _fracao_para_micros(fracao))
    except ValueError as e:
        raise erros.DataError(f"time {texto!r} não é uma hora válida: {e}") from e


def _para_bytes(texto: str) -> bytes:
    """Base64 StdEncoding com padding, espelhando `base64.StdEncoding` do Go."""
    try:
        return base64.b64decode(texto, validate=True)
    except (binascii.Error, ValueError) as e:
        # `validate=True` é o que faz lixo à direita virar erro em vez de um
        # array truncado em silêncio.
        raise erros.DataError(f"coluna binária não é base64 válido: {e}") from e


# ------------------------------------------------------------------ fio -> Python


def do_fio(valor: Any, jdbc: str) -> Any:
    """Converte UM valor já decodificado do JSON, guiado pelo tipo da coluna.

    `value` vem do `json.loads`, então já é str/int/float/bool/None/dict/list.
    O que falta é só o que o JSON não sabe expressar.
    """
    if valor is None:
        return None

    if jdbc == TIMESTAMP:
        if isinstance(valor, str):
            return para_timestamp(valor)
        raise erros.DataError(f"coluna TIMESTAMP trouxe {type(valor).__name__}, não texto")

    if jdbc == TIME:
        if isinstance(valor, str):
            return para_time(valor)
        raise erros.DataError(f"coluna TIME trouxe {type(valor).__name__}, não texto")

    if jdbc == VARBINARY:
        # Só aqui o base64 é decodificado. A convenção do JDBC mandaria
        # decodificar qualquer texto que pareça base64, e o resultado seria uma
        # coluna `bytes` ainda não fixada (que chega anunciada OTHER) ora
        # devolvendo o binário, ora os caracteres — conforme o dia.
        if isinstance(valor, str):
            return _para_bytes(valor)
        raise erros.DataError(f"coluna VARBINARY trouxe {type(valor).__name__}, não texto")

    if jdbc == DOUBLE:
        # O encoder emite um double de valor inteiro como `1`, e o json.loads
        # devolve int. Sem esta coerção a MESMA coluna teria dtype variável de
        # linha para linha — que é o que quebra um DataFrame. É a segunda
        # exceção nomeada da regra "o token manda": aqui o tipo declarado
        # resolve uma ambiguidade que o token não resolve.
        if isinstance(valor, bool):
            raise erros.DataError("coluna DOUBLE trouxe um booleano")
        if isinstance(valor, (int, float)):
            return float(valor)
        raise erros.DataError(f"coluna DOUBLE trouxe {type(valor).__name__}")

    if jdbc == BIGINT:
        # bool é subclasse de int no Python: sem esta guarda, `True` passaria
        # como 1 numa coluna BIGINT.
        if isinstance(valor, bool):
            raise erros.DataError("coluna BIGINT trouxe um booleano")
        if isinstance(valor, int):
            return valor
        raise erros.DataError(f"coluna BIGINT trouxe {type(valor).__name__}")

    # VARCHAR, OTHER e qualquer nome que este client ainda não conheça: o token
    # manda, verbatim. É o que faz um tipo NOVO no servidor não quebrar o
    # cursor — ele chega como o Python o representa e o usuário decide.
    #
    # A coluna `json` cai aqui e já é dict/list, porque o encoder a emite
    # INLINE no NDJSON, não como string escapada.
    return valor


# ------------------------------------------------------------------ Python -> fio

#: Os quatro tipos de wire de `protocol.Param.Type`. A lista é curta
#: porque é o contrato de `binder.coerceParam`: endereços, prefixos e temporais
#: viajam como string ou inteiro e são convertidos pela MESMA tabela dos
#: literais SQL.
TIPO_STRING = "string"
TIPO_INT64 = "int64"
TIPO_DOUBLE = "double"
TIPO_BOOLEAN = "boolean"

_MIN_INT64 = -(2**63)
_MAX_INT64 = 2**63 - 1


def _millis_do_instante(valor: datetime, indice: int) -> int:
    if valor.tzinfo is None or valor.utcoffset() is None:
        # Recusa ALTA, e não "assume UTC" nem "assume o fuso local". O
        # `.timestamp()` de um datetime naive usa o fuso da MÁQUINA, e o
        # parâmetro sairia deslocado por horas sem nada acusar — o mesmo erro
        # que o driver JDBC evitou recusando LocalDateTime.
        raise erros.DataError(
            f"parâmetro {indice}: datetime sem fuso não identifica um instante — "
            "use datetime(..., tzinfo=timezone.utc) ou .astimezone()"
        )
    return int((valor - _EPOCH) // timedelta(milliseconds=1))


def para_parametro(valor: Any, indice: int) -> dict[str, Any]:
    """Converte um valor do Python no `{"type": ..., "value": ...}` do wire.

    `indice` é 1-based, como o binder do servidor conta — a mensagem de erro
    tem de nomear o mesmo número que o usuário escreveu.
    """
    if valor is None:
        # Não existe sintaxe de parâmetro nulo no wire, e o servidor recusa
        # `"value": null` explicando isto mesmo. Recusar aqui, nomeando a
        # alternativa, é melhor que deixar a viagem acontecer para receber a
        # mesma recusa mais tarde.
        raise erros.NotSupportedError(
            f"parâmetro {indice}: o protocolo não tem parâmetro nulo — "
            "escreva `IS NULL` / `IS NOT NULL` na própria query"
        )

    # bool ANTES de int: bool é subclasse de int no Python, e a ordem invertida
    # mandaria True como o inteiro 1.
    if isinstance(valor, bool):
        return {"type": TIPO_BOOLEAN, "value": valor}

    if isinstance(valor, int):
        if not _MIN_INT64 <= valor <= _MAX_INT64:
            raise erros.DataError(f"parâmetro {indice}: {valor} não cabe num int64 do wire")
        return {"type": TIPO_INT64, "value": valor}

    if isinstance(valor, float):
        if valor != valor or valor in (float("inf"), float("-inf")):
            # NaN e infinito não são representáveis em JSON, e o encoder do
            # servidor os emite como `null` na leitura. Deixá-los sair daqui
            # produziria um corpo que não é JSON válido.
            raise erros.DataError(f"parâmetro {indice}: {valor} não é representável em JSON")
        return {"type": TIPO_DOUBLE, "value": valor}

    if isinstance(valor, Decimal):
        # Inteiro exato vai como int64 para não perder precisão no caminho;
        # o resto vira double, com a perda que o tipo do wire impõe.
        if valor == valor.to_integral_value():
            return para_parametro(int(valor), indice)
        return para_parametro(float(valor), indice)

    if isinstance(valor, datetime):
        # Epoch millis, NUNCA ISO-8601. Verificado em binder/coerce.go: uma
        # string contra coluna `datetime` é interpretada no `format:` DECLARADO
        # do campo, resolvido no fuso do host do SERVIDOR — um parâmetro em
        # ISO-8601 funcionaria para `timestamp` e falharia para `datetime`.
        return {"type": TIPO_INT64, "value": _millis_do_instante(valor, indice)}

    if isinstance(valor, time):
        if valor.tzinfo is not None:
            raise erros.DataError(
                f"parâmetro {indice}: hora do dia com fuso não tem significado no NishiDB"
            )
        millis = (
            valor.hour * 3_600_000
            + valor.minute * 60_000
            + valor.second * 1_000
            + valor.microsecond // 1_000
        )
        return {"type": TIPO_INT64, "value": millis}

    if isinstance(valor, str):
        # Endereços, prefixos e temporais em texto também caem aqui, e é o
        # certo: o servidor os converte pela mesma tabela dos literais SQL.
        return {"type": TIPO_STRING, "value": valor}

    if isinstance(valor, (bytes, bytearray, memoryview)):
        raise erros.NotSupportedError(
            f"parâmetro {indice}: o binder não compara coluna binária, "
            "então não há parâmetro de bytes"
        )

    raise erros.DataError(
        f"parâmetro {indice}: {type(valor).__name__} não tem tipo correspondente no wire"
    )
