"""O `Cursor` da PEP 249, sobre o stream NDJSON.

Streaming de verdade: `fetchone`, `fetchmany` e a iteração consomem o corpo
**uma linha por vez**. Só `fetchall` materializa — que é o que o método
significa. Ler o corpo inteiro em `execute()` violaria o §128 e é o bug que
aparece como "o script comeu 8 GB de RAM".
"""

from __future__ import annotations

import json
import warnings
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from . import erros, tipos
from . import leitor as _leitor
from . import sql as _sql
from .linha import Row, fabrica

if TYPE_CHECKING:  # pragma: no cover
    from .conexao import Connection

#: A tupla de `Cursor.description` da PEP 249 tem sete campos, e a linha de
#: schema publica exatamente os sete. `internal_size` é o único sem
#: correspondente no wire: ele mediria bytes em disco, que o NishiDB não expõe
#: por coluna — None é a resposta honesta, e a PEP a admite.
_SEM_TAMANHO_INTERNO = None


class Cursor:
    """Um resultado por vez, para frente apenas."""

    def __init__(self, conexao: Connection) -> None:
        self._conexao: Connection | None = conexao
        self._leitor: _leitor.Leitor | None = None
        self._gerador: Iterator[bytes] | None = None
        self._pendente: bytes | None = None
        self._description: list[tuple[Any, ...]] | None = None
        self._nomes: list[str] = []
        self._linha_cls: type[Row] = Row
        self._jdbc: list[str] = []
        self._rowcount = -1
        #: A falha fica GRUDADA. Quem engolir a exceção e chamar `fetchone()`
        #: de novo recebe a mesma exceção, nunca `None` — devolver None faria
        #: um resultado cortado no meio parecer um resultado que terminou bem.
        self._falha: BaseException | None = None
        self._fechado = False

        #: Teto de linhas desta consulta. 0 (ou acima do teto do dataset) faz o
        #: campo ser OMITIDO do corpo — ver `_max_rows_para_o_fio`.
        self.max_rows = 0
        #: Prazo desta consulta, em ms. 0 deixa valer o `query.timeout` do
        #: dataset. É aparado contra o teto porque o servidor RECUSA acima dele.
        self.timeout_ms = 0
        #: Dica de `fetchmany()`. Não vira nada no fio: o lote em que o
        #: servidor descarrega o buffer é `query.resultBatchSize`, configuração
        #: DELE, invisível para o cliente.
        self.arraysize = 1

    # ------------------------------------------------------------- protocolo

    @property
    def description(self) -> list[tuple[Any, ...]] | None:
        """As sete tuplas por coluna, direto da linha de schema — o client não
        recalcula nenhum dos números."""
        return self._description

    @property
    def rowcount(self) -> int:
        """-1 enquanto o stream corre; a contagem real depois do fecho.

        A PEP 249 autoriza -1 para "não determinável", e antes do fecho ele é
        genuinamente desconhecido: o servidor emite as linhas sem saber quantas
        virão.
        """
        return self._rowcount

    @property
    def connection(self) -> Connection | None:
        return self._conexao

    # --------------------------------------------------------------- execute

    def _exige_aberto(self) -> Connection:
        if self._fechado or self._conexao is None:
            raise erros.InterfaceError("o cursor está fechado")
        return self._conexao.exige_aberta()

    def _max_rows_para_o_fio(self, teto: int) -> int | None:
        """Omite o campo quando pedir não acrescenta nada.

        O servidor RECUSA `maxRows` acima do teto do dataset com
        MAX_ROWS_ABOVE_LIMIT em vez de aparar, e recusa `maxRows <= 0` com
        BAD_REQUEST — ele nunca apara em silêncio. Como o teto do dataset já
        satisfaz "no máximo n" quando n é maior que ele, omitir responde a
        pergunta sem virar erro na cara do usuário.
        """
        if self.max_rows <= 0:
            return None
        if teto > 0 and self.max_rows >= teto:
            return None
        return self.max_rows

    def _timeout_para_o_fio(self, teto: int) -> int | None:
        if self.timeout_ms <= 0:
            return None
        if teto > 0:
            return min(self.timeout_ms, teto)
        return self.timeout_ms

    def _parametros(self, valores: Any, esperados: int) -> list[dict[str, Any]]:
        if valores is None:
            valores = ()
        if isinstance(valores, dict):
            raise erros.NotSupportedError(
                "o NishiDB usa parâmetros POSICIONAIS (paramstyle 'qmark'): "
                "passe uma sequência, não um dict"
            )
        if isinstance(valores, (str, bytes)):
            raise erros.InterfaceError(
                "os parâmetros têm de vir numa sequência — uma string sozinha "
                "seria lida como uma sequência de caracteres"
            )
        seq = list(valores)
        if len(seq) != esperados:
            # Recusa no CLIENTE, com o número 1-based que o binder usa. O
            # servidor recusaria também, mas depois de uma viagem de rede e sem
            # poder dizer quantos o texto tem.
            raise erros.InterfaceError(
                f"a query tem {esperados} parâmetro(s) '?' e vieram {len(seq)}"
            )
        return [tipos.para_parametro(v, i + 1) for i, v in enumerate(seq)]

    def execute(
        self, operation: str, parameters: Any = None, *, dataset: str | None = None
    ) -> Cursor:
        """Executa a consulta.

        `dataset` é extensão à PEP 249 e existe para UM caso real: o namespace
        `_meta.fields` é uma tabela virtual sobre "este dataset", e o nome dele
        não aparece no `FROM` nem existe como coluna para filtrar. Sem o
        override, `_meta.fields` só descreveria o dataset da conexão.
        """
        conexao = self._exige_aberto()
        if not isinstance(operation, str):
            raise erros.InterfaceError("a query tem de ser uma string")
        self._encerra_resultado()

        varredura = _sql.varre(operation)
        # Explícito vence o FROM, que vence o da conexão. O palpite errado
        # NUNCA devolve dado de outra tabela: o servidor recusa no bind quando
        # os dois divergem — é isso que torna o atalho aceitável.
        dataset = dataset or varredura.dataset or conexao.dataset

        corpo: dict[str, Any] = {"dataset": dataset, "sql": operation}
        params = self._parametros(parameters, varredura.parametros)
        if params:
            corpo["parameters"] = params
        teto_linhas, teto_prazo = conexao.tetos_de(dataset)
        n = self._max_rows_para_o_fio(teto_linhas)
        if n is not None:
            corpo["maxRows"] = n
        prazo = self._timeout_para_o_fio(teto_prazo)
        if prazo is not None:
            corpo["timeoutMs"] = prazo

        # Exatamente estes campos e nenhum outro: o servidor decodifica com
        # DisallowUnknownFields, e uma chave a mais é 400 BAD_REQUEST.
        bruto = json.dumps(corpo, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

        stream = conexao.transporte.consulta(bruto)
        self._leitor = _leitor.Leitor(stream)
        self._gerador = self._leitor.linhas()
        self._rowcount = -1
        self._falha = None
        self._primeira_linha()
        return self

    def executemany(self, operation: str, seq_of_parameters: Any) -> None:
        raise erros.NotSupportedError(
            "o NishiDB não aceita escrita por SQL: os dados entram por ingestão "
            "(Kafka ou `nishidb ingest`), e executemany() não teria o que executar"
        )

    def _primeira_linha(self) -> None:
        """Avança até o schema estar lido.

        O servidor faz flush logo depois da linha de schema exatamente para que
        isto não espere o primeiro lote de linhas — `description` fica pronto
        assim que `execute()` volta, e um erro precoce aparece aqui.
        """
        try:
            self._pendente = next(self._gerador)  # type: ignore[arg-type]
        except StopIteration:
            self._pendente = None
            self._desfecho()
        except BaseException as e:
            self._falha = e
            raise
        self._monta_description()

    def _monta_description(self) -> None:
        schema = self._leitor.schema if self._leitor else None
        if schema is None:
            return
        self._nomes = [c.name for c in schema]
        # Uma classe por resultado: os nomes viram atributo de CLASSE e cada
        # linha custa só a tupla dos seus valores.
        self._linha_cls = fabrica(self._nomes)
        self._jdbc = [c.jdbc for c in schema]
        self._description = [
            (c.name, c.jdbc, c.display_size, _SEM_TAMANHO_INTERNO, c.precision, c.scale, c.nullable)
            for c in schema
        ]

    # ----------------------------------------------------------------- fetch

    def _desfecho(self) -> None:
        """Traduz o fecho do stream. Chamado quando o gerador se esgota."""
        assert self._leitor is not None
        falha = self._leitor.erro
        if falha is not None:
            if falha.code == erros.MAX_ROWS_EXCEEDED:
                # NUNCA exceção. O caso de fronteira — resultado com exatamente
                # maxRows linhas — é falso positivo conhecido, e distingui-lo no
                # servidor custaria varrer o dataset inteiro, que é justamente o
                # que o teto existe para evitar. As linhas já entregues valem.
                self._rowcount = falha.rows
                warnings.warn(
                    f"{falha.message} ({falha.rows} linhas entregues)",
                    erros.Warning,
                    stacklevel=3,
                )
                return
            self._rowcount = falha.rows
            e = erros.de_codigo(falha.code, falha.message, rows_delivered=falha.rows)
            self._falha = e
            raise e
        fim = self._leitor.fim
        if fim is not None:
            # `truncated` aqui só acontece quando o PRÓPRIO cliente pediu menos
            # linhas, e atender ao que foi pedido não é falha.
            self._rowcount = fim.rows

    def _converte(self, linha: bytes) -> Row:
        pares = _leitor.pares_da_linha(linha)
        n = len(self._nomes)
        valores: list[Any] = [None] * n
        # Casamento POSICIONAL, avançando o ponteiro do schema até o nome
        # bater. É o que absorve `SELECT a, a` — um dict por nome perderia a
        # coluna repetida em silêncio.
        j = 0
        for nome, valor in pares:
            k = j
            while k < n and self._nomes[k] != nome:
                k += 1
            if k >= n:
                raise erros.OperationalError(
                    f"a linha traz a coluna {nome!r}, que não casa nenhuma coluna "
                    "restante do schema — o protocolo foi violado"
                )
            valores[k] = tipos.do_fio(valor, self._jdbc[k])
            j = k + 1
        return self._linha_cls(valores)

    def _proxima(self) -> Row | None:
        if self._falha is not None:
            raise self._falha
        if self._gerador is None:
            raise erros.InterfaceError("nenhuma consulta foi executada neste cursor")
        if self._pendente is not None:
            bruto, self._pendente = self._pendente, None
            return self._converte(bruto)
        try:
            bruto = next(self._gerador)
        except StopIteration:
            self._desfecho()
            self._devolve_conexao()
            return None
        except BaseException as e:
            self._falha = e
            raise
        return self._converte(bruto)

    def _proxima_crua(self) -> bytes | None:
        """A próxima linha de dados SEM decodificar, como o `Reader.Linha()` do
        Go a entrega."""
        if self._falha is not None:
            raise self._falha
        if self._gerador is None:
            raise erros.InterfaceError("nenhuma consulta foi executada neste cursor")
        if self._pendente is not None:
            bruto, self._pendente = self._pendente, None
            return bruto
        try:
            return next(self._gerador)
        except StopIteration:
            self._desfecho()
            self._devolve_conexao()
            return None
        except BaseException as e:
            self._falha = e
            raise

    def linhas_cruas(self) -> Iterator[bytes]:
        """Itera as linhas NDJSON CRUAS, byte a byte como o servidor as emitiu.

        É o passthrough — a mesma saída do `nishidb query` — e é o escape para
        quem precisa da grafia exata do servidor: o `1e-07` de um double, ou os
        bytes originais de uma coluna `json` antes de o `json.loads` passar
        seus números por `float`.
        """
        self._exige_aberto()
        while True:
            bruto = self._proxima_crua()
            if bruto is None:
                return
            yield bruto

    def fetchone(self) -> Row | None:
        self._exige_aberto()
        return self._proxima()

    def fetchmany(self, size: int | None = None) -> list[Row]:
        self._exige_aberto()
        quantas = self.arraysize if size is None else size
        saida: list[Row] = []
        for _ in range(max(0, quantas)):
            linha = self._proxima()
            if linha is None:
                break
            saida.append(linha)
        return saida

    def fetchall(self) -> list[Row]:
        """Materializa o que RESTA. É o único método que o faz, e é o que o
        nome promete — para resultado grande, itere o cursor."""
        self._exige_aberto()
        saida: list[Row] = []
        while True:
            linha = self._proxima()
            if linha is None:
                return saida
            saida.append(linha)

    def __iter__(self) -> Iterator[Row]:
        return self

    def __next__(self) -> Row:
        linha = self.fetchone()
        if linha is None:
            raise StopIteration
        return linha

    # ----------------------------------------------------------------- ciclo

    def _devolve_conexao(self) -> None:
        if self._conexao is not None:
            self._conexao.transporte.fim_do_stream()

    def _encerra_resultado(self) -> None:
        """Larga o resultado corrente. Se ele não foi consumido até o fim, o
        transporte derruba a conexão — há bytes não lidos no fio."""
        self._leitor = None
        self._gerador = None
        self._pendente = None
        self._description = None
        self._nomes = []
        self._linha_cls = Row
        self._jdbc = []

    def close(self) -> None:
        if self._fechado:
            return
        self._fechado = True
        self._encerra_resultado()
        self._conexao = None

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # A PEP 249 exige que os dois existam e admite que não façam nada. Não há o
    # que dimensionar: os tipos do wire são quatro e saem do valor Python.
    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        pass
