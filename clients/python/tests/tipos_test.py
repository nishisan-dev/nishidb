"""Testes da conversão, nos dois sentidos.

A matriz inteira roda DUAS vezes: em UTC e em `Asia/Tokyo`. É o teste que pegou
os dois construtores errados no driver JDBC, e o Python tem as armadilhas
equivalentes — `datetime.fromtimestamp()` sem `tz` usa o fuso da máquina, e é o
construtor óbvio.
"""

from __future__ import annotations

import os
import time as _time
import unittest
from datetime import datetime, time, timezone
from decimal import Decimal

import nishidb
from nishidb import erros, tipos


class FusoHostil:
    """Troca o fuso do PROCESSO durante o teste e o devolve no fim."""

    def __init__(self, tz: str) -> None:
        self._tz = tz
        self._antes: str | None = None

    def __enter__(self) -> None:
        self._antes = os.environ.get("TZ")
        os.environ["TZ"] = self._tz
        _time.tzset()

    def __exit__(self, *_: object) -> None:
        if self._antes is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = self._antes
        _time.tzset()


class MatrizDeLeitura(unittest.TestCase):
    """Cada afirmação vale nos dois fusos. Se alguma depender do relógio da
    máquina, ela falha em Tóquio e passa em São Paulo — que é exatamente o
    modo de falha que este teste existe para impedir."""

    def _matriz(self):
        self.assertEqual(tipos.do_fio("texto", tipos.VARCHAR), "texto")
        self.assertEqual(tipos.do_fio(9223372036854775807, tipos.BIGINT), 9223372036854775807)
        self.assertEqual(tipos.do_fio(1e-07, tipos.DOUBLE), 1e-07)
        self.assertIs(tipos.do_fio(True, tipos.BOOLEAN), True)
        self.assertIsNone(tipos.do_fio(None, tipos.BIGINT))

        ts = tipos.do_fio("2026-08-23T21:53:04.123Z", tipos.TIMESTAMP)
        self.assertEqual(ts, datetime(2026, 8, 23, 21, 53, 4, 123000, tzinfo=timezone.utc))
        self.assertEqual(ts.tzinfo, timezone.utc)
        # O INSTANTE é o mesmo nos dois fusos — é o que faz um relatório não
        # mudar de dia conforme a máquina em que roda.
        self.assertEqual(ts.timestamp(), 1787521984.123)

        hr = tipos.do_fio("23:59:59.999", tipos.TIME)
        self.assertEqual(hr, time(23, 59, 59, 999000))
        # Naive de propósito: hora do dia não é instante, e carimbar fuso nela
        # seria inventar dado.
        self.assertIsNone(hr.tzinfo)

        self.assertEqual(tipos.do_fio("aGVsbG8=", tipos.VARBINARY), b"hello")
        self.assertEqual(tipos.do_fio({"k": 1}, tipos.VARCHAR), {"k": 1})
        self.assertEqual(tipos.do_fio("10.0.0.1/24", tipos.VARCHAR), "10.0.0.1/24")

    def test_em_utc(self):
        with FusoHostil("UTC"):
            self._matriz()

    def test_em_asia_tokyo(self):
        with FusoHostil("Asia/Tokyo"):
            self._matriz()

    def test_em_america_sao_paulo(self):
        with FusoHostil("America/Sao_Paulo"):
            self._matriz()


class Temporais(unittest.TestCase):
    def test_fracao_de_qualquer_tamanho_normaliza_para_micros(self):
        for grafia, micros in (
            ("2026-01-01T00:00:00Z", 0),
            ("2026-01-01T00:00:00.1Z", 100000),
            ("2026-01-01T00:00:00.123Z", 123000),
            ("2026-01-01T00:00:00.123456Z", 123456),
            ("2026-01-01T00:00:00.123456789Z", 123456),
        ):
            with self.subTest(grafia=grafia):
                self.assertEqual(tipos.para_timestamp(grafia).microsecond, micros)

    def test_timestamp_sem_z_e_recusado(self):
        # O encoder só emite UTC; acolher um offset explícito faria o client
        # aceitar um formato que o servidor não produz.
        with self.assertRaises(erros.DataError):
            tipos.para_timestamp("2026-01-01T00:00:00-03:00")

    def test_ano_fora_do_representavel_falha_alto(self):
        with self.assertRaises(erros.DataError):
            tipos.para_timestamp("0000-01-01T00:00:00.000Z")

    def test_time_nao_aceita_z(self):
        with self.assertRaises(erros.DataError):
            tipos.para_time("23:59:59.999Z")


class Binario(unittest.TestCase):
    def test_lixo_a_direita_e_erro_e_nao_array_truncado(self):
        with self.assertRaises(erros.DataError):
            tipos.do_fio("aGVsbG8=@@@", tipos.VARBINARY)

    def test_base64_so_e_decodificado_em_coluna_binaria(self):
        # Numa coluna de texto, o mesmo conteúdo é o TEXTO. A convenção do JDBC
        # faria a mesma coluna devolver ora o binário, ora os caracteres.
        self.assertEqual(tipos.do_fio("aGVsbG8=", tipos.VARCHAR), "aGVsbG8=")


class Coercoes(unittest.TestCase):
    def test_booleano_nao_passa_por_bigint(self):
        with self.assertRaises(erros.DataError):
            tipos.do_fio(True, tipos.BIGINT)

    def test_booleano_nao_passa_por_double(self):
        with self.assertRaises(erros.DataError):
            tipos.do_fio(True, tipos.DOUBLE)

    def test_coluna_other_segue_o_token(self):
        # É a única coluna cujo metadado pode discordar do dado: `auto` que o
        # dataset ainda não fixou, com ingestão concorrente.
        self.assertEqual(tipos.do_fio(7, tipos.OTHER), 7)
        self.assertEqual(tipos.do_fio("sete", tipos.OTHER), "sete")
        self.assertIs(tipos.do_fio(False, tipos.OTHER), False)


class Parametros(unittest.TestCase):
    def test_os_quatro_tipos_de_wire(self):
        self.assertEqual(tipos.para_parametro("x", 1), {"type": "string", "value": "x"})
        self.assertEqual(tipos.para_parametro(7, 1), {"type": "int64", "value": 7})
        self.assertEqual(tipos.para_parametro(1.5, 1), {"type": "double", "value": 1.5})
        self.assertEqual(tipos.para_parametro(True, 1), {"type": "boolean", "value": True})

    def test_datetime_viaja_como_epoch_millis(self):
        # NUNCA ISO-8601: uma string contra coluna `datetime` é interpretada no
        # `format:` declarado do campo, resolvido no fuso do host do SERVIDOR.
        p = tipos.para_parametro(datetime(2026, 8, 23, 21, 53, 4, 123000, tzinfo=timezone.utc), 1)
        self.assertEqual(p, {"type": "int64", "value": 1787521984123})

    def test_datetime_naive_e_recusado(self):
        # Assumir o fuso da máquina deslocaria o parâmetro por horas sem nada
        # acusar.
        with self.assertRaises(erros.DataError) as ctx:
            tipos.para_parametro(datetime(2026, 8, 23, 21, 53, 4), 1)
        self.assertIn("sem fuso", str(ctx.exception))

    def test_o_epoch_millis_nao_depende_do_fuso_da_maquina(self):
        instante = datetime(2026, 8, 23, 21, 53, 4, tzinfo=timezone.utc)
        with FusoHostil("Asia/Tokyo"):
            tokyo = tipos.para_parametro(instante, 1)
        with FusoHostil("America/Sao_Paulo"):
            sp = tipos.para_parametro(instante, 1)
        self.assertEqual(tokyo, sp)

    def test_time_viaja_como_millis_do_dia(self):
        p = tipos.para_parametro(time(23, 59, 59, 999000), 1)
        self.assertEqual(p, {"type": "int64", "value": 86399999})

    def test_decimal_inteiro_vai_como_int64(self):
        self.assertEqual(tipos.para_parametro(Decimal("7"), 1), {"type": "int64", "value": 7})

    def test_decimal_fracionario_vai_como_double(self):
        self.assertEqual(tipos.para_parametro(Decimal("1.5"), 1), {"type": "double", "value": 1.5})

    def test_nan_e_infinito_sao_recusados(self):
        for v in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(v=v), self.assertRaises(erros.DataError):
                tipos.para_parametro(v, 1)

    def test_bytes_sao_recusados_nomeando_a_razao(self):
        with self.assertRaises(erros.NotSupportedError) as ctx:
            tipos.para_parametro(b"abc", 1)
        self.assertIn("binária", str(ctx.exception))

    def test_o_indice_da_mensagem_e_1_based(self):
        with self.assertRaises(erros.DataError) as ctx:
            tipos.para_parametro(object(), 3)
        self.assertIn("parâmetro 3", str(ctx.exception))


class ObjetosDeTipo(unittest.TestCase):
    def test_casam_os_nomes_jdbc_do_servidor(self):
        self.assertEqual(nishidb.STRING, "VARCHAR")
        self.assertEqual(nishidb.BINARY, "VARBINARY")
        self.assertEqual(nishidb.NUMBER, "BIGINT")
        self.assertEqual(nishidb.NUMBER, "DOUBLE")
        self.assertEqual(nishidb.DATETIME, "TIMESTAMP")
        self.assertEqual(nishidb.DATETIME, "TIME")

    def test_nao_casam_o_que_nao_e_deles(self):
        self.assertNotEqual(nishidb.STRING, "BIGINT")
        self.assertNotEqual(nishidb.ROWID, "VARCHAR")


class ConstrutoresDaPep(unittest.TestCase):
    def test_date_e_meia_noite_em_utc(self):
        d = nishidb.Date(2026, 8, 23)
        self.assertEqual(d, datetime(2026, 8, 23, tzinfo=timezone.utc))

    def test_os_construtores_nao_dependem_do_fuso_da_maquina(self):
        with FusoHostil("Asia/Tokyo"):
            a = (nishidb.Date(2026, 8, 23), nishidb.Timestamp(2026, 8, 23, 21, 53, 4))
        with FusoHostil("America/Sao_Paulo"):
            b = (nishidb.Date(2026, 8, 23), nishidb.Timestamp(2026, 8, 23, 21, 53, 4))
        self.assertEqual(a, b)

    def test_binary_aceita_texto_e_bytes(self):
        self.assertEqual(nishidb.Binary("ab"), b"ab")
        self.assertEqual(nishidb.Binary(b"ab"), b"ab")


if __name__ == "__main__":
    unittest.main()
