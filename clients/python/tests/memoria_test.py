"""O gate do §128 no cliente.

É o equivalente Python do `-Xmx64m` que o surefire impõe aos testes do driver
JDBC: a prova de que o resultado ATRAVESSA o client em vez de virar heap nele.

A afirmação é sobre a FORMA da curva, não sobre um número absoluto: o pico de
memória ao percorrer 10 N linhas tem de ficar perto do pico de N. Um número
absoluto seria frágil (varia com a versão do Python e com o alocador); a razão
entre os dois pica é o que separa "streaming" de "materializou".

Este teste reprova sozinho no dia em que alguém trocar `readline()` por
`read()`, ou fizer `fetchall()` por dentro da iteração.
"""

from __future__ import annotations

import tracemalloc
import unittest

from servidor_falso import ServidorFalso, coluna, schema

import nishidb

#: A linha de dados usada em todas as medições. Colunas de tipos diferentes,
#: para que a conversão também esteja no caminho medido.
LINHA = {"id": "S-000000", "n": 1234567890, "d": 1.5, "ts": "2026-08-23T21:53:04.123Z"}

ESQUEMA = schema(
    coluna("id", "VARCHAR"),
    coluna("n", "BIGINT", type="int64"),
    coluna("d", "DOUBLE", type="double"),
    coluna("ts", "TIMESTAMP", type="timestamp"),
)


def pico_ao_percorrer(n: int) -> int:
    """Pico de memória alocada ao ITERAR n linhas, sem guardá-las."""
    with ServidorFalso() as srv:
        srv.responde_query_repetida(ESQUEMA, LINHA, n)
        cx = nishidb.connect(srv.dsn)
        try:
            cur = cx.cursor()
            cur.execute("SELECT id, n, d, ts FROM t")
            tracemalloc.start()
            try:
                vistas = 0
                for linha in cur:
                    # Toca um valor, para que a conversão não seja otimizada
                    # para fora do caminho medido.
                    if linha[0]:
                        vistas += 1
                _, pico = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            assert vistas == n, f"esperava {n} linhas, vi {vistas}"
            return pico
        finally:
            cx.close()


class Streaming(unittest.TestCase):
    def test_o_pico_nao_cresce_com_o_numero_de_linhas(self):
        pequeno = pico_ao_percorrer(2_000)
        grande = pico_ao_percorrer(20_000)
        # Dez vezes mais linhas. Materializar levaria a razão para perto de 10;
        # streaming a mantém perto de 1. O teto de 2 dá folga para ruído do
        # alocador sem deixar passar acumulação de verdade.
        razao = grande / max(pequeno, 1)
        self.assertLess(
            razao,
            2.0,
            f"o pico cresceu {razao:.1f}x com 10x mais linhas "
            f"({pequeno} -> {grande} bytes): o resultado está sendo acumulado",
        )

    def test_fetchall_materializa_e_e_isso_que_ele_promete(self):
        # O contraste que dá sentido ao teste acima: quando o método DIZ que
        # materializa, o pico cresce mesmo. Se este teste falhar, o de cima
        # está medindo outra coisa.
        def pico_do_fetchall(n: int) -> int:
            with ServidorFalso() as srv:
                srv.responde_query_repetida(ESQUEMA, LINHA, n)
                with nishidb.connect(srv.dsn) as cx:
                    cur = cx.cursor()
                    cur.execute("SELECT id, n, d, ts FROM t")
                    tracemalloc.start()
                    try:
                        linhas = cur.fetchall()
                        _, pico = tracemalloc.get_traced_memory()
                    finally:
                        tracemalloc.stop()
                    assert len(linhas) == n
                    return pico

        razao = pico_do_fetchall(20_000) / max(pico_do_fetchall(2_000), 1)
        self.assertGreater(razao, 5.0, "fetchall deveria acumular, e não acumulou")


if __name__ == "__main__":
    unittest.main()
