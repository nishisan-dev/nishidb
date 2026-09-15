"""Testes contra o BINÁRIO de verdade, com TLS e credencial.

Os outros arquivos provam que o client faz o que quem o escreveu ACHA que o
servidor emite. Este fecha a outra metade — e é o único que pega uma
divergência entre o fio suposto e o fio real.

São dirigidos por `scripts/python-it.sh` (`make test-python-it`), que sobe o
servidor e exporta as variáveis. Sem elas, os testes se auto-pulam para que
`python3 -m unittest` avulso continue funcionando — **e `ExigeHarness` abaixo
FALHA quando `NISHIDB_IT_REQUIRED=1` e a URL não veio**, que é o que torna
impossível o pior desfecho: harness quebrado, suíte verde e zero teste de
integração executado.
"""

from __future__ import annotations

import os
import tracemalloc
import unittest
from datetime import datetime, time, timezone

import nishidb

DSN = os.environ.get("NISHIDB_IT_DSN", "")
DSN_SEM_CA = os.environ.get("NISHIDB_IT_DSN_SEM_CA", "")
USUARIO = os.environ.get("NISHIDB_IT_USER", "")
LINHAS = int(os.environ.get("NISHIDB_IT_LINHAS", "0") or 0)

sem_harness = unittest.skipUnless(DSN, "sem NISHIDB_IT_DSN: rode `make test-python-it`")


class ExigeHarness(unittest.TestCase):
    """A guarda contra o SKIP silencioso — o débito 53 traduzido para Python."""

    def test_o_harness_precisa_estar_de_pe_quando_exigido(self):
        if os.environ.get("NISHIDB_IT_REQUIRED") != "1":
            self.skipTest("NISHIDB_IT_REQUIRED não está setada")
        self.assertTrue(
            DSN,
            "NISHIDB_IT_REQUIRED=1 mas NISHIDB_IT_DSN está vazia: o harness quebrou e "
            "a suíte teria passado verde sem rodar um único teste de integração",
        )


def conecta(**kwargs):
    return nishidb.connect(DSN, user=USUARIO or None, **kwargs)


@sem_harness
class MatrizDeTipos(unittest.TestCase):
    """Cada tipo do wire, do encoder do servidor até o valor Python."""

    @classmethod
    def setUpClass(cls):
        cls.cx = conecta()

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def linha_de_borda(self, qual: str):
        cur = self.cx.cursor()
        cur.execute(
            "SELECT id, t, inteiro, real, booleano, quando, v4, v6, rede4, rede6, "
            "cru, doc, momento_iso, hora_iso FROM todos_os_tipos WHERE id = ?",
            (qual,),
        )
        linhas = cur.fetchall()
        self.assertEqual(len(linhas), 1, f"esperava exatamente a linha {qual}")
        return linhas[0]

    def test_os_valores_de_borda_atravessam_intactos(self):
        r = self.linha_de_borda("borda-1")
        self.assertEqual(r["inteiro"], 9223372036854775807)
        self.assertIsInstance(r["inteiro"], int)
        self.assertEqual(r["real"], 1e-07)
        self.assertIs(r["booleano"], True)
        self.assertEqual(r["v4"], "255.255.255.255")
        self.assertEqual(r["v6"], "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")
        # Sem Masked() no encoder, os host bits ficam.
        self.assertEqual(r["rede4"], "10.0.0.1/24")
        self.assertEqual(r["rede6"], "2001:db8::1/32")
        self.assertEqual(r["cru"], b"Hello")
        self.assertEqual(r["doc"], {"a": [1, 2, {"b": None}]})
        # Derivado do epoch millis que o corpus grava (it-comum.sh), e não
        # escrito à mão: uma data digitada aqui afirmaria a aritmética de quem
        # a digitou, não a do servidor. O que se está provando é que o
        # instante ATRAVESSA — o corpus grava millis, o servidor os renderiza
        # em ISO-8601 UTC e o client os traz de volta ao mesmo ponto no tempo.
        self.assertEqual(
            r["quando"],
            datetime.fromtimestamp(1787570000123 / 1000, tz=timezone.utc),
        )
        self.assertEqual(r["hora_iso"], time(23, 59, 59, 999000))

    def test_o_menor_int64_e_o_maior_double_negativo(self):
        r = self.linha_de_borda("borda-2")
        self.assertEqual(r["inteiro"], -9223372036854775808)
        self.assertEqual(r["real"], -1.7976931348623157e308)
        self.assertEqual(r["cru"], b"")
        self.assertEqual(r["v6"], "::ffff:1.2.3.4")

    def test_double_faz_round_trip_de_bits(self):
        # A doutrina é nunca comparar double como TEXTO: o Go emite `1e-07` na
        # grafia 'g' e o Python o repr-aria como `1e-07` também, mas os dois
        # arredondam por IEEE-754 e é a igualdade de BITS que importa.
        import struct

        r = self.linha_de_borda("borda-1")
        self.assertEqual(struct.pack("<d", r["real"]), struct.pack("<d", 1e-07))

    def test_o_json_sai_inline_e_nao_como_texto(self):
        r = self.linha_de_borda("borda-1")
        self.assertIsInstance(r["doc"], dict)


@sem_harness
class Metadados(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = conecta()

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def test_projection_aliases_and_suffixes(self):
        for limit in ("", " LIMIT 0"):
            with self.subTest(limit=limit), self.cx.cursor() as cur:
                cur.execute(
                    'SELECT id, inteiro AS id, texto AS "FROM", inteiro amount '
                    "FROM todos_os_tipos WHERE id = 'borda-1' ORDER BY id" + limit
                )
                self.assertEqual([d[0] for d in cur.description], ["id_1", "id", "FROM", "amount"])
                self.assertEqual(
                    [d[1] for d in cur.description], ["VARCHAR", "BIGINT", "VARCHAR", "BIGINT"]
                )
                self.assertFalse(cur.description[0][6])
                self.assertTrue(cur.description[1][6])
                row = cur.fetchone()
                if limit:
                    self.assertIsNone(row)
                else:
                    self.assertEqual(row["id_1"], "borda-1")
                    self.assertEqual(row["id"], (1 << 63) - 1)
                    self.assertEqual(row["amount"], (1 << 63) - 1)
                    self.assertEqual(row["FROM"], 'linha com "aspas" e \\barra')
                    self.assertIsNone(cur.fetchone())

    def test_window_metric_reaches_the_catalog_and_projection_names(self):
        # O contador do corpus declara UMA métrica de janela. O valor da coluna
        # gerada pode ser NULL — o amostrador talvez ainda não tenha passado —,
        # então só nomes, tipos e presença são afirmados, nunca o valor.
        with self.cx.cursor() as cur:
            cur.execute("SELECT namespace, name FROM _meta.tables")
            tabelas = {(r["namespace"], r["name"]) for r in cur.fetchall()}
        self.assertIn(("_meta", "counter_metrics"), tabelas)
        self.assertIn(("_counters", "rows_by_text"), tabelas)

        with self.cx.cursor() as cur:
            cur.execute("SELECT id, counter, name, columns FROM _meta.counter_metrics")
            linhas = cur.fetchall()
        self.assertEqual(len(linhas), 1, "esperava exatamente a métrica do corpus")
        self.assertEqual(linhas[0]["id"], "rows_by_text|rows_5m")
        self.assertEqual(linhas[0]["counter"], "rows_by_text")
        self.assertEqual(linhas[0]["name"], "rows_5m")
        self.assertEqual(linhas[0]["columns"], "rows_5m_avg,rows_5m_max")

        nomes = ["rows_5m_avg", "x", "rows_5m_avg_1"]
        with self.cx.cursor() as cur:
            cur.execute(
                "SELECT rows_5m_avg, rows_5m_avg AS x, rows_5m_avg FROM _counters.rows_by_text"
            )
            self.assertEqual([d[0] for d in cur.description], nomes)
            self.assertEqual([d[1] for d in cur.description], ["DOUBLE"] * 3)
            row = cur.fetchone()
            self.assertIsNotNone(row, "a tabela do contador não trouxe grupo nenhum")
            for nome in nomes:
                valor = row[nome]
                self.assertTrue(valor is None or isinstance(valor, float), f"{nome} = {valor!r}")

    def test_a_chave_e_a_sequence_sao_nao_nulaveis(self):
        cur = self.cx.cursor()
        cur.execute("SELECT id, inteiro FROM todos_os_tipos LIMIT 1")
        por_nome = {d[0]: d for d in cur.description}
        self.assertFalse(
            por_nome["id"][6], "a chave é NOT NULL: o materializer recusa evento sem ela"
        )
        self.assertTrue(por_nome["inteiro"][6], "coluna comum é nulável")

    def test_os_numeros_do_descritor_vem_do_servidor(self):
        cur = self.cx.cursor()
        cur.execute("SELECT inteiro, quando, v6 FROM todos_os_tipos LIMIT 1")
        por_nome = {d[0]: d for d in cur.description}
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        self.assertEqual(por_nome["inteiro"][1], "BIGINT")
        self.assertEqual((por_nome["inteiro"][4], por_nome["inteiro"][2]), (19, 20))
        self.assertEqual((por_nome["quando"][4], por_nome["quando"][5]), (24, 3))
        # 39, e não os 45 do manual: o pior caso do nosso encoder é 8*4 + 7.
        self.assertEqual(por_nome["v6"][4], 39)

    def test_as_duas_rotas_nomeiam_a_coluna_com_alias_de_formas_diferentes(self):
        # Não é bug: a linha de schema traz o nome de SAÍDA (o alias, porque é
        # ele que é chave do objeto JSON de cada row) e /v1/metadata traz o
        # nome do CAMPO. Casar os dois por igualdade de nome não funciona.
        cur = self.cx.cursor()
        cur.execute("SELECT * FROM todos_os_tipos LIMIT 1")
        saida = {d[0] for d in cur.description}
        self.assertIn("t", saida)
        self.assertNotIn("texto", saida)

        meta = self.cx.metadata("todos_os_tipos")[0]
        por_nome = {c.name: c for c in meta.columns}
        self.assertIn("texto", por_nome)
        self.assertEqual(por_nome["texto"].alias, "t")

    def test_os_tetos_efetivos_sao_publicados(self):
        meta = self.cx.metadata("todos_os_tipos")[0]
        self.assertEqual(meta.key, "id")
        self.assertGreater(meta.max_rows, 0)
        # O teto, e não o prazo padrão — é contra ele que o timeout é aparado.
        self.assertGreaterEqual(meta.timeout_ms, meta.default_timeout_ms)

    def test_stats_publica_a_versao_do_processo(self):
        self.assertTrue(self.cx.stats().version)

    def test_health_responde_up(self):
        s = self.cx.health()
        self.assertTrue(s.up, f"o servidor do harness deveria estar UP, e está {s.status}")

    def test_describe_traz_o_indice_fisico(self):
        c = nishidb.Client(self.cx)
        campos = {r["name"]: r for r in c.describe("todos_os_tipos")}
        self.assertEqual(campos["texto"]["index"], "bitmap")
        self.assertIn("v4", campos)


@sem_harness
class Erros(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = conecta()

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def test_group_by_e_clausula_nao_suportada_e_nao_erro_de_sintaxe(self):
        # A distinção existe para a ferramenta de SQL: quem escreveu a cláusula
        # precisa descobrir que a v1 não agrupa, não procurar um erro de
        # digitação.
        cur = self.cx.cursor()
        with self.assertRaises(nishidb.NotSupportedError) as ctx:
            cur.execute("SELECT id FROM todos_os_tipos GROUP BY id")
        self.assertEqual(ctx.exception.code, nishidb.UNSUPPORTED_CLAUSE)

    def test_order_by_existe_e_nao_e_recusado(self):
        # O dialeto cresceu desde a Fase 7: ORDER BY, LIMIT, OFFSET e LIKE são
        # da v1 hoje. Este teste é o que impede a documentação do client de
        # voltar a listá-los como ausentes.
        cur = self.cx.cursor()
        cur.execute("SELECT id FROM todos_os_tipos ORDER BY id DESC LIMIT 3")
        self.assertLessEqual(len(cur.fetchall()), 3)

    def test_coluna_inexistente_e_erro_de_programacao_com_posicao(self):
        cur = self.cx.cursor()
        with self.assertRaises(nishidb.ProgrammingError) as ctx:
            cur.execute("SELECT naoexiste FROM todos_os_tipos")
        self.assertEqual(ctx.exception.code, nishidb.BIND)
        self.assertIsNotNone(ctx.exception.line)

    def test_dataset_inexistente_falha_ao_abrir(self):
        alvo = DSN.replace("/todos_os_tipos", "/nao_existe")
        with self.assertRaises(nishidb.ProgrammingError) as ctx:
            nishidb.connect(alvo, user=USUARIO or None)
        self.assertEqual(ctx.exception.code, nishidb.UNKNOWN_DATASET)

    def test_credencial_errada_e_recusada_sem_dizer_qual_metade_falhou(self):
        # O harness exporta NISHIDB_PASSWORD, e a regra de fonte única de senha
        # recusaria a combinação antes de a conexão sair. Tirar a variável aqui
        # é o que faz este teste exercitar o 401 do servidor, e não a validação
        # do próprio client (que já tem teste próprio em conexao_test.py).
        antes = os.environ.pop("NISHIDB_PASSWORD", None)
        try:
            with self.assertRaises(nishidb.OperationalError) as ctx:
                nishidb.connect(DSN, user=USUARIO or "leitor", password="senha-errada")
        finally:
            if antes is not None:
                os.environ["NISHIDB_PASSWORD"] = antes
        self.assertEqual(ctx.exception.code, nishidb.UNAUTHENTICATED)


@sem_harness
class Tls(unittest.TestCase):
    def test_sem_a_ca_a_conexao_falha(self):
        # A prova de que não há verificação desligada escondida. Se este teste
        # passar a conectar, alguém acrescentou um `insecure` em algum lugar.
        self.assertTrue(DSN_SEM_CA, "o harness precisa exportar NISHIDB_IT_DSN_SEM_CA")
        with self.assertRaises(nishidb.OperationalError):
            nishidb.connect(DSN_SEM_CA, user=USUARIO or None)


@sem_harness
class Parametros(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cx = conecta()

    @classmethod
    def tearDownClass(cls):
        cls.cx.close()

    def test_parametro_de_texto_filtra(self):
        cur = self.cx.cursor()
        cur.execute("SELECT id FROM todos_os_tipos WHERE id = ?", ("borda-1",))
        self.assertEqual([r["id"] for r in cur.fetchall()], ["borda-1"])

    def test_parametro_int64_no_limite(self):
        cur = self.cx.cursor()
        cur.execute("SELECT id FROM todos_os_tipos WHERE inteiro = ?", (9223372036854775807,))
        self.assertEqual([r["id"] for r in cur.fetchall()], ["borda-1"])

    def test_parametro_booleano(self):
        cur = self.cx.cursor()
        cur.execute("SELECT id FROM todos_os_tipos WHERE booleano = ? LIMIT 1", (True,))
        self.assertEqual(len(cur.fetchall()), 1)

    def test_like_filtra_por_padrao(self):
        cur = self.cx.cursor()
        cur.execute("SELECT id FROM todos_os_tipos WHERE id LIKE ? LIMIT 5", ("borda-%",))
        self.assertGreaterEqual(len(cur.fetchall()), 2)

    def test_max_rows_pedido_pelo_cliente_marca_truncado(self):
        cur = self.cx.cursor()
        cur.max_rows = 5
        cur.execute("SELECT id FROM todos_os_tipos")
        self.assertEqual(len(cur.fetchall()), 5)
        self.assertEqual(cur.rowcount, 5)


@sem_harness
class Volume(unittest.TestCase):
    """A prova do §128 contra o servidor real — o equivalente do `-Xmx64m` que
    o surefire impõe ao driver JDBC."""

    #: Teto ABSOLUTO de pico, generoso para o alocador do Python mas duas
    #: ordens de grandeza abaixo do que a materialização de 120 mil linhas
    #: custaria.
    TETO_BYTES = 16 * 1024 * 1024

    def test_o_resultado_inteiro_atravessa_com_memoria_limitada(self):
        self.assertGreaterEqual(LINHAS, 100_000, "o harness precisa gerar >= 100k linhas")
        with conecta() as cx:
            cur = cx.cursor()
            cur.execute("SELECT id, t, inteiro, real, quando FROM todos_os_tipos")
            tracemalloc.start()
            try:
                vistas = 0
                for linha in cur:
                    if linha[0]:
                        vistas += 1
                _, pico = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()

        self.assertGreaterEqual(vistas, LINHAS, f"esperava >= {LINHAS} linhas, vi {vistas}")
        self.assertLess(
            pico,
            self.TETO_BYTES,
            f"o pico foi {pico / 1024 / 1024:.1f} MiB percorrendo {vistas} linhas: "
            "o resultado está sendo acumulado em vez de atravessar",
        )

    def test_as_linhas_cruas_tambem_atravessam(self):
        with conecta() as cx:
            cur = cx.cursor()
            cur.execute("SELECT id FROM todos_os_tipos")
            tracemalloc.start()
            try:
                vistas = sum(1 for _ in cur.linhas_cruas())
                _, pico = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
        self.assertGreaterEqual(vistas, LINHAS)
        self.assertLess(pico, self.TETO_BYTES)


if __name__ == "__main__":
    unittest.main()
