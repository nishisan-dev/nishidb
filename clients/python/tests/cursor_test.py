"""Testes do cursor: o corpo que sai, as linhas que entram e os desfechos."""

from __future__ import annotations

import json
import unittest
import warnings

from servidor_falso import Resposta, ServidorFalso, coluna, erro, fim, schema

import nishidb


class Base(unittest.TestCase):
    def setUp(self):
        self.srv = ServidorFalso().__enter__()
        self.addCleanup(self.srv.__exit__, None, None, None)

    def conecta(self, **kwargs):
        cx = nishidb.connect(self.srv.dsn, **kwargs)
        self.addCleanup(cx.close)
        return cx

    def corpo_enviado(self) -> dict:
        return json.loads(self.srv.ultima("/v1/query").corpo)


class Description(Base):
    def test_as_sete_tuplas_saem_da_linha_de_schema(self):
        self.srv.responde_query(
            schema(
                coluna("id", "VARCHAR", nullable=False),
                coluna("n", "BIGINT", type="int64", precision=19, displaySize=20),
                coluna("t", "TIMESTAMP", type="timestamp", precision=24, scale=3, displaySize=24),
            ),
            fim(0),
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT id, n, t FROM t")
        # (name, type_code, display_size, internal_size, precision, scale, null_ok)
        self.assertEqual(
            cur.description,
            [
                ("id", "VARCHAR", 0, None, 0, 0, False),
                ("n", "BIGINT", 20, None, 19, 0, True),
                ("t", "TIMESTAMP", 24, None, 24, 3, True),
            ],
        )

    def test_o_type_code_compara_com_os_objetos_de_tipo_da_pep(self):
        self.srv.responde_query(schema(coluna("n", "BIGINT")), fim(0))
        cur = self.conecta().cursor()
        cur.execute("SELECT n FROM t")
        self.assertEqual(cur.description[0][1], nishidb.NUMBER)
        self.assertNotEqual(cur.description[0][1], nishidb.STRING)

    def test_description_fica_pronto_antes_da_primeira_linha(self):
        # O servidor faz flush logo depois do schema justamente para isto.
        self.srv.responde_query(schema(coluna("a")), {"a": "x"}, fim(1))
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual([d[0] for d in cur.description], ["a"])

    def test_jdbc_desconhecido_nao_quebra_o_cursor(self):
        # Um tipo NOVO no servidor não pode exigir release deste pacote: o
        # valor chega como o Python o representa e o usuário decide.
        self.srv.responde_query(schema(coluna("x", "GEOGRAPHY")), {"x": "POINT(1 2)"}, fim(1))
        cur = self.conecta().cursor()
        cur.execute("SELECT x FROM t")
        self.assertEqual(cur.fetchone()["x"], "POINT(1 2)")


class CorpoDaRequisicao(Base):
    def setUp(self):
        super().setUp()
        self.srv.responde_query(schema(coluna("a")), fim(0))

    def test_o_dataset_sai_do_from(self):
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM outro")
        self.assertEqual(self.corpo_enviado()["dataset"], "outro")

    def test_sem_from_reconhecivel_vale_o_dataset_da_conexao(self):
        cur = self.conecta().cursor()
        cur.execute("SELECT name FROM _meta.fields")
        self.assertEqual(self.corpo_enviado()["dataset"], "t")

    def test_o_corpo_tem_exatamente_os_campos_conhecidos(self):
        # O servidor decodifica com DisallowUnknownFields: uma chave a mais é
        # 400 BAD_REQUEST.
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(set(self.corpo_enviado()), {"dataset", "sql"})

    def test_os_quatro_tipos_de_wire(self):
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t WHERE a=? AND b=? AND c=? AND d=?", ("x", 7, 1.5, True))
        self.assertEqual(
            self.corpo_enviado()["parameters"],
            [
                {"type": "string", "value": "x"},
                {"type": "int64", "value": 7},
                {"type": "double", "value": 1.5},
                {"type": "boolean", "value": True},
            ],
        )

    def test_int64_no_limite_viaja_exato(self):
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t WHERE n = ?", (9223372036854775807,))
        # Confere nos BYTES, não no dict: é o texto no fio que prova que não
        # passou por float no caminho.
        self.assertIn(b'"value":9223372036854775807', self.srv.ultima("/v1/query").corpo)

    def test_int64_acima_do_teto_e_recusado_no_cliente(self):
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.DataError):
            cur.execute("SELECT a FROM t WHERE n = ?", (2**63,))

    def test_booleano_nao_vira_inteiro(self):
        # bool é subclasse de int no Python: a ordem errada mandaria True como 1.
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t WHERE b = ?", (True,))
        self.assertEqual(self.corpo_enviado()["parameters"][0]["type"], "boolean")

    def test_parametro_nulo_ensina_is_null(self):
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.NotSupportedError) as ctx:
            cur.execute("SELECT a FROM t WHERE a = ?", (None,))
        self.assertIn("IS NULL", str(ctx.exception))

    def test_contagem_de_parametros_errada_e_recusada_no_cliente(self):
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.InterfaceError):
            cur.execute("SELECT a FROM t WHERE a = ? AND b = ?", ("so-um",))

    def test_max_rows_abaixo_do_teto_e_enviado(self):
        cur = self.conecta().cursor()
        cur.max_rows = 200
        cur.execute("SELECT a FROM t")
        self.assertEqual(self.corpo_enviado()["maxRows"], 200)

    def test_max_rows_acima_do_teto_e_omitido(self):
        # O servidor RECUSA acima do teto em vez de aparar; o teto do dataset
        # já satisfaz "no máximo n".
        cur = self.conecta().cursor()
        cur.max_rows = 999999
        cur.execute("SELECT a FROM t")
        self.assertNotIn("maxRows", self.corpo_enviado())

    def test_max_rows_zero_e_omitido(self):
        # Zero é BAD_REQUEST no servidor, nunca "sem limite".
        cur = self.conecta().cursor()
        cur.max_rows = 0
        cur.execute("SELECT a FROM t")
        self.assertNotIn("maxRows", self.corpo_enviado())

    def test_timeout_e_aparado_contra_o_teto_do_dataset(self):
        cur = self.conecta().cursor()
        cur.timeout_ms = 99_999_999  # muito acima do teto de 600000 do fake
        cur.execute("SELECT a FROM t")
        self.assertEqual(self.corpo_enviado()["timeoutMs"], 600000)


class Linhas(Base):
    def test_converte_cada_tipo_do_fio(self):
        self.srv.responde_query(
            schema(
                coluna("s", "VARCHAR"),
                coluna("n", "BIGINT"),
                coluna("d", "DOUBLE"),
                coluna("b", "BOOLEAN"),
                coluna("ts", "TIMESTAMP"),
                coluna("hr", "TIME"),
                coluna("by", "VARBINARY"),
                coluna("js", "VARCHAR"),
                coluna("nu", "VARCHAR"),
            ),
            {
                "s": "texto",
                "n": 9223372036854775807,
                "d": 1e-07,
                "b": True,
                "ts": "2026-08-23T21:53:04.123Z",
                "hr": "23:59:59.999",
                "by": "aGVsbG8=",
                "js": {"k": [1, 2]},
                "nu": None,
            },
            fim(1),
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT * FROM t")
        r = cur.fetchone()
        self.assertEqual(r["s"], "texto")
        self.assertEqual(r["n"], 9223372036854775807)
        self.assertEqual(r["d"], 1e-07)
        self.assertIs(r["b"], True)
        self.assertEqual(r["ts"].isoformat(), "2026-08-23T21:53:04.123000+00:00")
        self.assertIsNotNone(r["ts"].tzinfo)
        self.assertEqual(r["hr"].isoformat(), "23:59:59.999000")
        self.assertIsNone(r["hr"].tzinfo)
        self.assertEqual(r["by"], b"hello")
        self.assertEqual(r["js"], {"k": [1, 2]})
        self.assertIsNone(r["nu"])

    def test_double_de_valor_inteiro_continua_float(self):
        # O encoder emite `1` para um double inteiro. Sem a coerção, a MESMA
        # coluna teria dtype variável de linha para linha — o que quebra um
        # DataFrame.
        self.srv.responde_query(schema(coluna("d", "DOUBLE")), {"d": 1}, {"d": 1.5}, fim(2))
        cur = self.conecta().cursor()
        cur.execute("SELECT d FROM t")
        valores = [r["d"] for r in cur]
        self.assertEqual(valores, [1.0, 1.5])
        self.assertTrue(all(isinstance(v, float) for v in valores))

    def test_coluna_repetida_nao_some(self):
        # `SELECT a, a` é legal e produz {"a":1,"a":2}. Um json.loads comum
        # devolveria UMA chave e a segunda coluna sumiria em silêncio.
        self.srv.responde_query_cru(
            json.dumps(schema(coluna("a", "BIGINT"), coluna("a", "BIGINT"))).encode()
            + b'\n{"a":1,"a":2}\n'
            + json.dumps(fim(1)).encode()
            + b"\n"
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT a, a FROM t")
        r = cur.fetchone()
        self.assertEqual(tuple(r), (1, 2))
        self.assertEqual(len(r), 2)
        # Por nome, a primeira vence — as duas seguem alcançáveis por posição.
        self.assertEqual(r["a"], 1)

    def test_a_linha_e_sequencia_e_mapping(self):
        self.srv.responde_query(schema(coluna("a"), coluna("b")), {"a": "1", "b": "2"}, fim(1))
        cur = self.conecta().cursor()
        cur.execute("SELECT a, b FROM t")
        r = cur.fetchone()
        self.assertIsInstance(r, tuple)
        self.assertEqual(r[0], "1")
        self.assertEqual(r["b"], "2")
        self.assertEqual(dict(r), {"a": "1", "b": "2"})

    def test_nome_de_coluna_sem_caixa_funciona_mas_o_exato_vence(self):
        self.srv.responde_query(schema(coluna("Nome")), {"Nome": "x"}, fim(1))
        cur = self.conecta().cursor()
        cur.execute("SELECT Nome FROM t")
        r = cur.fetchone()
        self.assertEqual(r["Nome"], "x")
        self.assertEqual(r["nome"], "x")

    def test_nome_ambiguo_sem_caixa_e_recusado(self):
        # `ab` não casa nenhuma coluna exatamente, e casa DUAS sem diferenciar
        # caixa. Devolver uma delas seria pior que recusar.
        self.srv.responde_query(schema(coluna("aB"), coluna("Ab")), {"aB": "1", "Ab": "2"}, fim(1))
        cur = self.conecta().cursor()
        cur.execute('SELECT "aB", "Ab" FROM t')
        r = cur.fetchone()
        self.assertEqual(r["aB"], "1")  # exato continua funcionando
        with self.assertRaises(nishidb.InterfaceError):
            r["ab"]

    def test_fetchmany_respeita_o_tamanho(self):
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, {"a": "2"}, {"a": "3"}, fim(3))
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(len(cur.fetchmany(2)), 2)
        self.assertEqual(len(cur.fetchmany(2)), 1)
        self.assertEqual(cur.fetchmany(2), [])

    def test_fetchall_traz_o_que_resta(self):
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, {"a": "2"}, fim(2))
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        cur.fetchone()
        self.assertEqual([r["a"] for r in cur.fetchall()], ["2"])

    def test_linhas_cruas_preservam_a_grafia_do_servidor(self):
        self.srv.responde_query_cru(
            json.dumps(schema(coluna("d", "DOUBLE"))).encode()
            + b'\n{"d":1e-07}\n'
            + json.dumps(fim(1)).encode()
            + b"\n"
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT d FROM t")
        self.assertEqual(list(cur.linhas_cruas()), [b'{"d":1e-07}'])


class ReusoDaConexao(Base):
    """A conexão HTTP é persistente, e devolvê-la ao estado ocioso é parte do
    contrato. Sem isto a SEGUNDA consulta de uma conexão falha — e foi bug
    real, achado só na integração porque não havia este teste."""

    def test_duas_consultas_na_mesma_conexao(self):
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, fim(1))
        cx = self.conecta()
        for _ in range(3):
            cur = cx.cursor()
            cur.execute("SELECT a FROM t")
            self.assertEqual([r["a"] for r in cur.fetchall()], ["1"])

    def test_consulta_depois_de_resultado_abandonado_no_meio(self):
        # Quem larga o stream deixa bytes não lidos no fio. A conexão tem de
        # ser derrubada e reaberta, nunca reaproveitada — reaproveitá-la faria
        # a resposta seguinte começar no meio da anterior.
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, {"a": "2"}, {"a": "3"}, fim(3))
        cx = self.conecta()
        cur = cx.cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(cur.fetchone()["a"], "1")  # abandona no meio

        outro = cx.cursor()
        outro.execute("SELECT a FROM t")
        self.assertEqual([r["a"] for r in outro.fetchall()], ["1", "2", "3"])

    def test_documento_depois_de_consulta(self):
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, fim(1))
        cx = self.conecta()
        cur = cx.cursor()
        cur.execute("SELECT a FROM t")
        cur.fetchall()
        self.assertEqual(cx.metadata()[0].name, "t")


class Rowcount(Base):
    def test_e_menos_um_enquanto_o_stream_corre(self):
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, {"a": "2"}, fim(2))
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(cur.rowcount, -1)
        cur.fetchone()
        self.assertEqual(cur.rowcount, -1)
        cur.fetchall()
        self.assertEqual(cur.rowcount, 2)


class Desfechos(Base):
    def test_fecho_de_erro_vira_excecao_citando_as_linhas_entregues(self):
        self.srv.responde_query(
            schema(coluna("a")), {"a": "1"}, erro("DATASET_CORRUPT", rows=1), status=200
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(cur.fetchone()["a"], "1")
        with self.assertRaises(nishidb.InternalError) as ctx:
            cur.fetchone()
        self.assertEqual(ctx.exception.code, "DATASET_CORRUPT")
        self.assertEqual(ctx.exception.rows_delivered, 1)

    def test_a_falha_e_sticky(self):
        # Quem engole a exceção e chama de novo NÃO pode receber None e
        # concluir que o resultado terminou bem.
        self.srv.responde_query(
            schema(coluna("a")), {"a": "1"}, erro("INTERNAL", rows=1), status=200
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(cur.fetchone()["a"], "1")
        with self.assertRaises(nishidb.InternalError):
            cur.fetchone()
        with self.assertRaises(nishidb.InternalError):
            cur.fetchone()

    def test_fecho_de_erro_logo_apos_o_schema_falha_no_execute(self):
        # O servidor faz flush depois da linha de schema, então um erro que
        # nasce antes da primeira linha chega enquanto `execute()` ainda corre.
        # Falhar ali é melhor que devolver um cursor que já nasce quebrado.
        self.srv.responde_query(schema(coluna("a")), erro("INTERNAL"), status=200)
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.InternalError):
            cur.execute("SELECT a FROM t")

    def test_max_rows_exceeded_e_aviso_e_preserva_as_linhas(self):
        self.srv.responde_query(
            schema(coluna("a")), {"a": "1"}, erro("MAX_ROWS_EXCEEDED", rows=1), status=200
        )
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            linhas = cur.fetchall()
        self.assertEqual([r["a"] for r in linhas], ["1"])
        self.assertEqual(len(avisos), 1)
        self.assertTrue(issubclass(avisos[0].category, nishidb.Warning))

    def test_truncado_pelo_proprio_cliente_e_sucesso(self):
        self.srv.responde_query(schema(coluna("a")), {"a": "1"}, fim(1, truncated=True))
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        self.assertEqual(len(cur.fetchall()), 1)
        self.assertEqual(cur.rowcount, 1)

    def test_stream_sem_fecho_e_falha(self):
        # Sem esta checagem o cliente apresentaria um resultado PARCIAL como
        # completo.
        self.srv.responde_query_cru(json.dumps(schema(coluna("a"))).encode() + b'\n{"a":"1"}\n')
        cur = self.conecta().cursor()
        cur.execute("SELECT a FROM t")
        cur.fetchone()
        with self.assertRaises(nishidb.OperationalError) as ctx:
            cur.fetchone()
        self.assertIn("sem a linha de fecho", str(ctx.exception))

    def test_primeira_linha_que_nao_e_schema_e_recusada(self):
        self.srv.responde_query_cru(b'{"a":"1"}\n')
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.OperationalError):
            cur.execute("SELECT a FROM t")

    def test_moldura_desconhecida_e_recusada(self):
        self.srv.responde_query(schema(coluna("a")), {"nishidb": "outra-coisa"})
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.OperationalError):
            cur.execute("SELECT a FROM t")

    def test_erro_antes_do_stream_traz_linha_e_coluna(self):
        self.srv.responde_erro("/v1/query", 400, "SYNTAX", "token inesperado", line=1, column=8)
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.ProgrammingError) as ctx:
            cur.execute("SELECT ~ FROM t")
        self.assertEqual((ctx.exception.line, ctx.exception.column), (1, 8))

    def test_clausula_nao_suportada_e_distinta_de_erro_de_sintaxe(self):
        self.srv.responde_erro("/v1/query", 400, "UNSUPPORTED_CLAUSE", "GROUP BY")
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.NotSupportedError):
            cur.execute("SELECT a FROM t GROUP BY a")

    def test_codigo_transiente_traz_a_dica_de_repetir(self):
        self.srv.responde(
            "/v1/query",
            Resposta(
                status=503,
                corpo=json.dumps(
                    {"error": {"code": "TOO_MANY_LONG_QUERIES", "message": "cheio"}}
                ).encode(),
                cabecalhos={"Retry-After": "30"},
            ),
        )
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.OperationalError) as ctx:
            cur.execute("SELECT a FROM t")
        self.assertTrue(ctx.exception.transient)
        self.assertEqual(ctx.exception.retry_after, 30.0)

    def test_codigo_desconhecido_nao_degrada_para_internal(self):
        # Degradar apagaria justamente a novidade que um servidor mais novo
        # está tentando contar.
        self.srv.responde_erro("/v1/query", 400, "CODIGO_DO_FUTURO", "algo novo")
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.DatabaseError) as ctx:
            cur.execute("SELECT a FROM t")
        self.assertEqual(ctx.exception.code, "CODIGO_DO_FUTURO")
        self.assertIn("CODIGO_DO_FUTURO", str(ctx.exception))
        self.assertNotIsInstance(ctx.exception, nishidb.InternalError)


class Capacidades(Base):
    def test_executemany_lanca(self):
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.NotSupportedError):
            cur.executemany("SELECT a FROM t", [(1,), (2,)])

    def test_commit_e_no_op(self):
        # A PEP 249 manda implementar sem efeito quando não há transação:
        # `with conexao:` chama commit ao sair, e lançar quebraria todo código
        # portável que só estava fechando o bloco.
        self.conecta().commit()

    def test_rollback_lanca(self):
        with self.assertRaises(nishidb.NotSupportedError):
            self.conecta().rollback()

    def test_cursor_fechado_recusa_uso(self):
        cur = self.conecta().cursor()
        cur.close()
        with self.assertRaises(nishidb.InterfaceError):
            cur.execute("SELECT a FROM t")

    def test_conexao_fechada_recusa_uso(self):
        cx = nishidb.connect(self.srv.dsn)
        cx.close()
        with self.assertRaises(nishidb.InterfaceError):
            cx.cursor()

    def test_dict_como_parametro_ensina_o_paramstyle(self):
        cur = self.conecta().cursor()
        with self.assertRaises(nishidb.NotSupportedError):
            cur.execute("SELECT a FROM t WHERE a = ?", {"a": 1})


if __name__ == "__main__":
    unittest.main()
