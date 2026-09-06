"""Testes da string de conexão, da resolução de credencial e do `connect()`."""

from __future__ import annotations

import base64
import os
import tempfile
import unittest
from unittest import mock

from servidor_falso import ServidorFalso

import nishidb
from nishidb import dsn as _dsn
from nishidb.conexao import ENV_SENHA


class StringDeConexao(unittest.TestCase):
    def test_o_minimo(self):
        d = _dsn.parse("nishidb://db01/eventos")
        self.assertEqual((d.host, d.port, d.dataset), ("db01", 9142, "eventos"))
        self.assertFalse(d.tls)

    def test_porta_explicita(self):
        self.assertEqual(_dsn.parse("nishidb://db01:9999/t").port, 9999)

    def test_ipv6_mantem_os_colchetes(self):
        d = _dsn.parse("nishidb://[2001:db8::1]:9143/t")
        self.assertEqual((d.host, d.port), ("[2001:db8::1]", 9143))

    def test_ipv6_sem_porta_usa_a_padrao(self):
        self.assertEqual(_dsn.parse("nishidb://[::1]/t").port, 9142)

    def test_base_monta_o_esquema_certo(self):
        self.assertEqual(_dsn.parse("nishidb://h:1/t").base, "http://h:1")
        self.assertEqual(_dsn.parse("nishidb://h:1/t?tls=true").base, "https://h:1")

    def test_esquema_errado_e_recusado(self):
        with self.assertRaises(nishidb.InterfaceError):
            _dsn.parse("http://db01/t")

    def test_sem_dataset_e_recusado(self):
        with self.assertRaises(nishidb.InterfaceError):
            _dsn.parse("nishidb://db01")

    def test_mais_de_um_segmento_e_recusado(self):
        # Não há catálogo nem schema no NishiDB; aceitar faria o segundo sumir.
        with self.assertRaises(nishidb.InterfaceError):
            _dsn.parse("nishidb://db01/a/b")

    def test_credencial_embutida_e_recusada_e_nao_ignorada(self):
        # Ignorá-la faria a conexão sair ANÔNIMA sem ninguém notar.
        with self.assertRaises(nishidb.InterfaceError) as ctx:
            _dsn.parse("nishidb://user:senha@db01/t")
        self.assertIn("credencial embutida", str(ctx.exception))

    def test_senha_na_query_string_e_recusada(self):
        # Divergência deliberada do JDBC, cujo fallback a aceita na prática.
        for chave in ("password", "passwd", "pwd"):
            with self.subTest(chave=chave), self.assertRaises(nishidb.InterfaceError) as ctx:
                _dsn.parse(f"nishidb://db01/t?{chave}=x")
            self.assertIn(ENV_SENHA, str(ctx.exception))

    def test_tls_so_aceita_true_ou_false(self):
        self.assertTrue(_dsn.parse("nishidb://h/t?tls=true").tls)
        self.assertFalse(_dsn.parse("nishidb://h/t?tls=false").tls)
        for ruim in ("1", "yes", "", "TRUE"):
            with self.subTest(ruim=ruim), self.assertRaises(nishidb.InterfaceError):
                _dsn.parse(f"nishidb://h/t?tls={ruim}")

    def test_tls_ca_exige_tls(self):
        # Sem esta recusa, apontar a CA e esquecer tls=true conectaria em texto
        # claro com a sensação de ter configurado TLS.
        with self.assertRaises(nishidb.InterfaceError):
            _dsn.parse("nishidb://h/t?tls_ca=/etc/ca.pem")

    def test_opcao_desconhecida_e_ignorada(self):
        # Assimetria deliberada: tolerar o que não entendemos, nunca adivinhar
        # o que entendemos.
        self.assertEqual(_dsn.parse("nishidb://h/t?futuro=42").dataset, "t")

    def test_porta_invalida_e_recusada(self):
        for ruim in ("0", "70000", "abc"):
            with self.subTest(ruim=ruim), self.assertRaises(nishidb.InterfaceError):
                _dsn.parse(f"nishidb://h:{ruim}/t")

    def test_dataset_percent_encoded_e_decodificado(self):
        self.assertEqual(_dsn.parse("nishidb://h/meu%20ds").dataset, "meu ds")


class ResolucaoDeSenha(unittest.TestCase):
    """As mesmas quatro recusas de `internal/cli/credential.go`."""

    def setUp(self):
        self.srv = ServidorFalso().__enter__()
        self.addCleanup(self.srv.__exit__, None, None, None)
        self.env = mock.patch.dict(os.environ, {}, clear=False)
        self.env.start()
        os.environ.pop(ENV_SENHA, None)
        self.addCleanup(self.env.stop)

    def test_anonimo_nao_manda_authorization(self):
        cx = nishidb.connect(self.srv.dsn)
        self.addCleanup(cx.close)
        self.assertNotIn("Authorization", self.srv.ultima("/v1/metadata").cabecalhos)

    def test_basic_e_preemptivo_no_primeiro_request(self):
        # Sem desafio-resposta: toda falha de autenticação custa 100 ms FIXOS
        # no servidor, e o ciclo 401→repete pagaria isso em toda requisição.
        cx = nishidb.connect(self.srv.dsn, user="leitor", password="segredo")
        self.addCleanup(cx.close)
        esperado = "Basic " + base64.b64encode(b"leitor:segredo").decode()
        self.assertEqual(self.srv.ultima("/v1/metadata").cabecalhos["Authorization"], esperado)

    def test_senha_sem_usuario_e_recusada(self):
        with self.assertRaises(nishidb.InterfaceError):
            nishidb.connect(self.srv.dsn, password="x")

    def test_usuario_sem_senha_e_recusado(self):
        with self.assertRaises(nishidb.InterfaceError):
            nishidb.connect(self.srv.dsn, user="leitor")

    def test_duas_fontes_ao_mesmo_tempo_sao_recusadas(self):
        os.environ[ENV_SENHA] = "do-ambiente"
        with self.assertRaises(nishidb.InterfaceError) as ctx:
            nishidb.connect(self.srv.dsn, user="leitor", password="do-argumento")
        self.assertIn("exclusivos", str(ctx.exception))

    def test_variavel_definida_e_vazia_nao_e_o_mesmo_que_ausente(self):
        # O caso do container cujo orquestrador injeta a chave sem valor: sem
        # esta recusa, o Basic sairia com senha vazia e o 401 não daria pista.
        os.environ[ENV_SENHA] = ""
        with self.assertRaises(nishidb.InterfaceError) as ctx:
            nishidb.connect(self.srv.dsn, user="leitor")
        self.assertIn("VAZIA", str(ctx.exception))

    def test_senha_pela_variavel_de_ambiente(self):
        os.environ[ENV_SENHA] = "do-ambiente"
        cx = nishidb.connect(self.srv.dsn, user="leitor")
        self.addCleanup(cx.close)
        esperado = "Basic " + base64.b64encode(b"leitor:do-ambiente").decode()
        self.assertEqual(self.srv.ultima("/v1/metadata").cabecalhos["Authorization"], esperado)

    def test_senha_por_arquivo_com_quebra_de_linha_aparada(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write("do-arquivo\n")
            caminho = f.name
        self.addCleanup(os.unlink, caminho)
        cx = nishidb.connect(self.srv.dsn, user="leitor", password_file=caminho)
        self.addCleanup(cx.close)
        esperado = "Basic " + base64.b64encode(b"leitor:do-arquivo").decode()
        self.assertEqual(self.srv.ultima("/v1/metadata").cabecalhos["Authorization"], esperado)

    def test_arquivo_de_senha_vazio_e_recusado(self):
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
            f.write("\n")
            caminho = f.name
        self.addCleanup(os.unlink, caminho)
        with self.assertRaises(nishidb.InterfaceError):
            nishidb.connect(self.srv.dsn, user="leitor", password_file=caminho)

    def test_nenhuma_mensagem_ecoa_o_valor_da_senha(self):
        os.environ[ENV_SENHA] = "s3nh4-secreta"
        with self.assertRaises(nishidb.InterfaceError) as ctx:
            nishidb.connect(self.srv.dsn, user="leitor", password="outra-secreta")
        texto = str(ctx.exception)
        self.assertNotIn("s3nh4-secreta", texto)
        self.assertNotIn("outra-secreta", texto)


class Abertura(unittest.TestCase):
    def setUp(self):
        self.srv = ServidorFalso().__enter__()
        self.addCleanup(self.srv.__exit__, None, None, None)

    def test_os_tetos_sao_buscados_antes_da_primeira_consulta(self):
        cx = nishidb.connect(self.srv.dsn)
        self.addCleanup(cx.close)
        self.assertEqual(self.srv.ultima("/v1/metadata").metodo, "GET")
        self.assertEqual(cx.tetos_de("t"), (1000, 600000))

    def test_dataset_inexistente_falha_ao_abrir_a_conexao(self):
        # E não na primeira query, que é onde o usuário já perdeu o contexto.
        self.srv.responde_erro("/v1/metadata", 404, "UNKNOWN_DATASET", "não registrado")
        with self.assertRaises(nishidb.ProgrammingError):
            nishidb.connect(self.srv.dsn)

    def test_argumentos_vencem_a_string_de_conexao(self):
        # A mesma precedência de `Properties > URL` do driver JDBC.
        d = _dsn.parse("nishidb://h/t?user=daurl")
        self.assertEqual(d.user, "daurl")
        cx = nishidb.connect(self.srv.dsn + "?user=daurl", user="doargumento", password="x")
        self.addCleanup(cx.close)
        esperado = "Basic " + base64.b64encode(b"doargumento:x").decode()
        self.assertEqual(self.srv.ultima("/v1/metadata").cabecalhos["Authorization"], esperado)

    def test_sem_dsn_exige_host_e_dataset(self):
        with self.assertRaises(nishidb.InterfaceError):
            nishidb.connect(host="db01")

    def test_context_manager_fecha(self):
        with nishidb.connect(self.srv.dsn) as cx:
            self.assertIsNotNone(cx.cursor())
        with self.assertRaises(nishidb.InterfaceError):
            cx.cursor()

    def test_a_hierarquia_de_excecoes_esta_na_conexao(self):
        # A PEP 249 recomenda expô-la, para quem tem só o objeto.
        cx = nishidb.connect(self.srv.dsn)
        self.addCleanup(cx.close)
        self.assertIs(cx.ProgrammingError, nishidb.ProgrammingError)
        self.assertTrue(issubclass(cx.DataError, cx.DatabaseError))
        self.assertTrue(issubclass(cx.DatabaseError, cx.Error))


class Saude(unittest.TestCase):
    def setUp(self):
        self.srv = ServidorFalso().__enter__()
        self.addCleanup(self.srv.__exit__, None, None, None)

    def test_503_no_health_e_resposta_e_nao_falha(self):
        # É A resposta: o estado está no corpo, e tratá-lo como falha de
        # transporte esconderia o diagnóstico que a rota existe para dar.
        import json

        from servidor_falso import Resposta

        self.srv.responde(
            "/v1/health",
            Resposta(
                status=503,
                corpo=json.dumps(
                    {
                        "status": "DEGRADED",
                        "kafka": "DOWN",
                        "storage": "UP",
                        "datasets": {"t": "UP"},
                        "detalhes": {"t": "ingestão parada"},
                    }
                ).encode(),
            ),
        )
        cx = nishidb.connect(self.srv.dsn)
        self.addCleanup(cx.close)
        s = cx.health()
        self.assertEqual(s.status, "DEGRADED")
        self.assertFalse(s.up)
        self.assertEqual(s.detalhes["t"], "ingestão parada")


if __name__ == "__main__":
    unittest.main()
