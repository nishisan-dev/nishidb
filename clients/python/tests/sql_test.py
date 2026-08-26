"""Testes do extrator de palpite.

Só stdlib (`unittest`), no mesmo espírito do painel `scripts/nishidb-stats.py`:
a suíte roda com `python3` e zero `pip install`, em qualquer máquina.
"""

from __future__ import annotations

import unittest

from nishidb import sql


class DatasetDoFrom(unittest.TestCase):
    def test_a_query_mais_simples_nomeia_o_dataset(self):
        self.assertEqual(sql.varre("SELECT * FROM eventos").dataset, "eventos")

    def test_a_caixa_do_nome_e_preservada(self):
        # Identificadores do NishiDB são case-sensitive e o binder resolve nome
        # EXATO: normalizar aqui faria o client pedir um dataset inexistente.
        self.assertEqual(sql.varre("SELECT * FROM RadiusSessions").dataset, "RadiusSessions")

    def test_from_em_qualquer_caixa_e_reconhecido(self):
        for grafia in ("from", "FROM", "From", "fRoM"):
            with self.subTest(grafia=grafia):
                self.assertEqual(sql.varre(f"SELECT a {grafia} t").dataset, "t")

    def test_espaco_nao_quebravel_nao_e_branco(self):
        # O oráculo da regra ASCII-only. `"\xa0".isspace()` é True no Python e
        # False no lexer do servidor: com `str.isspace()` no lugar da lista
        # fechada, o NBSP seria pulado e `t` viraria o dataset — um palpite que
        # o servidor jamais formaria.
        self.assertTrue("\xa0".isspace(), "premissa: o Python considera NBSP um branco")
        self.assertIsNone(sql.varre("SELECT a FROM\xa0t").dataset)

    def test_todo_branco_ascii_do_lexer_e_pulado(self):
        for branco in (" ", "\t", "\n", "\r", "\v", "\f"):
            with self.subTest(branco=repr(branco)):
                self.assertEqual(sql.varre(f"SELECT a FROM{branco}t").dataset, "t")

    def test_dataset_entre_aspas_sai_sem_as_aspas(self):
        self.assertEqual(sql.varre('SELECT * FROM "meu dataset"').dataset, "meu dataset")

    def test_aspa_duplicada_dentro_do_nome_e_escape(self):
        self.assertEqual(sql.varre('SELECT * FROM "a""b"').dataset, 'a"b')

    def test_nome_qualificado_abandona_o_palpite(self):
        # `_meta.fields` não é um dataset. Capturar "_meta" faria o servidor
        # responder 404; None faz valer o dataset da conexão.
        self.assertIsNone(sql.varre("SELECT name, type FROM _meta.fields").dataset)

    def test_ponto_depois_de_espaco_tambem_qualifica(self):
        self.assertIsNone(sql.varre("SELECT a FROM _meta . fields").dataset)

    def test_from_sem_nome_depois_nao_palpita(self):
        self.assertIsNone(sql.varre("SELECT * FROM").dataset)

    def test_coluna_chamada_from_entre_aspas_nao_dispara(self):
        # "from" entre aspas é NOME, nunca keyword.
        self.assertEqual(sql.varre('SELECT "from" FROM t').dataset, "t")

    def test_a_palavra_from_dentro_de_literal_nao_dispara(self):
        self.assertEqual(sql.varre("SELECT a FROM t WHERE s = 'FROM x'").dataset, "t")

    def test_sem_from_nenhum_o_dataset_e_nulo(self):
        self.assertIsNone(sql.varre("SELECT 1").dataset)

    def test_nunca_levanta_para_entrada_nenhuma(self):
        # O contrato central: é extrator de palpite, não validador. Quem recusa
        # SQL é o servidor, cuja mensagem tem posição de byte e nome de coluna.
        for entrada in ("", "   ", "'", '"', "SELECT * FROM 'aberto", "?" * 50, "\x00\xff FROM"):
            with self.subTest(entrada=entrada):
                sql.varre(entrada)  # não levantar já é o assert


class ContagemDeParametros(unittest.TestCase):
    def test_conta_os_interrogacao_do_where(self):
        self.assertEqual(sql.varre("SELECT * FROM t WHERE a = ? AND b = ?").parametros, 2)

    def test_o_dataset_nao_encerra_a_varredura(self):
        # Parar no dataset zeraria a contagem de toda query com parâmetro, e o
        # bind recusaria o índice 1.
        v = sql.varre("SELECT * FROM t WHERE a = ?")
        self.assertEqual(v.dataset, "t")
        self.assertEqual(v.parametros, 1)

    def test_interrogacao_dentro_de_literal_nao_conta(self):
        self.assertEqual(sql.varre("SELECT * FROM t WHERE s = '?'").parametros, 0)

    def test_aspa_duplicada_no_literal_mantem_o_interrogacao_dentro(self):
        # 'a''?''b' é UM literal com aspas escapadas. Sem a regra de escape, o
        # ? seria lido como estando fora dele.
        self.assertEqual(sql.varre("SELECT * FROM t WHERE s = 'a''?''b'").parametros, 0)

    def test_interrogacao_dentro_de_identificador_citado_nao_conta(self):
        self.assertEqual(sql.varre('SELECT "a?b" FROM t').parametros, 0)

    def test_literal_sem_fechamento_para_a_varredura(self):
        # O resto da entrada está DENTRO do literal; contá-lo inventaria
        # parâmetros que o servidor não tem.
        self.assertEqual(sql.varre("SELECT * FROM t WHERE s = 'aberto ? ?").parametros, 0)

    def test_sem_parametros_conta_zero(self):
        self.assertEqual(sql.varre("SELECT * FROM t").parametros, 0)


if __name__ == "__main__":
    unittest.main()
