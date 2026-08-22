#!/usr/bin/env python3
"""Testes do painel de texto. Só stdlib, no mesmo espírito do próprio painel
(ver o comentário do ProxyHandler em nishidb-stats.py): a ferramenta de
diagnóstico não pode exigir instalação no host em que se está diagnosticando,
e o teste dela também não.

O módulo é importado por CAMINHO, e não por `import nishidb_stats`: o nome do
arquivo tem hífen, que não é identificador Python válido para import direto.

Uso:
    python3 scripts/nishidb-stats_test.py
"""

from __future__ import annotations

import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

_CAMINHO = Path(__file__).parent / "nishidb-stats.py"
_SPEC = importlib.util.spec_from_file_location("nishidb_stats", _CAMINHO)
assert _SPEC is not None and _SPEC.loader is not None
nishidb_stats = importlib.util.module_from_spec(_SPEC)
# Registrado ANTES do exec_module: o módulo carregado usa @dataclass com
# `from __future__ import annotations`, e a resolução do dataclass procura o
# próprio módulo em sys.modules pelo nome — sem o registro prévio, o load
# quebra com "'NoneType' object has no attribute '__dict__'" em vez de
# executar o arquivo.
sys.modules[_SPEC.name] = nishidb_stats
_SPEC.loader.exec_module(nishidb_stats)


def _desenha(stats: dict) -> str:
    """Roda desenha() com saúde/amostra mínimas e devolve o texto impresso."""
    saude = {"status": "UP"}
    amostra = nishidb_stats.Amostra()
    buf = io.StringIO()
    with redirect_stdout(buf):
        nishidb_stats.desenha("127.0.0.1:9142", saude, stats, amostra, None)
    return buf.getvalue()


class TestCabecalhoAnunciaAVersao(unittest.TestCase):
    """A Task A3 alimenta process.version no /v1/stats; o painel só precisa
    lê-lo e mostrá-lo no cabeçalho — sem quebrar quando ele não vier."""

    def test_versao_presente_aparece_no_cabecalho(self):
        stats = {
            "process": {"version": "v9.9.9 (commit cafefeed, build 2026-01-02T03:04:05Z)"},
            "datasets": [],
        }
        saida = _desenha(stats)
        self.assertIn("v9.9.9 (commit cafefeed, build 2026-01-02T03:04:05Z)", saida)

    def test_servidor_antigo_sem_o_campo_nao_quebra(self):
        """/v1/stats de um binário anterior a esta task não tem
        process.version: o painel tem de desenhar normalmente."""
        stats = {"process": {}, "datasets": []}
        try:
            saida = _desenha(stats)
        except Exception as e:  # noqa: BLE001 — é exatamente "não pode quebrar" que se testa
            self.fail(f"desenha() levantou {e!r} com process.version ausente")
        self.assertNotIn("None", saida)

    def test_stats_sem_a_chave_process_nao_quebra(self):
        """Defesa extra: nem process em si é garantido vir de um servidor
        muito antigo ou de uma resposta parcial."""
        stats = {"datasets": []}
        try:
            _desenha(stats)
        except Exception as e:  # noqa: BLE001
            self.fail(f"desenha() levantou {e!r} sem a chave 'process'")


if __name__ == "__main__":
    unittest.main()
