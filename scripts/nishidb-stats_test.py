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


def _desenha_com_amostras(stats: dict, atual, anterior=None) -> str:
    """Como _desenha, mas com as Amostra() de /metrics escolhidas pelo teste —
    é o que permite simular duas raspagens do --watch (atual e anterior)."""
    saude = {"status": "UP"}
    buf = io.StringIO()
    with redirect_stdout(buf):
        nishidb_stats.desenha("127.0.0.1:9142", saude, stats, atual, anterior)
    return buf.getvalue()


def _stats_com_dataset(discarded: int | None = None) -> dict:
    ingestion: dict = {"state": "UP"}
    if discarded is not None:
        ingestion["discarded"] = discarded
    return {
        "process": {},
        "datasets": [{
            "name": "events",
            "storage": {},
            "ingestion": ingestion,
            "query": {},
        }],
    }


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


class TestDescartadosQuebramPorMotivo(unittest.TestCase):
    """Task A5 / issue #15: nishidb_ingest_invalid_total passa a carregar o
    rótulo `motivo` (internal/kafka/metricas.go) desde esta task. O painel
    tem de mostrar EVENT_TOO_LARGE NOMEADO — é a categoria que a issue #15
    investiga — e continuar funcionando, sem o detalhe, contra um binário
    anterior que publica o contador sem o rótulo."""

    def test_event_too_large_aparece_nomeado(self):
        atual = nishidb_stats.Amostra(series={
            ("nishidb_ingest_invalid_total", (("dataset", "events"), ("motivo", "EVENT_TOO_LARGE"))): 2.0,
            ("nishidb_ingest_invalid_total", (("dataset", "events"), ("motivo", "INVALID_JSON"))): 1.0,
        })
        saida = _desenha_com_amostras(_stats_com_dataset(discarded=3), atual)
        self.assertIn("EVENT_TOO_LARGE", saida)
        self.assertIn("INVALID_JSON", saida)
        self.assertIn("descartados", saida)

    def test_servidor_sem_o_rotulo_motivo_nao_quebra_e_mostra_o_total(self):
        """Binário anterior à Task A5: o contador vem só com `dataset`, sem
        `motivo`. Não pode quebrar, e o total continua aparecendo."""
        atual = nishidb_stats.Amostra(series={
            ("nishidb_ingest_invalid_total", (("dataset", "events"),)): 5.0,
        })
        try:
            saida = _desenha_com_amostras(_stats_com_dataset(discarded=5), atual)
        except Exception as e:  # noqa: BLE001 — "não pode quebrar" é o que se testa
            self.fail(f"desenha() levantou {e!r} sem o rótulo motivo")
        self.assertIn("descartados", saida)
        self.assertIn("5", saida)

    def test_sem_nenhuma_serie_de_invalid_total_nao_quebra(self):
        """Servidor sem nenhuma métrica de invalid ainda no ar (dataset
        recém-criado): por_rotulo devolve {} e a linha de total some junto
        com discarded=0 — nada disso pode lançar exceção."""
        try:
            saida = _desenha_com_amostras(_stats_com_dataset(discarded=0), nishidb_stats.Amostra())
        except Exception as e:  # noqa: BLE001
            self.fail(f"desenha() levantou {e!r} sem nenhuma série de invalid_total")
        self.assertNotIn("None", saida)


class TestTaxaDeDescartadosDestacaSubida(unittest.TestCase):
    """--watch: invalid SUBINDO entre duas amostras é o sinal forte (a causa
    está ativa AGORA); total > 0 estático e igual entre as duas amostras é a
    cauda já conhecida do piloto (docs/piloto-events.md, item 4) — aviso
    brando, não alarme."""

    def test_subindo_entre_amostras_e_alerta_forte(self):
        anterior = nishidb_stats.Amostra(
            series={("nishidb_ingest_invalid_total", (("dataset", "events"),)): 10.0},
            instante=0.0,
        )
        atual = nishidb_stats.Amostra(
            series={("nishidb_ingest_invalid_total", (("dataset", "events"),)): 15.0},
            instante=5.0,
        )
        saida = _desenha_com_amostras(_stats_com_dataset(discarded=15), atual, anterior)
        self.assertIn("SUBINDO", saida)

    def test_estatico_entre_amostras_e_apenas_aviso_brando(self):
        anterior = nishidb_stats.Amostra(
            series={("nishidb_ingest_invalid_total", (("dataset", "events"),)): 10.0},
            instante=0.0,
        )
        atual = nishidb_stats.Amostra(
            series={("nishidb_ingest_invalid_total", (("dataset", "events"),)): 10.0},
            instante=5.0,
        )
        saida = _desenha_com_amostras(_stats_com_dataset(discarded=10), atual, anterior)
        self.assertNotIn("SUBINDO", saida)
        self.assertIn("descartados", saida)

    def test_sem_amostra_anterior_nao_acusa_subida(self):
        """Primeira leitura (sem --watch, ou primeira iteração dele): não há
        'anterior' contra o que comparar, então não pode soar o alarme forte
        por falta de dado — só o aviso brando de histórico, se houver total."""
        atual = nishidb_stats.Amostra(
            series={("nishidb_ingest_invalid_total", (("dataset", "events"),)): 10.0},
        )
        saida = _desenha_com_amostras(_stats_com_dataset(discarded=10), atual, None)
        self.assertNotIn("SUBINDO", saida)


if __name__ == "__main__":
    unittest.main()
