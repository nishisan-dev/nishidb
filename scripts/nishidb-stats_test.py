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


def _amostra(instante: float, **series: float):
    """Amostra de /metrics com as séries do dataset `events`."""
    a = nishidb_stats.Amostra(instante=instante)
    for nome, v in series.items():
        a.series[(nome, (("dataset", "events"),))] = v
    return a


class TestPainelMostraEventosPorSegundo(unittest.TestCase):
    """Quem acompanha uma drenagem quer o ritmo, não só o acumulado."""

    def test_taxa_aparece_ao_lado_do_total(self):
        ant = _amostra(0.0, nishidb_ingest_events_total=1000)
        at = _amostra(2.0, nishidb_ingest_events_total=5000)
        saida = _desenha_com_amostras(_stats_com_dataset(), at, ant)
        self.assertRegex(saida, r"eventos ingeridos.*2[.,]000/s")


class TestPainelDizSeOConsumoDaConta(unittest.TestCase):
    """A pergunta que se faz olhando o lag é "está dando conta?", e ela
    precisa ser respondida por uma grandeza MEDIDA, não derivada.

    A primeira versão derivava a produção de `offset + lag`, supondo que a
    soma fosse o fim do tópico. Ela é — mas as duas parcelas não vêm do mesmo
    instante: o offset avança a cada lote e o lag é atualizado por consulta
    periódica ao broker. A soma anda para trás, e o painel chegou a imprimir
    `produz -310/s` no piloto.

    O lag em si é medido, e a variação dele JÁ é o balanço: caindo, o consumo
    dá conta; subindo, não.
    """

    def _saida(self, consumo, lag0, lag1, dt=30.0):
        ant = _amostra(0.0, nishidb_ingest_events_total=0.0, nishidb_kafka_lag=lag0)
        at = _amostra(dt, nishidb_ingest_events_total=consumo * dt, nishidb_kafka_lag=lag1)
        return _desenha_com_amostras(_stats_com_dataset(), at, ant)

    def test_lag_caindo_mostra_o_ritmo_e_o_eta(self):
        # Cai 1.500/s, e o ETA sai sobre o lag CORRENTE (105.000), não sobre o
        # da amostra anterior: 70 segundos.
        saida = self._saida(consumo=2000, lag0=150_000, lag1=105_000)
        self.assertIn("consome 2.000/s", saida)
        self.assertIn("caindo 1.500/s", saida)
        self.assertIn("1min 10s", saida)

    def test_lag_subindo_avisa_em_vez_de_calar(self):
        saida = self._saida(consumo=600, lag0=150_000, lag1=162_000)
        self.assertIn("consome 600/s", saida)
        self.assertIn("NÃO dá conta", saida)
        self.assertNotIn("ETA", saida)

    def test_lag_parado_nao_promete_prazo_nem_acusa_atraso(self):
        saida = self._saida(consumo=1000, lag0=150_000, lag1=150_000)
        self.assertIn("consome 1.000/s", saida)
        self.assertNotIn("ETA", saida)
        self.assertNotIn("NÃO dá conta", saida)

    def test_janela_curta_mostra_os_numeros_mas_nao_o_veredito(self):
        """Janela curta sobre produtor irregular MENTE.

        Medido no piloto: em 5 segundos o painel acusou "não dá conta", e em
        60 segundos a MESMA instância mostrou o lag caindo de verdade. Um
        vermelho errado custa mais que a ausência dele — manda investigar o que
        não existe e ensina a ignorar o painel.
        """
        saida = self._saida(consumo=600, lag0=150_000, lag1=151_600, dt=2.0)
        self.assertIn("consome 600/s", saida)
        self.assertIn("2s", saida)
        self.assertNotIn("NÃO dá conta", saida)

    def test_a_janela_aparece_na_linha(self):
        self.assertIn("em 30s", self._saida(consumo=2000, lag0=150_000, lag1=105_000))

    def test_nunca_afirma_producao_negativa(self):
        """O sintoma que derrubou a versão derivada: `produz -310/s`."""
        saida = self._saida(consumo=2000, lag0=150_000, lag1=105_000)
        self.assertNotIn("-", saida.split("lag do kafka")[1].split("\n")[0])


class TestPainelMostraOCrescimentoDoDicionario(unittest.TestCase):
    """O dicionário de campos e o que se perdeu no caminho.

    Nenhum dos dois aparecia: o `descartados` da ingestão conta EVENTOS, e
    campo recusado pelo teto ou valor que não coube no tipo não derrubam
    evento nenhum — eles ficavam invisíveis no painel mesmo já existindo no
    /v1/stats.
    """

    def _stats(self, **schema) -> dict:
        d = _stats_com_dataset()
        d["datasets"][0]["schema"] = schema
        d["datasets"][0]["ingestion"]["events"] = 188_618
        return d

    def test_campos_e_teto_aparecem(self):
        saida = _desenha(self._stats(fields=1495, fieldsDropped=0,
                                     nestedTypeMismatch=0, maxCols=65535,
                                     onMaxCols="ignore"))
        self.assertIn("1.495", saida)
        self.assertIn("65.535", saida)

    def test_valor_fora_do_tipo_aparece_com_a_proporcao(self):
        # Medido no piloto: um em cada cinco eventos traz pelo menos um valor
        # que não cabe no tipo fixado. O absoluto sozinho não diz se é cauda
        # ou regra; a proporção diz.
        saida = _desenha(self._stats(fields=1495, fieldsDropped=0,
                                     nestedTypeMismatch=39_951, maxCols=65535,
                                     onMaxCols="ignore"))
        self.assertIn("39.951", saida)
        self.assertIn("0,21", saida)

    def test_campo_descartado_pelo_teto_e_alerta_forte(self):
        # Aqui houve perda de DADO: a chave não virou coluna e o documento de
        # topo não a guarda em lugar nenhum.
        saida = _desenha(self._stats(fields=65535, fieldsDropped=12,
                                     nestedTypeMismatch=0, maxCols=65535,
                                     onMaxCols="ignore"))
        self.assertIn("12", saida)
        self.assertIn("investigar", saida)

    def test_servidor_sem_o_bloco_schema_nao_quebra(self):
        saida = _desenha(_stats_com_dataset())
        self.assertIn("dataset events", saida)


if __name__ == "__main__":
    unittest.main()


