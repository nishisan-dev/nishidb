#!/usr/bin/env python3
"""Painel de texto do NishiDB: lê /v1/health, /v1/stats e /metrics e imprime
em formato legível.

Sem dependência externa — só a stdlib, pelo mesmo motivo que o engine é um
binário único: a ferramenta de diagnóstico não pode exigir instalação no
servidor em que se está diagnosticando.

Uso:
    nishidb-stats.py                          # uma leitura
    nishidb-stats.py --watch 10               # repete a cada 10s, com taxas
    nishidb-stats.py --server 10.0.0.1:9142
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

TEMPO_LIMITE = 8.0


class ErroDeColeta(Exception):
    """Falha ao falar com o servidor. Nunca é silenciada: painel que inventa
    número é pior que painel que não abre."""


# --------------------------------------------------------------------------- coleta


# Abridor SEM proxy, deliberadamente. O urllib honra `http_proxy` do ambiente,
# e num host corporativo isso manda a consulta a 127.0.0.1 para o proxy da
# empresa — que responde 403 e faz o painel acusar o servidor de estar fora do
# ar. Foi o que aconteceu: /root/.bashrc exporta http_proxy, e como .bashrc só
# vale em shell INTERATIVO, o erro aparecia para quem digitava o comando e não
# para quem o rodava por ssh não-interativo.
#
# Não há caso em que este painel deva usar proxy: ele fala com um servidor que
# o operador nomeou, quase sempre no próprio host. ProxyHandler({}) desliga a
# descoberta por ambiente sem depender de no_proxy estar configurado.
_ABRIDOR = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _busca(base: str, rota: str) -> str:
    try:
        with _ABRIDOR.open(f"http://{base}{rota}", timeout=TEMPO_LIMITE) as r:
            return r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise ErroDeColeta(f"{rota} devolveu HTTP {e.code}") from e
    except (urllib.error.URLError, OSError) as e:
        raise ErroDeColeta(f"{rota} inalcançável: {e}") from e


def busca_json(base: str, rota: str) -> dict[str, Any]:
    bruto = _busca(base, rota)
    try:
        return json.loads(bruto)
    except json.JSONDecodeError as e:
        raise ErroDeColeta(f"{rota} não devolveu JSON: {bruto[:120]}") from e


@dataclass
class Amostra:
    """Uma raspagem do /metrics: valor por (nome, rótulos)."""

    series: dict[tuple[str, tuple[tuple[str, str], ...]], float] = field(default_factory=dict)
    instante: float = 0.0

    def valor(self, nome: str, **rotulos: str) -> float | None:
        for (n, rs), v in self.series.items():
            if n != nome:
                continue
            d = dict(rs)
            if all(d.get(k) == val for k, val in rotulos.items()):
                return v
        return None

    def soma(self, nome: str) -> float:
        return sum(v for (n, _), v in self.series.items() if n == nome)

    def por_rotulo(self, nome: str, rotulo: str) -> dict[str, float]:
        out: dict[str, float] = {}
        for (n, rs), v in self.series.items():
            if n == nome:
                d = dict(rs)
                if rotulo in d:
                    out[d[rotulo]] = out.get(d[rotulo], 0.0) + v
        return out

    def baldes(self, nome: str) -> list[tuple[float, float]]:
        """Baldes cumulativos de um histograma, ordenados por limite."""
        out: list[tuple[float, float]] = []
        for (n, rs), v in self.series.items():
            if n != f"{nome}_bucket":
                continue
            d = dict(rs)
            le = d.get("le")
            if le is None:
                continue
            out.append((float("inf") if le == "+Inf" else float(le), v))
        return sorted(out)


def raspa(base: str) -> Amostra:
    a = Amostra(instante=time.monotonic())
    for linha in _busca(base, "/metrics").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#"):
            continue
        try:
            corpo, valor = linha.rsplit(" ", 1)
            v = float(valor)
        except ValueError:
            continue
        if "{" in corpo:
            nome, resto = corpo.split("{", 1)
            rotulos: list[tuple[str, str]] = []
            for par in resto.rstrip("}").split(","):
                if "=" in par:
                    k, _, val = par.partition("=")
                    rotulos.append((k.strip(), val.strip().strip('"')))
            a.series[(nome, tuple(sorted(rotulos)))] = v
        else:
            a.series[(corpo, ())] = v
    return a


# --------------------------------------------------------------------------- formato


def bytes_legiveis(n: float | None) -> str:
    if n is None:
        return "—"
    unidade = ["B", "KiB", "MiB", "GiB", "TiB"]
    i, f = 0, float(n)
    while f >= 1024 and i < len(unidade) - 1:
        f /= 1024
        i += 1
    return f"{f:,.1f} {unidade[i]}".replace(",", ".") if i else f"{int(f)} B"


def milhar(n: float | None) -> str:
    if n is None:
        return "—"
    return f"{int(n):,}".replace(",", ".")


def duracao(seg: float | None) -> str:
    if seg is None or seg <= 0:
        return "—"
    seg = int(seg)
    if seg < 60:
        return f"{seg}s"
    if seg < 3600:
        return f"{seg // 60}min {seg % 60}s"
    return f"{seg // 3600}h {(seg % 3600) // 60}min"


def percentil(baldes: list[tuple[float, float]], p: float) -> str:
    """Percentil por interpolação nos baldes cumulativos, que é o que um
    histograma permite — não é o valor exato, e o nome do campo diz isso."""
    if not baldes:
        return "—"
    total = baldes[-1][1]
    if total <= 0:
        return "—"
    alvo = total * p
    anterior = 0.0
    for limite, acumulado in baldes:
        if acumulado >= alvo:
            if limite == float("inf"):
                return f">{anterior * 1000:.0f}ms"
            return f"{limite * 1000:.1f}ms"
        anterior = limite
    return "—"


# Cor só em terminal: redirecionado para arquivo, o escape vira lixo no meio
# do texto e atrapalha justamente quem foi salvar a saída para comparar depois.
COR = sys.stdout.isatty()


def c(codigo: str, texto: str) -> str:
    return f"\033[{codigo}m{texto}\033[0m" if COR else texto


def regua(titulo: str) -> str:
    largura = min(shutil.get_terminal_size((90, 24)).columns, 96)
    return f"\n{c('1', titulo)} " + "─" * max(0, largura - len(titulo) - 2)


def taxa(atual: float | None, anterior: float | None, dt: float, *, em_bytes: bool = False) -> str:
    """Variação por segundo entre duas leituras. Série de bytes sai em unidade
    humana: "+6.0 MiB/s" se lê, "+6.249.622/s" não."""
    if atual is None or anterior is None or dt <= 0:
        return ""
    d = (atual - anterior) / dt
    if abs(d) < 0.01:
        return "  (parado)"
    if em_bytes:
        return f"  ({'+' if d > 0 else '-'}{bytes_legiveis(abs(d))}/s)"
    return f"  ({d:+,.0f}/s)".replace(",", ".")


# --------------------------------------------------------------------------- painel


def desenha(base: str, saude: dict, stats: dict, m: Amostra, ant: Amostra | None) -> None:
    dt = (m.instante - ant.instante) if ant else 0.0

    estado = saude.get("status", "?")
    print(regua(f"NishiDB {base}"))
    partes = [c("32" if estado == "UP" else "31", estado)]
    for chave in ("kafka", "storage"):
        if chave in saude:
            partes.append(f"{chave}={saude[chave]}")
    print("  " + "   ".join(partes))
    if saude.get("detalhes"):
        for k, v in saude["detalhes"].items():
            print("    " + c("33", f"{k}: {v}"))

    for ds in stats.get("datasets", []):
        nome = ds.get("name", "?")
        st = ds.get("storage", {})
        ing = ds.get("ingestion", {})
        q = ds.get("query", {})
        comp = st.get("compaction", {})

        print(regua(f"dataset {nome}"))
        disco = st.get("bytes")
        lixo = st.get("garbageBytes")
        vivo = (disco - lixo) if (disco is not None and lixo is not None) else None
        pct = f" ({100 * lixo / disco:.0f}%)" if disco and lixo else ""

        print(f"  {'entidades vivas':<22} {milhar(st.get('rows')):>16}"
              f"{taxa(m.valor('nishidb_materialized_rows', dataset=nome), ant.valor('nishidb_materialized_rows', dataset=nome) if ant else None, dt)}")
        print(f"  {'disco':<22} {bytes_legiveis(disco):>16}"
              f"{taxa(m.valor('nishidb_storage_bytes', dataset=nome), ant.valor('nishidb_storage_bytes', dataset=nome) if ant else None, dt, em_bytes=True)}")
        print(f"  {'  garbage (≥)':<22} {bytes_legiveis(lixo):>16}{pct}")
        print(f"  {'  vivo (≤)':<22} {bytes_legiveis(vivo):>16}")
        print(f"  {'segmentos / corrente':<22} {str(st.get('segments', '—')) + ' / ' + str(st.get('currentSegment', '—')):>16}")

        # `running` diz se há passada EM VOO agora, não se a compaction está
        # habilitada — ela desce quando o laço termina. Ler os dois como a
        # mesma coisa faria o painel anunciar "desligada" logo depois de ela
        # ter devolvido gigabytes. Quem responde "devolveu alguma coisa?" é o
        # contador de bytes, não o medidor.
        lidos, escritos = comp.get("bytesRead"), comp.get("bytesWritten")
        devolvido = (lidos - escritos) if (lidos is not None and escritos is not None) else None
        if comp.get("running"):
            estado_comp = "moendo agora"
        elif escritos:
            estado_comp = "ociosa"
        else:
            estado_comp = "sem passada ainda"
        print(f"  {'compaction':<22} {estado_comp:>16}"
              f"{taxa(m.valor('nishidb_compaction_bytes_written', dataset=nome), ant.valor('nishidb_compaction_bytes_written', dataset=nome) if ant else None, dt, em_bytes=True)}")
        print(f"  {'  devolvido ao disco':<22} {bytes_legiveis(devolvido):>16}"
              f"   (leu {bytes_legiveis(lidos)}, reescreveu {bytes_legiveis(escritos)})")

        # Ingestão
        ev = m.valor("nishidb_ingest_events_total", dataset=nome)
        ev_ant = ant.valor("nishidb_ingest_events_total", dataset=nome) if ant else None
        lag = m.soma("nishidb_kafka_lag")
        estado_ing = ing.get("state", "?")
        marca_ing = "" if estado_ing == "UP" else "   " + c("31", f"ingestão {estado_ing}")
        print(f"  {'eventos ingeridos':<22} {milhar(ev):>16}{taxa(ev, ev_ant, dt)}{marca_ing}")
        # `applied` é o que virou estado; a diferença para `events` é o que o
        # dataset RECUSOU, e é a primeira coisa a olhar quando o número de
        # entidades não bate com o esperado.
        aplicados = ing.get("applied")
        if aplicados is not None and ev is not None and aplicados != ev:
            print(f"  {'  aplicados':<22} {milhar(aplicados):>16}"
                  f"   " + c("33", f"({milhar(ev - aplicados)} não viraram estado)"))
        for rotulo, chave, serie in (("descartados", "discarded", "nishidb_ingest_invalid_total"),
                                     ("stale", "stale", "nishidb_ingest_stale_total"),
                                     ("falhos", None, "nishidb_ingest_failed_total")):
            v = ing.get(chave) if chave else None
            if v is None:
                v = m.valor(serie, dataset=nome)
            alerta = ("  " + c("31", "← investigar")) if v and v > 0 and rotulo != "stale" else ""
            print(f"  {'  ' + rotulo:<22} {milhar(v):>16}{alerta}")

        if lag:
            eta = ""
            if ev is not None and ev_ant is not None and dt > 0:
                drenagem = (ev - ev_ant) / dt
                producao = 452.0  # medida no tópico; só para estimar o ETA
                liquido = drenagem - producao
                if liquido > 1:
                    eta = f"   ETA ≈ {duracao(lag / liquido)}"
            print(f"  {'lag do kafka':<22} {milhar(lag):>16}{eta}")

        # Consultas
        total = q.get("total")
        if total:
            b = m.baldes("nishidb_query_latency_seconds")
            print(f"  {'consultas':<22} {milhar(total):>16}"
                  f"   ativas {q.get('active', 0)}   p50 {percentil(b, .5)}  p95 {percentil(b, .95)}  p99 {percentil(b, .99)}")
            erros = m.por_rotulo("nishidb_query_errors", "categoria")
            if erros:
                print(f"  {'  erros':<22} {', '.join(f'{k}={int(v)}' for k, v in sorted(erros.items())):>16}")

    print(regua("processo"))
    rss = m.valor("nishidb_process_rss_bytes")
    heap = m.valor("nishidb_go_heap_bytes")
    print(f"  {'RSS':<22} {bytes_legiveis(rss):>16}   heap Go {bytes_legiveis(heap)}")
    ult = m.valor("nishidb_checkpoint_last_success_timestamp")
    if ult:
        idade = time.time() - ult
        alerta = ("  " + c("31", "← parado")) if idade > 300 else ""
        print(f"  {'último checkpoint':<22} {'há ' + duracao(idade):>16}{alerta}")
    n = m.soma("nishidb_checkpoint_duration_seconds_count")
    s = m.soma("nishidb_checkpoint_duration_seconds_sum")
    if n:
        print(f"  {'checkpoints':<22} {milhar(n):>16}   média {s / n * 1000:.0f}ms")
    print()
    # Flush explícito: sob pipe o Python bufferiza, e `--watch >> painel.log`
    # — que é o uso de long run — perderia a leitura se o processo fosse
    # encerrado entre um desenho e o próximo.
    sys.stdout.flush()


def main() -> int:
    ap = argparse.ArgumentParser(description="Painel de texto do NishiDB.")
    ap.add_argument("--server", default="127.0.0.1:9142", help="host:porta (padrão 127.0.0.1:9142)")
    ap.add_argument("--watch", type=float, metavar="SEG",
                    help="repete a cada SEG segundos, mostrando taxas entre leituras")
    ap.add_argument("--json", action="store_true", help="despeja o /v1/stats cru e sai")
    args = ap.parse_args()

    try:
        if args.json:
            print(json.dumps(busca_json(args.server, "/v1/stats"), indent=2, ensure_ascii=False))
            return 0

        anterior: Amostra | None = None
        while True:
            saude = busca_json(args.server, "/v1/health")
            stats = busca_json(args.server, "/v1/stats")
            atual = raspa(args.server)
            # Só limpa a tela num terminal de verdade: sob pipe ou redireção
            # o escape apagaria justamente a saída que se quis capturar.
            if args.watch and sys.stdout.isatty():
                print("\033[2J\033[H", end="")
            desenha(args.server, saude, stats, atual, anterior)
            if not args.watch:
                return 0
            anterior = atual
            time.sleep(args.watch)
    except ErroDeColeta as e:
        print(f"{c('31', 'erro:')} {e}", file=sys.stderr)
        print("  o servidor está no ar? systemctl status nishidb", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
