#!/usr/bin/env bash
#
# python-it.sh — sobe um NishiDB de verdade e roda o client Python contra ele.
#
# Os testes unitários do client falam com um `http.server` da própria stdlib, com
# respostas escritas à mão. Eles provam que o client faz o que quem o escreveu
# ACHA que o servidor emite. Este script fecha a outra metade: o binário real,
# com TLS e credencial de verdade, servindo um dataset com um campo de cada
# tipo — é aqui que uma divergência entre o fio suposto e o fio real aparece.
#
# A montagem é a MESMA do driver JDBC, e mora em scripts/it-comum.sh justamente
# para que os dois clients sejam provados contra o mesmo corpus.
#
# Uso:
#   scripts/python-it.sh
#
# Variáveis:
#   NISHIDB_IT_LINHAS   quantas linhas ingerir (default 120000 — acima das
#                       100k que a prova de memória do cliente exige)
#   NISHIDB_IT_KEEP     1 mantém o diretório temporário (para depurar)

set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LINHAS="${NISHIDB_IT_LINHAS:-120000}"

# shellcheck source=scripts/it-comum.sh
source "$RAIZ/scripts/it-comum.sh"

# ------------------------------------------------------------------ pré-voo

if ! command -v python3 >/dev/null 2>&1; then
  echo "erro: python3 não está no PATH — o client exige Python 3.10 ou mais novo." >&2
  echo "  Ubuntu/Debian: apt install python3" >&2
  exit 1
fi
if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "erro: $(python3 --version) é anterior ao piso 3.10 do client." >&2
  exit 1
fi
it::exige_go

TMP="$(mktemp -d)"
SERVIDOR_PID=""

limpa() {
  local codigo=$?
  if [[ -n "$SERVIDOR_PID" ]] && kill -0 "$SERVIDOR_PID" 2>/dev/null; then
    kill "$SERVIDOR_PID" 2>/dev/null || true
    wait "$SERVIDOR_PID" 2>/dev/null || true
  fi
  if [[ "${NISHIDB_IT_KEEP:-0}" == "1" ]]; then
    echo "diretório mantido em $TMP (NISHIDB_IT_KEEP=1)" >&2
  else
    rm -rf "$TMP"
  fi
  exit $codigo
}
trap limpa EXIT INT TERM

it::compila_servidor
it::certificado_e_senha
it::prepara_dataset
it::ingere
it::sobe_servidor

echo "== rodando os testes do client contra o servidor real =="
cd "$RAIZ/clients/python"
# NISHIDB_IT_REQUIRED=1 é o que torna impossível o pior desfecho: harness
# quebrado, suíte verde e zero teste de integração executado.
PYTHONPATH="src" \
NISHIDB_IT_DSN="nishidb://127.0.0.1:$PORTA/todos_os_tipos?tls=true&tls_ca=$TMP/cert.pem" \
NISHIDB_IT_DSN_SEM_CA="nishidb://127.0.0.1:$PORTA/todos_os_tipos?tls=true" \
NISHIDB_IT_USER="leitor" \
NISHIDB_PASSWORD="$SENHA" \
NISHIDB_IT_LINHAS="$LINHAS" \
NISHIDB_IT_REQUIRED=1 \
  python3 -m unittest discover -s tests -p 'integracao*_test.py' -v

echo "== ok =="
