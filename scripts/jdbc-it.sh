#!/usr/bin/env bash
#
# jdbc-it.sh — sobe um NishiDB de verdade e roda o driver JDBC contra ele.
#
# Os testes unitários do driver falam com o servidor HTTP do próprio JDK, com
# respostas escritas à mão. Eles provam que o driver faz o que quem o escreveu
# ACHA que o servidor emite. Este script fecha a outra metade: o binário real,
# com TLS e credencial de verdade, servindo um dataset com um campo de cada
# tipo — é aqui que uma divergência entre o fio suposto e o fio real aparece.
#
# A montagem (build, certificado, corpus, servidor) mora em scripts/it-comum.sh,
# compartilhada com o client Python: uma segunda cópia do corpus divergiria da
# primeira no primeiro tipo novo, e o client testado contra a cópia velha
# passaria a provar menos que o outro sem nada acusar.
#
# Tudo o que ele cria vive num diretório temporário que o trap apaga: nenhum
# arquivo na árvore, nenhum certificado versionado, nenhuma porta fixa.
#
# Uso:
#   scripts/jdbc-it.sh
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
#
# Ferramenta ausente FALHA ALTO e nomeia o que instalar. Um teste de
# integração que vira SKIP porque falta o JDK não é teste: é a mesma cegueira
# do débito 53 por uma porta nova.

if ! command -v java >/dev/null 2>&1; then
  echo "erro: java não está no PATH — o teste de integração do driver exige JDK 21+." >&2
  echo "  Ubuntu/Debian: apt install openjdk-21-jdk" >&2
  exit 1
fi
VERSAO_JAVA="$(java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/')"
if [[ ! "$VERSAO_JAVA" =~ ^[0-9]+$ ]] || (( VERSAO_JAVA < 21 )); then
  echo "erro: java $VERSAO_JAVA é anterior ao piso 21 do driver." >&2
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

echo "== rodando os testes do driver contra o servidor real =="
cd "$RAIZ/clients/jdbc"
NISHIDB_IT_URL="jdbc:nishidb://127.0.0.1:$PORTA/telecom?tls=true&tlsCaFile=$TMP/cert.pem" \
NISHIDB_IT_URL_SEM_CA="jdbc:nishidb://127.0.0.1:$PORTA/telecom?tls=true" \
NISHIDB_IT_USER="leitor" \
NISHIDB_IT_PASSWORD="$SENHA" \
NISHIDB_IT_LINHAS="$LINHAS" \
NISHIDB_IT_REQUIRED=1 \
  ./mvnw -B test

echo "== ok =="
