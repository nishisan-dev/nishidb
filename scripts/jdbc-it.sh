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
if ! command -v go >/dev/null 2>&1; then
  echo "erro: go não está no PATH — o script compila o servidor a partir da árvore." >&2
  exit 1
fi

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

echo "== 1/6 compilando o servidor =="
# Pelo alvo do Makefile, e não por um `go build` próprio: é ele que carimba os
# ldflags de versão, e o teste de integração confere que o driver publica essa
# versão em getDatabaseProductVersion. Uma segunda linha de build aqui
# divergiria da do Makefile em silêncio — e o sintoma seria o driver anunciando
# "dev" para um binário de release.
make -C "$RAIZ" build >/dev/null
cp "$RAIZ/nishidb" "$TMP/nishidb"

echo "== 2/6 certificado e credencial =="
go run "$RAIZ/scripts/certgen" -cert "$TMP/cert.pem" -key "$TMP/key.pem" -validade 2h
# A senha nasce aqui e vive num arquivo 0600. Ela chega à JVM por VARIÁVEL DE
# AMBIENTE, nunca por flag: argumento de processo é legível por qualquer
# usuário da máquina em /proc/<pid>/cmdline.
SENHA="$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)"
umask 077
printf '%s' "$SENHA" > "$TMP/senha.txt"
chmod 600 "$TMP/senha.txt"
umask 022

mkdir -p "$TMP/datasets" "$TMP/dados"
# O teto de linhas do corpus é 1000, e a prova de memória do cliente precisa do
# resultado INTEIRO atravessando o driver. O teto sobe só NESTA CÓPIA — o
# arquivo da árvore não é tocado, e o dataset de teste do repositório continua
# com o teto que os testes Go esperam.
sed 's/^  maxRows: .*/  maxRows: 1000000/' "$RAIZ/testdata/todos_os_tipos.yaml" \
  > "$TMP/datasets/todos_os_tipos.yaml"

echo "== 3/6 gerando e ingerindo $LINHAS linhas =="
# O campo `texto` tem índice BITMAP no schema, e por isso as linhas de volume
# repetem poucos valores nele: um bitmap por valor distinto significa um arquivo
# mmapeado por valor, e cem mil valores distintos esgotam o vm.max_map_count da
# máquina antes de a ingestão terminar. A variação do volume vai no `id` e no
# `inteiro`, que não são indexados.
#
# O corpus tem duas partes. As primeiras linhas são a MATRIZ DE TIPOS: valores
# de borda escolhidos (int64 no limite, double na grafia 'g', endereço 4-in-6,
# prefixo com host bits, json aninhado, os dois formatos temporais). O resto é
# volume, para a prova de memória do cliente — o driver tem de percorrer isso
# com heap fixo.
gera_ndjson() {
  cat <<'BORDA'
{"id":"borda-1","_SEQ":1,"texto":"linha com \"aspas\" e \\barra","inteiro":9223372036854775807,"real":1e-07,"booleano":true,"quando":1787570000123,"v4":"255.255.255.255","v6":"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff","rede4":"10.0.0.1/24","rede6":"2001:db8::1/32","cru":"SGVsbG8=","doc":{"a":[1,2,{"b":null}]},"momento":"Aug 23, 2026 09:53:04 PM","momento_iso":"2026-08-23T21:53:04","hora":"11:59:59 PM","hora_iso":"23:59:59.999"}
{"id":"borda-2","_SEQ":1,"texto":"","inteiro":-9223372036854775808,"real":-1.7976931348623157e+308,"booleano":false,"quando":0,"v4":"0.0.0.0","v6":"::ffff:1.2.3.4","rede4":"0.0.0.0/0","rede6":"::/0","cru":"","doc":{},"momento":"Jan 1, 1970 12:00:00 AM","momento_iso":"1970-01-01T00:00:00","hora":"12:00:00 AM","hora_iso":"00:00:00.000"}
BORDA
  awk -v n="$1" 'BEGIN {
    for (i = 1; i <= n; i++) {
      printf "{\"id\":\"linha-%d\",\"_SEQ\":1,\"texto\":\"grupo-%d\",\"inteiro\":%d,\"real\":%d.5,\"booleano\":%s,\"quando\":%d,\"v4\":\"10.0.%d.%d\",\"v6\":\"2001:db8::%x:%x\",\"rede4\":\"10.0.0.0/24\",\"rede6\":\"2001:db8::/32\",\"cru\":\"SGVsbG8=\",\"doc\":{\"n\":%d},\"momento\":\"Aug 23, 2026 09:53:04 PM\",\"momento_iso\":\"2026-08-23T21:53:04\",\"hora\":\"11:59:59 PM\",\"hora_iso\":\"23:59:59.999\"}\n", \
        i, (i % 8), i, i, (i % 2 ? "true" : "false"), 1787570000000 + i, (i / 256) % 256, i % 256, int(i / 65536), i % 65536, i
    }
  }' < /dev/null
}
gera_ndjson "$LINHAS" > "$TMP/corpus.ndjson"

cat > "$TMP/nishidb.yaml" <<YAML
version: 1
storage:
  path: $TMP/dados
datasets:
  directory: $TMP/datasets
YAML

"$TMP/nishidb" ingest --config "$TMP/nishidb.yaml" --dataset todos_os_tipos \
  --input "$TMP/corpus.ndjson" > "$TMP/ingest.log" 2>&1 ||
  { echo "erro: a ingestão falhou" >&2; tail -20 "$TMP/ingest.log" >&2; exit 1; }

# O código de saída NÃO basta. A política de erro da ingestão é log-and-skip:
# um evento que o schema recusa é descartado, anunciado no log, e o comando sai
# com ZERO. Um harness que confie só no exit code serve ao driver um dataset
# menor do que o que ele gerou — e o teste de volume falha acusando o driver por
# um defeito do corpus. Foi exatamente o que aconteceu: um IPv6 malformado a
# partir da linha 65 536 cortou o corpus pela metade, e o sintoma apareceu como
# "o driver não entregou todas as linhas".
ESPERADAS=$(wc -l < "$TMP/corpus.ndjson")
APLICADAS=$(sed -n 's/.*[^0-9]\([0-9][0-9]*\) aplicados.*/\1/p' "$TMP/ingest.log" | tail -1)
if [[ "$APLICADAS" != "$ESPERADAS" ]]; then
  echo "erro: o corpus tem $ESPERADAS linhas e a ingestão aplicou $APLICADAS —" >&2
  echo "  o dataset serviria menos do que o teste espera, e a falha apareceria no driver." >&2
  grep -i "descartad\|inválid\|invalid" "$TMP/ingest.log" >&2 || true
  exit 1
fi
echo "   corpus: $ESPERADAS linhas geradas, $APLICADAS aplicadas"

echo "== 4/6 subindo o servidor com TLS e credencial =="
# server.port: 0 é recusado pela configuração (porta zero não é uma porta
# declarada), então a porta é sorteada e a tentativa de bind é o teste: o
# net.Listen falha rápido e claro se ela estiver ocupada.
for tentativa in 1 2 3 4 5; do
  PORTA=$(( (RANDOM % 40000) + 20000 ))
  cat > "$TMP/nishidb.yaml" <<YAML
version: 1
storage:
  path: $TMP/dados
datasets:
  directory: $TMP/datasets
server:
  bind: 127.0.0.1
  port: $PORTA
  tls:
    enabled: true
    certificateFile: $TMP/cert.pem
    privateKeyFile: $TMP/key.pem
  authentication:
    enabled: true
    users:
      - username: leitor
        passwordFile: $TMP/senha.txt
        role: READ
YAML
  "$TMP/nishidb" server --config "$TMP/nishidb.yaml" > "$TMP/servidor.log" 2>&1 &
  SERVIDOR_PID=$!

  # Prontidão pela LINHA DE LOG, não por sleep: um sleep fixo ou passa a
  # esperar demais numa máquina lenta ou falha intermitente numa carregada.
  pronto=0
  fim=$(( $(date +%s) + 30 ))
  while [[ $(date +%s) -lt $fim ]]; do
    if grep -q "query.pronta" "$TMP/servidor.log" 2>/dev/null; then
      pronto=1
      break
    fi
    if ! kill -0 "$SERVIDOR_PID" 2>/dev/null; then
      break
    fi
    sleep 0.2
  done

  if [[ $pronto -eq 1 ]]; then
    break
  fi
  kill "$SERVIDOR_PID" 2>/dev/null || true
  wait "$SERVIDOR_PID" 2>/dev/null || true
  SERVIDOR_PID=""
  # Só COLISÃO DE PORTA merece outra tentativa. Qualquer outra falha — config
  # inválida, dataset ilegível, certificado ruim — repetiria idêntica cinco
  # vezes e sairia com a mensagem errada ("5 portas tentadas"), mandando
  # procurar uma porta ocupada que não existe.
  if ! grep -qi "address already in use\|bind" "$TMP/servidor.log" 2>/dev/null; then
    echo "erro: o servidor não subiu, e não foi por porta ocupada:" >&2
    cat "$TMP/servidor.log" >&2
    exit 1
  fi
  if [[ $tentativa -eq 5 ]]; then
    echo "erro: o servidor não ficou pronto em 30s (5 portas tentadas)" >&2
    cat "$TMP/servidor.log" >&2
    exit 1
  fi
done

echo "   servidor pronto em 127.0.0.1:$PORTA"

echo "== 5/6 rodando os testes do driver contra o servidor real =="
cd "$RAIZ/clients/jdbc"
NISHIDB_IT_URL="jdbc:nishidb://127.0.0.1:$PORTA/todos_os_tipos?tls=true&tlsCaFile=$TMP/cert.pem" \
NISHIDB_IT_URL_SEM_CA="jdbc:nishidb://127.0.0.1:$PORTA/todos_os_tipos?tls=true" \
NISHIDB_IT_USER="leitor" \
NISHIDB_IT_PASSWORD="$SENHA" \
NISHIDB_IT_LINHAS="$LINHAS" \
NISHIDB_IT_REQUIRED=1 \
  ./mvnw -B test

echo "== 6/6 ok =="
