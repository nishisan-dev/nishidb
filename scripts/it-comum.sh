# it-comum.sh — o harness de integração compartilhado pelos clients.
#
# Não é executável: é SOURCED por scripts/jdbc-it.sh e scripts/python-it.sh.
#
# Ele existe porque os dois precisam exatamente da mesma montagem — compilar o
# binário pelo alvo do Makefile, gerar certificado e senha efêmeros, ingerir um
# corpus com um campo de cada tipo do wire e subir o servidor com TLS e
# credencial — e uma segunda cópia dessa montagem divergiria da primeira no
# primeiro tipo novo. O corpus é o caso mais claro: ele codifica os valores de
# BORDA de cada tipo (int64 no limite, double na grafia 'g', 4-in-6, prefixo
# com host bits, json aninhado, dois formatos temporais), e um client testado
# contra uma cópia velha do corpus passaria a provar menos que o outro sem que
# nada acusasse.
#
# O que NÃO mora aqui: o pré-voo da linguagem (JDK / Python) e a execução da
# suíte, que são o que cada script tem de próprio.
#
# Contrato com quem faz o source:
#   entra: RAIZ (raiz do repositório), TMP (diretório temporário já criado)
#   sai:   SENHA, PORTA, SERVIDOR_PID, LINHAS

# it::exige_go falha alto quando o Go não está no PATH.
it::exige_go() {
  if ! command -v go >/dev/null 2>&1; then
    echo "erro: go não está no PATH — o script compila o servidor a partir da árvore." >&2
    exit 1
  fi
}

# it::compila_servidor builda pelo alvo do Makefile e copia o binário para o TMP.
#
# Pelo alvo, e não por um `go build` próprio: é ele que carimba os ldflags de
# versão, e os testes de integração conferem que o client publica essa versão.
# Uma segunda linha de build aqui divergiria da do Makefile em silêncio — e o
# sintoma seria o client anunciando "dev" para um binário de release.
it::compila_servidor() {
  echo "== compilando o servidor =="
  make -C "$RAIZ" build >/dev/null
  cp "$RAIZ/nishidb" "$TMP/nishidb"
}

# it::certificado_e_senha gera o par EC efêmero e a senha, exportando SENHA.
#
# A senha nasce aqui e vive num arquivo 0600. Ela chega ao processo de teste por
# VARIÁVEL DE AMBIENTE, nunca por flag: argumento de processo é legível por
# qualquer usuário da máquina em /proc/<pid>/cmdline (§95).
it::certificado_e_senha() {
  echo "== certificado e credencial =="
  go run "$RAIZ/scripts/certgen" -cert "$TMP/cert.pem" -key "$TMP/key.pem" -validade 2h
  SENHA="$(head -c 24 /dev/urandom | base64 | tr -d '/+=' | head -c 24)"
  umask 077
  printf '%s' "$SENHA" > "$TMP/senha.txt"
  chmod 600 "$TMP/senha.txt"
  umask 022
}

# it::prepara_dataset copia o dataset de teste com o teto de linhas elevado.
#
# O teto sobe só NESTA CÓPIA — o arquivo da árvore não é tocado, e o dataset de
# teste do repositório continua com o teto que os testes Go esperam.
it::prepara_dataset() {
  mkdir -p "$TMP/datasets" "$TMP/dados"
  sed 's/^  maxRows: .*/  maxRows: 1000000/' "$RAIZ/testdata/todos_os_tipos.yaml" \
    > "$TMP/datasets/todos_os_tipos.yaml"
}

# it::gera_corpus escreve $1 linhas de volume depois das linhas de borda.
#
# O campo `texto` tem índice BITMAP no schema, e por isso as linhas de volume
# repetem poucos valores nele: um bitmap por valor distinto significa um arquivo
# mmapeado por valor, e cem mil valores distintos esgotam o vm.max_map_count da
# máquina antes de a ingestão terminar. A variação do volume vai no `id` e no
# `inteiro`, que não são indexados.
it::gera_corpus() {
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

# it::ingere gera o corpus e o ingere, CONFERINDO a contagem aplicada.
#
# O código de saída NÃO basta. A política de erro da ingestão é log-and-skip: um
# evento que o schema recusa é descartado, anunciado no log, e o comando sai com
# ZERO. Um harness que confie só no exit code serve ao client um dataset menor do
# que o que ele gerou — e o teste de volume falha acusando o client por um defeito
# do corpus. Foi exatamente o que aconteceu uma vez: um IPv6 malformado a partir
# da linha 65 536 cortou o corpus pela metade, e o sintoma apareceu como "o driver
# não entregou todas as linhas".
it::ingere() {
  echo "== gerando e ingerindo $LINHAS linhas =="
  it::gera_corpus "$LINHAS" > "$TMP/corpus.ndjson"

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

  local esperadas aplicadas
  esperadas=$(wc -l < "$TMP/corpus.ndjson")
  aplicadas=$(sed -n 's/.*[^0-9]\([0-9][0-9]*\) aplicados.*/\1/p' "$TMP/ingest.log" | tail -1)
  if [[ "$aplicadas" != "$esperadas" ]]; then
    echo "erro: o corpus tem $esperadas linhas e a ingestão aplicou $aplicadas —" >&2
    echo "  o dataset serviria menos do que o teste espera, e a falha apareceria no client." >&2
    grep -i "descartad\|inválid\|invalid" "$TMP/ingest.log" >&2 || true
    exit 1
  fi
  echo "   corpus: $esperadas linhas geradas, $aplicadas aplicadas"
}

# it::sobe_servidor sobe o binário com TLS e Basic, exportando PORTA e SERVIDOR_PID.
it::sobe_servidor() {
  echo "== subindo o servidor com TLS e credencial =="
  # server.port: 0 é recusado pela configuração (porta zero não é uma porta
  # declarada), então a porta é sorteada e a tentativa de bind é o teste: o
  # net.Listen falha rápido e claro se ela estiver ocupada.
  local tentativa pronto fim
  # O leitor entra pela tabela de usuários, com o servidor ainda parado — o
  # mesmo caminho da gestão offline do operador. O nishidb.yaml da ingestão já
  # aponta o storage.path que o servidor vai usar; a primeira gestão cria a
  # tabela com o ADMIN de bootstrap, que o IT não usa.
  "$TMP/nishidb" users create --config "$TMP/nishidb.yaml" --name leitor --role READ \
    --new-password-file "$TMP/senha.txt" > "$TMP/usuarios.log" 2>&1 ||
    { echo "erro: criar o usuário leitor falhou" >&2; cat "$TMP/usuarios.log" >&2; exit 1; }
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
}
