#!/usr/bin/env bash

set -euo pipefail

#
# certgen.sh
#
# Gera uma CA temporária e um certificado de servidor assinado por ela
# para testes de integração JDBC.
#
# Arquivos gerados:
#
#   ca.pem       Certificado da CA
#   server.crt   Certificado do servidor
#   server.key   Chave privada do servidor
#
# A chave privada da CA é criada apenas temporariamente e removida ao final.
#
# Características:
#
#   - ECDSA P-256
#   - CA própria
#   - Certificado do servidor assinado pela CA
#   - Extended Key Usage: serverAuth
#   - SAN padrão:
#       DNS:localhost
#       IP:127.0.0.1
#       IP:::1
#
# Opcionalmente:
#
#   -auto-ips
#
# adiciona automaticamente os IPs encontrados nas interfaces da máquina.
#
# Também é possível adicionar IPs manualmente:
#
#   -ip 10.113.144.228
#   -ip 192.168.1.10
#
# Uso:
#
#   ./scripts/certgen.sh \
#       -cert /tmp/server.crt \
#       -key /tmp/server.key \
#       -ca /tmp/ca.pem \
#       -validade 2h \
#       -auto-ips
#
# Ou:
#
#   ./scripts/certgen.sh \
#       -cert /tmp/server.crt \
#       -key /tmp/server.key \
#       -ca /tmp/ca.pem \
#       -ip 10.113.144.228
#

CERT_FILE=""
KEY_FILE=""
CA_FILE=""
VALIDADE="2h"

AUTO_IPS=false

declare -a EXTRA_IPS=()

usage() {
    cat <<EOF
Uso:

  $0 -cert <arquivo> -key <arquivo> [opções]

Opções:

  -cert <arquivo>
      Caminho do certificado do servidor.

  -key <arquivo>
      Caminho da chave privada do servidor.

  -ca <arquivo>
      Caminho do certificado da CA.

      Padrão:
          ca.pem no mesmo diretório de -cert.

  -validade <duracao>
      Validade do certificado do servidor.

      Exemplos:
          30m
          2h
          12h
          1d

      Padrão:
          2h

  -auto-ips
      Detecta os IPs configurados na máquina e adiciona todos
      ao Subject Alternative Name do certificado.

  -ip <endereco>
      Adiciona manualmente um IP ao Subject Alternative Name.

      Pode ser usado várias vezes.

Exemplos:

  $0 \
      -cert server.crt \
      -key server.key

  $0 \
      -cert server.crt \
      -key server.key \
      -auto-ips

  $0 \
      -cert server.crt \
      -key server.key \
      -ip 10.113.144.228

  $0 \
      -cert server.crt \
      -key server.key \
      -auto-ips \
      -ip 10.113.144.228 \
      -ip 192.168.1.50

EOF
}

fail() {
    echo "certgen: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in

        -cert)
            [[ $# -ge 2 ]] || fail "-cert exige um argumento"
            CERT_FILE="$2"
            shift 2
            ;;

        -key)
            [[ $# -ge 2 ]] || fail "-key exige um argumento"
            KEY_FILE="$2"
            shift 2
            ;;

        -ca)
            [[ $# -ge 2 ]] || fail "-ca exige um argumento"
            CA_FILE="$2"
            shift 2
            ;;

        -validade)
            [[ $# -ge 2 ]] || fail "-validade exige um argumento"
            VALIDADE="$2"
            shift 2
            ;;

        -auto-ips)
            AUTO_IPS=true
            shift
            ;;

        -ip)
            [[ $# -ge 2 ]] || fail "-ip exige um argumento"
            EXTRA_IPS+=("$2")
            shift 2
            ;;

        -h|--help)
            usage
            exit 0
            ;;

        *)
            fail "argumento desconhecido: $1"
            ;;

    esac
done

[[ -n "$CERT_FILE" ]] || fail "-cert é obrigatório"
[[ -n "$KEY_FILE" ]] || fail "-key é obrigatório"

command -v openssl >/dev/null 2>&1 ||
    fail "openssl não encontrado"

command -v date >/dev/null 2>&1 ||
    fail "date não encontrado"

#
# Se -ca não for informado, coloca ca.pem ao lado do certificado.
#
if [[ -z "$CA_FILE" ]]; then
    CERT_DIR="$(dirname "$CERT_FILE")"
    CA_FILE="${CERT_DIR}/ca.pem"
fi

#
# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
#

duration_to_seconds() {
    local duration="$1"

    if [[ "$duration" =~ ^([0-9]+)s$ ]]; then
        echo "${BASH_REMATCH[1]}"
        return
    fi

    if [[ "$duration" =~ ^([0-9]+)m$ ]]; then
        echo "$(( BASH_REMATCH[1] * 60 ))"
        return
    fi

    if [[ "$duration" =~ ^([0-9]+)h$ ]]; then
        echo "$(( BASH_REMATCH[1] * 60 * 60 ))"
        return
    fi

    if [[ "$duration" =~ ^([0-9]+)d$ ]]; then
        echo "$(( BASH_REMATCH[1] * 24 * 60 * 60 ))"
        return
    fi

    fail "duração inválida: $duration; use algo como 30m, 2h ou 1d"
}

is_valid_ip() {
    local ip="$1"

    #
    # Deixamos o OpenSSL validar semanticamente IPv4/IPv6 mais adiante.
    # Aqui só evitamos strings obviamente inválidas ou vazias.
    #
    [[ -n "$ip" ]] || return 1

    [[ "$ip" != *" "* ]] || return 1

    return 0
}

add_unique_ip() {
    local candidate="$1"

    #
    # Remove eventual CIDR.
    #
    candidate="${candidate%%/*}"

    [[ -n "$candidate" ]] || return

    is_valid_ip "$candidate" || return

    local existing

    for existing in "${SAN_IPS[@]}"; do
        if [[ "$existing" == "$candidate" ]]; then
            return
        fi
    done

    SAN_IPS+=("$candidate")
}

detect_machine_ips() {
    local ip_address

    #
    # Primeira opção: `ip`.
    #
    # É a forma mais confiável em distribuições Linux modernas.
    #
    if command -v ip >/dev/null 2>&1; then

        while IFS= read -r ip_address; do
            [[ -n "$ip_address" ]] || continue
            add_unique_ip "$ip_address"
        done < <(
            ip -o addr show scope global 2>/dev/null |
                awk '{print $4}' |
                cut -d/ -f1
        )

        return
    fi

    #
    # Fallback para sistemas onde `ip` não está instalado.
    #
    if command -v hostname >/dev/null 2>&1; then

        while IFS= read -r ip_address; do
            [[ -n "$ip_address" ]] || continue
            add_unique_ip "$ip_address"
        done < <(
            hostname -I 2>/dev/null |
                tr ' ' '\n' |
                sed '/^[[:space:]]*$/d'
        )

        return
    fi

    echo "certgen: aviso: não foi possível detectar automaticamente os IPs" >&2
}

#
# ---------------------------------------------------------------------------
# SANs
# ---------------------------------------------------------------------------
#

declare -a SAN_IPS=()

#
# Sempre disponíveis.
#
add_unique_ip "127.0.0.1"
add_unique_ip "::1"

#
# IPs configurados explicitamente.
#
for EXTRA_IP in "${EXTRA_IPS[@]}"; do
    add_unique_ip "$EXTRA_IP"
done

#
# IPs encontrados automaticamente.
#
if [[ "$AUTO_IPS" == true ]]; then
    detect_machine_ips
fi

#
# ---------------------------------------------------------------------------
# Validade
# ---------------------------------------------------------------------------
#

VALIDADE_SECONDS="$(duration_to_seconds "$VALIDADE")"

TMP_DIR="$(mktemp -d)"

cleanup() {
    rm -rf "$TMP_DIR"
}

trap cleanup EXIT

CA_KEY="$TMP_DIR/ca.key"
CA_CONFIG="$TMP_DIR/ca.cnf"

SERVER_CSR="$TMP_DIR/server.csr"

CA_DATABASE="$TMP_DIR/index.txt"
CA_SERIAL="$TMP_DIR/serial"
CA_NEWCERTS="$TMP_DIR/newcerts"
CA_SIGN_CONFIG="$TMP_DIR/ca-sign.cnf"

mkdir -p "$CA_NEWCERTS"

touch "$CA_DATABASE"

printf '%X\n' "$(date +%s)" > "$CA_SERIAL"

#
# Mesmo comportamento do código Go original:
#
#   NotBefore = agora - 1 hora
#   NotAfter  = agora + validade
#
NOW_EPOCH="$(date +%s)"

NOT_BEFORE_EPOCH="$(( NOW_EPOCH - 3600 ))"
NOT_AFTER_EPOCH="$(( NOW_EPOCH + VALIDADE_SECONDS ))"

NOT_BEFORE="$(
    date -u \
        -d "@${NOT_BEFORE_EPOCH}" \
        '+%Y%m%d%H%M%SZ'
)"

NOT_AFTER="$(
    date -u \
        -d "@${NOT_AFTER_EPOCH}" \
        '+%Y%m%d%H%M%SZ'
)"

#
# ---------------------------------------------------------------------------
# Geração dinâmica da seção SAN
# ---------------------------------------------------------------------------
#

generate_san_section() {

    echo "[ subject_alt_names ]"

    echo "DNS.1 = localhost"

    local index=1
    local ip_address

    for ip_address in "${SAN_IPS[@]}"; do

        echo "IP.${index} = ${ip_address}"

        index=$(( index + 1 ))

    done
}

#
# ---------------------------------------------------------------------------
# CA
# ---------------------------------------------------------------------------
#

cat > "$CA_CONFIG" <<EOF
[ req ]
prompt = no
distinguished_name = subject
x509_extensions = extensions

[ subject ]
CN = nishidb-jdbc-it-ca

[ extensions ]
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
EOF

openssl ecparam \
    -name prime256v1 \
    -genkey \
    -noout \
    -out "$CA_KEY"

chmod 0600 "$CA_KEY"

openssl req \
    -new \
    -x509 \
    -sha256 \
    -key "$CA_KEY" \
    -out "$CA_FILE" \
    -days 1 \
    -config "$CA_CONFIG"

chmod 0644 "$CA_FILE"

#
# ---------------------------------------------------------------------------
# Chave do servidor
# ---------------------------------------------------------------------------
#

umask 077

openssl ecparam \
    -name prime256v1 \
    -genkey \
    -noout \
    -out "$KEY_FILE"

chmod 0600 "$KEY_FILE"

#
# ---------------------------------------------------------------------------
# CSR
# ---------------------------------------------------------------------------
#

openssl req \
    -new \
    -sha256 \
    -key "$KEY_FILE" \
    -out "$SERVER_CSR" \
    -subj "/CN=nishidb-jdbc-it"

#
# ---------------------------------------------------------------------------
# Configuração da CA para assinatura
# ---------------------------------------------------------------------------
#

cat > "$CA_SIGN_CONFIG" <<EOF
[ ca ]
default_ca = CA_default

[ CA_default ]

database = $CA_DATABASE
serial = $CA_SERIAL
new_certs_dir = $CA_NEWCERTS

certificate = $CA_FILE
private_key = $CA_KEY

default_md = sha256
default_days = 1

policy = policy_anything

copy_extensions = none

unique_subject = no

[ policy_anything ]

commonName = supplied

[ server_extensions ]

basicConstraints = critical,CA:false

keyUsage = critical,digitalSignature

extendedKeyUsage = serverAuth

subjectAltName = @subject_alt_names

subjectKeyIdentifier = hash

authorityKeyIdentifier = keyid,issuer

EOF

generate_san_section >> "$CA_SIGN_CONFIG"

#
# ---------------------------------------------------------------------------
# Assina certificado
# ---------------------------------------------------------------------------
#

openssl ca \
    -batch \
    -config "$CA_SIGN_CONFIG" \
    -in "$SERVER_CSR" \
    -out "$CERT_FILE" \
    -startdate "$NOT_BEFORE" \
    -enddate "$NOT_AFTER" \
    -extensions server_extensions \
    -notext

chmod 0644 "$CERT_FILE"

#
# ---------------------------------------------------------------------------
# Validação
# ---------------------------------------------------------------------------
#

openssl verify \
    -CAfile "$CA_FILE" \
    "$CERT_FILE"

#
# ---------------------------------------------------------------------------
# Resultado
# ---------------------------------------------------------------------------
#

echo
echo "certgen: certificados criados"
echo
echo "  CA:   $CA_FILE"
echo "  cert: $CERT_FILE"
echo "  key:  $KEY_FILE"
echo
echo "  validade: $VALIDADE"
echo
echo "Subject Alternative Names:"
echo
echo "  DNS: localhost"

for ip_address in "${SAN_IPS[@]}"; do
    echo "  IP:  $ip_address"
done

echo