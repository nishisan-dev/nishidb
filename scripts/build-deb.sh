#!/usr/bin/env bash
#
# build-deb.sh — empacota um binário `nishidb` já buildado num pacote .deb
# no layout FHS, para instalar em qualquer host Debian/Ubuntu.
#
# O pacote NÃO é o layout do piloto. O piloto usa releases com symlink
# (/app/nishidb/releases/<versão> + /app/nishidb/current), operado por
# scripts/nishidb-update.sh — decisão deliberada para trocar de versão sem
# reinstalar nada e permitir rollback instantâneo por symlink. O .deb serve
# outro público: quem quer `apt`/`dpkg` fazendo a instalação, upgrade e
# desinstalação pela ferramenta padrão da distribuição, com o binário em
# /usr/bin, a config em /etc e o estado em /var/lib — o layout que qualquer
# admin Debian já sabe onde procurar. Os dois convivem: nenhum dos dois
# comandos deste repositório (make build-deb, scripts/nishidb-update.sh)
# aciona o outro.
#
# Por que dpkg-deb puro, sem fakeroot e sem nfpm/action de terceiros: a
# mesma doutrina de binário único e zero dependência de runtime (§97) vale
# para o EMPACOTAMENTO — dpkg-deb já está no runner ubuntu-latest e em
# qualquer host Debian, então empacotar não pede instalar nada novo.
# --root-owner-group substitui o fakeroot: ele grava root:root nas entradas
# do .deb sem exigir uid 0 nem LD_PRELOAD, e é suportado desde o dpkg 1.19.1
# (2018) — bem abaixo do que este repositório já pressupõe em CI.
#
# O control leva `Depends: passwd` (o `useradd`/`userdel`/`getent` que os
# scripts de mantenedor chamam vêm desse pacote essencial, não do nosso
# binário estático) e `Installed-Size:` em KiB, medido com `du -k -s` sobre
# o staging INTEIRO exceto DEBIAN/ — dpkg usa esse número antes de baixar
# nada, e um pacote sem ele aparece com tamanho "desconhecido" em qualquer
# frontend. `DEBIAN/md5sums` é gerado a partir dos mesmos arquivos de dado
# (de novo, tudo exceto DEBIAN/), caminho relativo ao staging e sem o `./`
# do `find` — é o que permite `dpkg -V nishidb` auditar arquivo corrompido
# ou editado por fora do dpkg depois da instalação.
#
# A unit vai em usr/lib/systemd/system/, não em lib/systemd/system/: nos
# hosts com usrmerge (padrão desde Debian bookworm/Ubuntu 20.04+), /lib é só
# um symlink para /usr/lib, e o dpkg 1.22+ (Debian trixie) passa a RECUSAR
# pacote que instale arquivo pelo caminho aliased em vez do caminho real —
# o mesmo arquivo apontado pelos dois nomes conta como conflito de
# diversion. Instalar direto em usr/lib/ é o caminho que continua válido
# nos dois lados do alias.
#
# Uso:
#   scripts/build-deb.sh --binary <caminho> --version <versão> --arch <amd64|arm64> --out <dir>
#
# Ferramenta ausente FALHA alto no stderr nomeando o que instalar — nunca
# SKIP silencioso (doutrina do débito 53: gate que passa sem a ferramenta não
# é gate, é carimbo). Todas as mensagens de erro (stderr) são em inglês —
# quem lê este script rodando é o runner de CI ou qualquer operador Debian,
# não só o time do piloto; os identificadores locais seguem o padrão
# PT-BR de scripts/nishidb-update.sh.

set -euo pipefail

erro() {
  echo "error: $*" >&2
  exit 1
}

uso() {
  cat >&2 <<'EOF'
usage: scripts/build-deb.sh --binary <path> --version <version> --arch <amd64|arm64> --out <dir>
EOF
  exit 1
}

binario=""
versao=""
arquitetura=""
saida=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      binario="${2:-}"
      shift 2
      ;;
    --version)
      versao="${2:-}"
      shift 2
      ;;
    --arch)
      arquitetura="${2:-}"
      shift 2
      ;;
    --out)
      saida="${2:-}"
      shift 2
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      uso
      ;;
  esac
done

[[ -n "$binario" && -n "$versao" && -n "$arquitetura" && -n "$saida" ]] || uso

if ! command -v dpkg-deb >/dev/null 2>&1; then
  erro "dpkg-deb not found in PATH — install the dpkg-dev package (Debian/Ubuntu: apt install dpkg-dev)."
fi

# Formato de versão Debian (Policy §5.6.12, simplificado ao que o projeto
# gera): dígito inicial, seguido de dígitos/letras/pontuação de versão. Os
# builds locais carregam sufixo de `git describe` (ex.: 0.9.1-4-g4239716),
# que o regex aceita.
if [[ ! "$versao" =~ ^[0-9][A-Za-z0-9.+~-]*$ ]]; then
  erro "version '$versao' is not in the expected Debian format (e.g. 0.9.1 or 0.9.1-4-g4239716)."
fi

case "$arquitetura" in
  amd64|arm64) ;;
  *) erro "architecture '$arquitetura' is not supported — use amd64 or arm64." ;;
esac

[[ -f "$binario" ]] || erro "binary '$binario' does not exist."
[[ -s "$binario" ]] || erro "binary '$binario' is empty."

# e_machine mora nos bytes 18-19 do cabeçalho ELF (little-endian): 0x3e é
# amd64 (EM_X86_64), 0xb7 é arm64 (EM_AARCH64). od é POSIX e está em
# qualquer host que roda dpkg-deb; ler os dois bytes aqui é o que impede
# empacotar o binário da OUTRA arquitetura por engano — o workflow builda as
# duas no mesmo caminho `./nishidb`, trocando o conteúdo entre as duas
# chamadas, e um `--arch amd64` chamado depois do build arm64 (ou
# vice-versa) empacotaria silenciosamente o binário errado sem isto.
maquina_lida="$(od -An -tx1 -j18 -N2 "$binario" | tr -d ' \n')"
case "$arquitetura" in
  amd64) maquina_esperada="3e00" ;;
  arm64) maquina_esperada="b700" ;;
esac
if [[ "$maquina_lida" != "$maquina_esperada" ]]; then
  erro "binary '$binario' is not ELF $arquitetura (e_machine read: 0x${maquina_lida}, expected: 0x${maquina_esperada}) — looks like the binary for the other architecture."
fi

mkdir -p "$saida"

raiz_do_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
pacote_deb_dir="$raiz_do_repo/packaging/deb"

staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT

mkdir -p \
  "$staging/usr/bin" \
  "$staging/usr/lib/systemd/system" \
  "$staging/etc/nishidb/datasets" \
  "$staging/DEBIAN"

# mktemp -d cria o staging em 0700, e o umask do ambiente decide o modo dos
# diretórios criados por mkdir -p — nenhum dos dois é o que um .deb deve
# carregar. Normaliza para 0755 em tudo antes de instalar os arquivos, que
# têm modo explícito próprio.
chmod 0755 "$staging"
find "$staging" -type d -exec chmod 0755 {} +

install -m 0755 "$binario" "$staging/usr/bin/nishidb"
install -m 0644 "$pacote_deb_dir/nishidb.service" "$staging/usr/lib/systemd/system/nishidb.service"
install -m 0644 "$pacote_deb_dir/nishidb.yaml" "$staging/etc/nishidb/nishidb.yaml"
install -m 0755 "$pacote_deb_dir/postinst" "$staging/DEBIAN/postinst"
install -m 0755 "$pacote_deb_dir/prerm" "$staging/DEBIAN/prerm"
install -m 0755 "$pacote_deb_dir/postrm" "$staging/DEBIAN/postrm"

echo "/etc/nishidb/nishidb.yaml" > "$staging/DEBIAN/conffiles"

# Installed-Size é o staging INTEIRO exceto DEBIAN/ (control.tar não conta
# como "instalado" — é metadado do próprio dpkg), em KiB — a unidade que a
# Policy exige. --exclude é GNU du; o projeto já pressupõe GNU coreutils no
# runner e em qualquer host Debian/Ubuntu, mesma base de scripts/it-comum.sh.
installed_size_kb="$(du -k -s --exclude=DEBIAN "$staging" | cut -f1)"

cat > "$staging/DEBIAN/control" <<EOF
Package: nishidb
Version: $versao
Architecture: $arquitetura
Section: database
Priority: optional
Maintainer: Lucas Nishimura <lucasnishimura@gmail.com>
Homepage: https://github.com/nishisan-dev/nishidb
Installed-Size: $installed_size_kb
Depends: passwd
Description: JSON event materialization and query engine
 NishiDB keeps the current materialized state of each entity from a stream
 of partial JSON events (merge by field, sequence control) and answers SQL
 over that state, within a fixed memory budget. Single static binary, no
 external runtime dependency.
EOF

# md5sums cobre os arquivos de dado exceto DEBIAN/ E exceto os CONFFILES
# (caminho relativo ao staging, sem o "./" do find) — é o formato que
# `dpkg -V` espera para auditar arquivo alterado por fora do dpkg depois da
# instalação. Conffile fica de FORA de propósito: a integridade dele é
# rastreada pelo dpkg via DEBIAN/conffiles, não por md5sums — o operador
# EDITAR /etc/nishidb/nishidb.yaml é o caso normal (é para isso que o
# conffile existe), e listá-lo aqui faria `dpkg -V` acusar "alterado" em
# todo host que já tem a config própria, um falso positivo permanente.
(
  cd "$staging"
  find . -path ./DEBIAN -prune -o -type f -print | sed 's|^\./||' | LC_ALL=C sort |
    while IFS= read -r caminho_relativo; do
      if grep -qxF "/$caminho_relativo" "$staging/DEBIAN/conffiles"; then
        continue
      fi
      md5sum "$caminho_relativo"
    done
) > "$staging/DEBIAN/md5sums"

arquivo_saida="$saida/nishidb_${versao}_${arquitetura}.deb"
dpkg-deb --root-owner-group --build "$staging" "$arquivo_saida" >&2

echo "$arquivo_saida"
