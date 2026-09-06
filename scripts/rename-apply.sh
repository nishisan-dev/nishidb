#!/usr/bin/env bash
# Aplicador do ciclo de identificadores em inglês (planning/identificadores-em-ingles.md §4.6).
#
#   scripts/rename-apply.sh <dir-do-pacote> <script.rf> <diff-de-saida>
#
# Exige rf e gopls no PATH e falha alto sem eles (doutrina dos gates: ferramenta
# ausente nomeia o que instalar). Guarda o -diff do rf ANTES de aplicar, aplica,
# e roda build e vet do módulo inteiro. O rf é tudo-ou-nada por script: se ele
# recusar, nada foi escrito.
set -u
RF_VERSION="rsc.io/rf@v0.0.0-20260710193434-85dbf8d53aee"
GOPLS_VERSION="golang.org/x/tools/gopls@v0.23.0"

if [ $# -ne 3 ]; then
  echo "uso: $0 <dir-do-pacote> <script.rf> <diff-de-saida>" >&2
  exit 2
fi
dir=$1; script=$2; diff_out=$3

if ! command -v rf >/dev/null 2>&1; then
  echo "rename-apply: rf não está no PATH. Instale com: go install $RF_VERSION" >&2
  exit 3
fi
if ! command -v gopls >/dev/null 2>&1; then
  echo "rename-apply: gopls não está no PATH. Instale com: go install $GOPLS_VERSION" >&2
  exit 4
fi

export GOFLAGS="${GOFLAGS:-} -buildvcs=false"
raiz=$(pwd)
cd "$dir" || exit 2
conteudo=$(cat "$script")

rf -diff "$conteudo" > "$diff_out" 2>"$diff_out.err"
if [ $? -ne 0 ]; then
  echo "rename-apply: o rf recusou o script em $dir:" >&2
  cat "$diff_out.err" >&2
  exit 1
fi
rf "$conteudo" 2>"$diff_out.err"
if [ $? -ne 0 ]; then
  echo "rename-apply: o rf falhou ao aplicar em $dir (nada foi escrito):" >&2
  cat "$diff_out.err" >&2
  exit 1
fi
cd "$raiz" || exit 2
if ! go build ./... ; then
  echo "rename-apply: build vermelho depois de $dir; reverta com git checkout -- . e devolva o mapeamento ao juiz" >&2
  exit 2
fi
if ! go vet ./... ; then
  echo "rename-apply: vet vermelho depois de $dir; reverta com git checkout -- . e devolva o mapeamento ao juiz" >&2
  exit 2
fi
echo "rename-apply: $dir aplicado; diff em $diff_out"
