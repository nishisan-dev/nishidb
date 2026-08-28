#!/usr/bin/env bash
#
# nishidb-update.sh — atualiza o NishiDB por TROCA DE SYMLINK, com rollback
# automático quando a verificação pós-start falhar.
#
# A unidade de instalação E DE ROLLBACK é o PAR (binário, config), nunca o
# binário sozinho:
#
#   $NISHIDB_ROOT/releases/<versão>/{nishidb, nishidb.yaml}
#
# com o symlink $NISHIDB_ROOT/current apontando para o DIRETÓRIO da release
# (não para o binário). O motivo mora no parser de configuração: um binário
# VELHO, apontado para uma nishidb.yaml NOVA que já declara
# `server.tls`/`server.authentication`, ignora essas seções EM SILÊNCIO
# (KnownFields desligado hoje) — ele sobe, o /v1/health responde UP, e o
# processo fica exposto em texto claro e sem senha com um health verde.
# Reverter só o binário nessa situação devolveria exatamente esse estado: o
# pior possível é o que fica silenciosamente saudável. Por isso binário e
# config trocam SEMPRE juntos, como uma unidade só.
#
# Uso:
#   nishidb-update.sh <versão>
#
#   A release precisa já existir em $NISHIDB_ROOT/releases/<versão>/, com o
#   par completo (nishidb, nishidb.yaml). Este script não instala nada — só
#   troca o symlink corrente para uma release já presente em disco, verifica
#   a saúde do resultado dentro de um prazo, e desfaz a troca se a
#   verificação não fechar.
#
#   NOTA sobre o primeiro uso: a migração para o layout releases/<versão>/ (com
#   `current` symlinkado para dentro dele) é MANUAL — este script não a faz,
#   só pressupõe que ela já aconteceu. E a pré-checagem (le_versao_atual) exige
#   que o serviço CORRENTE já publique .process.version em /v1/stats; um
#   binário pré-Task A3 não publica esse campo, então a primeira chamada
#   real deste script só funciona depois que o binário em produção já foi
#   trocado, ao menos uma vez, por um que o publique.
#
# Variáveis de ambiente (todas com default de produção):
#   NISHIDB_ROOT                     default /app/nishidb
#   NISHIDB_UPDATE_SYSTEMCTL         default systemctl        (comando substituível, p/ testes)
#   NISHIDB_SERVICE                  default nishidb.service
#   NISHIDB_SERVER                   default http://127.0.0.1:9142  (plano LOOPBACK, sem credencial)
#   NISHIDB_UPDATE_TIMEOUT           default 60         (segundos; prazo da verificação
#                                    pós-start, reaproveitado também na verificação pós-rollback)
#   NISHIDB_UPDATE_INVALID_TOLERANCE default 5          (quantos `nishidb_ingest_invalid_total`
#                                    a mais, somados na janela inteira da verificação, ainda
#                                    contam como saudável — ver nota abaixo)
#
# Sobre a tolerância de invalid_total: o objetivo do check é pegar o binário
# NOVO classificando eventos válidos como inválidos EM MASSA (o defeito de
# rollback que esta task existe para cobrir), não um evento isolado e
# legítimo — a cauda de chaves grandes (>1024 bytes) do tópico `events` já
# produz descarte esporádico em produção, por política (`log-and-skip`), sem
# corrupção nenhuma. Falhar o update por causa de UM desses descartes
# legítimos seria pior que o problema que o check existe para pegar.
#
# Sobre a suposição de ingestão ativa: a verificação de "ingestão viva" (3/4
# abaixo) assume que existe pelo menos um dataset consumindo Kafka de fato —
# é o caso do piloto (tópico `events`, ~452 msg/s contínuos, sem janela
# morta plausível). Um deploy hipotético SEM nenhum dataset com Kafka
# habilitado nunca veria `nishidb_ingest_events_total` avançar nem
# `nishidb_kafka_lag` cair, e a verificação sempre estouraria o prazo. Não
# há flag para desligar esse check: não é o caso do piloto, e uma variável
# para um cenário que não existe hoje é só superfície não testada.
#
# Saída: 0 em sucesso (versão nova no ar e verificada); != 0 em qualquer
# falha, com o PASSO que falhou nomeado em stderr. Uma pré-checagem que
# recusa não toca no serviço; uma falha depois do `systemctl stop` sempre
# tenta o rollback automático antes de sair. Uma interrupção (Ctrl-C ou
# SIGTERM) depois que a mutação já começou tenta o MESMO rollback de
# emergência, e o script sempre imprime, ao sair — por qualquer caminho —,
# para onde o symlink `current` ficou apontando e o que `/v1/health`
# responde naquele instante.

set -euo pipefail

# ---------------------------------------------------------------- parâmetros

NISHIDB_ROOT="${NISHIDB_ROOT:-/app/nishidb}"
NISHIDB_UPDATE_SYSTEMCTL="${NISHIDB_UPDATE_SYSTEMCTL:-systemctl}"
NISHIDB_SERVICE="${NISHIDB_SERVICE:-nishidb.service}"
NISHIDB_SERVER="${NISHIDB_SERVER:-http://127.0.0.1:9142}"
NISHIDB_UPDATE_TIMEOUT="${NISHIDB_UPDATE_TIMEOUT:-60}"
NISHIDB_UPDATE_INVALID_TOLERANCE="${NISHIDB_UPDATE_INVALID_TOLERANCE:-5}"

# Intervalo entre amostras da verificação com prazo. Fixo, e não uma
# variável de ambiente: não está na lista de parametrização do brief (a
# "exigência do oráculo" enumera só as cinco acima), e um intervalo solto só
# mudaria quão fina é a amostragem dentro do MESMO prazo, sem abrir
# capacidade nova.
readonly INTERVALO_VERIFICACAO_SEGUNDOS=2

# ------------------------------------------------------------------- estado

# Estado usado pelos traps (INT/TERM/EXIT, mais abaixo). Declarado com
# default vazio/zero ANTES de qualquer `exit` possível: sob `set -u`, o
# handler de EXIT roda mesmo numa saída antes da pré-checagem (ex.: uso sem
# argumento), e uma referência a variável ainda não populada seria erro.
CURRENT_LINK=""
RELEASE_ANTERIOR=""
VERSAO_ANTERIOR=""
# MUTACAO_INICIADA vira 1 no primeiro `systemctl stop` bem-sucedido — daí
# em diante, o serviço real pode estar parado ou com o symlink já trocado, e
# uma interrupção não pode simplesmente sair: precisa tentar o rollback.
MUTACAO_INICIADA=0
# TRATANDO_INTERRUPCAO evita reentrância se um segundo sinal chegar enquanto
# o rollback de emergência do primeiro ainda está em andamento.
TRATANDO_INTERRUPCAO=0

if [[ $# -ne 1 ]]; then
  echo "uso: $(basename "$0") <versão>" >&2
  echo "  (a release precisa já existir em ${NISHIDB_ROOT}/releases/<versão>/)" >&2
  exit 2
fi
VERSAO="$1"

CURRENT_LINK="${NISHIDB_ROOT}/current"
RELEASE_NOVA="${NISHIDB_ROOT}/releases/${VERSAO}"

# FALHA_PASSO/FALHA_MSG carregam a última falha registrada por
# registra_falha — usadas tanto para a mensagem final quanto para decidir,
# no rollback, se ele mesmo falhou por um motivo diferente do que causou a
# atualização a falhar.
FALHA_PASSO=""
FALHA_MSG=""

# falha_fatal é só para a PRÉ-CHECAGEM: nada mudou em disco ainda (nenhum
# `systemctl stop` rodou), então sair na hora é seguro — não há o que
# desfazer.
falha_fatal() {
  echo "erro [pré-checagem: $1]: $2" >&2
  exit 1
}

# registra_falha é para qualquer passo DEPOIS do primeiro `systemctl stop`:
# o processo real pode já estar parado ou com o symlink trocado, então quem
# chama esta função precisa continuar até o rollback, não sair na hora.
registra_falha() {
  FALHA_PASSO="$1"
  FALHA_MSG="$2"
  echo "erro [$1]: $2" >&2
}

# --------------------------------------------------------------- utilitários

# extrai_campo_json lê um documento JSON da entrada padrão e imprime o
# campo pedido (aceita um caminho de um ou dois níveis: "status" ou
# "process.version"). Nunca falha alto: entrada vazia, HTTP de erro sem
# corpo, ou JSON malformado viram string vazia — quem chama decide o que
# fazer com "sem resposta ainda", que é esperado enquanto o serviço sobe.
#
# python3, não jq: jq não está garantido no host do piloto (nota do brief).
extrai_campo_json() {
  local caminho="$1"
  python3 -c '
import json, sys
caminho = sys.argv[1].split(".")
try:
    doc = json.load(sys.stdin)
    for chave in caminho:
        doc = doc.get(chave, {}) if isinstance(doc, dict) else {}
    print(doc if isinstance(doc, str) else "")
except Exception:
    print("")
' "$caminho"
}

# extrai_token_versao devolve só o PRIMEIRO campo (separado por espaço) de
# uma string de versão. buildinfo.String() (internal/buildinfo) monta
# "<versão> (commit <sha>, build <data>)" quando os ldflags vieram, ou só
# "<versão>"/"dev" no fallback — em qualquer um dos dois formatos, o texto
# ANTES do primeiro espaço é exatamente o identificador que o operador
# passa como <versão> no argumento deste script. Comparar por esse token
# (igualdade exata), e não por prefixo solto, é o que impede v1.2.3 de
# casar com v1.2.30/v1.2.35/v1.2.3-rc1 — a única defesa contra
# binário-velho-config-nova não pode aceitar uma release vizinha.
extrai_token_versao() {
  awk '{print $1}' <<<"$1"
}

# soma_metrica soma o último campo de toda linha do /metrics cujo nome
# (antes de um eventual "{") bate exatamente com o pedido — o mesmo formato
# que scripts/nishidb-stats.py já parseia manualmente, aqui via awk porque
# é texto de uma linha só por série, sem aninhamento.
soma_metrica() {
  local texto="$1" nome="$2"
  awk -v nome="$nome" '
    BEGIN { soma = 0 }
    /^#/ { next }
    NF == 0 { next }
    {
      chave = $1
      sub(/\{.*/, "", chave)
      if (chave == nome) { soma += $NF }
    }
    END { printf "%s", soma }
  ' <<<"$texto"
}

# tem_metrica diz se a série EXISTE no corpo, o que soma_metrica não distingue.
#
# soma_metrica devolve 0 tanto para "somei e deu zero" quanto para "a série não
# está lá", e desde 28/08/2026 a segunda situação é REAL e esperada: quando o
# broker não responde, nishidb_kafka_lag e nishidb_kafka_high_watermark deixam
# de ser emitidas de propósito (ausência é honesta, zero seria medição falsa).
#
# Sem esta distinção, a verificação de vida abaixo tinha um falso positivo
# silencioso: broker respondendo na primeira amostra (lag=500) e mudo na
# segunda (série ausente, soma=0) dava 0 < 500 — "o lag caiu" — e o deploy era
# aprovado por um sinal que não existia.
tem_metrica() {
  local texto="$1" nome="$2"
  awk -v nome="$nome" '
    /^#/ { next }
    NF == 0 { next }
    {
      chave = $1
      sub(/\{.*/, "", chave)
      if (chave == nome) { achou = 1; exit }
    }
    END { exit achou ? 0 : 1 }
  ' <<<"$texto"
}

# compara_numeros afirma "a $op b" em ponto flutuante (bash só compara
# inteiro nativamente, e os contadores do /metrics podem chegar como "0"
# vindos do awk, mas em geral são números decimais do Prometheus).
compara_numeros() {
  awk -v a="$1" -v b="$3" -v op="$2" 'BEGIN {
    if (op == ">") { exit !(a > b) }
    if (op == "<") { exit !(a < b) }
    exit 1
  }'
}

# curl_rota busca uma rota do servidor. SEMPRE --noproxy '*': a armadilha do
# proxy corporativo (docs/piloto-events.md) manda qualquer requisição para
# 127.0.0.1 através do proxy da empresa, que responde 403 e faz o script
# acusar o servidor de estar fora do ar quando na verdade está no ar. Sem
# -f: um /v1/health com status != UP volta HTTP 503 e AINDA ASSIM tem corpo
# JSON válido — descartar o corpo por causa do código HTTP esconderia
# exatamente o "DEGRADED" que se quer ler.
curl_rota() {
  curl -sS --noproxy '*' --max-time 5 "${NISHIDB_SERVER}$1"
}

# ------------------------------------------------------- interrupção e estado

# reporta_estado_real imprime, melhor esforço, para onde o symlink current
# aponta e o que /v1/health responde NESTE INSTANTE. Uma mensagem de erro
# sozinha não diz se o serviço ficou de pé, parado, ou em qual release — só
# uma consulta ao estado de verdade diz isso, e é o que um operador precisa
# antes de decidir o próximo passo.
reporta_estado_real() {
  local release_atual="desconhecida (current ausente ou não é symlink)"
  if [[ -n "$CURRENT_LINK" && -L "$CURRENT_LINK" ]]; then
    release_atual=$(readlink -f "$CURRENT_LINK" 2>/dev/null) || release_atual="ilegível"
  fi
  local saude="inalcançável"
  local corpo
  if corpo=$(curl -sS --noproxy '*' --max-time 3 "${NISHIDB_SERVER}/v1/health" 2>/dev/null); then
    saude=$(extrai_campo_json "status" <<<"$corpo")
    [[ -z "$saude" ]] && saude="resposta sem campo status"
  fi
  echo "estado real agora: current -> ${release_atual} | /v1/health -> ${saude}" >&2
}

# ao_sair roda em TODA saída do script — sucesso, erro fatal, ou depois de
# trata_interrupcao chamar `exit` — e é o que garante o "SEMPRE imprime o
# estado real resultante" mesmo numa saída que ninguém previu. Sai cedo se
# CURRENT_LINK ainda não foi calculado (erro de uso, antes de qualquer
# pré-checagem): nesse ponto não há nada em disco para reportar.
ao_sair() {
  [[ -n "$CURRENT_LINK" ]] || return 0
  reporta_estado_real
}
trap ao_sair EXIT

# trata_interrupcao responde a INT/TERM. Se a mutação já começou (o primeiro
# `systemctl stop` já rodou), o serviço real pode estar parado ou com o
# symlink trocado sem verificação nenhuma — a rede de segurança é tentar o
# MESMO rollback automático que uma falha de verificação dispararia.
trata_interrupcao() {
  local sinal="$1"
  if [[ $TRATANDO_INTERRUPCAO -eq 1 ]]; then
    exit 130
  fi
  TRATANDO_INTERRUPCAO=1

  echo "" >&2
  echo "erro: script interrompido por ${sinal}" >&2
  if [[ $MUTACAO_INICIADA -eq 1 ]]; then
    echo "  mutação já em andamento (systemctl stop já rodou) — tentando rollback de emergência para ${RELEASE_ANTERIOR:-a release anterior}" >&2
    if rollback; then
      echo "  rollback de emergência concluído" >&2
    else
      echo "  rollback de emergência TAMBÉM falhou no passo [${FALHA_PASSO}]: ${FALHA_MSG} — intervenção manual necessária" >&2
    fi
  else
    echo "  interrompido antes de qualquer mudança (pré-checagem) — nada para desfazer" >&2
  fi
  exit 130
}
trap 'trata_interrupcao INT' INT
trap 'trata_interrupcao TERM' TERM

# ----------------------------------------------------------------- pré-checagem

# le_versao_atual busca a versão corrente via /v1/stats. Falha alta: sem
# saber de onde se está partindo, nem a mensagem de rollback consegue dizer
# para onde deve voltar.
le_versao_atual() {
  local corpo
  if ! corpo=$(curl_rota /v1/stats); then
    falha_fatal "leitura da versão atual" \
      "${NISHIDB_SERVER}/v1/stats inalcançável — confirme que o serviço está no ar antes de atualizar"
  fi
  local versao
  versao=$(extrai_campo_json "process.version" <<<"$corpo")
  if [[ -z "$versao" ]]; then
    falha_fatal "leitura da versão atual" \
      "${NISHIDB_SERVER}/v1/stats respondeu sem .process.version — resposta: ${corpo:0:200}"
  fi
  printf '%s' "$versao"
}

pre_checagem() {
  echo "== 1/4 pré-checagem =="

  echo "  lendo versão corrente em ${NISHIDB_SERVER}/v1/stats"
  VERSAO_ANTERIOR=$(le_versao_atual)
  echo "  versão corrente: ${VERSAO_ANTERIOR}"

  if [[ ! -L "$CURRENT_LINK" ]]; then
    falha_fatal "release ausente" \
      "${CURRENT_LINK} não é um symlink — instalação fora do layout releases/<versão>/ esperado"
  fi
  RELEASE_ANTERIOR=$(readlink -f "$CURRENT_LINK") || falha_fatal "release ausente" \
    "não consegui resolver o destino de ${CURRENT_LINK}"
  echo "  release corrente (current): ${RELEASE_ANTERIOR}"

  if [[ ! -d "$RELEASE_NOVA" ]]; then
    falha_fatal "release ausente" \
      "${RELEASE_NOVA} não existe — publique o par (binário, config) da versão ${VERSAO} antes de rodar este script"
  fi
  local bin_novo="${RELEASE_NOVA}/nishidb"
  local cfg_nova="${RELEASE_NOVA}/nishidb.yaml"
  if [[ ! -f "$bin_novo" ]]; then
    falha_fatal "release incompleta" \
      "${bin_novo} ausente — o par (binário, config) da release ${VERSAO} está incompleto"
  fi
  if [[ ! -f "$cfg_nova" ]]; then
    falha_fatal "release incompleta" \
      "${cfg_nova} ausente — o par (binário, config) da release ${VERSAO} está incompleto"
  fi
  if [[ ! -x "$bin_novo" ]]; then
    falha_fatal "release incompleta" "${bin_novo} não é executável"
  fi

  echo "  checando ${bin_novo} --version"
  local versao_binario
  if ! versao_binario=$("$bin_novo" --version 2>&1); then
    falha_fatal "binário não responde" \
      "${bin_novo} --version falhou — binário da release ${VERSAO} não roda neste host (saída: ${versao_binario})"
  fi
  echo "  ${bin_novo} --version respondeu: ${versao_binario}"

  echo "  pré-checagem ok — release ${VERSAO} tem o par completo e responde a --version"
}

# ------------------------------------------------------- troca (stop/link/start)

# troca_para para o serviço, aponta o symlink current para a release pedida
# e sobe o serviço de novo — nesta ordem exata (§ do brief), sem inventar
# espera além do `systemctl start`: o fluxo SIGTERM→drena→checkpointa já é
# seguro por construção.
#
# NÃO usa falha_fatal: a partir do primeiro `systemctl stop`, uma falha aqui
# precisa passar pelo rollback (o serviço pode já estar parado ou o symlink
# já trocado), então ela registra e retorna 1 em vez de sair na hora.
troca_para() {
  local release_alvo="$1" rotulo="$2"

  echo "  parando ${NISHIDB_SERVICE} (${NISHIDB_UPDATE_SYSTEMCTL} stop)"
  if ! "$NISHIDB_UPDATE_SYSTEMCTL" stop "$NISHIDB_SERVICE"; then
    registra_falha "systemctl stop (${rotulo})" \
      "${NISHIDB_UPDATE_SYSTEMCTL} stop ${NISHIDB_SERVICE} falhou preparando a versão ${rotulo}"
    return 1
  fi
  # A partir daqui o serviço real já foi parado: uma interrupção (INT/TERM)
  # não pode mais só sair, precisa tentar o rollback (trata_interrupcao).
  MUTACAO_INICIADA=1

  echo "  trocando symlink current -> ${release_alvo}"
  if ! ln -sfn "$release_alvo" "$CURRENT_LINK"; then
    registra_falha "troca de symlink (${rotulo})" \
      "não consegui apontar ${CURRENT_LINK} para ${release_alvo}"
    return 1
  fi

  echo "  subindo ${NISHIDB_SERVICE} (${NISHIDB_UPDATE_SYSTEMCTL} start)"
  if ! "$NISHIDB_UPDATE_SYSTEMCTL" start "$NISHIDB_SERVICE"; then
    registra_falha "systemctl start (${rotulo})" \
      "${NISHIDB_UPDATE_SYSTEMCTL} start ${NISHIDB_SERVICE} falhou subindo a versão ${rotulo}"
    return 1
  fi

  return 0
}

# ----------------------------------------------------------------- verificação

# verifica_pos_start é a verificação com prazo do §3 do brief: dentro de
# NISHIDB_UPDATE_TIMEOUT segundos, afirma AO MESMO TEMPO que (1) /v1/health
# está UP, (2) a versão nova aparece em /v1/stats — a ÚNICA defesa contra
# binário-velho-config-nova, porque esse binário sobe, ignora as seções de
# segurança em silêncio e ainda assim fica UP —, (3) a ingestão está viva
# (nishidb_ingest_events_total avançou OU nishidb_kafka_lag caiu entre duas
# amostras) e (4) nishidb_ingest_invalid_total não subiu. Falhar em
# QUALQUER uma delas até o prazo esgotar é falha da verificação inteira.
verifica_pos_start() {
  local alvo_versao="$1"
  local fim
  fim=$(( $(date +%s) + NISHIDB_UPDATE_TIMEOUT ))

  local ok_saude=0 ok_versao=0 ok_ingestao=0 ok_invalido=1
  local base_eventos="" base_lag="" base_invalido="" base_tem_lag=0
  local ultima_versao="" ultimo_status="" ultimo_delta_invalido="0"

  while true; do
    local corpo_saude corpo_stats corpo_metrics

    ok_saude=0
    if corpo_saude=$(curl_rota /v1/health); then
      ultimo_status=$(extrai_campo_json "status" <<<"$corpo_saude")
      [[ "$ultimo_status" == "UP" ]] && ok_saude=1
    else
      ultimo_status=""
    fi

    ok_versao=0
    if corpo_stats=$(curl_rota /v1/stats); then
      ultima_versao=$(extrai_campo_json "process.version" <<<"$corpo_stats")
      [[ "$(extrai_token_versao "$ultima_versao")" == "$alvo_versao" ]] && ok_versao=1
    else
      ultima_versao=""
    fi

    if corpo_metrics=$(curl_rota /metrics); then
      local eventos lag invalido tem_lag
      eventos=$(soma_metrica "$corpo_metrics" nishidb_ingest_events_total)
      lag=$(soma_metrica "$corpo_metrics" nishidb_kafka_lag)
      invalido=$(soma_metrica "$corpo_metrics" nishidb_ingest_invalid_total)
      tem_lag=0
      if tem_metrica "$corpo_metrics" nishidb_kafka_lag; then tem_lag=1; fi

      if [[ -z "$base_eventos" ]]; then
        base_eventos="$eventos"; base_lag="$lag"; base_invalido="$invalido"
        base_tem_lag="$tem_lag"
      elif compara_numeros "$eventos" ">" "$base_eventos" ||
           { [[ "$tem_lag" -eq 1 && "$base_tem_lag" -eq 1 ]] &&
             compara_numeros "$lag" "<" "$base_lag"; }; then
        ok_ingestao=1
      fi

      # invalid_total: tolerância sobre o DELTA da janela INTEIRA (amostra
      # atual - primeira amostra desta verificação), recalculada a cada
      # iteração — não uma flag que, uma vez derrubada por um evento
      # isolado e legítimo, nunca mais volta (era exatamente esse o defeito:
      # a cauda de chaves >1024 bytes já descarta eventos válidos em
      # produção por política, e um único descarte na janela não pode
      # derrubar o update inteiro).
      ultimo_delta_invalido=$(awk -v a="$invalido" -v b="$base_invalido" 'BEGIN{printf "%s", a-b}')
      if compara_numeros "$ultimo_delta_invalido" ">" "$NISHIDB_UPDATE_INVALID_TOLERANCE"; then
        ok_invalido=0
      else
        ok_invalido=1
      fi
    fi

    if [[ $ok_saude -eq 1 && $ok_versao -eq 1 && $ok_ingestao -eq 1 && $ok_invalido -eq 1 ]]; then
      echo "  verificação ok: /v1/health UP, versão ${alvo_versao} confirmada, ingestão viva, invalid_total dentro da tolerância (Δ=${ultimo_delta_invalido})"
      return 0
    fi

    if [[ $(date +%s) -ge $fim ]]; then
      break
    fi
    sleep "$INTERVALO_VERIFICACAO_SEGUNDOS"
  done

  local motivos=""
  [[ $ok_saude    -eq 1 ]] || motivos+="/v1/health não ficou UP (última leitura: '${ultimo_status:-sem resposta}'); "
  [[ $ok_versao   -eq 1 ]] || motivos+="versão nova (${alvo_versao}) não confirmada em /v1/stats (última leitura: '${ultima_versao:-sem resposta}'); "
  [[ $ok_ingestao -eq 1 ]] || motivos+="ingestão sem sinal de progresso — nishidb_ingest_events_total parado e nishidb_kafka_lag não caiu; "
  [[ $ok_invalido -eq 1 ]] || motivos+="nishidb_ingest_invalid_total subiu além da tolerância (Δ=${ultimo_delta_invalido} > NISHIDB_UPDATE_INVALID_TOLERANCE=${NISHIDB_UPDATE_INVALID_TOLERANCE}); "
  registra_falha "verificação pós-start" "prazo de ${NISHIDB_UPDATE_TIMEOUT}s esgotado — ${motivos}"
  return 1
}

# verifica_rollback_saudavel confere só o que o brief pede do rollback: a
# versão VELHA voltou e está saudável. Comparação EXATA com VERSAO_ANTERIOR
# (a string literal já capturada em /v1/stats antes de qualquer troca) — não
# um prefixo como na verificação da versão nova, porque aqui já se conhece o
# texto exato que o mesmo binário reportava minutos atrás. Não reexige
# ingestão viva: rollback é o caminho de emergência, e sua verificação é
# deliberadamente mais enxuta que a da atualização.
verifica_rollback_saudavel() {
  local versao_esperada="$1"
  local fim
  fim=$(( $(date +%s) + NISHIDB_UPDATE_TIMEOUT ))
  local status="" versao=""

  while true; do
    local corpo
    if corpo=$(curl_rota /v1/health); then
      status=$(extrai_campo_json "status" <<<"$corpo")
    else
      status=""
    fi
    if corpo=$(curl_rota /v1/stats); then
      versao=$(extrai_campo_json "process.version" <<<"$corpo")
    else
      versao=""
    fi

    if [[ "$status" == "UP" && "$versao" == "$versao_esperada" ]]; then
      return 0
    fi
    if [[ $(date +%s) -ge $fim ]]; then
      registra_falha "rollback: verificação da versão anterior" \
        "após o rollback, /v1/health='${status:-sem resposta}' e /v1/stats.process.version='${versao:-sem resposta}' (esperava '${versao_esperada}' saudável)"
      return 1
    fi
    sleep "$INTERVALO_VERIFICACAO_SEGUNDOS"
  done
}

# --------------------------------------------------------------------- rollback

# rollback devolve o par (binário, config) anterior e confere que ele volta
# saudável. Reaproveita troca_para com o rótulo "rollback": mesma ordem
# stop → symlink → start, agora invertida para RELEASE_ANTERIOR.
rollback() {
  echo "== 4/4 rollback automático: voltando para ${RELEASE_ANTERIOR} (versão ${VERSAO_ANTERIOR}) =="
  if ! troca_para "$RELEASE_ANTERIOR" "rollback"; then
    return 1
  fi
  if ! verifica_rollback_saudavel "$VERSAO_ANTERIOR"; then
    return 1
  fi
  echo "  rollback confirmado: versão anterior (${VERSAO_ANTERIOR}) no ar e saudável"
  return 0
}

# ------------------------------------------------------------------------ main

main() {
  pre_checagem

  local atualizacao_ok=1
  echo "== 2/4 trocando para a versão ${VERSAO} =="
  if troca_para "$RELEASE_NOVA" "nova"; then
    echo "== 3/4 verificando a versão nova (prazo ${NISHIDB_UPDATE_TIMEOUT}s) =="
    if verifica_pos_start "$VERSAO"; then
      atualizacao_ok=0
    fi
  fi

  if [[ $atualizacao_ok -eq 0 ]]; then
    echo "atualização para ${VERSAO} concluída e verificada com sucesso."
    exit 0
  fi

  local passo_original="$FALHA_PASSO" msg_original="$FALHA_MSG"
  echo "atualização para ${VERSAO} falhou no passo [${passo_original}] — iniciando rollback automático" >&2

  if rollback; then
    echo "rollback concluído — serviço voltou para a versão anterior (${VERSAO_ANTERIOR}), saudável." >&2
  else
    echo "erro: ROLLBACK TAMBÉM FALHOU no passo [${FALHA_PASSO}]: ${FALHA_MSG} — intervenção manual necessária." >&2
    reporta_estado_real
  fi

  echo "erro: atualização para ${VERSAO} falhou no passo [${passo_original}]: ${msg_original}" >&2
  exit 1
}

main "$@"
