package gates_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A Task A4 (#11, parte 2) entrega scripts/nishidb-update.sh: o mecanismo que
// vai atravessar a fronteira "binário velho sem auth → binário novo com
// auth" no primeiro uso real, na Fase 11. A unidade de instalação e rollback
// é o PAR (binário, config) — releases/<versão>/{nishidb, nishidb.yaml} —, e
// não só o binário: o binário velho ignora `server.tls`/`server.authentication`
// EM SILÊNCIO (KnownFields desligado), então um rollback que trocasse só o
// binário e deixasse a config nova no lugar subiria exposto, sem senha, com
// health UP. O pior estado possível é silenciosamente verde.
//
// Este arquivo é o oráculo desse mecanismo. Sem ele, "o script faz rollback"
// é uma alegação de comentário — exatamente o defeito que a doutrina do
// projeto já fechou uma vez em internal/gates/lint_test.go (débito 53: gate
// que não é executado não é gate). Os dois testes abaixo EXECUTAM o script de
// verdade, com dois dublês:
//
//   - `systemctl`: um script de shell escrito pelo teste, que troca
//     `NISHIDB_UPDATE_SYSTEMCTL`. Ele não sobe processo nenhum — quem
//     responde às rotas é o httptest.Server abaixo —, só grava em disco
//     (estado-fake/versao, estado-fake/saude) o que o `/v1/stats` e o
//     `/v1/health` do dublê HTTP devem reportar a partir dali, simulando o
//     efeito de um `systemctl start` de verdade.
//   - HTTP: um httptest.Server cujos handlers LEEM esses dois arquivos a
//     cada requisição — o script os escreve, o servidor os lê, e nenhuma
//     goroutine de sincronização é necessária porque a troca (systemctl
//     start) e a leitura (curl da verificação) já acontecem em sequência,
//     como processos separados de verdade.
//
// O cenário (a) é o que prova o rollback: o dublê `systemctl` SEMPRE escreve
// "velha (dublê)" no `start`, não importa qual release está de fato linkada
// — simulando o defeito de fundo que a task existe para pegar (o servidor
// sobe mas nunca confirma a versão nova). O cenário (b) é o caminho feliz: o
// dublê resolve a versão HONESTAMENTE, rodando `current/nishidb --version`.

const caminhoDoScriptDeAtualizacao = raizDoRepo + "/scripts/nishidb-update.sh"

// ondeEstaOBash resolve o interpretador antes de montar o ambiente do
// filho, no mesmo espírito de ondeEstaOMake (lint_test.go): ausência de bash
// é FALHA, não SKIP — é o interpretador que a própria task exige ("bash",
// texto do brief), não um acessório opcional do teste.
func ondeEstaOBash(t *testing.T) string {
	t.Helper()
	caminho, err := exec.LookPath("bash")
	if err != nil {
		t.Fatalf("bash não está no PATH e é o interpretador exigido por scripts/nishidb-update.sh: %v", err)
	}
	return caminho
}

// stubBinario cria, em dir/nishidb, um binário de mentira cujo `--version`
// imprime rotulo — o bastante para a pré-checagem do script ("responde a
// --version") e para o dublê honesto do cenário (b) descobrir qual release
// está linkada.
func stubBinario(t *testing.T, dir, rotulo string) {
	t.Helper()
	conteudo := fmt.Sprintf("#!/bin/sh\necho '%s (dublê)'\n", rotulo)
	if err := os.WriteFile(filepath.Join(dir, "nishidb"), []byte(conteudo), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nishidb.yaml"), []byte("# config de teste, par da release "+rotulo+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// montaRaiz prepara um $NISHIDB_ROOT temporário com o layout exigido pela
// task: releases/velha e releases/nova, cada um com o PAR completo, e
// `current` apontando para releases/velha — o estado de produção antes de
// qualquer atualização.
func montaRaiz(t *testing.T) (raiz, estadoDir string) {
	t.Helper()
	raiz = t.TempDir()
	releases := filepath.Join(raiz, "releases")
	for _, rotulo := range []string{"velha", "nova"} {
		dir := filepath.Join(releases, rotulo)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		stubBinario(t, dir, rotulo)
	}
	if err := os.Symlink(filepath.Join(releases, "velha"), filepath.Join(raiz, "current")); err != nil {
		t.Fatal(err)
	}

	estadoDir = filepath.Join(raiz, "estado-fake")
	if err := os.MkdirAll(estadoDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(estadoDir, "versao"), []byte("velha (dublê)"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(estadoDir, "saude"), []byte("UP"), 0o644); err != nil {
		t.Fatal(err)
	}
	return raiz, estadoDir
}

// servidorFake responde /v1/health, /v1/stats e /metrics lendo o estado que o
// dublê `systemctl` grava em disco a cada `start`/`stop`. /metrics simula
// ingestão viva incrementando um contador a cada raspagem — eventos sempre
// avançam entre duas amostras, sem depender de tempo real decorrido —, e
// invalid_total fixo em 0 (o caso comum; ver servidorFakeComInvalido para os
// testes de tolerância).
func servidorFake(t *testing.T, estadoDir string) *httptest.Server {
	t.Helper()
	return servidorFakeComInvalido(t, estadoDir, func(int64) int64 { return 0 })
}

// servidorFakeComInvalido é a base de servidorFake, com o crescimento de
// nishidb_ingest_invalid_total controlado por quem chama — invalidoPorRaspagem
// recebe o número da raspagem atual (1-based) e devolve o valor ACUMULADO do
// contador naquele instante. Existe por causa do Critical 1 da revisão: a
// tolerância de invalid_total (NISHIDB_UPDATE_INVALID_TOLERANCE) só é
// provável com um servidor cujo invalid_total pode subir de propósito.
func servidorFakeComInvalido(t *testing.T, estadoDir string, invalidoPorRaspagem func(n int64) int64) *httptest.Server {
	t.Helper()
	var raspagens int64

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		bruto, _ := os.ReadFile(filepath.Join(estadoDir, "saude"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"status": %q}`, strings.TrimSpace(string(bruto)))
	})
	mux.HandleFunc("/v1/stats", func(w http.ResponseWriter, _ *http.Request) {
		bruto, _ := os.ReadFile(filepath.Join(estadoDir, "versao"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"process": {"version": %q}}`, strings.TrimSpace(string(bruto)))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		n := atomic.AddInt64(&raspagens, 1)
		lag := 100 - n*10
		if lag < 0 {
			lag = 0
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "nishidb_ingest_events_total{dataset=\"events\"} %d\n", n*1000)
		_, _ = fmt.Fprintf(w, "nishidb_kafka_lag{dataset=\"events\"} %d\n", lag)
		_, _ = fmt.Fprintf(w, "nishidb_ingest_invalid_total{dataset=\"events\"} %d\n", invalidoPorRaspagem(n))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// dubleSempreVelha é o systemctl de mentira do cenário (a): não importa qual
// release o script acabou de linkar, o `start` sempre reporta a versão
// VELHA — é a simulação direta do defeito que o rollback existe para pegar
// (binário novo sobe, mas o /v1/stats nunca confirma a versão nova).
const dubleSempreVelha = `#!/bin/sh
set -e
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  start)
    printf '%s' "velha (dublê)" > "$NISHIDB_TESTE_ESTADO/versao"
    printf 'UP' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
esac
exit 0
`

// dubleHonesto é o systemctl de mentira do cenário (b): resolve a versão de
// verdade rodando o binário que `current` aponta NO MOMENTO do start — o
// mesmo efeito que um `systemctl start` real teria, só que sem subir
// processo nenhum.
const dubleHonesto = `#!/bin/sh
set -e
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  start)
    versao_atual=$("$NISHIDB_TESTE_CURRENT/nishidb" --version)
    printf '%s' "$versao_atual" > "$NISHIDB_TESTE_ESTADO/versao"
    printf 'UP' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
esac
exit 0
`

// dubleVersaoVizinha prova o Critical 2 da revisão: escreve uma versão que
// COMEÇA com o mesmo prefixo do alvo ("nova"), mas é outra release inteira
// ("nova2") — a comparação por prefixo solto aceitava isso; a comparação
// por token exato não pode.
const dubleVersaoVizinha = `#!/bin/sh
set -e
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  start)
    printf '%s' "nova2 (dublê)" > "$NISHIDB_TESTE_ESTADO/versao"
    printf 'UP' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
esac
exit 0
`

// dubleVersaoComSufixoDeBuildinfo escreve o formato REAL de
// buildinfo.String() ("<versão> (commit <sha>, build <data>)"), com o
// mesmo token de versão do alvo ("nova"). Prova que a correção do Critical 2
// (comparar só o primeiro token) não passou a rejeitar o formato de
// verdade que o servidor produz.
const dubleVersaoComSufixoDeBuildinfo = `#!/bin/sh
set -e
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  start)
    printf '%s' "nova (commit abcdef1, build 2026-01-01T00:00:00Z)" > "$NISHIDB_TESTE_ESTADO/versao"
    printf 'UP' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
esac
exit 0
`

// dubleFalhaNoPrimeiroStart prova o Important 2 da revisão: o `systemctl
// start` da PRIMEIRA tentativa (a versão nova) falha de verdade — o dublê
// sai 1 em vez de aplicar o start. Da segunda chamada em diante (a do
// rollback) ele volta a se comportar como dubleHonesto, para o rollback
// poder de fato se recuperar e o teste conseguir distinguir "o passo certo
// falhou" de "o script trava".
const dubleFalhaNoPrimeiroStart = `#!/bin/sh
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  start)
    if [ ! -f "$NISHIDB_TESTE_ESTADO/ja-falhou-start" ]; then
      touch "$NISHIDB_TESTE_ESTADO/ja-falhou-start"
      echo "systemctl start: falha simulada pelo dublê" >&2
      exit 1
    fi
    versao_atual=$("$NISHIDB_TESTE_CURRENT/nishidb" --version)
    printf '%s' "$versao_atual" > "$NISHIDB_TESTE_ESTADO/versao"
    printf 'UP' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
esac
exit 0
`

// dubleQueNuncaSobeEDerrubaOServidor prova o Important 3: o `start` não
// escreve UP nem versão nenhuma (o processo "nunca sobe") e avisa o teste
// (tocando um arquivo-sinal no próprio estadoDir) para derrubar o
// httptest.Server de verdade, simulando a porta parando de responder — os
// ramos de curl FALHANDO em verifica_pos_start, não só um JSON dizendo
// DOWN.
const dubleQueNuncaSobeEDerrubaOServidor = `#!/bin/sh
set -e
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
  start)
    touch "$NISHIDB_TESTE_ESTADO/derruba-servidor"
    ;;
esac
exit 0
`

// dubleMataOPaiAposStart prova o Important 1 (trap de INT/TERM): assim que
// aplica o PRIMEIRO `start` (a versão nova), manda SIGTERM para o próprio
// processo pai — que é o nishidb-update.sh em execução, já que o dublê é
// filho direto dele. É determinístico (sem depender de acertar uma janela
// de tempo): o sinal é entregue enquanto o script está bloqueado esperando
// este `systemctl start` terminar, então o trap dispara assim que o dublê
// sai, ANTES de troca_para sequer conferir o exit code do start. O marcador
// ja-matou evita mandar um SEGUNDO sinal quando o rollback chama `start` de
// novo.
const dubleMataOPaiAposStart = `#!/bin/sh
set -e
echo "$@" >> "$NISHIDB_TESTE_LOG"
case "$1" in
  start)
    versao_atual=$("$NISHIDB_TESTE_CURRENT/nishidb" --version)
    printf '%s' "$versao_atual" > "$NISHIDB_TESTE_ESTADO/versao"
    printf 'UP' > "$NISHIDB_TESTE_ESTADO/saude"
    if [ ! -f "$NISHIDB_TESTE_ESTADO/ja-matou" ]; then
      touch "$NISHIDB_TESTE_ESTADO/ja-matou"
      kill -TERM "$PPID"
    fi
    ;;
  stop)
    printf 'DOWN' > "$NISHIDB_TESTE_ESTADO/saude"
    ;;
esac
exit 0
`

// escreveDuble grava o dublê `systemctl` num diretório próprio (fora de
// $NISHIDB_ROOT, para não colidir com o layout releases/current que o script
// varre) e devolve o caminho do executável e do arquivo de log de chamadas.
func escreveDuble(t *testing.T, conteudo string) (caminho, log string) {
	t.Helper()
	dir := t.TempDir()
	caminho = filepath.Join(dir, "systemctl-fake")
	if err := os.WriteFile(caminho, []byte(conteudo), 0o755); err != nil {
		t.Fatal(err)
	}
	log = filepath.Join(dir, "chamadas.log")
	return caminho, log
}

// rodaScript executa nishidb-update.sh de verdade, com PATH e ambiente
// totalmente controlados, e devolve stdout+stderr combinados e o erro de
// exec.Cmd.Run (nil quando o script sai 0). extras carrega variáveis de
// ambiente adicionais no formato CHAVE=valor (ex.:
// "NISHIDB_UPDATE_INVALID_TOLERANCE=1") — usadas pelos testes que precisam
// de um parâmetro além dos cinco básicos.
func rodaScript(t *testing.T, raiz, servidorURL, systemctlPath, logPath, versao, prazoSegundos string, extras ...string) (saida string, err error) {
	t.Helper()
	bash := ondeEstaOBash(t)
	cmd := exec.Command(bash, caminhoDoScriptDeAtualizacao, versao)
	cmd.Env = append(os.Environ(),
		"NISHIDB_ROOT="+raiz,
		"NISHIDB_UPDATE_SYSTEMCTL="+systemctlPath,
		"NISHIDB_SERVICE=nishidb-teste.service",
		"NISHIDB_SERVER="+servidorURL,
		"NISHIDB_UPDATE_TIMEOUT="+prazoSegundos,
		"NISHIDB_TESTE_ESTADO="+filepath.Join(raiz, "estado-fake"),
		"NISHIDB_TESTE_LOG="+logPath,
		"NISHIDB_TESTE_CURRENT="+filepath.Join(raiz, "current"),
	)
	cmd.Env = append(cmd.Env, extras...)
	var buf strings.Builder
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	return buf.String(), err
}

// releaseLinkada resolve, sem seguir o link até o final do filesystem, o
// nome da release (basename de releases/<nome>) para o qual `current`
// aponta agora — é o que os dois testes usam para afirmar que o symlink
// terminou no lugar certo.
func releaseLinkada(t *testing.T, raiz string) string {
	t.Helper()
	destino, err := filepath.EvalSymlinks(filepath.Join(raiz, "current"))
	if err != nil {
		t.Fatalf("current não é um symlink resolvível após o script rodar: %v", err)
	}
	return filepath.Base(destino)
}

// exitCode extrai o código de saída de um erro de exec.Cmd.Run, ou 0 quando
// err é nil — só para as mensagens de falha do teste serem legíveis.
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var saiu *exec.ExitError
	if errors.As(err, &saiu) {
		return saiu.ExitCode()
	}
	return -1
}

// TestScriptDeAtualizacaoFazRollbackQuandoAVersaoNovaNuncaAparece é o
// cenário (a) do brief: a "versão nova" nunca aparece no /v1/stats do
// dublê, mesmo depois do `systemctl start`. A verificação com prazo
// (NISHIDB_UPDATE_TIMEOUT curto, para o teste não esperar o default de 60s)
// tem de estourar, disparar o rollback automático, devolver o symlink para
// releases/velha e sair ≠ 0 NOMEANDO o passo que falhou — rollback mecânico
// sem essa prova é só comentário no cabeçalho do script.
func TestScriptDeAtualizacaoFazRollbackQuandoAVersaoNovaNuncaAparece(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleSempreVelha)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "3")

	if err == nil {
		t.Fatalf("o script saiu 0 com a versão nova NUNCA confirmada em /v1/stats — o rollback deveria ter disparado\nsaída completa:\n%s", saida)
	}
	if exitCode(err) == 0 {
		t.Errorf("exit code 0 (via outro caminho de erro que não exec.ExitError): %v", err)
	}

	if !strings.Contains(saida, "versão") && !strings.Contains(saida, "verificação") {
		t.Errorf("a saída não nomeia o passo que falhou (esperava menção a 'versão' ou 'verificação')\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(strings.ToLower(saida), "rollback") {
		t.Errorf("a saída não menciona o rollback disparado\nsaída completa:\n%s", saida)
	}

	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s após o rollback — esperava releases/velha (o par binário+config velho tem de voltar)", linkada)
	}

	log, _ := os.ReadFile(logPath)
	chamadas := string(log)
	if strings.Count(chamadas, "start") < 2 {
		t.Errorf("esperava pelo menos 2 chamadas de `start` ao systemctl dublê (a tentativa nova + o rollback), log:\n%s", chamadas)
	}
}

// TestScriptDeAtualizacaoConcluiQuandoAVersaoNovaAparece é o cenário (b), o
// caminho feliz: o dublê passa a responder a versão nova depois do
// `systemctl start` (resolvendo o binário HONESTAMENTE a partir de
// `current`). O script tem de sair 0 e deixar o symlink em releases/nova —
// sem isso o teste (a) sozinho não prova nada, porque um script que SEMPRE
// faz rollback (mesmo com tudo saudável) também passaria nele.
func TestScriptDeAtualizacaoConcluiQuandoAVersaoNovaAparece(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleHonesto)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com o caminho feliz (versão nova confirmada, ingestão avançando): %v\nsaída completa:\n%s", exitCode(err), err, saida)
	}

	if linkada := releaseLinkada(t, raiz); linkada != "nova" {
		t.Errorf("current aponta para releases/%s após o caminho feliz — esperava releases/nova", linkada)
	}

	log, _ := os.ReadFile(logPath)
	if n := strings.Count(string(log), "start"); n != 1 {
		t.Errorf("esperava exatamente 1 chamada de `start` (sem rollback) — log:\n%s (start apareceu %d vez(es))", string(log), n)
	}
	if strings.Contains(strings.ToLower(saida), "rollback") {
		t.Errorf("o caminho feliz não deveria mencionar rollback\nsaída completa:\n%s", saida)
	}

	versaoFinal, _ := os.ReadFile(filepath.Join(estadoDir, "versao"))
	if got := strings.TrimSpace(string(versaoFinal)); !strings.HasPrefix(got, "nova") {
		t.Errorf("estado-fake/versao = %q depois do caminho feliz — esperava prefixo 'nova'", got)
	}
}

// TestScriptDeAtualizacaoRecusaSemAVersaoDaReleaseNaPreChecagem cobre a
// pré-checagem do §1 do brief antes de qualquer systemctl ser chamado: pedir
// uma versão cuja release não existe (ou está incompleta) tem de falhar alto
// e ANTES de tocar no serviço — sem isso, um operador que erra o número da
// versão só descobre o problema depois do `systemctl stop` já ter derrubado
// o serviço em produção.
func TestScriptDeAtualizacaoRecusaSemAVersaoDaReleaseNaPreChecagem(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	// dublê que FALHARIA o teste se fosse chamado: qualquer invocação sai
	// com erro alto, provando que a pré-checagem não chegou a chamar
	// systemctl.
	const dubleNuncaDeveRodar = `#!/bin/sh
echo "systemctl NAO deveria ter sido chamado (args: $*)" >&2
exit 99
`
	systemctlPath, _ := escreveDuble(t, dubleNuncaDeveRodar)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, filepath.Join(t.TempDir(), "log"), "inexistente", "3")

	if err == nil {
		t.Fatalf("o script saiu 0 pedindo a versão 'inexistente', sem release correspondente\nsaída completa:\n%s", saida)
	}
	if exitCode(err) == 99 {
		t.Fatalf("o script chegou a invocar o systemctl dublê antes de recusar a pré-checagem\nsaída completa:\n%s", saida)
	}
	// A mensagem tem de nomear o que faltou, não só sair != 0 por algum
	// outro motivo (ex.: erro de sintaxe do próprio script) — senão este
	// teste passaria por acidente com qualquer script quebrado.
	if !strings.Contains(saida, "inexistente") || !strings.Contains(strings.ToLower(saida), "release") {
		t.Errorf("a recusa não nomeia a versão pedida nem menciona a release ausente\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(saida, "pré-checagem") {
		t.Errorf("a recusa não identifica o passo (pré-checagem)\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current mudou de releases/velha para releases/%s numa pré-checagem que deveria ter recusado ANTES de tocar no serviço", linkada)
	}
}

// ------------------------------------------------------- fix report: revisão

// Os testes abaixo foram acrescentados na correção dos achados da revisão
// (2 Critical, 3 Important, 2 Minor — ver task-a4-report.md, seção "Fix
// report"). Cada um teve RED confirmado contra o script ANTES da correção
// correspondente.

// TestScriptDeAtualizacaoToleraUmInvalidoIsoladoDentroDaJanela prova a
// correção do Critical 1: invalid_total subindo +1 durante toda a janela de
// verificação (bem dentro da tolerância default de 5) NÃO pode travar um
// update saudável — a cauda de chaves >1024 bytes já descarta eventos
// legítimos em produção, por política, e um único descarte não é o defeito
// de binário-novo-classificando-tudo-errado que o check existe para pegar.
func TestScriptDeAtualizacaoToleraUmInvalidoIsoladoDentroDaJanela(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFakeComInvalido(t, estadoDir, func(n int64) int64 {
		if n >= 2 {
			return 1
		}
		return 0
	})
	systemctlPath, logPath := escreveDuble(t, dubleHonesto)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com invalid_total subindo só +1 na janela inteira (tolerância default 5): %v\nsaída completa:\n%s", exitCode(err), err, saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "nova" {
		t.Errorf("current aponta para releases/%s — esperava releases/nova (um invalid isolado não deveria disparar rollback)", linkada)
	}
}

// TestScriptDeAtualizacaoFazRollbackQuandoInvalidoSobeAlemDaToleranciaConfigurada
// é o outro lado do Critical 1: invalid_total crescendo bem acima de
// qualquer tolerância pequena (o sinal de classificação em massa, não de
// descarte isolado) tem de continuar disparando o rollback.
func TestScriptDeAtualizacaoFazRollbackQuandoInvalidoSobeAlemDaToleranciaConfigurada(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFakeComInvalido(t, estadoDir, func(n int64) int64 {
		return n * 10 // cresce rápido: delta já estoura a tolerância default (5) na 2ª amostra
	})
	systemctlPath, logPath := escreveDuble(t, dubleHonesto)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "5")

	if err == nil {
		t.Fatalf("o script saiu 0 com invalid_total disparando bem acima da tolerância\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(saida, "invalid_total") {
		t.Errorf("a mensagem de falha não menciona invalid_total\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback para releases/velha", linkada)
	}
}

// TestScriptDeAtualizacaoRespeitaToleranciaCustomizadaDeInvalido prova que
// NISHIDB_UPDATE_INVALID_TOLERANCE é de fato LIDA do ambiente, e não um
// número fixo que só parece configurável: com a tolerância explicitamente
// reduzida para 1, um delta de +2 tem de reprovar — o mesmo delta que
// passaria pela tolerância default (5).
func TestScriptDeAtualizacaoRespeitaToleranciaCustomizadaDeInvalido(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFakeComInvalido(t, estadoDir, func(n int64) int64 {
		if n >= 2 {
			return 2
		}
		return 0
	})
	systemctlPath, logPath := escreveDuble(t, dubleHonesto)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "3", "NISHIDB_UPDATE_INVALID_TOLERANCE=1")

	if err == nil {
		t.Fatalf("o script saiu 0 com invalid_total subindo +2 e NISHIDB_UPDATE_INVALID_TOLERANCE=1 — a variável não está sendo respeitada\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback com a tolerância customizada estourada", linkada)
	}
}

// TestScriptDeAtualizacaoRecusaVersaoVizinhaPorPrefixo prova a correção do
// Critical 2: "nova2" não pode confirmar o alvo "nova" só porque começa com
// o mesmo texto. Antes da correção, o `case "$versao" in "$alvo"*)` aceitava
// isso — a "única defesa contra binário-velho-config-nova" ficava furada.
func TestScriptDeAtualizacaoRecusaVersaoVizinhaPorPrefixo(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleVersaoVizinha)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "3")

	if err == nil {
		t.Fatalf("o script saiu 0 com 'nova2' no /v1/stats — a versão vizinha não deveria confirmar o alvo 'nova'\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback diante da versão vizinha 'nova2'", linkada)
	}
	if !strings.Contains(saida, "nova2") {
		t.Errorf("a mensagem de falha não mostra a versão vizinha lida ('nova2 (dublê)')\nsaída completa:\n%s", saida)
	}
}

// TestScriptDeAtualizacaoAceitaVersaoComSufixoDeBuildinfo é o outro lado do
// Critical 2: a correção não pode rejeitar o formato REAL que
// buildinfo.String() produz ("<versão> (commit ..., build ...)"). Sem este
// teste, uma correção "por igualdade exata da string inteira" (em vez de
// por token) passaria no teste da versão vizinha e quebraria em produção.
func TestScriptDeAtualizacaoAceitaVersaoComSufixoDeBuildinfo(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleVersaoComSufixoDeBuildinfo)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com a versão 'nova (commit ..., build ...)' — formato real do buildinfo.String() deveria bater com o alvo 'nova': %v\nsaída completa:\n%s", exitCode(err), err, saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "nova" {
		t.Errorf("current aponta para releases/%s — esperava releases/nova", linkada)
	}
}

// TestScriptDeAtualizacaoFazRollbackQuandoOSystemctlStartFalha prova o
// Important 2: o `systemctl start` de verdade pode falhar (unidade não
// encontrada, dependência não satisfeita, etc.) — troca_para já tratava
// isso, mas nenhum teste exercitava o ramo antes desta correção.
func TestScriptDeAtualizacaoFazRollbackQuandoOSystemctlStartFalha(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleFalhaNoPrimeiroStart)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "5")

	if err == nil {
		t.Fatalf("o script saiu 0 com o systemctl start falhando na tentativa de subir a versão nova\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(saida, "systemctl start") {
		t.Errorf("a mensagem de falha não nomeia o passo 'systemctl start'\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(strings.ToLower(saida), "rollback") {
		t.Errorf("a saída não menciona o rollback\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback de volta para releases/velha", linkada)
	}
}

// TestScriptDeAtualizacaoFazRollbackQuandoOServidorNuncaResponde prova o
// Important 3: a porta pode parar de responder de vez (conexão recusada),
// não só devolver um JSON dizendo DOWN — os ramos de curl FALHANDO em
// verifica_pos_start (else ultimo_status=""/ultima_versao="") precisam
// tratar isso como "não confirmado", não travar nem confundir com sucesso.
// A goroutine derruba o httptest.Server assim que o dublê toca o arquivo-
// sinal, dentro do próprio `systemctl start`.
func TestScriptDeAtualizacaoFazRollbackQuandoOServidorNuncaResponde(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleQueNuncaSobeEDerrubaOServidor)

	sinal := filepath.Join(estadoDir, "derruba-servidor")
	derrubado := make(chan struct{})
	go func() {
		for {
			if _, err := os.Stat(sinal); err == nil {
				servidor.Close()
				close(derrubado)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "2")

	select {
	case <-derrubado:
	case <-time.After(10 * time.Second):
		t.Fatal("o sinal para derrubar o servidor fake nunca chegou — o dublê não tocou o marcador esperado")
	}

	if err == nil {
		t.Fatalf("o script saiu 0 com o servidor fake parando de responder logo após o start\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s — esperava o rollback devolver para releases/velha quando a porta para de responder", linkada)
	}
	if !strings.Contains(saida, "verificação") {
		t.Errorf("a saída não nomeia o passo de verificação\nsaída completa:\n%s", saida)
	}
}

// TestScriptDeAtualizacaoRespondeATerminacaoTentandoRollback prova o
// Important 1: um SIGTERM depois que a mutação já começou (o `systemctl
// stop` original já rodou) não pode simplesmente matar o script sem rede de
// segurança — o trap precisa tentar o MESMO rollback automático e reportar
// o estado real resultante. O timeout passado é longo (60s) DE PROPÓSITO:
// a interrupção é determinística (o dublê mata o próprio pai logo depois do
// primeiro start) e nunca deveria depender do prazo de verificação para
// disparar.
func TestScriptDeAtualizacaoRespondeATerminacaoTentandoRollback(t *testing.T) {
	raiz, estadoDir := montaRaiz(t)
	servidor := servidorFake(t, estadoDir)
	systemctlPath, logPath := escreveDuble(t, dubleMataOPaiAposStart)

	saida, err := rodaScript(t, raiz, servidor.URL, systemctlPath, logPath, "nova", "60")

	if err == nil {
		t.Fatalf("o script saiu 0 apesar de ter recebido SIGTERM no meio da mutação\nsaída completa:\n%s", saida)
	}
	if got := exitCode(err); got != 130 {
		t.Errorf("exit code = %d, esperava 130 (convenção escolhida pelo próprio handler de interrupção)\nsaída completa:\n%s", got, saida)
	}
	if !strings.Contains(saida, "interrompido") {
		t.Errorf("a saída não menciona a interrupção\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(strings.ToLower(saida), "rollback") {
		t.Errorf("a saída não menciona a tentativa de rollback de emergência\nsaída completa:\n%s", saida)
	}
	if !strings.Contains(saida, "estado real") {
		t.Errorf("a saída não reporta o estado real resultante (current/health) — Important 1 pede isso SEMPRE\nsaída completa:\n%s", saida)
	}
	if linkada := releaseLinkada(t, raiz); linkada != "velha" {
		t.Errorf("current aponta para releases/%s — esperava o rollback de emergência devolver para releases/velha", linkada)
	}
}
