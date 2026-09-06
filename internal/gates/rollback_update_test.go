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

const updateScriptPath = repoRoot + "/scripts/nishidb-update.sh"

// locateBash resolve o interpretador antes de montar o ambiente do
// filho, no mesmo espírito de ondeEstaOMake (lint_test.go): ausência de bash
// é FALHA, não SKIP — é o interpretador que a própria task exige ("bash",
// texto do brief), não um acessório opcional do teste.
func locateBash(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("bash")
	if err != nil {
		t.Fatalf("bash não está no PATH e é o interpretador exigido por scripts/nishidb-update.sh: %v", err)
	}
	return path
}

// stubBinary cria, em dir/nishidb, um binário de mentira cujo `--version`
// imprime rotulo — o bastante para a pré-checagem do script ("responde a
// --version") e para o dublê honesto do cenário (b) descobrir qual release
// está linkada.
func stubBinary(t *testing.T, dir, label string) {
	t.Helper()
	content := fmt.Sprintf("#!/bin/sh\necho '%s (dublê)'\n", label)
	if err := os.WriteFile(filepath.Join(dir, "nishidb"), []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nishidb.yaml"), []byte("# config de teste, par da release "+label+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// setupRoot prepara um $NISHIDB_ROOT temporário com o layout exigido pela
// task: releases/velha e releases/nova, cada um com o PAR completo, e
// `current` apontando para releases/velha — o estado de produção antes de
// qualquer atualização.
func setupRoot(t *testing.T) (root, stateDir string) {
	t.Helper()
	root = t.TempDir()
	releases := filepath.Join(root, "releases")
	for _, label := range []string{"velha", "nova"} {
		dir := filepath.Join(releases, label)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		stubBinary(t, dir, label)
	}
	if err := os.Symlink(filepath.Join(releases, "velha"), filepath.Join(root, "current")); err != nil {
		t.Fatal(err)
	}

	stateDir = filepath.Join(root, "estado-fake")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "versao"), []byte("velha (dublê)"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "saude"), []byte("UP"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, stateDir
}

// fakeServer responde /v1/health, /v1/stats e /metrics lendo o estado que o
// dublê `systemctl` grava em disco a cada `start`/`stop`. /metrics simula
// ingestão viva incrementando um contador a cada raspagem — eventos sempre
// avançam entre duas amostras, sem depender de tempo real decorrido —, e
// invalid_total fixo em 0 (o caso comum; ver servidorFakeComInvalido para os
// testes de tolerância).
func fakeServer(t *testing.T, stateDir string) *httptest.Server {
	t.Helper()
	return fakeServerWithInvalid(t, stateDir, func(int64) int64 { return 0 })
}

// newFakeMuxWithState registra /v1/health e /v1/stats — comuns a TODOS os
// cenários, formato antigo ou novo de invalid_total — lendo o estado que o
// dublê `systemctl` grava em disco a cada `start`/`stop`. Quem chama ainda
// precisa registrar /metrics: é o único handler que difere entre
// servidorFakeComInvalido (formato antigo, sem `motivo`) e
// servidorFakeComInvalidoPorMotivo (formato novo, Task A5).
func newFakeMuxWithState(stateDir string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", func(w http.ResponseWriter, _ *http.Request) {
		raw, _ := os.ReadFile(filepath.Join(stateDir, "saude"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"status": %q}`, strings.TrimSpace(string(raw)))
	})
	mux.HandleFunc("/v1/stats", func(w http.ResponseWriter, _ *http.Request) {
		raw, _ := os.ReadFile(filepath.Join(stateDir, "versao"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"process": {"version": %q}}`, strings.TrimSpace(string(raw)))
	})
	return mux
}

// writeCommonMetrics escreve, em /metrics, as duas linhas que TODOS os
// cenários compartilham — eventos processados e lag —, calculadas a partir
// do número da raspagem atual (1-based). Devolve esse número: quem chama
// decide, a partir dele, como escrever a(s) linha(s) de invalid_total.
func writeCommonMetrics(w http.ResponseWriter, scrapes *int64) int64 {
	n := atomic.AddInt64(scrapes, 1)
	lag := 100 - n*10
	if lag < 0 {
		lag = 0
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w, "nishidb_ingest_events_total{dataset=\"events\"} %d\n", n*1000)
	_, _ = fmt.Fprintf(w, "nishidb_kafka_lag{dataset=\"events\"} %d\n", lag)
	return n
}

// fakeServerWithInvalid é a base de servidorFake, com o crescimento de
// nishidb_ingest_invalid_total controlado por quem chama — invalidoPorRaspagem
// recebe o número da raspagem atual (1-based) e devolve o valor ACUMULADO do
// contador naquele instante. Existe por causa do Critical 1 da revisão: a
// tolerância de invalid_total (NISHIDB_UPDATE_INVALID_TOLERANCE) só é
// provável com um servidor cujo invalid_total pode subir de propósito.
//
// Emite o FORMATO ANTIGO — uma série sem o label `motivo`, como o binário
// PRÉ-A5 produzia. O binário velho ainda pode estar no ar no primeiro update
// real do piloto (Minor 4: a primeira migração para releases/ é manual), e
// soma_metrica (nishidb-update.sh) precisa continuar somando esse formato
// certo — ver TestScriptDeAtualizacaoToleraUmInvalidoIsoladoNoFormatoAntigoSemMotivo.
func fakeServerWithInvalid(t *testing.T, stateDir string, invalidAtScrape func(n int64) int64) *httptest.Server {
	t.Helper()
	var scrapes int64

	mux := newFakeMuxWithState(stateDir)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		n := writeCommonMetrics(w, &scrapes)
		_, _ = fmt.Fprintf(w, "nishidb_ingest_invalid_total{dataset=\"events\"} %d\n", invalidAtScrape(n))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// fakeServerWithInvalidByReason prova o Important 2 da revisão final: a
// Task A5 (internal/kafka/metrics.go) fez o binário passar a emitir
// nishidb_ingest_invalid_total SEMPRE com o label `motivo`, e possivelmente
// em VÁRIAS séries — uma por categoria §101 —, nunca mais a série única e
// sem label que servidorFakeComInvalido simula. Antes desta função, o
// oráculo do A4 só exercitava o formato que o binário atual não emite mais.
//
// invalidoPorRaspagem continua devolvendo o total ACUMULADO na raspagem n; a
// função o REPARTE em duas séries (EVENT_TOO_LARGE e INVALID_JSON, duas das
// seis categorias §101) que somam esse total — provando que soma_metrica,
// que soma por NOME ignorando o resto da chave (dataset E motivo), soma as
// duas corretamente. A repartição varia entre pares (metade em cada) e
// ímpares (a sobra vai para INVALID_JSON), então o teste também cobre o caso
// em que as duas séries não têm o mesmo valor.
func fakeServerWithInvalidByReason(t *testing.T, stateDir string, invalidAtScrape func(n int64) int64) *httptest.Server {
	t.Helper()
	var scrapes int64

	mux := newFakeMuxWithState(stateDir)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		n := writeCommonMetrics(w, &scrapes)
		total := invalidAtScrape(n)
		half := total / 2
		_, _ = fmt.Fprintf(w, "nishidb_ingest_invalid_total{dataset=\"events\",motivo=\"EVENT_TOO_LARGE\"} %d\n", half)
		_, _ = fmt.Fprintf(w, "nishidb_ingest_invalid_total{dataset=\"events\",motivo=\"INVALID_JSON\"} %d\n", total-half)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// fakeAlwaysOld é o systemctl de mentira do cenário (a): não importa qual
// release o script acabou de linkar, o `start` sempre reporta a versão
// VELHA — é a simulação direta do defeito que o rollback existe para pegar
// (binário novo sobe, mas o /v1/stats nunca confirma a versão nova).
const fakeAlwaysOld = `#!/bin/sh
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

// fakeHonest é o systemctl de mentira do cenário (b): resolve a versão de
// verdade rodando o binário que `current` aponta NO MOMENTO do start — o
// mesmo efeito que um `systemctl start` real teria, só que sem subir
// processo nenhum.
const fakeHonest = `#!/bin/sh
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

// fakeNeighborVersion prova o Critical 2 da revisão: escreve uma versão que
// COMEÇA com o mesmo prefixo do alvo ("nova"), mas é outra release inteira
// ("nova2") — a comparação por prefixo solto aceitava isso; a comparação
// por token exato não pode.
const fakeNeighborVersion = `#!/bin/sh
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

// fakeVersionWithBuildinfoSuffix escreve o formato REAL de
// buildinfo.String() ("<versão> (commit <sha>, build <data>)"), com o
// mesmo token de versão do alvo ("nova"). Prova que a correção do Critical 2
// (comparar só o primeiro token) não passou a rejeitar o formato de
// verdade que o servidor produz.
const fakeVersionWithBuildinfoSuffix = `#!/bin/sh
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

// fakeFailsFirstStart prova o Important 2 da revisão: o `systemctl
// start` da PRIMEIRA tentativa (a versão nova) falha de verdade — o dublê
// sai 1 em vez de aplicar o start. Da segunda chamada em diante (a do
// rollback) ele volta a se comportar como dubleHonesto, para o rollback
// poder de fato se recuperar e o teste conseguir distinguir "o passo certo
// falhou" de "o script trava".
const fakeFailsFirstStart = `#!/bin/sh
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

// fakeNeverUpSignalsServerClose prova o Important 3: o `start` não
// escreve UP nem versão nenhuma (o processo "nunca sobe") e avisa o teste
// (tocando um arquivo-sinal no próprio estadoDir) para derrubar o
// httptest.Server de verdade, simulando a porta parando de responder — os
// ramos de curl FALHANDO em verifica_pos_start, não só um JSON dizendo
// DOWN.
const fakeNeverUpSignalsServerClose = `#!/bin/sh
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

// fakeKillsParentAfterStart prova o Important 1 (trap de INT/TERM): assim que
// aplica o PRIMEIRO `start` (a versão nova), manda SIGTERM para o próprio
// processo pai — que é o nishidb-update.sh em execução, já que o dublê é
// filho direto dele. É determinístico (sem depender de acertar uma janela
// de tempo): o sinal é entregue enquanto o script está bloqueado esperando
// este `systemctl start` terminar, então o trap dispara assim que o dublê
// sai, ANTES de troca_para sequer conferir o exit code do start. O marcador
// ja-matou evita mandar um SEGUNDO sinal quando o rollback chama `start` de
// novo.
const fakeKillsParentAfterStart = `#!/bin/sh
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

// writeFakeSystemctl grava o dublê `systemctl` num diretório próprio (fora de
// $NISHIDB_ROOT, para não colidir com o layout releases/current que o script
// varre) e devolve o caminho do executável e do arquivo de log de chamadas.
func writeFakeSystemctl(t *testing.T, content string) (path, log string) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "systemctl-fake")
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	log = filepath.Join(dir, "chamadas.log")
	return path, log
}

// runScript executa nishidb-update.sh de verdade, com PATH e ambiente
// totalmente controlados, e devolve stdout+stderr combinados e o erro de
// exec.Cmd.Run (nil quando o script sai 0). extras carrega variáveis de
// ambiente adicionais no formato CHAVE=valor (ex.:
// "NISHIDB_UPDATE_INVALID_TOLERANCE=1") — usadas pelos testes que precisam
// de um parâmetro além dos cinco básicos.
func runScript(t *testing.T, root, serverURL, systemctlPath, logPath, version, timeoutSeconds string, extras ...string) (output string, err error) {
	t.Helper()
	bash := locateBash(t)
	cmd := exec.Command(bash, updateScriptPath, version)
	cmd.Env = append(os.Environ(),
		"NISHIDB_ROOT="+root,
		"NISHIDB_UPDATE_SYSTEMCTL="+systemctlPath,
		"NISHIDB_SERVICE=nishidb-teste.service",
		"NISHIDB_SERVER="+serverURL,
		"NISHIDB_UPDATE_TIMEOUT="+timeoutSeconds,
		"NISHIDB_TESTE_ESTADO="+filepath.Join(root, "estado-fake"),
		"NISHIDB_TESTE_LOG="+logPath,
		"NISHIDB_TESTE_CURRENT="+filepath.Join(root, "current"),
	)
	cmd.Env = append(cmd.Env, extras...)
	var buf strings.Builder
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	return buf.String(), err
}

// linkedRelease resolve, sem seguir o link até o final do filesystem, o
// nome da release (basename de releases/<nome>) para o qual `current`
// aponta agora — é o que os dois testes usam para afirmar que o symlink
// terminou no lugar certo.
func linkedRelease(t *testing.T, root string) string {
	t.Helper()
	target, err := filepath.EvalSymlinks(filepath.Join(root, "current"))
	if err != nil {
		t.Fatalf("current não é um symlink resolvível após o script rodar: %v", err)
	}
	return filepath.Base(target)
}

// exitCode extrai o código de saída de um erro de exec.Cmd.Run, ou 0 quando
// err é nil — só para as mensagens de falha do teste serem legíveis.
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
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
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeAlwaysOld)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "3")

	if err == nil {
		t.Fatalf("o script saiu 0 com a versão nova NUNCA confirmada em /v1/stats — o rollback deveria ter disparado\nsaída completa:\n%s", output)
	}
	if exitCode(err) == 0 {
		t.Errorf("exit code 0 (via outro caminho de erro que não exec.ExitError): %v", err)
	}

	if !strings.Contains(output, "versão") && !strings.Contains(output, "verificação") {
		t.Errorf("a saída não nomeia o passo que falhou (esperava menção a 'versão' ou 'verificação')\nsaída completa:\n%s", output)
	}
	if !strings.Contains(strings.ToLower(output), "rollback") {
		t.Errorf("a saída não menciona o rollback disparado\nsaída completa:\n%s", output)
	}

	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s após o rollback — esperava releases/velha (o par binário+config velho tem de voltar)", linked)
	}

	log, _ := os.ReadFile(logPath)
	calls := string(log)
	if strings.Count(calls, "start") < 2 {
		t.Errorf("esperava pelo menos 2 chamadas de `start` ao systemctl dublê (a tentativa nova + o rollback), log:\n%s", calls)
	}
}

// TestScriptDeAtualizacaoConcluiQuandoAVersaoNovaAparece é o cenário (b), o
// caminho feliz: o dublê passa a responder a versão nova depois do
// `systemctl start` (resolvendo o binário HONESTAMENTE a partir de
// `current`). O script tem de sair 0 e deixar o symlink em releases/nova —
// sem isso o teste (a) sozinho não prova nada, porque um script que SEMPRE
// faz rollback (mesmo com tudo saudável) também passaria nele.
func TestScriptDeAtualizacaoConcluiQuandoAVersaoNovaAparece(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeHonest)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com o caminho feliz (versão nova confirmada, ingestão avançando): %v\nsaída completa:\n%s", exitCode(err), err, output)
	}

	if linked := linkedRelease(t, root); linked != "nova" {
		t.Errorf("current aponta para releases/%s após o caminho feliz — esperava releases/nova", linked)
	}

	log, _ := os.ReadFile(logPath)
	if n := strings.Count(string(log), "start"); n != 1 {
		t.Errorf("esperava exatamente 1 chamada de `start` (sem rollback) — log:\n%s (start apareceu %d vez(es))", string(log), n)
	}
	if strings.Contains(strings.ToLower(output), "rollback") {
		t.Errorf("o caminho feliz não deveria mencionar rollback\nsaída completa:\n%s", output)
	}

	finalVersion, _ := os.ReadFile(filepath.Join(stateDir, "versao"))
	if got := strings.TrimSpace(string(finalVersion)); !strings.HasPrefix(got, "nova") {
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
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	// dublê que FALHARIA o teste se fosse chamado: qualquer invocação sai
	// com erro alto, provando que a pré-checagem não chegou a chamar
	// systemctl.
	const fakeMustNeverRun = `#!/bin/sh
echo "systemctl NAO deveria ter sido chamado (args: $*)" >&2
exit 99
`
	systemctlPath, _ := writeFakeSystemctl(t, fakeMustNeverRun)

	output, err := runScript(t, root, server.URL, systemctlPath, filepath.Join(t.TempDir(), "log"), "inexistente", "3")

	if err == nil {
		t.Fatalf("o script saiu 0 pedindo a versão 'inexistente', sem release correspondente\nsaída completa:\n%s", output)
	}
	if exitCode(err) == 99 {
		t.Fatalf("o script chegou a invocar o systemctl dublê antes de recusar a pré-checagem\nsaída completa:\n%s", output)
	}
	// A mensagem tem de nomear o que faltou, não só sair != 0 por algum
	// outro motivo (ex.: erro de sintaxe do próprio script) — senão este
	// teste passaria por acidente com qualquer script quebrado.
	if !strings.Contains(output, "inexistente") || !strings.Contains(strings.ToLower(output), "release") {
		t.Errorf("a recusa não nomeia a versão pedida nem menciona a release ausente\nsaída completa:\n%s", output)
	}
	if !strings.Contains(output, "pré-checagem") {
		t.Errorf("a recusa não identifica o passo (pré-checagem)\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current mudou de releases/velha para releases/%s numa pré-checagem que deveria ter recusado ANTES de tocar no serviço", linked)
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
//
// Usa o formato NOVO (servidorFakeComInvalidoPorMotivo, Important 2 da
// revisão final): é o que o binário atual — o que esta atualização instala —
// de fato emite.
func TestScriptDeAtualizacaoToleraUmInvalidoIsoladoDentroDaJanela(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServerWithInvalidByReason(t, stateDir, func(n int64) int64 {
		if n >= 2 {
			return 1
		}
		return 0
	})
	systemctlPath, logPath := writeFakeSystemctl(t, fakeHonest)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com invalid_total subindo só +1 na janela inteira (tolerância default 5): %v\nsaída completa:\n%s", exitCode(err), err, output)
	}
	if linked := linkedRelease(t, root); linked != "nova" {
		t.Errorf("current aponta para releases/%s — esperava releases/nova (um invalid isolado não deveria disparar rollback)", linked)
	}
}

// TestScriptDeAtualizacaoFazRollbackQuandoInvalidoSobeAlemDaToleranciaConfigurada
// é o outro lado do Critical 1: invalid_total crescendo bem acima de
// qualquer tolerância pequena (o sinal de classificação em massa, não de
// descarte isolado) tem de continuar disparando o rollback.
//
// Formato NOVO (servidorFakeComInvalidoPorMotivo, Important 2), pelo mesmo
// motivo do teste acima.
func TestScriptDeAtualizacaoFazRollbackQuandoInvalidoSobeAlemDaToleranciaConfigurada(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServerWithInvalidByReason(t, stateDir, func(n int64) int64 {
		return n * 10 // cresce rápido: delta já estoura a tolerância default (5) na 2ª amostra
	})
	systemctlPath, logPath := writeFakeSystemctl(t, fakeHonest)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "5")

	if err == nil {
		t.Fatalf("o script saiu 0 com invalid_total disparando bem acima da tolerância\nsaída completa:\n%s", output)
	}
	if !strings.Contains(output, "invalid_total") {
		t.Errorf("a mensagem de falha não menciona invalid_total\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback para releases/velha", linked)
	}
}

// TestScriptDeAtualizacaoRespeitaToleranciaCustomizadaDeInvalido prova que
// NISHIDB_UPDATE_INVALID_TOLERANCE é de fato LIDA do ambiente, e não um
// número fixo que só parece configurável: com a tolerância explicitamente
// reduzida para 1, um delta de +2 tem de reprovar — o mesmo delta que
// passaria pela tolerância default (5).
//
// Formato NOVO (servidorFakeComInvalidoPorMotivo, Important 2), pelo mesmo
// motivo dos dois testes acima.
func TestScriptDeAtualizacaoRespeitaToleranciaCustomizadaDeInvalido(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServerWithInvalidByReason(t, stateDir, func(n int64) int64 {
		if n >= 2 {
			return 2
		}
		return 0
	})
	systemctlPath, logPath := writeFakeSystemctl(t, fakeHonest)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "3", "NISHIDB_UPDATE_INVALID_TOLERANCE=1")

	if err == nil {
		t.Fatalf("o script saiu 0 com invalid_total subindo +2 e NISHIDB_UPDATE_INVALID_TOLERANCE=1 — a variável não está sendo respeitada\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback com a tolerância customizada estourada", linked)
	}
}

// TestScriptDeAtualizacaoToleraUmInvalidoIsoladoNoFormatoAntigoSemMotivo é o
// outro lado do Important 2 da revisão final: os três testes de tolerância
// acima provam o formato NOVO (com `motivo`), que é o que o binário desta
// branch emite — mas o binário PRÉ-A5 ainda pode estar no ar no primeiro
// update real do piloto (Minor 4: a primeira migração para o layout
// releases/ é manual, e nada força as duas pontas a trocarem no mesmo
// instante em produção). soma_metrica (nishidb-update.sh) é agnóstica a
// labels — soma por NOME, ignorando o que vem depois de `{` — mas só um
// teste contra o formato antigo de verdade prova que ela continua somando
// esse formato certo depois da mudança do A5. Sem este teste, o oráculo
// inteiro passaria a testar só o formato novo, e uma regressão que quebrasse
// especificamente a leitura da série SEM `motivo` passaria despercebida.
func TestScriptDeAtualizacaoToleraUmInvalidoIsoladoNoFormatoAntigoSemMotivo(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServerWithInvalid(t, stateDir, func(n int64) int64 {
		if n >= 2 {
			return 1
		}
		return 0
	})
	systemctlPath, logPath := writeFakeSystemctl(t, fakeHonest)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com invalid_total (formato ANTIGO, sem motivo) subindo só +1 na janela inteira (tolerância default 5): %v\nsaída completa:\n%s", exitCode(err), err, output)
	}
	if linked := linkedRelease(t, root); linked != "nova" {
		t.Errorf("current aponta para releases/%s — esperava releases/nova (um invalid isolado no formato antigo não deveria disparar rollback)", linked)
	}
}

// TestScriptDeAtualizacaoRecusaVersaoVizinhaPorPrefixo prova a correção do
// Critical 2: "nova2" não pode confirmar o alvo "nova" só porque começa com
// o mesmo texto. Antes da correção, o `case "$versao" in "$alvo"*)` aceitava
// isso — a "única defesa contra binário-velho-config-nova" ficava furada.
func TestScriptDeAtualizacaoRecusaVersaoVizinhaPorPrefixo(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeNeighborVersion)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "3")

	if err == nil {
		t.Fatalf("o script saiu 0 com 'nova2' no /v1/stats — a versão vizinha não deveria confirmar o alvo 'nova'\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback diante da versão vizinha 'nova2'", linked)
	}
	if !strings.Contains(output, "nova2") {
		t.Errorf("a mensagem de falha não mostra a versão vizinha lida ('nova2 (dublê)')\nsaída completa:\n%s", output)
	}
}

// TestScriptDeAtualizacaoAceitaVersaoComSufixoDeBuildinfo é o outro lado do
// Critical 2: a correção não pode rejeitar o formato REAL que
// buildinfo.String() produz ("<versão> (commit ..., build ...)"). Sem este
// teste, uma correção "por igualdade exata da string inteira" (em vez de
// por token) passaria no teste da versão vizinha e quebraria em produção.
func TestScriptDeAtualizacaoAceitaVersaoComSufixoDeBuildinfo(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeVersionWithBuildinfoSuffix)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "10")

	if err != nil {
		t.Fatalf("o script saiu %d com a versão 'nova (commit ..., build ...)' — formato real do buildinfo.String() deveria bater com o alvo 'nova': %v\nsaída completa:\n%s", exitCode(err), err, output)
	}
	if linked := linkedRelease(t, root); linked != "nova" {
		t.Errorf("current aponta para releases/%s — esperava releases/nova", linked)
	}
}

// TestScriptDeAtualizacaoFazRollbackQuandoOSystemctlStartFalha prova o
// Important 2: o `systemctl start` de verdade pode falhar (unidade não
// encontrada, dependência não satisfeita, etc.) — troca_para já tratava
// isso, mas nenhum teste exercitava o ramo antes desta correção.
func TestScriptDeAtualizacaoFazRollbackQuandoOSystemctlStartFalha(t *testing.T) {
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeFailsFirstStart)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "5")

	if err == nil {
		t.Fatalf("o script saiu 0 com o systemctl start falhando na tentativa de subir a versão nova\nsaída completa:\n%s", output)
	}
	if !strings.Contains(output, "systemctl start") {
		t.Errorf("a mensagem de falha não nomeia o passo 'systemctl start'\nsaída completa:\n%s", output)
	}
	if !strings.Contains(strings.ToLower(output), "rollback") {
		t.Errorf("a saída não menciona o rollback\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s — esperava rollback de volta para releases/velha", linked)
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
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeNeverUpSignalsServerClose)

	markerFile := filepath.Join(stateDir, "derruba-servidor")
	serverClosed := make(chan struct{})
	go func() {
		for {
			if _, err := os.Stat(markerFile); err == nil {
				server.Close()
				close(serverClosed)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "2")

	select {
	case <-serverClosed:
	case <-time.After(10 * time.Second):
		t.Fatal("o sinal para derrubar o servidor fake nunca chegou — o dublê não tocou o marcador esperado")
	}

	if err == nil {
		t.Fatalf("o script saiu 0 com o servidor fake parando de responder logo após o start\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s — esperava o rollback devolver para releases/velha quando a porta para de responder", linked)
	}
	if !strings.Contains(output, "verificação") {
		t.Errorf("a saída não nomeia o passo de verificação\nsaída completa:\n%s", output)
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
	root, stateDir := setupRoot(t)
	server := fakeServer(t, stateDir)
	systemctlPath, logPath := writeFakeSystemctl(t, fakeKillsParentAfterStart)

	output, err := runScript(t, root, server.URL, systemctlPath, logPath, "nova", "60")

	if err == nil {
		t.Fatalf("o script saiu 0 apesar de ter recebido SIGTERM no meio da mutação\nsaída completa:\n%s", output)
	}
	if got := exitCode(err); got != 130 {
		t.Errorf("exit code = %d, esperava 130 (convenção escolhida pelo próprio handler de interrupção)\nsaída completa:\n%s", got, output)
	}
	if !strings.Contains(output, "interrompido") {
		t.Errorf("a saída não menciona a interrupção\nsaída completa:\n%s", output)
	}
	if !strings.Contains(strings.ToLower(output), "rollback") {
		t.Errorf("a saída não menciona a tentativa de rollback de emergência\nsaída completa:\n%s", output)
	}
	if !strings.Contains(output, "estado real") {
		t.Errorf("a saída não reporta o estado real resultante (current/health) — Important 1 pede isso SEMPRE\nsaída completa:\n%s", output)
	}
	if linked := linkedRelease(t, root); linked != "velha" {
		t.Errorf("current aponta para releases/%s — esperava o rollback de emergência devolver para releases/velha", linked)
	}
}
