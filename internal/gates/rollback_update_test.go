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
// avançam entre duas amostras, sem depender de tempo real decorrido.
func servidorFake(t *testing.T, estadoDir string) *httptest.Server {
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
		_, _ = fmt.Fprintf(w, "nishidb_ingest_invalid_total{dataset=\"events\"} 0\n")
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
// exec.Cmd.Run (nil quando o script sai 0).
func rodaScript(t *testing.T, raiz, servidorURL, systemctlPath, logPath, versao, prazoSegundos string) (saida string, err error) {
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
