package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

const specWithoutKafka = `version: 1
name: %s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`

// specWithKafka aponta para um broker que NÃO existe. É deliberado: o teste
// exercita o caminho em que a ingestão não consegue subir, e o contrato do
// §51 é que as consultas subam assim mesmo.
const specWithKafka = `version: 1
name: %s
source:
  type: kafka
  kafka:
    enabled: %s
    bootstrapServers:
      - 127.0.0.1:1
    topic: t-%s
    consumer:
      groupId: g-%s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`

type env struct {
	t       *testing.T
	dir     string
	cfgPath string
	cfg     *config.Config
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n")
}

// newEnvWith monta o ambiente com o bloco `server` que o teste quiser.
// O %d recebe uma porta livre.
func newEnvWith(t *testing.T, blockServer string) *env {
	t.Helper()
	return newEnvWithStorage(t, blockServer, "")
}

// newEnvWithStorage acrescenta linhas ao bloco `storage:` do nishidb.yaml.
// Existe porque há asserção que precisa de MAIS DE UM segmento em disco: com um
// só, toda conta que percorre segmentos é indistinguível de uma que lê apenas o
// corrente, e o mutante que divide pela contagem sobrevive por ser identidade.
func newEnvWithStorage(t *testing.T, blockServer, storageExtra string) *env {
	t.Helper()
	dir := t.TempDir()
	data := filepath.Join(dir, "dados")
	datasets := filepath.Join(dir, "datasets")
	for _, d := range []string{data, datasets} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfgPath := filepath.Join(dir, "nishidb.yaml")
	// Porta livre escolhida pelo teste: o config recusa `port: 0` de
	// propósito (zero não é "sem valor"), então quem escolhe é quem testa.
	corpo := "version: 1\nstorage:\n  path: " + data + "\n" + storageExtra +
		"datasets:\n  directory: " + datasets +
		fmt.Sprintf(blockServer, portaLivre(t))
	if err := os.WriteFile(cfgPath, []byte(corpo), 0o644); err != nil {
		t.Fatal(err)
	}
	return &env{t: t, dir: dir, cfgPath: cfgPath}
}

func (a *env) writeDataset(name, yml string) {
	a.t.Helper()
	if err := os.WriteFile(filepath.Join(a.dir, "datasets", name+".yaml"), []byte(yml), 0o644); err != nil {
		a.t.Fatal(err)
	}
}

func (a *env) load() *config.Config {
	a.t.Helper()
	c, err := config.Load(a.cfgPath)
	if err != nil {
		a.t.Fatal(err)
	}
	a.cfg = c
	return c
}

// O contrato do §51, que é a razão de o servidor existir: um dataset cuja
// ingestão NÃO pode subir continua consultável, e o problema aparece no health
// em vez de derrubar o processo.
func TestIngestaoQueNaoSobeNaoDerrubaAsConsultas(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("bom", strings.ReplaceAll(specWithoutKafka, "%s", "bom"))
	a.writeDataset("quebrado", preenche(specWithKafka, "quebrado", "true"))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()

	waitHealth(t, srv, func(h protocol.Health) bool {
		return h.Datasets["quebrado"] == protocol.StateDown
	})

	h := readHealth(t, srv)
	if h.Datasets["bom"] != protocol.StateDisabled {
		t.Errorf("dataset sem Kafka = %q, quer %q", h.Datasets["bom"], protocol.StateDisabled)
	}
	if h.Kafka != protocol.StateDown {
		t.Errorf("kafka = %q, quer DOWN: um dataset parado não pode sair como UP", h.Kafka)
	}
	if h.Details["quebrado"] == "" {
		t.Error("o health não nomeia o motivo; num processo com vários datasets, DOWN sozinho não ajuda")
	}
	cancel()
	<-end
}

// D15 — descrever o consumer não é ligá-lo.
func TestEnabledFalseNaoSobeConsumer(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("inerte", preenche(specWithKafka, "inerte", "false"))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()

	// Se o consumer tivesse subido contra 127.0.0.1:1, o dataset apareceria
	// como DOWN em poucos segundos.
	time.Sleep(2 * time.Second)
	h := readHealth(t, srv)
	if h.Datasets["inerte"] != protocol.StateDisabled {
		t.Errorf("dataset = %q, quer DISABLED: enabled:false não pode subir consumer", h.Datasets["inerte"])
	}
	if h.Status != protocol.StateUp {
		t.Errorf("status = %q, quer UP", h.Status)
	}
	cancel()
	<-end
}

// D4 — a colisão de groupId é validação de CONJUNTO e tem de matar a subida,
// não virar estado parcial silencioso.
func TestGroupIdDuplicadoRecusaASubida(t *testing.T) {
	a := newEnv(t)
	// Os dois com o mesmo tópico e o mesmo group.
	yml := `version: 1
name: %NOME%
source:
  type: kafka
  kafka:
    enabled: true
    bootstrapServers: [127.0.0.1:1]
    topic: mesmo-topico
    consumer:
      groupId: mesmo-group
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`
	a.writeDataset("um", strings.ReplaceAll(yml, "%NOME%", "um"))
	a.writeDataset("dois", strings.ReplaceAll(yml, "%NOME%", "dois"))

	_, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		t.Fatal("quer recusa: dois datasets no mesmo group dividiriam as partições")
	}
	if !strings.Contains(err.Error(), "mesmo-group") {
		t.Errorf("a mensagem não nomeia o group: %v", err)
	}
}

// §92/§93: o scrape tem de trazer os nomes literais da spec.
func TestMetricsExpoeOsNomesDoSpec92(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	defer func() { cancel(); <-end }()

	corpo := fetch(t, srv, "/metrics")
	for _, name := range []string{
		"nishidb_go_heap_bytes",
		"nishidb_checkpoint_duration_seconds",
		"nishidb_checkpoint_last_success_timestamp",
	} {
		if !strings.Contains(corpo, name) {
			t.Errorf("métrica %s ausente do scrape", name)
		}
	}
	if !strings.Contains(corpo, `dataset="d"`) {
		t.Error("as séries não carregam o label dataset")
	}
}

// §94: a chave "kafka" é obrigatória, mesmo sem nenhum dataset consumindo.
func TestHealthTemAChaveKafka(t *testing.T) {
	a := newEnv(t)
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	defer func() { cancel(); <-end }()

	var bruto map[string]any
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/health")), &bruto); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"status", "kafka", "storage", "datasets"} {
		if _, ok := bruto[key]; !ok {
			t.Errorf("chave %q ausente do health do §94", key)
		}
	}
}

func preenche(tmpl, name, enabled string) string {
	s := strings.Replace(tmpl, "%s", name, 1)
	s = strings.Replace(s, "%s", enabled, 1)
	return strings.ReplaceAll(s, "%s", name)
}

func fetch(t *testing.T, srv *server.Server, path string) string {
	t.Helper()
	resp, err := http.Get("http://" + srv.Endereco() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("fechar o corpo da resposta: %v", err)
		}
	}()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func readHealth(t *testing.T, srv *server.Server) protocol.Health {
	t.Helper()
	var h protocol.Health
	if err := json.Unmarshal([]byte(fetch(t, srv, "/v1/health")), &h); err != nil {
		t.Fatalf("health: %v", err)
	}
	return h
}

// waitHealth espera por CONDIÇÃO, e não por sleep: dormir esconde tanto a
// lentidão quanto a ausência de progresso.
func waitHealth(t *testing.T, srv *server.Server, ok func(protocol.Health) bool) {
	t.Helper()
	prazo := time.Now().Add(15 * time.Second)
	for time.Now().Before(prazo) {
		if ok(readHealth(t, srv)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condição de health não foi atingida a tempo: %+v", readHealth(t, srv))
}

// portaLivre reserva uma porta pedindo :0 ao sistema e devolvendo-a em
// seguida. Há uma janela de corrida teórica; na prática é o padrão usado por
// suítes que precisam de porta previsível antes do bind.
func portaLivre(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	porta := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return porta
}

// `server.enabled: false` é o operador desligando deliberadamente, e recusar
// é o oposto de aceitar-e-ignorar — que é o defeito que a issue #1 denuncia.
// Ausente é outra coisa: rodar `nishidb server` já é o ato de ligar.
func TestServerEnabledFalseRecusaASubida(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n  enabled: false\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	_, err := server.Novo(a.load(), server.Options{})
	if err == nil {
		t.Fatal("quer recusa com server.enabled: false")
	}
	if !strings.Contains(err.Error(), "server.enabled") {
		t.Errorf("a mensagem não nomeia a chave: %v", err)
	}
}

func TestServerSemBlocoEnabledSobe(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("sem a chave o servidor deveria subir: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
}

// risesCapturandoLog monta o servidor com um logger próprio e devolve o que ele
// escreveu na subida.
func risesCapturandoLog(t *testing.T, a *env) string {
	t.Helper()
	var buf bytes.Buffer
	srv, err := server.Novo(a.load(), server.Options{
		Log: logging.New(&buf, logging.FormatJSON, slog.LevelInfo),
	})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// Com POST /v1/query no ar e sem autenticação até a Fase 11, escutar fora do
// loopback entrega o dataset a quem alcançar a porta. O default protege; quem
// abre precisa ver que abriu.
//
// A matriz de recusa do §77 (B2) exige `server.authentication` DECLARADA com
// `enabled: false` EXPLÍCITO para que o bind exposto ainda suba só com
// aviso — seção ausente vira recusa (célula 2 da matriz, ver
// security_test.go). Este teste continua sobre a célula "decisão já
// tomada pelo operador"; a célula da seção ausente tem teste próprio.
func TestBindForaDoLoopbackAvisa(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 0.0.0.0\n  port: %d\n  authentication:\n    enabled: false\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	if !strings.Contains(saida, "bind.exposto") {
		t.Fatalf("subida com bind aberto não avisou: %s", saida)
	}
	if !strings.Contains(saida, "/v1/query") {
		t.Fatalf("o aviso não diz o que está exposto: %s", saida)
	}
}

// Aviso que aparece sempre é ruído, e ruído treina o operador a ignorar.
func TestBindEmLoopbackNaoAvisa(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	if saida := risesCapturandoLog(t, a); strings.Contains(saida, "bind.exposto") {
		t.Fatalf("bind em loopback não deveria avisar: %s", saida)
	}
}

// TestBindEmLoopbackSemNadaDeSegurancaDeclaradoNaoLogaNada é a paridade do
// piloto por SILÊNCIO TOTAL, não só pela ausência da substring "bind.exposto"
// que TestBindEmLoopbackNaoAvisa já trava. A config do piloto de hoje (bind
// loopback, nenhum bloco tls/authentication/loopback no YAML) não pode ganhar
// NENHUM aviso novo na subida por causa desta fase — nem um WARN de uma
// checagem futura que alguém esqueça de gatear por bind exposto ou por
// authentication ligada. A asserção é sobre o BUFFER inteiro (deve ficar
// vazio), não sobre uma lista de substrings que a próxima linha de log
// nova esqueceria de atualizar.
func TestBindEmLoopbackSemNadaDeSegurancaDeclaradoNaoLogaNada(t *testing.T) {
	a := newEnvWith(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	if saida := risesCapturandoLog(t, a); saida != "" {
		t.Fatalf("subida do piloto (loopback, sem segurança declarada) logou algo: %s", saida)
	}
}

// Chave de runtime aceita e não implementada é nomeada na subida (resto do
// débito 15).
//
// A chave do teste MUDOU de `memoryTarget` para `managedMemoryTarget` porque a
// primeira ganhou leitor — e continuar avisando sobre ela seria mentir na
// direção OPOSTA: o operador desconfiaria de um limite que está de fato valendo,
// e subiria o valor para "compensar" um teto que ele acha inexistente. O
// contrapositivo — que a chave COM leitor não avisa mais — é a segunda metade
// deste teste.
func TestChaveDeRuntimeIgnoradaApareceNaSubida(t *testing.T) {
	a := newEnvWith(t,
		"\nserver:\n  bind: 127.0.0.1\n  port: %d\nruntime:\n  managedMemoryTarget: 1GiB\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	if !strings.Contains(saida, "runtime.managedMemoryTarget") {
		t.Fatalf("a chave sem implementação não foi nomeada: %s", saida)
	}
}

// E a chave que GANHOU leitor não é mais nomeada.
//
// Sem esta metade, tirar `memoryTarget` da lista de ignoradas e esquecer de
// ligá-la passaria — o operador deixaria de receber o aviso E continuaria sem
// teto, que é estritamente pior do que o estado anterior.
func TestChaveDeRuntimeComLeitorNaoEhMaisNomeadaNaSubida(t *testing.T) {
	a := newEnvWith(t,
		"\nserver:\n  bind: 127.0.0.1\n  port: %d\nruntime:\n  memoryTarget: 1GiB\n"+
			"  query:\n    maxBufferPerQuery: 4MiB\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	for _, key := range []string{"runtime.memoryTarget", "runtime.query.maxBufferPerQuery"} {
		if strings.Contains(saida, key) {
			t.Errorf("a subida ainda avisa que %s não tem efeito, e ela tem: %s", key, saida)
		}
	}
}

// storage.durability (débito 50) é aceita e ignorada — só existe o
// SyncEveryPut interno, que o CLI nunca liga. Mencioná-la na subida é a mesma
// doutrina do débito 15: chave que mente sem aviso é pior que chave ausente.
func TestStorageDurabilityIgnoradaApareceNaSubida(t *testing.T) {
	a := newEnvWithStorage(t, "\nserver:\n  bind: 127.0.0.1\n  port: %d\n",
		"  durability:\n    mode: sync\n    interval: 100ms\n")
	a.writeDataset("d", strings.ReplaceAll(specWithoutKafka, "%s", "d"))

	saida := risesCapturandoLog(t, a)
	if !strings.Contains(saida, "storage.durability") {
		t.Fatalf("a chave sem implementação não foi nomeada: %s", saida)
	}
}

// O corpo do /v1/health é contrato: o driver JDBC vai lê-lo pelo pacote
// protocolo, e o painel Python já o lê hoje. Mover o tipo de pacote não pode
// mudar um byte.
//
// A asserção é sobre os BYTES, e não sobre a struct decodificada, porque é o
// que um refactor de serialização de fato quebra: renomear uma tag json,
// perder o SetIndent, ganhar um omitempty. Nada disso aparece quando o teste
// decodifica de volta para a própria struct que produziu a saída.
func TestOCorpoDoHealthNaoMudouComAMudancaDePacote(t *testing.T) {
	srv := serverWithData(t, 1, blockPadrao)

	quer := "{\n" +
		"  \"status\": \"UP\",\n" +
		"  \"kafka\": \"DISABLED\",\n" +
		"  \"storage\": \"UP\",\n" +
		"  \"datasets\": {\n" +
		"    \"d\": \"DISABLED\"\n" +
		"  }\n" +
		"}\n"
	if got := fetch(t, srv, "/v1/health"); got != quer {
		t.Errorf("corpo do /v1/health mudou:\n--- got ---\n%s\n--- quer ---\n%s", got, quer)
	}
}
