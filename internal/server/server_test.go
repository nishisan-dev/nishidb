package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/server"
)

const specSemKafka = `version: 1
name: %s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
`

// specComKafka aponta para um broker que NÃO existe. É deliberado: o teste
// exercita o caminho em que a ingestão não consegue subir, e o contrato do
// §51 é que as consultas subam assim mesmo.
const specComKafka = `version: 1
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

type ambiente struct {
	t       *testing.T
	dir     string
	cfgPath string
	cfg     *config.Config
}

func novoAmbiente(t *testing.T) *ambiente {
	t.Helper()
	dir := t.TempDir()
	dados := filepath.Join(dir, "dados")
	datasets := filepath.Join(dir, "datasets")
	for _, d := range []string{dados, datasets} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfgPath := filepath.Join(dir, "nishidb.yaml")
	// Porta livre escolhida pelo teste: o config recusa `port: 0` de
	// propósito (zero não é "sem valor"), então quem escolhe é quem testa.
	corpo := "version: 1\nstorage:\n  path: " + dados +
		"\ndatasets:\n  directory: " + datasets +
		"\nserver:\n  bind: 127.0.0.1\n  port: " + strconv.Itoa(portaLivre(t)) + "\n"
	if err := os.WriteFile(cfgPath, []byte(corpo), 0o644); err != nil {
		t.Fatal(err)
	}
	return &ambiente{t: t, dir: dir, cfgPath: cfgPath}
}

func (a *ambiente) escreveDataset(nome, yml string) {
	a.t.Helper()
	if err := os.WriteFile(filepath.Join(a.dir, "datasets", nome+".yaml"), []byte(yml), 0o644); err != nil {
		a.t.Fatal(err)
	}
}

func (a *ambiente) carrega() *config.Config {
	a.t.Helper()
	c, err := config.CarregaConfig(a.cfgPath)
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
	a := novoAmbiente(t)
	a.escreveDataset("bom", strings.ReplaceAll(specSemKafka, "%s", "bom"))
	a.escreveDataset("quebrado", preenche(specComKafka, "quebrado", "true"))

	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}

	ctx, cancela := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancela()
	fim := make(chan error, 1)
	go func() { fim <- srv.Roda(ctx, "") }()

	esperaHealth(t, srv, func(h server.Saude) bool {
		return h.Datasets["quebrado"] == server.EstadoDown
	})

	h := leHealth(t, srv)
	if h.Datasets["bom"] != server.EstadoDesligado {
		t.Errorf("dataset sem Kafka = %q, quer %q", h.Datasets["bom"], server.EstadoDesligado)
	}
	if h.Kafka != server.EstadoDown {
		t.Errorf("kafka = %q, quer DOWN: um dataset parado não pode sair como UP", h.Kafka)
	}
	if h.Detalhes["quebrado"] == "" {
		t.Error("o health não nomeia o motivo; num processo com vários datasets, DOWN sozinho não ajuda")
	}
	cancela()
	<-fim
}

// D15 — descrever o consumer não é ligá-lo.
func TestEnabledFalseNaoSobeConsumer(t *testing.T) {
	a := novoAmbiente(t)
	a.escreveDataset("inerte", preenche(specComKafka, "inerte", "false"))

	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatalf("Novo: %v", err)
	}
	ctx, cancela := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancela()
	fim := make(chan error, 1)
	go func() { fim <- srv.Roda(ctx, "") }()

	// Se o consumer tivesse subido contra 127.0.0.1:1, o dataset apareceria
	// como DOWN em poucos segundos.
	time.Sleep(2 * time.Second)
	h := leHealth(t, srv)
	if h.Datasets["inerte"] != server.EstadoDesligado {
		t.Errorf("dataset = %q, quer DISABLED: enabled:false não pode subir consumer", h.Datasets["inerte"])
	}
	if h.Status != server.EstadoUP {
		t.Errorf("status = %q, quer UP", h.Status)
	}
	cancela()
	<-fim
}

// D4 — a colisão de groupId é validação de CONJUNTO e tem de matar a subida,
// não virar estado parcial silencioso.
func TestGroupIdDuplicadoRecusaASubida(t *testing.T) {
	a := novoAmbiente(t)
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
	a.escreveDataset("um", strings.ReplaceAll(yml, "%NOME%", "um"))
	a.escreveDataset("dois", strings.ReplaceAll(yml, "%NOME%", "dois"))

	_, err := server.Novo(a.carrega(), server.Opcoes{})
	if err == nil {
		t.Fatal("quer recusa: dois datasets no mesmo group dividiriam as partições")
	}
	if !strings.Contains(err.Error(), "mesmo-group") {
		t.Errorf("a mensagem não nomeia o group: %v", err)
	}
}

// §92/§93: o scrape tem de trazer os nomes literais da spec.
func TestMetricsExpoeOsNomesDoSpec92(t *testing.T) {
	a := novoAmbiente(t)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancela := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancela()
	fim := make(chan error, 1)
	go func() { fim <- srv.Roda(ctx, "") }()
	defer func() { cancela(); <-fim }()

	corpo := pega(t, srv, "/metrics")
	for _, nome := range []string{
		"nishidb_go_heap_bytes",
		"nishidb_checkpoint_duration_seconds",
		"nishidb_checkpoint_last_success_timestamp",
	} {
		if !strings.Contains(corpo, nome) {
			t.Errorf("métrica %s ausente do scrape", nome)
		}
	}
	if !strings.Contains(corpo, `dataset="d"`) {
		t.Error("as séries não carregam o label dataset")
	}
}

// §94: a chave "kafka" é obrigatória, mesmo sem nenhum dataset consumindo.
func TestHealthTemAChaveKafka(t *testing.T) {
	a := novoAmbiente(t)
	a.escreveDataset("d", strings.ReplaceAll(specSemKafka, "%s", "d"))

	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancela := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancela()
	fim := make(chan error, 1)
	go func() { fim <- srv.Roda(ctx, "") }()
	defer func() { cancela(); <-fim }()

	var bruto map[string]any
	if err := json.Unmarshal([]byte(pega(t, srv, "/v1/health")), &bruto); err != nil {
		t.Fatal(err)
	}
	for _, chave := range []string{"status", "kafka", "storage", "datasets"} {
		if _, ok := bruto[chave]; !ok {
			t.Errorf("chave %q ausente do health do §94", chave)
		}
	}
}

func preenche(tmpl, nome, enabled string) string {
	s := strings.Replace(tmpl, "%s", nome, 1)
	s = strings.Replace(s, "%s", enabled, 1)
	return strings.ReplaceAll(s, "%s", nome)
}

func pega(t *testing.T, srv *server.Servidor, caminho string) string {
	t.Helper()
	resp, err := http.Get("http://" + srv.Endereco() + caminho)
	if err != nil {
		t.Fatalf("GET %s: %v", caminho, err)
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

func leHealth(t *testing.T, srv *server.Servidor) server.Saude {
	t.Helper()
	var h server.Saude
	if err := json.Unmarshal([]byte(pega(t, srv, "/v1/health")), &h); err != nil {
		t.Fatalf("health: %v", err)
	}
	return h
}

// esperaHealth espera por CONDIÇÃO, e não por sleep: dormir esconde tanto a
// lentidão quanto a ausência de progresso.
func esperaHealth(t *testing.T, srv *server.Servidor, ok func(server.Saude) bool) {
	t.Helper()
	prazo := time.Now().Add(15 * time.Second)
	for time.Now().Before(prazo) {
		if ok(leHealth(t, srv)) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condição de health não foi atingida a tempo: %+v", leHealth(t, srv))
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
