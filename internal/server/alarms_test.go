package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// specServicesComAlarme é a spec de teste com um alarme declarado. O prazo é o
// mínimo permitido, para que a cadência da varredura (prazo/4) caiba na suíte.
const specServicesComAlarme = `version: 1
name: %s
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    slot:
      type: int64
    port:
      type: int64
    authenticated:
      type: boolean
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    alarms:
      - name: olt_down
        when: "total < 1000"
        sustainedFor: 10s
`

func serverWithAlarme(t *testing.T, n int) *server.Server {
	t.Helper()
	srv, _ := serverWithAlarmeManual(t, n)
	return srv
}

// serverWithAlarmeManual devolve o servidor e a função que o ENCERRA, para o
// teste que precisa observar o estado depois do encerramento — o run() comum
// encerra só no Cleanup, quando já não há o que conferir.
func serverWithAlarmeManual(t *testing.T, n int) (*server.Server, func()) {
	t.Helper()
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("servicos", fmt.Sprintf(specServicesComAlarme, "servicos"))
	seedServices(t, a, "servicos", n)
	srv, err := server.Novo(a.load(), server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	end := make(chan error, 1)
	go func() { end <- srv.Run(ctx, "") }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-end; err != nil {
				t.Errorf("Run: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	// Espera o laço subir: o Run instala as goroutines de fundo depois do
	// HTTP, e conferir antes disso testaria o escalonador.
	for i := 0; i < 200 && !srv.AlarmsActive("servicos"); i++ {
		time.Sleep(5 * time.Millisecond)
	}
	return srv, stop
}

// TestAlarmeDeclaradoSobeOLacoDeVarredura: a costura entre o YAML e o laço.
// Sem isto, um alarme declarado ficaria calado e nada no processo diria por quê.
func TestAlarmeDeclaradoSobeOLacoDeVarredura(t *testing.T) {
	srv := serverWithAlarme(t, 24)
	if !srv.AlarmsActive("servicos") {
		t.Fatal("o dataset declara alarme e o laço não subiu")
	}
}

// TestDatasetSemAlarmeNaoSobeLaco: não se paga goroutine para não fazer nada.
func TestDatasetSemAlarmeNaoSobeLaco(t *testing.T) {
	srv := serverWithServices(t, 24)
	if srv.AlarmsActive("servicos") {
		t.Fatal("subiu o laço de alarmes num dataset que não declara nenhum")
	}
}

// TestAlarmeInvalidoRecusaASubida: a condição é compilada contra o schema da
// tabela do contador na SUBIDA. Um nome de coluna errado tem de derrubar o
// processo agora, e não ficar esperando a madrugada em que o alarme dispararia.
func TestAlarmeInvalidoRecusaASubida(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	spec := strings.Replace(fmt.Sprintf(specServicesComAlarme, "servicos"),
		`when: "total < 1000"`, `when: "inexistente < 1000"`, 1)
	a.writeDataset("servicos", spec)
	if _, err := server.Novo(a.load(), server.Options{}); err == nil {
		t.Fatal("o servidor subiu com uma condição de alarme que não casa o schema da tabela")
	} else if !strings.Contains(err.Error(), "inexistente") {
		t.Errorf("a recusa não nomeia a coluna: %v", err)
	}
}

// TestEncerrarParaOLacoDeAlarmes prova que o encerramento CANCELA E AGUARDA o
// laço: o controle volta com o medidor em zero, e não com uma janela em que a
// goroutine ainda varre.
//
// Este teste NÃO prova a ORDEM em relação ao fechamento dos datasets, e é
// importante não achar que prova: a passada sobre algumas dezenas de grupos
// termina em microssegundos, então o laço já está dormindo no timer quando o
// encerramento chega, e invertendo a ordem no código cinco rodadas sob -race
// passam verdes. Quem fixa a ordem é o gate que lê a AST de encerra()
// (internal/gates/alarm_shutdown_test.go).
func TestEncerrarParaOLacoDeAlarmes(t *testing.T) {
	srv, stop := serverWithAlarmeManual(t, 24)
	if !srv.AlarmsActive("servicos") {
		t.Fatal("o laço não subiu; o teste não exercita nada")
	}
	stop()
	if srv.AlarmsActive("servicos") {
		t.Fatal("o encerramento devolveu o controle com o laço de alarmes ainda de pé")
	}
}

// specServicesComEmissao declara a emissão periódica no menor intervalo
// permitido, para que a primeira passada caiba na suíte.
const specServicesComEmissao = `version: 1
name: %s
schemaName: telecom
materialization:
  key:
    field: customerId
schema:
  fields:
    customerId:
      type: string
    olt:
      type: string
    slot:
      type: int64
    port:
      type: int64
    authenticated:
      type: boolean
counters:
  - name: autenticados_por_olt
    groupBy: [olt]
    where: authenticated = true
    emit:
      every: 10s
      maxBytes: 1MiB
      keep: 2
`

// TestEmissaoDeclaradaEscreveAserie é a costura ponta a ponta da parte 2: o
// YAML declara, o laço sobe, e o arquivo aparece no diretório do dataset com
// uma linha por grupo.
func TestEmissaoDeclaradaEscreveAserie(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("servicos", fmt.Sprintf(specServicesComEmissao, "servicos"))
	seedServices(t, a, "servicos", 24)
	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)

	path := filepath.Join(cfg.DatasetRootDir("servicos"), schema.EmitDirName, "autenticados_por_olt.ndjson")
	var linhas []string
	for i := 0; i < 400; i++ {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			linhas = strings.Split(strings.TrimRight(string(b), "\n"), "\n")
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(linhas) == 0 {
		t.Fatalf("nada escrito em %s", path)
	}
	// 24 serviços: 6 na OLT14 e 18 na G1M10, dois grupos no total.
	if len(linhas) != 2 {
		t.Fatalf("%d linhas na primeira passada, quero uma por grupo:\n%s", len(linhas), strings.Join(linhas, "\n"))
	}
	var l map[string]any
	if err := json.Unmarshal([]byte(linhas[0]), &l); err != nil {
		t.Fatalf("a linha não é JSON: %v\n%s", err, linhas[0])
	}
	if l["counter"] != "autenticados_por_olt" {
		t.Errorf("counter = %v", l["counter"])
	}
	if l["groupKey"] == nil || l["olt"] == nil || l["total"] == nil || l["at"] == nil {
		t.Errorf("a linha não traz os campos da série: %+v", l)
	}
	if !srv.EmitActive("servicos") {
		t.Error("o laço de emissão devia estar de pé")
	}
}

// TestDatasetSemEmissaoNaoCriaODiretorio: não se cria estrutura para quem não
// declarou nada.
func TestDatasetSemEmissaoNaoCriaODiretorio(t *testing.T) {
	a := newEnvWithStorage(t, blockPadrao, "")
	a.writeDataset("servicos", fmt.Sprintf(specServices, "servicos"))
	seedServices(t, a, "servicos", 8)
	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	if srv.EmitActive("servicos") {
		t.Fatal("subiu emissão num dataset que não a declara")
	}
	if _, err := os.Stat(filepath.Join(cfg.DatasetRootDir("servicos"), schema.EmitDirName)); !os.IsNotExist(err) {
		t.Errorf("o diretório da série foi criado sem ninguém declarar emissão (err=%v)", err)
	}
}
