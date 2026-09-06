package server_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// travaDeLinha é um io.Writer de log que trava a goroutine gravando na
// PRIMEIRA linha que contém `alvo`, e só a solta quando `libera` fecha.
//
// Existe porque storage.SetAposentadoriaTraceForTest — o gate que a suíte de
// internal/storage usa para travar uma passada de compaction no meio — mora
// num arquivo _test.go do PACOTE storage (export_test.go), e por isso é
// invisível para quem só o IMPORTA (internal/server): arquivos de teste
// nunca entram no archive que um pacote consumidor enxerga, nem em teste
// interno nem externo. A via disponível aqui é a de PRODUÇÃO que toda
// passada de compaction já usa de qualquer forma: o log estruturado —
// aposentaSegmento emite "compaction.segmento_aposentado" (compaction.go)
// depois de cada candidato inteiro, o mesmo instante em que o gate do
// pacote storage prenderia a goroutine.
type travaDeLinha struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	alvo   string
	visto  chan struct{}
	uma    sync.Once
	libera chan struct{}
}

func novaTravaDeLinha(alvo string) *travaDeLinha {
	return &travaDeLinha{alvo: alvo, visto: make(chan struct{}), libera: make(chan struct{})}
}

func (g *travaDeLinha) Write(p []byte) (int, error) {
	linha := string(p)
	g.mu.Lock()
	g.buf.WriteString(linha)
	g.mu.Unlock()

	if strings.Contains(linha, g.alvo) {
		primeira := false
		g.uma.Do(func() { primeira = true; close(g.visto) })
		if primeira {
			<-g.libera
		}
	}
	return len(p), nil
}

func (g *travaDeLinha) ocorrencias() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return strings.Count(g.buf.String(), g.alvo)
}

// TestSIGTERMDuranteCompactacaoManualCancelaAPassadaEEncerraRapido é o RED
// do Important 1 da revisão final: sem o ctx da passada manual composto com
// o ciclo de vida do servidor, http.Server.Shutdown NÃO cancela r.Context()
// (só espera o handler devolver, e por um tempo limitado) — uma passada de
// POST /v1/compact em voo no SIGTERM continua rodando até processar TODOS
// os candidatos elegíveis, e o compEmVoo.Wait() do Close (storage) fica sem
// teto até ela acabar sozinha, minutos depois em produção.
//
// O oráculo é a CONTAGEM de "compaction.segmento_aposentado" no log: com o
// ctx composto, a passada para no candidato QUE JÁ ESTAVA em voo quando o
// servidor cancela (exatamente 1, o gate deste teste) e nunca chega ao
// segundo — sem a composição, ela varre e aposenta os demais candidatos do
// corpus normalmente, e a contagem sobe.
//
// Determinístico sem sleep de corrida: travaDeLinha prova "em voo" por
// canal fechado, e o único tempo dado de propósito (50ms, depois de
// cancela() já ter fechado ctx.Done() de forma síncrona) é folga de
// agendador para a goroutine do context.AfterFunc rodar — não uma aposta de
// quanto a passada demora.
func TestSIGTERMDuranteCompactacaoManualCancelaAPassadaEEncerraRapido(t *testing.T) {
	a := novoAmbienteComStorage(t, blocoPadrao, fmt.Sprintf(storageComCompaction, "false"))
	a.escreveDataset("d", strings.ReplaceAll(specConsultavel, "%s", "d"))
	semeiaGarbage(t, a, "d", compKeys, compRodadas)

	trava := novaTravaDeLinha("compaction.segmento_aposentado")
	cfg := a.carrega()
	srv, err := server.Novo(cfg, server.Opcoes{Log: logging.New(trava, logging.FormatText, slog.LevelInfo)})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}

	ctx, cancela := context.WithCancel(context.Background())
	fimRoda := make(chan error, 1)
	go func() { fimRoda <- srv.Roda(ctx, "") }()
	esperaListenerAceitar(t, srv.Endereco())

	respCh := make(chan *http.Response, 1)
	go func() {
		resp, postErr := http.Post("http://"+srv.Endereco()+"/v1/compact", "", nil)
		if postErr != nil {
			t.Logf("POST /v1/compact: %v (aceitável se o encerramento cortou a conexão)", postErr)
			respCh <- nil
			return
		}
		respCh <- resp
	}()

	select {
	case <-trava.visto:
	case <-time.After(30 * time.Second):
		t.Fatal("a passada manual não aposentou nenhum segmento em 30s: sem uma passada em voo o teste não exercita nada")
	}

	// SIGTERM: cancela o MESMO ctx que main.go cancelaria via
	// signal.NotifyContext, com a passada manual travada no meio.
	cancela()
	// Folga de agendador para o context.AfterFunc (registrado no ctx da
	// passada manual) rodar antes de a passada retomar — não uma aposta de
	// duração de trabalho, só de troca de goroutine.
	time.Sleep(50 * time.Millisecond)
	close(trava.libera)

	select {
	case err := <-fimRoda:
		if err != nil {
			t.Fatalf("Roda: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Roda não retornou em 5s depois do gate liberado: o Close ficou esperando a passada manual processar " +
			"os candidatos restantes em vez de ser cancelada pelo encerramento do servidor")
	}

	if resp := <-respCh; resp != nil {
		defer fecha(t, resp)
		if resp.StatusCode == http.StatusOK {
			t.Errorf("status = 200: a passada terminou com sucesso — o cancelamento do servidor não interrompeu nada")
		}
	}

	if n := trava.ocorrencias(); n != 1 {
		t.Fatalf("compaction.segmento_aposentado apareceu %d vez(es) no log, quer exatamente 1: a passada continuou "+
			"aposentando candidatos depois do encerramento do servidor em vez de parar no primeiro (ctx cancelado pelo SIGTERM)", n)
	}
}
