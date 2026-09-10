package server_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// registroDoServidor é o io.Writer do logger do servidor sob concorrência: a
// subida escreve de dentro do Roda, numa goroutine, enquanto o teste lê. Um
// bytes.Buffer nu aí é corrida de dado, e o -race a acusaria antes de o oráculo
// dizer qualquer coisa.
type registroDoServidor struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *registroDoServidor) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *registroDoServidor) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

const (
	compKeys    = 60
	compRodadas = 25
)

// storageComCompaction é o bloco `storage:` do operador que decidiu sobre a
// chave. O segmentSize é pequeno pelo mesmo motivo do e2e: o que a eleição do
// §53 precisa é de uma PILHA de segmentos selados, não de arquivos grandes.
const storageComCompaction = "  segmentSize: 8KiB\n  compaction:\n    enabled: %s\n    minimumLiveRatio: 0.5\n"

// semeiaGarbage materializa `rodadas` updates sobre um key set FIXO e fecha o
// dataset — o mesmo ciclo do `nishidb ingest`, e a forma do dataset do §86: a
// rodada seguinte torna garbage a anterior inteira.
//
// É update de verdade pelo pipeline real (Materializer.Apply + Store.Put), e
// não arquivo montado à mão: o que decide a eleição do §53 é o tally de
// liveness, e ele só existe porque o writer o manteve record a record.
func semeiaGarbage(t *testing.T, a *ambiente, nome string, keys, rodadas int) {
	t.Helper()
	cfg := a.carrega()
	spec, err := schema.Load(cfg.DatasetSpecPath(nome), cfg.QueryDefaults())
	if err != nil {
		t.Fatal(err)
	}
	// A lista sai de schema.IndexedFieldNames, e não de um laço local: um helper de
	// teste que abre o dataset com um conjunto de índices diferente do que o
	// servidor usa reproduz, dentro da suíte, o modo de falha cruzado que aquela
	// função existe para impedir.
	indexados := schema.IndexedFieldNames(spec)
	st, err := storage.Open(storage.Options{
		Dir:                 cfg.DatasetDir(nome),
		Dataset:             nome,
		SegmentSize:         cfg.SegmentSize(),
		IndexedFieldsByName: indexados,
	})
	if err != nil {
		t.Fatal(err)
	}
	compilado, err := spec.Compile(st.FieldDict())
	if err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	mat, err := materializer.New(compilado, st)
	if err != nil {
		t.Fatal(err)
	}
	for r := 0; r < rodadas; r++ {
		for i := 0; i < keys; i++ {
			estado := "Start"
			if r%2 == 0 {
				estado = "Stop"
			}
			ev := fmt.Sprintf(`{"id":"S-%06d","status":%q,"porta":%d,"ip":"10.0.0.%d"}`,
				i, estado, 1000+r, i%256)
			if res := mat.Apply([]byte(ev)); res.Err != nil {
				_ = st.Close()
				t.Fatalf("rodada %d, evento %d: %v", r, i, res.Err)
			}
		}
	}
	// O Checkpoint é o que instala o segSize, e sem segSize a eleição do §53
	// não tem por onde dividir a liveness — um dataset semeado sem ele não
	// produziria candidato nenhum, e os dois lados deste teste ficariam verdes
	// por vacuidade.
	//
	// NOTA (Task B5, revisão): isto NÃO cria uma janela "segSize desconhecido"
	// para quem abrir este dataset depois — Store.Open() SEMPRE checkpointa
	// no fim da recovery (store.go, "Checkpoint imediato: manifest desde a
	// criação e estado limpo pós-recovery"), então segSize já está populado
	// no instante em que um servidor abre este diretório, com ou sem este
	// Checkpoint aqui. O daqui existe só para o PRÓPRIO teste medir o disco
	// ANTES de fechar (bancadaDeCompaction o faz por fora, com os.Stat).
	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// bancadaDeCompaction monta o dataset cheio de garbage, sobe o SERVIDOR com a
// chave do §78 no valor pedido e devolve o servidor de pé, o diretório do
// dataset, o log da subida e o disco medido ANTES de o processo abrir o
// dataset.
//
// O disco é medido antes da subida de propósito: a primeira passada do laço é
// imediata, então medir depois já mediria um diretório possivelmente
// compactado, e a diferença que este teste afere desapareceria na largada.
//
// O servidor entra na devolução pela Task 8: as três séries do §92 se leem
// PELA PORTA (/metrics), e sem o handle não há como raspá-lo — medir a
// compaction por dentro do processo é justamente o que o critério de aceite
// recusa.
func bancadaDeCompaction(t *testing.T, chave string) (srv *server.Servidor, dir string, registro *registroDoServidor, antes int64) {
	t.Helper()
	a := novoAmbienteComStorage(t, blocoPadrao, fmt.Sprintf(storageComCompaction, chave))
	a.escreveDataset("d", strings.ReplaceAll(specConsultavel, "%s", "d"))
	semeiaGarbage(t, a, "d", compKeys, compRodadas)

	cfg := a.carrega()
	dir = cfg.DatasetDir("d")
	antes = discoDosSegmentosDoDataset(t, dir)
	if antes == 0 {
		t.Fatal("nenhum segmento em disco depois da semeadura: não há o que compactar e as duas metades do teste passariam por vacuidade")
	}
	registro = &registroDoServidor{}
	srv, err := server.Novo(cfg, server.Opcoes{Log: logging.New(registro, logging.FormatText, slog.LevelInfo)})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv, dir, registro, antes
}

// discoDosSegmentosDoDataset é o `du` do operador sobre os segmentos: um
// os.Stat por arquivo, sem perguntar nada ao processo. É a medida que uma
// métrica errada não maquia.
func discoDosSegmentosDoDataset(t *testing.T, dir string) int64 {
	t.Helper()
	entradas, err := filepath.Glob(filepath.Join(dir, "data", "segment-*.dat"))
	if err != nil {
		t.Fatalf("glob dos segmentos: %v", err)
	}
	var total int64
	for _, p := range entradas {
		fi, err := os.Stat(p)
		if err != nil {
			// O arquivo pode ter sido aposentado ENTRE o glob e o stat: é
			// disco devolvido, não erro.
			continue
		}
		total += fi.Size()
	}
	return total
}

// TestCompactionSoSobeComAChaveDoOperadorLigada é o oráculo do ligaCompaction,
// que era mecanismo de produção NOVO sem cobertura nenhuma: trocar o
// `if !habilitada {` por `if false && !habilitada {` fazia o servidor subir a
// goroutine com a chave DESLIGADA no YAML — ignorando a decisão do operador — e
// deixava `go test ./...` inteiro verde.
//
// O teste tem duas metades e elas não são simétricas em papel:
//
//   - a metade LIGADA é o controle positivo, e é ela que CALIBRA a outra. Sem
//     ela, "o disco não encolheu com a chave desligada" seria compatível com um
//     dataset que nunca teve candidato, com um prazo curto demais ou com uma
//     semeadura que não produziu garbage — ou seja, verde por vacuidade;
//   - a metade DESLIGADA é o que a issue #1 cobra em outra chave: configuração
//     aceita e ignorada é configuração que mente. Ela afirma as duas coisas que
//     o operador tem direito a esperar — nada acontece com o disco, e ele é
//     AVISADO de que nada vai acontecer.
//
// O prazo da metade negativa sai da positiva, e não de um número escolhido: o
// que se dá à goroutine desligada é vinte vezes o tempo que a ligada levou para
// devolver metade do diretório, com piso de dois segundos.
func TestCompactionSoSobeComAChaveDoOperadorLigada(t *testing.T) {
	_, dirLigada, logLigada, antesLigada := bancadaDeCompaction(t, "true")

	comecou := time.Now()
	var depoisLigada int64
	prazo := time.Now().Add(60 * time.Second)
	for {
		depoisLigada = discoDosSegmentosDoDataset(t, dirLigada)
		if depoisLigada <= antesLigada/2 {
			break
		}
		if time.Now().After(prazo) {
			t.Fatalf("com storage.compaction.enabled: true o diretório saiu de %d para %d bytes em 60s: "+
				"a goroutine do §53-55 não subiu com o servidor, e a metade desligada deste teste não teria como distinguir nada\n%s",
				antesLigada, depoisLigada, logLigada.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	decorrido := time.Since(comecou)
	if !strings.Contains(logLigada.String(), "compaction.ligada") {
		t.Errorf("o servidor ligou a compaction sem registrar: o operador não tem como saber que ela está de pé\n%s", logLigada.String())
	}
	t.Logf("chave ligada: %d → %d bytes em %v", antesLigada, depoisLigada, decorrido)

	janela := 20 * decorrido
	if janela < 2*time.Second {
		janela = 2 * time.Second
	}

	_, dirDesligada, logDesligada, antesDesligada := bancadaDeCompaction(t, "false")
	time.Sleep(janela)
	depois := discoDosSegmentosDoDataset(t, dirDesligada)
	if depois < antesDesligada {
		t.Errorf("com storage.compaction.enabled: false o diretório caiu de %d para %d bytes em %v: "+
			"o servidor compactou contra a decisão do operador, e a chave que ele escreveu no YAML não decide nada",
			antesDesligada, depois, janela)
	}
	aposentados, err := filepath.Glob(filepath.Join(dirDesligada, "data", "*.retired"))
	if err != nil {
		t.Fatalf("glob dos aposentados: %v", err)
	}
	if len(aposentados) != 0 {
		t.Errorf("com a chave desligada apareceram %d arquivo(s) aposentado(s): %v — a goroutine subiu assim mesmo", len(aposentados), aposentados)
	}
	saida := logDesligada.String()
	if !strings.Contains(saida, "compaction.desligada") {
		t.Errorf("a chave desligada não produziu aviso: um processo que honra a decisão em SILÊNCIO é indistinguível de um que a ignora, "+
			"que é o defeito da issue #1 em outra chave\n%s", saida)
	}
	if strings.Contains(saida, "compaction.ligada") {
		t.Errorf("o servidor anunciou a compaction de pé com a chave desligada no YAML\n%s", saida)
	}
	t.Logf("chave desligada: %d → %d bytes após %v de espera", antesDesligada, depois, janela)
}
