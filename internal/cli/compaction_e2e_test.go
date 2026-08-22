package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/cli"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
)

// O critério de aceite da Task 7 do M4, pelo BINÁRIO.
//
// A Task 6 provou o MECANISMO: com as duas funções chamadas à mão sobre uma
// lista de alvos escolhida pelo teste, o disco volta e nenhuma row se perde.
// O que ela não podia provar é o PRODUTO, porque até a Task 7 não existia
// ninguém para disparar uma compaction — uma seleção que nunca elegesse nada,
// ou uma goroutine que nunca subisse, deixaria aquela suíte inteira verde.
//
// Aqui não há nada chamado à mão. Sobe-se o `nishidb` compilado com
// `storage.compaction.enabled: true`, joga-se ingestão contínua de updates
// sobre um key set FIXO — que é a forma do dataset do §86, 500 ev/s de updates
// sobre as mesmas sessões — e observa-se de FORA: tamanho de diretório pelo
// filesystem, séries pela porta HTTP, consulta pelo POST /v1/query, e os dois
// desfechos que um processo de verdade tem, o SIGTERM e o kill -9.

// e2eSpecYAML é o dataset da bancada. Os campos são poucos e diretos de
// propósito: o que este teste mede é ciclo de vida de ARQUIVO, e um schema
// rico só acrescentaria variáveis a um oráculo que precisa ser simples.
//
// `peso` existe para engordar o record. Sem ele cada evento teria algumas
// dezenas de bytes e seriam precisas centenas de milhares de mensagens para
// selar segmento — o teste mediria a vazão do kfake, não a compaction.
const e2eSpecYAML = `version: 1
name: sessoes
source:
  type: kafka
  kafka:
    enabled: true
    bootstrapServers:
      - %s
    topic: %s
    consumer:
      groupId: e2e-compaction
      autoOffsetReset: earliest
      maxPollRecords: 500
      ingestionQueueSize: 2000
materialization:
  key:
    field: id
  mode: merge
  sequence:
    field: seq
    type: int64
    strategy: greater
schema:
  include: ["*"]
  unknownFields: include
  fields:
    id:
      type: string
    seq:
      type: int64
    peso:
      type: string
query:
  maxRows: 100000
  timeout: 30s
`

// e2eConfigYAML é o nishidb.yaml do operador que LIGOU a compaction.
//
// O segmentSize é pequeno porque o §85 dimensiona 512 MiB para um disco de
// 50-100 GB, e reproduzir essa proporção custaria dezenas de gigabytes: o que
// importa para o mecanismo é haver uma PILHA de segmentos selados, não o
// tamanho de cada um. O checkpoint frequente tem o mesmo papel — é ele que
// move o segSize, e sem segSize a eleição do §53 não tem por onde dividir a
// liveness.
const e2eConfigYAML = `version: 1
storage:
  path: %s
  segmentSize: 256KiB
  checkpoint:
    interval: 500ms
  compaction:
    enabled: true
    minimumLiveRatio: 0.5
datasets:
  directory: %s
server:
  bind: 127.0.0.1
  port: %d
query:
  defaultMaxRows: 100000
  defaultTimeout: 30s
`

const (
	e2eDataset = "sessoes"
	e2eKeys    = 200
	// e2ePesoBytes deixa cada evento em torno de meio kilobyte, que com
	// segmentos de 256 KiB sela um arquivo a cada ~500 eventos.
	e2ePesoBytes = 400
	// e2eIntervaloDaRodada dá ao fluxo o ritmo do §86: 200 eventos a cada
	// 500ms são 400 ev/s, praticamente a carga que motiva este milestone. A
	// cadência também é o que faz o teste DURAR o suficiente para haver mais
	// de uma passada da compaction — despejar tudo de uma vez mediria a
	// vazão do kfake e devolveria o veredito antes de a goroutine acordar.
	e2eIntervaloDaRodada = 500 * time.Millisecond
)

type bancadaE2E struct {
	t        *testing.T
	raiz     string
	config   string
	dados    string
	porta    int
	binario  string
	broker   string
	topico   string
	produtor *kgo.Client

	cmd    *exec.Cmd
	saida  *bytes.Buffer
	morto  bool
	rodada int
}

// novaBancadaE2E monta o mundo do teste: um broker kfake de verdade (TCP, não
// dublê de interface), os dois YAMLs do produto e o binário compilado.
//
// O kfake é o mesmo broker em memória que a suíte do §105 usa na camada de
// unidade, e ele escuta numa porta real — é o que permite ao processo FILHO,
// que é o binário de produção sem nenhum seam de teste dentro, consumir de um
// tópico sem exigir daemon externo nem container.
func novaBancadaE2E(t *testing.T) *bancadaE2E {
	t.Helper()
	const topico = "e2e-compaction"
	cluster, err := kfake.NewCluster(
		kfake.SeedTopics(1, topico),
		kfake.NumBrokers(1),
		kfake.ClusterID("bancada-de-compaction"),
	)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	broker := cluster.ListenAddrs()[0]

	raiz := t.TempDir()
	dados := filepath.Join(raiz, "dados")
	specs := filepath.Join(raiz, "datasets")
	for _, d := range []string{dados, specs} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(specs, e2eDataset+".yaml"),
		[]byte(fmt.Sprintf(e2eSpecYAML, broker, topico)), 0o644); err != nil {
		t.Fatal(err)
	}
	porta := portaLivre(t)
	cfg := filepath.Join(raiz, "nishidb.yaml")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(e2eConfigYAML, dados, specs, porta)), 0o644); err != nil {
		t.Fatal(err)
	}

	binario := filepath.Join(raiz, "nishidb")
	build := exec.Command("go", "build", "-o", binario, "../../cmd/nishidb")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build do binário: %v\n%s", err, out)
	}

	produtor, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("produtor: %v", err)
	}
	t.Cleanup(produtor.Close)

	return &bancadaE2E{
		t: t, raiz: raiz, config: cfg, dados: dados, porta: porta,
		binario: binario, broker: broker, topico: topico, produtor: produtor,
	}
}

func (b *bancadaE2E) endereco() string { return "127.0.0.1:" + strconv.Itoa(b.porta) }

func (b *bancadaE2E) dirDoDataset() string { return filepath.Join(b.dados, e2eDataset) }

// sobe roda o binário como o operador o roda e espera o /v1/health responder.
//
// O stderr é capturado e devolvido em toda falha: um servidor que não sobe por
// causa da configuração explica o motivo lá, e sem isso o teste falharia
// dizendo apenas que a porta não respondeu.
func (b *bancadaE2E) sobe() {
	b.t.Helper()
	b.saida = &bytes.Buffer{}
	b.cmd = exec.Command(b.binario, "server", "--config", b.config, "--log-format", "texto")
	b.cmd.Stdout = b.saida
	b.cmd.Stderr = b.saida
	if err := b.cmd.Start(); err != nil {
		b.t.Fatalf("iniciar o servidor: %v", err)
	}
	b.t.Cleanup(func() {
		if b.cmd != nil && !b.morto {
			_ = b.cmd.Process.Kill()
			_ = b.cmd.Wait()
		}
	})

	prazo := time.Now().Add(30 * time.Second)
	for {
		resp, err := http.Get("http://" + b.endereco() + "/v1/health")
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		if time.Now().After(prazo) {
			b.t.Fatalf("o servidor não subiu em 30s: %v\n%s", err, b.saida.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// produzRodada publica um update de TODA key do corpus. O key set é fixo: a
// rodada seguinte torna garbage a anterior inteira, que é exatamente o dataset
// do §86 e a razão de o disco crescer 30-43 GB/dia contra 5,6-8 GB vivos.
func (b *bancadaE2E) produzRodada() int64 {
	b.t.Helper()
	b.rodada++
	peso := strings.Repeat("p", e2ePesoBytes)
	recs := make([]*kgo.Record, 0, e2eKeys)
	for i := 0; i < e2eKeys; i++ {
		id := fmt.Sprintf("s-%04d", i)
		valor := fmt.Sprintf(`{"id":%q,"seq":%d,"peso":%q}`, id, b.rodada, peso)
		recs = append(recs, &kgo.Record{Topic: b.topico, Key: []byte(id), Value: []byte(valor)})
	}
	ctx, cancela := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancela()
	if err := b.produtor.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		b.t.Fatalf("produzir a rodada %d: %v", b.rodada, err)
	}
	return int64(b.rodada)
}

// produzSemParar mantém a carga em voo até que o `parar` devolvido seja
// chamado, e ele espera a goroutine terminar antes de voltar.
//
// Existe porque os dois desfechos deste teste — o SIGTERM e o kill -9 — só
// significam alguma coisa com o processo TRABALHANDO. A compaction não roda o
// tempo todo: ela dorme cinco segundos entre passadas, e a janela em que há uma
// aposentadoria em voo é de dezenas de MILISSEGUNDOS. Esperar essa janela com a
// ingestão parada é esperar por um instante que talvez nunca volte a
// acontecer — sem eventos novos não há garbage novo, e sem garbage a eleição do
// §53 devolve lista vazia.
//
// Os erros de produção são engolidos DE PROPÓSITO: quem chama já não é a
// goroutine do teste (t.Fatalf de fora dela é uso indevido do testing), e o que
// interessa aqui é manter pressão, não auditar cada lote. A falha de verdade
// aparece no oráculo — o processo que não encerra, a row que não volta.
func (b *bancadaE2E) produzSemParar() (parar func()) {
	pedido := make(chan struct{})
	fim := make(chan struct{})
	peso := strings.Repeat("p", e2ePesoBytes)
	go func() {
		defer close(fim)
		for {
			select {
			case <-pedido:
				return
			default:
			}
			b.rodada++
			recs := make([]*kgo.Record, 0, e2eKeys)
			for i := 0; i < e2eKeys; i++ {
				id := fmt.Sprintf("s-%04d", i)
				recs = append(recs, &kgo.Record{
					Topic: b.topico, Key: []byte(id),
					Value: []byte(fmt.Sprintf(`{"id":%q,"seq":%d,"peso":%q}`, id, b.rodada, peso)),
				})
			}
			ctx, cancela := context.WithTimeout(context.Background(), 30*time.Second)
			_ = b.produtor.ProduceSync(ctx, recs...).FirstErr()
			cancela()
		}
	}()
	return sync.OnceFunc(func() {
		close(pedido)
		<-fim
	})
}

// bytesProduzidos é o volume que a ingestão já aplicou, medido pelo que o
// teste publicou. É o número contra o qual "o disco parou de crescer" só
// significa alguma coisa: num storage append-only sem compaction, o diretório
// cresce com ELE, byte a byte.
//
// O custo por record é um LIMITE INFERIOR DERIVADO DO PRODUTO, e a distinção
// importa porque esta é a única grandeza contra a qual o plateau é medido: o
// encoding.RecordHeaderSize é o header fixo que todo record carrega em disco
// (record.go), e o peso do evento entra INTEIRO no payload — o wire format do
// §31-33 grava a string como bytes, sem compressão. O record real desta bancada
// fica acima disso, porque sobram a key, os outros dois campos do evento e o
// framing de cada um (FIELD_ID + TYPE + LENGTH). Aqui esteve, até a revisão
// desta tarefa, um "+ 40" escolhido sem oráculo — número inventado embutido no
// denominador de duas asserções.
//
// Subestimar é a direção SEGURA, e é por isso que o limite inferior basta: as
// duas asserções do plateau dividem por este número (o piso não pode subir mais
// que `escritoNaSegundaMetade/4`, o pico não pode alcançar `produzido/3`), de
// modo que um produzido menor que o real APERTA as duas. O erro para baixo
// reprova compaction boa; ele nunca aprova compaction que não devolveu disco.
func (b *bancadaE2E) bytesProduzidos() int64 {
	return int64(b.rodada) * e2eKeys * (e2ePesoBytes + encoding.RecordHeaderSize)
}

// esperaIngestaoAlcancar bloqueia até TODA key responder com sequence >= alvo.
//
// Sem isto, cada medida de disco compararia um diretório que ainda não
// absorveu o que foi produzido — o teste mediria o lag do consumidor e
// chamaria o resultado de plateau.
func (b *bancadaE2E) esperaIngestaoAlcancar(alvo int64, prazo time.Duration) map[string]int64 {
	b.t.Helper()
	limite := time.Now().Add(prazo)
	for {
		linhas := b.consulta("SELECT id, seq FROM " + e2eDataset)
		if len(linhas) == e2eKeys {
			menor := int64(-1)
			for _, seq := range linhas {
				if menor < 0 || seq < menor {
					menor = seq
				}
			}
			if menor >= alvo {
				return linhas
			}
		}
		if time.Now().After(limite) {
			b.t.Fatalf("a ingestão não alcançou a rodada %d em %v (%d de %d keys materializadas)\n%s",
				alvo, prazo, len(linhas), e2eKeys, b.saida.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// consulta faz o POST /v1/query do §68 pelo CLIENTE DO PRODUTO — o mesmo
// `nishidb query --server` que o operador digita. Devolve id → seq.
//
// É consulta REMOTA de propósito: o processo de pé segura o flock, então esta
// é a única forma de ler o dataset enquanto a compaction reescreve locations
// por baixo. E ela é o item 4 do critério: a promessa do §51 é que a consulta
// responde o certo durante a compaction, não apesar dela.
func (b *bancadaE2E) consulta(sql string) map[string]int64 {
	b.t.Helper()
	var saida, erro bytes.Buffer
	code := cli.Run([]string{"query", "--server", b.endereco(), "--dataset", e2eDataset, sql},
		cli.Ambiente{Stdin: strings.NewReader(""), Stdout: &saida, Stderr: &erro, Ctx: context.Background()})
	if code != 0 {
		b.t.Fatalf("a consulta remota %q saiu com %d: %s", sql, code, erro.String())
	}
	return decodificaLinhas(b.t, saida.String())
}

func decodificaLinhas(t *testing.T, ndjson string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	texto := strings.TrimSpace(ndjson)
	if texto == "" {
		return out
	}
	for i, l := range strings.Split(texto, "\n") {
		var row struct {
			ID  string `json:"id"`
			Seq int64  `json:"seq"`
		}
		if err := json.Unmarshal([]byte(l), &row); err != nil {
			t.Fatalf("linha %d não é JSON válido: %v — %q", i+1, err, l)
		}
		if _, repetida := out[row.ID]; repetida {
			t.Fatalf("a key %q apareceu duas vezes no resultado: a compaction duplicou a row", row.ID)
		}
		out[row.ID] = row.Seq
	}
	return out
}

// metrica raspa o /metrics pela porta HTTP e devolve o valor da série do
// dataset. É a leitura do OPERADOR — nenhum campo Go, nenhum atalho por dentro
// do processo, que é o que o item 3 do critério exige.
func (b *bancadaE2E) metrica(nome string) float64 {
	b.t.Helper()
	resp, err := http.Get("http://" + b.endereco() + "/metrics")
	if err != nil {
		b.t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	corpo, err := io.ReadAll(resp.Body)
	if err != nil {
		b.t.Fatalf("ler /metrics: %v", err)
	}
	prefixo := nome + `{dataset="` + e2eDataset + `"} `
	for _, linha := range strings.Split(string(corpo), "\n") {
		if !strings.HasPrefix(linha, prefixo) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(linha, prefixo)), 64)
		if err != nil {
			b.t.Fatalf("a série %s saiu com valor ilegível %q: %v", nome, linha, err)
		}
		return v
	}
	b.t.Fatalf("a série %s não foi publicada para o dataset %q:\n%s", nome, e2eDataset, corpo)
	return 0
}

// discoDosSegmentos é o `du` do operador sobre os segmentos: um os.Stat por
// arquivo, sem perguntar nada ao processo. É a medida que responde à pergunta
// do §86 — o volume para de encher? — e a única que uma métrica errada não
// consegue maquiar.
func (b *bancadaE2E) discoDosSegmentos() int64 {
	b.t.Helper()
	entradas, err := filepath.Glob(filepath.Join(b.dirDoDataset(), "data", "segment-*.dat"))
	if err != nil {
		b.t.Fatalf("glob dos segmentos: %v", err)
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

// exigeSemAposentadoEmDisco é a reprovação do D5 que a Task 6 já fixava e que
// aqui vale sobre o produto: um `.retired` sobrevivente significa que o ciclo
// parou no meio e que os bytes ficaram presos num arquivo que ninguém lista.
func (b *bancadaE2E) exigeSemAposentadoEmDisco(quando string) {
	b.t.Helper()
	sobreviventes := b.aposentadosEmDisco()
	if len(sobreviventes) != 0 {
		b.t.Errorf("%s: sobraram %d arquivo(s) aposentado(s): %v", quando, len(sobreviventes), sobreviventes)
	}
}

// esperaJanelaDaAposentadoria bloqueia até um `.retired` existir em disco e
// devolve o caminho dele. É o único instrumento deste teste que olha para
// dentro do diretório em vez de perguntar ao processo, e ele existe porque a
// janela é curta demais para ser acertada por sorteio: entre o rename e o
// unlink corre um checkpoint, alguns milissegundos, contra os cinco segundos
// de cochilo entre duas passadas.
//
// A volta é um readdir e um cochilo curto. O cochilo não é cerimônia: sem ele
// o laço queima um core inteiro em readdir sobre o MESMO diretório em que o
// processo medido está renomeando e apagando arquivo — e é este laço que decide
// o instante do SIGTERM e do kill -9, ou seja a bancada estaria empilhando
// pressão de I/O sobre o alvo no exato instante que ela pretende observar.
//
// O que se garante é a RESOLUÇÃO da amostragem, e não "não perturbar o processo
// medido", que é o que este godoc dizia sem ter medido: um milissegundo fica de
// 40 a 65 VEZES abaixo dos 40 a 65 ms que uma passada de aposentadoria dura
// nesta bancada, de modo que o cochilo não estreita a janela que o laço existe
// para apanhar. São pouco menos de duas ordens de grandeza, e não as duas que
// esta frase afirmava — duas seriam 100x, e o número aqui é o que foi medido.
func (b *bancadaE2E) esperaJanelaDaAposentadoria(prazo time.Duration) string {
	b.t.Helper()
	limite := time.Now().Add(prazo)
	for {
		if achados := b.aposentadosEmDisco(); len(achados) > 0 {
			return achados[0]
		}
		if time.Now().After(limite) {
			b.t.Fatalf("nenhuma aposentadoria em voo em %v: a compaction parou de eleger candidatos e o kill -9 cairia num processo ocioso\n%s",
				prazo, b.saida.String())
		}
		time.Sleep(e2eResolucaoDaJanela)
	}
}

// e2eResolucaoDaJanela é o passo de amostragem do laço acima. Não é chave de
// nada: é o intervalo entre dois readdir, escolhido para ficar muito abaixo da
// janela observada e muito acima do custo de um syscall.
const e2eResolucaoDaJanela = time.Millisecond

func (b *bancadaE2E) aposentadosEmDisco() []string {
	achados, err := filepath.Glob(filepath.Join(b.dirDoDataset(), "data", "*.retired"))
	if err != nil {
		b.t.Fatalf("glob dos aposentados: %v", err)
	}
	return achados
}

// consultaLocal reabre o dataset pelo binário, com o processo já fora do ar.
// É o Open com recovery de verdade — e é ele que recusaria o dataset se a
// compaction tivesse deixado location apontando para arquivo removido.
func (b *bancadaE2E) consultaLocal() map[string]int64 {
	b.t.Helper()
	var saida, erro bytes.Buffer
	code := cli.Run([]string{"query", "--config", b.config, "--dataset", e2eDataset,
		"SELECT id, seq FROM " + e2eDataset},
		cli.Ambiente{Stdin: strings.NewReader(""), Stdout: &saida, Stderr: &erro, Ctx: context.Background()})
	if code != 0 {
		b.t.Fatalf("a consulta após a reabertura saiu com %d: %s", code, erro.String())
	}
	return decodificaLinhas(b.t, saida.String())
}

// extremos devolve o menor e o maior de uma série. O par é o instrumento do
// item 2: o menor é o piso do regime, o maior é o pior instante dele.
func extremos(v []int64) (piso, pico int64) {
	piso, pico = v[0], v[0]
	for _, x := range v[1:] {
		if x < piso {
			piso = x
		}
		if x > pico {
			pico = x
		}
	}
	return piso, pico
}

// TestCompactionPeloBinarioEstabilizaODiscoComOOperadorVendo é o critério de
// aceite da Task 7, itens 1 a 5, e — desde a Task 8 — a metade do item 3 que a
// errata do orquestrador adiou.
//
// O que ele afirma, e por que cada asserção não é substituível pela anterior:
//
//   - o DISCO para de crescer (item 2), medido no filesystem e comparado ao
//     volume produzido. Num append-only sem compaction os dois crescem juntos,
//     então a distância entre eles é a única prova de que algo devolveu espaço;
//   - o OPERADOR vê (item 3): nishidb_storage_bytes CAI numa raspagem de
//     /metrics. Uma compaction que devolvesse disco sem a série se mover
//     deixaria o painel do M4 inútil, que é o que este milestone existe para
//     evitar;
//   - e ele vê PELA SÉRIE DA COMPACTION, que é o critério da Task 8 cobrado
//     aqui: nishidb_compaction_bytes_written SOBE no /metrics enquanto a carga
//     corre, e nishidb_compaction_running vale 1 com o laço de pé. Até a Task 8
//     estas três eram nome declarado em nomes.go sem número atrás dele, e os
//     únicos números da compaction viviam no compaction.segmento_aposentado,
//     ou seja no log — que responde a pergunta do §86 para quem está lendo o
//     log àquela hora, e não para um painel. A distinção entre as duas séries
//     também importa: a de disco cai quando ALGUÉM devolve espaço, e um
//     `checkpoint` que relistasse o diretório a moveria sem compaction nenhuma;
//     a de bytes escritos só se move se o copy-forward do §55 tiver de fato
//     copiado record vivo para frente;
//   - a CONSULTA continua certa durante tudo isso (item 4): toda key presente,
//     nenhuma duas vezes, e nenhuma sequence REGREDINDO entre duas leituras —
//     que é exatamente o estrago que um copy-forward mal sincronizado produz,
//     publicando o payload antigo por cima do novo;
//   - o SIGTERM encerra limpo (item 5), sem `.retired` e com o Open seguinte
//     aceitando o dataset.
func TestCompactionPeloBinarioEstabilizaODiscoComOOperadorVendo(t *testing.T) {
	if testing.Short() {
		t.Skip("compila o binário, sobe um broker e roda uma carga de dezenas de segundos")
	}
	b := novaBancadaE2E(t)
	b.sobe()

	const rodadas = 60
	metade := rodadas / 2
	amostras := make([]int64, 0, rodadas)
	var quedaNaSerie bool
	serieAnterior := b.metrica(metrics.MetStorageBytes)
	picoDaSerie := serieAnterior
	// A série de bytes escritos é CONTADOR: a largada é lida antes da primeira
	// rodada justamente para que a asserção seja sobre o MOVIMENTO dela e não
	// sobre o valor absoluto — um processo reiniciado começa do zero, e o que o
	// operador acompanha no painel é a taxa.
	escritosNaLargada := b.metrica(metrics.MetCompactionBytesWritten)
	escritosAgora := escritosNaLargada
	var primeiraLeitura, ultimaLeitura map[string]int64
	var produzidoAteAMetade int64

	for r := 1; r <= rodadas; r++ {
		alvo := b.produzRodada()
		time.Sleep(e2eIntervaloDaRodada)
		linhas := b.esperaIngestaoAlcancar(alvo, 60*time.Second)

		serie := b.metrica(metrics.MetStorageBytes)
		escritosAgora = b.metrica(metrics.MetCompactionBytesWritten)
		if serie < serieAnterior {
			quedaNaSerie = true
		}
		if serie > picoDaSerie {
			picoDaSerie = serie
		}
		serieAnterior = serie
		amostras = append(amostras, b.discoDosSegmentos())

		switch r {
		case 1:
			primeiraLeitura = linhas
		case metade:
			produzidoAteAMetade = b.bytesProduzidos()
		case rodadas:
			ultimaLeitura = linhas
		}
	}

	produzido := b.bytesProduzidos()
	escritoNaSegundaMetade := produzido - produzidoAteAMetade
	pisoA, picoA := extremos(amostras[:metade])
	pisoB, picoB := extremos(amostras[metade:])
	t.Logf("disco em segment-*.dat, rodada a rodada: %v", amostras)
	t.Logf("primeira metade: piso %d, pico %d — segunda metade: piso %d, pico %d", pisoA, picoA, pisoB, picoB)
	t.Logf("volume produzido: %d bytes no total, %d só na segunda metade", produzido, escritoNaSegundaMetade)
	t.Logf("nishidb_storage_bytes: pico de %.0f pela porta HTTP, com queda observada = %v", picoDaSerie, quedaNaSerie)
	t.Logf("nishidb_compaction_bytes_written: %.0f na largada, %.0f na última raspagem", escritosNaLargada, escritosAgora)

	// Item 2, e a medida é o PISO da serra, não uma amostra solta.
	//
	// O disco em regime não é uma linha: ele sobe enquanto a ingestão apenda e
	// desce quando a passada aposenta, com amplitude igual ao que se escreve
	// entre duas passadas. Uma amostra única no fim mede a FASE em que ela
	// caiu, e não o regime — a mesma corrida devolve veredito diferente
	// conforme o instante. O que "parou de crescer" quer dizer é que o piço
	// para onde a serra volta não sobe, e é isso que se compara: contra o
	// VOLUME ESCRITO no trecho, porque num append-only sem compaction o piso
	// acompanha esse volume byte a byte.
	if pisoB-pisoA >= escritoNaSegundaMetade/4 {
		t.Errorf("o piso do diretório subiu %d bytes (de %d para %d) enquanto %d eram escritos: o disco continua acompanhando o volume, "+
			"que é o crescimento perpétuo que o §86 projeta em 2 a 3 dias até o ENOSPC",
			pisoB-pisoA, pisoA, pisoB, escritoNaSegundaMetade)
	}
	// E o teto do regime é absoluto: mesmo no PIOR instante da segunda metade
	// — a serra no alto, antes da passada — o diretório guarda uma fração do
	// que já foi escrito. Sem esta, um piso estável com picos crescentes
	// passaria verde, e picos crescentes são ENOSPC igual.
	if picoB >= produzido/3 {
		t.Errorf("no pior instante da segunda metade o diretório tinha %d bytes para %d produzidos sobre %d keys: "+
			"a maior parte disso é garbage e deveria ter voltado", picoB, produzido, e2eKeys)
	}
	// Item 3, primeira metade: a série do §92 acompanha o filesystem. Sem
	// isto o operador só descobriria a compaction pelo `du`.
	if !quedaNaSerie {
		t.Errorf("nishidb_storage_bytes nunca caiu em %d raspagens de /metrics (pico %.0f): "+
			"o disco voltou e o painel do M4 não mostrou nada", rodadas, picoDaSerie)
	}
	// Item 3, a metade que a errata passou para a Task 8, e ela não é a
	// anterior dita de outro jeito: a série de disco cai quando o diretório
	// encolhe, seja lá quem o tenha encolhido, enquanto esta só se move quando
	// o copy-forward do §55 copiou record VIVO para frente. É o único número
	// pela porta que diz que a compaction TRABALHOU, e não apenas que sobrou
	// menos arquivo.
	if escritosAgora <= escritosNaLargada {
		t.Errorf("nishidb_compaction_bytes_written saiu de %.0f para %.0f em %d raspagens de /metrics, com o disco em serra "+
			"(piso %d→%d) e %d bytes produzidos: o operador vê o diretório encolher e não tem série que diga que foi a compaction",
			escritosNaLargada, escritosAgora, rodadas, pisoA, pisoB, produzido)
	}
	// E o medidor do §92 diz que o laço está de pé. Com a chave ligada no YAML
	// e o processo respondendo, 0 aqui é a compaction que morreu sem avisar —
	// o compaction.parou existe para o log, este número existe para o alerta.
	if rodando := b.metrica(metrics.MetCompactionRunning); rodando != 1 {
		t.Errorf("nishidb_compaction_running = %.0f com storage.compaction.enabled: true e o servidor de pé, queria 1", rodando)
	}

	// Item 4. A sequence é `greater` (§25): ela NUNCA regride numa
	// materialização correta. Uma cópia do payload antigo publicada por cima
	// do novo aparece aqui, e só aqui — o record copiado é durável e tem CRC
	// válido, então nenhuma outra camada o distingue de uma escrita legítima.
	for id, antes := range primeiraLeitura {
		depois, presente := ultimaLeitura[id]
		if !presente {
			t.Fatalf("a key %q sumiu do resultado durante a compaction", id)
		}
		if depois < antes {
			t.Errorf("a key %q voltou no tempo durante a compaction: seq %d na primeira leitura, %d na última", id, antes, depois)
		}
	}
	if len(ultimaLeitura) != e2eKeys {
		t.Fatalf("a consulta durante a compaction devolveu %d rows, queria %d", len(ultimaLeitura), e2eKeys)
	}

	// Item 5, e o item diz "SIGTERM DURANTE uma compaction" — não "SIGTERM
	// depois de uma corrida em que houve compaction". A diferença é tudo o que
	// este trecho afirma: com o sinal disparado num instante arbitrário, o que
	// se exercita é o encerramento de um processo OCIOSO. A medida, feita nesta
	// bancada: as aposentadorias desta corrida se agrupam em seis passadas de
	// 40 a 65 MILISSEGUNDOS dentro de uma janela de 25 segundos, ou seja pouco
	// mais de 1% do tempo com compaction em voo — sortear o instante é acertar
	// o cochilo em 99 de cada 100 corridas.
	//
	// Então o instante não é sorteado, e o instrumento é o mesmo que o kill -9
	// já usava: espera-se o `.retired` APARECER no diretório, com a carga
	// mantida em voo para que haja garbage novo a eleger, e o sinal vai embora
	// ali. O que o Close tem de fazer nesse ponto é a decisão D7 — cancelar a
	// goroutine e AGUARDÁ-LA, em vez de esperar a passada inteira ou de
	// desmapear o índice sob os pés de quem está renomeando arquivo.
	pararCarga := b.produzSemParar()
	apanhado := b.esperaJanelaDaAposentadoria(90 * time.Second)
	t.Logf("SIGTERM disparado com %s em disco: a compaction está no meio de uma passada, não ociosa", filepath.Base(apanhado))
	if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		pararCarga()
		t.Fatalf("SIGTERM: %v", err)
	}
	esperado := make(chan error, 1)
	go func() { esperado <- b.cmd.Wait() }()
	select {
	case err := <-esperado:
		b.morto = true
		pararCarga()
		if err != nil {
			t.Fatalf("o servidor saiu com erro após o SIGTERM: %v\n%s", err, b.saida.String())
		}
	case <-time.After(60 * time.Second):
		pararCarga()
		t.Fatalf("o servidor não encerrou em 60s após o SIGTERM: o Close ficou preso atrás da compaction\n%s", b.saida.String())
	}
	b.exigeSemAposentadoEmDisco("após o SIGTERM")
	// O laço é FALHO STICKY: a primeira falha o encerra e ele não volta
	// sozinho. Sem esta asserção, uma compaction que morresse no meio da
	// corrida ainda deixaria o plateau verde — o garbage já recolhido não
	// volta a crescer no que resta do teste —, e o produto teria parado de
	// devolver disco sem ninguém notar.
	if strings.Contains(b.saida.String(), "compaction.parou") {
		t.Errorf("a compaction parou durante a corrida e não voltou:\n%s", b.saida.String())
	}

	// E o dataset reabre. É aqui que uma location pendurada num arquivo
	// removido apareceria: enquanto o índice está em memória ela não machuca
	// ninguém.
	reaberto := b.consultaLocal()
	if len(reaberto) != e2eKeys {
		t.Fatalf("após a reabertura o dataset devolveu %d rows, queria %d", len(reaberto), e2eKeys)
	}
	for id, seq := range ultimaLeitura {
		depois, presente := reaberto[id]
		if !presente {
			t.Errorf("a key %q sumiu na reabertura", id)
			continue
		}
		if depois < seq {
			t.Errorf("a key %q reabriu com seq %d, atrás dos %d que o servidor já respondia antes do SIGTERM", id, depois, seq)
		}
	}
	b.exigeSemAposentadoEmDisco("após a reabertura")
}

// TestKill9DuranteACompactionNoProdutoPreservaTodasAsRows é o item 6, e é o
// item que a Task 6 não podia cumprir: até a Task 7 nada no produto disparava
// uma compaction, então não havia janela a acertar.
//
// A morte é por SIGKILL: sem defer, sem Close, sem checkpoint final, com o
// kernel decidindo o que fez do writeback. O processo é morto DEPOIS de o
// /metrics já ter mostrado a série de disco cair — ou seja, com a compaction
// comprovadamente ativa e em ciclo, e não num processo que por acaso nunca
// compactou nada.
//
// O que se exige na volta é o §126.25 aplicado ao ciclo que APAGA arquivo:
// nenhum `.retired` sobrevivente, o Open reparando sem erro, e TODA row do
// corpus de volta — não uma amostra. O modo de falha desta fase é a row
// isolada que ficou apontando para um arquivo removido, e um sorteio a perde.
func TestKill9DuranteACompactionNoProdutoPreservaTodasAsRows(t *testing.T) {
	if testing.Short() {
		t.Skip("compila o binário, sobe um broker e mata um processo")
	}
	b := novaBancadaE2E(t)
	b.sobe()

	// Carga até a compaction estar comprovadamente em ciclo: a série tem de
	// CAIR pela porta HTTP. Matar o processo antes disso exercitaria um
	// dataset que só cresceu, e o teste passaria por vacuidade.
	serieAnterior := b.metrica(metrics.MetStorageBytes)
	var compactou bool
	var ultimaLeitura map[string]int64
	for r := 1; r <= 60 && !compactou; r++ {
		alvo := b.produzRodada()
		time.Sleep(e2eIntervaloDaRodada)
		ultimaLeitura = b.esperaIngestaoAlcancar(alvo, 60*time.Second)
		serie := b.metrica(metrics.MetStorageBytes)
		compactou = serie < serieAnterior
		serieAnterior = serie
	}
	if !compactou {
		t.Fatalf("nishidb_storage_bytes nunca caiu: a compaction não entrou em ciclo e não há o que este kill -9 exercite\n%s",
			b.saida.String())
	}
	if len(ultimaLeitura) != e2eKeys {
		t.Fatalf("a última leitura remota trouxe %d rows, queria %d: sem ela não há piso de sequence contra o qual medir o recovery",
			len(ultimaLeitura), e2eKeys)
	}

	// O estado CONFIRMADO, lido do manifest antes do sinal. É contra ele que
	// a recovery é medida: o kill -9 pode custar o que ainda não foi
	// checkpointado, nunca o que já foi.
	man := filepath.Join(b.dirDoDataset(), "checkpoint", "manifest.bin")
	m, err := checkpoint.Load(man)
	if err != nil {
		t.Fatalf("carregar o manifest antes do kill -9: %v", err)
	}
	confirmadas := m.RowCount
	if confirmadas == 0 {
		t.Fatal("nenhum checkpoint com rows antes do kill -9")
	}

	// A carga CONTINUA durante a morte: o processo é morto com o consumidor
	// aplicando eventos e a compaction em ciclo, que é a situação em que a
	// aposentadoria pode estar no meio de qualquer um dos quatro passos do D5.
	pararCarga := b.produzSemParar()

	// O instante da morte não é sorteado: o teste espera o `.retired` APARECER
	// no diretório e mata o processo ali. É a janela mais estreita e mais cara
	// do D5 — o arquivo já perdeu o nome canônico e o manifest vigente ainda o
	// nomeia, de modo que um Open que não reparasse encontraria um segmento
	// LISTADO e AUSENTE e recusaria o dataset inteiro (store_recovery.go).
	// Matar num instante qualquer quase sempre acerta o cochilo entre duas
	// passadas, e exercitaria um processo parado.
	apanhado := b.esperaJanelaDaAposentadoria(90 * time.Second)
	if err := b.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		pararCarga()
		t.Fatalf("SIGKILL: %v", err)
	}
	estado, err := b.cmd.Process.Wait()
	pararCarga()
	b.morto = true
	if err != nil {
		t.Fatalf("aguardar o processo morto: %v", err)
	}
	if estado.ExitCode() != -1 {
		t.Fatalf("o processo saiu com %d em vez de morrer pelo sinal", estado.ExitCode())
	}
	t.Logf("kill -9 disparado com %s em disco; sobreviventes ao sinal: %v",
		filepath.Base(apanhado), b.aposentadosEmDisco())

	// O reparo do D5 é do Open, e ele acontece na reabertura abaixo: se o
	// SIGKILL parou entre o rename e o checkpoint, o `.retired` é RESTAURADO;
	// se parou entre o checkpoint e o unlink, é removido como órfão. Em
	// nenhum dos dois o dataset pode ser recusado, e em nenhum dos dois pode
	// sobrar arquivo aposentado depois.
	rows := b.consultaLocal()
	b.exigeSemAposentadoEmDisco("após o kill -9 e a reabertura")
	t.Logf("kill -9 com %d rows confirmadas no manifest; %d rows de volta após o recovery", confirmadas, len(rows))
	if uint64(len(rows)) < confirmadas {
		t.Errorf("%d rows após o kill -9, mas o checkpoint já confirmava %d", len(rows), confirmadas)
	}
	// A conferência é por KEY e com PISO, e o piso é o que faz dela um oráculo.
	//
	// `presente` mais `seq > 0` diz apenas que alguma coisa voltou com aquele
	// nome: uma row que reaparecesse com a sequence de dezenas de rodadas atrás
	// passaria verde — e é exatamente esse o estrago de um copy-forward mal
	// sincronizado que sobrevive ao SIGKILL, porque o record antigo copiado é
	// durável, tem CRC válido e nenhuma outra camada o distingue de uma escrita
	// legítima.
	//
	// O piso é a ÚLTIMA leitura remota, e ele é conservador por construção: o
	// servidor já respondia aquele valor pela porta HTTP, o que só acontece
	// depois de o record estar no segmento, e o SIGKILL não descarta page cache
	// (o mesmo argumento que o godoc do durabilizaODiretorio registra). A carga
	// seguiu correndo depois dessa leitura, de modo que o piso fica ATRÁS do
	// estado real no instante da morte — o que ele proíbe é a regressão, não o
	// que o kill -9 legitimamente custa em rodadas não checkpointadas.
	menor, maior := int64(-1), int64(-1)
	for i := 0; i < e2eKeys; i++ {
		id := fmt.Sprintf("s-%04d", i)
		seq, presente := rows[id]
		if !presente {
			t.Errorf("a key %q não voltou do recovery: location pendurada num arquivo que a aposentadoria removeu", id)
			continue
		}
		if piso := ultimaLeitura[id]; seq < piso {
			t.Errorf("a key %q voltou do recovery com seq %d, atrás dos %d que o servidor já respondia pela porta HTTP: "+
				"o dataset regrediu no kill -9, que é o payload antigo do copy-forward publicado por cima do novo", id, seq, piso)
			continue
		}
		if menor < 0 || seq < menor {
			menor = seq
		}
		if seq > maior {
			maior = seq
		}
	}
	if menor < 0 {
		t.Fatal("nenhuma key voltou do recovery")
	}
	// A BANDA é o oráculo fino, e ela existe porque o piso acima é grosso: a
	// carga segue em voo depois da última leitura remota, então aquele piso fica
	// centenas de rodadas atrás do estado real no instante da morte, e uma row
	// que voltasse com a sequence de DEZENAS de rodadas atrás — que é
	// exatamente o estrago de um copy-forward mal sincronizado sobrevivendo ao
	// SIGKILL — passaria por cima dele.
	//
	// A banda não depende de saber qual era a rodada corrente. A carga publica
	// TODAS as keys a cada rodada, o consumidor as aplica em ordem, e o corte da
	// recovery é UM ponto do fluxo de append: no máximo uma rodada fica partida
	// entre as keys que entraram antes do corte e as que não entraram. Duas
	// rodadas de folga cobrem esse corte com margem; dezenas não são corte, são
	// record antigo ressuscitado.
	const bandaDoRecovery = 2
	if maior-menor > bandaDoRecovery {
		t.Errorf("as sequences do recovery se espalham por %d rodadas (de %d a %d) nas %d keys: a carga atualiza todas elas a cada "+
			"rodada, então essa dispersão não é o corte do kill -9 — é row voltando de um record que a compaction copiou do passado",
			maior-menor, menor, maior, e2eKeys)
	}
	t.Logf("recovery: sequences de %d a %d (banda de %d rodada(s)), todas acima do piso da última leitura remota",
		menor, maior, maior-menor)
}
