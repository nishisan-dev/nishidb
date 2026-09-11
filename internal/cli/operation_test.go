package cli_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// Os três comandos de leitura do §96 — `status`, `datasets` e `inspect` — nascem
// REMOTOS, e é isso que esta suíte afere.
//
// O argumento é o mesmo do débito 54, que a consulta remota fechou do lado da
// query: o flock do dataset é exclusivo por processo, então um comando de
// diagnóstico que só soubesse abrir o dataset local exigiria DERRUBAR o
// servidor para perguntar o estado dele — a pergunta e a resposta se
// excluiriam. Um `status` que só responde com o produto parado não é status.
//
// Os oráculos daqui comparam a saída do comando com o que as rotas respondem
// pela porta, e não com números escritos no teste: o comando é uma VISTA do
// /v1/stats e do /v1/metadata, e o que precisa ser provado é que ele mostra o
// que o servidor disse, não que ele imprime algo plausível.

// fieldOfSaida devolve o valor da line rotulada, no formato de duas colunas
// que os comandos usam.
//
// A separação é por DOIS espaços porque é o que o padding do tabwriter
// garante: rótulo com espaço simples dentro ("carga do primário") continua
// inteiro, e o corte cai onde a coluna de fato começa.
func fieldOfSaida(t *testing.T, saida, rotulo string) string {
	t.Helper()
	for _, l := range strings.Split(saida, "\n") {
		l = strings.TrimSpace(l)
		i := strings.Index(l, "  ")
		if i < 0 {
			continue
		}
		if strings.TrimSpace(l[:i]) == rotulo {
			return strings.TrimSpace(l[i:])
		}
	}
	t.Fatalf("a saída não tem o campo %q:\n%s", rotulo, saida)
	return ""
}

// celulasOfRow corta a line nas colunas em que o tabwriter a escreveu.
//
// O corte é pelos DOIS espaços do padding, e não por espaço simples, porque a
// CÉLULA é a unidade de quem confere valor posicional: `strings.Contains` na
// line inteira não distingue "30s" na coluna certa de "30s" em qualquer outra,
// e é cego para duas colunas trocadas de lugar.
//
// O argumento está escrito aqui desde a primeira rodada do §96, e valia só para
// a ÚLTIMA coluna de cada tabela — o resto da conferência era Contains na line
// inteira. O buraco tinha tamanho: dois mutantes de troca de coluna
// (COLUNAS↔MAXROWS no `datasets`, ALIAS↔TIPO no `inspect`) atravessavam a suíte
// inteira sem acender nada. Agora o corte alimenta a tabela toda, pelo
// tableOfSaida.
func celulasOfRow(l string) []string {
	var out []string
	for _, c := range strings.Split(strings.TrimSpace(l), "  ") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}

// tableOfSaida devolve o CABEÇALHO e as linhas de dados da tabela que começa
// no rótulo dado, já cortados em células.
//
// O cabeçalho volta junto porque é dele que sai a POSIÇÃO de cada rótulo, e é
// isso que separa duas coisas que um índice fixo no teste confundiria: mover
// uma coluna de lugar DE PROPÓSITO — cabeçalho e valores juntos — continua
// legítimo e não pode reprovar; deixar o valor sob o rótulo do vizinho é o
// defeito. O oráculo destes comandos é "o número está sob o rótulo dele", que é
// exatamente a pergunta que a suíte não sabia fazer.
//
// A tabela termina na primeira line em branco ou no fim da saída. Nos dois
// comandos ela é hoje o último bloco; parar no branco é o que impede que uma
// seção acrescentada depois escorra para dentro das linhas e passe por dado.
func tableOfSaida(t *testing.T, saida, primeiroRotulo string) (cabecalho []string, rows [][]string) {
	t.Helper()
	todas := strings.Split(saida, "\n")
	for i, l := range todas {
		c := celulasOfRow(l)
		if len(c) == 0 || c[0] != primeiroRotulo {
			continue
		}
		for _, resto := range todas[i+1:] {
			if strings.TrimSpace(resto) == "" {
				break
			}
			rows = append(rows, celulasOfRow(resto))
		}
		return c, rows
	}
	t.Fatalf("a saída não tem a tabela que começa em %q:\n%s", primeiroRotulo, saida)
	return nil, nil
}

// rowOfTable devolve a line cuja PRIMEIRA célula é a chave pedida — é como
// se lê uma tabela de colunas, no `datasets` e no `inspect`.
//
// Morrer aqui é a forma que o SUMIÇO de uma line tem de aparecer. Um comando
// que escrevesse o nome do primeiro dataset em toda line duplicaria o primeiro
// e faria os demais desaparecerem, e é procurando a line de CADA dataset que o
// teste descobre isso: contar as linhas, sozinho, não descobriria.
func rowOfTable(t *testing.T, rows [][]string, key string) []string {
	t.Helper()
	for _, l := range rows {
		if len(l) > 0 && l[0] == key {
			return l
		}
	}
	t.Fatalf("a tabela não tem a linha de %q:\n%v", key, rows)
	return nil
}

// celulaSobORotulo devolve a célula que a line escreveu sob o rótulo do
// cabeçalho.
func celulaSobORotulo(t *testing.T, cabecalho, row []string, rotulo string) string {
	t.Helper()
	for i, r := range cabecalho {
		if r != rotulo {
			continue
		}
		if i >= len(row) {
			t.Fatalf("a linha %v não tem célula sob %q (cabeçalho %v)", row, rotulo, cabecalho)
		}
		return row[i]
	}
	t.Fatalf("o cabeçalho %v não tem a coluna %q", cabecalho, rotulo)
	return ""
}

// confereByCelula compara a line da tabela com o oráculo CÉLULA A CÉLULA, na
// ordem em que o comando escreveu o cabeçalho.
//
// Todo rótulo do cabeçalho tem de ter oráculo: coluna sem conferência nenhuma é
// o buraco por onde os mutantes de troca passaram, e um mapa que apenas
// ignorasse o rótulo desconhecido reabriria o buraco no dia em que uma coluna
// nova nascesse.
//
// `aparte` nomeia as colunas cujo oráculo NÃO é igualdade de texto — hoje só a
// do prazo, que se confere de ida e volta (ver prazoOfSaida). Declarar a
// exceção pelo nome é o que a distingue do esquecimento.
func confereByCelula(t *testing.T, contexto string, cabecalho, row []string, expected map[string]string, aparte ...string) {
	t.Helper()
	if len(row) != len(cabecalho) {
		t.Fatalf("%s: a linha saiu com %d células e o cabeçalho %v tem %d: %v",
			contexto, len(row), cabecalho, len(cabecalho), row)
	}
	if len(expected)+len(aparte) != len(cabecalho) {
		t.Fatalf("%s: o cabeçalho %v tem %d colunas e o oráculo cobre %d (%d à parte): ou uma coluna "+
			"nasceu sem conferência, ou o oráculo confere uma que já não existe",
			contexto, cabecalho, len(cabecalho), len(expected), len(aparte))
	}
	for i, rotulo := range cabecalho {
		if slices.Contains(aparte, rotulo) {
			continue
		}
		quer, ok := expected[rotulo]
		if !ok {
			t.Fatalf("%s: a coluna %q não tem oráculo; o cabeçalho é %v", contexto, rotulo, cabecalho)
		}
		if row[i] != quer {
			t.Errorf("%s: sob %s a saída traz %q e o servidor respondeu %q (linha inteira: %v)",
				contexto, rotulo, row[i], quer, row)
		}
	}
}

// exigeColumnsDistinguiveis é a guarda `vistos` dos seis contadores do bloco
// `processo`, aplicada à TABELA.
//
// O argumento é o mesmo, com um grau a mais: lá, dois contadores com o MESMO
// valor tornam os dois rótulos indistinguíveis e o teste passa a aprovar um
// comando que lê o field do vizinho; aqui basta que UMA line separe duas
// colunas para que trocá-las apareça, então o que se exige é que, para cada par
// de colunas, exista pelo menos uma line em que elas difiram. Se o fixture
// deixar de separá-las, é o FIXTURE que precisa mudar — não o oráculo.
//
// As colunas de `aparte` ficam de fora porque o oráculo delas não é texto. Não
// é buraco: trocar a coluna do prazo com uma vizinha aparece do outro lado da
// troca, na coluna que recebeu um prazo onde deveria haver um número.
func exigeColumnsDistinguiveis(t *testing.T, cabecalho []string, expected []map[string]string, aparte ...string) {
	t.Helper()
	var rotulos []string
	for _, r := range cabecalho {
		if !slices.Contains(aparte, r) {
			rotulos = append(rotulos, r)
		}
	}
	for i := 0; i < len(rotulos); i++ {
		for j := i + 1; j < len(rotulos); j++ {
			separou := false
			for _, row := range expected {
				if row[rotulos[i]] != row[rotulos[j]] {
					separou = true
					break
				}
			}
			if !separou {
				t.Fatalf("as colunas %q e %q valem o mesmo em TODA linha do fixture (%d linhas): trocar as "+
					"duas fontes não mudaria uma vírgula da saída, e o teste aprovaria o comando trocado",
					rotulos[i], rotulos[j], len(expected))
			}
		}
	}
}

// rotuloOfPrazo é a coluna do `datasets` cujo oráculo é de ida e volta, e não
// de igualdade de texto — ver prazoOfSaida.
const rotuloOfPrazo = "TIMEOUT"

// prazoOfSaida devolve a duração que o comando escreveu, RECUPERADA do texto.
//
// A conferência é de ida e volta de propósito: o oráculo não é "o comando
// imprimiu a mesma string que eu formataria", que só afirmaria que dois
// trechos de código chamam o mesmo Sprintf, e sim que do texto para humano se
// recupera o MESMO prazo que a rota respondeu em milissegundos. É o argumento
// do bytesToReadableString aplicado ao tempo: número humanizado do qual não se recupera
// o valor esconde a diferença que o operador veio medir.
func prazoOfSaida(t *testing.T, text string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(text)
	if err != nil {
		t.Fatalf("o comando escreveu %q, que não é uma duração: %v", text, err)
	}
	return d
}

// estatisticas lê o /v1/stats pela porta, para servir de oráculo ao comando.
func estatisticas(t *testing.T, a *envRemote) protocol.Stats {
	t.Helper()
	var e protocol.Stats
	readJSONOfPorta(t, a, "/v1/stats", &e)
	return e
}

// waitConsultasDrenarem bloqueia até o servidor não reportar mais nenhuma
// consulta ativa, para que uma medição feita depois seja comparável com outra.
func waitConsultasDrenarem(t *testing.T, a *envRemote) {
	t.Helper()
	limit := time.Now().Add(5 * time.Second)
	for {
		e := estatisticas(t, a)
		ativas := int64(0)
		for _, d := range e.Datasets {
			ativas += d.Query.Active
		}
		if ativas == 0 {
			return
		}
		if time.Now().After(limit) {
			t.Fatalf("%d consulta(s) continuam ativas depois de 5s: o servidor não está drenando", ativas)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func metadados(t *testing.T, a *envRemote, consulta string) protocol.Metadata {
	t.Helper()
	var m protocol.Metadata
	readJSONOfPorta(t, a, "/v1/metadata"+consulta, &m)
	return m
}

func readJSONOfPorta(t *testing.T, a *envRemote, path string, destination any) {
	t.Helper()
	resp, err := http.Get("http://" + a.endereco() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("fechar o corpo de %s: %v", path, err)
		}
	}()
	corpo, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("lendo %s: %v", path, err)
	}
	if err := json.Unmarshal(corpo, destination); err != nil {
		t.Fatalf("decodificando %s: %v\n%s", path, err, corpo)
	}
}

// serverWithDataInAr monta o ambiente remoto, ingere e sobe o servidor. A
// ingestão acontece ANTES da subida porque o flock é exclusivo: é a mesma
// ordem que o operador segue.
func serverWithDataInAr(t *testing.T, eventos int) *envRemote {
	t.Helper()
	return risesWithEntry(t, newEnvRemote(t, 100000), corpus(t, eventos))
}

// risesWithEntry é o de baixo: ingere a entrada que o teste montou e sobe o
// servidor sobre ela.
//
// A separação existe porque o corpus do §12 não produz sozinho três fixtures de
// que esta suíte precisa — um teto de linhas baixo o bastante para truncar, uma
// coluna AUTO, e chaves em número suficiente para a carga do primário sair de
// zero. Cada um deles é uma line de entrada diferente, não um segundo helper.
func risesWithEntry(t *testing.T, a *envRemote, entry string) *envRemote {
	t.Helper()
	if code := a.runWithEntry(entry, "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.risesServer(t)
	return a
}

// acrescentaAoStorage insere o trecho no bloco `storage:` do YAML do ambiente,
// ANTES de o servidor subir.
//
// Reescreve o arquivo em vez de manter um segundo template porque o template do
// ambiente remoto é um só: uma cópia dele aqui envelheceria calada no dia em
// que o de lá mudasse. Se o ponto de inserção sumir, o teste morre dizendo
// isso — que é melhor do que subir com uma configuração diferente da que o
// teste afirma ter escrito.
func acrescentaAoStorage(t *testing.T, a *envRemote, trecho string) {
	t.Helper()
	yml, err := os.ReadFile(a.config)
	if err != nil {
		t.Fatal(err)
	}
	const target = "storage:\n"
	if !strings.Contains(string(yml), target) {
		t.Fatalf("o template do ambiente remoto não tem mais o bloco %q:\n%s", target, yml)
	}
	nw := strings.Replace(string(yml), target, target+trecho, 1)
	if err := os.WriteFile(a.config, []byte(nw), 0o644); err != nil {
		t.Fatal(err)
	}
}

// turnoffACompaction acrescenta `storage.compaction.enabled: false` ao YAML do
// ambiente. Subir com a compaction ligada e afirmar o contrário seria o teste
// mentindo sobre o próprio fixture, e é por isso que o acrescentaAoStorage
// morre quando não encontra onde inserir.
func turnoffACompaction(t *testing.T, a *envRemote) {
	t.Helper()
	acrescentaAoStorage(t, a, "  compaction:\n    enabled: false\n")
}

// afinaOStorageForCompactar deixa o ambiente com segmento PEQUENO e a política
// do §53 declarada, antes de o servidor subir.
//
// O segmentSize é o que faz a ingestão SELAR arquivos: com o padrão de 512 MiB
// (§85) o fixture inteiro caberia no primeiro segmento e a decisão D6 — o
// corrente nunca é candidato — deixaria a compaction sem um único alvo. É o
// mesmo encolhimento que o e2e da Task 7 faz, e pelo mesmo argumento: o que o
// mecanismo exige é uma PILHA de segmentos selados, não segmento grande.
//
// O minimumLiveRatio é DECLARADO, e não herdado, embora o default do §53 seja
// hoje exatamente este valor. O que este fixture precisa é que a eleição
// alcance os segmentos que o corpus deixa com pouca liveness, e um default
// alterado noutra tarefa mudaria calado o que o teste mede.
func afinaOStorageForCompactar(t *testing.T, a *envRemote) {
	t.Helper()
	acrescentaAoStorage(t, a, "  segmentSize: 64KiB\n  compaction:\n    minimumLiveRatio: 0.5\n")
}

// chavesDistintas monta n eventos com n chaves DIFERENTES.
//
// O corpus do §12 gira sobre 200 entidades, e o que o fixture da carga do
// primário precisa é de chave, não de evento: 20 mil eventos sobre 200 chaves
// deixam a primary com 200 slots ocupados.
func chavesDistintas(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, `{"id":"S%06d","_SEQ":1,"Acct-Status-Type":"Stop"}`+"\n", i)
	}
	return sb.String()
}

// updatesAboutChavesQuentesEFrias monta a entrada que dá à compaction o que
// devolver — e o que COPIAR.
//
// As duas famílias de chave têm papéis diferentes, e sem qualquer uma delas o
// fixture deixa de separar um par de rótulos do `status`:
//
//   - as QUENTES são atualizadas em toda rodada, e delas sai o GARBAGE: cada
//     update apenda um record novo e deixa o anterior morto, que é a forma do
//     dataset do §86 e a razão de o disco crescer. Sem elas nenhum segmento cai
//     abaixo do minimumLiveRatio, nada é eleito, nenhum arquivo é aposentado —
//     e `segmentos` continua igual a `corrente`, que é a coincidência que o
//     godoc de protocolo.StorageStats diz que a compaction quebrou.
//   - as FRIAS são escritas uma vez e nunca mais, espalhadas ao longo das
//     rodadas, e delas sai o `bytes escritos`: são os records VIVOS dentro de
//     um segmento condenado, que é o que o copy-forward do §55 tem de mover
//     para frente. Sem elas todo record vivo estaria no segmento corrente — que
//     a D6 protege —, os eleitos seriam 100% lixo, e a varredura leria
//     megabytes para escrever ZERO. Com `bytes lidos` e `bytes escritos`
//     valendo o mesmo zero, alimentar um com a fonte do outro não muda uma
//     vírgula da saída.
//
// A sequência das frias é sempre 1 porque elas nunca são reescritas; a das
// quentes é a rodada, que o `strategy: greater` do §12 aceita como update.
func updatesAboutChavesQuentesEFrias(quentes, rodadas, friasByRodada int) string {
	var sb strings.Builder
	fria := 0
	for r := 1; r <= rodadas; r++ {
		for k := 0; k < quentes; k++ {
			fmt.Fprintf(&sb, `{"id":"Q%04d","_SEQ":%d,"Acct-Status-Type":"Stop","User-Name":"u%04d","NAS-Identifier":"nas-%d"}`+"\n",
				k, r, k, r%7)
		}
		for i := 0; i < friasByRodada; i++ {
			fmt.Fprintf(&sb, `{"id":"F%06d","_SEQ":1,"Acct-Status-Type":"Start","User-Name":"f%06d","NAS-Identifier":"nas-fria"}`+"\n",
				fria, fria)
			fria++
		}
	}
	return sb.String()
}

// nameOfSegundoDataset e specOfSegundoDataset são o outro dataset do servidor,
// e existem por causa do `datasets`.
//
// O comando cuja única razão de existir é listar O QUE ESTE SERVIDOR SERVE
// estava sendo exercitado contra um servidor de UM dataset, e um servidor de um
// dataset não distingue "lista todos" de "imprime o primeiro N vezes": com uma
// line só, escrever o nome de m.Datasets[0] em todas elas é indistinguível de
// escrever o nome de cada uma. É a mesma família da guarda `vistos` dos seis
// contadores do bloco `processo` — dois valores iguais tornam dois rótulos
// indistinguíveis —, agora na dimensão da LINHA.
//
// Ele diverge do fixture do §12 em TODAS as colunas da tabela (nome, chave,
// número de colunas, teto de linhas e prazo), e isso não é enfeite: é o que o
// exigeColumnsDistinguiveis cobra para que trocar duas colunas de lugar apareça.
//
// Não declara `source`, então nenhum consumer sobe para ele (§12 + D15) e o
// dataset nasce vazio — o que basta, porque o `datasets` descreve SCHEMA e
// limites, não conteúdo.
const nameOfSegundoDataset = "auditoria_acessos"

const specOfSegundoDataset = `version: 1
name: auditoria_acessos
schemaName: auditoria
materialization:
  key:
    field: sessao
  mode: merge
  sequence:
    field: ordem
    type: int64
    strategy: greater
schema:
  include: ["*"]
  unknownFields: include
  fields:
    sessao:
      type: string
    ordem:
      type: int64
    resultado:
      type: string
      index: true
query:
  maxRows: 77
  timeout: 12s
`

// acrescentaOSegundoDataset escreve a spec no diretório que o servidor varre,
// ANTES de ele subir — é lá que o `Novo` descobre o que servir.
//
// O nome do ARQUIVO tem de casar o `name:` de dentro (o schema.Load recusa a
// divergência), então a conferência acontece aqui: uma constante editada pela
// metade falharia na subida do servidor, com uma mensagem sobre YAML, e não
// sobre o fixture.
func acrescentaOSegundoDataset(t *testing.T, a *envRemote) {
	t.Helper()
	if !strings.Contains(specOfSegundoDataset, "name: "+nameOfSegundoDataset+"\n") {
		t.Fatalf("a spec do segundo dataset não declara name: %s", nameOfSegundoDataset)
	}
	path := filepath.Join(a.dir, "datasets", nameOfSegundoDataset+".yaml")
	if err := os.WriteFile(path, []byte(specOfSegundoDataset), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestStatusRemotoFuncionaComOServidorNoAr é o critério que separa este
// comando do débito 54: ele tem de responder COM o servidor de pé, que é
// justamente quando o flock barra o caminho local.
//
// Os números conferidos são os do §96 ("status") lidos contra o /v1/stats: se
// o comando inventasse um segundo caminho para chegar neles, a divergência
// apareceria aqui — que é a mesma doutrina do /metrics contra o JSON.
func TestStatusRemotoFuncionaComOServidorNoAr(t *testing.T) {
	a := serverWithDataInAr(t, 50)

	// Tráfego ANTES do status, para que os três números da linha de consultas
	// deixem de ser zero. Enquanto eram, trocar Total por Errors era invisível
	// — o oráculo comparava "0 (0 ativas, 0 com erro)" consigo mesmo, e a
	// asserção parecia existir sem exercer nada.
	for i := 0; i < 2; i++ {
		if code := a.run("query", "--server", a.endereco(),
			"--dataset", "radius_sessions",
			"SELECT id FROM radius_sessions WHERE id = 'chave que nao existe'"); code != 0 {
			t.Fatalf("consulta de aquecimento saiu %d: %s", code, a.stderr.String())
		}
	}
	a.stdout.Reset()
	a.stderr.Reset()

	// As consultas de aquecimento precisam ter DRENADO antes de medir.
	//
	// O `query` do CLI retorna quando o cliente terminou de ler a resposta,
	// e nesse instante o servidor ainda pode não ter decrementado o medidor
	// de consultas ativas — o handler decrementa ao terminar, e o cliente vê
	// a última linha antes disso. Sem esperar, o `status` abaixo e o
	// /v1/stats mais adiante fotografam momentos DIFERENTES: a saída textual
	// sai com "2 ativas" e o oráculo, lido depois, com 0.
	//
	// Não é hipótese: foi assim que este teste falhou no CI (job "test
	// (./...)" da run 32676650648), passando em toda execução local e no
	// próprio -race da mesma run. Uma máquina mais lenta alarga a janela.
	waitConsultasDrenarem(t, a)

	if code := a.run("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d, queria 0: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	if !strings.Contains(saida, "radius_sessions") {
		t.Errorf("saída não nomeia o dataset: %q", saida)
	}

	e := estatisticas(t, a)
	if len(e.Datasets) != 1 {
		t.Fatalf("o /v1/stats trouxe %d datasets: o oráculo não se aplica", len(e.Datasets))
	}
	ds := e.Datasets[0]
	if ds.Storage.Rows == 0 {
		t.Fatal("o servidor reporta 0 rows: o teste não estaria provando nada")
	}

	casos := []struct {
		rotulo string
		quer   string
	}{
		{"rows", strconv.FormatUint(ds.Storage.Rows, 10)},
		{"segmentos", strconv.FormatUint(uint64(ds.Storage.Segments), 10)},
		{"corrente", strconv.FormatUint(uint64(ds.Storage.CurrentSegment), 10)},
		{"overlay", strconv.Itoa(ds.Storage.Overlay)},
	}
	for _, c := range casos {
		if got := fieldOfSaida(t, saida, c.rotulo); got != c.quer {
			t.Errorf("%s = %q na saída e %q no /v1/stats", c.rotulo, got, c.quer)
		}
	}

	// O disco é o que este milestone existe para tornar visível (§86): o
	// comando tem de trazer o número EXATO da rota, e não uma aproximação
	// humanizada da qual não se recupera o valor.
	if got := fieldOfSaida(t, saida, "disco"); !strings.HasPrefix(got, strconv.FormatInt(ds.Storage.Bytes, 10)+" B") {
		t.Errorf("disco = %q, queria começar com %d B", got, ds.Storage.Bytes)
	}
	if got := fieldOfSaida(t, saida, "garbage (>=)"); !strings.HasPrefix(got, strconv.FormatUint(ds.Storage.GarbageBytes, 10)+" B") {
		t.Errorf("garbage = %q, queria começar com %d B", got, ds.Storage.GarbageBytes)
	}

	// A ingestão do §96 sai pelo vocabulário do §94, o mesmo do /v1/health:
	// duas palavras diferentes para o mesmo estado obrigariam o operador a
	// aprender as duas.
	if got := fieldOfSaida(t, saida, "ingestão"); !strings.HasPrefix(got, ds.Ingestion.State) {
		t.Errorf("ingestão = %q, queria começar com %q", got, ds.Ingestion.State)
	}

	// A linha de consultas põe TRÊS números na MESMA frase, e era a última do
	// bloco sem oráculo: trocar Total por Errors deixava a suíte inteira verde.
	// A frase única é justamente o que torna a troca fácil de cometer e difícil
	// de enxergar — a saída continua plausível.
	querConsultas := fmt.Sprintf("%d (%d ativas, %d com erro)",
		ds.Query.Total, ds.Query.Active, ds.Query.Errors)
	if got := fieldOfSaida(t, saida, "consultas"); got != querConsultas {
		t.Errorf("consultas = %q, queria %q", got, querConsultas)
	}
}

// A compaction é o assunto deste milestone, e um `status` que não a mostrasse
// deixaria o operador sem o único lugar em que o laço de pé e os bytes movidos
// aparecem junto do disco que eles devolvem.
//
// O estado esperado é LIGADA porque é o default do §78: um YAML herdado do M1,
// sem o bloco storage.compaction, compacta. O que se afere não é só a palavra
// — é que ela acompanha o que a rota respondeu, e não um literal escolhido
// pelo comando.
func TestStatusMostraOEstadoDaCompaction(t *testing.T) {
	a := serverWithDataInAr(t, 20)
	waitCompactionInAr(t, a)

	if code := a.run("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	ds := estatisticas(t, a).Datasets[0]

	if !ds.Storage.Compaction.Running {
		t.Fatal("o /v1/stats reporta a compaction parada logo depois de ela subir")
	}
	if got := fieldOfSaida(t, saida, "compaction"); got != "ligada" {
		t.Errorf("compaction = %q, queria dizer que o laço está de pé", got)
	}
	if got := fieldOfSaida(t, saida, "bytes lidos"); !strings.HasPrefix(got, strconv.FormatInt(ds.Storage.Compaction.BytesRead, 10)+" B") {
		t.Errorf("bytes lidos = %q, queria %d B", got, ds.Storage.Compaction.BytesRead)
	}
	if got := fieldOfSaida(t, saida, "bytes escritos"); !strings.HasPrefix(got, strconv.FormatInt(ds.Storage.Compaction.BytesWritten, 10)+" B") {
		t.Errorf("bytes escritos = %q, queria %d B", got, ds.Storage.Compaction.BytesWritten)
	}
}

// TestStatusMostraACompactionDesligada é a outra metade do par acima, e sem
// ela o teste de cima não prova a palavra: `return "ligada"` no corpo do
// compactionStatus passa por todo oráculo que só sobe com a chave no
// default, e o `waitCompactionInAr` está lá justamente para garantir que o
// laço esteja de pé quando a saída for lida.
//
// O ramo que falta é o que o godoc da função nomeia como o único que importa:
// "desligada" com a chave ligada no YAML é a falha sticky do writer (§14.14),
// em que a goroutine parou sozinha e o disco voltou a crescer com o processo de
// pé. Essa falha não se provoca por fora sem avariar o writer; a chave em false
// produz o MESMO estado observável — laço parado, servidor no ar — e é o que
// separa a palavra de um literal.
func TestStatusMostraACompactionDesligada(t *testing.T) {
	a := newEnvRemote(t, 100000)
	turnoffACompaction(t, a)
	risesWithEntry(t, a, corpus(t, 20))

	if code := a.run("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	// Não há o que esperar aqui, ao contrário do teste de cima: com a chave em
	// false o laço não sobe NUNCA, e esperar por ele só saberia gastar o prazo
	// inteiro antes de falhar.
	ds := estatisticas(t, a).Datasets[0]
	if ds.Storage.Compaction.Running {
		t.Fatal("o /v1/stats reporta o laço de pé com storage.compaction.enabled: false")
	}
	if got := fieldOfSaida(t, saida, "compaction"); got != "desligada" {
		t.Errorf("compaction = %q com a chave em false; queria \"desligada\", que é a palavra "+
			"que o operador procura quando o disco cresce com o processo de pé (§14.14)", got)
	}
}

// waitCompactionInAr bloqueia até o /v1/stats reportar o laço de pé.
//
// A subida põe o HTTP no ar ANTES de ligar a compaction (§51), então há uma
// janela em que o /v1/health já responde e o laço ainda não subiu. Esperar pela
// CONDIÇÃO fecha a janela sem dormir — dormir esconderia tanto a lentidão
// quanto a ausência de progresso.
func waitCompactionInAr(t *testing.T, a *envRemote) {
	t.Helper()
	prazo := time.Now().Add(15 * time.Second)
	for time.Now().Before(prazo) {
		e := estatisticas(t, a)
		if len(e.Datasets) > 0 && e.Datasets[0].Storage.Compaction.Running {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a compaction não subiu em 15s: o default do §78 traz a chave ligada")
}

// Os quatro números que o M4 acrescentou ao `status` são DOIS PARES, e a
// distância dentro de cada par é a resposta do §86. Nenhum dos dois pares se
// distinguia no fixture parado, e é isso que este teste conserta.
//
// `segmentos` e `corrente` valiam AMBOS 1. O godoc de protocolo.StorageStats diz
// por que isso é grave: os dois coincidiam enquanto os IDs eram monotônicos e
// nada removia arquivo, premissa que a compaction quebrou — "a distância entre
// os dois é justamente o que a compaction devolveu". A produção escreve o mesmo
// argumento ao lado do campo ("publicar só um dos dois esconderia isso") e nada
// verificava: trocar as duas fontes, ou publicar o CurrentSegment sob o rótulo
// `segmentos`, deixava a suíte inteira verde.
//
// `bytes lidos` e `bytes escritos` valiam AMBOS 0, porque a suíte lia o status
// antes de qualquer passada de compaction concluir. Alimentar `bytes lidos` com
// o BytesWritten passava — e contra um servidor com compaction efetiva ele
// imprime "0 B" onde estão os 1,7 MB que a varredura de fato leu, que é
// exatamente o número que responde a pergunta do §86.
//
// O fixture é o que separa os quatro, e cada peça dele fecha um par: segmento de
// 64 KiB para haver PILHA de selados, chaves quentes para produzir o garbage que
// elege, e chaves frias espalhadas para haver dado VIVO dentro do que se elegeu.
func TestStatusSeparaOsSegmentosDoCorrenteEOsBytesDaCompaction(t *testing.T) {
	a := newEnvRemote(t, 100000)
	afinaOStorageForCompactar(t, a)
	risesWithEntry(t, a, updatesAboutChavesQuentesEFrias(50, 200, 5))

	saida, st := statusWithOsFieldsOfDiskSeparados(t, a)
	for _, c := range fieldsOfDiskOfStatus(st) {
		quer := strconv.FormatInt(c.value, 10)
		got := fieldOfSaida(t, saida, c.rotulo)
		if c.inBytes {
			// O byte EXATO vem primeiro no bytesToReadableString, e é ele que se
			// confere: quem compara duas medições — antes e depois de uma
			// passada — precisa do número, não da grandeza arredondada.
			if !strings.HasPrefix(got, quer+" B") {
				t.Errorf("%s = %q na saída e %s B no /v1/stats", c.rotulo, got, quer)
			}
			continue
		}
		if got != quer {
			t.Errorf("%s = %q na saída e %s no /v1/stats", c.rotulo, got, quer)
		}
	}
}

// fieldOfDisk é um dos números que o §96 publica sob rótulo próprio no bloco
// do dataset. `inBytes` marca os que saem pelo bytesToReadableString, cuja célula
// começa pelo valor exato e continua com a grandeza para humano.
type fieldOfDisk struct {
	rotulo  string
	value   int64
	inBytes bool
}

// fieldsOfDiskOfStatus é a lista que a espera e a conferência compartilham —
// as duas precisam falar dos MESMOS quatro números, senão o teste espera por uma
// condição e afirma outra.
func fieldsOfDiskOfStatus(st protocol.StorageStats) []fieldOfDisk {
	return []fieldOfDisk{
		{"segmentos", int64(st.Segments), false},
		{"corrente", int64(st.CurrentSegment), false},
		{"bytes lidos", st.Compaction.BytesRead, true},
		{"bytes escritos", st.Compaction.BytesWritten, true},
	}
}

// colisaoEntreOsFieldsOfDisk descreve a primeira razão pela qual os quatro
// números ainda não distinguem os quatro rótulos, ou devolve "" quando
// distinguem.
//
// É a guarda `vistos` dos seis contadores do bloco `processo`, na forma de
// predicado: dois números iguais tornam os dois rótulos indistinguíveis, e o
// teste passaria a aprovar um comando que lê o field do vizinho. Ela é
// predicado, e não asserção imediata, porque aqui o fixture leva algumas
// centenas de milissegundos para separá-los — a compaction precisa aposentar o
// primeiro segmento —, e afirmar antes disso seria reprovar o comando certo.
//
// Os dois contadores de bytes ainda são exigidos NÃO-ZERO à parte. Zero é o
// estado em que a suíte anterior os lia, e zero não é só igual ao vizinho: é
// igual a tudo o que ainda não aconteceu.
func colisaoEntreOsFieldsOfDisk(st protocol.StorageStats) string {
	fields := fieldsOfDiskOfStatus(st)
	for _, c := range fields {
		if c.inBytes && c.value == 0 {
			return fmt.Sprintf("%q ainda vale 0: nenhuma passada de compaction concluiu", c.rotulo)
		}
	}
	vistos := map[int64]string{}
	for _, c := range fields {
		if outro, ok := vistos[c.value]; ok {
			return fmt.Sprintf("%q e %q valem %d: com dois números iguais o teste não distingue os rótulos",
				outro, c.rotulo, c.value)
		}
		vistos[c.value] = c.rotulo
	}
	return ""
}

// statusWithOsFieldsOfDiskSeparados roda o `status` ENTRE duas leituras do
// /v1/stats e devolve a saída junto da amostra em que as duas coincidiram.
//
// A sanduíche existe porque este é o único teste da suíte em que a compaction
// TRABALHA enquanto a saída é lida: os quatro números se movem a cada segmento
// aposentado, e comparar a saída do comando com um /v1/stats lido depois seria
// comparar dois instantes diferentes — o teste reprovaria o comando certo por
// uma passada que aconteceu no meio. Exigir que a leitura de antes e a de depois
// sejam IDÊNTICAS fixa o instante sem parar a compaction, que é justamente o que
// o fixture existe para deixar acontecer.
//
// Esperar pela CONDIÇÃO, e não por tempo, é a mesma regra do waitCompactionInAr:
// dormir um prazo fixo esconderia tanto a lentidão quanto a ausência de
// progresso. E o prazo que expira morre dizendo QUAL par ainda colide, porque a
// resposta certa a ele é mexer no fixture, não no oráculo.
func statusWithOsFieldsOfDiskSeparados(t *testing.T, a *envRemote) (string, protocol.StorageStats) {
	t.Helper()
	motivo := "o /v1/stats não chegou a ser lido"
	prazo := time.Now().Add(30 * time.Second)
	for time.Now().Before(prazo) {
		before := storageOfDataset(t, a)
		if m := colisaoEntreOsFieldsOfDisk(before); m != "" {
			motivo = m
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if code := a.run("status", "--server", a.endereco()); code != 0 {
			t.Fatalf("status: exit = %d: %s", code, a.stderr.String())
		}
		saida := a.stdout.String()
		if storageOfDataset(t, a) == before {
			return saida, before
		}
		motivo = "a compaction moveu os números DURANTE o comando"
	}
	t.Fatalf("em 30s o fixture não pôs os quatro números do disco em valores distintos e estáveis: %s", motivo)
	return "", protocol.StorageStats{}
}

// storageOfDataset é o bloco de storage do dataset único do ambiente, lido pela
// porta. Morre quando o servidor traz outro número de datasets: um oráculo que
// pegasse o primeiro de vários estaria conferindo a saída de um contra os
// números de outro.
func storageOfDataset(t *testing.T, a *envRemote) protocol.StorageStats {
	t.Helper()
	e := estatisticas(t, a)
	if len(e.Datasets) != 1 {
		t.Fatalf("o /v1/stats trouxe %d datasets: o oráculo não se aplica", len(e.Datasets))
	}
	return e.Datasets[0].Storage
}

// A seção `processo` publica os seis contadores do motor, e eles são do
// PROCESSO e não de um dataset — repeti-los por dataset publicaria o mesmo
// número N vezes, e alguém somaria os N.
//
// Seis rótulos que o operador lê como verdade do motor precisam de seis
// valores DIFERENTES para serem provados: com o fixture parado, os seis valem
// 0, são indistinguíveis entre si, e trocar a fonte de um pelo campo do
// vizinho não muda uma vírgula da saída. Por isso este teste roda consulta
// antes de perguntar o status, e roda cada família de plano um número
// diferente de vezes — 2 full scans, 3 point lookups, 4 index scans, 6
// uniões, 5 linhas emitidas e 1 truncamento. O rótulo publicado continua sendo
// "bitmap scans": é nome de WIRE, e ele não acompanhou a unificação dos motores
// (ver o godoc de protocol.EngineStats).
//
// Os planos são os do §63: chave no WHERE dá point lookup, campo indexado dá
// index scan, OR de dois termos indexados dá união, e campo sem índice cai em
// full scan. Todos batem em valor que não existe, para que a única emissão do
// fixture seja a da consulta sem WHERE — que estoura o teto de 5 linhas do
// dataset e produz o truncamento.
func TestStatusPublicaOsContadoresDoMotorDoProcesso(t *testing.T) {
	a := risesWithEntry(t, newEnvRemote(t, 5), corpus(t, 50))

	consulta := func(vezes int, quer int, sql string) {
		t.Helper()
		for i := 0; i < vezes; i++ {
			if code := a.run("query", "--server", a.endereco(),
				"--dataset", "radius_sessions", sql); code != quer {
				t.Fatalf("exit = %d, quer %d para %q: %s", code, quer, sql, a.stderr.String())
			}
		}
	}
	// A sem WHERE é a única que emite: 5 linhas, o teto do dataset, e o exit 5
	// do truncamento.
	consulta(1, 5, "SELECT id FROM radius_sessions")
	consulta(1, 0, "SELECT id FROM radius_sessions WHERE username = 'ninguem'")
	consulta(3, 0, "SELECT id FROM radius_sessions WHERE id = 'chave que nao existe'")
	consulta(4, 0, "SELECT id FROM radius_sessions WHERE status = 'Fantasma'")
	consulta(6, 0, "SELECT id FROM radius_sessions WHERE status = 'Fantasma' OR status = 'Outro'")

	if code := a.run("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	q := estatisticas(t, a).Process.Query
	fields := []struct {
		rotulo string
		value  int64
	}{
		{"full scans", q.FullScans},
		{"point lookups", q.PointLookups},
		{"bitmap scans", q.BitmapScans},
		{"index unions", q.IndexUnions},
		{"linhas emitidas", q.Emitted},
		{"linhas truncadas", q.Truncated},
	}

	// A guarda vem ANTES da conferência: dois contadores com o mesmo valor
	// tornam os dois rótulos indistinguíveis, e o teste passaria a aprovar um
	// comando que lê o campo errado. Se o fixture deixar de separá-los, é o
	// fixture que precisa mudar — não o oráculo.
	vistos := map[int64]string{}
	for _, c := range fields {
		if outro, ok := vistos[c.value]; ok {
			t.Fatalf("%q e %q valem %d: com dois contadores iguais o teste não distingue os rótulos",
				outro, c.rotulo, c.value)
		}
		vistos[c.value] = c.rotulo
	}

	for _, c := range fields {
		if got := fieldOfSaida(t, saida, c.rotulo); got != strconv.FormatInt(c.value, 10) {
			t.Errorf("%s = %q na saída e %d no /v1/stats", c.rotulo, got, c.value)
		}
	}
}

// A carga do primário é o número que diz se o índice primário vai precisar
// crescer (o godoc de StorageStats põe o sinal em ~0,7), e o status é onde o
// operador o lê.
//
// O fixture tem 12 mil CHAVES por um motivo aritmético: a primary nasce com
// piso de 1<<20 slots (§5), a carga é ocupados/slots e a saída tem duas casas,
// então qualquer dataset abaixo de ~5,2 mil chaves imprime "0.00" — e um
// literal "0.00" no lugar do campo passaria por um oráculo montado sobre um
// fixture pequeno. Se um dia o piso mudar e este fixture voltar a imprimir
// zero, o teste falha dizendo isso em vez de aprovar a constante.
func TestStatusPublicaACargaDoPrimario(t *testing.T) {
	a := risesWithEntry(t, newEnvRemote(t, 100000), chavesDistintas(12000))

	if code := a.run("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	ds := estatisticas(t, a).Datasets[0]
	quer := strconv.FormatFloat(ds.Storage.PrimaryLoadFactor, 'f', 2, 64)
	if quer == "0.00" {
		t.Fatalf("a carga saiu 0.00 com %d rows (fator %v): neste fixture o campo já não se "+
			"distingue de uma constante", ds.Storage.Rows, ds.Storage.PrimaryLoadFactor)
	}
	if got := fieldOfSaida(t, saida, "carga do primário"); got != quer {
		t.Errorf("carga do primário = %q na saída e %q no /v1/stats", got, quer)
	}
}

// `datasets` responde a pergunta mais barata do §96 — o que este servidor
// serve — e é a que um operador faz antes de saber o nome exato para passar ao
// `inspect`.
//
// O servidor sobe com DOIS datasets, e essa é a diferença que faz o teste
// provar o comando. Contra um servidor de um dataset só, "lista o que este
// servidor serve" é indistinguível de "imprime o primeiro dataset uma vez": o
// comando que escrevesse o nome de m.Datasets[0] em toda linha passaria pela
// suíte inteira e, no servidor do operador, duplicaria o primeiro e faria o
// segundo desaparecer — justamente o comando cuja única razão de existir é
// não fazer isso.
//
// A conferência é POR CÉLULA em todas as colunas, e não por `strings.Contains`
// na linha: Contains é indiferente à ordem, e aprova sem piscar uma saída que
// escreva o número de colunas sob MAXROWS e o teto de linhas sob COLUNAS. Quem
// ancora a posição é o cabeçalho que o próprio comando escreveu.
func TestDatasetsListaOQueOServidorServe(t *testing.T) {
	a := newEnvRemote(t, 100000)
	acrescentaOSegundoDataset(t, a)
	risesWithEntry(t, a, corpus(t, 10))

	if code := a.run("datasets", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	m := metadados(t, a, "")
	if len(m.Datasets) < 2 {
		t.Fatalf("o /v1/metadata trouxe %d datasets: com menos de dois o comando não é exercitado "+
			"naquilo que ele existe para fazer", len(m.Datasets))
	}

	cabecalho, rows := tableOfSaida(t, saida, "DATASET")
	if len(rows) != len(m.Datasets) {
		t.Fatalf("a tabela saiu com %d linhas para %d datasets servidos:\n%s", len(rows), len(m.Datasets), saida)
	}

	oraculos := make([]map[string]string, 0, len(m.Datasets))
	for _, d := range m.Datasets {
		if d.TimeoutMs == 0 {
			t.Fatalf("o /v1/metadata respondeu timeout 0 para %q: contra ele um \"0s\" fixo passaria", d.Name)
		}
		oraculos = append(oraculos, map[string]string{
			"DATASET": d.Name,
			"KEY":     d.Key,
			"COLUNAS": strconv.Itoa(len(d.Columns)),
			"MAXROWS": strconv.Itoa(d.MaxRows),
		})
	}
	exigeColumnsDistinguiveis(t, cabecalho, oraculos, rotuloOfPrazo)

	for i, d := range m.Datasets {
		row := rowOfTable(t, rows, d.Name)
		confereByCelula(t, "dataset "+d.Name, cabecalho, row, oraculos[i], rotuloOfPrazo)

		// A coluna TIMEOUT é o prazo EFETIVO da consulta escrito para humano, e
		// é por ele que o operador decide se a query que ele vai mandar cabe no
		// dataset. O oráculo dela é de IDA E VOLTA (ver prazoOfSaida), mas a
		// célula continua sendo a da coluna: um "0s" fixo — ou o prazo na
		// coluna errada — diria ao operador que toda consulta morre na largada.
		celula := celulaSobORotulo(t, cabecalho, row, rotuloOfPrazo)
		if got := prazoOfSaida(t, celula); got != time.Duration(d.TimeoutMs)*time.Millisecond {
			t.Errorf("%s: TIMEOUT = %q (%v) na saída e %d ms no /v1/metadata", d.Name, celula, got, d.TimeoutMs)
		}
	}
}

// `inspect` é o comando do §96 que detalha o dataset, e o que o operador
// procura nele é o que a consulta vai poder usar: o tipo da coluna, o alias
// pelo qual ela é escrita no SQL e o ÍNDICE declarado — que é o que separa uma
// consulta com lookup de uma que cai em full scan.
func TestInspectDetalhaColunasEIndices(t *testing.T) {
	// O corpus do §12 só produz campo fora do schema em ~1% dos eventos, então
	// com dez deles a tabela sairia sem NENHUMA coluna auto — e a coluna AUTO
	// seria um traço fixo que nada distingue. O evento a mais fixa isso: campo
	// que o YAML não declara, com `unknownFields: include`, é coluna auto por
	// construção.
	a := risesWithEntry(t, newEnvRemote(t, 100000),
		corpus(t, 10)+`{"id":"S-auto","_SEQ":1,"Campo-Fora-Do-Schema":"presente"}`+"\n")

	if code := a.run("inspect", "--server", a.endereco(), "radius_sessions"); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	m := metadados(t, a, "?dataset=radius_sessions")
	if len(m.Datasets) != 1 {
		t.Fatalf("o /v1/metadata filtrado trouxe %d datasets", len(m.Datasets))
	}
	d := m.Datasets[0]
	if fieldOfSaida(t, saida, "key") != d.Key {
		t.Errorf("key = %q, queria %q", fieldOfSaida(t, saida, "key"), d.Key)
	}
	// O contador de colunas era o único campo do bloco sem oráculo, e o
	// inspect publica esse número DUAS vezes — aqui e como número de linhas da
	// tabela. Alimentá-lo com d.MaxRows deixava a suíte verde: é a mesma
	// família que o resto deste arquivo fecha, valor sob o rótulo do vizinho.
	if got, quer := fieldOfSaida(t, saida, "colunas"), strconv.Itoa(len(d.Columns)); got != quer {
		t.Errorf("colunas = %q, queria %q (o dataset tem %d colunas e maxRows %d)",
			got, quer, len(d.Columns), d.MaxRows)
	}
	if fieldOfSaida(t, saida, "maxRows") != strconv.Itoa(d.MaxRows) {
		t.Errorf("maxRows = %q, queria %d", fieldOfSaida(t, saida, "maxRows"), d.MaxRows)
	}
	// O timeout é o outro limite EFETIVO, e sai pelo mesmo argumento do
	// maxRows: quem consulta precisa do prazo que VAI VALER. Escrito para
	// humano, ele tem de voltar ao milissegundo que a rota respondeu.
	if d.TimeoutMs == 0 {
		t.Fatal("o /v1/metadata respondeu timeout 0: contra ele um \"0s\" fixo passaria")
	}
	if got := prazoOfSaida(t, fieldOfSaida(t, saida, "timeout")); got != time.Duration(d.TimeoutMs)*time.Millisecond {
		t.Errorf("timeout = %v na saída e %d ms no /v1/metadata", got, d.TimeoutMs)
	}

	// Toda coluna do metadata sai, e sai com os cinco atributos, cada um na SUA
	// coluna: uma tabela que omitisse uma linha mandaria o operador concluir que
	// aquele campo não existe, e uma que trocasse ALIAS com TIPO lhe daria o
	// nome do campo onde ele leria o tipo. A conferência antiga era
	// `strings.Contains` na linha inteira para tudo menos a última coluna, e
	// Contains é indiferente à ordem — trocar as duas fontes deixava a suíte
	// verde.
	if len(d.Columns) < 10 {
		t.Fatalf("o fixture tem %d colunas: poucas para distinguir tabela de amostra", len(d.Columns))
	}
	cabecalho, rows := tableOfSaida(t, saida, "COLUNA")
	if len(rows) != len(d.Columns) {
		t.Fatalf("a tabela saiu com %d linhas para %d colunas do /v1/metadata:\n%s", len(rows), len(d.Columns), saida)
	}

	// A coluna AUTO diz se o TIPO daquela coluna foi declarado no YAML ou fixado
	// pela primeira observação — e é o que explica ao operador por que um campo
	// aparece como "unknown", ou por que ele não pode receber índice. O ÍNDICE
	// publica o NOME do plano (bitmap/hash/none) e não um "sim/não": a D5
	// unificou a GRAFIA de entrada (`index: true` é a única), mas o wire
	// continua nomeando o plano que o executor de fato usa, que é a pergunta
	// operacional real.
	var auto int
	oraculos := make([]map[string]string, 0, len(d.Columns))
	for _, c := range d.Columns {
		alias := "-"
		if c.Alias != nil {
			alias = *c.Alias
		}
		mark := "-"
		if c.Auto {
			mark, auto = "sim", auto+1
		}
		oraculos = append(oraculos, map[string]string{
			"COLUNA": c.Name,
			"ALIAS":  alias,
			"TIPO":   c.Type,
			"JDBC":   c.JDBC,
			"ÍNDICE": c.Index,
			"AUTO":   mark,
		})
	}
	if auto == 0 {
		t.Fatal("nenhuma coluna auto no fixture: a coluna AUTO seria um traço fixo e o teste não provaria nada")
	}
	exigeColumnsDistinguiveis(t, cabecalho, oraculos)

	for i, c := range d.Columns {
		confereByCelula(t, "coluna "+c.Name, cabecalho, rowOfTable(t, rows, c.Name), oraculos[i])
	}

	// Os dois campos indexados do fixture do §12, conferidos na CÉLULA do
	// índice — e não com um `strings.Contains` na linha inteira, que não
	// distinguiria "a palavra bitmap aparece em algum lugar" de "o índice
	// desta coluna é bitmap".
	//
	// As DUAS saem "bitmap": antes da D5, NAS-IP-Address era `index: hash` e
	// saía diferente de Acct-Status-Type. Com um motor só, `index: true` é a
	// única grafia e o campo compilado carrega um bool — era o IndexKind, hoje
	// schema.CompiledField.Indexed; "bitmap" e "none" continuam sendo as duas
	// palavras que o wire publica, ver o godoc do IndexName.
	for column, quer := range map[string]string{"Acct-Status-Type": "bitmap", "NAS-IP-Address": "bitmap"} {
		if got := celulaSobORotulo(t, cabecalho, rowOfTable(t, rows, column), "ÍNDICE"); got != quer {
			t.Errorf("o índice de %s saiu %q, queria %q", column, got, quer)
		}
	}
}

// A guarda de cardinalidade do `inspect` não é alcançável pelo servidor: o
// filtro do §67 responde exatamente um dataset ou recusa com 404. Ela existe
// para o dia em que o protocolo mudar sob o comando — e é justamente isso que
// o comentário dela afirma, sem que nada verificasse.
//
// O oráculo é um servidor de mentira porque não há outro jeito de produzir a
// resposta ilegítima. O que se afere é o desfecho: o comando RECUSA, com o
// código de dataset, sem escrever tabela nenhuma no stdout. Aceitar a resposta
// e imprimir o primeiro elemento seria descrever um dataset que o operador não
// pediu, sob o nome que ele pediu.
func TestInspectRecusaMetadataQueNaoTrazExatamenteUmDataset(t *testing.T) {
	casos := []struct {
		name     string
		datasets []protocol.DatasetMeta
	}{
		{"nenhum", nil},
		{"dois", []protocol.DatasetMeta{{Name: "radius_sessions"}, {Name: "outro"}}},
	}
	for _, c := range casos {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", protocol.ContentTypeJSON)
				if err := json.NewEncoder(w).Encode(protocol.Metadata{Datasets: c.datasets}); err != nil {
					t.Errorf("escrevendo a resposta de mentira: %v", err)
				}
			}))
			defer srv.Close()

			a := newEnv(t)
			if code := a.run("inspect", "--server", srv.URL, "radius_sessions"); code != 3 {
				t.Fatalf("exit = %d, quer 3 (dataset): %s", code, a.stderr.String())
			}
			if a.stdout.Len() != 0 {
				t.Errorf("o comando descreveu um dataset a partir de resposta ilegítima: %q", a.stdout.String())
			}
			if !strings.Contains(a.stderr.String(), strconv.Itoa(len(c.datasets))+" datasets") {
				t.Errorf("a mensagem não diz quantos datasets vieram:\n%s", a.stderr.String())
			}
		})
	}
}

// A tabela de exit codes é a MESMA do `query --server`, pelo mesmo argumento:
// um script que consulta o status e decide pelo $? não pode precisar de uma
// segunda tabela.
func TestOsComandosDeLeituraTraduzemARecusaDoServidor(t *testing.T) {
	a := serverWithDataInAr(t, 5)

	t.Run("dataset desconhecido", func(t *testing.T) {
		code := a.run("inspect", "--server", a.endereco(), "fantasma")
		if code != 1 {
			t.Fatalf("exit = %d, quer 1", code)
		}
		if !strings.Contains(a.stderr.String(), "fantasma") {
			t.Errorf("a mensagem não nomeia o dataset pedido:\n%s", a.stderr.String())
		}
		if a.stdout.Len() != 0 {
			t.Errorf("recusa não pode escrever no stdout: %q", a.stdout.String())
		}
	})

	t.Run("servidor inalcançável", func(t *testing.T) {
		code := a.run("status", "--server", "127.0.0.1:1")
		if code != 1 {
			t.Fatalf("exit = %d, quer 1", code)
		}
		if !strings.Contains(a.stderr.String(), "127.0.0.1:1") {
			t.Errorf("a mensagem não nomeia o endereço tentado:\n%s", a.stderr.String())
		}
	})
}

// Sem --server não há para onde perguntar: o comando não tem caminho local por
// decisão (§96 + débito 54), e o erro precisa dizer isso em vez de deixar o
// operador procurando um --config que não existe.
func TestOsComandosDeLeituraExigemOServidor(t *testing.T) {
	a := newEnvRemote(t, 100000)

	for _, args := range [][]string{
		{"status"},
		{"datasets"},
		{"inspect", "radius_sessions"},
	} {
		if code := a.run(args...); code != 1 {
			t.Errorf("%v: exit = %d, quer 1", args, code)
		}
		if !strings.Contains(a.stderr.String(), "--server") {
			t.Errorf("%v: a mensagem não ensina o caminho:\n%s", args, a.stderr.String())
		}
	}

	// `inspect` sem dataset não tem o que inspecionar, e adivinhar qual seria
	// um comando diferente.
	if code := a.run("inspect", "--server", a.endereco()); code != 1 {
		t.Errorf("inspect sem dataset: exit = %d, quer 1", code)
	}
}

// Revisão da B6, Important 2: a paridade byte a byte "sem --user, sem
// Authorization" era só argumento estrutural (remoteCredential{} vazio) e
// unit de caixa branca (TestAplicaCredencialSoDefineBasicComUsuario,
// credential_internal_test.go). Aqui o oráculo é o HEADER que chega pela
// rede de verdade: um servidor de mentira captura o request de um `status
// --server` SEM nenhuma credencial e afirma Authorization == "".
func TestStatusSemUserNuncaEnviaHeaderAuthorization(t *testing.T) {
	var recebida bool
	var authorization string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recebida = true
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", protocol.ContentTypeJSON)
		if err := json.NewEncoder(w).Encode(protocol.Stats{}); err != nil {
			t.Errorf("escrevendo a resposta de mentira: %v", err)
		}
	}))
	defer srv.Close()

	a := newEnv(t)
	if code := a.run("status", "--server", srv.URL); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	if !recebida {
		t.Fatal("o servidor de mentira nunca recebeu a requisição")
	}
	if authorization != "" {
		t.Fatalf("Authorization presente sem --user: %q", authorization)
	}
}

// O texto de uso é o único lugar em que o operador descobre que os comandos
// existem. Ele já esqueceu o `--server` do `query` uma vez; aqui a omissão
// custaria os três comandos desta tarefa.
func TestOUsoAnunciaOsTresComandosDeLeitura(t *testing.T) {
	a := newEnv(t)
	if code := a.run("--help"); code != 0 {
		t.Fatalf("--help saiu com %d", code)
	}
	for _, cmd := range []string{"nishidb status", "nishidb datasets", "nishidb inspect"} {
		if !strings.Contains(a.stdout.String(), cmd) {
			t.Errorf("o texto de uso não anuncia %q", cmd)
		}
	}
}
