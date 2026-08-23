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

	"github.com/nishisan-dev/nishidb/internal/protocolo"
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

// campoDaSaida devolve o valor da linha rotulada, no formato de duas colunas
// que os comandos usam.
//
// A separação é por DOIS espaços porque é o que o padding do tabwriter
// garante: rótulo com espaço simples dentro ("carga do primário") continua
// inteiro, e o corte cai onde a coluna de fato começa.
func campoDaSaida(t *testing.T, saida, rotulo string) string {
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

// celulasDaLinha corta a linha nas colunas em que o tabwriter a escreveu.
//
// O corte é pelos DOIS espaços do padding, e não por espaço simples, porque a
// CÉLULA é a unidade de quem confere valor posicional: `strings.Contains` na
// linha inteira não distingue "30s" na coluna certa de "30s" em qualquer outra,
// e é cego para duas colunas trocadas de lugar.
//
// O argumento está escrito aqui desde a primeira rodada do §96, e valia só para
// a ÚLTIMA coluna de cada tabela — o resto da conferência era Contains na linha
// inteira. O buraco tinha tamanho: dois mutantes de troca de coluna
// (COLUNAS↔MAXROWS no `datasets`, ALIAS↔TIPO no `inspect`) atravessavam a suíte
// inteira sem acender nada. Agora o corte alimenta a tabela toda, pelo
// tabelaDaSaida.
func celulasDaLinha(l string) []string {
	var out []string
	for _, c := range strings.Split(strings.TrimSpace(l), "  ") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}

// tabelaDaSaida devolve o CABEÇALHO e as linhas de dados da tabela que começa
// no rótulo dado, já cortados em células.
//
// O cabeçalho volta junto porque é dele que sai a POSIÇÃO de cada rótulo, e é
// isso que separa duas coisas que um índice fixo no teste confundiria: mover
// uma coluna de lugar DE PROPÓSITO — cabeçalho e valores juntos — continua
// legítimo e não pode reprovar; deixar o valor sob o rótulo do vizinho é o
// defeito. O oráculo destes comandos é "o número está sob o rótulo dele", que é
// exatamente a pergunta que a suíte não sabia fazer.
//
// A tabela termina na primeira linha em branco ou no fim da saída. Nos dois
// comandos ela é hoje o último bloco; parar no branco é o que impede que uma
// seção acrescentada depois escorra para dentro das linhas e passe por dado.
func tabelaDaSaida(t *testing.T, saida, primeiroRotulo string) (cabecalho []string, linhas [][]string) {
	t.Helper()
	todas := strings.Split(saida, "\n")
	for i, l := range todas {
		c := celulasDaLinha(l)
		if len(c) == 0 || c[0] != primeiroRotulo {
			continue
		}
		for _, resto := range todas[i+1:] {
			if strings.TrimSpace(resto) == "" {
				break
			}
			linhas = append(linhas, celulasDaLinha(resto))
		}
		return c, linhas
	}
	t.Fatalf("a saída não tem a tabela que começa em %q:\n%s", primeiroRotulo, saida)
	return nil, nil
}

// linhaDaTabela devolve a linha cuja PRIMEIRA célula é a chave pedida — é como
// se lê uma tabela de colunas, no `datasets` e no `inspect`.
//
// Morrer aqui é a forma que o SUMIÇO de uma linha tem de aparecer. Um comando
// que escrevesse o nome do primeiro dataset em toda linha duplicaria o primeiro
// e faria os demais desaparecerem, e é procurando a linha de CADA dataset que o
// teste descobre isso: contar as linhas, sozinho, não descobriria.
func linhaDaTabela(t *testing.T, linhas [][]string, chave string) []string {
	t.Helper()
	for _, l := range linhas {
		if len(l) > 0 && l[0] == chave {
			return l
		}
	}
	t.Fatalf("a tabela não tem a linha de %q:\n%v", chave, linhas)
	return nil
}

// celulaSobORotulo devolve a célula que a linha escreveu sob o rótulo do
// cabeçalho.
func celulaSobORotulo(t *testing.T, cabecalho, linha []string, rotulo string) string {
	t.Helper()
	for i, r := range cabecalho {
		if r != rotulo {
			continue
		}
		if i >= len(linha) {
			t.Fatalf("a linha %v não tem célula sob %q (cabeçalho %v)", linha, rotulo, cabecalho)
		}
		return linha[i]
	}
	t.Fatalf("o cabeçalho %v não tem a coluna %q", cabecalho, rotulo)
	return ""
}

// conferePorCelula compara a linha da tabela com o oráculo CÉLULA A CÉLULA, na
// ordem em que o comando escreveu o cabeçalho.
//
// Todo rótulo do cabeçalho tem de ter oráculo: coluna sem conferência nenhuma é
// o buraco por onde os mutantes de troca passaram, e um mapa que apenas
// ignorasse o rótulo desconhecido reabriria o buraco no dia em que uma coluna
// nova nascesse.
//
// `aparte` nomeia as colunas cujo oráculo NÃO é igualdade de texto — hoje só a
// do prazo, que se confere de ida e volta (ver prazoDaSaida). Declarar a
// exceção pelo nome é o que a distingue do esquecimento.
func conferePorCelula(t *testing.T, contexto string, cabecalho, linha []string, esperado map[string]string, aparte ...string) {
	t.Helper()
	if len(linha) != len(cabecalho) {
		t.Fatalf("%s: a linha saiu com %d células e o cabeçalho %v tem %d: %v",
			contexto, len(linha), cabecalho, len(cabecalho), linha)
	}
	if len(esperado)+len(aparte) != len(cabecalho) {
		t.Fatalf("%s: o cabeçalho %v tem %d colunas e o oráculo cobre %d (%d à parte): ou uma coluna "+
			"nasceu sem conferência, ou o oráculo confere uma que já não existe",
			contexto, cabecalho, len(cabecalho), len(esperado), len(aparte))
	}
	for i, rotulo := range cabecalho {
		if slices.Contains(aparte, rotulo) {
			continue
		}
		quer, ok := esperado[rotulo]
		if !ok {
			t.Fatalf("%s: a coluna %q não tem oráculo; o cabeçalho é %v", contexto, rotulo, cabecalho)
		}
		if linha[i] != quer {
			t.Errorf("%s: sob %s a saída traz %q e o servidor respondeu %q (linha inteira: %v)",
				contexto, rotulo, linha[i], quer, linha)
		}
	}
}

// exigeColunasDistinguiveis é a guarda `vistos` dos seis contadores do bloco
// `processo`, aplicada à TABELA.
//
// O argumento é o mesmo, com um grau a mais: lá, dois contadores com o MESMO
// valor tornam os dois rótulos indistinguíveis e o teste passa a aprovar um
// comando que lê o campo do vizinho; aqui basta que UMA linha separe duas
// colunas para que trocá-las apareça, então o que se exige é que, para cada par
// de colunas, exista pelo menos uma linha em que elas difiram. Se o fixture
// deixar de separá-las, é o FIXTURE que precisa mudar — não o oráculo.
//
// As colunas de `aparte` ficam de fora porque o oráculo delas não é texto. Não
// é buraco: trocar a coluna do prazo com uma vizinha aparece do outro lado da
// troca, na coluna que recebeu um prazo onde deveria haver um número.
func exigeColunasDistinguiveis(t *testing.T, cabecalho []string, esperado []map[string]string, aparte ...string) {
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
			for _, linha := range esperado {
				if linha[rotulos[i]] != linha[rotulos[j]] {
					separou = true
					break
				}
			}
			if !separou {
				t.Fatalf("as colunas %q e %q valem o mesmo em TODA linha do fixture (%d linhas): trocar as "+
					"duas fontes não mudaria uma vírgula da saída, e o teste aprovaria o comando trocado",
					rotulos[i], rotulos[j], len(esperado))
			}
		}
	}
}

// rotuloDoPrazo é a coluna do `datasets` cujo oráculo é de ida e volta, e não
// de igualdade de texto — ver prazoDaSaida.
const rotuloDoPrazo = "TIMEOUT"

// prazoDaSaida devolve a duração que o comando escreveu, RECUPERADA do texto.
//
// A conferência é de ida e volta de propósito: o oráculo não é "o comando
// imprimiu a mesma string que eu formataria", que só afirmaria que dois
// trechos de código chamam o mesmo Sprintf, e sim que do texto para humano se
// recupera o MESMO prazo que a rota respondeu em milissegundos. É o argumento
// do bytesLegiveis aplicado ao tempo: número humanizado do qual não se recupera
// o valor esconde a diferença que o operador veio medir.
func prazoDaSaida(t *testing.T, texto string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(texto)
	if err != nil {
		t.Fatalf("o comando escreveu %q, que não é uma duração: %v", texto, err)
	}
	return d
}

// estatisticas lê o /v1/stats pela porta, para servir de oráculo ao comando.
func estatisticas(t *testing.T, a *ambienteRemoto) protocolo.Estatisticas {
	t.Helper()
	var e protocolo.Estatisticas
	leJSONDaPorta(t, a, "/v1/stats", &e)
	return e
}

func metadados(t *testing.T, a *ambienteRemoto, consulta string) protocolo.Metadados {
	t.Helper()
	var m protocolo.Metadados
	leJSONDaPorta(t, a, "/v1/metadata"+consulta, &m)
	return m
}

func leJSONDaPorta(t *testing.T, a *ambienteRemoto, caminho string, destino any) {
	t.Helper()
	resp, err := http.Get("http://" + a.endereco() + caminho)
	if err != nil {
		t.Fatalf("GET %s: %v", caminho, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("fechar o corpo de %s: %v", caminho, err)
		}
	}()
	corpo, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("lendo %s: %v", caminho, err)
	}
	if err := json.Unmarshal(corpo, destino); err != nil {
		t.Fatalf("decodificando %s: %v\n%s", caminho, err, corpo)
	}
}

// servidorComDadosNoAr monta o ambiente remoto, ingere e sobe o servidor. A
// ingestão acontece ANTES da subida porque o flock é exclusivo: é a mesma
// ordem que o operador segue.
func servidorComDadosNoAr(t *testing.T, eventos int) *ambienteRemoto {
	t.Helper()
	return sobeComEntrada(t, novoAmbienteRemoto(t, 100000), corpus(t, eventos))
}

// sobeComEntrada é o de baixo: ingere a entrada que o teste montou e sobe o
// servidor sobre ela.
//
// A separação existe porque o corpus do §12 não produz sozinho três fixtures de
// que esta suíte precisa — um teto de linhas baixo o bastante para truncar, uma
// coluna AUTO, e chaves em número suficiente para a carga do primário sair de
// zero. Cada um deles é uma linha de entrada diferente, não um segundo helper.
func sobeComEntrada(t *testing.T, a *ambienteRemoto, entrada string) *ambienteRemoto {
	t.Helper()
	if code := a.rodaComEntrada(entrada, "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.sobeServidor(t)
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
func acrescentaAoStorage(t *testing.T, a *ambienteRemoto, trecho string) {
	t.Helper()
	yml, err := os.ReadFile(a.config)
	if err != nil {
		t.Fatal(err)
	}
	const alvo = "storage:\n"
	if !strings.Contains(string(yml), alvo) {
		t.Fatalf("o template do ambiente remoto não tem mais o bloco %q:\n%s", alvo, yml)
	}
	novo := strings.Replace(string(yml), alvo, alvo+trecho, 1)
	if err := os.WriteFile(a.config, []byte(novo), 0o644); err != nil {
		t.Fatal(err)
	}
}

// desligaACompaction acrescenta `storage.compaction.enabled: false` ao YAML do
// ambiente. Subir com a compaction ligada e afirmar o contrário seria o teste
// mentindo sobre o próprio fixture, e é por isso que o acrescentaAoStorage
// morre quando não encontra onde inserir.
func desligaACompaction(t *testing.T, a *ambienteRemoto) {
	t.Helper()
	acrescentaAoStorage(t, a, "  compaction:\n    enabled: false\n")
}

// afinaOStorageParaCompactar deixa o ambiente com segmento PEQUENO e a política
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
func afinaOStorageParaCompactar(t *testing.T, a *ambienteRemoto) {
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

// updatesSobreChavesQuentesEFrias monta a entrada que dá à compaction o que
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
func updatesSobreChavesQuentesEFrias(quentes, rodadas, friasPorRodada int) string {
	var sb strings.Builder
	fria := 0
	for r := 1; r <= rodadas; r++ {
		for k := 0; k < quentes; k++ {
			fmt.Fprintf(&sb, `{"id":"Q%04d","_SEQ":%d,"Acct-Status-Type":"Stop","User-Name":"u%04d","NAS-Identifier":"nas-%d"}`+"\n",
				k, r, k, r%7)
		}
		for i := 0; i < friasPorRodada; i++ {
			fmt.Fprintf(&sb, `{"id":"F%06d","_SEQ":1,"Acct-Status-Type":"Start","User-Name":"f%06d","NAS-Identifier":"nas-fria"}`+"\n",
				fria, fria)
			fria++
		}
	}
	return sb.String()
}

// nomeDoSegundoDataset e specDoSegundoDataset são o outro dataset do servidor,
// e existem por causa do `datasets`.
//
// O comando cuja única razão de existir é listar O QUE ESTE SERVIDOR SERVE
// estava sendo exercitado contra um servidor de UM dataset, e um servidor de um
// dataset não distingue "lista todos" de "imprime o primeiro N vezes": com uma
// linha só, escrever o nome de m.Datasets[0] em todas elas é indistinguível de
// escrever o nome de cada uma. É a mesma família da guarda `vistos` dos seis
// contadores do bloco `processo` — dois valores iguais tornam dois rótulos
// indistinguíveis —, agora na dimensão da LINHA.
//
// Ele diverge do fixture do §12 em TODAS as colunas da tabela (nome, chave,
// número de colunas, teto de linhas e prazo), e isso não é enfeite: é o que o
// exigeColunasDistinguiveis cobra para que trocar duas colunas de lugar apareça.
//
// Não declara `source`, então nenhum consumer sobe para ele (§12 + D15) e o
// dataset nasce vazio — o que basta, porque o `datasets` descreve SCHEMA e
// limites, não conteúdo.
const nomeDoSegundoDataset = "auditoria_acessos"

const specDoSegundoDataset = `version: 1
name: auditoria_acessos
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
      index: bitmap
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
func acrescentaOSegundoDataset(t *testing.T, a *ambienteRemoto) {
	t.Helper()
	if !strings.Contains(specDoSegundoDataset, "name: "+nomeDoSegundoDataset+"\n") {
		t.Fatalf("a spec do segundo dataset não declara name: %s", nomeDoSegundoDataset)
	}
	caminho := filepath.Join(a.dir, "datasets", nomeDoSegundoDataset+".yaml")
	if err := os.WriteFile(caminho, []byte(specDoSegundoDataset), 0o644); err != nil {
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
	a := servidorComDadosNoAr(t, 50)

	// Tráfego ANTES do status, para que os três números da linha de consultas
	// deixem de ser zero. Enquanto eram, trocar Total por Errors era invisível
	// — o oráculo comparava "0 (0 ativas, 0 com erro)" consigo mesmo, e a
	// asserção parecia existir sem exercer nada.
	for i := 0; i < 2; i++ {
		if code := a.roda("query", "--server", a.endereco(),
			"--dataset", "radius_sessions",
			"SELECT id FROM radius_sessions WHERE id = 'chave que nao existe'"); code != 0 {
			t.Fatalf("consulta de aquecimento saiu %d: %s", code, a.stderr.String())
		}
	}
	a.stdout.Reset()
	a.stderr.Reset()

	if code := a.roda("status", "--server", a.endereco()); code != 0 {
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
		if got := campoDaSaida(t, saida, c.rotulo); got != c.quer {
			t.Errorf("%s = %q na saída e %q no /v1/stats", c.rotulo, got, c.quer)
		}
	}

	// O disco é o que este milestone existe para tornar visível (§86): o
	// comando tem de trazer o número EXATO da rota, e não uma aproximação
	// humanizada da qual não se recupera o valor.
	if got := campoDaSaida(t, saida, "disco"); !strings.HasPrefix(got, strconv.FormatInt(ds.Storage.Bytes, 10)+" B") {
		t.Errorf("disco = %q, queria começar com %d B", got, ds.Storage.Bytes)
	}
	if got := campoDaSaida(t, saida, "garbage (>=)"); !strings.HasPrefix(got, strconv.FormatUint(ds.Storage.GarbageBytes, 10)+" B") {
		t.Errorf("garbage = %q, queria começar com %d B", got, ds.Storage.GarbageBytes)
	}

	// A ingestão do §96 sai pelo vocabulário do §94, o mesmo do /v1/health:
	// duas palavras diferentes para o mesmo estado obrigariam o operador a
	// aprender as duas.
	if got := campoDaSaida(t, saida, "ingestão"); !strings.HasPrefix(got, ds.Ingestion.State) {
		t.Errorf("ingestão = %q, queria começar com %q", got, ds.Ingestion.State)
	}

	// A linha de consultas põe TRÊS números na MESMA frase, e era a última do
	// bloco sem oráculo: trocar Total por Errors deixava a suíte inteira verde.
	// A frase única é justamente o que torna a troca fácil de cometer e difícil
	// de enxergar — a saída continua plausível.
	querConsultas := fmt.Sprintf("%d (%d ativas, %d com erro)",
		ds.Query.Total, ds.Query.Active, ds.Query.Errors)
	if got := campoDaSaida(t, saida, "consultas"); got != querConsultas {
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
	a := servidorComDadosNoAr(t, 20)
	esperaCompactionNoAr(t, a)

	if code := a.roda("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()
	ds := estatisticas(t, a).Datasets[0]

	if !ds.Storage.Compaction.Running {
		t.Fatal("o /v1/stats reporta a compaction parada logo depois de ela subir")
	}
	if got := campoDaSaida(t, saida, "compaction"); got != "ligada" {
		t.Errorf("compaction = %q, queria dizer que o laço está de pé", got)
	}
	if got := campoDaSaida(t, saida, "bytes lidos"); !strings.HasPrefix(got, strconv.FormatInt(ds.Storage.Compaction.BytesRead, 10)+" B") {
		t.Errorf("bytes lidos = %q, queria %d B", got, ds.Storage.Compaction.BytesRead)
	}
	if got := campoDaSaida(t, saida, "bytes escritos"); !strings.HasPrefix(got, strconv.FormatInt(ds.Storage.Compaction.BytesWritten, 10)+" B") {
		t.Errorf("bytes escritos = %q, queria %d B", got, ds.Storage.Compaction.BytesWritten)
	}
}

// TestStatusMostraACompactionDesligada é a outra metade do par acima, e sem
// ela o teste de cima não prova a palavra: `return "ligada"` no corpo do
// estadoDaCompaction passa por todo oráculo que só sobe com a chave no
// default, e o `esperaCompactionNoAr` está lá justamente para garantir que o
// laço esteja de pé quando a saída for lida.
//
// O ramo que falta é o que o godoc da função nomeia como o único que importa:
// "desligada" com a chave ligada no YAML é a falha sticky do writer (§14.14),
// em que a goroutine parou sozinha e o disco voltou a crescer com o processo de
// pé. Essa falha não se provoca por fora sem avariar o writer; a chave em false
// produz o MESMO estado observável — laço parado, servidor no ar — e é o que
// separa a palavra de um literal.
func TestStatusMostraACompactionDesligada(t *testing.T) {
	a := novoAmbienteRemoto(t, 100000)
	desligaACompaction(t, a)
	sobeComEntrada(t, a, corpus(t, 20))

	if code := a.roda("status", "--server", a.endereco()); code != 0 {
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
	if got := campoDaSaida(t, saida, "compaction"); got != "desligada" {
		t.Errorf("compaction = %q com a chave em false; queria \"desligada\", que é a palavra "+
			"que o operador procura quando o disco cresce com o processo de pé (§14.14)", got)
	}
}

// esperaCompactionNoAr bloqueia até o /v1/stats reportar o laço de pé.
//
// A subida põe o HTTP no ar ANTES de ligar a compaction (§51), então há uma
// janela em que o /v1/health já responde e o laço ainda não subiu. Esperar pela
// CONDIÇÃO fecha a janela sem dormir — dormir esconderia tanto a lentidão
// quanto a ausência de progresso.
func esperaCompactionNoAr(t *testing.T, a *ambienteRemoto) {
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
	a := novoAmbienteRemoto(t, 100000)
	afinaOStorageParaCompactar(t, a)
	sobeComEntrada(t, a, updatesSobreChavesQuentesEFrias(50, 200, 5))

	saida, st := statusComOsCamposDeDiscoSeparados(t, a)
	for _, c := range camposDeDiscoDoStatus(st) {
		quer := strconv.FormatInt(c.valor, 10)
		got := campoDaSaida(t, saida, c.rotulo)
		if c.emBytes {
			// O byte EXATO vem primeiro no bytesLegiveis, e é ele que se
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

// campoDeDisco é um dos números que o §96 publica sob rótulo próprio no bloco
// do dataset. `emBytes` marca os que saem pelo bytesLegiveis, cuja célula
// começa pelo valor exato e continua com a grandeza para humano.
type campoDeDisco struct {
	rotulo  string
	valor   int64
	emBytes bool
}

// camposDeDiscoDoStatus é a lista que a espera e a conferência compartilham —
// as duas precisam falar dos MESMOS quatro números, senão o teste espera por uma
// condição e afirma outra.
func camposDeDiscoDoStatus(st protocolo.StorageStats) []campoDeDisco {
	return []campoDeDisco{
		{"segmentos", int64(st.Segments), false},
		{"corrente", int64(st.CurrentSegment), false},
		{"bytes lidos", st.Compaction.BytesRead, true},
		{"bytes escritos", st.Compaction.BytesWritten, true},
	}
}

// colisaoEntreOsCamposDeDisco descreve a primeira razão pela qual os quatro
// números ainda não distinguem os quatro rótulos, ou devolve "" quando
// distinguem.
//
// É a guarda `vistos` dos seis contadores do bloco `processo`, na forma de
// predicado: dois números iguais tornam os dois rótulos indistinguíveis, e o
// teste passaria a aprovar um comando que lê o campo do vizinho. Ela é
// predicado, e não asserção imediata, porque aqui o fixture leva algumas
// centenas de milissegundos para separá-los — a compaction precisa aposentar o
// primeiro segmento —, e afirmar antes disso seria reprovar o comando certo.
//
// Os dois contadores de bytes ainda são exigidos NÃO-ZERO à parte. Zero é o
// estado em que a suíte anterior os lia, e zero não é só igual ao vizinho: é
// igual a tudo o que ainda não aconteceu.
func colisaoEntreOsCamposDeDisco(st protocolo.StorageStats) string {
	campos := camposDeDiscoDoStatus(st)
	for _, c := range campos {
		if c.emBytes && c.valor == 0 {
			return fmt.Sprintf("%q ainda vale 0: nenhuma passada de compaction concluiu", c.rotulo)
		}
	}
	vistos := map[int64]string{}
	for _, c := range campos {
		if outro, ok := vistos[c.valor]; ok {
			return fmt.Sprintf("%q e %q valem %d: com dois números iguais o teste não distingue os rótulos",
				outro, c.rotulo, c.valor)
		}
		vistos[c.valor] = c.rotulo
	}
	return ""
}

// statusComOsCamposDeDiscoSeparados roda o `status` ENTRE duas leituras do
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
// Esperar pela CONDIÇÃO, e não por tempo, é a mesma regra do esperaCompactionNoAr:
// dormir um prazo fixo esconderia tanto a lentidão quanto a ausência de
// progresso. E o prazo que expira morre dizendo QUAL par ainda colide, porque a
// resposta certa a ele é mexer no fixture, não no oráculo.
func statusComOsCamposDeDiscoSeparados(t *testing.T, a *ambienteRemoto) (string, protocolo.StorageStats) {
	t.Helper()
	motivo := "o /v1/stats não chegou a ser lido"
	prazo := time.Now().Add(30 * time.Second)
	for time.Now().Before(prazo) {
		antes := storageDoDataset(t, a)
		if m := colisaoEntreOsCamposDeDisco(antes); m != "" {
			motivo = m
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if code := a.roda("status", "--server", a.endereco()); code != 0 {
			t.Fatalf("status: exit = %d: %s", code, a.stderr.String())
		}
		saida := a.stdout.String()
		if storageDoDataset(t, a) == antes {
			return saida, antes
		}
		motivo = "a compaction moveu os números DURANTE o comando"
	}
	t.Fatalf("em 30s o fixture não pôs os quatro números do disco em valores distintos e estáveis: %s", motivo)
	return "", protocolo.StorageStats{}
}

// storageDoDataset é o bloco de storage do dataset único do ambiente, lido pela
// porta. Morre quando o servidor traz outro número de datasets: um oráculo que
// pegasse o primeiro de vários estaria conferindo a saída de um contra os
// números de outro.
func storageDoDataset(t *testing.T, a *ambienteRemoto) protocolo.StorageStats {
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
// diferente de vezes — 2 full scans, 3 point lookups, 4 bitmap scans, 6
// uniões, 5 linhas emitidas e 1 truncamento.
//
// Os planos são os do §63: chave no WHERE dá point lookup, campo bitmap dá
// bitmap scan, OR de dois termos indexados dá união, e campo sem índice cai em
// full scan. Todos batem em valor que não existe, para que a única emissão do
// fixture seja a da consulta sem WHERE — que estoura o teto de 5 linhas do
// dataset e produz o truncamento.
func TestStatusPublicaOsContadoresDoMotorDoProcesso(t *testing.T) {
	a := sobeComEntrada(t, novoAmbienteRemoto(t, 5), corpus(t, 50))

	consulta := func(vezes int, quer int, sql string) {
		t.Helper()
		for i := 0; i < vezes; i++ {
			if code := a.roda("query", "--server", a.endereco(),
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

	if code := a.roda("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	q := estatisticas(t, a).Process.Query
	campos := []struct {
		rotulo string
		valor  int64
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
	for _, c := range campos {
		if outro, ok := vistos[c.valor]; ok {
			t.Fatalf("%q e %q valem %d: com dois contadores iguais o teste não distingue os rótulos",
				outro, c.rotulo, c.valor)
		}
		vistos[c.valor] = c.rotulo
	}

	for _, c := range campos {
		if got := campoDaSaida(t, saida, c.rotulo); got != strconv.FormatInt(c.valor, 10) {
			t.Errorf("%s = %q na saída e %d no /v1/stats", c.rotulo, got, c.valor)
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
	a := sobeComEntrada(t, novoAmbienteRemoto(t, 100000), chavesDistintas(12000))

	if code := a.roda("status", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	ds := estatisticas(t, a).Datasets[0]
	quer := strconv.FormatFloat(ds.Storage.PrimaryLoadFactor, 'f', 2, 64)
	if quer == "0.00" {
		t.Fatalf("a carga saiu 0.00 com %d rows (fator %v): neste fixture o campo já não se "+
			"distingue de uma constante", ds.Storage.Rows, ds.Storage.PrimaryLoadFactor)
	}
	if got := campoDaSaida(t, saida, "carga do primário"); got != quer {
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
	a := novoAmbienteRemoto(t, 100000)
	acrescentaOSegundoDataset(t, a)
	sobeComEntrada(t, a, corpus(t, 10))

	if code := a.roda("datasets", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	m := metadados(t, a, "")
	if len(m.Datasets) < 2 {
		t.Fatalf("o /v1/metadata trouxe %d datasets: com menos de dois o comando não é exercitado "+
			"naquilo que ele existe para fazer", len(m.Datasets))
	}

	cabecalho, linhas := tabelaDaSaida(t, saida, "DATASET")
	if len(linhas) != len(m.Datasets) {
		t.Fatalf("a tabela saiu com %d linhas para %d datasets servidos:\n%s", len(linhas), len(m.Datasets), saida)
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
	exigeColunasDistinguiveis(t, cabecalho, oraculos, rotuloDoPrazo)

	for i, d := range m.Datasets {
		linha := linhaDaTabela(t, linhas, d.Name)
		conferePorCelula(t, "dataset "+d.Name, cabecalho, linha, oraculos[i], rotuloDoPrazo)

		// A coluna TIMEOUT é o prazo EFETIVO da consulta escrito para humano, e
		// é por ele que o operador decide se a query que ele vai mandar cabe no
		// dataset. O oráculo dela é de IDA E VOLTA (ver prazoDaSaida), mas a
		// célula continua sendo a da coluna: um "0s" fixo — ou o prazo na
		// coluna errada — diria ao operador que toda consulta morre na largada.
		celula := celulaSobORotulo(t, cabecalho, linha, rotuloDoPrazo)
		if got := prazoDaSaida(t, celula); got != time.Duration(d.TimeoutMs)*time.Millisecond {
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
	a := sobeComEntrada(t, novoAmbienteRemoto(t, 100000),
		corpus(t, 10)+`{"id":"S-auto","_SEQ":1,"Campo-Fora-Do-Schema":"presente"}`+"\n")

	if code := a.roda("inspect", "--server", a.endereco(), "radius_sessions"); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	m := metadados(t, a, "?dataset=radius_sessions")
	if len(m.Datasets) != 1 {
		t.Fatalf("o /v1/metadata filtrado trouxe %d datasets", len(m.Datasets))
	}
	d := m.Datasets[0]
	if campoDaSaida(t, saida, "key") != d.Key {
		t.Errorf("key = %q, queria %q", campoDaSaida(t, saida, "key"), d.Key)
	}
	// O contador de colunas era o único campo do bloco sem oráculo, e o
	// inspect publica esse número DUAS vezes — aqui e como número de linhas da
	// tabela. Alimentá-lo com d.MaxRows deixava a suíte verde: é a mesma
	// família que o resto deste arquivo fecha, valor sob o rótulo do vizinho.
	if got, quer := campoDaSaida(t, saida, "colunas"), strconv.Itoa(len(d.Columns)); got != quer {
		t.Errorf("colunas = %q, queria %q (o dataset tem %d colunas e maxRows %d)",
			got, quer, len(d.Columns), d.MaxRows)
	}
	if campoDaSaida(t, saida, "maxRows") != strconv.Itoa(d.MaxRows) {
		t.Errorf("maxRows = %q, queria %d", campoDaSaida(t, saida, "maxRows"), d.MaxRows)
	}
	// O timeout é o outro limite EFETIVO, e sai pelo mesmo argumento do
	// maxRows: quem consulta precisa do prazo que VAI VALER. Escrito para
	// humano, ele tem de voltar ao milissegundo que a rota respondeu.
	if d.TimeoutMs == 0 {
		t.Fatal("o /v1/metadata respondeu timeout 0: contra ele um \"0s\" fixo passaria")
	}
	if got := prazoDaSaida(t, campoDaSaida(t, saida, "timeout")); got != time.Duration(d.TimeoutMs)*time.Millisecond {
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
	cabecalho, linhas := tabelaDaSaida(t, saida, "COLUNA")
	if len(linhas) != len(d.Columns) {
		t.Fatalf("a tabela saiu com %d linhas para %d colunas do /v1/metadata:\n%s", len(linhas), len(d.Columns), saida)
	}

	// A coluna AUTO diz se o TIPO daquela coluna foi declarado no YAML ou fixado
	// pela primeira observação — e é o que explica ao operador por que um campo
	// aparece como "unknown", ou por que ele não pode receber índice. O ÍNDICE
	// sai com o nome do YAML pelo mesmo motivo: publicá-lo como "sim/não"
	// apagaria a diferença entre bitmap e hash, que decide o plano.
	var auto int
	oraculos := make([]map[string]string, 0, len(d.Columns))
	for _, c := range d.Columns {
		alias := "-"
		if c.Alias != nil {
			alias = *c.Alias
		}
		marca := "-"
		if c.Auto {
			marca, auto = "sim", auto+1
		}
		oraculos = append(oraculos, map[string]string{
			"COLUNA": c.Name,
			"ALIAS":  alias,
			"TIPO":   c.Type,
			"JDBC":   c.JDBC,
			"ÍNDICE": c.Index,
			"AUTO":   marca,
		})
	}
	if auto == 0 {
		t.Fatal("nenhuma coluna auto no fixture: a coluna AUTO seria um traço fixo e o teste não provaria nada")
	}
	exigeColunasDistinguiveis(t, cabecalho, oraculos)

	for i, c := range d.Columns {
		conferePorCelula(t, "coluna "+c.Name, cabecalho, linhaDaTabela(t, linhas, c.Name), oraculos[i])
	}

	// Os dois índices declarados pelo fixture do §12, conferidos na CÉLULA do
	// índice: é a diferença entre "a palavra bitmap aparece em algum lugar da
	// linha" e "o índice desta coluna é bitmap".
	for coluna, quer := range map[string]string{"Acct-Status-Type": "bitmap", "NAS-IP-Address": "hash"} {
		if got := celulaSobORotulo(t, cabecalho, linhaDaTabela(t, linhas, coluna), "ÍNDICE"); got != quer {
			t.Errorf("o índice de %s saiu %q, queria %q", coluna, got, quer)
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
		nome     string
		datasets []protocolo.DatasetMeta
	}{
		{"nenhum", nil},
		{"dois", []protocolo.DatasetMeta{{Name: "radius_sessions"}, {Name: "outro"}}},
	}
	for _, c := range casos {
		t.Run(c.nome, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
				if err := json.NewEncoder(w).Encode(protocolo.Metadados{Datasets: c.datasets}); err != nil {
					t.Errorf("escrevendo a resposta de mentira: %v", err)
				}
			}))
			defer srv.Close()

			a := novoAmbiente(t)
			if code := a.roda("inspect", "--server", srv.URL, "radius_sessions"); code != 3 {
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
	a := servidorComDadosNoAr(t, 5)

	t.Run("dataset desconhecido", func(t *testing.T) {
		code := a.roda("inspect", "--server", a.endereco(), "fantasma")
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
		code := a.roda("status", "--server", "127.0.0.1:1")
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
	a := novoAmbienteRemoto(t, 100000)

	for _, args := range [][]string{
		{"status"},
		{"datasets"},
		{"inspect", "radius_sessions"},
	} {
		if code := a.roda(args...); code != 1 {
			t.Errorf("%v: exit = %d, quer 1", args, code)
		}
		if !strings.Contains(a.stderr.String(), "--server") {
			t.Errorf("%v: a mensagem não ensina o caminho:\n%s", args, a.stderr.String())
		}
	}

	// `inspect` sem dataset não tem o que inspecionar, e adivinhar qual seria
	// um comando diferente.
	if code := a.roda("inspect", "--server", a.endereco()); code != 1 {
		t.Errorf("inspect sem dataset: exit = %d, quer 1", code)
	}
}

// Revisão da B6, Important 2: a paridade byte a byte "sem --user, sem
// Authorization" era só argumento estrutural (credencialRemota{} vazio) e
// unit de caixa branca (TestAplicaCredencialSoDefineBasicComUsuario,
// credencial_internal_test.go). Aqui o oráculo é o HEADER que chega pela
// rede de verdade: um servidor de mentira captura o request de um `status
// --server` SEM nenhuma credencial e afirma Authorization == "".
func TestStatusSemUserNuncaEnviaHeaderAuthorization(t *testing.T) {
	var recebida bool
	var authorization string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recebida = true
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
		if err := json.NewEncoder(w).Encode(protocolo.Estatisticas{}); err != nil {
			t.Errorf("escrevendo a resposta de mentira: %v", err)
		}
	}))
	defer srv.Close()

	a := novoAmbiente(t)
	if code := a.roda("status", "--server", srv.URL); code != 0 {
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
	a := novoAmbiente(t)
	if code := a.roda("--help"); code != 0 {
		t.Fatalf("--help saiu com %d", code)
	}
	for _, cmd := range []string{"nishidb status", "nishidb datasets", "nishidb inspect"} {
		if !strings.Contains(a.stdout.String(), cmd) {
			t.Errorf("o texto de uso não anuncia %q", cmd)
		}
	}
}
