package cli_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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

// linhaDaSaida devolve a linha cuja primeira coluna é o valor pedido — é como
// se lê uma tabela de colunas, no `datasets` e no `inspect`.
func linhaDaSaida(t *testing.T, saida, primeiraColuna string) string {
	t.Helper()
	for _, l := range strings.Split(saida, "\n") {
		l = strings.TrimSpace(l)
		i := strings.Index(l, "  ")
		if i < 0 {
			continue
		}
		if strings.TrimSpace(l[:i]) == primeiraColuna {
			return l
		}
	}
	t.Fatalf("a saída não tem a linha de %q:\n%s", primeiraColuna, saida)
	return ""
}

// celulasDaLinha corta a linha nas colunas em que o tabwriter a escreveu.
//
// O corte é pelos DOIS espaços do padding, e não por espaço simples, porque a
// célula é o que interessa a quem confere valor posicional: a última coluna do
// `datasets` é o TIMEOUT e a última do `inspect` é o AUTO, e `strings.Contains`
// na linha inteira não distingue "30s" na coluna certa de "30s" em qualquer
// outra.
func celulasDaLinha(l string) []string {
	var out []string
	for _, c := range strings.Split(strings.TrimSpace(l), "  ") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}

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

// desligaACompaction acrescenta `storage.compaction.enabled: false` ao YAML do
// ambiente, ANTES de o servidor subir.
//
// Reescreve o arquivo em vez de manter um segundo template porque o template do
// ambiente remoto é um só: uma cópia dele aqui envelheceria calada no dia em
// que o de lá mudasse. Se o ponto de inserção sumir, o teste morre dizendo
// isso — que é melhor do que subir com a compaction ligada e afirmar o
// contrário.
func desligaACompaction(t *testing.T, a *ambienteRemoto) {
	t.Helper()
	yml, err := os.ReadFile(a.config)
	if err != nil {
		t.Fatal(err)
	}
	const alvo = "storage:\n"
	if !strings.Contains(string(yml), alvo) {
		t.Fatalf("o template do ambiente remoto não tem mais o bloco %q:\n%s", alvo, yml)
	}
	novo := strings.Replace(string(yml), alvo, alvo+"  compaction:\n    enabled: false\n", 1)
	if err := os.WriteFile(a.config, []byte(novo), 0o644); err != nil {
		t.Fatal(err)
	}
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

// TestStatusRemotoFuncionaComOServidorNoAr é o critério que separa este
// comando do débito 54: ele tem de responder COM o servidor de pé, que é
// justamente quando o flock barra o caminho local.
//
// Os números conferidos são os do §96 ("status") lidos contra o /v1/stats: se
// o comando inventasse um segundo caminho para chegar neles, a divergência
// apareceria aqui — que é a mesma doutrina do /metrics contra o JSON.
func TestStatusRemotoFuncionaComOServidorNoAr(t *testing.T) {
	a := servidorComDadosNoAr(t, 50)

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
func TestDatasetsListaOQueOServidorServe(t *testing.T) {
	a := servidorComDadosNoAr(t, 10)

	if code := a.roda("datasets", "--server", a.endereco()); code != 0 {
		t.Fatalf("exit = %d: %s", code, a.stderr.String())
	}
	saida := a.stdout.String()

	m := metadados(t, a, "")
	if len(m.Datasets) != 1 {
		t.Fatalf("o /v1/metadata trouxe %d datasets: o oráculo não se aplica", len(m.Datasets))
	}
	d := m.Datasets[0]

	linha := linhaDaSaida(t, saida, d.Name)
	for _, quer := range []string{
		d.Key,
		strconv.Itoa(len(d.Columns)),
		strconv.Itoa(d.MaxRows),
	} {
		if !strings.Contains(linha, quer) {
			t.Errorf("a linha do dataset não traz %q: %q", quer, linha)
		}
	}

	// A coluna TIMEOUT é o prazo EFETIVO da consulta escrito para humano, e é
	// por ele que o operador decide se a query que ele vai mandar cabe no
	// dataset. Conferi-la é conferir a célula: um "0s" fixo — ou o prazo na
	// coluna errada — diria ao operador que toda consulta morre na largada.
	celulas := celulasDaLinha(linha)
	if len(celulas) != 5 {
		t.Fatalf("a linha saiu com %d células, quer 5 (dataset, key, colunas, maxRows, timeout): %q", len(celulas), linha)
	}
	if d.TimeoutMs == 0 {
		t.Fatal("o /v1/metadata respondeu timeout 0: contra ele um \"0s\" fixo passaria")
	}
	if got := prazoDaSaida(t, celulas[4]); got != time.Duration(d.TimeoutMs)*time.Millisecond {
		t.Errorf("TIMEOUT = %q (%v) na saída e %d ms no /v1/metadata", celulas[4], got, d.TimeoutMs)
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

	// Toda coluna do metadata sai, e sai com os quatro atributos: uma tabela
	// que omitisse uma coluna mandaria o operador concluir que ela não existe.
	if len(d.Columns) < 10 {
		t.Fatalf("o fixture tem %d colunas: poucas para distinguir tabela de amostra", len(d.Columns))
	}
	var auto int
	for _, c := range d.Columns {
		linha := linhaDaSaida(t, saida, c.Name)
		if !strings.Contains(linha, c.Type) || !strings.Contains(linha, c.JDBC) || !strings.Contains(linha, c.Index) {
			t.Errorf("coluna %q sem tipo/jdbc/índice na saída: %q", c.Name, linha)
		}
		if c.Alias != nil && !strings.Contains(linha, *c.Alias) {
			t.Errorf("coluna %q sem o alias %q: %q", c.Name, *c.Alias, linha)
		}

		// A coluna AUTO diz se o TIPO daquela coluna foi declarado no YAML ou
		// fixado pela primeira observação — e é o que explica ao operador por
		// que um campo aparece como "unknown", ou por que ele não pode receber
		// índice. Um traço fixo em toda linha apagaria a distinção inteira.
		celulas := celulasDaLinha(linha)
		if len(celulas) != 6 {
			t.Fatalf("a linha de %q saiu com %d células, quer 6 (coluna, alias, tipo, jdbc, índice, auto): %q",
				c.Name, len(celulas), linha)
		}
		quer := "-"
		if c.Auto {
			quer, auto = "sim", auto+1
		}
		if celulas[5] != quer {
			t.Errorf("coluna %q com AUTO = %q e auto=%v no /v1/metadata: %q", c.Name, celulas[5], c.Auto, linha)
		}
	}
	if auto == 0 {
		t.Fatal("nenhuma coluna auto no fixture: a coluna AUTO seria um traço fixo e o teste não provaria nada")
	}

	// Os dois índices declarados pelo fixture do §12 aparecem com o nome que o
	// YAML usa: publicar "bitmap" como "sim/não" apagaria a diferença entre
	// bitmap e hash, que decide o plano.
	if l := linhaDaSaida(t, saida, "Acct-Status-Type"); !strings.Contains(l, "bitmap") {
		t.Errorf("o índice bitmap não aparece na linha: %q", l)
	}
	if l := linhaDaSaida(t, saida, "NAS-IP-Address"); !strings.Contains(l, "hash") {
		t.Errorf("o índice hash não aparece na linha: %q", l)
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
