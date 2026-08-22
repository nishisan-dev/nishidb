package cli_test

import (
	"encoding/json"
	"io"
	"net/http"
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
	a := novoAmbienteRemoto(t, 100000)
	if code := a.rodaComEntrada(corpus(t, eventos), "ingest",
		"--config", a.config, "--dataset", "radius_sessions", "--input", "-"); code != 0 {
		t.Fatalf("ingest saiu com %d: %s", code, a.stderr.String())
	}
	a.sobeServidor(t)
	return a
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
}

// `inspect` é o comando do §96 que detalha o dataset, e o que o operador
// procura nele é o que a consulta vai poder usar: o tipo da coluna, o alias
// pelo qual ela é escrita no SQL e o ÍNDICE declarado — que é o que separa uma
// consulta com lookup de uma que cai em full scan.
func TestInspectDetalhaColunasEIndices(t *testing.T) {
	a := servidorComDadosNoAr(t, 10)

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

	// Toda coluna do metadata sai, e sai com os quatro atributos: uma tabela
	// que omitisse uma coluna mandaria o operador concluir que ela não existe.
	if len(d.Columns) < 10 {
		t.Fatalf("o fixture tem %d colunas: poucas para distinguir tabela de amostra", len(d.Columns))
	}
	for _, c := range d.Columns {
		linha := linhaDaSaida(t, saida, c.Name)
		if !strings.Contains(linha, c.Type) || !strings.Contains(linha, c.JDBC) || !strings.Contains(linha, c.Index) {
			t.Errorf("coluna %q sem tipo/jdbc/índice na saída: %q", c.Name, linha)
		}
		if c.Alias != nil && !strings.Contains(linha, *c.Alias) {
			t.Errorf("coluna %q sem o alias %q: %q", c.Name, *c.Alias, linha)
		}
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
