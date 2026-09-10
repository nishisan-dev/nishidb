package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/server"
	"github.com/nishisan-dev/nishidb/internal/storage"
)

// specConsultavel é um dataset sem Kafka, com um campo indexado e um alias —
// o suficiente para exercitar projeção, WHERE com parâmetro e metadados.
const specConsultavel = `version: 1
name: %s
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    status:
      type: string
      alias: estado
      index: true
    porta:
      type: int64
    ip:
      type: ipv4
`

// semeia materializa n eventos no dataset ANTES de o servidor subir.
//
// A ingestão acontece pelo pipeline real (Materializer.Apply + Store.Put) e o
// flock é devolvido no Close — é o mesmo ciclo do `nishidb ingest`, e é o que
// deixa o servidor abrir um dataset com dado de verdade dentro.
func semeia(t *testing.T, a *ambiente, nome string, n int) {
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
	for i := 0; i < n; i++ {
		estado := "Start"
		if i%2 == 0 {
			estado = "Stop"
		}
		ev := fmt.Sprintf(`{"id":"S-%06d","status":%q,"porta":%d,"ip":"10.0.0.%d"}`,
			i, estado, 1000+i, i%256)
		if r := mat.Apply([]byte(ev)); r.Err != nil {
			_ = st.Close()
			t.Fatalf("evento %d: %v", i, r.Err)
		}
	}
	if err := st.Checkpoint(); err != nil {
		_ = st.Close()
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

// servidorComDados sobe um servidor de verdade, já com n linhas materializadas.
func servidorComDados(t *testing.T, n int, blocoServer string) *server.Servidor {
	t.Helper()
	srv, _ := servidorComDadosEDiretorio(t, n, blocoServer)
	return srv
}

// servidorComDadosEDiretorio faz o mesmo e ainda devolve o diretório do
// dataset — a raiz de onde pendem data/, metadata/, indexes/ e checkpoint/.
//
// O diretório existe porque há asserção que só fecha contra o FILESYSTEM. A
// nishidb_storage_bytes promete tamanho de arquivo, e a única maneira de
// provar que ela cumpre é somar os segment-*.dat por fora do Store: um
// oráculo que perguntasse ao próprio Store confirmaria apenas que o número foi
// copiado de um campo para outro, que é como o mutante de publicar o tally de
// liveness sob esse nome sobreviveu à suíte inteira.
//
// A separação em dois helpers é para não mexer nos chamadores que só querem o
// servidor: o de baixo é o real, e servidorComDados delega a ele.
func servidorComDadosEDiretorio(t *testing.T, n int, blocoServer string) (*server.Servidor, string) {
	t.Helper()
	return servidorComDadosDiretorioEStorage(t, n, blocoServer, "")
}

// servidorComDadosDiretorioEStorage é o de baixo: aceita linhas extras do bloco
// `storage:`, que é como um teste pede rotação de segmento.
func servidorComDadosDiretorioEStorage(t *testing.T, n int, blocoServer, storageExtra string) (*server.Servidor, string) {
	t.Helper()
	a := novoAmbienteComStorage(t, blocoServer, storageExtra)
	a.escreveDataset("d", strings.ReplaceAll(specConsultavel, "%s", "d"))
	semeia(t, a, "d", n)

	cfg := a.carrega()
	srv, err := server.Novo(cfg, server.Opcoes{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	roda(t, srv)
	return srv, cfg.DatasetDir("d")
}

// roda sobe o servidor numa goroutine e garante o encerramento gracioso no fim
// do teste — que é o que devolve o flock do dataset.
func roda(t *testing.T, srv *server.Servidor) {
	t.Helper()
	ctx, cancela := context.WithCancel(context.Background())
	fim := make(chan error, 1)
	go func() { fim <- srv.Roda(ctx, "") }()
	t.Cleanup(func() {
		cancela()
		if err := <-fim; err != nil {
			t.Errorf("Roda: %v", err)
		}
	})
}

const blocoPadrao = "\nserver:\n  bind: 127.0.0.1\n  port: %d\n"

// consulta faz o POST /v1/query e devolve a resposta bruta.
func consulta(t *testing.T, srv *server.Servidor, corpo string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query", strings.NewReader(corpo))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// fecha fecha o corpo e reclama do erro: um corpo que não fecha vaza conexão, e
// o teste seguinte herdaria o problema sem saber de onde veio.
func fecha(t *testing.T, resp *http.Response) {
	t.Helper()
	if err := resp.Body.Close(); err != nil {
		t.Errorf("fechando o corpo: %v", err)
	}
}

// resultado percorre a moldura inteira e devolve as rows cruas e o leitor.
func resultado(t *testing.T, resp *http.Response) ([]string, *protocol.Reader) {
	t.Helper()
	defer fecha(t, resp)
	l := protocol.NewReader(resp.Body)
	var rows []string
	for l.Next() {
		rows = append(rows, string(l.Line()))
	}
	if err := l.Err(); err != nil {
		t.Fatalf("leitura do stream: %v", err)
	}
	return rows, l
}

func falhaDe(t *testing.T, resp *http.Response) protocol.ErrorDetail {
	t.Helper()
	defer fecha(t, resp)
	var corpo protocol.ErrorBody
	if err := json.NewDecoder(resp.Body).Decode(&corpo); err != nil {
		t.Fatalf("corpo de erro ilegível: %v", err)
	}
	return corpo.Error
}

// A moldura completa: schema com os tipos, rows cruas do §69 e fecho.
func TestQueryRespondeAMolduraCompleta(t *testing.T) {
	srv := servidorComDados(t, 4, blocoPadrao)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id, ip FROM d"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != protocol.ContentTypeNDJSON {
		t.Fatalf("Content-Type = %q, quer %q", ct, protocol.ContentTypeNDJSON)
	}
	rows, l := resultado(t, resp)

	if l.Schema() == nil || l.Schema().Dataset != "d" {
		t.Fatalf("schema = %+v", l.Schema())
	}
	// A comparação é da struct INTEIRA, de propósito: campo novo na Coluna
	// que ninguém preencha quebra aqui, em vez de sair como zero no fio.
	quer := []protocol.Column{
		{Name: "id", Type: "string", JDBC: protocol.JDBCVarchar, Nullable: false, DisplaySize: 0},
		{Name: "ip", Type: "ipv4", JDBC: protocol.JDBCVarchar, Nullable: true, Precision: 15, DisplaySize: 15},
	}
	for i, c := range quer {
		if l.Schema().Columns[i] != c {
			t.Fatalf("coluna %d = %+v, quer %+v", i, l.Schema().Columns[i], c)
		}
	}
	if len(rows) != 4 {
		t.Fatalf("rows = %d, quer 4", len(rows))
	}
	// As linhas são o NDJSON cru do §69: nada de envelope.
	if !strings.HasPrefix(rows[0], `{"id":"S-000000","ip":"10.0.0.0"}`) {
		t.Fatalf("row = %s", rows[0])
	}
	if l.End() == nil || l.End().Rows != 4 || l.End().Truncated {
		t.Fatalf("fecho = %+v", l.End())
	}
}

// O `?` do §68 é o caminho que o CLI local NÃO alcança: o motor aceita
// parâmetro posicional desde o M1 e nada tinha como fornecê-lo.
func TestParametroPosicionalChegaAoMotor(t *testing.T) {
	srv := servidorComDados(t, 10, blocoPadrao)

	resp := consulta(t, srv, `{"dataset":"d",
		"sql":"SELECT id FROM d WHERE estado = ?",
		"parameters":[{"type":"string","value":"Stop"}]}`)
	rows, l := resultado(t, resp)

	if l.Failure() != nil {
		t.Fatalf("fecho de erro: %+v", l.Failure())
	}
	if len(rows) != 5 {
		t.Fatalf("rows = %d, quer 5 (metade dos eventos é Stop)", len(rows))
	}

	// E o parâmetro de outro tipo também: int64 comparado a campo int64.
	resp = consulta(t, srv, `{"dataset":"d",
		"sql":"SELECT id FROM d WHERE porta = ?",
		"parameters":[{"type":"int64","value":1003}]}`)
	rows, _ = resultado(t, resp)
	if len(rows) != 1 {
		t.Fatalf("rows = %d, quer 1", len(rows))
	}
}

// §69 e §128: a primeira linha tem de chegar ao cliente ANTES de a última ser
// produzida.
//
// A prova é a métrica de consultas em voo: se o cliente já leu uma row
// enquanto nishidb_query_active ainda é 1, a resposta não foi montada antes de
// sair. O resultado é grande de propósito — alguns MB não cabem nos buffers de
// socket, então um servidor que bufferizasse o resultado inteiro só começaria a
// escrever depois de terminar, e a leitura da primeira linha travaria.
func TestAPrimeiraLinhaChegaAntesDaUltimaSerProduzida(t *testing.T) {
	srv := servidorComDados(t, 30000, blocoPadrao)

	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, resp)

	l := protocol.NewReader(resp.Body)
	if !l.Next() {
		t.Fatalf("nenhuma row chegou: %v", l.Err())
	}
	// Uma row na mão, e a consulta ainda em voo do outro lado.
	if ativas := valorDaMetrica(t, srv, "nishidb_query_active"); ativas != 1 {
		t.Fatalf("nishidb_query_active = %v durante o stream, quer 1: "+
			"a resposta foi produzida inteira antes de sair", ativas)
	}

	n := 1
	for l.Next() {
		n++
	}
	if err := l.Err(); err != nil {
		t.Fatalf("leitura: %v", err)
	}
	if n != 30000 || l.End() == nil || l.End().Rows != 30000 {
		t.Fatalf("rows = %d, fecho = %+v", n, l.End())
	}
	// E, terminada a consulta, o medidor volta a zero.
	esperaMetrica(t, srv, "nishidb_query_active", 0)
}

// O teto do DATASET é truncamento involuntário: sai como fecho de ERRO, com o
// código que o CLI traduz em exit 5. Um resultado truncado apresentado como
// completo é resposta errada.
func TestTetoDoDatasetSaiComoFechoDeErro(t *testing.T) {
	srv := servidorComDados(t, 20, blocoPadrao+"query:\n  defaultMaxRows: 5\n")

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: depois do schema o status é sempre 200", resp.StatusCode)
	}
	rows, l := resultado(t, resp)

	if len(rows) != 5 {
		t.Fatalf("rows = %d, quer 5", len(rows))
	}
	if l.End() != nil {
		t.Fatalf("fecho de sucesso num resultado truncado: %+v", l.End())
	}
	if l.Failure() == nil || l.Failure().Code != protocol.CodeMaxRows || l.Failure().Rows != 5 {
		t.Fatalf("fecho = %+v, quer %s com 5 rows", l.Failure(), protocol.CodeMaxRows)
	}
}

// O maxRows do REQUEST é o cliente pedindo menos: não é erro, é o pedido sendo
// atendido — mas o fecho diz que houve corte.
func TestMaxRowsDoRequestTruncaSemErro(t *testing.T) {
	srv := servidorComDados(t, 20, blocoPadrao)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","maxRows":3}`)
	rows, l := resultado(t, resp)

	if len(rows) != 3 {
		t.Fatalf("rows = %d, quer 3", len(rows))
	}
	if l.Failure() != nil {
		t.Fatalf("pedir menos não é erro: %+v", l.Failure())
	}
	if l.End() == nil || !l.End().Truncated || l.End().Rows != 3 {
		t.Fatalf("fecho = %+v, quer truncated com 3 rows", l.End())
	}
}

// Pedir MAIS que o teto do dataset é recusado, não reduzido em silêncio: um
// clamp calado faria o cliente acreditar que recebeu tudo o que pediu.
func TestMaxRowsAcimaDoTetoEhRecusado(t *testing.T) {
	srv := servidorComDados(t, 5, blocoPadrao+"query:\n  defaultMaxRows: 10\n")

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","maxRows":1000}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, quer 400", resp.StatusCode)
	}
	f := falhaDe(t, resp)
	if f.Code != protocol.CodeMaxRowsAboveCeiling {
		t.Fatalf("código = %q", f.Code)
	}
	if !strings.Contains(f.Message, "10") {
		t.Fatalf("a mensagem não nomeia o teto efetivo: %q", f.Message)
	}
}

// Toda recusa ANTES da primeira linha sai com status de verdade e o mesmo
// corpo JSON — inclusive o método errado, que é por isso que a rota não delega
// o 405 ao mux.
func TestErrosAntesDaPrimeiraLinha(t *testing.T) {
	srv := servidorComDados(t, 3, blocoPadrao)
	base := "http://" + srv.Endereco() + "/v1/query"

	t.Run("método", func(t *testing.T) {
		resp, err := http.Get(base)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if resp.Header.Get("Allow") != http.MethodPost {
			t.Fatalf("Allow = %q", resp.Header.Get("Allow"))
		}
		if f := falhaDe(t, resp); f.Code != protocol.CodeMethod {
			t.Fatalf("código = %q", f.Code)
		}
	})

	t.Run("content-type", func(t *testing.T) {
		resp, err := http.Post(base, "text/plain", strings.NewReader(`{"dataset":"d","sql":"SELECT id FROM d"}`))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusUnsupportedMediaType {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if f := falhaDe(t, resp); f.Code != protocol.CodeMediaType {
			t.Fatalf("código = %q", f.Code)
		}
	})

	t.Run("corpo inválido", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if f := falhaDe(t, resp); f.Code != protocol.CodeBadRequest {
			t.Fatalf("código = %q", f.Code)
		}
	})

	t.Run("chave desconhecida", func(t *testing.T) {
		// `maxRow` com typo tem de ser recusado: aceito em silêncio, o cliente
		// acreditaria ter limitado o resultado.
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","maxRow":3}`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d", resp.StatusCode)
		}
	})

	t.Run("corpo grande demais", func(t *testing.T) {
		gordo := `{"dataset":"d","sql":"SELECT id FROM d WHERE id = '` +
			strings.Repeat("x", protocol.MaxRequestBody) + `'"}`
		resp := consulta(t, srv, gordo)
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, quer 413", resp.StatusCode)
		}
		if f := falhaDe(t, resp); f.Code != protocol.CodeBodyTooLarge {
			t.Fatalf("código = %q", f.Code)
		}
	})

	t.Run("dataset desconhecido", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":"nao_existe","sql":"SELECT id FROM nao_existe"}`)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if f := falhaDe(t, resp); f.Code != protocol.CodeUnknownDataset {
			t.Fatalf("código = %q", f.Code)
		}
	})

	t.Run("sintaxe com posição", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT FROM d"}`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		f := falhaDe(t, resp)
		if f.Code != protocol.CodeSyntax {
			t.Fatalf("código = %q", f.Code)
		}
		if f.Line != 1 || f.Column < 1 {
			t.Fatalf("posição = linha %d, coluna %d", f.Line, f.Column)
		}
	})

	t.Run("bind", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT nao_existe FROM d"}`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if f := falhaDe(t, resp); f.Code != protocol.CodeBind {
			t.Fatalf("código = %q", f.Code)
		}
	})

	t.Run("parâmetro inválido", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d WHERE estado = ?",
			"parameters":[{"type":"ipv4","value":"10.0.0.1"}]}`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		f := falhaDe(t, resp)
		if f.Code != protocol.CodeBadParameter {
			t.Fatalf("código = %q", f.Code)
		}
		if !strings.Contains(f.Message, "string") {
			t.Fatalf("a mensagem não ensina a alternativa: %q", f.Message)
		}
	})
}

// O §81 autoriza rejeitar consulta antes de comprometer a ingestão. Com uma
// vaga só, a segunda requisição simultânea sai 503 sem tocar o Store.
func TestConcorrenciaAcimaDoTetoEhRejeitada(t *testing.T) {
	srv := servidorComDados(t, 30000, blocoPadrao+"runtime:\n  query:\n    maxConcurrent: 1\n")

	// A primeira consulta fica em voo: o cliente lê uma linha e para de ler,
	// o que segura o servidor no meio do stream.
	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	primeira, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, primeira)

	l := protocol.NewReader(primeira.Body)
	if !l.Next() {
		t.Fatalf("a primeira consulta não produziu row: %v", l.Err())
	}

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, quer 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("503 sem Retry-After: o cliente não sabe quando voltar")
	}
	f := falhaDe(t, resp)
	if f.Code != protocol.CodeTooManyQueries {
		t.Fatalf("código = %q", f.Code)
	}
	if !strings.Contains(f.Message, "maxConcurrent") {
		t.Fatalf("a mensagem não nomeia a chave: %q", f.Message)
	}
}

// A vaga tem de voltar em TODO caminho de saída, inclusive nos de erro. Sem
// isso o servidor para de aceitar consultas depois de N falhas.
func TestAVagaVoltaDepoisDeCadaConsulta(t *testing.T) {
	srv := servidorComDados(t, 3, blocoPadrao+"runtime:\n  query:\n    maxConcurrent: 1\n")

	for i := 0; i < 5; i++ {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT nao_existe FROM d"}`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("iteração %d: status = %d", i, resp.StatusCode)
		}
		resp.Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste
	}
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("depois de 5 erros a vaga não voltou: status = %d", resp.StatusCode)
	}
	rows, _ := resultado(t, resp)
	if len(rows) != 3 {
		t.Fatalf("rows = %d", len(rows))
	}
}

// Consultas simultâneas dentro do teto não se atrapalham: o Engine é seguro
// para uso concorrente e o Store é um writer com muitos leitores (§27).
func TestConsultasSimultaneasDentroDoTeto(t *testing.T) {
	srv := servidorComDados(t, 500, blocoPadrao+"runtime:\n  query:\n    maxConcurrent: 8\n")

	var wg sync.WaitGroup
	erros := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id, porta FROM d"}`)
			defer fecha(t, resp)
			if resp.StatusCode != http.StatusOK {
				erros <- fmt.Errorf("status = %d", resp.StatusCode)
				return
			}
			l := protocol.NewReader(resp.Body)
			n := 0
			for l.Next() {
				n++
			}
			if l.Err() != nil {
				erros <- l.Err()
				return
			}
			if n != 500 {
				erros <- fmt.Errorf("rows = %d, quer 500", n)
			}
		}()
	}
	wg.Wait()
	close(erros)
	for err := range erros {
		t.Error(err)
	}
}

// A coluna que colide com a chave de moldura é recusada ANTES do primeiro
// byte: depois dele, a row seria indistinguível de uma linha de controle e não
// haveria mais como recusar nada.
func TestColunaComNomeReservadoEhRecusada(t *testing.T) {
	a := novoAmbienteCom(t, blocoPadrao)
	a.escreveDataset("d", `version: 1
name: d
materialization:
  key:
    field: id
schema:
  fields:
    id:
      type: string
    nishidb:
      type: string
`)
	srv, err := server.Novo(a.carrega(), server.Opcoes{})
	if err != nil {
		t.Fatal(err)
	}
	roda(t, srv)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id, nishidb FROM d"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, quer 400", resp.StatusCode)
	}
	f := falhaDe(t, resp)
	if f.Code != protocol.CodeReservedColumn {
		t.Fatalf("código = %q", f.Code)
	}
	if !strings.Contains(f.Message, "alias") {
		t.Fatalf("a mensagem não diz como contornar: %q", f.Message)
	}

	// E a mesma coluna com alias passa: a colisão é de NOME DE SAÍDA.
	resp = consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	resp.Body.Close() //nolint:errcheck // corpo já lido; o erro do fecho não muda o teste
}

// query.resultBatchSize ganhou leitor: é o lote de FLUSH da resposta. O
// contrato observável é que o resultado sai íntegro com qualquer lote — o
// número muda de quanto em quanto os bytes vão para o fio, não o que sai.
func TestResultBatchSizeNaoAlteraOResultado(t *testing.T) {
	for _, lote := range []int{1, 7, 4096} {
		t.Run(fmt.Sprint(lote), func(t *testing.T) {
			srv := servidorComDados(t, 100,
				blocoPadrao+fmt.Sprintf("query:\n  resultBatchSize: %d\n", lote))
			resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d"}`)
			rows, l := resultado(t, resp)
			if len(rows) != 100 || l.End() == nil || l.End().Rows != 100 {
				t.Fatalf("lote %d: rows = %d, fecho = %+v", lote, len(rows), l.End())
			}
		})
	}
}

// valorDaMetrica lê UMA série sem label do /metrics (ou a soma das séries com
// label, que é o que interessa em processo de um dataset só).
func valorDaMetrica(t *testing.T, srv *server.Servidor, nome string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + srv.Endereco() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, resp)
	corpo, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var total float64
	achou := false
	for _, linha := range strings.Split(string(corpo), "\n") {
		if !strings.HasPrefix(linha, nome) || strings.HasPrefix(linha, "#") {
			continue
		}
		resto := linha[len(nome):]
		if resto != "" && resto[0] != ' ' && resto[0] != '{' {
			continue // outro nome com o mesmo prefixo
		}
		i := strings.LastIndexByte(linha, ' ')
		var v float64
		if _, err := fmt.Sscanf(linha[i+1:], "%g", &v); err != nil {
			t.Fatalf("valor ilegível em %q: %v", linha, err)
		}
		total += v
		achou = true
	}
	if !achou {
		t.Fatalf("métrica %q ausente do /metrics", nome)
	}
	return total
}

// esperaMetrica espera a série atingir o valor: o decremento do medidor
// acontece no defer do handler, que pode rodar depois de o cliente já ter o
// último byte.
func esperaMetrica(t *testing.T, srv *server.Servidor, nome string, quer float64) {
	t.Helper()
	prazo := time.Now().Add(2 * time.Second)
	for {
		got := valorDaMetrica(t, srv, nome)
		if got == quer {
			return
		}
		if time.Now().After(prazo) {
			t.Fatalf("%s = %v, quer %v", nome, got, quer)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// §128 — a memória da consulta é O(lote), NUNCA O(resultado).
//
// A medida é o heap VIVO enquanto a consulta está em voo, com o cliente parado
// no meio do stream: nesse estado o servidor está bloqueado escrevendo, nada se
// move, e um runtime.GC() deixa de pé só o que a consulta realmente segura. O
// delta é tirado contra o heap do MESMO servidor sem consulta nenhuma, porque o
// índice primário e o overlay crescem com o número de linhas e apareceriam na
// medida absoluta como se fossem custo do stream.
//
// Um servidor que acumulasse o resultado seguraria, no caso grande, alguns MB a
// mais de linhas serializadas — e é exatamente isso que o teto abaixo recusa.
func TestOCustoEmVooNaoAcompanhaOTamanhoDoResultado(t *testing.T) {
	pequeno := custoEmVoo(t, 2000)
	grande := custoEmVoo(t, 40000)

	// 20× mais linhas. Se o resultado fosse acumulado, seriam ~3,5 MB a mais;
	// o teto de 1 MiB dá folga larga para ruído de heap sem deixar passar
	// acumulação.
	const teto = 1 << 20
	if grande > pequeno+teto {
		t.Fatalf("custo em voo: %d linhas = %d B, %d linhas = %d B (delta %d B > %d B): "+
			"a memória está acompanhando o tamanho do resultado",
			2000, pequeno, 40000, grande, grande-pequeno, teto)
	}
	t.Logf("custo em voo: 2.000 linhas = %d B, 40.000 linhas = %d B", pequeno, grande)
}

// custoEmVoo devolve quantos bytes de heap vivo a consulta segura enquanto
// está parada no meio do stream.
func custoEmVoo(t *testing.T, linhas int) uint64 {
	t.Helper()
	srv := servidorComDados(t, linhas, blocoPadrao)
	base := heapVivo()

	req, err := http.NewRequest(http.MethodPost, "http://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer fecha(t, resp)

	l := protocol.NewReader(resp.Body)
	if !l.Next() {
		t.Fatalf("nenhuma row chegou: %v", l.Err())
	}
	// A partir daqui ninguém lê: o servidor enche os buffers e bloqueia no
	// write, que é o estado estável de que a medida precisa.
	if ativas := valorDaMetrica(t, srv, "nishidb_query_active"); ativas != 1 {
		t.Fatalf("a consulta já tinha terminado quando o cliente parou de ler (query_active = %v): "+
			"a resposta foi produzida inteira antes de sair, e não há custo em voo para medir", ativas)
	}
	time.Sleep(200 * time.Millisecond)

	durante := heapVivo()
	if durante < base {
		return 0
	}
	return durante - base
}

func heapVivo() uint64 {
	// Dois GCs: o primeiro roda os finalizadores, o segundo coleta o que eles
	// liberaram. É a receita padrão para uma leitura estável de heap vivo.
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// A linha de schema sai no SEU PRÓPRIO chunk, antes de qualquer row.
//
// É o que faz Statement.executeQuery() devolver um ResultSetMetaData sem
// esperar o primeiro lote de resultBatchSize. Numa consulta que varre muito e
// emite pouco — full scan com WHERE seletivo, que é o caso comum no piloto — o
// cliente ficaria sem nenhum byte até o primeiro lote fechar, e uma ferramenta
// de SQL não teria como desenhar as colunas do grid enquanto isso.
//
// O oráculo é o CHUNKING cru, e não o tempo: um teste de latência passaria por
// acidente num dataset de teste, onde tudo termina em milissegundos. Sem o
// flush, o net/http junta schema e rows no mesmo chunk (elas cabem folgadas no
// buffer dele); com o flush, o primeiro chunk é o schema e nada mais. Por isso
// a requisição é feita na conexão crua: o http.Client desfaz o chunking antes
// de entregar o corpo, e com ele o teste não teria o que observar.
func TestALinhaDeSchemaSaiNoSeuProprioChunk(t *testing.T) {
	srv := servidorComDados(t, 4, blocoPadrao)

	conn, err := net.DialTimeout("tcp", srv.Endereco(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("fechar a conexão: %v", err)
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}

	corpo := `{"dataset":"d","sql":"SELECT id FROM d"}`
	req := "POST /v1/query HTTP/1.1\r\nHost: " + srv.Endereco() + "\r\n" +
		"Content-Type: application/json\r\n" +
		"Content-Length: " + strconv.Itoa(len(corpo)) + "\r\n\r\n" + corpo
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}

	br := bufio.NewReader(conn)
	// Cabeçalhos até a linha em branco.
	for {
		linha, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("lendo cabeçalhos: %v", err)
		}
		if linha == "\r\n" {
			break
		}
	}

	// Primeiro chunk: <tamanho em hex>\r\n<dados>\r\n
	cabecalho, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("lendo o tamanho do primeiro chunk: %v", err)
	}
	tamanho, err := strconv.ParseInt(strings.TrimSpace(cabecalho), 16, 64)
	if err != nil {
		t.Fatalf("tamanho de chunk ilegível %q — a resposta não veio chunked?", cabecalho)
	}
	dados := make([]byte, tamanho)
	if _, err := io.ReadFull(br, dados); err != nil {
		t.Fatalf("lendo o primeiro chunk: %v", err)
	}

	primeiro := string(dados)
	if !strings.HasPrefix(primeiro, `{"`+protocol.FrameKey+`":"`+protocol.FrameSchema+`"`) {
		t.Fatalf("o primeiro chunk não começa pela linha de schema: %q", primeiro)
	}
	if strings.Count(primeiro, "\n") != 1 {
		t.Errorf("o primeiro chunk traz mais que a linha de schema — o flush depois dela sumiu?\n%q", primeiro)
	}
}

// A cláusula não suportada chega ao cliente com código PRÓPRIO, 400 e posição.
//
// O código é o que o driver JDBC traduz para SQLState 0A000 (feature not
// supported) em vez de 42601 (sintaxe) — a diferença entre a ferramenta dizer
// "este banco não ordena" e "você escreveu errado".
//
// O teste também trava a decisão de a distinção morar num CAMPO do SyntaxError
// e não num tipo de erro próprio: com um tipo, a tradução no servidor
// dependeria da ORDEM dos cases, e inverter dois deles faria o código novo
// nunca sair — degradando em silêncio para SYNTAX, sem nada acusar.
func TestUnsupportedClauseSaiCom400EPosicaoNoProtocolo(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d GROUP BY id"}`)
	if resp.StatusCode != http.StatusBadRequest {
		fecha(t, resp)
		t.Fatalf("status = %d, quer 400", resp.StatusCode)
	}
	f := falhaDe(t, resp)
	if f.Code != protocol.CodeUnsupportedClause {
		t.Errorf("código = %q, quer %q", f.Code, protocol.CodeUnsupportedClause)
	}
	if !strings.Contains(f.Message, "GROUP BY") {
		t.Errorf("a mensagem não nomeia a cláusula: %q", f.Message)
	}
	// 1-based, apontando para o token da CLÁUSULA e não para o seguinte: é ele
	// que a ferramenta sublinha. Em "SELECT id FROM d ORDER BY id" o ORDER
	// começa no byte 17 (0-based), logo coluna 18.
	if f.Line != 1 || f.Column != 18 {
		t.Errorf("posição = linha %d, coluna %d; quer 1/18 (o token ORDER)", f.Line, f.Column)
	}
}

// Uma coluna chamada `order` continua consultável pelo protocolo, e não só no
// parser: é a garantia de que a mensagem melhor não custou uma query que
// funcionava.
func TestColunaChamadaOrderContinuaConsultavelPeloProtocolo(t *testing.T) {
	srv := servidorComDados(t, 2, blocoPadrao)

	// `porta` existe no dataset de teste; o que se prova aqui é que o
	// identificador `order` chega ao BINDER (que o recusa por não existir no
	// schema) em vez de morrer no parser como cláusula.
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT order FROM d"}`)
	if resp.StatusCode != http.StatusBadRequest {
		fecha(t, resp)
		t.Fatalf("status = %d, quer 400", resp.StatusCode)
	}
	if f := falhaDe(t, resp); f.Code != protocol.CodeBind {
		t.Errorf("código = %q, quer %q: `order` tem de chegar ao binder como nome de coluna", f.Code, protocol.CodeBind)
	}
}

// A tabela virtual é servida pela MESMA rota de consulta, contra o dataset
// nomeado na requisição. É o que dispensa suporte em cliente nenhum: o driver
// e o DBeaver já sabem executar SELECT.
func TestMetaFieldsRespondePelaRotaDeQuery(t *testing.T) {
	srv := servidorComDados(t, 1, blocoPadrao)
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT name, type, auto FROM _meta.fields"}`)
	linhas, fim := resultado(t, resp)
	if fim.End() == nil {
		t.Fatal("o stream não trouxe a linha de fecho")
	}
	if len(linhas) == 0 {
		t.Fatal("nenhuma linha; o dataset tem campos")
	}
	for _, l := range linhas {
		if !strings.Contains(l, `"name"`) || !strings.Contains(l, `"type"`) {
			t.Fatalf("linha sem as colunas pedidas: %s", l)
		}
	}
}
