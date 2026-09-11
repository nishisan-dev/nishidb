package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/server"
)

// serverWithBlockOfQuery sobe um servidor cujo dataset tem o bloco `query`
// que o teste quiser, já com n linhas materializadas.
//
// Existe porque serverWithData fixa specConsultavel, que não declara `query`
// nenhum — e toda esta frente é sobre a diferença entre `query.timeout` e
// `query.maxTimeout`. O bloco é APENDADO à spec padrão, então os campos, a key
// e o índice continuam os mesmos que semeia() alimenta.
func serverWithBlockOfQuery(t *testing.T, n int, blockServer, blockQuery string) *server.Server {
	t.Helper()
	a := newEnvWithStorage(t, blockServer, "")
	a.writeDataset("d", strings.ReplaceAll(specConsultavel, "%s", "d")+blockQuery)
	seed(t, a, "d", n)

	cfg := a.load()
	srv, err := server.Novo(cfg, server.Options{})
	if err != nil {
		t.Fatalf("subida: %v", err)
	}
	run(t, srv)
	return srv
}

// rowsQueNaoCabemInSocket é quantas linhas o dataset precisa ter para que uma
// consulta segure a vaga de verdade enquanto o cliente para de ler.
//
// O número não é folclore: cada row deste dataset serializa em ~60 bytes, e o
// buffer de socket do loopback absorve alguns megabytes. Com 30 mil linhas
// (~1,8 MiB) o servidor escreve o resultado INTEIRO no buffer do kernel, a
// consulta termina, a vaga volta — e um teste que supõe a consulta presa passa
// a medir outra coisa, ou pisca. Com 100 mil (~6 MiB) a escrita bloqueia de
// verdade no primeiro cliente que para de ler.
const rowsQueNaoCabemInSocket = 100000

// streamAberto é uma consulta em voo: o cliente leu o começo e parou, o que
// segura a vaga do servidor até o corpo ser fechado.
type streamAberto struct {
	resp   *http.Response
	reader *protocol.Reader
}

// openStream faz o POST e devolve o leitor SEM consumir o stream. É o que
// permite a um teste manter uma consulta ocupando vaga enquanto faz outra.
func openStream(t *testing.T, srv *server.Server, corpo string) *streamAberto {
	t.Helper()
	resp := consulta(t, srv, corpo)
	return &streamAberto{resp: resp, reader: protocol.NewReader(resp.Body)}
}

// desfechoOfStream percorre a moldura e devolve quantas rows saíram e o código
// do fecho — "" quando o fecho foi de sucesso.
func desfechoOfStream(t *testing.T, resp *http.Response) (int, string) {
	t.Helper()
	defer fecha(t, resp)
	l := protocol.NewReader(resp.Body)
	n := 0
	for l.Next() {
		n++
	}
	if err := l.Err(); err != nil {
		t.Fatalf("leitura do stream: %v", err)
	}
	if e := l.Failure(); e != nil {
		return n, e.Code
	}
	return n, ""
}

// O request pode ALONGAR o prazo até query.maxTimeout, e nunca além dele.
//
// As duas metades são necessárias e nenhuma sozinha basta. A do teto (o 400) já
// existia contra query.timeout e continuaria passando se o alongamento fosse só
// aceito e depois cortado lá embaixo. A do alongamento é a que prova que o
// prazo maior VALE: o dataset declara 1 ms, uma varredura de 30 mil linhas por
// HTTP jamais cabe nisso, e a mesma consulta com timeoutMs de 20 s tem de
// entregar as 30 mil. Enquanto o executor reaplicava o `query.timeout` por cima
// do contexto recebido, este caso morria em TIMEOUT com o teto configurado
// dizendo que ele podia.
func TestTimeoutDoRequestPodeAlongarAteOMaxTimeoutENuncaAlemDele(t *testing.T) {
	const rows = 30000
	srv := serverWithBlockOfQuery(t, rows, blockPadrao,
		"query:\n  timeout: 1ms\n  maxTimeout: 30s\n")

	t.Run("sem timeoutMs vale o prazo padrão, e ele é curto", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`)
		// O prazo de 1 ms pode estourar ANTES da primeira linha (504) ou no meio
		// do stream (200 + fecho de erro). As duas são o mesmo desfecho para
		// quem chamou; o que não pode acontecer é a consulta terminar bem.
		if resp.StatusCode == http.StatusGatewayTimeout {
			f := failOf(t, resp)
			if f.Code != protocol.CodeTimeout {
				t.Fatalf("código = %q, quer %q", f.Code, protocol.CodeTimeout)
			}
			return
		}
		n, codigo := desfechoOfStream(t, resp)
		if codigo != protocol.CodeTimeout {
			t.Fatalf("fecho = %q com %d rows, quer %q: o prazo padrão de 1ms não pode varrer %d linhas",
				codigo, n, protocol.CodeTimeout, rows)
		}
	})

	t.Run("timeoutMs abaixo do teto ALONGA de verdade", func(t *testing.T) {
		resp := consulta(t, srv,
			`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d","timeoutMs":20000}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, quer 200", resp.StatusCode)
		}
		n, codigo := desfechoOfStream(t, resp)
		if codigo != "" {
			t.Fatalf("fecho = %q: o prazo pedido (20s) está dentro do teto (30s) e tem de valer", codigo)
		}
		if n != rows {
			t.Fatalf("rows = %d, quer %d", n, rows)
		}
	})

	t.Run("timeoutMs exatamente no teto é aceito", func(t *testing.T) {
		// O teto é INCLUSIVE. Um `>=` no lugar do `>` recusaria o único valor
		// que o operador escreveu no YAML, que é o mais provável de ser pedido.
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1","timeoutMs":30000}`)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, quer 200: o teto é inclusive", resp.StatusCode)
		}
		fecha(t, resp)
	})

	t.Run("timeoutMs acima do teto é recusado nomeando query.maxTimeout", func(t *testing.T) {
		resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","timeoutMs":30001}`)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, quer 400", resp.StatusCode)
		}
		f := failOf(t, resp)
		// A mensagem tem de mandar mexer no TETO, não no prazo padrão: subir o
		// `query.timeout` para atender um pedido pontual daria prazo longo a
		// todo cliente que nunca pediu — que é o defeito que a separação das
		// duas chaves veio corrigir.
		if !strings.Contains(f.Message, "query.maxTimeout") {
			t.Fatalf("a mensagem não nomeia a chave certa: %q", f.Message)
		}
		if strings.Contains(f.Message, "query.timeout") {
			t.Fatalf("a mensagem manda ajustar o prazo padrão, que alongaria para todo mundo: %q", f.Message)
		}
	})
}

// O outro lado da compatibilidade, agora no protocolo: um dataset SEM
// maxTimeout tem de recusar exatamente o que recusava antes.
//
// O par de números é a asserção inteira: o teto efetivo é o `query.timeout`
// declarado, então 2000 ms passa e 2001 ms não. Qualquer implementação que
// tivesse deixado o teto virar o default global (30s) ou desaparecer faria os
// dois passarem, e o dataset ganharia prazo que ninguém lhe deu.
func TestDatasetSemMaxTimeoutSeComportaExatamenteComoHoje(t *testing.T) {
	srv := serverWithBlockOfQuery(t, 3, blockPadrao, "query:\n  timeout: 2s\n")

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","timeoutMs":2000}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("timeoutMs igual ao query.timeout: status = %d, quer 200", resp.StatusCode)
	}
	fecha(t, resp)

	resp = consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","timeoutMs":2001}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("timeoutMs 1ms acima do query.timeout: status = %d, quer 400", resp.StatusCode)
	}
	fecha(t, resp)

	// E o metadata publica o MESMO número dos dois lados, que é o que faz o
	// clamp do driver JDBC continuar aparando no ponto de sempre.
	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata").Datasets[0]
	if m.TimeoutMs != 2000 {
		t.Errorf("metadata timeoutMs = %d, quer 2000", m.TimeoutMs)
	}
	if m.DefaultTimeoutMs != 2000 {
		t.Errorf("metadata defaultTimeoutMs = %d, quer 2000", m.DefaultTimeoutMs)
	}
}

// O contrato com o driver, afirmado do lado Go.
//
// NishiConnection lê o campo `timeoutMs` do /v1/metadata e NishiStatement o usa
// como TETO num Math.min sobre o setQueryTimeout da aplicação
// (timeoutParaOFio). O campo já significava "o número que o servidor faz
// valer" — o clamp existe justamente porque o servidor RECUSA acima dele. Por
// isso quem vai para lá é o MaxTimeout: publicar o prazo padrão faria o driver
// clampar em 2 s uma consulta que o dataset autoriza a durar 10 minutos, e o
// teto novo não valeria para cliente nenhum. Zero linha de Java muda.
func TestMetadataPublicaOTetoQueOClampDoDriverUsa(t *testing.T) {
	srv := serverWithBlockOfQuery(t, 1, blockPadrao, "query:\n  timeout: 2s\n  maxTimeout: 10m\n")

	m := readJSON[protocol.Metadata](t, srv, "/v1/metadata").Datasets[0]
	if m.TimeoutMs != (10 * time.Minute).Milliseconds() {
		t.Errorf("timeoutMs = %d, quer o TETO (%d): é contra este campo que o driver clampa",
			m.TimeoutMs, (10 * time.Minute).Milliseconds())
	}
	if m.DefaultTimeoutMs != 2000 {
		t.Errorf("defaultTimeoutMs = %d, quer 2000 (o prazo padrão)", m.DefaultTimeoutMs)
	}

	// E o teto publicado é ACIONÁVEL: o servidor aceita de verdade um pedido
	// no valor que ele mesmo anuncia. Um metadata que anuncia teto maior do que
	// o servidor honra faria o driver clampar em algo que volta 400.
	corpo := fmt.Sprintf(`{"dataset":"d","sql":"SELECT id FROM d LIMIT 1","timeoutMs":%d}`, m.TimeoutMs)
	resp := consulta(t, srv, corpo)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d: o servidor recusou o teto que ele mesmo publica", resp.StatusCode)
	}
	fecha(t, resp)
}

// O sub-pool em ação: a consulta longa não pode tomar a última vaga geral.
//
// Com maxConcurrent 2 e maxLongRunning 1, uma consulta longa em voo ocupa uma
// vaga de cada. A SEGUNDA longa é recusada pelo sub-pool — e é essa recusa que
// deixa a segunda vaga geral livre. Sem o sub-pool, ela entraria, as duas vagas
// gerais estariam ocupadas por consultas de minutos, e a consulta curta —
// aquela que responde em milissegundos e é a razão de o produto existir —
// receberia 503 pelo tempo que as longas durassem.
func TestQueryLongaNaoOcupaAUltimaVagaDaQueryCurta(t *testing.T) {
	srv := serverWithBlockOfQuery(t, rowsQueNaoCabemInSocket,
		blockPadrao+"runtime:\n  query:\n    maxConcurrent: 2\n    maxLongRunning: 1\n",
		"query:\n  timeout: 1s\n  maxTimeout: 60s\n")

	// A primeira longa fica em voo: o cliente lê uma linha e para de ler, o que
	// segura o servidor no meio do stream com as duas vagas na mão.
	primeira := openStream(t, srv, `{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d","timeoutMs":30000}`)
	defer fecha(t, primeira.resp)
	if !primeira.reader.Next() {
		t.Fatalf("a primeira consulta não produziu row: %v", primeira.reader.Err())
	}

	// A segunda longa bate no sub-pool, com código e Retry-After próprios.
	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","timeoutMs":30000}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("segunda consulta longa: status = %d, quer 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("503 sem Retry-After: o cliente não sabe quando voltar")
	}
	f := failOf(t, resp)
	if f.Code != protocol.CodeTooManyLongQueries {
		t.Fatalf("código = %q, quer %q: a recusa do sub-pool tem código próprio porque a AÇÃO é outra",
			f.Code, protocol.CodeTooManyLongQueries)
	}
	if !strings.Contains(f.Message, "maxLongRunning") {
		t.Errorf("a mensagem não nomeia a chave: %q", f.Message)
	}

	// E a asserção que carrega o teste: a consulta CURTA passa. A vaga geral
	// que a segunda longa teria tomado continua livre.
	curta := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1"}`)
	if curta.StatusCode != http.StatusOK {
		t.Fatalf("consulta curta: status = %d, quer 200 — a vaga que sobrou é dela", curta.StatusCode)
	}
	n, codigo := desfechoOfStream(t, curta)
	if codigo != "" || n != 1 {
		t.Fatalf("consulta curta: %d rows, fecho %q", n, codigo)
	}
}

// A consulta que NÃO pede prazo não é longa, e por isso não gasta vaga do
// sub-pool.
//
// É o outro lado do teste acima e o que impede a correção de exagerar: se todo
// request contasse como longo, um maxLongRunning de 2 viraria o teto real de
// concorrência do servidor e o maxConcurrent de 16 não valeria para nada.
func TestConsultaNoPrazoPadraoNaoGastaVagaDoSubPool(t *testing.T) {
	srv := serverWithBlockOfQuery(t, rowsQueNaoCabemInSocket,
		blockPadrao+"runtime:\n  query:\n    maxConcurrent: 4\n    maxLongRunning: 1\n",
		"query:\n  timeout: 60s\n  maxTimeout: 120s\n")

	// Três consultas no prazo padrão, todas em voo ao mesmo tempo. Com o
	// sub-pool de UMA vaga, se elas contassem como longas a terceira sairia 503.
	var abertas []*streamAberto
	for i := 0; i < 3; i++ {
		s := openStream(t, srv, `{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d"}`)
		if s.resp.StatusCode != http.StatusOK {
			t.Fatalf("consulta %d no prazo padrão: status = %d, quer 200", i, s.resp.StatusCode)
		}
		if !s.reader.Next() {
			t.Fatalf("consulta %d não produziu row: %v", i, s.reader.Err())
		}
		abertas = append(abertas, s)
	}
	for _, s := range abertas {
		fecha(t, s.resp)
	}
}

// Cancelamento de ponta a ponta: o cliente que desiste devolve a vaga.
//
// O caminho já existia (o r.Context() cancela, encerra() fecha a fonte, a vaga
// sai no defer) e nunca teve teste. Com o prazo longo virando padrão ele deixa
// de ser detalhe: uma vaga presa por um cliente que fechou o socket agora fica
// presa por MINUTOS, e num pool de 16 bastam 16 desistências para o servidor
// parar de aceitar consulta com o processo saudável.
func TestClienteQueDesisteLiberaAVaga(t *testing.T) {
	srv := serverWithBlockOfQuery(t, rowsQueNaoCabemInSocket,
		blockPadrao+"runtime:\n  query:\n    maxConcurrent: 1\n",
		"query:\n  timeout: 1s\n  maxTimeout: 300s\n")

	ctx, desiste := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+srv.Endereco()+"/v1/query",
		strings.NewReader(`{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d","timeoutMs":300000}`))
	if err != nil {
		desiste()
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		desiste()
		t.Fatal(err)
	}
	l := protocol.NewReader(resp.Body)
	if !l.Next() {
		desiste()
		t.Fatalf("a consulta não produziu row: %v", l.Err())
	}

	// Com a única vaga ocupada, a segunda consulta sai 503. É a prova de que o
	// estado de partida é o que o teste supõe — sem ela, um servidor que já
	// tivesse liberado a vaga por outro motivo faria o final passar de graça.
	ocupada := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1"}`)
	if ocupada.StatusCode != http.StatusServiceUnavailable {
		fecha(t, ocupada)
		desiste()
		t.Fatalf("status = %d com a vaga ocupada, quer 503", ocupada.StatusCode)
	}
	fecha(t, ocupada)

	// O cliente desiste no meio do stream.
	desiste()
	resp.Body.Close() //nolint:errcheck // o corpo foi abortado de propósito; o erro do fecho é o esperado

	// A vaga volta ASSIM QUE o handler percebe o cancelamento, que acontece na
	// próxima escrita — não no instante do cancel. Por isso a espera é um laço
	// com prazo, e não um sleep fixo: um sleep curto seria flaky e um longo
	// esconderia uma vaga que só volta em dez segundos.
	prazo := time.Now().Add(15 * time.Second)
	for {
		livre := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1"}`)
		if livre.StatusCode == http.StatusOK {
			n, codigo := desfechoOfStream(t, livre)
			if codigo != "" || n != 1 {
				t.Fatalf("depois de a vaga voltar: %d rows, fecho %q", n, codigo)
			}
			return
		}
		fecha(t, livre)
		if time.Now().After(prazo) {
			t.Fatal("a vaga não voltou depois de o cliente desistir: o servidor parou de aceitar consultas")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// O registro de queries em voo alimenta o /v1/stats: o operador precisa ver
// QUAL consulta está correndo e HÁ QUANTO TEMPO, não só quantas.
func TestStatsListaAConsultaEmVooComIdadeEMarcaDeLonga(t *testing.T) {
	srv := serverWithBlockOfQuery(t, rowsQueNaoCabemInSocket,
		blockPadrao, "query:\n  timeout: 1s\n  maxTimeout: 60s\n")

	// PRIMEIRO o caso vazio, e ele não é cerimônia: um registro que anota e não
	// retira passaria em qualquer asserção feita com uma consulta em voo, e o
	// sintoma em produção seria o pior possível — o painel mostrando consultas
	// de horas que terminaram há muito tempo, e o mapa crescendo sem teto por
	// requisição atendida.
	curta := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d LIMIT 1"}`)
	if n, codigo := desfechoOfStream(t, curta); codigo != "" || n != 1 {
		t.Fatalf("consulta curta: %d rows, fecho %q", n, codigo)
	}
	waitRecordVazio(t, srv)

	aberta := openStream(t, srv, `{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d","timeoutMs":30000}`)
	defer fecha(t, aberta.resp)
	if !aberta.reader.Next() {
		t.Fatalf("a consulta não produziu row: %v", aberta.reader.Err())
	}

	e := readJSON[protocol.Stats](t, srv, "/v1/stats")
	if len(e.Process.InFlight) != 1 {
		t.Fatalf("inFlight = %d entradas, quer 1", len(e.Process.InFlight))
	}
	v := e.Process.InFlight[0]
	if v.Dataset != "d" {
		t.Errorf("dataset = %q, quer %q", v.Dataset, "d")
	}
	if !v.Long {
		t.Error("long = false: a consulta pediu 30s num dataset de prazo padrão 1s")
	}
	if v.SQLHash == "" {
		t.Error("sqlHash vazio: sem ele o operador não distingue quatro consultas presas iguais de quatro diferentes")
	}
	// O texto do SQL NÃO pode vazar: ele carrega literais com dado do cliente,
	// e o §95 proíbe publicar payload por default.
	bruto, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(bruto), "SELECT") {
		t.Errorf("o SQL cru foi publicado no /v1/stats: %s", bruto)
	}
	if v.RunningMs < 0 {
		t.Errorf("runningMs = %d: idade não pode ser negativa", v.RunningMs)
	}
}

// waitRecordVazio bloqueia até o /v1/stats não listar mais consulta nenhuma.
//
// É laço com prazo, e não asserção direta, porque a saída do registro acontece
// nos defers do handler — que rodam DEPOIS de o último byte chegar ao cliente.
// Uma asserção imediata pegaria a consulta que acabou de terminar e piscaria; um
// sleep fixo esconderia um registro que só esvazia em dez segundos, ou nunca.
func waitRecordVazio(t *testing.T, srv *server.Server) {
	t.Helper()
	prazo := time.Now().Add(5 * time.Second)
	for {
		e := readJSON[protocol.Stats](t, srv, "/v1/stats")
		if len(e.Process.InFlight) == 0 {
			if v := valueOfMetrica(t, srv, "nishidb_query_longest_running_seconds"); v != 0 {
				t.Errorf("nishidb_query_longest_running_seconds = %v sem consulta em voo, quer 0", v)
			}
			return
		}
		if time.Now().After(prazo) {
			t.Fatalf("a consulta terminada continua no registro: %+v — o defer não retira, "+
				"e o mapa cresce por requisição atendida", e.Process.InFlight)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// As duas séries novas aparecem no scrape, e a de idade CRESCE sozinha.
//
// O crescimento é a asserção que importa: um medidor escrito na admissão
// ficaria congelado no instante em que a consulta começou, mostrando o mesmo
// número para sempre — e o estado que a série existe para denunciar é
// justamente aquele em que nada acontece e o tempo passa.
func TestMetricasDeQueryLongaAparecemNoScrapeECrescem(t *testing.T) {
	srv := serverWithBlockOfQuery(t, rowsQueNaoCabemInSocket,
		blockPadrao+"runtime:\n  query:\n    maxConcurrent: 2\n    maxLongRunning: 1\n",
		"query:\n  timeout: 1s\n  maxTimeout: 60s\n")

	aberta := openStream(t, srv, `{"dataset":"d","sql":"SELECT id, status, porta, ip FROM d","timeoutMs":30000}`)
	defer fecha(t, aberta.resp)
	if !aberta.reader.Next() {
		t.Fatalf("a consulta não produziu row: %v", aberta.reader.Err())
	}

	primeiro := valueOfMetrica(t, srv, "nishidb_query_longest_running_seconds")
	time.Sleep(150 * time.Millisecond)
	segundo := valueOfMetrica(t, srv, "nishidb_query_longest_running_seconds")
	if segundo <= primeiro {
		t.Errorf("idade da consulta mais antiga: %v e depois %v — o medidor não anda com o relógio",
			primeiro, segundo)
	}

	// A recusa do sub-pool alimenta o contador do PROCESSO, que já existe com
	// ZERO antes da primeira recusa.
	//
	// Este teste afirmava o contrário até 05/09/2026 — "as famílias deste
	// registry são lazy, e um contador zerado publicado em todo servidor
	// saudável seria uma série a mais em cada scrape do parque". O argumento
	// vale para família COM label, onde o custo é uma série por VALOR e
	// publicar `reason="senha"` com zero inventaria uma categoria que ninguém
	// exercitou; é por isso que a nishidb_auth_failures_total continua lazy.
	//
	// Aqui ele não se sustenta: as duas séries de recusa são declaradas SEM
	// label, então o custo é de duas séries por PROCESSO — e o que se comprava
	// com elas era um alarme que não dispara, porque
	// `absent(nishidb_query_long_rejected_total)` significava ao mesmo tempo
	// "nunca recusou" e "o alvo caiu". Série que alarma não pode nascer no
	// primeiro alarme (issue #47).
	const name = "nishidb_query_long_rejected_total"
	if v := valueOfMetrica(t, srv, name); v != 0 {
		t.Errorf("%s = %v antes de qualquer recusa, quer 0 — a série tem de existir zerada", name, v)
	}

	resp := consulta(t, srv, `{"dataset":"d","sql":"SELECT id FROM d","timeoutMs":30000}`)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, quer 503", resp.StatusCode)
	}
	fecha(t, resp)
	if v := valueOfMetrica(t, srv, name); v != 1 {
		t.Errorf("%s = %v, quer 1", name, v)
	}
}
