package server

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// permitidoInLeitura é o valor do header Allow das duas rotas que aceitam HEAD.
//
// HEAD é GET sem corpo, e o net/http já o serve assim quando o handler escreve
// normalmente — descartar o corpo é trabalho dele. Aceitá-lo aqui preserva o
// que o mux fazia sozinho quando o padrão era "GET /metrics".
const permitidoInLeitura = "GET, HEAD"

// metodoOfLeitura responde se o método é um dos que /metrics e /v1/health
// servem.
func metodoOfLeitura(r *http.Request) bool {
	return r.Method == http.MethodGet || r.Method == http.MethodHead
}

// handleMetrics serve a exposição do §93.
//
// O lag é atualizado AQUI, no scrape, e não por um timer: perguntar ao broker
// num timer publicaria um número velho cuja idade o operador não conhece.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// PRIMEIRA linha do handler, antes do laço abaixo. Enquanto o método vivia
	// no padrão de registro, o mux recusava de graça; agora a recusa é nossa, e
	// pô-la depois do laço faria cada POST inválido disparar a consulta ao
	// broker, com o timeout de 5 s do scrape — numa rota que um cron raspa.
	if !metodoOfLeitura(r) {
		s.rejectSimples(w, r, permitidoInLeitura)
		return
	}
	ctx, cancel := timeoutOfScrape(r)
	defer cancel()
	// Em PARALELO, e com prazo POR DATASET. Em série, dentro de um orçamento
	// único, o último dataset da lista recebe o que sobrou dos anteriores — e
	// com brokers lentos isso é zero, então ele nunca publica marca d'água
	// enquanto os primeiros publicam sempre. O prazo próprio é o que mantém a
	// leitura de cada dataset independente da sorte dos vizinhos.
	var wait sync.WaitGroup
	for _, d := range s.datasets {
		d.mu.Lock()
		cons := d.cons
		d.mu.Unlock()
		if cons == nil {
			continue
		}
		wait.Add(1)
		go func(c *kafka.Consumidor) {
			defer wait.Done()
			ctxD, cancelD := context.WithTimeout(ctx, kafka.PrazoDaMarcaDagua)
			defer cancelD()
			c.AtualizaMarkDagua(ctxD)
		}(cons)
	}
	wait.Wait()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := s.reg.WriteTo(w); err != nil {
		s.log.Warn("metrics.escrita_falhou", "scrape interrompido", "erro", err.Error())
	}
}

func timeoutOfScrape(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 5*time.Second)
}

// handleHealth serve o §94.
//
// A chave "kafka" agrega: basta UM dataset com ingestão parada para ela sair
// de UP. É deliberado — um health que dissesse UP com metade da ingestão
// parada seria pior que não existir.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !metodoOfLeitura(r) {
		s.rejectSimples(w, r, permitidoInLeitura)
		return
	}
	h := protocol.Health{
		Status:   protocol.StateUp,
		Kafka:    protocol.StateUp,
		Storage:  protocol.StateUp,
		Datasets: make(map[string]string, len(s.datasets)),
		Details:  map[string]string{},
	}
	withIngestion := 0

	for _, d := range s.datasets {
		state, detalhe := stateOfIngestion(d)
		if state != protocol.StateDisabled {
			withIngestion++
		}
		if detalhe != "" {
			h.Details[d.nome] = detalhe
		}
		h.Datasets[d.nome] = state

		switch state {
		case protocol.StateDown:
			h.Kafka = protocol.StateDown
			h.Status = protocol.StateDegraded
		case protocol.StateDegraded:
			if h.Kafka == protocol.StateUp {
				h.Kafka = protocol.StateDegraded
			}
		}
	}
	if withIngestion == 0 {
		// Nenhum dataset consome: dizer "kafka: UP" seria afirmar saúde de um
		// subsistema que não está em uso.
		h.Kafka = protocol.StateDisabled
	}

	w.Header().Set("Content-Type", "application/json")
	if h.Status != protocol.StateUp {
		// 503 para que um load balancer tire a instância de rotação sem
		// precisar interpretar o corpo.
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(h); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		s.log.Warn("health.escrita_falhou", "resposta interrompida", "erro", err.Error())
	}
}

// stateOfIngestion classifica a ingestão de UM dataset no vocabulário do §94, e
// devolve junto o motivo — vazio quando não há o que explicar.
//
// É uma função só porque tem dois chamadores: o /v1/health e o /v1/stats. Com a
// classificação duplicada, as duas rotas descreveriam a mesma realidade com
// palavras diferentes no dia em que uma delas ganhasse um caso novo — e o
// operador teria de descobrir qual das duas está certa.
func stateOfIngestion(d *dataset) (estado, detalhe string) {
	d.mu.Lock()
	cons, err := d.cons, d.erroIngestao
	d.mu.Unlock()

	k := d.spec.Source.Kafka
	switch {
	case k == nil || !k.Enabled:
		// Sem fonte declarada, ou pausado pelo `enabled: false`. Não é
		// anomalia e não pode contaminar a chave agregada.
		return protocol.StateDisabled, ""
	case err != nil:
		return protocol.StateDown, err.Error()
	case cons == nil:
		return protocol.StateDegraded, "ingestão ainda subindo"
	}
	if p := cons.Pausa(); p != nil {
		return protocol.StateDown, p.Error()
	}
	return protocol.StateUp, ""
}

// risesPprof liga os endpoints do §112 num listener PRÓPRIO.
// taxaOfBlockProfile amostra um evento de bloqueio a cada N nanossegundos de
// espera acumulada. 10 µs registra o que dura o suficiente para importar sem
// instrumentar cada sincronização curta.
const taxaOfBlockProfile = 10_000

// fractionOfMutexProfile amostra 1 em cada N eventos de contenção. 100 é o valor
// que a documentação do runtime usa como ponto de partida razoável.
const fractionOfMutexProfile = 100

func (s *Server) risesPprof(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// O Index já serve /debug/pprof/block e /debug/pprof/mutex, e os DOIS
	// nascem desligados no runtime: sem SetBlockProfileRate e
	// SetMutexProfileFraction eles respondem "sampling period=0", ou seja, um
	// perfil vazio. Conferido no piloto em 28/08/2026 — as duas rotas
	// existiam e não serviam para nada.
	//
	// Isso importa porque contenção de lock é exatamente o que este produto
	// precisa diagnosticar: o writer é serializado por um mutex por dataset, e
	// stalls de segundos já foram observados sem atribuição possível.
	//
	// Ligados aqui, e não por default no processo: a amostragem custa em TODA
	// aquisição de lock e em TODO bloqueio, e o pprof já é modo
	// administrativo. Quem abre a porta aceita o preço; quem não a abre não
	// paga nada.
	runtime.SetBlockProfileRate(taxaOfBlockProfile)
	runtime.SetMutexProfileFraction(fractionOfMutexProfile)

	s.pprofSrv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := s.pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("pprof.falhou", "endpoint de debug caiu", "erro", err.Error())
		}
	}()
	s.log.Warn("pprof.ligado",
		"endpoints de debug ATIVOS; o §112 os autoriza apenas em modo administrativo — não exponha esta porta",
		"endereco", addr)
}

// risesLoopback liga o SEGUNDO listener do plano loopback (decisão 4 do plano
// do ciclo, §77-78): serve o MESMO mux (montado de novo por buildMux — sem
// estado compartilhado, ver o report da B3), sem TLS e sem o middleware de
// autenticação, sobre s.loopbackLn (já bindado por Novo).
//
// O handler NÃO é mais rt.Raw() puro desde a Task B5: é muxLoopback(rt), que
// aplica o RULING do §77b sobre o MESMO mux — rota READ continua anônima
// (paridade com o que este listener já fazia), rota ADMIN (as duas novas do
// §96) responde 403. É o que preserva o painel, o script de update e o cron
// locais funcionando sem credencial, sem abrir compact/checkpoint anônimos
// para qualquer usuário local do host do piloto — precedente arquitetural:
// risesPprof, acima, também é um segundo listener isolado do principal,
// embora com objetivo diferente (debug vs. ferramental local).
func (s *Server) risesLoopback() {
	rt := s.buildMux()
	s.loopbackSrv = &http.Server{
		Handler:           s.muxLoopback(rt),
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          s.errorLog("loopback"),
	}
	go func() {
		if err := s.loopbackSrv.Serve(s.loopbackLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("loopback.falhou", "o listener loopback caiu", "erro", err.Error())
		}
	}()
	s.log.Info("loopback.ligado",
		"listener sem TLS/autenticação de pé (decisão 4, plano duplo de observabilidade)",
		"endereco", s.loopbackLn.Addr().String())
}

// errorLog adapta o Logger estruturado para o *log.Logger que
// http.Server.ErrorLog exige — net/http só sabe escrever para um io.Writer
// cru, nunca para o logger estruturado do §95 diretamente. Sem isto, o
// ruído de handshake que um scanner corporativo gera na porta exposta (TLS
// de versão errada, HTTP puro na porta TLS) cairia como log.Printf cru no
// journal, fora do formato JSON/texto do resto do processo.
//
// listener nomeia a origem ("principal" ou "loopback") — os DOIS http.Server
// usam este adaptador, cada um com o seu rótulo, para que uma linha de erro
// não fique ambígua sobre qual dos dois listeners a gerou.
func (s *Server) errorLog(listener string) *log.Logger {
	return log.New(errorLogForLogger{log: s.log, listener: listener}, "", 0)
}

// errorLogForLogger é o io.Writer por trás de errorLog: cada Write de
// net/http vira UM evento estruturado, nunca uma linha crua.
type errorLogForLogger struct {
	log      *logging.Logger
	listener string
}

func (w errorLogForLogger) Write(p []byte) (int, error) {
	w.log.Warn("http.erro_transporte",
		"net/http reportou um erro de conexão/handshake (comum com scanners batendo na porta)",
		"listener", w.listener, "mensagem", strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// Pausas devolve, por dataset, o motivo de a ingestão estar parada. Vazio
// quando tudo corre bem. Serve ao teste e ao futuro `nishidb status`.
func (s *Server) Pausas() map[string]error {
	out := map[string]error{}
	for _, d := range s.datasets {
		d.mu.Lock()
		cons, err := d.cons, d.erroIngestao
		d.mu.Unlock()
		switch {
		case err != nil:
			out[d.nome] = err
		case cons != nil:
			if p := cons.Pausa(); p != nil {
				out[d.nome] = p
			}
		}
	}
	return out
}
