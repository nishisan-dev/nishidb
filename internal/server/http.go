package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/pprof"
	"time"
)

// handleMetrics serve a exposição do §93.
//
// O lag é atualizado AQUI, no scrape, e não por um timer: perguntar ao broker
// num timer publicaria um número velho cuja idade o operador não conhece.
func (s *Servidor) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancela := timeoutDoScrape(r)
	defer cancela()
	for _, d := range s.datasets {
		d.mu.Lock()
		cons := d.cons
		d.mu.Unlock()
		if cons != nil {
			cons.AtualizaLag(ctx)
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := s.reg.WriteTo(w); err != nil {
		s.log.Warn("metrics.escrita_falhou", "scrape interrompido", "erro", err.Error())
	}
}

func timeoutDoScrape(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 5*time.Second)
}

// Saude é o corpo do §94.
type Saude struct {
	Status   string            `json:"status"`
	Kafka    string            `json:"kafka"`
	Storage  string            `json:"storage"`
	Datasets map[string]string `json:"datasets"`
	// Detalhes nomeia o problema por dataset. Não está no §94 e existe
	// porque "DOWN" sozinho manda o operador procurar no log de qual dos
	// datasets, num processo que pode hospedar vários (§82).
	Detalhes map[string]string `json:"detalhes,omitempty"`
}

// handleHealth serve o §94.
//
// A chave "kafka" agrega: basta UM dataset com ingestão parada para ela sair
// de UP. É deliberado — um health que dissesse UP com metade da ingestão
// parada seria pior que não existir.
func (s *Servidor) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h := Saude{
		Status:   EstadoUP,
		Kafka:    EstadoUP,
		Storage:  EstadoUP,
		Datasets: make(map[string]string, len(s.datasets)),
		Detalhes: map[string]string{},
	}
	comIngestao := 0

	for _, d := range s.datasets {
		estado := EstadoUP
		d.mu.Lock()
		cons, erro := d.cons, d.erroIngestao
		d.mu.Unlock()

		k := d.spec.Source.Kafka
		switch {
		case k == nil || !k.Enabled:
			// Sem fonte declarada, ou pausado pelo `enabled: false`. Não é
			// anomalia e não pode contaminar a chave agregada.
			estado = EstadoDesligado
		default:
			comIngestao++
			switch {
			case erro != nil:
				estado = EstadoDown
				h.Detalhes[d.nome] = erro.Error()
			case cons == nil:
				estado = EstadoDegraded
				h.Detalhes[d.nome] = "ingestão ainda subindo"
			default:
				if p := cons.Pausa(); p != nil {
					estado = EstadoDown
					h.Detalhes[d.nome] = p.Error()
				}
			}
		}
		h.Datasets[d.nome] = estado

		switch estado {
		case EstadoDown:
			h.Kafka = EstadoDown
			h.Status = EstadoDegraded
		case EstadoDegraded:
			if h.Kafka == EstadoUP {
				h.Kafka = EstadoDegraded
			}
		}
	}
	if comIngestao == 0 {
		// Nenhum dataset consome: dizer "kafka: UP" seria afirmar saúde de um
		// subsistema que não está em uso.
		h.Kafka = EstadoDesligado
	}

	w.Header().Set("Content-Type", "application/json")
	if h.Status != EstadoUP {
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

// sobePprof liga os endpoints do §112 num listener PRÓPRIO.
func (s *Servidor) sobePprof(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

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

// Pausas devolve, por dataset, o motivo de a ingestão estar parada. Vazio
// quando tudo corre bem. Serve ao teste e ao futuro `nishidb status`.
func (s *Servidor) Pausas() map[string]error {
	out := map[string]error{}
	for _, d := range s.datasets {
		d.mu.Lock()
		cons, erro := d.cons, d.erroIngestao
		d.mu.Unlock()
		switch {
		case erro != nil:
			out[d.nome] = erro
		case cons != nil:
			if p := cons.Pausa(); p != nil {
				out[d.nome] = p
			}
		}
	}
	return out
}
