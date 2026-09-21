// O middleware HTTP Basic e o mecanismo de papéis do §74-77 (Fase 11).
//
// A costura central deste arquivo é a que a B4 e a B5 herdam: buildMux
// constrói o mux CRU — só rotas, papel required por rota, ZERO autenticação —
// e withAuth é o WRAPPER ÚNICO que a Fase 11 pede em torno dele no
// listener EXPOSTO. A B5 registra as duas rotas ADMIN-only do §96 (POST
// /v1/compact e POST /v1/checkpoint) chamando router.registra com
// config.RoleAdmin — o mecanismo já saiu da B3 testado com um handler de
// teste (auth_internal_test.go), e agora tem produção de verdade
// para exercitá-lo.
//
// Quando a autenticação está DESLIGADA (s.users == nil, resolvido em
// Novo()), withAuth devolve o PRÓPRIO mux como http.Handler —
// não um wrapper que "deixa passar tudo", o mux em si. É o que garante
// paridade byte a byte com o piloto de hoje: não há função nenhuma a mais no
// caminho do request além do que já existia antes desta task.
//
// O listener LOOPBACK (B4) serve o MESMO mux sem autenticação — mas não mais
// cru: muxLoopback (mais abaixo) embrulha rt.Raw() para aplicar o RULING do
// plano do ciclo (§77b): o loopback é READ-only por construção. Uma rota
// ADMIN respondendo sem credencial no loopback seria stall de ingestão por
// qualquer usuário local sem privilégio, no host de múltiplos usuários do
// piloto — então padrão cujo papel registrado é ADMIN responde 403 também
// ali, reusando roleOfRota (a MESMA leitura de rt.papeis que withAuth
// usa, nunca uma segunda classificação de rota). Rotas READ continuam
// anônimas no loopback, intocadas.

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocol"
)

// router é o mux cru mais o papel required por rota.
//
// O campo mux é privado ao pacote de propósito (revisão pós-implementação):
// expor o *http.ServeMux em si daria a qualquer código dentro de
// internal/server um jeito de registrar rota SEM papel (rt.mux.HandleFunc
// direto, por fora de registra), e HandleFunc/Handle são capacidades de
// ESCRITA que ninguém além de registra deveria ter. Quem precisa só de
// SERVIR o mux cru — a B4, no listener loopback — usa Raw(), que devolve
// http.Handler: a interface tem SÓ ServeHTTP, sem HandleFunc/Handle. Isso não
// impede um bypass por quem insistir em usar o campo mux direto (Go não tem
// privacidade por arquivo dentro do mesmo pacote) — quem impede de fato é o
// fail-closed em withAuth, que recusa qualquer rota casada sem entrada
// em papeis. As duas defesas são complementares: Raw() torna o bypass
// deliberado (não o caminho óbvio), e o fail-closed torna o bypass INÓCUO
// mesmo que aconteça.
type router struct {
	mux *http.ServeMux
	// roles mapeia o PADRÃO do mux (o texto exato passado a
	// http.ServeMux.HandleFunc, ex. "GET /metrics" ou "/v1/query") para o
	// papel mínimo required. http.ServeMux.Handler(r) devolve esse mesmo texto
	// de padrão para um request já roteado, o que permite ao wrapper
	// perguntar "que papel esta rota exige?" sem duplicar a lógica de match
	// do mux.
	roles map[string]string
}

func newRouter() *router {
	return &router{mux: http.NewServeMux(), roles: make(map[string]string)}
}

// register é a ÚNICA via correta para acrescentar rota — a B5 a chama para as
// rotas ADMIN-only que ainda não existem: basta rt.register(pattern,
// config.RoleAdmin, handler). O padrão aceita a mesma sintaxe de
// http.ServeMux.HandleFunc, incluindo o prefixo de método do Go 1.22
// ("GET /metrics"). É a única função do pacote que grava em rt.papeis, e é
// essa gravação — não o HandleFunc em si — que faz uma rota ser reconhecida
// pelo fail-closed de withAuth.
func (rt *router) register(pattern, role string, h http.HandlerFunc) {
	rt.mux.HandleFunc(pattern, h)
	rt.roles[pattern] = role
}

// Raw devolve o mux como http.Handler PURO — só ServeHTTP, sem acesso a
// HandleFunc/Handle. É isto que a B4 serve, sem embrulho, no listener
// loopback: http.Serve(loopbackLn, rt.Raw()), nunca o *http.ServeMux em si.
func (rt *router) Raw() http.Handler {
	return rt.mux
}

// roleOfRoute resolve, para um request já roteado, o papel mínimo que a rota
// CASADA exige — a pergunta "que papel isto precisa?" que os DOIS pontos do
// pacote que decidem sobre papel fazem: withAuth (papel insuficiente
// → 403, revisão da B3) e o wrapper do plano loopback (papel ADMIN → 403,
// RULING do §77b, Task B5). Extraída da closure original de withAuth
// por instrução da revisão da B4: rt.papeis é a ÚNICA fonte de verdade sobre
// papel de rota, e duplicar esta leitura nos dois pontos divergiria no dia em
// que uma rota nova mudasse de papel e só um dos dois fosse atualizado.
//
// existe=false quando a rota é DESCONHECIDA ou o método não é aceito
// (mux.Handler devolve padrão vazio) — não há papel a checar, e o mux
// resolve 404/405 sozinho. existe=true com papel=="" é o BUG DE REGISTRO
// fail-closed (rota casou no mux sem passar por router.registra, a única
// função que grava em rt.papeis): um papel VÁLIDO nunca é string vazia — o
// enum é fechado em {config.RoleRead, config.RoleAdmin} desde a B1 —, então
// os dois chamadores tratam papel=="" com existe=true como recusa, nunca
// como ausência legítima de restrição.
//
// pattern também volta (revisão da B5, Minor 5): os dois chamadores
// precisam dele só nos ramos de recusa, para nomear a rota na mensagem de
// log/erro — devolvê-lo aqui evita que os dois repitam rt.mux.Handler(r),
// que já rodou dentro desta função.
func (rt *router) roleOfRoute(r *http.Request) (role, pattern string, exists bool) {
	_, pattern = rt.mux.Handler(r)
	return rt.roles[pattern], pattern, pattern != ""
}

// buildMux constrói o mux CRU do servidor: as cinco rotas de LEITURA (READ) e
// as duas rotas MUTANTES do §96 (ADMIN, Task B5, decisão D3 do roadmap) —
// POST /v1/compact e POST /v1/checkpoint, as primeiras que mudam estado pelo
// protocolo remoto. Sem nenhuma autenticação embutida: é o roteamento cru,
// reutilizável pelos DOIS listeners (exposto, com withAuth; loopback,
// com muxLoopback).
//
// TODAS as sete rotas são registradas SEM método no padrão, e cada handler
// confere o método por dentro. A razão é uma só: toda recusa do protocolo tem
// o MESMO corpo JSON, e o 405 que o mux geraria sozinho tem corpo em texto
// puro. O driver JDBC decodifica a recusa num caminho só, para traduzi-la em
// SQLState; uma rota que responde texto puro obriga a um segundo caminho.
//
// /metrics e /v1/health eram a exceção, herdada de antes da Fase 11 — o padrão
// levava o prefixo "GET " (Go 1.22) e o mux respondia sozinho. Ao tirar o
// método do padrão, o mux deixa de tratar HEAD como GET automaticamente, então
// as duas passam a aceitar os dois métodos POR DENTRO: quem raspa /metrics usa
// GET, mas health check por HEAD existe, e o objetivo desta uniformização é o
// CORPO da recusa, não estreitar o que já funciona.
func (s *Server) buildMux() *router {
	rt := newRouter()
	rt.register("/metrics", config.RoleRead, s.handleMetrics)
	rt.register("/v1/health", config.RoleRead, s.handleHealth)
	rt.register("/v1/query", config.RoleRead, s.handleQuery)
	rt.register("/v1/metadata", config.RoleRead, s.handleMetadata)
	rt.register("/v1/stats", config.RoleRead, s.handleStats)
	rt.register("/v1/compact", config.RoleAdmin, s.handleCompact)
	rt.register("/v1/checkpoint", config.RoleAdmin, s.handleCheckpoint)
	// As duas rotas dos contadores mantidos na escrita são ADMIN pela mesma
	// razão das duas acima: a auditoria segura um snapshot pela varredura do
	// dataset, e a reconstrução segura o writer. A LEITURA dos contadores
	// continua sendo SQL (`_counters.<nome>`), pela rota de consulta.
	rt.register("/v1/counters/audit", config.RoleAdmin, s.handleAuditOfCounters)
	rt.register("/v1/counters/rebuild", config.RoleAdmin, s.handleRebuildOfCounters)
	// Gestão de usuário (issue #40, users.go). A troca de senha é READ porque
	// todo usuário troca a PRÓPRIA; a regra "só a própria, salvo ADMIN" mora
	// no handler, que conhece o nome do principal.
	rt.register("/v1/users/create", config.RoleAdmin, s.handleUserCreate)
	rt.register("/v1/users/password", config.RoleRead, s.handleUserPassword)
	rt.register("/v1/users/role", config.RoleAdmin, s.handleUserRole)
	rt.register("/v1/users/delete", config.RoleAdmin, s.handleUserDelete)
	rt.register("/v1/replication/cursor", config.RoleAdmin, s.handleReplicationCursor)
	rt.register("/v1/replication/batch", config.RoleAdmin, s.handleReplicationBatch)
	rt.register("/v1/replication/snapshot", config.RoleAdmin, s.handleReplicationSnapshot)
	return rt
}

// withAuth é o wrapper único do §77: autentica (HTTP Basic contra a tabela
// de usuários, s.users), autoriza por papel, e só então delega ao mux cru,
// com o principal autenticado no context do request.
//
// A ORDEM dentro do handler é a razão de existir das três regras do plano
// (§77): a autenticação roda ANTES de qualquer coisa que o mux faria —
// inclusive antes de o mux decidir se a rota existe. Um path desconhecido sem
// credencial sai 401 e não 404 porque rt.mux.ServeHTTP só é chamado depois de
// s.autentica devolver ok==true; da mesma forma, o corpo da requisição nunca é
// lido e a vaga do §80 nunca é disputada para uma requisição que falha aqui,
// porque handleQuery — quem lê o corpo e disputa a vaga — está atrás desta
// checagem, nunca na frente dela.
//
// A decisão de papel é FAIL-CLOSED (revisão pós-implementação): um padrão que
// CASOU (pattern != "") mas não tem entrada em rt.papeis só pode significar
// que algum código registrou rota direto em rt.mux, por fora de
// router.registra — a única via que grava papel junto com o handler. Isso é
// bug de REGISTRO, não ausência legítima de restrição, e por isso não cai em
// "sem checagem": nomeia o defeito e recusa com 500. Padrão vazio (rota
// desconhecida ou método não aceito) segue direto para o mux, que resolve
// 404/405 sozinho — não há handler nenhum ali para vazar dado sem checagem.
func (s *Server) withAuth(rt *router) http.Handler {
	if s.users == nil {
		return rt.Raw()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := s.authenticate(w, r)
		if !ok {
			return // 401 já escrito por s.authenticate (via rejectAuth)
		}
		// rt.roleOfRota consulta o roteamento SEM executá-lo — é a mesma
		// decisão que rt.mux.ServeHTTP tomaria a seguir, só que aqui só
		// precisamos saber que papel a rota CASADA exige (exists==false para
		// rota desconhecida ou método não aceito).
		required, pattern, exists := rt.roleOfRoute(r)
		if exists {
			switch {
			case required == "":
				s.rejectRecordWithoutRole(w, r, pattern)
				return
			case !roleAuthorized(p.role, required):
				s.rejectAuthorization(w, r, pattern, p.role, required)
				return
			}
		}
		rt.mux.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), p)))
	})
}

// muxLoopback é o wrapper que o listener loopback (B4, http.go:risesLoopback)
// serve em vez de rt.Raw() puro — o RULING do plano do ciclo (§77b, Task
// B5): o loopback SERVE sem autenticação (decisão 4 da B4), mas uma rota cujo
// papel registrado é ADMIN não pode responder anônima nele. O host do piloto
// tem múltiplos usuários locais, e compact/checkpoint anônimos por qualquer
// um deles é stall de ingestão sem privilégio nenhum de por meio.
//
// Reusa rt.roleOfRota — a MESMA leitura de rt.papeis que withAuth usa
// — por instrução da revisão da B4: buildMux() é a única fonte de registro,
// então uma segunda classificação de rota aqui só poderia divergir dela, nunca
// completá-la. Rotas READ (papel == config.RoleRead) e padrão vazio (rota
// desconhecida ou método não aceito, existe==false) continuam servidas sem
// checagem nenhuma — o comportamento de hoje, intocado.
//
// O bug de registro (existe==true, papel=="") é recusado aqui também, com o
// MESMO 500 genérico do plano exposto: um "não sei que papel isto exige" não
// pode virar acesso anônimo só porque este listener não tem middleware de
// autenticação — a ausência de auth no loopback é uma decisão sobre rotas
// READ conhecidas, nunca uma licença para bug de registro passar batido.
func (s *Server) muxLoopback(rt *router) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role, pattern, exists := rt.roleOfRoute(r)
		if exists {
			switch role {
			case "":
				s.rejectRecordWithoutRole(w, r, pattern)
				return
			case config.RoleAdmin:
				s.rejectLoopbackAdmin(w, r)
				return
			}
		}
		rt.Raw().ServeHTTP(w, r)
	})
}

// roleAuthorized aplica ADMIN ⊇ READ: ADMIN alcança toda rota de leitura, e
// READ só alcança rota READ. O enum de papel é fechado em {RoleRead,
// RoleAdmin} desde a B1 — um terceiro papel exigiria um caso novo aqui, não
// uma extensão silenciosa desta função.
func roleAuthorized(userRole, requiredRole string) bool {
	if userRole == config.RoleAdmin {
		return true
	}
	return userRole == requiredRole
}

// delayOfFail é o atraso FIXO aplicado a TODA falha de autenticação, sempre
// o mesmo valor, sem relação com qual foi a falha nem com quantas vieram
// antes deste request. Mitiga o timing attack de enumeração de usuário
// (responder mais rápido para "usuário não existe" do que para "senha
// errada" vazaria, pelo tempo, a mesma informação que o digest fantasma
// fecha pelo corpo). Não é lockout: não há contador por IP nem por usuário
// aqui — isso abriria a porta para auto-DoS contra um username conhecido, e
// o plano nomeia rate-limit por IP como débito separado (B7).
const delayOfFail = 100 * time.Millisecond

// O enum FECHADO do label `reason` de nishidb_auth_failures_total. Fechado
// pela mesma razão de datasetDesconhecido (metrics_query.go): um label
// alimentado pelo texto que o cliente digitou seria uma série nova a cada
// tentativa, sem teto contra qualquer varredura de credencial.
const (
	reasonHeader   = "header"   // Authorization ausente, mal formado, ou sem "usuario:senha" decodificável
	reasonUser     = "user"     // o usuário do request não está na tabela de usuários
	reasonPassword = "password" // o usuário existe; a senha não confere
)

// unknownUserInLog é o valor do campo `user` do evento auth.recusada
// quando o username do request não casa com nenhum usuário CONFIGURADO. O
// dedo gordo mais comum neste formulário é digitar a SENHA no campo de
// usuário; ecoar esse valor cru no log vazaria a senha sob o rótulo "user".
const unknownUserInLog = "desconhecido"

// authenticate resolve o HTTP Basic do request contra a tabela de usuários.
//
// Devolve o principal autenticado e true quando a credencial confere. Em
// qualquer outro caso já escreveu o 401 inteiro (atraso, métrica, log e corpo)
// através de rejectAuth, e devolve false — o chamador só precisa `return` sem
// tocar em w de novo.
//
// O tempo constante entre "usuário não existe" e "senha errada" é da tabela
// (users.Store.Authenticate): os dois caminhos pagam o mesmo KDF, o
// inexistente contra um hash fantasma. Aqui só se decide a razão da métrica
// e do log, e a resposta HTTP é a mesma para as duas.
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request) (principal, bool) {
	username, password, hasCredential := r.BasicAuth()
	if !hasCredential {
		// Cobre header ausente, "Basic" sem payload, base64 inválido e
		// payload decodificado sem ':' — r.BasicAuth devolve ok==false para
		// todas as quatro formas, sem sequer expor um username candidato.
		s.rejectAuth(w, r, reasonHeader, unknownUserInLog)
		return principal{}, false
	}

	role, ok, known := s.users.Authenticate(username, []byte(password))
	if !ok {
		reason := reasonUser
		userInLog := unknownUserInLog
		if known {
			// Só entra no log quando CASA com um usuário da tabela (o dedo
			// gordo mais comum é digitar a senha no campo de usuário; ecoar
			// esse valor cru vazaria a senha sob o rótulo "user").
			reason = reasonPassword
			userInLog = username
		}
		s.rejectAuth(w, r, reason, userInLog)
		return principal{}, false
	}
	return principal{username: username, role: role}, true
}

// rejectAuth é o ÚNICO ponto de saída de toda falha de autenticação:
// atraso fixo, métrica, log e o 401 em si, sempre nesta ordem, sempre o MESMO
// corpo — é o que faz "usuário não existe" e "senha errada" serem
// indistinguíveis para quem só vê a resposta HTTP.
func (s *Server) rejectAuth(w http.ResponseWriter, r *http.Request, reason, userInLog string) {
	s.authFalhas.With(reason).Inc()

	// ANTES de responder, cobrindo TODA falha — inclusive header ausente, que
	// nem chega a comparar senha nenhuma. Sem isso, essas falhas sairiam mais
	// rápido que as de senha errada, e a diferença de tempo reabriria pelo
	// header a mesma pergunta ("existe caminho mais curto para alguma
	// resposta?") que o digest fantasma fecha para "o usuário existe?".
	time.Sleep(delayOfFail)

	w.Header().Set("WWW-Authenticate", `Basic realm="nishidb"`)
	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	w.WriteHeader(http.StatusUnauthorized)
	// A mensagem é a MESMA para qualquer razão: variar o texto por razão
	// devolveria pelo corpo a distinção que o digest fantasma existe para
	// esconder. "Usuário não existe" e "senha errada" têm de produzir a
	// resposta EXATA, byte a byte (§77).
	if err := json.NewEncoder(w).Encode(protocol.ErrorBody{Error: protocol.ErrorDetail{
		Code:    protocol.CodeUnauthenticated,
		Message: "credenciais inválidas",
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 401 de autenticação", "erro", err.Error())
	}

	// remoto e user, no mesmo formato de query.recusada (query.go): o header
	// Authorization em si NUNCA é logado, em nível nenhum — nenhum caminho
	// acima passa r.Header adiante para o logger.
	s.log.Warn("auth.recusada", "requisição recusada por falha de autenticação",
		"remote", r.RemoteAddr, "user", userInLog, "reason", reason)
}

// rejectAuthorization é o 403 de papel insuficiente. Diferente do 401, aqui não
// há preocupação de byte-identidade: o cliente já provou identidade com uma
// credencial válida, e dizer que papel falta não revela nada que ele já não
// soubesse (o papel required pela rota não é segredo, e o papel dele é o dele
// mesmo).
func (s *Server) rejectAuthorization(w http.ResponseWriter, r *http.Request, route, userRole, requiredRole string) {
	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	w.WriteHeader(http.StatusForbidden)
	if err := json.NewEncoder(w).Encode(protocol.ErrorBody{Error: protocol.ErrorDetail{
		Code:    protocol.CodeForbidden,
		Message: fmt.Sprintf("papel %s não alcança esta rota; requer %s", userRole, requiredRole),
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 403 de autorização", "erro", err.Error())
	}
	s.log.Warn("auth.negada", "requisição autenticada sem papel suficiente para a rota",
		"remote", r.RemoteAddr, "route", route, "role", userRole, "required", requiredRole)
}

// rejectLoopbackAdmin é o 403 do RULING do §77b: rota ADMIN batida pelo
// listener loopback. Diferente de rejectAuthorization, aqui não existe
// identidade nenhuma provada (o loopback não autentica ninguém) — a recusa é
// sobre o LISTENER, não sobre o papel de um usuário, e a mensagem orienta
// para o caminho que de fato alcança a rota: o listener exposto e autenticado.
func (s *Server) rejectLoopbackAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	w.WriteHeader(http.StatusForbidden)
	if err := json.NewEncoder(w).Encode(protocol.ErrorBody{Error: protocol.ErrorDetail{
		Code:    protocol.CodeForbidden,
		Message: "rotas administrativas não respondem no plano loopback (§77b); use o listener autenticado (server.authentication)",
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 403 do loopback", "erro", err.Error())
	}
	s.log.Warn("auth.loopback_admin_recusado",
		"requisição ADMIN recusada no plano loopback: o loopback é READ-only por construção (§77b)",
		"remote", r.RemoteAddr)
}

// rejectRecordWithoutRole é o fail-closed do mecanismo de papel (revisão
// pós-implementação): uma rota CASOU no mux (pattern != "") mas não tem
// entrada em rt.papeis. Isso só acontece se algum código dentro do pacote
// chamou rt.mux.HandleFunc/Handle diretamente, por fora de router.registra
// — a única via que grava em rt.papeis junto com o handler. Não é erro de
// CLIENTE nem ausência legítima de restrição: é bug de REGISTRO, e por isso
// nunca deixa passar sem checagem. 500 para aparecer alto em teste e em log —
// nunca um papel default silencioso (ADMIN implícito mascararia o bug até
// alguém notar em produção).
//
// O corpo HTTP é genérico de propósito (retoque da revisão da B3): nomear
// símbolos internos ("use router.registra") ou a própria rota para um
// cliente já AUTENTICADO devolveria detalhe de implementação a alguém que só
// tem uma credencial válida, sem relação com quem opera o servidor. O
// detalhe inteiro — rota e a dica de correção — vai SÓ para s.log.Error, que
// é quem o operador lê.
func (s *Server) rejectRecordWithoutRole(w http.ResponseWriter, r *http.Request, pattern string) {
	w.Header().Set("Content-Type", protocol.ContentTypeJSON)
	w.WriteHeader(http.StatusInternalServerError)
	if err := json.NewEncoder(w).Encode(protocol.ErrorBody{Error: protocol.ErrorDetail{
		Code:    protocol.CodeInternal,
		Message: "erro interno do servidor",
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 500 de registro sem papel", "erro", err.Error())
	}
	s.log.Error("auth.rota_sem_papel",
		"rota registrada sem papel required — bug de registro, use router.registra, nunca HandleFunc direto",
		"remote", r.RemoteAddr, "route", pattern)
}

// registerMetricsOfAuth instala a série de falha de autenticação. Fica fora
// de metricsOfQuery (metrics_query.go) de propósito: uma falha de auth
// acontece ANTES de qualquer dataset ser resolvido, então não existe label de
// dataset para carregar — misturar as duas famílias forçaria um valor de
// dataset artificial só para preencher a assinatura.
func registerMetricsOfAuth(reg *metrics.Registry) *metrics.CounterFamily {
	return reg.Counter(metrics.MetAuthFailuresTotal,
		"falhas de autenticação HTTP Basic no protocolo remoto, por motivo", metrics.LabelReason)
}
