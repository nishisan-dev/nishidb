// O middleware HTTP Basic e o mecanismo de papéis do §74-77 (Fase 11).
//
// A costura central deste arquivo é a que a B4 e a B5 herdam: montaMux
// constrói o mux CRU — só rotas, papel exigido por rota, ZERO autenticação —
// e comAutenticacao é o WRAPPER ÚNICO que a Fase 11 pede em torno dele no
// listener EXPOSTO. A B5 registra as duas rotas ADMIN-only do §96 (POST
// /v1/compact e POST /v1/checkpoint) chamando roteador.registra com
// config.RoleAdmin — o mecanismo já saiu da B3 testado com um handler de
// teste (autenticacao_internal_test.go), e agora tem produção de verdade
// para exercitá-lo.
//
// Quando a autenticação está DESLIGADA (s.credenciais == nil, resolvido pela
// B2 em Novo()), comAutenticacao devolve o PRÓPRIO mux como http.Handler —
// não um wrapper que "deixa passar tudo", o mux em si. É o que garante
// paridade byte a byte com o piloto de hoje: não há função nenhuma a mais no
// caminho do request além do que já existia antes desta task.
//
// O listener LOOPBACK (B4) serve o MESMO mux sem autenticação — mas não mais
// cru: muxLoopback (mais abaixo) embrulha rt.Cru() para aplicar o RULING do
// plano do ciclo (§77b): o loopback é READ-only por construção. Uma rota
// ADMIN respondendo sem credencial no loopback seria stall de ingestão por
// qualquer usuário local sem privilégio, no host de múltiplos usuários do
// piloto — então padrão cujo papel registrado é ADMIN responde 403 também
// ali, reusando papelDaRota (a MESMA leitura de rt.papeis que comAutenticacao
// usa, nunca uma segunda classificação de rota). Rotas READ continuam
// anônimas no loopback, intocadas.

package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/metrics"
	"github.com/nishisan-dev/nishidb/internal/protocolo"
)

// roteador é o mux cru mais o papel exigido por rota.
//
// O campo mux é privado ao pacote de propósito (revisão pós-implementação):
// expor o *http.ServeMux em si daria a qualquer código dentro de
// internal/server um jeito de registrar rota SEM papel (rt.mux.HandleFunc
// direto, por fora de registra), e HandleFunc/Handle são capacidades de
// ESCRITA que ninguém além de registra deveria ter. Quem precisa só de
// SERVIR o mux cru — a B4, no listener loopback — usa Cru(), que devolve
// http.Handler: a interface tem SÓ ServeHTTP, sem HandleFunc/Handle. Isso não
// impede um bypass por quem insistir em usar o campo mux direto (Go não tem
// privacidade por arquivo dentro do mesmo pacote) — quem impede de fato é o
// fail-closed em comAutenticacao, que recusa qualquer rota casada sem entrada
// em papeis. As duas defesas são complementares: Cru() torna o bypass
// deliberado (não o caminho óbvio), e o fail-closed torna o bypass INÓCUO
// mesmo que aconteça.
type roteador struct {
	mux *http.ServeMux
	// papeis mapeia o PADRÃO do mux (o texto exato passado a
	// http.ServeMux.HandleFunc, ex. "GET /metrics" ou "/v1/query") para o
	// papel mínimo exigido. http.ServeMux.Handler(r) devolve esse mesmo texto
	// de padrão para um request já roteado, o que permite ao wrapper
	// perguntar "que papel esta rota exige?" sem duplicar a lógica de match
	// do mux.
	papeis map[string]string
}

func novoRoteador() *roteador {
	return &roteador{mux: http.NewServeMux(), papeis: make(map[string]string)}
}

// registra é a ÚNICA via correta para acrescentar rota — a B5 a chama para as
// rotas ADMIN-only que ainda não existem: basta rt.registra(padrao,
// config.RoleAdmin, handler). O padrão aceita a mesma sintaxe de
// http.ServeMux.HandleFunc, incluindo o prefixo de método do Go 1.22
// ("GET /metrics"). É a única função do pacote que grava em rt.papeis, e é
// essa gravação — não o HandleFunc em si — que faz uma rota ser reconhecida
// pelo fail-closed de comAutenticacao.
func (rt *roteador) registra(padrao, papel string, h http.HandlerFunc) {
	rt.mux.HandleFunc(padrao, h)
	rt.papeis[padrao] = papel
}

// Cru devolve o mux como http.Handler PURO — só ServeHTTP, sem acesso a
// HandleFunc/Handle. É isto que a B4 serve, sem embrulho, no listener
// loopback: http.Serve(loopbackLn, rt.Cru()), nunca o *http.ServeMux em si.
func (rt *roteador) Cru() http.Handler {
	return rt.mux
}

// papelDaRota resolve, para um request já roteado, o papel mínimo que a rota
// CASADA exige — a pergunta "que papel isto precisa?" que os DOIS pontos do
// pacote que decidem sobre papel fazem: comAutenticacao (papel insuficiente
// → 403, revisão da B3) e o wrapper do plano loopback (papel ADMIN → 403,
// RULING do §77b, Task B5). Extraída da closure original de comAutenticacao
// por instrução da revisão da B4: rt.papeis é a ÚNICA fonte de verdade sobre
// papel de rota, e duplicar esta leitura nos dois pontos divergiria no dia em
// que uma rota nova mudasse de papel e só um dos dois fosse atualizado.
//
// existe=false quando a rota é DESCONHECIDA ou o método não é aceito
// (mux.Handler devolve padrão vazio) — não há papel a checar, e o mux
// resolve 404/405 sozinho. existe=true com papel=="" é o BUG DE REGISTRO
// fail-closed (rota casou no mux sem passar por roteador.registra, a única
// função que grava em rt.papeis): um papel VÁLIDO nunca é string vazia — o
// enum é fechado em {config.RoleRead, config.RoleAdmin} desde a B1 —, então
// os dois chamadores tratam papel=="" com existe=true como recusa, nunca
// como ausência legítima de restrição.
//
// padrao também volta (revisão da B5, Minor 5): os dois chamadores
// precisam dele só nos ramos de recusa, para nomear a rota na mensagem de
// log/erro — devolvê-lo aqui evita que os dois repitam rt.mux.Handler(r),
// que já rodou dentro desta função.
func (rt *roteador) papelDaRota(r *http.Request) (papel, padrao string, existe bool) {
	_, padrao = rt.mux.Handler(r)
	return rt.papeis[padrao], padrao, padrao != ""
}

// montaMux constrói o mux CRU do servidor: as cinco rotas de LEITURA (READ) e
// as duas rotas MUTANTES do §96 (ADMIN, Task B5, decisão D3 do roadmap) —
// POST /v1/compact e POST /v1/checkpoint, as primeiras que mudam estado pelo
// protocolo remoto. Sem nenhuma autenticação embutida: é o roteamento cru,
// reutilizável pelos DOIS listeners (exposto, com comAutenticacao; loopback,
// com muxLoopback).
//
// CINCO das sete rotas (/v1/query, /v1/metadata, /v1/stats, /v1/compact,
// /v1/checkpoint) são registradas SEM método no padrão, e o handler confere
// o método por dentro — a mesma razão de sempre: toda recusa do protocolo
// tem o MESMO corpo JSON, e o 405 que o mux geraria sozinho tem corpo em
// texto puro. /metrics e /v1/health são a EXCEÇÃO, herdada de antes da Fase
// 11: o padrão já levava o prefixo "GET " (Go 1.22), e um método errado
// nelas sai com o 405 de texto puro do próprio mux, fora do contrato
// CorpoErro — nenhuma das duas tem handler que confira método por dentro.
func (s *Servidor) montaMux() *roteador {
	rt := novoRoteador()
	rt.registra("GET /metrics", config.RoleRead, s.handleMetrics)
	rt.registra("GET /v1/health", config.RoleRead, s.handleHealth)
	rt.registra("/v1/query", config.RoleRead, s.handleQuery)
	rt.registra("/v1/metadata", config.RoleRead, s.handleMetadata)
	rt.registra("/v1/stats", config.RoleRead, s.handleStats)
	rt.registra("/v1/compact", config.RoleAdmin, s.handleCompact)
	rt.registra("/v1/checkpoint", config.RoleAdmin, s.handleCheckpoint)
	return rt
}

// comAutenticacao é o wrapper único do §77: autentica (HTTP Basic contra
// s.credenciais), autoriza por papel, e só então delega ao mux cru.
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
// CASOU (padrao != "") mas não tem entrada em rt.papeis só pode significar
// que algum código registrou rota direto em rt.mux, por fora de
// roteador.registra — a única via que grava papel junto com o handler. Isso é
// bug de REGISTRO, não ausência legítima de restrição, e por isso não cai em
// "sem checagem": nomeia o defeito e recusa com 500. Padrão vazio (rota
// desconhecida ou método não aceito) segue direto para o mux, que resolve
// 404/405 sozinho — não há handler nenhum ali para vazar dado sem checagem.
func (s *Servidor) comAutenticacao(rt *roteador) http.Handler {
	if s.credenciais == nil {
		return rt.Cru()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		papel, ok := s.autentica(w, r)
		if !ok {
			return // 401 já escrito por s.autentica (via recusaAutenticacao)
		}
		// rt.papelDaRota consulta o roteamento SEM executá-lo — é a mesma
		// decisão que rt.mux.ServeHTTP tomaria a seguir, só que aqui só
		// precisamos saber que papel a rota CASADA exige (existe==false para
		// rota desconhecida ou método não aceito).
		exigido, padrao, existe := rt.papelDaRota(r)
		if existe {
			switch {
			case exigido == "":
				s.recusaRegistroSemPapel(w, r, padrao)
				return
			case !papelAutorizado(papel, exigido):
				s.recusaAutorizacao(w, r, padrao, papel, exigido)
				return
			}
		}
		rt.mux.ServeHTTP(w, r)
	})
}

// muxLoopback é o wrapper que o listener loopback (B4, http.go:sobeLoopback)
// serve em vez de rt.Cru() puro — o RULING do plano do ciclo (§77b, Task
// B5): o loopback SERVE sem autenticação (decisão 4 da B4), mas uma rota cujo
// papel registrado é ADMIN não pode responder anônima nele. O host do piloto
// tem múltiplos usuários locais, e compact/checkpoint anônimos por qualquer
// um deles é stall de ingestão sem privilégio nenhum de por meio.
//
// Reusa rt.papelDaRota — a MESMA leitura de rt.papeis que comAutenticacao usa
// — por instrução da revisão da B4: montaMux() é a única fonte de registro,
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
func (s *Servidor) muxLoopback(rt *roteador) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		papel, padrao, existe := rt.papelDaRota(r)
		if existe {
			switch papel {
			case "":
				s.recusaRegistroSemPapel(w, r, padrao)
				return
			case config.RoleAdmin:
				s.recusaLoopbackAdmin(w, r)
				return
			}
		}
		rt.Cru().ServeHTTP(w, r)
	})
}

// papelAutorizado aplica ADMIN ⊇ READ: ADMIN alcança toda rota de leitura, e
// READ só alcança rota READ. O enum de papel é fechado em {RoleRead,
// RoleAdmin} desde a B1 — um terceiro papel exigiria um caso novo aqui, não
// uma extensão silenciosa desta função.
func papelAutorizado(papelUsuario, papelExigido string) bool {
	if papelUsuario == config.RoleAdmin {
		return true
	}
	return papelUsuario == papelExigido
}

// atrasoDeFalha é o atraso FIXO aplicado a TODA falha de autenticação, sempre
// o mesmo valor, sem relação com qual foi a falha nem com quantas vieram
// antes deste request. Mitiga o timing attack de enumeração de usuário
// (responder mais rápido para "usuário não existe" do que para "senha
// errada" vazaria, pelo tempo, a mesma informação que o digest fantasma
// fecha pelo corpo). Não é lockout: não há contador por IP nem por usuário
// aqui — isso abriria a porta para auto-DoS contra um username conhecido, e
// o plano nomeia rate-limit por IP como débito separado (B7).
const atrasoDeFalha = 100 * time.Millisecond

// O enum FECHADO do label `reason` de nishidb_auth_failures_total. Fechado
// pela mesma razão de datasetDesconhecido (metricas_query.go): um label
// alimentado pelo texto que o cliente digitou seria uma série nova a cada
// tentativa, sem teto contra qualquer varredura de credencial.
const (
	razaoHeader  = "header"  // Authorization ausente, mal formado, ou sem "usuario:senha" decodificável
	razaoUsuario = "usuario" // o usuário do request não está em server.authentication.users
	razaoSenha   = "senha"   // o usuário existe; a senha não confere
)

// usuarioDesconhecidoNoLog é o valor do campo `user` do evento auth.recusada
// quando o username do request não casa com nenhum usuário CONFIGURADO. O
// dedo gordo mais comum neste formulário é digitar a SENHA no campo de
// usuário; ecoar esse valor cru no log vazaria a senha sob o rótulo "user".
const usuarioDesconhecidoNoLog = "desconhecido"

// credencialFantasma é o digest comparado contra um usuário INEXISTENTE, para
// que o caminho de código de "usuário não existe" seja O MESMO de "senha
// errada" até o ConstantTimeCompare — nenhum `if` a mais entre os dois casos
// além do que decide QUAL digest é o alvo da comparação. O texto de origem
// não precisa ser secreto: mesmo que uma senha real por coincidência fosse
// igual a ele, ela só autenticaria contra um username que, por definição,
// também não existe em s.credenciais — o resultado observável (401) seria o
// mesmo de qualquer forma.
var credencialFantasma = sha256.Sum256([]byte("nishidb-credencial-fantasma-nunca-configurada-por-um-operador-de-verdade"))

// autentica resolve o HTTP Basic do request contra s.credenciais.
//
// Devolve o papel do usuário autenticado e true quando a credencial confere.
// Em qualquer outro caso já escreveu o 401 inteiro (atraso, métrica, log e
// corpo) através de recusaAutenticacao, e devolve false — o chamador só
// precisa `return` sem tocar em w de novo.
func (s *Servidor) autentica(w http.ResponseWriter, r *http.Request) (papel string, ok bool) {
	usuario, senha, temCredencial := r.BasicAuth()
	if !temCredencial {
		// Cobre header ausente, "Basic" sem payload, base64 inválido e
		// payload decodificado sem ':' — r.BasicAuth devolve ok==false para
		// todas as quatro formas, sem sequer expor um username candidato.
		s.recusaAutenticacao(w, r, razaoHeader, usuarioDesconhecidoNoLog)
		return "", false
	}

	cred, existe := s.credenciais[usuario]
	alvo := credencialFantasma
	if existe {
		alvo = cred.digestSenha
	}

	// sha256 dos DOIS lados ANTES do ConstantTimeCompare: cred.digestSenha (ou
	// o fantasma) já são 32 bytes fixos (B2). A senha do request tem
	// comprimento ARBITRÁRIO — compará-la crua contra um digest faria
	// ConstantTimeCompare devolver "não bate" IMEDIATAMENTE por tamanho
	// diferente (a doc do pacote: "If the lengths of x and y do not match, it
	// returns 0 immediately"), o que vazaria pelo TEMPO o comprimento da
	// senha certa — quanto mais curta a senha tentada, mais cedo o compare
	// desistiria. Hashear os dois lados antes faz as duas entradas terem
	// SEMPRE 32 bytes, fechando esse canal antes mesmo de chegar ao compare.
	digest := sha256.Sum256([]byte(senha))
	combina := subtle.ConstantTimeCompare(digest[:], alvo[:]) == 1

	if !existe || !combina {
		razao := razaoUsuario
		usuarioNoLog := usuarioDesconhecidoNoLog
		if existe {
			// Só entra no log quando CASA com um usuário configurado (o
			// dedo gordo mais comum é digitar a senha no campo de usuário; ecoar
			// esse valor cru vazaria a senha sob o rótulo "user").
			razao = razaoSenha
			usuarioNoLog = usuario
		}
		s.recusaAutenticacao(w, r, razao, usuarioNoLog)
		return "", false
	}
	return cred.role, true
}

// recusaAutenticacao é o ÚNICO ponto de saída de toda falha de autenticação:
// atraso fixo, métrica, log e o 401 em si, sempre nesta ordem, sempre o MESMO
// corpo — é o que faz "usuário não existe" e "senha errada" serem
// indistinguíveis para quem só vê a resposta HTTP.
func (s *Servidor) recusaAutenticacao(w http.ResponseWriter, r *http.Request, razao, usuarioNoLog string) {
	s.authFalhas.Com(razao).Inc()

	// ANTES de responder, cobrindo TODA falha — inclusive header ausente, que
	// nem chega a comparar senha nenhuma. Sem isso, essas falhas sairiam mais
	// rápido que as de senha errada, e a diferença de tempo reabriria pelo
	// header a mesma pergunta ("existe caminho mais curto para alguma
	// resposta?") que o digest fantasma fecha para "o usuário existe?".
	time.Sleep(atrasoDeFalha)

	w.Header().Set("WWW-Authenticate", `Basic realm="nishidb"`)
	w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
	w.WriteHeader(http.StatusUnauthorized)
	// A mensagem é a MESMA para qualquer razão: variar o texto por razão
	// devolveria pelo corpo a distinção que o digest fantasma existe para
	// esconder. "Usuário não existe" e "senha errada" têm de produzir a
	// resposta EXATA, byte a byte (§77).
	if err := json.NewEncoder(w).Encode(protocolo.CorpoErro{Error: protocolo.Falha{
		Code:    protocolo.CodUnauthenticated,
		Message: "credenciais inválidas",
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 401 de autenticação", "erro", err.Error())
	}

	// remoto e user, no mesmo formato de query.recusada (query.go): o header
	// Authorization em si NUNCA é logado, em nível nenhum — nenhum caminho
	// acima passa r.Header adiante para o logger.
	s.log.Warn("auth.recusada", "requisição recusada por falha de autenticação",
		"remoto", r.RemoteAddr, "user", usuarioNoLog, "motivo", razao)
}

// recusaAutorizacao é o 403 de papel insuficiente. Diferente do 401, aqui não
// há preocupação de byte-identidade: o cliente já provou identidade com uma
// credencial válida, e dizer que papel falta não revela nada que ele já não
// soubesse (o papel exigido pela rota não é segredo, e o papel dele é o dele
// mesmo).
func (s *Servidor) recusaAutorizacao(w http.ResponseWriter, r *http.Request, rota, papelUsuario, papelExigido string) {
	w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
	w.WriteHeader(http.StatusForbidden)
	if err := json.NewEncoder(w).Encode(protocolo.CorpoErro{Error: protocolo.Falha{
		Code:    protocolo.CodForbidden,
		Message: fmt.Sprintf("papel %s não alcança esta rota; requer %s", papelUsuario, papelExigido),
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 403 de autorização", "erro", err.Error())
	}
	s.log.Warn("auth.negada", "requisição autenticada sem papel suficiente para a rota",
		"remoto", r.RemoteAddr, "rota", rota, "papel", papelUsuario, "exigido", papelExigido)
}

// recusaLoopbackAdmin é o 403 do RULING do §77b: rota ADMIN batida pelo
// listener loopback. Diferente de recusaAutorizacao, aqui não existe
// identidade nenhuma provada (o loopback não autentica ninguém) — a recusa é
// sobre o LISTENER, não sobre o papel de um usuário, e a mensagem orienta
// para o caminho que de fato alcança a rota: o listener exposto e autenticado.
func (s *Servidor) recusaLoopbackAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
	w.WriteHeader(http.StatusForbidden)
	if err := json.NewEncoder(w).Encode(protocolo.CorpoErro{Error: protocolo.Falha{
		Code:    protocolo.CodForbidden,
		Message: "rotas administrativas não respondem no plano loopback (§77b); use o listener autenticado (server.authentication)",
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 403 do loopback", "erro", err.Error())
	}
	s.log.Warn("auth.loopback_admin_recusado",
		"requisição ADMIN recusada no plano loopback: o loopback é READ-only por construção (§77b)",
		"remoto", r.RemoteAddr)
}

// recusaRegistroSemPapel é o fail-closed do mecanismo de papel (revisão
// pós-implementação): uma rota CASOU no mux (padrao != "") mas não tem
// entrada em rt.papeis. Isso só acontece se algum código dentro do pacote
// chamou rt.mux.HandleFunc/Handle diretamente, por fora de roteador.registra
// — a única via que grava em rt.papeis junto com o handler. Não é erro de
// CLIENTE nem ausência legítima de restrição: é bug de REGISTRO, e por isso
// nunca deixa passar sem checagem. 500 para aparecer alto em teste e em log —
// nunca um papel default silencioso (ADMIN implícito mascararia o bug até
// alguém notar em produção).
//
// O corpo HTTP é genérico de propósito (retoque da revisão da B3): nomear
// símbolos internos ("use roteador.registra") ou a própria rota para um
// cliente já AUTENTICADO devolveria detalhe de implementação a alguém que só
// tem uma credencial válida, sem relação com quem opera o servidor. O
// detalhe inteiro — rota e a dica de correção — vai SÓ para s.log.Error, que
// é quem o operador lê.
func (s *Servidor) recusaRegistroSemPapel(w http.ResponseWriter, r *http.Request, padrao string) {
	w.Header().Set("Content-Type", protocolo.ContentTypeJSON)
	w.WriteHeader(http.StatusInternalServerError)
	if err := json.NewEncoder(w).Encode(protocolo.CorpoErro{Error: protocolo.Falha{
		Code:    protocolo.CodInterno,
		Message: "erro interno do servidor",
	}}); err != nil {
		s.log.Warn("query.escrita_falhou", "não foi possível entregar o corpo do 500 de registro sem papel", "erro", err.Error())
	}
	s.log.Error("auth.rota_sem_papel",
		"rota registrada sem papel exigido — bug de registro, use roteador.registra, nunca HandleFunc direto",
		"remoto", r.RemoteAddr, "rota", padrao)
}

// registraMetricasDeAuth instala a série de falha de autenticação. Fica fora
// de metricasDeQuery (metricas_query.go) de propósito: uma falha de auth
// acontece ANTES de qualquer dataset ser resolvido, então não existe label de
// dataset para carregar — misturar as duas famílias forçaria um valor de
// dataset artificial só para preencher a assinatura.
func registraMetricasDeAuth(reg *metrics.Registry) *metrics.FamiliaContador {
	return reg.Contador(metrics.MetAuthFailuresTotal,
		"falhas de autenticação HTTP Basic no protocolo remoto, por motivo", metrics.LabelReason)
}
