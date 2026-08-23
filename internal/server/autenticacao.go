// O middleware HTTP Basic e o mecanismo de papéis do §74-77 (Fase 11).
//
// A costura central deste arquivo é a que a B4 e a B5 herdam: montaMux
// constrói o mux CRU — só rotas, papel exigido por rota, ZERO autenticação —
// e comAutenticacao é o WRAPPER ÚNICO que a Fase 11 pede em torno dele. A B4
// serve o mux cru, sem passar por comAutenticacao nenhuma, no listener
// loopback: é assim que o painel local continua anônimo mesmo com
// server.authentication ligado no listener exposto. A B5 registra rotas
// ADMIN-only chamando roteador.registra com config.RoleAdmin — o mecanismo já
// sai desta task testado com um handler de teste (autenticacao_internal_test.go).
//
// Quando a autenticação está DESLIGADA (s.credenciais == nil, resolvido pela
// B2 em Novo()), comAutenticacao devolve o PRÓPRIO mux como http.Handler —
// não um wrapper que "deixa passar tudo", o mux em si. É o que garante
// paridade byte a byte com o piloto de hoje: não há função nenhuma a mais no
// caminho do request além do que já existia antes desta task.

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

// roteador é o mux cru mais o papel exigido por rota. O campo mux fica
// acessível dentro do pacote de propósito: é o que a B4 serve sem embrulho no
// listener loopback (http.Serve(loopbackLn, rt.mux)), e é o mesmo *http.ServeMux
// que comAutenticacao embrulha para o listener exposto — a MESMA definição de
// rota, dois caminhos de acesso.
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

// registra é COMO uma rota entra no mux com um papel mínimo — o mecanismo que
// a B5 usa para as rotas ADMIN-only que ainda não existem: basta chamar
// rt.registra(padrao, config.RoleAdmin, handler). O padrão aceita a mesma
// sintaxe de http.ServeMux.HandleFunc, incluindo o prefixo de método do Go 1.22
// ("GET /metrics").
func (rt *roteador) registra(padrao, papel string, h http.HandlerFunc) {
	rt.mux.HandleFunc(padrao, h)
	rt.papeis[padrao] = papel
}

// montaMux constrói o mux CRU do servidor: as cinco rotas de hoje, todas
// exigindo READ (o plano do §77 é claro — "no plano exposto TUDO exige
// auth" — e as rotas mutantes/ADMIN-only ficam para a B5). Sem nenhuma
// autenticação embutida: é exatamente o que sobeHTTP registrava antes desta
// task, palavra por palavra, só que agora nomeado e reutilizável pela B4.
//
// As rotas de /v1/query, /v1/metadata e /v1/stats são registradas SEM método
// no padrão, e o handler confere o método por dentro — a mesma razão de
// sempre: toda recusa do protocolo tem o MESMO corpo JSON, e o 405 que o mux
// geraria sozinho tem corpo em texto puro.
func (s *Servidor) montaMux() *roteador {
	rt := novoRoteador()
	rt.registra("GET /metrics", config.RoleRead, s.handleMetrics)
	rt.registra("GET /v1/health", config.RoleRead, s.handleHealth)
	rt.registra("/v1/query", config.RoleRead, s.handleQuery)
	rt.registra("/v1/metadata", config.RoleRead, s.handleMetadata)
	rt.registra("/v1/stats", config.RoleRead, s.handleStats)
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
func (s *Servidor) comAutenticacao(rt *roteador) http.Handler {
	if s.credenciais == nil {
		return rt.mux
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		papel, ok := s.autentica(w, r)
		if !ok {
			return // 401 já escrito por s.autentica (via recusaAutenticacao)
		}
		// mux.Handler consulta o roteamento SEM executá-lo — é a mesma
		// decisão que rt.mux.ServeHTTP tomaria a seguir, só que aqui só
		// precisamos do PADRÃO que casou (ou "" para rota desconhecida ou
		// método não aceito), para saber que papel a rota exige.
		_, padrao := rt.mux.Handler(r)
		if exigido, existe := rt.papeis[padrao]; existe && !papelAutorizado(papel, exigido) {
			s.recusaAutorizacao(w, r, padrao, papel, exigido)
			return
		}
		rt.mux.ServeHTTP(w, r)
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

// registraMetricasDeAuth instala a série de falha de autenticação. Fica fora
// de metricasDeQuery (metricas_query.go) de propósito: uma falha de auth
// acontece ANTES de qualquer dataset ser resolvido, então não existe label de
// dataset para carregar — misturar as duas famílias forçaria um valor de
// dataset artificial só para preencher a assinatura.
func registraMetricasDeAuth(reg *metrics.Registry) *metrics.FamiliaContador {
	return reg.Contador(metrics.MetAuthFailuresTotal,
		"falhas de autenticação HTTP Basic no protocolo remoto, por motivo", metrics.LabelReason)
}
