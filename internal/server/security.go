package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"github.com/nishisan-dev/nishidb/internal/config"
	"github.com/nishisan-dev/nishidb/internal/logging"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// validateCertificateAtStartup confere, na subida e ANTES do bind, que o par
// certificado/chave de server.tls CARREGA — LoadX509KeyPair e não apenas
// "os arquivos existem", porque um PEM corrompido ou uma chave que não
// corresponde ao certificado só apareceria hoje no primeiro handshake TLS,
// e nesse ponto o listener já está aberto e aceitando conexão.
//
// Devolve o par CARREGADO (nil quando TLS está desligado) para que o Serve
// (B4, risesHTTP em server.go) reuse o MESMO tls.Certificate no ServeTLS, e
// avisaValidadeOfCert (abaixo) leia o leaf sem um segundo
// LoadX509KeyPair — o par nunca é relido do disco depois deste ponto;
// rotação de certificado é reiniciar o processo (limite nomeado: não há
// SIGHUP-reload aqui, débito 72).
func validateCertificateAtStartup(cfg *config.Config) (*tls.Certificate, error) {
	if !cfg.TLSEnabled() {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.Server.TLS.CertificateFile, cfg.Server.TLS.PrivateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("server.tls: certificado %s / chave %s: %w",
			cfg.Server.TLS.CertificateFile, cfg.Server.TLS.PrivateKeyFile, err)
	}
	return &cert, nil
}

// avisoOfExpirationOfCert é o piso do WARN de validade do leaf: sem
// SIGHUP-reload, trocar o certificado exige um restart planejado, e isso
// precisa de aviso ANTES do vencimento, não no dia — 30 dias dá tempo de
// pedir um certificado novo a uma CA corporativa sem pressa (§ decisão 4 do
// plano do ciclo).
const avisoOfExpirationOfCert = 30 * 24 * time.Hour

// avisaValidadeOfCert parseia o leaf do certificado já CARREGADO por
// validateCertificateAtStartup (nunca relido do disco) e publica a métrica de
// expiração — SEMPRE que TLS está ligado, para que o operador derive "quanto
// falta" no painel/alerta sem que esta função precise decidir o que é "pouco
// tempo" além do próprio WARN abaixo.
//
// NUNCA recusa a subida por causa da validade: um certificado expirado ainda
// cifra o transporte (a cadeia é validada pelo CLIENTE no handshake, não pelo
// servidor), e recusar aqui derrubaria as consultas por um problema que o
// operador corrige sem downtime nenhum se o aviso chegar a tempo — é
// exatamente o cenário que "ERRO no log + métrica" cobre em vez de recusa.
// Só devolve erro se o leaf em si não decodificar, o que não deveria
// acontecer com um par que já passou por LoadX509KeyPair (violação de
// invariante).
func avisaValidadeOfCert(cert tls.Certificate, log *logging.Logger, reg *metrics.Registry) error {
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("server.tls: leaf do certificado não decodifica (violação de invariante: "+
			"LoadX509KeyPair já deveria ter recusado): %w", err)
	}

	reg.Gauge(metrics.MetTLSCertExpiryTimestamp,
		"epoch segundos do NotAfter do leaf de server.tls; o operador deriva quanto falta no painel/alerta").
		Unlabeled().Set(float64(leaf.NotAfter.Unix()))

	notAfter := leaf.NotAfter.UTC().Format(time.RFC3339)
	switch restante := time.Until(leaf.NotAfter); {
	case restante <= 0:
		log.Error("tls.certificado_expirado",
			"o certificado de server.tls já expirou; o servidor SOBE mesmo assim (recusar derrubaria "+
				"as consultas) — troque o par certificado/chave e reinicie (rotação é restart, sem "+
				"SIGHUP-reload)",
			"notAfter", notAfter)
	case restante < avisoOfExpirationOfCert:
		log.Warn("tls.certificado_expirando",
			"o certificado de server.tls vence em menos de 30 dias",
			"notAfter", notAfter)
	}
	return nil
}

// loopbackAddress resolve o endereço do listener secundário do plano
// loopback (decisão 4 do plano do ciclo, §77-78) a partir de
// server.loopback. Devolve "" quando desligado — nenhum listener a mais.
//
// Quando ligado, server.loopback.port é OBRIGATÓRIO: a B1 deixou Port
// *int sem default de propósito (nenhuma das 8 recusas do parse cobria o
// caso, ver task-b1-report.md), sinalizando que a ATIVAÇÃO decidiria. Sem
// porta declarada não há endereço determinístico para bindar, e um default
// silencioso (a porta principal + 1, por exemplo) escolheria pelo operador
// numa decisão que é dele — a mesma doutrina do resto do §77: recusar,
// nomeando a chave, nunca assumir.
func loopbackAddress(cfg *config.Config) (string, error) {
	if !cfg.LoopbackEnabled() {
		return "", nil
	}
	if cfg.Server.Loopback.Port == nil {
		return "", fmt.Errorf(
			"server.loopback.enabled: true exige server.loopback.port declarado (sem default: a porta é " +
				"decisão do operador, não do processo)")
	}
	return fmt.Sprintf("127.0.0.1:%d", *cfg.Server.Loopback.Port), nil
}

// evaluateExposicaoEAuth substitui o aviso avisaAboutAExposicao (que só
// CONSTATAVA exposição e nunca recusava) pela matriz de recusa do §77: a
// combinação de bind exposto, autenticação e TLS decide se o servidor PODE
// subir, e não só se ele deveria avisar. Run ANTES do bind.
//
// As cinco células da matriz do plano (a ordem de checagem abaixo não é a
// ordem da tabela, mas cobre as mesmas cinco, cada uma com teste em
// security_test.go):
//
//  1. bind em loopback, qualquer autenticação/TLS -> sobe. Avisa
//     "auth.sem_tls" se a autenticação estiver ligada sem TLS: HTTP Basic
//     transmite a senha em texto claro mesmo que só alcançável pelo próprio
//     host, e isso é diferente de seguro — só é um risco menor.
//  2. bind exposto + server.authentication AUSENTE do arquivo -> recusa
//     pedindo decisão explícita. O KnownFields GLOBAL continua desligado
//     (doutrina preservada pela B1): sem esta checagem, um
//     `authentification:` (typo) desapareceria em silêncio e o servidor
//     subiria exposto e sem senha nenhuma, achando (e deixando o operador
//     achar) que tinha autenticação.
//  3. bind exposto + authentication.enabled: false EXPLÍCITO -> sobe com o
//     aviso atual (bind.exposto): o operador já decidiu, e a decisão dele
//     vale.
//  4. bind exposto + autenticação ligada + TLS desligado -> recusa (§77):
//     HTTP Basic sem TLS transmitiria a senha em texto claro pela rede
//     aberta — não é o mesmo risco pequeno do loopback.
//  5. bind exposto + autenticação ligada + TLS ligado -> sobe, sem aviso: é
//     a combinação que o §77 pede.
func evaluateExposicaoEAuth(cfg *config.Config, log *logging.Logger) error {
	if !cfg.BindExposed() {
		if cfg.AuthenticationEnabled() && !cfg.TLSEnabled() {
			log.Warn("auth.sem_tls",
				"server.authentication está ligada sem server.tls: HTTP Basic transmite a senha em texto "+
					"claro, mesmo que o bind só seja alcançável pelo próprio host",
				"endereco", cfg.Addr())
		}
		return nil
	}

	if !cfg.AuthenticationDeclared() {
		return fmt.Errorf(
			"server.bind (%s) está exposto e server.authentication não aparece na configuração: "+
				"declare a seção explicitamente — enabled: true com server.tls.enabled: true, ou "+
				"enabled: false assumindo o risco — para que a ausência de credencial na rede seja uma "+
				"decisão do operador, e não um typo engolido em silêncio (§77)",
			cfg.Server.Bind)
	}

	if !cfg.AuthenticationEnabled() {
		// enabled: false EXPLÍCITO: a decisão já foi tomada, e o aviso de
		// sempre é o que resta a fazer.
		avisaAboutAExposicao(cfg, log)
		return nil
	}

	if !cfg.TLSEnabled() {
		return fmt.Errorf(
			"server.bind (%s) está exposto com server.authentication.enabled: true e server.tls.enabled: "+
				"false: autenticação HTTP Basic sem TLS transmitiria a senha em texto claro pela rede "+
				"(§77); ligue server.tls.enabled ou restrinja server.bind ao loopback",
			cfg.Server.Bind)
	}
	return nil
}

// avisaAboutAExposicao registra, uma vez por subida, que o bind escolhido
// alcança a rede. Só é chamada quando o operador já tomou a decisão — bind
// exposto com authentication.enabled: false EXPLÍCITO (célula 3 da matriz
// acima); a recusa por ausência da seção (célula 2) e a recusa por
// autenticação sem TLS (célula 4) não chegam aqui.
//
// A mensagem NÃO pode dizer que falta autenticação: a Fase 11 entregou o
// §74-77, e esta célula só é alcançada porque o OPERADOR desligou
// authentication.enabled explicitamente — "ainda não tem" seria mentira
// sobre uma escolha que ele fez de propósito. E não pode citar só
// /v1/query: desde a Fase 11 a mesma porta sem credencial também serve
// POST /v1/compact e POST /v1/checkpoint (ADMIN) anônimos no plano
// principal (godoc de buildMux/withAuth, auth.go) — quem
// alcança o bind lê E MUTA o dataset, não só lê.
func avisaAboutAExposicao(cfg *config.Config, log *logging.Logger) {
	log.Warn("bind.exposto",
		"o servidor vai escutar FORA do loopback com server.authentication.enabled: false — decisão explícita "+
			"do operador, não lacuna do produto: quem alcançar esta porta lê (POST /v1/query) E MUTA "+
			"(POST /v1/compact, POST /v1/checkpoint) o dataset inteiro, sem credencial nenhuma",
		"endereco", cfg.Addr())
}
