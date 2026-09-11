package cli

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// Credencial e TLS dos comandos remotos (Fase 11, Task B6) — o que torna o
// protocolo do §74-77 (Task B3) e o TLS do servidor (Task B4) alcançáveis
// pelo operador que só tem o binário do CLI.
//
// TODO comando remoto (`query --server`, `status`, `datasets`, `inspect`,
// `compact`, `checkpoint`) chama setupRemoteFlags no seu FlagSet e, logo
// após o Parse, remoteFlags.resolve() — o ÚNICO lugar em que as fontes de
// senha são lidas e validadas. Dali em diante o comando carrega só um
// remoteCredential, que os TRÊS pontos de costura do protocolo remoto
// (remoteQuery em query_remote.go, searchDocument em operation.go,
// postMutation em mutation.go — a B5 já apontava o último deles) consomem via
// remoteClient (monta o *http.Client, com Proxy: nil e o CertPool de
// --tls-ca) e aplicaCredencial (o req.SetBasicAuth, quando --user foi dado).
//
// NUNCA existe uma flag de senha: apareceria em `ps aux` para qualquer
// usuário da máquina (§95). A senha entra SOMENTE por NISHIDB_PASSWORD (env,
// que não sobrevive a um `ps`) ou --password-file — o mesmo desenho do
// servidor (internal/server/security.go:resolvePassword — trailing newline
// aparado), mas SEM a checagem de modo 0600 de lá: aquele arquivo é lido por
// um PROCESSO SERVIDOR que pode compartilhar host com outros usuários e
// datasets sensíveis; este é lido pelo comando do PRÓPRIO operador, na
// máquina dele — exigir chmod aqui teria custo (mais um jeito de o comando
// falhar) sem reduzir exposição real nenhuma, e por isso a doutrina diverge
// aqui, deliberadamente.

// envPass é o ÚNICO nome de variável de ambiente aceito para a senha do
// cliente — diferente de server.authentication.users.passwordEnv (que
// NOMEIA uma variável arbitrária por usuário), aqui o cliente escolhe um
// nome fixo, o que permite documentar "NISHIDB_PASSWORD" sem indireção
// nenhuma.
const envPass = "NISHIDB_PASSWORD"

// remoteCredential é o que os três pontos de costura recebem depois que
// remoteFlags.resolve() validou as fontes. user/senha vazios (o caso de
// hoje, sem --user) significam "sem Authorization" — a paridade byte a byte
// com o comportamento anterior a esta task.
type remoteCredential struct {
	user  string
	pass  string
	tlsCA string
}

// remoteFlags são as três flags que TODO comando remoto aceita.
// setupRemoteFlags as acrescenta ao FlagSet do comando, ANTES do Parse —
// o mesmo padrão de --server em cada um deles.
type remoteFlags struct {
	user         *string
	passwordFile *string
	tlsCA        *string
}

func setupRemoteFlags(fs *flag.FlagSet) *remoteFlags {
	return &remoteFlags{
		user: fs.String("user", "",
			"usuário HTTP Basic (a senha NUNCA é uma flag: use "+envPass+" ou --password-file)"),
		passwordFile: fs.String("password-file", "",
			"arquivo com a senha (quebra de linha final aparada); exclusivo com "+envPass),
		tlsCA: fs.String("tls-ca", "",
			"CA (PEM) para validar o certificado do servidor — liga TLS neste cliente"),
	}
}

// resolve valida a precedência das fontes de senha e lê a que foi escolhida.
//
// As quatro recusas são as do plano: --user sem nenhuma fonte, uma fonte sem
// --user (as duas leituras, env e arquivo, contam como "uma fonte"), e as
// duas fontes ao mesmo tempo. Nenhuma mensagem ecoa o VALOR de nada — só
// nomeia a flag/variável.
func (f *remoteFlags) resolve() (remoteCredential, error) {
	passwordFromEnv, hasEnv := os.LookupEnv(envPass)
	hasFile := *f.passwordFile != ""
	hasUser := *f.user != ""

	switch {
	case !hasUser && (hasEnv || hasFile):
		return remoteCredential{}, fmt.Errorf(
			"%s/--password-file exige --user (a senha sozinha não identifica ninguém)", envPass)
	case hasUser && hasEnv && hasFile:
		return remoteCredential{}, fmt.Errorf(
			"%s e --password-file são exclusivos: escolha UMA fonte de senha", envPass)
	case hasUser && !hasEnv && !hasFile:
		return remoteCredential{}, fmt.Errorf(
			"--user exige uma senha por %s ou --password-file", envPass)
	case hasUser && hasEnv && !hasFile && passwordFromEnv == "":
		// Mesma distinção do servidor (security.go:resolvePassword): "ausente"
		// e "definida e vazia" NÃO são a mesma coisa — o caso do container
		// cujo orquestrador injeta a chave sem valor, e sem esta recusa o
		// Basic sairia com senha vazia e o 401 do servidor não daria pista
		// nenhuma de qual das duas aconteceu.
		return remoteCredential{}, fmt.Errorf(
			"%s está definida e VAZIA — não é a mesma coisa que ausente; preencha a variável ou use "+
				"--password-file", envPass)
	case !hasUser:
		// Nem --user, nem fonte de senha: caminho anônimo, byte a byte igual
		// ao comportamento anterior a esta task. --tls-ca continua valendo —
		// TLS e autenticação são decisões independentes do servidor (§77).
		return remoteCredential{tlsCA: *f.tlsCA}, nil
	}

	password := passwordFromEnv
	if hasFile {
		b, err := os.ReadFile(*f.passwordFile)
		if err != nil {
			return remoteCredential{}, fmt.Errorf("--password-file: %w", err)
		}
		// Mesma aparagem do servidor (security.go:resolvePassword): todo
		// `echo senha > arquivo` termina em LF ou CRLF.
		password = strings.TrimRight(string(b), "\r\n")
		if password == "" {
			return remoteCredential{}, fmt.Errorf(
				"--password-file %s está vazio (ou só contém quebra de linha) após aparar o \\n/\\r\\n final",
				*f.passwordFile)
		}
	}
	return remoteCredential{user: *f.user, pass: password, tlsCA: *f.tlsCA}, nil
}

// remoteClient monta o *http.Client COMPARTILHADO pelos três pontos de
// costura do protocolo remoto (remoteQuery, searchDocument, postMutation)
// — um cliente novo por invocação do comando, nunca um estado global.
//
// Transport próprio com Proxy: nil — nunca http.DefaultClient, que honra
// http_proxy/https_proxy do ambiente. Com o bind exposto da Fase C, um
// `query --server 10.200.20.218` de shell interativo iria para o proxy
// corporativo, que responde 403 e faz o operador culpar o servidor errado
// (docs/piloto-events.md:142) — a mesma armadilha que o painel
// (nishidb-stats.py:39-47) já fechou para si. O CLI fala com um servidor que
// o operador NOMEOU na line de comando, nunca com a internet: não há
// cenário em que consultar o proxy é o certo.
//
// Clone() de http.DefaultTransport, não um &http.Transport{} vazio: preserva
// keep-alive, HTTP/2 e os outros defaults de desempenho do cliente padrão —
// só o Proxy muda. Um Transport zerado teria o MESMO efeito funcional aqui,
// mas mudaria comportamento que ninguém pediu para mudar.
//
// --tls-ca monta o CertPool no MOLDE do cliente Kafka
// (internal/kafka/config.go:109): um x509.CertPool com o PEM apontado,
// MinVersion TLS12. PROIBIDO InsecureSkipVerify — não há flag --insecure
// neste binário, por decisão do plano (viraria o modo permanente de operação
// em três semanas, doutrina citada no brief da task); quem precisa depurar
// um handshake usa openssl s_client.
func remoteClient(cred remoteCredential) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil

	if cred.tlsCA != "" {
		pemBytes, err := os.ReadFile(cred.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("--tls-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("--tls-ca %s não contém certificado PEM válido", cred.tlsCA)
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{Transport: transport, CheckRedirect: refuseRedirect}, nil
}

// refuseRedirect recusa QUALQUER redirect — 3xx same-host incluído — em vez
// de confiar na política default do stdlib (que remove o Authorization em
// redirect CROSS-host mas o PRESERVA em SAME-host, ver net/http.Client.Do).
// O CLI do NishiDB fala com UM servidor que o operador nomeou na line de
// comando; esse servidor não redireciona, e um 3xx só pode significar
// configuração errada no meio do caminho (proxy reverso, endereço trocado) —
// nunca algo a seguir silenciosamente com a credencial embarcada. Recusar a
// classe inteira elimina o risco de vazar Authorization por um redirect
// same-host, em vez de depender da distinção same-host/cross-host que o
// stdlib faz por outro motivo (compatibilidade com navegador, não segurança
// de credencial de API).
//
// req é a PRÓXIMA requisição que o stdlib montaria para seguir o redirect —
// req.Response é a resposta 3xx que a originou (ver net/http/client.go,
// send()), e é dali que o status code sai; req.URL já é o destino resolvido.
// Retornar erro aqui faz http.Client.Do devolver esse erro (embrulhado num
// *url.Error) SEM jamais montar ou enviar a requisição seguinte — nenhum
// header, credencial incluída, é reenviado a lugar nenhum.
func refuseRedirect(req *http.Request, _ []*http.Request) error {
	status := 0
	if req.Response != nil {
		status = req.Response.StatusCode
	}
	return fmt.Errorf(
		"o servidor respondeu com redirect (%d) para %s — o NishiDB não redireciona; verifique o endereço",
		status, req.URL)
}

// applyCredential injeta o HTTP Basic quando --user foi dado. É o ÚNICO
// lugar em que Authorization entra num *http.Request do protocolo remoto —
// o ponto de costura que a B5 já apontava em postMutation, agora estendido a
// remoteQuery e searchDocument.
func applyCredential(req *http.Request, cred remoteCredential) {
	if cred.user != "" {
		req.SetBasicAuth(cred.user, cred.pass)
	}
}
