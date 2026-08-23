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
// `compact`, `checkpoint`) chama registraFlagsRemotas no seu FlagSet e, logo
// após o Parse, flagsRemotas.resolve() — o ÚNICO lugar em que as fontes de
// senha são lidas e validadas. Dali em diante o comando carrega só um
// credencialRemota, que os TRÊS pontos de costura do protocolo remoto
// (consultaRemota em query_remota.go, buscaDocumento em operacao.go,
// postaMutacao em mutacao.go — a B5 já apontava o último deles) consomem via
// clienteRemoto (monta o *http.Client, com Proxy: nil e o CertPool de
// --tls-ca) e aplicaCredencial (o req.SetBasicAuth, quando --user foi dado).
//
// NUNCA existe uma flag de senha: apareceria em `ps aux` para qualquer
// usuário da máquina (§95). A senha entra SOMENTE por NISHIDB_PASSWORD (env,
// que não sobrevive a um `ps`) ou --password-file — o mesmo desenho do
// servidor (internal/server/seguranca.go:resolveSenha — trailing newline
// aparado), mas SEM a checagem de modo 0600 de lá: aquele arquivo é lido por
// um PROCESSO SERVIDOR que pode compartilhar host com outros usuários e
// datasets sensíveis; este é lido pelo comando do PRÓPRIO operador, na
// máquina dele — exigir chmod aqui teria custo (mais um jeito de o comando
// falhar) sem reduzir exposição real nenhuma, e por isso a doutrina diverge
// aqui, deliberadamente.

// envSenha é o ÚNICO nome de variável de ambiente aceito para a senha do
// cliente — diferente de server.authentication.users.passwordEnv (que
// NOMEIA uma variável arbitrária por usuário), aqui o cliente escolhe um
// nome fixo, o que permite documentar "NISHIDB_PASSWORD" sem indireção
// nenhuma.
const envSenha = "NISHIDB_PASSWORD"

// credencialRemota é o que os três pontos de costura recebem depois que
// flagsRemotas.resolve() validou as fontes. usuario/senha vazios (o caso de
// hoje, sem --user) significam "sem Authorization" — a paridade byte a byte
// com o comportamento anterior a esta task.
type credencialRemota struct {
	usuario string
	senha   string
	tlsCA   string
}

// flagsRemotas são as três flags que TODO comando remoto aceita.
// registraFlagsRemotas as acrescenta ao FlagSet do comando, ANTES do Parse —
// o mesmo padrão de --server em cada um deles.
type flagsRemotas struct {
	usuario      *string
	passwordFile *string
	tlsCA        *string
}

func registraFlagsRemotas(fs *flag.FlagSet) *flagsRemotas {
	return &flagsRemotas{
		usuario: fs.String("user", "",
			"usuário HTTP Basic (a senha NUNCA é uma flag: use "+envSenha+" ou --password-file)"),
		passwordFile: fs.String("password-file", "",
			"arquivo com a senha (quebra de linha final aparada); exclusivo com "+envSenha),
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
func (f *flagsRemotas) resolve() (credencialRemota, error) {
	senhaEnv, temEnv := os.LookupEnv(envSenha)
	temArquivo := *f.passwordFile != ""
	temUsuario := *f.usuario != ""

	switch {
	case !temUsuario && (temEnv || temArquivo):
		return credencialRemota{}, fmt.Errorf(
			"%s/--password-file exige --user (a senha sozinha não identifica ninguém)", envSenha)
	case temUsuario && temEnv && temArquivo:
		return credencialRemota{}, fmt.Errorf(
			"%s e --password-file são exclusivos: escolha UMA fonte de senha", envSenha)
	case temUsuario && !temEnv && !temArquivo:
		return credencialRemota{}, fmt.Errorf(
			"--user exige uma senha por %s ou --password-file", envSenha)
	case !temUsuario:
		// Nem --user, nem fonte de senha: caminho anônimo, byte a byte igual
		// ao comportamento anterior a esta task. --tls-ca continua valendo —
		// TLS e autenticação são decisões independentes do servidor (§77).
		return credencialRemota{tlsCA: *f.tlsCA}, nil
	}

	senha := senhaEnv
	if temArquivo {
		b, err := os.ReadFile(*f.passwordFile)
		if err != nil {
			return credencialRemota{}, fmt.Errorf("--password-file: %w", err)
		}
		// Mesma aparagem do servidor (seguranca.go:resolveSenha): todo
		// `echo senha > arquivo` termina em LF ou CRLF.
		senha = strings.TrimRight(string(b), "\r\n")
		if senha == "" {
			return credencialRemota{}, fmt.Errorf(
				"--password-file %s está vazio (ou só contém quebra de linha) após aparar o \\n/\\r\\n final",
				*f.passwordFile)
		}
	}
	return credencialRemota{usuario: *f.usuario, senha: senha, tlsCA: *f.tlsCA}, nil
}

// clienteRemoto monta o *http.Client COMPARTILHADO pelos três pontos de
// costura do protocolo remoto (consultaRemota, buscaDocumento, postaMutacao)
// — um cliente novo por invocação do comando, nunca um estado global.
//
// Transport próprio com Proxy: nil — nunca http.DefaultClient, que honra
// http_proxy/https_proxy do ambiente. Com o bind exposto da Fase C, um
// `query --server 10.200.20.218` de shell interativo iria para o proxy
// corporativo, que responde 403 e faz o operador culpar o servidor errado
// (docs/piloto-events.md:142) — a mesma armadilha que o painel
// (nishidb-stats.py:39-47) já fechou para si. O CLI fala com um servidor que
// o operador NOMEOU na linha de comando, nunca com a internet: não há
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
func clienteRemoto(cred credencialRemota) (*http.Client, error) {
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
	return &http.Client{Transport: transport, CheckRedirect: recusaRedirect}, nil
}

// recusaRedirect recusa QUALQUER redirect — 3xx same-host incluído — em vez
// de confiar na política default do stdlib (que remove o Authorization em
// redirect CROSS-host mas o PRESERVA em SAME-host, ver net/http.Client.Do).
// O CLI do NishiDB fala com UM servidor que o operador nomeou na linha de
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
func recusaRedirect(req *http.Request, _ []*http.Request) error {
	status := 0
	if req.Response != nil {
		status = req.Response.StatusCode
	}
	return fmt.Errorf(
		"o servidor respondeu com redirect (%d) para %s — o NishiDB não redireciona; verifique o endereço",
		status, req.URL)
}

// aplicaCredencial injeta o HTTP Basic quando --user foi dado. É o ÚNICO
// lugar em que Authorization entra num *http.Request do protocolo remoto —
// o ponto de costura que a B5 já apontava em postaMutacao, agora estendido a
// consultaRemota e buscaDocumento.
func aplicaCredencial(req *http.Request, cred credencialRemota) {
	if cred.usuario != "" {
		req.SetBasicAuth(cred.usuario, cred.senha)
	}
}
