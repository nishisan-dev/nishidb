package config

import (
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// TLSSpec é server.tls (§77): cifra o transporte do servidor HTTP.
//
// Enabled é bool simples, e não ponteiro: ausente e `false` significam a
// MESMA coisa aqui — TLS é recurso novo e opt-in, sem YAML herdado cujo
// comportamento dependa do oposto (ao contrário de Server.Enabled, onde
// ausente já vale "true" porque subir o comando já é o ato de ligar).
type TLSSpec struct {
	Enabled         bool   `yaml:"enabled"`
	CertificateFile string `yaml:"certificateFile"`
	PrivateKeyFile  string `yaml:"privateKeyFile"`
}

// UserSpec é um item de server.authentication.users (§75).
//
// Não há campo Password: a senha em si NUNCA atravessa o YAML — o §75 exige
// recusa explícita de um `password` literal, e não há onde decodificá-lo
// mesmo que viesse (validaUsuarios recusa a chave por não estar na
// whitelist). PasswordEnv/PasswordFile guardam ONDE a credencial mora — nome
// de variável de ambiente ou caminho de arquivo —, nunca o valor; resolver o
// valor é ativação (B2, em server.Novo), não parse.
type UserSpec struct {
	Username     string `yaml:"username"`
	PasswordEnv  string `yaml:"passwordEnv"`
	PasswordFile string `yaml:"passwordFile"`
	Role         string `yaml:"role"`
}

// AuthenticationSpec é server.authentication (§74-76). Enabled segue a
// mesma regra de TLSSpec.Enabled: bool simples, ausente vale FALSE.
type AuthenticationSpec struct {
	Enabled bool       `yaml:"enabled"`
	Users   []UserSpec `yaml:"users"`
}

// LoopbackSpec é server.loopback — um SEGUNDO listener, sem TLS/auth,
// inventado (a spec não o tem) para o plano duplo de observabilidade da
// Fase 11: quando o bind principal passa a exigir credencial, painel,
// script e cron que hoje falam com `127.0.0.1:9142` continuam funcionando
// sem senha por este listener. Enabled é bool simples, default FALSE:
// nenhum YAML herdado ganha um listener novo sem o operador pedir.
type LoopbackSpec struct {
	Enabled bool `yaml:"enabled"`
	// Port é ponteiro porque não tem default: ausente não é "porta zero", é
	// "sem porta declarada" — a exigência de declará-la quando o listener
	// está ligado é ativação (B2), não desta tarefa. O que ESTA tarefa
	// recusa é a porta declarada colidir com server.port (recusa 8).
	Port *int `yaml:"port"`
}

// Papéis do §74: enum fechado, e não string livre — um valor fora dele
// autorizaria um usuário sem que o middleware da B3 soubesse o que fazer com
// ele.
const (
	RoleRead  = "READ"
	RoleAdmin = "ADMIN"
)

// validaServerSeguranca valida FORMA nos blocos server.tls,
// server.authentication e server.loopback (§74-78): whitelist manual de
// chave dentro de cada bloco (o KnownFields GLOBAL continua desligado, ver
// comentário em CarregaConfig) e as recusas cruzadas que dependem só do
// arquivo. Roda depois do decode tipado (c já tem TLS/Authentication/
// Loopback preenchidos) e depois de c.serverPort resolvido, porque a recusa
// 8 compara loopback.port contra a porta PRINCIPAL JÁ COM O DEFAULT
// aplicado — colidir com 9142 implícito é colisão tanto quanto colidir com
// um `port:` explícito.
//
// A validação de FORMA roda sempre que o bloco está presente, independente
// de `enabled` — mesma doutrina de storage.compaction.minimumLiveRatio
// (débito 49): um YAML com `role` inválido não vira "certo" só porque
// authentication está desligada; quem ligasse `enabled: true` meses depois
// descobriria o erro só ali. As duas exceções são as próprias recusas 5 e 6,
// que SÃO sobre a implicação de `enabled: true`.
func validaServerSeguranca(c *Config, raiz *yaml.Node, path string) error {
	servidor := filho(raizDoDocumento(raiz), "server")

	if err := chavesConhecidas(filho(servidor, "tls"), "server.tls",
		"enabled", "certificateFile", "privateKeyFile"); err != nil {
		return fmt.Errorf("configuração %s: %w", path, err)
	}
	if c.Server.TLS.Enabled {
		var faltando []string
		if c.Server.TLS.CertificateFile == "" {
			faltando = append(faltando, "server.tls.certificateFile")
		}
		if c.Server.TLS.PrivateKeyFile == "" {
			faltando = append(faltando, "server.tls.privateKeyFile")
		}
		if len(faltando) > 0 {
			return fmt.Errorf("configuração %s: server.tls.enabled: true exige %s",
				path, strings.Join(faltando, " e "))
		}
	}

	authNode := filho(servidor, "authentication")
	if err := chavesConhecidas(authNode, "server.authentication", "enabled", "users"); err != nil {
		return fmt.Errorf("configuração %s: %w", path, err)
	}
	if err := validaUsuarios(c.Server.Authentication.Users, filho(authNode, "users"), path); err != nil {
		return err
	}
	if c.Server.Authentication.Enabled && len(c.Server.Authentication.Users) == 0 {
		return fmt.Errorf(
			"configuração %s: server.authentication.enabled: true exige ao menos um item em server.authentication.users",
			path)
	}

	if err := chavesConhecidas(filho(servidor, "loopback"), "server.loopback", "enabled", "port"); err != nil {
		return fmt.Errorf("configuração %s: %w", path, err)
	}
	if p := c.Server.Loopback.Port; p != nil && *p == c.serverPort {
		return fmt.Errorf(
			"configuração %s: server.loopback.port (%d) não pode ser igual a server.port (%d)",
			path, *p, c.serverPort)
	}

	return nil
}

// validaUsuarios recusa chave desconhecida em cada item de
// server.authentication.users e as quatro condições de FORMA do §75 que
// valem por usuário: username presente e único na lista, role no enum
// fechado, e exatamente uma fonte de senha entre passwordEnv/passwordFile.
//
// usersNode é o nó cru da lista (para a whitelist manual); users é a mesma
// lista já decodificada tipada (para as checagens semânticas). Os dois
// existem porque decodificação tipada e whitelist manual enxergam o
// documento de formas diferentes — ver comentário de chavesConhecidas.
func validaUsuarios(users []UserSpec, usersNode *yaml.Node, path string) error {
	if usersNode != nil {
		for i, item := range usersNode.Content {
			caminho := fmt.Sprintf("server.authentication.users[%d]", i)
			if err := chavesConhecidas(item, caminho,
				"username", "passwordEnv", "passwordFile", "role"); err != nil {
				return fmt.Errorf("configuração %s: %w", path, err)
			}
		}
	}

	vistos := make(map[string]bool, len(users))
	for i, u := range users {
		caminho := fmt.Sprintf("server.authentication.users[%d]", i)

		if u.Username == "" {
			return fmt.Errorf("configuração %s: %s.username é obrigatório", path, caminho)
		}
		if vistos[u.Username] {
			return fmt.Errorf("configuração %s: %s.username %q duplicado entre users", path, caminho, u.Username)
		}
		vistos[u.Username] = true

		switch {
		case u.PasswordEnv == "" && u.PasswordFile == "":
			return fmt.Errorf("configuração %s: %s exige exatamente um entre passwordEnv e passwordFile (nenhum declarado)",
				path, caminho)
		case u.PasswordEnv != "" && u.PasswordFile != "":
			return fmt.Errorf("configuração %s: %s exige exatamente um entre passwordEnv e passwordFile (os dois declarados)",
				path, caminho)
		}

		switch u.Role {
		case RoleRead, RoleAdmin:
		case "":
			return fmt.Errorf("configuração %s: %s.role é obrigatório (%s|%s)", path, caminho, RoleRead, RoleAdmin)
		default:
			return fmt.Errorf("configuração %s: %s.role %q inválido (%s|%s)", path, caminho, u.Role, RoleRead, RoleAdmin)
		}
	}
	return nil
}

// raizDoDocumento devolve o nó-mapa de topo do documento YAML, pulando o
// DocumentNode que o yaml.v3 sempre insere como envelope. Documento vazio
// (arquivo sem conteúdo) devolve nil, e filho(nil, ...) trata isso como
// bloco ausente — o mesmo comportamento que o Config zerado já tinha antes
// desta tarefa.
func raizDoDocumento(n *yaml.Node) *yaml.Node {
	if n != nil && n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return n.Content[0]
	}
	return n
}

// filho devolve o valor de `chave` dentro do nó de mapeamento pai, ou nil se
// a chave não existe ou pai não é mapa (bloco ausente) — não há sentinela
// melhor porque yaml.Node é struct e não interface.
func filho(pai *yaml.Node, chave string) *yaml.Node {
	if pai == nil {
		return nil
	}
	for pai.Kind == yaml.AliasNode {
		pai = pai.Alias
	}
	if pai.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(pai.Content); i += 2 {
		if pai.Content[i].Value == chave {
			return pai.Content[i+1]
		}
	}
	return nil
}

// chavesConhecidas recusa chave desconhecida ou duplicada dentro de um nó de
// mapeamento — WHITELIST MANUAL, no molde de bloco() em
// internal/schema/source.go (mesma técnica descrita em
// docs/operacao-kafka.md:115-119 para source/errors). A repetição é
// deliberada: o KnownFields GLOBAL continua desligado de propósito
// (comentário em CarregaConfig), porque o arquivo de um binário mais novo
// (M3) não pode ficar inválido para este. Os blocos de segurança da Fase 11
// são justamente os que não podem herdar essa tolerância — "enabld: true"
// (typo) engolido em silêncio é o typo mais caro do arquivo.
//
// n == nil (bloco ausente) não é erro: cada chamador decide separadamente se
// a ausência do bloco é aceitável. A iteração é na ordem do documento para
// que o primeiro erro seja sempre o mesmo — ordem de mapa faria a mensagem
// variar entre execuções.
func chavesConhecidas(n *yaml.Node, caminho string, permitidas ...string) error {
	if n == nil {
		return nil
	}
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("%s deve ser um mapa", caminho)
	}
	vistas := make(map[string]bool, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i].Value
		if !slices.Contains(permitidas, k) {
			return fmt.Errorf("%s.%s: chave desconhecida (conhecidas: %s)",
				caminho, k, strings.Join(permitidas, ", "))
		}
		if vistas[k] {
			return fmt.Errorf("%s.%s: chave duplicada", caminho, k)
		}
		vistas[k] = true
	}
	return nil
}
