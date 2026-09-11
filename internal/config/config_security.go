package config

import (
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/nishisan-dev/nishidb/internal/units"
	"gopkg.in/yaml.v3"
)

// TLSSpec é server.tls (§77): cifra o transporte do servidor HTTP.
//
// Enabled é bool simples, e não ponteiro: ausente e `false` significam a
// MESMA coisa aqui — TLS é recurso novo e opt-in, sem YAML herdado cujo
// comportamento dependa do oposto (ao contrário de Server.Enabled, onde
// ausente já vale "true" porque subir o comando já é o ato de ligar).
//
// Isto é ASSIMÉTRICO com AuthenticationSpec.Enabled (que é ponteiro) de
// propósito, e não inconsistência a "corrigir": nenhuma célula da matriz de
// recusa do §77 (B2) precisa distinguir "tls ausente" de "tls.enabled: false
// explícito" — só authentication precisa (ver comentário lá). LoopbackSpec
// segue a mesma regra de TLSSpec por não ter dependente na matriz.
type TLSSpec struct {
	Enabled         bool   `yaml:"enabled"`
	CertificateFile string `yaml:"certificateFile"`
	PrivateKeyFile  string `yaml:"privateKeyFile"`
}

// UserSpec é um item de server.authentication.users (§75).
//
// Não há campo Password: a senha em si NUNCA atravessa o YAML — o §75 exige
// recusa explícita de um `password` literal, e não há onde decodificá-lo
// mesmo que viesse (validateUsers recusa a chave por não estar na
// whitelist). PasswordEnv/PasswordFile guardam ONDE a credencial mora — nome
// de variável de ambiente ou caminho de arquivo —, nunca o valor; resolver o
// valor é ativação (B2, em server.New), não parse.
type UserSpec struct {
	Username     string `yaml:"username"`
	PasswordEnv  string `yaml:"passwordEnv"`
	PasswordFile string `yaml:"passwordFile"`
	Role         string `yaml:"role"`
}

// AuthenticationSpec é server.authentication (§74-76).
//
// Enabled é PONTEIRO — ao contrário de TLSSpec.Enabled e
// LoopbackSpec.Enabled, que são bool simples. A diferença é a matriz de
// recusa do §77 (B2, ruling do controlador): ela distingue "authentication
// AUSENTE" (bind exposto sem NENHUMA decisão sobre auth — recusa pedindo
// decisão explícita do operador) de "authentication.enabled: false
// EXPLÍCITO" (o operador decidiu, e a decisão vale — sobe com aviso). bool
// simples apagaria essa distinção (nil e false virariam a mesma coisa); TLS
// e loopback não têm célula de matriz que precise dela, então continuam
// bool. Não é a mesma regra de Server.Enabled: lá nil vale TRUE (rodar o
// comando já é o ato de ligar); aqui nil vale FALSE — ver
// AuthenticationEnabled.
//
// AuthenticationDeclared() responde "a seção apareceu no YAML?" com
// independência do valor de Enabled (inclusive nil); AuthenticationEnabled()
// responde "está LIGADA?", derivado do ponteiro.
type AuthenticationSpec struct {
	Enabled *bool      `yaml:"enabled"`
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
	// recusa é a porta declarada colidir de fato com server.port — mesma
	// porta E o bind principal alcançando 127.0.0.1 (recusa 8, refinada pela
	// revisão final: ver bindCollidesWithLoopback, acima). Um bind principal em
	// IP específico não-loopback pode repetir a porta do loopback sem
	// colisão nenhuma no kernel — é o desenho da Fase C.
	Port *int `yaml:"port"`
}

// bindCollidesWithLoopback diz se o bind do listener PRINCIPAL alcança o mesmo
// endereço que o listener loopback usa fixo (127.0.0.1, loopbackAddress em
// internal/server/security.go) — a única condição em que os dois listeners
// na MESMA porta colidiriam de fato no kernel.
//
// Dois casos colidem: o bind principal É um endereço loopback (mesma
// doutrina de BindExposed — IsLoopback cobre 127.0.0.0/8 inteiro, não só
// 127.0.0.1 literal — e "localhost" antes do ParseIP, pela mesma razão que
// BindExposed o trata à parte), ou é o unspecified (0.0.0.0/::), que escuta
// em TODAS as interfaces locais, 127.0.0.1 inclusive. Um IP ESPECÍFICO e
// NÃO-loopback (o caso da Fase C: expor 10.200.20.218:9142 com o plano
// loopback também em :9142) não colide — para o kernel são dois sockets em
// endereços diferentes, e os dois listeners coexistem sem disputar porta
// nenhuma.
//
// Nome que não resolve para IP (hostname arbitrário) é tratado como
// podendo colidir — mesma doutrina de BindExposed: presumir inocência de um
// endereço que não se sabe qual é seria errar para o lado errado, e aqui o
// lado errado é deixar dois listeners tentarem bindar a mesma porta na
// mesma interface sem avisar antes.
func bindCollidesWithLoopback(bind string) bool {
	if bind == "localhost" {
		return true
	}
	ip := net.ParseIP(bind)
	if ip == nil {
		return true
	}
	return ip.IsLoopback() || ip.IsUnspecified()
}

// Papéis do §74: enum fechado, e não string livre — um valor fora dele
// autorizaria um usuário sem que o middleware da B3 soubesse o que fazer com
// ele.
const (
	RoleRead  = "READ"
	RoleAdmin = "ADMIN"
)

// validateServerSecurity valida FORMA nos blocos server.tls,
// server.authentication e server.loopback (§74-78): whitelist manual de
// chave dentro de cada bloco (o KnownFields GLOBAL continua desligado, ver
// comentário em Load) e as recusas cruzadas que dependem só do
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
func validateServerSecurity(c *Config, root *yaml.Node, path string) error {
	serverNode := child(documentRoot(root), "server")

	if err := knownKeys(child(serverNode, "tls"), "server.tls",
		"enabled", "certificateFile", "privateKeyFile"); err != nil {
		return fmt.Errorf("configuração %s: %w", path, err)
	}
	if c.Server.TLS.Enabled {
		var missing []string
		if c.Server.TLS.CertificateFile == "" {
			missing = append(missing, "server.tls.certificateFile")
		}
		if c.Server.TLS.PrivateKeyFile == "" {
			missing = append(missing, "server.tls.privateKeyFile")
		}
		if len(missing) > 0 {
			return fmt.Errorf("configuração %s: server.tls.enabled: true exige %s",
				path, strings.Join(missing, " e "))
		}
	}

	authNode := child(serverNode, "authentication")
	// A seção "apareceu no YAML" é fato do DOCUMENTO, não do valor decodificado
	// de enabled: authNode != nil já responde isso mesmo quando `enabled` vem
	// omitido dentro do bloco (Enabled fica nil do mesmo jeito que ficaria com
	// o bloco inteiro ausente). É o que AuthenticationDeclared expõe para a
	// matriz do §77.
	c.authenticationDeclared = authNode != nil
	if err := knownKeys(authNode, "server.authentication", "enabled", "users"); err != nil {
		return fmt.Errorf("configuração %s: %w", path, err)
	}
	if err := validateUsers(c.Server.Authentication.Users, child(authNode, "users"), path); err != nil {
		return err
	}
	if c.AuthenticationEnabled() && len(c.Server.Authentication.Users) == 0 {
		return fmt.Errorf(
			"configuração %s: server.authentication.enabled: true exige ao menos um item em server.authentication.users",
			path)
	}

	if err := knownKeys(child(serverNode, "loopback"), "server.loopback", "enabled", "port"); err != nil {
		return fmt.Errorf("configuração %s: %w", path, err)
	}
	// Mesma faixa de server.port (Minor 7 da revisão final): sem isto,
	// `port: 0` passa direto pela recusa 8 (0 nunca é igual a c.serverPort) e
	// só aparece na subida como net.Listen("127.0.0.1:0") — uma porta
	// EFÊMERA escolhida pelo kernel, em silêncio, no lugar do endereço
	// determinístico que server.loopback.port promete declarar.
	if _, err := units.PositiveInt(c.Server.Loopback.Port, 0); err != nil {
		return fmt.Errorf("configuração %s: server.loopback.port: %w", path, err)
	}
	if p := c.Server.Loopback.Port; p != nil && *p > 65535 {
		return fmt.Errorf("configuração %s: server.loopback.port %d fora de 1..65535", path, *p)
	}
	if p := c.Server.Loopback.Port; p != nil && *p == c.serverPort && bindCollidesWithLoopback(c.Server.Bind) {
		return fmt.Errorf(
			"configuração %s: server.loopback.port (%d) colide com server.port (%d): o bind principal (%s) "+
				"alcança 127.0.0.1, o endereço fixo do listener loopback, e os dois bindariam a mesma porta na "+
				"MESMA interface. Um server.bind em IP específico e NÃO-loopback pode reusar a porta do "+
				"loopback sem colisão nenhuma",
			path, *p, c.serverPort, c.Server.Bind)
	}

	return nil
}

// validateUsers recusa chave desconhecida em cada item de
// server.authentication.users e as quatro condições de FORMA do §75 que
// valem por usuário: username presente e único na lista, role no enum
// fechado, e exatamente uma fonte de senha entre passwordEnv/passwordFile.
//
// usersNode é o nó cru da lista (para a whitelist manual); users é a mesma
// lista já decodificada tipada (para as checagens semânticas). Os dois
// existem porque decodificação tipada e whitelist manual enxergam o
// documento de formas diferentes — ver comentário de knownKeys.
func validateUsers(users []UserSpec, usersNode *yaml.Node, path string) error {
	if usersNode != nil {
		for i, item := range usersNode.Content {
			keyPath := fmt.Sprintf("server.authentication.users[%d]", i)
			if err := knownKeys(item, keyPath,
				"username", "passwordEnv", "passwordFile", "role"); err != nil {
				return fmt.Errorf("configuração %s: %w", path, err)
			}
		}
	}

	seen := make(map[string]bool, len(users))
	for i, u := range users {
		keyPath := fmt.Sprintf("server.authentication.users[%d]", i)

		if u.Username == "" {
			return fmt.Errorf("configuração %s: %s.username é obrigatório", path, keyPath)
		}
		if seen[u.Username] {
			return fmt.Errorf("configuração %s: %s.username %q duplicado entre users", path, keyPath, u.Username)
		}
		seen[u.Username] = true

		switch {
		case u.PasswordEnv == "" && u.PasswordFile == "":
			return fmt.Errorf("configuração %s: %s exige exatamente um entre passwordEnv e passwordFile (nenhum declarado)",
				path, keyPath)
		case u.PasswordEnv != "" && u.PasswordFile != "":
			return fmt.Errorf("configuração %s: %s exige exatamente um entre passwordEnv e passwordFile (os dois declarados)",
				path, keyPath)
		}

		// Comparação de string exata, de propósito: o enum é fechado e
		// CASE-SENSITIVE — "read"/"Read" não casam com RoleRead ("READ"). Uma
		// normalização de case pareceria conveniência, mas o middleware da B3
		// compara o papel resolvido contra RoleRead/RoleAdmin com == simples;
		// aceitar variação aqui só empurraria a mesma pergunta para lá, sem
		// necessidade — melhor recusar cedo, no parse, nomeando o valor
		// exato que o operador escreveu.
		switch u.Role {
		case RoleRead, RoleAdmin:
		case "":
			return fmt.Errorf("configuração %s: %s.role é obrigatório (%s|%s)", path, keyPath, RoleRead, RoleAdmin)
		default:
			return fmt.Errorf("configuração %s: %s.role %q inválido (%s|%s)", path, keyPath, u.Role, RoleRead, RoleAdmin)
		}
	}
	return nil
}

// documentRoot devolve o nó-mapa de topo do documento YAML, pulando o
// DocumentNode que o yaml.v3 sempre insere como envelope. Documento vazio
// (arquivo sem conteúdo) devolve nil, e child(nil, ...) trata isso como
// bloco ausente — o mesmo comportamento que o Config zerado já tinha antes
// desta tarefa.
func documentRoot(n *yaml.Node) *yaml.Node {
	if n != nil && n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return n.Content[0]
	}
	return n
}

// child devolve o valor de `key` dentro do nó de mapeamento parent, ou nil se
// a chave não existe ou parent não é mapa (bloco ausente) — não há sentinela
// melhor porque yaml.Node é struct e não interface.
func child(parent *yaml.Node, key string) *yaml.Node {
	if parent == nil {
		return nil
	}
	for parent.Kind == yaml.AliasNode {
		parent = parent.Alias
	}
	if parent.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(parent.Content); i += 2 {
		if parent.Content[i].Value == key {
			return parent.Content[i+1]
		}
	}
	return nil
}

// knownKeys recusa chave desconhecida ou duplicada dentro de um nó de
// mapeamento — WHITELIST MANUAL, no molde de bloco() em
// internal/schema/source.go (mesma técnica descrita em
// docs/operacao-kafka.md:115-119 para source/errors). A repetição é
// deliberada: o KnownFields GLOBAL continua desligado de propósito
// (comentário em Load), porque o arquivo de um binário mais novo
// (M3) não pode ficar inválido para este. Os blocos de segurança da Fase 11
// são justamente os que não podem herdar essa tolerância — "enabld: true"
// (typo) engolido em silêncio é o typo mais caro do arquivo.
//
// n == nil (bloco ausente) não é erro: cada chamador decide separadamente se
// a ausência do bloco é aceitável. A iteração é na ordem do documento para
// que o primeiro erro seja sempre o mesmo — ordem de mapa faria a mensagem
// variar entre execuções.
func knownKeys(n *yaml.Node, keyPath string, known ...string) error {
	if n == nil {
		return nil
	}
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("%s deve ser um mapa", keyPath)
	}
	seen := make(map[string]bool, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i].Value
		if !slices.Contains(known, k) {
			return fmt.Errorf("%s.%s: chave desconhecida (conhecidas: %s)",
				keyPath, k, strings.Join(known, ", "))
		}
		if seen[k] {
			return fmt.Errorf("%s.%s: chave duplicada", keyPath, k)
		}
		seen[k] = true
	}
	return nil
}
