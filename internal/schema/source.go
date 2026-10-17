package schema

import (
	"net"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
	"time"
)

// SourceSpec é a fonte de eventos do dataset (§12).
//
// Até o M1 isto era `{Type string; Kafka map[string]any}` com validação
// nenhuma: `type` aceitava qualquer string e o mapa Kafka era um saco opaco
// que ninguém lia. O efeito prático era o pior tipo de defeito — o YAML do
// projeto (testdata/radius.materialization.yaml, cópia do §12) declarava
// bootstrapServers, groupId e SASL_SCRAM-SHA-512, e nenhum comando reclamava.
// Configuração aceita e não lida é configuração que mente.
type SourceSpec struct {
	// Type é "" quando não há bloco `source`, ou "kafka".
	Type string
	// Kafka é nil quando Type != "kafka".
	Kafka *KafkaSpec
}

// KafkaSpec é o bloco `source.kafka` (§12).
type KafkaSpec struct {
	// Enabled liga o consumer. É INVENTADO (a spec não o tem) e o default é
	// FALSE por uma razão concreta: o exemplo do §12, que é o fixture deste
	// repositório e o arquivo que qualquer operador copia, aponta para
	// kafka01:9092. Se a presença do bloco ligasse o consumer, uma troca de
	// binário subiria um consumer real contra um cluster que não existe.
	//
	// Serve também de botão de pausa declarativo: parar a ingestão de UM
	// dataset numa instância que serve vários não deveria exigir matar o
	// processo.
	Enabled bool

	BootstrapServers []string
	Topic            string
	Consumer         ConsumerSpec
	Security         SecuritySpec
}

// ConsumerSpec é `source.kafka.consumer` (§12, §29).
type ConsumerSpec struct {
	GroupID string
	// AutoOffsetReset vale APENAS quando não há offset local e o dataset está
	// vazio. Depois disso o checkpoint local é autoritativo (§49) e o
	// consumer sempre faz seek explícito, mesmo que o group tenha offset mais
	// avançado no broker.
	AutoOffsetReset string
	// MaxPollRecords é o tamanho do lote entregue ao writer. NÃO é teto de
	// memória — ver IngestionQueueBytes.
	MaxPollRecords int
	// IngestionQueueSize é a capacidade, em REGISTROS, do channel limitado
	// poller→writer (§29; fila ilimitada é proibida pelo §128).
	IngestionQueueSize int
	// IngestionQueueBytes é o segundo gate da mesma fila, em BYTES. É
	// INVENTADO e existe porque contar registros não é unidade de memória:
	// 5000 slots com payload grande estouram o orçamento de 1 GiB do §109 sem
	// que nenhuma regra de contagem reclame. O §79 exige expor "Kafka
	// buffers" e o §108 exige a rodada de 8M com ingestão Kafka real.
	IngestionQueueBytes int64
	// WatchdogTimeout é por quanto tempo a ingestão pode ficar SEM PROGRESSO
	// com o broker declarando mensagens antes de o consumidor ser recriado.
	// Zero desliga o watchdog.
	//
	// "Sem progresso" é uma conjunção de três fatos, e são três porque dois
	// não bastam — ver o godoc do watchdog no pacote kafka. O que importa
	// aqui: um tópico legitimamente parado NÃO dispara, porque o lag é zero.
	WatchdogTimeout time.Duration
	// WatchdogAttempts é quantas vezes o consumidor é recriado antes de
	// desistir. Desistir devolve o dataset ao estado que a ingestão falha já
	// tem hoje: consultas de pé, health reportando, e nada em silêncio.
	WatchdogAttempts int
	// DecodeWorkers é quantas goroutinas decodificam eventos em paralelo antes
	// da esteira que os reordena para o writer único.
	//
	// Nasce em 1, e o default não é timidez: ligar paralelismo por default
	// mudaria o perfil de memória de todo dataset existente — cada slot da
	// esteira retém buffers até a marca d'água — sem medição que o justifique
	// no dataset de quem atualiza. É a mesma convenção que o débito 95 fixou
	// para `query.scanWorkers`.
	//
	// Aplicar não é aumentar sem pensar: o decode é ~33% do CPU por evento, e
	// pela lei de Amdahl o ganho satura perto de 1,5× por mais que se
	// acrescente. O benchmark de 1/2/4/8 existe para que a escolha saia de
	// medida, não de intuição.
	DecodeWorkers int
}

// SecuritySpec é `source.kafka.security` (§12).
//
// As credenciais são NOMES de variável de ambiente, nunca valores: o YAML é
// versionado e o §95 proíbe registrar senha. A resolução acontece na subida do
// consumer, não no parse — é isso que mantém o fixture do §12 válido numa
// suíte que roda sem KAFKA_USERNAME/KAFKA_PASSWORD no ambiente.
type SecuritySpec struct {
	Protocol      string `yaml:"protocol,omitempty"`
	SASLMechanism string `yaml:"saslMechanism,omitempty"`
	UsernameEnv   string `yaml:"usernameEnv,omitempty"`
	PasswordEnv   string `yaml:"passwordEnv,omitempty"`
	CAFile        string `yaml:"caFile,omitempty"`
}

// ErrorsSpec é o bloco `errors` da raiz do dataset (§101, §102).
//
// A spec mostra o bloco sem indicar o pai; ele mora na raiz porque a política
// de evento inválido vale para QUALQUER ingestão do dataset, e não só para a
// vinda do Kafka — o `nishidb ingest` também a lê.
type ErrorsSpec struct {
	// InvalidEvent é `log-and-skip` (default) ou `stop`.
	//
	// `stop` NÃO encerra o processo. Ele para a ingestão do dataset e mantém
	// as consultas de pé, que é a ordem que o §51 já estabelece (queries
	// sobem antes de a ingestão retomar). Encerrar teria dois defeitos: o
	// §82 permite vários datasets por instância, então derrubaria consultas
	// alheias; e como o §49 obriga seek no offset do checkpoint e o §48 só
	// avança o offset após checkpoint durável, o evento que causou o stop é
	// exatamente o primeiro após o offset persistido — reiniciar reproduz o
	// mesmo evento e para de novo, o que sob supervisor é crash loop.
	InvalidEvent string
	// DeadLetterTopic é ACEITO e validado na forma, e RECUSADO na subida do
	// consumer com "não implementado nesta versão" (§102 não o torna
	// bloqueante). Recusá-lo no parse tornaria inválido o snippet literal da
	// spec; aceitá-lo em silêncio repetiria o defeito que esta mudança
	// conserta.
	DeadLetterTopic string
}

// Valores válidos e defaults. Ficam exportados porque o consumer, o CLI e a
// documentação precisam citar os mesmos, e um default duplicado é um default
// que diverge.
const (
	SourceKafka = "kafka"

	AutoOffsetResetEarliest = "earliest"
	AutoOffsetResetLatest   = "latest"

	ProtocolPlaintext     = "PLAINTEXT"
	ProtocolSSL           = "SSL"
	ProtocolSASLPlaintext = "SASL_PLAINTEXT"
	ProtocolSASLSSL       = "SASL_SSL"

	SASLPlain       = "PLAIN"
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"

	InvalidEventLogAndSkip = "log-and-skip"
	InvalidEventStop       = "stop"
)

// Defaults dos campos que a spec só mostra em bloco de "Exemplo": herdar o
// valor de um exemplo é invenção, e está declarada como tal.
const (
	DefaultMaxPollRecords      = 1000
	DefaultIngestionQueueSize  = 5000
	DefaultIngestionQueueBytes = 64 << 20

	// DefaultWatchdogTimeout é a janela sem progresso que recria o consumidor.
	//
	// Um minuto sai da assimetria entre os dois erros. Disparar cedo demais
	// recria um consumidor que estava só esperando o writer terminar um lote
	// grande; disparar tarde demais deixa a ingestão morta pelo tempo que a
	// janela custar — e o incidente de 27/08/2026 no piloto durou uma hora
	// porque NINGUÉM estava olhando. Sessenta segundos é curto o bastante
	// para não perder o turno e longo o bastante para não brigar com um
	// checkpoint, que no piloto para o writer por 774 ms.
	DefaultWatchdogTimeout = time.Minute
	// MinWatchdogTimeout é o piso declarável.
	MinWatchdogTimeout = 10 * time.Second
	// DefaultWatchdogAttempts é quantas recriações antes de desistir.
	DefaultWatchdogAttempts = 3

	// DefaultDecodeWorkers é um: a ingestão nasce serial, como sempre foi.
	DefaultDecodeWorkers = 1
	// MaxDecodeWorkers é o teto declarável.
	//
	// Existe porque a chave dimensiona goroutinas E memória: cada
	// decodificador a mais é um slot a mais em voo, cada slot retém buffers
	// até a marca d'água, e o orçamento do §109 é de 1 GiB para o processo
	// inteiro. Um 10000 digitado por engano não deve ser aceito e depois
	// explicado pelo OOM killer. O valor é folgado — o ganho satura em poucos
	// workers, pela fração serial do pipeline —, então recusar acima dele não
	// tira nada de ninguém.
	MaxDecodeWorkers = 64
)

// WatchdogDisabled é o literal que desliga o watchdog. `0` seria recusado
// como qualquer outra duração não positiva: zero não é "sem watchdog", é uma
// janela que nenhum relógio poderia honrar.
const WatchdogDisabled = "never"

// SASL diz se o protocolo exige credencial.
func (s SecuritySpec) SASL() bool {
	return s.Protocol == ProtocolSASLPlaintext || s.Protocol == ProtocolSASLSSL
}

// TLS diz se o transporte é cifrado.
func (s SecuritySpec) TLS() bool {
	return s.Protocol == ProtocolSSL || s.Protocol == ProtocolSASLSSL
}

// block lê um nó de mapeamento recusando chave desconhecida ou duplicada.
//
// A recusa é por WHITELIST MANUAL, e não por yaml.KnownFields, por duas
// razões. A primeira é técnica: no yaml.v3 o KnownFields só existe em
// *Decoder, vale para o documento INTEIRO, e Node.Decode constrói um decoder
// novo que não herda a estritude — enquanto o Parse usa yaml.Unmarshal puro.
// A segunda é de doutrina: ligar a estritude no documento contrariaria a
// decisão registrada em internal/cli/config.go, de que o arquivo de um
// milestone futuro não pode ser inválido para o binário de hoje.
//
// Aqui vale o contrário, e de propósito: `source.kafka` e `errors` são blocos
// NOVOS, sem legado a preservar. Sem esta checagem, `errors: invalidEvant:
// stop` (typo) parseia limpo, cai no default, e a política que o operador
// escreveu simplesmente não existe.
//
// A iteração é na ordem do documento para que o primeiro erro seja sempre o
// mesmo — ordem de mapa faria a mensagem variar entre execuções.

// absent diz se o bloco não veio no documento. Kind zero é o nó que o
// yaml.v3 deixa quando a chave não existe — não há sentinela melhor, porque o
// campo é valor e não ponteiro.
func absent(n *yaml.Node) bool { return n == nil || n.Kind == 0 }

func block(n *yaml.Node, keyPath string, allowed ...string) (map[string]*yaml.Node, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.MappingNode {
		return nil, specErrf("%s deve ser um mapa", keyPath)
	}
	out := make(map[string]*yaml.Node, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i].Value
		if !slices.Contains(allowed, k) {
			return nil, specErrf("%s.%s: chave desconhecida (conhecidas: %s)",
				keyPath, k, strings.Join(allowed, ", "))
		}
		if _, dup := out[k]; dup {
			return nil, specErrf("%s.%s: chave duplicada", keyPath, k)
		}
		out[k] = n.Content[i+1]
	}
	return out, nil
}

// decodeInto decodifica um nó no alvo, traduzindo o erro do yaml para a
// gramática de erro do pacote (caminho completo da chave + valor recebido).
func decodeInto(n *yaml.Node, keyPath string, target any) error {
	if err := n.Decode(target); err != nil {
		return specErrf("%s: valor inválido (%v)", keyPath, err)
	}
	return nil
}

// text lê uma chave string opcional.
func text(m map[string]*yaml.Node, keyPath, key string) (string, error) {
	n, ok := m[key]
	if !ok {
		return "", nil
	}
	var v string
	if err := decodeInto(n, keyPath+"."+key, &v); err != nil {
		return "", err
	}
	return strings.TrimSpace(v), nil
}

// validateSource valida o bloco `source` (§12). Bloco ausente é válido: o
// dataset alimentado por `nishidb ingest` não tem fonte declarada, e
// testdata/todos_os_tipos.yaml precisa continuar válido.
func validateSource(n *yaml.Node) (SourceSpec, error) {
	var s SourceSpec
	if absent(n) {
		return s, nil
	}
	m, err := block(n, "source", "type", "kafka")
	if err != nil {
		return s, err
	}
	typ, err := text(m, "source", "type")
	if err != nil {
		return s, err
	}
	nk, hasKafka := m["kafka"]

	// O enum fechado é a diferença entre descobrir o erro de digitação no
	// parse e descobri-lo na primeira tentativa de conexão, em produção.
	switch {
	case typ == "":
		return s, specErrf("source.type obrigatório quando o bloco source existe (v1: %s)", SourceKafka)
	case typ != SourceKafka:
		return s, specErrf("source.type %q não suportado (v1: %s)", typ, SourceKafka)
	case !hasKafka:
		return s, specErrf("source.type: %s exige o bloco source.kafka", SourceKafka)
	}
	s.Type = SourceKafka

	k, err := validateKafka(nk)
	if err != nil {
		return s, err
	}
	s.Kafka = k
	return s, nil
}

func validateKafka(n *yaml.Node) (*KafkaSpec, error) {
	m, err := block(n, "source.kafka",
		"enabled", "bootstrapServers", "topic", "consumer", "security")
	if err != nil {
		return nil, err
	}
	k := &KafkaSpec{}

	if en, ok := m["enabled"]; ok {
		if err := decodeInto(en, "source.kafka.enabled", &k.Enabled); err != nil {
			return nil, err
		}
	}

	bn, ok := m["bootstrapServers"]
	if !ok {
		return nil, specErrf("source.kafka.bootstrapServers obrigatório")
	}
	if err := decodeInto(bn, "source.kafka.bootstrapServers", &k.BootstrapServers); err != nil {
		return nil, err
	}
	if err := validateBootstrap(k.BootstrapServers); err != nil {
		return nil, err
	}

	if k.Topic, err = text(m, "source.kafka", "topic"); err != nil {
		return nil, err
	}
	if k.Topic == "" {
		return nil, specErrf("source.kafka.topic obrigatório")
	}

	if k.Consumer, err = validateConsumer(m["consumer"]); err != nil {
		return nil, err
	}
	if k.Security, err = validateSecurity(m["security"]); err != nil {
		return nil, err
	}
	return k, nil
}

// validateBootstrap exige host:porta em cada item.
//
// Usa net.SplitHostPort, e não um split ingênuo por ':', porque a spec não
// define a gramática do item e um split por ':' tornaria um cluster IPv6-only
// ([2001:db8::1]:9092) inconfigurável — e não com um erro de conexão, mas com
// um erro de PARSE, que impediria até `nishidb query` de abrir o dataset.
func validateBootstrap(items []string) error {
	return ValidateKafkaBootstrap(items, "source.kafka.bootstrapServers")
}

// ValidateKafkaBootstrap checks connection addresses for consumers and producers.
//
// Recusa a vírgula dentro de um item: "a,b:9092" passa no net.SplitHostPort
// (o host seria "a,b"), mas é a lista colada num item só por engano no YAML.
// Além de nunca conectar, ele quebraria o checkpoint do publicador Kafka, que
// guarda os brokers juntados por vírgula e recusaria gravar esse item para
// sempre (errBootstrapComma em internal/replicationkafka).
func ValidateKafkaBootstrap(items []string, keyPath string) error {
	if len(items) == 0 {
		return specErrf("%s não pode ser lista vazia", keyPath)
	}
	for i, item := range items {
		item = strings.TrimSpace(item)
		if strings.Contains(item, ",") {
			return specErrf("%s[%d] %q contém vírgula: liste um broker por item", keyPath, i, item)
		}
		host, port, err := net.SplitHostPort(item)
		if err != nil {
			return specErrf("%s[%d] %q inválido (esperado host:porta)", keyPath, i, item)
		}
		if host == "" {
			return specErrf("%s[%d] %q sem host", keyPath, i, item)
		}
		p, err := strconv.Atoi(port)
		if err != nil || p < 1 || p > 65535 {
			return specErrf("%s[%d] %q: porta inválida (1..65535)", keyPath, i, item)
		}
	}
	return nil
}

func validateConsumer(n *yaml.Node) (ConsumerSpec, error) {
	c := ConsumerSpec{
		AutoOffsetReset:     AutoOffsetResetEarliest,
		MaxPollRecords:      DefaultMaxPollRecords,
		IngestionQueueSize:  DefaultIngestionQueueSize,
		IngestionQueueBytes: DefaultIngestionQueueBytes,
		WatchdogTimeout:     DefaultWatchdogTimeout,
		WatchdogAttempts:    DefaultWatchdogAttempts,
		DecodeWorkers:       DefaultDecodeWorkers,
	}
	if n == nil {
		return c, specErrf("source.kafka.consumer.groupId obrigatório")
	}
	m, err := block(n, "source.kafka.consumer",
		"groupId", "autoOffsetReset", "maxPollRecords", "ingestionQueueSize", "ingestionQueueBytes",
		"watchdogTimeout", "watchdogTentativas", "decodeWorkers")
	if err != nil {
		return c, err
	}

	if c.GroupID, err = text(m, "source.kafka.consumer", "groupId"); err != nil {
		return c, err
	}
	if c.GroupID == "" {
		return c, specErrf("source.kafka.consumer.groupId obrigatório")
	}
	if strings.ContainsAny(c.GroupID, " \t\n\r") {
		return c, specErrf("source.kafka.consumer.groupId %q não pode conter espaço", c.GroupID)
	}

	if v, err := text(m, "source.kafka.consumer", "autoOffsetReset"); err != nil {
		return c, err
	} else if v != "" {
		if v != AutoOffsetResetEarliest && v != AutoOffsetResetLatest {
			return c, specErrf("source.kafka.consumer.autoOffsetReset %q inválido (%s|%s)",
				v, AutoOffsetResetEarliest, AutoOffsetResetLatest)
		}
		c.AutoOffsetReset = v
	}

	if c.MaxPollRecords, err = integer(m, "source.kafka.consumer", "maxPollRecords", DefaultMaxPollRecords); err != nil {
		return c, err
	}
	if c.IngestionQueueSize, err = integer(m, "source.kafka.consumer", "ingestionQueueSize", DefaultIngestionQueueSize); err != nil {
		return c, err
	}
	if bytesTxt, err := text(m, "source.kafka.consumer", "ingestionQueueBytes"); err != nil {
		return c, err
	} else if c.IngestionQueueBytes, err = units.Size(bytesTxt, DefaultIngestionQueueBytes); err != nil {
		return c, specErrf("source.kafka.consumer.ingestionQueueBytes: %v", err)
	}

	if v, err := text(m, "source.kafka.consumer", "watchdogTimeout"); err != nil {
		return c, err
	} else if v == WatchdogDisabled {
		c.WatchdogTimeout = 0
	} else if v != "" {
		if c.WatchdogTimeout, err = units.Duration(v, 0); err != nil {
			return c, specErrf("source.kafka.consumer.watchdogTimeout: %v", err)
		}
		if c.WatchdogTimeout < MinWatchdogTimeout {
			// Abaixo disso o watchdog compete com o próprio produto: um
			// checkpoint do piloto para o writer por centenas de
			// milissegundos, e uma janela curta transformaria essa pausa
			// normal em recriação de consumidor.
			return c, specErrf(
				"source.kafka.consumer.watchdogTimeout %s é menor que o mínimo de %s — "+
					"abaixo disso uma pausa NORMAL de checkpoint viraria recriação de consumidor; "+
					"use %q para desligar",
				v, MinWatchdogTimeout, WatchdogDisabled)
		}
	}
	if c.WatchdogAttempts, err = integer(m, "source.kafka.consumer", "watchdogTentativas", DefaultWatchdogAttempts); err != nil {
		return c, err
	}
	if c.DecodeWorkers, err = integer(m, "source.kafka.consumer", "decodeWorkers", DefaultDecodeWorkers); err != nil {
		return c, err
	}
	if c.DecodeWorkers > MaxDecodeWorkers {
		// RECUSA, e não arredondamento para o teto: o operador que escreveu
		// 10000 tem uma expectativa errada sobre o que a chave faz, e um
		// processo que aceitasse em silêncio o deixaria com essa expectativa.
		return c, specErrf(
			"source.kafka.consumer.decodeWorkers %d passa do máximo de %d — "+
				"cada decodificador é um slot em voo com buffers retidos, e o orçamento "+
				"de memória do processo é de 1 GiB (§109)",
			c.DecodeWorkers, MaxDecodeWorkers)
	}
	return c, nil
}

// integer aplica a mesma regra de units.PositiveInt: ausente herda o
// padrão, zero e negativo são recusados. Uma fila de capacidade 0 não é "sem
// fila", é uma fila que nada atravessa.
func integer(m map[string]*yaml.Node, keyPath, key string, defaultValue int) (int, error) {
	n, ok := m[key]
	if !ok {
		return defaultValue, nil
	}
	var v int
	if err := decodeInto(n, keyPath+"."+key, &v); err != nil {
		return 0, err
	}
	out, err := units.PositiveInt(&v, defaultValue)
	if err != nil {
		return 0, specErrf("%s.%s: %v", keyPath, key, err)
	}
	return out, nil
}

func validateSecurity(n *yaml.Node) (SecuritySpec, error) {
	return ParseKafkaSecurity(n, "source.kafka.security")
}

// ParseKafkaSecurity validates the shared producer/consumer security contract.
func ParseKafkaSecurity(n *yaml.Node, keyPath string) (SecuritySpec, error) {
	s := SecuritySpec{Protocol: ProtocolPlaintext}
	if n == nil {
		return s, nil
	}
	m, err := block(n, keyPath,
		"protocol", "saslMechanism", "usernameEnv", "passwordEnv", "caFile")
	if err != nil {
		return s, err
	}

	if v, err := text(m, keyPath, "protocol"); err != nil {
		return s, err
	} else if v != "" {
		switch v {
		case ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL:
			s.Protocol = v
		default:
			return s, specErrf("%s.protocol %q inválido (%s|%s|%s|%s)",
				keyPath, v, ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL)
		}
	}

	if s.SASLMechanism, err = text(m, keyPath, "saslMechanism"); err != nil {
		return s, err
	}
	if s.UsernameEnv, err = text(m, keyPath, "usernameEnv"); err != nil {
		return s, err
	}
	if s.PasswordEnv, err = text(m, keyPath, "passwordEnv"); err != nil {
		return s, err
	}
	if s.CAFile, err = text(m, keyPath, "caFile"); err != nil {
		return s, err
	}

	// Coerência: o que só faz sentido com SASL é OBRIGATÓRIO com SASL e
	// PROIBIDO sem. Aceitar `usernameEnv` sob PLAINTEXT seria aceitar uma
	// credencial que nunca será usada — o operador acharia que autenticou.
	if s.SASL() {
		switch s.SASLMechanism {
		case SASLPlain, SASLScramSHA256, SASLScramSHA512:
		case "":
			return s, specErrf("%s.saslMechanism obrigatório com protocol %s", keyPath, s.Protocol)
		default:
			return s, specErrf("%s.saslMechanism %q inválido (%s|%s|%s)",
				keyPath, s.SASLMechanism, SASLPlain, SASLScramSHA256, SASLScramSHA512)
		}
		if s.UsernameEnv == "" {
			return s, specErrf("%s.usernameEnv obrigatório com protocol %s", keyPath, s.Protocol)
		}
		if s.PasswordEnv == "" {
			return s, specErrf("%s.passwordEnv obrigatório com protocol %s", keyPath, s.Protocol)
		}
		if err := validateEnvName(keyPath+".usernameEnv", s.UsernameEnv); err != nil {
			return s, err
		}
		if err := validateEnvName(keyPath+".passwordEnv", s.PasswordEnv); err != nil {
			return s, err
		}
	} else {
		if s.SASLMechanism != "" {
			return s, specErrf("%s.saslMechanism não se aplica a protocol %s", keyPath, s.Protocol)
		}
		if s.UsernameEnv != "" || s.PasswordEnv != "" {
			return s, specErrf("%s.usernameEnv/passwordEnv não se aplicam a protocol %s", keyPath, s.Protocol)
		}
	}
	if s.CAFile != "" && !s.TLS() {
		return s, specErrf("%s.caFile não se aplica a protocol %s (use %s ou %s)",
			keyPath, s.Protocol, ProtocolSSL, ProtocolSASLSSL)
	}
	return s, nil
}

// validateEnvName confere que o valor é um NOME de variável de ambiente. É a
// última linha de defesa contra o erro que o §95 mais teme: alguém escrever a
// senha onde deveria ir o nome da variável, e ela ir para o repositório.
func validateEnvName(keyPath, v string) error {
	if v == "" {
		return specErrf("%s vazio", keyPath)
	}
	for i, c := range v {
		ok := c == '_' ||
			(c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(i > 0 && c >= '0' && c <= '9')
		if !ok {
			return specErrf("%s %q não é nome de variável de ambiente ([A-Za-z_][A-Za-z0-9_]*) — "+
				"esta chave recebe o NOME da variável, nunca o valor", keyPath, v)
		}
	}
	return nil
}

// validateErrors valida o bloco `errors` da raiz (§101, §102).
func validateErrors(n *yaml.Node) (ErrorsSpec, error) {
	e := ErrorsSpec{InvalidEvent: InvalidEventLogAndSkip}
	if absent(n) {
		return e, nil
	}
	m, err := block(n, "errors", "invalidEvent", "deadLetterTopic")
	if err != nil {
		return e, err
	}
	if v, err := text(m, "errors", "invalidEvent"); err != nil {
		return e, err
	} else if v != "" {
		if v != InvalidEventLogAndSkip && v != InvalidEventStop {
			return e, specErrf("errors.invalidEvent %q inválido (%s|%s)",
				v, InvalidEventLogAndSkip, InvalidEventStop)
		}
		e.InvalidEvent = v
	}
	if e.DeadLetterTopic, err = text(m, "errors", "deadLetterTopic"); err != nil {
		return e, err
	}
	if _, ok := m["deadLetterTopic"]; ok && e.DeadLetterTopic == "" {
		return e, specErrf("errors.deadLetterTopic vazio (omita a chave para não usar dead-letter)")
	}
	return e, nil
}
