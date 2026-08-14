package schema

import (
	"net"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/nishisan-dev/nishidb/internal/units"
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
}

// SecuritySpec é `source.kafka.security` (§12).
//
// As credenciais são NOMES de variável de ambiente, nunca valores: o YAML é
// versionado e o §95 proíbe registrar senha. A resolução acontece na subida do
// consumer, não no parse — é isso que mantém o fixture do §12 válido numa
// suíte que roda sem KAFKA_USERNAME/KAFKA_PASSWORD no ambiente.
type SecuritySpec struct {
	Protocol      string
	SASLMechanism string
	UsernameEnv   string
	PasswordEnv   string
	CAFile        string
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
	MaxPollRecordsPadrao      = 1000
	IngestionQueueSizePadrao  = 5000
	IngestionQueueBytesPadrao = 64 << 20
)

// SASL diz se o protocolo exige credencial.
func (s SecuritySpec) SASL() bool {
	return s.Protocol == ProtocolSASLPlaintext || s.Protocol == ProtocolSASLSSL
}

// TLS diz se o transporte é cifrado.
func (s SecuritySpec) TLS() bool {
	return s.Protocol == ProtocolSSL || s.Protocol == ProtocolSASLSSL
}

// bloco lê um nó de mapeamento recusando chave desconhecida ou duplicada.
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
// ausente diz se o bloco não veio no documento. Kind zero é o nó que o
// yaml.v3 deixa quando a chave não existe — não há sentinela melhor, porque o
// campo é valor e não ponteiro.
func ausente(n *yaml.Node) bool { return n == nil || n.Kind == 0 }

func bloco(n *yaml.Node, caminho string, permitidas ...string) (map[string]*yaml.Node, error) {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n == nil || n.Kind != yaml.MappingNode {
		return nil, specErrf("%s deve ser um mapa", caminho)
	}
	out := make(map[string]*yaml.Node, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i].Value
		if !slices.Contains(permitidas, k) {
			return nil, specErrf("%s.%s: chave desconhecida (conhecidas: %s)",
				caminho, k, strings.Join(permitidas, ", "))
		}
		if _, dup := out[k]; dup {
			return nil, specErrf("%s.%s: chave duplicada", caminho, k)
		}
		out[k] = n.Content[i+1]
	}
	return out, nil
}

// decodeEm decodifica um nó no alvo, traduzindo o erro do yaml para a
// gramática de erro do pacote (caminho completo da chave + valor recebido).
func decodeEm(n *yaml.Node, caminho string, alvo any) error {
	if err := n.Decode(alvo); err != nil {
		return specErrf("%s: valor inválido (%v)", caminho, err)
	}
	return nil
}

// texto lê uma chave string opcional.
func texto(m map[string]*yaml.Node, caminho, chave string) (string, error) {
	n, ok := m[chave]
	if !ok {
		return "", nil
	}
	var v string
	if err := decodeEm(n, caminho+"."+chave, &v); err != nil {
		return "", err
	}
	return strings.TrimSpace(v), nil
}

// validaSource valida o bloco `source` (§12). Bloco ausente é válido: o
// dataset alimentado por `nishidb ingest` não tem fonte declarada, e
// testdata/todos_os_tipos.yaml precisa continuar válido.
func validaSource(n *yaml.Node) (SourceSpec, error) {
	var s SourceSpec
	if ausente(n) {
		return s, nil
	}
	m, err := bloco(n, "source", "type", "kafka")
	if err != nil {
		return s, err
	}
	tipo, err := texto(m, "source", "type")
	if err != nil {
		return s, err
	}
	nk, temKafka := m["kafka"]

	// O enum fechado é a diferença entre descobrir o erro de digitação no
	// parse e descobri-lo na primeira tentativa de conexão, em produção.
	switch {
	case tipo == "":
		return s, specErrf("source.type obrigatório quando o bloco source existe (v1: %s)", SourceKafka)
	case tipo != SourceKafka:
		return s, specErrf("source.type %q não suportado (v1: %s)", tipo, SourceKafka)
	case !temKafka:
		return s, specErrf("source.type: %s exige o bloco source.kafka", SourceKafka)
	}
	s.Type = SourceKafka

	k, err := validaKafka(nk)
	if err != nil {
		return s, err
	}
	s.Kafka = k
	return s, nil
}

func validaKafka(n *yaml.Node) (*KafkaSpec, error) {
	m, err := bloco(n, "source.kafka",
		"enabled", "bootstrapServers", "topic", "consumer", "security")
	if err != nil {
		return nil, err
	}
	k := &KafkaSpec{}

	if en, ok := m["enabled"]; ok {
		if err := decodeEm(en, "source.kafka.enabled", &k.Enabled); err != nil {
			return nil, err
		}
	}

	bn, ok := m["bootstrapServers"]
	if !ok {
		return nil, specErrf("source.kafka.bootstrapServers obrigatório")
	}
	if err := decodeEm(bn, "source.kafka.bootstrapServers", &k.BootstrapServers); err != nil {
		return nil, err
	}
	if err := validaBootstrap(k.BootstrapServers); err != nil {
		return nil, err
	}

	if k.Topic, err = texto(m, "source.kafka", "topic"); err != nil {
		return nil, err
	}
	if k.Topic == "" {
		return nil, specErrf("source.kafka.topic obrigatório")
	}

	if k.Consumer, err = validaConsumer(m["consumer"]); err != nil {
		return nil, err
	}
	if k.Security, err = validaSecurity(m["security"]); err != nil {
		return nil, err
	}
	return k, nil
}

// validaBootstrap exige host:porta em cada item.
//
// Usa net.SplitHostPort, e não um split ingênuo por ':', porque a spec não
// define a gramática do item e um split por ':' tornaria um cluster IPv6-only
// ([2001:db8::1]:9092) inconfigurável — e não com um erro de conexão, mas com
// um erro de PARSE, que impediria até `nishidb query` de abrir o dataset.
func validaBootstrap(itens []string) error {
	if len(itens) == 0 {
		return specErrf("source.kafka.bootstrapServers não pode ser lista vazia")
	}
	for i, item := range itens {
		item = strings.TrimSpace(item)
		host, porta, err := net.SplitHostPort(item)
		if err != nil {
			return specErrf("source.kafka.bootstrapServers[%d] %q inválido (esperado host:porta)", i, item)
		}
		if host == "" {
			return specErrf("source.kafka.bootstrapServers[%d] %q sem host", i, item)
		}
		p, err := strconv.Atoi(porta)
		if err != nil || p < 1 || p > 65535 {
			return specErrf("source.kafka.bootstrapServers[%d] %q: porta inválida (1..65535)", i, item)
		}
	}
	return nil
}

func validaConsumer(n *yaml.Node) (ConsumerSpec, error) {
	c := ConsumerSpec{
		AutoOffsetReset:     AutoOffsetResetEarliest,
		MaxPollRecords:      MaxPollRecordsPadrao,
		IngestionQueueSize:  IngestionQueueSizePadrao,
		IngestionQueueBytes: IngestionQueueBytesPadrao,
	}
	if n == nil {
		return c, specErrf("source.kafka.consumer.groupId obrigatório")
	}
	m, err := bloco(n, "source.kafka.consumer",
		"groupId", "autoOffsetReset", "maxPollRecords", "ingestionQueueSize", "ingestionQueueBytes")
	if err != nil {
		return c, err
	}

	if c.GroupID, err = texto(m, "source.kafka.consumer", "groupId"); err != nil {
		return c, err
	}
	if c.GroupID == "" {
		return c, specErrf("source.kafka.consumer.groupId obrigatório")
	}
	if strings.ContainsAny(c.GroupID, " \t\n\r") {
		return c, specErrf("source.kafka.consumer.groupId %q não pode conter espaço", c.GroupID)
	}

	if v, err := texto(m, "source.kafka.consumer", "autoOffsetReset"); err != nil {
		return c, err
	} else if v != "" {
		if v != AutoOffsetResetEarliest && v != AutoOffsetResetLatest {
			return c, specErrf("source.kafka.consumer.autoOffsetReset %q inválido (%s|%s)",
				v, AutoOffsetResetEarliest, AutoOffsetResetLatest)
		}
		c.AutoOffsetReset = v
	}

	if c.MaxPollRecords, err = inteiro(m, "source.kafka.consumer", "maxPollRecords", MaxPollRecordsPadrao); err != nil {
		return c, err
	}
	if c.IngestionQueueSize, err = inteiro(m, "source.kafka.consumer", "ingestionQueueSize", IngestionQueueSizePadrao); err != nil {
		return c, err
	}
	if bytesTxt, err := texto(m, "source.kafka.consumer", "ingestionQueueBytes"); err != nil {
		return c, err
	} else if c.IngestionQueueBytes, err = units.Tamanho(bytesTxt, IngestionQueueBytesPadrao); err != nil {
		return c, specErrf("source.kafka.consumer.ingestionQueueBytes: %v", err)
	}
	return c, nil
}

// inteiro aplica a mesma regra de units.InteiroPositivo: ausente herda o
// padrão, zero e negativo são recusados. Uma fila de capacidade 0 não é "sem
// fila", é uma fila que nada atravessa.
func inteiro(m map[string]*yaml.Node, caminho, chave string, padrao int) (int, error) {
	n, ok := m[chave]
	if !ok {
		return padrao, nil
	}
	var v int
	if err := decodeEm(n, caminho+"."+chave, &v); err != nil {
		return 0, err
	}
	out, err := units.InteiroPositivo(&v, padrao)
	if err != nil {
		return 0, specErrf("%s.%s: %v", caminho, chave, err)
	}
	return out, nil
}

func validaSecurity(n *yaml.Node) (SecuritySpec, error) {
	s := SecuritySpec{Protocol: ProtocolPlaintext}
	if n == nil {
		return s, nil
	}
	m, err := bloco(n, "source.kafka.security",
		"protocol", "saslMechanism", "usernameEnv", "passwordEnv", "caFile")
	if err != nil {
		return s, err
	}

	if v, err := texto(m, "source.kafka.security", "protocol"); err != nil {
		return s, err
	} else if v != "" {
		switch v {
		case ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL:
			s.Protocol = v
		default:
			return s, specErrf("source.kafka.security.protocol %q inválido (%s|%s|%s|%s)",
				v, ProtocolPlaintext, ProtocolSSL, ProtocolSASLPlaintext, ProtocolSASLSSL)
		}
	}

	if s.SASLMechanism, err = texto(m, "source.kafka.security", "saslMechanism"); err != nil {
		return s, err
	}
	if s.UsernameEnv, err = texto(m, "source.kafka.security", "usernameEnv"); err != nil {
		return s, err
	}
	if s.PasswordEnv, err = texto(m, "source.kafka.security", "passwordEnv"); err != nil {
		return s, err
	}
	if s.CAFile, err = texto(m, "source.kafka.security", "caFile"); err != nil {
		return s, err
	}

	// Coerência: o que só faz sentido com SASL é OBRIGATÓRIO com SASL e
	// PROIBIDO sem. Aceitar `usernameEnv` sob PLAINTEXT seria aceitar uma
	// credencial que nunca será usada — o operador acharia que autenticou.
	if s.SASL() {
		switch s.SASLMechanism {
		case SASLPlain, SASLScramSHA256, SASLScramSHA512:
		case "":
			return s, specErrf("source.kafka.security.saslMechanism obrigatório com protocol %s", s.Protocol)
		default:
			return s, specErrf("source.kafka.security.saslMechanism %q inválido (%s|%s|%s)",
				s.SASLMechanism, SASLPlain, SASLScramSHA256, SASLScramSHA512)
		}
		if s.UsernameEnv == "" {
			return s, specErrf("source.kafka.security.usernameEnv obrigatório com protocol %s", s.Protocol)
		}
		if s.PasswordEnv == "" {
			return s, specErrf("source.kafka.security.passwordEnv obrigatório com protocol %s", s.Protocol)
		}
		if err := validaNomeDeEnv("source.kafka.security.usernameEnv", s.UsernameEnv); err != nil {
			return s, err
		}
		if err := validaNomeDeEnv("source.kafka.security.passwordEnv", s.PasswordEnv); err != nil {
			return s, err
		}
	} else {
		if s.SASLMechanism != "" {
			return s, specErrf("source.kafka.security.saslMechanism não se aplica a protocol %s", s.Protocol)
		}
		if s.UsernameEnv != "" || s.PasswordEnv != "" {
			return s, specErrf("source.kafka.security.usernameEnv/passwordEnv não se aplicam a protocol %s", s.Protocol)
		}
	}
	if s.CAFile != "" && !s.TLS() {
		return s, specErrf("source.kafka.security.caFile não se aplica a protocol %s (use %s ou %s)",
			s.Protocol, ProtocolSSL, ProtocolSASLSSL)
	}
	return s, nil
}

// validaNomeDeEnv confere que o valor é um NOME de variável de ambiente. É a
// última linha de defesa contra o erro que o §95 mais teme: alguém escrever a
// senha onde deveria ir o nome da variável, e ela ir para o repositório.
func validaNomeDeEnv(caminho, v string) error {
	if v == "" {
		return specErrf("%s vazio", caminho)
	}
	for i, c := range v {
		ok := c == '_' ||
			(c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(i > 0 && c >= '0' && c <= '9')
		if !ok {
			return specErrf("%s %q não é nome de variável de ambiente ([A-Za-z_][A-Za-z0-9_]*) — "+
				"esta chave recebe o NOME da variável, nunca o valor", caminho, v)
		}
	}
	return nil
}

// validaErrors valida o bloco `errors` da raiz (§101, §102).
func validaErrors(n *yaml.Node) (ErrorsSpec, error) {
	e := ErrorsSpec{InvalidEvent: InvalidEventLogAndSkip}
	if ausente(n) {
		return e, nil
	}
	m, err := bloco(n, "errors", "invalidEvent", "deadLetterTopic")
	if err != nil {
		return e, err
	}
	if v, err := texto(m, "errors", "invalidEvent"); err != nil {
		return e, err
	} else if v != "" {
		if v != InvalidEventLogAndSkip && v != InvalidEventStop {
			return e, specErrf("errors.invalidEvent %q inválido (%s|%s)",
				v, InvalidEventLogAndSkip, InvalidEventStop)
		}
		e.InvalidEvent = v
	}
	if e.DeadLetterTopic, err = texto(m, "errors", "deadLetterTopic"); err != nil {
		return e, err
	}
	if _, ok := m["deadLetterTopic"]; ok && e.DeadLetterTopic == "" {
		return e, specErrf("errors.deadLetterTopic vazio (omita a chave para não usar dead-letter)")
	}
	return e, nil
}
