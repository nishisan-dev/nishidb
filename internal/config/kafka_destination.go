package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/units"
	"gopkg.in/yaml.v3"
)

// kafkaTopicName é a regra do nome de tópico para todo destino Kafka de
// produção: a série (`seriesDelivery.kafka`) e os alarmes
// (`alarmDelivery.kafka`). Mora aqui, e não num dos dois blocos, para que
// nenhum deles redeclare a regex.
var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

// kafkaTarget é a conexão de um destino Kafka de produção, comum à série e
// aos alarmes. Os dois blocos continuam com tipos próprios — o nome, as chaves
// a mais e o sentinela de erro são de cada um —, e só a validação da conexão
// é compartilhada, parametrizada pelo sentinela e pelo caminho da chave.
//
// O timeout não entra aqui: cada bloco o resolve pela própria função
// (SeriesKafkaSpec.DeliveryTimeout, AlarmKafkaSpec.Limits), ambas sobre
// deliveryTimeout, e uma segunda conferência só mascararia a primeira.
type kafkaTarget struct {
	path, topic string
	bootstrap   []string
	security    schema.SecuritySpec
}

// validate confere tópico, bootstrap e o contrato de segurança. Credencial e
// CA NÃO são resolvidas aqui: isso é da subida do produtor
// (kafka.ResolveSecurity), que lê o ambiente e o disco.
func (k kafkaTarget) validate(sentinel error) error {
	if !kafkaTopicName.MatchString(k.topic) || k.topic == "." || k.topic == ".." {
		return fmt.Errorf("%w: %s.topic is invalid", sentinel, k.path)
	}
	if err := schema.ValidateKafkaBootstrap(k.bootstrap, k.path+".bootstrapServers"); err != nil {
		return fmt.Errorf("%w: %v", sentinel, err)
	}
	var node yaml.Node
	if err := node.Encode(k.security); err != nil {
		return fmt.Errorf("%w: %v", sentinel, err)
	}
	if _, err := schema.ParseKafkaSecurity(&node, k.path+".security"); err != nil {
		return fmt.Errorf("%w: %v", sentinel, err)
	}
	return nil
}

// deliveryTimeout resolve o prazo de UMA tentativa de entrega: default 5s,
// faixa (0, 5m]. É a mesma regra para o webhook, o Kafka dos alarmes e o
// Kafka da série, e por isso a mensagem fala em destino, e não no tipo.
func deliveryTimeout(sentinel error, name, raw string) (time.Duration, error) {
	d, err := units.Duration(raw, 5*time.Second)
	if err != nil || d <= 0 || d > 5*time.Minute {
		return 0, fmt.Errorf("%w: destination %q timeout must be in (0, 5m]", sentinel, name)
	}
	return d, nil
}

// parseKafkaTargetNode valida, no nó YAML de um destino Kafka, o que só o nó
// enxerga: chave desconhecida, timeout em branco e o bloco de segurança, que
// devolve com os defaults aplicados. extra são as chaves próprias do bloco
// além das da conexão.
func parseKafkaTargetNode(item *yaml.Node, path string, sentinel error, extra ...string) (schema.SecuritySpec, error) {
	keys := append([]string{"name", "bootstrapServers", "topic", "timeout", "security"}, extra...)
	if err := knownKeys(item, path, keys...); err != nil {
		return schema.SecuritySpec{}, fmt.Errorf("%w: %v", sentinel, err)
	}
	if timeout := child(item, "timeout"); timeout != nil && strings.TrimSpace(timeout.Value) == "" {
		return schema.SecuritySpec{}, fmt.Errorf("%w: %s.timeout must not be blank", sentinel, path)
	}
	security, err := schema.ParseKafkaSecurity(child(item, "security"), path+".security")
	if err != nil {
		return schema.SecuritySpec{}, fmt.Errorf("%w: %v", sentinel, err)
	}
	return security, nil
}

// sequenceNode resolve o alias e exige lista.
func sequenceNode(n *yaml.Node, sentinel error, path string) (*yaml.Node, error) {
	for n.Kind == yaml.AliasNode {
		n = n.Alias
	}
	if n.Kind != yaml.SequenceNode {
		return nil, fmt.Errorf("%w: %s must be a list", sentinel, path)
	}
	return n, nil
}
