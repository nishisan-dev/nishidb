package config

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/schema"
)

const kafkaTargetsConfig = "kafkaTargets:\n  - name: metrics\n    bootstrapServers: ['localhost:9092', '[::1]:9092']\n    topic: counter.samples\n"

func TestKafkaTargetDefaultsAndSecurity(t *testing.T) {
	c, err := Load(writeConfig(t, kafkaTargetsConfig))
	if err != nil {
		t.Fatal(err)
	}
	k := c.KafkaTargets[0]
	d, err := k.DeliveryTimeout()
	if err != nil || d != 5*time.Second || k.Security.Protocol != schema.ProtocolPlaintext {
		t.Fatalf("%+v %v %v", k, d, err)
	}
	if k.MaxMessageBytes != DefaultKafkaMessageBytes {
		t.Fatalf("maxMessageBytes ausente deveria resolver para o default %d, veio %d", DefaultKafkaMessageBytes, k.MaxMessageBytes)
	}
	for _, mechanism := range []string{schema.SASLPlain, schema.SASLScramSHA256, schema.SASLScramSHA512} {
		c, err = Load(writeConfig(t, kafkaTargetsConfig+"    timeout: 200ms\n    security:\n      protocol: SASL_SSL\n      saslMechanism: "+mechanism+"\n      usernameEnv: TEST_KAFKA_USER\n      passwordEnv: TEST_KAFKA_PASSWORD\n      caFile: /ca.pem\n"))
		if err != nil {
			t.Fatal(err)
		}
		k = c.KafkaTargets[0]
		if k.Security.SASLMechanism != mechanism || k.Security.CAFile != "/ca.pem" {
			t.Fatalf("security lost: %+v", k)
		}
	}
}

func TestKafkaTargetRejectsInvalidConfiguration(t *testing.T) {
	for _, body := range []string{
		"kafkaTargets: {}\n", "kafkaTargets: metrics\n",
		kafkaTargetsConfig + "    unexpected: true\n", kafkaTargetsConfig + "    timeout: ''\n", kafkaTargetsConfig + "    timeout: 0s\n", kafkaTargetsConfig + "    timeout: 301s\n",
		strings.Replace(kafkaTargetsConfig, "name: metrics", "name: file", 1), strings.Replace(kafkaTargetsConfig, "name: metrics", "name: bad-name", 1),
		strings.Replace(kafkaTargetsConfig, "counter.samples", "..", 1), strings.Replace(kafkaTargetsConfig, "counter.samples", "bad topic", 1),
		strings.Replace(kafkaTargetsConfig, "['localhost:9092', '[::1]:9092']", "[]", 1), strings.Replace(kafkaTargetsConfig, "localhost:9092", "localhost:99999", 1),
		kafkaTargetsConfig + "    security:\n      protocol: UNKNOWN\n", kafkaTargetsConfig + "    security:\n      passwordEnv: PASSWORD\n",
		kafkaTargetsConfig + "    security:\n      protocol: SASL_SSL\n", kafkaTargetsConfig + "    security:\n      caFile: /ca.pem\n",
		kafkaTargetsConfig + "    security:\n      protocoll: SSL\n",
		kafkaTargetsConfig + "  - name: metrics\n    bootstrapServers: ['localhost:9092']\n    topic: other\n",
	} {
		_, err := Load(writeConfig(t, body))
		if err == nil || !errors.Is(err, ErrKafkaTarget) && !strings.Contains(err.Error(), "cannot unmarshal") {
			t.Errorf("wrong refusal for %s: %v", body, err)
		}
	}
}

// O broker com vírgula num item só é recusado na carga de `kafkaTargets:`,
// pela guarda do schema: aceito, ele quebraria para sempre o checkpoint do
// publicador da replicação, que junta os brokers por vírgula.
func TestKafkaTargetRefusesBootstrapServerWithComma(t *testing.T) {
	_, err := Load(writeConfig(t, strings.Replace(kafkaTargetsConfig, "'localhost:9092'", "'kafka01,kafka02:9092'", 1)))
	if !errors.Is(err, ErrKafkaTarget) || !strings.Contains(err.Error(), "contém vírgula") {
		t.Fatalf("recusa = %v, quer ErrKafkaTarget com a mensagem da guarda da vírgula", err)
	}
}

// TestKafkaTargetMaxMessageBytesAcceptsSizesInRange cobre as duas grafias que
// a chave aceita (unidade binária e bytes puros) e as duas pontas da faixa,
// que são inclusivas.
func TestKafkaTargetMaxMessageBytesAcceptsSizesInRange(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int64
	}{
		{"1MiB", 1 << 20}, {"2097152", 2 << 20}, {"1KiB", 1 << 10}, {"64MiB", 64 << 20},
	} {
		c, err := Load(writeConfig(t, kafkaTargetsConfig+"    maxMessageBytes: "+tc.value+"\n"))
		if err != nil {
			t.Fatalf("%s: %v", tc.value, err)
		}
		if got := c.KafkaTargets[0].MaxMessageBytes; got != tc.want {
			t.Errorf("maxMessageBytes %s resolveu para %d, queria %d", tc.value, got, tc.want)
		}
	}
}

// TestKafkaTargetMaxMessageBytesRefusesOutOfRangeWithoutRounding assevera a
// mensagem da PRÓPRIA guarda de faixa: 1023 e 64MiB+1 são tamanhos válidos
// para o units.Size, então só a faixa os recusa. O vazio, o decimal e o zero
// são recusados pela guarda de forma/units, e o teste confere que a recusa
// nomeia a chave.
func TestKafkaTargetMaxMessageBytesRefusesOutOfRangeWithoutRounding(t *testing.T) {
	for _, value := range []string{"1023", "67108865", "65MiB"} {
		_, err := Load(writeConfig(t, kafkaTargetsConfig+"    maxMessageBytes: "+value+"\n"))
		if !errors.Is(err, ErrKafkaTarget) || !strings.Contains(err.Error(), "kafkaTargets[0].maxMessageBytes must be in [1KiB, 64MiB]") {
			t.Errorf("maxMessageBytes %s: recusa errada ou ausente: %v", value, err)
		}
	}
	for _, value := range []string{"''", "1MB", "0", "-5"} {
		_, err := Load(writeConfig(t, kafkaTargetsConfig+"    maxMessageBytes: "+value+"\n"))
		if !errors.Is(err, ErrKafkaTarget) || !strings.Contains(err.Error(), "kafkaTargets[0].maxMessageBytes") {
			t.Errorf("maxMessageBytes %s: recusa errada ou ausente: %v", value, err)
		}
	}
}

// TestKafkaTargetMaxMessageBytesRangeAppliesToGoAssembledConfig cobre a outra
// porta, o Config montado em Go (sem Load), nas duas pontas da faixa e logo
// fora delas. Zero é "não declarado" e vale o default.
func TestKafkaTargetMaxMessageBytesRangeAppliesToGoAssembledConfig(t *testing.T) {
	for _, tc := range []struct {
		value int64
		ok    bool
	}{
		{0, true}, {1023, false}, {1 << 10, true}, {64 << 20, true}, {64<<20 + 1, false},
	} {
		c := &Config{KafkaTargets: []KafkaTargetSpec{{Name: "m", BootstrapServers: []string{"localhost:9092"}, Topic: "t", MaxMessageBytes: tc.value}}}
		err := c.ValidateKafkaTargets()
		if tc.ok && err != nil {
			t.Errorf("maxMessageBytes %d montado em Go deveria passar: %v", tc.value, err)
		}
		if !tc.ok && (!errors.Is(err, ErrKafkaTarget) || !strings.Contains(err.Error(), `destination "m" maxMessageBytes must be in [1KiB, 64MiB]`)) {
			t.Errorf("maxMessageBytes %d montado em Go: recusa errada ou ausente: %v", tc.value, err)
		}
	}
}

// TestLegacySeriesDeliveryRefusedWithRenameMessage prova a guarda da chave
// antiga. Sem ela o decoder (KnownFields desligado) ignoraria o bloco em
// silêncio e o arquivo subiria sem destino nenhum — por isso a asserção é a
// frase da PRÓPRIA guarda, e o mesmo arquivo sem o bloco antigo tem de subir.
func TestLegacySeriesDeliveryRefusedWithRenameMessage(t *testing.T) {
	legacy := "seriesDelivery:\n  kafka:\n    - name: metrics\n      bootstrapServers: ['localhost:9092']\n      topic: counter.samples\n"
	_, err := Load(writeConfig(t, legacy))
	if !errors.Is(err, ErrKafkaTarget) || !strings.Contains(err.Error(), "seriesDelivery was renamed to kafkaTargets") {
		t.Fatalf("seriesDelivery deveria ser recusado com a mensagem de rename, veio: %v", err)
	}
	// Qualquer forma é recusada — mapa vazio, nulo, lista: a presença da chave
	// é o que denuncia o arquivo escrito para a versão anterior.
	for _, body := range []string{"seriesDelivery: {}\n", "seriesDelivery:\n", "seriesDelivery: []\n"} {
		if _, err := Load(writeConfig(t, body)); !errors.Is(err, ErrKafkaTarget) || !strings.Contains(err.Error(), "seriesDelivery was renamed to kafkaTargets") {
			t.Errorf("%q deveria ser recusado com a mensagem de rename, veio: %v", body, err)
		}
	}
	if _, err := Load(writeConfig(t, kafkaTargetsConfig)); err != nil {
		t.Fatalf("o mesmo destino em kafkaTargets deveria subir: %v", err)
	}
}
