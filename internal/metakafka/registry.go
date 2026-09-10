// Package metakafka serve a tabela virtual `_meta.kafka`: o estado da fonte
// Kafka do dataset consultado, por SQL, uma linha por partição.
//
// Mora fora de `internal/kafka` e fora de `internal/query` porque precisa dos
// dois lados, e nenhum dos dois pode passar a importar o outro — é o mesmo
// argumento do `internal/counters`, e a mesma prova: `go list -deps` mostra
// que a ingestão não conhece o motor de consulta e o motor não conhece a
// ingestão. Um pacote separado é o que mantém isso verdadeiro.
package metakafka

import (
	"context"
	"fmt"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/query/meta"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// TabelaMetaKafka é o nome da tabela sob o `_meta`.
const TabelaMetaKafka = "kafka"

// Declarada é a identidade do fluxo COMO O YAML A ESCREVE, e vale sem
// consumidor nenhum: é ela que faz `SELECT * FROM _meta.kafka` responder num
// dataset com `enabled: false`, num servidor que ainda não subiu a ingestão e
// no `nishidb query --config`, onde não há processo de ingestão para observar.
type Declarada struct {
	// Declarado é "o dataset tem bloco source.kafka". Separado de Habilitado
	// porque as duas ausências pedem remédios diferentes: uma é "não há
	// fonte", a outra é "há fonte e alguém a desligou".
	Declarado  bool
	Habilitado bool
	Topic      string
	GroupID    string
	// Bootstrap já vem canonizado por checkpoint.CanonicalBootstrap: é a MESMA
	// forma que a origem grava no kafka.offsets e compara na divergência. Duas
	// grafias do mesmo endereço fariam a coluna discordar do arquivo sem nada
	// estar quebrado.
	Bootstrap string
}

// DoYAML monta a Declarada a partir do bloco do dataset. nil é aceito e vale
// "não declara Kafka" — que é uma resposta, não um erro.
func DoYAML(k *schema.KafkaSpec) Declarada {
	if k == nil {
		return Declarada{}
	}
	return Declarada{
		Declarado:  true,
		Habilitado: k.Enabled,
		Topic:      k.Topic,
		GroupID:    k.Consumer.GroupID,
		Bootstrap:  checkpoint.CanonicalBootstrap(k.BootstrapServers),
	}
}

// Consumido é o que a tabela precisa do consumidor CORRENTE. Interface, e não
// o *kafka.Consumidor, pela mesma razão do Leitor e do Contado em
// internal/counters: o teste da tabela não precisa de broker, e
// *kafka.Consumidor a satisfaz.
type Consumido interface {
	Instantaneo(ctx context.Context) kafka.Instantaneo
}

// Fonte é o que a tabela precisa do dataset, e é toda ela RESOLVIDA NA HORA.
//
// NENHUM campo guarda o consumidor. O registro acontece na abertura do
// dataset, ANTES de a ingestão subir — ali o ponteiro é nil —, e ele ainda
// TROCA depois disso, a cada recriação pelo watchdog. Um consumidor capturado
// no registro descreveria para sempre o primeiro que subiu; depois de uma
// recriação, descreveria um consumidor MORTO, cujo mapa de posições congelou
// no último offset que ele aplicou.
//
// Todo closure é opcional. Nil não é erro: é ausência, e ausência vira NULL na
// linha. É o que faz o `nishidb query --config` — sem servidor, sem
// consumidor, sem watchdog — responder com o que existe, sem inventar.
type Fonte struct {
	Declarada Declarada

	// Consumidor devolve o consumidor de AGORA, ou nil. Chamado UMA vez por
	// consulta, e nunca sob o lock do dataset: o Instantaneo pode falar com o
	// broker.
	Consumidor func() Consumido

	// Duraveis é o tri-estado de Store.SourceOffsets, passado adiante inteiro:
	// ok=false é "nunca consumiu", err != nil é "existe e está ilegível", e os
	// dois viram NULL na coluna — porque nenhum dos dois é um offset.
	Duraveis func() (checkpoint.SourceOffsets, bool, error)

	// Estado classifica a ingestão no vocabulário do §94. No servidor é a
	// MESMA função que o /v1/health e o /v1/stats usam — não uma segunda
	// classificação que descreveria a mesma realidade com outra palavra.
	// Estado vazio vira NULL.
	Estado func() (estado, detalhe string)

	// Vigia devolve as duas séries do watchdog, lidas dos MESMOS handles que
	// alimentam nishidb_kafka_watchdog_trips e _desistiu. ok=false enquanto
	// não há goroutine de ingestão — e aí as colunas são NULL, não zero:
	// "nunca recriou" e "não há watchdog" são afirmações diferentes.
	Vigia func() (trips uint64, desistiu, ok bool)
}

// EstadoDeclarado é o que o YAML SOZINHO prova sobre o estado, e é usado
// APENAS quando Fonte.Estado é nil.
//
// Sem bloco Kafka, ou com `enabled: false`, o veredito é DISABLED e não
// depende de processo nenhum. Com `enabled: true` e sem classificador, a
// resposta é "" (→ NULL): um processo que não observa a ingestão não pode
// dizer nem UP nem DOWN, e DEGRADED ("ainda subindo") seria mentira.
func DeclaredState(d Declarada) (estado, detalhe string) {
	if !d.Declarado || !d.Habilitado {
		return protocol.StateDisabled, ""
	}
	return "", ""
}

// Registra declara `_meta.kafka` sob o dataset já registrado no motor.
//
// Chamado por todo caminho que registra um dataset BASE — servidor e CLI —,
// SEMPRE, inclusive num dataset que não declara Kafka: "esta tabela não tem
// fonte" é uma resposta, e `SELECT *` nunca pode ser erro.
func Register(eng *query.Engine, dataset string, f Fonte) error {
	sc, store, err := Table(f)
	if err != nil {
		return err
	}
	return eng.RegisterVirtual(dataset, meta.Namespace, TabelaMetaKafka, sc, store)
}

func columnError(nome string) error {
	return fmt.Errorf("metakafka: coluna %q sem FieldID no schema sintético", nome)
}
