package kafka_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nishisan-dev/nishidb/internal/kafka"
)

// Os dois testes do débito 56. O checkpoint — e o commit de offset que vem
// depois dele (§48) — era disparado só POR EVENTO: terminado o consumo, o
// offset commitado no group parava no último checkpoint e o
// `nishidb_kafka_lag` ficava marcando a diferença até chegar evento novo.
// Medido na rodada de aceite da Fase 6: lag de 499.999 parado com o
// consumidor em dia.

const (
	// intervaloOcioso é curto de propósito. O que se afirma aqui é que EXISTE
	// disparo por tempo, não qual é o valor de produção; esperar os 30s do
	// §78 deixaria a suíte inútil sem provar nada a mais.
	intervaloOcioso = 100 * time.Millisecond

	// prazoOcioso é o teto do consumo. Generoso porque é prazo de FALHA: o
	// caminho verde sai em ~um intervalo, e quem espera os cinco segundos
	// inteiros é o teste que está reprovando.
	prazoOcioso = 5 * time.Second

	eventosOciosos = 10
)

// cadenciaOciosa isola o disparo por TEMPO: a contagem está uma ordem de
// grandeza acima dos eventos produzidos, então um checkpoint durável só pode
// ter vindo do relógio.
var cadenciaOciosa = kafka.Cadencia{Eventos: 1000, Intervalo: intervaloOcioso}

// TestCheckpointPorTempoDestravaOLagComIngestaoOciosa reproduz o débito 56:
// com a fila seca, o disparo por contagem nunca vem e o writer dorme no
// channel para sempre — a posição consumida nunca vira posição durável, e o
// commit no broker (§48) que vem depois dela também não acontece.
//
// O veredito é colhido DENTRO do laço, com o consumidor ainda de pé: o
// checkpoint de encerramento grava o kafka.offsets de qualquer jeito, e uma
// asserção feita depois do consumo ficaria verde com o débito aberto.
func TestCheckpointPorTempoDestravaOLagComIngestaoOciosa(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	produzOciosos(t, b)

	var destravou bool
	if _, err := b.consomeComCadencia(prazoOcioso, cadenciaOciosa, func(d kafka.Desfecho) bool {
		if d.Eventos < eventosOciosos {
			return false
		}
		// Daqui em diante a ingestão está ociosa: tudo o que havia no tópico
		// já foi decidido e nada novo é produzido.
		_, destravou = b.leOffsets()
		return destravou
	}); err != nil {
		t.Fatal(err)
	}
	if !destravou {
		t.Errorf("kafka.offsets não apareceu em %s de ingestão ociosa, com %d eventos decididos e "+
			"cadência por contagem em %d: o checkpoint por tempo não disparou e o lag fica congelado",
			prazoOcioso, eventosOciosos, cadenciaOciosa.Eventos)
	}
}

// TestCheckpointPorTempoNaoRepeteSemEventoNovo fixa o outro lado da mesma
// moeda: o despertar do laço AVALIA a regra, não a substitui.
//
// Sem evento decidido desde o último checkpoint não há posição nova para
// tornar durável nem offset novo para commitar. Um despertar que
// checkpointasse por si só faria todo dataset parado reescrever o manifest e
// dar fsync a cada intervalo, para sempre — write amplification permanente
// num dataset que não recebeu um único byte, e o §78 põe o intervalo padrão
// em 30s justamente porque o checkpoint não é barato.
func TestCheckpointPorTempoNaoRepeteSemEventoNovo(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	produzOciosos(t, b)

	// A ociosidade é medida em intervalos, não em relógio de parede: é o
	// número de despertares que teriam checkpointado indevidamente.
	const intervalosOciosos = 5

	base := contaCheckpoints(t, b)
	var naPrimeira, aoFim int
	var marca time.Time
	if _, err := b.consomeComCadencia(prazoOcioso, cadenciaOciosa, func(d kafka.Desfecho) bool {
		if d.Eventos < eventosOciosos {
			return false
		}
		if marca.IsZero() {
			n := contaCheckpoints(t, b)
			if n == base {
				return false // o disparo por tempo ainda não veio
			}
			naPrimeira, marca = n, time.Now()
			return false
		}
		if time.Since(marca) < intervalosOciosos*intervaloOcioso {
			return false
		}
		aoFim = contaCheckpoints(t, b)
		return true
	}); err != nil {
		t.Fatal(err)
	}

	if marca.IsZero() {
		t.Fatalf("o checkpoint por tempo não disparou em %s; sem ele este teste não tem o que afirmar",
			prazoOcioso)
	}
	if aoFim != naPrimeira {
		t.Errorf("checkpoints = %d após %d intervalos ociosos, quer %d: o despertar por tempo está "+
			"checkpointando sem evento novo", aoFim, intervalosOciosos, naPrimeira)
	}
}

// produzOciosos publica menos eventos que o limiar por contagem de
// cadenciaOciosa. É essa desproporção que faz o teste falar do relógio.
func produzOciosos(t *testing.T, b *bancada) {
	t.Helper()
	recs := make([]*kgo.Record, 0, eventosOciosos)
	for i := 0; i < eventosOciosos; i++ {
		recs = append(recs, evento(fmt.Sprintf("S%02d", i), 1, "Start"))
	}
	b.produz(recs...)
}

// contaCheckpoints lê a contagem de checkpoints BEM-SUCEDIDOS pela série do
// §92 que o store já publica. É o contador que existe — inventar um segundo
// só para o teste mediria outra coisa, e é a série que o operador olha.
func contaCheckpoints(t *testing.T, b *bancada) int {
	t.Helper()
	var sb strings.Builder
	if _, err := b.reg.WriteTo(&sb); err != nil {
		t.Fatalf("scrape do registry: %v", err)
	}
	const prefixo = `nishidb_checkpoint_duration_seconds_count{dataset="radius_sessions"} `
	for _, l := range strings.Split(sb.String(), "\n") {
		valor, ok := strings.CutPrefix(l, prefixo)
		if !ok {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(valor))
		if err != nil {
			t.Fatalf("contagem de checkpoints %q não é inteiro: %v", valor, err)
		}
		return n
	}
	t.Fatalf("série %q ausente no scrape", prefixo)
	return 0
}
