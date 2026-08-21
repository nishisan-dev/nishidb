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

// TestDespertarPorTempoNaoEncerraOLacoDoWriter cobra a única consequência
// observável da distinção entre DESPERTAR e FIM DE FILA — o terceiro retorno
// do recebeOuAcorda, e a cláusula que o consome no Roda.
//
// Os dois testes acima não conseguem cobrá-la, por construção: eles afirmam
// sobre artefatos que o ENCERRAMENTO também produz. O kafka.offsets é gravado
// pelo finaliza() na saída do laço, e uma contagem de checkpoints que não
// cresce é exatamente o que um writer morto deixa para trás. Um laço que
// encerra no primeiro tique satisfaz as duas asserções.
//
// Colapsada a distinção, o despertar cai no ramo de fila fechada e o writer
// devolve nil pelo finaliza(). O consumidor só cancela o poll quando o writer
// devolve ERRO (consumidor.go, no `if err != nil`), então nada acontece: sem
// pausa, sem log, sem métrica. O poller segue buscando, a fila enche até o
// gate do §128 e o Envia bloqueia para sempre. Em produção o intervalo é o
// storage.checkpoint.interval (§78, 30s por padrão) — a ingestão morreria em
// silêncio após meio minuto de ociosidade, que é o débito 56 agravado.
//
// O que se afirma aqui é a sobrevivência do laço: passados vários intervalos
// com a fila seca, os eventos produzidos DEPOIS têm de ser consumidos.
func TestDespertarPorTempoNaoEncerraOLacoDoWriter(t *testing.T) {
	// A ociosidade é contada em intervalos porque o que importa é o número de
	// despertares atravessados, não o relógio de parede.
	const (
		primeiraLeva      = 1
		segundaLeva       = 5
		intervalosOciosos = 4
	)

	b := novaBancada(t, 1)
	defer b.fecha()
	produzLeva(t, b, "A", primeiraLeva)

	var marca time.Time
	var produziu bool
	d, err := b.consomeComCadencia(prazoOcioso, cadenciaOciosa, func(d kafka.Desfecho) bool {
		switch {
		case d.Eventos < primeiraLeva:
			return false
		case marca.IsZero():
			// A primeira leva acabou e nada mais é produzido: daqui em diante
			// o writer só é acordado pelo relógio.
			marca = time.Now()
			return false
		case !produziu:
			if time.Since(marca) < intervalosOciosos*intervaloOcioso {
				return false
			}
			produzLeva(t, b, "B", segundaLeva)
			produziu = true
			return false
		default:
			return d.Eventos >= primeiraLeva+segundaLeva
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if d.Eventos < primeiraLeva+segundaLeva {
		t.Errorf("eventos consumidos = %d, quer %d: depois de %d intervalos ociosos o laço não "+
			"consumiu a leva nova — o despertar por tempo encerrou o writer em vez de reavaliar "+
			"o checkpoint, e a ingestão parou sem erro, sem pausa e sem métrica",
			d.Eventos, primeiraLeva+segundaLeva, intervalosOciosos)
	}
}

// produzOciosos publica menos eventos que o limiar por contagem de
// cadenciaOciosa. É essa desproporção que faz o teste falar do relógio.
func produzOciosos(t *testing.T, b *bancada) {
	t.Helper()
	produzLeva(t, b, "S", eventosOciosos)
}

// produzLeva publica n eventos de entidades distintas. O prefixo separa as
// levas: chaves repetidas entre elas cairiam na regra de sequência do §37 e a
// segunda leva viraria stale, decidida sem ter sido aplicada.
func produzLeva(t *testing.T, b *bancada, prefixo string, n int) {
	t.Helper()
	recs := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		recs = append(recs, evento(fmt.Sprintf("%s%02d", prefixo, i), 1, "Start"))
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

// TestCheckpointPorTempoVoltaADispararDepoisDeNovaLeva fixa a propriedade que
// mantém o débito 56 fechado EM REGIME, e que nenhum dos dois testes irmãos
// alcança: um afirma que o disparo por tempo acontece ao menos UMA vez, o
// outro que ele NÃO se repete sem evento novo. Um relógio de disparo único
// satisfaz os dois e reabre o débito no segundo período de ociosidade — que é
// o primeiro que um consumidor de verdade encontra, porque tráfego real vem em
// levas com fila seca entre elas.
//
// Medido: trocar o `time.NewTicker` do despertador por um `time.NewTimer`
// deixa a suíte inteira verde. É por isso que o comentário do despertador
// argumenta pelo ticker, e é isto que sustenta o argumento.
func TestCheckpointPorTempoVoltaADispararDepoisDeNovaLeva(t *testing.T) {
	b := novaBancada(t, 1)
	defer b.fecha()
	produzLeva(t, b, "A", eventosOciosos)

	base := contaCheckpoints(t, b)
	var aposA, aposB int

	if _, err := b.consomeComCadencia(prazoOcioso, cadenciaOciosa, func(d kafka.Desfecho) bool {
		if aposA == 0 {
			// Primeira ociosidade: espera a leva A ser decidida inteira e o
			// relógio disparar pela primeira vez.
			if d.Eventos < eventosOciosos {
				return false
			}
			if n := contaCheckpoints(t, b); n > base {
				aposA = n
				// A segunda leva usa prefixo próprio: chave repetida cairia na
				// regra de sequência do §37 e sairia como stale, decidida sem
				// ter sido aplicada — não haveria posição nova a tornar
				// durável e o segundo disparo seria barrado com razão.
				produzLeva(t, b, "B", eventosOciosos)
			}
			return false
		}
		// Segunda ociosidade: a leva B já foi decidida e a fila secou de novo.
		if d.Eventos < 2*eventosOciosos {
			return false
		}
		if n := contaCheckpoints(t, b); n > aposA {
			aposB = n
			return true
		}
		return false
	}); err != nil {
		t.Fatal(err)
	}

	if aposA == 0 {
		t.Fatal("o primeiro checkpoint por tempo não veio: o teste não chegou a medir a repetição")
	}
	if aposB <= aposA {
		t.Errorf("checkpoints: %d ao fim da primeira ociosidade e %d ao fim da segunda — "+
			"o relógio disparou uma vez só, e o débito 56 reabre a partir da segunda leva", aposA, aposB)
	}
}
