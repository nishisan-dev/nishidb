package kafka

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/metrics"
)

// TestRegistraMetricasPreRegistraAsSeisCategoriasDeEventoRuimEmZero prova o
// Minor 3 da revisão final da branch feature/m5-fundacao: antes da Task A5,
// nishidb_ingest_invalid_total nascia SEM label, e por isso já existia desde
// a subida, em zero. Depois de separar a série por motivo (famInvalidos), a
// família só materializa cada série no primeiro Apply que descartar um
// evento daquela categoria — mudança observável na superfície /metrics que o
// piloto em produção já expõe. Como o enum §101 de evento ruim é fechado
// (ErrorCategory.BadEvent()), pré-registrar as seis categorias em
// registraMetricas restaura a presença-em-zero, que é a prática canônica do
// Prometheus para séries cuja cardinalidade é conhecida de antemão.
func TestRegistraMetricasPreRegistraAsSeisCategoriasDeEventoRuimEmZero(t *testing.T) {
	m := registraMetricas(nil, "events")

	amostras := m.famInvalidos.Coleta()
	vistas := make(map[string]float64, len(amostras))
	for _, a := range amostras {
		var motivo string
		for _, l := range a.Labels {
			if l.Nome == metrics.LabelMotivo {
				motivo = l.Valor
			}
		}
		vistas[motivo] = a.Valor
	}

	// As seis categorias são lidas do próprio enum (BadEvent), não
	// hardcodadas aqui: o mesmo defeito que o comentário de NumCategories em
	// internal/materializer/materializer.go já nomeia (laço com limite
	// escrito à mão parando de reportar a última categoria) valeria para
	// este teste se a lista de motivos fosse copiada à mão.
	var esperadas []string
	for cat := materializer.ErrorCategory(0); cat < materializer.NumCategories; cat++ {
		if cat.BadEvent() {
			esperadas = append(esperadas, cat.String())
		}
	}
	if len(esperadas) != 6 {
		t.Fatalf("materializer.ErrorCategory.BadEvent() devolveu %d categoria(s), esperava 6 — o enum §101 mudou de tamanho, e este teste (e o comentário do Minor 3) precisam ser revistos junto", len(esperadas))
	}

	for _, motivo := range esperadas {
		valor, ok := vistas[motivo]
		if !ok {
			t.Errorf("motivo=%q não aparece em %s logo após a subida — a presença-em-zero foi perdida", motivo, metrics.MetIngestInvalidTotal)
			continue
		}
		if valor != 0 {
			t.Errorf("motivo=%q = %v, queria 0 (nenhum evento descartado ainda)", motivo, valor)
		}
	}
	if len(vistas) != len(esperadas) {
		t.Errorf("%s nasceu com %d série(s), esperava exatamente %d (nenhuma categoria a mais nem a menos)", metrics.MetIngestInvalidTotal, len(vistas), len(esperadas))
	}
}

// Cada série de espera sai da sua fonte, e cada nome sai UMA vez.
//
// A Fase 4 pôs os decodificadores entre a fila e o writer, e com isso a espera
// da FILA deixou de ser a do writer. Trocar as duas fontes não quebraria nada
// visível — as duas séries continuariam sendo emitidas, com números plausíveis
// —, e o painel passaria a ler ociosidade de K decodificadores como se fosse a
// de um writer. A tabela de decisão de docs/operacao-kafka.md compara `rate()`
// com 1: com oito decodificadores parados, o número certo é 8.
//
// Os valores são distintos e improváveis de propósito: um teste que instalasse
// as duas fontes com o mesmo número passaria com elas trocadas.
func TestCadaEsperaSaiDaSuaFonteEUmaVezSo(t *testing.T) {
	const (
		esperaDosDecodificadores = 11 * time.Second
		esperaDoWriter           = 23 * time.Second
	)
	m := registraMetricas(nil, "events")
	m.OlhaAFila(func() (int, int64) { return 0, 0 })
	m.OlhaOsTempos(func() (time.Duration, time.Duration) { return 0, esperaDosDecodificadores })
	m.OlhaAEsteira(func() int64 { return 0 }, func() time.Duration { return esperaDoWriter })

	valores := map[string][]float64{}
	for _, a := range append(m.amostrasDaFila(), m.amostrasDaEsteira()...) {
		valores[a.Nome] = append(valores[a.Nome], a.Valor)
	}

	for nome, quero := range map[string]float64{
		metrics.MetKafkaWriterIdleSeconds: esperaDoWriter.Seconds(),
		metrics.MetKafkaDecodeIdleSeconds: esperaDosDecodificadores.Seconds(),
	} {
		got, ok := valores[nome]
		if !ok {
			t.Errorf("%s não foi emitida", nome)
			continue
		}
		if len(got) != 1 {
			t.Errorf("%s foi emitida %d vezes; nome repetido é exposição malformada", nome, len(got))
			continue
		}
		if got[0] != quero {
			t.Errorf("%s = %v, quero %v — as duas fontes estão trocadas", nome, got[0], quero)
		}
	}
}

// O gauge de decodificadores nunca publica ZERO.
//
// A família materializa a série assim que ela é criada, e o Registry não
// desregistra: um dataset cujo consumidor não sobe — broker fora, assignment
// recusado, posição divergente — publicaria `decodeWorkers = 0` se o Set
// estivesse depois do primeiro caminho de erro. E zero não descreve estado
// nenhum: o parse do YAML RECUSA 0, então o valor é impossível por construção.
// Ele descreveria o gauge não ter sido escrito, que é a única coisa que o
// operador não pode adivinhar olhando o painel.
func TestOGaugeDeDecodificadoresNuncaSaiEmZero(t *testing.T) {
	// A prova é feita pelo Abre de verdade, e num caminho que FALHA cedo: sem
	// broker, ele não passa do primeiro contato. Se o Set estivesse depois desse
	// ponto, o gauge ficaria no zero com que a família nasce.
	cfg := &Config{
		Dataset: "events", Topic: "t", GroupID: "g",
		Brokers:        []string{"127.0.0.1:1"}, // recusa conexão
		MaxPollRecords: 32, QueueSize: 8, QueueBytes: 1 << 20,
		DecodeWorkers: 3,
	}
	reg := metrics.NewRegistry()
	ctx, cancela := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancela()
	if c, err := Abre(ctx, cfg, Opcoes{Store: storeGolden{}, Metrics: reg}); err == nil {
		c.Fecha()
		t.Skip("o Abre subiu: este teste precisa do caminho de falha para valer")
	}

	// Lê pela superfície de exposição, que é o que o operador vê.
	var buf bytes.Buffer
	if _, err := reg.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	var linha string
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(l, metrics.MetIngestDecodeWorkers+"{") {
			linha = l
		}
	}
	if linha == "" {
		t.Fatalf("%s não foi publicada", metrics.MetIngestDecodeWorkers)
	}
	if !strings.HasSuffix(linha, " 3") {
		t.Errorf("%s depois de um Abre que falhou: %q. Quero 3 — zero é impossível, "+
			"o parse do YAML recusa 0, e só significaria que o Set ficou atrás de um "+
			"caminho de erro", metrics.MetIngestDecodeWorkers, linha)
	}
}
