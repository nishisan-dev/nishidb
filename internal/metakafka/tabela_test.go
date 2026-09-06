package metakafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/kafka"
	"github.com/nishisan-dev/nishidb/internal/metakafka"
	"github.com/nishisan-dev/nishidb/internal/protocol"
	"github.com/nishisan-dev/nishidb/internal/storage/checkpoint"
)

// TestMetaKafkaUmaLinhaPorParticao: offset e lag são por partição, então a
// tabela é uma linha por partição — e as colunas de dataset repetem em todas,
// para que "e a fila, está cheia?" não exija uma segunda consulta.
func TestMetaKafkaUmaLinhaPorParticao(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 100, 140), pos(1, 50, 50), pos(2, 7, 9))}
	eng := motorComKafka(t, fonteCom(c))

	rows := consulta(t, eng, "SELECT partition, appliedOffset, highWatermark, lag, topic, groupId, queueRecords FROM _meta.kafka ORDER BY partition")
	if len(rows) != 3 {
		t.Fatalf("%d linhas, quero 3: %v", len(rows), rows)
	}
	if got := coluna(rows, "partition"); got != "0,1,2" {
		t.Fatalf("partition = %s", got)
	}
	if got := coluna(rows, "appliedOffset"); got != "100,50,7" {
		t.Fatalf("appliedOffset = %s", got)
	}
	if got := coluna(rows, "lag"); got != "40,0,2" {
		t.Fatalf("lag = %s", got)
	}
	// As de dataset repetem.
	if got := coluna(rows, "topic"); got != "service-view,service-view,service-view" {
		t.Fatalf("topic = %s", got)
	}
	if got := coluna(rows, "queueRecords"); got != "3,3,3" {
		t.Fatalf("queueRecords = %s", got)
	}
	// O bootstrap sai canonizado — ordenado —, na mesma forma que a origem
	// grava no kafka.offsets. Duas grafias fariam a coluna discordar do
	// arquivo sem nada estar quebrado.
	if got := coluna(consulta(t, eng, "SELECT bootstrap FROM _meta.kafka LIMIT 1"), "bootstrap"); got != "kafka01:9092,kafka02:9092" {
		t.Fatalf("bootstrap = %q", got)
	}
}

// TestMetaKafkaLagFechaComOHighWatermark: em toda linha,
// lag == highWatermark - appliedOffset. É o invariante que só um instantâneo
// único sustenta.
func TestMetaKafkaLagFechaComOHighWatermark(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 100, 140), pos(1, 3, 900))}
	rows := consulta(t, motorComKafka(t, fonteCom(c)), "SELECT appliedOffset, highWatermark, lag FROM _meta.kafka")
	for _, l := range rows {
		off, hwm, lag := inteiro(t, l["appliedOffset"]), inteiro(t, l["highWatermark"]), inteiro(t, l["lag"])
		if hwm-off != lag {
			t.Fatalf("lag %d, mas hwm-off %d: as colunas deixaram de sair do mesmo instantâneo", lag, hwm-off)
		}
	}
}

// TestMetaKafkaSemKafkaRespondeUmaLinhaDeResumo: dataset que não declara
// Kafka responde UMA linha, nunca erro. "Não consome" é uma resposta.
func TestMetaKafkaSemKafkaRespondeUmaLinhaDeResumo(t *testing.T) {
	eng := motorComKafka(t, metakafka.Fonte{})
	rows := consulta(t, eng, "SELECT id, enabled, state, partition, topic, appliedOffset FROM _meta.kafka")
	if len(rows) != 1 {
		t.Fatalf("%d linhas, quero 1: %v", len(rows), rows)
	}
	l := rows[0]
	if l["id"] != metakafka.ChaveDoResumo {
		t.Fatalf("id = %q, quero %q", l["id"], metakafka.ChaveDoResumo)
	}
	if l["enabled"] != "false" {
		t.Fatalf("enabled = %q; sem bloco Kafka o dataset não consome, e isso é um fato", l["enabled"])
	}
	if l["state"] != protocol.EstadoDesligado {
		t.Fatalf("state = %q, quero %q", l["state"], protocol.EstadoDesligado)
	}
	// As de partição e as de identidade são ausência, não zero nem "".
	for _, col := range []string{"partition", "topic", "appliedOffset"} {
		if l[col] != "<null>" {
			t.Fatalf("%s = %q, quero NULL", col, l[col])
		}
	}
}

// TestMetaKafkaDesligadoTrazOTopicoDoYAML: com `enabled: false` a identidade
// do fluxo continua saindo — é o que permite conferir para onde o dataset
// apontaria se fosse religado.
func TestMetaKafkaDesligadoTrazOTopicoDoYAML(t *testing.T) {
	d := declarada()
	d.Habilitado = false
	eng := motorComKafka(t, metakafka.Fonte{Declarada: d})
	rows := consulta(t, eng, "SELECT id, enabled, state, topic, groupId FROM _meta.kafka")
	if len(rows) != 1 {
		t.Fatalf("%d linhas, quero 1", len(rows))
	}
	l := rows[0]
	if l["enabled"] != "false" || l["state"] != protocol.EstadoDesligado {
		t.Fatalf("enabled=%q state=%q", l["enabled"], l["state"])
	}
	if l["topic"] != "service-view" || l["groupId"] != "piloto-7" {
		t.Fatalf("topic=%q groupId=%q", l["topic"], l["groupId"])
	}
}

// TestMetaKafkaSemParticaoAtribuidaRespondeOResumo: consumidor de pé, sem
// partição ainda — a tabela responde a linha de resumo, e não vazio.
func TestMetaKafkaSemParticaoAtribuidaRespondeOResumo(t *testing.T) {
	c := &consumidorFalso{inst: kafka.Instantaneo{Desfecho: kafka.Desfecho{Eventos: 0}}}
	rows := consulta(t, motorComKafka(t, fonteCom(c)), "SELECT id, partition, events FROM _meta.kafka")
	if len(rows) != 1 || rows[0]["id"] != metakafka.ChaveDoResumo {
		t.Fatalf("quero uma linha de resumo: %v", rows)
	}
	if rows[0]["partition"] != "<null>" {
		t.Fatalf("partition = %q, quero NULL", rows[0]["partition"])
	}
	// Com consumidor, as colunas vivas saem — inclusive zeradas, que aqui é
	// medição.
	if rows[0]["events"] != "0" {
		t.Fatalf("events = %q, quero 0 medido", rows[0]["events"])
	}
}

// TestMetaKafkaComParticoesNaoTemLinhaDeResumo: o resumo existe SÓ quando não
// há partição. Com partições, ele seria uma linha a mais no resultado.
func TestMetaKafkaComParticoesNaoTemLinhaDeResumo(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1))}
	eng := motorComKafka(t, fonteCom(c))
	if rows := consulta(t, eng, "SELECT id FROM _meta.kafka"); len(rows) != 1 || rows[0]["id"] != "0" {
		t.Fatalf("quero só a linha da partição 0: %v", rows)
	}
	if rows := consulta(t, eng, "SELECT id FROM _meta.kafka WHERE id = 'source'"); len(rows) != 0 {
		t.Fatalf("com partições não pode haver resumo: %v", rows)
	}
}

// TestMetaKafkaColunasVivasSaoNulasSemConsumidor: sem processo de ingestão
// para observar, fila, desfecho e partições são AUSÊNCIA. Um zero ali diria
// "medi e deu zero" onde não houve medição.
func TestMetaKafkaColunasVivasSaoNulasSemConsumidor(t *testing.T) {
	eng := motorComKafka(t, metakafka.Fonte{Declarada: declarada()})
	vivas := []string{"queueRecords", "queueBytes", "events", "applied", "stale",
		"discarded", "failed", "partitionsAssigned", "partitionsTotal", "lastBatchAt"}
	rows := consulta(t, eng, "SELECT "+juntar(vivas)+" FROM _meta.kafka")
	if len(rows) != 1 {
		t.Fatalf("%d linhas, quero 1", len(rows))
	}
	for _, col := range vivas {
		if rows[0][col] != "<null>" {
			t.Fatalf("%s = %q sem consumidor; quero NULL", col, rows[0][col])
		}
	}
	// E o IS NULL do SQL enxerga a mesma coisa que o render.
	if rows := consulta(t, eng, "SELECT id FROM _meta.kafka WHERE queueRecords IS NULL"); len(rows) != 1 {
		t.Fatalf("IS NULL tinha de achar a linha: %v", rows)
	}
	if rows := consulta(t, eng, "SELECT id FROM _meta.kafka WHERE queueRecords = 0"); len(rows) != 0 {
		t.Fatalf("ausência não pode casar com zero: %v", rows)
	}
}

// TestMetaKafkaOmiteHighWatermarkVencido: marca vencida não vira lag zero. A
// posição APLICADA continua saindo — ela não depende do broker, e sumir com
// ela esconderia o progresso justamente quando o broker está fora do ar.
func TestMetaKafkaOmiteHighWatermarkVencido(t *testing.T) {
	c := &consumidorFalso{inst: kafka.Instantaneo{
		Particoes: []kafka.PosicaoDaParticao{{Particao: 0, Aplicado: 42}},
	}}
	rows := consulta(t, motorComKafka(t, fonteCom(c)),
		"SELECT appliedOffset, highWatermark, lag, markAt FROM _meta.kafka")
	if len(rows) != 1 {
		t.Fatalf("%d linhas, quero 1", len(rows))
	}
	if rows[0]["appliedOffset"] != "42" {
		t.Fatalf("a posição aplicada não depende do broker: %q", rows[0]["appliedOffset"])
	}
	for _, col := range []string{"highWatermark", "lag", "markAt"} {
		if rows[0][col] != "<null>" {
			t.Fatalf("%s = %q sem leitura do broker; quero NULL", col, rows[0][col])
		}
	}
}

// TestMetaKafkaOffsetDuravelVemDoCheckpoint: committedOffset é o do último
// checkpoint durável, que é o que o commit no group publica. Partição fora do
// arquivo é NULL, não zero.
func TestMetaKafkaOffsetDuravelVemDoCheckpoint(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 100, 100), pos(1, 20, 20))}
	f := fonteCom(c)
	f.Duraveis = duraveisCom(map[int32]int64{0: 90})
	rows := consulta(t, motorComKafka(t, f), "SELECT partition, appliedOffset, committedOffset FROM _meta.kafka ORDER BY partition")
	if got := coluna(rows, "committedOffset"); got != "90,<null>" {
		t.Fatalf("committedOffset = %s; a partição fora do arquivo é ausência, não zero", got)
	}
}

// TestMetaKafkaOffsetDuravelEhNuloNosDoisRamosDoTriEstado: "nunca consumiu" e
// "o arquivo está ilegível" não são um offset, e os dois viram NULL.
func TestMetaKafkaOffsetDuravelEhNuloNosDoisRamosDoTriEstado(t *testing.T) {
	casos := map[string]func() (checkpoint.SourceOffsets, bool, error){
		"nunca consumiu": func() (checkpoint.SourceOffsets, bool, error) {
			return checkpoint.SourceOffsets{}, false, nil
		},
		"arquivo ilegível": func() (checkpoint.SourceOffsets, bool, error) {
			return checkpoint.SourceOffsets{}, false, errors.New("crc inválido")
		},
	}
	for nome, duraveis := range casos {
		t.Run(nome, func(t *testing.T) {
			c := &consumidorFalso{inst: instantaneoCom(pos(0, 5, 5))}
			f := fonteCom(c)
			f.Duraveis = duraveis
			rows := consulta(t, motorComKafka(t, f), "SELECT committedOffset FROM _meta.kafka")
			if rows[0]["committedOffset"] != "<null>" {
				t.Fatalf("committedOffset = %q, quero NULL", rows[0]["committedOffset"])
			}
		})
	}
}

// TestMetaKafkaWatchdogEhNuloSemGoroutineDeIngestao: "não há watchdog" e
// "nunca recriou" são afirmações diferentes.
func TestMetaKafkaWatchdogEhNuloSemGoroutineDeIngestao(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1))}
	f := fonteCom(c)
	rows := consulta(t, motorComKafka(t, f), "SELECT watchdogTrips, watchdogGaveUp FROM _meta.kafka")
	if rows[0]["watchdogTrips"] != "<null>" || rows[0]["watchdogGaveUp"] != "<null>" {
		t.Fatalf("sem vigia as duas colunas são ausência: %v", rows[0])
	}

	f.Vigia = func() (uint64, bool, bool) { return 2, true, true }
	rows = consulta(t, motorComKafka(t, f), "SELECT watchdogTrips, watchdogGaveUp FROM _meta.kafka")
	if rows[0]["watchdogTrips"] != "2" || rows[0]["watchdogGaveUp"] != "true" {
		t.Fatalf("com vigia os números saem: %v", rows[0])
	}
}

// TestMetaKafkaEstadoVemDoClassificador: quando há classificador, é ELE que
// decide — nunca uma segunda leitura do YAML ao lado dele.
func TestMetaKafkaEstadoVemDoClassificador(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1))}
	f := fonteCom(c)
	f.Estado = func() (string, string) { return protocol.EstadoDegraded, "ingestão ainda subindo" }
	rows := consulta(t, motorComKafka(t, f), "SELECT state, detail FROM _meta.kafka")
	if rows[0]["state"] != protocol.EstadoDegraded || rows[0]["detail"] != "ingestão ainda subindo" {
		t.Fatalf("state=%q detail=%q", rows[0]["state"], rows[0]["detail"])
	}
}

// TestMetaKafkaSemClassificadorODeclaradoNaoInventaEstado: um processo que não
// observa a ingestão não pode dizer nem UP nem DOWN. Com `enabled: true` e sem
// classificador — o caminho do `nishidb query --config` — o estado é NULL.
func TestMetaKafkaSemClassificadorODeclaradoNaoInventaEstado(t *testing.T) {
	rows := consulta(t, motorComKafka(t, metakafka.Fonte{Declarada: declarada()}), "SELECT state FROM _meta.kafka")
	if rows[0]["state"] != "<null>" {
		t.Fatalf("state = %q; quero NULL, porque este processo não observa ingestão", rows[0]["state"])
	}
}

// TestMetaKafkaConsultaOConsumidorUmaVezPorConsulta: o retrato é colhido no
// AbreSnapshot, não por linha. Uma ida ao broker por LINHA seria carga
// multiplicada pelo número de partições.
func TestMetaKafkaConsultaOConsumidorUmaVezPorConsulta(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1), pos(1, 2, 2), pos(2, 3, 3))}
	eng := motorComKafka(t, fonteCom(c))
	if rows := consulta(t, eng, "SELECT partition, lag FROM _meta.kafka"); len(rows) != 3 {
		t.Fatalf("%d linhas, quero 3", len(rows))
	}
	if c.chamadas != 1 {
		t.Fatalf("%d idas ao consumidor para uma consulta de 3 linhas, quero 1", c.chamadas)
	}
}

// TestMetaKafkaMostraUmInstanteSo: o resultado inteiro é de UMA medição. Com
// leitura no presente, três partições poderiam carregar três markAt e um lag
// que não fecha com o HWM da mesma linha.
func TestMetaKafkaMostraUmInstanteSo(t *testing.T) {
	c := &consumidorMutante{
		instantes: []kafka.Instantaneo{
			instantaneoCom(pos(0, 10, 20), pos(1, 10, 20)),
			instantaneoCom(pos(0, 999, 999), pos(1, 999, 999)),
		},
	}
	eng := motorComKafka(t, fonteCom(c))
	rows := consulta(t, eng, "SELECT partition, appliedOffset, markAt FROM _meta.kafka ORDER BY partition")
	if got := coluna(rows, "appliedOffset"); got != "10,10" {
		t.Fatalf("appliedOffset = %s; o retrato mudou no meio do resultado", got)
	}
	if a, b := rows[0]["markAt"], rows[1]["markAt"]; a != b {
		t.Fatalf("markAt diferentes na mesma consulta: %q e %q", a, b)
	}
	// A consulta SEGUINTE vê o retrato novo: o congelamento é por consulta.
	rows = consulta(t, eng, "SELECT appliedOffset FROM _meta.kafka")
	if got := coluna(rows, "appliedOffset"); got != "999,999" {
		t.Fatalf("a segunda consulta tinha de ver o retrato novo: %s", got)
	}
}

// consumidorMutante devolve um retrato diferente a cada chamada: é o dublê que
// torna observável a diferença entre congelar e ler no presente.
type consumidorMutante struct {
	instantes []kafka.Instantaneo
	i         int
}

func (c *consumidorMutante) Instantaneo(context.Context) kafka.Instantaneo {
	inst := c.instantes[c.i]
	if c.i+1 < len(c.instantes) {
		c.i++
	}
	return inst
}

// TestMetaKafkaPointLookupAcompanhaOScan: para todo id que o scan devolve, o
// `WHERE id = '<id>'` devolve exatamente aquela linha. Point lookup que
// discordasse do scan seria falso negativo — proibido.
func TestMetaKafkaPointLookupAcompanhaOScan(t *testing.T) {
	casos := map[string]metakafka.Fonte{
		"com partições": fonteCom(&consumidorFalso{inst: instantaneoCom(pos(0, 1, 1), pos(3, 2, 2), pos(17, 3, 3))}),
		"só o resumo":   {Declarada: declarada()},
	}
	for nome, f := range casos {
		t.Run(nome, func(t *testing.T) {
			eng := motorComKafka(t, f)
			doScan := consulta(t, eng, "SELECT id, appliedOffset FROM _meta.kafka")
			if len(doScan) == 0 {
				t.Fatal("o scan não devolveu linha nenhuma")
			}
			for _, l := range doScan {
				achado := consulta(t, eng, "SELECT id, appliedOffset FROM _meta.kafka WHERE id = '"+l["id"]+"'")
				if len(achado) != 1 || achado[0]["id"] != l["id"] {
					t.Fatalf("point lookup de %q devolveu %v", l["id"], achado)
				}
			}
			// Uma chave que o scan não traz não pode aparecer.
			if rows := consulta(t, eng, "SELECT id FROM _meta.kafka WHERE id = '99'"); len(rows) != 0 {
				t.Fatalf("partição inexistente devolveu linha: %v", rows)
			}
		})
	}
}

// TestParticaoNovaEntraSemEmbaralharAsOutras: uma partição que nasce entre
// duas consultas entra na consulta seguinte, e cada linha continua carregando
// o offset da SUA partição.
//
// A ancoragem é o par (partition, appliedOffset), e não o rowID: os rowIDs
// valem dentro do retrato e nenhuma consulta os enxerga. Uma versão anterior
// mantinha um registro estável de partição → rowID entre consultas; ele foi
// removido porque não protegia nada observável — a mutação que o desfazia
// deixava a suíte verde, o que é o sinal de que o estado não pagava.
func TestParticaoNovaEntraSemEmbaralharAsOutras(t *testing.T) {
	c := &consumidorMutante{instantes: []kafka.Instantaneo{
		instantaneoCom(pos(3, 30, 30)),
		instantaneoCom(pos(1, 10, 10), pos(3, 30, 30)),
	}}
	eng := motorComKafka(t, fonteCom(c))
	// Primeira consulta: só a partição 3, que ganha o primeiro rowID.
	if rows := consulta(t, eng, "SELECT partition FROM _meta.kafka"); coluna(rows, "partition") != "3" {
		t.Fatalf("primeira consulta = %v", rows)
	}
	// A partição 1 nasce depois. O ORDER BY é do SQL; o que se afere aqui é
	// que a 3 continua respondendo, com o seu offset.
	rows := consulta(t, eng, "SELECT partition, appliedOffset FROM _meta.kafka ORDER BY partition")
	if got := coluna(rows, "partition"); got != "1,3" {
		t.Fatalf("partition = %s", got)
	}
	if got := coluna(rows, "appliedOffset"); got != "10,30" {
		t.Fatalf("appliedOffset = %s; a partição que nasceu depois embaralhou as linhas", got)
	}
}

// TestMetaKafkaNaoRetemRetratoDepoisDoClose: um retrato por consulta que não
// fosse devolvido ficaria preso pela vida do processo.
func TestMetaKafkaNaoRetemRetratoDepoisDoClose(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1))}
	f := fonteCom(c)
	sc, store, err := metakafka.Tabela(f)
	if err != nil {
		t.Fatal(err)
	}
	_ = sc
	for i := 0; i < 5; i++ {
		snap := store.AbreSnapshot()
		store.FechaSnapshot(snap)
	}
	if n := metakafka.RetratosAbertosForTest(store); n != 0 {
		t.Fatalf("%d retratos retidos depois de fechar todos", n)
	}
}

// TestMetaKafkaRecusaSnapshotDesconhecido: época que não está aberta é defeito
// de pareamento, e a leitura tem de ACUSAR. Devolver "não achei" faria a linha
// sumir do resultado sem nada aparecer.
func TestMetaKafkaRecusaSnapshotDesconhecido(t *testing.T) {
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1))}
	_, store, err := metakafka.Tabela(fonteCom(c))
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = store.GetByRowID(snapshotDeEpoca(999, 9), 2, nil)
	if err == nil {
		t.Fatal("época desconhecida tem de ser erro, não ausência de linha")
	}
}

// TestMetaKafkaRecusaColunaInexistenteComONomeQualificado: a mensagem do
// binder nomeia `_meta.kafka`, e não o nome interno do schema sintético.
func TestMetaKafkaRecusaColunaInexistenteComONomeQualificado(t *testing.T) {
	eng := motorComKafka(t, metakafka.Fonte{Declarada: declarada()})
	_, err := eng.Execute(context.Background(), dataset, "SELECT nao_existe FROM _meta.kafka", nil)
	if err == nil {
		t.Fatal("coluna inexistente tem de ser recusada")
	}
	if !contem(err.Error(), "_meta.kafka") {
		t.Fatalf("a recusa tem de nomear a tabela como o usuário a escreveu: %v", err)
	}
}

func TestMetaKafkaUltimoLoteSaiComoTimestamp(t *testing.T) {
	quando := time.Unix(1_757_000_001, 0)
	c := &consumidorFalso{inst: instantaneoCom(pos(0, 1, 1))}
	rows := consulta(t, motorComKafka(t, fonteCom(c)), "SELECT lastBatchAt FROM _meta.kafka")
	if rows[0]["lastBatchAt"] != inteiroTexto(quando.UnixMilli()) {
		t.Fatalf("lastBatchAt = %q", rows[0]["lastBatchAt"])
	}
}
