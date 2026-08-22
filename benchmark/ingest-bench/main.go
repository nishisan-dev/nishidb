package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	var (
		spec     = flag.String("dataset", "testdata/radius.materialization.yaml", "materialization definition (YAML)")
		dataDir  = flag.String("data", "", "diretório do dataset — DISCO REAL, ver aviso abaixo")
		entities = flag.Int("entities", 1_000_000, "entidades a criar na fase LOAD")
		seed     = flag.Uint64("seed", 42, "semente do corpus (determinística)")
		steady   = flag.Duration("steady", 0, "duração da fase STEADY (0 = pular)")
		rate     = flag.Int("rate", 500, "eventos/s no STEADY (0 = sem limite)")
		readers  = flag.Int("queries", 0, "goroutines de leitura de baixo nível no STEADY")
		sqlQrs   = flag.Int("sql-queries", 0, "goroutines de consulta SQL concorrente no STEADY")
		sample   = flag.Duration("sample", 5*time.Second, "intervalo de amostragem de memória")
		segment  = flag.Int64("segment-size", 0, "tamanho do segmento em bytes (0 = default do Store)")
		// Checkpoint LIGADO por default: sem ele o overlay de locations fica
		// em heap até o primeiro checkpoint, e o heap passa a crescer com o
		// dataset (medido: 36 MiB para 1M rows, contra 2,7 MiB com
		// checkpoints). Rodar sem checkpoint mede um cenário que a operação
		// real não tem — e reprova o §110 por um motivo que não é da
		// arquitetura.
		ckpt     = flag.Int("checkpoint-every", 500_000, "eventos entre checkpoints (0 = nenhum)")
		rssLimit = flag.Uint64("rss-limit", 1<<30, "gate §109: teto de RSS em bytes")
		baseline = flag.String("heap-baseline", "", "report de uma rodada MENOR, para o gate §110")
		out      = flag.String("report", "", "arquivo de saída do relatório JSON (vazio = stdout)")

		// A compaction vem LIGADA, com o mesmo default do §78
		// (storage.compaction.minimumLiveRatio). É a operação real: um YAML sem
		// a chave já sobe com ela ligada, e uma rodada de aceite que medisse o
		// disco com a compaction desligada estaria medindo outro produto.
		//
		// `-compaction-ratio 0` é o CONTROLE NEGATIVO do gate de disco: sem
		// ninguém devolvendo disco, a série do STEADY é a do append-only puro e
		// o gate tem de reprovar. É a rodada que prova que o gate mede alguma
		// coisa, e ela custa uma linha de comando.
		compRatio = flag.Float64("compaction-ratio", 0.50,
			"minimumLiveRatio da compaction do §53-55 (0 = desligada, controle negativo do gate de disco)")

		// Ingestão Kafka REAL. Sem -kafka a rodada usa o gerador em memória,
		// que é o modo rápido de regressão — e o que o M1 media, deixando o
		// custo do consumidor fora do orçamento publicado (§108, débito M2 #2).
		kafkaBrokers = flag.String("kafka", "", "brokers (host:porta[,host:porta]); vazio usa o gerador em memória")
		kafkaTopic   = flag.String("kafka-topic", "nishidb-bench", "tópico da rodada com ingestão Kafka real")
		kafkaGroup   = flag.String("kafka-group", "nishidb-bench", "consumer group da rodada")
		kafkaQueue   = flag.Int("kafka-queue", 5000, "capacidade da fila limitada, em registros")
		kafkaBytes   = flag.Int64("kafka-queue-bytes", 64<<20, "teto da fila limitada, em bytes")
		kafkaPoll    = flag.Int("kafka-max-poll", 1000, "registros por fetch")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `ingest-bench — rodada de aceite de memória do NishiDB (spec §108-111)

Responde: o dataset inteiro cabe no orçamento de RSS, com o heap desacoplado
do volume de dados?

⚠️  -data PRECISA apontar para DISCO REAL. A rodada de 8M grava vários GiB de
    segmentos e índices, com fallocate de verdade; em /tmp montado como tmpfs
    isso vira RAM e derruba a máquina — e ainda invalida a medição, porque o
    RSS passaria a incluir o próprio dataset.

Uso típico (gate §110 exige duas rodadas, a menor primeiro):

  GOMEMLIMIT=512MiB GOGC=100 ingest-bench -data /var/tmp/nb-1m \
      -entities 1000000 -steady 1m -report 1m.json
  GOMEMLIMIT=512MiB GOGC=100 ingest-bench -data /var/tmp/nb-8m \
      -entities 8000000 -steady 10m -queries 8 -heap-baseline 1m.json -report 8m.json

Sai com código 1 se algum gate DURO for violado OU ficar sem avaliação. A
primeira rodada, a de referência, reprova por construção: sem -heap-baseline
o §110 não tem o que comparar, e gate duro não avaliado não é gate aprovado.
Ela vale pelo report que produz, não pelo veredito. O aceite é a SEGUNDA.

O gate disk_plateau (§53-55) exige -steady: ele lê a curva de disco da fase de
REGIME, que é onde os updates sobre key set fixo geram garbage sem gerar row
nova. Sem STEADY não há série, o gate fica NÃO AVALIADO e a rodada reprova —
uma rodada só de LOAD não prova que o disco para de crescer.

E a janela precisa ser longa o bastante para o PRIMEIRO segmento selado
esgotar: como os updates sorteiam as keys, isso custa da ordem de
-entities × ln(rows por segmento) eventos, e até lá a curva é indistinguível do
append-only puro. Quem encurta essa espera é -entities, -rate e -steady;
-checkpoint-every não é o que decide. O artefato responde de que lado está o
problema — compaction_running e compaction_bytes_read em store_stats.

`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "erro: -data é obrigatório (aponte para disco real)")
		flag.Usage()
		os.Exit(2)
	}

	var brokers []string
	if *kafkaBrokers != "" {
		brokers = strings.Split(*kafkaBrokers, ",")
		for i := range brokers {
			brokers[i] = strings.TrimSpace(brokers[i])
		}
	}

	rep, err := Run(Config{
		SpecPath:        *spec,
		DataDir:         *dataDir,
		Entities:        *entities,
		Seed:            *seed,
		SteadyDuration:  *steady,
		SteadyRate:      *rate,
		Readers:         *readers,
		SQLReaders:      *sqlQrs,
		SampleInterval:  *sample,
		SegmentSize:     *segment,
		CheckpointOn:    *ckpt,
		RSSLimitBytes:   *rssLimit,
		KafkaBrokers:    brokers,
		KafkaTopic:      *kafkaTopic,
		KafkaGroup:      *kafkaGroup,
		KafkaQueueSize:  *kafkaQueue,
		KafkaQueueBytes: *kafkaBytes,
		KafkaMaxPoll:    *kafkaPoll,

		HeapBaseline:    *baseline,
		CompactionRatio: *compRatio,
		Out:             os.Stderr,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "erro: %v\n", err)
		os.Exit(2)
	}

	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "erro serializando relatório: %v\n", err)
		os.Exit(2)
	}
	if *out == "" {
		os.Stdout.Write(raw)
		fmt.Println()
	} else if err := os.WriteFile(*out, raw, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "erro gravando %s: %v\n", *out, err)
		os.Exit(2)
	}

	fmt.Fprintln(os.Stderr, "\n--- GATES ---")
	for _, g := range rep.Gates {
		fmt.Fprintf(os.Stderr, "%-8s %-22s %-6s %s\n", rotuloDoGate(g), g.Name, g.Spec, g.Detail)
	}
	// As duas reprovações saem separadas: violado manda investigar regressão,
	// NÃO AVALIADO manda repetir a rodada com o insumo que faltou. Juntá-las
	// numa frase só faria quem roda a referência do §110 procurar um gate
	// violado que não existe.
	violados, naoAvaliados := gatesDurosPendentes(rep.Gates)
	if len(violados) > 0 {
		fmt.Fprintf(os.Stderr, "\nrodada REPROVADA: gate duro violado: %s\n", strings.Join(violados, ", "))
	}
	if len(naoAvaliados) > 0 {
		fmt.Fprintf(os.Stderr, "\nrodada REPROVADA: gate duro NÃO AVALIADO: %s — "+
			"gate que ninguém pôde avaliar não é gate aprovado\n", strings.Join(naoAvaliados, ", "))
	}
	if !rep.Passed {
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "\nrodada aprovada")
}
