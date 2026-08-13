package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
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

Sai com código 1 se algum gate DURO for violado.

`)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "erro: -data é obrigatório (aponte para disco real)")
		flag.Usage()
		os.Exit(2)
	}

	rep, err := Run(Config{
		SpecPath:       *spec,
		DataDir:        *dataDir,
		Entities:       *entities,
		Seed:           *seed,
		SteadyDuration: *steady,
		SteadyRate:     *rate,
		Readers:        *readers,
		SQLReaders:     *sqlQrs,
		SampleInterval: *sample,
		SegmentSize:    *segment,
		CheckpointOn:   *ckpt,
		RSSLimitBytes:  *rssLimit,
		HeapBaseline:   *baseline,
		Out:            os.Stderr,
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
		status := "PASS"
		switch {
		case g.Skipped:
			status = "SKIP"
		case !g.Passed && g.Hard:
			status = "FAIL"
		case !g.Passed:
			status = "WARN"
		}
		fmt.Fprintf(os.Stderr, "%-6s %-22s %-6s %s\n", status, g.Name, g.Spec, g.Detail)
	}
	if !rep.Passed {
		fmt.Fprintln(os.Stderr, "\nrodada REPROVADA: gate duro violado")
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "\nrodada aprovada")
}
