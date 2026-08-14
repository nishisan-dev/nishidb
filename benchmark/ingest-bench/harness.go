// Command ingest-bench responde a pergunta central do M1: o dataset inteiro
// (8M entidades) cabe em menos de 1 GiB de RSS, com o heap desacoplado do
// tamanho dos dados?
//
// Três fases (design §5.2):
//
//   - LOAD: cria as N entidades com eventos completos, sem limite de taxa —
//     mede o throughput máximo de ingestão (§88: ≥ 2.000 ev/s) e o RSS
//     pós-carga;
//   - STEADY: carga de regime (85% de interim-update parcial) na taxa
//     configurada, com leituras concorrentes opcionais, amostrando memória;
//   - REPORT: JSON com séries temporais, agregados e o veredito dos gates.
//
// O binário sai com código ≠ 0 quando um gate DURO é violado OU fica sem
// avaliação: o harness existe para reprovar, não só para relatar. Um número
// fora do orçamento que só aparece num arquivo de relatório é um número que
// ninguém lê — e um gate que ninguém pôde avaliar não é um gate aprovado.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/memprof"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// Config parametriza a rodada.
type Config struct {
	SpecPath string // YAML da materialization definition
	DataDir  string // diretório do dataset (DISCO REAL — ver o requisito no main)
	Entities int    // entidades a criar na fase LOAD
	Seed     uint64

	SteadyDuration time.Duration // duração da fase STEADY
	SteadyRate     int           // eventos/s na fase STEADY (0 = sem limite)
	Readers        int           // goroutines de leitura de baixo nível no STEADY
	// SQLReaders são goroutines de consulta SQL concorrentes no STEADY, pelo
	// caminho completo (parse → bind → plan → executor → NDJSON). A §109 exige
	// o número de memória com ingestão E consultas; a rodada anterior mediu
	// com leitores de baixo nível porque o SQL ainda não existia.
	SQLReaders     int
	SampleInterval time.Duration // intervalo de amostragem de memória
	// SampleEveryEvents amostra também por CONTAGEM, não só por tempo
	// (0 → Entities/50). Sem isso a resolução da curva depende da duração da
	// rodada, e é justamente a curva que mostra se o crescimento é plano,
	// linear ou em DEGRAU (rehash da primary) — o que diz se a arquitetura
	// escala além do ponto medido. Um pico e um valor final não dizem.
	SampleEveryEvents int

	SegmentSize   int64  // tamanho do segmento (0 = default do Store)
	CheckpointOn  int    // eventos entre checkpoints (0 = nenhum)
	RSSLimitBytes uint64 // gate §109 (0 = desliga)
	HeapBaseline  string // report de uma rodada menor, para o gate §110

	Out io.Writer // destino do progresso (nil = silencioso)
}

// PhaseReport agrega uma fase.
type PhaseReport struct {
	Name        string  `json:"name"`
	Events      int64   `json:"events"`
	Created     int64   `json:"created"`
	Updated     int64   `json:"updated"`
	Stale       int64   `json:"stale"`
	Skipped     int64   `json:"skipped"`
	Failed      int64   `json:"failed"`
	DurationSec float64 `json:"duration_sec"`
	// IngestSec e CheckpointSec decompõem a duração. A separação existe
	// porque, ao throughput medido (~50k ev/s), o pipeline NÃO é o gargalo da
	// rodada — checkpoint e fsync são. Sem a decomposição, uma rodada que
	// estoure o orçamento de tempo exigiria repetir 3 minutos de ingestão só
	// para descobrir de que lado olhar.
	IngestSec       float64 `json:"ingest_sec"`
	CheckpointSec   float64 `json:"checkpoint_sec"`
	CheckpointCount int     `json:"checkpoint_count"`
	// EventsPerSec é sobre a duração TOTAL (o número que a operação sente).
	EventsPerSec float64 `json:"events_per_sec"`
	// IngestEventsPerSec exclui o tempo de checkpoint: é o teto do pipeline.
	IngestEventsPerSec float64 `json:"ingest_events_per_sec"`
	AllocsPerEven      float64 `json:"allocs_per_event"`
	MaxRSSBytes        uint64  `json:"max_rss_bytes"`
	EndRSSBytes        uint64  `json:"end_rss_bytes"`
	EndAnonBytes       uint64  `json:"end_anon_rss_bytes"`
	EndFileBytes       uint64  `json:"end_file_rss_bytes"`
	HeapStable         uint64  `json:"heap_stable_bytes"` // HeapAlloc após runtime.GC()
	GCPerMin           float64 `json:"gc_per_min"`
}

// QueryStats são as latências de uma classe de leitura.
type QueryStats struct {
	Class   string  `json:"class"`
	Count   int64   `json:"count"`
	P50Micr float64 `json:"p50_us"`
	P95Micr float64 `json:"p95_us"`
	P99Micr float64 `json:"p99_us"`
	MaxMicr float64 `json:"max_us"`
}

// GateStatus é o veredito de um gate, e tem TRÊS estados de propósito.
//
// Enquanto foram dois booleanos (`passed` + `skipped`), o artefato conseguia
// afirmar as duas coisas ao mesmo tempo — e afirmava: um gate DURO sem como
// ser avaliado saía com "skipped": true E "passed": true, e não reprovava a
// rodada. As rodadas de 1M em benchmark/baselines estão gravadas assim. Num
// relatório de aceite, "não medi" lido como "está bom" é a pior das leituras
// possíveis, e a representação não pode deixá-la existir.
type GateStatus string

const (
	// GatePassou: o gate foi avaliado e o critério vale.
	GatePassou GateStatus = "pass"
	// GateReprovou: o gate foi avaliado e o critério NÃO vale.
	GateReprovou GateStatus = "fail"
	// GateNaoAvaliado: faltou insumo para avaliar (o §110 sem baseline é o
	// caso). Não é aprovação: sendo o gate duro, a rodada não é aprovada.
	GateNaoAvaliado GateStatus = "skip"
)

// statusDe traduz o booleano de um critério já avaliado.
func statusDe(passou bool) GateStatus {
	if passou {
		return GatePassou
	}
	return GateReprovou
}

// GateResult é o veredito de um critério de aceite.
type GateResult struct {
	Name   string     `json:"name"`
	Spec   string     `json:"spec"`
	Status GateStatus `json:"status"`
	Hard   bool       `json:"hard"` // gate duro reprova a rodada (exit != 0)
	Detail string     `json:"detail"`
}

// aprovaRodada: a rodada só é aprovada quando TODO gate duro foi avaliado e
// passou. Gate soft entra no relatório para acompanhamento e não decide nada,
// violado ou não avaliado.
func aprovaRodada(gates []GateResult) bool {
	violados, naoAvaliados := gatesDurosPendentes(gates)
	return len(violados) == 0 && len(naoAvaliados) == 0
}

// gatesDurosPendentes separa os dois motivos de reprovação. Eles pedem ações
// diferentes do operador: violado é regressão para investigar, não avaliado é
// insumo que faltou na linha de comando.
func gatesDurosPendentes(gates []GateResult) (violados, naoAvaliados []string) {
	for _, g := range gates {
		if !g.Hard {
			continue
		}
		switch g.Status {
		case GateReprovou:
			violados = append(violados, g.Name)
		case GateNaoAvaliado:
			naoAvaliados = append(naoAvaliados, g.Name)
		}
	}
	return violados, naoAvaliados
}

// rotuloDoGate é o rótulo impresso no resumo. O gate duro NÃO AVALIADO tem
// rótulo próprio: reutilizar SKIP o faria parecer inofensivo como o soft, e
// reutilizar FAIL esconderia que ninguém mediu nada.
func rotuloDoGate(g GateResult) string {
	switch g.Status {
	case GatePassou:
		return "PASS"
	case GateNaoAvaliado:
		if g.Hard {
			return "SEM-AVAL"
		}
		return "SKIP"
	default:
		if g.Hard {
			return "FAIL"
		}
		return "WARN"
	}
}

// Report é o artefato final.
type Report struct {
	StartedAt  time.Time     `json:"started_at"`
	GoVersion  string        `json:"go_version"`
	GOMEMLIMIT string        `json:"gomemlimit"`
	GOGC       string        `json:"gogc"`
	Config     ConfigSummary `json:"config"`
	Load       PhaseReport   `json:"load"`
	Steady     PhaseReport   `json:"steady"`
	Queries    []QueryStats  `json:"queries,omitempty"`
	SQLErrors  int64         `json:"sql_errors"`
	// FullScanSec é a duração de UMA varredura completa do dataset (predicado
	// que não casa nada), medida fora do rodízio de consultas.
	FullScanSec   float64          `json:"full_scan_sec"`
	FullScanBytes int64            `json:"full_scan_bytes"`
	Samples       []Sample         `json:"samples"`
	Gates         []GateResult     `json:"gates"`
	StoreStats    StoreStatsReport `json:"store_stats"`
	// Passed é o veredito da RODADA: verdadeiro só quando todo gate duro foi
	// avaliado e passou. Um gate duro que ficou sem avaliação reprova a rodada
	// — inclusive a de referência do §110, que por construção não tem baseline
	// e portanto não prova o critério.
	Passed bool `json:"passed"`
}

// ConfigSummary registra no relatório o que produziu os números — sem isso,
// dois reports não são comparáveis.
type ConfigSummary struct {
	Entities       int    `json:"entities"`
	Seed           uint64 `json:"seed"`
	SteadySec      int    `json:"steady_sec"`
	SteadyRate     int    `json:"steady_rate"`
	Readers        int    `json:"readers"`
	SQLReaders     int    `json:"sql_readers"`
	SegmentSize    int64  `json:"segment_size"`
	CheckpointOn   int    `json:"checkpoint_every_events"`
	RSSLimitBytes  uint64 `json:"rss_limit_bytes"`
	CorpusGolden   string `json:"corpus_golden_note"`
	SpecPath       string `json:"spec_path"`
	DataDirPresent bool   `json:"data_dir_present"`
	// LoadCorpus e SteadyCorpus registram o PERFIL do corpus de cada fase no
	// próprio artefato. Sem isso, um corpus que empobrece em silêncio (uma
	// Config parcial, cujos ratios ficam zerados por serem literais) continua
	// produzindo números bonitos e um documento de aceite que cita evidência
	// inexistente — foi o que aconteceu com a primeira rodada de 8M, cujo LOAD
	// tinha zero patch enquanto o critério afirmava 85%.
	LoadCorpus   CorpusProfile `json:"load_corpus"`
	SteadyCorpus CorpusProfile `json:"steady_corpus"`
}

// CorpusProfile é o perfil do gerador de uma fase, como ele foi de fato usado.
type CorpusProfile struct {
	PatchRatio        float64 `json:"patch_ratio"`
	ExtendedJSONRatio float64 `json:"extended_json_ratio"`
	UnknownFieldRatio float64 `json:"unknown_field_ratio"`
	NullFieldRatio    float64 `json:"null_field_ratio"`
	Sequential        bool    `json:"sequential"`
}

func perfilDoCorpus(c radiusgen.Config) CorpusProfile {
	return CorpusProfile{
		PatchRatio:        c.PatchRatio,
		ExtendedJSONRatio: c.ExtendedJSONRatio,
		UnknownFieldRatio: c.UnknownFieldRatio,
		NullFieldRatio:    c.NullFieldRatio,
		Sequential:        c.Sequential,
	}
}

// StoreStatsReport é o estado final do dataset.
type StoreStatsReport struct {
	RowCount          uint64  `json:"row_count"`
	SegmentsTotal     uint32  `json:"segments_total"`
	PrimaryLoadFactor float64 `json:"primary_load_factor"`
	DiskBytes         int64   `json:"disk_bytes"`
}

// Sample é uma amostra da série temporal.
type Sample struct {
	ElapsedSec  float64 `json:"elapsed_sec"`
	Phase       string  `json:"phase"`
	Events      int64   `json:"events"`
	VmRSSBytes  uint64  `json:"vm_rss_bytes"`
	AnonBytes   uint64  `json:"anon_rss_bytes"`
	FileBytes   uint64  `json:"file_rss_bytes"`
	HeapAlloc   uint64  `json:"heap_alloc_bytes"`
	HeapObjects uint64  `json:"heap_objects"`
	NumGC       uint64  `json:"num_gc"`
}

const (
	gib = 1 << 30
	mib = 1 << 20
	// defaultRSSLimit é o orçamento da spec §109.
	defaultRSSLimit uint64 = 1 * gib
	// heapNegligibleBytes é o piso abaixo do qual a RAZÃO entre heaps deixa
	// de ser informativa.
	//
	// O §110 ("heap(8M) < 2× heap(1M)") pressupõe um heap grande o bastante
	// para a razão significar acoplamento. Medido aqui, o heap estável ficou
	// em ~1 MiB para 8M entidades — nessa escala, 500 KiB de variação normal
	// entre ciclos de GC vira "2,3x" e reprova uma rodada cujo heap é 0,1% do
	// orçamento de RSS. Abaixo deste piso, o desacoplamento está provado pelo
	// valor ABSOLUTO, e insistir na razão só produziria falso positivo.
	heapNegligibleBytes uint64 = 16 * mib
)

// Run executa a rodada completa e devolve o relatório. Erro só para falha
// operacional (dataset não abre, disco cheio); gate violado NÃO é erro — vira
// veredito no Report, que o caller transforma em exit code.
func Run(cfg Config) (*Report, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("ingest-bench: DataDir vazio")
	}
	if cfg.Entities <= 0 {
		return nil, fmt.Errorf("ingest-bench: Entities = %d, deve ser > 0", cfg.Entities)
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 5 * time.Second
	}
	if cfg.SampleEveryEvents <= 0 {
		// ~50 pontos na curva do LOAD, com um piso para não amostrar a cada
		// punhado de eventos em rodadas pequenas (o Read faz stop-the-world).
		cfg.SampleEveryEvents = max(cfg.Entities/50, 100)
	}
	if cfg.Out == nil {
		cfg.Out = io.Discard
	}

	spec, err := schema.Load(cfg.SpecPath, schema.DefaultsPadrao)
	if err != nil {
		return nil, fmt.Errorf("ingest-bench: carregando spec: %w", err)
	}
	var bitmapFields []string
	for name, f := range spec.Schema.Fields {
		if f.Index == "bitmap" {
			bitmapFields = append(bitmapFields, name)
		}
	}
	sort.Strings(bitmapFields) // ordem determinística na criação do dicionário

	st, err := storage.Open(storage.Options{
		Dir:              cfg.DataDir,
		SegmentSize:      cfg.SegmentSize,
		EstimatedRows:    uint64(cfg.Entities),
		BitmapFieldNames: bitmapFields,
	})
	if err != nil {
		return nil, fmt.Errorf("ingest-bench: abrindo dataset: %w", err)
	}
	defer st.Close()

	c, err := spec.Compile(st.FieldDict())
	if err != nil {
		return nil, fmt.Errorf("ingest-bench: compilando spec: %w", err)
	}
	m := materializer.New(c, st)

	rep := &Report{
		StartedAt:  time.Now(),
		GoVersion:  runtime.Version(),
		GOMEMLIMIT: os.Getenv("GOMEMLIMIT"),
		GOGC:       os.Getenv("GOGC"),
		Config: ConfigSummary{
			Entities: cfg.Entities, Seed: cfg.Seed,
			SteadySec: int(cfg.SteadyDuration.Seconds()), SteadyRate: cfg.SteadyRate,
			Readers: cfg.Readers, SQLReaders: cfg.SQLReaders, SegmentSize: cfg.SegmentSize,
			CheckpointOn: cfg.CheckpointOn, RSSLimitBytes: cfg.RSSLimitBytes,
			SpecPath:       cfg.SpecPath,
			DataDirPresent: true,
			CorpusGolden: "corpus determinístico: mesma Seed e Entities reproduzem os mesmos " +
				"eventos (ver TestCorpusGolden no radiusgen)",
		},
	}

	col := &collector{cfg: cfg, start: time.Now()}

	// FASE LOAD — o workload de REFERÊNCIA (patch parcial, Extended JSON, campo
	// desconhecido, null explícito), percorrendo as entidades em ordem para que
	// N eventos criem exatamente N rows.
	//
	// Partir de DefaultConfig() e sobrescrever, e nunca montar uma Config
	// literal: os ratios são campos literais, e uma Config parcial produz um
	// corpus SEM patch nenhum — verde, rápido, e sem exercitar o merge. Foi
	// assim que a primeira rodada de 8M mediu ingestão de eventos completos
	// enquanto o documento de aceite afirmava 85% de patch.
	loadCfg := radiusgen.DefaultConfig()
	loadCfg.Seed = cfg.Seed
	loadCfg.Entities = cfg.Entities
	loadCfg.Sequential = true
	rep.Config.LoadCorpus = perfilDoCorpus(loadCfg)
	loadGen, err := radiusgen.New(loadCfg)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(cfg.Out, "LOAD: criando %d entidades...\n", cfg.Entities)
	rep.Load, err = runPhase(phaseArgs{
		name: "load", cfg: cfg, m: m, gen: loadGen, col: col,
		events: int64(cfg.Entities), rate: 0, st: st,
	})
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(cfg.Out, "LOAD: %d eventos em %.1fs (%.0f ev/s; ingestão %.1fs a %.0f ev/s, "+
		"%d checkpoints em %.1fs), RSS %s\n",
		rep.Load.Events, rep.Load.DurationSec, rep.Load.EventsPerSec,
		rep.Load.IngestSec, rep.Load.IngestEventsPerSec,
		rep.Load.CheckpointCount, rep.Load.CheckpointSec, humanBytes(rep.Load.EndRSSBytes))

	// FASE STEADY — carga de regime com leituras concorrentes.
	if cfg.SteadyDuration > 0 {
		steadyCfg := radiusgen.DefaultConfig()
		steadyCfg.Seed = cfg.Seed + 1
		steadyCfg.Entities = cfg.Entities
		rep.Config.SteadyCorpus = perfilDoCorpus(steadyCfg)
		steadyGen, err := radiusgen.New(steadyCfg)
		if err != nil {
			return nil, err
		}
		readers := startReaders(cfg, st, c)
		sqlReaders := startSQLReaders(cfg, st, c, amostraDeKeys(cfg))
		fmt.Fprintf(cfg.Out, "STEADY: %s a %d ev/s com %d leitores e %d consultas SQL...\n",
			cfg.SteadyDuration, cfg.SteadyRate, cfg.Readers, cfg.SQLReaders)
		rep.Steady, err = runPhase(phaseArgs{
			name: "steady", cfg: cfg, m: m, gen: steadyGen, col: col,
			duration: cfg.SteadyDuration, rate: cfg.SteadyRate, st: st,
		})
		if err != nil {
			readers.stop()
			sqlReaders.stop()
			return nil, err
		}
		rep.Queries = readers.stop()
		// A varredura completa é medida UMA vez, fora do rodízio: dentro dele
		// ela monopolizaria os leitores e deixaria as rotas dos gates sem
		// amostras.
		if dur, bytes, err := sqlReaders.MedeVarreduraCompleta(); err == nil && dur > 0 {
			rep.FullScanSec = dur.Seconds()
			rep.FullScanBytes = bytes
			fmt.Fprintf(cfg.Out, "STEADY: varredura completa em %s\n", dur.Round(time.Millisecond))
		}
		statsSQL, errosSQL := sqlReaders.stop()
		rep.Queries = append(rep.Queries, statsSQL...)
		rep.SQLErrors = errosSQL
		fmt.Fprintf(cfg.Out, "STEADY: %d eventos, RSS max %s, %d erros de query\n",
			rep.Steady.Events, humanBytes(rep.Steady.MaxRSSBytes), errosSQL)
	}

	rep.Samples = col.samples
	stats := st.Stats()
	rep.StoreStats = StoreStatsReport{
		RowCount:          stats.RowCount,
		SegmentsTotal:     stats.SegmentsTotal,
		PrimaryLoadFactor: stats.PrimaryLoadFactor,
		DiskBytes:         dirBytes(cfg.DataDir),
	}
	rep.Gates = evaluateGates(cfg, rep)
	rep.Passed = aprovaRodada(rep.Gates)
	return rep, nil
}

type phaseArgs struct {
	name     string
	cfg      Config
	m        *materializer.Materializer
	gen      *radiusgen.Generator
	col      *collector
	st       *storage.Store
	events   int64         // parar após N eventos (0 = usar duration)
	duration time.Duration // parar após o tempo (0 = usar events)
	rate     int           // eventos/s (0 = sem limite)
}

// runPhase roda uma fase, amostrando memória e contabilizando desfechos.
func runPhase(a phaseArgs) (PhaseReport, error) {
	rep := PhaseReport{Name: a.name}
	antes, err := memprof.Read()
	if err != nil {
		return rep, fmt.Errorf("ingest-bench: amostra inicial: %w", err)
	}

	var (
		buf       []byte
		início    = time.Now()
		fim       time.Time
		próxima   = início.Add(a.cfg.SampleInterval)
		limiter   *rateLimiter
		desdeCkpt int
	)
	if a.duration > 0 {
		fim = início.Add(a.duration)
	}
	if a.rate > 0 {
		limiter = newRateLimiter(a.rate)
	}
	// Ponto de origem da curva: sem ele, o primeiro dado já vem depois de
	// milhares de eventos e a inclinação inicial fica invisível.
	if err := a.col.sample(a.name, 0, &rep); err != nil {
		return rep, err
	}

	for i := int64(0); ; i++ {
		if a.events > 0 && i >= a.events {
			break
		}
		if !fim.IsZero() && time.Now().After(fim) {
			break
		}
		if limiter != nil {
			limiter.wait()
		}

		buf = a.gen.Next(buf[:0])
		r := a.m.Apply(buf)
		switch r.Action {
		case materializer.ActionCreated:
			rep.Created++
		case materializer.ActionUpdated, materializer.ActionReplaced:
			rep.Updated++
		case materializer.ActionStale:
			rep.Stale++
		case materializer.ActionSkipped:
			rep.Skipped++
		case materializer.ActionFailed:
			rep.Failed++
			// Writer em modo falho é terminal: insistir só produziria o mesmo
			// erro milhões de vezes e um relatório de lixo.
			if r.Fatal() {
				return rep, fmt.Errorf("ingest-bench: writer em modo falho após %d eventos: %w", i, r.Err)
			}
		}
		rep.Events++

		if a.cfg.CheckpointOn > 0 {
			desdeCkpt++
			if desdeCkpt >= a.cfg.CheckpointOn {
				desdeCkpt = 0
				ckptInício := time.Now()
				if err := a.st.Checkpoint(); err != nil {
					return rep, fmt.Errorf("ingest-bench: checkpoint: %w", err)
				}
				rep.CheckpointSec += time.Since(ckptInício).Seconds()
				rep.CheckpointCount++
			}
		}
		// Amostra por TEMPO ou por CONTAGEM, o que vier primeiro: o tempo
		// cobre a fase lenta (checkpoint, fsync), a contagem garante
		// resolução na curva independentemente de quanto a rodada durou.
		porTempo := time.Now().After(próxima)
		porEventos := rep.Events%int64(a.cfg.SampleEveryEvents) == 0
		if porTempo || porEventos {
			próxima = time.Now().Add(a.cfg.SampleInterval)
			if err := a.col.sample(a.name, rep.Events, &rep); err != nil {
				return rep, err
			}
		}
	}

	rep.DurationSec = time.Since(início).Seconds()
	rep.IngestSec = rep.DurationSec - rep.CheckpointSec
	if rep.DurationSec > 0 {
		rep.EventsPerSec = float64(rep.Events) / rep.DurationSec
	}
	if rep.IngestSec > 0 {
		rep.IngestEventsPerSec = float64(rep.Events) / rep.IngestSec
	}

	// Heap estável: mede DEPOIS de um GC forçado, senão o número é lixo
	// pendente e não estado vivo — é essa distinção que o gate §110 exige.
	runtime.GC()
	depois, err := memprof.Read()
	if err != nil {
		return rep, fmt.Errorf("ingest-bench: amostra final: %w", err)
	}
	rep.HeapStable = depois.HeapAlloc
	rep.EndRSSBytes = depois.VmRSSBytes
	rep.EndAnonBytes = depois.AnonRSSBytes
	rep.EndFileBytes = depois.FileRSSBytes
	if rep.MaxRSSBytes < depois.VmRSSBytes {
		rep.MaxRSSBytes = depois.VmRSSBytes
	}
	if rep.Events > 0 {
		// Inclui as alocações do próprio harness (gerador não aloca; o
		// memprof.Read de cada amostra aloca ~2, diluídas em milhões).
		rep.AllocsPerEven = float64(depois.Mallocs-antes.Mallocs) / float64(rep.Events)
	}
	if d := rep.DurationSec / 60; d > 0 {
		rep.GCPerMin = float64(depois.NumGC-antes.NumGC) / d
	}
	return rep, nil
}

// collector acumula a série temporal e o pico de RSS.
type collector struct {
	cfg     Config
	start   time.Time
	samples []Sample
}

func (c *collector) sample(phase string, events int64, rep *PhaseReport) error {
	s, err := memprof.Read()
	if err != nil {
		return fmt.Errorf("ingest-bench: amostra de memória: %w", err)
	}
	c.samples = append(c.samples, Sample{
		ElapsedSec:  time.Since(c.start).Seconds(),
		Phase:       phase,
		Events:      events,
		VmRSSBytes:  s.VmRSSBytes,
		AnonBytes:   s.AnonRSSBytes,
		FileBytes:   s.FileRSSBytes,
		HeapAlloc:   s.HeapAlloc,
		HeapObjects: s.HeapObjects,
		NumGC:       s.NumGC,
	})
	if s.VmRSSBytes > rep.MaxRSSBytes {
		rep.MaxRSSBytes = s.VmRSSBytes
	}
	return nil
}

// rateLimiter é um ticker simples: o harness precisa de taxa aproximada, não
// de escalonamento preciso.
type rateLimiter struct {
	interval time.Duration
	próximo  time.Time
}

func newRateLimiter(perSec int) *rateLimiter {
	return &rateLimiter{
		interval: time.Duration(float64(time.Second) / float64(perSec)),
		próximo:  time.Now(),
	}
}

func (r *rateLimiter) wait() {
	r.próximo = r.próximo.Add(r.interval)
	if d := time.Until(r.próximo); d > 0 {
		time.Sleep(d)
	}
}

// readerPool roda leituras concorrentes com o writer durante o STEADY.
//
// Não são queries SQL (o executor é a Q15): são os TRÊS CAMINHOS de acesso
// que ele vai usar por baixo — point lookup, bitmap com limite e scan. O que
// se prova aqui é a promessa de concorrência do Store (leitores seguros com o
// writer ativo, §45/§46) sob carga real, que nenhum teste unitário exercita.
type readerPool struct {
	stopCh chan struct{}
	wg     sync.WaitGroup
	lat    [3]*latencyRec
}

var classNames = [3]string{"point_lookup", "bitmap_limit_100", "scan_1k"}

// amostraDeKeys devolve keys REAIS do dataset, tiradas do mesmo gerador
// determinístico da fase LOAD — é o que garante que o point lookup medido
// encontra row, em vez de medir o caminho de "não achou".
func amostraDeKeys(cfg Config) [][]byte {
	gen, err := radiusgen.New(radiusgen.Config{
		Seed: cfg.Seed + 2, Entities: cfg.Entities, Sequential: true,
	})
	if err != nil {
		return nil
	}
	keys := make([][]byte, 0, 1024)
	var buf []byte
	for i := 0; i < 1024 && i < cfg.Entities; i++ {
		buf = gen.Next(buf[:0])
		if k := extractKey(buf); k != nil {
			keys = append(keys, k)
		}
	}
	return keys
}

func startReaders(cfg Config, st *storage.Store, c *schema.Compiled) *readerPool {
	p := &readerPool{stopCh: make(chan struct{})}
	for i := range p.lat {
		p.lat[i] = newLatencyRec()
	}
	if cfg.Readers <= 0 {
		return p
	}
	statusField, ok := c.FieldByName([]byte("Acct-Status-Type"))
	if !ok {
		return p
	}
	gen, err := radiusgen.New(radiusgen.Config{Seed: cfg.Seed + 2, Entities: cfg.Entities, Sequential: true})
	if err != nil {
		return p
	}
	// As keys vêm do MESMO gerador determinístico: uma amostra do começo do
	// espaço de entidades, garantidamente presente no dataset.
	keys := make([][]byte, 0, 1024)
	var buf []byte
	for i := 0; i < 1024 && i < cfg.Entities; i++ {
		buf = gen.Next(buf[:0])
		if k := extractKey(buf); k != nil {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return p
	}

	for i := 0; i < cfg.Readers; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			var rbuf []byte
			n := id
			for {
				select {
				case <-p.stopCh:
					return
				default:
				}
				classe := n % 3
				início := time.Now()
				switch classe {
				case 0:
					_, out, _, _ := st.GetByKey(keys[n%len(keys)], rbuf[:0])
					rbuf = out
				case 1:
					rbuf = lookupBitmapLimit(st, statusField.ID, rbuf, 100)
				default:
					rbuf = scanLimit(st, rbuf, 1000)
				}
				p.lat[classe].add(time.Since(início))
				n++
			}
		}(i)
	}
	return p
}

func (p *readerPool) stop() []QueryStats {
	close(p.stopCh)
	p.wg.Wait()
	var out []QueryStats
	for i, rec := range p.lat {
		if s, ok := rec.stats(classNames[i]); ok {
			out = append(out, s)
		}
	}
	return out
}

func lookupBitmapLimit(st *storage.Store, field types.FieldID, buf []byte, limit int) []byte {
	v := types.Value{Type: types.TypeString, Bytes: []byte("Interim-Update")}
	it, err := st.LookupBitmap(field, v)
	if err != nil {
		return buf
	}
	defer it.Close()
	n := 0
	for n < limit && it.Next() {
		_, out, ok, err := st.GetByRowID(it.RowID(), buf[:0])
		buf = out
		if err == nil && ok {
			n++
		}
	}
	return buf
}

func scanLimit(st *storage.Store, buf []byte, limit int) []byte {
	it := st.ScanLive()
	defer it.Close()
	n := 0
	for n < limit && it.Next() {
		_, out, ok, err := st.GetByRowID(it.RowID(), buf[:0])
		buf = out
		if err == nil && ok {
			n++
		}
	}
	return buf
}

// latencyRec guarda uma amostra reservoir das latências: guardar tudo numa
// rodada de 10 minutos seria dezenas de MiB no processo que está medindo
// memória — o instrumento entraria no número.
type latencyRec struct {
	mu     sync.Mutex
	count  int64
	sample []time.Duration
	max    time.Duration
	rng    uint64
}

const latencyReservoir = 4096

func newLatencyRec() *latencyRec {
	return &latencyRec{sample: make([]time.Duration, 0, latencyReservoir), rng: 0x2545F4914F6CDD1D}
}

func (l *latencyRec) add(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.count++
	if d > l.max {
		l.max = d
	}
	if len(l.sample) < latencyReservoir {
		l.sample = append(l.sample, d)
		return
	}
	// xorshift: o reservoir precisa de aleatoriedade barata, não de qualidade
	// criptográfica.
	l.rng ^= l.rng << 13
	l.rng ^= l.rng >> 7
	l.rng ^= l.rng << 17
	if j := l.rng % uint64(l.count); j < latencyReservoir {
		l.sample[j] = d
	}
}

func (l *latencyRec) stats(class string) (QueryStats, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.count == 0 {
		return QueryStats{}, false
	}
	s := append([]time.Duration(nil), l.sample...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	pct := func(p float64) float64 {
		if len(s) == 0 {
			return 0
		}
		i := int(math.Ceil(p*float64(len(s)))) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(s) {
			i = len(s) - 1
		}
		return float64(s[i].Microseconds())
	}
	return QueryStats{
		Class: class, Count: l.count,
		P50Micr: pct(0.50), P95Micr: pct(0.95), P99Micr: pct(0.99),
		MaxMicr: float64(l.max.Microseconds()),
	}, true
}

// evaluateGates aplica os critérios de aceite. Gate DURO violado OU não
// avaliado reprova a rodada; gate soft entra no relatório para acompanhamento.
func evaluateGates(cfg Config, rep *Report) []GateResult {
	limite := cfg.RSSLimitBytes
	if limite == 0 {
		limite = defaultRSSLimit
	}
	maxRSS := rep.Load.MaxRSSBytes
	if rep.Steady.MaxRSSBytes > maxRSS {
		maxRSS = rep.Steady.MaxRSSBytes
	}

	gates := []GateResult{{
		Name: "rss_limit", Spec: "§109", Hard: true,
		Status: statusDe(maxRSS < limite),
		Detail: fmt.Sprintf("max(VmRSS) = %s, limite %s", humanBytes(maxRSS), humanBytes(limite)),
	}}

	// §110: o heap não pode acompanhar o dataset. Exige o report de uma
	// rodada MENOR como referência — sem ela o gate não tem o que comparar e
	// fica NÃO AVALIADO, que é estado próprio: não é aprovação, e por ser gate
	// duro derruba a rodada. A rodada de referência reprova por construção,
	// e é assim que tem de ser: ela não prova o §110 de ninguém, nem do
	// próprio volume que mediu.
	heapGate := GateResult{Name: "heap_decoupled", Spec: "§110", Hard: true}
	switch {
	case cfg.HeapBaseline == "":
		heapGate.Status = GateNaoAvaliado
		heapGate.Detail = "sem -heap-baseline: rode antes com menos entidades e passe o report"
	default:
		base, err := loadReport(cfg.HeapBaseline)
		switch {
		case err != nil:
			heapGate.Status = GateReprovou
			heapGate.Detail = fmt.Sprintf("lendo baseline %s: %v", cfg.HeapBaseline, err)
		case base.Config.Entities >= cfg.Entities:
			heapGate.Status = GateReprovou
			heapGate.Detail = fmt.Sprintf("baseline tem %d entidades, esta rodada %d: a referência precisa ser MENOR",
				base.Config.Entities, cfg.Entities)
		default:
			heapAtual := rep.Steady.HeapStable
			if heapAtual == 0 {
				heapAtual = rep.Load.HeapStable
			}
			heapBase := base.Steady.HeapStable
			if heapBase == 0 {
				heapBase = base.Load.HeapStable
			}
			var passouHeap bool
			passouHeap, heapGate.Detail = avaliaHeap(
				heapAtual, heapBase, cfg.Entities, base.Config.Entities)
			heapGate.Status = statusDe(passouHeap)
		}
	}
	gates = append(gates, heapGate)

	// §88: throughput de ingestão.
	gates = append(gates, GateResult{
		Name: "ingest_throughput", Spec: "§88", Hard: true,
		Status: statusDe(rep.Load.EventsPerSec >= 2000),
		Detail: fmt.Sprintf("LOAD a %.0f ev/s, mínimo 2000", rep.Load.EventsPerSec),
	})

	// §111: allocs/evento — soft no M1, vira gate na Fase 12 (design §5.2).
	//
	// O gate mede o LOAD, e não o STEADY, quando há consultas concorrentes: a
	// §111 é sobre o custo de INGERIR um evento, e o numerador do STEADY passa
	// a incluir as alocações das queries enquanto o denominador continua sendo
	// só os eventos ingeridos. Com 4 leitores SQL a 500 ev/s o número vira duas
	// ordens de grandeza maior e não fala mais de ingestão nenhuma.
	//
	// Os dois números aparecem no detalhe: o do LOAD é a ingestão pura, o do
	// STEADY é a carga mista — e a diferença entre eles é informação, não ruído.
	allocs, fase := rep.Load.AllocsPerEven, "load (ingestão pura)"
	misto := ""
	if rep.Steady.Events > 0 {
		if rep.Config.SQLReaders > 0 {
			misto = fmt.Sprintf("; steady com %d leitores SQL = %.2f (inclui as alocações das queries)",
				rep.Config.SQLReaders, rep.Steady.AllocsPerEven)
		} else {
			allocs, fase = rep.Steady.AllocsPerEven, "steady"
		}
	}
	gates = append(gates, GateResult{
		Name: "allocs_per_event", Spec: "§111", Hard: false,
		Status: statusDe(allocs <= 9),
		Detail: fmt.Sprintf("%.4f allocs/evento na fase %s (alvo 9; soft no M1)%s", allocs, fase, misto),
	})

	// Nenhum evento pode ser perdido em silêncio: descarte é sinal de corpus
	// ou pipeline quebrado, não de carga.
	perdidos := rep.Load.Failed + rep.Steady.Failed + rep.Load.Skipped + rep.Steady.Skipped
	if rep.Config.SQLReaders > 0 {
		gates = append(gates, GateResult{
			Name: "no_query_error", Spec: "§106", Hard: true,
			Status: statusDe(rep.SQLErrors == 0),
			Detail: fmt.Sprintf("%d erros nas consultas SQL concorrentes com a ingestão", rep.SQLErrors),
		})
	}
	gates = append(gates, GateResult{
		Name: "no_event_loss", Spec: "§101", Hard: true,
		Status: statusDe(perdidos == 0),
		Detail: fmt.Sprintf("%d eventos descartados (skipped+failed)", perdidos),
	})

	// Latências (§89/§90) — soft no M1: sem o executor, medem o storage.
	for _, q := range rep.Queries {
		var teto float64
		var spec string
		// As rotas SQL são DURAS: a §89 e a §90 falam da latência que o
		// usuário paga, e o caminho dele é o completo (parse → bind → plan →
		// executor → NDJSON). As de baixo nível continuam medidas, agora como
		// referência — a diferença entre as duas é o custo do front-end SQL.
		duro := false
		switch q.Class {
		case "sql_point_lookup":
			teto, spec, duro = 10_000, "§89", true
		case "sql_bitmap_limit_100":
			teto, spec, duro = 50_000, "§90", true
		case "sql_intersect_limit_100", "sql_union_limit_100", "sql_full_scan_limit_100":
			teto, spec = 50_000, "§90"
		case "point_lookup":
			teto, spec = 10_000, "§89 (baixo nível, referência)"
		case "bitmap_limit_100":
			teto, spec = 50_000, "§90 (baixo nível, referência)"
		default:
			continue
		}
		passou := teto == 0 || q.P95Micr <= teto
		gates = append(gates, GateResult{
			Name: "latency_" + q.Class, Spec: spec, Hard: duro,
			Status: statusDe(passou),
			Detail: fmt.Sprintf("p95 = %.0f µs, teto %.0f µs (%d amostras)", q.P95Micr, teto, q.Count),
		})
	}
	return gates
}

// avaliaHeap decide o §110 comparando o heap estável das duas rodadas.
//
// Dois regimes, e a distinção importa: enquanto o heap for grande o bastante
// para significar acoplamento, vale a razão da spec (teto 2x). Abaixo do piso
// ele deixa de ser sinal — o heap medido para 8M ficou em ~1 MiB, e nessa
// escala a variação normal entre ciclos de GC produz "2,3x" e reprovaria uma
// rodada cujo heap é 0,1% do orçamento de RSS. O que a §110 quer garantir é
// que o heap NÃO acompanhe o dataset, e o valor absoluto desprezível prova
// isso com mais força do que qualquer razão.
func avaliaHeap(heapAtual, heapBase uint64, entAtual, entBase int) (bool, string) {
	razãoDados := float64(entAtual) / float64(entBase)
	razão := math.Inf(1)
	if heapBase > 0 {
		razão = float64(heapAtual) / float64(heapBase)
	}
	// A razão e os valores absolutos entram no detalhe nos DOIS regimes,
	// inclusive quando o piso decide. Passar não pode significar esconder: um
	// bug que leve o heap de 850 KiB para 15 MiB (18x) passa corretamente
	// neste gate — 15 MiB é 1,5% do orçamento, e o RSS tem gate próprio —,
	// mas quem comparar duas rodadas precisa VER a regressão.
	medida := fmt.Sprintf("heap %s (%d entidades) vs %s (%d entidades): %.2fx de heap para %.1fx de dados",
		humanBytes(heapAtual), entAtual, humanBytes(heapBase), entBase, razão, razãoDados)

	if heapAtual <= heapNegligibleBytes {
		return true, fmt.Sprintf("%s — abaixo do piso de %s, onde a razão vira ruído de GC "+
			"e o desacoplamento está provado pela grandeza absoluta",
			medida, humanBytes(heapNegligibleBytes))
	}
	return razão < 2, medida + ", teto 2x"
}

func loadReport(path string) (*Report, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Report
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// extractKey lê o valor do campo "id" do evento sem parsear o JSON inteiro.
func extractKey(event []byte) []byte {
	const marca = `"id":"`
	i := indexOf(event, marca)
	if i < 0 {
		return nil
	}
	rest := event[i+len(marca):]
	for j := 0; j < len(rest); j++ {
		if rest[j] == '"' {
			return append([]byte(nil), rest[:j]...)
		}
	}
	return nil
}

func indexOf(haystack []byte, needle string) int {
	n := len(needle)
	for i := 0; i+n <= len(haystack); i++ {
		if string(haystack[i:i+n]) == needle {
			return i
		}
	}
	return -1
}

// dirBytes soma o tamanho aparente dos arquivos do dataset. Não é RSS: é o
// disco ocupado, e a distinção entre os dois é o ponto da spec §79.
func dirBytes(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		p := dir + string(os.PathSeparator) + e.Name()
		if e.IsDir() {
			total += dirBytes(p)
			continue
		}
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

func humanBytes(n uint64) string {
	switch {
	case n >= gib:
		return fmt.Sprintf("%.2f GiB", float64(n)/gib)
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
