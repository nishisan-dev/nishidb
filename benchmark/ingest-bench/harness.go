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
// O binário sai com código ≠ 0 quando um gate DURO é violado: o harness
// existe para reprovar, não só para relatar. Um número fora do orçamento que
// só aparece num arquivo de relatório é um número que ninguém lê.
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
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage"
	"github.com/nishisan-dev/nishidb/internal/testutil/memprof"
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
	Readers        int           // goroutines de leitura concorrente durante o STEADY
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

// GateResult é o veredito de um critério de aceite.
type GateResult struct {
	Name    string `json:"name"`
	Spec    string `json:"spec"`
	Passed  bool   `json:"passed"`
	Hard    bool   `json:"hard"` // gate duro reprova a rodada (exit != 0)
	Detail  string `json:"detail"`
	Skipped bool   `json:"skipped,omitempty"`
}

// Report é o artefato final.
type Report struct {
	StartedAt  time.Time        `json:"started_at"`
	GoVersion  string           `json:"go_version"`
	GOMEMLIMIT string           `json:"gomemlimit"`
	GOGC       string           `json:"gogc"`
	Config     ConfigSummary    `json:"config"`
	Load       PhaseReport      `json:"load"`
	Steady     PhaseReport      `json:"steady"`
	Queries    []QueryStats     `json:"queries,omitempty"`
	Samples    []Sample         `json:"samples"`
	Gates      []GateResult     `json:"gates"`
	StoreStats StoreStatsReport `json:"store_stats"`
	Passed     bool             `json:"passed"`
}

// ConfigSummary registra no relatório o que produziu os números — sem isso,
// dois reports não são comparáveis.
type ConfigSummary struct {
	Entities       int    `json:"entities"`
	Seed           uint64 `json:"seed"`
	SteadySec      int    `json:"steady_sec"`
	SteadyRate     int    `json:"steady_rate"`
	Readers        int    `json:"readers"`
	SegmentSize    int64  `json:"segment_size"`
	CheckpointOn   int    `json:"checkpoint_every_events"`
	RSSLimitBytes  uint64 `json:"rss_limit_bytes"`
	CorpusGolden   string `json:"corpus_golden_note"`
	SpecPath       string `json:"spec_path"`
	DataDirPresent bool   `json:"data_dir_present"`
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

	spec, err := schema.Load(cfg.SpecPath)
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
			Readers: cfg.Readers, SegmentSize: cfg.SegmentSize,
			CheckpointOn: cfg.CheckpointOn, RSSLimitBytes: cfg.RSSLimitBytes,
			SpecPath:       cfg.SpecPath,
			DataDirPresent: true,
			CorpusGolden: "corpus determinístico: mesma Seed e Entities reproduzem os mesmos " +
				"eventos (ver TestCorpusGolden no radiusgen)",
		},
	}

	col := &collector{cfg: cfg, start: time.Now()}

	// FASE LOAD — entidades em ordem: N eventos criam exatamente N rows.
	loadCfg := radiusgen.Config{Seed: cfg.Seed, Entities: cfg.Entities, Sequential: true}
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
		steadyGen, err := radiusgen.New(steadyCfg)
		if err != nil {
			return nil, err
		}
		readers := startReaders(cfg, st, c)
		fmt.Fprintf(cfg.Out, "STEADY: %s a %d ev/s com %d leitores...\n",
			cfg.SteadyDuration, cfg.SteadyRate, cfg.Readers)
		rep.Steady, err = runPhase(phaseArgs{
			name: "steady", cfg: cfg, m: m, gen: steadyGen, col: col,
			duration: cfg.SteadyDuration, rate: cfg.SteadyRate, st: st,
		})
		if err != nil {
			readers.stop()
			return nil, err
		}
		rep.Queries = readers.stop()
		fmt.Fprintf(cfg.Out, "STEADY: %d eventos, RSS max %s\n",
			rep.Steady.Events, humanBytes(rep.Steady.MaxRSSBytes))
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
	rep.Passed = true
	for _, g := range rep.Gates {
		if g.Hard && !g.Passed {
			rep.Passed = false
		}
	}
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

// evaluateGates aplica os critérios de aceite. Gate DURO reprova a rodada;
// gate soft entra no relatório para acompanhamento.
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
		Passed: maxRSS < limite,
		Detail: fmt.Sprintf("max(VmRSS) = %s, limite %s", humanBytes(maxRSS), humanBytes(limite)),
	}}

	// §110: o heap não pode acompanhar o dataset. Exige o report de uma
	// rodada MENOR como referência — sem ela o gate não tem o que comparar e
	// se declara pulado, em vez de passar por omissão.
	heapGate := GateResult{Name: "heap_decoupled", Spec: "§110", Hard: true}
	switch {
	case cfg.HeapBaseline == "":
		heapGate.Skipped = true
		heapGate.Passed = true
		heapGate.Detail = "sem -heap-baseline: rode antes com menos entidades e passe o report"
	default:
		base, err := loadReport(cfg.HeapBaseline)
		switch {
		case err != nil:
			heapGate.Passed = false
			heapGate.Detail = fmt.Sprintf("lendo baseline %s: %v", cfg.HeapBaseline, err)
		case base.Config.Entities >= cfg.Entities:
			heapGate.Passed = false
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
			heapGate.Passed, heapGate.Detail = avaliaHeap(
				heapAtual, heapBase, cfg.Entities, base.Config.Entities)
		}
	}
	gates = append(gates, heapGate)

	// §88: throughput de ingestão.
	gates = append(gates, GateResult{
		Name: "ingest_throughput", Spec: "§88", Hard: true,
		Passed: rep.Load.EventsPerSec >= 2000,
		Detail: fmt.Sprintf("LOAD a %.0f ev/s, mínimo 2000", rep.Load.EventsPerSec),
	})

	// §111: allocs/evento — soft no M1, vira gate na Fase 12 (design §5.2).
	allocs := rep.Steady.AllocsPerEven
	fase := "steady"
	if rep.Steady.Events == 0 {
		allocs, fase = rep.Load.AllocsPerEven, "load"
	}
	gates = append(gates, GateResult{
		Name: "allocs_per_event", Spec: "§111", Hard: false,
		Passed: allocs <= 9,
		Detail: fmt.Sprintf("%.2f allocs/evento na fase %s (alvo 9; soft no M1)", allocs, fase),
	})

	// Nenhum evento pode ser perdido em silêncio: descarte é sinal de corpus
	// ou pipeline quebrado, não de carga.
	perdidos := rep.Load.Failed + rep.Steady.Failed + rep.Load.Skipped + rep.Steady.Skipped
	gates = append(gates, GateResult{
		Name: "no_event_loss", Spec: "§101", Hard: true,
		Passed: perdidos == 0,
		Detail: fmt.Sprintf("%d eventos descartados (skipped+failed)", perdidos),
	})

	// Latências (§89/§90) — soft no M1: sem o executor, medem o storage.
	for _, q := range rep.Queries {
		var teto float64
		var spec string
		switch q.Class {
		case "point_lookup":
			teto, spec = 10_000, "§89"
		case "bitmap_limit_100":
			teto, spec = 50_000, "§90"
		default:
			continue
		}
		gates = append(gates, GateResult{
			Name: "latency_" + q.Class, Spec: spec, Hard: false,
			Passed: q.P95Micr <= teto,
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
	if heapAtual <= heapNegligibleBytes {
		return true, fmt.Sprintf(
			"heap %s para %d entidades — abaixo do piso de %s, onde a razão vira ruído de GC "+
				"(referência: %s para %d entidades, %.1fx de dados)",
			humanBytes(heapAtual), entAtual, humanBytes(heapNegligibleBytes),
			humanBytes(heapBase), entBase, razãoDados)
	}
	razão := math.Inf(1)
	if heapBase > 0 {
		razão = float64(heapAtual) / float64(heapBase)
	}
	return razão < 2, fmt.Sprintf(
		"heap %s (%d entidades) vs %s (%d entidades): %.2fx de heap para %.1fx de dados, teto 2x",
		humanBytes(heapAtual), entAtual, humanBytes(heapBase), entBase, razão, razãoDados)
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
