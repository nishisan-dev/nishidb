package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const specPath = "../../testdata/radius.materialization.yaml"

// A mecânica é testada em ESCALA PEQUENA e em TempDir: 300 entidades ocupam
// alguns MiB. A rodada de verdade (milhões de entidades, vários GiB) é
// operação manual com -data em disco real — ver o aviso no main.
func baseConfig(t *testing.T, entities int) Config {
	t.Helper()
	return Config{
		SpecPath: specPath,
		DataDir:  filepath.Join(t.TempDir(), "dataset"),
		Entities: entities,
		Seed:     7,
		// 4 MiB: o default de 512 MiB faria fallocate de meio giga por
		// segmento para gravar poucos MiB de teste.
		SegmentSize:    4 << 20,
		SampleInterval: 10 * time.Millisecond,
	}
}

// A fase LOAD precisa criar EXATAMENTE uma row por entidade — é o que dá
// sentido a "8M entidades" no relatório. Com sorteio uniforme seriam ~63%.
func TestLoadCriaUmaRowPorEntidade(t *testing.T) {
	const entidades = 300
	rep, err := Run(baseConfig(t, entidades))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Load.Events != entidades {
		t.Errorf("Load.Events = %d, want %d", rep.Load.Events, entidades)
	}
	if rep.Load.Created != entidades {
		t.Errorf("Load.Created = %d, want %d (o LOAD não pode reencontrar entidade)", rep.Load.Created, entidades)
	}
	if rep.Load.Updated != 0 || rep.Load.Stale != 0 {
		t.Errorf("LOAD com update/stale: %+v", rep.Load)
	}
	if rep.StoreStats.RowCount != entidades {
		t.Errorf("RowCount = %d, want %d", rep.StoreStats.RowCount, entidades)
	}
	if rep.Load.EventsPerSec <= 0 {
		t.Errorf("EventsPerSec = %f", rep.Load.EventsPerSec)
	}
	if rep.StoreStats.DiskBytes <= 0 {
		t.Error("DiskBytes = 0: o dataset não foi gravado")
	}
	// Nenhum evento pode ser descartado: o corpus é válido por construção.
	if rep.Load.Skipped != 0 || rep.Load.Failed != 0 {
		t.Errorf("eventos descartados no LOAD: %+v", rep.Load)
	}
}

// STEADY roda a carga de regime e produz série temporal e agregados.
func TestSteadyProduzAmostrasEAgregados(t *testing.T) {
	cfg := baseConfig(t, 500)
	cfg.SteadyDuration = 300 * time.Millisecond
	cfg.SteadyRate = 0 // sem limite: o teste não pode depender de relógio
	// Amostragem densa: numa rodada de teste o default (Entities/50, piso
	// 100) daria dois pontos, e a curva é o que se quer verificar aqui.
	cfg.SampleEveryEvents = 25

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Steady.Events == 0 {
		t.Fatal("STEADY não processou eventos")
	}
	// A carga de regime é dominada por patches sobre entidades existentes.
	if rep.Steady.Updated == 0 {
		t.Errorf("STEADY sem updates: %+v", rep.Steady)
	}
	if len(rep.Samples) == 0 {
		t.Fatal("nenhuma amostra de memória coletada")
	}
	// A curva precisa começar na origem e ter resolução por CONTAGEM, não só
	// por tempo: é ela que mostra se o crescimento é plano, linear ou em
	// degrau, e uma rodada curta não pode ficar com dois pontos.
	if rep.Samples[0].Events != 0 {
		t.Errorf("primeira amostra em %d eventos, want 0 (origem da curva)", rep.Samples[0].Events)
	}
	var noLoad int
	for _, s := range rep.Samples {
		if s.Phase == "load" {
			noLoad++
		}
	}
	if noLoad < 10 {
		t.Errorf("só %d pontos na curva do LOAD: resolução insuficiente para ver degrau", noLoad)
	}
	var viuSteady bool
	for _, s := range rep.Samples {
		if s.Phase == "steady" {
			viuSteady = true
		}
		if s.VmRSSBytes == 0 {
			t.Errorf("amostra com RSS zero: %+v", s)
		}
	}
	if !viuSteady {
		t.Error("nenhuma amostra rotulada como steady")
	}
	if rep.Steady.HeapStable == 0 {
		t.Error("HeapStable = 0: o heap é medido após GC forçado, não pode zerar")
	}
	if rep.Steady.MaxRSSBytes == 0 {
		t.Error("MaxRSSBytes = 0")
	}
}

// O rate limiter precisa segurar a taxa: sem isso o STEADY não representa
// carga de regime, vira outra fase LOAD.
func TestSteadyRespeitaRate(t *testing.T) {
	cfg := baseConfig(t, 100)
	cfg.SteadyDuration = 400 * time.Millisecond
	cfg.SteadyRate = 200 // ~80 eventos na janela

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Folga larga (o relógio do CI é ruidoso), mas o suficiente para acusar
	// um limiter inerte: sem ele seriam dezenas de milhares.
	if rep.Steady.Events > 400 {
		t.Errorf("STEADY processou %d eventos a 200 ev/s em 0,4s: o rate limiter não segurou",
			rep.Steady.Events)
	}
	if rep.Steady.Events == 0 {
		t.Error("STEADY não processou nada")
	}
}

// Leitores concorrentes com o writer: prova a promessa de concorrência do
// Store sob carga e produz as latências por classe.
func TestReadersConcorrentesProduzemLatencias(t *testing.T) {
	cfg := baseConfig(t, 300)
	cfg.SteadyDuration = 300 * time.Millisecond
	cfg.Readers = 3

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Queries) == 0 {
		t.Fatal("nenhuma estatística de leitura")
	}
	classes := map[string]bool{}
	for _, q := range rep.Queries {
		classes[q.Class] = true
		if q.Count == 0 {
			t.Errorf("classe %s sem amostras", q.Class)
		}
		if q.P50Micr > q.P99Micr {
			t.Errorf("classe %s: p50 (%.0f) > p99 (%.0f)", q.Class, q.P50Micr, q.P99Micr)
		}
	}
	for _, want := range []string{"point_lookup", "bitmap_limit_100", "scan_1k"} {
		if !classes[want] {
			t.Errorf("classe %q ausente do relatório", want)
		}
	}
}

// O gate de RSS é o critério §109. Com um teto absurdamente baixo ele precisa
// REPROVAR — um harness que só relata não serve para nada.
func TestGateRSSReprova(t *testing.T) {
	cfg := baseConfig(t, 100)
	cfg.RSSLimitBytes = 1 // impossível de cumprir

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Passed {
		t.Fatal("rodada aprovada com teto de RSS de 1 byte")
	}
	g := gateByName(t, rep, "rss_limit")
	if g.Status != GateReprovou || !g.Hard {
		t.Errorf("gate rss_limit = %+v", g)
	}
	if !strings.Contains(g.Detail, "limite") {
		t.Errorf("detalhe do gate não explica o limite: %q", g.Detail)
	}
}

// Sem baseline, o gate §110 fica NÃO AVALIADO em vez de passar por omissão —
// a diferença entre "não medi" e "está bom" é a que importa num relatório de
// aceite. Que isso também reprova a rodada está em
// TestGateDuroPuladoNaoEhGateAprovado.
func TestGateHeapSemBaselineFicaNaoAvaliado(t *testing.T) {
	rep, err := Run(baseConfig(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "heap_decoupled")
	if g.Status != GateNaoAvaliado {
		t.Errorf("gate heap sem baseline deveria ficar não avaliado: %+v", g)
	}
	if !strings.Contains(g.Detail, "heap-baseline") {
		t.Errorf("detalhe não instrui como habilitar: %q", g.Detail)
	}
}

// Com baseline de uma rodada MENOR, o gate compara os heaps.
func TestGateHeapComBaselineCompara(t *testing.T) {
	base, err := Run(baseConfig(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "base.json")
	writeReport(t, path, base)

	cfg := baseConfig(t, 400)
	cfg.HeapBaseline = path
	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "heap_decoupled")
	if g.Status == GateNaoAvaliado {
		t.Fatal("gate não avaliado mesmo com baseline")
	}
	// O detalhe precisa nomear as duas rodadas comparadas, seja pelo regime
	// da razão ou pelo do piso.
	if !strings.Contains(g.Detail, "100 entidades") || !strings.Contains(g.Detail, "400 entidades") {
		t.Errorf("detalhe não identifica as duas rodadas: %q", g.Detail)
	}
}

// Os dois regimes do §110 com valores sintéticos: a rodada real nunca produz
// heap grande o bastante para exercitar o caminho da razão, e é justamente ele
// que carrega o critério da spec.
func TestAvaliaHeapDoisRegimes(t *testing.T) {
	const mib = 1 << 20
	casos := []struct {
		nome                  string
		atual, base           uint64
		entAtual, entBase     int
		querPassar            bool
		trechoEsperadoNoTexto string
	}{
		{"desprezível encerra pelo valor", 1 * mib, 400 * 1024, 8_000_000, 1_000_000, true, "piso"},
		{"desprezível mesmo com razão alta", 15 * mib, 1 * mib, 8_000_000, 1_000_000, true, "piso"},
		// Passar não pode esconder: mesmo decidido pelo piso, o detalhe traz
		// a razão e os dois valores, senão uma regressão de 850 KiB para 15
		// MiB some do relatório de quem compara duas rodadas.
		{"piso ainda mostra a razão", 15 * mib, 1 * mib, 8_000_000, 1_000_000, true, "15.00x de heap"},
		{"piso ainda mostra os absolutos", 15 * mib, 1 * mib, 8_000_000, 1_000_000, true, "15.0 MiB"},
		{"acoplado reprova", 300 * mib, 100 * mib, 8_000_000, 1_000_000, false, "teto 2x"},
		{"desacoplado passa", 150 * mib, 100 * mib, 8_000_000, 1_000_000, true, "teto 2x"},
		{"exatamente no dobro reprova", 200 * mib, 100 * mib, 8_000_000, 1_000_000, false, "teto 2x"},
		{"base zerada não divide por zero", 100 * mib, 0, 8_000_000, 1_000_000, false, "teto 2x"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			passou, detalhe := avaliaHeap(tc.atual, tc.base, tc.entAtual, tc.entBase)
			if passou != tc.querPassar {
				t.Errorf("passou = %v, want %v (detalhe: %s)", passou, tc.querPassar, detalhe)
			}
			if !strings.Contains(detalhe, tc.trechoEsperadoNoTexto) {
				t.Errorf("detalhe = %q, want conter %q", detalhe, tc.trechoEsperadoNoTexto)
			}
		})
	}
}

// Heap absoluto desprezível encerra o §110 pelo valor, não pela razão: com
// ~1 MiB de heap, a variação normal entre ciclos de GC vira "2,3x" e
// reprovaria uma rodada cujo heap é 0,1% do orçamento. Foi o que aconteceu
// numa rodada real de 8M, e é por isso que o piso existe.
func TestGateHeapPisoAbsolutoEncerraPelaGrandeza(t *testing.T) {
	base, err := Run(baseConfig(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	// Força a razão a estourar (heap atual 3x o da referência) mantendo os
	// dois valores muito abaixo do piso.
	base.Load.HeapStable = 300 * 1024
	base.Steady.HeapStable = 0
	path := filepath.Join(t.TempDir(), "base.json")
	writeReport(t, path, base)

	cfg := baseConfig(t, 400)
	cfg.HeapBaseline = path
	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	heapAtual := rep.Steady.HeapStable
	if heapAtual == 0 {
		heapAtual = rep.Load.HeapStable
	}
	if heapAtual > 16*1024*1024 {
		t.Skipf("heap de %d B acima do piso: o teste não exercita o caminho", heapAtual)
	}
	g := gateByName(t, rep, "heap_decoupled")
	if g.Status != GatePassou {
		t.Errorf("gate reprovou com heap desprezível: %+v", g)
	}
	if !strings.Contains(g.Detail, "piso") {
		t.Errorf("detalhe não explica por que a razão foi dispensada: %q", g.Detail)
	}
}

// Baseline MAIOR que a rodada atual não prova desacoplamento nenhum: o gate
// precisa reprovar em vez de comparar na direção errada.
func TestGateHeapRecusaBaselineMaior(t *testing.T) {
	base, err := Run(baseConfig(t, 400))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "base.json")
	writeReport(t, path, base)

	cfg := baseConfig(t, 100)
	cfg.HeapBaseline = path
	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "heap_decoupled")
	if g.Status != GateReprovou {
		t.Errorf("gate deveria REPROVAR com baseline maior que a rodada: %+v", g)
	}
	if !strings.Contains(g.Detail, "MENOR") {
		t.Errorf("detalhe não explica o problema: %q", g.Detail)
	}
	if rep.Passed {
		t.Error("rodada aprovada com gate duro violado")
	}
}

func TestGateHeapBaselineIlegivel(t *testing.T) {
	cfg := baseConfig(t, 100)
	cfg.HeapBaseline = filepath.Join(t.TempDir(), "nao-existe.json")
	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "heap_decoupled")
	if g.Status != GateReprovou {
		t.Errorf("baseline ilegível precisa REPROVAR, não passar nem ficar sem avaliação: %+v", g)
	}
	if !strings.Contains(g.Detail, "nao-existe.json") {
		t.Errorf("detalhe não nomeia o arquivo: %q", g.Detail)
	}
}

// Checkpoint periódico durante a ingestão não pode quebrar a rodada — é o
// caminho que a operação real usa.
func TestCheckpointDuranteIngestao(t *testing.T) {
	cfg := baseConfig(t, 300)
	cfg.CheckpointOn = 100

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.StoreStats.RowCount != 300 {
		t.Errorf("RowCount = %d, want 300", rep.StoreStats.RowCount)
	}
	// O manifest precisa existir depois dos checkpoints.
	manifest := filepath.Join(cfg.DataDir, "checkpoint")
	if _, err := os.Stat(manifest); err != nil {
		t.Errorf("diretório de checkpoint ausente: %v", err)
	}

	// O tempo de checkpoint sai SEPARADO do de ingestão: ao throughput
	// medido, o gargalo da rodada é o checkpoint, e descobrir isso não pode
	// exigir repetir a rodada inteira.
	if rep.Load.CheckpointCount != 3 {
		t.Errorf("CheckpointCount = %d, want 3 (300 eventos a cada 100)", rep.Load.CheckpointCount)
	}
	if rep.Load.CheckpointSec <= 0 {
		t.Error("CheckpointSec = 0 com checkpoints executados")
	}
	if rep.Load.IngestSec <= 0 || rep.Load.IngestSec >= rep.Load.DurationSec {
		t.Errorf("IngestSec = %f fora de (0, %f)", rep.Load.IngestSec, rep.Load.DurationSec)
	}
	if soma := rep.Load.IngestSec + rep.Load.CheckpointSec; abs(soma-rep.Load.DurationSec) > 1e-6 {
		t.Errorf("ingestão + checkpoint = %f, want %f", soma, rep.Load.DurationSec)
	}
	// Excluir o tempo de checkpoint só pode AUMENTAR a taxa.
	if rep.Load.IngestEventsPerSec <= rep.Load.EventsPerSec {
		t.Errorf("IngestEventsPerSec (%.0f) <= EventsPerSec (%.0f)",
			rep.Load.IngestEventsPerSec, rep.Load.EventsPerSec)
	}
}

// Sem checkpoint, a decomposição continua coerente: tudo é ingestão.
func TestSemCheckpointTudoEhIngestao(t *testing.T) {
	rep, err := Run(baseConfig(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Load.CheckpointCount != 0 || rep.Load.CheckpointSec != 0 {
		t.Errorf("checkpoint contabilizado sem estar ligado: %+v", rep.Load)
	}
	if abs(rep.Load.IngestSec-rep.Load.DurationSec) > 1e-6 {
		t.Errorf("IngestSec = %f, want %f", rep.Load.IngestSec, rep.Load.DurationSec)
	}
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// Config inválida falha ANTES de tocar em disco.
func TestConfigInvalida(t *testing.T) {
	casos := []struct {
		nome string
		cfg  Config
		msg  string
	}{
		{"sem data dir", Config{SpecPath: specPath, Entities: 10}, "DataDir"},
		{"entities zero", Config{SpecPath: specPath, DataDir: t.TempDir(), Entities: 0}, "Entities"},
		{"spec inexistente", Config{SpecPath: "nao-existe.yaml", DataDir: t.TempDir(), Entities: 10}, "spec"},
	}
	for _, tc := range casos {
		t.Run(tc.nome, func(t *testing.T) {
			_, err := Run(tc.cfg)
			if err == nil {
				t.Fatal("erro esperado")
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
		})
	}
}

// O relatório precisa ser JSON válido e carregar o que produziu os números —
// dois reports sem config registrada não são comparáveis.
func TestReportSerializaEIdentificaARodada(t *testing.T) {
	cfg := baseConfig(t, 150)
	cfg.SteadyDuration = 100 * time.Millisecond
	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	var voltou Report
	if err := json.Unmarshal(raw, &voltou); err != nil {
		t.Fatalf("relatório não faz round-trip: %v", err)
	}
	if voltou.Config.Entities != 150 || voltou.Config.Seed != cfg.Seed {
		t.Errorf("config no relatório = %+v", voltou.Config)
	}
	if voltou.GoVersion == "" {
		t.Error("GoVersion ausente")
	}
	if len(voltou.Gates) < 4 {
		t.Errorf("%d gates no relatório, esperado ao menos 4", len(voltou.Gates))
	}
	if voltou.StoreStats.RowCount != rep.StoreStats.RowCount {
		t.Error("StoreStats não sobreviveu ao round-trip")
	}
}

// extractKey é usada pelos leitores para achar as keys sem parsear o evento
// inteiro; um erro ali silenciaria as leituras (iterador vazio, latência
// fantasma de ~0).
func TestExtractKey(t *testing.T) {
	casos := []struct {
		event, want string
	}{
		{`{"id":"CMP-V0001PPCLP-013","x":1}`, "CMP-V0001PPCLP-013"},
		{`{"a":"b","id":"X-1","c":2}`, "X-1"},
		{`{"id":""}`, ""},
		{`{"sem":"id"}`, ""},
		{`{"id":`, ""},
	}
	for _, tc := range casos {
		got := extractKey([]byte(tc.event))
		if string(got) != tc.want {
			t.Errorf("extractKey(%s) = %q, want %q", tc.event, got, tc.want)
		}
	}
}

// TestGateDeDiscoReprovaCrescimentoLinear afere o gate contra a série que a
// AUSÊNCIA de compaction produz. Um gate que não reprova o append-only puro
// não está medindo nada — é o mesmo erro do oráculo do débito 6, um nível
// acima: lá o harness media latência e não conferia resultado nenhum; aqui ele
// mediria disco e nunca discordaria de uma curva que sobe para sempre.
//
// As duas séries são as do esboço da Task 11, e a ordem das asserções é a que
// importa: a que prova o gate é a PRIMEIRA. Um gate que só aprova plateau é
// satisfeito por `return GatePassou` — a reprovação do crescimento linear é a
// única metade que nenhuma implementação degenerada atende.
func TestGateDeDiscoReprovaCrescimentoLinear(t *testing.T) {
	linear := []int64{1e9, 2e9, 3e9, 4e9, 5e9, 6e9}
	if g := avaliaGateDeDisco(linear); g.Status != GateReprovou {
		t.Errorf("gate = %q para crescimento linear, queria %q: %s", g.Status, GateReprovou, g.Detail)
	}
	plateau := []int64{1e9, 1.8e9, 2.1e9, 2.15e9, 2.1e9, 2.2e9}
	if g := avaliaGateDeDisco(plateau); g.Status != GatePassou {
		t.Errorf("gate = %q para plateau, queria %q: %s", g.Status, GatePassou, g.Detail)
	}
}

// Falta de insumo é NÃO AVALIADO, e não aprovação: série curta demais para ter
// duas metades, e série cuja primeira metade não cresceu, não descrevem
// compaction devolvendo disco — descrevem uma janela que não mediu nada.
//
// A segunda família é a que engana: uma série toda plana É um plateau, e um
// gate ingênuo a aprovaria. Só que o plateau de um harness que não escreveu
// nada não prova compaction nenhuma — é a aprovação por omissão que a errata
// do M1 tirou de circulação.
func TestGateDeDiscoSemInsumoFicaNaoAvaliado(t *testing.T) {
	casos := map[string][]int64{
		"série vazia":                    nil,
		"amostras abaixo do mínimo":      {1e9, 1.1e9, 1.2e9, 1.25e9, 1.3e9},
		"primeira metade plana":          {1e9, 1e9, 1e9, 1e9, 1e9, 1e9},
		"primeira metade em queda":       {3e9, 2e9, 1e9, 1e9, 1e9, 1e9},
		"primeira metade plana com pico": {1e9, 1e9, 1e9, 1e9, 9e9, 9e9},
	}
	for nome, serie := range casos {
		g := avaliaGateDeDisco(serie)
		if g.Status != GateNaoAvaliado {
			t.Errorf("%s: Status = %q, want %q (%s)", nome, g.Status, GateNaoAvaliado, g.Detail)
		}
		if !g.Hard {
			t.Errorf("%s: o gate precisa ser DURO, senão não avaliado deixa de custar alguma coisa", nome)
		}
	}
}

// A razão é entre INCLINAÇÕES — crescimento por amostra —, e não entre os
// totais das duas metades. A diferença não é cosmética: com número ímpar de
// variações as metades têm tamanhos diferentes, e comparar somas brutas faria o
// veredito depender de quantas amostras caíram de cada lado, que é instrumento
// medindo a si mesmo.
//
// Nesta série a segunda metade cresce a 45% da inclinação da primeira, e isso
// reprova. Somando bruto daria 0,30 — abaixo do teto — e a mesma curva seria
// aprovada.
func TestGateDeDiscoNormalizaPorAmostra(t *testing.T) {
	serie := []int64{1e9, 1.4e9, 1.7e9, 2e9, 2.15e9, 2.3e9}
	g := avaliaGateDeDisco(serie)
	if g.Status != GateReprovou {
		t.Errorf("Status = %q, want %q: %s", g.Status, GateReprovou, g.Detail)
	}
	if !strings.Contains(g.Detail, "razão 0.450") {
		t.Errorf("o detalhe precisa publicar a razão POR AMOSTRA (0.450): %q", g.Detail)
	}
}

// A série é a do STEADY, e só dele. O LOAD cresce por construção — ele CRIA o
// dataset —, e deixá-lo entrar encheria a primeira metade da janela com um
// crescimento que a compaction não tem por que absorver, aprovando qualquer
// coisa por comparação.
func TestSerieDeDiscoIgnoraOLoad(t *testing.T) {
	amostras := []Sample{
		{Phase: "load", DiskBytes: 10},
		{Phase: "load", DiskBytes: 900},
		{Phase: "steady", DiskBytes: 30},
		{Phase: "steady", DiskBytes: 31},
	}
	got := serieDeDiscoDoSteady(amostras)
	if want := []int64{30, 31}; !slices.Equal(got, want) {
		t.Errorf("serieDeDiscoDoSteady = %v, want %v", got, want)
	}
}

// O gate precisa aparecer AVALIADO no relatório de uma rodada com STEADY,
// senão ele é prosa: a série existe, a janela tem as duas metades, e o veredito
// é pass ou fail — nunca "faltou insumo".
//
// ESCALA: 500 entidades e 300 ms de STEADY em TempDir. Nesta escala a
// compaction não chega a agir (o intervalo entre passadas é de 5 s), então o
// que este teste prova é o CAMINHO — amostragem, extração da série e veredito
// — e não o plateau. O plateau é a rodada de aceite da Task 12.
func TestGateDeDiscoEntraAvaliadoNaRodadaComSteady(t *testing.T) {
	cfg := baseConfig(t, 500)
	cfg.SteadyDuration = 300 * time.Millisecond
	cfg.SteadyRate = 0 // sem limite: o teste não pode depender de relógio
	cfg.SampleEveryEvents = 25

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "disk_plateau")
	if !g.Hard {
		t.Error("o gate de disco precisa ser DURO: é a prova de que a compaction devolve disco")
	}
	if g.Status == GateNaoAvaliado {
		t.Fatalf("gate NÃO AVALIADO numa rodada com STEADY: %s", g.Detail)
	}
	var amostrasSteady int
	for _, s := range rep.Samples {
		if s.Phase != "steady" {
			continue
		}
		amostrasSteady++
		if s.DiskBytes <= 0 {
			t.Fatalf("amostra do STEADY com disco %d: a série do gate sai daqui", s.DiskBytes)
		}
	}
	if amostrasSteady < amostrasMinimasDeDisco {
		t.Errorf("%d amostras no STEADY, mínimo %d para o gate ter o que ler",
			amostrasSteady, amostrasMinimasDeDisco)
	}
}

// Sem fase STEADY não há série, e aí o gate fica NÃO AVALIADO — que por ser
// DURO reprova a rodada. É a mesma regra do §110 sem baseline: "não medi" não
// pode ser lido como "está bom", e uma rodada rápida de regressão não prova
// que o disco estabiliza.
func TestGateDeDiscoSemSteadyReprovaPorNaoAvaliado(t *testing.T) {
	rep, err := Run(baseConfig(t, 100))
	if err != nil {
		t.Fatal(err)
	}
	g := gateByName(t, rep, "disk_plateau")
	if g.Status != GateNaoAvaliado {
		t.Errorf("Status = %q, want %q (%s)", g.Status, GateNaoAvaliado, g.Detail)
	}
	_, naoAvaliados := gatesDurosPendentes(rep.Gates)
	if !slices.Contains(naoAvaliados, "disk_plateau") {
		t.Errorf("o gate de disco não avaliado precisa entrar em gatesDurosPendentes: %v", naoAvaliados)
	}
	if rep.Passed {
		t.Error("rodada aprovada sem nunca ter medido o disco em regime")
	}
}

// dirBytes é o INSTRUMENTO do gate, e um instrumento que devolve número sem ter
// medido é o defeito do débito 6 na sua forma mais direta. Aqui a direção do
// erro agrava: zero por diretório ilegível é lido pela série como "o disco
// desabou", que é exatamente o formato de um plateau — o gate aprovaria a
// própria cegueira.
func TestDirBytesRecusaDevolverZeroQuandoNaoMediu(t *testing.T) {
	if n, err := dirBytes(filepath.Join(t.TempDir(), "nao-existe")); err == nil {
		t.Errorf("dirBytes de diretório inexistente devolveu %d sem erro", n)
	}
}

// A soma é recursiva e é sobre o tamanho APARENTE: é o disco que o dataset
// ocupa, e é a grandeza contra a qual o plateau é medido.
func TestDirBytesSomaSubdiretorios(t *testing.T) {
	raiz := t.TempDir()
	if err := os.WriteFile(filepath.Join(raiz, "a.dat"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(raiz, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.dat"), make([]byte, 23), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := dirBytes(raiz)
	if err != nil {
		t.Fatal(err)
	}
	if got != 123 {
		t.Errorf("dirBytes = %d, want 123", got)
	}
}

func gateByName(t *testing.T, rep *Report, name string) GateResult {
	t.Helper()
	for _, g := range rep.Gates {
		if g.Name == name {
			return g
		}
	}
	t.Fatalf("gate %q ausente do relatório", name)
	return GateResult{}
}

func writeReport(t *testing.T, path string, rep *Report) {
	t.Helper()
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}
