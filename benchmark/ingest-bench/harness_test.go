package main

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/buildinfo"
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
	var inLoad int
	for _, s := range rep.Samples {
		if s.Phase == "load" {
			inLoad++
		}
	}
	if inLoad < 10 {
		t.Errorf("só %d pontos na curva do LOAD: resolução insuficiente para ver degrau", inLoad)
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
		name                 string
		current, base        uint64
		entCurrent, entBase  int
		querPassar           bool
		trechoExpectedInText string
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
		t.Run(tc.name, func(t *testing.T) {
			passou, detalhe := evaluateHeap(tc.current, tc.base, tc.entCurrent, tc.entBase)
			if passou != tc.querPassar {
				t.Errorf("passou = %v, want %v (detalhe: %s)", passou, tc.querPassar, detalhe)
			}
			if !strings.Contains(detalhe, tc.trechoExpectedInText) {
				t.Errorf("detalhe = %q, want conter %q", detalhe, tc.trechoExpectedInText)
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
		name string
		cfg  Config
		msg  string
	}{
		{"sem data dir", Config{SpecPath: specPath, Entities: 10}, "DataDir"},
		{"entities zero", Config{SpecPath: specPath, DataDir: t.TempDir(), Entities: 0}, "Entities"},
		{"spec inexistente", Config{SpecPath: "nao-existe.yaml", DataDir: t.TempDir(), Entities: 10}, "spec"},
	}
	for _, tc := range casos {
		t.Run(tc.name, func(t *testing.T) {
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
	// nishidb_version identifica o COMMIT do engine que produziu o número —
	// sem ele, dois reports com o mesmo GoVersion mas engines diferentes (um
	// antes e outro depois de uma otimização) não são distinguíveis.
	if voltou.NishidbVersion != buildinfo.String() {
		t.Errorf("NishidbVersion = %q, quer %q (buildinfo.String())", voltou.NishidbVersion, buildinfo.String())
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
	if g := evaluateGateOfDisk(linear); g.Status != GateReprovou {
		t.Errorf("gate = %q para crescimento linear, queria %q: %s", g.Status, GateReprovou, g.Detail)
	}
	plateau := []int64{1e9, 1.8e9, 2.1e9, 2.15e9, 2.1e9, 2.2e9}
	if g := evaluateGateOfDisk(plateau); g.Status != GatePassou {
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
	for name, serie := range casos {
		g := evaluateGateOfDisk(serie)
		if g.Status != GateNaoAvaliado {
			t.Errorf("%s: Status = %q, want %q (%s)", name, g.Status, GateNaoAvaliado, g.Detail)
		}
		if !g.Hard {
			t.Errorf("%s: o gate precisa ser DURO, senão não avaliado deixa de custar alguma coisa", name)
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
	g := evaluateGateOfDisk(serie)
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
	samples := []Sample{
		{Phase: "load", DiskBytes: 10},
		{Phase: "load", DiskBytes: 900},
		{Phase: "steady", DiskBytes: 30},
		{Phase: "steady", DiskBytes: 31},
	}
	got := serieOfDiskOfSteady(samples)
	if want := []int64{30, 31}; !slices.Equal(got, want) {
		t.Errorf("serieOfDiskOfSteady = %v, want %v", got, want)
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
	var samplesSteady int
	for _, s := range rep.Samples {
		if s.Phase != "steady" {
			continue
		}
		samplesSteady++
		if s.DiskBytes <= 0 {
			t.Fatalf("amostra do STEADY com disco %d: a série do gate sai daqui", s.DiskBytes)
		}
	}
	if samplesSteady < samplesMinimasOfDisk {
		t.Errorf("%d amostras no STEADY, mínimo %d para o gate ter o que ler",
			samplesSteady, samplesMinimasOfDisk)
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

// A compaction do §53-55 é o MECANISMO que o gate de disco existe para aferir,
// e o oráculo dele não pode ser o TEXTO que a rodada imprime.
//
// A prova por mutação de que não pode: com a asserção anterior — a linha
// "COMPACTION: ligada" na saída —, remover a chamada de `st.StartCompaction`
// e MANTER o Fprintf deixava a suíte inteira verde, e o artefato publicava
// `compaction_min_live_ratio: 0.50` para uma rodada de append-only puro. É a
// família do débito 6 na sua forma mais cara, porque o que o instrumento
// afirmava sem medir era justamente o mecanismo que este harness existe para
// aferir. E é também a constraint 8 do plano: não se testa prosa.
//
// O que se cobra agora é ESTADO que só existe se a goroutine do §53-55 tiver
// subido — `Stats().CompactionRunning`, que o Run lê com o Store ainda aberto.
// Ele responde por dois desfechos diferentes e os dois interessam: ninguém
// ligou a compaction, e a goroutine ligou e MORREU no meio da rodada (o
// `compaction.parou`), caso em que o disco volta a crescer e o artefato, sem
// este campo, continuaria dizendo 0,50.
//
// As três asserções afirmam coisas diferentes e nenhuma cobre a outra: o campo
// do Report prova o ligamento, o campo do JSON prova que ele CHEGA ao artefato
// — é lá que a rodada de aceite é conferida, meses depois —, e a razão prova
// que o número publicado é o que a Config pediu.
//
// O limite, escrito porque a leitura é que pega e não a suíte: `true` prova
// que a goroutine subiu e chegou viva ao fim, não com que razão ela foi
// iniciada (Stats não expõe a política vigente), nem que ela tenha compactado
// coisa alguma. Quem responde a segunda pergunta é o
// TestCompactionEmDatasetReabertoDevolveDiscoDeVerdade.
//
// ESCALA: 300 entidades em TempDir, sem STEADY. Nesta escala o segmento 1 é o
// corrente do começo ao fim (300 records não enchem 4 MiB) e a decisão D6 nunca
// elege o corrente, então a passada imediata não tem candidato — os contadores
// de bytes ficam em zero por medida, e é por isso que o oráculo aqui é o
// medidor de atividade.
func TestCompactionLigadaSobeAGoroutineEChegaVivaAoFim(t *testing.T) {
	const razao = 0.50
	cfg := baseConfig(t, 300)
	cfg.CompactionRatio = razao

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.StoreStats.CompactionRunning {
		t.Error("compaction_running = false numa rodada com -compaction-ratio 0,50: " +
			"ou a goroutine do §53-55 não subiu, ou ela morreu no meio — nos dois casos ninguém devolveu disco")
	}
	art := compactionInArtefato(t, rep)
	if !art.Rodando {
		t.Error("o artefato publica compaction_running = false: quem lê o JSON meses depois não tem outra evidência")
	}
	if art.Ratio != razao {
		t.Errorf("compaction_min_live_ratio = %v no artefato, want %v: o JSON tem de dizer com que política a rodada mediu",
			art.Ratio, razao)
	}
}

// O controle negativo tem o mesmo direito ao registro, e pelo mesmo motivo: uma
// rodada com `-compaction-ratio 0` mede o append-only puro, e um artefato que
// não diga isso é indistinguível do artefato da rodada com compaction. Os dois
// campos precisam concordar com a Config nos DOIS sentidos — sem este teste,
// fixar `compaction_running` em true (ou o ratio em qualquer constante) passa
// despercebido.
func TestCompactionDesligadaRegistraQueNinguemEstavaDevolvendoDisco(t *testing.T) {
	cfg := baseConfig(t, 200)
	cfg.CompactionRatio = 0

	rep, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rep.StoreStats.CompactionRunning {
		t.Error("compaction_running = true com -compaction-ratio 0: o campo não está lendo o Store")
	}
	art := compactionInArtefato(t, rep)
	if art.Rodando {
		t.Error("o artefato do controle negativo publica compaction_running = true")
	}
	if art.Ratio != 0 {
		t.Errorf("compaction_min_live_ratio = %v no artefato, want 0", art.Ratio)
	}
	if art.Lidos != 0 || art.Escritos != 0 {
		t.Errorf("a rodada sem compaction publicou %d bytes lidos e %d escritos", art.Lidos, art.Escritos)
	}
}

// TestCompactionEmDatasetReabertoDevolveDiscoDeVerdade é o oráculo do TRABALHO,
// e ele fecha a metade que o medidor de atividade não alcança: uma goroutine
// que sobe, elege lista vazia e dorme para sempre também publica
// compaction_running true.
//
// A montagem é a única que produz trabalho dentro de uma rodada de suíte, e ela
// sai da mecânica, não de um sleep: a primeira passada é IMEDIATA
// (compaction.go), e o Checkpoint incondicional do Open reconstrói o segSize
// com o tamanho REAL de todos os segmentos do diretório. Um dataset REABERTO já
// nasce, portanto, com os segmentos selados da rodada anterior elegíveis —
// enquanto num dataset novo só existe o corrente, que a D6 nunca elege. É a
// mesma semeadura que o
// TestCandidatosElegeOSegmentoSemeadoPeloOpenSemCheckpointDoChamador fixa do
// lado do storage.
//
// A rodada de montagem roda com a compaction DESLIGADA, para que todo byte que
// a segunda mover seja trabalho dela. As três asserções de premissa existem
// porque sem segmento selado e sem row superada a segunda rodada não teria
// candidato, e a asserção final passaria por vacuidade em vez de por medida.
//
// ESCALA: 300 entidades e segmentos de 128 KiB em TempDir. Quem sela segmento
// aqui é o STEADY, não o LOAD — a versão anterior deste bloco afirmava o
// contrário, e o mecanismo a desmente. MEDIDO nesta mesma Config com
// SteadyDuration = 0: o LOAD termina com load.events = 300 e
// segments_total = 1, que é o CORRENTE, com 84.427 B dos 131.072 B do teto
// (header de 64 B mais os 300 records da carga, ~281 B cada). Rodado assim, o
// teste morre na primeira premissa — e morre certo, porque nenhum arquivo foi
// selado.
//
// O que os 128 KiB compram é ENCURTAR o STEADY necessário, não eliminar a
// dependência da vazão da máquina: com eles o primeiro selo custa da ordem de
// 130 a 170 eventos de regime (medido aqui: 127 eventos ainda dão 1 segmento,
// 180 já dão 2), contra os 4 MiB do baseConfig, onde os mesmos 200 ms rendem
// 5.196 eventos NESTA máquina e nenhum selo. A dependência continua de pé, é a
// duração abaixo que a paga, e é ela — não o SegmentSize — a alavanca de quem
// for calibrar este teste numa máquina mais lenta. Escrever o contrário é o
// mesmo defeito pelo qual a dica do -checkpoint-every foi reprovada: causa
// nomeada que o mecanismo desmente, e uma investigação inteira no lugar errado.
func TestCompactionEmDatasetReabertoDevolveDiscoDeVerdade(t *testing.T) {
	cfg := baseConfig(t, 300)
	cfg.SegmentSize = 128 << 10
	cfg.SteadyDuration = 200 * time.Millisecond
	// Sem limitador. A fase continua limitada pelo RELÓGIO da linha acima, e
	// portanto pela vazão: tirar o rate é o que dá o MÁXIMO de eventos dentro
	// da janela, não o que torna a contagem determinística. A margem que sobra
	// é medida, não postulada — quatro rodadas de 200 ms deram alguns milhares
	// de eventos e mais de dez segmentos, contra os ~150 do primeiro selo, e
	// aos 30 ms a premissa ainda passou nas quatro. As duas medidas
	// reproduziram numa segunda máquina; a FAIXA exata, não: lá as mesmas
	// quatro rodadas deram 2.349–2.789 eventos e 10–12 segmentos, abaixo do
	// piso que a primeira redação fixava. Por isso o número aqui é ordem de
	// grandeza e não intervalo — faixa medida numa máquina é amostra, não
	// propriedade do teste, e o que sustenta a margem é a distância até o selo
	// (dez segmentos contra um), que sobrevive às duas.
	//
	// O que esta prosa deliberadamente NÃO afirma é a frequência de reprovação
	// num prazo mais curto. A primeira redação dizia que aos 20 ms a premissa
	// "reprovou em três de quatro"; remedida, deu o oposto — três de quatro
	// PASSARAM. Frequência de falha entre rodadas é o número menos durável que
	// se pode fixar em comentário permanente, porque mede a carga da máquina e
	// não o mecanismo.
	// Quem quiser a fase independente de vazão tem de limitá-la por EVENTOS
	// (phaseArgs.events), que é mecanismo de produção e não está neste débito.
	cfg.SteadyRate = 0
	cfg.SampleEveryEvents = 200

	montagem, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if montagem.StoreStats.SegmentsTotal < 2 {
		t.Fatalf("a montagem terminou com %d segmento(s): sem um SELADO a D6 recusa todo candidato e a "+
			"rodada seguinte não teria o que compactar. Quem sela aqui é o STEADY: em %s esta máquina "+
			"emitiu %d eventos e não rodou o segmento — a alavanca é SteadyDuration, não SegmentSize",
			montagem.StoreStats.SegmentsTotal, cfg.SteadyDuration, montagem.Steady.Events)
	}
	if montagem.Steady.Updated == 0 {
		t.Fatal("a montagem não produziu update nenhum: sem row superada não há garbage para devolver")
	}
	if montagem.StoreStats.CompactionBytesRead != 0 {
		t.Fatalf("a montagem leu %d bytes de compaction com -compaction-ratio 0: o controle negativo não é controle",
			montagem.StoreStats.CompactionBytesRead)
	}

	// MESMO DataDir: é a reabertura que torna os segmentos selados elegíveis.
	reabertura := cfg
	reabertura.CompactionRatio = 0.50

	// Run() NÃO espera a primeira passada da compaction antes de colher o
	// StoreStats: ele lê o snapshot (linha ~567 do harness.go) logo depois do
	// STEADY e devolve, sem sincronizar com a goroutine de fundo que
	// StartCompaction subiu. A JANELA que essa goroutine tem para ganhar uma
	// fatia de CPU e terminar de varrer é só a duração real do LOAD mais os
	// 200ms do STEADY acima — e -race, que infla o overhead de sincronização
	// de 5 a 20x, num runner de DOIS núcleos pode esgotar essa janela inteira
	// sem o agendador jamais escalonar a goroutine. Não há data race nenhuma
	// no log: é contenção de agendador, não corrida de dados — foi assim que
	// a run 32599767555 reprovou -race na main num runner lento, embora o
	// MESMO commit tivesse passado -race na run do PR meia hora antes e
	// passe localmente com -race -count=3 em 12 vCPUs.
	//
	// O retry NÃO é "dar outra chance à MESMA goroutine parada": Run() fecha
	// o Store no seu defer, e Close chama ForCompaction, que cancela e
	// ESPERA a goroutine morrer (D7) antes de devolver. Cada tentativa sobe,
	// portanto, um Store NOVO com uma goroutine de compaction NOVA — e
	// reexecuta o LOAD e o STEADY inteiros por cima do que já existe no
	// DataDir. O dataset não fica parado entre tentativas, ele CRESCE: mais
	// eventos de LOAD tornam mais stale o que já estava nos segmentos
	// selados (cada update é uma escrita nova no corrente, não uma edição no
	// lugar), e mais um STEADY de 200ms pode selar segmento novo em cima. É
	// isso que faz o retry CONVERGIR e não só insistir: a cada volta há mais
	// candidato elegível e um live ratio mais baixo para a D6 escolher, e o
	// LOAD+STEADY de cada tentativa é o que dá à goroutine nova uma janela
	// de tempo real para ganhar sua fatia de CPU — não a sorte de repetir a
	// mesma corrida. Por isso o polling é por CONDIÇÃO (a compaction
	// realmente varreu e devolveu disco), com um prazo generoso, e não um
	// sleep fixo: numa máquina rápida a primeira tentativa já basta e o
	// prazo nunca é consultado; o custo só é pago quando o runner estiver de
	// fato lento.
	const prazoPrimeiraPass = 30 * time.Second
	limit := time.Now().Add(prazoPrimeiraPass)
	var rep *Report
	var tentativas int
	for {
		tentativas++
		var err error
		rep, err = Run(reabertura)
		if err != nil {
			t.Fatal(err)
		}
		if !rep.StoreStats.CompactionRunning {
			t.Fatal("compaction_running = false na reabertura: a goroutine não chegou viva ao fim")
		}
		trabalhou := rep.StoreStats.CompactionBytesRead > 0 &&
			rep.StoreStats.CompactionBytesWritten < rep.StoreStats.CompactionBytesRead
		if trabalhou {
			break
		}
		if time.Now().After(limit) {
			t.Logf("a compaction não fechou uma passada completa em %d tentativa(s) e %s de prazo; "+
				"seguindo para as asserções abaixo, que nomeiam o sintoma exato", tentativas, prazoPrimeiraPass)
			break
		}
	}
	if rep.StoreStats.CompactionBytesRead <= 0 {
		t.Errorf("compaction_bytes_read = %d após %d tentativa(s), na ÚLTIMA reabertura com %d segmentos e garbage de %d updates nela: "+
			"a compaction subiu e não varreu nada",
			rep.StoreStats.CompactionBytesRead, tentativas, rep.StoreStats.SegmentsTotal, rep.Steady.Updated)
	}
	// Escritos < lidos é a afirmação inteira do §53-55: o que a varredura leu
	// da origem e NÃO reescreveu é o disco que a aposentadoria devolveu. Iguais
	// seria uma cópia que não descartou nada.
	if rep.StoreStats.CompactionBytesWritten >= rep.StoreStats.CompactionBytesRead {
		t.Errorf("compaction_bytes_written = %d contra %d lidos: a varredura copiou tudo para frente e não devolveu disco",
			rep.StoreStats.CompactionBytesWritten, rep.StoreStats.CompactionBytesRead)
	}
	art := compactionInArtefato(t, rep)
	if art.Lidos != rep.StoreStats.CompactionBytesRead || art.Escritos != rep.StoreStats.CompactionBytesWritten {
		t.Errorf("o artefato publica (%d, %d) contra (%d, %d) do relatório: os contadores não chegam ao JSON",
			art.Lidos, art.Escritos, rep.StoreStats.CompactionBytesRead, rep.StoreStats.CompactionBytesWritten)
	}
}

// A faixa do §78 é [0, 1): 0 desliga, e >= 1 elegeria TODO segmento — a
// compaction passaria a reescrever o dataset inteiro a cada passada, que não é
// a política, é um laço de cópia. A validação existia sem oráculo nenhum;
// removê-la inteira deixava a suíte verde e a rodada de aceite entregue a um
// número que ninguém cobrou.
func TestCompactionRatioForaDaFaixaRecusaARodada(t *testing.T) {
	for _, ratio := range []float64{-0.5, -0.01, 1.0, 1.5} {
		cfg := baseConfig(t, 50)
		cfg.CompactionRatio = ratio
		_, err := Run(cfg)
		if err == nil {
			t.Errorf("Run aceitou CompactionRatio = %v, fora de [0, 1)", ratio)
			continue
		}
		if !strings.Contains(err.Error(), "CompactionRatio") {
			t.Errorf("CompactionRatio = %v: erro %q não nomeia o campo recusado", ratio, err)
		}
	}
}

// artefatoOfCompaction são os quatro campos com que o JSON responde "a
// compaction rodou nesta rodada, e com que política".
type artefatoOfCompaction struct {
	Ratio    float64
	Rodando  bool
	Lidos    int64
	Escritos int64
}

// compactionInArtefato lê esses campos do JSON, e não do struct em memória: o
// que a rodada de aceite deixa para trás é o arquivo, e um campo que não sai
// no arquivo não existe para quem for conferi-la depois.
//
// Ponteiros de propósito: assim a ausência de qualquer um deles (tag renomeada,
// campo removido) falha em vez de virar o zero-value — que é exatamente o valor
// que uma rodada SEM compaction publicaria, ou seja o mutante que mais
// interessa pegar.
func compactionInArtefato(t *testing.T, rep *Report) artefatoOfCompaction {
	t.Helper()
	raw, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	var artefato struct {
		Config struct {
			Ratio *float64 `json:"compaction_min_live_ratio"`
		} `json:"config"`
		StoreStats struct {
			Rodando  *bool  `json:"compaction_running"`
			Lidos    *int64 `json:"compaction_bytes_read"`
			Escritos *int64 `json:"compaction_bytes_written"`
		} `json:"store_stats"`
	}
	if err := json.Unmarshal(raw, &artefato); err != nil {
		t.Fatal(err)
	}
	if artefato.Config.Ratio == nil || artefato.StoreStats.Rodando == nil ||
		artefato.StoreStats.Lidos == nil || artefato.StoreStats.Escritos == nil {
		t.Fatalf("o artefato não publica os quatro campos de compaction: %s", raw)
	}
	return artefatoOfCompaction{
		Ratio:    *artefato.Config.Ratio,
		Rodando:  *artefato.StoreStats.Rodando,
		Lidos:    *artefato.StoreStats.Lidos,
		Escritos: *artefato.StoreStats.Escritos,
	}
}

// Uma entrada que o instrumento não CONSEGUIU medir tem de abortar a rodada, e
// não sumir da soma. O caminho coberto até aqui era o `ReadDir` falhando na
// raiz; o caminho por ENTRADA — o que o commit anterior afirma ter fechado —
// seguia sem oráculo, e trocar o `return` por um `continue` restaurava o
// engolimento antigo com a suíte verde.
//
// O diretório em 0444 é o que separa os dois erros no sistema de arquivos real:
// LEITURA sem BUSCA deixa o `getdents` listar a entrada e faz o `lstat` dela
// falhar com EACCES. É um erro de instrumento, não uma aposentadoria, e a
// diferença é a direção do estrago: engolir aqui DIMINUI a soma, e soma que
// diminui é o desenho que a série lê como "a compaction devolveu disco".
func TestDirBytesAbortaQuandoNaoConsegueMedirUmaEntrada(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignora o bit de busca do diretório: sem EACCES não há o que provar")
	}
	dir := filepath.Join(t.TempDir(), "sem-busca")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "seg-0.dat"), make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o444); err != nil {
		t.Fatal(err)
	}
	// Antes da limpeza do TempDir, que roda depois desta por ser LIFO.
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	n, err := dirBytes(dir)
	if err == nil {
		t.Fatalf("dirBytes devolveu %d para uma entrada que não conseguiu medir", n)
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("erro de permissão classificado como arquivo inexistente: %v", err)
	}
}

// TestDirBytesAbortaQuandoNaoConsegueMedirUmSubdiretorio fecha o quarto ramo
// por entrada — o erro NÃO-ENOENT vindo da RECURSÃO —, e é o único que a suíte
// não alcançava. Os testes ao lado cobrem o subdiretório que SUMIU (não conta, e
// está certo) e o ARQUIVO ilegível (aborta); trocar por `continue` o
// `return 0, err` da recursão deixava os dois verdes e restaurava o engolimento
// na direção mais cara: um subdiretório inteiro descontado da soma, sem uma
// linha de log.
//
// A direção do estrago é a que o próprio comentário do dirBytes nomeia, e é por
// ela que o ramo existe: engolir DIMINUI a soma, e soma que diminui é o desenho
// que a série do gate lê como "a compaction devolveu disco". O dataset com um
// subdiretório ilegível reprovaria de um jeito ou de outro — mas reprovaria por
// plateau APROVADO em cima de cegueira, não por instrumento que se recusou a
// medir, e os dois diagnósticos mandam o operador a lugares opostos.
//
// O 0o000 é o modo que faz o `os.ReadDir` da recursão falhar com EACCES: sem
// LEITURA e sem BUSCA a listagem não acontece, e o erro que sobe não é
// fs.ErrNotExist. E o arquivo vivo se chama "a-vivo.dat" para ordenar ANTES do
// subdiretório na listagem: assim a soma parcial já vale 77 quando a recursão
// falha, e a asserção do zero deixa de ser vácua.
//
// O `t.Skip` sob root é o preço de medir permissão de verdade, e ele deixaria o
// ramo sem oráculo em CI que rode como root. Não deixa: o caso "subdiretório
// que não é diretório" do TestSomaDasEntradasSoIgnoraOQueSumiu cobra a MESMA
// decisão por ENOTDIR, que independe de euid. Este teste é o que prova o ramo
// contra o sistema de arquivos real; aquele é o que continua cobrando sob root.
func TestDirBytesAbortaQuandoNaoConsegueMedirUmSubdiretorio(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignora os bits do diretório: sem EACCES não há o que provar")
	}
	raiz := t.TempDir()
	if err := os.WriteFile(filepath.Join(raiz, "a-vivo.dat"), make([]byte, 77), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(raiz, "z-sub-ilegivel")
	if err := os.Mkdir(sub, 0o000); err != nil {
		t.Fatal(err)
	}
	// Antes da limpeza do TempDir, que roda depois desta por ser LIFO.
	t.Cleanup(func() { _ = os.Chmod(sub, 0o755) })

	n, err := dirBytes(raiz)
	if err == nil {
		t.Fatalf("dirBytes devolveu %d com um subdiretório que não conseguiu medir", n)
	}
	if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("erro de permissão na recursão classificado como arquivo inexistente: %v", err)
	}
	if n != 0 {
		t.Errorf("soma parcial %d junto do erro do subdiretório — o chamador que ignorar o erro lê isso como disco medido", n)
	}
}

// TestAmostraAbortaAFaseQuandoNaoConsegueMedirODisco cobra a decisão que o
// comentário do collector.sample argumenta e que nenhum teste alcançava: a
// leitura de disco que falhou ABORTA, em vez de virar um ponto inventado na
// curva.
//
// Trocar aquele `return err` por um `disco = 0` deixava a suíte inteira verde,
// e é a pior mutação possível neste arquivo — a amostra inventada entra na
// série como uma queda a zero, e queda é o desenho que o gate lê como devolução
// de disco. O instrumento cego aprovaria a própria cegueira, que é o argumento
// que o comentário escreve e que até aqui ninguém cobrava.
//
// A asserção que carrega o teste é a SEGUNDA. Devolver erro é metade; a outra
// metade é a série NÃO ganhar o ponto, porque um sample que apendasse antes de
// devolver o erro deixaria o ponto inventado no relatório de quem ignorasse o
// retorno.
//
// A fase é chamada com duração de 1 ns de propósito: assim a MUTAÇÃO também tem
// desfecho limpo — o laço sai na primeira volta sem tocar no gerador, o
// runPhase retorna nil, e o teste falha por asserção em vez de por panic num
// campo que a montagem não preencheu.
func TestAmostraAbortaAFaseQuandoNaoConsegueMedirODisco(t *testing.T) {
	cfg := baseConfig(t, 10)
	cfg.DataDir = filepath.Join(t.TempDir(), "diretorio-nunca-criado")
	col := &collector{cfg: cfg, start: time.Now()}

	_, err := runPhase(phaseArgs{name: "steady", cfg: cfg, col: col, duration: time.Nanosecond})
	if err == nil {
		t.Fatal("a fase terminou sem erro com o disco do dataset ilegível: a rodada seguiria relatando o que não conseguiu medir")
	}
	if !strings.Contains(err.Error(), "diretorio-nunca-criado") {
		t.Errorf("o erro não nomeia o diretório que não pôde ser medido: %v", err)
	}
	if len(col.samples) != 0 {
		t.Errorf("%d amostra(s) coletada(s) apesar da falha de leitura (%+v): ponto inventado na curva é "+
			"exatamente o que o gate lê como plateau", len(col.samples), col.samples)
	}
}

// A exceção do arquivo que SUMIU é a única leniência do instrumento, e ela é
// deliberada: o `unlink` do segmento `.retired` roda concorrente com a
// varredura, e aqueles bytes de fato não estão mais no disco. Contá-los como
// erro reprovaria a compaction por ter funcionado.
//
// Os quatro casos são os quatro ramos por entrada, e a suíte não os alcançava
// por arquivo real — o ENOENT do `Info` só acontece se o arquivo desaparecer
// ENTRE o `ReadDir` e o `stat`, corrida que o teste não controla. Descrevendo
// as entradas, cada ramo responde: sumiu não conta, sumiu na recursão também
// não, o Info que falhou por outro motivo aborta, e a recursão que falhou por
// outro motivo aborta igual. A entrada viva ANTES da descrita faz dois
// trabalhos: impede um `return 0, nil` preguiçoso de passar nos dois primeiros
// casos, e nos dois que ABORTAM dá conteúdo à soma parcial — devolver 77 junto
// do erro entrega ao chamador um número que parece medida e não é.
func TestSomaDasEntradasSoIgnoraOQueSumiu(t *testing.T) {
	raiz := t.TempDir()
	if err := os.WriteFile(filepath.Join(raiz, "vivo.dat"), make([]byte, 77), 0o644); err != nil {
		t.Fatal(err)
	}
	reais, err := os.ReadDir(raiz)
	if err != nil {
		t.Fatal(err)
	}
	vivo := reais[0]

	casos := []struct {
		name    string
		entry   fs.DirEntry
		want    int64
		wantErr bool
	}{
		{
			name:  "arquivo aposentado entre o ReadDir e o stat",
			entry: entryDescrita{name: "seg-9.retired", errOfInfo: fs.ErrNotExist},
			want:  77,
		},
		{
			name:  "subdiretório que sumiu durante a recursão",
			entry: entryDescrita{name: "sub-que-sumiu", diretorio: true},
			want:  77,
		},
		{
			name:    "entrada ilegível: instrumento cego aborta",
			entry:   entryDescrita{name: "seg-10.dat", errOfInfo: fs.ErrPermission},
			wantErr: true,
		},
		{
			// O ramo NÃO-ENOENT da RECURSÃO, e aqui ele não depende de euid: a
			// entrada se descreve como diretório apontando para o ARQUIVO
			// vivo, então o os.ReadDir da recursão falha com ENOTDIR — que não
			// é fs.ErrNotExist e que root também leva. O
			// TestDirBytesAbortaQuandoNaoConsegueMedirUmSubdiretorio cobre o
			// mesmo ramo por permissão real e PULA sob root; sem este caso,
			// trocar aquele `return 0, err` por `continue` sobreviveria em CI
			// que rode como root, que é o normal em container.
			name:    "subdiretório que não é diretório: recursão aborta",
			entry:   entryDescrita{name: "vivo.dat", diretorio: true},
			wantErr: true,
		},
	}
	for _, tc := range casos {
		got, err := somaDasEntradas(raiz, []fs.DirEntry{vivo, tc.entry})
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: somaDasEntradas devolveu %d sem erro", tc.name, got)
			} else if got != 0 {
				t.Errorf("%s: soma parcial %d junto do erro — o chamador que ignorar o erro lê isso como disco medido", tc.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: somaDasEntradas = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// entryDescrita é uma entrada de diretório cujo destino o teste escolhe. Não
// é falsificação de medida: o que ela descreve é o estado que o sistema de
// arquivos entrega quando o arquivo some entre a listagem e o stat — o caso que
// a aposentadoria do §53-55 produz em toda passada, e que só a corrida real
// fabrica.
type entryDescrita struct {
	name      string
	diretorio bool
	errOfInfo error
}

func (e entryDescrita) Name() string { return e.name }
func (e entryDescrita) IsDir() bool  { return e.diretorio }

func (e entryDescrita) Type() fs.FileMode {
	if e.diretorio {
		return fs.ModeDir
	}
	return 0
}

func (e entryDescrita) Info() (fs.FileInfo, error) { return nil, e.errOfInfo }

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

// A PROVA POR MUTAÇÃO do débito 6: com o oráculo violado, a rodada tem de
// ficar VERMELHA.
//
// Antes deste gate, uma consulta que devolvesse zero linhas sem erro passava
// em no_query_error (não errou) e passava melhor ainda nos gates de latência,
// porque não devolver linha é mais rápido que devolver. Um bug que zerasse
// todo resultado deixava a rodada inteira verde — e otimização de performance
// é justamente o tipo de mudança que produz esse bug.
//
// A mutação é injetada no Report em vez de no executor de propósito: o que se
// está provando é que o GATE reprova, e um teste que dependesse de quebrar o
// executor mediria as duas coisas ao mesmo tempo.
func TestGateDoOraculoReprovaConsultaQueNaoDevolveLinha(t *testing.T) {
	rep := Report{
		Config:    ConfigSummary{SQLReaders: 2},
		SQLErrors: 0,
		Queries: []QueryStats{
			{Class: "sql_point_lookup", Count: 500, RowsExpected: 1, OracleFails: 0},
			{Class: "sql_bitmap_limit_100", Count: 500, RowsExpected: 100, OracleFails: 500},
		},
	}
	gates := evaluateGates(Config{SQLReaders: 2}, &rep)

	var achou bool
	for _, g := range gates {
		if g.Name != "query_oracle" {
			continue
		}
		achou = true
		if !g.Hard {
			t.Error("query_oracle não é gate DURO; um oráculo que não reprova é decoração")
		}
		if g.Status != GateReprovou {
			t.Errorf("query_oracle = %v com 500 violações; quer reprovado", g.Status)
		}
		if !strings.Contains(g.Detail, "sql_bitmap_limit_100") {
			t.Errorf("o detalhe não nomeia a rota que violou: %q", g.Detail)
		}
	}
	if !achou {
		t.Fatal("gate query_oracle não foi avaliado com leitores SQL ligados")
	}
}

// E o contrário: sem violação, o gate passa e diz por quê.
func TestGateDoOraculoPassaQuandoTodasDevolvemOEsperado(t *testing.T) {
	rep := Report{
		Config:  ConfigSummary{SQLReaders: 2},
		Queries: []QueryStats{{Class: "sql_point_lookup", Count: 500, RowsExpected: 1}},
	}
	for _, g := range evaluateGates(Config{SQLReaders: 2}, &rep) {
		if g.Name == "query_oracle" && g.Status != GatePassou {
			t.Fatalf("query_oracle = %+v sem nenhuma violação", g)
		}
	}
}
