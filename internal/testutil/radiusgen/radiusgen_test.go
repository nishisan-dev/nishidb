package radiusgen_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/jsonp"
	"github.com/nishisan-dev/nishidb/internal/materializer"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/testutil/fakestorage"
	"github.com/nishisan-dev/nishidb/internal/testutil/radiusgen"
)

const specPath = "../../../testdata/radius.materialization.yaml"

func gen(t testing.TB, cfg radiusgen.Config) *radiusgen.Generator {
	t.Helper()
	g, err := radiusgen.New(cfg)
	if err != nil {
		t.Fatalf("New(%+v): %v", cfg, err)
	}
	return g
}

// collect gera n eventos, cada um em seu próprio slice (cópia — o Next
// devolve uma janela sobre o buffer do caller).
func collect(t testing.TB, g *radiusgen.Generator, n int) [][]byte {
	t.Helper()
	out := make([][]byte, 0, n)
	var buf []byte
	for i := 0; i < n; i++ {
		buf = g.Next(buf[:0])
		out = append(out, bytes.Clone(buf))
	}
	return out
}

// Mesma Config ⇒ corpus idêntico byte a byte. Sem isto, uma regressão de
// memória medida entre duas rodadas do harness não seria comparável: os
// eventos teriam mudado junto.
func TestDeterminismoByteAByte(t *testing.T) {
	cfg := radiusgen.Config{Seed: 42, Entities: 500}
	a := collect(t, gen(t, cfg), 2000)
	b := collect(t, gen(t, cfg), 2000)

	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Fatalf("evento %d diverge entre duas rodadas da mesma seed:\n a=%s\n b=%s", i, a[i], b[i])
		}
	}
}

// Golden do corpus: trava a sequência entre VERSÕES do gerador, não só entre
// duas rodadas do mesmo binário (que é tudo o que o teste acima prova).
//
// É o que dá sentido prático ao determinismo: comparar a medição de hoje com
// a da semana passada só vale se o corpus for o mesmo. Mexer na ordem de
// consumo do rng, num literal do evento ou nos pesos muda este hash — e aí a
// atualização é uma DECISÃO: as medições anteriores deixam de ser
// comparáveis e o número velho vira histórico, não baseline.
func TestCorpusGolden(t *testing.T) {
	cfg := radiusgen.DefaultConfig()
	cfg.Seed = 20260813
	cfg.Entities = 1000

	h := sha256.New()
	g := gen(t, cfg)
	var buf []byte
	for i := 0; i < 1000; i++ {
		buf = g.Next(buf[:0])
		h.Write(buf)
		h.Write([]byte{'\n'})
	}
	const golden = "d4e08e42ca123d838652a70feb8d66c2085e45a61a094fe47c825f77a2641b4e"
	if got := hex.EncodeToString(h.Sum(nil)); got != golden {
		t.Errorf("corpus mudou:\n got  %s\n want %s\n"+
			"Se a mudança é intencional, atualize o golden — e saiba que as "+
			"medições feitas com o corpus antigo deixam de ser comparáveis.", got, golden)
	}
}

func TestSeedsDiferentesGeramCorpusDiferente(t *testing.T) {
	a := collect(t, gen(t, radiusgen.Config{Seed: 1, Entities: 500}), 200)
	b := collect(t, gen(t, radiusgen.Config{Seed: 2, Entities: 500}), 200)

	identicalCount := 0
	for i := range a {
		if bytes.Equal(a[i], b[i]) {
			identicalCount++
		}
	}
	if identicalCount > len(a)/10 {
		t.Errorf("%d/%d eventos idênticos entre seeds diferentes — o rng não está sendo consumido", identicalCount, len(a))
	}
}

// Oráculo independente: o que o gerador produz precisa ser JSON válido para a
// stdlib, não só para o nosso tokenizer. Um gerador que emitisse escape
// inválido treinaria a engine contra uma entrada que o mundo real não produz.
func TestTodoEventoParseiaNosDoisParsers(t *testing.T) {
	g := gen(t, radiusgen.Config{Seed: 7, Entities: 1000, UnknownFieldRatio: 0.05, NullFieldRatio: 0.2})
	var buf []byte
	for i := 0; i < 5000; i++ {
		buf = g.Next(buf[:0])

		var m map[string]any
		if err := json.Unmarshal(buf, &m); err != nil {
			t.Fatalf("evento %d inválido para encoding/json: %v\n%s", i, err, buf)
		}
		if len(m) == 0 {
			t.Fatalf("evento %d vazio", i)
		}

		var p jsonp.Parser
		p.Reset(buf)
		fields := 0
		for {
			_, ok, err := p.Next()
			if err != nil {
				t.Fatalf("evento %d inválido para jsonp: %v\n%s", i, err, buf)
			}
			if !ok {
				break
			}
			fields++
		}
		// O jsonp enxerga os mesmos campos de topo que a stdlib.
		if fields != len(m) {
			t.Fatalf("evento %d: jsonp viu %d campos, encoding/json viu %d\n%s", i, fields, len(m), buf)
		}
	}
}

// classify separa o evento nas dimensões que as proporções da Config
// controlam.
type shape struct {
	patch, extended, unknown, null bool
	fields                         int
}

func classify(t *testing.T, event []byte) shape {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(event, &m); err != nil {
		t.Fatalf("evento inválido: %v", err)
	}
	s := shape{fields: len(m), patch: len(m) <= 12}
	for name, raw := range m {
		v := string(raw)
		switch {
		case strings.HasPrefix(name, "X-Vendor-Attr-"):
			s.unknown = true
		case v == "null":
			s.null = true
		}
		if strings.Contains(v, `"$numberLong"`) || strings.Contains(v, `"$date"`) {
			s.extended = true
		}
	}
	return s
}

// As proporções são o que faz o corpus representar a carga real (85% de
// interim-update parcial): se elas escorregarem, o harness mede um workload
// que não é o do cliente.
func TestProporcoesRespeitadas(t *testing.T) {
	const n = 10000
	cfg := radiusgen.Config{
		Seed:              99,
		Entities:          2000,
		PatchRatio:        0.85,
		ExtendedJSONRatio: 0.10,
		UnknownFieldRatio: 0.01,
		NullFieldRatio:    0.05,
	}
	events := collect(t, gen(t, cfg), n)

	var patches, extended, unknown, nulls int
	for _, e := range events {
		s := classify(t, e)
		if s.patch {
			patches++
			if s.null {
				nulls++
			}
		}
		if s.extended {
			extended++
		}
		if s.unknown {
			unknown++
		}
	}

	// Tolerância de 2 pontos percentuais: é amostragem, não cota.
	check := func(name string, got, total int, want float64) {
		t.Helper()
		frac := float64(got) / float64(total)
		if diff := frac - want; diff > 0.02 || diff < -0.02 {
			t.Errorf("%s = %.3f (%d/%d), want ~%.3f", name, frac, got, total, want)
		}
	}
	check("PatchRatio", patches, n, 0.85)
	check("ExtendedJSONRatio", extended, n, 0.10)
	check("UnknownFieldRatio", unknown, n, 0.01)
	check("NullFieldRatio (sobre patches)", nulls, patches, 0.05)
}

// Ratio zero significa zero — é o que permite ao harness isolar uma dimensão
// de cada vez (ex.: medir sem Extended JSON).
func TestRatiosZeradosDesligamAsVariacoes(t *testing.T) {
	cfg := radiusgen.Config{
		Seed: 3, Entities: 100,
		PatchRatio: 1, ExtendedJSONRatio: 0, UnknownFieldRatio: 0, NullFieldRatio: 0,
	}
	for i, e := range collect(t, gen(t, cfg), 2000) {
		s := classify(t, e)
		if !s.patch || s.extended || s.unknown || s.null {
			t.Fatalf("evento %d = %+v, want só patch puro\n%s", i, s, e)
		}
	}
}

func TestPatchRatioZeroSoGeraEventoCheio(t *testing.T) {
	cfg := radiusgen.Config{Seed: 4, Entities: 100, PatchRatio: 0}
	for i, e := range collect(t, gen(t, cfg), 500) {
		if s := classify(t, e); s.patch {
			t.Fatalf("evento %d é patch com PatchRatio=0 (%d campos)\n%s", i, s.fields, e)
		}
	}
}

// _SEQ monotônico POR ENTIDADE é o que dá sentido ao stale check do
// materializer: um corpus com sequence retrocedendo faria a ingestão
// descartar eventos legítimos e a medição viraria ruído.
func TestSequenceMonotonicaPorEntidade(t *testing.T) {
	g := gen(t, radiusgen.Config{Seed: 11, Entities: 50})
	last := map[string]int64{}
	var buf []byte
	for i := 0; i < 5000; i++ {
		buf = g.Next(buf[:0])
		var m map[string]json.RawMessage
		if err := json.Unmarshal(buf, &m); err != nil {
			t.Fatal(err)
		}
		id := string(bytes.Trim(m["id"], `"`))
		seq := extractSeq(t, m["_SEQ"])
		if prev, seen := last[id]; seen && seq <= prev {
			t.Fatalf("entidade %s: _SEQ %d após %d (evento %d)", id, seq, prev, i)
		}
		last[id] = seq
	}
	if len(last) < 2 {
		t.Fatalf("corpus tocou %d entidades; o teste não prova nada", len(last))
	}
}

// extractSeq lê o _SEQ nos dois shapes (número cru e $numberLong).
func extractSeq(t *testing.T, raw json.RawMessage) int64 {
	t.Helper()
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}
	var ext struct {
		NumberLong string `json:"$numberLong"`
	}
	if err := json.Unmarshal(raw, &ext); err != nil {
		t.Fatalf("_SEQ em shape desconhecido: %s", raw)
	}
	var v int64
	if _, err := fmt.Sscan(ext.NumberLong, &v); err != nil {
		t.Fatalf("$numberLong inválido: %s", ext.NumberLong)
	}
	return v
}

// A key precisa ser estável e 1:1 com a entidade — é ela que resolve a
// primary. Entidades distintas não podem colidir, e a mesma entidade não pode
// mudar de id entre eventos.
func TestKeyEstavelEUnicaPorEntidade(t *testing.T) {
	// Acima de 256 de propósito: com poucas entities, um ip derivado só do
	// último octeto passaria despercebido — e a colisão de ip entre entities
	// distintas é exatamente o que estraga um índice hash no harness.
	const entities = 1000
	g := gen(t, radiusgen.Config{Seed: 5, Entities: entities, PatchRatio: 0})
	ipByID := map[string]string{}
	var buf []byte
	for i := 0; i < 4000; i++ {
		buf = g.Next(buf[:0])
		var m map[string]json.RawMessage
		if err := json.Unmarshal(buf, &m); err != nil {
			t.Fatal(err)
		}
		id := string(bytes.Trim(m["id"], `"`))
		ip, hasIP := m["Framed-IP-Address"]
		if !hasIP {
			continue // patch não carrega o ip
		}
		value := string(bytes.Trim(ip, `"`))
		if prev, seen := ipByID[id]; seen && prev != value {
			t.Fatalf("entidade %s mudou de ip: %s → %s", id, prev, value)
		}
		ipByID[id] = value
	}
	idByIP := map[string]string{}
	for id, ip := range ipByID {
		if other, dup := idByIP[ip]; dup {
			t.Fatalf("ip %s pertence a duas entidades: %s e %s", ip, other, id)
		}
		idByIP[ip] = id
	}
	if len(ipByID) > entities {
		t.Fatalf("%d ids distintos para %d entidades", len(ipByID), entities)
	}
}

// Sequential existe para a fase LOAD: N eventos precisam criar exatamente N
// entidades. Com sorteio uniforme seriam necessários ~1,6N eventos, e na
// rodada de 8M isso é uma hora de disco a mais.
func TestSequentialCobreTodasAsEntidades(t *testing.T) {
	const entities = 500
	g := gen(t, radiusgen.Config{Seed: 31, Entities: entities, Sequential: true, PatchRatio: 0})

	appearances := map[string]int{}
	for _, e := range collect(t, g, entities) {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(e, &m); err != nil {
			t.Fatal(err)
		}
		appearances[string(bytes.Trim(m["id"], `"`))]++
	}
	if len(appearances) != entities {
		t.Fatalf("%d entidades distintas em %d eventos, want %d", len(appearances), entities, entities)
	}
	for id, n := range appearances {
		if n != 1 {
			t.Fatalf("entidade %s apareceu %d vezes na primeira passada", id, n)
		}
	}

	// A segunda passada reencontra as MESMAS entidades (é o que faz o
	// harness poder alternar LOAD e STEADY sobre o mesmo conjunto).
	for _, e := range collect(t, g, entities) {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(e, &m); err != nil {
			t.Fatal(err)
		}
		id := string(bytes.Trim(m["id"], `"`))
		if appearances[id] == 0 {
			t.Fatalf("entidade %s só apareceu na segunda passada", id)
		}
	}
}

// Sem Sequential, o sorteio uniforme deixa entidades de fora — é justamente o
// que justifica o modo.
func TestSorteioUniformeNaoCobreTodasAsEntidades(t *testing.T) {
	const entities = 500
	g := gen(t, radiusgen.Config{Seed: 31, Entities: entities, PatchRatio: 0})
	seen := map[string]bool{}
	for _, e := range collect(t, g, entities) {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(e, &m); err != nil {
			t.Fatal(err)
		}
		seen[string(bytes.Trim(m["id"], `"`))] = true
	}
	if len(seen) == entities {
		t.Fatal("sorteio uniforme cobriu todas as entidades: o modo Sequential perdeu a razão de existir")
	}
}

// O NAS tem cardinalidade de centenas e distribuição concentrada (poucos NAS
// levam a maior parte das sessões) — é o que torna o índice bitmap sobre ele
// um caso realista em vez de um índice uniforme de brinquedo.
func TestNASCardinalidadeEConcentracao(t *testing.T) {
	const nas = 300
	g := gen(t, radiusgen.Config{Seed: 8, Entities: 5000, NASCount: nas, PatchRatio: 0})
	counts := map[string]int{}
	total := 0
	var buf []byte
	for i := 0; i < 5000; i++ {
		buf = g.Next(buf[:0])
		var m map[string]json.RawMessage
		if err := json.Unmarshal(buf, &m); err != nil {
			t.Fatal(err)
		}
		counts[string(m["NAS-Identifier"])]++
		total++
	}
	if len(counts) < 2 || len(counts) > nas {
		t.Fatalf("%d NAS distintos, want entre 2 e %d", len(counts), nas)
	}
	// Concentração: o NAS mais popular precisa levar bem mais que a fatia
	// uniforme (1/n).
	largest := 0
	for _, c := range counts {
		if c > largest {
			largest = c
		}
	}
	if float64(largest)/float64(total) <= 2.0/float64(len(counts)) {
		t.Errorf("NAS mais popular leva %.4f do tráfego com %d NAS: distribuição uniforme demais",
			float64(largest)/float64(total), len(counts))
	}
}

// O teste que fecha o ciclo: o corpus sintético precisa ser ingerível pelo
// pipeline REAL com o YAML de produção. Um gerador que produzisse tipo
// incompatível (ip malformado, timestamp fora do formato) só apareceria na
// rodada de 8M, como taxa de descarte inexplicada.
func TestCorpusIngerivelPeloMaterializer(t *testing.T) {
	spec, err := schema.Load(specPath, schema.DefaultsPadrao)
	if err != nil {
		t.Fatal(err)
	}
	c, err := spec.Compile(fakestorage.NewDict())
	if err != nil {
		t.Fatal(err)
	}
	m, err := materializer.New(c, fakestorage.New(fakestorage.Indexes{}))
	if err != nil {
		t.Fatal(err)
	}

	g := gen(t, radiusgen.Config{Seed: 21, Entities: 300, UnknownFieldRatio: 0.02, NullFieldRatio: 0.1})
	var buf []byte
	var created, updated int
	for i := 0; i < 3000; i++ {
		buf = g.Next(buf[:0])
		r := m.Apply(buf)
		switch r.Action {
		case materializer.ActionCreated:
			created++
		case materializer.ActionUpdated:
			updated++
		default:
			t.Fatalf("evento %d: Action=%v Category=%v Err=%v\n%s", i, r.Action, r.Category, r.Err, buf)
		}
	}
	if created != 300 {
		t.Errorf("created = %d, want 300 (uma por entidade)", created)
	}
	if updated == 0 {
		t.Error("nenhum update: o corpus não está reencontrando as entidades")
	}
	mt := m.Metrics()
	if mt.InvalidTotal != 0 || mt.FailedTotal != 0 || mt.StaleTotal != 0 {
		t.Errorf("métricas = %+v, want zero descarte", mt)
	}
}

func TestGenerateCorpus(t *testing.T) {
	cfg := radiusgen.Config{Seed: 13, Entities: 100}
	corpus, err := radiusgen.GenerateCorpus(cfg, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(corpus) != 500 {
		t.Fatalf("corpus com %d eventos, want 500", len(corpus))
	}
	// Cada evento tem backing array PRÓPRIO: um corpus onde os eventos
	// compartilham buffer seria sobrescrito pelo próximo Next e o benchmark
	// mediria lixo.
	want := collect(t, gen(t, cfg), 500)
	for i := range corpus {
		if !bytes.Equal(corpus[i], want[i]) {
			t.Fatalf("evento %d do corpus diverge do Next direto:\n c=%s\n n=%s", i, corpus[i], want[i])
		}
		if i > 0 && &corpus[i][0] == &corpus[i-1][0] {
			t.Fatalf("eventos %d e %d compartilham backing array", i-1, i)
		}
	}
}

func TestConfigInvalida(t *testing.T) {
	cases := []struct {
		name string
		cfg  radiusgen.Config
		msg  string
	}{
		{"entities negativo", radiusgen.Config{Entities: -1}, "Entities"},
		{"ratio acima de 1", radiusgen.Config{PatchRatio: 1.5}, "PatchRatio"},
		{"ratio negativo", radiusgen.Config{ExtendedJSONRatio: -0.1}, "ExtendedJSONRatio"},
		{"unknown ratio inválido", radiusgen.Config{UnknownFieldRatio: 2}, "UnknownFieldRatio"},
		{"null ratio inválido", radiusgen.Config{NullFieldRatio: -1}, "NullFieldRatio"},
		{"status values negativo", radiusgen.Config{StatusValues: -3}, "StatusValues"},
		{"nas count negativo", radiusgen.Config{NASCount: -2}, "NASCount"},
		{"terminate causes negativo", radiusgen.Config{TerminateCauses: -5}, "TerminateCauses"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := radiusgen.New(tc.cfg)
			if err == nil {
				t.Fatalf("Config inválida aceita: %+v", tc.cfg)
			}
			if g != nil {
				t.Error("Generator devolvido junto com erro")
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("erro = %q, want conter %q", err, tc.msg)
			}
		})
	}
}

// DefaultConfig é o workload de referência — quem quer "a carga real" pede
// por ele, não por uma Config vazia.
func TestDefaultConfigEhOWorkloadDeReferencia(t *testing.T) {
	cfg := radiusgen.DefaultConfig()
	cfg.Seed = 77
	cfg.Entities = 2000

	var patches, extended int
	events := collect(t, gen(t, cfg), 5000)
	for _, e := range events {
		s := classify(t, e)
		if s.patch {
			patches++
		}
		if s.extended {
			extended++
		}
	}
	if frac := float64(patches) / float64(len(events)); frac < 0.83 || frac > 0.87 {
		t.Errorf("PatchRatio = %.3f, want ~0.85", frac)
	}
	if frac := float64(extended) / float64(len(events)); frac < 0.08 || frac > 0.12 {
		t.Errorf("ExtendedJSONRatio = %.3f, want ~0.10", frac)
	}
}

// Config{} tem ratio zero em tudo — zero é zero, não "use o default". É o que
// permite ao harness isolar uma dimensão por vez; um default silencioso
// transformaria "medir sem Extended JSON" em "medir com 10%".
func TestConfigZeradaTemRatiosZerados(t *testing.T) {
	for i, e := range collect(t, gen(t, radiusgen.Config{}), 500) {
		s := classify(t, e)
		if s.patch || s.extended || s.unknown || s.null {
			t.Fatalf("evento %d = %+v, want evento cheio sem variações\n%s", i, s, e)
		}
	}
	// E a cardinalidade zerada VIRA default: sem isso não haveria entidade
	// para sortear.
	if _, err := radiusgen.New(radiusgen.Config{}); err != nil {
		t.Fatalf("Config{} deveria ser válida: %v", err)
	}
}

// O Next respeita o slice recebido: appenda a partir do fim e não toca no que
// já estava lá.
func TestNextAppendaEmDst(t *testing.T) {
	g := gen(t, radiusgen.Config{Seed: 17, Entities: 10})
	prefix := []byte("PREFIXO:")
	got := g.Next(append([]byte(nil), prefix...))
	if !bytes.HasPrefix(got, prefix) {
		t.Fatalf("Next sobrescreveu o conteúdo de dst: %s", got)
	}
	if err := json.Unmarshal(got[len(prefix):], &map[string]any{}); err != nil {
		t.Fatalf("evento após o prefixo é inválido: %v", err)
	}
}
