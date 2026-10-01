package counters_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nishisan-dev/nishidb/internal/counters"
	"github.com/nishisan-dev/nishidb/internal/index/valuekey"
	"github.com/nishisan-dev/nishidb/internal/predicate"
	"github.com/nishisan-dev/nishidb/internal/query"
	"github.com/nishisan-dev/nishidb/internal/schema"
	"github.com/nishisan-dev/nishidb/internal/storage/counter"
	"github.com/nishisan-dev/nishidb/internal/storage/encoding"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// fakeAggregate é o publicado de um grupo numa métrica de mentira.
type fakeAggregate struct {
	samples uint64
	avg     float64
	lowest  int64
	highest int64
}

// fakeMetricReader é um MetricReader com valores fixos por id de grupo. A
// leitura de um mapa com chave uint32 não aloca, e é isso que deixa o gate de
// alocação medir a tabela, e não o fake.
type fakeMetricReader struct {
	byGroup map[uint32]fakeAggregate
	usage   [4]int64
}

func (r *fakeMetricReader) Aggregate(id uint32) (uint64, float64, int64, int64) {
	a := r.byGroup[id]
	return a.samples, a.avg, a.lowest, a.highest
}

func (r *fakeMetricReader) Usage() (int64, int64, int64, int64) {
	return r.usage[0], r.usage[1], r.usage[2], r.usage[3]
}

// fakeMetricSource resolve a métrica pelo par `<counter>|<metric>`.
type fakeMetricSource map[string]*fakeMetricReader

func (s fakeMetricSource) Metric(counterName, metric string) (counters.MetricReader, bool) {
	r, ok := s[counterName+"|"+metric]
	if !ok {
		return nil, false
	}
	return r, true
}

func windowMetric(name string, window time.Duration, aggregates ...string) schema.CounterMetricSpec {
	return schema.CounterMetricSpec{
		Name:       name,
		Type:       schema.MetricTypeWindow,
		Source:     schema.MetricSourceTotal,
		Window:     window,
		Aggregates: aggregates,
	}
}

// exampleMetrics são as duas métricas do contrato aprovado: a média de 5 min e
// o máximo e o mínimo de 24 h — o mínimo declarado DEPOIS do máximo, para que a
// ordem das colunas seja a declarada e não a alfabética.
func exampleMetrics() []schema.CounterMetricSpec {
	return []schema.CounterMetricSpec{
		windowMetric("auth_5m", 5*time.Minute, schema.AggregateAvg),
		windowMetric("auth_24h", 24*time.Hour, schema.AggregateMax, schema.AggregateMin),
	}
}

// ponAverages são as médias publicadas dos três grupos com amostra. Nenhuma
// cabe em float32 (1e-9, 40.1 e 5.1 não têm representação exata em 32 bits), e
// é isso que faz um arredondamento no caminho — double → float32 → double —
// aparecer na comparação bit a bit. 0.25, 40.5 e 5 cabiam, e deixavam o
// arredondamento passar verde.
var ponAverages = map[string]float64{
	"PRCTA_G1M10|1|12":   1e-9,
	"PRCTA_G1M10|3|2":    40.1,
	"BR_SPOMO_OLT10|2|1": 5.1,
}

// ponMetricSource publica as métricas de exemplo sobre os quatro grupos do
// populaPON. O grupo PRCTA_G1M10|1|9 tem valores mas NENHUMA amostra: é o que
// separa "a coluna fica ausente" de "a coluna sai com o que o leitor devolveu".
func ponMetricSource(t *testing.T, c *counter.Counter) fakeMetricSource {
	t.Helper()
	id := func(text string) uint32 {
		id, ok := c.ByText([]byte(text))
		if !ok {
			t.Fatalf("grupo %q não existe no contador", text)
		}
		return id
	}
	auth5m := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{
		id("PRCTA_G1M10|1|9"):    {samples: 0, avg: 7},
		id("PRCTA_G1M10|1|12"):   {samples: 3, avg: ponAverages["PRCTA_G1M10|1|12"]},
		id("PRCTA_G1M10|3|2"):    {samples: 3, avg: ponAverages["PRCTA_G1M10|3|2"]},
		id("BR_SPOMO_OLT10|2|1"): {samples: 1, avg: ponAverages["BR_SPOMO_OLT10|2|1"]},
	}}
	auth24h := &fakeMetricReader{byGroup: map[uint32]fakeAggregate{
		id("PRCTA_G1M10|1|9"):    {samples: 0, lowest: 7, highest: 7},
		id("PRCTA_G1M10|1|12"):   {samples: 3, lowest: 0, highest: 47},
		id("PRCTA_G1M10|3|2"):    {samples: 3, lowest: 12, highest: 58},
		id("BR_SPOMO_OLT10|2|1"): {samples: 1, lowest: 5, highest: 900},
	}}
	name := c.Def().Name
	return fakeMetricSource{name + "|auth_5m": auth5m, name + "|auth_24h": auth24h}
}

// columnsOf executa a consulta e devolve os nomes e os tipos de saída, na
// ordem da projeção.
func columnsOf(t *testing.T, eng *query.Engine, sql string) (names []string, kinds []types.FieldType, nullable []bool) {
	t.Helper()
	it, err := eng.Execute(context.Background(), "servicos", sql, nil)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = it.Close() }()
	for _, c := range it.Columns() {
		names = append(names, c.Name)
		kinds = append(kinds, c.Type)
		nullable = append(nullable, c.Nullable)
	}
	return names, kinds, nullable
}

// sameColumnSet compara os nomes de saída como CONJUNTO. O `SELECT *` sai na
// ordem de FieldID do schema compilado — determinística, e decidida pelo
// compilador, não pela ordem do include —, então cobrar a sequência aqui
// cobraria o compilador e não a tabela.
func sameColumnSet(t *testing.T, names []string, want string) {
	t.Helper()
	got := slices.Clone(names)
	slices.Sort(got)
	wanted := strings.Split(want, ",")
	slices.Sort(wanted)
	if !slices.Equal(got, wanted) {
		t.Fatalf("colunas = %s, quero o conjunto %s", strings.Join(names, ","), want)
	}
}

// TestCounterTableWithMetricsPublishesTypedNullableMetricColumns: a tabela
// ganha uma coluna por agregado declarado, com avg double e min/max int64,
// todas nuláveis.
func TestCounterTableWithMetricsPublishesTypedNullableMetricColumns(t *testing.T) {
	c := openCounter(t, defByPON("c"))
	populaPON(t, c)
	eng := engineWithCounterMetrics(t, c, exampleMetrics(), ponMetricSource(t, c))

	names, kinds, nullable := columnsOf(t, eng, "SELECT * FROM _counters.c")
	sameColumnSet(t, names, "groupKey,olt,slot,port,total,rows,auth_5m_avg,auth_24h_max,auth_24h_min")
	wantKinds := map[string]types.FieldType{
		"auth_5m_avg":  types.TypeDouble,
		"auth_24h_max": types.TypeInt64,
		"auth_24h_min": types.TypeInt64,
	}
	for i, name := range names {
		want, ok := wantKinds[name]
		if !ok {
			continue
		}
		if kinds[i] != want {
			t.Errorf("tipo de %s = %#x, quero %#x", name, kinds[i], want)
		}
		if !nullable[i] {
			t.Errorf("%s tem de ser nulável: sem amostra a coluna fica ausente", name)
		}
	}
}

// TestCounterTableWithMetricsReadsPublishedAggregates: com amostra a linha
// carrega o agregado publicado; sem amostra as colunas ficam AUSENTES — NULL é
// ausência, e `IS NULL` acha o grupo — mesmo com o leitor devolvendo números.
func TestCounterTableWithMetricsReadsPublishedAggregates(t *testing.T) {
	c := openCounter(t, defByPON("c"))
	populaPON(t, c)
	eng := engineWithCounterMetrics(t, c, exampleMetrics(), ponMetricSource(t, c))

	rows := consulta(t, eng, "SELECT groupKey, auth_5m_avg, auth_24h_max, auth_24h_min FROM _counters.c ORDER BY groupKey ASC")
	if got, want := column(rows, "groupKey"), "BR_SPOMO_OLT10|2|1,PRCTA_G1M10|1|12,PRCTA_G1M10|1|9,PRCTA_G1M10|3|2"; got != want {
		t.Fatalf("groupKeys = %s, quero %s", got, want)
	}
	if got, want := column(rows, "auth_5m_avg"), "5.1,1e-09,<null>,40.1"; got != want {
		t.Errorf("auth_5m_avg = %s, quero %s", got, want)
	}
	if got, want := column(rows, "auth_24h_max"), "900,47,<null>,58"; got != want {
		t.Errorf("auth_24h_max = %s, quero %s", got, want)
	}
	if got, want := column(rows, "auth_24h_min"), "5,0,<null>,12"; got != want {
		t.Errorf("auth_24h_min = %s, quero %s", got, want)
	}

	rows = consulta(t, eng, "SELECT groupKey FROM _counters.c WHERE auth_5m_avg IS NULL AND auth_24h_max IS NULL AND auth_24h_min IS NULL")
	if got := column(rows, "groupKey"); got != "PRCTA_G1M10|1|9" {
		t.Fatalf("IS NULL achou %q; quero só o grupo sem amostra", got)
	}

	// Bit a bit, e não só pelo texto: o double tem de sair da linha com os
	// MESMOS 64 bits que o leitor publicou (ver ponAverages).
	it, err := eng.Execute(context.Background(), "servicos", "SELECT groupKey, auth_5m_avg FROM _counters.c", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	seen := 0
	for it.Next() {
		var group string
		var avg struct {
			null bool
			bits uint64
		}
		for _, col := range it.Row().Cols() {
			switch col.Name {
			case "groupKey":
				group = string(col.Val.Bytes)
			case "auth_5m_avg":
				avg.null, avg.bits = col.Null, uint64(col.Val.I64)
			}
		}
		want, hasSample := ponAverages[group]
		if !hasSample {
			continue
		}
		seen++
		if avg.null || avg.bits != math.Float64bits(want) {
			t.Errorf("auth_5m_avg de %s = bits %#x (null=%v), quero os bits de %v (%#x)", group, avg.bits, avg.null, want, math.Float64bits(want))
		}
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != len(ponAverages) {
		t.Fatalf("%d grupos com média conferidos, quero %d", seen, len(ponAverages))
	}
}

// TestCounterTableWithMetricsAnswersProjectionFilterAndOrder: a coluna da
// métrica é uma coluna como outra qualquer para o motor — alias e repetição
// passam pela numeração da projeção, o WHERE filtra (ausente compara FALSE) e o
// ORDER BY ordena com o ausente por último.
func TestCounterTableWithMetricsAnswersProjectionFilterAndOrder(t *testing.T) {
	c := openCounter(t, defByPON("c"))
	populaPON(t, c)
	eng := engineWithCounterMetrics(t, c, exampleMetrics(), ponMetricSource(t, c))

	const projection = "SELECT groupKey, auth_5m_avg, auth_5m_avg AS x, auth_5m_avg FROM _counters.c WHERE groupKey = 'PRCTA_G1M10|3|2'"
	names, _, _ := columnsOf(t, eng, projection)
	if got, want := strings.Join(names, ","), "groupKey,auth_5m_avg,x,auth_5m_avg_1"; got != want {
		t.Fatalf("nomes de saída = %s, quero %s", got, want)
	}
	rows := consulta(t, eng, projection)
	if len(rows) != 1 || rows[0]["auth_5m_avg"] != "40.1" || rows[0]["x"] != "40.1" || rows[0]["auth_5m_avg_1"] != "40.1" {
		t.Fatalf("projeção = %v", rows)
	}

	rows = consulta(t, eng, "SELECT groupKey FROM _counters.c WHERE auth_5m_avg < 1")
	if got := column(rows, "groupKey"); got != "PRCTA_G1M10|1|12" {
		t.Fatalf("auth_5m_avg < 1 = %q; quero só PRCTA_G1M10|1|12 (o grupo sem amostra é ausente e compara FALSE)", got)
	}

	rows = consulta(t, eng, "SELECT groupKey, auth_24h_max FROM _counters.c ORDER BY auth_24h_max DESC")
	if got, want := column(rows, "auth_24h_max"), "900,58,47,<null>"; got != want {
		t.Fatalf("ORDER BY auth_24h_max DESC = %s, quero %s", got, want)
	}
}

// TestCounterTableWithoutMetricSourceLeavesMetricColumnsNull: sem fonte o
// schema é o MESMO — as colunas existem e a projeção as aceita — e toda linha
// as tem ausentes.
func TestCounterTableWithoutMetricSourceLeavesMetricColumnsNull(t *testing.T) {
	c := openCounter(t, defByPON("c"))
	populaPON(t, c)
	eng := engineWithCounterMetrics(t, c, exampleMetrics(), nil)

	names, _, _ := columnsOf(t, eng, "SELECT * FROM _counters.c")
	sameColumnSet(t, names, "groupKey,olt,slot,port,total,rows,auth_5m_avg,auth_24h_max,auth_24h_min")
	rows := consulta(t, eng, "SELECT groupKey FROM _counters.c WHERE auth_5m_avg IS NULL AND auth_24h_max IS NULL AND auth_24h_min IS NULL")
	if len(rows) != 4 {
		t.Fatalf("%d grupos com as métricas NULL, quero os 4: %v", len(rows), rows)
	}
}

// TestConditionOnMetricColumnCompilesAgainstTableSchema: a condição de um
// alarme compila pelo avaliador compartilhado contra o TableSchema com as
// métricas, e avalia a linha que a tabela produz — é o que deixa o alarme usar
// `auth_5m_avg < 1` sem gramática nova.
func TestConditionOnMetricColumnCompilesAgainstTableSchema(t *testing.T) {
	c := openCounter(t, defByPON("c"))
	populaPON(t, c)
	metrics := exampleMetrics()
	sc, err := counters.TableSchema(c.Def(), metrics)
	if err != nil {
		t.Fatal(err)
	}
	prog, slots, err := predicate.CompileCondition("counters[0].alarms[0].when", "auth_5m_avg < 1", sc)
	if err != nil {
		t.Fatalf("a condição sobre a coluna da métrica tem de compilar: %v", err)
	}
	eval := predicate.NewAvaliadorOfPayload(prog, slots.IDs)

	_, store, err := counters.Table(c.Def(), c, metrics, ponMetricSource(t, c))
	if err != nil {
		t.Fatal(err)
	}
	snap := store.OpenSnapshot()
	defer store.CloseSnapshot(snap)
	cases := map[string]bool{
		"PRCTA_G1M10|1|12":   true,  // 1e-9
		"PRCTA_G1M10|3|2":    false, // 40.1
		"PRCTA_G1M10|1|9":    false, // sem amostra: ausente compara FALSE
		"BR_SPOMO_OLT10|2|1": false, // 5.1
	}
	for group, want := range cases {
		rec, _, ok, err := store.GetByKey(snap, []byte(group), nil)
		if err != nil || !ok {
			t.Fatalf("linha de %s: ok=%v err=%v", group, ok, err)
		}
		got, err := eval.Evaluate(rec.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("auth_5m_avg < 1 em %s = %v, quero %v", group, got, want)
		}
	}

	if _, _, err := predicate.CompileCondition("when", "auth_5m_max < 1", sc); err == nil {
		t.Fatal("auth_5m_max não foi declarado e a condição tem de ser recusada")
	}
}

// TestTableSchemaRefusesMetricColumnCollidingWithGroupingColumn: a colisão de
// uma coluna gerada é recusada pela MESMA lista do YAML, nomeando as duas
// pontas.
func TestTableSchemaRefusesMetricColumnCollidingWithGroupingColumn(t *testing.T) {
	def := defByOLT("c")
	def.Names = []string{"auth_5m_avg"}
	_, err := counters.TableSchema(def, exampleMetrics())
	if err == nil {
		t.Fatal("a coluna gerada auth_5m_avg colide com a do groupBy e foi aceita")
	}
	for _, want := range []string{`groupBy column "auth_5m_avg"`, `column "auth_5m_avg" generated by metrics[0]`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("erro %q não menciona %q", err, want)
		}
	}
}

// TestTableSchemaRefusesMoreMetricsThanTheCeiling: o teto é o que dimensiona o
// array de pilha da linha, e uma Definition montada sem YAML não o atravessa.
func TestTableSchemaRefusesMoreMetricsThanTheCeiling(t *testing.T) {
	metrics := make([]schema.CounterMetricSpec, 0, schema.MaxCounterMetrics+1)
	for i := 0; i <= schema.MaxCounterMetrics; i++ {
		metrics = append(metrics, windowMetric("m"+string(rune('a'+i)), time.Minute, schema.AggregateAvg, schema.AggregateMin, schema.AggregateMax))
	}
	if _, err := counters.TableSchema(defByOLT("c"), metrics); err == nil {
		t.Fatalf("%d métricas aceitas; o teto é %d", len(metrics), schema.MaxCounterMetrics)
	}
	if _, err := counters.TableSchema(defByOLT("c"), metrics[:schema.MaxCounterMetrics]); err != nil {
		t.Fatalf("%d métricas com os três agregados cabem no teto: %v", schema.MaxCounterMetrics, err)
	}
}

// TestTableSchemaRefusesUnknownAggregate: agregado sem coluna não vira coluna
// de tipo inventado.
func TestTableSchemaRefusesUnknownAggregate(t *testing.T) {
	_, err := counters.TableSchema(defByOLT("c"), []schema.CounterMetricSpec{windowMetric("auth_5m", 5*time.Minute, "p99")})
	if err == nil || !strings.Contains(err.Error(), `"p99"`) {
		t.Fatalf("agregado p99 tem de ser recusado nomeando-o: %v", err)
	}
}

// TestTableRefusesMetricSourceWithoutDeclaredMetric: com fonte presente, uma
// métrica declarada que ela não resolve é erro na montagem, e não uma coluna
// NULL para sempre.
func TestTableRefusesMetricSourceWithoutDeclaredMetric(t *testing.T) {
	c := openCounter(t, defByOLT("c"))
	_, _, err := counters.Table(c.Def(), c, exampleMetrics(), fakeMetricSource{})
	if err == nil || !strings.Contains(err.Error(), `"auth_5m"`) {
		t.Fatalf("a fonte sem a métrica auth_5m tem de ser recusada nomeando-a: %v", err)
	}
}

// ceilingGroupingColumns e ceilingColumns descrevem a linha no TETO da tabela:
// o groupBy com o máximo de colunas, as duas fixas e schema.MaxCounterMetrics
// métricas com os três agregados — a conta que dimensiona o array de pilha do
// record.
const (
	ceilingGroupingColumns = 16
	ceilingColumns         = ceilingGroupingColumns + 3 + schema.MaxCounterMetrics*3 + schema.MaxCounterMeasures
)

// ceilingGroupText é o groupKey do único grupo da tabela no teto.
const ceilingGroupText = "teto-1"

// ceilingReader é um Reader de UM grupo vivo (id 1), cuja chave composta tem
// ceilingGroupingColumns componentes.
type ceilingReader struct {
	key  []byte
	text []byte
}

func (r *ceilingReader) Limit() uint32                                   { return 2 }
func (r *ceilingReader) MeasureCount(_ uint32, index int, _ int64) int64 { return int64(index+1) * 30 }

func (r *ceilingReader) Group(id uint32) ([]byte, []byte, int64, int64, bool) {
	if id != 1 {
		return nil, nil, 0, 0, false
	}
	return r.key, r.text, 42, 57, true
}

func (r *ceilingReader) ByText(text []byte) (uint32, bool) {
	if string(text) != string(r.text) {
		return 0, false
	}
	return 1, true
}

// ceilingTable monta a tabela no teto e devolve o schema, o Store e o valor
// esperado de cada coluna, pelo nome. O groupBy alterna os três tipos aceitos;
// cada métrica publica uma média que não cabe em float32, um mínimo negativo e
// um máximo, todos distintos entre as métricas — uma coluna trocada de lugar,
// arredondada ou perdida não passa pela comparação.
func ceilingTable(t *testing.T) (*schema.Compiled, query.Store, map[string]types.Value) {
	t.Helper()
	def := counter.Definition{Name: "teto"}
	want := make(map[string]types.Value, ceilingColumns)
	components := make([]types.Value, 0, ceilingGroupingColumns)
	for i := 0; i < ceilingGroupingColumns; i++ {
		name := fmt.Sprintf("g%02d", i)
		var v types.Value
		switch i % 3 {
		case 0:
			v = types.Value{Type: types.TypeString, Bytes: []byte(name + "_v")}
		case 1:
			v = types.Value{Type: types.TypeInt64, I64: int64(-1000 * i)}
		default:
			v = types.Value{Type: types.TypeBool, I64: int64(i % 2)}
		}
		def.Columns = append(def.Columns, types.FieldID(i+1))
		def.Names = append(def.Names, name)
		def.Types = append(def.Types, v.Type)
		components = append(components, v)
		want[name] = v
	}
	key, err := valuekey.AppendComposite(nil, components)
	if err != nil {
		t.Fatal(err)
	}
	reader := &ceilingReader{key: key, text: []byte(ceilingGroupText)}
	want[schema.CounterKeyColumn] = types.Value{Type: types.TypeString, Bytes: reader.text}
	want[schema.CounterTotalColumn] = types.Value{Type: types.TypeInt64, I64: 42}
	want[schema.CounterRowsColumn] = types.Value{Type: types.TypeInt64, I64: 57}

	src := fakeMetricSource{}
	metrics := make([]schema.CounterMetricSpec, 0, schema.MaxCounterMetrics)
	for i := 0; i < schema.MaxCounterMetrics; i++ {
		name := fmt.Sprintf("m%d", i)
		avg := 0.1 + float64(i)*1e-9
		lowest, highest := -int64(i)-1, int64(i)*1000+7
		metrics = append(metrics, windowMetric(name, time.Duration(i+1)*time.Minute,
			schema.AggregateAvg, schema.AggregateMin, schema.AggregateMax))
		src[def.Name+"|"+name] = &fakeMetricReader{byGroup: map[uint32]fakeAggregate{
			1: {samples: 1, avg: avg, lowest: lowest, highest: highest},
		}}
		want[schema.MetricColumnName(name, schema.AggregateAvg)] = types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(avg))}
		want[schema.MetricColumnName(name, schema.AggregateMin)] = types.Value{Type: types.TypeInt64, I64: lowest}
		want[schema.MetricColumnName(name, schema.AggregateMax)] = types.Value{Type: types.TypeInt64, I64: highest}
	}
	for i := range schema.MaxCounterMeasures {
		name := fmt.Sprintf("rate%d", i)
		def.Measures = append(def.Measures, counter.MeasureDefinition{Name: name, Window: time.Minute, Unit: time.Second})
		want[name] = types.Value{Type: types.TypeDouble, I64: int64(math.Float64bits(float64(i+1) * 0.5))}
	}
	if len(want) != ceilingColumns {
		t.Fatalf("a bancada montou %d colunas, o teto é %d", len(want), ceilingColumns)
	}
	sc, store, err := counters.Table(def, reader, metrics, src)
	if err != nil {
		t.Fatalf("a tabela no teto tem de ser aceita: %v", err)
	}
	return sc, store, want
}

// checkCeilingPayload confere o payload campo a campo: exatamente as colunas
// esperadas, FieldID estritamente crescente e cada valor igual ao esperado —
// o double comparado pelos 64 bits.
func checkCeilingPayload(t *testing.T, sc *schema.Compiled, payload []byte, want map[string]types.Value) {
	t.Helper()
	nameOf := make(map[types.FieldID]string, len(want))
	for name := range want {
		f, ok := sc.FieldByName([]byte(name))
		if !ok {
			t.Fatalf("coluna %q fora do schema da tabela", name)
		}
		nameOf[f.ID] = name
	}
	var fr encoding.FieldReader
	fr.Reset(payload)
	n := 0
	var last types.FieldID
	for {
		id, v, ok := fr.Next()
		if !ok {
			break
		}
		if n > 0 && id <= last {
			t.Errorf("FieldID %d depois de %d: o payload exige ordem estritamente crescente", id, last)
		}
		last = id
		n++
		name, known := nameOf[id]
		if !known {
			t.Errorf("FieldID %d não é coluna esperada", id)
			continue
		}
		w := want[name]
		if v.Type != w.Type || v.I64 != w.I64 || string(v.Bytes) != string(w.Bytes) {
			t.Errorf("%s = %+v, quero %+v", name, v, w)
		}
	}
	if err := fr.Err(); err != nil {
		t.Fatal(err)
	}
	if n != len(want) {
		t.Fatalf("%d colunas no payload, quero %d", n, len(want))
	}
}

// TestCounterTableAtRowCeilingPublishesEveryColumn: a linha no teto — 16
// colunas do groupBy, três fixas, oito medidas e oito métricas com três agregados — sai
// inteira, em FieldID crescente, com cada média igual bit a bit à publicada.
// É o único teste que ENCHE o array de pilha do record: abaixo do teto, um
// array com uma posição a menos passaria verde.
func TestCounterTableAtRowCeilingPublishesEveryColumn(t *testing.T) {
	sc, store, want := ceilingTable(t)
	snap := store.OpenSnapshot()
	defer store.CloseSnapshot(snap)

	rec, _, ok, err := store.GetByRowID(snap, 1, nil)
	if err != nil || !ok {
		t.Fatalf("linha no teto por rowID: ok=%v err=%v", ok, err)
	}
	checkCeilingPayload(t, sc, rec.Payload, want)

	rec, _, ok, err = store.GetByKey(snap, []byte(ceilingGroupText), nil)
	if err != nil || !ok {
		t.Fatalf("linha no teto pela key: ok=%v err=%v", ok, err)
	}
	checkCeilingPayload(t, sc, rec.Payload, want)
}
