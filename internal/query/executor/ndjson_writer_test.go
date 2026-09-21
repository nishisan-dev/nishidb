package executor

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/budget"
	"github.com/nishisan-dev/nishidb/internal/types"
)

// withoutCeiling é o orçamento ILIMITADO, para os testes cujo assunto NÃO é o
// orçamento — concatenação, flush, erro sticky, estabilidade da capacidade.
//
// Passar um teto real neles acoplaria cada asserção a um número que não tem
// nada a ver com o que ela afirma, e a primeira mudança de default quebraria a
// suíte inteira por um motivo que não é o dela. Os testes que EXERCEM o
// orçamento declaram o teto deles, explicitamente — ver budget_writer_test.go.
func withoutCeiling() *budget.PerQuery { return budget.NewGlobal(0).Query(0) }

func rowOfTest(i int) Row {
	return Row{cols: []ColView{
		col("id", str("K"+strings.Repeat("0", 4)+string(rune('0'+i%10)))),
		col("n", i64(int64(i))),
	}}
}

func TestStreamWriterConcatena(t *testing.T) {
	var buf bytes.Buffer
	s := NewStreamWriter(&buf, 64, withoutCeiling()) // buffer pequeno de propósito: força vários flushes

	var expected []byte
	for i := 0; i < 50; i++ {
		r := rowOfTest(i)
		expected = AppendRow(expected, r)
		if err := s.WriteRow(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("saída divergiu da concatenação de AppendRow")
	}
	if n := bytes.Count(buf.Bytes(), []byte{'\n'}); n != 50 {
		t.Errorf("%d linhas, want 50", n)
	}
}

// Sem Flush o resto pendente NÃO aparece: o chamador é obrigado a chamá-lo, e
// esquecer produz um resultado truncado que parece completo.
func TestStreamWriterExigeFlush(t *testing.T) {
	var buf bytes.Buffer
	s := NewStreamWriter(&buf, 1<<20, withoutCeiling())
	if err := s.WriteRow(rowOfTest(0)); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Errorf("escreveu %d bytes antes do Flush", buf.Len())
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Error("Flush não escreveu nada")
	}
}

// espiao conta escritas e pode falhar sob demanda.
type espiao struct {
	escritas int
	fail     error
	curto    bool
}

func (e *espiao) Write(p []byte) (int, error) {
	e.escritas++
	if e.fail != nil {
		return 0, e.fail
	}
	if e.curto {
		return len(p) - 1, nil
	}
	return len(p), nil
}

func TestStreamWriterErroEhStickyEIdentificavel(t *testing.T) {
	fail := errors.New("cano quebrado")
	e := &espiao{fail: fail}
	s := NewStreamWriter(e, 16, withoutCeiling())

	err := s.WriteRow(rowOfTest(0))
	if err == nil {
		t.Fatal("erro de escrita não propagou")
	}
	if !errors.Is(err, ErrWrite) {
		t.Errorf("erro = %v, want errors.Is ErrWrite", err)
	}
	if !errors.Is(err, fail) {
		t.Errorf("erro = %v, want preservar a causa", err)
	}
	if !strings.Contains(err.Error(), "ndjson: escrita falhou") {
		t.Errorf("mensagem = %q", err)
	}

	// Sticky: nada mais é escrito, e o erro é o MESMO.
	before := e.escritas
	for i := 0; i < 5; i++ {
		if got := s.WriteRow(rowOfTest(i)); got != err {
			t.Fatalf("erro %d = %v, want o mesmo erro sticky", i, got)
		}
	}
	if got := s.Flush(); got != err {
		t.Errorf("Flush = %v, want o mesmo erro sticky", got)
	}
	if e.escritas != before {
		t.Errorf("escreveu %d vezes depois da falha", e.escritas-before)
	}
}

func TestStreamWriterShortWrite(t *testing.T) {
	s := NewStreamWriter(&espiao{curto: true}, 16, withoutCeiling())
	err := s.WriteRow(rowOfTest(0))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Errorf("erro = %v, want io.ErrShortWrite", err)
	}
	if !errors.Is(err, ErrWrite) {
		t.Errorf("erro = %v, want também ErrWrite", err)
	}
}

// Linha maior que o buffer: cresce, escreve e não trava.
func TestStreamWriterLinhaMaiorQueOBuffer(t *testing.T) {
	var buf bytes.Buffer
	s := NewStreamWriter(&buf, 8, withoutCeiling())
	grande := Row{cols: []ColView{col("txt", str(strings.Repeat("x", 10_000)))}}

	if err := s.WriteRow(grande); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() < 10_000 {
		t.Errorf("escreveu %d bytes, want a linha inteira", buf.Len())
	}
	if n := bytes.Count(buf.Bytes(), []byte{'\n'}); n != 1 {
		t.Errorf("%d linhas, want 1", n)
	}
}

// O gate que traduz "memória O(batch), nunca O(resultado)": a capacidade do
// buffer é a MESMA para 10 e para 200 mil linhas. Se crescesse com o número de
// linhas, o writer estaria acumulando o resultado — que é exatamente o que
// este projeto existe para não fazer.
func TestStreamWriterCapEstavel(t *testing.T) {
	capacidade := func(rows int) int {
		s := NewStreamWriter(io.Discard, 4096, withoutCeiling())
		for i := 0; i < rows; i++ {
			if err := s.WriteRow(rowOfTest(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
		return cap(s.buf)
	}

	// A comparação é entre DOIS tamanhos já muito acima do limiar de flush.
	// Comparar com um resultado pequeno demais (10 linhas) não prova nada: ali
	// o buffer nunca chega ao limiar e portanto nunca cresce — o teste passaria
	// por falta de exercício, não por correção.
	duzentosMil, quatrocentosMil := capacidade(200_000), capacidade(400_000)
	if duzentosMil != quatrocentosMil {
		t.Errorf("cap(buf) = %d com 200.000 linhas e %d com 400.000: o buffer cresce com o RESULTADO",
			duzentosMil, quatrocentosMil)
	}
	// O teto é bufSize + a maior linha, arredondado pela política de
	// crescimento do runtime: o append que ultrapassa o limiar acontece ANTES
	// do flush, então a capacidade estaciona um pouco acima de bufSize. É
	// exatamente essa folga que o orçamento por consulta limita hoje — ver
	// TestStreamWriterRecusaCrescerAlemDoOrcamento —, e é por ela que o
	// bufSize inicial fica em METADE do teto quando o teto é apertado.
	if quatrocentosMil > 2*4096 {
		t.Errorf("cap(buf) = %d, want <= 2x bufSize", quatrocentosMil)
	}
	t.Logf("cap(buf) estável em %d bytes (bufSize 4096) de 200.000 a 400.000 linhas", quatrocentosMil)
}

// bufSize <= 0 cai no padrão: um writer com buffer zero faria flush por linha.
func TestStreamWriterBufPadrao(t *testing.T) {
	for _, n := range []int{0, -1} {
		s := NewStreamWriter(io.Discard, n, withoutCeiling())
		if cap(s.buf) != DefaultBufSize || s.flushAt != DefaultBufSize {
			t.Errorf("bufSize %d: cap=%d flushAt=%d, want %d", n, cap(s.buf), s.flushAt, DefaultBufSize)
		}
	}
}

// Nenhum byte extra, nenhum byte perdido.
func TestStreamWriterBytesEscritos(t *testing.T) {
	var buf bytes.Buffer
	s := NewStreamWriter(&buf, 128, withoutCeiling())
	total := 0
	for i := 0; i < 500; i++ {
		r := rowOfTest(i)
		total += len(AppendRow(nil, r))
		if err := s.WriteRow(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != total {
		t.Errorf("escreveu %d bytes, want %d", buf.Len(), total)
	}
}

// ---------------------------------------------------------------- gates

var garbage []byte

// G1: zero alocação por linha. O dst é pré-crescido e truncado a cada
// iteração, como faz o StreamWriter.
func TestGateAppendRowZeroAlloc(t *testing.T) {
	row := Row{cols: []ColView{
		col("id", str("ABC")),
		col("ip", types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}}),
		col("n", i64(4096)),
		col("f", f64(-3.5)),
		col("t", ts(1700000000123)),
		col("b", types.Value{Type: types.TypeBytes, Bytes: []byte{0, 1}}),
		col("j", types.Value{Type: types.TypeJSON, Bytes: []byte(`{"a":1}`)}),
		{Name: "nulo", Null: true},
	}}
	dst := make([]byte, 0, 4096)

	got := testing.AllocsPerRun(1000, func() {
		dst = AppendRow(dst[:0], row)
	})
	if got != 0 {
		t.Errorf("AppendRow = %.1f allocs/op, want 0", got)
	}
	garbage = dst
}

// G2: zero alocação por TIPO. netip.AppendTo, base64.AppendEncode e
// time.Date/Clock foram escolhidos por não alocarem — o gate impede que uma
// troca futura por String() passe despercebida.
func TestGateAppendValueZeroAllocPorTipo(t *testing.T) {
	casos := map[string]types.Value{
		"int64":     i64(-42),
		"double":    f64(3.14),
		"bool":      {Type: types.TypeBool, I64: 1},
		"timestamp": ts(1700000000123),
		"ipv4":      {Type: types.TypeIPv4, Bytes: []byte{10, 0, 0, 1}},
		"ipv6":      {Type: types.TypeIPv6, Bytes: make([]byte, 16)},
		"prefixo4":  {Type: types.TypeIPv4Prefix, Bytes: []byte{10, 0, 0, 0, 8}},
		"prefixo6":  {Type: types.TypeIPv6Prefix, Bytes: append(make([]byte, 16), 64)},
		"string":    str("valor com \"aspas\" e acento ção"),
		"bytes":     {Type: types.TypeBytes, Bytes: []byte{1, 2, 3, 4, 5}},
		"json":      {Type: types.TypeJSON, Bytes: []byte(`{"a":[1,2]}`)},
		"nulo":      {},
	}
	dst := make([]byte, 0, 512)
	for name, v := range casos {
		t.Run(name, func(t *testing.T) {
			got := testing.AllocsPerRun(1000, func() { dst = AppendValue(dst[:0], v) })
			if got != 0 {
				t.Errorf("AppendValue(%s) = %.1f allocs/op, want 0", name, got)
			}
		})
	}
	garbage = dst
}

// G1-writer: o StreamWriter não pode alocar em regime. Sem este gate, uma
// realocação por flush passa pela suíte inteira e leva a alocação total a
// 1,4x o tamanho da saída — num caminho cujo contrato é justamente memória
// constante.
//
// A medição pula as primeiras escritas: o buffer cresce até o teto
// (bufSize + maiorLinha) e SÓ DEPOIS entra em regime. Medir o aquecimento
// junto mediria o crescimento, não o regime.
func TestGateStreamWriterZeroAllocEmRegime(t *testing.T) {
	if raceBuild {
		t.Skip("gate de alocação não roda sob -race: a instrumentação altera a contagem")
	}
	s := NewStreamWriter(io.Discard, 4096, withoutCeiling())
	row := rowOfTest(1)
	for i := 0; i < 2000; i++ { // aquecimento: leva o buffer ao teto
		if err := s.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	// A medição é ABSOLUTA, e não por testing.AllocsPerRun: ele faz divisão
	// INTEIRA (mallocs/runs), então qualquer taxa abaixo de 1 alocação por
	// chamada aparece como 0,0. Uma realocação a cada flush — que acontece a
	// cada ~146 rows com este buffer — é exatamente uma taxa dessas, e passaria
	// despercebida.
	const rodadas = 3000
	if got := mallocsIn(t, func() {
		for i := 0; i < rodadas; i++ {
			if err := s.WriteRow(row); err != nil {
				t.Fatal(err)
			}
		}
	}); got != 0 {
		t.Errorf("%d alocações em %d WriteRow, want 0 (%.4f por chamada)",
			got, rodadas, float64(got)/rodadas)
	}
	if got := mallocsIn(t, func() {
		for i := 0; i < 200; i++ {
			if err := s.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}); got != 0 {
		t.Errorf("%d alocações em 200 Flush, want 0", got)
	}
}

// mallocsIn conta as alocações ABSOLUTAS de f, com o GC parado para que o
// número seja o de mallocs e não o saldo.
func mallocsIn(t *testing.T, f func()) uint64 {
	t.Helper()
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	var before, depois runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&depois)
	return depois.Mallocs - before.Mallocs
}

// Os três caminhos do encoder que os gates anteriores NÃO exercitavam: o json
// com control char (que passa pelo laço de sanitização), a string com UTF-8
// inválido (que passa pelo caminho de escape) e o escape dentro do AppendRow.
func TestGateCaminhosNaoTriviaisZeroAlloc(t *testing.T) {
	casos := map[string]Row{
		"json com control char": {cols: []ColView{col("j",
			types.Value{Type: types.TypeJSON, Bytes: []byte("{\"a\": 1,\n\t\"b\": 2}")})}},
		"utf-8 inválido": {cols: []ColView{col("s", str("a\xffb\xc3"))}},
		"escape no valor e no nome": {cols: []ColView{
			{Name: "com\"aspas", Val: str("linha\nnova\ttab")}}},
	}
	dst := make([]byte, 0, 4096)
	for name, row := range casos {
		t.Run(name, func(t *testing.T) {
			if got := testing.AllocsPerRun(1000, func() { dst = AppendRow(dst[:0], row) }); got != 0 {
				t.Errorf("%s = %.1f allocs/op, want 0", name, got)
			}
		})
	}
	garbage = dst
}

func BenchmarkAppendRow(b *testing.B) {
	row := Row{cols: []ColView{
		col("id", str("CMP-V0001PPCLP-001")),
		col("status", str("Stop")),
		col("ip", types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}}),
		col("octets", i64(4096)),
		col("started", ts(1700000000123)),
		{Name: "cause", Null: true},
	}}
	dst := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = AppendRow(dst[:0], row)
	}
	garbage = dst
}

func BenchmarkAppendTimestamp(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = AppendValue(dst[:0], ts(1700000000123))
	}
	garbage = dst
}

func BenchmarkAppendJSONString(b *testing.B) {
	s := []byte(`um valor com "aspas", acentuação e uma quebra
de linha`)
	dst := make([]byte, 0, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = AppendJSONString(dst[:0], s)
	}
	garbage = dst
}

// BenchmarkStreamWriterComOrcamento mede o caminho de PRODUÇÃO: teto ativo.
//
// O BenchmarkStreamWriter logo abaixo roda sem teto, e o `s.teto > 0` do
// WriteRow curto-circuita nele — ou seja, ele NÃO mede o custo do orçamento.
// Medir só aquele e concluir "não custou nada" seria medir o ramo que não
// existe em produção. Os dois juntos é que respondem quanto o teto cobra.
func BenchmarkStreamWriterComOrcamento(b *testing.B) {
	orc := budget.NewGlobal(512 << 20).Query(4 << 20)
	if !orc.Admit() {
		b.Fatal("o orçamento do benchmark não coube")
	}
	s := NewStreamWriter(io.Discard, DefaultBufSize, orc)
	r := rowOfTest(7)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.WriteRow(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStreamWriter(b *testing.B) {
	s := NewStreamWriter(io.Discard, DefaultBufSize, withoutCeiling())
	row := rowOfTest(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.WriteRow(row); err != nil {
			b.Fatal(err)
		}
	}
	s.Flush()
}
