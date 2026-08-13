package executor

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/types"
)

func rowDeTeste(i int) Row {
	return Row{cols: []ColView{
		col("id", str("K"+strings.Repeat("0", 4)+string(rune('0'+i%10)))),
		col("n", i64(int64(i))),
	}}
}

func TestStreamWriterConcatena(t *testing.T) {
	var buf bytes.Buffer
	s := NewStreamWriter(&buf, 64) // buffer pequeno de propósito: força vários flushes

	var esperado []byte
	for i := 0; i < 50; i++ {
		r := rowDeTeste(i)
		esperado = AppendRow(esperado, r)
		if err := s.WriteRow(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), esperado) {
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
	s := NewStreamWriter(&buf, 1<<20)
	if err := s.WriteRow(rowDeTeste(0)); err != nil {
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
	falha    error
	curto    bool
}

func (e *espiao) Write(p []byte) (int, error) {
	e.escritas++
	if e.falha != nil {
		return 0, e.falha
	}
	if e.curto {
		return len(p) - 1, nil
	}
	return len(p), nil
}

func TestStreamWriterErroEhStickyEIdentificavel(t *testing.T) {
	falha := errors.New("cano quebrado")
	e := &espiao{falha: falha}
	s := NewStreamWriter(e, 16)

	err := s.WriteRow(rowDeTeste(0))
	if err == nil {
		t.Fatal("erro de escrita não propagou")
	}
	if !errors.Is(err, ErrWrite) {
		t.Errorf("erro = %v, want errors.Is ErrWrite", err)
	}
	if !errors.Is(err, falha) {
		t.Errorf("erro = %v, want preservar a causa", err)
	}
	if !strings.Contains(err.Error(), "ndjson: escrita falhou") {
		t.Errorf("mensagem = %q", err)
	}

	// Sticky: nada mais é escrito, e o erro é o MESMO.
	antes := e.escritas
	for i := 0; i < 5; i++ {
		if got := s.WriteRow(rowDeTeste(i)); got != err {
			t.Fatalf("erro %d = %v, want o mesmo erro sticky", i, got)
		}
	}
	if got := s.Flush(); got != err {
		t.Errorf("Flush = %v, want o mesmo erro sticky", got)
	}
	if e.escritas != antes {
		t.Errorf("escreveu %d vezes depois da falha", e.escritas-antes)
	}
	if s.Err() != err {
		t.Errorf("Err() = %v", s.Err())
	}
}

func TestStreamWriterShortWrite(t *testing.T) {
	s := NewStreamWriter(&espiao{curto: true}, 16)
	err := s.WriteRow(rowDeTeste(0))
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
	s := NewStreamWriter(&buf, 8)
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
	capacidade := func(linhas int) int {
		s := NewStreamWriter(io.Discard, 4096)
		for i := 0; i < linhas; i++ {
			if err := s.WriteRow(rowDeTeste(i)); err != nil {
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
	// do flush, então a capacidade estaciona um pouco acima de bufSize.
	if quatrocentosMil > 2*4096 {
		t.Errorf("cap(buf) = %d, want <= 2x bufSize", quatrocentosMil)
	}
	t.Logf("cap(buf) estável em %d bytes (bufSize 4096) de 200.000 a 400.000 linhas", quatrocentosMil)
}

// bufSize <= 0 cai no padrão: um writer com buffer zero faria flush por linha.
func TestStreamWriterBufPadrao(t *testing.T) {
	for _, n := range []int{0, -1} {
		s := NewStreamWriter(io.Discard, n)
		if cap(s.buf) != DefaultBufSize || s.flushAt != DefaultBufSize {
			t.Errorf("bufSize %d: cap=%d flushAt=%d, want %d", n, cap(s.buf), s.flushAt, DefaultBufSize)
		}
	}
}

// Nenhum byte extra, nenhum byte perdido.
func TestStreamWriterBytesEscritos(t *testing.T) {
	var buf bytes.Buffer
	s := NewStreamWriter(&buf, 128)
	total := 0
	for i := 0; i < 500; i++ {
		r := rowDeTeste(i)
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

var lixo []byte

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
		ColView{Name: "nulo", Null: true},
	}}
	dst := make([]byte, 0, 4096)

	got := testing.AllocsPerRun(1000, func() {
		dst = AppendRow(dst[:0], row)
	})
	if got != 0 {
		t.Errorf("AppendRow = %.1f allocs/op, want 0", got)
	}
	lixo = dst
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
	for nome, v := range casos {
		t.Run(nome, func(t *testing.T) {
			got := testing.AllocsPerRun(1000, func() { dst = AppendValue(dst[:0], v) })
			if got != 0 {
				t.Errorf("AppendValue(%s) = %.1f allocs/op, want 0", nome, got)
			}
		})
	}
	lixo = dst
}

func BenchmarkAppendRow(b *testing.B) {
	row := Row{cols: []ColView{
		col("id", str("CMP-V0001PPCLP-001")),
		col("status", str("Stop")),
		col("ip", types.Value{Type: types.TypeIPv4, Bytes: []byte{177, 1, 2, 3}}),
		col("octets", i64(4096)),
		col("started", ts(1700000000123)),
		ColView{Name: "cause", Null: true},
	}}
	dst := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = AppendRow(dst[:0], row)
	}
	lixo = dst
}

func BenchmarkAppendTimestamp(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = AppendValue(dst[:0], ts(1700000000123))
	}
	lixo = dst
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
	lixo = dst
}

func BenchmarkStreamWriter(b *testing.B) {
	s := NewStreamWriter(io.Discard, DefaultBufSize)
	row := rowDeTeste(1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.WriteRow(row); err != nil {
			b.Fatal(err)
		}
	}
	s.Flush()
}
