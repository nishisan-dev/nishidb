package executor

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/nishisan-dev/nishidb/internal/query/budget"
)

// rowOfBytes é uma linha de tamanho controlado, para exercer o orçamento.
func rowOfBytes(n int) Row {
	return Row{cols: []ColView{col("txt", str(strings.Repeat("x", n)))}}
}

// O writer RECUSA crescer além do orçamento, em vez de crescer em silêncio.
//
// Era o débito 28: uma linha de 1 MiB esticava o array e o `buf[:0]` mantinha a
// capacidade crescida pela vida do writer, mesmo que as 100 mil linhas seguintes
// fossem pequenas. O sintoma não aparecia como erro — aparecia como RSS, num
// processo que ninguém ligava àquela consulta, multiplicado pelo número de
// consultas simultâneas.
//
// As duas metades importam: a linha grande tem de FALHAR, e a capacidade tem de
// VOLTAR ao que foi pago. Só a primeira faria o writer recusar o erro e continuar
// segurando o megabyte, que é evitar o sintoma errado.
func TestStreamWriterRecusaCrescerAlemDoOrcamento(t *testing.T) {
	const buf, ceiling = 1 << 10, 4 << 10
	var saida bytes.Buffer
	s := NewStreamWriter(&saida, buf, admitido(t, budget.NewGlobal(1<<20).Query(ceiling)))

	// Uma linha que cabe no orçamento (mas não no buffer inicial) é ACEITA: o
	// orçamento é teto, não proibição de crescer.
	if err := s.WriteRow(rowOfBytes(2 << 10)); err != nil {
		t.Fatalf("linha de 2 KiB dentro do teto de 4 KiB: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	cresceu := cap(s.buf)
	if int64(cresceu) > ceiling {
		t.Fatalf("cap(buf) = %d acima do teto %d: o orçamento não está sendo aplicado", cresceu, ceiling)
	}

	// Uma linha ACIMA do teto é recusada.
	err := s.WriteRow(rowOfBytes(64 << 10))
	if err == nil {
		t.Fatal("a linha de 64 KiB passou num orçamento de 4 KiB: o writer voltou a crescer em silêncio")
	}
	if !errors.Is(err, ErrRowTooLarge) {
		t.Fatalf("erro = %v, quer errors.Is ErrRowTooLarge", err)
	}
	// E a memória volta: recusar o erro e ficar com o array grande evitaria o
	// sintoma errado — o débito é a MEMÓRIA, não a mensagem.
	if int64(cap(s.buf)) > ceiling {
		t.Errorf("cap(buf) = %d depois da recusa, acima do teto %d: o array grande ficou", cap(s.buf), ceiling)
	}

	// Sticky, como toda falha deste writer: continuar escrevendo depois de uma
	// recusa produziria um stream truncado no meio de uma linha.
	if got := s.WriteRow(rowOfBytes(8)); !errors.Is(got, ErrRowTooLarge) {
		t.Errorf("depois da recusa: %v, quer o mesmo erro sticky", got)
	}
}

// A recusa NOMEIA o limite e o tamanho da linha, em vez de crescer.
//
// Os dois números são o que torna o erro acionável: sem o tamanho da linha o
// operador não sabe se subir a chave resolve ou se a projeção é absurda, e sem o
// teto ele não sabe qual chave mexer. Uma mensagem genérica mandaria procurar um
// bug no motor, que é o que INTERNAL fazia antes de este código existir.
func TestLinhaAcimaDoOrcamentoNomeiaOLimiteEmVezDeCrescer(t *testing.T) {
	const ceiling = 4 << 10
	s := NewStreamWriter(io.Discard, 1<<10, admitido(t, budget.NewGlobal(1<<20).Query(ceiling)))

	err := s.WriteRow(rowOfBytes(64 << 10))
	if err == nil {
		t.Fatal("a linha grande não foi recusada")
	}
	msg := err.Error()
	for _, exigido := range []string{
		"4096",                            // o teto, em bytes
		"runtime.query.maxBufferPerQuery", // a chave que o operador mexe
	} {
		if !strings.Contains(msg, exigido) {
			t.Errorf("a mensagem não traz %q: %q", exigido, msg)
		}
	}
	// O tamanho da linha: sem ele, "não coube" não distingue uma projeção
	// absurda de um teto apertado demais.
	if !strings.Contains(msg, "65") {
		t.Errorf("a mensagem não traz o tamanho da linha: %q", msg)
	}
}

// As linhas ANTERIORES não podem se perder na recusa.
//
// O chamador já as contou (o `emitidas` do servidor) e já as anunciou na linha
// de fecho de erro. Descartá-las com o buffer faria o fecho anunciar mais
// linhas do que o cliente recebeu — um resultado parcial apresentado como se
// fosse maior do que é, que é o defeito que a moldura NDJSON existe para
// impedir.
func TestLinhaRecusadaNaoDescartaAsLinhasJaEscritas(t *testing.T) {
	var saida bytes.Buffer
	s := NewStreamWriter(&saida, 1<<10, admitido(t, budget.NewGlobal(1<<20).Query(4<<10)))

	const before = 5
	for i := 0; i < before; i++ {
		if err := s.WriteRow(rowOfBytes(16)); err != nil {
			t.Fatalf("linha %d: %v", i, err)
		}
	}
	if err := s.WriteRow(rowOfBytes(64 << 10)); !errors.Is(err, ErrRowTooLarge) {
		t.Fatalf("erro = %v, quer ErrRowTooLarge", err)
	}
	if n := bytes.Count(saida.Bytes(), []byte{'\n'}); n != before {
		t.Errorf("o cliente recebeu %d linhas, quer as %d que já tinham sido aceitas", n, before)
	}
}

// O buffer inicial não NASCE maior que o orçamento.
//
// O servidor sempre pede DefaultBufSize (64 KiB), então um `maxBufferPerQuery`
// abaixo disso — configuração legítima em máquina pequena — faria todo buffer
// nascer estourado. E o alvo fica em METADE do teto, não no teto inteiro: o
// append que cruza o limiar de flush é o que estica o array, e sem folga ele
// estouraria o teto toda vez que o buffer enchesse, transformando um orçamento
// apertado em "nenhuma consulta funciona".
func TestBufferInicialNaoNasceMaiorQueOOrcamento(t *testing.T) {
	casos := []struct {
		ceiling int64
		querCap int
	}{
		{ceiling: 4 << 20, querCap: DefaultBufSize}, // folgado: o alvo do stream vale
		{ceiling: 64 << 10, querCap: 32 << 10},      // igual ao alvo: metade
		{ceiling: 8 << 10, querCap: 4 << 10},        // apertado: metade
	}
	for _, c := range casos {
		g := budget.NewGlobal(1 << 30)
		s := NewStreamWriter(io.Discard, DefaultBufSize, admitido(t, g.Query(c.ceiling)))
		if cap(s.buf) != c.querCap {
			t.Errorf("teto %d: cap(buf) = %d, quer %d", c.ceiling, cap(s.buf), c.querCap)
		}
		// E ele funciona: um writer que nasce estourado devolveria erro na
		// primeira linha, que é o modo de falha que o cap veio impedir.
		if err := s.WriteRow(rowOfBytes(64)); err != nil {
			t.Errorf("teto %d: o writer nasceu inútil: %v", c.ceiling, err)
		}
	}
}

// admitido reserva o orçamento da consulta e falha o teste se ele não couber.
//
// O writer NÃO reserva — quem reserva é a admissão, em handleQuery —, então todo
// teste que constrói um writer com orçamento tem de passar pela mesma porta que
// o servidor passa. Sem isto, os testes exercitariam um caminho que produção não
// tem.
func admitido(t *testing.T, p *budget.PerQuery) *budget.PerQuery {
	t.Helper()
	if !p.Admit() {
		t.Fatalf("o orçamento de %d bytes não coube no processo do teste", p.Ceiling())
	}
	return p
}
