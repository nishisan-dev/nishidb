package executor

import (
	"errors"
	"io"
)

// ErrWrite é a categoria de falha de escrita do StreamWriter.
var ErrWrite = errors.New("ndjson: escrita falhou")

// DefaultBufSize é o tamanho-alvo do buffer de saída: 64 KiB dá vazão de
// escrita razoável sem pesar no orçamento de RSS.
const DefaultBufSize = 64 << 10

// StreamWriter serializa rows para w com buffer de tamanho FIXO. É onde o
// "memória O(batch), nunca O(resultado)" deixa de ser intenção e vira
// invariante verificável: o buffer é truncado a cada flush e nunca acumula o
// resultado.
//
// Teto de memória: uma linha maior que bufSize faz o append crescer o buffer
// para caber a linha, e o buf[:0] mantém a capacidade crescida. O buffer
// converge para bufSize + maiorLinha — independente do NÚMERO de linhas, que é
// exatamente o contrato.
//
// Não é thread-safe. Uma vez em erro, fica STICKY: toda chamada seguinte
// devolve o mesmo erro sem tocar no io.Writer. Continuar escrevendo depois de
// uma falha produziria um stream truncado no meio de uma linha, que o cliente
// leria como dado.
type StreamWriter struct {
	w       io.Writer
	buf     []byte
	flushAt int
	err     error
}

// NewStreamWriter cria o writer. bufSize <= 0 usa DefaultBufSize.
func NewStreamWriter(w io.Writer, bufSize int) *StreamWriter {
	if bufSize <= 0 {
		bufSize = DefaultBufSize
	}
	return &StreamWriter{w: w, buf: make([]byte, 0, bufSize), flushAt: bufSize}
}

// WriteRow serializa row e descarrega quando o buffer passa do limiar.
func (s *StreamWriter) WriteRow(row Row) error {
	if s.err != nil {
		return s.err
	}
	s.buf = AppendRow(s.buf, row)
	if len(s.buf) >= s.flushAt {
		return s.flush()
	}
	return nil
}

// Flush escreve o que estiver pendente. É OBRIGATÓRIO ao fim do stream: sem
// ele, a última fatia de linhas fica no buffer e o cliente vê um resultado
// truncado.
func (s *StreamWriter) Flush() error {
	if s.err != nil {
		return s.err
	}
	return s.flush()
}

func (s *StreamWriter) flush() error {
	if len(s.buf) == 0 {
		return nil
	}
	n, err := s.w.Write(s.buf)
	if err == nil && n < len(s.buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// errors.Join em vez de fmt.Errorf: mantém o fmt fora do arquivo e
		// preserva o errors.Is para os DOIS erros.
		s.err = errors.Join(ErrWrite, err)
		s.buf = s.buf[:0]
		return s.err
	}
	s.buf = s.buf[:0] // trunca, não realoca
	return nil
}

// Err devolve o erro sticky (nil se saudável).
func (s *StreamWriter) Err() error { return s.err }
