package executor

import (
	"errors"
	"io"
	"strconv"

	"github.com/nishisan-dev/nishidb/internal/query/budget"
)

// ErrWrite é a categoria de falha de escrita do StreamWriter.
var ErrWrite = errors.New("ndjson: escrita falhou")

// ErrRowTooLarge é a linha que não cabe no orçamento de buffer da consulta
// (runtime.query.maxBufferPerQuery).
//
// Existe como sentinela porque o desfecho tem de ser NOMEÁVEL: enquanto o
// writer crescia em silêncio, uma linha de 1 MiB não produzia erro nenhum — ela
// fixava a capacidade do buffer pela vida do writer, e o sintoma aparecia
// depois, como RSS, num processo que ninguém relacionava com aquela consulta. O
// protocolo traduz este sentinela para ROW_TOO_LARGE, e o driver JDBC para o
// SQLState de limite de programa.
var ErrRowTooLarge = errors.New("ndjson: linha acima do orçamento da consulta")

// DefaultBufSize é o tamanho-alvo do buffer de saída: 64 KiB dá vazão de
// escrita razoável sem pesar no orçamento de RSS.
const DefaultBufSize = 64 << 10

// StreamWriter serializa rows para w com buffer de tamanho FIXO. É onde o
// "memória O(batch), nunca O(resultado)" deixa de ser intenção e vira
// invariante verificável: o buffer é truncado a cada flush e nunca acumula o
// resultado.
//
// O buffer tem TETO. Antes não tinha: uma linha maior que bufSize fazia o append
// esticá-lo, e o `buf[:0]` mantinha a capacidade crescida — uma única row de
// 1 MiB fixava ~1 MiB pela vida do writer, mesmo que as 100 mil linhas seguintes
// fossem pequenas, e com N consultas simultâneas o pior caso era N × maiorLinha
// de RSS residente. Era o débito 28, cujo próprio texto dizia que o remédio é o
// `runtime.query.maxBufferPerQuery` do §80. Hoje a capacidade não passa do teto,
// e a linha que não cabe vira ErrRowTooLarge em vez de crescimento silencioso.
//
// Não é thread-safe. Uma vez em erro, fica STICKY: toda chamada seguinte
// devolve o mesmo erro sem tocar no io.Writer. Continuar escrevendo depois de
// uma falha produziria um stream truncado no meio de uma linha, que o cliente
// leria como dado.
type StreamWriter struct {
	w       io.Writer
	buf     []byte
	flushAt int
	// ceiling é o cap máximo que buf pode alcançar; 0 é ilimitado. É COPIADO do
	// orçamento na criação em vez de perguntado a cada linha porque WriteRow é
	// chamado uma vez por row: uma indireção por linha é custo que o gate de
	// alocação e o benchmark de vazão pagam por nada.
	ceiling int64
	err     error
}

// NewStreamWriter cria o writer. bufSize <= 0 usa DefaultBufSize.
//
// orc é o orçamento que este writer tem de respeitar e NUNCA é nil: quem não
// tem orçamento configurado passa budget.NewGlobal(0).Query(0), que diz
// "sem teto" em voz alta. Um nil aceito como "sem teto" faria o writer do
// servidor perder o orçamento por um esquecimento de fiação, sem nada acusar —
// que é exatamente o modo de falha que esta mudança veio fechar.
//
// O writer NÃO reserva: quem reserva é o chamador, na admissão
// (budget.PerQuery.Admit), antes de o primeiro byte ir para o fio. Aqui só
// se FAZ VALER o teto. A separação é o que permite à recusa por memória do
// PROCESSO ser um 503 com Retry-After, em vez de um stream que morre na linha
// quarenta mil por causa da memória de outra consulta — e deixa WriteRow sem
// nenhuma chamada ao orçamento, portanto sem CAS nem contenção por linha.
func NewStreamWriter(w io.Writer, bufSize int, orc *budget.PerQuery) *StreamWriter {
	if bufSize <= 0 {
		bufSize = DefaultBufSize
	}
	ceiling := orc.Ceiling()
	if ceiling > 0 && int64(bufSize) > ceiling/2 {
		// METADE do orçamento vira buffer de trabalho; a outra metade é FOLGA.
		//
		// A folga não é conservadorismo: o append que cruza o limiar de flush é
		// justamente o que estica o array, porque o limiar só é testado depois
		// dele. Com o buffer ocupando o orçamento inteiro, esse append estouraria
		// o teto TODA VEZ que o buffer enchesse — e um `maxBufferPerQuery` menor
		// que os 64 KiB default do stream (configuração legítima em máquina
		// pequena) viraria "nenhuma consulta funciona".
		if bufSize = int(ceiling / 2); bufSize <= 0 {
			bufSize = 1
		}
	}

	// O orçamento NÃO é reservado aqui: quem reserva é o chamador, na ADMISSÃO
	// (PerQuery.Admit), antes de qualquer byte ir para o fio. É o que permite
	// à recusa por memória do processo ser um 503 honesto com Retry-After, em
	// vez de um stream que morre na linha quarenta mil por culpa da memória de
	// outra consulta. O writer só FAZ VALER o teto que já foi reservado.
	return &StreamWriter{w: w, buf: make([]byte, 0, bufSize), flushAt: bufSize, ceiling: ceiling}
}

// WriteRow serializa row e descarrega quando o buffer passa do limiar.
func (s *StreamWriter) WriteRow(row Row) error {
	if s.err != nil {
		return s.err
	}
	before := len(s.buf)
	s.buf = AppendRow(s.buf, row)

	// O teto é sobre a CAPACIDADE, não sobre o comprimento: o que ocupa memória
	// é o array, e é ele que o `buf[:0]` do flush preservava.
	//
	// O custo desta comparação foi MEDIDO, e não estimado: BenchmarkStreamWriter
	// (sem teto) contra BenchmarkStreamWriterComOrcamento (teto de 4 MiB, que é
	// o default do §80), -count=6 e benchstat — 34,09 ns/op contra 34,57 ns/op,
	// +1,41% (p=0,026), com 0 B/op e 0 allocs/op nos dois. Meio nanossegundo por
	// row é o preço do débito 28 deixar de existir.
	if s.ceiling > 0 && int64(cap(s.buf)) > s.ceiling {
		row := int64(len(s.buf) - before)
		s.buf = s.buf[:before]
		// As linhas ANTERIORES já foram contadas pelo chamador (o `emitidas` do
		// servidor) e vão ser anunciadas na linha de fecho. Descartá-las com o
		// buffer faria o fecho prometer mais linhas do que o cliente recebeu. Se
		// o flush falhar, o erro dele é o mais fundamental e é o que fica.
		if err := s.flush(); err != nil {
			return err
		}
		// O array grande vira lixo AGORA. Sem isto, recusar a linha teria
		// evitado a mensagem e não a memória — que é o débito inteiro.
		s.buf = make([]byte, 0, s.flushAt)
		s.err = errOfBudget(row, s.ceiling)
		return s.err
	}

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

// erroDeOrcamento carrega os DOIS números que tornam a recusa acionável: o
// tamanho da linha e o teto que ela furou.
//
// É um tipo, e não um fmt.Errorf, para manter o `fmt` fora deste arquivo — a
// mesma escolha que o errors.Join do flush registra. O Unwrap é o que faz
// errors.Is(err, ErrRowTooLarge) valer para quem classifica o desfecho.
type erroDeOrcamento struct{ row, ceiling int64 }

func errOfBudget(row, ceiling int64) error { return &erroDeOrcamento{row: row, ceiling: ceiling} }

func (e *erroDeOrcamento) Error() string {
	return ErrRowTooLarge.Error() + ": a linha tem " + strconv.FormatInt(e.row, 10) +
		" bytes e o orçamento desta consulta é de " + strconv.FormatInt(e.ceiling, 10) +
		" bytes (runtime.query.maxBufferPerQuery)"
}

func (e *erroDeOrcamento) Unwrap() error { return ErrRowTooLarge }
