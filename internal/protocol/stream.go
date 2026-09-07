package protocol

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrStream é a categoria de falha na LEITURA de uma resposta de /v1/query:
// stream que não começa pelo schema, moldura ilegível, ou fim sem linha de
// fecho.
var ErrStream = errors.New("protocolo: resposta malformada")

// WriteLine serializa v como uma linha NDJSON (objeto + LF).
//
// SetEscapeHTML(false) alinha a moldura ao encoder das rows, que também não
// escapa `<`, `>` e `&` (ver AppendJSONString): o consumidor é um driver JDBC,
// não uma página HTML, e duas políticas de escaping no mesmo stream fariam o
// mesmo nome de coluna sair diferente na moldura e no dado.
func WriteLine(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// framePrefix é o começo literal que o WriteLine produz para as três
// linhas de moldura. Nenhuma linha de dados pode começar assim: o servidor
// recusa a query cuja projeção tenha uma coluna chamada FrameKey, antes do
// primeiro byte.
var framePrefix = []byte(`{"` + FrameKey + `":`)

// Reader percorre uma resposta de /v1/query.
//
// O contrato que ele impõe é o que o driver da Fase 7 vai precisar: a primeira
// linha é o schema, as do meio são dados, e a última é SEMPRE um fecho. Um
// stream que acaba sem fecho é falha — é assim que uma conexão cortada no meio
// deixa de ser indistinguível de um resultado que terminou.
//
// As linhas de dados são entregues CRUAS, sem decodificar: o CLI só as repassa
// ao stdout, e decodificá-las para recodificá-las seria trabalho jogado fora
// (além de arriscar mudar a grafia dos doubles no caminho).
type Reader struct {
	br      *bufio.Reader
	schema  *SchemaLine
	line    []byte
	end     *EndLine
	failure *ErrorLine
	err     error
	done    bool
}

// NewReader prepara a leitura de r.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// Next avança para a próxima linha de DADOS. false = fecho, erro de transporte
// ou resposta malformada — End(), Failure() e Err() distinguem.
func (l *Reader) Next() bool {
	for {
		if l.done {
			return false
		}
		line, err := l.br.ReadBytes('\n')
		if err != nil {
			l.done = true
			if errors.Is(err, io.EOF) && len(bytes.TrimSpace(line)) == 0 {
				// EOF limpo, mas sem fecho: o servidor morreu, a rede caiu ou
				// alguém truncou o corpo. Sem esta checagem o cliente
				// apresentaria um resultado parcial como completo.
				l.err = fmt.Errorf("%w: o stream terminou sem a linha de fecho", ErrStream)
				return false
			}
			if errors.Is(err, io.EOF) {
				l.err = fmt.Errorf("%w: última linha sem terminador", ErrStream)
				return false
			}
			l.err = fmt.Errorf("%w: %w", ErrStream, err)
			return false
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			continue
		}

		if !bytes.HasPrefix(line, framePrefix) {
			if l.schema == nil {
				l.done = true
				l.err = fmt.Errorf("%w: a primeira linha não é o schema", ErrStream)
				return false
			}
			l.line = line
			return true
		}
		if !l.readFrame(line) {
			return false
		}
	}
}

// readFrame consome uma linha de readFrame. Devolve true quando a leitura deve
// continuar (era o schema), false quando o stream acabou.
func (l *Reader) readFrame(line []byte) bool {
	var which struct {
		Nishidb string `json:"nishidb"`
	}
	if err := json.Unmarshal(line, &which); err != nil {
		l.done = true
		l.err = fmt.Errorf("%w: moldura ilegível: %w", ErrStream, err)
		return false
	}
	switch which.Nishidb {
	case FrameSchema:
		if l.schema != nil {
			l.done = true
			l.err = fmt.Errorf("%w: segunda linha de schema no mesmo stream", ErrStream)
			return false
		}
		var s SchemaLine
		if err := json.Unmarshal(line, &s); err != nil {
			l.done = true
			l.err = fmt.Errorf("%w: schema ilegível: %w", ErrStream, err)
			return false
		}
		l.schema = &s
		return true
	case FrameEnd:
		var f EndLine
		if err := json.Unmarshal(line, &f); err != nil {
			l.done = true
			l.err = fmt.Errorf("%w: fecho ilegível: %w", ErrStream, err)
			return false
		}
		l.end = &f
	case FrameError:
		var e ErrorLine
		if err := json.Unmarshal(line, &e); err != nil {
			l.done = true
			l.err = fmt.Errorf("%w: fecho de erro ilegível: %w", ErrStream, err)
			return false
		}
		l.failure = &e
	default:
		l.done = true
		l.err = fmt.Errorf("%w: moldura %q desconhecida", ErrStream, which.Nishidb)
		return false
	}
	l.done = true
	return false
}

// Schema é a primeira linha, disponível assim que o primeiro Next lê o stream.
func (l *Reader) Schema() *SchemaLine { return l.schema }

// Line é a linha de dados corrente, CRUA e sem o LF. Válida até o próximo
// Next.
func (l *Reader) Line() []byte { return l.line }

// End é o fecho de sucesso, ou nil se a consulta terminou em erro.
func (l *Reader) End() *EndLine { return l.end }

// Failure é o fecho de falha, ou nil se a consulta terminou bem.
func (l *Reader) Failure() *ErrorLine { return l.failure }

// Err é a falha de TRANSPORTE ou de formato — não a falha da consulta, que sai
// por Failure(). Separá-las é o que permite ao cliente distinguir "o servidor
// disse que a query quebrou" de "o servidor sumiu no meio da frase".
func (l *Reader) Err() error { return l.err }
