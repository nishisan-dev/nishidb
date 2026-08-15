package protocolo

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

// EscreveLinha serializa v como uma linha NDJSON (objeto + LF).
//
// SetEscapeHTML(false) alinha a moldura ao encoder das rows, que também não
// escapa `<`, `>` e `&` (ver AppendJSONString): o consumidor é um driver JDBC,
// não uma página HTML, e duas políticas de escaping no mesmo stream fariam o
// mesmo nome de coluna sair diferente na moldura e no dado.
func EscreveLinha(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// prefixoMoldura é o começo literal que o EscreveLinha produz para as três
// linhas de moldura. Nenhuma linha de dados pode começar assim: o servidor
// recusa a query cuja projeção tenha uma coluna chamada ChaveMoldura, antes do
// primeiro byte.
var prefixoMoldura = []byte(`{"` + ChaveMoldura + `":`)

// Leitor percorre uma resposta de /v1/query.
//
// O contrato que ele impõe é o que o driver da Fase 7 vai precisar: a primeira
// linha é o schema, as do meio são dados, e a última é SEMPRE um fecho. Um
// stream que acaba sem fecho é falha — é assim que uma conexão cortada no meio
// deixa de ser indistinguível de um resultado que terminou.
//
// As linhas de dados são entregues CRUAS, sem decodificar: o CLI só as repassa
// ao stdout, e decodificá-las para recodificá-las seria trabalho jogado fora
// (além de arriscar mudar a grafia dos doubles no caminho).
type Leitor struct {
	br     *bufio.Reader
	schema *LinhaSchema
	linha  []byte
	fim    *LinhaFim
	falha  *LinhaErro
	err    error
	done   bool
}

// NovoLeitor prepara a leitura de r.
func NovoLeitor(r io.Reader) *Leitor {
	return &Leitor{br: bufio.NewReader(r)}
}

// Next avança para a próxima linha de DADOS. false = fecho, erro de transporte
// ou resposta malformada — Fim(), Erro() e Err() distinguem.
func (l *Leitor) Next() bool {
	for {
		if l.done {
			return false
		}
		linha, err := l.br.ReadBytes('\n')
		if err != nil {
			l.done = true
			if errors.Is(err, io.EOF) && len(bytes.TrimSpace(linha)) == 0 {
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
		linha = bytes.TrimRight(linha, "\r\n")
		if len(linha) == 0 {
			continue
		}

		if !bytes.HasPrefix(linha, prefixoMoldura) {
			if l.schema == nil {
				l.done = true
				l.err = fmt.Errorf("%w: a primeira linha não é o schema", ErrStream)
				return false
			}
			l.linha = linha
			return true
		}
		if !l.moldura(linha) {
			return false
		}
	}
}

// moldura consome uma linha de moldura. Devolve true quando a leitura deve
// continuar (era o schema), false quando o stream acabou.
func (l *Leitor) moldura(linha []byte) bool {
	var qual struct {
		Nishidb string `json:"nishidb"`
	}
	if err := json.Unmarshal(linha, &qual); err != nil {
		l.done = true
		l.err = fmt.Errorf("%w: moldura ilegível: %w", ErrStream, err)
		return false
	}
	switch qual.Nishidb {
	case MolduraSchema:
		if l.schema != nil {
			l.done = true
			l.err = fmt.Errorf("%w: segunda linha de schema no mesmo stream", ErrStream)
			return false
		}
		var s LinhaSchema
		if err := json.Unmarshal(linha, &s); err != nil {
			l.done = true
			l.err = fmt.Errorf("%w: schema ilegível: %w", ErrStream, err)
			return false
		}
		l.schema = &s
		return true
	case MolduraFim:
		var f LinhaFim
		if err := json.Unmarshal(linha, &f); err != nil {
			l.done = true
			l.err = fmt.Errorf("%w: fecho ilegível: %w", ErrStream, err)
			return false
		}
		l.fim = &f
	case MolduraErro:
		var e LinhaErro
		if err := json.Unmarshal(linha, &e); err != nil {
			l.done = true
			l.err = fmt.Errorf("%w: fecho de erro ilegível: %w", ErrStream, err)
			return false
		}
		l.falha = &e
	default:
		l.done = true
		l.err = fmt.Errorf("%w: moldura %q desconhecida", ErrStream, qual.Nishidb)
		return false
	}
	l.done = true
	return false
}

// Schema é a primeira linha, disponível assim que o primeiro Next lê o stream.
func (l *Leitor) Schema() *LinhaSchema { return l.schema }

// Linha é a linha de dados corrente, CRUA e sem o LF. Válida até o próximo
// Next.
func (l *Leitor) Linha() []byte { return l.linha }

// Fim é o fecho de sucesso, ou nil se a consulta terminou em erro.
func (l *Leitor) Fim() *LinhaFim { return l.fim }

// Erro é o fecho de falha, ou nil se a consulta terminou bem.
func (l *Leitor) Erro() *LinhaErro { return l.falha }

// Err é a falha de TRANSPORTE ou de formato — não a falha da consulta, que sai
// por Erro(). Separá-las é o que permite ao cliente distinguir "o servidor
// disse que a query quebrou" de "o servidor sumiu no meio da frase".
func (l *Leitor) Err() error { return l.err }
