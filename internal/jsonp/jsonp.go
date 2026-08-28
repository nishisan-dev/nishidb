// Package jsonp implementa o tokenizer JSON de baixa alocação do hot path de
// ingestão (spec §20). Cobre o subset necessário: objeto top-level com chaves
// string; valores string, number, bool, null; objetos/arrays aninhados são
// entregues como bytes JSON opacos. Wrappers Mongo Extended JSON (spec §19)
// são desembrulhados durante o parse.
//
// O modelo proibido map[string]any nunca é materializado: o consumidor puxa
// campo a campo via Next(), recebendo janelas ([]byte) sobre o input.
//
// Chaves duplicadas no objeto top-level NÃO são erro: o parser emite todas
// as ocorrências, na ordem do documento. A política last-wins é
// responsabilidade do consumidor (o PatchBuffer do materializer a aplica).
//
// Leniência estrutural (decisão de design): o skip de objetos/arrays
// aninhados é puramente estrutural — conta profundidade e respeita strings,
// mas NÃO valida o conteúdo. O Raw de um KindObject/KindArray pode não ser
// JSON válido (ex.: `{ffff}`). A validação acontece na borda de
// armazenamento: o typeconv rejeita a janela com encoding/json.Valid ao
// converter para o tipo `json` (TYPE_CONVERSION).
package jsonp

import (
	"bytes"
	"errors"
	"fmt"
	"unicode/utf16"
	"unicode/utf8"
)

// Kind é o tipo JSON do valor de um campo.
type Kind uint8

const (
	KindString Kind = iota + 1
	KindNumber
	KindBool
	KindNull
	KindObject // valor aninhado: Raw = bytes JSON completos do objeto
	KindArray  // valor aninhado: Raw = bytes JSON completos do array
)

// Ext indica que o valor veio de um wrapper Mongo Extended JSON (spec §19).
// O wrapper nunca é exposto: só o valor desembrulhado.
type Ext uint8

const (
	ExtNone       Ext = iota
	ExtNumberLong     // era {"$numberLong":"..."} → Kind=KindNumber, Raw=dígitos
	ExtDate           // era {"$date":...} → KindString (relaxed) ou KindNumber (canonical)
)

// Field é um campo top-level do evento.
//
// Key e Raw são válidos SOMENTE até a próxima chamada de Next ou Reset:
// podem apontar para o input ou para o scratch interno reutilizado.
// O consumidor copia o que precisar reter.
type Field struct {
	Key  []byte // nome do campo, unescaped
	Kind Kind
	Ext  Ext
	Raw  []byte // string: conteúdo unescaped; number: dígitos; bool: "true"/"false";
	// null: nil; object/array: bytes JSON crus
}

// ErrInvalidJSON é a categoria de todo erro sintático deste pacote
// (spec §101: INVALID_JSON). Use errors.Is(err, ErrInvalidJSON).
var ErrInvalidJSON = errors.New("invalid json")

// SyntaxError descreve um erro sintático com a posição (byte) no input.
type SyntaxError struct {
	Pos int
	Msg string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("json inválido na posição %d: %s", e.Pos, e.Msg)
}

func (e *SyntaxError) Unwrap() error { return ErrInvalidJSON }

type state uint8

const (
	stateStart state = iota // antes do '{' top-level
	stateField              // esperando próximo campo
	stateDone               // objeto consumido com sucesso
	stateErr                // erro emitido; Next repete o erro
)

// Parser é um tokenizer pull sobre um evento JSON. Zero-alloc em steady
// state: reutilize a mesma instância via Reset. Não é thread-safe.
type Parser struct {
	data    []byte
	pos     int
	scratch []byte
	state   state
	err     error
}

// BytesRetidos é a capacidade do scratch que este parser segura entre eventos.
//
// Existe para a contabilidade de memória da ingestão paralela: com N slots de
// decodificação vivos ao mesmo tempo, o scratch de cada um sobrevive entre
// eventos e cresce até a marca d'água do maior valor com escape que passou por
// ele. Sem esta leitura o gauge de memória dos slots mentiria por omissão.
func (p *Parser) BytesRetidos() int { return cap(p.scratch) }

// BytesDeEntradaRetidos é o tamanho do documento que este parser ainda aponta.
//
// Não é memória DELE: é a janela sobre o evento que o Reset guardou. Existe
// para que um teste possa afirmar que a referência foi solta — sem ela, "o slot
// não segura mais o lote de fetch" é uma afirmação que ninguém consegue checar.
func (p *Parser) BytesDeEntradaRetidos() int { return len(p.data) }

// Encolhe solta o scratch, devolvendo a memória ao coletor.
//
// O que ocupa memória é o ARRAY, não o comprimento — é a lição do débito 28 —,
// então `p.scratch[:0]` não devolveria nada. O próximo evento realoca.
func (p *Parser) Encolhe() { p.scratch = nil }

// SoltaEntrada esquece o documento que este parser percorreu.
//
// `data` é uma JANELA sobre o evento, e o parser a guarda entre um Reset e o
// seguinte. Num pipeline em que N parsers vivem em N slots ociosos, essa janela
// impede o buffer de fetch inteiro de ser coletado — o evento tem alguns KiB, o
// lote que o trouxe tem megabytes. Quem terminou de usar o documento chama isto.
//
// Não é o Encolhe: aquele solta o buffer que é NOSSO e será realocado; este
// solta a referência a memória de outro dono.
func (p *Parser) SoltaEntrada() {
	p.data = nil
	p.pos = 0
	p.state = stateDone
}

// Reset prepara o parser para um novo evento, reutilizando o scratch.
func (p *Parser) Reset(data []byte) {
	p.data = data
	p.pos = 0
	p.scratch = p.scratch[:0]
	p.state = stateStart
	p.err = nil
}

// Next devolve o próximo campo top-level. Retorna (campo, true, nil) para
// cada campo, (zero, false, nil) no fim do objeto e (zero, false, err) em
// JSON inválido. Field.Key/Raw são válidos só até a próxima chamada.
func (p *Parser) Next() (Field, bool, error) {
	switch p.state {
	case stateDone:
		return Field{}, false, nil
	case stateErr:
		return Field{}, false, p.err
	case stateStart:
		p.skipWS()
		if p.pos >= len(p.data) || p.data[p.pos] != '{' {
			return Field{}, false, p.fail("esperado '{'")
		}
		p.pos++
		p.skipWS()
		if p.pos < len(p.data) && p.data[p.pos] == '}' {
			p.pos++
			if err := p.expectEOF(); err != nil {
				return Field{}, false, err
			}
			p.state = stateDone
			return Field{}, false, nil
		}
		p.state = stateField
	}
	p.scratch = p.scratch[:0]
	return p.parseField()
}

// parseField consome `"key" : value [, | }]` e decide o próximo estado.
func (p *Parser) parseField() (Field, bool, error) {
	var f Field
	key, err := p.parseString()
	if err != nil {
		return Field{}, false, p.failErr(err)
	}
	f.Key = key
	p.skipWS()
	if p.pos >= len(p.data) || p.data[p.pos] != ':' {
		return Field{}, false, p.fail("esperado ':'")
	}
	p.pos++
	p.skipWS()
	if err := p.parseValue(&f); err != nil {
		return Field{}, false, p.failErr(err)
	}
	p.skipWS()
	if p.pos >= len(p.data) {
		return Field{}, false, p.fail("objeto não terminado")
	}
	switch p.data[p.pos] {
	case ',':
		p.pos++
		p.skipWS()
		p.state = stateField
	case '}':
		p.pos++
		if err := p.expectEOF(); err != nil {
			return Field{}, false, err
		}
		p.state = stateDone
	default:
		return Field{}, false, p.fail("esperado ',' ou '}'")
	}
	return f, true, nil
}

// parseValue preenche f.Kind/f.Raw/f.Ext a partir do valor em p.pos.
func (p *Parser) parseValue(f *Field) error {
	if p.pos >= len(p.data) {
		return &SyntaxError{Pos: p.pos, Msg: "valor ausente"}
	}
	switch c := p.data[p.pos]; {
	case c == '"':
		raw, err := p.parseString()
		if err != nil {
			return err
		}
		f.Kind = KindString
		f.Raw = raw
		return nil
	case c == '-' || (c >= '0' && c <= '9'):
		raw, err := p.parseNumber()
		if err != nil {
			return err
		}
		f.Kind = KindNumber
		f.Raw = raw
		return nil
	case c == 't':
		if err := p.expectLiteral("true"); err != nil {
			return err
		}
		f.Kind = KindBool
		f.Raw = trueBytes
		return nil
	case c == 'f':
		if err := p.expectLiteral("false"); err != nil {
			return err
		}
		f.Kind = KindBool
		f.Raw = falseBytes
		return nil
	case c == 'n':
		if err := p.expectLiteral("null"); err != nil {
			return err
		}
		f.Kind = KindNull
		f.Raw = nil
		return nil
	case c == '{':
		if ok, err := p.tryExtended(f); err != nil {
			return err
		} else if ok {
			return nil
		}
		raw, err := p.skipComposite()
		if err != nil {
			return err
		}
		f.Kind = KindObject
		f.Raw = raw
		return nil
	case c == '[':
		raw, err := p.skipComposite()
		if err != nil {
			return err
		}
		f.Kind = KindArray
		f.Raw = raw
		return nil
	}
	return &SyntaxError{Pos: p.pos, Msg: "valor inválido"}
}

var (
	dollarNumberLong = []byte("$numberLong")
	dollarDate       = []byte("$date")
)

// tryExtended tenta desembrulhar um wrapper Mongo Extended JSON (spec §19)
// com p.pos no '{' do valor. Shapes aceitos (exatamente 1 chave):
//
//	{"$numberLong":"<dígitos>"}            → KindNumber, ExtNumberLong
//	{"$date":"<string>"}                   → KindString, ExtDate (relaxed)
//	{"$date":{"$numberLong":"<dígitos>"}}  → KindNumber, ExtDate (canonical)
//
// Qualquer outro shape restaura a posição e retorna ok=false (o caller trata
// o objeto como opaco). Só valores diretos de campos top-level são
// inspecionados; wrappers aninhados em objetos/arrays opacos ficam crus.
func (p *Parser) tryExtended(f *Field) (bool, error) {
	savePos, saveScratch := p.pos, len(p.scratch)
	restore := func() {
		p.pos = savePos
		p.scratch = p.scratch[:saveScratch]
	}
	p.pos++ // '{'
	p.skipWS()
	if p.pos >= len(p.data) || p.data[p.pos] != '"' {
		restore()
		return false, nil
	}
	key, err := p.parseString()
	if err != nil {
		restore()
		return false, nil // erro estrutural será reportado pelo skipComposite
	}
	isNumberLong := bytes.Equal(key, dollarNumberLong)
	isDate := !isNumberLong && bytes.Equal(key, dollarDate)
	if !isNumberLong && !isDate {
		restore()
		return false, nil
	}
	p.skipWS()
	if p.pos >= len(p.data) || p.data[p.pos] != ':' {
		restore()
		return false, nil
	}
	p.pos++
	p.skipWS()
	if p.pos >= len(p.data) {
		restore()
		return false, nil
	}
	switch {
	case p.data[p.pos] == '"':
		raw, err := p.parseString()
		if err != nil || !p.closeWrapper() {
			restore()
			return false, nil
		}
		if isNumberLong {
			if !int64Digits(raw) {
				restore()
				return false, nil
			}
			f.Kind, f.Ext, f.Raw = KindNumber, ExtNumberLong, raw
		} else {
			f.Kind, f.Ext, f.Raw = KindString, ExtDate, raw
		}
		return true, nil
	case isDate && p.data[p.pos] == '{':
		// canonical: valor é {"$numberLong":"..."}
		p.pos++
		p.skipWS()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			restore()
			return false, nil
		}
		inner, err := p.parseString()
		if err != nil || !bytes.Equal(inner, dollarNumberLong) {
			restore()
			return false, nil
		}
		p.skipWS()
		if p.pos >= len(p.data) || p.data[p.pos] != ':' {
			restore()
			return false, nil
		}
		p.pos++
		p.skipWS()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			restore()
			return false, nil
		}
		raw, err := p.parseString()
		if err != nil || !p.closeWrapper() || !p.closeWrapper() || !int64Digits(raw) {
			restore()
			return false, nil
		}
		f.Kind, f.Ext, f.Raw = KindNumber, ExtDate, raw
		return true, nil
	}
	restore()
	return false, nil
}

// int64Digits valida o conteúdo exigido de um $numberLong: -?[0-9]+ não
// vazio. Qualquer outra string faz o wrapper degradar para objeto opaco —
// aceitar lixo aqui viraria erro de TYPE_CONVERSION no consumidor, categoria
// errada para um shape que o design manda tratar como valor json cru.
func int64Digits(b []byte) bool {
	if len(b) > 0 && b[0] == '-' {
		b = b[1:]
	}
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// closeWrapper consome um '}' (com whitespace antes); false se não houver.
func (p *Parser) closeWrapper() bool {
	p.skipWS()
	if p.pos >= len(p.data) || p.data[p.pos] != '}' {
		return false
	}
	p.pos++
	return true
}

// skipComposite consome um objeto/array aninhado a partir de p.pos (em '{'
// ou '[') e devolve a janela crua do input. Faz apenas validação estrutural
// (balanceamento de {}/[] e strings com escapes) — o conteúdo é opaco por
// design: vira valor do tipo json sem re-serialização.
func (p *Parser) skipComposite() ([]byte, error) {
	start := p.pos
	depth := 0
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case '{', '[':
			depth++
			p.pos++
		case '}', ']':
			depth--
			p.pos++
			if depth == 0 {
				return p.data[start:p.pos], nil
			}
		case '"':
			p.pos++
			for p.pos < len(p.data) {
				if c := p.data[p.pos]; c == '"' {
					p.pos++
					break
				} else if c == '\\' {
					p.pos += 2
				} else {
					p.pos++
				}
			}
			if p.pos > len(p.data) {
				return nil, &SyntaxError{Pos: len(p.data), Msg: "string não terminada em valor aninhado"}
			}
		default:
			p.pos++
		}
	}
	return nil, &SyntaxError{Pos: p.pos, Msg: "valor aninhado não terminado"}
}

var (
	trueBytes  = []byte("true")
	falseBytes = []byte("false")
)

// parseNumber consome um número JSON (int [frac] [exp]) e devolve a janela.
func (p *Parser) parseNumber() ([]byte, error) {
	start := p.pos
	if p.data[p.pos] == '-' {
		p.pos++
	}
	intStart := p.pos
	digits := 0
	for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
		p.pos++
		digits++
	}
	if digits == 0 {
		return nil, &SyntaxError{Pos: p.pos, Msg: "número inválido"}
	}
	if digits > 1 && p.data[intStart] == '0' {
		return nil, &SyntaxError{Pos: intStart, Msg: "número inválido: leading zero"}
	}
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		p.pos++
		digits = 0
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
			digits++
		}
		if digits == 0 {
			return nil, &SyntaxError{Pos: p.pos, Msg: "número inválido: fração vazia"}
		}
	}
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		digits = 0
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
			digits++
		}
		if digits == 0 {
			return nil, &SyntaxError{Pos: p.pos, Msg: "número inválido: expoente vazio"}
		}
	}
	return p.data[start:p.pos], nil
}

// expectLiteral consome exatamente lit (true/false/null) a partir de p.pos.
func (p *Parser) expectLiteral(lit string) error {
	if len(p.data)-p.pos < len(lit) || string(p.data[p.pos:p.pos+len(lit)]) != lit {
		return &SyntaxError{Pos: p.pos, Msg: "literal inválido"}
	}
	p.pos += len(lit)
	return nil
}

// parseString consome uma string JSON (a partir da aspa de abertura) e
// devolve o conteúdo. Fast path sem escapes: janela do input.
func (p *Parser) parseString() ([]byte, error) {
	if p.pos >= len(p.data) || p.data[p.pos] != '"' {
		return nil, &SyntaxError{Pos: p.pos, Msg: "esperada string"}
	}
	p.pos++
	start := p.pos
	for p.pos < len(p.data) {
		switch c := p.data[p.pos]; {
		case c == '"':
			s := p.data[start:p.pos]
			p.pos++
			return s, nil
		case c == '\\':
			return p.parseStringSlow(start)
		case c < 0x20:
			// RFC 8259: control chars devem ser escapados.
			return nil, &SyntaxError{Pos: p.pos, Msg: "control char não escapado em string"}
		default:
			p.pos++
		}
	}
	return nil, &SyntaxError{Pos: p.pos, Msg: "string não terminada"}
}

// parseStringSlow trata strings com escapes, materializando o conteúdo no
// scratch reutilizado. p.pos aponta para a primeira '\'; o prefixo literal
// data[start:p.pos] já foi validado. A janela retornada é válida até o
// próximo Next/Reset — appends posteriores no scratch podem realocá-lo, mas
// a janela antiga continua apontando para conteúdo correto.
func (p *Parser) parseStringSlow(start int) ([]byte, error) {
	scratchStart := len(p.scratch)
	p.scratch = append(p.scratch, p.data[start:p.pos]...)
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		if c < 0x20 {
			return nil, &SyntaxError{Pos: p.pos, Msg: "control char não escapado em string"}
		}
		switch c {
		case '"':
			p.pos++
			return p.scratch[scratchStart:], nil
		case '\\':
			p.pos++
			if p.pos >= len(p.data) {
				return nil, &SyntaxError{Pos: p.pos, Msg: "escape truncado"}
			}
			esc := p.data[p.pos]
			p.pos++
			switch esc {
			case '"', '\\', '/':
				p.scratch = append(p.scratch, esc)
			case 'b':
				p.scratch = append(p.scratch, '\b')
			case 'f':
				p.scratch = append(p.scratch, '\f')
			case 'n':
				p.scratch = append(p.scratch, '\n')
			case 'r':
				p.scratch = append(p.scratch, '\r')
			case 't':
				p.scratch = append(p.scratch, '\t')
			case 'u':
				if err := p.appendUnicodeEscape(); err != nil {
					return nil, err
				}
			default:
				return nil, &SyntaxError{Pos: p.pos - 1, Msg: "escape inválido"}
			}
		default:
			p.scratch = append(p.scratch, c)
			p.pos++
		}
	}
	return nil, &SyntaxError{Pos: p.pos, Msg: "string não terminada"}
}

// appendUnicodeEscape decodifica \uXXXX (p.pos já após o 'u'), inclusive
// surrogate pairs, e appenda o rune em UTF-8 no scratch.
func (p *Parser) appendUnicodeEscape() error {
	r, err := p.parseHex4()
	if err != nil {
		return err
	}
	if utf16.IsSurrogate(r) {
		if r >= 0xDC00 {
			return &SyntaxError{Pos: p.pos, Msg: "low surrogate isolado"}
		}
		if len(p.data)-p.pos < 2 || p.data[p.pos] != '\\' || p.data[p.pos+1] != 'u' {
			return &SyntaxError{Pos: p.pos, Msg: "high surrogate sem par"}
		}
		p.pos += 2
		r2, err := p.parseHex4()
		if err != nil {
			return err
		}
		combined := utf16.DecodeRune(r, r2)
		if combined == utf8.RuneError {
			return &SyntaxError{Pos: p.pos, Msg: "surrogate pair inválido"}
		}
		r = combined
	}
	p.scratch = utf8.AppendRune(p.scratch, r)
	return nil
}

// parseHex4 consome exatamente 4 dígitos hex a partir de p.pos.
func (p *Parser) parseHex4() (rune, error) {
	if len(p.data)-p.pos < 4 {
		return 0, &SyntaxError{Pos: p.pos, Msg: "\\u truncado"}
	}
	var r rune
	for i := 0; i < 4; i++ {
		c := p.data[p.pos]
		var v rune
		switch {
		case c >= '0' && c <= '9':
			v = rune(c - '0')
		case c >= 'a' && c <= 'f':
			v = rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v = rune(c-'A') + 10
		default:
			return 0, &SyntaxError{Pos: p.pos, Msg: "dígito hex inválido em \\u"}
		}
		r = r<<4 | v
		p.pos++
	}
	return r, nil
}

// expectEOF valida que só resta whitespace após o '}' final.
func (p *Parser) expectEOF() error {
	p.skipWS()
	if p.pos != len(p.data) {
		return p.fail("conteúdo após o fim do objeto")
	}
	return nil
}

func (p *Parser) skipWS() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *Parser) fail(msg string) error {
	return p.failErr(&SyntaxError{Pos: p.pos, Msg: msg})
}

func (p *Parser) failErr(err error) error {
	p.state = stateErr
	p.err = err
	return err
}
