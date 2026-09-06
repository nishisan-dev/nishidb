package typeparse

import (
	"fmt"
	"strings"
	"time"
)

// Este arquivo implementa os tipos `datetime` e `time` do schema: um padrão
// declarado no YAML é COMPILADO uma vez, no load, e vira um programa linear
// de tokens que o hot path interpreta sobre []byte sem alocar.
//
// O vocabulário de tokens é FECHADO e mora só aqui. O pacote schema valida
// um `format:` chamando CompileLayout — nunca com uma segunda lista, que
// envelheceria em silêncio e recusaria na ingestão o que aceitou no load.
//
// O que NÃO existe, e é decisão: token de fuso. `datetime` é wall-clock sem
// offset, resolvido pelo fuso do host; quem tem offset na origem declara o
// campo como `timestamp` e ganha RFC3339.

// Domain diz para que tipo de campo o padrão está sendo compilado. É um
// enum local, e não types.FieldType, porque este pacote é folha (só stdlib
// e unsafex) e precisa continuar sendo: ele é importado pela ingestão E pelo
// binder, e é isso que impede as duas coerções de divergirem (risco R3).
type Domain uint8

// Os domínios de formato suportados.
const (
	DomainDateTime Domain = iota + 1 // data + hora, resolvida para instante
	DomainTime                       // hora do dia, sem data e sem fuso
)

type tokKind uint8

const (
	tokLiteral tokKind = iota + 1
	tokYear
	tokMonthNum
	tokMonthAbbr
	tokDay
	tokHour24
	tokHour12
	tokMinute
	tokSecond
	tokMillis
	tokMeridiem
)

// consumesDigits reporta se o token lê dígitos da entrada. Só ele decide a
// ambiguidade de largura variável na compilação.
func (k tokKind) consumesDigits() bool {
	switch k {
	case tokYear, tokMonthNum, tokDay, tokHour24, tokHour12, tokMinute, tokSecond, tokMillis:
		return true
	}
	return false
}

type tok struct {
	kind           tokKind
	minDig, maxDig uint8  // dígitos aceitos, para os tokens numéricos
	off, n         uint16 // janela em Layout.lits, para tokLiteral
}

// Layout é um padrão compilado. Construído UMA vez, no load da spec; o parse
// é uma interpretação que não aloca e não retém a entrada.
type Layout struct {
	pattern string
	domain  Domain
	toks    []tok
	lits    []byte
	hasDate bool
}

// Pattern devolve o padrão como o operador o escreveu, para mensagens de erro
// — sem ele, "literal inválido para a coluna x" não diz o que se esperava.
func (l *Layout) Pattern() string { return l.pattern }

var (
	errDtShort     = parseError("entrada curta demais para o formato declarado")
	errDtLiteral   = parseError("texto fixo do formato não confere")
	errDtDigit     = parseError("dígito esperado")
	errDtMonthAbbr = parseError("mês abreviado desconhecido (esperado Jan..Dec)")
	errDtMeridiem  = parseError("AM/PM esperado")
	errDtTrailing  = parseError("conteúdo após o fim do formato")
	errDtRange     = parseError("componente de data/hora fora de range")
	errDtDate      = parseError("data inexistente")
)

// monthsEnUS são as abreviações que o java.text.DateFormat emite em locale
// en_US, em minúsculas: a comparação folda a caixa, então "Aug", "AUG" e
// "aug" casam igual. É a mesma tolerância já concedida ao T/Z do RFC3339 e
// pelo mesmo motivo — o formatador emite uma forma só, mas transformações no
// caminho (uppercase de log, normalização de ETL) mudam a caixa sem mudar o
// significado.
//
// Array e não map: 12 comparações de 3 bytes custam menos que um hash, e um
// map alocaria na inicialização do pacote sem ganho nenhum.
var monthsEnUS = [12][3]byte{
	{'j', 'a', 'n'}, {'f', 'e', 'b'}, {'m', 'a', 'r'}, {'a', 'p', 'r'},
	{'m', 'a', 'y'}, {'j', 'u', 'n'}, {'j', 'u', 'l'}, {'a', 'u', 'g'},
	{'s', 'e', 'p'}, {'o', 'c', 't'}, {'n', 'o', 'v'}, {'d', 'e', 'c'},
}

// SupportedTokens descreve o vocabulário para a mensagem de recusa. Quem
// erra um token precisa ver a lista, não só "token inválido".
func SupportedTokens() string {
	return "yyyy MM M MMM dd d HH H hh h mm ss SSS a, texto fixo entre aspas simples"
}

// CompileLayout traduz o padrão declarado no YAML para o programa de tokens.
// Todo erro de formato é reportado AQUI, no load, e não na ingestão: um
// padrão inválido que só falhasse por evento produziria um dataset que não
// ingere nada sob log-and-skip — só WARNs, sem nada apontando para a causa.
func CompileLayout(pattern string, d Domain) (*Layout, error) {
	if d != DomainDateTime && d != DomainTime {
		return nil, fmt.Errorf("domínio de formato desconhecido: %d", d)
	}
	if pattern == "" {
		return nil, fmt.Errorf("format vazio (tokens: %s)", SupportedTokens())
	}
	l := &Layout{pattern: pattern, domain: d}
	var seen uint16
	mark := func(k tokKind) error {
		bit := uint16(1) << k
		if seen&bit != 0 {
			return fmt.Errorf("format %q: componente %q aparece mais de uma vez", pattern, componentName(k))
		}
		seen |= bit
		return nil
	}

	for i := 0; i < len(pattern); {
		c := pattern[i]
		switch {
		case c == '\'':
			end, lit, err := readQuoted(pattern, i)
			if err != nil {
				return nil, err
			}
			l.appendLiteral(lit)
			i = end

		case isASCIILetter(c):
			j := i
			for j < len(pattern) && pattern[j] == c {
				j++
			}
			run := pattern[i:j]
			k, minDig, maxDig, ok := tokenForRun(run)
			if !ok {
				return nil, fmt.Errorf("format %q: token %q não é suportado (tokens: %s); "+
					"texto fixo com letras precisa de aspas simples, como 'T'",
					pattern, run, SupportedTokens())
			}
			if err := mark(k); err != nil {
				return nil, err
			}
			l.toks = append(l.toks, tok{kind: k, minDig: minDig, maxDig: maxDig})
			i = j

		default:
			// Bloco de caracteres não-letra vira um literal só: menos tokens
			// para interpretar por valor no hot path.
			j := i
			for j < len(pattern) && !isASCIILetter(pattern[j]) && pattern[j] != '\'' {
				j++
			}
			l.appendLiteral(pattern[i:j])
			i = j
		}
	}

	if err := l.validateAmbiguity(); err != nil {
		return nil, err
	}
	if err := l.validateDomain(seen); err != nil {
		return nil, err
	}
	l.hasDate = seen&bitOf(tokYear) != 0
	return l, nil
}

func bitOf(k tokKind) uint16 { return uint16(1) << k }

func isASCIILetter(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

func (l *Layout) appendLiteral(s string) {
	if s == "" {
		return
	}
	off := len(l.lits)
	l.lits = append(l.lits, s...)
	l.toks = append(l.toks, tok{kind: tokLiteral, off: uint16(off), n: uint16(len(s))})
}

// readQuoted consome 'texto' a partir de padrao[i] == '\”. Duas aspas
// seguidas (”) são uma aspa literal, como no CLDR.
func readQuoted(pattern string, i int) (int, string, error) {
	if i+1 < len(pattern) && pattern[i+1] == '\'' {
		return i + 2, "'", nil
	}
	end := strings.IndexByte(pattern[i+1:], '\'')
	if end < 0 {
		return 0, "", fmt.Errorf("format %q: aspa simples aberta e não fechada", pattern)
	}
	return i + 1 + end + 1, pattern[i+1 : i+1+end], nil
}

func tokenForRun(run string) (k tokKind, minDig, maxDig uint8, ok bool) {
	switch run {
	case "yyyy":
		return tokYear, 4, 4, true
	case "MM":
		return tokMonthNum, 2, 2, true
	case "M":
		return tokMonthNum, 1, 2, true
	case "MMM":
		return tokMonthAbbr, 0, 0, true
	case "dd":
		return tokDay, 2, 2, true
	case "d":
		return tokDay, 1, 2, true
	case "HH":
		return tokHour24, 2, 2, true
	case "H":
		return tokHour24, 1, 2, true
	case "hh":
		return tokHour12, 2, 2, true
	case "h":
		return tokHour12, 1, 2, true
	case "mm":
		return tokMinute, 2, 2, true
	case "ss":
		return tokSecond, 2, 2, true
	case "SSS":
		return tokMillis, 3, 3, true
	case "a":
		return tokMeridiem, 0, 0, true
	}
	return 0, 0, 0, false
}

func componentName(k tokKind) string {
	switch k {
	case tokYear:
		return "ano"
	case tokMonthNum, tokMonthAbbr:
		return "mês"
	case tokDay:
		return "dia"
	case tokHour24, tokHour12:
		return "hora"
	case tokMinute:
		return "minuto"
	case tokSecond:
		return "segundo"
	case tokMillis:
		return "milissegundo"
	case tokMeridiem:
		return "meridiano"
	}
	return "literal"
}

// validateAmbiguity recusa um token numérico de largura variável colado em
// outro que também lê dígitos: "yyyyMd" não tem leitura única, e escolher uma
// por conta própria produziria datas erradas em silêncio.
func (l *Layout) validateAmbiguity() error {
	for i := 0; i+1 < len(l.toks); i++ {
		a, b := l.toks[i], l.toks[i+1]
		if a.kind.consumesDigits() && a.maxDig > a.minDig && b.kind.consumesDigits() {
			return fmt.Errorf("format %q: %q de largura variável colado em %q é ambíguo; "+
				"separe com texto fixo ou use a forma de largura fixa",
				l.pattern, componentName(a.kind), componentName(b.kind))
		}
	}
	return nil
}

func (l *Layout) validateDomain(seen uint16) error {
	has := func(ks ...tokKind) bool {
		for _, k := range ks {
			if seen&bitOf(k) != 0 {
				return true
			}
		}
		return false
	}
	hasAnyDateToken := has(tokYear, tokMonthNum, tokMonthAbbr, tokDay)
	hasHour12, hasHour24 := has(tokHour12), has(tokHour24)
	hasMeridiem := has(tokMeridiem)

	// O relógio de 12h sem AM/PM é ambíguo entre manhã e noite, e o de 24h
	// com AM/PM é contraditório. Nos dois casos o dado sai errado sem sinal.
	if hasHour12 && !hasMeridiem {
		return fmt.Errorf("format %q: hh/h exige o token a (AM/PM); sem ele 08:54 é ambíguo", l.pattern)
	}
	if hasHour24 && hasMeridiem {
		return fmt.Errorf("format %q: HH/H é relógio de 24h e não admite o token a (AM/PM)", l.pattern)
	}
	if hasMeridiem && !hasHour12 {
		return fmt.Errorf("format %q: o token a (AM/PM) exige hh ou h", l.pattern)
	}

	switch l.domain {
	case DomainTime:
		if hasAnyDateToken {
			return fmt.Errorf("format %q: time é hora do dia e não admite tokens de data (yyyy, MM, MMM, dd)", l.pattern)
		}
		if !hasHour12 && !hasHour24 {
			return fmt.Errorf("format %q: time exige hora (HH, H, hh ou h)", l.pattern)
		}
		if !has(tokMinute) {
			return fmt.Errorf("format %q: time exige minuto (mm)", l.pattern)
		}
	case DomainDateTime:
		missing := ""
		switch {
		case !has(tokYear):
			missing = "ano (yyyy)"
		case !has(tokMonthNum, tokMonthAbbr):
			missing = "mês (MM, M ou MMM)"
		case !has(tokDay):
			missing = "dia (dd ou d)"
		case !hasHour12 && !hasHour24:
			missing = "hora (HH, H, hh ou h)"
		case !has(tokMinute):
			missing = "minuto (mm)"
		}
		if missing != "" {
			return fmt.Errorf("format %q: datetime exige %s", l.pattern, missing)
		}
	}
	return nil
}

// components são os componentes lidos de uma entrada. Struct de valor, na pilha:
// nada aqui escapa para o heap.
type components struct {
	year, month, day int
	hour, min, sec   int
	millis           int
	pm, hasMeridiem  bool
}

// interpret roda o programa de tokens sobre raw. Devolve erro estático em
// qualquer divergência — o valor de entrada não entra na mensagem; quem o
// interpola (e trunca) é o consumidor.
func (l *Layout) interpret(raw []byte) (components, error) {
	c := components{year: 1970, month: 1, day: 1}
	pos := 0
	for _, tk := range l.toks {
		switch tk.kind {
		case tokLiteral:
			lit := l.lits[tk.off : tk.off+tk.n]
			if pos+len(lit) > len(raw) {
				return c, errDtShort
			}
			for i := range lit {
				if raw[pos+i] != lit[i] {
					return c, errDtLiteral
				}
			}
			pos += len(lit)

		case tokMonthAbbr:
			if pos+3 > len(raw) {
				return c, errDtShort
			}
			month, ok := monthAbbrEnUS(raw[pos : pos+3])
			if !ok {
				return c, errDtMonthAbbr
			}
			c.month = month
			pos += 3

		case tokMeridiem:
			if pos+2 > len(raw) {
				return c, errDtShort
			}
			switch {
			case (raw[pos]|0x20) == 'a' && (raw[pos+1]|0x20) == 'm':
			case (raw[pos]|0x20) == 'p' && (raw[pos+1]|0x20) == 'm':
				c.pm = true
			default:
				return c, errDtMeridiem
			}
			c.hasMeridiem = true
			pos += 2

		default:
			v, n, err := readDigits(raw[pos:], int(tk.minDig), int(tk.maxDig))
			if err != nil {
				return c, err
			}
			pos += n
			switch tk.kind {
			case tokYear:
				c.year = v
			case tokMonthNum:
				c.month = v
			case tokDay:
				c.day = v
			case tokHour24, tokHour12:
				c.hour = v
			case tokMinute:
				c.min = v
			case tokSecond:
				c.sec = v
			case tokMillis:
				c.millis = v
			}
		}
	}
	if pos != len(raw) {
		// Pega, entre outras coisas, um offset colado no fim ("... PM -03:00").
		// Este formato não tem fuso, e ignorá-lo em silêncio daria o instante
		// errado sem nenhum sinal.
		return c, errDtTrailing
	}
	return c, nil
}

// readDigits lê entre min e max dígitos decimais, o máximo que houver.
func readDigits(b []byte, minDig, maxDig int) (v, n int, err error) {
	for n < maxDig && n < len(b) && b[n] >= '0' && b[n] <= '9' {
		v = v*10 + int(b[n]-'0')
		n++
	}
	if n < minDig {
		if n == len(b) {
			return 0, 0, errDtShort
		}
		return 0, 0, errDtDigit
	}
	return v, n, nil
}

func monthAbbrEnUS(b []byte) (int, bool) {
	c0, c1, c2 := b[0]|0x20, b[1]|0x20, b[2]|0x20
	for i := range monthsEnUS {
		m := &monthsEnUS[i]
		if m[0] == c0 && m[1] == c1 && m[2] == c2 {
			return i + 1, true
		}
	}
	return 0, false
}

// validate confere as faixas de calendário e resolve o relógio de 12h.
func (c *components) validate(withDate bool) error {
	if c.min > 59 || c.sec > 59 || c.millis > 999 {
		return errDtRange
	}
	if c.hasMeridiem {
		// 12 AM é 00 e 12 PM é 12 — a única regra deste formato que não sai
		// certa por acidente. %12 zera o 12 e preserva 1..11; o PM soma 12.
		if c.hour < 1 || c.hour > 12 {
			return errDtRange
		}
		c.hour %= 12
		if c.pm {
			c.hour += 12
		}
	} else if c.hour > 23 {
		return errDtRange
	}
	if withDate && (c.month < 1 || c.month > 12 || c.day < 1 || c.day > 31) {
		return errDtRange
	}
	return nil
}

// DateTime converte a entrada para epoch millis, resolvendo o instante em loc.
//
// loc é PARÂMETRO, e não time.Local capturado aqui dentro, por duas razões:
// um teste precisa fixar o fuso sem mexer num global do processo (time.Local
// é resolvido uma vez, na primeira leitura, e um Setenv depois disso não o
// muda), e a assinatura deixa VISÍVEL que o resultado depende do fuso — a
// propriedade mais fácil de esquecer deste tipo. No produto, os dois
// chamadores passam time.Local, e só ele.
func (l *Layout) DateTime(raw []byte, loc *time.Location) (int64, error) {
	c, err := l.interpret(raw)
	if err != nil {
		return 0, err
	}
	if err := c.validate(true); err != nil {
		return 0, err
	}
	// A validade da DATA é conferida em UTC, não em loc, e isso não é
	// preciosismo: time.Date normaliza componente inválido (30/02 vira 02/03)
	// e a validação canônica é comparar o resultado com a entrada — mas em loc
	// essa comparação tem falso positivo. Em America/Santiago a virada de DST
	// de 1990 recua a meia-noite, e time.Date(1990-09-16 00:30, loc) devolve
	// 1990-09-15 23:30: o round-trip em loc recusaria como "data inexistente"
	// uma data de calendário que existe. UTC não tem DST, então a checagem
	// fica pura.
	//
	// DST no INSTANTE, por outro lado, é aceito como time.Date o resolve, e
	// isso é decisão, não omissão:
	//
	//   - hora AMBÍGUA (recuo de relógio, a que acontece duas vezes):
	//     escolhe-se a primeira ocorrência. Uma hora por ano fica deslocada e
	//     não há como acertar — a informação que desempataria não está no dado;
	//   - hora INEXISTENTE (adiantamento): normaliza para frente.
	//
	// Recusar as duas foi considerado e descartado: transformaria uma hora por
	// ano de saída legítima do produtor em TYPE_CONVERSION sob log-and-skip,
	// isto é, descarte silencioso — pior que gravar o instante determinístico.
	// O que importa é ingestão e binder chegarem ao MESMO número para o mesmo
	// texto (risco R3), e time.Date garante isso por ser determinístico.
	// Quem precisa de instante sem ambiguidade declara o campo como timestamp
	// e manda offset na origem.
	roundTrip := time.Date(c.year, time.Month(c.month), c.day, 0, 0, 0, 0, time.UTC)
	if y, m, d := roundTrip.Date(); y != c.year || int(m) != c.month || d != c.day {
		return 0, errDtDate
	}
	return time.Date(c.year, time.Month(c.month), c.day, c.hour, c.min, c.sec,
		c.millis*int(time.Millisecond), loc).UnixMilli(), nil
}

// TimeOfDay converte a entrada para millis desde a meia-noite, em
// [0, 86_400_000).
//
// Não recebe fuso e não chama time.Date: hora do dia não é instante, então
// não há DST, não há data e não há nada a resolver. É a assimetria que
// justifica `time` ser um tipo separado, e não um datetime capado.
func (l *Layout) TimeOfDay(raw []byte) (int64, error) {
	c, err := l.interpret(raw)
	if err != nil {
		return 0, err
	}
	if err := c.validate(false); err != nil {
		return 0, err
	}
	return int64(c.hour)*3600_000 + int64(c.min)*60_000 + int64(c.sec)*1000 + int64(c.millis), nil
}
