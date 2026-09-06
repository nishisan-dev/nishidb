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
// um `format:` chamando CompilaLayout — nunca com uma segunda lista, que
// envelheceria em silêncio e recusaria na ingestão o que aceitou no load.
//
// O que NÃO existe, e é decisão: token de fuso. `datetime` é wall-clock sem
// offset, resolvido pelo fuso do host; quem tem offset na origem declara o
// campo como `timestamp` e ganha RFC3339.

// Dominio diz para que tipo de campo o padrão está sendo compilado. É um
// enum local, e não types.FieldType, porque este pacote é folha (só stdlib
// e unsafex) e precisa continuar sendo: ele é importado pela ingestão E pelo
// binder, e é isso que impede as duas coerções de divergirem (risco R3).
type Dominio uint8

// Os domínios de formato suportados.
const (
	DominioDateTime Dominio = iota + 1 // data + hora, resolvida para instante
	DominioTime                        // hora do dia, sem data e sem fuso
)

type tokKind uint8

const (
	tokLiteral tokKind = iota + 1
	tokAno
	tokMesNum
	tokMesAbrev
	tokDia
	tokHora24
	tokHora12
	tokMinuto
	tokSegundo
	tokMillis
	tokMeridiano
)

// consomeDigitos reporta se o token lê dígitos da entrada. Só ele decide a
// ambiguidade de largura variável na compilação.
func (k tokKind) consomeDigitos() bool {
	switch k {
	case tokAno, tokMesNum, tokDia, tokHora24, tokHora12, tokMinuto, tokSegundo, tokMillis:
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
	padrao  string
	dominio Dominio
	toks    []tok
	lits    []byte
	temData bool
}

// Padrao devolve o padrão como o operador o escreveu, para mensagens de erro
// — sem ele, "literal inválido para a coluna x" não diz o que se esperava.
func (l *Layout) Padrao() string { return l.padrao }

var (
	errDtCurto     = parseErr("entrada curta demais para o formato declarado")
	errDtLiteral   = parseErr("texto fixo do formato não confere")
	errDtDigito    = parseErr("dígito esperado")
	errDtMesAbrev  = parseErr("mês abreviado desconhecido (esperado Jan..Dec)")
	errDtMeridiano = parseErr("AM/PM esperado")
	errDtTrailing  = parseErr("conteúdo após o fim do formato")
	errDtRange     = parseErr("componente de data/hora fora de range")
	errDtData      = parseErr("data inexistente")
)

// mesesEnUS são as abreviações que o java.text.DateFormat emite em locale
// en_US, em minúsculas: a comparação folda a caixa, então "Aug", "AUG" e
// "aug" casam igual. É a mesma tolerância já concedida ao T/Z do RFC3339 e
// pelo mesmo motivo — o formatador emite uma forma só, mas transformações no
// caminho (uppercase de log, normalização de ETL) mudam a caixa sem mudar o
// significado.
//
// Array e não map: 12 comparações de 3 bytes custam menos que um hash, e um
// map alocaria na inicialização do pacote sem ganho nenhum.
var mesesEnUS = [12][3]byte{
	{'j', 'a', 'n'}, {'f', 'e', 'b'}, {'m', 'a', 'r'}, {'a', 'p', 'r'},
	{'m', 'a', 'y'}, {'j', 'u', 'n'}, {'j', 'u', 'l'}, {'a', 'u', 'g'},
	{'s', 'e', 'p'}, {'o', 'c', 't'}, {'n', 'o', 'v'}, {'d', 'e', 'c'},
}

// TokensSuportados descreve o vocabulário para a mensagem de recusa. Quem
// erra um token precisa ver a lista, não só "token inválido".
func TokensSuportados() string {
	return "yyyy MM M MMM dd d HH H hh h mm ss SSS a, texto fixo entre aspas simples"
}

// CompilaLayout traduz o padrão declarado no YAML para o programa de tokens.
// Todo erro de formato é reportado AQUI, no load, e não na ingestão: um
// padrão inválido que só falhasse por evento produziria um dataset que não
// ingere nada sob log-and-skip — só WARNs, sem nada apontando para a causa.
func CompilaLayout(padrao string, d Dominio) (*Layout, error) {
	if d != DominioDateTime && d != DominioTime {
		return nil, fmt.Errorf("domínio de formato desconhecido: %d", d)
	}
	if padrao == "" {
		return nil, fmt.Errorf("format vazio (tokens: %s)", TokensSuportados())
	}
	l := &Layout{padrao: padrao, dominio: d}
	var vistos uint16
	marca := func(k tokKind) error {
		bit := uint16(1) << k
		if vistos&bit != 0 {
			return fmt.Errorf("format %q: componente %q aparece mais de uma vez", padrao, nomeDoToken(k))
		}
		vistos |= bit
		return nil
	}

	for i := 0; i < len(padrao); {
		c := padrao[i]
		switch {
		case c == '\'':
			fim, lit, err := leEscapado(padrao, i)
			if err != nil {
				return nil, err
			}
			l.acrescentaLiteral(lit)
			i = fim

		case ehLetraASCII(c):
			j := i
			for j < len(padrao) && padrao[j] == c {
				j++
			}
			run := padrao[i:j]
			k, minDig, maxDig, ok := tokenDaRun(run)
			if !ok {
				return nil, fmt.Errorf("format %q: token %q não é suportado (tokens: %s); "+
					"texto fixo com letras precisa de aspas simples, como 'T'",
					padrao, run, TokensSuportados())
			}
			if err := marca(k); err != nil {
				return nil, err
			}
			l.toks = append(l.toks, tok{kind: k, minDig: minDig, maxDig: maxDig})
			i = j

		default:
			// Bloco de caracteres não-letra vira um literal só: menos tokens
			// para interpretar por valor no hot path.
			j := i
			for j < len(padrao) && !ehLetraASCII(padrao[j]) && padrao[j] != '\'' {
				j++
			}
			l.acrescentaLiteral(padrao[i:j])
			i = j
		}
	}

	if err := l.validaAmbiguidade(); err != nil {
		return nil, err
	}
	if err := l.validaDominio(vistos); err != nil {
		return nil, err
	}
	l.temData = vistos&bitDe(tokAno) != 0
	return l, nil
}

func bitDe(k tokKind) uint16 { return uint16(1) << k }

func ehLetraASCII(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }

func (l *Layout) acrescentaLiteral(s string) {
	if s == "" {
		return
	}
	off := len(l.lits)
	l.lits = append(l.lits, s...)
	l.toks = append(l.toks, tok{kind: tokLiteral, off: uint16(off), n: uint16(len(s))})
}

// leEscapado consome 'texto' a partir de padrao[i] == '\”. Duas aspas
// seguidas (”) são uma aspa literal, como no CLDR.
func leEscapado(padrao string, i int) (int, string, error) {
	if i+1 < len(padrao) && padrao[i+1] == '\'' {
		return i + 2, "'", nil
	}
	fim := strings.IndexByte(padrao[i+1:], '\'')
	if fim < 0 {
		return 0, "", fmt.Errorf("format %q: aspa simples aberta e não fechada", padrao)
	}
	return i + 1 + fim + 1, padrao[i+1 : i+1+fim], nil
}

func tokenDaRun(run string) (k tokKind, minDig, maxDig uint8, ok bool) {
	switch run {
	case "yyyy":
		return tokAno, 4, 4, true
	case "MM":
		return tokMesNum, 2, 2, true
	case "M":
		return tokMesNum, 1, 2, true
	case "MMM":
		return tokMesAbrev, 0, 0, true
	case "dd":
		return tokDia, 2, 2, true
	case "d":
		return tokDia, 1, 2, true
	case "HH":
		return tokHora24, 2, 2, true
	case "H":
		return tokHora24, 1, 2, true
	case "hh":
		return tokHora12, 2, 2, true
	case "h":
		return tokHora12, 1, 2, true
	case "mm":
		return tokMinuto, 2, 2, true
	case "ss":
		return tokSegundo, 2, 2, true
	case "SSS":
		return tokMillis, 3, 3, true
	case "a":
		return tokMeridiano, 0, 0, true
	}
	return 0, 0, 0, false
}

func nomeDoToken(k tokKind) string {
	switch k {
	case tokAno:
		return "ano"
	case tokMesNum, tokMesAbrev:
		return "mês"
	case tokDia:
		return "dia"
	case tokHora24, tokHora12:
		return "hora"
	case tokMinuto:
		return "minuto"
	case tokSegundo:
		return "segundo"
	case tokMillis:
		return "milissegundo"
	case tokMeridiano:
		return "meridiano"
	}
	return "literal"
}

// validaAmbiguidade recusa um token numérico de largura variável colado em
// outro que também lê dígitos: "yyyyMd" não tem leitura única, e escolher uma
// por conta própria produziria datas erradas em silêncio.
func (l *Layout) validaAmbiguidade() error {
	for i := 0; i+1 < len(l.toks); i++ {
		a, b := l.toks[i], l.toks[i+1]
		if a.kind.consomeDigitos() && a.maxDig > a.minDig && b.kind.consomeDigitos() {
			return fmt.Errorf("format %q: %q de largura variável colado em %q é ambíguo; "+
				"separe com texto fixo ou use a forma de largura fixa",
				l.padrao, nomeDoToken(a.kind), nomeDoToken(b.kind))
		}
	}
	return nil
}

func (l *Layout) validaDominio(vistos uint16) error {
	tem := func(ks ...tokKind) bool {
		for _, k := range ks {
			if vistos&bitDe(k) != 0 {
				return true
			}
		}
		return false
	}
	temData := tem(tokAno, tokMesNum, tokMesAbrev, tokDia)
	temHora12, temHora24 := tem(tokHora12), tem(tokHora24)
	temMeridiano := tem(tokMeridiano)

	// O relógio de 12h sem AM/PM é ambíguo entre manhã e noite, e o de 24h
	// com AM/PM é contraditório. Nos dois casos o dado sai errado sem sinal.
	if temHora12 && !temMeridiano {
		return fmt.Errorf("format %q: hh/h exige o token a (AM/PM); sem ele 08:54 é ambíguo", l.padrao)
	}
	if temHora24 && temMeridiano {
		return fmt.Errorf("format %q: HH/H é relógio de 24h e não admite o token a (AM/PM)", l.padrao)
	}
	if temMeridiano && !temHora12 {
		return fmt.Errorf("format %q: o token a (AM/PM) exige hh ou h", l.padrao)
	}

	switch l.dominio {
	case DominioTime:
		if temData {
			return fmt.Errorf("format %q: time é hora do dia e não admite tokens de data (yyyy, MM, MMM, dd)", l.padrao)
		}
		if !temHora12 && !temHora24 {
			return fmt.Errorf("format %q: time exige hora (HH, H, hh ou h)", l.padrao)
		}
		if !tem(tokMinuto) {
			return fmt.Errorf("format %q: time exige minuto (mm)", l.padrao)
		}
	case DominioDateTime:
		faltando := ""
		switch {
		case !tem(tokAno):
			faltando = "ano (yyyy)"
		case !tem(tokMesNum, tokMesAbrev):
			faltando = "mês (MM, M ou MMM)"
		case !tem(tokDia):
			faltando = "dia (dd ou d)"
		case !temHora12 && !temHora24:
			faltando = "hora (HH, H, hh ou h)"
		case !tem(tokMinuto):
			faltando = "minuto (mm)"
		}
		if faltando != "" {
			return fmt.Errorf("format %q: datetime exige %s", l.padrao, faltando)
		}
	}
	return nil
}

// campos são os componentes lidos de uma entrada. Struct de valor, na pilha:
// nada aqui escapa para o heap.
type campos struct {
	ano, mes, dia    int
	hora, min, seg   int
	millis           int
	pm, temMeridiano bool
}

// interpreta roda o programa de tokens sobre raw. Devolve erro estático em
// qualquer divergência — o valor de entrada não entra na mensagem; quem o
// interpola (e trunca) é o consumidor.
func (l *Layout) interpreta(raw []byte) (campos, error) {
	c := campos{ano: 1970, mes: 1, dia: 1}
	pos := 0
	for _, tk := range l.toks {
		switch tk.kind {
		case tokLiteral:
			lit := l.lits[tk.off : tk.off+tk.n]
			if pos+len(lit) > len(raw) {
				return c, errDtCurto
			}
			for i := range lit {
				if raw[pos+i] != lit[i] {
					return c, errDtLiteral
				}
			}
			pos += len(lit)

		case tokMesAbrev:
			if pos+3 > len(raw) {
				return c, errDtCurto
			}
			mes, ok := mesAbreviadoEnUS(raw[pos : pos+3])
			if !ok {
				return c, errDtMesAbrev
			}
			c.mes = mes
			pos += 3

		case tokMeridiano:
			if pos+2 > len(raw) {
				return c, errDtCurto
			}
			switch {
			case (raw[pos]|0x20) == 'a' && (raw[pos+1]|0x20) == 'm':
			case (raw[pos]|0x20) == 'p' && (raw[pos+1]|0x20) == 'm':
				c.pm = true
			default:
				return c, errDtMeridiano
			}
			c.temMeridiano = true
			pos += 2

		default:
			v, n, err := leDigitos(raw[pos:], int(tk.minDig), int(tk.maxDig))
			if err != nil {
				return c, err
			}
			pos += n
			switch tk.kind {
			case tokAno:
				c.ano = v
			case tokMesNum:
				c.mes = v
			case tokDia:
				c.dia = v
			case tokHora24, tokHora12:
				c.hora = v
			case tokMinuto:
				c.min = v
			case tokSegundo:
				c.seg = v
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

// leDigitos lê entre min e max dígitos decimais, o máximo que houver.
func leDigitos(b []byte, minDig, maxDig int) (v, n int, err error) {
	for n < maxDig && n < len(b) && b[n] >= '0' && b[n] <= '9' {
		v = v*10 + int(b[n]-'0')
		n++
	}
	if n < minDig {
		if n == len(b) {
			return 0, 0, errDtCurto
		}
		return 0, 0, errDtDigito
	}
	return v, n, nil
}

func mesAbreviadoEnUS(b []byte) (int, bool) {
	c0, c1, c2 := b[0]|0x20, b[1]|0x20, b[2]|0x20
	for i := range mesesEnUS {
		m := &mesesEnUS[i]
		if m[0] == c0 && m[1] == c1 && m[2] == c2 {
			return i + 1, true
		}
	}
	return 0, false
}

// valida confere as faixas de calendário e resolve o relógio de 12h.
func (c *campos) valida(comData bool) error {
	if c.min > 59 || c.seg > 59 || c.millis > 999 {
		return errDtRange
	}
	if c.temMeridiano {
		// 12 AM é 00 e 12 PM é 12 — a única regra deste formato que não sai
		// certa por acidente. %12 zera o 12 e preserva 1..11; o PM soma 12.
		if c.hora < 1 || c.hora > 12 {
			return errDtRange
		}
		c.hora %= 12
		if c.pm {
			c.hora += 12
		}
	} else if c.hora > 23 {
		return errDtRange
	}
	if comData && (c.mes < 1 || c.mes > 12 || c.dia < 1 || c.dia > 31) {
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
	c, err := l.interpreta(raw)
	if err != nil {
		return 0, err
	}
	if err := c.valida(true); err != nil {
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
	prova := time.Date(c.ano, time.Month(c.mes), c.dia, 0, 0, 0, 0, time.UTC)
	if y, m, d := prova.Date(); y != c.ano || int(m) != c.mes || d != c.dia {
		return 0, errDtData
	}
	return time.Date(c.ano, time.Month(c.mes), c.dia, c.hora, c.min, c.seg,
		c.millis*int(time.Millisecond), loc).UnixMilli(), nil
}

// TimeOfDay converte a entrada para millis desde a meia-noite, em
// [0, 86_400_000).
//
// Não recebe fuso e não chama time.Date: hora do dia não é instante, então
// não há DST, não há data e não há nada a resolver. É a assimetria que
// justifica `time` ser um tipo separado, e não um datetime capado.
func (l *Layout) TimeOfDay(raw []byte) (int64, error) {
	c, err := l.interpreta(raw)
	if err != nil {
		return 0, err
	}
	if err := c.valida(false); err != nil {
		return 0, err
	}
	return int64(c.hora)*3600_000 + int64(c.min)*60_000 + int64(c.seg)*1000 + int64(c.millis), nil
}
