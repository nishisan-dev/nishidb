package executor

import (
	"encoding/base64"
	"math"
	"net/netip"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/nishisan-dev/nishidb/internal/types"
)

// Serialização NDJSON: uma row = um objeto JSON = uma linha terminada em LF.
//
// O arquivo NÃO importa fmt nem encoding/json de propósito (há teste que
// verifica): os dois trazem reflexão e formatação genérica para o caminho
// quente, e o gate de zero alocação por linha depende de não haver nenhuma das
// duas. encoding/json aparece só nos testes, como oráculo.

// AppendRow serializa row como um objeto JSON seguido de '\n' (LF, nunca
// CRLF) e devolve dst crescido.
//
// NÃO retém nada de row: tudo é copiado. É isso que permite ao executor
// invalidar a Row no próximo Next.
//
// A ordem das chaves é a da PROJEÇÃO, literalmente — inclusive se ela repetir
// uma coluna (`SELECT a, a` produz a chave duas vezes). Deduplicar é decisão
// do binder, não do encoder.
func AppendRow(dst []byte, row Row) []byte {
	dst = append(dst, '{')
	for i, c := range row.Cols() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONStringS(dst, c.Name)
		dst = append(dst, ':')
		// Null e Val.Type == TypeNull são duas representações do mesmo estado.
		// Checar as duas é barato e erra para o lado seguro: se o executor
		// setar uma e esquecer a outra, sai null em vez de lixo.
		if c.Null || c.Val.Type == types.TypeNull {
			dst = append(dst, "null"...)
			continue
		}
		dst = AppendValue(dst, c.Val)
	}
	return append(dst, '}', '\n')
}

// AppendValue serializa UM valor JSON (sem nome, sem vírgula).
//
// Valor fisicamente malformado — largura errada, bits de prefixo fora de
// faixa, tag desconhecida, double não-finito, json vazio — vira `null`. Nunca
// texto de erro (o netip produziria "invalid IP", que chegaria ao cliente como
// um dado plausível), nunca panic.
//
// PARA O PRÓXIMO DEPURADOR: se um null inexplicável aparecer num campo que
// deveria ter valor, o bug NÃO está aqui. Estes estados são inalcançáveis pelo
// caminho normal — o FieldReader falha com ErrCorrupt antes de produzir um
// Value de largura errada. Procure no storage, no executor, ou num Value
// construído à mão em teste.
func AppendValue(dst []byte, v types.Value) []byte {
	switch v.Type {
	case types.TypeInt64:
		return strconv.AppendInt(dst, v.I64, 10)

	case types.TypeDouble:
		f := math.Float64frombits(uint64(v.I64))
		// O guard vem ANTES do AppendFloat: com 'g' os não-finitos sairiam
		// como NaN/+Inf/-Inf, que não são JSON válido.
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return append(dst, "null"...)
		}
		// 'g' é a forma mais curta que faz round-trip, e diverge do
		// encoding/json na GRAFIA (ele usa 'f' abaixo de 1e21 e 'e' fora, com
		// expoente sem zero à esquerda) em cerca de um quarto dos doubles: nós
		// emitimos 1e-07 onde ele emitiria 1e-7. As duas formas são JSON válido
		// e decodificam para o MESMO float64 — a divergência é textual, e só
		// importa para quem compara a saída byte a byte com o encoder padrão.
		return strconv.AppendFloat(dst, f, 'g', -1, 64)

	case types.TypeBool:
		if v.I64 != 0 {
			return append(dst, "true"...)
		}
		return append(dst, "false"...)

	case types.TypeTimestamp, types.TypeDateTime:
		// datetime sai como INSTANTE UTC, byte a byte igual ao timestamp,
		// porque é o que ele é depois da ingestão: o texto de relógio de
		// parede foi consumido para resolver o instante e não é preservado.
		//
		// As duas alternativas foram descartadas. Ecoar o format declarado
		// obrigaria AppendValue a receber schema, e a assinatura pura
		// (dst, types.Value) é o que mantém este arquivo sem fmt e sem
		// encoding/json. Renderizar em time.Local com offset faria o MESMO
		// dataset servido de dois hosts produzir bytes diferentes — o que é
		// inaceitável num formato de fio.
		return appendTimestamp(dst, v.I64)

	case types.TypeTime:
		// Fora do dia é valor fisicamente malformado, e a política deste
		// arquivo é null — nunca texto de erro, que chegaria ao cliente como
		// um dado plausível. Inalcançável pelo caminho normal: a ingestão e o
		// binder recusam antes.
		if v.I64 < 0 || v.I64 >= types.MillisPerDay {
			return append(dst, "null"...)
		}
		return appendHoraOfDia(dst, v.I64)

	case types.TypeIPv4:
		if len(v.Bytes) != 4 {
			return append(dst, "null"...)
		}
		dst = append(dst, '"')
		dst = netip.AddrFrom4([4]byte(v.Bytes)).AppendTo(dst)
		return append(dst, '"')

	case types.TypeIPv6:
		if len(v.Bytes) != 16 {
			return append(dst, "null"...)
		}
		dst = append(dst, '"')
		dst = netip.AddrFrom16([16]byte(v.Bytes)).AppendTo(dst)
		return append(dst, '"')

	case types.TypeIPv4Prefix:
		if len(v.Bytes) != 5 || v.Bytes[4] > 32 {
			return append(dst, "null"...)
		}
		dst = append(dst, '"')
		// SEM Masked(): a ingestão usa netip.ParsePrefix, que PRESERVA os host
		// bits. Mascarar aqui faria 10.0.0.1/24 virar 10.0.0.0/24 — perda
		// silenciosa de dado no round-trip.
		dst = netip.PrefixFrom(netip.AddrFrom4([4]byte(v.Bytes[:4])), int(v.Bytes[4])).AppendTo(dst)
		return append(dst, '"')

	case types.TypeIPv6Prefix:
		if len(v.Bytes) != 17 || v.Bytes[16] > 128 {
			return append(dst, "null"...)
		}
		dst = append(dst, '"')
		dst = netip.PrefixFrom(netip.AddrFrom16([16]byte(v.Bytes[:16])), int(v.Bytes[16])).AppendTo(dst)
		return append(dst, '"')

	case types.TypeString:
		return AppendJSONString(dst, v.Bytes)

	case types.TypeBytes:
		// StdEncoding COM padding, espelhando o AppendDecode da ingestão. O
		// alfabeto std nunca precisa de escaping.
		dst = append(dst, '"')
		dst = base64.StdEncoding.AppendEncode(dst, v.Bytes)
		return append(dst, '"')

	case types.TypeJSON:
		return appendJSONInline(dst, v.Bytes)

	default:
		return append(dst, "null"...)
	}
}

// appendJSONInline emite o documento json VERBATIM, trocando todo byte < 0x20
// por espaço.
//
// A validade sintática já foi garantida na ingestão (typeconv chama
// json.Valid), e revalidar por linha custaria um scanner completo no caminho
// quente sem ter canal de erro. O que a ingestão NÃO garante é o LAYOUT: o
// tokenizer persiste a janela crua do evento, então um documento
// pretty-printed chega aqui com '\n' literal dentro — e emitido verbatim ele
// quebra o framing NDJSON, fazendo o cliente ler duas linhas inválidas no
// lugar de uma.
//
// A troca é segura porque, num JSON válido, bytes < 0x20 só existem como
// whitespace FORA de string (0x09/0x0A/0x0D): dentro de string a RFC proíbe
// control char cru, e sequências UTF-8 multibyte têm todos os bytes >= 0x80.
// E se o documento NÃO for válido, a troca ainda garante que nenhuma linha
// extra seja emitida: a falha degrada para "uma linha com json estranho" em
// vez de "o stream inteiro dessincronizado".
func appendJSONInline(dst, b []byte) []byte {
	if len(b) == 0 {
		// Um json vazio inlinado produziria `"attrs":,` — JSON inválido.
		return append(dst, "null"...)
	}
	i := indexControle(b)
	if i < 0 {
		return append(dst, b...) // caminho comum: um memmove
	}
	start := 0
	for ; i < len(b); i++ {
		if b[i] >= 0x20 {
			continue
		}
		dst = append(dst, b[start:i]...)
		dst = append(dst, ' ')
		start = i + 1
	}
	return append(dst, b[start:]...)
}

func indexControle(b []byte) int {
	for i, c := range b {
		if c < 0x20 {
			return i
		}
	}
	return -1
}

// AppendJSONString appenda s como string JSON entre aspas.
//
// O escaping é o MÍNIMO da RFC 8259: só `"`, `\` e bytes < 0x20. Não escapa
// `/`, `<`, `>`, `&` (o consumidor é um driver JDBC, não uma página HTML) nem
// U+2028/U+2029 (válidos crus, e em UTF-8 não contêm 0x0A, então não ameaçam o
// framing).
//
// O scan de control chars NÃO é opcional, por mais tentador que seja pular
// direto para um append quando não há aspas: o tokenizer DECODIFICA `\n` para
// o byte cru ao materializar, então uma string armazenada CONTÉM 0x0A — e sem
// o escaping ela quebraria o framing NDJSON.
//
// Byte de UTF-8 inválido vira o escape \ufffd (seis caracteres ASCII, não o
// rune cru), com a mesma paridade do encoding/json. UTF-8 inválido chega aqui
// de verdade: o tokenizer copia a janela do evento sem validar, e a ingestão
// grava esses bytes verbatim para string.
func AppendJSONString(dst, s []byte) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < 0x80 {
			i++
			continue
		}
		if c >= 0x80 {
			r, tam := utf8.DecodeRune(s[i:])
			if r == utf8.RuneError && tam == 1 {
				dst = append(dst, s[start:i]...)
				dst = append(dst, escapeFFFD...)
				i++
				start = i
				continue
			}
			i += tam
			continue
		}
		dst = append(dst, s[start:i]...)
		dst = appendEscape(dst, c)
		i++
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// appendJSONStringS é o irmão de AppendJSONString para string.
//
// A duplicação é deliberada: a conversão string→[]byte alocaria, e resolver
// isso com unsafe seria usar o lado PERIGOSO da conversão (o resultado seria
// mutável) só para economizar um corpo de função. Indexar a string não aloca.
func appendJSONStringS(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c >= 0x20 && c != '"' && c != '\\' && c < 0x80 {
			i++
			continue
		}
		if c >= 0x80 {
			r, tam := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && tam == 1 {
				dst = append(dst, s[start:i]...)
				dst = append(dst, escapeFFFD...)
				i++
				start = i
				continue
			}
			i += tam
			continue
		}
		dst = append(dst, s[start:i]...)
		dst = appendEscape(dst, c)
		i++
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// escapeFFFD é o U+FFFD ESCAPADO (6 caracteres ASCII), e não o rune cru de 3
// bytes. Os dois decodificam para o mesmo caractere, mas a forma escapada é a
// do encoding/json — e paridade com o encoder padrão é o que permite comparar
// a saída com o oráculo byte a byte no teste.
const escapeFFFD = `\ufffd`

const hexDigitos = "0123456789abcdef"

func appendEscape(dst []byte, c byte) []byte {
	switch c {
	case '"':
		return append(dst, '\\', '"')
	case '\\':
		return append(dst, '\\', '\\')
	case 0x08:
		return append(dst, '\\', 'b')
	case 0x09:
		return append(dst, '\\', 't')
	case 0x0A:
		return append(dst, '\\', 'n')
	case 0x0C:
		return append(dst, '\\', 'f')
	case 0x0D:
		return append(dst, '\\', 'r')
	default:
		return append(dst, '\\', 'u', '0', '0', hexDigitos[c>>4], hexDigitos[c&0xf])
	}
}

// appendTimestamp escreve YYYY-MM-DDThh:mm:ss.sssZ — sempre 3 dígitos de
// fração, sempre Z.
//
// Formatador à mão porque o time.AppendFormat custa ~118 ns/op contra ~25 do
// manual, e este é o caminho quente de um campo presente em toda row de
// accounting. O AppendFormat é usado como ORÁCULO no teste.
func appendTimestamp(dst []byte, millis int64) []byte {
	t := time.UnixMilli(millis).UTC()
	ano, mes, dia := t.Date()
	h, min, sec := t.Clock()
	// Go usa resto truncado: (-1) % 1000 == -1. Sem o ajuste, o millis
	// negativo produziria fração negativa e um timestamp malformado.
	frac := millis % 1000
	if frac < 0 {
		frac += 1000
	}

	dst = append(dst, '"')
	dst = appendAno(dst, ano)
	dst = append(dst, '-')
	dst = append2(dst, int(mes))
	dst = append(dst, '-')
	dst = append2(dst, dia)
	dst = append(dst, 'T')
	dst = append2(dst, h)
	dst = append(dst, ':')
	dst = append2(dst, min)
	dst = append(dst, ':')
	dst = append2(dst, sec)
	dst = append(dst, '.')
	dst = append3(dst, int(frac))
	return append(dst, 'Z', '"')
}

// appendHoraOfDia escreve "hh:mm:ss.sss" — sempre 3 dígitos de fração e SEM
// 'Z': não há instante aqui, e não há fuso a anunciar. Anunciar um faria a
// hora do dia parecer um instante UTC, que é exatamente a confusão que o tipo
// separado existe para evitar.
func appendHoraOfDia(dst []byte, millis int64) []byte {
	dst = append(dst, '"')
	dst = append2(dst, int(millis/3600_000))
	dst = append(dst, ':')
	dst = append2(dst, int(millis/60_000%60))
	dst = append(dst, ':')
	dst = append2(dst, int(millis/1000%60))
	dst = append(dst, '.')
	dst = append3(dst, int(millis%1000))
	return append(dst, '"')
}

// appendAno replica a convenção do time.AppendFormat para o layout "2006":
// sinal '-' quando negativo e no mínimo 4 dígitos do valor absoluto.
func appendAno(dst []byte, ano int) []byte {
	if ano < 0 {
		dst = append(dst, '-')
		ano = -ano
	}
	switch {
	case ano < 10:
		dst = append(dst, '0', '0', '0')
	case ano < 100:
		dst = append(dst, '0', '0')
	case ano < 1000:
		dst = append(dst, '0')
	}
	return strconv.AppendInt(dst, int64(ano), 10)
}

func append2(dst []byte, v int) []byte {
	return append(dst, byte('0'+v/10), byte('0'+v%10))
}

func append3(dst []byte, v int) []byte {
	return append(dst, byte('0'+v/100), byte('0'+(v/10)%10), byte('0'+v%10))
}
